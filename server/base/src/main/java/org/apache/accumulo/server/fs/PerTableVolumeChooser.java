/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.accumulo.server.fs;

import java.util.concurrent.ConcurrentHashMap;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.conf.AccumuloConfiguration;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.server.client.HdfsZooInstance;
import org.apache.accumulo.server.conf.ServerConfigurationFactory;
import org.apache.accumulo.server.conf.TableConfiguration;
import org.apache.accumulo.start.classloader.vfs.AccumuloVFSClassLoader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A {@link VolumeChooser} that delegates to another volume chooser based on other properties: table.custom.volume.chooser for tables, and
 * general.custom.scoped.volume.chooser for scopes. general.custor.{scope}.volume.chooser can override the system wide setting for
 * general.custom.scoped.volume.chooser. At the this this was written, the only known scope was "logger".
 */
public class PerTableVolumeChooser implements VolumeChooser {
  private static final Logger log = LoggerFactory.getLogger(PerTableVolumeChooser.class);
  // TODO Add hint of expected size to construction, see ACCUMULO-3410
  /* Track VolumeChooser instances so they can keep state. */
  private final ConcurrentHashMap<String,VolumeChooser> tableSpecificChooser = new ConcurrentHashMap<>();
  private final ConcurrentHashMap<String,VolumeChooser> scopeSpecificChooser = new ConcurrentHashMap<>();
  private final RandomVolumeChooser randomChooser = new RandomVolumeChooser();
  // TODO has to be lazily initialized currently because of the reliance on HdfsZooInstance. see ACCUMULO-3411
  private volatile ServerConfigurationFactory serverConfs;

  public static final String TABLE_VOLUME_CHOOSER = Property.TABLE_ARBITRARY_PROP_PREFIX.getKey() + "volume.chooser";

  public static final String SCOPED_VOLUME_CHOOSER(String scope) {
    return Property.GENERAL_ARBITRARY_PROP_PREFIX.getKey() + scope + ".volume.chooser";
  }

  public static final String DEFAULT_SCOPED_VOLUME_CHOOSER = SCOPED_VOLUME_CHOOSER("scoped");

  @Override
  public String choose(VolumeChooserEnvironment env, String[] options) throws AccumuloException {
    if (log.isTraceEnabled()) {
      log.trace("PerTableVolumeChooser.choose");
    }
    VolumeChooser chooser;
    if (!env.hasTableId() && !env.hasScope()) {
      // Should only get here during Initialize. Configurations are not yet available.
      return randomChooser.choose(env, options);
    }

    ServerConfigurationFactory localConf = loadConf();
    if (env.hasScope()) {
      // use the system configuration
      chooser = getVolumeChooserForNonTable(env, localConf);
    } else { // if (env.hasTableId()) {
      // use the table configuration
      chooser = getVolumeChooserForTable(env, localConf);
    }

    return chooser.choose(env, options);
  }

  private VolumeChooser getVolumeChooserForTable(VolumeChooserEnvironment env, ServerConfigurationFactory localConf) throws AccumuloException {
    if (log.isTraceEnabled()) {
      log.trace("Looking up property " + TABLE_VOLUME_CHOOSER + " for Table id: " + env.getTableId());
    }
    final TableConfiguration tableConf = localConf.getTableConfiguration(env.getTableId());
    String clazz = tableConf.get(TABLE_VOLUME_CHOOSER);

    return createVolumeChooser(clazz, TABLE_VOLUME_CHOOSER, env.getTableId(), tableSpecificChooser);
  }

  private VolumeChooser getVolumeChooserForNonTable(VolumeChooserEnvironment env, ServerConfigurationFactory localConf) throws AccumuloException {
    String property = SCOPED_VOLUME_CHOOSER(env.getScope());

    if (log.isTraceEnabled()) {
      log.trace("Looking up property: " + property);
    }

    AccumuloConfiguration systemConfiguration = localConf.getSystemConfiguration();
    String clazz = systemConfiguration.get(property);
    // only if the custom property is not set to we fallback to the table volume chooser setting
    if (null == clazz) {
      log.debug("Property not found: " + property + " using " + DEFAULT_SCOPED_VOLUME_CHOOSER);
      property = DEFAULT_SCOPED_VOLUME_CHOOSER;
      clazz = systemConfiguration.get(DEFAULT_SCOPED_VOLUME_CHOOSER);
    }

    return createVolumeChooser(clazz, property, env.getScope(), scopeSpecificChooser);
  }

  private VolumeChooser createVolumeChooser(String clazz, String property, String key, ConcurrentHashMap<String,VolumeChooser> cache) throws AccumuloException {
    if (null == clazz || clazz.isEmpty()) {
      String msg = "Property " + property + " must be set" + (null == clazz ? " " : " properly ") + "to use the " + getClass().getSimpleName();
      log.error(msg);
      throw new AccumuloException(msg);
    }

    VolumeChooser chooser = cache.get(key);
    if (chooser == null || !(chooser.getClass().getName().equals(clazz))) {
      if (log.isTraceEnabled() && chooser != null) {
        // TODO stricter definition of when the updated property is used, ref ACCUMULO-3412
        log.trace("Change detected for " + property + " for " + key);
      }
      VolumeChooser temp;
      try {
        temp = loadClass(clazz);
      } catch (Exception e) {
        String msg = "Failed to create instance for " + key + " configured to use " + clazz + " via " + property;
        log.error(msg);
        throw new AccumuloException(msg, e);
      }
      if (chooser == null) {
        chooser = cache.putIfAbsent(key, temp);
        if (chooser == null) {
          chooser = temp;
          // Otherwise, someone else beat us to initializing; use theirs.
        }
      } else {
        VolumeChooser last = cache.replace(key, temp);
        if (chooser.equals(last)) {
          chooser = temp;
        } else {
          // Someone else beat us to updating; use theirs.
          chooser = last;
        }
      }
    }
    return chooser;
  }

  private VolumeChooser loadClass(String className) throws ClassNotFoundException, IllegalAccessException, InstantiationException {
    // not attempting to load context because this approach to loading the class is for non-tables only
    return AccumuloVFSClassLoader.loadClass(className, VolumeChooser.class).newInstance();
  }

  private ServerConfigurationFactory loadConf() {
    // This local variable is an intentional component of the single-check idiom.
    ServerConfigurationFactory localConf = serverConfs;
    if (localConf == null) {
      // If we're under contention when first getting here we'll throw away some initializations.
      localConf = new ServerConfigurationFactory(HdfsZooInstance.getInstance());
      serverConfs = localConf;
    }
    return localConf;
  }

}
