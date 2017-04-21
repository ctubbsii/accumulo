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
 * A {@link VolumeChooser} that delegates to another volume chooser based on the presence of an experimental table property,
 * {@link Property#TABLE_VOLUME_CHOOSER}. If it isn't found, defaults back to {@link RandomVolumeChooser}.
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
  private volatile VolumeChooser fallbackVolumeChooser = null;

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
    lazilyCreateFallbackChooser();
    if (env.hasScope()) {
      // use the system configuration
      chooser = getVolumeChooserForNonTable(env, localConf);
    } else { // if (env.hasTableId()) {
      // use the table configuration
      chooser = getVolumeChooserForTable(env, localConf);
    }

    return chooser.choose(env, options);
  }

  private VolumeChooser getVolumeChooserForTable(VolumeChooserEnvironment env, ServerConfigurationFactory localConf) {
    VolumeChooser chooser;
    if (log.isTraceEnabled()) {
      log.trace("Table id: " + env.getTableId());
    }
    final TableConfiguration tableConf = localConf.getTableConfiguration(env.getTableId());
    String clazz = tableConf.get(Property.TABLE_VOLUME_CHOOSER);
    if (null == clazz || clazz.isEmpty()) {
      chooser = fallbackVolumeChooser;
    } else {
      chooser = tableSpecificChooser.get(env.getTableId());
      if (chooser == null) {
        VolumeChooser temp = Property.createTableInstanceFromPropertyName(tableConf, Property.TABLE_VOLUME_CHOOSER, VolumeChooser.class, fallbackVolumeChooser);
        chooser = tableSpecificChooser.putIfAbsent(env.getTableId(), temp);
        if (chooser == null) {
          chooser = temp;
          // Otherwise, someone else beat us to initializing; use theirs.
        }
      } else if (!(chooser.getClass().getName().equals(clazz))) {
        if (log.isTraceEnabled()) {
          log.trace("change detected for table id: " + env.getTableId());
        }
        // the configuration for this table's chooser has been updated. In the case of failure to instantiate we'll repeat here next call.
        // TODO stricter definition of when the updated property is used, ref ACCUMULO-3412
        VolumeChooser temp = Property.createTableInstanceFromPropertyName(tableConf, Property.TABLE_VOLUME_CHOOSER, VolumeChooser.class, fallbackVolumeChooser);
        VolumeChooser last = tableSpecificChooser.replace(env.getTableId(), temp);
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

  private VolumeChooser getVolumeChooserForNonTable(VolumeChooserEnvironment env, ServerConfigurationFactory localConf) {
    VolumeChooser chooser;
    final String customProperty = Property.GENERAL_ARBITRARY_PROP_PREFIX.getKey() + env.getScope() + ".volume.chooser";

    if (log.isTraceEnabled()) {
      log.trace("Scope: " + env.getScope());
      log.trace("Looking up property: " + customProperty);
    }

    AccumuloConfiguration systemConfiguration = localConf.getConfiguration();
    String clazz = systemConfiguration.get(customProperty);
    if (null == clazz || clazz.isEmpty()) {
      log.debug("Property not found: " + customProperty + " using fallback chooser.");
      return fallbackVolumeChooser;
    } else {
      chooser = scopeSpecificChooser.get(env.getScope());
      if (chooser == null) {
        VolumeChooser temp;
        try {
          temp = loadClassForCustomProperty(clazz);
        } catch (Exception e) {
          log.error("Failed to create instance for " + env.getScope() + " configured to use " + clazz + " via " + customProperty);
          return fallbackVolumeChooser;
        }
        chooser = scopeSpecificChooser.putIfAbsent(env.getScope(), temp);
        if (chooser == null) {
          chooser = temp;
          // Otherwise, someone else beat us to initializing; use theirs.
        }
      } else if (!(chooser.getClass().getName().equals(clazz))) {
        if (log.isTraceEnabled()) {
          log.trace("change detected for scope: " + env.getScope());
        }
        // the configuration for this scope's chooser has been updated. In the case of failure to instantiate we'll repeat here next call.
        // TODO stricter definition of when the updated property is used, ref ACCUMULO-3412
        VolumeChooser temp;
        try {
          temp = loadClassForCustomProperty(clazz);
        } catch (Exception e) {
          log.error("Failed to create instance for " + env.getScope() + " configured to use " + clazz + " via " + customProperty);
          return fallbackVolumeChooser;
        }
        VolumeChooser last = scopeSpecificChooser.replace(env.getScope(), temp);
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

  private VolumeChooser loadClassForCustomProperty(String className) throws ClassNotFoundException, IllegalAccessException, InstantiationException {
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

  private void lazilyCreateFallbackChooser() throws AccumuloException {
    VolumeChooser currentFallback = fallbackVolumeChooser;
    if (currentFallback == null || !(currentFallback.getClass().getName().equals(serverConfs.getConfiguration().get(Property.TABLE_VOLUME_CHOOSER)))) {
      if (log.isTraceEnabled()) {
        log.trace("Creating fallbackVolumeChooser.");
      }
      VolumeChooser temp = createFallbackChooser(serverConfs.getConfiguration());
      if (fallbackVolumeChooser == null || fallbackVolumeChooser.equals(currentFallback)) {
        fallbackVolumeChooser = temp;
        if (log.isTraceEnabled()) {
          log.trace("Updated fallbackVolumeChooser to " + (null != fallbackVolumeChooser ? fallbackVolumeChooser.getClass().getName() : "null"));
        }
      } // otherwise it was already updated by another thread
    }
  }

  private VolumeChooser createFallbackChooser(AccumuloConfiguration accumuloConfiguration) throws AccumuloException {
    if (log.isTraceEnabled()) {
      log.trace("Creating fallback chooser.");
    }
    // attempt to load the system-wide default volume chooser
    String clazz = accumuloConfiguration.get(Property.TABLE_VOLUME_CHOOSER);
    if (null == clazz || clazz.isEmpty()) {
      log.error("Cannot create fallback volume chooser with the system-wide configuration. " + Property.TABLE_VOLUME_CHOOSER.getKey()
          + " is required as a system-wide property.");
      throw new AccumuloException(Property.TABLE_VOLUME_CHOOSER.getKey() + " was not found in the system-wide configuration.");
    }
    return Property.createInstanceFromPropertyName(accumuloConfiguration, Property.TABLE_VOLUME_CHOOSER, VolumeChooser.class, null);
  }
}
