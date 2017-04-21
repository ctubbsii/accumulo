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

import static org.apache.commons.lang.ArrayUtils.EMPTY_STRING_ARRAY;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.volume.Volume;
import org.apache.accumulo.server.client.HdfsZooInstance;
import org.apache.accumulo.server.conf.ServerConfigurationFactory;
import org.apache.commons.collections.map.LRUMap;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A {@link RandomVolumeChooser} that limits its choices from a given set of options to the subset of those options preferred for a particular table. Defaults
 * to selecting from all of the options presented. Can be customized via the table property table.custom.preferredVolumes, which should contain a comma
 * separated list of {@link Volume} URIs. Note that both the property name and the format of its value are specific to this particular implementation.
 */
public class PreferredVolumeChooser extends RandomVolumeChooser {
  private static final Logger log = LoggerFactory.getLogger(PreferredVolumeChooser.class);

  public static final String PREFERRED_VOLUMES_CUSTOM_KEY = Property.TABLE_ARBITRARY_PROP_PREFIX + "preferredVolumes";

  @SuppressWarnings("unchecked")
  private final Map<String,Set<String>> parsedPreferredVolumes = Collections.synchronizedMap(new LRUMap(1000));
  // TODO has to be lazily initialized currently because of the reliance on HdfsZooInstance. see ACCUMULO-3411
  private volatile ServerConfigurationFactory serverConfs;

  @Override
  public String choose(VolumeChooserEnvironment env, String[] options) throws AccumuloException {
    if (!env.hasTableId() && !env.hasScope()) {
      // this should only happen during initialize
      log.warn("No table id or scope, so it's not possible to determine preferred volumes.  Using all volumes.");
      return super.choose(env, options);
    }
    ServerConfigurationFactory localConf = loadConf();

    String systemPreferredVolumes = localConf.getConfiguration().get(PREFERRED_VOLUMES_CUSTOM_KEY);
    if (null == systemPreferredVolumes || systemPreferredVolumes.isEmpty()) {
      String logMessage = "Default preferred volumes are missing.";
      log.debug(logMessage);
      throw new AccumuloException(logMessage);
    }

    String volumes = null;
    if (env.hasTableId()) {
      volumes = localConf.getTableConfiguration(env.getTableId()).get(PREFERRED_VOLUMES_CUSTOM_KEY);
    } else if (env.hasScope()) {
      volumes = localConf.getConfiguration().get(Property.GENERAL_ARBITRARY_PROP_PREFIX.getKey() + env.getScope() + ".preferredVolumes");
    }

    // if there was an empty or missing property, use the system-wide volumes
    if (null == volumes || volumes.isEmpty()) {
      if (env.hasTableId()) {
        log.warn("Missing property for TableID " + env.getTableId() + " but it should have picked up default volumes.");
      } else {
        log.debug("Missing preferred volumes for scope " + env.getScope() + ". Using default volumes.");
      }
      volumes = systemPreferredVolumes;
    }

    if (log.isTraceEnabled()) {
      log.trace("In custom chooser");
      log.trace("Volumes: " + volumes);
      if (env.hasTableId()) {
        log.trace("TableID: " + env.getTableId());
      } else if (env.hasScope()) {
        log.trace("scope: " + env.getScope());
      }
    }

    ArrayList<String> filteredOptions = getIntersection(options, volumes);

    // invalid preferred volumes for this environment result in falling back to system-wide preferred volumes
    if (filteredOptions.isEmpty() && !systemPreferredVolumes.equals(volumes)) { // we may already be using the defaults
      filteredOptions = getIntersection(options, systemPreferredVolumes);
    }

    // Neither default volumes nor specific volumes intersect with defined volumes
    if (filteredOptions.isEmpty()) {
      String logMessage = "After filtering preferred volumes, there are no valid instance volumes.";
      log.warn(logMessage);
      throw new AccumuloException(logMessage);
    }

    // Randomly choose the volume from the preferred volumes
    String choice = super.choose(env, filteredOptions.toArray(EMPTY_STRING_ARRAY));
    if (log.isTraceEnabled()) {
      log.trace("Choice = " + choice);
    }
    return choice;
  }

  private ArrayList<String> getIntersection(String[] options, String volumes) {
    Set<String> preferred = parseVolumes(volumes);
    return filterWithPreferred(options, preferred);
  }

  private ArrayList<String> filterWithPreferred(String[] options, Set<String> preferred) {
    // Only keep the options that are in the preferred set
    final ArrayList<String> filteredOptions = new ArrayList<>(Arrays.asList(options));
    filteredOptions.retainAll(preferred);
    return filteredOptions;
  }

  private Set<String> parseVolumes(String volumes) {
    // If the preferred volumes property was specified, split the returned string by the comma and add use it to filter the given options.
    Set<String> preferred = parsedPreferredVolumes.get(volumes);
    if (preferred == null) {
      preferred = new HashSet<>(Arrays.asList(StringUtils.split(volumes, ',')));
      parsedPreferredVolumes.put(volumes, preferred);
    }
    return preferred;
  }

  private ServerConfigurationFactory loadConf() {
    // Get the current table's properties, and find the preferred volumes property
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
