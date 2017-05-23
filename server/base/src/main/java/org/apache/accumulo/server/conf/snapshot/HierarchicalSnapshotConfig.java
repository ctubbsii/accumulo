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
package org.apache.accumulo.server.conf.snapshot;

import java.util.Collections;
import java.util.Map;
import java.util.Optional;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import org.apache.accumulo.core.conf.Property;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HierarchicalSnapshotConfig implements ConfigurationSnapshot {

  private static final Logger log = LoggerFactory.getLogger(ConfigurationSnapshot.class);

  private final Map<String,String> config;
  private final ConfigurationSnapshot parent;
  public final int sequenceId;

  public HierarchicalSnapshotConfig(int seqId, final Map<String,String> config, final Optional<ConfigurationSnapshot> parent) {
    this.sequenceId = seqId;
    this.config = Collections.unmodifiableMap(config);
    this.parent = parent.orElse(ConfigurationSnapshot.EMPTY);
  }

  @Override
  public String get(final Property property) {
    String key = property.getKey();
    String value = config.get(key);

    if (value == null || !property.getType().isValidFormat(value)) {
      if (value != null) {
        log.error("Using default value for " + key + " due to improperly formatted " + property.getType() + ": " + value);
      }
      value = parent.get(property);
    }
    return value;
  }

  @Override
  public void getProperties(final Map<String,String> destination, final Predicate<String> filter, final Predicate<String> parentFilter) {
    // add stuff from parent, which passes both filters
    parent.getProperties(destination, parentFilter != null ? parentFilter.and(filter) : filter, null);

    // clobber with stuff that passes the non-parent filter
    destination.putAll(config.entrySet().stream().filter(x -> filter.test(x.getKey())).collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue)));
  }

  @Override
  public String toString() {
    return config.toString();
  }

  @Override
  public boolean equals(Object obj) {
    return obj instanceof HierarchicalSnapshotConfig && ((HierarchicalSnapshotConfig) obj).sequenceId == this.sequenceId;
  }

  @Override
  public int hashCode() {
    return this.sequenceId;
  }

}
