/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.accumulo.core.client.admin;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Set;
import java.util.SortedMap;
import java.util.SortedSet;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.client.sample.SamplerConfiguration;
import org.apache.accumulo.core.client.summary.Summarizer;
import org.apache.accumulo.core.client.summary.SummarizerConfiguration;
import org.apache.accumulo.core.clientImpl.TableOperationsHelper;
import org.apache.accumulo.core.clientImpl.TabletMergeabilityUtil;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.iterators.IteratorUtil.IteratorScope;
import org.apache.accumulo.core.iterators.user.VersioningIterator;
import org.apache.accumulo.core.iteratorsImpl.IteratorConfigUtil;
import org.apache.accumulo.core.sample.impl.SamplerConfigurationImpl;
import org.apache.accumulo.core.summary.SummarizerConfigurationUtil;
import org.apache.accumulo.core.util.LocalityGroupUtil;
import org.apache.accumulo.core.util.LocalityGroupUtil.LocalityGroupConfigurationError;
import org.apache.hadoop.io.Text;

import com.google.common.collect.ImmutableSortedMap;

/**
 * This object stores table creation parameters. Currently includes: {@link TimeType}, whether to
 * include default iterators, and user-specified initial properties
 *
 * @since 1.7.0
 */
public class NewTableConfiguration {

  private static final TimeType DEFAULT_TIME_TYPE = TimeType.MILLIS;
  private TimeType timeType = DEFAULT_TIME_TYPE;

  private static final InitialTableState DEFAULT_CREATION_MODE = InitialTableState.ONLINE;
  private InitialTableState initialTableState = DEFAULT_CREATION_MODE;

  private boolean limitVersion = true;

  private Map<String,String> properties = Collections.emptyMap();
  private Map<String,String> samplerProps = Collections.emptyMap();
  private Map<String,String> summarizerProps = Collections.emptyMap();
  private Map<String,String> localityProps = Collections.emptyMap();
  private final Map<String,String> iteratorProps = new HashMap<>();
  private SortedMap<Text,TabletMergeability> splitProps = Collections.emptySortedMap();
  private TabletAvailability initialTabletAvailability = TabletAvailability.ONDEMAND;

  private void checkDisjoint(Map<String,String> props, Map<String,String> derivedProps,
      String kind) {
    checkArgument(Collections.disjoint(props.keySet(), derivedProps.keySet()),
        "Properties and derived %s properties are not disjoint", kind);
  }

  /**
   * Configure logical or millisecond time for tables created with this configuration.
   *
   * @param tt the time type to use; defaults to milliseconds
   * @return this
   */
  public NewTableConfiguration setTimeType(TimeType tt) {
    checkArgument(tt != null, "TimeType is null");
    this.timeType = tt;
    return this;
  }

  /**
   * Retrieve the time type currently configured.
   *
   * @return the time type
   */
  public TimeType getTimeType() {
    return timeType;
  }

  /**
   * Currently the only default iterator is the {@link VersioningIterator}. This method will cause
   * the table to be created without that iterator, or any others which may become defaults in the
   * future.
   *
   * @return this
   */
  public NewTableConfiguration withoutDefaultIterators() {
    this.limitVersion = false;
    return this;
  }

  /**
   * Create the new table in an offline state.
   *
   * @return this
   *
   * @since 2.0.0
   */
  public NewTableConfiguration createOffline() {
    this.initialTableState = InitialTableState.OFFLINE;
    return this;
  }

  /**
   * Return value indicating whether table is to be created in offline or online mode.
   *
   * @since 2.0.0
   */
  public InitialTableState getInitialTableState() {
    return initialTableState;
  }

  /**
   * Sets additional properties to be applied to tables created with this configuration. Additional
   * calls to this method replace properties set by previous calls.
   *
   * @param props additional properties to add to the table when it is created
   * @return this
   */
  public NewTableConfiguration setProperties(Map<String,String> props) {
    checkArgument(props != null, "properties is null");
    checkDisjoint(props, samplerProps, "sampler");
    checkDisjoint(props, summarizerProps, "summarizer");
    checkDisjoint(props, localityProps, "locality group");
    checkDisjoint(props, iteratorProps, "iterator");
    checkTableProperties(props);

    try {
      LocalityGroupUtil.checkLocalityGroups(props);
    } catch (LocalityGroupConfigurationError e) {
      throw new IllegalArgumentException(e);
    }

    this.properties = new HashMap<>(props);
    return this;
  }

  /**
   * Retrieves the complete set of currently configured table properties to be applied to a table
   * when this configuration object is used.
   *
   * @return the current properties configured
   */
  public Map<String,String> getProperties() {
    Map<String,String> propertyMap = new HashMap<>();

    if (limitVersion) {
      propertyMap.putAll(IteratorConfigUtil.generateInitialTableProperties(limitVersion));
    }

    propertyMap.putAll(summarizerProps);
    propertyMap.putAll(samplerProps);
    propertyMap.putAll(properties);
    propertyMap.putAll(iteratorProps);
    propertyMap.putAll(localityProps);
    return Collections.unmodifiableMap(propertyMap);
  }

  /**
   * Return Collection of split values.
   *
   * @return Collection containing splits associated with this NewTableConfiguration object.
   *
   * @since 2.0.0
   */
  public Collection<Text> getSplits() {
    return splitProps.keySet();
  }

  /**
   * Return Collection of split values and associated TabletMergeability.
   *
   * @return Collection containing splits and TabletMergeability associated with this
   *         NewTableConfiguration object.
   *
   * @since 4.0.0
   */
  public SortedMap<Text,TabletMergeability> getSplitsMap() {
    return splitProps;
  }

  /**
   * Enable building a sample data set on the new table using the given sampler configuration.
   *
   * @return this
   *
   * @since 1.8.0
   */
  public NewTableConfiguration enableSampling(SamplerConfiguration samplerConfiguration) {
    requireNonNull(samplerConfiguration);
    Map<String,String> tmp =
        new SamplerConfigurationImpl(samplerConfiguration).toTablePropertiesMap();
    checkDisjoint(properties, tmp, "sampler");
    this.samplerProps = tmp;
    return this;
  }

  /**
   * Enables creating summary statistics using {@link Summarizer}'s for the new table.
   *
   * @return this
   *
   * @since 2.0.0
   */
  public NewTableConfiguration enableSummarization(SummarizerConfiguration... configs) {
    requireNonNull(configs);
    Map<String,String> tmp =
        SummarizerConfigurationUtil.toTablePropertiesMap(Arrays.asList(configs));
    checkDisjoint(properties, tmp, "summarizer");
    summarizerProps = tmp;
    return this;
  }

  /**
   * Configures a table's locality groups prior to initial table creation.
   *
   * Allows locality groups to be set prior to table creation. Additional calls to this method prior
   * to table creation will overwrite previous locality group mappings.
   *
   * @param groups mapping of locality group names to column families in the locality group
   *
   * @since 2.0.0
   *
   * @see TableOperations#setLocalityGroups
   */
  public NewTableConfiguration setLocalityGroups(Map<String,Set<Text>> groups) {
    // ensure locality groups do not overlap
    LocalityGroupUtil.ensureNonOverlappingGroups(groups);
    Map<String,String> tmp = new HashMap<>();
    for (Entry<String,Set<Text>> entry : groups.entrySet()) {
      Set<Text> colFams = entry.getValue();
      String value = LocalityGroupUtil.encodeColumnFamilies(colFams);
      tmp.put(Property.TABLE_LOCALITY_GROUP_PREFIX + entry.getKey(), value);
    }
    tmp.put(Property.TABLE_LOCALITY_GROUPS.getKey(), String.join(",", groups.keySet()));
    checkDisjoint(properties, tmp, "locality groups");
    localityProps = tmp;
    return this;
  }

  /**
   * Create a new table with pre-configured splits from the provided input collection.
   *
   * @param splits A SortedSet of String values to be used as split points in a newly created table.
   * @return this
   *
   * @since 2.0.0
   */
  @SuppressWarnings("unchecked")
  public NewTableConfiguration withSplits(final SortedSet<Text> splits) {
    checkArgument(splits != null, "splits set is null");
    checkArgument(!splits.isEmpty(), "splits set is empty");
    return withSplits(TabletMergeabilityUtil.userDefaultSplits(splits));
  }

  public NewTableConfiguration withSplits(final SortedMap<Text,TabletMergeability> splits) {
    checkArgument(splits != null, "splits set is null");
    checkArgument(!splits.isEmpty(), "splits set is empty");
    this.splitProps = ImmutableSortedMap.copyOf(splits);
    return this;
  }

  /**
   * Configure iterator settings for a table prior to its creation.
   *
   * Additional calls to this method before table creation will overwrite previous iterator
   * settings.
   *
   * @param setting object specifying the properties of the iterator
   * @return this
   *
   * @since 2.0.0
   *
   * @see TableOperations#attachIterator(String, IteratorSetting)
   */
  public NewTableConfiguration attachIterator(IteratorSetting setting) {
    return attachIterator(setting, EnumSet.allOf(IteratorScope.class));
  }

  /**
   * Configure iterator settings for a table prior to its creation.
   *
   * @param setting object specifying the properties of the iterator
   * @param scopes enumerated set of iterator scopes
   * @return this
   *
   * @since 2.0.0
   *
   * @see TableOperations#attachIterator(String, IteratorSetting, EnumSet)
   */
  public NewTableConfiguration attachIterator(IteratorSetting setting,
      EnumSet<IteratorScope> scopes) {
    Objects.requireNonNull(setting, "setting cannot be null!");
    Objects.requireNonNull(scopes, "scopes cannot be null!");
    try {
      TableOperationsHelper.checkIteratorConflicts(iteratorProps, setting, scopes);
    } catch (AccumuloException e) {
      throw new IllegalArgumentException("The specified IteratorSetting"
          + " conflicts with an iterator already defined on this NewTableConfiguration", e);
    }
    for (IteratorScope scope : scopes) {
      String root = String.format("%s%s.%s", Property.TABLE_ITERATOR_PREFIX,
          scope.name().toLowerCase(), setting.getName());
      for (Entry<String,String> prop : setting.getOptions().entrySet()) {
        iteratorProps.put(root + ".opt." + prop.getKey(), prop.getValue());
      }
      iteratorProps.put(root, setting.getPriority() + "," + setting.getIteratorClass());
      // verify that the iteratorProps assigned and the properties do not share any keys.
      checkDisjoint(properties, iteratorProps, "iterator");
    }
    return this;
  }

  /**
   * Sets the initial tablet availability for all tablets. If not set, the default is
   * {@link TabletAvailability#ONDEMAND}
   *
   * @since 4.0.0
   */
  public NewTableConfiguration
      withInitialTabletAvailability(final TabletAvailability tabletAvailability) {
    this.initialTabletAvailability = tabletAvailability;
    return this;
  }

  /**
   * @since 4.0.0
   */
  public TabletAvailability getInitialTabletAvailability() {
    return this.initialTabletAvailability;
  }

  /**
   * Verify the provided properties are valid table properties.
   */
  private void checkTableProperties(Map<String,String> props) {
    props.keySet().forEach((key) -> {
      if (!key.startsWith(Property.TABLE_PREFIX.toString())) {
        throw new IllegalArgumentException("'" + key + "' is not a valid table property");
      }
    });
  }
}
