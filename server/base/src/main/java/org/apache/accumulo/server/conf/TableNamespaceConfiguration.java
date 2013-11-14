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
package org.apache.accumulo.server.conf;

import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.accumulo.core.Constants;
import org.apache.accumulo.core.client.Instance;
import org.apache.accumulo.core.conf.AccumuloConfiguration;
import org.apache.accumulo.core.conf.ConfigurationObserver;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.zookeeper.ZooUtil;
import org.apache.accumulo.fate.zookeeper.ZooCache;
import org.apache.accumulo.server.client.HdfsZooInstance;
import org.apache.log4j.Logger;

public class TableNamespaceConfiguration extends AccumuloConfiguration {
  private static final Logger log = Logger.getLogger(TableNamespaceConfiguration.class);

  private final AccumuloConfiguration parent;
  private static ZooCache propCache = null;
  protected String namespaceId = null;
  protected Instance inst = null;
  private Set<ConfigurationObserver> observers;

  public TableNamespaceConfiguration(String namespaceId, AccumuloConfiguration parent) {
    inst = HdfsZooInstance.getInstance();
    this.parent = parent;
    this.namespaceId = namespaceId;
    this.observers = Collections.synchronizedSet(new HashSet<ConfigurationObserver>());
  }

  @Override
  public String get(Property property) {
    String key = property.getKey();
    String value = get(getPropCache(), key);

    if (value == null || !property.getType().isValidFormat(value)) {
      if (value != null)
        log.error("Using default value for " + key + " due to improperly formatted " + property.getType() + ": " + value);
      if (!isIterConst(property.getKey()))
        value = parent.get(property);
    }
    return value;
  }

  private String get(ZooCache zc, String key) {
    String zPath = ZooUtil.getRoot(inst.getInstanceID()) + Constants.ZNAMESPACES + "/" + getNamespaceId() + Constants.ZNAMESPACE_CONF + "/" + key;
    byte[] v = zc.get(zPath);
    String value = null;
    if (v != null)
      value = new String(v, Constants.UTF8);
    return value;
  }

  private static ZooCache getPropCache() {
    Instance inst = HdfsZooInstance.getInstance();
    if (propCache == null)
      synchronized (TableNamespaceConfiguration.class) {
        if (propCache == null)
          propCache = new ZooCache(inst.getZooKeepers(), inst.getZooKeepersSessionTimeOut(), new TableNamespaceConfWatcher(inst));
      }
    return propCache;
  }

  private class SystemNamespaceFilter implements PropertyFilter {

    private PropertyFilter userFilter;

    SystemNamespaceFilter(PropertyFilter userFilter) {
      this.userFilter = userFilter;
    }

    @Override
    public boolean accept(String key) {
      if (isIterConst(key))
        return false;
      return userFilter.accept(key);
    }

  }

  @Override
  public void getProperties(Map<String,String> props, PropertyFilter filter) {

    PropertyFilter parentFilter = filter;

    // exclude system iterators/constraints from the system namespace
    // so they don't affect the metadata or root tables.
    if (this.namespaceId.equals(Constants.SYSTEM_TABLE_NAMESPACE_ID))
      parentFilter = new SystemNamespaceFilter(filter);

    parent.getProperties(props, parentFilter);

    ZooCache zc = getPropCache();

    List<String> children = zc.getChildren(ZooUtil.getRoot(inst.getInstanceID()) + Constants.ZNAMESPACES + "/" + getNamespaceId() + Constants.ZNAMESPACE_CONF);
    if (children != null) {
      for (String child : children) {
        if (child != null && filter.accept(child)) {
          String value = get(zc, child);
          if (value != null)
            props.put(child, value);
        }
      }
    }
  }

  protected String getNamespaceId() {
    return namespaceId;
  }

  public void addObserver(ConfigurationObserver co) {
    if (namespaceId == null) {
      String err = "Attempt to add observer for non-table-namespace configuration";
      log.error(err);
      throw new RuntimeException(err);
    }
    iterator();
    observers.add(co);
  }

  public void removeObserver(ConfigurationObserver configObserver) {
    if (namespaceId == null) {
      String err = "Attempt to remove observer for non-table-namespace configuration";
      log.error(err);
      throw new RuntimeException(err);
    }
    observers.remove(configObserver);
  }

  public void expireAllObservers() {
    Collection<ConfigurationObserver> copy = Collections.unmodifiableCollection(observers);
    for (ConfigurationObserver co : copy)
      co.sessionExpired();
  }

  public void propertyChanged(String key) {
    Collection<ConfigurationObserver> copy = Collections.unmodifiableCollection(observers);
    for (ConfigurationObserver co : copy)
      co.propertyChanged(key);
  }

  public void propertiesChanged(String key) {
    Collection<ConfigurationObserver> copy = Collections.unmodifiableCollection(observers);
    for (ConfigurationObserver co : copy)
      co.propertiesChanged();
  }

  protected boolean isIterConst(String key) {
    return key.startsWith(Property.TABLE_ITERATOR_PREFIX.getKey()) || key.startsWith(Property.TABLE_CONSTRAINT_PREFIX.getKey());
  }
}
