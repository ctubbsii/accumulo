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
package org.apache.accumulo.server.master.balancer;

import java.util.Map;
import org.apache.accumulo.core.client.Instance;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.server.AccumuloServerContext;
import org.apache.accumulo.server.conf.ServerConfigurationFactory;

/**
 * Default implementation of TabletBalancerEnvironment.
 */
public class TabletBalancerEnvironment {

  protected ServerConfigurationFactory configurationFactory;
  protected Instance instance;
  protected AccumuloServerContext context;
  protected Map<String,String> customTableProperties;

  public TabletBalancerEnvironment(Instance instance, ServerConfigurationFactory configurationFactory) {
    context = new AccumuloServerContext(instance, configurationFactory);
    this.instance = instance;
    this.configurationFactory = configurationFactory;
    customTableProperties = this.configurationFactory.getSystemConfiguration().getAllPropertiesWithPrefix(Property.TABLE_ARBITRARY_PROP_PREFIX);
  }

  public ServerConfigurationFactory getServerConfigurationFactory() {
    return configurationFactory;
  }

  public Instance getInstance() {
    return instance;
  }

  public AccumuloServerContext getContext() {
    return context;
  }

  public Map<String, String> getCustomTableProperties() {
    return customTableProperties;
  }
  public String getCustomTableProperty(String property) {
    return customTableProperties.get(property);
  }
}
