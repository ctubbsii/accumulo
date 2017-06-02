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

import org.apache.accumulo.core.client.Instance;
import org.apache.accumulo.server.AccumuloServerContext;
import org.apache.accumulo.server.conf.ServerConfigurationFactory;

/**
 * Default implementation of TabletBalancerEnvironment.
 */
public class DefaultTabletBalancerEnvironment implements TabletBalancerEnvironment {

  protected ServerConfigurationFactory configuration;
  protected Instance instance;
  protected AccumuloServerContext context;

  public DefaultTabletBalancerEnvironment(Instance instance, ServerConfigurationFactory conf) {
    context = new AccumuloServerContext(instance, conf);
    this.instance = instance;
    configuration = conf;
  }

  @Override
  public ServerConfigurationFactory getConfiguration() {
    return configuration;
  }

  @Override
  public Instance getInstance() {
    return instance;
  }

  @Override
  public AccumuloServerContext getContext() {
    return context;
  }
}
