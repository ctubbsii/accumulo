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
package org.apache.accumulo.api.util;

import org.apache.accumulo.api.Accumulo;
import org.apache.accumulo.api.client.AccumuloClient;

/**
 * The service programming interface for the client factory. Clients will not use this interface directly. Service providers use it to provide an implementation
 * for the main API entry point, {@link Accumulo}.
 *
 * @since 2.0.0
 */
public interface AccumuloClientFactory {

  AccumuloClient.Builder newClient();

  AccumuloClient.Resources newClientResources();

}
