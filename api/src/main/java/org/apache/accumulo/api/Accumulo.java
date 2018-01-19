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
package org.apache.accumulo.api;

import org.apache.accumulo.api.client.AccumuloClient;
import org.apache.accumulo.api.util.AccumuloClientFactory;
import org.apache.accumulo.api.util.AccumuloServiceLoader;

/**
 * A factory class which represents the main entry point for Accumulo client code. Clients interact with Accumulo by calling the static methods on this class to
 * create the appropriate client objects.
 *
 * <p>
 * Example:<br>
 * <code>
 * ClientConfiguration config = ClientConfiguration.loadDefaults();
 * try (AccumuloClient client = Accumulo.newClient(config)) {
 *   ...
 * } catch (AccumuloException e) {
 *   ...
 * }
 * </code>
 *
 * @since 2.0.0
 */
public final class Accumulo {

  private static final AccumuloClientFactory FACTORY = AccumuloServiceLoader.loadService(AccumuloClientFactory.class);

  /**
   * Create an Accumulo client builder, used to construct a client.
   *
   * @return a builder object for Accumulo clients
   */
  public static AccumuloClient.Builder newClient() {
    return FACTORY.newClient();
  }

  /**
   * Create a new AccumuloClient.Resources, to share among multiple clients. To use this option, you must create your clients using {@link #newClient()} .
   *
   * <p>
   * To prevent resource leaks, one must close these resources when no longer in use by any clients. It will not automatically be closed when the clients are
   * closed.
   *
   * @return an opaque resources object for sharing among multiple clients.
   */
  public static AccumuloClient.Resources newClientResources() {
    return FACTORY.newClientResources();
  }

  // factory class is not instantiable
  private Accumulo() {}

}
