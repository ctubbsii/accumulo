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
package org.apache.accumulo.api.client;

import java.util.Set;

import org.apache.accumulo.api.Accumulo;
import org.apache.accumulo.api.AccumuloException;
import org.apache.accumulo.api.client.io.MultiTableWriter;
import org.apache.accumulo.api.data.Namespace;
import org.apache.accumulo.api.security.User;

/**
 * API for client operations, such as reading from a table, writing to a table, managing tables and namespaces, or administering an instance.
 *
 * @since 2.0.0
 */
public interface AccumuloClient extends AutoCloseable {

  /**
   * A builder for constructing an {@link AccumuloClient}.
   */
  interface Builder {

    /**
     * Modify the builder object with the provided connection information.
     *
     * @param instanceName
     *          the name of the Accumulo instance to connect
     * @param zooKeepers
     *          the comma-separated list of hostname:port where this Accumulo's ZooKeepers are running (the :port part being optional and defaulting to :2181)
     * @return this, with the given connection information set
     */
    Buildable withZooKeeper(String instanceName, String zooKeepers);

    /**
     * Modify the builder object with the provided configuration
     *
     * @return this, with the given configuration set
     */
    Buildable withConfiguration(Object configuration);

    /**
     * Modify the builder object with the provided resources. Resource objects provided here should not be closed. If not provided, the client will be built
     * with a resource object that will exist for this client only.
     *
     * @param resources
     *          of the type provided by {@link Accumulo#newClientResources()}
     * @return this, with the given resources set
     */
    Buildable withResources(Resources resources);

    /**
     * Modify the builder object with the provided credentials. If these are not specified, the current system user's name is used, and the credentials are
     * attempted to be read from the user's configured environment.
     *
     * @param user
     *          attributes to authenticate a user
     * @return this, with the given credentials set
     */
    Buildable forUser(User user);

    /**
     * Modify the builder object with the provided namespace name. This configures the client to use the specified namespace for tables not qualified with a
     * namespace. If this is not specified, the default namespace, "", will be used for unqualified table names.
     *
     * @param namespace
     *          the name of the desired default namespace
     * @return this, with the given namespace set as the default
     */
    Buildable usingNamespace(String namespace);

  }

  interface Buildable extends Builder {

    /**
     * Build the {@link AccumuloClient} object from the options previously provided. If no options have been provided, defaults will be used. This may involve
     * reading configuration from a file in a well-known location, or from system properties.
     *
     * @return an instance of {@link AccumuloClient}, configured from the current state of this builder
     * @throws AccumuloException
     *           if the client cannot be constructed
     */
    AccumuloClient build() throws AccumuloException;

  }

  /**
   * An opaque container for resources used by a client. This container can be reused by several clients, but should be closed when no longer in use, to free up
   * resources.
   */
  interface Resources extends AutoCloseable {

    @Override
    void close() throws AccumuloException;

  }

  /**
   * Closing this will free up any internal resources used by this client, and the client will no longer be usable. Closing this will <b>not</b> close any
   * shared {@link Resources} passed to it. The caller must also close those resources to prevent resource leaks.
   *
   * <p>
   * {@inheritDoc}
   *
   */
  @Override
  void close();

  /**
   * Instantiate a new writer, which batches mutations for multiple tables.
   *
   * @return a multi-table writer
   */
  MultiTableWriter multiTableWriter(Object configuration);

  /**
   * Switch this client's current namespace to the one specified. No checks are done to ensure it exists.
   *
   * @param namespace
   *          the new namespace to set as the current
   */
  void useNamespace(Namespace namespace);

  /**
   * Retrieve the current namespace, which is the one set from {@link Builder#usingNamespace(String)}, or the default namespace which is an empty string.
   *
   * @return the namespace object requested
   */
  Namespace currentNamespace();

  /**
   * Retrieve a specific namespace, by its current name.
   *
   * @param name
   *          the current name of the requested namespace, empty string for the default namespace
   * @return the namespace object requested
   * @throws NamespaceNotFound
   *           if the namespace cannot be found with the specified name
   */
  Namespace namespace(String name) throws NamespaceNotFound;

  /**
   * Retrieves a set of objects representing the current table namespaces in the system.
   *
   * @return a set of namespace objects
   */
  Set<Namespace> namespaces();

  /**
   * Construct a new namespace, using a builder.
   *
   * @param name
   *          the desired name of the new namespace
   * @return a namespace builder
   */
  Namespace.Builder namespaceBuilder(String name);

}
