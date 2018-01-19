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
package org.apache.accumulo.core.api.impl;

import java.util.Arrays;
import java.util.LinkedHashSet;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.accumulo.api.client.AccumuloClient;
import org.apache.accumulo.api.client.NamespaceNotFound;
import org.apache.accumulo.api.client.TableNotFound;
import org.apache.accumulo.api.client.io.MultiTableWriter;
import org.apache.accumulo.api.data.Namespace;
import org.apache.accumulo.api.data.Table;
import org.apache.accumulo.api.security.User;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.ClientConfiguration;
import org.apache.accumulo.core.client.Instance;
import org.apache.accumulo.core.client.impl.ClientContext;
import org.apache.accumulo.core.client.impl.Credentials;
import org.apache.accumulo.core.client.impl.Tables;
import org.apache.accumulo.core.client.security.tokens.AuthenticationToken;

/**
 *
 */
public class AccumuloClientImpl extends ClientContext implements AccumuloClient {

  public static class BuilderImpl implements Buildable {

    private Optional<Object> configuration = Optional.empty();
    private Optional<User> user = Optional.empty();
    private Optional<ClientResourcesImpl> resources = Optional.empty();
    private Optional<String> defaultNamespace = Optional.empty();
    private Optional<String> instanceName = Optional.empty();
    private Optional<LinkedHashSet<String>> zooKeepers = Optional.empty();

    @Override
    public AccumuloClient build() {
      // TODO verify required parameters here
      // use default resources if not specified
      if (!user.isPresent() && configuration.isPresent()) {
        // TODO construct credentials from configuration
      }
      return new AccumuloClientImpl(instanceName, zooKeepers, configuration.orElse(ClientConfiguration.loadDefault()), user.get(), resources, defaultNamespace);
    }

    @Override
    public Buildable withConfiguration(Object configuration) {
      // TODO parse configuration here
      this.configuration = Optional.of(configuration);
      return this;
    }

    @Override
    public Buildable withResources(AccumuloClient.Resources resources) {
      this.resources = Optional.of(ClientResourcesImpl.class.cast(resources));
      return this;
    }

    @Override
    public Buildable forUser(User credentials) {
      this.user = Optional.of(credentials);
      return this;
    }

    @Override
    public Buildable usingNamespace(String namespace) {
      this.defaultNamespace = Optional.of(namespace);
      return this;
    }

    @Override
    public Buildable withZooKeeper(String instanceName, String zooKeepers) {
      this.instanceName = Optional.of(instanceName);
      this.zooKeepers = Optional.of(zooKeepers).map(input -> new LinkedHashSet<>(Arrays.asList(input.split(","))));
      return null;
    }
  }

  private final boolean usingSharedResources;
  private final ClientResourcesImpl resources;
  private String defaultNamespace;

  public AccumuloClientImpl(final Optional<String> instanceName, final Optional<LinkedHashSet<String>> zooKeepers, final Object configuration, final User user,
      final Optional<ClientResourcesImpl> resources, final Optional<String> namespace) {
    super(getInstanceFromConfig(configuration, instanceName, zooKeepers),
        new Credentials(user.getPrincipal(), AuthenticationToken.class.cast(user.getToken())), (ClientConfiguration) configuration);
    usingSharedResources = resources.isPresent();
    this.resources = resources.orElse(new ClientResourcesImpl());
    this.defaultNamespace = namespace.orElse(Namespace.DEFAULT);
  }

  private static Instance getInstanceFromConfig(Object configuration, Optional<String> instanceName, Optional<LinkedHashSet<String>> zooKeepers) {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public void close() {
    if (!usingSharedResources) {
      resources.close();
    }
  }

  // TODO do we need this?
  Set<Table> tables() {
    return Tables.getNameToIdMap(getInstance()).entrySet().stream().map(input -> {
      try {
        if (input.getValue() != null) {
          return new TableImpl(AccumuloClientImpl.this, input.getKey(), input.getValue());
        }
        return null;
      } catch (TableNotFound e) {
        throw new AssertionError("Inconceivable", e);
      }
    }).filter(x -> x != null).collect(Collectors.toSet());
  }

  @Override
  public Set<Namespace> namespaces() {
    try {
      return getConnector().namespaceOperations().namespaceIdMap().entrySet().stream().map(input -> {
        try {
          if (input.getValue() != null) {
            return NamespaceImpl.getNamespace(AccumuloClientImpl.this, input.getKey(), Namespace.ID.of(input.getValue()));
          }
          return null;
        } catch (NamespaceNotFound e) {
          throw new AssertionError("Inconceivable", e);
        }
      }).filter(x -> x != null).collect(Collectors.toSet());
    } catch (org.apache.accumulo.core.client.AccumuloException | AccumuloSecurityException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
      throw new RuntimeException() {
        // TODO replace with a real exception
        private static final long serialVersionUID = 1L;
      };
    }
  }

  @Override
  public Namespace namespace(String name) throws NamespaceNotFound {
    return NamespaceImpl.getNamespace(this, name);
  }

  @Override
  public MultiTableWriter multiTableWriter(Object configuration) {
    return null;
  }

  @Override
  public Namespace.Builder namespaceBuilder(String name) {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public Namespace currentNamespace() {
    try {
      // TODO do this once, when constructed, and never do it again
      return namespace(defaultNamespace);
    } catch (NamespaceNotFound e) {
      // TODO should never happen; we should set the currentNamespace at the beginning
      e.printStackTrace();
      throw new IllegalStateException(e);
    }
  }

  @Override
  public void useNamespace(Namespace namespace) {
    // TODO Auto-generated method stub
    // use atomic reference
  }

}
