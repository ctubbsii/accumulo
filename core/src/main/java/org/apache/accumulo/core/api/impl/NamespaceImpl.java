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

import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.accumulo.api.client.NamespaceNotEmpty;
import org.apache.accumulo.api.client.NamespaceNotFound;
import org.apache.accumulo.api.client.TableNotFound;
import org.apache.accumulo.api.data.Namespace;
import org.apache.accumulo.api.data.Table;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.NamespaceNotEmptyException;
import org.apache.accumulo.core.client.NamespaceNotFoundException;
import org.apache.accumulo.core.client.impl.Namespaces;

/**
 *
 */
public class NamespaceImpl implements Namespace {

  public static Namespace getNamespace(AccumuloClientImpl client, String name) throws NamespaceNotFound {
    // TODO make this use WeakReferences, stored in client's resources
    return new NamespaceImpl(client, name);
  }

  public static Namespace getNamespace(AccumuloClientImpl client, String name, Namespace.ID id) throws NamespaceNotFound {
    return new NamespaceImpl(client, name, id);
  }

  private AccumuloClientImpl client;

  private ID id;

  private String name;

  private NamespaceImpl(final AccumuloClientImpl client, final String name) throws NamespaceNotFound {
    this(client, name, Namespaces.getNameToIdMap(client.getInstance()));
  }

  private NamespaceImpl(final AccumuloClientImpl client, final String name, final Map<String,Namespace.ID> nameToIdMap) throws NamespaceNotFound {
    this(client, name, nameToIdMap.get(name));
  }

  private NamespaceImpl(AccumuloClientImpl client, String name, Namespace.ID id) throws NamespaceNotFound {
    this.client = client;
    this.name = name;
    if (id == null)
      throw new NamespaceNotFound(this.name);
    this.id = id;
  }

  @Override
  public void delete() throws NamespaceNotFound, NamespaceNotEmpty {
    if (ID.ACCUMULO.equals(getId()) || ID.DEFAULT.equals(getId())) {
      throw new UnsupportedOperationException("Cannot delete built-in namespace identified as: " + getId());
    }
    try {
      client.getConnector().namespaceOperations().delete(getName());
    } catch (org.apache.accumulo.core.client.AccumuloException | AccumuloSecurityException e) {
      // TODO deal with security exceptions and IOExceptions
      throw new IllegalStateException("", e);
    } catch (NamespaceNotFoundException e) {
      throw new NamespaceNotFound(getId());
    } catch (NamespaceNotEmptyException e) {
      throw new NamespaceNotEmpty(getId());
    }
  }

  @Override
  public boolean equals(Object obj) {
    if (obj instanceof NamespaceImpl)
      return Objects.equals(getId(), NamespaceImpl.class.cast(obj).getId());
    return false;
  }

  @Override
  public boolean exists() {
    return Namespaces.exists(client.getInstance(), Namespace.ID.of(getId().canonicalID()));
  }

  @Override
  public ID getId() {
    return id;
  }

  @Override
  public String getName() {
    return name;
  }

  @Override
  public int hashCode() {
    return getId().hashCode();
  }

  @Override
  public boolean isDefaultNamespace() {
    return ID.DEFAULT.equals(getId());
  }

  @Override
  public boolean isSystemNamespace() {
    return ID.ACCUMULO.equals(getId());
  }

  @Override
  public boolean isUserNamespace() {
    return !isSystemNamespace();
  }

  @Override
  public Table.Builder tableBuilder(String name) {
    return new TableImpl.BuilderImpl(client, this, name);
  }

  @Override
  public void removeProperty(String key) {
    // TODO Auto-generated method stub

  }

  @Override
  public void rename(String newName) {
    // TODO Auto-generated method stub

  }

  @Override
  public void setProperty(String key, String value) {
    // TODO Auto-generated method stub

  }

  @Override
  public Table table(final String name) throws TableNotFound {
    Set<Table> matchingTables = tables().stream().filter(x -> x.getSimpleName().equals(name)).collect(Collectors.toSet());
    if (matchingTables.isEmpty()) {
      throw new TableNotFound(name);
    }
    if (matchingTables.size() != 1)
      throw new IllegalStateException("Found multiple tables whose names match " + name);
    return matchingTables.iterator().next();
  }

  @Override
  public Set<Table> tables() {
    return client.tables().stream().filter(t -> t.getNamespace().getId().canonicalID().equals(getId().canonicalID())).collect(Collectors.toSet());
  }
}
