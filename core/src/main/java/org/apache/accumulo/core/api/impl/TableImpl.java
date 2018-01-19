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

import java.nio.file.Path;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.stream.Collectors;

import org.apache.accumulo.api.client.NamespaceNotFound;
import org.apache.accumulo.api.client.TableExists;
import org.apache.accumulo.api.client.TableNotFound;
import org.apache.accumulo.api.client.io.ScanConfiguration;
import org.apache.accumulo.api.client.io.TableScanner;
import org.apache.accumulo.api.client.io.TableWriter;
import org.apache.accumulo.api.client.io.WriterConfig;
import org.apache.accumulo.api.data.Bytes;
import org.apache.accumulo.api.data.IteratorSetting;
import org.apache.accumulo.api.data.Namespace;
import org.apache.accumulo.api.data.Range;
import org.apache.accumulo.api.data.Table;
import org.apache.accumulo.api.data.TimeType;
import org.apache.accumulo.api.plugins.CompactionStrategy;
import org.apache.accumulo.api.plugins.TableConstraint;
import org.apache.accumulo.api.security.Authorizations;
import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.TableExistsException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.admin.NewTableConfiguration;
import org.apache.accumulo.core.client.impl.Tables;
import org.apache.hadoop.io.Text;

/**
 *
 */
public class TableImpl implements Table {

  private AccumuloClientImpl client;
  private String name;
  private ID id;
  private Namespace namespace;

  public static class BuilderImpl implements Table.Builder {

    private AccumuloClientImpl client;
    private String name;
    private Namespace namespace;
    private TimeType time = TimeType.MILLIS;
    private boolean useDefaultIterators = true;

    public BuilderImpl(AccumuloClientImpl client, Namespace namespace, String name) {
      this.client = client;
      this.namespace = namespace;
      this.name = name;
    }

    @Override
    public Table build() throws TableExists {
      // TODO create a proper table
      try {
        // TODO ensure table name is unqualified first
        String namespacePrefix = namespace.getName();
        namespacePrefix = namespacePrefix + (namespacePrefix.isEmpty() ? "" : ".");
        NewTableConfiguration ntc = new NewTableConfiguration().setTimeType(org.apache.accumulo.core.client.admin.TimeType.valueOf(time.name()));
        ntc = useDefaultIterators ? ntc : ntc.withoutDefaultIterators();
        client.getConnector().tableOperations().create(namespacePrefix + name, ntc);
        return new TableImpl(client, name);
      } catch (TableNotFound | AccumuloException | AccumuloSecurityException | TableExistsException e) {
        // TODO Auto-generated catch block
        e.printStackTrace();
        throw new AssertionError("Inconceivable", e);
      }
    }

    @Override
    public Builder withConstraint(TableConstraint constraint) {
      // TODO Auto-generated method stub
      return this;
    }

    @Override
    public Builder withIterator(IteratorSetting iterator) {
      // TODO Auto-generated method stub
      return this;
    }

    @Override
    public Builder withIterator(IteratorSetting iterator, int priority) {
      // TODO Auto-generated method stub
      return this;
    }

    @Override
    public Builder withoutDefaultIterators() {
      this.useDefaultIterators = false;
      return this;
    }

    @Override
    public Builder fromBackup(Path directory) {
      // TODO Auto-generated method stub
      return this;
    }

    @Override
    public Builder usingTimeType(TimeType time) {
      this.time = time;
      return this;
    }

    @Override
    public Builder withAdditionalProperties(Map<String,String> properties) {
      // TODO Auto-generated method stub
      return null;
    }

  }

  public TableImpl(final AccumuloClientImpl client, final String name) throws TableNotFound {
    this(client, name, Tables.getNameToIdMap(client.getInstance()));
  }

  public TableImpl(final AccumuloClientImpl client, final String name, final Map<String,Table.ID> nameToIdMap) throws TableNotFound {
    this(client, name, nameToIdMap.get(name));
  }

  public TableImpl(final AccumuloClientImpl client, final String name, final Table.ID id) throws TableNotFound {
    this.client = client;
    this.name = name;
    if (id == null)
      throw new TableNotFound(this.name);
    String tmpId = id.canonicalID();
    if (ID.ROOT.canonicalID().equals(tmpId)) {
      this.id = ID.ROOT;
    } else if (ID.METADATA.canonicalID().equals(tmpId)) {
      this.id = ID.METADATA;
    } else if (ID.REPLICATION.canonicalID().equals(tmpId)) {
      this.id = ID.REPLICATION;
    } else {
      this.id = ID.of(tmpId);
    }
    try {
      this.namespace = NamespaceImpl.getNamespace(client, name.substring(0, Math.max(0, name.lastIndexOf("."))));
    } catch (NamespaceNotFound e) {
      // if the namespace no longer exists, the table can't possibly, either, since the namespace has to be empty of all tables in order to be deleted; resolve
      // this race condition gracefully by just reporting this table not existing
      throw new TableNotFound(name);
    }
  }

  @Override
  public void addSplits(Set<Bytes> splits) {
    try {
      client.getConnector().tableOperations().addSplits(name, splits.stream().map(x -> new Text(x.getBytes())).collect(Collectors.toCollection(TreeSet::new)));
    } catch (TableNotFoundException | AccumuloException | AccumuloSecurityException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }
  }

  @Override
  public void compact(CompactionStrategy strategy) {
    // TODO Auto-generated method stub
  }

  @Override
  public void delete(Range range) {
    // TODO Auto-generated method stub
  }

  @Override
  public boolean exists() {
    return Tables.getIdToNameMap(client.getInstance()).containsKey(getId());
  }

  @Override
  public void exportBackup(Path directory) {
    // TODO Auto-generated method stub

  }

  @Override
  public void flush() {
    // TODO Auto-generated method stub
  }

  @Override
  public ID getId() {
    return id;
  }

  @Override
  public Bytes getMaxRow(Range range, Authorizations authorizations) {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public String getName() {
    try {
      return Tables.getTableName(client.getInstance(), getId());
    } catch (TableNotFoundException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
      return name;
    }
  }

  @Override
  public Set<Bytes> getSplits() {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public void merge(Range range) {
    // TODO Auto-generated method stub
  }

  @Override
  public boolean isSystemTable() {
    return getNamespace().isSystemNamespace();
  }

  @Override
  public boolean isUserTable() {
    return !isSystemTable();
  }

  @Override
  public Namespace getNamespace() {
    return namespace;
  }

  @Override
  public String getSimpleName() {
    return name.substring(Math.max(0, name.lastIndexOf(".")), name.length());
  }

  @Override
  public String toString() {
    return com.google.common.base.Objects.toStringHelper(Table.class).add("name", name).add("id", id.canonicalID()).toString();
  }

  @Override
  public TableScanner newScanner(ScanConfiguration scanConfiguration) {
    return new TableScannerImpl(client, scanConfiguration);
  }

  @Override
  public TableWriter newWriter(WriterConfig config) {
    return new TableWriterImpl(client, config);
  }

}
