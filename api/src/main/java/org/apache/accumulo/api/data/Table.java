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
package org.apache.accumulo.api.data;

import java.nio.file.Path;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;

import org.apache.accumulo.api.client.BadConfiguration;
import org.apache.accumulo.api.client.TableExists;
import org.apache.accumulo.api.client.io.ScanConfiguration;
import org.apache.accumulo.api.client.io.TableScanner;
import org.apache.accumulo.api.client.io.TableWriter;
import org.apache.accumulo.api.client.io.WriterConfig;
import org.apache.accumulo.api.plugins.CompactionStrategy;
import org.apache.accumulo.api.plugins.TableConstraint;
import org.apache.accumulo.api.security.Authorizations;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;

public interface Table {

  /**
   * A builder for constructing an Accumulo {@link Table}.
   */
  public interface Builder {

    /**
     * Build the {@link Table} object from the options previously provided.
     *
     * @return an instance of {@link Table}, configured from the current state of this builder
     * @throws BadConfiguration
     *           if the provided configuration is malformed or insufficient to build a table
     */
    Table build() throws BadConfiguration, TableExists;

    Table.Builder fromBackup(Path directory);

    Table.Builder usingTimeType(TimeType time);

    Table.Builder withAdditionalProperties(Map<String,String> properties);

    Table.Builder withConstraint(TableConstraint constraint);

    Table.Builder withIterator(IteratorSetting iterator);

    Table.Builder withIterator(IteratorSetting iterator, int priority);

    Table.Builder withoutDefaultIterators();

  }

  public class ID extends AbstractId {
    private static final long serialVersionUID = 1L;

    static final Cache<String,ID> cache = CacheBuilder.newBuilder().weakValues().build();

    public static final ID METADATA = of("!0");
    public static final ID REPLICATION = of("+rep");
    public static final ID ROOT = of("+r");

    private ID(final String canonical) {
      super(canonical);
    }

    /**
     * Get a Table.ID object for the provided canonical string.
     *
     * @param canonical
     *          table ID string
     * @return Table.ID object
     */
    public static ID of(final String canonical) {
      try {
        return cache.get(canonical, () -> new ID(canonical));
      } catch (ExecutionException e) {
        throw new AssertionError("This should never happen: ID constructor should never return null.");
      }
    }
  }

  void addSplits(Set<Bytes> splits);

  void compact(CompactionStrategy strategy);

  void delete(Range range);

  boolean exists();

  void exportBackup(Path directory);

  void flush();

  ID getId();

  Bytes getMaxRow(Range range, Authorizations authorizations);

  String getName();

  Namespace getNamespace();

  String getSimpleName();

  Set<Bytes> getSplits();

  boolean isSystemTable();

  boolean isUserTable();

  void merge(Range range);

  TableScanner newScanner(ScanConfiguration scanConfiguration);

  TableWriter newWriter(WriterConfig config);
}
