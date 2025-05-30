/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.accumulo.server.util;

import static com.google.common.util.concurrent.Uninterruptibles.sleepUninterruptibly;
import static org.apache.accumulo.core.metadata.schema.TabletMetadata.ColumnType.AVAILABILITY;
import static org.apache.accumulo.core.metadata.schema.TabletMetadata.ColumnType.CLONED;
import static org.apache.accumulo.core.metadata.schema.TabletMetadata.ColumnType.DIR;
import static org.apache.accumulo.core.metadata.schema.TabletMetadata.ColumnType.FILES;
import static org.apache.accumulo.core.metadata.schema.TabletMetadata.ColumnType.LAST;
import static org.apache.accumulo.core.metadata.schema.TabletMetadata.ColumnType.LOCATION;
import static org.apache.accumulo.core.metadata.schema.TabletMetadata.ColumnType.LOGS;
import static org.apache.accumulo.core.metadata.schema.TabletMetadata.ColumnType.PREV_ROW;
import static org.apache.accumulo.core.metadata.schema.TabletMetadata.ColumnType.TIME;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.concurrent.TimeUnit;

import org.apache.accumulo.core.Constants;
import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.BatchWriterConfig;
import org.apache.accumulo.core.client.MutationsRejectedException;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.clientImpl.BatchWriterImpl;
import org.apache.accumulo.core.clientImpl.ScannerImpl;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.TableId;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.dataImpl.KeyExtent;
import org.apache.accumulo.core.gc.ReferenceFile;
import org.apache.accumulo.core.lock.ServiceLock;
import org.apache.accumulo.core.metadata.StoredTabletFile;
import org.apache.accumulo.core.metadata.SystemTables;
import org.apache.accumulo.core.metadata.schema.Ample;
import org.apache.accumulo.core.metadata.schema.DataFileValue;
import org.apache.accumulo.core.metadata.schema.MetadataSchema.TabletsSection;
import org.apache.accumulo.core.metadata.schema.MetadataSchema.TabletsSection.ClonedColumnFamily;
import org.apache.accumulo.core.metadata.schema.MetadataSchema.TabletsSection.CurrentLocationColumnFamily;
import org.apache.accumulo.core.metadata.schema.MetadataSchema.TabletsSection.DataFileColumnFamily;
import org.apache.accumulo.core.metadata.schema.MetadataSchema.TabletsSection.LastLocationColumnFamily;
import org.apache.accumulo.core.metadata.schema.MetadataSchema.TabletsSection.ServerColumnFamily;
import org.apache.accumulo.core.metadata.schema.TabletDeletedException;
import org.apache.accumulo.core.metadata.schema.TabletMetadata;
import org.apache.accumulo.core.metadata.schema.TabletsMetadata;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.tabletserver.log.LogEntry;
import org.apache.accumulo.core.util.FastFormat;
import org.apache.accumulo.core.util.Pair;
import org.apache.accumulo.server.ServerContext;
import org.apache.accumulo.server.gc.AllVolumesDirectory;
import org.apache.hadoop.io.Text;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.annotations.VisibleForTesting;

/**
 * provides a reference to the metadata table for updates by tablet servers
 */
public class MetadataTableUtil {

  public static final Text EMPTY_TEXT = new Text();
  private static final Logger log = LoggerFactory.getLogger(MetadataTableUtil.class);

  private MetadataTableUtil() {}

  public static void putLockID(ServerContext context, ServiceLock zooLock, Mutation m) {
    ServerColumnFamily.LOCK_COLUMN.put(m, new Value(zooLock.getLockID().serialize()));
  }

  public static void deleteTable(TableId tableId, boolean insertDeletes, ServerContext context,
      ServiceLock lock) throws AccumuloException {
    try (
        Scanner ms =
            new ScannerImpl(context, SystemTables.METADATA.tableId(), Authorizations.EMPTY);
        BatchWriter bw = new BatchWriterImpl(context, SystemTables.METADATA.tableId(),
            new BatchWriterConfig().setMaxMemory(1000000)
                .setMaxLatency(120000L, TimeUnit.MILLISECONDS).setMaxWriteThreads(2))) {

      // scan metadata for our table and delete everything we find
      Mutation m = null;
      Ample ample = context.getAmple();
      ms.setRange(new KeyExtent(tableId, null, null).toMetaRange());

      // insert deletes before deleting data from metadata... this makes the code fault tolerant
      if (insertDeletes) {

        ms.fetchColumnFamily(DataFileColumnFamily.NAME);
        ServerColumnFamily.DIRECTORY_COLUMN.fetch(ms);

        for (Entry<Key,Value> cell : ms) {
          Key key = cell.getKey();

          if (key.getColumnFamily().equals(DataFileColumnFamily.NAME)) {
            StoredTabletFile stf = new StoredTabletFile(key.getColumnQualifierData().toString());
            bw.addMutation(ample.createDeleteMutation(ReferenceFile.forFile(tableId, stf)));
          }

          if (ServerColumnFamily.DIRECTORY_COLUMN.hasColumns(key)) {
            var uri = new AllVolumesDirectory(tableId, cell.getValue().toString());
            bw.addMutation(ample.createDeleteMutation(uri));
          }
        }

        bw.flush();

        ms.clearColumns();
      }

      for (Entry<Key,Value> cell : ms) {
        Key key = cell.getKey();

        if (m == null) {
          m = new Mutation(key.getRow());
          if (lock != null) {
            putLockID(context, lock, m);
          }
        }

        if (key.getRow().compareTo(m.getRow(), 0, m.getRow().length) != 0) {
          bw.addMutation(m);
          m = new Mutation(key.getRow());
          if (lock != null) {
            putLockID(context, lock, m);
          }
        }
        m.putDelete(key.getColumnFamily(), key.getColumnQualifier());
      }

      if (m != null) {
        bw.addMutation(m);
      }
    }
  }

  public static Pair<List<LogEntry>,SortedMap<StoredTabletFile,DataFileValue>>
      getFileAndLogEntries(ServerContext context, KeyExtent extent) throws IOException {
    ArrayList<LogEntry> result = new ArrayList<>();
    TreeMap<StoredTabletFile,DataFileValue> sizes = new TreeMap<>();

    TabletMetadata tablet = context.getAmple().readTablet(extent, FILES, LOGS, PREV_ROW, DIR);

    if (tablet == null) {
      throw new IllegalStateException("Tablet " + extent + " not found in metadata");
    }

    result.addAll(tablet.getLogs());

    tablet.getFilesMap().forEach(sizes::put);

    return new Pair<>(result, sizes);
  }

  private static Mutation createCloneMutation(TableId srcTableId, TableId tableId,
      Iterable<Entry<Key,Value>> tablet) {

    KeyExtent ke = KeyExtent.fromMetaRow(tablet.iterator().next().getKey().getRow());
    Mutation m = new Mutation(TabletsSection.encodeRow(tableId, ke.endRow()));

    for (Entry<Key,Value> entry : tablet) {
      if (entry.getKey().getColumnFamily().equals(DataFileColumnFamily.NAME)) {
        String cf = entry.getKey().getColumnQualifier().toString();
        if (!cf.startsWith("../") && !cf.contains(":")) {
          cf = "../" + srcTableId + entry.getKey().getColumnQualifier();
        }
        m.put(entry.getKey().getColumnFamily(), new Text(cf), entry.getValue());
      } else if (entry.getKey().getColumnFamily().equals(CurrentLocationColumnFamily.NAME)) {
        m.put(LastLocationColumnFamily.NAME, entry.getKey().getColumnQualifier(), entry.getValue());
      } else if (entry.getKey().getColumnFamily().equals(LastLocationColumnFamily.NAME)) {
        // skip
      } else {
        m.put(entry.getKey().getColumnFamily(), entry.getKey().getColumnQualifier(),
            entry.getValue());
      }
    }
    return m;
  }

  private static TabletsMetadata createCloneScanner(String testTableName, TableId tableId,
      AccumuloClient client) {

    String tableName;
    Range range;

    if (testTableName != null) {
      tableName = testTableName;
      range = TabletsSection.getRange(tableId);
    } else if (tableId.equals(SystemTables.METADATA.tableId())) {
      tableName = SystemTables.ROOT.tableName();
      range = TabletsSection.getRange();
    } else {
      tableName = SystemTables.METADATA.tableName();
      range = TabletsSection.getRange(tableId);
    }

    return TabletsMetadata.builder(client).scanTable(tableName).overRange(range).checkConsistency()
        .saveKeyValues().fetch(FILES, LOCATION, LAST, CLONED, PREV_ROW, TIME, AVAILABILITY).build();
  }

  @VisibleForTesting
  public static void initializeClone(String testTableName, TableId srcTableId, TableId tableId,
      AccumuloClient client, BatchWriter bw) throws MutationsRejectedException {

    try (TabletsMetadata cloneScanner = createCloneScanner(testTableName, srcTableId, client)) {
      Iterator<TabletMetadata> ti = cloneScanner.iterator();

      if (!ti.hasNext()) {
        throw new IllegalStateException(" table deleted during clone?  srcTableId = " + srcTableId);
      }

      while (ti.hasNext()) {
        bw.addMutation(createCloneMutation(srcTableId, tableId, ti.next().getKeyValues()));
      }
    }

    bw.flush();
  }

  private static int compareEndRows(Text endRow1, Text endRow2) {
    return new KeyExtent(TableId.of("0"), endRow1, null)
        .compareTo(new KeyExtent(TableId.of("0"), endRow2, null));
  }

  @VisibleForTesting
  public static int checkClone(String testTableName, TableId srcTableId, TableId tableId,
      AccumuloClient client, BatchWriter bw)
      throws TableNotFoundException, MutationsRejectedException {

    int rewrites = 0;

    try (TabletsMetadata srcTM = createCloneScanner(testTableName, srcTableId, client);
        TabletsMetadata cloneTM = createCloneScanner(testTableName, tableId, client)) {
      Iterator<TabletMetadata> srcIter = srcTM.iterator();
      Iterator<TabletMetadata> cloneIter = cloneTM.iterator();

      if (!cloneIter.hasNext() || !srcIter.hasNext()) {
        throw new IllegalStateException(
            " table deleted during clone?  srcTableId = " + srcTableId + " tableId=" + tableId);
      }

      while (cloneIter.hasNext()) {
        TabletMetadata cloneTablet = cloneIter.next();
        Text cloneEndRow = cloneTablet.getEndRow();
        HashSet<StoredTabletFile> cloneFiles = new HashSet<>();

        boolean cloneSuccessful = cloneTablet.getCloned() != null;

        if (!cloneSuccessful) {
          cloneFiles.addAll(cloneTablet.getFiles());
        }

        List<TabletMetadata> srcTablets = new ArrayList<>();
        TabletMetadata srcTablet = srcIter.next();
        srcTablets.add(srcTablet);

        Text srcEndRow = srcTablet.getEndRow();
        int cmp = compareEndRows(cloneEndRow, srcEndRow);
        if (cmp < 0) {
          throw new TabletDeletedException(
              "Tablets deleted from src during clone : " + cloneEndRow + " " + srcEndRow);
        }

        HashSet<StoredTabletFile> srcFiles = new HashSet<>();
        if (!cloneSuccessful) {
          srcFiles.addAll(srcTablet.getFiles());
        }

        while (cmp > 0) {
          srcTablet = srcIter.next();
          srcTablets.add(srcTablet);
          srcEndRow = srcTablet.getEndRow();
          cmp = compareEndRows(cloneEndRow, srcEndRow);
          if (cmp < 0) {
            throw new TabletDeletedException(
                "Tablets deleted from src during clone : " + cloneEndRow + " " + srcEndRow);
          }

          if (!cloneSuccessful) {
            srcFiles.addAll(srcTablet.getFiles());
          }
        }

        if (cloneSuccessful) {
          continue;
        }

        if (srcFiles.containsAll(cloneFiles)) {
          // write out marker that this tablet was successfully cloned
          Mutation m = new Mutation(cloneTablet.getExtent().toMetaRow());
          m.put(ClonedColumnFamily.NAME, new Text(""), new Value("OK"));
          bw.addMutation(m);
        } else {
          // delete existing cloned tablet entry
          Mutation m = new Mutation(cloneTablet.getExtent().toMetaRow());

          for (Entry<Key,Value> entry : cloneTablet.getKeyValues()) {
            Key k = entry.getKey();
            m.putDelete(k.getColumnFamily(), k.getColumnQualifier(), k.getTimestamp());
          }

          bw.addMutation(m);

          for (TabletMetadata st : srcTablets) {
            bw.addMutation(createCloneMutation(srcTableId, tableId, st.getKeyValues()));
          }

          rewrites++;
        }
      }
    }

    bw.flush();
    return rewrites;

  }

  public static void cloneTable(ServerContext context, TableId srcTableId, TableId tableId)
      throws Exception {

    try (BatchWriter bw = context.createBatchWriter(SystemTables.METADATA.tableName())) {

      while (true) {

        try {
          initializeClone(null, srcTableId, tableId, context, bw);

          // the following loop looks changes in the file that occurred during the copy.. if files
          // were dereferenced then they could have been GCed

          while (true) {
            int rewrites = checkClone(null, srcTableId, tableId, context, bw);

            if (rewrites == 0) {
              break;
            }
          }

          bw.flush();
          break;

        } catch (TabletDeletedException tde) {
          // tablets were merged in the src table
          bw.flush();

          // delete what we have cloned and try again
          deleteTable(tableId, false, context, null);

          log.debug("Tablets merged in table {} while attempting to clone, trying again",
              srcTableId);

          sleepUninterruptibly(100, TimeUnit.MILLISECONDS);
        }
      }

      // delete the clone markers and create directory entries
      Scanner mscanner =
          context.createScanner(SystemTables.METADATA.tableName(), Authorizations.EMPTY);
      mscanner.setRange(new KeyExtent(tableId, null, null).toMetaRange());
      mscanner.fetchColumnFamily(ClonedColumnFamily.NAME);

      int dirCount = 0;

      for (Entry<Key,Value> entry : mscanner) {
        Key k = entry.getKey();
        Mutation m = new Mutation(k.getRow());
        m.putDelete(k.getColumnFamily(), k.getColumnQualifier());
        byte[] dirName =
            FastFormat.toZeroPaddedString(dirCount++, 8, 16, Constants.CLONE_PREFIX_BYTES);
        ServerColumnFamily.DIRECTORY_COLUMN.put(m, new Value(dirName));

        bw.addMutation(m);
      }
    }
  }
}
