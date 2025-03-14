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
package org.apache.accumulo.gc;

import static org.apache.accumulo.core.metadata.schema.TabletMetadata.ColumnType.LAST;
import static org.apache.accumulo.core.metadata.schema.TabletMetadata.ColumnType.LOGS;
import static org.apache.accumulo.core.metadata.schema.TabletMetadata.ColumnType.SUSPEND;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Stream;

import org.apache.accumulo.core.client.admin.TabletAvailability;
import org.apache.accumulo.core.conf.AccumuloConfiguration;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.dataImpl.KeyExtent;
import org.apache.accumulo.core.gc.thrift.GCStatus;
import org.apache.accumulo.core.gc.thrift.GcCycleStats;
import org.apache.accumulo.core.metadata.TServerInstance;
import org.apache.accumulo.core.metadata.schema.TabletMetadata;
import org.apache.accumulo.core.metadata.schema.TabletMetadata.Location;
import org.apache.accumulo.core.util.Pair;
import org.apache.accumulo.server.ServerContext;
import org.apache.accumulo.server.fs.VolumeManager;
import org.apache.accumulo.server.log.WalStateManager;
import org.apache.accumulo.server.log.WalStateManager.WalState;
import org.apache.accumulo.server.manager.LiveTServerSet;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.easymock.EasyMock;
import org.junit.jupiter.api.Test;

public class GarbageCollectWriteAheadLogsTest {

  private final TServerInstance server1 = new TServerInstance("localhost:1234[SESSION]");
  private final TServerInstance server2 = new TServerInstance("localhost:1234[OTHERSESS]");
  private final UUID id = UUID.randomUUID();
  private final Map<TServerInstance,List<UUID>> markers =
      Collections.singletonMap(server1, Collections.singletonList(id));
  private final Map<TServerInstance,List<UUID>> markers2 =
      Collections.singletonMap(server2, Collections.singletonList(id));
  private final Path path = new Path("hdfs://localhost:9000/accumulo/wal/localhost+1234/" + id);
  private final KeyExtent extent = KeyExtent.fromMetaRow(new Text("1<"));
  private final TabletMetadata tabletAssignedToServer1;
  private final TabletMetadata tabletAssignedToServer2;

  {
    try {
      tabletAssignedToServer1 =
          TabletMetadata.builder(extent).putLocation(Location.current(server1))
              .putTabletAvailability(TabletAvailability.HOSTED).build(LAST, SUSPEND, LOGS);
      tabletAssignedToServer2 =
          TabletMetadata.builder(extent).putLocation(Location.current(server2))
              .putTabletAvailability(TabletAvailability.UNHOSTED).build(LAST, SUSPEND, LOGS);
    } catch (Exception ex) {
      throw new RuntimeException(ex);
    }
  }

  private final Stream<TabletMetadata> tabletOnServer1List = Stream.of(tabletAssignedToServer1);
  private final Stream<TabletMetadata> tabletOnServer2List = Stream.of(tabletAssignedToServer2);

  @Test
  public void testRemoveUnusedLog() throws Exception {
    AccumuloConfiguration conf = EasyMock.createMock(AccumuloConfiguration.class);
    ServerContext context = EasyMock.createMock(ServerContext.class);
    VolumeManager fs = EasyMock.createMock(VolumeManager.class);
    EasyMock.expect(fs.moveToTrash(EasyMock.anyObject())).andReturn(false).anyTimes();
    WalStateManager marker = EasyMock.createMock(WalStateManager.class);
    LiveTServerSet tserverSet = EasyMock.createMock(LiveTServerSet.class);

    GCStatus status = new GCStatus(null, null, null, new GcCycleStats());

    EasyMock.expect(context.getConfiguration()).andReturn(conf).times(2);
    EasyMock.expect(conf.getCount(Property.GC_DELETE_WAL_THREADS)).andReturn(8).anyTimes();
    tserverSet.scanServers();
    EasyMock.expectLastCall();
    EasyMock.expect(tserverSet.getCurrentServers()).andReturn(Collections.singleton(server1));

    EasyMock.expect(marker.getAllMarkers()).andReturn(markers).once();
    EasyMock.expect(marker.state(server1, id)).andReturn(new Pair<>(WalState.UNREFERENCED, path));
    EasyMock.expect(fs.deleteRecursively(path)).andReturn(true).once();
    marker.removeWalMarker(server1, id);
    EasyMock.expectLastCall().once();
    EasyMock.replay(conf, context, fs, marker, tserverSet);
    var gc = new GarbageCollectWriteAheadLogs(context, fs, tserverSet, marker) {
      @Override
      protected Map<UUID,Path> getSortedWALogs() {
        return Collections.emptyMap();
      }

      @Override
      Stream<TabletMetadata> createStore(Set<TServerInstance> liveTservers) {
        return tabletOnServer1List;
      }
    };
    gc.collect(status);
    assertThrows(IllegalStateException.class, () -> gc.collect(status),
        "Should only be able to call collect once");

    EasyMock.verify(conf, context, fs, marker, tserverSet);
  }

  @Test
  public void testKeepClosedLog() throws Exception {
    AccumuloConfiguration conf = EasyMock.createMock(AccumuloConfiguration.class);
    ServerContext context = EasyMock.createMock(ServerContext.class);
    VolumeManager fs = EasyMock.createMock(VolumeManager.class);
    WalStateManager marker = EasyMock.createMock(WalStateManager.class);
    LiveTServerSet tserverSet = EasyMock.createMock(LiveTServerSet.class);

    GCStatus status = new GCStatus(null, null, null, new GcCycleStats());

    EasyMock.expect(context.getConfiguration()).andReturn(conf).times(2);
    EasyMock.expect(conf.getCount(Property.GC_DELETE_WAL_THREADS)).andReturn(8).anyTimes();
    tserverSet.scanServers();
    EasyMock.expectLastCall();
    EasyMock.expect(tserverSet.getCurrentServers()).andReturn(Collections.singleton(server1));

    EasyMock.expect(marker.getAllMarkers()).andReturn(markers).once();
    EasyMock.expect(marker.state(server1, id)).andReturn(new Pair<>(WalState.CLOSED, path));
    EasyMock.replay(conf, context, marker, tserverSet, fs);
    var gc = new GarbageCollectWriteAheadLogs(context, fs, tserverSet, marker) {

      @Override
      protected Map<UUID,Path> getSortedWALogs() {
        return Collections.emptyMap();
      }

      @Override
      Stream<TabletMetadata> createStore(Set<TServerInstance> liveTservers) {
        return tabletOnServer1List;
      }
    };
    gc.collect(status);
    EasyMock.verify(conf, context, marker, tserverSet, fs);
  }

  @Test
  public void deleteUnreferencedLogOnDeadServer() throws Exception {
    AccumuloConfiguration conf = EasyMock.createMock(AccumuloConfiguration.class);
    ServerContext context = EasyMock.createMock(ServerContext.class);
    VolumeManager fs = EasyMock.createMock(VolumeManager.class);
    EasyMock.expect(fs.moveToTrash(EasyMock.anyObject())).andReturn(false).anyTimes();
    WalStateManager marker = EasyMock.createMock(WalStateManager.class);
    LiveTServerSet tserverSet = EasyMock.createMock(LiveTServerSet.class);

    GCStatus status = new GCStatus(null, null, null, new GcCycleStats());

    EasyMock.expect(context.getConfiguration()).andReturn(conf).times(2);
    EasyMock.expect(conf.getCount(Property.GC_DELETE_WAL_THREADS)).andReturn(8).anyTimes();
    tserverSet.scanServers();
    EasyMock.expectLastCall();
    EasyMock.expect(tserverSet.getCurrentServers()).andReturn(Collections.singleton(server1));

    EasyMock.expect(marker.getAllMarkers()).andReturn(markers2).once();
    EasyMock.expect(marker.state(server2, id)).andReturn(new Pair<>(WalState.OPEN, path));

    EasyMock.expect(fs.deleteRecursively(path)).andReturn(true).once();
    marker.removeWalMarker(server2, id);
    EasyMock.expectLastCall().once();
    marker.forget(server2);
    EasyMock.expectLastCall().once();
    EasyMock.replay(conf, context, fs, marker, tserverSet);
    var gc = new GarbageCollectWriteAheadLogs(context, fs, tserverSet, marker) {
      @Override
      protected Map<UUID,Path> getSortedWALogs() {
        return Collections.emptyMap();
      }

      @Override
      Stream<TabletMetadata> createStore(Set<TServerInstance> liveTservers) {
        return tabletOnServer1List;
      }
    };
    gc.collect(status);
    EasyMock.verify(conf, context, fs, marker, tserverSet);
  }

  @Test
  public void ignoreReferenceLogOnDeadServer() throws Exception {
    AccumuloConfiguration conf = EasyMock.createMock(AccumuloConfiguration.class);
    ServerContext context = EasyMock.createMock(ServerContext.class);
    VolumeManager fs = EasyMock.createMock(VolumeManager.class);
    WalStateManager marker = EasyMock.createMock(WalStateManager.class);
    LiveTServerSet tserverSet = EasyMock.createMock(LiveTServerSet.class);

    GCStatus status = new GCStatus(null, null, null, new GcCycleStats());

    EasyMock.expect(context.getConfiguration()).andReturn(conf).times(2);
    EasyMock.expect(conf.getCount(Property.GC_DELETE_WAL_THREADS)).andReturn(8).anyTimes();
    tserverSet.scanServers();
    EasyMock.expectLastCall();
    EasyMock.expect(tserverSet.getCurrentServers()).andReturn(Collections.singleton(server1));

    EasyMock.expect(marker.getAllMarkers()).andReturn(markers2).once();
    EasyMock.expect(marker.state(server2, id)).andReturn(new Pair<>(WalState.OPEN, path));

    EasyMock.replay(conf, context, fs, marker, tserverSet);
    GarbageCollectWriteAheadLogs gc =
        new GarbageCollectWriteAheadLogs(context, fs, tserverSet, marker) {
          @Override
          protected Map<UUID,Path> getSortedWALogs() {
            return Collections.emptyMap();
          }

          @Override
          Stream<TabletMetadata> createStore(Set<TServerInstance> liveTservers) {
            return tabletOnServer2List;
          }
        };
    gc.collect(status);

    EasyMock.verify(conf, context, fs, marker, tserverSet);
  }

}
