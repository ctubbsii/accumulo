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
package org.apache.accumulo.manager.upgrade;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.apache.accumulo.manager.upgrade.Upgrader12to13.ZTABLE_NAME;
import static org.easymock.EasyMock.aryEq;
import static org.easymock.EasyMock.createMock;
import static org.easymock.EasyMock.createStrictMock;
import static org.easymock.EasyMock.eq;
import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.expectLastCall;
import static org.easymock.EasyMock.replay;
import static org.easymock.EasyMock.verify;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.apache.accumulo.core.Constants;
import org.apache.accumulo.core.client.NamespaceNotFoundException;
import org.apache.accumulo.core.clientImpl.NamespaceMapping;
import org.apache.accumulo.core.data.InstanceId;
import org.apache.accumulo.core.fate.zookeeper.ZooReader;
import org.apache.accumulo.core.fate.zookeeper.ZooReaderWriter;
import org.apache.accumulo.core.fate.zookeeper.ZooUtil;
import org.apache.accumulo.core.fate.zookeeper.ZooUtil.NodeMissingPolicy;
import org.apache.accumulo.core.zookeeper.ZooSession;
import org.apache.accumulo.server.ServerContext;
import org.apache.zookeeper.KeeperException;
import org.junit.jupiter.api.Test;

public class Upgrader12to13Test {

  @Test
  public void testZooKeeperUpgradeFailsServerCheck()
      throws InterruptedException, KeeperException, NamespaceNotFoundException {
    Upgrader12to13 upgrader = new Upgrader12to13();
    InstanceId iid = InstanceId.of(UUID.randomUUID());

    ServerContext context = createMock(ServerContext.class);
    ZooSession zk = createStrictMock(ZooSession.class);
    ZooReader zr = createStrictMock(ZooReader.class);
    ZooReaderWriter zrw = createStrictMock(ZooReaderWriter.class);

    expect(context.getInstanceID()).andReturn(iid).anyTimes();
    expect(context.getZooSession()).andReturn(zk).anyTimes();
    expect(zk.asReader()).andReturn(zr).anyTimes();
    expect(zk.asReaderWriter()).andReturn(zrw).anyTimes();

    expect(zr.getChildren(Constants.ZCOMPACTORS)).andReturn(List.of());
    expect(zr.getChildren(Constants.ZSSERVERS)).andReturn(List.of("localhost:9996"));
    expect(zr.getChildren(Constants.ZSSERVERS + "/localhost:9996")).andReturn(List.of());
    zrw.recursiveDelete(Constants.ZSSERVERS + "/localhost:9996", NodeMissingPolicy.SKIP);
    expect(zr.getChildren(Constants.ZTSERVERS)).andReturn(List.of("localhost:9997"));
    expect(zr.getChildren(Constants.ZTSERVERS + "/localhost:9997"))
        .andReturn(List.of(UUID.randomUUID().toString()));

    replay(context, zk, zr, zrw);
    IllegalStateException e =
        assertThrows(IllegalStateException.class, () -> upgrader.upgradeZookeeper(context));
    assertTrue(e.getMessage()
        .contains("Was expecting either a nothing, a resource group name or an empty directory"));
    verify(context, zk, zr, zrw);

  }

  @Test
  public void testAddingTableMappingToZooKeeper() throws InterruptedException, KeeperException {
    Upgrader12to13 upgrader = new Upgrader12to13();
    InstanceId iid = InstanceId.of(UUID.randomUUID());

    ServerContext context = createMock(ServerContext.class);
    ZooSession zk = createStrictMock(ZooSession.class);
    ZooReaderWriter zrw = createStrictMock(ZooReaderWriter.class);

    expect(context.getInstanceID()).andReturn(iid).anyTimes();
    expect(context.getZooSession()).andReturn(zk).anyTimes();
    expect(zk.asReaderWriter()).andReturn(zrw).anyTimes();

    Map<String,String> mockTables = Map.of("t1Id", "t1", "t2Id", "t2", "t3Id", "t3");
    List<String> mockTableIds = List.copyOf(mockTables.keySet());
    Map<String,String> mockTableToNamespace =
        Map.of("t1Id", "ns1Id", "t2Id", "ns1Id", "t3Id", "ns2Id");

    expect(zrw.getChildren(eq(Constants.ZTABLES))).andReturn(mockTableIds).once();
    for (String tableId : mockTableIds) {
      expect(zrw.getData(Constants.ZTABLES + "/" + tableId + ZTABLE_NAME))
          .andReturn(mockTables.get(tableId).getBytes(UTF_8)).once();
      expect(zrw.getData(Constants.ZTABLES + "/" + tableId + Constants.ZTABLE_NAMESPACE))
          .andReturn(mockTableToNamespace.get(tableId).getBytes(UTF_8)).once();
    }

    Map<String,Map<String,String>> expectedNamespaceMaps = new HashMap<>();
    for (Map.Entry<String,String> entry : mockTables.entrySet()) {
      String tableId = entry.getKey();
      expectedNamespaceMaps.computeIfAbsent(mockTableToNamespace.get(tableId), k -> new HashMap<>())
          .put(tableId, entry.getValue());
    }

    for (Map.Entry<String,Map<String,String>> entry : expectedNamespaceMaps.entrySet()) {
      expect(zrw.putPersistentData(
          eq(Constants.ZNAMESPACES + "/" + entry.getKey() + Constants.ZTABLES),
          aryEq(NamespaceMapping.serializeMap(entry.getValue())),
          eq(ZooUtil.NodeExistsPolicy.FAIL))).andReturn(true).once();
    }

    for (String table : mockTables.keySet()) {
      zrw.delete(Constants.ZTABLES + "/" + table + ZTABLE_NAME);
      expectLastCall().once();
    }

    replay(context, zk, zrw);
    upgrader.addTableMappingsToZooKeeper(context);
    verify(context, zk, zrw);
  }
}
