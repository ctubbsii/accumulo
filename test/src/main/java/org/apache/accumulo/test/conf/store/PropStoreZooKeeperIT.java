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
package org.apache.accumulo.test.conf.store;

import static org.apache.accumulo.harness.AccumuloITBase.ZOOKEEPER_TESTING_SERVER;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.File;
import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.accumulo.core.Constants;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.data.TableId;
import org.apache.accumulo.core.fate.zookeeper.ZooUtil;
import org.apache.accumulo.core.zookeeper.ZooSession;
import org.apache.accumulo.core.zookeeper.ZooSession.ZKUtil;
import org.apache.accumulo.server.conf.codec.VersionedPropCodec;
import org.apache.accumulo.server.conf.codec.VersionedProperties;
import org.apache.accumulo.server.conf.store.PropChangeListener;
import org.apache.accumulo.server.conf.store.PropStore;
import org.apache.accumulo.server.conf.store.PropStoreKey;
import org.apache.accumulo.server.conf.store.TablePropKey;
import org.apache.accumulo.server.conf.store.impl.ZooPropStore;
import org.apache.accumulo.test.zookeeper.ZooKeeperTestingServer;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.data.Stat;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Tag(ZOOKEEPER_TESTING_SERVER)
public class PropStoreZooKeeperIT {

  private static final Logger log = LoggerFactory.getLogger(PropStoreZooKeeperIT.class);
  private static final VersionedPropCodec propCodec = VersionedPropCodec.getDefault();
  private static ZooKeeperTestingServer testZk = null;
  private static ZooSession zk;
  private PropStore propStore = null;
  private final TableId tIdA = TableId.of("A");
  private final TableId tIdB = TableId.of("B");

  @TempDir
  private static File tempDir;

  @BeforeAll
  public static void setupZk() throws Exception {
    testZk = new ZooKeeperTestingServer(tempDir);
    // prop store uses a chrooted ZK, so it is relocatable, but create a convenient empty node to
    // work in for the test, so we can easily clean it up after each test
    try (var zkInit = testZk.newClient()) {
      zkInit.create("/instanceRoot", null, ZooUtil.PUBLIC, CreateMode.PERSISTENT);
    }
    // create a chrooted client for the tests to use
    zk = testZk.newClient("/instanceRoot");
  }

  @AfterAll
  public static void shutdownZK() throws Exception {
    try {
      zk.close();
    } finally {
      testZk.close();
    }
  }

  @BeforeEach
  public void setupZnodes() throws Exception {
    zk.asReaderWriter().mkdirs(Constants.ZCONFIG);
    zk.create(Constants.ZTABLES, new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
    zk.create(Constants.ZTABLES + "/" + tIdA.canonical(), new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE,
        CreateMode.PERSISTENT);
    zk.create(Constants.ZTABLES + "/" + tIdA.canonical() + "/conf", new byte[0],
        ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);

    zk.create(Constants.ZTABLES + "/" + tIdB.canonical(), new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE,
        CreateMode.PERSISTENT);
    zk.create(Constants.ZTABLES + "/" + tIdB.canonical() + "/conf", new byte[0],
        ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
    propStore = ZooPropStore.initialize(zk);
  }

  @AfterEach
  public void cleanupZnodes() throws Exception {
    for (var child : zk.getChildren("/", null)) {
      ZKUtil.deleteRecursive(zk, "/" + child);
    }
  }

  /**
   * Verify that when a config node does not exist, null is returned instead of an exception.
   */
  @Test
  public void createNoProps() throws InterruptedException, KeeperException {
    var propKey = TablePropKey.of(tIdA);

    // read from ZK, after delete no node and node not created.
    assertNull(zk.exists(propKey.getPath(), null));
    assertThrows(IllegalStateException.class, () -> propStore.get(propKey));
  }

  @Test
  public void failOnDuplicate() throws InterruptedException, KeeperException {
    var propKey = TablePropKey.of(tIdA);

    assertNull(zk.exists(propKey.getPath(), null)); // check node does not exist in ZK

    propStore.create(propKey, Map.of());
    Thread.sleep(25); // yield.

    assertNotNull(zk.exists(propKey.getPath(), null)); // check not created
    assertThrows(IllegalStateException.class, () -> propStore.create(propKey, null));

    assertNotNull(propStore.get(propKey));
  }

  @Test
  public void createWithProps() throws InterruptedException, KeeperException, IOException {
    var propKey = TablePropKey.of(tIdA);
    Map<String,String> initialProps = new HashMap<>();
    initialProps.put(Property.TABLE_BLOOM_ENABLED.getKey(), "true");
    propStore.create(propKey, initialProps);

    VersionedProperties vProps = propStore.get(propKey);
    assertNotNull(vProps);
    assertEquals("true", vProps.asMap().get(Property.TABLE_BLOOM_ENABLED.getKey()));

    // check using direct read from ZK
    byte[] bytes = zk.getData(propKey.getPath(), null, new Stat());
    var readFromZk = propCodec.fromBytes(0, bytes);
    var propsA = propStore.get(propKey);
    assertEquals(readFromZk.asMap(), propsA.asMap());
  }

  @Test
  public void update() throws InterruptedException {
    TestChangeListener listener = new TestChangeListener();

    var propKey = TablePropKey.of(tIdA);
    propStore.registerAsListener(propKey, listener);

    Map<String,String> initialProps = new HashMap<>();
    initialProps.put(Property.TABLE_BLOOM_ENABLED.getKey(), "true");
    propStore.create(propKey, initialProps);

    var props1 = propStore.get(propKey);

    assertEquals("true", props1.asMap().get(Property.TABLE_BLOOM_ENABLED.getKey()));

    long version0 = props1.getDataVersion();

    Map<String,String> updateProps = new HashMap<>();
    updateProps.put(Property.TABLE_BLOOM_ENABLED.getKey(), "false");
    updateProps.put(Property.TABLE_MAJC_RATIO.getKey(), "5");

    log.trace("calling update()");

    propStore.putAll(propKey, updateProps);

    // allow change notification to propagate
    Thread.sleep(150);

    log.trace("calling get()");

    var props2 = propStore.get(propKey);
    // validate version changed on write.
    long version1 = props2.getDataVersion();

    log.trace("V0: {}, V1: {}", version0, version1);

    assertTrue(version0 < version1);

    assertNotNull(propStore.get(propKey));
    assertEquals(2, props2.asMap().size());
    assertEquals("false", props2.asMap().get(Property.TABLE_BLOOM_ENABLED.getKey()));
    assertEquals("5", props2.asMap().get(Property.TABLE_MAJC_RATIO.getKey()));

    propStore.removeProperties(propKey,
        Collections.singletonList(Property.TABLE_MAJC_RATIO.getKey()));
    Thread.sleep(150);
    // validate version changed on write

    var props3 = propStore.get(propKey);
    log.trace("current props: {}", props3.print(true));

    long version2 = props3.getDataVersion();
    log.trace("versions created by test: v0: {}, v1: {}, v2: {}", version0, version1, version2);

    assertTrue(version0 < version2);
    assertTrue(version1 < version2);

    // allow change to propagate
    Thread.sleep(150);

    var props4 = propStore.get(propKey);
    assertEquals(1, props4.asMap().size());
    assertEquals("false", props4.asMap().get(Property.TABLE_BLOOM_ENABLED.getKey()));
    assertNull(props4.asMap().get(Property.TABLE_MAJC_RATIO.getKey()));

    log.trace("changed count: {}", listener.changeCounts);

    assertEquals(2, (int) listener.getChangeCounts().get(propKey));
    assertNull(listener.getDeleteCounts().get(propKey));

  }

  @Test
  public void deleteTest() {
    var tableAPropKey = TablePropKey.of(tIdA);
    var tableBPropKey = TablePropKey.of(tIdB);

    Map<String,String> initialProps = new HashMap<>();
    initialProps.put(Property.TABLE_BLOOM_ENABLED.getKey(), "true");
    propStore.create(tableAPropKey, initialProps);
    propStore.create(tableBPropKey, initialProps);

    assertNotNull(propStore.get(tableAPropKey));
    assertNotNull(propStore.get(tableBPropKey));

    var props1 = propStore.get(tableAPropKey);
    assertEquals("true", props1.asMap().get(Property.TABLE_BLOOM_ENABLED.getKey()));
  }

  /**
   * Delete a node and validate delete is propogated via ZooKeeper watcher. Uses multiple caches
   * that should only be coordinating via ZooKeeper events. When a node is deleted, the ZooKeeper
   * node deleted event should also clear the node from all caches.
   *
   * @throws InterruptedException Any exception is a test failure.
   */
  @Test
  public void deleteThroughWatcher() throws InterruptedException {
    TestChangeListener listener = new TestChangeListener();

    var tableAPropKey = TablePropKey.of(tIdA);
    var tableBPropKey = TablePropKey.of(tIdB);

    propStore.registerAsListener(tableAPropKey, listener);
    propStore.registerAsListener(tableBPropKey, listener);

    Map<String,String> initialProps = new HashMap<>();
    initialProps.put(Property.TABLE_BLOOM_ENABLED.getKey(), "true");
    propStore.create(tableAPropKey, initialProps);
    propStore.create(tableBPropKey, initialProps);

    var propsA = propStore.get(tableAPropKey);

    assertEquals("true", propsA.asMap().get(Property.TABLE_BLOOM_ENABLED.getKey()));

    // use alternate prop store - change will propagate via ZooKeeper
    PropStore propStore2 = ZooPropStore.initialize(zk);

    propStore2.delete(tableAPropKey);

    log.trace("After delete on 2nd store for table: {}", tableAPropKey);

    Thread.sleep(150);

    // no node should not be created, should throw an exception
    assertThrows(IllegalStateException.class, () -> propStore.get(tableAPropKey));
    assertNotNull(propStore.get(tableBPropKey));

    // validate change count not triggered
    assertNull(listener.getChangeCounts().get(tableAPropKey));
    assertNull(listener.getChangeCounts().get(tableBPropKey));

    // validate delete only for table A
    assertEquals(1, (int) listener.getDeleteCounts().get(tableAPropKey));
    assertNull(listener.getChangeCounts().get(tableAPropKey));
  }

  /**
   * Simulate change in props by process external to the prop store instance.
   */
  @Test
  public void externalChange() throws IOException, InterruptedException, KeeperException {

    TestChangeListener listener = new TestChangeListener();

    var tableAPropKey = TablePropKey.of(tIdA);
    var tableBPropKey = TablePropKey.of(tIdB);

    propStore.registerAsListener(tableAPropKey, listener);
    propStore.registerAsListener(tableBPropKey, listener);

    Map<String,String> initialProps = new HashMap<>();
    initialProps.put(Property.TABLE_BLOOM_ENABLED.getKey(), "true");
    propStore.create(tableAPropKey, initialProps);
    propStore.create(tableBPropKey, initialProps);

    assertNotNull(propStore.get(tableAPropKey));
    assertNotNull(propStore.get(tableBPropKey));

    VersionedProperties firstRead = propStore.get(tableAPropKey);
    assertEquals("true", firstRead.asMap().get(Property.TABLE_BLOOM_ENABLED.getKey()));

    // This assumes default is resolved at a higher level
    assertNull(firstRead.asMap().get(Property.TABLE_BLOOM_SIZE.getKey()));

    Map<String,String> update = new HashMap<>();
    var bloomSize = "1_000_000";
    update.put(Property.TABLE_BLOOM_SIZE.getKey(), bloomSize);
    VersionedProperties pendingProps = firstRead.addOrUpdate(update);

    log.debug("Writing props to trigger change notification {}", pendingProps.print(true));

    byte[] updatedBytes = propCodec.toBytes(pendingProps);
    // force external write to ZooKeeper
    zk.asReaderWriter().overwritePersistentData(tableAPropKey.getPath(), updatedBytes,
        (int) firstRead.getDataVersion());

    Thread.sleep(150);

    log.trace("Test - waiting for event");

    VersionedProperties updateRead = propStore.get(tableAPropKey);
    log.trace("Re-read: {}", updateRead.print(true));

    // original values
    assertNull(firstRead.asMap().get(Property.TABLE_BLOOM_SIZE.getKey()));

    log.trace("Updated: {}", updateRead.print(true));
    // values after update
    assertNotNull(updateRead.asMap().get(Property.TABLE_BLOOM_SIZE.getKey()));
    assertEquals(bloomSize, updateRead.asMap().get(Property.TABLE_BLOOM_SIZE.getKey()));

    log.trace("Prop changes {}", listener.getChangeCounts());
    log.trace("Prop deletes {}", listener.getDeleteCounts());

  }

  private static class TestChangeListener implements PropChangeListener {

    private final Map<PropStoreKey,Integer> changeCounts = new ConcurrentHashMap<>();
    private final Map<PropStoreKey,Integer> deleteCounts = new ConcurrentHashMap<>();

    @Override
    public void zkChangeEvent(PropStoreKey propStoreKey) {
      changeCounts.merge(propStoreKey, 1, Integer::sum);
    }

    @Override
    public void cacheChangeEvent(PropStoreKey propStoreKey) {
      changeCounts.merge(propStoreKey, 1, Integer::sum);
    }

    @Override
    public void deleteEvent(PropStoreKey propStoreKey) {
      deleteCounts.merge(propStoreKey, 1, Integer::sum);
    }

    @Override
    public void connectionEvent() {

    }

    public Map<PropStoreKey,Integer> getChangeCounts() {
      return changeCounts;
    }

    public Map<PropStoreKey,Integer> getDeleteCounts() {
      return deleteCounts;
    }
  }
}
