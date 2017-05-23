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
package org.apache.accumulo.server.conf.blob;

import static java.nio.charset.StandardCharsets.UTF_8;

import java.util.Collections;
import java.util.Map;
import java.util.Optional;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.accumulo.fate.zookeeper.ZooCache;
import org.apache.commons.lang.RandomStringUtils;

import com.google.common.base.Objects;
import com.google.common.base.Objects.ToStringHelper;
import com.google.gson.Gson;

public class ConfBlob {

  private static final String CURRENT_VERSION = "1";

  private final String path;
  private final AtomicReference<HierarchicalSnapshotConfig> snapshotConf;

  public static class VersionedConfBlob {
    private static final Gson gson = new Gson();

    public String version;
    public Map<String,String> value;

    public VersionedConfBlob() {}

    public VersionedConfBlob(Map<String,String> value) {
      this.version = CURRENT_VERSION;
      this.value = value;
    }

    public String serialize() {
      return gson.toJson(this);
    }

    public static VersionedConfBlob fromJson(byte[] source) {
      try {
        return gson.fromJson(new String(source, UTF_8), VersionedConfBlob.class);
      } catch (RuntimeException e) {
        // this could happen, but let the caller handle it
        throw e;
      }
    }
  }

  public ConfBlob(final String path, final ZooCache zooCache) {
    this.path = path;
    this.snapshotConf = new AtomicReference<>(new HierarchicalSnapshotConfig(-1, Collections.emptyMap(), Optional.empty()));
  }

  public String getPath() {
    return path;
  }

  public SnapshotConfig getSnapshot() {
    return snapshotConf.get();
  }

  /**
   * Construct from a byte array source.
   *
   * @param source
   *          must be UTF-8 formatted bytes
   */
  void fromJSON(int seqId, byte[] source) {
    VersionedConfBlob blob = VersionedConfBlob.fromJson(source);
    if (!CURRENT_VERSION.equals(blob.version)) {
      throw new IllegalArgumentException("Unsupported configuration version " + blob.version + " for " + new String(source, UTF_8));
    }
    snapshotConf.set(new HierarchicalSnapshotConfig(seqId, blob.value, Optional.empty()));
  }

  @Override
  public String toString() {
    SnapshotConfig snapshot = snapshotConf.get();
    ToStringHelper s = Objects.toStringHelper(this).add("Path", getPath());
    s.add("SequenceId", snapshot.getSequenceId());
    s.add("Current", snapshot.toJSON());
    return s.toString();
  }

  public static void main(String[] args) {
    ConfBlob confBlob = new ConfBlob("/some/path", null);
    Map<String,String> map = new TreeMap<>();
    for (int i = 0; i < 5; ++i) {
      String k = RandomStringUtils.randomAlphabetic(5);
      String v = RandomStringUtils.randomAlphabetic(5);
      map.put(k, v);
    }
    confBlob.snapshotConf.set(new HierarchicalSnapshotConfig(-1, map, Optional.empty()));

    // System.out.print(confBlob.getSnapshot().size() + "\t\t\t\t\t");
    System.out.println(confBlob.getSnapshot());

    String str = confBlob.snapshotConf.get().toJSON();
    System.out.println("\t\t" + confBlob);

    confBlob.fromJSON(-1, str.getBytes(UTF_8));
    // System.out.print(confBlob.getSnapshot().size() + "\t\t\t\t\t");
    System.out.println(confBlob.getSnapshot());
  }

}
