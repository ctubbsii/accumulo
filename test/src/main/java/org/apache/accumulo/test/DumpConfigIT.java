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
package org.apache.accumulo.test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.File;
import java.nio.file.Files;
import java.time.Duration;
import java.util.Collections;

import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.metadata.SystemTables;
import org.apache.accumulo.miniclusterImpl.MiniAccumuloConfigImpl;
import org.apache.accumulo.server.util.Admin;
import org.apache.accumulo.test.functional.ConfigurableMacBase;
import org.apache.accumulo.test.functional.FunctionalTestUtils;
import org.apache.hadoop.conf.Configuration;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

public class DumpConfigIT extends ConfigurableMacBase {

  @TempDir
  private static File tempDir;

  @Override
  protected Duration defaultTimeout() {
    return Duration.ofMinutes(2);
  }

  @Override
  public void configure(MiniAccumuloConfigImpl cfg, Configuration hadoopCoreSite) {
    cfg.setSiteConfig(Collections.singletonMap(Property.TABLE_FILE_BLOCK_SIZE.getKey(), "1234567"));
  }

  @SuppressFBWarnings(value = "PATH_TRAVERSAL_IN",
      justification = "user.dir is suitable test input")
  @Test
  public void test() throws Exception {
    File folder = tempDir.toPath().resolve(testName() + "/").toFile();
    assertTrue(folder.isDirectory() || folder.mkdir(), "failed to create dir: " + folder);
    File siteFileBackup = folder.toPath().resolve("accumulo.properties.bak").toFile();
    assertFalse(siteFileBackup.exists());
    assertEquals(0, exec(Admin.class, "dumpConfig", "-a", "-d", folder.getPath()).waitFor());
    assertTrue(siteFileBackup.exists());
    String site = FunctionalTestUtils.readAll(Files.newInputStream(siteFileBackup.toPath()));
    assertTrue(site.contains(Property.TABLE_FILE_BLOCK_SIZE.getKey()));
    assertTrue(site.contains("1234567"));
    String meta = FunctionalTestUtils.readAll(
        Files.newInputStream(folder.toPath().resolve(SystemTables.METADATA.tableName() + ".cfg")));
    assertTrue(meta.contains(Property.TABLE_FILE_REPLICATION.getKey()));
    String systemPerm =
        FunctionalTestUtils.readAll(Files.newInputStream(folder.toPath().resolve("root_user.cfg")));
    assertTrue(systemPerm.contains("grant System.ALTER_USER -s -u root"));
    assertTrue(systemPerm
        .contains("grant Table.READ -t " + SystemTables.METADATA.tableName() + " -u root"));
    assertFalse(systemPerm
        .contains("grant Table.DROP -t " + SystemTables.METADATA.tableName() + " -u root"));
  }
}
