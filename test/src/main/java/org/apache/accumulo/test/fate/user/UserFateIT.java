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
package org.apache.accumulo.test.fate.user;

import static org.apache.accumulo.test.fate.user.UserFateStoreIT.createFateTable;

import java.util.stream.StreamSupport;

import org.apache.accumulo.core.client.Accumulo;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.clientImpl.ClientContext;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.fate.AbstractFateStore.FateIdGenerator;
import org.apache.accumulo.core.fate.FateId;
import org.apache.accumulo.core.fate.ReadOnlyFateStore.TStatus;
import org.apache.accumulo.core.fate.user.UserFateStore;
import org.apache.accumulo.core.fate.user.schema.FateSchema.TxColumnFamily;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.harness.SharedMiniClusterBase;
import org.apache.accumulo.server.ServerContext;
import org.apache.accumulo.test.fate.FateIT;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;

public class UserFateIT extends FateIT {

  private String table;

  @BeforeAll
  public static void setup() throws Exception {
    SharedMiniClusterBase.startMiniCluster();
  }

  @AfterAll
  public static void teardown() {
    SharedMiniClusterBase.stopMiniCluster();
  }

  @Override
  public void executeTest(FateTestExecutor<TestEnv> testMethod, int maxDeferred,
      FateIdGenerator fateIdGenerator) throws Exception {
    table = getUniqueNames(1)[0];
    try (ClientContext client =
        (ClientContext) Accumulo.newClient().from(getClientProps()).build()) {
      createFateTable(client, table);
      testMethod.execute(new UserFateStore<>(client, table, maxDeferred, fateIdGenerator),
          getCluster().getServerContext());
    }
  }

  @Override
  protected TStatus getTxStatus(ServerContext context, FateId fateId) {
    try (Scanner scanner = context.createScanner(table, Authorizations.EMPTY)) {
      scanner.setRange(getRow(fateId));
      TxColumnFamily.STATUS_COLUMN.fetch(scanner);
      return StreamSupport.stream(scanner.spliterator(), false)
          .map(e -> TStatus.valueOf(e.getValue().toString())).findFirst().orElse(TStatus.UNKNOWN);
    } catch (TableNotFoundException e) {
      throw new IllegalStateException(table + " not found!", e);
    }
  }

  private static Range getRow(FateId fateId) {
    return new Range(fateId.getTxUUIDStr());
  }
}
