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

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Map.Entry;

import org.apache.accumulo.core.conf.SiteConfiguration;
import org.apache.accumulo.core.manager.thrift.BulkImportStatus;
import org.apache.accumulo.core.manager.thrift.DeadServer;
import org.apache.accumulo.core.manager.thrift.ManagerMonitorInfo;
import org.apache.accumulo.core.manager.thrift.RecoveryStatus;
import org.apache.accumulo.core.manager.thrift.TableInfo;
import org.apache.accumulo.core.manager.thrift.TabletServerStatus;
import org.apache.accumulo.core.rpc.clients.ThriftClientTypes;
import org.apache.accumulo.core.trace.TraceUtil;
import org.apache.accumulo.server.ServerContext;
import org.apache.accumulo.server.util.TableInfoUtil;

public class GetManagerStats {
  public static void main(String[] args) throws Exception {
    ManagerMonitorInfo stats = null;
    var context = new ServerContext(SiteConfiguration.auto());
    stats = ThriftClientTypes.MANAGER.execute(context,
        client -> client.getManagerStats(TraceUtil.traceInfo(), context.rpcCreds()));
    out(0, "State: " + stats.getState().name());
    out(0, "Goal State: " + stats.getGoalState().name());
    if (stats.isSetServersShuttingDown() && !stats.getServersShuttingDown().isEmpty()) {
      out(0, "Servers to shutdown");
      for (String server : stats.getServersShuttingDown()) {
        out(1, "%s", server);
      }
    }
    out(0, "Unassigned tablets: %d", stats.getUnassignedTablets());
    if (stats.isSetBadTServers() && !stats.getBadTServers().isEmpty()) {
      out(0, "Bad servers");

      for (Entry<String,Byte> entry : stats.getBadTServers().entrySet()) {
        out(1, "%s: %d", entry.getKey(), (int) entry.getValue());
      }
    }
    out(0, "Dead tablet servers count: %s", stats.getDeadTabletServersSize());
    for (DeadServer dead : stats.getDeadTabletServers()) {
      out(1, "Dead tablet server: %s", dead.getServer());
      out(2, "Last report: %s", new SimpleDateFormat().format(new Date(dead.getLastStatus())));
      out(2, "Cause: %s", dead.getStatus());
    }
    out(0, "Bulk imports: %s", stats.getBulkImportsSize());
    for (BulkImportStatus bulk : stats.getBulkImports()) {
      out(1, "Import directory: %s", bulk.getFilename());
      out(2, "Bulk state %s", bulk.getState());
      out(2, "Bulk start %s", bulk.getStartTime());
    }
    if (stats.isSetTableMap() && !stats.getTableMap().isEmpty()) {
      out(0, "Tables");
      for (Entry<String,TableInfo> entry : stats.getTableMap().entrySet()) {
        TableInfo v = entry.getValue();
        out(1, "%s", entry.getKey());
        out(2, "Records: %d", v.getRecs());
        out(2, "Records in Memory: %d", v.getRecsInMemory());
        out(2, "Tablets: %d", v.getTablets());
        out(2, "Online Tablets: %d", v.getOnlineTablets());
        out(2, "Ingest Rate: %.2f", v.getIngestRate());
        out(2, "Query Rate: %.2f", v.getQueryRate());
      }
    }
    if (stats.isSetTServerInfo() && !stats.getTServerInfo().isEmpty()) {
      out(0, "Tablet Servers");
      long now = System.currentTimeMillis();
      for (TabletServerStatus server : stats.getTServerInfo()) {
        TableInfo summary = TableInfoUtil.summarizeTableStats(server);
        out(1, "Name: %s", server.getName());
        out(2, "Ingest: %.2f", summary.getIngestRate());
        out(2, "Last Contact: %s", server.getLastContact());
        out(2, "OS Load Average: %.2f", server.getOsLoad());
        out(2, "Queries: %.2f", summary.getQueryRate());
        out(2, "Time Difference: %.1f", ((now - server.getLastContact()) / 1000.));
        out(2, "Total Records: %d", summary.getRecs());
        out(2, "Lookups: %d", server.getLookups());
        if (server.getHoldTime() > 0) {
          out(2, "Hold Time: %d", server.getHoldTime());
        }
        if (server.isSetTableMap() && !server.getTableMap().isEmpty()) {
          out(2, "Tables");
          for (Entry<String,TableInfo> status : server.getTableMap().entrySet()) {
            TableInfo info = status.getValue();
            out(3, "Table: %s", status.getKey());
            out(4, "Tablets: %d", info.getOnlineTablets());
            out(4, "Records: %d", info.getRecs());
            out(4, "Records in Memory: %d", info.getRecsInMemory());
            out(4, "Ingest: %.2f", info.getIngestRate());
            out(4, "Queries: %.2f", info.getQueryRate());
            out(4, "Major Compacting: %d", info.isSetMajors() ? info.getMajors().getRunning() : 0);
            out(4, "Queued for Major Compaction: %d",
                info.isSetMajors() ? info.getMajors().getQueued() : 0);
            out(4, "Minor Compacting: %d", info.isSetMinors() ? info.getMinors().getRunning() : 0);
            out(4, "Queued for Minor Compaction: %d",
                info.isSetMinors() ? info.getMinors().getQueued() : 0);
          }
        }
        out(2, "Recoveries: %d", server.getLogSortsSize());
        for (RecoveryStatus sort : server.getLogSorts()) {
          out(3, "File: %s", sort.getName());
          out(3, "Progress: %.2f%%", sort.getProgress() * 100);
          out(3, "Time running: %s", sort.getRuntime() / 1000.);
        }
        out(3, "Bulk imports: %s", stats.getBulkImportsSize());
        for (BulkImportStatus bulk : stats.getBulkImports()) {
          out(4, "Import file: %s", bulk.getFilename());
          out(5, "Bulk state %s", bulk.getState());
          out(5, "Bulk start %s", bulk.getStartTime());
        }

      }
    }
  }

  private static void out(int indent, String fmtString, Object... args) {
    for (int i = 0; i < indent; i++) {
      System.out.print(" ");
    }
    System.out.printf(fmtString + "%n", args);
  }

}
