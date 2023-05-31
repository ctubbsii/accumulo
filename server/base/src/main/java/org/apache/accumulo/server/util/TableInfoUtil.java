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

import java.util.HashMap;
import java.util.Map;

import org.apache.accumulo.core.manager.thrift.Compacting;
import org.apache.accumulo.core.manager.thrift.ManagerMonitorInfo;
import org.apache.accumulo.core.manager.thrift.TableInfo;
import org.apache.accumulo.core.manager.thrift.TabletServerStatus;

public class TableInfoUtil {

  public static void add(TableInfo total, TableInfo more) {
    if (!total.isSetMinors()) {
      total.setMinors(new Compacting());
    }
    if (!total.isSetMajors()) {
      total.setMajors(new Compacting());
    }
    if (!total.isSetScans()) {
      total.setScans(new Compacting());
    }

    if (more.isSetMinors()) {
      total.getMinors().setRunning(total.getMinors().getRunning() + more.getMinors().getRunning());
      total.getMinors().setQueued(total.getMinors().getQueued() + more.getMinors().getQueued());
    }
    if (more.isSetMajors()) {
      total.getMajors().setRunning(total.getMajors().getRunning() + more.getMajors().getRunning());
      total.getMajors().setQueued(total.getMajors().getQueued() + more.getMajors().getQueued());
    }
    if (more.isSetScans()) {
      total.getScans().setRunning(total.getScans().getRunning() + more.getScans().getRunning());
      total.getScans().setQueued(total.getScans().getQueued() + more.getScans().getQueued());
    }

    total.setOnlineTablets(total.getOnlineTablets() + more.getOnlineTablets());
    total.setRecs(total.getRecs() + more.getRecs());
    total.setRecsInMemory(total.getRecsInMemory() + more.getRecsInMemory());
    total.setTablets(total.getTablets() + more.getTablets());
    total.setIngestRate(total.getIngestRate() + more.getIngestRate());
    total.setIngestByteRate(total.getIngestByteRate() + more.getIngestByteRate());
    total.setQueryRate(total.getQueryRate() + more.getQueryRate());
    total.setQueryByteRate(total.getQueryByteRate() + more.getQueryByteRate());
    total.setScanRate(total.getScanRate() + more.getScanRate());
  }

  public static TableInfo summarizeTableStats(TabletServerStatus status) {
    TableInfo summary = new TableInfo();
    summary.setMajors(new Compacting());
    summary.setMinors(new Compacting());
    summary.setScans(new Compacting());
    for (TableInfo rates : status.getTableMap().values()) {
      TableInfoUtil.add(summary, rates);
    }
    return summary;
  }

  public static Map<String,Double> summarizeTableStats(ManagerMonitorInfo mmi) {
    Map<String,Double> compactingByTable = new HashMap<>();
    if (mmi != null && mmi.isSetTServerInfo()) {
      for (TabletServerStatus status : mmi.getTServerInfo()) {
        if (status != null && status.isSetTableMap()) {
          for (String table : status.getTableMap().keySet()) {
            Double holdTime = compactingByTable.get(table);
            compactingByTable.put(table,
                Math.max(holdTime == null ? 0. : holdTime, status.getHoldTime()));
          }
        }
      }
    }
    return compactingByTable;
  }

}
