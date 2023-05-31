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
package org.apache.accumulo.monitor.rest.tservers;

import static org.apache.accumulo.monitor.util.ParameterValidator.HOSTNAME_PORT_REGEX;

import java.lang.management.ManagementFactory;
import java.util.ArrayList;
import java.util.List;

import jakarta.inject.Inject;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Pattern;
import jakarta.ws.rs.Consumes;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.POST;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.PathParam;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.QueryParam;
import jakarta.ws.rs.core.MediaType;

import org.apache.accumulo.core.clientImpl.ClientContext;
import org.apache.accumulo.core.data.TableId;
import org.apache.accumulo.core.dataImpl.KeyExtent;
import org.apache.accumulo.core.manager.thrift.ManagerMonitorInfo;
import org.apache.accumulo.core.manager.thrift.RecoveryStatus;
import org.apache.accumulo.core.manager.thrift.TabletServerStatus;
import org.apache.accumulo.core.rpc.ThriftUtil;
import org.apache.accumulo.core.rpc.clients.ThriftClientTypes;
import org.apache.accumulo.core.tabletserver.thrift.ActionStats;
import org.apache.accumulo.core.tabletserver.thrift.TabletServerClientService;
import org.apache.accumulo.core.tabletserver.thrift.TabletStats;
import org.apache.accumulo.core.trace.TraceUtil;
import org.apache.accumulo.core.util.AddressUtil;
import org.apache.accumulo.monitor.Monitor;
import org.apache.accumulo.monitor.rest.manager.ManagerResource;
import org.apache.accumulo.server.manager.state.DeadServerList;
import org.apache.accumulo.server.util.ActionStatsUpdator;

import com.google.common.net.HostAndPort;

/**
 * Generates tserver lists as JSON objects
 *
 * @since 2.0.0
 */
@Path("/tservers")
@Produces({MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML})
public class TabletServerResource {

  @Inject
  private Monitor monitor;

  // Variable names become JSON keys
  private TabletStats total;
  private TabletStats historical;

  /**
   * Generates tserver summary
   *
   * @return tserver summary
   */
  @GET
  public TabletServers getTserverSummary() {
    ManagerMonitorInfo mmi = monitor.getMmi();
    if (mmi == null) {
      return new TabletServers();
    }

    TabletServers tserverInfo = new TabletServers(mmi.getTServerInfoSize());
    for (TabletServerStatus status : mmi.getTServerInfo()) {
      tserverInfo.addTablet(new TabletServer(monitor, status));
    }

    tserverInfo.addBadTabletServer(ManagerResource.getTables(monitor));

    return tserverInfo;
  }

  /**
   * REST call to clear dead servers from list
   *
   * @param server Dead server to clear
   */
  @POST
  @Consumes(MediaType.TEXT_PLAIN)
  public void clearDeadServer(
      @QueryParam("server") @NotNull @Pattern(regexp = HOSTNAME_PORT_REGEX) String server) {
    DeadServerList obit = new DeadServerList(monitor.getContext());
    obit.delete(server);
  }

  /**
   * Generates a recovery tserver list
   *
   * @return Recovery tserver list
   */
  @Path("recovery")
  @GET
  public TabletServersRecovery getTserverRecovery() {
    TabletServersRecovery recoveryList = new TabletServersRecovery();

    ManagerMonitorInfo mmi = monitor.getMmi();
    if (mmi == null) {
      return new TabletServersRecovery();
    }

    for (TabletServerStatus server : mmi.getTServerInfo()) {
      if (server.isSetLogSorts()) {
        for (RecoveryStatus recovery : server.getLogSorts()) {
          String serv = AddressUtil.parseAddress(server.getName(), false).getHost();
          String log = recovery.getName();
          int time = recovery.getRuntime();
          double progress = recovery.getProgress();

          recoveryList.addRecovery(new TabletServerRecoveryInformation(serv, log, time, progress));
        }
      }
    }

    return recoveryList;
  }

  /**
   * Generates details for the selected tserver
   *
   * @param tserverAddress TServer name
   * @return TServer details
   */
  @Path("{address}")
  @GET
  public TabletServerSummary getTserverDetails(
      @PathParam("address") @NotNull @Pattern(regexp = HOSTNAME_PORT_REGEX) String tserverAddress)
      throws Exception {
    ManagerMonitorInfo mmi = monitor.getMmi();
    if (mmi == null) {
      return new TabletServerSummary();
    }

    boolean tserverExists = false;
    for (TabletServerStatus ts : mmi.getTServerInfo()) {
      if (tserverAddress.equals(ts.getName())) {
        tserverExists = true;
        break;
      }
    }

    if (!tserverExists) {
      return null;
    }

    double splitStdDev = 0;
    double minorStdDev = 0;
    double minorQueueStdDev = 0;
    double majorStdDev = 0;
    double majorQueueStdDev = 0;
    double currentMinorAvg = 0;
    double currentMajorAvg = 0;
    double currentMinorStdDev = 0;
    double currentMajorStdDev = 0;
    total =
        new TabletStats(null, new ActionStats(), new ActionStats(), new ActionStats(), 0, 0, 0, 0);
    HostAndPort address = HostAndPort.fromString(tserverAddress);
    historical =
        new TabletStats(null, new ActionStats(), new ActionStats(), new ActionStats(), 0, 0, 0, 0);
    List<TabletStats> tsStats = new ArrayList<>();

    try {
      ClientContext context = monitor.getContext();
      TabletServerClientService.Client client =
          ThriftUtil.getClient(ThriftClientTypes.TABLET_SERVER, address, context);
      try {
        for (String tableId : mmi.getTableMap().keySet()) {
          tsStats.addAll(client.getTabletStats(TraceUtil.traceInfo(), context.rpcCreds(), tableId));
        }
        historical = client.getHistoricalStats(TraceUtil.traceInfo(), context.rpcCreds());
      } finally {
        ThriftUtil.returnClient(client, context);
      }
    } catch (Exception e) {
      return null;
    }

    List<CurrentOperations> currentOps = doCurrentOperations(tsStats);

    if (total.getMinors().getNum() != 0) {
      currentMinorAvg = (long) (total.getMinors().getElapsed() / total.getMinors().getNum());
    }
    if (total.getMinors().getElapsed() != 0 && total.getMinors().getNum() != 0) {
      currentMinorStdDev = stddev(total.getMinors().getElapsed(), total.getMinors().getNum(),
          total.getMinors().getSumDev());
    }
    if (total.getMajors().getNum() != 0) {
      currentMajorAvg = total.getMajors().getElapsed() / total.getMajors().getNum();
    }
    if (total.getMajors().getElapsed() != 0 && total.getMajors().getNum() != 0
        && total.getMajors().getElapsed() > total.getMajors().getNum()) {
      currentMajorStdDev = stddev(total.getMajors().getElapsed(), total.getMajors().getNum(),
          total.getMajors().getSumDev());
    }

    ActionStatsUpdator.update(total.getMinors(), historical.getMinors());
    ActionStatsUpdator.update(total.getMajors(), historical.getMajors());

    minorStdDev = stddev(total.getMinors().getElapsed(), total.getMinors().getNum(),
        total.getMinors().getSumDev());
    minorQueueStdDev = stddev(total.getMinors().getQueueTime(), total.getMinors().getNum(),
        total.getMinors().getQueueSumDev());
    majorStdDev = stddev(total.getMajors().getElapsed(), total.getMajors().getNum(),
        total.getMajors().getSumDev());
    majorQueueStdDev = stddev(total.getMajors().getQueueTime(), total.getMajors().getNum(),
        total.getMajors().getQueueSumDev());
    splitStdDev = stddev(historical.getSplits().getElapsed(), historical.getSplits().getNum(),
        historical.getSplits().getSumDev());

    TabletServerDetailInformation details = doDetails(tsStats.size());

    List<AllTimeTabletResults> allTime =
        doAllTimeResults(majorQueueStdDev, minorQueueStdDev, splitStdDev, majorStdDev, minorStdDev);

    CurrentTabletResults currentRes = doCurrentTabletResults(currentMinorAvg, currentMinorStdDev,
        currentMajorAvg, currentMajorStdDev);

    return new TabletServerSummary(details, allTime, currentRes, currentOps);
  }

  /**
   * Generates the server stats
   *
   * @return Server stat list
   */
  @Path("serverStats")
  @GET
  public ServerStats getServerStats() {

    final int concurrentScans = monitor.getContext().getConfiguration().getScanExecutors(false)
        .stream().mapToInt(sec -> sec.maxThreads).sum();

    ServerStats stats = new ServerStats();

    stats.addStats(
        new ServerStat(ManagementFactory.getOperatingSystemMXBean().getAvailableProcessors(), true,
            100, "OS Load", "osload"));
    stats.addStats(new ServerStat(1000, true, 1, "Ingest Entries", "ingest"));
    stats.addStats(new ServerStat(10000, true, 1, "Scan Entries", "query"));
    stats.addStats(new ServerStat(10, true, 10, "Ingest MB", "ingestMB"));
    stats.addStats(new ServerStat(5, true, 10, "Scan MB", "queryMB"));
    stats.addStats(new ServerStat(concurrentScans * 2, false, 1, "Running Scans", "scans"));
    stats.addStats(new ServerStat(50, true, 10, "Scan Sessions", "scansessions"));
    stats.addStats(new ServerStat(60000, false, 1, "Hold Time", "holdtime"));
    stats.addStats(new ServerStat(1, false, 100, "Overall Avg", true, "allavg"));
    stats.addStats(new ServerStat(1, false, 100, "Overall Max", true, "allmax"));

    return stats;
  }

  private TabletServerDetailInformation doDetails(int numTablets) {

    return new TabletServerDetailInformation(numTablets, total.getNumEntries(),
        total.getMinors().getStatus(), total.getMajors().getStatus(),
        historical.getSplits().getStatus());
  }

  private List<AllTimeTabletResults> doAllTimeResults(double majorQueueStdDev,
      double minorQueueStdDev, double splitStdDev, double majorStdDev, double minorStdDev) {

    List<AllTimeTabletResults> allTime = new ArrayList<>();

    // Minor Compaction Operation
    allTime.add(new AllTimeTabletResults("Minor&nbsp;Compaction", total.getMinors().getNum(),
        total.getMinors().getFail(),
        total.getMinors().getNum() != 0
            ? (total.getMinors().getQueueTime() / total.getMinors().getNum()) : null,
        minorQueueStdDev,
        total.getMinors().getNum() != 0
            ? (total.getMinors().getElapsed() / total.getMinors().getNum()) : null,
        minorStdDev, total.getMinors().getElapsed()));

    // Major Compaction Operation
    allTime.add(new AllTimeTabletResults("Major&nbsp;Compaction", total.getMajors().getNum(),
        total.getMajors().getFail(),
        total.getMajors().getNum() != 0
            ? (total.getMajors().getQueueTime() / total.getMajors().getNum()) : null,
        majorQueueStdDev,
        total.getMajors().getNum() != 0
            ? (total.getMajors().getElapsed() / total.getMajors().getNum()) : null,
        majorStdDev, total.getMajors().getElapsed()));
    // Split Operation
    allTime.add(new AllTimeTabletResults("Split", historical.getSplits().getNum(),
        historical.getSplits().getFail(), null, null,
        historical.getSplits().getNum() != 0
            ? (historical.getSplits().getElapsed() / historical.getSplits().getNum()) : null,
        splitStdDev, historical.getSplits().getElapsed()));

    return allTime;
  }

  private CurrentTabletResults doCurrentTabletResults(double currentMinorAvg,
      double currentMinorStdDev, double currentMajorAvg, double currentMajorStdDev) {

    return new CurrentTabletResults(currentMinorAvg, currentMinorStdDev, currentMajorAvg,
        currentMajorStdDev);
  }

  private List<CurrentOperations> doCurrentOperations(List<TabletStats> tsStats) throws Exception {

    List<CurrentOperations> currentOperations = new ArrayList<>();

    for (TabletStats info : tsStats) {
      if (!info.isSetExtent()) {
        historical = info;
        continue;
      }
      total.setNumEntries(total.getNumEntries() + info.getNumEntries());
      ActionStatsUpdator.update(total.getMinors(), info.getMinors());
      ActionStatsUpdator.update(total.getMajors(), info.getMajors());

      KeyExtent extent = KeyExtent.fromThrift(info.getExtent());
      TableId tableId = extent.tableId();
      String displayExtent = String.format("[%s]", extent.obscured());

      String tableName = monitor.getContext().getPrintableTableInfoFromId(tableId);

      currentOperations.add(new CurrentOperations(tableName, tableId, displayExtent,
          info.getNumEntries(), info.getIngestRate(), info.getQueryRate(),
          info.getMinors().getNum() != 0 ? info.getMinors().getElapsed() / info.getMinors().getNum()
              : null,
          stddev(info.getMinors().getElapsed(), info.getMinors().getNum(),
              info.getMinors().getSumDev()),
          info.getMinors().getElapsed() != 0
              ? info.getMinors().getCount() / info.getMinors().getElapsed() : null,
          info.getMajors().getNum() != 0 ? info.getMajors().getElapsed() / info.getMajors().getNum()
              : null,
          stddev(info.getMajors().getElapsed(), info.getMajors().getNum(),
              info.getMajors().getSumDev()),
          info.getMajors().getElapsed() != 0
              ? info.getMajors().getCount() / info.getMajors().getElapsed() : null));
    }

    return currentOperations;
  }

  private static double stddev(double elapsed, double num, double sumDev) {
    if (num != 0) {
      double average = elapsed / num;
      return Math.sqrt((sumDev / num) - (average * average));
    }
    return 0;
  }
}
