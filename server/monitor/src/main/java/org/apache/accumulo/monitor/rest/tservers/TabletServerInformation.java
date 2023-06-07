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

import java.util.ArrayList;
import java.util.List;

import jakarta.xml.bind.annotation.XmlAttribute;

import org.apache.accumulo.core.manager.thrift.RecoveryStatus;
import org.apache.accumulo.core.manager.thrift.TableInfo;
import org.apache.accumulo.core.manager.thrift.TabletServerStatus;
import org.apache.accumulo.monitor.Monitor;
import org.apache.accumulo.monitor.rest.tables.CompactionsList;
import org.apache.accumulo.monitor.rest.tables.CompactionsTypes;
import org.apache.accumulo.monitor.rest.trace.RecoveryStatusInformation;
import org.apache.accumulo.server.util.TableInfoUtil;

/**
 * Generates tserver information
 *
 * @since 2.0.0
 */
public class TabletServerInformation {

  // Variable names become JSON keys
  @XmlAttribute(name = "id")
  public String server;

  public String hostname;
  public long lastContact;
  public long responseTime;
  public double osload;
  public String version;

  public CompactionsTypes compactions;

  public int tablets;
  public double ingest;
  public double query;
  public double ingestMB;
  public double queryMB;
  public Integer scans; // For backwards compatibility, has same information as scansRunning
  public Double scansessions;
  public Double scanssessions; // For backwards compatibility
  public long holdtime;

  // New variables

  public String ip;
  private Integer scansRunning;
  private Integer scansQueued;
  // combo string with running value and number queued in parenthesis
  public String minorCombo;
  public String majorCombo;
  public String scansCombo;
  private Integer minorRunning;
  private Integer minorQueued;

  private Integer majorRunning;
  private Integer majorQueued;

  private CompactionsList scansCompacting; // if scans is removed, change scansCompacting to scans
  private CompactionsList major;
  private CompactionsList minor;
  public long entries;
  public long lookups;
  public long indexCacheHits;
  public long indexCacheRequests;
  public long dataCacheHits;
  public long dataCacheRequests;
  public double indexCacheHitRate;
  public double dataCacheHitRate;
  public List<RecoveryStatusInformation> logRecoveries;

  public TabletServerInformation() {}

  /**
   * Generate tserver information from thrift status
   *
   * @param thriftStatus Thrift status to obtain information
   */
  public TabletServerInformation(Monitor monitor, TabletServerStatus thriftStatus) {
    TableInfo summary = TableInfoUtil.summarizeTableStats(thriftStatus);
    updateTabletServerInfo(monitor, thriftStatus, summary);
  }

  /**
   * Generate tserver information from thrift status and table summary
   *
   * @param thriftStatus Thrift status to obtain information
   * @param summary Table info summary
   */
  public void updateTabletServerInfo(Monitor monitor, TabletServerStatus thriftStatus,
      TableInfo summary) {

    long now = System.currentTimeMillis();

    this.server = this.ip = this.hostname = thriftStatus.getName();
    this.tablets = summary.getTablets();
    this.lastContact = now - thriftStatus.getLastContact();
    this.responseTime = thriftStatus.getResponseTime();
    this.entries = summary.getRecs();
    this.ingest = cleanNumber(summary.getIngestRate());
    this.query = cleanNumber(summary.getQueryRate());

    this.holdtime = thriftStatus.getHoldTime();

    this.scansRunning = summary.isSetScans() ? summary.getScans().getRunning() : 0;
    this.scansQueued = summary.isSetScans() ? summary.getScans().getQueued() : 0;
    this.scansCombo = scansRunning + "(" + scansQueued + ")";

    this.scans = this.scansRunning;

    this.scansCompacting = new CompactionsList(this.scansRunning, this.scansQueued);

    this.minorRunning = summary.isSetMinors() ? summary.getMinors().getRunning() : 0;
    this.minorQueued = summary.isSetMinors() ? summary.getMinors().getQueued() : 0;
    this.minorCombo = minorRunning + "(" + minorQueued + ")";

    this.minor = new CompactionsList(this.minorRunning, this.minorQueued);

    this.majorRunning = summary.isSetMajors() ? summary.getMajors().getRunning() : 0;
    this.majorQueued = summary.isSetMajors() ? summary.getMajors().getQueued() : 0;
    this.majorCombo = majorRunning + "(" + majorQueued + ")";

    this.major = new CompactionsList(this.majorRunning, this.majorQueued);

    this.compactions = new CompactionsTypes(scansCompacting, major, minor);

    this.osload = thriftStatus.getOsLoad();
    this.version = thriftStatus.getVersion();
    this.lookups = thriftStatus.getLookups();

    this.dataCacheHits = thriftStatus.getDataCacheHits();
    this.dataCacheRequests = thriftStatus.getDataCacheRequest();
    this.indexCacheHits = thriftStatus.getIndexCacheHits();
    this.indexCacheRequests = thriftStatus.getIndexCacheRequest();

    this.indexCacheHitRate = this.indexCacheHits / (double) Math.max(this.indexCacheRequests, 1);
    this.dataCacheHitRate = this.dataCacheHits / (double) Math.max(this.dataCacheRequests, 1);

    this.ingestMB = cleanNumber(summary.getIngestByteRate());
    this.queryMB = cleanNumber(summary.getQueryByteRate());

    this.scansessions = monitor.getLookupRate();
    this.scanssessions = this.scansessions; // For backwards compatibility

    this.logRecoveries = new ArrayList<>(thriftStatus.getLogSortsSize());
    for (RecoveryStatus recovery : thriftStatus.getLogSorts()) {
      logRecoveries.add(new RecoveryStatusInformation(recovery));
    }
  }

  /**
   * Return zero for fractions. Partial numbers don't make sense in metrics.
   */
  private double cleanNumber(double dirtyNumber) {
    return dirtyNumber < 1 ? 0 : dirtyNumber;
  }
}
