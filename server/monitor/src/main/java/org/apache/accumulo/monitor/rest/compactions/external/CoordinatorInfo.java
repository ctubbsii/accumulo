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
package org.apache.accumulo.monitor.rest.compactions.external;

import java.util.Optional;
import java.util.Set;

import com.google.common.net.HostAndPort;

public class CoordinatorInfo {

  // Variable names become JSON keys
  public long lastContact;
  public String server;
  public int numQueues;
  public int numCompactors;

  public CoordinatorInfo(Optional<HostAndPort> serverOpt, ExternalCompactionInfo ecInfo) {
    server = serverOpt.map(HostAndPort::toString).orElse("none");
    var groupToCompactors = ecInfo.getCompactors();
    numQueues = groupToCompactors.size();
    numCompactors = groupToCompactors.values().stream().mapToInt(Set::size).sum();
    lastContact = System.currentTimeMillis() - ecInfo.getFetchedTimeMillis();
  }
}
