/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.accumulo.fate.zookeeper;

import static java.util.concurrent.TimeUnit.NANOSECONDS;

import java.io.IOException;
import java.net.UnknownHostException;
import java.security.SecureRandom;

import org.apache.accumulo.core.util.AddressUtil;
import org.apache.accumulo.fate.util.UtilWaitThread;
import org.apache.zookeeper.Watcher.Event.KeeperState;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.ZooKeeper.States;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ZooSession {

  private static final Logger log = LoggerFactory.getLogger(ZooSession.class);

  private static final SecureRandom random = new SecureRandom();

  /**
   * @param hosts
   *          comma separated list of zk servers
   * @param timeout
   *          in milliseconds
   * @param scheme
   *          authentication type, e.g. 'digest', may be null
   * @param auth
   *          authentication-scheme-specific token, may be null
   */
  public static ZooKeeper getSession(String hosts, int timeout, String scheme, byte[] auth) {
    log.debug("Connecting to {} with timeout {} with auth", hosts, timeout);
    final int TIME_BETWEEN_CONNECT_CHECKS_MS = 100;
    int connectTimeWait = Math.min(10_000, timeout);
    boolean tryAgain = true;
    long sleepTime = 100;
    ZooKeeper zooKeeper = null;

    long startTime = System.nanoTime();

    while (tryAgain) {
      try {
        zooKeeper = new ZooKeeper(hosts, timeout, event -> {
          if (event.getState() == KeeperState.Expired) {
            log.debug("Session expired, state of current session : {}", event.getState());
          }
        });
        // it may take some time to get connected to zookeeper if some of the servers are down
        for (int i = 0; i < connectTimeWait / TIME_BETWEEN_CONNECT_CHECKS_MS && tryAgain; i++) {
          if (zooKeeper.getState().equals(States.CONNECTED)) {
            if (auth != null)
              ZooUtil.auth(zooKeeper, scheme, auth);
            tryAgain = false;
          } else
            UtilWaitThread.sleep(TIME_BETWEEN_CONNECT_CHECKS_MS);
        }

      } catch (IOException e) {
        if (e instanceof UnknownHostException) {
          /*
           * Make sure we wait at least as long as the JVM TTL for negative DNS responses
           */
          int ttl = AddressUtil.getAddressCacheNegativeTtl((UnknownHostException) e);
          sleepTime = Math.max(sleepTime, (ttl + 1) * 1000L);
        }
        log.warn("Connection to zooKeeper failed, will try again in "
            + String.format("%.2f secs", sleepTime / 1000.0), e);
      } finally {
        if (tryAgain && zooKeeper != null)
          try {
            zooKeeper.close();
            zooKeeper = null;
          } catch (InterruptedException e) {
            log.warn("interrupted", e);
          }
      }

      long stopTime = System.nanoTime();
      long duration = NANOSECONDS.toMillis(stopTime - startTime);

      if (duration > 2L * timeout) {
        throw new RuntimeException("Failed to connect to zookeeper (" + hosts
            + ") within 2x zookeeper timeout period " + timeout);
      }

      if (tryAgain) {
        if (2L * timeout < duration + sleepTime + connectTimeWait)
          sleepTime = 2L * timeout - duration - connectTimeWait;
        if (sleepTime < 0) {
          connectTimeWait -= sleepTime;
          sleepTime = 0;
        }
        UtilWaitThread.sleep(sleepTime);
        if (sleepTime < 10000)
          sleepTime = sleepTime + (long) (sleepTime * random.nextDouble());
      }
    }

    return zooKeeper;
  }

}
