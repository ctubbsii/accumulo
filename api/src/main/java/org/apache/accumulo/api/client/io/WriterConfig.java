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
package org.apache.accumulo.api.client.io;

import java.util.Objects;
import java.util.concurrent.TimeUnit;

public class WriterConfig {
  public enum Durability {
    // Note, the order of these is important; the "highest" Durability is used in group commits.
    /**
     * Use the durability as specified by the table or system configuration.
     */
    DEFAULT,

    /**
     * Write mutations to the write-ahead log, and ensure the data is stored on remote servers, but perhaps not on persistent storage.
     */
    FLUSH,

    /**
     * Write mutations the the write-ahead log. Data may be sitting the the servers output buffers, and not replicated anywhere.
     */
    LOG,

    /**
     * Don't bother writing mutations to the write-ahead log.
     */
    NONE,

    /**
     * Write mutations to the write-ahead log, and ensure the data is saved to persistent storage.
     */
    SYNC
  }

  private static final Long DEFAULT_MAX_LATENCY = 2 * 60 * 1000l;

  private static final Long DEFAULT_MAX_MEMORY = 50 * 1024 * 1024l;
  private static final Integer DEFAULT_MAX_WRITE_THREADS = 3;

  private static final Long DEFAULT_TIMEOUT = Long.MAX_VALUE;
  private Durability durability = Durability.DEFAULT;

  private Long maxLatency = null;
  private Long maxMemory = null;

  private Integer maxWriteThreads = null;

  private Long timeout = null;

  @Override
  public boolean equals(Object o) {
    if (o instanceof WriterConfig) {
      WriterConfig other = (WriterConfig) o;

      return Objects.equals(maxMemory, other.maxMemory) && Objects.equals(maxLatency, other.maxLatency)
          && Objects.equals(maxWriteThreads, other.maxWriteThreads) && Objects.equals(timeout, other.timeout) && Objects.equals(durability, other.durability);
    }
    return false;
  }

  /**
   * @return the durability to be used by the BatchWriter
   */
  public Durability getDurability() {
    return durability;
  }

  public long getMaxLatency(TimeUnit timeUnit) {
    return timeUnit.convert(maxLatency != null ? maxLatency : DEFAULT_MAX_LATENCY, TimeUnit.MILLISECONDS);
  }

  public long getMaxMemory() {
    return maxMemory != null ? maxMemory : DEFAULT_MAX_MEMORY;
  }

  public int getMaxWriteThreads() {
    return maxWriteThreads != null ? maxWriteThreads : DEFAULT_MAX_WRITE_THREADS;
  }

  public long getTimeout(TimeUnit timeUnit) {
    return timeUnit.convert(timeout != null ? timeout : DEFAULT_TIMEOUT, TimeUnit.MILLISECONDS);
  }

  @Override
  public int hashCode() {
    return Objects.hash(new Object[] {maxMemory, maxLatency, maxWriteThreads, timeout, durability});
  }

  /**
   * Change the durability for the BatchWriter session. The default durability is "default" which is the table's durability setting. If the durability is set to
   * something other than the default, it will override the durability setting of the table.
   *
   * @param durability
   *          the Durability to be used by the BatchWriter
   */
  public WriterConfig setDurability(Durability durability) {
    this.durability = durability;
    return this;
  }

  /**
   * Sets the maximum amount of time to hold the data in memory before flushing it to servers.<br>
   * For no maximum, set to zero, or {@link Long#MAX_VALUE} with {@link TimeUnit#MILLISECONDS}.
   *
   * <p>
   * {@link TimeUnit#MICROSECONDS} or {@link TimeUnit#NANOSECONDS} will be truncated to the nearest {@link TimeUnit#MILLISECONDS}.<br>
   * If this truncation would result in making the value zero when it was specified as non-zero, then a minimum value of one {@link TimeUnit#MILLISECONDS} will
   * be used.
   *
   * <p>
   * <b>Default:</b> 120 seconds
   *
   * @param maxLatency
   *          the maximum latency, in the unit specified by the value of {@code timeUnit}
   * @param timeUnit
   *          determines how {@code maxLatency} will be interpreted
   * @throws IllegalArgumentException
   *           if {@code maxLatency} is less than 0
   * @return {@code this} to allow chaining of set methods
   */
  public WriterConfig setMaxLatency(long maxLatency, TimeUnit timeUnit) {
    if (maxLatency < 0)
      throw new IllegalArgumentException("Negative max latency not allowed " + maxLatency);
    if (maxLatency == 0)
      this.maxLatency = Long.MAX_VALUE;
    else
      // make small, positive values that truncate to 0 when converted use the minimum millis instead
      this.maxLatency = Math.max(1, timeUnit.toMillis(maxLatency));
    return this;
  }

  /**
   * Sets the maximum memory to batch before writing. The smaller this value, the more frequently the {@link TableWriter} will write.<br>
   * If set to a value smaller than a single mutation, then it will {@link TableWriter#flush()} after each added mutation. Must be non-negative.
   *
   * <p>
   * <b>Default:</b> 50M
   *
   * @param maxMemory
   *          max size in bytes
   * @throws IllegalArgumentException
   *           if {@code maxMemory} is less than 0
   * @return {@code this} to allow chaining of set methods
   */
  public WriterConfig setMaxMemory(long maxMemory) {
    if (maxMemory < 0)
      throw new IllegalArgumentException("Max memory must be non-negative " + maxMemory);
    this.maxMemory = maxMemory;
    return this;
  }

  /**
   * Sets the maximum number of threads to use for writing data to the tablet servers.
   *
   * <p>
   * <b>Default:</b> 3
   *
   * @param maxWriteThreads
   *          the maximum threads to use
   * @throws IllegalArgumentException
   *           if {@code maxWriteThreads} is non-positive
   * @return {@code this} to allow chaining of set methods
   */
  public WriterConfig setMaxWriteThreads(int maxWriteThreads) {
    if (maxWriteThreads <= 0)
      throw new IllegalArgumentException("Max threads must be positive " + maxWriteThreads);
    this.maxWriteThreads = maxWriteThreads;
    return this;
  }

  /**
   * Sets the maximum amount of time an unresponsive server will be re-tried. When this timeout is exceeded, the {@link TableWriter} should throw an exception. <br>
   * For no timeout, set to zero, or {@link Long#MAX_VALUE} with {@link TimeUnit#MILLISECONDS}.
   *
   * <p>
   * {@link TimeUnit#MICROSECONDS} or {@link TimeUnit#NANOSECONDS} will be truncated to the nearest {@link TimeUnit#MILLISECONDS}.<br>
   * If this truncation would result in making the value zero when it was specified as non-zero, then a minimum value of one {@link TimeUnit#MILLISECONDS} will
   * be used.
   *
   * <p>
   * <b>Default:</b> {@link Long#MAX_VALUE} (no timeout)
   *
   * @param timeout
   *          the timeout, in the unit specified by the value of {@code timeUnit}
   * @param timeUnit
   *          determines how {@code timeout} will be interpreted
   * @throws IllegalArgumentException
   *           if {@code timeout} is less than 0
   * @return {@code this} to allow chaining of set methods
   */
  public WriterConfig setTimeout(long timeout, TimeUnit timeUnit) {
    if (timeout < 0)
      throw new IllegalArgumentException("Negative timeout not allowed " + timeout);
    if (timeout == 0)
      this.timeout = Long.MAX_VALUE;
    else
      // make small, positive values that truncate to 0 when converted use the minimum millis instead
      this.timeout = Math.max(1, timeUnit.toMillis(timeout));
    return this;
  }

  @Override
  public String toString() {
    return com.google.common.base.Objects.toStringHelper(this).add("maxMemory", getMaxMemory() + "B")
        .add("maxLatency", getMaxLatency(TimeUnit.MILLISECONDS) + "ms").add("maxWriteThreads", getMaxWriteThreads())
        .add("timeout", getTimeout(TimeUnit.MILLISECONDS) + "ms").add("durability", getDurability()).toString();
  }

}
