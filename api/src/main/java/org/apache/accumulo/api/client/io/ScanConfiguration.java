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

import static java.util.Objects.requireNonNull;

import java.util.Collections;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.accumulo.api.data.Column;
import org.apache.accumulo.api.data.IteratorSetting;
import org.apache.accumulo.api.security.Authorizations;

public class ScanConfiguration {

  private final TreeMap<Integer,IteratorSetting> scanIterators = new TreeMap<>();
  private final Set<Column> fetchedColumns = Collections.synchronizedSet(new TreeSet<Column>());
  private final Set<Column.Family> fetchedColumnFamilies = Collections.synchronizedSet(new TreeSet<Column.Family>());

  private final AtomicReference<Authorizations> authorizations = new AtomicReference<>(Authorizations.EMPTY);
  private final AtomicLong timeout = new AtomicLong(Long.MAX_VALUE);

  /**
   * Appends a scan-time iterator to this scanner immediately following all other iterators configured on this scanner. This is achieved by inserting the given
   * iterator at the priority of the highest priority iterator previously defined on this scanner, plus one, or 100, whichever is greater.
   *
   * @return this
   */
  public ScanConfiguration appendIterator(IteratorSetting iterator) {
    requireNonNull(iterator, "iterator is null");
    synchronized (scanIterators) {
      int nextPriority = 100;
      if (!scanIterators.isEmpty()) {
        nextPriority = Math.max(100, scanIterators.lastKey() + 1);
      }
      scanIterators.put(nextPriority, iterator);
    }
    return this;
  }

  /**
   * Inserts a scan-time iterator at the given priority.
   *
   * @param priority
   *          the priority for this iterator; for reference, default iterators use priority 20 or less; it is recommended that scan time iterators start at 100;
   *          data is processed through iterators at the lowest priority first; if two iterators are defined at the same priority, the order is undefined
   * @param iterator
   *          fully specified scan-time iterator, including all options for the iterator. Any changes to the iterator setting after this call are not propagated
   *          to the stored iterator.
   * @throws IllegalArgumentException
   *           if the setting conflicts with existing iterators
   */
  public ScanConfiguration insertIterator(int priority, IteratorSetting iterator) {
    requireNonNull(iterator, "iterator is null");
    if (priority <= 0)
      throw new IllegalArgumentException("priority must be greater than zero");
    synchronized (scanIterators) {
      if (scanIterators.containsKey(priority))
        throw new IllegalArgumentException("iterator priority " + priority + " is already in use");
      scanIterators.put(priority, iterator);
    }
    return this;
  }

  /**
   * Adds a column to the list of columns that will be fetched by this scanner. The column is identified by family and qualifier. By default when no columns
   * have been added the scanner fetches all columns.
   *
   * @param columnFamily
   *          the column family of the column to be fetched
   * @param columnQualifier
   *          the column qualifier of the column to be fetched
   * @return this
   */
  public ScanConfiguration fetchColumn(Column.Family columnFamily, Column.Qualifier columnQualifier) {
    requireNonNull(columnFamily, "columnFamily is null");
    requireNonNull(columnQualifier, "columnQualifier is null");
    fetchedColumns.add(new Column(columnFamily, columnQualifier));
    return this;
  }

  /**
   * Adds a column family to the list of columns that will be fetched by this scanner. By default when no columns have been added the scanner fetches all
   * columns.
   *
   * @param columnFamily
   *          the column family to be fetched
   * @return this
   */
  public ScanConfiguration fetchColumnFamily(Column.Family columnFamily) {
    requireNonNull(columnFamily, "columnFamily is null");
    this.fetchedColumnFamilies.add(columnFamily);
    return this;
  }

  /**
   * This setting configures the subset of a user's authorizations to use for the scanner
   *
   * @param authorizations
   *          the authorizations to use (default: {@link Authorizations#EMPTY})
   * @return this
   */
  public ScanConfiguration setScanAuthorizations(Authorizations authorizations) {
    requireNonNull(authorizations, "authorizations is null");
    this.authorizations.set(authorizations);
    return this;
  }

  /**
   * This setting determines how long a scanner will automatically retry when a failure occurs. By default a scanner will retry forever.
   *
   * Setting to zero, or setting to Long.MAX_VALUE and TimeUnit.MILLISECONDS, means to retry forever. Setting to a value less than 1 millisecond is equivalent
   * to setting the value to zero, which will retry forever.
   *
   * @param units
   *          determines how timeout is interpreted
   * @return this
   */
  public ScanConfiguration setTimeout(long timeout, TimeUnit units) {
    if (timeout < 0)
      throw new IllegalArgumentException("timeout must be non-negative");
    requireNonNull(units, "units is null");
    this.timeout.set(TimeUnit.MILLISECONDS.convert(timeout, units));
    return this;
  }

}
