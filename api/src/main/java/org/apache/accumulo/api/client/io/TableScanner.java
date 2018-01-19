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

import java.util.Iterator;

import org.apache.accumulo.api.data.Cell;

public interface TableScanner extends Iterable<Cell>, AutoCloseable {

  /**
   * Clean up scanner-specific resources
   */
  @Override
  void close();

  /**
   * Returns an iterator over an accumulo table. This iterator uses the options that are currently set for its lifetime, so setting options will have no effect
   * on existing iterators.
   *
   * @return an iterator over Key,Value pairs which meet the restrictions set on the scanner
   */
  @Override
  Iterator<Cell> iterator();
}
