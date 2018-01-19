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

import org.apache.accumulo.api.data.Table;

/**
 * This class enables efficient batch writing to multiple tables. Normally, when creating a batch writer for each table, each has its own memory and network
 * resources. Using this class, however, these resources may be shared among multiple tables.
 */
public interface MultiTableWriter {

  /**
   * Flush and release all resources. This writer cannot be used again, once closed.
   *
   * @throws MutationsRejected
   *           when queued mutations are unable to be inserted
   *
   */
  void close() throws MutationsRejected;

  /**
   * Send all queued mutations for all tables to accumulo.
   *
   * @throws MutationsRejected
   *           when queued mutations are unable to be inserted
   */
  void flush() throws MutationsRejected;

  /**
   * Returns a BatchWriter for a particular table.
   *
   * @param table
   *          the name of a table whose batch writer you wish to retrieve
   * @return an instance of a batch writer for the specified table
   */
  TableWriter getTableWriter(Table table);

}
