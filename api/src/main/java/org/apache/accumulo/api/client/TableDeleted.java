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
package org.apache.accumulo.api.client;

import org.apache.accumulo.api.data.Table;
import org.apache.accumulo.api.data.Table.ID;

/**
 * This exception is thrown if a table is deleted after an operation starts.
 *
 * For example if table A exist when a scan is started, but is deleted during the scan then this exception is thrown.
 *
 */
public class TableDeleted extends IllegalStateException {

  private static final long serialVersionUID = 1L;

  private ID id;

  public TableDeleted(Table.ID id) {
    super("Table not found with the id \"" + id + "\"");
    this.id = id;
  }

  public ID getTableId() {
    return id;
  }

}
