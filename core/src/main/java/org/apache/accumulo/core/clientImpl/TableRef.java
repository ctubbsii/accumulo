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
package org.apache.accumulo.core.clientImpl;

import static java.util.Objects.requireNonNull;

import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.clientImpl.thrift.TTable;
import org.apache.accumulo.core.data.TableId;

// a reference to a table, after its ID has been resolved
public class TableRef {

  public static final TableRef NONE = new TableRef();
  private final TableId id;
  private final String name;

  // used only for creating NONE
  private TableRef() {
    this.id = null;
    this.name = null;
  }

  TableRef(TableId tableId, String tableName) {
    this.id = requireNonNull(tableId);
    this.name = requireNonNull(tableName);
  }

  static TableRef resolve(ClientContext context, String tableName) throws TableNotFoundException {
    return new TableRef(context.getTableId(tableName), tableName);
  }

  TTable toThrift() {
    return new TTable(id.canonical(), name);
  }

  public TableId id() {
    return id;
  }

  public String name() {
    return name;
  }

}
