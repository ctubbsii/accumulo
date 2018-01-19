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

import org.apache.accumulo.api.AccumuloException;
import org.apache.accumulo.api.data.Table;

/**
 * Thrown when a requested table does not exist.
 */
public class TableNotFound extends AccumuloException {

  private static final long serialVersionUID = 1L;

  public TableNotFound(CharSequence name) {
    super("Table not found with the name \"" + name + "\"");
  }

  public TableNotFound(CharSequence name, Throwable cause) {
    super("Table not found with the name \"" + name + "\"", cause);
  }

  public TableNotFound(Table.ID id) {
    super("Table not found with the id \"" + id + "\"");
  }

}
