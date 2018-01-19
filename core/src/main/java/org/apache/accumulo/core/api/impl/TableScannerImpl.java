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
package org.apache.accumulo.core.api.impl;

import java.util.Iterator;

import org.apache.accumulo.api.client.io.ScanConfiguration;
import org.apache.accumulo.api.client.io.TableScanner;
import org.apache.accumulo.api.data.Cell;

/**
 *
 */
public class TableScannerImpl implements TableScanner {

  /**
   *
   */
  public TableScannerImpl(AccumuloClientImpl client, ScanConfiguration scanConfiguration) {
    // TODO Auto-generated constructor stub
  }

  @Override
  public void close() {
    // TODO Auto-generated method stub

  }

  @Override
  public Iterator<Cell> iterator() {
    // TODO Auto-generated method stub
    return null;
  }

}
