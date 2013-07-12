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
package org.apache.accumulo.core.util.shell.commands;

import org.apache.accumulo.core.Constants;
import org.apache.accumulo.core.client.impl.Tables;
import org.apache.accumulo.core.util.shell.Shell;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;

public class DeleteTableCommand extends TableOperation {
  private Option forceOpt;
  
  public int execute(final String fullCommand, final CommandLine cl, final Shell shellState) throws Exception {
    if (cl.hasOption(forceOpt.getOpt())) {
      super.force();
    } else {
      super.noForce();
    }
    return super.execute(fullCommand, cl, shellState);
  }
  
  @Override
  public String description() {
    return "deletes a table";
  }
  
  @Override
  protected void doTableOp(final Shell shellState, final String tableName) throws Exception {
    shellState.getConnector().tableOperations().delete(tableName);
    shellState.getReader().println("Table: [" + tableName + "] has been deleted.");
    
    String n = Tables.extractNamespace(tableName);
    String checkTable = tableName;
    if (n.equals(Constants.DEFAULT_TABLE_NAMESPACE) || n.equals(Constants.SYSTEM_TABLE_NAMESPACE)) {
      checkTable = Tables.extractTableName(tableName);
    }
    if (shellState.getTableName().equals(checkTable)) {
      shellState.setTableName("");
    }
  }
  
  @Override
  public Options getOptions() {
    forceOpt = new Option("f", "force", false, "force deletion without prompting");
    final Options opts = super.getOptions();
    
    opts.addOption(forceOpt);
    return opts;
  }
}
