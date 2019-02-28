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
package org.apache.accumulo.master.tableOps;

import org.apache.accumulo.core.trace.Trace;
import org.apache.accumulo.core.trace.thrift.TInfo;
import org.apache.accumulo.fate.Repo;
import org.apache.accumulo.master.Master;
import org.apache.htrace.core.TraceScope;

public class TraceRepo implements Repo<Master> {

  private static final long serialVersionUID = 1L;

  long traceId; // the parent's "high" 64 bits (same all spans within a trace)
  long parentId; // the parent's "low" 64 bits (unique per span)
  Repo<Master> repo;

  public TraceRepo(Repo<Master> repo) {
    this.repo = repo;
    TInfo tinfo = Trace.traceInfo();
    traceId = tinfo.traceId;
    parentId = tinfo.parentId;
  }

  @Override
  public long isReady(long tid, Master environment) throws Exception {
    try (TraceScope t = Trace.trace(environment.getContext().getTracer(),
        new TInfo(traceId, parentId), repo.getDescription())) {
      return repo.isReady(tid, environment);
    }
  }

  @Override
  public Repo<Master> call(long tid, Master environment) throws Exception {
    try (TraceScope t = Trace.trace(environment.getContext().getTracer(),
        new TInfo(traceId, parentId), repo.getDescription())) {
      Repo<Master> result = repo.call(tid, environment);
      if (result == null)
        return null;
      return new TraceRepo(result);
    }
  }

  @Override
  public void undo(long tid, Master environment) throws Exception {
    try (TraceScope t = Trace.trace(environment.getContext().getTracer(),
        new TInfo(traceId, parentId), repo.getDescription())) {
      repo.undo(tid, environment);
    }
  }

  @Override
  public String getDescription() {
    return repo.getDescription();
  }

  @Override
  public String getReturn() {
    return repo.getReturn();
  }

}
