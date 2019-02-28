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
package org.apache.accumulo.tracer;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.Callable;

import org.apache.accumulo.core.trace.Trace;
import org.apache.accumulo.core.trace.thrift.TInfo;
import org.apache.accumulo.tracer.thrift.TestService;
import org.apache.accumulo.tracer.thrift.TestService.Iface;
import org.apache.accumulo.tracer.thrift.TestService.Processor;
import org.apache.htrace.core.Sampler;
import org.apache.htrace.core.Span;
import org.apache.htrace.core.SpanId;
import org.apache.htrace.core.SpanReceiver;
import org.apache.htrace.core.TraceScope;
import org.apache.htrace.core.Tracer;
import org.apache.htrace.core.TracerPool;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.server.TServer;
import org.apache.thrift.server.TThreadPoolServer;
import org.apache.thrift.transport.TServerSocket;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

public class TracerTest {

  @Rule
  public TestName testName = new TestName();

  static class TestReceiver extends SpanReceiver {
    public final HashSet<Span> rootSpans = new HashSet<>();
    public final Map<SpanId,List<Span>> spansByParent = new HashMap<>();

    public TestReceiver() {}

    @Override
    public void receiveSpan(Span s) {
      if (s.getParents().length == 0) {
        rootSpans.add(s);
      } else {
        for (SpanId parent : s.getParents()) {
          spansByParent.computeIfAbsent(parent, x -> new ArrayList<>()).add(s);
        }
      }
    }

    @Override
    public void close() {}
  }

  private TestReceiver testReceiver;

  @Before
  public void addTestReceiver() {
    testReceiver = new TestReceiver();
    TracerPool.getGlobalTracerPool().addReceiver(testReceiver);
  }

  @After
  public void removeTestReceiver() {
    if (testReceiver != null) {
      TracerPool.getGlobalTracerPool().removeAndCloseReceiver(testReceiver);
    }
  }

  private <T extends Sampler> Tracer createTestTracer(T sampler) {
    Tracer tracer = new Tracer.Builder(testName.getMethodName()).build();
    tracer.addSampler(Objects.requireNonNull(sampler));
    return tracer;
  }

  @Test
  public void testTraceNoSampling() throws Exception {
    // try tracing with no sampling
    try (Tracer tracer = createTestTracer(Sampler.NEVER)) {
      assertNull(Tracer.curThreadTracer());
      try (TraceScope span = tracer.newScope("nop")) {
        assertNull(Tracer.curThreadTracer());
      }
      assertNull(Tracer.curThreadTracer());
      assertEquals(0, testReceiver.rootSpans.size());
      assertEquals(0, testReceiver.spansByParent.size());
    }
  }

  @Test
  public void testSingleRoot() throws Exception {
    try (Tracer tracer = createTestTracer(Sampler.ALWAYS)) {
      assertNull(Tracer.curThreadTracer()); // tracing not started
      Span curSpan;

      // a root span
      try (TraceScope span = tracer.newScope("nop")) {
        assertNotNull(Tracer.curThreadTracer()); // tracing is running
        curSpan = Tracer.getCurrentSpan();
      }
      assertNull(Tracer.curThreadTracer()); // tracing stopped
      assertEquals(1, testReceiver.rootSpans.size()); // one span received
      assertTrue(testReceiver.rootSpans.contains(curSpan));
      assertFalse(testReceiver.spansByParent.containsKey(curSpan.getSpanId())); // no children
    }
  }

  @Test
  public void testTwoRootsWithChildren() throws Exception {
    Span rootSpan, innerSpan;
    try (Tracer tracer = createTestTracer(Sampler.ALWAYS)) {
      assertNull(Tracer.curThreadTracer()); // tracing not started

      // root span
      try (TraceScope span = tracer.newScope("outside root span")) {
        assertNotNull(Tracer.curThreadTracer()); // tracing is running
        rootSpan = Tracer.getCurrentSpan();
        assertNotNull(rootSpan);

        try (TraceScope inner = tracer.newScope("inside span")) {
          Thread.sleep(100); // duration at least 100ms
          assertNotNull(Tracer.curThreadTracer()); // tracing is running
          innerSpan = Tracer.getCurrentSpan();
          assertNotNull(innerSpan);
        }
      }

      assertNull(Tracer.curThreadTracer()); // tracing not started
    }
    assertEquals(1, testReceiver.rootSpans.size());
    assertTrue(testReceiver.rootSpans.contains(rootSpan));
    assertEquals(1, testReceiver.spansByParent.size());
    assertTrue(testReceiver.spansByParent.containsKey(rootSpan.getSpanId()));
    assertEquals(1, testReceiver.spansByParent.get(rootSpan.getSpanId()).size());
    Span child = testReceiver.spansByParent.get(rootSpan.getSpanId()).get(0);
    assertEquals(1, child.getParents().length);
    assertEquals(rootSpan.getSpanId(), child.getParents()[0]);
    assertTrue(child.getStopTimeMillis() - child.getStartTimeMillis() >= 100);
  }

  static class Service implements TestService.Iface {
    @Override
    public boolean checkTrace(TInfo t, String message) {
      Tracer testTracer = new Tracer.Builder(Service.class.getName()).build();
      testTracer.addSampler(Sampler.NEVER);
      try (TraceScope trace = testTracer.newScope(message)) {
        return Tracer.curThreadTracer() != null;
      }
    }
  }

  @SuppressFBWarnings(value = {"UNENCRYPTED_SOCKET", "UNENCRYPTED_SERVER_SOCKET"},
      justification = "insecure, known risk, test socket")
  @Test
  public void testThrift() throws Exception {
    try (Tracer testTracer = createTestTracer(Sampler.ALWAYS)) {
      ServerSocket socket = new ServerSocket(0);
      TServerSocket transport = new TServerSocket(socket);
      transport.listen();
      TThreadPoolServer.Args args = new TThreadPoolServer.Args(transport);
      args.processor(new Processor<Iface>(Trace.wrapService(testTracer, new Service())));
      final TServer tserver = new TThreadPoolServer(args);
      Thread t = new Thread(tserver::serve);
      t.start();
      TTransport clientTransport = new TSocket(new Socket("localhost", socket.getLocalPort()));
      TestService.Iface client = new TestService.Client(new TBinaryProtocol(clientTransport),
          new TBinaryProtocol(clientTransport));
      client = Trace.wrapClient(testTracer, client);
      assertFalse(client.checkTrace(null, "test"));

      SpanId startTraceId;
      try (TraceScope start = testTracer.newScope("start")) {
        assertTrue(client.checkTrace(null, "my test"));
        startTraceId = start.getSpan().getSpanId();
      }

      assertNotNull(testReceiver.spansByParent.get(startTraceId));
      String[] traces = {"my test", "checkTrace", "client:checkTrace", "start"};
      assertEquals(traces.length, testReceiver.spansByParent.get(startTraceId).size());
      for (int i = 0; i < traces.length; i++) {
        assertEquals(traces[i],
            testReceiver.spansByParent.get(startTraceId).get(i).getDescription());
      }

      tserver.stop();
      t.join(100);
    }
  }

  Callable<Object> callable;

  @Before
  public void setup() {
    callable = () -> {
      throw new IOException();
    };
  }

  /**
   * Verify that exceptions propagate up through the trace wrapping with sampling enabled, as the
   * cause of the reflexive exceptions.
   */
  @Test(expected = IOException.class)
  public void testTracedException() throws Throwable {
    Tracer tracer = createTestTracer(Sampler.ALWAYS);
    try {
      tracer.wrap(callable, testName.getMethodName()).call();
    } catch (InvocationTargetException e) {
      throw e.getCause();
    }
  }

  /**
   * Verify that exceptions propagate up through the trace wrapping with sampling disabled, as the
   * cause of the reflexive exceptions.
   */
  @Test(expected = IOException.class)
  public void testUntracedException() throws Throwable {
    try (Tracer tracer = createTestTracer(Sampler.NEVER)) {
      tracer.wrap(callable, testName.getMethodName()).call();
    } catch (InvocationTargetException e) {
      throw e.getCause();
    }
  }
}
