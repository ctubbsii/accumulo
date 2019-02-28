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
package org.apache.accumulo.core.trace;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Proxy;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import org.apache.accumulo.core.conf.ClientProperty;
import org.apache.accumulo.core.conf.ConfigurationTypeHelper;
import org.apache.accumulo.core.trace.thrift.TInfo;
import org.apache.htrace.core.HTraceConfiguration;
import org.apache.htrace.core.ProbabilitySampler;
import org.apache.htrace.core.Sampler;
import org.apache.htrace.core.SpanId;
import org.apache.htrace.core.TraceScope;
import org.apache.htrace.core.Tracer;

/**
 * Utility class for tracing within Accumulo. Not intended for client use!
 */
public class Trace {

  public static final String TRACE_HOST_PROPERTY = "trace.host";
  public static final String TRACE_SERVICE_PROPERTY = "trace.service";
  public static final String TRACER_ZK_HOST = "tracer.zookeeper.host";
  public static final String TRACER_ZK_TIMEOUT = "tracer.zookeeper.timeout";
  public static final String TRACER_ZK_PATH = "tracer.zookeeper.path";

  private static Tracer tracer = new Tracer.Builder("AccumuloTracer").build();

  public static TraceScope startSpan(String description, Sampler sampler) {
    tracer.addSampler(sampler); // not correct, but works for now
    return tracer.newScope(description);
  }

  /**
   * Enable tracing by setting up SpanReceivers for the current process. If host name is null, it
   * will be determined. If service name is null, the simple name of the class will be used.
   * Properties required in the client configuration include
   * {@link org.apache.accumulo.core.conf.ClientProperty#TRACE_SPAN_RECEIVERS} and any properties
   * specific to the span receiver.
   */
  public static Tracer createTracerForClient(String hostname, String service,
      Properties properties) {
    // @formatter:off
      return createTracer(hostname, service,
          ClientProperty.TRACE_SPAN_RECEIVERS.getValue(properties),
          ClientProperty.INSTANCE_ZOOKEEPERS.getValue(properties),
          ConfigurationTypeHelper
              .getTimeInMillis(ClientProperty.INSTANCE_ZOOKEEPERS_TIMEOUT.getValue(properties)),
          ClientProperty.TRACE_ZOOKEEPER_PATH.getValue(properties),
          ClientProperty.toMap(
              ClientProperty.getPrefix(properties, ClientProperty.TRACE_SPAN_RECEIVER_PREFIX)),
          ClientProperty.TRACE_SPAN_RECEIVER_PREFIX);
      // @formatter:on
  }

  public static Tracer createTracer(String hostname, String service, String spanReceivers,
      String zookeepers, long timeout, String zkPath, Map<String,String> spanReceiverProps,
      String rcvrPropsPrefix) {

    HashMap<String,String> props = new HashMap<>();
    if (hostname != null) {
      props.put(TRACE_HOST_PROPERTY, hostname);
    }
    if (service != null) {
      props.put(TRACE_SERVICE_PROPERTY, service);
    }
    props.put(Tracer.SPAN_RECEIVER_CLASSES_KEY, spanReceivers.replace(',', ';'));
    props.put(TRACER_ZK_HOST, zookeepers);
    props.put(TRACER_ZK_TIMEOUT, Long.toString(timeout));
    props.put(TRACER_ZK_PATH, zkPath);
    spanReceiverProps.forEach((k, v) -> props.put(k.substring(rcvrPropsPrefix.length()), v));

    Tracer tracer = new Tracer.Builder(service).conf(HTraceConfiguration.fromMap(props)).build();
    return tracer;
  }

  /**
   * Continue a trace by starting a new span with a given parent and description.
   */
  public static TraceScope trace(Tracer tracer, TInfo info, String description) {
    SpanId parent = new SpanId(info.traceId, info.parentId);
    return parent.isValid() ? tracer.newScope(description, parent) : tracer.newNullScope();
  }

  private static final TInfo DONT_TRACE = new TInfo(0, 0);

  /**
   * Obtain {@link org.apache.accumulo.core.trace.thrift.TInfo} for the current span.
   */
  public static TInfo traceInfo() {
    SpanId span = Tracer.getCurrentSpanId();
    if (span != null) {
      return new TInfo(span.getHigh(), span.getLow());
    }
    return DONT_TRACE;
  }

  public static ProbabilitySampler probabilitySampler(double fraction) {
    return new ProbabilitySampler(HTraceConfiguration.fromMap(Collections
        .singletonMap(ProbabilitySampler.SAMPLER_FRACTION_CONF_KEY, Double.toString(fraction))));
  }

  /**
   * To move trace data from client to server, the RPC call must be annotated to take a TInfo object
   * as its first argument. The user can simply pass null, so long as they wrap their Client and
   * Service objects with these functions.
   *
   * <pre>
   * Trace.on(&quot;remoteMethod&quot;);
   * Iface c = new Client();
   * c = TraceWrap.client(c);
   * c.remoteMethod(null, arg2, arg3);
   * Trace.off();
   * </pre>
   *
   * The wrapper will see the annotated method and send or re-establish the trace information.
   *
   * Note that the result of these calls is a Proxy object that conforms to the basic interfaces,
   * but is not your concrete instance.
   */
  public static <T> T wrapClient(Tracer tracer, final T instance) {
    InvocationHandler handler = (obj, method, args) -> {
      if (args == null || args.length < 1 || args[0] != null) {
        return method.invoke(instance, args);
      }
      if (TInfo.class.isAssignableFrom(method.getParameterTypes()[0])) {
        args[0] = traceInfo();
      }
      try (TraceScope span = tracer.newScope("client:" + method.getName())) {
        return method.invoke(instance, args);
      } catch (InvocationTargetException ex) {
        throw ex.getCause();
      }
    };
    return wrapRpc(handler, instance);
  }

  public static <T> T wrapService(Tracer tracer, final T instance) {
    InvocationHandler handler = (obj, method, args) -> {
      try {
        if (args == null || args.length < 1 || args[0] == null || !(args[0] instanceof TInfo)) {
          return method.invoke(instance, args);
        }
        try (TraceScope span = trace(tracer, (TInfo) args[0], method.getName())) {
          return method.invoke(instance, args);
        }
      } catch (InvocationTargetException ex) {
        throw ex.getCause();
      }
    };
    return wrapRpc(handler, instance);
  }

  private static <T> T wrapRpc(final InvocationHandler handler, final T instance) {
    @SuppressWarnings("unchecked")
    T proxiedInstance = (T) Proxy.newProxyInstance(instance.getClass().getClassLoader(),
        instance.getClass().getInterfaces(), handler);
    return proxiedInstance;
  }

}
