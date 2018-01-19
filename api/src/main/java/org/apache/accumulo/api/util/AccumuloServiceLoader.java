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
package org.apache.accumulo.api.util;

import java.util.Iterator;
import java.util.ServiceLoader;

/**
 * A basic {@link ServiceLoader} utility class for binding Accumulo providers at runtime.
 *
 * @since 2.0.0
 */
public class AccumuloServiceLoader {

  /**
   * Uses Java's {@link ServiceLoader} to bind exactly one implementing provider to a given API at runtime.
   *
   * @param api
   *          The API class whose implementation provider is bound at runtime.
   * @return An instance of the provider which implements the class.
   * @throws IllegalStateException
   *           if not exactly one service provider was discovered on the classpath
   */
  public static <T> T loadService(Class<T> api) {
    ServiceLoader<T> loader = ServiceLoader.load(api);
    Iterator<T> iter = loader.iterator();
    T found = null;
    // search for the first non-null provider
    while (iter.hasNext() && null == (found = iter.next())) {}
    // ensure we've found a provider when finished looping
    if (found == null) {
      throw new IllegalStateException("No ServiceLoader binding for \"" + api.getName() + "\" found on the classpath. "
          + "Ensure you have exactly one \"accumulo-core.jar\" on your classpath.");
    }
    // detect multiple bindings
    while (iter.hasNext()) {
      if (iter.next() != null) {
        throw new IllegalStateException("Multiple ServiceLoader bindings for \"" + api.getName() + "\" found on the classpath. "
            + "Ensure you have exactly one \"accumulo-core.jar\" on your classpath.");
      }
    }
    return found;
  }

}
