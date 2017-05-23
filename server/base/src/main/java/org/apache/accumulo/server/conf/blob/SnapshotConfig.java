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
package org.apache.accumulo.server.conf.blob;

import java.util.Collections;
import java.util.Map;
import java.util.function.Predicate;

import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.server.conf.snapshot.ConfigurationSnapshotSerializer.VersionedConfBlob;

public interface SnapshotConfig {

  String get(Property property);

  void getProperties(Map<String,String> destination, Predicate<String> filter, Predicate<String> parentFilter);

  int getSequenceId();

  String toJSON();

  public static final SnapshotConfig EMPTY = new SnapshotConfig() {

    private final String jsonString = new VersionedConfBlob(Collections.<String,String> emptyMap()).serialize();

    @Override
    public String get(final Property property) {
      return null;
    }

    @Override
    public void getProperties(final Map<String,String> destination, final Predicate<String> filter, final Predicate<String> parentFilter) {}

    @Override
    public int getSequenceId() {
      return 0;
    }

    @Override
    public String toJSON() {
      return jsonString;
    }

  };

}
