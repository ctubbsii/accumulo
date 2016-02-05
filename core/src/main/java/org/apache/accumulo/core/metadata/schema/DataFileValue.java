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
package org.apache.accumulo.core.metadata.schema;

import static java.nio.charset.StandardCharsets.UTF_8;

import java.util.Base64;
import java.util.Optional;

import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.Value;

import com.google.common.collect.BoundType;
import com.google.common.collect.Range;
import com.google.common.collect.TreeRangeSet;
import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonPrimitive;

public class DataFileValue {

  private static final Gson gson = new Gson();

  private final long size;
  private final long numEntries;
  private long time;
  private final Optional<TreeRangeSet<ByteSequence>> ranges;

  public DataFileValue(long size, long numEntries, long time) {
    this.size = size;
    this.numEntries = numEntries;
    this.time = time;
    this.ranges = Optional.empty();
  }

  public DataFileValue(long size, long numEntries) {
    this.size = size;
    this.numEntries = numEntries;
    this.time = -1;
    this.ranges = Optional.empty();
  }

  public DataFileValue(byte[] encodedDFV) {
    String[] ba = new String(encodedDFV, UTF_8).split(",");

    size = Long.parseLong(ba[0]);
    numEntries = Long.parseLong(ba[1]);

    if (ba.length == 3)
      time = Long.parseLong(ba[2]);
    else
      time = -1;

    this.ranges = Optional.empty();
  }

  public long getSize() {
    return size;
  }

  public long getNumEntries() {
    return numEntries;
  }

  public boolean isTimeSet() {
    return time >= 0;
  }

  public long getTime() {
    return time;
  }

  public Optional<TreeRangeSet<ByteSequence>> getRanges() {
    return ranges;
  }

  public byte[] encode() {
    return encodeAsString().getBytes(UTF_8);
  }

  public String encodeAsString() {
    StringBuilder sb = new StringBuilder(getSize() + "," + numEntries);
    if (isTimeSet()) {
      sb.append("," + getTime());
    }
    if (ranges.isPresent()) {
      JsonArray serializedRanges = new JsonArray();
      for (Range<ByteSequence> r : ranges.get().asRanges()) {
        serializedRanges.add(new JsonPrimitive(encodeRange(r)));
        ;
      }
      sb.append(gson.toJson(serializedRanges));
    }
    return sb.toString();
  }

  private static String encodeRange(Range<ByteSequence> r) {
    StringBuilder sb = new StringBuilder();
    if (r.hasLowerBound()) {
      sb.append(r.lowerBoundType() == BoundType.CLOSED ? '[' : '(');
      sb.append(Base64.getEncoder().encodeToString(r.lowerEndpoint().toArray()));
    } else {
      sb.append("(-inf");
    }
    sb.append(',');
    if (r.hasUpperBound()) {
      sb.append(Base64.getEncoder().encodeToString(r.upperEndpoint().toArray()));
      sb.append(r.upperBoundType() == BoundType.CLOSED ? ']' : ')');
    } else {
      sb.append("+inf)");
    }
    return sb.toString();
  }

  public Value encodeAsValue() {
    return new Value(encode());
  }

  @Override
  public boolean equals(Object o) {
    if (o instanceof DataFileValue) {
      DataFileValue odfv = (DataFileValue) o;

      return size == odfv.size && numEntries == odfv.numEntries;
    }

    return false;
  }

  @Override
  public int hashCode() {
    return Long.valueOf(size + numEntries).hashCode();
  }

  @Override
  public String toString() {
    return size + " " + numEntries;
  }

  public void setTime(long time) {
    if (time < 0)
      throw new IllegalArgumentException();
    this.time = time;
  }
}
