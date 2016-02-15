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
package org.apache.accumulo.core.data;

import static com.google.common.base.Charsets.UTF_8;
import static com.google.common.base.Preconditions.checkNotNull;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import com.google.common.base.Optional;
import com.google.common.collect.Range;

public class TableRowRange {
  private final String tableId;
  private final Range<ByteSequence> rowRange;

  public TableRowRange(final String tableId, final Optional<? extends ByteSequence> previousEndRow, final Optional<? extends ByteSequence> endRow) {
    this.tableId = checkNotNull(tableId);
    checkNotNull(previousEndRow);
    checkNotNull(endRow);
    if (previousEndRow.isPresent()) {
      if (endRow.isPresent()) {
        this.rowRange = Range.<ByteSequence> openClosed(previousEndRow.get(), endRow.get());
      } else {
        this.rowRange = Range.<ByteSequence> greaterThan(previousEndRow.get());
      }
    } else {
      if (endRow.isPresent()) {
        // functionally equivalent to Range.closed(ArrayByteSequence.empty(), endRow.get())
        this.rowRange = Range.<ByteSequence> atMost(endRow.get());
      } else {
        // functionally equivalent to atLeast(ArrayByteSequence.empty())
        this.rowRange = Range.<ByteSequence> all();
      }
    }
  }

  public void serialize(final DataOutput out) throws IOException {
    // write tableId first
    out.writeUTF(getTableId());
    Range<ByteSequence> r = getRowRange();
    // then write previousEndRow
    if (r.hasLowerBound()) {
      out.writeInt(r.lowerEndpoint().length());
      out.write(r.lowerEndpoint().toArray());
    } else {
      out.writeInt(-1);
    }
    // finally write endRow
    if (r.hasUpperBound()) {
      out.writeInt(r.upperEndpoint().length());
      out.write(r.upperEndpoint().toArray());
    } else {
      out.writeInt(-1);
    }
  }

  public TableRowRange deserialize(final DataInput in) throws IOException {
    // read tableId first
    String tableId = in.readUTF();
    Optional<ArrayByteSequence> previousEndRow;
    Optional<ArrayByteSequence> endRow;

    int len;
    // then read previousEndRow
    if ((len = in.readInt()) >= 0) {
      byte[] b = new byte[len];
      in.readFully(b);
      previousEndRow = Optional.of(new ArrayByteSequence(b));
    } else {
      previousEndRow = Optional.absent();
    }
    // finally, read endRow
    if ((len = in.readInt()) >= 0) {
      byte[] b = new byte[len];
      in.readFully(b);
      endRow = Optional.of(new ArrayByteSequence(b));
    } else {
      endRow = Optional.absent();
    }
    return new TableRowRange(tableId, previousEndRow, endRow);
  }

  public String getTableId() {
    return tableId;
  }

  public Range<ByteSequence> getRowRange() {
    return rowRange;
  }

  public boolean overlaps(TableRowRange other) {
    return !getRowRange().intersection(other.getRowRange()).isEmpty();
  }

  @Override
  public boolean equals(Object obj) {
    if (obj instanceof TableRowRange) {
      TableRowRange other = (TableRowRange) obj;
      return getTableId().equals(other.getTableId()) && getRowRange().equals(other.getRowRange());
    }
    return false;
  }

  @Override
  public int hashCode() {
    return getTableId().hashCode() + getRowRange().hashCode();
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder(getTableId());
    Range<ByteSequence> r = getRowRange();
    // endRow
    sb.append(r.hasUpperBound() ? ";" + printable(r.upperEndpoint()) : "<");
    // previousEndRow
    sb.append(r.hasLowerBound() ? ";" + printable(r.lowerEndpoint()) : "<");
    return sb.toString();
  }

  private static String printable(final ByteSequence bytes) {
    byte[] a = bytes.toArray();
    int max = 64;
    String s = new String(a, 0, Math.max(bytes.length(), max), UTF_8);
    s = a.length > max ? s + "... TRUNCATED" : s;
    return s.replaceAll(";", "\\\\;").replaceAll("\\\\", "\\\\\\\\");
  }

}
