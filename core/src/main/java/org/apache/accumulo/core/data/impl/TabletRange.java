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
package org.apache.accumulo.core.data.impl;

import java.nio.ByteBuffer;

import org.apache.accumulo.core.data.ArrayByteSequence;
import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.Key;
import org.apache.hadoop.io.Text;

import com.google.common.collect.Range;
import com.google.common.collect.RangeSet;

/**
 * Utility class for computing read boundaries for a file within a particular tablet
 */
public class TabletRange {

  private final Range<ByteSequence> bytesRange;

  public TabletRange(final KeyExtent keyExtent) {
    this(keyExtent.getPrevEndRow(), keyExtent.getEndRow());
  }

  public TabletRange(final Text previousEndRow, final Text endRow) {
    this(previousEndRow == null ? null : ByteBuffer.wrap(previousEndRow.getBytes(), 0, previousEndRow.getLength()),
        endRow == null ? null : ByteBuffer.wrap(endRow.getBytes(), 0, endRow.getLength()));
  }

  public TabletRange(final ByteBuffer previousEndRow, final ByteBuffer endRow) {
    this(previousEndRow == null ? null : new ArrayByteSequence(previousEndRow), endRow == null ? null : new ArrayByteSequence(endRow));
  }

  public TabletRange(final ByteSequence previousEndRow, final ByteSequence endRow) {
    if (previousEndRow == null && endRow == null) {
      bytesRange = Range.all();
    } else if (previousEndRow == null) {
      bytesRange = Range.atMost(endRow);
    } else if (endRow == null) {
      bytesRange = Range.greaterThan(previousEndRow);
    } else {
      bytesRange = Range.openClosed(previousEndRow, endRow);
    }
  }

  /**
   * When moving a file into a tablet, check to see if the bounds of the file fit within the tablet
   */
  public boolean containsKey(final Key first, final Key last) {
    return bytesRange.contains(first.getRowData()) || bytesRange.contains(last.getRowData());
  }

  /**
   * When adding a file to a tablet, specify the range this tablet should use in the file
   */
  public Range<ByteSequence> intersect(final Key first, final Key last) {
    return bytesRange.intersection(Range.closed(first.getRowData(), last.getRowData()));
  }

  /**
   * When a file is already included in a tablet, check to see if any of its bounds are adjacent to these new bounds
   */
  public RangeSet<ByteSequence> mergeOverlaps(RangeSet<ByteSequence> existing, Range<ByteSequence> toAdd) {
    existing.add(toAdd);
    return existing;
    // TODO create a DiscreteDomain for ByteSequence to canonicalize so coalescing works properly
  }

}
