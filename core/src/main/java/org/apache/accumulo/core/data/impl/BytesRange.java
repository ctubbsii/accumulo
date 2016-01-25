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
import java.util.Arrays;

import org.apache.accumulo.core.data.ArrayByteSequence;
import org.apache.accumulo.core.data.ByteSequence;
import org.apache.hadoop.io.Text;

import com.google.common.base.Function;
import com.google.common.base.Optional;
import com.google.common.collect.BoundType;
import com.google.common.collect.Iterables;
import com.google.common.collect.Range;
import com.google.common.collect.RangeSet;
import com.google.common.collect.TreeRangeSet;

/**
 * Utility for representing a range of lexicographically ordered byte arrays.
 */
public class BytesRange {

  /**
   * The range represented by this tablet.
   */
  private final Range<ByteSequence> bytesRange;

  /**
   * A function to intersect any range with this tablet's range.
   */
  private final Function<Range<ByteSequence>,Range<ByteSequence>> intersector;

  /**
   * Constructor for creating a range of rows for a KeyExtent. This shouldn't even really exist. KeyExtent should be composed of a tableId and a BytesRange.
   * This class shouldn't be aware of KeyExtent.
   */
  BytesRange(final KeyExtent keyExtent) {
    this(keyExtent.getPrevEndRow(), BoundType.OPEN, keyExtent.getEndRow(), BoundType.CLOSED);
  }

  /**
   * Constructor for specifying a range of by byte arrays.
   */
  public BytesRange(final byte[] lower, final BoundType lowerType, final byte[] upper, final BoundType upperType) {
    this(lower == null ? null : ByteBuffer.wrap(lower), lowerType, upper == null ? null : ByteBuffer.wrap(upper), upperType);
  }

  /**
   * Constructor for specifying a range of by byte arrays from their Text representation.
   */
  public BytesRange(final Text lower, final BoundType lowerType, final Text upper, final BoundType upperType) {
    this(lower == null ? null : ByteBuffer.wrap(lower.getBytes(), 0, lower.getLength()), lowerType, upper == null ? null : ByteBuffer.wrap(upper.getBytes(), 0,
        upper.getLength()), upperType);
  }

  /**
   * Constructor for specifying a range of by byte arrays from their ByteBuffer representation.
   */
  public BytesRange(final ByteBuffer lower, final BoundType lowerType, final ByteBuffer upper, final BoundType upperType) {
    this(lower == null ? null : new ArrayByteSequence(lower), lowerType, upper == null ? null : new ArrayByteSequence(upper), upperType);
  }

  /**
   * Constructor for specifying a range of by byte arrays from their ByteSequence representation.
   */
  public BytesRange(final ByteSequence lower, final BoundType lowerType, final ByteSequence upper, final BoundType upperType) {
    if (lower == null && upper == null) {
      bytesRange = Range.all();
    } else if (lower == null) {
      bytesRange = Range.upTo(upper, upperType);
    } else if (upper == null) {
      bytesRange = Range.downTo(lower, lowerType);
    } else {
      bytesRange = Range.range(lower, lowerType, upper, upperType);
    }
    intersector = new Function<Range<ByteSequence>,Range<ByteSequence>>() {
      @Override
      public Range<ByteSequence> apply(final Range<ByteSequence> input) {
        return input.intersection(bytesRange);
      }
    };
  }

  /**
   * When adding a file to a tablet represented by this range, merge the file's previous ranges with those in this tablet.
   *
   * @param newRanges
   *          the new ranges for the file to add to this tablet; this may be a closed range over the file's first and last row, or its previous ranges from an
   *          originating tablet during a merge
   * @param originalRanges
   *          the ranges for the same file which may already exist in this tablet
   * @return the new set of ranges to store in this tablet's metadata for the file, if non-empty
   */
  public RangeSet<ByteSequence> mergeFileRanges(final RangeSet<ByteSequence> newRanges, final Optional<RangeSet<ByteSequence>> originalRanges) {

    // seed the result with any ranges for the file already present in this tablet
    RangeSet<ByteSequence> result = TreeRangeSet.create();
    if (originalRanges.isPresent()) {
      result.addAll(originalRanges.get());
    }

    // intersect new ranges with this tablet, then coalesce them with any existing ranges
    for (Range<ByteSequence> r : Iterables.transform(newRanges.asRanges(), intersector)) {
      result.add(r);
    }

    return result;
  }

  public static void main(String[] args) {
    // start with a default tablet which spans (-inf,l]
    BytesRange tablet = new BytesRange(new ArrayByteSequence("a"), BoundType.OPEN, new ArrayByteSequence("m"), BoundType.CLOSED);

    // create a file in this tablet spanning [a,d]
    RangeSet<ByteSequence> file = TreeRangeSet.create();
    file.add(Range.<ByteSequence> encloseAll(Arrays.<ByteSequence> asList(new ArrayByteSequence("b"), new ArrayByteSequence("d"))));
    System.out.println(tablet.bytesRange + "\t" + file);

    // add a reference to the file going from (j,z]
    System.out.println("Adding (j,z]");
    RangeSet<ByteSequence> newFileRefs = TreeRangeSet.create();
    newFileRefs.add(Range.<ByteSequence> openClosed(new ArrayByteSequence("j"), new ArrayByteSequence("z")));
    file.addAll(tablet.mergeFileRanges(newFileRefs, Optional.of(file)));
    System.out.println(tablet.bytesRange + "\t" + file);

    // add a reference to the file going from (p,q]
    System.out.println("Adding (p,q]");
    newFileRefs.add(Range.<ByteSequence> openClosed(new ArrayByteSequence("p"), new ArrayByteSequence("q")));
    file.addAll(tablet.mergeFileRanges(newFileRefs, Optional.of(file)));
    System.out.println(tablet.bytesRange + "\t" + file);

    // add a reference to the file going from ("",c)
    System.out.println("Adding ('',c)");
    newFileRefs.add(Range.<ByteSequence> open(new ArrayByteSequence(""), new ArrayByteSequence("c")));
    file.addAll(tablet.mergeFileRanges(newFileRefs, Optional.of(file)));
    System.out.println(tablet.bytesRange + "\t" + file);

    // add a reference to the file going from (d,j]
    System.out.println("Adding (d,j]");
    newFileRefs.add(Range.<ByteSequence> openClosed(new ArrayByteSequence("d"), new ArrayByteSequence("j")));
    file.addAll(tablet.mergeFileRanges(newFileRefs, Optional.of(file)));
    System.out.println(tablet.bytesRange + "\t" + file);
  }

}
