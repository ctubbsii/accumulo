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
package org.apache.accumulo.api.data;

import static java.nio.charset.StandardCharsets.UTF_8;

import java.nio.charset.Charset;
import java.util.Arrays;

import com.google.common.base.Preconditions;

public class Bytes implements Comparable<Bytes> {

  private static final Bytes EMPTY_BYTES = Bytes.wrap(new byte[0]);

  public static Bytes empty() {
    return EMPTY_BYTES;
  }

  public static int compareBytes(final Bytes bs1, final Bytes bs2) {
    int minLen = Math.min(bs1.length(), bs2.length());

    for (int i = 0; i < minLen; i++) {
      int a = (bs1.byteAt(i) & 0xff);
      int b = (bs2.byteAt(i) & 0xff);

      if (a != b) {
        return a - b;
      }
    }

    return bs1.length() - bs2.length();
  }

  public static Bytes copyOf(final byte[] bytes) {
    return copyOf(bytes, 0, bytes.length);
  }

  public static Bytes copyOf(final byte[] bytes, final int offset, final int length) {
    return new Bytes(bytes, offset, length, true);
  }

  public static Bytes from(final CharSequence str) {
    return from(str, UTF_8);
  }

  public static Bytes from(final CharSequence str, final Charset charset) {
    return wrap(str.toString().getBytes(charset));
  }

  private static Bytes wrap(byte[] bytes) {
    return wrap(bytes, 0, bytes.length);
  }

  public static Bytes wrap(byte[] bytes, final int offset, final int length) {
    return new Bytes(bytes, offset, length, false);
  }

  private final byte[] bytes;

  private final int length;

  private final int offset;

  protected Bytes(byte[] bytes, boolean copy) {
    this(bytes, 0, bytes.length, copy);
  }

  protected Bytes(byte[] bytes, int offset, int length, boolean copy) {
    int end = offset + length;
    Preconditions.checkPositionIndexes(offset, end, bytes.length);
    if (copy) {
      this.bytes = Arrays.copyOfRange(bytes, offset, end);
      this.offset = 0;
      this.length = end;
    } else {
      this.bytes = bytes;
      this.offset = offset;
      this.length = length;
    }
  }

  protected Bytes(CharSequence chars) {
    this(chars.toString().getBytes(UTF_8), false);
  }

  public byte byteAt(int i) {
    Preconditions.checkPositionIndex(i, this.length);
    return bytes[offset + i];
  }

  @Override
  public int hashCode() {
    return Arrays.hashCode(bytes);
  }

  @Override
  public boolean equals(Object obj) {
    return this == obj || (obj != null && obj instanceof Bytes && 0 == compareTo((Bytes) obj));
  }

  @Override
  public int compareTo(Bytes o) {
    return Bytes.compareBytes(this, o);
  }

  public byte[] getBytes() {
    if (isBackedByArray()) {
      return Arrays.copyOfRange(bytes, offset(), offset() + length());
    } else {
      byte[] copy = new byte[length()];
      for (int i = 0; i < copy.length; ++i) {
        copy[i] = byteAt(i);
      }
      return copy;
    }
  }

  public boolean isBackedByArray() {
    return true;
  }

  public int length() {
    return this.length;
  }

  public int offset() {
    return this.offset;
  }

}
