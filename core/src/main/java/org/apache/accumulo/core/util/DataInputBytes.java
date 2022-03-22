/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.accumulo.core.util;

import java.io.ByteArrayInputStream;
import java.io.DataInput;
import java.io.DataInputStream;
import java.io.IOException;

/**
 * This class provides a simple {@link DataInput} implementation that wraps a byte array, without
 * requiring the caller to wrap the byte array with {@link DataInputStream} and
 * {@link ByteArrayInputStream} themselves, or manage those {@link AutoCloseable} objects'
 * lifecycles in try-with-resources, since the close method on those do nothing.
 */
public class DataInputBytes implements DataInput {

  /**
   * This does not need to be closed, because {@link DataInputStream#close()} and the underlying
   * {@link ByteArrayInputStream#close()} are both NOOPs.
   */
  private final DataInputStream dis;

  private DataInputBytes(byte[] bytes, int offset, int length) {
    dis = new DataInputStream(new ByteArrayInputStream(bytes, offset, length));
  }

  public static DataInputBytes wrap(byte[] bytes) {
    return DataInputBytes.wrap(bytes, 0, bytes.length);
  }

  public static DataInputBytes wrap(byte[] bytes, int offset, int length) {
    return new DataInputBytes(bytes, offset, length);
  }

  @Override
  public void readFully(byte[] b) throws IOException {
    dis.readFully(b);
  }

  @Override
  public void readFully(byte[] b, int off, int len) throws IOException {
    dis.readFully(b, off, len);
  }

  @Override
  public int skipBytes(int n) throws IOException {
    return dis.skipBytes(n);
  }

  @Override
  public boolean readBoolean() throws IOException {
    return dis.readBoolean();
  }

  @Override
  public byte readByte() throws IOException {
    return dis.readByte();
  }

  @Override
  public int readUnsignedByte() throws IOException {
    return dis.readUnsignedByte();
  }

  @Override
  public short readShort() throws IOException {
    return dis.readShort();
  }

  @Override
  public int readUnsignedShort() throws IOException {
    return dis.readUnsignedShort();
  }

  @Override
  public char readChar() throws IOException {
    return dis.readChar();
  }

  @Override
  public int readInt() throws IOException {
    return dis.readInt();
  }

  @Override
  public long readLong() throws IOException {
    return dis.readLong();
  }

  @Override
  public float readFloat() throws IOException {
    return dis.readFloat();
  }

  @Override
  public double readDouble() throws IOException {
    return dis.readDouble();
  }

  /**
   * @deprecated because this wraps the deprecated {@link DataInputStream#readLine()}
   */
  @Deprecated
  @Override
  public String readLine() throws IOException {
    return dis.readLine();
  }

  @Override
  public String readUTF() throws IOException {
    return dis.readUTF();
  }
}
