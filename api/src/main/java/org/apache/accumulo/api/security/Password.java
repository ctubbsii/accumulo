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
package org.apache.accumulo.api.security;

import static java.nio.charset.StandardCharsets.UTF_8;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.util.Arrays;
import java.util.Base64;
import java.util.Collections;
import java.util.Set;

import javax.security.auth.DestroyFailedException;

public class Password implements AuthenticationToken {

  private byte[] password = null;

  public byte[] getPassword() {
    return Arrays.copyOf(password, password.length);
  }

  /**
   * Constructor for deserializing from an input stream. Use with {@link #readFields(DataInput)}.
   */
  public Password() {
    password = new byte[0];
  }

  /**
   * Constructs a token from a copy of the password. Destroying the argument after construction will not destroy the copy in this token, and destroying this
   * token will only destroy the copy held inside this token, not the argument.
   *
   * Password tokens created with this constructor will store the password as UTF-8 bytes.
   */
  public Password(CharSequence password) {
    setPassword(CharBuffer.wrap(password)); // read only buffer
  }

  /**
   * Constructs a token from a copy of the password. Destroying the argument after construction will not destroy the copy in this token, and destroying this
   * token will only destroy the copy held inside this token, not the argument.
   */
  public Password(byte[] password) {
    this.password = Arrays.copyOf(password, password.length);
  }

  /**
   * Constructs a token from a copy of the password. This will read the ByteBuffer's remaining bytes from the current position. If the user wishes to reuse the
   * buffer, they should take care to record the current position with {@link ByteBuffer#mark()} before calling this method, and call {@link ByteBuffer#reset()}
   * afterwards.
   *
   * <p>
   * Destroying the argument after construction will not destroy the copy in this token, and destroying this token will only destroy the copy held inside this
   * token, not the argument.
   */
  public Password(ByteBuffer password) {
    password.get(this.password = new byte[password.remaining()]);
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    password = Base64.getDecoder().decode(in.readUTF().getBytes(UTF_8));
  }

  @Override
  public void write(DataOutput out) throws IOException {
    out.writeUTF(new String(Base64.getEncoder().encode(password), UTF_8));
  }

  @Override
  public void destroy() throws DestroyFailedException {
    Arrays.fill(password, (byte) 0x00);
    password = null;
  }

  @Override
  public boolean isDestroyed() {
    return password == null;
  }

  @Override
  public int hashCode() {
    return Arrays.hashCode(password);
  }

  @Override
  public boolean equals(Object obj) {
    return this == obj || (obj != null && obj instanceof Password && Arrays.equals(password, ((Password) obj).password));
  }

  private void setPassword(CharBuffer charBuffer) {
    // encode() kicks back a C-string, which is not compatible with the old passwording system
    ByteBuffer bb = UTF_8.encode(charBuffer);
    // create array using byte buffer length
    this.password = new byte[bb.remaining()];
    bb.get(this.password);
    if (!bb.isReadOnly()) {
      // clear byte buffer
      bb.rewind();
      while (bb.remaining() > 0) {
        bb.put((byte) 0);
      }
    }
  }

  @Override
  public void init(Properties properties) {
    if (properties.containsKey("password")) {
      setPassword(CharBuffer.wrap(properties.get("password")));
    } else
      throw new IllegalArgumentException("Missing 'password' property");
  }

  @Override
  public Set<TokenProperty> getProperties() {
    return Collections.singleton(new TokenProperty("password", "the password for the principal", true));
  }
}
