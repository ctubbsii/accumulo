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
import java.util.Base64;
import java.util.Objects;
import java.util.Optional;

import com.google.common.io.ByteArrayDataOutput;
import com.google.common.io.ByteStreams;

/**
 * This class wraps a user principal and authentication token for use in the API, in a non-serialized form. This object is used to carry this information only,
 * and does not validate them. They will be validated upon initiating some action in the API that requires authentication.
 *
 * <p>
 * Only references to the provided credentials are kept. They are not copied, so that the caller retains control over their authentication token and can destroy
 * it, if necessary, in order to revoke privileges. When the {@link AuthenticationToken#destroy()} method is called on the token provided, this user's
 * credentials will no longer be valid, and any subsequent client operations attempted to be performed using these credentials will fail (see ACCUMULO-1312).
 * Any RPC operations already initiated before the token is destroyed will still proceed, however.
 *
 * @since 2.0.0
 */
public final class User {

  private static final String ABSENT = "-";
  private static final String DELIM = ":";
  private static final Base64.Decoder DECODER = Base64.getDecoder();
  private static final Base64.Encoder ENCODER = Base64.getEncoder();

  /**
   * Creates a new credentials object.
   *
   * @param principal
   *          unique identifier for the entity (e.g. a user or service) authorized for these credentials
   * @param token
   *          authentication token used to prove that the principal for these credentials has been properly verified
   * @return a container representing the user's credentials
   */
  public static User identifiedWith(final String principal, final AuthenticationToken token) {
    return new User(Optional.ofNullable(principal), Optional.ofNullable(token));
  }

  public static User fromSerialized(final DataInput in) throws IOException {
    try {
      String serializedForm = in.readLine();
      String[] split = serializedForm.split(DELIM, 3);
      String principal = split[0].equals(ABSENT) ? null : new String(DECODER.decode(split[0]), UTF_8);
      String tokenType = split[1].equals(ABSENT) ? null : new String(DECODER.decode(split[1]), UTF_8);
      AuthenticationToken token = null;
      if (tokenType != null) {
        byte[] tokenBytes = DECODER.decode(split[2]);
        try {
          token = Class.forName(tokenType).asSubclass(AuthenticationToken.class).newInstance();
        } catch (InstantiationException | IllegalAccessException | ClassNotFoundException e) {
          throw new IOException(tokenType + " cannot be instantiated.", e);
        }
        token.readFields(ByteStreams.newDataInput(tokenBytes));
      }
      return new User(Optional.ofNullable(principal), Optional.ofNullable(token));
    } catch (Exception e) {
      throw new IOException("Unable to deserialize credentials", e);
    }
  }

  private final Optional<String> principal;
  private final Optional<AuthenticationToken> token;
  private String toString;

  private User(final Optional<String> principal, final Optional<AuthenticationToken> token) {
    this.principal = principal;
    this.token = token;
  }

  @Override
  public boolean equals(Object obj) {
    if (obj == null || !(obj instanceof User))
      return false;
    User other = User.class.cast(obj);
    return Objects.equals(getPrincipal(), other.getPrincipal()) && Objects.equals(getToken(), other.getToken());
  }

  /**
   * Gets the principal. This principal should represent a unique identifier for the entity (e.g. a user or service) authorized to use these credentials.
   *
   * @return the principal or null, if not set
   */
  public String getPrincipal() {
    return principal.orElse(null);
  }

  /**
   * Gets the authentication token. This token is used to prove that the caller represents the given principal.
   *
   * @return the token or null, if not set
   */
  public AuthenticationToken getToken() {
    return token.orElse(null);
  }

  @Override
  public int hashCode() {
    return Objects.hash(getPrincipal(), getToken());
  }

  @Override
  public String toString() {
    if (toString == null) {
      toString = com.google.common.base.Objects.toStringHelper(this).add("principal", principal.orElse(ABSENT))
          .add("token", token.isPresent() ? getToken().getClass().getSimpleName() + DELIM + "<hidden>" : ABSENT).toString();
    }
    return toString;
  }

  /**
   * This implementation writes credentials in the format: <code>principal:tokenClass:tokenBytes</code>, followed by a newline character (\n), where each
   * component (principal, tokenClass, and tokenBytes) are first base64-encoded. The <code>tokenBytes</code> are the result of the token serializing itself,
   * before base64-encoding.
   *
   * @see #fromSerialized(DataInput)
   * @throws IOException
   *           if there is any error in serialization
   */
  public void write(DataOutput out) throws IOException {
    try {
      String s = principal.isPresent() ? new String(ENCODER.encode(principal.get().getBytes(UTF_8)), UTF_8) : ABSENT;
      s += DELIM;
      if (token.isPresent()) {
        ByteArrayDataOutput tokenOut = ByteStreams.newDataOutput(128);
        token.get().write(tokenOut);
        tokenOut.toByteArray();
        s += new String(ENCODER.encode(token.get().getClass().getName().getBytes(UTF_8)), UTF_8);
        s += DELIM;
        s += new String(ENCODER.encode(tokenOut.toByteArray()), UTF_8);
      } else {
        s += ABSENT + DELIM + ABSENT;
      }
      s += "\n";
      byte[] serializedBytes = s.getBytes(UTF_8);
      if (token.isPresent() && token.get().isDestroyed()) {
        throw new IllegalStateException("Authentication token is destroyed");
      }
      out.write(serializedBytes, 0, serializedBytes.length);
    } catch (Exception e) {
      throw new IOException("Unable to serialize credentials", e);
    }
  }
}
