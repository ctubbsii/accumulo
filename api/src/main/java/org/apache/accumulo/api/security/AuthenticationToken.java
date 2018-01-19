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

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

import javax.security.auth.DestroyFailedException;
import javax.security.auth.Destroyable;

import com.google.common.collect.ForwardingMap;

/**
 *
 *
 * @since 2.0.0 (previously <code>org.apache.accumulo.core.client.security.tokens.AuthenticationToken</code> in 1.5.0)
 */
public interface AuthenticationToken extends Destroyable {

  public static class Properties extends ForwardingMap<String,char[]> implements Destroyable {

    private AtomicBoolean destroyed = new AtomicBoolean();
    private HashMap<String,char[]> map = new HashMap<>();

    @Override
    public void destroy() throws DestroyFailedException {
      for (Entry<String,char[]> entry : this.entrySet()) {
        char[] val = this.get(entry.getKey());
        Arrays.fill(val, (char) 0);
      }
      this.clear();
      destroyed.set(true);
    }

    @Override
    public boolean isDestroyed() {
      return destroyed.get();
    }

    public char[] put(String key, CharSequence value) {
      char[] toPut = new char[value.length()];
      for (int i = 0; i < value.length(); i++)
        toPut[i] = value.charAt(i);
      return put(key, toPut);
    }

    public void putAllStrings(Map<String,? extends CharSequence> map) {
      for (Map.Entry<String,? extends CharSequence> entry : map.entrySet()) {
        put(entry.getKey(), entry.getValue());
      }
    }

    @Override
    protected Map<String,char[]> delegate() {
      if (destroyed.get())
        throw new IllegalStateException("Cannot use a token destroyed by the user");
      return map;
    }
  }

  public static class TokenProperty implements Comparable<TokenProperty> {
    private String key, description;
    private boolean masked;

    public TokenProperty(String name, String description, boolean mask) {
      this.key = name;
      this.description = description;
      this.masked = mask;
    }

    @Override
    public int compareTo(TokenProperty o) {
      return key.compareTo(o.key);
    }

    @Override
    public boolean equals(Object o) {
      if (o instanceof TokenProperty)
        return Objects.equals(key, ((TokenProperty) o).key);
      return false;
    }

    public String getDescription() {
      return this.description;
    }

    public String getKey() {
      return this.key;
    }

    public boolean getMask() {
      return this.masked;
    }

    @Override
    public int hashCode() {
      return Objects.hashCode(key);
    }

    @Override
    public String toString() {
      return this.key + " - " + description;
    }
  }

  Set<TokenProperty> getProperties();

  void init(Properties properties);

  void readFields(DataInput in) throws IOException;

  /**
   * Writes this object's fields to a stream which can later be read into a new, equivalent object. This is needed to serialize tokens for an RPC to the server.
   *
   * @param out
   *          the output sink for fields
   * @throws IOException
   *           if there is a problem writing this object's fields
   */
  void write(DataOutput out) throws IOException;

}
