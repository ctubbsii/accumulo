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

public class Column {

  public static class Family extends Bytes {
    public Family(CharSequence chars) {
      super(chars);
    }

    public Family(byte[] bytes, boolean copy) {
      super(bytes, copy);
    }

    public Family(byte[] bytes, int offset, int length, boolean copy) {
      super(bytes, offset, length, copy);
    }
  }

  public static class Qualifier extends Bytes {
    public Qualifier(CharSequence chars) {
      super(chars);
    }

    public Qualifier(byte[] bytes, boolean copy) {
      super(bytes, copy);
    }

    public Qualifier(byte[] bytes, int offset, int length, boolean copy) {
      super(bytes, offset, length, copy);
    }
  }

  public static class Visibility extends Bytes {
    public Visibility(CharSequence chars) {
      super(chars);
    }
  }

  private Column.Family cf;
  private Qualifier cq;
  private Visibility cv;

  public Column(Family columnFamily, Qualifier columnQualifier) {
    this(columnFamily, columnQualifier, null);
  }

  public Column(Family columnFamily, Qualifier columnQualifier, Visibility columnVisibility) {
    this.cf = columnFamily;
    this.cq = columnQualifier;
    this.cv = columnVisibility;
  }

  public Column.Family getFamily() {
    return cf;
  }

  public Column.Qualifier getQualifier() {
    return cq;
  }

  public Column.Visibility getVisibility() {
    return cv;
  }

}
