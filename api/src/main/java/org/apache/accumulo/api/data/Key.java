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

import java.util.Optional;

public class Key {

  public interface BuildableKey {
    Key build();
  }

  public interface RowBuilder extends BuildableKey {
    ColumnFamilyBuilder row(CharSequence row);

    ColumnFamilyBuilder row(Bytes row);

    ColumnFamilyBuilder rowAfter(CharSequence row);

    ColumnFamilyBuilder rowAfter(Bytes row);
  }

  public interface ColumnFamilyBuilder extends BuildableKey {
    ColumnQualifierBuilder fam(CharSequence columnFamily);

    ColumnQualifierBuilder fam(Bytes columnFamily);

    ColumnQualifierBuilder famAfter(CharSequence columnFamily);

    ColumnQualifierBuilder famAfter(Bytes columnFamily);

  }

  public interface ColumnQualifierBuilder extends BuildableKey {
    ColumnVisibilityBuilder qual(CharSequence columnQualifier);

    ColumnVisibilityBuilder qual(Bytes columnQualifier);

    ColumnVisibilityBuilder qualAfter(CharSequence columnQualifier);

    ColumnVisibilityBuilder qualAfter(Bytes columnQualifier);
  }

  public interface ColumnVisibilityBuilder extends BuildableKey {
    VersionBuilder vis(CharSequence visibility);
  }

  public interface VersionBuilder extends BuildableKey {
    BuildableKey firstVersion();

    BuildableKey lastVersion();

    BuildableKey oldest();

    BuildableKey newest();

    BuildableKey version(long version);

  }

  public static class Builder implements RowBuilder, ColumnFamilyBuilder, ColumnQualifierBuilder, ColumnVisibilityBuilder, VersionBuilder {

    private Optional<Bytes> row = Optional.empty();
    private Optional<Bytes> cf = Optional.empty();
    private Optional<Bytes> cq = Optional.empty();
    private Optional<String> cv = Optional.empty();
    private Optional<Long> ver = Optional.empty();

    private Builder() {}

    @Override
    public ColumnFamilyBuilder row(CharSequence row) {
      return row(Bytes.from(row));
    }

    @Override
    public ColumnFamilyBuilder row(Bytes row) {
      this.row = Optional.of(row);
      return this;
    }

    @Override
    public ColumnFamilyBuilder rowAfter(CharSequence row) {
      return rowAfter(Bytes.from(row));
    }

    @Override
    public ColumnFamilyBuilder rowAfter(Bytes row) {
      // TODO Auto-generated method stub
      return null;
    }

    @Override
    public ColumnQualifierBuilder fam(CharSequence columnFamily) {
      return fam(Bytes.from(columnFamily));
    }

    @Override
    public ColumnQualifierBuilder fam(Bytes columnFamily) {
      this.cf = Optional.of(columnFamily);
      return this;
    }

    @Override
    public ColumnQualifierBuilder famAfter(CharSequence columnFamily) {
      return famAfter(Bytes.from(columnFamily));
    }

    @Override
    public ColumnQualifierBuilder famAfter(Bytes columnFamily) {
      // TODO Auto-generated method stub
      return null;
    }

    @Override
    public ColumnVisibilityBuilder qual(CharSequence columnQualifier) {
      return qual(Bytes.from(columnQualifier));
    }

    @Override
    public ColumnVisibilityBuilder qual(Bytes columnQualifier) {
      this.cq = Optional.of(columnQualifier);
      return this;
    }

    @Override
    public ColumnVisibilityBuilder qualAfter(CharSequence columnQualifier) {
      return qualAfter(Bytes.from(columnQualifier));
    }

    @Override
    public ColumnVisibilityBuilder qualAfter(Bytes columnQualifier) {
      // TODO Auto-generated method stub
      return null;
    }

    @Override
    public VersionBuilder vis(CharSequence visibility) {
      this.cv = Optional.of(visibility.toString());
      return this;
    }

    @Override
    public BuildableKey version(long version) {
      this.ver = Optional.of(version);
      return this;
    }

    @Override
    public BuildableKey firstVersion() {
      return newest();
    }

    @Override
    public BuildableKey newest() {
      this.ver = Optional.of(Long.MAX_VALUE);
      return this;
    }

    @Override
    public BuildableKey oldest() {
      this.ver = Optional.of(Long.MIN_VALUE);
      return this;
    }

    @Override
    public BuildableKey lastVersion() {
      return oldest();
    }

    @Override
    public Key build() {
      Bytes e = Bytes.empty();
      return new Key(row.orElse(e), cf.orElse(e), cq.orElse(e), cv.orElse(""), ver.orElse(Long.MAX_VALUE), false);
    }

  }

  public static RowBuilder newKey() {
    return new Builder();
  }

  private final Bytes row;
  private final Bytes cf;
  private final Bytes cq;
  private final String cv;
  private final long ver;
  private final boolean deleted;

  Key(Bytes row, Bytes cf, Bytes cq, String cv, long ver, boolean deleted) {
    this.row = row;
    this.cf = cf;
    this.cq = cq;
    this.cv = cv;
    this.ver = ver;
    this.deleted = deleted;
  }

  public Bytes getRow() {
    return row;
  }

  public Bytes getColumnFamily() {
    return cf;
  }

  public Bytes getColumnQualifier() {
    return cq;
  }

  public String getColumnVisibility() {
    return cv;
  }

  public long getVersion() {
    return ver;
  }

  protected boolean isDeleted() {
    return deleted;
  }

}
