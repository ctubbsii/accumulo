/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
/**
 * Autogenerated by Thrift Compiler (0.17.0)
 *
 * DO NOT EDIT UNLESS YOU ARE SURE THAT YOU KNOW WHAT YOU ARE DOING
 *  @generated
 */
package org.apache.accumulo.core.dataImpl.thrift;

@SuppressWarnings({"cast", "rawtypes", "serial", "unchecked", "unused"})
public class UpdateErrors implements org.apache.thrift.TBase<UpdateErrors, UpdateErrors._Fields>, java.io.Serializable, Cloneable, Comparable<UpdateErrors> {
  private static final org.apache.thrift.protocol.TStruct STRUCT_DESC = new org.apache.thrift.protocol.TStruct("UpdateErrors");

  private static final org.apache.thrift.protocol.TField FAILED_EXTENTS_FIELD_DESC = new org.apache.thrift.protocol.TField("failedExtents", org.apache.thrift.protocol.TType.MAP, (short)1);
  private static final org.apache.thrift.protocol.TField VIOLATION_SUMMARIES_FIELD_DESC = new org.apache.thrift.protocol.TField("violationSummaries", org.apache.thrift.protocol.TType.LIST, (short)2);
  private static final org.apache.thrift.protocol.TField AUTHORIZATION_FAILURES_FIELD_DESC = new org.apache.thrift.protocol.TField("authorizationFailures", org.apache.thrift.protocol.TType.MAP, (short)3);

  private static final org.apache.thrift.scheme.SchemeFactory STANDARD_SCHEME_FACTORY = new UpdateErrorsStandardSchemeFactory();
  private static final org.apache.thrift.scheme.SchemeFactory TUPLE_SCHEME_FACTORY = new UpdateErrorsTupleSchemeFactory();

  private @org.apache.thrift.annotation.Nullable java.util.Map<TKeyExtent,java.lang.Long> failedExtents; // required
  private @org.apache.thrift.annotation.Nullable java.util.List<TConstraintViolationSummary> violationSummaries; // required
  private @org.apache.thrift.annotation.Nullable java.util.Map<TKeyExtent,org.apache.accumulo.core.clientImpl.thrift.SecurityErrorCode> authorizationFailures; // required

  /** The set of fields this struct contains, along with convenience methods for finding and manipulating them. */
  public enum _Fields implements org.apache.thrift.TFieldIdEnum {
    FAILED_EXTENTS((short)1, "failedExtents"),
    VIOLATION_SUMMARIES((short)2, "violationSummaries"),
    AUTHORIZATION_FAILURES((short)3, "authorizationFailures");

    private static final java.util.Map<java.lang.String, _Fields> byName = new java.util.HashMap<java.lang.String, _Fields>();

    static {
      for (_Fields field : java.util.EnumSet.allOf(_Fields.class)) {
        byName.put(field.getFieldName(), field);
      }
    }

    /**
     * Find the _Fields constant that matches fieldId, or null if its not found.
     */
    @org.apache.thrift.annotation.Nullable
    public static _Fields findByThriftId(int fieldId) {
      switch(fieldId) {
        case 1: // FAILED_EXTENTS
          return FAILED_EXTENTS;
        case 2: // VIOLATION_SUMMARIES
          return VIOLATION_SUMMARIES;
        case 3: // AUTHORIZATION_FAILURES
          return AUTHORIZATION_FAILURES;
        default:
          return null;
      }
    }

    /**
     * Find the _Fields constant that matches fieldId, throwing an exception
     * if it is not found.
     */
    public static _Fields findByThriftIdOrThrow(int fieldId) {
      _Fields fields = findByThriftId(fieldId);
      if (fields == null) throw new java.lang.IllegalArgumentException("Field " + fieldId + " doesn't exist!");
      return fields;
    }

    /**
     * Find the _Fields constant that matches name, or null if its not found.
     */
    @org.apache.thrift.annotation.Nullable
    public static _Fields findByName(java.lang.String name) {
      return byName.get(name);
    }

    private final short _thriftId;
    private final java.lang.String _fieldName;

    _Fields(short thriftId, java.lang.String fieldName) {
      _thriftId = thriftId;
      _fieldName = fieldName;
    }

    @Override
    public short getThriftFieldId() {
      return _thriftId;
    }

    @Override
    public java.lang.String getFieldName() {
      return _fieldName;
    }
  }

  // isset id assignments
  public static final java.util.Map<_Fields, org.apache.thrift.meta_data.FieldMetaData> metaDataMap;
  static {
    java.util.Map<_Fields, org.apache.thrift.meta_data.FieldMetaData> tmpMap = new java.util.EnumMap<_Fields, org.apache.thrift.meta_data.FieldMetaData>(_Fields.class);
    tmpMap.put(_Fields.FAILED_EXTENTS, new org.apache.thrift.meta_data.FieldMetaData("failedExtents", org.apache.thrift.TFieldRequirementType.DEFAULT, 
        new org.apache.thrift.meta_data.MapMetaData(org.apache.thrift.protocol.TType.MAP, 
            new org.apache.thrift.meta_data.StructMetaData(org.apache.thrift.protocol.TType.STRUCT, TKeyExtent.class), 
            new org.apache.thrift.meta_data.FieldValueMetaData(org.apache.thrift.protocol.TType.I64))));
    tmpMap.put(_Fields.VIOLATION_SUMMARIES, new org.apache.thrift.meta_data.FieldMetaData("violationSummaries", org.apache.thrift.TFieldRequirementType.DEFAULT, 
        new org.apache.thrift.meta_data.ListMetaData(org.apache.thrift.protocol.TType.LIST, 
            new org.apache.thrift.meta_data.StructMetaData(org.apache.thrift.protocol.TType.STRUCT, TConstraintViolationSummary.class))));
    tmpMap.put(_Fields.AUTHORIZATION_FAILURES, new org.apache.thrift.meta_data.FieldMetaData("authorizationFailures", org.apache.thrift.TFieldRequirementType.DEFAULT, 
        new org.apache.thrift.meta_data.MapMetaData(org.apache.thrift.protocol.TType.MAP, 
            new org.apache.thrift.meta_data.StructMetaData(org.apache.thrift.protocol.TType.STRUCT, TKeyExtent.class), 
            new org.apache.thrift.meta_data.EnumMetaData(org.apache.thrift.protocol.TType.ENUM, org.apache.accumulo.core.clientImpl.thrift.SecurityErrorCode.class))));
    metaDataMap = java.util.Collections.unmodifiableMap(tmpMap);
    org.apache.thrift.meta_data.FieldMetaData.addStructMetaDataMap(UpdateErrors.class, metaDataMap);
  }

  public UpdateErrors() {
  }

  public UpdateErrors(
    java.util.Map<TKeyExtent,java.lang.Long> failedExtents,
    java.util.List<TConstraintViolationSummary> violationSummaries,
    java.util.Map<TKeyExtent,org.apache.accumulo.core.clientImpl.thrift.SecurityErrorCode> authorizationFailures)
  {
    this();
    this.failedExtents = failedExtents;
    this.violationSummaries = violationSummaries;
    this.authorizationFailures = authorizationFailures;
  }

  /**
   * Performs a deep copy on <i>other</i>.
   */
  public UpdateErrors(UpdateErrors other) {
    if (other.isSetFailedExtents()) {
      java.util.Map<TKeyExtent,java.lang.Long> __this__failedExtents = new java.util.HashMap<TKeyExtent,java.lang.Long>(other.failedExtents.size());
      for (java.util.Map.Entry<TKeyExtent, java.lang.Long> other_element : other.failedExtents.entrySet()) {

        TKeyExtent other_element_key = other_element.getKey();
        java.lang.Long other_element_value = other_element.getValue();

        TKeyExtent __this__failedExtents_copy_key = new TKeyExtent(other_element_key);

        java.lang.Long __this__failedExtents_copy_value = other_element_value;

        __this__failedExtents.put(__this__failedExtents_copy_key, __this__failedExtents_copy_value);
      }
      this.failedExtents = __this__failedExtents;
    }
    if (other.isSetViolationSummaries()) {
      java.util.List<TConstraintViolationSummary> __this__violationSummaries = new java.util.ArrayList<TConstraintViolationSummary>(other.violationSummaries.size());
      for (TConstraintViolationSummary other_element : other.violationSummaries) {
        __this__violationSummaries.add(new TConstraintViolationSummary(other_element));
      }
      this.violationSummaries = __this__violationSummaries;
    }
    if (other.isSetAuthorizationFailures()) {
      java.util.Map<TKeyExtent,org.apache.accumulo.core.clientImpl.thrift.SecurityErrorCode> __this__authorizationFailures = new java.util.HashMap<TKeyExtent,org.apache.accumulo.core.clientImpl.thrift.SecurityErrorCode>(other.authorizationFailures.size());
      for (java.util.Map.Entry<TKeyExtent, org.apache.accumulo.core.clientImpl.thrift.SecurityErrorCode> other_element : other.authorizationFailures.entrySet()) {

        TKeyExtent other_element_key = other_element.getKey();
        org.apache.accumulo.core.clientImpl.thrift.SecurityErrorCode other_element_value = other_element.getValue();

        TKeyExtent __this__authorizationFailures_copy_key = new TKeyExtent(other_element_key);

        org.apache.accumulo.core.clientImpl.thrift.SecurityErrorCode __this__authorizationFailures_copy_value = other_element_value;

        __this__authorizationFailures.put(__this__authorizationFailures_copy_key, __this__authorizationFailures_copy_value);
      }
      this.authorizationFailures = __this__authorizationFailures;
    }
  }

  @Override
  public UpdateErrors deepCopy() {
    return new UpdateErrors(this);
  }

  @Override
  public void clear() {
    this.failedExtents = null;
    this.violationSummaries = null;
    this.authorizationFailures = null;
  }

  public int getFailedExtentsSize() {
    return (this.failedExtents == null) ? 0 : this.failedExtents.size();
  }

  public void putToFailedExtents(TKeyExtent key, long val) {
    if (this.failedExtents == null) {
      this.failedExtents = new java.util.HashMap<TKeyExtent,java.lang.Long>();
    }
    this.failedExtents.put(key, val);
  }

  @org.apache.thrift.annotation.Nullable
  public java.util.Map<TKeyExtent,java.lang.Long> getFailedExtents() {
    return this.failedExtents;
  }

  public UpdateErrors setFailedExtents(@org.apache.thrift.annotation.Nullable java.util.Map<TKeyExtent,java.lang.Long> failedExtents) {
    this.failedExtents = failedExtents;
    return this;
  }

  public void unsetFailedExtents() {
    this.failedExtents = null;
  }

  /** Returns true if field failedExtents is set (has been assigned a value) and false otherwise */
  public boolean isSetFailedExtents() {
    return this.failedExtents != null;
  }

  public void setFailedExtentsIsSet(boolean value) {
    if (!value) {
      this.failedExtents = null;
    }
  }

  public int getViolationSummariesSize() {
    return (this.violationSummaries == null) ? 0 : this.violationSummaries.size();
  }

  @org.apache.thrift.annotation.Nullable
  public java.util.Iterator<TConstraintViolationSummary> getViolationSummariesIterator() {
    return (this.violationSummaries == null) ? null : this.violationSummaries.iterator();
  }

  public void addToViolationSummaries(TConstraintViolationSummary elem) {
    if (this.violationSummaries == null) {
      this.violationSummaries = new java.util.ArrayList<TConstraintViolationSummary>();
    }
    this.violationSummaries.add(elem);
  }

  @org.apache.thrift.annotation.Nullable
  public java.util.List<TConstraintViolationSummary> getViolationSummaries() {
    return this.violationSummaries;
  }

  public UpdateErrors setViolationSummaries(@org.apache.thrift.annotation.Nullable java.util.List<TConstraintViolationSummary> violationSummaries) {
    this.violationSummaries = violationSummaries;
    return this;
  }

  public void unsetViolationSummaries() {
    this.violationSummaries = null;
  }

  /** Returns true if field violationSummaries is set (has been assigned a value) and false otherwise */
  public boolean isSetViolationSummaries() {
    return this.violationSummaries != null;
  }

  public void setViolationSummariesIsSet(boolean value) {
    if (!value) {
      this.violationSummaries = null;
    }
  }

  public int getAuthorizationFailuresSize() {
    return (this.authorizationFailures == null) ? 0 : this.authorizationFailures.size();
  }

  public void putToAuthorizationFailures(TKeyExtent key, org.apache.accumulo.core.clientImpl.thrift.SecurityErrorCode val) {
    if (this.authorizationFailures == null) {
      this.authorizationFailures = new java.util.HashMap<TKeyExtent,org.apache.accumulo.core.clientImpl.thrift.SecurityErrorCode>();
    }
    this.authorizationFailures.put(key, val);
  }

  @org.apache.thrift.annotation.Nullable
  public java.util.Map<TKeyExtent,org.apache.accumulo.core.clientImpl.thrift.SecurityErrorCode> getAuthorizationFailures() {
    return this.authorizationFailures;
  }

  public UpdateErrors setAuthorizationFailures(@org.apache.thrift.annotation.Nullable java.util.Map<TKeyExtent,org.apache.accumulo.core.clientImpl.thrift.SecurityErrorCode> authorizationFailures) {
    this.authorizationFailures = authorizationFailures;
    return this;
  }

  public void unsetAuthorizationFailures() {
    this.authorizationFailures = null;
  }

  /** Returns true if field authorizationFailures is set (has been assigned a value) and false otherwise */
  public boolean isSetAuthorizationFailures() {
    return this.authorizationFailures != null;
  }

  public void setAuthorizationFailuresIsSet(boolean value) {
    if (!value) {
      this.authorizationFailures = null;
    }
  }

  @Override
  public void setFieldValue(_Fields field, @org.apache.thrift.annotation.Nullable java.lang.Object value) {
    switch (field) {
    case FAILED_EXTENTS:
      if (value == null) {
        unsetFailedExtents();
      } else {
        setFailedExtents((java.util.Map<TKeyExtent,java.lang.Long>)value);
      }
      break;

    case VIOLATION_SUMMARIES:
      if (value == null) {
        unsetViolationSummaries();
      } else {
        setViolationSummaries((java.util.List<TConstraintViolationSummary>)value);
      }
      break;

    case AUTHORIZATION_FAILURES:
      if (value == null) {
        unsetAuthorizationFailures();
      } else {
        setAuthorizationFailures((java.util.Map<TKeyExtent,org.apache.accumulo.core.clientImpl.thrift.SecurityErrorCode>)value);
      }
      break;

    }
  }

  @org.apache.thrift.annotation.Nullable
  @Override
  public java.lang.Object getFieldValue(_Fields field) {
    switch (field) {
    case FAILED_EXTENTS:
      return getFailedExtents();

    case VIOLATION_SUMMARIES:
      return getViolationSummaries();

    case AUTHORIZATION_FAILURES:
      return getAuthorizationFailures();

    }
    throw new java.lang.IllegalStateException();
  }

  /** Returns true if field corresponding to fieldID is set (has been assigned a value) and false otherwise */
  @Override
  public boolean isSet(_Fields field) {
    if (field == null) {
      throw new java.lang.IllegalArgumentException();
    }

    switch (field) {
    case FAILED_EXTENTS:
      return isSetFailedExtents();
    case VIOLATION_SUMMARIES:
      return isSetViolationSummaries();
    case AUTHORIZATION_FAILURES:
      return isSetAuthorizationFailures();
    }
    throw new java.lang.IllegalStateException();
  }

  @Override
  public boolean equals(java.lang.Object that) {
    if (that instanceof UpdateErrors)
      return this.equals((UpdateErrors)that);
    return false;
  }

  public boolean equals(UpdateErrors that) {
    if (that == null)
      return false;
    if (this == that)
      return true;

    boolean this_present_failedExtents = true && this.isSetFailedExtents();
    boolean that_present_failedExtents = true && that.isSetFailedExtents();
    if (this_present_failedExtents || that_present_failedExtents) {
      if (!(this_present_failedExtents && that_present_failedExtents))
        return false;
      if (!this.failedExtents.equals(that.failedExtents))
        return false;
    }

    boolean this_present_violationSummaries = true && this.isSetViolationSummaries();
    boolean that_present_violationSummaries = true && that.isSetViolationSummaries();
    if (this_present_violationSummaries || that_present_violationSummaries) {
      if (!(this_present_violationSummaries && that_present_violationSummaries))
        return false;
      if (!this.violationSummaries.equals(that.violationSummaries))
        return false;
    }

    boolean this_present_authorizationFailures = true && this.isSetAuthorizationFailures();
    boolean that_present_authorizationFailures = true && that.isSetAuthorizationFailures();
    if (this_present_authorizationFailures || that_present_authorizationFailures) {
      if (!(this_present_authorizationFailures && that_present_authorizationFailures))
        return false;
      if (!this.authorizationFailures.equals(that.authorizationFailures))
        return false;
    }

    return true;
  }

  @Override
  public int hashCode() {
    int hashCode = 1;

    hashCode = hashCode * 8191 + ((isSetFailedExtents()) ? 131071 : 524287);
    if (isSetFailedExtents())
      hashCode = hashCode * 8191 + failedExtents.hashCode();

    hashCode = hashCode * 8191 + ((isSetViolationSummaries()) ? 131071 : 524287);
    if (isSetViolationSummaries())
      hashCode = hashCode * 8191 + violationSummaries.hashCode();

    hashCode = hashCode * 8191 + ((isSetAuthorizationFailures()) ? 131071 : 524287);
    if (isSetAuthorizationFailures())
      hashCode = hashCode * 8191 + authorizationFailures.hashCode();

    return hashCode;
  }

  @Override
  public int compareTo(UpdateErrors other) {
    if (!getClass().equals(other.getClass())) {
      return getClass().getName().compareTo(other.getClass().getName());
    }

    int lastComparison = 0;

    lastComparison = java.lang.Boolean.compare(isSetFailedExtents(), other.isSetFailedExtents());
    if (lastComparison != 0) {
      return lastComparison;
    }
    if (isSetFailedExtents()) {
      lastComparison = org.apache.thrift.TBaseHelper.compareTo(this.failedExtents, other.failedExtents);
      if (lastComparison != 0) {
        return lastComparison;
      }
    }
    lastComparison = java.lang.Boolean.compare(isSetViolationSummaries(), other.isSetViolationSummaries());
    if (lastComparison != 0) {
      return lastComparison;
    }
    if (isSetViolationSummaries()) {
      lastComparison = org.apache.thrift.TBaseHelper.compareTo(this.violationSummaries, other.violationSummaries);
      if (lastComparison != 0) {
        return lastComparison;
      }
    }
    lastComparison = java.lang.Boolean.compare(isSetAuthorizationFailures(), other.isSetAuthorizationFailures());
    if (lastComparison != 0) {
      return lastComparison;
    }
    if (isSetAuthorizationFailures()) {
      lastComparison = org.apache.thrift.TBaseHelper.compareTo(this.authorizationFailures, other.authorizationFailures);
      if (lastComparison != 0) {
        return lastComparison;
      }
    }
    return 0;
  }

  @org.apache.thrift.annotation.Nullable
  @Override
  public _Fields fieldForId(int fieldId) {
    return _Fields.findByThriftId(fieldId);
  }

  @Override
  public void read(org.apache.thrift.protocol.TProtocol iprot) throws org.apache.thrift.TException {
    scheme(iprot).read(iprot, this);
  }

  @Override
  public void write(org.apache.thrift.protocol.TProtocol oprot) throws org.apache.thrift.TException {
    scheme(oprot).write(oprot, this);
  }

  @Override
  public java.lang.String toString() {
    java.lang.StringBuilder sb = new java.lang.StringBuilder("UpdateErrors(");
    boolean first = true;

    sb.append("failedExtents:");
    if (this.failedExtents == null) {
      sb.append("null");
    } else {
      sb.append(this.failedExtents);
    }
    first = false;
    if (!first) sb.append(", ");
    sb.append("violationSummaries:");
    if (this.violationSummaries == null) {
      sb.append("null");
    } else {
      sb.append(this.violationSummaries);
    }
    first = false;
    if (!first) sb.append(", ");
    sb.append("authorizationFailures:");
    if (this.authorizationFailures == null) {
      sb.append("null");
    } else {
      sb.append(this.authorizationFailures);
    }
    first = false;
    sb.append(")");
    return sb.toString();
  }

  public void validate() throws org.apache.thrift.TException {
    // check for required fields
    // check for sub-struct validity
  }

  private void writeObject(java.io.ObjectOutputStream out) throws java.io.IOException {
    try {
      write(new org.apache.thrift.protocol.TCompactProtocol(new org.apache.thrift.transport.TIOStreamTransport(out)));
    } catch (org.apache.thrift.TException te) {
      throw new java.io.IOException(te);
    }
  }

  private void readObject(java.io.ObjectInputStream in) throws java.io.IOException, java.lang.ClassNotFoundException {
    try {
      read(new org.apache.thrift.protocol.TCompactProtocol(new org.apache.thrift.transport.TIOStreamTransport(in)));
    } catch (org.apache.thrift.TException te) {
      throw new java.io.IOException(te);
    }
  }

  private static class UpdateErrorsStandardSchemeFactory implements org.apache.thrift.scheme.SchemeFactory {
    @Override
    public UpdateErrorsStandardScheme getScheme() {
      return new UpdateErrorsStandardScheme();
    }
  }

  private static class UpdateErrorsStandardScheme extends org.apache.thrift.scheme.StandardScheme<UpdateErrors> {

    @Override
    public void read(org.apache.thrift.protocol.TProtocol iprot, UpdateErrors struct) throws org.apache.thrift.TException {
      org.apache.thrift.protocol.TField schemeField;
      iprot.readStructBegin();
      while (true)
      {
        schemeField = iprot.readFieldBegin();
        if (schemeField.type == org.apache.thrift.protocol.TType.STOP) { 
          break;
        }
        switch (schemeField.id) {
          case 1: // FAILED_EXTENTS
            if (schemeField.type == org.apache.thrift.protocol.TType.MAP) {
              {
                org.apache.thrift.protocol.TMap _map58 = iprot.readMapBegin();
                struct.failedExtents = new java.util.HashMap<TKeyExtent,java.lang.Long>(2*_map58.size);
                @org.apache.thrift.annotation.Nullable TKeyExtent _key59;
                long _val60;
                for (int _i61 = 0; _i61 < _map58.size; ++_i61)
                {
                  _key59 = new TKeyExtent();
                  _key59.read(iprot);
                  _val60 = iprot.readI64();
                  struct.failedExtents.put(_key59, _val60);
                }
                iprot.readMapEnd();
              }
              struct.setFailedExtentsIsSet(true);
            } else { 
              org.apache.thrift.protocol.TProtocolUtil.skip(iprot, schemeField.type);
            }
            break;
          case 2: // VIOLATION_SUMMARIES
            if (schemeField.type == org.apache.thrift.protocol.TType.LIST) {
              {
                org.apache.thrift.protocol.TList _list62 = iprot.readListBegin();
                struct.violationSummaries = new java.util.ArrayList<TConstraintViolationSummary>(_list62.size);
                @org.apache.thrift.annotation.Nullable TConstraintViolationSummary _elem63;
                for (int _i64 = 0; _i64 < _list62.size; ++_i64)
                {
                  _elem63 = new TConstraintViolationSummary();
                  _elem63.read(iprot);
                  struct.violationSummaries.add(_elem63);
                }
                iprot.readListEnd();
              }
              struct.setViolationSummariesIsSet(true);
            } else { 
              org.apache.thrift.protocol.TProtocolUtil.skip(iprot, schemeField.type);
            }
            break;
          case 3: // AUTHORIZATION_FAILURES
            if (schemeField.type == org.apache.thrift.protocol.TType.MAP) {
              {
                org.apache.thrift.protocol.TMap _map65 = iprot.readMapBegin();
                struct.authorizationFailures = new java.util.HashMap<TKeyExtent,org.apache.accumulo.core.clientImpl.thrift.SecurityErrorCode>(2*_map65.size);
                @org.apache.thrift.annotation.Nullable TKeyExtent _key66;
                @org.apache.thrift.annotation.Nullable org.apache.accumulo.core.clientImpl.thrift.SecurityErrorCode _val67;
                for (int _i68 = 0; _i68 < _map65.size; ++_i68)
                {
                  _key66 = new TKeyExtent();
                  _key66.read(iprot);
                  _val67 = org.apache.accumulo.core.clientImpl.thrift.SecurityErrorCode.findByValue(iprot.readI32());
                  struct.authorizationFailures.put(_key66, _val67);
                }
                iprot.readMapEnd();
              }
              struct.setAuthorizationFailuresIsSet(true);
            } else { 
              org.apache.thrift.protocol.TProtocolUtil.skip(iprot, schemeField.type);
            }
            break;
          default:
            org.apache.thrift.protocol.TProtocolUtil.skip(iprot, schemeField.type);
        }
        iprot.readFieldEnd();
      }
      iprot.readStructEnd();

      // check for required fields of primitive type, which can't be checked in the validate method
      struct.validate();
    }

    @Override
    public void write(org.apache.thrift.protocol.TProtocol oprot, UpdateErrors struct) throws org.apache.thrift.TException {
      struct.validate();

      oprot.writeStructBegin(STRUCT_DESC);
      if (struct.failedExtents != null) {
        oprot.writeFieldBegin(FAILED_EXTENTS_FIELD_DESC);
        {
          oprot.writeMapBegin(new org.apache.thrift.protocol.TMap(org.apache.thrift.protocol.TType.STRUCT, org.apache.thrift.protocol.TType.I64, struct.failedExtents.size()));
          for (java.util.Map.Entry<TKeyExtent, java.lang.Long> _iter69 : struct.failedExtents.entrySet())
          {
            _iter69.getKey().write(oprot);
            oprot.writeI64(_iter69.getValue());
          }
          oprot.writeMapEnd();
        }
        oprot.writeFieldEnd();
      }
      if (struct.violationSummaries != null) {
        oprot.writeFieldBegin(VIOLATION_SUMMARIES_FIELD_DESC);
        {
          oprot.writeListBegin(new org.apache.thrift.protocol.TList(org.apache.thrift.protocol.TType.STRUCT, struct.violationSummaries.size()));
          for (TConstraintViolationSummary _iter70 : struct.violationSummaries)
          {
            _iter70.write(oprot);
          }
          oprot.writeListEnd();
        }
        oprot.writeFieldEnd();
      }
      if (struct.authorizationFailures != null) {
        oprot.writeFieldBegin(AUTHORIZATION_FAILURES_FIELD_DESC);
        {
          oprot.writeMapBegin(new org.apache.thrift.protocol.TMap(org.apache.thrift.protocol.TType.STRUCT, org.apache.thrift.protocol.TType.I32, struct.authorizationFailures.size()));
          for (java.util.Map.Entry<TKeyExtent, org.apache.accumulo.core.clientImpl.thrift.SecurityErrorCode> _iter71 : struct.authorizationFailures.entrySet())
          {
            _iter71.getKey().write(oprot);
            oprot.writeI32(_iter71.getValue().getValue());
          }
          oprot.writeMapEnd();
        }
        oprot.writeFieldEnd();
      }
      oprot.writeFieldStop();
      oprot.writeStructEnd();
    }

  }

  private static class UpdateErrorsTupleSchemeFactory implements org.apache.thrift.scheme.SchemeFactory {
    @Override
    public UpdateErrorsTupleScheme getScheme() {
      return new UpdateErrorsTupleScheme();
    }
  }

  private static class UpdateErrorsTupleScheme extends org.apache.thrift.scheme.TupleScheme<UpdateErrors> {

    @Override
    public void write(org.apache.thrift.protocol.TProtocol prot, UpdateErrors struct) throws org.apache.thrift.TException {
      org.apache.thrift.protocol.TTupleProtocol oprot = (org.apache.thrift.protocol.TTupleProtocol) prot;
      java.util.BitSet optionals = new java.util.BitSet();
      if (struct.isSetFailedExtents()) {
        optionals.set(0);
      }
      if (struct.isSetViolationSummaries()) {
        optionals.set(1);
      }
      if (struct.isSetAuthorizationFailures()) {
        optionals.set(2);
      }
      oprot.writeBitSet(optionals, 3);
      if (struct.isSetFailedExtents()) {
        {
          oprot.writeI32(struct.failedExtents.size());
          for (java.util.Map.Entry<TKeyExtent, java.lang.Long> _iter72 : struct.failedExtents.entrySet())
          {
            _iter72.getKey().write(oprot);
            oprot.writeI64(_iter72.getValue());
          }
        }
      }
      if (struct.isSetViolationSummaries()) {
        {
          oprot.writeI32(struct.violationSummaries.size());
          for (TConstraintViolationSummary _iter73 : struct.violationSummaries)
          {
            _iter73.write(oprot);
          }
        }
      }
      if (struct.isSetAuthorizationFailures()) {
        {
          oprot.writeI32(struct.authorizationFailures.size());
          for (java.util.Map.Entry<TKeyExtent, org.apache.accumulo.core.clientImpl.thrift.SecurityErrorCode> _iter74 : struct.authorizationFailures.entrySet())
          {
            _iter74.getKey().write(oprot);
            oprot.writeI32(_iter74.getValue().getValue());
          }
        }
      }
    }

    @Override
    public void read(org.apache.thrift.protocol.TProtocol prot, UpdateErrors struct) throws org.apache.thrift.TException {
      org.apache.thrift.protocol.TTupleProtocol iprot = (org.apache.thrift.protocol.TTupleProtocol) prot;
      java.util.BitSet incoming = iprot.readBitSet(3);
      if (incoming.get(0)) {
        {
          org.apache.thrift.protocol.TMap _map75 = iprot.readMapBegin(org.apache.thrift.protocol.TType.STRUCT, org.apache.thrift.protocol.TType.I64); 
          struct.failedExtents = new java.util.HashMap<TKeyExtent,java.lang.Long>(2*_map75.size);
          @org.apache.thrift.annotation.Nullable TKeyExtent _key76;
          long _val77;
          for (int _i78 = 0; _i78 < _map75.size; ++_i78)
          {
            _key76 = new TKeyExtent();
            _key76.read(iprot);
            _val77 = iprot.readI64();
            struct.failedExtents.put(_key76, _val77);
          }
        }
        struct.setFailedExtentsIsSet(true);
      }
      if (incoming.get(1)) {
        {
          org.apache.thrift.protocol.TList _list79 = iprot.readListBegin(org.apache.thrift.protocol.TType.STRUCT);
          struct.violationSummaries = new java.util.ArrayList<TConstraintViolationSummary>(_list79.size);
          @org.apache.thrift.annotation.Nullable TConstraintViolationSummary _elem80;
          for (int _i81 = 0; _i81 < _list79.size; ++_i81)
          {
            _elem80 = new TConstraintViolationSummary();
            _elem80.read(iprot);
            struct.violationSummaries.add(_elem80);
          }
        }
        struct.setViolationSummariesIsSet(true);
      }
      if (incoming.get(2)) {
        {
          org.apache.thrift.protocol.TMap _map82 = iprot.readMapBegin(org.apache.thrift.protocol.TType.STRUCT, org.apache.thrift.protocol.TType.I32); 
          struct.authorizationFailures = new java.util.HashMap<TKeyExtent,org.apache.accumulo.core.clientImpl.thrift.SecurityErrorCode>(2*_map82.size);
          @org.apache.thrift.annotation.Nullable TKeyExtent _key83;
          @org.apache.thrift.annotation.Nullable org.apache.accumulo.core.clientImpl.thrift.SecurityErrorCode _val84;
          for (int _i85 = 0; _i85 < _map82.size; ++_i85)
          {
            _key83 = new TKeyExtent();
            _key83.read(iprot);
            _val84 = org.apache.accumulo.core.clientImpl.thrift.SecurityErrorCode.findByValue(iprot.readI32());
            struct.authorizationFailures.put(_key83, _val84);
          }
        }
        struct.setAuthorizationFailuresIsSet(true);
      }
    }
  }

  private static <S extends org.apache.thrift.scheme.IScheme> S scheme(org.apache.thrift.protocol.TProtocol proto) {
    return (org.apache.thrift.scheme.StandardScheme.class.equals(proto.getScheme()) ? STANDARD_SCHEME_FACTORY : TUPLE_SCHEME_FACTORY).getScheme();
  }
  private static void unusedMethod() {}
}

