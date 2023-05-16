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
public class TConditionalSession implements org.apache.thrift.TBase<TConditionalSession, TConditionalSession._Fields>, java.io.Serializable, Cloneable, Comparable<TConditionalSession> {
  private static final org.apache.thrift.protocol.TStruct STRUCT_DESC = new org.apache.thrift.protocol.TStruct("TConditionalSession");

  private static final org.apache.thrift.protocol.TField SESSION_ID_FIELD_DESC = new org.apache.thrift.protocol.TField("sessionId", org.apache.thrift.protocol.TType.I64, (short)1);
  private static final org.apache.thrift.protocol.TField TSERVER_LOCK_FIELD_DESC = new org.apache.thrift.protocol.TField("tserverLock", org.apache.thrift.protocol.TType.STRING, (short)2);
  private static final org.apache.thrift.protocol.TField TTL_FIELD_DESC = new org.apache.thrift.protocol.TField("ttl", org.apache.thrift.protocol.TType.I64, (short)3);

  private static final org.apache.thrift.scheme.SchemeFactory STANDARD_SCHEME_FACTORY = new TConditionalSessionStandardSchemeFactory();
  private static final org.apache.thrift.scheme.SchemeFactory TUPLE_SCHEME_FACTORY = new TConditionalSessionTupleSchemeFactory();

  private long sessionId; // required
  private @org.apache.thrift.annotation.Nullable java.lang.String tserverLock; // required
  private long ttl; // required

  /** The set of fields this struct contains, along with convenience methods for finding and manipulating them. */
  public enum _Fields implements org.apache.thrift.TFieldIdEnum {
    SESSION_ID((short)1, "sessionId"),
    TSERVER_LOCK((short)2, "tserverLock"),
    TTL((short)3, "ttl");

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
        case 1: // SESSION_ID
          return SESSION_ID;
        case 2: // TSERVER_LOCK
          return TSERVER_LOCK;
        case 3: // TTL
          return TTL;
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
  private static final int __SESSIONID_ISSET_ID = 0;
  private static final int __TTL_ISSET_ID = 1;
  private byte __isset_bitfield = 0;
  public static final java.util.Map<_Fields, org.apache.thrift.meta_data.FieldMetaData> metaDataMap;
  static {
    java.util.Map<_Fields, org.apache.thrift.meta_data.FieldMetaData> tmpMap = new java.util.EnumMap<_Fields, org.apache.thrift.meta_data.FieldMetaData>(_Fields.class);
    tmpMap.put(_Fields.SESSION_ID, new org.apache.thrift.meta_data.FieldMetaData("sessionId", org.apache.thrift.TFieldRequirementType.DEFAULT, 
        new org.apache.thrift.meta_data.FieldValueMetaData(org.apache.thrift.protocol.TType.I64)));
    tmpMap.put(_Fields.TSERVER_LOCK, new org.apache.thrift.meta_data.FieldMetaData("tserverLock", org.apache.thrift.TFieldRequirementType.DEFAULT, 
        new org.apache.thrift.meta_data.FieldValueMetaData(org.apache.thrift.protocol.TType.STRING)));
    tmpMap.put(_Fields.TTL, new org.apache.thrift.meta_data.FieldMetaData("ttl", org.apache.thrift.TFieldRequirementType.DEFAULT, 
        new org.apache.thrift.meta_data.FieldValueMetaData(org.apache.thrift.protocol.TType.I64)));
    metaDataMap = java.util.Collections.unmodifiableMap(tmpMap);
    org.apache.thrift.meta_data.FieldMetaData.addStructMetaDataMap(TConditionalSession.class, metaDataMap);
  }

  public TConditionalSession() {
  }

  public TConditionalSession(
    long sessionId,
    java.lang.String tserverLock,
    long ttl)
  {
    this();
    this.sessionId = sessionId;
    setSessionIdIsSet(true);
    this.tserverLock = tserverLock;
    this.ttl = ttl;
    setTtlIsSet(true);
  }

  /**
   * Performs a deep copy on <i>other</i>.
   */
  public TConditionalSession(TConditionalSession other) {
    __isset_bitfield = other.__isset_bitfield;
    this.sessionId = other.sessionId;
    if (other.isSetTserverLock()) {
      this.tserverLock = other.tserverLock;
    }
    this.ttl = other.ttl;
  }

  @Override
  public TConditionalSession deepCopy() {
    return new TConditionalSession(this);
  }

  @Override
  public void clear() {
    setSessionIdIsSet(false);
    this.sessionId = 0;
    this.tserverLock = null;
    setTtlIsSet(false);
    this.ttl = 0;
  }

  public long getSessionId() {
    return this.sessionId;
  }

  public TConditionalSession setSessionId(long sessionId) {
    this.sessionId = sessionId;
    setSessionIdIsSet(true);
    return this;
  }

  public void unsetSessionId() {
    __isset_bitfield = org.apache.thrift.EncodingUtils.clearBit(__isset_bitfield, __SESSIONID_ISSET_ID);
  }

  /** Returns true if field sessionId is set (has been assigned a value) and false otherwise */
  public boolean isSetSessionId() {
    return org.apache.thrift.EncodingUtils.testBit(__isset_bitfield, __SESSIONID_ISSET_ID);
  }

  public void setSessionIdIsSet(boolean value) {
    __isset_bitfield = org.apache.thrift.EncodingUtils.setBit(__isset_bitfield, __SESSIONID_ISSET_ID, value);
  }

  @org.apache.thrift.annotation.Nullable
  public java.lang.String getTserverLock() {
    return this.tserverLock;
  }

  public TConditionalSession setTserverLock(@org.apache.thrift.annotation.Nullable java.lang.String tserverLock) {
    this.tserverLock = tserverLock;
    return this;
  }

  public void unsetTserverLock() {
    this.tserverLock = null;
  }

  /** Returns true if field tserverLock is set (has been assigned a value) and false otherwise */
  public boolean isSetTserverLock() {
    return this.tserverLock != null;
  }

  public void setTserverLockIsSet(boolean value) {
    if (!value) {
      this.tserverLock = null;
    }
  }

  public long getTtl() {
    return this.ttl;
  }

  public TConditionalSession setTtl(long ttl) {
    this.ttl = ttl;
    setTtlIsSet(true);
    return this;
  }

  public void unsetTtl() {
    __isset_bitfield = org.apache.thrift.EncodingUtils.clearBit(__isset_bitfield, __TTL_ISSET_ID);
  }

  /** Returns true if field ttl is set (has been assigned a value) and false otherwise */
  public boolean isSetTtl() {
    return org.apache.thrift.EncodingUtils.testBit(__isset_bitfield, __TTL_ISSET_ID);
  }

  public void setTtlIsSet(boolean value) {
    __isset_bitfield = org.apache.thrift.EncodingUtils.setBit(__isset_bitfield, __TTL_ISSET_ID, value);
  }

  @Override
  public void setFieldValue(_Fields field, @org.apache.thrift.annotation.Nullable java.lang.Object value) {
    switch (field) {
    case SESSION_ID:
      if (value == null) {
        unsetSessionId();
      } else {
        setSessionId((java.lang.Long)value);
      }
      break;

    case TSERVER_LOCK:
      if (value == null) {
        unsetTserverLock();
      } else {
        setTserverLock((java.lang.String)value);
      }
      break;

    case TTL:
      if (value == null) {
        unsetTtl();
      } else {
        setTtl((java.lang.Long)value);
      }
      break;

    }
  }

  @org.apache.thrift.annotation.Nullable
  @Override
  public java.lang.Object getFieldValue(_Fields field) {
    switch (field) {
    case SESSION_ID:
      return getSessionId();

    case TSERVER_LOCK:
      return getTserverLock();

    case TTL:
      return getTtl();

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
    case SESSION_ID:
      return isSetSessionId();
    case TSERVER_LOCK:
      return isSetTserverLock();
    case TTL:
      return isSetTtl();
    }
    throw new java.lang.IllegalStateException();
  }

  @Override
  public boolean equals(java.lang.Object that) {
    if (that instanceof TConditionalSession)
      return this.equals((TConditionalSession)that);
    return false;
  }

  public boolean equals(TConditionalSession that) {
    if (that == null)
      return false;
    if (this == that)
      return true;

    boolean this_present_sessionId = true;
    boolean that_present_sessionId = true;
    if (this_present_sessionId || that_present_sessionId) {
      if (!(this_present_sessionId && that_present_sessionId))
        return false;
      if (this.sessionId != that.sessionId)
        return false;
    }

    boolean this_present_tserverLock = true && this.isSetTserverLock();
    boolean that_present_tserverLock = true && that.isSetTserverLock();
    if (this_present_tserverLock || that_present_tserverLock) {
      if (!(this_present_tserverLock && that_present_tserverLock))
        return false;
      if (!this.tserverLock.equals(that.tserverLock))
        return false;
    }

    boolean this_present_ttl = true;
    boolean that_present_ttl = true;
    if (this_present_ttl || that_present_ttl) {
      if (!(this_present_ttl && that_present_ttl))
        return false;
      if (this.ttl != that.ttl)
        return false;
    }

    return true;
  }

  @Override
  public int hashCode() {
    int hashCode = 1;

    hashCode = hashCode * 8191 + org.apache.thrift.TBaseHelper.hashCode(sessionId);

    hashCode = hashCode * 8191 + ((isSetTserverLock()) ? 131071 : 524287);
    if (isSetTserverLock())
      hashCode = hashCode * 8191 + tserverLock.hashCode();

    hashCode = hashCode * 8191 + org.apache.thrift.TBaseHelper.hashCode(ttl);

    return hashCode;
  }

  @Override
  public int compareTo(TConditionalSession other) {
    if (!getClass().equals(other.getClass())) {
      return getClass().getName().compareTo(other.getClass().getName());
    }

    int lastComparison = 0;

    lastComparison = java.lang.Boolean.compare(isSetSessionId(), other.isSetSessionId());
    if (lastComparison != 0) {
      return lastComparison;
    }
    if (isSetSessionId()) {
      lastComparison = org.apache.thrift.TBaseHelper.compareTo(this.sessionId, other.sessionId);
      if (lastComparison != 0) {
        return lastComparison;
      }
    }
    lastComparison = java.lang.Boolean.compare(isSetTserverLock(), other.isSetTserverLock());
    if (lastComparison != 0) {
      return lastComparison;
    }
    if (isSetTserverLock()) {
      lastComparison = org.apache.thrift.TBaseHelper.compareTo(this.tserverLock, other.tserverLock);
      if (lastComparison != 0) {
        return lastComparison;
      }
    }
    lastComparison = java.lang.Boolean.compare(isSetTtl(), other.isSetTtl());
    if (lastComparison != 0) {
      return lastComparison;
    }
    if (isSetTtl()) {
      lastComparison = org.apache.thrift.TBaseHelper.compareTo(this.ttl, other.ttl);
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
    java.lang.StringBuilder sb = new java.lang.StringBuilder("TConditionalSession(");
    boolean first = true;

    sb.append("sessionId:");
    sb.append(this.sessionId);
    first = false;
    if (!first) sb.append(", ");
    sb.append("tserverLock:");
    if (this.tserverLock == null) {
      sb.append("null");
    } else {
      sb.append(this.tserverLock);
    }
    first = false;
    if (!first) sb.append(", ");
    sb.append("ttl:");
    sb.append(this.ttl);
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
      // it doesn't seem like you should have to do this, but java serialization is wacky, and doesn't call the default constructor.
      __isset_bitfield = 0;
      read(new org.apache.thrift.protocol.TCompactProtocol(new org.apache.thrift.transport.TIOStreamTransport(in)));
    } catch (org.apache.thrift.TException te) {
      throw new java.io.IOException(te);
    }
  }

  private static class TConditionalSessionStandardSchemeFactory implements org.apache.thrift.scheme.SchemeFactory {
    @Override
    public TConditionalSessionStandardScheme getScheme() {
      return new TConditionalSessionStandardScheme();
    }
  }

  private static class TConditionalSessionStandardScheme extends org.apache.thrift.scheme.StandardScheme<TConditionalSession> {

    @Override
    public void read(org.apache.thrift.protocol.TProtocol iprot, TConditionalSession struct) throws org.apache.thrift.TException {
      org.apache.thrift.protocol.TField schemeField;
      iprot.readStructBegin();
      while (true)
      {
        schemeField = iprot.readFieldBegin();
        if (schemeField.type == org.apache.thrift.protocol.TType.STOP) { 
          break;
        }
        switch (schemeField.id) {
          case 1: // SESSION_ID
            if (schemeField.type == org.apache.thrift.protocol.TType.I64) {
              struct.sessionId = iprot.readI64();
              struct.setSessionIdIsSet(true);
            } else { 
              org.apache.thrift.protocol.TProtocolUtil.skip(iprot, schemeField.type);
            }
            break;
          case 2: // TSERVER_LOCK
            if (schemeField.type == org.apache.thrift.protocol.TType.STRING) {
              struct.tserverLock = iprot.readString();
              struct.setTserverLockIsSet(true);
            } else { 
              org.apache.thrift.protocol.TProtocolUtil.skip(iprot, schemeField.type);
            }
            break;
          case 3: // TTL
            if (schemeField.type == org.apache.thrift.protocol.TType.I64) {
              struct.ttl = iprot.readI64();
              struct.setTtlIsSet(true);
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
    public void write(org.apache.thrift.protocol.TProtocol oprot, TConditionalSession struct) throws org.apache.thrift.TException {
      struct.validate();

      oprot.writeStructBegin(STRUCT_DESC);
      oprot.writeFieldBegin(SESSION_ID_FIELD_DESC);
      oprot.writeI64(struct.sessionId);
      oprot.writeFieldEnd();
      if (struct.tserverLock != null) {
        oprot.writeFieldBegin(TSERVER_LOCK_FIELD_DESC);
        oprot.writeString(struct.tserverLock);
        oprot.writeFieldEnd();
      }
      oprot.writeFieldBegin(TTL_FIELD_DESC);
      oprot.writeI64(struct.ttl);
      oprot.writeFieldEnd();
      oprot.writeFieldStop();
      oprot.writeStructEnd();
    }

  }

  private static class TConditionalSessionTupleSchemeFactory implements org.apache.thrift.scheme.SchemeFactory {
    @Override
    public TConditionalSessionTupleScheme getScheme() {
      return new TConditionalSessionTupleScheme();
    }
  }

  private static class TConditionalSessionTupleScheme extends org.apache.thrift.scheme.TupleScheme<TConditionalSession> {

    @Override
    public void write(org.apache.thrift.protocol.TProtocol prot, TConditionalSession struct) throws org.apache.thrift.TException {
      org.apache.thrift.protocol.TTupleProtocol oprot = (org.apache.thrift.protocol.TTupleProtocol) prot;
      java.util.BitSet optionals = new java.util.BitSet();
      if (struct.isSetSessionId()) {
        optionals.set(0);
      }
      if (struct.isSetTserverLock()) {
        optionals.set(1);
      }
      if (struct.isSetTtl()) {
        optionals.set(2);
      }
      oprot.writeBitSet(optionals, 3);
      if (struct.isSetSessionId()) {
        oprot.writeI64(struct.sessionId);
      }
      if (struct.isSetTserverLock()) {
        oprot.writeString(struct.tserverLock);
      }
      if (struct.isSetTtl()) {
        oprot.writeI64(struct.ttl);
      }
    }

    @Override
    public void read(org.apache.thrift.protocol.TProtocol prot, TConditionalSession struct) throws org.apache.thrift.TException {
      org.apache.thrift.protocol.TTupleProtocol iprot = (org.apache.thrift.protocol.TTupleProtocol) prot;
      java.util.BitSet incoming = iprot.readBitSet(3);
      if (incoming.get(0)) {
        struct.sessionId = iprot.readI64();
        struct.setSessionIdIsSet(true);
      }
      if (incoming.get(1)) {
        struct.tserverLock = iprot.readString();
        struct.setTserverLockIsSet(true);
      }
      if (incoming.get(2)) {
        struct.ttl = iprot.readI64();
        struct.setTtlIsSet(true);
      }
    }
  }

  private static <S extends org.apache.thrift.scheme.IScheme> S scheme(org.apache.thrift.protocol.TProtocol proto) {
    return (org.apache.thrift.scheme.StandardScheme.class.equals(proto.getScheme()) ? STANDARD_SCHEME_FACTORY : TUPLE_SCHEME_FACTORY).getScheme();
  }
  private static void unusedMethod() {}
}

