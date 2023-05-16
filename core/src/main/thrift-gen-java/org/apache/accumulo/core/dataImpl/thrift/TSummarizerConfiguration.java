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
public class TSummarizerConfiguration implements org.apache.thrift.TBase<TSummarizerConfiguration, TSummarizerConfiguration._Fields>, java.io.Serializable, Cloneable, Comparable<TSummarizerConfiguration> {
  private static final org.apache.thrift.protocol.TStruct STRUCT_DESC = new org.apache.thrift.protocol.TStruct("TSummarizerConfiguration");

  private static final org.apache.thrift.protocol.TField CLASSNAME_FIELD_DESC = new org.apache.thrift.protocol.TField("classname", org.apache.thrift.protocol.TType.STRING, (short)1);
  private static final org.apache.thrift.protocol.TField OPTIONS_FIELD_DESC = new org.apache.thrift.protocol.TField("options", org.apache.thrift.protocol.TType.MAP, (short)2);
  private static final org.apache.thrift.protocol.TField CONFIG_ID_FIELD_DESC = new org.apache.thrift.protocol.TField("configId", org.apache.thrift.protocol.TType.STRING, (short)3);

  private static final org.apache.thrift.scheme.SchemeFactory STANDARD_SCHEME_FACTORY = new TSummarizerConfigurationStandardSchemeFactory();
  private static final org.apache.thrift.scheme.SchemeFactory TUPLE_SCHEME_FACTORY = new TSummarizerConfigurationTupleSchemeFactory();

  private @org.apache.thrift.annotation.Nullable java.lang.String classname; // required
  private @org.apache.thrift.annotation.Nullable java.util.Map<java.lang.String,java.lang.String> options; // required
  private @org.apache.thrift.annotation.Nullable java.lang.String configId; // required

  /** The set of fields this struct contains, along with convenience methods for finding and manipulating them. */
  public enum _Fields implements org.apache.thrift.TFieldIdEnum {
    CLASSNAME((short)1, "classname"),
    OPTIONS((short)2, "options"),
    CONFIG_ID((short)3, "configId");

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
        case 1: // CLASSNAME
          return CLASSNAME;
        case 2: // OPTIONS
          return OPTIONS;
        case 3: // CONFIG_ID
          return CONFIG_ID;
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
    tmpMap.put(_Fields.CLASSNAME, new org.apache.thrift.meta_data.FieldMetaData("classname", org.apache.thrift.TFieldRequirementType.DEFAULT, 
        new org.apache.thrift.meta_data.FieldValueMetaData(org.apache.thrift.protocol.TType.STRING)));
    tmpMap.put(_Fields.OPTIONS, new org.apache.thrift.meta_data.FieldMetaData("options", org.apache.thrift.TFieldRequirementType.DEFAULT, 
        new org.apache.thrift.meta_data.MapMetaData(org.apache.thrift.protocol.TType.MAP, 
            new org.apache.thrift.meta_data.FieldValueMetaData(org.apache.thrift.protocol.TType.STRING), 
            new org.apache.thrift.meta_data.FieldValueMetaData(org.apache.thrift.protocol.TType.STRING))));
    tmpMap.put(_Fields.CONFIG_ID, new org.apache.thrift.meta_data.FieldMetaData("configId", org.apache.thrift.TFieldRequirementType.DEFAULT, 
        new org.apache.thrift.meta_data.FieldValueMetaData(org.apache.thrift.protocol.TType.STRING)));
    metaDataMap = java.util.Collections.unmodifiableMap(tmpMap);
    org.apache.thrift.meta_data.FieldMetaData.addStructMetaDataMap(TSummarizerConfiguration.class, metaDataMap);
  }

  public TSummarizerConfiguration() {
  }

  public TSummarizerConfiguration(
    java.lang.String classname,
    java.util.Map<java.lang.String,java.lang.String> options,
    java.lang.String configId)
  {
    this();
    this.classname = classname;
    this.options = options;
    this.configId = configId;
  }

  /**
   * Performs a deep copy on <i>other</i>.
   */
  public TSummarizerConfiguration(TSummarizerConfiguration other) {
    if (other.isSetClassname()) {
      this.classname = other.classname;
    }
    if (other.isSetOptions()) {
      java.util.Map<java.lang.String,java.lang.String> __this__options = new java.util.HashMap<java.lang.String,java.lang.String>(other.options);
      this.options = __this__options;
    }
    if (other.isSetConfigId()) {
      this.configId = other.configId;
    }
  }

  @Override
  public TSummarizerConfiguration deepCopy() {
    return new TSummarizerConfiguration(this);
  }

  @Override
  public void clear() {
    this.classname = null;
    this.options = null;
    this.configId = null;
  }

  @org.apache.thrift.annotation.Nullable
  public java.lang.String getClassname() {
    return this.classname;
  }

  public TSummarizerConfiguration setClassname(@org.apache.thrift.annotation.Nullable java.lang.String classname) {
    this.classname = classname;
    return this;
  }

  public void unsetClassname() {
    this.classname = null;
  }

  /** Returns true if field classname is set (has been assigned a value) and false otherwise */
  public boolean isSetClassname() {
    return this.classname != null;
  }

  public void setClassnameIsSet(boolean value) {
    if (!value) {
      this.classname = null;
    }
  }

  public int getOptionsSize() {
    return (this.options == null) ? 0 : this.options.size();
  }

  public void putToOptions(java.lang.String key, java.lang.String val) {
    if (this.options == null) {
      this.options = new java.util.HashMap<java.lang.String,java.lang.String>();
    }
    this.options.put(key, val);
  }

  @org.apache.thrift.annotation.Nullable
  public java.util.Map<java.lang.String,java.lang.String> getOptions() {
    return this.options;
  }

  public TSummarizerConfiguration setOptions(@org.apache.thrift.annotation.Nullable java.util.Map<java.lang.String,java.lang.String> options) {
    this.options = options;
    return this;
  }

  public void unsetOptions() {
    this.options = null;
  }

  /** Returns true if field options is set (has been assigned a value) and false otherwise */
  public boolean isSetOptions() {
    return this.options != null;
  }

  public void setOptionsIsSet(boolean value) {
    if (!value) {
      this.options = null;
    }
  }

  @org.apache.thrift.annotation.Nullable
  public java.lang.String getConfigId() {
    return this.configId;
  }

  public TSummarizerConfiguration setConfigId(@org.apache.thrift.annotation.Nullable java.lang.String configId) {
    this.configId = configId;
    return this;
  }

  public void unsetConfigId() {
    this.configId = null;
  }

  /** Returns true if field configId is set (has been assigned a value) and false otherwise */
  public boolean isSetConfigId() {
    return this.configId != null;
  }

  public void setConfigIdIsSet(boolean value) {
    if (!value) {
      this.configId = null;
    }
  }

  @Override
  public void setFieldValue(_Fields field, @org.apache.thrift.annotation.Nullable java.lang.Object value) {
    switch (field) {
    case CLASSNAME:
      if (value == null) {
        unsetClassname();
      } else {
        setClassname((java.lang.String)value);
      }
      break;

    case OPTIONS:
      if (value == null) {
        unsetOptions();
      } else {
        setOptions((java.util.Map<java.lang.String,java.lang.String>)value);
      }
      break;

    case CONFIG_ID:
      if (value == null) {
        unsetConfigId();
      } else {
        setConfigId((java.lang.String)value);
      }
      break;

    }
  }

  @org.apache.thrift.annotation.Nullable
  @Override
  public java.lang.Object getFieldValue(_Fields field) {
    switch (field) {
    case CLASSNAME:
      return getClassname();

    case OPTIONS:
      return getOptions();

    case CONFIG_ID:
      return getConfigId();

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
    case CLASSNAME:
      return isSetClassname();
    case OPTIONS:
      return isSetOptions();
    case CONFIG_ID:
      return isSetConfigId();
    }
    throw new java.lang.IllegalStateException();
  }

  @Override
  public boolean equals(java.lang.Object that) {
    if (that instanceof TSummarizerConfiguration)
      return this.equals((TSummarizerConfiguration)that);
    return false;
  }

  public boolean equals(TSummarizerConfiguration that) {
    if (that == null)
      return false;
    if (this == that)
      return true;

    boolean this_present_classname = true && this.isSetClassname();
    boolean that_present_classname = true && that.isSetClassname();
    if (this_present_classname || that_present_classname) {
      if (!(this_present_classname && that_present_classname))
        return false;
      if (!this.classname.equals(that.classname))
        return false;
    }

    boolean this_present_options = true && this.isSetOptions();
    boolean that_present_options = true && that.isSetOptions();
    if (this_present_options || that_present_options) {
      if (!(this_present_options && that_present_options))
        return false;
      if (!this.options.equals(that.options))
        return false;
    }

    boolean this_present_configId = true && this.isSetConfigId();
    boolean that_present_configId = true && that.isSetConfigId();
    if (this_present_configId || that_present_configId) {
      if (!(this_present_configId && that_present_configId))
        return false;
      if (!this.configId.equals(that.configId))
        return false;
    }

    return true;
  }

  @Override
  public int hashCode() {
    int hashCode = 1;

    hashCode = hashCode * 8191 + ((isSetClassname()) ? 131071 : 524287);
    if (isSetClassname())
      hashCode = hashCode * 8191 + classname.hashCode();

    hashCode = hashCode * 8191 + ((isSetOptions()) ? 131071 : 524287);
    if (isSetOptions())
      hashCode = hashCode * 8191 + options.hashCode();

    hashCode = hashCode * 8191 + ((isSetConfigId()) ? 131071 : 524287);
    if (isSetConfigId())
      hashCode = hashCode * 8191 + configId.hashCode();

    return hashCode;
  }

  @Override
  public int compareTo(TSummarizerConfiguration other) {
    if (!getClass().equals(other.getClass())) {
      return getClass().getName().compareTo(other.getClass().getName());
    }

    int lastComparison = 0;

    lastComparison = java.lang.Boolean.compare(isSetClassname(), other.isSetClassname());
    if (lastComparison != 0) {
      return lastComparison;
    }
    if (isSetClassname()) {
      lastComparison = org.apache.thrift.TBaseHelper.compareTo(this.classname, other.classname);
      if (lastComparison != 0) {
        return lastComparison;
      }
    }
    lastComparison = java.lang.Boolean.compare(isSetOptions(), other.isSetOptions());
    if (lastComparison != 0) {
      return lastComparison;
    }
    if (isSetOptions()) {
      lastComparison = org.apache.thrift.TBaseHelper.compareTo(this.options, other.options);
      if (lastComparison != 0) {
        return lastComparison;
      }
    }
    lastComparison = java.lang.Boolean.compare(isSetConfigId(), other.isSetConfigId());
    if (lastComparison != 0) {
      return lastComparison;
    }
    if (isSetConfigId()) {
      lastComparison = org.apache.thrift.TBaseHelper.compareTo(this.configId, other.configId);
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
    java.lang.StringBuilder sb = new java.lang.StringBuilder("TSummarizerConfiguration(");
    boolean first = true;

    sb.append("classname:");
    if (this.classname == null) {
      sb.append("null");
    } else {
      sb.append(this.classname);
    }
    first = false;
    if (!first) sb.append(", ");
    sb.append("options:");
    if (this.options == null) {
      sb.append("null");
    } else {
      sb.append(this.options);
    }
    first = false;
    if (!first) sb.append(", ");
    sb.append("configId:");
    if (this.configId == null) {
      sb.append("null");
    } else {
      sb.append(this.configId);
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

  private static class TSummarizerConfigurationStandardSchemeFactory implements org.apache.thrift.scheme.SchemeFactory {
    @Override
    public TSummarizerConfigurationStandardScheme getScheme() {
      return new TSummarizerConfigurationStandardScheme();
    }
  }

  private static class TSummarizerConfigurationStandardScheme extends org.apache.thrift.scheme.StandardScheme<TSummarizerConfiguration> {

    @Override
    public void read(org.apache.thrift.protocol.TProtocol iprot, TSummarizerConfiguration struct) throws org.apache.thrift.TException {
      org.apache.thrift.protocol.TField schemeField;
      iprot.readStructBegin();
      while (true)
      {
        schemeField = iprot.readFieldBegin();
        if (schemeField.type == org.apache.thrift.protocol.TType.STOP) { 
          break;
        }
        switch (schemeField.id) {
          case 1: // CLASSNAME
            if (schemeField.type == org.apache.thrift.protocol.TType.STRING) {
              struct.classname = iprot.readString();
              struct.setClassnameIsSet(true);
            } else { 
              org.apache.thrift.protocol.TProtocolUtil.skip(iprot, schemeField.type);
            }
            break;
          case 2: // OPTIONS
            if (schemeField.type == org.apache.thrift.protocol.TType.MAP) {
              {
                org.apache.thrift.protocol.TMap _map94 = iprot.readMapBegin();
                struct.options = new java.util.HashMap<java.lang.String,java.lang.String>(2*_map94.size);
                @org.apache.thrift.annotation.Nullable java.lang.String _key95;
                @org.apache.thrift.annotation.Nullable java.lang.String _val96;
                for (int _i97 = 0; _i97 < _map94.size; ++_i97)
                {
                  _key95 = iprot.readString();
                  _val96 = iprot.readString();
                  struct.options.put(_key95, _val96);
                }
                iprot.readMapEnd();
              }
              struct.setOptionsIsSet(true);
            } else { 
              org.apache.thrift.protocol.TProtocolUtil.skip(iprot, schemeField.type);
            }
            break;
          case 3: // CONFIG_ID
            if (schemeField.type == org.apache.thrift.protocol.TType.STRING) {
              struct.configId = iprot.readString();
              struct.setConfigIdIsSet(true);
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
    public void write(org.apache.thrift.protocol.TProtocol oprot, TSummarizerConfiguration struct) throws org.apache.thrift.TException {
      struct.validate();

      oprot.writeStructBegin(STRUCT_DESC);
      if (struct.classname != null) {
        oprot.writeFieldBegin(CLASSNAME_FIELD_DESC);
        oprot.writeString(struct.classname);
        oprot.writeFieldEnd();
      }
      if (struct.options != null) {
        oprot.writeFieldBegin(OPTIONS_FIELD_DESC);
        {
          oprot.writeMapBegin(new org.apache.thrift.protocol.TMap(org.apache.thrift.protocol.TType.STRING, org.apache.thrift.protocol.TType.STRING, struct.options.size()));
          for (java.util.Map.Entry<java.lang.String, java.lang.String> _iter98 : struct.options.entrySet())
          {
            oprot.writeString(_iter98.getKey());
            oprot.writeString(_iter98.getValue());
          }
          oprot.writeMapEnd();
        }
        oprot.writeFieldEnd();
      }
      if (struct.configId != null) {
        oprot.writeFieldBegin(CONFIG_ID_FIELD_DESC);
        oprot.writeString(struct.configId);
        oprot.writeFieldEnd();
      }
      oprot.writeFieldStop();
      oprot.writeStructEnd();
    }

  }

  private static class TSummarizerConfigurationTupleSchemeFactory implements org.apache.thrift.scheme.SchemeFactory {
    @Override
    public TSummarizerConfigurationTupleScheme getScheme() {
      return new TSummarizerConfigurationTupleScheme();
    }
  }

  private static class TSummarizerConfigurationTupleScheme extends org.apache.thrift.scheme.TupleScheme<TSummarizerConfiguration> {

    @Override
    public void write(org.apache.thrift.protocol.TProtocol prot, TSummarizerConfiguration struct) throws org.apache.thrift.TException {
      org.apache.thrift.protocol.TTupleProtocol oprot = (org.apache.thrift.protocol.TTupleProtocol) prot;
      java.util.BitSet optionals = new java.util.BitSet();
      if (struct.isSetClassname()) {
        optionals.set(0);
      }
      if (struct.isSetOptions()) {
        optionals.set(1);
      }
      if (struct.isSetConfigId()) {
        optionals.set(2);
      }
      oprot.writeBitSet(optionals, 3);
      if (struct.isSetClassname()) {
        oprot.writeString(struct.classname);
      }
      if (struct.isSetOptions()) {
        {
          oprot.writeI32(struct.options.size());
          for (java.util.Map.Entry<java.lang.String, java.lang.String> _iter99 : struct.options.entrySet())
          {
            oprot.writeString(_iter99.getKey());
            oprot.writeString(_iter99.getValue());
          }
        }
      }
      if (struct.isSetConfigId()) {
        oprot.writeString(struct.configId);
      }
    }

    @Override
    public void read(org.apache.thrift.protocol.TProtocol prot, TSummarizerConfiguration struct) throws org.apache.thrift.TException {
      org.apache.thrift.protocol.TTupleProtocol iprot = (org.apache.thrift.protocol.TTupleProtocol) prot;
      java.util.BitSet incoming = iprot.readBitSet(3);
      if (incoming.get(0)) {
        struct.classname = iprot.readString();
        struct.setClassnameIsSet(true);
      }
      if (incoming.get(1)) {
        {
          org.apache.thrift.protocol.TMap _map100 = iprot.readMapBegin(org.apache.thrift.protocol.TType.STRING, org.apache.thrift.protocol.TType.STRING); 
          struct.options = new java.util.HashMap<java.lang.String,java.lang.String>(2*_map100.size);
          @org.apache.thrift.annotation.Nullable java.lang.String _key101;
          @org.apache.thrift.annotation.Nullable java.lang.String _val102;
          for (int _i103 = 0; _i103 < _map100.size; ++_i103)
          {
            _key101 = iprot.readString();
            _val102 = iprot.readString();
            struct.options.put(_key101, _val102);
          }
        }
        struct.setOptionsIsSet(true);
      }
      if (incoming.get(2)) {
        struct.configId = iprot.readString();
        struct.setConfigIdIsSet(true);
      }
    }
  }

  private static <S extends org.apache.thrift.scheme.IScheme> S scheme(org.apache.thrift.protocol.TProtocol proto) {
    return (org.apache.thrift.scheme.StandardScheme.class.equals(proto.getScheme()) ? STANDARD_SCHEME_FACTORY : TUPLE_SCHEME_FACTORY).getScheme();
  }
  private static void unusedMethod() {}
}

