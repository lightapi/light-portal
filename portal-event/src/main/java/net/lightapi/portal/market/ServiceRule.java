/**
 * Autogenerated by Avro
 *
 * DO NOT EDIT DIRECTLY
 */
package net.lightapi.portal.market;

import org.apache.avro.generic.GenericArray;
import org.apache.avro.specific.SpecificData;
import org.apache.avro.util.Utf8;
import org.apache.avro.message.BinaryMessageEncoder;
import org.apache.avro.message.BinaryMessageDecoder;
import org.apache.avro.message.SchemaStore;

@org.apache.avro.specific.AvroGenerated
public class ServiceRule extends org.apache.avro.specific.SpecificRecordBase implements org.apache.avro.specific.SpecificRecord {
  private static final long serialVersionUID = 1138808138606214883L;


  public static final org.apache.avro.Schema SCHEMA$ = new org.apache.avro.Schema.Parser().parse("{\"type\":\"record\",\"name\":\"ServiceRule\",\"namespace\":\"net.lightapi.portal.market\",\"fields\":[{\"name\":\"ruleId\",\"type\":{\"type\":\"string\",\"avro.java.string\":\"String\"},\"doc\":\"rule id\"},{\"name\":\"roles\",\"type\":[\"null\",{\"type\":\"array\",\"items\":{\"type\":\"string\",\"avro.java.string\":\"String\"}}],\"doc\":\"roles that accesses the endpoint\",\"default\":null},{\"name\":\"variables\",\"type\":[\"null\",{\"type\":\"string\",\"avro.java.string\":\"String\"}],\"doc\":\"rule variables if any\",\"default\":null}]}");
  public static org.apache.avro.Schema getClassSchema() { return SCHEMA$; }

  private static final SpecificData MODEL$ = new SpecificData();

  private static final BinaryMessageEncoder<ServiceRule> ENCODER =
      new BinaryMessageEncoder<ServiceRule>(MODEL$, SCHEMA$);

  private static final BinaryMessageDecoder<ServiceRule> DECODER =
      new BinaryMessageDecoder<ServiceRule>(MODEL$, SCHEMA$);

  /**
   * Return the BinaryMessageEncoder instance used by this class.
   * @return the message encoder used by this class
   */
  public static BinaryMessageEncoder<ServiceRule> getEncoder() {
    return ENCODER;
  }

  /**
   * Return the BinaryMessageDecoder instance used by this class.
   * @return the message decoder used by this class
   */
  public static BinaryMessageDecoder<ServiceRule> getDecoder() {
    return DECODER;
  }

  /**
   * Create a new BinaryMessageDecoder instance for this class that uses the specified {@link SchemaStore}.
   * @param resolver a {@link SchemaStore} used to find schemas by fingerprint
   * @return a BinaryMessageDecoder instance for this class backed by the given SchemaStore
   */
  public static BinaryMessageDecoder<ServiceRule> createDecoder(SchemaStore resolver) {
    return new BinaryMessageDecoder<ServiceRule>(MODEL$, SCHEMA$, resolver);
  }

  /**
   * Serializes this ServiceRule to a ByteBuffer.
   * @return a buffer holding the serialized data for this instance
   * @throws java.io.IOException if this instance could not be serialized
   */
  public java.nio.ByteBuffer toByteBuffer() throws java.io.IOException {
    return ENCODER.encode(this);
  }

  /**
   * Deserializes a ServiceRule from a ByteBuffer.
   * @param b a byte buffer holding serialized data for an instance of this class
   * @return a ServiceRule instance decoded from the given buffer
   * @throws java.io.IOException if the given bytes could not be deserialized into an instance of this class
   */
  public static ServiceRule fromByteBuffer(
      java.nio.ByteBuffer b) throws java.io.IOException {
    return DECODER.decode(b);
  }

  /** rule id */
  private java.lang.String ruleId;
  /** roles that accesses the endpoint */
  private java.util.List<java.lang.String> roles;
  /** rule variables if any */
  private java.lang.String variables;

  /**
   * Default constructor.  Note that this does not initialize fields
   * to their default values from the schema.  If that is desired then
   * one should use <code>newBuilder()</code>.
   */
  public ServiceRule() {}

  /**
   * All-args constructor.
   * @param ruleId rule id
   * @param roles roles that accesses the endpoint
   * @param variables rule variables if any
   */
  public ServiceRule(java.lang.String ruleId, java.util.List<java.lang.String> roles, java.lang.String variables) {
    this.ruleId = ruleId;
    this.roles = roles;
    this.variables = variables;
  }

  public org.apache.avro.specific.SpecificData getSpecificData() { return MODEL$; }
  public org.apache.avro.Schema getSchema() { return SCHEMA$; }
  // Used by DatumWriter.  Applications should not call.
  public java.lang.Object get(int field$) {
    switch (field$) {
    case 0: return ruleId;
    case 1: return roles;
    case 2: return variables;
    default: throw new IndexOutOfBoundsException("Invalid index: " + field$);
    }
  }

  // Used by DatumReader.  Applications should not call.
  @SuppressWarnings(value="unchecked")
  public void put(int field$, java.lang.Object value$) {
    switch (field$) {
    case 0: ruleId = value$ != null ? value$.toString() : null; break;
    case 1: roles = (java.util.List<java.lang.String>)value$; break;
    case 2: variables = value$ != null ? value$.toString() : null; break;
    default: throw new IndexOutOfBoundsException("Invalid index: " + field$);
    }
  }

  /**
   * Gets the value of the 'ruleId' field.
   * @return rule id
   */
  public java.lang.String getRuleId() {
    return ruleId;
  }


  /**
   * Sets the value of the 'ruleId' field.
   * rule id
   * @param value the value to set.
   */
  public void setRuleId(java.lang.String value) {
    this.ruleId = value;
  }

  /**
   * Gets the value of the 'roles' field.
   * @return roles that accesses the endpoint
   */
  public java.util.List<java.lang.String> getRoles() {
    return roles;
  }


  /**
   * Sets the value of the 'roles' field.
   * roles that accesses the endpoint
   * @param value the value to set.
   */
  public void setRoles(java.util.List<java.lang.String> value) {
    this.roles = value;
  }

  /**
   * Gets the value of the 'variables' field.
   * @return rule variables if any
   */
  public java.lang.String getVariables() {
    return variables;
  }


  /**
   * Sets the value of the 'variables' field.
   * rule variables if any
   * @param value the value to set.
   */
  public void setVariables(java.lang.String value) {
    this.variables = value;
  }

  /**
   * Creates a new ServiceRule RecordBuilder.
   * @return A new ServiceRule RecordBuilder
   */
  public static net.lightapi.portal.market.ServiceRule.Builder newBuilder() {
    return new net.lightapi.portal.market.ServiceRule.Builder();
  }

  /**
   * Creates a new ServiceRule RecordBuilder by copying an existing Builder.
   * @param other The existing builder to copy.
   * @return A new ServiceRule RecordBuilder
   */
  public static net.lightapi.portal.market.ServiceRule.Builder newBuilder(net.lightapi.portal.market.ServiceRule.Builder other) {
    if (other == null) {
      return new net.lightapi.portal.market.ServiceRule.Builder();
    } else {
      return new net.lightapi.portal.market.ServiceRule.Builder(other);
    }
  }

  /**
   * Creates a new ServiceRule RecordBuilder by copying an existing ServiceRule instance.
   * @param other The existing instance to copy.
   * @return A new ServiceRule RecordBuilder
   */
  public static net.lightapi.portal.market.ServiceRule.Builder newBuilder(net.lightapi.portal.market.ServiceRule other) {
    if (other == null) {
      return new net.lightapi.portal.market.ServiceRule.Builder();
    } else {
      return new net.lightapi.portal.market.ServiceRule.Builder(other);
    }
  }

  /**
   * RecordBuilder for ServiceRule instances.
   */
  @org.apache.avro.specific.AvroGenerated
  public static class Builder extends org.apache.avro.specific.SpecificRecordBuilderBase<ServiceRule>
    implements org.apache.avro.data.RecordBuilder<ServiceRule> {

    /** rule id */
    private java.lang.String ruleId;
    /** roles that accesses the endpoint */
    private java.util.List<java.lang.String> roles;
    /** rule variables if any */
    private java.lang.String variables;

    /** Creates a new Builder */
    private Builder() {
      super(SCHEMA$, MODEL$);
    }

    /**
     * Creates a Builder by copying an existing Builder.
     * @param other The existing Builder to copy.
     */
    private Builder(net.lightapi.portal.market.ServiceRule.Builder other) {
      super(other);
      if (isValidValue(fields()[0], other.ruleId)) {
        this.ruleId = data().deepCopy(fields()[0].schema(), other.ruleId);
        fieldSetFlags()[0] = other.fieldSetFlags()[0];
      }
      if (isValidValue(fields()[1], other.roles)) {
        this.roles = data().deepCopy(fields()[1].schema(), other.roles);
        fieldSetFlags()[1] = other.fieldSetFlags()[1];
      }
      if (isValidValue(fields()[2], other.variables)) {
        this.variables = data().deepCopy(fields()[2].schema(), other.variables);
        fieldSetFlags()[2] = other.fieldSetFlags()[2];
      }
    }

    /**
     * Creates a Builder by copying an existing ServiceRule instance
     * @param other The existing instance to copy.
     */
    private Builder(net.lightapi.portal.market.ServiceRule other) {
      super(SCHEMA$, MODEL$);
      if (isValidValue(fields()[0], other.ruleId)) {
        this.ruleId = data().deepCopy(fields()[0].schema(), other.ruleId);
        fieldSetFlags()[0] = true;
      }
      if (isValidValue(fields()[1], other.roles)) {
        this.roles = data().deepCopy(fields()[1].schema(), other.roles);
        fieldSetFlags()[1] = true;
      }
      if (isValidValue(fields()[2], other.variables)) {
        this.variables = data().deepCopy(fields()[2].schema(), other.variables);
        fieldSetFlags()[2] = true;
      }
    }

    /**
      * Gets the value of the 'ruleId' field.
      * rule id
      * @return The value.
      */
    public java.lang.String getRuleId() {
      return ruleId;
    }


    /**
      * Sets the value of the 'ruleId' field.
      * rule id
      * @param value The value of 'ruleId'.
      * @return This builder.
      */
    public net.lightapi.portal.market.ServiceRule.Builder setRuleId(java.lang.String value) {
      validate(fields()[0], value);
      this.ruleId = value;
      fieldSetFlags()[0] = true;
      return this;
    }

    /**
      * Checks whether the 'ruleId' field has been set.
      * rule id
      * @return True if the 'ruleId' field has been set, false otherwise.
      */
    public boolean hasRuleId() {
      return fieldSetFlags()[0];
    }


    /**
      * Clears the value of the 'ruleId' field.
      * rule id
      * @return This builder.
      */
    public net.lightapi.portal.market.ServiceRule.Builder clearRuleId() {
      ruleId = null;
      fieldSetFlags()[0] = false;
      return this;
    }

    /**
      * Gets the value of the 'roles' field.
      * roles that accesses the endpoint
      * @return The value.
      */
    public java.util.List<java.lang.String> getRoles() {
      return roles;
    }


    /**
      * Sets the value of the 'roles' field.
      * roles that accesses the endpoint
      * @param value The value of 'roles'.
      * @return This builder.
      */
    public net.lightapi.portal.market.ServiceRule.Builder setRoles(java.util.List<java.lang.String> value) {
      validate(fields()[1], value);
      this.roles = value;
      fieldSetFlags()[1] = true;
      return this;
    }

    /**
      * Checks whether the 'roles' field has been set.
      * roles that accesses the endpoint
      * @return True if the 'roles' field has been set, false otherwise.
      */
    public boolean hasRoles() {
      return fieldSetFlags()[1];
    }


    /**
      * Clears the value of the 'roles' field.
      * roles that accesses the endpoint
      * @return This builder.
      */
    public net.lightapi.portal.market.ServiceRule.Builder clearRoles() {
      roles = null;
      fieldSetFlags()[1] = false;
      return this;
    }

    /**
      * Gets the value of the 'variables' field.
      * rule variables if any
      * @return The value.
      */
    public java.lang.String getVariables() {
      return variables;
    }


    /**
      * Sets the value of the 'variables' field.
      * rule variables if any
      * @param value The value of 'variables'.
      * @return This builder.
      */
    public net.lightapi.portal.market.ServiceRule.Builder setVariables(java.lang.String value) {
      validate(fields()[2], value);
      this.variables = value;
      fieldSetFlags()[2] = true;
      return this;
    }

    /**
      * Checks whether the 'variables' field has been set.
      * rule variables if any
      * @return True if the 'variables' field has been set, false otherwise.
      */
    public boolean hasVariables() {
      return fieldSetFlags()[2];
    }


    /**
      * Clears the value of the 'variables' field.
      * rule variables if any
      * @return This builder.
      */
    public net.lightapi.portal.market.ServiceRule.Builder clearVariables() {
      variables = null;
      fieldSetFlags()[2] = false;
      return this;
    }

    @Override
    @SuppressWarnings("unchecked")
    public ServiceRule build() {
      try {
        ServiceRule record = new ServiceRule();
        record.ruleId = fieldSetFlags()[0] ? this.ruleId : (java.lang.String) defaultValue(fields()[0]);
        record.roles = fieldSetFlags()[1] ? this.roles : (java.util.List<java.lang.String>) defaultValue(fields()[1]);
        record.variables = fieldSetFlags()[2] ? this.variables : (java.lang.String) defaultValue(fields()[2]);
        return record;
      } catch (org.apache.avro.AvroMissingFieldException e) {
        throw e;
      } catch (java.lang.Exception e) {
        throw new org.apache.avro.AvroRuntimeException(e);
      }
    }
  }

  @SuppressWarnings("unchecked")
  private static final org.apache.avro.io.DatumWriter<ServiceRule>
    WRITER$ = (org.apache.avro.io.DatumWriter<ServiceRule>)MODEL$.createDatumWriter(SCHEMA$);

  @Override public void writeExternal(java.io.ObjectOutput out)
    throws java.io.IOException {
    WRITER$.write(this, SpecificData.getEncoder(out));
  }

  @SuppressWarnings("unchecked")
  private static final org.apache.avro.io.DatumReader<ServiceRule>
    READER$ = (org.apache.avro.io.DatumReader<ServiceRule>)MODEL$.createDatumReader(SCHEMA$);

  @Override public void readExternal(java.io.ObjectInput in)
    throws java.io.IOException {
    READER$.read(this, SpecificData.getDecoder(in));
  }

  @Override protected boolean hasCustomCoders() { return true; }

  @Override public void customEncode(org.apache.avro.io.Encoder out)
    throws java.io.IOException
  {
    out.writeString(this.ruleId);

    if (this.roles == null) {
      out.writeIndex(0);
      out.writeNull();
    } else {
      out.writeIndex(1);
      long size0 = this.roles.size();
      out.writeArrayStart();
      out.setItemCount(size0);
      long actualSize0 = 0;
      for (java.lang.String e0: this.roles) {
        actualSize0++;
        out.startItem();
        out.writeString(e0);
      }
      out.writeArrayEnd();
      if (actualSize0 != size0)
        throw new java.util.ConcurrentModificationException("Array-size written was " + size0 + ", but element count was " + actualSize0 + ".");
    }

    if (this.variables == null) {
      out.writeIndex(0);
      out.writeNull();
    } else {
      out.writeIndex(1);
      out.writeString(this.variables);
    }

  }

  @Override public void customDecode(org.apache.avro.io.ResolvingDecoder in)
    throws java.io.IOException
  {
    org.apache.avro.Schema.Field[] fieldOrder = in.readFieldOrderIfDiff();
    if (fieldOrder == null) {
      this.ruleId = in.readString();

      if (in.readIndex() != 1) {
        in.readNull();
        this.roles = null;
      } else {
        long size0 = in.readArrayStart();
        java.util.List<java.lang.String> a0 = this.roles;
        if (a0 == null) {
          a0 = new SpecificData.Array<java.lang.String>((int)size0, SCHEMA$.getField("roles").schema().getTypes().get(1));
          this.roles = a0;
        } else a0.clear();
        SpecificData.Array<java.lang.String> ga0 = (a0 instanceof SpecificData.Array ? (SpecificData.Array<java.lang.String>)a0 : null);
        for ( ; 0 < size0; size0 = in.arrayNext()) {
          for ( ; size0 != 0; size0--) {
            java.lang.String e0 = (ga0 != null ? ga0.peek() : null);
            e0 = in.readString();
            a0.add(e0);
          }
        }
      }

      if (in.readIndex() != 1) {
        in.readNull();
        this.variables = null;
      } else {
        this.variables = in.readString();
      }

    } else {
      for (int i = 0; i < 3; i++) {
        switch (fieldOrder[i].pos()) {
        case 0:
          this.ruleId = in.readString();
          break;

        case 1:
          if (in.readIndex() != 1) {
            in.readNull();
            this.roles = null;
          } else {
            long size0 = in.readArrayStart();
            java.util.List<java.lang.String> a0 = this.roles;
            if (a0 == null) {
              a0 = new SpecificData.Array<java.lang.String>((int)size0, SCHEMA$.getField("roles").schema().getTypes().get(1));
              this.roles = a0;
            } else a0.clear();
            SpecificData.Array<java.lang.String> ga0 = (a0 instanceof SpecificData.Array ? (SpecificData.Array<java.lang.String>)a0 : null);
            for ( ; 0 < size0; size0 = in.arrayNext()) {
              for ( ; size0 != 0; size0--) {
                java.lang.String e0 = (ga0 != null ? ga0.peek() : null);
                e0 = in.readString();
                a0.add(e0);
              }
            }
          }
          break;

        case 2:
          if (in.readIndex() != 1) {
            in.readNull();
            this.variables = null;
          } else {
            this.variables = in.readString();
          }
          break;

        default:
          throw new java.io.IOException("Corrupt ResolvingDecoder.");
        }
      }
    }
  }
}
