/**
 * Autogenerated by Avro
 *
 * DO NOT EDIT DIRECTLY
 */
package net.lightapi.portal.service;

import org.apache.avro.specific.SpecificData;
import org.apache.avro.util.Utf8;
import org.apache.avro.message.BinaryMessageEncoder;
import org.apache.avro.message.BinaryMessageDecoder;
import org.apache.avro.message.SchemaStore;

@org.apache.avro.specific.AvroGenerated
public class ServiceUpdatedEvent extends org.apache.avro.specific.SpecificRecordBase implements org.apache.avro.specific.SpecificRecord {
  private static final long serialVersionUID = 5927886605322843138L;


  public static final org.apache.avro.Schema SCHEMA$ = new org.apache.avro.Schema.Parser().parse("{\"type\":\"record\",\"name\":\"ServiceUpdatedEvent\",\"namespace\":\"net.lightapi.portal.service\",\"fields\":[{\"name\":\"EventId\",\"type\":{\"type\":\"record\",\"name\":\"EventId\",\"namespace\":\"com.networknt.kafka.common\",\"fields\":[{\"name\":\"id\",\"type\":{\"type\":\"string\",\"avro.java.string\":\"String\"},\"doc\":\"a unique identifier for the event\"},{\"name\":\"userId\",\"type\":{\"type\":\"string\",\"avro.java.string\":\"String\"},\"doc\":\"the user who creates the event\"},{\"name\":\"hostId\",\"type\":{\"type\":\"string\",\"avro.java.string\":\"String\"},\"doc\":\"the host which is the event is created\"},{\"name\":\"nonce\",\"type\":\"long\",\"doc\":\"the number of the transactions for the user\"},{\"name\":\"timestamp\",\"type\":\"long\",\"doc\":\"time the event is recorded\",\"default\":0},{\"name\":\"derived\",\"type\":\"boolean\",\"doc\":\"indicate if the event is derived from event processor\",\"default\":false}]}},{\"name\":\"hostId\",\"type\":{\"type\":\"string\",\"avro.java.string\":\"String\"},\"doc\":\"host id\"},{\"name\":\"apiId\",\"type\":{\"type\":\"string\",\"avro.java.string\":\"String\"},\"doc\":\"api id\"},{\"name\":\"apiName\",\"type\":{\"type\":\"string\",\"avro.java.string\":\"String\"},\"doc\":\"api name\"},{\"name\":\"apiStatus\",\"type\":{\"type\":\"string\",\"avro.java.string\":\"String\"},\"doc\":\"api status\"},{\"name\":\"value\",\"type\":{\"type\":\"string\",\"avro.java.string\":\"String\"},\"doc\":\"service detail in JSON\"}]}");
  public static org.apache.avro.Schema getClassSchema() { return SCHEMA$; }

  private static final SpecificData MODEL$ = new SpecificData();

  private static final BinaryMessageEncoder<ServiceUpdatedEvent> ENCODER =
      new BinaryMessageEncoder<>(MODEL$, SCHEMA$);

  private static final BinaryMessageDecoder<ServiceUpdatedEvent> DECODER =
      new BinaryMessageDecoder<>(MODEL$, SCHEMA$);

  /**
   * Return the BinaryMessageEncoder instance used by this class.
   * @return the message encoder used by this class
   */
  public static BinaryMessageEncoder<ServiceUpdatedEvent> getEncoder() {
    return ENCODER;
  }

  /**
   * Return the BinaryMessageDecoder instance used by this class.
   * @return the message decoder used by this class
   */
  public static BinaryMessageDecoder<ServiceUpdatedEvent> getDecoder() {
    return DECODER;
  }

  /**
   * Create a new BinaryMessageDecoder instance for this class that uses the specified {@link SchemaStore}.
   * @param resolver a {@link SchemaStore} used to find schemas by fingerprint
   * @return a BinaryMessageDecoder instance for this class backed by the given SchemaStore
   */
  public static BinaryMessageDecoder<ServiceUpdatedEvent> createDecoder(SchemaStore resolver) {
    return new BinaryMessageDecoder<>(MODEL$, SCHEMA$, resolver);
  }

  /**
   * Serializes this ServiceUpdatedEvent to a ByteBuffer.
   * @return a buffer holding the serialized data for this instance
   * @throws java.io.IOException if this instance could not be serialized
   */
  public java.nio.ByteBuffer toByteBuffer() throws java.io.IOException {
    return ENCODER.encode(this);
  }

  /**
   * Deserializes a ServiceUpdatedEvent from a ByteBuffer.
   * @param b a byte buffer holding serialized data for an instance of this class
   * @return a ServiceUpdatedEvent instance decoded from the given buffer
   * @throws java.io.IOException if the given bytes could not be deserialized into an instance of this class
   */
  public static ServiceUpdatedEvent fromByteBuffer(
      java.nio.ByteBuffer b) throws java.io.IOException {
    return DECODER.decode(b);
  }

  private com.networknt.kafka.common.EventId EventId;
  /** host id */
  private java.lang.String hostId;
  /** api id */
  private java.lang.String apiId;
  /** api name */
  private java.lang.String apiName;
  /** api status */
  private java.lang.String apiStatus;
  /** service detail in JSON */
  private java.lang.String value;

  /**
   * Default constructor.  Note that this does not initialize fields
   * to their default values from the schema.  If that is desired then
   * one should use <code>newBuilder()</code>.
   */
  public ServiceUpdatedEvent() {}

  /**
   * All-args constructor.
   * @param EventId The new value for EventId
   * @param hostId host id
   * @param apiId api id
   * @param apiName api name
   * @param apiStatus api status
   * @param value service detail in JSON
   */
  public ServiceUpdatedEvent(com.networknt.kafka.common.EventId EventId, java.lang.String hostId, java.lang.String apiId, java.lang.String apiName, java.lang.String apiStatus, java.lang.String value) {
    this.EventId = EventId;
    this.hostId = hostId;
    this.apiId = apiId;
    this.apiName = apiName;
    this.apiStatus = apiStatus;
    this.value = value;
  }

  @Override
  public org.apache.avro.specific.SpecificData getSpecificData() { return MODEL$; }

  @Override
  public org.apache.avro.Schema getSchema() { return SCHEMA$; }

  // Used by DatumWriter.  Applications should not call.
  @Override
  public java.lang.Object get(int field$) {
    switch (field$) {
    case 0: return EventId;
    case 1: return hostId;
    case 2: return apiId;
    case 3: return apiName;
    case 4: return apiStatus;
    case 5: return value;
    default: throw new IndexOutOfBoundsException("Invalid index: " + field$);
    }
  }

  // Used by DatumReader.  Applications should not call.
  @Override
  @SuppressWarnings(value="unchecked")
  public void put(int field$, java.lang.Object value$) {
    switch (field$) {
    case 0: EventId = (com.networknt.kafka.common.EventId)value$; break;
    case 1: hostId = value$ != null ? value$.toString() : null; break;
    case 2: apiId = value$ != null ? value$.toString() : null; break;
    case 3: apiName = value$ != null ? value$.toString() : null; break;
    case 4: apiStatus = value$ != null ? value$.toString() : null; break;
    case 5: value = value$ != null ? value$.toString() : null; break;
    default: throw new IndexOutOfBoundsException("Invalid index: " + field$);
    }
  }

  /**
   * Gets the value of the 'EventId' field.
   * @return The value of the 'EventId' field.
   */
  public com.networknt.kafka.common.EventId getEventId() {
    return EventId;
  }


  /**
   * Sets the value of the 'EventId' field.
   * @param value the value to set.
   */
  public void setEventId(com.networknt.kafka.common.EventId value) {
    this.EventId = value;
  }

  /**
   * Gets the value of the 'hostId' field.
   * @return host id
   */
  public java.lang.String getHostId() {
    return hostId;
  }


  /**
   * Sets the value of the 'hostId' field.
   * host id
   * @param value the value to set.
   */
  public void setHostId(java.lang.String value) {
    this.hostId = value;
  }

  /**
   * Gets the value of the 'apiId' field.
   * @return api id
   */
  public java.lang.String getApiId() {
    return apiId;
  }


  /**
   * Sets the value of the 'apiId' field.
   * api id
   * @param value the value to set.
   */
  public void setApiId(java.lang.String value) {
    this.apiId = value;
  }

  /**
   * Gets the value of the 'apiName' field.
   * @return api name
   */
  public java.lang.String getApiName() {
    return apiName;
  }


  /**
   * Sets the value of the 'apiName' field.
   * api name
   * @param value the value to set.
   */
  public void setApiName(java.lang.String value) {
    this.apiName = value;
  }

  /**
   * Gets the value of the 'apiStatus' field.
   * @return api status
   */
  public java.lang.String getApiStatus() {
    return apiStatus;
  }


  /**
   * Sets the value of the 'apiStatus' field.
   * api status
   * @param value the value to set.
   */
  public void setApiStatus(java.lang.String value) {
    this.apiStatus = value;
  }

  /**
   * Gets the value of the 'value' field.
   * @return service detail in JSON
   */
  public java.lang.String getValue() {
    return value;
  }


  /**
   * Sets the value of the 'value' field.
   * service detail in JSON
   * @param value the value to set.
   */
  public void setValue(java.lang.String value) {
    this.value = value;
  }

  /**
   * Creates a new ServiceUpdatedEvent RecordBuilder.
   * @return A new ServiceUpdatedEvent RecordBuilder
   */
  public static net.lightapi.portal.service.ServiceUpdatedEvent.Builder newBuilder() {
    return new net.lightapi.portal.service.ServiceUpdatedEvent.Builder();
  }

  /**
   * Creates a new ServiceUpdatedEvent RecordBuilder by copying an existing Builder.
   * @param other The existing builder to copy.
   * @return A new ServiceUpdatedEvent RecordBuilder
   */
  public static net.lightapi.portal.service.ServiceUpdatedEvent.Builder newBuilder(net.lightapi.portal.service.ServiceUpdatedEvent.Builder other) {
    if (other == null) {
      return new net.lightapi.portal.service.ServiceUpdatedEvent.Builder();
    } else {
      return new net.lightapi.portal.service.ServiceUpdatedEvent.Builder(other);
    }
  }

  /**
   * Creates a new ServiceUpdatedEvent RecordBuilder by copying an existing ServiceUpdatedEvent instance.
   * @param other The existing instance to copy.
   * @return A new ServiceUpdatedEvent RecordBuilder
   */
  public static net.lightapi.portal.service.ServiceUpdatedEvent.Builder newBuilder(net.lightapi.portal.service.ServiceUpdatedEvent other) {
    if (other == null) {
      return new net.lightapi.portal.service.ServiceUpdatedEvent.Builder();
    } else {
      return new net.lightapi.portal.service.ServiceUpdatedEvent.Builder(other);
    }
  }

  /**
   * RecordBuilder for ServiceUpdatedEvent instances.
   */
  @org.apache.avro.specific.AvroGenerated
  public static class Builder extends org.apache.avro.specific.SpecificRecordBuilderBase<ServiceUpdatedEvent>
    implements org.apache.avro.data.RecordBuilder<ServiceUpdatedEvent> {

    private com.networknt.kafka.common.EventId EventId;
    private com.networknt.kafka.common.EventId.Builder EventIdBuilder;
    /** host id */
    private java.lang.String hostId;
    /** api id */
    private java.lang.String apiId;
    /** api name */
    private java.lang.String apiName;
    /** api status */
    private java.lang.String apiStatus;
    /** service detail in JSON */
    private java.lang.String value;

    /** Creates a new Builder */
    private Builder() {
      super(SCHEMA$, MODEL$);
    }

    /**
     * Creates a Builder by copying an existing Builder.
     * @param other The existing Builder to copy.
     */
    private Builder(net.lightapi.portal.service.ServiceUpdatedEvent.Builder other) {
      super(other);
      if (isValidValue(fields()[0], other.EventId)) {
        this.EventId = data().deepCopy(fields()[0].schema(), other.EventId);
        fieldSetFlags()[0] = other.fieldSetFlags()[0];
      }
      if (other.hasEventIdBuilder()) {
        this.EventIdBuilder = com.networknt.kafka.common.EventId.newBuilder(other.getEventIdBuilder());
      }
      if (isValidValue(fields()[1], other.hostId)) {
        this.hostId = data().deepCopy(fields()[1].schema(), other.hostId);
        fieldSetFlags()[1] = other.fieldSetFlags()[1];
      }
      if (isValidValue(fields()[2], other.apiId)) {
        this.apiId = data().deepCopy(fields()[2].schema(), other.apiId);
        fieldSetFlags()[2] = other.fieldSetFlags()[2];
      }
      if (isValidValue(fields()[3], other.apiName)) {
        this.apiName = data().deepCopy(fields()[3].schema(), other.apiName);
        fieldSetFlags()[3] = other.fieldSetFlags()[3];
      }
      if (isValidValue(fields()[4], other.apiStatus)) {
        this.apiStatus = data().deepCopy(fields()[4].schema(), other.apiStatus);
        fieldSetFlags()[4] = other.fieldSetFlags()[4];
      }
      if (isValidValue(fields()[5], other.value)) {
        this.value = data().deepCopy(fields()[5].schema(), other.value);
        fieldSetFlags()[5] = other.fieldSetFlags()[5];
      }
    }

    /**
     * Creates a Builder by copying an existing ServiceUpdatedEvent instance
     * @param other The existing instance to copy.
     */
    private Builder(net.lightapi.portal.service.ServiceUpdatedEvent other) {
      super(SCHEMA$, MODEL$);
      if (isValidValue(fields()[0], other.EventId)) {
        this.EventId = data().deepCopy(fields()[0].schema(), other.EventId);
        fieldSetFlags()[0] = true;
      }
      this.EventIdBuilder = null;
      if (isValidValue(fields()[1], other.hostId)) {
        this.hostId = data().deepCopy(fields()[1].schema(), other.hostId);
        fieldSetFlags()[1] = true;
      }
      if (isValidValue(fields()[2], other.apiId)) {
        this.apiId = data().deepCopy(fields()[2].schema(), other.apiId);
        fieldSetFlags()[2] = true;
      }
      if (isValidValue(fields()[3], other.apiName)) {
        this.apiName = data().deepCopy(fields()[3].schema(), other.apiName);
        fieldSetFlags()[3] = true;
      }
      if (isValidValue(fields()[4], other.apiStatus)) {
        this.apiStatus = data().deepCopy(fields()[4].schema(), other.apiStatus);
        fieldSetFlags()[4] = true;
      }
      if (isValidValue(fields()[5], other.value)) {
        this.value = data().deepCopy(fields()[5].schema(), other.value);
        fieldSetFlags()[5] = true;
      }
    }

    /**
      * Gets the value of the 'EventId' field.
      * @return The value.
      */
    public com.networknt.kafka.common.EventId getEventId() {
      return EventId;
    }


    /**
      * Sets the value of the 'EventId' field.
      * @param value The value of 'EventId'.
      * @return This builder.
      */
    public net.lightapi.portal.service.ServiceUpdatedEvent.Builder setEventId(com.networknt.kafka.common.EventId value) {
      validate(fields()[0], value);
      this.EventIdBuilder = null;
      this.EventId = value;
      fieldSetFlags()[0] = true;
      return this;
    }

    /**
      * Checks whether the 'EventId' field has been set.
      * @return True if the 'EventId' field has been set, false otherwise.
      */
    public boolean hasEventId() {
      return fieldSetFlags()[0];
    }

    /**
     * Gets the Builder instance for the 'EventId' field and creates one if it doesn't exist yet.
     * @return This builder.
     */
    public com.networknt.kafka.common.EventId.Builder getEventIdBuilder() {
      if (EventIdBuilder == null) {
        if (hasEventId()) {
          setEventIdBuilder(com.networknt.kafka.common.EventId.newBuilder(EventId));
        } else {
          setEventIdBuilder(com.networknt.kafka.common.EventId.newBuilder());
        }
      }
      return EventIdBuilder;
    }

    /**
     * Sets the Builder instance for the 'EventId' field
     * @param value The builder instance that must be set.
     * @return This builder.
     */

    public net.lightapi.portal.service.ServiceUpdatedEvent.Builder setEventIdBuilder(com.networknt.kafka.common.EventId.Builder value) {
      clearEventId();
      EventIdBuilder = value;
      return this;
    }

    /**
     * Checks whether the 'EventId' field has an active Builder instance
     * @return True if the 'EventId' field has an active Builder instance
     */
    public boolean hasEventIdBuilder() {
      return EventIdBuilder != null;
    }

    /**
      * Clears the value of the 'EventId' field.
      * @return This builder.
      */
    public net.lightapi.portal.service.ServiceUpdatedEvent.Builder clearEventId() {
      EventId = null;
      EventIdBuilder = null;
      fieldSetFlags()[0] = false;
      return this;
    }

    /**
      * Gets the value of the 'hostId' field.
      * host id
      * @return The value.
      */
    public java.lang.String getHostId() {
      return hostId;
    }


    /**
      * Sets the value of the 'hostId' field.
      * host id
      * @param value The value of 'hostId'.
      * @return This builder.
      */
    public net.lightapi.portal.service.ServiceUpdatedEvent.Builder setHostId(java.lang.String value) {
      validate(fields()[1], value);
      this.hostId = value;
      fieldSetFlags()[1] = true;
      return this;
    }

    /**
      * Checks whether the 'hostId' field has been set.
      * host id
      * @return True if the 'hostId' field has been set, false otherwise.
      */
    public boolean hasHostId() {
      return fieldSetFlags()[1];
    }


    /**
      * Clears the value of the 'hostId' field.
      * host id
      * @return This builder.
      */
    public net.lightapi.portal.service.ServiceUpdatedEvent.Builder clearHostId() {
      hostId = null;
      fieldSetFlags()[1] = false;
      return this;
    }

    /**
      * Gets the value of the 'apiId' field.
      * api id
      * @return The value.
      */
    public java.lang.String getApiId() {
      return apiId;
    }


    /**
      * Sets the value of the 'apiId' field.
      * api id
      * @param value The value of 'apiId'.
      * @return This builder.
      */
    public net.lightapi.portal.service.ServiceUpdatedEvent.Builder setApiId(java.lang.String value) {
      validate(fields()[2], value);
      this.apiId = value;
      fieldSetFlags()[2] = true;
      return this;
    }

    /**
      * Checks whether the 'apiId' field has been set.
      * api id
      * @return True if the 'apiId' field has been set, false otherwise.
      */
    public boolean hasApiId() {
      return fieldSetFlags()[2];
    }


    /**
      * Clears the value of the 'apiId' field.
      * api id
      * @return This builder.
      */
    public net.lightapi.portal.service.ServiceUpdatedEvent.Builder clearApiId() {
      apiId = null;
      fieldSetFlags()[2] = false;
      return this;
    }

    /**
      * Gets the value of the 'apiName' field.
      * api name
      * @return The value.
      */
    public java.lang.String getApiName() {
      return apiName;
    }


    /**
      * Sets the value of the 'apiName' field.
      * api name
      * @param value The value of 'apiName'.
      * @return This builder.
      */
    public net.lightapi.portal.service.ServiceUpdatedEvent.Builder setApiName(java.lang.String value) {
      validate(fields()[3], value);
      this.apiName = value;
      fieldSetFlags()[3] = true;
      return this;
    }

    /**
      * Checks whether the 'apiName' field has been set.
      * api name
      * @return True if the 'apiName' field has been set, false otherwise.
      */
    public boolean hasApiName() {
      return fieldSetFlags()[3];
    }


    /**
      * Clears the value of the 'apiName' field.
      * api name
      * @return This builder.
      */
    public net.lightapi.portal.service.ServiceUpdatedEvent.Builder clearApiName() {
      apiName = null;
      fieldSetFlags()[3] = false;
      return this;
    }

    /**
      * Gets the value of the 'apiStatus' field.
      * api status
      * @return The value.
      */
    public java.lang.String getApiStatus() {
      return apiStatus;
    }


    /**
      * Sets the value of the 'apiStatus' field.
      * api status
      * @param value The value of 'apiStatus'.
      * @return This builder.
      */
    public net.lightapi.portal.service.ServiceUpdatedEvent.Builder setApiStatus(java.lang.String value) {
      validate(fields()[4], value);
      this.apiStatus = value;
      fieldSetFlags()[4] = true;
      return this;
    }

    /**
      * Checks whether the 'apiStatus' field has been set.
      * api status
      * @return True if the 'apiStatus' field has been set, false otherwise.
      */
    public boolean hasApiStatus() {
      return fieldSetFlags()[4];
    }


    /**
      * Clears the value of the 'apiStatus' field.
      * api status
      * @return This builder.
      */
    public net.lightapi.portal.service.ServiceUpdatedEvent.Builder clearApiStatus() {
      apiStatus = null;
      fieldSetFlags()[4] = false;
      return this;
    }

    /**
      * Gets the value of the 'value' field.
      * service detail in JSON
      * @return The value.
      */
    public java.lang.String getValue() {
      return value;
    }


    /**
      * Sets the value of the 'value' field.
      * service detail in JSON
      * @param value The value of 'value'.
      * @return This builder.
      */
    public net.lightapi.portal.service.ServiceUpdatedEvent.Builder setValue(java.lang.String value) {
      validate(fields()[5], value);
      this.value = value;
      fieldSetFlags()[5] = true;
      return this;
    }

    /**
      * Checks whether the 'value' field has been set.
      * service detail in JSON
      * @return True if the 'value' field has been set, false otherwise.
      */
    public boolean hasValue() {
      return fieldSetFlags()[5];
    }


    /**
      * Clears the value of the 'value' field.
      * service detail in JSON
      * @return This builder.
      */
    public net.lightapi.portal.service.ServiceUpdatedEvent.Builder clearValue() {
      value = null;
      fieldSetFlags()[5] = false;
      return this;
    }

    @Override
    @SuppressWarnings("unchecked")
    public ServiceUpdatedEvent build() {
      try {
        ServiceUpdatedEvent record = new ServiceUpdatedEvent();
        if (EventIdBuilder != null) {
          try {
            record.EventId = this.EventIdBuilder.build();
          } catch (org.apache.avro.AvroMissingFieldException e) {
            e.addParentField(record.getSchema().getField("EventId"));
            throw e;
          }
        } else {
          record.EventId = fieldSetFlags()[0] ? this.EventId : (com.networknt.kafka.common.EventId) defaultValue(fields()[0]);
        }
        record.hostId = fieldSetFlags()[1] ? this.hostId : (java.lang.String) defaultValue(fields()[1]);
        record.apiId = fieldSetFlags()[2] ? this.apiId : (java.lang.String) defaultValue(fields()[2]);
        record.apiName = fieldSetFlags()[3] ? this.apiName : (java.lang.String) defaultValue(fields()[3]);
        record.apiStatus = fieldSetFlags()[4] ? this.apiStatus : (java.lang.String) defaultValue(fields()[4]);
        record.value = fieldSetFlags()[5] ? this.value : (java.lang.String) defaultValue(fields()[5]);
        return record;
      } catch (org.apache.avro.AvroMissingFieldException e) {
        throw e;
      } catch (java.lang.Exception e) {
        throw new org.apache.avro.AvroRuntimeException(e);
      }
    }
  }

  @SuppressWarnings("unchecked")
  private static final org.apache.avro.io.DatumWriter<ServiceUpdatedEvent>
    WRITER$ = (org.apache.avro.io.DatumWriter<ServiceUpdatedEvent>)MODEL$.createDatumWriter(SCHEMA$);

  @Override public void writeExternal(java.io.ObjectOutput out)
    throws java.io.IOException {
    WRITER$.write(this, SpecificData.getEncoder(out));
  }

  @SuppressWarnings("unchecked")
  private static final org.apache.avro.io.DatumReader<ServiceUpdatedEvent>
    READER$ = (org.apache.avro.io.DatumReader<ServiceUpdatedEvent>)MODEL$.createDatumReader(SCHEMA$);

  @Override public void readExternal(java.io.ObjectInput in)
    throws java.io.IOException {
    READER$.read(this, SpecificData.getDecoder(in));
  }

  @Override protected boolean hasCustomCoders() { return true; }

  @Override public void customEncode(org.apache.avro.io.Encoder out)
    throws java.io.IOException
  {
    this.EventId.customEncode(out);

    out.writeString(this.hostId);

    out.writeString(this.apiId);

    out.writeString(this.apiName);

    out.writeString(this.apiStatus);

    out.writeString(this.value);

  }

  @Override public void customDecode(org.apache.avro.io.ResolvingDecoder in)
    throws java.io.IOException
  {
    org.apache.avro.Schema.Field[] fieldOrder = in.readFieldOrderIfDiff();
    if (fieldOrder == null) {
      if (this.EventId == null) {
        this.EventId = new com.networknt.kafka.common.EventId();
      }
      this.EventId.customDecode(in);

      this.hostId = in.readString();

      this.apiId = in.readString();

      this.apiName = in.readString();

      this.apiStatus = in.readString();

      this.value = in.readString();

    } else {
      for (int i = 0; i < 6; i++) {
        switch (fieldOrder[i].pos()) {
        case 0:
          if (this.EventId == null) {
            this.EventId = new com.networknt.kafka.common.EventId();
          }
          this.EventId.customDecode(in);
          break;

        case 1:
          this.hostId = in.readString();
          break;

        case 2:
          this.apiId = in.readString();
          break;

        case 3:
          this.apiName = in.readString();
          break;

        case 4:
          this.apiStatus = in.readString();
          break;

        case 5:
          this.value = in.readString();
          break;

        default:
          throw new java.io.IOException("Corrupt ResolvingDecoder.");
        }
      }
    }
  }
}
