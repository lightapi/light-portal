/**
 * Autogenerated by Avro
 *
 * DO NOT EDIT DIRECTLY
 */
package net.lightapi.portal.market;

import org.apache.avro.specific.SpecificData;
import org.apache.avro.util.Utf8;
import org.apache.avro.message.BinaryMessageEncoder;
import org.apache.avro.message.BinaryMessageDecoder;
import org.apache.avro.message.SchemaStore;

@org.apache.avro.specific.AvroGenerated
public class JsonSchemaDeletedEvent extends org.apache.avro.specific.SpecificRecordBase implements org.apache.avro.specific.SpecificRecord {
  private static final long serialVersionUID = 4626989008894921885L;


  public static final org.apache.avro.Schema SCHEMA$ = new org.apache.avro.Schema.Parser().parse("{\"type\":\"record\",\"name\":\"JsonSchemaDeletedEvent\",\"namespace\":\"net.lightapi.portal.market\",\"fields\":[{\"name\":\"EventId\",\"type\":{\"type\":\"record\",\"name\":\"EventId\",\"namespace\":\"com.networknt.kafka.common\",\"fields\":[{\"name\":\"id\",\"type\":{\"type\":\"string\",\"avro.java.string\":\"String\"},\"doc\":\"a unique identifier for the event\"},{\"name\":\"userId\",\"type\":{\"type\":\"string\",\"avro.java.string\":\"String\"},\"doc\":\"the user who creates the event\"},{\"name\":\"hostId\",\"type\":{\"type\":\"string\",\"avro.java.string\":\"String\"},\"doc\":\"the host which is the event is created\"},{\"name\":\"nonce\",\"type\":\"long\",\"doc\":\"the number of the transactions for the user\"},{\"name\":\"timestamp\",\"type\":\"long\",\"doc\":\"time the event is recorded\",\"default\":0},{\"name\":\"derived\",\"type\":\"boolean\",\"doc\":\"indicate if the event is derived from event processor\",\"default\":false}]}},{\"name\":\"hostId\",\"type\":{\"type\":\"string\",\"avro.java.string\":\"String\"},\"doc\":\"host id\"},{\"name\":\"id\",\"type\":{\"type\":\"string\",\"avro.java.string\":\"String\"},\"doc\":\"id\"}]}");
  public static org.apache.avro.Schema getClassSchema() { return SCHEMA$; }

  private static final SpecificData MODEL$ = new SpecificData();

  private static final BinaryMessageEncoder<JsonSchemaDeletedEvent> ENCODER =
      new BinaryMessageEncoder<>(MODEL$, SCHEMA$);

  private static final BinaryMessageDecoder<JsonSchemaDeletedEvent> DECODER =
      new BinaryMessageDecoder<>(MODEL$, SCHEMA$);

  /**
   * Return the BinaryMessageEncoder instance used by this class.
   * @return the message encoder used by this class
   */
  public static BinaryMessageEncoder<JsonSchemaDeletedEvent> getEncoder() {
    return ENCODER;
  }

  /**
   * Return the BinaryMessageDecoder instance used by this class.
   * @return the message decoder used by this class
   */
  public static BinaryMessageDecoder<JsonSchemaDeletedEvent> getDecoder() {
    return DECODER;
  }

  /**
   * Create a new BinaryMessageDecoder instance for this class that uses the specified {@link SchemaStore}.
   * @param resolver a {@link SchemaStore} used to find schemas by fingerprint
   * @return a BinaryMessageDecoder instance for this class backed by the given SchemaStore
   */
  public static BinaryMessageDecoder<JsonSchemaDeletedEvent> createDecoder(SchemaStore resolver) {
    return new BinaryMessageDecoder<>(MODEL$, SCHEMA$, resolver);
  }

  /**
   * Serializes this JsonSchemaDeletedEvent to a ByteBuffer.
   * @return a buffer holding the serialized data for this instance
   * @throws java.io.IOException if this instance could not be serialized
   */
  public java.nio.ByteBuffer toByteBuffer() throws java.io.IOException {
    return ENCODER.encode(this);
  }

  /**
   * Deserializes a JsonSchemaDeletedEvent from a ByteBuffer.
   * @param b a byte buffer holding serialized data for an instance of this class
   * @return a JsonSchemaDeletedEvent instance decoded from the given buffer
   * @throws java.io.IOException if the given bytes could not be deserialized into an instance of this class
   */
  public static JsonSchemaDeletedEvent fromByteBuffer(
      java.nio.ByteBuffer b) throws java.io.IOException {
    return DECODER.decode(b);
  }

  private com.networknt.kafka.common.EventId EventId;
  /** host id */
  private java.lang.String hostId;
  /** id */
  private java.lang.String id;

  /**
   * Default constructor.  Note that this does not initialize fields
   * to their default values from the schema.  If that is desired then
   * one should use <code>newBuilder()</code>.
   */
  public JsonSchemaDeletedEvent() {}

  /**
   * All-args constructor.
   * @param EventId The new value for EventId
   * @param hostId host id
   * @param id id
   */
  public JsonSchemaDeletedEvent(com.networknt.kafka.common.EventId EventId, java.lang.String hostId, java.lang.String id) {
    this.EventId = EventId;
    this.hostId = hostId;
    this.id = id;
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
    case 2: return id;
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
    case 2: id = value$ != null ? value$.toString() : null; break;
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
   * Gets the value of the 'id' field.
   * @return id
   */
  public java.lang.String getId() {
    return id;
  }


  /**
   * Sets the value of the 'id' field.
   * id
   * @param value the value to set.
   */
  public void setId(java.lang.String value) {
    this.id = value;
  }

  /**
   * Creates a new JsonSchemaDeletedEvent RecordBuilder.
   * @return A new JsonSchemaDeletedEvent RecordBuilder
   */
  public static net.lightapi.portal.market.JsonSchemaDeletedEvent.Builder newBuilder() {
    return new net.lightapi.portal.market.JsonSchemaDeletedEvent.Builder();
  }

  /**
   * Creates a new JsonSchemaDeletedEvent RecordBuilder by copying an existing Builder.
   * @param other The existing builder to copy.
   * @return A new JsonSchemaDeletedEvent RecordBuilder
   */
  public static net.lightapi.portal.market.JsonSchemaDeletedEvent.Builder newBuilder(net.lightapi.portal.market.JsonSchemaDeletedEvent.Builder other) {
    if (other == null) {
      return new net.lightapi.portal.market.JsonSchemaDeletedEvent.Builder();
    } else {
      return new net.lightapi.portal.market.JsonSchemaDeletedEvent.Builder(other);
    }
  }

  /**
   * Creates a new JsonSchemaDeletedEvent RecordBuilder by copying an existing JsonSchemaDeletedEvent instance.
   * @param other The existing instance to copy.
   * @return A new JsonSchemaDeletedEvent RecordBuilder
   */
  public static net.lightapi.portal.market.JsonSchemaDeletedEvent.Builder newBuilder(net.lightapi.portal.market.JsonSchemaDeletedEvent other) {
    if (other == null) {
      return new net.lightapi.portal.market.JsonSchemaDeletedEvent.Builder();
    } else {
      return new net.lightapi.portal.market.JsonSchemaDeletedEvent.Builder(other);
    }
  }

  /**
   * RecordBuilder for JsonSchemaDeletedEvent instances.
   */
  @org.apache.avro.specific.AvroGenerated
  public static class Builder extends org.apache.avro.specific.SpecificRecordBuilderBase<JsonSchemaDeletedEvent>
    implements org.apache.avro.data.RecordBuilder<JsonSchemaDeletedEvent> {

    private com.networknt.kafka.common.EventId EventId;
    private com.networknt.kafka.common.EventId.Builder EventIdBuilder;
    /** host id */
    private java.lang.String hostId;
    /** id */
    private java.lang.String id;

    /** Creates a new Builder */
    private Builder() {
      super(SCHEMA$, MODEL$);
    }

    /**
     * Creates a Builder by copying an existing Builder.
     * @param other The existing Builder to copy.
     */
    private Builder(net.lightapi.portal.market.JsonSchemaDeletedEvent.Builder other) {
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
      if (isValidValue(fields()[2], other.id)) {
        this.id = data().deepCopy(fields()[2].schema(), other.id);
        fieldSetFlags()[2] = other.fieldSetFlags()[2];
      }
    }

    /**
     * Creates a Builder by copying an existing JsonSchemaDeletedEvent instance
     * @param other The existing instance to copy.
     */
    private Builder(net.lightapi.portal.market.JsonSchemaDeletedEvent other) {
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
      if (isValidValue(fields()[2], other.id)) {
        this.id = data().deepCopy(fields()[2].schema(), other.id);
        fieldSetFlags()[2] = true;
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
    public net.lightapi.portal.market.JsonSchemaDeletedEvent.Builder setEventId(com.networknt.kafka.common.EventId value) {
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

    public net.lightapi.portal.market.JsonSchemaDeletedEvent.Builder setEventIdBuilder(com.networknt.kafka.common.EventId.Builder value) {
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
    public net.lightapi.portal.market.JsonSchemaDeletedEvent.Builder clearEventId() {
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
    public net.lightapi.portal.market.JsonSchemaDeletedEvent.Builder setHostId(java.lang.String value) {
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
    public net.lightapi.portal.market.JsonSchemaDeletedEvent.Builder clearHostId() {
      hostId = null;
      fieldSetFlags()[1] = false;
      return this;
    }

    /**
      * Gets the value of the 'id' field.
      * id
      * @return The value.
      */
    public java.lang.String getId() {
      return id;
    }


    /**
      * Sets the value of the 'id' field.
      * id
      * @param value The value of 'id'.
      * @return This builder.
      */
    public net.lightapi.portal.market.JsonSchemaDeletedEvent.Builder setId(java.lang.String value) {
      validate(fields()[2], value);
      this.id = value;
      fieldSetFlags()[2] = true;
      return this;
    }

    /**
      * Checks whether the 'id' field has been set.
      * id
      * @return True if the 'id' field has been set, false otherwise.
      */
    public boolean hasId() {
      return fieldSetFlags()[2];
    }


    /**
      * Clears the value of the 'id' field.
      * id
      * @return This builder.
      */
    public net.lightapi.portal.market.JsonSchemaDeletedEvent.Builder clearId() {
      id = null;
      fieldSetFlags()[2] = false;
      return this;
    }

    @Override
    @SuppressWarnings("unchecked")
    public JsonSchemaDeletedEvent build() {
      try {
        JsonSchemaDeletedEvent record = new JsonSchemaDeletedEvent();
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
        record.id = fieldSetFlags()[2] ? this.id : (java.lang.String) defaultValue(fields()[2]);
        return record;
      } catch (org.apache.avro.AvroMissingFieldException e) {
        throw e;
      } catch (java.lang.Exception e) {
        throw new org.apache.avro.AvroRuntimeException(e);
      }
    }
  }

  @SuppressWarnings("unchecked")
  private static final org.apache.avro.io.DatumWriter<JsonSchemaDeletedEvent>
    WRITER$ = (org.apache.avro.io.DatumWriter<JsonSchemaDeletedEvent>)MODEL$.createDatumWriter(SCHEMA$);

  @Override public void writeExternal(java.io.ObjectOutput out)
    throws java.io.IOException {
    WRITER$.write(this, SpecificData.getEncoder(out));
  }

  @SuppressWarnings("unchecked")
  private static final org.apache.avro.io.DatumReader<JsonSchemaDeletedEvent>
    READER$ = (org.apache.avro.io.DatumReader<JsonSchemaDeletedEvent>)MODEL$.createDatumReader(SCHEMA$);

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

    out.writeString(this.id);

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

      this.id = in.readString();

    } else {
      for (int i = 0; i < 3; i++) {
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
          this.id = in.readString();
          break;

        default:
          throw new java.io.IOException("Corrupt ResolvingDecoder.");
        }
      }
    }
  }
}
