/**
 * Autogenerated by Avro
 *
 * DO NOT EDIT DIRECTLY
 */
package net.lightapi.portal.user;

import org.apache.avro.generic.GenericArray;
import org.apache.avro.specific.SpecificData;
import org.apache.avro.util.Utf8;
import org.apache.avro.message.BinaryMessageEncoder;
import org.apache.avro.message.BinaryMessageDecoder;
import org.apache.avro.message.SchemaStore;

@org.apache.avro.specific.AvroGenerated
public class OrderCreatedEvent extends org.apache.avro.specific.SpecificRecordBase implements org.apache.avro.specific.SpecificRecord {
  private static final long serialVersionUID = 6922094065493488945L;


  public static final org.apache.avro.Schema SCHEMA$ = new org.apache.avro.Schema.Parser().parse("{\"type\":\"record\",\"name\":\"OrderCreatedEvent\",\"namespace\":\"net.lightapi.portal.user\",\"fields\":[{\"name\":\"EventId\",\"type\":{\"type\":\"record\",\"name\":\"EventId\",\"namespace\":\"com.networknt.kafka.common\",\"fields\":[{\"name\":\"id\",\"type\":{\"type\":\"string\",\"avro.java.string\":\"String\"},\"doc\":\"a unique identifier\"},{\"name\":\"nonce\",\"type\":\"long\",\"doc\":\"the number of the transactions for the id\"},{\"name\":\"derived\",\"type\":\"boolean\",\"doc\":\"indicate if the event is derived from event processor\",\"default\":false}]}},{\"name\":\"email\",\"type\":{\"type\":\"string\",\"avro.java.string\":\"String\"},\"doc\":\"email of the merchant\"},{\"name\":\"order\",\"type\":{\"type\":\"string\",\"avro.java.string\":\"String\"},\"doc\":\"An order object\"},{\"name\":\"timestamp\",\"type\":\"long\",\"doc\":\"time the event is recorded\",\"default\":0}]}");
  public static org.apache.avro.Schema getClassSchema() { return SCHEMA$; }

  private static final SpecificData MODEL$ = new SpecificData();

  private static final BinaryMessageEncoder<OrderCreatedEvent> ENCODER =
      new BinaryMessageEncoder<OrderCreatedEvent>(MODEL$, SCHEMA$);

  private static final BinaryMessageDecoder<OrderCreatedEvent> DECODER =
      new BinaryMessageDecoder<OrderCreatedEvent>(MODEL$, SCHEMA$);

  /**
   * Return the BinaryMessageEncoder instance used by this class.
   * @return the message encoder used by this class
   */
  public static BinaryMessageEncoder<OrderCreatedEvent> getEncoder() {
    return ENCODER;
  }

  /**
   * Return the BinaryMessageDecoder instance used by this class.
   * @return the message decoder used by this class
   */
  public static BinaryMessageDecoder<OrderCreatedEvent> getDecoder() {
    return DECODER;
  }

  /**
   * Create a new BinaryMessageDecoder instance for this class that uses the specified {@link SchemaStore}.
   * @param resolver a {@link SchemaStore} used to find schemas by fingerprint
   * @return a BinaryMessageDecoder instance for this class backed by the given SchemaStore
   */
  public static BinaryMessageDecoder<OrderCreatedEvent> createDecoder(SchemaStore resolver) {
    return new BinaryMessageDecoder<OrderCreatedEvent>(MODEL$, SCHEMA$, resolver);
  }

  /**
   * Serializes this OrderCreatedEvent to a ByteBuffer.
   * @return a buffer holding the serialized data for this instance
   * @throws java.io.IOException if this instance could not be serialized
   */
  public java.nio.ByteBuffer toByteBuffer() throws java.io.IOException {
    return ENCODER.encode(this);
  }

  /**
   * Deserializes a OrderCreatedEvent from a ByteBuffer.
   * @param b a byte buffer holding serialized data for an instance of this class
   * @return a OrderCreatedEvent instance decoded from the given buffer
   * @throws java.io.IOException if the given bytes could not be deserialized into an instance of this class
   */
  public static OrderCreatedEvent fromByteBuffer(
      java.nio.ByteBuffer b) throws java.io.IOException {
    return DECODER.decode(b);
  }

  private com.networknt.kafka.common.EventId EventId;
  /** email of the merchant */
  private java.lang.String email;
  /** An order object */
  private java.lang.String order;
  /** time the event is recorded */
  private long timestamp;

  /**
   * Default constructor.  Note that this does not initialize fields
   * to their default values from the schema.  If that is desired then
   * one should use <code>newBuilder()</code>.
   */
  public OrderCreatedEvent() {}

  /**
   * All-args constructor.
   * @param EventId The new value for EventId
   * @param email email of the merchant
   * @param order An order object
   * @param timestamp time the event is recorded
   */
  public OrderCreatedEvent(com.networknt.kafka.common.EventId EventId, java.lang.String email, java.lang.String order, java.lang.Long timestamp) {
    this.EventId = EventId;
    this.email = email;
    this.order = order;
    this.timestamp = timestamp;
  }

  public org.apache.avro.specific.SpecificData getSpecificData() { return MODEL$; }
  public org.apache.avro.Schema getSchema() { return SCHEMA$; }
  // Used by DatumWriter.  Applications should not call.
  public java.lang.Object get(int field$) {
    switch (field$) {
    case 0: return EventId;
    case 1: return email;
    case 2: return order;
    case 3: return timestamp;
    default: throw new IndexOutOfBoundsException("Invalid index: " + field$);
    }
  }

  // Used by DatumReader.  Applications should not call.
  @SuppressWarnings(value="unchecked")
  public void put(int field$, java.lang.Object value$) {
    switch (field$) {
    case 0: EventId = (com.networknt.kafka.common.EventId)value$; break;
    case 1: email = value$ != null ? value$.toString() : null; break;
    case 2: order = value$ != null ? value$.toString() : null; break;
    case 3: timestamp = (java.lang.Long)value$; break;
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
   * Gets the value of the 'email' field.
   * @return email of the merchant
   */
  public java.lang.String getEmail() {
    return email;
  }


  /**
   * Sets the value of the 'email' field.
   * email of the merchant
   * @param value the value to set.
   */
  public void setEmail(java.lang.String value) {
    this.email = value;
  }

  /**
   * Gets the value of the 'order' field.
   * @return An order object
   */
  public java.lang.String getOrder() {
    return order;
  }


  /**
   * Sets the value of the 'order' field.
   * An order object
   * @param value the value to set.
   */
  public void setOrder(java.lang.String value) {
    this.order = value;
  }

  /**
   * Gets the value of the 'timestamp' field.
   * @return time the event is recorded
   */
  public long getTimestamp() {
    return timestamp;
  }


  /**
   * Sets the value of the 'timestamp' field.
   * time the event is recorded
   * @param value the value to set.
   */
  public void setTimestamp(long value) {
    this.timestamp = value;
  }

  /**
   * Creates a new OrderCreatedEvent RecordBuilder.
   * @return A new OrderCreatedEvent RecordBuilder
   */
  public static net.lightapi.portal.user.OrderCreatedEvent.Builder newBuilder() {
    return new net.lightapi.portal.user.OrderCreatedEvent.Builder();
  }

  /**
   * Creates a new OrderCreatedEvent RecordBuilder by copying an existing Builder.
   * @param other The existing builder to copy.
   * @return A new OrderCreatedEvent RecordBuilder
   */
  public static net.lightapi.portal.user.OrderCreatedEvent.Builder newBuilder(net.lightapi.portal.user.OrderCreatedEvent.Builder other) {
    if (other == null) {
      return new net.lightapi.portal.user.OrderCreatedEvent.Builder();
    } else {
      return new net.lightapi.portal.user.OrderCreatedEvent.Builder(other);
    }
  }

  /**
   * Creates a new OrderCreatedEvent RecordBuilder by copying an existing OrderCreatedEvent instance.
   * @param other The existing instance to copy.
   * @return A new OrderCreatedEvent RecordBuilder
   */
  public static net.lightapi.portal.user.OrderCreatedEvent.Builder newBuilder(net.lightapi.portal.user.OrderCreatedEvent other) {
    if (other == null) {
      return new net.lightapi.portal.user.OrderCreatedEvent.Builder();
    } else {
      return new net.lightapi.portal.user.OrderCreatedEvent.Builder(other);
    }
  }

  /**
   * RecordBuilder for OrderCreatedEvent instances.
   */
  @org.apache.avro.specific.AvroGenerated
  public static class Builder extends org.apache.avro.specific.SpecificRecordBuilderBase<OrderCreatedEvent>
    implements org.apache.avro.data.RecordBuilder<OrderCreatedEvent> {

    private com.networknt.kafka.common.EventId EventId;
    private com.networknt.kafka.common.EventId.Builder EventIdBuilder;
    /** email of the merchant */
    private java.lang.String email;
    /** An order object */
    private java.lang.String order;
    /** time the event is recorded */
    private long timestamp;

    /** Creates a new Builder */
    private Builder() {
      super(SCHEMA$, MODEL$);
    }

    /**
     * Creates a Builder by copying an existing Builder.
     * @param other The existing Builder to copy.
     */
    private Builder(net.lightapi.portal.user.OrderCreatedEvent.Builder other) {
      super(other);
      if (isValidValue(fields()[0], other.EventId)) {
        this.EventId = data().deepCopy(fields()[0].schema(), other.EventId);
        fieldSetFlags()[0] = other.fieldSetFlags()[0];
      }
      if (other.hasEventIdBuilder()) {
        this.EventIdBuilder = com.networknt.kafka.common.EventId.newBuilder(other.getEventIdBuilder());
      }
      if (isValidValue(fields()[1], other.email)) {
        this.email = data().deepCopy(fields()[1].schema(), other.email);
        fieldSetFlags()[1] = other.fieldSetFlags()[1];
      }
      if (isValidValue(fields()[2], other.order)) {
        this.order = data().deepCopy(fields()[2].schema(), other.order);
        fieldSetFlags()[2] = other.fieldSetFlags()[2];
      }
      if (isValidValue(fields()[3], other.timestamp)) {
        this.timestamp = data().deepCopy(fields()[3].schema(), other.timestamp);
        fieldSetFlags()[3] = other.fieldSetFlags()[3];
      }
    }

    /**
     * Creates a Builder by copying an existing OrderCreatedEvent instance
     * @param other The existing instance to copy.
     */
    private Builder(net.lightapi.portal.user.OrderCreatedEvent other) {
      super(SCHEMA$, MODEL$);
      if (isValidValue(fields()[0], other.EventId)) {
        this.EventId = data().deepCopy(fields()[0].schema(), other.EventId);
        fieldSetFlags()[0] = true;
      }
      this.EventIdBuilder = null;
      if (isValidValue(fields()[1], other.email)) {
        this.email = data().deepCopy(fields()[1].schema(), other.email);
        fieldSetFlags()[1] = true;
      }
      if (isValidValue(fields()[2], other.order)) {
        this.order = data().deepCopy(fields()[2].schema(), other.order);
        fieldSetFlags()[2] = true;
      }
      if (isValidValue(fields()[3], other.timestamp)) {
        this.timestamp = data().deepCopy(fields()[3].schema(), other.timestamp);
        fieldSetFlags()[3] = true;
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
    public net.lightapi.portal.user.OrderCreatedEvent.Builder setEventId(com.networknt.kafka.common.EventId value) {
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

    public net.lightapi.portal.user.OrderCreatedEvent.Builder setEventIdBuilder(com.networknt.kafka.common.EventId.Builder value) {
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
    public net.lightapi.portal.user.OrderCreatedEvent.Builder clearEventId() {
      EventId = null;
      EventIdBuilder = null;
      fieldSetFlags()[0] = false;
      return this;
    }

    /**
      * Gets the value of the 'email' field.
      * email of the merchant
      * @return The value.
      */
    public java.lang.String getEmail() {
      return email;
    }


    /**
      * Sets the value of the 'email' field.
      * email of the merchant
      * @param value The value of 'email'.
      * @return This builder.
      */
    public net.lightapi.portal.user.OrderCreatedEvent.Builder setEmail(java.lang.String value) {
      validate(fields()[1], value);
      this.email = value;
      fieldSetFlags()[1] = true;
      return this;
    }

    /**
      * Checks whether the 'email' field has been set.
      * email of the merchant
      * @return True if the 'email' field has been set, false otherwise.
      */
    public boolean hasEmail() {
      return fieldSetFlags()[1];
    }


    /**
      * Clears the value of the 'email' field.
      * email of the merchant
      * @return This builder.
      */
    public net.lightapi.portal.user.OrderCreatedEvent.Builder clearEmail() {
      email = null;
      fieldSetFlags()[1] = false;
      return this;
    }

    /**
      * Gets the value of the 'order' field.
      * An order object
      * @return The value.
      */
    public java.lang.String getOrder() {
      return order;
    }


    /**
      * Sets the value of the 'order' field.
      * An order object
      * @param value The value of 'order'.
      * @return This builder.
      */
    public net.lightapi.portal.user.OrderCreatedEvent.Builder setOrder(java.lang.String value) {
      validate(fields()[2], value);
      this.order = value;
      fieldSetFlags()[2] = true;
      return this;
    }

    /**
      * Checks whether the 'order' field has been set.
      * An order object
      * @return True if the 'order' field has been set, false otherwise.
      */
    public boolean hasOrder() {
      return fieldSetFlags()[2];
    }


    /**
      * Clears the value of the 'order' field.
      * An order object
      * @return This builder.
      */
    public net.lightapi.portal.user.OrderCreatedEvent.Builder clearOrder() {
      order = null;
      fieldSetFlags()[2] = false;
      return this;
    }

    /**
      * Gets the value of the 'timestamp' field.
      * time the event is recorded
      * @return The value.
      */
    public long getTimestamp() {
      return timestamp;
    }


    /**
      * Sets the value of the 'timestamp' field.
      * time the event is recorded
      * @param value The value of 'timestamp'.
      * @return This builder.
      */
    public net.lightapi.portal.user.OrderCreatedEvent.Builder setTimestamp(long value) {
      validate(fields()[3], value);
      this.timestamp = value;
      fieldSetFlags()[3] = true;
      return this;
    }

    /**
      * Checks whether the 'timestamp' field has been set.
      * time the event is recorded
      * @return True if the 'timestamp' field has been set, false otherwise.
      */
    public boolean hasTimestamp() {
      return fieldSetFlags()[3];
    }


    /**
      * Clears the value of the 'timestamp' field.
      * time the event is recorded
      * @return This builder.
      */
    public net.lightapi.portal.user.OrderCreatedEvent.Builder clearTimestamp() {
      fieldSetFlags()[3] = false;
      return this;
    }

    @Override
    @SuppressWarnings("unchecked")
    public OrderCreatedEvent build() {
      try {
        OrderCreatedEvent record = new OrderCreatedEvent();
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
        record.email = fieldSetFlags()[1] ? this.email : (java.lang.String) defaultValue(fields()[1]);
        record.order = fieldSetFlags()[2] ? this.order : (java.lang.String) defaultValue(fields()[2]);
        record.timestamp = fieldSetFlags()[3] ? this.timestamp : (java.lang.Long) defaultValue(fields()[3]);
        return record;
      } catch (org.apache.avro.AvroMissingFieldException e) {
        throw e;
      } catch (java.lang.Exception e) {
        throw new org.apache.avro.AvroRuntimeException(e);
      }
    }
  }

  @SuppressWarnings("unchecked")
  private static final org.apache.avro.io.DatumWriter<OrderCreatedEvent>
    WRITER$ = (org.apache.avro.io.DatumWriter<OrderCreatedEvent>)MODEL$.createDatumWriter(SCHEMA$);

  @Override public void writeExternal(java.io.ObjectOutput out)
    throws java.io.IOException {
    WRITER$.write(this, SpecificData.getEncoder(out));
  }

  @SuppressWarnings("unchecked")
  private static final org.apache.avro.io.DatumReader<OrderCreatedEvent>
    READER$ = (org.apache.avro.io.DatumReader<OrderCreatedEvent>)MODEL$.createDatumReader(SCHEMA$);

  @Override public void readExternal(java.io.ObjectInput in)
    throws java.io.IOException {
    READER$.read(this, SpecificData.getDecoder(in));
  }

  @Override protected boolean hasCustomCoders() { return true; }

  @Override public void customEncode(org.apache.avro.io.Encoder out)
    throws java.io.IOException
  {
    this.EventId.customEncode(out);

    out.writeString(this.email);

    out.writeString(this.order);

    out.writeLong(this.timestamp);

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

      this.email = in.readString();

      this.order = in.readString();

      this.timestamp = in.readLong();

    } else {
      for (int i = 0; i < 4; i++) {
        switch (fieldOrder[i].pos()) {
        case 0:
          if (this.EventId == null) {
            this.EventId = new com.networknt.kafka.common.EventId();
          }
          this.EventId.customDecode(in);
          break;

        case 1:
          this.email = in.readString();
          break;

        case 2:
          this.order = in.readString();
          break;

        case 3:
          this.timestamp = in.readLong();
          break;

        default:
          throw new java.io.IOException("Corrupt ResolvingDecoder.");
        }
      }
    }
  }
}
