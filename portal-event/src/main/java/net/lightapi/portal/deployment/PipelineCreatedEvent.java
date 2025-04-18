/**
 * Autogenerated by Avro
 *
 * DO NOT EDIT DIRECTLY
 */
package net.lightapi.portal.deployment;

import org.apache.avro.specific.SpecificData;
import org.apache.avro.util.Utf8;
import org.apache.avro.message.BinaryMessageEncoder;
import org.apache.avro.message.BinaryMessageDecoder;
import org.apache.avro.message.SchemaStore;

@org.apache.avro.specific.AvroGenerated
public class PipelineCreatedEvent extends org.apache.avro.specific.SpecificRecordBase implements org.apache.avro.specific.SpecificRecord {
  private static final long serialVersionUID = 2401071169770000581L;


  public static final org.apache.avro.Schema SCHEMA$ = new org.apache.avro.Schema.Parser().parse("{\"type\":\"record\",\"name\":\"PipelineCreatedEvent\",\"namespace\":\"net.lightapi.portal.deployment\",\"fields\":[{\"name\":\"EventId\",\"type\":{\"type\":\"record\",\"name\":\"EventId\",\"namespace\":\"com.networknt.kafka.common\",\"fields\":[{\"name\":\"id\",\"type\":{\"type\":\"string\",\"avro.java.string\":\"String\"},\"doc\":\"a unique identifier for the event\"},{\"name\":\"userId\",\"type\":{\"type\":\"string\",\"avro.java.string\":\"String\"},\"doc\":\"the user who creates the event\"},{\"name\":\"hostId\",\"type\":{\"type\":\"string\",\"avro.java.string\":\"String\"},\"doc\":\"the host which is the event is created\"},{\"name\":\"nonce\",\"type\":\"long\",\"doc\":\"the number of the transactions for the user\"},{\"name\":\"timestamp\",\"type\":\"long\",\"doc\":\"time the event is recorded\",\"default\":0},{\"name\":\"derived\",\"type\":\"boolean\",\"doc\":\"indicate if the event is derived from event processor\",\"default\":false}]}},{\"name\":\"pipelineId\",\"type\":{\"type\":\"string\",\"avro.java.string\":\"String\"},\"doc\":\"pipeline id\"},{\"name\":\"platformId\",\"type\":{\"type\":\"string\",\"avro.java.string\":\"String\"},\"doc\":\"platform id\"},{\"name\":\"endpoint\",\"type\":{\"type\":\"string\",\"avro.java.string\":\"String\"},\"doc\":\"endpoint\"},{\"name\":\"requestSchema\",\"type\":{\"type\":\"string\",\"avro.java.string\":\"String\"},\"doc\":\"request schema\"},{\"name\":\"responseSchema\",\"type\":{\"type\":\"string\",\"avro.java.string\":\"String\"},\"doc\":\"response schema\"}]}");
  public static org.apache.avro.Schema getClassSchema() { return SCHEMA$; }

  private static final SpecificData MODEL$ = new SpecificData();

  private static final BinaryMessageEncoder<PipelineCreatedEvent> ENCODER =
      new BinaryMessageEncoder<>(MODEL$, SCHEMA$);

  private static final BinaryMessageDecoder<PipelineCreatedEvent> DECODER =
      new BinaryMessageDecoder<>(MODEL$, SCHEMA$);

  /**
   * Return the BinaryMessageEncoder instance used by this class.
   * @return the message encoder used by this class
   */
  public static BinaryMessageEncoder<PipelineCreatedEvent> getEncoder() {
    return ENCODER;
  }

  /**
   * Return the BinaryMessageDecoder instance used by this class.
   * @return the message decoder used by this class
   */
  public static BinaryMessageDecoder<PipelineCreatedEvent> getDecoder() {
    return DECODER;
  }

  /**
   * Create a new BinaryMessageDecoder instance for this class that uses the specified {@link SchemaStore}.
   * @param resolver a {@link SchemaStore} used to find schemas by fingerprint
   * @return a BinaryMessageDecoder instance for this class backed by the given SchemaStore
   */
  public static BinaryMessageDecoder<PipelineCreatedEvent> createDecoder(SchemaStore resolver) {
    return new BinaryMessageDecoder<>(MODEL$, SCHEMA$, resolver);
  }

  /**
   * Serializes this PipelineCreatedEvent to a ByteBuffer.
   * @return a buffer holding the serialized data for this instance
   * @throws java.io.IOException if this instance could not be serialized
   */
  public java.nio.ByteBuffer toByteBuffer() throws java.io.IOException {
    return ENCODER.encode(this);
  }

  /**
   * Deserializes a PipelineCreatedEvent from a ByteBuffer.
   * @param b a byte buffer holding serialized data for an instance of this class
   * @return a PipelineCreatedEvent instance decoded from the given buffer
   * @throws java.io.IOException if the given bytes could not be deserialized into an instance of this class
   */
  public static PipelineCreatedEvent fromByteBuffer(
      java.nio.ByteBuffer b) throws java.io.IOException {
    return DECODER.decode(b);
  }

  private com.networknt.kafka.common.EventId EventId;
  /** pipeline id */
  private java.lang.String pipelineId;
  /** platform id */
  private java.lang.String platformId;
  /** endpoint */
  private java.lang.String endpoint;
  /** request schema */
  private java.lang.String requestSchema;
  /** response schema */
  private java.lang.String responseSchema;

  /**
   * Default constructor.  Note that this does not initialize fields
   * to their default values from the schema.  If that is desired then
   * one should use <code>newBuilder()</code>.
   */
  public PipelineCreatedEvent() {}

  /**
   * All-args constructor.
   * @param EventId The new value for EventId
   * @param pipelineId pipeline id
   * @param platformId platform id
   * @param endpoint endpoint
   * @param requestSchema request schema
   * @param responseSchema response schema
   */
  public PipelineCreatedEvent(com.networknt.kafka.common.EventId EventId, java.lang.String pipelineId, java.lang.String platformId, java.lang.String endpoint, java.lang.String requestSchema, java.lang.String responseSchema) {
    this.EventId = EventId;
    this.pipelineId = pipelineId;
    this.platformId = platformId;
    this.endpoint = endpoint;
    this.requestSchema = requestSchema;
    this.responseSchema = responseSchema;
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
    case 1: return pipelineId;
    case 2: return platformId;
    case 3: return endpoint;
    case 4: return requestSchema;
    case 5: return responseSchema;
    default: throw new IndexOutOfBoundsException("Invalid index: " + field$);
    }
  }

  // Used by DatumReader.  Applications should not call.
  @Override
  @SuppressWarnings(value="unchecked")
  public void put(int field$, java.lang.Object value$) {
    switch (field$) {
    case 0: EventId = (com.networknt.kafka.common.EventId)value$; break;
    case 1: pipelineId = value$ != null ? value$.toString() : null; break;
    case 2: platformId = value$ != null ? value$.toString() : null; break;
    case 3: endpoint = value$ != null ? value$.toString() : null; break;
    case 4: requestSchema = value$ != null ? value$.toString() : null; break;
    case 5: responseSchema = value$ != null ? value$.toString() : null; break;
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
   * Gets the value of the 'pipelineId' field.
   * @return pipeline id
   */
  public java.lang.String getPipelineId() {
    return pipelineId;
  }


  /**
   * Sets the value of the 'pipelineId' field.
   * pipeline id
   * @param value the value to set.
   */
  public void setPipelineId(java.lang.String value) {
    this.pipelineId = value;
  }

  /**
   * Gets the value of the 'platformId' field.
   * @return platform id
   */
  public java.lang.String getPlatformId() {
    return platformId;
  }


  /**
   * Sets the value of the 'platformId' field.
   * platform id
   * @param value the value to set.
   */
  public void setPlatformId(java.lang.String value) {
    this.platformId = value;
  }

  /**
   * Gets the value of the 'endpoint' field.
   * @return endpoint
   */
  public java.lang.String getEndpoint() {
    return endpoint;
  }


  /**
   * Sets the value of the 'endpoint' field.
   * endpoint
   * @param value the value to set.
   */
  public void setEndpoint(java.lang.String value) {
    this.endpoint = value;
  }

  /**
   * Gets the value of the 'requestSchema' field.
   * @return request schema
   */
  public java.lang.String getRequestSchema() {
    return requestSchema;
  }


  /**
   * Sets the value of the 'requestSchema' field.
   * request schema
   * @param value the value to set.
   */
  public void setRequestSchema(java.lang.String value) {
    this.requestSchema = value;
  }

  /**
   * Gets the value of the 'responseSchema' field.
   * @return response schema
   */
  public java.lang.String getResponseSchema() {
    return responseSchema;
  }


  /**
   * Sets the value of the 'responseSchema' field.
   * response schema
   * @param value the value to set.
   */
  public void setResponseSchema(java.lang.String value) {
    this.responseSchema = value;
  }

  /**
   * Creates a new PipelineCreatedEvent RecordBuilder.
   * @return A new PipelineCreatedEvent RecordBuilder
   */
  public static net.lightapi.portal.deployment.PipelineCreatedEvent.Builder newBuilder() {
    return new net.lightapi.portal.deployment.PipelineCreatedEvent.Builder();
  }

  /**
   * Creates a new PipelineCreatedEvent RecordBuilder by copying an existing Builder.
   * @param other The existing builder to copy.
   * @return A new PipelineCreatedEvent RecordBuilder
   */
  public static net.lightapi.portal.deployment.PipelineCreatedEvent.Builder newBuilder(net.lightapi.portal.deployment.PipelineCreatedEvent.Builder other) {
    if (other == null) {
      return new net.lightapi.portal.deployment.PipelineCreatedEvent.Builder();
    } else {
      return new net.lightapi.portal.deployment.PipelineCreatedEvent.Builder(other);
    }
  }

  /**
   * Creates a new PipelineCreatedEvent RecordBuilder by copying an existing PipelineCreatedEvent instance.
   * @param other The existing instance to copy.
   * @return A new PipelineCreatedEvent RecordBuilder
   */
  public static net.lightapi.portal.deployment.PipelineCreatedEvent.Builder newBuilder(net.lightapi.portal.deployment.PipelineCreatedEvent other) {
    if (other == null) {
      return new net.lightapi.portal.deployment.PipelineCreatedEvent.Builder();
    } else {
      return new net.lightapi.portal.deployment.PipelineCreatedEvent.Builder(other);
    }
  }

  /**
   * RecordBuilder for PipelineCreatedEvent instances.
   */
  @org.apache.avro.specific.AvroGenerated
  public static class Builder extends org.apache.avro.specific.SpecificRecordBuilderBase<PipelineCreatedEvent>
    implements org.apache.avro.data.RecordBuilder<PipelineCreatedEvent> {

    private com.networknt.kafka.common.EventId EventId;
    private com.networknt.kafka.common.EventId.Builder EventIdBuilder;
    /** pipeline id */
    private java.lang.String pipelineId;
    /** platform id */
    private java.lang.String platformId;
    /** endpoint */
    private java.lang.String endpoint;
    /** request schema */
    private java.lang.String requestSchema;
    /** response schema */
    private java.lang.String responseSchema;

    /** Creates a new Builder */
    private Builder() {
      super(SCHEMA$, MODEL$);
    }

    /**
     * Creates a Builder by copying an existing Builder.
     * @param other The existing Builder to copy.
     */
    private Builder(net.lightapi.portal.deployment.PipelineCreatedEvent.Builder other) {
      super(other);
      if (isValidValue(fields()[0], other.EventId)) {
        this.EventId = data().deepCopy(fields()[0].schema(), other.EventId);
        fieldSetFlags()[0] = other.fieldSetFlags()[0];
      }
      if (other.hasEventIdBuilder()) {
        this.EventIdBuilder = com.networknt.kafka.common.EventId.newBuilder(other.getEventIdBuilder());
      }
      if (isValidValue(fields()[1], other.pipelineId)) {
        this.pipelineId = data().deepCopy(fields()[1].schema(), other.pipelineId);
        fieldSetFlags()[1] = other.fieldSetFlags()[1];
      }
      if (isValidValue(fields()[2], other.platformId)) {
        this.platformId = data().deepCopy(fields()[2].schema(), other.platformId);
        fieldSetFlags()[2] = other.fieldSetFlags()[2];
      }
      if (isValidValue(fields()[3], other.endpoint)) {
        this.endpoint = data().deepCopy(fields()[3].schema(), other.endpoint);
        fieldSetFlags()[3] = other.fieldSetFlags()[3];
      }
      if (isValidValue(fields()[4], other.requestSchema)) {
        this.requestSchema = data().deepCopy(fields()[4].schema(), other.requestSchema);
        fieldSetFlags()[4] = other.fieldSetFlags()[4];
      }
      if (isValidValue(fields()[5], other.responseSchema)) {
        this.responseSchema = data().deepCopy(fields()[5].schema(), other.responseSchema);
        fieldSetFlags()[5] = other.fieldSetFlags()[5];
      }
    }

    /**
     * Creates a Builder by copying an existing PipelineCreatedEvent instance
     * @param other The existing instance to copy.
     */
    private Builder(net.lightapi.portal.deployment.PipelineCreatedEvent other) {
      super(SCHEMA$, MODEL$);
      if (isValidValue(fields()[0], other.EventId)) {
        this.EventId = data().deepCopy(fields()[0].schema(), other.EventId);
        fieldSetFlags()[0] = true;
      }
      this.EventIdBuilder = null;
      if (isValidValue(fields()[1], other.pipelineId)) {
        this.pipelineId = data().deepCopy(fields()[1].schema(), other.pipelineId);
        fieldSetFlags()[1] = true;
      }
      if (isValidValue(fields()[2], other.platformId)) {
        this.platformId = data().deepCopy(fields()[2].schema(), other.platformId);
        fieldSetFlags()[2] = true;
      }
      if (isValidValue(fields()[3], other.endpoint)) {
        this.endpoint = data().deepCopy(fields()[3].schema(), other.endpoint);
        fieldSetFlags()[3] = true;
      }
      if (isValidValue(fields()[4], other.requestSchema)) {
        this.requestSchema = data().deepCopy(fields()[4].schema(), other.requestSchema);
        fieldSetFlags()[4] = true;
      }
      if (isValidValue(fields()[5], other.responseSchema)) {
        this.responseSchema = data().deepCopy(fields()[5].schema(), other.responseSchema);
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
    public net.lightapi.portal.deployment.PipelineCreatedEvent.Builder setEventId(com.networknt.kafka.common.EventId value) {
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

    public net.lightapi.portal.deployment.PipelineCreatedEvent.Builder setEventIdBuilder(com.networknt.kafka.common.EventId.Builder value) {
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
    public net.lightapi.portal.deployment.PipelineCreatedEvent.Builder clearEventId() {
      EventId = null;
      EventIdBuilder = null;
      fieldSetFlags()[0] = false;
      return this;
    }

    /**
      * Gets the value of the 'pipelineId' field.
      * pipeline id
      * @return The value.
      */
    public java.lang.String getPipelineId() {
      return pipelineId;
    }


    /**
      * Sets the value of the 'pipelineId' field.
      * pipeline id
      * @param value The value of 'pipelineId'.
      * @return This builder.
      */
    public net.lightapi.portal.deployment.PipelineCreatedEvent.Builder setPipelineId(java.lang.String value) {
      validate(fields()[1], value);
      this.pipelineId = value;
      fieldSetFlags()[1] = true;
      return this;
    }

    /**
      * Checks whether the 'pipelineId' field has been set.
      * pipeline id
      * @return True if the 'pipelineId' field has been set, false otherwise.
      */
    public boolean hasPipelineId() {
      return fieldSetFlags()[1];
    }


    /**
      * Clears the value of the 'pipelineId' field.
      * pipeline id
      * @return This builder.
      */
    public net.lightapi.portal.deployment.PipelineCreatedEvent.Builder clearPipelineId() {
      pipelineId = null;
      fieldSetFlags()[1] = false;
      return this;
    }

    /**
      * Gets the value of the 'platformId' field.
      * platform id
      * @return The value.
      */
    public java.lang.String getPlatformId() {
      return platformId;
    }


    /**
      * Sets the value of the 'platformId' field.
      * platform id
      * @param value The value of 'platformId'.
      * @return This builder.
      */
    public net.lightapi.portal.deployment.PipelineCreatedEvent.Builder setPlatformId(java.lang.String value) {
      validate(fields()[2], value);
      this.platformId = value;
      fieldSetFlags()[2] = true;
      return this;
    }

    /**
      * Checks whether the 'platformId' field has been set.
      * platform id
      * @return True if the 'platformId' field has been set, false otherwise.
      */
    public boolean hasPlatformId() {
      return fieldSetFlags()[2];
    }


    /**
      * Clears the value of the 'platformId' field.
      * platform id
      * @return This builder.
      */
    public net.lightapi.portal.deployment.PipelineCreatedEvent.Builder clearPlatformId() {
      platformId = null;
      fieldSetFlags()[2] = false;
      return this;
    }

    /**
      * Gets the value of the 'endpoint' field.
      * endpoint
      * @return The value.
      */
    public java.lang.String getEndpoint() {
      return endpoint;
    }


    /**
      * Sets the value of the 'endpoint' field.
      * endpoint
      * @param value The value of 'endpoint'.
      * @return This builder.
      */
    public net.lightapi.portal.deployment.PipelineCreatedEvent.Builder setEndpoint(java.lang.String value) {
      validate(fields()[3], value);
      this.endpoint = value;
      fieldSetFlags()[3] = true;
      return this;
    }

    /**
      * Checks whether the 'endpoint' field has been set.
      * endpoint
      * @return True if the 'endpoint' field has been set, false otherwise.
      */
    public boolean hasEndpoint() {
      return fieldSetFlags()[3];
    }


    /**
      * Clears the value of the 'endpoint' field.
      * endpoint
      * @return This builder.
      */
    public net.lightapi.portal.deployment.PipelineCreatedEvent.Builder clearEndpoint() {
      endpoint = null;
      fieldSetFlags()[3] = false;
      return this;
    }

    /**
      * Gets the value of the 'requestSchema' field.
      * request schema
      * @return The value.
      */
    public java.lang.String getRequestSchema() {
      return requestSchema;
    }


    /**
      * Sets the value of the 'requestSchema' field.
      * request schema
      * @param value The value of 'requestSchema'.
      * @return This builder.
      */
    public net.lightapi.portal.deployment.PipelineCreatedEvent.Builder setRequestSchema(java.lang.String value) {
      validate(fields()[4], value);
      this.requestSchema = value;
      fieldSetFlags()[4] = true;
      return this;
    }

    /**
      * Checks whether the 'requestSchema' field has been set.
      * request schema
      * @return True if the 'requestSchema' field has been set, false otherwise.
      */
    public boolean hasRequestSchema() {
      return fieldSetFlags()[4];
    }


    /**
      * Clears the value of the 'requestSchema' field.
      * request schema
      * @return This builder.
      */
    public net.lightapi.portal.deployment.PipelineCreatedEvent.Builder clearRequestSchema() {
      requestSchema = null;
      fieldSetFlags()[4] = false;
      return this;
    }

    /**
      * Gets the value of the 'responseSchema' field.
      * response schema
      * @return The value.
      */
    public java.lang.String getResponseSchema() {
      return responseSchema;
    }


    /**
      * Sets the value of the 'responseSchema' field.
      * response schema
      * @param value The value of 'responseSchema'.
      * @return This builder.
      */
    public net.lightapi.portal.deployment.PipelineCreatedEvent.Builder setResponseSchema(java.lang.String value) {
      validate(fields()[5], value);
      this.responseSchema = value;
      fieldSetFlags()[5] = true;
      return this;
    }

    /**
      * Checks whether the 'responseSchema' field has been set.
      * response schema
      * @return True if the 'responseSchema' field has been set, false otherwise.
      */
    public boolean hasResponseSchema() {
      return fieldSetFlags()[5];
    }


    /**
      * Clears the value of the 'responseSchema' field.
      * response schema
      * @return This builder.
      */
    public net.lightapi.portal.deployment.PipelineCreatedEvent.Builder clearResponseSchema() {
      responseSchema = null;
      fieldSetFlags()[5] = false;
      return this;
    }

    @Override
    @SuppressWarnings("unchecked")
    public PipelineCreatedEvent build() {
      try {
        PipelineCreatedEvent record = new PipelineCreatedEvent();
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
        record.pipelineId = fieldSetFlags()[1] ? this.pipelineId : (java.lang.String) defaultValue(fields()[1]);
        record.platformId = fieldSetFlags()[2] ? this.platformId : (java.lang.String) defaultValue(fields()[2]);
        record.endpoint = fieldSetFlags()[3] ? this.endpoint : (java.lang.String) defaultValue(fields()[3]);
        record.requestSchema = fieldSetFlags()[4] ? this.requestSchema : (java.lang.String) defaultValue(fields()[4]);
        record.responseSchema = fieldSetFlags()[5] ? this.responseSchema : (java.lang.String) defaultValue(fields()[5]);
        return record;
      } catch (org.apache.avro.AvroMissingFieldException e) {
        throw e;
      } catch (java.lang.Exception e) {
        throw new org.apache.avro.AvroRuntimeException(e);
      }
    }
  }

  @SuppressWarnings("unchecked")
  private static final org.apache.avro.io.DatumWriter<PipelineCreatedEvent>
    WRITER$ = (org.apache.avro.io.DatumWriter<PipelineCreatedEvent>)MODEL$.createDatumWriter(SCHEMA$);

  @Override public void writeExternal(java.io.ObjectOutput out)
    throws java.io.IOException {
    WRITER$.write(this, SpecificData.getEncoder(out));
  }

  @SuppressWarnings("unchecked")
  private static final org.apache.avro.io.DatumReader<PipelineCreatedEvent>
    READER$ = (org.apache.avro.io.DatumReader<PipelineCreatedEvent>)MODEL$.createDatumReader(SCHEMA$);

  @Override public void readExternal(java.io.ObjectInput in)
    throws java.io.IOException {
    READER$.read(this, SpecificData.getDecoder(in));
  }

  @Override protected boolean hasCustomCoders() { return true; }

  @Override public void customEncode(org.apache.avro.io.Encoder out)
    throws java.io.IOException
  {
    this.EventId.customEncode(out);

    out.writeString(this.pipelineId);

    out.writeString(this.platformId);

    out.writeString(this.endpoint);

    out.writeString(this.requestSchema);

    out.writeString(this.responseSchema);

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

      this.pipelineId = in.readString();

      this.platformId = in.readString();

      this.endpoint = in.readString();

      this.requestSchema = in.readString();

      this.responseSchema = in.readString();

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
          this.pipelineId = in.readString();
          break;

        case 2:
          this.platformId = in.readString();
          break;

        case 3:
          this.endpoint = in.readString();
          break;

        case 4:
          this.requestSchema = in.readString();
          break;

        case 5:
          this.responseSchema = in.readString();
          break;

        default:
          throw new java.io.IOException("Corrupt ResolvingDecoder.");
        }
      }
    }
  }
}
