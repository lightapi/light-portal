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
public class PlatformUpdatedEvent extends org.apache.avro.specific.SpecificRecordBase implements org.apache.avro.specific.SpecificRecord {
  private static final long serialVersionUID = -2319989080641230817L;


  public static final org.apache.avro.Schema SCHEMA$ = new org.apache.avro.Schema.Parser().parse("{\"type\":\"record\",\"name\":\"PlatformUpdatedEvent\",\"namespace\":\"net.lightapi.portal.deployment\",\"fields\":[{\"name\":\"EventId\",\"type\":{\"type\":\"record\",\"name\":\"EventId\",\"namespace\":\"com.networknt.kafka.common\",\"fields\":[{\"name\":\"id\",\"type\":{\"type\":\"string\",\"avro.java.string\":\"String\"},\"doc\":\"a unique identifier for the event\"},{\"name\":\"userId\",\"type\":{\"type\":\"string\",\"avro.java.string\":\"String\"},\"doc\":\"the user who creates the event\"},{\"name\":\"hostId\",\"type\":{\"type\":\"string\",\"avro.java.string\":\"String\"},\"doc\":\"the host which is the event is created\"},{\"name\":\"nonce\",\"type\":\"long\",\"doc\":\"the number of the transactions for the user\"},{\"name\":\"timestamp\",\"type\":\"long\",\"doc\":\"time the event is recorded\",\"default\":0},{\"name\":\"derived\",\"type\":\"boolean\",\"doc\":\"indicate if the event is derived from event processor\",\"default\":false}]}},{\"name\":\"platformId\",\"type\":{\"type\":\"string\",\"avro.java.string\":\"String\"},\"doc\":\"platform id\"},{\"name\":\"platformName\",\"type\":{\"type\":\"string\",\"avro.java.string\":\"String\"},\"doc\":\"platform name\"},{\"name\":\"platformVersion\",\"type\":{\"type\":\"string\",\"avro.java.string\":\"String\"},\"doc\":\"platform version\"},{\"name\":\"clientType\",\"type\":{\"type\":\"string\",\"avro.java.string\":\"String\"},\"doc\":\"client type\"},{\"name\":\"clientUrl\",\"type\":{\"type\":\"string\",\"avro.java.string\":\"String\"},\"doc\":\"client url\"},{\"name\":\"credentials\",\"type\":{\"type\":\"string\",\"avro.java.string\":\"String\"},\"doc\":\"credentials\"},{\"name\":\"value\",\"type\":{\"type\":\"string\",\"avro.java.string\":\"String\"},\"doc\":\"value in json\"}]}");
  public static org.apache.avro.Schema getClassSchema() { return SCHEMA$; }

  private static final SpecificData MODEL$ = new SpecificData();

  private static final BinaryMessageEncoder<PlatformUpdatedEvent> ENCODER =
      new BinaryMessageEncoder<>(MODEL$, SCHEMA$);

  private static final BinaryMessageDecoder<PlatformUpdatedEvent> DECODER =
      new BinaryMessageDecoder<>(MODEL$, SCHEMA$);

  /**
   * Return the BinaryMessageEncoder instance used by this class.
   * @return the message encoder used by this class
   */
  public static BinaryMessageEncoder<PlatformUpdatedEvent> getEncoder() {
    return ENCODER;
  }

  /**
   * Return the BinaryMessageDecoder instance used by this class.
   * @return the message decoder used by this class
   */
  public static BinaryMessageDecoder<PlatformUpdatedEvent> getDecoder() {
    return DECODER;
  }

  /**
   * Create a new BinaryMessageDecoder instance for this class that uses the specified {@link SchemaStore}.
   * @param resolver a {@link SchemaStore} used to find schemas by fingerprint
   * @return a BinaryMessageDecoder instance for this class backed by the given SchemaStore
   */
  public static BinaryMessageDecoder<PlatformUpdatedEvent> createDecoder(SchemaStore resolver) {
    return new BinaryMessageDecoder<>(MODEL$, SCHEMA$, resolver);
  }

  /**
   * Serializes this PlatformUpdatedEvent to a ByteBuffer.
   * @return a buffer holding the serialized data for this instance
   * @throws java.io.IOException if this instance could not be serialized
   */
  public java.nio.ByteBuffer toByteBuffer() throws java.io.IOException {
    return ENCODER.encode(this);
  }

  /**
   * Deserializes a PlatformUpdatedEvent from a ByteBuffer.
   * @param b a byte buffer holding serialized data for an instance of this class
   * @return a PlatformUpdatedEvent instance decoded from the given buffer
   * @throws java.io.IOException if the given bytes could not be deserialized into an instance of this class
   */
  public static PlatformUpdatedEvent fromByteBuffer(
      java.nio.ByteBuffer b) throws java.io.IOException {
    return DECODER.decode(b);
  }

  private com.networknt.kafka.common.EventId EventId;
  /** platform id */
  private java.lang.String platformId;
  /** platform name */
  private java.lang.String platformName;
  /** platform version */
  private java.lang.String platformVersion;
  /** client type */
  private java.lang.String clientType;
  /** client url */
  private java.lang.String clientUrl;
  /** credentials */
  private java.lang.String credentials;
  /** value in json */
  private java.lang.String value;

  /**
   * Default constructor.  Note that this does not initialize fields
   * to their default values from the schema.  If that is desired then
   * one should use <code>newBuilder()</code>.
   */
  public PlatformUpdatedEvent() {}

  /**
   * All-args constructor.
   * @param EventId The new value for EventId
   * @param platformId platform id
   * @param platformName platform name
   * @param platformVersion platform version
   * @param clientType client type
   * @param clientUrl client url
   * @param credentials credentials
   * @param value value in json
   */
  public PlatformUpdatedEvent(com.networknt.kafka.common.EventId EventId, java.lang.String platformId, java.lang.String platformName, java.lang.String platformVersion, java.lang.String clientType, java.lang.String clientUrl, java.lang.String credentials, java.lang.String value) {
    this.EventId = EventId;
    this.platformId = platformId;
    this.platformName = platformName;
    this.platformVersion = platformVersion;
    this.clientType = clientType;
    this.clientUrl = clientUrl;
    this.credentials = credentials;
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
    case 1: return platformId;
    case 2: return platformName;
    case 3: return platformVersion;
    case 4: return clientType;
    case 5: return clientUrl;
    case 6: return credentials;
    case 7: return value;
    default: throw new IndexOutOfBoundsException("Invalid index: " + field$);
    }
  }

  // Used by DatumReader.  Applications should not call.
  @Override
  @SuppressWarnings(value="unchecked")
  public void put(int field$, java.lang.Object value$) {
    switch (field$) {
    case 0: EventId = (com.networknt.kafka.common.EventId)value$; break;
    case 1: platformId = value$ != null ? value$.toString() : null; break;
    case 2: platformName = value$ != null ? value$.toString() : null; break;
    case 3: platformVersion = value$ != null ? value$.toString() : null; break;
    case 4: clientType = value$ != null ? value$.toString() : null; break;
    case 5: clientUrl = value$ != null ? value$.toString() : null; break;
    case 6: credentials = value$ != null ? value$.toString() : null; break;
    case 7: value = value$ != null ? value$.toString() : null; break;
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
   * Gets the value of the 'platformName' field.
   * @return platform name
   */
  public java.lang.String getPlatformName() {
    return platformName;
  }


  /**
   * Sets the value of the 'platformName' field.
   * platform name
   * @param value the value to set.
   */
  public void setPlatformName(java.lang.String value) {
    this.platformName = value;
  }

  /**
   * Gets the value of the 'platformVersion' field.
   * @return platform version
   */
  public java.lang.String getPlatformVersion() {
    return platformVersion;
  }


  /**
   * Sets the value of the 'platformVersion' field.
   * platform version
   * @param value the value to set.
   */
  public void setPlatformVersion(java.lang.String value) {
    this.platformVersion = value;
  }

  /**
   * Gets the value of the 'clientType' field.
   * @return client type
   */
  public java.lang.String getClientType() {
    return clientType;
  }


  /**
   * Sets the value of the 'clientType' field.
   * client type
   * @param value the value to set.
   */
  public void setClientType(java.lang.String value) {
    this.clientType = value;
  }

  /**
   * Gets the value of the 'clientUrl' field.
   * @return client url
   */
  public java.lang.String getClientUrl() {
    return clientUrl;
  }


  /**
   * Sets the value of the 'clientUrl' field.
   * client url
   * @param value the value to set.
   */
  public void setClientUrl(java.lang.String value) {
    this.clientUrl = value;
  }

  /**
   * Gets the value of the 'credentials' field.
   * @return credentials
   */
  public java.lang.String getCredentials() {
    return credentials;
  }


  /**
   * Sets the value of the 'credentials' field.
   * credentials
   * @param value the value to set.
   */
  public void setCredentials(java.lang.String value) {
    this.credentials = value;
  }

  /**
   * Gets the value of the 'value' field.
   * @return value in json
   */
  public java.lang.String getValue() {
    return value;
  }


  /**
   * Sets the value of the 'value' field.
   * value in json
   * @param value the value to set.
   */
  public void setValue(java.lang.String value) {
    this.value = value;
  }

  /**
   * Creates a new PlatformUpdatedEvent RecordBuilder.
   * @return A new PlatformUpdatedEvent RecordBuilder
   */
  public static net.lightapi.portal.deployment.PlatformUpdatedEvent.Builder newBuilder() {
    return new net.lightapi.portal.deployment.PlatformUpdatedEvent.Builder();
  }

  /**
   * Creates a new PlatformUpdatedEvent RecordBuilder by copying an existing Builder.
   * @param other The existing builder to copy.
   * @return A new PlatformUpdatedEvent RecordBuilder
   */
  public static net.lightapi.portal.deployment.PlatformUpdatedEvent.Builder newBuilder(net.lightapi.portal.deployment.PlatformUpdatedEvent.Builder other) {
    if (other == null) {
      return new net.lightapi.portal.deployment.PlatformUpdatedEvent.Builder();
    } else {
      return new net.lightapi.portal.deployment.PlatformUpdatedEvent.Builder(other);
    }
  }

  /**
   * Creates a new PlatformUpdatedEvent RecordBuilder by copying an existing PlatformUpdatedEvent instance.
   * @param other The existing instance to copy.
   * @return A new PlatformUpdatedEvent RecordBuilder
   */
  public static net.lightapi.portal.deployment.PlatformUpdatedEvent.Builder newBuilder(net.lightapi.portal.deployment.PlatformUpdatedEvent other) {
    if (other == null) {
      return new net.lightapi.portal.deployment.PlatformUpdatedEvent.Builder();
    } else {
      return new net.lightapi.portal.deployment.PlatformUpdatedEvent.Builder(other);
    }
  }

  /**
   * RecordBuilder for PlatformUpdatedEvent instances.
   */
  @org.apache.avro.specific.AvroGenerated
  public static class Builder extends org.apache.avro.specific.SpecificRecordBuilderBase<PlatformUpdatedEvent>
    implements org.apache.avro.data.RecordBuilder<PlatformUpdatedEvent> {

    private com.networknt.kafka.common.EventId EventId;
    private com.networknt.kafka.common.EventId.Builder EventIdBuilder;
    /** platform id */
    private java.lang.String platformId;
    /** platform name */
    private java.lang.String platformName;
    /** platform version */
    private java.lang.String platformVersion;
    /** client type */
    private java.lang.String clientType;
    /** client url */
    private java.lang.String clientUrl;
    /** credentials */
    private java.lang.String credentials;
    /** value in json */
    private java.lang.String value;

    /** Creates a new Builder */
    private Builder() {
      super(SCHEMA$, MODEL$);
    }

    /**
     * Creates a Builder by copying an existing Builder.
     * @param other The existing Builder to copy.
     */
    private Builder(net.lightapi.portal.deployment.PlatformUpdatedEvent.Builder other) {
      super(other);
      if (isValidValue(fields()[0], other.EventId)) {
        this.EventId = data().deepCopy(fields()[0].schema(), other.EventId);
        fieldSetFlags()[0] = other.fieldSetFlags()[0];
      }
      if (other.hasEventIdBuilder()) {
        this.EventIdBuilder = com.networknt.kafka.common.EventId.newBuilder(other.getEventIdBuilder());
      }
      if (isValidValue(fields()[1], other.platformId)) {
        this.platformId = data().deepCopy(fields()[1].schema(), other.platformId);
        fieldSetFlags()[1] = other.fieldSetFlags()[1];
      }
      if (isValidValue(fields()[2], other.platformName)) {
        this.platformName = data().deepCopy(fields()[2].schema(), other.platformName);
        fieldSetFlags()[2] = other.fieldSetFlags()[2];
      }
      if (isValidValue(fields()[3], other.platformVersion)) {
        this.platformVersion = data().deepCopy(fields()[3].schema(), other.platformVersion);
        fieldSetFlags()[3] = other.fieldSetFlags()[3];
      }
      if (isValidValue(fields()[4], other.clientType)) {
        this.clientType = data().deepCopy(fields()[4].schema(), other.clientType);
        fieldSetFlags()[4] = other.fieldSetFlags()[4];
      }
      if (isValidValue(fields()[5], other.clientUrl)) {
        this.clientUrl = data().deepCopy(fields()[5].schema(), other.clientUrl);
        fieldSetFlags()[5] = other.fieldSetFlags()[5];
      }
      if (isValidValue(fields()[6], other.credentials)) {
        this.credentials = data().deepCopy(fields()[6].schema(), other.credentials);
        fieldSetFlags()[6] = other.fieldSetFlags()[6];
      }
      if (isValidValue(fields()[7], other.value)) {
        this.value = data().deepCopy(fields()[7].schema(), other.value);
        fieldSetFlags()[7] = other.fieldSetFlags()[7];
      }
    }

    /**
     * Creates a Builder by copying an existing PlatformUpdatedEvent instance
     * @param other The existing instance to copy.
     */
    private Builder(net.lightapi.portal.deployment.PlatformUpdatedEvent other) {
      super(SCHEMA$, MODEL$);
      if (isValidValue(fields()[0], other.EventId)) {
        this.EventId = data().deepCopy(fields()[0].schema(), other.EventId);
        fieldSetFlags()[0] = true;
      }
      this.EventIdBuilder = null;
      if (isValidValue(fields()[1], other.platformId)) {
        this.platformId = data().deepCopy(fields()[1].schema(), other.platformId);
        fieldSetFlags()[1] = true;
      }
      if (isValidValue(fields()[2], other.platformName)) {
        this.platformName = data().deepCopy(fields()[2].schema(), other.platformName);
        fieldSetFlags()[2] = true;
      }
      if (isValidValue(fields()[3], other.platformVersion)) {
        this.platformVersion = data().deepCopy(fields()[3].schema(), other.platformVersion);
        fieldSetFlags()[3] = true;
      }
      if (isValidValue(fields()[4], other.clientType)) {
        this.clientType = data().deepCopy(fields()[4].schema(), other.clientType);
        fieldSetFlags()[4] = true;
      }
      if (isValidValue(fields()[5], other.clientUrl)) {
        this.clientUrl = data().deepCopy(fields()[5].schema(), other.clientUrl);
        fieldSetFlags()[5] = true;
      }
      if (isValidValue(fields()[6], other.credentials)) {
        this.credentials = data().deepCopy(fields()[6].schema(), other.credentials);
        fieldSetFlags()[6] = true;
      }
      if (isValidValue(fields()[7], other.value)) {
        this.value = data().deepCopy(fields()[7].schema(), other.value);
        fieldSetFlags()[7] = true;
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
    public net.lightapi.portal.deployment.PlatformUpdatedEvent.Builder setEventId(com.networknt.kafka.common.EventId value) {
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

    public net.lightapi.portal.deployment.PlatformUpdatedEvent.Builder setEventIdBuilder(com.networknt.kafka.common.EventId.Builder value) {
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
    public net.lightapi.portal.deployment.PlatformUpdatedEvent.Builder clearEventId() {
      EventId = null;
      EventIdBuilder = null;
      fieldSetFlags()[0] = false;
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
    public net.lightapi.portal.deployment.PlatformUpdatedEvent.Builder setPlatformId(java.lang.String value) {
      validate(fields()[1], value);
      this.platformId = value;
      fieldSetFlags()[1] = true;
      return this;
    }

    /**
      * Checks whether the 'platformId' field has been set.
      * platform id
      * @return True if the 'platformId' field has been set, false otherwise.
      */
    public boolean hasPlatformId() {
      return fieldSetFlags()[1];
    }


    /**
      * Clears the value of the 'platformId' field.
      * platform id
      * @return This builder.
      */
    public net.lightapi.portal.deployment.PlatformUpdatedEvent.Builder clearPlatformId() {
      platformId = null;
      fieldSetFlags()[1] = false;
      return this;
    }

    /**
      * Gets the value of the 'platformName' field.
      * platform name
      * @return The value.
      */
    public java.lang.String getPlatformName() {
      return platformName;
    }


    /**
      * Sets the value of the 'platformName' field.
      * platform name
      * @param value The value of 'platformName'.
      * @return This builder.
      */
    public net.lightapi.portal.deployment.PlatformUpdatedEvent.Builder setPlatformName(java.lang.String value) {
      validate(fields()[2], value);
      this.platformName = value;
      fieldSetFlags()[2] = true;
      return this;
    }

    /**
      * Checks whether the 'platformName' field has been set.
      * platform name
      * @return True if the 'platformName' field has been set, false otherwise.
      */
    public boolean hasPlatformName() {
      return fieldSetFlags()[2];
    }


    /**
      * Clears the value of the 'platformName' field.
      * platform name
      * @return This builder.
      */
    public net.lightapi.portal.deployment.PlatformUpdatedEvent.Builder clearPlatformName() {
      platformName = null;
      fieldSetFlags()[2] = false;
      return this;
    }

    /**
      * Gets the value of the 'platformVersion' field.
      * platform version
      * @return The value.
      */
    public java.lang.String getPlatformVersion() {
      return platformVersion;
    }


    /**
      * Sets the value of the 'platformVersion' field.
      * platform version
      * @param value The value of 'platformVersion'.
      * @return This builder.
      */
    public net.lightapi.portal.deployment.PlatformUpdatedEvent.Builder setPlatformVersion(java.lang.String value) {
      validate(fields()[3], value);
      this.platformVersion = value;
      fieldSetFlags()[3] = true;
      return this;
    }

    /**
      * Checks whether the 'platformVersion' field has been set.
      * platform version
      * @return True if the 'platformVersion' field has been set, false otherwise.
      */
    public boolean hasPlatformVersion() {
      return fieldSetFlags()[3];
    }


    /**
      * Clears the value of the 'platformVersion' field.
      * platform version
      * @return This builder.
      */
    public net.lightapi.portal.deployment.PlatformUpdatedEvent.Builder clearPlatformVersion() {
      platformVersion = null;
      fieldSetFlags()[3] = false;
      return this;
    }

    /**
      * Gets the value of the 'clientType' field.
      * client type
      * @return The value.
      */
    public java.lang.String getClientType() {
      return clientType;
    }


    /**
      * Sets the value of the 'clientType' field.
      * client type
      * @param value The value of 'clientType'.
      * @return This builder.
      */
    public net.lightapi.portal.deployment.PlatformUpdatedEvent.Builder setClientType(java.lang.String value) {
      validate(fields()[4], value);
      this.clientType = value;
      fieldSetFlags()[4] = true;
      return this;
    }

    /**
      * Checks whether the 'clientType' field has been set.
      * client type
      * @return True if the 'clientType' field has been set, false otherwise.
      */
    public boolean hasClientType() {
      return fieldSetFlags()[4];
    }


    /**
      * Clears the value of the 'clientType' field.
      * client type
      * @return This builder.
      */
    public net.lightapi.portal.deployment.PlatformUpdatedEvent.Builder clearClientType() {
      clientType = null;
      fieldSetFlags()[4] = false;
      return this;
    }

    /**
      * Gets the value of the 'clientUrl' field.
      * client url
      * @return The value.
      */
    public java.lang.String getClientUrl() {
      return clientUrl;
    }


    /**
      * Sets the value of the 'clientUrl' field.
      * client url
      * @param value The value of 'clientUrl'.
      * @return This builder.
      */
    public net.lightapi.portal.deployment.PlatformUpdatedEvent.Builder setClientUrl(java.lang.String value) {
      validate(fields()[5], value);
      this.clientUrl = value;
      fieldSetFlags()[5] = true;
      return this;
    }

    /**
      * Checks whether the 'clientUrl' field has been set.
      * client url
      * @return True if the 'clientUrl' field has been set, false otherwise.
      */
    public boolean hasClientUrl() {
      return fieldSetFlags()[5];
    }


    /**
      * Clears the value of the 'clientUrl' field.
      * client url
      * @return This builder.
      */
    public net.lightapi.portal.deployment.PlatformUpdatedEvent.Builder clearClientUrl() {
      clientUrl = null;
      fieldSetFlags()[5] = false;
      return this;
    }

    /**
      * Gets the value of the 'credentials' field.
      * credentials
      * @return The value.
      */
    public java.lang.String getCredentials() {
      return credentials;
    }


    /**
      * Sets the value of the 'credentials' field.
      * credentials
      * @param value The value of 'credentials'.
      * @return This builder.
      */
    public net.lightapi.portal.deployment.PlatformUpdatedEvent.Builder setCredentials(java.lang.String value) {
      validate(fields()[6], value);
      this.credentials = value;
      fieldSetFlags()[6] = true;
      return this;
    }

    /**
      * Checks whether the 'credentials' field has been set.
      * credentials
      * @return True if the 'credentials' field has been set, false otherwise.
      */
    public boolean hasCredentials() {
      return fieldSetFlags()[6];
    }


    /**
      * Clears the value of the 'credentials' field.
      * credentials
      * @return This builder.
      */
    public net.lightapi.portal.deployment.PlatformUpdatedEvent.Builder clearCredentials() {
      credentials = null;
      fieldSetFlags()[6] = false;
      return this;
    }

    /**
      * Gets the value of the 'value' field.
      * value in json
      * @return The value.
      */
    public java.lang.String getValue() {
      return value;
    }


    /**
      * Sets the value of the 'value' field.
      * value in json
      * @param value The value of 'value'.
      * @return This builder.
      */
    public net.lightapi.portal.deployment.PlatformUpdatedEvent.Builder setValue(java.lang.String value) {
      validate(fields()[7], value);
      this.value = value;
      fieldSetFlags()[7] = true;
      return this;
    }

    /**
      * Checks whether the 'value' field has been set.
      * value in json
      * @return True if the 'value' field has been set, false otherwise.
      */
    public boolean hasValue() {
      return fieldSetFlags()[7];
    }


    /**
      * Clears the value of the 'value' field.
      * value in json
      * @return This builder.
      */
    public net.lightapi.portal.deployment.PlatformUpdatedEvent.Builder clearValue() {
      value = null;
      fieldSetFlags()[7] = false;
      return this;
    }

    @Override
    @SuppressWarnings("unchecked")
    public PlatformUpdatedEvent build() {
      try {
        PlatformUpdatedEvent record = new PlatformUpdatedEvent();
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
        record.platformId = fieldSetFlags()[1] ? this.platformId : (java.lang.String) defaultValue(fields()[1]);
        record.platformName = fieldSetFlags()[2] ? this.platformName : (java.lang.String) defaultValue(fields()[2]);
        record.platformVersion = fieldSetFlags()[3] ? this.platformVersion : (java.lang.String) defaultValue(fields()[3]);
        record.clientType = fieldSetFlags()[4] ? this.clientType : (java.lang.String) defaultValue(fields()[4]);
        record.clientUrl = fieldSetFlags()[5] ? this.clientUrl : (java.lang.String) defaultValue(fields()[5]);
        record.credentials = fieldSetFlags()[6] ? this.credentials : (java.lang.String) defaultValue(fields()[6]);
        record.value = fieldSetFlags()[7] ? this.value : (java.lang.String) defaultValue(fields()[7]);
        return record;
      } catch (org.apache.avro.AvroMissingFieldException e) {
        throw e;
      } catch (java.lang.Exception e) {
        throw new org.apache.avro.AvroRuntimeException(e);
      }
    }
  }

  @SuppressWarnings("unchecked")
  private static final org.apache.avro.io.DatumWriter<PlatformUpdatedEvent>
    WRITER$ = (org.apache.avro.io.DatumWriter<PlatformUpdatedEvent>)MODEL$.createDatumWriter(SCHEMA$);

  @Override public void writeExternal(java.io.ObjectOutput out)
    throws java.io.IOException {
    WRITER$.write(this, SpecificData.getEncoder(out));
  }

  @SuppressWarnings("unchecked")
  private static final org.apache.avro.io.DatumReader<PlatformUpdatedEvent>
    READER$ = (org.apache.avro.io.DatumReader<PlatformUpdatedEvent>)MODEL$.createDatumReader(SCHEMA$);

  @Override public void readExternal(java.io.ObjectInput in)
    throws java.io.IOException {
    READER$.read(this, SpecificData.getDecoder(in));
  }

  @Override protected boolean hasCustomCoders() { return true; }

  @Override public void customEncode(org.apache.avro.io.Encoder out)
    throws java.io.IOException
  {
    this.EventId.customEncode(out);

    out.writeString(this.platformId);

    out.writeString(this.platformName);

    out.writeString(this.platformVersion);

    out.writeString(this.clientType);

    out.writeString(this.clientUrl);

    out.writeString(this.credentials);

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

      this.platformId = in.readString();

      this.platformName = in.readString();

      this.platformVersion = in.readString();

      this.clientType = in.readString();

      this.clientUrl = in.readString();

      this.credentials = in.readString();

      this.value = in.readString();

    } else {
      for (int i = 0; i < 8; i++) {
        switch (fieldOrder[i].pos()) {
        case 0:
          if (this.EventId == null) {
            this.EventId = new com.networknt.kafka.common.EventId();
          }
          this.EventId.customDecode(in);
          break;

        case 1:
          this.platformId = in.readString();
          break;

        case 2:
          this.platformName = in.readString();
          break;

        case 3:
          this.platformVersion = in.readString();
          break;

        case 4:
          this.clientType = in.readString();
          break;

        case 5:
          this.clientUrl = in.readString();
          break;

        case 6:
          this.credentials = in.readString();
          break;

        case 7:
          this.value = in.readString();
          break;

        default:
          throw new java.io.IOException("Corrupt ResolvingDecoder.");
        }
      }
    }
  }
}
