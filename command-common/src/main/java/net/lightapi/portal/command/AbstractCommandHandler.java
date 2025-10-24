package net.lightapi.portal.command;

import com.networknt.config.JsonMapper;
import com.networknt.db.provider.DbProvider;
import com.networknt.httpstring.AttachmentConstants;
import com.networknt.monad.Failure;
import com.networknt.monad.Result;
import com.networknt.monad.Success;
import com.networknt.rpc.HybridHandler;
import com.networknt.service.SingletonServiceFactory;
import com.networknt.status.Status;
import com.networknt.utility.Constants;
import com.networknt.utility.NioUtils;
import com.networknt.utility.UuidUtil;
import io.cloudevents.CloudEvent;
import io.cloudevents.core.builder.CloudEventBuilder;
import io.undertow.server.HttpServerExchange;
import net.lightapi.portal.EventTypeUtil;
import net.lightapi.portal.HybridQueryClient;
import net.lightapi.portal.PortalConstants;
import net.lightapi.portal.PortalUtil;
import net.lightapi.portal.db.PortalDbProvider;
import org.slf4j.Logger;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.OffsetDateTime;
import java.util.Map;
import java.util.Objects;
/**
 * Abstract base class for handling schema commands that produce CloudEvents.
 * Implements the common logic for validation, nonce retrieval, event creation,
 * and Kafka publishing using the Template Method pattern.
 */
public abstract class AbstractCommandHandler implements HybridHandler {
    // Use protected logger so subclasses can potentially use it,
    // initialized specifically in subclasses for correct class name logging.
    // Alternatively, initialize here using getClass() if not static.
    // protected final Logger logger = LoggerFactory.getLogger(getClass());

    // Make config and constants protected or private static final
    public static PortalDbProvider dbProvider = (PortalDbProvider) SingletonServiceFactory.getBean(DbProvider.class);
    public static final String USER_ID = "userId";
    public static final String HOST_ID = "hostId";

    protected static final String INCORRECT_TOKEN_TYPE = "ERR11601";
    protected static final String OBJECT_NOT_FOUND = "ERR11637";

    /**
     * Subclasses must implement this to provide the specific CloudEvent type string. The type must be unique within the event source.
     * @return The CloudEvent type (e.g., "SchemaCreatedEvent").
     */
    protected abstract String getCloudEventType();

    /**
     * Subclasses must implement this to provide aggregate type
     * @return The CloudEvent aggregate type (e.g., "AuthCode").
     */
    protected String getCloudEventAggregateType() {
        return EventTypeUtil.deriveAggregateTypeFromEventType(getCloudEventType());
    }

    /**
     * Subclasses must implement this to provide aggregate io
     * @param map The input map.
     * @return The CloudEvent aggregate id (e.g., "01964b05-5532-7c79-8cde-191dcbd421b7").
     */
    protected String getCloudEventAggregateId(Map<String, Object> map) {
        return EventTypeUtil.getAggregateId(getCloudEventType(), map);
    }

    /**
     * Subclasses can implement this to do additional validation with input map.
     * @param exchange The HTTP server exchange.
     * @param map The input map.
     */
    protected Result<Map<String, Object>> validateInput(HttpServerExchange exchange, Map<String, Object> map, String userId, String host) {
        // Default implementation does nothing. Subclasses can override if needed.
        return Success.of(map);
    }

    /**
     * Subclasses can implement this to enrich the input map. In most cases, it will call the query side service to get some data and put it in the map.
     * @param exchange The HTTP server exchange.
     * @param map The input map.
     */
    protected Result<Map<String, Object>> enrichInput(HttpServerExchange exchange, Map<String, Object> map) {
        // Default implementation does nothing. Subclasses can override if needed.
        return Success.of(map);
    }

    /**
     * Subclasses can override this to return customized response to the caller.
     * @param map The input map.
     */
    protected String customizeOutput(Map<String, Object> map) {
        // Default implementation does nothing. Just return the map.
        return JsonMapper.toJson(map);
    }

    /**
     * Subclasses can override this to return customized status to the caller. By default, it validates
     * the token as authorization code token as most portal command services require a login user. However,
     * some service like createAuthCode does not require a login user and the token type is client credentials token.
     */
    protected Result<Map<String, Object>> validateTokenType(String userId, Map<String, Object> map) {
        if (userId == null) {
            getLogger().error("Incorrect token type: userId is null. Must be Authorization Code Token.");
            return Failure.of(new Status(INCORRECT_TOKEN_TYPE, "Authorization Code Token"));
        }
        return Success.of(map);
    }

    /**
     * Subclasses can override this to invoke additional action(s) before returning. For example, send an email after
     * the password reset event is sent. Invoke the Ansible executor in the createDeployment handler, etc.
     * If there is no additional action, just return the input map.
     *
     * @param exchange The HTTP server exchange.
     *
     */
    protected Result<Map<String, Object>> additionalAction(HttpServerExchange exchange, Map<String, Object> map, String userId, String host) {
        return Success.of(map);
    }

    /**
     * Subclasses can override this to use customized logic to populate the old aggregateVersion and new aggregateVersion
     * based on the event type and existing aggregateVersion in the data map.
     *
     */
    protected Result<Map<String, Object>> populateAggregateVersion(Map<String, Object> map) {
        String eventType = getCloudEventType();
        // for created event, we can derive the new aggregate versions.
        if (eventType.endsWith("CreatedEvent") || eventType.endsWith("OnboardedEvent")) {
            // For auth code created event, we don't need to increment the aggregate version.
            map.put(PortalConstants.NEW_AGGREGATE_VERSION, 1);
            map.put(PortalConstants.AGGREGATE_VERSION, 0);
            return Success.of(map);
        }
        // For other events, we need to increment the aggregate version.
        if( !map.containsKey(PortalConstants.AGGREGATE_VERSION)) {
            getLogger().error("The input map does not contain the aggregate version for event type {}.", eventType);
            return Failure.of(new Status(OBJECT_NOT_FOUND, "aggregateVersion", eventType));
        }
        int oldAggregateVersion = ((Number) Objects.requireNonNull(map.get(PortalConstants.AGGREGATE_VERSION))).intValue();
        // Increment the aggregate version by 1 for the new event.
        int newAggregateVersion = oldAggregateVersion + 1;
        map.put(PortalConstants.NEW_AGGREGATE_VERSION, newAggregateVersion);
        return Success.of(map);
    }

    @Override
    public ByteBuffer handle(HttpServerExchange exchange, Object input) {
        Logger logger = getLogger();
        System.out.println(logger.getName());
        if(logger.isTraceEnabled()) logger.trace("input = {}", input);

        // --- 1. Input Validation and Audit Info ---
        Map<String, Object> map = (Map<String, Object>) input;
        Map<String, Object> auditInfo = exchange.getAttachment(AttachmentConstants.AUDIT_INFO);
        String userId = (String) auditInfo.get(Constants.USER_ID_STRING);
        Result<Map<String, Object>> tokenTypeResult = validateTokenType(userId, map);
        if (tokenTypeResult.isFailure()) {
            return NioUtils.toByteBuffer(getStatus(exchange, tokenTypeResult.getError()));
        }

        // handle the admin to create global entity for some commands.
        String role = (String) auditInfo.get(Constants.ROLE);
        if(role != null && role.contains(PortalConstants.ADMIN_ROLE)) {
            Boolean globalFlag = (Boolean)map.get(PortalConstants.GLOBAL_FLAG);
            if(globalFlag != null && globalFlag) {
                // remove the hostId from the map to allow the admin to create global entity.
                // It means the table row will have null for hostId.
                map.remove(PortalConstants.HOST_ID);
                map.remove(PortalConstants.GLOBAL_FLAG);
            }
        }

        String host = (String) auditInfo.get(Constants.HOST);
        if (host == null) {
            host = (String) map.get(HOST_ID);
        }
        if(userId == null) {
            userId = (String) map.get(USER_ID);
        }
        if(logger.isTraceEnabled()) logger.trace("userId = {}, host = {}", userId, host);

        // Validate input map
        Result<Map<String, Object>> validatedResult = validateInput(exchange, map, userId, host);
        if (validatedResult.isFailure()) {
            return NioUtils.toByteBuffer(getStatus(exchange, validatedResult.getError()));
        }

        // Enrich input map with additional data if needed
        Result<Map<String, Object>> enrichedResult = enrichInput(exchange, map);
        if (enrichedResult.isFailure()) {
            return NioUtils.toByteBuffer(getStatus(exchange, enrichedResult.getError()));
        }
        if(userId == null) {
            // in case the userId is enriched in the enrichInput() method. i.e. createUser command.
            userId = (String)map.get(USER_ID);
        }
        // --- 2. Get Nonce ---
        Number nonce;
        Result<String> result = HybridQueryClient.getNonceByUserId(exchange, userId);
        if(result.isFailure()) {
            if(result.getError().getStatusCode() != 404) {
                return NioUtils.toByteBuffer(getStatus(exchange, result.getError()));
            } else {
                // this is a brand-new user that is created or onboarded.
                nonce = 1;
            }
        } else {
            nonce = PortalUtil.parseNumber(result.getResult());
        }
        if(logger.isTraceEnabled()) logger.trace("nonce = {}", nonce);

        // get the new aggregate version and put it in the map.
        Result<Map<String, Object>> aggregateVersionResult = populateAggregateVersion(map);
        if (aggregateVersionResult.isFailure()) {
            return NioUtils.toByteBuffer(getStatus(exchange, aggregateVersionResult.getError()));
        }

        // --- 3. Build CloudEvent ---
        CloudEvent[] events = buildCloudEvent(map, userId, host, nonce);
        if(logger.isTraceEnabled()) {
            // Log the created CloudEvents for debugging purposes
            for (CloudEvent event : events) {
                logger.trace("Created CloudEvent: id={}, type={}, source={}, data={}",
                        event.getId(), event.getType(), event.getSource(), new String(event.getData().toBytes(), StandardCharsets.UTF_8));
            }
        }

        // --- 4. Write to Postgres Database ---
        Result<String> eventStoreResult = dbProvider.insertEventStore(events);
        if(eventStoreResult.isFailure()) {
            logger.error("Failed to insert event store: {}", eventStoreResult.getError());
            return NioUtils.toByteBuffer(getStatus(exchange, eventStoreResult.getError()));
        }

        // --- 5. Additional Action ---
        Result<Map<String, Object>> additionalActionResult = additionalAction(exchange, map, userId, host);
        if (additionalActionResult.isFailure()) {
            return NioUtils.toByteBuffer(getStatus(exchange, additionalActionResult.getError()));
        }
        // --- 5. Return Success Response ---
        // Return the original input map as confirmation, serialized to JSON
        return NioUtils.toByteBuffer(customizeOutput(map));
    }

    /**
     * Builds the CloudEvent object array from the provided map, userId, host, and nonce. Overrides the default method if
     * you want to customize the CloudEvent creation logic to emit more than one CloudEvent.
     */
    protected CloudEvent[] buildCloudEvent(Map<String, Object> map, String userId, String host, Number nonce) {
        String eventType = getCloudEventType(); // e.g., "ClientCreatedEvent"
        String derivedAggregateType = EventTypeUtil.deriveAggregateTypeFromEventType(eventType);
        if (derivedAggregateType == null) {
            // Fallback to explicit Aggregate Type defined in command handler (if derivation fails)
            derivedAggregateType = getCloudEventAggregateType(); // e.g., PortalConstants.AGGREGATE_CLIENT
            if (derivedAggregateType == null) {
                throw new IllegalStateException("Failed to derive or get aggregate type for event: " + eventType);
            }
            logger.warn("Could not derive aggregate type for eventType '{}'. Using explicit aggregate type: {}", eventType, derivedAggregateType);
        }
        String aggregateId = EventTypeUtil.getAggregateId(eventType, map);
        if (aggregateId == null) {
            // Fallback to the method that gets aggregate id from the map.
            aggregateId = getCloudEventAggregateId(map);
            if (aggregateId == null) {
                throw new IllegalStateException("Failed to derive or get aggregate id for event: " + eventType);
            }
            logger.warn("Could not derive aggregate id for eventType '{}'. Using aggregateId from map", eventType);
        }

        CloudEventBuilder eventTemplate = CloudEventBuilder.v1()
                .withSource(PortalConstants.EVENT_SOURCE)
                .withType(eventType);

        String data = JsonMapper.toJson(map);
        if(getLogger().isTraceEnabled()) getLogger().trace("event user = {} host = {} type = {} and data = {}", userId, host, getCloudEventType(), data);
        return new CloudEvent[]{eventTemplate.newBuilder()
                .withId(UuidUtil.getUUID().toString())
                .withTime(OffsetDateTime.now())
                .withSubject(aggregateId)
                .withExtension(Constants.USER, userId)
                .withExtension(PortalConstants.NONCE, nonce)
                .withExtension(Constants.HOST, host)
                .withExtension(PortalConstants.AGGREGATE_TYPE, derivedAggregateType)
                .withExtension(PortalConstants.EVENT_AGGREGATE_VERSION, (Number)map.get(PortalConstants.NEW_AGGREGATE_VERSION))
                .withData("application/json", data.getBytes(StandardCharsets.UTF_8))
                .build()};
    }

    /**
     * Gets the specific logger instance for the concrete subclass.
     * Must be implemented by subclasses.
     */
    protected abstract Logger getLogger();


}
