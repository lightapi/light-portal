package net.lightapi.portal.command;
import com.networknt.config.Config;
import com.networknt.config.JsonMapper;
import com.networknt.httpstring.AttachmentConstants;
import com.networknt.monad.Failure;
import com.networknt.monad.Result;
import com.networknt.monad.Success;
import com.networknt.status.Status;
import com.networknt.utility.Constants;
import com.networknt.utility.NioUtils;
import com.networknt.rpc.HybridHandler;
import com.networknt.utility.Util;
import com.networknt.utility.UuidUtil;
import io.cloudevents.CloudEvent;
import io.cloudevents.core.builder.CloudEventBuilder;
import io.cloudevents.jackson.JsonFormat;
import io.undertow.server.HttpServerExchange;
import net.lightapi.portal.HybridQueryClient;
import net.lightapi.portal.PortalConfig;
import net.lightapi.portal.PortalConstants;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.net.URI;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.OffsetDateTime;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CountDownLatch;
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
    protected static final PortalConfig config = (PortalConfig) Config.getInstance().getJsonObjectConfig(PortalConfig.CONFIG_NAME, PortalConfig.class);
    private static final JsonFormat jsonFormat = new JsonFormat();
    public static final String USER_ID = "userId";
    public static final String HOST_ID = "hostId";

    protected static final String INCORRECT_TOKEN_TYPE = "ERR11601";
    protected static final String SEND_MESSAGE_EXCEPTION = "ERR11605";
    public static final URI EVENT_SOURCE = URI.create("https://github.com/lightapi/light-portal");

    /**
     * Subclasses must implement this to provide the specific CloudEvent type string. The type must be unique within the event source.
     * @return The CloudEvent type (e.g., "SchemaCreatedEvent").
     */
    protected abstract String getCloudEventType();

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

        // --- 2. Get Nonce ---
        Result<String> result = HybridQueryClient.getNonceByUserId(exchange, userId);
        if(result.isFailure()) {
            return NioUtils.toByteBuffer(getStatus(exchange, result.getError()));
        }
        Number nonce = parseNumber(result.getResult());
        if(logger.isTraceEnabled()) logger.trace("nonce = {}", nonce);

        // --- 3. Build CloudEvent ---
        CloudEvent event = buildCloudEvent(map, userId, host, nonce);

        // --- 4. Send to Kafka ---
        ProducerRecord<String, byte[]> record = new ProducerRecord<>(config.getTopic(), (config.isMultitenancy() ? host : userId), jsonFormat.serialize(event));
        final CountDownLatch latch = new CountDownLatch(1);
        try {
            HybridCommandStartup.producer.send(record, (recordMetadata, e) -> {
                if (Objects.nonNull(e)) {
                    logger.error("Exception occurred while pushing the event", e);
                } else {
                    logger.info("Event record pushed successfully. Received Record Metadata is {}",
                            recordMetadata);
                }
                latch.countDown();
            });
            latch.await();
        } catch (InterruptedException e) {
            logger.error("Exception:", e);
            return NioUtils.toByteBuffer(getStatus(exchange, SEND_MESSAGE_EXCEPTION, e.getMessage(), config.isMultitenancy() ? host : userId));
        }
        Result<Map<String, Object>> additionalActionResult = additionalAction(exchange, map, userId, host);
        if (additionalActionResult.isFailure()) {
            return NioUtils.toByteBuffer(getStatus(exchange, additionalActionResult.getError()));
        }
        // --- 5. Return Success Response ---
        // Return the original input map as confirmation, serialized to JSON
        return NioUtils.toByteBuffer(customizeOutput(map));
    }


    /**
     * Builds the CloudEvent object.
     */
    protected CloudEvent buildCloudEvent(Map<String, Object> map, String userId, String host, Number nonce) {

        CloudEventBuilder eventTemplate = CloudEventBuilder.v1()
                .withSource(EVENT_SOURCE)
                .withType(getCloudEventType());

        String data = JsonMapper.toJson(map);
        if(getLogger().isTraceEnabled()) getLogger().trace("event user = {} host = {} type = {} and data = {}", userId, host, getCloudEventType(), data);
        return eventTemplate.newBuilder()
                .withId(UuidUtil.getUUID().toString())
                .withTime(OffsetDateTime.now())
                .withExtension(Constants.USER, userId)
                .withExtension(PortalConstants.NONCE, nonce)
                .withExtension(Constants.HOST, host)
                .withData("application/json", data.getBytes(StandardCharsets.UTF_8))
                .build();
    }

    /**
     * Gets the specific logger instance for the concrete subclass.
     * Must be implemented by subclasses.
     */
    protected abstract Logger getLogger();

    public static Number parseNumber(String str) throws NumberFormatException {
        if (str == null || str.trim().isEmpty()) {
            throw new NumberFormatException("Input string is null or empty");
        }
        String trimmed = str.trim();

        // Check if the string represents a floating-point number
        if (trimmed.matches(".*[.eE].*")) {
            try {
                return parseFloatingPoint(trimmed);
            } catch (NumberFormatException e) {
                throw new NumberFormatException("Invalid floating-point number: " + trimmed);
            }
        } else {
            // Handle integer types (Integer, Long, or BigInteger)
            try {
                return parseInteger(trimmed);
            } catch (NumberFormatException e) {
                throw new NumberFormatException("Invalid integer number: " + trimmed);
            }
        }
    }

    private static Number parseFloatingPoint(String str) {
        try {
            return Double.parseDouble(str); // Try Double first
        } catch (NumberFormatException e) {
            return new BigDecimal(str); // Fallback to BigDecimal for precision
        }
    }

    private static Number parseInteger(String str) {
        try {
            return Integer.parseInt(str); // Try Integer first
        } catch (NumberFormatException e1) {
            try {
                return Long.parseLong(str); // Then Long
            } catch (NumberFormatException e2) {
                return new BigInteger(str); // Fallback to BigInteger for large values
            }
        }
    }

}
