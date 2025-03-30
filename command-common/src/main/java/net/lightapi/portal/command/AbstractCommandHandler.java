package net.lightapi.portal.command;
import com.networknt.config.Config;
import com.networknt.config.JsonMapper;
import com.networknt.httpstring.AttachmentConstants;
import com.networknt.monad.Result;
import com.networknt.monad.Success;
import com.networknt.utility.Constants;
import com.networknt.utility.NioUtils;
import com.networknt.rpc.HybridHandler;
import com.networknt.utility.Util;
import io.cloudevents.CloudEvent;
import io.cloudevents.core.builder.CloudEventBuilder;
import io.undertow.server.HttpServerExchange;
import net.lightapi.portal.HybridQueryClient;
import net.lightapi.portal.PortalConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;

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

    @Override
    public ByteBuffer handle(HttpServerExchange exchange, Object input) {
        Logger logger = getLogger(); // Get logger specific to subclass
        if(logger.isTraceEnabled()) logger.trace("input = {}", input);

        // --- 1. Input Validation and Audit Info ---
        Map<String, Object> map = (Map<String, Object>) input;
        Map<String, Object> auditInfo = exchange.getAttachment(AttachmentConstants.AUDIT_INFO);
        String userId = (String) auditInfo.get(Constants.USER_ID_STRING);
        if (userId == null) {
            logger.error("Incorrect token type: userId is null. Must be Authorization Code Token.");
            return NioUtils.toByteBuffer(getStatus(exchange, INCORRECT_TOKEN_TYPE, "Authorization Code Token"));
        }
        String host = (String) auditInfo.get(Constants.HOST);
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
        long nonce = Long.parseLong(result.getResult());

        // --- 3. Build CloudEvent ---
        CloudEvent event = buildCloudEvent(map, userId, host, nonce);

        // --- 4. Send to Kafka ---
        ProducerRecord<String, CloudEvent> record = new ProducerRecord<>(config.getTopic(), (config.isMultitenancy() ? host : userId), event);
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

        // --- 5. Return Success Response ---
        // Return the original input map as confirmation, serialized to JSON
        return NioUtils.toByteBuffer(customizeOutput(map));
    }


    /**
     * Builds the CloudEvent object.
     */
    protected CloudEvent buildCloudEvent(Map<String, Object> map, String userId, String host, long nonce) {

        CloudEventBuilder eventTemplate = CloudEventBuilder.v1()
                .withSource(EVENT_SOURCE)
                .withType(getCloudEventType()); // Use the abstract method

        String data = JsonMapper.toJson(map);

        return eventTemplate.newBuilder()
                .withId(Util.getUUID())
                .withTime(OffsetDateTime.now())
                .withExtension("user", userId) // Use correct casing
                .withExtension("nonce", nonce)
                .withExtension("host", host)   // Use correct casing
                .withData("application/json", data.getBytes(StandardCharsets.UTF_8))
                .build();
    }

    /**
     * Gets the specific logger instance for the concrete subclass.
     * Must be implemented by subclasses.
     */
    protected abstract Logger getLogger();


}
