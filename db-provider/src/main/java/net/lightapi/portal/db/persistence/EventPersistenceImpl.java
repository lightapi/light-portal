package net.lightapi.portal.db.persistence;

import com.networknt.config.JsonMapper;
import com.networknt.monad.Failure;
import com.networknt.monad.Result;
import com.networknt.monad.Success;
import com.networknt.status.Status;
import com.networknt.utility.Constants;
import com.networknt.utility.UuidUtil;
import io.cloudevents.CloudEvent;
import io.cloudevents.jackson.JsonFormat;
import net.lightapi.portal.PortalConstants;
import net.lightapi.portal.db.PortalDbProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.OffsetDateTime;
import java.util.*;
import java.util.stream.Collectors;

import static com.networknt.config.JsonMapper.objectMapper;
import static com.networknt.db.provider.SqlDbStartupHook.ds;
import static net.lightapi.portal.db.PortalDbProviderImpl.GENERIC_EXCEPTION;
import static net.lightapi.portal.db.PortalDbProviderImpl.SQL_EXCEPTION;

public class EventPersistenceImpl implements EventPersistence {
    private static final Logger logger = LoggerFactory.getLogger(EventPersistenceImpl.class);
    private static final String OBJECT_NOT_FOUND = PortalDbProvider.OBJECT_NOT_FOUND;

    private static final JsonFormat jsonFormat = new JsonFormat();

    public static final String insertEventStoreSql = "INSERT INTO event_store_t " +
            "(id, host_id, user_id, nonce, aggregate_id, aggregate_version, aggregate_type, event_type, event_ts, payload, metadata) " +
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?::jsonb, ?::jsonb)"; // Use ?::jsonb for JSONB casting

    // SQL for outbox_message_t table
    public static final String insertOutboxMessageSql = "INSERT INTO outbox_message_t " +
            "(id, host_id, user_id, nonce, aggregate_id, aggregate_version, aggregate_type, event_type, event_ts, payload, metadata, c_offset, transaction_id) " +
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?::jsonb, ?::jsonb, ?, ?)"; // Use ?::jsonb for JSONB casting

    /**
     * Inserts multiple CloudEvents into the event_store_t and outbox_message_t tables
     * within a single database transaction.
     *
     * @param events An array of CloudEvent objects to be persisted.
     * @return A Result indicating success or failure.
     */
    @Override
    public Result<String> insertEventStore(CloudEvent[] events) {
        // SQL for event_store_t table

        if (events == null || events.length == 0) {
            return Success.of("No events to insert.");
        }

        Result<String> result;
        try (Connection conn = ds.getConnection()) {
            conn.setAutoCommit(false); // Start transaction
            try (PreparedStatement eventStorePst = conn.prepareStatement(insertEventStoreSql);
                 PreparedStatement outboxPst = conn.prepareStatement(insertOutboxMessageSql)) {

                long currentOffset = reserveOffsets(conn, events.length);
                UUID transactionId = UuidUtil.getUUID();
                for (CloudEvent event : events) {
                    // Extract common CloudEvents attributes
                    UUID eventId = UUID.fromString(event.getId());
                    OffsetDateTime eventTs = event.getTime(); // Already OffsetDateTime
                    String aggregateId = event.getSubject(); // CloudEvent 'subject' maps to 'aggregate_id'
                    String eventType = event.getType();

                    // Extract custom CloudEvents extensions or properties if needed (e.g., host_id, user_id, aggregate_type)
                    // Assuming host_id and user_id are UUID strings in CloudEvent extensions
                    UUID hostId = null;
                    if (event.getExtension(Constants.HOST) instanceof String hostIdStr) {
                        hostId = UUID.fromString(hostIdStr);
                    } else if (event.getExtension(Constants.HOST) instanceof UUID hostIdUuid) {
                        hostId = hostIdUuid;
                    }
                    if (hostId == null) {
                        // Handle error or use a default/null value if host_id is truly optional
                        // For NOT NULL column, you must provide a value or throw error
                        throw new IllegalArgumentException("CloudEvent missing required host_id extension.");
                    }

                    UUID userId = null;
                    if (event.getExtension(Constants.USER) instanceof String userIdStr) {
                        userId = UUID.fromString(userIdStr);
                    } else if (event.getExtension(Constants.USER) instanceof UUID userIdUuid) {
                        userId = userIdUuid;
                    }
                    if (userId == null) {
                        throw new IllegalArgumentException("CloudEvent missing required user_id extension.");
                    }

                    // Assuming nonce is also an extension
                    long nonce = ((Number) Objects.requireNonNull(event.getExtension(PortalConstants.NONCE))).longValue();

                    // Assuming aggregate_type is also an extension
                    String aggregateType =(String)event.getExtension(PortalConstants.AGGREGATE_TYPE);
                    if (aggregateType == null) {
                        throw new IllegalArgumentException("Could not determine aggregatetype from CloudEvent.");
                    }

                    // Assuming aggregateVersion is also an extension
                    Number aggregateVersionNumber = (Number)event.getExtension(PortalConstants.EVENT_AGGREGATE_VERSION);
                    if (aggregateVersionNumber == null) {
                        throw new IllegalArgumentException("CloudEvent missing required aggregate_version extension.");
                    }
                    long aggregateVersion = aggregateVersionNumber.longValue();

                    // Extract payload which is the CloudEvent
                    byte[] payloadByte = jsonFormat.serialize(event);
                    String payloadJson = new String(payloadByte, StandardCharsets.UTF_8);
                    // Extract metadata (CloudEvent context attributes + custom extensions)
                    // You might want to include all CloudEvent attributes (id, source, time, etc.) in metadata
                    // or just custom extensions not explicitly mapped to columns.
                    // For simplicity, let's just include all extensions as metadata JSON.
                    Map<String, Object> extensions = event.getExtensionNames().stream()
                            .filter(
                                    extName -> !extName.equals(Constants.HOST) &&
                                            !extName.equals(Constants.USER) &&
                                            !extName.equals(PortalConstants.AGGREGATE_TYPE) &&
                                            !extName.equals(PortalConstants.EVENT_AGGREGATE_VERSION) &&
                                            !extName.equals(PortalConstants.NONCE)
                            )
                            .collect(Collectors.toMap(
                                    extName -> extName,
                                    event::getExtension
                            ));
                    String metadataJson = objectMapper.writeValueAsString(extensions);

                    // Set parameters for event_store_t
                    eventStorePst.setObject(1, eventId);
                    eventStorePst.setObject(2, hostId);
                    eventStorePst.setObject(3, userId);
                    eventStorePst.setLong(4,  nonce);
                    eventStorePst.setString(5, aggregateId);
                    eventStorePst.setLong(6,  aggregateVersion);
                    eventStorePst.setString(7, aggregateType);
                    eventStorePst.setString(8, eventType);
                    eventStorePst.setObject(9, eventTs); // Use OffsetDateTime for TIMESTAMP WITH TIME ZONE
                    eventStorePst.setString(10, payloadJson);
                    eventStorePst.setString(11, metadataJson);
                    eventStorePst.addBatch();

                    // Set parameters for outbox_message_t
                    outboxPst.setObject(1, eventId);
                    outboxPst.setObject(2, hostId);
                    outboxPst.setObject(3, userId);
                    outboxPst.setLong(4,  nonce);
                    outboxPst.setString(5, aggregateId);
                    outboxPst.setLong(6,  aggregateVersion);
                    outboxPst.setString(7, aggregateType);
                    outboxPst.setString(8, eventType);
                    outboxPst.setObject(9, eventTs); // Use OffsetDateTime for TIMESTAMP WITH TIME ZONE
                    outboxPst.setString(10, payloadJson);
                    outboxPst.setString(11, metadataJson);
                    outboxPst.setLong(12, currentOffset++);
                    outboxPst.setObject(13, transactionId);
                    outboxPst.addBatch();
                }

                // Execute all batched inserts
                eventStorePst.executeBatch();
                outboxPst.executeBatch();

                conn.commit(); // Commit the transaction
                result = Success.of("Successfully inserted " + events.length + " events.");

                // In a real application, you might remove the notificationService calls from here
                // and have a consumer process them from Kafka.
                // notificationService.insertNotification(event, true, null); // This belongs to a consumer of Kafka
            } catch (SQLException e) {
                // Log and rollback on SQL errors
                logger.error("SQLException:", e);
                conn.rollback();
                // notificationService.insertNotification(event, false, e.getMessage()); // This belongs to a consumer of Kafka
                result = Failure.of(new Status(SQL_EXCEPTION, e.getMessage()));
            } catch (Exception e) {
                // Catch other exceptions
                logger.error("Exception:", e);
                conn.rollback();
                result = Failure.of(new Status(GENERIC_EXCEPTION, e.getMessage()));
            }
        } catch (SQLException e) {
            // Catch connection errors
            logger.error("SQLException:", e);
            result = Failure.of(new Status(SQL_EXCEPTION, e.getMessage()));
        }
        return result;
    }

    /**
     * Retrieves events from the event store based on various filters. This method supports pagination and it is
     * used on the UI from the event page.
     *
     * @param offset offset for pagination
     * @param limit limit for pagination
     * @param hostId host identifier, can be null
     * @param id unique identifier for the event, can be null
     * @param userId user identifier, can be null
     * @param nonce user nonce
     * @param aggregateId aggregate identifier, can be null
     * @param aggregateVersion sequence number for optimistic concurrency control
     * @param aggregateType aggregate type, can be null
     * @param eventType event type, can be null
     * @param payload payload of the event, can be null
     * @param metaData metadata of the event, can be null
     * @return a Result containing the events in JSON format or an error status
     */
    @Override
    public Result<String> getEventStore(int offset, int limit, String hostId, String id, String userId, Long nonce, String aggregateId, Long aggregateVersion, String aggregateType, String eventType, String payload, String metaData) {
        return null;
    }

    /**
     * This method is used on the UI for exporting the event store data to a file.
     *
     * @param hostId host identifier, can be null
     * @param id unique identifier for the event, can be null
     * @param userId user identifier, can be null
     * @param nonce user nonce
     * @param aggregateId aggregate identifier, can be null
     * @param aggregateVersion sequence number for optimistic concurrency control
     * @param aggregateType aggregate type, can be null
     * @param eventType event type, can be null
     * @param payload payload of the event, can be null
     * @param metaData metadata of the event, can be null
     * @return a Result containing the events in JSON format or an error status
     */
    @Override
    public Result<String> exportEventStore(String hostId, String id, String userId, Long nonce, String aggregateId, Long aggregateVersion, String aggregateType, String eventType, String payload, String metaData) {
        return null;
    }

    /**
     * Get the max aggregate version from the aggregate id by query the event_store_t.
     *
     * @param aggregateId aggregate id
     * @return A Result of Json object that contains key aggregateVersion and a number value for the max aggregateVersion.
     */
    @Override
    public int getMaxAggregateVersion(String aggregateId) {
        final String sql = "SELECT MAX(aggregate_version) aggregate_version FROM event_store_t WHERE aggregate_id = ?";
        int aggregateVersion = 0;
        try (final Connection conn = ds.getConnection()) {
            try (PreparedStatement statement = conn.prepareStatement(sql)) {
                statement.setObject(1, aggregateId);
                try (ResultSet resultSet = statement.executeQuery()) {
                    if(resultSet.next()) {
                        aggregateVersion = resultSet.getInt("aggregate_version");
                    }
                }
            }
        } catch (SQLException e) {
            logger.error("SQLException:", e);
        } catch (Exception e) {
            logger.error("Exception:", e);
        }
        return aggregateVersion;
    }

    private long reserveOffsets(Connection conn, int batchSize) throws SQLException {
        String sql = "UPDATE log_counter SET next_offset = next_offset + ? WHERE id = 1 RETURNING next_offset - ?";
        try (PreparedStatement pstmt = conn.prepareStatement(sql)) {
            pstmt.setInt(1, batchSize);
            pstmt.setInt(2, batchSize);
            try (ResultSet rs = pstmt.executeQuery()) {
                if (rs.next()) {
                    return rs.getLong(1);
                }
            }
        }
        throw new SQLException("Failed to reserve offsets from log_counter");
    }
}
