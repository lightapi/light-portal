package net.lightapi.portal.db.persistence;


import com.fasterxml.jackson.databind.node.ObjectNode;
import com.networknt.monad.Failure;
import com.networknt.monad.Result;
import com.networknt.monad.Success;
import com.networknt.status.Status;
import com.networknt.utility.Constants;
import io.cloudevents.CloudEvent;
import net.lightapi.portal.PortalConstants;

import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.time.OffsetDateTime;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;

import static com.networknt.config.JsonMapper.objectMapper;
import static com.networknt.db.provider.SqlDbStartupHook.ds;
import static net.lightapi.portal.db.PortalDbProviderImpl.GENERIC_EXCEPTION;
import static net.lightapi.portal.db.PortalDbProviderImpl.SQL_EXCEPTION;

public class EventPersistenceImpl implements EventPersistence {
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
        final String insertEventStoreSql = "INSERT INTO event_store_t " +
                "(id, host_id, user_id, aggregate_id, aggregate_type, event_type, sequence_number, event_ts, payload, metadata) " +
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?::jsonb, ?::jsonb)"; // Use ?::jsonb for JSONB casting

        // SQL for outbox_message_t table
        final String insertOutboxMessageSql = "INSERT INTO outbox_message_t " +
                "(id, host_id, user_id, aggregate_id, aggregate_type, event_type, event_ts, payload, metadata) " +
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?::jsonb, ?::jsonb)"; // Use ?::jsonb for JSONB casting

        if (events == null || events.length == 0) {
            return Success.of("No events to insert.");
        }

        Result<String> result;
        try (Connection conn = ds.getConnection()) {
            conn.setAutoCommit(false); // Start transaction

            try (PreparedStatement eventStorePst = conn.prepareStatement(insertEventStoreSql);
                 PreparedStatement outboxPst = conn.prepareStatement(insertOutboxMessageSql)) {

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

                    // Assuming aggregate_type is also an extension or derived from event.getType()
                    String aggregateType =(String)event.getExtension(PortalConstants.AGGREGATE_TYPE);
                    if (aggregateType == null) {
                        throw new IllegalArgumentException("Could not determine aggregatetype from CloudEvent.");
                    }


                    // Extract payload (data)
                    // CloudEvent.getData() returns Data, need to convert to JSON string
                    String payloadJson = "{}"; // Default empty JSON
                    if (event.getData() != null) {
                        payloadJson = new String(event.getData().toBytes(), StandardCharsets.UTF_8);
                    }

                    // Extract metadata (CloudEvent context attributes + custom extensions)
                    // You might want to include all CloudEvent attributes (id, source, time, etc.) in metadata
                    // or just custom extensions not explicitly mapped to columns.
                    // For simplicity, let's just include all extensions as metadata JSON.
                    Map<String, Object> extensions = event.getExtensionNames().stream()
                            .filter(extName -> !extName.equals(Constants.HOST) && !extName.equals(Constants.USER) && !extName.equals(PortalConstants.AGGREGATE_TYPE))
                            .collect(Collectors.toMap(
                                    extName -> extName,
                                    event::getExtension
                            ));
                    String metadataJson = objectMapper.writeValueAsString(extensions);

                    // Sequence Number: This is the tricky one in a batch of events
                    // The ApplicationService should have already determined the correct sequence number for each event
                    // before calling this insert method. The DomainEvent object (which gets serialized into payload)
                    // should contain the sequence number.
                    // For simplicity here, I'll assume you extract it from the CloudEvent payload,
                    // or it's provided as an extension.
                    // This is CRITICAL for OCC.
                    long sequenceNumber = 0; // Placeholder
                    // You MUST retrieve the sequence number from your event payload or metadata correctly.
                    // E.g., if your payload JSON structure has a "sequenceNumber" field:
                    // JsonNode payloadNode = objectMapper.readTree(payloadJson);
                    // sequenceNumber = payloadNode.has("sequenceNumber") ? payloadNode.get("sequenceNumber").asLong() : 0;
                    // Or if it's a CloudEvent extension:
                    // if (event.getExtension("sequenceNumber") instanceof Long seq) sequenceNumber = seq;


                    // Set parameters for event_store_t
                    eventStorePst.setObject(1, eventId);
                    eventStorePst.setObject(2, hostId);
                    eventStorePst.setObject(3, userId);
                    eventStorePst.setString(4, aggregateId);
                    eventStorePst.setString(5, aggregateType);
                    eventStorePst.setString(6, eventType);
                    eventStorePst.setLong(7, sequenceNumber); // Make sure this is correct for OCC
                    eventStorePst.setObject(8, eventTs); // Use OffsetDateTime for TIMESTAMP WITH TIME ZONE
                    eventStorePst.setString(9, payloadJson);
                    eventStorePst.setString(10, metadataJson);
                    eventStorePst.addBatch();

                    // Set parameters for outbox_message_t
                    outboxPst.setObject(1, eventId);
                    outboxPst.setObject(2, hostId);
                    outboxPst.setObject(3, userId);
                    outboxPst.setString(4, aggregateId);
                    outboxPst.setString(5, aggregateType);
                    outboxPst.setString(6, eventType);
                    outboxPst.setObject(7, eventTs); // Use OffsetDateTime for TIMESTAMP WITH TIME ZONE
                    outboxPst.setString(8, payloadJson);
                    outboxPst.setString(9, metadataJson);
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
                // logger.error("SQLException:", e);
                conn.rollback();
                // notificationService.insertNotification(event, false, e.getMessage()); // This belongs to a consumer of Kafka
                result = Failure.of(new Status(SQL_EXCEPTION, e.getMessage()));
            } catch (Exception e) {
                // Catch other exceptions
                // logger.error("Exception:", e);
                conn.rollback();
                result = Failure.of(new Status(GENERIC_EXCEPTION, e.getMessage()));
            }
        } catch (SQLException e) {
            // Catch connection errors
            // logger.error("SQLException:", e);
            result = Failure.of(new Status(SQL_EXCEPTION, e.getMessage()));
        }
        return result;
    }

}
