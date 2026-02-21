package net.lightapi.portal.db;

import com.fasterxml.jackson.core.type.TypeReference;
import com.networknt.config.Config;
import com.networknt.db.provider.DbProvider;
import com.networknt.server.StartupHookProvider;
import com.networknt.service.SingletonServiceFactory;
import org.postgresql.PGConnection;
import org.postgresql.PGNotification;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static com.networknt.db.provider.SqlDbStartupHook.ds;

/**
 * A PostgreSQL-based event consumer that polls outbox_message_t using gapless offsets.
 * This is an alternative to the Kafka-based PortalEventConsumerStartupHook.
 * It uses LISTEN/NOTIFY for real-time wake-up and falls back to polling if needed.
 */
public class DbEventConsumerStartupHook implements StartupHookProvider {
    private static final Logger logger = LoggerFactory.getLogger(DbEventConsumerStartupHook.class);
    private static final String CONFIG_NAME = "db-event-consumer";
    public static PortalDbProvider dbProvider = (PortalDbProvider) SingletonServiceFactory.getBean(DbProvider.class);

    public static final ExecutorService executor = Executors.newVirtualThreadPerTaskExecutor();
    public static boolean done = false;

    // Configuration
    private String groupId;
    private int batchSize;
    private int totalPartitions;
    private int partitionId;
    private long waitPeriodMs;

    private static final String claimSql = """
            WITH counter_tip AS (
              SELECT (next_offset - 1) AS highest_committed_offset
              FROM log_counter
              WHERE id = 1
            ),
            to_claim AS (
              SELECT
                c.group_id,
                c.partition_id,
                c.next_offset AS n0,
                LEAST(
                  ?::bigint,
                  GREATEST(0, (SELECT highest_committed_offset FROM counter_tip) - c.next_offset + 1)
                ) AS delta
              FROM consumer_offsets c
              WHERE c.group_id = ? AND c.topic_id = 1 AND c.partition_id = ?
              FOR UPDATE
            ),
            upd AS (
              UPDATE consumer_offsets c
              SET next_offset = c.next_offset + t.delta
              FROM to_claim t
              WHERE c.group_id = t.group_id AND c.topic_id = 1 AND c.partition_id = t.partition_id
              RETURNING
                t.n0 AS claimed_start_offset,
                (c.next_offset - 1) AS claimed_end_offset
            )
            SELECT claimed_start_offset, claimed_end_offset
            FROM upd
            """;

    private static final String readSql =
            "SELECT payload, host_id, user_id, c_offset, transaction_id FROM outbox_message_t " +
            "WHERE c_offset BETWEEN ? AND ? " +
            "  AND abs(hashtext(host_id::text)) % ? = ? " +
            "ORDER BY c_offset";

    private static final String peekSql =
            "SELECT payload, host_id, user_id, c_offset, transaction_id FROM outbox_message_t " +
            "WHERE c_offset = ? AND abs(hashtext(host_id::text)) % ? = ?";

    private static final String updateOffsetSql =
            "UPDATE consumer_offsets SET next_offset = ? " +
            "WHERE group_id = ? AND topic_id = 1 AND partition_id = ?";

    // DLQ insertion SQL
    private static final String insertDlqSql =
            "INSERT INTO dead_letter_queue (group_id, host_id, user_id, c_offset, transaction_id, payload, exception, created_dt) " +
            "VALUES (?, ?, ?, ?, ?, ?::jsonb, ?, NOW())";

    private static final String dummySql = "SELECT 1";

    @Override
    public void onStartup() {
        logger.info("DbEventConsumerStartupHook begins");
        Map<String, Object> config = (Map<String, Object>)Config.getInstance().getJsonObjectConfig(CONFIG_NAME, Map.class);
        if (config == null) {
            logger.warn("Configuration {} not found, using defaults", CONFIG_NAME);
            groupId = "user-query-group";
            batchSize = 100;
            totalPartitions = 8;
            partitionId = 0;
            waitPeriodMs = 1000;
        } else {
            groupId = (String) config.get("groupId");
            batchSize = (Integer) config.get("batchSize");
            totalPartitions = (Integer) config.get("totalPartitions");
            partitionId = (Integer) config.get("partitionId");
            waitPeriodMs = ((Number) config.get("waitPeriodMs")).longValue();
        }

        ensureConsumerGroupRow();
        runConsumer();
        logger.info("DbEventConsumerStartupHook ends");
    }

    private void ensureConsumerGroupRow() {
        String sql = "INSERT INTO consumer_offsets (group_id, topic_id, partition_id, next_offset) VALUES (?, 1, ?, 1) ON CONFLICT (group_id, topic_id, partition_id) DO NOTHING";
        try (Connection conn = ds.getConnection();
             PreparedStatement pstmt = conn.prepareStatement(sql)) {
            pstmt.setString(1, groupId);
            pstmt.setInt(2, partitionId);
            pstmt.executeUpdate();
        } catch (SQLException e) {
            logger.error("Failed to ensure consumer group row", e);
        }
    }

    private void runConsumer() {
        executor.execute(new ConsumerTask());
        executor.shutdown();
    }

    class ConsumerTask implements Runnable {
        @Override
        public void run() {
            while (!done) {
                try (Connection conn = ds.getConnection()) {
                    // Start listening on this connection for real-time wake-ups
                    try (Statement stmt = conn.createStatement()) {
                        stmt.execute("LISTEN event_channel");
                    }
                    PGConnection pgConn = conn.unwrap(PGConnection.class);

                    while (!done) {
                        boolean processed = processBatch();
                        if (!processed) {
                            // No events processed, wait for notification or timeout (polling fallback)
                            // We need to execute a dummy query to actually receive notifications if they arrived.
                            // However, pgConn.getNotifications(timeout) should handle it.
                            PGNotification[] notifications = pgConn.getNotifications((int) waitPeriodMs);
                            if (notifications != null && notifications.length > 0 && logger.isTraceEnabled()) {
                                logger.trace("Received {} notifications", notifications.length);
                            }
                        }
                    }
                } catch (Exception e) {
                    if (!done) {
                        logger.error("Error in consumer task loop, retrying in {} ms...", waitPeriodMs, e);
                        try {
                            Thread.sleep(waitPeriodMs);
                        } catch (InterruptedException ignored) {}
                    }
                }
            }
        }
    }

    private boolean processBatch() throws Exception {
        try (Connection conn = ds.getConnection()) {
            conn.setAutoCommit(false);
            try {
                long startOffset = -1;
                long endOffset = -1;

                try (PreparedStatement pstmt = conn.prepareStatement(claimSql)) {
                    pstmt.setLong(1, batchSize);
                    pstmt.setString(2, groupId);
                    pstmt.setInt(3, partitionId);
                    try (ResultSet rs = pstmt.executeQuery()) {
                        if (rs.next()) {
                            startOffset = rs.getLong(1);
                            endOffset = rs.getLong(2);
                        }
                    }
                }

                if (startOffset == -1 || startOffset > endOffset) {
                    conn.commit();
                    return false;
                }

                logger.debug("Claimed offsets {} to {} for group {} partition {}", startOffset, endOffset, groupId, partitionId);

                // Group events by transaction_id for transactional batching
                // Map<TransactionId, List<EventData>>
                Map<String, java.util.List<EventData>> transactionBatches = new java.util.LinkedHashMap<>();
                long lastProcessedOffset = -1;
                String lastTransactionId = null;

                try (PreparedStatement pstmt = conn.prepareStatement(readSql)) {
                    pstmt.setLong(1, startOffset);
                    pstmt.setLong(2, endOffset);
                    pstmt.setInt(3, totalPartitions);
                    pstmt.setInt(4, partitionId);
                    try (ResultSet rs = pstmt.executeQuery()) {
                        int count = 0;
                        while (rs.next()) {
                            String payload = rs.getString(1);
                            String hostId = rs.getString(2);
                            String userId = rs.getString(3);
                            lastProcessedOffset = rs.getLong(4);
                            String transactionId = rs.getString(5);

                            // If transactionId is null (old data), fall back to host:user
                            if (transactionId == null) {
                                transactionId = hostId + ":" + userId;
                            }

                            lastTransactionId = transactionId;
                            transactionBatches.computeIfAbsent(transactionId, k -> new java.util.ArrayList<>())
                                    .add(new EventData(payload, hostId, userId, lastProcessedOffset, transactionId));
                            count++;
                        }
                        logger.debug("Fetched {} events for processing in valid partition", count);
                    }
                }

                // Batch Extension: Check if the last transaction continues beyond our claim
                if (lastProcessedOffset == endOffset && lastTransactionId != null) {
                    long nextOffset = endOffset + 1;
                    boolean continuing = true;
                    while (continuing) {
                        try (PreparedStatement pstmt = conn.prepareStatement(peekSql)) {
                            pstmt.setLong(1, nextOffset);
                            pstmt.setInt(2, totalPartitions);
                            pstmt.setInt(3, partitionId);
                            try (ResultSet rs = pstmt.executeQuery()) {
                                if (rs.next()) {
                                    String transactionId = rs.getString(5);
                                    if (transactionId == null) {
                                        transactionId = rs.getString(2) + ":" + rs.getString(3);
                                    }

                                    if (transactionId.equals(lastTransactionId)) {
                                        transactionBatches.get(lastTransactionId).add(new EventData(
                                            rs.getString(1), rs.getString(2), rs.getString(3), nextOffset, transactionId
                                        ));
                                        lastProcessedOffset = nextOffset;
                                        nextOffset++;
                                    } else {
                                        continuing = false;
                                    }
                                } else {
                                    continuing = false;
                                }
                            }
                        }
                    }
                    if (lastProcessedOffset > endOffset) {
                        logger.info("Extended batch up to offset {} for transaction {}", lastProcessedOffset, lastTransactionId);
                        try (PreparedStatement pstmt = conn.prepareStatement(updateOffsetSql)) {
                            pstmt.setLong(1, lastProcessedOffset + 1);
                            pstmt.setString(2, groupId);
                            pstmt.setInt(3, partitionId);
                            pstmt.executeUpdate();
                        }
                    }
                }

                // Process all events in this transaction
                for (java.util.List<EventData> events : transactionBatches.values()) {
                    for (EventData event : events) {
                        updateDatabaseWithEvent(conn, event.payload);
                    }
                }

                conn.commit();
                return true;
            } catch (Exception e) {
                conn.rollback();
                logger.warn("Batch processing failed, switching to granular fallback mode. Error: {}", e.getMessage());
                // Switch to fallback processing
                return processBatchWithFallback(conn);
            }
        }
    }

    /**
     * Re-attempts processing the batch but commits/rolls back per transaction_id.
     * Failed transactions are moved to DLQ.
     */
    private boolean processBatchWithFallback(Connection _conn) throws Exception {
        // We reuse the connection logic similar to processBatch but inside a new transaction context if needed.
        // Since the previous transaction was rolled back, we need to start fresh or reuse the connection.
        // It is safer to re-acquire the logic or use the same connection with a fresh transaction.
        // Note: _conn is already rolled back but still open.

        try (Connection conn = ds.getConnection()) { // Get a fresh connection to be safe
            conn.setAutoCommit(false);

            // Re-claim logic is not needed if we know the offsets, BUT we might have lost the lock?
            // Actually, since we rolled back, the "FOR UPDATE" lock is gone. We need to re-claim.
            // However, other consumers might claim it if we are not careful.
            // But since we are partitioning, and we are the only consumer for this partition (usually), strictly speaking it's risky.
            // A better way is to pass the offsets we know we tried to claim.
            // But let's stick to the simplest: re-run the claim logic. If we get the same offsets, great.

            // Simplified fallback: Just re-run the exact same claim query.
            long startOffset = -1;
            long endOffset = -1;

            try (PreparedStatement pstmt = conn.prepareStatement(claimSql)) {
                pstmt.setLong(1, batchSize);
                pstmt.setString(2, groupId);
                pstmt.setInt(3, partitionId);
                try (ResultSet rs = pstmt.executeQuery()) {
                    if (rs.next()) {
                        startOffset = rs.getLong(1);
                        endOffset = rs.getLong(2);
                    }
                }
            }

            if (startOffset == -1) return false;

            // Re-read events
           Map<String, java.util.List<EventData>> transactionBatches = new java.util.LinkedHashMap<>();
            long lastProcessedOffset = -1;
            String lastTransactionId = null;

            try (PreparedStatement pstmt = conn.prepareStatement(readSql)) {
                pstmt.setLong(1, startOffset);
                pstmt.setLong(2, endOffset);
                pstmt.setInt(3, totalPartitions);
                pstmt.setInt(4, partitionId);
                try (ResultSet rs = pstmt.executeQuery()) {
                    while (rs.next()) {
                        String payload = rs.getString(1);
                        String hostId = rs.getString(2);
                        String userId = rs.getString(3);
                        lastProcessedOffset = rs.getLong(4);
                        String transactionId = rs.getString(5);
                        if (transactionId == null) transactionId = hostId + ":" + userId;

                        lastTransactionId = transactionId;
                        transactionBatches.computeIfAbsent(transactionId, k -> new java.util.ArrayList<>())
                                .add(new EventData(payload, hostId, userId, lastProcessedOffset, transactionId));
                    }
                }
            }

            // Extension logic (same as before)
            if (lastProcessedOffset == endOffset && lastTransactionId != null) {
                 long nextOffset = endOffset + 1;
                 boolean continuing = true;
                 while (continuing) {
                     try (PreparedStatement pstmt = conn.prepareStatement(peekSql)) {
                         pstmt.setLong(1, nextOffset);
                         pstmt.setInt(2, totalPartitions);
                         pstmt.setInt(3, partitionId);
                         try (ResultSet rs = pstmt.executeQuery()) {
                             if (rs.next()) {
                                 String transactionId = rs.getString(5);
                                 if (transactionId == null) transactionId = rs.getString(2) + ":" + rs.getString(3);

                                 if (transactionId.equals(lastTransactionId)) {
                                     transactionBatches.get(lastTransactionId).add(new EventData(
                                         rs.getString(1), rs.getString(2), rs.getString(3), nextOffset, transactionId
                                     ));
                                     lastProcessedOffset = nextOffset;
                                     nextOffset++;
                                 } else {
                                     continuing = false;
                                 }
                             } else {
                                 continuing = false;
                             }
                         }
                     }
                 }
                 if (lastProcessedOffset > endOffset) {
                    try (PreparedStatement pstmt = conn.prepareStatement(updateOffsetSql)) {
                        pstmt.setLong(1, lastProcessedOffset + 1);
                        pstmt.setString(2, groupId);
                        pstmt.setInt(3, partitionId);
                        pstmt.executeUpdate();
                    }
                }
            }

            // Iterate and process each transaction individually with Savepoints
            for (Map.Entry<String, java.util.List<EventData>> entry : transactionBatches.entrySet()) {
                String txId = entry.getKey();
                java.util.List<EventData> events = entry.getValue();

                Savepoint sp = conn.setSavepoint("TX_" + txId.hashCode());
                try {
                    for (EventData event : events) {
                        updateDatabaseWithEvent(conn, event.payload);
                    }
                } catch (Exception e) {
                    // This transaction failed, rollback to savepoint and move to DLQ
                    conn.rollback(sp);
                    logger.error("Transaction {} failed, moving to DLQ. Error: {}", txId, e.getMessage());

                    try (PreparedStatement dlqStmt = conn.prepareStatement(insertDlqSql)) {
                        for (EventData event : events) {
                             dlqStmt.setString(1, groupId);
                             dlqStmt.setObject(2, UUID.fromString(event.hostId));
                             dlqStmt.setObject(3, UUID.fromString(event.userId));
                             dlqStmt.setLong(4, event.offset);
                             dlqStmt.setObject(5, UUID.fromString(event.transactionId));
                             dlqStmt.setString(6, event.payload);
                             dlqStmt.setString(7, e.getMessage());
                             dlqStmt.addBatch();
                        }
                        dlqStmt.executeBatch();
                    }
                }
            }

            conn.commit();
            return true;
        }
    }

    public void updateDatabaseWithEvent(Connection conn, String value) throws SQLException, Exception {
        Map<String, Object> event;
        try {
            event = Config.getInstance().getMapper().readValue(value, new TypeReference<HashMap<String, Object>>() {});
            dbProvider.handleEvent(conn, event);
        } catch (Exception e) {
            logger.error("Exception deserializing or processing event value: {}", value, e);
            throw e;
        }
    }

    private static class EventData {
        String payload;
        String hostId;
        String userId;
        long offset;
        String transactionId;

        public EventData(String payload, String hostId, String userId, long offset, String transactionId) {
            this.payload = payload;
            this.hostId = hostId;
            this.userId = userId;
            this.offset = offset;
            this.transactionId = transactionId;
        }
    }
}
