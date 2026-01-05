package net.lightapi.portal.db.persistence;

import com.networknt.config.JsonMapper;
import com.networknt.db.provider.DbProvider;
import com.networknt.monad.Failure;
import com.networknt.monad.Result;
import com.networknt.monad.Success;
import com.networknt.service.SingletonServiceFactory;
import com.networknt.status.Status;
import com.networknt.utility.Constants;
import com.networknt.utility.TimeUtil;
import com.networknt.utility.UuidUtil;
import io.cloudevents.core.v1.CloudEventV1;
import net.lightapi.portal.EventTypeUtil;
import net.lightapi.portal.PortalConstants;
import net.lightapi.portal.db.ConcurrencyException;
import net.lightapi.portal.db.PortalDbProvider;
import net.lightapi.portal.db.util.NotificationService;
import net.lightapi.portal.db.util.SqlUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.*;
import java.util.concurrent.TimeUnit;

import static com.networknt.db.provider.SqlDbStartupHook.ds;
import static net.lightapi.portal.db.PortalDbProviderImpl.GENERIC_EXCEPTION;
import static net.lightapi.portal.db.PortalDbProviderImpl.SQL_EXCEPTION;
import static net.lightapi.portal.db.util.SqlUtil.*;

public class SchedulePersistenceImpl implements SchedulePersistence {

    private static final Logger logger = LoggerFactory.getLogger(SchedulePersistenceImpl.class);
    // Consider moving these to a shared constants class if they are truly general
    private static final String SQL_EXCEPTION = PortalDbProvider.SQL_EXCEPTION;
    private static final String GENERIC_EXCEPTION = PortalDbProvider.GENERIC_EXCEPTION;
    private static final String OBJECT_NOT_FOUND = PortalDbProvider.OBJECT_NOT_FOUND;

    private final NotificationService notificationService;
    public PortalDbProvider dbProvider = (PortalDbProvider) SingletonServiceFactory.getBean(DbProvider.class);

    public SchedulePersistenceImpl(NotificationService notificationService) {
        this.notificationService = notificationService;
    }

    @Override
    public void createSchedule(Connection conn, Map<String, Object> event) throws SQLException, Exception {
        // Use UPSERT based on the Primary Key (schedule_id): INSERT ON CONFLICT DO UPDATE
        // This handles:
        // 1. First time insert (no conflict).
        // 2. Re-creation (conflict on schedule_id) -> UPDATE the existing soft-deleted row (setting active=TRUE and new version).

        final String sql =
                """
                INSERT INTO schedule_t(
                    schedule_id,
                    host_id,
                    schedule_name,
                    frequency_unit,
                    frequency_time,
                    start_ts,
                    next_run_ts,
                    event_topic,
                    event_type,
                    event_data,
                    update_user,
                    update_ts,
                    aggregate_version,
                    active
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, TRUE)
                ON CONFLICT (schedule_id) DO UPDATE
                SET host_id = EXCLUDED.host_id,
                    schedule_name = EXCLUDED.schedule_name,
                    frequency_unit = EXCLUDED.frequency_unit,
                    frequency_time = EXCLUDED.frequency_time,
                    start_ts = EXCLUDED.start_ts,
                    next_run_ts = EXCLUDED.next_run_ts,
                    event_topic = EXCLUDED.event_topic,
                    event_type = EXCLUDED.event_type,
                    event_data = EXCLUDED.event_data,
                    update_user = EXCLUDED.update_user,
                    update_ts = EXCLUDED.update_ts,
                    aggregate_version = EXCLUDED.aggregate_version,
                    active = TRUE
                -- OCC/IDM: Only update if the incoming event is newer
                WHERE schedule_t.aggregate_version < EXCLUDED.aggregate_version
                AND schedule_t.active = FALSE
                """;

        // Note: The original code uses a non-standard map retrieval: Map<String, Object> map = (Map<String, Object>)event.get(PortalConstants.DATA);
        // Assuming SqlUtil.extractEventData(event) is the correct utility based on other methods.
        Map<String, Object> map = SqlUtil.extractEventData(event);
        String scheduleId = (String) map.get("scheduleId");
        long newAggregateVersion = SqlUtil.getNewAggregateVersion(event);

        try (PreparedStatement statement = conn.prepareStatement(sql)) {
            // INSERT values (12 placeholders + active=TRUE in SQL, total 12 dynamic values)
            int i = 1;

            // 1: schedule_id (Required)
            statement.setObject(i++, UUID.fromString(scheduleId));
            // 2: host_id (Required - from event data)
            statement.setObject(i++, UUID.fromString((String)map.get("hostId")));
            // 3. schedule_name (Required)
            statement.setString(i++, (String)map.get("scheduleName"));
            // 4. frequency_unit (Required)
            statement.setString(i++, (String)map.get("frequencyUnit"));
            // 5. frequency_time (Required, Integer)
            statement.setInt(i++, ((Number) map.get("frequencyTime")).intValue());

            // 6. start_ts (Required)
            Object startTsObj = map.get("startTs");
            OffsetDateTime startTs = startTsObj instanceof String ? OffsetDateTime.parse((String)startTsObj) : (OffsetDateTime)startTsObj;
            statement.setObject(i++, startTs);
            // 7. next_run_ts (Required - initially same as start_ts)
            statement.setObject(i++, startTs);

            // 8. event_topic (Required)
            statement.setString(i++, (String)map.get("eventTopic"));
            // 9. event_type (Required)
            statement.setString(i++, (String)map.get("eventType"));
            // 10. event_data (Required, TEXT - assuming JSON stored as string)
            statement.setString(i++, (String)map.get("eventData"));

            // 11. update_user (From event metadata)
            statement.setString(i++, (String)event.get(Constants.USER));
            // 12. update_ts (From event metadata)
            Object updateTsObj = event.get(CloudEventV1.TIME);
            statement.setObject(i++, updateTsObj instanceof String ? OffsetDateTime.parse((String)updateTsObj) : (OffsetDateTime)updateTsObj);
            // 13. aggregate_version
            statement.setLong(i++, newAggregateVersion);

            // Execute insert
            int count = statement.executeUpdate();
            if (count == 0) {
                // count=0 means the ON CONFLICT clause was hit, BUT the WHERE clause (aggregate_version < EXCLUDED.aggregate_version) failed.
                // This is the desired idempotent/out-of-order protection behavior. Log and ignore.
                logger.warn("Creation/Reactivation skipped for scheduleId {} aggregateVersion {}. A newer or same version already exists.", scheduleId, newAggregateVersion);
            }
        } catch (SQLException e) {
            logger.error("SQLException during createSchedule for scheduleId {} aggregateVersion {}: {}", scheduleId, newAggregateVersion, e.getMessage(), e);
            throw e;
        } catch (Exception e) {
            logger.error("Exception during createSchedule for scheduleId {} aggregateVersion {}: {}", scheduleId, newAggregateVersion, e.getMessage(), e);
            throw e;
        }
    }

    @Override
    public void updateSchedule(Connection conn, Map<String, Object> event) throws SQLException, Exception {
        // We attempt to update the record IF the incoming event's aggregate_version is greater than the current projection's version.
        // This enforces Idempotence (IDM) and Optimistic Concurrency Control (OCC) by ensuring version monotonicity.
        // We explicitly set active = TRUE as an UPDATE event implies the schedule should be active.
        final String sql =
                """
                UPDATE schedule_t
                SET schedule_name = ?,
                    frequency_unit = ?,
                    frequency_time = ?,
                    start_ts = ?,
                    next_run_ts = ?,
                    event_topic = ?,
                    event_type = ?,
                    event_data = ?,
                    update_user = ?,
                    update_ts = ?,
                    aggregate_version = ?,
                    active = TRUE
                WHERE schedule_id = ?
                  AND aggregate_version < ?
                """; // <<< CRITICAL: Changed aggregate_version = ? to aggregate_version < ? to enforce monotonicity (OCC/IDM)

        // Note: The original code uses a non-standard map retrieval: Map<String, Object> map = (Map<String, Object>)event.get(PortalConstants.DATA);
        // Assuming SqlUtil.extractEventData(event) is the correct utility based on other methods.
        Map<String, Object> map = SqlUtil.extractEventData(event);
        String scheduleId = (String) map.get("scheduleId");
        long newAggregateVersion = SqlUtil.getNewAggregateVersion(event);

        try (PreparedStatement statement = conn.prepareStatement(sql)) {
            // SET values (10 dynamic values + active = TRUE in SQL)
            int i = 1;
            // 1: schedule_name
            statement.setString(i++, (String)map.get("scheduleName"));
            // 2: frequency_unit
            statement.setString(i++, (String)map.get("frequencyUnit"));
            // 3: frequency_time
            statement.setInt(i++, ((Number) map.get("frequencyTime")).intValue());
            // 4: start_ts
            OffsetDateTime startTs = OffsetDateTime.parse((String)map.get("startTs"));
            statement.setObject(i++, startTs);
            // 5: next_run_ts
            statement.setObject(i++, startTs);
            // 6: event_topic
            statement.setString(i++, (String)map.get("eventTopic"));
            // 7: event_type
            statement.setString(i++, (String)map.get("eventType"));
            // 8: event_data
            statement.setString(i++, (String)map.get("eventData"));
            // 9: update_user
            statement.setString(i++, (String)event.get(Constants.USER));
            // 10: update_ts
            Object updateTsObj = event.get(CloudEventV1.TIME);
            statement.setObject(i++, updateTsObj instanceof String ? OffsetDateTime.parse((String)updateTsObj) : (OffsetDateTime)updateTsObj);
            // 11: aggregate_version
            statement.setLong(i++, newAggregateVersion);

            // WHERE conditions (2 placeholders)
            // 12: schedule_id
            statement.setObject(i++, UUID.fromString(scheduleId));
            // 13: aggregate_version < ? (new version for OCC/IDM check)
            statement.setLong(i++, newAggregateVersion);

            // Execute update
            int count = statement.executeUpdate();
            if (count == 0) {
                // If 0 rows updated, it means the record was either not found
                // OR aggregate_version >= newAggregateVersion (OCC/IDM check failed).
                // We IGNORE the failure and log a warning, as this is the desired idempotent/monotonic behavior.
                logger.warn("Update skipped for scheduleId {} aggregateVersion {}. Record not found or a newer/same version already exists.", scheduleId, newAggregateVersion);
            }
        } catch (SQLException e) {
            logger.error("SQLException during updateSchedule for scheduleId {} aggregateVersion {}: {}", scheduleId, newAggregateVersion, e.getMessage(), e);
            throw e;
        } catch (Exception e) {
            logger.error("Exception during updateSchedule for scheduleId {} aggregateVersion {}: {}", scheduleId, newAggregateVersion, e.getMessage(), e);
            throw e;
        }
    }

    @Override
    public void deleteSchedule(Connection conn, Map<String, Object> event) throws SQLException, Exception {
        // Use UPDATE to implement Soft Delete (setting active = FALSE).
        // OCC/IDM is enforced by checking aggregate_version < newAggregateVersion.
        final String sql =
                """
                UPDATE schedule_t
                SET active = FALSE,
                    update_user = ?,
                    update_ts = ?,
                    aggregate_version = ?
                WHERE schedule_id = ?
                  AND aggregate_version < ?
                """; // <<< CRITICAL: Added aggregate_version < ? to enforce monotonicity (OCC/IDM)

        // Note: The original code uses a non-standard map retrieval: Map<String, Object> map = (Map<String, Object>)event.get(PortalConstants.DATA);
        // Assuming SqlUtil.extractEventData(event) is the correct utility based on other methods.
        Map<String, Object> map = SqlUtil.extractEventData(event);
        String scheduleId = (String) map.get("scheduleId");
        // A delete event represents a state change, so it should have a new, incremented version.
        long newAggregateVersion = SqlUtil.getNewAggregateVersion(event);

        try (PreparedStatement statement = conn.prepareStatement(sql)) {
            // SET values (3 placeholders)
            // 1: update_user
            statement.setString(1, (String)event.get(Constants.USER));
            // 2: update_ts
            statement.setObject(2, OffsetDateTime.parse((String)event.get(CloudEventV1.TIME)));
            // 3: aggregate_version (the new version)
            statement.setLong(3, newAggregateVersion);

            // WHERE conditions (2 placeholders)
            // 4: schedule_id
            statement.setObject(4, UUID.fromString(scheduleId));
            // 5: aggregate_version < ? (new version for OCC/IDM check)
            statement.setLong(5, newAggregateVersion);

            int count = statement.executeUpdate();
            if (count == 0) {
                // If 0 rows updated, it means:
                // 1. The record was not found (already deleted or never existed).
                // 2. The OCC/IDM check failed (aggregate_version >= newAggregateVersion).
                // We IGNORE the failure and log a warning, as this is the desired idempotent/monotonic behavior.
                logger.warn("Soft delete skipped for scheduleId {} aggregateVersion {}. Record not found or a newer/same version already exists.", scheduleId, newAggregateVersion);
            }
        } catch (SQLException e) {
            logger.error("SQLException during deleteSchedule for scheduleId {} aggregateVersion {}: {}", scheduleId, newAggregateVersion, e.getMessage(), e);
            throw e;
        } catch (Exception e) {
            logger.error("Exception during deleteSchedule for scheduleId {} aggregateVersion {}: {}", scheduleId, newAggregateVersion, e.getMessage(), e);
            throw e;
        }
    }

    @Override
    public Result<String> getSchedule(int offset, int limit, String filtersJson, String globalFilter, String sortingJson, boolean active, String hostId) {
        Result<String> result = null;
        List<Map<String, Object>> filters = parseJsonList(filtersJson);
        List<Map<String, Object>> sorting = parseJsonList(sortingJson);

        String s =
            """
            SELECT COUNT(*) OVER () AS total,
            schedule_id, host_id, schedule_name, frequency_unit, frequency_time,
            start_ts, next_run_ts, event_topic, event_type, event_data, update_user,
            update_ts, aggregate_version, active
            FROM schedule_t
            WHERE host_id = ?
            """;

        List<Object> parameters = new ArrayList<>();
        parameters.add(UUID.fromString(hostId));

        String activeClause = SqlUtil.buildMultiTableActiveClause(active);
        String[] searchColumns = {"schedule_name"};
        String sqlBuilder = s + activeClause +
                dynamicFilter(Arrays.asList("schedule_id", "host_id"), Arrays.asList(searchColumns), filters, null, parameters) +
                globalFilter(globalFilter, searchColumns, parameters) +
                dynamicSorting("schedule_name", sorting, null) +
                "\nLIMIT ? OFFSET ?";

        parameters.add(limit);
        parameters.add(offset);
        int total = 0; // Variable to store total count
        List<Map<String, Object>> schedules = new ArrayList<>(); // List to hold results

        try (final Connection conn = ds.getConnection();
            PreparedStatement preparedStatement = conn.prepareStatement(sqlBuilder)) {
            populateParameters(preparedStatement, parameters);
            boolean isFirstRow = true; // Flag to get total count only once
            try (ResultSet resultSet = preparedStatement.executeQuery()) {
                // Process the results
                while (resultSet.next()) {
                    Map<String, Object> map = new HashMap<>();
                    if (isFirstRow) {
                        total = resultSet.getInt("total"); // Get total count
                        isFirstRow = false;
                    }
                    // Populate map with data for the current row
                    map.put("scheduleId", resultSet.getObject("schedule_id", UUID.class));
                    map.put("hostId", resultSet.getObject("host_id", UUID.class));
                    map.put("scheduleName", resultSet.getString("schedule_name"));
                    map.put("frequencyUnit", resultSet.getString("frequency_unit"));
                    map.put("frequencyTime", resultSet.getInt("frequency_time"));
                    map.put("startTs", resultSet.getObject("start_ts", OffsetDateTime.class));
                    map.put("nextRunTs", resultSet.getObject("next_run_ts", OffsetDateTime.class));
                    map.put("eventTopic", resultSet.getString("event_topic"));
                    map.put("eventType", resultSet.getString("event_type"));
                    map.put("eventData", resultSet.getString("event_data")); // Assuming TEXT -> String
                    map.put("updateUser", resultSet.getString("update_user"));
                    map.put("updateTs", resultSet.getObject("update_ts") != null ? resultSet.getObject("update_ts", OffsetDateTime.class) : null);
                    map.put("aggregateVersion", resultSet.getLong("aggregate_version"));
                    map.put("active", resultSet.getBoolean("active"));
                    schedules.add(map);
                }
            }
            Map<String, Object> resultMap = new HashMap<>();
            resultMap.put("total", total);
            resultMap.put("schedules", schedules); // Use a descriptive key
            result = Success.of(JsonMapper.toJson(resultMap)); // Serialize and return Success

        } catch (SQLException e) {
            logger.error("SQLException getting schedules:", e);
            result = Failure.of(new Status(SQL_EXCEPTION, e.getMessage()));
        } catch (Exception e) { // Catch other potential runtime exceptions
            logger.error("Unexpected exception getting schedules:", e);
            result = Failure.of(new Status(GENERIC_EXCEPTION, e.getMessage()));
        }
        return result;
    }

    @Override
    public Result<String> getScheduleLabel(String hostId) {
        Result<String> result = null;
        // Select only the ID and name columns needed for labels, filter by host_id
        final String sql = "SELECT schedule_id, schedule_name FROM schedule_t WHERE host_id = ? AND active = TRUE ORDER BY schedule_name";
        List<Map<String, Object>> labels = new ArrayList<>(); // Initialize list for labels

        try (Connection connection = ds.getConnection();
             PreparedStatement preparedStatement = connection.prepareStatement(sql)) {

            preparedStatement.setObject(1, UUID.fromString(hostId)); // Set the hostId parameter

            try (ResultSet resultSet = preparedStatement.executeQuery()) {
                // Iterate through results and build the label map list
                while (resultSet.next()) {
                    Map<String, Object> map = new HashMap<>();
                    map.put("id", resultSet.getString("schedule_id"));    // Key "id"
                    map.put("label", resultSet.getString("schedule_name")); // Key "label"
                    labels.add(map);
                }
            }
            // Serialize the list of labels to JSON and return Success
            result = Success.of(JsonMapper.toJson(labels));

        } catch (SQLException e) {
            logger.error("SQLException getting schedule labels for hostId {}:", hostId, e);
            result = Failure.of(new Status(SQL_EXCEPTION, e.getMessage()));
        } catch (Exception e) { // Catch other potential runtime exceptions
            logger.error("Unexpected exception getting schedule labels for hostId {}:", hostId, e);
            result = Failure.of(new Status(GENERIC_EXCEPTION, e.getMessage()));
        }
        return result;
    }

    @Override
    public Result<String> getScheduleById(String scheduleId) {
        Result<String> result = null;
        // Select all columns from schedule_t for the given schedule_id
        final String sql =
                """
                SELECT schedule_id, host_id, schedule_name, frequency_unit, frequency_time,
                start_ts, next_run_ts, event_topic, event_type, event_data, update_user, update_ts, aggregate_version
                FROM schedule_t WHERE schedule_id = ?
                """;
        Map<String, Object> scheduleMap = null; // Initialize map to null

        try (Connection conn = ds.getConnection()) {
            // No setAutoCommit(false) needed for SELECT
            try (PreparedStatement statement = conn.prepareStatement(sql)) {
                statement.setObject(1, UUID.fromString(scheduleId));

                try (ResultSet resultSet = statement.executeQuery()) {
                    // Check if a row was found
                    if (resultSet.next()) {
                        scheduleMap = new HashMap<>(); // Create map only if found
                        scheduleMap.put("scheduleId", resultSet.getObject("schedule_id", UUID.class));
                        scheduleMap.put("hostId", resultSet.getObject("host_id", UUID.class));
                        scheduleMap.put("scheduleName", resultSet.getString("schedule_name"));
                        scheduleMap.put("frequencyUnit", resultSet.getString("frequency_unit"));
                        scheduleMap.put("frequencyTime", resultSet.getInt("frequency_time"));
                        scheduleMap.put("startTs", resultSet.getTimestamp("start_ts") != null ? resultSet.getTimestamp("start_ts").toInstant().atOffset(ZoneOffset.UTC).toString() : null);
                        scheduleMap.put("next_run_ts", resultSet.getTimestamp("next_run_ts") != null ? resultSet.getTimestamp("next_run_ts").toInstant().atOffset(ZoneOffset.UTC).toString() : null);
                        scheduleMap.put("eventTopic", resultSet.getString("event_topic"));
                        scheduleMap.put("eventType", resultSet.getString("event_type"));
                        scheduleMap.put("eventData", resultSet.getString("event_data")); // Assuming TEXT -> String
                        scheduleMap.put("updateUser", resultSet.getString("update_user"));
                        scheduleMap.put("updateTs", resultSet.getTimestamp("update_ts") != null ? resultSet.getTimestamp("update_ts").toInstant().atOffset(ZoneOffset.UTC).toString() : null);
                        scheduleMap.put("aggregateVersion", resultSet.getLong("aggregate_version"));
                    }
                }
                if (scheduleMap != null) {
                    result = Success.of(JsonMapper.toJson(scheduleMap));
                } else {
                    result = Failure.of(new Status(OBJECT_NOT_FOUND, "schedule", scheduleId)); // Changed to Failure consistent with AccessControlPersistence
                }
            }
        } catch (SQLException e) {
            logger.error("SQLException getting schedule by id {}:", scheduleId, e);
            result = Failure.of(new Status(SQL_EXCEPTION, e.getMessage()));
        } catch (Exception e) { // Catch other potential runtime exceptions
            logger.error("Unexpected exception getting schedule by id {}:", scheduleId, e);
            result = Failure.of(new Status(GENERIC_EXCEPTION, e.getMessage()));
        }
        return result;
    }
    @Override
    public int acquireLock(String instanceId, int lockId, OffsetDateTime lockTimeout) throws Exception {
        // Try to update the lock if it's expired or held by us (from a previous run/crash)
        String sql =
            """
            UPDATE scheduler_lock_t SET instance_id = ?, last_heartbeat = ?
            WHERE lock_id = ? AND (instance_id = ? OR last_heartbeat < ?)
            """;
        int updated = 0;
        try (Connection conn = ds.getConnection(); PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setString(1, instanceId);
            ps.setTimestamp(2, Timestamp.from(Instant.now()));
            ps.setInt(3, lockId);
            ps.setString(4, instanceId);
            ps.setObject(5, lockTimeout);
            updated = ps.executeUpdate();
        }
        return updated;
    }

    @Override
    public int renewLock(String instanceId, int lockId) throws Exception {
        String sql =
            """
            UPDATE scheduler_lock_t SET instance_id = ?, last_heartbeat = ?
            WHERE lock_id = ? AND (instance_id = ? OR last_heartbeat < ?)
            """;
        int updated = 0;
        try (Connection conn = ds.getConnection(); PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setString(1, instanceId);
            ps.setObject(2, OffsetDateTime.now());
            ps.setInt(3, lockId);
            ps.setString(4, instanceId);
            ps.setObject(5, OffsetDateTime.now());
            updated = ps.executeUpdate();
        }
        return updated;
    }

    @Override
    public int releaseLock(String instanceId, int lockId) throws Exception{
        String sql =
            """
            UPDATE scheduler_lock_t SET instance_id = 'none' WHERE lock_id = ? AND instance_id = ?
            """;
        int updated = 0;
        try (Connection conn = ds.getConnection(); PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setInt(1, lockId);
            ps.setString(2, instanceId);
            updated = ps.executeUpdate();
        }
        return updated;
    }

    @Override
    public Result<List<Map<String, Object>>> pollTasks(OffsetDateTime nextRunTs) {
        Result<List<Map<String, Object>>> result = null;
        // Select only the ID and name columns needed for labels, filter by host_id
        final String sql = "SELECT * FROM schedule_t WHERE active = TRUE AND next_run_ts <= ?";
        List<Map<String, Object>> tasks = new ArrayList<>(); // Initialize list for tasks

        try (Connection connection = ds.getConnection();
            PreparedStatement preparedStatement = connection.prepareStatement(sql)) {
            preparedStatement.setObject(1, nextRunTs);
            try (ResultSet rs = preparedStatement.executeQuery()) {
                while (rs.next()) {
                    Map<String, Object> task = new HashMap<>();
                    task.put("schedule_id", rs.getString("schedule_id"));
                    task.put("host_id", rs.getString("host_id"));
                    task.put("schedule_name", rs.getString("schedule_name"));
                    task.put("frequency_unit", rs.getString("frequency_unit"));
                    task.put("frequency_time", rs.getInt("frequency_time"));
                    task.put("start_ts", rs.getObject("start_ts") != null ? rs.getObject("start_ts", OffsetDateTime.class) : null);
                    task.put("next_run_ts", rs.getObject("next_run_ts") != null ? rs.getObject("next_run_ts", OffsetDateTime.class) : null);
                    task.put("event_topic", rs.getString("event_topic"));
                    task.put("event_type", rs.getString("event_type"));
                    task.put("event_data", rs.getString("event_data"));
                    task.put("aggregate_version", rs.getLong("aggregate_version"));
                    task.put("active", rs.getBoolean("active"));
                    tasks.add(task);
                }
            }
            result = Success.of(tasks);
        } catch (SQLException e) {
            logger.error("SQLException getting schedules for next run {}:", nextRunTs, e);
            result = Failure.of(new Status(SQL_EXCEPTION, e.getMessage()));
        } catch (Exception e) { // Catch other potential runtime exceptions
            logger.error("Unexpected exception getting schedules for next run {}:", nextRunTs, e);
            result = Failure.of(new Status(GENERIC_EXCEPTION, e.getMessage()));
        }
        return result;

    }

    @Override
    public Result<String> executeTask(Map<String, Object> task, long executionTimeMillis) {
        String scheduleId = (String) task.get("schedule_id");
        UUID hostId = UUID.fromString((String) task.get("host_id"));
        String eventType = (String) task.get("event_type");
        String eventData = (String) task.get("event_data");
        String frequencyUnitStr = (String) task.get("frequency_unit");
        int frequencyTime = (Integer) task.get("frequency_time");
        TimeUnit timeUnit = TimeUnit.valueOf(frequencyUnitStr.toUpperCase());
        long freq = TimeUtil.oneTimeUnitMillisecond(timeUnit) * frequencyTime;
        long nextRunTs = executionTimeMillis + freq;

        Result<String> result = null;
        if(logger.isTraceEnabled()) logger.trace("eventData = {}", eventData);
        Map<String, Object> eventMap = JsonMapper.string2Map(eventData);
        // add or replace id
        eventMap.put("id", UuidUtil.getUUID().toString());
        // get user to calculate the nonce, user cannot be null, and it should be validated when create the schedule.
        String userId = (String)eventMap.get(Constants.USER);
        if(logger.isTraceEnabled()) logger.trace("userId = {}", userId);
        long nonce = queryNonceByUserId(userId);
        eventMap.put("nonce", nonce);
        String aggregateId = (String)eventMap.get("subject");
        if(logger.isTraceEnabled()) logger.trace("id = {} aggregateId = {} user = {} nonce = {}", eventMap.get("id"), aggregateId, userId, nonce);
        // calculate aggregateType
        String aggregateType = EventTypeUtil.deriveAggregateTypeFromEventType(eventType);
        eventMap.put(PortalConstants.AGGREGATE_TYPE, aggregateType);
        // calculate aggregateVersion.
        int maxAggregateVersion = getMaxAggregateVersion(aggregateId);
        if(maxAggregateVersion == 0) {
            // first time creating for the aggregate.
            eventMap.put(PortalConstants.NEW_AGGREGATE_VERSION, 1);
            eventMap.put(PortalConstants.AGGREGATE_VERSION, 0);
            maxAggregateVersion = 1;
        } else {
            // the aggregate exists in the event source table.
            maxAggregateVersion = maxAggregateVersion + 1;
            eventMap.put(PortalConstants.NEW_AGGREGATE_VERSION, maxAggregateVersion);
        }
        if(logger.isTraceEnabled()) logger.trace("aggregateId = {} aggregateVersion = {}", aggregateId, maxAggregateVersion);

        try (Connection conn = ds.getConnection();) {
            conn.setAutoCommit(false);
            try {
                // 1. Update schedule_t start_ts and next_run_ts to prevent double execution and prepare for next cycle
                String updateSchedule = "UPDATE schedule_t SET start_ts = ?, next_run_ts = ? WHERE schedule_id = ?";
                try (PreparedStatement ps = conn.prepareStatement(updateSchedule)) {
                    ps.setTimestamp(1, Timestamp.from(Instant.ofEpochMilli(executionTimeMillis)));
                    ps.setTimestamp(2, Timestamp.from(Instant.ofEpochMilli(nextRunTs)));
                    ps.setObject(3, java.util.UUID.fromString(scheduleId));
                    int updated = ps.executeUpdate();
                    if (updated == 0) {
                        // This could happen if another instance (or thread) updated it first
                        logger.warn("Task for schedule {} already processed or version mismatch. Rolling back.", scheduleId);
                        conn.rollback();
                        return Failure.of(new Status(GENERIC_EXCEPTION, "Task already processed or version mismatch."));
                    }
                }

                UUID eventId = UuidUtil.getUUID();
                long offset = reserveOffsets(conn, 1);

                // 2. Insert into event_store_t
                // Using assumed column names based on user request and common patterns
                try (PreparedStatement ps = conn.prepareStatement(EventPersistenceImpl.insertEventStoreSql)) {

                    ps.setObject(1, eventId);
                    ps.setObject(2, hostId);
                    ps.setObject(3, UUID.fromString(userId));
                    ps.setLong(4,  nonce);
                    ps.setString(5, aggregateId);
                    ps.setLong(6,  maxAggregateVersion);
                    ps.setString(7, aggregateType);
                    ps.setString(8, eventType);
                    ps.setObject(9, OffsetDateTime.now()); // Use OffsetDateTime for TIMESTAMP WITH TIME ZONE
                    ps.setString(10, JsonMapper.toJson(eventMap));
                    ps.setString(11, "{}");

                    ps.executeUpdate();
                }

                // 3. Insert into outbox_message_t
                try (PreparedStatement ps = conn.prepareStatement(EventPersistenceImpl.insertOutboxMessageSql)) {
                    ps.setObject(1, eventId);
                    ps.setObject(2, hostId);
                    ps.setObject(3, UUID.fromString(userId));
                    ps.setLong(4,  nonce);
                    ps.setString(5, aggregateId);
                    ps.setLong(6,  maxAggregateVersion);
                    ps.setString(7, aggregateType);
                    ps.setString(8, eventType);
                    ps.setObject(9, OffsetDateTime.now()); // Use OffsetDateTime for TIMESTAMP WITH TIME ZONE
                    ps.setString(10, JsonMapper.toJson(eventMap));
                    ps.setString(11, "{}");
                    ps.setLong(12, offset);

                    ps.executeUpdate();
                }
                conn.commit();
                if (logger.isInfoEnabled()) {
                    logger.info("Successfully executed task for schedule {}. Event ID: {}", scheduleId, eventId);
                }
                result = Success.of(eventId.toString());
            } catch (Exception e) {
                conn.rollback();
                throw e;
            } finally {
                conn.setAutoCommit(true);
            }
        } catch (SQLException e) {
            logger.error("SQLException:", e);
            result = Failure.of(new Status(SQL_EXCEPTION, e.getMessage()));
        }
        return result;
    }

    private long queryNonceByUserId(String userId){
        final String updateNonceSql = "UPDATE user_t SET nonce = nonce + 1 WHERE user_id = ? RETURNING nonce;";
        try (Connection connection = ds.getConnection();
             PreparedStatement statement = connection.prepareStatement(updateNonceSql)) {
            Long nonce = null;
            statement.setObject(1, UUID.fromString(userId));
            try (ResultSet resultSet = statement.executeQuery()) {
                if(resultSet.next()){
                    nonce = (Long)resultSet.getObject(1);
                }
            }
            if (nonce == null)
                return 1L;
            else
                return nonce;
        } catch (SQLException e) {
            logger.error("SQLException:", e);
            return 1L;
        } catch (Exception e) {
            logger.error("Exception:", e);
            return 1L;
        }
    }

    private int getMaxAggregateVersion(String aggregateId) {
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
