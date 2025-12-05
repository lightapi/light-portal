package net.lightapi.portal.db.persistence;

import com.networknt.config.JsonMapper;
import com.networknt.monad.Failure;
import com.networknt.monad.Result;
import com.networknt.monad.Success;
import com.networknt.status.Status;
import com.networknt.utility.Constants;
import io.cloudevents.core.v1.CloudEventV1;
import net.lightapi.portal.PortalConstants;
import net.lightapi.portal.db.ConcurrencyException;
import net.lightapi.portal.db.PortalDbProvider;
import net.lightapi.portal.db.util.NotificationService;
import net.lightapi.portal.db.util.SqlUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.OffsetDateTime;
import java.util.*;

import static com.networknt.db.provider.SqlDbStartupHook.ds;
import static net.lightapi.portal.db.util.SqlUtil.*;

public class SchedulePersistenceImpl implements SchedulePersistence {

    private static final Logger logger = LoggerFactory.getLogger(SchedulePersistenceImpl.class);
    // Consider moving these to a shared constants class if they are truly general
    private static final String SQL_EXCEPTION = PortalDbProvider.SQL_EXCEPTION;
    private static final String GENERIC_EXCEPTION = PortalDbProvider.GENERIC_EXCEPTION;
    private static final String OBJECT_NOT_FOUND = PortalDbProvider.OBJECT_NOT_FOUND;

    private final NotificationService notificationService;

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
                    event_topic,
                    event_type,
                    event_data,
                    update_user,
                    update_ts,
                    aggregate_version,
                    active
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, TRUE)
                ON CONFLICT (schedule_id) DO UPDATE
                SET host_id = EXCLUDED.host_id,
                    schedule_name = EXCLUDED.schedule_name,
                    frequency_unit = EXCLUDED.frequency_unit,
                    frequency_time = EXCLUDED.frequency_time,
                    start_ts = EXCLUDED.start_ts,
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
            statement.setObject(i++, OffsetDateTime.parse((String)map.get("startTs")));

            // 7. event_topic (Required)
            statement.setString(i++, (String)map.get("eventTopic"));
            // 8. event_type (Required)
            statement.setString(i++, (String)map.get("eventType"));
            // 9. event_data (Required, TEXT - assuming JSON stored as string)
            statement.setString(i++, (String)map.get("eventData"));

            // 10. update_user (From event metadata)
            statement.setString(i++, (String)event.get(Constants.USER));
            // 11. update_ts (From event metadata)
            statement.setObject(i++, OffsetDateTime.parse((String)event.get(CloudEventV1.TIME)));
            // 12. aggregate_version
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
            statement.setObject(i++, OffsetDateTime.parse((String)map.get("startTs")));
            // 5: event_topic
            statement.setString(i++, (String)map.get("eventTopic"));
            // 6: event_type
            statement.setString(i++, (String)map.get("eventType"));
            // 7: event_data
            statement.setString(i++, (String)map.get("eventData"));
            // 8: update_user
            statement.setString(i++, (String)event.get(Constants.USER));
            // 9: update_ts
            statement.setObject(i++, OffsetDateTime.parse((String)event.get(CloudEventV1.TIME)));
            // 10: aggregate_version
            statement.setLong(i++, newAggregateVersion);

            // WHERE conditions (2 placeholders)
            // 11: schedule_id
            statement.setObject(i++, UUID.fromString(scheduleId));
            // 12: aggregate_version < ? (new version for OCC/IDM check)
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
    public Result<String> getSchedule(int offset, int limit, String filtersJson, String globalFilter, String sortingJson, String hostId) {
        Result<String> result = null;
        List<Map<String, Object>> filters = parseJsonList(filtersJson);
        List<Map<String, Object>> sorting = parseJsonList(sortingJson);

        String s =
            """
            SELECT COUNT(*) OVER () AS total,
            schedule_id, host_id, schedule_name, frequency_unit, frequency_time,
            start_ts, event_topic, event_type, event_data, update_user,
            update_ts, aggregate_version, active
            FROM schedule_t
            WHERE host_id = ?
            """;

        List<Object> parameters = new ArrayList<>();
        parameters.add(UUID.fromString(hostId));

        String[] searchColumns = {"schedule_name"};
        String sqlBuilder = s + dynamicFilter(Arrays.asList("schedule_id", "host_id"), Arrays.asList(searchColumns), filters, null, parameters) +
                globalFilter(globalFilter, searchColumns, parameters) +
                dynamicSorting("schedule_name", sorting, null) +
                "\nLIMIT ? OFFSET ?";

        parameters.add(limit);
        parameters.add(offset);
        int total = 0; // Variable to store total count
        List<Map<String, Object>> schedules = new ArrayList<>(); // List to hold results

        try (Connection connection = ds.getConnection();
            PreparedStatement preparedStatement = connection.prepareStatement(sqlBuilder)) {
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
                    map.put("startTs", resultSet.getObject("start_ts") != null ? resultSet.getObject("start_ts", OffsetDateTime.class) : null);
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
        final String sql = "SELECT schedule_id, schedule_name FROM schedule_t WHERE host_id = ? ORDER BY schedule_name";
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
                start_ts, event_topic, event_type, event_data, update_user, update_ts, aggregate_version
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
                        scheduleMap.put("startTs", resultSet.getObject("start_ts") != null ? resultSet.getObject("start_ts", OffsetDateTime.class) : null);
                        scheduleMap.put("eventTopic", resultSet.getString("event_topic"));
                        scheduleMap.put("eventType", resultSet.getString("event_type"));
                        scheduleMap.put("eventData", resultSet.getString("event_data")); // Assuming TEXT -> String
                        scheduleMap.put("updateUser", resultSet.getString("update_user"));
                        scheduleMap.put("updateTs", resultSet.getObject("update_ts") != null ? resultSet.getObject("update_ts", OffsetDateTime.class) : null);
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
}
