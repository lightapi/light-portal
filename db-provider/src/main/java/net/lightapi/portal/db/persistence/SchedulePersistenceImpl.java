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
        final String sql = "INSERT INTO schedule_t(schedule_id, host_id, schedule_name, frequency_unit, frequency_time, " +
                "start_ts, event_topic, event_type, event_data, update_user, update_ts, aggregate_version) " +
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
        Map<String, Object> map = (Map<String, Object>)event.get(PortalConstants.DATA);
        String scheduleId = (String) map.get("scheduleId");
        long newAggregateVersion = SqlUtil.getNewAggregateVersion(event);

        try (PreparedStatement statement = conn.prepareStatement(sql)) {

            // 1. schedule_id (Required)
            statement.setObject(1, UUID.fromString(scheduleId));
            // 2. host_id (Required - from event metadata)
            statement.setObject(2, UUID.fromString((String)map.get("hostId")));
            // 3. schedule_name (Required)
            statement.setString(3, (String)map.get("scheduleName"));
            // 4. frequency_unit (Required)
            statement.setString(4, (String)map.get("frequencyUnit"));
            // 5. frequency_time (Required, Integer)
            statement.setInt(5, ((Number) map.get("frequencyTime")).intValue());
            statement.setObject(6, OffsetDateTime.parse((String)map.get("startTs")));
            // 6. event_topic (Required)
            statement.setString(7, (String)map.get("eventTopic"));
            // 7. event_type (Required)
            statement.setString(8, (String)map.get("eventType"));
            // 8. event_data (Required, TEXT - assuming JSON stored as string)
            statement.setString(9, (String)map.get("eventData"));

            // 9. update_user (From event metadata)
            statement.setString(10, (String)event.get(Constants.USER));

            // 10. update_ts (From event metadata)
            statement.setObject(11, OffsetDateTime.parse((String)event.get(CloudEventV1.TIME)));
            // 11. aggregate_version
            statement.setLong(12, newAggregateVersion);

            // Execute insert
            int count = statement.executeUpdate();
            if (count == 0) {
                throw new SQLException(String.format("Failed during createSchedule for scheduleId%s with aggregateVersion %d", scheduleId, newAggregateVersion));
            }
        } catch (SQLException e) {
            logger.error("SQLException during createClient for scheduleId {} aggregateVersion {}: {}", scheduleId, newAggregateVersion, e.getMessage(), e);
            throw e;
        } catch (Exception e) {
            logger.error("Exception during createClient for scheduleId {} aggregateVersion {}: {}", scheduleId, newAggregateVersion, e.getMessage(), e);
            throw e;
        }
    }

    private boolean queryScheduleExists(Connection conn, String scheduleId) throws SQLException {
        final String sql =
                """
                SELECT COUNT(*) FROM schedule_t WHERE schedule_id = ?
                """;
        try (PreparedStatement pst = conn.prepareStatement(sql)) {
            pst.setObject(1, UUID.fromString(scheduleId));
            try (ResultSet rs = pst.executeQuery()) {
                return rs.next() && rs.getInt(1) > 0;
            }
        }
    }

    @Override
    public void updateSchedule(Connection conn, Map<String, Object> event) throws SQLException, Exception {
        // SQL statement for updating schedule_t
        // Assuming host_id might not be typically updated, focusing on schedule details
        final String sql =
                """
                UPDATE schedule_t SET schedule_name = ?, frequency_unit = ?, frequency_time = ?,
                start_ts = ?, event_topic = ?, event_type = ?, event_data = ?,
                update_user = ?, update_ts = ?, aggregate_version = ?
                WHERE schedule_id = ? AND aggregate_version = ?
                """;
        Map<String, Object> map = (Map<String, Object>)event.get(PortalConstants.DATA);
        String scheduleId = (String) map.get("scheduleId");
        long oldAggregateVersion = SqlUtil.getOldAggregateVersion(event);
        long newAggregateVersion = SqlUtil.getNewAggregateVersion(event);

        try (PreparedStatement statement = conn.prepareStatement(sql)) {
            statement.setString(1, (String)map.get("scheduleName"));
            statement.setString(2, (String)map.get("frequencyUnit"));
            statement.setInt(3, ((Number) map.get("frequencyTime")).intValue());
            statement.setObject(4, OffsetDateTime.parse((String)map.get("startTs")));
            statement.setString(5, (String)map.get("eventTopic"));
            statement.setString(6, (String)map.get("eventType"));
            statement.setString(7, (String)map.get("eventData"));
            statement.setString(8, (String)event.get(Constants.USER));
            statement.setObject(9, OffsetDateTime.parse((String)event.get(CloudEventV1.TIME)));
            statement.setLong(10, newAggregateVersion);
            statement.setObject(11, UUID.fromString(scheduleId));
            statement.setLong(12, oldAggregateVersion);

            // Execute update
            int count = statement.executeUpdate();
            if (count == 0) {
                if (queryScheduleExists(conn, scheduleId)) {
                    throw new ConcurrencyException("Optimistic concurrency conflict during updateSchedule for scheduleId " + scheduleId + ". Expected version " + oldAggregateVersion + " but found a different version " + newAggregateVersion + ".");
                } else {
                    throw new SQLException("No record found during updateSchedule for scheduleId " + scheduleId + ".");
                }
            }
        } catch (SQLException e) {
            logger.error("SQLException during updateClient for scheduleId {} (old: {}) -> (new: {}): {}", scheduleId, oldAggregateVersion, newAggregateVersion, e.getMessage(), e);
            throw e;
        } catch (Exception e) {
            logger.error("Exception during updateClient for scheduleId {} (old: {}) -> (new: {}): {}", scheduleId, oldAggregateVersion, newAggregateVersion, e.getMessage(), e);
            throw e;
        }
    }

    @Override
    public void deleteSchedule(Connection conn, Map<String, Object> event) throws SQLException, Exception {
        // SQL statement for deleting from schedule_t
        final String sql = "DELETE FROM schedule_t WHERE schedule_id = ? AND aggregate_version = ?";
        Map<String, Object> map = (Map<String, Object>)event.get(PortalConstants.DATA);
        String scheduleId = (String) map.get("scheduleId");
        long oldAggregateVersion = SqlUtil.getOldAggregateVersion(event);

        try (PreparedStatement statement = conn.prepareStatement(sql)) {
            statement.setObject(1, UUID.fromString(scheduleId));
            statement.setLong(2, oldAggregateVersion);
            int count = statement.executeUpdate();
            if (count == 0) {
                if (queryScheduleExists(conn, scheduleId)) {
                    throw new ConcurrencyException("Optimistic concurrency conflict during deleteSchedule for scheduleId " + scheduleId + " aggregateVersion " + oldAggregateVersion + " but found a different version or already updated.");
                } else {
                    throw new SQLException("No record found during deleteSchedule for scheduleId " + scheduleId + ". It might have been already deleted.");
                }
            }
        } catch (SQLException e) {
            logger.error("SQLException during deleteSchedule for scheduleId {} aggregateVersion {}: {}", scheduleId, oldAggregateVersion, e.getMessage(), e);
            throw e;
        } catch (Exception e) {
            logger.error("Exception during deleteSchedule for scheduleId {} aggregateVersion {}: {}", scheduleId, oldAggregateVersion, e.getMessage(), e);
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
