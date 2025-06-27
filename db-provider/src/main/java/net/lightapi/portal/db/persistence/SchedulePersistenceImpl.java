package net.lightapi.portal.db.persistence;

import com.networknt.config.JsonMapper;
import com.networknt.monad.Failure;
import com.networknt.monad.Result;
import com.networknt.monad.Success;
import com.networknt.status.Status;
import com.networknt.utility.Constants;
import io.cloudevents.core.v1.CloudEventV1;
import net.lightapi.portal.PortalConstants;
import net.lightapi.portal.db.PortalDbProvider;
import net.lightapi.portal.db.util.NotificationService;
import net.lightapi.portal.db.util.SqlUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection; // Added import
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException; // Added import
import java.sql.Types;
import java.time.OffsetDateTime;
import java.util.*;

import static com.networknt.db.provider.SqlDbStartupHook.ds;
import static java.sql.Types.NULL;
import static net.lightapi.portal.db.util.SqlUtil.addCondition;

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
                "start_ts, event_topic, event_type, event_data, update_user, update_ts) " +
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
        Map<String, Object> map = (Map<String, Object>)event.get(PortalConstants.DATA);
        String scheduleId = (String) map.get("scheduleId"); // Get scheduleId for PK, return, logging

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


            // Execute insert
            int count = statement.executeUpdate();
            if (count == 0) {
                throw new SQLException("Failed to insert the schedule with id " + scheduleId);
            }

            // Success path
            notificationService.insertNotification(event, true, null);

        } catch (SQLException e) {
            // Check for duplicate key violation (PK = schedule_id)
            if ("23505".equals(e.getSQLState())) { // Standard SQLState for unique violation
                logger.error("Duplicate schedule entry for ID {}: {}", scheduleId, e.getMessage());
                notificationService.insertNotification(event, false, "Duplicate entry for schedule " + scheduleId);
                throw new SQLException(new Status("ERR_DUPLICATE_SCHEDULE", "Schedule already exists with ID " + scheduleId, e.getMessage()).toString(), e); // Re-throw wrapped in SQLException
            } else {
                logger.error("SQLException during schedule creation for {}:", scheduleId, e);
                notificationService.insertNotification(event, false, e.getMessage());
                throw e; // Re-throw SQLException
            }
        } catch (Exception e) { // Catch other potential runtime exceptions (like ClassCastException)
            logger.error("Unexpected exception during schedule creation for {}:", scheduleId, e);
            notificationService.insertNotification(event, false, e.getMessage());
            throw e; // Re-throw generic Exception
        }
    }

    @Override
    public void updateSchedule(Connection conn, Map<String, Object> event) throws SQLException, Exception {
        // SQL statement for updating schedule_t
        // Assuming host_id might not be typically updated, focusing on schedule details
        final String sql = "UPDATE schedule_t SET schedule_name = ?, frequency_unit = ?, frequency_time = ?, " +
                "start_ts = ?, event_topic = ?, event_type = ?, event_data = ?, update_user = ?, update_ts = ? " +
                "WHERE schedule_id = ?";
        Map<String, Object> map = (Map<String, Object>)event.get(PortalConstants.DATA);
        String scheduleId = (String) map.get("scheduleId"); // Get scheduleId for WHERE clause and return

        try (PreparedStatement statement = conn.prepareStatement(sql)) {

            // Set parameters for the UPDATE statement
            // 1. schedule_name (Required)
            statement.setString(1, (String)map.get("scheduleName"));
            // 2. frequency_unit (Required)
            statement.setString(2, (String)map.get("frequencyUnit"));
            // 3. frequency_time (Required, Integer)
            statement.setInt(3, ((Number) map.get("frequencyTime")).intValue());
            statement.setObject(4, OffsetDateTime.parse((String)map.get("startTs")));
            // 4. event_topic (Required)
            statement.setString(5, (String)map.get("eventTopic"));
            // 5. event_type (Required)
            statement.setString(6, (String)map.get("eventType"));
            // 6. event_data (Required, TEXT - assuming JSON stored as string)
            statement.setString(7, (String)map.get("eventData"));

            // 7. update_user (From event metadata)
            statement.setString(8, (String)event.get(Constants.USER));

            // 8. update_ts (From event metadata)
            statement.setObject(9, OffsetDateTime.parse((String)event.get(CloudEventV1.TIME)));

            // 9. schedule_id (For WHERE clause - Required)
            statement.setObject(10, UUID.fromString(scheduleId));

            // Execute update
            int count = statement.executeUpdate();
            if (count == 0) {
                // Record not found to update
                throw new SQLException("Failed to update the schedule with id " + scheduleId + " - record not found.");
            }

            // Success path
            notificationService.insertNotification(event, true, null);

        } catch (SQLException e) {
            logger.error("SQLException during schedule update for {}:", scheduleId, e);
            notificationService.insertNotification(event, false, e.getMessage());
            throw e; // Re-throw SQLException
        } catch (Exception e) { // Catch other potential runtime exceptions
            logger.error("Unexpected exception during schedule update for {}:", scheduleId, e);
            notificationService.insertNotification(event, false, e.getMessage());
            throw e; // Re-throw generic Exception
        }
    }

    @Override
    public void deleteSchedule(Connection conn, Map<String, Object> event) throws SQLException, Exception {
        // SQL statement for deleting from schedule_t
        final String sql = "DELETE FROM schedule_t WHERE schedule_id = ?";
        Map<String, Object> map = (Map<String, Object>)event.get(PortalConstants.DATA);
        String scheduleId = (String) map.get("scheduleId"); // Get scheduleId for WHERE clause and for messages

        // Basic check for required field (Primary Key part)
        if (scheduleId == null) {
            logger.error("Missing required field 'scheduleId' in data payload for deleteSchedule: {}", map);
            throw new IllegalArgumentException(new Status("ERR_MISSING_SCHEDULE_DELETE_ID", "'scheduleId' missing in deleteSchedule data").toString());
        }

        try (PreparedStatement statement = conn.prepareStatement(sql)) {

            // Set parameter for the WHERE clause
            statement.setObject(1, UUID.fromString(scheduleId));

            // Execute delete
            int count = statement.executeUpdate();
            if (count == 0) {
                // Record not found to delete. Following template by throwing.
                throw new SQLException("Failed to delete the schedule with id " + scheduleId + " - record not found.");
            }

            // Success path
            notificationService.insertNotification(event, true, null);

        } catch (SQLException e) {
            logger.error("SQLException during schedule delete for {}:", scheduleId, e);
            notificationService.insertNotification(event, false, e.getMessage());
            throw e; // Re-throw SQLException
        } catch (Exception e) { // Catch other potential runtime exceptions
            logger.error("Unexpected exception during schedule delete for {}:", scheduleId, e);
            notificationService.insertNotification(event, false, e.getMessage());
            throw e; // Re-throw generic Exception
        }
    }

    @Override
    public Result<String> getSchedule(int offset, int limit, String hostId, String scheduleId, String scheduleName, String frequencyUnit,
                                      Integer frequencyTime, String startTs, String eventTopic, String eventType, String eventData) {
        Result<String> result = null;
        StringBuilder sqlBuilder = new StringBuilder();
        sqlBuilder.append("SELECT COUNT(*) OVER () AS total,\n" +
                "schedule_id, host_id, schedule_name, frequency_unit, frequency_time, " +
                "start_ts, event_topic, event_type, event_data, update_user, update_ts\n" +
                "FROM schedule_t\n" +
                "WHERE 1=1\n"); // Start WHERE clause

        List<Object> parameters = new ArrayList<>();
        StringBuilder whereClause = new StringBuilder();

        // Add conditions based on input parameters using the helper
        addCondition(whereClause, parameters, "host_id", hostId != null ? UUID.fromString(hostId) : null);
        addCondition(whereClause, parameters, "schedule_id", scheduleId != null ? UUID.fromString(scheduleId) : null);
        addCondition(whereClause, parameters, "schedule_name", scheduleName);
        addCondition(whereClause, parameters, "frequency_unit", frequencyUnit);
        addCondition(whereClause, parameters, "frequency_time", frequencyTime);
        addCondition(whereClause, parameters, "event_topic", eventTopic);
        addCondition(whereClause, parameters, "event_type", eventType);
        // eventData is TEXT, exact match might not be useful, consider LIKE or omit
        // addCondition(whereClause, parameters, "event_data", eventData);


        // Append the dynamic WHERE conditions if any were added
        if (!whereClause.isEmpty()) {
            sqlBuilder.append("AND ").append(whereClause);
        }

        // Add ordering and pagination
        sqlBuilder.append(" ORDER BY schedule_name\n" + // Default order
                "LIMIT ? OFFSET ?");

        parameters.add(limit);  // Add limit parameter
        parameters.add(offset); // Add offset parameter

        String sql = sqlBuilder.toString();
        int total = 0; // Variable to store total count
        List<Map<String, Object>> schedules = new ArrayList<>(); // List to hold results

        try (Connection connection = ds.getConnection();
             PreparedStatement preparedStatement = connection.prepareStatement(sql)) {

            // Bind all collected parameters
            for (int i = 0; i < parameters.size(); i++) {
                preparedStatement.setObject(i + 1, parameters.get(i));
            }

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

                    schedules.add(map); // Add map to the list
                }
            }

            // Prepare the final result map containing total count and the list of schedules
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
        final String sql = "SELECT schedule_id, host_id, schedule_name, frequency_unit, frequency_time, " +
                "start_ts, event_topic, event_type, event_data, update_user, update_ts " +
                "FROM schedule_t WHERE schedule_id = ?";
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
                    }
                }
                // Check if map was populated (i.e., record found)
                if (scheduleMap != null) {
                    result = Success.of(JsonMapper.toJson(scheduleMap));
                } else {
                    // Record not found
                    result = Failure.of(new Status(OBJECT_NOT_FOUND, "schedule", scheduleId)); // Changed to Failure consistent with AccessControlPersistence
                }
            }
            // No commit/rollback needed for SELECT
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
