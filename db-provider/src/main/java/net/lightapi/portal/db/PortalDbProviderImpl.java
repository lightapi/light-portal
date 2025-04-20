package net.lightapi.portal.db;

import com.networknt.config.JsonMapper;
import com.networknt.monad.Failure;
import com.networknt.monad.Result;
import com.networknt.monad.Success;
import com.networknt.status.Status;
import com.networknt.utility.Constants;
import io.cloudevents.core.v1.CloudEventV1;
import net.lightapi.portal.PortalConstants;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;
import java.time.OffsetDateTime;
import java.util.*;

import static com.networknt.db.provider.SqlDbStartupHook.ds;
import static java.sql.Types.NULL;

public class PortalDbProviderImpl implements PortalDbProvider {
    public static final Logger logger = LoggerFactory.getLogger(PortalDbProviderImpl.class);
    public static final String SQL_EXCEPTION = "ERR10017";
    public static final String GENERIC_EXCEPTION = "ERR10014";
    public static final String OBJECT_NOT_FOUND = "ERR11637";

    public static final String INSERT_NOTIFICATION = "INSERT INTO notification_t (id, host_id, user_id, nonce, event_class, event_json, process_ts, " +
            "is_processed, error) VALUES (?, ?, ?, ?, ?,  ?, ?, ?, ?)";

    @Override
    public Result<String> createRefTable(Map<String, Object> event) {
        final String sql = "INSERT INTO ref_table_t(table_id, host_id, table_name, table_desc, active, editable, update_user, update_ts) " +
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?)";
        Result<String> result;
        Map<String, Object> map = (Map<String, Object>)event.get(PortalConstants.DATA);
        String tableId = (String) map.get("tableId"); // Get tableId for return/logging/error

        try (Connection conn = ds.getConnection()) {
            conn.setAutoCommit(false);
            try (PreparedStatement statement = conn.prepareStatement(sql)) {

                // 1. table_id (Required)
                statement.setObject(1, UUID.fromString(tableId));

                // 2. host_id (Optional - NULL means global)
                String hostId = (String) map.get("hostId");
                if (hostId != null && !hostId.isEmpty()) {
                    statement.setObject(2, UUID.fromString(hostId));
                } else {
                    statement.setNull(2, Types.VARCHAR); // NULL for global
                }

                // 3. table_name (Required)
                statement.setString(3, (String)map.get("tableName"));

                // 4. table_desc (Optional)
                String tableDesc = (String) map.get("tableDesc");
                if (tableDesc != null && !tableDesc.isEmpty()) {
                    statement.setString(4, tableDesc);
                } else {
                    statement.setNull(4, Types.VARCHAR); // NULL for no description
                }

                // 5. active (Optional - handle default)
                if (map.containsKey("active") && map.get("active") instanceof Boolean) {
                    statement.setBoolean(5, (Boolean) map.get("active"));
                } else {
                    statement.setBoolean(5, true); // Default value
                }

                // 6. editable (Optional - handle default)
                if (map.containsKey("editable") && map.get("editable") instanceof Boolean) {
                    statement.setBoolean(6, (Boolean) map.get("editable"));
                } else {
                    statement.setBoolean(6, true); // Default value
                }

                // 7. update_user (From event metadata)
                statement.setString(7, (String)event.get(Constants.USER));

                // 8. update_ts (From event metadata)
                statement.setObject(8, OffsetDateTime.parse((String)event.get(CloudEventV1.TIME)));

                // Execute insert
                int count = statement.executeUpdate();
                if (count == 0) {
                    throw new SQLException("failed to insert the reference table with id " + tableId);
                }

                // Success path
                conn.commit();
                result =  Success.of(tableId); // Return tableId
                insertNotification(event, true, null);

            } catch (SQLException e) {
                logger.error("SQLException:", e);
                conn.rollback();
                insertNotification(event, false, e.getMessage());
                result = Failure.of(new Status(SQL_EXCEPTION, e.getMessage()));
            } catch (Exception e) { // Catch other potential runtime exceptions
                logger.error("Exception:", e);
                conn.rollback();
                insertNotification(event, false, e.getMessage());
                result = Failure.of(new Status(GENERIC_EXCEPTION, e.getMessage()));
            }
        } catch (SQLException e) {
            logger.error("SQLException:", e);
            result = Failure.of(new Status(SQL_EXCEPTION, e.getMessage()));
        }
        return result;
    }

    @Override
    public Result<String> updateRefTable(Map<String, Object> event) {
        // Note: host_id and table_name uniqueness/changes are usually handled
        // at the service layer, potentially requiring delete/create or careful checks.
        // This update focuses on mutable fields like desc, active, editable.
        final String sql = "UPDATE ref_table_t SET table_name = ?, table_desc = ?, active = ?, editable = ?, " +
                "update_user = ?, update_ts = ? WHERE table_id = ?";
        Result<String> result;
        Map<String, Object> map = (Map<String, Object>)event.get(PortalConstants.DATA);
        String tableId = (String) map.get("tableId"); // Get tableId for WHERE clause and return

        try (Connection conn = ds.getConnection()) {
            conn.setAutoCommit(false);
            try (PreparedStatement statement = conn.prepareStatement(sql)) {

                // 1. table_name (Required in update payload)
                statement.setString(1, (String)map.get("tableName"));

                // 2. table_desc (Optional)
                String tableDesc = (String) map.get("tableDesc");
                if (tableDesc != null && !tableDesc.isEmpty()) {
                    statement.setString(2, tableDesc);
                } else {
                    statement.setNull(2, Types.VARCHAR); // NULL for no description
                }

                // 3. active (Optional - handle default/presence)
                // Assume if not present in update payload, it doesn't change.
                // If it *must* be provided in update, remove containsKey check.
                if (map.containsKey("active") && map.get("active") instanceof Boolean) {
                    statement.setBoolean(3, (Boolean) map.get("active"));
                } else {
                    // What to do if missing? Keep current value? Need to fetch first,
                    // or assume update payload is complete for fields being updated.
                    // For simplicity here, setting based on payload or defaulting.
                    // A more robust update might fetch existing record first.
                    statement.setBoolean(3, true);
                }

                // 4. editable (Optional - handle default/presence)
                if (map.containsKey("editable") && map.get("editable") instanceof Boolean) {
                    statement.setBoolean(4, (Boolean) map.get("editable"));
                } else {
                    statement.setBoolean(4, true);
                }

                // 5. update_user (From event metadata)
                statement.setString(5, (String)event.get(Constants.USER));

                // 6. update_ts (From event metadata)
                statement.setObject(6, OffsetDateTime.parse((String)event.get(CloudEventV1.TIME)));

                // 7. table_id (For WHERE clause - Required)
                statement.setObject(7, UUID.fromString(tableId));

                // Execute update
                int count = statement.executeUpdate();
                if (count == 0) {
                    throw new SQLException("failed to update the reference table with id " + tableId + " - record not found.");
                }

                // Success path
                conn.commit();
                result =  Success.of(tableId); // Return tableId
                insertNotification(event, true, null);

            } catch (SQLException e) {
                logger.error("SQLException:", e);
                conn.rollback();
                insertNotification(event, false, e.getMessage());
                result = Failure.of(new Status(SQL_EXCEPTION, e.getMessage()));
            } catch (Exception e) { // Catch other potential runtime exceptions
                logger.error("Exception:", e);
                conn.rollback();
                insertNotification(event, false, e.getMessage());
                result = Failure.of(new Status(GENERIC_EXCEPTION, e.getMessage()));
            }
        } catch (SQLException e) {
            logger.error("SQLException:", e);
            result = Failure.of(new Status(SQL_EXCEPTION, e.getMessage()));
        }
        return result;
    }

    @Override
    public Result<String> deleteRefTable(Map<String, Object> event) {
        final String sql = "DELETE FROM ref_table_t WHERE table_id = ?";
        Result<String> result;
        Map<String, Object> map = (Map<String, Object>)event.get(PortalConstants.DATA);
        String tableId = (String) map.get("tableId"); // Get tableId for WHERE clause and return

        try (Connection conn = ds.getConnection()) {
            conn.setAutoCommit(false);
            try (PreparedStatement statement = conn.prepareStatement(sql)) {
                statement.setObject(1, UUID.fromString(tableId));

                int count = statement.executeUpdate();
                if (count == 0) {
                    throw new SQLException("failed to delete the reference table with id " + tableId + " - record not found.");
                }
                conn.commit();
                result =  Success.of(tableId); // Return tableId
                insertNotification(event, true, null);

            } catch (SQLException e) {
                logger.error("SQLException:", e);
                conn.rollback();
                insertNotification(event, false, e.getMessage());
                result = Failure.of(new Status(SQL_EXCEPTION, e.getMessage()));
            } catch (Exception e) { // Catch other potential runtime exceptions
                logger.error("Exception:", e);
                conn.rollback();
                insertNotification(event, false, e.getMessage());
                result = Failure.of(new Status(GENERIC_EXCEPTION, e.getMessage()));
            }
        } catch (SQLException e) {
            logger.error("SQLException:", e);
            result = Failure.of(new Status(SQL_EXCEPTION, e.getMessage()));
        }
        return result;
    }

    @Override
    public Result<String> getRefTable(int offset, int limit, String hostId, String tableId, String tableName, String tableDesc,
                                      Boolean active, Boolean editable) {
        Result<String> result = null;
        StringBuilder sqlBuilder = new StringBuilder();
        sqlBuilder.append("SELECT COUNT(*) OVER () AS total,\n" +
                "table_id, host_id, table_name, table_desc, active, editable, update_user, update_ts\n" +
                "FROM ref_table_t\n" +
                "WHERE 1=1\n");

        List<Object> parameters = new ArrayList<>();
        StringBuilder whereClause = new StringBuilder();

        // --- Modified host_id condition to include global tables ---
        if (hostId != null && !hostId.isEmpty()) {
            whereClause.append("("); // Start OR group
            addCondition(whereClause, parameters, "host_id", hostId != null ? UUID.fromString(hostId) : null);
            whereClause.append(" OR host_id IS NULL)");         // Include global tables
        } else {
            // If hostId is null or empty, only fetch global tables
            addCondition(whereClause, parameters, "host_id", null); // Fetch only global
        }
        // --- Rest of the conditions ---
        addCondition(whereClause, parameters, "table_id", tableId != null ? UUID.fromString(tableId) : null);
        addCondition(whereClause, parameters, "table_name", tableName);
        addCondition(whereClause, parameters, "table_desc", tableDesc); // Might need LIKE for descriptions
        addCondition(whereClause, parameters, "active", active);
        addCondition(whereClause, parameters, "editable", editable);


        if (!whereClause.isEmpty()) {
            sqlBuilder.append("AND ").append(whereClause);
        }

        sqlBuilder.append(" ORDER BY table_name\n" +  // Order by table_name as default
                "LIMIT ? OFFSET ?");

        parameters.add(limit);
        parameters.add(offset);

        String sql = sqlBuilder.toString();
        int total = 0;
        List<Map<String, Object>> refTables = new ArrayList<>();

        try (Connection connection = ds.getConnection();
             PreparedStatement preparedStatement = connection.prepareStatement(sql)) {

            for (int i = 0; i < parameters.size(); i++) {
                preparedStatement.setObject(i + 1, parameters.get(i));
            }

            boolean isFirstRow = true;
            try (ResultSet resultSet = preparedStatement.executeQuery()) {
                while (resultSet.next()) {
                    Map<String, Object> map = new HashMap<>();
                    if (isFirstRow) {
                        total = resultSet.getInt("total");
                        isFirstRow = false;
                    }
                    map.put("tableId", resultSet.getObject("table_id", UUID.class));
                    map.put("hostId", resultSet.getObject("host_id", UUID.class));
                    map.put("tableName", resultSet.getString("table_name"));
                    map.put("tableDesc", resultSet.getString("table_desc"));
                    map.put("active", resultSet.getBoolean("active"));
                    map.put("editable", resultSet.getBoolean("editable"));
                    map.put("updateUser", resultSet.getString("update_user"));
                    map.put("updateTs", resultSet.getObject("update_ts") != null ? resultSet.getObject("update_ts", OffsetDateTime.class) : null);

                    refTables.add(map);
                }
            }

            Map<String, Object> resultMap = new HashMap<>();
            resultMap.put("total", total);
            resultMap.put("refTables", refTables); // Use a descriptive key for the list
            result = Success.of(JsonMapper.toJson(resultMap));

        } catch (SQLException e) {
            logger.error("SQLException:", e);
            result = Failure.of(new Status(SQL_EXCEPTION, e.getMessage()));
        } catch (Exception e) {
            logger.error("Exception:", e);
            result = Failure.of(new Status(GENERIC_EXCEPTION, e.getMessage()));
        }
        return result;
    }


    @Override
    public Result<String> getRefTableById(String tableId) {
        Result<String> result = null;
        // Select all columns from ref_table_t for the given table_id
        final String sql = "SELECT table_id, host_id, table_name, table_desc, active, editable, " +
                "update_user, update_ts FROM ref_table_t WHERE table_id = ?";
        Map<String, Object> map = null; // Initialize map to null

        try (Connection conn = ds.getConnection()) {
            // No need for setAutoCommit(false) for a SELECT
            try (PreparedStatement statement = conn.prepareStatement(sql)) {
                statement.setObject(1, UUID.fromString(tableId)); // Set the tableId parameter

                try (ResultSet resultSet = statement.executeQuery()) {
                    // Check if a row was found
                    if (resultSet.next()) {
                        map = new HashMap<>(); // Create map only if found
                        map.put("tableId", resultSet.getObject("table_id", UUID.class));
                        map.put("hostId", resultSet.getObject("host_id", UUID.class));
                        map.put("tableName", resultSet.getString("table_name"));
                        map.put("tableDesc", resultSet.getString("table_desc"));
                        map.put("active", resultSet.getBoolean("active"));
                        map.put("editable", resultSet.getBoolean("editable"));
                        map.put("updateUser", resultSet.getString("update_user"));
                        map.put("updateTs", resultSet.getObject("update_ts") != null ? resultSet.getObject("update_ts", OffsetDateTime.class) : null);
                    }
                }
                // Check if map was populated (i.e., record found)
                if (map != null && !map.isEmpty()) {
                    result = Success.of(JsonMapper.toJson(map));
                } else {
                    // Record not found
                    result = Failure.of(new Status(OBJECT_NOT_FOUND, tableId));
                }
            }
            // No commit/rollback needed for SELECT
        } catch (SQLException e) {
            logger.error("SQLException getting reference table by id {}:", tableId, e);
            result = Failure.of(new Status(SQL_EXCEPTION, e.getMessage()));
        } catch (Exception e) { // Catch other potential runtime exceptions
            logger.error("Unexpected exception getting reference table by id {}:", tableId, e);
            result = Failure.of(new Status(GENERIC_EXCEPTION, e.getMessage()));
        }
        return result;
    }


    @Override
    public Result<String> getRefTableLabel(String hostId) {
        Result<String> result = null;
        StringBuilder sqlBuilder = new StringBuilder();
        // Select only the ID and name columns needed for labels
        sqlBuilder.append("SELECT table_id, table_name FROM ref_table_t WHERE 1=1 "); // Base query

        // Apply host filtering (tenant-specific + global, or global only)
        if (hostId != null && !hostId.isEmpty()) {
            sqlBuilder.append("AND (host_id = ? OR host_id IS NULL)"); // Tenant-specific OR Global
        } else {
            sqlBuilder.append("AND host_id IS NULL"); // Only Global if hostId is null/empty
        }

        // Optionally add ordering
        sqlBuilder.append(" ORDER BY table_name");

        String sql = sqlBuilder.toString();
        List<Map<String, Object>> labels = new ArrayList<>(); // Initialize list for labels

        try (Connection connection = ds.getConnection();
             PreparedStatement preparedStatement = connection.prepareStatement(sql)) {

            // Set the hostId parameter only if it's part of the query
            if (hostId != null && !hostId.isEmpty()) {
                preparedStatement.setObject(1, UUID.fromString(hostId));
            }

            try (ResultSet resultSet = preparedStatement.executeQuery()) {
                // Iterate through results and build the label map list
                while (resultSet.next()) {
                    Map<String, Object> map = new HashMap<>();
                    map.put("id", resultSet.getString("table_id"));    // Key "id"
                    map.put("label", resultSet.getString("table_name")); // Key "label"
                    labels.add(map);
                }
            }
            // Serialize the list of labels to JSON and return Success
            result = Success.of(JsonMapper.toJson(labels));

        } catch (SQLException e) {
            logger.error("SQLException getting reference table labels for hostId {}:", hostId, e);
            result = Failure.of(new Status(SQL_EXCEPTION, e.getMessage()));
        } catch (Exception e) { // Catch other potential runtime exceptions
            logger.error("Unexpected exception getting reference table labels for hostId {}:", hostId, e);
            result = Failure.of(new Status(GENERIC_EXCEPTION, e.getMessage()));
        }
        return result;
    }

    @Override
    public Result<String> createRefValue(Map<String, Object> event) {
        final String sql = "INSERT INTO ref_value_t(value_id, table_id, value_code, value_desc, " +
                "start_time, end_time, display_order, active, update_user, update_ts) " +
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
        Result<String> result;
        Map<String, Object> map = (Map<String, Object>)event.get(PortalConstants.DATA);
        String valueId = (String) map.get("valueId"); // Get valueId for return/logging/error

        try (Connection conn = ds.getConnection()) {
            conn.setAutoCommit(false);
            try (PreparedStatement statement = conn.prepareStatement(sql)) {

                // 1. value_id (Required)
                statement.setObject(1, UUID.fromString(valueId));
                // 2. table_id (Required)
                statement.setObject(2, UUID.fromString((String)map.get("tableId")));
                // 3. value_code (Required)
                statement.setString(3, (String)map.get("valueCode"));

                // 4. value_desc (Optional)
                String valueDesc = (String) map.get("valueDesc");
                if (valueDesc != null && !valueDesc.isEmpty()) {
                    statement.setString(4, valueDesc);
                } else {
                    statement.setNull(4, Types.VARCHAR); // NULL for no description
                }

                // 5. start_time (Optional OffsetDateTime)
                if (map.get("startTime") != null && map.get("startTime") instanceof String) {
                    try {
                        statement.setObject(5, OffsetDateTime.parse((String)map.get("startTime")));
                    } catch (java.time.format.DateTimeParseException e) {
                        logger.warn("Invalid format for startTime '{}', setting NULL.", map.get("startTime"), e);
                        statement.setNull(5, Types.TIMESTAMP_WITH_TIMEZONE);
                    }
                } else {
                    statement.setNull(5, Types.TIMESTAMP_WITH_TIMEZONE);
                }

                // 6. end_time (Optional OffsetDateTime)
                if (map.get("endTime") != null && map.get("endTime") instanceof String) {
                    try {
                        statement.setObject(6, OffsetDateTime.parse((String)map.get("endTime")));
                    } catch (java.time.format.DateTimeParseException e) {
                        logger.warn("Invalid format for endTime '{}', setting NULL.", map.get("endTime"), e);
                        statement.setNull(6, Types.TIMESTAMP_WITH_TIMEZONE);
                    }
                } else {
                    statement.setNull(6, Types.TIMESTAMP_WITH_TIMEZONE);
                }

                // 7. display_order (Optional Integer)
                if (map.get("displayOrder") instanceof Number) {
                    statement.setInt(7, ((Number) map.get("displayOrder")).intValue());
                } else {
                    statement.setNull(7, Types.INTEGER); // Or set explicit default: statement.setInt(7, 0);
                }

                // 8. active (Optional Boolean - handle default)
                if (map.containsKey("active") && map.get("active") instanceof Boolean) {
                    statement.setBoolean(8, (Boolean) map.get("active"));
                } else {
                    statement.setBoolean(8, true); // Default value
                }

                // 9. update_user (From event metadata)
                statement.setString(9, (String)event.get(Constants.USER));

                // 10. update_ts (From event metadata)
                statement.setObject(10, OffsetDateTime.parse((String)event.get(CloudEventV1.TIME)));


                // Execute insert
                int count = statement.executeUpdate();
                if (count == 0) {
                    throw new SQLException("failed to insert the reference value with id " + valueId);
                }

                // Success path
                conn.commit();
                result =  Success.of(valueId);
                insertNotification(event, true, null);

            } catch (SQLException e) {
                logger.error("SQLException:", e);
                conn.rollback();
                insertNotification(event, false, e.getMessage());
                result = Failure.of(new Status(SQL_EXCEPTION, e.getMessage()));
            } catch (Exception e) { // Catch other potential runtime exceptions like parsing errors if not caught earlier
                logger.error("Exception:", e);
                conn.rollback();
                insertNotification(event, false, e.getMessage());
                result = Failure.of(new Status(GENERIC_EXCEPTION, e.getMessage()));
            }
        } catch (SQLException e) {
            logger.error("SQLException:", e);
            result = Failure.of(new Status(SQL_EXCEPTION, e.getMessage()));
        }
        return result;
    }

    @Override
    public Result<String> updateRefValue(Map<String, Object> event) {
        // Define the UPDATE SQL statement
        // Assuming table_id might be updatable, though potentially less common
        final String sql = "UPDATE ref_value_t SET table_id = ?, value_code = ?, value_desc = ?, start_time = ?, " +
                "end_time = ?, display_order = ?, active = ?, update_user = ?, update_ts = ? " +
                "WHERE value_id = ?";
        Result<String> result;
        Map<String, Object> map = (Map<String, Object>)event.get(PortalConstants.DATA);
        String valueId = (String) map.get("valueId"); // Get valueId for WHERE clause and return

        try (Connection conn = ds.getConnection()) {
            conn.setAutoCommit(false);
            try (PreparedStatement statement = conn.prepareStatement(sql)) {

                // 1. table_id (Required in update payload?) - Assuming it might change
                statement.setObject(1, UUID.fromString((String)map.get("tableId")));
                // 2. value_code (Required in update payload)
                statement.setString(2, (String)map.get("valueCode"));

                // 3. value_desc (Optional)
                String valueDesc = (String) map.get("valueDesc");
                if (valueDesc != null && !valueDesc.isEmpty()) {
                    statement.setString(3, valueDesc);
                } else {
                    statement.setNull(3, Types.VARCHAR); // NULL for no description
                }

                // 4. start_time (Optional OffsetDateTime)
                if (map.get("startTime") != null && map.get("startTime") instanceof String) {
                    try {
                        statement.setObject(4, OffsetDateTime.parse((String)map.get("startTime")));
                    } catch (java.time.format.DateTimeParseException e) {
                        logger.warn("Invalid format for startTime '{}', setting NULL.", map.get("startTime"), e);
                        statement.setNull(4, Types.TIMESTAMP_WITH_TIMEZONE);
                    }
                } else {
                    statement.setNull(4, Types.TIMESTAMP_WITH_TIMEZONE);
                }

                // 5. end_time (Optional OffsetDateTime)
                if (map.get("endTime") != null && map.get("endTime") instanceof String) {
                    try {
                        statement.setObject(5, OffsetDateTime.parse((String)map.get("endTime")));
                    } catch (java.time.format.DateTimeParseException e) {
                        logger.warn("Invalid format for endTime '{}', setting NULL.", map.get("endTime"), e);
                        statement.setNull(5, Types.TIMESTAMP_WITH_TIMEZONE);
                    }
                } else {
                    statement.setNull(5, Types.TIMESTAMP_WITH_TIMEZONE);
                }

                // 6. display_order (Optional Integer)
                if (map.get("displayOrder") instanceof Number) {
                    statement.setInt(6, ((Number) map.get("displayOrder")).intValue());
                } else {
                    statement.setNull(6, Types.INTEGER);
                }

                // 7. active (Optional Boolean)
                if (map.containsKey("active") && map.get("active") instanceof Boolean) {
                    statement.setBoolean(7, (Boolean) map.get("active"));
                } else {
                    // Decide update behavior: set default? Or assume not changing if missing?
                    // Setting default like template here:
                    statement.setBoolean(7, true);
                }

                // 8. update_user (From event metadata)
                statement.setString(8, (String)event.get(Constants.USER));

                // 9. update_ts (From event metadata)
                statement.setObject(9, OffsetDateTime.parse((String)event.get(CloudEventV1.TIME)));

                // 10. value_id (For WHERE clause - Required)
                statement.setObject(10, UUID.fromString(valueId));

                // Execute update
                int count = statement.executeUpdate();
                if (count == 0) {
                    throw new SQLException("failed to update the reference value with id " + valueId + " - record not found.");
                }

                // Success path
                conn.commit();
                result =  Success.of(valueId);
                insertNotification(event, true, null);

            } catch (SQLException e) {
                logger.error("SQLException:", e);
                conn.rollback();
                insertNotification(event, false, e.getMessage());
                result = Failure.of(new Status(SQL_EXCEPTION, e.getMessage()));
            } catch (Exception e) { // Catch other potential runtime exceptions
                logger.error("Exception:", e);
                conn.rollback();
                insertNotification(event, false, e.getMessage());
                result = Failure.of(new Status(GENERIC_EXCEPTION, e.getMessage()));
            }
        } catch (SQLException e) {
            logger.error("SQLException:", e);
            result = Failure.of(new Status(SQL_EXCEPTION, e.getMessage()));
        }
        return result;
    }

    @Override
    public Result<String> deleteRefValue(Map<String, Object> event) {
        final String sql = "DELETE FROM ref_value_t WHERE value_id = ?";
        Result<String> result;
        Map<String, Object> map = (Map<String, Object>)event.get(PortalConstants.DATA);
        String valueId = (String) map.get("valueId"); // Get valueId for WHERE clause and return

        try (Connection conn = ds.getConnection()) {
            conn.setAutoCommit(false);
            try (PreparedStatement statement = conn.prepareStatement(sql)) {
                statement.setObject(1, UUID.fromString(valueId));

                int count = statement.executeUpdate();
                if (count == 0) {
                    // Record not found to delete. Following template by throwing,
                    // but you might consider this a success case in DELETE.
                    throw new SQLException("failed to delete the reference value with id " + valueId + " - record not found.");
                }
                conn.commit();
                result =  Success.of(valueId);
                insertNotification(event, true, null);

            } catch (SQLException e) {
                logger.error("SQLException:", e);
                conn.rollback();
                insertNotification(event, false, e.getMessage());
                result = Failure.of(new Status(SQL_EXCEPTION, e.getMessage()));
            } catch (Exception e) { // Catch other potential runtime exceptions
                logger.error("Exception:", e);
                conn.rollback();
                insertNotification(event, false, e.getMessage());
                result = Failure.of(new Status(GENERIC_EXCEPTION, e.getMessage()));
            }
        } catch (SQLException e) {
            logger.error("SQLException:", e);
            result = Failure.of(new Status(SQL_EXCEPTION, e.getMessage()));
        }
        return result;
    }

    @Override
    public Result<String> getRefValue(int offset, int limit, String valueId, String tableId, String valueCode, String valueDesc,
                                      Integer displayOrder, Boolean active) {
        Result<String> result = null;
        StringBuilder sqlBuilder = new StringBuilder();
        sqlBuilder.append("SELECT COUNT(*) OVER () AS total,\n" +
                "value_id, table_id, value_code, value_desc, start_time, end_time, " +
                "display_order, active, update_user, update_ts\n" +
                "FROM ref_value_t\n" +
                "WHERE 1=1\n");

        List<Object> parameters = new ArrayList<>();
        StringBuilder whereClause = new StringBuilder();

        // Add conditions based on input parameters
        addCondition(whereClause, parameters, "value_id", valueId != null ? UUID.fromString(valueId) : null);
        addCondition(whereClause, parameters, "table_id", tableId != null ? UUID.fromString(tableId) : null);
        addCondition(whereClause, parameters, "value_code", valueCode);
        addCondition(whereClause, parameters, "value_desc", valueDesc); // Might need LIKE for descriptions
        addCondition(whereClause, parameters, "display_order", displayOrder);
        addCondition(whereClause, parameters, "active", active);

        if (!whereClause.isEmpty()) {
            sqlBuilder.append("AND ").append(whereClause);
        }

        // Add ordering and pagination
        sqlBuilder.append(" ORDER BY display_order, value_code\n" + // Default order
                "LIMIT ? OFFSET ?");

        parameters.add(limit);
        parameters.add(offset);

        String sql = sqlBuilder.toString();
        int total = 0;
        List<Map<String, Object>> refValues = new ArrayList<>();

        try (Connection connection = ds.getConnection();
             PreparedStatement preparedStatement = connection.prepareStatement(sql)) {

            // Bind parameters
            for (int i = 0; i < parameters.size(); i++) {
                preparedStatement.setObject(i + 1, parameters.get(i));
            }

            boolean isFirstRow = true;
            try (ResultSet resultSet = preparedStatement.executeQuery()) {
                // Process results
                while (resultSet.next()) {
                    Map<String, Object> map = new HashMap<>();
                    if (isFirstRow) {
                        total = resultSet.getInt("total");
                        isFirstRow = false;
                    }
                    map.put("valueId", resultSet.getObject("value_id", UUID.class));
                    map.put("tableId", resultSet.getObject("table_id", UUID.class));
                    map.put("valueCode", resultSet.getString("value_code"));
                    map.put("valueDesc", resultSet.getString("value_desc"));
                    map.put("startTime", resultSet.getObject("start_time") != null ? resultSet.getObject("start_time", OffsetDateTime.class) : null);
                    map.put("endTime", resultSet.getObject("end_time") != null ? resultSet.getObject("end_time", OffsetDateTime.class) : null);
                    map.put("displayOrder", resultSet.getInt("display_order"));
                    map.put("active", resultSet.getBoolean("active"));
                    map.put("updateUser", resultSet.getString("update_user"));
                    map.put("updateTs", resultSet.getObject("update_ts") != null ? resultSet.getObject("update_ts", OffsetDateTime.class) : null);

                    refValues.add(map);
                }
            }

            // Prepare final result map
            Map<String, Object> resultMap = new HashMap<>();
            resultMap.put("total", total);
            resultMap.put("refValues", refValues); // Use a descriptive key
            result = Success.of(JsonMapper.toJson(resultMap));

        } catch (SQLException e) {
            logger.error("SQLException:", e);
            result = Failure.of(new Status(SQL_EXCEPTION, e.getMessage()));
        } catch (Exception e) {
            logger.error("Exception:", e);
            result = Failure.of(new Status(GENERIC_EXCEPTION, e.getMessage()));
        }
        return result;
    }

    @Override
    public Result<String> getRefValueById(String valueId) {
        Result<String> result = null;
        // Select all columns from ref_value_t for the given value_id
        final String sql = "SELECT value_id, table_id, value_code, value_desc, start_time, end_time, " +
                "display_order, active, update_user, update_ts " +
                "FROM ref_value_t WHERE value_id = ?";
        Map<String, Object> refValueMap = null; // Initialize map to null

        try (Connection conn = ds.getConnection()) {
            // No setAutoCommit(false) needed for SELECT
            try (PreparedStatement statement = conn.prepareStatement(sql)) {
                statement.setObject(1, UUID.fromString(valueId));

                try (ResultSet resultSet = statement.executeQuery()) {
                    // Check if a row was found
                    if (resultSet.next()) {
                        refValueMap = new HashMap<>(); // Create map only if found
                        refValueMap.put("valueId", resultSet.getObject("value_id", UUID.class));
                        refValueMap.put("tableId", resultSet.getObject("table_id", UUID.class));
                        refValueMap.put("valueCode", resultSet.getString("value_code"));
                        refValueMap.put("valueDesc", resultSet.getString("value_desc"));
                        refValueMap.put("startTime", resultSet.getObject("start_time") != null ? resultSet.getObject("start_time", OffsetDateTime.class) : null);
                        refValueMap.put("endTime", resultSet.getObject("end_time") != null ? resultSet.getObject("end_time", OffsetDateTime.class) : null);
                        refValueMap.put("displayOrder", resultSet.getInt("display_order"));
                        refValueMap.put("active", resultSet.getBoolean("active"));
                        refValueMap.put("updateUser", resultSet.getString("update_user"));
                        refValueMap.put("updateTs", resultSet.getObject("update_ts") != null ? resultSet.getObject("update_ts", OffsetDateTime.class) : null);
                    }
                }
                // Check if map was populated (i.e., record found)
                if (refValueMap != null && !refValueMap.isEmpty()) {
                    result = Success.of(JsonMapper.toJson(refValueMap));
                } else {
                    // Record not found
                    result = Failure.of(new Status(OBJECT_NOT_FOUND, "valueId = " + valueId));
                }
            }
            // No commit/rollback needed for SELECT
        } catch (SQLException e) {
            logger.error("SQLException getting reference value by id {}:", valueId, e);
            result = Failure.of(new Status(SQL_EXCEPTION, e.getMessage()));
        } catch (Exception e) { // Catch other potential runtime exceptions
            logger.error("Unexpected exception getting reference value by id {}:", valueId, e);
            result = Failure.of(new Status(GENERIC_EXCEPTION, e.getMessage()));
        }
        return result;
    }

    @Override
    public Result<String> getRefValueLabel(String tableId) {
        Result<String> result = null;
        // Select value_id (for 'id') and value_code (for 'label')
        // Filter by table_id and only include active values, order for display
        final String sql = "SELECT value_id, value_code FROM ref_value_t " +
                "WHERE table_id = ? AND active = TRUE " +
                "ORDER BY display_order, value_code";
        List<Map<String, Object>> labels = new ArrayList<>(); // Initialize list for labels

        try (Connection connection = ds.getConnection();
             PreparedStatement preparedStatement = connection.prepareStatement(sql)) {

            preparedStatement.setObject(1, UUID.fromString(tableId));

            try (ResultSet resultSet = preparedStatement.executeQuery()) {
                // Iterate through results and build the label map list
                while (resultSet.next()) {
                    Map<String, Object> map = new HashMap<>();
                    map.put("id", resultSet.getString("value_id"));    // Key "id"
                    map.put("label", resultSet.getString("value_code")); // Key "label"
                    labels.add(map);
                }
            }
            // Serialize the list of labels to JSON and return Success
            result = Success.of(JsonMapper.toJson(labels));

        } catch (SQLException e) {
            logger.error("SQLException getting reference value labels for tableId {}:", tableId, e);
            result = Failure.of(new Status(SQL_EXCEPTION, e.getMessage()));
        } catch (Exception e) { // Catch other potential runtime exceptions
            logger.error("Unexpected exception getting reference value labels for tableId {}:", tableId, e);
            result = Failure.of(new Status(GENERIC_EXCEPTION, e.getMessage()));
        }
        return result;
    }

    @Override
    public Result<String> createRefLocale(Map<String, Object> event) {
        // SQL statement for inserting into value_locale_t
        final String sql = "INSERT INTO value_locale_t(value_id, language, value_label) " +
                "VALUES (?, ?, ?)";
        Result<String> result;
        Map<String, Object> map = (Map<String, Object>)event.get(PortalConstants.DATA);
        String valueId = (String) map.get("valueId");   // Get valueId for FK and return
        String language = (String) map.get("language"); // Get language for PK and return

        // Basic check for required fields (Primary Key parts)
        if (valueId == null || language == null) {
            logger.error("Missing required fields (valueId, language) in data payload for createRefLocale: {}", map);
            return Failure.of(new Status("ERR_MISSING_LOCALE_FIELDS", "valueId or language missing in createRefLocale data"));
        }
        // Construct a unique identifier string for the success result
        String createdId = valueId + ":" + language;

        try (Connection conn = ds.getConnection()) {
            conn.setAutoCommit(false);
            try (PreparedStatement statement = conn.prepareStatement(sql)) {

                // 1. value_id (Required, part of PK)
                statement.setObject(1, UUID.fromString(valueId));

                // 2. language (Required, part of PK)
                statement.setString(2, language);

                // 3. value_label (Optional)
                String valueLabel = (String) map.get("valueLabel");
                if (valueLabel != null && !valueLabel.isEmpty()) {
                    statement.setString(3, valueLabel);
                } else {
                    statement.setNull(3, Types.VARCHAR); // NULL for no label
                }

                // Execute insert
                int count = statement.executeUpdate();
                if (count == 0) {
                    throw new SQLException("failed to insert the value locale with id " + createdId);
                }

                // Success path
                conn.commit();
                result =  Success.of(createdId); // Return composite identifier
                insertNotification(event, true, null);

            } catch (SQLException e) {
                // Check for duplicate key violation (PK = value_id, language)
                if ("23505".equals(e.getSQLState())) { // Standard SQLState for unique violation
                    logger.error("Duplicate value locale entry for {}: {}", createdId, e.getMessage());
                    conn.rollback(); // Rollback on duplicate
                    insertNotification(event, false, "Duplicate entry for " + createdId);
                    result = Failure.of(new Status("ERR_DUPLICATE_LOCALE", "Value locale already exists for " + createdId, e.getMessage()));
                } else {
                    logger.error("SQLException during value locale creation transaction for {}:", createdId, e);
                    conn.rollback();
                    insertNotification(event, false, e.getMessage());
                    result = Failure.of(new Status(SQL_EXCEPTION, e.getMessage()));
                }
            } catch (Exception e) { // Catch other potential runtime exceptions
                logger.error("Unexpected exception during value locale creation transaction for {}:", createdId, e);
                conn.rollback();
                insertNotification(event, false, e.getMessage());
                result = Failure.of(new Status(GENERIC_EXCEPTION, e.getMessage()));
            }
        } catch (SQLException e) { // Errors getting connection or setting auto-commit
            logger.error("SQLException setting up connection/transaction for value locale creation:", e);
            result = Failure.of(new Status(SQL_EXCEPTION, e.getMessage()));
        }
        return result;
    }

    @Override
    public Result<String> updateRefLocale(Map<String, Object> event) {
        // SQL statement for updating value_locale_t
        final String sql = "UPDATE value_locale_t SET value_label = ? " +
                "WHERE value_id = ? AND language = ?";
        Result<String> result;
        Map<String, Object> map = (Map<String, Object>)event.get(PortalConstants.DATA);
        String valueId = (String) map.get("valueId");   // Get valueId for WHERE clause and return
        String language = (String) map.get("language"); // Get language for WHERE clause and return

        // Basic check for required fields (Primary Key parts)
        if (valueId == null || language == null) {
            logger.error("Missing required fields (valueId, language) in data payload for updateRefLocale: {}", map);
            return Failure.of(new Status("ERR_MISSING_LOCALE_KEYS", "valueId or language missing in updateRefLocale data"));
        }
        String updatedId = valueId + ":" + language; // Identifier for logging/return

        try (Connection conn = ds.getConnection()) {
            conn.setAutoCommit(false);
            try (PreparedStatement statement = conn.prepareStatement(sql)) {

                // 1. value_label (Optional - Set if present, otherwise NULL)
                String valueLabel = (String) map.get("valueLabel");
                if (valueLabel != null && !valueLabel.isEmpty()) {
                    statement.setString(1, valueLabel);
                } else {
                    statement.setNull(1, Types.VARCHAR); // NULL for no label
                }

                // 2. value_id (For WHERE clause)
                statement.setObject(2, UUID.fromString(valueId));

                // 3. language (For WHERE clause)
                statement.setString(3, language);


                // Execute update
                int count = statement.executeUpdate();
                if (count == 0) {
                    // Record not found to update
                    throw new SQLException("failed to update the value locale with id " + updatedId + " - record not found.");
                }

                // Success path
                conn.commit();
                result =  Success.of(updatedId); // Return composite identifier
                insertNotification(event, true, null);

            } catch (SQLException e) {
                logger.error("SQLException during value locale update transaction for {}:", updatedId, e);
                conn.rollback();
                insertNotification(event, false, e.getMessage());
                result = Failure.of(new Status(SQL_EXCEPTION, e.getMessage()));
            } catch (Exception e) { // Catch other potential runtime exceptions
                logger.error("Unexpected exception during value locale update transaction for {}:", updatedId, e);
                conn.rollback();
                insertNotification(event, false, e.getMessage());
                result = Failure.of(new Status(GENERIC_EXCEPTION, e.getMessage()));
            }
        } catch (SQLException e) { // Errors getting connection or setting auto-commit
            logger.error("SQLException setting up connection/transaction for value locale update:", e);
            result = Failure.of(new Status(SQL_EXCEPTION, e.getMessage()));
        }
        return result;
    }

    @Override
    public Result<String> deleteRefLocale(Map<String, Object> event) {
        // SQL statement for deleting from value_locale_t using the composite key
        final String sql = "DELETE FROM value_locale_t WHERE value_id = ? AND language = ?";
        Result<String> result;
        Map<String, Object> map = (Map<String, Object>)event.get(PortalConstants.DATA);
        String valueId = (String) map.get("valueId");   // Get valueId for WHERE clause and return
        String language = (String) map.get("language"); // Get language for WHERE clause and return

        // Basic check for required fields (Primary Key parts)
        if (valueId == null || language == null) {
            logger.error("Missing required fields (valueId, language) in data payload for deleteRefLocale: {}", map);
            return Failure.of(new Status("ERR_MISSING_LOCALE_KEYS", "valueId or language missing in deleteRefLocale data"));
        }
        String deletedId = valueId + ":" + language; // Identifier for logging/return

        try (Connection conn = ds.getConnection()) {
            conn.setAutoCommit(false);
            try (PreparedStatement statement = conn.prepareStatement(sql)) {

                // Set parameters for the WHERE clause
                statement.setObject(1, UUID.fromString(valueId));
                statement.setString(2, language);

                // Execute delete
                int count = statement.executeUpdate();
                if (count == 0) {
                    // Record not found to delete. Following template by throwing.
                    throw new SQLException("failed to delete the value locale with id " + deletedId + " - record not found.");
                }

                // Success path
                conn.commit();
                result =  Success.of(deletedId); // Return composite identifier
                insertNotification(event, true, null);

            } catch (SQLException e) {
                logger.error("SQLException during value locale delete transaction for {}:", deletedId, e);
                conn.rollback();
                insertNotification(event, false, e.getMessage());
                result = Failure.of(new Status(SQL_EXCEPTION, e.getMessage()));
            } catch (Exception e) { // Catch other potential runtime exceptions
                logger.error("Unexpected exception during value locale delete transaction for {}:", deletedId, e);
                conn.rollback();
                insertNotification(event, false, e.getMessage());
                result = Failure.of(new Status(GENERIC_EXCEPTION, e.getMessage()));
            }
        } catch (SQLException e) { // Errors getting connection or setting auto-commit
            logger.error("SQLException setting up connection/transaction for value locale delete:", e);
            result = Failure.of(new Status(SQL_EXCEPTION, e.getMessage()));
        }
        return result;
    }

    @Override
    public Result<String> getRefLocale(int offset, int limit, String valueId, String language, String valueLabel) {
        Result<String> result = null;
        StringBuilder sqlBuilder = new StringBuilder();
        // Include COUNT for total and select columns from value_locale_t
        sqlBuilder.append("SELECT COUNT(*) OVER () AS total,\n" +
                "value_id, language, value_label\n" +
                "FROM value_locale_t\n" +
                "WHERE 1=1\n"); // Start WHERE clause

        List<Object> parameters = new ArrayList<>();
        StringBuilder whereClause = new StringBuilder();

        // Add conditions based on input parameters using the helper
        addCondition(whereClause, parameters, "value_id", valueId != null ? UUID.fromString(valueId) : null);
        addCondition(whereClause, parameters, "language", language);
        addCondition(whereClause, parameters, "value_label", valueLabel); // Might need LIKE for labels

        // Append the dynamic WHERE conditions if any were added
        if (!whereClause.isEmpty()) {
            sqlBuilder.append("AND ").append(whereClause);
        }

        // Add ordering and pagination
        sqlBuilder.append(" ORDER BY value_id, language\n" + // Sensible default order
                "LIMIT ? OFFSET ?");

        parameters.add(limit);  // Add limit parameter
        parameters.add(offset); // Add offset parameter

        String sql = sqlBuilder.toString();
        int total = 0; // Variable to store total count
        List<Map<String, Object>> locales = new ArrayList<>(); // List to hold results

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
                    map.put("valueId", resultSet.getObject("value_id", UUID.class));
                    map.put("language", resultSet.getString("language"));
                    map.put("valueLabel", resultSet.getString("value_label"));

                    locales.add(map); // Add map to the list
                }
            }

            // Prepare the final result map containing total count and the list of locales
            Map<String, Object> resultMap = new HashMap<>();
            resultMap.put("total", total);
            resultMap.put("locales", locales); // Use a descriptive key
            result = Success.of(JsonMapper.toJson(resultMap)); // Serialize and return Success

        } catch (SQLException e) {
            logger.error("SQLException getting reference locales:", e);
            result = Failure.of(new Status(SQL_EXCEPTION, e.getMessage()));
        } catch (Exception e) { // Catch other potential runtime exceptions
            logger.error("Unexpected exception getting reference locales:", e);
            result = Failure.of(new Status(GENERIC_EXCEPTION, e.getMessage()));
        }
        return result;
    }


    @Override
    public Result<String> createRefRelationType(Map<String, Object> event) {
        // SQL statement for inserting into relation_type_t
        final String sql = "INSERT INTO relation_type_t(relation_id, relation_name, relation_desc, update_user, update_ts) " +
                "VALUES (?, ?, ?, ?, ?)";
        Result<String> result;
        Map<String, Object> map = (Map<String, Object>)event.get(PortalConstants.DATA);
        String relationId = (String) map.get("relationId"); // Get relationId for PK, return, logging

        // Basic check for required fields from the table definition
        if (relationId == null || map.get("relationName") == null || map.get("relationDesc") == null) {
            logger.error("Missing required fields (relationId, relationName, relationDesc) in data payload for createRefRelationType: {}", map);
            return Failure.of(new Status("ERR_MISSING_REL_TYPE_FIELDS", "relationId, relationName, or relationDesc missing in createRefRelationType data"));
        }

        try (Connection conn = ds.getConnection()) {
            conn.setAutoCommit(false);
            try (PreparedStatement statement = conn.prepareStatement(sql)) {

                // 1. relation_id (Required)
                statement.setObject(1, UUID.fromString(relationId));
                // 2. relation_name (Required)
                statement.setString(2, (String)map.get("relationName"));
                // 3. relation_desc (Required)
                statement.setString(3, (String)map.get("relationDesc"));

                // 4. update_user (From event metadata)
                statement.setString(4, (String)event.get(Constants.USER));

                // 5. update_ts (From event metadata)
                statement.setObject(5, OffsetDateTime.parse((String)event.get(CloudEventV1.TIME)));


                // Execute insert
                int count = statement.executeUpdate();
                if (count == 0) {
                    throw new SQLException("failed to insert the relation type with id " + relationId);
                }

                // Success path
                conn.commit();
                result =  Success.of(relationId); // Return relationId
                insertNotification(event, true, null);

            } catch (SQLException e) {
                // Check for duplicate key violation (PK = relation_id OR unique relation_name)
                if ("23505".equals(e.getSQLState())) { // Standard SQLState for unique violation
                    logger.error("Duplicate relation type entry for ID {} or Name '{}': {}", relationId, map.get("relationName"), e.getMessage());
                    conn.rollback(); // Rollback on duplicate
                    insertNotification(event, false, "Duplicate entry for relation type " + relationId + " or name " + map.get("relationName"));
                    result = Failure.of(new Status("ERR_DUPLICATE_REL_TYPE", "Relation type already exists with ID " + relationId + " or name " + map.get("relationName"), e.getMessage()));
                } else {
                    logger.error("SQLException during relation type creation transaction for {}:", relationId, e);
                    conn.rollback();
                    insertNotification(event, false, e.getMessage());
                    result = Failure.of(new Status(SQL_EXCEPTION, e.getMessage()));
                }
            } catch (Exception e) { // Catch other potential runtime exceptions
                logger.error("Unexpected exception during relation type creation transaction for {}:", relationId, e);
                conn.rollback();
                insertNotification(event, false, e.getMessage());
                result = Failure.of(new Status(GENERIC_EXCEPTION, e.getMessage()));
            }
        } catch (SQLException e) { // Errors getting connection or setting auto-commit
            logger.error("SQLException setting up connection/transaction for relation type creation:", e);
            result = Failure.of(new Status(SQL_EXCEPTION, e.getMessage()));
        }
        return result;
    }

    @Override
    public Result<String> updateRefRelationType(Map<String, Object> event) {
        // SQL statement for updating relation_type_t
        final String sql = "UPDATE relation_type_t SET relation_name = ?, relation_desc = ?, " +
                "update_user = ?, update_ts = ? WHERE relation_id = ?";
        Result<String> result;
        Map<String, Object> map = (Map<String, Object>)event.get(PortalConstants.DATA);
        String relationId = (String) map.get("relationId"); // Get relationId for WHERE clause and return

        // Basic check for required fields needed for update
        if (relationId == null || map.get("relationName") == null || map.get("relationDesc") == null) {
            logger.error("Missing required fields (relationId, relationName, relationDesc) in data payload for updateRefRelationType: {}", map);
            return Failure.of(new Status("ERR_MISSING_REL_TYPE_UPDATE_FIELDS", "relationId, relationName, or relationDesc missing in updateRefRelationType data"));
        }

        try (Connection conn = ds.getConnection()) {
            conn.setAutoCommit(false);
            try (PreparedStatement statement = conn.prepareStatement(sql)) {

                // Set parameters for the UPDATE statement
                // 1. relation_name (Required)
                statement.setString(1, (String)map.get("relationName"));
                // 2. relation_desc (Required)
                statement.setString(2, (String)map.get("relationDesc"));

                // 3. update_user (From event metadata)
                statement.setString(3, (String)event.get(Constants.USER));

                // 4. update_ts (From event metadata)
                statement.setObject(4, OffsetDateTime.parse((String)event.get(CloudEventV1.TIME)));

                // 5. relation_id (For WHERE clause - Required)
                statement.setObject(5, UUID.fromString(relationId));


                // Execute update
                int count = statement.executeUpdate();
                if (count == 0) {
                    // Record not found to update
                    throw new SQLException("failed to update the relation type with id " + relationId + " - record not found.");
                }

                // Success path
                conn.commit();
                result =  Success.of(relationId); // Return relationId
                insertNotification(event, true, null);

            } catch (SQLException e) {
                // Check for duplicate key violation (unique relation_name)
                if ("23505".equals(e.getSQLState())) {
                    logger.error("Duplicate relation type name '{}' conflict for ID {}: {}", map.get("relationName"), relationId, e.getMessage());
                    conn.rollback();
                    insertNotification(event, false, "Duplicate entry for relation type name " + map.get("relationName"));
                    result = Failure.of(new Status("ERR_DUPLICATE_REL_TYPE_NAME", "Relation type name '" + map.get("relationName") + "' already exists.", e.getMessage()));
                } else {
                    logger.error("SQLException during relation type update transaction for {}:", relationId, e);
                    conn.rollback();
                    insertNotification(event, false, e.getMessage());
                    result = Failure.of(new Status(SQL_EXCEPTION, e.getMessage()));
                }
            } catch (Exception e) { // Catch other potential runtime exceptions
                logger.error("Unexpected exception during relation type update transaction for {}:", relationId, e);
                conn.rollback();
                insertNotification(event, false, e.getMessage());
                result = Failure.of(new Status(GENERIC_EXCEPTION, e.getMessage()));
            }
        } catch (SQLException e) { // Errors getting connection or setting auto-commit
            logger.error("SQLException setting up connection/transaction for relation type update:", e);
            result = Failure.of(new Status(SQL_EXCEPTION, e.getMessage()));
        }
        return result;
    }

    @Override
    public Result<String> deleteRefRelationType(Map<String, Object> event) {
        // SQL statement for deleting from relation_type_t
        final String sql = "DELETE FROM relation_type_t WHERE relation_id = ?";
        Result<String> result;
        Map<String, Object> map = (Map<String, Object>)event.get(PortalConstants.DATA);
        String relationId = (String) map.get("relationId"); // Get relationId for WHERE clause and return

        // Basic check for required field
        if (relationId == null) {
            logger.error("Missing required field 'relationId' in data payload for deleteRefRelationType: {}", map);
            return Failure.of(new Status("ERR_MISSING_REL_TYPE_DELETE_ID", "'relationId' missing in deleteRefRelationType data"));
        }

        try (Connection conn = ds.getConnection()) {
            conn.setAutoCommit(false);
            try (PreparedStatement statement = conn.prepareStatement(sql)) {

                // Set parameter for the WHERE clause
                statement.setObject(1, UUID.fromString(relationId));

                // Execute delete
                int count = statement.executeUpdate();
                if (count == 0) {
                    // Record not found to delete. Following template by throwing.
                    throw new SQLException("failed to delete the relation type with id " + relationId + " - record not found.");
                }

                // Success path
                conn.commit();
                result =  Success.of(relationId); // Return relationId
                insertNotification(event, true, null);

            } catch (SQLException e) {
                logger.error("SQLException during relation type delete transaction for {}:", relationId, e);
                conn.rollback();
                insertNotification(event, false, e.getMessage());
                result = Failure.of(new Status(SQL_EXCEPTION, e.getMessage()));
            } catch (Exception e) { // Catch other potential runtime exceptions
                logger.error("Unexpected exception during relation type delete transaction for {}:", relationId, e);
                conn.rollback();
                insertNotification(event, false, e.getMessage());
                result = Failure.of(new Status(GENERIC_EXCEPTION, e.getMessage()));
            }
        } catch (SQLException e) { // Errors getting connection or setting auto-commit
            logger.error("SQLException setting up connection/transaction for relation type delete:", e);
            result = Failure.of(new Status(SQL_EXCEPTION, e.getMessage()));
        }
        return result;
    }

    @Override
    public Result<String> getRefRelationType(int offset, int limit, String relationId, String relationName,
                                             String relationDesc) {
        Result<String> result = null;
        StringBuilder sqlBuilder = new StringBuilder();
        // Select all columns from relation_type_t and include total count
        sqlBuilder.append("SELECT COUNT(*) OVER () AS total,\n" +
                "relation_id, relation_name, relation_desc, update_user, update_ts\n" +
                "FROM relation_type_t\n" +
                "WHERE 1=1\n"); // Start WHERE clause

        List<Object> parameters = new ArrayList<>();
        StringBuilder whereClause = new StringBuilder();

        // Add conditions based on input parameters using the helper
        addCondition(whereClause, parameters, "relation_id", relationId != null ? UUID.fromString(relationId) : null);
        addCondition(whereClause, parameters, "relation_name", relationName);
        addCondition(whereClause, parameters, "relation_desc", relationDesc); // Might need LIKE

        // Append the dynamic WHERE conditions if any were added
        if (!whereClause.isEmpty()) {
            sqlBuilder.append("AND ").append(whereClause);
        }

        // Add ordering and pagination
        sqlBuilder.append(" ORDER BY relation_name\n" + // Sensible default order
                "LIMIT ? OFFSET ?");

        parameters.add(limit);  // Add limit parameter
        parameters.add(offset); // Add offset parameter

        String sql = sqlBuilder.toString();
        int total = 0; // Variable to store total count
        List<Map<String, Object>> relationTypes = new ArrayList<>(); // List to hold results

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
                    map.put("relationId", resultSet.getObject("relation_id", UUID.class));
                    map.put("relationName", resultSet.getString("relation_name"));
                    map.put("relationDesc", resultSet.getString("relation_desc"));
                    map.put("updateUser", resultSet.getString("update_user"));
                    map.put("updateTs", resultSet.getObject("update_ts") != null ? resultSet.getObject("update_ts", OffsetDateTime.class) : null);

                    relationTypes.add(map); // Add map to the list
                }
            }

            // Prepare the final result map containing total count and the list of relation types
            Map<String, Object> resultMap = new HashMap<>();
            resultMap.put("total", total);
            resultMap.put("relationTypes", relationTypes); // Use a descriptive key
            result = Success.of(JsonMapper.toJson(resultMap)); // Serialize and return Success

        } catch (SQLException e) {
            logger.error("SQLException getting reference relation types:", e);
            result = Failure.of(new Status(SQL_EXCEPTION, e.getMessage()));
        } catch (Exception e) { // Catch other potential runtime exceptions
            logger.error("Unexpected exception getting reference relation types:", e);
            result = Failure.of(new Status(GENERIC_EXCEPTION, e.getMessage()));
        }
        return result;
    }


    @Override
    public Result<String> createRefRelation(Map<String, Object> event) {
        // SQL statement for inserting into relation_t
        final String sql = "INSERT INTO relation_t(relation_id, value_id_from, value_id_to, active, update_user, update_ts) " +
                "VALUES (?, ?, ?, ?, ?, ?)";
        Result<String> result;
        Map<String, Object> map = (Map<String, Object>)event.get(PortalConstants.DATA);
        String relationId = (String) map.get("relationId");     // Part of PK
        String valueIdFrom = (String) map.get("valueIdFrom"); // Part of PK
        String valueIdTo = (String) map.get("valueIdTo");     // Part of PK

        // Basic check for required fields (Primary Key parts)
        if (relationId == null || valueIdFrom == null || valueIdTo == null) {
            logger.error("Missing required fields (relationId, valueIdFrom, valueIdTo) in data payload for createRefRelation: {}", map);
            return Failure.of(new Status("ERR_MISSING_REL_KEYS", "relationId, valueIdFrom, or valueIdTo missing in createRefRelation data"));
        }
        // Construct a unique identifier string for the success result/logging
        String createdId = relationId + ":" + valueIdFrom + ":" + valueIdTo;

        try (Connection conn = ds.getConnection()) {
            conn.setAutoCommit(false);
            try (PreparedStatement statement = conn.prepareStatement(sql)) {

                // 1. relation_id (Required, part of PK)
                statement.setObject(1, UUID.fromString(relationId));
                // 2. value_id_from (Required, part of PK)
                statement.setObject(2, UUID.fromString(valueIdFrom));
                // 3. value_id_to (Required, part of PK)
                statement.setObject(3, UUID.fromString(valueIdTo));

                // 4. active (Optional Boolean - handle default)
                if (map.containsKey("active") && map.get("active") instanceof Boolean) {
                    statement.setBoolean(4, (Boolean) map.get("active"));
                } else {
                    statement.setBoolean(4, true); // Default value from schema is TRUE
                }

                // 5. update_user (From event metadata)
                statement.setString(5, (String)event.get(Constants.USER));

                // 6. update_ts (From event metadata)
                statement.setObject(6, OffsetDateTime.parse((String)event.get(CloudEventV1.TIME)));


                // Execute insert
                int count = statement.executeUpdate();
                if (count == 0) {
                    throw new SQLException("failed to insert the relation with id " + createdId);
                }

                // Success path
                conn.commit();
                result =  Success.of(createdId); // Return composite identifier
                insertNotification(event, true, null);

            } catch (SQLException e) {
                // Check for duplicate key violation (PK = relation_id, value_id_from, value_id_to)
                if ("23505".equals(e.getSQLState())) { // Standard SQLState for unique violation
                    logger.error("Duplicate relation entry for {}: {}", createdId, e.getMessage());
                    conn.rollback(); // Rollback on duplicate
                    insertNotification(event, false, "Duplicate entry for relation " + createdId);
                    result = Failure.of(new Status("ERR_DUPLICATE_RELATION", "Relation already exists for " + createdId, e.getMessage()));
                } else {
                    logger.error("SQLException during relation creation transaction for {}:", createdId, e);
                    conn.rollback();
                    insertNotification(event, false, e.getMessage());
                    result = Failure.of(new Status(SQL_EXCEPTION, e.getMessage()));
                }
            } catch (Exception e) { // Catch other potential runtime exceptions
                logger.error("Unexpected exception during relation creation transaction for {}:", createdId, e);
                conn.rollback();
                insertNotification(event, false, e.getMessage());
                result = Failure.of(new Status(GENERIC_EXCEPTION, e.getMessage()));
            }
        } catch (SQLException e) { // Errors getting connection or setting auto-commit
            logger.error("SQLException setting up connection/transaction for relation creation:", e);
            result = Failure.of(new Status(SQL_EXCEPTION, e.getMessage()));
        }
        return result;
    }

    @Override
    public Result<String> updateRefRelation(Map<String, Object> event) {
        // SQL statement for updating relation_t
        // Only active, update_user, update_ts are typically mutable for a relation
        final String sql = "UPDATE relation_t SET active = ?, update_user = ?, update_ts = ? " +
                "WHERE relation_id = ? AND value_id_from = ? AND value_id_to = ?";
        Result<String> result;
        Map<String, Object> map = (Map<String, Object>)event.get(PortalConstants.DATA);
        String relationId = (String) map.get("relationId");     // Part of PK for WHERE
        String valueIdFrom = (String) map.get("valueIdFrom"); // Part of PK for WHERE
        String valueIdTo = (String) map.get("valueIdTo");     // Part of PK for WHERE

        // Basic check for required fields (Primary Key parts)
        if (relationId == null || valueIdFrom == null || valueIdTo == null) {
            logger.error("Missing required fields (relationId, valueIdFrom, valueIdTo) in data payload for updateRefRelation: {}", map);
            return Failure.of(new Status("ERR_MISSING_REL_KEYS_UPDATE", "relationId, valueIdFrom, or valueIdTo missing in updateRefRelation data"));
        }
        // Construct a unique identifier string for the success result/logging
        String updatedId = relationId + ":" + valueIdFrom + ":" + valueIdTo;

        try (Connection conn = ds.getConnection()) {
            conn.setAutoCommit(false);
            try (PreparedStatement statement = conn.prepareStatement(sql)) {

                // Set parameters for the UPDATE statement
                // 1. active (Optional Boolean - handle default/presence)
                if (map.containsKey("active") && map.get("active") instanceof Boolean) {
                    statement.setBoolean(1, (Boolean) map.get("active"));
                } else {
                    // Decide update behavior: set default? Or assume not changing if missing?
                    // Setting default like template here:
                    statement.setBoolean(1, true);
                }

                // 2. update_user (From event metadata)
                statement.setString(2, (String)event.get(Constants.USER));

                // 3. update_ts (From event metadata)
                statement.setObject(3, OffsetDateTime.parse((String)event.get(CloudEventV1.TIME)));

                // Set parameters for the WHERE clause (PK parts)
                // 4. relation_id
                statement.setObject(4, UUID.fromString(relationId));
                // 5. value_id_from
                statement.setObject(5, UUID.fromString(valueIdFrom));
                // 6. value_id_to
                statement.setObject(6, UUID.fromString(valueIdTo));


                // Execute update
                int count = statement.executeUpdate();
                if (count == 0) {
                    // Record not found to update
                    throw new SQLException("failed to update the relation with id " + updatedId + " - record not found.");
                }

                // Success path
                conn.commit();
                result =  Success.of(updatedId); // Return composite identifier
                insertNotification(event, true, null);

            } catch (SQLException e) {
                logger.error("SQLException during relation update transaction for {}:", updatedId, e);
                conn.rollback();
                insertNotification(event, false, e.getMessage());
                result = Failure.of(new Status(SQL_EXCEPTION, e.getMessage()));
            } catch (Exception e) { // Catch other potential runtime exceptions
                logger.error("Unexpected exception during relation update transaction for {}:", updatedId, e);
                conn.rollback();
                insertNotification(event, false, e.getMessage());
                result = Failure.of(new Status(GENERIC_EXCEPTION, e.getMessage()));
            }
        } catch (SQLException e) { // Errors getting connection or setting auto-commit
            logger.error("SQLException setting up connection/transaction for relation update:", e);
            result = Failure.of(new Status(SQL_EXCEPTION, e.getMessage()));
        }
        return result;
    }

    @Override
    public Result<String> deleteRefRelation(Map<String, Object> event) {
        // SQL statement for deleting from relation_t using the composite key
        final String sql = "DELETE FROM relation_t WHERE relation_id = ? AND value_id_from = ? AND value_id_to = ?";
        Result<String> result;
        Map<String, Object> map = (Map<String, Object>)event.get(PortalConstants.DATA);
        String relationId = (String) map.get("relationId");     // Part of PK for WHERE
        String valueIdFrom = (String) map.get("valueIdFrom"); // Part of PK for WHERE
        String valueIdTo = (String) map.get("valueIdTo");     // Part of PK for WHERE

        // Basic check for required fields (Primary Key parts)
        if (relationId == null || valueIdFrom == null || valueIdTo == null) {
            logger.error("Missing required fields (relationId, valueIdFrom, valueIdTo) in data payload for deleteRefRelation: {}", map);
            return Failure.of(new Status("ERR_MISSING_REL_KEYS_DELETE", "relationId, valueIdFrom, or valueIdTo missing in deleteRefRelation data"));
        }
        // Construct a unique identifier string for the success result/logging
        String deletedId = relationId + ":" + valueIdFrom + ":" + valueIdTo;

        try (Connection conn = ds.getConnection()) {
            conn.setAutoCommit(false);
            try (PreparedStatement statement = conn.prepareStatement(sql)) {

                // Set parameters for the WHERE clause (PK parts)
                statement.setObject(1, UUID.fromString(relationId));
                statement.setObject(2, UUID.fromString(valueIdFrom));
                statement.setObject(3, UUID.fromString(valueIdTo));

                // Execute delete
                int count = statement.executeUpdate();
                if (count == 0) {
                    // Record not found to delete. Following template by throwing.
                    throw new SQLException("failed to delete the relation with id " + deletedId + " - record not found.");
                }

                // Success path
                conn.commit();
                result =  Success.of(deletedId); // Return composite identifier
                insertNotification(event, true, null);

            } catch (SQLException e) {
                logger.error("SQLException during relation delete transaction for {}:", deletedId, e);
                conn.rollback();
                insertNotification(event, false, e.getMessage());
                result = Failure.of(new Status(SQL_EXCEPTION, e.getMessage()));
            } catch (Exception e) { // Catch other potential runtime exceptions
                logger.error("Unexpected exception during relation delete transaction for {}:", deletedId, e);
                conn.rollback();
                insertNotification(event, false, e.getMessage());
                result = Failure.of(new Status(GENERIC_EXCEPTION, e.getMessage()));
            }
        } catch (SQLException e) { // Errors getting connection or setting auto-commit
            logger.error("SQLException setting up connection/transaction for relation delete:", e);
            result = Failure.of(new Status(SQL_EXCEPTION, e.getMessage()));
        }
        return result;
    }

    @Override
    public Result<String> getRefRelation(int offset, int limit, String relationId, String valueIdFrom, String valueIdTo,
                                         Boolean active) {
        Result<String> result = null;
        StringBuilder sqlBuilder = new StringBuilder();
        // Select all columns from relation_t and include total count
        sqlBuilder.append("SELECT COUNT(*) OVER () AS total,\n" +
                "relation_id, value_id_from, value_id_to, active, update_user, update_ts\n" +
                "FROM relation_t\n" +
                "WHERE 1=1\n"); // Start WHERE clause

        List<Object> parameters = new ArrayList<>();
        StringBuilder whereClause = new StringBuilder();

        // Add conditions based on input parameters using the helper
        addCondition(whereClause, parameters, "relation_id", relationId != null ? UUID.fromString(relationId) : null);
        addCondition(whereClause, parameters, "value_id_from", valueIdFrom != null ? UUID.fromString(valueIdFrom) : null);
        addCondition(whereClause, parameters, "value_id_to", valueIdTo != null ? UUID.fromString(valueIdTo) : null);
        addCondition(whereClause, parameters, "active", active);


        // Append the dynamic WHERE conditions if any were added
        if (!whereClause.isEmpty()) {
            sqlBuilder.append("AND ").append(whereClause);
        }

        // Add ordering and pagination
        sqlBuilder.append(" ORDER BY relation_id, value_id_from, value_id_to\n" + // Order by PK
                "LIMIT ? OFFSET ?");

        parameters.add(limit);  // Add limit parameter
        parameters.add(offset); // Add offset parameter

        String sql = sqlBuilder.toString();
        int total = 0; // Variable to store total count
        List<Map<String, Object>> relations = new ArrayList<>(); // List to hold results

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
                    map.put("relationId", resultSet.getObject("relation_id", UUID.class));
                    map.put("valueIdFrom", resultSet.getObject("value_id_from", UUID.class));
                    map.put("valueIdTo", resultSet.getObject("value_id_to", UUID.class));
                    map.put("active", resultSet.getBoolean("active"));
                    map.put("updateUser", resultSet.getString("update_user"));
                    map.put("updateTs", resultSet.getObject("update_ts") != null ? resultSet.getObject("update_ts", OffsetDateTime.class) : null);

                    relations.add(map); // Add map to the list
                }
            }

            // Prepare the final result map containing total count and the list of relations
            Map<String, Object> resultMap = new HashMap<>();
            resultMap.put("total", total);
            resultMap.put("relations", relations); // Use a descriptive key
            result = Success.of(JsonMapper.toJson(resultMap)); // Serialize and return Success

        } catch (SQLException e) {
            logger.error("SQLException getting reference relations:", e);
            result = Failure.of(new Status(SQL_EXCEPTION, e.getMessage()));
        } catch (Exception e) { // Catch other potential runtime exceptions
            logger.error("Unexpected exception getting reference relations:", e);
            result = Failure.of(new Status(GENERIC_EXCEPTION, e.getMessage()));
        }
        return result;
    }

    @Override
    public Result<String> queryRefTable(int offset, int limit, String hostId, String tableName, String tableDesc, String active, String editable, String common) {
        Result<String> result;
        String sql = """
                SELECT COUNT(*) OVER () AS total,
                               rt.table_id,
                               rt.table_name,
                               rt.table_desc,
                               rt.active,
                               rt.editable,
                               rt.common,
                               rht.host_id
                        FROM ref_table_t rt
                        JOIN ref_host_t rht ON rt.table_id = rht.table_id
                        WHERE rht.host_id = ?
                        AND rt.active = ?
                        AND rt.editable = ?
                        AND (
                            rt.common = ?
                                OR  rht.host_id = ?
                        )
                        AND (
                            ? IS NULL OR ? = '*' OR rt.table_name LIKE '%' || ? || '%'
                        )
                        AND (
                            ? IS NULL OR ? = '*' OR rt.table_desc LIKE '%' || ? || '%'
                        )
                        ORDER BY rt.table_name
                        LIMIT ? OFFSET ?;""";

        int total = 0;
        List<Map<String, Object>> tables = new ArrayList<>();

        try (Connection connection = ds.getConnection();
             PreparedStatement preparedStatement = connection.prepareStatement(sql)) {
            preparedStatement.setObject(1, UUID.fromString(hostId));
            preparedStatement.setString(2, active);
            preparedStatement.setString(3, editable);
            preparedStatement.setString(4, common);
            preparedStatement.setObject(5, UUID.fromString(hostId));
            preparedStatement.setString(6, tableName);
            preparedStatement.setString(7, tableName);
            preparedStatement.setString(8, tableName);
            preparedStatement.setString(9, tableDesc);
            preparedStatement.setString(10, tableDesc);
            preparedStatement.setString(11, tableDesc);
            preparedStatement.setInt(12, limit);
            preparedStatement.setInt(13, offset);

            boolean isFirstRow = true;
            try (ResultSet resultSet = preparedStatement.executeQuery()) {
                while (resultSet.next()) {
                    Map<String, Object> map = new HashMap<>();
                    // only get the total once as it is the same for all rows.
                    if (isFirstRow) {
                        total = resultSet.getInt("total");
                        isFirstRow = false;
                    }

                    map.put("tableId", resultSet.getObject("table_id", UUID.class));
                    map.put("tableName", resultSet.getString("table_name"));
                    map.put("tableDesc", resultSet.getString("table_desc"));
                    map.put("active", resultSet.getString("active"));
                    map.put("editable", resultSet.getString("editable"));
                    map.put("common", resultSet.getString("common"));
                    map.put("hostId", resultSet.getObject("host_id", UUID.class));
                    tables.add(map);
                }
            }
            // now, we have the total and the list of tables, we need to put them into a map.
            Map<String, Object> resultMap = new HashMap<>();
            resultMap.put("total", total);
            resultMap.put("tables", tables);
            result = Success.of(JsonMapper.toJson(resultMap));
        } catch (SQLException e) {
            logger.error("SQLException:", e);
            result = Failure.of(new Status(SQL_EXCEPTION, e.getMessage()));
        } catch (Exception e) {
            logger.error("Exception:", e);
            result = Failure.of(new Status(GENERIC_EXCEPTION, e.getMessage()));
        }
        return result;
    }

    @Override
    public Result<String> loginUserByEmail(String email) {
        Result<String> result;
        String sql = """
                SELECT
                    uh.host_id,
                    u.user_id,
                    u.email,
                    u.user_type,
                    u.password,
                    u.verified,
                    CASE
                        WHEN u.user_type = 'E' THEN e.employee_id
                        WHEN u.user_type = 'C' THEN c.customer_id
                        ELSE NULL
                    END AS entity_id,
                    CASE WHEN u.user_type = 'E' THEN string_agg(DISTINCT p.position_id, ' ' ORDER BY p.position_id) ELSE NULL END AS positions,
                    string_agg(DISTINCT r.role_id, ' ' ORDER BY r.role_id) AS roles,
                    string_agg(DISTINCT g.group_id, ' ' ORDER BY g.group_id) AS groups,
                     CASE
                        WHEN COUNT(DISTINCT at.attribute_id || '^=^' || aut.attribute_value) > 0 THEN string_agg(DISTINCT at.attribute_id || '^=^' || aut.attribute_value, '~' ORDER BY at.attribute_id || '^=^' || aut.attribute_value)
                        ELSE NULL
                    END AS attributes
                FROM
                    user_t AS u
                LEFT JOIN
                    user_host_t AS uh ON u.user_id = uh.user_id
                LEFT JOIN
                    role_user_t AS ru ON u.user_id = ru.user_id
                LEFT JOIN
                    role_t AS r ON ru.host_id = r.host_id AND ru.role_id = r.role_id
                LEFT JOIN
                    attribute_user_t AS aut ON u.user_id = aut.user_id
                LEFT JOIN
                    attribute_t AS at ON aut.host_id = at.host_id AND aut.attribute_id = at.attribute_id
                LEFT JOIN
                    group_user_t AS gu ON u.user_id = gu.user_id
                LEFT JOIN
                    group_t AS g ON gu.host_id = g.host_id AND gu.group_id = g.group_id
                LEFT JOIN
                    employee_t AS e ON uh.host_id = e.host_id AND u.user_id = e.user_id
                LEFT JOIN
                    customer_t AS c ON uh.host_id = c.host_id AND u.user_id = c.user_id
                LEFT JOIN
                    employee_position_t AS ep ON e.host_id = ep.host_id AND e.employee_id = ep.employee_id
                LEFT JOIN
                    position_t AS p ON ep.host_id = p.host_id AND ep.position_id = p.position_id
                WHERE
                    u.email = ?
                    AND u.locked = FALSE
                    AND u.verified = TRUE
                GROUP BY
                    uh.host_id, u.user_id, u.user_type, e.employee_id, c.customer_id;
                """;
        try (final Connection conn = ds.getConnection()) {
            Map<String, Object> map = new HashMap<>();
            try (PreparedStatement statement = conn.prepareStatement(sql)) {
                statement.setString(1, email);
                try (ResultSet resultSet = statement.executeQuery()) {
                    if (resultSet.next()) {
                        map.put("hostId", resultSet.getObject("host_id", UUID.class));
                        map.put("userId", resultSet.getObject("user_id", UUID.class));
                        map.put("email", resultSet.getString("email"));
                        map.put("userType", resultSet.getString("user_type"));
                        map.put("entityId", resultSet.getString("entity_id"));
                        map.put("password", resultSet.getString("password"));
                        map.put("verified", resultSet.getBoolean("verified"));
                        map.put("positions", resultSet.getString("positions"));
                        map.put("roles", resultSet.getString("roles"));
                        map.put("groups", resultSet.getString("groups"));
                        map.put("attributes", resultSet.getString("attributes"));
                    }
                }
            }
            if (map.isEmpty())
                result = Failure.of(new Status(OBJECT_NOT_FOUND, "user", email));
            else
                result = Success.of(JsonMapper.toJson(map));
        } catch (SQLException e) {
            logger.error("SQLException:", e);
            result = Failure.of(new Status(SQL_EXCEPTION, e.getMessage()));
        } catch (Exception e) {
            logger.error("Exception:", e);
            result = Failure.of(new Status(GENERIC_EXCEPTION, e.getMessage()));
        }
        return result;
    }

    @Override
    public Result<String> queryUserByEmail(String email) {
        Result<String> result = null;
        String sql =
                "SELECT h.host_id, u.user_id, u.email, u.password, u.language, \n" +
                        "u.first_name, u.last_name, u.user_type, u.phone_number, u.gender,\n" +
                        "u.birthday, u.country, u.province, u.city, u.address,\n" +
                        "u.post_code, u.verified, u.token, u.locked, u.nonce \n" +
                        "FROM user_t u, user_host_t h\n" +
                        "WHERE u.user_id = h.user_id\n" +
                        "AND email = ?";
        try (final Connection conn = ds.getConnection()) {
            Map<String, Object> map = new HashMap<>();
            try (PreparedStatement statement = conn.prepareStatement(sql)) {
                statement.setString(1, email);
                try (ResultSet resultSet = statement.executeQuery()) {
                    if (resultSet.next()) {
                        map.put("hostId", resultSet.getObject("host_id", UUID.class));
                        map.put("userId", resultSet.getObject("user_id", UUID.class));
                        map.put("email", resultSet.getString("email"));
                        map.put("password", resultSet.getString("password"));
                        map.put("language", resultSet.getString("language"));

                        map.put("firstName", resultSet.getString("first_name"));
                        map.put("lastName", resultSet.getString("last_name"));
                        map.put("userType", resultSet.getString("user_type"));
                        map.put("phoneNumber", resultSet.getString("phone_number"));
                        map.put("gender", resultSet.getString("gender"));

                        map.put("birthday", resultSet.getDate("birthday"));
                        map.put("country", resultSet.getString("country"));
                        map.put("province", resultSet.getString("province"));
                        map.put("city", resultSet.getString("city"));
                        map.put("address", resultSet.getString("address"));

                        map.put("postCode", resultSet.getString("post_code"));
                        map.put("verified", resultSet.getBoolean("verified"));
                        map.put("token", resultSet.getString("token"));
                        map.put("locked", resultSet.getBoolean("locked"));
                        map.put("nonce", resultSet.getLong("nonce"));
                    }
                }
            }
            if (map.size() == 0)
                result = Failure.of(new Status(OBJECT_NOT_FOUND, "user", email));
            else
                result = Success.of(JsonMapper.toJson(map));
        } catch (SQLException e) {
            logger.error("SQLException:", e);
            result = Failure.of(new Status(SQL_EXCEPTION, e.getMessage()));
        } catch (Exception e) {
            logger.error("Exception:", e);
            result = Failure.of(new Status(GENERIC_EXCEPTION, e.getMessage()));
        }
        return result;
    }

    @Override
    public Result<String> queryUserById(String userId) {
        Result<String> result = null;

        String sql = """
                SELECT h.host_id, u.user_id, u.email, u.password, u.language,
                u.first_name, u.last_name, u.user_type, u.phone_number, u.gender,
                u.birthday, u.country, u.province, u.city, u.address,
                u.post_code, u.verified, u.token, u.locked, u.nonce
                FROM user_t u, user_host_t h
                WHERE u.user_id = h.user_id
                AND u.user_id = ?
                """;

        try (final Connection conn = ds.getConnection()) {
            Map<String, Object> map = new HashMap();
            try (PreparedStatement statement = conn.prepareStatement(sql)) {
                statement.setObject(1, UUID.fromString(userId));
                try (ResultSet resultSet = statement.executeQuery()) {
                    if (resultSet.next()) {
                        map.put("hostId", resultSet.getObject("host_id", UUID.class));
                        map.put("userId", resultSet.getObject("user_id", UUID.class));
                        map.put("email", resultSet.getString("email"));
                        map.put("password", resultSet.getString("password"));
                        map.put("language", resultSet.getString("language"));

                        map.put("firstName", resultSet.getString("first_name"));
                        map.put("lastName", resultSet.getString("last_name"));
                        map.put("userType", resultSet.getString("user_type"));
                        map.put("phoneNumber", resultSet.getString("phone_number"));
                        map.put("gender", resultSet.getString("gender"));

                        map.put("birthday", resultSet.getDate("birthday"));
                        map.put("country", resultSet.getString("country"));
                        map.put("province", resultSet.getString("province"));
                        map.put("city", resultSet.getString("city"));
                        map.put("address", resultSet.getString("address"));

                        map.put("postCode", resultSet.getString("post_code"));
                        map.put("verified", resultSet.getBoolean("verified"));
                        map.put("token", resultSet.getString("token"));
                        map.put("locked", resultSet.getBoolean("locked"));
                        map.put("nonce", resultSet.getLong("nonce"));
                    }
                }
            }
            if (map.size() == 0)
                result = Failure.of(new Status(OBJECT_NOT_FOUND, "user", userId));
            else
                result = Success.of(JsonMapper.toJson(map));
        } catch (SQLException e) {
            logger.error("SQLException:", e);
            result = Failure.of(new Status(SQL_EXCEPTION, e.getMessage()));
        } catch (Exception e) {
            logger.error("Exception:", e);
            result = Failure.of(new Status(GENERIC_EXCEPTION, e.getMessage()));
        }
        return result;
    }

    @Override
    public Result<String> queryUserByTypeEntityId(String userType, String entityId) {
        Result<String> result = null;

        String sqlEmployee = """
                SELECT h.host_id, u.user_id, e.employee_id as entity_id, u.email, u.password,
                u.language, u.first_name, u.last_name, u.user_type, u.phone_number,
                u.gender, u.birthday, u.country, u.province, u.city,
                u.address, u.post_code, u.verified, u.token, u.locked,
                u.nonce
                FROM user_t u, user_host_t h, employee_t e
                WHERE u.user_id = h.user_id
                AND h.host_id = e.host_id
                AND h.user_id = e.user_id
                AND e.employee_id = ?
                """;

        String sqlCustomer =
                "SELECT h.host_id, u.user_id, c.customer_id as entity_id, u.email, u.password, \n" +
                "u.language, u.first_name, u.last_name, u.user_type, u.phone_number, \n" +
                "u.gender, u.birthday, u.country, u.province, u.city, \n" +
                "u.address, u.post_code, u.verified, u.token, u.locked, \n" +
                "u.nonce\n" +
                "FROM user_t u, user_host_t h, customer_t c\n" +
                "WHERE u.user_id = h.user_id\n" +
                "AND h.host_id = c.host_id\n" +
                "AND h.user_id = c.user_id\n" +
                "AND c.customer_id = ? \n";

        String sql = userType.equals("E") ? sqlEmployee : sqlCustomer;
        try (final Connection conn = ds.getConnection()) {
            Map<String, Object> map = new HashMap<>();
            try (PreparedStatement statement = conn.prepareStatement(sql)) {
                statement.setString(1, entityId);
                try (ResultSet resultSet = statement.executeQuery()) {
                    if (resultSet.next()) {
                        map.put("hostId", resultSet.getObject("host_id", UUID.class));
                        map.put("userId", resultSet.getObject("user_id", UUID.class));
                        map.put("entityId", resultSet.getString("entity_id"));
                        map.put("email", resultSet.getString("email"));
                        map.put("password", resultSet.getString("password"));

                        map.put("language", resultSet.getString("language"));
                        map.put("firstName", resultSet.getString("first_name"));
                        map.put("lastName", resultSet.getString("last_name"));
                        map.put("userType", resultSet.getString("user_type"));
                        map.put("phoneNumber", resultSet.getString("phone_number"));

                        map.put("gender", resultSet.getString("gender"));
                        map.put("birthday", resultSet.getDate("birthday"));
                        map.put("country", resultSet.getString("country"));
                        map.put("province", resultSet.getString("province"));
                        map.put("city", resultSet.getString("city"));

                        map.put("address", resultSet.getString("address"));
                        map.put("postCode", resultSet.getString("post_code"));
                        map.put("verified", resultSet.getBoolean("verified"));
                        map.put("token", resultSet.getString("token"));
                        map.put("locked", resultSet.getBoolean("locked"));

                        map.put("nonce", resultSet.getLong("nonce"));
                    }
                }
            }
            if (map.size() == 0)
                result = Failure.of(new Status(OBJECT_NOT_FOUND, "entityId", entityId));
            else
                result = Success.of(JsonMapper.toJson(map));
        } catch (SQLException e) {
            logger.error("SQLException:", e);
            result = Failure.of(new Status(SQL_EXCEPTION, e.getMessage()));
        } catch (Exception e) {
            logger.error("Exception:", e);
            result = Failure.of(new Status(GENERIC_EXCEPTION, e.getMessage()));
        }
        return result;
    }

    @Override
    public Result<String> queryUserByWallet(String cryptoType, String cryptoAddress) {
        Result<String> result = null;
        String sql =
                "SELECT h.host_id, u.user_id, u.email, u.password, u.language, \n" +
                        "u.first_name, u.last_name, u.user_type, u.phone_number, u.gender,\n" +
                        "u.birthday, u.country, u.province, u.city, u.address,\n" +
                        "u.post_code, u.verified, u.token, u.locked, u.nonce \n" +
                        "FROM user_t u, user_host_t h, user_crypto_wallet_t w\n" +
                        "WHERE u.user_id = h.user_id\n" +
                        "AND u.user_id = w.user_id\n" +
                        "AND w.crypto_type = ?\n" +
                        "AND w.crypto_address = ?";
        try (final Connection conn = ds.getConnection()) {
            Map<String, Object> map = new HashMap();
            try (PreparedStatement statement = conn.prepareStatement(sql)) {
                statement.setString(1, cryptoType);
                statement.setString(2, cryptoAddress);
                try (ResultSet resultSet = statement.executeQuery()) {
                    if (resultSet.next()) {
                        map.put("hostId", resultSet.getObject("host_id", UUID.class));
                        map.put("userId", resultSet.getObject("user_id", UUID.class));
                        map.put("firstName", resultSet.getInt("first_name"));
                        map.put("lastName", resultSet.getString("last_name"));
                        map.put("email", resultSet.getString("email"));
                        map.put("language", resultSet.getString("language"));
                        map.put("gender", resultSet.getString("gender"));
                        map.put("birthday", resultSet.getString("birthday"));
                        map.put("taijiWallet", resultSet.getString("taiji_wallet"));
                        map.put("country", resultSet.getString("country"));
                        map.put("province", resultSet.getString("province"));
                        map.put("city", resultSet.getString("city"));
                        map.put("postCode", resultSet.getString("post_code"));
                        map.put("address", resultSet.getString("address"));
                        map.put("verified", resultSet.getBoolean("verified"));
                        map.put("token", resultSet.getString("token"));
                        map.put("locked", resultSet.getBoolean("locked"));
                    }
                }
            }
            if (map.isEmpty())
                result = Failure.of(new Status(OBJECT_NOT_FOUND, "user", cryptoType + cryptoAddress));
            else
                result = Success.of(JsonMapper.toJson(map));
        } catch (SQLException e) {
            logger.error("SQLException:", e);
            result = Failure.of(new Status(SQL_EXCEPTION, e.getMessage()));
        } catch (Exception e) {
            logger.error("Exception:", e);
            result = Failure.of(new Status(GENERIC_EXCEPTION, e.getMessage()));
        }
        return result;
    }

    @Override
    public Result<String> queryUserByHostId(int offset, int limit, String hostId, String email, String language, String userType,
                                     String entityId, String referralId, String managerId, String firstName, String lastName,
                                     String phoneNumber, String gender, String birthday, String country, String province, String city,
                                     String address, String postCode, Boolean verified, Boolean locked) {
        Result<String> result = null;
        StringBuilder sqlBuilder = new StringBuilder();
        sqlBuilder.append("SELECT COUNT(*) OVER () AS total,\n" +
                "uh.host_id, u.user_id, u.email, u.language, u.first_name, u.last_name, u.user_type, u.phone_number, " +
                "u.gender, u.birthday, u.country, u.province, u.city, u.address, u.post_code, u.verified, u.locked,\n" +
                "COALESCE(c.customer_id, e.employee_id) AS entity_id, c.referral_id, e.manager_id\n" +
                "FROM user_t u\n" +
                "LEFT JOIN user_host_t uh ON u.user_id = uh.user_id\n" +
                "LEFT JOIN customer_t c ON uh.host_id = c.host_id AND u.user_id = c.user_id\n" +
                "LEFT JOIN employee_t e ON uh.host_id = e.host_id AND u.user_id = e.user_id\n" +
                "WHERE uh.host_id = ?\n");

        List<Object> parameters = new ArrayList<>();
        parameters.add(UUID.fromString(hostId));

        StringBuilder whereClause = new StringBuilder();

        addCondition(whereClause, parameters, "u.email", email);
        addCondition(whereClause, parameters, "u.language", language);
        addCondition(whereClause, parameters, "u.user_type", userType);
        addCondition(whereClause, parameters, "COALESCE(c.customer_id, e.employee_id)", entityId); // Using COALESCE here
        addCondition(whereClause, parameters, "c.referral_id", referralId);
        addCondition(whereClause, parameters, "e.manager_id", managerId);
        addCondition(whereClause, parameters, "u.first_name", firstName);
        addCondition(whereClause, parameters, "u.last_name", lastName);
        addCondition(whereClause, parameters, "u.phone_number", phoneNumber);
        addCondition(whereClause, parameters, "u.gender", gender);
        addCondition(whereClause, parameters, "u.birthday", birthday);
        addCondition(whereClause, parameters, "u.country", country);
        addCondition(whereClause, parameters, "u.province", province);
        addCondition(whereClause, parameters, "u.city", city);
        addCondition(whereClause, parameters, "u.address", address);
        addCondition(whereClause, parameters, "u.post_code", postCode);
        addCondition(whereClause, parameters, "u.verified", verified);
        addCondition(whereClause, parameters, "u.locked", locked);

        if (!whereClause.isEmpty()) {
            sqlBuilder.append("AND ").append(whereClause);
        }

        sqlBuilder.append(" ORDER BY u.last_name\n" +
                "LIMIT ? OFFSET ?");

        parameters.add(limit);
        parameters.add(offset);

        String sql = sqlBuilder.toString();
        if(logger.isDebugEnabled()) logger.debug("sql = {}", sql);
        int total = 0;
        List<Map<String, Object>> users = new ArrayList<>();

        try (Connection connection = ds.getConnection();
             PreparedStatement preparedStatement = connection.prepareStatement(sql)) {
            for (int i = 0; i < parameters.size(); i++) {
                preparedStatement.setObject(i + 1, parameters.get(i));
            }


            boolean isFirstRow = true;
            try (ResultSet resultSet = preparedStatement.executeQuery()) {
                while (resultSet.next()) {
                    Map<String, Object> map = new HashMap<>();

                    if (isFirstRow) {
                        total = resultSet.getInt("total");
                        isFirstRow = false;
                    }
                    map.put("hostId", resultSet.getObject("host_id", UUID.class));
                    map.put("userId", resultSet.getObject("user_id", UUID.class));
                    map.put("email", resultSet.getString("email"));
                    map.put("language", resultSet.getString("language"));
                    map.put("userType", resultSet.getString("user_type"));
                    map.put("firstName", resultSet.getString("first_name"));
                    map.put("lastName", resultSet.getString("last_name"));
                    map.put("phoneNumber", resultSet.getString("phone_number"));
                    map.put("gender", resultSet.getString("gender"));
                    // handling date properly
                    map.put("birthday", resultSet.getDate("birthday") != null ? resultSet.getDate("birthday").toString() : null);
                    map.put("country", resultSet.getString("country"));
                    map.put("province", resultSet.getString("province"));
                    map.put("city", resultSet.getString("city"));
                    map.put("address", resultSet.getString("address"));
                    map.put("postCode", resultSet.getString("post_code"));
                    map.put("verified", resultSet.getBoolean("verified"));
                    map.put("locked", resultSet.getBoolean("locked"));
                    map.put("entityId", resultSet.getString("entity_id"));
                    map.put("referralId", resultSet.getString("referral_id"));
                    map.put("managerId", resultSet.getString("manager_id"));

                    users.add(map);
                }
            }

            Map<String, Object> resultMap = new HashMap<>();
            resultMap.put("total", total);
            resultMap.put("users", users);
            result = Success.of(JsonMapper.toJson(resultMap));
        } catch (SQLException e) {
            logger.error("SQLException:", e);
            result = Failure.of(new Status(SQL_EXCEPTION, e.getMessage()));
        } catch (Exception e) {
            logger.error("Exception:", e);
            result = Failure.of(new Status(GENERIC_EXCEPTION, e.getMessage()));
        }

        return result;
    }

    @Override
    public Result<String> queryNotification(int offset, int limit, String hostId, String userId, Long nonce, String eventClass, Boolean successFlag,
                                            Timestamp processTs, String eventJson, String error) {
        Result<String> result = null;
        StringBuilder sqlBuilder = new StringBuilder();
        sqlBuilder.append("SELECT COUNT(*) OVER () AS total,\n" +
                "host_id, user_id, nonce, event_class, is_processed, process_ts, event_json, error\n" +
                "FROM notification_t\n" +
                "WHERE host_id = ?\n");

        List<Object> parameters = new ArrayList<>();
        parameters.add(UUID.fromString(hostId));

        StringBuilder whereClause = new StringBuilder();

        addCondition(whereClause, parameters, "user_id", userId != null ? UUID.fromString(userId) : null);
        addCondition(whereClause, parameters, "nonce", nonce);
        addCondition(whereClause, parameters, "event_class", eventClass);
        addCondition(whereClause, parameters, "is_processed", successFlag);
        addCondition(whereClause, parameters, "event_json", eventJson);
        addCondition(whereClause, parameters, "error", error);

        if (whereClause.length() > 0) {
            sqlBuilder.append("AND ").append(whereClause);
        }

        sqlBuilder.append(" ORDER BY process_ts DESC\n" +
                "LIMIT ? OFFSET ?");

        parameters.add(limit);
        parameters.add(offset);

        String sql = sqlBuilder.toString();
        int total = 0;
        List<Map<String, Object>> notifications = new ArrayList<>();

        try (Connection connection = ds.getConnection();
             PreparedStatement preparedStatement = connection.prepareStatement(sql)) {

            for (int i = 0; i < parameters.size(); i++) {
                preparedStatement.setObject(i + 1, parameters.get(i));
            }


            boolean isFirstRow = true;
            try (ResultSet resultSet = preparedStatement.executeQuery()) {
                while (resultSet.next()) {
                    Map<String, Object> map = new HashMap<>();
                    if (isFirstRow) {
                        total = resultSet.getInt("total");
                        isFirstRow = false;
                    }
                    map.put("hostId", resultSet.getObject("host_id", UUID.class));
                    map.put("userId", resultSet.getObject("user_id", UUID.class));
                    map.put("nonce", resultSet.getLong("nonce"));
                    map.put("eventClass", resultSet.getString("event_class"));
                    map.put("processFlag", resultSet.getBoolean("is_processed"));
                    // handling date properly
                    map.put("processTs", resultSet.getTimestamp("process_ts") != null ? resultSet.getTimestamp("process_ts").toString() : null);
                    map.put("eventJson", resultSet.getString("event_json"));
                    map.put("error", resultSet.getString("error"));
                    notifications.add(map);
                }
            }


            Map<String, Object> resultMap = new HashMap<>();
            resultMap.put("total", total);
            resultMap.put("notifications", notifications);
            result = Success.of(JsonMapper.toJson(resultMap));


        } catch (SQLException e) {
            logger.error("SQLException:", e);
            result = Failure.of(new Status(SQL_EXCEPTION, e.getMessage()));
        } catch (Exception e) {
            logger.error("Exception:", e);
            result = Failure.of(new Status(GENERIC_EXCEPTION, e.getMessage()));
        }
        return result;
    }

    @Override
    public Result<String> queryEmailByWallet(String cryptoType, String cryptoAddress) {
        Result<String> result = null;
        String sql = """
                SELECT email
                FROM user_t u, user_crypto_wallet_t w
                WHERE u.user_id = w.user_id
                AND w.crypto_type = ?
                AND w.crypto_address = ?
                """;
        try (final Connection conn = ds.getConnection()) {
            String email = null;
            try (PreparedStatement statement = conn.prepareStatement(sql)) {
                statement.setString(1, cryptoType);
                statement.setString(2, cryptoAddress);
                try (ResultSet resultSet = statement.executeQuery()) {
                    if (resultSet.next()) {
                        email = resultSet.getString("email");
                    }
                }
            }
            if (email == null)
                result = Failure.of(new Status(OBJECT_NOT_FOUND, "user email", cryptoType + cryptoAddress));
            else
                result = Success.of(email);
        } catch (SQLException e) {
            logger.error("SQLException:", e);
            result = Failure.of(new Status(SQL_EXCEPTION, e.getMessage()));
        } catch (Exception e) {
            logger.error("Exception:", e);
            result = Failure.of(new Status(GENERIC_EXCEPTION, e.getMessage()));
        }
        return result;
    }

    /**
     * insert notification into database using different connection and transaction.
     *
     * @param event The map of the cloud event
     * @param flag   The flag of the notification
     * @param error  The error message of the notification
     * @throws SQLException when there is an error in the database access
     */
    public void insertNotification(Map<String, Object> event, boolean flag, String error) throws SQLException {
        try (Connection conn = ds.getConnection();
            PreparedStatement statement = conn.prepareStatement(INSERT_NOTIFICATION)) {
            statement.setObject(1, UUID.fromString((String)event.get(CloudEventV1.ID)));
            statement.setObject(2, UUID.fromString((String)event.get(Constants.HOST)));
            statement.setObject(3, UUID.fromString((String)event.get(Constants.USER)));
            statement.setLong(4, ((Number)event.get(PortalConstants.NONCE)).longValue());
            statement.setString(5, (String)event.get(CloudEventV1.TYPE));
            statement.setString(6, JsonMapper.toJson(event));
            statement.setObject(7, OffsetDateTime.parse((String)event.get(CloudEventV1.TIME)));
            statement.setBoolean(8, flag);
            if (error != null && !error.isEmpty()) {
                statement.setString(9, error);
            } else {
                statement.setNull(9, NULL);
            }
            statement.executeUpdate();
        } catch (SQLException e) {
            logger.error("SQLException:", e);
            throw new RuntimeException(e);
        }
    }

    /**
     * check the email, user_id, is unique. if not, write an error notification. If yes, insert
     * the user into database and write a success notification.
     *
     * @param event event that is created by user service
     * @return result of email
     */
    @Override
    public Result<String> createUser(Map<String, Object> event) {

        final String queryEmailEntityId = """
                SELECT u.user_id, u.email, COALESCE(c.customer_id, e.employee_id) AS entity_id
                FROM user_t u
                LEFT JOIN user_host_t uh ON u.user_id = uh.user_id
                LEFT JOIN customer_t c ON uh.host_id = c.host_id AND u.user_id = c.user_id
                LEFT JOIN employee_t e ON uh.host_id = e.host_id AND u.user_id = e.user_id
                WHERE
                    (u.email = ? OR COALESCE(c.customer_id, e.employee_id) = ?)
                    AND u.user_type IN ('C', 'E')
                """;

        final String insertUser = "INSERT INTO user_t (user_id, email, password, language, first_name, " +
                "last_name, user_type, phone_number, gender, birthday, " +
                "country, province, city, address, post_code, " +
                "verified, token, locked) " +
                "VALUES (?, ?, ?, ?, ?,   ?, ?, ?, ?, ?,   ?, ?, ?, ?, ?,  ?, ?, ?)";

        final String insertUserHost = "INSERT INTO user_host_t (user_id, host_id) VALUES (?, ?)";

        final String insertCustomer = "INSERT INTO customer_t (host_id, customer_id, user_id, referral_id) " +
                "VALUES (?, ?, ?, ?)";

        final String insertEmployee = "INSERT INTO employee_t (host_id, employee_id, user_id, manager_id) " +
                "VALUES (?, ?, ?, ?)";


        Result<String> result = null;
        Map<String, Object> map = (Map<String, Object>)event.get(PortalConstants.DATA);
        try (Connection conn = ds.getConnection()){
            conn.setAutoCommit(false);
            try {
                try (PreparedStatement statement = conn.prepareStatement(queryEmailEntityId)) {
                    statement.setString(1, (String)map.get("email"));
                    statement.setString(2, (String)map.get("entityId"));
                    try (ResultSet resultSet = statement.executeQuery()) {
                        if (resultSet.next()) {
                            // found duplicate record, write an error notification.
                            logger.error("entityId {} or email {} already exists in database.", map.get("entityId"), map.get("email"));
                            throw new SQLException(String.format("entityId %s or email %s already exists in database.", map.get("entityId"), map.get("email")));
                        }
                    }
                }

                // no duplicate record, insert the user into database and write a success notification.
                try (PreparedStatement statement = conn.prepareStatement(insertUser)) {
                    statement.setObject(1, UUID.fromString((String)map.get("userId")));
                    statement.setString(2, (String)map.get("email"));
                    statement.setString(3, (String)map.get("password"));
                    statement.setString(4, (String)map.get("language"));
                    String firstName = (String)map.get("firstName");
                    if (firstName != null && !firstName.isEmpty())
                        statement.setString(5, firstName);
                    else
                        statement.setNull(5, NULL);

                    String lastName = (String)map.get("lastName");
                    if (lastName != null && !lastName.isEmpty())
                        statement.setString(6, lastName);
                    else
                        statement.setNull(6, NULL);

                    statement.setString(7, (String)map.get("userType"));

                    String phoneNumber = (String)map.get("phoneNumber");
                    if (phoneNumber != null && !phoneNumber.isEmpty())
                        statement.setString(8, phoneNumber);
                    else
                        statement.setNull(8, NULL);

                    String gender = (String) map.get("gender");
                    if (gender != null && !gender.isEmpty()) {
                        statement.setString(9, gender);
                    } else {
                        statement.setNull(9, NULL);
                    }

                    java.util.Date birthday = (java.util.Date)map.get("birthday");
                    if (birthday != null) {
                        statement.setDate(10, new java.sql.Date(birthday.getTime()));
                    } else {
                        statement.setNull(10, NULL);
                    }

                    String country = (String)map.get("country");
                    if (country != null && !country.isEmpty()) {
                        statement.setString(11, country);
                    } else {
                        statement.setNull(11, NULL);
                    }

                    String province = (String)map.get("province");
                    if (province != null && !province.isEmpty()) {
                        statement.setString(12, province);
                    } else {
                        statement.setNull(12, NULL);
                    }

                    String city = (String)map.get("city");
                    if (city != null && !city.isEmpty()) {
                        statement.setString(13, city);
                    } else {
                        statement.setNull(13, NULL);
                    }

                    String address = (String)map.get("address");
                    if (address != null && !address.isEmpty()) {
                        statement.setString(14, address);
                    } else {
                        statement.setNull(14, NULL);
                    }

                    String postCode = (String)map.get("postCode");
                    if (postCode != null && !postCode.isEmpty()) {
                        statement.setString(15, postCode);
                    } else {
                        statement.setNull(15, NULL);
                    }
                    statement.setBoolean(16, (Boolean)map.get("verified"));
                    statement.setString(17, (String)map.get("token"));
                    statement.setBoolean(18, (Boolean)map.get("locked"));
                    statement.execute();
                }
                try (PreparedStatement statement = conn.prepareStatement(insertUserHost)) {
                    statement.setObject(1, UUID.fromString((String)map.get("userId")));
                    statement.setObject(2, UUID.fromString((String)map.get("hostId")));
                    statement.execute();
                }
                // insert customer or employee based on user_type
                if(map.get("userType").equals("E")) {
                    try (PreparedStatement statement = conn.prepareStatement(insertEmployee)) {
                        statement.setObject(1, UUID.fromString((String)map.get("hostId")));
                        statement.setString(2, (String)map.get("entityId"));
                        statement.setObject(3, UUID.fromString((String)map.get("userId")));
                        String managerId = (String)map.get("managerId");
                        if(map.get("managerId") != null && !managerId.isEmpty()) {
                            statement.setString(4, managerId);
                        } else {
                            statement.setNull(4, NULL);
                        }
                        statement.execute();
                    }
                } else if(map.get("userType").equals("C")) {
                    try (PreparedStatement statement = conn.prepareStatement(insertCustomer)) {
                        statement.setObject(1, UUID.fromString((String)map.get("hostId")));
                        statement.setString(2, (String)map.get("entityId"));
                        statement.setObject(3, UUID.fromString((String)map.get("userId")));
                        String referralId = (String)map.get("referralId");
                        if(referralId != null && !referralId.isEmpty()) {
                            statement.setString(4, referralId);
                        } else {
                            statement.setNull(4, NULL);
                        }
                        statement.execute();
                    }
                } else {
                    throw new SQLException("user_type is not valid: " + map.get("userType"));
                }
                conn.commit();
                result = Success.of((String)map.get("userId"));
                insertNotification(event, true, null);
            } catch (SQLException e) {
                logger.error("SQLException:", e);
                conn.rollback();
                insertNotification(event, false, e.getMessage());
                result = Failure.of(new Status(SQL_EXCEPTION, e.getMessage()));
            } catch (Exception e) {
                logger.error("Exception:", e);
                conn.rollback();
                insertNotification(event, false, e.getMessage());
                result = Failure.of(new Status(GENERIC_EXCEPTION, e.getMessage()));
            }
        } catch (SQLException e) {
            logger.error("SQLException:", e);
            result = Failure.of(new Status(SQL_EXCEPTION, e.getMessage()));
        }
        return result;
    }


    @Override
    public Result<Integer> queryNonceByUserId(String userId){
        final String updateNonceSql = "UPDATE user_t SET nonce = nonce + 1 WHERE user_id = ? RETURNING nonce;";
        Result<Integer> result = null;
        try (Connection connection = ds.getConnection();
             PreparedStatement statement = connection.prepareStatement(updateNonceSql)) {
            Integer nonce = null;
            statement.setObject(1, UUID.fromString(userId));
            try (ResultSet resultSet = statement.executeQuery()) {
                if(resultSet.next()){
                    nonce = resultSet.getInt(1);
                }
            }
            if (nonce == null)
                result = Failure.of(new Status(OBJECT_NOT_FOUND, "user nonce", userId));
            else
                result = Success.of(nonce);

        } catch (SQLException e) {
            logger.error("SQLException:", e);
            result = Failure.of(new Status(SQL_EXCEPTION, e.getMessage()));
        } catch (Exception e) {
            logger.error("Exception:", e);
            result = Failure.of(new Status(GENERIC_EXCEPTION, e.getMessage()));
        }
        return result;
    }

    /**
     * check the input token with the saved token in user_t table to ensure match. If matched, update the verified to true
     * and nonce in the user_t table and a success notification. If not matched, write an error notification.
     *
     * @param event event that is created by user service
     * @return result of email
     */
    @Override
    public Result<String> confirmUser(Map<String, Object> event) {
        final String queryTokenByEmail = "SELECT token FROM user_t WHERE user_id = ? AND token = ?";
        final String updateUserByEmail = "UPDATE user_t SET token = null, verified = true, nonce = ? WHERE user_id = ?";
        Result<String> result;
        Map<String, Object> map = (Map<String, Object>)event.get(PortalConstants.DATA);
        try (Connection conn = ds.getConnection()){
            conn.setAutoCommit(false);
            try (PreparedStatement statement = conn.prepareStatement(queryTokenByEmail)) {
                statement.setString(1, (String)event.get(Constants.USER));
                statement.setString(2, (String)map.get("token"));
                try (ResultSet resultSet = statement.executeQuery()) {
                    if (resultSet.next()) {
                        // found the token record, update user_t for token, verified flog and nonce, write a success notification.
                        try (PreparedStatement updateStatement = conn.prepareStatement(updateUserByEmail)) {
                            updateStatement.setLong(1, ((Number)event.get(PortalConstants.NONCE)).longValue() + 1);
                            updateStatement.setString(2, (String)event.get(Constants.USER));
                            updateStatement.execute();
                        }
                    } else {
                        // record is not found with the email and token. write an error notification.
                        throw new SQLException(String.format("token %s is not matched for userId %s.", map.get("token"), event.get(Constants.USER)));
                    }
                }
                conn.commit();
                result = Success.of((String)event.get(Constants.USER));
                insertNotification(event, true, null);
            } catch (SQLException e) {
                logger.error("SQLException:", e);
                conn.rollback();
                insertNotification(event, false, e.getMessage());
                result = Failure.of(new Status(SQL_EXCEPTION, e.getMessage()));
            } catch (Exception e) {
                logger.error("Exception:", e);
                conn.rollback();
                insertNotification(event, false, e.getMessage());
                result = Failure.of(new Status(GENERIC_EXCEPTION, e.getMessage()));
            }
        } catch (SQLException e) {
            logger.error("SQLException:", e);
            result = Failure.of(new Status(SQL_EXCEPTION, e.getMessage()));
        }
        return result;
    }

    /**
     * Update the verified to true and nonce in the user_t table based on the hostId and userId. Write a success notification.
     *
     * @param event UserVerifiedEvent
     * @return  Result of userId
     */
    @Override
    public Result<String> verifyUser(Map<String, Object> event) {
        final String updateUserByUserId = "UPDATE user_t SET token = null, verified = true WHERE user_id = ?";
        Result<String> result;
        Map<String, Object> map = (Map<String, Object>)event.get(PortalConstants.DATA);
        try (Connection conn = ds.getConnection()){
            conn.setAutoCommit(false);
            try (PreparedStatement statement = conn.prepareStatement(updateUserByUserId)) {
                statement.setObject(1, UUID.fromString((String)map.get("userId")));
                statement.execute();
                conn.commit();
                result = Success.of((String)map.get("userId"));
                insertNotification(event, true, null);
            } catch (SQLException e) {
                logger.error("SQLException:", e);
                conn.rollback();
                insertNotification(event, false, e.getMessage());
                result = Failure.of(new Status(SQL_EXCEPTION, e.getMessage()));
            } catch (Exception e) {
                logger.error("Exception:", e);
                conn.rollback();
                insertNotification(event, false, e.getMessage());
                result = Failure.of(new Status(GENERIC_EXCEPTION, e.getMessage()));
            }
        } catch (SQLException e) {
            logger.error("SQLException:", e);
            result = Failure.of(new Status(SQL_EXCEPTION, e.getMessage()));
        }
        return result;

    }

    /**
     * check the email, user_id is unique. if not, write an error notification. If yes, insert
     * the user into database and write a success notification.
     *
     * @param event event that is created by user service
     * @return result of email
     */
    @Override
    public Result<String> createSocialUser(Map<String, Object> event) {
        final String queryIdEmail = "SELECT nonce FROM user_t WHERE user_id = ? OR email = ?";
        final String insertUser = "INSERT INTO user_t (host_id, user_id, first_name, last_name, email, language, " +
                "verified, gender, birthday, country, province, city, post_code, address) " +
                "VALUES (?, ?, ?, ?, ?,   ?, ?, ?, ?, ?,   ?, ?, ?, ?)";
        Result<String> result = null;
        Map<String, Object> map = (Map<String, Object>)event.get(PortalConstants.DATA);
        try(Connection conn = ds.getConnection()) {
            conn.setAutoCommit(false);
            try {
                try (PreparedStatement statement = conn.prepareStatement(queryIdEmail)) {
                    statement.setObject(1, UUID.fromString((String)map.get("userId")));
                    statement.setString(2, (String)map.get("email"));
                    try (ResultSet resultSet = statement.executeQuery()) {
                        if (resultSet.next()) {
                            // found duplicate record, write an error notification.
                            throw new SQLException(String.format("userId %s or email %s already exists in database.", map.get("userId"), map.get("email")));
                        }
                    }
                }
                // no duplicate record, insert the user into database and write a success notification.
                try (PreparedStatement statement = conn.prepareStatement(insertUser)) {
                    statement.setObject(1, UUID.fromString((String)map.get("hostId")));
                    statement.setObject(2, UUID.fromString((String)map.get("userId")));
                    String firstName = (String)map.get("firstName");
                    if (firstName != null && !firstName.isEmpty())
                        statement.setString(3, firstName);
                    else
                        statement.setNull(3, NULL);
                    String lastName = (String)map.get("lastName");
                    if (lastName != null && !lastName.isEmpty())
                        statement.setString(4, lastName);
                    else
                        statement.setNull(4, NULL);

                    statement.setString(5, (String)map.get("email"));
                    statement.setString(6, (String)map.get("language"));
                    statement.setBoolean(7, (Boolean)map.get("verified"));
                    String gender = (String)map.get("gender");
                    if (gender != null && !gender.isEmpty()) {
                        statement.setString(8, gender);
                    } else {
                        statement.setNull(8, NULL);
                    }
                    java.util.Date birthday = (java.util.Date) map.get("birthday");
                    if (birthday != null) {
                        statement.setDate(9, new java.sql.Date(birthday.getTime()));
                    } else {
                        statement.setNull(9, NULL);
                    }
                    String country = (String)map.get("country");
                    if (country != null && !country.isEmpty()) {
                        statement.setString(10, country);
                    } else {
                        statement.setNull(10, NULL);
                    }
                    String province = (String)map.get("province");
                    if (province != null && !province.isEmpty()) {
                        statement.setString(11, province);
                    } else {
                        statement.setNull(11, NULL);
                    }
                    String city = (String)map.get("city");
                    if (city != null && !city.isEmpty()) {
                        statement.setString(12, city);
                    } else {
                        statement.setNull(12, NULL);
                    }
                    String postCode = (String)map.get("postCode");
                    if (postCode != null && !postCode.isEmpty()) {
                        statement.setString(13, postCode);
                    } else {
                        statement.setNull(13, NULL);
                    }
                    String address = (String)map.get("address");
                    if (address != null && !address.isEmpty()) {
                        statement.setString(14, address);
                    } else {
                        statement.setNull(14, NULL);
                    }
                    statement.execute();
                }
                conn.commit();
                result = Success.of((String)map.get("userId"));
                insertNotification(event, true, null);
            } catch (SQLException e) {
                logger.error("SQLException:", e);
                conn.rollback();
                insertNotification(event, false, e.getMessage());
                result = Failure.of(new Status(SQL_EXCEPTION, e.getMessage()));
            } catch (Exception e) {
                logger.error("Exception:", e);
                conn.rollback();
                insertNotification(event, false, e.getMessage());
                result = Failure.of(new Status(GENERIC_EXCEPTION, e.getMessage()));
            }
        } catch (SQLException e) {
            logger.error("SQLException:", e);
            result = Failure.of(new Status(SQL_EXCEPTION, e.getMessage()));
        }
        return result;
    }

    /**
     * update user if it exists in database.
     *
     * @param event event that is created by user service
     * @return result of email
     */
    @Override
    public Result<String> updateUser(Map<String, Object> event) {
        final String updateUser = "UPDATE user_t SET language = ?, first_name = ?, last_name = ?, phone_number = ?," +
                "gender = ?, birthday = ?, country = ?, province = ?, city = ?, address = ?, post_code = ? " +
                "WHERE user_id = ?";
        final String updateCustomer = "UPDATE customer_t SET referral_id = ? WHERE host_id = ? AND customer_id = ?";
        final String updateEmployee = "UPDATE employee_t SET manager_id = ? WHERE host_id = ? AND employee_id = ?";
        Result<String> result = null;
        Map<String, Object> map = (Map<String, Object>)event.get(PortalConstants.DATA);
        try (Connection conn = ds.getConnection()){
            conn.setAutoCommit(false);

            // no duplicate record, insert the user into database and write a success notification.
            try (PreparedStatement statement = conn.prepareStatement(updateUser)) {
                statement.setString(1, (String)map.get("language"));
                String firstName = (String)map.get("firstName");
                if (firstName != null && !firstName.isEmpty())
                    statement.setString(2, firstName);
                else
                    statement.setNull(2, NULL);
                String lastName = (String)map.get("lastName");
                if (lastName != null && !lastName.isEmpty())
                    statement.setString(3, lastName);
                else
                    statement.setNull(3, NULL);
                String phoneNumber = (String)map.get("phoneNumber");
                if (phoneNumber != null && !phoneNumber.isEmpty())
                    statement.setString(4, phoneNumber);
                else
                    statement.setNull(4, NULL);
                String gender = (String)map.get("gender");
                if(gender != null && !gender.isEmpty()) {
                    statement.setString(5, gender);
                } else {
                    statement.setNull(5, NULL);
                }

                java.util.Date birthday = (java.util.Date) map.get("birthday");
                if (birthday != null) {
                    statement.setDate(6, new java.sql.Date(birthday.getTime()));
                } else {
                    statement.setNull(6, NULL);
                }

                String country = (String)map.get("country");
                if (country != null && !country.isEmpty()) {
                    statement.setString(7, country);
                } else {
                    statement.setNull(7, NULL);
                }

                String province = (String)map.get("province");
                if (province != null && !province.isEmpty()) {
                    statement.setString(8, province);
                } else {
                    statement.setNull(8, NULL);
                }

                String city = (String)map.get("city");
                if (city != null && !city.isEmpty()) {
                    statement.setString(9, city);
                } else {
                    statement.setNull(9, NULL);
                }

                String address = (String)map.get("address");
                if (address != null && !address.isEmpty()) {
                    statement.setString(10, address);
                } else {
                    statement.setNull(10, NULL);
                }

                String postCode = (String)map.get("postCode");
                if (postCode != null && !postCode.isEmpty()) {
                    statement.setString(11, postCode);
                } else {
                    statement.setNull(11, NULL);
                }
                statement.setObject(12, UUID.fromString((String)map.get("userId")));
                int count = statement.executeUpdate();
                if (count == 0) {
                    // no record is updated, write an error notification.
                    throw new SQLException(String.format("no record is updated by userId %s", map.get("userId")));
                }
                // TODO there are old country, province and city in the event for maproot, so we need to update them
                // update customer or employee based on user_type
                if(map.get("userType").equals("E")) {
                    try (PreparedStatement updateStatement = conn.prepareStatement(updateEmployee)) {
                        String managerId = (String)map.get("managerId");
                        if(managerId != null && !managerId.isEmpty()) {
                            updateStatement.setString(1, managerId);
                        } else {
                            updateStatement.setNull(1, NULL);
                        }
                        updateStatement.setObject(2, UUID.fromString((String)map.get("hostId")));
                        updateStatement.setString(3, (String)map.get("entityId"));
                        updateStatement.execute();
                    }
                } else if(map.get("userType").equals("C")) {
                    try (PreparedStatement updateStatement = conn.prepareStatement(updateCustomer)) {
                        String referralId = (String)map.get("referralId");
                        if(referralId != null && !referralId.isEmpty()) {
                            updateStatement.setString(1, referralId);
                        } else {
                            updateStatement.setNull(1, NULL);
                        }
                        updateStatement.setObject(2, UUID.fromString((String)map.get("hostId")));
                        updateStatement.setString(3, (String)map.get("entityId"));
                        updateStatement.execute();
                    }
                } else {
                    throw new SQLException("userType is not valid: " + map.get("userType"));
                }
                conn.commit();
                if(logger.isTraceEnabled()) logger.trace("update user success: {}", map.get("userId"));
                result = Success.of((String)map.get("userId"));
                insertNotification(event, true, null);
            } catch (SQLException e) {
                logger.error("SQLException:", e);
                conn.rollback();
                insertNotification(event, false, e.getMessage());
                result = Failure.of(new Status(SQL_EXCEPTION, e.getMessage()));
            } catch (Exception e) {
                logger.error("Exception:", e);
                conn.rollback();
                insertNotification(event, false, e.getMessage());
                result = Failure.of(new Status(GENERIC_EXCEPTION, e.getMessage()));
            }
        } catch (SQLException e) {
            logger.error("SQLException:", e);
            result = Failure.of(new Status(SQL_EXCEPTION, e.getMessage()));
        }
        return result;
    }

    /**
     * delete user from user_t table and all other tables related to this user.
     *
     * @param event event that is created by user service
     * @return result of email
     */
    @Override
    public Result<String> deleteUser(Map<String, Object> event) {
        // delete only user_t, other tables will be cacade deleted by database
        final String deleteUserById = "DELETE from user_t WHERE user_id = ?";
        Result<String> result;
        Map<String, Object> map = (Map<String, Object>)event.get(PortalConstants.DATA);
        try (Connection conn = ds.getConnection()) {
            conn.setAutoCommit(false);
            try (PreparedStatement statement = conn.prepareStatement(deleteUserById)) {
                statement.setObject(1, UUID.fromString((String)map.get("userId")));
                int count = statement.executeUpdate();
                if (count == 0) {
                    // no record is deleted, write an error notification.
                    throw new SQLException(String.format("no record is deleted by userId %s", map.get("userId")));
                }
                conn.commit();
                result = Success.of((String)map.get("userId"));
                insertNotification(event, true, null);
            } catch (SQLException e) {
                logger.error("SQLException:", e);
                conn.rollback();
                insertNotification(event, false, e.getMessage());
                result = Failure.of(new Status(SQL_EXCEPTION, e.getMessage()));
            } catch (Exception e) {
                logger.error("Exception:", e);
                conn.rollback();
                insertNotification(event, false, e.getMessage());
                result = Failure.of(new Status(GENERIC_EXCEPTION, e.getMessage()));
            }
        } catch (SQLException e) {
            logger.error("SQLException:", e);
            result = Failure.of(new Status(SQL_EXCEPTION, e.getMessage()));
        }
        return result;
    }

    /**
     * update user_t for the forget password token by email
     *
     * @param event event that is created by user service
     * @return result of email
     */
    @Override
    public Result<String> forgetPassword(Map<String, Object> event) {
        final String deleteUserByEmail = "UPDATE user_t SET token = ?, nonce = ? WHERE email = ?";
        Result<String> result;
        Map<String, Object> map = (Map<String, Object>)event.get(PortalConstants.DATA);
        try (Connection conn = ds.getConnection()) {
            conn.setAutoCommit(false);
            try (PreparedStatement statement = conn.prepareStatement(deleteUserByEmail)) {
                statement.setString(1, (String)map.get("token"));
                statement.setLong(2, ((Number)event.get(PortalConstants.NONCE)).longValue() + 1);
                statement.setString(3, (String)map.get("email"));
                int count = statement.executeUpdate();
                if (count == 0) {
                    // no record is deleted, write an error notification.
                    throw new SQLException(String.format("no token is updated by email %s", map.get("email")));
                }
                conn.commit();
                result = Success.of((String)map.get("email"));
                insertNotification(event, true, null);
            } catch (SQLException e) {
                logger.error("SQLException:", e);
                conn.rollback();
                insertNotification(event, false, e.getMessage());
                result = Failure.of(new Status(SQL_EXCEPTION, e.getMessage()));
            } catch (Exception e) {
                logger.error("Exception:", e);
                conn.rollback();
                insertNotification(event, false, e.getMessage());
                result = Failure.of(new Status(GENERIC_EXCEPTION, e.getMessage()));
            }
        } catch (SQLException e) {
            logger.error("SQLException:", e);
            result = Failure.of(new Status(SQL_EXCEPTION, e.getMessage()));
        }
        return result;
    }

    /**
     * update user_t to reset the password by email
     *
     * @param event event that is created by user service
     * @return result of email
     */
    @Override
    public Result<String> resetPassword(Map<String, Object> event) {
        final String deleteUserByEmail = "UPDATE user_t SET token = ?, nonce = ? WHERE email = ?";
        Result<String> result;
        Map<String, Object> map = (Map<String, Object>)event.get(PortalConstants.DATA);
        try (Connection conn = ds.getConnection()) {
            conn.setAutoCommit(false);
            try (PreparedStatement statement = conn.prepareStatement(deleteUserByEmail)) {
                statement.setString(1, (String)map.get("token"));
                statement.setLong(2, ((Number)event.get(PortalConstants.NONCE)).longValue() + 1);
                statement.setString(3, (String)map.get("email"));
                int count = statement.executeUpdate();
                if (count == 0) {
                    // no record is deleted, write an error notification.
                    throw new SQLException(String.format("no token is updated by email %s", map.get("email")));
                }
                conn.commit();
                result = Success.of((String)map.get("email"));
                insertNotification(event, true, null);
            } catch (SQLException e) {
                logger.error("SQLException:", e);
                conn.rollback();
                insertNotification(event, false, e.getMessage());
                result = Failure.of(new Status(SQL_EXCEPTION, e.getMessage()));
            } catch (Exception e) {
                logger.error("Exception:", e);
                conn.rollback();
                insertNotification(event, false, e.getMessage());
                result = Failure.of(new Status(GENERIC_EXCEPTION, e.getMessage()));
            }
        } catch (SQLException e) {
            logger.error("SQLException:", e);
            result = Failure.of(new Status(SQL_EXCEPTION, e.getMessage()));
        }
        return result;
    }

    /**
     * update user_t to change the password by email
     *
     * @param event event that is created by user service
     * @return result of email
     */
    @Override
    public Result<String> changePassword(Map<String, Object> event) {
        final String updatePasswordByEmail = "UPDATE user_t SET password = ?, nonce = ? WHERE email = ? AND password = ?";
        Result<String> result;
        Map<String, Object> map = (Map<String, Object>)event.get(PortalConstants.DATA);
        try (Connection conn = ds.getConnection()) {
            conn.setAutoCommit(false);
            try (PreparedStatement statement = conn.prepareStatement(updatePasswordByEmail)) {
                statement.setString(1, (String)map.get("password"));
                statement.setLong(2, ((Number)event.get(PortalConstants.NONCE)).longValue() + 1);
                statement.setString(3, (String)map.get("email"));
                statement.setString(4, (String)map.get("oldPassword"));
                int count = statement.executeUpdate();
                if (count == 0) {
                    // no record is updated, write an error notification.
                    throw new SQLException(String.format("no password is updated by email %s", map.get("email")));
                }
                conn.commit();
                result = Success.of((String)map.get("email"));
                insertNotification(event, true, null);
            } catch (SQLException e) {
                logger.error("SQLException:", e);
                conn.rollback();
                insertNotification(event, false, e.getMessage());
                result = Failure.of(new Status(SQL_EXCEPTION, e.getMessage()));
            } catch (Exception e) {
                logger.error("Exception:", e);
                conn.rollback();
                insertNotification(event, false, e.getMessage());
                result = Failure.of(new Status(GENERIC_EXCEPTION, e.getMessage()));
            }
        } catch (SQLException e) {
            logger.error("SQLException:", e);
            result = Failure.of(new Status(SQL_EXCEPTION, e.getMessage()));
        }
        return result;
    }

    @Override
    public Result<String> updatePayment(Map<String, Object> event) {
        return null;
    }

    @Override
    public Result<String> deletePayment(Map<String, Object> event) {
        return null;
    }

    @Override
    public Result<String> createOrder(Map<String, Object> event) {
        return null;
    }

    @Override
    public Result<String> cancelOrder(Map<String, Object> event) {
        return null;
    }

    @Override
    public Result<String> deliverOrder(Map<String, Object> event) {
        return null;
    }

    /**
     * send private message to user. Update the nonce of the from user and insert a message
     * to message_t table. Send a notification to the from user about the event processing result.
     *
     * @param event event that is created by user service
     * @return result of email
     */
    @Override
    public Result<String> sendPrivateMessage(Map<String, Object> event) {
        final String insertMessage = "INSERT INTO message_t (from_id, nonce, to_email, subject, content, send_time) VALUES (?, ?, ?, ?, ?, ?)";
        Result<String> result;
        Map<String, Object> map = (Map<String, Object>)event.get(PortalConstants.DATA);
        try (Connection conn = ds.getConnection()) {
            conn.setAutoCommit(false);
            try (PreparedStatement statement = conn.prepareStatement(insertMessage)) {
                statement.setString(1, (String)map.get("fromId"));
                statement.setLong(2, ((Number)event.get(PortalConstants.NONCE)).longValue());
                statement.setString(3, (String)map.get("toEmail"));
                statement.setString(4, (String)map.get("subject"));
                statement.setString(5, (String)map.get("content"));
                statement.setObject(6, OffsetDateTime.parse((String)event.get(CloudEventV1.TIME)));
                statement.executeUpdate();

                conn.commit();
                result = Success.of((String)map.get("fromId"));
                insertNotification(event, true, null);
            } catch (SQLException e) {
                logger.error("SQLException:", e);
                conn.rollback();
                insertNotification(event, false, e.getMessage());
                result = Failure.of(new Status(SQL_EXCEPTION, e.getMessage()));
            } catch (Exception e) {
                logger.error("Exception:", e);
                conn.rollback();
                insertNotification(event, false, e.getMessage());
                result = Failure.of(new Status(GENERIC_EXCEPTION, e.getMessage()));
            }
        } catch (SQLException e) {
            logger.error("SQLException:", e);
            result = Failure.of(new Status(SQL_EXCEPTION, e.getMessage()));
        }
        return result;

    }

    @Override
    public Result<String> queryUserLabel(String hostId) {
        Result<String> result = null;
        String sql = "SELECT u.user_id, u.email FROM user_t u, user_host_t h WHERE u.user_id = h.user_id AND h.host_id = ?";
        List<Map<String, Object>> labels = new ArrayList<>();
        try (Connection connection = ds.getConnection();
             PreparedStatement preparedStatement = connection.prepareStatement(sql)) {
            preparedStatement.setObject(1, UUID.fromString(hostId));
            try (ResultSet resultSet = preparedStatement.executeQuery()) {
                while (resultSet.next()) {
                    Map<String, Object> map = new HashMap<>();
                    map.put("id", resultSet.getString("user_id"));
                    map.put("label", resultSet.getString("email"));
                    labels.add(map);
                }
            }
            result = Success.of(JsonMapper.toJson(labels));
        } catch (SQLException e) {
            logger.error("SQLException:", e);
            result = Failure.of(new Status(SQL_EXCEPTION, e.getMessage()));
        } catch (Exception e) {
            logger.error("Exception:", e);
            result = Failure.of(new Status(GENERIC_EXCEPTION, e.getMessage()));
        }
        return result;
    }

    @Override
    public Result<String> createRefreshToken(Map<String, Object> event) {
        final String insertUser = "INSERT INTO auth_refresh_token_t (refresh_token, host_id, provider_id, user_id, entity_id, user_type, " +
                "email, roles, groups, positions, attributes, client_id, scope, csrf, custom_claim, update_user, update_ts) " +
                "VALUES (?, ?, ?, ?, ?,   ?, ?, ?, ?, ?,   ?, ?, ?, ?, ?, ?, ?)";
        Result<String> result = null;
        Map<String, Object> map = (Map<String, Object>)event.get(PortalConstants.DATA);
        try (Connection conn = ds.getConnection()) {
            conn.setAutoCommit(false);
            // no duplicate record, insert the user into database and write a success notification.
            try (PreparedStatement statement = conn.prepareStatement(insertUser)) {
                statement.setObject(1, UUID.fromString((String)map.get("refreshToken")));
                statement.setObject(2, UUID.fromString((String)map.get("hostId")));
                statement.setString(3, (String)map.get("providerId"));
                statement.setObject(4, UUID.fromString((String)map.get("userId")));
                statement.setString(5, (String)map.get("entityId"));
                statement.setString(6, (String)map.get("userType"));
                statement.setString(7, (String)map.get("email"));
                String roles = (String)map.get("roles");
                if (roles != null && !roles.isEmpty())
                    statement.setString(8, roles);
                else
                    statement.setNull(8, NULL);
                String groups = (String)map.get("groups");
                if (groups != null && !groups.isEmpty())
                    statement.setString(9, groups);
                else
                    statement.setNull(9, NULL);
                String positions = (String)map.get("positions");
                if (positions != null && !positions.isEmpty())
                    statement.setString(10, positions);
                else
                    statement.setNull(10, NULL);
                String attributes = (String)map.get("attributes");
                if (attributes != null && !attributes.isEmpty())
                    statement.setString(11, attributes);
                else
                    statement.setNull(11, NULL);
                String clientId = (String)map.get("clientId");
                if (clientId != null && !clientId.isEmpty())
                    statement.setObject(12, UUID.fromString(clientId));
                else
                    statement.setNull(12, NULL);
                String scope = (String)map.get("scope");
                if (scope != null && !scope.isEmpty())
                    statement.setString(13, scope);
                else
                    statement.setNull(13, NULL);
                String csrf = (String)map.get("csrf");
                if (csrf != null && !csrf.isEmpty())
                    statement.setString(14, csrf);
                else
                    statement.setNull(14, NULL);

                String customClaim  = (String)map.get("customClaim");
                if (customClaim != null && !customClaim.isEmpty())
                    statement.setString(15, customClaim);
                else
                    statement.setNull(15, NULL);

                statement.setString(16, (String)event.get(Constants.USER));
                statement.setObject(17, OffsetDateTime.parse((String)event.get(CloudEventV1.TIME)));
                int count = statement.executeUpdate();
                if (count == 0) {
                    // no record is inserted, write an error notification.
                    throw new SQLException(String.format("no record is inserted for refresh token %s", map.get("refreshToken")));
                }
                conn.commit();
                result = Success.of((String)map.get("refreshToken"));
                insertNotification(event, true, null);
            } catch (SQLException e) {
                logger.error("SQLException:", e);
                conn.rollback();
                insertNotification(event, false, e.getMessage());
                result = Failure.of(new Status(SQL_EXCEPTION, e.getMessage()));
            } catch (Exception e) {
                logger.error("Exception:", e);
                conn.rollback();
                insertNotification(event, false, e.getMessage());
                result = Failure.of(new Status(GENERIC_EXCEPTION, e.getMessage()));
            }
        } catch (SQLException e) {
            logger.error("SQLException:", e);
            result = Failure.of(new Status(SQL_EXCEPTION, e.getMessage()));
        }
        return result;
    }

    @Override
    public Result<String> deleteRefreshToken(Map<String, Object> event) {
        final String deleteApp = "DELETE from auth_refresh_token_t WHERE refresh_token = ? AND user_id = ?";
        Result<String> result;
        Map<String, Object> map = (Map<String, Object>)event.get(PortalConstants.DATA);
        try (Connection conn = ds.getConnection()) {
            conn.setAutoCommit(false);
            try (PreparedStatement statement = conn.prepareStatement(deleteApp)) {
                statement.setObject(1, UUID.fromString((String)map.get("refreshToken")));
                statement.setObject(2, UUID.fromString((String)map.get("userId")));
                int count = statement.executeUpdate();
                if (count == 0) {
                    // no record is deleted, write an error notification.
                    throw new SQLException(String.format("no record is deleted for refresh token %s", map.get("refreshToken")));
                }
                conn.commit();
                result = Success.of((String)map.get("userId"));
                insertNotification(event, true, null);
            } catch (SQLException e) {
                logger.error("SQLException:", e);
                conn.rollback();
                insertNotification(event, false, e.getMessage());
                result = Failure.of(new Status(SQL_EXCEPTION, e.getMessage()));
            } catch (Exception e) {
                logger.error("Exception:", e);
                conn.rollback();
                insertNotification(event, false, e.getMessage());
                result = Failure.of(new Status(GENERIC_EXCEPTION, e.getMessage()));
            }
        } catch (SQLException e) {
            logger.error("SQLException:", e);
            result = Failure.of(new Status(SQL_EXCEPTION, e.getMessage()));
        }
        return result;
    }

    @Override
    public Result<String> listRefreshToken(int offset, int limit, String refreshToken, String hostId, String userId, String entityId,
                                           String email, String firstName, String lastName, String clientId, String appId,
                                           String appName, String scope, String userType, String roles, String groups, String positions,
                                           String attributes, String csrf, String customClaim, String updateUser, Timestamp updateTs) {
        Result<String> result = null;
        StringBuilder sqlBuilder = new StringBuilder();
        sqlBuilder.append("SELECT COUNT(*) OVER () AS total,\n" +
                "r.host_id, r.refresh_token, r.user_id, r.user_type, r.entity_id, r.email, u.first_name, u.last_name, \n" +
                "r.client_id, a.app_id, a.app_name, r.scope, r.roles, r.groups, r.positions, r.attributes, r.csrf, " +
                "r.custom_claim, r.update_user, r.update_ts \n" +
                "FROM auth_refresh_token_t r, user_t u, app_t a, auth_client_t c\n" +
                "WHERE r.user_id = u.user_id AND r.client_id = c.client_id AND a.app_id = c.app_id\n" +
                "AND r.host_id = ?\n");

        List<Object> parameters = new ArrayList<>();
        parameters.add(UUID.fromString(hostId));

        StringBuilder whereClause = new StringBuilder();

        addCondition(whereClause, parameters, "r.refresh_token", refreshToken != null ? UUID.fromString(refreshToken) : null);
        addCondition(whereClause, parameters, "r.user_id", userId != null ? UUID.fromString(userId) : null);
        addCondition(whereClause, parameters, "r.user_type", userType);
        addCondition(whereClause, parameters, "u.entity_id", entityId);
        addCondition(whereClause, parameters, "u.email", email);
        addCondition(whereClause, parameters, "r.first_name", firstName);
        addCondition(whereClause, parameters, "r.last_name", lastName);
        addCondition(whereClause, parameters, "r.client_id", clientId != null ? UUID.fromString(clientId) : null);
        addCondition(whereClause, parameters, "a.app_id", appId);
        addCondition(whereClause, parameters, "a.app_name", appName);
        addCondition(whereClause, parameters, "r.scope", scope);
        addCondition(whereClause, parameters, "r.roles", roles);
        addCondition(whereClause, parameters, "r.groups", groups);
        addCondition(whereClause, parameters, "r.positions", positions);
        addCondition(whereClause, parameters, "r.attributes", attributes);
        addCondition(whereClause, parameters, "r.csrf", csrf);
        addCondition(whereClause, parameters, "r.custom_claim", customClaim);
        addCondition(whereClause, parameters, "r.update_user", updateUser);
        addCondition(whereClause, parameters, "r.update_ts", updateTs);


        if (!whereClause.isEmpty()) {
            sqlBuilder.append("AND ").append(whereClause);
        }

        sqlBuilder.append(" ORDER BY r.user_id\n" +
                "LIMIT ? OFFSET ?");

        parameters.add(limit);
        parameters.add(offset);

        String sql = sqlBuilder.toString();
        int total = 0;
        List<Map<String, Object>> tokens = new ArrayList<>();

        try (Connection connection = ds.getConnection();
             PreparedStatement preparedStatement = connection.prepareStatement(sql)) {

            for (int i = 0; i < parameters.size(); i++) {
                preparedStatement.setObject(i + 1, parameters.get(i));
            }


            boolean isFirstRow = true;
            try (ResultSet resultSet = preparedStatement.executeQuery()) {
                while (resultSet.next()) {
                    Map<String, Object> map = new HashMap<>();
                    if (isFirstRow) {
                        total = resultSet.getInt("total");
                        isFirstRow = false;
                    }

                    map.put("hostId", resultSet.getObject("host_id", UUID.class));
                    map.put("refreshToken", resultSet.getObject("refresh_token", UUID.class));
                    map.put("userId", resultSet.getObject("user_id", UUID.class));
                    map.put("userType", resultSet.getString("user_type"));
                    map.put("entityId", resultSet.getString("entity_id"));
                    map.put("email", resultSet.getString("email"));
                    map.put("firstName", resultSet.getString("first_name"));
                    map.put("lastName", resultSet.getString("last_name"));
                    map.put("clientId", resultSet.getObject("client_id", UUID.class));
                    map.put("appId", resultSet.getString("app_id"));
                    map.put("appName", resultSet.getString("app_name"));
                    map.put("scope", resultSet.getString("scope"));
                    map.put("roles", resultSet.getString("roles"));
                    map.put("groups", resultSet.getString("groups"));
                    map.put("positions", resultSet.getString("positions"));
                    map.put("attributes", resultSet.getString("attributes"));
                    map.put("csrf", resultSet.getString("csrf"));
                    map.put("customClaim", resultSet.getString("custom_claim"));
                    map.put("updateUser", resultSet.getString("update_user"));
                    map.put("updateTs", resultSet.getObject("update_ts") != null ? resultSet.getObject("update_ts", OffsetDateTime.class) : null);
                    tokens.add(map);
                }
            }


            Map<String, Object> resultMap = new HashMap<>();
            resultMap.put("total", total);
            resultMap.put("tokens", tokens);
            result = Success.of(JsonMapper.toJson(resultMap));

        } catch (SQLException e) {
            logger.error("SQLException:", e);
            result = Failure.of(new Status(SQL_EXCEPTION, e.getMessage()));
        } catch (Exception e) {
            logger.error("Exception:", e);
            result = Failure.of(new Status(GENERIC_EXCEPTION, e.getMessage()));
        }
        return result;
    }

    @Override
    public Result<String> queryRefreshToken(String refreshToken) {
        Result<String> result = null;
        String sql =
                "SELECT refresh_token, host_id, provider_id, user_id, entity_id, user_type, email, roles, groups, " +
                        "positions, attributes, client_id, scope, csrf, custom_claim\n" +
                        "FROM auth_refresh_token_t\n" +
                        "WHERE refresh_token = ?\n";
        try (final Connection conn = ds.getConnection()) {
            Map<String, Object> map = new HashMap<>();
            try (PreparedStatement statement = conn.prepareStatement(sql)) {
                statement.setObject(1, UUID.fromString(refreshToken));
                try (ResultSet resultSet = statement.executeQuery()) {
                    if (resultSet.next()) {
                        map.put("refreshToken", resultSet.getObject("refresh_token", UUID.class));
                        map.put("hostId", resultSet.getObject("host_id", UUID.class));
                        map.put("providerId", resultSet.getString("provider_id"));
                        map.put("userId", resultSet.getObject("user_id", UUID.class));
                        map.put("entityId", resultSet.getString("entity_id"));
                        map.put("userType", resultSet.getString("user_type"));
                        map.put("email", resultSet.getString("email"));
                        map.put("roles", resultSet.getString("roles"));
                        map.put("groups", resultSet.getString("groups"));
                        map.put("positions", resultSet.getString("positions"));
                        map.put("attributes", resultSet.getString("attributes"));
                        map.put("clientId", resultSet.getObject("client_id", UUID.class));
                        map.put("scope", resultSet.getString("scope"));
                        map.put("csrf", resultSet.getString("csrf"));
                        map.put("customClaim", resultSet.getString("custom_claim"));
                    }
                }
            }
            if (map.isEmpty())
                result = Failure.of(new Status(OBJECT_NOT_FOUND, "refresh token", refreshToken));
            else
                result = Success.of(JsonMapper.toJson(map));
        } catch (SQLException e) {
            logger.error("SQLException:", e);
            result = Failure.of(new Status(SQL_EXCEPTION, e.getMessage()));
        } catch (Exception e) {
            logger.error("Exception:", e);
            result = Failure.of(new Status(GENERIC_EXCEPTION, e.getMessage()));
        }
        return result;
    }

    @Override
    public Result<String> createAuthCode(Map<String, Object> event) {

        final String sql = "INSERT INTO auth_code_t(host_id, provider_id, auth_code, user_id, entity_id, user_type, email, roles," +
                "redirect_uri, scope, remember, code_challenge, challenge_method, update_user, update_ts) " +
                "VALUES (?, ?, ?, ?, ?, ?, ?,  ?, ?, ?, ?, ?, ?, ?, ?)";
        Result<String> result;
        Map<String, Object> map = (Map<String, Object>)event.get(PortalConstants.DATA);
        try (Connection conn = ds.getConnection()) {
            conn.setAutoCommit(false);
            // no duplicate record, insert the user into database and write a success notification.
            try (PreparedStatement statement = conn.prepareStatement(sql)) {
                statement.setObject(1, UUID.fromString((String)map.get("hostId")));
                statement.setString(2, (String)map.get("providerId"));
                statement.setString(3, (String)map.get("authCode"));
                if(map.containsKey("userId")) {
                    statement.setObject(4, UUID.fromString((String)map.get("userId")));
                } else {
                    statement.setNull(4, Types.VARCHAR);
                }
                if(map.containsKey("entityId")) {
                    statement.setString(5, (String)map.get("entityId"));
                } else {
                    statement.setNull(5, Types.VARCHAR);
                }
                if(map.containsKey("userType")) {
                    statement.setString(6, (String)map.get("userType"));
                } else {
                    statement.setNull(6, Types.VARCHAR);
                }
                if(map.containsKey("email")) {
                    statement.setString(7, (String)map.get("email"));
                } else {
                    statement.setNull(7, Types.VARCHAR);
                }
                if(map.containsKey("roles")) {
                    statement.setString(8, (String)map.get("roles"));
                } else {
                    statement.setNull(8, Types.VARCHAR);
                }
                if(map.containsKey("redirectUri")) {
                    statement.setString(9, (String)map.get("redirectUri"));
                } else {
                    statement.setNull(9, Types.VARCHAR);
                }
                if(map.containsKey("scope")) {
                    statement.setString(10, (String)map.get("scope"));
                } else {
                    statement.setNull(10, Types.VARCHAR);
                }
                if(map.containsKey("remember")) {
                    statement.setString(11, (String)map.get("remember"));
                } else {
                    statement.setNull(11, Types.CHAR);
                }
                if(map.containsKey("codeChallenge")) {
                    statement.setString(12, (String)map.get("codeChallenge"));
                } else {
                    statement.setNull(12, Types.VARCHAR);
                }
                if(map.containsKey("challengeMethod")) {
                    statement.setString(13, (String)map.get("challengeMethod"));
                } else {
                    statement.setNull(13, Types.VARCHAR);
                }
                statement.setString(14, (String)event.get(Constants.USER));
                statement.setObject(15, OffsetDateTime.parse((String)event.get(CloudEventV1.TIME)));
                int count = statement.executeUpdate();
                if (count == 0) {
                    throw new SQLException("failed to insert the auth code with id " + map.get("authCode"));
                }
                conn.commit();
                result = Success.of((String)map.get("authCode"));
                insertNotification(event, true, null);
            } catch (SQLException e) {
                logger.error("SQLException:", e);
                conn.rollback();
                insertNotification(event, false, e.getMessage());
                result = Failure.of(new Status(SQL_EXCEPTION, e.getMessage()));
            } catch (Exception e) {
                logger.error("Exception:", e);
                conn.rollback();
                insertNotification(event, false, e.getMessage());
                result = Failure.of(new Status(GENERIC_EXCEPTION, e.getMessage()));
            }
        } catch (SQLException e) {
            logger.error("SQLException:", e);
            result = Failure.of(new Status(SQL_EXCEPTION, e.getMessage()));
        }
        return result;
    }

    @Override
    public Result<String> deleteAuthCode(Map<String, Object> event) {
        final String sql = "DELETE FROM auth_code_t WHERE host_id = ? AND auth_code = ?";
        Result<String> result;
        Map<String, Object> map = (Map<String, Object>)event.get(PortalConstants.DATA);
        try (Connection conn = ds.getConnection()) {
            conn.setAutoCommit(false);
            try (PreparedStatement statement = conn.prepareStatement(sql)) {
                statement.setObject(1, UUID.fromString((String)map.get("hostId")));
                statement.setString(2, (String)map.get("authCode"));
                int count = statement.executeUpdate();
                if (count == 0) {
                    // no record is deleted, write an error notification.
                    throw new SQLException(String.format("no record is deleted for auth code " + "hostId " + map.get("hostId") + " authCode " + map.get("authCode")));
                }
                conn.commit();
                result = Success.of((String)map.get("authCode"));
                insertNotification(event, true, null);
            } catch (SQLException e) {
                logger.error("SQLException:", e);
                conn.rollback();
                insertNotification(event, false, e.getMessage());
                result = Failure.of(new Status(SQL_EXCEPTION, e.getMessage()));
            } catch (Exception e) {
                logger.error("Exception:", e);
                conn.rollback();
                insertNotification(event, false, e.getMessage());
                result = Failure.of(new Status(GENERIC_EXCEPTION, e.getMessage()));
            }

        } catch (SQLException e) {
            logger.error("SQLException:", e);
            result = Failure.of(new Status(SQL_EXCEPTION, e.getMessage()));
        }
        return result;
    }

    @Override
    public Result<String> queryAuthCode(String authCode) {
        final String sql = "SELECT host_id, provider_id, auth_code, user_id, entity_id, user_type, email, " +
                "roles, redirect_uri, scope, remember, code_challenge, challenge_method " +
                "FROM auth_code_t WHERE auth_code = ?";
        Result<String> result;
        try (Connection connection = ds.getConnection();
             PreparedStatement preparedStatement = connection.prepareStatement(sql)) {
            preparedStatement.setString(1, authCode);
            try (ResultSet resultSet = preparedStatement.executeQuery()) {
                if (resultSet.next()) {
                    Map<String, Object> map = new HashMap<>();
                    map.put("hostId", resultSet.getObject("host_id", UUID.class));
                    map.put("providerId", resultSet.getString("provider_id"));
                    map.put("authCode", resultSet.getString("auth_code"));
                    map.put("userId", resultSet.getObject("user_id", UUID.class));
                    map.put("entityId", resultSet.getString("entity_id"));
                    map.put("userType", resultSet.getString("user_type"));
                    map.put("email", resultSet.getString("email"));
                    map.put("roles", resultSet.getString("roles"));
                    map.put("redirectUri", resultSet.getString("redirect_uri"));
                    map.put("scope", resultSet.getString("scope"));
                    map.put("remember", resultSet.getString("remember"));
                    map.put("codeChallenge", resultSet.getString("code_challenge"));
                    map.put("challengeMethod", resultSet.getString("challenge_method"));
                    result = Success.of(JsonMapper.toJson(map));
                } else {
                    result = Failure.of(new Status(OBJECT_NOT_FOUND, "auth code", authCode));
                }
            }
        } catch (SQLException e) {
            logger.error("SQLException:", e);
            result = Failure.of(new Status(SQL_EXCEPTION, e.getMessage()));
        } catch (Exception e) {
            logger.error("Exception:", e);
            result = Failure.of(new Status(GENERIC_EXCEPTION, e.getMessage()));
        }
        return result;
    }

    @Override
    public Result<String> listAuthCode(int offset, int limit, String hostId, String authCode, String userId,
                                       String entityId, String userType, String email, String roles, String groups, String positions,
                                       String attributes, String redirectUri, String scope, String remember, String codeChallenge,
                                       String challengeMethod, String updateUser, Timestamp updateTs) {
        Result<String> result = null;
        StringBuilder sqlBuilder = new StringBuilder();
        sqlBuilder.append("SELECT COUNT(*) OVER () AS total,\n" +
                "host_id, auth_code, user_id, entity_id, user_type, email, roles, redirect_uri, scope, remember, " +
                "code_challenge, challenge_method, update_user, update_ts\n" +
                "FROM auth_code_t\n" +
                "WHERE host_id = ?\n");


        List<Object> parameters = new ArrayList<>();
        parameters.add(UUID.fromString(hostId));

        StringBuilder whereClause = new StringBuilder();

        addCondition(whereClause, parameters, "auth_code", authCode);
        addCondition(whereClause, parameters, "user_id", userId != null ? UUID.fromString(userId) : null);
        addCondition(whereClause, parameters, "entity_id", entityId);
        addCondition(whereClause, parameters, "user_type", userType);
        addCondition(whereClause, parameters, "email", email);
        addCondition(whereClause, parameters, "roles", roles);
        addCondition(whereClause, parameters, "redirect_uri", redirectUri);
        addCondition(whereClause, parameters, "scope", scope);
        addCondition(whereClause, parameters, "remember", remember);
        addCondition(whereClause, parameters, "code_challenge", codeChallenge);
        addCondition(whereClause, parameters, "challenge_method", challengeMethod);

        if (!whereClause.isEmpty()) {
            sqlBuilder.append("AND ").append(whereClause);
        }

        sqlBuilder.append(" ORDER BY update_ts\n" +
                "LIMIT ? OFFSET ?");

        parameters.add(limit);
        parameters.add(offset);

        String sql = sqlBuilder.toString();
        int total = 0;
        List<Map<String, Object>> authCodes = new ArrayList<>();

        try (Connection connection = ds.getConnection();
             PreparedStatement preparedStatement = connection.prepareStatement(sql)) {

            for (int i = 0; i < parameters.size(); i++) {
                preparedStatement.setObject(i + 1, parameters.get(i));
            }


            boolean isFirstRow = true;
            try (ResultSet resultSet = preparedStatement.executeQuery()) {
                while (resultSet.next()) {
                    Map<String, Object> map = new HashMap<>();

                    if (isFirstRow) {
                        total = resultSet.getInt("total");
                        isFirstRow = false;
                    }

                    map.put("hostId", resultSet.getObject("host_id", UUID.class));
                    map.put("authCode", resultSet.getString("auth_code"));
                    map.put("userId", resultSet.getObject("user_id", UUID.class));
                    map.put("entityId", resultSet.getString("entity_id"));
                    map.put("userType", resultSet.getString("user_type"));
                    map.put("email", resultSet.getString("email"));
                    map.put("roles", resultSet.getString("roles"));
                    map.put("redirectUri", resultSet.getString("redirect_uri"));
                    map.put("scope", resultSet.getString("scope"));
                    map.put("remember", resultSet.getString("remember"));
                    map.put("codeChallenge", resultSet.getString("code_challenge"));
                    map.put("challengeMethod", resultSet.getString("challenge_method"));
                    map.put("updateUser", resultSet.getString("update_user"));
                    map.put("updateTs", resultSet.getTimestamp("update_ts") != null ? resultSet.getTimestamp("update_ts").toString() : null);

                    authCodes.add(map);
                }
            }

            Map<String, Object> resultMap = new HashMap<>();
            resultMap.put("total", total);
            resultMap.put("codes", authCodes);
            result = Success.of(JsonMapper.toJson(resultMap));

        } catch (SQLException e) {
            logger.error("SQLException:", e);
            result = Failure.of(new Status(SQL_EXCEPTION, e.getMessage()));
        } catch (Exception e) {
            logger.error("Exception:", e);
            result = Failure.of(new Status(GENERIC_EXCEPTION, e.getMessage()));
        }
        return result;
    }

    @Override
    public Result<Map<String, Object>> queryProviderById(String providerId) {
        final String sql = "SELECT host_id, provider_id, provider_name, jwk " +
                "from auth_provider_t WHERE provider_id = ?";
        Result<Map<String, Object>> result;
        try (final Connection conn = ds.getConnection()) {
            Map<String, Object> map = new HashMap<>();
            try (PreparedStatement statement = conn.prepareStatement(sql)) {
                statement.setString(1, providerId);
                try (ResultSet resultSet = statement.executeQuery()) {
                    if (resultSet.next()) {
                        map.put("hostId", resultSet.getObject("host_id", UUID.class));
                        map.put("providerId", resultSet.getString("provider_id"));
                        map.put("providerName", resultSet.getString("provider_name"));
                        map.put("jwk", resultSet.getString("jwk"));
                    }
                }
            }
            if (map.isEmpty())
                result = Failure.of(new Status(OBJECT_NOT_FOUND, "auth provider", providerId));
            else
                result = Success.of(map);
        } catch (SQLException e) {
            logger.error("SQLException:", e);
            result = Failure.of(new Status(SQL_EXCEPTION, e.getMessage()));
        } catch (Exception e) {
            logger.error("Exception:", e);
            result = Failure.of(new Status(GENERIC_EXCEPTION, e.getMessage()));
        }
        return result;
    }

    @Override
    public Result<String> queryProvider(int offset, int limit, String hostId, String providerId, String providerName, String providerDesc,
                                        String operationOwner, String deliveryOwner, String jwk, String updateUser, Timestamp updateTs) {
        Result<String> result = null;
        StringBuilder sqlBuilder = new StringBuilder();
        sqlBuilder.append("SELECT COUNT(*) OVER () AS total,\n" +
                "host_id, provider_id, provider_name, provider_desc, operation_owner, delivery_owner, jwk, update_user, update_ts\n" +
                "FROM auth_provider_t\n" +
                "WHERE host_id = ?\n");

        List<Object> parameters = new ArrayList<>();
        parameters.add(UUID.fromString(hostId));

        StringBuilder whereClause = new StringBuilder();

        addCondition(whereClause, parameters, "provider_id", providerId);
        addCondition(whereClause, parameters, "provider_name", providerName);
        addCondition(whereClause, parameters, "provider_desc", providerDesc);
        addCondition(whereClause, parameters, "operation_owner", operationOwner);
        addCondition(whereClause, parameters, "delivery_owner", deliveryOwner);
        addCondition(whereClause, parameters, "jwk", jwk);

        if (!whereClause.isEmpty()) {
            sqlBuilder.append("AND ").append(whereClause);
        }

        sqlBuilder.append(" ORDER BY provider_id\n" +
                "LIMIT ? OFFSET ?");

        parameters.add(limit);
        parameters.add(offset);

        String sql = sqlBuilder.toString();
        int total = 0;
        List<Map<String, Object>> providers = new ArrayList<>();

        try (Connection connection = ds.getConnection();
             PreparedStatement preparedStatement = connection.prepareStatement(sql)) {

            for (int i = 0; i < parameters.size(); i++) {
                preparedStatement.setObject(i + 1, parameters.get(i));
            }

            boolean isFirstRow = true;
            try (ResultSet resultSet = preparedStatement.executeQuery()) {
                while (resultSet.next()) {
                    Map<String, Object> map = new HashMap<>();
                    if (isFirstRow) {
                        total = resultSet.getInt("total");
                        isFirstRow = false;
                    }
                    map.put("hostId", resultSet.getObject("host_id", UUID.class));
                    map.put("providerId", resultSet.getString("provider_id"));
                    map.put("providerName", resultSet.getString("provider_name"));
                    map.put("providerDesc", resultSet.getString("provider_desc"));
                    map.put("operationOwner", resultSet.getString("operation_owner"));
                    map.put("deliveryOwner", resultSet.getString("delivery_owner"));
                    map.put("jwk", resultSet.getString("jwk"));
                    map.put("updateUser", resultSet.getString("update_user"));
                    // handling date properly
                    map.put("updateTs", resultSet.getTimestamp("update_ts") != null ? resultSet.getTimestamp("update_ts").toString() : null);
                    providers.add(map);
                }
            }

            Map<String, Object> resultMap = new HashMap<>();
            resultMap.put("total", total);
            resultMap.put("providers", providers);
            result = Success.of(JsonMapper.toJson(resultMap));

        } catch (SQLException e) {
            logger.error("SQLException:", e);
            result = Failure.of(new Status(SQL_EXCEPTION, e.getMessage()));
        } catch (Exception e) {
            logger.error("Exception:", e);
            result = Failure.of(new Status(GENERIC_EXCEPTION, e.getMessage()));
        }
        return result;
    }

    @Override
    public Result<String> createAuthProvider(Map<String, Object> event) {
        final String sql = "INSERT INTO auth_provider_t(host_id, provider_id, provider_name, provider_desc, " +
                "operation_owner, delivery_owner, jwk, update_user, update_ts) " +
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)";
        Result<String> result;
        Map<String, Object> map = (Map<String, Object>)event.get(PortalConstants.DATA);
        try (Connection conn = ds.getConnection()) {
            conn.setAutoCommit(false);
            try (PreparedStatement statement = conn.prepareStatement(sql)) {
                statement.setObject(1, UUID.fromString((String)map.get("hostId")));
                statement.setString(2, (String)map.get("providerId"));
                statement.setString(3, (String)map.get("providerName"));

                if(map.containsKey("providerDesc")) {
                    statement.setString(4, (String)map.get("providerDesc"));
                } else {
                    statement.setNull(4, Types.VARCHAR);
                }
                if(map.containsKey("operationOwner")) {
                    statement.setString(5, (String)map.get("operationOwner"));
                } else {
                    statement.setNull(5, Types.VARCHAR);
                }
                if(map.containsKey("deliveryOwner")) {
                    statement.setString(6, (String)map.get("deliveryOwner"));
                } else {
                    statement.setNull(6, Types.VARCHAR);
                }
                if(map.containsKey("jwk")) {
                    statement.setString(7, (String)map.get("jwk"));
                } else {
                    statement.setNull(7, Types.VARCHAR);
                }
                statement.setString(8, (String)event.get(Constants.USER));
                statement.setObject(9, OffsetDateTime.parse((String)event.get(CloudEventV1.TIME)));

                int count = statement.executeUpdate();
                if (count == 0) {
                    throw new SQLException("failed to insert the auth provider with id " + map.get("providerId"));
                }

                // Insert keys into auth_provider_key_t
                String keySql = "INSERT INTO auth_provider_key_t(provider_id, kid, public_key, private_key, key_type, update_user, update_ts) " +
                "VALUES (?, ?, ?, ?, ?, ?, ?)";

                try (PreparedStatement keyStatement = conn.prepareStatement(keySql)) {
                    Map<String, Object> keys = (Map<String, Object>) map.get("keys");

                    keyStatement.setString(1, (String)map.get("providerId"));

                    Map<String, Object> lcMap = (Map<String, Object>) keys.get("LC");
                    // add long live current key
                    keyStatement.setString(2, (String)lcMap.get("kid"));
                    keyStatement.setString(3, (String)lcMap.get("publicKey"));
                    keyStatement.setString(4, (String)lcMap.get("privateKey"));
                    keyStatement.setString(5, "LC");
                    keyStatement.setString(6, (String)event.get(Constants.USER));
                    keyStatement.setObject(7, OffsetDateTime.parse((String)event.get(CloudEventV1.TIME)));
                    keyStatement.executeUpdate();

                    // add long live previous key
                    Map<String, Object> lpMap = (Map<String, Object>) keys.get("LP");
                    keyStatement.setString(2, (String)lpMap.get("kid"));
                    keyStatement.setString(3, (String)lpMap.get("publicKey"));
                    keyStatement.setString(4, (String)lpMap.get("privateKey"));
                    keyStatement.setString(5, "LP");
                    keyStatement.setString(6, (String)event.get(Constants.USER));
                    keyStatement.setObject(7, OffsetDateTime.parse((String)event.get(CloudEventV1.TIME)));
                    keyStatement.executeUpdate();

                    // add token current key
                    Map<String, Object> tcMap = (Map<String, Object>) keys.get("TC");
                    keyStatement.setString(2, (String)tcMap.get("kid"));
                    keyStatement.setString(3, (String)tcMap.get("publicKey"));
                    keyStatement.setString(4, (String)tcMap.get("privateKey"));
                    keyStatement.setString(5, "TC");
                    keyStatement.setString(6, (String)event.get(Constants.USER));
                    keyStatement.setObject(7, OffsetDateTime.parse((String)event.get(CloudEventV1.TIME)));
                    keyStatement.executeUpdate();

                    // add token previous key
                    Map<String, Object> tpMap = (Map<String, Object>) keys.get("TP");
                    keyStatement.setString(2, (String)tpMap.get("kid"));
                    keyStatement.setString(3, (String)tpMap.get("publicKey"));
                    keyStatement.setString(4, (String)tpMap.get("privateKey"));
                    keyStatement.setString(5, "TP");
                    keyStatement.setString(6, (String)event.get(Constants.USER));
                    keyStatement.setObject(7, OffsetDateTime.parse((String)event.get(CloudEventV1.TIME)));
                    keyStatement.executeUpdate();

                } catch(SQLException ex) {
                    throw new SQLException("failed to insert the auth provider key with provider id " + map.get("providerId"));
                }
                conn.commit();
                result = Success.of((String)map.get("providerId"));
                insertNotification(event, true, null);
            } catch (SQLException e) {
                logger.error("SQLException:", e);
                conn.rollback();
                insertNotification(event, false, e.getMessage());
                result = Failure.of(new Status(SQL_EXCEPTION, e.getMessage()));
            } catch (Exception e) {
                logger.error("Exception:", e);
                conn.rollback();
                insertNotification(event, false, e.getMessage());
                result = Failure.of(new Status(GENERIC_EXCEPTION, e.getMessage()));
            }
        } catch (SQLException e) {
            logger.error("SQLException:", e);
            result = Failure.of(new Status(SQL_EXCEPTION, e.getMessage()));
        }
        return result;
    }

    @Override
    public Result<String> rotateAuthProvider(Map<String, Object> event) {
        final String sqlJwk = "UPDATE auth_provider_t SET jwk = ?, update_user = ?, update_ts = ? " +
                "WHERE provider_id = ?";
        final String sqlInsert = "INSERT INTO auth_provider_key_t(provider_id, kid, public_key, private_key, key_type, update_user, update_ts) " +
                "VALUES (?, ?, ?, ?, ?,  ?, ?)";
        final String sqlUpdate = "UPDATE auth_provider_key_t SET key_type = ?, update_user = ?, update_ts = ? " +
                "WHERE provider_id = ? AND kid = ?";
        final String sqlDelete = "DELETE FROM auth_provider_key_t WHERE provider_id = ? AND kid = ?";


        Result<String> result;
        Map<String, Object> map = (Map<String, Object>)event.get(PortalConstants.DATA);

        try (Connection conn = ds.getConnection()) {
            conn.setAutoCommit(false);
            try (PreparedStatement statement = conn.prepareStatement(sqlJwk)) {
                String jwk = (String) map.get("jwk");
                statement.setString(1, jwk);
                statement.setString(2, (String)event.get(Constants.USER));
                statement.setObject(3, OffsetDateTime.parse((String)event.get(CloudEventV1.TIME)));
                statement.setString(4, (String)map.get("providerId"));

                int count = statement.executeUpdate();
                if (count == 0) {
                    throw new SQLException("failed to update the jwk for auth provider with id " + map.get("providerId"));
                }

                try (PreparedStatement statementInsert = conn.prepareStatement(sqlInsert)) {
                    Map<String, Object> insertMap = (Map<String, Object>) map.get("insert");
                    statementInsert.setString(1, (String)map.get("providerId"));
                    statementInsert.setString(2, (String) insertMap.get("kid"));
                    statementInsert.setString(3, (String) insertMap.get("publicKey"));
                    statementInsert.setString(4, (String) insertMap.get("privateKey"));
                    statementInsert.setString(5, (String) insertMap.get("keyType"));
                    statementInsert.setString(6, (String)event.get(Constants.USER));
                    statementInsert.setObject(7, OffsetDateTime.parse((String)event.get(CloudEventV1.TIME)));

                    count = statementInsert.executeUpdate();
                    if (count == 0) {
                        throw new SQLException("failed to insert the auth provider key with provider id " + map.get("providerId"));
                    }
                }
                try (PreparedStatement statementUpdate = conn.prepareStatement(sqlUpdate)) {
                    Map<String, Object> updateMap = (Map<String, Object>) map.get("update");
                    statementUpdate.setString(1, (String) updateMap.get("keyType"));
                    statementUpdate.setString(2, (String)event.get(Constants.USER));
                    statementUpdate.setObject(3, OffsetDateTime.parse((String)event.get(CloudEventV1.TIME)));
                    statementUpdate.setString(4, (String)map.get("providerId"));
                    statementUpdate.setString(5, (String)updateMap.get("kid"));
                    count = statementUpdate.executeUpdate();
                    if (count == 0) {
                        throw new SQLException("failed to update the auth provider key with provider id " + map.get("providerId"));
                    }
                }
                try (PreparedStatement statementDelete = conn.prepareStatement(sqlDelete)) {
                    Map<String, Object> deleteMap = (Map<String, Object>) map.get("delete");
                    statementDelete.setString(1, (String)map.get("providerId"));
                    statementDelete.setString(2, (String)deleteMap.get("kid"));
                    count = statementDelete.executeUpdate();
                    if (count == 0) {
                        throw new SQLException("failed to update the auth provider key with provider id " + map.get("providerId"));
                    }
                }
                conn.commit();
                result = Success.of((String)map.get("providerId"));
                insertNotification(event, true, null);
            } catch (SQLException e) {
                logger.error("SQLException:", e);
                conn.rollback();
                insertNotification(event, false, e.getMessage());
                result = Failure.of(new Status(SQL_EXCEPTION, e.getMessage()));
            } catch (Exception e) {
                logger.error("Exception:", e);
                conn.rollback();
                insertNotification(event, false, e.getMessage());
                result = Failure.of(new Status(GENERIC_EXCEPTION, e.getMessage()));
            }
        } catch (SQLException e) {
            logger.error("SQLException:", e);
            result = Failure.of(new Status(SQL_EXCEPTION, e.getMessage()));
        }
        return result;
    }

    @Override
    public Result<String> updateAuthProvider(Map<String, Object> event) {
        final String sql = "UPDATE auth_provider_t SET provider_name = ?, provider_desc = ?, " +
                "operation_owner = ?, delivery_owner = ?, update_user = ?, update_ts = ? " +
                "WHERE host_id = ? and provider_id = ?";
        Result<String> result;
        Map<String, Object> map = (Map<String, Object>)event.get(PortalConstants.DATA);

        try (Connection conn = ds.getConnection()) {
            conn.setAutoCommit(false);
            try (PreparedStatement statement = conn.prepareStatement(sql)) {
                statement.setString(1, (String)map.get("providerName"));
                if(map.containsKey("providerDesc")) {
                    statement.setString(2, (String)map.get("providerDesc"));
                } else {
                    statement.setNull(2, Types.VARCHAR);
                }
                if(map.containsKey("operationOwner")) {
                    statement.setString(3, (String)map.get("operationOwner"));
                } else {
                    statement.setNull(3, Types.VARCHAR);
                }
                if(map.containsKey("deliveryOwner")) {
                    statement.setString(4, (String)map.get("deliveryOwner"));
                } else {
                    statement.setNull(4, Types.VARCHAR);
                }
                statement.setString(5, (String)event.get(Constants.USER));
                statement.setObject(6, OffsetDateTime.parse((String)event.get(CloudEventV1.TIME)));
                statement.setObject(7, UUID.fromString((String)map.get("hostId")));
                statement.setString(8, (String)map.get("providerId"));

                int count = statement.executeUpdate();
                if (count == 0) {
                    throw new SQLException("failed to update the auth provider with id " + map.get("providerId"));
                }
                conn.commit();
                result = Success.of((String)map.get("providerId"));
                insertNotification(event, true, null);
            } catch (SQLException e) {
                logger.error("SQLException:", e);
                conn.rollback();
                insertNotification(event, false, e.getMessage());
                result = Failure.of(new Status(SQL_EXCEPTION, e.getMessage()));
            } catch (Exception e) {
                logger.error("Exception:", e);
                conn.rollback();
                insertNotification(event, false, e.getMessage());
                result = Failure.of(new Status(GENERIC_EXCEPTION, e.getMessage()));
            }
        } catch (SQLException e) {
            logger.error("SQLException:", e);
            result = Failure.of(new Status(SQL_EXCEPTION, e.getMessage()));
        }
        return result;
    }

    @Override
    public Result<String> deleteAuthProvider(Map<String, Object> event) {
        final String sql = "DELETE FROM auth_provider_t WHERE host_id = ? and provider_id = ?";
        Result<String> result;
        Map<String, Object> map = (Map<String, Object>)event.get(PortalConstants.DATA);

        try (Connection conn = ds.getConnection()) {
            conn.setAutoCommit(false);
            try (PreparedStatement statement = conn.prepareStatement(sql)) {
                statement.setObject(1, UUID.fromString((String)map.get("hostId")));
                statement.setString(2, (String)map.get("providerId"));

                int count = statement.executeUpdate();
                if (count == 0) {
                    throw new SQLException("failed to delete the auth provider with id " + map.get("providerId"));
                }
                conn.commit();
                result = Success.of((String)map.get("providerId"));
                insertNotification(event, true, null);

            } catch (SQLException e) {
                logger.error("SQLException:", e);
                conn.rollback();
                insertNotification(event, false, e.getMessage());
                result = Failure.of(new Status(SQL_EXCEPTION, e.getMessage()));
            } catch (Exception e) {
                logger.error("Exception:", e);
                conn.rollback();
                insertNotification(event, false, e.getMessage());
                result = Failure.of(new Status(GENERIC_EXCEPTION, e.getMessage()));
            }
        } catch (SQLException e) {
            logger.error("SQLException:", e);
            result = Failure.of(new Status(SQL_EXCEPTION, e.getMessage()));
        }
        return result;
    }

    @Override
    public Result<String> queryProviderKey(String providerId) {
        Result<String> result = null;
        String sql = "SELECT provider_id, kid, public_key, private_key, key_type, update_user, update_ts\n" +
                "FROM auth_provider_key_t\n" +
                "WHERE provider_id = ?\n";

        List<Map<String, Object>> providerKeys = new ArrayList<>();

        try (Connection connection = ds.getConnection();
             PreparedStatement preparedStatement = connection.prepareStatement(sql)) {
            preparedStatement.setString(1, providerId);
            try (ResultSet resultSet = preparedStatement.executeQuery()) {
                while (resultSet.next()) {
                    Map<String, Object> map = new HashMap<>();
                    map.put("providerId", resultSet.getString("provider_id"));
                    map.put("kid", resultSet.getString("kid"));
                    map.put("publicKey", resultSet.getString("public_key"));
                    map.put("privateKey", resultSet.getString("private_key"));
                    map.put("keyType", resultSet.getString("key_type"));
                    map.put("updateUser", resultSet.getString("update_user"));
                    // handling date properly
                    map.put("updateTs", resultSet.getTimestamp("update_ts") != null ? resultSet.getTimestamp("update_ts").toString() : null);
                    providerKeys.add(map);
                }
            }
            result = Success.of(JsonMapper.toJson(providerKeys));
        } catch (SQLException e) {
            logger.error("SQLException:", e);
            result = Failure.of(new Status(SQL_EXCEPTION, e.getMessage()));
        } catch (Exception e) {
            logger.error("Exception:", e);
            result = Failure.of(new Status(GENERIC_EXCEPTION, e.getMessage()));
        }
        return result;
    }

    @Override
    public Result<String> queryApp(int offset, int limit, String hostId, String appId, String appName, String appDesc,
                                   Boolean isKafkaApp, String operationOwner, String deliveryOwner) {
        Result<String> result = null;
        StringBuilder sqlBuilder = new StringBuilder();
        sqlBuilder.append("SELECT COUNT(*) OVER () AS total,\n" +
                "host_id, app_id, app_name, app_desc, is_kafka_app, operation_owner, delivery_owner\n" +
                "FROM app_t\n" +
                "WHERE host_id = ?\n");

        List<Object> parameters = new ArrayList<>();
        parameters.add(UUID.fromString(hostId));

        StringBuilder whereClause = new StringBuilder();

        addCondition(whereClause, parameters, "app_id", appId);
        addCondition(whereClause, parameters, "app_name", appName);
        addCondition(whereClause, parameters, "app_desc", appDesc);
        addCondition(whereClause, parameters, "is_kafka_app", isKafkaApp);
        addCondition(whereClause, parameters, "operation_owner", operationOwner);
        addCondition(whereClause, parameters, "delivery_owner", deliveryOwner);

        if (!whereClause.isEmpty()) {
            sqlBuilder.append("AND ").append(whereClause);
        }

        sqlBuilder.append(" ORDER BY app_id\n" +
                "LIMIT ? OFFSET ?");

        parameters.add(limit);
        parameters.add(offset);

        String sql = sqlBuilder.toString();
        int total = 0;
        List<Map<String, Object>> apps = new ArrayList<>();

        try (Connection connection = ds.getConnection();
             PreparedStatement preparedStatement = connection.prepareStatement(sql)) {

            for (int i = 0; i < parameters.size(); i++) {
                preparedStatement.setObject(i + 1, parameters.get(i));
            }


            boolean isFirstRow = true;
            try (ResultSet resultSet = preparedStatement.executeQuery()) {
                while (resultSet.next()) {
                    Map<String, Object> map = new HashMap<>();
                    if (isFirstRow) {
                        total = resultSet.getInt("total");
                        isFirstRow = false;
                    }
                    map.put("hostId", resultSet.getObject("host_id", UUID.class));
                    map.put("appId", resultSet.getString("app_id"));
                    map.put("appName", resultSet.getString("app_name"));
                    map.put("appDesc", resultSet.getString("app_desc"));
                    map.put("isKafkaApp", resultSet.getBoolean("is_kafka_app"));
                    map.put("operationOwner", resultSet.getString("operation_owner"));
                    map.put("deliveryOwner", resultSet.getString("delivery_owner"));
                    apps.add(map);
                }
            }

            Map<String, Object> resultMap = new HashMap<>();
            resultMap.put("total", total);
            resultMap.put("apps", apps);
            result = Success.of(JsonMapper.toJson(resultMap));

        } catch (SQLException e) {
            logger.error("SQLException:", e);
            result = Failure.of(new Status(SQL_EXCEPTION, e.getMessage()));
        } catch (Exception e) {
            logger.error("Exception:", e);
            result = Failure.of(new Status(GENERIC_EXCEPTION, e.getMessage()));
        }
        return result;
    }

    @Override
    public Result<String> queryClient(int offset, int limit, String hostId, String appId, String apiId,
                                      String clientId, String clientName,
                                      String clientType, String clientProfile, String clientScope,
                                      String customClaim, String redirectUri, String authenticateClass,
                                      String deRefClientId) {
        Result<String> result = null;
        StringBuilder sqlBuilder = new StringBuilder();
        sqlBuilder.append("SELECT COUNT(*) OVER () AS total,\n" +
                "client_id, host_id, app_id, api_id, client_name, client_type, client_profile, " +
                "client_scope, custom_claim, " +
                "redirect_uri, authenticate_class, deref_client_id, update_user, update_ts\n" +
                "FROM auth_client_t\n" +
                "WHERE host_id = ?\n");

        List<Object> parameters = new ArrayList<>();
        parameters.add(UUID.fromString(hostId));

        StringBuilder whereClause = new StringBuilder();

        addCondition(whereClause, parameters, "app_id", appId);
        addCondition(whereClause, parameters, "api_id", apiId);
        addCondition(whereClause, parameters, "client_id", clientId != null ? UUID.fromString(clientId) : null);
        addCondition(whereClause, parameters, "client_name", clientName);
        addCondition(whereClause, parameters, "client_type", clientType);
        addCondition(whereClause, parameters, "client_profile", clientProfile);
        addCondition(whereClause, parameters, "client_scope", clientScope);
        addCondition(whereClause, parameters, "custom_claim", customClaim);
        addCondition(whereClause, parameters, "redirect_uri", redirectUri);
        addCondition(whereClause, parameters, "authenticate_class", authenticateClass);
        addCondition(whereClause, parameters, "deref_client_id", deRefClientId != null ? UUID.fromString(deRefClientId) : null);

        if (!whereClause.isEmpty()) {
            sqlBuilder.append("AND ").append(whereClause);
        }

        sqlBuilder.append(" ORDER BY client_id\n" +
                "LIMIT ? OFFSET ?");

        parameters.add(limit);
        parameters.add(offset);

        String sql = sqlBuilder.toString();
        int total = 0;
        List<Map<String, Object>> clients = new ArrayList<>();

        try (Connection connection = ds.getConnection();
             PreparedStatement preparedStatement = connection.prepareStatement(sql)) {

            for (int i = 0; i < parameters.size(); i++) {
                preparedStatement.setObject(i + 1, parameters.get(i));
            }


            boolean isFirstRow = true;
            try (ResultSet resultSet = preparedStatement.executeQuery()) {
                while (resultSet.next()) {
                    Map<String, Object> map = new HashMap<>();
                    if (isFirstRow) {
                        total = resultSet.getInt("total");
                        isFirstRow = false;
                    }

                    map.put("hostId", resultSet.getObject("host_id", UUID.class));
                    map.put("clientId", resultSet.getObject("client_id", UUID.class));
                    map.put("appId", resultSet.getString("app_id"));
                    map.put("apiId", resultSet.getString("api_id"));
                    map.put("clientName", resultSet.getString("client_name"));
                    map.put("clientType", resultSet.getString("client_type"));
                    map.put("clientProfile", resultSet.getString("client_profile"));
                    map.put("clientScope", resultSet.getString("client_scope"));
                    map.put("customClaim", resultSet.getString("custom_claim"));
                    map.put("redirectUri", resultSet.getString("redirect_uri"));
                    map.put("authenticateClass", resultSet.getString("authenticate_class"));
                    map.put("deRefClientId", resultSet.getObject("deref_client_id", UUID.class));
                    map.put("updateUser", resultSet.getString("update_user"));
                    map.put("updateTs", resultSet.getObject("update_ts") != null ? resultSet.getObject("update_ts", OffsetDateTime.class) : null);
                    clients.add(map);
                }
            }

            Map<String, Object> resultMap = new HashMap<>();
            resultMap.put("total", total);
            resultMap.put("clients", clients);
            result = Success.of(JsonMapper.toJson(resultMap));

        } catch (SQLException e) {
            logger.error("SQLException:", e);
            result = Failure.of(new Status(SQL_EXCEPTION, e.getMessage()));
        } catch (Exception e) {
            logger.error("Exception:", e);
            result = Failure.of(new Status(GENERIC_EXCEPTION, e.getMessage()));
        }
        return result;
    }

    @Override
    public Result<String> createApp(Map<String, Object> event) {
        final String sql = "INSERT INTO app_t(host_id, app_id, app_name, app_desc, is_kafka_app, operation_owner, delivery_owner, update_user, update_ts) " +
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)";
        Result<String> result;
        Map<String, Object> map = (Map<String, Object>)event.get(PortalConstants.DATA);

        try (Connection conn = ds.getConnection()) {
            conn.setAutoCommit(false);
            try (PreparedStatement statement = conn.prepareStatement(sql)) {
                statement.setString(1, (String)event.get(Constants.HOST));
                statement.setString(2, (String)map.get("appId"));
                statement.setString(3, (String)map.get("appName"));
                if (map.containsKey("appDesc")) {
                    statement.setString(4, (String) map.get("appDesc"));
                } else {
                    statement.setNull(4, Types.VARCHAR);
                }
                if (map.containsKey("isKafkaApp")) {
                    statement.setBoolean(5, (Boolean) map.get("isKafkaApp"));
                } else {
                    statement.setNull(5, Types.BOOLEAN);
                }
                if (map.containsKey("operationOwner")) {
                    statement.setString(6, (String) map.get("operationOwner"));
                } else {
                    statement.setNull(6, Types.VARCHAR);
                }
                if (map.containsKey("deliveryOwner")) {
                    statement.setString(7, (String) map.get("deliveryOwner"));
                } else {
                    statement.setNull(7, Types.VARCHAR);
                }
                statement.setString(8, (String)event.get(Constants.USER));
                statement.setObject(9, OffsetDateTime.parse((String)event.get(CloudEventV1.TIME)));

                int count = statement.executeUpdate();
                if (count == 0) {
                    throw new SQLException("failed to insert the app with id " + map.get("appId"));
                }
                conn.commit();
                result = Success.of((String)map.get("appId"));
                insertNotification(event, true, null);

            }   catch (SQLException e) {
                logger.error("SQLException:", e);
                conn.rollback();
                insertNotification(event, false, e.getMessage());
                result = Failure.of(new Status(SQL_EXCEPTION, e.getMessage()));
            }  catch (Exception e) {
                logger.error("Exception:", e);
                conn.rollback();
                insertNotification(event, false, e.getMessage());
                result = Failure.of(new Status(GENERIC_EXCEPTION, e.getMessage()));
            }
        } catch (SQLException e) {
            logger.error("SQLException:", e);
            result = Failure.of(new Status(SQL_EXCEPTION, e.getMessage()));
        }
        return result;
    }

    @Override
    public Result<String> updateApp(Map<String, Object> event) {
        final String sql = "UPDATE app_t SET app_name = ?, app_desc = ?, is_kafka_app = ?, operation_owner = ?, delivery_owner = ?, update_user = ?, update_ts = ? " +
                "WHERE host_id = ? and app_id = ?";
        Result<String> result;
        Map<String, Object> map = (Map<String, Object>)event.get(PortalConstants.DATA);

        try (Connection conn = ds.getConnection()) {
            conn.setAutoCommit(false);
            try (PreparedStatement statement = conn.prepareStatement(sql)) {
                statement.setString(1, (String)map.get("appName"));

                if (map.containsKey("appDesc")) {
                    statement.setString(2, (String) map.get("appDesc"));
                } else {
                    statement.setNull(2, Types.VARCHAR);
                }

                if (map.containsKey("isKafkaApp")) {
                    statement.setBoolean(3, (Boolean) map.get("isKafkaApp"));
                } else {
                    statement.setNull(3, Types.BOOLEAN);
                }

                if (map.containsKey("operationOwner")) {
                    statement.setString(4, (String) map.get("operationOwner"));
                } else {
                    statement.setNull(4, Types.VARCHAR);
                }
                if (map.containsKey("deliveryOwner")) {
                    statement.setString(5, (String) map.get("deliveryOwner"));
                } else {
                    statement.setNull(5, Types.VARCHAR);
                }
                statement.setString(6, (String)event.get(Constants.USER));
                statement.setObject(7, OffsetDateTime.parse((String)event.get(CloudEventV1.TIME)));
                statement.setObject(8, UUID.fromString((String)map.get("hostId")));
                statement.setString(9, (String)map.get("appId"));

                int count = statement.executeUpdate();
                if (count == 0) {
                    throw new SQLException("failed to update the app with id " + (String)map.get("appId"));
                }
                conn.commit();
                result = Success.of((String)map.get("appId"));
                insertNotification(event, true, null);

            }  catch (SQLException e) {
                logger.error("SQLException:", e);
                conn.rollback();
                insertNotification(event, false, e.getMessage());
                result = Failure.of(new Status(SQL_EXCEPTION, e.getMessage()));
            }   catch (Exception e) {
                logger.error("Exception:", e);
                conn.rollback();
                insertNotification(event, false, e.getMessage());
                result = Failure.of(new Status(GENERIC_EXCEPTION, e.getMessage()));
            }
        } catch (SQLException e) {
            logger.error("SQLException:", e);
            result = Failure.of(new Status(SQL_EXCEPTION, e.getMessage()));
        }
        return result;
    }

    @Override
    public Result<String> deleteApp(Map<String, Object> event) {
        final String sql = "DELETE FROM app_t WHERE host_id = ? AND app_id = ?";
        Result<String> result;
        Map<String, Object> map = (Map<String, Object>)event.get(PortalConstants.DATA);

        try (Connection conn = ds.getConnection()) {
            conn.setAutoCommit(false);
            try (PreparedStatement statement = conn.prepareStatement(sql)) {
                statement.setObject(1, UUID.fromString((String)map.get("hostId")));
                statement.setString(2, (String)map.get("appId"));

                int count = statement.executeUpdate();
                if (count == 0) {
                    throw new SQLException("failed to delete the app with id " + (String)map.get("appId"));
                }
                conn.commit();
                result = Success.of((String)map.get("appId"));
                insertNotification(event, true, null);
            }  catch (SQLException e) {
                logger.error("SQLException:", e);
                conn.rollback();
                insertNotification(event, false, e.getMessage());
                result = Failure.of(new Status(SQL_EXCEPTION, e.getMessage()));
            } catch (Exception e) {
                logger.error("Exception:", e);
                conn.rollback();
                insertNotification(event, false, e.getMessage());
                result = Failure.of(new Status(GENERIC_EXCEPTION, e.getMessage()));
            }
        } catch (SQLException e) {
            logger.error("SQLException:", e);
            result = Failure.of(new Status(SQL_EXCEPTION, e.getMessage()));
        }
        return result;
    }

    @Override
    public Result<String> getAppIdLabel(String hostId) {
        Result<String> result = null;
        String sql = "SELECT app_id, app_name FROM app_t WHERE host_id = ?";
        List<Map<String, Object>> labels = new ArrayList<>();
        try (Connection connection = ds.getConnection();
             PreparedStatement preparedStatement = connection.prepareStatement(sql)) {
            preparedStatement.setObject(1, UUID.fromString(hostId));
            try (ResultSet resultSet = preparedStatement.executeQuery()) {
                while (resultSet.next()) {
                    Map<String, Object> map = new HashMap<>();
                    map.put("id", resultSet.getString("app_id"));
                    map.put("label", resultSet.getString("app_name"));
                    labels.add(map);
                }
            }
            result = Success.of(JsonMapper.toJson(labels));
        } catch (SQLException e) {
            logger.error("SQLException:", e);
            result = Failure.of(new Status(SQL_EXCEPTION, e.getMessage()));
        } catch (Exception e) {
            logger.error("Exception:", e);
            result = Failure.of(new Status(GENERIC_EXCEPTION, e.getMessage()));
        }
        return result;

    }

    @Override
    public Result<String> createClient(Map<String, Object> event) {
        final String insertUser = "INSERT INTO auth_client_t (host_id, app_id, api_id, client_name, client_id, " +
                "client_type, client_profile, client_secret, client_scope, custom_claim, redirect_uri, " +
                "authenticate_class, deref_client_id, update_user, update_ts) " +
                "VALUES (?, ?, ?, ?, ?,   ?, ?, ?, ?, ?,   ?, ?, ?, ?, ?)";
        Result<String> result = null;
        Map<String, Object> map = (Map<String, Object>)event.get(PortalConstants.DATA);
        try (Connection conn = ds.getConnection()) {
            conn.setAutoCommit(false);
            // no duplicate record, insert the user into database and write a success notification.
            try (PreparedStatement statement = conn.prepareStatement(insertUser)) {
                statement.setString(1, (String)event.get(Constants.HOST));
                String appId = (String)map.get("appId");
                if (appId != null && !appId.isEmpty()) {
                    statement.setString(2, appId);
                } else {
                    statement.setNull(2, NULL);
                }
                String apiId = (String)map.get("apiId");
                if (apiId != null && !apiId.isEmpty()) {
                    statement.setString(3, apiId);
                } else {
                    statement.setNull(3, NULL);
                }
                statement.setString(4, (String)map.get("clientName"));
                statement.setObject(5, UUID.fromString((String)map.get("clientId")));
                statement.setString(6, (String)map.get("clientType"));
                statement.setString(7, (String)map.get("clientProfile"));
                statement.setString(8, (String)map.get("clientSecret"));

                String clientScope = (String)map.get("clientScope");
                if (clientScope != null && !clientScope.isEmpty()) {
                    statement.setString(9, clientScope);
                } else {
                    statement.setNull(9, NULL);
                }
                String customClaim = (String)map.get("customClaim");
                if (customClaim != null && !customClaim.isEmpty()) {
                    statement.setString(10, customClaim);
                } else {
                    statement.setNull(10, NULL);
                }
                String redirectUri = (String)map.get("redirectUri");
                if (redirectUri != null && !redirectUri.isEmpty()) {
                    statement.setString(11, redirectUri);
                } else {
                    statement.setNull(11, NULL);
                }
                String authenticateClass = (String)map.get("authenticateClass");
                if (authenticateClass != null && !authenticateClass.isEmpty()) {
                    statement.setString(12, authenticateClass);
                } else {
                    statement.setNull(12, NULL);
                }
                String deRefClientId = (String)map.get("deRefClientId");
                if (deRefClientId != null && !deRefClientId.isEmpty()) {
                    statement.setObject(13, UUID.fromString(deRefClientId));
                } else {
                    statement.setNull(13, NULL);
                }
                statement.setString(14, (String)event.get(Constants.USER));
                statement.setObject(15, OffsetDateTime.parse((String)event.get(CloudEventV1.TIME)));
                int count = statement.executeUpdate();
                if (count == 0) {
                    throw new SQLException(String.format("no record is inserted for client %s", map.get("clientId")));
                }
                conn.commit();
                result = Success.of((String)map.get("clientId"));
                insertNotification(event, true, null);
            } catch (SQLException e) {
                logger.error("SQLException:", e);
                conn.rollback();
                insertNotification(event, false, e.getMessage());
                result = Failure.of(new Status(SQL_EXCEPTION, e.getMessage()));
            } catch (Exception e) {
                logger.error("Exception:", e);
                conn.rollback();
                insertNotification(event, false, e.getMessage());
                result = Failure.of(new Status(GENERIC_EXCEPTION, e.getMessage()));
            }
        } catch (SQLException e) {
            logger.error("SQLException:", e);
            result = Failure.of(new Status(SQL_EXCEPTION, e.getMessage()));
        }
        return result;
    }

    @Override
    public Result<String> updateClient(Map<String, Object> event) {
        final String updateApplication = "UPDATE auth_client_t SET app_id = ?, api_id = ?, client_name = ?, " +
                "client_type = ?, client_profile = ?, " +
                "client_scope = ?, custom_claim = ?, redirect_uri = ?, authenticate_class = ?, " +
                "deref_client_id = ?, update_user = ?, update_ts = ? " +
                "WHERE host_id = ? AND client_id = ?";

        Result<String> result = null;
        Map<String, Object> map = (Map<String, Object>)event.get(PortalConstants.DATA);
        try (Connection conn = ds.getConnection()) {
            conn.setAutoCommit(false);

            try (PreparedStatement statement = conn.prepareStatement(updateApplication)) {
                String appId = (String)map.get("appId");
                if (appId != null && !appId.isEmpty()) {
                    statement.setString(1, appId);
                } else {
                    statement.setNull(1, NULL);
                }
                String apiId = (String)map.get("apiId");
                if (apiId != null && !apiId.isEmpty()) {
                    statement.setString(2, apiId);
                } else {
                    statement.setNull(2, NULL);
                }
                statement.setString(3, (String)map.get("clientName"));
                statement.setString(4, (String)map.get("clientType"));
                statement.setString(5, (String)map.get("clientProfile"));
                String clientScope = (String)map.get("clientScope");
                if (clientScope != null && !clientScope.isEmpty()) {
                    statement.setString(6, clientScope);
                } else {
                    statement.setNull(6, NULL);
                }
                String customClaim = (String)map.get("customClaim");
                if (customClaim != null && !customClaim.isEmpty()) {
                    statement.setString(7, customClaim);
                } else {
                    statement.setNull(7, NULL);
                }
                String redirectUri = (String)map.get("redirectUri");
                if (redirectUri != null && !redirectUri.isEmpty()) {
                    statement.setString(8, redirectUri);
                } else {
                    statement.setNull(8, NULL);
                }
                String authenticateClass = (String)map.get("authenticateClass");
                if (authenticateClass != null && !authenticateClass.isEmpty()) {
                    statement.setString(9, authenticateClass);
                } else {
                    statement.setNull(9, NULL);
                }
                String deRefClientId = (String)map.get("deRefClientId");
                if (deRefClientId != null && !deRefClientId.isEmpty()) {
                    statement.setObject(10, UUID.fromString(deRefClientId));
                } else {
                    statement.setNull(10, NULL);
                }
                statement.setString(11, (String)event.get(Constants.USER));
                statement.setObject(12, OffsetDateTime.parse((String)event.get(CloudEventV1.TIME)));
                statement.setObject(13, UUID.fromString((String)map.get("hostId")));
                statement.setObject(14, UUID.fromString((String)map.get("clientId")));
                int count = statement.executeUpdate();
                if (count == 0) {
                    // no record is updated, write an error notification.
                    throw new SQLException(String.format("no record is updated for client %s", map.get("clientId")));
                }
                conn.commit();
                result = Success.of((String)map.get("clientId"));
                insertNotification(event, true, null);
            } catch (SQLException e) {
                logger.error("SQLException:", e);
                conn.rollback();
                insertNotification(event, false, e.getMessage());
                result = Failure.of(new Status(SQL_EXCEPTION, e.getMessage()));
            } catch (Exception e) {
                logger.error("Exception:", e);
                conn.rollback();
                insertNotification(event, false, e.getMessage());
                result = Failure.of(new Status(GENERIC_EXCEPTION, e.getMessage()));
            }
        } catch (SQLException e) {
            logger.error("SQLException:", e);
            result = Failure.of(new Status(SQL_EXCEPTION, e.getMessage()));
        }
        return result;
    }

    @Override
    public Result<String> deleteClient(Map<String, Object> event) {
        final String deleteApp = "DELETE from auth_client_t WHERE host_id = ? AND client_id = ?";
        Result<String> result;
        Map<String, Object> map = (Map<String, Object>)event.get(PortalConstants.DATA);
        try (Connection conn = ds.getConnection()) {
            conn.setAutoCommit(false);
            try (PreparedStatement statement = conn.prepareStatement(deleteApp)) {
                statement.setObject(1, UUID.fromString((String)map.get("hostId")));
                statement.setObject(2, UUID.fromString((String)map.get("clientId")));
                int count = statement.executeUpdate();
                if (count == 0) {
                    // no record is deleted, write an error notification.
                    throw new SQLException(String.format("no record is deleted for client %s", map.get("clientId")));
                }
                conn.commit();
                result = Success.of((String)map.get("clientId"));
                insertNotification(event, true, null);
            } catch (SQLException e) {
                logger.error("SQLException:", e);
                conn.rollback();
                insertNotification(event, false, e.getMessage());
                result = Failure.of(new Status(SQL_EXCEPTION, e.getMessage()));
            } catch (Exception e) {
                logger.error("Exception:", e);
                conn.rollback();
                insertNotification(event, false, e.getMessage());
                result = Failure.of(new Status(GENERIC_EXCEPTION, e.getMessage()));
            }
        } catch (SQLException e) {
            logger.error("SQLException:", e);
            result = Failure.of(new Status(SQL_EXCEPTION, e.getMessage()));
        }
        return result;
    }

    @Override
    public Result<String> queryClientByClientId(String clientId) {
        if(logger.isTraceEnabled()) logger.trace("queryClientByClientId: clientId = {}", clientId);
        Result<String> result;
        String sql =
                "SELECT host_id, app_id, api_id, client_name, client_id, client_type, client_profile, client_secret, " +
                        "client_scope, custom_claim,\n" +
                        "redirect_uri, authenticate_class, deref_client_id, update_user, update_ts\n" +
                        "FROM auth_client_t \n" +
                        "WHERE client_id = ?";
        try (final Connection conn = ds.getConnection()) {
            Map<String, Object> map = new HashMap<>();
            try (PreparedStatement statement = conn.prepareStatement(sql)) {
                statement.setObject(1, UUID.fromString(clientId));
                try (ResultSet resultSet = statement.executeQuery()) {
                    if (resultSet.next()) {
                        map.put("hostId", resultSet.getObject("host_id", UUID.class));
                        map.put("appId", resultSet.getString("app_id"));
                        map.put("apiId", resultSet.getString("api_id"));
                        map.put("clientName", resultSet.getString("client_name"));
                        map.put("clientId", resultSet.getObject("client_id", UUID.class));
                        map.put("clientType", resultSet.getString("client_type"));
                        map.put("clientProfile", resultSet.getString("client_profile"));
                        map.put("clientSecret", resultSet.getString("client_secret"));
                        map.put("clientScope", resultSet.getString("client_scope"));
                        map.put("customClaim", resultSet.getString("custom_claim"));
                        map.put("redirectUri", resultSet.getString("redirect_uri"));
                        map.put("authenticateClass", resultSet.getString("authenticate_class"));
                        map.put("deRefClientId", resultSet.getObject("deref_client_id", UUID.class));
                        map.put("updateUser", resultSet.getString("update_user"));
                        map.put("updateTs", resultSet.getObject("update_ts") != null ? resultSet.getObject("update_ts", OffsetDateTime.class) : null);
                    }
                }
            }
            if (map.isEmpty())
                result = Failure.of(new Status(OBJECT_NOT_FOUND, "application with clientId ", clientId));
            else
                result = Success.of(JsonMapper.toJson(map));
        } catch (SQLException e) {
            logger.error("SQLException:", e);
            result = Failure.of(new Status(SQL_EXCEPTION, e.getMessage()));
        } catch (Exception e) {
            logger.error("Exception:", e);
            result = Failure.of(new Status(GENERIC_EXCEPTION, e.getMessage()));
        }
        return result;

    }

    @Override
    public Result<String> queryClientByProviderClientId(String providerId, String clientId) {
        Result<String> result;
        String sql =
                "SELECT c.host_id, a.provider_id, a.client_id, c.client_type, c.client_profile, c.client_secret, \n" +
                        "c.client_scope, c.custom_claim, c.redirect_uri, c.authenticate_class, c.deref_client_id\n" +
                        "FROM auth_client_t c, auth_provider_client_t a\n" +
                        "WHERE c.host_id = a.host_id AND c.client_id = a.client_id\n" +
                        "AND a.provider_id = ?\n" +
                        "AND a.client_id = ?\n";
        try (final Connection conn = ds.getConnection()) {
            Map<String, Object> map = new HashMap<>();
            try (PreparedStatement statement = conn.prepareStatement(sql)) {
                statement.setString(1, providerId);
                statement.setObject(2, UUID.fromString(clientId));

                try (ResultSet resultSet = statement.executeQuery()) {
                    if (resultSet.next()) {
                        map.put("hostId", resultSet.getObject("host_id", UUID.class));
                        map.put("providerId", resultSet.getString("provider_id"));
                        map.put("clientId", resultSet.getObject("client_id", UUID.class));
                        map.put("clientType", resultSet.getString("client_type"));
                        map.put("clientProfile", resultSet.getString("client_profile"));
                        map.put("clientSecret", resultSet.getString("client_secret"));
                        map.put("clientScope", resultSet.getString("client_scope"));
                        map.put("customClaim", resultSet.getString("custom_claim"));
                        map.put("redirectUri", resultSet.getString("redirect_uri"));
                        map.put("authenticateClass", resultSet.getString("authenticate_class"));
                        map.put("deRefClientId", resultSet.getObject("deref_client_id", UUID.class));
                    }
                }
            }
            if (map.isEmpty())
                result = Failure.of(new Status(OBJECT_NOT_FOUND, "client", "providerId " +  providerId + "clientId " + clientId));
            else
                result = Success.of(JsonMapper.toJson(map));
        } catch (SQLException e) {
            logger.error("SQLException:", e);
            result = Failure.of(new Status(SQL_EXCEPTION, e.getMessage()));
        } catch (Exception e) {
            logger.error("Exception:", e);
            result = Failure.of(new Status(GENERIC_EXCEPTION, e.getMessage()));
        }
        return result;
    }

    @Override
    public Result<String> queryClientByHostAppId(String host_id, String appId) {
        Result<String> result;
        String sql =
                "SELECT host_id, app_id, client_id, client_type, client_profile, client_scope, custom_claim, \n" +
                        "redirect_uri, authenticate_class, deref_client_id, update_user, update_ts \n" +
                        "FROM auth_client_t c\n" +
                        "WHERE host_id = ? AND app_id = ?";
        try (final Connection conn = ds.getConnection()) {
            Map<String, Object> map = new HashMap<>();
            try (PreparedStatement statement = conn.prepareStatement(sql)) {
                statement.setString(1, host_id);
                statement.setString(2, appId);
                try (ResultSet resultSet = statement.executeQuery()) {
                    if (resultSet.next()) {
                        map.put("hostId", resultSet.getObject("host_id", UUID.class));
                        map.put("appId", resultSet.getString("app_id"));
                        map.put("clientId", resultSet.getObject("client_id", UUID.class));
                        map.put("clientType", resultSet.getString("client_type"));
                        map.put("clientProfile", resultSet.getString("client_profile"));
                        map.put("clientScope", resultSet.getString("client_scope"));
                        map.put("customClaim", resultSet.getString("custom_claim"));
                        map.put("redirectUri", resultSet.getString("redirect_uri"));
                        map.put("authenticateClass", resultSet.getString("authenticate_class"));
                        map.put("deRefClientId", resultSet.getObject("deref_client_id", UUID.class));
                        map.put("updateUser", resultSet.getString("update_user"));
                        map.put("updateTs", resultSet.getObject("update_ts") != null ? resultSet.getObject("update_ts", OffsetDateTime.class) : null);
                    }
                }
            }
            if (map.size() == 0)
                result = Failure.of(new Status(OBJECT_NOT_FOUND, "client with appId ", appId));
            else
                result = Success.of(JsonMapper.toJson(map));
        } catch (SQLException e) {
            logger.error("SQLException:", e);
            result = Failure.of(new Status(SQL_EXCEPTION, e.getMessage()));
        } catch (Exception e) {
            logger.error("Exception:", e);
            result = Failure.of(new Status(GENERIC_EXCEPTION, e.getMessage()));
        }
        return result;

    }

    @Override
    public Result<String> createService(Map<String, Object> event) {
        final String insertUser = "INSERT INTO api_t (host_id, api_id, api_name, " +
                "api_desc, operation_owner, delivery_owner, region, business_group, " +
                "lob, platform, capability, git_repo, api_tags, " +
                "api_status, update_user, update_ts) " +
                "VALUES (?, ?, ?, ?, ?,   ?, ?, ?, ?, ?,   ?, ?, ?, ?, ?,   ?)";
        Result<String> result = null;
        Map<String, Object> map = (Map<String, Object>)event.get(PortalConstants.DATA);
        try (Connection conn = ds.getConnection()) {
            conn.setAutoCommit(false);
            // no duplicate record, insert the user into database and write a success notification.
            try (PreparedStatement statement = conn.prepareStatement(insertUser)) {
                statement.setString(1, (String)event.get(Constants.HOST));
                statement.setString(2, (String)map.get("apiId"));
                statement.setString(3, (String)map.get("apiName"));
                if (map.containsKey("apiDesc")) {
                    String apiDesc = (String) map.get("apiDesc");
                    if (apiDesc != null && !apiDesc.trim().isEmpty()) {
                        statement.setString(4, apiDesc);
                    } else {
                        statement.setNull(4, Types.VARCHAR);
                    }
                } else {
                    statement.setNull(4, Types.VARCHAR);
                }
                if (map.containsKey("operationOwner")) {
                    String operationOwner = (String) map.get("operationOwner");
                    if(operationOwner != null && !operationOwner.trim().isEmpty()) {
                        statement.setString(5, operationOwner);
                    } else {
                        statement.setNull(5, Types.VARCHAR);
                    }
                } else {
                    statement.setNull(5, Types.VARCHAR);
                }
                if (map.containsKey("deliveryOwner")) {
                    String deliveryOwner = (String) map.get("deliveryOwner");
                    if(deliveryOwner != null && !deliveryOwner.trim().isEmpty()) {
                        statement.setString(6, deliveryOwner);
                    } else {
                        statement.setNull(6, Types.VARCHAR);
                    }
                } else {
                    statement.setNull(6, Types.VARCHAR);
                }
                if (map.containsKey("region")) {
                    String region = (String) map.get("region");
                    if(region != null && !region.trim().isEmpty()) {
                        statement.setString(7, region);
                    } else {
                        statement.setNull(7, Types.VARCHAR);
                    }
                } else {
                    statement.setNull(7, Types.VARCHAR);
                }
                if (map.containsKey("businessGroup")) {
                    String businessGroup = (String) map.get("businessGroup");
                    if(businessGroup != null && !businessGroup.trim().isEmpty()) {
                        statement.setString(8, businessGroup);
                    } else {
                        statement.setNull(8, Types.VARCHAR);
                    }
                } else {
                    statement.setNull(8, Types.VARCHAR);
                }
                if (map.containsKey("lob")) {
                    String lob = (String) map.get("lob");
                    if(lob != null && !lob.trim().isEmpty()) {
                        statement.setString(9, lob);
                    } else {
                        statement.setNull(9, Types.VARCHAR);
                    }
                } else {
                    statement.setNull(9, Types.VARCHAR);
                }
                if (map.containsKey("platform")) {
                    String platform = (String) map.get("platform");
                    if(platform != null && !platform.trim().isEmpty()) {
                        statement.setString(10, platform);
                    } else {
                        statement.setNull(10, Types.VARCHAR);
                    }
                } else {
                    statement.setNull(10, Types.VARCHAR);
                }
                if (map.containsKey("capability")) {
                    String capability = (String) map.get("capability");
                    if(capability != null && !capability.trim().isEmpty()) {
                        statement.setString(11, capability);
                    } else {
                        statement.setNull(11, Types.VARCHAR);
                    }
                } else {
                    statement.setNull(11, Types.VARCHAR);
                }
                if (map.containsKey("gitRepo")) {
                    String gitRepo = (String) map.get("gitRepo");
                    if(gitRepo != null && !gitRepo.trim().isEmpty()) {
                        statement.setString(12, gitRepo);
                    } else {
                        statement.setNull(12, Types.VARCHAR);
                    }
                } else {
                    statement.setNull(12, Types.VARCHAR);
                }
                if (map.containsKey("apiTags")) {
                    String apiTags = (String) map.get("apiTags");
                    if(apiTags != null && !apiTags.trim().isEmpty()) {
                        statement.setString(13, apiTags);
                    } else {
                        statement.setNull(13, Types.VARCHAR);
                    }
                } else {
                    statement.setNull(13, Types.VARCHAR);
                }

                statement.setString(14, (String)map.get("apiStatus"));
                statement.setString(15, (String)event.get(Constants.USER));
                statement.setObject(16, OffsetDateTime.parse((String)event.get(CloudEventV1.TIME)));
                int count = statement.executeUpdate();
                if (count == 0) {
                    throw new SQLException(String.format("no record is inserted for api %s", map.get("apiId")));
                }
                conn.commit();
                result = Success.of((String)map.get("apiId"));
                insertNotification(event, true, null);
            } catch (SQLException e) {
                logger.error("SQLException:", e);
                conn.rollback();
                insertNotification(event, false, e.getMessage());
                result = Failure.of(new Status(SQL_EXCEPTION, e.getMessage()));
            } catch (Exception e) {
                logger.error("Exception:", e);
                conn.rollback();
                result = Failure.of(new Status(GENERIC_EXCEPTION, e.getMessage()));
            }
        } catch (SQLException e) {
            logger.error("SQLException:", e);
            result = Failure.of(new Status(SQL_EXCEPTION, e.getMessage()));
        }
        return result;
    }

    @Override
    public Result<String> updateService(Map<String, Object> event) {
        final String updateApi = "UPDATE api_t SET api_name = ?, api_desc = ? " +
                "operation_owner = ?, delivery_owner = ?, region = ?, business_group = ?, lob = ?, platform = ?, " +
                "capability = ?, git_repo = ?, api_tags = ?, api_status = ?,  update_user = ?, update_ts = ? " +
                "WHERE host_id = ? AND api_id = ?";

        Result<String> result = null;
        Map<String, Object> map = (Map<String, Object>)event.get(PortalConstants.DATA);
        try (Connection conn = ds.getConnection()) {
            conn.setAutoCommit(false);

            try (PreparedStatement statement = conn.prepareStatement(updateApi)) {
                statement.setString(1, (String)map.get("apiName"));
                if (map.containsKey("apiDesc")) {
                    String apiDesc = (String) map.get("apiDesc");
                    if (apiDesc != null && !apiDesc.trim().isEmpty()) {
                        statement.setString(2, apiDesc);
                    } else {
                        statement.setNull(2, Types.VARCHAR);
                    }
                } else {
                    statement.setNull(2, Types.VARCHAR);
                }
                if (map.containsKey("operationOwner")) {
                    String operationOwner = (String) map.get("operationOwner");
                    if(operationOwner != null && !operationOwner.trim().isEmpty()) {
                        statement.setString(3, operationOwner);
                    } else {
                        statement.setNull(3, Types.VARCHAR);
                    }
                } else {
                    statement.setNull(3, Types.VARCHAR);
                }
                if (map.containsKey("deliveryOwner")) {
                    String deliveryOwner = (String) map.get("deliveryOwner");
                    if(deliveryOwner != null && !deliveryOwner.trim().isEmpty()) {
                        statement.setString(4, deliveryOwner);
                    } else {
                        statement.setNull(4, Types.VARCHAR);
                    }
                } else {
                    statement.setNull(4, Types.VARCHAR);
                }
                if (map.containsKey("region")) {
                    String region = (String) map.get("region");
                    if(region != null && !region.trim().isEmpty()) {
                        statement.setString(5, region);
                    } else {
                        statement.setNull(5, Types.VARCHAR);
                    }
                } else {
                    statement.setNull(5, Types.VARCHAR);
                }
                if (map.containsKey("businessGroup")) {
                    String businessGroup = (String) map.get("businessGroup");
                    if(businessGroup != null && !businessGroup.trim().isEmpty()) {
                        statement.setString(6, businessGroup);
                    } else {
                        statement.setNull(6, Types.VARCHAR);
                    }
                } else {
                    statement.setNull(6, Types.VARCHAR);
                }
                if (map.containsKey("lob")) {
                    String lob = (String) map.get("lob");
                    if(lob != null && !lob.trim().isEmpty()) {
                        statement.setString(7, lob);
                    } else {
                        statement.setNull(7, Types.VARCHAR);
                    }
                } else {
                    statement.setNull(7, Types.VARCHAR);
                }
                if (map.containsKey("platform")) {
                    String platform = (String) map.get("platform");
                    if(platform != null && !platform.trim().isEmpty()) {
                        statement.setString(8, platform);
                    } else {
                        statement.setNull(8, Types.VARCHAR);
                    }
                } else {
                    statement.setNull(8, Types.VARCHAR);
                }
                if (map.containsKey("capability")) {
                    String capability = (String) map.get("capability");
                    if(capability != null && !capability.trim().isEmpty()) {
                        statement.setString(9, capability);
                    } else {
                        statement.setNull(9, Types.VARCHAR);
                    }
                } else {
                    statement.setNull(9, Types.VARCHAR);
                }
                if (map.containsKey("gitRepo")) {
                    String gitRepo = (String) map.get("gitRepo");
                    if(gitRepo != null && !gitRepo.trim().isEmpty()) {
                        statement.setString(10, gitRepo);
                    } else {
                        statement.setNull(10, Types.VARCHAR);
                    }
                } else {
                    statement.setNull(10, Types.VARCHAR);
                }
                if (map.containsKey("apiTags")) {
                    String apiTags = (String) map.get("apiTags");
                    if(apiTags != null && !apiTags.trim().isEmpty()) {
                        statement.setString(11, apiTags);
                    } else {
                        statement.setNull(11, Types.VARCHAR);
                    }
                } else {
                    statement.setNull(11, Types.VARCHAR);
                }
                statement.setString(12, (String)map.get("apiStatus"));
                statement.setString(13, (String)event.get(Constants.USER));
                statement.setObject(14, OffsetDateTime.parse((String)event.get(CloudEventV1.TIME)));
                statement.setString(15, (String)event.get(Constants.HOST));
                statement.setString(16, (String)map.get("apiId"));

                int count = statement.executeUpdate();
                if (count == 0) {
                    // no record is updated, write an error notification.
                    throw new SQLException(String.format("no record is updated for api %s", map.get("apiId")));
                }
                conn.commit();
                result = Success.of((String)map.get("apiId"));
                insertNotification(event, true, null);
            } catch (SQLException e) {
                logger.error("SQLException:", e);
                conn.rollback();
                insertNotification(event, false, e.getMessage());
                result = Failure.of(new Status(SQL_EXCEPTION, e.getMessage()));
            } catch (Exception e) {
                logger.error("Exception:", e);
                conn.rollback();
                insertNotification(event, false, e.getMessage());
                result = Failure.of(new Status(GENERIC_EXCEPTION, e.getMessage()));
            }
        } catch (SQLException e) {
            logger.error("SQLException:", e);
            result = Failure.of(new Status(SQL_EXCEPTION, e.getMessage()));
        }
        return result;

    }

    @Override
    public Result<String> deleteService(Map<String, Object> event) {
        final String deleteApplication = "DELETE from api_t WHERE host_id = ? AND api_id = ?";
        Result<String> result;
        Map<String, Object> map = (Map<String, Object>)event.get(PortalConstants.DATA);
        try (Connection conn = ds.getConnection()) {
            conn.setAutoCommit(false);
            try (PreparedStatement statement = conn.prepareStatement(deleteApplication)) {
                statement.setString(1, (String)event.get(Constants.HOST));
                statement.setString(2, (String)map.get("apiId"));
                int count = statement.executeUpdate();
                if (count == 0) {
                    // no record is deleted, write an error notification.
                    throw new SQLException(String.format("no record is deleted for api %s", map.get("apiId")));
                }
                conn.commit();
                result = Success.of((String)map.get("apiId"));
                insertNotification(event, true, null);
            } catch (SQLException e) {
                logger.error("SQLException:", e);
                conn.rollback();
                insertNotification(event, false, e.getMessage());
                result = Failure.of(new Status(SQL_EXCEPTION, e.getMessage()));
            } catch (Exception e) {
                logger.error("Exception:", e);
                conn.rollback();
                insertNotification(event, false, e.getMessage());
                result = Failure.of(new Status(GENERIC_EXCEPTION, e.getMessage()));
            }
        } catch (SQLException e) {
            logger.error("SQLException:", e);
            result = Failure.of(new Status(SQL_EXCEPTION, e.getMessage()));
        }
        return result;
    }

    @Override
    public Result<String> queryService(int offset, int limit, String hostId, String apiId, String apiName,
                                       String apiDesc, String operationOwner, String deliveryOwner, String region, String businessGroup,
                                       String lob, String platform, String capability, String gitRepo, String apiTags, String apiStatus) {
        Result<String> result = null;
        StringBuilder sqlBuilder = new StringBuilder();
        sqlBuilder.append("SELECT COUNT(*) OVER () AS total,\n" +
                "host_id, api_id, api_name,\n" +
                "api_desc, operation_owner, delivery_owner, region, business_group,\n" +
                "lob, platform, capability, git_repo, api_tags, api_status\n" +
                "FROM api_t\n" +
                "WHERE host_id = ?\n");


        List<Object> parameters = new ArrayList<>();
        parameters.add(UUID.fromString(hostId));

        StringBuilder whereClause = new StringBuilder();

        addCondition(whereClause, parameters, "api_id", apiId);
        addCondition(whereClause, parameters, "api_name", apiName);
        addCondition(whereClause, parameters, "api_desc", apiDesc);
        addCondition(whereClause, parameters, "operation_owner", operationOwner);
        addCondition(whereClause, parameters, "delivery_owner", deliveryOwner);
        addCondition(whereClause, parameters, "region", region);
        addCondition(whereClause, parameters, "business_group", businessGroup);
        addCondition(whereClause, parameters, "lob", lob);
        addCondition(whereClause, parameters, "platform", platform);
        addCondition(whereClause, parameters, "capability", capability);
        addCondition(whereClause, parameters, "git_repo", gitRepo);
        addCondition(whereClause, parameters, "api_tags", apiTags);
        addCondition(whereClause, parameters, "api_status", apiStatus);

        if (!whereClause.isEmpty()) {
            sqlBuilder.append("AND ").append(whereClause);
        }


        sqlBuilder.append(" ORDER BY api_id\n" +
                "LIMIT ? OFFSET ?");

        parameters.add(limit);
        parameters.add(offset);
        String sql = sqlBuilder.toString();
        int total = 0;
        List<Map<String, Object>> services = new ArrayList<>();

        try (Connection connection = ds.getConnection();
             PreparedStatement preparedStatement = connection.prepareStatement(sql)) {

            for (int i = 0; i < parameters.size(); i++) {
                preparedStatement.setObject(i + 1, parameters.get(i));
            }


            boolean isFirstRow = true;
            try (ResultSet resultSet = preparedStatement.executeQuery()) {
                while (resultSet.next()) {
                    Map<String, Object> map = new HashMap<>();
                    // only get the total once as it is the same for all rows.
                    if (isFirstRow) {
                        total = resultSet.getInt("total");
                        isFirstRow = false;
                    }

                    map.put("hostId", resultSet.getObject("host_id", UUID.class));
                    map.put("apiId", resultSet.getString("api_id"));
                    map.put("apiName", resultSet.getString("api_name"));
                    map.put("apiDesc", resultSet.getString("api_desc"));
                    map.put("operationOwner", resultSet.getString("operation_owner"));
                    map.put("deliveryOwner", resultSet.getString("delivery_owner"));
                    map.put("region", resultSet.getString("region"));
                    map.put("businessGroup", resultSet.getString("business_group"));
                    map.put("lob", resultSet.getString("lob"));
                    map.put("platform", resultSet.getString("platform"));
                    map.put("capability", resultSet.getString("capability"));
                    map.put("gitRepo", resultSet.getString("git_repo"));
                    map.put("apiTags", resultSet.getString("api_tags"));
                    map.put("apiStatus", resultSet.getString("api_status"));
                    services.add(map);
                }
            }
            // now, we have the total and the list of tables, we need to put them into a map.
            Map<String, Object> resultMap = new HashMap<>();
            resultMap.put("total", total);
            resultMap.put("services", services);
            result = Success.of(JsonMapper.toJson(resultMap));
        } catch (SQLException e) {
            logger.error("SQLException:", e);
            result = Failure.of(new Status(SQL_EXCEPTION, e.getMessage()));
        } catch (Exception e) {
            logger.error("Exception:", e);
            result = Failure.of(new Status(GENERIC_EXCEPTION, e.getMessage()));
        }
        return result;
    }

    @Override
    public Result<String> queryApiLabel(String hostId) {
        Result<String> result = null;
        String sql = "SELECT api_id, api_name FROM api_t WHERE host_id = ?";
        List<Map<String, Object>> labels = new ArrayList<>();
        try (Connection connection = ds.getConnection();
             PreparedStatement preparedStatement = connection.prepareStatement(sql)) {
            preparedStatement.setObject(1, UUID.fromString(hostId));
            try (ResultSet resultSet = preparedStatement.executeQuery()) {
                while (resultSet.next()) {
                    Map<String, Object> map = new HashMap<>();
                    map.put("id", resultSet.getString("api_id"));
                    map.put("label", resultSet.getString("api_name"));
                    labels.add(map);
                }
            }
            result = Success.of(JsonMapper.toJson(labels));
        } catch (SQLException e) {
            logger.error("SQLException:", e);
            result = Failure.of(new Status(SQL_EXCEPTION, e.getMessage()));
        } catch (Exception e) {
            logger.error("Exception:", e);
            result = Failure.of(new Status(GENERIC_EXCEPTION, e.getMessage()));
        }
        return result;
    }

    @Override
    public Result<String> queryApiVersionLabel(String hostId, String apiId) {
        Result<String> result = null;
        String sql = "SELECT api_version FROM api_version_t WHERE host_id = ? AND api_id = ?";
        List<Map<String, Object>> versions = new ArrayList<>();
        try (Connection connection = ds.getConnection();
             PreparedStatement preparedStatement = connection.prepareStatement(sql)) {
            preparedStatement.setObject(1, UUID.fromString(hostId));
            preparedStatement.setString(2, apiId);
            try (ResultSet resultSet = preparedStatement.executeQuery()) {
                while (resultSet.next()) {
                    Map<String, Object> map = new HashMap<>();
                    String id = resultSet.getString("api_version");
                    map.put("id", id);
                    map.put("label", id);
                    versions.add(map);
                }
            }
            result = Success.of(JsonMapper.toJson(versions));
        } catch (SQLException e) {
            logger.error("SQLException:", e);
            result = Failure.of(new Status(SQL_EXCEPTION, e.getMessage()));
        } catch (Exception e) {
            logger.error("Exception:", e);
            result = Failure.of(new Status(GENERIC_EXCEPTION, e.getMessage()));
        }
        return result;
    }

    @Override
    public Result<String> queryEndpointLabel(String hostId, String apiId, String apiVersion) {
        Result<String> result = null;
        String sql = "SELECT endpoint FROM api_endpoint_t WHERE host_id = ? AND api_id = ? AND api_version = ?";
        List<Map<String, Object>> labels = new ArrayList<>();
        try (Connection connection = ds.getConnection();
             PreparedStatement preparedStatement = connection.prepareStatement(sql)) {
            preparedStatement.setObject(1, UUID.fromString(hostId));
            preparedStatement.setString(2, apiId);
            preparedStatement.setString(3, apiVersion);
            try (ResultSet resultSet = preparedStatement.executeQuery()) {
                while (resultSet.next()) {
                    Map<String, Object> map = new HashMap<>();
                    String id = resultSet.getString("endpoint");
                    map.put("id", id);
                    map.put("label", id);
                    labels.add(map);
                }
            }
            result = Success.of(JsonMapper.toJson(labels));
        } catch (SQLException e) {
            logger.error("SQLException:", e);
            result = Failure.of(new Status(SQL_EXCEPTION, e.getMessage()));
        } catch (Exception e) {
            logger.error("Exception:", e);
            result = Failure.of(new Status(GENERIC_EXCEPTION, e.getMessage()));
        }
        return result;
    }

    @Override
    public Result<String> createServiceVersion(Map<String, Object> event, List<Map<String, Object>> endpoints) {
        final String insertUser = "INSERT INTO api_version_t (host_id, api_id, api_version, api_type, service_id, api_version_desc, " +
                "spec_link, spec, update_user, update_ts) " +
                "VALUES (?, ?, ?, ?, ?,   ?, ?, ?, ?, ?)";
        final String insertEndpoint = "INSERT INTO api_endpoint_t (host_id, api_id, api_version, endpoint, http_method, " +
                "endpoint_path, endpoint_name, endpoint_desc, update_user, update_ts) " +
                "VALUES (?,? ,?, ?, ?,  ?, ?, ?, ?, ?)";
        final String insertScope = "INSERT INTO api_endpoint_scope_t (host_id, api_id, api_version, endpoint, scope, scope_desc, " +
                "update_user, update_ts) " +
                "VALUES (?, ?, ?, ?, ?,  ?, ?, ?)";

        Result<String> result = null;
        Map<String, Object> map = (Map<String, Object>)event.get(PortalConstants.DATA);
        try (Connection conn = ds.getConnection()) {
            conn.setAutoCommit(false);
            // no duplicate record, insert the user into database and write a success notification.
            try (PreparedStatement statement = conn.prepareStatement(insertUser)) {
                statement.setString(1, (String)event.get(Constants.HOST));
                statement.setString(2, (String)map.get("apiId"));
                statement.setString(3, (String)map.get("apiVersion"));
                statement.setString(4, (String)map.get("apiType"));
                statement.setString(5, (String)map.get("serviceId"));

                if (map.containsKey("apiVersionDesc")) {
                    String apiDesc = (String) map.get("apiVersionDesc");
                    if (apiDesc != null && !apiDesc.trim().isEmpty()) {
                        statement.setString(6, apiDesc);
                    } else {
                        statement.setNull(6, Types.VARCHAR);
                    }
                } else {
                    statement.setNull(6, Types.VARCHAR);
                }
                if (map.containsKey("specLink")) {
                    String specLink = (String) map.get("specLink");
                    if (specLink != null && !specLink.trim().isEmpty()) {
                        statement.setString(7, specLink);
                    } else {
                        statement.setNull(7, Types.VARCHAR);
                    }
                } else {
                    statement.setNull(7, Types.VARCHAR);
                }
                if (map.containsKey("spec")) {
                    String spec = (String) map.get("spec");
                    if (spec != null && !spec.trim().isEmpty()) {
                        statement.setString(8, spec);
                    } else {
                        statement.setNull(8, Types.VARCHAR);
                    }
                } else {
                    statement.setNull(8, Types.VARCHAR);
                }
                statement.setString(9, (String)event.get(Constants.USER));
                statement.setObject(10, OffsetDateTime.parse((String)event.get(CloudEventV1.TIME)));
                int count = statement.executeUpdate();
                if (count == 0) {
                    throw new SQLException(String.format("no record is inserted for api version %s", "hostId " + event.get(Constants.HOST) + " apiId " + map.get("apiId") + " apiVersion " + map.get("apiVersion")));
                }
                if(endpoints != null && !endpoints.isEmpty()) {
                    // insert endpoints
                    for (Map<String, Object> endpoint : endpoints) {
                        try (PreparedStatement statementInsert = conn.prepareStatement(insertEndpoint)) {
                            statementInsert.setString(1, (String)event.get(Constants.HOST));
                            statementInsert.setString(2, (String)map.get("apiId"));
                            statementInsert.setString(3, (String)map.get("apiVersion"));
                            statementInsert.setString(4, (String) endpoint.get("endpoint"));

                            if (endpoint.get("httpMethod") == null)
                                statementInsert.setNull(5, NULL);
                            else
                                statementInsert.setString(5, ((String) endpoint.get("httpMethod")).toLowerCase().trim());

                            if (endpoint.get("endpointPath") == null)
                                statementInsert.setNull(6, NULL);
                            else
                                statementInsert.setString(6, (String) endpoint.get("endpointPath"));

                            if (endpoint.get("endpointName") == null)
                                statementInsert.setNull(7, NULL);
                            else
                                statementInsert.setString(7, (String) endpoint.get("endpointName"));

                            if (endpoint.get("endpointDesc") == null)
                                statementInsert.setNull(8, NULL);
                            else
                                statementInsert.setString(8, (String) endpoint.get("endpointDesc"));

                            statementInsert.setString(9, (String)event.get(Constants.USER));
                            statementInsert.setObject(10, OffsetDateTime.parse((String)event.get(CloudEventV1.TIME)));
                            statementInsert.executeUpdate();
                        }
                        // insert scopes
                        List<String> scopes = (List<String>) endpoint.get("scopes");
                        if(scopes != null && !scopes.isEmpty()) {
                            for (String scope : scopes) {
                                String[] scopeDesc = scope.split(":");
                                try (PreparedStatement statementScope = conn.prepareStatement(insertScope)) {
                                    statementScope.setString(1, (String)event.get(Constants.HOST));
                                    statementScope.setString(2, (String)map.get("apiId"));
                                    statementScope.setString(3, (String)map.get("apiVersion"));
                                    statementScope.setString(4, (String) endpoint.get("endpoint"));
                                    statementScope.setString(5, scopeDesc[0]);
                                    if (scopeDesc.length == 1)
                                        statementScope.setNull(6, NULL);
                                    else
                                        statementScope.setString(6, scopeDesc[1]);
                                    statementScope.setString(7, (String)event.get(Constants.USER));
                                    statementScope.setObject(8, OffsetDateTime.parse((String)event.get(CloudEventV1.TIME)));
                                    statementScope.executeUpdate();
                                }
                            }
                        }
                    }
                }
                conn.commit();
                result = Success.of((String)map.get("apiId"));
                insertNotification(event, true, null);
            } catch (SQLException e) {
                logger.error("SQLException:", e);
                conn.rollback();
                insertNotification(event, false, e.getMessage());
                result = Failure.of(new Status(SQL_EXCEPTION, e.getMessage()));
            } catch (Exception e) {
                logger.error("Exception:", e);
                conn.rollback();
                insertNotification(event, false, e.getMessage());
                result = Failure.of(new Status(GENERIC_EXCEPTION, e.getMessage()));
            }
        } catch (SQLException e) {
            logger.error("SQLException:", e);
            result = Failure.of(new Status(SQL_EXCEPTION, e.getMessage()));
        }
        return result;
    }

    @Override
    public Result<String> updateServiceVersion(Map<String, Object> event, List<Map<String, Object>> endpoints) {
        final String updateApi = "UPDATE api_version_t SET api_type = ?, service_id = ?, api_version_desc = ?, spec_link = ?,  spec = ?," +
                "update_user = ?, update_ts = ? " +
                "WHERE host_id = ? AND api_id = ? AND api_version = ?";
        final String deleteEndpoint = "DELETE FROM api_endpoint_t WHERE host_id = ? AND api_id = ? AND api_version = ?";
        final String insertEndpoint = "INSERT INTO api_endpoint_t (host_id, api_id, api_version, endpoint, http_method, " +
                "endpoint_path, endpoint_name, endpoint_desc, update_user, update_ts) " +
                "VALUES (?,? ,?, ?, ?,  ?, ?, ?, ?, ?)";
        final String insertScope = "INSERT INTO api_endpoint_scope_t (host_id, api_id, api_version, endpoint, scope, scope_desc, " +
                "update_user, update_ts) " +
                "VALUES (?, ?, ?, ?, ?,  ?, ?, ?)";

        Result<String> result = null;
        Map<String, Object> map = (Map<String, Object>)event.get(PortalConstants.DATA);
        try (Connection conn = ds.getConnection()) {
            conn.setAutoCommit(false);

            try (PreparedStatement statement = conn.prepareStatement(updateApi)) {
                statement.setString(1, (String)map.get("apiType"));
                statement.setString(2, (String)map.get("serviceId"));

                if (map.containsKey("apiVersionDesc")) {
                    String apiDesc = (String) map.get("apiVersionDesc");
                    if (apiDesc != null && !apiDesc.trim().isEmpty()) {
                        statement.setString(3, apiDesc);
                    } else {
                        statement.setNull(3, Types.VARCHAR);
                    }
                } else {
                    statement.setNull(3, Types.VARCHAR);
                }
                if (map.containsKey("specLink")) {
                    String specLink = (String) map.get("specLink");
                    if (specLink != null && !specLink.trim().isEmpty()) {
                        statement.setString(4, specLink);
                    } else {
                        statement.setNull(4, Types.VARCHAR);
                    }
                } else {
                    statement.setNull(4, Types.VARCHAR);
                }
                if (map.containsKey("spec")) {
                    String spec = (String) map.get("spec");
                    if (spec != null && !spec.trim().isEmpty()) {
                        statement.setString(5, spec);
                    } else {
                        statement.setNull(5, Types.VARCHAR);
                    }
                } else {
                    statement.setNull(5, Types.VARCHAR);
                }

                statement.setString(6, (String)event.get(Constants.USER));
                statement.setObject(7, OffsetDateTime.parse((String)event.get(CloudEventV1.TIME)));
                statement.setString(8, (String)event.get(Constants.HOST));
                statement.setString(9, (String)map.get("apiId"));
                statement.setString(10, (String)map.get("apiVersion"));

                int count = statement.executeUpdate();
                if (count == 0) {
                    throw new SQLException(String.format("no record is updated for api version %s", "hostId " + (event.get(Constants.HOST) + " apiId " + map.get("apiId") + " apiVersion " + map.get("apiVersion"))));
                }
                if(endpoints != null && !endpoints.isEmpty()) {
                    // delete endpoints for the api version. the api_endpoint_scope_t will be deleted by the cascade.
                    try (PreparedStatement statementDelete = conn.prepareStatement(deleteEndpoint)) {
                        statementDelete.setString(1, (String)event.get(Constants.HOST));
                        statementDelete.setString(2, (String)map.get("apiId"));
                        statementDelete.setString(3, (String)map.get("apiVersion"));
                        statementDelete.executeUpdate();
                    }
                    // insert endpoints
                    for (Map<String, Object> endpoint : endpoints) {
                        try (PreparedStatement statementInsert = conn.prepareStatement(insertEndpoint)) {
                            statementInsert.setString(1, (String)event.get(Constants.HOST));
                            statementInsert.setString(2, (String)map.get("apiId"));
                            statementInsert.setString(3, (String)map.get("apiVersion"));
                            statementInsert.setString(4, (String) endpoint.get("endpoint"));

                            if (endpoint.get("httpMethod") == null)
                                statementInsert.setNull(5, NULL);
                            else
                                statementInsert.setString(5, ((String) endpoint.get("httpMethod")).toLowerCase().trim());

                            if (endpoint.get("endpointPath") == null)
                                statementInsert.setNull(6, NULL);
                            else
                                statementInsert.setString(6, (String) endpoint.get("endpointPath"));

                            if (endpoint.get("endpointName") == null)
                                statementInsert.setNull(7, NULL);
                            else
                                statementInsert.setString(7, (String) endpoint.get("endpointName"));

                            if (endpoint.get("endpointDesc") == null)
                                statementInsert.setNull(8, NULL);
                            else
                                statementInsert.setString(8, (String) endpoint.get("endpointDesc"));

                            statementInsert.setString(9, (String)event.get(Constants.USER));
                            statementInsert.setObject(10, OffsetDateTime.parse((String)event.get(CloudEventV1.TIME)));
                            statementInsert.executeUpdate();
                        }
                        // insert scopes
                        List<String> scopes = (List<String>) endpoint.get("scopes");
                        if (scopes != null && !scopes.isEmpty()) {
                            for (String scope : scopes) {
                                String[] scopeDesc = scope.split(":");
                                try (PreparedStatement statementScope = conn.prepareStatement(insertScope)) {
                                    statementScope.setString(1, (String)event.get(Constants.HOST));
                                    statementScope.setString(2, (String)map.get("apiId"));
                                    statementScope.setString(3, (String)map.get("apiVersion"));
                                    statementScope.setString(4, (String) endpoint.get("endpoint"));
                                    statementScope.setString(5, scopeDesc[0]);
                                    if (scopeDesc.length == 1)
                                        statementScope.setNull(6, NULL);
                                    else
                                        statementScope.setString(6, scopeDesc[1]);
                                    statementScope.setString(7, (String)event.get(Constants.USER));
                                    statementScope.setObject(8, OffsetDateTime.parse((String)event.get(CloudEventV1.TIME)));
                                    statementScope.executeUpdate();
                                }
                            }
                        }
                    }
                }
                conn.commit();
                result = Success.of((String)map.get("apiId"));
                insertNotification(event, true, null);
            } catch (SQLException e) {
                logger.error("SQLException:", e);
                conn.rollback();
                insertNotification(event, false, e.getMessage());
                result = Failure.of(new Status(SQL_EXCEPTION, e.getMessage()));
            } catch (Exception e) {
                logger.error("Exception:", e);
                conn.rollback();
                insertNotification(event, false, e.getMessage());
                result = Failure.of(new Status(GENERIC_EXCEPTION, e.getMessage()));
            }
        } catch (SQLException e) {
            logger.error("SQLException:", e);
            result = Failure.of(new Status(SQL_EXCEPTION, e.getMessage()));
        }
        return result;

    }

    @Override
    public Result<String> deleteServiceVersion(Map<String, Object> event) {
        final String deleteApplication = "DELETE from api_version_t WHERE host_id = ? AND api_id = ? AND api_version = ?";
        Result<String> result;
        Map<String, Object> map = (Map<String, Object>)event.get(PortalConstants.DATA);
        try (Connection conn = ds.getConnection()) {
            conn.setAutoCommit(false);
            try (PreparedStatement statement = conn.prepareStatement(deleteApplication)) {
                statement.setString(1, (String)event.get(Constants.HOST));
                statement.setString(2, (String)map.get("apiId"));
                statement.setString(3, (String)map.get("apiVersion"));
                int count = statement.executeUpdate();
                if (count == 0) {
                    // no record is deleted, write an error notification.
                    throw new SQLException(String.format("no record is deleted for api version %s", "hostId " + event.get(Constants.HOST) + " apiId " + map.get("apiId") + " apiVersion " + map.get("apiVersion")));
                }
                conn.commit();
                result = Success.of((String)event.get(Constants.USER));
                insertNotification(event, true, null);
            } catch (SQLException e) {
                logger.error("SQLException:", e);
                conn.rollback();
                insertNotification(event, false, e.getMessage());
                result = Failure.of(new Status(SQL_EXCEPTION, e.getMessage()));
            } catch (Exception e) {
                logger.error("Exception:", e);
                conn.rollback();
                insertNotification(event, false, e.getMessage());
                result = Failure.of(new Status(GENERIC_EXCEPTION, e.getMessage()));
            }

        } catch (SQLException e) {
            logger.error("SQLException:", e);
            result = Failure.of(new Status(SQL_EXCEPTION, e.getMessage()));
        }
        return result;

    }

    @Override
    public Result<String> queryServiceVersion(String hostId, String apiId) {
        Result<String> result = null;
        String sql = "SELECT host_id, api_id, api_version, api_type, service_id,\n" +
                "api_version_desc, spec_link, spec\n" +
                "FROM api_version_t\n" +
                "WHERE host_id = ? AND api_id = ?\n" +
                "ORDER BY api_version";

        List<Map<String, Object>> serviceVersions = new ArrayList<>();

        try (Connection connection = ds.getConnection();
             PreparedStatement preparedStatement = connection.prepareStatement(sql)) {
            preparedStatement.setObject(1, UUID.fromString(hostId));
            preparedStatement.setString(2, apiId);
            try (ResultSet resultSet = preparedStatement.executeQuery()) {
                while (resultSet.next()) {
                    Map<String, Object> map = new HashMap<>();
                    map.put("hostId", resultSet.getObject("host_id", UUID.class));
                    map.put("apiId", resultSet.getString("api_id"));
                    map.put("apiVersion", resultSet.getString("api_version"));
                    map.put("apiType", resultSet.getString("api_type"));
                    map.put("serviceId", resultSet.getString("service_id"));
                    map.put("apiVersionDesc", resultSet.getString("api_version_desc"));
                    map.put("specLink", resultSet.getString("spec_link"));
                    map.put("spec", resultSet.getString("spec"));
                    serviceVersions.add(map);
                }
            }
            result = Success.of(JsonMapper.toJson(serviceVersions));
        } catch (SQLException e) {
            logger.error("SQLException:", e);
            result = Failure.of(new Status(SQL_EXCEPTION, e.getMessage()));
        } catch (Exception e) {
            logger.error("Exception:", e);
            result = Failure.of(new Status(GENERIC_EXCEPTION, e.getMessage()));
        }
        return result;
    }

    private void addCondition(StringBuilder whereClause, List<Object> parameters, String columnName, String value) {
        if (value != null && !value.equals("*")) {
            if (!whereClause.isEmpty()) {
                whereClause.append(" AND ");
            }
            whereClause.append(columnName);
            whereClause.append(" LIKE '%' || ? || '%'");
            parameters.add(value);

        }
    }

    private void addCondition(StringBuilder whereClause, List<Object> parameters, String columnName, Object value) {
        if (value != null) {
            if (!whereClause.isEmpty()) {
                whereClause.append(" AND ");
            }
            whereClause.append(columnName).append(" = ?");
            parameters.add(value);
        }
    }

    @Override
    public Result<String> updateServiceSpec(Map<String, Object> event, List<Map<String, Object>> endpoints) {
        final String updateApiVersion = "UPDATE api_version_t SET spec = ?, " +
                "update_user = ?, update_ts = ? " +
                "WHERE host_id = ? AND api_id = ? AND api_version = ?";
        final String deleteEndpoint = "DELETE FROM api_endpoint_t WHERE host_id = ? AND api_id = ? AND api_version = ?";
        final String insertEndpoint = "INSERT INTO api_endpoint_t (host_id, api_id, api_version, endpoint, http_method, " +
                "endpoint_path, endpoint_name, endpoint_desc, update_user, update_ts) " +
                "VALUES (?,? ,?, ?, ?,  ?, ?, ?, ?, ?)";
        final String insertScope = "INSERT INTO api_endpoint_scope_t (host_id, api_id, api_version, endpoint, scope, scope_desc, " +
                "update_user, update_ts) " +
                "VALUES (?, ?, ?, ?, ?,  ?, ?, ?)";


        Result<String> result = null;
        Map<String, Object> map = (Map<String, Object>)event.get(PortalConstants.DATA);
        try (Connection conn = ds.getConnection()) {
            conn.setAutoCommit(false);
            try {
                // update spec
                try (PreparedStatement statement = conn.prepareStatement(updateApiVersion)) {
                    statement.setString(1, (String)map.get("spec"));
                    statement.setString(2, (String)event.get(Constants.USER));
                    statement.setObject(3, OffsetDateTime.parse((String)event.get(CloudEventV1.TIME)));
                    statement.setString(4, (String)event.get(Constants.HOST));
                    statement.setString(5, (String)map.get("apiId"));
                    statement.setString(6, (String)map.get("apiVersion"));

                    int count = statement.executeUpdate();
                    if (count == 0) {
                        // no record is updated, write an error notification.
                        throw new SQLException(String.format("no record is updated for api version " + " hostId " + event.get(Constants.HOST) + " apiId " + map.get("apiId") + " apiVersion " + map.get("apiVersion")));
                    }
                }
                // delete endpoints for the api version. the api_endpoint_scope_t will be deleted by the cascade.
                try (PreparedStatement statement = conn.prepareStatement(deleteEndpoint)) {
                    statement.setString(1, (String)event.get(Constants.HOST));
                    statement.setString(2, (String)map.get("apiId"));
                    statement.setString(3, (String)map.get("apiVersion"));
                    statement.executeUpdate();
                }
                // insert endpoints
                for (Map<String, Object> endpoint : endpoints) {
                    try (PreparedStatement statement = conn.prepareStatement(insertEndpoint)) {
                        statement.setString(1, (String)event.get(Constants.HOST));
                        statement.setString(2, (String)map.get("apiId"));
                        statement.setString(3, (String)map.get("apiVersion"));
                        statement.setString(4, (String)endpoint.get("endpoint"));

                        if (endpoint.get("httpMethod") == null)
                            statement.setNull(5, NULL);
                        else
                            statement.setString(5, ((String) endpoint.get("httpMethod")).toLowerCase().trim());

                        if (endpoint.get("endpointPath") == null)
                            statement.setNull(6, NULL);
                        else
                            statement.setString(6, (String) endpoint.get("endpointPath"));

                        if (endpoint.get("endpointName") == null)
                            statement.setNull(7, NULL);
                        else
                            statement.setString(7, (String) endpoint.get("endpointName"));

                        if (endpoint.get("endpointDesc") == null)
                            statement.setNull(8, NULL);
                        else
                            statement.setString(8, (String) endpoint.get("endpointDesc"));

                        statement.setString(9, (String)event.get(Constants.USER));
                        statement.setObject(10, OffsetDateTime.parse((String)event.get(CloudEventV1.TIME)));
                        statement.executeUpdate();
                    }
                    // insert scopes
                    List<String> scopes = (List<String>) endpoint.get("scopes");
                    if(scopes != null && !scopes.isEmpty()) {
                        for (String scope : scopes) {
                            String[] scopeDesc = scope.split(":");
                            try (PreparedStatement statement = conn.prepareStatement(insertScope)) {
                                statement.setString(1, (String)event.get(Constants.HOST));
                                statement.setString(2, (String)map.get("apiId"));
                                statement.setString(3, (String)map.get("apiVersion"));
                                statement.setString(4, (String) endpoint.get("endpoint"));
                                statement.setString(5, scopeDesc[0]);
                                if (scopeDesc.length == 1)
                                    statement.setNull(6, NULL);
                                else
                                    statement.setString(6, scopeDesc[1]);
                                statement.setString(7, (String)event.get(Constants.USER));
                                statement.setObject(8, OffsetDateTime.parse((String)event.get(CloudEventV1.TIME)));
                                statement.executeUpdate();
                            }
                        }
                    }
                }
                conn.commit();
                result = Success.of((String)map.get("apiId"));
                insertNotification(event, true, null);
            } catch (SQLException e) {
                logger.error("SQLException:", e);
                conn.rollback();
                insertNotification(event, false, e.getMessage());
                result = Failure.of(new Status(SQL_EXCEPTION, e.getMessage()));
            } catch (Exception e) {
                logger.error("Exception:", e);
                conn.rollback();
                insertNotification(event, false, e.getMessage());
                result = Failure.of(new Status(GENERIC_EXCEPTION, e.getMessage()));
            }
        } catch (SQLException e) {
            logger.error("SQLException:", e);
            result = Failure.of(new Status(SQL_EXCEPTION, e.getMessage()));
        }
        return result;

    }

    @Override
    public Result<String> queryServiceEndpoint(int offset, int limit, String hostId, String apiId, String apiVersion, String endpoint, String method, String path, String desc) {
        Result<String> result = null;
        StringBuilder sqlBuilder = new StringBuilder();
        sqlBuilder.append("SELECT COUNT(*) OVER () AS total,\n" +
                "host_id, api_id, api_version, endpoint, http_method,\n" +
                "endpoint_path, endpoint_desc\n" +
                "FROM api_endpoint_t\n" +
                "WHERE host_id = ? AND api_id = ? AND api_version = ?\n");

        List<Object> parameters = new ArrayList<>();
        parameters.add(UUID.fromString(hostId));
        parameters.add(apiId);
        parameters.add(apiVersion);

        StringBuilder whereClause = new StringBuilder();
        addCondition(whereClause, parameters, "endpoint", endpoint);
        addCondition(whereClause, parameters, "http_method", method);
        addCondition(whereClause, parameters, "endpoint_path", path);
        addCondition(whereClause, parameters, "endpoint_desc", desc);

        if(!whereClause.isEmpty()) {
            sqlBuilder.append(" AND ").append(whereClause);
        }


        sqlBuilder.append(" ORDER BY endpoint\n" +
                "LIMIT ? OFFSET ?");

        parameters.add(limit);
        parameters.add(offset);

        String sql = sqlBuilder.toString();
        int total = 0;
        List<Map<String, Object>> endpoints = new ArrayList<>();


        try (Connection connection = ds.getConnection();
             PreparedStatement preparedStatement = connection.prepareStatement(sql)) {

            for (int i = 0; i < parameters.size(); i++) {
                preparedStatement.setObject(i + 1, parameters.get(i));
            }

            boolean isFirstRow = true;
            try (ResultSet resultSet = preparedStatement.executeQuery()) {
                while (resultSet.next()) {
                    Map<String, Object> map = new HashMap<>();

                    if (isFirstRow) {
                        total = resultSet.getInt("total");
                        isFirstRow = false;
                    }


                    map.put("hostId", resultSet.getObject("host_id", UUID.class));
                    map.put("apiId", resultSet.getString("api_id"));
                    map.put("apiVersion", resultSet.getString("api_version"));
                    map.put("endpoint", resultSet.getString("endpoint"));
                    map.put("httpMethod", resultSet.getString("http_method"));
                    map.put("endpointPath", resultSet.getString("endpoint_path"));
                    map.put("endpointDesc", resultSet.getString("endpoint_desc"));
                    endpoints.add(map);
                }
            }

            Map<String, Object> resultMap = new HashMap<>();
            resultMap.put("total", total);
            resultMap.put("endpoints", endpoints);
            result = Success.of(JsonMapper.toJson(resultMap));
        } catch (SQLException e) {
            logger.error("SQLException:", e);
            result = Failure.of(new Status(SQL_EXCEPTION, e.getMessage()));
        } catch (Exception e) {
            logger.error("Exception:", e);
            result = Failure.of(new Status(GENERIC_EXCEPTION, e.getMessage()));
        }
        return result;
    }

    @Override
    public Result<String> queryEndpointScope(String hostId, String apiId, String apiVersion, String endpoint) {
        Result<String> result = null;
        String sql = "SELECT host_id, api_id, api_version, endpoint, scope, scope_desc \n" +
                "FROM api_endpoint_scope_t\n" +
                "WHERE host_id = ?\n" +
                "AND api_id = ?\n" +
                "AND api_version = ?\n" +
                "AND endpoint = ?\n" +
                "ORDER BY scope";

        List<Map<String, Object>> scopes = new ArrayList<>();

        try (Connection connection = ds.getConnection();
             PreparedStatement preparedStatement = connection.prepareStatement(sql)) {
            preparedStatement.setObject(1, UUID.fromString(hostId));
            preparedStatement.setString(2, apiId);
            preparedStatement.setString(3, apiVersion);
            preparedStatement.setString(4, endpoint);
            try (ResultSet resultSet = preparedStatement.executeQuery()) {
                while (resultSet.next()) {
                    Map<String, Object> map = new HashMap<>();
                    map.put("hostId", resultSet.getObject("host_id", UUID.class));
                    map.put("apiId", resultSet.getString("api_id"));
                    map.put("apiVersion", resultSet.getString("api_version"));
                    map.put("endpoint", resultSet.getString("endpoint"));
                    map.put("scope", resultSet.getString("scope"));
                    map.put("scopeDesc", resultSet.getString("scope_desc"));
                    scopes.add(map);
                }
            }
            result = Success.of(JsonMapper.toJson(scopes));
        } catch (SQLException e) {
            logger.error("SQLException:", e);
            result = Failure.of(new Status(SQL_EXCEPTION, e.getMessage()));
        } catch (Exception e) {
            logger.error("Exception:", e);
            result = Failure.of(new Status(GENERIC_EXCEPTION, e.getMessage()));
        }
        return result;

    }

    @Override
    public Result<String> queryEndpointRule(String hostId, String apiId, String apiVersion, String endpoint) {
        Result<String> result = null;
        String sql = "SELECT a.host_id, a.api_id, a.api_version, a.endpoint, r.rule_type, a.rule_id\n" +
                "FROM api_endpoint_rule_t a, rule_t r\n" +
                "WHERE a.rule_id = r.rule_id\n" +
                "AND a.host_id = ?\n" +
                "AND a.api_id = ?\n" +
                "AND a.api_version = ?\n" +
                "AND a.endpoint = ?\n" +
                "ORDER BY r.rule_type";

        List<Map<String, Object>> rules = new ArrayList<>();

        try (Connection connection = ds.getConnection();
             PreparedStatement preparedStatement = connection.prepareStatement(sql)) {
            preparedStatement.setObject(1, UUID.fromString(hostId));
            preparedStatement.setString(2, apiId);
            preparedStatement.setString(3, apiVersion);
            preparedStatement.setString(4, endpoint);
            try (ResultSet resultSet = preparedStatement.executeQuery()) {
                while (resultSet.next()) {
                    Map<String, Object> map = new HashMap<>();
                    map.put("hostId", resultSet.getObject("host_id", UUID.class));
                    map.put("apiId", resultSet.getString("api_id"));
                    map.put("apiVersion", resultSet.getString("api_version"));
                    map.put("endpoint", resultSet.getString("endpoint"));
                    map.put("ruleType", resultSet.getString("rule_type"));
                    map.put("ruleId", resultSet.getString("rule_id"));
                    rules.add(map);
                }
            }
            result = Success.of(JsonMapper.toJson(rules));
        } catch (SQLException e) {
            logger.error("SQLException:", e);
            result = Failure.of(new Status(SQL_EXCEPTION, e.getMessage()));
        } catch (Exception e) {
            logger.error("Exception:", e);
            result = Failure.of(new Status(GENERIC_EXCEPTION, e.getMessage()));
        }
        return result;

    }

    @Override
    public Result<String> createEndpointRule(Map<String, Object> event) {
        final String insertUser = "INSERT INTO api_endpoint_rule_t (host_id, api_id, api_version, endpoint, rule_id, " +
                "update_user, update_ts) " +
                "VALUES (?, ?, ?, ?, ?,   ?, ?)";
        Result<String> result = null;
        Map<String, Object> map = (Map<String, Object>)event.get(PortalConstants.DATA);
        try (Connection conn = ds.getConnection()) {
            conn.setAutoCommit(false);
            // no duplicate record, insert the user into database and write a success notification.
            try (PreparedStatement statement = conn.prepareStatement(insertUser)) {
                statement.setString(1, (String)event.get(Constants.HOST));
                statement.setString(2, (String)map.get("apiId"));
                statement.setString(3, (String)map.get("apiVersion"));
                statement.setString(4, (String)map.get("endpoint"));
                statement.setString(5, (String)map.get("ruleId"));
                statement.setString(6, (String)event.get(Constants.USER));
                statement.setObject(7, OffsetDateTime.parse((String)event.get(CloudEventV1.TIME)));
                int count = statement.executeUpdate();
                if (count == 0) {
                    throw new SQLException(String.format("no record is inserted for api version " + "hostId " + event.get(Constants.HOST) + " apiId " + map.get("apiId") + " apiVersion " + map.get("apiVersion")));
                }
                conn.commit();
                result = Success.of((String)map.get("apiId"));
                insertNotification(event, true, null);
            } catch (SQLException e) {
                logger.error("SQLException:", e);
                conn.rollback();
                insertNotification(event, false, e.getMessage());
                result = Failure.of(new Status(SQL_EXCEPTION, e.getMessage()));
            } catch (Exception e) {
                logger.error("Exception:", e);
                conn.rollback();
                insertNotification(event, false, e.getMessage());
                result = Failure.of(new Status(GENERIC_EXCEPTION, e.getMessage()));
            }
        } catch (SQLException e) {
            logger.error("SQLException:", e);
            result = Failure.of(new Status(SQL_EXCEPTION, e.getMessage()));
        }
        return result;
    }

    @Override
    public Result<String> deleteEndpointRule(Map<String, Object> event) {
        final String deleteApplication = "DELETE from api_endpoint_rule_t WHERE host_id = ? AND api_id = ? AND api_version = ? AND endpoint = ? AND rule_id = ?";
        Result<String> result;
        Map<String, Object> map = (Map<String, Object>)event.get(PortalConstants.DATA);
        try (Connection conn = ds.getConnection()) {
            conn.setAutoCommit(false);
            try (PreparedStatement statement = conn.prepareStatement(deleteApplication)) {
                statement.setString(1, (String)event.get(Constants.HOST));
                statement.setString(2, (String)map.get("apiId"));
                statement.setString(3, (String)map.get("apiVersion"));
                statement.setString(4, (String)map.get("endpoint"));
                statement.setString(5, (String)map.get("ruleId"));

                int count = statement.executeUpdate();
                if (count == 0) {
                    // no record is deleted, write an error notification.
                    throw new SQLException(String.format("no record is deleted for endpoint rule " + "hostId " + event.get(Constants.HOST) + " apiId " + map.get("apiId") + " apiVersion " + map.get("apiVersion")));
                }
                conn.commit();
                result = Success.of((String)event.get(Constants.USER));
                insertNotification(event, true, null);
            } catch (SQLException e) {
                logger.error("SQLException:", e);
                conn.rollback();
                insertNotification(event, false, e.getMessage());
                result = Failure.of(new Status(SQL_EXCEPTION, e.getMessage()));
            } catch (Exception e) {
                logger.error("Exception:", e);
                conn.rollback();
                insertNotification(event, false, e.getMessage());
                result = Failure.of(new Status(GENERIC_EXCEPTION, e.getMessage()));
            }

        } catch (SQLException e) {
            logger.error("SQLException:", e);
            result = Failure.of(new Status(SQL_EXCEPTION, e.getMessage()));
        }
        return result;
    }

    @Override
    public Result<String> queryServiceRule(String hostId, String apiId, String apiVersion) {
        Result<String> result = null;
        String sql = "SELECT a.host_id, a.api_id, a.api_version, a.endpoint, r.rule_type, a.rule_id\n" +
                "FROM api_endpoint_rule_t a, rule_t r\n" +
                "WHERE a.rule_id = r.rule_id\n" +
                "AND a.host_id =?\n" +
                "AND a.api_id = ?\n" +
                "AND a.api_version = ?\n" +
                "ORDER BY r.rule_type";
        String sqlRuleBody = "SELECT rule_id, rule_body FROM rule_t WHERE rule_id = ?";
        List<Map<String, Object>> rules = new ArrayList<>();
        Map<String, Object> ruleBodies = new HashMap<>();

        try (Connection connection = ds.getConnection();
             PreparedStatement preparedStatement = connection.prepareStatement(sql)) {
            preparedStatement.setObject(1, UUID.fromString(hostId));
            preparedStatement.setString(2, apiId);
            preparedStatement.setString(3, apiVersion);
            try (ResultSet resultSet = preparedStatement.executeQuery()) {
                while (resultSet.next()) {
                    Map<String, Object> map = new HashMap<>();
                    map.put("hostId", resultSet.getObject("host_id", UUID.class));
                    map.put("apiId", resultSet.getString("api_id"));
                    map.put("apiVersion", resultSet.getString("api_version"));
                    map.put("endpoint", resultSet.getString("endpoint"));
                    map.put("ruleType", resultSet.getString("rule_type"));
                    String ruleId = resultSet.getString("rule_id");
                    map.put("ruleId", ruleId);
                    rules.add(map);

                    // Get rule body if not already cached
                    if (!ruleBodies.containsKey(ruleId)) {
                        String ruleBody = fetchRuleBody(connection, sqlRuleBody, ruleId);
                        // convert the json string to map.
                        Map<String, Object> bodyMap = JsonMapper.string2Map(ruleBody);
                        ruleBodies.put(ruleId, bodyMap);
                    }
                }
            }
            Map<String, Object> combinedResult = new HashMap<>();
            combinedResult.put("rules", rules);
            combinedResult.put("ruleBodies", ruleBodies);
            result = Success.of(JsonMapper.toJson(combinedResult));
        } catch (SQLException e) {
            logger.error("SQLException:", e);
            result = Failure.of(new Status(SQL_EXCEPTION, e.getMessage()));
        } catch (Exception e) {
            logger.error("Exception:", e);
            result = Failure.of(new Status(GENERIC_EXCEPTION, e.getMessage()));
        }
        return result;
    }

    private String fetchRuleBody(Connection connection, String sqlRuleBody, String ruleId) throws SQLException {
        try (PreparedStatement preparedStatement = connection.prepareStatement(sqlRuleBody)) {
            preparedStatement.setString(1, ruleId);
            try (ResultSet resultSet = preparedStatement.executeQuery()) {
                if (resultSet.next()) {
                    return resultSet.getString("rule_body");
                }
            }
        }
        return null; // or throw an exception if you consider this an error state.
    }

    @Override
    public Result<String> queryServicePermission(String hostId, String apiId, String apiVersion) {
        Result<String> result = null;
        String sql = "SELECT\n" +
                "    CASE\n" +
                "        WHEN COUNT(ae.endpoint) > 0 THEN\n" +
                "            JSON_AGG(\n" +
                "                JSON_BUILD_OBJECT(\n" +
                "                    'endpoint', ae.endpoint,\n" +
                "                    'roles', COALESCE((\n" +
                "                        SELECT JSON_ARRAYAGG(\n" +
                "                            JSON_BUILD_OBJECT(\n" +
                "                                'roleId', rp.role_id\n" +
                "                            )\n" +
                "                        )\n" +
                "                        FROM role_permission_t rp\n" +
                "                        WHERE rp.host_id = ?\n" +
                "                        AND rp.api_id = ?\n" +
                "                        AND rp.api_version = ?\n" +
                "                        AND rp.endpoint = ae.endpoint\n" +
                "                    ), '[]'),\n" +
                "                    'positions', COALESCE((\n" +
                "                        SELECT JSON_ARRAYAGG(\n" +
                "                             JSON_BUILD_OBJECT(\n" +
                "                                'positionId', pp.position_id\n" +
                "                             )\n" +
                "                         )\n" +
                "                        FROM position_permission_t pp\n" +
                "                        WHERE pp.host_id = ?\n" +
                "                        AND pp.api_id = ?\n" +
                "                        AND pp.api_version = ?\n" +
                "                        AND pp.endpoint = ae.endpoint\n" +
                "                    ), '[]'),\n" +
                "                    'groups', COALESCE((\n" +
                "                        SELECT JSON_ARRAYAGG(\n" +
                "                            JSON_BUILD_OBJECT(\n" +
                "                               'groupId', gp.group_id\n" +
                "                            )\n" +
                "                        )\n" +
                "                        FROM group_permission_t gp\n" +
                "                        WHERE gp.host_id = ?\n" +
                "                        AND gp.api_id = ?\n" +
                "                        AND gp.api_version = ?\n" +
                "                        AND gp.endpoint = ae.endpoint\n" +
                "                    ), '[]'),\n" +
                "                    'attributes', COALESCE((\n" +
                "                        SELECT JSON_ARRAYAGG(\n" +
                "                            JSON_BUILD_OBJECT(\n" +
                "                                'attribute_id', ap.attribute_id, \n" +
                "                                'attribute_value', ap.attribute_value, \n" +
                "                                'attribute_type', a.attribute_type\n" +
                "                            )\n" +
                "                        )\n" +
                "                        FROM attribute_permission_t ap, attribute_t a\n" +
                "                        WHERE ap.attribute_id = a.attribute_id\n" +
                "                        AND ap.host_id = ?\n" +
                "                        AND ap.api_id = ?\n" +
                "                        AND ap.api_version = ?\n" +
                "                        AND ap.endpoint = ae.endpoint\n" +
                "                    ), '[]'),\n" +
                "                    'users', COALESCE((\n" +
                "                        SELECT JSON_ARRAYAGG(\n" +
                "                            JSON_BUILD_OBJECT(\n" +
                "                                 'userId', user_id,\n" +
                "                                 'startTs', start_ts,\n" +
                "                                 'endTs', end_ts\n" +
                "                            )\n" +
                "                        )\n" +
                "                        FROM user_permission_t up\n" +
                "                        WHERE up.host_id = ?\n" +
                "                        AND up.api_id = ?\n" +
                "                        AND up.api_version = ?\n" +
                "                        AND up.endpoint = ae.endpoint\n" +
                "                    ), '[]')\n" +
                "                )\n" +
                "            )\n" +
                "        ELSE NULL\n" +
                "    END AS permissions\n" +
                "FROM\n" +
                "    api_endpoint_t ae\n" +
                "WHERE\n" +
                "    ae.host_id = ?\n" +
                "    AND ae.api_id = ?\n" +
                "    AND ae.api_version = ?;\n";

        try (Connection connection = ds.getConnection();
             PreparedStatement preparedStatement = connection.prepareStatement(sql)) {
            preparedStatement.setObject(1, UUID.fromString(hostId));
            preparedStatement.setString(2, apiId);
            preparedStatement.setString(3, apiVersion);
            preparedStatement.setObject(4, UUID.fromString(hostId));
            preparedStatement.setString(5, apiId);
            preparedStatement.setString(6, apiVersion);
            preparedStatement.setObject(7, UUID.fromString(hostId));
            preparedStatement.setString(8, apiId);
            preparedStatement.setString(9, apiVersion);
            preparedStatement.setObject(10, UUID.fromString(hostId));
            preparedStatement.setString(11, apiId);
            preparedStatement.setString(12, apiVersion);
            preparedStatement.setObject(13, UUID.fromString(hostId));
            preparedStatement.setString(14, apiId);
            preparedStatement.setString(15, apiVersion);
            preparedStatement.setObject(16, UUID.fromString(hostId));
            preparedStatement.setString(17, apiId);
            preparedStatement.setString(18, apiVersion);

            try (ResultSet resultSet = preparedStatement.executeQuery()) {
                if (resultSet.next()) {
                    String permissionsJson = resultSet.getString("permissions");
                    result = Success.of(permissionsJson);
                }
            }
        } catch (SQLException e) {
            logger.error("SQLException:", e);
            result = Failure.of(new Status(SQL_EXCEPTION, e.getMessage()));
        } catch (Exception e) {
            logger.error("Exception:", e);
            result = Failure.of(new Status(GENERIC_EXCEPTION, e.getMessage()));
        }
        return result;
    }

    @Override
    public Result<List<String>> queryServiceFilter(String hostId, String apiId, String apiVersion) {
        Result<List<String>> result = null;
        String sql = "SELECT\n" +
                "    JSON_BUILD_OBJECT(\n" +
                "        'role_row', JSON_AGG(\n" +
                "            JSON_BUILD_OBJECT(\n" +
                "                'endpoint', endpoint,\n" +
                "                'roleId', role_id,\n" +
                "                'colName', col_name,\n" +
                "                'operator', operator,\n" +
                "                'colValue', col_value\n" +
                "            )\n" +
                "        )\n" +
                "    ) AS result\n" +
                "FROM\n" +
                "    role_row_filter_t\n" +
                "WHERE\n" +
                "    host_id = ?\n" +
                "    AND api_id = ?\n" +
                "    AND api_version = ?\n" +
                "GROUP BY ()\n" +
                "HAVING COUNT(*) > 0 \n" +
                "UNION ALL\n" +
                "SELECT\n" +
                "    JSON_BUILD_OBJECT(\n" +
                "        'role_col', JSON_AGG(\n" +
                "            JSON_BUILD_OBJECT(\n" +
                "                'endpoint', endpoint,\n" +
                "                'roleId', role_id,\n" +
                "                'columns', columns\n" +
                "            )\n" +
                "        )\n" +
                "    ) AS result\n" +
                "FROM\n" +
                "    role_col_filter_t\n" +
                "WHERE\n" +
                "    host_id = ?\n" +
                "    AND api_id = ?\n" +
                "    AND api_version = ?\n" +
                "GROUP BY ()\n" +
                "HAVING COUNT(*) > 0\n" +
                "UNION ALL\n" +
                "SELECT\n" +
                "    JSON_BUILD_OBJECT(\n" +
                "        'group_row', JSON_AGG(\n" +
                "            JSON_BUILD_OBJECT(\n" +
                "                'endpoint', endpoint,\n" +
                "                'groupId', group_id,\n" +
                "                'colName', col_name,\n" +
                "                'operator', operator,\n" +
                "                'colValue', col_value\n" +
                "            )\n" +
                "        )\n" +
                "    ) AS result\n" +
                "FROM\n" +
                "    group_row_filter_t\n" +
                "WHERE\n" +
                "    host_id = ?\n" +
                "    AND api_id = ?\n" +
                "    AND api_version = ?\n" +
                "GROUP BY ()\n" +
                "HAVING COUNT(*) > 0 \n" +
                "UNION ALL\n" +
                "SELECT\n" +
                "    JSON_BUILD_OBJECT(\n" +
                "        'group_col', JSON_AGG(\n" +
                "            JSON_BUILD_OBJECT(\n" +
                "                'endpoint', endpoint,\n" +
                "                'groupId', group_id,\n" +
                "                'columns', columns\n" +
                "            )\n" +
                "        )\n" +
                "    ) AS result\n" +
                "FROM\n" +
                "    group_col_filter_t\n" +
                "WHERE\n" +
                "    host_id = ?\n" +
                "    AND api_id = ?\n" +
                "    AND api_version = ?\n" +
                "GROUP BY ()\n" +
                "HAVING COUNT(*) > 0\n" +
                "UNION ALL\n" +
                "SELECT\n" +
                "    JSON_BUILD_OBJECT(\n" +
                "        'position_row', JSON_AGG(\n" +
                "            JSON_BUILD_OBJECT(\n" +
                "                'endpoint', endpoint,\n" +
                "                'positionId', position_id,\n" +
                "                'colName', col_name,\n" +
                "                'operator', operator,\n" +
                "                'colValue', col_value\n" +
                "            )\n" +
                "        )\n" +
                "    ) AS result\n" +
                "FROM\n" +
                "    position_row_filter_t\n" +
                "WHERE\n" +
                "    host_id = ?\n" +
                "    AND api_id = ?\n" +
                "    AND api_version = ?\n" +
                "GROUP BY ()\n" +
                "HAVING COUNT(*) > 0 \n" +
                "UNION ALL\n" +
                "SELECT\n" +
                "    JSON_BUILD_OBJECT(\n" +
                "        'position_col', JSON_AGG(\n" +
                "            JSON_BUILD_OBJECT(\n" +
                "                'endpoint', endpoint,\n" +
                "                'positionId', position_id,\n" +
                "                'columns', columns\n" +
                "            )\n" +
                "        )\n" +
                "    ) AS result\n" +
                "FROM\n" +
                "    position_col_filter_t\n" +
                "WHERE\n" +
                "    host_id = ?\n" +
                "    AND api_id = ?\n" +
                "    AND api_version = ?\n" +
                "GROUP BY ()\n" +
                "HAVING COUNT(*) > 0\n" +
                "UNION ALL\n" +
                "SELECT\n" +
                "    JSON_BUILD_OBJECT(\n" +
                "        'attribute_row', JSON_AGG(\n" +
                "            JSON_BUILD_OBJECT(\n" +
                "                'endpoint', endpoint,\n" +
                "                'attributeId', attribute_id,\n" +
                "                'attributeValue', attribute_value,\n" +
                "                'colName', col_name,\n" +
                "                'operator', operator,\n" +
                "                'colValue', col_value\n" +
                "            )\n" +
                "        )\n" +
                "    ) AS result\n" +
                "FROM\n" +
                "    attribute_row_filter_t\n" +
                "WHERE\n" +
                "    host_id = ?\n" +
                "    AND api_id = ?\n" +
                "    AND api_version = ?\n" +
                "GROUP BY ()\n" +
                "HAVING COUNT(*) > 0 \n" +
                "UNION ALL\n" +
                "SELECT\n" +
                "    JSON_BUILD_OBJECT(\n" +
                "        'attribute_col', JSON_AGG(\n" +
                "            JSON_BUILD_OBJECT(\n" +
                "                'endpoint', endpoint,\n" +
                "                'attributeId', attribute_id,\n" +
                "                'attributeValue', attribute_value,\n" +
                "                'columns', columns\n" +
                "            )\n" +
                "        )\n" +
                "    ) AS result\n" +
                "FROM\n" +
                "    attribute_col_filter_t\n" +
                "WHERE\n" +
                "    host_id = ?\n" +
                "    AND api_id = ?\n" +
                "    AND api_version = ?\n" +
                "GROUP BY ()\n" +
                "HAVING COUNT(*) > 0\n" +
                "UNION ALL\n" +
                "SELECT\n" +
                "    JSON_BUILD_OBJECT(\n" +
                "        'user_row', JSON_AGG(\n" +
                "            JSON_BUILD_OBJECT(\n" +
                "                'endpoint', endpoint,\n" +
                "                'userId', user_id,\n" +
                "                'startTs', start_ts,\n" +
                "                'endTs', end_ts,\n" +
                "                'colName', col_name,\n" +
                "                'operator', operator,\n" +
                "                'colValue', col_value\n" +
                "            )\n" +
                "        )\n" +
                "    ) AS result\n" +
                "FROM\n" +
                "    user_row_filter_t\n" +
                "WHERE\n" +
                "    host_id = ?\n" +
                "    AND api_id = ?\n" +
                "    AND api_version = ?\n" +
                "GROUP BY ()\n" +
                "HAVING COUNT(*) > 0 \n" +
                "UNION ALL\n" +
                "SELECT\n" +
                "    JSON_BUILD_OBJECT(\n" +
                "        'user_col', JSON_AGG(\n" +
                "            JSON_BUILD_OBJECT(\n" +
                "                'endpoint', endpoint,\n" +
                "                'userId', user_id,\n" +
                "                'startTs', start_ts,\n" +
                "                'endTs', end_ts,\n" +
                "                'columns', columns\n" +
                "            )\n" +
                "        )\n" +
                "    ) AS result\n" +
                "FROM\n" +
                "    user_col_filter_t\n" +
                "WHERE\n" +
                "    host_id = ?\n" +
                "    AND api_id = ?\n" +
                "    AND api_version = ?\n" +
                "GROUP BY ()\n" +
                "HAVING COUNT(*) > 0\n";

        try (Connection connection = ds.getConnection();
             PreparedStatement preparedStatement = connection.prepareStatement(sql)) {
            preparedStatement.setObject(1, UUID.fromString(hostId));
            preparedStatement.setString(2, apiId);
            preparedStatement.setString(3, apiVersion);
            preparedStatement.setObject(4, UUID.fromString(hostId));
            preparedStatement.setString(5, apiId);
            preparedStatement.setString(6, apiVersion);
            preparedStatement.setObject(7, UUID.fromString(hostId));
            preparedStatement.setString(8, apiId);
            preparedStatement.setString(9, apiVersion);
            preparedStatement.setObject(10, UUID.fromString(hostId));
            preparedStatement.setString(11, apiId);
            preparedStatement.setString(12, apiVersion);
            preparedStatement.setObject(13, UUID.fromString(hostId));
            preparedStatement.setString(14, apiId);
            preparedStatement.setString(15, apiVersion);
            preparedStatement.setObject(16, UUID.fromString(hostId));
            preparedStatement.setString(17, apiId);
            preparedStatement.setString(18, apiVersion);
            preparedStatement.setObject(19, UUID.fromString(hostId));
            preparedStatement.setString(20, apiId);
            preparedStatement.setString(21, apiVersion);
            preparedStatement.setObject(22, UUID.fromString(hostId));
            preparedStatement.setString(23, apiId);
            preparedStatement.setString(24, apiVersion);
            preparedStatement.setObject(25, UUID.fromString(hostId));
            preparedStatement.setString(26, apiId);
            preparedStatement.setString(27, apiVersion);
            preparedStatement.setObject(28, UUID.fromString(hostId));
            preparedStatement.setString(29, apiId);
            preparedStatement.setString(30, apiVersion);
            List<String> list = new ArrayList<>();
            try (ResultSet resultSet = preparedStatement.executeQuery()) {
                while (resultSet.next()) {
                    String json = resultSet.getString("result");
                    list.add(json);
                }
            }
            result = Success.of(list);
        } catch (SQLException e) {
            logger.error("SQLException:", e);
            result = Failure.of(new Status(SQL_EXCEPTION, e.getMessage()));
        } catch (Exception e) {
            logger.error("Exception:", e);
            result = Failure.of(new Status(GENERIC_EXCEPTION, e.getMessage()));
        }
        return result;
    }

    @Override
    public Result<String> getServiceIdLabel(String hostId) {
        Result<String> result = null;
        String sql = "SELECT service_id FROM api_version_t WHERE host_id =  ?";
        List<Map<String, Object>> labels = new ArrayList<>();
        try (Connection connection = ds.getConnection();
             PreparedStatement preparedStatement = connection.prepareStatement(sql)) {
            preparedStatement.setObject(1, UUID.fromString(hostId));
            try (ResultSet resultSet = preparedStatement.executeQuery()) {
                while (resultSet.next()) {
                    Map<String, Object> map = new HashMap<>();
                    map.put("id", resultSet.getString("service_id"));
                    map.put("label", resultSet.getString("service_id"));
                    labels.add(map);
                }
            }
            result = Success.of(JsonMapper.toJson(labels));
        } catch (SQLException e) {
            logger.error("SQLException:", e);
            result = Failure.of(new Status(SQL_EXCEPTION, e.getMessage()));
        } catch (Exception e) {
            logger.error("Exception:", e);
            result = Failure.of(new Status(GENERIC_EXCEPTION, e.getMessage()));
        }
        return result;
    }

    @Override
    public Result<String> createOrg(Map<String, Object> event) {
        final String insertOrg = "INSERT INTO org_t (domain, org_name, org_desc, org_owner, update_user, update_ts) " +
                "VALUES (?, ?, ?, ?, ?,  ?)";
        final String insertHost = "INSERT INTO host_t(host_id, domain, sub_domain, host_desc, host_owner, update_user, update_ts) " +
                "VALUES (?, ?, ?, ?, ?, ?, ?)";
        final String insertRole = "INSERT INTO role_t (host_id, role_id, role_desc, update_user, update_ts) " +
                "VALUES (?, ?, ?, ?, ?)";
        final String insertRoleUser = "INSERT INTO role_user_t (host_id, role_id, user_id, update_user, update_ts) " +
                "VALUES (?, ?, ?, ?, ?)";
        final String updateUserHost = "UPDATE user_host_t SET host_id = ?, update_user = ?, update_ts = ? WHERE user_id = ?";


        Result<String> result;
        Map<String, Object> map = (Map<String, Object>)event.get(PortalConstants.DATA);
        try (Connection conn = ds.getConnection()) {
            conn.setAutoCommit(false);
            try (PreparedStatement statement = conn.prepareStatement(insertOrg)) {
                statement.setString(1, (String)map.get("domain"));
                statement.setString(2, (String)map.get("orgName"));
                statement.setString(3, (String)map.get("orgDesc"));
                statement.setString(4, (String)map.get("orgOwner"));  // org owner is the user id in the eventId
                statement.setString(5, (String)event.get(Constants.USER));
                statement.setObject(6, OffsetDateTime.parse((String)event.get(CloudEventV1.TIME)));
                int count = statement.executeUpdate();
                if (count == 0) {
                    throw new SQLException("failed to insert the org " + map.get("domain"));
                }
                try (PreparedStatement hostStatement = conn.prepareStatement(insertHost)) {
                    hostStatement.setString(1, (String)event.get(Constants.HOST));
                    hostStatement.setString(2, (String)map.get("domain"));
                    hostStatement.setString(3, (String)map.get("subDomain"));
                    hostStatement.setString(4, (String)map.get("hostDesc"));
                    hostStatement.setString(5, (String)map.get("hostOwner")); // host owner can be another person selected by the org owner.
                    hostStatement.setString(6, (String)event.get(Constants.USER));
                    hostStatement.setObject(7, OffsetDateTime.parse((String)event.get(CloudEventV1.TIME)));
                    hostStatement.executeUpdate();
                }
                // create user, org-admin and host-admin roles for the hostId by default.
                try (PreparedStatement roleStatement = conn.prepareStatement(insertRole)) {
                    roleStatement.setString(1, (String)event.get(Constants.HOST));
                    roleStatement.setString(2, "user");
                    roleStatement.setString(3, "user role");
                    roleStatement.setString(4, (String)event.get(Constants.USER));
                    roleStatement.setObject(5, OffsetDateTime.parse((String)event.get(CloudEventV1.TIME)));
                    roleStatement.executeUpdate();
                }
                try (PreparedStatement roleStatement = conn.prepareStatement(insertRole)) {
                    roleStatement.setString(1, (String)event.get(Constants.HOST));
                    roleStatement.setString(2, "org-admin");
                    roleStatement.setString(3, "org-admin role");
                    roleStatement.setString(4, (String)event.get(Constants.USER));
                    roleStatement.setObject(5, OffsetDateTime.parse((String)event.get(CloudEventV1.TIME)));
                    roleStatement.executeUpdate();
                }
                try (PreparedStatement roleStatement = conn.prepareStatement(insertRole)) {
                    roleStatement.setString(1, (String)event.get(Constants.HOST));
                    roleStatement.setString(2, "host-admin");
                    roleStatement.setString(3, "host-admin role");
                    roleStatement.setString(4, (String)event.get(Constants.USER));
                    roleStatement.setObject(5, OffsetDateTime.parse((String)event.get(CloudEventV1.TIME)));
                    roleStatement.executeUpdate();
                }
                // insert role user to user for the host
                try (PreparedStatement roleUserStatement = conn.prepareStatement(insertRoleUser)) {
                    roleUserStatement.setString(1, (String)event.get(Constants.HOST));
                    roleUserStatement.setString(2, "user");
                    roleUserStatement.setString(3, (String)map.get("orgOwner"));
                    roleUserStatement.setString(4, (String)event.get(Constants.USER));
                    roleUserStatement.setObject(5, OffsetDateTime.parse((String)event.get(CloudEventV1.TIME)));
                    roleUserStatement.executeUpdate();
                }
                // insert role org-admin to user for the host
                try (PreparedStatement roleUserStatement = conn.prepareStatement(insertRoleUser)) {
                    roleUserStatement.setString(1, (String)event.get(Constants.HOST));
                    roleUserStatement.setString(2, "org-admin");
                    roleUserStatement.setString(3, (String)map.get("orgOwner"));
                    roleUserStatement.setString(4, (String)event.get(Constants.USER));
                    roleUserStatement.setObject(5, OffsetDateTime.parse((String)event.get(CloudEventV1.TIME)));
                    roleUserStatement.executeUpdate();
                }
                // insert host-admin to user for the host
                try (PreparedStatement roleUserStatement = conn.prepareStatement(insertRoleUser)) {
                    roleUserStatement.setString(1, (String)event.get(Constants.HOST));
                    roleUserStatement.setString(2, "host-admin");
                    roleUserStatement.setString(3, (String)map.get("hostOwner"));
                    roleUserStatement.setString(4, (String)event.get(Constants.USER));
                    roleUserStatement.setObject(5, OffsetDateTime.parse((String)event.get(CloudEventV1.TIME)));
                    roleUserStatement.executeUpdate();
                }
                // switch the current user to the hostId by updating to same user pointing to two hosts.
                try (PreparedStatement userHostStatement = conn.prepareStatement(updateUserHost)) {
                    userHostStatement.setString(1, (String)event.get(Constants.HOST));
                    userHostStatement.setString(2, (String)event.get(Constants.USER));
                    userHostStatement.setObject(3, OffsetDateTime.parse((String)event.get(CloudEventV1.TIME)));
                    userHostStatement.setString(4, (String)map.get("orgOwner"));

                    userHostStatement.executeUpdate();
                }
                conn.commit();
                result = Success.of((String)map.get("domain"));
                insertNotification(event, true, null);
            } catch (SQLException e) {
                logger.error("SQLException:", e);
                conn.rollback();
                insertNotification(event, false, e.getMessage());
                result = Failure.of(new Status(SQL_EXCEPTION, e.getMessage()));
            } catch (Exception e) {
                logger.error("Exception:", e);
                conn.rollback();
                result = Failure.of(new Status(GENERIC_EXCEPTION, e.getMessage()));
            }
        } catch (SQLException e) {
            logger.error("SQLException:", e);
            result = Failure.of(new Status(SQL_EXCEPTION, e.getMessage()));
        }
        return result;
    }

    @Override
    public Result<String> updateOrg(Map<String, Object> event) {
        final String updateHost = "UPDATE org_t SET org_name = ?, org_desc = ?, org_owner = ?, " +
                "update_user = ?, update_ts = ? " +
                "WHERE domain = ?";

        Result<String> result = null;
        Map<String, Object> map = (Map<String, Object>)event.get(PortalConstants.DATA);
        try (Connection conn = ds.getConnection()) {
            conn.setAutoCommit(false);
            try (PreparedStatement statement = conn.prepareStatement(updateHost)) {
                String orgName = (String)map.get("orgName");
                if (orgName != null && !orgName.isEmpty()) {
                    statement.setString(1, orgName);
                } else {
                    statement.setNull(1, NULL);
                }
                String orgDesc = (String)map.get("orgDesc");
                if (orgDesc != null && !orgDesc.isEmpty()) {
                    statement.setString(2, orgDesc);
                } else {
                    statement.setNull(2, NULL);
                }
                String orgOwner = (String)map.get("orgOwner");
                if (orgOwner != null && !orgOwner.isEmpty()) {
                    statement.setString(3, orgOwner);
                } else {
                    statement.setNull(3, NULL);
                }
                statement.setString(4, (String)event.get(Constants.USER));
                statement.setObject(5, OffsetDateTime.parse((String)event.get(CloudEventV1.TIME)));
                statement.setString(6, (String)map.get("domain"));

                int count = statement.executeUpdate();
                if (count == 0) {
                    // no record is updated, write an error notification.
                    throw new SQLException("no record is updated for org " + map.get("domain"));
                }
                conn.commit();
                result = Success.of((String)map.get("domain"));
                insertNotification(event, true, null);
            } catch (SQLException e) {
                logger.error("SQLException:", e);
                conn.rollback();
                insertNotification(event, false, e.getMessage());
                result = Failure.of(new Status(SQL_EXCEPTION, e.getMessage()));
            } catch (Exception e) {
                logger.error("Exception:", e);
                conn.rollback();
                insertNotification(event, false, e.getMessage());
                result = Failure.of(new Status(GENERIC_EXCEPTION, e.getMessage()));
            }
        } catch (SQLException e) {
            logger.error("SQLException:", e);
            result = Failure.of(new Status(SQL_EXCEPTION, e.getMessage()));
        }
        return result;

    }

    @Override
    public Result<String> deleteOrg(Map<String, Object> event) {
        final String deleteHost = "DELETE FROM org_t WHERE domain = ?";
        Result<String> result;
        Map<String, Object> map = (Map<String, Object>)event.get(PortalConstants.DATA);
        try (Connection conn = ds.getConnection()) {
            conn.setAutoCommit(false);
            try (PreparedStatement statement = conn.prepareStatement(deleteHost)) {
                statement.setString(1, (String)map.get("domain"));
                int count = statement.executeUpdate();
                if (count == 0) {
                    throw new SQLException("no record is deleted for org " + map.get("domain"));
                }
                conn.commit();
                result = Success.of((String)map.get("domain"));
                insertNotification(event, true, null);
            } catch (SQLException e) {
                logger.error("SQLException:", e);
                conn.rollback();
                insertNotification(event, false, e.getMessage());
                result = Failure.of(new Status(SQL_EXCEPTION, e.getMessage()));
            } catch (Exception e) {
                logger.error("Exception:", e);
                conn.rollback();
                insertNotification(event, false, e.getMessage());
                result = Failure.of(new Status(GENERIC_EXCEPTION, e.getMessage()));
            }
        } catch (SQLException e) {
            logger.error("SQLException:", e);
            result = Failure.of(new Status(SQL_EXCEPTION, e.getMessage()));
        }
        return result;
    }

    @Override
    public Result<String> createHost(Map<String, Object> event) {
        final String insertHost = "INSERT INTO host_t (host_id, domain, sub_domain, host_desc, host_owner, update_user, update_ts) " +
                "VALUES (?, ?, ?, ?, ?,  ?, ?)";
        Result<String> result;
        Map<String, Object> map = (Map<String, Object>)event.get(PortalConstants.DATA);
        try (Connection conn = ds.getConnection()) {
            conn.setAutoCommit(false);
            try (PreparedStatement statement = conn.prepareStatement(insertHost)) {
                statement.setString(1, (String)event.get(Constants.HOST));
                statement.setString(2, (String)map.get("domain"));
                statement.setString(3, (String)map.get("subDomain"));
                statement.setString(4, (String)map.get("hostDesc"));
                statement.setString(5, (String)map.get("hostOwner"));
                statement.setString(6, (String)event.get(Constants.USER));
                statement.setObject(7, OffsetDateTime.parse((String)event.get(CloudEventV1.TIME)));
                int count = statement.executeUpdate();
                if (count == 0) {
                    throw new SQLException("failed to insert the host " + map.get("domain"));
                }
                conn.commit();
                result = Success.of((String)event.get(Constants.HOST));
                insertNotification(event, true, null);
            } catch (SQLException e) {
                logger.error("SQLException:", e);
                conn.rollback();
                insertNotification(event, false, e.getMessage());
                result = Failure.of(new Status(SQL_EXCEPTION, e.getMessage()));
            } catch (Exception e) {
                logger.error("Exception:", e);
                conn.rollback();
                result = Failure.of(new Status(GENERIC_EXCEPTION, e.getMessage()));
            }
        } catch (SQLException e) {
            logger.error("SQLException:", e);
            result = Failure.of(new Status(SQL_EXCEPTION, e.getMessage()));
        }
        return result;
    }

    @Override
    public Result<String> updateHost(Map<String, Object> event) {
        final String updateHost = "UPDATE host_t SET domain = ?, sub_domain = ?, host_desc = ?, host_owner = ?, " +
                "update_user = ?, update_ts = ? " +
                "WHERE host_id = ?";

        Result<String> result = null;
        Map<String, Object> map = (Map<String, Object>)event.get(PortalConstants.DATA);
        try (Connection conn = ds.getConnection()) {
            conn.setAutoCommit(false);

            try (PreparedStatement statement = conn.prepareStatement(updateHost)) {
                statement.setString(1, (String)map.get("domain"));
                String subDomain = (String)map.get("subDomain");
                if (subDomain != null && !subDomain.isEmpty()) {
                    statement.setString(2, subDomain);
                } else {
                    statement.setNull(2, NULL);
                }
                String hostDesc = (String)map.get("hostDesc");
                if (hostDesc != null && !hostDesc.isEmpty()) {
                    statement.setString(3, hostDesc);
                } else {
                    statement.setNull(3, NULL);
                }
                String hostOwner = (String)map.get("hostOwner");
                if (hostOwner != null && !hostOwner.isEmpty()) {
                    statement.setString(4, hostOwner);
                } else {
                    statement.setNull(4, NULL);
                }
                statement.setString(5, (String)event.get(Constants.USER));
                statement.setObject(6, OffsetDateTime.parse((String)event.get(CloudEventV1.TIME)));
                statement.setString(7, (String)event.get(Constants.HOST));

                int count = statement.executeUpdate();
                if (count == 0) {
                    // no record is updated, write an error notification.
                    throw new SQLException("no record is updated for host " + (String)event.get(Constants.HOST));
                }
                conn.commit();
                result = Success.of((String)event.get(Constants.HOST));
                insertNotification(event, true, null);
            } catch (SQLException e) {
                logger.error("SQLException:", e);
                conn.rollback();
                insertNotification(event, false, e.getMessage());
                result = Failure.of(new Status(SQL_EXCEPTION, e.getMessage()));
            } catch (Exception e) {
                logger.error("Exception:", e);
                conn.rollback();
                insertNotification(event, false, e.getMessage());
                result = Failure.of(new Status(GENERIC_EXCEPTION, e.getMessage()));
            }
        } catch (SQLException e) {
            logger.error("SQLException:", e);
            result = Failure.of(new Status(SQL_EXCEPTION, e.getMessage()));
        }
        return result;
    }

    @Override
    public Result<String> deleteHost(Map<String, Object> event) {
        final String deleteHost = "DELETE from host_t WHERE host_id = ?";
        Result<String> result;
        Map<String, Object> map = (Map<String, Object>)event.get(PortalConstants.DATA);
        try (Connection conn = ds.getConnection()) {
            conn.setAutoCommit(false);
            try (PreparedStatement statement = conn.prepareStatement(deleteHost)) {
                statement.setString(1, (String)event.get(Constants.HOST));
                int count = statement.executeUpdate();
                if (count == 0) {
                    throw new SQLException("no record is deleted for host " + (String)event.get(Constants.HOST));
                }
                conn.commit();
                result = Success.of((String)event.get(Constants.HOST));
                insertNotification(event, true, null);
            } catch (SQLException e) {
                logger.error("SQLException:", e);
                conn.rollback();
                insertNotification(event, false, e.getMessage());
                result = Failure.of(new Status(SQL_EXCEPTION, e.getMessage()));
            } catch (Exception e) {
                logger.error("Exception:", e);
                conn.rollback();
                insertNotification(event, false, e.getMessage());
                result = Failure.of(new Status(GENERIC_EXCEPTION, e.getMessage()));
            }
        } catch (SQLException e) {
            logger.error("SQLException:", e);
            result = Failure.of(new Status(SQL_EXCEPTION, e.getMessage()));
        }
        return result;
    }

    @Override
    public Result<String> switchHost(Map<String, Object> event) {
        final String updateUserHost = "UPDATE user_host_t SET host_id = ?, update_user = ?, update_ts = ? WHERE user_id = ?";
        Result<String> result;
        Map<String, Object> map = (Map<String, Object>)event.get(PortalConstants.DATA);
        try (Connection conn = ds.getConnection()) {
            conn.setAutoCommit(false);
            try (PreparedStatement statement = conn.prepareStatement(updateUserHost)) {
                statement.setString(1, (String)event.get(Constants.HOST));
                statement.setString(2, (String)event.get(Constants.USER));
                statement.setObject(3, OffsetDateTime.parse((String)event.get(CloudEventV1.TIME)));
                statement.setString(4, (String)event.get(Constants.USER));
                int count = statement.executeUpdate();
                if (count == 0) {
                    throw new SQLException("no record is updated for user " + (String)event.get(Constants.USER));
                }
                conn.commit();
                result = Success.of((String)event.get(Constants.USER));
                insertNotification(event, true, null);
            } catch (SQLException e) {
                logger.error("SQLException:", e);
                conn.rollback();
                insertNotification(event, false, e.getMessage());
                result = Failure.of(new Status(SQL_EXCEPTION, e.getMessage()));
            } catch (Exception e) {
                logger.error("Exception:", e);
                conn.rollback();
                insertNotification(event, false, e.getMessage());
                result = Failure.of(new Status(GENERIC_EXCEPTION, e.getMessage()));
            }
        } catch (SQLException e) {
            logger.error("SQLException:", e);
            result = Failure.of(new Status(SQL_EXCEPTION, e.getMessage()));
        }
        return result;
    }

    @Override
    public Result<String> queryHostDomainById(String hostId) {
        final String sql = "SELECT sub_domain || '.' || domain AS domain FROM host_t WHERE host_id = ?";
        Result<String> result;
        String domain = null;
        try (final Connection conn = ds.getConnection(); final PreparedStatement statement = conn.prepareStatement(sql)) {
            statement.setObject(1, UUID.fromString(hostId));
            try (ResultSet resultSet = statement.executeQuery()) {
                if (resultSet.next()) {
                    domain = resultSet.getString("domain");
                }
            }
            if (domain == null)
                result = Failure.of(new Status(OBJECT_NOT_FOUND, "host domain", hostId));
            else
                result = Success.of(domain);
        } catch (SQLException e) {
            logger.error("SQLException:", e);
            result = Failure.of(new Status(SQL_EXCEPTION, e.getMessage()));
        } catch (Exception e) {
            logger.error("Exception:", e);
            result = Failure.of(new Status(GENERIC_EXCEPTION, e.getMessage()));
        }
        return result;
    }

    @Override
    public Result<String> queryHostById(String id) {
        final String queryHostById = "SELECT host_id, domain, sub_domain, host_desc, host_owner, " +
                "update_user, update_ts FROM host_t WHERE host_id = ?";
        Result<String> result;
        try (final Connection conn = ds.getConnection()) {
            Map<String, Object> map = new HashMap<>();
            try (PreparedStatement statement = conn.prepareStatement(queryHostById)) {
                statement.setObject(1, UUID.fromString(id));
                try (ResultSet resultSet = statement.executeQuery()) {
                    if (resultSet.next()) {
                        map.put("hostId", resultSet.getObject("host_id", UUID.class));
                        map.put("domain", resultSet.getString("domain"));
                        map.put("subDomain", resultSet.getString("sub_domain"));
                        map.put("hostDesc", resultSet.getString("host_desc"));
                        map.put("hostOwner", resultSet.getString("host_owner"));
                        map.put("updateUser", resultSet.getString("update_user"));
                        map.put("updateTs", resultSet.getObject("update_ts") != null ? resultSet.getObject("update_ts", OffsetDateTime.class) : null);
                    }
                }
            }
            if (map.isEmpty())
                result = Failure.of(new Status(OBJECT_NOT_FOUND, "host with id", id));
            else
                result = Success.of(JsonMapper.toJson(map));
        } catch (SQLException e) {
            logger.error("SQLException:", e);
            result = Failure.of(new Status(SQL_EXCEPTION, e.getMessage()));
        } catch (Exception e) {
            logger.error("Exception:", e);
            result = Failure.of(new Status(GENERIC_EXCEPTION, e.getMessage()));
        }
        return result;
    }

    @Override
    public Result<Map<String, Object>> queryHostByOwner(String owner) {
        final String queryHostByOwner = "SELECT * from host_t WHERE org_owner = ?";
        Result<Map<String, Object>> result;
        try (final Connection conn = ds.getConnection()) {
            Map<String, Object> map = new HashMap<>();
            try (PreparedStatement statement = conn.prepareStatement(queryHostByOwner)) {
                statement.setString(1, owner);
                try (ResultSet resultSet = statement.executeQuery()) {
                    if (resultSet.next()) {
                        map.put("hostId", resultSet.getObject("host_id", UUID.class));
                        map.put("hostDomain", resultSet.getString("host_domain"));
                        map.put("orgName", resultSet.getString("org_name"));
                        map.put("orgDesc", resultSet.getString("org_desc"));
                        map.put("orgOwner", resultSet.getString("org_owner"));
                        map.put("jwk", resultSet.getString("jwk"));
                        map.put("updateUser", resultSet.getString("update_user"));
                        map.put("updateTs", resultSet.getObject("update_ts") != null ? resultSet.getObject("update_ts", OffsetDateTime.class) : null);
                    }
                }
            }
            if (map.size() == 0)
                result = Failure.of(new Status(OBJECT_NOT_FOUND, "host with owner ", owner));
            else
                result = Success.of(map);
        } catch (SQLException e) {
            logger.error("SQLException:", e);
            result = Failure.of(new Status(SQL_EXCEPTION, e.getMessage()));
        } catch (Exception e) {
            logger.error("Exception:", e);
            result = Failure.of(new Status(GENERIC_EXCEPTION, e.getMessage()));
        }
        return result;
    }

    @Override
    public Result<String> getOrg(int offset, int limit, String domain, String orgName, String orgDesc, String orgOwner) {
        Result<String> result = null;
        StringBuilder sqlBuilder = new StringBuilder();
        sqlBuilder.append("SELECT COUNT(*) OVER () AS total,\n" +
                "domain, org_name, org_desc, org_owner, update_user, update_ts \n" +
                "FROM org_t\n" +
                "WHERE 1=1\n");

        List<Object> parameters = new ArrayList<>();

        StringBuilder whereClause = new StringBuilder();

        addCondition(whereClause, parameters, "domain", domain);
        addCondition(whereClause, parameters, "org_name", orgName);
        addCondition(whereClause, parameters, "org_desc", orgDesc);
        addCondition(whereClause, parameters, "org_owner", orgOwner);

        if (!whereClause.isEmpty()) {
            sqlBuilder.append("AND ").append(whereClause);
        }

        sqlBuilder.append("ORDER BY domain\n" +
                "LIMIT ? OFFSET ?");

        parameters.add(limit);
        parameters.add(offset);

        String sql = sqlBuilder.toString();
        int total = 0;
        List<Map<String, Object>> orgs = new ArrayList<>();

        try (Connection connection = ds.getConnection();
             PreparedStatement preparedStatement = connection.prepareStatement(sql)) {

            for (int i = 0; i < parameters.size(); i++) {
                preparedStatement.setObject(i + 1, parameters.get(i));
            }


            boolean isFirstRow = true;
            try (ResultSet resultSet = preparedStatement.executeQuery()) {
                while (resultSet.next()) {
                    Map<String, Object> map = new HashMap<>();
                    if (isFirstRow) {
                        total = resultSet.getInt("total");
                        isFirstRow = false;
                    }
                    map.put("domain", resultSet.getString("domain"));
                    map.put("orgName", resultSet.getString("org_name"));
                    map.put("orgDesc", resultSet.getString("org_desc"));
                    map.put("orgOwner", resultSet.getString("org_owner"));
                    map.put("updateUser", resultSet.getString("update_user"));
                    // handling date properly
                    map.put("updateTs", resultSet.getTimestamp("update_ts") != null ? resultSet.getTimestamp("update_ts").toString() : null);
                    orgs.add(map);
                }
            }


            Map<String, Object> resultMap = new HashMap<>();
            resultMap.put("total", total);
            resultMap.put("orgs", orgs);
            result = Success.of(JsonMapper.toJson(resultMap));


        } catch (SQLException e) {
            logger.error("SQLException:", e);
            result = Failure.of(new Status(SQL_EXCEPTION, e.getMessage()));
        } catch (Exception e) {
            logger.error("Exception:", e);
            result = Failure.of(new Status(GENERIC_EXCEPTION, e.getMessage()));
        }
        return result;
    }

    @Override
    public Result<String> getHost(int offset, int limit, String hostId, String domain, String subDomain, String hostDesc, String hostOwner) {
        Result<String> result = null;
        StringBuilder sqlBuilder = new StringBuilder();
        sqlBuilder.append("SELECT COUNT(*) OVER () AS total,\n" +
                "host_id, domain, sub_domain, host_desc, host_owner, update_user, update_ts \n" +
                "FROM host_t\n" +
                "WHERE 1=1\n");


        List<Object> parameters = new ArrayList<>();

        StringBuilder whereClause = new StringBuilder();

        addCondition(whereClause, parameters, "host_id", hostId != null ? UUID.fromString(hostId) : null);
        addCondition(whereClause, parameters, "domain", domain);
        addCondition(whereClause, parameters, "sub_domain", subDomain);
        addCondition(whereClause, parameters, "host_desc", hostDesc);
        addCondition(whereClause, parameters, "host_owner", hostOwner);

        if (!whereClause.isEmpty()) {
            sqlBuilder.append("AND ").append(whereClause);
        }

        sqlBuilder.append(" ORDER BY domain\n" +
                "LIMIT ? OFFSET ?");


        parameters.add(limit);
        parameters.add(offset);
        String sql = sqlBuilder.toString();
        if(logger.isTraceEnabled()) logger.trace("sql: {}", sql);
        int total = 0;
        List<Map<String, Object>> hosts = new ArrayList<>();

        try (Connection connection = ds.getConnection();
             PreparedStatement preparedStatement = connection.prepareStatement(sql)) {

            for (int i = 0; i < parameters.size(); i++) {
                preparedStatement.setObject(i + 1, parameters.get(i));
            }

            boolean isFirstRow = true;
            try (ResultSet resultSet = preparedStatement.executeQuery()) {
                if(logger.isTraceEnabled()) logger.trace("resultSet: {}", resultSet);
                while (resultSet.next()) {
                    if(logger.isTraceEnabled()) logger.trace("at least there is 1 row here in the resultSet");
                    Map<String, Object> map = new HashMap<>();
                    if (isFirstRow) {
                        total = resultSet.getInt("total");
                        isFirstRow = false;
                    }
                    map.put("hostId", resultSet.getObject("host_id", UUID.class));
                    map.put("domain", resultSet.getString("domain"));
                    map.put("subDomain", resultSet.getString("sub_domain"));
                    map.put("hostDesc", resultSet.getString("host_desc"));
                    map.put("hostOwner", resultSet.getString("host_owner"));
                    map.put("updateUser", resultSet.getString("update_user"));
                    // handling date properly
                    map.put("updateTs", resultSet.getTimestamp("update_ts") != null ? resultSet.getTimestamp("update_ts").toString() : null);
                    hosts.add(map);
                }
            }

            Map<String, Object> resultMap = new HashMap<>();
            resultMap.put("total", total);
            resultMap.put("hosts", hosts);
            if(logger.isTraceEnabled()) logger.trace("resultMap: {}", resultMap);
            result = Success.of(JsonMapper.toJson(resultMap));
        } catch (SQLException e) {
            logger.error("SQLException:", e);
            result = Failure.of(new Status(SQL_EXCEPTION, e.getMessage()));
        } catch (Exception e) {
            logger.error("Exception:", e);
            result = Failure.of(new Status(GENERIC_EXCEPTION, e.getMessage()));
        }
        return result;
    }

    @Override
    public Result<String> getHostByDomain(String domain, String subDomain, String hostDesc) {
        Result<String> result = null;
        StringBuilder sqlBuilder = new StringBuilder();
        sqlBuilder.append("SELECT host_id, domain, sub_domain, host_desc, host_owner, update_user, update_ts \n" +
                "FROM host_t\n" +
                "WHERE 1=1\n");

        List<Object> parameters = new ArrayList<>();

        StringBuilder whereClause = new StringBuilder();
        addCondition(whereClause, parameters, "domain", domain);
        addCondition(whereClause, parameters, "sub_domain", subDomain);
        addCondition(whereClause, parameters, "host_desc", hostDesc);

        if (!whereClause.isEmpty()) {
            sqlBuilder.append("AND ").append(whereClause);
        }

        sqlBuilder.append(" ORDER BY sub_domain");

        String sql = sqlBuilder.toString();
        if(logger.isTraceEnabled()) logger.trace("sql: {}", sql);
        List<Map<String, Object>> hosts = new ArrayList<>();
        try (Connection connection = ds.getConnection();
             PreparedStatement preparedStatement = connection.prepareStatement(sql)) {

            for (int i = 0; i < parameters.size(); i++) {
                preparedStatement.setObject(i + 1, parameters.get(i));
            }

            try (ResultSet resultSet = preparedStatement.executeQuery()) {
                while (resultSet.next()) {
                    Map<String, Object> map = new HashMap<>();
                    map.put("hostId", resultSet.getObject("host_id", UUID.class));
                    map.put("domain", resultSet.getString("domain"));
                    map.put("subDomain", resultSet.getString("sub_domain"));
                    map.put("hostDesc", resultSet.getString("host_desc"));
                    map.put("hostOwner", resultSet.getString("host_owner"));
                    map.put("updateUser", resultSet.getString("update_user"));
                    map.put("updateTs", resultSet.getTimestamp("update_ts") != null ? resultSet.getTimestamp("update_ts").toString() : null);
                    hosts.add(map);
                }
            }

            if(hosts.isEmpty()) {
                result = Failure.of(new Status(OBJECT_NOT_FOUND, "host", "domain, subDomain or hostDesc"));
            } else {
                result = Success.of(JsonMapper.toJson(hosts));
            }
        } catch (SQLException e) {
            logger.error("SQLException:", e);
            result = Failure.of(new Status(SQL_EXCEPTION, e.getMessage()));
        }  catch (Exception e) {
            logger.error("Exception:", e);
            result = Failure.of(new Status(GENERIC_EXCEPTION, e.getMessage()));
        }
        return result;
    }

    @Override
    public Result<String> getHostLabel() {
        final String getHostLabel = "SELECT host_id, domain, sub_domain FROM host_t ORDER BY domain, sub_domain";
        Result<String> result;
        List<Map<String, Object>> hosts = new ArrayList<>();
        try (final Connection conn = ds.getConnection()) {
            try (PreparedStatement statement = conn.prepareStatement(getHostLabel)) {
                try (ResultSet resultSet = statement.executeQuery()) {
                    while (resultSet.next()) {
                        Map<String, Object> map = new HashMap<>();
                        map.put("id", resultSet.getString("host_id"));
                        map.put("label", resultSet.getString("sub_domain") + "." + resultSet.getString("domain"));
                        hosts.add(map);
                    }
                }
            }
            if(hosts.isEmpty()) {
                result = Failure.of(new Status(OBJECT_NOT_FOUND, "host", "any key"));
            } else {
                result = Success.of(JsonMapper.toJson(hosts));
            }
        } catch (SQLException e) {
            logger.error("SQLException:", e);
            result = Failure.of(new Status(SQL_EXCEPTION, e.getMessage()));
        } catch (Exception e) {
            logger.error("Exception:", e);
            result = Failure.of(new Status(GENERIC_EXCEPTION, e.getMessage()));
        }
        return result;
    }

    @Override
    public Result<String> createConfig(Map<String, Object> event) {
        final String sql = "INSERT INTO config_t(config_id, config_name, config_phase, config_type, light4j_version, " +
                "class_path, config_desc, update_user, update_ts) " +
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)";
        Result<String> result;
        Map<String, Object> map = (Map<String, Object>)event.get(PortalConstants.DATA);
        try (Connection conn = ds.getConnection()) {
            conn.setAutoCommit(false);
            try (PreparedStatement statement = conn.prepareStatement(sql)) {
                statement.setObject(1, UUID.fromString((String)map.get("configId")));
                statement.setString(2, (String)map.get("configName"));
                statement.setString(3, (String)map.get("configPhase"));
                statement.setString(4, (String)map.get("configType"));

                if (map.containsKey("light4jVersion")) {
                    statement.setString(5, (String) map.get("light4jVersion"));
                } else {
                    statement.setNull(5, Types.VARCHAR);
                }

                if (map.containsKey("classPath")) {
                    statement.setString(6, (String) map.get("classPath"));
                } else {
                    statement.setNull(6, Types.VARCHAR);
                }

                if (map.containsKey("configDesc")) {
                    statement.setString(7, (String) map.get("configDesc"));
                } else {
                    statement.setNull(7, Types.VARCHAR);
                }
                statement.setString(8, (String)event.get(Constants.USER));
                statement.setObject(9, OffsetDateTime.parse((String)event.get(CloudEventV1.TIME)));


                int count = statement.executeUpdate();
                if (count == 0) {
                    throw new SQLException("failed to insert the config with id " + map.get("configId"));
                }
                conn.commit();
                result = Success.of((String)map.get("configId"));
                insertNotification(event, true, null);

            } catch (SQLException e) {
                logger.error("SQLException:", e);
                conn.rollback();
                insertNotification(event, false, e.getMessage());
                result = Failure.of(new Status(SQL_EXCEPTION, e.getMessage()));
            } catch (Exception e) {
                logger.error("Exception:", e);
                conn.rollback();
                insertNotification(event, false, e.getMessage());
                result = Failure.of(new Status(GENERIC_EXCEPTION, e.getMessage()));
            }
        } catch (SQLException e) {
            logger.error("SQLException:", e);
            result = Failure.of(new Status(SQL_EXCEPTION, e.getMessage()));
        }
        return result;
    }

    @Override
    public Result<String> updateConfig(Map<String, Object> event) {
        final String sql = "UPDATE config_t SET config_name = ?, config_phase = ?, config_type = ?, " +
                "light4j_version = ?, class_path = ?, config_desc = ?, update_user = ?, update_ts = ? " +
                "WHERE config_id = ?";
        Result<String> result;
        Map<String, Object> map = (Map<String, Object>)event.get(PortalConstants.DATA);
        try (Connection conn = ds.getConnection()) {
            conn.setAutoCommit(false);
            try (PreparedStatement statement = conn.prepareStatement(sql)) {
                statement.setString(1, (String)map.get("configName"));
                statement.setString(2, (String)map.get("configPhase"));
                statement.setString(3, (String)map.get("configType"));

                if (map.containsKey("light4jVersion")) {
                    statement.setString(4, (String) map.get("light4jVersion"));
                } else {
                    statement.setNull(4, Types.VARCHAR);
                }

                if (map.containsKey("classPath")) {
                    statement.setString(5, (String) map.get("classPath"));
                } else {
                    statement.setNull(5, Types.VARCHAR);
                }

                if (map.containsKey("configDesc")) {
                    statement.setString(6, (String) map.get("configDesc"));
                } else {
                    statement.setNull(6, Types.VARCHAR);
                }
                statement.setString(7, (String)event.get(Constants.USER));
                statement.setObject(8, OffsetDateTime.parse((String)event.get(CloudEventV1.TIME)));
                statement.setObject(9, UUID.fromString((String)map.get("configId")));

                int count = statement.executeUpdate();
                if (count == 0) {
                    throw new SQLException("failed to update config with id " + map.get("configId"));
                }
                conn.commit();
                result = Success.of((String)map.get("configId"));
                insertNotification(event, true, null);
            }  catch (SQLException e) {
                logger.error("SQLException:", e);
                conn.rollback();
                insertNotification(event, false, e.getMessage());
                result = Failure.of(new Status(SQL_EXCEPTION, e.getMessage()));
            }  catch (Exception e) {
                logger.error("Exception:", e);
                conn.rollback();
                insertNotification(event, false, e.getMessage());
                result = Failure.of(new Status(GENERIC_EXCEPTION, e.getMessage()));
            }
        }  catch (SQLException e) {
            logger.error("SQLException:", e);
            result = Failure.of(new Status(SQL_EXCEPTION, e.getMessage()));
        }
        return result;
    }


    @Override
    public Result<String> deleteConfig(Map<String, Object> event) {
        final String sql = "DELETE FROM config_t WHERE config_id = ?";
        Result<String> result;
        Map<String, Object> map = (Map<String, Object>)event.get(PortalConstants.DATA);
        try (Connection conn = ds.getConnection()) {
            conn.setAutoCommit(false);
            try (PreparedStatement statement = conn.prepareStatement(sql)) {
                statement.setObject(1, UUID.fromString((String)map.get("configId")));

                int count = statement.executeUpdate();
                if (count == 0) {
                    throw new SQLException("failed to delete config with id " + map.get("configId"));
                }
                conn.commit();
                result = Success.of((String)map.get("configId"));
                insertNotification(event, true, null);
            } catch (SQLException e) {
                logger.error("SQLException:", e);
                conn.rollback();
                insertNotification(event, false, e.getMessage());
                result = Failure.of(new Status(SQL_EXCEPTION, e.getMessage()));
            } catch (Exception e) {
                logger.error("Exception:", e);
                conn.rollback();
                insertNotification(event, false, e.getMessage());
                result = Failure.of(new Status(GENERIC_EXCEPTION, e.getMessage()));
            }
        }  catch (SQLException e) {
            logger.error("SQLException:", e);
            result = Failure.of(new Status(SQL_EXCEPTION, e.getMessage()));
        }
        return result;
    }


    @Override
    public Result<String> getConfig(int offset, int limit, String configId, String configName, String configPhase,
                                    String configType, String light4jVersion, String classPath, String configDesc) {
        Result<String> result = null;
        StringBuilder sqlBuilder = new StringBuilder();
        sqlBuilder.append("SELECT COUNT(*) OVER () AS total,\n" +
                "config_id, config_name, config_phase, config_type, light4j_version, class_path, config_desc, update_user, update_ts\n" +
                "FROM config_t\n" +
                "WHERE 1=1\n");

        List<Object> parameters = new ArrayList<>();

        StringBuilder whereClause = new StringBuilder();
        addCondition(whereClause, parameters, "config_id", configId != null ? UUID.fromString(configId) : null);
        addCondition(whereClause, parameters, "config_name", configName);
        addCondition(whereClause, parameters, "config_phase", configPhase);
        addCondition(whereClause, parameters, "config_type", configType);
        addCondition(whereClause, parameters, "light4j_version", light4jVersion);
        addCondition(whereClause, parameters, "class_path", classPath);
        addCondition(whereClause, parameters, "config_desc", configDesc);

        if (!whereClause.isEmpty()) {
            sqlBuilder.append("AND ").append(whereClause);
        }

        sqlBuilder.append(" ORDER BY config_id\n" +
                "LIMIT ? OFFSET ?");

        parameters.add(limit);
        parameters.add(offset);

        String sql = sqlBuilder.toString();
        int total = 0;
        List<Map<String, Object>> configs = new ArrayList<>();

        try (Connection connection = ds.getConnection();
             PreparedStatement preparedStatement = connection.prepareStatement(sql)) {

            for (int i = 0; i < parameters.size(); i++) {
                preparedStatement.setObject(i + 1, parameters.get(i));
            }

            boolean isFirstRow = true;
            try (ResultSet resultSet = preparedStatement.executeQuery()) {
                while (resultSet.next()) {
                    Map<String, Object> map = new HashMap<>();
                    if (isFirstRow) {
                        total = resultSet.getInt("total");
                        isFirstRow = false;
                    }
                    map.put("configId", resultSet.getObject("config_id", UUID.class));
                    map.put("configName", resultSet.getString("config_name"));
                    map.put("configPhase", resultSet.getString("config_phase"));
                    map.put("configType", resultSet.getString("config_type"));
                    map.put("light4jVersion", resultSet.getString("light4j_version"));
                    map.put("classPath", resultSet.getString("class_path"));
                    map.put("configDesc", resultSet.getString("config_desc"));
                    map.put("updateUser", resultSet.getString("update_user"));
                    // handling date properly
                    map.put("updateTs", resultSet.getTimestamp("update_ts") != null ? resultSet.getTimestamp("update_ts").toString() : null);

                    configs.add(map);
                }
            }

            Map<String, Object> resultMap = new HashMap<>();
            resultMap.put("total", total);
            resultMap.put("configs", configs);
            result = Success.of(JsonMapper.toJson(resultMap));


        } catch (SQLException e) {
            logger.error("SQLException:", e);
            result = Failure.of(new Status(SQL_EXCEPTION, e.getMessage()));
        } catch (Exception e) {
            logger.error("Exception:", e);
            result = Failure.of(new Status(GENERIC_EXCEPTION, e.getMessage()));
        }
        return result;
    }

    @Override
    public Result<String> queryConfigById(String configId) {
        final String queryConfigById = "SELECT config_id, config_name, config_phase, config_type, light4j_version, " +
                "class_path, config_desc, update_user, update_ts FROM config_t WHERE config_id = ?";
        Result<String> result;
        Map<String, Object> config = new HashMap<>();

        try (Connection conn = ds.getConnection();
             PreparedStatement statement = conn.prepareStatement(queryConfigById)) {

            statement.setObject(1, UUID.fromString(configId));

            try (ResultSet resultSet = statement.executeQuery()) {
                if (resultSet.next()) {
                    config.put("configId", resultSet.getObject("config_id", UUID.class));
                    config.put("configName", resultSet.getString("config_name"));
                    config.put("configPhase", resultSet.getString("config_phase"));
                    config.put("configType", resultSet.getString("config_type"));
                    config.put("light4jVersion", resultSet.getString("light4j_version"));
                    config.put("classPath", resultSet.getString("class_path"));
                    config.put("configDesc", resultSet.getString("config_desc"));
                    config.put("updateUser", resultSet.getString("update_user"));
                    // handling date properly
                    config.put("updateTs", resultSet.getTimestamp("update_ts") != null ? resultSet.getTimestamp("update_ts").toString() : null);
                    result = Success.of(JsonMapper.toJson(config));
                } else {
                    result = Failure.of(new Status(OBJECT_NOT_FOUND, "config", configId));
                }
            }

        } catch (SQLException e) {
            logger.error("SQLException:", e);
            result = Failure.of(new Status(SQL_EXCEPTION, e.getMessage()));
        }  catch (Exception e) {
            logger.error("Exception:", e);
            result = Failure.of(new Status(GENERIC_EXCEPTION, e.getMessage()));
        }
        return result;
    }

    @Override
    public Result<String> getConfigIdLabel() {
        final String sql = "SELECT config_id, config_name FROM config_t ORDER BY config_name";
        Result<String> result;
        try (final Connection conn = ds.getConnection()) {
            List<Map<String, Object>> list = new ArrayList<>();
            try (PreparedStatement statement = conn.prepareStatement(sql)) {
                try (ResultSet resultSet = statement.executeQuery()) {
                    while (resultSet.next()) {
                        Map<String, Object> map = new HashMap<>();
                        map.put("id", resultSet.getObject("config_id", UUID.class));
                        map.put("label", resultSet.getString("config_name"));
                        list.add(map);
                    }
                }
            }
            if (list.isEmpty())
                result = Failure.of(new Status(OBJECT_NOT_FOUND, "configId", "any"));
            else
                result = Success.of(JsonMapper.toJson(list));
        } catch (SQLException e) {
            logger.error("SQLException:", e);
            result = Failure.of(new Status(SQL_EXCEPTION, e.getMessage()));
        } catch (Exception e) {
            logger.error("Exception:", e);
            result = Failure.of(new Status(GENERIC_EXCEPTION, e.getMessage()));
        }
        return result;
    }

    @Override
    public Result<String> getConfigIdApiAppLabel(String resourceType) {
        final String sql = "SELECT distinct c.config_id, c.config_name \n" +
                "FROM config_t c, config_property_t p \n" +
                "WHERE c.config_id = p.config_id \n" +
                "AND (p.value_type = 'map' or p.value_type = 'list')\n" +
                "AND p.resource_type LIKE ?\n" +
                "ORDER BY config_name\n";
        Result<String> result;
        try (final Connection conn = ds.getConnection()) {
            List<Map<String, Object>> list = new ArrayList<>();
            try (PreparedStatement statement = conn.prepareStatement(sql)) {
                statement.setString(1, "%" + resourceType + "%");
                try (ResultSet resultSet = statement.executeQuery()) {
                    while (resultSet.next()) {
                        Map<String, Object> map = new HashMap<>();
                        map.put("id", resultSet.getString("config_id"));
                        map.put("label", resultSet.getString("config_name"));
                        list.add(map);
                    }
                }
            }
            if (list.isEmpty())
                result = Failure.of(new Status(OBJECT_NOT_FOUND, "configId", "any"));
            else
                result = Success.of(JsonMapper.toJson(list));
        } catch (SQLException e) {
            logger.error("SQLException:", e);
            result = Failure.of(new Status(SQL_EXCEPTION, e.getMessage()));
        } catch (Exception e) {
            logger.error("Exception:", e);
            result = Failure.of(new Status(GENERIC_EXCEPTION, e.getMessage()));
        }
        return result;
    }

    @Override
    public Result<String> getPropertyNameLabel(String configId) {
        final String sql = "SELECT property_name FROM config_property_t WHERE config_id = ? ORDER BY display_order";
        Result<String> result;
        try (final Connection conn = ds.getConnection()) {
            List<Map<String, Object>> list = new ArrayList<>();
            try (PreparedStatement statement = conn.prepareStatement(sql)) {
                statement.setObject(1, UUID.fromString(configId));
                try (ResultSet resultSet = statement.executeQuery()) {
                    while (resultSet.next()) {
                        Map<String, Object> map = new HashMap<>();
                        map.put("id", resultSet.getString("property_name"));
                        map.put("label", resultSet.getString("property_name"));
                        list.add(map);
                    }
                }
            }
            if (list.isEmpty())
                result = Failure.of(new Status(OBJECT_NOT_FOUND, "config property", configId));
            else
                result = Success.of(JsonMapper.toJson(list));
        } catch (SQLException e) {
            logger.error("SQLException:", e);
            result = Failure.of(new Status(SQL_EXCEPTION, e.getMessage()));
        } catch (Exception e) {
            logger.error("Exception:", e);
            result = Failure.of(new Status(GENERIC_EXCEPTION, e.getMessage()));
        }
        return result;
    }

    @Override
    public Result<String> getPropertyNameApiAppLabel(String configId, String resourceType) {
        final String sql = "SELECT property_name \n" +
                "FROM config_property_t\n" +
                "WHERE config_id = ?\n" +
                "AND (value_type = 'map' or value_type = 'list')\n" +
                "AND resource_type LIKE ? \n" +
                "ORDER BY display_order\n";
        Result<String> result;
        try (final Connection conn = ds.getConnection()) {
            List<Map<String, Object>> list = new ArrayList<>();
            try (PreparedStatement statement = conn.prepareStatement(sql)) {
                statement.setObject(1, UUID.fromString(configId));
                statement.setString(2, "%" + resourceType + "%");
                try (ResultSet resultSet = statement.executeQuery()) {
                    while (resultSet.next()) {
                        Map<String, Object> map = new HashMap<>();
                        map.put("id", resultSet.getString("property_name"));
                        map.put("label", resultSet.getString("property_name"));
                        list.add(map);
                    }
                }
            }
            if (list.isEmpty())
                result = Failure.of(new Status(OBJECT_NOT_FOUND, "config property", configId));
            else
                result = Success.of(JsonMapper.toJson(list));
        } catch (SQLException e) {
            logger.error("SQLException:", e);
            result = Failure.of(new Status(SQL_EXCEPTION, e.getMessage()));
        } catch (Exception e) {
            logger.error("Exception:", e);
            result = Failure.of(new Status(GENERIC_EXCEPTION, e.getMessage()));
        }
        return result;
    }

    @Override
    public Result<String> createConfigProperty(Map<String, Object> event) {
        final String sql = "INSERT INTO config_property_t (config_id, property_name, property_type, property_value, property_file, " +
                "resource_type, value_type, display_order, required, property_desc, light4j_version, update_user, update_ts) " +
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
        Result<String> result;
        Map<String, Object> map = (Map<String, Object>)event.get(PortalConstants.DATA);
        try (Connection conn = ds.getConnection()) {
            conn.setAutoCommit(false); // Start transaction
            try (PreparedStatement statement = conn.prepareStatement(sql)) {
                statement.setObject(1, UUID.fromString((String)map.get("configId")));
                statement.setString(2, (String)map.get("propertyName"));
                statement.setString(3, (String)map.get("propertyType"));

                // Handle property_value (required)
                if (map.containsKey("propertyValue")) {
                    statement.setString(4, (String) map.get("propertyValue"));
                } else {
                    statement.setNull(4, Types.VARCHAR); // Or throw exception if it's truly required, but DB default is not set.
                }

                // Handle property_file (optional)
                if (map.containsKey("propertyFile")) {
                    statement.setString(5, (String) map.get("propertyFile"));
                } else {
                    statement.setNull(5, Types.VARCHAR);
                }
                // Handle resource_type (optional)
                if (map.containsKey("resourceType")) {
                    statement.setString(6, (String) map.get("resourceType"));
                } else {
                    statement.setString(6, "none");
                }

                // Handle value_type (optional)
                if (map.containsKey("valueType")) {
                    statement.setString(7, (String) map.get("valueType"));
                } else {
                    statement.setNull(7, Types.VARCHAR);
                }

                // Handle display_order (optional)
                if (map.containsKey("displayOrder")) {
                    statement.setInt(8, Integer.parseInt(map.get("displayOrder").toString()));
                } else {
                    statement.setNull(8, Types.INTEGER);
                }

                // Handle required (optional)
                if (map.containsKey("required")) {
                    statement.setBoolean(9, Boolean.parseBoolean(map.get("required").toString()));
                } else {
                    statement.setBoolean(9, false);
                }

                // Handle property_desc (optional)
                if (map.containsKey("propertyDesc")) {
                    statement.setString(10, (String) map.get("propertyDesc"));
                } else {
                    statement.setNull(10, Types.VARCHAR);
                }

                // Handle light4j_version (optional)
                if(map.containsKey("light4jVersion")) {
                    statement.setString(11, (String) map.get("light4jVersion"));
                } else {
                    statement.setNull(11, Types.VARCHAR);
                }


                statement.setString(12, (String)event.get(Constants.USER));
                statement.setObject(13, OffsetDateTime.parse((String)event.get(CloudEventV1.TIME)));


                int count = statement.executeUpdate();
                if (count == 0) {
                    throw new SQLException("Failed to insert the config property with id " + map.get("configId") + " and name " + map.get("propertyName"));
                }
                conn.commit(); // Commit transaction
                result = Success.of((String)map.get("configId"));
                // Assuming insertNotification is a method you have for handling notifications
                insertNotification(event, true, null);

            } catch (SQLException e) {
                logger.error("SQLException:", e);
                conn.rollback(); // Rollback transaction on error
                insertNotification(event, false, e.getMessage());
                result = Failure.of(new Status("SQL_EXCEPTION", e.getMessage())); // Use Status
            } catch (Exception e) {
                logger.error("Exception:", e);
                conn.rollback(); // Rollback transaction on error
                insertNotification(event, false, e.getMessage());

                result = Failure.of(new Status("GENERIC_EXCEPTION", e.getMessage()));  // Use Status
            }
        } catch (SQLException e) {
            logger.error("SQLException:", e);
            result = Failure.of(new Status("SQL_EXCEPTION", e.getMessage())); // Use Status
        }
        return result;
    }

    @Override
    public Result<String> updateConfigProperty(Map<String, Object> event) {
        final String sql = "UPDATE config_property_t SET property_type = ?, property_value = ?, property_file = ?, " +
                "resource_type = ?, value_type = ?, display_order = ?, required = ?, property_desc = ?, " +
                "light4j_version = ?, update_user = ?, update_ts = ? " +
                "WHERE config_id = ? AND property_name = ?";

        Result<String> result;
        Map<String, Object> map = (Map<String, Object>)event.get(PortalConstants.DATA);
        try (Connection conn = ds.getConnection()) {
            conn.setAutoCommit(false);
            try (PreparedStatement statement = conn.prepareStatement(sql)) {
                // Set the update values from the event and the parsed JSON
                statement.setString(1, (String)map.get("propertyType"));

                // Handle property_value (optional in update, but check in map)
                if (map.containsKey("propertyValue")) {
                    statement.setString(2, (String) map.get("propertyValue"));
                } else {
                    statement.setNull(2, Types.VARCHAR); // Or keep existing value if you prefer
                }

                // Handle property_file
                if (map.containsKey("propertyFile")) {
                    statement.setString(3, (String) map.get("propertyFile"));
                } else {
                    statement.setNull(3, Types.VARCHAR);
                }

                // Handle resource_type
                if (map.containsKey("resourceType")) {
                    statement.setString(4, (String) map.get("resourceType"));
                } else {
                    statement.setNull(4, Types.VARCHAR); // Could set to 'none' or a DB default, or keep existing.
                }

                // Handle value_type
                if (map.containsKey("valueType")) {
                    statement.setString(5, (String) map.get("valueType"));
                } else {
                    statement.setNull(5, Types.VARCHAR);
                }

                // Handle display_order
                if (map.containsKey("displayOrder")) {
                    statement.setInt(6, Integer.parseInt(map.get("displayOrder").toString()));
                } else {
                    statement.setNull(6, Types.INTEGER);
                }

                // Handle required
                if (map.containsKey("required")) {
                    statement.setBoolean(7, Boolean.parseBoolean(map.get("required").toString()));
                } else {
                    statement.setNull(7, Types.BOOLEAN); //or statement.setBoolean(7, false);
                }

                // Handle property_desc
                if (map.containsKey("propertyDesc")) {
                    statement.setString(8, (String) map.get("propertyDesc"));
                } else {
                    statement.setNull(8, Types.VARCHAR);
                }

                // Handle light4j_version
                if (map.containsKey("light4jVersion")) {
                    statement.setString(9, (String) map.get("light4jVersion"));
                } else {
                    statement.setNull(9, Types.VARCHAR);
                }

                statement.setString(10, (String)event.get(Constants.USER));
                statement.setObject(11, OffsetDateTime.parse((String)event.get(CloudEventV1.TIME)));

                // WHERE clause: Crucial for updating the correct row!
                statement.setObject(12, UUID.fromString((String)map.get("configId")));
                statement.setString(13, (String)map.get("propertyName"));


                int count = statement.executeUpdate();
                if (count == 0) {
                    // No rows were updated.  This could mean the config_id and property_name
                    // combination doesn't exist, or it could be a concurrency issue.
                    throw new SQLException("Failed to update config property.  No rows affected for config_id: " + map.get("configId") + " and property_name: " + map.get("propertyName"));
                }

                conn.commit();
                result = Success.of((String)map.get("configId"));
                insertNotification(event, true, null);

            } catch (SQLException e) {
                logger.error("SQLException:", e);
                conn.rollback();
                insertNotification(event, false, e.getMessage());
                result = Failure.of(new Status("SQL_EXCEPTION", e.getMessage()));
            } catch (Exception e) {
                logger.error("Exception:", e);
                conn.rollback();
                insertNotification(event, false, e.getMessage());
                result = Failure.of(new Status("GENERIC_EXCEPTION", e.getMessage()));
            }
        } catch (SQLException e) {
            logger.error("SQLException:", e);
            result = Failure.of(new Status("SQL_EXCEPTION", e.getMessage()));
        }
        return result;
    }

    @Override
    public Result<String> deleteConfigProperty(Map<String, Object> event) {
        final String sql = "DELETE FROM config_property_t WHERE config_id = ? AND property_name = ?";
        Result<String> result;
        Map<String, Object> map = (Map<String, Object>)event.get(PortalConstants.DATA);
        try (Connection conn = ds.getConnection()) {
            conn.setAutoCommit(false);
            try (PreparedStatement statement = conn.prepareStatement(sql)) {
                statement.setObject(1, UUID.fromString((String)map.get("configId")));
                statement.setString(2, (String)map.get("propertyName"));

                int count = statement.executeUpdate();
                if (count == 0) {
                    throw new SQLException("Failed to delete config property. No rows affected for config_id: " + map.get("configId") + " and property_name: " + map.get("propertyName"));
                }
                conn.commit();
                result = Success.of((String)map.get("configId"));
                insertNotification(event, true, null);

            } catch (SQLException e) {
                logger.error("SQLException:", e);
                conn.rollback();
                insertNotification(event, false, e.getMessage());
                result = Failure.of(new Status("SQL_EXCEPTION", e.getMessage()));
            } catch (Exception e) {
                logger.error("Exception:", e);
                conn.rollback();
                insertNotification(event, false, e.getMessage());
                result = Failure.of(new Status("GENERIC_EXCEPTION", e.getMessage()));
            }
        } catch (SQLException e) {
            logger.error("SQLException:", e);
            result = Failure.of(new Status("SQL_EXCEPTION", e.getMessage()));
        }
        return result;
    }



    @Override
    public Result<String> getConfigProperty(int offset, int limit, String configId, String configName, String propertyName,
                                            String propertyType, String light4jVersion, Integer displayOrder, Boolean required,
                                            String propertyDesc, String propertyValue, String valueType, String propertyFile,
                                            String resourceType) {
        Result<String> result = null;

        StringBuilder sqlBuilder = new StringBuilder();
        sqlBuilder.append("SELECT COUNT(*) OVER () AS total,\n" +
                "cp.config_id, cp.property_name, cp.property_type, cp.light4j_version, cp.display_order, cp.required, " +
                "cp.property_desc, cp.property_value, cp.value_type, cp.property_file, cp.resource_type, cp.update_user, cp.update_ts, " +
                "c.config_name \n" +
                "FROM config_property_t cp\n" +
                "JOIN config_t c ON cp.config_id = c.config_id\n" +
                "WHERE 1=1\n");

        List<Object> parameters = new ArrayList<>();

        StringBuilder whereClause = new StringBuilder();
        addCondition(whereClause, parameters, "cp.config_id", configId != null ? UUID.fromString(configId) : null);
        addCondition(whereClause, parameters, "c.config_name", configName);
        addCondition(whereClause, parameters, "cp.property_name", propertyName);
        addCondition(whereClause, parameters, "cp.property_type", propertyType);
        addCondition(whereClause, parameters, "cp.light4j_version", light4jVersion);
        addCondition(whereClause, parameters, "cp.display_order", displayOrder);
        addCondition(whereClause, parameters, "cp.required", required);
        addCondition(whereClause, parameters, "cp.property_desc", propertyDesc);
        addCondition(whereClause, parameters, "cp.property_value", propertyValue);
        addCondition(whereClause, parameters, "cp.value_type", valueType);
        addCondition(whereClause, parameters, "cp.property_file", propertyFile);
        addCondition(whereClause, parameters, "cp.resource_type", resourceType);


        if (whereClause.length() > 0) {
            sqlBuilder.append("AND ").append(whereClause);
        }

        sqlBuilder.append(" ORDER BY cp.config_id, cp.property_name\n" +
                "LIMIT ? OFFSET ?");

        parameters.add(limit);
        parameters.add(offset);

        String sql = sqlBuilder.toString();
        int total = 0;
        List<Map<String, Object>> configProperties = new ArrayList<>();

        try (Connection connection = ds.getConnection();
             PreparedStatement preparedStatement = connection.prepareStatement(sql)) {

            for (int i = 0; i < parameters.size(); i++) {
                preparedStatement.setObject(i + 1, parameters.get(i));
            }

            boolean isFirstRow = true;
            try (ResultSet resultSet = preparedStatement.executeQuery()) {
                while (resultSet.next()) {
                    Map<String, Object> map = new HashMap<>();
                    if (isFirstRow) {
                        total = resultSet.getInt("total");
                        isFirstRow = false;
                    }
                    map.put("configId", resultSet.getObject("config_id", UUID.class));
                    map.put("configName", resultSet.getString("config_name")); // Get config_name
                    map.put("propertyName", resultSet.getString("property_name"));
                    map.put("propertyType", resultSet.getString("property_type"));
                    map.put("light4jVersion", resultSet.getString("light4j_version"));
                    map.put("displayOrder", resultSet.getInt("display_order"));  // Could be null
                    map.put("required", resultSet.getBoolean("required"));      // Could be null
                    map.put("propertyDesc", resultSet.getString("property_desc"));
                    map.put("propertyValue", resultSet.getString("property_value"));
                    map.put("valueType", resultSet.getString("value_type"));
                    map.put("propertyFile", resultSet.getString("property_file"));
                    map.put("resourceType", resultSet.getString("resource_type"));
                    map.put("updateUser", resultSet.getString("update_user"));
                    map.put("updateTs", resultSet.getTimestamp("update_ts") != null ? resultSet.getTimestamp("update_ts").toString() : null);

                    configProperties.add(map);
                }
            }

            Map<String, Object> resultMap = new HashMap<>();
            resultMap.put("total", total);
            resultMap.put("configProperties", configProperties);  // Changed key name
            result = Success.of(JsonMapper.toJson(resultMap));

        } catch (SQLException e) {
            logger.error("SQLException:", e);
            result = Failure.of(new Status("SQL_EXCEPTION", e.getMessage()));
        } catch (Exception e) {
            logger.error("Exception:", e);
            result = Failure.of(new Status("GENERIC_EXCEPTION", e.getMessage()));
        }
        return result;
    }

    @Override
    public Result<String> queryConfigPropertyById(String configId) {
        Result<String> result = null;

        String sql = "SELECT cp.config_id, cp.property_name, cp.property_type, cp.light4j_version, cp.display_order, cp.required, " +
                "cp.property_desc, cp.property_value, cp.value_type, cp.property_file, cp.resource_type, cp.update_user, cp.update_ts, " +
                "c.config_name " +
                "FROM config_property_t cp " +
                "JOIN config_t c ON cp.config_id = c.config_id " +
                "WHERE cp.config_id = ?";

        try (Connection connection = ds.getConnection();
             PreparedStatement preparedStatement = connection.prepareStatement(sql)) {

            preparedStatement.setObject(1, UUID.fromString(configId));

            List<Map<String, Object>> configProperties = new ArrayList<>();
            try (ResultSet resultSet = preparedStatement.executeQuery()) {
                while (resultSet.next()) {
                    Map<String, Object> map = new HashMap<>();
                    map.put("configId", resultSet.getObject("config_id", UUID.class));
                    map.put("configName", resultSet.getString("config_name"));
                    map.put("propertyName", resultSet.getString("property_name"));
                    map.put("propertyType", resultSet.getString("property_type"));
                    map.put("light4jVersion", resultSet.getString("light4j_version"));
                    map.put("displayOrder", resultSet.getInt("display_order"));
                    map.put("required", resultSet.getBoolean("required"));
                    map.put("propertyDesc", resultSet.getString("property_desc"));
                    map.put("propertyValue", resultSet.getString("property_value"));
                    map.put("valueType", resultSet.getString("value_type"));
                    map.put("propertyFile", resultSet.getString("property_file"));
                    map.put("resourceType", resultSet.getString("resource_type"));
                    map.put("updateUser", resultSet.getString("update_user"));
                    map.put("updateTs", resultSet.getTimestamp("update_ts") != null ? resultSet.getTimestamp("update_ts").toString() : null);
                    configProperties.add(map);
                }
            }

            if (configProperties.isEmpty()) {
                // return Failure.of(new Status("CONFIG_PROPERTY_NOT_FOUND", configId)); // Consider a more specific status
                result = Success.of("[]"); // Return an empty JSON array.  This is generally better than a 404.

            } else {
                result = Success.of(JsonMapper.toJson(configProperties)); // Return the list of properties as JSON
            }

        } catch (SQLException e) {
            logger.error("SQLException:", e);
            result = Failure.of(new Status("SQL_EXCEPTION", e.getMessage()));
        } catch (Exception e) {
            logger.error("Exception:", e);
            result = Failure.of(new Status("GENERIC_EXCEPTION", e.getMessage()));
        }
        return result;
    }

    @Override
    public Result<String> queryConfigPropertyByIdName(String configId, String propertyName) {
        Result<String> result = null;

        String sql = "SELECT cp.config_id, cp.property_name, cp.property_type, cp.light4j_version, cp.display_order, cp.required, " +
                "cp.property_desc, cp.property_value, cp.value_type, cp.property_file, cp.resource_type, cp.update_user, cp.update_ts, " +
                "c.config_name " +
                "FROM config_property_t cp " +
                "INNER JOIN config_t c ON cp.config_id = c.config_id " +
                "WHERE cp.config_id = ? " +
                "AND cp.property_name = ?";


        try (Connection connection = ds.getConnection();
             PreparedStatement preparedStatement = connection.prepareStatement(sql)) {

            preparedStatement.setObject(1, UUID.fromString(configId));
            preparedStatement.setString(2, propertyName);

            Map<String, Object> map = new HashMap<>();
            try (ResultSet resultSet = preparedStatement.executeQuery()) {
                if (resultSet.next()) {
                    map.put("configId", resultSet.getObject("config_id", UUID.class));
                    map.put("configName", resultSet.getString("config_name"));
                    map.put("propertyName", resultSet.getString("property_name"));
                    map.put("propertyType", resultSet.getString("property_type"));
                    map.put("light4jVersion", resultSet.getString("light4j_version"));
                    map.put("displayOrder", resultSet.getInt("display_order"));
                    map.put("required", resultSet.getBoolean("required"));
                    map.put("propertyDesc", resultSet.getString("property_desc"));
                    map.put("propertyValue", resultSet.getString("property_value"));
                    map.put("valueType", resultSet.getString("value_type"));
                    map.put("propertyFile", resultSet.getString("property_file"));
                    map.put("resourceType", resultSet.getString("resource_type"));
                    map.put("updateUser", resultSet.getString("update_user"));
                    map.put("updateTs", resultSet.getTimestamp("update_ts") != null ? resultSet.getTimestamp("update_ts").toString() : null);
                }
            }

            if (map.isEmpty()) {
                result = Failure.of(new Status(OBJECT_NOT_FOUND, "config property", "configId = " + configId + " propertyName = " + propertyName));
            } else {
                result = Success.of(JsonMapper.toJson(map));
            }
        } catch (SQLException e) {
            logger.error("SQLException:", e);
            result = Failure.of(new Status("SQL_EXCEPTION", e.getMessage()));
        } catch (Exception e) {
            logger.error("Exception:", e);
            result = Failure.of(new Status("GENERIC_EXCEPTION", e.getMessage()));
        }
        return result;
    }

    @Override
    public Result<String> createConfigEnvironment(Map<String, Object> event) {
        final String sql = "INSERT INTO environment_property_t (environment, config_id, property_name, " +
                "property_value, property_file, update_user, update_ts) " +
                "VALUES (?, ?, ?, ?, ?, ?, ?)";
        Result<String> result;
        Map<String, Object> map = (Map<String, Object>)event.get(PortalConstants.DATA);
        try (Connection conn = ds.getConnection()) {
            conn.setAutoCommit(false);
            try (PreparedStatement statement = conn.prepareStatement(sql)) {
                statement.setString(1, (String)map.get("environment"));
                statement.setObject(2, UUID.fromString((String)map.get("configId")));
                statement.setString(3, (String)map.get("propertyName"));

                // Handle property_value (optional)
                if (map.containsKey("propertyValue")) {
                    statement.setString(4, (String) map.get("propertyValue"));
                } else {
                    statement.setNull(4, Types.VARCHAR);
                }

                // Handle property_file (optional)
                if (map.containsKey("propertyFile")) {
                    statement.setString(5, (String) map.get("propertyFile"));
                } else {
                    statement.setNull(5, Types.VARCHAR);
                }

                statement.setString(6, (String)event.get(Constants.USER));
                statement.setObject(7, OffsetDateTime.parse((String)event.get(CloudEventV1.TIME)));

                int count = statement.executeUpdate();
                if (count == 0) {
                    throw new SQLException("Failed to insert environment property for environment: " + map.get("environment") +
                            ", config_id: " + map.get("configId") + ", property_name: " + map.get("propertyName"));
                }
                conn.commit();
                result = Success.of((String)map.get("configId"));
                insertNotification(event, true, null);


            } catch (SQLException e) {
                logger.error("SQLException:", e);
                conn.rollback();
                insertNotification(event, false, e.getMessage());
                result = Failure.of(new Status("SQL_EXCEPTION", e.getMessage()));
            } catch (Exception e) {
                logger.error("Exception:", e);
                conn.rollback();
                insertNotification(event, false, e.getMessage());

                result = Failure.of(new Status("GENERIC_EXCEPTION", e.getMessage()));
            }
        } catch (SQLException e) {
            logger.error("SQLException:", e);
            result = Failure.of(new Status("SQL_EXCEPTION", e.getMessage()));
        }
        return result;
    }

    @Override
    public Result<String> updateConfigEnvironment(Map<String, Object> event) {
        final String sql = "UPDATE environment_property_t SET property_value = ?, property_file = ?, update_user = ?, update_ts = ? " +
                "WHERE environment = ? AND config_id = ? AND property_name = ?";
        Result<String> result;
        Map<String, Object> map = (Map<String, Object>)event.get(PortalConstants.DATA);
        try (Connection conn = ds.getConnection()) {
            conn.setAutoCommit(false);
            try (PreparedStatement statement = conn.prepareStatement(sql)) {

                // Handle property_value (optional)
                if (map.containsKey("propertyValue")) {
                    statement.setString(1, (String) map.get("propertyValue"));
                } else {
                    statement.setNull(1, Types.VARCHAR); // Or keep existing
                }

                // Handle property_file (optional)
                if (map.containsKey("propertyFile")) {
                    statement.setString(2, (String) map.get("propertyFile"));
                } else {
                    statement.setNull(2, Types.VARCHAR); // Or keep existing
                }

                statement.setString(3, (String)event.get(Constants.USER));
                statement.setObject(4, OffsetDateTime.parse((String)event.get(CloudEventV1.TIME)));

                // WHERE clause parameters (from the event, *not* the JSON)
                statement.setString(5, (String)map.get("environment"));
                statement.setObject(6, UUID.fromString((String)map.get("configId")));
                statement.setString(7, (String)map.get("propertyName"));

                int count = statement.executeUpdate();
                if (count == 0) {
                    throw new SQLException("Failed to update environment property. No rows affected for environment: " + map.get("environment") +
                            ", config_id: " + map.get("configId") + ", property_name: " + map.get("propertyName"));
                }
                conn.commit();
                result = Success.of((String)map.get("configId"));  // Or a composite key.
                insertNotification(event, true, null);

            } catch (SQLException e) {
                logger.error("SQLException:", e);
                conn.rollback();
                insertNotification(event, false, e.getMessage());
                result = Failure.of(new Status("SQL_EXCEPTION", e.getMessage()));
            } catch (Exception e) {
                logger.error("Exception:", e);
                conn.rollback();
                insertNotification(event, false, e.getMessage());
                result = Failure.of(new Status("GENERIC_EXCEPTION", e.getMessage()));
            }
        } catch (SQLException e) {
            logger.error("SQLException:", e);
            result = Failure.of(new Status("SQL_EXCEPTION", e.getMessage()));
        }
        return result;
    }


    @Override
    public Result<String> deleteConfigEnvironment(Map<String, Object> event) {
        final String sql = "DELETE FROM environment_property_t WHERE environment = ? AND config_id = ? AND property_name = ?";
        Result<String> result;
        Map<String, Object> map = (Map<String, Object>)event.get(PortalConstants.DATA);
        try (Connection conn = ds.getConnection()) {
            conn.setAutoCommit(false);
            try (PreparedStatement statement = conn.prepareStatement(sql)) {
                statement.setString(1, (String)map.get("environment"));
                statement.setObject(2, UUID.fromString((String)map.get("configId")));
                statement.setString(3, (String)map.get("propertyName"));

                int count = statement.executeUpdate();
                if (count == 0) {
                    throw new SQLException("Failed to delete environment property. No rows affected for environment: " + map.get("environment") +
                            ", config_id: " + map.get("configId") + ", property_name: " + map.get("propertyName"));
                }
                conn.commit();
                result = Success.of((String)map.get("configId")); // Or a composite key
                insertNotification(event, true, null);

            } catch (SQLException e) {
                logger.error("SQLException:", e);
                conn.rollback();
                insertNotification(event, false, e.getMessage());
                result = Failure.of(new Status("SQL_EXCEPTION", e.getMessage()));
            } catch (Exception e) {
                logger.error("Exception:", e);
                conn.rollback();
                insertNotification(event, false, e.getMessage());
                result = Failure.of(new Status("GENERIC_EXCEPTION", e.getMessage()));
            }
        } catch (SQLException e) {
            logger.error("SQLException:", e);
            result = Failure.of(new Status("SQL_EXCEPTION", e.getMessage()));
        }
        return result;
    }

    @Override
    public Result<String> getConfigEnvironment(int offset, int limit, String environment, String configId, String configName,
                                               String propertyName, String propertyValue, String propertyFile) {
        Result<String> result = null;
        StringBuilder sqlBuilder = new StringBuilder();
        sqlBuilder.append("SELECT COUNT(*) OVER () AS total,\n" +
                "ep.environment, ep.config_id, ep.property_name, ep.property_value, ep.property_file, ep.update_user, ep.update_ts, \n" +
                "c.config_name \n" +  // Include config_name
                "FROM environment_property_t ep\n" +
                "JOIN config_t c ON ep.config_id = c.config_id\n" + // Join with config_t
                "WHERE 1=1\n");

        List<Object> parameters = new ArrayList<>();

        StringBuilder whereClause = new StringBuilder();
        addCondition(whereClause, parameters, "ep.environment", environment);
        addCondition(whereClause, parameters, "ep.config_id", configId != null ? UUID.fromString(configId) : null);
        addCondition(whereClause, parameters, "c.config_name", configName); // Filter by config_name
        addCondition(whereClause, parameters, "ep.property_name", propertyName);
        addCondition(whereClause, parameters, "ep.property_value", propertyValue);
        addCondition(whereClause, parameters, "ep.property_file", propertyFile);


        if (!whereClause.isEmpty()) {
            sqlBuilder.append("AND ").append(whereClause);
        }

        sqlBuilder.append(" ORDER BY ep.environment, ep.config_id, ep.property_name\n" +
                "LIMIT ? OFFSET ?");

        parameters.add(limit);
        parameters.add(offset);

        String sql = sqlBuilder.toString();
        int total = 0;
        List<Map<String, Object>> configEnvironments = new ArrayList<>();

        try (Connection connection = ds.getConnection();
             PreparedStatement preparedStatement = connection.prepareStatement(sql)) {

            for (int i = 0; i < parameters.size(); i++) {
                preparedStatement.setObject(i + 1, parameters.get(i));
            }

            boolean isFirstRow = true;
            try (ResultSet resultSet = preparedStatement.executeQuery()) {
                while (resultSet.next()) {
                    Map<String, Object> map = new HashMap<>();
                    if (isFirstRow) {
                        total = resultSet.getInt("total");
                        isFirstRow = false;
                    }
                    map.put("environment", resultSet.getString("environment"));
                    map.put("configId", resultSet.getObject("config_id", UUID.class));
                    map.put("configName", resultSet.getString("config_name")); // Get from joined table
                    map.put("propertyName", resultSet.getString("property_name"));
                    map.put("propertyValue", resultSet.getString("property_value"));
                    map.put("propertyFile", resultSet.getString("property_file"));
                    map.put("updateUser", resultSet.getString("update_user"));
                    map.put("updateTs", resultSet.getTimestamp("update_ts") != null ? resultSet.getTimestamp("update_ts").toString() : null);

                    configEnvironments.add(map);
                }
            }

            Map<String, Object> resultMap = new HashMap<>();
            resultMap.put("total", total);
            resultMap.put("configEnvironments", configEnvironments);
            result = Success.of(JsonMapper.toJson(resultMap));

        } catch (SQLException e) {
            logger.error("SQLException:", e);
            result = Failure.of(new Status("SQL_EXCEPTION", e.getMessage()));
        } catch (Exception e) {
            logger.error("Exception:", e);
            result = Failure.of(new Status("GENERIC_EXCEPTION", e.getMessage()));
        }
        return result;
    }


    @Override
    public Result<String> createInstanceApi(Map<String, Object> event) {
        final String sql = "INSERT INTO instance_api_t(host_id, instance_id, api_id, api_version, active, update_user, update_ts) " +
                "VALUES (?, ?, ?, ?, ?, ?, ?)";
        Result<String> result;
        Map<String, Object> map = (Map<String, Object>)event.get(PortalConstants.DATA);
        try (Connection conn = ds.getConnection()) {
            conn.setAutoCommit(false);
            try (PreparedStatement statement = conn.prepareStatement(sql)) {
                statement.setString(1, (String)event.get(Constants.HOST));
                statement.setObject(2, UUID.fromString((String)map.get("instanceId")));
                statement.setString(3, (String)map.get("apiId"));
                statement.setString(4, (String)map.get("apiVersion"));
                if (map.containsKey("active")) {
                    statement.setBoolean(5, (Boolean) map.get("active"));
                } else {
                    statement.setNull(5, Types.BOOLEAN);
                }
                statement.setString(6, (String)event.get(Constants.USER));
                statement.setObject(7, OffsetDateTime.parse((String)event.get(CloudEventV1.TIME)));

                int count = statement.executeUpdate();
                if (count == 0) {
                    throw new SQLException("failed to insert the instance api");
                }
                conn.commit();
                result = Success.of(String.format("Instance API created for instanceId: %s, apiId: %s, apiVersion: %s",
                        map.get("instanceId"), map.get("apiId"), map.get("apiVersion"))); // return some kind of key.
                insertNotification(event, true, null);

            }   catch (SQLException e) {
                logger.error("SQLException:", e);
                conn.rollback();
                insertNotification(event, false, e.getMessage());
                result = Failure.of(new Status(SQL_EXCEPTION, e.getMessage()));
            } catch (Exception e) {
                logger.error("Exception:", e);
                conn.rollback();
                insertNotification(event, false, e.getMessage());
                result = Failure.of(new Status(GENERIC_EXCEPTION, e.getMessage()));
            }
        } catch (SQLException e) {
            logger.error("SQLException:", e);
            result = Failure.of(new Status(SQL_EXCEPTION, e.getMessage()));
        }
        return result;
    }

    @Override
    public Result<String> updateInstanceApi(Map<String, Object> event) {
        final String sql = "UPDATE instance_api_t SET active = ?, update_user = ?, update_ts = ? " +
                "WHERE host_id = ? and instance_id = ? and api_id = ? and api_version = ?";
        Result<String> result;
        Map<String, Object> map = (Map<String, Object>)event.get(PortalConstants.DATA);
        try (Connection conn = ds.getConnection()) {
            conn.setAutoCommit(false);
            try (PreparedStatement statement = conn.prepareStatement(sql)) {
                if (map.containsKey("active")) {
                    statement.setBoolean(1, (Boolean) map.get("active"));
                } else {
                    statement.setNull(1, Types.BOOLEAN);
                }
                statement.setString(2, (String)event.get(Constants.USER));
                statement.setObject(3, OffsetDateTime.parse((String)event.get(CloudEventV1.TIME)));
                statement.setString(4, (String)event.get(Constants.HOST));
                statement.setObject(5, UUID.fromString((String)map.get("instanceId")));
                statement.setString(6, (String)map.get("apiId"));
                statement.setString(7, (String)map.get("apiVersion"));

                int count = statement.executeUpdate();
                if (count == 0) {
                    throw new SQLException("failed to update instance api");
                }
                conn.commit();
                result = Success.of(String.format("Instance API updated for instanceId: %s, apiId: %s, apiVersion: %s",
                        map.get("instanceId"), map.get("apiId"), map.get("apiVersion")));
                insertNotification(event, true, null);
            } catch (SQLException e) {
                logger.error("SQLException:", e);
                conn.rollback();
                insertNotification(event, false, e.getMessage());
                result = Failure.of(new Status(SQL_EXCEPTION, e.getMessage()));
            }  catch (Exception e) {
                logger.error("Exception:", e);
                conn.rollback();
                insertNotification(event, false, e.getMessage());
                result = Failure.of(new Status(GENERIC_EXCEPTION, e.getMessage()));
            }
        } catch (SQLException e) {
            logger.error("SQLException:", e);
            result = Failure.of(new Status(SQL_EXCEPTION, e.getMessage()));
        }
        return result;
    }

    @Override
    public Result<String> deleteInstanceApi(Map<String, Object> event) {
        final String sql = "DELETE FROM instance_api_t WHERE host_id = ? AND instance_id = ? AND api_id = ? AND api_version = ?";
        Result<String> result;
        Map<String, Object> map = (Map<String, Object>)event.get(PortalConstants.DATA);
        try (Connection conn = ds.getConnection()) {
            conn.setAutoCommit(false);
            try (PreparedStatement statement = conn.prepareStatement(sql)) {
                statement.setString(1, (String)event.get(Constants.HOST));
                statement.setObject(2, UUID.fromString((String)map.get("instanceId")));
                statement.setString(3, (String)map.get("apiId"));
                statement.setString(4, (String)map.get("apiVersion"));

                int count = statement.executeUpdate();
                if (count == 0) {
                    throw new SQLException("failed to delete the instance api with id " + (String)map.get("apiId"));
                }
                conn.commit();
                result = Success.of(String.format("Instance API deleted for instanceId: %s, apiId: %s, apiVersion: %s",
                        map.get("instanceId"), map.get("apiId"), map.get("apiVersion")));
                insertNotification(event, true, null);

            } catch (SQLException e) {
                logger.error("SQLException:", e);
                conn.rollback();
                insertNotification(event, false, e.getMessage());
                result = Failure.of(new Status(SQL_EXCEPTION, e.getMessage()));
            }  catch (Exception e) {
                logger.error("Exception:", e);
                conn.rollback();
                insertNotification(event, false, e.getMessage());
                result = Failure.of(new Status(GENERIC_EXCEPTION, e.getMessage()));
            }
        } catch (SQLException e) {
            logger.error("SQLException:", e);
            result = Failure.of(new Status(SQL_EXCEPTION, e.getMessage()));
        }
        return result;
    }

    @Override
    public Result<String> getInstanceApi(int offset, int limit, String hostId, String instanceId, String apiId, String apiVersion,
                                         Boolean active) {
        Result<String> result = null;
        StringBuilder sqlBuilder = new StringBuilder();
        sqlBuilder.append("SELECT COUNT(*) OVER () AS total,\n" +
                "host_id, instance_id, api_id, api_version, active, update_user, update_ts\n" +
                "FROM instance_api_t\n" +
                "WHERE 1=1\n");

        List<Object> parameters = new ArrayList<>();

        StringBuilder whereClause = new StringBuilder();

        addCondition(whereClause, parameters, "host_id", hostId != null ? UUID.fromString(hostId) : null);
        addCondition(whereClause, parameters, "instance_id", instanceId != null ? UUID.fromString(instanceId) : null);
        addCondition(whereClause, parameters, "api_id", apiId);
        addCondition(whereClause, parameters, "api_version", apiVersion);
        addCondition(whereClause, parameters, "active", active);


        if (!whereClause.isEmpty()) {
            sqlBuilder.append("AND ").append(whereClause);
        }

        sqlBuilder.append("ORDER BY instance_id, api_id, api_version\n" + // Added ordering
                "LIMIT ? OFFSET ?");

        parameters.add(limit);
        parameters.add(offset);

        String sql = sqlBuilder.toString();
        int total = 0;
        List<Map<String, Object>> instanceApis = new ArrayList<>();

        try (Connection connection = ds.getConnection();
             PreparedStatement preparedStatement = connection.prepareStatement(sql)) {

            for (int i = 0; i < parameters.size(); i++) {
                preparedStatement.setObject(i + 1, parameters.get(i));
            }

            boolean isFirstRow = true;
            try (ResultSet resultSet = preparedStatement.executeQuery()) {
                while (resultSet.next()) {
                    Map<String, Object> map = new HashMap<>();
                    if (isFirstRow) {
                        total = resultSet.getInt("total");
                        isFirstRow = false;
                    }
                    map.put("hostId", resultSet.getObject("host_id", UUID.class));
                    map.put("instanceId", resultSet.getObject("instance_id", UUID.class));
                    map.put("apiId", resultSet.getString("api_id"));
                    map.put("apiVersion", resultSet.getString("api_version"));
                    map.put("active", resultSet.getBoolean("active"));
                    map.put("updateUser", resultSet.getString("update_user"));
                    // handling date properly
                    map.put("updateTs", resultSet.getTimestamp("update_ts") != null ? resultSet.getTimestamp("update_ts").toString() : null);
                    instanceApis.add(map);
                }
            }

            Map<String, Object> resultMap = new HashMap<>();
            resultMap.put("total", total);
            resultMap.put("instanceApis", instanceApis);
            result = Success.of(JsonMapper.toJson(resultMap));


        } catch (SQLException e) {
            logger.error("SQLException:", e);
            result = Failure.of(new Status(SQL_EXCEPTION, e.getMessage()));
        } catch (Exception e) {
            logger.error("Exception:", e);
            result = Failure.of(new Status(GENERIC_EXCEPTION, e.getMessage()));
        }
        return result;
    }

    @Override
    public Result<String> createConfigInstanceApi(Map<String, Object> event) {
        final String sql = "INSERT INTO instance_api_property_t (host_id, instance_id, api_id, api_version, config_id, " +
                "property_name, property_value, update_user, update_ts) VALUES (?, ?, ?, ?, ?,  ?, ?, ?, ?)";
        Result<String> result;
        Map<String, Object> map = (Map<String, Object>)event.get(PortalConstants.DATA);
        try (Connection conn = ds.getConnection()) {
            conn.setAutoCommit(false);
            try (PreparedStatement statement = conn.prepareStatement(sql)) {
                statement.setString(1, (String)event.get(Constants.HOST));
                statement.setObject(2, UUID.fromString((String)map.get("instanceId")));
                statement.setString(3, (String)map.get("apiId"));
                statement.setString(4, (String)map.get("apiVersion"));
                statement.setObject(5, UUID.fromString((String)map.get("configId")));
                statement.setString(6, (String)map.get("propertyName"));
                if (map.containsKey("propertyValue")) {
                    statement.setString(7, (String)map.get("propertyValue"));
                } else {
                    statement.setNull(7, Types.VARCHAR);
                }
                statement.setString(8, (String)event.get(Constants.USER));
                statement.setObject(9, OffsetDateTime.parse((String)event.get(CloudEventV1.TIME)));

                int count = statement.executeUpdate();
                if (count == 0) {
                    throw new SQLException("Failed to insert instance API for host_id: " + event.get(Constants.HOST) +
                            ", instance_id: " + map.get("instanceId") + ", api_id: " + map.get("apiId") + ", api_version: " + map.get("apiVersion"));
                }
                conn.commit();
                result = Success.of((String)event.get(Constants.HOST) + "|" + map.get("instanceId") + "|" + map.get("apiId") + "|" + map.get("apiVersion"));
                insertNotification(event, true, null);

            } catch (SQLException e) {
                logger.error("SQLException:", e);
                conn.rollback();
                insertNotification(event, false, e.getMessage());
                result = Failure.of(new Status("SQL_EXCEPTION", e.getMessage()));
            } catch (Exception e) {
                logger.error("Exception:", e);
                conn.rollback();
                insertNotification(event, false, e.getMessage());
                result = Failure.of(new Status("GENERIC_EXCEPTION", e.getMessage()));
            }
        } catch (SQLException e) {
            logger.error("SQLException:", e);
            result = Failure.of(new Status("SQL_EXCEPTION", e.getMessage()));
        }
        return result;
    }

    @Override
    public Result<String> updateConfigInstanceApi(Map<String, Object> event) {
        final String sql = "UPDATE instance_api_property_t SET " +
                "property_value = ?, update_user = ?, update_ts = ? " +
                "WHERE host_id = ? AND instance_id = ? AND api_id = ? AND api_version = ? AND config_id = ? AND property_name = ?";
        Result<String> result;
        Map<String, Object> map = (Map<String, Object>)event.get(PortalConstants.DATA);
        try (Connection conn = ds.getConnection()) {
            conn.setAutoCommit(false);
            try (PreparedStatement statement = conn.prepareStatement(sql)) {
                if (map.containsKey("propertyValue")) {
                    statement.setString(1, (String)map.get("propertyValue"));
                } else {
                    statement.setNull(1, Types.VARCHAR);
                }
                statement.setString(2, (String)event.get(Constants.USER));
                statement.setObject(3, OffsetDateTime.parse((String)event.get(CloudEventV1.TIME)));
                statement.setString(4, (String)event.get(Constants.HOST));
                statement.setObject(5, UUID.fromString((String)map.get("instanceId")));
                statement.setString(6, (String)map.get("apiId"));
                statement.setString(7, (String)map.get("apiVersion"));
                statement.setObject(8, UUID.fromString((String)map.get("configId")));
                statement.setString(9, (String)map.get("propertyName"));

                int count = statement.executeUpdate();
                if (count == 0) {
                    throw new SQLException("Failed to update instance API. No rows affected for host_id: " + event.get(Constants.HOST) +
                            ", instance_id: " + map.get("instanceId") + ", api_id: " + map.get("apiId") + ", api_version: " + map.get("apiVersion") +
                            ", config_id: " + map.get("configId") + ", property_name: " + map.get("propertyName"));
                }
                conn.commit();
                result = Success.of(event.get(Constants.HOST) + "|" + map.get("instanceId") + "|" + map.get("apiId") + "|" + map.get("apiVersion") + "|" + map.get("configId") + "|" + map.get("propertyName"));
                insertNotification(event, true, null);

            } catch (SQLException e) {
                logger.error("SQLException:", e);
                conn.rollback();
                insertNotification(event, false, e.getMessage());
                result = Failure.of(new Status("SQL_EXCEPTION", e.getMessage()));
            } catch (Exception e) {
                logger.error("Exception:", e);
                conn.rollback();
                insertNotification(event, false, e.getMessage());
                result = Failure.of(new Status("GENERIC_EXCEPTION", e.getMessage()));
            }
        } catch (SQLException e) {
            logger.error("SQLException:", e);
            result = Failure.of(new Status("SQL_EXCEPTION", e.getMessage()));
        }
        return result;
    }

    @Override
    public Result<String> deleteConfigInstanceApi(Map<String, Object> event) {
        final String sql = "DELETE FROM instance_api_property_t " +
                "WHERE host_id = ? AND instance_id = ? AND api_id = ? AND api_version = ? AND config_id = ? AND property_name = ?";
        Result<String> result;
        Map<String, Object> map = (Map<String, Object>)event.get(PortalConstants.DATA);
        try (Connection conn = ds.getConnection()) {
            conn.setAutoCommit(false);
            try (PreparedStatement statement = conn.prepareStatement(sql)) {
                statement.setString(1, (String)event.get(Constants.HOST));
                statement.setObject(2, UUID.fromString((String)map.get("instanceId")));
                statement.setString(3, (String)map.get("apiId"));
                statement.setString(4, (String)map.get("apiVersion"));
                statement.setObject(5, UUID.fromString((String)map.get("configId")));
                statement.setString(6, (String)map.get("propertyName"));

                int count = statement.executeUpdate();
                if (count == 0) {
                    throw new SQLException("Failed to delete instance API. No rows affected for host_id: " + event.get(Constants.HOST) +
                            ", instance_id: " + map.get("instanceId") + ", api_id: " + map.get("apiId") + ", api_version: " + map.get("apiVersion"));
                }
                conn.commit();
                result = Success.of(event.get(Constants.HOST) + "|" + map.get("instanceId") + "|" + map.get("apiId") + "|" + map.get("apiVersion"));
                insertNotification(event, true, null);

            } catch (SQLException e) {
                logger.error("SQLException:", e);
                conn.rollback();
                insertNotification(event, false, e.getMessage());
                result = Failure.of(new Status("SQL_EXCEPTION", e.getMessage()));
            } catch (Exception e) {
                logger.error("Exception:", e);
                conn.rollback();
                insertNotification(event, false, e.getMessage());
                result = Failure.of(new Status("GENERIC_EXCEPTION", e.getMessage()));
            }
        } catch (SQLException e) {
            logger.error("SQLException:", e);
            result = Failure.of(new Status("SQL_EXCEPTION", e.getMessage()));
        }
        return result;
    }

    @Override
    public Result<String> getConfigInstanceApi(int offset, int limit, String hostId, String instanceId, String instanceName,
                                               String apiId, String apiVersion, String configId, String configName,
                                               String propertyName, String propertyValue, String propertyFile) {
        Result<String> result = null;

        StringBuilder sqlBuilder = new StringBuilder();
        sqlBuilder.append("SELECT COUNT(*) OVER () AS total,\n" +
                "ia.host_id, ia.instance_id, i.instance_name, ia.api_id, ia.api_version, ia.active, ia.update_user, ia.update_ts,\n" +
                "iap.config_id, c.config_name, iap.property_name, iap.property_value, iap.property_file\n" +
                "FROM instance_api_t ia\n" +
                "INNER JOIN instance_t i ON ia.host_id =i.host_id AND ia.instance_id = i.instance_id \n" +
                "INNER JOIN instance_api_property_t iap ON ia.host_id = iap.host_id AND ia.instance_id = iap.instance_id AND ia.api_id = iap.api_id AND ia.api_version = iap.api_version\n" +
                "INNER JOIN config_t c ON iap.config_id = c.config_id\n" +
                "WHERE 1=1\n");

        List<Object> parameters = new ArrayList<>();

        StringBuilder whereClause = new StringBuilder();
        addCondition(whereClause, parameters, "ia.host_id", hostId != null ? UUID.fromString(hostId) : null);
        addCondition(whereClause, parameters, "ia.instance_id", instanceId != null ? UUID.fromString(instanceId) : null);
        addCondition(whereClause, parameters, "i.instance_name", instanceName);
        addCondition(whereClause, parameters, "ia.api_id", apiId);
        addCondition(whereClause, parameters, "ia.api_version", apiVersion);
        addCondition(whereClause, parameters, "iap.config_id", configId != null ? UUID.fromString(configId) : null);
        addCondition(whereClause, parameters, "c.config_name", configName);
        addCondition(whereClause, parameters, "iap.property_name", propertyName);
        addCondition(whereClause, parameters, "iap.property_value", propertyValue);
        addCondition(whereClause, parameters, "iap.property_file", propertyFile);


        if (!whereClause.isEmpty()) {
            sqlBuilder.append("AND ").append(whereClause);
        }

        sqlBuilder.append(" ORDER BY ia.host_id, ia.instance_id, ia.api_id, ia.api_version, iap.config_id, iap.property_name\n" +
                "LIMIT ? OFFSET ?");

        parameters.add(limit);
        parameters.add(offset);

        String sql = sqlBuilder.toString();
        int total = 0;
        List<Map<String, Object>> instanceApis = new ArrayList<>();

        try (Connection connection = ds.getConnection();
             PreparedStatement preparedStatement = connection.prepareStatement(sql)) {

            for (int i = 0; i < parameters.size(); i++) {
                preparedStatement.setObject(i + 1, parameters.get(i));
            }

            boolean isFirstRow = true;
            try (ResultSet resultSet = preparedStatement.executeQuery()) {
                while (resultSet.next()) {
                    Map<String, Object> map = new HashMap<>();
                    if (isFirstRow) {
                        total = resultSet.getInt("total");
                        isFirstRow = false;
                    }

                    map.put("hostId", resultSet.getObject("host_id", UUID.class));
                    map.put("instanceId", resultSet.getObject("instance_id", UUID.class));
                    map.put("instanceName", resultSet.getString("instance_name"));
                    map.put("apiId", resultSet.getString("api_id"));
                    map.put("apiVersion", resultSet.getString("api_version"));
                    map.put("configId", resultSet.getObject("config_id", UUID.class));
                    map.put("configName", resultSet.getString("config_name"));
                    map.put("propertyName", resultSet.getString("property_name"));
                    map.put("propertyValue", resultSet.getString("property_value"));
                    map.put("propertyFile", resultSet.getString("property_file"));
                    map.put("updateUser", resultSet.getString("update_user"));
                    map.put("updateTs", resultSet.getTimestamp("update_ts") != null ? resultSet.getTimestamp("update_ts").toString() : null);
                    instanceApis.add(map);
                }
            }

            Map<String, Object> resultMap = new HashMap<>();
            resultMap.put("total", total);
            resultMap.put("instanceApis", instanceApis);
            result = Success.of(JsonMapper.toJson(resultMap));

        } catch (SQLException e) {
            logger.error("SQLException:", e);
            result = Failure.of(new Status("SQL_EXCEPTION", e.getMessage()));
        } catch (Exception e) {
            logger.error("Exception:", e);
            result = Failure.of(new Status("GENERIC_EXCEPTION", e.getMessage()));
        }
        return result;
    }

    @Override
    public Result<String> createInstanceApp(Map<String, Object> event) {
        final String sql = "INSERT INTO instance_app_t(host_id, instance_id, app_id, app_version, active, update_user, update_ts) " +
                "VALUES (?, ?, ?, ?, ?, ?, ?)";
        Result<String> result;
        Map<String, Object> map = (Map<String, Object>)event.get(PortalConstants.DATA);
        try (Connection conn = ds.getConnection()) {
            conn.setAutoCommit(false);
            try (PreparedStatement statement = conn.prepareStatement(sql)) {
                statement.setString(1, (String)event.get(Constants.HOST));
                statement.setObject(2, UUID.fromString((String)map.get("instanceId")));
                statement.setString(3, (String)map.get("appId"));
                statement.setString(4, (String)map.get("appVersion"));
                if (map.containsKey("active")) {
                    statement.setBoolean(5, (Boolean) map.get("active"));
                } else {
                    statement.setNull(5, Types.BOOLEAN);
                }
                statement.setString(6, (String)event.get(Constants.USER));
                statement.setObject(7, OffsetDateTime.parse((String)event.get(CloudEventV1.TIME)));
                int count = statement.executeUpdate();
                if (count == 0) {
                    throw new SQLException("failed to insert the instance app with event " + event.toString());
                }
                conn.commit();
                result = Success.of(String.format("Instance App created for instanceId: %s, appId: %s, appVersion: %s",
                        map.get("instanceId"), map.get("appId"), map.get("appVersion")));
                insertNotification(event, true, null);

            } catch (SQLException e) {
                logger.error("SQLException:", e);
                conn.rollback();
                insertNotification(event, false, e.getMessage());
                result = Failure.of(new Status(SQL_EXCEPTION, e.getMessage()));
            } catch (Exception e) {
                logger.error("Exception:", e);
                conn.rollback();
                insertNotification(event, false, e.getMessage());
                result = Failure.of(new Status(GENERIC_EXCEPTION, e.getMessage()));
            }
        } catch (SQLException e) {
            logger.error("SQLException:", e);
            result = Failure.of(new Status(SQL_EXCEPTION, e.getMessage()));
        }
        return result;
    }

    @Override
    public Result<String> updateInstanceApp(Map<String, Object> event) {
        final String sql = "UPDATE instance_app_t SET active = ?, update_user = ?, update_ts = ? " +
                "WHERE host_id = ? and instance_id = ? and app_id = ? and app_version = ?";
        Result<String> result;
        Map<String, Object> map = (Map<String, Object>)event.get(PortalConstants.DATA);
        try (Connection conn = ds.getConnection()) {
            conn.setAutoCommit(false);
            try (PreparedStatement statement = conn.prepareStatement(sql)) {
                if (map.containsKey("active")) {
                    statement.setBoolean(1, (Boolean) map.get("active"));
                } else {
                    statement.setNull(1, Types.BOOLEAN);
                }
                statement.setString(2, (String)event.get(Constants.USER));
                statement.setObject(3, OffsetDateTime.parse((String)event.get(CloudEventV1.TIME)));
                statement.setString(4, (String)event.get(Constants.HOST));
                statement.setObject(5, UUID.fromString((String)map.get("instanceId")));
                statement.setString(6, (String)map.get("appId"));
                statement.setString(7, (String)map.get("appVersion"));
                int count = statement.executeUpdate();
                if (count == 0) {
                    throw new SQLException("failed to update the instance app with id " + map.get("appId"));
                }
                conn.commit();
                result = Success.of(String.format("Instance App updated for instanceId: %s, appId: %s, appVersion: %s",
                        map.get("instanceId"), map.get("appId"), map.get("appVersion")));
                insertNotification(event, true, null);

            } catch (SQLException e) {
                logger.error("SQLException:", e);
                conn.rollback();
                insertNotification(event, false, e.getMessage());
                result = Failure.of(new Status(SQL_EXCEPTION, e.getMessage()));
            } catch (Exception e) {
                logger.error("Exception:", e);
                conn.rollback();
                insertNotification(event, false, e.getMessage());
                result = Failure.of(new Status(GENERIC_EXCEPTION, e.getMessage()));
            }
        } catch (SQLException e) {
            logger.error("SQLException:", e);
            result = Failure.of(new Status(SQL_EXCEPTION, e.getMessage()));
        }
        return result;
    }

    @Override
    public Result<String> deleteInstanceApp(Map<String, Object> event) {
        final String sql = "DELETE FROM instance_app_t WHERE host_id = ? AND instance_id = ? AND app_id = ? AND app_version = ?";
        Result<String> result;
        Map<String, Object> map = (Map<String, Object>)event.get(PortalConstants.DATA);
        try (Connection conn = ds.getConnection()) {
            conn.setAutoCommit(false);
            try (PreparedStatement statement = conn.prepareStatement(sql)) {
                statement.setString(1, (String)event.get(Constants.HOST));
                statement.setObject(2, UUID.fromString((String)map.get("instanceId")));
                statement.setString(3, (String)map.get("appId"));
                statement.setString(4, (String)map.get("appVersion"));

                int count = statement.executeUpdate();
                if (count == 0) {
                    throw new SQLException("failed to delete the instance app with id " + map.get("appId"));
                }
                conn.commit();
                result = Success.of(String.format("Instance app deleted for instanceId: %s, appId: %s, appVersion: %s",
                        map.get("instanceId"), map.get("appId"), map.get("appVersion")));
                insertNotification(event, true, null);
            } catch (SQLException e) {
                logger.error("SQLException:", e);
                conn.rollback();
                insertNotification(event, false, e.getMessage());
                result = Failure.of(new Status(SQL_EXCEPTION, e.getMessage()));
            } catch (Exception e) {
                logger.error("Exception:", e);
                conn.rollback();
                insertNotification(event, false, e.getMessage());
                result = Failure.of(new Status(GENERIC_EXCEPTION, e.getMessage()));
            }
        } catch (SQLException e) {
            logger.error("SQLException:", e);
            result = Failure.of(new Status(SQL_EXCEPTION, e.getMessage()));
        }
        return result;
    }

    @Override
    public Result<String> getInstanceApp(int offset, int limit, String hostId, String instanceId, String appId, String appVersion,
                                         Boolean active) {
        Result<String> result = null;
        StringBuilder sqlBuilder = new StringBuilder();
        sqlBuilder.append("SELECT COUNT(*) OVER () AS total,\n" +
                "host_id, instance_id, app_id, app_version, active, update_user, update_ts\n" +
                "FROM instance_app_t\n" +
                "WHERE 1=1\n");

        List<Object> parameters = new ArrayList<>();

        StringBuilder whereClause = new StringBuilder();

        addCondition(whereClause, parameters, "host_id", hostId != null ? UUID.fromString(hostId) : null);
        addCondition(whereClause, parameters, "instance_id", instanceId != null ? UUID.fromString(instanceId) : null);
        addCondition(whereClause, parameters, "app_id", appId);
        addCondition(whereClause, parameters, "app_version", appVersion);
        addCondition(whereClause, parameters, "active", active);

        if (!whereClause.isEmpty()) {
            sqlBuilder.append("AND ").append(whereClause);
        }

        sqlBuilder.append("ORDER BY instance_id, app_id, app_version\n" +
                "LIMIT ? OFFSET ?");

        parameters.add(limit);
        parameters.add(offset);

        String sql = sqlBuilder.toString();
        int total = 0;
        List<Map<String, Object>> instanceApps = new ArrayList<>();

        try (Connection connection = ds.getConnection();
             PreparedStatement preparedStatement = connection.prepareStatement(sql)) {

            for (int i = 0; i < parameters.size(); i++) {
                preparedStatement.setObject(i + 1, parameters.get(i));
            }

            boolean isFirstRow = true;
            try (ResultSet resultSet = preparedStatement.executeQuery()) {
                while (resultSet.next()) {
                    Map<String, Object> map = new HashMap<>();
                    if (isFirstRow) {
                        total = resultSet.getInt("total");
                        isFirstRow = false;
                    }
                    map.put("hostId", resultSet.getObject("host_id", UUID.class));
                    map.put("instanceId", resultSet.getObject("instance_id", UUID.class));
                    map.put("appId", resultSet.getString("app_id"));
                    map.put("appVersion", resultSet.getString("app_version"));
                    map.put("active", resultSet.getBoolean("active"));
                    map.put("updateUser", resultSet.getString("update_user"));
                    // handling date properly
                    map.put("updateTs", resultSet.getTimestamp("update_ts") != null ? resultSet.getTimestamp("update_ts").toString() : null);
                    instanceApps.add(map);
                }
            }

            Map<String, Object> resultMap = new HashMap<>();
            resultMap.put("total", total);
            resultMap.put("instanceApps", instanceApps);
            result = Success.of(JsonMapper.toJson(resultMap));


        } catch (SQLException e) {
            logger.error("SQLException:", e);
            result = Failure.of(new Status(SQL_EXCEPTION, e.getMessage()));
        } catch (Exception e) {
            logger.error("Exception:", e);
            result = Failure.of(new Status(GENERIC_EXCEPTION, e.getMessage()));
        }
        return result;
    }

    @Override
    public Result<String> createConfigInstanceApp(Map<String, Object> event) {
        final String sql = "INSERT INTO instance_app_property_t (host_id, instance_id, app_id, app_version, config_id, property_name, property_value, update_user, update_ts) " +
                "VALUES (?, ?, ?, ?, ?,  ?, ?, ?, ?)";
        Result<String> result;
        Map<String, Object> map = (Map<String, Object>)event.get(PortalConstants.DATA);
        try (Connection conn = ds.getConnection()) {
            conn.setAutoCommit(false);
            try (PreparedStatement statement = conn.prepareStatement(sql)) {
                statement.setString(1, (String)event.get(Constants.HOST));
                statement.setObject(2, UUID.fromString((String)map.get("instanceId")));
                statement.setString(3, (String)map.get("appId"));
                statement.setString(4, (String)map.get("appVersion"));
                statement.setObject(5, UUID.fromString((String)map.get("configId")));
                statement.setString(6, (String)map.get("propertyName"));
                if (map.containsKey("propertyValue")) {
                    statement.setString(7, (String)map.get("propertyValue"));
                } else {
                    statement.setNull(7, Types.VARCHAR);
                }
                statement.setString(8, (String)event.get(Constants.USER));
                statement.setObject(9, OffsetDateTime.parse((String)event.get(CloudEventV1.TIME)));

                int count = statement.executeUpdate();
                if (count == 0) {
                    throw new SQLException("Failed to insert instance app for host_id: " + event.get(Constants.HOST) +
                            ", instance_id: " + map.get("instanceId") + ", app_id: " + map.get("appId") + ", app_version: " + map.get("appVersion"));
                }
                conn.commit();
                result = Success.of(event.get(Constants.HOST) + "|" + map.get("instanceId") + "|" + map.get("appId") + "|" + map.get("appVersion"));
                insertNotification(event, true, null);


            } catch (SQLException e) {
                logger.error("SQLException:", e);
                conn.rollback();
                insertNotification(event, false, e.getMessage());
                result = Failure.of(new Status("SQL_EXCEPTION", e.getMessage()));
            } catch (Exception e) {
                logger.error("Exception:", e);
                conn.rollback();
                insertNotification(event, false, e.getMessage());
                result = Failure.of(new Status("GENERIC_EXCEPTION", e.getMessage()));
            }
        } catch (SQLException e) {
            logger.error("SQLException:", e);
            result = Failure.of(new Status("SQL_EXCEPTION", e.getMessage()));
        }
        return result;
    }


    @Override
    public Result<String> updateConfigInstanceApp(Map<String, Object> event) {
        final String sql = "UPDATE instance_app_property_t SET " +
                "property_value = ?, update_user = ?, update_ts = ? " +
                "WHERE host_id = ? AND instance_id = ? AND app_id = ? AND app_version = ? AND config_id = ? AND property_name = ?";

        Result<String> result;
        Map<String, Object> map = (Map<String, Object>)event.get(PortalConstants.DATA);
        try (Connection conn = ds.getConnection()) {
            conn.setAutoCommit(false);
            try (PreparedStatement statement = conn.prepareStatement(sql)) {
                if (map.containsKey("propertyValue")) {
                    statement.setString(1, (String)map.get("propertyValue"));
                } else {
                    statement.setNull(1, Types.VARCHAR);
                }
                statement.setString(2, (String)event.get(Constants.USER));
                statement.setObject(3, OffsetDateTime.parse((String)event.get(CloudEventV1.TIME)));
                statement.setString(4, (String)event.get(Constants.HOST));
                statement.setObject(5, UUID.fromString((String)map.get("instanceId")));
                statement.setString(6, (String)map.get("appId"));
                statement.setString(7, (String)map.get("appVersion"));
                statement.setObject(8, UUID.fromString((String)map.get("configId")));
                statement.setString(9, (String)map.get("propertyName"));

                int count = statement.executeUpdate();
                if (count == 0) {
                    throw new SQLException("Failed to update instance app.  No rows affected for host_id: " + event.get(Constants.HOST) +
                            ", instance_id: " + map.get("instanceId") + ", app_id: " + map.get("appId") + ", app_version: " + map.get("appVersion") +
                            ", config_id: " + map.get("configId") + ", property_name: " + map.get("propertyName"));
                }
                conn.commit();
                result = Success.of(event.get(Constants.HOST) + "|" + map.get("instanceId") + "|" + map.get("appId") + "|" + map.get("appVersion") + "|" +  map.get("configId") + "|" + map.get("propertyName"));
                insertNotification(event, true, null);

            } catch (SQLException e) {
                logger.error("SQLException:", e);
                conn.rollback();
                insertNotification(event, false, e.getMessage());
                result = Failure.of(new Status("SQL_EXCEPTION", e.getMessage()));
            } catch (Exception e) {
                logger.error("Exception:", e);
                conn.rollback();
                insertNotification(event, false, e.getMessage());
                result = Failure.of(new Status("GENERIC_EXCEPTION", e.getMessage()));
            }
        } catch (SQLException e) {
            logger.error("SQLException:", e);
            result = Failure.of(new Status("SQL_EXCEPTION", e.getMessage()));
        }
        return result;
    }


    @Override
    public Result<String> deleteConfigInstanceApp(Map<String, Object> event) {
        final String sql = "DELETE FROM instance_app_property_t " +
                "WHERE host_id = ? AND instance_id = ? AND app_id = ? AND app_version = ? AND config_id = ? AND property_name = ?";
        Result<String> result;
        Map<String, Object> map = (Map<String, Object>)event.get(PortalConstants.DATA);
        try (Connection conn = ds.getConnection()) {
            conn.setAutoCommit(false);
            try (PreparedStatement statement = conn.prepareStatement(sql)) {
                statement.setString(1, (String)event.get(Constants.HOST));
                statement.setObject(2, UUID.fromString((String)map.get("instanceId")));
                statement.setString(3, (String)map.get("appId"));
                statement.setString(4, (String)map.get("appVersion"));
                statement.setObject(5, UUID.fromString((String)map.get("configId")));
                statement.setString(6, (String)map.get("propertyName"));

                int count = statement.executeUpdate();
                if (count == 0) {
                    throw new SQLException("Failed to delete instance app. No rows affected for host_id: " + event.get(Constants.HOST) +
                            ", instance_id: " + map.get("instanceId") + ", app_id: " + map.get("appId") + ", app_version: " + map.get("appVersion"));
                }
                conn.commit();
                result = Success.of(event.get(Constants.HOST) + "|" + map.get("instanceId") + "|" + map.get("appId") + "|" + map.get("appVersion"));
                insertNotification(event, true, null);

            } catch (SQLException e) {
                logger.error("SQLException:", e);
                conn.rollback();
                insertNotification(event, false, e.getMessage());
                result = Failure.of(new Status("SQL_EXCEPTION", e.getMessage()));
            } catch (Exception e) {
                logger.error("Exception:", e);
                conn.rollback();
                insertNotification(event, false, e.getMessage());
                result = Failure.of(new Status("GENERIC_EXCEPTION", e.getMessage()));
            }
        } catch (SQLException e) {
            logger.error("SQLException:", e);
            result = Failure.of(new Status("SQL_EXCEPTION", e.getMessage()));
        }
        return result;
    }

    @Override
    public Result<String> getConfigInstanceApp(int offset, int limit, String hostId, String instanceId, String instanceName,
                                               String appId, String appVersion, String configId, String configName,
                                               String propertyName, String propertyValue, String propertyFile) {
        Result<String> result = null;

        StringBuilder sqlBuilder = new StringBuilder();
        sqlBuilder.append("SELECT COUNT(*) OVER () AS total,\n" +
                "ia.host_id, ia.instance_id, i.instance_name, ia.app_id, ia.app_version, ia.active, ia.update_user, ia.update_ts,\n" +
                "iap.config_id, iap.property_name, iap.property_value, iap.property_file, c.config_name\n" +
                "FROM instance_app_t ia\n" +
                "INNER JOIN instance_t i ON ia.host_id =i.host_id AND ia.instance_id = i.instance_id \n" +
                "INNER JOIN instance_app_property_t iap ON ia.host_id = iap.host_id AND ia.instance_id = iap.instance_id AND ia.app_id = iap.app_id AND ia.app_version = iap.app_version\n" +
                "INNER JOIN config_t c ON iap.config_id = c.config_id\n" +
                "WHERE 1=1\n");

        List<Object> parameters = new ArrayList<>();

        StringBuilder whereClause = new StringBuilder();
        addCondition(whereClause, parameters, "ia.host_id", hostId != null ? UUID.fromString(hostId) : null);
        addCondition(whereClause, parameters, "ia.instance_id", instanceId != null ? UUID.fromString(instanceId) : null);
        addCondition(whereClause, parameters, "i.instance_name", instanceName);
        addCondition(whereClause, parameters, "ia.app_id", appId);
        addCondition(whereClause, parameters, "ia.app_version", appVersion);
        addCondition(whereClause, parameters, "iap.config_id", configId != null ? UUID.fromString(configId) : null);
        addCondition(whereClause, parameters, "c.config_name", configName); // Filter by config_name
        addCondition(whereClause, parameters, "iap.property_name", propertyName);
        addCondition(whereClause, parameters, "iap.property_value", propertyValue);
        addCondition(whereClause, parameters, "iap.property_file", propertyFile);


        if (whereClause.length() > 0) {
            sqlBuilder.append("AND ").append(whereClause);
        }

        sqlBuilder.append(" ORDER BY ia.host_id, ia.instance_id, ia.app_id, ia.app_version, iap.config_id, iap.property_name\n" +
                "LIMIT ? OFFSET ?");


        parameters.add(limit);
        parameters.add(offset);

        String sql = sqlBuilder.toString();
        int total = 0;
        List<Map<String, Object>> instanceApps = new ArrayList<>();

        try (Connection connection = ds.getConnection();
             PreparedStatement preparedStatement = connection.prepareStatement(sql)) {

            for (int i = 0; i < parameters.size(); i++) {
                preparedStatement.setObject(i + 1, parameters.get(i));
            }

            boolean isFirstRow = true;
            try (ResultSet resultSet = preparedStatement.executeQuery()) {
                while (resultSet.next()) {
                    Map<String, Object> map = new HashMap<>();
                    if (isFirstRow) {
                        total = resultSet.getInt("total");
                        isFirstRow = false;
                    }
                    map.put("hostId", resultSet.getObject("host_id", UUID.class));
                    map.put("instanceId", resultSet.getObject("instance_id", UUID.class));
                    map.put("instanceName", resultSet.getString("instance_name"));
                    map.put("appId", resultSet.getString("app_id"));
                    map.put("appVersion", resultSet.getString("app_version"));
                    map.put("configId", resultSet.getObject("config_id", UUID.class));
                    map.put("configName", resultSet.getString("config_name")); // Get from joined table
                    map.put("propertyName", resultSet.getString("property_name"));
                    map.put("propertyValue", resultSet.getString("property_value"));
                    map.put("propertyFile", resultSet.getString("property_file"));
                    map.put("updateUser", resultSet.getString("update_user"));
                    map.put("updateTs", resultSet.getTimestamp("update_ts") != null ? resultSet.getTimestamp("update_ts").toString() : null);
                    instanceApps.add(map);
                }
            }
            Map<String, Object> resultMap = new HashMap<>();
            resultMap.put("total", total);
            resultMap.put("instanceApps", instanceApps);
            result = Success.of(JsonMapper.toJson(resultMap));

        } catch (SQLException e) {
            logger.error("SQLException:", e);
            result = Failure.of(new Status("SQL_EXCEPTION", e.getMessage()));
        } catch (Exception e) {
            logger.error("Exception:", e);
            result = Failure.of(new Status("GENERIC_EXCEPTION", e.getMessage()));
        }
        return result;
    }


    @Override
    public Result<String> createConfigInstance(Map<String, Object> event) {
        // The table is now instance_property_t, NOT instance_t
        final String sql = "INSERT INTO instance_property_t (host_id, instance_id, config_id, property_name, " +
                "property_value, property_file, update_user, update_ts) VALUES (?, ?, ?, ?, ?, ?, ?, ?)";
        Result<String> result;
        Map<String, Object> map = (Map<String, Object>)event.get(PortalConstants.DATA);
        try (Connection conn = ds.getConnection()) {
            conn.setAutoCommit(false);
            try (PreparedStatement statement = conn.prepareStatement(sql)) {
                statement.setString(1, (String)event.get(Constants.HOST));
                statement.setObject(2, UUID.fromString((String)map.get("instanceId")));
                statement.setObject(3, UUID.fromString((String)map.get("configId")));
                statement.setString(4, (String)map.get("propertyName"));

                // Handle 'property_value' (optional)
                if (map.containsKey("propertyValue")) {
                    statement.setString(5, (String) map.get("propertyValue"));
                } else {
                    statement.setNull(5, Types.VARCHAR);
                }

                // Handle 'property_file' (optional)
                if (map.containsKey("propertyFile")) {
                    statement.setString(6, (String) map.get("propertyFile"));
                } else {
                    statement.setNull(6, Types.VARCHAR);
                }

                statement.setString(7, (String)event.get(Constants.USER));
                statement.setObject(8, OffsetDateTime.parse((String)event.get(CloudEventV1.TIME)));

                int count = statement.executeUpdate();
                if (count == 0) {
                    throw new SQLException("Failed to insert instance property for host_id: " + event.get(Constants.HOST) +
                            ", instance_id: " + map.get("instanceId") + ", config_id: " + map.get("configId") +
                            ", property_name: " + map.get("propertyName"));
                }
                conn.commit();
                result = Success.of(event.get(Constants.HOST) + "|" + map.get("instanceId") + "|" + map.get("configId") + "|" + map.get("propertyName"));
                insertNotification(event, true, null);


            } catch (SQLException e) {
                logger.error("SQLException:", e);
                conn.rollback();
                insertNotification(event, false, e.getMessage());
                result = Failure.of(new Status("SQL_EXCEPTION", e.getMessage()));
            } catch (Exception e) {
                logger.error("Exception:", e);
                conn.rollback();
                insertNotification(event, false, e.getMessage());
                result = Failure.of(new Status("GENERIC_EXCEPTION", e.getMessage()));
            }
        } catch (SQLException e) {
            logger.error("SQLException:", e);
            result = Failure.of(new Status("SQL_EXCEPTION", e.getMessage()));
        }
        return result;
    }


    @Override
    public Result<String> updateConfigInstance(Map<String, Object> event) {
        final String sql = "UPDATE instance_property_t SET property_value = ?, property_file = ?, update_user = ?, update_ts = ? " +
                "WHERE host_id = ? AND instance_id = ? AND config_id = ? AND property_name = ?";
        Result<String> result;
        Map<String, Object> map = (Map<String, Object>)event.get(PortalConstants.DATA);
        try (Connection conn = ds.getConnection()) {
            conn.setAutoCommit(false);
            try (PreparedStatement statement = conn.prepareStatement(sql)) {

                // Handle 'property_value' (optional)
                if (map.containsKey("propertyValue")) {
                    statement.setString(1, (String) map.get("propertyValue"));
                } else {
                    statement.setNull(1, Types.VARCHAR); // Or keep existing
                }

                // Handle 'property_file' (optional)
                if (map.containsKey("propertyFile")) {
                    statement.setString(2, (String) map.get("propertyFile"));
                } else {
                    statement.setNull(2, Types.VARCHAR); // Or keep existing
                }

                statement.setString(3, (String)event.get(Constants.USER));
                statement.setObject(4, OffsetDateTime.parse((String)event.get(CloudEventV1.TIME)));

                // WHERE clause parameters (from the event, NOT the JSON)
                statement.setString(5, (String)event.get(Constants.HOST));
                statement.setObject(6, UUID.fromString((String)map.get("instanceId")));
                statement.setObject(7, UUID.fromString((String)map.get("configId")));
                statement.setString(8, (String)map.get("propertyName"));

                int count = statement.executeUpdate();
                if (count == 0) {
                    throw new SQLException("Failed to update instance property. No rows affected for host_id: " + event.get(Constants.HOST) +
                            ", instance_id: " + map.get("instanceId") + ", config_id: " + map.get("configId") +
                            ", property_name: " + map.get("propertyName"));
                }
                conn.commit();
                result = Success.of(event.get(Constants.HOST) + "|" + map.get("instanceId") + "|" + map.get("configId") + "|" + map.get("propertyName"));

                insertNotification(event, true, null);

            } catch (SQLException e) {
                logger.error("SQLException:", e);
                conn.rollback();
                insertNotification(event, false, e.getMessage());
                result = Failure.of(new Status("SQL_EXCEPTION", e.getMessage()));
            } catch (Exception e) {
                logger.error("Exception:", e);
                conn.rollback();
                insertNotification(event, false, e.getMessage());
                result = Failure.of(new Status("GENERIC_EXCEPTION", e.getMessage()));
            }
        } catch (SQLException e) {
            logger.error("SQLException:", e);
            result = Failure.of(new Status("SQL_EXCEPTION", e.getMessage()));
        }
        return result;
    }


    @Override
    public Result<String> deleteConfigInstance(Map<String, Object> event) {
        final String sql = "DELETE FROM instance_property_t WHERE host_id = ? AND instance_id = ? AND config_id = ? AND property_name = ?";
        Result<String> result;
        Map<String, Object> map = (Map<String, Object>)event.get(PortalConstants.DATA);
        try (Connection conn = ds.getConnection()) {
            conn.setAutoCommit(false);
            try (PreparedStatement statement = conn.prepareStatement(sql)) {
                statement.setString(1, (String)event.get(Constants.HOST));
                statement.setObject(2, UUID.fromString((String)map.get("instanceId")));
                statement.setObject(3, UUID.fromString((String)map.get("configId")));
                statement.setString(4, (String)map.get("propertyName"));

                int count = statement.executeUpdate();
                if (count == 0) {
                    throw new SQLException("Failed to delete instance property. No rows affected for host_id: " + event.get(Constants.HOST) +
                            ", instance_id: " + map.get("instanceId") + ", config_id: " + map.get("configId") +
                            ", property_name: " + map.get("propertyName"));
                }
                conn.commit();
                result = Success.of(event.get(Constants.HOST) + "|" + map.get("instanceId") + "|" + map.get("configId") + "|" + map.get("propertyName"));
                insertNotification(event, true, null);

            } catch (SQLException e) {
                logger.error("SQLException:", e);
                conn.rollback();
                insertNotification(event, false, e.getMessage());
                result = Failure.of(new Status("SQL_EXCEPTION", e.getMessage()));
            } catch (Exception e) {
                logger.error("Exception:", e);
                conn.rollback();
                insertNotification(event, false, e.getMessage());
                result = Failure.of(new Status("GENERIC_EXCEPTION", e.getMessage()));
            }
        } catch (SQLException e) {
            logger.error("SQLException:", e);
            result = Failure.of(new Status("SQL_EXCEPTION", e.getMessage()));
        }
        return result;
    }

    @Override
    public Result<String> getConfigInstance(int offset, int limit, String hostId, String instanceId,
                                            String configId, String configName,
                                            String propertyName, String propertyValue, String propertyFile) {
        Result<String> result = null;

        StringBuilder sqlBuilder = new StringBuilder();
        sqlBuilder.append("SELECT COUNT(*) OVER () AS total,\n" +
                "ip.host_id, ip.instance_id, ip.config_id, ip.property_name, ip.property_value, ip.property_file, " +
                "ip.update_user, ip.update_ts, c.config_name \n" +
                "FROM instance_property_t ip\n" +
                "LEFT JOIN config_t c ON ip.config_id = c.config_id\n" +
                "WHERE 1=1\n");

        List<Object> parameters = new ArrayList<>();

        StringBuilder whereClause = new StringBuilder();
        addCondition(whereClause, parameters, "ip.host_id", hostId != null ? UUID.fromString(hostId) : null);
        addCondition(whereClause, parameters, "ip.instance_id", instanceId != null ? UUID.fromString(instanceId) : null);
        addCondition(whereClause, parameters, "ip.config_id", configId != null ? UUID.fromString(configId) : null);
        addCondition(whereClause, parameters, "c.config_name", configName); // Filter by config_name
        addCondition(whereClause, parameters, "ip.property_name", propertyName);
        addCondition(whereClause, parameters, "ip.property_value", propertyValue);
        addCondition(whereClause, parameters, "ip.property_file", propertyFile);

        if (!whereClause.isEmpty()) {
            sqlBuilder.append("AND ").append(whereClause);
        }

        sqlBuilder.append(" ORDER BY ip.host_id, ip.instance_id, ip.config_id, ip.property_name\n" +
                "LIMIT ? OFFSET ?");

        parameters.add(limit);
        parameters.add(offset);

        String sql = sqlBuilder.toString();
        int total = 0;
        List<Map<String, Object>> instanceProperties = new ArrayList<>();

        try (Connection connection = ds.getConnection();
             PreparedStatement preparedStatement = connection.prepareStatement(sql)) {

            for (int i = 0; i < parameters.size(); i++) {
                preparedStatement.setObject(i + 1, parameters.get(i));
            }

            boolean isFirstRow = true;
            try (ResultSet resultSet = preparedStatement.executeQuery()) {
                while (resultSet.next()) {
                    Map<String, Object> map = new HashMap<>();
                    if (isFirstRow) {
                        total = resultSet.getInt("total");
                        isFirstRow = false;
                    }
                    map.put("hostId", resultSet.getObject("host_id", UUID.class));
                    map.put("instanceId", resultSet.getObject("instance_id", UUID.class));
                    map.put("configId", resultSet.getObject("config_id", UUID.class));
                    map.put("configName", resultSet.getString("config_name")); // Get from joined table
                    map.put("propertyName", resultSet.getString("property_name"));
                    map.put("propertyValue", resultSet.getString("property_value"));
                    map.put("propertyFile", resultSet.getString("property_file"));
                    map.put("updateUser", resultSet.getString("update_user"));
                    map.put("updateTs", resultSet.getTimestamp("update_ts") != null ? resultSet.getTimestamp("update_ts").toString() : null);

                    instanceProperties.add(map);
                }
            }

            Map<String, Object> resultMap = new HashMap<>();
            resultMap.put("total", total);
            resultMap.put("instanceProperties", instanceProperties);
            result = Success.of(JsonMapper.toJson(resultMap));

        } catch (SQLException e) {
            logger.error("SQLException:", e);
            result = Failure.of(new Status("SQL_EXCEPTION", e.getMessage()));
        } catch (Exception e) {
            logger.error("Exception:", e);
            result = Failure.of(new Status("GENERIC_EXCEPTION", e.getMessage()));
        }
        return result;
    }


    @Override
    public Result<String> createConfigProduct(Map<String, Object> event) {
        final String sql = "INSERT INTO product_property_t (product_id, config_id, property_name, property_value, property_file, update_user, update_ts) VALUES (?, ?, ?, ?, ?, ?, ?)";
        Result<String> result;
        Map<String, Object> map = (Map<String, Object>)event.get(PortalConstants.DATA);
        try (Connection conn = ds.getConnection()) {
            conn.setAutoCommit(false);
            try (PreparedStatement statement = conn.prepareStatement(sql)) {
                statement.setString(1, (String)map.get("productId"));
                statement.setObject(2, UUID.fromString((String)map.get("configId")));
                statement.setString(3, (String)map.get("propertyName"));

                // Handle 'property_value' (optional)
                if (map.containsKey("propertyValue")) {
                    statement.setString(4, (String) map.get("propertyValue"));
                } else {
                    statement.setNull(4, Types.VARCHAR);
                }

                // Handle 'property_file' (optional)
                if (map.containsKey("propertyFile")) {
                    statement.setString(5, (String) map.get("propertyFile"));
                } else {
                    statement.setNull(5, Types.VARCHAR);
                }

                statement.setString(6, (String)event.get(Constants.USER));
                statement.setObject(7, OffsetDateTime.parse((String)event.get(CloudEventV1.TIME)));

                int count = statement.executeUpdate();
                if (count == 0) {
                    throw new SQLException("Failed to insert product property for product_id: " + map.get("productId") +
                            ", config_id: " + map.get("configId") + ", property_name: " + map.get("propertyName"));
                }
                conn.commit();
                result = Success.of(map.get("productId") + "|" + map.get("configId") + "|" + map.get("propertyName"));
                insertNotification(event, true, null);

            } catch (SQLException e) {
                logger.error("SQLException:", e);
                conn.rollback();
                insertNotification(event, false, e.getMessage());
                result = Failure.of(new Status("SQL_EXCEPTION", e.getMessage()));
            } catch (Exception e) {
                logger.error("Exception:", e);
                conn.rollback();
                insertNotification(event, false, e.getMessage());
                result = Failure.of(new Status("GENERIC_EXCEPTION", e.getMessage()));
            }
        } catch (SQLException e) {
            logger.error("SQLException:", e);
            result = Failure.of(new Status("SQL_EXCEPTION", e.getMessage()));
        }
        return result;
    }

    @Override
    public Result<String> updateConfigProduct(Map<String, Object> event) {
        final String sql = "UPDATE product_property_t SET property_value = ?, property_file = ?, update_user = ?, update_ts = ? " +
                "WHERE product_id = ? AND config_id = ? AND property_name = ?";
        Result<String> result;
        Map<String, Object> map = (Map<String, Object>)event.get(PortalConstants.DATA);
        try (Connection conn = ds.getConnection()) {
            conn.setAutoCommit(false);
            try (PreparedStatement statement = conn.prepareStatement(sql)) {

                // Handle 'property_value' (optional)
                if (map.containsKey("propertyValue")) {
                    statement.setString(1, (String) map.get("propertyValue"));
                } else {
                    statement.setNull(1, Types.VARCHAR); // Or keep existing
                }

                // Handle 'property_file' (optional)
                if (map.containsKey("propertyFile")) {
                    statement.setString(2, (String) map.get("propertyFile"));
                } else {
                    statement.setNull(2, Types.VARCHAR); // Or keep existing
                }

                statement.setString(3, (String)event.get(Constants.USER));
                statement.setObject(4, OffsetDateTime.parse((String)event.get(CloudEventV1.TIME)));

                // WHERE clause parameters (from the event, NOT the JSON)
                statement.setString(5, (String)map.get("productId"));
                statement.setObject(6, UUID.fromString((String)map.get("configId")));
                statement.setString(7, (String)map.get("propertyName"));

                int count = statement.executeUpdate();
                if (count == 0) {
                    throw new SQLException("Failed to update product property. No rows affected for product_id: " + map.get("productId") +
                            ", config_id: " + map.get("configId") + ", property_name: " + map.get("propertyName"));
                }
                conn.commit();
                result = Success.of(map.get("productId") + "|" + map.get("configId") + "|" + map.get("propertyName"));
                insertNotification(event, true, null);

            } catch (SQLException e) {
                logger.error("SQLException:", e);
                conn.rollback();
                insertNotification(event, false, e.getMessage());
                result = Failure.of(new Status("SQL_EXCEPTION", e.getMessage()));
            } catch (Exception e) {
                logger.error("Exception:", e);
                conn.rollback();
                insertNotification(event, false, e.getMessage());
                result = Failure.of(new Status("GENERIC_EXCEPTION", e.getMessage()));
            }
        } catch (SQLException e) {
            logger.error("SQLException:", e);
            result = Failure.of(new Status("SQL_EXCEPTION", e.getMessage()));
        }
        return result;
    }

    @Override
    public Result<String> deleteConfigProduct(Map<String, Object> event) {
        final String sql = "DELETE FROM product_property_t WHERE product_id = ? AND config_id = ? AND property_name = ?";
        Result<String> result;
        Map<String, Object> map = (Map<String, Object>)event.get(PortalConstants.DATA);
        try (Connection conn = ds.getConnection()) {
            conn.setAutoCommit(false);
            try (PreparedStatement statement = conn.prepareStatement(sql)) {
                statement.setString(1, (String)map.get("productId"));
                statement.setObject(2, UUID.fromString((String)map.get("configId")));
                statement.setString(3, (String)map.get("propertyName"));

                int count = statement.executeUpdate();
                if (count == 0) {
                    throw new SQLException("Failed to delete product property. No rows affected for product_id: " + map.get("productId") +
                            ", config_id: " + map.get("configId") + ", property_name: " + map.get("propertyName"));
                }
                conn.commit();
                result = Success.of(map.get("productId") + "|" + map.get("configId") + "|" + map.get("propertyName"));
                insertNotification(event, true, null);

            } catch (SQLException e) {
                logger.error("SQLException:", e);
                conn.rollback();
                insertNotification(event, false, e.getMessage());
                result = Failure.of(new Status("SQL_EXCEPTION", e.getMessage()));
            } catch (Exception e) {
                logger.error("Exception:", e);
                conn.rollback();
                insertNotification(event, false, e.getMessage());
                result = Failure.of(new Status("GENERIC_EXCEPTION", e.getMessage()));
            }
        } catch (SQLException e) {
            logger.error("SQLException:", e);
            result = Failure.of(new Status("SQL_EXCEPTION", e.getMessage()));
        }
        return result;
    }

    @Override
    public Result<String> getConfigProduct(int offset, int limit, String productId,
                                           String configId, String configName,
                                           String propertyName, String propertyValue, String propertyFile) {
        Result<String> result = null;

        StringBuilder sqlBuilder = new StringBuilder();
        sqlBuilder.append("SELECT COUNT(*) OVER () AS total,\n" +
                "pp.product_id, pp.config_id, pp.property_name, pp.property_value, pp.property_file, pp.update_user, pp.update_ts, \n" +
                "c.config_name \n" + // Include config_name from config_t
                "FROM product_property_t pp\n" +
                "LEFT JOIN config_t c ON pp.config_id = c.config_id\n" + // Left join with config_t
                "WHERE 1=1\n");

        List<Object> parameters = new ArrayList<>();

        StringBuilder whereClause = new StringBuilder();
        addCondition(whereClause, parameters, "pp.product_id", productId);
        addCondition(whereClause, parameters, "pp.config_id", configId != null ? UUID.fromString(configId) : null);
        addCondition(whereClause, parameters, "c.config_name", configName); // Filter by config_name
        addCondition(whereClause, parameters, "pp.property_name", propertyName);
        addCondition(whereClause, parameters, "pp.property_value", propertyValue);
        addCondition(whereClause, parameters, "pp.property_file", propertyFile);

        if (!whereClause.isEmpty()) {
            sqlBuilder.append("AND ").append(whereClause);
        }

        sqlBuilder.append(" ORDER BY pp.product_id, pp.config_id, pp.property_name\n" +
                "LIMIT ? OFFSET ?");

        parameters.add(limit);
        parameters.add(offset);

        String sql = sqlBuilder.toString();
        int total = 0;
        List<Map<String, Object>> productProperties = new ArrayList<>();

        try (Connection connection = ds.getConnection();
             PreparedStatement preparedStatement = connection.prepareStatement(sql)) {

            for (int i = 0; i < parameters.size(); i++) {
                preparedStatement.setObject(i + 1, parameters.get(i));
            }

            boolean isFirstRow = true;
            try (ResultSet resultSet = preparedStatement.executeQuery()) {
                while (resultSet.next()) {
                    Map<String, Object> map = new HashMap<>();
                    if (isFirstRow) {
                        total = resultSet.getInt("total");
                        isFirstRow = false;
                    }
                    map.put("productId", resultSet.getString("product_id"));
                    map.put("configId", resultSet.getObject("config_id", UUID.class));
                    map.put("configName", resultSet.getString("config_name"));
                    map.put("propertyName", resultSet.getString("property_name"));
                    map.put("propertyValue", resultSet.getString("property_value"));
                    map.put("propertyFile", resultSet.getString("property_file"));
                    map.put("updateUser", resultSet.getString("update_user"));
                    map.put("updateTs", resultSet.getTimestamp("update_ts") != null ? resultSet.getTimestamp("update_ts").toString() : null);

                    productProperties.add(map);
                }
            }

            Map<String, Object> resultMap = new HashMap<>();
            resultMap.put("total", total);
            resultMap.put("productProperties", productProperties);
            result = Success.of(JsonMapper.toJson(resultMap));

        } catch (SQLException e) {
            logger.error("SQLException:", e);
            result = Failure.of(new Status("SQL_EXCEPTION", e.getMessage()));
        } catch (Exception e) {
            logger.error("Exception:", e);
            result = Failure.of(new Status("GENERIC_EXCEPTION", e.getMessage()));
        }
        return result;
    }

    @Override
    public Result<String> createConfigProductVersion(Map<String, Object> event) {
        final String sql = "INSERT INTO product_version_property_t (host_id, product_id, product_version, " +
                "config_id, property_name, property_value, property_file, update_user, update_ts) " +
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)";
        Result<String> result;
        Map<String, Object> map = (Map<String, Object>)event.get(PortalConstants.DATA);
        try (Connection conn = ds.getConnection()) {
            conn.setAutoCommit(false);
            try (PreparedStatement statement = conn.prepareStatement(sql)) {
                statement.setString(1, (String)event.get(Constants.HOST));
                statement.setString(2, (String)map.get("productId"));
                statement.setString(3, (String)map.get("productVersion"));
                statement.setObject(4, UUID.fromString((String)map.get("configId")));
                statement.setString(5, (String)map.get("propertyName"));

                // Handle 'property_value' (optional)
                if (map.containsKey("propertyValue")) {
                    statement.setString(6, (String) map.get("propertyValue"));
                } else {
                    statement.setNull(6, Types.VARCHAR);
                }

                // Handle 'property_file' (optional)
                if (map.containsKey("propertyFile")) {
                    statement.setString(7, (String) map.get("propertyFile"));
                } else {
                    statement.setNull(7, Types.VARCHAR);
                }

                statement.setString(8, (String)event.get(Constants.USER));
                statement.setObject(9, OffsetDateTime.parse((String)event.get(CloudEventV1.TIME)));

                int count = statement.executeUpdate();
                if (count == 0) {
                    throw new SQLException("Failed to insert product version property for host_id: " + event.get(Constants.HOST) +
                            ", product_id: " + map.get("productId") + ", product_version: " + map.get("productVersion") +
                            ", config_id: " + map.get("configId") + ", property_name: " + map.get("propertyName"));
                }
                conn.commit();
                result = Success.of(event.get(Constants.HOST) + "|" + map.get("productId") + "|" + map.get("productVersion") + "|" + map.get("configId") + "|" + map.get("propertyName")); // Composite key
                insertNotification(event, true, null);

            } catch (SQLException e) {
                logger.error("SQLException:", e);
                conn.rollback();
                insertNotification(event, false, e.getMessage());
                result = Failure.of(new Status("SQL_EXCEPTION", e.getMessage()));
            } catch (Exception e) {
                logger.error("Exception:", e);
                conn.rollback();
                insertNotification(event, false, e.getMessage());
                result = Failure.of(new Status("GENERIC_EXCEPTION", e.getMessage()));
            }
        } catch (SQLException e) {
            logger.error("SQLException:", e);
            result = Failure.of(new Status("SQL_EXCEPTION", e.getMessage()));
        }
        return result;
    }

    @Override
    public Result<String> updateConfigProductVersion(Map<String, Object> event) {
        final String sql = "UPDATE product_version_property_t SET property_value = ?, property_file = ?, update_user = ?, update_ts = ? " +
                "WHERE host_id = ? AND product_id = ? AND product_version = ? AND config_id = ? AND property_name = ?";
        Result<String> result;
        Map<String, Object> map = (Map<String, Object>)event.get(PortalConstants.DATA);
        try (Connection conn = ds.getConnection()) {
            conn.setAutoCommit(false);
            try (PreparedStatement statement = conn.prepareStatement(sql)) {

                // Handle 'property_value' (optional)
                if (map.containsKey("propertyValue")) {
                    statement.setString(1, (String) map.get("propertyValue"));
                } else {
                    statement.setNull(1, Types.VARCHAR); // Or keep existing
                }

                // Handle 'property_file' (optional)
                if (map.containsKey("propertyFile")) {
                    statement.setString(2, (String) map.get("propertyFile"));
                } else {
                    statement.setNull(2, Types.VARCHAR); // Or keep existing
                }

                statement.setString(3, (String)event.get(Constants.USER));
                statement.setObject(4, OffsetDateTime.parse((String)event.get(CloudEventV1.TIME)));

                // WHERE clause parameters (from the event, NOT the JSON)
                statement.setString(5, (String)event.get(Constants.HOST));
                statement.setString(6, (String)map.get("productId"));
                statement.setString(7, (String)map.get("productVersion"));
                statement.setObject(8, UUID.fromString((String)map.get("configId")));
                statement.setString(9, (String)map.get("propertyName"));

                int count = statement.executeUpdate();
                if (count == 0) {
                    throw new SQLException("Failed to update product version property. No rows affected for host_id: " + event.get(Constants.HOST) +
                            ", product_id: " + map.get("productId") + ", product_version: " + map.get("productVersion") +
                            ", config_id: " + map.get("configId") + ", property_name: " + map.get("propertyName"));
                }
                conn.commit();
                result = Success.of(event.get(Constants.HOST) + "|" + map.get("productId") + "|" + map.get("productVersion") + "|" + map.get("configId") + "|" + map.get("propertyName"));
                insertNotification(event, true, null);

            } catch (SQLException e) {
                logger.error("SQLException:", e);
                conn.rollback();
                insertNotification(event, false, e.getMessage());
                result = Failure.of(new Status("SQL_EXCEPTION", e.getMessage()));
            } catch (Exception e) {
                logger.error("Exception:", e);
                conn.rollback();
                insertNotification(event, false, e.getMessage());
                result = Failure.of(new Status("GENERIC_EXCEPTION", e.getMessage()));
            }
        } catch (SQLException e) {
            logger.error("SQLException:", e);
            result = Failure.of(new Status("SQL_EXCEPTION", e.getMessage()));
        }
        return result;
    }


    @Override
    public Result<String> deleteConfigProductVersion(Map<String, Object> event) {
        final String sql = "DELETE FROM product_version_property_t WHERE host_id = ? AND product_id = ? " +
                "AND product_version = ? AND config_id = ? AND property_name = ?";
        Result<String> result;
        Map<String, Object> map = (Map<String, Object>)event.get(PortalConstants.DATA);
        try (Connection conn = ds.getConnection()) {
            conn.setAutoCommit(false);
            try (PreparedStatement statement = conn.prepareStatement(sql)) {
                statement.setString(1, (String)event.get(Constants.HOST));
                statement.setString(2, (String)map.get("productId"));
                statement.setString(3, (String)map.get("productVersion"));
                statement.setObject(4, UUID.fromString((String)map.get("configId")));
                statement.setString(5, (String)map.get("propertyName"));

                int count = statement.executeUpdate();
                if (count == 0) {
                    throw new SQLException("Failed to delete product version property. No rows affected for host_id: " + event.get(Constants.HOST) +
                            ", product_id: " + map.get("productId") + ", product_version: " + map.get("productVersion") +
                            ", config_id: " + map.get("configId") + ", property_name: " + map.get("propertyName"));
                }
                conn.commit();
                result = Success.of(event.get(Constants.HOST) + "|" + map.get("productId") + "|" + map.get("productVersion") + "|" + map.get("configId") + "|" + map.get("propertyName"));
                insertNotification(event, true, null);
            } catch (SQLException e) {
                logger.error("SQLException:", e);
                conn.rollback();
                insertNotification(event, false, e.getMessage());
                result = Failure.of(new Status("SQL_EXCEPTION", e.getMessage()));
            } catch (Exception e) {
                logger.error("Exception:", e);
                conn.rollback();
                insertNotification(event, false, e.getMessage());
                result = Failure.of(new Status("GENERIC_EXCEPTION", e.getMessage()));
            }
        } catch (SQLException e) {
            logger.error("SQLException:", e);
            result = Failure.of(new Status("SQL_EXCEPTION", e.getMessage()));
        }
        return result;
    }


    @Override
    public Result<String> getConfigProductVersion(int offset, int limit, String hostId, String productId, String productVersion,
                                                  String configId, String configName,
                                                  String propertyName, String propertyValue, String propertyFile) {
        Result<String> result = null;

        StringBuilder sqlBuilder = new StringBuilder();
        sqlBuilder.append("SELECT COUNT(*) OVER () AS total,\n" +
                "pvp.host_id, pvp.product_id, pvp.product_version, pvp.config_id, pvp.property_name, " +
                "pvp.property_value, pvp.property_file, pvp.update_user, pvp.update_ts, \n" +
                "c.config_name \n" + // Include config_name from config_t
                "FROM product_version_property_t pvp\n" +
                "LEFT JOIN config_t c ON pvp.config_id = c.config_id\n" +  // Left join with config_t
                "WHERE 1=1\n");

        List<Object> parameters = new ArrayList<>();

        StringBuilder whereClause = new StringBuilder();
        addCondition(whereClause, parameters, "pvp.host_id", hostId != null ? UUID.fromString(hostId) : null);
        addCondition(whereClause, parameters, "pvp.product_id", productId);
        addCondition(whereClause, parameters, "pvp.product_version", productVersion);
        addCondition(whereClause, parameters, "pvp.config_id", configId != null ? UUID.fromString(configId) : null);
        addCondition(whereClause, parameters, "c.config_name", configName);  // Filter by config_name
        addCondition(whereClause, parameters, "pvp.property_name", propertyName);
        addCondition(whereClause, parameters, "pvp.property_value", propertyValue);
        addCondition(whereClause, parameters, "pvp.property_file", propertyFile);


        if (!whereClause.isEmpty()) {
            sqlBuilder.append("AND ").append(whereClause);
        }

        sqlBuilder.append(" ORDER BY pvp.host_id, pvp.product_id, pvp.product_version, pvp.config_id, pvp.property_name\n" +
                "LIMIT ? OFFSET ?");

        parameters.add(limit);
        parameters.add(offset);

        String sql = sqlBuilder.toString();
        int total = 0;
        List<Map<String, Object>> productVersionProperties = new ArrayList<>();

        try (Connection connection = ds.getConnection();
             PreparedStatement preparedStatement = connection.prepareStatement(sql)) {

            for (int i = 0; i < parameters.size(); i++) {
                preparedStatement.setObject(i + 1, parameters.get(i));
            }

            boolean isFirstRow = true;
            try (ResultSet resultSet = preparedStatement.executeQuery()) {
                while (resultSet.next()) {
                    Map<String, Object> map = new HashMap<>();
                    if (isFirstRow) {
                        total = resultSet.getInt("total");
                        isFirstRow = false;
                    }
                    map.put("hostId", resultSet.getObject("host_id", UUID.class));
                    map.put("productId", resultSet.getString("product_id"));
                    map.put("productVersion", resultSet.getString("product_version"));
                    map.put("configId", resultSet.getObject("config_id", UUID.class));
                    map.put("configName", resultSet.getString("config_name")); // Get from joined table
                    map.put("propertyName", resultSet.getString("property_name"));
                    map.put("propertyValue", resultSet.getString("property_value"));
                    map.put("propertyFile", resultSet.getString("property_file"));
                    map.put("updateUser", resultSet.getString("update_user"));
                    map.put("updateTs", resultSet.getTimestamp("update_ts") != null ? resultSet.getTimestamp("update_ts").toString() : null);

                    productVersionProperties.add(map);
                }
            }

            Map<String, Object> resultMap = new HashMap<>();
            resultMap.put("total", total);
            resultMap.put("productVersionProperties", productVersionProperties);
            result = Success.of(JsonMapper.toJson(resultMap));

        } catch (SQLException e) {
            logger.error("SQLException:", e);
            result = Failure.of(new Status("SQL_EXCEPTION", e.getMessage()));
        } catch (Exception e) {
            logger.error("Exception:", e);
            result = Failure.of(new Status("GENERIC_EXCEPTION", e.getMessage()));
        }
        return result;
    }

    @Override
    public Result<Map<String, Object>> queryCurrentProviderKey(String providerId) {
        final String queryConfigById = "SELECT provider_id, kid, public_key, " +
                "private_key, key_type, update_user, update_ts " +
                "FROM auth_provider_key_t WHERE provider_id = ? AND key_type = 'TC'";
        Result<Map<String, Object>> result;
        try (final Connection conn = ds.getConnection()) {
            Map<String, Object> map = new HashMap<>();
            try (PreparedStatement statement = conn.prepareStatement(queryConfigById)) {
                statement.setString(1, providerId);
                try (ResultSet resultSet = statement.executeQuery()) {
                    if (resultSet.next()) {
                        map.put("providerId", resultSet.getString("provider_id"));
                        map.put("kid", resultSet.getString("kid"));
                        map.put("publicKey", resultSet.getString("public_key"));
                        map.put("privateKey", resultSet.getString("private_key"));
                        map.put("keyType", resultSet.getString("key_type"));
                        map.put("updateUser", resultSet.getString("update_user"));
                        map.put("updateTs", resultSet.getTimestamp("update_ts"));
                    }
                }
            }
            if (map.isEmpty())
                result = Failure.of(new Status(OBJECT_NOT_FOUND, "provider key with id", providerId));
            else
                result = Success.of(map);
        } catch (SQLException e) {
            logger.error("SQLException:", e);
            result = Failure.of(new Status(SQL_EXCEPTION, e.getMessage()));
        } catch (Exception e) {
            logger.error("Exception:", e);
            result = Failure.of(new Status(GENERIC_EXCEPTION, e.getMessage()));
        }
        return result;
    }

    @Override
    public Result<Map<String, Object>> queryLongLiveProviderKey(String providerId) {
        final String queryConfigById = "SELECT provider_id, kid, public_key, " +
                "private_key, key_type, update_user, update_ts " +
                "FROM auth_provider_key_t WHERE provider_id = ? AND key_type = 'LC'";
        Result<Map<String, Object>> result;
        try (final Connection conn = ds.getConnection()) {
            Map<String, Object> map = new HashMap<>();
            try (PreparedStatement statement = conn.prepareStatement(queryConfigById)) {
                statement.setString(1, providerId);
                try (ResultSet resultSet = statement.executeQuery()) {
                    if (resultSet.next()) {
                        map.put("providerId", resultSet.getString("provider_id"));
                        map.put("kid", resultSet.getString("kid"));
                        map.put("publicKey", resultSet.getString("public_key"));
                        map.put("privateKey", resultSet.getString("private_key"));
                        map.put("keyType", resultSet.getString("key_type"));
                        map.put("updateUser", resultSet.getString("update_user"));
                        map.put("updateTs", resultSet.getObject("update_ts") != null ? resultSet.getObject("update_ts", OffsetDateTime.class) : null);
                    }
                }
            }
            if (map.isEmpty())
                result = Failure.of(new Status(OBJECT_NOT_FOUND, "provider key with id", providerId));
            else
                result = Success.of(map);
        } catch (SQLException e) {
            logger.error("SQLException:", e);
            result = Failure.of(new Status(SQL_EXCEPTION, e.getMessage()));
        } catch (Exception e) {
            logger.error("Exception:", e);
            result = Failure.of(new Status(GENERIC_EXCEPTION, e.getMessage()));
        }
        return result;
    }

    @Override
    public Result<String> createRule(Map<String, Object> event) {
        final String insertRule = "INSERT INTO rule_t (rule_id, rule_name, rule_version, rule_type, rule_group, " +
                "rule_desc, rule_body, rule_owner, common, update_user, update_ts) " +
                "VALUES (?, ?, ?, ?, ?,   ?, ?, ?, ?, ?,  ?)";
        final String insertHostRule = "INSERT INTO rule_host_t (host_id, rule_id, update_user, update_ts) " +
                "VALUES (?, ?, ?, ?)";

        Result<String> result = null;
        Map<String, Object> map = (Map<String, Object>)event.get(PortalConstants.DATA);
        try (Connection conn = ds.getConnection()) {
            conn.setAutoCommit(false);
            // no duplicate record, insert the user into database and write a success notification.
            try {
                try (PreparedStatement statement = conn.prepareStatement(insertRule)) {
                    statement.setString(1, (String)map.get("ruleId"));
                    statement.setString(2, (String)map.get("ruleName"));
                    statement.setString(3, (String)map.get("ruleVersion"));
                    statement.setString(4, (String)map.get("ruleType"));
                    String ruleGroup = (String)map.get("ruleGroup");
                    if (ruleGroup != null && !ruleGroup.isEmpty())
                        statement.setString(5, ruleGroup);
                    else
                        statement.setNull(5, NULL);
                    String ruleDesc = (String)map.get("ruleDesc");
                    if (ruleDesc != null && !ruleDesc.isEmpty())
                        statement.setString(6, ruleDesc);
                    else
                        statement.setNull(6, NULL);
                    statement.setString(7, (String)map.get("ruleBody"));
                    statement.setString(8, (String)map.get("ruleOwner"));
                    statement.setString(9, (String)map.get("common"));
                    statement.setString(10, (String)event.get(Constants.USER));
                    statement.setObject(11, OffsetDateTime.parse((String)event.get(CloudEventV1.TIME)));
                    int count = statement.executeUpdate();
                    if (count == 0) {
                        throw new SQLException("failed to insert the rule " + map.get("ruleId"));
                    }
                }
                try (PreparedStatement statement = conn.prepareStatement(insertHostRule)) {
                    statement.setString(1, (String)event.get(Constants.HOST));
                    statement.setString(2, (String)map.get("ruleId"));
                    statement.setString(3, (String)event.get(Constants.USER));
                    statement.setObject(4, OffsetDateTime.parse((String)event.get(CloudEventV1.TIME)));
                    int count = statement.executeUpdate();
                    if (count == 0) {
                        throw new SQLException("failed to insert the host_rule for host " + event.get(Constants.HOST) + " rule " + map.get("ruleId"));
                    }
                }
                conn.commit();
                result = Success.of((String)map.get("ruleId"));
                insertNotification(event, true, null);
            } catch (SQLException e) {
                logger.error("SQLException:", e);
                conn.rollback();
                insertNotification(event, false, e.getMessage());
                result = Failure.of(new Status(SQL_EXCEPTION, e.getMessage()));
            } catch (Exception e) {
                logger.error("Exception:", e);
                conn.rollback();
                insertNotification(event, false, e.getMessage());
                result = Failure.of(new Status(GENERIC_EXCEPTION, e.getMessage()));
            }
        } catch (SQLException e) {
            logger.error("SQLException:", e);
            result = Failure.of(new Status(SQL_EXCEPTION, e.getMessage()));
        }
        return result;
    }

    @Override
    public Result<String> updateRule(Map<String, Object> event) {
        final String updateRule = "UPDATE rule_t SET rule_name = ?, rule_version = ?, rule_type = ?, rule_group = ?, rule_desc = ?, " +
                "rule_body = ?, rule_owner = ?, common = ?, update_user = ?, update_ts = ? " +
                "WHERE rule_id = ?";

        Result<String> result = null;
        Map<String, Object> map = (Map<String, Object>)event.get(PortalConstants.DATA);
        try (Connection conn = ds.getConnection()) {
            conn.setAutoCommit(false);

            try (PreparedStatement statement = conn.prepareStatement(updateRule)) {
                String ruleName = (String)map.get("ruleName");
                if(ruleName != null && !ruleName.isEmpty()) {
                    statement.setString(1, ruleName);
                } else {
                    statement.setNull(1, NULL);
                }
                String ruleVersion = (String)map.get("ruleVersion");
                if (ruleVersion != null && !ruleVersion.isEmpty()) {
                    statement.setString(2, ruleVersion);
                } else {
                    statement.setNull(2, NULL);
                }
                String ruleType = (String)map.get("ruleType");
                if (ruleType != null && !ruleType.isEmpty()) {
                    statement.setString(3, ruleType);
                } else {
                    statement.setNull(3, NULL);
                }
                String ruleGroup = (String)map.get("ruleGroup");
                if (ruleGroup != null && !ruleGroup.isEmpty()) {
                    statement.setString(4, ruleGroup);
                } else {
                    statement.setNull(4, NULL);
                }
                String ruleDesc = (String)map.get("ruleDesc");
                if (ruleDesc != null && !ruleDesc.isEmpty()) {
                    statement.setString(5, ruleDesc);
                } else {
                    statement.setNull(5, NULL);
                }
                String ruleBody = (String)map.get("ruleBody");
                if(ruleBody != null && !ruleBody.isEmpty()) {
                    statement.setString(6, ruleBody);
                } else {
                    statement.setNull(6, NULL);
                }
                String ruleOwner = (String)map.get("ruleOwner");
                if(ruleOwner != null && !ruleOwner.isEmpty()) {
                    statement.setString(7, ruleOwner);
                } else {
                    statement.setNull(7, NULL);
                }
                String common = (String)map.get("common");
                if(common != null && !common.isEmpty()) {
                    statement.setString(8, common);
                } else {
                    statement.setNull(8, NULL);
                }
                statement.setString(9, (String)event.get(Constants.USER));
                statement.setObject(10, OffsetDateTime.parse((String)event.get(CloudEventV1.TIME)));
                statement.setString(11, (String)map.get("ruleId"));

                int count = statement.executeUpdate();
                if (count == 0) {
                    throw new SQLException("no record is updated for rule " + map.get("ruleId"));
                }
                conn.commit();
                result = Success.of((String)map.get("ruleId"));
                insertNotification(event, true, null);
            } catch (SQLException e) {
                logger.error("SQLException:", e);
                conn.rollback();
                insertNotification(event, false, e.getMessage());
                result = Failure.of(new Status(SQL_EXCEPTION, e.getMessage()));
            } catch (Exception e) {
                logger.error("Exception:", e);
                conn.rollback();
                insertNotification(event, false, e.getMessage());
                result = Failure.of(new Status(GENERIC_EXCEPTION, e.getMessage()));
            }
        } catch (SQLException e) {
            logger.error("SQLException:", e);
            result = Failure.of(new Status(SQL_EXCEPTION, e.getMessage()));
        }
        return result;

    }

    @Override
    public Result<String> deleteRule(Map<String, Object> event) {
        final String deleteRule = "DELETE from rule_t WHERE rule_id = ?";
        final String deleteHostRule = "DELETE from rule_host_t WHERE host_id = ? AND rule_id = ?";
        Result<String> result;
        Map<String, Object> map = (Map<String, Object>)event.get(PortalConstants.DATA);
        try (Connection conn = ds.getConnection()) {
            conn.setAutoCommit(false);
            try {
                try (PreparedStatement statement = conn.prepareStatement(deleteRule)) {
                    statement.setString(1, (String)map.get("ruleId"));
                    int count = statement.executeUpdate();
                    if (count == 0) {
                        throw new SQLException("no record is deleted for rule " + map.get("ruleId"));
                    }
                }
                try (PreparedStatement statement = conn.prepareStatement(deleteHostRule)) {
                    statement.setString(1, (String)event.get(Constants.HOST));
                    statement.setString(2, (String)map.get("ruleId"));
                    int count = statement.executeUpdate();
                    if (count == 0) {
                        throw new SQLException("no record is deleted for host " + (String)event.get(Constants.HOST) + " rule " + map.get("ruleId"));
                    }
                }
                conn.commit();
                result = Success.of((String)event.get(Constants.USER));
                insertNotification(event, true, null);
            } catch (SQLException e) {
                logger.error("SQLException:", e);
                conn.rollback();
                insertNotification(event, false, e.getMessage());
                result = Failure.of(new Status(SQL_EXCEPTION, e.getMessage()));
            } catch (Exception e) {
                logger.error("Exception:", e);
                conn.rollback();
                insertNotification(event, false, e.getMessage());
                result = Failure.of(new Status(GENERIC_EXCEPTION, e.getMessage()));
            }
        } catch (SQLException e) {
            logger.error("SQLException:", e);
            result = Failure.of(new Status(SQL_EXCEPTION, e.getMessage()));
        }
        return result;
    }


    @Override
    public Result<List<Map<String, Object>>> queryRuleByHostGroup(String hostId, String groupId) {
        Result<List<Map<String, Object>>> result;
        String sql = "SELECT rule_id, host_id, rule_type, rule_group, rule_visibility, rule_description, rule_body, rule_owner " +
                "update_user, update_ts " +
                "FROM rule_t WHERE host_id = ? AND rule_group = ?";
        try (final Connection conn = ds.getConnection()) {
            List<Map<String, Object>> list = new ArrayList<>();
            try (PreparedStatement statement = conn.prepareStatement(sql)) {
                statement.setObject(1, UUID.fromString(hostId));
                statement.setString(2, groupId);
                try (ResultSet resultSet = statement.executeQuery()) {
                    while (resultSet.next()) {
                        Map<String, Object> map = new HashMap<>();
                        map.put("ruleId", resultSet.getString("rule_id"));
                        map.put("hostId", resultSet.getObject("host_id", UUID.class));
                        map.put("ruleType", resultSet.getString("rule_type"));
                        map.put("ruleGroup", resultSet.getBoolean("rule_group"));
                        map.put("ruleVisibility", resultSet.getString("rule_visibility"));
                        map.put("ruleDescription", resultSet.getString("rule_description"));
                        map.put("ruleBody", resultSet.getString("rule_body"));
                        map.put("ruleOwner", resultSet.getString("rule_owner"));
                        map.put("updateUser", resultSet.getString("update_user"));
                        map.put("updateTs", resultSet.getObject("update_ts") != null ? resultSet.getObject("update_ts", OffsetDateTime.class) : null);
                        list.add(map);
                    }
                }
            }
            if (list.isEmpty())
                result = Failure.of(new Status(OBJECT_NOT_FOUND, "rule with rule group ", groupId));
            else
                result = Success.of(list);
        } catch (SQLException e) {
            logger.error("SQLException:", e);
            result = Failure.of(new Status(SQL_EXCEPTION, e.getMessage()));
        } catch (Exception e) {
            logger.error("Exception:", e);
            result = Failure.of(new Status(GENERIC_EXCEPTION, e.getMessage()));
        }
        return result;
    }

    @Override
    public Result<String> queryRule(int offset, int limit, String hostId, String ruleId, String ruleName,
                                    String ruleVersion, String ruleType, String ruleGroup, String ruleDesc,
                                    String ruleBody, String ruleOwner, String common) {
        Result<String> result;
        String sql;
        List<Object> parameters = new ArrayList<>();
        if(common == null || common.equalsIgnoreCase("N")) {
            StringBuilder sqlBuilder = new StringBuilder();
            sqlBuilder.append("SELECT COUNT(*) OVER () AS total, h.host_id, r.rule_id, r.rule_name, r.rule_version, " +
                    "r.rule_type, r.rule_group, r.common, r.rule_desc, r.rule_body, r.rule_owner, " +
                    "r.update_user, r.update_ts " +
                    "FROM rule_t r, rule_host_t h " +
                    "WHERE r.rule_id = h.rule_id " +
                    "AND h.host_id = ?\n");
            parameters.add(UUID.fromString(hostId));

            StringBuilder whereClause = new StringBuilder();

            addCondition(whereClause, parameters, "r.rule_id", ruleId);
            addCondition(whereClause, parameters, "r.rule_name", ruleName);
            addCondition(whereClause, parameters, "r.rule_version", ruleVersion);
            addCondition(whereClause, parameters, "r.rule_type", ruleType);
            addCondition(whereClause, parameters, "r.rule_group", ruleGroup);
            addCondition(whereClause, parameters, "r.rule_desc", ruleDesc);
            addCondition(whereClause, parameters, "r.rule_body", ruleBody);
            addCondition(whereClause, parameters, "r.rule_owner", ruleOwner);

            if (!whereClause.isEmpty()) {
                sqlBuilder.append("AND ").append(whereClause);
            }
            sqlBuilder.append(" ORDER BY rule_id\n" +
                    "LIMIT ? OFFSET ?");

            parameters.add(limit);
            parameters.add(offset);
            sql = sqlBuilder.toString();
        } else {
            StringBuilder sqlBuilder = new StringBuilder();
            sqlBuilder.append("SELECT \n" +
                    "                        COUNT(*) OVER () AS total,\n" +
                    "                        host_id,\n" +
                    "                        rule_id,\n" +
                    "                        rule_name,\n" +
                    "                        rule_version,\n" +
                    "                        rule_type,\n" +
                    "                        rule_group,\n" +
                    "                        common,\n" +
                    "                        rule_desc,\n" +
                    "                        rule_body,\n" +
                    "                        rule_owner,\n" +
                    "                        update_user,\n" +
                    "                        update_ts\n" +
                    "                    FROM (\n" +
                    "                       SELECT \n" +
                    "                        h.host_id,\n" +
                    "                        r.rule_id,\n" +
                    "                        r.rule_name,\n" +
                    "                        r.rule_version,\n" +
                    "                        r.rule_type,\n" +
                    "                        r.rule_group,\n" +
                    "                        r.common,\n" +
                    "                        r.rule_desc,\n" +
                    "                        r.rule_body,\n" +
                    "                        r.rule_owner,\n" +
                    "                        r.update_user,\n" +
                    "                        r.update_ts\n" +
                    "                    FROM rule_t r\n" +
                    "                    JOIN rule_host_t h ON r.rule_id = h.rule_id\n" +
                    "                    WHERE h.host_id = ?\n");
            parameters.add(UUID.fromString(hostId));
            StringBuilder whereClause = new StringBuilder();

            addCondition(whereClause, parameters, "r.rule_id", ruleId);
            addCondition(whereClause, parameters, "r.rule_name", ruleName);
            addCondition(whereClause, parameters, "r.rule_version", ruleVersion);
            addCondition(whereClause, parameters, "r.rule_type", ruleType);
            addCondition(whereClause, parameters, "r.rule_group", ruleGroup);
            addCondition(whereClause, parameters, "r.rule_desc", ruleDesc);
            addCondition(whereClause, parameters, "r.rule_body", ruleBody);
            addCondition(whereClause, parameters, "r.rule_owner", ruleOwner);
            if (!whereClause.isEmpty()) {
                sqlBuilder.append("AND ").append(whereClause);
            }

            sqlBuilder.append("                    \n" +
                    "                    UNION ALL\n" +
                    "                    \n" +
                    "                   SELECT\n" +
                    "                        h.host_id,\n" +
                    "                        r.rule_id,\n" +
                    "                        r.rule_name,\n" +
                    "                        r.rule_version,\n" +
                    "                        r.rule_type,\n" +
                    "                        r.rule_group,\n" +
                    "                        r.common,\n" +
                    "                        r.rule_desc,\n" +
                    "                        r.rule_body,\n" +
                    "                        r.rule_owner,\n" +
                    "                        r.update_user,\n" +
                    "                        r.update_ts\n" +
                    "                    FROM rule_t r\n" +
                    "                    JOIN rule_host_t h ON r.rule_id = h.rule_id\n" +
                    "                    WHERE r.common = 'Y'\n" +
                    "                      AND h.host_id != ?\n" +
                    "                       AND  NOT EXISTS (\n" +
                    "                         SELECT 1\n" +
                    "                        FROM rule_host_t eh\n" +
                    "                         WHERE eh.rule_id = r.rule_id\n" +
                    "                         AND eh.host_id=?\n" +
                    "                     )\n");
            parameters.add(UUID.fromString(hostId));
            parameters.add(UUID.fromString(hostId));


            StringBuilder whereClauseCommon = new StringBuilder();
            addCondition(whereClauseCommon, parameters, "r.rule_id", ruleId);
            addCondition(whereClauseCommon, parameters, "r.rule_name", ruleName);
            addCondition(whereClauseCommon, parameters, "r.rule_version", ruleVersion);
            addCondition(whereClauseCommon, parameters, "r.rule_type", ruleType);
            addCondition(whereClauseCommon, parameters, "r.rule_group", ruleGroup);
            addCondition(whereClauseCommon, parameters, "r.rule_desc", ruleDesc);
            addCondition(whereClauseCommon, parameters, "r.rule_body", ruleBody);
            addCondition(whereClauseCommon, parameters, "r.rule_owner", ruleOwner);

            if (!whereClauseCommon.isEmpty()) {
                sqlBuilder.append("AND ").append(whereClauseCommon);
            }


            sqlBuilder.append("                 ) AS combined_rules\n");

            sqlBuilder.append("ORDER BY rule_id\n" +
                    "LIMIT ? OFFSET ?");

            parameters.add(limit);
            parameters.add(offset);

            sql = sqlBuilder.toString();
        }

        int total = 0;
        List<Map<String, Object>> rules = new ArrayList<>();

        try (final Connection conn = ds.getConnection(); PreparedStatement preparedStatement = conn.prepareStatement(sql)) {
            for (int i = 0; i < parameters.size(); i++) {
                preparedStatement.setObject(i + 1, parameters.get(i));
            }
            boolean isFirstRow = true;

            try (ResultSet resultSet = preparedStatement.executeQuery()) {
                while (resultSet.next()) {
                    Map<String, Object> map = new HashMap<>();
                    // only get the total once as it is the same for all rows.
                    if (isFirstRow) {
                        total = resultSet.getInt("total");
                        isFirstRow = false;
                    }
                    map.put("hostId", resultSet.getObject("host_id", UUID.class));
                    map.put("ruleId", resultSet.getString("rule_id"));
                    map.put("ruleName", resultSet.getString("rule_name"));
                    map.put("ruleVersion", resultSet.getString("rule_version"));
                    map.put("ruleType", resultSet.getString("rule_type"));
                    map.put("ruleGroup", resultSet.getBoolean("rule_group"));
                    map.put("common", resultSet.getString("common"));
                    map.put("ruleDesc", resultSet.getString("rule_desc"));
                    map.put("ruleBody", resultSet.getString("rule_body"));
                    map.put("ruleOwner", resultSet.getString("rule_owner"));
                    map.put("updateUser", resultSet.getString("update_user"));
                    map.put("updateTs", resultSet.getObject("update_ts") != null ? resultSet.getObject("update_ts", OffsetDateTime.class) : null);
                    rules.add(map);
                }
            }
            Map<String, Object> resultMap = new HashMap<>();
            resultMap.put("total", total);
            resultMap.put("rules", rules);
            result = Success.of(JsonMapper.toJson(resultMap));
        } catch (SQLException e) {
            logger.error("SQLException:", e);
            result = Failure.of(new Status(SQL_EXCEPTION, e.getMessage()));
        } catch (Exception e) {
            logger.error("Exception:", e);
            result = Failure.of(new Status(GENERIC_EXCEPTION, e.getMessage()));
        }
        return result;
    }

    @Override
    public Result<Map<String, Object>> queryRuleById(String ruleId) {
        Result<Map<String, Object>> result;
        String sql = "SELECT rule_id, host_id, rule_type, rule_group, rule_visibility, rule_description, rule_body, rule_owner " +
                "update_user, update_ts " +
                "FROM rule_t WHERE rule_id = ?";
        try (final Connection conn = ds.getConnection()) {
            Map<String, Object> map = new HashMap<>();
            try (PreparedStatement statement = conn.prepareStatement(sql)) {
                statement.setString(1, ruleId);
                try (ResultSet resultSet = statement.executeQuery()) {
                    if (resultSet.next()) {
                        map.put("ruleId", resultSet.getString("rule_id"));
                        map.put("hostId", resultSet.getObject("host_id", UUID.class));
                        map.put("ruleType", resultSet.getString("rule_type"));
                        map.put("ruleGroup", resultSet.getBoolean("rule_group"));
                        map.put("ruleVisibility", resultSet.getString("rule_visibility"));
                        map.put("ruleDescription", resultSet.getString("rule_description"));
                        map.put("ruleBody", resultSet.getString("rule_body"));
                        map.put("ruleOwner", resultSet.getString("rule_owner"));
                        map.put("updateUser", resultSet.getString("update_user"));
                        map.put("updateTs", resultSet.getObject("update_ts") != null ? resultSet.getObject("update_ts", OffsetDateTime.class) : null);
                    }
                }
            }
            if (map.isEmpty())
                result = Failure.of(new Status(OBJECT_NOT_FOUND, "rule with ruleId ", ruleId));
            else
                result = Success.of(map);
        } catch (SQLException e) {
            logger.error("SQLException:", e);
            result = Failure.of(new Status(SQL_EXCEPTION, e.getMessage()));
        } catch (Exception e) {
            logger.error("Exception:", e);
            result = Failure.of(new Status(GENERIC_EXCEPTION, e.getMessage()));
        }
        return result;
    }

    @Override
    public Result<String> queryRuleByHostType(String hostId, String ruleType) {
        Result<String> result;
        String sql = "SELECT r.rule_id\n" +
                "FROM rule_t r, rule_host_t h\n" +
                "WHERE r.rule_id = h.rule_id\n" +
                "AND h.host_id = ?\n" +
                "AND r.rule_type = ?\n" +
                "UNION\n" +
                "SELECT r.rule_id r\n" +
                "FROM rule_t r, rule_host_t h\n" +
                "WHERE h.host_id != ?\n" +
                "AND r.rule_type = ?\n" +
                "AND r.common = 'Y'";
        try (final Connection conn = ds.getConnection()) {
            List<Map<String, Object>> list = new ArrayList<>();
            try (PreparedStatement statement = conn.prepareStatement(sql)) {
                statement.setObject(1, UUID.fromString(hostId));
                statement.setString(2, ruleType);
                statement.setObject(3, UUID.fromString(hostId));
                statement.setString(4, ruleType);

                try (ResultSet resultSet = statement.executeQuery()) {
                    while (resultSet.next()) {
                        Map<String, Object> map = new HashMap<>();
                        map.put("id", resultSet.getString("rule_id"));
                        map.put("label", resultSet.getString("rule_id"));
                        list.add(map);
                    }
                }
            }
            if (list.isEmpty())
                result = Failure.of(new Status(OBJECT_NOT_FOUND, "rule with host id and rule type ", hostId  + "|" + ruleType));
            else
                result = Success.of(JsonMapper.toJson(list));
        } catch (SQLException e) {
            logger.error("SQLException:", e);
            result = Failure.of(new Status(SQL_EXCEPTION, e.getMessage()));
        } catch (Exception e) {
            logger.error("Exception:", e);
            result = Failure.of(new Status(GENERIC_EXCEPTION, e.getMessage()));
        }
        return result;
    }

    @Override
    public Result<List<Map<String, Object>>> queryRuleByHostApiId(String hostId, String apiId, String apiVersion) {
        Result<List<Map<String, Object>>> result;
        String sql = "SELECT h.host_id, r.rule_id, r.rule_type, a.endpoint, r.rule_body\n" +
                "FROM rule_t r, rule_host_t h, api_endpoint_rule_t a \n" +
                "WHERE r.rule_id = h.rule_id\n" +
                "AND h.host_id = a.host_id\n" +
                "AND h.host_id = ?\n" +
                "AND a.api_id = ?\n" +
                "AND a.api_version = ?";
        try (final Connection conn = ds.getConnection()) {
            List<Map<String, Object>> list = new ArrayList<>();
            try (PreparedStatement statement = conn.prepareStatement(sql)) {
                statement.setObject(1, UUID.fromString(hostId));
                statement.setString(2, apiId);
                statement.setString(3, apiVersion);
                try (ResultSet resultSet = statement.executeQuery()) {
                    while (resultSet.next()) {
                        Map<String, Object> map = new HashMap<>();
                        map.put("hostId", resultSet.getObject("host_id", UUID.class));
                        map.put("ruleId", resultSet.getString("rule_id"));
                        map.put("ruleType", resultSet.getString("rule_type"));
                        map.put("endpoint", resultSet.getString("endpoint"));
                        map.put("ruleBody", resultSet.getString("rule_body"));
                        list.add(map);
                    }
                }
            }
            if (list.isEmpty())
                result = Failure.of(new Status(OBJECT_NOT_FOUND, "rule with hostId " + hostId + " apiId " + apiId + " apiVersion " + apiVersion));
            else
                result = Success.of(list);
        } catch (SQLException e) {
            logger.error("SQLException:", e);
            result = Failure.of(new Status(SQL_EXCEPTION, e.getMessage()));
        } catch (Exception e) {
            logger.error("Exception:", e);
            result = Failure.of(new Status(GENERIC_EXCEPTION, e.getMessage()));
        }
        return result;
    }


    @Override
    public Result<String> createRole(Map<String, Object> event) {
        final String insertRole = "INSERT INTO role_t (host_id, role_id, role_desc, update_user, update_ts) " +
                "VALUES (?, ?, ?, ?, ?)";

        Result<String> result = null;
        Map<String, Object> map = (Map<String, Object>)event.get(PortalConstants.DATA);
        try (Connection conn = ds.getConnection()) {
            conn.setAutoCommit(false);
            // no duplicate record, insert the user into database and write a success notification.
            try (PreparedStatement statement = conn.prepareStatement(insertRole)) {
                statement.setString(1, (String)event.get(Constants.HOST));
                statement.setString(2, (String)map.get("roleId"));
                String roleDesc = (String)map.get("roleDesc");
                if (roleDesc != null && !roleDesc.isEmpty())
                    statement.setString(3, roleDesc);
                else
                    statement.setNull(3, NULL);

                statement.setString(4, (String)event.get(Constants.USER));
                statement.setObject(5, OffsetDateTime.parse((String)event.get(CloudEventV1.TIME)));

                int count = statement.executeUpdate();
                if (count == 0) {
                    throw new SQLException("failed to insert role " + map.get("roleId"));
                }
                conn.commit();
                result = Success.of((String)map.get("roleId"));
                insertNotification(event, true, null);
            } catch (SQLException e) {
                logger.error("SQLException:", e);
                conn.rollback();
                insertNotification(event, false, e.getMessage());
                result = Failure.of(new Status(SQL_EXCEPTION, e.getMessage()));
            } catch (Exception e) {
                logger.error("Exception:", e);
                conn.rollback();
                insertNotification(event, false, e.getMessage());
                result = Failure.of(new Status(GENERIC_EXCEPTION, e.getMessage()));
            }
        } catch (SQLException e) {
            logger.error("SQLException:", e);
            result = Failure.of(new Status(SQL_EXCEPTION, e.getMessage()));
        }
        return result;
    }

    @Override
    public Result<String> updateRole(Map<String, Object> event) {
        final String updateRole = "UPDATE role_t SET role_desc = ?, update_user = ?, update_ts = ? " +
                "WHERE host_id = ? AND role_id = ?";

        Result<String> result = null;
        Map<String, Object> map = (Map<String, Object>)event.get(PortalConstants.DATA);
        try (Connection conn = ds.getConnection()) {
            conn.setAutoCommit(false);

            try (PreparedStatement statement = conn.prepareStatement(updateRole)) {
                String roleDesc = (String)map.get("roleDesc");
                if(roleDesc != null && !roleDesc.isEmpty()) {
                    statement.setString(1, roleDesc);
                } else {
                    statement.setNull(1, NULL);
                }
                statement.setString(2, (String)event.get(Constants.USER));
                statement.setObject(3, OffsetDateTime.parse((String)event.get(CloudEventV1.TIME)));
                statement.setString(4, (String)event.get(Constants.HOST));
                statement.setString(5, (String)map.get("roleId"));

                int count = statement.executeUpdate();
                if (count == 0) {
                    throw new SQLException("no record is updated for role " + map.get("roleId"));
                }
                conn.commit();
                result = Success.of((String)map.get("roleId"));
                insertNotification(event, true, null);
            } catch (SQLException e) {
                logger.error("SQLException:", e);
                conn.rollback();
                insertNotification(event, false, e.getMessage());
                result = Failure.of(new Status(SQL_EXCEPTION, e.getMessage()));
            } catch (Exception e) {
                logger.error("Exception:", e);
                conn.rollback();
                insertNotification(event, false, e.getMessage());
                result = Failure.of(new Status(GENERIC_EXCEPTION, e.getMessage()));
            }
        } catch (SQLException e) {
            logger.error("SQLException:", e);
            result = Failure.of(new Status(SQL_EXCEPTION, e.getMessage()));
        }
        return result;

    }

    @Override
    public Result<String> deleteRole(Map<String, Object> event) {
        final String deleteRole = "DELETE from role_t WHERE host_id = ? AND role_id = ?";
        Result<String> result;
        Map<String, Object> map = (Map<String, Object>)event.get(PortalConstants.DATA);
        try (Connection conn = ds.getConnection()) {
            conn.setAutoCommit(false);
            try (PreparedStatement statement = conn.prepareStatement(deleteRole)) {
                statement.setString(1, (String)event.get(Constants.HOST));
                statement.setString(2, (String)map.get("roleId"));
                int count = statement.executeUpdate();
                if (count == 0) {
                    throw new SQLException("no record is deleted for role " + (String)map.get("roleId"));
                }
                conn.commit();
                result = Success.of((String)event.get(Constants.USER));
                insertNotification(event, true, null);
            } catch (SQLException e) {
                logger.error("SQLException:", e);
                conn.rollback();
                insertNotification(event, false, e.getMessage());
                result = Failure.of(new Status(SQL_EXCEPTION, e.getMessage()));
            } catch (Exception e) {
                logger.error("Exception:", e);
                conn.rollback();
                insertNotification(event, false, e.getMessage());
                result = Failure.of(new Status(GENERIC_EXCEPTION, e.getMessage()));
            }
        } catch (SQLException e) {
            logger.error("SQLException:", e);
            result = Failure.of(new Status(SQL_EXCEPTION, e.getMessage()));
        }
        return result;
    }

    @Override
    public Result<String> queryRole(int offset, int limit, String hostId, String roleId, String roleDesc) {
        Result<String> result;
        StringBuilder sqlBuilder = new StringBuilder();
        sqlBuilder.append("SELECT COUNT(*) OVER () AS total, host_id, role_id, role_desc, update_user, update_ts " +
                "FROM role_t " +
                "WHERE host_id = ?\n");


        List<Object> parameters = new ArrayList<>();
        parameters.add(UUID.fromString(hostId));


        StringBuilder whereClause = new StringBuilder();

        addCondition(whereClause, parameters, "role_id", roleId);
        addCondition(whereClause, parameters, "role_desc", roleDesc);


        if (!whereClause.isEmpty()) {
            sqlBuilder.append("AND ").append(whereClause);
        }

        sqlBuilder.append(" ORDER BY role_id\n" +
                "LIMIT ? OFFSET ?");

        parameters.add(limit);
        parameters.add(offset);

        String sql = sqlBuilder.toString();
        if(logger.isTraceEnabled()) logger.trace("queryRole sql: {}", sql);
        int total = 0;
        List<Map<String, Object>> roles = new ArrayList<>();


        try (final Connection conn = ds.getConnection(); PreparedStatement preparedStatement = conn.prepareStatement(sql)) {
            for (int i = 0; i < parameters.size(); i++) {
                preparedStatement.setObject(i + 1, parameters.get(i));
            }
            boolean isFirstRow = true;
            try (ResultSet resultSet = preparedStatement.executeQuery()) {
                while (resultSet.next()) {
                    Map<String, Object> map = new HashMap<>();
                    // only get the total once as it is the same for all rows.
                    if (isFirstRow) {
                        total = resultSet.getInt("total");
                        isFirstRow = false;
                    }
                    map.put("hostId", resultSet.getObject("host_id", UUID.class));
                    map.put("roleId", resultSet.getString("role_id"));
                    map.put("roleDesc", resultSet.getString("role_desc"));
                    map.put("updateUser", resultSet.getString("update_user"));
                    map.put("updateTs", resultSet.getObject("update_ts") != null ? resultSet.getObject("update_ts", OffsetDateTime.class) : null);
                    roles.add(map);
                }
            }
            Map<String, Object> resultMap = new HashMap<>();
            resultMap.put("total", total);
            resultMap.put("roles", roles);
            result = Success.of(JsonMapper.toJson(resultMap));


        } catch (SQLException e) {
            logger.error("SQLException:", e);
            result = Failure.of(new Status(SQL_EXCEPTION, e.getMessage()));
        } catch (Exception e) {
            logger.error("Exception:", e);
            result = Failure.of(new Status(GENERIC_EXCEPTION, e.getMessage()));
        }

        return result;
    }

    @Override
    public Result<String> queryRoleLabel(String hostId) {
        final String sql = "SELECT role_id from role_t WHERE host_id = ?";
        Result<String> result;
        try (final Connection conn = ds.getConnection()) {
            List<Map<String, Object>> list = new ArrayList<>();
            try (PreparedStatement statement = conn.prepareStatement(sql)) {
                statement.setObject(1, UUID.fromString(hostId));
                try (ResultSet resultSet = statement.executeQuery()) {
                    while (resultSet.next()) {
                        Map<String, Object> map = new HashMap<>();
                        String id = resultSet.getString("role_id");
                        map.put("id", id);
                        map.put("label", id);
                        list.add(map);
                    }
                }
            }
            if (list.isEmpty())
                result = Failure.of(new Status(OBJECT_NOT_FOUND, "role", hostId));
            else
                result = Success.of(JsonMapper.toJson(list));
        } catch (SQLException e) {
            logger.error("SQLException:", e);
            result = Failure.of(new Status(SQL_EXCEPTION, e.getMessage()));
        } catch (Exception e) {
            logger.error("Exception:", e);
            result = Failure.of(new Status(GENERIC_EXCEPTION, e.getMessage()));
        }
        return result;
    }

    @Override
    public Result<String> queryRolePermission(int offset, int limit, String hostId, String roleId, String apiId, String apiVersion, String endpoint) {
        Result<String> result;
        StringBuilder sqlBuilder = new StringBuilder();
        sqlBuilder.append("SELECT COUNT(*) OVER () AS total, \n" +
                "r.host_id, r.role_id, p.api_id, p.api_version, p.endpoint\n" +
                "FROM role_t r, role_permission_t p\n" +
                "WHERE r.role_id = p.role_id\n" +
                "AND r.host_id = ?\n");


        List<Object> parameters = new ArrayList<>();
        parameters.add(UUID.fromString(hostId));


        StringBuilder whereClause = new StringBuilder();

        addCondition(whereClause, parameters, "r.role_id", roleId);
        addCondition(whereClause, parameters, "p.api_id", apiId);
        addCondition(whereClause, parameters, "p.api_version", apiVersion);
        addCondition(whereClause, parameters, "p.endpoint", endpoint);

        if (!whereClause.isEmpty()) {
            sqlBuilder.append("AND ").append(whereClause);
        }

        sqlBuilder.append(" ORDER BY r.role_id, p.api_id, p.api_version, p.endpoint\n" +
                "LIMIT ? OFFSET ?");

        parameters.add(limit);
        parameters.add(offset);

        String sql = sqlBuilder.toString();
        if(logger.isTraceEnabled()) logger.trace("queryRolePermission sql: {}", sql);
        int total = 0;
        List<Map<String, Object>> rolePermissions = new ArrayList<>();


        try (final Connection conn = ds.getConnection(); PreparedStatement preparedStatement = conn.prepareStatement(sql)) {
            for (int i = 0; i < parameters.size(); i++) {
                preparedStatement.setObject(i + 1, parameters.get(i));
            }
            boolean isFirstRow = true;
            try (ResultSet resultSet = preparedStatement.executeQuery()) {
                while (resultSet.next()) {
                    Map<String, Object> map = new HashMap<>();
                    // only get the total once as it is the same for all rows.
                    if (isFirstRow) {
                        total = resultSet.getInt("total");
                        isFirstRow = false;
                    }
                    map.put("hostId", resultSet.getObject("host_id", UUID.class));
                    map.put("roleId", resultSet.getString("role_id"));
                    map.put("apiId", resultSet.getString("api_id"));
                    map.put("apiVersion", resultSet.getString("api_version"));
                    map.put("endpoint", resultSet.getString("endpoint"));
                    rolePermissions.add(map);
                }
            }
            Map<String, Object> resultMap = new HashMap<>();
            resultMap.put("total", total);
            resultMap.put("rolePermissions", rolePermissions);
            result = Success.of(JsonMapper.toJson(resultMap));

        } catch (SQLException e) {
            logger.error("SQLException:", e);
            result = Failure.of(new Status(SQL_EXCEPTION, e.getMessage()));
        } catch (Exception e) {
            logger.error("Exception:", e);
            result = Failure.of(new Status(GENERIC_EXCEPTION, e.getMessage()));
        }

        return result;

    }

    @Override
    public Result<String> queryRoleUser(int offset, int limit, String hostId, String roleId, String userId, String entityId, String email, String firstName, String lastName, String userType) {
        Result<String> result;
        StringBuilder sqlBuilder = new StringBuilder();
        sqlBuilder.append("SELECT COUNT(*) OVER () AS total, \n" +
                "r.host_id, r.role_id, r.start_ts, r.end_ts, \n" +
                "u.user_id, u.email, u.user_type, \n" +
                "CASE\n" +
                "    WHEN u.user_type = 'C' THEN c.customer_id\n" +
                "    WHEN u.user_type = 'E' THEN e.employee_id\n" +
                "    ELSE NULL -- Handle other cases if needed\n" +
                "END AS entity_id,\n" +
                "e.manager_id, u.first_name, u.last_name\n" +
                "FROM user_t u\n" +
                "LEFT JOIN\n" +
                "    customer_t c ON u.user_id = c.user_id AND u.user_type = 'C'\n" +
                "LEFT JOIN\n" +
                "    employee_t e ON u.user_id = e.user_id AND u.user_type = 'E'\n" +
                "INNER JOIN\n" +
                "    role_user_t r ON r.user_id = u.user_id\n" +
                "AND r.host_id = ?\n");


        List<Object> parameters = new ArrayList<>();
        parameters.add(UUID.fromString(hostId));


        StringBuilder whereClause = new StringBuilder();

        addCondition(whereClause, parameters, "r.role_id", roleId);
        addCondition(whereClause, parameters, "u.user_id", userId != null ? UUID.fromString(userId) : null);
        addCondition(whereClause, parameters, "entity_id", entityId);
        addCondition(whereClause, parameters, "u.email", email);
        addCondition(whereClause, parameters, "u.first_name", firstName);
        addCondition(whereClause, parameters, "u.last_name", lastName);
        addCondition(whereClause, parameters, "u.user_type", userType);

        if (!whereClause.isEmpty()) {
            sqlBuilder.append("AND ").append(whereClause);
        }

        sqlBuilder.append(" ORDER BY r.role_id, u.user_id\n" +
                "LIMIT ? OFFSET ?");

        parameters.add(limit);
        parameters.add(offset);

        String sql = sqlBuilder.toString();
        if(logger.isTraceEnabled()) logger.trace("queryRoleUser sql: {}", sql);
        int total = 0;
        List<Map<String, Object>> roleUsers = new ArrayList<>();


        try (final Connection conn = ds.getConnection(); PreparedStatement preparedStatement = conn.prepareStatement(sql)) {
            for (int i = 0; i < parameters.size(); i++) {
                preparedStatement.setObject(i + 1, parameters.get(i));
            }
            boolean isFirstRow = true;
            try (ResultSet resultSet = preparedStatement.executeQuery()) {
                while (resultSet.next()) {
                    Map<String, Object> map = new HashMap<>();
                    // only get the total once as it is the same for all rows.
                    if (isFirstRow) {
                        total = resultSet.getInt("total");
                        isFirstRow = false;
                    }
                    map.put("hostId", resultSet.getObject("host_id", UUID.class));
                    map.put("roleId", resultSet.getString("role_id"));
                    map.put("startTs", resultSet.getTimestamp("start_ts"));
                    map.put("endTs", resultSet.getTimestamp("end_ts"));
                    map.put("userId", resultSet.getObject("user_id", UUID.class));
                    map.put("entityId", resultSet.getString("entity_id"));
                    map.put("email", resultSet.getString("email"));
                    map.put("firstName", resultSet.getString("first_name"));
                    map.put("lastName", resultSet.getString("last_name"));
                    map.put("userType", resultSet.getString("user_type"));
                    roleUsers.add(map);
                }
            }
            Map<String, Object> resultMap = new HashMap<>();
            resultMap.put("total", total);
            resultMap.put("roleUsers", roleUsers);
            result = Success.of(JsonMapper.toJson(resultMap));

        } catch (SQLException e) {
            logger.error("SQLException:", e);
            result = Failure.of(new Status(SQL_EXCEPTION, e.getMessage()));
        } catch (Exception e) {
            logger.error("Exception:", e);
            result = Failure.of(new Status(GENERIC_EXCEPTION, e.getMessage()));
        }

        return result;

    }

    @Override
    public Result<String> createRolePermission(Map<String, Object> event) {
        final String insertRole = "INSERT INTO role_permission_t (host_id, role_id, api_id, api_version, endpoint, update_user, update_ts) " +
                "VALUES (?, ?, ?, ?, ?,  ?, ?)";

        Result<String> result = null;
        Map<String, Object> map = (Map<String, Object>)event.get(PortalConstants.DATA);
        try (Connection conn = ds.getConnection()) {
            conn.setAutoCommit(false);
            // no duplicate record, insert the user into database and write a success notification.
            try (PreparedStatement statement = conn.prepareStatement(insertRole)) {
                statement.setString(1, (String)event.get(Constants.HOST));
                statement.setString(2, (String)map.get("roleId"));
                statement.setString(3, (String)map.get("apiId"));
                statement.setString(4, (String)map.get("apiVersion"));
                statement.setString(5, (String)map.get("endpoint"));
                statement.setString(6, (String)event.get(Constants.USER));
                statement.setObject(7, OffsetDateTime.parse((String)event.get(CloudEventV1.TIME)));

                int count = statement.executeUpdate();
                if (count == 0) {
                    throw new SQLException("failed to insert role permission " + (String)map.get("roleId"));
                }
                conn.commit();
                result = Success.of((String)map.get("roleId"));
                insertNotification(event, true, null);
            } catch (SQLException e) {
                logger.error("SQLException:", e);
                conn.rollback();
                insertNotification(event, false, e.getMessage());
                result = Failure.of(new Status(SQL_EXCEPTION, e.getMessage()));
            } catch (Exception e) {
                logger.error("Exception:", e);
                conn.rollback();
                insertNotification(event, false, e.getMessage());
                result = Failure.of(new Status(GENERIC_EXCEPTION, e.getMessage()));
            }
        } catch (SQLException e) {
            logger.error("SQLException:", e);
            result = Failure.of(new Status(SQL_EXCEPTION, e.getMessage()));
        }
        return result;

    }

    @Override
    public Result<String> deleteRolePermission(Map<String, Object> event) {
        final String deleteRole = "DELETE from role_permission_t WHERE host_id = ? AND role_id = ? AND api_id = ? AND api_version = ? AND endpoint = ?";
        Result<String> result;
        Map<String, Object> map = (Map<String, Object>)event.get(PortalConstants.DATA);
        try (Connection conn = ds.getConnection()) {
            conn.setAutoCommit(false);
            try (PreparedStatement statement = conn.prepareStatement(deleteRole)) {
                statement.setString(1, (String)event.get(Constants.HOST));
                statement.setString(2, (String)map.get("roleId"));
                statement.setString(3, (String)map.get("apiId"));
                statement.setString(4, (String)map.get("apiVersion"));
                statement.setString(5, (String)map.get("endpoint"));

                int count = statement.executeUpdate();
                if (count == 0) {
                    throw new SQLException("no record is deleted for role " + (String)map.get("roleId"));
                }
                conn.commit();
                result = Success.of((String)event.get(Constants.USER));
                insertNotification(event, true, null);
            } catch (SQLException e) {
                logger.error("SQLException:", e);
                conn.rollback();
                insertNotification(event, false, e.getMessage());
                result = Failure.of(new Status(SQL_EXCEPTION, e.getMessage()));
            } catch (Exception e) {
                logger.error("Exception:", e);
                conn.rollback();
                insertNotification(event, false, e.getMessage());
                result = Failure.of(new Status(GENERIC_EXCEPTION, e.getMessage()));
            }
        } catch (SQLException e) {
            logger.error("SQLException:", e);
            result = Failure.of(new Status(SQL_EXCEPTION, e.getMessage()));
        }
        return result;
    }

    @Override
    public Result<String> createRoleUser(Map<String, Object> event) {
        final String insertRole = "INSERT INTO role_user_t (host_id, role_id, user_id, start_ts, end_ts, update_user, update_ts) " +
                "VALUES (?, ?, ?, ?, ?,  ?, ?)";

        Result<String> result = null;
        Map<String, Object> map = (Map<String, Object>)event.get(PortalConstants.DATA);
        try (Connection conn = ds.getConnection()) {
            conn.setAutoCommit(false);
            // no duplicate record, insert the user into database and write a success notification.
            try (PreparedStatement statement = conn.prepareStatement(insertRole)) {
                statement.setString(1, (String)event.get(Constants.HOST));
                statement.setString(2, (String)map.get("roleId"));
                statement.setString(3, (String)event.get(Constants.USER));

                String startTs = (String)map.get("startTs");
                if(startTs != null && !startTs.isEmpty())
                    statement.setObject(4, OffsetDateTime.parse(startTs));
                else
                    statement.setNull(4, NULL);
                String endTs = (String)map.get("endTs");
                if (endTs != null && !endTs.isEmpty()) {
                    statement.setObject(5, OffsetDateTime.parse(endTs));
                } else {
                    statement.setNull(5, NULL);
                }
                statement.setString(6, (String)event.get(Constants.USER));
                statement.setObject(7,  OffsetDateTime.parse((String)event.get(CloudEventV1.TIME)));

                int count = statement.executeUpdate();
                if (count == 0) {
                    throw new SQLException("failed to insert role user " + map.get("roleId"));
                }
                conn.commit();
                result = Success.of((String)map.get("roleId"));
                insertNotification(event, true, null);
            } catch (SQLException e) {
                logger.error("SQLException:", e);
                conn.rollback();
                insertNotification(event, false, e.getMessage());
                result = Failure.of(new Status(SQL_EXCEPTION, e.getMessage()));
            } catch (Exception e) {
                logger.error("Exception:", e);
                conn.rollback();
                insertNotification(event, false, e.getMessage());
                result = Failure.of(new Status(GENERIC_EXCEPTION, e.getMessage()));
            }
        } catch (SQLException e) {
            logger.error("SQLException:", e);
            result = Failure.of(new Status(SQL_EXCEPTION, e.getMessage()));
        }
        return result;

    }

    @Override
    public Result<String> updateRoleUser(Map<String, Object> event) {
        final String updateRole = "UPDATE role_user_t SET start_ts = ?, end_ts = ?, update_user = ?, update_ts = ? " +
                "WHERE host_id = ? AND role_id = ? AND user_id = ?";

        Result<String> result = null;
        Map<String, Object> map = (Map<String, Object>)event.get(PortalConstants.DATA);
        try (Connection conn = ds.getConnection()) {
            conn.setAutoCommit(false);

            try (PreparedStatement statement = conn.prepareStatement(updateRole)) {
                String startTs = (String)map.get("startTs");
                if(startTs != null && !startTs.isEmpty())
                    statement.setObject(1, OffsetDateTime.parse(startTs));
                else
                    statement.setNull(1, NULL);
                String endTs = (String)map.get("endTs");
                if (endTs != null && !endTs.isEmpty()) {
                    statement.setObject(2, OffsetDateTime.parse(endTs));
                } else {
                    statement.setNull(2, NULL);
                }
                statement.setString(3, (String)event.get(Constants.USER));
                statement.setObject(4, OffsetDateTime.parse((String)event.get(CloudEventV1.TIME)));
                statement.setString(5, (String)event.get(Constants.HOST));
                statement.setString(6, (String)map.get("roleId"));
                statement.setString(7, (String)event.get(Constants.USER));

                int count = statement.executeUpdate();
                if (count == 0) {
                    throw new SQLException("no record is updated for role user " + (String)map.get("roleId"));
                }
                conn.commit();
                result = Success.of((String)map.get("roleId"));
                insertNotification(event, true, null);
            } catch (SQLException e) {
                logger.error("SQLException:", e);
                conn.rollback();
                insertNotification(event, false, e.getMessage());
                result = Failure.of(new Status(SQL_EXCEPTION, e.getMessage()));
            } catch (Exception e) {
                logger.error("Exception:", e);
                conn.rollback();
                insertNotification(event, false, e.getMessage());
                result = Failure.of(new Status(GENERIC_EXCEPTION, e.getMessage()));
            }
        } catch (SQLException e) {
            logger.error("SQLException:", e);
            result = Failure.of(new Status(SQL_EXCEPTION, e.getMessage()));
        }
        return result;

    }

    @Override
    public Result<String> deleteRoleUser(Map<String, Object> event) {
        final String deleteRole = "DELETE from role_user_t WHERE host_id = ? AND role_id = ? AND user_id = ?";
        Result<String> result;
        Map<String, Object> map = (Map<String, Object>)event.get(PortalConstants.DATA);
        try (Connection conn = ds.getConnection()) {
            conn.setAutoCommit(false);
            try (PreparedStatement statement = conn.prepareStatement(deleteRole)) {
                statement.setString(1, (String)event.get(Constants.HOST));
                statement.setString(2, (String)map.get("roleId"));
                statement.setString(3, (String)event.get(Constants.USER));

                int count = statement.executeUpdate();
                if (count == 0) {
                    throw new SQLException("no record is deleted for role user " + (String)map.get("roleId"));
                }
                conn.commit();
                result = Success.of((String)event.get(Constants.USER));
                insertNotification(event, true, null);
            } catch (SQLException e) {
                logger.error("SQLException:", e);
                conn.rollback();
                insertNotification(event, false, e.getMessage());
                result = Failure.of(new Status(SQL_EXCEPTION, e.getMessage()));
            } catch (Exception e) {
                logger.error("Exception:", e);
                conn.rollback();
                insertNotification(event, false, e.getMessage());
                result = Failure.of(new Status(GENERIC_EXCEPTION, e.getMessage()));
            }
        } catch (SQLException e) {
            logger.error("SQLException:", e);
            result = Failure.of(new Status(SQL_EXCEPTION, e.getMessage()));
        }
        return result;

    }

    @Override
    public Result<String> queryRoleRowFilter(int offset, int limit, String hostId, String roleId, String apiId, String apiVersion, String endpoint) {
        Result<String> result;
        StringBuilder sqlBuilder = new StringBuilder();
        sqlBuilder.append("SELECT COUNT(*) OVER () AS total, \n" +
                "r.host_id, r.role_id, p.api_id, p.api_version, p.endpoint, p.col_name, p.operator, p.col_value\n" +
                "FROM role_t r, role_row_filter_t p\n" +
                "WHERE r.role_id = p.role_id\n" +
                "AND r.host_id = ?\n");

        List<Object> parameters = new ArrayList<>();
        parameters.add(UUID.fromString(hostId));

        StringBuilder whereClause = new StringBuilder();

        addCondition(whereClause, parameters, "r.role_id", roleId);
        addCondition(whereClause, parameters, "p.api_id", apiId);
        addCondition(whereClause, parameters, "p.api_version", apiVersion);
        addCondition(whereClause, parameters, "p.endpoint", endpoint);

        if (!whereClause.isEmpty()) {
            sqlBuilder.append("AND ").append(whereClause);
        }

        sqlBuilder.append(" ORDER BY r.role_id, p.api_id, p.api_version, p.endpoint, p.col_name\n" +
                "LIMIT ? OFFSET ?");

        parameters.add(limit);
        parameters.add(offset);

        String sql = sqlBuilder.toString();
        if(logger.isTraceEnabled()) logger.trace("queryRoleRowFilter sql: {}", sql);
        int total = 0;
        List<Map<String, Object>> roleRowFilters = new ArrayList<>();

        try (final Connection conn = ds.getConnection(); PreparedStatement preparedStatement = conn.prepareStatement(sql)) {
            for (int i = 0; i < parameters.size(); i++) {
                preparedStatement.setObject(i + 1, parameters.get(i));
            }
            boolean isFirstRow = true;
            try (ResultSet resultSet = preparedStatement.executeQuery()) {
                while (resultSet.next()) {
                    Map<String, Object> map = new HashMap<>();
                    // only get the total once as it is the same for all rows.
                    if (isFirstRow) {
                        total = resultSet.getInt("total");
                        isFirstRow = false;
                    }
                    map.put("hostId", resultSet.getObject("host_id", UUID.class));
                    map.put("roleId", resultSet.getString("role_id"));
                    map.put("apiId", resultSet.getString("api_id"));
                    map.put("apiVersion", resultSet.getString("api_version"));
                    map.put("endpoint", resultSet.getString("endpoint"));
                    map.put("colName", resultSet.getString("col_name"));
                    map.put("operator", resultSet.getString("operator"));
                    map.put("colValue", resultSet.getString("col_value"));
                    roleRowFilters.add(map);
                }
            }
            Map<String, Object> resultMap = new HashMap<>();
            resultMap.put("total", total);
            resultMap.put("roleRowFilters", roleRowFilters);
            result = Success.of(JsonMapper.toJson(resultMap));

        } catch (SQLException e) {
            logger.error("SQLException:", e);
            result = Failure.of(new Status(SQL_EXCEPTION, e.getMessage()));
        } catch (Exception e) {
            logger.error("Exception:", e);
            result = Failure.of(new Status(GENERIC_EXCEPTION, e.getMessage()));
        }

        return result;
    }

    @Override
    public Result<String> deleteRoleRowFilter(Map<String, Object> event) {
        final String deleteRole = "DELETE from role_row_filter_t WHERE host_id = ? AND role_id = ? AND api_id = ? AND api_version = ? AND endpoint = ? AND col_name = ?";
        Result<String> result;
        Map<String, Object> map = (Map<String, Object>)event.get(PortalConstants.DATA);
        try (Connection conn = ds.getConnection()) {
            conn.setAutoCommit(false);
            try (PreparedStatement statement = conn.prepareStatement(deleteRole)) {
                statement.setString(1, (String)event.get(Constants.HOST));
                statement.setString(2, (String)map.get("roleId"));
                statement.setString(3, (String)map.get("apiId"));
                statement.setString(4, (String)map.get("apiVersion"));
                statement.setString(5, (String)map.get("endpoint"));
                statement.setString(6, (String)map.get("colName"));

                int count = statement.executeUpdate();
                if (count == 0) {
                    throw new SQLException("no record is deleted for role row filter " + (String)map.get("roleId"));
                }
                conn.commit();
                result = Success.of((String)event.get(Constants.USER));
                insertNotification(event, true, null);
            } catch (SQLException e) {
                logger.error("SQLException:", e);
                conn.rollback();
                insertNotification(event, false, e.getMessage());
                result = Failure.of(new Status(SQL_EXCEPTION, e.getMessage()));
            } catch (Exception e) {
                logger.error("Exception:", e);
                conn.rollback();
                insertNotification(event, false, e.getMessage());
                result = Failure.of(new Status(GENERIC_EXCEPTION, e.getMessage()));
            }
        } catch (SQLException e) {
            logger.error("SQLException:", e);
            result = Failure.of(new Status(SQL_EXCEPTION, e.getMessage()));
        }
        return result;
    }

    @Override
    public Result<String> createRoleRowFilter(Map<String, Object> event) {
        final String insertRole = "INSERT INTO role_row_filter_t (host_id, role_id, api_id, api_version, endpoint, col_name, operator, col_value, update_user, update_ts) " +
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";

        Result<String> result = null;
        Map<String, Object> map = (Map<String, Object>)event.get(PortalConstants.DATA);
        try (Connection conn = ds.getConnection()) {
            conn.setAutoCommit(false);
            // no duplicate record, insert the user into database and write a success notification.
            try (PreparedStatement statement = conn.prepareStatement(insertRole)) {
                statement.setString(1, (String)event.get(Constants.HOST));
                statement.setString(2, (String)map.get("roleId"));
                statement.setString(3, (String)map.get("apiId"));
                statement.setString(4, (String)map.get("apiVersion"));
                statement.setString(5, (String)map.get("endpoint"));
                statement.setString(6, (String)map.get("colName"));
                statement.setString(7, (String)map.get("operator"));
                statement.setString(8, (String)map.get("colValue"));
                statement.setString(9, (String)event.get(Constants.USER));
                statement.setObject(10, OffsetDateTime.parse((String)event.get(CloudEventV1.TIME)));

                int count = statement.executeUpdate();
                if (count == 0) {
                    throw new SQLException("failed to insert role row filter " + (String)map.get("roleId"));
                }
                conn.commit();
                result = Success.of((String)map.get("roleId"));
                insertNotification(event, true, null);
            } catch (SQLException e) {
                logger.error("SQLException:", e);
                conn.rollback();
                insertNotification(event, false, e.getMessage());
                result = Failure.of(new Status(SQL_EXCEPTION, e.getMessage()));
            } catch (Exception e) {
                logger.error("Exception:", e);
                conn.rollback();
                insertNotification(event, false, e.getMessage());
                result = Failure.of(new Status(GENERIC_EXCEPTION, e.getMessage()));
            }
        } catch (SQLException e) {
            logger.error("SQLException:", e);
            result = Failure.of(new Status(SQL_EXCEPTION, e.getMessage()));
        }
        return result;
    }

    @Override
    public Result<String> updateRoleRowFilter(Map<String, Object> event) {
        final String updateRole = "UPDATE role_row_filter_t SET operator = ?, col_value = ?, update_user = ?, update_ts = ? " +
                "WHERE host_id = ? AND role_id = ? AND api_id = ? AND api_version = ? AND endpoint = ? AND col_name = ?";

        Result<String> result = null;
        Map<String, Object> map = (Map<String, Object>)event.get(PortalConstants.DATA);
        try (Connection conn = ds.getConnection()) {
            conn.setAutoCommit(false);

            try (PreparedStatement statement = conn.prepareStatement(updateRole)) {
                statement.setString(1, (String)map.get("operator"));
                statement.setString(2, (String)map.get("colValue"));
                statement.setString(3, (String)event.get(Constants.USER));
                statement.setObject(4, OffsetDateTime.parse((String)event.get(CloudEventV1.TIME)));
                statement.setString(5, (String)event.get(Constants.HOST));
                statement.setString(6, (String)map.get("roleId"));
                statement.setString(7, (String)map.get("apiId"));
                statement.setString(8, (String)map.get("apiVersion"));
                statement.setString(9, (String)map.get("endpoint"));
                statement.setString(10, (String)map.get("colName"));

                int count = statement.executeUpdate();
                if (count == 0) {
                    throw new SQLException("no record is updated for role row filter " + (String)map.get("roleId"));
                }
                conn.commit();
                result = Success.of((String)map.get("roleId"));
                insertNotification(event, true, null);
            } catch (SQLException e) {
                logger.error("SQLException:", e);
                conn.rollback();
                insertNotification(event, false, e.getMessage());
                result = Failure.of(new Status(SQL_EXCEPTION, e.getMessage()));
            } catch (Exception e) {
                logger.error("Exception:", e);
                conn.rollback();
                insertNotification(event, false, e.getMessage());
                result = Failure.of(new Status(GENERIC_EXCEPTION, e.getMessage()));
            }
        } catch (SQLException e) {
            logger.error("SQLException:", e);
            result = Failure.of(new Status(SQL_EXCEPTION, e.getMessage()));
        }
        return result;
    }

    @Override
    public Result<String> queryRoleColFilter(int offset, int limit, String hostId, String roleId, String apiId, String apiVersion, String endpoint) {
        Result<String> result;
        StringBuilder sqlBuilder = new StringBuilder();
        sqlBuilder.append("SELECT COUNT(*) OVER () AS total, \n" +
                "r.host_id, r.role_id, p.api_id, p.api_version, p.endpoint, p.columns\n" +
                "FROM role_t r, role_col_filter_t p\n" +
                "WHERE r.role_id = p.role_id\n" +
                "AND r.host_id = ?\n");

        List<Object> parameters = new ArrayList<>();
        parameters.add(UUID.fromString(hostId));

        StringBuilder whereClause = new StringBuilder();

        addCondition(whereClause, parameters, "r.role_id", roleId);
        addCondition(whereClause, parameters, "p.api_id", apiId);
        addCondition(whereClause, parameters, "p.api_version", apiVersion);
        addCondition(whereClause, parameters, "p.endpoint", endpoint);

        if (!whereClause.isEmpty()) {
            sqlBuilder.append("AND ").append(whereClause);
        }

        sqlBuilder.append(" ORDER BY r.role_id, p.api_id, p.api_version, p.endpoint\n" +
                "LIMIT ? OFFSET ?");

        parameters.add(limit);
        parameters.add(offset);

        String sql = sqlBuilder.toString();
        if(logger.isTraceEnabled()) logger.trace("queryRoleColFilter sql: {}", sql);
        int total = 0;
        List<Map<String, Object>> roleColFilters = new ArrayList<>();

        try (final Connection conn = ds.getConnection(); PreparedStatement preparedStatement = conn.prepareStatement(sql)) {
            for (int i = 0; i < parameters.size(); i++) {
                preparedStatement.setObject(i + 1, parameters.get(i));
            }
            boolean isFirstRow = true;
            try (ResultSet resultSet = preparedStatement.executeQuery()) {
                while (resultSet.next()) {
                    Map<String, Object> map = new HashMap<>();
                    // only get the total once as it is the same for all rows.
                    if (isFirstRow) {
                        total = resultSet.getInt("total");
                        isFirstRow = false;
                    }
                    map.put("hostId", resultSet.getObject("host_id", UUID.class));
                    map.put("roleId", resultSet.getString("role_id"));
                    map.put("apiId", resultSet.getString("api_id"));
                    map.put("apiVersion", resultSet.getString("api_version"));
                    map.put("endpoint", resultSet.getString("endpoint"));
                    map.put("columns", resultSet.getString("columns"));
                    roleColFilters.add(map);
                }
            }
            Map<String, Object> resultMap = new HashMap<>();
            resultMap.put("total", total);
            resultMap.put("roleColFilters", roleColFilters);
            result = Success.of(JsonMapper.toJson(resultMap));

        } catch (SQLException e) {
            logger.error("SQLException:", e);
            result = Failure.of(new Status(SQL_EXCEPTION, e.getMessage()));
        } catch (Exception e) {
            logger.error("Exception:", e);
            result = Failure.of(new Status(GENERIC_EXCEPTION, e.getMessage()));
        }

        return result;
    }

    @Override
    public Result<String> createRoleColFilter(Map<String, Object> event) {
        final String insertRole = "INSERT INTO role_col_filter_t (host_id, role_id, api_id, api_version, endpoint, columns, update_user, update_ts) " +
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?)";

        Result<String> result = null;
        Map<String, Object> map = (Map<String, Object>)event.get(PortalConstants.DATA);
        try (Connection conn = ds.getConnection()) {
            conn.setAutoCommit(false);
            // no duplicate record, insert the user into database and write a success notification.
            try (PreparedStatement statement = conn.prepareStatement(insertRole)) {
                statement.setString(1, (String)event.get(Constants.HOST));
                statement.setString(2, (String)map.get("roleId"));
                statement.setString(3, (String)map.get("apiId"));
                statement.setString(4, (String)map.get("apiVersion"));
                statement.setString(5, (String)map.get("endpoint"));
                statement.setString(6, (String)map.get("columns"));
                statement.setString(7, (String)event.get(Constants.USER));
                statement.setObject(8, OffsetDateTime.parse((String)event.get(CloudEventV1.TIME)));

                int count = statement.executeUpdate();
                if (count == 0) {
                    throw new SQLException("failed to insert role col filter " + (String)map.get("roleId"));
                }
                conn.commit();
                result = Success.of((String)map.get("roleId"));
                insertNotification(event, true, null);
            } catch (SQLException e) {
                logger.error("SQLException:", e);
                conn.rollback();
                insertNotification(event, false, e.getMessage());
                result = Failure.of(new Status(SQL_EXCEPTION, e.getMessage()));
            } catch (Exception e) {
                logger.error("Exception:", e);
                conn.rollback();
                insertNotification(event, false, e.getMessage());
                result = Failure.of(new Status(GENERIC_EXCEPTION, e.getMessage()));
            }
        } catch (SQLException e) {
            logger.error("SQLException:", e);
            result = Failure.of(new Status(SQL_EXCEPTION, e.getMessage()));
        }
        return result;
    }

    @Override
    public Result<String> deleteRoleColFilter(Map<String, Object> event) {
        final String deleteRole = "DELETE from role_col_filter_t WHERE host_id = ? AND role_id = ? AND api_id = ? AND api_version = ? AND endpoint = ?";
        Result<String> result;
        Map<String, Object> map = (Map<String, Object>)event.get(PortalConstants.DATA);
        try (Connection conn = ds.getConnection()) {
            conn.setAutoCommit(false);
            try (PreparedStatement statement = conn.prepareStatement(deleteRole)) {
                statement.setString(1, (String)event.get(Constants.HOST));
                statement.setString(2, (String)map.get("roleId"));
                statement.setString(3, (String)map.get("apiId"));
                statement.setString(4, (String)map.get("apiVersion"));
                statement.setString(5, (String)map.get("endpoint"));

                int count = statement.executeUpdate();
                if (count == 0) {
                    throw new SQLException("no record is deleted for role col filter " + (String)map.get("roleId"));
                }
                conn.commit();
                result = Success.of((String)event.get(Constants.USER));
                insertNotification(event, true, null);
            } catch (SQLException e) {
                logger.error("SQLException:", e);
                conn.rollback();
                insertNotification(event, false, e.getMessage());
                result = Failure.of(new Status(SQL_EXCEPTION, e.getMessage()));
            } catch (Exception e) {
                logger.error("Exception:", e);
                conn.rollback();
                insertNotification(event, false, e.getMessage());
                result = Failure.of(new Status(GENERIC_EXCEPTION, e.getMessage()));
            }
        } catch (SQLException e) {
            logger.error("SQLException:", e);
            result = Failure.of(new Status(SQL_EXCEPTION, e.getMessage()));
        }
        return result;
    }

    @Override
    public Result<String> updateRoleColFilter(Map<String, Object> event) {
        final String updateRole = "UPDATE role_col_filter_t SET columns = ?, update_user = ?, update_ts = ? " +
                "WHERE host_id = ? AND role_id = ? AND api_id = ? AND api_version = ? AND endpoint = ?";

        Result<String> result = null;
        Map<String, Object> map = (Map<String, Object>)event.get(PortalConstants.DATA);
        try (Connection conn = ds.getConnection()) {
            conn.setAutoCommit(false);

            try (PreparedStatement statement = conn.prepareStatement(updateRole)) {
                statement.setString(1, (String)map.get("columns"));
                statement.setString(2, (String)event.get(Constants.USER));
                statement.setObject(3, OffsetDateTime.parse((String)event.get(CloudEventV1.TIME)));
                statement.setString(4, (String)event.get(Constants.HOST));
                statement.setString(5, (String)map.get("roleId"));
                statement.setString(6, (String)map.get("apiId"));
                statement.setString(7, (String)map.get("apiVersion"));
                statement.setString(8, (String)map.get("endpoint"));

                int count = statement.executeUpdate();
                if (count == 0) {
                    throw new SQLException("no record is updated for role col filter " + (String)map.get("roleId"));
                }
                conn.commit();
                result = Success.of((String)map.get("roleId"));
                insertNotification(event, true, null);
            } catch (SQLException e) {
                logger.error("SQLException:", e);
                conn.rollback();
                insertNotification(event, false, e.getMessage());
                result = Failure.of(new Status(SQL_EXCEPTION, e.getMessage()));
            } catch (Exception e) {
                logger.error("Exception:", e);
                conn.rollback();
                insertNotification(event, false, e.getMessage());
                result = Failure.of(new Status(GENERIC_EXCEPTION, e.getMessage()));
            }
        } catch (SQLException e) {
            logger.error("SQLException:", e);
            result = Failure.of(new Status(SQL_EXCEPTION, e.getMessage()));
        }
        return result;
    }

    @Override
    public Result<String> createGroup(Map<String, Object> event) {
        final String insertGroup = "INSERT INTO group_t (host_id, group_id, group_desc, update_user, update_ts) " +
                "VALUES (?, ?, ?, ?, ?)";

        Result<String> result = null;
        Map<String, Object> map = (Map<String, Object>)event.get(PortalConstants.DATA);
        try (Connection conn = ds.getConnection()) {
            conn.setAutoCommit(false);
            try (PreparedStatement statement = conn.prepareStatement(insertGroup)) {
                statement.setString(1, (String)event.get(Constants.HOST));
                statement.setString(2, (String)map.get("groupId"));
                String groupDesc = (String)map.get("groupDesc");
                if (groupDesc != null && !groupDesc.isEmpty())
                    statement.setString(3, groupDesc);
                else
                    statement.setNull(3, NULL);

                statement.setString(4, (String)event.get(Constants.USER));
                statement.setObject(5, OffsetDateTime.parse((String)event.get(CloudEventV1.TIME)));

                int count = statement.executeUpdate();
                if (count == 0) {
                    throw new SQLException("failed to insert group " + (String)map.get("groupId"));
                }
                conn.commit();
                result = Success.of((String)map.get("groupId"));
                insertNotification(event, true, null);
            } catch (SQLException e) {
                logger.error("SQLException:", e);
                conn.rollback();
                insertNotification(event, false, e.getMessage());
                result = Failure.of(new Status(SQL_EXCEPTION, e.getMessage()));
            } catch (Exception e) {
                logger.error("Exception:", e);
                conn.rollback();
                insertNotification(event, false, e.getMessage());
                result = Failure.of(new Status(GENERIC_EXCEPTION, e.getMessage()));
            }
        } catch (SQLException e) {
            logger.error("SQLException:", e);
            result = Failure.of(new Status(SQL_EXCEPTION, e.getMessage()));
        }
        return result;
    }

    @Override
    public Result<String> updateGroup(Map<String, Object> event) {
        final String updateGroup = "UPDATE group_t SET group_desc = ?, update_user = ?, update_ts = ? " +
                "WHERE host_id = ? AND group_id = ?";

        Result<String> result = null;
        Map<String, Object> map = (Map<String, Object>)event.get(PortalConstants.DATA);
        try (Connection conn = ds.getConnection()) {
            conn.setAutoCommit(false);
            try (PreparedStatement statement = conn.prepareStatement(updateGroup)) {
                String groupDesc = (String)map.get("groupDesc");
                if(groupDesc != null && !groupDesc.isEmpty()) {
                    statement.setString(1, groupDesc);
                } else {
                    statement.setNull(1, NULL);
                }
                statement.setString(2, (String)event.get(Constants.USER));
                statement.setObject(3, OffsetDateTime.parse((String)event.get(CloudEventV1.TIME)));
                statement.setString(4, (String)event.get(Constants.HOST));
                statement.setString(5, (String)map.get("groupId"));

                int count = statement.executeUpdate();
                if (count == 0) {
                    throw new SQLException("no record is updated for group " + (String)map.get("groupId"));
                }
                conn.commit();
                result = Success.of((String)map.get("groupId"));
                insertNotification(event, true, null);
            } catch (SQLException e) {
                logger.error("SQLException:", e);
                conn.rollback();
                insertNotification(event, false, e.getMessage());
                result = Failure.of(new Status(SQL_EXCEPTION, e.getMessage()));
            } catch (Exception e) {
                logger.error("Exception:", e);
                conn.rollback();
                insertNotification(event, false, e.getMessage());
                result = Failure.of(new Status(GENERIC_EXCEPTION, e.getMessage()));
            }
        } catch (SQLException e) {
            logger.error("SQLException:", e);
            result = Failure.of(new Status(SQL_EXCEPTION, e.getMessage()));
        }
        return result;

    }

    @Override
    public Result<String> deleteGroup(Map<String, Object> event) {
        final String deleteGroup = "DELETE from group_t WHERE host_id = ? AND group_id = ?";
        Result<String> result;
        Map<String, Object> map = (Map<String, Object>)event.get(PortalConstants.DATA);
        try (Connection conn = ds.getConnection()) {
            conn.setAutoCommit(false);
            try (PreparedStatement statement = conn.prepareStatement(deleteGroup)) {
                statement.setString(1, (String)event.get(Constants.HOST));
                statement.setString(2, (String)map.get("groupId"));
                int count = statement.executeUpdate();
                if (count == 0) {
                    throw new SQLException("no record is deleted for group " + (String)map.get("groupId"));
                }
                conn.commit();
                result = Success.of((String)event.get(Constants.USER));
                insertNotification(event, true, null);
            } catch (SQLException e) {
                logger.error("SQLException:", e);
                conn.rollback();
                insertNotification(event, false, e.getMessage());
                result = Failure.of(new Status(SQL_EXCEPTION, e.getMessage()));
            } catch (Exception e) {
                logger.error("Exception:", e);
                conn.rollback();
                insertNotification(event, false, e.getMessage());
                result = Failure.of(new Status(GENERIC_EXCEPTION, e.getMessage()));
            }
        } catch (SQLException e) {
            logger.error("SQLException:", e);
            result = Failure.of(new Status(SQL_EXCEPTION, e.getMessage()));
        }
        return result;
    }

    @Override
    public Result<String> queryGroup(int offset, int limit, String hostId, String groupId, String groupDesc) {
        Result<String> result;
        StringBuilder sqlBuilder = new StringBuilder();
        sqlBuilder.append("SELECT COUNT(*) OVER () AS total, host_id, group_id, group_desc, update_user, update_ts " +
                "FROM group_t " +
                "WHERE host_id = ?\n");
        List<Object> parameters = new ArrayList<>();
        parameters.add(UUID.fromString(hostId));

        StringBuilder whereClause = new StringBuilder();
        addCondition(whereClause, parameters, "group_id", groupId);
        addCondition(whereClause, parameters, "group_desc", groupDesc);


        if (!whereClause.isEmpty()) {
            sqlBuilder.append("AND ").append(whereClause);
        }

        sqlBuilder.append(" ORDER BY group_id\n" +
                "LIMIT ? OFFSET ?");
        parameters.add(limit);
        parameters.add(offset);

        String sql = sqlBuilder.toString();
        int total = 0;
        List<Map<String, Object>> groups = new ArrayList<>();

        try (final Connection conn = ds.getConnection(); PreparedStatement preparedStatement = conn.prepareStatement(sql)) {
            for (int i = 0; i < parameters.size(); i++) {
                preparedStatement.setObject(i + 1, parameters.get(i));
            }
            boolean isFirstRow = true;
            try (ResultSet resultSet = preparedStatement.executeQuery()) {
                while (resultSet.next()) {
                    Map<String, Object> map = new HashMap<>();
                    // only get the total once as it is the same for all rows.
                    if (isFirstRow) {
                        total = resultSet.getInt("total");
                        isFirstRow = false;
                    }
                    map.put("hostId", resultSet.getObject("host_id", UUID.class));
                    map.put("groupId", resultSet.getString("group_id"));
                    map.put("groupDesc", resultSet.getString("group_desc"));
                    map.put("updateUser", resultSet.getString("update_user"));
                    map.put("updateTs", resultSet.getObject("update_ts") != null ? resultSet.getObject("update_ts", OffsetDateTime.class) : null);
                    groups.add(map);
                }
            }
            Map<String, Object> resultMap = new HashMap<>();
            resultMap.put("total", total);
            resultMap.put("groups", groups);
            result = Success.of(JsonMapper.toJson(resultMap));
        } catch (SQLException e) {
            logger.error("SQLException:", e);
            result = Failure.of(new Status(SQL_EXCEPTION, e.getMessage()));
        } catch (Exception e) {
            logger.error("Exception:", e);
            result = Failure.of(new Status(GENERIC_EXCEPTION, e.getMessage()));
        }
        return result;
    }

    @Override
    public Result<String> queryGroupLabel(String hostId) {
        final String sql = "SELECT group_id from group_t WHERE host_id = ?";
        Result<String> result;
        try (final Connection conn = ds.getConnection()) {
            List<Map<String, Object>> list = new ArrayList<>();
            try (PreparedStatement statement = conn.prepareStatement(sql)) {
                statement.setObject(1, UUID.fromString(hostId));
                try (ResultSet resultSet = statement.executeQuery()) {
                    while (resultSet.next()) {
                        Map<String, Object> map = new HashMap<>();
                        String id = resultSet.getString("group_id");
                        map.put("id", id);
                        map.put("label", id);
                        list.add(map);
                    }
                }
            }
            if (list.isEmpty())
                result = Failure.of(new Status(OBJECT_NOT_FOUND, "group", hostId));
            else
                result = Success.of(JsonMapper.toJson(list));
        } catch (SQLException e) {
            logger.error("SQLException:", e);
            result = Failure.of(new Status(SQL_EXCEPTION, e.getMessage()));
        } catch (Exception e) {
            logger.error("Exception:", e);
            result = Failure.of(new Status(GENERIC_EXCEPTION, e.getMessage()));
        }
        return result;
    }

    @Override
    public Result<String> queryGroupPermission(int offset, int limit, String hostId, String groupId, String apiId, String apiVersion, String endpoint) {
        Result<String> result;
        StringBuilder sqlBuilder = new StringBuilder();
        sqlBuilder.append("SELECT COUNT(*) OVER () AS total, \n" +
                "g.host_id, g.group_id, p.api_id, p.api_version, p.endpoint\n" +
                "FROM group_t g, group_permission_t p\n" +
                "WHERE g.group_id = p.group_id\n" +
                "AND g.host_id = ?\n");


        List<Object> parameters = new ArrayList<>();
        parameters.add(UUID.fromString(hostId));


        StringBuilder whereClause = new StringBuilder();

        addCondition(whereClause, parameters, "g.group_id", groupId);
        addCondition(whereClause, parameters, "p.api_id", apiId);
        addCondition(whereClause, parameters, "p.api_version", apiVersion);
        addCondition(whereClause, parameters, "p.endpoint", endpoint);

        if (!whereClause.isEmpty()) {
            sqlBuilder.append("AND ").append(whereClause);
        }

        sqlBuilder.append(" ORDER BY g.group_id, p.api_id, p.api_version, p.endpoint\n" +
                "LIMIT ? OFFSET ?");

        parameters.add(limit);
        parameters.add(offset);

        String sql = sqlBuilder.toString();
        if(logger.isTraceEnabled()) logger.trace("queryGroupPermission sql: {}", sql);
        int total = 0;
        List<Map<String, Object>> groupPermissions = new ArrayList<>();


        try (final Connection conn = ds.getConnection(); PreparedStatement preparedStatement = conn.prepareStatement(sql)) {
            for (int i = 0; i < parameters.size(); i++) {
                preparedStatement.setObject(i + 1, parameters.get(i));
            }
            boolean isFirstRow = true;
            try (ResultSet resultSet = preparedStatement.executeQuery()) {
                while (resultSet.next()) {
                    Map<String, Object> map = new HashMap<>();
                    // only get the total once as it is the same for all rows.
                    if (isFirstRow) {
                        total = resultSet.getInt("total");
                        isFirstRow = false;
                    }
                    map.put("hostId", resultSet.getObject("host_id", UUID.class));
                    map.put("groupId", resultSet.getString("group_id"));
                    map.put("apiId", resultSet.getString("api_id"));
                    map.put("apiVersion", resultSet.getString("api_version"));
                    map.put("endpoint", resultSet.getString("endpoint"));
                    groupPermissions.add(map);
                }
            }
            Map<String, Object> resultMap = new HashMap<>();
            resultMap.put("total", total);
            resultMap.put("groupPermissions", groupPermissions);
            result = Success.of(JsonMapper.toJson(resultMap));

        } catch (SQLException e) {
            logger.error("SQLException:", e);
            result = Failure.of(new Status(SQL_EXCEPTION, e.getMessage()));
        } catch (Exception e) {
            logger.error("Exception:", e);
            result = Failure.of(new Status(GENERIC_EXCEPTION, e.getMessage()));
        }

        return result;

    }

    @Override
    public Result<String> queryGroupUser(int offset, int limit, String hostId, String groupId, String userId, String entityId, String email, String firstName, String lastName, String userType) {
        Result<String> result;
        StringBuilder sqlBuilder = new StringBuilder();
        sqlBuilder.append("SELECT COUNT(*) OVER () AS total, \n" +
                "g.host_id, g.group_id, g.start_ts, g.end_ts, \n" +
                "u.user_id, u.email, u.user_type, \n" +
                "CASE\n" +
                "    WHEN u.user_type = 'C' THEN c.customer_id\n" +
                "    WHEN u.user_type = 'E' THEN e.employee_id\n" +
                "    ELSE NULL -- Handle other cases if needed\n" +
                "END AS entity_id,\n" +
                "e.manager_id, u.first_name, u.last_name\n" +
                "FROM user_t u\n" +
                "LEFT JOIN\n" +
                "    customer_t c ON u.user_id = c.user_id AND u.user_type = 'C'\n" +
                "LEFT JOIN\n" +
                "    employee_t e ON u.user_id = e.user_id AND u.user_type = 'E'\n" +
                "INNER JOIN\n" +
                "    group_user_t g ON g.user_id = u.user_id\n" +
                "AND g.host_id = ?\n");

        List<Object> parameters = new ArrayList<>();
        parameters.add(UUID.fromString(hostId));


        StringBuilder whereClause = new StringBuilder();

        addCondition(whereClause, parameters, "g.group_id", groupId);
        addCondition(whereClause, parameters, "u.user_id", userId != null ? UUID.fromString(userId) : null);
        addCondition(whereClause, parameters, "entity_id", entityId);
        addCondition(whereClause, parameters, "u.email", email);
        addCondition(whereClause, parameters, "u.first_name", firstName);
        addCondition(whereClause, parameters, "u.last_name", lastName);
        addCondition(whereClause, parameters, "u.user_type", userType);

        if (!whereClause.isEmpty()) {
            sqlBuilder.append("AND ").append(whereClause);
        }

        sqlBuilder.append(" ORDER BY g.group_id, u.user_id\n" +
                "LIMIT ? OFFSET ?");

        parameters.add(limit);
        parameters.add(offset);

        String sql = sqlBuilder.toString();
        if(logger.isTraceEnabled()) logger.trace("queryGroupUser sql: {}", sql);
        int total = 0;
        List<Map<String, Object>> groupUsers = new ArrayList<>();


        try (final Connection conn = ds.getConnection(); PreparedStatement preparedStatement = conn.prepareStatement(sql)) {
            for (int i = 0; i < parameters.size(); i++) {
                preparedStatement.setObject(i + 1, parameters.get(i));
            }
            boolean isFirstRow = true;
            try (ResultSet resultSet = preparedStatement.executeQuery()) {
                while (resultSet.next()) {
                    Map<String, Object> map = new HashMap<>();
                    // only get the total once as it is the same for all rows.
                    if (isFirstRow) {
                        total = resultSet.getInt("total");
                        isFirstRow = false;
                    }
                    map.put("hostId", resultSet.getObject("host_id", UUID.class));
                    map.put("groupId", resultSet.getString("group_id"));
                    map.put("startTs", resultSet.getTimestamp("start_ts"));
                    map.put("endTs", resultSet.getTimestamp("end_ts"));
                    map.put("userId", resultSet.getObject("user_id", UUID.class));
                    map.put("entityId", resultSet.getString("entity_id"));
                    map.put("email", resultSet.getString("email"));
                    map.put("firstName", resultSet.getString("first_name"));
                    map.put("lastName", resultSet.getString("last_name"));
                    map.put("userType", resultSet.getString("user_type"));
                    groupUsers.add(map);
                }
            }
            Map<String, Object> resultMap = new HashMap<>();
            resultMap.put("total", total);
            resultMap.put("groupUsers", groupUsers);
            result = Success.of(JsonMapper.toJson(resultMap));

        } catch (SQLException e) {
            logger.error("SQLException:", e);
            result = Failure.of(new Status(SQL_EXCEPTION, e.getMessage()));
        } catch (Exception e) {
            logger.error("Exception:", e);
            result = Failure.of(new Status(GENERIC_EXCEPTION, e.getMessage()));
        }

        return result;

    }

    @Override
    public Result<String> createGroupPermission(Map<String, Object> event) {
        final String insertGroup = "INSERT INTO group_permission_t (host_id, group_id, api_id, api_version, endpoint, update_user, update_ts) " +
                "VALUES (?, ?, ?, ?, ?,  ?, ?)";

        Result<String> result = null;
        Map<String, Object> map = (Map<String, Object>)event.get(PortalConstants.DATA);
        try (Connection conn = ds.getConnection()) {
            conn.setAutoCommit(false);
            // no duplicate record, insert the user into database and write a success notification.
            try (PreparedStatement statement = conn.prepareStatement(insertGroup)) {
                statement.setString(1, (String)event.get(Constants.HOST));
                statement.setString(2, (String)map.get("groupId"));
                statement.setString(3, (String)map.get("apiId"));
                statement.setString(4, (String)map.get("apiVersion"));
                statement.setString(5, (String)map.get("endpoint"));
                statement.setString(6, (String)event.get(Constants.USER));
                statement.setObject(7, OffsetDateTime.parse((String)event.get(CloudEventV1.TIME)));

                int count = statement.executeUpdate();
                if (count == 0) {
                    throw new SQLException("failed to insert group permission " + (String)map.get("groupId"));
                }
                conn.commit();
                result = Success.of((String)map.get("groupId"));
                insertNotification(event, true, null);
            } catch (SQLException e) {
                logger.error("SQLException:", e);
                conn.rollback();
                insertNotification(event, false, e.getMessage());
                result = Failure.of(new Status(SQL_EXCEPTION, e.getMessage()));
            } catch (Exception e) {
                logger.error("Exception:", e);
                conn.rollback();
                insertNotification(event, false, e.getMessage());
                result = Failure.of(new Status(GENERIC_EXCEPTION, e.getMessage()));
            }
        } catch (SQLException e) {
            logger.error("SQLException:", e);
            result = Failure.of(new Status(SQL_EXCEPTION, e.getMessage()));
        }
        return result;
    }
    @Override
    public Result<String> deleteGroupPermission(Map<String, Object> event) {
        final String deleteGroup = "DELETE from group_permission_t WHERE host_id = ? AND group_id = ? AND api_id = ? AND api_version = ? AND endpoint = ?";
        Result<String> result;
        Map<String, Object> map = (Map<String, Object>)event.get(PortalConstants.DATA);
        try (Connection conn = ds.getConnection()) {
            conn.setAutoCommit(false);
            try (PreparedStatement statement = conn.prepareStatement(deleteGroup)) {
                statement.setString(1, (String)event.get(Constants.HOST));
                statement.setString(2, (String)map.get("groupId"));
                statement.setString(3, (String)map.get("apiId"));
                statement.setString(4, (String)map.get("apiVersion"));
                statement.setString(5, (String)map.get("endpoint"));

                int count = statement.executeUpdate();
                if (count == 0) {
                    throw new SQLException("no record is deleted for group permission " + (String)map.get("groupId"));
                }
                conn.commit();
                result = Success.of((String)event.get(Constants.USER));
                insertNotification(event, true, null);
            } catch (SQLException e) {
                logger.error("SQLException:", e);
                conn.rollback();
                insertNotification(event, false, e.getMessage());
                result = Failure.of(new Status(SQL_EXCEPTION, e.getMessage()));
            } catch (Exception e) {
                logger.error("Exception:", e);
                conn.rollback();
                insertNotification(event, false, e.getMessage());
                result = Failure.of(new Status(GENERIC_EXCEPTION, e.getMessage()));
            }
        } catch (SQLException e) {
            logger.error("SQLException:", e);
            result = Failure.of(new Status(SQL_EXCEPTION, e.getMessage()));
        }
        return result;

    }
    @Override
    public Result<String> createGroupUser(Map<String, Object> event) {
        final String insertGroup = "INSERT INTO group_user_t (host_id, group_id, user_id, start_ts, end_ts, update_user, update_ts) " +
                "VALUES (?, ?, ?, ?, ?,  ?, ?)";

        Result<String> result = null;
        Map<String, Object> map = (Map<String, Object>)event.get(PortalConstants.DATA);
        try (Connection conn = ds.getConnection()) {
            conn.setAutoCommit(false);
            // no duplicate record, insert the user into database and write a success notification.
            try (PreparedStatement statement = conn.prepareStatement(insertGroup)) {
                statement.setString(1, (String)event.get(Constants.HOST));
                statement.setString(2, (String)map.get("groupId"));
                statement.setString(3, (String)event.get(Constants.USER));
                String startTs = (String)map.get("startTs");
                if(startTs != null && !startTs.isEmpty())
                    statement.setObject(4, OffsetDateTime.parse(startTs));
                else
                    statement.setNull(4, NULL);
                String endTs = (String)map.get("endTs");
                if (endTs != null && !endTs.isEmpty()) {
                    statement.setObject(5, OffsetDateTime.parse(endTs));
                } else {
                    statement.setNull(5, NULL);
                }
                statement.setString(6, (String)event.get(Constants.USER));
                statement.setObject(7, OffsetDateTime.parse((String)event.get(CloudEventV1.TIME)));

                int count = statement.executeUpdate();
                if (count == 0) {
                    throw new SQLException("failed to insert group user " + (String)map.get("groupId"));
                }
                conn.commit();
                result = Success.of((String)map.get("groupId"));
                insertNotification(event, true, null);
            } catch (SQLException e) {
                logger.error("SQLException:", e);
                conn.rollback();
                insertNotification(event, false, e.getMessage());
                result = Failure.of(new Status(SQL_EXCEPTION, e.getMessage()));
            } catch (Exception e) {
                logger.error("Exception:", e);
                conn.rollback();
                insertNotification(event, false, e.getMessage());
                result = Failure.of(new Status(GENERIC_EXCEPTION, e.getMessage()));
            }
        } catch (SQLException e) {
            logger.error("SQLException:", e);
            result = Failure.of(new Status(SQL_EXCEPTION, e.getMessage()));
        }
        return result;

    }
    @Override
    public Result<String> updateGroupUser(Map<String, Object> event) {
        final String updateGroup = "UPDATE group_user_t SET start_ts = ?, end_ts = ?, update_user = ?, update_ts = ? " +
                "WHERE host_id = ? AND group_id = ? AND user_id = ?";

        Result<String> result = null;
        Map<String, Object> map = (Map<String, Object>)event.get(PortalConstants.DATA);
        try (Connection conn = ds.getConnection()) {
            conn.setAutoCommit(false);

            try (PreparedStatement statement = conn.prepareStatement(updateGroup)) {
                String startTs = (String)map.get("startTs");
                if(startTs != null && !startTs.isEmpty())
                    statement.setObject(1, OffsetDateTime.parse(startTs));
                else
                    statement.setNull(1, NULL);
                String endTs = (String)map.get("endTs");
                if (endTs != null && !endTs.isEmpty()) {
                    statement.setObject(2, OffsetDateTime.parse(endTs));
                } else {
                    statement.setNull(2, NULL);
                }
                statement.setString(3, (String)event.get(Constants.USER));
                statement.setObject(4, OffsetDateTime.parse((String)event.get(CloudEventV1.TIME)));
                statement.setString(5, (String)event.get(Constants.HOST));
                statement.setString(6, (String)map.get("groupId"));
                statement.setString(7, (String)event.get(Constants.USER));

                int count = statement.executeUpdate();
                if (count == 0) {
                    throw new SQLException("no record is updated for group user " + (String)map.get("groupId"));
                }
                conn.commit();
                result = Success.of((String)map.get("groupId"));
                insertNotification(event, true, null);
            } catch (SQLException e) {
                logger.error("SQLException:", e);
                conn.rollback();
                insertNotification(event, false, e.getMessage());
                result = Failure.of(new Status(SQL_EXCEPTION, e.getMessage()));
            } catch (Exception e) {
                logger.error("Exception:", e);
                conn.rollback();
                insertNotification(event, false, e.getMessage());
                result = Failure.of(new Status(GENERIC_EXCEPTION, e.getMessage()));
            }
        } catch (SQLException e) {
            logger.error("SQLException:", e);
            result = Failure.of(new Status(SQL_EXCEPTION, e.getMessage()));
        }
        return result;

    }
    @Override
    public Result<String> deleteGroupUser(Map<String, Object> event) {
        final String deleteGroup = "DELETE from group_user_t WHERE host_id = ? AND group_id = ? AND user_id = ?";
        Result<String> result;
        Map<String, Object> map = (Map<String, Object>)event.get(PortalConstants.DATA);
        try (Connection conn = ds.getConnection()) {
            conn.setAutoCommit(false);
            try (PreparedStatement statement = conn.prepareStatement(deleteGroup)) {
                statement.setString(1, (String)event.get(Constants.HOST));
                statement.setString(2, (String)map.get("groupId"));
                statement.setString(3, (String)event.get(Constants.USER));

                int count = statement.executeUpdate();
                if (count == 0) {
                    throw new SQLException("no record is deleted for group user " + (String)map.get("groupId"));
                }
                conn.commit();
                result = Success.of((String)event.get(Constants.USER));
                insertNotification(event, true, null);
            } catch (SQLException e) {
                logger.error("SQLException:", e);
                conn.rollback();
                insertNotification(event, false, e.getMessage());
                result = Failure.of(new Status(SQL_EXCEPTION, e.getMessage()));
            } catch (Exception e) {
                logger.error("Exception:", e);
                conn.rollback();
                insertNotification(event, false, e.getMessage());
                result = Failure.of(new Status(GENERIC_EXCEPTION, e.getMessage()));
            }
        } catch (SQLException e) {
            logger.error("SQLException:", e);
            result = Failure.of(new Status(SQL_EXCEPTION, e.getMessage()));
        }
        return result;

    }

    @Override
    public Result<String> queryGroupRowFilter(int offset, int limit, String hostId, String GroupId, String apiId, String apiVersion, String endpoint) {
        Result<String> result;
        StringBuilder sqlBuilder = new StringBuilder();
        sqlBuilder.append("SELECT COUNT(*) OVER () AS total, \n" +
                "g.host_id, g.group_id, p.api_id, p.api_version, p.endpoint, p.col_name, p.operator, p.col_value\n" +
                "FROM group_t g, group_row_filter_t p\n" +
                "WHERE g.group_id = p.group_id\n" +
                "AND g.host_id = ?\n");

        List<Object> parameters = new ArrayList<>();
        parameters.add(UUID.fromString(hostId));

        StringBuilder whereClause = new StringBuilder();

        addCondition(whereClause, parameters, "g.group_id", GroupId);
        addCondition(whereClause, parameters, "p.api_id", apiId);
        addCondition(whereClause, parameters, "p.api_version", apiVersion);
        addCondition(whereClause, parameters, "p.endpoint", endpoint);

        if (!whereClause.isEmpty()) {
            sqlBuilder.append("AND ").append(whereClause);
        }

        sqlBuilder.append(" ORDER BY g.group_id, p.api_id, p.api_version, p.endpoint\n" +
                "LIMIT ? OFFSET ?");

        parameters.add(limit);
        parameters.add(offset);

        String sql = sqlBuilder.toString();
        if(logger.isTraceEnabled()) logger.trace("queryGroupRowFilter sql: {}", sql);
        int total = 0;
        List<Map<String, Object>> groupRowFilters = new ArrayList<>();

        try (final Connection conn = ds.getConnection(); PreparedStatement preparedStatement = conn.prepareStatement(sql)) {
            for (int i = 0; i < parameters.size(); i++) {
                preparedStatement.setObject(i + 1, parameters.get(i));
            }
            boolean isFirstRow = true;
            try (ResultSet resultSet = preparedStatement.executeQuery()) {
                while (resultSet.next()) {
                    Map<String, Object> map = new HashMap<>();
                    // only get the total once as it is the same for all rows.
                    if (isFirstRow) {
                        total = resultSet.getInt("total");
                        isFirstRow = false;
                    }
                    map.put("hostId", resultSet.getObject("host_id", UUID.class));
                    map.put("groupId", resultSet.getString("group_id"));
                    map.put("apiId", resultSet.getString("api_id"));
                    map.put("apiVersion", resultSet.getString("api_version"));
                    map.put("endpoint", resultSet.getString("endpoint"));
                    map.put("colName", resultSet.getString("col_name"));
                    map.put("operator", resultSet.getString("operator"));
                    map.put("colValue", resultSet.getString("col_value"));
                    groupRowFilters.add(map);
                }
            }
            Map<String, Object> resultMap = new HashMap<>();
            resultMap.put("total", total);
            resultMap.put("groupRowFilters", groupRowFilters);
            result = Success.of(JsonMapper.toJson(resultMap));

        } catch (SQLException e) {
            logger.error("SQLException:", e);
            result = Failure.of(new Status(SQL_EXCEPTION, e.getMessage()));
        } catch (Exception e) {
            logger.error("Exception:", e);
            result = Failure.of(new Status(GENERIC_EXCEPTION, e.getMessage()));
        }

        return result;
    }

    @Override
    public Result<String> createGroupRowFilter(Map<String, Object> event) {
        final String insertGroup = "INSERT INTO group_row_filter_t (host_id, group_id, api_id, api_version, endpoint, col_name, operator, col_value, update_user, update_ts) " +
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";

        Result<String> result = null;
        Map<String, Object> map = (Map<String, Object>)event.get(PortalConstants.DATA);
        try (Connection conn = ds.getConnection()) {
            conn.setAutoCommit(false);
            // no duplicate record, insert the user into database and write a success notification.
            try (PreparedStatement statement = conn.prepareStatement(insertGroup)) {
                statement.setString(1, (String)event.get(Constants.HOST));
                statement.setString(2, (String)map.get("groupId"));
                statement.setString(3, (String)map.get("apiId"));
                statement.setString(4, (String)map.get("apiVersion"));
                statement.setString(5, (String)map.get("endpoint"));
                statement.setString(6, (String)map.get("colName"));
                statement.setString(7, (String)map.get("operator"));
                statement.setString(8, (String)map.get("colValue"));
                statement.setString(9, (String)event.get(Constants.USER));
                statement.setObject(10, OffsetDateTime.parse((String)event.get(CloudEventV1.TIME)));

                int count = statement.executeUpdate();
                if (count == 0) {
                    throw new SQLException("failed to insert group row filter " + (String)map.get("groupId"));
                }
                conn.commit();
                result = Success.of((String)map.get("groupId"));
                insertNotification(event, true, null);
            } catch (SQLException e) {
                logger.error("SQLException:", e);
                conn.rollback();
                insertNotification(event, false, e.getMessage());
                result = Failure.of(new Status(SQL_EXCEPTION, e.getMessage()));
            } catch (Exception e) {
                logger.error("Exception:", e);
                conn.rollback();
                insertNotification(event, false, e.getMessage());
                result = Failure.of(new Status(GENERIC_EXCEPTION, e.getMessage()));
            }
        } catch (SQLException e) {
            logger.error("SQLException:", e);
            result = Failure.of(new Status(SQL_EXCEPTION, e.getMessage()));
        }
        return result;
    }

    @Override
    public Result<String> updateGroupRowFilter(Map<String, Object> event) {
        final String updateGroup = "UPDATE group_row_filter_t SET operator = ?, col_value = ?, update_user = ?, update_ts = ? " +
                "WHERE host_id = ? AND group_id = ? AND api_id = ? AND api_version = ? AND endpoint = ? AND col_name = ?";

        Result<String> result = null;
        Map<String, Object> map = (Map<String, Object>)event.get(PortalConstants.DATA);
        try (Connection conn = ds.getConnection()) {
            conn.setAutoCommit(false);

            try (PreparedStatement statement = conn.prepareStatement(updateGroup)) {
                statement.setString(1, (String)map.get("operator"));
                statement.setString(2, (String)map.get("colValue"));
                statement.setString(3, (String)event.get(Constants.USER));
                statement.setObject(4, OffsetDateTime.parse((String)event.get(CloudEventV1.TIME)));
                statement.setString(5, (String)event.get(Constants.HOST));
                statement.setString(6, (String)map.get("groupId"));
                statement.setString(7, (String)map.get("apiId"));
                statement.setString(8, (String)map.get("apiVersion"));
                statement.setString(9, (String)map.get("endpoint"));
                statement.setString(10, (String)map.get("colName"));

                int count = statement.executeUpdate();
                if (count == 0) {
                    throw new SQLException("no record is updated for group row filter " + (String)map.get("groupId"));
                }
                conn.commit();
                result = Success.of((String)map.get("groupId"));
                insertNotification(event, true, null);
            } catch (SQLException e) {
                logger.error("SQLException:", e);
                conn.rollback();
                insertNotification(event, false, e.getMessage());
                result = Failure.of(new Status(SQL_EXCEPTION, e.getMessage()));
            } catch (Exception e) {
                logger.error("Exception:", e);
                conn.rollback();
                insertNotification(event, false, e.getMessage());
                result = Failure.of(new Status(GENERIC_EXCEPTION, e.getMessage()));
            }
        } catch (SQLException e) {
            logger.error("SQLException:", e);
            result = Failure.of(new Status(SQL_EXCEPTION, e.getMessage()));
        }
        return result;
    }

    @Override
    public Result<String> deleteGroupRowFilter(Map<String, Object> event) {
        final String deleteGroup = "DELETE from group_row_filter_t WHERE host_id = ? AND group_id = ? AND api_id = ? AND api_version = ? AND endpoint = ? AND col_name = ?";
        Result<String> result;
        Map<String, Object> map = (Map<String, Object>)event.get(PortalConstants.DATA);
        try (Connection conn = ds.getConnection()) {
            conn.setAutoCommit(false);
            try (PreparedStatement statement = conn.prepareStatement(deleteGroup)) {
                statement.setString(1, (String)event.get(Constants.HOST));
                statement.setString(2, (String)map.get("groupId"));
                statement.setString(3, (String)map.get("apiId"));
                statement.setString(4, (String)map.get("apiVersion"));
                statement.setString(5, (String)map.get("endpoint"));
                statement.setString(6, (String)map.get("colName"));

                int count = statement.executeUpdate();
                if (count == 0) {
                    throw new SQLException("no record is deleted for group row filter " + (String)map.get("groupId"));
                }
                conn.commit();
                result = Success.of((String)event.get(Constants.USER));
                insertNotification(event, true, null);
            } catch (SQLException e) {
                logger.error("SQLException:", e);
                conn.rollback();
                insertNotification(event, false, e.getMessage());
                result = Failure.of(new Status(SQL_EXCEPTION, e.getMessage()));
            } catch (Exception e) {
                logger.error("Exception:", e);
                conn.rollback();
                insertNotification(event, false, e.getMessage());
                result = Failure.of(new Status(GENERIC_EXCEPTION, e.getMessage()));
            }
        } catch (SQLException e) {
            logger.error("SQLException:", e);
            result = Failure.of(new Status(SQL_EXCEPTION, e.getMessage()));
        }
        return result;
    }

    @Override
    public Result<String> queryGroupColFilter(int offset, int limit, String hostId, String GroupId, String apiId, String apiVersion, String endpoint) {
        Result<String> result;
        StringBuilder sqlBuilder = new StringBuilder();
        sqlBuilder.append("SELECT COUNT(*) OVER () AS total, \n" +
                "g.host_id, g.group_id, p.api_id, p.api_version, p.endpoint, p.columns\n" +
                "FROM group_t g, group_col_filter_t p\n" +
                "WHERE g.group_id = p.group_id\n" +
                "AND g.host_id = ?\n");

        List<Object> parameters = new ArrayList<>();
        parameters.add(UUID.fromString(hostId));

        StringBuilder whereClause = new StringBuilder();

        addCondition(whereClause, parameters, "g.group_id", GroupId);
        addCondition(whereClause, parameters, "p.api_id", apiId);
        addCondition(whereClause, parameters, "p.api_version", apiVersion);
        addCondition(whereClause, parameters, "p.endpoint", endpoint);

        if (!whereClause.isEmpty()) {
            sqlBuilder.append("AND ").append(whereClause);
        }

        sqlBuilder.append(" ORDER BY g.group_id, p.api_id, p.api_version, p.endpoint\n" +
                "LIMIT ? OFFSET ?");

        parameters.add(limit);
        parameters.add(offset);

        String sql = sqlBuilder.toString();
        if(logger.isTraceEnabled()) logger.trace("queryGroupColFilter sql: {}", sql);
        int total = 0;
        List<Map<String, Object>> groupColFilters = new ArrayList<>();

        try (final Connection conn = ds.getConnection(); PreparedStatement preparedStatement = conn.prepareStatement(sql)) {
            for (int i = 0; i < parameters.size(); i++) {
                preparedStatement.setObject(i + 1, parameters.get(i));
            }
            boolean isFirstRow = true;
            try (ResultSet resultSet = preparedStatement.executeQuery()) {
                while (resultSet.next()) {
                    Map<String, Object> map = new HashMap<>();
                    // only get the total once as it is the same for all rows.
                    if (isFirstRow) {
                        total = resultSet.getInt("total");
                        isFirstRow = false;
                    }
                    map.put("hostId", resultSet.getObject("host_id", UUID.class));
                    map.put("groupId", resultSet.getString("group_id"));
                    map.put("apiId", resultSet.getString("api_id"));
                    map.put("apiVersion", resultSet.getString("api_version"));
                    map.put("endpoint", resultSet.getString("endpoint"));
                    map.put("columns", resultSet.getString("columns"));
                    groupColFilters.add(map);
                }
            }
            Map<String, Object> resultMap = new HashMap<>();
            resultMap.put("total", total);
            resultMap.put("groupColFilters", groupColFilters);
            result = Success.of(JsonMapper.toJson(resultMap));

        } catch (SQLException e) {
            logger.error("SQLException:", e);
            result = Failure.of(new Status(SQL_EXCEPTION, e.getMessage()));
        } catch (Exception e) {
            logger.error("Exception:", e);
            result = Failure.of(new Status(GENERIC_EXCEPTION, e.getMessage()));
        }

        return result;
    }

    @Override
    public Result<String> createGroupColFilter(Map<String, Object> event) {
        final String insertGroup = "INSERT INTO group_col_filter_t (host_id, group_id, api_id, api_version, endpoint, columns, update_user, update_ts) " +
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?)";

        Result<String> result = null;
        Map<String, Object> map = (Map<String, Object>)event.get(PortalConstants.DATA);
        try (Connection conn = ds.getConnection()) {
            conn.setAutoCommit(false);
            // no duplicate record, insert the user into database and write a success notification.
            try (PreparedStatement statement = conn.prepareStatement(insertGroup)) {
                statement.setString(1, (String)event.get(Constants.HOST));
                statement.setString(2, (String)map.get("groupId"));
                statement.setString(3, (String)map.get("apiId"));
                statement.setString(4, (String)map.get("apiVersion"));
                statement.setString(5, (String)map.get("endpoint"));
                statement.setString(6, (String)map.get("columns"));
                statement.setString(7, (String)event.get(Constants.USER));
                statement.setObject(8, OffsetDateTime.parse((String)event.get(CloudEventV1.TIME)));

                int count = statement.executeUpdate();
                if (count == 0) {
                    throw new SQLException("failed to insert group col filter " + (String)map.get("groupId"));
                }
                conn.commit();
                result = Success.of((String)map.get("groupId"));
                insertNotification(event, true, null);
            } catch (SQLException e) {
                logger.error("SQLException:", e);
                conn.rollback();
                insertNotification(event, false, e.getMessage());
                result = Failure.of(new Status(SQL_EXCEPTION, e.getMessage()));
            } catch (Exception e) {
                logger.error("Exception:", e);
                conn.rollback();
                insertNotification(event, false, e.getMessage());
                result = Failure.of(new Status(GENERIC_EXCEPTION, e.getMessage()));
            }
        } catch (SQLException e) {
            logger.error("SQLException:", e);
            result = Failure.of(new Status(SQL_EXCEPTION, e.getMessage()));
        }
        return result;
    }

    @Override
    public Result<String> updateGroupColFilter(Map<String, Object> event) {
        final String updateGroup = "UPDATE group_col_filter_t SET columns = ?, update_user = ?, update_ts = ? " +
                "WHERE host_id = ? AND group_id = ? AND api_id = ? AND api_version = ? AND endpoint = ?";

        Result<String> result = null;
        Map<String, Object> map = (Map<String, Object>)event.get(PortalConstants.DATA);
        try (Connection conn = ds.getConnection()) {
            conn.setAutoCommit(false);

            try (PreparedStatement statement = conn.prepareStatement(updateGroup)) {
                statement.setString(1, (String)map.get("columns"));
                statement.setString(2, (String)event.get(Constants.USER));
                statement.setObject(3, OffsetDateTime.parse((String)event.get(CloudEventV1.TIME)));
                statement.setString(4, (String)event.get(Constants.HOST));
                statement.setString(5, (String)map.get("groupId"));
                statement.setString(6, (String)map.get("apiId"));
                statement.setString(7, (String)map.get("apiVersion"));
                statement.setString(8, (String)map.get("endpoint"));

                int count = statement.executeUpdate();
                if (count == 0) {
                    throw new SQLException("no record is updated for group col filter " + (String)map.get("groupId"));
                }
                conn.commit();
                result = Success.of((String)map.get("groupId"));
                insertNotification(event, true, null);
            } catch (SQLException e) {
                logger.error("SQLException:", e);
                conn.rollback();
                insertNotification(event, false, e.getMessage());
                result = Failure.of(new Status(SQL_EXCEPTION, e.getMessage()));
            } catch (Exception e) {
                logger.error("Exception:", e);
                conn.rollback();
                insertNotification(event, false, e.getMessage());
                result = Failure.of(new Status(GENERIC_EXCEPTION, e.getMessage()));
            }
        } catch (SQLException e) {
            logger.error("SQLException:", e);
            result = Failure.of(new Status(SQL_EXCEPTION, e.getMessage()));
        }
        return result;
    }

    @Override
    public Result<String> deleteGroupColFilter(Map<String, Object> event) {
        final String deleteGroup = "DELETE from group_col_filter_t WHERE host_id = ? AND group_id = ? AND api_id = ? AND api_version = ? AND endpoint = ?";
        Result<String> result;
        Map<String, Object> map = (Map<String, Object>)event.get(PortalConstants.DATA);
        try (Connection conn = ds.getConnection()) {
            conn.setAutoCommit(false);
            try (PreparedStatement statement = conn.prepareStatement(deleteGroup)) {
                statement.setString(1, (String)event.get(Constants.HOST));
                statement.setString(2, (String)map.get("groupId"));
                statement.setString(3, (String)map.get("apiId"));
                statement.setString(4, (String)map.get("apiVersion"));
                statement.setString(5, (String)map.get("endpoint"));

                int count = statement.executeUpdate();
                if (count == 0) {
                    throw new SQLException("no record is deleted for group col filter " + (String)map.get("groupId"));
                }
                conn.commit();
                result = Success.of((String)event.get(Constants.USER));
                insertNotification(event, true, null);
            } catch (SQLException e) {
                logger.error("SQLException:", e);
                conn.rollback();
                insertNotification(event, false, e.getMessage());
                result = Failure.of(new Status(SQL_EXCEPTION, e.getMessage()));
            } catch (Exception e) {
                logger.error("Exception:", e);
                conn.rollback();
                insertNotification(event, false, e.getMessage());
                result = Failure.of(new Status(GENERIC_EXCEPTION, e.getMessage()));
            }
        } catch (SQLException e) {
            logger.error("SQLException:", e);
            result = Failure.of(new Status(SQL_EXCEPTION, e.getMessage()));
        }
        return result;
    }


    @Override
    public Result<String> createPosition(Map<String, Object> event) {
        final String insertPosition = "INSERT INTO position_t (host_id, position_id, position_desc, " +
                "inherit_to_ancestor, inherit_to_sibling, update_user, update_ts) " +
                "VALUES (?, ?, ?, ?, ?, ?, ?)";

        Result<String> result = null;
        Map<String, Object> map = (Map<String, Object>)event.get(PortalConstants.DATA);
        try (Connection conn = ds.getConnection()) {
            conn.setAutoCommit(false);
            try (PreparedStatement statement = conn.prepareStatement(insertPosition)) {
                statement.setString(1, (String)event.get(Constants.HOST));
                statement.setString(2, (String)map.get("positionId"));
                String positionDesc = (String)map.get("positionDesc");
                if (positionDesc != null && !positionDesc.isEmpty())
                    statement.setString(3, positionDesc);
                else
                    statement.setNull(3, NULL);
                String inheritToAncestor = (String)map.get("inheritToAncestor");
                if(inheritToAncestor != null && !inheritToAncestor.isEmpty())
                    statement.setString(4, inheritToAncestor);
                else
                    statement.setNull(4, NULL);
                String inheritToSibling = (String)map.get("inheritToSibling");
                if(inheritToSibling != null && !inheritToSibling.isEmpty())
                    statement.setString(5, inheritToSibling);
                else
                    statement.setNull(5, NULL);

                statement.setString(6, (String)event.get(Constants.USER));
                statement.setObject(7, OffsetDateTime.parse((String)event.get(CloudEventV1.TIME)));

                int count = statement.executeUpdate();
                if (count == 0) {
                    throw new SQLException("failed to insert position " + (String)map.get("positionId"));
                }
                conn.commit();
                result = Success.of((String)map.get("positionId"));
                insertNotification(event, true, null);
            } catch (SQLException e) {
                logger.error("SQLException:", e);
                conn.rollback();
                insertNotification(event, false, e.getMessage());
                result = Failure.of(new Status(SQL_EXCEPTION, e.getMessage()));
            } catch (Exception e) {
                logger.error("Exception:", e);
                conn.rollback();
                insertNotification(event, false, e.getMessage());
                result = Failure.of(new Status(GENERIC_EXCEPTION, e.getMessage()));
            }
        } catch (SQLException e) {
            logger.error("SQLException:", e);
            result = Failure.of(new Status(SQL_EXCEPTION, e.getMessage()));
        }
        return result;
    }

    @Override
    public Result<String> updatePosition(Map<String, Object> event) {
        final String updatePosition = "UPDATE position_t SET position_desc = ?, inherit_to_ancestor = ?, inherit_to_sibling = ?, " +
                "update_user = ?, update_ts = ? " +
                "WHERE host_id = ? AND position_id = ?";

        Result<String> result = null;
        Map<String, Object> map = (Map<String, Object>)event.get(PortalConstants.DATA);
        try (Connection conn = ds.getConnection()) {
            conn.setAutoCommit(false);
            try (PreparedStatement statement = conn.prepareStatement(updatePosition)) {
                String positionDesc = (String)map.get("positionDesc");
                if(positionDesc != null && !positionDesc.isEmpty()) {
                    statement.setString(1, positionDesc);
                } else {
                    statement.setNull(1, NULL);
                }
                String inheritToAncestor = (String)map.get("inheritToAncestor");
                if(inheritToAncestor != null && !inheritToAncestor.isEmpty()) {
                    statement.setString(2, inheritToAncestor);
                } else {
                    statement.setNull(2, NULL);
                }
                String inheritToSibling = (String)map.get("inheritToSibling");
                if(inheritToSibling != null && !inheritToSibling.isEmpty()) {
                    statement.setString(3, inheritToSibling);
                } else {
                    statement.setNull(3, NULL);
                }
                statement.setString(4, (String)event.get(Constants.USER));
                statement.setObject(5, OffsetDateTime.parse((String)event.get(CloudEventV1.TIME)));

                statement.setString(6, (String)event.get(Constants.HOST));
                statement.setString(7, (String)map.get("positionId"));

                int count = statement.executeUpdate();
                if (count == 0) {
                    throw new SQLException("no record is updated for position " + (String)map.get("positionId"));
                }
                conn.commit();
                result = Success.of((String)map.get("positionId"));
                insertNotification(event, true, null);
            } catch (SQLException e) {
                logger.error("SQLException:", e);
                conn.rollback();
                insertNotification(event, false, e.getMessage());
                result = Failure.of(new Status(SQL_EXCEPTION, e.getMessage()));
            } catch (Exception e) {
                logger.error("Exception:", e);
                conn.rollback();
                insertNotification(event, false, e.getMessage());
                result = Failure.of(new Status(GENERIC_EXCEPTION, e.getMessage()));
            }
        } catch (SQLException e) {
            logger.error("SQLException:", e);
            result = Failure.of(new Status(SQL_EXCEPTION, e.getMessage()));
        }
        return result;

    }

    @Override
    public Result<String> deletePosition(Map<String, Object> event) {
        final String deleteGroup = "DELETE from position_t WHERE host_id = ? AND position_id = ?";
        Result<String> result;
        Map<String, Object> map = (Map<String, Object>)event.get(PortalConstants.DATA);
        try (Connection conn = ds.getConnection()) {
            conn.setAutoCommit(false);
            try (PreparedStatement statement = conn.prepareStatement(deleteGroup)) {
                statement.setString(1, (String)event.get(Constants.HOST));
                statement.setString(2, (String)map.get("positionId"));
                int count = statement.executeUpdate();
                if (count == 0) {
                    throw new SQLException("no record is deleted for position " + (String)map.get("positionId"));
                }
                conn.commit();
                result = Success.of((String)event.get(Constants.USER));
                insertNotification(event, true, null);
            } catch (SQLException e) {
                logger.error("SQLException:", e);
                conn.rollback();
                insertNotification(event, false, e.getMessage());
                result = Failure.of(new Status(SQL_EXCEPTION, e.getMessage()));
            } catch (Exception e) {
                logger.error("Exception:", e);
                conn.rollback();
                insertNotification(event, false, e.getMessage());
                result = Failure.of(new Status(GENERIC_EXCEPTION, e.getMessage()));
            }
        } catch (SQLException e) {
            logger.error("SQLException:", e);
            result = Failure.of(new Status(SQL_EXCEPTION, e.getMessage()));
        }
        return result;
    }


    @Override
    public Result<String> queryPosition(int offset, int limit, String hostId, String positionId, String positionDesc, String inheritToAncestor, String inheritToSibling) {
        Result<String> result;
        StringBuilder sqlBuilder = new StringBuilder();
        sqlBuilder.append("SELECT COUNT(*) OVER () AS total, host_id, position_id, position_desc, inherit_to_ancestor, inherit_to_sibling, update_user, update_ts " +
                "FROM position_t " +
                "WHERE host_id = ?\n");
        List<Object> parameters = new ArrayList<>();
        parameters.add(UUID.fromString(hostId));

        StringBuilder whereClause = new StringBuilder();
        addCondition(whereClause, parameters, "position_id", positionId);
        addCondition(whereClause, parameters, "position_desc", positionDesc);
        addCondition(whereClause, parameters, "inherit_to_ancestor", inheritToAncestor);
        addCondition(whereClause, parameters, "inherit_to_sibling", inheritToSibling);


        if (whereClause.length() > 0) {
            sqlBuilder.append("AND ").append(whereClause);
        }
        sqlBuilder.append(" ORDER BY position_id\n" +
                "LIMIT ? OFFSET ?");
        parameters.add(limit);
        parameters.add(offset);

        String sql = sqlBuilder.toString();

        int total = 0;
        List<Map<String, Object>> positions = new ArrayList<>();

        try (final Connection conn = ds.getConnection(); PreparedStatement preparedStatement = conn.prepareStatement(sql)) {
            for (int i = 0; i < parameters.size(); i++) {
                preparedStatement.setObject(i + 1, parameters.get(i));
            }
            boolean isFirstRow = true;

            try (ResultSet resultSet = preparedStatement.executeQuery()) {
                while (resultSet.next()) {
                    Map<String, Object> map = new HashMap<>();
                    // only get the total once as it is the same for all rows.
                    if (isFirstRow) {
                        total = resultSet.getInt("total");
                        isFirstRow = false;
                    }
                    map.put("hostId", resultSet.getObject("host_id", UUID.class));
                    map.put("positionId", resultSet.getString("position_id"));
                    map.put("positionDesc", resultSet.getString("position_desc"));
                    map.put("inheritToAncestor", resultSet.getString("inherit_to_ancestor"));
                    map.put("inheritToSibling", resultSet.getString("inherit_to_sibling"));
                    map.put("updateUser", resultSet.getString("update_user"));
                    map.put("updateTs", resultSet.getObject("update_ts") != null ? resultSet.getObject("update_ts", OffsetDateTime.class) : null);
                    positions.add(map);
                }
            }
            Map<String, Object> resultMap = new HashMap<>();
            resultMap.put("total", total);
            resultMap.put("positions", positions);
            result = Success.of(JsonMapper.toJson(resultMap));
        } catch (SQLException e) {
            logger.error("SQLException:", e);
            result = Failure.of(new Status(SQL_EXCEPTION, e.getMessage()));
        } catch (Exception e) {
            logger.error("Exception:", e);
            result = Failure.of(new Status(GENERIC_EXCEPTION, e.getMessage()));
        }
        return result;
    }

    @Override
    public Result<String> queryPositionLabel(String hostId) {
        final String sql = "SELECT position_id from position_t WHERE host_id = ?";
        Result<String> result;
        try (final Connection conn = ds.getConnection()) {
            List<Map<String, Object>> list = new ArrayList<>();
            try (PreparedStatement statement = conn.prepareStatement(sql)) {
                statement.setObject(1, UUID.fromString(hostId));
                try (ResultSet resultSet = statement.executeQuery()) {
                    while (resultSet.next()) {
                        Map<String, Object> map = new HashMap<>();
                        String id = resultSet.getString("position_id");
                        map.put("id", id);
                        map.put("label", id);
                        list.add(map);
                    }
                }
            }
            if (list.isEmpty())
                result = Failure.of(new Status(OBJECT_NOT_FOUND, "position", hostId));
            else
                result = Success.of(JsonMapper.toJson(list));
        } catch (SQLException e) {
            logger.error("SQLException:", e);
            result = Failure.of(new Status(SQL_EXCEPTION, e.getMessage()));
        } catch (Exception e) {
            logger.error("Exception:", e);
            result = Failure.of(new Status(GENERIC_EXCEPTION, e.getMessage()));
        }
        return result;
    }

    @Override
    public Result<String> queryPositionPermission(int offset, int limit, String hostId, String positionId, String inheritToAncestor, String inheritToSibling, String apiId, String apiVersion, String endpoint) {
        Result<String> result;
        StringBuilder sqlBuilder = new StringBuilder();
        sqlBuilder.append("SELECT COUNT(*) OVER () AS total, \n" +
                "o.host_id, o.position_id, o.inherit_to_ancestor, o.inherit_to_sibling, " +
                "p.api_id, p.api_version, p.endpoint\n" +
                "FROM position_t o, position_permission_t p\n" +
                "WHERE o.position_id = p.position_id\n" +
                "AND o.host_id = ?\n");


        List<Object> parameters = new ArrayList<>();
        parameters.add(UUID.fromString(hostId));


        StringBuilder whereClause = new StringBuilder();

        addCondition(whereClause, parameters, "o.position_id", positionId);
        addCondition(whereClause, parameters, "o.inherit_to_ancestor", inheritToAncestor);
        addCondition(whereClause, parameters, "o.inherit_to_sibling", inheritToSibling);
        addCondition(whereClause, parameters, "p.api_id", apiId);
        addCondition(whereClause, parameters, "p.api_version", apiVersion);
        addCondition(whereClause, parameters, "p.endpoint", endpoint);

        if (whereClause.length() > 0) {
            sqlBuilder.append("AND ").append(whereClause);
        }

        sqlBuilder.append(" ORDER BY o.position_id, p.api_id, p.api_version, p.endpoint\n" +
                "LIMIT ? OFFSET ?");

        parameters.add(limit);
        parameters.add(offset);

        String sql = sqlBuilder.toString();
        if(logger.isTraceEnabled()) logger.trace("queryPositionPermission sql: {}", sql);
        int total = 0;
        List<Map<String, Object>> positionPermissions = new ArrayList<>();


        try (final Connection conn = ds.getConnection(); PreparedStatement preparedStatement = conn.prepareStatement(sql)) {
            for (int i = 0; i < parameters.size(); i++) {
                preparedStatement.setObject(i + 1, parameters.get(i));
            }
            boolean isFirstRow = true;
            try (ResultSet resultSet = preparedStatement.executeQuery()) {
                while (resultSet.next()) {
                    Map<String, Object> map = new HashMap<>();
                    // only get the total once as it is the same for all rows.
                    if (isFirstRow) {
                        total = resultSet.getInt("total");
                        isFirstRow = false;
                    }
                    map.put("hostId", resultSet.getObject("host_id", UUID.class));
                    map.put("positionId", resultSet.getString("position_id"));
                    map.put("inheritToAncestor", resultSet.getString("inherit_to_ancestor"));
                    map.put("inheritToSibling", resultSet.getString("inherit_to_sibling"));
                    map.put("apiId", resultSet.getString("api_id"));
                    map.put("apiVersion", resultSet.getString("api_version"));
                    map.put("endpoint", resultSet.getString("endpoint"));
                    positionPermissions.add(map);
                }
            }
            Map<String, Object> resultMap = new HashMap<>();
            resultMap.put("total", total);
            resultMap.put("positionPermissions", positionPermissions);
            result = Success.of(JsonMapper.toJson(resultMap));

        } catch (SQLException e) {
            logger.error("SQLException:", e);
            result = Failure.of(new Status(SQL_EXCEPTION, e.getMessage()));
        } catch (Exception e) {
            logger.error("Exception:", e);
            result = Failure.of(new Status(GENERIC_EXCEPTION, e.getMessage()));
        }

        return result;

    }

    @Override
    public Result<String> queryPositionUser(int offset, int limit, String hostId, String positionId, String positionType, String inheritToAncestor, String inheritToSibling, String userId, String entityId, String email, String firstName, String lastName, String userType) {
        Result<String> result;
        StringBuilder sqlBuilder = new StringBuilder();
        sqlBuilder.append("SELECT COUNT(*) OVER () AS total, \n" +
                "ep.host_id, ep.position_id, ep.position_type, \n " +
                "ep.start_ts, ep.end_ts, u.user_id, \n" +
                "u.email, u.user_type, e.employee_id AS entity_id,\n" +
                "e.manager_id, u.first_name, u.last_name\n" +
                "FROM user_t u\n" +
                "INNER JOIN\n" +
                "    employee_t e ON u.user_id = e.user_id AND u.user_type = 'E'\n" +
                "INNER JOIN\n" +
                "    employee_position_t ep ON ep.employee_id = e.employee_id\n" +
                "AND ep.host_id = ?\n");

        List<Object> parameters = new ArrayList<>();
        parameters.add(UUID.fromString(hostId));


        StringBuilder whereClause = new StringBuilder();

        addCondition(whereClause, parameters, "ep.position_id", positionId);
        addCondition(whereClause, parameters, "ep.position_type", positionType);
        addCondition(whereClause, parameters, "u.user_id", userId != null ? UUID.fromString(userId) : null);
        addCondition(whereClause, parameters, "entity_id", entityId);
        addCondition(whereClause, parameters, "u.email", email);
        addCondition(whereClause, parameters, "u.first_name", firstName);
        addCondition(whereClause, parameters, "u.last_name", lastName);
        addCondition(whereClause, parameters, "u.user_type", userType);

        if (!whereClause.isEmpty()) {
            sqlBuilder.append("AND ").append(whereClause);
        }

        sqlBuilder.append(" ORDER BY ep.position_id, u.user_id\n" +
                "LIMIT ? OFFSET ?");

        parameters.add(limit);
        parameters.add(offset);

        String sql = sqlBuilder.toString();
        if(logger.isTraceEnabled()) logger.trace("queryPositionUser sql: {}", sql);
        int total = 0;
        List<Map<String, Object>> positionUsers = new ArrayList<>();


        try (final Connection conn = ds.getConnection(); PreparedStatement preparedStatement = conn.prepareStatement(sql)) {
            for (int i = 0; i < parameters.size(); i++) {
                preparedStatement.setObject(i + 1, parameters.get(i));
            }
            boolean isFirstRow = true;
            try (ResultSet resultSet = preparedStatement.executeQuery()) {
                while (resultSet.next()) {
                    Map<String, Object> map = new HashMap<>();
                    // only get the total once as it is the same for all rows.
                    if (isFirstRow) {
                        total = resultSet.getInt("total");
                        isFirstRow = false;
                    }
                    map.put("hostId", resultSet.getObject("host_id", UUID.class));
                    map.put("positionId", resultSet.getString("position_id"));
                    map.put("positionType", resultSet.getString("position_type"));
                    map.put("startTs", resultSet.getDate("start_ts"));
                    map.put("endTs", resultSet.getString("end_ts"));
                    map.put("userId", resultSet.getObject("user_id", UUID.class));
                    map.put("entityId", resultSet.getString("entity_id"));
                    map.put("email", resultSet.getString("email"));
                    map.put("firstName", resultSet.getString("first_name"));
                    map.put("lastName", resultSet.getString("last_name"));
                    map.put("userType", resultSet.getString("user_type"));
                    positionUsers.add(map);
                }
            }
            Map<String, Object> resultMap = new HashMap<>();
            resultMap.put("total", total);
            resultMap.put("positionUsers", positionUsers);
            result = Success.of(JsonMapper.toJson(resultMap));

        } catch (SQLException e) {
            logger.error("SQLException:", e);
            result = Failure.of(new Status(SQL_EXCEPTION, e.getMessage()));
        } catch (Exception e) {
            logger.error("Exception:", e);
            result = Failure.of(new Status(GENERIC_EXCEPTION, e.getMessage()));
        }

        return result;

    }
    @Override
    public Result<String> createPositionPermission(Map<String, Object> event) {
        final String insertGroup = "INSERT INTO position_permission_t (host_id, position_id, api_id, api_version, endpoint, update_user, update_ts) " +
                "VALUES (?, ?, ?, ?, ?,  ?, ?)";

        Result<String> result = null;
        Map<String, Object> map = (Map<String, Object>)event.get(PortalConstants.DATA);
        try (Connection conn = ds.getConnection()) {
            conn.setAutoCommit(false);
            // no duplicate record, insert the user into database and write a success notification.
            try (PreparedStatement statement = conn.prepareStatement(insertGroup)) {
                statement.setString(1, (String)event.get(Constants.HOST));
                statement.setString(2, (String)map.get("positionId"));
                statement.setString(3, (String)map.get("apiId"));
                statement.setString(4, (String)map.get("apiVersion"));
                statement.setString(5, (String)map.get("endpoint"));
                statement.setString(6, (String)event.get(Constants.USER));
                statement.setObject(7, OffsetDateTime.parse((String)event.get(CloudEventV1.TIME)));

                int count = statement.executeUpdate();
                if (count == 0) {
                    throw new SQLException("failed to insert position permission " + (String)map.get("positionId"));
                }
                conn.commit();
                result = Success.of((String)map.get("positionId"));
                insertNotification(event, true, null);
            } catch (SQLException e) {
                logger.error("SQLException:", e);
                conn.rollback();
                insertNotification(event, false, e.getMessage());
                result = Failure.of(new Status(SQL_EXCEPTION, e.getMessage()));
            } catch (Exception e) {
                logger.error("Exception:", e);
                conn.rollback();
                insertNotification(event, false, e.getMessage());
                result = Failure.of(new Status(GENERIC_EXCEPTION, e.getMessage()));
            }
        } catch (SQLException e) {
            logger.error("SQLException:", e);
            result = Failure.of(new Status(SQL_EXCEPTION, e.getMessage()));
        }
        return result;

    }

    @Override
    public Result<String> deletePositionPermission(Map<String, Object> event) {
        final String deleteGroup = "DELETE from position_permission_t WHERE host_id = ? AND position_id = ? AND api_id = ? AND api_version = ? AND endpoint = ?";
        Result<String> result;
        Map<String, Object> map = (Map<String, Object>)event.get(PortalConstants.DATA);
        try (Connection conn = ds.getConnection()) {
            conn.setAutoCommit(false);
            try (PreparedStatement statement = conn.prepareStatement(deleteGroup)) {
                statement.setString(1, (String)event.get(Constants.HOST));
                statement.setString(2, (String)map.get("positionId"));
                statement.setString(3, (String)map.get("apiId"));
                statement.setString(4, (String)map.get("apiVersion"));
                statement.setString(5, (String)map.get("endpoint"));

                int count = statement.executeUpdate();
                if (count == 0) {
                    throw new SQLException("no record is deleted for position permission " + (String)map.get("positionId"));
                }
                conn.commit();
                result = Success.of((String)event.get(Constants.USER));
                insertNotification(event, true, null);
            } catch (SQLException e) {
                logger.error("SQLException:", e);
                conn.rollback();
                insertNotification(event, false, e.getMessage());
                result = Failure.of(new Status(SQL_EXCEPTION, e.getMessage()));
            } catch (Exception e) {
                logger.error("Exception:", e);
                conn.rollback();
                insertNotification(event, false, e.getMessage());
                result = Failure.of(new Status(GENERIC_EXCEPTION, e.getMessage()));
            }
        } catch (SQLException e) {
            logger.error("SQLException:", e);
            result = Failure.of(new Status(SQL_EXCEPTION, e.getMessage()));
        }
        return result;
    }

    @Override
    public Result<String> createPositionUser(Map<String, Object> event) {
        final String insertGroup = "INSERT INTO position_user_t (host_id, position_id, user_id, start_ts, end_ts, update_user, update_ts) " +
                "VALUES (?, ?, ?, ?, ?,  ?, ?)";

        Result<String> result = null;
        Map<String, Object> map = (Map<String, Object>)event.get(PortalConstants.DATA);
        try (Connection conn = ds.getConnection()) {
            conn.setAutoCommit(false);
            // no duplicate record, insert the user into database and write a success notification.
            try (PreparedStatement statement = conn.prepareStatement(insertGroup)) {
                statement.setString(1, (String)event.get(Constants.HOST));
                statement.setString(2, (String)map.get("positionId"));
                statement.setString(3, (String)event.get(Constants.USER));
                String startTs = (String)map.get("startTs");
                if(startTs != null && !startTs.isEmpty())
                    statement.setObject(4, OffsetDateTime.parse(startTs));
                else
                    statement.setNull(4, NULL);
                String endTs = (String)map.get("endTs");
                if (endTs != null && !endTs.isEmpty()) {
                    statement.setObject(5, OffsetDateTime.parse(endTs));
                } else {
                    statement.setNull(5, NULL);
                }
                statement.setString(6, (String)event.get(Constants.USER));
                statement.setObject(7, OffsetDateTime.parse((String)event.get(CloudEventV1.TIME)));

                int count = statement.executeUpdate();
                if (count == 0) {
                    throw new SQLException("failed to insert position user " + map.get("positionId"));
                }
                conn.commit();
                result = Success.of((String)map.get("positionId"));
                insertNotification(event, true, null);
            } catch (SQLException e) {
                logger.error("SQLException:", e);
                conn.rollback();
                insertNotification(event, false, e.getMessage());
                result = Failure.of(new Status(SQL_EXCEPTION, e.getMessage()));
            } catch (Exception e) {
                logger.error("Exception:", e);
                conn.rollback();
                insertNotification(event, false, e.getMessage());
                result = Failure.of(new Status(GENERIC_EXCEPTION, e.getMessage()));
            }
        } catch (SQLException e) {
            logger.error("SQLException:", e);
            result = Failure.of(new Status(SQL_EXCEPTION, e.getMessage()));
        }
        return result;

    }

    @Override
    public Result<String> updatePositionUser(Map<String, Object> event) {
        final String updateGroup = "UPDATE position_user_t SET start_ts = ?, end_ts = ?, update_user = ?, update_ts = ? " +
                "WHERE host_id = ? AND position_id = ? AND user_id = ?";

        Result<String> result = null;
        Map<String, Object> map = (Map<String, Object>)event.get(PortalConstants.DATA);
        try (Connection conn = ds.getConnection()) {
            conn.setAutoCommit(false);

            try (PreparedStatement statement = conn.prepareStatement(updateGroup)) {
                String startTs = (String)map.get("startTs");
                if(startTs != null && !startTs.isEmpty())
                    statement.setObject(1, OffsetDateTime.parse(startTs));
                else
                    statement.setNull(1, NULL);
                String endTs = (String)map.get("endTs");
                if (endTs != null && !endTs.isEmpty()) {
                    statement.setObject(2, OffsetDateTime.parse(endTs));
                } else {
                    statement.setNull(2, NULL);
                }
                statement.setString(3, (String)event.get(Constants.USER));
                statement.setObject(4, OffsetDateTime.parse((String)event.get(CloudEventV1.TIME)));
                statement.setString(5, (String)event.get(Constants.HOST));
                statement.setString(6, (String)map.get("positionId"));
                statement.setString(7, (String)event.get(Constants.USER));

                int count = statement.executeUpdate();
                if (count == 0) {
                    throw new SQLException("no record is updated for position user " + (String)map.get("positionId"));
                }
                conn.commit();
                result = Success.of((String)map.get("positionId"));
                insertNotification(event, true, null);
            } catch (SQLException e) {
                logger.error("SQLException:", e);
                conn.rollback();
                insertNotification(event, false, e.getMessage());
                result = Failure.of(new Status(SQL_EXCEPTION, e.getMessage()));
            } catch (Exception e) {
                logger.error("Exception:", e);
                conn.rollback();
                insertNotification(event, false, e.getMessage());
                result = Failure.of(new Status(GENERIC_EXCEPTION, e.getMessage()));
            }
        } catch (SQLException e) {
            logger.error("SQLException:", e);
            result = Failure.of(new Status(SQL_EXCEPTION, e.getMessage()));
        }
        return result;

    }

    @Override
    public Result<String> deletePositionUser(Map<String, Object> event) {
        final String deleteGroup = "DELETE from position_user_t WHERE host_id = ? AND position_id = ? AND user_id = ?";
        Result<String> result;
        Map<String, Object> map = (Map<String, Object>)event.get(PortalConstants.DATA);
        try (Connection conn = ds.getConnection()) {
            conn.setAutoCommit(false);
            try (PreparedStatement statement = conn.prepareStatement(deleteGroup)) {
                statement.setString(1, (String)event.get(Constants.HOST));
                statement.setString(2, (String)map.get("positionId"));
                statement.setString(3, (String)event.get(Constants.USER));

                int count = statement.executeUpdate();
                if (count == 0) {
                    throw new SQLException("no record is deleted for position user " + (String)map.get("positionId"));
                }
                conn.commit();
                result = Success.of((String)event.get(Constants.USER));
                insertNotification(event, true, null);
            } catch (SQLException e) {
                logger.error("SQLException:", e);
                conn.rollback();
                insertNotification(event, false, e.getMessage());
                result = Failure.of(new Status(SQL_EXCEPTION, e.getMessage()));
            } catch (Exception e) {
                logger.error("Exception:", e);
                conn.rollback();
                insertNotification(event, false, e.getMessage());
                result = Failure.of(new Status(GENERIC_EXCEPTION, e.getMessage()));
            }
        } catch (SQLException e) {
            logger.error("SQLException:", e);
            result = Failure.of(new Status(SQL_EXCEPTION, e.getMessage()));
        }
        return result;

    }

    @Override
    public Result<String> queryPositionRowFilter(int offset, int limit, String hostId, String PositionId, String apiId, String apiVersion, String endpoint) {
        Result<String> result;
        StringBuilder sqlBuilder = new StringBuilder();
        sqlBuilder.append("SELECT COUNT(*) OVER () AS total, \n" +
                "o.host_id, o.position_id, p.api_id, p.api_version, p.endpoint, p.col_name, p.operator, p.col_value\n" +
                "FROM position_t o, position_row_filter_t p\n" +
                "WHERE o.position_id = p.position_id\n" +
                "AND o.host_id = ?\n");

        List<Object> parameters = new ArrayList<>();
        parameters.add(UUID.fromString(hostId));

        StringBuilder whereClause = new StringBuilder();

        addCondition(whereClause, parameters, "o.position_id", PositionId);
        addCondition(whereClause, parameters, "p.api_id", apiId);
        addCondition(whereClause, parameters, "p.api_version", apiVersion);
        addCondition(whereClause, parameters, "p.endpoint", endpoint);

        if (whereClause.length() > 0) {
            sqlBuilder.append("AND ").append(whereClause);
        }

        sqlBuilder.append(" ORDER BY o.position_id, p.api_id, p.api_version, p.endpoint\n" +
                "LIMIT ? OFFSET ?");

        parameters.add(limit);
        parameters.add(offset);

        String sql = sqlBuilder.toString();
        if(logger.isTraceEnabled()) logger.trace("queryPositionRowFilter sql: {}", sql);
        int total = 0;
        List<Map<String, Object>> positionRowFilters = new ArrayList<>();

        try (final Connection conn = ds.getConnection(); PreparedStatement preparedStatement = conn.prepareStatement(sql)) {
            for (int i = 0; i < parameters.size(); i++) {
                preparedStatement.setObject(i + 1, parameters.get(i));
            }
            boolean isFirstRow = true;
            try (ResultSet resultSet = preparedStatement.executeQuery()) {
                while (resultSet.next()) {
                    Map<String, Object> map = new HashMap<>();
                    // only get the total once as it is the same for all rows.
                    if (isFirstRow) {
                        total = resultSet.getInt("total");
                        isFirstRow = false;
                    }
                    map.put("hostId", resultSet.getObject("host_id", UUID.class));
                    map.put("positionId", resultSet.getString("position_id"));
                    map.put("apiId", resultSet.getString("api_id"));
                    map.put("apiVersion", resultSet.getString("api_version"));
                    map.put("endpoint", resultSet.getString("endpoint"));
                    map.put("colName", resultSet.getString("col_name"));
                    map.put("operator", resultSet.getString("operator"));
                    map.put("colValue", resultSet.getString("col_value"));
                    positionRowFilters.add(map);
                }
            }
            Map<String, Object> resultMap = new HashMap<>();
            resultMap.put("total", total);
            resultMap.put("positionRowFilters", positionRowFilters);
            result = Success.of(JsonMapper.toJson(resultMap));

        } catch (SQLException e) {
            logger.error("SQLException:", e);
            result = Failure.of(new Status(SQL_EXCEPTION, e.getMessage()));
        } catch (Exception e) {
            logger.error("Exception:", e);
            result = Failure.of(new Status(GENERIC_EXCEPTION, e.getMessage()));
        }
        return result;
    }

    @Override
    public Result<String> createPositionRowFilter(Map<String, Object> event) {
        final String insertGroup = "INSERT INTO position_row_filter_t (host_id, position_id, api_id, api_version, endpoint, col_name, operator, col_value, update_user, update_ts) " +
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";

        Result<String> result = null;
        Map<String, Object> map = (Map<String, Object>)event.get(PortalConstants.DATA);
        try (Connection conn = ds.getConnection()) {
            conn.setAutoCommit(false);
            // no duplicate record, insert the user into database and write a success notification.
            try (PreparedStatement statement = conn.prepareStatement(insertGroup)) {
                statement.setString(1, (String)event.get(Constants.HOST));
                statement.setString(2, (String)map.get("positionId"));
                statement.setString(3, (String)map.get("apiId"));
                statement.setString(4, (String)map.get("apiVersion"));
                statement.setString(5, (String)map.get("endpoint"));
                statement.setString(6, (String)map.get("colName"));
                statement.setString(7, (String)map.get("operator"));
                statement.setString(8, (String)map.get("colValue"));
                statement.setString(9, (String)event.get(Constants.USER));
                statement.setObject(10, OffsetDateTime.parse((String)event.get(CloudEventV1.TIME)));

                int count = statement.executeUpdate();
                if (count == 0) {
                    throw new SQLException("failed to insert position row filter " + (String)map.get("positionId"));
                }
                conn.commit();
                result = Success.of((String)map.get("positionId"));
                insertNotification(event, true, null);
            } catch (SQLException e) {
                logger.error("SQLException:", e);
                conn.rollback();
                insertNotification(event, false, e.getMessage());
                result = Failure.of(new Status(SQL_EXCEPTION, e.getMessage()));
            } catch (Exception e) {
                logger.error("Exception:", e);
                conn.rollback();
                insertNotification(event, false, e.getMessage());
                result = Failure.of(new Status(GENERIC_EXCEPTION, e.getMessage()));
            }
        } catch (SQLException e) {
            logger.error("SQLException:", e);
            result = Failure.of(new Status(SQL_EXCEPTION, e.getMessage()));
        }
        return result;
    }

    @Override
    public Result<String> updatePositionRowFilter(Map<String, Object> event) {
        final String updateGroup = "UPDATE position_row_filter_t SET operator = ?, col_value = ?, update_user = ?, update_ts = ? " +
                "WHERE host_id = ? AND position_id = ? AND api_id = ? AND api_version = ? AND endpoint = ? AND col_name = ?";

        Result<String> result = null;
        Map<String, Object> map = (Map<String, Object>)event.get(PortalConstants.DATA);
        try (Connection conn = ds.getConnection()) {
            conn.setAutoCommit(false);

            try (PreparedStatement statement = conn.prepareStatement(updateGroup)) {
                statement.setString(1, (String)map.get("operator"));
                statement.setString(2, (String)map.get("colValue"));
                statement.setString(3, (String)event.get(Constants.USER));
                statement.setObject(4, OffsetDateTime.parse((String)event.get(CloudEventV1.TIME)));
                statement.setString(5, (String)event.get(Constants.HOST));
                statement.setString(6, (String)map.get("positionId"));
                statement.setString(7, (String)map.get("apiId"));
                statement.setString(8, (String)map.get("apiVersion"));
                statement.setString(9, (String)map.get("endpoint"));
                statement.setString(10, (String)map.get("colName"));

                int count = statement.executeUpdate();
                if (count == 0) {
                    throw new SQLException("no record is updated for position row filter " + (String)map.get("positionId"));
                }
                conn.commit();
                result = Success.of((String)map.get("positionId"));
                insertNotification(event, true, null);
            } catch (SQLException e) {
                logger.error("SQLException:", e);
                conn.rollback();
                insertNotification(event, false, e.getMessage());
                result = Failure.of(new Status(SQL_EXCEPTION, e.getMessage()));
            } catch (Exception e) {
                logger.error("Exception:", e);
                conn.rollback();
                insertNotification(event, false, e.getMessage());
                result = Failure.of(new Status(GENERIC_EXCEPTION, e.getMessage()));
            }
        } catch (SQLException e) {
            logger.error("SQLException:", e);
            result = Failure.of(new Status(SQL_EXCEPTION, e.getMessage()));
        }
        return result;
    }

    @Override
    public Result<String> deletePositionRowFilter(Map<String, Object> event) {
        final String deleteGroup = "DELETE from position_row_filter_t WHERE host_id = ? AND position_id = ? AND api_id = ? AND api_version = ? AND endpoint = ? AND col_name = ?";
        Result<String> result;
        Map<String, Object> map = (Map<String, Object>)event.get(PortalConstants.DATA);
        try (Connection conn = ds.getConnection()) {
            conn.setAutoCommit(false);
            try (PreparedStatement statement = conn.prepareStatement(deleteGroup)) {
                statement.setString(1, (String)event.get(Constants.HOST));
                statement.setString(2, (String)map.get("positionId"));
                statement.setString(3, (String)map.get("apiId"));
                statement.setString(4, (String)map.get("apiVersion"));
                statement.setString(5, (String)map.get("endpoint"));
                statement.setString(6, (String)map.get("colName"));

                int count = statement.executeUpdate();
                if (count == 0) {
                    throw new SQLException("no record is deleted for position row filter " + (String)map.get("positionId"));
                }
                conn.commit();
                result = Success.of((String)event.get(Constants.USER));
                insertNotification(event, true, null);
            } catch (SQLException e) {
                logger.error("SQLException:", e);
                conn.rollback();
                insertNotification(event, false, e.getMessage());
                result = Failure.of(new Status(SQL_EXCEPTION, e.getMessage()));
            } catch (Exception e) {
                logger.error("Exception:", e);
                conn.rollback();
                insertNotification(event, false, e.getMessage());
                result = Failure.of(new Status(GENERIC_EXCEPTION, e.getMessage()));
            }
        } catch (SQLException e) {
            logger.error("SQLException:", e);
            result = Failure.of(new Status(SQL_EXCEPTION, e.getMessage()));
        }
        return result;
    }

    @Override
    public Result<String> queryPositionColFilter(int offset, int limit, String hostId, String PositionId, String apiId, String apiVersion, String endpoint) {
        Result<String> result;
        StringBuilder sqlBuilder = new StringBuilder();
        sqlBuilder.append("SELECT COUNT(*) OVER () AS total, \n" +
                "o.host_id, o.position_id, p.api_id, p.api_version, p.endpoint, p.columns\n" +
                "FROM position_t o, position_col_filter_t p\n" +
                "WHERE o.position_id = p.position_id\n" +
                "AND o.host_id = ?\n");

        List<Object> parameters = new ArrayList<>();
        parameters.add(UUID.fromString(hostId));

        StringBuilder whereClause = new StringBuilder();

        addCondition(whereClause, parameters, "o.position_id", PositionId);
        addCondition(whereClause, parameters, "p.api_id", apiId);
        addCondition(whereClause, parameters, "p.api_version", apiVersion);
        addCondition(whereClause, parameters, "p.endpoint", endpoint);

        if (!whereClause.isEmpty()) {
            sqlBuilder.append("AND ").append(whereClause);
        }

        sqlBuilder.append(" ORDER BY o.position_id, p.api_id, p.api_version, p.endpoint\n" +
                "LIMIT ? OFFSET ?");

        parameters.add(limit);
        parameters.add(offset);

        String sql = sqlBuilder.toString();
        if(logger.isTraceEnabled()) logger.trace("queryPositionColFilter sql: {}", sql);
        int total = 0;
        List<Map<String, Object>> positionColFilters = new ArrayList<>();

        try (final Connection conn = ds.getConnection(); PreparedStatement preparedStatement = conn.prepareStatement(sql)) {
            for (int i = 0; i < parameters.size(); i++) {
                preparedStatement.setObject(i + 1, parameters.get(i));
            }
            boolean isFirstRow = true;
            try (ResultSet resultSet = preparedStatement.executeQuery()) {
                while (resultSet.next()) {
                    Map<String, Object> map = new HashMap<>();
                    // only get the total once as it is the same for all rows.
                    if (isFirstRow) {
                        total = resultSet.getInt("total");
                        isFirstRow = false;
                    }
                    map.put("hostId", resultSet.getObject("host_id", UUID.class));
                    map.put("positionId", resultSet.getString("position_id"));
                    map.put("apiId", resultSet.getString("api_id"));
                    map.put("apiVersion", resultSet.getString("api_version"));
                    map.put("endpoint", resultSet.getString("endpoint"));
                    map.put("columns", resultSet.getString("columns"));
                    positionColFilters.add(map);
                }
            }
            Map<String, Object> resultMap = new HashMap<>();
            resultMap.put("total", total);
            resultMap.put("positionColFilters", positionColFilters);
            result = Success.of(JsonMapper.toJson(resultMap));

        } catch (SQLException e) {
            logger.error("SQLException:", e);
            result = Failure.of(new Status(SQL_EXCEPTION, e.getMessage()));
        } catch (Exception e) {
            logger.error("Exception:", e);
            result = Failure.of(new Status(GENERIC_EXCEPTION, e.getMessage()));
        }
        return result;
    }

    @Override
    public Result<String> createPositionColFilter(Map<String, Object> event) {
        final String insertGroup = "INSERT INTO position_col_filter_t (host_id, position_id, api_id, api_version, endpoint, columns, update_user, update_ts) " +
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?)";

        Result<String> result = null;
        Map<String, Object> map = (Map<String, Object>)event.get(PortalConstants.DATA);
        try (Connection conn = ds.getConnection()) {
            conn.setAutoCommit(false);
            // no duplicate record, insert the user into database and write a success notification.
            try (PreparedStatement statement = conn.prepareStatement(insertGroup)) {
                statement.setString(1, (String)event.get(Constants.HOST));
                statement.setString(2, (String)map.get("positionId"));
                statement.setString(3, (String)map.get("apiId"));
                statement.setString(4, (String)map.get("apiVersion"));
                statement.setString(5, (String)map.get("endpoint"));
                statement.setString(6, (String)map.get("columns"));
                statement.setString(7, (String)event.get(Constants.USER));
                statement.setObject(8, OffsetDateTime.parse((String)event.get(CloudEventV1.TIME)));

                int count = statement.executeUpdate();
                if (count == 0) {
                    throw new SQLException("failed to insert position col filter " + (String)map.get("positionId"));
                }
                conn.commit();
                result = Success.of((String)map.get("positionId"));
                insertNotification(event, true, null);
            } catch (SQLException e) {
                logger.error("SQLException:", e);
                conn.rollback();
                insertNotification(event, false, e.getMessage());
                result = Failure.of(new Status(SQL_EXCEPTION, e.getMessage()));
            } catch (Exception e) {
                logger.error("Exception:", e);
                conn.rollback();
                insertNotification(event, false, e.getMessage());
                result = Failure.of(new Status(GENERIC_EXCEPTION, e.getMessage()));
            }
        } catch (SQLException e) {
            logger.error("SQLException:", e);
            result = Failure.of(new Status(SQL_EXCEPTION, e.getMessage()));
        }
        return result;
    }

    @Override
    public Result<String> updatePositionColFilter(Map<String, Object> event) {
        final String updateGroup = "UPDATE position_col_filter_t SET columns = ?, update_user = ?, update_ts = ? " +
                "WHERE host_id = ? AND position_id = ? AND api_id = ? AND api_version = ? AND endpoint = ?";

        Result<String> result = null;
        Map<String, Object> map = (Map<String, Object>)event.get(PortalConstants.DATA);
        try (Connection conn = ds.getConnection()) {
            conn.setAutoCommit(false);

            try (PreparedStatement statement = conn.prepareStatement(updateGroup)) {
                statement.setString(1, (String)map.get("columns"));
                statement.setString(2, (String)event.get(Constants.USER));
                statement.setObject(3, OffsetDateTime.parse((String)event.get(CloudEventV1.TIME)));
                statement.setString(4, (String)event.get(Constants.HOST));
                statement.setString(5, (String)map.get("positionId"));
                statement.setString(6, (String)map.get("apiId"));
                statement.setString(7, (String)map.get("apiVersion"));
                statement.setString(8, (String)map.get("endpoint"));

                int count = statement.executeUpdate();
                if (count == 0) {
                    throw new SQLException("no record is updated for position col filter " + (String)map.get("positionId"));
                }
                conn.commit();
                result = Success.of((String)map.get("positionId"));
                insertNotification(event, true, null);
            } catch (SQLException e) {
                logger.error("SQLException:", e);
                conn.rollback();
                insertNotification(event, false, e.getMessage());
                result = Failure.of(new Status(SQL_EXCEPTION, e.getMessage()));
            } catch (Exception e) {
                logger.error("Exception:", e);
                conn.rollback();
                insertNotification(event, false, e.getMessage());
                result = Failure.of(new Status(GENERIC_EXCEPTION, e.getMessage()));
            }
        } catch (SQLException e) {
            logger.error("SQLException:", e);
            result = Failure.of(new Status(SQL_EXCEPTION, e.getMessage()));
        }
        return result;
    }

    @Override
    public Result<String> deletePositionColFilter(Map<String, Object> event) {
        final String deleteGroup = "DELETE from position_col_filter_t WHERE host_id = ? AND position_id = ? AND api_id = ? AND api_version = ? AND endpoint = ?";
        Result<String> result;
        Map<String, Object> map = (Map<String, Object>)event.get(PortalConstants.DATA);
        try (Connection conn = ds.getConnection()) {
            conn.setAutoCommit(false);
            try (PreparedStatement statement = conn.prepareStatement(deleteGroup)) {
                statement.setString(1, (String)event.get(Constants.HOST));
                statement.setString(2, (String)map.get("positionId"));
                statement.setString(3, (String)map.get("apiId"));
                statement.setString(4, (String)map.get("apiVersion"));
                statement.setString(5, (String)map.get("endpoint"));

                int count = statement.executeUpdate();
                if (count == 0) {
                    throw new SQLException("no record is deleted for position col filter " + (String)map.get("positionId"));
                }
                conn.commit();
                result = Success.of((String)event.get(Constants.USER));
                insertNotification(event, true, null);
            } catch (SQLException e) {
                logger.error("SQLException:", e);
                conn.rollback();
                insertNotification(event, false, e.getMessage());
                result = Failure.of(new Status(SQL_EXCEPTION, e.getMessage()));
            } catch (Exception e) {
                logger.error("Exception:", e);
                conn.rollback();
                insertNotification(event, false, e.getMessage());
                result = Failure.of(new Status(GENERIC_EXCEPTION, e.getMessage()));
            }
        } catch (SQLException e) {
            logger.error("SQLException:", e);
            result = Failure.of(new Status(SQL_EXCEPTION, e.getMessage()));
        }
        return result;
    }

    @Override
    public Result<String> createAttribute(Map<String, Object> event) {
        final String insertAttribute = "INSERT INTO attribute_t (host_id, attribute_id, attribute_type, " +
                "attribute_desc, update_user, update_ts) " +
                "VALUES (?, ?, ?, ?, ?, ?)";

        Result<String> result = null;
        Map<String, Object> map = (Map<String, Object>)event.get(PortalConstants.DATA);
        try (Connection conn = ds.getConnection()) {
            conn.setAutoCommit(false);
            try (PreparedStatement statement = conn.prepareStatement(insertAttribute)) {
                statement.setString(1, (String)event.get(Constants.HOST));
                statement.setString(2, (String)map.get("attributeId"));
                String attributeType = (String)map.get("attributeType");
                if(attributeType != null && !attributeType.isEmpty())
                    statement.setString(3, attributeType);
                else
                    statement.setNull(3, NULL);
                String attributeDesc = (String)map.get("attributeDesc");
                if (attributeDesc != null && !attributeDesc.isEmpty())
                    statement.setString(4, attributeDesc);
                else
                    statement.setNull(4, NULL);

                statement.setString(5, (String)event.get(Constants.USER));
                statement.setObject(6, OffsetDateTime.parse((String)event.get(CloudEventV1.TIME)));

                int count = statement.executeUpdate();
                if (count == 0) {
                    throw new SQLException("failed to insert attribute " + map.get("attributeId"));
                }
                conn.commit();
                result = Success.of((String)map.get("attributeId"));
                insertNotification(event, true, null);
            } catch (SQLException e) {
                logger.error("SQLException:", e);
                conn.rollback();
                insertNotification(event, false, e.getMessage());
                result = Failure.of(new Status(SQL_EXCEPTION, e.getMessage()));
            } catch (Exception e) {
                logger.error("Exception:", e);
                conn.rollback();
                insertNotification(event, false, e.getMessage());
                result = Failure.of(new Status(GENERIC_EXCEPTION, e.getMessage()));
            }
        } catch (SQLException e) {
            logger.error("SQLException:", e);
            result = Failure.of(new Status(SQL_EXCEPTION, e.getMessage()));
        }
        return result;
    }

    @Override
    public Result<String> updateAttribute(Map<String, Object> event) {
        final String updateAttribute = "UPDATE attribute_t SET attribute_desc = ?, attribute_type = ?," +
                "update_user = ?, update_ts = ? " +
                "WHERE host_id = ? AND attribute_id = ?";

        Result<String> result = null;
        Map<String, Object> map = (Map<String, Object>)event.get(PortalConstants.DATA);
        try (Connection conn = ds.getConnection()) {
            conn.setAutoCommit(false);
            try (PreparedStatement statement = conn.prepareStatement(updateAttribute)) {
                String attributeDesc = (String)map.get("attributeDesc");
                if(attributeDesc != null && !attributeDesc.isEmpty()) {
                    statement.setString(1, attributeDesc);
                } else {
                    statement.setNull(1, NULL);
                }
                String attributeType = (String)map.get("attributeType");
                if(attributeType != null && !attributeType.isEmpty()) {
                    statement.setString(2, attributeType);
                } else {
                    statement.setNull(2, NULL);
                }
                statement.setString(3, (String)event.get(Constants.USER));
                statement.setObject(4, OffsetDateTime.parse((String)event.get(CloudEventV1.TIME)));

                statement.setString(5, (String)event.get(Constants.HOST));
                String attributeId = (String)map.get("attributeId");
                statement.setString(6, attributeId);

                int count = statement.executeUpdate();
                if (count == 0) {
                    throw new SQLException("no record is updated for attribute " + attributeId);
                }
                conn.commit();
                result = Success.of(attributeId);
                insertNotification(event, true, null);
            } catch (SQLException e) {
                logger.error("SQLException:", e);
                conn.rollback();
                insertNotification(event, false, e.getMessage());
                result = Failure.of(new Status(SQL_EXCEPTION, e.getMessage()));
            } catch (Exception e) {
                logger.error("Exception:", e);
                conn.rollback();
                insertNotification(event, false, e.getMessage());
                result = Failure.of(new Status(GENERIC_EXCEPTION, e.getMessage()));
            }
        } catch (SQLException e) {
            logger.error("SQLException:", e);
            result = Failure.of(new Status(SQL_EXCEPTION, e.getMessage()));
        }
        return result;

    }

    @Override
    public Result<String> deleteAttribute(Map<String, Object> event) {
        final String deleteGroup = "DELETE from attribute_t WHERE host_id = ? AND attribute_id = ?";
        Result<String> result;
        Map<String, Object> map = (Map<String, Object>)event.get(PortalConstants.DATA);
        try (Connection conn = ds.getConnection()) {
            conn.setAutoCommit(false);
            try (PreparedStatement statement = conn.prepareStatement(deleteGroup)) {
                statement.setString(1, (String)event.get(Constants.HOST));
                statement.setString(2, (String)map.get("attributeId"));
                int count = statement.executeUpdate();
                if (count == 0) {
                    throw new SQLException("no record is deleted for attribute " + (String)map.get("attributeId"));
                }
                conn.commit();
                result = Success.of((String)event.get(Constants.USER));
                insertNotification(event, true, null);
            } catch (SQLException e) {
                logger.error("SQLException:", e);
                conn.rollback();
                insertNotification(event, false, e.getMessage());
                result = Failure.of(new Status(SQL_EXCEPTION, e.getMessage()));
            } catch (Exception e) {
                logger.error("Exception:", e);
                conn.rollback();
                insertNotification(event, false, e.getMessage());
                result = Failure.of(new Status(GENERIC_EXCEPTION, e.getMessage()));
            }
        } catch (SQLException e) {
            logger.error("SQLException:", e);
            result = Failure.of(new Status(SQL_EXCEPTION, e.getMessage()));
        }
        return result;
    }

    @Override
    public Result<String> queryAttribute(int offset, int limit, String hostId, String attributeId, String attributeType, String attributeDesc) {
        Result<String> result;
        StringBuilder sqlBuilder = new StringBuilder();
        sqlBuilder.append("SELECT COUNT(*) OVER () AS total, host_id, attribute_id, attribute_type, attribute_desc, update_user, update_ts " +
                "FROM attribute_t " +
                "WHERE host_id = ?\n");

        List<Object> parameters = new ArrayList<>();
        parameters.add(UUID.fromString(hostId));

        StringBuilder whereClause = new StringBuilder();
        addCondition(whereClause, parameters, "attribute_id", attributeId);
        addCondition(whereClause, parameters, "attribute_type", attributeType);
        addCondition(whereClause, parameters, "attribute_desc", attributeDesc);

        if (!whereClause.isEmpty()) {
            sqlBuilder.append("AND ").append(whereClause);
        }
        sqlBuilder.append(" ORDER BY attribute_id\n" +
                "LIMIT ? OFFSET ?");

        parameters.add(limit);
        parameters.add(offset);

        String sql = sqlBuilder.toString();

        int total = 0;
        List<Map<String, Object>> attributes = new ArrayList<>();
        try (final Connection conn = ds.getConnection(); PreparedStatement preparedStatement = conn.prepareStatement(sql)) {
            for (int i = 0; i < parameters.size(); i++) {
                preparedStatement.setObject(i + 1, parameters.get(i));
            }
            boolean isFirstRow = true;
            try (ResultSet resultSet = preparedStatement.executeQuery()) {
                while (resultSet.next()) {
                    Map<String, Object> map = new HashMap<>();
                    // only get the total once as it is the same for all rows.
                    if (isFirstRow) {
                        total = resultSet.getInt("total");
                        isFirstRow = false;
                    }
                    map.put("hostId", resultSet.getObject("host_id", UUID.class));
                    map.put("attributeId", resultSet.getString("attribute_id"));
                    map.put("attributeType", resultSet.getString("attribute_type"));
                    map.put("attributeDesc", resultSet.getString("attribute_desc"));
                    map.put("updateUser", resultSet.getString("update_user"));
                    map.put("updateTs", resultSet.getObject("update_ts") != null ? resultSet.getObject("update_ts", OffsetDateTime.class) : null);
                    attributes.add(map);
                }
            }

            Map<String, Object> resultMap = new HashMap<>();
            resultMap.put("total", total);
            resultMap.put("attributes", attributes);
            result = Success.of(JsonMapper.toJson(resultMap));
        } catch (SQLException e) {
            logger.error("SQLException:", e);
            result = Failure.of(new Status(SQL_EXCEPTION, e.getMessage()));
        } catch (Exception e) {
            logger.error("Exception:", e);
            result = Failure.of(new Status(GENERIC_EXCEPTION, e.getMessage()));
        }
        return result;
    }

    @Override
    public Result<String> queryAttributeLabel(String hostId) {
        final String sql = "SELECT attribute_id from attribute_t WHERE host_id = ?";
        Result<String> result;
        try (final Connection conn = ds.getConnection()) {
            List<Map<String, Object>> list = new ArrayList<>();
            try (PreparedStatement statement = conn.prepareStatement(sql)) {
                statement.setObject(1, UUID.fromString(hostId));
                try (ResultSet resultSet = statement.executeQuery()) {
                    while (resultSet.next()) {
                        Map<String, Object> map = new HashMap<>();
                        String id = resultSet.getString("attribute_id");
                        map.put("id", id);
                        map.put("label", id);
                        list.add(map);
                    }
                }
            }
            if (list.isEmpty())
                result = Failure.of(new Status(OBJECT_NOT_FOUND, "attribute", hostId));
            else
                result = Success.of(JsonMapper.toJson(list));
        } catch (SQLException e) {
            logger.error("SQLException:", e);
            result = Failure.of(new Status(SQL_EXCEPTION, e.getMessage()));
        } catch (Exception e) {
            logger.error("Exception:", e);
            result = Failure.of(new Status(GENERIC_EXCEPTION, e.getMessage()));
        }
        return result;
    }

    @Override
    public Result<String> queryAttributePermission(int offset, int limit, String hostId, String attributeId, String attributeType, String attributeValue, String apiId, String apiVersion, String endpoint) {
        Result<String> result;
        StringBuilder sqlBuilder = new StringBuilder();
        sqlBuilder.append("SELECT COUNT(*) OVER () AS total, \n" +
                "a.host_id, a.attribute_id, a.attribute_type, p.attribute_value, " +
                "p.api_id, p.api_version, p.endpoint\n" +
                "FROM attribute_t a, attribute_permission_t p\n" +
                "WHERE a.attribute_id = p.attribute_id\n" +
                "AND a.host_id = ?\n");


        List<Object> parameters = new ArrayList<>();
        parameters.add(UUID.fromString(hostId));


        StringBuilder whereClause = new StringBuilder();

        addCondition(whereClause, parameters, "a.attribute_id", attributeId);
        addCondition(whereClause, parameters, "a.attribute_type", attributeType);
        addCondition(whereClause, parameters, "a.attribute_value", attributeValue);
        addCondition(whereClause, parameters, "p.api_id", apiId);
        addCondition(whereClause, parameters, "p.api_version", apiVersion);
        addCondition(whereClause, parameters, "p.endpoint", endpoint);

        if (!whereClause.isEmpty()) {
            sqlBuilder.append("AND ").append(whereClause);
        }

        sqlBuilder.append(" ORDER BY a.attribute_id, p.api_id, p.api_version, p.endpoint\n" +
                "LIMIT ? OFFSET ?");

        parameters.add(limit);
        parameters.add(offset);

        String sql = sqlBuilder.toString();
        if(logger.isTraceEnabled()) logger.trace("queryAttributePermission sql: {}", sql);
        int total = 0;
        List<Map<String, Object>> attributePermissions = new ArrayList<>();


        try (final Connection conn = ds.getConnection(); PreparedStatement preparedStatement = conn.prepareStatement(sql)) {
            for (int i = 0; i < parameters.size(); i++) {
                preparedStatement.setObject(i + 1, parameters.get(i));
            }
            boolean isFirstRow = true;
            try (ResultSet resultSet = preparedStatement.executeQuery()) {
                while (resultSet.next()) {
                    Map<String, Object> map = new HashMap<>();
                    // only get the total once as it is the same for all rows.
                    if (isFirstRow) {
                        total = resultSet.getInt("total");
                        isFirstRow = false;
                    }
                    map.put("hostId", resultSet.getObject("host_id", UUID.class));
                    map.put("attributeId", resultSet.getString("attribute_id"));
                    map.put("attributeType", resultSet.getString("attribute_type"));
                    map.put("attributeValue", resultSet.getString("attribute_value"));
                    map.put("apiId", resultSet.getString("api_id"));
                    map.put("apiVersion", resultSet.getString("api_version"));
                    map.put("endpoint", resultSet.getString("endpoint"));
                    attributePermissions.add(map);
                }
            }
            Map<String, Object> resultMap = new HashMap<>();
            resultMap.put("total", total);
            resultMap.put("attributePermissions", attributePermissions);
            result = Success.of(JsonMapper.toJson(resultMap));

        } catch (SQLException e) {
            logger.error("SQLException:", e);
            result = Failure.of(new Status(SQL_EXCEPTION, e.getMessage()));
        } catch (Exception e) {
            logger.error("Exception:", e);
            result = Failure.of(new Status(GENERIC_EXCEPTION, e.getMessage()));
        }

        return result;

    }

    @Override
    public Result<String> queryAttributeUser(int offset, int limit, String hostId, String attributeId, String attributeType, String attributeValue, String userId, String entityId, String email, String firstName, String lastName, String userType) {
        Result<String> result;
        StringBuilder sqlBuilder = new StringBuilder();
        sqlBuilder.append("SELECT COUNT(*) OVER () AS total, \n" +
                "a.host_id, a.attribute_id, at.attribute_type, a.attribute_value, \n" +
                "a.start_ts, a.end_ts, \n" +
                "u.user_id, u.email, u.user_type, \n" +
                "CASE\n" +
                "    WHEN u.user_type = 'C' THEN c.customer_id\n" +
                "    WHEN u.user_type = 'E' THEN e.employee_id\n" +
                "    ELSE NULL -- Handle other cases if needed\n" +
                "END AS entity_id,\n" +
                "e.manager_id, u.first_name, u.last_name\n" +
                "FROM user_t u\n" +
                "LEFT JOIN\n" +
                "    customer_t c ON u.user_id = c.user_id AND u.user_type = 'C'\n" +
                "LEFT JOIN\n" +
                "    employee_t e ON u.user_id = e.user_id AND u.user_type = 'E'\n" +
                "INNER JOIN\n" +
                "    attribute_user_t a ON a.user_id = u.user_id\n" +
                "INNER JOIN\n" +
                "    attribute_t at ON at.attribute_id = a.attribute_id\n" +
                "AND a.host_id = ?\n");

        List<Object> parameters = new ArrayList<>();
        parameters.add(UUID.fromString(hostId));


        StringBuilder whereClause = new StringBuilder();

        addCondition(whereClause, parameters, "a.attribute_id", attributeId);
        addCondition(whereClause, parameters, "a.attribute_type", attributeType);
        addCondition(whereClause, parameters, "a.attribute_value", attributeValue);
        addCondition(whereClause, parameters, "u.user_id", userId != null ? UUID.fromString(userId) : null);
        addCondition(whereClause, parameters, "entity_id", entityId);
        addCondition(whereClause, parameters, "u.email", email);
        addCondition(whereClause, parameters, "u.first_name", firstName);
        addCondition(whereClause, parameters, "u.last_name", lastName);
        addCondition(whereClause, parameters, "u.user_type", userType);

        if (!whereClause.isEmpty()) {
            sqlBuilder.append("AND ").append(whereClause);
        }

        sqlBuilder.append(" ORDER BY a.attribute_id, u.user_id\n" +
                "LIMIT ? OFFSET ?");

        parameters.add(limit);
        parameters.add(offset);

        String sql = sqlBuilder.toString();
        if(logger.isTraceEnabled()) logger.trace("queryGroupUser sql: {}", sql);
        int total = 0;
        List<Map<String, Object>> attributeUsers = new ArrayList<>();


        try (final Connection conn = ds.getConnection(); PreparedStatement preparedStatement = conn.prepareStatement(sql)) {
            for (int i = 0; i < parameters.size(); i++) {
                preparedStatement.setObject(i + 1, parameters.get(i));
            }
            boolean isFirstRow = true;
            try (ResultSet resultSet = preparedStatement.executeQuery()) {
                while (resultSet.next()) {
                    Map<String, Object> map = new HashMap<>();
                    // only get the total once as it is the same for all rows.
                    if (isFirstRow) {
                        total = resultSet.getInt("total");
                        isFirstRow = false;
                    }
                    map.put("hostId", resultSet.getObject("host_id", UUID.class));
                    map.put("attributeId", resultSet.getString("attribute_id"));
                    map.put("attributeType", resultSet.getString("attribute_type"));
                    map.put("attributeValue", resultSet.getString("attribute_value"));
                    map.put("startTs", resultSet.getDate("start_ts"));
                    map.put("endTs", resultSet.getString("end_ts"));
                    map.put("userId", resultSet.getObject("user_id", UUID.class));
                    map.put("entityId", resultSet.getString("entity_id"));
                    map.put("email", resultSet.getString("email"));
                    map.put("firstName", resultSet.getString("first_name"));
                    map.put("lastName", resultSet.getString("last_name"));
                    map.put("userType", resultSet.getString("user_type"));
                    attributeUsers.add(map);
                }
            }
            Map<String, Object> resultMap = new HashMap<>();
            resultMap.put("total", total);
            resultMap.put("attributeUsers", attributeUsers);
            result = Success.of(JsonMapper.toJson(resultMap));

        } catch (SQLException e) {
            logger.error("SQLException:", e);
            result = Failure.of(new Status(SQL_EXCEPTION, e.getMessage()));
        } catch (Exception e) {
            logger.error("Exception:", e);
            result = Failure.of(new Status(GENERIC_EXCEPTION, e.getMessage()));
        }

        return result;

    }

    @Override
    public Result<String> createAttributePermission(Map<String, Object> event) {
        final String insertGroup = "INSERT INTO attribute_permission_t (host_id, attribute_id, attribute_value, api_id, api_version, endpoint, update_user, update_ts) " +
                "VALUES (?, ?, ?, ?, ?, ?,  ?, ?)";

        Result<String> result = null;
        Map<String, Object> map = (Map<String, Object>)event.get(PortalConstants.DATA);
        try (Connection conn = ds.getConnection()) {
            conn.setAutoCommit(false);
            // no duplicate record, insert the user into database and write a success notification.
            try (PreparedStatement statement = conn.prepareStatement(insertGroup)) {
                statement.setString(1, (String)event.get(Constants.HOST));
                statement.setString(2, (String)map.get("attributeId"));
                statement.setString(3, (String)map.get("attributeValue"));
                statement.setString(4, (String)map.get("apiId"));
                statement.setString(5, (String)map.get("apiVersion"));
                statement.setString(6, (String)map.get("endpoint"));
                statement.setString(7, (String)event.get(Constants.USER));
                statement.setObject(8, OffsetDateTime.parse((String)event.get(CloudEventV1.TIME)));

                int count = statement.executeUpdate();
                if (count == 0) {
                    throw new SQLException("failed to insert attribute permission " + (String)map.get("attributeId"));
                }
                conn.commit();
                result = Success.of((String)map.get("attributeId"));
                insertNotification(event, true, null);
            } catch (SQLException e) {
                logger.error("SQLException:", e);
                conn.rollback();
                insertNotification(event, false, e.getMessage());
                result = Failure.of(new Status(SQL_EXCEPTION, e.getMessage()));
            } catch (Exception e) {
                logger.error("Exception:", e);
                conn.rollback();
                insertNotification(event, false, e.getMessage());
                result = Failure.of(new Status(GENERIC_EXCEPTION, e.getMessage()));
            }
        } catch (SQLException e) {
            logger.error("SQLException:", e);
            result = Failure.of(new Status(SQL_EXCEPTION, e.getMessage()));
        }
        return result;
    }

    @Override
    public Result<String> updateAttributePermission(Map<String, Object> event) {
        final String updateGroup = "UPDATE attribute_permission_t SET attribute_value = ?, update_user = ?, update_ts = ? " +
                "WHERE host_id = ? AND attribute_id = ? AND api_id = ? AND api_version = ? AND endpoint = ?";

        Result<String> result = null;
        Map<String, Object> map = (Map<String, Object>)event.get(PortalConstants.DATA);
        try (Connection conn = ds.getConnection()) {
            conn.setAutoCommit(false);
            try (PreparedStatement statement = conn.prepareStatement(updateGroup)) {
                statement.setString(1, (String)map.get("attributeValue"));
                statement.setString(2, (String)event.get(Constants.USER));
                statement.setObject(3, OffsetDateTime.parse((String)event.get(CloudEventV1.TIME)));

                statement.setString(4, (String)event.get(Constants.HOST));
                statement.setString(5, (String)map.get("attributeId"));
                statement.setString(6, (String)map.get("apiId"));
                statement.setString(7, (String)map.get("apiVersion"));
                statement.setString(8, (String)map.get("endpoint"));

                int count = statement.executeUpdate();
                if (count == 0) {
                    throw new SQLException("no record is updated for attribute permission " + (String)map.get("attributeId"));
                }
                conn.commit();
                result = Success.of((String)map.get("attributeId"));
                insertNotification(event, true, null);
            } catch (SQLException e) {
                logger.error("SQLException:", e);
                conn.rollback();
                insertNotification(event, false, e.getMessage());
                result = Failure.of(new Status(SQL_EXCEPTION, e.getMessage()));
            } catch (Exception e) {
                logger.error("Exception:", e);
                conn.rollback();
                insertNotification(event, false, e.getMessage());
                result = Failure.of(new Status(GENERIC_EXCEPTION, e.getMessage()));
            }
        } catch (SQLException e) {
            logger.error("SQLException:", e);
            result = Failure.of(new Status(SQL_EXCEPTION, e.getMessage()));
        }
        return result;

    }

    @Override
    public Result<String> deleteAttributePermission(Map<String, Object> event) {
        final String deleteGroup = "DELETE from attribute_permission_t WHERE host_id = ? AND attribute_id = ? " +
                "AND api_id = ? AND api_version = ? AND endpoint = ?";
        Result<String> result;
        Map<String, Object> map = (Map<String, Object>)event.get(PortalConstants.DATA);
        try (Connection conn = ds.getConnection()) {
            conn.setAutoCommit(false);
            try (PreparedStatement statement = conn.prepareStatement(deleteGroup)) {
                statement.setString(1, (String)event.get(Constants.HOST));
                statement.setString(2, (String)map.get("attributeId"));
                statement.setString(3, (String)map.get("apiId"));
                statement.setString(4, (String)map.get("apiVersion"));
                statement.setString(5, (String)map.get("endpoint"));

                int count = statement.executeUpdate();
                if (count == 0) {
                    throw new SQLException("no record is deleted for attribute permission " + (String)map.get("attributeId"));
                }
                conn.commit();
                result = Success.of((String)event.get(Constants.USER));
                insertNotification(event, true, null);
            } catch (SQLException e) {
                logger.error("SQLException:", e);
                conn.rollback();
                insertNotification(event, false, e.getMessage());
                result = Failure.of(new Status(SQL_EXCEPTION, e.getMessage()));
            } catch (Exception e) {
                logger.error("Exception:", e);
                conn.rollback();
                insertNotification(event, false, e.getMessage());
                result = Failure.of(new Status(GENERIC_EXCEPTION, e.getMessage()));
            }
        } catch (SQLException e) {
            logger.error("SQLException:", e);
            result = Failure.of(new Status(SQL_EXCEPTION, e.getMessage()));
        }
        return result;

    }

    @Override
    public Result<String> createAttributeUser(Map<String, Object> event) {
        final String insertGroup = "INSERT INTO attribute_user_t (host_id, attribute_id, attribute_value, user_id, start_ts, end_ts, update_user, update_ts) " +
                "VALUES (?, ?, ?, ?, ?,  ?, ?, ?)";

        Result<String> result = null;
        Map<String, Object> map = (Map<String, Object>)event.get(PortalConstants.DATA);
        try (Connection conn = ds.getConnection()) {
            conn.setAutoCommit(false);
            // no duplicate record, insert the user into database and write a success notification.
            try (PreparedStatement statement = conn.prepareStatement(insertGroup)) {
                statement.setString(1, (String)event.get(Constants.HOST));
                String attributeId = (String)map.get("attributeId");
                statement.setString(2, attributeId);
                statement.setString(3, (String)map.get("attributeValue"));
                String startTs = (String)map.get("startTs");
                if(startTs != null && !startTs.isEmpty())
                    statement.setObject(4, OffsetDateTime.parse(startTs));
                else
                    statement.setNull(4, NULL);
                String endTs = (String)map.get("endTs");
                if (endTs != null && !endTs.isEmpty()) {
                    statement.setObject(5, OffsetDateTime.parse(endTs));
                } else {
                    statement.setNull(5, NULL);
                }
                statement.setString(6, (String)event.get(Constants.USER));
                statement.setObject(7, OffsetDateTime.parse((String)event.get(CloudEventV1.TIME)));

                int count = statement.executeUpdate();
                if (count == 0) {
                    throw new SQLException("failed to insert attribute user " + attributeId);
                }
                conn.commit();
                result = Success.of(attributeId);
                insertNotification(event, true, null);
            } catch (SQLException e) {
                logger.error("SQLException:", e);
                conn.rollback();
                insertNotification(event, false, e.getMessage());
                result = Failure.of(new Status(SQL_EXCEPTION, e.getMessage()));
            } catch (Exception e) {
                logger.error("Exception:", e);
                conn.rollback();
                insertNotification(event, false, e.getMessage());
                result = Failure.of(new Status(GENERIC_EXCEPTION, e.getMessage()));
            }
        } catch (SQLException e) {
            logger.error("SQLException:", e);
            result = Failure.of(new Status(SQL_EXCEPTION, e.getMessage()));
        }
        return result;

    }

    @Override
    public Result<String> updateAttributeUser(Map<String, Object> event) {
        final String updateGroup = "UPDATE attribute_user_t SET attribute_value = ?, start_ts = ?, end_ts = ?, update_user = ?, update_ts = ? " +
                "WHERE host_id = ? AND attribute_id = ? AND user_id = ?";

        Result<String> result = null;
        Map<String, Object> map = (Map<String, Object>)event.get(PortalConstants.DATA);
        try (Connection conn = ds.getConnection()) {
            conn.setAutoCommit(false);

            try (PreparedStatement statement = conn.prepareStatement(updateGroup)) {
                statement.setString(1, (String)map.get("attributeValue"));
                String startTs = (String)map.get("startTs");
                if(startTs != null && !startTs.isEmpty())
                    statement.setObject(2, OffsetDateTime.parse(startTs));
                else
                    statement.setNull(2, NULL);
                String endTs = (String)map.get("endTs");
                if (endTs != null && !endTs.isEmpty()) {
                    statement.setObject(3, OffsetDateTime.parse(endTs));
                } else {
                    statement.setNull(3, NULL);
                }
                statement.setString(4, (String)event.get(Constants.USER));
                statement.setObject(5, OffsetDateTime.parse((String)event.get(CloudEventV1.TIME)));
                statement.setString(6, (String)event.get(Constants.HOST));
                String attributeId = (String)map.get("attributeId");
                statement.setString(7, attributeId);
                statement.setString(8, (String)event.get(Constants.USER));

                int count = statement.executeUpdate();
                if (count == 0) {
                    throw new SQLException("no record is updated for attribute user " + attributeId);
                }
                conn.commit();
                result = Success.of(attributeId);
                insertNotification(event, true, null);
            } catch (SQLException e) {
                logger.error("SQLException:", e);
                conn.rollback();
                insertNotification(event, false, e.getMessage());
                result = Failure.of(new Status(SQL_EXCEPTION, e.getMessage()));
            } catch (Exception e) {
                logger.error("Exception:", e);
                conn.rollback();
                insertNotification(event, false, e.getMessage());
                result = Failure.of(new Status(GENERIC_EXCEPTION, e.getMessage()));
            }
        } catch (SQLException e) {
            logger.error("SQLException:", e);
            result = Failure.of(new Status(SQL_EXCEPTION, e.getMessage()));
        }
        return result;

    }

    @Override
    public Result<String> deleteAttributeUser(Map<String, Object> event) {
        final String deleteGroup = "DELETE from attribute_user_t WHERE host_id = ? AND attribute_id = ? AND user_id = ?";
        Result<String> result;
        Map<String, Object> map = (Map<String, Object>)event.get(PortalConstants.DATA);
        try (Connection conn = ds.getConnection()) {
            conn.setAutoCommit(false);
            try (PreparedStatement statement = conn.prepareStatement(deleteGroup)) {
                statement.setString(1, (String)event.get(Constants.HOST));
                statement.setString(2, (String)map.get("attributeId"));
                statement.setString(3, (String)event.get(Constants.USER));

                int count = statement.executeUpdate();
                if (count == 0) {
                    throw new SQLException("no record is deleted for attribute user " + (String)map.get("attributeId"));
                }
                conn.commit();
                result = Success.of((String)event.get(Constants.USER));
                insertNotification(event, true, null);
            } catch (SQLException e) {
                logger.error("SQLException:", e);
                conn.rollback();
                insertNotification(event, false, e.getMessage());
                result = Failure.of(new Status(SQL_EXCEPTION, e.getMessage()));
            } catch (Exception e) {
                logger.error("Exception:", e);
                conn.rollback();
                insertNotification(event, false, e.getMessage());
                result = Failure.of(new Status(GENERIC_EXCEPTION, e.getMessage()));
            }
        } catch (SQLException e) {
            logger.error("SQLException:", e);
            result = Failure.of(new Status(SQL_EXCEPTION, e.getMessage()));
        }
        return result;

    }

    @Override
    public Result<String> queryAttributeRowFilter(int offset, int limit, String hostId, String attributeId, String attributeValue, String apiId, String apiVersion, String endpoint) {
        Result<String> result;
        StringBuilder sqlBuilder = new StringBuilder();
        sqlBuilder.append("SELECT COUNT(*) OVER () AS total, \n" +
                "a.host_id, a.attribute_id, at.attribute_type, p.attribute_value, " +
                "p.api_id, p.api_version, p.endpoint, p.col_name, p.operator, p.col_value\n" +
                "FROM attribute_t a, attribute_row_filter_t p, attribute_user_t at\n" +
                "WHERE a.attribute_id = p.attribute_id\n" +
                "AND a.host_id = ?\n");

        List<Object> parameters = new ArrayList<>();
        parameters.add(UUID.fromString(hostId));

        StringBuilder whereClause = new StringBuilder();

        addCondition(whereClause, parameters, "a.attribute_id", attributeId);
        addCondition(whereClause, parameters, "p.attribute_value", attributeValue);
        addCondition(whereClause, parameters, "p.api_id", apiId);
        addCondition(whereClause, parameters, "p.api_version", apiVersion);
        addCondition(whereClause, parameters, "p.endpoint", endpoint);

        if (whereClause.length() > 0) {
            sqlBuilder.append("AND ").append(whereClause);
        }

        sqlBuilder.append(" ORDER BY a.attribute_id, p.api_id, p.api_version, p.endpoint\n" +
                "LIMIT ? OFFSET ?");

        parameters.add(limit);
        parameters.add(offset);

        String sql = sqlBuilder.toString();
        if(logger.isTraceEnabled()) logger.trace("queryAttributeRowFilter sql: {}", sql);
        int total = 0;
        List<Map<String, Object>> attributeRowFilters = new ArrayList<>();

        try (final Connection conn = ds.getConnection(); PreparedStatement preparedStatement = conn.prepareStatement(sql)) {
            for (int i = 0; i < parameters.size(); i++) {
                preparedStatement.setObject(i + 1, parameters.get(i));
            }
            boolean isFirstRow = true;
            try (ResultSet resultSet = preparedStatement.executeQuery()) {
                while (resultSet.next()) {
                    Map<String, Object> map = new HashMap<>();
                    // only get the total once as it is the same for all rows.
                    if (isFirstRow) {
                        total = resultSet.getInt("total");
                        isFirstRow = false;
                    }
                    map.put("hostId", resultSet.getObject("host_id", UUID.class));
                    map.put("attributeId", resultSet.getString("attribute_id"));
                    map.put("attributeType", resultSet.getString("attribute_type"));
                    map.put("attributeValue", resultSet.getString("attribute_value"));
                    map.put("apiId", resultSet.getString("api_id"));
                    map.put("apiVersion", resultSet.getString("api_version"));
                    map.put("endpoint", resultSet.getString("endpoint"));
                    map.put("colName", resultSet.getString("col_name"));
                    map.put("operator", resultSet.getString("operator"));
                    map.put("colValue", resultSet.getString("col_value"));
                    attributeRowFilters.add(map);
                }
            }
            Map<String, Object> resultMap = new HashMap<>();
            resultMap.put("total", total);
            resultMap.put("attributeRowFilters", attributeRowFilters);
            result = Success.of(JsonMapper.toJson(resultMap));

        } catch (SQLException e) {
            logger.error("SQLException:", e);
            result = Failure.of(new Status(SQL_EXCEPTION, e.getMessage()));
        } catch (Exception e) {
            logger.error("Exception:", e);
            result = Failure.of(new Status(GENERIC_EXCEPTION, e.getMessage()));
        }

        return result;
    }

    @Override
    public Result<String> createAttributeRowFilter(Map<String, Object> event) {
        final String insertGroup = "INSERT INTO attribute_row_filter_t (host_id, attribute_id, attribute_value, api_id, api_version, endpoint, col_name, operator, col_value, update_user, update_ts) " +
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";

        Result<String> result = null;
        Map<String, Object> map = (Map<String, Object>)event.get(PortalConstants.DATA);
        try (Connection conn = ds.getConnection()) {
            conn.setAutoCommit(false);
            // no duplicate record, insert the user into database and write a success notification.
            try (PreparedStatement statement = conn.prepareStatement(insertGroup)) {
                statement.setString(1, (String)event.get(Constants.HOST));
                statement.setString(2, (String)map.get("attributeId"));
                statement.setString(3, (String)map.get("attributeValue"));
                statement.setString(4, (String)map.get("apiId"));
                statement.setString(5, (String)map.get("apiVersion"));
                statement.setString(6, (String)map.get("endpoint"));
                statement.setString(7, (String)map.get("colName"));
                statement.setString(8, (String)map.get("operator"));
                statement.setString(9, (String)map.get("colValue"));
                statement.setString(10, (String)event.get(Constants.USER));
                statement.setObject(11, OffsetDateTime.parse((String)event.get(CloudEventV1.TIME)));

                int count = statement.executeUpdate();
                if (count == 0) {
                    throw new SQLException("failed to insert attribute row filter " + (String)map.get("attributeId"));
                }
                conn.commit();
                result = Success.of((String)map.get("attributeId"));
                insertNotification(event, true, null);
            } catch (SQLException e) {
                logger.error("SQLException:", e);
                conn.rollback();
                insertNotification(event, false, e.getMessage());
                result = Failure.of(new Status(SQL_EXCEPTION, e.getMessage()));
            } catch (Exception e) {
                logger.error("Exception:", e);
                conn.rollback();
                insertNotification(event, false, e.getMessage());
                result = Failure.of(new Status(GENERIC_EXCEPTION, e.getMessage()));
            }
        } catch (SQLException e) {
            logger.error("SQLException:", e);
            result = Failure.of(new Status(SQL_EXCEPTION, e.getMessage()));
        }
        return result;
    }

    @Override
    public Result<String> updateAttributeRowFilter(Map<String, Object> event) {
        final String updateGroup = "UPDATE attribute_row_filter_t SET attribute_value = ?, api_id = ?, api_version = ?, endpoint = ?, col_name = ?, operator = ?, col_value = ?, update_user = ?, update_ts = ? " +
                "WHERE host_id = ? AND attribute_id = ?";

        Result<String> result = null;
        Map<String, Object> map = (Map<String, Object>)event.get(PortalConstants.DATA);
        try (Connection conn = ds.getConnection()) {
            conn.setAutoCommit(false);
            try (PreparedStatement statement = conn.prepareStatement(updateGroup)) {
                statement.setString(1, (String)map.get("attributeValue"));
                statement.setString(2, (String)map.get("apiId"));
                statement.setString(3, (String)map.get("apiVersion"));
                statement.setString(4, (String)map.get("endpoint"));
                statement.setString(5, (String)map.get("colName"));
                statement.setString(6, (String)map.get("operator"));
                statement.setString(7, (String)map.get("colValue"));
                statement.setString(8, (String)event.get(Constants.USER));
                statement.setObject(9, OffsetDateTime.parse((String)event.get(CloudEventV1.TIME)));

                statement.setString(10, (String)event.get(Constants.HOST));
                statement.setString(11, (String)map.get("attributeId"));

                int count = statement.executeUpdate();
                if (count == 0) {
                    throw new SQLException("no record is updated for attribute row filter " + (String)map.get("attributeId"));
                }
                conn.commit();
                result = Success.of((String)map.get("attributeId"));
                insertNotification(event, true, null);
            } catch (SQLException e) {
                logger.error("SQLException:", e);
                conn.rollback();
                insertNotification(event, false, e.getMessage());
                result = Failure.of(new Status(SQL_EXCEPTION, e.getMessage()));
            } catch (Exception e) {
                logger.error("Exception:", e);
                conn.rollback();
                insertNotification(event, false, e.getMessage());
                result = Failure.of(new Status(GENERIC_EXCEPTION, e.getMessage()));
            }
        } catch (SQLException e) {
            logger.error("SQLException:", e);
            result = Failure.of(new Status(SQL_EXCEPTION, e.getMessage()));
        }
        return result;
    }

    @Override
    public Result<String> deleteAttributeRowFilter(Map<String, Object> event) {
        final String deleteGroup = "DELETE from attribute_row_filter_t WHERE host_id = ? AND attribute_id = ? " +
                "AND api_id = ? AND api_version = ? AND endpoint = ?";
        Result<String> result;
        Map<String, Object> map = (Map<String, Object>)event.get(PortalConstants.DATA);
        try (Connection conn = ds.getConnection()) {
            conn.setAutoCommit(false);
            try (PreparedStatement statement = conn.prepareStatement(deleteGroup)) {
                statement.setString(1, (String)event.get(Constants.HOST));
                statement.setString(2, (String)map.get("attributeId"));
                statement.setString(3, (String)map.get("apiId"));
                statement.setString(4, (String)map.get("apiVersion"));
                statement.setString(5, (String)map.get("endpoint"));

                int count = statement.executeUpdate();
                if (count == 0) {
                    throw new SQLException("no record is deleted for attribute row filter " + (String)map.get("attributeId"));
                }
                conn.commit();
                result = Success.of((String)event.get(Constants.USER));
                insertNotification(event, true, null);
            } catch (SQLException e) {
                logger.error("SQLException:", e);
                conn.rollback();
                insertNotification(event, false, e.getMessage());
                result = Failure.of(new Status(SQL_EXCEPTION, e.getMessage()));
            } catch (Exception e) {
                logger.error("Exception:", e);
                conn.rollback();
                insertNotification(event, false, e.getMessage());
                result = Failure.of(new Status(GENERIC_EXCEPTION, e.getMessage()));
            }
        } catch (SQLException e) {
            logger.error("SQLException:", e);
            result = Failure.of(new Status(SQL_EXCEPTION, e.getMessage()));
        }
        return result;
    }

    @Override
    public Result<String> queryAttributeColFilter(int offset, int limit, String hostId, String attributeId, String attributeValue, String apiId, String apiVersion, String endpoint) {
        Result<String> result;
        StringBuilder sqlBuilder = new StringBuilder();
        sqlBuilder.append("SELECT COUNT(*) OVER () AS total, \n" +
                "a.host_id, a.attribute_id, a.attribute_type, p.attribute_value, " +
                "p.api_id, p.api_version, p.endpoint, p.columns\n" +
                "FROM attribute_t a, attribute_col_filter_t p\n" +
                "WHERE a.attribute_id = p.attribute_id\n" +
                "AND a.host_id = ?\n");

        List<Object> parameters = new ArrayList<>();
        parameters.add(UUID.fromString(hostId));

        StringBuilder whereClause = new StringBuilder();

        addCondition(whereClause, parameters, "a.attribute_id", attributeId);
        addCondition(whereClause, parameters, "p.attribute_value", attributeValue);
        addCondition(whereClause, parameters, "p.api_id", apiId);
        addCondition(whereClause, parameters, "p.api_version", apiVersion);
        addCondition(whereClause, parameters, "p.endpoint", endpoint);

        if (!whereClause.isEmpty()) {
            sqlBuilder.append("AND ").append(whereClause);
        }

        sqlBuilder.append(" ORDER BY a.attribute_id, p.api_id, p.api_version, p.endpoint\n" +
                "LIMIT ? OFFSET ?");

        parameters.add(limit);
        parameters.add(offset);

        String sql = sqlBuilder.toString();
        if(logger.isTraceEnabled()) logger.trace("queryAttributeColFilter sql: {}", sql);
        int total = 0;
        List<Map<String, Object>> attributeColFilters = new ArrayList<>();

        try (final Connection conn = ds.getConnection(); PreparedStatement preparedStatement = conn.prepareStatement(sql)) {
            for (int i = 0; i < parameters.size(); i++) {
                preparedStatement.setObject(i + 1, parameters.get(i));
            }
            boolean isFirstRow = true;
            try (ResultSet resultSet = preparedStatement.executeQuery()) {
                while (resultSet.next()) {
                    Map<String, Object> map = new HashMap<>();
                    // only get the total once as it is the same for all rows.
                    if (isFirstRow) {
                        total = resultSet.getInt("total");
                        isFirstRow = false;
                    }
                    map.put("hostId", resultSet.getObject("host_id", UUID.class));
                    map.put("attributeId", resultSet.getString("attribute_id"));
                    map.put("attributeType", resultSet.getString("attribute_type"));
                    map.put("attributeValue", resultSet.getString("attribute_value"));
                    map.put("apiId", resultSet.getString("api_id"));
                    map.put("apiVersion", resultSet.getString("api_version"));
                    map.put("endpoint", resultSet.getString("endpoint"));
                    map.put("columns", resultSet.getString("columns"));
                    attributeColFilters.add(map);
                }
            }
            Map<String, Object> resultMap = new HashMap<>();
            resultMap.put("total", total);
            resultMap.put("attributeColFilters", attributeColFilters);
            result = Success.of(JsonMapper.toJson(resultMap));

        } catch (SQLException e) {
            logger.error("SQLException:", e);
            result = Failure.of(new Status(SQL_EXCEPTION, e.getMessage()));
        } catch (Exception e) {
            logger.error("Exception:", e);
            result = Failure.of(new Status(GENERIC_EXCEPTION, e.getMessage()));
        }

        return result;
    }

    @Override
    public Result<String> createAttributeColFilter(Map<String, Object> event) {
        final String insertGroup = "INSERT INTO attribute_col_filter_t (host_id, attribute_id, attribute_value, api_id, api_version, endpoint, columns, update_user, update_ts) " +
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)";

        Result<String> result = null;
        Map<String, Object> map = (Map<String, Object>)event.get(PortalConstants.DATA);
        try (Connection conn = ds.getConnection()) {
            conn.setAutoCommit(false);
            // no duplicate record, insert the user into database and write a success notification.
            try (PreparedStatement statement = conn.prepareStatement(insertGroup)) {
                statement.setString(1, (String)event.get(Constants.HOST));
                statement.setString(2, (String)map.get("attributeId"));
                statement.setString(3, (String)map.get("attributeValue"));
                statement.setString(4, (String)map.get("apiId"));
                statement.setString(5, (String)map.get("apiVersion"));
                statement.setString(6, (String)map.get("endpoint"));
                statement.setString(7, (String)map.get("columns"));
                statement.setString(8, (String)event.get(Constants.USER));
                statement.setObject(9, OffsetDateTime.parse((String)event.get(CloudEventV1.TIME)));

                int count = statement.executeUpdate();
                if (count == 0) {
                    throw new SQLException("failed to insert attribute col filter " + (String)map.get("attributeId"));
                }
                conn.commit();
                result = Success.of((String)map.get("attributeId"));
                insertNotification(event, true, null);
            } catch (SQLException e) {
                logger.error("SQLException:", e);
                conn.rollback();
                insertNotification(event, false, e.getMessage());
                result = Failure.of(new Status(SQL_EXCEPTION, e.getMessage()));
            } catch (Exception e) {
                logger.error("Exception:", e);
                conn.rollback();
                insertNotification(event, false, e.getMessage());
                result = Failure.of(new Status(GENERIC_EXCEPTION, e.getMessage()));
            }
        } catch (SQLException e) {
            logger.error("SQLException:", e);
            result = Failure.of(new Status(SQL_EXCEPTION, e.getMessage()));
        }
        return result;
    }

    @Override
    public Result<String> updateAttributeColFilter(Map<String, Object> event) {
        final String updateGroup = "UPDATE attribute_col_filter_t SET attribute_value = ?, api_id = ?, api_version = ?, endpoint = ?, columns = ?, update_user = ?, update_ts = ? " +
                "WHERE host_id = ? AND attribute_id = ?";

        Result<String> result = null;
        Map<String, Object> map = (Map<String, Object>)event.get(PortalConstants.DATA);
        try (Connection conn = ds.getConnection()) {
            conn.setAutoCommit(false);
            try (PreparedStatement statement = conn.prepareStatement(updateGroup)) {
                statement.setString(1, (String)map.get("attributeValue"));
                statement.setString(2, (String)map.get("apiId"));
                statement.setString(3, (String)map.get("apiVersion"));
                statement.setString(4, (String)map.get("endpoint"));
                statement.setString(5, (String)map.get("columns"));
                statement.setString(6, (String)event.get(Constants.USER));
                statement.setObject(7, OffsetDateTime.parse((String)event.get(CloudEventV1.TIME)));

                statement.setString(8, (String)event.get(Constants.HOST));
                statement.setString(9, (String)map.get("attributeId"));

                int count = statement.executeUpdate();
                if (count == 0) {
                    throw new SQLException("no record is updated for attribute col filter " + (String)map.get("attributeId"));
                }
                conn.commit();
                result = Success.of((String)map.get("attributeId"));
                insertNotification(event, true, null);
            } catch (SQLException e) {
                logger.error("SQLException:", e);
                conn.rollback();
                insertNotification(event, false, e.getMessage());
                result = Failure.of(new Status(SQL_EXCEPTION, e.getMessage()));
            } catch (Exception e) {
                logger.error("Exception:", e);
                conn.rollback();
                insertNotification(event, false, e.getMessage());
                result = Failure.of(new Status(GENERIC_EXCEPTION, e.getMessage()));
            }
        } catch (SQLException e) {
            logger.error("SQLException:", e);
            result = Failure.of(new Status(SQL_EXCEPTION, e.getMessage()));
        }
        return result;
    }

    @Override
    public Result<String> deleteAttributeColFilter(Map<String, Object> event) {
        final String deleteGroup = "DELETE from attribute_col_filter_t WHERE host_id = ? AND attribute_id = ? " +
                "AND api_id = ? AND api_version = ? AND endpoint = ?";
        Result<String> result;
        Map<String, Object> map = (Map<String, Object>)event.get(PortalConstants.DATA);
        try (Connection conn = ds.getConnection()) {
            conn.setAutoCommit(false);
            try (PreparedStatement statement = conn.prepareStatement(deleteGroup)) {
                statement.setObject(1, UUID.fromString((String)map.get("hostId")));
                statement.setString(2, (String)map.get("attributeId"));
                statement.setString(3, (String)map.get("apiId"));
                statement.setString(4, (String)map.get("apiVersion"));
                statement.setString(5, (String)map.get("endpoint"));

                int count = statement.executeUpdate();
                if (count == 0) {
                    throw new SQLException("no record is deleted for attribute col filter " + (String)map.get("attributeId"));
                }
                conn.commit();
                result = Success.of((String)event.get(Constants.USER));
                insertNotification(event, true, null);
            } catch (SQLException e) {
                logger.error("SQLException:", e);
                conn.rollback();
                insertNotification(event, false, e.getMessage());
                result = Failure.of(new Status(SQL_EXCEPTION, e.getMessage()));
            } catch (Exception e) {
                logger.error("Exception:", e);
                conn.rollback();
                insertNotification(event, false, e.getMessage());
                result = Failure.of(new Status(GENERIC_EXCEPTION, e.getMessage()));
            }
        } catch (SQLException e) {
            logger.error("SQLException:", e);
            result = Failure.of(new Status(SQL_EXCEPTION, e.getMessage()));
        }
        return result;
    }

    @Override
    public Result<String> createProduct(Map<String, Object> event) {
        final String sql = "INSERT INTO product_version_t(host_id, product_id, product_version, " +
                "light4j_version, break_code, break_config, release_note, version_desc, release_type, current, " +
                "version_status, update_user, update_ts) " +
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
        final String sqlUpdate = "UPDATE product_version_t SET current = false \n" +
                "WHERE host_id = ?\n" +
                "AND product_id = ?\n" +
                "AND product_version != ?";
        Result<String> result;
        Map<String, Object> map = (Map<String, Object>)event.get(PortalConstants.DATA);
        try (Connection conn = ds.getConnection()) {
            conn.setAutoCommit(false);
            try (PreparedStatement statement = conn.prepareStatement(sql)) {
                statement.setString(1, (String)event.get(Constants.HOST));
                statement.setString(2, (String)map.get("productId"));
                statement.setString(3, (String)map.get("productVersion"));
                statement.setString(4, (String)map.get("light4jVersion"));
                if (map.containsKey("breakCode")) {
                    statement.setBoolean(5, (Boolean) map.get("breakCode"));
                } else {
                    statement.setNull(5, Types.BOOLEAN);
                }
                if (map.containsKey("breakConfig")) {
                    statement.setBoolean(6, (Boolean) map.get("breakConfig"));
                } else {
                    statement.setNull(6, Types.BOOLEAN);
                }
                if (map.containsKey("releaseNote")) {
                    statement.setString(7, (String) map.get("releaseNote"));
                } else {
                    statement.setNull(7, Types.VARCHAR);
                }
                if (map.containsKey("versionDesc")) {
                    statement.setString(8, (String) map.get("versionDesc"));
                } else {
                    statement.setNull(8, Types.VARCHAR);
                }
                statement.setString(9, (String)map.get("releaseType"));
                statement.setBoolean(10, (Boolean)map.get("current"));
                statement.setString(11, (String)map.get("versionStatus"));
                statement.setString(12, (String)event.get(Constants.USER));
                statement.setObject(13, OffsetDateTime.parse((String)event.get(CloudEventV1.TIME)));

                int count = statement.executeUpdate();
                if (count == 0) {
                    throw new SQLException("failed to insert the product with id " + (String)map.get("productId"));
                }
                // try to update current to false for others if current is true.
                if((Boolean)map.get("current")) {
                    try (PreparedStatement statementUpdate = conn.prepareStatement(sqlUpdate)) {
                        statementUpdate.setString(1, (String)event.get(Constants.HOST));
                        statementUpdate.setString(2, (String)map.get("productId"));
                        statementUpdate.setString(3, (String)map.get("productVersion"));
                        statementUpdate.executeUpdate();
                    }
                }
                conn.commit();
                result = Success.of((String)map.get("productId"));
                insertNotification(event, true, null);
            } catch (SQLException e) {
                logger.error("SQLException:", e);
                conn.rollback();
                insertNotification(event, false, e.getMessage());
                result = Failure.of(new Status(SQL_EXCEPTION, e.getMessage()));
            } catch (Exception e) {
                logger.error("Exception:", e);
                conn.rollback();
                insertNotification(event, false, e.getMessage());
                result = Failure.of(new Status(GENERIC_EXCEPTION, e.getMessage()));
            }
        } catch (SQLException e) {
            logger.error("SQLException:", e);
            result = Failure.of(new Status(SQL_EXCEPTION, e.getMessage()));
        }
        return result;
    }

    @Override
    public Result<String> updateProduct(Map<String, Object> event) {
        final String sql = "UPDATE product_version_t SET light4j_version = ?, break_code = ?, break_config = ?, " +
                "release_note = ?, version_desc = ?, release_type = ?, current = ?, version_status = ?, update_user = ?, update_ts = ? " +
                "WHERE host_id = ? and product_id = ? and product_version = ?";
        final String sqlUpdate = "UPDATE product_version_t SET current = false \n" +
                "WHERE host_id = ?\n" +
                "AND product_id = ?\n" +
                "AND product_version != ?";

        Result<String> result;
        Map<String, Object> map = (Map<String, Object>)event.get(PortalConstants.DATA);
        try (Connection conn = ds.getConnection()) {
            conn.setAutoCommit(false);
            try (PreparedStatement statement = conn.prepareStatement(sql)) {
                statement.setString(1, (String)map.get("light4jVersion"));
                if (map.containsKey("breakCode")) {
                    statement.setBoolean(2, (Boolean) map.get("breakCode"));
                } else {
                    statement.setNull(2, Types.BOOLEAN);
                }
                if (map.containsKey("breakConfig")) {
                    statement.setBoolean(3, (Boolean) map.get("breakConfig"));
                } else {
                    statement.setNull(3, Types.BOOLEAN);
                }
                if (map.containsKey("releaseNote")) {
                    statement.setString(4, (String) map.get("releaseNote"));
                } else {
                    statement.setNull(4, Types.VARCHAR);
                }

                if (map.containsKey("versionDesc")) {
                    statement.setString(5, (String) map.get("versionDesc"));
                } else {
                    statement.setNull(5, Types.VARCHAR);
                }
                statement.setString(6, (String)map.get("releaseType"));
                statement.setBoolean(7, (Boolean)map.get("current"));
                statement.setString(8, (String)map.get("versionStatus"));
                statement.setString(9, (String)event.get(Constants.USER));
                statement.setObject(10, OffsetDateTime.parse((String)event.get(CloudEventV1.TIME)));
                statement.setString(11, (String)event.get(Constants.HOST));
                statement.setString(12, (String)map.get("productId"));
                statement.setString(13, (String)map.get("productVersion"));
                int count = statement.executeUpdate();
                if (count == 0) {
                    throw new SQLException("failed to update the product with id " + (String)map.get("productId"));
                }
                // try to update current to false for others if current is true.
                if((Boolean)map.get("current")) {
                    try (PreparedStatement statementUpdate = conn.prepareStatement(sqlUpdate)) {
                        statementUpdate.setString(1, (String)event.get(Constants.HOST));
                        statementUpdate.setString(2, (String)map.get("productId"));
                        statementUpdate.setString(3, (String)map.get("productVersion"));
                        statementUpdate.executeUpdate();
                    }
                }
                conn.commit();
                result = Success.of((String)map.get("productId"));
                insertNotification(event, true, null);
            } catch (SQLException e) {
                logger.error("SQLException:", e);
                conn.rollback();
                insertNotification(event, false, e.getMessage());
                result = Failure.of(new Status(SQL_EXCEPTION, e.getMessage()));
            } catch (Exception e) {
                logger.error("Exception:", e);
                conn.rollback();
                insertNotification(event, false, e.getMessage());
                result = Failure.of(new Status(GENERIC_EXCEPTION, e.getMessage()));
            }
        } catch (SQLException e) {
            logger.error("SQLException:", e);
            result = Failure.of(new Status(SQL_EXCEPTION, e.getMessage()));
        }
        return result;
    }

    @Override
    public Result<String> deleteProduct(Map<String, Object> event) {
        final String sql = "DELETE FROM product_version_t WHERE host_id = ? " +
                "AND product_id = ? AND product_version = ?";
        Result<String> result;
        Map<String, Object> map = (Map<String, Object>)event.get(PortalConstants.DATA);
        try (Connection conn = ds.getConnection()) {
            conn.setAutoCommit(false);
            try (PreparedStatement statement = conn.prepareStatement(sql)) {
                statement.setString(1, (String)event.get(Constants.HOST));
                statement.setString(2, (String)map.get("productId"));
                statement.setString(3, (String)map.get("productVersion"));


                int count = statement.executeUpdate();
                if (count == 0) {
                    throw new SQLException("failed to delete the product with id " + (String)map.get("productId"));
                }
                conn.commit();
                result = Success.of((String)map.get("productId"));
                insertNotification(event, true, null);


            } catch (SQLException e) {
                logger.error("SQLException:", e);
                conn.rollback();
                insertNotification(event, false, e.getMessage());
                result = Failure.of(new Status(SQL_EXCEPTION, e.getMessage()));
            }
            catch (Exception e) {
                logger.error("Exception:", e);
                conn.rollback();
                insertNotification(event, false, e.getMessage());
                result = Failure.of(new Status(GENERIC_EXCEPTION, e.getMessage()));
            }
        } catch (SQLException e) {
            logger.error("SQLException:", e);
            result = Failure.of(new Status(SQL_EXCEPTION, e.getMessage()));
        }
        return result;
    }

    @Override
    public Result<String> getProduct(int offset, int limit, String hostId, String productId, String productVersion,
                                     String light4jVersion, Boolean breakCode, Boolean breakConfig, String releaseNote,
                                     String versionDesc, String releaseType, Boolean current, String versionStatus) {
        Result<String> result = null;
        StringBuilder sqlBuilder = new StringBuilder();
        sqlBuilder.append("SELECT COUNT(*) OVER () AS total,\n" +
                "host_id, product_id, product_version, light4j_version, break_code, break_config, release_note,\n" +
                "version_desc, release_type, current, version_status, update_user, update_ts\n" +
                "FROM product_version_t\n" +
                "WHERE 1=1\n");

        List<Object> parameters = new ArrayList<>();

        StringBuilder whereClause = new StringBuilder();
        addCondition(whereClause, parameters, "host_id", hostId != null ? UUID.fromString(hostId) : null);
        addCondition(whereClause, parameters, "product_id", productId);
        addCondition(whereClause, parameters, "product_version", productVersion);
        addCondition(whereClause, parameters, "light4j_version", light4jVersion);
        addCondition(whereClause, parameters, "break_code", breakCode);
        addCondition(whereClause, parameters, "break_config", breakConfig);
        addCondition(whereClause, parameters, "release_note", releaseNote);
        addCondition(whereClause, parameters, "version_desc", versionDesc);
        addCondition(whereClause, parameters, "release_type", releaseType);
        addCondition(whereClause, parameters, "current", current);
        addCondition(whereClause, parameters, "version_status", versionStatus);

        if (whereClause.length() > 0) {
            sqlBuilder.append("AND ").append(whereClause);
        }

        sqlBuilder.append(" ORDER BY product_id, product_version DESC\n" +
                "LIMIT ? OFFSET ?");

        parameters.add(limit);
        parameters.add(offset);

        String sql = sqlBuilder.toString();
        int total = 0;
        List<Map<String, Object>> products = new ArrayList<>();

        try (Connection connection = ds.getConnection();
             PreparedStatement preparedStatement = connection.prepareStatement(sql)) {

            for (int i = 0; i < parameters.size(); i++) {
                preparedStatement.setObject(i + 1, parameters.get(i));
            }

            boolean isFirstRow = true;
            try (ResultSet resultSet = preparedStatement.executeQuery()) {
                while (resultSet.next()) {
                    Map<String, Object> map = new HashMap<>();
                    if (isFirstRow) {
                        total = resultSet.getInt("total");
                        isFirstRow = false;
                    }
                    map.put("hostId", resultSet.getObject("host_id", UUID.class));
                    map.put("productId", resultSet.getString("product_id"));
                    map.put("productVersion", resultSet.getString("product_version"));
                    map.put("light4jVersion", resultSet.getString("light4j_version"));
                    map.put("breakCode", resultSet.getBoolean("break_code"));
                    map.put("breakConfig", resultSet.getBoolean("break_config"));
                    map.put("releaseNote", resultSet.getString("release_note"));
                    map.put("versionDesc", resultSet.getString("version_desc"));
                    map.put("releaseType", resultSet.getString("release_type"));
                    map.put("current", resultSet.getBoolean("current"));
                    map.put("versionStatus", resultSet.getString("version_status"));
                    map.put("updateUser", resultSet.getString("update_user"));
                    // handling date properly
                    map.put("updateTs", resultSet.getTimestamp("update_ts") != null ? resultSet.getTimestamp("update_ts").toString() : null);
                    products.add(map);
                }
            }

            Map<String, Object> resultMap = new HashMap<>();
            resultMap.put("total", total);
            resultMap.put("products", products);
            result = Success.of(JsonMapper.toJson(resultMap));

        } catch (SQLException e) {
            logger.error("SQLException:", e);
            result = Failure.of(new Status(SQL_EXCEPTION, e.getMessage()));
        } catch (Exception e) {
            logger.error("Exception:", e);
            result = Failure.of(new Status(GENERIC_EXCEPTION, e.getMessage()));
        }
        return result;
    }

    @Override
    public Result<String> getProductIdLabel(String hostId) {
        Result<String> result = null;
        String sql = "SELECT DISTINCT product_id FROM product_version_t WHERE host_id = ?";
        List<Map<String, Object>> labels = new ArrayList<>();
        try (Connection connection = ds.getConnection();
             PreparedStatement preparedStatement = connection.prepareStatement(sql)) {
            preparedStatement.setObject(1, UUID.fromString(hostId));
            try (ResultSet resultSet = preparedStatement.executeQuery()) {
                while (resultSet.next()) {
                    Map<String, Object> map = new HashMap<>();
                    map.put("id", resultSet.getString("product_id"));
                    map.put("label", resultSet.getString("product_id"));
                    labels.add(map);
                }
            }
            result = Success.of(JsonMapper.toJson(labels));
        } catch (SQLException e) {
            logger.error("SQLException:", e);
            result = Failure.of(new Status(SQL_EXCEPTION, e.getMessage()));
        } catch (Exception e) {
            logger.error("Exception:", e);
            result = Failure.of(new Status(GENERIC_EXCEPTION, e.getMessage()));
        }
        return result;
    }

    @Override
    public Result<String> getProductVersionLabel(String hostId, String productId) {
        Result<String> result = null;
        String sql = "SELECT product_version FROM product_version_t WHERE host_id = ? AND product_id = ?";
        List<Map<String, Object>> versions = new ArrayList<>();
        try (Connection connection = ds.getConnection();
             PreparedStatement preparedStatement = connection.prepareStatement(sql)) {
            preparedStatement.setObject(1, UUID.fromString(hostId));
            preparedStatement.setString(2, productId);
            try (ResultSet resultSet = preparedStatement.executeQuery()) {
                while (resultSet.next()) {
                    Map<String, Object> map = new HashMap<>();
                    String id = resultSet.getString("product_version");
                    map.put("id", id);
                    map.put("label", id);
                    versions.add(map);
                }
            }
            result = Success.of(JsonMapper.toJson(versions));
        } catch (SQLException e) {
            logger.error("SQLException:", e);
            result = Failure.of(new Status(SQL_EXCEPTION, e.getMessage()));
        } catch (Exception e) {
            logger.error("Exception:", e);
            result = Failure.of(new Status(GENERIC_EXCEPTION, e.getMessage()));
        }
        return result;
    }

    @Override
    public Result<String> createInstance(Map<String, Object> event) {
        final String sql = "INSERT INTO instance_t(host_id, instance_id, instance_name, product_id, product_version, " +
                "service_id, api_id, api_version, environment, pipeline_id, service_desc, instance_desc, tag_id, update_user, update_ts) " +
                "VALUES (?, ?, ?, ?, ?,  ?, ?, ?, ?, ?,  ?, ?, ?, ?, ?)";
        Result<String> result;
        Map<String, Object> map = (Map<String, Object>)event.get(PortalConstants.DATA);
        try (Connection conn = ds.getConnection()) {
            conn.setAutoCommit(false);
            try (PreparedStatement statement = conn.prepareStatement(sql)) {
                statement.setString(1, (String)event.get(Constants.HOST));
                statement.setObject(2, UUID.fromString((String)map.get("instanceId")));
                statement.setString(3, (String)map.get("instanceName"));
                statement.setString(4, (String)map.get("productId"));
                statement.setString(5, (String)map.get("productVersion"));
                statement.setString(6, (String)map.get("serviceId"));

                if (map.containsKey("apiId")) {
                    statement.setString(7, (String) map.get("apiId"));
                } else {
                    statement.setNull(7, Types.VARCHAR);
                }
                if (map.containsKey("apiVersion")) {
                    statement.setString(8, (String) map.get("apiVersion"));
                } else {
                    statement.setNull(8, Types.VARCHAR);
                }

                if (map.containsKey("environment")) {
                    statement.setString(9, (String) map.get("environment"));
                } else {
                    statement.setNull(9, Types.VARCHAR);
                }
                statement.setString(10, (String)map.get("pipelineId"));
                if (map.containsKey("serviceDesc")) {
                    statement.setString(11, (String) map.get("serviceDesc"));
                } else {
                    statement.setNull(11, Types.VARCHAR);
                }
                if(map.containsKey("instanceDesc")) {
                    statement.setString(12, (String) map.get("instanceDesc"));
                } else {
                    statement.setNull(12, Types.VARCHAR);
                }
                if(map.containsKey("tagId")) {
                    statement.setObject(13, UUID.fromString((String)map.get("tagId")));
                } else {
                    statement.setNull(13, Types.VARCHAR);
                }
                statement.setString(14, (String)event.get(Constants.USER));
                statement.setObject(15, OffsetDateTime.parse((String)event.get(CloudEventV1.TIME)));

                int count = statement.executeUpdate();
                if (count == 0) {
                    throw new SQLException("failed to insert the instance with id " + map.get("instanceId"));
                }
                conn.commit();
                result = Success.of((String)map.get("instanceId"));
                insertNotification(event, true, null);

            } catch (SQLException e) {
                logger.error("SQLException:", e);
                conn.rollback();
                insertNotification(event, false, e.getMessage());
                result = Failure.of(new Status(SQL_EXCEPTION, e.getMessage()));
            } catch (Exception e) {
                logger.error("Exception:", e);
                conn.rollback();
                insertNotification(event, false, e.getMessage());
                result = Failure.of(new Status(GENERIC_EXCEPTION, e.getMessage()));
            }
        } catch (SQLException e) {
            logger.error("SQLException:", e);
            result = Failure.of(new Status(SQL_EXCEPTION, e.getMessage()));
        }
        return result;
    }

    @Override
    public Result<String> updateInstance(Map<String, Object> event) {
        final String sql = "UPDATE instance_t SET instance_name = ?, product_id = ?, product_version = ?, service_id = ?, " +
                "api_id = ?, api_version = ?, environment = ?, pipeline_id = ?, " +
                "service_desc = ?, instance_desc = ?, tag_id = ?, update_user = ?, update_ts = ? " +
                "WHERE host_id = ? and instance_id = ?";
        Result<String> result;
        Map<String, Object> map = (Map<String, Object>)event.get(PortalConstants.DATA);
        try (Connection conn = ds.getConnection()) {
            conn.setAutoCommit(false);
            try (PreparedStatement statement = conn.prepareStatement(sql)) {
                statement.setString(1, (String)map.get("instanceName"));
                statement.setString(2, (String)map.get("productId"));
                statement.setString(3, (String)map.get("productVersion"));
                statement.setString(4, (String)map.get("serviceId"));
                if (map.containsKey("apiId")) {
                    statement.setString(5, (String) map.get("apiId"));
                } else {
                    statement.setNull(5, Types.VARCHAR);
                }
                if (map.containsKey("apiVersion")) {
                    statement.setString(6, (String) map.get("apiVersion"));
                } else {
                    statement.setNull(6, Types.VARCHAR);
                }
                if (map.containsKey("environment")) {
                    statement.setString(7, (String) map.get("environment"));
                } else {
                    statement.setNull(7, Types.VARCHAR);
                }
                statement.setString(8, (String)map.get("pipelineId"));
                if (map.containsKey("serviceDesc")) {
                    statement.setString(9, (String) map.get("serviceDesc"));
                } else {
                    statement.setNull(9, Types.VARCHAR);
                }
                if(map.containsKey("instanceDesc")) {
                    statement.setString(10, (String) map.get("instanceDesc"));
                } else {
                    statement.setNull(10, Types.VARCHAR);
                }
                if(map.containsKey("tagId")) {
                    statement.setObject(11, UUID.fromString((String) map.get("tagId")));
                } else {
                    statement.setNull(11, Types.VARCHAR);
                }
                statement.setString(12, (String)event.get(Constants.USER));
                statement.setObject(13, OffsetDateTime.parse((String)event.get(CloudEventV1.TIME)));
                statement.setString(14, (String)event.get(Constants.HOST));
                statement.setObject(15, UUID.fromString((String)map.get("instanceId")));

                int count = statement.executeUpdate();
                if (count == 0) {
                    throw new SQLException("failed to update the instance with id " + map.get("instanceId"));
                }
                conn.commit();
                result = Success.of((String)map.get("instanceId"));
                insertNotification(event, true, null);


            } catch (SQLException e) {
                logger.error("SQLException:", e);
                conn.rollback();
                insertNotification(event, false, e.getMessage());
                result = Failure.of(new Status(SQL_EXCEPTION, e.getMessage()));
            }  catch (Exception e) {
                logger.error("Exception:", e);
                conn.rollback();
                insertNotification(event, false, e.getMessage());
                result = Failure.of(new Status(GENERIC_EXCEPTION, e.getMessage()));
            }
        } catch (SQLException e) {
            logger.error("SQLException:", e);
            result = Failure.of(new Status(SQL_EXCEPTION, e.getMessage()));
        }
        return result;
    }

    @Override
    public Result<String> deleteInstance(Map<String, Object> event) {
        final String sql = "DELETE FROM instance_t WHERE host_id = ? AND instance_id = ?";
        Result<String> result;
        Map<String, Object> map = (Map<String, Object>)event.get(PortalConstants.DATA);
        try (Connection conn = ds.getConnection()) {
            conn.setAutoCommit(false);
            try (PreparedStatement statement = conn.prepareStatement(sql)) {
                statement.setString(1, (String)event.get(Constants.HOST));
                statement.setObject(2, UUID.fromString((String)map.get("instanceId")));

                int count = statement.executeUpdate();
                if (count == 0) {
                    throw new SQLException("failed to delete the instance with id " + map.get("instanceId"));
                }
                conn.commit();
                result = Success.of((String)map.get("instanceId"));
                insertNotification(event, true, null);

            } catch (SQLException e) {
                logger.error("SQLException:", e);
                conn.rollback();
                insertNotification(event, false, e.getMessage());
                result = Failure.of(new Status(SQL_EXCEPTION, e.getMessage()));
            }
            catch (Exception e) {
                logger.error("Exception:", e);
                conn.rollback();
                insertNotification(event, false, e.getMessage());
                result = Failure.of(new Status(GENERIC_EXCEPTION, e.getMessage()));
            }
        } catch (SQLException e) {
            logger.error("SQLException:", e);
            result = Failure.of(new Status(SQL_EXCEPTION, e.getMessage()));
        }
        return result;
    }

    @Override
    public Result<String> getInstance(int offset, int limit, String hostId, String instanceId, String instanceName,
                                      String productId, String productVersion, String serviceId, String apiId, String apiVersion,
                                      String environment, String pipelineId, String serviceDesc, String instanceDesc, String tagId) {
        Result<String> result = null;
        StringBuilder sqlBuilder = new StringBuilder();
        sqlBuilder.append("SELECT COUNT(*) OVER () AS total,\n" +
                "host_id, instance_id, instance_name, product_id, product_version, service_id, api_id, api_version, " +
                "environment, pipeline_id, service_desc, instance_desc, tag_id, update_user, update_ts \n" +
                "FROM instance_t\n" +
                "WHERE 1=1\n");

        List<Object> parameters = new ArrayList<>();

        StringBuilder whereClause = new StringBuilder();

        addCondition(whereClause, parameters, "host_id", hostId != null ? UUID.fromString(hostId) : null);
        addCondition(whereClause, parameters, "instance_id", instanceId != null ? UUID.fromString(instanceId) : null);
        addCondition(whereClause, parameters, "instance_name", instanceName);
        addCondition(whereClause, parameters, "product_id", productId);
        addCondition(whereClause, parameters, "product_version", productVersion);
        addCondition(whereClause, parameters, "service_id", serviceId);
        addCondition(whereClause, parameters, "api_id", apiId);
        addCondition(whereClause, parameters, "api_version", apiVersion);
        addCondition(whereClause, parameters, "environment", environment);
        addCondition(whereClause, parameters, "pipeline_id", pipelineId);
        addCondition(whereClause, parameters, "service_desc", serviceDesc);
        addCondition(whereClause, parameters, "instance_desc", instanceDesc);
        addCondition(whereClause, parameters, "tag_id", tagId != null ? UUID.fromString(tagId) : null);


        if (!whereClause.isEmpty()) {
            sqlBuilder.append("AND ").append(whereClause);
        }

        sqlBuilder.append(" ORDER BY instance_id\n" +
                "LIMIT ? OFFSET ?");

        parameters.add(limit);
        parameters.add(offset);

        String sql = sqlBuilder.toString();
        int total = 0;
        List<Map<String, Object>> instances = new ArrayList<>();

        try (Connection connection = ds.getConnection();
             PreparedStatement preparedStatement = connection.prepareStatement(sql)) {

            for (int i = 0; i < parameters.size(); i++) {
                preparedStatement.setObject(i + 1, parameters.get(i));
            }

            boolean isFirstRow = true;
            try (ResultSet resultSet = preparedStatement.executeQuery()) {
                while (resultSet.next()) {
                    Map<String, Object> map = new HashMap<>();
                    if (isFirstRow) {
                        total = resultSet.getInt("total");
                        isFirstRow = false;
                    }
                    map.put("hostId", resultSet.getObject("host_id", UUID.class));
                    map.put("instanceId", resultSet.getObject("instance_id", UUID.class));
                    map.put("instanceName", resultSet.getString("instance_name"));
                    map.put("productId", resultSet.getString("product_id"));
                    map.put("productVersion", resultSet.getString("product_version"));
                    map.put("serviceId", resultSet.getString("service_id"));
                    map.put("apiId", resultSet.getString("api_id"));
                    map.put("apiVersion", resultSet.getString("api_version"));
                    map.put("environment", resultSet.getString("environment"));
                    map.put("pipelineId", resultSet.getString("pipeline_id"));
                    map.put("serviceDesc", resultSet.getString("service_desc"));
                    map.put("instanceDesc", resultSet.getString("instance_desc"));
                    map.put("tagId", resultSet.getObject("tag_id", UUID.class));
                    map.put("updateUser", resultSet.getString("update_user"));
                    // handling date properly
                    map.put("updateTs", resultSet.getTimestamp("update_ts") != null ? resultSet.getTimestamp("update_ts").toString() : null);
                    instances.add(map);
                }
            }
            Map<String, Object> resultMap = new HashMap<>();
            resultMap.put("total", total);
            resultMap.put("instances", instances);
            result = Success.of(JsonMapper.toJson(resultMap));


        } catch (SQLException e) {
            logger.error("SQLException:", e);
            result = Failure.of(new Status(SQL_EXCEPTION, e.getMessage()));
        }  catch (Exception e) {
            logger.error("Exception:", e);
            result = Failure.of(new Status(GENERIC_EXCEPTION, e.getMessage()));
        }
        return result;
    }

    @Override
    public Result<String> getInstanceLabel(String hostId) {
        Result<String> result = null;
        String sql = "SELECT instance_id, instance_name FROM instance_t WHERE host_id = ?";
        List<Map<String, Object>> labels = new ArrayList<>();
        try (Connection connection = ds.getConnection();
             PreparedStatement preparedStatement = connection.prepareStatement(sql)) {
            preparedStatement.setObject(1, UUID.fromString(hostId));
            try (ResultSet resultSet = preparedStatement.executeQuery()) {
                while (resultSet.next()) {
                    Map<String, Object> map = new HashMap<>();
                    map.put("id", resultSet.getString("instance_id"));
                    map.put("label", resultSet.getString("instance_name"));
                    labels.add(map);
                }
            }
            result = Success.of(JsonMapper.toJson(labels));
        } catch (SQLException e) {
            logger.error("SQLException:", e);
            result = Failure.of(new Status(SQL_EXCEPTION, e.getMessage()));
        } catch (Exception e) {
            logger.error("Exception:", e);
            result = Failure.of(new Status(GENERIC_EXCEPTION, e.getMessage()));
        }
        return result;
    }


    @Override
    public Result<String> createPipeline(Map<String, Object> event) {
        final String sql = "INSERT INTO pipeline_t(host_id, pipeline_id, platform_id, endpoint, request_schema, response_schema, update_user, update_ts) " +
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?)";
        Result<String> result;
        Map<String, Object> map = (Map<String, Object>)event.get(PortalConstants.DATA);
        try (Connection conn = ds.getConnection()) {
            conn.setAutoCommit(false);
            try (PreparedStatement statement = conn.prepareStatement(sql)) {
                statement.setString(1, (String)event.get(Constants.HOST));
                statement.setString(2, (String)map.get("pipelineId"));
                statement.setObject(3, UUID.fromString((String)map.get("platformId")));
                statement.setString(4, (String)map.get("endpoint"));
                statement.setString(5, (String)map.get("requestSchema"));
                statement.setString(6, (String)map.get("responseSchema"));
                statement.setString(7, (String)event.get(Constants.USER));
                statement.setObject(8, OffsetDateTime.parse((String)event.get(CloudEventV1.TIME)));

                int count = statement.executeUpdate();
                if (count == 0) {
                    throw new SQLException("failed to insert the pipeline with id " + map.get("pipelineId"));
                }
                conn.commit();
                result = Success.of((String)map.get("pipelineId"));
                insertNotification(event, true, null);
            }   catch (SQLException e) {
                logger.error("SQLException:", e);
                conn.rollback();
                insertNotification(event, false, e.getMessage());
                result = Failure.of(new Status(SQL_EXCEPTION, e.getMessage()));
            } catch (Exception e) {
                logger.error("Exception:", e);
                conn.rollback();
                insertNotification(event, false, e.getMessage());
                result = Failure.of(new Status(GENERIC_EXCEPTION, e.getMessage()));
            }
        } catch (SQLException e) {
            logger.error("SQLException:", e);
            result = Failure.of(new Status(SQL_EXCEPTION, e.getMessage()));
        }
        return result;
    }

    @Override
    public Result<String> updatePipeline(Map<String, Object> event) {
        final String sql = "UPDATE pipeline_t SET platform_id = ?, endpoint = ?, request_schema = ?, response_schema = ?, update_user = ?, update_ts = ? " +
                "WHERE host_id = ? and pipeline_id = ?";
        Result<String> result;
        Map<String, Object> map = (Map<String, Object>)event.get(PortalConstants.DATA);
        try (Connection conn = ds.getConnection()) {
            conn.setAutoCommit(false);
            try (PreparedStatement statement = conn.prepareStatement(sql)) {
                statement.setObject(1, UUID.fromString((String)map.get("platformId")));
                statement.setString(2, (String)map.get("endpoint"));
                statement.setString(3, (String)map.get("requestSchema"));
                statement.setString(4, (String)map.get("responseSchema"));
                statement.setString(5,(String) event.get(Constants.USER));
                statement.setObject(6, OffsetDateTime.parse((String)event.get(CloudEventV1.TIME)));
                statement.setString(7, (String)event.get(Constants.HOST));
                statement.setString(8, (String)map.get("pipelineId"));


                int count = statement.executeUpdate();
                if (count == 0) {
                    throw new SQLException("failed to update the pipeline with id " + map.get("pipelineId"));
                }
                conn.commit();
                result = Success.of((String)map.get("pipelineId"));
                insertNotification(event, true, null);

            }  catch (SQLException e) {
                logger.error("SQLException:", e);
                conn.rollback();
                insertNotification(event, false, e.getMessage());
                result = Failure.of(new Status(SQL_EXCEPTION, e.getMessage()));
            } catch (Exception e) {
                logger.error("Exception:", e);
                conn.rollback();
                insertNotification(event, false, e.getMessage());
                result = Failure.of(new Status(GENERIC_EXCEPTION, e.getMessage()));
            }
        } catch (SQLException e) {
            logger.error("SQLException:", e);
            result = Failure.of(new Status(SQL_EXCEPTION, e.getMessage()));
        }
        return result;
    }

    @Override
    public Result<String> deletePipeline(Map<String, Object> event) {
        final String sql = "DELETE FROM pipeline_t WHERE host_id = ? AND pipeline_id = ?";
        Result<String> result;
        Map<String, Object> map = (Map<String, Object>)event.get(PortalConstants.DATA);
        try (Connection conn = ds.getConnection()) {
            conn.setAutoCommit(false);
            try (PreparedStatement statement = conn.prepareStatement(sql)) {
                statement.setString(1, (String)event.get(Constants.HOST));
                statement.setString(2, (String)map.get("pipelineId"));

                int count = statement.executeUpdate();
                if (count == 0) {
                    throw new SQLException("failed to delete the pipeline with id " + map.get("pipelineId"));
                }
                conn.commit();
                result = Success.of((String)map.get("pipelineId"));
                insertNotification(event, true, null);
            }   catch (SQLException e) {
                logger.error("SQLException:", e);
                conn.rollback();
                insertNotification(event, false, e.getMessage());
                result = Failure.of(new Status(SQL_EXCEPTION, e.getMessage()));
            }  catch (Exception e) {
                logger.error("Exception:", e);
                conn.rollback();
                insertNotification(event, false, e.getMessage());
                result = Failure.of(new Status(GENERIC_EXCEPTION, e.getMessage()));
            }
        } catch (SQLException e) {
            logger.error("SQLException:", e);
            result = Failure.of(new Status(SQL_EXCEPTION, e.getMessage()));
        }
        return result;
    }


    @Override
    public Result<String> getPipeline(int offset, int limit, String hostId, String pipelineId, String platformId, String endpoint,
                                      String requestSchema, String responseSchema) {
        Result<String> result = null;
        StringBuilder sqlBuilder = new StringBuilder();
        sqlBuilder.append("SELECT COUNT(*) OVER () AS total,\n" +
                "host_id, pipeline_id, platform_id, endpoint, request_schema, response_schema, update_user, update_ts\n" +
                "FROM pipeline_t\n" +
                "WHERE 1=1\n");

        List<Object> parameters = new ArrayList<>();

        StringBuilder whereClause = new StringBuilder();
        addCondition(whereClause, parameters, "host_id", hostId != null ? UUID.fromString(hostId) : null);
        addCondition(whereClause, parameters, "pipeline_id", pipelineId);
        addCondition(whereClause, parameters, "platform_id", platformId != null ? UUID.fromString(platformId) : null);
        addCondition(whereClause, parameters, "endpoint", endpoint);
        addCondition(whereClause, parameters, "request_schema", requestSchema);
        addCondition(whereClause, parameters, "response_schema", responseSchema);


        if (!whereClause.isEmpty()) {
            sqlBuilder.append("AND ").append(whereClause);
        }

        sqlBuilder.append(" ORDER BY pipeline_id\n" +
                "LIMIT ? OFFSET ?");

        parameters.add(limit);
        parameters.add(offset);

        String sql = sqlBuilder.toString();
        int total = 0;
        List<Map<String, Object>> pipelines = new ArrayList<>();

        try (Connection connection = ds.getConnection();
             PreparedStatement preparedStatement = connection.prepareStatement(sql)) {

            for (int i = 0; i < parameters.size(); i++) {
                preparedStatement.setObject(i + 1, parameters.get(i));
            }


            boolean isFirstRow = true;
            try (ResultSet resultSet = preparedStatement.executeQuery()) {
                while (resultSet.next()) {
                    Map<String, Object> map = new HashMap<>();
                    if (isFirstRow) {
                        total = resultSet.getInt("total");
                        isFirstRow = false;
                    }
                    map.put("hostId", resultSet.getObject("host_id", UUID.class));
                    map.put("pipelineId", resultSet.getString("pipeline_id"));
                    map.put("platformId", resultSet.getObject("platform_id", UUID.class));
                    map.put("endpoint", resultSet.getString("endpoint"));
                    map.put("requestSchema", resultSet.getString("request_schema"));
                    map.put("responseSchema", resultSet.getString("response_schema"));
                    map.put("updateUser", resultSet.getString("update_user"));
                    // handling date properly
                    map.put("updateTs", resultSet.getTimestamp("update_ts") != null ? resultSet.getTimestamp("update_ts").toString() : null);
                    pipelines.add(map);
                }
            }
            Map<String, Object> resultMap = new HashMap<>();
            resultMap.put("total", total);
            resultMap.put("pipelines", pipelines);
            result = Success.of(JsonMapper.toJson(resultMap));


        } catch (SQLException e) {
            logger.error("SQLException:", e);
            result = Failure.of(new Status(SQL_EXCEPTION, e.getMessage()));
        } catch (Exception e) {
            logger.error("Exception:", e);
            result = Failure.of(new Status(GENERIC_EXCEPTION, e.getMessage()));
        }
        return result;
    }

    @Override
    public Result<String> getPipelineLabel(String hostId) {
        Result<String> result = null;
        String sql = "SELECT pipeline_id, pipeline_id FROM pipeline_t WHERE host_id = ?";
        List<Map<String, Object>> labels = new ArrayList<>();
        try (Connection connection = ds.getConnection();
             PreparedStatement preparedStatement = connection.prepareStatement(sql)) {
            preparedStatement.setObject(1, UUID.fromString(hostId));
            try (ResultSet resultSet = preparedStatement.executeQuery()) {
                while (resultSet.next()) {
                    Map<String, Object> map = new HashMap<>();
                    String pipelineId = resultSet.getString("pipeline_id");
                    map.put("id", pipelineId);
                    map.put("label", pipelineId);
                    labels.add(map);
                }
            }
            result = Success.of(JsonMapper.toJson(labels));
        } catch (SQLException e) {
            logger.error("SQLException:", e);
            result = Failure.of(new Status(SQL_EXCEPTION, e.getMessage()));
        } catch (Exception e) {
            logger.error("Exception:", e);
            result = Failure.of(new Status(GENERIC_EXCEPTION, e.getMessage()));
        }
        return result;
    }

    @Override
    public Result<String> createPlatform(Map<String, Object> event) {
        final String sql = "INSERT INTO platform_t(host_id, platform_id, platform_name, platform_version, " +
                "client_type, client_url, credentials, proxy_url, proxy_port, environment, system_env, runtime_env, " +
                "zone, region, lob, update_user, update_ts) " +
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
        Result<String> result;
        Map<String, Object> map = (Map<String, Object>)event.get(PortalConstants.DATA);
        try (Connection conn = ds.getConnection()) {
            conn.setAutoCommit(false);
            try (PreparedStatement statement = conn.prepareStatement(sql)) {
                statement.setString(1, (String)event.get(Constants.HOST));
                statement.setObject(2, UUID.fromString((String)map.get("platformId")));
                statement.setString(3, (String)map.get("platformName"));
                statement.setString(4, (String)map.get("platformVersion"));
                statement.setString(5, (String)map.get("clientType"));
                statement.setString(6, (String)map.get("clientUrl"));
                statement.setString(7, (String)map.get("credentials"));

                if (map.containsKey("proxyUrl")) {
                    statement.setString(8, (String) map.get("proxyUrl"));
                } else {
                    statement.setNull(8, Types.VARCHAR);
                }
                if (map.containsKey("proxyPort")) {
                    statement.setInt(9, (Integer)map.get("proxyPort"));
                } else {
                    statement.setNull(9, Types.INTEGER);
                }
                if (map.containsKey("environment")) {
                    statement.setString(10, (String) map.get("environment"));
                } else {
                    statement.setNull(10, Types.VARCHAR);
                }
                if(map.containsKey("systemEnv")) {
                    statement.setString(11, (String) map.get("systemEnv"));
                } else {
                    statement.setNull(11, Types.VARCHAR);
                }
                if(map.containsKey("runtimeEnv")) {
                    statement.setString(12, (String) map.get("runtimeEnv"));
                } else {
                    statement.setNull(12, Types.VARCHAR);
                }
                if(map.containsKey("zone")) {
                    statement.setString(13, (String) map.get("zone"));
                } else {
                    statement.setNull(13, Types.VARCHAR);
                }
                if(map.containsKey("region")) {
                    statement.setString(14, (String) map.get("region"));
                } else {
                    statement.setNull(14, Types.VARCHAR);
                }
                if(map.containsKey("lob")) {
                    statement.setString(15, (String) map.get("lob"));
                } else {
                    statement.setNull(15, Types.VARCHAR);
                }
                statement.setString(16, (String)event.get(Constants.USER));
                statement.setObject(17, OffsetDateTime.parse((String)event.get(CloudEventV1.TIME)));


                int count = statement.executeUpdate();
                if (count == 0) {
                    throw new SQLException("failed to insert the platform with id " + map.get("platformId"));
                }
                conn.commit();
                result =  Success.of((String)map.get("platformId"));
                insertNotification(event, true, null);

            } catch (SQLException e) {
                logger.error("SQLException:", e);
                conn.rollback();
                insertNotification(event, false, e.getMessage());
                result = Failure.of(new Status(SQL_EXCEPTION, e.getMessage()));
            }  catch (Exception e) {
                logger.error("Exception:", e);
                conn.rollback();
                insertNotification(event, false, e.getMessage());
                result = Failure.of(new Status(GENERIC_EXCEPTION, e.getMessage()));
            }
        } catch (SQLException e) {
            logger.error("SQLException:", e);
            result = Failure.of(new Status(SQL_EXCEPTION, e.getMessage()));
        }
        return result;
    }

    @Override
    public Result<String> updatePlatform(Map<String, Object> event) {
        final String sql = "UPDATE platform_t SET platform_name = ?, platform_version = ?, " +
                "client_type = ?, client_url = ?, credentials = ?, proxy_url = ?, proxy_port = ?, " +
                "environment = ?, system_env = ?, runtime_env = ?, zone = ?, region = ?, lob = ?, " +
                "update_user = ?, update_ts = ? " +
                "WHERE host_id = ? and platform_id = ?";
        Result<String> result;
        Map<String, Object> map = (Map<String, Object>)event.get(PortalConstants.DATA);
        try (Connection conn = ds.getConnection()) {
            conn.setAutoCommit(false);
            try (PreparedStatement statement = conn.prepareStatement(sql)) {
                statement.setString(1, (String)map.get("platformName"));
                statement.setString(2, (String)map.get("platformVersion"));
                statement.setString(3, (String)map.get("clientType"));
                statement.setString(4, (String)map.get("clientUrl"));
                statement.setString(5, (String)map.get("credentials"));
                if (map.containsKey("proxyUrl")) {
                    statement.setString(6, (String) map.get("proxyUrl"));
                } else {
                    statement.setNull(6, Types.VARCHAR);
                }
                if (map.containsKey("proxyPort")) {
                    statement.setInt(7, (Integer) map.get("proxyPort"));
                } else {
                    statement.setNull(7, Types.INTEGER);
                }
                if (map.containsKey("environment")) {
                    statement.setString(8, (String) map.get("environment"));
                } else {
                    statement.setNull(8, Types.VARCHAR);
                }
                if(map.containsKey("systemEnv")) {
                    statement.setString(9, (String) map.get("systemEnv"));
                } else {
                    statement.setNull(9, Types.VARCHAR);
                }
                if(map.containsKey("runtimeEnv")) {
                    statement.setString(10, (String) map.get("runtimeEnv"));
                } else {
                    statement.setNull(10, Types.VARCHAR);
                }
                if(map.containsKey("zone")) {
                    statement.setString(11, (String) map.get("zone"));
                } else {
                    statement.setNull(11, Types.VARCHAR);
                }
                if(map.containsKey("region")) {
                    statement.setString(12, (String) map.get("region"));
                } else {
                    statement.setNull(12, Types.VARCHAR);
                }
                if(map.containsKey("lob")) {
                    statement.setString(13, (String) map.get("lob"));
                } else {
                    statement.setNull(13, Types.VARCHAR);
                }
                statement.setString(14, (String)event.get(Constants.USER));
                statement.setObject(15, OffsetDateTime.parse((String)event.get(CloudEventV1.TIME)));
                statement.setString(16, (String)event.get(Constants.HOST));
                statement.setObject(17, UUID.fromString((String)map.get("platformId")));

                int count = statement.executeUpdate();
                if (count == 0) {
                    throw new SQLException("failed to update the platform with id " + map.get("platformId"));
                }
                conn.commit();
                result = Success.of((String)map.get("platformId"));
                insertNotification(event, true, null);

            }  catch (SQLException e) {
                logger.error("SQLException:", e);
                conn.rollback();
                insertNotification(event, false, e.getMessage());
                result = Failure.of(new Status(SQL_EXCEPTION, e.getMessage()));
            }   catch (Exception e) {
                logger.error("Exception:", e);
                conn.rollback();
                insertNotification(event, false, e.getMessage());
                result = Failure.of(new Status(GENERIC_EXCEPTION, e.getMessage()));
            }
        } catch (SQLException e) {
            logger.error("SQLException:", e);
            result = Failure.of(new Status(SQL_EXCEPTION, e.getMessage()));
        }
        return result;
    }

    @Override
    public Result<String> deletePlatform(Map<String, Object> event) {
        final String sql = "DELETE FROM platform_t WHERE host_id = ? AND platform_id = ?";
        Result<String> result;
        Map<String, Object> map = (Map<String, Object>)event.get(PortalConstants.DATA);

        try (Connection conn = ds.getConnection()) {
            conn.setAutoCommit(false);
            try (PreparedStatement statement = conn.prepareStatement(sql)) {
                statement.setString(1, (String)event.get(Constants.HOST));
                statement.setObject(2, UUID.fromString((String)map.get("platformId")));

                int count = statement.executeUpdate();
                if (count == 0) {
                    throw new SQLException("failed to delete the platform with id " + map.get("platformId"));
                }
                conn.commit();
                result =  Success.of((String)map.get("platformId"));
                insertNotification(event, true, null);


            } catch (SQLException e) {
                logger.error("SQLException:", e);
                conn.rollback();
                insertNotification(event, false, e.getMessage());
                result = Failure.of(new Status(SQL_EXCEPTION, e.getMessage()));
            } catch (Exception e) {
                logger.error("Exception:", e);
                conn.rollback();
                insertNotification(event, false, e.getMessage());
                result = Failure.of(new Status(GENERIC_EXCEPTION, e.getMessage()));
            }
        } catch (SQLException e) {
            logger.error("SQLException:", e);
            result = Failure.of(new Status(SQL_EXCEPTION, e.getMessage()));
        }
        return result;
    }


    @Override
    public Result<String> getPlatform(int offset, int limit, String hostId, String platformId, String platformName, String platformVersion,
                                      String clientType, String clientUrl, String credentials, String proxyUrl, Integer proxyPort,
                                      String environment, String systemEnv, String runtimeEnv, String zone, String region, String lob) {
        Result<String> result = null;
        StringBuilder sqlBuilder = new StringBuilder();
        sqlBuilder.append("SELECT COUNT(*) OVER () AS total,\n" +
                "host_id, platform_id, platform_name, platform_version, client_type, client_url, " +
                "credentials, proxy_url, " +
                "proxy_port, environment, system_env, runtime_env, zone, region, lob, update_user, update_ts \n" +
                "FROM platform_t\n" +
                "WHERE 1=1\n");

        List<Object> parameters = new ArrayList<>();


        StringBuilder whereClause = new StringBuilder();
        addCondition(whereClause, parameters, "host_id", hostId != null ? UUID.fromString(hostId) : null);
        addCondition(whereClause, parameters, "platform_id", platformId != null ? UUID.fromString(platformId) : null);
        addCondition(whereClause, parameters, "platform_name", platformName);
        addCondition(whereClause, parameters, "platform_version", platformVersion);
        addCondition(whereClause, parameters, "client_type", clientType);
        addCondition(whereClause, parameters, "client_url", clientUrl);
        addCondition(whereClause, parameters, "credentials", credentials);
        addCondition(whereClause, parameters, "proxy_url", proxyUrl);
        addCondition(whereClause, parameters, "proxy_port", proxyPort);
        addCondition(whereClause, parameters, "environment", environment);
        addCondition(whereClause, parameters, "system_env", systemEnv);
        addCondition(whereClause, parameters, "runtime_env", runtimeEnv);
        addCondition(whereClause, parameters, "zone", zone);
        addCondition(whereClause, parameters, "region", region);
        addCondition(whereClause, parameters, "lob", lob);


        if (!whereClause.isEmpty()) {
            sqlBuilder.append("AND ").append(whereClause);
        }

        sqlBuilder.append(" ORDER BY platform_id\n" +
                "LIMIT ? OFFSET ?");


        parameters.add(limit);
        parameters.add(offset);

        String sql = sqlBuilder.toString();
        int total = 0;
        List<Map<String, Object>> platforms = new ArrayList<>();

        try (Connection connection = ds.getConnection();
             PreparedStatement preparedStatement = connection.prepareStatement(sql)) {

            for (int i = 0; i < parameters.size(); i++) {
                preparedStatement.setObject(i + 1, parameters.get(i));
            }

            boolean isFirstRow = true;
            try (ResultSet resultSet = preparedStatement.executeQuery()) {
                while (resultSet.next()) {
                    Map<String, Object> map = new HashMap<>();
                    if (isFirstRow) {
                        total = resultSet.getInt("total");
                        isFirstRow = false;
                    }
                    map.put("hostId", resultSet.getObject("host_id", UUID.class));
                    map.put("platformId", resultSet.getObject("platform_id", UUID.class));
                    map.put("platformName", resultSet.getString("platform_name"));
                    map.put("platformVersion", resultSet.getString("platform_version"));
                    map.put("clientType", resultSet.getString("client_type"));
                    map.put("clientUrl", resultSet.getString("client_url"));
                    map.put("credentials", resultSet.getString("credentials"));
                    map.put("proxyUrl", resultSet.getString("proxy_url"));
                    map.put("proxyPort", resultSet.getInt("proxy_port"));
                    map.put("environment", resultSet.getString("environment"));
                    map.put("systemEnv", resultSet.getString("system_env"));
                    map.put("runtimeEnv", resultSet.getString("runtime_env"));
                    map.put("zone", resultSet.getString("zone"));
                    map.put("region", resultSet.getString("region"));
                    map.put("lob", resultSet.getString("lob"));
                    map.put("updateUser", resultSet.getString("update_user"));
                    // handling date properly
                    map.put("updateTs", resultSet.getTimestamp("update_ts") != null ? resultSet.getTimestamp("update_ts").toString() : null);

                    platforms.add(map);
                }
            }

            Map<String, Object> resultMap = new HashMap<>();
            resultMap.put("total", total);
            resultMap.put("platforms", platforms);
            result = Success.of(JsonMapper.toJson(resultMap));

        } catch (SQLException e) {
            logger.error("SQLException:", e);
            result = Failure.of(new Status(SQL_EXCEPTION, e.getMessage()));
        } catch (Exception e) {
            logger.error("Exception:", e);
            result = Failure.of(new Status(GENERIC_EXCEPTION, e.getMessage()));
        }
        return result;
    }

    @Override
    public Result<String> getPlatformLabel(String hostId) {
        Result<String> result = null;
        String sql = "SELECT platform_id, platform_name FROM platform_t WHERE host_id = ?";
        List<Map<String, Object>> labels = new ArrayList<>();
        try (Connection connection = ds.getConnection();
             PreparedStatement preparedStatement = connection.prepareStatement(sql)) {
            preparedStatement.setObject(1, UUID.fromString(hostId));
            try (ResultSet resultSet = preparedStatement.executeQuery()) {
                while (resultSet.next()) {
                    Map<String, Object> map = new HashMap<>();
                    map.put("id", resultSet.getString("platform_id"));
                    map.put("label", resultSet.getString("platform_name"));
                    labels.add(map);
                }
            }
            result = Success.of(JsonMapper.toJson(labels));
        } catch (SQLException e) {
            logger.error("SQLException:", e);
            result = Failure.of(new Status(SQL_EXCEPTION, e.getMessage()));
        } catch (Exception e) {
            logger.error("Exception:", e);
            result = Failure.of(new Status(GENERIC_EXCEPTION, e.getMessage()));
        }
        return result;
    }

    @Override
    public Result<String> createDeployment(Map<String, Object> event) {
        final String sql = "INSERT INTO deployment_t(host_id, deployment_id, instance_id, " +
                "deployment_status, deployment_type, schedule_ts, update_user, update_ts) " +
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?)";
        Result<String> result;
        Map<String, Object> map = (Map<String, Object>)event.get(PortalConstants.DATA);
        try (Connection conn = ds.getConnection()) {
            conn.setAutoCommit(false);
            try (PreparedStatement statement = conn.prepareStatement(sql)) {
                statement.setString(1, (String)event.get(Constants.HOST));
                statement.setObject(2, UUID.fromString((String)map.get("deploymentId")));
                statement.setObject(3, UUID.fromString((String)map.get("instanceId")));
                statement.setString(4, (String)map.get("deploymentStatus"));
                statement.setString(5, (String)map.get("deploymentType"));
                statement.setObject(6, map.get("scheduleTs") != null ? OffsetDateTime.parse((String)map.get("scheduleTs")) : OffsetDateTime.parse((String)event.get(CloudEventV1.TIME)));
                statement.setString(7, (String)event.get(Constants.USER));
                statement.setObject(8, OffsetDateTime.parse((String)event.get(CloudEventV1.TIME)));

                int count = statement.executeUpdate();
                if (count == 0) {
                    throw new SQLException("failed to insert the deployment with id " + map.get("deploymentId"));
                }
                conn.commit();
                result = Success.of((String)map.get("deploymentId"));
                insertNotification(event, true, null);
            }  catch (SQLException e) {
                logger.error("SQLException:", e);
                conn.rollback();
                insertNotification(event, false, e.getMessage());
                result = Failure.of(new Status(SQL_EXCEPTION, e.getMessage()));
            } catch (Exception e) {
                logger.error("Exception:", e);
                conn.rollback();
                insertNotification(event, false, e.getMessage());
                result = Failure.of(new Status(GENERIC_EXCEPTION, e.getMessage()));
            }
        }  catch (SQLException e) {
            logger.error("SQLException:", e);
            result = Failure.of(new Status(SQL_EXCEPTION, e.getMessage()));
        }
        return result;
    }

    @Override
    public Result<String> updateDeployment(Map<String, Object> event) {
        final String sql = "UPDATE deployment_t SET instance_id = ?, deployment_status = ?, deployment_type = ?, " +
                "schedule_ts = ?, update_user = ?, update_ts = ? " +
                "WHERE host_id = ? and deployment_id = ?";
        Result<String> result;
        Map<String, Object> map = (Map<String, Object>)event.get(PortalConstants.DATA);
        OffsetDateTime offsetDateTime = OffsetDateTime.parse((String)event.get(CloudEventV1.TIME));

        try (Connection conn = ds.getConnection()) {
            conn.setAutoCommit(false);
            try (PreparedStatement statement = conn.prepareStatement(sql)) {
                statement.setObject(1, UUID.fromString((String)map.get("instanceId")));
                statement.setString(2, (String)map.get("deploymentStatus"));
                statement.setString(3, (String)map.get("deploymentType"));
                // use the event time if schedule time is not provided. We cannot use now as this event might be replayed.
                statement.setObject(4, map.get("scheduleTs") != null ? OffsetDateTime.parse((String)map.get("scheduleTs")) : OffsetDateTime.parse((String)event.get(CloudEventV1.TIME)));
                statement.setString(5, (String)event.get(Constants.USER));
                statement.setObject(6, OffsetDateTime.parse((String)event.get(CloudEventV1.TIME)));
                statement.setString(7, (String)event.get(Constants.HOST));
                statement.setObject(8, UUID.fromString((String)map.get("deploymentId")));
                int count = statement.executeUpdate();
                if (count == 0) {
                    throw new SQLException("failed to update the deployment with id " + map.get("deploymentId"));
                }
                conn.commit();
                result = Success.of((String)map.get("deploymentId"));
                insertNotification(event, true, null);
            }  catch (SQLException e) {
                logger.error("SQLException:", e);
                conn.rollback();
                insertNotification(event, false, e.getMessage());
                result = Failure.of(new Status(SQL_EXCEPTION, e.getMessage()));
            }  catch (Exception e) {
                logger.error("Exception:", e);
                conn.rollback();
                insertNotification(event, false, e.getMessage());
                result = Failure.of(new Status(GENERIC_EXCEPTION, e.getMessage()));
            }
        }   catch (SQLException e) {
            logger.error("SQLException:", e);
            result = Failure.of(new Status(SQL_EXCEPTION, e.getMessage()));
        }
        return result;
    }

    @Override
    public Result<String> updateDeploymentJobId(Map<String, Object> event) {
        final String sql = "UPDATE deployment_t SET platform_job_id = ?, update_user = ?, update_ts = ? " +
                "WHERE host_id = ? and deployment_id = ?";
        Result<String> result;
        Map<String, Object> map = (Map<String, Object>)event.get(PortalConstants.DATA);
        try (Connection conn = ds.getConnection()) {
            conn.setAutoCommit(false);
            try (PreparedStatement statement = conn.prepareStatement(sql)) {
                statement.setString(1, (String)map.get("platformJobId"));
                statement.setString(2, (String)event.get(Constants.USER));
                statement.setObject(3, OffsetDateTime.parse((String)event.get(CloudEventV1.TIME)));
                statement.setString(4, (String)event.get(Constants.HOST));
                statement.setObject(5, UUID.fromString((String)map.get("deploymentId")));
                int count = statement.executeUpdate();
                if (count == 0) {
                    throw new SQLException("failed to update the platform job id with deploymentId " + map.get("deploymentId"));
                }
                conn.commit();
                result = Success.of((String)map.get("deploymentId"));
                insertNotification(event, true, null);
            }  catch (SQLException e) {
                logger.error("SQLException:", e);
                conn.rollback();
                insertNotification(event, false, e.getMessage());
                result = Failure.of(new Status(SQL_EXCEPTION, e.getMessage()));
            }  catch (Exception e) {
                logger.error("Exception:", e);
                conn.rollback();
                insertNotification(event, false, e.getMessage());
                result = Failure.of(new Status(GENERIC_EXCEPTION, e.getMessage()));
            }
        }   catch (SQLException e) {
            logger.error("SQLException:", e);
            result = Failure.of(new Status(SQL_EXCEPTION, e.getMessage()));
        }
        return result;

    }

    @Override
    public Result<String> updateDeploymentStatus(Map<String, Object> event) {
        final String sql = "UPDATE deployment_t SET deployment_status = ?, update_user = ?, update_ts = ? " +
                "WHERE host_id = ? and deployment_id = ?";
        Result<String> result;
        Map<String, Object> map = (Map<String, Object>)event.get(PortalConstants.DATA);

        try (Connection conn = ds.getConnection()) {
            conn.setAutoCommit(false);
            try (PreparedStatement statement = conn.prepareStatement(sql)) {
                statement.setString(1, (String)map.get("deploymentStatus"));
                statement.setString(2, (String)event.get(Constants.USER));
                statement.setObject(3, OffsetDateTime.parse((String)event.get(CloudEventV1.TIME)));
                statement.setString(4, (String)event.get(Constants.HOST));
                statement.setObject(5, UUID.fromString((String)map.get("deploymentId")));
                int count = statement.executeUpdate();
                if (count == 0) {
                    throw new SQLException("failed to update the deployment status with deploymentId " + map.get("deploymentId"));
                }
                conn.commit();
                result = Success.of((String)map.get("deploymentId"));
                insertNotification(event, true, null);
            }  catch (SQLException e) {
                logger.error("SQLException:", e);
                conn.rollback();
                insertNotification(event, false, e.getMessage());
                result = Failure.of(new Status(SQL_EXCEPTION, e.getMessage()));
            }  catch (Exception e) {
                logger.error("Exception:", e);
                conn.rollback();
                insertNotification(event, false, e.getMessage());
                result = Failure.of(new Status(GENERIC_EXCEPTION, e.getMessage()));
            }
        }   catch (SQLException e) {
            logger.error("SQLException:", e);
            result = Failure.of(new Status(SQL_EXCEPTION, e.getMessage()));
        }
        return result;

    }

    @Override
    public Result<String> deleteDeployment(Map<String, Object> event) {
        final String sql = "DELETE FROM deployment_t WHERE host_id = ? AND deployment_id = ?";
        Result<String> result;
        Map<String, Object> map = (Map<String, Object>)event.get(PortalConstants.DATA);
        try (Connection conn = ds.getConnection()) {
            conn.setAutoCommit(false);
            try (PreparedStatement statement = conn.prepareStatement(sql)) {
                statement.setString(1, (String)event.get(Constants.HOST));
                statement.setObject(2, UUID.fromString((String)map.get("deploymentId")));

                int count = statement.executeUpdate();
                if (count == 0) {
                    throw new SQLException("failed to delete the deployment with id " + map.get("deploymentId"));
                }
                conn.commit();
                result = Success.of((String)map.get("deploymentId"));
                insertNotification(event, true, null);

            }   catch (SQLException e) {
                logger.error("SQLException:", e);
                conn.rollback();
                insertNotification(event, false, e.getMessage());
                result = Failure.of(new Status(SQL_EXCEPTION, e.getMessage()));
            } catch (Exception e) {
                logger.error("Exception:", e);
                conn.rollback();
                insertNotification(event, false, e.getMessage());
                result = Failure.of(new Status(GENERIC_EXCEPTION, e.getMessage()));
            }
        }  catch (SQLException e) {
            logger.error("SQLException:", e);
            result = Failure.of(new Status(SQL_EXCEPTION, e.getMessage()));
        }
        return result;
    }

    @Override
    public Result<String> getDeployment(int offset, int limit, String hostId, String deploymentId,
                                        String instanceId, String deploymentStatus,
                                        String deploymentType, String platformJobId) {
        Result<String> result = null;
        StringBuilder sqlBuilder = new StringBuilder();
        sqlBuilder.append("SELECT COUNT(*) OVER () AS total,\n" +
                "host_id, deployment_id, instance_id, deployment_status, deployment_type, " +
                "schedule_ts, platform_job_id, update_user, update_ts\n" +
                "FROM deployment_t\n" +
                "WHERE 1=1\n");

        List<Object> parameters = new ArrayList<>();

        StringBuilder whereClause = new StringBuilder();

        addCondition(whereClause, parameters, "host_id", hostId != null ? UUID.fromString(hostId) : null);
        addCondition(whereClause, parameters, "deployment_id", deploymentId != null ? UUID.fromString(deploymentId) : null);
        addCondition(whereClause, parameters, "instance_id", instanceId != null ? UUID.fromString(instanceId) : null);
        addCondition(whereClause, parameters, "deployment_status", deploymentStatus);
        addCondition(whereClause, parameters, "deployment_type", deploymentType);
        addCondition(whereClause, parameters, "platform_job_id", platformJobId);

        if (!whereClause.isEmpty()) {
            sqlBuilder.append("AND ").append(whereClause);
        }

        sqlBuilder.append(" ORDER BY deployment_id\n" +
                "LIMIT ? OFFSET ?");

        parameters.add(limit);
        parameters.add(offset);

        String sql = sqlBuilder.toString();
        int total = 0;
        List<Map<String, Object>> deployments = new ArrayList<>();

        try (Connection connection = ds.getConnection();
             PreparedStatement preparedStatement = connection.prepareStatement(sql)) {

            for (int i = 0; i < parameters.size(); i++) {
                preparedStatement.setObject(i + 1, parameters.get(i));
            }

            boolean isFirstRow = true;
            try (ResultSet resultSet = preparedStatement.executeQuery()) {
                while (resultSet.next()) {
                    Map<String, Object> map = new HashMap<>();
                    if (isFirstRow) {
                        total = resultSet.getInt("total");
                        isFirstRow = false;
                    }
                    map.put("hostId", resultSet.getObject("host_id", UUID.class));
                    map.put("deploymentId", resultSet.getObject("deployment_id", UUID.class));
                    map.put("instanceId", resultSet.getObject("instance_id", UUID.class));
                    map.put("deploymentStatus", resultSet.getString("deployment_status"));
                    map.put("deploymentType", resultSet.getString("deployment_type"));
                    ;
                    map.put("scheduleTs", resultSet.getObject("schedule_ts") != null ? resultSet.getObject("schedule_ts", OffsetDateTime.class) : null);
                    map.put("platformJobId", resultSet.getString("platform_job_id"));
                    map.put("updateUser", resultSet.getString("update_user"));
                    map.put("updateTs", resultSet.getObject("update_ts") != null ? resultSet.getObject("update_ts", OffsetDateTime.class) : null);

                    deployments.add(map);
                }
            }

            Map<String, Object> resultMap = new HashMap<>();
            resultMap.put("total", total);
            resultMap.put("deployments", deployments);
            result = Success.of(JsonMapper.toJson(resultMap));

        } catch (SQLException e) {
            logger.error("SQLException:", e);
            result = Failure.of(new Status(SQL_EXCEPTION, e.getMessage()));
        } catch (Exception e) {
            logger.error("Exception:", e);
            result = Failure.of(new Status(GENERIC_EXCEPTION, e.getMessage()));
        }
        return result;
    }

    @Override
    public Result<String> createCategory(Map<String, Object> event) {
        final String sql = "INSERT INTO category_t(host_id, category_id, entity_type, category_name, " +
                "category_desc, parent_category_id, sort_order, update_user, update_ts) " +
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)";
        Result<String> result;
        Map<String, Object> map = (Map<String, Object>)event.get(PortalConstants.DATA);
        try (Connection conn = ds.getConnection()) {
            conn.setAutoCommit(false);
            try (PreparedStatement statement = conn.prepareStatement(sql)) {
                String hostId = (String)map.get("hostId");
                if (hostId != null && !hostId.isBlank()) {
                    statement.setObject(1, UUID.fromString(hostId));
                } else {
                    statement.setNull(1, Types.VARCHAR);
                }

                statement.setObject(2, UUID.fromString((String)map.get("categoryId")));
                statement.setString(3, (String)map.get("entityType"));
                statement.setString(4, (String)map.get("categoryName"));

                String categoryDesc = (String)map.get("categoryDesc");
                if (categoryDesc != null && !categoryDesc.isBlank()) {
                    statement.setString(5, (String) map.get("categoryDesc"));
                } else {
                    statement.setNull(5, Types.VARCHAR);
                }
                String parentCategoryId = (String)map.get("parentCategoryId");
                if (parentCategoryId != null && !parentCategoryId.isBlank()) {
                    statement.setObject(6, UUID.fromString((String) map.get("parentCategoryId")));
                } else {
                    statement.setNull(6, Types.VARCHAR);
                }
                Number sortOrder = (Number)map.get("sortOrder");
                if (sortOrder != null) {
                    statement.setInt(7, sortOrder.intValue());
                } else {
                    statement.setNull(7, Types.INTEGER);
                }
                statement.setString(8, (String)event.get(Constants.USER));
                statement.setObject(9, OffsetDateTime.parse((String)event.get(CloudEventV1.TIME)));

                int count = statement.executeUpdate();
                if (count == 0) {
                    throw new SQLException("failed to insert the category with id " + map.get("categoryId"));
                }
                conn.commit();
                result =  Success.of((String)map.get("categoryId"));
                insertNotification(event, true, null);

            } catch (SQLException e) {
                logger.error("SQLException:", e);
                conn.rollback();
                insertNotification(event, false, e.getMessage());
                result = Failure.of(new Status(SQL_EXCEPTION, e.getMessage()));
            } catch (Exception e) { // Catch other potential runtime exceptions
                logger.error("Exception:", e);
                conn.rollback();
                insertNotification(event, false, e.getMessage());
                result = Failure.of(new Status(GENERIC_EXCEPTION, e.getMessage()));
            }
        } catch (SQLException e) {
            logger.error("SQLException:", e);
            result = Failure.of(new Status(SQL_EXCEPTION, e.getMessage()));
        }
        return result;
    }

    @Override
    public Result<String> updateCategory(Map<String, Object> event) {
        final String sql = "UPDATE category_t SET category_name = ?, category_desc = ?, parent_category_id = ?, " +
                "sort_order = ?, update_user = ?, update_ts = ? WHERE category_id = ?";
        Result<String> result;
        Map<String, Object> map = (Map<String, Object>)event.get(PortalConstants.DATA);
        try (Connection conn = ds.getConnection()) {
            conn.setAutoCommit(false);
            try (PreparedStatement statement = conn.prepareStatement(sql)) {
                statement.setString(1, (String)map.get("categoryName"));

                String categoryDesc = (String)map.get("categoryDesc");
                if (categoryDesc != null && !categoryDesc.isBlank()) {
                    statement.setString(2, categoryDesc);
                } else {
                    statement.setNull(2, Types.VARCHAR);
                }
                String parentCategoryId = (String)map.get("parentCategoryId");
                if (parentCategoryId != null && !parentCategoryId.isBlank()) {
                    statement.setObject(3, UUID.fromString((String) map.get("parentCategoryId")));
                } else {
                    statement.setNull(3, Types.VARCHAR);
                }
                Number sortOrder = (Number)map.get("sortOrder");
                if (sortOrder != null) {
                    statement.setInt(4, sortOrder.intValue());
                } else {
                    statement.setNull(4, Types.INTEGER);
                }
                statement.setString(5, (String)event.get(Constants.USER));
                statement.setObject(6, OffsetDateTime.parse((String)event.get(CloudEventV1.TIME)));
                statement.setObject(7, UUID.fromString((String)map.get("categoryId")));

                int count = statement.executeUpdate();
                if (count == 0) {
                    throw new SQLException("failed to update the category with id " + map.get("categoryId"));
                }
                conn.commit();
                result =  Success.of((String)map.get("categoryId"));
                insertNotification(event, true, null);

            } catch (SQLException e) {
                logger.error("SQLException:", e);
                conn.rollback();
                insertNotification(event, false, e.getMessage());
                result = Failure.of(new Status(SQL_EXCEPTION, e.getMessage()));
            } catch (Exception e) { // Catch other potential runtime exceptions
                logger.error("Exception:", e);
                conn.rollback();
                insertNotification(event, false, e.getMessage());
                result = Failure.of(new Status(GENERIC_EXCEPTION, e.getMessage()));
            }
        } catch (SQLException e) {
            logger.error("SQLException:", e);
            result = Failure.of(new Status(SQL_EXCEPTION, e.getMessage()));
        }
        return result;
    }

    @Override
    public Result<String> deleteCategory(Map<String, Object> event) {
        final String sql = "DELETE FROM category_t WHERE category_id = ?";
        Result<String> result;
        Map<String, Object> map = (Map<String, Object>)event.get(PortalConstants.DATA);
        String categoryId = (String) map.get("categoryId");
        try (Connection conn = ds.getConnection()) {
            conn.setAutoCommit(false);
            try (PreparedStatement statement = conn.prepareStatement(sql)) {
                statement.setObject(1, UUID.fromString(categoryId));
                int count = statement.executeUpdate();
                if (count == 0) {
                    throw new SQLException("failed to delete the category with id " + categoryId);
                }
                conn.commit();
                result =  Success.of(categoryId);
                insertNotification(event, true, null);
            } catch (SQLException e) {
                logger.error("SQLException:", e);
                conn.rollback();
                insertNotification(event, false, e.getMessage());
                result = Failure.of(new Status(SQL_EXCEPTION, e.getMessage()));
            } catch (Exception e) { // Catch other potential runtime exceptions
                logger.error("Exception:", e);
                conn.rollback();
                insertNotification(event, false, e.getMessage());
                result = Failure.of(new Status(GENERIC_EXCEPTION, e.getMessage()));
            }
        } catch (SQLException e) {
            logger.error("SQLException:", e);
            result = Failure.of(new Status(SQL_EXCEPTION, e.getMessage()));
        }
        return result;
    }

    @Override
    public Result<String> getCategory(int offset, int limit, String hostId, String categoryId, String entityType,
                                      String categoryName, String categoryDesc, String parentCategoryId,
                                      String parentCategoryName, Integer sortOrder) {
        Result<String> result = null;
        StringBuilder sqlBuilder = new StringBuilder();
        sqlBuilder.append("SELECT COUNT(*) OVER () AS total,\n" +
                "cat.category_id, cat.host_id, cat.entity_type, cat.category_name, cat.category_desc, cat.parent_category_id, \n" +
                "cat.sort_order, cat.update_user, cat.update_ts,\n" +
                "parent_cat.category_name AS parent_category_name\n" + // Select parent category name
                "FROM category_t cat\n" +
                "LEFT JOIN category_t parent_cat ON cat.parent_category_id = parent_cat.category_id\n" + // Self-join
                "WHERE ");

        List<Object> parameters = new ArrayList<>();
        // Use a separate list to build condition strings to manage AND correctly
        List<String> conditions = new ArrayList<>();

        // --- Handle host_id condition first ---
        if (hostId != null && !hostId.isEmpty()) {
            conditions.add("(cat.host_id = ? OR cat.host_id IS NULL)");
            parameters.add(UUID.fromString(hostId));
        } else {
            conditions.add("cat.host_id IS NULL");
            // No parameter for IS NULL
        }

        // --- Add other conditions using the helper ---
        addConditionToList(conditions, parameters, "cat.category_id", categoryId != null ? UUID.fromString(categoryId) : null);
        addConditionToList(conditions, parameters, "cat.entity_type", entityType);
        addConditionToList(conditions, parameters, "cat.category_name", categoryName);
        addConditionToList(conditions, parameters, "cat.category_desc", categoryDesc); // Consider LIKE here if needed
        addConditionToList(conditions, parameters, "cat.parent_category_id", parentCategoryId != null ? UUID.fromString(parentCategoryId) : null);
        // parentCategoryName is derived, not filtered here
        addConditionToList(conditions, parameters, "cat.sort_order", sortOrder);

        // --- Join conditions with AND ---
        sqlBuilder.append(String.join(" AND ", conditions));

        // --- Add ORDER BY, LIMIT, OFFSET ---
        sqlBuilder.append(" ORDER BY cat.category_name\n" +
                "LIMIT ? OFFSET ?");

        parameters.add(limit);
        parameters.add(offset);

        String sql = sqlBuilder.toString();
        // if(logger.isTraceEnabled()) logger.trace("sql = {}", sql);
        int total = 0;
        List<Map<String, Object>> categories = new ArrayList<>();

        try (Connection connection = ds.getConnection();
             PreparedStatement preparedStatement = connection.prepareStatement(sql)) {

            for (int i = 0; i < parameters.size(); i++) {
                preparedStatement.setObject(i + 1, parameters.get(i));
            }

            boolean isFirstRow = true;
            try (ResultSet resultSet = preparedStatement.executeQuery()) {
                while (resultSet.next()) {
                    Map<String, Object> map = new HashMap<>();
                    if (isFirstRow) {
                        total = resultSet.getInt("total");
                        isFirstRow = false;
                    }
                    map.put("categoryId", resultSet.getObject("category_id", UUID.class));
                    map.put("hostId", resultSet.getObject("host_id", UUID.class));
                    map.put("entityType", resultSet.getString("entity_type"));
                    map.put("categoryName", resultSet.getString("category_name"));
                    map.put("categoryDesc", resultSet.getString("category_desc"));
                    map.put("parentCategoryId", resultSet.getObject("parent_category_id", UUID.class));
                    map.put("parentCategoryName", resultSet.getString("parent_category_name")); // Get parent category name from join
                    map.put("sortOrder", resultSet.getInt("sort_order"));
                    map.put("updateUser", resultSet.getString("update_user"));
                    map.put("updateTs", resultSet.getObject("update_ts") != null ? resultSet.getObject("update_ts", OffsetDateTime.class) : null);

                    categories.add(map);
                }
            }

            Map<String, Object> resultMap = new HashMap<>();
            resultMap.put("total", total);
            resultMap.put("categories", categories);
            result = Success.of(JsonMapper.toJson(resultMap));

        } catch (SQLException e) {
            logger.error("SQLException:", e);
            result = Failure.of(new Status(SQL_EXCEPTION, e.getMessage()));
        } catch (Exception e) {
            logger.error("Exception:", e);
            result = Failure.of(new Status(GENERIC_EXCEPTION, e.getMessage()));
        }
        return result;
    }

    /**
     * Helper method to add a condition string and parameter to lists
     * if the value is not null.
     * @param conditions List of condition strings (e.g., "col = ?")
     * @param parameters List of parameters for PreparedStatement
     * @param columnName Database column name
     * @param columnValue Column value from method parameters
     */
    private void addConditionToList(List<String> conditions, List<Object> parameters, String columnName, Object columnValue) {
        if (columnValue != null) {
            // Treat empty strings as no filter for VARCHAR columns if desired
            if (columnValue instanceof String && ((String) columnValue).isEmpty()) {
                return; // Skip empty string filters
            }
            conditions.add(columnName + " LIKE ?");
            parameters.add("%" + columnValue + "%");
        }
    }

    @Override
    public Result<String> getCategoryLabel(String hostId) {
        Result<String> result = null;
        String sql = "SELECT category_id, category_name FROM category_t WHERE host_id = ? OR host_id IS NULL";
        List<Map<String, Object>> labels = new ArrayList<>();
        try (Connection connection = ds.getConnection();
             PreparedStatement preparedStatement = connection.prepareStatement(sql)) {
            preparedStatement.setObject(1, UUID.fromString(hostId));
            try (ResultSet resultSet = preparedStatement.executeQuery()) {
                while (resultSet.next()) {
                    Map<String, Object> map = new HashMap<>();
                    map.put("id", resultSet.getString("category_id"));
                    map.put("label", resultSet.getString("category_name"));
                    labels.add(map);
                }
            }
            result = Success.of(JsonMapper.toJson(labels));
        } catch (SQLException e) {
            logger.error("SQLException:", e);
            result = Failure.of(new Status(SQL_EXCEPTION, e.getMessage()));
        } catch (Exception e) {
            logger.error("Exception:", e);
            result = Failure.of(new Status(GENERIC_EXCEPTION, e.getMessage()));
        }
        return result;
    }

    @Override
    public Result<String> getCategoryById(String categoryId) {
        Result<String> result = null;
        String sql = "SELECT category_id, host_id, entity_type, category_name, category_desc, parent_category_id, " +
                "sort_order, update_user, update_ts FROM category_t WHERE category_id = ?";
        Map<String, Object> map = null;
        try (Connection connection = ds.getConnection();
             PreparedStatement preparedStatement = connection.prepareStatement(sql)) {
            preparedStatement.setObject(1, UUID.fromString(categoryId));
            try (ResultSet resultSet = preparedStatement.executeQuery()) {
                if (resultSet.next()) {
                    map = new HashMap<>();
                    map.put("categoryId", resultSet.getObject("category_id", UUID.class));
                    map.put("hostId", resultSet.getObject("host_id", UUID.class));
                    map.put("entityType", resultSet.getString("entity_type"));
                    map.put("categoryName", resultSet.getString("category_name"));
                    map.put("categoryDesc", resultSet.getString("category_desc"));
                    map.put("parentCategoryId", resultSet.getObject("parent_category_id", UUID.class));
                    map.put("sortOrder", resultSet.getInt("sort_order"));
                    map.put("updateUser", resultSet.getString("update_user"));
                    map.put("updateTs", resultSet.getObject("update_ts") != null ? resultSet.getObject("update_ts", OffsetDateTime.class) : null);
                }
            }
            if (map != null && !map.isEmpty()) {
                result = Success.of(JsonMapper.toJson(map));
            }
        } catch (SQLException e) {
            logger.error("SQLException:", e);
            result = Failure.of(new Status(SQL_EXCEPTION, e.getMessage()));
        } catch (Exception e) {
            logger.error("Exception:", e);
            result = Failure.of(new Status(GENERIC_EXCEPTION, e.getMessage()));
        }
        return result;
    }

    @Override
    public Result<String> getCategoryByName(String hostId, String categoryName) {
        Result<String> result = null;
        StringBuilder sqlBuilder = new StringBuilder();
        sqlBuilder.append("SELECT category_id, host_id, entity_type, category_name, category_desc, parent_category_id, \n" +
                "sort_order, update_user, update_ts\n" +
                "FROM category_t\n" +
                "WHERE category_name = ?\n"); // Filter by category_name

        if (hostId != null && !hostId.isEmpty()) {
            sqlBuilder.append("AND (host_id = ? OR host_id IS NULL)"); // Tenant-specific OR Global
        } else {
            sqlBuilder.append("AND host_id IS NULL"); // Only Global if hostId is null/empty
        }

        String sql = sqlBuilder.toString();
        Map<String, Object> map = null;
        try (Connection connection = ds.getConnection();
             PreparedStatement preparedStatement = connection.prepareStatement(sql)) {

            int parameterIndex = 1;
            preparedStatement.setString(parameterIndex++, categoryName); // 1. categoryName

            if (hostId != null && !hostId.isEmpty()) {
                preparedStatement.setObject(parameterIndex++, UUID.fromString(hostId)); // 2. hostId (for tenant-specific OR global)
            }

            try (ResultSet resultSet = preparedStatement.executeQuery()) {
                if (resultSet.next()) {
                    map = new HashMap<>();
                    map.put("categoryId", resultSet.getObject("category_id", UUID.class));
                    map.put("hostId", resultSet.getObject("host_id", UUID.class));
                    map.put("entityType", resultSet.getString("entity_type"));
                    map.put("categoryName", resultSet.getString("category_name"));
                    map.put("categoryDesc", resultSet.getString("category_desc"));
                    map.put("parentCategoryId", resultSet.getObject("parent_category_id", UUID.class));
                    map.put("sortOrder", resultSet.getInt("sort_order"));
                    map.put("updateUser", resultSet.getString("update_user"));
                    map.put("updateTs", resultSet.getObject("update_ts") != null ? resultSet.getObject("update_ts", OffsetDateTime.class) : null);
                }
            }
            if (map != null && !map.isEmpty()) {
                result = Success.of(JsonMapper.toJson(map));
            }
        } catch (SQLException e) {
            logger.error("SQLException:", e);
            result = Failure.of(new Status(SQL_EXCEPTION, e.getMessage()));
        } catch (Exception e) {
            logger.error("Exception:", e);
            result = Failure.of(new Status(GENERIC_EXCEPTION, e.getMessage()));
        }
        return result;
    }

    @Override
    public Result<String> getCategoryByType(String hostId, String entityType) {
        Result<String> result = null;
        StringBuilder sqlBuilder = new StringBuilder();
        sqlBuilder.append("SELECT category_id, host_id, entity_type, category_name, category_desc, parent_category_id, \n" +
                "sort_order, update_user, update_ts\n" +
                "FROM category_t\n" +
                "WHERE entity_type = ?\n"); // Filter by entity_type

        if (hostId != null && !hostId.isEmpty()) {
            sqlBuilder.append("AND (host_id = ? OR host_id IS NULL)"); // Tenant-specific OR Global
        } else {
            sqlBuilder.append("AND host_id IS NULL"); // Only Global if hostId is null/empty
        }

        String sql = sqlBuilder.toString();
        List<Map<String, Object>> categories = new ArrayList<>();
        try (Connection connection = ds.getConnection();
             PreparedStatement preparedStatement = connection.prepareStatement(sql)) {

            int parameterIndex = 1;
            preparedStatement.setString(parameterIndex++, entityType); // 1. entityType
            if (hostId != null && !hostId.isEmpty()) {
                preparedStatement.setObject(parameterIndex++, UUID.fromString(hostId)); // 2. hostId (for tenant-specific OR global)
            }

            try (ResultSet resultSet = preparedStatement.executeQuery()) {
                while (resultSet.next()) {
                    Map<String, Object> map = new HashMap<>();
                    map.put("categoryId", resultSet.getObject("category_id", UUID.class));
                    map.put("hostId", resultSet.getObject("host_id", UUID.class));
                    map.put("entityType", resultSet.getString("entity_type"));
                    map.put("categoryName", resultSet.getString("category_name"));
                    map.put("categoryDesc", resultSet.getString("category_desc"));
                    map.put("parentCategoryId", resultSet.getObject("parent_category_id", UUID.class));
                    map.put("sortOrder", resultSet.getInt("sort_order"));
                    map.put("updateUser", resultSet.getString("update_user"));
                    map.put("updateTs", resultSet.getObject("update_ts") != null ? resultSet.getObject("update_ts", OffsetDateTime.class) : null);

                    categories.add(map);
                }
            }

            Map<String, Object> resultMap = new HashMap<>();
            resultMap.put("categories", categories);
            result = Success.of(JsonMapper.toJson(resultMap));

        } catch (SQLException e) {
            logger.error("SQLException:", e);
            result = Failure.of(new Status(SQL_EXCEPTION, e.getMessage()));
        } catch (Exception e) {
            logger.error("Exception:", e);
            result = Failure.of(new Status(GENERIC_EXCEPTION, e.getMessage()));
        }
        return result;
    }

    @Override
    public Result<String> getCategoryTree(String hostId, String entityType) {
        Result<String> result = null;
        StringBuilder sqlBuilder = new StringBuilder();
        sqlBuilder.append("SELECT cat.category_id, cat.host_id, cat.entity_type, cat.category_name, cat.category_desc, cat.parent_category_id, \n" +
                "cat.sort_order, cat.update_user, cat.update_ts\n" +
                "FROM category_t cat\n" +
                "WHERE cat.entity_type = ?\n"); // Filter by entity_type

        if (hostId != null && !hostId.isEmpty()) {
            sqlBuilder.append("AND (cat.host_id = ? OR cat.host_id IS NULL)"); // Tenant-specific OR Global
        } else {
            sqlBuilder.append("AND cat.host_id IS NULL"); // Only Global if hostId is null/empty
        }

        sqlBuilder.append(" ORDER BY cat.sort_order, cat.category_name"); // Order for tree consistency

        String sql = sqlBuilder.toString();
        List<Map<String, Object>> categoryList = new ArrayList<>(); // Flat list initially

        try (Connection connection = ds.getConnection();
             PreparedStatement preparedStatement = connection.prepareStatement(sql)) {

            int parameterIndex = 1;
            preparedStatement.setString(parameterIndex++, entityType); // 1. entityType
            if (hostId != null && !hostId.isEmpty()) {
                preparedStatement.setObject(parameterIndex++, UUID.fromString(hostId)); // 2. hostId (for tenant-specific OR global)
            }

            try (ResultSet resultSet = preparedStatement.executeQuery()) {
                while (resultSet.next()) {
                    Map<String, Object> map = new HashMap<>();
                    map.put("categoryId", resultSet.getObject("category_id", UUID.class));
                    map.put("hostId", resultSet.getObject("host_id", UUID.class));
                    map.put("entityType", resultSet.getString("entity_type"));
                    map.put("categoryName", resultSet.getString("category_name"));
                    map.put("categoryDesc", resultSet.getString("category_desc"));
                    map.put("parentCategoryId", resultSet.getObject("parent_category_id", UUID.class));
                    map.put("sortOrder", resultSet.getInt("sort_order"));
                    map.put("updateUser", resultSet.getString("update_user"));
                    map.put("updateTs", resultSet.getObject("update_ts") != null ? resultSet.getObject("update_ts", OffsetDateTime.class) : null);
                    map.put("children", new ArrayList<>()); // Initialize children list for tree structure

                    categoryList.add(map);
                }
            }

            // Build the category tree structure
            List<Map<String, Object>> categoryTree = buildCategoryTree(categoryList);

            Map<String, Object> resultMap = new HashMap<>();
            resultMap.put("categories", categoryTree);
            result = Success.of(JsonMapper.toJson(resultMap));

        } catch (SQLException e) {
            logger.error("SQLException:", e);
            result = Failure.of(new Status(SQL_EXCEPTION, e.getMessage()));
        } catch (Exception e) {
            logger.error("Exception:", e);
            result = Failure.of(new Status(GENERIC_EXCEPTION, e.getMessage()));
        }
        return result;
    }

    /**
     * Helper method to build the category tree from a flat list of categories.
     *
     * @param categoryList Flat list of category maps.
     * @return List of root category maps representing the tree structure.
     */
    private List<Map<String, Object>> buildCategoryTree(List<Map<String, Object>> categoryList) {
        Map<String, Map<String, Object>> categoryLookup = new HashMap<>(); // For quick lookup by categoryId
        List<Map<String, Object>> rootCategories = new ArrayList<>();

        // 1. Populate the lookup map for efficient access by categoryId
        for (Map<String, Object> category : categoryList) {
            categoryLookup.put((String) category.get("categoryId"), category);
        }

        // 2. Iterate again to build the tree structure
        for (Map<String, Object> category : categoryList) {
            String parentCategoryId = (String) category.get("parentCategoryId");
            if (parentCategoryId != null && !parentCategoryId.isEmpty()) {
                // If it has a parent, add it as a child to the parent category
                Map<String, Object> parentCategory = categoryLookup.get(parentCategoryId);
                if (parentCategory != null && !parentCategory.isEmpty()) {
                    ((List<Map<String, Object>>) parentCategory.get("children")).add(category);
                } else {
                    logger.warn("Parent category not found for categoryId: {}, parentCategoryId: {}", category.get("categoryId"), parentCategoryId);
                    // Handle missing parent category (e.g., log warning, add to root, skip, etc.)
                    rootCategories.add(category); // Add to root as fallback if parent is missing?
                }
            } else {
                // If no parent, it's a root category
                rootCategories.add(category);
            }
        }
        return rootCategories;
    }

    @Override
    public Result<String> createSchema(Map<String, Object> event) {
        final String sql = "INSERT INTO schema_t(host_id, schema_id, schema_version, schema_type, spec_version, schema_source, schema_name, schema_desc, schema_body, schema_owner, schema_status, example, comment_status, update_user, update_ts) " +
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
        final String insertSchemaCategorySql = "INSERT INTO entity_category_t (entity_id, entity_type, category_id) VALUES (?, ?, ?)"; // Added SQL for entity_category_t
        final String insertSchemaTagSql = "INSERT INTO entity_tag_t (entity_id, entity_type, tag_id) VALUES (?, ?, ?)"; // Added SQL for entity_tag_t

        Result<String> result;
        Map<String, Object> map = (Map<String, Object>)event.get(PortalConstants.DATA);
        String schemaId = (String) map.get("schemaId"); // Get schemaId for return/logging/error
        List<String> categoryIds = (List<String>) map.get("categoryId"); // Get categoryIds from event data if present
        List<String> tagIds = (List<String>) map.get("tagIds"); // Get tagIds from event data if present

        try (Connection conn = ds.getConnection()) {
            conn.setAutoCommit(false);
            try (PreparedStatement statement = conn.prepareStatement(sql)) {
                String hostId = (String)map.get("hostId");
                if (hostId != null && !hostId.isBlank()) {
                    statement.setObject(1, UUID.fromString(hostId));
                } else {
                    statement.setNull(1, Types.VARCHAR);
                }

                statement.setString(2, schemaId); // Required
                statement.setString(3, (String)map.get("schemaVersion")); // Required
                statement.setString(4, (String)map.get("schemaType")); // Required
                statement.setString(5, (String)map.get("specVersion")); // Required
                statement.setString(6, (String)map.get("schemaSource")); // Required
                statement.setString(7, (String)map.get("schemaName")); // Required

                String schemaDesc = (String)map.get("schemaDesc");
                if (schemaDesc != null && !schemaDesc.isBlank()) {
                    statement.setString(8, schemaDesc);
                } else {
                    statement.setNull(8, Types.VARCHAR);
                }
                statement.setString(9, (String)map.get("schemaBody")); // Required
                statement.setString(10, (String)map.get("schemaOwner")); // Required
                statement.setString(11, (String)map.get("schemaStatus")); // Required

                String example = (String)map.get("example");
                if (example != null && !example.isBlank()) {
                    statement.setString(12, example);
                } else {
                    statement.setNull(12, Types.VARCHAR);
                }
                statement.setString(13, (String)map.get("commentStatus")); // Required
                statement.setString(14, (String)event.get(Constants.USER));
                statement.setObject(15, OffsetDateTime.parse((String)event.get(CloudEventV1.TIME)));

                int count = statement.executeUpdate();
                if (count == 0) {
                    throw new SQLException("failed to insert the schema with id " + map.get("schemaId"));
                }

                // Insert into entity_categories_t if categoryId is present
                if (categoryIds != null && !categoryIds.isEmpty()) {
                    try (PreparedStatement insertCategoryStatement = conn.prepareStatement(insertSchemaCategorySql)) {
                        for (String categoryId : categoryIds) {
                            insertCategoryStatement.setString(1, schemaId);
                            insertCategoryStatement.setString(2, "schema"); // entity_type = "schema"
                            insertCategoryStatement.setObject(3, UUID.fromString(categoryId));
                            insertCategoryStatement.addBatch(); // Batch inserts for efficiency
                        }
                        insertCategoryStatement.executeBatch(); // Execute batch insert
                    }
                }
                // Insert into entity_tag_t if tagIds are present
                if (tagIds != null && !tagIds.isEmpty()) {
                    try (PreparedStatement insertTagStatement = conn.prepareStatement(insertSchemaTagSql)) {
                        for (String tagId : tagIds) {
                            insertTagStatement.setString(1, schemaId);
                            insertTagStatement.setString(2, "schema"); // entity_type = "schema"
                            insertTagStatement.setObject(3, UUID.fromString(tagId));
                            insertTagStatement.addBatch(); // Batch inserts for efficiency
                        }
                        insertTagStatement.executeBatch(); // Execute batch insert
                    }
                }

                conn.commit();
                result =  Success.of((String)map.get("schemaId")); // Return schemaId
                insertNotification(event, true, null);

            } catch (SQLException e) {
                logger.error("SQLException:", e);
                conn.rollback();
                insertNotification(event, false, e.getMessage());
                result = Failure.of(new Status(SQL_EXCEPTION, e.getMessage()));
            } catch (Exception e) { // Catch other potential runtime exceptions
                logger.error("Exception:", e);
                conn.rollback();
                insertNotification(event, false, e.getMessage());
                result = Failure.of(new Status(GENERIC_EXCEPTION, e.getMessage()));
            }
        } catch (SQLException e) {
            logger.error("SQLException:", e);
            result = Failure.of(new Status(SQL_EXCEPTION, e.getMessage()));
        }
        return result;
    }

    @Override
    public Result<String> updateSchema(Map<String, Object> event) {
        final String sql = "UPDATE schema_t SET schema_version = ?, schema_type = ?, spec_version = ?, schema_source = ?, schema_name = ?, schema_desc = ?, schema_body = ?, schema_owner = ?, schema_status = ?, example = ?, comment_status = ?, update_user = ?, update_ts = ? WHERE schema_id = ?";
        final String deleteSchemaCategorySql = "DELETE FROM entity_category_t WHERE entity_id = ? AND entity_type = ?";
        final String insertSchemaCategorySql = "INSERT INTO entity_category_t (entity_id, entity_type, category_id) VALUES (?, ?, ?)";
        final String deleteSchemaTagSql = "DELETE FROM entity_tag_t WHERE entity_id = ? AND entity_type = ?";
        final String insertSchemaTagSql = "INSERT INTO entity_tag_t (entity_id, entity_type, tag_id) VALUES (?, ?, ?)";

        Result<String> result;
        Map<String, Object> map = (Map<String, Object>)event.get(PortalConstants.DATA);
        String schemaId = (String) map.get("schemaId");
        List<String> categoryIds = (List<String>) map.get("categoryIds");
        List<String> tagIds = (List<String>) map.get("tagIds");

        try (Connection conn = ds.getConnection()) {
            conn.setAutoCommit(false);
            try (PreparedStatement statement = conn.prepareStatement(sql)) {
                statement.setString(1, (String)map.get("schemaVersion"));
                statement.setString(2, (String)map.get("schemaType"));
                statement.setString(3, (String)map.get("specVersion"));
                statement.setString(4, (String)map.get("schemaSource"));
                statement.setString(5, (String)map.get("schemaName"));
                String schemaDesc = (String)map.get("schemaDesc");
                if (schemaDesc != null && !schemaDesc.isBlank()) {
                    statement.setString(6, schemaDesc);
                } else {
                    statement.setNull(6, Types.VARCHAR);
                }
                statement.setString(7, (String)map.get("schemaBody"));
                statement.setString(8, (String)map.get("schemaOwner"));
                statement.setString(9, (String)map.get("schemaStatus"));
                String example = (String)map.get("example");
                if (example != null && !example.isBlank()) {
                    statement.setString(10, example);
                } else {
                    statement.setNull(10, Types.VARCHAR);
                }
                statement.setString(11, (String)map.get("commentStatus"));
                statement.setString(12, (String)event.get(Constants.USER));
                statement.setObject(13, OffsetDateTime.parse((String)event.get(CloudEventV1.TIME)));
                statement.setString(14, schemaId);

                int count = statement.executeUpdate();
                if (count == 0) {
                    throw new SQLException("failed to update the schema with id " + schemaId);
                }
                // --- Replace Category Associations ---
                // 1. Delete existing links for this schema
                try (PreparedStatement deleteCategoryStatement = conn.prepareStatement(deleteSchemaCategorySql)) {
                    deleteCategoryStatement.setString(1, schemaId);
                    deleteCategoryStatement.setString(2, "schema"); // entity_type = "schema"
                    deleteCategoryStatement.executeUpdate(); // Execute delete (no need to check count for DELETE)
                }

                // 2. Insert new links if categoryIds are provided in the event
                if (categoryIds != null && !categoryIds.isEmpty()) {
                    try (PreparedStatement insertCategoryStatement = conn.prepareStatement(insertSchemaCategorySql)) {
                        for (String categoryId : categoryIds) {
                            insertCategoryStatement.setString(1, schemaId);
                            insertCategoryStatement.setString(2, "schema"); // entity_type = "schema"
                            insertCategoryStatement.setObject(3, UUID.fromString(categoryId));
                            insertCategoryStatement.addBatch(); // Batch inserts for efficiency
                        }
                        insertCategoryStatement.executeBatch(); // Execute batch insert
                    }
                }
                // --- End Replace Category Associations ---

                // --- Replace Tag Associations ---
                // 1. Delete existing links for this schema
                try (PreparedStatement deleteTagStatement = conn.prepareStatement(deleteSchemaTagSql)) {
                    deleteTagStatement.setString(1, schemaId);
                    deleteTagStatement.setString(2, "schema"); // entity_type = "schema"
                    deleteTagStatement.executeUpdate();
                }

                // 2. Insert new links if tagIds are provided in the event
                if (tagIds != null && !tagIds.isEmpty()) {
                    try (PreparedStatement insertTagStatement = conn.prepareStatement(insertSchemaTagSql)) {
                        for (String tagId : tagIds) {
                            insertTagStatement.setString(1, schemaId);
                            insertTagStatement.setString(2, "schema"); // entity_type = "schema"
                            insertTagStatement.setObject(3, UUID.fromString(tagId));
                            insertTagStatement.addBatch(); // Batch inserts for efficiency
                        }
                        insertTagStatement.executeBatch(); // Execute batch insert
                    }
                }
                // --- End Replace Tag Associations ---

                conn.commit();
                result =  Success.of(schemaId); // Return schemaId
                insertNotification(event, true, null);

            } catch (SQLException e) {
                logger.error("SQLException:", e);
                conn.rollback();
                insertNotification(event, false, e.getMessage());
                result = Failure.of(new Status(SQL_EXCEPTION, e.getMessage()));
            } catch (Exception e) { // Catch other potential runtime exceptions
                logger.error("Exception:", e);
                conn.rollback();
                insertNotification(event, false, e.getMessage());
                result = Failure.of(new Status(GENERIC_EXCEPTION, e.getMessage()));
            }
        } catch (SQLException e) {
            logger.error("SQLException:", e);
            result = Failure.of(new Status(SQL_EXCEPTION, e.getMessage()));
        }
        return result;
    }

    @Override
    public Result<String> deleteSchema(Map<String, Object> event) {
        final String sql = "DELETE FROM schema_t WHERE schema_id = ?";
        Result<String> result;
        Map<String, Object> map = (Map<String, Object>)event.get(PortalConstants.DATA);
        String schemaId = (String) map.get("schemaId");
        try (Connection conn = ds.getConnection()) {
            conn.setAutoCommit(false);
            try (PreparedStatement statement = conn.prepareStatement(sql)) {
                statement.setString(1, schemaId);

                int count = statement.executeUpdate();
                if (count == 0) {
                    throw new SQLException("failed to delete the schema with id " + schemaId);
                }
                conn.commit();
                result =  Success.of(schemaId); // Return schemaId
                insertNotification(event, true, null);

            } catch (SQLException e) {
                logger.error("SQLException:", e);
                conn.rollback();
                insertNotification(event, false, e.getMessage());
                result = Failure.of(new Status(SQL_EXCEPTION, e.getMessage()));
            } catch (Exception e) { // Catch other potential runtime exceptions
                logger.error("Exception:", e);
                conn.rollback();
                insertNotification(event, false, e.getMessage());
                result = Failure.of(new Status(GENERIC_EXCEPTION, e.getMessage()));
            }
        } catch (SQLException e) {
            logger.error("SQLException:", e);
            result = Failure.of(new Status(SQL_EXCEPTION, e.getMessage()));
        }
        return result;
    }

    @Override
    public Result<String> getSchema(int offset, int limit, String hostId, String schemaId, String schemaVersion, String schemaType,
                                    String specVersion, String schemaSource, String schemaName, String schemaDesc, String schemaBody,
                                    String schemaOwner, String schemaStatus, String example, String commentStatus) {
        Result<String> result = null;
        StringBuilder sqlBuilder = new StringBuilder();
        sqlBuilder.append("SELECT COUNT(*) OVER () AS total,\n" +
                "schema_id, host_id, schema_version, schema_type, spec_version, schema_source, schema_name, schema_desc, schema_body, \n" +
                "schema_owner, schema_status, example, comment_status, update_user, update_ts\n" +
                "FROM schema_t\n" +
                "WHERE 1=1\n");

        List<Object> parameters = new ArrayList<>();
        StringBuilder whereClause = new StringBuilder();

        addCondition(whereClause, parameters, "host_id", hostId != null ? UUID.fromString(hostId) : null);
        addCondition(whereClause, parameters, "schema_id", schemaId);
        addCondition(whereClause, parameters, "schema_version", schemaVersion);
        addCondition(whereClause, parameters, "schema_type", schemaType);
        addCondition(whereClause, parameters, "spec_version", specVersion);
        addCondition(whereClause, parameters, "schema_source", schemaSource);
        addCondition(whereClause, parameters, "schema_name", schemaName);
        addCondition(whereClause, parameters, "schema_desc", schemaDesc);
        // schemaBody is usually not used in get list query for performance reasons
        addCondition(whereClause, parameters, "schema_owner", schemaOwner);
        addCondition(whereClause, parameters, "schema_status", schemaStatus);
        // categoryId is not a column in schema_t table, so it is ignored in WHERE clause
        addCondition(whereClause, parameters, "example", example);
        addCondition(whereClause, parameters, "comment_status", commentStatus);


        if (!whereClause.isEmpty()) {
            sqlBuilder.append("AND ").append(whereClause);
        }

        sqlBuilder.append(" ORDER BY schema_name\n" +
                "LIMIT ? OFFSET ?");

        parameters.add(limit);
        parameters.add(offset);

        String sql = sqlBuilder.toString();
        int total = 0;
        List<Map<String, Object>> schemas = new ArrayList<>();

        try (Connection connection = ds.getConnection();
             PreparedStatement preparedStatement = connection.prepareStatement(sql)) {

            for (int i = 0; i < parameters.size(); i++) {
                preparedStatement.setObject(i + 1, parameters.get(i));
            }

            boolean isFirstRow = true;
            try (ResultSet resultSet = preparedStatement.executeQuery()) {
                while (resultSet.next()) {
                    Map<String, Object> map = new HashMap<>();
                    if (isFirstRow) {
                        total = resultSet.getInt("total");
                        isFirstRow = false;
                    }
                    map.put("schemaId", resultSet.getString("schema_id"));
                    map.put("hostId", resultSet.getObject("host_id", UUID.class));
                    map.put("schemaVersion", resultSet.getString("schema_version"));
                    map.put("schemaType", resultSet.getString("schema_type"));
                    map.put("specVersion", resultSet.getString("spec_version"));
                    map.put("schemaSource", resultSet.getString("schema_source"));
                    map.put("schemaName", resultSet.getString("schema_name"));
                    map.put("schemaDesc", resultSet.getString("schema_desc"));
                    // schemaBody is usually not returned in get list query for performance reasons
                    // map.put("schemaBody", resultSet.getString("schema_body"));
                    map.put("schemaOwner", resultSet.getString("schema_owner"));
                    map.put("schemaStatus", resultSet.getString("schema_status"));
                    map.put("example", resultSet.getString("example"));
                    map.put("commentStatus", resultSet.getString("comment_status"));
                    map.put("updateUser", resultSet.getString("update_user"));
                    map.put("updateTs", resultSet.getObject("update_ts") != null ? resultSet.getObject("update_ts", OffsetDateTime.class) : null);

                    schemas.add(map);
                }
            }

            Map<String, Object> resultMap = new HashMap<>();
            resultMap.put("total", total);
            resultMap.put("schemas", schemas);
            result = Success.of(JsonMapper.toJson(resultMap));

        } catch (SQLException e) {
            logger.error("SQLException:", e);
            result = Failure.of(new Status(SQL_EXCEPTION, e.getMessage()));
        } catch (Exception e) {
            logger.error("Exception:", e);
            result = Failure.of(new Status(GENERIC_EXCEPTION, e.getMessage()));
        }
        return result;
    }

    @Override
    public Result<String> getSchemaLabel(String hostId) {
        Result<String> result = null;
        StringBuilder sqlBuilder = new StringBuilder();
        sqlBuilder.append("SELECT schema_id, schema_name FROM schema_t WHERE 1=1 "); // Base query

        if (hostId != null && !hostId.isEmpty()) {
            sqlBuilder.append("AND (host_id = ? OR host_id IS NULL)"); // Tenant-specific OR Global
        } else {
            sqlBuilder.append("AND host_id IS NULL"); // Only Global if hostId is null/empty
        }

        String sql = sqlBuilder.toString();
        List<Map<String, Object>> labels = new ArrayList<>();
        try (Connection connection = ds.getConnection();
             PreparedStatement preparedStatement = connection.prepareStatement(sql)) {

            if (hostId != null && !hostId.isEmpty()) {
                preparedStatement.setObject(1, UUID.fromString(hostId)); // Set hostId parameter if provided
            }

            try (ResultSet resultSet = preparedStatement.executeQuery()) {
                while (resultSet.next()) {
                    Map<String, Object> map = new HashMap<>();
                    map.put("id", resultSet.getString("schema_id"));
                    map.put("label", resultSet.getString("schema_name"));
                    labels.add(map);
                }
            }
            result = Success.of(JsonMapper.toJson(labels));
        } catch (SQLException e) {
            logger.error("SQLException:", e);
            result = Failure.of(new Status(SQL_EXCEPTION, e.getMessage()));
        } catch (Exception e) {
            logger.error("Exception:", e);
            result = Failure.of(new Status(GENERIC_EXCEPTION, e.getMessage()));
        }
        return result;
    }


    @Override
    public Result<String> getSchemaById(String schemaId) {
        Result<String> result = null;
        String sql = "SELECT schema_id, host_id, schema_version, schema_type, spec_version, schema_source, schema_name, schema_desc, schema_body, " +
                "schema_owner, schema_status, example, comment_status, update_user, update_ts FROM schema_t WHERE schema_id = ?";
        Map<String, Object> map = null;
        try (Connection conn = ds.getConnection()) {
            conn.setAutoCommit(false);
            try (PreparedStatement statement = conn.prepareStatement(sql)) {
                statement.setString(1, schemaId);
                try (ResultSet resultSet = statement.executeQuery()) {
                    if (resultSet.next()) {
                        map = new HashMap<>();
                        map.put("schemaId", resultSet.getString("schema_id"));
                        map.put("hostId", resultSet.getObject("host_id", UUID.class));
                        map.put("schemaVersion", resultSet.getString("schema_version"));
                        map.put("schemaType", resultSet.getString("schema_type"));
                        map.put("specVersion", resultSet.getString("spec_version"));
                        map.put("schemaSource", resultSet.getString("schema_source"));
                        map.put("schemaName", resultSet.getString("schema_name"));
                        map.put("schemaDesc", resultSet.getString("schema_desc"));
                        map.put("schemaBody", resultSet.getString("schema_body"));
                        map.put("schemaOwner", resultSet.getString("schema_owner"));
                        map.put("schemaStatus", resultSet.getString("schema_status"));
                        map.put("example", resultSet.getString("example"));
                        map.put("commentStatus", resultSet.getString("comment_status"));
                        map.put("updateUser", resultSet.getString("update_user"));
                        map.put("updateTs", resultSet.getObject("update_ts") != null ? resultSet.getObject("update_ts", OffsetDateTime.class) : null);
                    }
                }
                if (map != null && !map.isEmpty()) {
                    result = Success.of(JsonMapper.toJson(map));
                } else {
                    result = Success.of(null); // Or perhaps Failure.of(NOT_FOUND Status) if schema must exist
                }
            }
        } catch (SQLException e) {
            logger.error("SQLException:", e);
            result = Failure.of(new Status(SQL_EXCEPTION, e.getMessage()));
        } catch (Exception e) {
            logger.error("Exception:", e);
            result = Failure.of(new Status(GENERIC_EXCEPTION, e.getMessage()));
        }
        return result;
    }

    @Override
    public Result<String> getSchemaByCategoryId(String categoryId) {
        Result<String> result = null;
        StringBuilder sqlBuilder = new StringBuilder();
        sqlBuilder.append("SELECT schema_t.schema_id, schema_t.host_id, schema_t.schema_version, schema_t.schema_type, schema_t.spec_version, schema_t.schema_source, \n" +
                "schema_t.schema_name, schema_t.schema_desc, schema_t.schema_body, \n" +
                "schema_t.schema_owner, schema_t.schema_status, schema_t.example, schema_t.comment_status, schema_t.update_user, schema_t.update_ts\n" +
                "FROM schema_t\n" +
                "INNER JOIN entity_category_t ON schema_t.schema_id = entity_category_t.entity_id\n" + // Join with entity_category_t
                "WHERE entity_type = 'schema' AND entity_category_t.category_id = ?"); // Filter by categoryId using join table

        List<Map<String, Object>> schemas = new ArrayList<>();
        try (Connection connection = ds.getConnection();
             PreparedStatement preparedStatement = connection.prepareStatement(sqlBuilder.toString())) {

            preparedStatement.setObject(1, UUID.fromString(categoryId));

            try (ResultSet resultSet = preparedStatement.executeQuery()) {
                while (resultSet.next()) {
                    Map<String, Object> map = new HashMap<>();
                    map.put("schemaId", resultSet.getString("schema_id"));
                    map.put("hostId", resultSet.getObject("host_id", UUID.class));
                    map.put("schemaVersion", resultSet.getString("schema_version"));
                    map.put("schemaType", resultSet.getString("schema_type"));
                    map.put("specVersion", resultSet.getString("spec_version"));
                    map.put("schemaSource", resultSet.getString("schema_source"));
                    map.put("schemaName", resultSet.getString("schema_name"));
                    map.put("schemaDesc", resultSet.getString("schema_desc"));
                    map.put("schemaBody", resultSet.getString("schema_body"));
                    map.put("schemaOwner", resultSet.getString("schema_owner"));
                    map.put("schemaStatus", resultSet.getString("schema_status"));
                    map.put("example", resultSet.getString("example"));
                    map.put("commentStatus", resultSet.getString("comment_status"));
                    map.put("updateUser", resultSet.getString("update_user"));
                    map.put("updateTs", resultSet.getObject("update_ts") != null ? resultSet.getObject("update_ts", OffsetDateTime.class) : null);

                    schemas.add(map);
                }
            }

            Map<String, Object> resultMap = new HashMap<>();
            resultMap.put("schemas", schemas);
            result = Success.of(JsonMapper.toJson(resultMap));

        } catch (SQLException e) {
            logger.error("SQLException:", e);
            result = Failure.of(new Status(SQL_EXCEPTION, e.getMessage()));
        } catch (Exception e) {
            logger.error("Exception:", e);
            result = Failure.of(new Status(GENERIC_EXCEPTION, e.getMessage()));
        }
        return result;
    }

    @Override
    public Result<String> getSchemaByTagId(String tagId) {
        Result<String> result = null;
        StringBuilder sqlBuilder = new StringBuilder();
        sqlBuilder.append("SELECT schema_t.schema_id, schema_t.host_id, schema_t.schema_version, schema_t.schema_type, schema_t.spec_version, schema_t.schema_source, \n" +
                "schema_t.schema_name, schema_t.schema_desc, schema_t.schema_body, \n" +
                "schema_t.schema_owner, schema_t.schema_status, schema_t.example, schema_t.comment_status, schema_t.update_user, schema_t.update_ts\n" +
                "FROM schema_t\n" +
                "INNER JOIN entity_tag_t ON schema_t.schema_id = entity_tag_t.entity_id\n" +
                "WHERE entity_type = 'schema' AND entity_tag_t.tag_id = ?");

        List<Map<String, Object>> schemas = new ArrayList<>();
        try (Connection connection = ds.getConnection();
             PreparedStatement preparedStatement = connection.prepareStatement(sqlBuilder.toString())) {

            preparedStatement.setObject(1, UUID.fromString(tagId));

            try (ResultSet resultSet = preparedStatement.executeQuery()) {
                while (resultSet.next()) {
                    Map<String, Object> map = new HashMap<>();
                    map.put("schemaId", resultSet.getString("schema_id"));
                    map.put("hostId", resultSet.getObject("host_id", UUID.class));
                    map.put("schemaVersion", resultSet.getString("schema_version"));
                    map.put("schemaType", resultSet.getString("schema_type"));
                    map.put("specVersion", resultSet.getString("spec_version"));
                    map.put("schemaSource", resultSet.getString("schema_source"));
                    map.put("schemaName", resultSet.getString("schema_name"));
                    map.put("schemaDesc", resultSet.getString("schema_desc"));
                    map.put("schemaBody", resultSet.getString("schema_body"));
                    map.put("schemaOwner", resultSet.getString("schema_owner"));
                    map.put("schemaStatus", resultSet.getString("schema_status"));
                    map.put("example", resultSet.getString("example"));
                    map.put("commentStatus", resultSet.getString("comment_status"));
                    map.put("updateUser", resultSet.getString("update_user"));
                    map.put("updateTs", resultSet.getObject("update_ts") != null ? resultSet.getObject("update_ts", OffsetDateTime.class) : null);

                    schemas.add(map);
                }
            }

            Map<String, Object> resultMap = new HashMap<>();
            resultMap.put("schemas", schemas);
            result = Success.of(JsonMapper.toJson(resultMap));

        } catch (SQLException e) {
            logger.error("SQLException:", e);
            result = Failure.of(new Status(SQL_EXCEPTION, e.getMessage()));
        } catch (Exception e) {
            logger.error("Exception:", e);
            result = Failure.of(new Status(GENERIC_EXCEPTION, e.getMessage()));
        }
        return result;
    }

    @Override
    public Result<String> createTag(Map<String, Object> event) {
        final String sql = "INSERT INTO tag_t(host_id, tag_id, entity_type, tag_name, " +
                "tag_desc, update_user, update_ts) " +
                "VALUES (?, ?, ?, ?, ?, ?, ?)";
        Result<String> result;
        Map<String, Object> map = (Map<String, Object>)event.get(PortalConstants.DATA);
        String tagId = (String) map.get("tagId"); // Get tagId for return/logging/error

        try (Connection conn = ds.getConnection()) {
            conn.setAutoCommit(false);
            try (PreparedStatement statement = conn.prepareStatement(sql)) {
                String hostId = (String)map.get("hostId");
                if (hostId != null && !hostId.isEmpty()) {
                    statement.setObject(1, UUID.fromString(hostId));
                } else {
                    statement.setNull(1, Types.VARCHAR);
                }

                statement.setObject(2, UUID.fromString(tagId)); // Required
                statement.setString(3, (String)map.get("entityType")); // Required
                statement.setString(4, (String)map.get("tagName")); // Required

                String tagDesc = (String)map.get("tagDesc");
                if (tagDesc != null && !tagDesc.isBlank()) {
                    statement.setString(5, tagDesc);
                } else {
                    statement.setNull(5, Types.VARCHAR);
                }

                statement.setString(6, (String)event.get(Constants.USER));
                statement.setObject(7, OffsetDateTime.parse((String)event.get(CloudEventV1.TIME)));

                int count = statement.executeUpdate();
                if (count == 0) {
                    throw new SQLException("failed to insert the tag with id " + tagId);
                }
                conn.commit();
                result =  Success.of(tagId); // Return tagId
                insertNotification(event, true, null);

            } catch (SQLException e) {
                logger.error("SQLException:", e);
                conn.rollback();
                insertNotification(event, false, e.getMessage());
                result = Failure.of(new Status(SQL_EXCEPTION, e.getMessage()));
            } catch (Exception e) { // Catch other potential runtime exceptions
                logger.error("Exception:", e);
                conn.rollback();
                insertNotification(event, false, e.getMessage());
                result = Failure.of(new Status(GENERIC_EXCEPTION, e.getMessage()));
            }
        } catch (SQLException e) {
            logger.error("SQLException:", e);
            result = Failure.of(new Status(SQL_EXCEPTION, e.getMessage()));
        }
        return result;
    }

    @Override
    public Result<String> updateTag(Map<String, Object> event) {
        final String sql = "UPDATE tag_t SET tag_name = ?, tag_desc = ?, update_user = ?, update_ts = ? WHERE tag_id = ?";
        Result<String> result;
        Map<String, Object> map = (Map<String, Object>)event.get(PortalConstants.DATA);
        String tagId = (String) map.get("tagId");

        try (Connection conn = ds.getConnection()) {
            conn.setAutoCommit(false);
            try (PreparedStatement statement = conn.prepareStatement(sql)) {
                statement.setString(1, (String)map.get("tagName"));
                String tagDesc = (String)map.get("tagDesc");
                if (tagDesc != null && !tagDesc.isBlank()) {
                    statement.setString(2, tagDesc);
                } else {
                    statement.setNull(2, Types.VARCHAR);
                }
                statement.setString(3, (String)event.get(Constants.USER));
                statement.setObject(4, OffsetDateTime.parse((String)event.get(CloudEventV1.TIME)));
                statement.setObject(5, UUID.fromString(tagId));

                int count = statement.executeUpdate();
                if (count == 0) {
                    throw new SQLException("failed to update the tag with id " + map.get("tagId"));
                }
                conn.commit();
                result =  Success.of((String)map.get("tagId")); // Return tagId
                insertNotification(event, true, null);

            } catch (SQLException e) {
                logger.error("SQLException:", e);
                conn.rollback();
                insertNotification(event, false, e.getMessage());
                result = Failure.of(new Status(SQL_EXCEPTION, e.getMessage()));
            } catch (Exception e) { // Catch other potential runtime exceptions
                logger.error("Exception:", e);
                conn.rollback();
                insertNotification(event, false, e.getMessage());
                result = Failure.of(new Status(GENERIC_EXCEPTION, e.getMessage()));
            }
        } catch (SQLException e) {
            logger.error("SQLException:", e);
            result = Failure.of(new Status(SQL_EXCEPTION, e.getMessage()));
        }
        return result;
    }

    @Override
    public Result<String> deleteTag(Map<String, Object> event) {
        final String sql = "DELETE FROM tag_t WHERE tag_id = ?";
        Result<String> result;
        Map<String, Object> map = (Map<String, Object>)event.get(PortalConstants.DATA);
        String tagId = (String) map.get("tagId");
        try (Connection conn = ds.getConnection()) {
            conn.setAutoCommit(false);
            try (PreparedStatement statement = conn.prepareStatement(sql)) {
                statement.setObject(1, UUID.fromString(tagId));

                int count = statement.executeUpdate();
                if (count == 0) {
                    throw new SQLException("failed to delete the tag with id " + map.get("tagId"));
                }
                conn.commit();
                result =  Success.of(tagId); // Return tagId
                insertNotification(event, true, null);

            } catch (SQLException e) {
                logger.error("SQLException:", e);
                conn.rollback();
                insertNotification(event, false, e.getMessage());
                result = Failure.of(new Status(SQL_EXCEPTION, e.getMessage()));
            } catch (Exception e) { // Catch other potential runtime exceptions
                logger.error("Exception:", e);
                conn.rollback();
                insertNotification(event, false, e.getMessage());
                result = Failure.of(new Status(GENERIC_EXCEPTION, e.getMessage()));
            }
        } catch (SQLException e) {
            logger.error("SQLException:", e);
            result = Failure.of(new Status(SQL_EXCEPTION, e.getMessage()));
        }
        return result;
    }

    @Override
    public Result<String> getTag(int offset, int limit, String hostId, String tagId, String entityType, String tagName, String tagDesc) {
        Result<String> result = null;
        StringBuilder sqlBuilder = new StringBuilder();
        sqlBuilder.append("SELECT COUNT(*) OVER () AS total,\n" +
                "tag_id, host_id, entity_type, tag_name, tag_desc, update_user, update_ts\n" +
                "FROM tag_t\n" +
                "WHERE 1=1\n");

        List<Object> parameters = new ArrayList<>();
        StringBuilder whereClause = new StringBuilder();

        // --- Modified host_id condition to include global tags ---
        if (hostId != null && !hostId.isEmpty()) {
            whereClause.append("("); // Start OR group
            addCondition(whereClause, parameters, "host_id", UUID.fromString(hostId)); // Tenant-specific tags
            whereClause.append(" OR host_id IS NULL)");         // Include global tags
        } else {
            // If hostId is null or empty, only fetch global tags
            addCondition(whereClause, parameters, "host_id", null); // Fetch only global
        }
        // --- Rest of the conditions ---
        addCondition(whereClause, parameters, "tag_id", tagId != null ? UUID.fromString(tagId) : null);
        addCondition(whereClause, parameters, "entity_type", entityType);
        addCondition(whereClause, parameters, "tag_name", tagName);
        addCondition(whereClause, parameters, "tag_desc", tagDesc);

        if (!whereClause.isEmpty()) {
            sqlBuilder.append("AND ").append(whereClause);
        }

        sqlBuilder.append(" ORDER BY tag_name\n" +
                "LIMIT ? OFFSET ?");

        parameters.add(limit);
        parameters.add(offset);

        String sql = sqlBuilder.toString();
        int total = 0;
        List<Map<String, Object>> tags = new ArrayList<>();

        try (Connection connection = ds.getConnection();
             PreparedStatement preparedStatement = connection.prepareStatement(sql)) {

            for (int i = 0; i < parameters.size(); i++) {
                preparedStatement.setObject(i + 1, parameters.get(i));
            }

            boolean isFirstRow = true;
            try (ResultSet resultSet = preparedStatement.executeQuery()) {
                while (resultSet.next()) {
                    Map<String, Object> map = new HashMap<>();
                    if (isFirstRow) {
                        total = resultSet.getInt("total");
                        isFirstRow = false;
                    }
                    map.put("tagId", resultSet.getObject("tag_id", UUID.class));
                    map.put("hostId", resultSet.getObject("host_id", UUID.class));
                    map.put("entityType", resultSet.getString("entity_type"));
                    map.put("tagName", resultSet.getString("tag_name"));
                    map.put("tagDesc", resultSet.getString("tag_desc"));
                    map.put("updateUser", resultSet.getString("update_user"));
                    map.put("updateTs", resultSet.getObject("update_ts") != null ? resultSet.getObject("update_ts", OffsetDateTime.class) : null);

                    tags.add(map);
                }
            }

            Map<String, Object> resultMap = new HashMap<>();
            resultMap.put("total", total);
            resultMap.put("tags", tags);
            result = Success.of(JsonMapper.toJson(resultMap));

        } catch (SQLException e) {
            logger.error("SQLException:", e);
            result = Failure.of(new Status(SQL_EXCEPTION, e.getMessage()));
        } catch (Exception e) {
            logger.error("Exception:", e);
            result = Failure.of(new Status(GENERIC_EXCEPTION, e.getMessage()));
        }
        return result;
    }

    @Override
    public Result<String> getTagLabel(String hostId) {
        Result<String> result = null;
        String sql = "SELECT tag_id, tag_name FROM tag_t WHERE host_id = ? OR host_id IS NULL";
        List<Map<String, Object>> labels = new ArrayList<>();
        try (Connection connection = ds.getConnection();
             PreparedStatement preparedStatement = connection.prepareStatement(sql)) {
            preparedStatement.setObject(1, UUID.fromString(hostId));
            try (ResultSet resultSet = preparedStatement.executeQuery()) {
                while (resultSet.next()) {
                    Map<String, Object> map = new HashMap<>();
                    map.put("id", resultSet.getString("tag_id"));
                    map.put("label", resultSet.getString("tag_name"));
                    labels.add(map);
                }
            }
            result = Success.of(JsonMapper.toJson(labels));
        } catch (SQLException e) {
            logger.error("SQLException:", e);
            result = Failure.of(new Status(SQL_EXCEPTION, e.getMessage()));
        } catch (Exception e) {
            logger.error("Exception:", e);
            result = Failure.of(new Status(GENERIC_EXCEPTION, e.getMessage()));
        }
        return result;
    }

    @Override
    public Result<String> getTagById(String tagId) {
        Result<String> result = null;
        String sql = "SELECT tag_id, host_id, entity_type, tag_name, tag_desc, update_user, update_ts FROM tag_t WHERE tag_id = ?";
        Map<String, Object> map = null;
        try (Connection conn = ds.getConnection()) {
            conn.setAutoCommit(false); // Although not strictly needed for SELECT, keeping template consistent
            try (PreparedStatement statement = conn.prepareStatement(sql)) {
                statement.setString(1, tagId);
                try (ResultSet resultSet = statement.executeQuery()) {
                    if (resultSet.next()) {
                        map = new HashMap<>();
                        map.put("tagId", resultSet.getObject("tag_id", UUID.class));
                        map.put("hostId", resultSet.getObject("host_id", UUID.class));
                        map.put("entityType", resultSet.getString("entity_type"));
                        map.put("tagName", resultSet.getString("tag_name"));
                        map.put("tagDesc", resultSet.getString("tag_desc"));
                        map.put("updateUser", resultSet.getString("update_user"));
                        map.put("updateTs", resultSet.getObject("update_ts") != null ? resultSet.getObject("update_ts", OffsetDateTime.class) : null);
                    }
                }
                if (map != null && !map.isEmpty()) {
                    result = Success.of(JsonMapper.toJson(map));
                } else {
                    result = Success.of(null); // Or perhaps Failure.of(NOT_FOUND Status) if tag must exist
                }
            }
        } catch (SQLException e) {
            logger.error("SQLException:", e);
            result = Failure.of(new Status(SQL_EXCEPTION, e.getMessage()));
        } catch (Exception e) {
            logger.error("Exception:", e);
            result = Failure.of(new Status(GENERIC_EXCEPTION, e.getMessage()));
        }
        return result;
    }


    @Override
    public Result<String> getTagByName(String hostId, String tagName) {
        Result<String> result = null;
        StringBuilder sqlBuilder = new StringBuilder();
        sqlBuilder.append("SELECT tag_id, host_id, entity_type, tag_name, tag_desc, update_user, update_ts\n" +
                "FROM tag_t\n" +
                "WHERE tag_name = ?\n"); // Filter by tagName

        if (hostId != null && !hostId.isEmpty()) {
            sqlBuilder.append("AND (host_id = ? OR host_id IS NULL)"); // Tenant-specific OR Global
        } else {
            sqlBuilder.append("AND host_id IS NULL"); // Only Global if hostId is null/empty
        }

        String sql = sqlBuilder.toString();
        Map<String, Object> map = null;
        try (Connection conn = ds.getConnection()) {
            conn.setAutoCommit(false); // Although not strictly needed for SELECT, keeping template consistent
            try (PreparedStatement preparedStatement = conn.prepareStatement(sql)) {

                int parameterIndex = 1;
                preparedStatement.setString(parameterIndex++, tagName); // 1. tagName

                if (hostId != null && !hostId.isEmpty()) {
                    preparedStatement.setObject(parameterIndex++, UUID.fromString(hostId)); // 2. hostId (for tenant-specific OR global)
                }

                try (ResultSet resultSet = preparedStatement.executeQuery()) {
                    if (resultSet.next()) {
                        map = new HashMap<>();
                        map.put("tagId", resultSet.getObject("tag_id", UUID.class));
                        map.put("hostId", resultSet.getObject("host_id", UUID.class));
                        map.put("entityType", resultSet.getString("entity_type"));
                        map.put("tagName", resultSet.getString("tag_name"));
                        map.put("tagDesc", resultSet.getString("tag_desc"));
                        map.put("updateUser", resultSet.getString("update_user"));
                        map.put("updateTs", resultSet.getObject("update_ts") != null ? resultSet.getObject("update_ts", OffsetDateTime.class) : null);
                    }
                }
                if (map != null && !map.isEmpty()) {
                    result = Success.of(JsonMapper.toJson(map));
                } else {
                    result = Failure.of(new Status(OBJECT_NOT_FOUND, tagName));
                }
            }
        } catch (SQLException e) {
            logger.error("SQLException:", e);
            result = Failure.of(new Status(SQL_EXCEPTION, e.getMessage()));
        } catch (Exception e) {
            logger.error("Exception:", e);
            result = Failure.of(new Status(GENERIC_EXCEPTION, e.getMessage()));
        }
        return result;
    }


    @Override
    public Result<String> getTagByType(String hostId, String entityType) {
        Result<String> result = null;
        StringBuilder sqlBuilder = new StringBuilder();
        sqlBuilder.append("SELECT tag_id, host_id, entity_type, tag_name, tag_desc, update_user, update_ts\n" +
                "FROM tag_t\n" +
                "WHERE entity_type = ?\n"); // Filter by entityType

        if (hostId != null && !hostId.isEmpty()) {
            sqlBuilder.append("AND (host_id = ? OR host_id IS NULL)"); // Tenant-specific OR Global
        } else {
            sqlBuilder.append("AND host_id IS NULL"); // Only Global if hostId is null/empty
        }

        String sql = sqlBuilder.toString();
        List<Map<String, Object>> tags = new ArrayList<>();
        try (Connection conn = ds.getConnection(); PreparedStatement preparedStatement = conn.prepareStatement(sql)) {
            int parameterIndex = 1;
            preparedStatement.setString(parameterIndex++, entityType); // 1. entityType

            if (hostId != null && !hostId.isEmpty()) {
                preparedStatement.setObject(parameterIndex++, UUID.fromString(hostId)); // 2. hostId (for tenant-specific OR global)
            }

            try (ResultSet resultSet = preparedStatement.executeQuery()) {
                while (resultSet.next()) {
                    Map<String, Object> map = new HashMap<>();
                    map.put("tagId", resultSet.getObject("tag_id", UUID.class));
                    map.put("hostId", resultSet.getObject("host_id", UUID.class));
                    map.put("entityType", resultSet.getString("entity_type"));
                    map.put("tagName", resultSet.getString("tag_name"));
                    map.put("tagDesc", resultSet.getString("tag_desc"));
                    map.put("updateUser", resultSet.getString("update_user"));
                    map.put("updateTs", resultSet.getObject("update_ts") != null ? resultSet.getObject("update_ts", OffsetDateTime.class) : null);

                    tags.add(map);
                }
            }
            Map<String, Object> resultMap = new HashMap<>();
            resultMap.put("tags", tags);
            result = Success.of(JsonMapper.toJson(resultMap));
        } catch (SQLException e) {
            logger.error("SQLException:", e);
            result = Failure.of(new Status(SQL_EXCEPTION, e.getMessage()));
        } catch (Exception e) {
            logger.error("Exception:", e);
            result = Failure.of(new Status(GENERIC_EXCEPTION, e.getMessage()));
        }
        return result;
    }

    @Override
    public Result<String> createSchedule(Map<String, Object> event) {
        final String sql = "INSERT INTO schedule_t(schedule_id, host_id, schedule_name, frequency_unit, frequency_time, " +
                "start_ts, event_topic, event_type, event_data, update_user, update_ts) " +
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
        Result<String> result;
        Map<String, Object> map = (Map<String, Object>)event.get(PortalConstants.DATA);
        String scheduleId = (String) map.get("scheduleId"); // Get scheduleId for PK, return, logging

        try (Connection conn = ds.getConnection()) {
            conn.setAutoCommit(false);
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
                    throw new SQLException("failed to insert the schedule with id " + scheduleId);
                }

                // Success path
                conn.commit();
                result =  Success.of(scheduleId); // Return scheduleId
                insertNotification(event, true, null);

            } catch (SQLException e) {
                // Check for duplicate key violation (PK = schedule_id)
                if ("23505".equals(e.getSQLState())) { // Standard SQLState for unique violation
                    logger.error("Duplicate schedule entry for ID {}: {}", scheduleId, e.getMessage());
                    conn.rollback(); // Rollback on duplicate
                    insertNotification(event, false, "Duplicate entry for schedule " + scheduleId);
                    result = Failure.of(new Status("ERR_DUPLICATE_SCHEDULE", "Schedule already exists with ID " + scheduleId, e.getMessage()));
                } else {
                    logger.error("SQLException during schedule creation transaction for {}:", scheduleId, e);
                    conn.rollback();
                    insertNotification(event, false, e.getMessage());
                    result = Failure.of(new Status(SQL_EXCEPTION, e.getMessage()));
                }
            } catch (Exception e) { // Catch other potential runtime exceptions (like ClassCastException)
                logger.error("Unexpected exception during schedule creation transaction for {}:", scheduleId, e);
                conn.rollback();
                insertNotification(event, false, e.getMessage());
                result = Failure.of(new Status(GENERIC_EXCEPTION, e.getMessage()));
            }
        } catch (SQLException e) { // Errors getting connection or setting auto-commit
            logger.error("SQLException setting up connection/transaction for schedule creation:", e);
            result = Failure.of(new Status(SQL_EXCEPTION, e.getMessage()));
        }
        return result;
    }

    @Override
    public Result<String> updateSchedule(Map<String, Object> event) {
        // SQL statement for updating schedule_t
        // Assuming host_id might not be typically updated, focusing on schedule details
        final String sql = "UPDATE schedule_t SET schedule_name = ?, frequency_unit = ?, frequency_time = ?, " +
                "start_ts = ?, event_topic = ?, event_type = ?, event_data = ?, update_user = ?, update_ts = ? " +
                "WHERE schedule_id = ?";
        Result<String> result;
        Map<String, Object> map = (Map<String, Object>)event.get(PortalConstants.DATA);
        String scheduleId = (String) map.get("scheduleId"); // Get scheduleId for WHERE clause and return

        try (Connection conn = ds.getConnection()) {
            conn.setAutoCommit(false);
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
                    throw new SQLException("failed to update the schedule with id " + scheduleId + " - record not found.");
                }

                // Success path
                conn.commit();
                result =  Success.of(scheduleId); // Return scheduleId
                insertNotification(event, true, null);

            } catch (SQLException e) {
                logger.error("SQLException during schedule update transaction for {}:", scheduleId, e);
                conn.rollback();
                insertNotification(event, false, e.getMessage());
                result = Failure.of(new Status(SQL_EXCEPTION, e.getMessage()));
            } catch (Exception e) { // Catch other potential runtime exceptions
                logger.error("Unexpected exception during schedule update transaction for {}:", scheduleId, e);
                conn.rollback();
                insertNotification(event, false, e.getMessage());
                result = Failure.of(new Status(GENERIC_EXCEPTION, e.getMessage()));
            }
        } catch (SQLException e) { // Errors getting connection or setting auto-commit
            logger.error("SQLException setting up connection/transaction for schedule update:", e);
            result = Failure.of(new Status(SQL_EXCEPTION, e.getMessage()));
        }
        return result;
    }

    @Override
    public Result<String> deleteSchedule(Map<String, Object> event) {
        // SQL statement for deleting from schedule_t
        final String sql = "DELETE FROM schedule_t WHERE schedule_id = ?";
        Result<String> result;
        Map<String, Object> map = (Map<String, Object>)event.get(PortalConstants.DATA);
        String scheduleId = (String) map.get("scheduleId"); // Get scheduleId for WHERE clause and return

        // Basic check for required field (Primary Key part)
        if (scheduleId == null) {
            logger.error("Missing required field 'scheduleId' in data payload for deleteSchedule: {}", map);
            return Failure.of(new Status("ERR_MISSING_SCHEDULE_DELETE_ID", "'scheduleId' missing in deleteSchedule data"));
        }

        try (Connection conn = ds.getConnection()) {
            conn.setAutoCommit(false);
            try (PreparedStatement statement = conn.prepareStatement(sql)) {

                // Set parameter for the WHERE clause
                statement.setObject(1, UUID.fromString(scheduleId));

                // Execute delete
                int count = statement.executeUpdate();
                if (count == 0) {
                    // Record not found to delete. Following template by throwing.
                    throw new SQLException("failed to delete the schedule with id " + scheduleId + " - record not found.");
                }

                // Success path
                conn.commit();
                result =  Success.of(scheduleId); // Return scheduleId
                insertNotification(event, true, null);

            } catch (SQLException e) {
                logger.error("SQLException during schedule delete transaction for {}:", scheduleId, e);
                conn.rollback();
                insertNotification(event, false, e.getMessage());
                result = Failure.of(new Status(SQL_EXCEPTION, e.getMessage()));
            } catch (Exception e) { // Catch other potential runtime exceptions
                logger.error("Unexpected exception during schedule delete transaction for {}:", scheduleId, e);
                conn.rollback();
                insertNotification(event, false, e.getMessage());
                result = Failure.of(new Status(GENERIC_EXCEPTION, e.getMessage()));
            }
        } catch (SQLException e) { // Errors getting connection or setting auto-commit
            logger.error("SQLException setting up connection/transaction for schedule delete:", e);
            result = Failure.of(new Status(SQL_EXCEPTION, e.getMessage()));
        }
        return result;
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
                "event_topic, event_type, event_data, update_user, update_ts " +
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
                    result = Success.of(null); // Or Failure with NOT_FOUND status
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
