package net.lightapi.portal.db.persistence;

import com.networknt.config.JsonMapper;
import com.networknt.db.provider.SqlDbStartupHook;
import com.networknt.monad.Failure;
import com.networknt.monad.Result;
import com.networknt.monad.Success;
import com.networknt.status.Status;
import com.networknt.utility.Constants;
import io.cloudevents.core.v1.CloudEventV1;
import net.lightapi.portal.PortalConstants;
import net.lightapi.portal.db.PortalDbProvider; // For shared constants initially
import net.lightapi.portal.db.util.NotificationService;
import net.lightapi.portal.db.util.SqlUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;
import java.time.OffsetDateTime;
import java.util.*;

import static com.networknt.db.provider.SqlDbStartupHook.ds;
import static net.lightapi.portal.db.util.SqlUtil.addCondition;

public class ReferenceDataPersistenceImpl implements ReferenceDataPersistence {
    private static final Logger logger = LoggerFactory.getLogger(ReferenceDataPersistenceImpl.class);
    // Consider moving these to a shared constants class if they are truly general
    private static final String SQL_EXCEPTION = PortalDbProvider.SQL_EXCEPTION;
    private static final String GENERIC_EXCEPTION = PortalDbProvider.GENERIC_EXCEPTION;
    private static final String OBJECT_NOT_FOUND = PortalDbProvider.OBJECT_NOT_FOUND;

    private final NotificationService notificationService;

    public ReferenceDataPersistenceImpl(NotificationService notificationService) {
        this.notificationService = notificationService;
    }

    @Override
    public Result<String> createRefTable(Map<String, Object> event) {
        final String sql = """
                INSERT INTO ref_table_t
                  (table_id, host_id, table_name, table_desc, active, editable, update_user, update_ts)
                VALUES
                  (?, ?, ?, ?, ?, ?, ?, ?)
                """;
        Result<String> result;
        Map<String, Object> map = (Map<String, Object>)event.get(PortalConstants.DATA);
        String tableId = (String) map.get("tableId");

        try (Connection conn = SqlDbStartupHook.ds.getConnection()) {
            conn.setAutoCommit(false);
            try (PreparedStatement statement = conn.prepareStatement(sql)) {
                statement.setObject(1, UUID.fromString(tableId));
                String hostId = (String) map.get("hostId");
                if (hostId != null && !hostId.isEmpty()) {
                    statement.setObject(2, UUID.fromString(hostId));
                } else {
                    statement.setNull(2, Types.OTHER); // Assuming UUID type, OTHER is a safe bet for setNull with UUIDs
                }
                statement.setString(3, (String)map.get("tableName"));
                String tableDesc = (String) map.get("tableDesc");
                if (tableDesc != null && !tableDesc.isEmpty()) {
                    statement.setString(4, tableDesc);
                } else {
                    statement.setNull(4, Types.VARCHAR);
                }
                Boolean active = (Boolean) map.get("active");
                if (active != null) {
                    statement.setBoolean(5, active);
                } else {
                    statement.setNull(5, Types.BOOLEAN);
                }
                Boolean editable = (Boolean)map.get("editable");
                if (editable != null) {
                    statement.setBoolean(6, editable);
                } else {
                    statement.setNull(6, Types.BOOLEAN);
                }
                statement.setString(7, (String)event.get(Constants.USER));
                statement.setObject(8, OffsetDateTime.parse((String)event.get(CloudEventV1.TIME)));

                int count = statement.executeUpdate();
                if (count == 0) {
                    throw new SQLException("failed to insert the reference table with id " + tableId);
                }
                conn.commit();
                result =  Success.of(tableId);
                notificationService.insertNotification(event, true, null);
            } catch (SQLException e) {
                logger.error("SQLException:", e);
                conn.rollback();
                notificationService.insertNotification(event, false, e.getMessage());
                result = Failure.of(new Status(SQL_EXCEPTION, e.getMessage()));
            } catch (Exception e) {
                logger.error("Exception:", e);
                conn.rollback();
                notificationService.insertNotification(event, false, e.getMessage());
                result = Failure.of(new Status(GENERIC_EXCEPTION, e.getMessage()));
            }
        } catch (SQLException e) {
            logger.error("SQLException on getting connection:", e);
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
                    statement.setNull(2, Types.VARCHAR);
                }
                Boolean active = (Boolean)map.get("active");
                if (active != null) {
                    statement.setBoolean(3, active);
                } else {
                    statement.setNull(3, Types.BOOLEAN);
                }
                Boolean editable = (Boolean)map.get("editable");
                if (editable != null) {
                    statement.setBoolean(4, editable);
                } else {
                    statement.setNull(4, Types.BOOLEAN);
                }

                statement.setString(5, (String)event.get(Constants.USER));

                statement.setObject(6, OffsetDateTime.parse((String)event.get(CloudEventV1.TIME)));

                statement.setObject(7, UUID.fromString(tableId));

                // Execute update
                int count = statement.executeUpdate();
                if (count == 0) {
                    throw new SQLException("failed to update the reference table with id " + tableId + " - record not found.");
                }

                // Success path
                conn.commit();
                result =  Success.of(tableId); // Return tableId
                notificationService.insertNotification(event, true, null);

            } catch (SQLException e) {
                logger.error("SQLException:", e);
                conn.rollback();
                notificationService.insertNotification(event, false, e.getMessage());
                result = Failure.of(new Status(SQL_EXCEPTION, e.getMessage()));
            } catch (Exception e) { // Catch other potential runtime exceptions
                logger.error("Exception:", e);
                conn.rollback();
                notificationService.insertNotification(event, false, e.getMessage());
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
                notificationService.insertNotification(event, true, null);

            } catch (SQLException e) {
                logger.error("SQLException:", e);
                conn.rollback();
                notificationService.insertNotification(event, false, e.getMessage());
                result = Failure.of(new Status(SQL_EXCEPTION, e.getMessage()));
            } catch (Exception e) { // Catch other potential runtime exceptions
                logger.error("Exception:", e);
                conn.rollback();
                notificationService.insertNotification(event, false, e.getMessage());
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
        String s =
                """
                        SELECT COUNT(*) OVER () AS total,
                        table_id, host_id, table_name, table_desc, active, editable, update_user, update_ts
                        FROM ref_table_t
                """;
        StringBuilder sqlBuilder = new StringBuilder();
        sqlBuilder.append(s).append("\n");

        List<Object> parameters = new ArrayList<>();
        StringBuilder whereClause = new StringBuilder();

        if (hostId != null && !hostId.isEmpty()) {
            // Manually construct the OR group for host_id
            whereClause.append(" WHERE (host_id = ? OR host_id IS NULL)");
            parameters.add(UUID.fromString(hostId));
        } else {
            // Only add 'host_id IS NULL' if hostId parameter is NOT provided
            // This means we ONLY want global tables in this case.
            // If hostId WAS provided, the '(cond OR NULL)' handles both cases.
            whereClause.append(" WHERE host_id IS NULL");
        }

        // --- Rest of the conditions ---
        addCondition(whereClause, parameters, "table_id", tableId != null ? UUID.fromString(tableId) : null);
        addCondition(whereClause, parameters, "table_name", tableName);
        addCondition(whereClause, parameters, "table_desc", tableDesc);
        addCondition(whereClause, parameters, "active", active);
        addCondition(whereClause, parameters, "editable", editable);


        if (!whereClause.isEmpty()) {
            sqlBuilder.append(whereClause);
        }

        sqlBuilder.append(" ORDER BY table_name\n" +  // Order by table_name as default
                "LIMIT ? OFFSET ?");

        parameters.add(limit);
        parameters.add(offset);

        String sql = sqlBuilder.toString();
        if(logger.isTraceEnabled()) logger.trace("sql = {}", sql);
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
        Map<String, Object> map = null;

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
                "start_ts, end_ts, display_order, active, update_user, update_ts) " +
                "VALUES (?, ?, ?, ?, ?,  ?, ?, ?, ?, ?)";
        Result<String> result;
        Map<String, Object> map = (Map<String, Object>)event.get(PortalConstants.DATA);
        String valueId = (String) map.get("valueId");

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
                if (map.get("startTs") != null && map.get("startTs") instanceof String) {
                    try {
                        statement.setObject(5, OffsetDateTime.parse((String)map.get("startTs")));
                    } catch (java.time.format.DateTimeParseException e) {
                        logger.warn("Invalid format for startTs '{}', setting NULL.", map.get("startTs"), e);
                        statement.setNull(5, Types.TIMESTAMP_WITH_TIMEZONE);
                    }
                } else {
                    statement.setNull(5, Types.TIMESTAMP_WITH_TIMEZONE);
                }

                // 6. end_time (Optional OffsetDateTime)
                if (map.get("endTs") != null && map.get("endTs") instanceof String) {
                    try {
                        statement.setObject(6, OffsetDateTime.parse((String)map.get("endTs")));
                    } catch (java.time.format.DateTimeParseException e) {
                        logger.warn("Invalid format for endTs '{}', setting NULL.", map.get("endTs"), e);
                        statement.setNull(6, Types.TIMESTAMP_WITH_TIMEZONE);
                    }
                } else {
                    statement.setNull(6, Types.TIMESTAMP_WITH_TIMEZONE);
                }

                // 7. display_order (Optional Integer)
                if (map.get("displayOrder") instanceof Number) {
                    statement.setInt(7, ((Number) map.get("displayOrder")).intValue());
                } else {
                    statement.setNull(7, Types.INTEGER);
                }

                Boolean active = (Boolean)map.get("active");
                if (active != null) {
                    statement.setBoolean(8, active);
                } else {
                    statement.setNull(8, Types.BOOLEAN);
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
                notificationService.insertNotification(event, true, null);

            } catch (SQLException e) {
                logger.error("SQLException:", e);
                conn.rollback();
                notificationService.insertNotification(event, false, e.getMessage());
                result = Failure.of(new Status(SQL_EXCEPTION, e.getMessage()));
            } catch (Exception e) { // Catch other potential runtime exceptions like parsing errors if not caught earlier
                logger.error("Exception:", e);
                conn.rollback();
                notificationService.insertNotification(event, false, e.getMessage());
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
        final String sql = "UPDATE ref_value_t SET table_id = ?, value_code = ?, value_desc = ?, start_ts = ?, " +
                "end_ts = ?, display_order = ?, active = ?, update_user = ?, update_ts = ? " +
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
                    statement.setNull(3, Types.VARCHAR);
                }

                // 4. start_time (Optional OffsetDateTime)
                if (map.get("startTs") != null && map.get("startTs") instanceof String) {
                    try {
                        statement.setObject(4, OffsetDateTime.parse((String)map.get("startTs")));
                    } catch (java.time.format.DateTimeParseException e) {
                        logger.warn("Invalid format for startTs '{}', setting NULL.", map.get("startTs"), e);
                        statement.setNull(4, Types.TIMESTAMP_WITH_TIMEZONE);
                    }
                } else {
                    statement.setNull(4, Types.TIMESTAMP_WITH_TIMEZONE);
                }

                // 5. end_time (Optional OffsetDateTime)
                if (map.get("endTs") != null && map.get("endTs") instanceof String) {
                    try {
                        statement.setObject(5, OffsetDateTime.parse((String)map.get("endTs")));
                    } catch (java.time.format.DateTimeParseException e) {
                        logger.warn("Invalid format for endTs '{}', setting NULL.", map.get("endTs"), e);
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

                Boolean active = (Boolean)map.get("active");
                if (active != null) {
                    statement.setBoolean(7, active);
                } else {
                    // Decide update behavior: set default? Or assume not changing if missing?
                    // Setting default like template here:
                    statement.setNull(7, Types.BOOLEAN);
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
                notificationService.insertNotification(event, true, null);

            } catch (SQLException e) {
                logger.error("SQLException:", e);
                conn.rollback();
                notificationService.insertNotification(event, false, e.getMessage());
                result = Failure.of(new Status(SQL_EXCEPTION, e.getMessage()));
            } catch (Exception e) { // Catch other potential runtime exceptions
                logger.error("Exception:", e);
                conn.rollback();
                notificationService.insertNotification(event, false, e.getMessage());
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
                notificationService.insertNotification(event, true, null);

            } catch (SQLException e) {
                logger.error("SQLException:", e);
                conn.rollback();
                notificationService.insertNotification(event, false, e.getMessage());
                result = Failure.of(new Status(SQL_EXCEPTION, e.getMessage()));
            } catch (Exception e) { // Catch other potential runtime exceptions
                logger.error("Exception:", e);
                conn.rollback();
                notificationService.insertNotification(event, false, e.getMessage());
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
        String s =
                """
                    SELECT COUNT(*) OVER () AS total,
                    v.value_id, v.table_id, t.table_name, v.value_code, v.value_desc, v.start_ts, v.end_ts,
                    v.display_order, v.active, v.update_user, v.update_ts
                    FROM ref_value_t v
                    INNER JOIN ref_table_t t ON t.table_id = v.table_id\s
                    WHERE 1=1
                """;
        StringBuilder sqlBuilder = new StringBuilder();
        sqlBuilder.append(s).append("\n");

        List<Object> parameters = new ArrayList<>();
        StringBuilder whereClause = new StringBuilder();

        // Add conditions based on input parameters
        addCondition(whereClause, parameters, "v.value_id", valueId != null ? UUID.fromString(valueId) : null);
        addCondition(whereClause, parameters, "v.table_id", tableId != null ? UUID.fromString(tableId) : null);
        addCondition(whereClause, parameters, "v.value_code", valueCode);
        addCondition(whereClause, parameters, "v.value_desc", valueDesc);
        addCondition(whereClause, parameters, "v.display_order", displayOrder);
        addCondition(whereClause, parameters, "v.active", active);

        if (!whereClause.isEmpty()) {
            sqlBuilder.append("AND ").append(whereClause);
        }

        // Add ordering and pagination
        sqlBuilder.append(" ORDER BY v.display_order, v.value_code\n" +
                "LIMIT ? OFFSET ?");

        parameters.add(limit);
        parameters.add(offset);

        String sql = sqlBuilder.toString();
        if(logger.isTraceEnabled()) logger.trace("sql = {}", sql);
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
                    map.put("tableName", resultSet.getString("table_name"));
                    map.put("valueCode", resultSet.getString("value_code"));
                    map.put("valueDesc", resultSet.getString("value_desc"));
                    map.put("startTs", resultSet.getObject("start_ts") != null ? resultSet.getObject("start_ts", OffsetDateTime.class) : null);
                    map.put("endTs", resultSet.getObject("end_ts") != null ? resultSet.getObject("end_ts", OffsetDateTime.class) : null);
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
            resultMap.put("refValues", refValues);
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
        final String sql = "SELECT value_id, table_id, value_code, value_desc, start_ts, end_ts, " +
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
                        refValueMap.put("startTs", resultSet.getObject("start_ts") != null ? resultSet.getObject("start_ts", OffsetDateTime.class) : null);
                        refValueMap.put("endTs", resultSet.getObject("end_ts") != null ? resultSet.getObject("end_ts", OffsetDateTime.class) : null);
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
        final String sql =
                """
                   INSERT INTO value_locale_t(value_id, language, value_label)
                   VALUES (?, ?, ?)
                """;

        Result<String> result;
        Map<String, Object> map = (Map<String, Object>)event.get(PortalConstants.DATA);
        String valueId = (String) map.get("valueId");
        String language = (String) map.get("language");

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
                    statement.setNull(3, Types.VARCHAR);
                }

                // Execute insert
                int count = statement.executeUpdate();
                if (count == 0) {
                    throw new SQLException("failed to insert the value locale with id " + createdId);
                }

                // Success path
                conn.commit();
                result =  Success.of(createdId); // Return composite identifier
                notificationService.insertNotification(event, true, null);

            } catch (SQLException e) {
                logger.error("SQLException during value locale creation transaction for {}:", createdId, e);
                conn.rollback();
                notificationService.insertNotification(event, false, e.getMessage());
                result = Failure.of(new Status(SQL_EXCEPTION, e.getMessage()));
            } catch (Exception e) { // Catch other potential runtime exceptions
                logger.error("Unexpected exception during value locale creation transaction for {}:", createdId, e);
                conn.rollback();
                notificationService.insertNotification(event, false, e.getMessage());
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
        final String sql =
                """
                UPDATE value_locale_t SET value_label = ?
                WHERE value_id = ? AND language = ?
                """;
        Result<String> result;
        Map<String, Object> map = (Map<String, Object>)event.get(PortalConstants.DATA);
        String valueId = (String) map.get("valueId");
        String language = (String) map.get("language");

        String updatedId = valueId + ":" + language;

        try (Connection conn = ds.getConnection()) {
            conn.setAutoCommit(false);
            try (PreparedStatement statement = conn.prepareStatement(sql)) {

                String valueLabel = (String) map.get("valueLabel");
                if (valueLabel != null && !valueLabel.isEmpty()) {
                    statement.setString(1, valueLabel);
                } else {
                    statement.setNull(1, Types.VARCHAR);
                }

                statement.setObject(2, UUID.fromString(valueId));
                statement.setString(3, language);


                int count = statement.executeUpdate();
                if (count == 0) {
                    // Record not found to update
                    throw new SQLException("failed to update the value locale with id " + updatedId + " - record not found.");
                }

                // Success path
                conn.commit();
                result =  Success.of(updatedId); // Return composite identifier
                notificationService.insertNotification(event, true, null);

            } catch (SQLException e) {
                logger.error("SQLException during value locale update transaction for {}:", updatedId, e);
                conn.rollback();
                notificationService.insertNotification(event, false, e.getMessage());
                result = Failure.of(new Status(SQL_EXCEPTION, e.getMessage()));
            } catch (Exception e) { // Catch other potential runtime exceptions
                logger.error("Unexpected exception during value locale update transaction for {}:", updatedId, e);
                conn.rollback();
                notificationService.insertNotification(event, false, e.getMessage());
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
        String valueId = (String) map.get("valueId");
        String language = (String) map.get("language");

        String deletedId = valueId + ":" + language;

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
                notificationService.insertNotification(event, true, null);

            } catch (SQLException e) {
                logger.error("SQLException during value locale delete transaction for {}:", deletedId, e);
                conn.rollback();
                notificationService.insertNotification(event, false, e.getMessage());
                result = Failure.of(new Status(SQL_EXCEPTION, e.getMessage()));
            } catch (Exception e) { // Catch other potential runtime exceptions
                logger.error("Unexpected exception during value locale delete transaction for {}:", deletedId, e);
                conn.rollback();
                notificationService.insertNotification(event, false, e.getMessage());
                result = Failure.of(new Status(GENERIC_EXCEPTION, e.getMessage()));
            }
        } catch (SQLException e) { // Errors getting connection or setting auto-commit
            logger.error("SQLException setting up connection/transaction for value locale delete:", e);
            result = Failure.of(new Status(SQL_EXCEPTION, e.getMessage()));
        }
        return result;
    }

    @Override
    public Result<String> getRefLocale(int offset, int limit, String valueId, String valueCode, String valueDesc, String language, String valueLabel) {
        Result<String> result = null;
        String s =
                """
                    SELECT COUNT(*) OVER () AS total,
                    l.value_id, v.value_code, v.value_desc, l.language, l.value_label
                    FROM value_locale_t l
                    INNER JOIN ref_value_t v ON v.value_id = l.value_id
                    WHERE 1=1
                """;
        StringBuilder sqlBuilder = new StringBuilder();
        sqlBuilder.append(s).append("\n");

        List<Object> parameters = new ArrayList<>();
        StringBuilder whereClause = new StringBuilder();

        // Add conditions based on input parameters using the helper
        addCondition(whereClause, parameters, "l.value_id", valueId != null ? UUID.fromString(valueId) : null);
        addCondition(whereClause, parameters, "v.value_code", valueCode);
        addCondition(whereClause, parameters, "v.value_desc", valueDesc);
        addCondition(whereClause, parameters, "l.language", language);
        addCondition(whereClause, parameters, "l.value_label", valueLabel);

        // Append the dynamic WHERE conditions if any were added
        if (!whereClause.isEmpty()) {
            sqlBuilder.append("AND ").append(whereClause);
        }

        // Add ordering and pagination
        sqlBuilder.append(" ORDER BY l.value_id, l.language\n" +
                "LIMIT ? OFFSET ?");

        parameters.add(limit);
        parameters.add(offset);

        String sql = sqlBuilder.toString();
        int total = 0;
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
                        total = resultSet.getInt("total");
                        isFirstRow = false;
                    }
                    // Populate map with data for the current row
                    map.put("valueId", resultSet.getObject("value_id", UUID.class));
                    map.put("valueCode", resultSet.getString("value_code"));
                    map.put("valueDesc", resultSet.getString("value_desc"));
                    map.put("language", resultSet.getString("language"));
                    map.put("valueLabel", resultSet.getString("value_label"));

                    locales.add(map);
                }
            }

            // Prepare the final result map containing total count and the list of locales
            Map<String, Object> resultMap = new HashMap<>();
            resultMap.put("total", total);
            resultMap.put("locales", locales);
            result = Success.of(JsonMapper.toJson(resultMap));
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

        try (Connection conn = ds.getConnection()) {
            conn.setAutoCommit(false);
            try (PreparedStatement statement = conn.prepareStatement(sql)) {

                statement.setObject(1, UUID.fromString(relationId));
                statement.setString(2, (String)map.get("relationName"));
                statement.setString(3, (String)map.get("relationDesc"));
                statement.setString(4, (String)event.get(Constants.USER));
                statement.setObject(5, OffsetDateTime.parse((String)event.get(CloudEventV1.TIME)));

                // Execute insert
                int count = statement.executeUpdate();
                if (count == 0) {
                    throw new SQLException("failed to insert the relation type with id " + relationId);
                }

                // Success path
                conn.commit();
                result =  Success.of(relationId); // Return relationId
                notificationService.insertNotification(event, true, null);

            } catch (SQLException e) {
                logger.error("SQLException during relation type creation transaction for {}:", relationId, e);
                conn.rollback();
                notificationService.insertNotification(event, false, e.getMessage());
                result = Failure.of(new Status(SQL_EXCEPTION, e.getMessage()));
            } catch (Exception e) { // Catch other potential runtime exceptions
                logger.error("Unexpected exception during relation type creation transaction for {}:", relationId, e);
                conn.rollback();
                notificationService.insertNotification(event, false, e.getMessage());
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
        String relationId = (String) map.get("relationId");

        try (Connection conn = ds.getConnection()) {
            conn.setAutoCommit(false);
            try (PreparedStatement statement = conn.prepareStatement(sql)) {

                statement.setString(1, (String)map.get("relationName"));
                statement.setString(2, (String)map.get("relationDesc"));
                statement.setString(3, (String)event.get(Constants.USER));
                statement.setObject(4, OffsetDateTime.parse((String)event.get(CloudEventV1.TIME)));
                statement.setObject(5, UUID.fromString(relationId));

                int count = statement.executeUpdate();
                if (count == 0) {
                    // Record not found to update
                    throw new SQLException("failed to update the relation type with id " + relationId + " - record not found.");
                }

                conn.commit();
                result =  Success.of(relationId); // Return relationId
                notificationService.insertNotification(event, true, null);

            } catch (SQLException e) {
                logger.error("SQLException during relation type update transaction for {}:", relationId, e);
                conn.rollback();
                notificationService.insertNotification(event, false, e.getMessage());
                result = Failure.of(new Status(SQL_EXCEPTION, e.getMessage()));
            } catch (Exception e) { // Catch other potential runtime exceptions
                logger.error("Unexpected exception during relation type update transaction for {}:", relationId, e);
                conn.rollback();
                notificationService.insertNotification(event, false, e.getMessage());
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
                notificationService.insertNotification(event, true, null);

            } catch (SQLException e) {
                logger.error("SQLException during relation type delete transaction for {}:", relationId, e);
                conn.rollback();
                notificationService.insertNotification(event, false, e.getMessage());
                result = Failure.of(new Status(SQL_EXCEPTION, e.getMessage()));
            } catch (Exception e) { // Catch other potential runtime exceptions
                logger.error("Unexpected exception during relation type delete transaction for {}:", relationId, e);
                conn.rollback();
                notificationService.insertNotification(event, false, e.getMessage());
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
        String s =
                """
                        SELECT COUNT(*) OVER () AS total,
                        relation_id, relation_name, relation_desc, update_user, update_ts
                        FROM relation_type_t
                        WHERE 1=1
                """;
        StringBuilder sqlBuilder = new StringBuilder();
        sqlBuilder.append(s).append("\n");

        List<Object> parameters = new ArrayList<>();
        StringBuilder whereClause = new StringBuilder();

        // Add conditions based on input parameters using the helper
        addCondition(whereClause, parameters, "relation_id", relationId != null ? UUID.fromString(relationId) : null);
        addCondition(whereClause, parameters, "relation_name", relationName);
        addCondition(whereClause, parameters, "relation_desc", relationDesc);

        // Append the dynamic WHERE conditions if any were added
        if (!whereClause.isEmpty()) {
            sqlBuilder.append("AND ").append(whereClause);
        }

        // Add ordering and pagination
        sqlBuilder.append(" ORDER BY relation_name\n" +
                "LIMIT ? OFFSET ?");

        parameters.add(limit);
        parameters.add(offset);

        String sql = sqlBuilder.toString();
        int total = 0;
        List<Map<String, Object>> relationTypes = new ArrayList<>();

        try (Connection connection = ds.getConnection();
             PreparedStatement preparedStatement = connection.prepareStatement(sql)) {

            // Bind all collected parameters
            for (int i = 0; i < parameters.size(); i++) {
                preparedStatement.setObject(i + 1, parameters.get(i));
            }

            boolean isFirstRow = true;
            try (ResultSet resultSet = preparedStatement.executeQuery()) {
                // Process the results
                while (resultSet.next()) {
                    Map<String, Object> map = new HashMap<>();
                    if (isFirstRow) {
                        total = resultSet.getInt("total");
                        isFirstRow = false;
                    }
                    // Populate map with data for the current row
                    map.put("relationId", resultSet.getObject("relation_id", UUID.class));
                    map.put("relationName", resultSet.getString("relation_name"));
                    map.put("relationDesc", resultSet.getString("relation_desc"));
                    map.put("updateUser", resultSet.getString("update_user"));
                    map.put("updateTs", resultSet.getObject("update_ts") != null ? resultSet.getObject("update_ts", OffsetDateTime.class) : null);

                    relationTypes.add(map);
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

                Boolean active = (Boolean) map.get("active");
                if (active != null) {
                    statement.setBoolean(4, active);
                } else {
                    statement.setBoolean(4, true);
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
                notificationService.insertNotification(event, true, null);

            } catch (SQLException e) {
                logger.error("SQLException during relation creation transaction for {}:", createdId, e);
                conn.rollback();
                notificationService.insertNotification(event, false, e.getMessage());
                result = Failure.of(new Status(SQL_EXCEPTION, e.getMessage()));
            } catch (Exception e) { // Catch other potential runtime exceptions
                logger.error("Unexpected exception during relation creation transaction for {}:", createdId, e);
                conn.rollback();
                notificationService.insertNotification(event, false, e.getMessage());
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

        // Construct a unique identifier string for the success result/logging
        String updatedId = relationId + ":" + valueIdFrom + ":" + valueIdTo;

        try (Connection conn = ds.getConnection()) {
            conn.setAutoCommit(false);
            try (PreparedStatement statement = conn.prepareStatement(sql)) {

                Boolean active = (Boolean)map.get("active");
                if (active != null) {
                    statement.setBoolean(1, active);
                } else {
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
                notificationService.insertNotification(event, true, null);

            } catch (SQLException e) {
                logger.error("SQLException during relation update transaction for {}:", updatedId, e);
                conn.rollback();
                notificationService.insertNotification(event, false, e.getMessage());
                result = Failure.of(new Status(SQL_EXCEPTION, e.getMessage()));
            } catch (Exception e) { // Catch other potential runtime exceptions
                logger.error("Unexpected exception during relation update transaction for {}:", updatedId, e);
                conn.rollback();
                notificationService.insertNotification(event, false, e.getMessage());
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
                notificationService.insertNotification(event, true, null);

            } catch (SQLException e) {
                logger.error("SQLException during relation delete transaction for {}:", deletedId, e);
                conn.rollback();
                notificationService.insertNotification(event, false, e.getMessage());
                result = Failure.of(new Status(SQL_EXCEPTION, e.getMessage()));
            } catch (Exception e) { // Catch other potential runtime exceptions
                logger.error("Unexpected exception during relation delete transaction for {}:", deletedId, e);
                conn.rollback();
                notificationService.insertNotification(event, false, e.getMessage());
                result = Failure.of(new Status(GENERIC_EXCEPTION, e.getMessage()));
            }
        } catch (SQLException e) { // Errors getting connection or setting auto-commit
            logger.error("SQLException setting up connection/transaction for relation delete:", e);
            result = Failure.of(new Status(SQL_EXCEPTION, e.getMessage()));
        }
        return result;
    }

    @Override
    public Result<String> getRefRelation(int offset, int limit, String relationId, String relationName, String valueIdFrom,
                                         String valueCodeFrom, String valueIdTo, String valueCodeTo, Boolean active) {
        Result<String> result = null;
        String s =
                """
                        SELECT COUNT(*) OVER () AS total,
                        r.relation_id, t.relation_name, r.value_id_from, v1.value_code value_code_from, r.value_id_to,\s
                        v2.value_code value_code_to, r.active, r.update_user, r.update_ts
                        FROM relation_t r
                        INNER JOIN relation_type_t t ON r.relation_id = t.relation_id
                        INNER JOIN ref_value_t v1 ON v1.value_id = r.value_id_from
                        INNER JOIN ref_value_t v2 ON v2.value_id = r.value_id_to\s
                        WHERE 1=1
                """;
        StringBuilder sqlBuilder = new StringBuilder();
        // Select all columns from relation_t and include total count
        sqlBuilder.append(s).append("\n");

        List<Object> parameters = new ArrayList<>();
        StringBuilder whereClause = new StringBuilder();

        // Add conditions based on input parameters using the helper
        addCondition(whereClause, parameters, "r.relation_id", relationId != null ? UUID.fromString(relationId) : null);
        addCondition(whereClause, parameters, "t.relation_name", relationName);
        addCondition(whereClause, parameters, "r.value_id_from", valueIdFrom != null ? UUID.fromString(valueIdFrom) : null);
        addCondition(whereClause, parameters, "v1.value_code_from", valueCodeFrom);
        addCondition(whereClause, parameters, "r.value_id_to", valueIdTo != null ? UUID.fromString(valueIdTo) : null);
        addCondition(whereClause, parameters, "v2.value_code_to", valueCodeTo);
        addCondition(whereClause, parameters, "r.active", active);


        // Append the dynamic WHERE conditions if any were added
        if (!whereClause.isEmpty()) {
            sqlBuilder.append("AND ").append(whereClause);
        }

        // Add ordering and pagination
        sqlBuilder.append(" ORDER BY r.relation_id, r.value_id_from, r.value_id_to\n" +
                "LIMIT ? OFFSET ?");

        parameters.add(limit);
        parameters.add(offset);

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
                    map.put("relationName", resultSet.getString("relation_name"));
                    map.put("valueIdFrom", resultSet.getObject("value_id_from", UUID.class));
                    map.put("valueCodeFrom", resultSet.getString("value_code_from"));
                    map.put("valueIdTo", resultSet.getObject("value_id_to", UUID.class));
                    map.put("valueCodeTo", resultSet.getString("value_code_to"));
                    map.put("active", resultSet.getBoolean("active"));
                    map.put("updateUser", resultSet.getString("update_user"));
                    map.put("updateTs", resultSet.getObject("update_ts") != null ? resultSet.getObject("update_ts", OffsetDateTime.class) : null);
                    relations.add(map);
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

}
