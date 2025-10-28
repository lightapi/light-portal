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

import java.sql.*;
import java.time.OffsetDateTime;
import java.util.*;

import static com.networknt.db.provider.SqlDbStartupHook.ds;
import static net.lightapi.portal.db.util.SqlUtil.*;

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
    public void createRefTable(Connection conn, Map<String, Object> event) throws SQLException, Exception {
        final String sql =
                """
                INSERT INTO ref_table_t (table_id, host_id, table_name, table_desc, active, editable, update_user, update_ts, aggregate_version)
                VALUES (?, ?, ?, ?, ?,  ?, ?, ?, ?)
                """;
        Map<String, Object> map = (Map<String, Object>)event.get(PortalConstants.DATA);
        String tableId = (String) map.get("tableId");
        long newAggregateVersion = SqlUtil.getNewAggregateVersion(event);

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
            statement.setLong(9, newAggregateVersion);

            int count = statement.executeUpdate();
            if (count == 0) {
                throw new SQLException(String.format("Failed during createRefTable for tableId %s with aggregateVersion %d", tableId, newAggregateVersion));
            }
        } catch (SQLException e) {
            logger.error("SQLException during createRefTable for tableId {} aggregateVersion {}: {}", tableId, newAggregateVersion, e.getMessage(), e);
            throw e;
        } catch (Exception e) {
            logger.error("Exception during createRefTable for tableId {} aggregateVersion {}: {}", tableId, newAggregateVersion, e.getMessage(), e);
            throw e;
        }
    }

    private boolean queryRefTableExists(Connection conn, String tableId) throws SQLException {
        final String sql =
                """
                SELECT COUNT(*) FROM ref_table_t WHERE table_id = ?
                """;
        try (PreparedStatement pst = conn.prepareStatement(sql)) {
            pst.setObject(1, UUID.fromString(tableId));
            try (ResultSet rs = pst.executeQuery()) {
                return rs.next() && rs.getInt(1) > 0;
            }
        }
    }

    @Override
    public void updateRefTable(Connection conn, Map<String, Object> event) throws SQLException, Exception {
        final String sql =
                """
                UPDATE ref_table_t SET table_name = ?, table_desc = ?, active = ?, editable = ?,
                update_user = ?, update_ts = ?, aggregate_version = ? WHERE table_id = ? AND aggregate_version = ?
                """;
        Map<String, Object> map = (Map<String, Object>)event.get(PortalConstants.DATA);
        String tableId = (String) map.get("tableId");
        long oldAggregateVersion = SqlUtil.getOldAggregateVersion(event);
        long newAggregateVersion = SqlUtil.getNewAggregateVersion(event);

        try (PreparedStatement statement = conn.prepareStatement(sql)) {

            // 1. table_name (Required in update payload)
            String tableName = (String)map.get("tableName");
            statement.setString(1, tableName);

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
            statement.setLong(7, newAggregateVersion);

            statement.setObject(8, UUID.fromString(tableId));
            statement.setLong(9, oldAggregateVersion);

            // Execute update
            int count = statement.executeUpdate();
            if (count == 0) {
                if (queryRefTableExists(conn, tableId)) {
                    throw new ConcurrencyException("Optimistic concurrency conflict during updateRefTable for tableId " + tableId + ". Expected version " + oldAggregateVersion + " but found a different version " + newAggregateVersion + ".");
                } else {
                    throw new SQLException("No record found during updateRefTable for tableId " + tableId + ".");
                }
            }
        } catch (SQLException e) {
            logger.error("SQLException during updateRefTable for tableId {} (old: {}) -> (new: {}): {}", tableId, oldAggregateVersion, newAggregateVersion, e.getMessage(), e);
            throw e;
        } catch (Exception e) { // Catch other potential runtime exceptions
            logger.error("Exception during updateRefTable for tableId {} (old: {}) -> (new: {}): {}", tableId, oldAggregateVersion, newAggregateVersion, e.getMessage(), e);
            throw e;
        }
    }

    @Override
    public void deleteRefTable(Connection conn, Map<String, Object> event) throws SQLException, Exception {
        final String sql = "DELETE FROM ref_table_t WHERE table_id = ? AND aggregate_version = ?";
        Map<String, Object> map = (Map<String, Object>)event.get(PortalConstants.DATA);
        String tableId = (String) map.get("tableId");
        long oldAggregateVersion = SqlUtil.getOldAggregateVersion(event);

        try (PreparedStatement statement = conn.prepareStatement(sql)) {
            statement.setObject(1, UUID.fromString(tableId));
            statement.setLong(2, oldAggregateVersion);

            int count = statement.executeUpdate();
            if (count == 0) {
                if (queryRefTableExists(conn, tableId)) {
                    throw new ConcurrencyException("Optimistic concurrency conflict during deleteRefTable for tableId " + tableId + " aggregateVersion " + oldAggregateVersion + " but found a different version or already updated.");
                } else {
                    throw new SQLException("No record found during deleteRefTable for tableId " + tableId + ". It might have been already deleted.");
                }
            }
        } catch (SQLException e) {
            logger.error("SQLException during deleteRefTable for tableId {} aggregateVersion {}: {}", tableId, oldAggregateVersion, e.getMessage(), e);
            throw e;
        } catch (Exception e) {
            logger.error("Exception during deleteRefTable for tableId {} aggregateVersion {}: {}", tableId, oldAggregateVersion, e.getMessage(), e);
            throw e;
        }
    }

    @Override
    public Result<String> getRefTable(int offset, int limit, String filtersJson, String globalFilter, String sortingJson, String hostId) {
        Result<String> result = null;

        // Assuming filtersJson and sortingJson contain List of Maps like:
        // filters: [{"id":"tableName","value":"env"}]
        // sorting: [{"id":"tableId","desc":false}]
        List<Map<String, Object>> filters = parseJsonList(filtersJson);
        List<Map<String, Object>> sorting = parseJsonList(sortingJson);

        String s =
                """
                SELECT COUNT(*) OVER () AS total,
                table_id, host_id, table_name, table_desc, active, editable, update_user, update_ts, aggregate_version
                FROM ref_table_t
                """;
        StringBuilder sqlBuilder = new StringBuilder(s);

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

        // Material React Table Filters (Dynamic Filters)
        for (Map<String, Object> filter : filters) {
            String filterId = (String) filter.get("id"); // Column name
            String dbColumnName = camelToSnake(filterId);
            Object filterValue = filter.get("value");    // Value to filter by
            if (filterId != null && filterValue != null && !filterValue.toString().isEmpty()) {
                // Using LIKE for flexible filtering, assuming string/text columns
                whereClause.append(" AND ").append(dbColumnName).append(" ILIKE ?"); // ILIKE is case-insensitive LIKE in Postgres
                parameters.add("%" + filterValue + "%");
            }
        }

        // Global Filter (Search across multiple columns)
        if (globalFilter != null && !globalFilter.isEmpty()) {
            whereClause.append(" AND (");
            // Define columns to search for global filter (e.g., table_name, table_desc)
            String[] globalSearchColumns = {"table_name", "table_desc"};
            List<String> globalConditions = new ArrayList<>();
            for (String col : globalSearchColumns) {
                globalConditions.add(col + " ILIKE ?");
                parameters.add("%" + globalFilter + "%");
            }
            whereClause.append(String.join(" OR ", globalConditions));
            whereClause.append(")");
        }

        // Append the constructed WHERE clause
        sqlBuilder.append(whereClause);


        // Dynamic Sorting
        StringBuilder orderByClause = new StringBuilder();
        if (sorting.isEmpty()) {
            // Default sort if none provided
            orderByClause.append(" ORDER BY table_name");
        } else {
            orderByClause.append(" ORDER BY ");
            List<String> sortExpressions = new ArrayList<>();
            for (Map<String, Object> sort : sorting) {
                String sortId = (String) sort.get("id");
                String dbColumnName = camelToSnake(sortId);
                Boolean isDesc = (Boolean) sort.get("desc"); // 'desc' is typically a boolean or "true"/"false" string
                if (sortId != null && !sortId.isEmpty()) {
                    String direction = (isDesc != null && isDesc) ? "DESC" : "ASC";
                    // Quote column name to handle SQL keywords or mixed case
                    sortExpressions.add(dbColumnName + " " + direction);
                }
            }
            // Use default if dynamic sort failed to produce anything
            orderByClause.append(sortExpressions.isEmpty() ? "table_name" : String.join(", ", sortExpressions));
        }
        sqlBuilder.append(orderByClause);

        // Pagination
        sqlBuilder.append("\nLIMIT ? OFFSET ?");

        parameters.add(limit);
        parameters.add(offset);

        String sql = sqlBuilder.toString();
        if(logger.isTraceEnabled()) logger.trace("sql = {}", sql);

        int total = 0;
        List<Map<String, Object>> refTables = new ArrayList<>();

        try (Connection connection = ds.getConnection();
             PreparedStatement preparedStatement = connection.prepareStatement(sql)) {

            for (int i = 0; i < parameters.size(); i++) {
                // Ensure proper type setting (especially for UUIDs, Booleans, etc.)
                if (parameters.get(i) instanceof UUID) {
                    preparedStatement.setObject(i + 1, parameters.get(i));
                } else if (parameters.get(i) instanceof Boolean) {
                    preparedStatement.setBoolean(i + 1, (Boolean) parameters.get(i));
                } else {
                    preparedStatement.setObject(i + 1, parameters.get(i));
                }
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
                    map.put("aggregateVersion", resultSet.getLong("aggregate_version"));

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
                "update_user, update_ts, aggregate_version FROM ref_table_t WHERE table_id = ?";
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
                        map.put("aggregateVersion", resultSet.getLong("aggregate_version"));
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
    public void createRefValue(Connection conn, Map<String, Object> event) throws SQLException, Exception {
        final String sql =
                """
                INSERT INTO ref_value_t(value_id, table_id, value_code, value_desc,
                start_ts, end_ts, display_order, active, update_user, update_ts, aggregate_version)
                VALUES (?, ?, ?, ?, ?,  ?, ?, ?, ?, ?,  ?)
                """;
        Map<String, Object> map = (Map<String, Object>)event.get(PortalConstants.DATA);
        String valueId = (String) map.get("valueId");
        long newAggregateVersion = SqlUtil.getNewAggregateVersion(event);

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
            // 11. aggregate_version (New version for optimistic concurrency)
            statement.setLong(11, newAggregateVersion);

            // Execute insert
            int count = statement.executeUpdate();
            if (count == 0) {
                throw new SQLException(String.format("Failed during createRefValue valueId  %s with aggregateVersion %d", valueId, newAggregateVersion));
            }
        } catch (SQLException e) {
            logger.error("SQLException during createRefValue for valueId {} aggregateVersion {}: {}", valueId, newAggregateVersion, e.getMessage(), e);
            throw e;
        } catch (Exception e) {
            logger.error("Exception during createRefValue for valueId {} aggregateVersion {}: {}", valueId, newAggregateVersion, e.getMessage(), e);
            throw e;
        }
    }

    private boolean queryRefValueExists(Connection conn, String valueId) throws SQLException {
        final String sql =
                """
                SELECT COUNT(*) FROM ref_value_t WHERE value_id = ?
                """;
        try (PreparedStatement pst = conn.prepareStatement(sql)) {
            pst.setObject(1, UUID.fromString(valueId));
            try (ResultSet rs = pst.executeQuery()) {
                return rs.next() && rs.getInt(1) > 0;
            }
        }
    }

    @Override
    public void updateRefValue(Connection conn, Map<String, Object> event) throws SQLException, Exception {
        final String sql = "UPDATE ref_value_t SET table_id = ?, value_code = ?, value_desc = ?, start_ts = ?, " +
                "end_ts = ?, display_order = ?, active = ?, update_user = ?, update_ts = ?, aggregate_version = ? " +
                "WHERE value_id = ? AND aggregate_version = ?";
        Map<String, Object> map = (Map<String, Object>)event.get(PortalConstants.DATA);
        String valueId = (String) map.get("valueId");
        long oldAggregateVersion = SqlUtil.getOldAggregateVersion(event);
        long newAggregateVersion = SqlUtil.getNewAggregateVersion(event);

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
            // 10. aggregate_version (New version for optimistic concurrency)
            statement.setLong(10, newAggregateVersion);

            // 11. value_id (For WHERE clause - Required)
            statement.setObject(11, UUID.fromString(valueId));
            // 12. aggregate_version (For WHERE clause - Required)
            statement.setLong(12, oldAggregateVersion);

            // Execute update
            int count = statement.executeUpdate();
            if (count == 0) {
                if (queryRefValueExists(conn, valueId)) {
                    throw new ConcurrencyException("Optimistic concurrency conflict during updateRefValue for valueId " + valueId + ". Expected version " + oldAggregateVersion + " but found a different version " + newAggregateVersion + ".");
                } else {
                    throw new SQLException("No record found during updateRefValue for valueId " + valueId + ".");
                }
            }
        } catch (SQLException e) {
            logger.error("SQLException during updateRefValue for valueId {} (old: {}) -> (new: {}): {}", valueId, oldAggregateVersion, newAggregateVersion, e.getMessage(), e);
            throw e;
        } catch (Exception e) { // Catch other potential runtime exceptions
            logger.error("Exception during updateRefValue for valueId {} (old: {}) -> (new: {}): {}", valueId, oldAggregateVersion, newAggregateVersion, e.getMessage(), e);
            throw e;
        }
    }

    @Override
    public void deleteRefValue(Connection conn, Map<String, Object> event) throws SQLException, Exception {
        final String sql = "DELETE FROM ref_value_t WHERE value_id = ? AND aggregate_version = ?";
        Result<String> result;
        Map<String, Object> map = (Map<String, Object>)event.get(PortalConstants.DATA);
        String valueId = (String) map.get("valueId"); // Get valueId for WHERE clause and return
        long oldAggregateVersion = SqlUtil.getOldAggregateVersion(event);

        try (PreparedStatement statement = conn.prepareStatement(sql)) {
            statement.setObject(1, UUID.fromString(valueId));
            statement.setLong(2, oldAggregateVersion);

            int count = statement.executeUpdate();
            if (count == 0) {
                if (queryRefValueExists(conn, valueId)) {
                    throw new ConcurrencyException("Optimistic concurrency conflict during deleteRefValue for valueId " + valueId + " aggregateVersion " + oldAggregateVersion + " but found a different version or already updated.");
                } else {
                    throw new SQLException("No record found during deleteRefValue for valueId " + valueId + ". It might have been already deleted.");
                }

            }
        } catch (SQLException e) {
            logger.error("SQLException during deleteRefValue for valueId {} aggregateVersion {}: {}", valueId, oldAggregateVersion, e.getMessage(), e);
            throw e;
        } catch (Exception e) { // Catch other potential runtime exceptions
            logger.error("Exception during deleteRefValue for valueId {} aggregateVersion {}: {}", valueId, oldAggregateVersion, e.getMessage(), e);
            throw e;
        }
    }

    @Override
    public Result<String> getRefValue(int offset, int limit, String filtersJson, String globalFilter, String sortingJson) {
        Result<String> result = null;
        final Map<String, String> columnMap = Map.of(
            "valueId", "v.value_id",
            "tableId", "v.table_id",
            "tableName", "t.table_name",
            "valueCode", "v.value_code",
            "valueDesc", "v.value_desc",
            "startTs", "v.start_ts",
            "endTs", "v.end_ts",
            "displayOrder", "v.display_order",
            "active", "v.active"
        );
        List<Map<String, Object>> filters = parseJsonList(filtersJson);
        List<Map<String, Object>> sorting = parseJsonList(sortingJson);

        String s =
            """
                SELECT COUNT(*) OVER () AS total,
                v.value_id, v.table_id, t.table_name, v.value_code, v.value_desc, v.start_ts, v.end_ts,
                v.display_order, v.active, v.update_user, v.update_ts, v.aggregate_version
                FROM ref_value_t v
                INNER JOIN ref_table_t t ON t.table_id = v.table_id
                WHERE 1=1
            """;

        List<Object> parameters = new ArrayList<>();

        String[] searchColumns = {"t.table_name", "v.value_code", "v.value_desc"};
        String sqlBuilder = s + dynamicFilter(Arrays.asList("v.table_id", "v.value_id"), Arrays.asList(searchColumns), filters, columnMap, parameters) +
                globalFilter(globalFilter, searchColumns, parameters) +
                dynamicSorting("v.display_order, v.value_code", sorting, columnMap) +
                "\nLIMIT ? OFFSET ?";

        parameters.add(limit);
        parameters.add(offset);

        String sql = sqlBuilder.toString();
        if(logger.isTraceEnabled()) logger.trace("sql = {}", sql);
        int total = 0;
        List<Map<String, Object>> refValues = new ArrayList<>();

        try (Connection connection = ds.getConnection();
            PreparedStatement preparedStatement = connection.prepareStatement(sql)) {

            populateParameters(preparedStatement, parameters);

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
                    map.put("aggregateVersion", resultSet.getLong("aggregate_version"));

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
                "display_order, active, update_user, update_ts, aggregate_version " +
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
                        refValueMap.put("aggregateVersion", resultSet.getLong("aggregate_version"));
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
    public void createRefLocale(Connection conn, Map<String, Object> event) throws SQLException, Exception {
        // SQL statement for inserting into value_locale_t
        final String sql =
                """
                   INSERT INTO value_locale_t(value_id, language, value_label, aggregate_version)
                   VALUES (?, ?, ?, ?)
                """;

        Result<String> result;
        Map<String, Object> map = (Map<String, Object>)event.get(PortalConstants.DATA);
        String valueId = (String) map.get("valueId");
        String language = (String) map.get("language");
        long newAggregateVersion = SqlUtil.getNewAggregateVersion(event);

        // Construct a unique identifier string for the success result
        String createdId = valueId + ":" + language;

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
            // 4. aggregate_version (New version for optimistic concurrency)
            statement.setLong(4, newAggregateVersion);

            // Execute insert
            int count = statement.executeUpdate();
            if (count == 0) {
                throw new SQLException(String.format("Failed during createRefLocale for valueId %s language %s with aggregateVersion %d", valueId, language, newAggregateVersion));
            }
        } catch (SQLException e) {
            logger.error("SQLException during createRefLocale for valueId {} language {} aggregateVersion {}: {}", createdId, language, newAggregateVersion, e.getMessage(), e);
            throw e;
        } catch (Exception e) { // Catch other potential runtime exceptions
            logger.error("Exception during createRefLocale for valueId {} language {} aggregateVersion {}: {}", createdId, language, newAggregateVersion, e.getMessage(), e);
            throw e;
        }
    }

    private boolean queryRefLocaleExists(Connection conn, String valueId, String language) throws SQLException {
        final String sql =
                """
                SELECT COUNT(*) FROM value_locale_t WHERE value_id = ? AND language = ?
                """;
        try (PreparedStatement pst = conn.prepareStatement(sql)) {
            pst.setObject(1, UUID.fromString(valueId));
            pst.setString(2, language);
            try (ResultSet rs = pst.executeQuery()) {
                return rs.next() && rs.getInt(1) > 0;
            }
        }
    }

    @Override
    public void updateRefLocale(Connection conn, Map<String, Object> event) throws SQLException, Exception{
        final String sql =
                """
                UPDATE value_locale_t SET value_label = ?, aggregate_version = ?
                WHERE value_id = ? AND language = ? AND aggregate_version = ?
                """;
        Result<String> result;
        Map<String, Object> map = (Map<String, Object>)event.get(PortalConstants.DATA);
        String valueId = (String) map.get("valueId");
        String language = (String) map.get("language");
        long oldAggregateVersion = SqlUtil.getOldAggregateVersion(event);
        long newAggregateVersion = SqlUtil.getNewAggregateVersion(event);

        String updatedId = valueId + ":" + language;

        try (PreparedStatement statement = conn.prepareStatement(sql)) {

            String valueLabel = (String) map.get("valueLabel");
            if (valueLabel != null && !valueLabel.isEmpty()) {
                statement.setString(1, valueLabel);
            } else {
                statement.setNull(1, Types.VARCHAR);
            }
            // 2. aggregate_version (New version for optimistic concurrency)
            statement.setLong(2, newAggregateVersion);

            statement.setObject(3, UUID.fromString(valueId));
            statement.setString(4, language);
            statement.setLong(5, oldAggregateVersion);


            int count = statement.executeUpdate();
            if (count == 0) {
                if (queryRefLocaleExists(conn, valueId, language)) {
                    throw new ConcurrencyException("Optimistic concurrency conflict during updateRefLocale for valueId " + valueId + " language " + language + ". Expected version " + oldAggregateVersion + " but found a different version " + newAggregateVersion + ".");
                } else {
                    throw new SQLException("No record found during updateRefLocale for valueId " + valueId + " language " + language + ".");
                }
            }
        } catch (SQLException e) {
            logger.error("SQLException during updateRefLocale for valueId {} language {}  (old: {}) -> (new: {}): {}", valueId, language, oldAggregateVersion, newAggregateVersion, e.getMessage(), e);
            throw e;
        } catch (Exception e) {
            logger.error("Exception during updateRefLocale for valueId {} language {}  (old: {}) -> (new: {}): {}", valueId, language, oldAggregateVersion, newAggregateVersion, e.getMessage(), e);
            throw e;
        }
    }

    @Override
    public void deleteRefLocale(Connection conn, Map<String, Object> event) throws SQLException, Exception {
        final String sql = "DELETE FROM value_locale_t WHERE value_id = ? AND language = ? AND aggregate_version = ?";
        Map<String, Object> map = (Map<String, Object>)event.get(PortalConstants.DATA);
        String valueId = (String) map.get("valueId");
        String language = (String) map.get("language");
        long oldAggregateVersion = SqlUtil.getOldAggregateVersion(event);

        String deletedId = valueId + ":" + language;

        try (PreparedStatement statement = conn.prepareStatement(sql)) {

            // Set parameters for the WHERE clause
            statement.setObject(1, UUID.fromString(valueId));
            statement.setString(2, language);
            statement.setLong(3, oldAggregateVersion);

            // Execute delete
            int count = statement.executeUpdate();
            if (count == 0) {
                if (queryRefLocaleExists(conn, valueId, language)) {
                    throw new ConcurrencyException("Optimistic concurrency conflict during deleteRefLocale for valueId " + valueId + " language " + language + " aggregateVersion " + oldAggregateVersion + " but found a different version or already updated.");
                } else {
                    throw new SQLException("No record found during deleteRefLocale for valueId " + valueId + " language " + language + ". It might have been already deleted.");
                }
            }
        } catch (SQLException e) {
            logger.error("SQLException during deleteRefLocale for valueId {} language {} aggregateVersion {}: {}", valueId, language, oldAggregateVersion, e.getMessage(), e);
            throw e;
        } catch (Exception e) {
            logger.error("Exception during deleteRefLocale for valueId {} language {} aggregateVersion {}: {}", valueId, language, oldAggregateVersion, e.getMessage(), e);
            throw e;
        }
    }

    @Override
    public Result<String> getRefLocale(int offset, int limit, String filtersJson, String globalFilter, String sortingJson) {
        Result<String> result = null;
        final Map<String, String> columnMap = Map.of(
                "valueId", "l.value_id",
                "valueCode", "v.value_code",
                "valueDesc", "v.value_desc",
                "language", "l.language",
                "valueLabel", "l.value_label",
                "aggregateVersion", "l.aggregate_version"
        );
        List<Map<String, Object>> filters = parseJsonList(filtersJson);
        List<Map<String, Object>> sorting = parseJsonList(sortingJson);

        String s =
                """
                    SELECT COUNT(*) OVER () AS total,
                    l.value_id, v.value_code, v.value_desc, l.language, l.value_label, l.aggregate_version
                    FROM value_locale_t l
                    INNER JOIN ref_value_t v ON v.value_id = l.value_id
                    WHERE 1=1
                """;

        List<Object> parameters = new ArrayList<>();

        String[] searchColumns = {"v.value_code", "v.value_desc", "l.value_label"};
        String sqlBuilder = s + dynamicFilter(Arrays.asList("l.value_id"), Arrays.asList(searchColumns), filters, columnMap, parameters) +
                globalFilter(globalFilter, searchColumns, parameters) +
                dynamicSorting("l.value_id, l.language", sorting, columnMap) +
                "\nLIMIT ? OFFSET ?";

        parameters.add(limit);
        parameters.add(offset);

        String sql = sqlBuilder.toString();
        int total = 0;
        List<Map<String, Object>> locales = new ArrayList<>(); // List to hold results

        try (Connection connection = ds.getConnection();
            PreparedStatement preparedStatement = connection.prepareStatement(sql)) {

            populateParameters(preparedStatement, parameters);

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
                    map.put("aggregateVersion", resultSet.getLong("aggregate_version"));

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
    public void createRefRelationType(Connection conn, Map<String, Object> event) throws SQLException, Exception {
        // SQL statement for inserting into relation_type_t
        final String sql =
                """
                INSERT INTO relation_type_t(relation_id, relation_name, relation_desc, update_user, update_ts, aggregate_version)
                VALUES (?, ?, ?, ?, ?,  ?)
                """;
        Map<String, Object> map = (Map<String, Object>)event.get(PortalConstants.DATA);
        String relationId = (String) map.get("relationId");
        long newAggregateVersion = SqlUtil.getNewAggregateVersion(event);

        try (PreparedStatement statement = conn.prepareStatement(sql)) {

            statement.setObject(1, UUID.fromString(relationId));
            statement.setString(2, (String)map.get("relationName"));
            statement.setString(3, (String)map.get("relationDesc"));
            statement.setString(4, (String)event.get(Constants.USER));
            statement.setObject(5, OffsetDateTime.parse((String)event.get(CloudEventV1.TIME)));
            statement.setLong(6, newAggregateVersion);
            // Execute insert
            int count = statement.executeUpdate();
            if (count == 0) {
                throw new SQLException(String.format("Failed during createRefRelationType for relationId %s with aggregateVersion %d", relationId, newAggregateVersion));
            }
        } catch (SQLException e) {
            logger.error("SQLException during createRefRelationType for relationId {} aggregateVersion {}: {}", relationId, newAggregateVersion, e.getMessage(), e);
            throw e;
        } catch (Exception e) {
            logger.error("Exception during createRefRelationType for relationId {} aggregateVersion {}: {}", relationId, newAggregateVersion, e.getMessage(), e);
            throw e;
        }
    }

    private boolean queryRefRelationTypeExists(Connection conn, String relationId) throws SQLException {
        final String sql =
                """
                SELECT COUNT(*) FROM relation_type_t WHERE relation_id = ?
                """;
        try (PreparedStatement pst = conn.prepareStatement(sql)) {
            pst.setObject(1, UUID.fromString(relationId));
            try (ResultSet rs = pst.executeQuery()) {
                return rs.next() && rs.getInt(1) > 0;
            }
        }
    }

    @Override
    public void updateRefRelationType(Connection conn, Map<String, Object> event) throws SQLException, Exception {
        // SQL statement for updating relation_type_t
        final String sql =
                """
                UPDATE relation_type_t SET relation_name = ?, relation_desc = ?,
                update_user = ?, update_ts = ?, aggregate_version = ? WHERE relation_id = ? AND aggregate_version = ?
                """;
        Map<String, Object> map = (Map<String, Object>)event.get(PortalConstants.DATA);
        String relationId = (String) map.get("relationId");
        long oldAggregateVersion = SqlUtil.getOldAggregateVersion(event);
        long newAggregateVersion = SqlUtil.getNewAggregateVersion(event);

        try (PreparedStatement statement = conn.prepareStatement(sql)) {

            statement.setString(1, (String)map.get("relationName"));
            statement.setString(2, (String)map.get("relationDesc"));
            statement.setString(3, (String)event.get(Constants.USER));
            statement.setObject(4, OffsetDateTime.parse((String)event.get(CloudEventV1.TIME)));
            statement.setLong(5, newAggregateVersion);
            statement.setObject(6, UUID.fromString(relationId));
            statement.setLong(7, oldAggregateVersion);

            int count = statement.executeUpdate();
            if (count == 0) {
                if (queryRefRelationTypeExists(conn, relationId)) {
                    throw new ConcurrencyException("Optimistic concurrency conflict during updateRefRelationType for relationId " + relationId + ". Expected version " + oldAggregateVersion + " but found a different version " + newAggregateVersion + ".");
                } else {
                    throw new SQLException("No record found during updateRefRelationType for relationId " + relationId + ".");
                }
            }
        } catch (SQLException e) {
            logger.error("SQLException during updateRefRelationType for relationId {} (old: {}) -> (new: {}): {}", relationId, oldAggregateVersion, newAggregateVersion, e.getMessage(), e);
            throw e;
        } catch (Exception e) { // Catch other potential runtime exceptions
            logger.error("Exception during updateRefRelationType for relationId {} (old: {}) -> (new: {}): {}", relationId, oldAggregateVersion, newAggregateVersion, e.getMessage(), e);
            throw e;
        }
    }

    @Override
    public void deleteRefRelationType(Connection conn, Map<String, Object> event) throws SQLException, Exception {
        // SQL statement for deleting from relation_type_t
        final String sql = "DELETE FROM relation_type_t WHERE relation_id = ? AND aggregate_version = ?";
        Map<String, Object> map = (Map<String, Object>)event.get(PortalConstants.DATA);
        String relationId = (String) map.get("relationId");
        long oldAggregateVersion = SqlUtil.getOldAggregateVersion(event);

        try (PreparedStatement statement = conn.prepareStatement(sql)) {

            // Set parameter for the WHERE clause
            statement.setObject(1, UUID.fromString(relationId));
            statement.setLong(2, oldAggregateVersion);

            // Execute delete
            int count = statement.executeUpdate();
            if (count == 0) {
                if (queryRefRelationTypeExists(conn, relationId)) {
                    throw new ConcurrencyException("Optimistic concurrency conflict during deleteRefRelationType for relationId " + relationId + " aggregateVersion " + oldAggregateVersion + " but found a different version or already updated.");
                } else {
                    throw new SQLException("No record found during deleteRefRelationType for relationId " + relationId + ". It might have been already deleted.");
                }
            }
        } catch (SQLException e) {
            logger.error("SQLException during deleteRefRelationType for relationId {} aggregateVersion {}: {}", relationId, oldAggregateVersion, e.getMessage(), e);
            throw e;
        } catch (Exception e) { // Catch other potential runtime exceptions
            logger.error("Exception during deleteRefRelationType for relationId {} aggregateVersion {}: {}", relationId, oldAggregateVersion, e.getMessage(), e);
            throw e;
        }
    }

    @Override
    public Result<String> getRefRelationType(int offset, int limit, String filtersJson, String globalFilter, String sortingJson) {
        Result<String> result = null;
        List<Map<String, Object>> filters = parseJsonList(filtersJson);
        List<Map<String, Object>> sorting = parseJsonList(sortingJson);

        String s =
            """
                SELECT COUNT(*) OVER () AS total,
                relation_id, relation_name, relation_desc, update_user, update_ts, aggregate_version
                FROM relation_type_t
                WHERE 1=1
            """;
        StringBuilder sqlBuilder = new StringBuilder(s);

        List<Object> parameters = new ArrayList<>();
        StringBuilder whereClause = new StringBuilder();

        // Material React Table Filters (Dynamic Filters) ---
        for (Map<String, Object> filter : filters) {
            String filterId = (String) filter.get("id"); // Column name
            String dbColumnName = camelToSnake(filterId);
            Object filterValue = filter.get("value");    // Value to filter by
            if (filterId != null && filterValue != null && !filterValue.toString().isEmpty()) {
                if(dbColumnName.equals("relation_id")) {
                    whereClause.append(" AND ").append(dbColumnName).append(" = ?");
                    parameters.add(UUID.fromString(filterValue.toString()));
                } else {
                    // Using LIKE for flexible filtering, assuming string/text columns
                    whereClause.append(" AND ").append(dbColumnName).append(" ILIKE ?"); // ILIKE is case-insensitive LIKE in Postgres
                    parameters.add("%" + filterValue + "%");
                }
            }
        }

        // Global Filter (Search across multiple columns)
        if (globalFilter != null && !globalFilter.isEmpty()) {
            whereClause.append(" AND (");
            // Define columns to search for global filter (e.g., table_name, table_desc)
            String[] globalSearchColumns = {"relation_name", "relation_desc"};
            List<String> globalConditions = new ArrayList<>();
            for (String col : globalSearchColumns) {
                globalConditions.add(col + " ILIKE ?");
                parameters.add("%" + globalFilter + "%");
            }
            whereClause.append(String.join(" OR ", globalConditions));
            whereClause.append(")");
        }

        // Append the constructed WHERE clause
        sqlBuilder.append(whereClause);


        // Dynamic Sorting
        StringBuilder orderByClause = new StringBuilder();
        if (sorting.isEmpty()) {
            // Default sort if none provided
            orderByClause.append(" ORDER BY relation_name");
        } else {
            orderByClause.append(" ORDER BY ");
            List<String> sortExpressions = new ArrayList<>();
            for (Map<String, Object> sort : sorting) {
                String sortId = (String) sort.get("id");
                String dbColumnName = camelToSnake(sortId);
                Boolean isDesc = (Boolean) sort.get("desc"); // 'desc' is typically a boolean or "true"/"false" string
                if (sortId != null && !sortId.isEmpty()) {
                    String direction = (isDesc != null && isDesc) ? "DESC" : "ASC";
                    // Quote column name to handle SQL keywords or mixed case
                    sortExpressions.add(dbColumnName + " " + direction);
                }
            }
            // Use default if dynamic sort failed to produce anything
            orderByClause.append(sortExpressions.isEmpty() ? "relation_name" : String.join(", ", sortExpressions));
        }
        sqlBuilder.append(orderByClause);

        // Pagination
        sqlBuilder.append("\nLIMIT ? OFFSET ?");

        parameters.add(limit);
        parameters.add(offset);

        String sql = sqlBuilder.toString();
        if(logger.isTraceEnabled()) logger.trace("sql = {}", sql);

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
                    map.put("aggregateVersion", resultSet.getLong("aggregate_version"));

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
    public void createRefRelation(Connection conn, Map<String, Object> event) throws SQLException, Exception {
        // SQL statement for inserting into relation_t
        final String sql =
                """
                INSERT INTO relation_t(relation_id, value_id_from, value_id_to, active, update_user, update_ts, aggregate_version)
                VALUES (?, ?, ?, ?, ?, ?, ?)
                """;

        Map<String, Object> map = (Map<String, Object>)event.get(PortalConstants.DATA);
        String relationId = (String) map.get("relationId");     // Part of PK
        String valueIdFrom = (String) map.get("valueIdFrom"); // Part of PK
        String valueIdTo = (String) map.get("valueIdTo");     // Part of PK
        long newAggregateVersion = SqlUtil.getNewAggregateVersion(event);

        // Construct a unique identifier string for the success result/logging
        String createdId = relationId + ":" + valueIdFrom + ":" + valueIdTo;

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
            // 7. aggregate_version (New version for optimistic concurrency)
            statement.setLong(7, newAggregateVersion);
            // Execute insert
            int count = statement.executeUpdate();
            if (count == 0) {
                throw new SQLException(String.format("Failed during createRefRelation for relationId %s valueIdFrom %s valueIdTo %s with aggregateVersion %d", relationId, valueIdFrom, valueIdTo, newAggregateVersion));
            }
        } catch (SQLException e) {
            logger.error("SQLException during createRefRelation for relationId {} valueIdFrom {} valueIdTo {} aggregateVersion {}: {}", relationId, valueIdFrom, valueIdTo, newAggregateVersion, e.getMessage(), e);
            throw e;
        } catch (Exception e) {
            logger.error("Exception during createRefRelation for relationId {} valueIdFrom {} valueIdTo {} aggregateVersion {}: {}", relationId, valueIdFrom, valueIdTo, newAggregateVersion, e.getMessage(), e);
            throw e;
        }
    }

    private boolean queryRefRelationExists(Connection conn, String relationId, String valueIdFrom, String valueIdTo) throws SQLException {
        final String sql =
                """
                SELECT COUNT(*) FROM relation_t WHERE relation_id = ? AND value_id_from = ? AND value_id_to = ?
                """;
        try (PreparedStatement pst = conn.prepareStatement(sql)) {
            pst.setObject(1, UUID.fromString(relationId));
            pst.setObject(2, UUID.fromString(valueIdFrom));
            pst.setObject(3, UUID.fromString(valueIdTo));

            try (ResultSet rs = pst.executeQuery()) {
                return rs.next() && rs.getInt(1) > 0;
            }
        }
    }

    @Override
    public void updateRefRelation(Connection conn, Map<String, Object> event) throws SQLException, Exception {
        final String sql =
                """
                UPDATE relation_t SET active = ?, update_user = ?, update_ts = ?, aggregate_version = ?
                WHERE relation_id = ? AND value_id_from = ? AND value_id_to = ? AND aggregate_version = ?
                """;
        Result<String> result;
        Map<String, Object> map = (Map<String, Object>)event.get(PortalConstants.DATA);
        String relationId = (String) map.get("relationId");
        String valueIdFrom = (String) map.get("valueIdFrom");
        String valueIdTo = (String) map.get("valueIdTo");
        long oldAggregateVersion = SqlUtil.getOldAggregateVersion(event);
        long newAggregateVersion = SqlUtil.getNewAggregateVersion(event);

        // Construct a unique identifier string for the success result/logging
        String updatedId = relationId + ":" + valueIdFrom + ":" + valueIdTo;

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
            // 4. aggregate_version (New version for optimistic concurrency)
            statement.setLong(4, newAggregateVersion);

            // Set parameters for the WHERE clause (PK parts)
            // 5. relation_id
            statement.setObject(5, UUID.fromString(relationId));
            // 6. value_id_from
            statement.setObject(6, UUID.fromString(valueIdFrom));
            // 7. value_id_to
            statement.setObject(7, UUID.fromString(valueIdTo));
            // 8. aggregate_version (Old version for optimistic concurrency)
            statement.setLong(8, oldAggregateVersion);


            // Execute update
            int count = statement.executeUpdate();
            if (count == 0) {
                if (queryRefRelationExists(conn, relationId, valueIdFrom, valueIdTo)) {
                    throw new ConcurrencyException("Optimistic concurrency conflict during updateRefRelation for relationId " + relationId + " valueIdFrom " + valueIdFrom + " valueIdTo " + valueIdTo + ". Expected version " + oldAggregateVersion + " but found a different version " + newAggregateVersion + ".");
                } else {
                    throw new SQLException("No record found during updateRefRelation for to update for relationId " + relationId + " valueIdFrom " + valueIdFrom + " valueIdTo " + valueIdTo + ".");
                }
            }
        } catch (SQLException e) {
            logger.error("SQLException during updateRefRelation for relationId {} valueIdFrom {} valueIdTo {} (old: {}) -> (new: {}): {}", relationId, valueIdFrom, valueIdTo, oldAggregateVersion, newAggregateVersion, e.getMessage(), e);
            throw e;
        } catch (Exception e) {
            logger.error("Exception during updateRefRelation for relationId {} valueIdFrom {} valueIdTo {} (old: {}) -> (new: {}): {}", relationId, valueIdFrom, valueIdTo, oldAggregateVersion, newAggregateVersion, e.getMessage(), e);
            throw e;
        }
    }

    @Override
    public void deleteRefRelation(Connection conn, Map<String, Object> event) throws SQLException, Exception {
        final String sql = "DELETE FROM relation_t WHERE relation_id = ? AND value_id_from = ? AND value_id_to = ? AND aggregate_version = ?";
        Map<String, Object> map = (Map<String, Object>)event.get(PortalConstants.DATA);
        String relationId = (String) map.get("relationId");
        String valueIdFrom = (String) map.get("valueIdFrom");
        String valueIdTo = (String) map.get("valueIdTo");
        long oldAggregateVersion = SqlUtil.getOldAggregateVersion(event);

        // Construct a unique identifier string for the success result/logging
        String deletedId = relationId + ":" + valueIdFrom + ":" + valueIdTo;

        try (PreparedStatement statement = conn.prepareStatement(sql)) {

            // Set parameters for the WHERE clause (PK parts)
            statement.setObject(1, UUID.fromString(relationId));
            statement.setObject(2, UUID.fromString(valueIdFrom));
            statement.setObject(3, UUID.fromString(valueIdTo));
            statement.setLong(4, oldAggregateVersion);

            // Execute delete
            int count = statement.executeUpdate();
            if (count == 0) {
                if (queryRefRelationExists(conn, relationId, valueIdFrom, valueIdTo)) {
                    throw new ConcurrencyException("Optimistic concurrency conflict during deleteRefRelation for relationId " + relationId + " valueIdFrom " + valueIdFrom + " valueIdTo " + valueIdTo + " aggregateVersion " + oldAggregateVersion + " but found a different version or already updated.");
                } else {
                    throw new SQLException("No record found during deleteRefRelation for relationId " + relationId + " valueIdFrom " + valueIdFrom + " valueIdTo " + valueIdTo + ". It might have been already deleted.");
                }
            }
        } catch (SQLException e) {
            logger.error("SQLException during deleteRefRelation for relationId {} valueIdFrom {} valueIdTo {} aggregateVersion {}: {}", relationId, valueIdFrom, valueIdTo, oldAggregateVersion, e.getMessage(), e);
            throw e;
        } catch (Exception e) {
            logger.error("Exception during deleteRefRelation for relationId {} valueIdFrom {} valueIdTo {} aggregateVersion {}: {}", relationId, valueIdFrom, valueIdTo, oldAggregateVersion, e.getMessage(), e);
            throw e;
        }
    }

    @Override
    public Result<String> getRefRelation(int offset, int limit, String filtersJson, String globalFilter, String sortingJson) {
        Result<String> result = null;
        final Map<String, String> columnMap = Map.of(
                "relationId", "r.relation_id",
                "relationName", "t.relation_name",
                "valueIdFrom", "r.value_id_from",
                "valueCodeFrom", "v1.value_code",
                "valueIdTo", "r.value_id_to",
                "valueCodeTo", "v2.value_code",
                "active", "r.active"
        );
        List<Map<String, Object>> filters = parseJsonList(filtersJson);
        List<Map<String, Object>> sorting = parseJsonList(sortingJson);

        String s =
            """
                SELECT COUNT(*) OVER () AS total,
                r.relation_id, t.relation_name, r.value_id_from, v1.value_code value_code_from, r.value_id_to,
                v2.value_code value_code_to, r.active, r.update_user, r.update_ts, r.aggregate_version
                FROM relation_t r
                INNER JOIN relation_type_t t ON r.relation_id = t.relation_id
                INNER JOIN ref_value_t v1 ON v1.value_id = r.value_id_from
                INNER JOIN ref_value_t v2 ON v2.value_id = r.value_id_to
                WHERE 1=1
            """;
        List<Object> parameters = new ArrayList<>();
        String[] searchColumns = {"t.relation_name", "v1.value_code", "v2.value_code"};
        String sqlBuilder = s + dynamicFilter(Arrays.asList("r.relation_id", "value_id_from", "value_id_to"), Arrays.asList(searchColumns), filters, columnMap, parameters) +
                globalFilter(globalFilter, searchColumns, parameters) +
                dynamicSorting("r.relation_id, r.value_id_from, r.value_id_to", sorting, columnMap) +
                "\nLIMIT ? OFFSET ?";

        parameters.add(limit);
        parameters.add(offset);

        String sql = sqlBuilder.toString();
        if(logger.isTraceEnabled()) logger.trace("sql = {}", sql);
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
                    map.put("aggregateVersion", resultSet.getLong("aggregate_version"));
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

    @Override
    public Result<String> getToValueCode(String relationName, String fromValueCode) {
        Result<String> result;
        try {
            // Build the base SQL query
            StringBuilder sqlBuilder = new StringBuilder(
                    """
                    SELECT STRING_AGG(v2.value_code, ', ') AS value_code_to
                    FROM relation_t r
                    INNER JOIN relation_type_t t ON r.relation_id = t.relation_id
                    INNER JOIN ref_value_t v1 ON v1.value_id = r.value_id_from
                    INNER JOIN ref_value_t v2 ON v2.value_id = r.value_id_to
                    WHERE t.relation_name = ?
                    AND r.active = true
                    """
            );

            // Handle fromValueCode - could be single value or comma-separated list
            List<String> valueCodes = new ArrayList<>();

            if (fromValueCode != null && !fromValueCode.trim().isEmpty()) {
                // Split by comma and trim each value
                String[] codes = fromValueCode.split(",");
                for (String code : codes) {
                    String trimmedCode = code.trim();
                    if (!trimmedCode.isEmpty()) {
                        valueCodes.add(trimmedCode);
                    }
                }
            }

            // Add the IN clause if we have value codes
            if (!valueCodes.isEmpty()) {
                sqlBuilder.append(" AND v1.value_code IN (");
                // Create placeholders for each value
                String placeholders = String.join(",", Collections.nCopies(valueCodes.size(), "?"));
                sqlBuilder.append(placeholders).append(")");
            }

            String sql = sqlBuilder.toString();
            if(logger.isTraceEnabled()) logger.trace("sql = {}", sql);


            // Execute the query
            try (final Connection conn = ds.getConnection(); PreparedStatement stmt = conn.prepareStatement(sql)) {

                // Set parameters
                int paramIndex = 1;
                stmt.setString(paramIndex++, relationName);

                // Set value codes for IN clause
                for (String code : valueCodes) {
                    stmt.setString(paramIndex++, code);
                }

                try (ResultSet rs = stmt.executeQuery()) {
                    if (rs.next()) {
                        String aggregatedValues = rs.getString("value_code_to");
                        result = Success.of(aggregatedValues);
                    } else {
                        result = Failure.of(new Status(OBJECT_NOT_FOUND, "relationName", relationName));
                    }
                }
            }
        } catch (Exception e) {
            logger.error("Exception:", e);
            result = Failure.of(new Status(GENERIC_EXCEPTION, e.getMessage()));
        }
        return result;
    }

}
