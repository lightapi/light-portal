package net.lightapi.portal.db.persistence;

import com.networknt.config.JsonMapper;
import com.networknt.monad.Failure;
import com.networknt.monad.Result;
import com.networknt.monad.Success;
import com.networknt.status.Status;
import com.networknt.utility.Constants;
import io.cloudevents.core.v1.CloudEventV1;
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
        // Use UPSERT based on the Primary Key (table_id): INSERT ON CONFLICT DO UPDATE
        // This handles:
        // 1. First time insert (no conflict).
        // 2. Re-creation (conflict on table_id) -> UPDATE the existing soft-deleted row (setting active=TRUE and new version).

        final String sql =
                """
                INSERT INTO ref_table_t (
                    table_id,
                    host_id,
                    table_name,
                    table_desc,
                    active,
                    editable,
                    update_user,
                    update_ts,
                    aggregate_version
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT (table_id) DO UPDATE
                SET host_id = EXCLUDED.host_id,
                    table_name = EXCLUDED.table_name,
                    table_desc = EXCLUDED.table_desc,
                    active = EXCLUDED.active, -- Use the value from the incoming event
                    editable = EXCLUDED.editable,
                    update_user = EXCLUDED.update_user,
                    update_ts = EXCLUDED.update_ts,
                    aggregate_version = EXCLUDED.aggregate_version
                -- OCC/IDM: Only update if the incoming event is newer
                WHERE ref_table_t.aggregate_version < EXCLUDED.aggregate_version
                AND ref_table_t.active = FALSE
                """;

        // Note: The original code uses a non-standard map retrieval: Map<String, Object> map = (Map<String, Object>)event.get(PortalConstants.DATA);
        // Assuming SqlUtil.extractEventData(event) is the correct utility based on other methods.
        Map<String, Object> map = SqlUtil.extractEventData(event);
        String tableId = (String) map.get("tableId");
        long newAggregateVersion = SqlUtil.getNewAggregateVersion(event);

        try (PreparedStatement statement = conn.prepareStatement(sql)) {
            // INSERT values (9 placeholders)
            int i = 1;
            // 1: table_id (Required)
            statement.setObject(i++, UUID.fromString(tableId));

            // 2: host_id
            String hostId = (String) map.get("hostId");
            if (hostId != null && !hostId.isEmpty()) {
                statement.setObject(i++, UUID.fromString(hostId));
            } else {
                statement.setNull(i++, Types.OTHER);
            }
            // 3: table_name (Required)
            statement.setString(i++, (String)map.get("tableName"));

            // 4: table_desc
            String tableDesc = (String) map.get("tableDesc");
            if (tableDesc != null && !tableDesc.isEmpty()) {
                statement.setString(i++, tableDesc);
            } else {
                statement.setNull(i++, Types.VARCHAR);
            }

            // 5: active (Using TRUE if not specified, aligning with soft delete reactivation pattern)
            Boolean active = (Boolean) map.getOrDefault("active", Boolean.TRUE);
            statement.setBoolean(i++, active);

            // 6: editable
            Boolean editable = (Boolean)map.get("editable");
            if (editable != null) {
                statement.setBoolean(i++, editable);
            } else {
                statement.setNull(i++, Types.BOOLEAN);
            }

            // 7: update_user
            statement.setString(i++, (String)event.get(Constants.USER));
            // 8: update_ts
            statement.setObject(i++, OffsetDateTime.parse((String)event.get(CloudEventV1.TIME)));
            // 9: aggregate_version
            statement.setLong(i++, newAggregateVersion);

            int count = statement.executeUpdate();
            if (count == 0) {
                // count=0 means the ON CONFLICT clause was hit, BUT the WHERE clause (aggregate_version < EXCLUDED.aggregate_version) failed.
                // This is the desired idempotent/out-of-order protection behavior. Log and ignore.
                logger.warn("Creation/Reactivation skipped for tableId {} aggregateVersion {}. A newer or same version already exists.", tableId, newAggregateVersion);
            }
        } catch (SQLException e) {
            logger.error("SQLException during createRefTable for tableId {} aggregateVersion {}: {}", tableId, newAggregateVersion, e.getMessage(), e);
            throw e;
        } catch (Exception e) {
            logger.error("Exception during createRefTable for tableId {} aggregateVersion {}: {}", tableId, newAggregateVersion, e.getMessage(), e);
            throw e;
        }
    }


    @Override
    public void updateRefTable(Connection conn, Map<String, Object> event) throws SQLException, Exception {
        // We attempt to update the record IF the incoming event's aggregate_version is greater than the current projection's version.
        // This enforces Idempotence (IDM) and Optimistic Concurrency Control (OCC) by ensuring version monotonicity.
        // Note: The 'active' state is updated based on the event payload, supporting both activation/deactivation.
        final String sql =
                """
                UPDATE ref_table_t
                SET table_name = ?,
                    table_desc = ?,
                    active = ?,
                    editable = ?,
                    update_user = ?,
                    update_ts = ?,
                    aggregate_version = ?
                WHERE table_id = ?
                  AND aggregate_version < ?
                """; // <<< CRITICAL: Changed aggregate_version = ? to aggregate_version < ? to enforce monotonicity (OCC/IDM)

        // Note: The original code uses a non-standard map retrieval: Map<String, Object> map = (Map<String, Object>)event.get(PortalConstants.DATA);
        // Assuming SqlUtil.extractEventData(event) is the correct utility based on other methods.
        Map<String, Object> map = SqlUtil.extractEventData(event);
        String tableId = (String) map.get("tableId");
        long newAggregateVersion = SqlUtil.getNewAggregateVersion(event);

        try (PreparedStatement statement = conn.prepareStatement(sql)) {
            // SET values (7 dynamic values)
            int i = 1;
            // 1: table_name (Required in update payload)
            String tableName = (String)map.get("tableName");
            statement.setString(i++, tableName);

            // 2: table_desc (Optional)
            String tableDesc = (String) map.get("tableDesc");
            if (tableDesc != null && !tableDesc.isEmpty()) {
                statement.setString(i++, tableDesc);
            } else {
                statement.setNull(i++, Types.VARCHAR);
            }
            // 3: active (Required/Defaulted - using provided value)
            // The original logic used setBoolean with potential NULL, but DDL is NOT NULL DEFAULT TRUE.
            // Assuming the event provides the desired BOOLEAN state for 'active'.
            Boolean active = (Boolean)map.getOrDefault("active", Boolean.TRUE);
            statement.setBoolean(i++, active);

            // 4: editable (Optional)
            Boolean editable = (Boolean)map.get("editable");
            if (editable != null) {
                statement.setBoolean(i++, editable);
            } else {
                // Assuming we default to TRUE if null is passed, or preserve existing if logic is more complex.
                // Since this is an update, we might set to null/default, but DDL is NOT NULL. Setting to NOT NULL DDL default if null from map.
                statement.setBoolean(i++, (Boolean)map.getOrDefault("editable", Boolean.TRUE));
            }

            // 5: update_user
            statement.setString(i++, (String)event.get(Constants.USER));
            // 6: update_ts
            statement.setObject(i++, OffsetDateTime.parse((String)event.get(CloudEventV1.TIME)));
            // 7: aggregate_version
            statement.setLong(i++, newAggregateVersion);

            // WHERE conditions (2 placeholders)
            // 8: table_id
            statement.setObject(i++, UUID.fromString(tableId));
            // 9: aggregate_version < ? (new version for OCC/IDM check)
            statement.setLong(i++, newAggregateVersion);

            // Execute update
            int count = statement.executeUpdate();
            if (count == 0) {
                // If 0 rows updated, it means the record was either not found
                // OR aggregate_version >= newAggregateVersion (OCC/IDM check failed).
                // We IGNORE the failure and log a warning, as this is the desired idempotent/monotonic behavior.
                logger.warn("Update skipped for tableId {} aggregateVersion {}. Record not found or a newer/same version already exists.", tableId, newAggregateVersion);
            }
        } catch (SQLException e) {
            logger.error("SQLException during updateRefTable for tableId {} aggregateVersion {}: {}", tableId, newAggregateVersion, e.getMessage(), e);
            throw e;
        } catch (Exception e) { // Catch other potential runtime exceptions
            logger.error("Exception during updateRefTable for tableId {} aggregateVersion {}: {}", tableId, newAggregateVersion, e.getMessage(), e);
            throw e;
        }
    }

    @Override
    public void deleteRefTable(Connection conn, Map<String, Object> event) throws SQLException, Exception {
        // Use UPDATE to implement Soft Delete (setting active = FALSE).
        // OCC/IDM is enforced by checking aggregate_version < newAggregateVersion.
        final String sql =
                """
                UPDATE ref_table_t
                SET active = FALSE,
                    update_user = ?,
                    update_ts = ?,
                    aggregate_version = ?
                WHERE table_id = ?
                  AND aggregate_version < ?
                """; // <<< CRITICAL: Added aggregate_version < ? to enforce monotonicity (OCC/IDM)

        // Note: The original code uses a non-standard map retrieval: Map<String, Object> map = (Map<String, Object>)event.get(PortalConstants.DATA);
        // Assuming SqlUtil.extractEventData(event) is the correct utility based on other methods.
        Map<String, Object> map = SqlUtil.extractEventData(event);
        String tableId = (String) map.get("tableId");
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
            // 4: table_id
            statement.setObject(4, UUID.fromString(tableId));
            // 5: aggregate_version < ? (new version for OCC/IDM check)
            statement.setLong(5, newAggregateVersion);

            int count = statement.executeUpdate();
            if (count == 0) {
                // If 0 rows updated, it means:
                // 1. The record was not found (already deleted or never existed).
                // 2. The OCC/IDM check failed (aggregate_version >= newAggregateVersion).
                // We IGNORE the failure and log a warning, as this is the desired idempotent/monotonic behavior.
                logger.warn("Soft delete skipped for tableId {} aggregateVersion {}. Record not found or a newer/same version already exists.", tableId, newAggregateVersion);
            }
        } catch (SQLException e) {
            logger.error("SQLException during deleteRefTable for tableId {} aggregateVersion {}: {}", tableId, newAggregateVersion, e.getMessage(), e);
            throw e;
        } catch (Exception e) {
            logger.error("Exception during deleteRefTable for tableId {} aggregateVersion {}: {}", tableId, newAggregateVersion, e.getMessage(), e);
            throw e;
        }
    }


    @Override
    public Result<String> getRefTable(int offset, int limit, String filtersJson, String globalFilter, String sortingJson, boolean active, String hostId) {
        Result<String> result = null;

        List<Map<String, Object>> filters = parseJsonList(filtersJson);
        List<Map<String, Object>> sorting = parseJsonList(sortingJson);

        String s =
                """
                SELECT COUNT(*) OVER () AS total,
                table_id, host_id, table_name, table_desc, active, editable, update_user, update_ts, aggregate_version
                FROM ref_table_t
                """;

        List<Object> parameters = new ArrayList<>();
        if (hostId != null && !hostId.isEmpty()) {
            s = s + " WHERE (host_id = ? OR host_id IS NULL)";
            parameters.add(UUID.fromString(hostId));
        } else {
            s = s + " WHERE host_id IS NULL";
        }

        String activeClause = SqlUtil.buildMultiTableActiveClause(active);
        String[] searchColumns = {"table_name", "table_desc"};
        String sqlBuilder = s + activeClause +
                dynamicFilter(Arrays.asList("table_id", "host_id"), Arrays.asList(searchColumns), filters, null, parameters) +
                globalFilter(globalFilter, searchColumns, parameters) +
                dynamicSorting("table_name", sorting, null) +
                "\nLIMIT ? OFFSET ?";

        parameters.add(limit);
        parameters.add(offset);

        int total = 0;
        List<Map<String, Object>> refTables = new ArrayList<>();

        try (Connection connection = ds.getConnection();
            PreparedStatement preparedStatement = connection.prepareStatement(sqlBuilder)) {
            populateParameters(preparedStatement, parameters);
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
        sqlBuilder.append("SELECT table_id, table_name FROM ref_table_t WHERE active = TRUE "); // Base query

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
        // Use UPSERT based on the Primary Key (value_id): INSERT ON CONFLICT DO UPDATE
        // This handles:
        // 1. First time insert (no conflict).
        // 2. Re-creation (conflict on value_id) -> UPDATE the existing soft-deleted row (setting active=TRUE and new version).

        final String sql =
                """
                INSERT INTO ref_value_t(
                    value_id,
                    table_id,
                    value_code,
                    value_desc,
                    start_ts,
                    end_ts,
                    display_order,
                    active,
                    update_user,
                    update_ts,
                    aggregate_version
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT (value_id) DO UPDATE
                SET table_id = EXCLUDED.table_id,
                    value_code = EXCLUDED.value_code,
                    value_desc = EXCLUDED.value_desc,
                    start_ts = EXCLUDED.start_ts,
                    end_ts = EXCLUDED.end_ts,
                    display_order = EXCLUDED.display_order,
                    active = EXCLUDED.active,
                    update_user = EXCLUDED.update_user,
                    update_ts = EXCLUDED.update_ts,
                    aggregate_version = EXCLUDED.aggregate_version
                -- OCC/IDM: Only update if the incoming event is newer
                WHERE ref_value_t.aggregate_version < EXCLUDED.aggregate_version
                AND ref_value_t.active = FALSE
                """;

        // Note: The original code uses a non-standard map retrieval: Map<String, Object> map = (Map<String, Object>)event.get(PortalConstants.DATA);
        // Assuming SqlUtil.extractEventData(event) is the correct utility based on other methods.
        Map<String, Object> map = SqlUtil.extractEventData(event);
        String valueId = (String) map.get("valueId");
        long newAggregateVersion = SqlUtil.getNewAggregateVersion(event);

        try (PreparedStatement statement = conn.prepareStatement(sql)) {
            // INSERT values (11 placeholders)
            int i = 1;
            // 1. value_id (Required)
            statement.setObject(i++, UUID.fromString(valueId));
            // 2. table_id (Required)
            statement.setObject(i++, UUID.fromString((String)map.get("tableId")));
            // 3. value_code (Required)
            statement.setString(i++, (String)map.get("valueCode"));

            // 4. value_desc (Optional)
            String valueDesc = (String) map.get("valueDesc");
            if (valueDesc != null && !valueDesc.isEmpty()) {
                statement.setString(i++, valueDesc);
            } else {
                statement.setNull(i++, Types.VARCHAR);
            }

            // 5. start_time (Optional OffsetDateTime)
            if (map.get("startTs") != null && map.get("startTs") instanceof String) {
                try {
                    statement.setObject(i++, OffsetDateTime.parse((String)map.get("startTs")));
                } catch (java.time.format.DateTimeParseException e) {
                    logger.warn("Invalid format for startTs '{}', setting NULL.", map.get("startTs"), e);
                    statement.setNull(i++, Types.TIMESTAMP_WITH_TIMEZONE);
                }
            } else {
                statement.setNull(i++, Types.TIMESTAMP_WITH_TIMEZONE);
            }

            // 6. end_time (Optional OffsetDateTime)
            if (map.get("endTs") != null && map.get("endTs") instanceof String) {
                try {
                    statement.setObject(i++, OffsetDateTime.parse((String)map.get("endTs")));
                } catch (java.time.format.DateTimeParseException e) {
                    logger.warn("Invalid format for endTs '{}', setting NULL.", map.get("endTs"), e);
                    statement.setNull(i++, Types.TIMESTAMP_WITH_TIMEZONE);
                }
            } else {
                statement.setNull(i++, Types.TIMESTAMP_WITH_TIMEZONE);
            }

            // 7. display_order (Optional Integer)
            if (map.get("displayOrder") instanceof Number) {
                statement.setInt(i++, ((Number) map.get("displayOrder")).intValue());
            } else {
                statement.setNull(i++, Types.INTEGER);
            }

            // 8. active (Default TRUE in DDL, but respecting payload if present, and enabling soft-delete reactivation)
            Boolean active = (Boolean)map.getOrDefault("active", Boolean.TRUE);
            statement.setBoolean(i++, active);


            // 9. update_user (From event metadata)
            statement.setString(i++, (String)event.get(Constants.USER));
            // 10. update_ts (From event metadata)
            statement.setObject(i++, OffsetDateTime.parse((String)event.get(CloudEventV1.TIME)));
            // 11. aggregate_version (New version for optimistic concurrency)
            statement.setLong(i++, newAggregateVersion);

            // Execute insert
            int count = statement.executeUpdate();
            if (count == 0) {
                // count=0 means the ON CONFLICT clause was hit, BUT the WHERE clause (aggregate_version < EXCLUDED.aggregate_version) failed.
                // This is the desired idempotent/out-of-order protection behavior. Log and ignore.
                logger.warn("Creation/Reactivation skipped for valueId {} aggregateVersion {}. A newer or same version already exists.", valueId, newAggregateVersion);
            }
        } catch (SQLException e) {
            logger.error("SQLException during createRefValue for valueId {} aggregateVersion {}: {}", valueId, newAggregateVersion, e.getMessage(), e);
            throw e;
        } catch (Exception e) {
            logger.error("Exception during createRefValue for valueId {} aggregateVersion {}: {}", valueId, newAggregateVersion, e.getMessage(), e);
            throw e;
        }
    }

    @Override
    public void updateRefValue(Connection conn, Map<String, Object> event) throws SQLException, Exception {
        // We attempt to update the record IF the incoming event's aggregate_version is greater than the current projection's version.
        // This enforces Idempotence (IDM) and Optimistic Concurrency Control (OCC) by ensuring version monotonicity.
        // Note: The 'active' state is updated based on the event payload (if present).
        final String sql =
                """
                UPDATE ref_value_t
                SET table_id = ?,
                    value_code = ?,
                    value_desc = ?,
                    start_ts = ?,
                    end_ts = ?,
                    display_order = ?,
                    active = ?,
                    update_user = ?,
                    update_ts = ?,
                    aggregate_version = ?
                WHERE value_id = ?
                  AND aggregate_version < ?
                """; // <<< CRITICAL: Changed aggregate_version = ? to aggregate_version < ? to enforce monotonicity (OCC/IDM)

        // Note: The original code uses a non-standard map retrieval: Map<String, Object> map = (Map<String, Object>)event.get(PortalConstants.DATA);
        // Assuming SqlUtil.extractEventData(event) is the correct utility based on other methods.
        Map<String, Object> map = SqlUtil.extractEventData(event);
        String valueId = (String) map.get("valueId");
        long newAggregateVersion = SqlUtil.getNewAggregateVersion(event);

        try (PreparedStatement statement = conn.prepareStatement(sql)) {
            // SET values (10 dynamic values)
            int i = 1;
            // 1. table_id (Required)
            statement.setObject(i++, UUID.fromString((String)map.get("tableId")));
            // 2. value_code (Required)
            statement.setString(i++, (String)map.get("valueCode"));

            // 3. value_desc (Optional)
            String valueDesc = (String) map.get("valueDesc");
            if (valueDesc != null && !valueDesc.isEmpty()) {
                statement.setString(i++, valueDesc);
            } else {
                statement.setNull(i++, Types.VARCHAR);
            }

            // 4. start_time (Optional OffsetDateTime)
            if (map.get("startTs") != null && map.get("startTs") instanceof String) {
                try {
                    statement.setObject(i++, OffsetDateTime.parse((String)map.get("startTs")));
                } catch (java.time.format.DateTimeParseException e) {
                    logger.warn("Invalid format for startTs '{}', setting NULL.", map.get("startTs"), e);
                    statement.setNull(i++, Types.TIMESTAMP_WITH_TIMEZONE);
                }
            } else {
                statement.setNull(i++, Types.TIMESTAMP_WITH_TIMEZONE);
            }

            // 5. end_time (Optional OffsetDateTime)
            if (map.get("endTs") != null && map.get("endTs") instanceof String) {
                try {
                    statement.setObject(i++, OffsetDateTime.parse((String)map.get("endTs")));
                } catch (java.time.format.DateTimeParseException e) {
                    logger.warn("Invalid format for endTs '{}', setting NULL.", map.get("endTs"), e);
                    statement.setNull(i++, Types.TIMESTAMP_WITH_TIMEZONE);
                }
            } else {
                statement.setNull(i++, Types.TIMESTAMP_WITH_TIMEZONE);
            }

            // 6. display_order (Optional Integer)
            if (map.get("displayOrder") instanceof Number) {
                statement.setInt(i++, ((Number) map.get("displayOrder")).intValue());
            } else {
                statement.setNull(i++, Types.INTEGER);
            }

            // 7. active (Default TRUE in DDL, using payload value or default/TRUE for IDM)
            Boolean active = (Boolean)map.getOrDefault("active", Boolean.TRUE);
            statement.setBoolean(i++, active);


            // 8. update_user (From event metadata)
            statement.setString(i++, (String)event.get(Constants.USER));
            // 9. update_ts (From event metadata)
            statement.setObject(i++, OffsetDateTime.parse((String)event.get(CloudEventV1.TIME)));
            // 10. aggregate_version (New version for optimistic concurrency)
            statement.setLong(i++, newAggregateVersion);

            // WHERE conditions (2 placeholders)
            // 11. value_id (For WHERE clause - Required)
            statement.setObject(i++, UUID.fromString(valueId));
            // 12. aggregate_version < ? (new version for OCC/IDM check)
            statement.setLong(i++, newAggregateVersion);

            // Execute update
            int count = statement.executeUpdate();
            if (count == 0) {
                // If 0 rows updated, it means the record was either not found
                // OR aggregate_version >= newAggregateVersion (OCC/IDM check failed).
                // We IGNORE the failure and log a warning, as this is the desired idempotent/monotonic behavior.
                logger.warn("Update skipped for valueId {} aggregateVersion {}. Record not found or a newer/same version already exists.", valueId, newAggregateVersion);
            }
        } catch (SQLException e) {
            logger.error("SQLException during updateRefValue for valueId {} aggregateVersion {}: {}", valueId, newAggregateVersion, e.getMessage(), e);
            throw e;
        } catch (Exception e) { // Catch other potential runtime exceptions
            logger.error("Exception during updateRefValue for valueId {} aggregateVersion {}: {}", valueId, newAggregateVersion, e.getMessage(), e);
            throw e;
        }
    }

    @Override
    public void deleteRefValue(Connection conn, Map<String, Object> event) throws SQLException, Exception {
        // Use UPDATE to implement Soft Delete (setting active = FALSE).
        // OCC/IDM is enforced by checking aggregate_version < newAggregateVersion.
        final String sql =
                """
                UPDATE ref_value_t
                SET active = FALSE,
                    update_user = ?,
                    update_ts = ?,
                    aggregate_version = ?
                WHERE value_id = ?
                  AND aggregate_version < ?
                """; // <<< CRITICAL: Added aggregate_version < ? to enforce monotonicity (OCC/IDM)

        // Note: The original code uses a non-standard map retrieval: Map<String, Object> map = (Map<String, Object>)event.get(PortalConstants.DATA);
        // Assuming SqlUtil.extractEventData(event) is the correct utility based on other methods.
        Map<String, Object> map = SqlUtil.extractEventData(event);
        String valueId = (String) map.get("valueId");
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
            // 4: value_id
            statement.setObject(4, UUID.fromString(valueId));
            // 5: aggregate_version < ? (new version for OCC/IDM check)
            statement.setLong(5, newAggregateVersion);

            int count = statement.executeUpdate();
            if (count == 0) {
                // If 0 rows updated, it means:
                // 1. The record was not found (already deleted or never existed).
                // 2. The OCC/IDM check failed (aggregate_version >= newAggregateVersion).
                // We IGNORE the failure and log a warning, as this is the desired idempotent/monotonic behavior.
                logger.warn("Soft delete skipped for valueId {} aggregateVersion {}. Record not found or a newer/same version already exists.", valueId, newAggregateVersion);
            }
        } catch (SQLException e) {
            logger.error("SQLException during deleteRefValue for valueId {} aggregateVersion {}: {}", valueId, newAggregateVersion, e.getMessage(), e);
            throw e;
        } catch (Exception e) { // Catch other potential runtime exceptions
            logger.error("Exception during deleteRefValue for valueId {} aggregateVersion {}: {}", valueId, newAggregateVersion, e.getMessage(), e);
            throw e;
        }
    }

    @Override
    public Result<String> getRefValue(int offset, int limit, String filtersJson, String globalFilter, String sortingJson, boolean active) {
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

        String activeClause = SqlUtil.buildMultiTableActiveClause(active, "v", "t");
        String[] searchColumns = {"t.table_name", "v.value_code", "v.value_desc"};
        String sqlBuilder = s + activeClause +
                dynamicFilter(Arrays.asList("v.table_id", "v.value_id"), Arrays.asList(searchColumns), filters, columnMap, parameters) +
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
        // Use UPSERT based on the Primary Key (value_id, language): INSERT ON CONFLICT DO UPDATE
        // This handles:
        // 1. First time insert (no conflict).
        // 2. Re-creation (conflict on value_id, language) -> UPDATE the existing soft-deleted row (setting active=TRUE and new version).

        final String sql =
                """
                INSERT INTO value_locale_t(
                    value_id,
                    language,
                    value_label,
                    update_user,
                    update_ts,
                    aggregate_version,
                    active
                ) VALUES (?, ?, ?, ?, ?, ?, TRUE)
                ON CONFLICT (value_id, language) DO UPDATE
                SET value_label = EXCLUDED.value_label,
                    update_user = EXCLUDED.update_user,
                    update_ts = EXCLUDED.update_ts,
                    aggregate_version = EXCLUDED.aggregate_version,
                    active = TRUE
                -- OCC/IDM: Only update if the incoming event is newer
                WHERE value_locale_t.aggregate_version < EXCLUDED.aggregate_version
                AND value_locale_t.active = FALSE
                """;

        // Note: The original code uses a non-standard map retrieval: Map<String, Object> map = (Map<String, Object>)event.get(PortalConstants.DATA);
        // Assuming SqlUtil.extractEventData(event) is the correct utility based on other methods.
        Map<String, Object> map = SqlUtil.extractEventData(event);
        String valueId = (String) map.get("valueId");
        String language = (String) map.get("language");
        long newAggregateVersion = SqlUtil.getNewAggregateVersion(event);

        // Construct a unique identifier string for logging/exceptions
        String createdId = valueId + ":" + language;

        try (PreparedStatement statement = conn.prepareStatement(sql)) {
            // INSERT values (6 placeholders + active=TRUE in SQL, total 6 dynamic values)
            int i = 1;
            // 1. value_id (Required, part of PK)
            statement.setObject(i++, UUID.fromString(valueId));
            // 2. language (Required, part of PK)
            statement.setString(i++, language);

            // 3. value_label (Required as per DDL NOT NULL, though original code allowed NULL)
            String valueLabel = (String) map.get("valueLabel");
            // Assuming value_label is NOT NULL based on DDL, but if null in payload, it could cause SQL error.
            // Using logic to set string, letting SQL integrity check fail if NOT NULL is violated.
            if (valueLabel != null && !valueLabel.isEmpty()) {
                statement.setString(i++, valueLabel);
            } else {
                // The DDL says NOT NULL, so setting to empty string or throw error might be better.
                // Sticking to original logic where it set NULL but using Types.VARCHAR
                statement.setNull(i++, Types.VARCHAR);
            }

            // 4. update_user
            statement.setString(i++, (String)event.get(Constants.USER));
            // 5. update_ts
            statement.setObject(i++, OffsetDateTime.parse((String)event.get(CloudEventV1.TIME)));
            // 6. aggregate_version
            statement.setLong(i++, newAggregateVersion);

            // Execute insert
            int count = statement.executeUpdate();
            if (count == 0) {
                // count=0 means the ON CONFLICT clause was hit, BUT the WHERE clause (aggregate_version < EXCLUDED.aggregate_version) failed.
                // This is the desired idempotent/out-of-order protection behavior. Log and ignore.
                logger.warn("Creation/Reactivation skipped for valueId {} language {} aggregateVersion {}. A newer or same version already exists.", valueId, language, newAggregateVersion);
            }
        } catch (SQLException e) {
            logger.error("SQLException during createRefLocale for valueId {} language {} aggregateVersion {}: {}", createdId, language, newAggregateVersion, e.getMessage(), e);
            throw e;
        } catch (Exception e) { // Catch other potential runtime exceptions
            logger.error("Exception during createRefLocale for valueId {} language {} aggregateVersion {}: {}", createdId, language, newAggregateVersion, e.getMessage(), e);
            throw e;
        }
    }

    @Override
    public void updateRefLocale(Connection conn, Map<String, Object> event) throws SQLException, Exception{
        // We attempt to update the record IF the incoming event's aggregate_version is greater than the current projection's version.
        // This enforces Idempotence (IDM) and Optimistic Concurrency Control (OCC) by ensuring version monotonicity.
        // We explicitly set active = TRUE as an UPDATE event implies the locale should be active.
        final String sql =
                """
                UPDATE value_locale_t
                SET value_label = ?,
                    update_user = ?,
                    update_ts = ?,
                    aggregate_version = ?,
                    active = TRUE
                WHERE value_id = ?
                  AND language = ?
                  AND aggregate_version < ?
                """; // <<< CRITICAL: Changed aggregate_version = ? to aggregate_version < ? to enforce monotonicity (OCC/IDM)

        // Note: The original code uses a non-standard map retrieval: Map<String, Object> map = (Map<String, Object>)event.get(PortalConstants.DATA);
        // Assuming SqlUtil.extractEventData(event) is the correct utility based on other methods.
        Map<String, Object> map = SqlUtil.extractEventData(event);
        String valueId = (String) map.get("valueId");
        String language = (String) map.get("language");
        long newAggregateVersion = SqlUtil.getNewAggregateVersion(event);

        String updatedId = valueId + ":" + language;

        try (PreparedStatement statement = conn.prepareStatement(sql)) {
            // SET values (3 dynamic values + aggregate_version + active = TRUE in SQL)
            int i = 1;
            // 1. value_label
            String valueLabel = (String) map.get("valueLabel");
            if (valueLabel != null && !valueLabel.isEmpty()) {
                statement.setString(i++, valueLabel);
            } else {
                // The DDL says NOT NULL, so setting to empty string or appropriate value might be needed,
                // but sticking to original logic of setNull if value not present/empty.
                statement.setNull(i++, Types.VARCHAR);
            }
            // 2. update_user
            statement.setString(i++, (String)event.get(Constants.USER));
            // 3. update_ts
            statement.setObject(i++, OffsetDateTime.parse((String)event.get(CloudEventV1.TIME)));
            // 4. aggregate_version (New version for optimistic concurrency)
            statement.setLong(i++, newAggregateVersion);

            // WHERE conditions (3 placeholders)
            // 5. value_id
            statement.setObject(i++, UUID.fromString(valueId));
            // 6. language
            statement.setString(i++, language);
            // 7. aggregate_version < ? (new version for OCC/IDM check)
            statement.setLong(i++, newAggregateVersion);

            int count = statement.executeUpdate();
            if (count == 0) {
                // If 0 rows updated, it means the record was either not found
                // OR aggregate_version >= newAggregateVersion (OCC/IDM check failed).
                // We IGNORE the failure and log a warning, as this is the desired idempotent/monotonic behavior.
                logger.warn("Update skipped for valueId {} language {} aggregateVersion {}. Record not found or a newer/same version already exists.", valueId, language, newAggregateVersion);
            }
        } catch (SQLException e) {
            logger.error("SQLException during updateRefLocale for valueId {} language {}  aggregateVersion {}: {}", valueId, language, newAggregateVersion, e.getMessage(), e);
            throw e;
        } catch (Exception e) {
            logger.error("Exception during updateRefLocale for valueId {} language {}  aggregateVersion {}: {}", valueId, language, newAggregateVersion, e.getMessage(), e);
            throw e;
        }
    }

    @Override
    public void deleteRefLocale(Connection conn, Map<String, Object> event) throws SQLException, Exception {
        // Use UPDATE to implement Soft Delete (setting active = FALSE).
        // OCC/IDM is enforced by checking aggregate_version < newAggregateVersion.
        final String sql =
                """
                UPDATE value_locale_t
                SET active = FALSE,
                    update_user = ?,
                    update_ts = ?,
                    aggregate_version = ?
                WHERE value_id = ?
                  AND language = ?
                  AND aggregate_version < ?
                """; // <<< CRITICAL: Added aggregate_version < ? to enforce monotonicity (OCC/IDM)

        // Note: The original code uses a non-standard map retrieval: Map<String, Object> map = (Map<String, Object>)event.get(PortalConstants.DATA);
        // Assuming SqlUtil.extractEventData(event) is the correct utility based on other methods.
        Map<String, Object> map = SqlUtil.extractEventData(event);
        String valueId = (String) map.get("valueId");
        String language = (String) map.get("language");
        // A delete event represents a state change, so it should have a new, incremented version.
        long newAggregateVersion = SqlUtil.getNewAggregateVersion(event);

        String deletedId = valueId + ":" + language;

        try (PreparedStatement statement = conn.prepareStatement(sql)) {
            // SET values (3 placeholders)
            // 1: update_user
            statement.setString(1, (String)event.get(Constants.USER));
            // 2: update_ts
            statement.setObject(2, OffsetDateTime.parse((String)event.get(CloudEventV1.TIME)));
            // 3: aggregate_version (the new version)
            statement.setLong(3, newAggregateVersion);

            // WHERE conditions (3 placeholders)
            // 4: value_id
            statement.setObject(4, UUID.fromString(valueId));
            // 5: language
            statement.setString(5, language);
            // 6: aggregate_version < ? (new version for OCC/IDM check)
            statement.setLong(6, newAggregateVersion);

            int count = statement.executeUpdate();
            if (count == 0) {
                // If 0 rows updated, it means:
                // 1. The record was not found (already deleted or never existed).
                // 2. The OCC/IDM check failed (aggregate_version >= newAggregateVersion).
                // We IGNORE the failure and log a warning, as this is the desired idempotent/monotonic behavior.
                logger.warn("Soft delete skipped for valueId {} language {} aggregateVersion {}. Record not found or a newer/same version already exists.", valueId, language, newAggregateVersion);
            }
        } catch (SQLException e) {
            logger.error("SQLException during deleteRefLocale for valueId {} language {} aggregateVersion {}: {}", valueId, language, newAggregateVersion, e.getMessage(), e);
            throw e;
        } catch (Exception e) {
            logger.error("Exception during deleteRefLocale for valueId {} language {} aggregateVersion {}: {}", valueId, language, newAggregateVersion, e.getMessage(), e);
            throw e;
        }
    }


    @Override
    public Result<String> getRefLocale(int offset, int limit, String filtersJson, String globalFilter, String sortingJson, boolean active) {
        Result<String> result = null;
        final Map<String, String> columnMap = Map.of(
                "valueId", "l.value_id",
                "valueCode", "v.value_code",
                "valueDesc", "v.value_desc",
                "language", "l.language",
                "valueLabel", "l.value_label",
                "aggregateVersion", "l.aggregate_version",
                "active", "l.active"
        );
        List<Map<String, Object>> filters = parseJsonList(filtersJson);
        List<Map<String, Object>> sorting = parseJsonList(sortingJson);

        String s =
                """
                    SELECT COUNT(*) OVER () AS total,
                    l.value_id, v.value_code, v.value_desc, l.language, l.value_label, l.aggregate_version, l.active
                    FROM value_locale_t l
                    INNER JOIN ref_value_t v ON v.value_id = l.value_id
                    WHERE 1=1
                """;

        List<Object> parameters = new ArrayList<>();

        String activeClause = SqlUtil.buildMultiTableActiveClause(active, "l", "v");
        String[] searchColumns = {"v.value_code", "v.value_desc", "l.value_label"};
        String sqlBuilder = s + activeClause +
                dynamicFilter(Arrays.asList("l.value_id"), Arrays.asList(searchColumns), filters, columnMap, parameters) +
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
                    map.put("active", resultSet.getBoolean("active"));

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
        // Use UPSERT based on the Primary Key (relation_id): INSERT ON CONFLICT DO UPDATE
        // This handles:
        // 1. First time insert (no conflict).
        // 2. Re-creation (conflict on relation_id) -> UPDATE the existing soft-deleted row (setting active=TRUE and new version).

        final String sql =
                """
                INSERT INTO relation_type_t(
                    relation_id,
                    relation_name,
                    relation_desc,
                    update_user,
                    update_ts,
                    aggregate_version,
                    active
                ) VALUES (?, ?, ?, ?, ?, ?, TRUE)
                ON CONFLICT (relation_id) DO UPDATE
                SET relation_name = EXCLUDED.relation_name,
                    relation_desc = EXCLUDED.relation_desc,
                    update_user = EXCLUDED.update_user,
                    update_ts = EXCLUDED.update_ts,
                    aggregate_version = EXCLUDED.aggregate_version,
                    active = TRUE
                -- OCC/IDM: Only update if the incoming event is newer
                WHERE relation_type_t.aggregate_version < EXCLUDED.aggregate_version
                AND relation_type_t.active = FALSE
                """;

        // Note: The original code uses a non-standard map retrieval: Map<String, Object> map = (Map<String, Object>)event.get(PortalConstants.DATA);
        // Assuming SqlUtil.extractEventData(event) is the correct utility based on other methods.
        Map<String, Object> map = SqlUtil.extractEventData(event);
        String relationId = (String) map.get("relationId");
        long newAggregateVersion = SqlUtil.getNewAggregateVersion(event);

        try (PreparedStatement statement = conn.prepareStatement(sql)) {
            // INSERT values (6 placeholders + active=TRUE in SQL, total 6 dynamic values)
            int i = 1;
            // 1. relation_id
            statement.setObject(i++, UUID.fromString(relationId));
            // 2. relation_name (Required)
            statement.setString(i++, (String)map.get("relationName"));
            // 3. relation_desc (Required)
            statement.setString(i++, (String)map.get("relationDesc"));

            // 4. update_user
            statement.setString(i++, (String)event.get(Constants.USER));
            // 5. update_ts
            statement.setObject(i++, OffsetDateTime.parse((String)event.get(CloudEventV1.TIME)));
            // 6. aggregate_version
            statement.setLong(i++, newAggregateVersion);

            // Execute insert
            int count = statement.executeUpdate();
            if (count == 0) {
                // count=0 means the ON CONFLICT clause was hit, BUT the WHERE clause (aggregate_version < EXCLUDED.aggregate_version) failed.
                // This is the desired idempotent/out-of-order protection behavior. Log and ignore.
                logger.warn("Creation/Reactivation skipped for relationId {} aggregateVersion {}. A newer or same version already exists.", relationId, newAggregateVersion);
            }
        } catch (SQLException e) {
            logger.error("SQLException during createRefRelationType for relationId {} aggregateVersion {}: {}", relationId, newAggregateVersion, e.getMessage(), e);
            throw e;
        } catch (Exception e) {
            logger.error("Exception during createRefRelationType for relationId {} aggregateVersion {}: {}", relationId, newAggregateVersion, e.getMessage(), e);
            throw e;
        }
    }


    @Override
    public void updateRefRelationType(Connection conn, Map<String, Object> event) throws SQLException, Exception {
        // We attempt to update the record IF the incoming event's aggregate_version is greater than the current projection's version.
        // This enforces Idempotence (IDM) and Optimistic Concurrency Control (OCC) by ensuring version monotonicity.
        // We explicitly set active = TRUE as an UPDATE event implies the relation type should be active.
        final String sql =
                """
                UPDATE relation_type_t
                SET relation_name = ?,
                    relation_desc = ?,
                    update_user = ?,
                    update_ts = ?,
                    aggregate_version = ?,
                    active = TRUE
                WHERE relation_id = ?
                  AND aggregate_version < ?
                """; // <<< CRITICAL: Changed aggregate_version = ? to aggregate_version < ? to enforce monotonicity (OCC/IDM)

        // Note: The original code uses a non-standard map retrieval: Map<String, Object> map = (Map<String, Object>)event.get(PortalConstants.DATA);
        // Assuming SqlUtil.extractEventData(event) is the correct utility based on other methods.
        Map<String, Object> map = SqlUtil.extractEventData(event);
        String relationId = (String) map.get("relationId");
        long newAggregateVersion = SqlUtil.getNewAggregateVersion(event);

        try (PreparedStatement statement = conn.prepareStatement(sql)) {
            // SET values (5 dynamic values + active = TRUE in SQL)
            int i = 1;
            // 1. relation_name
            statement.setString(i++, (String)map.get("relationName"));
            // 2. relation_desc
            statement.setString(i++, (String)map.get("relationDesc"));
            // 3. update_user
            statement.setString(i++, (String)event.get(Constants.USER));
            // 4. update_ts
            statement.setObject(i++, OffsetDateTime.parse((String)event.get(CloudEventV1.TIME)));
            // 5. aggregate_version
            statement.setLong(i++, newAggregateVersion);

            // WHERE conditions (2 placeholders)
            // 6. relation_id
            statement.setObject(i++, UUID.fromString(relationId));
            // 7. aggregate_version < ? (new version for OCC/IDM check)
            statement.setLong(i++, newAggregateVersion);

            int count = statement.executeUpdate();
            if (count == 0) {
                // If 0 rows updated, it means the record was either not found
                // OR aggregate_version >= newAggregateVersion (OCC/IDM check failed).
                // We IGNORE the failure and log a warning, as this is the desired idempotent/monotonic behavior.
                logger.warn("Update skipped for relationId {} aggregateVersion {}. Record not found or a newer/same version already exists.", relationId, newAggregateVersion);
            }
        } catch (SQLException e) {
            logger.error("SQLException during updateRefRelationType for relationId {} aggregateVersion {}: {}", relationId, newAggregateVersion, e.getMessage(), e);
            throw e;
        } catch (Exception e) { // Catch other potential runtime exceptions
            logger.error("Exception during updateRefRelationType for relationId {} aggregateVersion {}: {}", relationId, newAggregateVersion, e.getMessage(), e);
            throw e;
        }
    }

    @Override
    public void deleteRefRelationType(Connection conn, Map<String, Object> event) throws SQLException, Exception {
        // Use UPDATE to implement Soft Delete (setting active = FALSE).
        // OCC/IDM is enforced by checking aggregate_version < newAggregateVersion.
        final String sql =
                """
                UPDATE relation_type_t
                SET active = FALSE,
                    update_user = ?,
                    update_ts = ?,
                    aggregate_version = ?
                WHERE relation_id = ?
                  AND aggregate_version < ?
                """; // <<< CRITICAL: Added aggregate_version < ? to enforce monotonicity (OCC/IDM)

        // Note: The original code uses a non-standard map retrieval: Map<String, Object> map = (Map<String, Object>)event.get(PortalConstants.DATA);
        // Assuming SqlUtil.extractEventData(event) is the correct utility based on other methods.
        Map<String, Object> map = SqlUtil.extractEventData(event);
        String relationId = (String) map.get("relationId");
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
            // 4: relation_id
            statement.setObject(4, UUID.fromString(relationId));
            // 5: aggregate_version < ? (new version for OCC/IDM check)
            statement.setLong(5, newAggregateVersion);

            int count = statement.executeUpdate();
            if (count == 0) {
                // If 0 rows updated, it means:
                // 1. The record was not found (already deleted or never existed).
                // 2. The OCC/IDM check failed (aggregate_version >= newAggregateVersion).
                // We IGNORE the failure and log a warning, as this is the desired idempotent/monotonic behavior.
                logger.warn("Soft delete skipped for relationId {} aggregateVersion {}. Record not found or a newer/same version already exists.", relationId, newAggregateVersion);
            }
        } catch (SQLException e) {
            logger.error("SQLException during deleteRefRelationType for relationId {} aggregateVersion {}: {}", relationId, newAggregateVersion, e.getMessage(), e);
            throw e;
        } catch (Exception e) { // Catch other potential runtime exceptions
            logger.error("Exception during deleteRefRelationType for relationId {} aggregateVersion {}: {}", relationId, newAggregateVersion, e.getMessage(), e);
            throw e;
        }
    }


    @Override
    public Result<String> getRefRelationType(int offset, int limit, String filtersJson, String globalFilter, String sortingJson, boolean active) {
        Result<String> result = null;
        List<Map<String, Object>> filters = parseJsonList(filtersJson);
        List<Map<String, Object>> sorting = parseJsonList(sortingJson);

        String s =
            """
                SELECT COUNT(*) OVER () AS total,
                relation_id, relation_name, relation_desc, update_user, update_ts, aggregate_version, active
                FROM relation_type_t
                WHERE 1=1
            """;

        List<Object> parameters = new ArrayList<>();

        String activeClause = SqlUtil.buildMultiTableActiveClause(active);
        String[] searchColumns = {"relation_name", "relation_desc"};
        String sqlBuilder = s + dynamicFilter(Arrays.asList("relation_id"), Arrays.asList(searchColumns), filters, null, parameters) +
                globalFilter(globalFilter, searchColumns, parameters) +
                dynamicSorting("relation_name", sorting, null) +
                "\nLIMIT ? OFFSET ?";

        parameters.add(limit);
        parameters.add(offset);

        int total = 0;
        List<Map<String, Object>> relationTypes = new ArrayList<>();

        try (Connection connection = ds.getConnection();
            PreparedStatement preparedStatement = connection.prepareStatement(sqlBuilder)) {
            populateParameters(preparedStatement, parameters);
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
                    map.put("active", resultSet.getBoolean("active"));
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
        // Use UPSERT based on the Primary Key (relation_id, value_id_from, value_id_to): INSERT ON CONFLICT DO UPDATE
        // This handles:
        // 1. First time insert (no conflict).
        // 2. Re-creation (conflict on PK) -> UPDATE the existing soft-deleted row (setting active=TRUE and new version).

        final String sql =
                """
                INSERT INTO relation_t(
                    relation_id,
                    value_id_from,
                    value_id_to,
                    active,
                    update_user,
                    update_ts,
                    aggregate_version
                ) VALUES (?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT (relation_id, value_id_from, value_id_to) DO UPDATE
                SET active = EXCLUDED.active,
                    update_user = EXCLUDED.update_user,
                    update_ts = EXCLUDED.update_ts,
                    aggregate_version = EXCLUDED.aggregate_version
                -- OCC/IDM: Only update if the incoming event is newer
                WHERE relation_t.aggregate_version < EXCLUDED.aggregate_version
                AND relation_t.active = FALSE
                """;

        // Note: The original code uses a non-standard map retrieval: Map<String, Object> map = (Map<String, Object>)event.get(PortalConstants.DATA);
        // Assuming SqlUtil.extractEventData(event) is the correct utility based on other methods.
        Map<String, Object> map = SqlUtil.extractEventData(event);
        String relationId = (String) map.get("relationId");     // Part of PK
        String valueIdFrom = (String) map.get("valueIdFrom"); // Part of PK
        String valueIdTo = (String) map.get("valueIdTo");     // Part of PK
        long newAggregateVersion = SqlUtil.getNewAggregateVersion(event);

        // Construct a unique identifier string for the success result/logging
        String createdId = relationId + ":" + valueIdFrom + ":" + valueIdTo;

        try (PreparedStatement statement = conn.prepareStatement(sql)) {

            // INSERT values (7 placeholders)
            int i = 1;
            // 1. relation_id (Required, part of PK)
            statement.setObject(i++, UUID.fromString(relationId));
            // 2. value_id_from (Required, part of PK)
            statement.setObject(i++, UUID.fromString(valueIdFrom));
            // 3. value_id_to (Required, part of PK)
            statement.setObject(i++, UUID.fromString(valueIdTo));

            // 4. active (Default TRUE in DDL, using payload value or TRUE)
            Boolean active = (Boolean) map.getOrDefault("active", Boolean.TRUE);
            statement.setBoolean(i++, active);

            // 5. update_user (From event metadata)
            statement.setString(i++, (String)event.get(Constants.USER));

            // 6. update_ts (From event metadata)
            statement.setObject(i++, OffsetDateTime.parse((String)event.get(CloudEventV1.TIME)));
            // 7. aggregate_version (New version for optimistic concurrency)
            statement.setLong(i++, newAggregateVersion);

            // Execute insert
            int count = statement.executeUpdate();
            if (count == 0) {
                // count=0 means the ON CONFLICT clause was hit, BUT the WHERE clause (aggregate_version < EXCLUDED.aggregate_version) failed.
                // This is the desired idempotent/out-of-order protection behavior. Log and ignore.
                logger.warn("Creation/Reactivation skipped for relationId {} valueIdFrom {} valueIdTo {} aggregateVersion {}. A newer or same version already exists.", relationId, valueIdFrom, valueIdTo, newAggregateVersion);
            }
        } catch (SQLException e) {
            logger.error("SQLException during createRefRelation for relationId {} valueIdFrom {} valueIdTo {} aggregateVersion {}: {}", relationId, valueIdFrom, valueIdTo, newAggregateVersion, e.getMessage(), e);
            throw e;
        } catch (Exception e) {
            logger.error("Exception during createRefRelation for relationId {} valueIdFrom {} valueIdTo {} aggregateVersion {}: {}", relationId, valueIdFrom, valueIdTo, newAggregateVersion, e.getMessage(), e);
            throw e;
        }
    }

    @Override
    public void updateRefRelation(Connection conn, Map<String, Object> event) throws SQLException, Exception {
        // We attempt to update the record IF the incoming event's aggregate_version is greater than the current projection's version.
        // This enforces Idempotence (IDM) and Optimistic Concurrency Control (OCC) by ensuring version monotonicity.
        // Note: The 'active' state is updated based on the event payload (if present).
        final String sql =
                """
                UPDATE relation_t
                SET active = ?,
                    update_user = ?,
                    update_ts = ?,
                    aggregate_version = ?
                WHERE relation_id = ?
                  AND value_id_from = ?
                  AND value_id_to = ?
                  AND aggregate_version < ?
                """; // <<< CRITICAL: Changed aggregate_version = ? to aggregate_version < ? to enforce monotonicity (OCC/IDM)

        // Note: The original code uses a non-standard map retrieval: Map<String, Object> map = (Map<String, Object>)event.get(PortalConstants.DATA);
        // Assuming SqlUtil.extractEventData(event) is the correct utility based on other methods.
        Map<String, Object> map = SqlUtil.extractEventData(event);
        String relationId = (String) map.get("relationId");
        String valueIdFrom = (String) map.get("valueIdFrom");
        String valueIdTo = (String) map.get("valueIdTo");
        long newAggregateVersion = SqlUtil.getNewAggregateVersion(event);

        String updatedId = relationId + ":" + valueIdFrom + ":" + valueIdTo;

        try (PreparedStatement statement = conn.prepareStatement(sql)) {
            // SET values (3 dynamic values + active in SQL)
            int i = 1;
            // 1. active (Default TRUE in DDL, using payload value or TRUE)
            Boolean active = (Boolean)map.getOrDefault("active", Boolean.TRUE);
            statement.setBoolean(i++, active);
            // 2. update_user (From event metadata)
            statement.setString(i++, (String)event.get(Constants.USER));
            // 3. update_ts (From event metadata)
            statement.setObject(i++, OffsetDateTime.parse((String)event.get(CloudEventV1.TIME)));
            // 4. aggregate_version (New version for optimistic concurrency)
            statement.setLong(i++, newAggregateVersion);

            // WHERE conditions (4 placeholders)
            // 5. relation_id
            statement.setObject(i++, UUID.fromString(relationId));
            // 6. value_id_from
            statement.setObject(i++, UUID.fromString(valueIdFrom));
            // 7. value_id_to
            statement.setObject(i++, UUID.fromString(valueIdTo));
            // 8. aggregate_version < ? (new version for OCC/IDM check)
            statement.setLong(i++, newAggregateVersion);


            // Execute update
            int count = statement.executeUpdate();
            if (count == 0) {
                // If 0 rows updated, it means the record was either not found
                // OR aggregate_version >= newAggregateVersion (OCC/IDM check failed).
                // We IGNORE the failure and log a warning, as this is the desired idempotent/monotonic behavior.
                logger.warn("Update skipped for relationId {} valueIdFrom {} valueIdTo {} aggregateVersion {}. Record not found or a newer/same version already exists.", relationId, valueIdFrom, valueIdTo, newAggregateVersion);
            }
        } catch (SQLException e) {
            logger.error("SQLException during updateRefRelation for relationId {} valueIdFrom {} valueIdTo {} aggregateVersion {}: {}", relationId, valueIdFrom, valueIdTo, newAggregateVersion, e.getMessage(), e);
            throw e;
        } catch (Exception e) {
            logger.error("Exception during updateRefRelation for relationId {} valueIdFrom {} valueIdTo {} aggregateVersion {}: {}", relationId, valueIdFrom, valueIdTo, newAggregateVersion, e.getMessage(), e);
            throw e;
        }
    }

    @Override
    public void deleteRefRelation(Connection conn, Map<String, Object> event) throws SQLException, Exception {
        // Use UPDATE to implement Soft Delete (setting active = FALSE).
        // OCC/IDM is enforced by checking aggregate_version < newAggregateVersion.
        final String sql =
                """
                UPDATE relation_t
                SET active = FALSE,
                    update_user = ?,
                    update_ts = ?,
                    aggregate_version = ?
                WHERE relation_id = ?
                  AND value_id_from = ?
                  AND value_id_to = ?
                  AND aggregate_version < ?
                """; // <<< CRITICAL: Added aggregate_version < ? to enforce monotonicity (OCC/IDM)

        // Note: The original code uses a non-standard map retrieval: Map<String, Object> map = (Map<String, Object>)event.get(PortalConstants.DATA);
        // Assuming SqlUtil.extractEventData(event) is the correct utility based on other methods.
        Map<String, Object> map = SqlUtil.extractEventData(event);
        String relationId = (String) map.get("relationId");
        String valueIdFrom = (String) map.get("valueIdFrom");
        String valueIdTo = (String) map.get("valueIdTo");
        // A delete event represents a state change, so it should have a new, incremented version.
        long newAggregateVersion = SqlUtil.getNewAggregateVersion(event);

        String deletedId = relationId + ":" + valueIdFrom + ":" + valueIdTo;

        try (PreparedStatement statement = conn.prepareStatement(sql)) {
            // SET values (3 placeholders)
            // 1: update_user
            statement.setString(1, (String)event.get(Constants.USER));
            // 2: update_ts
            statement.setObject(2, OffsetDateTime.parse((String)event.get(CloudEventV1.TIME)));
            // 3: aggregate_version (the new version)
            statement.setLong(3, newAggregateVersion);

            // WHERE conditions (4 placeholders)
            // 4: relation_id
            statement.setObject(4, UUID.fromString(relationId));
            // 5: value_id_from
            statement.setObject(5, UUID.fromString(valueIdFrom));
            // 6. value_id_to
            statement.setObject(6, UUID.fromString(valueIdTo));
            // 7: aggregate_version < ? (new version for OCC/IDM check)
            statement.setLong(7, newAggregateVersion);

            // Execute update
            int count = statement.executeUpdate();
            if (count == 0) {
                // If 0 rows updated, it means:
                // 1. The record was not found (already deleted or never existed).
                // 2. The OCC/IDM check failed (aggregate_version >= newAggregateVersion).
                // We IGNORE the failure and log a warning, as this is the desired idempotent/monotonic behavior.
                logger.warn("Soft delete skipped for relationId {} valueIdFrom {} valueIdTo {} aggregateVersion {}. Record not found or a newer/same version already exists.", relationId, valueIdFrom, valueIdTo, newAggregateVersion);
            }
        } catch (SQLException e) {
            logger.error("SQLException during deleteRefRelation for relationId {} valueIdFrom {} valueIdTo {} aggregateVersion {}: {}", relationId, valueIdFrom, valueIdTo, newAggregateVersion, e.getMessage(), e);
            throw e;
        } catch (Exception e) {
            logger.error("Exception during deleteRefRelation for relationId {} valueIdFrom {} valueIdTo {} aggregateVersion {}: {}", relationId, valueIdFrom, valueIdTo, newAggregateVersion, e.getMessage(), e);
            throw e;
        }
    }

    @Override
    public Result<String> getRefRelation(int offset, int limit, String filtersJson, String globalFilter, String sortingJson, boolean active) {
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

        String activeClause = SqlUtil.buildMultiTableActiveClause(active, "r", "t", "v1", "v2");
        String[] searchColumns = {"t.relation_name", "v1.value_code", "v2.value_code"};
        String sqlBuilder = s + activeClause +
                dynamicFilter(Arrays.asList("r.relation_id", "value_id_from", "value_id_to"), Arrays.asList(searchColumns), filters, columnMap, parameters) +
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
