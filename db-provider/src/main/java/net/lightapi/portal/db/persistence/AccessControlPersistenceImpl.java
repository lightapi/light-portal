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
import static java.sql.Types.NULL;
import static net.lightapi.portal.db.util.SqlUtil.*;

public class AccessControlPersistenceImpl implements AccessControlPersistence {
    private static final Logger logger = LoggerFactory.getLogger(AccessControlPersistenceImpl.class);
    private static final String SQL_EXCEPTION = PortalDbProvider.SQL_EXCEPTION;
    private static final String GENERIC_EXCEPTION = PortalDbProvider.GENERIC_EXCEPTION;
    private static final String OBJECT_NOT_FOUND = PortalDbProvider.OBJECT_NOT_FOUND;

    private final NotificationService notificationService;

    public AccessControlPersistenceImpl(NotificationService notificationService) {
        this.notificationService = notificationService;
    }

    @Override
    public void createRole(Connection conn, Map<String, Object> event) throws SQLException, Exception {
        // Use UPSERT: INSERT ON CONFLICT DO UPDATE
        // This handles:
        // 1. First time insert (no conflict).
        // 2. Re-creation (conflict on host_id, role_id) -> UPDATE the existing soft-deleted row (setting active=TRUE and new version).

        final String sql =
                """
                INSERT INTO role_t(host_id, role_id, role_desc, update_user, update_ts, aggregate_version, active)
                VALUES (?, ?, ?, ?, ?, ?, TRUE)
                ON CONFLICT (host_id, role_id) DO UPDATE
                SET role_desc = EXCLUDED.role_desc,
                    update_user = EXCLUDED.update_user,
                    update_ts = EXCLUDED.update_ts,
                    aggregate_version = EXCLUDED.aggregate_version,
                    active = TRUE
                -- OCC/IDM: Only update if the incoming event is newer AND the existing row was soft-deleted (re-creation).
                WHERE role_t.aggregate_version < EXCLUDED.aggregate_version AND role_t.active = FALSE
                """;

        Map<String, Object> map = SqlUtil.extractEventData(event);
        String hostId = (String)map.get("hostId");
        String roleId = (String)map.get("roleId");
        long newAggregateVersion = SqlUtil.getNewAggregateVersion(event);
        try (PreparedStatement statement = conn.prepareStatement(sql)) {
            statement.setObject(1, UUID.fromString(hostId));
            statement.setString(2, roleId);

            // role_desc
            String roleDesc = (String)map.get("roleDesc");
            if (roleDesc != null && !roleDesc.isEmpty())
                statement.setString(3, roleDesc);
            else
                statement.setNull(3, Types.VARCHAR); // Use Types.VARCHAR

            // update_user
            statement.setString(4, (String)event.get(Constants.USER));
            // update_ts
            statement.setObject(5, OffsetDateTime.parse((String)event.get(CloudEventV1.TIME)));
            // aggregate_version
            statement.setLong(6, newAggregateVersion);

            int count = statement.executeUpdate();
            if (count == 0) {
                // count=0 means:
                // 1. The ON CONFLICT clause was hit, AND
                // 2. The WHERE clause failed, either because:
                //    a) The existing row had a newer or same aggregate_version (OCC/IDM protection), OR
                //    b) The existing row was already active (not soft-deleted).
                // This is the desired idempotent/out-of-order protection behavior for a 'create' event. Log and ignore.
                logger.warn("Creation/Reactivation skipped for hostId {} roleId {} aggregateVersion {}. A newer, same, or active version already exists.", hostId, roleId, newAggregateVersion);
            }
        } catch (SQLException e) {
            logger.error("SQLException during createRole for hostId {} roleId {} aggregateVersion {}: {}", hostId, roleId, newAggregateVersion, e.getMessage(), e);
            throw e;
        } catch (Exception e) {
            logger.error("Exception during createRole for hostId {} roleId {} aggregateVersion {}: {}", hostId, roleId, newAggregateVersion, e.getMessage(), e);
            throw e;
        }
    }

    @Override
    public void updateRole(Connection conn, Map<String, Object> event) throws SQLException, Exception {
        // We attempt to update the record IF the incoming event's aggregate_version is greater than the current projection's version.
        // This enforces Idempotence (IDM) and Optimistic Concurrency Control (OCC) by ensuring version monotonicity.
        // We explicitly set active = TRUE as an UPDATE event implies the role should be active.
        final String sql =
                """
                UPDATE role_t
                SET role_desc = ?,
                    update_user = ?,
                    update_ts = ?,
                    aggregate_version = ?,
                    active = TRUE
                WHERE host_id = ?
                  AND role_id = ?
                  AND aggregate_version < ?
                """; // <<< CRITICAL: Added aggregate_version < ? to enforce monotonicity (OCC/IDM)

        Map<String, Object> map = SqlUtil.extractEventData(event);
        String hostId = (String)map.get("hostId");
        String roleId = (String)map.get("roleId");
        long newAggregateVersion = SqlUtil.getNewAggregateVersion(event);

        try (PreparedStatement statement = conn.prepareStatement(sql)) {
            // role_desc
            String roleDesc = (String)map.get("roleDesc");
            if(roleDesc != null && !roleDesc.isEmpty()) {
                statement.setString(1, roleDesc);
            } else {
                statement.setNull(1, Types.VARCHAR);
            }
            // update_user
            statement.setString(2, (String)event.get(Constants.USER));
            // update_ts
            statement.setObject(3, OffsetDateTime.parse((String)event.get(CloudEventV1.TIME)));
            // aggregate_version
            statement.setLong(4, newAggregateVersion);
            // host_id
            statement.setObject(5, UUID.fromString(hostId));
            // role_id
            statement.setString(6, roleId);
            // aggregate_version < ?
            statement.setLong(7, newAggregateVersion);

            int count = statement.executeUpdate();
            if (count == 0) {
                // If 0 rows updated, it means the record was either not found (which is a data issue for an update event, but ignored for IDM)
                // OR aggregate_version >= newAggregateVersion (OCC/IDM check failed).
                // We IGNORE the failure and log a warning, as this is the desired idempotent/monotonic behavior for event projection.
                logger.warn("Update skipped for hostId {} roleId {} aggregateVersion {}. Record not found or a newer/same version already exists.", hostId, roleId, newAggregateVersion);
            }
        } catch (SQLException e) {
            logger.error("SQLException during updateRole for hostId {} roleId {} aggregateVersion {}: {}", hostId, roleId, newAggregateVersion, e.getMessage(), e);
            throw e;
        } catch (Exception e) {
            logger.error("Exception during updateRole for hostId {} roleId {} aggregateVersion: {}: {}", hostId, roleId, newAggregateVersion, e.getMessage(), e);
            throw e;
        }
    }

    @Override
    public void deleteRole(Connection conn, Map<String, Object> event) throws SQLException, Exception {
        // Use UPDATE to implement Soft Delete (setting active = FALSE).
        // OCC/IDM is enforced by checking aggregate_version < newAggregateVersion.
        final String sql =
                """
                UPDATE role_t
                SET active = FALSE,
                    update_user = ?,
                    update_ts = ?,
                    aggregate_version = ?
                WHERE host_id = ?
                  AND role_id = ?
                  AND aggregate_version < ?
                """; // <<< CRITICAL: Added aggregate_version < ? to enforce monotonicity (OCC/IDM)

        Map<String, Object> map = SqlUtil.extractEventData(event);
        String hostId = (String)map.get("hostId");
        String roleId = (String)map.get("roleId");
        // A delete event represents a state change, so it should have a new, incremented version.
        long newAggregateVersion = SqlUtil.getNewAggregateVersion(event);

        try (PreparedStatement statement = conn.prepareStatement(sql)) {
            // update_user
            statement.setString(1, (String)event.get(Constants.USER));
            // update_ts
            statement.setObject(2, OffsetDateTime.parse((String)event.get(CloudEventV1.TIME)));
            // aggregate_version (the new version)
            statement.setLong(3, newAggregateVersion);
            // host_id
            statement.setObject(4, UUID.fromString(hostId));
            // role_id
            statement.setString(5, roleId);
            // aggregate_version < ? (new version for OCC/IDM check)
            statement.setLong(6, newAggregateVersion);

            int count = statement.executeUpdate();
            if (count == 0) {
                // If 0 rows updated, it means:
                // 1. The record was not found (already deleted or never existed).
                // 2. The OCC/IDM check failed (aggregate_version >= newAggregateVersion).
                // We IGNORE the failure and log a warning, as this is the desired idempotent/monotonic behavior.
                logger.warn("Soft delete skipped for hostId {} roleId {} aggregateVersion {}. Record not found or a newer/same version already exists.", hostId, roleId, newAggregateVersion);
            }
        } catch (SQLException e) {
            logger.error("SQLException during deleteRole for hostId {} roleId {} aggregateVersion {}: {}", hostId, roleId, newAggregateVersion, e.getMessage(), e);
            throw e;
        } catch (Exception e) {
            logger.error("Exception during deleteRole for hostId {} roleId {} aggregateVersion {}: {}", hostId, roleId, newAggregateVersion, e.getMessage(), e);
            throw e;
        }
    }

    @Override
    public Result<String> queryRole(int offset, int limit, String filtersJson, String globalFilter, String sortingJson, boolean active, String hostId) {
        Result<String> result;
        List<Map<String, Object>> filters = parseJsonList(filtersJson);
        List<Map<String, Object>> sorting = parseJsonList(sortingJson);

        String s =
            """
                SELECT COUNT(*) OVER () AS total,
                host_id, role_id, role_desc, update_user,
                update_ts, aggregate_version, active
                FROM role_t
                WHERE host_id = ?
            """;

        List<Object> parameters = new ArrayList<>();
        parameters.add(UUID.fromString(hostId));

        String activeClause = SqlUtil.buildMultiTableActiveClause(active);
        String[] searchColumns = {"role_id", "role_desc"};
        String sqlBuilder = s + activeClause +
                dynamicFilter(Arrays.asList("host_id"), Arrays.asList(searchColumns), filters, null, parameters) +
                globalFilter(globalFilter, searchColumns, parameters) +
                dynamicSorting("role_id", sorting, null) +
                "\nLIMIT ? OFFSET ?";

        parameters.add(limit);
        parameters.add(offset);

        if(logger.isTraceEnabled()) logger.trace("queryRole sql: {}", sqlBuilder);

        int total = 0;
        List<Map<String, Object>> roles = new ArrayList<>();


        try (final Connection conn = ds.getConnection(); PreparedStatement preparedStatement = conn.prepareStatement(sqlBuilder)) {

            populateParameters(preparedStatement, parameters);

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
                    map.put("aggregateVersion", resultSet.getLong("aggregate_version"));
                    map.put("active", resultSet.getBoolean("active"));
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
    public Result<String> getRoleById(String hostId, String roleId) {
        final String sql =
                """
                SELECT host_id, role_id, role_desc, aggregate_version, active, update_user, update_ts
                FROM role_t
                WHERE host_id = ? AND role_id = ?
                """;
        Result<String> result;
        Map<String, Object> map = new HashMap<>();

        String searchId = hostId + ":" + roleId;

        try (Connection conn = ds.getConnection();
             PreparedStatement statement = conn.prepareStatement(sql)) {

            statement.setObject(1, UUID.fromString(hostId));
            statement.setString(2, roleId);

            try (ResultSet resultSet = statement.executeQuery()) {
                if (resultSet.next()) {
                    map.put("hostId", resultSet.getObject("host_id", UUID.class));
                    map.put("roleId", resultSet.getString("role_id"));
                    map.put("roleDesc", resultSet.getString("role_desc"));
                    map.put("aggregateVersion", resultSet.getLong("aggregate_version"));
                    map.put("active", resultSet.getBoolean("active"));
                    map.put("updateUser", resultSet.getString("update_user"));
                    map.put("updateTs", resultSet.getObject("update_ts") != null ? resultSet.getObject("update_ts", OffsetDateTime.class) : null);

                    result = Success.of(JsonMapper.toJson(map));
                } else {
                    result = Failure.of(new Status(OBJECT_NOT_FOUND, "role", searchId));
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
    public Result<String> queryRoleLabel(String hostId) {
        final String sql = "SELECT role_id from role_t WHERE host_id = ? AND active = TRUE";
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
    public Result<String> queryRolePermission(int offset, int limit, String filtersJson, String globalFilter, String sortingJson, boolean active, String hostId) {
        Result<String> result;
        final Map<String, String> columnMap = new HashMap<>(Map.of(
                "hostId", "r.host_id",
                "roleId", "r.role_id",
                "apiVersionId", "av.api_version_id",
                "apiId", "av.api_id",
                "apiVersion", "av.api_version",
                "endpointId", "rp.endpoint_id",
                "endpoint", "ae.endpoint",
                "aggregateVersion", "rp.aggregate_version",
                "updateUser", "rp.update_user",
                "updateTs", "rp.update_ts"
        ));

        columnMap.put("active", "rp.active");

        List<Map<String, Object>> filters = parseJsonList(filtersJson);
        List<Map<String, Object>> sorting = parseJsonList(sortingJson);

        String s =
                """
                SELECT COUNT(*) OVER () AS total,
                r.host_id, r.role_id, av.api_version_id, av.api_id, av.api_version, rp.endpoint_id,
                ae.endpoint, rp.aggregate_version, rp.update_user, rp.update_ts, rp.active
                FROM role_permission_t rp
                JOIN role_t r ON r.role_id = rp.role_id
                JOIN api_endpoint_t ae ON rp.host_id = ae.host_id AND rp.endpoint_id = ae.endpoint_id
                JOIN api_version_t av ON ae.host_id = av.host_id AND ae.api_version_id = av.api_version_id
                AND r.host_id = ?
                """;

        List<Object> parameters = new ArrayList<>();
        parameters.add(UUID.fromString(hostId));

        // Generate the active clause. 'rp' (role_permission_t) is the primary table, so pass it first.
        String activeClause = SqlUtil.buildMultiTableActiveClause(active, "rp", "r", "ae", "av");

        String[] searchColumns = {"r.role_id", "av.api_id", "ae.endpoint"};
        String sqlBuilder = s + activeClause +
                dynamicFilter(Arrays.asList("r.host_id", "av.api_version_id", "rp.endpoint_id"), Arrays.asList(searchColumns), filters, columnMap, parameters) +
                globalFilter(globalFilter, searchColumns, parameters) +
                dynamicSorting("r.role_id, av.api_id, av.api_version, ae.endpoint", sorting, columnMap) +
                "\nLIMIT ? OFFSET ?";

        parameters.add(limit);
        parameters.add(offset);

        if(logger.isTraceEnabled()) logger.trace("queryRolePermission sql: {}", sqlBuilder);
        int total = 0;
        List<Map<String, Object>> rolePermissions = new ArrayList<>();
        try (final Connection conn = ds.getConnection(); PreparedStatement preparedStatement = conn.prepareStatement(sqlBuilder)) {
            populateParameters(preparedStatement, parameters);
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
                    map.put("apiVersionId", resultSet.getString("api_version_id"));
                    map.put("apiId", resultSet.getString("api_id"));
                    map.put("apiVersion", resultSet.getString("api_version"));
                    map.put("endpointId", resultSet.getObject("endpoint_id", UUID.class));
                    map.put("endpoint", resultSet.getString("endpoint"));
                    map.put("aggregateVersion", resultSet.getLong("aggregate_version"));
                    map.put("updateUser", resultSet.getString("update_user"));
                    map.put("updateTs", resultSet.getObject("update_ts") != null ? resultSet.getObject("update_ts", OffsetDateTime.class) : null);
                    map.put("active", resultSet.getBoolean("active"));
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
    public Result<String> queryRoleUser(int offset, int limit, String filtersJson, String globalFilter, String sortingJson, boolean active, String hostId) {
        Result<String> result;
        final Map<String, String> columnMap = new HashMap<>(Map.of(
                "hostId", "r.host_id",
                "roleId", "r.role_id",
                "startTs", "r.start_ts",
                "endTs", "r.end_ts",
                "aggregateVersion", "r.aggregate_version",
                "updateUser", "r.update_user",
                "updateTs", "r.update_ts",
                "userId", "u.user_id",
                "email", "u.email",
                "userType", "u.user_type"
        ));
        columnMap.put("entityId", "CASE WHEN u.user_type = 'C' THEN c.customer_id WHEN u.user_type = 'E' THEN e.employee_id ELSE NULL -- Handle other cases if needed END");
        columnMap.put("firstName", "u.first_name");
        columnMap.put("lastName", "u.last_name");
        columnMap.put("managerId", "e.manager_id");
        columnMap.put("active", "r.active");


        List<Map<String, Object>> filters = parseJsonList(filtersJson);
        List<Map<String, Object>> sorting = parseJsonList(sortingJson);

        String s =
                """
                SELECT COUNT(*) OVER () AS total,
                r.host_id, r.role_id, r.start_ts, r.end_ts,
                r.aggregate_version, r.update_user, r.update_ts,
                u.user_id, u.email, u.user_type, r.active,
                CASE
                    WHEN u.user_type = 'C' THEN c.customer_id
                    WHEN u.user_type = 'E' THEN e.employee_id
                    ELSE NULL -- Handle other cases if needed
                END AS entity_id,
                e.manager_id, u.first_name, u.last_name
                FROM user_t u
                LEFT JOIN
                    customer_t c ON u.user_id = c.user_id AND u.user_type = 'C'
                LEFT JOIN
                    employee_t e ON u.user_id = e.user_id AND u.user_type = 'E'
                INNER JOIN
                    role_user_t r ON r.user_id = u.user_id
                AND r.host_id = ?
                """;

        List<Object> parameters = new ArrayList<>();
        parameters.add(UUID.fromString(hostId));

        String activeClause = SqlUtil.buildMultiTableActiveClause(active, "u", "c", "e", "r");
        String[] searchColumns = {"r.role_id", "u.email", "u.first_name", "u.last_name"};
        String sqlBuilder = s + activeClause +
                dynamicFilter(Arrays.asList("r.host_id", "u.user_id"), Arrays.asList(searchColumns), filters, columnMap, parameters) +
                globalFilter(globalFilter, searchColumns, parameters) +
                dynamicSorting("r.role_id, u.user_id", sorting, columnMap) +
                "\nLIMIT ? OFFSET ?";

        parameters.add(limit);
        parameters.add(offset);

        if(logger.isTraceEnabled()) logger.trace("queryRoleUser sql: {}", sqlBuilder);
        int total = 0;
        List<Map<String, Object>> roleUsers = new ArrayList<>();

        try (final Connection conn = ds.getConnection(); PreparedStatement preparedStatement = conn.prepareStatement(sqlBuilder)) {
            populateParameters(preparedStatement, parameters);
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
                    map.put("startTs", resultSet.getObject("start_ts") != null ? resultSet.getObject("start_ts", OffsetDateTime.class) : null);
                    map.put("endTs", resultSet.getObject("end_ts") != null ? resultSet.getObject("end_ts", OffsetDateTime.class) : null);
                    map.put("userId", resultSet.getObject("user_id", UUID.class));
                    map.put("entityId", resultSet.getString("entity_id"));
                    map.put("email", resultSet.getString("email"));
                    map.put("firstName", resultSet.getString("first_name"));
                    map.put("lastName", resultSet.getString("last_name"));
                    map.put("userType", resultSet.getString("user_type"));
                    map.put("managerId", resultSet.getString("manager_id"));
                    map.put("aggregateVersion", resultSet.getLong("aggregate_version"));
                    map.put("updateUser", resultSet.getString("update_user"));
                    map.put("updateTs", resultSet.getObject("update_ts") != null ? resultSet.getObject("update_ts", OffsetDateTime.class) : null);
                    map.put("active", resultSet.getBoolean("active"));
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
    public Result<String> getRoleUserById(String hostId, String roleId, String userId) {
        final String sql =
                """
                SELECT host_id, role_id, user_id, start_ts, end_ts,
                aggregate_version, active, update_user, update_ts
                FROM role_user_t
                WHERE host_id = ? AND role_id = ? AND user_id = ?
                """;
        Result<String> result;
        Map<String, Object> map = new HashMap<>();

        String searchId = hostId + ":" + roleId + ":" + userId;

        try (Connection conn = ds.getConnection();
             PreparedStatement statement = conn.prepareStatement(sql)) {

            statement.setObject(1, UUID.fromString(hostId));
            statement.setString(2, roleId);
            statement.setObject(3, UUID.fromString(userId));

            try (ResultSet resultSet = statement.executeQuery()) {
                if (resultSet.next()) {
                    map.put("hostId", resultSet.getObject("host_id", UUID.class));
                    map.put("roleId", resultSet.getString("role_id"));
                    map.put("userId", resultSet.getObject("user_id", UUID.class));
                    map.put("startTs", resultSet.getObject("start_ts") != null ? resultSet.getObject("start_ts", OffsetDateTime.class) : null);
                    map.put("endTs", resultSet.getObject("end_ts") != null ? resultSet.getObject("end_ts", OffsetDateTime.class) : null);
                    map.put("aggregateVersion", resultSet.getLong("aggregate_version"));
                    map.put("active", resultSet.getBoolean("active"));
                    map.put("updateUser", resultSet.getString("update_user"));
                    map.put("updateTs", resultSet.getObject("update_ts") != null ? resultSet.getObject("update_ts", OffsetDateTime.class) : null);

                    result = Success.of(JsonMapper.toJson(map));
                } else {
                    result = Failure.of(new Status(OBJECT_NOT_FOUND, "role_user", searchId));
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
    public void createRolePermission(Connection conn, Map<String, Object> event) throws SQLException, Exception {
        // Use UPSERT: INSERT ON CONFLICT DO UPDATE
        // This handles:
        // 1. First time insert (no conflict).
        // 2. Re-creation (conflict on host_id, role_id, endpoint_id) -> UPDATE the existing soft-deleted row (setting active=TRUE and new version).

        final String sql =
                """
                INSERT INTO role_permission_t(host_id, role_id, endpoint_id, update_user, update_ts, aggregate_version, active)
                VALUES (?, ?, ?, ?, ?, ?, TRUE)
                ON CONFLICT (host_id, role_id, endpoint_id) DO UPDATE
                SET update_user = EXCLUDED.update_user,
                    update_ts = EXCLUDED.update_ts,
                    aggregate_version = EXCLUDED.aggregate_version,
                    active = TRUE
                -- OCC/IDM: Only update if the incoming event is newer AND the existing row was inactive (re-creation/idempotent).
                WHERE role_permission_t.aggregate_version < EXCLUDED.aggregate_version AND role_permission_t.active = FALSE
                """;

        Map<String, Object> map = SqlUtil.extractEventData(event);
        String hostId = (String)map.get("hostId");
        String roleId = (String)map.get("roleId");
        String endpointId = (String)map.get("endpointId");
        long newAggregateVersion = SqlUtil.getNewAggregateVersion(event);
        try (PreparedStatement statement = conn.prepareStatement(sql)) {
            // INSERT values (7 placeholders)
            // 1
            statement.setObject(1, UUID.fromString(hostId));
            // 2
            statement.setString(2, roleId);
            // 3
            statement.setObject(3, UUID.fromString(endpointId));
            // 4: update_user
            statement.setString(4, (String)event.get(Constants.USER));
            // 5: update_ts
            statement.setObject(5, OffsetDateTime.parse((String)event.get(CloudEventV1.TIME)));
            // 6: aggregate_version
            statement.setLong(6, newAggregateVersion);
            // 7: active (is set to TRUE in SQL)

            int count = statement.executeUpdate();
            if (count == 0) {
                // count=0 means:
                // 1. The ON CONFLICT clause was hit, AND
                // 2. The WHERE clause failed (aggregate_version < EXCLUDED.aggregate_version)
                // This is the desired idempotent/out-of-order protection behavior. Log and ignore.
                logger.warn("Creation/Reactivation skipped for hostId {} roleId {} endpointId {} aggregateVersion {}. A newer or same version already exists.", hostId, roleId, endpointId, newAggregateVersion);
            }
        } catch (SQLException e) {
            logger.error("SQLException during createRolePermission for hostId {} roleId {} endpointId {} aggregateVersion {}: {}", hostId, roleId, endpointId, newAggregateVersion, e.getMessage(), e);
            throw e;
        } catch (Exception e) {
            logger.error("Exception during createRolePermission for hostId {} roleId {} endpointId {} aggregateVersion {}: {}", hostId, roleId, endpointId, newAggregateVersion, e.getMessage(), e);
            throw e;
        }
    }

    @Override
    public void deleteRolePermission(Connection conn, Map<String, Object> event) throws SQLException, Exception {
        // Use UPDATE to implement Soft Delete (setting active = FALSE).
        // OCC/IDM is enforced by checking aggregate_version < newAggregateVersion.
        final String sql =
                """
                UPDATE role_permission_t
                SET active = FALSE,
                    update_user = ?,
                    update_ts = ?,
                    aggregate_version = ?
                WHERE host_id = ?
                  AND role_id = ?
                  AND endpoint_id = ?
                  AND aggregate_version < ?
                """; // <<< CRITICAL: Added aggregate_version < ? to enforce monotonicity (OCC/IDM)

        Map<String, Object> map = SqlUtil.extractEventData(event);
        String hostId = (String)map.get("hostId");
        String roleId = (String)map.get("roleId");
        String endpointId = (String)map.get("endpointId");
        // A delete event represents a state change, so it should have a new, incremented version.
        long newAggregateVersion = SqlUtil.getNewAggregateVersion(event);

        try (PreparedStatement statement = conn.prepareStatement(sql)) {
            // SET values (4 placeholders)
            // 1: update_user
            statement.setString(1, (String)event.get(Constants.USER));
            // 2: update_ts
            statement.setObject(2, OffsetDateTime.parse((String)event.get(CloudEventV1.TIME)));
            // 3: aggregate_version (the new version)
            statement.setLong(3, newAggregateVersion);

            // WHERE conditions (4 placeholders)
            // 4: host_id
            statement.setObject(4, UUID.fromString(hostId));
            // 5: role_id
            statement.setString(5, roleId);
            // 6: endpoint_id
            statement.setObject(6, UUID.fromString(endpointId));
            // 7: aggregate_version < ? (new version for OCC/IDM check)
            statement.setLong(7, newAggregateVersion);

            int count = statement.executeUpdate();
            if (count == 0) {
                // If 0 rows updated, it means:
                // 1. The record was not found (already deleted or never existed).
                // 2. The OCC/IDM check failed (aggregate_version >= newAggregateVersion).
                // We IGNORE the failure and log a warning, as this is the desired idempotent/monotonic behavior.
                logger.warn("Soft delete skipped for hostId {} roleId {} endpointId {} aggregateVersion {}. Record not found or a newer/same version already exists.", hostId, roleId, endpointId, newAggregateVersion);
            }
        } catch (SQLException e) {
            logger.error("SQLException during deleteRolePermission for hostId {} roleId {} endpointId {} aggregateVersion {}: {}", hostId, roleId, endpointId, newAggregateVersion, e.getMessage(), e);
            throw e;
        } catch (Exception e) {
            logger.error("Exception during deleteRolePermission for hostId {} roleId {} endpointId {} aggregateVersion {}: {}", hostId, roleId, endpointId, newAggregateVersion, e.getMessage(), e);
            throw e;
        }
    }

    @Override
    public void createRoleUser(Connection conn, Map<String, Object> event) throws SQLException, Exception {
        // Use UPSERT: INSERT ON CONFLICT DO UPDATE
        // This handles:
        // 1. First time insert (no conflict).
        // 2. Re-creation (conflict on host_id, role_id, user_id) -> UPDATE the existing soft-deleted row (setting active=TRUE and new version).

        final String sql =
                """
                INSERT INTO role_user_t(host_id, role_id, user_id, start_ts, end_ts, update_user, update_ts, aggregate_version, active)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, TRUE)
                ON CONFLICT (host_id, role_id, user_id) DO UPDATE
                SET start_ts = EXCLUDED.start_ts,
                    end_ts = EXCLUDED.end_ts,
                    update_user = EXCLUDED.update_user,
                    update_ts = EXCLUDED.update_ts,
                    aggregate_version = EXCLUDED.aggregate_version,
                    active = TRUE
                -- OCC/IDM: Only update if the incoming event is newer
                WHERE role_user_t.aggregate_version < EXCLUDED.aggregate_version AND role_user_t.active = FALSE
                """;

        Map<String, Object> map = SqlUtil.extractEventData(event);
        String hostId = (String)map.get("hostId");
        String roleId = (String)map.get("roleId");
        String userId = (String)map.get("userId");
        long newAggregateVersion = SqlUtil.getNewAggregateVersion(event);

        try (PreparedStatement statement = conn.prepareStatement(sql)) {
            // INSERT values (8 placeholders + active=TRUE in SQL, total 8 dynamic values)
            // 1: host_id
            statement.setObject(1, UUID.fromString(hostId));
            // 2: role_id
            statement.setString(2, roleId);
            // 3: user_id
            statement.setObject(3, UUID.fromString(userId));

            // 4: start_ts
            String startTs = (String)map.get("startTs");
            if(startTs != null && !startTs.isEmpty())
                statement.setObject(4, OffsetDateTime.parse(startTs));
            else
                statement.setNull(4, Types.TIMESTAMP_WITH_TIMEZONE); // Use Types.TIMESTAMP_WITH_TIMEZONE

            // 5: end_ts
            String endTs = (String)map.get("endTs");
            if (endTs != null && !endTs.isEmpty()) {
                statement.setObject(5, OffsetDateTime.parse(endTs));
            } else {
                statement.setNull(5, Types.TIMESTAMP_WITH_TIMEZONE); // Use Types.TIMESTAMP_WITH_TIMEZONE
            }

            // 6: update_user
            statement.setString(6, (String)event.get(Constants.USER));
            // 7: update_ts
            statement.setObject(7,  OffsetDateTime.parse((String)event.get(CloudEventV1.TIME)));
            // 8: aggregate_version
            statement.setLong(8, newAggregateVersion);

            int count = statement.executeUpdate();
            if (count == 0) {
                // count=0 means the ON CONFLICT clause was hit, BUT the WHERE clause (aggregate_version < EXCLUDED.aggregate_version) failed.
                // This is the desired idempotent/out-of-order protection behavior. Log and ignore.
                logger.warn("Creation/Reactivation skipped for hostId {} roleId {} userId {} aggregateVersion {}. A newer or same version already exists.", hostId, roleId, userId, newAggregateVersion);
            }
        } catch (SQLException e) {
            logger.error("SQLException during createRoleUser for hostId {} roleId {} userId {} and aggregateVersion {}: {}", hostId, roleId, userId, newAggregateVersion, e.getMessage(), e);
            throw e;
        } catch (Exception e) {
            logger.error("Exception during createRoleUser for hostId {} roleId {} userId {} and aggregateVersion {}: {}", hostId, roleId, userId, newAggregateVersion, e.getMessage(), e);
            throw e;
        }
    }

    @Override
    public void updateRoleUser(Connection conn, Map<String, Object> event) throws SQLException, Exception {
        // We attempt to update the record IF the incoming event's aggregate_version is greater than the current projection's version.
        // This enforces Idempotence (IDM) and Optimistic Concurrency Control (OCC) by ensuring version monotonicity.
        // We explicitly set active = TRUE as an UPDATE event implies the role-user assignment should be active.
        final String sql =
                """
                UPDATE role_user_t
                SET start_ts = ?,
                    end_ts = ?,
                    update_user = ?,
                    update_ts = ?,
                    aggregate_version = ?,
                    active = TRUE
                WHERE host_id = ?
                  AND role_id = ?
                  AND user_id = ?
                  AND aggregate_version < ?
                """; // <<< CRITICAL: Added aggregate_version < ? to enforce monotonicity (OCC/IDM)

        // Note: The original code uses a non-standard map retrieval: Map<String, Object> map = (Map<String, Object>)event.get(PortalConstants.DATA);
        // Assuming SqlUtil.extractEventData(event) is the correct utility based on other methods.
        Map<String, Object> map = SqlUtil.extractEventData(event);

        String hostId = (String)map.get("hostId");
        String roleId = (String)map.get("roleId");
        String userId = (String)map.get("userId");
        long newAggregateVersion = SqlUtil.getNewAggregateVersion(event);

        try (PreparedStatement statement = conn.prepareStatement(sql)) {
            // SET values (5 dynamic values + active = TRUE in SQL)

            // 1: start_ts
            String startTs = (String)map.get("startTs");
            if(startTs != null && !startTs.isEmpty())
                statement.setObject(1, OffsetDateTime.parse(startTs));
            else
                statement.setNull(1, Types.TIMESTAMP_WITH_TIMEZONE); // Use Types.TIMESTAMP_WITH_TIMEZONE

            // 2: end_ts
            String endTs = (String)map.get("endTs");
            if (endTs != null && !endTs.isEmpty()) {
                statement.setObject(2, OffsetDateTime.parse(endTs));
            } else {
                statement.setNull(2, Types.TIMESTAMP_WITH_TIMEZONE); // Use Types.TIMESTAMP_WITH_TIMEZONE
            }

            // 3: update_user
            statement.setString(3, (String)event.get(Constants.USER));
            // 4: update_ts
            statement.setObject(4, OffsetDateTime.parse((String)event.get(CloudEventV1.TIME)));
            // 5: aggregate_version
            statement.setLong(5, newAggregateVersion);

            // WHERE conditions (4 placeholders)
            // 6: host_id
            statement.setObject(6, UUID.fromString(hostId));
            // 7: role_id
            statement.setString(7, roleId);
            // 8: user_id
            statement.setObject(8, UUID.fromString(userId));
            // 9: aggregate_version < ? (new version for OCC/IDM check)
            statement.setLong(9, newAggregateVersion);

            int count = statement.executeUpdate();
            if (count == 0) {
                // If 0 rows updated, it means the record was either not found
                // OR aggregate_version >= newAggregateVersion (OCC/IDM check failed).
                // We IGNORE the failure and log a warning, as this is the desired idempotent/monotonic behavior.
                logger.warn("Update skipped for hostId {} roleId {} userId {} aggregateVersion {}. Record not found or a newer/same version already exists.", hostId, roleId, userId, newAggregateVersion);
            }
        } catch (SQLException e) {
            logger.error("SQLException during updateRoleUser for hostId {} roleId {} userId {} aggregateVersion {}: {}", hostId, roleId, userId, newAggregateVersion, e.getMessage(), e);
            throw e;
        } catch (Exception e) {
            logger.error("Exception during updateRoleUser for hostId {} roleId {} userId {} aggregateVersion {}: {}", hostId, roleId, userId, newAggregateVersion, e.getMessage(), e);
            throw e;
        }
    }

    @Override
    public void deleteRoleUser(Connection conn, Map<String, Object> event) throws SQLException, Exception {
        // Use UPDATE to implement Soft Delete (setting active = FALSE).
        // OCC/IDM is enforced by checking aggregate_version < newAggregateVersion.
        final String sql =
                """
                UPDATE role_user_t
                SET active = FALSE,
                    update_user = ?,
                    update_ts = ?,
                    aggregate_version = ?
                WHERE host_id = ?
                  AND role_id = ?
                  AND user_id = ?
                  AND aggregate_version < ?
                """; // <<< CRITICAL: Added aggregate_version < ? to enforce monotonicity (OCC/IDM)

        Map<String, Object> map = SqlUtil.extractEventData(event);
        String hostId = (String)map.get("hostId");
        String roleId = (String)map.get("roleId");
        String userId = (String)map.get("userId");
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

            // WHERE conditions (4 placeholders)
            // 4: host_id
            statement.setObject(4, UUID.fromString(hostId));
            // 5: role_id
            statement.setString(5, roleId);
            // 6: user_id
            statement.setObject(6, UUID.fromString(userId));
            // 7: aggregate_version < ? (new version for OCC/IDM check)
            statement.setLong(7, newAggregateVersion);

            int count = statement.executeUpdate();
            if (count == 0) {
                // If 0 rows updated, it means:
                // 1. The record was not found (already deleted or never existed).
                // 2. The OCC/IDM check failed (aggregate_version >= newAggregateVersion).
                // We IGNORE the failure and log a warning, as this is the desired idempotent/monotonic behavior.
                logger.warn("Soft delete skipped for hostId {} roleId {} userId {} aggregateVersion {}. Record not found or a newer/same version already exists.", hostId, roleId, userId, newAggregateVersion);
            }
        } catch (SQLException e) {
            logger.error("SQLException during deleteRoleUser for hostId {} roleId {} userId {} aggregateVersion {}: {}", hostId, roleId, userId, newAggregateVersion, e.getMessage(), e);
            throw e;
        } catch (Exception e) {
            logger.error("Exception during deleteRoleUser for hostId {} roleId {} userId {} aggregateVersion {}: {}", hostId, roleId, userId, newAggregateVersion, e.getMessage(), e);
            throw e;
        }
    }
    @Override
    public Result<String> queryRoleRowFilter(int offset, int limit, String filtersJson, String globalFilter, String sortingJson, boolean active, String hostId) {
        Result<String> result;
        final Map<String, String> columnMap = new HashMap<>(Map.of(
                "hostId", "r.host_id",
                "roleId", "r.role_id",
                "endpointId", "p.endpoint_id",
                "endpoint", "ae.endpoint",
                "apiVersionId", "av.api_version_id",
                "apiId", "av.api_id",
                "apiVersion", "av.api_version",
                "colName", "p.col_name",
                "operator", "p.operator",
                "colValue", "p.col_value"
        ));
        columnMap.put("updateUser", "p.update_user");
        columnMap.put("updateTs", "p.update_ts");
        columnMap.put("aggregateVersion", "p.aggregate_version");
        columnMap.put("active", "p.active");


        List<Map<String, Object>> filters = parseJsonList(filtersJson);
        List<Map<String, Object>> sorting = parseJsonList(sortingJson);

        String s =
                """
                SELECT COUNT(*) OVER () AS total, r.host_id, r.role_id, p.endpoint_id, ae.endpoint,
                av.api_version_id, av.api_id, av.api_version, p.col_name, p.operator, p.col_value,
                p.update_user, p.update_ts, p.aggregate_version, p.active
                FROM role_row_filter_t p
                JOIN role_t r ON r.host_id = p.host_id AND r.role_id = p.role_id
                JOIN api_endpoint_t ae ON ae.host_id = p.host_id AND ae.endpoint_id = p.endpoint_id
                JOIN api_version_t av ON av.host_id = ae.host_id AND av.api_version_id = ae.api_version_id
                WHERE p.host_id = ?
                """;

        List<Object> parameters = new ArrayList<>();
        parameters.add(UUID.fromString(hostId));

        String activeClause = SqlUtil.buildMultiTableActiveClause(active, "p", "r", "ae", "av");
        String[] searchColumns = {"r.role_id", "ae.endpoint", "p.col_name", "p.col_value"};
        String sqlBuilder = s + activeClause +
                dynamicFilter(Arrays.asList("r.host_id", "p.endpoint_id", "av.api_version_id"), Arrays.asList(searchColumns), filters, columnMap, parameters) +
                globalFilter(globalFilter, searchColumns, parameters) +
                dynamicSorting("p.role_id, av.api_id, av.api_version, ae.endpoint, p.col_name", sorting, columnMap) +
                "\nLIMIT ? OFFSET ?";

        parameters.add(limit);
        parameters.add(offset);


        if(logger.isTraceEnabled()) logger.trace("queryRoleRowFilter sql: {}", sqlBuilder);
        int total = 0;
        List<Map<String, Object>> roleRowFilters = new ArrayList<>();

        try (final Connection conn = ds.getConnection(); PreparedStatement preparedStatement = conn.prepareStatement(sqlBuilder)) {
            populateParameters(preparedStatement, parameters);
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
                    map.put("endpointId", resultSet.getObject("endpoint_id", UUID.class));
                    map.put("endpoint", resultSet.getString("endpoint"));
                    map.put("updateUser", resultSet.getString("update_user"));
                    map.put("updateTs", resultSet.getObject("update_ts") != null ? resultSet.getObject("update_ts", OffsetDateTime.class) : null);
                    map.put("aggregateVersion", resultSet.getLong("aggregate_version"));
                    map.put("apiVersionId", resultSet.getString("api_version_id"));
                    map.put("apiId", resultSet.getString("api_id"));
                    map.put("apiVersion", resultSet.getString("api_version"));
                    map.put("colName", resultSet.getString("col_name"));
                    map.put("operator", resultSet.getString("operator"));
                    map.put("colValue", resultSet.getString("col_value"));
                    map.put("active", resultSet.getBoolean("active"));
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
    public Result<String> getRoleRowFilterById(String hostId, String roleId, String endpointId, String colName) {
        final String sql =
                """
                SELECT host_id, role_id, endpoint_id, col_name, operator, col_value,
                aggregate_version, active, update_user, update_ts
                FROM role_row_filter_t
                WHERE host_id = ? AND role_id = ? AND endpoint_id = ? AND col_name = ?
                """;
        Result<String> result;
        Map<String, Object> map = new HashMap<>();

        String searchId = hostId + ":" + roleId + ":" + endpointId + ":" + colName;

        try (Connection conn = ds.getConnection();
             PreparedStatement statement = conn.prepareStatement(sql)) {

            statement.setObject(1, UUID.fromString(hostId));
            statement.setString(2, roleId);
            statement.setObject(3, UUID.fromString(endpointId));
            statement.setString(4, colName);

            try (ResultSet resultSet = statement.executeQuery()) {
                if (resultSet.next()) {
                    map.put("hostId", resultSet.getObject("host_id", UUID.class));
                    map.put("roleId", resultSet.getString("role_id"));
                    map.put("endpointId", resultSet.getObject("endpoint_id", UUID.class));
                    map.put("colName", resultSet.getString("col_name"));
                    map.put("operator", resultSet.getString("operator"));
                    map.put("colValue", resultSet.getString("col_value"));
                    map.put("aggregateVersion", resultSet.getLong("aggregate_version"));
                    map.put("active", resultSet.getBoolean("active"));
                    map.put("updateUser", resultSet.getString("update_user"));
                    map.put("updateTs", resultSet.getObject("update_ts") != null ? resultSet.getObject("update_ts", OffsetDateTime.class) : null);

                    result = Success.of(JsonMapper.toJson(map));
                } else {
                    result = Failure.of(new Status(OBJECT_NOT_FOUND, "role_row_filter", searchId));
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
    public void deleteRoleRowFilter(Connection conn, Map<String, Object> event) throws SQLException, Exception {
        // Use UPDATE to implement Soft Delete (setting active = FALSE).
        // OCC/IDM is enforced by checking aggregate_version < newAggregateVersion.
        final String sql =
                """
                UPDATE role_row_filter_t
                SET active = FALSE,
                    update_user = ?,
                    update_ts = ?,
                    aggregate_version = ?
                WHERE host_id = ?
                  AND role_id = ?
                  AND endpoint_id = ?
                  AND col_name = ?
                  AND aggregate_version < ?
                """; // <<< CRITICAL: Added aggregate_version < ? to enforce monotonicity (OCC/IDM)

        Map<String, Object> map = SqlUtil.extractEventData(event);
        String hostId = (String)map.get("hostId");
        String roleId = (String)map.get("roleId");
        String endpointId = (String)map.get("endpointId");
        String colName = (String)map.get("colName");
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

            // WHERE conditions (5 placeholders)
            // 4: host_id
            statement.setObject(4, UUID.fromString(hostId));
            // 5: role_id
            statement.setString(5, roleId);
            // 6: endpoint_id
            statement.setObject(6, UUID.fromString(endpointId));
            // 7: col_name
            statement.setString(7, colName);
            // 8: aggregate_version < ? (new version for OCC/IDM check)
            statement.setLong(8, newAggregateVersion);

            int count = statement.executeUpdate();
            if (count == 0) {
                // If 0 rows updated, it means:
                // 1. The record was not found (already deleted or never existed).
                // 2. The OCC/IDM check failed (aggregate_version >= newAggregateVersion).
                // We IGNORE the failure and log a warning, as this is the desired idempotent/monotonic behavior.
                logger.warn("Soft delete skipped for hostId {} roleId {} endpointId {} colName {} aggregateVersion {}. Record not found or a newer/same version already exists.", hostId, roleId, endpointId, colName, newAggregateVersion);
            }
        } catch (SQLException e) {
            logger.error("SQLException during deleteRoleRowFilter for hostId {} roleId {} endpointId {} colName {} aggregateVersion {}: {}", hostId, roleId, endpointId, colName, newAggregateVersion, e.getMessage(), e);
            throw e;
        } catch (Exception e) {
            logger.error("Exception during deleteRoleRowFilter for hostId {} roleId {} endpointId {} colName {} aggregateVersion {}: {}", hostId, roleId, endpointId, colName, newAggregateVersion, e.getMessage(), e);
            throw e;
        }
    }

    @Override
    public void createRoleRowFilter(Connection conn, Map<String, Object> event) throws SQLException, Exception {
        // Use UPSERT: INSERT ON CONFLICT DO UPDATE
        // This handles:
        // 1. First time insert (no conflict).
        // 2. Re-creation (conflict on host_id, role_id, endpoint_id, col_name) -> UPDATE the existing soft-deleted row (setting active=TRUE and new version).

        final String sql =
                """
                INSERT INTO role_row_filter_t (
                    host_id,
                    role_id,
                    endpoint_id,
                    col_name,
                    operator,
                    col_value,
                    update_user,
                    update_ts,
                    aggregate_version,
                    active
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, TRUE)
                ON CONFLICT (host_id, role_id, endpoint_id, col_name) DO UPDATE
                SET operator = EXCLUDED.operator,
                    col_value = EXCLUDED.col_value,
                    update_user = EXCLUDED.update_user,
                    update_ts = EXCLUDED.update_ts,
                    aggregate_version = EXCLUDED.aggregate_version,
                    active = TRUE
                -- OCC/IDM: Only update if the incoming event is newer
                WHERE role_row_filter_t.aggregate_version < EXCLUDED.aggregate_version AND role_row_filter_t.active = FALSE
                """;

        Map<String, Object> map = SqlUtil.extractEventData(event);
        String hostId = (String)map.get("hostId");
        String roleId = (String)map.get("roleId");
        String endpointId = (String)map.get("endpointId");
        String colName = (String)map.get("colName");

        long newAggregateVersion = SqlUtil.getNewAggregateVersion(event);
        try (PreparedStatement statement = conn.prepareStatement(sql)) {
            // INSERT values (9 placeholders + active=TRUE in SQL, total 9 dynamic values)
            // 1: host_id
            statement.setObject(1, UUID.fromString(hostId));
            // 2: role_id
            statement.setString(2, roleId);
            // 3: endpoint_id
            statement.setObject(3, UUID.fromString(endpointId));
            // 4: col_name
            statement.setString(4, colName);
            // 5: operator
            statement.setString(5, (String)map.get("operator"));
            // 6: col_value
            statement.setString(6, (String)map.get("colValue"));
            // 7: update_user
            statement.setString(7, (String)event.get(Constants.USER));
            // 8: update_ts
            statement.setObject(8, OffsetDateTime.parse((String)event.get(CloudEventV1.TIME)));
            // 9: aggregate_version
            statement.setLong(9, newAggregateVersion);

            int count = statement.executeUpdate();
            if (count == 0) {
                // count=0 means the ON CONFLICT clause was hit, BUT the WHERE clause (aggregate_version < EXCLUDED.aggregate_version) failed.
                // This is the desired idempotent/out-of-order protection behavior. Log and ignore.
                logger.warn("Creation/Reactivation skipped for hostId {} roleId {} endpointId {} colName {} aggregateVersion {}. A newer or same version already exists.", hostId, roleId, endpointId, colName, newAggregateVersion);
            }
        } catch (SQLException e) {
            logger.error("SQLException during createRoleRowFilter for hostId {} roleId {} endpointId {} colName {} aggregateVersion {}: {}", hostId, roleId, endpointId, colName, newAggregateVersion, e.getMessage(), e);
            throw e;
        } catch (Exception e) {
            logger.error("Exception during createRoleRowFilter for hostId {} roleId {} endpointId {} colName {} aggregateVersion {}: {}", hostId, roleId, endpointId, colName, newAggregateVersion, e.getMessage(), e);
            throw e;
        }
    }


    @Override
    public void updateRoleRowFilter(Connection conn, Map<String, Object> event) throws SQLException, Exception {
        // We attempt to update the record IF the incoming event's aggregate_version is greater than the current projection's version.
        // This enforces Idempotence (IDM) and Optimistic Concurrency Control (OCC) by ensuring version monotonicity.
        // We explicitly set active = TRUE as an UPDATE event implies the row filter should be active.
        final String sql =
                """
                UPDATE role_row_filter_t
                SET
                    operator = ?,
                    col_value = ?,
                    update_user = ?,
                    update_ts = ?,
                    aggregate_version = ?,
                    active = TRUE
                WHERE
                    host_id = ?
                    AND role_id = ?
                    AND endpoint_id = ? -- Fixed syntax from original: AND endpoint_id ?
                    AND col_name = ?
                    AND aggregate_version < ?
                """; // <<< CRITICAL: Changed aggregate_version = ? to aggregate_version < ? to enforce monotonicity (OCC/IDM)

        // Note: The original code uses a non-standard map retrieval: Map<String, Object> map = (Map<String, Object>)event.get(PortalConstants.DATA);
        // Assuming SqlUtil.extractEventData(event) is the correct utility based on other methods.
        Map<String, Object> map = SqlUtil.extractEventData(event);

        String hostId = (String)map.get("hostId");
        String roleId = (String)map.get("roleId");
        String endpointId = (String)map.get("endpointId");
        String colName = (String)map.get("colName");
        long newAggregateVersion = SqlUtil.getNewAggregateVersion(event);

        try (PreparedStatement statement = conn.prepareStatement(sql)) {
            // SET values (5 dynamic values + active = TRUE in SQL)
            // 1: operator
            statement.setString(1, (String)map.get("operator"));
            // 2: col_value
            statement.setString(2, (String)map.get("colValue"));
            // 3: update_user
            statement.setString(3, (String)event.get(Constants.USER));
            // 4: update_ts
            statement.setObject(4, OffsetDateTime.parse((String)event.get(CloudEventV1.TIME)));
            // 5: aggregate_version
            statement.setLong(5, newAggregateVersion);

            // WHERE conditions (5 placeholders)
            // 6: host_id
            statement.setObject(6, UUID.fromString(hostId));
            // 7: role_id
            statement.setString(7, roleId);
            // 8: endpoint_id
            statement.setObject(8, UUID.fromString(endpointId));
            // 9: col_name
            statement.setString(9, colName);
            // 10: aggregate_version < ? (new version for OCC/IDM check)
            statement.setLong(10, newAggregateVersion);

            int count = statement.executeUpdate();
            if (count == 0) {
                // If 0 rows updated, it means the record was either not found
                // OR aggregate_version >= newAggregateVersion (OCC/IDM check failed).
                // We IGNORE the failure and log a warning, as this is the desired idempotent/monotonic behavior.
                logger.warn("Update skipped for hostId {} roleId {} endpointId {} colName {} aggregateVersion {}. Record not found or a newer/same version already exists.", hostId, roleId, endpointId, colName, newAggregateVersion);
            }
        } catch (SQLException e) {
            logger.error("SQLException during updateRoleRowFilter for hostId {} roleId {} endpointId {} colName {} aggregateVersion {}: {}", hostId, roleId, endpointId, colName, newAggregateVersion, e.getMessage(), e);
            throw e;
        } catch (Exception e) {
            logger.error("Exception during updateRoleRowFilter for hostId {} roleId {} endpointId {} colName {} aggregateVersion {}: {}", hostId, roleId, endpointId, colName, newAggregateVersion, e.getMessage(), e);
            throw e;
        }
    }


    @Override
    public Result<String> queryRoleColFilter(int offset, int limit, String filtersJson, String globalFilter, String sortingJson, boolean active, String hostId) {
        Result<String> result;
        final Map<String, String> columnMap = new HashMap<>(Map.of(
                "hostId", "r.host_id",
                "roleId", "r.role_id",
                "endpointId", "ae.endpoint_id",
                "endpoint", "ae.endpoint",
                "apiVersionId", "av.api_version_id",
                "apiId", "av.api_id",
                "apiVersion", "av.api_version",
                "columns", "rcf.columns",
                "updateUser", "rcf.update_user",
                "updateTs", "rcf.update_ts"
        ));
        columnMap.put("aggregateVersion", "rcf.aggregate_version");
        columnMap.put("active", "rcf.active");


        List<Map<String, Object>> filters = parseJsonList(filtersJson);
        List<Map<String, Object>> sorting = parseJsonList(sortingJson);

        String s =
                """
                SELECT COUNT(*) OVER () AS total,
                r.host_id, r.role_id, av.api_version_id, av.api_id, av.api_version,
                ae.endpoint_id, ae.endpoint, rcf.columns, rcf.aggregate_version,
                rcf.update_user, rcf.update_ts, rcf.active
                FROM role_col_filter_t rcf
                JOIN role_t r ON r.role_id = rcf.role_id
                JOIN api_endpoint_t ae ON rcf.host_id = ae.host_id AND rcf.endpoint_id = ae.endpoint_id
                JOIN api_version_t av ON ae.host_id = av.host_id AND ae.api_version_id = av.api_version_id
                AND r.host_id = ?
                """;

        List<Object> parameters = new ArrayList<>();
        parameters.add(UUID.fromString(hostId));

        String activeClause = SqlUtil.buildMultiTableActiveClause(active, "rcf", "r", "ae", "av");
        String[] searchColumns = {"r.role_id", "ae.endpoint", "rcf.columns"};
        String sqlBuilder = s + activeClause +
                dynamicFilter(Arrays.asList("r.host_id", "av.api_version_id", "ae.endpoint_id"), Arrays.asList(searchColumns), filters, columnMap, parameters) +
                globalFilter(globalFilter, searchColumns, parameters) +
                dynamicSorting("r.role_id, av.api_id, av.api_version, ae.endpoint", sorting, columnMap) +
                "\nLIMIT ? OFFSET ?";

        parameters.add(limit);
        parameters.add(offset);

        if(logger.isTraceEnabled()) logger.trace("queryRoleColFilter sql: {}", sqlBuilder);
        int total = 0;
        List<Map<String, Object>> roleColFilters = new ArrayList<>();

        try (final Connection conn = ds.getConnection(); PreparedStatement preparedStatement = conn.prepareStatement(sqlBuilder)) {
            populateParameters(preparedStatement, parameters);
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
                    map.put("endpointId", resultSet.getObject("endpoint_id", UUID.class));
                    map.put("updateUser", resultSet.getString("update_user"));
                    map.put("updateTs", resultSet.getObject("update_ts") != null ? resultSet.getObject("update_ts", OffsetDateTime.class) : null);
                    map.put("aggregateVersion", resultSet.getLong("aggregate_version"));
                    map.put("apiVersionId", resultSet.getString("api_version_id"));
                    map.put("apiId", resultSet.getString("api_id"));
                    map.put("apiVersion", resultSet.getString("api_version"));
                    map.put("endpoint", resultSet.getString("endpoint"));
                    map.put("columns", resultSet.getString("columns"));
                    map.put("active", resultSet.getBoolean("active"));
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
    public Result<String> getRoleColFilterById(String hostId, String roleId, String endpointId) {
        final String sql =
                """
                SELECT host_id, role_id, endpoint_id, columns,
                aggregate_version, active, update_user, update_ts
                FROM role_col_filter_t
                WHERE host_id = ? AND role_id = ? AND endpoint_id = ?
                """;
        Result<String> result;
        Map<String, Object> map = new HashMap<>();

        String searchId = hostId + ":" + roleId + ":" + endpointId;

        try (Connection conn = ds.getConnection();
             PreparedStatement statement = conn.prepareStatement(sql)) {

            statement.setObject(1, UUID.fromString(hostId));
            statement.setString(2, roleId);
            statement.setObject(3, UUID.fromString(endpointId));

            try (ResultSet resultSet = statement.executeQuery()) {
                if (resultSet.next()) {
                    map.put("hostId", resultSet.getObject("host_id", UUID.class));
                    map.put("roleId", resultSet.getString("role_id"));
                    map.put("endpointId", resultSet.getObject("endpoint_id", UUID.class));
                    map.put("columns", resultSet.getString("columns"));
                    map.put("aggregateVersion", resultSet.getLong("aggregate_version"));
                    map.put("active", resultSet.getBoolean("active"));
                    map.put("updateUser", resultSet.getString("update_user"));
                    map.put("updateTs", resultSet.getObject("update_ts") != null ? resultSet.getObject("update_ts", OffsetDateTime.class) : null);

                    result = Success.of(JsonMapper.toJson(map));
                } else {
                    result = Failure.of(new Status(OBJECT_NOT_FOUND, "role_col_filter", searchId));
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
    public void createRoleColFilter(Connection conn, Map<String, Object> event) throws SQLException, Exception {
        // Use UPSERT: INSERT ON CONFLICT DO UPDATE
        // This handles:
        // 1. First time insert (no conflict).
        // 2. Re-creation (conflict on host_id, role_id, endpoint_id) -> UPDATE the existing soft-deleted row (setting active=TRUE and new version).

        final String sql =
                """
                INSERT INTO role_col_filter_t (
                    host_id,
                    role_id,
                    endpoint_id,
                    columns,
                    update_user,
                    update_ts,
                    aggregate_version,
                    active
                ) VALUES (?, ?, ?, ?, ?, ?, ?, TRUE)
                ON CONFLICT (host_id, role_id, endpoint_id) DO UPDATE
                SET columns = EXCLUDED.columns,
                    update_user = EXCLUDED.update_user,
                    update_ts = EXCLUDED.update_ts,
                    aggregate_version = EXCLUDED.aggregate_version,
                    active = TRUE
                -- OCC/IDM: Only update if the incoming event is newer
                WHERE role_col_filter_t.aggregate_version < EXCLUDED.aggregate_version AND role_col_filter_t.active = FALSE
                """;

        // Note: The original code uses a non-standard map retrieval: Map<String, Object> map = (Map<String, Object>)event.get(PortalConstants.DATA);
        // Assuming SqlUtil.extractEventData(event) is the correct utility based on other methods.
        Map<String, Object> map = SqlUtil.extractEventData(event);

        String hostId = (String)map.get("hostId");
        String roleId = (String)map.get("roleId");
        String endpointId = (String)map.get("endpointId");

        long newAggregateVersion = SqlUtil.getNewAggregateVersion(event);
        try (PreparedStatement statement = conn.prepareStatement(sql)) {
            // INSERT values (7 placeholders + active=TRUE in SQL, total 7 dynamic values)
            // 1: host_id
            statement.setObject(1, UUID.fromString(hostId));
            // 2: role_id
            statement.setString(2, roleId);
            // 3: endpoint_id
            statement.setObject(3, UUID.fromString(endpointId));
            // 4: columns
            statement.setString(4, (String)map.get("columns"));
            // 5: update_user
            statement.setString(5, (String)event.get(Constants.USER));
            // 6: update_ts
            statement.setObject(6, OffsetDateTime.parse((String)event.get(CloudEventV1.TIME)));
            // 7: aggregate_version
            statement.setLong(7, newAggregateVersion);

            int count = statement.executeUpdate();
            if (count == 0) {
                // count=0 means the ON CONFLICT clause was hit, BUT the WHERE clause (aggregate_version < EXCLUDED.aggregate_version) failed.
                // This is the desired idempotent/out-of-order protection behavior. Log and ignore.
                logger.warn("Creation/Reactivation skipped for hostId {} roleId {} endpointId {} aggregateVersion {}. A newer or same version already exists.", hostId, roleId, endpointId, newAggregateVersion);
            }
        } catch (SQLException e) {
            logger.error("SQLException during createRoleColFilter for hostId {} roleId {} endpointId {} aggregateVersion {}: {}", hostId, roleId, endpointId, newAggregateVersion, e.getMessage(), e);
            throw e;
        } catch (Exception e) {
            logger.error("Exception during createRoleColFilter for hostId {} roleId {} endpointId {} aggregateVersion {}: {}", hostId, roleId, endpointId, newAggregateVersion, e.getMessage(), e);
            throw e;
        }
    }

    @Override
    public void deleteRoleColFilter(Connection conn, Map<String, Object> event) throws SQLException, Exception {
        // Use UPDATE to implement Soft Delete (setting active = FALSE).
        // OCC/IDM is enforced by checking aggregate_version < newAggregateVersion.
        final String sql =
                """
                UPDATE role_col_filter_t
                SET active = FALSE,
                    update_user = ?,
                    update_ts = ?,
                    aggregate_version = ?
                WHERE host_id = ?
                  AND role_id = ?
                  AND endpoint_id = ?
                  AND aggregate_version < ?
                """; // <<< CRITICAL: Added aggregate_version < ? to enforce monotonicity (OCC/IDM)

        // Note: The original code uses a non-standard map retrieval: Map<String, Object> map = (Map<String, Object>)event.get(PortalConstants.DATA);
        // Assuming SqlUtil.extractEventData(event) is the correct utility based on other methods.
        Map<String, Object> map = SqlUtil.extractEventData(event);

        String hostId = (String)map.get("hostId");
        String roleId = (String)map.get("roleId");
        String endpointId = (String)map.get("endpointId");
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

            // WHERE conditions (4 placeholders)
            // 4: host_id
            statement.setObject(4, UUID.fromString(hostId));
            // 5: role_id
            statement.setString(5, roleId);
            // 6: endpoint_id
            statement.setObject(6, UUID.fromString(endpointId));
            // 7: aggregate_version < ? (new version for OCC/IDM check)
            statement.setLong(7, newAggregateVersion);

            int count = statement.executeUpdate();
            if (count == 0) {
                // If 0 rows updated, it means:
                // 1. The record was not found (already deleted or never existed).
                // 2. The OCC/IDM check failed (aggregate_version >= newAggregateVersion).
                // We IGNORE the failure and log a warning, as this is the desired idempotent/monotonic behavior.
                logger.warn("Soft delete skipped for hostId {} roleId {} endpointId {} aggregateVersion {}. Record not found or a newer/same version already exists.", hostId, roleId, endpointId, newAggregateVersion);
            }
        } catch (SQLException e) {
            logger.error("SQLException during deleteRoleColFilter for hostId {} roleId {} endpointId {} aggregateVersion {}: {}", hostId, roleId, endpointId, newAggregateVersion, e.getMessage(), e);
            throw e;
        } catch (Exception e) {
            logger.error("Exception during deleteRoleColFilter for hostId {} roleId {} endpointId {} aggregateVersion {}: {}", hostId, roleId, endpointId, newAggregateVersion, e.getMessage(), e);
            throw e;
        }
    }

    @Override
    public void updateRoleColFilter(Connection conn, Map<String, Object> event) throws SQLException, Exception {
        // We attempt to update the record IF the incoming event's aggregate_version is greater than the current projection's version.
        // This enforces Idempotence (IDM) and Optimistic Concurrency Control (OCC) by ensuring version monotonicity.
        // We explicitly set active = TRUE as an UPDATE event implies the column filter should be active.
        final String sql =
                """
                UPDATE role_col_filter_t
                SET
                    columns = ?,
                    update_user = ?,
                    update_ts = ?, -- Added missing comma from original SQL
                    aggregate_version = ?,
                    active = TRUE
                WHERE
                    host_id = ?
                    AND role_id = ?
                    AND endpoint_id = ?
                    AND aggregate_version < ?
                """; // <<< CRITICAL: Changed aggregate_version = ? to aggregate_version < ? to enforce monotonicity (OCC/IDM)

        // Note: The original code uses a non-standard map retrieval: Map<String, Object> map = (Map<String, Object>)event.get(PortalConstants.DATA);
        // Assuming SqlUtil.extractEventData(event) is the correct utility based on other methods.
        Map<String, Object> map = SqlUtil.extractEventData(event);

        String hostId = (String)map.get("hostId");
        String roleId = (String)map.get("roleId");
        String endpointId = (String)map.get("endpointId");
        long newAggregateVersion = SqlUtil.getNewAggregateVersion(event);

        try (PreparedStatement statement = conn.prepareStatement(sql)) {
            // SET values (4 dynamic values + active = TRUE in SQL)
            // 1: columns
            statement.setString(1, (String)map.get("columns"));
            // 2: update_user
            statement.setString(2, (String)event.get(Constants.USER));
            // 3: update_ts
            statement.setObject(3, OffsetDateTime.parse((String)event.get(CloudEventV1.TIME)));
            // 4: aggregate_version
            statement.setLong(4, newAggregateVersion);

            // WHERE conditions (4 placeholders)
            // 5: host_id
            statement.setObject(5, UUID.fromString(hostId));
            // 6: role_id
            statement.setString(6, roleId);
            // 7: endpoint_id
            statement.setObject(7, UUID.fromString(endpointId));
            // 8: aggregate_version < ? (new version for OCC/IDM check)
            statement.setLong(8, newAggregateVersion);

            int count = statement.executeUpdate();
            if (count == 0) {
                // If 0 rows updated, it means the record was either not found
                // OR aggregate_version >= newAggregateVersion (OCC/IDM check failed).
                // We IGNORE the failure and log a warning, as this is the desired idempotent/monotonic behavior.
                logger.warn("Update skipped for hostId {} roleId {} endpointId {} aggregateVersion {}. Record not found or a newer/same version already exists.", hostId, roleId, endpointId, newAggregateVersion);
            }
        } catch (SQLException e) {
            logger.error("SQLException during updateRoleColFilter for hostId {} roleId {} endpointId {} aggregateVersion {}: {}", hostId, roleId, endpointId, newAggregateVersion, e.getMessage(), e);
            throw e;
        } catch (Exception e) {
            logger.error("Exception during updateRoleColFilter for hostId {} roleId {} endpointId {} aggregateVersion {}: {}", hostId, roleId, endpointId, newAggregateVersion, e.getMessage(), e);
            throw e;
        }
    }

    @Override
    public void createGroup(Connection conn, Map<String, Object> event) throws SQLException, Exception {
        // Use UPSERT: INSERT ON CONFLICT DO UPDATE
        // This handles:
        // 1. First time insert (no conflict).
        // 2. Re-creation (conflict on host_id, group_id) -> UPDATE the existing soft-deleted row (setting active=TRUE and new version).

        final String sql =
                """
                INSERT INTO group_t(host_id, group_id, group_desc, update_user, update_ts, aggregate_version, active)
                VALUES (?, ?, ?, ?, ?, ?, TRUE)
                ON CONFLICT (host_id, group_id) DO UPDATE
                SET group_desc = EXCLUDED.group_desc,
                    update_user = EXCLUDED.update_user,
                    update_ts = EXCLUDED.update_ts,
                    aggregate_version = EXCLUDED.aggregate_version,
                    active = TRUE
                -- OCC/IDM: Only update if the incoming event is newer
                WHERE group_t.aggregate_version < EXCLUDED.aggregate_version AND group_t.active = FALSE
                """;

        // Note: The original code uses a non-standard map retrieval: Map<String, Object> map = (Map<String, Object>)event.get(PortalConstants.DATA);
        // Assuming SqlUtil.extractEventData(event) is the correct utility based on other methods.
        Map<String, Object> map = SqlUtil.extractEventData(event);

        String hostId = (String)map.get("hostId");
        String groupId = (String)map.get("groupId");
        long newAggregateVersion = SqlUtil.getNewAggregateVersion(event);

        try (PreparedStatement statement = conn.prepareStatement(sql)) {
            // INSERT values (6 placeholders + active=TRUE in SQL, total 6 dynamic values)
            // 1: host_id
            statement.setObject(1, UUID.fromString(hostId));
            // 2: group_id
            statement.setString(2, groupId);

            // 3: group_desc
            String groupDesc = (String)map.get("groupDesc");
            if (groupDesc != null && !groupDesc.isEmpty())
                statement.setString(3, groupDesc);
            else
                statement.setNull(3, Types.VARCHAR); // Use Types.VARCHAR

            // 4: update_user
            statement.setString(4, (String)event.get(Constants.USER));
            // 5: update_ts
            statement.setObject(5, OffsetDateTime.parse((String)event.get(CloudEventV1.TIME)));
            // 6: aggregate_version
            statement.setLong(6, newAggregateVersion);

            int count = statement.executeUpdate();
            if (count == 0) {
                // count=0 means the ON CONFLICT clause was hit, BUT the WHERE clause (aggregate_version < EXCLUDED.aggregate_version) failed.
                // This is the desired idempotent/out-of-order protection behavior. Log and ignore.
                logger.warn("Creation/Reactivation skipped for hostId {} groupId {} aggregateVersion {}. A newer or same version already exists.", hostId, groupId, newAggregateVersion);
            }
        } catch (SQLException e) {
            logger.error("SQLException during createGroup for hostId {} groupId {} aggregateVersion {}: {}", hostId, groupId, newAggregateVersion, e.getMessage(), e);
            throw e;
        } catch (Exception e) {
            logger.error("Exception during createGroup for hostId {} groupId {} aggregateVersion {}: {}", hostId, groupId, newAggregateVersion, e.getMessage(), e);
            throw e;
        }
    }

    @Override
    public void updateGroup(Connection conn, Map<String, Object> event) throws SQLException, Exception {
        // We attempt to update the record IF the incoming event's aggregate_version is greater than the current projection's version.
        // This enforces Idempotence (IDM) and Optimistic Concurrency Control (OCC) by ensuring version monotonicity.
        // We explicitly set active = TRUE as an UPDATE event implies the group should be active.
        final String sql =
                """
                UPDATE group_t
                SET group_desc = ?,
                    update_user = ?,
                    update_ts = ?,
                    aggregate_version = ?,
                    active = TRUE
                WHERE host_id = ?
                  AND group_id = ?
                  AND aggregate_version < ?
                """; // <<< CRITICAL: Changed aggregate_version = ? to aggregate_version < ? to enforce monotonicity (OCC/IDM)

        // Note: The original code uses a non-standard map retrieval: Map<String, Object> map = (Map<String, Object>)event.get(PortalConstants.DATA);
        // Assuming SqlUtil.extractEventData(event) is the correct utility based on other methods.
        Map<String, Object> map = SqlUtil.extractEventData(event);

        String hostId = (String)map.get("hostId");
        String groupId = (String)map.get("groupId");
        long newAggregateVersion = SqlUtil.getNewAggregateVersion(event);

        try (PreparedStatement statement = conn.prepareStatement(sql)) {
            // SET values (4 dynamic values + active = TRUE in SQL)
            // 1: group_desc
            String groupDesc = (String)map.get("groupDesc");
            if(groupDesc != null && !groupDesc.isEmpty()) {
                statement.setString(1, groupDesc);
            } else {
                statement.setNull(1, Types.VARCHAR); // Use Types.VARCHAR
            }
            // 2: update_user
            statement.setString(2, (String)event.get(Constants.USER));
            // 3: update_ts
            statement.setObject(3, OffsetDateTime.parse((String)event.get(CloudEventV1.TIME)));
            // 4: aggregate_version
            statement.setLong(4, newAggregateVersion);

            // WHERE conditions (3 placeholders)
            // 5: host_id (Corrected parameter 5 to use the hostId variable from map, which is typical)
            statement.setObject(5, UUID.fromString(hostId));
            // 6: group_id
            statement.setString(6, groupId);
            // 7: aggregate_version < ? (new version for OCC/IDM check)
            statement.setLong(7, newAggregateVersion);

            int count = statement.executeUpdate();
            if (count == 0) {
                // If 0 rows updated, it means the record was either not found
                // OR aggregate_version >= newAggregateVersion (OCC/IDM check failed).
                // We IGNORE the failure and log a warning, as this is the desired idempotent/monotonic behavior.
                logger.warn("Update skipped for hostId {} groupId {} aggregateVersion {}. Record not found or a newer/same version already exists.", hostId, groupId, newAggregateVersion);
            }
        } catch (SQLException e) {
            logger.error("SQLException during updateGroup for hostId {} groupId {} aggregateVersion {}: {}", hostId, groupId, newAggregateVersion, e.getMessage(), e);
            throw e;
        } catch (Exception e) {
            logger.error("Exception during updateGroup for hostId {} groupId {} aggregateVersion {}: {}", hostId, groupId, newAggregateVersion, e.getMessage(), e);
            throw e;
        }
    }

    @Override
    public void deleteGroup(Connection conn, Map<String, Object> event) throws SQLException, Exception {
        // Use UPDATE to implement Soft Delete (setting active = FALSE).
        // OCC/IDM is enforced by checking aggregate_version < newAggregateVersion.
        final String sql =
                """
                UPDATE group_t
                SET active = FALSE,
                    update_user = ?,
                    update_ts = ?,
                    aggregate_version = ?
                WHERE host_id = ?
                  AND group_id = ?
                  AND aggregate_version < ?
                """; // <<< CRITICAL: Added aggregate_version < ? to enforce monotonicity (OCC/IDM)

        // Note: The original code uses a non-standard map retrieval: Map<String, Object> map = (Map<String, Object>)event.get(PortalConstants.DATA);
        // Assuming SqlUtil.extractEventData(event) is the correct utility based on other methods.
        Map<String, Object> map = SqlUtil.extractEventData(event);

        String hostId = (String)map.get("hostId");
        String groupId = (String)map.get("groupId");
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

            // WHERE conditions (3 placeholders)
            // 4: host_id
            statement.setObject(4, UUID.fromString(hostId));
            // 5: group_id
            statement.setString(5, groupId);
            // 6: aggregate_version < ? (new version for OCC/IDM check)
            statement.setLong(6, newAggregateVersion);

            int count = statement.executeUpdate();
            if (count == 0) {
                // If 0 rows updated, it means:
                // 1. The record was not found (already deleted or never existed).
                // 2. The OCC/IDM check failed (aggregate_version >= newAggregateVersion).
                // We IGNORE the failure and log a warning, as this is the desired idempotent/monotonic behavior.
                logger.warn("Soft delete skipped for hostId {} groupId {} aggregateVersion {}. Record not found or a newer/same version already exists.", hostId, groupId, newAggregateVersion);
            }
        } catch (SQLException e) {
            logger.error("SQLException during deleteGroup for hostId {} groupId {} aggregateVersion {}: {}", hostId, groupId, newAggregateVersion, e.getMessage(), e);
            throw e;
        } catch (Exception e) {
            logger.error("Exception during deleteGroup for hostId {} groupId {} aggregateVersion {}: {}", hostId, groupId, newAggregateVersion, e.getMessage(), e);
            throw e;
        }
    }

    @Override
    public Result<String> queryGroup(int offset, int limit, String filtersJson, String globalFilter, String sortingJson, boolean active, String hostId) {
        Result<String> result;
        List<Map<String, Object>> filters = parseJsonList(filtersJson);
        List<Map<String, Object>> sorting = parseJsonList(sortingJson);
        String s =
            """
            SELECT COUNT(*) OVER () AS total,
            host_id, group_id, group_desc, update_user,
            update_ts, aggregate_version, active
            FROM group_t
            WHERE host_id = ?
            """;

        List<Object> parameters = new ArrayList<>();
        parameters.add(UUID.fromString(hostId));

        String activeClause = SqlUtil.buildMultiTableActiveClause(active);
        String[] searchColumns = {"group_id", "group_desc"};
        String sqlBuilder = s + activeClause +
                dynamicFilter(Arrays.asList("host_id"), Arrays.asList(searchColumns), filters, null, parameters) +
                globalFilter(globalFilter, searchColumns, parameters) +
                dynamicSorting("group_id", sorting, null) +
                "\nLIMIT ? OFFSET ?";

        parameters.add(limit);
        parameters.add(offset);

        int total = 0;
        List<Map<String, Object>> groups = new ArrayList<>();

        try (final Connection conn = ds.getConnection(); PreparedStatement preparedStatement = conn.prepareStatement(sqlBuilder)) {
            populateParameters(preparedStatement, parameters);
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
                    map.put("aggregateVersion", resultSet.getLong("aggregate_version"));
                    map.put("active", resultSet.getBoolean("active"));
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
    public Result<String> getGroupById(String hostId, String groupId) {
        final String sql =
                """
                SELECT host_id, group_id, group_desc, aggregate_version, active, update_user, update_ts
                FROM group_t
                WHERE host_id = ? AND group_id = ?
                """;
        Result<String> result;
        Map<String, Object> map = new HashMap<>();

        String searchId = hostId + ":" + groupId;

        try (Connection conn = ds.getConnection();
             PreparedStatement statement = conn.prepareStatement(sql)) {

            statement.setObject(1, UUID.fromString(hostId));
            statement.setString(2, groupId);

            try (ResultSet resultSet = statement.executeQuery()) {
                if (resultSet.next()) {
                    map.put("hostId", resultSet.getObject("host_id", UUID.class));
                    map.put("groupId", resultSet.getString("group_id"));
                    map.put("groupDesc", resultSet.getString("group_desc"));
                    map.put("aggregateVersion", resultSet.getLong("aggregate_version"));
                    map.put("active", resultSet.getBoolean("active"));
                    map.put("updateUser", resultSet.getString("update_user"));
                    map.put("updateTs", resultSet.getObject("update_ts") != null ? resultSet.getObject("update_ts", OffsetDateTime.class) : null);

                    result = Success.of(JsonMapper.toJson(map));
                } else {
                    result = Failure.of(new Status(OBJECT_NOT_FOUND, "group", searchId));
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
    public Result<String> queryGroupLabel(String hostId) {
        final String sql = "SELECT group_id from group_t WHERE host_id = ? AND active = TRUE";
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
    public Result<String> queryGroupPermission(int offset, int limit, String filtersJson, String globalFilter, String sortingJson, boolean active, String hostId) {
        Result<String> result;
        final Map<String, String> columnMap = new HashMap<>(Map.of(
                "hostId", "gp.host_id",
                "groupId", "gp.group_id",
                "apiId", "av.api_id",
                "apiVersion", "av.api_version",
                "apiVersionId", "av.api_version_id",
                "endpoint", "ae.endpoint",
                "endpointId", "ae.endpoint_id",
                "aggregateVersion", "gp.aggregate_version",
                "updateUser", "gp.update_user",
                "updateTs", "gp.update_ts"
        ));
        columnMap.put("active", "gp.active");

        List<Map<String, Object>> filters = parseJsonList(filtersJson);
        List<Map<String, Object>> sorting = parseJsonList(sortingJson);

        String s =
            """
                SELECT COUNT(*) OVER () AS total,
                gp.host_id, gp.group_id, av.api_id, av.api_version, av.api_version_id, ae.endpoint_id,
                ae.endpoint, gp.aggregate_version, gp.update_user, gp.update_ts, gp.active
                FROM group_permission_t gp
                JOIN group_t g ON gp.group_id = g.group_id
                JOIN api_endpoint_t ae ON gp.host_id = ae.host_id AND gp.endpoint_id = ae.endpoint_id
                JOIN api_version_t av ON ae.host_id = av.host_id AND ae.api_version_id = av.api_version_id
                WHERE gp.host_id = ?
            """;

        List<Object> parameters = new ArrayList<>();
        parameters.add(UUID.fromString(hostId));

        String activeClause = SqlUtil.buildMultiTableActiveClause(active, "gp", "g", "ae", "av");
        String[] searchColumns = {"gp.group_id", "ae.endpoint"};
        String sqlBuilder = s + activeClause +
                dynamicFilter(Arrays.asList("gp.host_id", "av.api_version_id", "ae.endpoint_id"), Arrays.asList(searchColumns), filters, columnMap, parameters) +
                globalFilter(globalFilter, searchColumns, parameters) +
                dynamicSorting("gp.group_id, av.api_id, av.api_version, ae.endpoint", sorting, columnMap) +
                "\nLIMIT ? OFFSET ?";

        parameters.add(limit);
        parameters.add(offset);


        if(logger.isTraceEnabled()) logger.trace("queryGroupPermission sql: {}", sqlBuilder);
        int total = 0;
        List<Map<String, Object>> groupPermissions = new ArrayList<>();


        try (final Connection conn = ds.getConnection(); PreparedStatement preparedStatement = conn.prepareStatement(sqlBuilder)) {
            populateParameters(preparedStatement, parameters);
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
                    map.put("apiVersionId", resultSet.getObject("api_version_id", UUID.class));
                    map.put("endpoint", resultSet.getString("endpoint"));
                    map.put("endpointId", resultSet.getObject("endpoint_id", UUID.class));
                    map.put("aggregateVersion", resultSet.getLong("aggregate_version"));
                    map.put("updateUser", resultSet.getString("update_user"));
                    map.put("updateTs", resultSet.getObject("update_ts") != null ? resultSet.getObject("update_ts", OffsetDateTime.class) : null);
                    map.put("active", resultSet.getBoolean("active"));
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
    public Result<String> queryGroupUser(int offset, int limit, String filtersJson, String globalFilter, String sortingJson, boolean active, String hostId) {
        Result<String> result;

        final Map<String, String> columnMap = new HashMap<>(Map.of(
                "hostId", "g.host_id",
                "groupId", "g.group_id",
                "startTs", "g.start_ts",
                "endTs", "g.end_ts",
                "userId", "u.user_id",
                "email", "u.email",
                "userType", "u.user_type",
                "updateUser", "g.update_user",
                "updateTs", "g.update_ts",
                "aggregateVersion", "g.aggregate_version"
        ));
        columnMap.put("entityId", "CASE WHEN u.user_type = 'C' THEN c.customer_id WHEN u.user_type = 'E' THEN e.employee_id ELSE NULL -- Handle other cases if needed END");
        columnMap.put("firstName", "u.first_name");
        columnMap.put("lastName", "u.last_name");
        columnMap.put("managerId", "e.manager_id");
        columnMap.put("active", "g.active");

        List<Map<String, Object>> filters = parseJsonList(filtersJson);
        List<Map<String, Object>> sorting = parseJsonList(sortingJson);

        String s =
            """
                SELECT COUNT(*) OVER () AS total,
                g.host_id, g.group_id, g.start_ts, g.end_ts,
                g.aggregate_version, g.update_user, g.update_ts,
                u.user_id, u.email, u.user_type, g.active,
                CASE
                    WHEN u.user_type = 'C' THEN c.customer_id
                    WHEN u.user_type = 'E' THEN e.employee_id
                    ELSE NULL -- Handle other cases if needed
                END AS entity_id,
                e.manager_id, u.first_name, u.last_name
                FROM user_t u
                LEFT JOIN
                    customer_t c ON u.user_id = c.user_id AND u.user_type = 'C'
                LEFT JOIN
                    employee_t e ON u.user_id = e.user_id AND u.user_type = 'E'
                INNER JOIN
                    group_user_t g ON g.user_id = u.user_id
                WHERE g.host_id = ?
            """;

        List<Object> parameters = new ArrayList<>();
        parameters.add(UUID.fromString(hostId));


        String activeClause = SqlUtil.buildMultiTableActiveClause(active, "u", "c", "e", "g");
        String[] searchColumns = {"g.group_id", "u.email", "u.first_name", "u.last_name"};
        String sqlBuilder = s + activeClause +
                dynamicFilter(Arrays.asList("g.host_id", "u.user_id"), Arrays.asList(searchColumns), filters, columnMap, parameters) +
                globalFilter(globalFilter, searchColumns, parameters) +
                dynamicSorting("g.group_id, u.user_id", sorting, columnMap) +
                "\nLIMIT ? OFFSET ?";

        parameters.add(limit);
        parameters.add(offset);

        if(logger.isTraceEnabled()) logger.trace("queryGroupUser sql: {}", sqlBuilder);
        int total = 0;
        List<Map<String, Object>> groupUsers = new ArrayList<>();


        try (final Connection conn = ds.getConnection(); PreparedStatement preparedStatement = conn.prepareStatement(sqlBuilder)) {
            populateParameters(preparedStatement, parameters);
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
                    map.put("startTs", resultSet.getObject("start_ts") != null ? resultSet.getObject("start_ts", OffsetDateTime.class) : null);
                    map.put("endTs", resultSet.getObject("end_ts") != null ? resultSet.getObject("end_ts", OffsetDateTime.class) : null);
                    map.put("userId", resultSet.getObject("user_id", UUID.class));
                    map.put("entityId", resultSet.getString("entity_id"));
                    map.put("email", resultSet.getString("email"));
                    map.put("firstName", resultSet.getString("first_name"));
                    map.put("lastName", resultSet.getString("last_name"));
                    map.put("userType", resultSet.getString("user_type"));
                    map.put("updateUser", resultSet.getString("update_user"));
                    map.put("updateTs", resultSet.getObject("update_ts") != null ? resultSet.getObject("update_ts", OffsetDateTime.class) : null);
                    map.put("aggregateVersion", resultSet.getLong("aggregate_version"));
                    map.put("active", resultSet.getBoolean("active"));
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
    public Result<String> getGroupUserById(String hostId, String groupId, String userId) {
        final String sql =
                """
                SELECT host_id, group_id, user_id, start_ts, end_ts,
                aggregate_version, active, update_user, update_ts
                FROM group_user_t
                WHERE host_id = ? AND group_id = ? AND user_id = ?
                """;
        Result<String> result;
        Map<String, Object> map = new HashMap<>();

        String searchId = hostId + ":" + groupId + ":" + userId;

        try (Connection conn = ds.getConnection();
             PreparedStatement statement = conn.prepareStatement(sql)) {

            statement.setObject(1, UUID.fromString(hostId));
            statement.setString(2, groupId);
            statement.setObject(3, UUID.fromString(userId));

            try (ResultSet resultSet = statement.executeQuery()) {
                if (resultSet.next()) {
                    map.put("hostId", resultSet.getObject("host_id", UUID.class));
                    map.put("groupId", resultSet.getString("group_id"));
                    map.put("userId", resultSet.getObject("user_id", UUID.class));
                    map.put("startTs", resultSet.getObject("start_ts") != null ? resultSet.getObject("start_ts", OffsetDateTime.class) : null);
                    map.put("endTs", resultSet.getObject("end_ts") != null ? resultSet.getObject("end_ts", OffsetDateTime.class) : null);
                    map.put("aggregateVersion", resultSet.getLong("aggregate_version"));
                    map.put("active", resultSet.getBoolean("active"));
                    map.put("updateUser", resultSet.getString("update_user"));
                    map.put("updateTs", resultSet.getObject("update_ts") != null ? resultSet.getObject("update_ts", OffsetDateTime.class) : null);

                    result = Success.of(JsonMapper.toJson(map));
                } else {
                    result = Failure.of(new Status(OBJECT_NOT_FOUND, "group_user", searchId));
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
    public void createGroupPermission(Connection conn, Map<String, Object> event) throws SQLException, Exception {
        // Use UPSERT: INSERT ON CONFLICT DO UPDATE
        // This handles:
        // 1. First time insert (no conflict).
        // 2. Re-creation (conflict on host_id, group_id, endpoint_id) -> UPDATE the existing soft-deleted row (setting active=TRUE and new version).

        final String sql =
                """
                INSERT INTO group_permission_t (
                    host_id,
                    group_id,
                    endpoint_id,
                    update_user,
                    update_ts,
                    aggregate_version,
                    active
                ) VALUES (?, ?, ?, ?, ?, ?, TRUE)
                ON CONFLICT (host_id, group_id, endpoint_id) DO UPDATE
                SET update_user = EXCLUDED.update_user,
                    update_ts = EXCLUDED.update_ts,
                    aggregate_version = EXCLUDED.aggregate_version,
                    active = TRUE
                -- OCC/IDM: Only update if the incoming event is newer
                WHERE group_permission_t.aggregate_version < EXCLUDED.aggregate_version AND group_permission_t.active = FALSE
                """;

        // Note: The original code uses a non-standard map retrieval: Map<String, Object> map = (Map<String, Object>)event.get(PortalConstants.DATA);
        // Assuming SqlUtil.extractEventData(event) is the correct utility based on other methods.
        Map<String, Object> map = SqlUtil.extractEventData(event);

        String hostId = (String)map.get("hostId");
        String groupId = (String)map.get("groupId");
        String endpointId = (String)map.get("endpointId");
        long newAggregateVersion = SqlUtil.getNewAggregateVersion(event);

        try (PreparedStatement statement = conn.prepareStatement(sql)) {
            // INSERT values (6 placeholders + active=TRUE in SQL, total 6 dynamic values)
            // 1: host_id
            statement.setObject(1, UUID.fromString(hostId));
            // 2: group_id
            statement.setString(2, groupId);
            // 3: endpoint_id
            statement.setObject(3, UUID.fromString(endpointId));
            // 4: update_user
            statement.setString(4, (String)event.get(Constants.USER));
            // 5: update_ts
            statement.setObject(5, OffsetDateTime.parse((String)event.get(CloudEventV1.TIME)));
            // 6: aggregate_version
            statement.setLong(6, newAggregateVersion);

            int count = statement.executeUpdate();
            if (count == 0) {
                // count=0 means the ON CONFLICT clause was hit, BUT the WHERE clause (aggregate_version < EXCLUDED.aggregate_version) failed.
                // This is the desired idempotent/out-of-order protection behavior. Log and ignore.
                logger.warn("Creation/Reactivation skipped for hostId {} groupId {} endpointId {} aggregateVersion {}. A newer or same version already exists.", hostId, groupId, endpointId, newAggregateVersion);
            }
        } catch (SQLException e) {
            logger.error("SQLException during createGroupPermission for hostId {} groupId {} endpointId {} aggregateVersion {}: {}", hostId, groupId, endpointId, newAggregateVersion, e.getMessage(), e);
            throw e;
        } catch (Exception e) {
            logger.error("Exception during createGroupPermission for hostId {} groupId {} endpointId {} aggregateVersion {}: {}", hostId, groupId, endpointId, newAggregateVersion, e.getMessage(), e);
            throw e;
        }
    }

    @Override
    public void deleteGroupPermission(Connection conn, Map<String, Object> event) throws SQLException, Exception {
        // Use UPDATE to implement Soft Delete (setting active = FALSE).
        // OCC/IDM is enforced by checking aggregate_version < newAggregateVersion.
        final String sql =
                """
                UPDATE group_permission_t
                SET active = FALSE,
                    update_user = ?,
                    update_ts = ?,
                    aggregate_version = ?
                WHERE host_id = ?
                  AND group_id = ?
                  AND endpoint_id = ?
                  AND aggregate_version < ?
                """; // <<< CRITICAL: Added aggregate_version < ? to enforce monotonicity (OCC/IDM)

        // Note: The original code uses a non-standard map retrieval: Map<String, Object> map = (Map<String, Object>)event.get(PortalConstants.DATA);
        // Assuming SqlUtil.extractEventData(event) is the correct utility based on other methods.
        Map<String, Object> map = SqlUtil.extractEventData(event);

        String hostId = (String)map.get("hostId");
        String groupId = (String)map.get("groupId");
        String endpointId = (String)map.get("endpointId");
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

            // WHERE conditions (4 placeholders)
            // 4: host_id
            statement.setObject(4, UUID.fromString(hostId));
            // 5: group_id
            statement.setString(5, groupId);
            // 6: endpoint_id
            statement.setObject(6, UUID.fromString(endpointId));
            // 7: aggregate_version < ? (new version for OCC/IDM check)
            statement.setLong(7, newAggregateVersion);

            int count = statement.executeUpdate();
            if (count == 0) {
                // If 0 rows updated, it means:
                // 1. The record was not found (already deleted or never existed).
                // 2. The OCC/IDM check failed (aggregate_version >= newAggregateVersion).
                // We IGNORE the failure and log a warning, as this is the desired idempotent/monotonic behavior.
                logger.warn("Soft delete skipped for hostId {} groupId {} endpointId {} aggregateVersion {}. Record not found or a newer/same version already exists.", hostId, groupId, endpointId, newAggregateVersion);
            }
        } catch (SQLException e) {
            logger.error("SQLException during deleteGroupPermission for hostId {} groupId {} endpointId {} aggregateVersion {}: {}", hostId, groupId, endpointId, newAggregateVersion, e.getMessage(), e);
            throw e;
        } catch (Exception e) {
            logger.error("Exception during deleteGroupPermission for hostId {} groupId {} endpointId {} aggregateVersion {}: {}", hostId, groupId, endpointId, newAggregateVersion, e.getMessage(), e);
            throw e;
        }
    }

    @Override
    public void createGroupUser(Connection conn, Map<String, Object> event) throws SQLException, Exception {
        // Use UPSERT: INSERT ON CONFLICT DO UPDATE
        // This handles:
        // 1. First time insert (no conflict).
        // 2. Re-creation (conflict on host_id, group_id, user_id) -> UPDATE the existing soft-deleted row (setting active=TRUE and new version).

        final String sql =
                """
                INSERT INTO group_user_t
                (host_id, group_id, user_id, start_ts, end_ts, update_user, update_ts, aggregate_version, active)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, TRUE)
                ON CONFLICT (host_id, group_id, user_id) DO UPDATE
                SET start_ts = EXCLUDED.start_ts,
                    end_ts = EXCLUDED.end_ts,
                    update_user = EXCLUDED.update_user,
                    update_ts = EXCLUDED.update_ts,
                    aggregate_version = EXCLUDED.aggregate_version,
                    active = TRUE
                -- OCC/IDM: Only update if the incoming event is newer
                WHERE group_user_t.aggregate_version < EXCLUDED.aggregate_version AND group_user_t.active = FALSE
                """;

        // Note: The original code uses a non-standard map retrieval: Map<String, Object> map = (Map<String, Object>)event.get(PortalConstants.DATA);
        // Assuming SqlUtil.extractEventData(event) is the correct utility based on other methods.
        Map<String, Object> map = SqlUtil.extractEventData(event);

        String hostId = (String)map.get("hostId");
        String groupId = (String)map.get("groupId");
        String userId = (String)map.get("userId");
        long newAggregateVersion = SqlUtil.getNewAggregateVersion(event);

        try (PreparedStatement statement = conn.prepareStatement(sql)) {
            // INSERT values (8 placeholders + active=TRUE in SQL, total 8 dynamic values)
            // 1: host_id
            statement.setObject(1, UUID.fromString(hostId));
            // 2: group_id
            statement.setString(2, groupId);
            // 3: user_id
            statement.setObject(3, UUID.fromString(userId));

            // 4: start_ts
            String startTs = (String)map.get("startTs");
            if(startTs != null && !startTs.isEmpty())
                statement.setObject(4, OffsetDateTime.parse(startTs));
            else
                statement.setNull(4, Types.TIMESTAMP_WITH_TIMEZONE); // Use Types.TIMESTAMP_WITH_TIMEZONE

            // 5: end_ts
            String endTs = (String)map.get("endTs");
            if (endTs != null && !endTs.isEmpty()) {
                statement.setObject(5, OffsetDateTime.parse(endTs));
            } else {
                statement.setNull(5, Types.TIMESTAMP_WITH_TIMEZONE); // Use Types.TIMESTAMP_WITH_TIMEZONE
            }

            // 6: update_user
            statement.setString(6, (String)event.get(Constants.USER));
            // 7: update_ts
            statement.setObject(7, OffsetDateTime.parse((String)event.get(CloudEventV1.TIME)));
            // 8: aggregate_version
            statement.setLong(8, newAggregateVersion);

            int count = statement.executeUpdate();
            if (count == 0) {
                // count=0 means the ON CONFLICT clause was hit, BUT the WHERE clause (aggregate_version < EXCLUDED.aggregate_version) failed.
                // This is the desired idempotent/out-of-order protection behavior. Log and ignore.
                logger.warn("Creation/Reactivation skipped for hostId {} groupId {} userId {} aggregateVersion {}. A newer or same version already exists.", hostId, groupId, userId, newAggregateVersion);
            }
        } catch (SQLException e) {
            logger.error("SQLException during createGroupUser for hostId {} groupId {} userId {} and aggregateVersion {}: {}", hostId, groupId, userId, newAggregateVersion, e.getMessage(), e);
            throw e;
        } catch (Exception e) {
            logger.error("Exception during createGroupUser for hostId {} groupId {} userId {} and aggregateVersion {}: {}", hostId, groupId, userId, newAggregateVersion, e.getMessage(), e);
            throw e;
        }
    }

    @Override
    public void updateGroupUser(Connection conn, Map<String, Object> event) throws SQLException, Exception {
        // We attempt to update the record IF the incoming event's aggregate_version is greater than the current projection's version.
        // This enforces Idempotence (IDM) and Optimistic Concurrency Control (OCC) by ensuring version monotonicity.
        // We explicitly set active = TRUE as an UPDATE event implies the group-user assignment should be active.
        final String sql =
                """
                UPDATE group_user_t
                SET start_ts = ?,
                    end_ts = ?,
                    update_user = ?,
                    update_ts = ?,
                    aggregate_version = ?,
                    active = TRUE
                WHERE host_id = ?
                  AND group_id = ?
                  AND user_id = ?
                  AND aggregate_version < ?
                """; // <<< CRITICAL: Changed aggregate_version = ? to aggregate_version < ? to enforce monotonicity (OCC/IDM)

        // Note: The original code uses a non-standard map retrieval: Map<String, Object> map = (Map<String, Object>)event.get(PortalConstants.DATA);
        // Assuming SqlUtil.extractEventData(event) is the correct utility based on other methods.
        Map<String, Object> map = SqlUtil.extractEventData(event);

        String hostId = (String)map.get("hostId");
        String groupId = (String)map.get("groupId");
        String userId = (String)map.get("userId");
        long newAggregateVersion = SqlUtil.getNewAggregateVersion(event);

        try (PreparedStatement statement = conn.prepareStatement(sql)) {
            // SET values (5 dynamic values + active = TRUE in SQL)

            // 1: start_ts
            String startTs = (String)map.get("startTs");
            if(startTs != null && !startTs.isEmpty())
                statement.setObject(1, OffsetDateTime.parse(startTs));
            else
                statement.setNull(1, Types.TIMESTAMP_WITH_TIMEZONE); // Use Types.TIMESTAMP_WITH_TIMEZONE

            // 2: end_ts
            String endTs = (String)map.get("endTs");
            if (endTs != null && !endTs.isEmpty()) {
                statement.setObject(2, OffsetDateTime.parse(endTs));
            } else {
                statement.setNull(2, Types.TIMESTAMP_WITH_TIMEZONE); // Use Types.TIMESTAMP_WITH_TIMEZONE
            }

            // 3: update_user
            statement.setString(3, (String)event.get(Constants.USER));
            // 4: update_ts
            statement.setObject(4, OffsetDateTime.parse((String)event.get(CloudEventV1.TIME)));
            // 5: aggregate_version
            statement.setLong(5, newAggregateVersion);

            // WHERE conditions (4 placeholders)
            // 6: host_id (Corrected parameter 6 to use the hostId variable from map)
            statement.setObject(6, UUID.fromString(hostId));
            // 7: group_id
            statement.setString(7, groupId);
            // 8: user_id
            statement.setObject(8, UUID.fromString(userId));
            // 9: aggregate_version < ? (new version for OCC/IDM check)
            statement.setLong(9, newAggregateVersion);

            int count = statement.executeUpdate();
            if (count == 0) {
                // If 0 rows updated, it means the record was either not found
                // OR aggregate_version >= newAggregateVersion (OCC/IDM check failed).
                // We IGNORE the failure and log a warning, as this is the desired idempotent/monotonic behavior.
                logger.warn("Update skipped for hostId {} groupId {} userId {} aggregateVersion {}. Record not found or a newer/same version already exists.", hostId, groupId, userId, newAggregateVersion);
            }
        } catch (SQLException e) {
            logger.error("SQLException during updateGroupUser for hostId {} groupId {} userId {} aggregateVersion {}: {}", hostId, groupId, userId, newAggregateVersion, e.getMessage(), e);
            throw e;
        } catch (Exception e) {
            logger.error("Exception during updateGroupUser for hostId {} groupId {} userId {} aggregateVersion {}: {}", hostId, groupId, userId, newAggregateVersion, e.getMessage(), e);
            throw e;
        }
    }


    @Override
    public void deleteGroupUser(Connection conn, Map<String, Object> event) throws SQLException, Exception {
        // Use UPDATE to implement Soft Delete (setting active = FALSE).
        // OCC/IDM is enforced by checking aggregate_version < newAggregateVersion.
        final String sql =
                """
                UPDATE group_user_t
                SET active = FALSE,
                    update_user = ?,
                    update_ts = ?,
                    aggregate_version = ?
                WHERE host_id = ?
                  AND group_id = ?
                  AND user_id = ?
                  AND aggregate_version < ?
                """; // <<< CRITICAL: Added aggregate_version < ? to enforce monotonicity (OCC/IDM)

        // Note: The original code uses a non-standard map retrieval: Map<String, Object> map = (Map<String, Object>)event.get(PortalConstants.DATA);
        // Assuming SqlUtil.extractEventData(event) is the correct utility based on other methods.
        Map<String, Object> map = SqlUtil.extractEventData(event);

        String hostId = (String)map.get("hostId");
        String groupId = (String)map.get("groupId");
        String userId = (String)map.get("userId");
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

            // WHERE conditions (4 placeholders)
            // 4: host_id
            statement.setObject(4, UUID.fromString(hostId));
            // 5: group_id
            statement.setString(5, groupId);
            // 6: user_id
            statement.setObject(6, UUID.fromString(userId));
            // 7: aggregate_version < ? (new version for OCC/IDM check)
            statement.setLong(7, newAggregateVersion);

            int count = statement.executeUpdate();
            if (count == 0) {
                // If 0 rows updated, it means:
                // 1. The record was not found (already deleted or never existed).
                // 2. The OCC/IDM check failed (aggregate_version >= newAggregateVersion).
                // We IGNORE the failure and log a warning, as this is the desired idempotent/monotonic behavior.
                logger.warn("Soft delete skipped for hostId {} groupId {} userId {} aggregateVersion {}. Record not found or a newer/same version already exists.", hostId, groupId, userId, newAggregateVersion);
            }
        } catch (SQLException e) {
            logger.error("SQLException during deleteGroupUser for hostId {} groupId {} userId {} aggregateVersion {}: {}", hostId, groupId, userId, newAggregateVersion, e.getMessage(), e);
            throw e;
        } catch (Exception e) {
            logger.error("Exception during deleteGroupUser for hostId {} groupId {} userId {} aggregateVersion {}: {}", hostId, groupId, userId, newAggregateVersion, e.getMessage(), e);
            throw e;
        }
    }

    @Override
    public Result<String> queryGroupRowFilter(int offset, int limit, String filtersJson, String globalFilter, String sortingJson, boolean active, String hostId) {
        Result<String> result;
        final Map<String, String> columnMap = new HashMap<>(Map.of(
                "hostId", "g.host_id",
                "groupId", "g.group_id",
                "endpointId", "p.endpoint_id",
                "endpoint", "ae.endpoint",
                "apiVersionId", "av.api_version_id",
                "apiId", "av.api_id",
                "apiVersion", "av.api_version",
                "colName", "p.col_name",
                "operator", "p.operator",
                "colValue", "p.col_value"
        ));
        columnMap.put("updateUser", "p.update_user");
        columnMap.put("updateTs", "p.update_ts");
        columnMap.put("aggregateVersion", "p.aggregate_version");
        columnMap.put("active", "p.active");


        List<Map<String, Object>> filters = parseJsonList(filtersJson);
        List<Map<String, Object>> sorting = parseJsonList(sortingJson);

        String s =
            """
            SELECT COUNT(*) OVER () AS total, g.host_id, g.group_id, p.endpoint_id, ae.endpoint,
            av.api_version_id, av.api_id, av.api_version, p.col_name, p.operator, p.col_value,
            p.update_user, p.update_ts, p.aggregate_version, p.active
            FROM group_row_filter_t p
            JOIN group_t g ON g.host_id = p.host_id AND g.group_id = p.group_id
            JOIN api_endpoint_t ae ON ae.host_id = p.host_id AND ae.endpoint_id = p.endpoint_id
            JOIN api_version_t av ON av.host_id = ae.host_id AND av.api_version_id = ae.api_version_id
            WHERE p.host_id = ?
            """;

        List<Object> parameters = new ArrayList<>();
        parameters.add(UUID.fromString(hostId));

        String activeClause = SqlUtil.buildMultiTableActiveClause(active, "p", "g", "ae", "av");
        String[] searchColumns = {"g.group_id", "ae.endpoint", "p.col_name", "p.col_value"};
        String sqlBuilder = s + activeClause +
                dynamicFilter(Arrays.asList("g.host_id", "p.endpoint_id", "av.api_version_id"), Arrays.asList(searchColumns), filters, columnMap, parameters) +
                globalFilter(globalFilter, searchColumns, parameters) +
                dynamicSorting("g.group_id, av.api_id, av.api_version, ae.endpoint, p.col_name, p.operator", sorting, columnMap) +
                "\nLIMIT ? OFFSET ?";

        parameters.add(limit);
        parameters.add(offset);

        if(logger.isTraceEnabled()) logger.trace("queryGroupRowFilter sql: {}", sqlBuilder);
        int total = 0;
        List<Map<String, Object>> groupRowFilters = new ArrayList<>();

        try (final Connection conn = ds.getConnection(); PreparedStatement preparedStatement = conn.prepareStatement(sqlBuilder)) {
            populateParameters(preparedStatement, parameters);
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
                    map.put("endpointId", resultSet.getObject("endpoint_id", UUID.class));
                    map.put("apiVersionId", resultSet.getObject("api_version_id", UUID.class));
                    map.put("apiId", resultSet.getString("api_id"));
                    map.put("apiVersion", resultSet.getString("api_version"));
                    map.put("endpoint", resultSet.getString("endpoint"));
                    map.put("colName", resultSet.getString("col_name"));
                    map.put("operator", resultSet.getString("operator"));
                    map.put("colValue", resultSet.getString("col_value"));
                    map.put("updateUser", resultSet.getString("update_user"));
                    map.put("updateTs", resultSet.getObject("update_ts") != null ? resultSet.getObject("update_ts", OffsetDateTime.class) : null);
                    map.put("aggregateVersion", resultSet.getLong("aggregate_version"));
                    map.put("active", resultSet.getBoolean("active"));
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
    public Result<String> getGroupRowFilterById(String hostId, String groupId, String endpointId, String colName) {
        final String sql =
                """
                SELECT host_id, group_id, endpoint_id, col_name, operator, col_value,
                aggregate_version, active, update_user, update_ts
                FROM group_row_filter_t
                WHERE host_id = ? AND group_id = ? AND endpoint_id = ? AND col_name = ?
                """;
        Result<String> result;
        Map<String, Object> map = new HashMap<>();

        String searchId = hostId + ":" + groupId + ":" + endpointId + ":" + colName;

        try (Connection conn = ds.getConnection();
             PreparedStatement statement = conn.prepareStatement(sql)) {

            statement.setObject(1, UUID.fromString(hostId));
            statement.setString(2, groupId);
            statement.setObject(3, UUID.fromString(endpointId));
            statement.setString(4, colName);

            try (ResultSet resultSet = statement.executeQuery()) {
                if (resultSet.next()) {
                    map.put("hostId", resultSet.getObject("host_id", UUID.class));
                    map.put("groupId", resultSet.getString("group_id"));
                    map.put("endpointId", resultSet.getObject("endpoint_id", UUID.class));
                    map.put("colName", resultSet.getString("col_name"));
                    map.put("operator", resultSet.getString("operator"));
                    map.put("colValue", resultSet.getString("col_value"));
                    map.put("aggregateVersion", resultSet.getLong("aggregate_version"));
                    map.put("active", resultSet.getBoolean("active"));
                    map.put("updateUser", resultSet.getString("update_user"));
                    map.put("updateTs", resultSet.getObject("update_ts") != null ? resultSet.getObject("update_ts", OffsetDateTime.class) : null);

                    result = Success.of(JsonMapper.toJson(map));
                } else {
                    result = Failure.of(new Status(OBJECT_NOT_FOUND, "group_row_filter", searchId));
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
    public void createGroupRowFilter(Connection conn, Map<String, Object> event) throws SQLException, Exception {
        // Use UPSERT: INSERT ON CONFLICT DO UPDATE
        // This handles:
        // 1. First time insert (no conflict).
        // 2. Re-creation (conflict on host_id, group_id, endpoint_id, col_name) -> UPDATE the existing soft-deleted row (setting active=TRUE and new version).

        final String sql =
                """
                INSERT INTO group_row_filter_t (
                    host_id,
                    group_id,
                    endpoint_id,
                    col_name,
                    operator,
                    col_value,
                    update_user,
                    update_ts,
                    aggregate_version,
                    active
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, TRUE)
                ON CONFLICT (host_id, group_id, endpoint_id, col_name) DO UPDATE
                SET operator = EXCLUDED.operator,
                    col_value = EXCLUDED.col_value,
                    update_user = EXCLUDED.update_user,
                    update_ts = EXCLUDED.update_ts,
                    aggregate_version = EXCLUDED.aggregate_version,
                    active = TRUE
                -- OCC/IDM: Only update if the incoming event is newer
                WHERE group_row_filter_t.aggregate_version < EXCLUDED.aggregate_version AND group_row_filter_t.active = FALSE
                """;

        // Note: The original code uses a non-standard map retrieval: Map<String, Object> map = (Map<String, Object>)event.get(PortalConstants.DATA);
        // Assuming SqlUtil.extractEventData(event) is the correct utility based on other methods.
        Map<String, Object> map = SqlUtil.extractEventData(event);

        String hostId = (String)map.get("hostId");
        String groupId = (String)map.get("groupId");
        String endpointId = (String)map.get("endpointId");
        String colName = (String)map.get("colName");
        long newAggregateVersion = SqlUtil.getNewAggregateVersion(event);

        try (PreparedStatement statement = conn.prepareStatement(sql)) {
            // INSERT values (9 placeholders + active=TRUE in SQL, total 9 dynamic values)
            // 1: host_id
            statement.setObject(1, UUID.fromString(hostId));
            // 2: group_id
            statement.setString(2, groupId);
            // 3: endpoint_id
            statement.setObject(3, UUID.fromString(endpointId));
            // 4: col_name
            statement.setString(4, colName);
            // 5: operator
            statement.setString(5, (String)map.get("operator"));
            // 6: col_value
            statement.setString(6, (String)map.get("colValue"));
            // 7: update_user
            statement.setString(7, (String)event.get(Constants.USER));
            // 8: update_ts
            statement.setObject(8, OffsetDateTime.parse((String)event.get(CloudEventV1.TIME)));
            // 9: aggregate_version
            statement.setLong(9, newAggregateVersion);

            int count = statement.executeUpdate();
            if (count == 0) {
                // count=0 means the ON CONFLICT clause was hit, BUT the WHERE clause (aggregate_version < EXCLUDED.aggregate_version) failed.
                // This is the desired idempotent/out-of-order protection behavior. Log and ignore.
                logger.warn("Creation/Reactivation skipped for hostId {} groupId {} endpointId {} colName {} aggregateVersion {}. A newer or same version already exists.", hostId, groupId, endpointId, colName, newAggregateVersion);
            }
        } catch (SQLException e) {
            logger.error("SQLException during createGroupRowFilter for hostId {} groupId {} endpointId {} colName {} aggregateVersion {}: {}", hostId, groupId, endpointId, colName, newAggregateVersion, e.getMessage(), e);
            throw e;
        } catch (Exception e) {
            logger.error("Exception during createGroupRowFilter for hostId {} groupId {} endpointId {} colName {} aggregateVersion {}: {}", hostId, groupId, endpointId, colName, newAggregateVersion, e.getMessage(), e);
            throw e;
        }
    }

    @Override
    public void updateGroupRowFilter(Connection conn, Map<String, Object> event) throws SQLException, Exception {
        // We attempt to update the record IF the incoming event's aggregate_version is greater than the current projection's version.
        // This enforces Idempotence (IDM) and Optimistic Concurrency Control (OCC) by ensuring version monotonicity.
        // We explicitly set active = TRUE as an UPDATE event implies the row filter should be active.
        final String sql =
                """
                UPDATE group_row_filter_t
                SET
                    operator = ?,
                    col_value = ?,
                    update_user = ?,
                    update_ts = ?,
                    aggregate_version = ?,
                    active = TRUE
                WHERE
                    host_id = ?
                    AND group_id = ?
                    AND endpoint_id = ?
                    AND col_name = ?
                    AND aggregate_version < ?
                """; // <<< CRITICAL: Changed aggregate_version = ? to aggregate_version < ? to enforce monotonicity (OCC/IDM)

        // Note: The original code uses a non-standard map retrieval: Map<String, Object> map = (Map<String, Object>)event.get(PortalConstants.DATA);
        // Assuming SqlUtil.extractEventData(event) is the correct utility based on other methods.
        Map<String, Object> map = SqlUtil.extractEventData(event);

        String hostId = (String)map.get("hostId");
        String groupId = (String)map.get("groupId");
        String endpointId = (String)map.get("endpointId");
        String colName = (String)map.get("colName");
        long newAggregateVersion = SqlUtil.getNewAggregateVersion(event);

        try (PreparedStatement statement = conn.prepareStatement(sql)) {
            // SET values (5 dynamic values + active = TRUE in SQL)
            // 1: operator
            statement.setString(1, (String)map.get("operator"));
            // 2: col_value
            statement.setString(2, (String)map.get("colValue"));
            // 3: update_user
            statement.setString(3, (String)event.get(Constants.USER));
            // 4: update_ts
            statement.setObject(4, OffsetDateTime.parse((String)event.get(CloudEventV1.TIME)));
            // 5: aggregate_version
            statement.setLong(5, newAggregateVersion);

            // WHERE conditions (5 placeholders)
            // 6: host_id
            statement.setObject(6, UUID.fromString(hostId));
            // 7: group_id
            statement.setString(7, groupId);
            // 8: endpoint_id
            statement.setObject(8, UUID.fromString(endpointId));
            // 9: col_name
            statement.setString(9, colName);
            // 10: aggregate_version < ? (new version for OCC/IDM check)
            statement.setLong(10, newAggregateVersion);

            int count = statement.executeUpdate();
            if (count == 0) {
                // If 0 rows updated, it means the record was either not found
                // OR aggregate_version >= newAggregateVersion (OCC/IDM check failed).
                // We IGNORE the failure and log a warning, as this is the desired idempotent/monotonic behavior.
                logger.warn("Update skipped for hostId {} groupId {} endpointId {} colName {} aggregateVersion {}. Record not found or a newer/same version already exists.", hostId, groupId, endpointId, colName, newAggregateVersion);
            }
        } catch (SQLException e) {
            logger.error("SQLException during updateGroupRowFilter for hostId {} groupId {} endpointId {} colName {} aggregateVersion {}: {}", hostId, groupId, endpointId, colName, newAggregateVersion, e.getMessage(), e);
            throw e;
        } catch (Exception e) {
            logger.error("Exception during updateGroupRowFilter for hostId {} groupId {} endpointId {} colName {} aggregateVersion {}: {}", hostId, groupId, endpointId, colName, newAggregateVersion, e.getMessage(), e);
            throw e;
        }
    }

    @Override
    public void deleteGroupRowFilter(Connection conn, Map<String, Object> event) throws SQLException, Exception {
        // Use UPDATE to implement Soft Delete (setting active = FALSE).
        // OCC/IDM is enforced by checking aggregate_version < newAggregateVersion.
        final String sql =
                """
                UPDATE group_row_filter_t
                SET active = FALSE,
                    update_user = ?,
                    update_ts = ?,
                    aggregate_version = ?
                WHERE host_id = ?
                  AND group_id = ?
                  AND endpoint_id = ?
                  AND col_name = ?
                  AND aggregate_version < ?
                """; // <<< CRITICAL: Added aggregate_version < ? to enforce monotonicity (OCC/IDM)

        // Note: The original code uses a non-standard map retrieval: Map<String, Object> map = (Map<String, Object>)event.get(PortalConstants.DATA);
        // Assuming SqlUtil.extractEventData(event) is the correct utility based on other methods.
        Map<String, Object> map = SqlUtil.extractEventData(event);

        String hostId = (String)map.get("hostId");
        String groupId = (String)map.get("groupId");
        String endpointId = (String)map.get("endpointId");
        String colName = (String)map.get("colName");
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

            // WHERE conditions (5 placeholders)
            // 4: host_id
            statement.setObject(4, UUID.fromString(hostId));
            // 5: group_id
            statement.setString(5, groupId);
            // 6: endpoint_id
            statement.setObject(6, UUID.fromString(endpointId));
            // 7: col_name
            statement.setString(7, colName);
            // 8: aggregate_version < ? (new version for OCC/IDM check)
            statement.setLong(8, newAggregateVersion);

            int count = statement.executeUpdate();
            if (count == 0) {
                // If 0 rows updated, it means:
                // 1. The record was not found (already deleted or never existed).
                // 2. The OCC/IDM check failed (aggregate_version >= newAggregateVersion).
                // We IGNORE the failure and log a warning, as this is the desired idempotent/monotonic behavior.
                logger.warn("Soft delete skipped for hostId {} groupId {} endpointId {} colName {} aggregateVersion {}. Record not found or a newer/same version already exists.", hostId, groupId, endpointId, colName, newAggregateVersion);
            }
        } catch (SQLException e) {
            logger.error("SQLException during deleteGroupRowFilter for hostId {} groupId {} endpointId {} colName {} aggregateVersion {}: {}", hostId, groupId, endpointId, colName, newAggregateVersion, e.getMessage(), e);
            throw e;
        } catch (Exception e) {
            logger.error("Exception during deleteGroupRowFilter for hostId {} groupId {} endpointId {} colName {} aggregateVersion {}: {}", hostId, groupId, endpointId, colName, newAggregateVersion, e.getMessage(), e);
            throw e;
        }
    }

    @Override
    public Result<String> queryGroupColFilter(int offset, int limit, String filtersJson, String globalFilter, String sortingJson, boolean active, String hostId) {
        Result<String> result;
        final Map<String, String> columnMap = new HashMap<>(Map.of(
                "hostId", "g.host_id",
                "groupId", "g.group_id",
                "endpointId", "ae.endpoint_id",
                "endpoint", "ae.endpoint",
                "apiVersionId", "av.api_version_id",
                "apiId", "av.api_id",
                "apiVersion", "av.api_version",
                "columns", "cf.columns",
                "updateUser", "cf.update_user",
                "updateTs", "cf.update_ts"
        ));
        columnMap.put("aggregateVersion", "cf.aggregate_version");
        columnMap.put("active", "cf.active");

        List<Map<String, Object>> filters = parseJsonList(filtersJson);
        List<Map<String, Object>> sorting = parseJsonList(sortingJson);

        String s =
            """
            SELECT COUNT(*) OVER () AS total,
            g.host_id, g.group_id, av.api_version_id, av.api_id, av.api_version,
            ae.endpoint_id, ae.endpoint, cf.columns, cf.aggregate_version, cf.active,
            cf.update_user, cf.update_ts
            FROM group_col_filter_t cf
            JOIN group_t g ON g.group_id = cf.group_id
            JOIN api_endpoint_t ae ON cf.host_id = ae.host_id AND cf.endpoint_id = ae.endpoint_id
            JOIN api_version_t av ON ae.host_id = av.host_id AND ae.api_version_id = av.api_version_id
            WHERE cf.host_id = ?
            """;

        List<Object> parameters = new ArrayList<>();
        parameters.add(UUID.fromString(hostId));

        String activeClause = SqlUtil.buildMultiTableActiveClause(active, "cf", "g", "ae", "av");
        String[] searchColumns = {"g.group_id", "ae.endpoint", "cf.columns"};
        String sqlBuilder = s + activeClause +
                dynamicFilter(Arrays.asList("g.host_id", "av.api_version_id", "ae.endpoint_id"), Arrays.asList(searchColumns), filters, columnMap, parameters) +
                globalFilter(globalFilter, searchColumns, parameters) +
                dynamicSorting("g.group_id, av.api_id, av.api_version, ae.endpoint", sorting, columnMap) +
                "\nLIMIT ? OFFSET ?";

        parameters.add(limit);
        parameters.add(offset);

        if(logger.isTraceEnabled()) logger.trace("queryGroupColFilter sql: {}", sqlBuilder);
        int total = 0;
        List<Map<String, Object>> groupColFilters = new ArrayList<>();

        try (final Connection conn = ds.getConnection(); PreparedStatement preparedStatement = conn.prepareStatement(sqlBuilder)) {
            populateParameters(preparedStatement, parameters);
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
                    map.put("apiVersionId", resultSet.getObject("api_version_id", UUID.class));
                    map.put("endpointId", resultSet.getObject("endpoint_id", UUID.class));
                    map.put("apiId", resultSet.getString("api_id"));
                    map.put("apiVersion", resultSet.getString("api_version"));
                    map.put("endpoint", resultSet.getString("endpoint"));
                    map.put("columns", resultSet.getString("columns"));
                    map.put("updateUser", resultSet.getString("update_user"));
                    map.put("updateTs", resultSet.getObject("update_ts") != null ? resultSet.getObject("update_ts", OffsetDateTime.class) : null);
                    map.put("aggregateVersion", resultSet.getLong("aggregate_version"));
                    map.put("active", resultSet.getBoolean("active"));
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
    public Result<String> getGroupColFilterById(String hostId, String groupId, String endpointId) {
        final String sql =
                """
                SELECT host_id, group_id, endpoint_id, columns,
                aggregate_version, active, update_user, update_ts
                FROM group_col_filter_t
                WHERE host_id = ? AND group_id = ? AND endpoint_id = ?
                """;
        Result<String> result;
        Map<String, Object> map = new HashMap<>();

        String searchId = hostId + ":" + groupId + ":" + endpointId;

        try (Connection conn = ds.getConnection();
             PreparedStatement statement = conn.prepareStatement(sql)) {

            statement.setObject(1, UUID.fromString(hostId));
            statement.setString(2, groupId);
            statement.setObject(3, UUID.fromString(endpointId));

            try (ResultSet resultSet = statement.executeQuery()) {
                if (resultSet.next()) {
                    map.put("hostId", resultSet.getObject("host_id", UUID.class));
                    map.put("groupId", resultSet.getString("group_id"));
                    map.put("endpointId", resultSet.getObject("endpoint_id", UUID.class));
                    map.put("columns", resultSet.getString("columns"));
                    map.put("aggregateVersion", resultSet.getLong("aggregate_version"));
                    map.put("active", resultSet.getBoolean("active"));
                    map.put("updateUser", resultSet.getString("update_user"));
                    map.put("updateTs", resultSet.getObject("update_ts") != null ? resultSet.getObject("update_ts", OffsetDateTime.class) : null);

                    result = Success.of(JsonMapper.toJson(map));
                } else {
                    result = Failure.of(new Status(OBJECT_NOT_FOUND, "group_col_filter", searchId));
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
    public void createGroupColFilter(Connection conn, Map<String, Object> event) throws SQLException, Exception {
        // Use UPSERT: INSERT ON CONFLICT DO UPDATE
        // This handles:
        // 1. First time insert (no conflict).
        // 2. Re-creation (conflict on host_id, group_id, endpoint_id) -> UPDATE the existing soft-deleted row (setting active=TRUE and new version).

        final String sql =
                """
                INSERT INTO group_col_filter_t (
                    host_id,
                    group_id,
                    endpoint_id,
                    columns,
                    update_user,
                    update_ts,
                    aggregate_version,
                    active
                ) VALUES (?, ?, ?, ?, ?, ?, ?, TRUE)
                ON CONFLICT (host_id, group_id, endpoint_id) DO UPDATE
                SET columns = EXCLUDED.columns,
                    update_user = EXCLUDED.update_user,
                    update_ts = EXCLUDED.update_ts,
                    aggregate_version = EXCLUDED.aggregate_version,
                    active = TRUE
                -- OCC/IDM: Only update if the incoming event is newer
                WHERE group_col_filter_t.aggregate_version < EXCLUDED.aggregate_version AND group_col_filter_t.active = FALSE
                """;

        // Note: The original code uses a non-standard map retrieval: Map<String, Object> map = (Map<String, Object>)event.get(PortalConstants.DATA);
        // Assuming SqlUtil.extractEventData(event) is the correct utility based on other methods.
        Map<String, Object> map = SqlUtil.extractEventData(event);

        String hostId = (String)map.get("hostId");
        String groupId = (String)map.get("groupId");
        String endpointId = (String)map.get("endpointId");
        long newAggregateVersion = SqlUtil.getNewAggregateVersion(event);

        try (PreparedStatement statement = conn.prepareStatement(sql)) {
            // INSERT values (7 placeholders + active=TRUE in SQL, total 7 dynamic values)
            // 1: host_id
            statement.setObject(1, UUID.fromString(hostId));
            // 2: group_id
            statement.setString(2, groupId);
            // 3: endpoint_id
            statement.setObject(3, UUID.fromString(endpointId));
            // 4: columns
            statement.setString(4, (String)map.get("columns"));
            // 5: update_user
            statement.setString(5, (String)event.get(Constants.USER));
            // 6: update_ts
            statement.setObject(6, OffsetDateTime.parse((String)event.get(CloudEventV1.TIME)));
            // 7: aggregate_version
            statement.setLong(7, newAggregateVersion);

            int count = statement.executeUpdate();
            if (count == 0) {
                // count=0 means the ON CONFLICT clause was hit, BUT the WHERE clause (aggregate_version < EXCLUDED.aggregate_version) failed.
                // This is the desired idempotent/out-of-order protection behavior. Log and ignore.
                logger.warn("Creation/Reactivation skipped for hostId {} groupId {} endpointId {} aggregateVersion {}. A newer or same version already exists.", hostId, groupId, endpointId, newAggregateVersion);
            }
        } catch (SQLException e) {
            logger.error("SQLException during createGroupColFilter for hostId {} groupId {} endpointId {} aggregateVersion {}: {}", hostId, groupId, endpointId, newAggregateVersion, e.getMessage(), e);
            throw e;
        } catch (Exception e) {
            logger.error("Exception during createGroupColFilter for hostId {} groupId {} endpointId {} aggregateVersion {}: {}", hostId, groupId, endpointId, newAggregateVersion, e.getMessage(), e);
            throw e;
        }
    }

    @Override
    public void updateGroupColFilter(Connection conn, Map<String, Object> event) throws SQLException, Exception {
        // We attempt to update the record IF the incoming event's aggregate_version is greater than the current projection's version.
        // This enforces Idempotence (IDM) and Optimistic Concurrency Control (OCC) by ensuring version monotonicity.
        // We explicitly set active = TRUE as an UPDATE event implies the column filter should be active.
        final String sql =
                """
                UPDATE group_col_filter_t
                SET
                    columns = ?,
                    update_user = ?,
                    update_ts = ?,
                    aggregate_version = ?,
                    active = TRUE
                WHERE
                    host_id = ?
                    AND group_id = ?
                    AND endpoint_id = ?
                    AND aggregate_version < ?
                """; // <<< CRITICAL: Changed aggregate_version = ? to aggregate_version < ? to enforce monotonicity (OCC/IDM)

        // Note: The original code uses a non-standard map retrieval: Map<String, Object> map = (Map<String, Object>)event.get(PortalConstants.DATA);
        // Assuming SqlUtil.extractEventData(event) is the correct utility based on other methods.
        Map<String, Object> map = SqlUtil.extractEventData(event);

        String hostId = (String)map.get("hostId");
        String groupId = (String)map.get("groupId");
        String endpointId = (String)map.get("endpointId");
        long newAggregateVersion = SqlUtil.getNewAggregateVersion(event);

        try (PreparedStatement statement = conn.prepareStatement(sql)) {
            // SET values (4 dynamic values + active = TRUE in SQL)
            // 1: columns
            statement.setString(1, (String)map.get("columns"));
            // 2: update_user
            statement.setString(2, (String)event.get(Constants.USER));
            // 3: update_ts
            statement.setObject(3, OffsetDateTime.parse((String)event.get(CloudEventV1.TIME)));
            // 4: aggregate_version
            statement.setLong(4, newAggregateVersion);

            // WHERE conditions (4 placeholders)
            // 5: host_id
            statement.setObject(5, UUID.fromString(hostId));
            // 6: group_id
            statement.setString(6, groupId);
            // 7: endpoint_id
            statement.setObject(7, UUID.fromString(endpointId));
            // 8: aggregate_version < ? (new version for OCC/IDM check)
            statement.setLong(8, newAggregateVersion);

            int count = statement.executeUpdate();
            if (count == 0) {
                // If 0 rows updated, it means the record was either not found
                // OR aggregate_version >= newAggregateVersion (OCC/IDM check failed).
                // We IGNORE the failure and log a warning, as this is the desired idempotent/monotonic behavior.
                logger.warn("Update skipped for hostId {} groupId {} endpointId {} aggregateVersion {}. Record not found or a newer/same version already exists.", hostId, groupId, endpointId, newAggregateVersion);
            }
        } catch (SQLException e) {
            logger.error("SQLException during updateGroupColFilter for hostId {} groupId {} endpointId {} aggregateVersion {}: {}", hostId, groupId, endpointId, newAggregateVersion, e.getMessage(), e);
            throw e;
        } catch (Exception e) {
            logger.error("Exception during updateGroupColFilter for hostId {} groupId {} endpointId {} aggregateVersion {}: {}", hostId, groupId, endpointId, newAggregateVersion, e.getMessage(), e);
            throw e;
        }
    }

    @Override
    public void deleteGroupColFilter(Connection conn, Map<String, Object> event) throws SQLException, Exception {
        // Use UPDATE to implement Soft Delete (setting active = FALSE).
        // OCC/IDM is enforced by checking aggregate_version < newAggregateVersion.
        final String sql =
                """
                UPDATE group_col_filter_t
                SET active = FALSE,
                    update_user = ?,
                    update_ts = ?,
                    aggregate_version = ?
                WHERE host_id = ?
                  AND group_id = ?
                  AND endpoint_id = ?
                  AND aggregate_version < ?
                """; // <<< CRITICAL: Added aggregate_version < ? to enforce monotonicity (OCC/IDM)

        // Note: The original code uses a non-standard map retrieval: Map<String, Object> map = (Map<String, Object>)event.get(PortalConstants.DATA);
        // Assuming SqlUtil.extractEventData(event) is the correct utility based on other methods.
        Map<String, Object> map = SqlUtil.extractEventData(event);

        String hostId = (String)map.get("hostId");
        String groupId = (String)map.get("groupId");
        String endpointId = (String)map.get("endpointId");
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

            // WHERE conditions (4 placeholders)
            // 4: host_id
            statement.setObject(4, UUID.fromString(hostId));
            // 5: group_id
            statement.setString(5, groupId);
            // 6: endpoint_id
            statement.setObject(6, UUID.fromString(endpointId));
            // 7: aggregate_version < ? (new version for OCC/IDM check)
            statement.setLong(7, newAggregateVersion);

            int count = statement.executeUpdate();
            if (count == 0) {
                // If 0 rows updated, it means:
                // 1. The record was not found (already deleted or never existed).
                // 2. The OCC/IDM check failed (aggregate_version >= newAggregateVersion).
                // We IGNORE the failure and log a warning, as this is the desired idempotent/monotonic behavior.
                logger.warn("Soft delete skipped for hostId {} groupId {} endpointId {} aggregateVersion {}. Record not found or a newer/same version already exists.", hostId, groupId, endpointId, newAggregateVersion);
            }
        } catch (SQLException e) {
            logger.error("SQLException during deleteGroupColFilter for hostId {} groupId {} endpointId {} aggregateVersion {}: {}", hostId, groupId, endpointId, newAggregateVersion, e.getMessage(), e);
            throw e;
        } catch (Exception e) {
            logger.error("Exception during deleteGroupColFilter for hostId {} groupId {} endpointId {} aggregateVersion {}: {}", hostId, groupId, endpointId, newAggregateVersion, e.getMessage(), e);
            throw e;
        }
    }

    @Override
    public void createPosition(Connection conn, Map<String, Object> event) throws SQLException, Exception {
        // Use UPSERT: INSERT ON CONFLICT DO UPDATE
        // This handles:
        // 1. First time insert (no conflict).
        // 2. Re-creation (conflict on host_id, position_id) -> UPDATE the existing soft-deleted row (setting active=TRUE and new version).

        final String sql =
                """
                INSERT INTO position_t (
                    host_id,
                    position_id,
                    position_desc,
                    inherit_to_ancestor,
                    inherit_to_sibling,
                    update_user,
                    update_ts,
                    aggregate_version,
                    active
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, TRUE)
                ON CONFLICT (host_id, position_id) DO UPDATE
                SET position_desc = EXCLUDED.position_desc,
                    inherit_to_ancestor = EXCLUDED.inherit_to_ancestor,
                    inherit_to_sibling = EXCLUDED.inherit_to_sibling,
                    update_user = EXCLUDED.update_user,
                    update_ts = EXCLUDED.update_ts,
                    aggregate_version = EXCLUDED.aggregate_version,
                    active = TRUE
                -- OCC/IDM: Only update if the incoming event is newer
                WHERE position_t.aggregate_version < EXCLUDED.aggregate_version AND position_t.active = FALSE
                """;

        // Note: The original code uses a non-standard map retrieval: Map<String, Object> map = (Map<String, Object>)event.get(PortalConstants.DATA);
        // Assuming SqlUtil.extractEventData(event) is the correct utility based on other methods.
        Map<String, Object> map = SqlUtil.extractEventData(event);

        String hostId = (String)map.get("hostId");
        String positionId = (String)map.get("positionId");
        long newAggregateVersion = SqlUtil.getNewAggregateVersion(event);

        try (PreparedStatement statement = conn.prepareStatement(sql)) {
            // INSERT values (8 placeholders + active=TRUE in SQL, total 8 dynamic values)
            // 1: host_id
            statement.setObject(1, UUID.fromString(hostId));
            // 2: position_id
            statement.setString(2, positionId);

            // 3: position_desc
            String positionDesc = (String)map.get("positionDesc");
            if (positionDesc != null && !positionDesc.isEmpty())
                statement.setString(3, positionDesc);
            else
                statement.setNull(3, Types.VARCHAR); // Use Types.VARCHAR

            // 4: inherit_to_ancestor
            String inheritToAncestor = (String)map.get("inheritToAncestor");
            if(inheritToAncestor != null && !inheritToAncestor.isEmpty())
                statement.setString(4, inheritToAncestor);
            else
                statement.setNull(4, Types.CHAR); // Use Types.CHAR

            // 5: inherit_to_sibling
            String inheritToSibling = (String)map.get("inheritToSibling");
            if(inheritToSibling != null && !inheritToSibling.isEmpty())
                statement.setString(5, inheritToSibling);
            else
                statement.setNull(5, Types.CHAR); // Use Types.CHAR

            // 6: update_user
            statement.setString(6, (String)event.get(Constants.USER));
            // 7: update_ts
            statement.setObject(7, OffsetDateTime.parse((String)event.get(CloudEventV1.TIME)));
            // 8: aggregate_version
            statement.setLong(8, newAggregateVersion);

            int count = statement.executeUpdate();
            if (count == 0) {
                // count=0 means the ON CONFLICT clause was hit, BUT the WHERE clause (aggregate_version < EXCLUDED.aggregate_version) failed.
                // This is the desired idempotent/out-of-order protection behavior. Log and ignore.
                logger.warn("Creation/Reactivation skipped for hostId {} positionId {} aggregateVersion {}. A newer or same version already exists.", hostId, positionId, newAggregateVersion);
            }
        } catch (SQLException e) {
            logger.error("SQLException during createPosition for hostId {} positionId {} aggregateVersion {}: {}", hostId, positionId, newAggregateVersion, e.getMessage(), e);
            throw e;
        } catch (Exception e) {
            logger.error("Exception during createPosition for hostId {} positionId {} aggregateVersion {}: {}", hostId, positionId, newAggregateVersion, e.getMessage(), e);
            throw e;
        }
    }

    @Override
    public void updatePosition(Connection conn, Map<String, Object> event) throws SQLException, Exception {
        // We attempt to update the record IF the incoming event's aggregate_version is greater than the current projection's version.
        // This enforces Idempotence (IDM) and Optimistic Concurrency Control (OCC) by ensuring version monotonicity.
        // We explicitly set active = TRUE as an UPDATE event implies the position should be active.
        final String sql =
                """
                UPDATE position_t
                SET position_desc = ?,
                    inherit_to_ancestor = ?,
                    inherit_to_sibling = ?,
                    update_user = ?,
                    update_ts = ?,
                    aggregate_version = ?,
                    active = TRUE
                WHERE host_id = ?
                  AND position_id = ?
                  AND aggregate_version < ?
                """; // <<< CRITICAL: Changed aggregate_version = ? to aggregate_version < ? to enforce monotonicity (OCC/IDM)

        // Note: The original code uses a non-standard map retrieval: Map<String, Object> map = (Map<String, Object>)event.get(PortalConstants.DATA);
        // Assuming SqlUtil.extractEventData(event) is the correct utility based on other methods.
        Map<String, Object> map = SqlUtil.extractEventData(event);

        String hostId = (String)map.get("hostId");
        String positionId = (String)map.get("positionId");
        long newAggregateVersion = SqlUtil.getNewAggregateVersion(event);

        try (PreparedStatement statement = conn.prepareStatement(sql)) {
            // SET values (6 dynamic values + active = TRUE in SQL)
            // 1: position_desc
            String positionDesc = (String)map.get("positionDesc");
            if(positionDesc != null && !positionDesc.isEmpty()) {
                statement.setString(1, positionDesc);
            } else {
                statement.setNull(1, Types.VARCHAR); // Use Types.VARCHAR
            }
            // 2: inherit_to_ancestor
            String inheritToAncestor = (String)map.get("inheritToAncestor");
            if(inheritToAncestor != null && !inheritToAncestor.isEmpty()) {
                statement.setString(2, inheritToAncestor);
            } else {
                statement.setNull(2, Types.CHAR); // Use Types.CHAR
            }
            // 3: inherit_to_sibling
            String inheritToSibling = (String)map.get("inheritToSibling");
            if(inheritToSibling != null && !inheritToSibling.isEmpty()) {
                statement.setString(3, inheritToSibling);
            } else {
                statement.setNull(3, Types.CHAR); // Use Types.CHAR
            }
            // 4: update_user
            statement.setString(4, (String)event.get(Constants.USER));
            // 5: update_ts
            statement.setObject(5, OffsetDateTime.parse((String)event.get(CloudEventV1.TIME)));
            // 6: aggregate_version
            statement.setLong(6, newAggregateVersion);

            // WHERE conditions (3 placeholders)
            // 7: host_id (Corrected parameter 7 to use the hostId variable from map)
            statement.setObject(7, UUID.fromString(hostId));
            // 8: position_id
            statement.setString(8, positionId);
            // 9: aggregate_version < ? (new version for OCC/IDM check)
            statement.setLong(9, newAggregateVersion);

            int count = statement.executeUpdate();
            if (count == 0) {
                // If 0 rows updated, it means the record was either not found
                // OR aggregate_version >= newAggregateVersion (OCC/IDM check failed).
                // We IGNORE the failure and log a warning, as this is the desired idempotent/monotonic behavior.
                logger.warn("Update skipped for hostId {} positionId {} aggregateVersion {}. Record not found or a newer/same version already exists.", hostId, positionId, newAggregateVersion);
            }
        } catch (SQLException e) {
            logger.error("SQLException during updatePosition for hostId {} positionId {} aggregateVersion {}: {}", hostId, positionId, newAggregateVersion, e.getMessage(), e);
            throw e;
        } catch (Exception e) {
            logger.error("Exception during updatePosition for hostId {} positionId {} aggregateVersion {}: {}", hostId, positionId, newAggregateVersion, e.getMessage(), e);
            throw e;
        }
    }

    @Override
    public void deletePosition(Connection conn, Map<String, Object> event) throws SQLException, Exception {
        // Use UPDATE to implement Soft Delete (setting active = FALSE).
        // OCC/IDM is enforced by checking aggregate_version < newAggregateVersion.
        final String sql =
                """
                UPDATE position_t
                SET active = FALSE,
                    update_user = ?,
                    update_ts = ?,
                    aggregate_version = ?
                WHERE host_id = ?
                  AND position_id = ?
                  AND aggregate_version < ?
                """; // <<< CRITICAL: Added aggregate_version < ? to enforce monotonicity (OCC/IDM)

        // Note: The original code uses a non-standard map retrieval: Map<String, Object> map = (Map<String, Object>)event.get(PortalConstants.DATA);
        // Assuming SqlUtil.extractEventData(event) is the correct utility based on other methods.
        Map<String, Object> map = SqlUtil.extractEventData(event);

        String hostId = (String)map.get("hostId");
        String positionId = (String)map.get("positionId");
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

            // WHERE conditions (3 placeholders)
            // 4: host_id
            statement.setObject(4, UUID.fromString(hostId));
            // 5: position_id
            statement.setString(5, positionId);
            // 6: aggregate_version < ? (new version for OCC/IDM check)
            statement.setLong(6, newAggregateVersion);

            int count = statement.executeUpdate();
            if (count == 0) {
                // If 0 rows updated, it means:
                // 1. The record was not found (already deleted or never existed).
                // 2. The OCC/IDM check failed (aggregate_version >= newAggregateVersion).
                // We IGNORE the failure and log a warning, as this is the desired idempotent/monotonic behavior.
                logger.warn("Soft delete skipped for hostId {} positionId {} aggregateVersion {}. Record not found or a newer/same version already exists.", hostId, positionId, newAggregateVersion);
            }
        } catch (SQLException e) {
            logger.error("SQLException during deletePosition for hostId {} positionId {} aggregateVersion {}: {}", hostId, positionId, newAggregateVersion, e.getMessage(), e);
            throw e;
        } catch (Exception e) {
            logger.error("Exception during deletePosition for hostId {} positionId {} aggregateVersion {}: {}", hostId, positionId, newAggregateVersion, e.getMessage(), e);
            throw e;
        }
    }

    @Override
    public Result<String> queryPosition(int offset, int limit, String filtersJson, String globalFilter, String sortingJson, boolean active, String hostId) {
        Result<String> result;
        List<Map<String, Object>> filters = parseJsonList(filtersJson);
        List<Map<String, Object>> sorting = parseJsonList(sortingJson);

        String s =
            """
            SELECT COUNT(*) OVER () AS total,
            host_id, position_id, position_desc, inherit_to_ancestor, inherit_to_sibling,
            update_user, update_ts, aggregate_version, active
            FROM position_t
            WHERE host_id = ?
            """;

        List<Object> parameters = new ArrayList<>();
        parameters.add(UUID.fromString(hostId));

        String activeClause = SqlUtil.buildMultiTableActiveClause(active);
        String[] searchColumns = {"position_id", "position_desc"};
        String sqlBuilder = s + dynamicFilter(Arrays.asList("host_id"), Arrays.asList(searchColumns), filters, null, parameters) +
                globalFilter(globalFilter, searchColumns, parameters) +
                dynamicSorting("position_id", sorting, null) +
                "\nLIMIT ? OFFSET ?";

        parameters.add(limit);
        parameters.add(offset);

        int total = 0;
        List<Map<String, Object>> positions = new ArrayList<>();
        try (final Connection conn = ds.getConnection(); PreparedStatement preparedStatement = conn.prepareStatement(sqlBuilder)) {
            populateParameters(preparedStatement, parameters);
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
                    map.put("aggregateVersion", resultSet.getLong("aggregate_version"));
                    map.put("active", resultSet.getBoolean("active"));
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
    public Result<String> getPositionById(String hostId, String positionId) {
        final String sql =
                """
                SELECT host_id, position_id, position_desc, inherit_to_ancestor, inherit_to_sibling,
                aggregate_version, active, update_user, update_ts
                FROM position_t
                WHERE host_id = ? AND position_id = ?
                """;
        Result<String> result;
        Map<String, Object> map = new HashMap<>();

        String searchId = hostId + ":" + positionId;

        try (Connection conn = ds.getConnection();
             PreparedStatement statement = conn.prepareStatement(sql)) {

            statement.setObject(1, UUID.fromString(hostId));
            statement.setString(2, positionId);

            try (ResultSet resultSet = statement.executeQuery()) {
                if (resultSet.next()) {
                    map.put("hostId", resultSet.getObject("host_id", UUID.class));
                    map.put("positionId", resultSet.getString("position_id"));
                    map.put("positionDesc", resultSet.getString("position_desc"));
                    map.put("inheritToAncestor", resultSet.getString("inherit_to_ancestor"));
                    map.put("inheritToSibling", resultSet.getString("inherit_to_sibling"));
                    map.put("aggregateVersion", resultSet.getLong("aggregate_version"));
                    map.put("active", resultSet.getBoolean("active"));
                    map.put("updateUser", resultSet.getString("update_user"));
                    map.put("updateTs", resultSet.getObject("update_ts") != null ? resultSet.getObject("update_ts", OffsetDateTime.class) : null);

                    result = Success.of(JsonMapper.toJson(map));
                } else {
                    result = Failure.of(new Status(OBJECT_NOT_FOUND, "position", searchId));
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
    public Result<String> queryPositionLabel(String hostId) {
        final String sql = "SELECT position_id from position_t WHERE host_id = ? AND active = TRUE";
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
    public Result<String> queryPositionPermission(int offset, int limit, String filtersJson, String globalFilter, String sortingJson, boolean active, String hostId) {
        Result<String> result;
        final Map<String, String> columnMap = new HashMap<>(Map.of(
                "hostId", "pp.host_id",
                "positionId", "pp.position_id",
                "apiVersionId", "av.api_version_id",
                "apiId", "av.api_id",
                "apiVersion", "av.api_version",
                "endpointId", "ae.endpoint_id",
                "endpoint", "ae.endpoint",
                "aggregateVersion", "pp.aggregate_version",
                "updateUser", "pp.update_user",
                "updateTs", "pp.update_ts"
        ));
        columnMap.put("inheritToAncestor", "p.inherit_to_ancestor");
        columnMap.put("inheritToSibling", "p.inherit_to_sibling");
        columnMap.put("active", "pp.active");

        List<Map<String, Object>> filters = parseJsonList(filtersJson);
        List<Map<String, Object>> sorting = parseJsonList(sortingJson);

        String s =
            """
                SELECT COUNT(*) OVER () AS total,
                pp.host_id, pp.position_id, p.inherit_to_ancestor, p.inherit_to_sibling,
                av.api_version_id, av.api_id, av.api_version, ae.endpoint_id, ae.endpoint,
                pp.aggregate_version, pp.update_user, pp.update_ts, pp.active
                FROM position_permission_t pp
                JOIN position_t p ON pp.position_id = p.position_id
                JOIN api_endpoint_t ae ON pp.host_id = ae.host_id AND pp.endpoint_id = ae.endpoint_id
                JOIN api_version_t av ON ae.host_id = av.host_id AND ae.api_version_id = av.api_version_id
                WHERE pp.host_id = ?
            """;

        List<Object> parameters = new ArrayList<>();
        parameters.add(UUID.fromString(hostId));

        String activeClause = SqlUtil.buildMultiTableActiveClause(active, "pp", "p", "ae", "av");
        String[] searchColumns = {"pp.position_id", "ae.endpoint"};
        String sqlBuilder = s + activeClause +
                dynamicFilter(Arrays.asList("pp.host_id", "av.api_version_id", "ae.endpoint_id"), Arrays.asList(searchColumns), filters, columnMap, parameters) +
                globalFilter(globalFilter, searchColumns, parameters) +
                dynamicSorting("pp.position_id, av.api_id, av.api_version, ae.endpoint", sorting, columnMap) +
                "\nLIMIT ? OFFSET ?";

        parameters.add(limit);
        parameters.add(offset);

        if(logger.isTraceEnabled()) logger.trace("queryPositionPermission sql: {}", sqlBuilder);
        int total = 0;
        List<Map<String, Object>> positionPermissions = new ArrayList<>();


        try (final Connection conn = ds.getConnection(); PreparedStatement preparedStatement = conn.prepareStatement(sqlBuilder)) {
            populateParameters(preparedStatement, parameters);
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
                    map.put("apiVersionId", resultSet.getString("api_version_id"));
                    map.put("apiId", resultSet.getString("api_id"));
                    map.put("apiVersion", resultSet.getString("api_version"));
                    map.put("endpointId", resultSet.getString("endpoint_id"));
                    map.put("endpoint", resultSet.getString("endpoint"));
                    map.put("aggregateVersion", resultSet.getLong("aggregate_version"));
                    map.put("updateUser", resultSet.getString("update_user"));
                    map.put("updateTs", resultSet.getObject("update_ts") != null ? resultSet.getObject("update_ts", OffsetDateTime.class) : null);
                    map.put("active", resultSet.getBoolean("active"));
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
    public Result<String> queryPositionUser(int offset, int limit, String filtersJson, String globalFilter, String sortingJson, boolean active, String hostId) {
        Result<String> result;
        final Map<String, String> columnMap = new HashMap<>(Map.of(
                "hostId", "ep.host_id",
                "positionId", "ep.position_id",
                "startTs", "ep.start_ts",
                "endTs", "ep.end_ts",
                "aggregateVersion", "ep.aggregate_version",
                "updateUser", "ep.update_user",
                "updateTs", "ep.update_ts",
                "userId", "u.user_id",
                "email", "u.email",
                "userType", "u.user_type"
        ));
        columnMap.put("entityId", "e.employee_id");
        columnMap.put("firstName", "u.first_name");
        columnMap.put("lastName", "u.last_name");
        columnMap.put("managerId", "e.manager_id");
        columnMap.put("positionType", "ep.position_type");
        columnMap.put("active", "ep.active");


        List<Map<String, Object>> filters = parseJsonList(filtersJson);
        List<Map<String, Object>> sorting = parseJsonList(sortingJson);

        String s =
           """
                SELECT COUNT(*) OVER () AS total,
                ep.host_id, ep.position_id, ep.position_type,
                ep.start_ts, ep.end_ts, u.user_id,
                ep.aggregate_version, ep.update_user, ep.update_ts,
                u.email, u.user_type, e.employee_id AS entity_id,
                e.manager_id, u.first_name, u.last_name, ep.active
                FROM user_t u
                INNER JOIN
                    employee_t e ON u.user_id = e.user_id AND u.user_type = 'E'
                INNER JOIN
                    user_position_t ep ON ep.user_id = u.user_id
                WHERE ep.host_id = ?
            """;

        List<Object> parameters = new ArrayList<>();
        parameters.add(UUID.fromString(hostId));

        String activeClause = SqlUtil.buildMultiTableActiveClause(active, "u", "e", "ep");
        String[] searchColumns = {"ep.position_id", "u.email", "u.first_name", "u.last_name"};
        String sqlBuilder = s + activeClause +
                dynamicFilter(Arrays.asList("ep.host_id", "u.user_id"), Arrays.asList(searchColumns), filters, columnMap, parameters) +
                globalFilter(globalFilter, searchColumns, parameters) +
                dynamicSorting("ep.position_id, u.user_id", sorting, columnMap) +
                "\nLIMIT ? OFFSET ?";

        parameters.add(limit);
        parameters.add(offset);

        if(logger.isTraceEnabled()) logger.trace("queryPositionUser sql: {}", sqlBuilder);
        int total = 0;
        List<Map<String, Object>> positionUsers = new ArrayList<>();


        try (final Connection conn = ds.getConnection(); PreparedStatement preparedStatement = conn.prepareStatement(sqlBuilder)) {
            populateParameters(preparedStatement, parameters);
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
                    map.put("startTs", resultSet.getObject("start_ts") != null ? resultSet.getObject("start_ts", OffsetDateTime.class) : null);
                    map.put("endTs", resultSet.getObject("end_ts") != null ? resultSet.getObject("end_ts", OffsetDateTime.class) : null);
                    map.put("userId", resultSet.getObject("user_id", UUID.class));
                    map.put("entityId", resultSet.getString("entity_id"));
                    map.put("email", resultSet.getString("email"));
                    map.put("firstName", resultSet.getString("first_name"));
                    map.put("lastName", resultSet.getString("last_name"));
                    map.put("userType", resultSet.getString("user_type"));
                    map.put("updateUser", resultSet.getString("update_user"));
                    map.put("updateTs", resultSet.getObject("update_ts") != null ? resultSet.getObject("update_ts", OffsetDateTime.class) : null);
                    map.put("aggregateVersion", resultSet.getLong("aggregate_version"));
                    map.put("active", resultSet.getBoolean("active"));
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
    public Result<String> getPositionUserById(String hostId, String positionId, String userId) {
        final String sql =
                """
                SELECT host_id, user_id, position_id, position_type, start_ts, end_ts,
                aggregate_version, active, update_user, update_ts
                FROM user_position_t
                WHERE host_id = ? AND position_id = ? AND user_id = ?
                """;
        Result<String> result;
        Map<String, Object> map = new HashMap<>();

        String searchId = hostId + ":" + positionId + ":" + userId;

        try (Connection conn = ds.getConnection();
             PreparedStatement statement = conn.prepareStatement(sql)) {

            statement.setObject(1, UUID.fromString(hostId));
            statement.setString(2, positionId);
            // Assuming employee_id in the DB is a UUID/VARCHAR that can be set by the employeeId parameter
            statement.setObject(3, UUID.fromString(userId));

            try (ResultSet resultSet = statement.executeQuery()) {
                if (resultSet.next()) {
                    map.put("hostId", resultSet.getObject("host_id", UUID.class));
                    map.put("userId", resultSet.getObject("user_id", UUID.class));
                    map.put("positionId", resultSet.getString("position_id"));
                    map.put("positionType", resultSet.getString("position_type"));
                    map.put("startTs", resultSet.getObject("start_ts") != null ? resultSet.getObject("start_ts", OffsetDateTime.class) : null);
                    map.put("endTs", resultSet.getObject("end_ts") != null ? resultSet.getObject("end_ts", OffsetDateTime.class) : null);
                    map.put("aggregateVersion", resultSet.getLong("aggregate_version"));
                    map.put("active", resultSet.getBoolean("active"));
                    map.put("updateUser", resultSet.getString("update_user"));
                    map.put("updateTs", resultSet.getObject("update_ts") != null ? resultSet.getObject("update_ts", OffsetDateTime.class) : null);

                    result = Success.of(JsonMapper.toJson(map));
                } else {
                    result = Failure.of(new Status(OBJECT_NOT_FOUND, "user_position", searchId));
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
    public void createPositionPermission(Connection conn, Map<String, Object> event) throws SQLException, Exception {
        // Use UPSERT: INSERT ON CONFLICT DO UPDATE
        // This handles:
        // 1. First time insert (no conflict).
        // 2. Re-creation (conflict on host_id, position_id, endpoint_id) -> UPDATE the existing soft-deleted row (setting active=TRUE and new version).

        final String sql =
                """
                INSERT INTO position_permission_t (
                    host_id,
                    position_id,
                    endpoint_id,
                    update_user,
                    update_ts,
                    aggregate_version,
                    active
                ) VALUES (?, ?, ?, ?, ?, ?, TRUE)
                ON CONFLICT (host_id, position_id, endpoint_id) DO UPDATE
                SET update_user = EXCLUDED.update_user,
                    update_ts = EXCLUDED.update_ts,
                    aggregate_version = EXCLUDED.aggregate_version,
                    active = TRUE
                -- OCC/IDM: Only update if the incoming event is newer
                WHERE position_permission_t.aggregate_version < EXCLUDED.aggregate_version AND position_permission_t.active = FALSE
                """;

        // Note: The original code uses a non-standard map retrieval: Map<String, Object> map = (Map<String, Object>)event.get(PortalConstants.DATA);
        // Assuming SqlUtil.extractEventData(event) is the correct utility based on other methods.
        Map<String, Object> map = SqlUtil.extractEventData(event);

        String hostId = (String)map.get("hostId");
        String positionId = (String)map.get("positionId");
        String endpointId = (String)map.get("endpointId");
        long newAggregateVersion = SqlUtil.getNewAggregateVersion(event);

        try (PreparedStatement statement = conn.prepareStatement(sql)) {
            // INSERT values (6 placeholders + active=TRUE in SQL, total 6 dynamic values)
            // 1: host_id
            statement.setObject(1, UUID.fromString(hostId));
            // 2: position_id
            statement.setString(2, positionId);
            // 3: endpoint_id
            statement.setObject(3, UUID.fromString(endpointId));
            // 4: update_user
            statement.setString(4, (String)event.get(Constants.USER));
            // 5: update_ts
            statement.setObject(5, OffsetDateTime.parse((String)event.get(CloudEventV1.TIME)));
            // 6: aggregate_version
            statement.setLong(6, newAggregateVersion);

            int count = statement.executeUpdate();
            if (count == 0) {
                // count=0 means the ON CONFLICT clause was hit, BUT the WHERE clause (aggregate_version < EXCLUDED.aggregate_version) failed.
                // This is the desired idempotent/out-of-order protection behavior. Log and ignore.
                logger.warn("Creation/Reactivation skipped for hostId {} positionId {} endpointId {} aggregateVersion {}. A newer or same version already exists.", hostId, positionId, endpointId, newAggregateVersion);
            }
        } catch (SQLException e) {
            logger.error("SQLException during createPositionPermission for hostId {} positionId {} endpointId {} aggregateVersion {}: {}", hostId, positionId, endpointId, newAggregateVersion, e.getMessage(), e);
            throw e;
        } catch (Exception e) {
            logger.error("Exception during createPositionPermission for hostId {} positionId {} endpointId {} aggregateVersion {}: {}", hostId, positionId, endpointId, newAggregateVersion, e.getMessage(), e);
            throw e;
        }
    }

    @Override
    public void deletePositionPermission(Connection conn, Map<String, Object> event) throws SQLException, Exception {
        // Use UPDATE to implement Soft Delete (setting active = FALSE).
        // OCC/IDM is enforced by checking aggregate_version < newAggregateVersion.
        final String sql =
                """
                UPDATE position_permission_t
                SET active = FALSE,
                    update_user = ?,
                    update_ts = ?,
                    aggregate_version = ?
                WHERE host_id = ?
                  AND position_id = ?
                  AND endpoint_id = ?
                  AND aggregate_version < ?
                """; // <<< CRITICAL: Added aggregate_version < ? to enforce monotonicity (OCC/IDM)

        // Note: The original code uses a non-standard map retrieval: Map<String, Object> map = (Map<String, Object>)event.get(PortalConstants.DATA);
        // Assuming SqlUtil.extractEventData(event) is the correct utility based on other methods.
        Map<String, Object> map = SqlUtil.extractEventData(event);

        String hostId = (String)map.get("hostId");
        String positionId = (String)map.get("positionId");
        String endpointId = (String)map.get("endpointId");
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

            // WHERE conditions (4 placeholders)
            // 4: host_id
            statement.setObject(4, UUID.fromString(hostId));
            // 5: position_id
            statement.setString(5, positionId);
            // 6: endpoint_id
            statement.setObject(6, UUID.fromString(endpointId));
            // 7: aggregate_version < ? (new version for OCC/IDM check)
            statement.setLong(7, newAggregateVersion);

            int count = statement.executeUpdate();
            if (count == 0) {
                // If 0 rows updated, it means:
                // 1. The record was not found (already deleted or never existed).
                // 2. The OCC/IDM check failed (aggregate_version >= newAggregateVersion).
                // We IGNORE the failure and log a warning, as this is the desired idempotent/monotonic behavior.
                logger.warn("Soft delete skipped for hostId {} positionId {} endpointId {} aggregateVersion {}. Record not found or a newer/same version already exists.", hostId, positionId, endpointId, newAggregateVersion);
            }
        } catch (SQLException e) {
            logger.error("SQLException during deletePositionPermission for hostId {} positionId {} endpointId {} aggregateVersion {}: {}", hostId, positionId, endpointId, newAggregateVersion, e.getMessage(), e);
            throw e;
        } catch (Exception e) {
            logger.error("Exception during deletePositionPermission for hostId {} positionId {} endpointId {} aggregateVersion {}: {}", hostId, positionId, endpointId, newAggregateVersion, e.getMessage(), e);
            throw e;
        }
    }

    @Override
    public void createPositionUser(Connection conn, Map<String, Object> event) throws SQLException, Exception {
        // Use UPSERT: INSERT ON CONFLICT DO UPDATE
        // This handles:
        // 1. First time insert (no conflict).
        // 2. Re-creation (conflict on host_id, user_id, position_id) -> UPDATE the existing soft-deleted row (setting active=TRUE and new version).
        // position_type is hardcoded to 'P' (Primary/Own position) for a create event.

        final String sql =
                """
                INSERT INTO user_position_t (
                    host_id,
                    user_id,
                    position_id,
                    position_type,
                    start_ts,
                    end_ts,
                    update_user,
                    update_ts,
                    aggregate_version,
                    active
                ) VALUES (?, ?, ?, 'P', ?, ?, ?, ?, ?, TRUE)
                ON CONFLICT (host_id, user_id, position_id) DO UPDATE
                SET start_ts = EXCLUDED.start_ts,
                    end_ts = EXCLUDED.end_ts,
                    update_user = EXCLUDED.update_user,
                    update_ts = EXCLUDED.update_ts,
                    aggregate_version = EXCLUDED.aggregate_version,
                    active = TRUE
                -- OCC/IDM: Only update if the incoming event is newer
                WHERE user_position_t.aggregate_version < EXCLUDED.aggregate_version AND user_position_t.active = FALSE
                """;

        // Note: The original code uses a non-standard map retrieval: Map<String, Object> map = (Map<String, Object>)event.get(PortalConstants.DATA);
        // Assuming SqlUtil.extractEventData(event) is the correct utility based on other methods.
        Map<String, Object> map = SqlUtil.extractEventData(event);

        String hostId = (String)map.get("hostId");
        String positionId = (String)map.get("positionId");
        String userId = (String)map.get("userId");
        long newAggregateVersion = SqlUtil.getNewAggregateVersion(event);

        try (PreparedStatement statement = conn.prepareStatement(sql)) {
            // INSERT values (8 placeholders in VALUES clause)
            // 1: host_id
            statement.setObject(1, UUID.fromString(hostId));
            // 2: user_id
            statement.setObject(2, UUID.fromString(userId));
            // 3: position_id
            statement.setString(3, positionId);
            // 4: start_ts (skipping position_type as it's hardcoded 'P')
            String startTs = (String)map.get("startTs");
            if(startTs != null && !startTs.isEmpty())
                statement.setObject(4, OffsetDateTime.parse(startTs));
            else
                statement.setNull(4, Types.TIMESTAMP_WITH_TIMEZONE); // Use Types.TIMESTAMP_WITH_TIMEZONE

            // 5: end_ts
            String endTs = (String)map.get("endTs");
            if (endTs != null && !endTs.isEmpty()) {
                statement.setObject(5, OffsetDateTime.parse(endTs));
            } else {
                statement.setNull(5, Types.TIMESTAMP_WITH_TIMEZONE); // Use Types.TIMESTAMP_WITH_TIMEZONE
            }

            // 6: update_user
            statement.setString(6, (String)event.get(Constants.USER));
            // 7: update_ts
            statement.setObject(7, OffsetDateTime.parse((String)event.get(CloudEventV1.TIME)));
            // 8: aggregate_version
            statement.setLong(8, newAggregateVersion); // skipping active as it's hardcoded TRUE

            int count = statement.executeUpdate();
            if (count == 0) {
                // count=0 means the ON CONFLICT clause was hit, BUT the WHERE clause (aggregate_version < EXCLUDED.aggregate_version) failed.
                // This is the desired idempotent/out-of-order protection behavior. Log and ignore.
                logger.warn("Creation/Reactivation skipped for hostId {} positionId {} userId {} aggregateVersion {}. A newer or same version already exists.", hostId, positionId, userId, newAggregateVersion);
            }
        } catch (SQLException e) {
            logger.error("SQLException during createPositionUser for hostId {} positionId {} userId {} and aggregateVersion {}: {}", hostId, positionId, userId, newAggregateVersion, e.getMessage(), e);
            throw e;
        } catch (Exception e) {
            logger.error("Exception during createPositionUser for hostId {} positionId {} userId {} and aggregateVersion {}: {}", hostId, positionId, userId, newAggregateVersion, e.getMessage(), e);
            throw e;
        }
    }

    @Override
    public void updatePositionUser(Connection conn, Map<String, Object> event) throws SQLException, Exception {
        // We attempt to update the record IF the incoming event's aggregate_version is greater than the current projection's version.
        // This enforces Idempotence (IDM) and Optimistic Concurrency Control (OCC) by ensuring version monotonicity.
        // We explicitly set active = TRUE as an UPDATE event implies the assignment should be active.
        // Assuming this update is only for the directly assigned position ('P').
        final String sql =
                """
                UPDATE user_position_t
                SET start_ts = ?,
                    end_ts = ?,
                    update_user = ?,
                    update_ts = ?,
                    aggregate_version = ?,
                    active = TRUE
                WHERE host_id = ?
                  AND position_id = ?
                  AND user_id = ?
                  AND position_type = 'P' -- Assuming only primary assignments are updated via this method
                  AND aggregate_version < ?
                """; // <<< CRITICAL: Changed aggregate_version = ? to aggregate_version < ? to enforce monotonicity (OCC/IDM)

        // Note: The original code uses a non-standard map retrieval: Map<String, Object> map = (Map<String, Object>)event.get(PortalConstants.DATA);
        // Assuming SqlUtil.extractEventData(event) is the correct utility based on other methods.
        Map<String, Object> map = SqlUtil.extractEventData(event);

        String hostId = (String)map.get("hostId");
        String positionId = (String)map.get("positionId");
        String userId = (String)map.get("userId");
        long newAggregateVersion = SqlUtil.getNewAggregateVersion(event);

        try (PreparedStatement statement = conn.prepareStatement(sql)) {
            // SET values (5 dynamic values + active = TRUE in SQL)

            // 1: start_ts
            String startTs = (String)map.get("startTs");
            if(startTs != null && !startTs.isEmpty())
                statement.setObject(1, OffsetDateTime.parse(startTs));
            else
                statement.setNull(1, Types.TIMESTAMP_WITH_TIMEZONE); // Use Types.TIMESTAMP_WITH_TIMEZONE

            // 2: end_ts
            String endTs = (String)map.get("endTs");
            if (endTs != null && !endTs.isEmpty()) {
                statement.setObject(2, OffsetDateTime.parse(endTs));
            } else {
                statement.setNull(2, Types.TIMESTAMP_WITH_TIMEZONE); // Use Types.TIMESTAMP_WITH_TIMEZONE
            }

            // 3: update_user
            statement.setString(3, (String)event.get(Constants.USER));
            // 4: update_ts
            statement.setObject(4, OffsetDateTime.parse((String)event.get(CloudEventV1.TIME)));
            // 5: aggregate_version
            statement.setLong(5, newAggregateVersion);

            // WHERE conditions (4 placeholders)
            // 6: host_id
            statement.setObject(6, UUID.fromString(hostId));
            // 7: position_id
            statement.setString(7, positionId);
            // 8: user_id
            statement.setObject(8, UUID.fromString(userId));
            // position_type = 'P' (hardcoded in SQL)
            // 9: aggregate_version < ? (new version for OCC/IDM check)
            statement.setLong(9, newAggregateVersion);

            int count = statement.executeUpdate();
            if (count == 0) {
                // If 0 rows updated, it means the record was either not found (or not 'P' type)
                // OR aggregate_version >= newAggregateVersion (OCC/IDM check failed).
                // We IGNORE the failure and log a warning, as this is the desired idempotent/monotonic behavior.
                logger.warn("Update skipped for hostId {} positionId {} userId {} aggregateVersion {}. Record not found, not a primary assignment, or a newer/same version already exists.", hostId, positionId, userId, newAggregateVersion);
            }
        } catch (SQLException e) {
            logger.error("SQLException during updatePositionUser for hostId {} positionId {} userId {} aggregateVersion {}: {}", hostId, positionId, userId, newAggregateVersion, e.getMessage(), e);
            throw e;
        } catch (Exception e) {
            logger.error("Exception during updatePositionUser for hostId {} positionId {} userId {} aggregateVersion {}: {}", hostId, positionId, userId, newAggregateVersion, e.getMessage(), e);
            throw e;
        }
    }

    @Override
    public void deletePositionUser(Connection conn, Map<String, Object> event) throws SQLException, Exception {
        // Use UPDATE to implement Soft Delete (setting active = FALSE) on the directly assigned position ('P').
        // OCC/IDM is enforced by checking aggregate_version < newAggregateVersion.
        final String sql =
                """
                UPDATE user_position_t
                SET active = FALSE,
                    update_user = ?,
                    update_ts = ?,
                    aggregate_version = ?
                WHERE host_id = ?
                  AND position_id = ?
                  AND user_id = ?
                  AND position_type = 'P' -- Assuming delete is only for the primary assignment
                  AND aggregate_version < ?
                """; // <<< CRITICAL: Added aggregate_version < ? to enforce monotonicity (OCC/IDM)

        // Note: The original code uses a non-standard map retrieval: Map<String, Object> map = (Map<String, Object>)event.get(PortalConstants.DATA);
        // Assuming SqlUtil.extractEventData(event) is the correct utility based on other methods.
        Map<String, Object> map = SqlUtil.extractEventData(event);

        String hostId = (String)map.get("hostId");
        String positionId = (String)map.get("positionId");
        String userId = (String)map.get("userId");
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

            // WHERE conditions (4 placeholders + position_type = 'P')
            // 4: host_id
            statement.setObject(4, UUID.fromString(hostId));
            // 5: position_id
            statement.setString(5, positionId);
            // 6: user_id
            statement.setObject(6, UUID.fromString(userId));
            // 7: aggregate_version < ? (new version for OCC/IDM check)
            statement.setLong(7, newAggregateVersion);

            int count = statement.executeUpdate();
            if (count == 0) {
                // If 0 rows updated, it means:
                // 1. The record was not found (already deleted or never existed, or not 'P' type).
                // 2. The OCC/IDM check failed (aggregate_version >= newAggregateVersion).
                // We IGNORE the failure and log a warning, as this is the desired idempotent/monotonic behavior.
                logger.warn("Soft delete skipped for hostId {} positionId {} userId {} aggregateVersion {}. Record not found, not a primary assignment, or a newer/same version already exists.", hostId, positionId, userId, newAggregateVersion);
            }
        } catch (SQLException e) {
            logger.error("SQLException during deletePositionUser for hostId {} positionId {} userId {} aggregateVersion {}: {}", hostId, positionId, userId, newAggregateVersion, e.getMessage(), e);
            throw e;
        } catch (Exception e) {
            logger.error("Exception during deletePositionUser for hostId {} positionId {} userId {} aggregateVersion {}: {}", hostId, positionId, userId, newAggregateVersion, e.getMessage(), e);
            throw e;
        }
    }

    @Override
    public Result<String> queryPositionRowFilter(int offset, int limit, String filtersJson, String globalFilter, String sortingJson, boolean active, String hostId) {
        Result<String> result;
        final Map<String, String> columnMap = new HashMap<>(Map.of(
                "hostId", "o.host_id",
                "positionId", "o.position_id",
                "endpointId", "p.endpoint_id",
                "endpoint", "ae.endpoint",
                "apiVersionId", "av.api_version_id",
                "apiId", "av.api_id",
                "apiVersion", "av.api_version",
                "colName", "p.col_name",
                "operator", "p.operator",
                "colValue", "p.col_value"
        ));
        columnMap.put("updateUser", "p.update_user");
        columnMap.put("updateTs", "p.update_ts");
        columnMap.put("aggregateVersion", "p.aggregate_version");
        columnMap.put("active", "p.active");


        List<Map<String, Object>> filters = parseJsonList(filtersJson);
        List<Map<String, Object>> sorting = parseJsonList(sortingJson);

        String s =
            """
            SELECT COUNT(*) OVER () AS total, o.host_id, o.position_id, p.endpoint_id, ae.endpoint,
            av.api_version_id, av.api_id, av.api_version, p.col_name, p.operator, p.col_value,
            p.update_user, p.update_ts, p.aggregate_version, p.active
            FROM position_row_filter_t p
            JOIN position_t o ON o.host_id = p.host_id AND o.position_id = p.position_id
            JOIN api_endpoint_t ae ON ae.host_id = p.host_id AND ae.endpoint_id = p.endpoint_id
            JOIN api_version_t av ON av.host_id = ae.host_id AND av.api_version_id = ae.api_version_id
            WHERE p.host_id = ?
            """;

        List<Object> parameters = new ArrayList<>();
        parameters.add(UUID.fromString(hostId));

        String activeClause = SqlUtil.buildMultiTableActiveClause(active, "p", "o", "ae", "av");
        String[] searchColumns = {"o.position_id", "ae.endpoint", "p.col_name", "p.col_value"};
        String sqlBuilder = s + activeClause +
                dynamicFilter(Arrays.asList("o.host_id", "p.endpoint_id", "av.api_version_id"), Arrays.asList(searchColumns), filters, columnMap, parameters) +
                globalFilter(globalFilter, searchColumns, parameters) +
                dynamicSorting("o.position_id, av.api_id, av.api_version, ae.endpoint, p.col_name, p.operator", sorting, columnMap) +
                "\nLIMIT ? OFFSET ?";

        parameters.add(limit);
        parameters.add(offset);

        if(logger.isTraceEnabled()) logger.trace("queryPositionRowFilter sql: {}", sqlBuilder);
        int total = 0;
        List<Map<String, Object>> positionRowFilters = new ArrayList<>();

        try (final Connection conn = ds.getConnection(); PreparedStatement preparedStatement = conn.prepareStatement(sqlBuilder)) {
            populateParameters(preparedStatement, parameters);
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
                    map.put("endpointId", resultSet.getObject("endpoint_id", UUID.class));
                    map.put("apiVersionId", resultSet.getObject("api_version_id", UUID.class));
                    map.put("apiId", resultSet.getString("api_id"));
                    map.put("apiVersion", resultSet.getString("api_version"));
                    map.put("endpoint", resultSet.getString("endpoint"));
                    map.put("colName", resultSet.getString("col_name"));
                    map.put("operator", resultSet.getString("operator"));
                    map.put("colValue", resultSet.getString("col_value"));
                    map.put("updateUser", resultSet.getString("update_user"));
                    map.put("updateTs", resultSet.getObject("update_ts") != null ? resultSet.getObject("update_ts", OffsetDateTime.class) : null);
                    map.put("aggregateVersion", resultSet.getLong("aggregate_version"));
                    map.put("active", resultSet.getBoolean("active"));
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
    public Result<String> getPositionRowFilterById(String hostId, String positionId, String endpointId, String colName) {
        final String sql =
                """
                SELECT host_id, position_id, endpoint_id, col_name, operator, col_value,
                aggregate_version, active, update_user, update_ts
                FROM position_row_filter_t
                WHERE host_id = ? AND position_id = ? AND endpoint_id = ? AND col_name = ?
                """;
        Result<String> result;
        Map<String, Object> map = new HashMap<>();

        String searchId = hostId + ":" + positionId + ":" + endpointId + ":" + colName;

        try (Connection conn = ds.getConnection();
             PreparedStatement statement = conn.prepareStatement(sql)) {

            statement.setObject(1, UUID.fromString(hostId));
            statement.setString(2, positionId);
            statement.setObject(3, UUID.fromString(endpointId));
            statement.setString(4, colName);

            try (ResultSet resultSet = statement.executeQuery()) {
                if (resultSet.next()) {
                    map.put("hostId", resultSet.getObject("host_id", UUID.class));
                    map.put("positionId", resultSet.getString("position_id"));
                    map.put("endpointId", resultSet.getObject("endpoint_id", UUID.class));
                    map.put("colName", resultSet.getString("col_name"));
                    map.put("operator", resultSet.getString("operator"));
                    map.put("colValue", resultSet.getString("col_value"));
                    map.put("aggregateVersion", resultSet.getLong("aggregate_version"));
                    map.put("active", resultSet.getBoolean("active"));
                    map.put("updateUser", resultSet.getString("update_user"));
                    map.put("updateTs", resultSet.getObject("update_ts") != null ? resultSet.getObject("update_ts", OffsetDateTime.class) : null);

                    result = Success.of(JsonMapper.toJson(map));
                } else {
                    result = Failure.of(new Status(OBJECT_NOT_FOUND, "position_row_filter", searchId));
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
    public void createPositionRowFilter(Connection conn, Map<String, Object> event) throws SQLException, Exception {
        // Use UPSERT: INSERT ON CONFLICT DO UPDATE
        // This handles:
        // 1. First time insert (no conflict).
        // 2. Re-creation (conflict on host_id, position_id, endpoint_id, col_name) -> UPDATE the existing soft-deleted row (setting active=TRUE and new version).

        final String sql =
                """
                INSERT INTO position_row_filter_t (
                    host_id,
                    position_id,
                    endpoint_id,
                    col_name,
                    operator,
                    col_value,
                    update_user,
                    update_ts,
                    aggregate_version,
                    active
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, TRUE)
                ON CONFLICT (host_id, position_id, endpoint_id, col_name) DO UPDATE
                SET operator = EXCLUDED.operator,
                    col_value = EXCLUDED.col_value,
                    update_user = EXCLUDED.update_user,
                    update_ts = EXCLUDED.update_ts,
                    aggregate_version = EXCLUDED.aggregate_version,
                    active = TRUE
                -- OCC/IDM: Only update if the incoming event is newer
                WHERE position_row_filter_t.aggregate_version < EXCLUDED.aggregate_version AND position_row_filter_t.active = FALSE
                """;

        // Note: The original code uses a non-standard map retrieval: Map<String, Object> map = (Map<String, Object>)event.get(PortalConstants.DATA);
        // Assuming SqlUtil.extractEventData(event) is the correct utility based on other methods.
        Map<String, Object> map = SqlUtil.extractEventData(event);

        String hostId = (String)map.get("hostId");
        String positionId = (String)map.get("positionId");
        String endpointId = (String)map.get("endpointId");
        String colName = (String)map.get("colName");
        long newAggregateVersion = SqlUtil.getNewAggregateVersion(event);

        try (PreparedStatement statement = conn.prepareStatement(sql)) {
            // INSERT values (9 placeholders + active=TRUE in SQL, total 9 dynamic values)
            // 1: host_id
            statement.setObject(1, UUID.fromString(hostId));
            // 2: position_id
            statement.setString(2, positionId);
            // 3: endpoint_id
            statement.setObject(3, UUID.fromString(endpointId));
            // 4: col_name
            statement.setString(4, colName);
            // 5: operator
            statement.setString(5, (String)map.get("operator"));
            // 6: col_value
            statement.setString(6, (String)map.get("colValue"));
            // 7: update_user
            statement.setString(7, (String)event.get(Constants.USER));
            // 8: update_ts
            statement.setObject(8, OffsetDateTime.parse((String)event.get(CloudEventV1.TIME)));
            // 9: aggregate_version
            statement.setLong(9, newAggregateVersion);

            int count = statement.executeUpdate();
            if (count == 0) {
                // count=0 means the ON CONFLICT clause was hit, BUT the WHERE clause (aggregate_version < EXCLUDED.aggregate_version) failed.
                // This is the desired idempotent/out-of-order protection behavior. Log and ignore.
                logger.warn("Creation/Reactivation skipped for hostId {} positionId {} endpointId {} colName {} aggregateVersion {}. A newer or same version already exists.", hostId, positionId, endpointId, colName, newAggregateVersion);
            }
        } catch (SQLException e) {
            logger.error("SQLException during createPositionRowFilter for hostId {} positionId {} endpointId {} colName {} aggregateVersion {}: {}", hostId, positionId, endpointId, colName, newAggregateVersion, e.getMessage(), e);
            throw e;
        } catch (Exception e) {
            logger.error("Exception during createPositionRowFilter for hostId {} positionId {} endpointId {} colName {} aggregateVersion {}: {}", hostId, positionId, endpointId, colName, newAggregateVersion, e.getMessage(), e);
            throw e;
        }
    }

    @Override
    public void updatePositionRowFilter(Connection conn, Map<String, Object> event) throws SQLException, Exception {
        // We attempt to update the record IF the incoming event's aggregate_version is greater than the current projection's version.
        // This enforces Idempotence (IDM) and Optimistic Concurrency Control (OCC) by ensuring version monotonicity.
        // We explicitly set active = TRUE as an UPDATE event implies the row filter should be active.
        final String sql =
                """
                UPDATE position_row_filter_t
                SET
                    operator = ?,
                    col_value = ?,
                    update_user = ?,
                    update_ts = ?,
                    aggregate_version = ?,
                    active = TRUE
                WHERE
                    host_id = ?
                    AND position_id = ?
                    AND endpoint_id = ?
                    AND col_name = ?
                    AND aggregate_version < ?
                """; // <<< CRITICAL: Changed aggregate_version = ? to aggregate_version < ? to enforce monotonicity (OCC/IDM)

        // Note: The original code uses a non-standard map retrieval: Map<String, Object> map = (Map<String, Object>)event.get(PortalConstants.DATA);
        // Assuming SqlUtil.extractEventData(event) is the correct utility based on other methods.
        Map<String, Object> map = SqlUtil.extractEventData(event);

        String hostId = (String)map.get("hostId");
        String positionId = (String)map.get("positionId");
        String endpointId = (String)map.get("endpointId");
        String colName = (String)map.get("colName");
        long newAggregateVersion = SqlUtil.getNewAggregateVersion(event);

        try (PreparedStatement statement = conn.prepareStatement(sql)) {
            // SET values (5 dynamic values + active = TRUE in SQL)
            // 1: operator
            statement.setString(1, (String)map.get("operator"));
            // 2: col_value
            statement.setString(2, (String)map.get("colValue"));
            // 3: update_user
            statement.setString(3, (String)event.get(Constants.USER));
            // 4: update_ts
            statement.setObject(4, OffsetDateTime.parse((String)event.get(CloudEventV1.TIME)));
            // 5: aggregate_version
            statement.setLong(5, newAggregateVersion);

            // WHERE conditions (5 placeholders)
            // 6: host_id
            statement.setObject(6, UUID.fromString(hostId));
            // 7: position_id
            statement.setString(7, positionId);
            // 8: endpoint_id
            statement.setObject(8, UUID.fromString(endpointId));
            // 9: col_name
            statement.setString(9, colName);
            // 10: aggregate_version < ? (new version for OCC/IDM check)
            statement.setLong(10, newAggregateVersion);

            int count = statement.executeUpdate();
            if (count == 0) {
                // If 0 rows updated, it means the record was either not found
                // OR aggregate_version >= newAggregateVersion (OCC/IDM check failed).
                // We IGNORE the failure and log a warning, as this is the desired idempotent/monotonic behavior.
                logger.warn("Update skipped for hostId {} positionId {} endpointId {} colName {} aggregateVersion {}. Record not found or a newer/same version already exists.", hostId, positionId, endpointId, colName, newAggregateVersion);
            }
        } catch (SQLException e) {
            logger.error("SQLException during updatePositionRowFilter for hostId {} positionId {} endpointId {} colName {} aggregateVersion {}: {}", hostId, positionId, endpointId, colName, newAggregateVersion, e.getMessage(), e);
            throw e;
        } catch (Exception e) {
            logger.error("Exception during updatePositionRowFilter for hostId {} positionId {} endpointId {} colName {} aggregateVersion {}: {}", hostId, positionId, endpointId, colName, newAggregateVersion, e.getMessage(), e);
            throw e;
        }
    }

    @Override
    public void deletePositionRowFilter(Connection conn, Map<String, Object> event) throws SQLException, Exception {
        // Use UPDATE to implement Soft Delete (setting active = FALSE).
        // OCC/IDM is enforced by checking aggregate_version < newAggregateVersion.
        final String sql =
                """
                UPDATE position_row_filter_t
                SET active = FALSE,
                    update_user = ?,
                    update_ts = ?,
                    aggregate_version = ?
                WHERE host_id = ?
                  AND position_id = ?
                  AND endpoint_id = ?
                  AND col_name = ?
                  AND aggregate_version < ?
                """; // <<< CRITICAL: Added aggregate_version < ? to enforce monotonicity (OCC/IDM)

        // Note: The original code uses a non-standard map retrieval: Map<String, Object> map = (Map<String, Object>)event.get(PortalConstants.DATA);
        // Assuming SqlUtil.extractEventData(event) is the correct utility based on other methods.
        Map<String, Object> map = SqlUtil.extractEventData(event);

        String hostId = (String)map.get("hostId");
        String positionId = (String)map.get("positionId");
        String endpointId = (String)map.get("endpointId");
        String colName = (String)map.get("colName");
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

            // WHERE conditions (5 placeholders)
            // 4: host_id
            statement.setObject(4, UUID.fromString(hostId));
            // 5: position_id
            statement.setString(5, positionId);
            // 6: endpoint_id
            statement.setObject(6, UUID.fromString(endpointId));
            // 7: col_name
            statement.setString(7, colName);
            // 8: aggregate_version < ? (new version for OCC/IDM check)
            statement.setLong(8, newAggregateVersion);

            int count = statement.executeUpdate();
            if (count == 0) {
                // If 0 rows updated, it means:
                // 1. The record was not found (already deleted or never existed).
                // 2. The OCC/IDM check failed (aggregate_version >= newAggregateVersion).
                // We IGNORE the failure and log a warning, as this is the desired idempotent/monotonic behavior.
                logger.warn("Soft delete skipped for hostId {} positionId {} endpointId {} colName {} aggregateVersion {}. Record not found or a newer/same version already exists.", hostId, positionId, endpointId, colName, newAggregateVersion);
            }
        } catch (SQLException e) {
            logger.error("SQLException during deletePositionRowFilter for hostId {} positionId {} endpointId {} colName {} aggregateVersion {}: {}", hostId, positionId, endpointId, colName, newAggregateVersion, e.getMessage(), e);
            throw e;
        } catch (Exception e) {
            logger.error("Exception during deletePositionRowFilter for hostId {} positionId {} endpointId {} colName {} aggregateVersion {}: {}", hostId, positionId, endpointId, colName, newAggregateVersion, e.getMessage(), e);
            throw e;
        }
    }

    @Override
    public Result<String> queryPositionColFilter(int offset, int limit, String filtersJson, String globalFilter, String sortingJson, boolean active, String hostId) {
        Result<String> result;
        final Map<String, String> columnMap = new HashMap<>(Map.of(
                "hostId", "o.host_id",
                "positionId", "o.position_id",
                "endpointId", "ae.endpoint_id",
                "endpoint", "ae.endpoint",
                "apiVersionId", "av.api_version_id",
                "apiId", "av.api_id",
                "apiVersion", "av.api_version",
                "columns", "cf.columns",
                "updateUser", "cf.update_user",
                "updateTs", "cf.update_ts"
        ));
        columnMap.put("aggregateVersion", "cf.aggregate_version");
        columnMap.put("active", "cf.active");


        List<Map<String, Object>> filters = parseJsonList(filtersJson);
        List<Map<String, Object>> sorting = parseJsonList(sortingJson);

        String s =
            """
            SELECT COUNT(*) OVER () AS total,
            o.host_id, o.position_id, av.api_version_id, av.api_id, av.api_version,
            ae.endpoint_id, ae.endpoint, cf.columns, cf.aggregate_version,
            cf.update_user, cf.update_ts, cf.active
            FROM position_col_filter_t cf
            JOIN position_t o ON o.position_id = cf.position_id
            JOIN api_endpoint_t ae ON cf.host_id = ae.host_id AND cf.endpoint_id = ae.endpoint_id
            JOIN api_version_t av ON ae.host_id = av.host_id AND ae.api_version_id = av.api_version_id
            WHERE cf.host_id = ?
            """;

        List<Object> parameters = new ArrayList<>();
        parameters.add(UUID.fromString(hostId));

        String activeClause = SqlUtil.buildMultiTableActiveClause(active, "cf", "o", "ae", "av");
        String[] searchColumns = {"o.position_id", "ae.endpoint", "cf.columns"};
        String sqlBuilder = s + activeClause +
                dynamicFilter(Arrays.asList("o.host_id", "av.api_version_id", "ae.endpoint_id"), Arrays.asList(searchColumns), filters, columnMap, parameters) +
                globalFilter(globalFilter, searchColumns, parameters) +
                dynamicSorting("o.position_id, av.api_id, av.api_version, ae.endpoint", sorting, columnMap) +
                "\nLIMIT ? OFFSET ?";

        parameters.add(limit);
        parameters.add(offset);

        if(logger.isTraceEnabled()) logger.trace("queryPositionColFilter sql: {}", sqlBuilder);
        int total = 0;
        List<Map<String, Object>> positionColFilters = new ArrayList<>();

        try (final Connection conn = ds.getConnection(); PreparedStatement preparedStatement = conn.prepareStatement(sqlBuilder)) {
            populateParameters(preparedStatement, parameters);
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
                    map.put("apiVersionId", resultSet.getObject("api_version_id", UUID.class));
                    map.put("endpointId", resultSet.getObject("endpoint_id", UUID.class));
                    map.put("apiId", resultSet.getString("api_id"));
                    map.put("apiVersion", resultSet.getString("api_version"));
                    map.put("endpoint", resultSet.getString("endpoint"));
                    map.put("columns", resultSet.getString("columns"));
                    map.put("updateUser", resultSet.getString("update_user"));
                    map.put("updateTs", resultSet.getObject("update_ts") != null ? resultSet.getObject("update_ts", OffsetDateTime.class) : null);
                    map.put("aggregateVersion", resultSet.getLong("aggregate_version"));
                    map.put("active", resultSet.getBoolean("active"));
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
    public Result<String> getPositionColFilterById(String hostId, String positionId, String endpointId) {
        final String sql =
                """
                SELECT host_id, position_id, endpoint_id, columns,
                aggregate_version, active, update_user, update_ts
                FROM position_col_filter_t
                WHERE host_id = ? AND position_id = ? AND endpoint_id = ?
                """;
        Result<String> result;
        Map<String, Object> map = new HashMap<>();

        String searchId = hostId + ":" + positionId + ":" + endpointId;

        try (Connection conn = ds.getConnection();
             PreparedStatement statement = conn.prepareStatement(sql)) {

            statement.setObject(1, UUID.fromString(hostId));
            statement.setString(2, positionId);
            statement.setObject(3, UUID.fromString(endpointId));

            try (ResultSet resultSet = statement.executeQuery()) {
                if (resultSet.next()) {
                    map.put("hostId", resultSet.getObject("host_id", UUID.class));
                    map.put("positionId", resultSet.getString("position_id"));
                    map.put("endpointId", resultSet.getObject("endpoint_id", UUID.class));
                    map.put("columns", resultSet.getString("columns"));
                    map.put("aggregateVersion", resultSet.getLong("aggregate_version"));
                    map.put("active", resultSet.getBoolean("active"));
                    map.put("updateUser", resultSet.getString("update_user"));
                    map.put("updateTs", resultSet.getObject("update_ts") != null ? resultSet.getObject("update_ts", OffsetDateTime.class) : null);

                    result = Success.of(JsonMapper.toJson(map));
                } else {
                    result = Failure.of(new Status(OBJECT_NOT_FOUND, "position_col_filter", searchId));
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
    public void createPositionColFilter(Connection conn, Map<String, Object> event) throws SQLException, Exception {
        // Use UPSERT: INSERT ON CONFLICT DO UPDATE
        // This handles:
        // 1. First time insert (no conflict).
        // 2. Re-creation (conflict on host_id, position_id, endpoint_id) -> UPDATE the existing soft-deleted row (setting active=TRUE and new version).

        final String sql =
                """
                INSERT INTO position_col_filter_t (
                    host_id,
                    position_id,
                    endpoint_id,
                    columns,
                    update_user,
                    update_ts,
                    aggregate_version,
                    active
                ) VALUES (?, ?, ?, ?, ?, ?, ?, TRUE)
                ON CONFLICT (host_id, position_id, endpoint_id) DO UPDATE
                SET columns = EXCLUDED.columns,
                    update_user = EXCLUDED.update_user,
                    update_ts = EXCLUDED.update_ts,
                    aggregate_version = EXCLUDED.aggregate_version,
                    active = TRUE
                -- OCC/IDM: Only update if the incoming event is newer
                WHERE position_col_filter_t.aggregate_version < EXCLUDED.aggregate_version AND position_col_filter_t.active = FALSE
                """;

        // Note: The original code uses a non-standard map retrieval: Map<String, Object> map = (Map<String, Object>)event.get(PortalConstants.DATA);
        // Assuming SqlUtil.extractEventData(event) is the correct utility based on other methods.
        Map<String, Object> map = SqlUtil.extractEventData(event);

        String hostId = (String)map.get("hostId");
        String positionId = (String)map.get("positionId");
        String endpointId = (String)map.get("endpointId");
        long newAggregateVersion = SqlUtil.getNewAggregateVersion(event);

        try (PreparedStatement statement = conn.prepareStatement(sql)) {
            // INSERT values (7 placeholders + active=TRUE in SQL, total 7 dynamic values)
            // 1: host_id
            statement.setObject(1, UUID.fromString(hostId));
            // 2: position_id
            statement.setString(2, positionId);
            // 3: endpoint_id
            statement.setObject(3, UUID.fromString(endpointId));
            // 4: columns
            statement.setString(4, (String)map.get("columns"));
            // 5: update_user
            statement.setString(5, (String)event.get(Constants.USER));
            // 6: update_ts
            statement.setObject(6, OffsetDateTime.parse((String)event.get(CloudEventV1.TIME)));
            // 7: aggregate_version
            statement.setLong(7, newAggregateVersion);

            int count = statement.executeUpdate();
            if (count == 0) {
                // count=0 means the ON CONFLICT clause was hit, BUT the WHERE clause (aggregate_version < EXCLUDED.aggregate_version) failed.
                // This is the desired idempotent/out-of-order protection behavior. Log and ignore.
                logger.warn("Creation/Reactivation skipped for hostId {} positionId {} endpointId {} aggregateVersion {}. A newer or same version already exists.", hostId, positionId, endpointId, newAggregateVersion);
            }
        } catch (SQLException e) {
            logger.error("SQLException during createPositionColFilter for hostId {} positionId {} endpointId {} aggregateVersion {}: {}", hostId, positionId, endpointId, newAggregateVersion, e.getMessage(), e);
            throw e;
        } catch (Exception e) {
            logger.error("Exception during createPositionColFilter for hostId {} positionId {} endpointId {} aggregateVersion {}: {}", hostId, positionId, endpointId, newAggregateVersion, e.getMessage(), e);
            throw e;
        }
    }

    @Override
    public void updatePositionColFilter(Connection conn, Map<String, Object> event) throws SQLException, Exception {
        // We attempt to update the record IF the incoming event's aggregate_version is greater than the current projection's version.
        // This enforces Idempotence (IDM) and Optimistic Concurrency Control (OCC) by ensuring version monotonicity.
        // We explicitly set active = TRUE as an UPDATE event implies the column filter should be active.
        final String sql =
                """
                UPDATE position_col_filter_t
                SET
                    columns = ?,
                    update_user = ?,
                    update_ts = ?,
                    aggregate_version = ?,
                    active = TRUE
                WHERE
                    host_id = ?
                    AND position_id = ?
                    AND endpoint_id = ?
                    AND aggregate_version < ?
                """; // <<< CRITICAL: Changed aggregate_version = ? to aggregate_version < ? to enforce monotonicity (OCC/IDM)

        // Note: The original code uses a non-standard map retrieval: Map<String, Object> map = (Map<String, Object>)event.get(PortalConstants.DATA);
        // Assuming SqlUtil.extractEventData(event) is the correct utility based on other methods.
        Map<String, Object> map = SqlUtil.extractEventData(event);

        String hostId = (String)map.get("hostId");
        String positionId = (String)map.get("positionId");
        String endpointId = (String)map.get("endpointId");
        long newAggregateVersion = SqlUtil.getNewAggregateVersion(event);

        try (PreparedStatement statement = conn.prepareStatement(sql)) {
            // SET values (4 dynamic values + active = TRUE in SQL)
            // 1: columns
            statement.setString(1, (String)map.get("columns"));
            // 2: update_user
            statement.setString(2, (String)event.get(Constants.USER));
            // 3: update_ts
            statement.setObject(3, OffsetDateTime.parse((String)event.get(CloudEventV1.TIME)));
            // 4: aggregate_version
            statement.setLong(4, newAggregateVersion);

            // WHERE conditions (4 placeholders)
            // 5: host_id
            statement.setObject(5, UUID.fromString(hostId));
            // 6: position_id
            statement.setString(6, positionId);
            // 7: endpoint_id
            statement.setObject(7, UUID.fromString(endpointId));
            // 8: aggregate_version < ? (new version for OCC/IDM check)
            statement.setLong(8, newAggregateVersion);

            int count = statement.executeUpdate();
            if (count == 0) {
                // If 0 rows updated, it means the record was either not found
                // OR aggregate_version >= newAggregateVersion (OCC/IDM check failed).
                // We IGNORE the failure and log a warning, as this is the desired idempotent/monotonic behavior.
                logger.warn("Update skipped for hostId {} positionId {} endpointId {} aggregateVersion {}. Record not found or a newer/same version already exists.", hostId, positionId, endpointId, newAggregateVersion);
            }
        } catch (SQLException e) {
            logger.error("SQLException during updatePositionColFilter for hostId {} positionId {} endpointId {} aggregateVersion {}: {}", hostId, positionId, endpointId, newAggregateVersion, e.getMessage(), e);
            throw e;
        } catch (Exception e) {
            logger.error("Exception during updatePositionColFilter for hostId {} positionId {} endpointId {} aggregateVersion {}: {}", hostId, positionId, endpointId, newAggregateVersion, e.getMessage(), e);
            throw e;
        }
    }

    @Override
    public void deletePositionColFilter(Connection conn, Map<String, Object> event) throws SQLException, Exception {
        // Use UPDATE to implement Soft Delete (setting active = FALSE).
        // OCC/IDM is enforced by checking aggregate_version < newAggregateVersion.
        final String sql =
                """
                UPDATE position_col_filter_t
                SET active = FALSE,
                    update_user = ?,
                    update_ts = ?,
                    aggregate_version = ?
                WHERE host_id = ?
                  AND position_id = ?
                  AND endpoint_id = ?
                  AND aggregate_version < ?
                """; // <<< CRITICAL: Added aggregate_version < ? to enforce monotonicity (OCC/IDM)

        // Note: The original code uses a non-standard map retrieval: Map<String, Object> map = (Map<String, Object>)event.get(PortalConstants.DATA);
        // Assuming SqlUtil.extractEventData(event) is the correct utility based on other methods.
        Map<String, Object> map = SqlUtil.extractEventData(event);

        String hostId = (String)map.get("hostId");
        String positionId = (String)map.get("positionId");
        String endpointId = (String)map.get("endpointId");
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

            // WHERE conditions (4 placeholders)
            // 4: host_id
            statement.setObject(4, UUID.fromString(hostId));
            // 5: position_id
            statement.setString(5, positionId);
            // 6: endpoint_id
            statement.setObject(6, UUID.fromString(endpointId));
            // 7: aggregate_version < ? (new version for OCC/IDM check)
            statement.setLong(7, newAggregateVersion);

            int count = statement.executeUpdate();
            if (count == 0) {
                // If 0 rows updated, it means:
                // 1. The record was not found (already deleted or never existed).
                // 2. The OCC/IDM check failed (aggregate_version >= newAggregateVersion).
                // We IGNORE the failure and log a warning, as this is the desired idempotent/monotonic behavior.
                logger.warn("Soft delete skipped for hostId {} positionId {} endpointId {} aggregateVersion {}. Record not found or a newer/same version already exists.", hostId, positionId, endpointId, newAggregateVersion);
            }
        } catch (SQLException e) {
            logger.error("SQLException during deletePositionColFilter for hostId {} positionId {} endpointId {} aggregateVersion {}: {}", hostId, positionId, endpointId, newAggregateVersion, e.getMessage(), e);
            throw e;
        } catch (Exception e) {
            logger.error("Exception during deletePositionColFilter for hostId {} positionId {} endpointId {} aggregateVersion {}: {}", hostId, positionId, endpointId, newAggregateVersion, e.getMessage(), e);
            throw e;
        }
    }

    @Override
    public void createAttribute(Connection conn, Map<String, Object> event) throws SQLException, Exception {
        // Use UPSERT: INSERT ON CONFLICT DO UPDATE
        // This handles:
        // 1. First time insert (no conflict).
        // 2. Re-creation (conflict on host_id, attribute_id) -> UPDATE the existing soft-deleted row (setting active=TRUE and new version).

        final String sql =
                """
                INSERT INTO attribute_t (
                    host_id,
                    attribute_id,
                    attribute_type,
                    attribute_desc,
                    update_user,
                    update_ts,
                    aggregate_version,
                    active
                ) VALUES (?, ?, ?, ?, ?, ?, ?, TRUE)
                ON CONFLICT (host_id, attribute_id) DO UPDATE
                SET attribute_type = EXCLUDED.attribute_type,
                    attribute_desc = EXCLUDED.attribute_desc,
                    update_user = EXCLUDED.update_user,
                    update_ts = EXCLUDED.update_ts,
                    aggregate_version = EXCLUDED.aggregate_version,
                    active = TRUE
                -- OCC/IDM: Only update if the incoming event is newer
                WHERE attribute_t.aggregate_version < EXCLUDED.aggregate_version AND attribute_t.active = FALSE
                """;

        // Note: The original code uses a non-standard map retrieval: Map<String, Object> map = (Map<String, Object>)event.get(PortalConstants.DATA);
        // Assuming SqlUtil.extractEventData(event) is the correct utility based on other methods.
        Map<String, Object> map = SqlUtil.extractEventData(event);

        String hostId = (String)map.get("hostId");
        String attributeId = (String)map.get("attributeId");
        long newAggregateVersion = SqlUtil.getNewAggregateVersion(event);

        try (PreparedStatement statement = conn.prepareStatement(sql)) {
            // INSERT values (7 placeholders + active=TRUE in SQL, total 7 dynamic values)
            // 1: host_id
            statement.setObject(1, UUID.fromString(hostId));
            // 2: attribute_id
            statement.setString(2, attributeId);

            // 3: attribute_type
            String attributeType = (String)map.get("attributeType");
            if(attributeType != null && !attributeType.isEmpty())
                statement.setString(3, attributeType);
            else
                statement.setNull(3, Types.VARCHAR); // Use Types.VARCHAR

            // 4: attribute_desc
            String attributeDesc = (String)map.get("attributeDesc");
            if (attributeDesc != null && !attributeDesc.isEmpty())
                statement.setString(4, attributeDesc);
            else
                statement.setNull(4, Types.VARCHAR); // Use Types.VARCHAR

            // 5: update_user
            statement.setString(5, (String)event.get(Constants.USER));
            // 6: update_ts
            statement.setObject(6, OffsetDateTime.parse((String)event.get(CloudEventV1.TIME)));
            // 7: aggregate_version
            statement.setLong(7, newAggregateVersion);

            int count = statement.executeUpdate();
            if (count == 0) {
                // count=0 means the ON CONFLICT clause was hit, BUT the WHERE clause (aggregate_version < EXCLUDED.aggregate_version) failed.
                // This is the desired idempotent/out-of-order protection behavior. Log and ignore.
                logger.warn("Creation/Reactivation skipped for hostId {} attributeId {} aggregateVersion {}. A newer or same version already exists.", hostId, attributeId, newAggregateVersion);
            }
        } catch (SQLException e) {
            logger.error("SQLException during createAttribute for hostId {} attributeId {} aggregateVersion {}: {}", hostId, attributeId, newAggregateVersion, e.getMessage(), e);
            throw e;
        } catch (Exception e) {
            logger.error("Exception during createAttribute for hostId {} attributeId {} aggregateVersion {}: {}", hostId, attributeId, newAggregateVersion, e.getMessage(), e);
            throw e;
        }
    }

    @Override
    public void updateAttribute(Connection conn, Map<String, Object> event) throws SQLException, Exception {
        // We attempt to update the record IF the incoming event's aggregate_version is greater than the current projection's version.
        // This enforces Idempotence (IDM) and Optimistic Concurrency Control (OCC) by ensuring version monotonicity.
        // We explicitly set active = TRUE as an UPDATE event implies the attribute should be active.
        final String sql =
                """
                UPDATE attribute_t
                SET attribute_desc = ?,
                    attribute_type = ?,
                    update_user = ?,
                    update_ts = ?,
                    aggregate_version = ?,
                    active = TRUE
                WHERE host_id = ?
                  AND attribute_id = ?
                  AND aggregate_version < ?
                """; // <<< CRITICAL: Changed aggregate_version = ? to aggregate_version < ? to enforce monotonicity (OCC/IDM)

        // Note: The original code uses a non-standard map retrieval: Map<String, Object> map = (Map<String, Object>)event.get(PortalConstants.DATA);
        // Assuming SqlUtil.extractEventData(event) is the correct utility based on other methods.
        Map<String, Object> map = SqlUtil.extractEventData(event);

        String hostId = (String)map.get("hostId");
        String attributeId = (String)map.get("attributeId");
        long newAggregateVersion = SqlUtil.getNewAggregateVersion(event);

        try (PreparedStatement statement = conn.prepareStatement(sql)) {
            // SET values (5 dynamic values + active = TRUE in SQL)
            // 1: attribute_desc
            String attributeDesc = (String)map.get("attributeDesc");
            if(attributeDesc != null && !attributeDesc.isEmpty()) {
                statement.setString(1, attributeDesc);
            } else {
                statement.setNull(1, Types.VARCHAR); // Use Types.VARCHAR
            }
            // 2: attribute_type
            String attributeType = (String)map.get("attributeType");
            if(attributeType != null && !attributeType.isEmpty()) {
                statement.setString(2, attributeType);
            } else {
                statement.setNull(2, Types.VARCHAR); // Use Types.VARCHAR
            }
            // 3: update_user
            statement.setString(3, (String)event.get(Constants.USER));
            // 4: update_ts
            statement.setObject(4, OffsetDateTime.parse((String)event.get(CloudEventV1.TIME)));
            // 5: aggregate_version
            statement.setLong(5, newAggregateVersion);

            // WHERE conditions (3 placeholders)
            // 6: host_id
            statement.setObject(6, UUID.fromString(hostId));
            // 7: attribute_id
            statement.setString(7, attributeId);
            // 8: aggregate_version < ? (new version for OCC/IDM check)
            statement.setLong(8, newAggregateVersion);

            int count = statement.executeUpdate();
            if (count == 0) {
                // If 0 rows updated, it means the record was either not found
                // OR aggregate_version >= newAggregateVersion (OCC/IDM check failed).
                // We IGNORE the failure and log a warning, as this is the desired idempotent/monotonic behavior.
                logger.warn("Update skipped for hostId {} attributeId {} aggregateVersion {}. Record not found or a newer/same version already exists.", hostId, attributeId, newAggregateVersion);
            }
        } catch (SQLException e) {
            logger.error("SQLException during updateAttribute for hostId {} attributeId {} aggregateVersion {}: {}", hostId, attributeId, newAggregateVersion, e.getMessage(), e);
            throw e;
        } catch (Exception e) {
            logger.error("Exception during updateAttribute for hostId {} attributeId {} aggregateVersion {}: {}", hostId, attributeId, newAggregateVersion, e.getMessage(), e);
            throw e;
        }
    }

    @Override
    public void deleteAttribute(Connection conn, Map<String, Object> event) throws SQLException, Exception {
        // Use UPDATE to implement Soft Delete (setting active = FALSE).
        // OCC/IDM is enforced by checking aggregate_version < newAggregateVersion.
        final String sql =
                """
                UPDATE attribute_t
                SET active = FALSE,
                    update_user = ?,
                    update_ts = ?,
                    aggregate_version = ?
                WHERE host_id = ?
                  AND attribute_id = ?
                  AND aggregate_version < ?
                """; // <<< CRITICAL: Added aggregate_version < ? to enforce monotonicity (OCC/IDM)

        // Note: The original code uses a non-standard map retrieval: Map<String, Object> map = (Map<String, Object>)event.get(PortalConstants.DATA);
        // Assuming SqlUtil.extractEventData(event) is the correct utility based on other methods.
        Map<String, Object> map = SqlUtil.extractEventData(event);

        String hostId = (String)map.get("hostId");
        String attributeId = (String)map.get("attributeId");
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

            // WHERE conditions (3 placeholders)
            // 4: host_id
            statement.setObject(4, UUID.fromString(hostId));
            // 5: attribute_id
            statement.setString(5, attributeId);
            // 6: aggregate_version < ? (new version for OCC/IDM check)
            statement.setLong(6, newAggregateVersion);

            int count = statement.executeUpdate();
            if (count == 0) {
                // If 0 rows updated, it means:
                // 1. The record was not found (already deleted or never existed).
                // 2. The OCC/IDM check failed (aggregate_version >= newAggregateVersion).
                // We IGNORE the failure and log a warning, as this is the desired idempotent/monotonic behavior.
                logger.warn("Soft delete skipped for hostId {} attributeId {} aggregateVersion {}. Record not found or a newer/same version already exists.", hostId, attributeId, newAggregateVersion);
            }
        } catch (SQLException e) {
            logger.error("SQLException during deleteAttribute for hostId {} attributeId {} aggregateVersion {}: {}", hostId, attributeId, newAggregateVersion, e.getMessage(), e);
            throw e;
        } catch (Exception e) {
            logger.error("Exception during deleteAttribute for hostId {} attributeId {} aggregateVersion {}: {}", hostId, attributeId, newAggregateVersion, e.getMessage(), e);
            throw e;
        }
    }

    @Override
    public Result<String> queryAttribute(int offset, int limit, String filtersJson, String globalFilter, String sortingJson, boolean active, String hostId) {
        Result<String> result;
        List<Map<String, Object>> filters = parseJsonList(filtersJson);
        List<Map<String, Object>> sorting = parseJsonList(sortingJson);

        String s =
            """
            SELECT COUNT(*) OVER () AS total,
            host_id, attribute_id, attribute_type, attribute_desc,
            update_user, update_ts, aggregate_version, active
            FROM attribute_t
            WHERE host_id = ?
            """;

        List<Object> parameters = new ArrayList<>();
        parameters.add(UUID.fromString(hostId));

        String activeClause = SqlUtil.buildMultiTableActiveClause(active);
        String[] searchColumns = {"attribute_id", "attribute_desc"};
        String sqlBuilder = s + activeClause +
                dynamicFilter(Arrays.asList("host_id"), Arrays.asList(searchColumns), filters, null, parameters) +
                globalFilter(globalFilter, searchColumns, parameters) +
                dynamicSorting("attribute_id", sorting, null) +
                "\nLIMIT ? OFFSET ?";

        parameters.add(limit);
        parameters.add(offset);

        int total = 0;
        List<Map<String, Object>> attributes = new ArrayList<>();
        try (final Connection conn = ds.getConnection(); PreparedStatement preparedStatement = conn.prepareStatement(sqlBuilder)) {
            populateParameters(preparedStatement, parameters);
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
                    map.put("aggregateVersion", resultSet.getLong("aggregate_version"));
                    map.put("active", resultSet.getBoolean("active"));
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
    public Result<String> getAttributeById(String hostId, String attributeId) {
        final String sql =
                """
                SELECT host_id, attribute_id, attribute_type, attribute_desc,
                aggregate_version, active, update_user, update_ts
                FROM attribute_t
                WHERE host_id = ? AND attribute_id = ?
                """;
        Result<String> result;
        Map<String, Object> map = new HashMap<>();

        String searchId = hostId + ":" + attributeId;

        try (Connection conn = ds.getConnection();
             PreparedStatement statement = conn.prepareStatement(sql)) {

            statement.setObject(1, UUID.fromString(hostId));
            statement.setString(2, attributeId);

            try (ResultSet resultSet = statement.executeQuery()) {
                if (resultSet.next()) {
                    map.put("hostId", resultSet.getObject("host_id", UUID.class));
                    map.put("attributeId", resultSet.getString("attribute_id"));
                    map.put("attributeType", resultSet.getString("attribute_type"));
                    map.put("attributeDesc", resultSet.getString("attribute_desc"));
                    map.put("aggregateVersion", resultSet.getLong("aggregate_version"));
                    map.put("active", resultSet.getBoolean("active"));
                    map.put("updateUser", resultSet.getString("update_user"));
                    map.put("updateTs", resultSet.getObject("update_ts") != null ? resultSet.getObject("update_ts", OffsetDateTime.class) : null);

                    result = Success.of(JsonMapper.toJson(map));
                } else {
                    result = Failure.of(new Status(OBJECT_NOT_FOUND, "attribute", searchId));
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
    public Result<String> queryAttributeLabel(String hostId) {
        final String sql = "SELECT attribute_id from attribute_t WHERE host_id = ? AND active = TRUE";
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
    public Result<String> queryAttributePermission(int offset, int limit, String filtersJson, String globalFilter, String sortingJson, boolean active, String hostId) {
        Result<String> result;
        final Map<String, String> columnMap = new HashMap<>(Map.of(
                "hostId", "ap.host_id",
                "attributeId", "ap.attribute_id",
                "attributeType", "a.attribute_type",
                "attributeValue", "ap.attribute_value",
                "apiVersionId", "av.api_version_id",
                "apiId", "av.api_id",
                "apiVersion", "av.api_version",
                "endpointId", "ae.endpoint_id",
                "endpoint", "ae.endpoint",
                "updateUser", "ap.update_user"
        ));
        columnMap.put("aggregateVersion", "ap.aggregate_version");
        columnMap.put("updateTs", "ap.update_ts");
        columnMap.put("active", "ap.active");


        List<Map<String, Object>> filters = parseJsonList(filtersJson);
        List<Map<String, Object>> sorting = parseJsonList(sortingJson);

        String s =
            """
                SELECT COUNT(*) OVER () AS total,
                ap.host_id, ap.attribute_id, a.attribute_type, ap.attribute_value,
                av.api_version_id, av.api_id, av.api_version, ap.endpoint_id, ae.endpoint,
                ap.aggregate_version, ap.update_user, ap.update_ts, ap.active
                FROM attribute_permission_t ap
                JOIN attribute_t a ON ap.attribute_id = a.attribute_id
                JOIN api_endpoint_t ae ON ap.host_id = ae.host_id AND ap.endpoint_id = ae.endpoint_id
                JOIN api_version_t av ON ae.host_id = av.host_id AND ae.api_version_id = av.api_version_id
                AND ap.host_id = ?
            """;

        List<Object> parameters = new ArrayList<>();
        parameters.add(UUID.fromString(hostId));

        String activeClause = SqlUtil.buildMultiTableActiveClause(active, "ap", "a", "ae", "av");
        String[] searchColumns = {"ap.attribute_id", "ap.attribute_value", "ae.endpoint"};
        String sqlBuilder = s + activeClause +
                dynamicFilter(Arrays.asList("ap.host_id", "av.api_version_id", "ap.endpoint_id"), Arrays.asList(searchColumns), filters, columnMap, parameters) +
                globalFilter(globalFilter, searchColumns, parameters) +
                dynamicSorting("ap.attribute_id, av.api_id, av.api_version, ae.endpoint", sorting, columnMap) +
                "\nLIMIT ? OFFSET ?";

        parameters.add(limit);
        parameters.add(offset);

        if(logger.isTraceEnabled()) logger.trace("queryAttributePermission sql: {}", sqlBuilder);
        int total = 0;
        List<Map<String, Object>> attributePermissions = new ArrayList<>();


        try (final Connection conn = ds.getConnection(); PreparedStatement preparedStatement = conn.prepareStatement(sqlBuilder)) {
            populateParameters(preparedStatement, parameters);
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
                    map.put("apiVersionId", resultSet.getString("api_version_id"));
                    map.put("apiId", resultSet.getString("api_id"));
                    map.put("apiVersion", resultSet.getString("api_version"));
                    map.put("endpointId", resultSet.getString("endpoint_id"));
                    map.put("endpoint", resultSet.getString("endpoint"));
                    map.put("aggregateVersion", resultSet.getLong("aggregate_version"));
                    map.put("updateUser", resultSet.getString("update_user"));
                    map.put("updateTs", resultSet.getObject("update_ts") != null ? resultSet.getObject("update_ts", OffsetDateTime.class) : null);
                    map.put("active", resultSet.getBoolean("active"));
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
    public Result<String> getAttributePermissionById(String hostId, String attributeId, String endpointId) {
        final String sql =
                """
                SELECT host_id, attribute_id, endpoint_id, attribute_value,
                aggregate_version, active, update_user, update_ts
                FROM attribute_permission_t
                WHERE host_id = ? AND attribute_id = ? AND endpoint_id = ?
                """;
        Result<String> result;
        Map<String, Object> map = new HashMap<>();

        String searchId = hostId + ":" + attributeId + ":" + endpointId;

        try (Connection conn = ds.getConnection();
             PreparedStatement statement = conn.prepareStatement(sql)) {

            statement.setObject(1, UUID.fromString(hostId));
            statement.setString(2, attributeId);
            statement.setObject(3, UUID.fromString(endpointId));

            try (ResultSet resultSet = statement.executeQuery()) {
                if (resultSet.next()) {
                    map.put("hostId", resultSet.getObject("host_id", UUID.class));
                    map.put("attributeId", resultSet.getString("attribute_id"));
                    map.put("endpointId", resultSet.getObject("endpoint_id", UUID.class));
                    map.put("attributeValue", resultSet.getString("attribute_value"));
                    map.put("aggregateVersion", resultSet.getLong("aggregate_version"));
                    map.put("active", resultSet.getBoolean("active"));
                    map.put("updateUser", resultSet.getString("update_user"));
                    map.put("updateTs", resultSet.getObject("update_ts") != null ? resultSet.getObject("update_ts", OffsetDateTime.class) : null);

                    result = Success.of(JsonMapper.toJson(map));
                } else {
                    result = Failure.of(new Status(OBJECT_NOT_FOUND, "attribute_permission", searchId));
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
    public Result<String> queryAttributeUser(int offset, int limit, String filtersJson, String globalFilter, String sortingJson, boolean active, String hostId) {
        Result<String> result;
        final Map<String, String> columnMap = new HashMap<>(Map.of(
                "hostId", "a.host_id",
                "attributeId", "a.attribute_id",
                "startTs", "a.start_ts",
                "endTs", "a.end_ts",
                "aggregateVersion", "a.aggregate_version",
                "updateUser", "a.update_user",
                "updateTs", "a.update_ts",
                "userId", "u.user_id",
                "email", "u.email",
                "userType", "u.user_type"
        ));
        columnMap.put("entityId", "CASE WHEN u.user_type = 'C' THEN c.customer_id WHEN u.user_type = 'E' THEN e.employee_id ELSE NULL -- Handle other cases if needed END");
        columnMap.put("firstName", "u.first_name");
        columnMap.put("lastName", "u.last_name");
        columnMap.put("managerId", "e.manager_id");
        columnMap.put("attributeType", "at.attribute_type");
        columnMap.put("attributeValue", "a.attribute_value");
        columnMap.put("active", "a.active");




        List<Map<String, Object>> filters = parseJsonList(filtersJson);
        List<Map<String, Object>> sorting = parseJsonList(sortingJson);

        String s =
            """
                SELECT COUNT(*) OVER () AS total,
                a.host_id, a.attribute_id, at.attribute_type, a.attribute_value,
                a.start_ts, a.end_ts,
                a.aggregate_version, a.update_user, a.update_ts,
                u.user_id, u.email, u.user_type,
                CASE
                    WHEN u.user_type = 'C' THEN c.customer_id
                    WHEN u.user_type = 'E' THEN e.employee_id
                    ELSE NULL -- Handle other cases if needed
                END AS entity_id,
                e.manager_id, u.first_name, u.last_name, a.active
                FROM user_t u
                LEFT JOIN
                    customer_t c ON u.user_id = c.user_id AND u.user_type = 'C'
                LEFT JOIN
                    employee_t e ON u.user_id = e.user_id AND u.user_type = 'E'
                INNER JOIN
                    attribute_user_t a ON a.user_id = u.user_id
                INNER JOIN
                    attribute_t at ON at.attribute_id = a.attribute_id
                AND a.host_id = ?
            """;

        List<Object> parameters = new ArrayList<>();
        parameters.add(UUID.fromString(hostId));

        String activeClause = SqlUtil.buildMultiTableActiveClause(active, "u", "c", "e", "a", "at");
        String[] searchColumns = {"a.attribute_id", "a.attribute_value", "u.email", "u.first_name", "u.last_name"};
        String sqlBuilder = s + activeClause +
                dynamicFilter(Arrays.asList("a.host_id", "u.user_id"), Arrays.asList(searchColumns), filters, columnMap, parameters) +
                globalFilter(globalFilter, searchColumns, parameters) +
                dynamicSorting("a.attribute_id, u.user_id", sorting, columnMap) +
                "\nLIMIT ? OFFSET ?";

        parameters.add(limit);
        parameters.add(offset);

        if(logger.isTraceEnabled()) logger.trace("queryGroupUser sql: {}", sqlBuilder);
        int total = 0;
        List<Map<String, Object>> attributeUsers = new ArrayList<>();


        try (final Connection conn = ds.getConnection(); PreparedStatement preparedStatement = conn.prepareStatement(sqlBuilder)) {
            populateParameters(preparedStatement, parameters);
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
                    map.put("startTs", resultSet.getObject("start_ts") != null ? resultSet.getObject("start_ts", OffsetDateTime.class) : null);
                    map.put("endTs", resultSet.getObject("end_ts") != null ? resultSet.getObject("end_ts", OffsetDateTime.class) : null);
                    map.put("userId", resultSet.getObject("user_id", UUID.class));
                    map.put("entityId", resultSet.getString("entity_id"));
                    map.put("email", resultSet.getString("email"));
                    map.put("firstName", resultSet.getString("first_name"));
                    map.put("lastName", resultSet.getString("last_name"));
                    map.put("userType", resultSet.getString("user_type"));
                    map.put("updateUser", resultSet.getString("update_user"));
                    map.put("updateTs", resultSet.getObject("update_ts") != null ? resultSet.getObject("update_ts", OffsetDateTime.class) : null);
                    map.put("aggregateVersion", resultSet.getLong("aggregate_version"));
                    map.put("active", resultSet.getBoolean("active"));
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
    public Result<String> getAttributeUserById(String hostId, String attributeId, String userId) {
        final String sql =
                """
                SELECT host_id, attribute_id, user_id, attribute_value, start_ts, end_ts,
                aggregate_version, active, update_user, update_ts
                FROM attribute_user_t
                WHERE host_id = ? AND attribute_id = ? AND user_id = ?
                """;
        Result<String> result;
        Map<String, Object> map = new HashMap<>();

        String searchId = hostId + ":" + attributeId + ":" + userId;

        try (Connection conn = ds.getConnection();
             PreparedStatement statement = conn.prepareStatement(sql)) {

            statement.setObject(1, UUID.fromString(hostId));
            statement.setString(2, attributeId);
            statement.setObject(3, UUID.fromString(userId));

            try (ResultSet resultSet = statement.executeQuery()) {
                if (resultSet.next()) {
                    map.put("hostId", resultSet.getObject("host_id", UUID.class));
                    map.put("attributeId", resultSet.getString("attribute_id"));
                    map.put("userId", resultSet.getObject("user_id", UUID.class));
                    map.put("attributeValue", resultSet.getString("attribute_value"));
                    map.put("startTs", resultSet.getObject("start_ts") != null ? resultSet.getObject("start_ts", OffsetDateTime.class) : null);
                    map.put("endTs", resultSet.getObject("end_ts") != null ? resultSet.getObject("end_ts", OffsetDateTime.class) : null);
                    map.put("aggregateVersion", resultSet.getLong("aggregate_version"));
                    map.put("active", resultSet.getBoolean("active"));
                    map.put("updateUser", resultSet.getString("update_user"));
                    map.put("updateTs", resultSet.getObject("update_ts") != null ? resultSet.getObject("update_ts", OffsetDateTime.class) : null);

                    result = Success.of(JsonMapper.toJson(map));
                } else {
                    result = Failure.of(new Status(OBJECT_NOT_FOUND, "attribute_user", searchId));
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
    public void createAttributePermission(Connection conn, Map<String, Object> event) throws SQLException, Exception {
        // Use UPSERT: INSERT ON CONFLICT DO UPDATE
        // This handles:
        // 1. First time insert (no conflict).
        // 2. Re-creation (conflict on host_id, attribute_id, endpoint_id) -> UPDATE the existing soft-deleted row (setting active=TRUE and new version).

        final String sql =
                """
                INSERT INTO attribute_permission_t (
                    host_id,
                    attribute_id,
                    attribute_value,
                    endpoint_id,
                    update_user,
                    update_ts,
                    aggregate_version,
                    active
                ) VALUES (?, ?, ?, ?, ?, ?, ?, TRUE)
                ON CONFLICT (host_id, attribute_id, endpoint_id) DO UPDATE
                SET attribute_value = EXCLUDED.attribute_value,
                    update_user = EXCLUDED.update_user,
                    update_ts = EXCLUDED.update_ts,
                    aggregate_version = EXCLUDED.aggregate_version,
                    active = TRUE
                -- OCC/IDM: Only update if the incoming event is newer
                WHERE attribute_permission_t.aggregate_version < EXCLUDED.aggregate_version AND attribute_permission_t.active = FALSE
                """;

        // Note: The original code uses a non-standard map retrieval: Map<String, Object> map = (Map<String, Object>)event.get(PortalConstants.DATA);
        // Assuming SqlUtil.extractEventData(event) is the correct utility based on other methods.
        Map<String, Object> map = SqlUtil.extractEventData(event);

        String hostId = (String)map.get("hostId");
        String attributeId = (String)map.get("attributeId");
        String endpointId = (String)map.get("endpointId");
        long newAggregateVersion = SqlUtil.getNewAggregateVersion(event);

        try (PreparedStatement statement = conn.prepareStatement(sql)) {
            // INSERT values (7 placeholders + active=TRUE in SQL, total 7 dynamic values)
            // 1: host_id
            statement.setObject(1, UUID.fromString(hostId));
            // 2: attribute_id
            statement.setString(2, attributeId);
            // 3: attribute_value
            statement.setString(3, (String)map.get("attributeValue"));
            // 4: endpoint_id
            statement.setObject(4, UUID.fromString(endpointId));
            // 5: update_user
            statement.setString(5, (String)event.get(Constants.USER));
            // 6: update_ts
            statement.setObject(6, OffsetDateTime.parse((String)event.get(CloudEventV1.TIME)));
            // 7: aggregate_version
            statement.setLong(7, newAggregateVersion);

            int count = statement.executeUpdate();
            if (count == 0) {
                // count=0 means the ON CONFLICT clause was hit, BUT the WHERE clause (aggregate_version < EXCLUDED.aggregate_version) failed.
                // This is the desired idempotent/out-of-order protection behavior. Log and ignore.
                logger.warn("Creation/Reactivation skipped for hostId {} attributeId {} endpointId {} aggregateVersion {}. A newer or same version already exists.", hostId, attributeId, endpointId, newAggregateVersion);
            }
        } catch (SQLException e) {
            logger.error("SQLException during createAttributePermission for hostId {} attributeId {} endpointId {} aggregateVersion {}: {}", hostId, attributeId, endpointId, newAggregateVersion, e.getMessage(), e);
            throw e;
        } catch (Exception e) {
            logger.error("Exception during createAttributePermission for hostId {} attributeId {} endpointId {} aggregateVersion {}: {}", hostId, attributeId, endpointId, newAggregateVersion, e.getMessage(), e);
            throw e;
        }
    }

    @Override
    public void updateAttributePermission(Connection conn, Map<String, Object> event) throws SQLException, Exception {
        // We attempt to update the record IF the incoming event's aggregate_version is greater than the current projection's version.
        // This enforces Idempotence (IDM) and Optimistic Concurrency Control (OCC) by ensuring version monotonicity.
        // We explicitly set active = TRUE as an UPDATE event implies the permission should be active.
        final String sql =
                """
                UPDATE attribute_permission_t
                SET attribute_value = ?,
                    update_user = ?,
                    update_ts = ?,
                    aggregate_version = ?,
                    active = TRUE
                WHERE host_id = ?
                  AND attribute_id = ?
                  AND endpoint_id = ?
                  AND aggregate_version < ?
                """; // <<< CRITICAL: Changed aggregate_version = ? to aggregate_version < ? to enforce monotonicity (OCC/IDM)

        // Note: The original code uses a non-standard map retrieval: Map<String, Object> map = (Map<String, Object>)event.get(PortalConstants.DATA);
        // Assuming SqlUtil.extractEventData(event) is the correct utility based on other methods.
        Map<String, Object> map = SqlUtil.extractEventData(event);

        String hostId = (String)map.get("hostId");
        String attributeId = (String)map.get("attributeId");
        String endpointId = (String)map.get("endpointId");
        long newAggregateVersion = SqlUtil.getNewAggregateVersion(event);

        try (PreparedStatement statement = conn.prepareStatement(sql)) {
            // SET values (4 dynamic values + active = TRUE in SQL)
            // 1: attribute_value
            statement.setString(1, (String)map.get("attributeValue"));
            // 2: update_user
            statement.setString(2, (String)event.get(Constants.USER));
            // 3: update_ts
            statement.setObject(3, OffsetDateTime.parse((String)event.get(CloudEventV1.TIME)));
            // 4: aggregate_version
            statement.setLong(4, newAggregateVersion);

            // WHERE conditions (4 placeholders)
            // 5: host_id
            statement.setObject(5, UUID.fromString(hostId));
            // 6: attribute_id
            statement.setString(6, attributeId);
            // 7: endpoint_id
            statement.setObject(7, UUID.fromString(endpointId));
            // 8: aggregate_version < ? (new version for OCC/IDM check)
            statement.setLong(8, newAggregateVersion);

            int count = statement.executeUpdate();
            if (count == 0) {
                // If 0 rows updated, it means the record was either not found
                // OR aggregate_version >= newAggregateVersion (OCC/IDM check failed).
                // We IGNORE the failure and log a warning, as this is the desired idempotent/monotonic behavior.
                logger.warn("Update skipped for hostId {} attributeId {} endpointId {} aggregateVersion {}. Record not found or a newer/same version already exists.", hostId, attributeId, endpointId, newAggregateVersion);
            }
        } catch (SQLException e) {
            logger.error("SQLException during updateAttributePermission for hostId {} attributeId {} endpointId {} aggregateVersion {}: {}", hostId, attributeId, endpointId, newAggregateVersion, e.getMessage(), e);
            throw e;
        } catch (Exception e) {
            logger.error("Exception during updateAttributePermission for hostId {} attributeId {} endpointId {} aggregateVersion {}: {}", hostId, attributeId, endpointId, newAggregateVersion, e.getMessage(), e);
            throw e;
        }
    }

    @Override
    public void deleteAttributePermission(Connection conn, Map<String, Object> event) throws SQLException, Exception {
        // Use UPDATE to implement Soft Delete (setting active = FALSE).
        // OCC/IDM is enforced by checking aggregate_version < newAggregateVersion.
        final String sql =
                """
                UPDATE attribute_permission_t
                SET active = FALSE,
                    update_user = ?,
                    update_ts = ?,
                    aggregate_version = ?
                WHERE host_id = ?
                  AND attribute_id = ?
                  AND endpoint_id = ?
                  AND aggregate_version < ?
                """; // <<< CRITICAL: Added aggregate_version < ? to enforce monotonicity (OCC/IDM)

        // Note: The original code uses a non-standard map retrieval: Map<String, Object> map = (Map<String, Object>)event.get(PortalConstants.DATA);
        // Assuming SqlUtil.extractEventData(event) is the correct utility based on other methods.
        Map<String, Object> map = SqlUtil.extractEventData(event);

        String hostId = (String)map.get("hostId");
        String attributeId = (String)map.get("attributeId");
        String endpointId = (String)map.get("endpointId");
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

            // WHERE conditions (4 placeholders)
            // 4: host_id
            statement.setObject(4, UUID.fromString(hostId));
            // 5: attribute_id
            statement.setString(5, attributeId);
            // 6: endpoint_id
            statement.setObject(6, UUID.fromString(endpointId));
            // 7: aggregate_version < ? (new version for OCC/IDM check)
            statement.setLong(7, newAggregateVersion);

            int count = statement.executeUpdate();
            if (count == 0) {
                // If 0 rows updated, it means:
                // 1. The record was not found (already deleted or never existed).
                // 2. The OCC/IDM check failed (aggregate_version >= newAggregateVersion).
                // We IGNORE the failure and log a warning, as this is the desired idempotent/monotonic behavior.
                logger.warn("Soft delete skipped for hostId {} attributeId {} endpointId {} aggregateVersion {}. Record not found or a newer/same version already exists.", hostId, attributeId, endpointId, newAggregateVersion);
            }
        } catch (SQLException e) {
            logger.error("SQLException during deleteAttributePermission for hostId {} attributeId {} endpointId {} aggregateVersion {}: {}", hostId, attributeId, endpointId, newAggregateVersion, e.getMessage(), e);
            throw e;
        } catch (Exception e) {
            logger.error("Exception during deleteAttributePermission for hostId {} attributeId {} endpointId {} aggregateVersion {}: {}", hostId, attributeId, endpointId, newAggregateVersion, e.getMessage(), e);
            throw e;
        }
    }

    @Override
    public void createAttributeUser(Connection conn, Map<String, Object> event) throws SQLException, Exception {
        // Use UPSERT: INSERT ON CONFLICT DO UPDATE
        // This handles:
        // 1. First time insert (no conflict).
        // 2. Re-creation (conflict on host_id, attribute_id, user_id) -> UPDATE the existing soft-deleted row (setting active=TRUE and new version).

        final String sql =
                """
                INSERT INTO attribute_user_t (
                    host_id,
                    attribute_id,
                    attribute_value,
                    user_id,
                    start_ts,
                    end_ts,
                    update_user,
                    update_ts,
                    aggregate_version,
                    active
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, TRUE)
                ON CONFLICT (host_id, attribute_id, user_id) DO UPDATE
                SET attribute_value = EXCLUDED.attribute_value,
                    start_ts = EXCLUDED.start_ts,
                    end_ts = EXCLUDED.end_ts,
                    update_user = EXCLUDED.update_user,
                    update_ts = EXCLUDED.update_ts,
                    aggregate_version = EXCLUDED.aggregate_version,
                    active = TRUE
                -- OCC/IDM: Only update if the incoming event is newer
                WHERE attribute_user_t.aggregate_version < EXCLUDED.aggregate_version AND attribute_user_t.active = FALSE
                """;

        // Note: The original code uses a non-standard map retrieval: Map<String, Object> map = (Map<String, Object>)event.get(PortalConstants.DATA);
        // Assuming SqlUtil.extractEventData(event) is the correct utility based on other methods.
        Map<String, Object> map = SqlUtil.extractEventData(event);

        String hostId = (String)map.get("hostId");
        String attributeId = (String)map.get("attributeId");
        String userId = (String)map.get("userId");
        long newAggregateVersion = SqlUtil.getNewAggregateVersion(event);

        try (PreparedStatement statement = conn.prepareStatement(sql)) {
            // INSERT values (9 placeholders + active=TRUE in SQL, total 9 dynamic values)
            // 1: host_id
            statement.setObject(1, UUID.fromString(hostId));
            // 2: attribute_id
            statement.setString(2, attributeId);
            // 3: attribute_value
            statement.setString(3, (String)map.get("attributeValue"));
            // 4: user_id
            statement.setObject(4, UUID.fromString(userId));

            // 5: start_ts
            String startTs = (String)map.get("startTs");
            if(startTs != null && !startTs.isEmpty())
                statement.setObject(5, OffsetDateTime.parse(startTs));
            else
                statement.setNull(5, Types.TIMESTAMP_WITH_TIMEZONE); // Use Types.TIMESTAMP_WITH_TIMEZONE

            // 6: end_ts
            String endTs = (String)map.get("endTs");
            if (endTs != null && !endTs.isEmpty()) {
                statement.setObject(6, OffsetDateTime.parse(endTs));
            } else {
                statement.setNull(6, Types.TIMESTAMP_WITH_TIMEZONE); // Use Types.TIMESTAMP_WITH_TIMEZONE
            }

            // 7: update_user
            statement.setString(7, (String)event.get(Constants.USER));
            // 8: update_ts
            statement.setObject(8, OffsetDateTime.parse((String)event.get(CloudEventV1.TIME)));
            // 9: aggregate_version
            statement.setLong(9, newAggregateVersion);

            int count = statement.executeUpdate();
            if (count == 0) {
                // count=0 means the ON CONFLICT clause was hit, BUT the WHERE clause (aggregate_version < EXCLUDED.aggregate_version) failed.
                // This is the desired idempotent/out-of-order protection behavior. Log and ignore.
                logger.warn("Creation/Reactivation skipped for hostId {} attributeId {} userId {} aggregateVersion {}. A newer or same version already exists.", hostId, attributeId, userId, newAggregateVersion);
            }
        } catch (SQLException e) {
            logger.error("SQLException during createAttributeUser for hostId {} attributeId {} userId {} and aggregateVersion {}: {}", hostId, attributeId, userId, newAggregateVersion, e.getMessage(), e);
            throw e;
        } catch (Exception e) {
            logger.error("Exception during createAttributeUser for hostId {} attributeId {} userId {} and aggregateVersion {}: {}", hostId, attributeId, userId, newAggregateVersion, e.getMessage(), e);
            throw e;
        }
    }

    @Override
    public void updateAttributeUser(Connection conn, Map<String, Object> event) throws SQLException, Exception {
        // We attempt to update the record IF the incoming event's aggregate_version is greater than the current projection's version.
        // This enforces Idempotence (IDM) and Optimistic Concurrency Control (OCC) by ensuring version monotonicity.
        // We explicitly set active = TRUE as an UPDATE event implies the attribute assignment should be active.
        final String sql =
                """
                UPDATE attribute_user_t
                SET attribute_value = ?,
                    start_ts = ?,
                    end_ts = ?,
                    update_user = ?,
                    update_ts = ?,
                    aggregate_version = ?,
                    active = TRUE
                WHERE host_id = ?
                  AND attribute_id = ?
                  AND user_id = ?
                  AND aggregate_version < ?
                """; // <<< CRITICAL: Changed aggregate_version = ? to aggregate_version < ? to enforce monotonicity (OCC/IDM)

        // Note: The original code uses a non-standard map retrieval: Map<String, Object> map = (Map<String, Object>)event.get(PortalConstants.DATA);
        // Assuming SqlUtil.extractEventData(event) is the correct utility based on other methods.
        Map<String, Object> map = SqlUtil.extractEventData(event);

        String hostId = (String)map.get("hostId");
        String attributeId = (String)map.get("attributeId");
        String userId = (String)map.get("userId");
        long newAggregateVersion = SqlUtil.getNewAggregateVersion(event);

        try (PreparedStatement statement = conn.prepareStatement(sql)) {
            // SET values (6 dynamic values + active = TRUE in SQL)
            // 1: attribute_value
            statement.setString(1, (String)map.get("attributeValue"));

            // 2: start_ts
            String startTs = (String)map.get("startTs");
            if(startTs != null && !startTs.isEmpty())
                statement.setObject(2, OffsetDateTime.parse(startTs));
            else
                statement.setNull(2, Types.TIMESTAMP_WITH_TIMEZONE); // Use Types.TIMESTAMP_WITH_TIMEZONE

            // 3: end_ts
            String endTs = (String)map.get("endTs");
            if (endTs != null && !endTs.isEmpty()) {
                statement.setObject(3, OffsetDateTime.parse(endTs));
            } else {
                statement.setNull(3, Types.TIMESTAMP_WITH_TIMEZONE); // Use Types.TIMESTAMP_WITH_TIMEZONE
            }

            // 4: update_user
            statement.setString(4, (String)event.get(Constants.USER));
            // 5: update_ts
            statement.setObject(5, OffsetDateTime.parse((String)event.get(CloudEventV1.TIME)));
            // 6: aggregate_version
            statement.setLong(6, newAggregateVersion);

            // WHERE conditions (4 placeholders)
            // 7: host_id
            statement.setObject(7, UUID.fromString(hostId));
            // 8: attribute_id
            statement.setString(8, attributeId);
            // 9: user_id
            statement.setObject(9, UUID.fromString(userId));
            // 10: aggregate_version < ? (new version for OCC/IDM check)
            statement.setLong(10, newAggregateVersion);

            int count = statement.executeUpdate();
            if (count == 0) {
                // If 0 rows updated, it means the record was either not found
                // OR aggregate_version >= newAggregateVersion (OCC/IDM check failed).
                // We IGNORE the failure and log a warning, as this is the desired idempotent/monotonic behavior.
                logger.warn("Update skipped for hostId {} attributeId {} userId {} aggregateVersion {}. Record not found or a newer/same version already exists.", hostId, attributeId, userId, newAggregateVersion);
            }
        } catch (SQLException e) {
            logger.error("SQLException during updateAttributeUser for hostId {} attributeId {} userId {} aggregateVersion {}: {}", hostId, attributeId, userId, newAggregateVersion, e.getMessage(), e);
            throw e;
        } catch (Exception e) {
            logger.error("Exception during updateAttributeUser for hostId {} attributeId {} userId {} aggregateVersion {}: {}", hostId, attributeId, userId, newAggregateVersion, e.getMessage(), e);
            throw e;
        }
    }

    @Override
    public void deleteAttributeUser(Connection conn, Map<String, Object> event) throws SQLException, Exception {
        // Use UPDATE to implement Soft Delete (setting active = FALSE).
        // OCC/IDM is enforced by checking aggregate_version < newAggregateVersion.
        final String sql =
                """
                UPDATE attribute_user_t
                SET active = FALSE,
                    update_user = ?,
                    update_ts = ?,
                    aggregate_version = ?
                WHERE host_id = ?
                  AND attribute_id = ?
                  AND user_id = ?
                  AND aggregate_version < ?
                """; // <<< CRITICAL: Added aggregate_version < ? to enforce monotonicity (OCC/IDM)

        // Note: The original code uses a non-standard map retrieval: Map<String, Object> map = (Map<String, Object>)event.get(PortalConstants.DATA);
        // Assuming SqlUtil.extractEventData(event) is the correct utility based on other methods.
        Map<String, Object> map = SqlUtil.extractEventData(event);

        String hostId = (String)map.get("hostId");
        String attributeId = (String)map.get("attributeId");
        String userId = (String)map.get("userId");
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

            // WHERE conditions (4 placeholders)
            // 4: host_id
            statement.setObject(4, UUID.fromString(hostId));
            // 5: attribute_id
            statement.setString(5, attributeId);
            // 6: user_id
            statement.setObject(6, UUID.fromString(userId));
            // 7: aggregate_version < ? (new version for OCC/IDM check)
            statement.setLong(7, newAggregateVersion);

            int count = statement.executeUpdate();
            if (count == 0) {
                // If 0 rows updated, it means:
                // 1. The record was not found (already deleted or never existed).
                // 2. The OCC/IDM check failed (aggregate_version >= newAggregateVersion).
                // We IGNORE the failure and log a warning, as this is the desired idempotent/monotonic behavior.
                logger.warn("Soft delete skipped for hostId {} attributeId {} userId {} aggregateVersion {}. Record not found or a newer/same version already exists.", hostId, attributeId, userId, newAggregateVersion);
            }
        } catch (SQLException e) {
            logger.error("SQLException during deleteAttributeUser for hostId {} attributeId {} userId {} aggregateVersion {}: {}", hostId, attributeId, userId, newAggregateVersion, e.getMessage(), e);
            throw e;
        } catch (Exception e) {
            logger.error("Exception during deleteAttributeUser for hostId {} attributeId {} userId {} aggregateVersion {}: {}", hostId, attributeId, userId, newAggregateVersion, e.getMessage(), e);
            throw e;
        }
    }

    @Override
    public Result<String> queryAttributeRowFilter(int offset, int limit, String filtersJson, String globalFilter, String sortingJson, boolean active, String hostId) {
        Result<String> result;
        final Map<String, String> columnMap = new HashMap<>(Map.of(
                "hostId", "a.host_id",
                "attributeId", "a.attribute_id",
                "attributeType", "a.attribute_type",
                "attributeValue", "a.attribute_value",
                "endpointId", "p.endpoint_id",
                "endpoint", "ae.endpoint",
                "apiVersionId", "av.api_version_id",
                "apiId", "av.api_id",
                "apiVersion", "av.api_version",
                "colName", "p.col_name"
        ));
        columnMap.put("operator", "p.operator");
        columnMap.put("colValue", "p.col_value");
        columnMap.put("updateUser", "p.update_user");
        columnMap.put("updateTs", "p.update_ts");
        columnMap.put("aggregateVersion", "p.aggregate_version");
        columnMap.put("active", "p.active");


        List<Map<String, Object>> filters = parseJsonList(filtersJson);
        List<Map<String, Object>> sorting = parseJsonList(sortingJson);

        String s =
            """
                SELECT COUNT(*) OVER () AS total, a.host_id, a.attribute_id, a.attribute_type,\s
                p.attribute_value, p.endpoint_id, ae.endpoint,
                av.api_version_id, av.api_id, av.api_version, p.col_name, p.operator, p.col_value,
                p.update_user, p.update_ts, p.aggregate_version, p.active
                FROM attribute_row_filter_t p
                JOIN attribute_t a ON a.host_id = p.host_id AND a.attribute_id = p.attribute_id
                JOIN api_endpoint_t ae ON ae.host_id = p.host_id AND ae.endpoint_id = p.endpoint_id
                JOIN api_version_t av ON av.host_id = ae.host_id AND av.api_version_id = ae.api_version_id
                WHERE p.host_id = ?
            """;

        List<Object> parameters = new ArrayList<>();
        parameters.add(UUID.fromString(hostId));

        String activeClause = SqlUtil.buildMultiTableActiveClause(active, "p", "a", "ae", "av");
        String[] searchColumns = {"a.attribute_id", "p.attribute_value", "ae.endpoint", "p.col_name", "p.col_value"};
        String sqlBuilder = s + activeClause +
                dynamicFilter(Arrays.asList("a.host_id", "p.endpoint_id", "av.api_version_id"), Arrays.asList(searchColumns), filters, columnMap, parameters) +
                globalFilter(globalFilter, searchColumns, parameters) +
                dynamicSorting("a.attribute_id, av.api_id, av.api_version, ae.endpoint, p.col_name, p.operator", sorting, columnMap) +
                "\nLIMIT ? OFFSET ?";

        parameters.add(limit);
        parameters.add(offset);

        if(logger.isTraceEnabled()) logger.trace("queryAttributeRowFilter sql: {}", sqlBuilder);
        int total = 0;
        List<Map<String, Object>> attributeRowFilters = new ArrayList<>();

        try (final Connection conn = ds.getConnection(); PreparedStatement preparedStatement = conn.prepareStatement(sqlBuilder)) {
            populateParameters(preparedStatement, parameters);
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
                    map.put("endpointId", resultSet.getString("endpoint_id"));
                    map.put("apiVersionId", resultSet.getObject("api_version_id", UUID.class));
                    map.put("apiId", resultSet.getString("api_id"));
                    map.put("apiVersion", resultSet.getString("api_version"));
                    map.put("endpoint", resultSet.getString("endpoint"));
                    map.put("colName", resultSet.getString("col_name"));
                    map.put("operator", resultSet.getString("operator"));
                    map.put("colValue", resultSet.getString("col_value"));
                    map.put("updateUser", resultSet.getString("update_user"));
                    map.put("updateTs", resultSet.getObject("update_ts") != null ? resultSet.getObject("update_ts", OffsetDateTime.class) : null);
                    map.put("aggregateVersion", resultSet.getLong("aggregate_version"));
                    map.put("active", resultSet.getBoolean("active"));
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
    public Result<String> getAttributeRowFilterById(String hostId, String attributeId, String endpointId, String colName) {
        final String sql =
                """
                SELECT host_id, attribute_id, endpoint_id, attribute_value, col_name, operator, col_value,
                aggregate_version, active, update_user, update_ts
                FROM attribute_row_filter_t
                WHERE host_id = ? AND attribute_id = ? AND endpoint_id = ? AND col_name = ?
                """;
        Result<String> result;
        Map<String, Object> map = new HashMap<>();

        String searchId = hostId + ":" + attributeId + ":" + endpointId + ":" + colName;

        try (Connection conn = ds.getConnection();
             PreparedStatement statement = conn.prepareStatement(sql)) {

            statement.setObject(1, UUID.fromString(hostId));
            statement.setString(2, attributeId);
            statement.setObject(3, UUID.fromString(endpointId));
            statement.setString(4, colName);

            try (ResultSet resultSet = statement.executeQuery()) {
                if (resultSet.next()) {
                    map.put("hostId", resultSet.getObject("host_id", UUID.class));
                    map.put("attributeId", resultSet.getString("attribute_id"));
                    map.put("endpointId", resultSet.getObject("endpoint_id", UUID.class));
                    map.put("attributeValue", resultSet.getString("attribute_value"));
                    map.put("colName", resultSet.getString("col_name"));
                    map.put("operator", resultSet.getString("operator"));
                    map.put("colValue", resultSet.getString("col_value"));
                    map.put("aggregateVersion", resultSet.getLong("aggregate_version"));
                    map.put("active", resultSet.getBoolean("active"));
                    map.put("updateUser", resultSet.getString("update_user"));
                    map.put("updateTs", resultSet.getObject("update_ts") != null ? resultSet.getObject("update_ts", OffsetDateTime.class) : null);

                    result = Success.of(JsonMapper.toJson(map));
                } else {
                    result = Failure.of(new Status(OBJECT_NOT_FOUND, "attribute_row_filter", searchId));
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
    public void createAttributeRowFilter(Connection conn, Map<String, Object> event) throws SQLException, Exception {
        // Use UPSERT: INSERT ON CONFLICT DO UPDATE
        // This handles:
        // 1. First time insert (no conflict).
        // 2. Re-creation (conflict on host_id, attribute_id, endpoint_id, col_name) -> UPDATE the existing soft-deleted row (setting active=TRUE and new version).

        final String sql =
                """
                INSERT INTO attribute_row_filter_t (
                    host_id,
                    attribute_id,
                    attribute_value,
                    endpoint_id,
                    col_name,
                    operator,
                    col_value,
                    update_user,
                    update_ts,
                    aggregate_version,
                    active
                ) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, TRUE)
                ON CONFLICT (host_id, attribute_id, endpoint_id, col_name) DO UPDATE
                SET attribute_value = EXCLUDED.attribute_value,
                    operator = EXCLUDED.operator,
                    col_value = EXCLUDED.col_value,
                    update_user = EXCLUDED.update_user,
                    update_ts = EXCLUDED.update_ts,
                    aggregate_version = EXCLUDED.aggregate_version,
                    active = TRUE
                -- OCC/IDM: Only update if the incoming event is newer
                WHERE attribute_row_filter_t.aggregate_version < EXCLUDED.aggregate_version AND attribute_row_filter_t.active = FALSE
                """;

        // Note: The original code uses a non-standard map retrieval: Map<String, Object> map = (Map<String, Object>)event.get(PortalConstants.DATA);
        // Assuming SqlUtil.extractEventData(event) is the correct utility based on other methods.
        Map<String, Object> map = SqlUtil.extractEventData(event);

        String hostId = (String)map.get("hostId");
        String attributeId = (String)map.get("attributeId");
        String endpointId = (String)map.get("endpointId");
        String colName = (String)map.get("colName");
        long newAggregateVersion = SqlUtil.getNewAggregateVersion(event);

        try (PreparedStatement statement = conn.prepareStatement(sql)) {
            // INSERT values (10 placeholders + active=TRUE in SQL, total 10 dynamic values)
            // 1: host_id
            statement.setObject(1, UUID.fromString(hostId));
            // 2: attribute_id
            statement.setString(2, attributeId);
            // 3: attribute_value
            statement.setString(3, (String)map.get("attributeValue"));
            // 4: endpoint_id
            statement.setObject(4, UUID.fromString(endpointId));
            // 5: col_name
            statement.setString(5, colName);
            // 6: operator
            statement.setString(6, (String)map.get("operator"));
            // 7: col_value
            statement.setString(7, (String)map.get("colValue"));
            // 8: update_user
            statement.setString(8, (String)event.get(Constants.USER));
            // 9: update_ts
            statement.setObject(9, OffsetDateTime.parse((String)event.get(CloudEventV1.TIME)));
            // 10: aggregate_version
            statement.setLong(10, newAggregateVersion);

            int count = statement.executeUpdate();
            if (count == 0) {
                // count=0 means the ON CONFLICT clause was hit, BUT the WHERE clause (aggregate_version < EXCLUDED.aggregate_version) failed.
                // This is the desired idempotent/out-of-order protection behavior. Log and ignore.
                logger.warn("Creation/Reactivation skipped for hostId {} attributeId {} endpointId {} colName {} aggregateVersion {}. A newer or same version already exists.", hostId, attributeId, endpointId, colName, newAggregateVersion);
            }
        } catch (SQLException e) {
            logger.error("SQLException during createAttributeRowFilter for hostId {} attributeId {} endpointId {} colName {} aggregateVersion {}: {}", hostId, attributeId, endpointId, colName, newAggregateVersion, e.getMessage(), e);
            throw e;
        } catch (Exception e) {
            logger.error("Exception during createAttributeRowFilter for hostId {} attributeId {} endpointId {} colName {} aggregateVersion {}: {}", hostId, attributeId, endpointId, colName, newAggregateVersion, e.getMessage(), e);
            throw e;
        }
    }

    @Override
    public void updateAttributeRowFilter(Connection conn, Map<String, Object> event) throws SQLException, Exception {
        // We attempt to update the record IF the incoming event's aggregate_version is greater than the current projection's version.
        // This enforces Idempotence (IDM) and Optimistic Concurrency Control (OCC) by ensuring version monotonicity.
        // We explicitly set active = TRUE as an UPDATE event implies the row filter should be active.
        final String sql =
                """
                UPDATE attribute_row_filter_t
                SET
                    attribute_value = ?,
                    operator = ?,
                    col_value = ?,
                    update_user = ?,
                    update_ts = ?,
                    aggregate_version = ?,
                    active = TRUE
                WHERE
                    host_id = ?
                    AND attribute_id = ?
                    AND endpoint_id = ?
                    AND col_name = ?
                    AND aggregate_version < ?
                """; // <<< CRITICAL: Changed aggregate_version = ? to aggregate_version < ? to enforce monotonicity (OCC/IDM)

        // Note: The original code uses a non-standard map retrieval: Map<String, Object> map = (Map<String, Object>)event.get(PortalConstants.DATA);
        // Assuming SqlUtil.extractEventData(event) is the correct utility based on other methods.
        Map<String, Object> map = SqlUtil.extractEventData(event);

        String hostId = (String)map.get("hostId");
        String attributeId = (String)map.get("attributeId");
        String endpointId = (String)map.get("endpointId");
        String colName = (String)map.get("colName");
        long newAggregateVersion = SqlUtil.getNewAggregateVersion(event);

        try (PreparedStatement statement = conn.prepareStatement(sql)) {
            // SET values (6 dynamic values + active = TRUE in SQL)
            // 1: attribute_value
            statement.setString(1, (String)map.get("attributeValue"));
            // 2: operator
            statement.setString(2, (String)map.get("operator"));
            // 3: col_value
            statement.setString(3, (String)map.get("colValue"));
            // 4: update_user
            statement.setString(4, (String)event.get(Constants.USER));
            // 5: update_ts
            statement.setObject(5, OffsetDateTime.parse((String)event.get(CloudEventV1.TIME)));
            // 6: aggregate_version
            statement.setLong(6, newAggregateVersion);

            // WHERE conditions (5 placeholders)
            // 7: host_id
            statement.setObject(7, UUID.fromString(hostId));
            // 8: attribute_id
            statement.setString(8, attributeId);
            // 9: endpoint_id
            statement.setObject(9, UUID.fromString(endpointId));
            // 10: col_name
            statement.setString(10, colName);
            // 11: aggregate_version < ? (new version for OCC/IDM check)
            statement.setLong(11, newAggregateVersion);


            int count = statement.executeUpdate();
            if (count == 0) {
                // If 0 rows updated, it means the record was either not found
                // OR aggregate_version >= newAggregateVersion (OCC/IDM check failed).
                // We IGNORE the failure and log a warning, as this is the desired idempotent/monotonic behavior.
                logger.warn("Update skipped for hostId {} attributeId {} endpointId {} colName {} aggregateVersion {}. Record not found or a newer/same version already exists.", hostId, attributeId, endpointId, colName, newAggregateVersion);
            }
        } catch (SQLException e) {
            logger.error("SQLException during updateAttributeRowFilter for hostId {} attributeId {} endpointId {} colName {} aggregateVersion {}: {}", hostId, attributeId, endpointId, colName, newAggregateVersion, e.getMessage(), e);
            throw e;
        } catch (Exception e) {
            logger.error("Exception during updateAttributeRowFilter for hostId {} attributeId {} endpointId {} colName {} aggregateVersion {}: {}", hostId, attributeId, endpointId, colName, newAggregateVersion, e.getMessage(), e);
            throw e;
        }
    }

    @Override
    public void deleteAttributeRowFilter(Connection conn, Map<String, Object> event) throws SQLException, Exception {
        // Use UPDATE to implement Soft Delete (setting active = FALSE).
        // OCC/IDM is enforced by checking aggregate_version < newAggregateVersion.
        final String sql =
                """
                UPDATE attribute_row_filter_t
                SET active = FALSE,
                    update_user = ?,
                    update_ts = ?,
                    aggregate_version = ?
                WHERE host_id = ?
                  AND attribute_id = ?
                  AND endpoint_id = ?
                  AND col_name = ?
                  AND aggregate_version < ?
                """; // <<< CRITICAL: Added aggregate_version < ? to enforce monotonicity (OCC/IDM)

        // Note: The original code uses a non-standard map retrieval: Map<String, Object> map = (Map<String, Object>)event.get(PortalConstants.DATA);
        // Assuming SqlUtil.extractEventData(event) is the correct utility based on other methods.
        Map<String, Object> map = SqlUtil.extractEventData(event);

        String hostId = (String)map.get("hostId");
        String attributeId = (String)map.get("attributeId");
        String endpointId = (String)map.get("endpointId");
        String colName = (String)map.get("colName");
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

            // WHERE conditions (5 placeholders)
            // 4: host_id
            statement.setObject(4, UUID.fromString(hostId));
            // 5: attribute_id
            statement.setString(5, attributeId);
            // 6: endpoint_id
            statement.setObject(6, UUID.fromString(endpointId));
            // 7: col_name
            statement.setString(7, colName);
            // 8: aggregate_version < ? (new version for OCC/IDM check)
            statement.setLong(8, newAggregateVersion);

            int count = statement.executeUpdate();
            if (count == 0) {
                // If 0 rows updated, it means:
                // 1. The record was not found (already deleted or never existed).
                // 2. The OCC/IDM check failed (aggregate_version >= newAggregateVersion).
                // We IGNORE the failure and log a warning, as this is the desired idempotent/monotonic behavior.
                logger.warn("Soft delete skipped for hostId {} attributeId {} endpointId {} colName {} aggregateVersion {}. Record not found or a newer/same version already exists.", hostId, attributeId, endpointId, colName, newAggregateVersion);
            }
        } catch (SQLException e) {
            logger.error("SQLException during deleteAttributeRowFilter for hostId {} attributeId {} endpointId {} colName {} aggregateVersion {}: {}", hostId, attributeId, endpointId, colName, newAggregateVersion, e.getMessage(), e);
            throw e;
        } catch (Exception e) {
            logger.error("Exception during deleteAttributeRowFilter for hostId {} attributeId {} endpointId {} colName {} aggregateVersion {}: {}", hostId, attributeId, endpointId, colName, newAggregateVersion, e.getMessage(), e);
            throw e;
        }
    }

    @Override
    public Result<String> queryAttributeColFilter(int offset, int limit, String filtersJson, String globalFilter, String sortingJson, boolean active, String hostId) {
        Result<String> result;
        final Map<String, String> columnMap = new HashMap<>(Map.of(
                "hostId", "a.host_id",
                "attributeId", "a.attribute_id",
                "attributeType", "a.attribute_type",
                "attributeValue", "cf.attribute_value",
                "endpointId", "ae.endpoint_id",
                "endpoint", "ae.endpoint",
                "apiVersionId", "av.api_version_id",
                "apiId", "av.api_id",
                "apiVersion", "av.api_version",
                "columns", "cf.columns"
        ));
        columnMap.put("updateUser", "cf.update_user");
        columnMap.put("updateTs", "cf.update_ts");
        columnMap.put("aggregateVersion", "rcf.aggregate_version");
        columnMap.put("active", "cf.active");


        List<Map<String, Object>> filters = parseJsonList(filtersJson);
        List<Map<String, Object>> sorting = parseJsonList(sortingJson);

        String s =
            """
            SELECT COUNT(*) OVER () AS total,
            a.host_id, a.attribute_id, a.attribute_type, cf.attribute_value,
            av.api_version_id, av.api_id, av.api_version, ae.endpoint_id, ae.endpoint, cf.columns, cf.aggregate_version,
            cf.update_user, cf.update_ts, cf.active
            FROM attribute_col_filter_t cf
            JOIN attribute_t a ON a.attribute_id = cf.attribute_id
            JOIN api_endpoint_t ae ON cf.host_id = ae.host_id AND cf.endpoint_id = ae.endpoint_id
            JOIN api_version_t av ON ae.host_id = av.host_id AND ae.api_version_id = av.api_version_id
            WHERE cf.host_id = ?
            """;

        List<Object> parameters = new ArrayList<>();
        parameters.add(UUID.fromString(hostId));

        String activeClause = SqlUtil.buildMultiTableActiveClause(active, "cf", "a", "ae", "av");
        String[] searchColumns = {"a.attribute_id", "cf.attribute_value", "ae.endpoint", "cf.columns"};
        String sqlBuilder = s + activeClause +
                dynamicFilter(Arrays.asList("a.host_id", "av.api_version_id", "ae.endpoint_id"), Arrays.asList(searchColumns), filters, columnMap, parameters) +
                globalFilter(globalFilter, searchColumns, parameters) +
                dynamicSorting("a.attribute_id, av.api_id, av.api_version, ae.endpoint", sorting, columnMap) +
                "\nLIMIT ? OFFSET ?";

        parameters.add(limit);
        parameters.add(offset);

        if(logger.isTraceEnabled()) logger.trace("queryAttributeColFilter sql: {}", sqlBuilder);
        int total = 0;
        List<Map<String, Object>> attributeColFilters = new ArrayList<>();

        try (final Connection conn = ds.getConnection(); PreparedStatement preparedStatement = conn.prepareStatement(sqlBuilder)) {
            populateParameters(preparedStatement, parameters);
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
                    map.put("apiVersionId", resultSet.getObject("api_version_id", UUID.class));
                    map.put("endpointId", resultSet.getObject("endpoint_id", UUID.class));
                    map.put("apiId", resultSet.getString("api_id"));
                    map.put("apiVersion", resultSet.getString("api_version"));
                    map.put("endpoint", resultSet.getString("endpoint"));
                    map.put("columns", resultSet.getString("columns"));
                    map.put("updateUser", resultSet.getString("update_user"));
                    map.put("updateTs", resultSet.getObject("update_ts") != null ? resultSet.getObject("update_ts", OffsetDateTime.class) : null);
                    map.put("aggregateVersion", resultSet.getLong("aggregate_version"));
                    map.put("active", resultSet.getBoolean("active"));
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
    public Result<String> getAttributeColFilterById(String hostId, String attributeId, String endpointId) {
        final String sql =
                """
                SELECT host_id, attribute_id, endpoint_id, attribute_value, columns,
                aggregate_version, active, update_user, update_ts
                FROM attribute_col_filter_t
                WHERE host_id = ? AND attribute_id = ? AND endpoint_id = ?
                """;
        Result<String> result;
        Map<String, Object> map = new HashMap<>();

        String searchId = hostId + ":" + attributeId + ":" + endpointId;

        try (Connection conn = ds.getConnection();
             PreparedStatement statement = conn.prepareStatement(sql)) {

            statement.setObject(1, UUID.fromString(hostId));
            statement.setString(2, attributeId);
            statement.setObject(3, UUID.fromString(endpointId));

            try (ResultSet resultSet = statement.executeQuery()) {
                if (resultSet.next()) {
                    map.put("hostId", resultSet.getObject("host_id", UUID.class));
                    map.put("attributeId", resultSet.getString("attribute_id"));
                    map.put("endpointId", resultSet.getObject("endpoint_id", UUID.class));
                    map.put("attributeValue", resultSet.getString("attribute_value"));
                    map.put("columns", resultSet.getString("columns"));
                    map.put("aggregateVersion", resultSet.getLong("aggregate_version"));
                    map.put("active", resultSet.getBoolean("active"));
                    map.put("updateUser", resultSet.getString("update_user"));
                    map.put("updateTs", resultSet.getObject("update_ts") != null ? resultSet.getObject("update_ts", OffsetDateTime.class) : null);

                    result = Success.of(JsonMapper.toJson(map));
                } else {
                    result = Failure.of(new Status(OBJECT_NOT_FOUND, "attribute_col_filter", searchId));
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
    public void createAttributeColFilter(Connection conn, Map<String, Object> event) throws SQLException, Exception {
        // Use UPSERT: INSERT ON CONFLICT DO UPDATE
        // This handles:
        // 1. First time insert (no conflict).
        // 2. Re-creation (conflict on host_id, attribute_id, endpoint_id) -> UPDATE the existing soft-deleted row (setting active=TRUE and new version).

        final String sql =
                """
                INSERT INTO attribute_col_filter_t (
                    host_id,
                    attribute_id,
                    attribute_value,
                    endpoint_id,
                    columns,
                    update_user,
                    update_ts,
                    aggregate_version,
                    active
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, TRUE)
                ON CONFLICT (host_id, attribute_id, endpoint_id) DO UPDATE
                SET attribute_value = EXCLUDED.attribute_value,
                    columns = EXCLUDED.columns,
                    update_user = EXCLUDED.update_user,
                    update_ts = EXCLUDED.update_ts,
                    aggregate_version = EXCLUDED.aggregate_version,
                    active = TRUE
                -- OCC/IDM: Only update if the incoming event is newer
                WHERE attribute_col_filter_t.aggregate_version < EXCLUDED.aggregate_version AND attribute_col_filter_t.active = FALSE
                """;

        // Note: The original code uses a non-standard map retrieval: Map<String, Object> map = (Map<String, Object>)event.get(PortalConstants.DATA);
        // Assuming SqlUtil.extractEventData(event) is the correct utility based on other methods.
        Map<String, Object> map = SqlUtil.extractEventData(event);

        String hostId = (String)map.get("hostId");
        String attributeId = (String)map.get("attributeId");
        String endpointId = (String)map.get("endpointId");
        long newAggregateVersion = SqlUtil.getNewAggregateVersion(event);

        try (PreparedStatement statement = conn.prepareStatement(sql)) {
            // INSERT values (8 placeholders + active=TRUE in SQL, total 8 dynamic values)
            // 1: host_id
            statement.setObject(1, UUID.fromString(hostId));
            // 2: attribute_id
            statement.setString(2, attributeId);
            // 3: attribute_value
            statement.setString(3, (String)map.get("attributeValue"));
            // 4: endpoint_id
            statement.setObject(4, UUID.fromString(endpointId));
            // 5: columns
            statement.setString(5, (String)map.get("columns"));
            // 6: update_user
            statement.setString(6, (String)event.get(Constants.USER));
            // 7: update_ts
            statement.setObject(7, OffsetDateTime.parse((String)event.get(CloudEventV1.TIME)));
            // 8: aggregate_version
            statement.setLong(8, newAggregateVersion);

            int count = statement.executeUpdate();
            if (count == 0) {
                // count=0 means the ON CONFLICT clause was hit, BUT the WHERE clause (aggregate_version < EXCLUDED.aggregate_version) failed.
                // This is the desired idempotent/out-of-order protection behavior. Log and ignore.
                logger.warn("Creation/Reactivation skipped for hostId {} attributeId {} endpointId {} aggregateVersion {}. A newer or same version already exists.", hostId, attributeId, endpointId, newAggregateVersion);
            }
        } catch (SQLException e) {
            logger.error("SQLException during createAttributeColFilter for hostId {} attributeId {} endpointId {} aggregateVersion {}: {}", hostId, attributeId, endpointId, newAggregateVersion, e.getMessage(), e);
            throw e;
        } catch (Exception e) {
            logger.error("Exception during createAttributeColFilter for hostId {} attributeId {} endpointId {} aggregateVersion {}: {}", hostId, attributeId, endpointId, newAggregateVersion, e.getMessage(), e);
            throw e;
        }
    }


    @Override
    public void updateAttributeColFilter(Connection conn, Map<String, Object> event) throws SQLException, Exception {
        // We attempt to update the record IF the incoming event's aggregate_version is greater than the current projection's version.
        // This enforces Idempotence (IDM) and Optimistic Concurrency Control (OCC) by ensuring version monotonicity.
        // We explicitly set active = TRUE as an UPDATE event implies the column filter should be active.
        final String sql =
                """
                UPDATE attribute_col_filter_t
                SET
                    attribute_value = ?,
                    columns = ?,
                    update_user = ?,
                    update_ts = ?, -- Added missing comma from original SQL
                    aggregate_version = ?,
                    active = TRUE
                WHERE
                    host_id = ?
                    AND attribute_id = ?
                    AND endpoint_id = ?
                    AND aggregate_version < ?
                """; // <<< CRITICAL: Changed aggregate_version = ? to aggregate_version < ? to enforce monotonicity (OCC/IDM)

        // Note: The original code uses a non-standard map retrieval: Map<String, Object> map = (Map<String, Object>)event.get(PortalConstants.DATA);
        // Assuming SqlUtil.extractEventData(event) is the correct utility based on other methods.
        Map<String, Object> map = SqlUtil.extractEventData(event);

        String hostId = (String)map.get("hostId");
        String attributeId = (String)map.get("attributeId");
        String endpointId = (String)map.get("endpointId");
        long newAggregateVersion = SqlUtil.getNewAggregateVersion(event);

        try (PreparedStatement statement = conn.prepareStatement(sql)) {
            // SET values (5 dynamic values + active = TRUE in SQL)
            // 1: attribute_value
            statement.setString(1, (String)map.get("attributeValue"));
            // 2: columns
            statement.setString(2, (String)map.get("columns"));
            // 3: update_user
            statement.setString(3, (String)event.get(Constants.USER));
            // 4: update_ts
            statement.setObject(4, OffsetDateTime.parse((String)event.get(CloudEventV1.TIME)));
            // 5: aggregate_version
            statement.setLong(5, newAggregateVersion);

            // WHERE conditions (4 placeholders)
            // 6: host_id
            statement.setObject(6, UUID.fromString(hostId));
            // 7: attribute_id
            statement.setString(7, attributeId);
            // 8: endpoint_id
            statement.setObject(8, UUID.fromString(endpointId));
            // 9: aggregate_version < ? (new version for OCC/IDM check)
            statement.setLong(9, newAggregateVersion);

            int count = statement.executeUpdate();
            if (count == 0) {
                // If 0 rows updated, it means the record was either not found
                // OR aggregate_version >= newAggregateVersion (OCC/IDM check failed).
                // We IGNORE the failure and log a warning, as this is the desired idempotent/monotonic behavior.
                logger.warn("Update skipped for hostId {} attributeId {} endpointId {} aggregateVersion {}. Record not found or a newer/same version already exists.", hostId, attributeId, endpointId, newAggregateVersion);
            }
        } catch (SQLException e) {
            logger.error("SQLException during updateAttributeColFilter for hostId {} attributeId {} endpointId {} aggregateVersion {}: {}", hostId, attributeId, endpointId, newAggregateVersion, e.getMessage(), e);
            throw e;
        } catch (Exception e) {
            logger.error("Exception during updateAttributeColFilter for hostId {} attributeId {} endpointId {} aggregateVersion {}: {}", hostId, attributeId, endpointId, newAggregateVersion, e.getMessage(), e);
            throw e;
        }
    }

    @Override
    public void deleteAttributeColFilter(Connection conn, Map<String, Object> event) throws SQLException, Exception {
        // Use UPDATE to implement Soft Delete (setting active = FALSE).
        // OCC/IDM is enforced by checking aggregate_version < newAggregateVersion.
        final String sql =
                """
                UPDATE attribute_col_filter_t
                SET active = FALSE,
                    update_user = ?,
                    update_ts = ?,
                    aggregate_version = ?
                WHERE host_id = ?
                  AND attribute_id = ?
                  AND endpoint_id = ?
                  AND aggregate_version < ?
                """; // <<< CRITICAL: Added aggregate_version < ? to enforce monotonicity (OCC/IDM)

        // Note: The original code uses a non-standard map retrieval: Map<String, Object> map = (Map<String, Object>)event.get(PortalConstants.DATA);
        // Assuming SqlUtil.extractEventData(event) is the correct utility based on other methods.
        Map<String, Object> map = SqlUtil.extractEventData(event);

        String hostId = (String)map.get("hostId");
        String attributeId = (String)map.get("attributeId");
        String endpointId = (String)map.get("endpointId");
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

            // WHERE conditions (4 placeholders)
            // 4: host_id
            statement.setObject(4, UUID.fromString(hostId));
            // 5: attribute_id
            statement.setString(5, attributeId);
            // 6: endpoint_id
            statement.setObject(6, UUID.fromString(endpointId));
            // 7: aggregate_version < ? (new version for OCC/IDM check)
            statement.setLong(7, newAggregateVersion);

            int count = statement.executeUpdate();
            if (count == 0) {
                // If 0 rows updated, it means:
                // 1. The record was not found (already deleted or never existed).
                // 2. The OCC/IDM check failed (aggregate_version >= newAggregateVersion).
                // We IGNORE the failure and log a warning, as this is the desired idempotent/monotonic behavior.
                logger.warn("Soft delete skipped for hostId {} attributeId {} endpointId {} aggregateVersion {}. Record not found or a newer/same version already exists.", hostId, attributeId, endpointId, newAggregateVersion);
            }
        } catch (SQLException e) {
            logger.error("SQLException during deleteAttributeColFilter for hostId {} attributeId {} endpointId {} aggregateVersion {}: {}", hostId, attributeId, endpointId, newAggregateVersion, e.getMessage(), e);
            throw e;
        } catch (Exception e) {
            logger.error("Exception during deleteAttributeColFilter for hostId {} attributeId {} endpointId {} aggregateVersion {}: {}", hostId, attributeId, endpointId, newAggregateVersion, e.getMessage(), e);
            throw e;
        }
    }
}
