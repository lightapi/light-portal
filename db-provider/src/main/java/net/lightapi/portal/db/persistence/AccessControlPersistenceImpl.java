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
        final String insertRole = "INSERT INTO role_t (host_id, role_id, role_desc, update_user, update_ts, aggregate_version) " +
                "VALUES (?, ?, ?, ?, ?,  ?)";

        Map<String, Object> map = SqlUtil.extractEventData(event);
        String hostId = (String)map.get("hostId");
        String roleId = (String)map.get("roleId");
        long newAggregateVersion = SqlUtil.getNewAggregateVersion(event);
        try (PreparedStatement statement = conn.prepareStatement(insertRole)) {
            statement.setObject(1, UUID.fromString(hostId));
            statement.setString(2, roleId);
            String roleDesc = (String)map.get("roleDesc");
            if (roleDesc != null && !roleDesc.isEmpty())
                statement.setString(3, roleDesc);
            else
                statement.setNull(3, NULL);

            statement.setString(4, (String)event.get(Constants.USER));
            statement.setObject(5, OffsetDateTime.parse((String)event.get(CloudEventV1.TIME)));
            statement.setLong(6, newAggregateVersion);

            int count = statement.executeUpdate();
            if (count == 0) {
                throw new SQLException(String.format("Failed during createRole for hostId %s roleId %s with aggregateVersion %d. It might already exist.", hostId, roleId, newAggregateVersion));
            }
        } catch (SQLException e) {
            logger.error("SQLException during createRole for hostId {} roleId {} aggregateVersion {}: {}", hostId, roleId, newAggregateVersion, e.getMessage(), e);
            throw e;
        } catch (Exception e) {
            logger.error("Exception during createRole for hostId {} roleId {} aggregateVersion {}: {}", hostId, roleId, newAggregateVersion, e.getMessage(), e);
            throw e;
        }
    }

    // Helper to check if a role exists (to differentiate 'not found' from 'conflict')
    private boolean queryRoleExists(Connection conn, String hostId, String roleId) throws SQLException {
        final String sql = "SELECT COUNT(*) FROM role_t WHERE host_id = ? AND role_id = ?";
        try (PreparedStatement pst = conn.prepareStatement(sql)) {
            pst.setObject(1, UUID.fromString(hostId));
            pst.setString(2, roleId);
            try (ResultSet rs = pst.executeQuery()) {
                return rs.next() && rs.getInt(1) > 0;
            }
        }
    }

    @Override
    public void updateRole(Connection conn, Map<String, Object> event) throws SQLException, Exception {
        final String updateRole = "UPDATE role_t SET role_desc = ?, update_user = ?, update_ts = ?, aggregate_version = ? " +
                "WHERE host_id = ? AND role_id = ? AND aggregate_version = ?";

        Map<String, Object> map = SqlUtil.extractEventData(event);
        String hostId = (String)map.get("hostId");
        String roleId = (String)map.get("roleId");
        long oldAggregateVersion = SqlUtil.getOldAggregateVersion(event);
        long newAggregateVersion = SqlUtil.getNewAggregateVersion(event);

        try (PreparedStatement statement = conn.prepareStatement(updateRole)) {
            String roleDesc = (String)map.get("roleDesc");
            if(roleDesc != null && !roleDesc.isEmpty()) {
                statement.setString(1, roleDesc);
            } else {
                statement.setNull(1, NULL);
            }
            statement.setString(2, (String)event.get(Constants.USER));
            statement.setObject(3, OffsetDateTime.parse((String)event.get(CloudEventV1.TIME)));
            statement.setLong(4, newAggregateVersion);
            statement.setObject(5, UUID.fromString(hostId));
            statement.setString(6, roleId);
            statement.setLong(7, oldAggregateVersion);

            int count = statement.executeUpdate();
            if (count == 0) {
                if (queryRoleExists(conn, hostId, roleId)) {
                    throw new ConcurrencyException(String.format("Optimistic concurrency conflict during updateRole for hostId %s roleId %s. Expected version %d but found a different version %d.", hostId, roleId, oldAggregateVersion, newAggregateVersion));
                } else {
                    throw new SQLException(String.format("No record found during updateRole for hostId %s roleId %s.", hostId, roleId));
                }
            }
        } catch (SQLException e) {
            logger.error("SQLException during updateRole for hostId {} roleId {} (old: {}) -> (new: {}): {}", hostId, roleId, oldAggregateVersion, newAggregateVersion, e.getMessage(), e);
            throw e;
        } catch (Exception e) {
            logger.error("Exception during updateRole for hostId {} roleId {} (old: {}) -> (new: {}): {}", hostId, roleId, oldAggregateVersion, newAggregateVersion, e.getMessage(), e);
            throw e;
        }
    }


    @Override
    public void deleteRole(Connection conn, Map<String, Object> event) throws SQLException, Exception {
        final String deleteRole = "DELETE from role_t WHERE host_id = ? AND role_id = ? AND aggregate_version = ?";
        Map<String, Object> map = SqlUtil.extractEventData(event);
        String roleId = (String)map.get("roleId");
        String hostId = (String)map.get("hostId");
        long oldAggregateVersion = SqlUtil.getOldAggregateVersion(event);
        try (PreparedStatement statement = conn.prepareStatement(deleteRole)) {
            statement.setObject(1, UUID.fromString(hostId));
            statement.setString(2, roleId);
            statement.setLong(3, oldAggregateVersion);
            int count = statement.executeUpdate();
            if (count == 0) {
                if (queryRoleExists(conn, hostId, roleId)) {
                    throw new ConcurrencyException(String.format("Optimistic concurrency conflict during deleteRole for hostId %s roleId %s aggregateVersion %d but found a different version or already updated. ", hostId, roleId, oldAggregateVersion));
                } else {
                    throw new SQLException(String.format("No record found during deleteRole for hostId %s roleId %s. It might have been already deleted.", hostId, roleId)) ;
                }
            }
        } catch (SQLException e) {
            logger.error("SQLException during deleteRole for hostId {} roleId {} aggregateVersion {}: {}", hostId, roleId, oldAggregateVersion, e.getMessage(), e);
            throw e;
        } catch (Exception e) {
            logger.error("Exception during deleteRole for hostId {} roleId {} aggregateVersion {}: {}", hostId, roleId, oldAggregateVersion, e.getMessage(), e);
            throw e;
        }
    }

    @Override
    public Result<String> queryRole(int offset, int limit, String filtersJson, String globalFilter, String sortingJson, String hostId) {
        Result<String> result;
        List<Map<String, Object>> filters = parseJsonList(filtersJson);
        List<Map<String, Object>> sorting = parseJsonList(sortingJson);

        String s =
            """
                SELECT COUNT(*) OVER () AS total,
                host_id, role_id, role_desc, update_user,
                update_ts, aggregate_version
                FROM role_t
                WHERE host_id = ?
            """;

        List<Object> parameters = new ArrayList<>();
        parameters.add(UUID.fromString(hostId));


        String[] searchColumns = {"role_id", "role_desc"};
        String sqlBuilder = s + dynamicFilter(Arrays.asList("host_id"), filters, null, parameters) +
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
    public Result<String> queryRolePermission(int offset, int limit, String filtersJson, String globalFilter, String sortingJson, String hostId) {
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

        List<Map<String, Object>> filters = parseJsonList(filtersJson);
        List<Map<String, Object>> sorting = parseJsonList(sortingJson);

        String s =
                """
                SELECT COUNT(*) OVER () AS total,
                r.host_id, r.role_id, av.api_version_id, av.api_id, av.api_version,
                rp.endpoint_id, ae.endpoint, rp.aggregate_version, rp.update_user, rp.update_ts
                FROM role_permission_t rp
                JOIN role_t r ON r.role_id = rp.role_id
                JOIN api_endpoint_t ae ON rp.host_id = ae.host_id AND rp.endpoint_id = ae.endpoint_id
                JOIN api_version_t av ON ae.host_id = av.host_id AND ae.api_version_id = av.api_version_id
                AND r.host_id = ?
                """;

        List<Object> parameters = new ArrayList<>();
        parameters.add(UUID.fromString(hostId));

        String[] searchColumns = {"r.role_id", "ae.endpoint"};
        String sqlBuilder = s + dynamicFilter(Arrays.asList("r.host_id", "av.api_version_id", "rp.endpoint_id"), filters, columnMap, parameters) +
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
    public Result<String> queryRoleUser(int offset, int limit, String filtersJson, String globalFilter, String sortingJson, String hostId) {
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

        List<Map<String, Object>> filters = parseJsonList(filtersJson);
        List<Map<String, Object>> sorting = parseJsonList(sortingJson);

        String s =
                """
                SELECT COUNT(*) OVER () AS total,
                r.host_id, r.role_id, r.start_ts, r.end_ts,
                r.aggregate_version, r.update_user, r.update_ts,
                u.user_id, u.email, u.user_type,
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


        String[] searchColumns = {"r.role_id", "u.email", "u.first_name", "u.last_name"};
        String sqlBuilder = s + dynamicFilter(Arrays.asList("r.host_id", "u.user_id"), filters, columnMap, parameters) +
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
    public void createRolePermission(Connection conn, Map<String, Object> event) throws SQLException, Exception {
        final String insertRole =
                """
                    INSERT INTO role_permission_t (host_id, role_id, endpoint_id, update_user, update_ts, aggregate_version)
                    VALUES (?, ?, ?, ?, ?, ?)
                """;

        Map<String, Object> map = SqlUtil.extractEventData(event);
        String hostId = (String)map.get("hostId");
        String roleId = (String)map.get("roleId");
        String endpointId = (String)map.get("endpointId");
        long newAggregateVersion = SqlUtil.getNewAggregateVersion(event);
        try (PreparedStatement statement = conn.prepareStatement(insertRole)) {
            statement.setObject(1, UUID.fromString(hostId));
            statement.setString(2, roleId);
            statement.setObject(3, UUID.fromString(endpointId));
            statement.setString(7, (String)event.get(Constants.USER));
            statement.setObject(8, OffsetDateTime.parse((String)event.get(CloudEventV1.TIME)));
            statement.setLong(9, newAggregateVersion);
            int count = statement.executeUpdate();
            if (count == 0) {
                throw new SQLException(String.format("Failed during createRolePermission for hostId %s roleId %s endpointId %s with aggregateVersion %d. It might already exist.", hostId, roleId, endpointId, newAggregateVersion));
            }
        } catch (SQLException e) {
            logger.error("SQLException during createRolePermission for hostId {} roleId {} endpointId {} aggregateVersion {}: {}", hostId, roleId, endpointId, newAggregateVersion, e.getMessage(), e);
            throw e;
        } catch (Exception e) {
            logger.error("Exception during createRolePermission for hostId {} roleId {} endpointId {} aggregateVersion {}: {}", hostId, roleId, endpointId, newAggregateVersion, e.getMessage(), e);
            throw e;
        }
    }

    // Helper to check if a role exists (to differentiate 'not found' from 'conflict')
    private boolean queryRolePermissionExists(Connection conn, String hostId, String roleId, String endpointId) throws SQLException {
        final String sql =
                """
                    SELECT COUNT(*) FROM role_permission_t
                    WHERE host_id = ?
                    AND role_id = ?
                    AND endpoint_id = ?
                """;
        try (PreparedStatement statement = conn.prepareStatement(sql)) {
            statement.setObject(1, UUID.fromString(hostId));
            statement.setString(2, roleId);
            statement.setObject(3, UUID.fromString(endpointId));

            try (ResultSet rs = statement.executeQuery()) {
                return rs.next() && rs.getInt(1) > 0;
            }
        }
    }


    @Override
    public void deleteRolePermission(Connection conn, Map<String, Object> event) throws SQLException, Exception {
        final String deleteRole =
            """
                DELETE FROM role_permission_t
                WHERE host_id = ?
                AND role_id = ?
                AND endpoint_id = ?
                AND aggregate_version = ?
            """;
        Result<String> result;
        Map<String, Object> map = SqlUtil.extractEventData(event);
        String hostId = (String)map.get("hostId");
        String roleId = (String)map.get("roleId");
        String endpointId = (String)map.get("endpointId");
        long oldAggregateVersion = SqlUtil.getOldAggregateVersion(event);
        try (PreparedStatement statement = conn.prepareStatement(deleteRole)) {
            statement.setObject(1, UUID.fromString(hostId));
            statement.setString(2, roleId);
            statement.setObject(3, UUID.fromString(endpointId));
            statement.setLong(4, oldAggregateVersion);
            int count = statement.executeUpdate();
            if (count == 0) {
                if (queryRolePermissionExists(conn, hostId, roleId, endpointId)) {
                    throw new ConcurrencyException(String.format("Optimistic concurrency conflict during deleteRolePermission for hostId %s roleId %s endpointId %s with aggregateVersion %d but found a different version or already updated.", hostId, roleId, endpointId, oldAggregateVersion));
                } else {
                    throw new SQLException(String.format("No record found during deleteRolePermission for hostId %s roleId %s endpointId %s. It might have been already deleted.", hostId, roleId, endpointId));
                }
            }
        } catch (SQLException e) {
            logger.error("SQLException during deleteRolePermission for hostId {} roleId {} endpointId {} aggregateVersion {}: {}", hostId, roleId, endpointId, oldAggregateVersion, e.getMessage(), e);
            throw e;
        } catch (Exception e) {
            logger.error("Exception during deleteRolePermission for hostId {} roleId {} endpointId {} aggregateVersion {}: {}", hostId, roleId, endpointId, oldAggregateVersion, e.getMessage(), e);
            throw e;
        }
    }

    @Override
    public void createRoleUser(Connection conn, Map<String, Object> event) throws SQLException, Exception {
        final String insertRole = "INSERT INTO role_user_t (host_id, role_id, user_id, start_ts, " +
                "end_ts, update_user, update_ts, aggregate_version) " +
                "VALUES (?, ?, ?, ?, ?,  ?, ?, ?)";

        Map<String, Object> map = SqlUtil.extractEventData(event);
        String hostId = (String)map.get("hostId");
        String roleId = (String)map.get("roleId");
        String userId = (String)map.get("userId");
        long newAggregateVersion = SqlUtil.getNewAggregateVersion(event);
        try (PreparedStatement statement = conn.prepareStatement(insertRole)) {
            statement.setObject(1, UUID.fromString(hostId));
            statement.setString(2, roleId);
            statement.setObject(3, UUID.fromString(userId));

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
            statement.setLong(8, newAggregateVersion);

            int count = statement.executeUpdate();
            if (count == 0) {
                throw new SQLException(String.format("Failed during createRoleUser for hostId %s roleId %s userId %s with aggregateVersion %d. It might already exist.", hostId, roleId, userId, newAggregateVersion));
            }
        } catch (SQLException e) {
            logger.error("SQLException during createRoleUser for hostId {} roleId {} userId {} and aggregateVersion {}: {}", hostId, roleId, userId, newAggregateVersion, e.getMessage(), e);
            throw e;
        } catch (Exception e) {
            logger.error("Exception during createRoleUser for hostId {} roleId {} userId {} and aggregateVersion {}: {}", hostId, roleId, userId, newAggregateVersion, e.getMessage(), e);
            throw e;
        }
    }

    // Helper to check if a role exists (to differentiate 'not found' from 'conflict')
    private boolean queryRoleUserExists(Connection conn, String hostId, String roleId, String userId) throws SQLException {
        final String sql =
                """
                SELECT COUNT(*) FROM role_user_t WHERE host_id = ? AND role_id = ? AND user_id = ?
                """;
        try (PreparedStatement pst = conn.prepareStatement(sql)) {
            pst.setObject(1, UUID.fromString(hostId));
            pst.setString(2, roleId);
            pst.setObject(3, UUID.fromString(userId));
            try (ResultSet rs = pst.executeQuery()) {
                return rs.next() && rs.getInt(1) > 0;
            }
        }
    }

    @Override
    public void updateRoleUser(Connection conn, Map<String, Object> event) throws SQLException, Exception {
        final String updateRole =
            """
            UPDATE role_user_t SET start_ts = ?, end_ts = ?, update_user = ?, update_ts = ?, aggregate_version = ?
            WHERE host_id = ? AND role_id = ? AND user_id = ? AND aggregate_version = ?
            """;

        Map<String, Object> map = (Map<String, Object>)event.get(PortalConstants.DATA);
        String hostId = (String)map.get("hostId");
        String roleId = (String)map.get("roleId");
        String userId = (String)map.get("userId");
        long oldAggregateVersion = SqlUtil.getOldAggregateVersion(event);
        long newAggregateVersion = SqlUtil.getNewAggregateVersion(event);

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
            statement.setLong(5, newAggregateVersion);
            statement.setObject(6, UUID.fromString(hostId));
            statement.setString(7, roleId);
            statement.setObject(8, UUID.fromString(userId));
            statement.setLong(9, oldAggregateVersion);

            int count = statement.executeUpdate();
            if (count == 0) {
                if (queryRoleUserExists(conn, hostId, roleId, userId)) {
                    throw new ConcurrencyException(String.format("Optimistic concurrency conflict during updateRoleUser for hostId %s roleId %s userId %s. Expected version %d but found a different version %d.", hostId, roleId, userId, oldAggregateVersion, newAggregateVersion));
                } else {
                    throw new SQLException(String.format("No record found during updateRoleUser for hostId %s roleId %s userId %s.", hostId, roleId, userId));
                }
            }
        } catch (SQLException e) {
            logger.error("SQLException during updateRoleUser for hostId {} roleId {} userId {} (old: {}) -> (new: {}): {}", hostId, roleId, userId, oldAggregateVersion, newAggregateVersion, e.getMessage(), e);
            throw e;
        } catch (Exception e) {
            logger.error("Exception during updateRoleUser for hostId {} roleId {} userId {} (old: {}) -> (new: {}): {}", hostId, roleId, userId, oldAggregateVersion, newAggregateVersion, e.getMessage(), e);
            throw e;
        }
    }

    @Override
    public void deleteRoleUser(Connection conn, Map<String, Object> event) throws SQLException, Exception {
        final String deleteRole =
            """
            DELETE from role_user_t
            WHERE host_id = ? AND role_id = ? AND user_id = ? AND aggregate_version = ?
            """;
        Map<String, Object> map = SqlUtil.extractEventData(event);
        String hostId = (String)map.get("hostId");
        String roleId = (String)map.get("roleId");
        String userId = (String)map.get("userId");
        long oldAggregateVersion = SqlUtil.getOldAggregateVersion(event);
        try (PreparedStatement statement = conn.prepareStatement(deleteRole)) {
            statement.setObject(1, UUID.fromString(hostId));
            statement.setString(2, roleId);
            statement.setObject(3, UUID.fromString(userId));
            statement.setLong(4, oldAggregateVersion);

            int count = statement.executeUpdate();
            if (count == 0) {
                if (queryRoleUserExists(conn, hostId, roleId, userId)) {
                    throw new ConcurrencyException(String.format("Optimistic concurrency conflict during deleteRoleUser for hostId %s roleId %s userId %s aggregateVersion %d but found a different version or already updated. ", hostId, roleId, userId, oldAggregateVersion));
                } else {
                    throw new SQLException(String.format("No record found during deleteRoleUser for hostId %s roleId %s userId %s. It might have been already deleted.", hostId, roleId, userId));
                }
            }
        } catch (SQLException e) {
            logger.error("SQLException during deleteRoleUser for hostId {} roleId {} userId {} aggregateVersion {}: {}", hostId, roleId, userId, oldAggregateVersion,  e.getMessage(), e);
            throw e;
        } catch (Exception e) {
            logger.error("Exception during deleteRoleUser for hostId {} roleId {} userId {} aggregateVersion {}: {}", hostId, roleId, userId, oldAggregateVersion, e.getMessage(), e);
            throw e;
        }
    }

    @Override
    public Result<String> queryRoleRowFilter(int offset, int limit, String filtersJson, String globalFilter, String sortingJson, String hostId) {
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

        List<Map<String, Object>> filters = parseJsonList(filtersJson);
        List<Map<String, Object>> sorting = parseJsonList(sortingJson);

        String s =
                """
                SELECT COUNT(*) OVER () AS total, r.host_id, r.role_id, p.endpoint_id, ae.endpoint,
                av.api_version_id, av.api_id, av.api_version, p.col_name, p.operator, p.col_value,
                p.update_user, p.update_ts, p.aggregate_version
                FROM role_row_filter_t p
                JOIN role_t r ON r.host_id = p.host_id AND r.role_id = p.role_id
                JOIN api_endpoint_t ae ON ae.host_id = p.host_id AND ae.endpoint_id = p.endpoint_id
                JOIN api_version_t av ON av.host_id = ae.host_id AND av.api_version_id = ae.api_version_id
                WHERE p.host_id = ?
                """;

        List<Object> parameters = new ArrayList<>();
        parameters.add(UUID.fromString(hostId));

        String[] searchColumns = {"r.role_id", "ae.endpoint", "p.col_name", "p.col_value"};
        String sqlBuilder = s + dynamicFilter(Arrays.asList("r.host_id", "p.endpoint_id", "av.api_version_id"), filters, columnMap, parameters) +
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

    private boolean queryRoleRowFilterExists(Connection conn, String hostId, String roleId, String endpointId, String colName) throws SQLException {
        final String sql =
                """
                SELECT COUNT(*) FROM role_row_filter_t WHERE host_id = ? AND role_id = ? AND endpoint_id = ? AND col_name = ?
                """;
        try (PreparedStatement pst = conn.prepareStatement(sql)) {
            pst.setObject(1, UUID.fromString(hostId));
            pst.setString(2, roleId);
            pst.setObject(3, UUID.fromString(endpointId));
            pst.setString(4, colName);
            try (ResultSet rs = pst.executeQuery()) {
                return rs.next() && rs.getInt(1) > 0;
            }
        }
    }

    @Override
    public void deleteRoleRowFilter(Connection conn, Map<String, Object> event) throws SQLException, Exception {
        final String deleteRole =
                """
                DELETE from role_row_filter_t
                WHERE host_id = ? AND role_id = ?
                AND endpoint_id = ? AND col_name = ?
                AND aggregate_version = ?
                """;

        Map<String, Object> map = SqlUtil.extractEventData(event);
        String hostId = (String)map.get("hostId");
        String roleId = (String)map.get("roleId");
        String endpointId = (String)map.get("endpointId");
        String colName = (String)map.get("colName");
        long oldAggregateVersion = SqlUtil.getOldAggregateVersion(event);
        try (PreparedStatement statement = conn.prepareStatement(deleteRole)) {
            statement.setObject(1, UUID.fromString(hostId));
            statement.setString(2, roleId);
            statement.setObject(3, UUID.fromString(endpointId));
            statement.setString(4, colName);
            statement.setLong(5, oldAggregateVersion);

            int count = statement.executeUpdate();
            if (count == 0) {
                if (queryRoleRowFilterExists(conn, hostId, roleId, endpointId, colName)) {
                    throw new ConcurrencyException(String.format("Optimistic concurrency conflict during deleteRoleRowFilter for hostId %s roleId %s endpointId %s colName %s aggregateVersion %d but found a different version or already updated.", hostId, roleId, endpointId, colName, oldAggregateVersion));
                } else {
                    throw new SQLException(String.format("No record found during deleteRoleRowFilter for hostId %s roleId %s endpointId %s colName %s. It might have been already deleted.", hostId, roleId, endpointId, colName));
                }
            }
        } catch (SQLException e) {
            logger.error("SQLException during deleteRoleRowFilter for hostId {} roleId {} endpointId {} colName {} aggregateVersion {}: {}", hostId, roleId, endpointId, colName, oldAggregateVersion, e.getMessage(), e);
            throw e;
        } catch (Exception e) {
            logger.error("Exception during deleteRoleRowFilter for hostId {} roleId {} endpointId {} colName {} aggregateVersion {}: {}", hostId, roleId, endpointId, colName, oldAggregateVersion, e.getMessage(), e);
            throw e;
        }
    }

    @Override
    public void createRoleRowFilter(Connection conn, Map<String, Object> event) throws SQLException, Exception {
        final String insertRole = """
                INSERT INTO role_row_filter_t (
                    host_id,
                    role_id,
                    endpoint_id,
                    col_name,
                    operator,
                    col_value,
                    update_user,
                    update_ts,
                    aggregate_version
                ) VALUES (?, ?, ?, ?, ?,  ?, ?, ?, ?)
                """;

        Map<String, Object> map = SqlUtil.extractEventData(event);
        String hostId = (String)map.get("hostId");
        String roleId = (String)map.get("roleId");
        String endpointId = (String)map.get("endpointId");
        String colName = (String)map.get("colName");

        long newAggregateVersion = SqlUtil.getNewAggregateVersion(event);
        try (PreparedStatement statement = conn.prepareStatement(insertRole)) {
            statement.setObject(1, UUID.fromString(hostId));
            statement.setString(2, roleId);
            statement.setObject(3, UUID.fromString(endpointId));
            statement.setString(4, colName);
            statement.setString(5, (String)map.get("operator"));
            statement.setString(6, (String)map.get("colValue"));
            statement.setString(7, (String)event.get(Constants.USER));
            statement.setObject(8, OffsetDateTime.parse((String)event.get(CloudEventV1.TIME)));
            statement.setLong(9, newAggregateVersion);

            int count = statement.executeUpdate();
            if (count == 0) {
                String s = String.format("Failed during createRoleRowFilter for hostId %s roleId %s endpointId %s colName %s with aggregate version %d. It might already exist.", hostId, roleId, endpointId, colName, newAggregateVersion);
                throw new SQLException(s);
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
        final String updateRole =
                """
                    UPDATE role_row_filter_t
                    SET
                        operator = ?,
                        col_value = ?,
                        update_user = ?,
                        update_ts = ?,
                        aggregate_version = ?
                    WHERE
                        host_id = ?
                        AND role_id = ?
                        AND endpoint_id ?
                        AND col_name = ?
                        AND aggregate_version = ?
                """;

        Map<String, Object> map = (Map<String, Object>)event.get(PortalConstants.DATA);
        String hostId = (String)map.get("hostId");
        String roleId = (String)map.get("roleId");
        String endpointId = (String)map.get("endpointId");
        String colName = (String)map.get("colName");
        long oldAggregateVersion = SqlUtil.getOldAggregateVersion(event);
        long newAggregateVersion = SqlUtil.getNewAggregateVersion(event);

        try (PreparedStatement statement = conn.prepareStatement(updateRole)) {
            statement.setString(1, (String)map.get("operator"));
            statement.setString(2, (String)map.get("colValue"));
            statement.setString(3, (String)event.get(Constants.USER));
            statement.setObject(4, OffsetDateTime.parse((String)event.get(CloudEventV1.TIME)));
            statement.setLong(5, newAggregateVersion);
            statement.setObject(6, UUID.fromString(hostId));
            statement.setString(7, roleId);
            statement.setObject(8, UUID.fromString(endpointId));
            statement.setString(9, colName);
            statement.setLong(10, oldAggregateVersion);
            int count = statement.executeUpdate();
            if (count == 0) {
                if (queryRoleRowFilterExists(conn, hostId, roleId, endpointId, colName)) {
                    throw new ConcurrencyException(String.format("Optimistic concurrency conflict during updateRoleRowFilter for hostId %s roleId %s endpointId %s colName %s. Expected aggregateVersion %d but found a different version %d.", hostId, roleId, endpointId, colName, oldAggregateVersion, newAggregateVersion));
                } else {
                    throw new SQLException(String.format("No record found during updateRoleRowFilter for hostId %s roleId %s endpointId %s colName %s.", hostId, roleId, endpointId, colName));
                }
            }
        } catch (SQLException e) {
            logger.error("SQLException during updateRoleRowFilter for hostId {} roleId {} endpointId {} colName {} (old: {}) -> (new: {}): {}", hostId, roleId, endpointId, colName, oldAggregateVersion, newAggregateVersion, e.getMessage(), e);
            throw e;
        } catch (Exception e) {
            logger.error("Exception during updateRoleRowFilter for hostId {} roleId {} endpointId {} colName {} (old: {}) -> (new: {}): {}", hostId, roleId, endpointId, colName, oldAggregateVersion, newAggregateVersion, e.getMessage(), e);
            throw e;
        }
    }

    @Override
    public Result<String> queryRoleColFilter(int offset, int limit, String filtersJson, String globalFilter, String sortingJson, String hostId) {
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

        List<Map<String, Object>> filters = parseJsonList(filtersJson);
        List<Map<String, Object>> sorting = parseJsonList(sortingJson);

        String s =
                """
                SELECT COUNT(*) OVER () AS total,
                r.host_id, r.role_id, av.api_version_id, av.api_id, av.api_version,
                ae.endpoint_id, ae.endpoint, rcf.columns, rcf.aggregate_version,
                rcf.update_user, rcf.update_ts
                FROM role_col_filter_t rcf
                JOIN role_t r ON r.role_id = rcf.role_id
                JOIN api_endpoint_t ae ON rcf.host_id = ae.host_id AND rcf.endpoint_id = ae.endpoint_id
                JOIN api_version_t av ON ae.host_id = av.host_id AND ae.api_version_id = av.api_version_id
                AND r.host_id = ?
                """;

        List<Object> parameters = new ArrayList<>();
        parameters.add(UUID.fromString(hostId));

        String[] searchColumns = {"r.role_id", "ae.endpoint", "rcf.columns"};
        String sqlBuilder = s + dynamicFilter(Arrays.asList("r.host_id", "av.api_version_id", "ae.endpoint_id"), filters, columnMap, parameters) +
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
    public void createRoleColFilter(Connection conn, Map<String, Object> event) throws SQLException, Exception {
        final String insertRole =
                """
                    INSERT INTO role_col_filter_t (
                        host_id,
                        role_id,
                        endpoint_id,
                        columns,
                        update_user,
                        update_ts,
                        aggregate_version
                    ) VALUES (?, ?, ?, ?, ?,  ?, ?)
                """;

        Map<String, Object> map = (Map<String, Object>)event.get(PortalConstants.DATA);
        String hostId = (String)map.get("hostId");
        String roleId = (String)map.get("roleId");
        String endpointId = (String)map.get("endpointId");

        long newAggregateVersion = SqlUtil.getNewAggregateVersion(event);
        try (PreparedStatement statement = conn.prepareStatement(insertRole)) {
            statement.setObject(1, UUID.fromString(hostId));
            statement.setString(2, roleId);
            statement.setObject(3, UUID.fromString(endpointId));
            statement.setString(4, (String)map.get("columns"));
            statement.setString(5, (String)event.get(Constants.USER));
            statement.setObject(6, OffsetDateTime.parse((String)event.get(CloudEventV1.TIME)));
            statement.setLong(7, newAggregateVersion);

            int count = statement.executeUpdate();
            if (count == 0) {
                throw new SQLException(String.format("Failed during createRoleColFilter for hostId %s roleId %s endpointId %s with aggregate version %d. It might already exist.", hostId, roleId, endpointId, newAggregateVersion));
            }
        } catch (SQLException e) {
            logger.error("SQLException during createRoleColFilter for hostId {} roleId {} endpointId {} aggregateVersion {}: {}", hostId, roleId, endpointId, newAggregateVersion, e.getMessage(), e);
            throw e;
        } catch (Exception e) {
            logger.error("Exception during createRoleColFilter for hostId {} roleId {} endpointId {} aggregateVersion {}: {}", hostId, roleId, endpointId, newAggregateVersion, e.getMessage(), e);
            throw e;
        }
    }

    private boolean queryRoleColFilterExists(Connection conn, String hostId, String roleId, String endpointId) throws SQLException {
        final String sql =
                """
                SELECT COUNT(*) FROM role_col_filter_t WHERE host_id = ? AND role_id = ? AND endpoint_id = ?
                """;
        try (PreparedStatement pst = conn.prepareStatement(sql)) {
            pst.setObject(1, UUID.fromString(hostId));
            pst.setString(2, roleId);
            pst.setObject(3, UUID.fromString(endpointId));
            try (ResultSet rs = pst.executeQuery()) {
                return rs.next() && rs.getInt(1) > 0;
            }
        }
    }

    @Override
    public void deleteRoleColFilter(Connection conn, Map<String, Object> event) throws SQLException, Exception {
        final String deleteRole =
            """
                DELETE FROM role_col_filter_t
                WHERE host_id = ?
                AND role_id = ?
                AND endpoint_id = ?
                AND aggregate_version = ?
            """;

        Map<String, Object> map = (Map<String, Object>)event.get(PortalConstants.DATA);
        String hostId = (String)map.get("hostId");
        String roleId = (String)map.get("roleId");
        String endpointId = (String)map.get("endpointId");
        long oldAggregateVersion = SqlUtil.getOldAggregateVersion(event);

        try (PreparedStatement statement = conn.prepareStatement(deleteRole)) {
            statement.setObject(1, UUID.fromString(hostId));
            statement.setString(2, roleId);
            statement.setObject(3, UUID.fromString(endpointId));
            statement.setLong(4, oldAggregateVersion);
            int count = statement.executeUpdate();
            if (count == 0) {
                if (queryRoleColFilterExists(conn, hostId, roleId, endpointId)) {
                    throw new ConcurrencyException(String.format("Optimistic concurrency conflict during deleteRoleColFilter for hostId %s roleId %s endpointId %s aggregateVersion %d but found a different version or already updated.", hostId, roleId, endpointId, oldAggregateVersion));
                } else {
                    throw new SQLException(String.format("No record found during deleteRoleColFilter for hostId %s roleId %s endpointId %s. It might have been already deleted.", hostId, roleId, endpointId));
                }
            }
        } catch (SQLException e) {
            logger.error("SQLException during deleteRoleColFilter for hostId {} roleId {} endpointId {} aggregateVersion {}: {}", hostId, roleId, endpointId, oldAggregateVersion, e.getMessage(), e);
            throw e;
        } catch (Exception e) {
            logger.error("Exception during deleteRoleColFilter for hostId {} roleId {} endpointId {} aggregateVersion {}: {}", hostId, roleId, endpointId, oldAggregateVersion, e.getMessage(), e);
            throw e;
        }
    }

    @Override
    public void updateRoleColFilter(Connection conn, Map<String, Object> event) throws SQLException, Exception {
        final String updateRole =
            """
                UPDATE role_col_filter_t
                SET
                    columns = ?,
                    update_user = ?,
                    update_ts = ?
                    aggregate_version = ?
                WHERE
                    host_id = ?
                    AND role_id = ?
                    AND endpoint_id = ?
                    AND aggregate_version = ?
            """;

        Map<String, Object> map = (Map<String, Object>)event.get(PortalConstants.DATA);
        String hostId = (String)map.get("hostId");
        String roleId = (String)map.get("roleId");
        String endpointId = (String)map.get("endpointId");
        long oldAggregateVersion = SqlUtil.getOldAggregateVersion(event);
        long newAggregateVersion = SqlUtil.getNewAggregateVersion(event);

        try (PreparedStatement statement = conn.prepareStatement(updateRole)) {
            statement.setString(1, (String)map.get("columns"));
            statement.setString(2, (String)event.get(Constants.USER));
            statement.setObject(3, OffsetDateTime.parse((String)event.get(CloudEventV1.TIME)));
            statement.setLong(4, newAggregateVersion);
            statement.setObject(5, UUID.fromString(hostId));
            statement.setString(6, roleId);
            statement.setObject(7, UUID.fromString(endpointId));
            statement.setLong(8, oldAggregateVersion);

            int count = statement.executeUpdate();
            if (count == 0) {
                if (queryRoleColFilterExists(conn, hostId, roleId, endpointId)) {
                    throw new ConcurrencyException(String.format("Optimistic concurrency conflict during updateRoleColFilter for hostId %s roleId %s endpointId %s. Expected version %d but found a different version %d.", hostId, roleId, endpointId, oldAggregateVersion, newAggregateVersion));
                } else {
                    throw new SQLException(String.format("No record found during updateRoleColFilter for hostId %s roleId %s endpointId %s.", hostId, roleId, endpointId));
                }
            }
        } catch (SQLException e) {
            logger.error("SQLException during updateRoleColFilter for hostId {} roleId {} endpointId {} (old: {}) -> (new: {}): {}", hostId, roleId, endpointId, oldAggregateVersion, newAggregateVersion, e.getMessage(), e);
            throw e;
        } catch (Exception e) {
            logger.error("Exception during updateRoleColFilter for hostId {} roleId {} endpointId {} (old: {}) -> (new: {}): {}", hostId, roleId, endpointId, oldAggregateVersion, newAggregateVersion, e.getMessage(), e);
            throw e;
        }
    }

    @Override
    public void createGroup(Connection conn, Map<String, Object> event) throws SQLException, Exception {
        final String insertGroup =
            """
            INSERT INTO group_t (host_id, group_id, group_desc, update_user, update_ts, aggregate_version)
            VALUES (?, ?, ?, ?, ?,  ?)
            """;

        Map<String, Object> map = (Map<String, Object>)event.get(PortalConstants.DATA);
        String hostId = (String)map.get("hostId");
        String groupId = (String)map.get("groupId");
        long newAggregateVersion = SqlUtil.getNewAggregateVersion(event);

        try (PreparedStatement statement = conn.prepareStatement(insertGroup)) {
            statement.setObject(1, UUID.fromString(hostId));
            statement.setString(2, groupId);
            String groupDesc = (String)map.get("groupDesc");
            if (groupDesc != null && !groupDesc.isEmpty())
                statement.setString(3, groupDesc);
            else
                statement.setNull(3, NULL);

            statement.setString(4, (String)event.get(Constants.USER));
            statement.setObject(5, OffsetDateTime.parse((String)event.get(CloudEventV1.TIME)));
            statement.setLong(6, newAggregateVersion);

            int count = statement.executeUpdate();
            if (count == 0) {
                throw new SQLException(String.format("Failed during createGroup for hostId %s groupId %s with aggregate version %d. It might already exist.", hostId, groupId, newAggregateVersion));
            }
        } catch (SQLException e) {
            logger.error("SQLException during createGroup for hostId {} groupId {} aggregateVersion {}: {}", hostId, groupId, newAggregateVersion, e.getMessage(), e);
            throw e;
        } catch (Exception e) {
            logger.error("Exception during createGroup for hostId {} groupId {} aggregateVersion {}: {}", hostId, groupId, newAggregateVersion, e.getMessage(), e);
            throw e;
        }
    }

    private boolean queryGroupExists(Connection conn, String hostId, String groupId) throws SQLException {
        final String sql = "SELECT COUNT(*) FROM group_t WHERE host_id = ? AND group_id = ?";
        try (PreparedStatement pst = conn.prepareStatement(sql)) {
            pst.setObject(1, UUID.fromString(hostId));
            pst.setString(2, groupId);
            try (ResultSet rs = pst.executeQuery()) {
                return rs.next() && rs.getInt(1) > 0;
            }
        }
    }

    @Override
    public void updateGroup(Connection conn, Map<String, Object> event) throws SQLException, Exception {
        final String updateGroup =
            """
            UPDATE group_t SET group_desc = ?, update_user = ?, update_ts = ?, aggregate_version = ?
            WHERE host_id = ? AND group_id = ? AND aggregate_version = ?
            """;

        Map<String, Object> map = (Map<String, Object>)event.get(PortalConstants.DATA);
        String hostId = (String)map.get("hostId");
        String groupId = (String)map.get("groupId");
        long oldAggregateVersion = SqlUtil.getOldAggregateVersion(event);
        long newAggregateVersion = SqlUtil.getNewAggregateVersion(event);

        try (PreparedStatement statement = conn.prepareStatement(updateGroup)) {
            String groupDesc = (String)map.get("groupDesc");
            if(groupDesc != null && !groupDesc.isEmpty()) {
                statement.setString(1, groupDesc);
            } else {
                statement.setNull(1, NULL);
            }
            statement.setString(2, (String)event.get(Constants.USER));
            statement.setObject(3, OffsetDateTime.parse((String)event.get(CloudEventV1.TIME)));
            statement.setLong(4, newAggregateVersion);
            statement.setObject(5, UUID.fromString((String)event.get(Constants.HOST)));
            statement.setString(6, groupId);
            statement.setLong(7, oldAggregateVersion);

            int count = statement.executeUpdate();
            if (count == 0) {
                if (queryGroupExists(conn, hostId, groupId)) {
                    throw new ConcurrencyException(String.format("Optimistic concurrency conflict during updateGroup for hostId %s groupId %s. Expected version %d but found a different version %d.", hostId, groupId, oldAggregateVersion, newAggregateVersion));
                } else {
                    throw new SQLException(String.format("No record found during updateGroup for hostId %s groupId %s.", hostId, groupId));
                }
            }
        } catch (SQLException e) {
            logger.error("SQLException during updateGroup for hostId {} groupId {} (old: {}) -> (new: {}): {}", hostId, groupId, oldAggregateVersion, newAggregateVersion, e.getMessage(), e);
            throw e;
        } catch (Exception e) {
            logger.error("Exception during updateGroup for hostId {} groupId {} (old: {}) -> (new: {}): {}", hostId, groupId, oldAggregateVersion, newAggregateVersion, e.getMessage(), e);
            throw e;
        }
    }

    @Override
    public void deleteGroup(Connection conn, Map<String, Object> event) throws SQLException, Exception {
        final String deleteGroup = "DELETE from group_t WHERE host_id = ? AND group_id = ? AND aggregate_version = ?";
        Map<String, Object> map = (Map<String, Object>)event.get(PortalConstants.DATA);
        String hostId = (String)map.get("hostId");
        String groupId = (String)map.get("groupId");
        long oldAggregateVersion = SqlUtil.getOldAggregateVersion(event);

        try (PreparedStatement statement = conn.prepareStatement(deleteGroup)) {
            statement.setObject(1, UUID.fromString(hostId));
            statement.setString(2, groupId);
            statement.setLong(3, oldAggregateVersion);
            int count = statement.executeUpdate();
            if (count == 0) {
                if (queryGroupExists(conn, hostId, groupId)) {
                    throw new ConcurrencyException(String.format("Optimistic concurrency conflict during deleteGroup for hostId %s groupId %s aggregateVersion %d but found a different version or already updated. ", hostId, groupId, oldAggregateVersion));
                } else {
                    throw new SQLException(String.format("No record found during deleteGroup for hostId %s groupId %s. It might have been already deleted.", hostId, groupId)) ;
                }
            }
        } catch (SQLException e) {
            logger.error("SQLException during deleteGroup for hostId {} groupId {} aggregateVersion {}: {}", hostId, groupId, oldAggregateVersion, e.getMessage(), e);
            throw e;
        } catch (Exception e) {
            logger.error("Exception during deleteGroup for hostId {} groupId {} aggregateVersion {}: {}", hostId, groupId, oldAggregateVersion, e.getMessage(), e);
            throw e;
        }
    }

    @Override
    public Result<String> queryGroup(int offset, int limit, String filtersJson, String globalFilter, String sortingJson, String hostId) {
        Result<String> result;
        List<Map<String, Object>> filters = parseJsonList(filtersJson);
        List<Map<String, Object>> sorting = parseJsonList(sortingJson);
        String s =
            """
            SELECT COUNT(*) OVER () AS total,
            host_id, group_id, group_desc, update_user,
            update_ts, aggregate_version
            FROM group_t
            WHERE host_id = ?
            """;

        List<Object> parameters = new ArrayList<>();
        parameters.add(UUID.fromString(hostId));

        String[] searchColumns = {"group_id", "group_desc"};
        String sqlBuilder = s + dynamicFilter(Arrays.asList("host_id"), filters, null, parameters) +
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
    public Result<String> queryGroupPermission(int offset, int limit, String filtersJson, String globalFilter, String sortingJson, String hostId) {
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

        List<Map<String, Object>> filters = parseJsonList(filtersJson);
        List<Map<String, Object>> sorting = parseJsonList(sortingJson);

        String s =
            """
                SELECT COUNT(*) OVER () AS total,
                gp.host_id, gp.group_id, av.api_id, av.api_version, av.api_version_id, ae.endpoint_id,
                ae.endpoint, gp.aggregate_version, gp.update_user, gp.update_ts
                FROM group_permission_t gp
                JOIN group_t g ON gp.group_id = g.group_id
                JOIN api_endpoint_t ae ON gp.host_id = ae.host_id AND gp.endpoint_id = ae.endpoint_id
                JOIN api_version_t av ON ae.host_id = av.host_id AND ae.api_version_id = av.api_version_id
                WHERE gp.host_id = ?
            """;

        List<Object> parameters = new ArrayList<>();
        parameters.add(UUID.fromString(hostId));

        String[] searchColumns = {"gp.group_id", "ae.endpoint"};
        String sqlBuilder = s + dynamicFilter(Arrays.asList("gp.host_id", "av.api_version_id", "ae.endpoint_id"), filters, columnMap, parameters) +
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
    public Result<String> queryGroupUser(int offset, int limit, String filtersJson, String globalFilter, String sortingJson, String hostId) {
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

        List<Map<String, Object>> filters = parseJsonList(filtersJson);
        List<Map<String, Object>> sorting = parseJsonList(sortingJson);

        String s =
            """
                SELECT COUNT(*) OVER () AS total,
                g.host_id, g.group_id, g.start_ts, g.end_ts,
                g.aggregate_version, g.update_user, g.update_ts,
                u.user_id, u.email, u.user_type,
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


        String[] searchColumns = {"g.group_id", "u.email", "u.first_name", "u.last_name"};
        String sqlBuilder = s + dynamicFilter(Arrays.asList("g.host_id", "u.user_id"), filters, columnMap, parameters) +
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
    public void createGroupPermission(Connection conn, Map<String, Object> event) throws SQLException, Exception {
        final String insertGroup =
                """
                    INSERT INTO group_permission_t (host_id, group_id, endpoint_id, update_user, update_ts, aggregate_version)
                    VALUES (?, ?, ?, ?, ?,  ?)
                """;

        Map<String, Object> map = (Map<String, Object>)event.get(PortalConstants.DATA);
        String hostId = (String)map.get("hostId");
        String groupId = (String)map.get("groupId");
        String endpointId = (String)map.get("endpointId");
        long newAggregateVersion = SqlUtil.getNewAggregateVersion(event);

        try (PreparedStatement statement = conn.prepareStatement(insertGroup)) {
            statement.setObject(1, UUID.fromString(hostId));
            statement.setString(2, groupId);
            statement.setObject(3, UUID.fromString(endpointId));
            statement.setString(4, (String)event.get(Constants.USER));
            statement.setObject(5, OffsetDateTime.parse((String)event.get(CloudEventV1.TIME)));
            statement.setLong(6, newAggregateVersion);

            int count = statement.executeUpdate();
            if (count == 0) {
                throw new SQLException(String.format("Failed during createGroupPermission for hostId %s groupId %s endpointId %s with aggregateVersion %d. It might already exist.", hostId, groupId, endpointId, newAggregateVersion));
            }
        } catch (SQLException e) {
            logger.error("SQLException during createGroupPermission for hostId {} groupId {} endpointId {} aggregateVersion {}: {}", hostId, groupId, endpointId, newAggregateVersion, e.getMessage(), e);
            throw e;
        } catch (Exception e) {
            logger.error("Exception during createGroupPermission for hostId {} groupId {} endpointId {} aggregateVersion {}: {}", hostId, groupId, endpointId, newAggregateVersion, e.getMessage(), e);
            throw e;
        }
    }

    private boolean queryGroupPermissionExists(Connection conn, String hostId, String groupId, String endpointId) throws SQLException {
        final String sql =
                """
                    SELECT COUNT(*) FROM group_permission_t
                    WHERE host_id = ?
                    AND group_id = ?
                    AND endpoint_id = ?
                """;
        try (PreparedStatement statement = conn.prepareStatement(sql)) {
            statement.setObject(1, UUID.fromString(hostId));
            statement.setString(2, groupId);
            statement.setObject(3, UUID.fromString(endpointId));

            try (ResultSet rs = statement.executeQuery()) {
                return rs.next() && rs.getInt(1) > 0;
            }
        }
    }

    @Override
    public void deleteGroupPermission(Connection conn, Map<String, Object> event) throws SQLException, Exception {
        final String deleteGroup =
            """
                DELETE FROM group_permission_t
                WHERE host_id = ?
                AND group_id = ?
                AND endpoint_id = ?
                AND aggregate_version = ?
            """;

        Map<String, Object> map = (Map<String, Object>)event.get(PortalConstants.DATA);
        String hostId = (String)map.get("hostId");
        String groupId = (String)map.get("groupId");
        String endpointId = (String)map.get("endpointId");
        long oldAggregateVersion = SqlUtil.getOldAggregateVersion(event);

        try (PreparedStatement statement = conn.prepareStatement(deleteGroup)) {
            statement.setObject(1, UUID.fromString(hostId));
            statement.setString(2, groupId);
            statement.setObject(3, UUID.fromString(endpointId));
            statement.setLong(4, oldAggregateVersion);

            int count = statement.executeUpdate();
            if (count == 0) {
                if (queryGroupPermissionExists(conn, hostId, groupId, endpointId)) {
                    throw new ConcurrencyException(String.format("Optimistic concurrency conflict during deleteGroupPermission for hostId %s groupId %s endpointId %s with aggregateVersion %d but found a different version or already updated.", hostId, groupId, endpointId, oldAggregateVersion));
                } else {
                    throw new SQLException(String.format("No record found during deleteGroupPermission for hostId %s groupId %s endpointId %s. It might have been already deleted.", hostId, groupId, endpointId));
                }
            }
        } catch (SQLException e) {
            logger.error("SQLException during deleteGroupPermission for hostId {} groupId {} endpointId {} aggregateVersion {}: {}", hostId, groupId, endpointId, oldAggregateVersion, e.getMessage(), e);
            throw e;
        } catch (Exception e) {
            logger.error("Exception during deleteGroupPermission for hostId {} groupId {} endpointId {} aggregateVersion {}: {}", hostId, groupId, endpointId, oldAggregateVersion, e.getMessage(), e);
            throw e;
        }
    }
    @Override
    public void createGroupUser(Connection conn, Map<String, Object> event) throws SQLException, Exception {
        final String insertGroup =
            """
            INSERT INTO group_user_t
            (host_id, group_id, user_id, start_ts, end_ts, update_user, update_ts, aggregate_version)
            VALUES (?, ?, ?, ?, ?,  ?, ?, ?)
            """;

        Map<String, Object> map = (Map<String, Object>)event.get(PortalConstants.DATA);
        String hostId = (String)map.get("hostId");
        String groupId = (String)map.get("groupId");
        String userId = (String)map.get("userId");
        long newAggregateVersion = SqlUtil.getNewAggregateVersion(event);

        try (PreparedStatement statement = conn.prepareStatement(insertGroup)) {
            statement.setObject(1, UUID.fromString(hostId));
            statement.setString(2, groupId);
            statement.setObject(3, UUID.fromString(userId));
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
            statement.setLong(8, newAggregateVersion);

            int count = statement.executeUpdate();
            if (count == 0) {
                throw new SQLException(String.format("Failed during createGroupUser for hostId %s groupId %s userId %s with aggregateVersion %d. It might already exist.", hostId, groupId, userId, newAggregateVersion));
            }
        } catch (SQLException e) {
            logger.error("SQLException during createGroupUser for hostId {} groupId {} userId {} and aggregateVersion {}: {}", hostId, groupId, userId, newAggregateVersion, e.getMessage(), e);
            throw e;
        } catch (Exception e) {
            logger.error("Exception during createGroupUser for hostId {} groupId {} userId {} and aggregateVersion {}: {}", hostId, groupId, userId, newAggregateVersion, e.getMessage(), e);
            throw e;
        }
    }

    private boolean queryGroupUserExists(Connection conn, String hostId, String groupId, String userId) throws SQLException {
        final String sql =
                """
                SELECT COUNT(*) FROM group_user_t WHERE host_id = ? AND group_id = ? AND user_id = ?
                """;
        try (PreparedStatement pst = conn.prepareStatement(sql)) {
            pst.setObject(1, UUID.fromString(hostId));
            pst.setString(2, groupId);
            pst.setObject(3, UUID.fromString(userId));
            try (ResultSet rs = pst.executeQuery()) {
                return rs.next() && rs.getInt(1) > 0;
            }
        }
    }

    @Override
    public void updateGroupUser(Connection conn, Map<String, Object> event) throws SQLException, Exception {
        final String updateGroup =
            """
            UPDATE group_user_t SET start_ts = ?, end_ts = ?, update_user = ?, update_ts = ?, aggregate_version = ?
            WHERE host_id = ? AND group_id = ? AND user_id = ? AND aggregate_version = ?
            """;

        Map<String, Object> map = (Map<String, Object>)event.get(PortalConstants.DATA);
        String hostId = (String)map.get("hostId");
        String groupId = (String)map.get("groupId");
        String userId = (String)map.get("userId");
        long oldAggregateVersion = SqlUtil.getOldAggregateVersion(event);
        long newAggregateVersion = SqlUtil.getNewAggregateVersion(event);

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
            statement.setLong(5, newAggregateVersion);
            statement.setObject(6, UUID.fromString((String)event.get(Constants.HOST)));
            statement.setString(7, groupId);
            statement.setObject(8, UUID.fromString(userId));
            statement.setLong(9, oldAggregateVersion);

            int count = statement.executeUpdate();
            if (count == 0) {
                if (queryGroupUserExists(conn, hostId, groupId, userId)) {
                    throw new ConcurrencyException(String.format("Optimistic concurrency conflict during updateGroupUser for hostId %s groupId %s userId %s. Expected version %d but found a different version %d.", hostId, groupId, userId, oldAggregateVersion, newAggregateVersion));
                } else {
                    throw new SQLException(String.format("No record found during updateGroupUser for hostId %s groupId %s userId %s.", hostId, groupId, userId));
                }
            }
        } catch (SQLException e) {
            logger.error("SQLException during updateGroupUser for hostId {} groupId {} userId {} (old: {}) -> (new: {}): {}", hostId, groupId, userId, oldAggregateVersion, newAggregateVersion, e.getMessage(), e);
            throw e;
        } catch (Exception e) {
            logger.error("Exception during updateGroupUser for hostId {} groupId {} userId {} (old: {}) -> (new: {}): {}", hostId, groupId, userId, oldAggregateVersion, newAggregateVersion, e.getMessage(), e);
            throw e;
        }
    }
    @Override
    public void deleteGroupUser(Connection conn, Map<String, Object> event) throws SQLException, Exception {
        final String deleteGroup =
            """
            DELETE from group_user_t
            WHERE host_id = ? AND group_id = ? AND user_id = ? AND aggregate_version = ?
            """;

        Map<String, Object> map = (Map<String, Object>)event.get(PortalConstants.DATA);
        String hostId = (String)map.get("hostId");
        String groupId = (String)map.get("groupId");
        String userId = (String)map.get("userId");
        long oldAggregateVersion = SqlUtil.getOldAggregateVersion(event);

        try (PreparedStatement statement = conn.prepareStatement(deleteGroup)) {
            statement.setObject(1, UUID.fromString(hostId));
            statement.setString(2, groupId);
            statement.setObject(3, UUID.fromString(userId));
            statement.setLong(4, oldAggregateVersion);

            int count = statement.executeUpdate();
            if (count == 0) {
                if (queryGroupUserExists(conn, hostId, groupId, userId)) {
                    throw new ConcurrencyException(String.format("Optimistic concurrency conflict during deleteGroupUser for hostId %s groupId %s userId %s aggregateVersion %d but found a different version or already updated. ", hostId, groupId, userId, oldAggregateVersion));
                } else {
                    throw new SQLException(String.format("No record found during deleteGroupUser for hostId %s groupId %s userId %s. It might have been already deleted.", hostId, groupId, userId));
                }
            }
        } catch (SQLException e) {
            logger.error("SQLException during deleteGroupUser for hostId {} groupId {} userId {} aggregateVersion {}: {}", hostId, groupId, userId, oldAggregateVersion,  e.getMessage(), e);
            throw e;
        } catch (Exception e) {
            logger.error("Exception during deleteGroupUser for hostId {} groupId {} userId {} aggregateVersion {}: {}", hostId, groupId, userId, oldAggregateVersion, e.getMessage(), e);
            throw e;
        }
    }

    @Override
    public Result<String> queryGroupRowFilter(int offset, int limit, String filtersJson, String globalFilter, String sortingJson, String hostId) {
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

        List<Map<String, Object>> filters = parseJsonList(filtersJson);
        List<Map<String, Object>> sorting = parseJsonList(sortingJson);

        String s =
            """
            SELECT COUNT(*) OVER () AS total, g.host_id, g.group_id, p.endpoint_id, ae.endpoint,
            av.api_version_id, av.api_id, av.api_version, p.col_name, p.operator, p.col_value,
            p.update_user, p.update_ts, p.aggregate_version
            FROM group_row_filter_t p
            JOIN group_t g ON g.host_id = p.host_id AND g.group_id = p.group_id
            JOIN api_endpoint_t ae ON ae.host_id = p.host_id AND ae.endpoint_id = p.endpoint_id
            JOIN api_version_t av ON av.host_id = ae.host_id AND av.api_version_id = ae.api_version_id
            WHERE p.host_id = ?
            """;

        List<Object> parameters = new ArrayList<>();
        parameters.add(UUID.fromString(hostId));

        String[] searchColumns = {"g.group_id", "ae.endpoint", "p.col_name", "p.col_value"};
        String sqlBuilder = s + dynamicFilter(Arrays.asList("g.host_id", "p.endpoint_id", "av.api_version_id"), filters, columnMap, parameters) +
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
    public void createGroupRowFilter(Connection conn, Map<String, Object> event) throws SQLException, Exception {
        final String insertGroup =
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
                    aggregate_version
                ) VALUES (?, ?, ?, ?, ?,  ?, ?, ?, ?)
            """;

        Map<String, Object> map = (Map<String, Object>)event.get(PortalConstants.DATA);
        String hostId = (String)map.get("hostId");
        String groupId = (String)map.get("groupId");
        String endpointId = (String)map.get("endpointId");
        String colName = (String)map.get("colName");
        long newAggregateVersion = SqlUtil.getNewAggregateVersion(event);

        try (PreparedStatement statement = conn.prepareStatement(insertGroup)) {
            statement.setObject(1, UUID.fromString(hostId));
            statement.setString(2, groupId);
            statement.setObject(3, UUID.fromString(endpointId));
            statement.setString(4, colName);
            statement.setString(5, (String)map.get("operator"));
            statement.setString(6, (String)map.get("colValue"));
            statement.setString(7, (String)event.get(Constants.USER));
            statement.setObject(8, OffsetDateTime.parse((String)event.get(CloudEventV1.TIME)));
            statement.setLong(9, newAggregateVersion);

            int count = statement.executeUpdate();
            if (count == 0) {
                throw new SQLException(String.format("Failed during createGroupRowFilter for hostId %s groupId %s endpointId %s colName %s with aggregate version %d. It might already exist.", hostId, groupId, endpointId, colName, newAggregateVersion));
            }
        } catch (SQLException e) {
            logger.error("SQLException during createGroupRowFilter for hostId {} groupId {} endpointId {} colName {} aggregateVersion {}: {}", hostId, groupId, endpointId, colName, newAggregateVersion, e.getMessage(), e);
            throw e;
        } catch (Exception e) {
            logger.error("Exception during createGroupRowFilter for hostId {} groupId {} endpointId {} colName {} aggregateVersion {}: {}", hostId, groupId, endpointId, colName, newAggregateVersion, e.getMessage(), e);
            throw e;
        }
    }

    private boolean queryGroupRowFilterExists(Connection conn, String hostId, String groupId, String endpointId, String colName) throws SQLException {
        final String sql =
                """
                SELECT COUNT(*) FROM group_row_filter_t WHERE host_id = ? AND group_id = ? AND endpoint_id = ? AND col_name = ?
                """;
        try (PreparedStatement pst = conn.prepareStatement(sql)) {
            pst.setObject(1, UUID.fromString(hostId));
            pst.setString(2, groupId);
            pst.setObject(3, UUID.fromString(endpointId));
            pst.setString(4, colName);
            try (ResultSet rs = pst.executeQuery()) {
                return rs.next() && rs.getInt(1) > 0;
            }
        }
    }

    @Override
    public void updateGroupRowFilter(Connection conn, Map<String, Object> event) throws SQLException, Exception {
        final String updateGroup =
                """
                    UPDATE group_row_filter_t
                    SET
                        operator = ?,
                        col_value = ?,
                        update_user = ?,
                        update_ts = ?,
                        aggregate_version = ?
                    WHERE
                        host_id = ?
                        AND group_id = ?
                        AND endpoint_id = ?
                        AND col_name = ?
                        AND aggregate_version = ?
                """;

        Map<String, Object> map = (Map<String, Object>)event.get(PortalConstants.DATA);
        String hostId = (String)map.get("hostId");
        String groupId = (String)map.get("groupId");
        String endpointId = (String)map.get("endpointId");
        String colName = (String)map.get("colName");
        long oldAggregateVersion = SqlUtil.getOldAggregateVersion(event);
        long newAggregateVersion = SqlUtil.getNewAggregateVersion(event);

        try (PreparedStatement statement = conn.prepareStatement(updateGroup)) {
            statement.setString(1, (String)map.get("operator"));
            statement.setString(2, (String)map.get("colValue"));
            statement.setString(3, (String)event.get(Constants.USER));
            statement.setObject(4, OffsetDateTime.parse((String)event.get(CloudEventV1.TIME)));
            statement.setLong(5, newAggregateVersion);
            statement.setObject(6, UUID.fromString(hostId));
            statement.setString(7, groupId);
            statement.setObject(8, UUID.fromString(endpointId));
            statement.setString(9, colName);
            statement.setLong(10, oldAggregateVersion);

            int count = statement.executeUpdate();
            if (count == 0) {
                if (queryGroupRowFilterExists(conn, hostId, groupId, endpointId, colName)) {
                    throw new ConcurrencyException(String.format("Optimistic concurrency conflict during updateGroupRowFilter for hostId %s groupId %s endpointId %s colName %s. Expected aggregateVersion %d but found a different version %d.", hostId, groupId, endpointId, colName, oldAggregateVersion, newAggregateVersion));
                } else {
                    throw new SQLException(String.format("No record found during updateGroupRowFilter for hostId %s groupId %s endpointId %s colName %s.", hostId, groupId, endpointId, colName));
                }
            }
        } catch (SQLException e) {
            logger.error("SQLException during updateGroupRowFilter for hostId {} groupId {} endpointId {} colName {} (old: {}) -> (new: {}): {}", hostId, groupId, endpointId, colName, oldAggregateVersion, newAggregateVersion, e.getMessage(), e);
            throw e;
        } catch (Exception e) {
            logger.error("Exception during updateGroupRowFilter for hostId {} groupId {} endpointId {} colName {} (old: {}) -> (new: {}): {}", hostId, groupId, endpointId, colName, oldAggregateVersion, newAggregateVersion, e.getMessage(), e);
            throw e;
        }
    }

    @Override
    public void deleteGroupRowFilter(Connection conn, Map<String, Object> event) throws SQLException, Exception {
        final String deleteGroup =
            """
                DELETE FROM group_row_filter_t
                WHERE host_id = ?
                AND group_id = ?
                AND endpoint_id = ?
                AND col_name = ?
                AND aggregate_version = ?
            """;
        Map<String, Object> map = (Map<String, Object>)event.get(PortalConstants.DATA);
        String hostId = (String)map.get("hostId");
        String groupId = (String)map.get("groupId");
        String endpointId = (String)map.get("endpointId");
        String colName = (String)map.get("colName");
        long oldAggregateVersion = SqlUtil.getOldAggregateVersion(event);
        try (PreparedStatement statement = conn.prepareStatement(deleteGroup)) {
            statement.setObject(1, UUID.fromString(hostId));
            statement.setString(2, groupId);
            statement.setObject(3, UUID.fromString(endpointId));
            statement.setString(4, colName);
            statement.setLong(5, oldAggregateVersion);

            int count = statement.executeUpdate();
            if (count == 0) {
                if (queryGroupRowFilterExists(conn, hostId, groupId, endpointId, colName)) {
                    throw new ConcurrencyException(String.format("Optimistic concurrency conflict during deleteGroupRowFilter for hostId %s groupId %s endpointId %s colName %s aggregateVersion %d but found a different version or already updated.", hostId, groupId, endpointId, colName, oldAggregateVersion));
                } else {
                    throw new SQLException(String.format("No record found during deleteGroupRowFilter for hostId %s groupId %s endpointId %s colName %s. It might have been already deleted.", hostId, groupId, endpointId, colName));
                }
            }
        } catch (SQLException e) {
            logger.error("SQLException during deleteGroupRowFilter for hostId {} groupId {} endpointId {} colName {} aggregateVersion {}: {}", hostId, groupId, endpointId, colName, oldAggregateVersion, e.getMessage(), e);
            throw e;
        } catch (Exception e) {
            logger.error("Exception during deleteGroupRowFilter for hostId {} groupId {} endpointId {} colName {} aggregateVersion {}: {}", hostId, groupId, endpointId, colName, oldAggregateVersion, e.getMessage(), e);
            throw e;
        }
    }

    @Override
    public Result<String> queryGroupColFilter(int offset, int limit, String filtersJson, String globalFilter, String sortingJson, String hostId) {
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

        List<Map<String, Object>> filters = parseJsonList(filtersJson);
        List<Map<String, Object>> sorting = parseJsonList(sortingJson);

        String s =
            """
            SELECT COUNT(*) OVER () AS total,
            g.host_id, g.group_id, av.api_version_id, av.api_id, av.api_version,
            ae.endpoint_id, ae.endpoint, cf.columns, cf.aggregate_version,
            cf.update_user, cf.update_ts
            FROM group_col_filter_t cf
            JOIN group_t g ON g.group_id = cf.group_id
            JOIN api_endpoint_t ae ON cf.host_id = ae.host_id AND cf.endpoint_id = ae.endpoint_id
            JOIN api_version_t av ON ae.host_id = av.host_id AND ae.api_version_id = av.api_version_id
            WHERE cf.host_id = ?
            """;

        List<Object> parameters = new ArrayList<>();
        parameters.add(UUID.fromString(hostId));

        String[] searchColumns = {"g.group_id", "ae.endpoint", "cf.columns"};
        String sqlBuilder = s + dynamicFilter(Arrays.asList("g.host_id", "av.api_version_id", "ae.endpoint_id"), filters, columnMap, parameters) +
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
    public void createGroupColFilter(Connection conn, Map<String, Object> event) throws SQLException, Exception {
        final String insertGroup =
            """
                INSERT INTO group_col_filter_t (
                    host_id,
                    group_id,
                    endpoint_id,
                    columns,
                    update_user,
                    update_ts,
                    aggregate_version
                ) VALUES (?, ?, ?, ?, ?,  ?, ?)
            """;

        Map<String, Object> map = (Map<String, Object>)event.get(PortalConstants.DATA);
        String hostId = (String)map.get("hostId");
        String groupId = (String)map.get("groupId");
        String endpointId = (String)map.get("endpointId");
        long newAggregateVersion = SqlUtil.getNewAggregateVersion(event);

        try (PreparedStatement statement = conn.prepareStatement(insertGroup)) {
            statement.setObject(1, UUID.fromString(hostId));
            statement.setString(2, groupId);
            statement.setObject(3, UUID.fromString(endpointId));
            statement.setString(4, (String)map.get("columns"));
            statement.setString(5, (String)event.get(Constants.USER));
            statement.setObject(6, OffsetDateTime.parse((String)event.get(CloudEventV1.TIME)));
            statement.setLong(7, newAggregateVersion);

            int count = statement.executeUpdate();
            if (count == 0) {
                throw new SQLException(String.format("Failed during createGroupColFilter for hostId %s groupId %s endpointId %s with aggregate version %d. It might already exist.", hostId, groupId, endpointId, newAggregateVersion));
            }
        } catch (SQLException e) {
            logger.error("SQLException during createGroupColFilter for hostId {} groupId {} endpointId {} aggregateVersion {}: {}", hostId, groupId, endpointId, newAggregateVersion, e.getMessage(), e);
            throw e;
        } catch (Exception e) {
            logger.error("Exception during createGroupColFilter for hostId {} groupId {} endpointId {} aggregateVersion {}: {}", hostId, groupId, endpointId, newAggregateVersion, e.getMessage(), e);
            throw e;
        }
    }

    private boolean queryGroupColFilterExists(Connection conn, String hostId, String groupId, String endpointId) throws SQLException {
        final String sql =
            """
            SELECT COUNT(*) FROM group_col_filter_t WHERE host_id = ? AND group_id = ? AND endpoint_id = ?
            """;
        try (PreparedStatement pst = conn.prepareStatement(sql)) {
            pst.setObject(1, UUID.fromString(hostId));
            pst.setString(2, groupId);
            pst.setObject(3, UUID.fromString(endpointId));
            try (ResultSet rs = pst.executeQuery()) {
                return rs.next() && rs.getInt(1) > 0;
            }
        }
    }

    @Override
    public void updateGroupColFilter(Connection conn, Map<String, Object> event) throws SQLException, Exception {
        final String updateGroup =
            """
                UPDATE group_col_filter_t
                SET
                    columns = ?,
                    update_user = ?,
                    update_ts = ?,
                    aggregate_version = ?
                WHERE
                    host_id = ?
                    AND group_id = ?
                    AND endpoint_id = ?
                    AND aggregate_version = ?
            """;

        Map<String, Object> map = (Map<String, Object>)event.get(PortalConstants.DATA);
        String hostId = (String)map.get("hostId");
        String groupId = (String)map.get("groupId");
        String endpointId = (String)map.get("endpointId");
        long oldAggregateVersion = SqlUtil.getOldAggregateVersion(event);
        long newAggregateVersion = SqlUtil.getNewAggregateVersion(event);

        try (PreparedStatement statement = conn.prepareStatement(updateGroup)) {
            statement.setString(1, (String)map.get("columns"));
            statement.setString(2, (String)event.get(Constants.USER));
            statement.setObject(3, OffsetDateTime.parse((String)event.get(CloudEventV1.TIME)));
            statement.setLong(4, newAggregateVersion);
            statement.setObject(5, UUID.fromString(hostId));
            statement.setString(6, groupId);
            statement.setObject(7, UUID.fromString(endpointId));
            statement.setLong(8, oldAggregateVersion);

            int count = statement.executeUpdate();
            if (count == 0) {
                if (queryGroupColFilterExists(conn, hostId, groupId, endpointId)) {
                    throw new ConcurrencyException(String.format("Optimistic concurrency conflict during updateGroupColFilter for hostId %s groupId %s endpointId %s. Expected version %d but found a different version %d.", hostId, groupId, endpointId, oldAggregateVersion, newAggregateVersion));
                } else {
                    throw new SQLException(String.format("No record found during updateGroupColFilter for hostId %s groupId %s endpointId %s.", hostId, groupId, endpointId));
                }
            }
        } catch (SQLException e) {
            logger.error("SQLException during updateGroupColFilter for hostId {} groupId {} endpointId {} (old: {}) -> (new: {}): {}", hostId, groupId, endpointId, oldAggregateVersion, newAggregateVersion, e.getMessage(), e);
            throw e;
        } catch (Exception e) {
            logger.error("Exception during updateGroupColFilter for hostId {} groupId {} endpointId {} (old: {}) -> (new: {}): {}", hostId, groupId, endpointId, oldAggregateVersion, newAggregateVersion, e.getMessage(), e);
            throw e;
        }
    }

    @Override
    public void deleteGroupColFilter(Connection conn, Map<String, Object> event) throws SQLException, Exception {
        final String deleteGroup =
                """
                    DELETE FROM group_col_filter_t
                    WHERE host_id = ?
                    AND group_id = ?
                    AND endpoint_id = ?
                    AND aggregate_version = ?
                """;
        Map<String, Object> map = (Map<String, Object>)event.get(PortalConstants.DATA);
        String hostId = (String)map.get("hostId");
        String groupId = (String)map.get("groupId");
        String endpointId = (String)map.get("endpointId");
        long oldAggregateVersion = SqlUtil.getOldAggregateVersion(event);

        try (PreparedStatement statement = conn.prepareStatement(deleteGroup)) {
            statement.setObject(1, UUID.fromString(hostId));
            statement.setString(2, groupId);
            statement.setObject(3, UUID.fromString(endpointId));
            statement.setLong(4, oldAggregateVersion);

            int count = statement.executeUpdate();
            if (count == 0) {
                if (queryGroupColFilterExists(conn, hostId, groupId, endpointId)) {
                    throw new ConcurrencyException(String.format("Optimistic concurrency conflict during deleteGroupColFilter for hostId %s groupId %s endpointId %s aggregateVersion %d but found a different version or already updated.", hostId, groupId, endpointId, oldAggregateVersion));
                } else {
                    throw new SQLException(String.format("No record found during deleteGroupColFilter for hostId %s groupId %s endpointId %s. It might have been already deleted.", hostId, groupId, endpointId));
                }
            }
        } catch (SQLException e) {
            logger.error("SQLException during deleteGroupColFilter for hostId {} groupId {} endpointId {} aggregateVersion {}: {}", hostId, groupId, endpointId, oldAggregateVersion, e.getMessage(), e);
            throw e;
        } catch (Exception e) {
            logger.error("Exception during deleteGroupColFilter for hostId {} groupId {} endpointId {} aggregateVersion {}: {}", hostId, groupId, endpointId, oldAggregateVersion, e.getMessage(), e);
            throw e;
        }
    }

    @Override
    public void createPosition(Connection conn, Map<String, Object> event) throws SQLException, Exception {
        final String insertPosition =
            """
            INSERT INTO position_t (host_id, position_id, position_desc,
            inherit_to_ancestor, inherit_to_sibling, update_user, update_ts, aggregate_version)
            VALUES (?, ?, ?, ?, ?,  ?, ?, ?)
            """;

        Map<String, Object> map = (Map<String, Object>)event.get(PortalConstants.DATA);
        String hostId = (String)map.get("hostId");
        String positionId = (String)map.get("positionId");
        long newAggregateVersion = SqlUtil.getNewAggregateVersion(event);

        try (PreparedStatement statement = conn.prepareStatement(insertPosition)) {
            statement.setObject(1, UUID.fromString(hostId));
            statement.setString(2, positionId);
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
            statement.setLong(8, newAggregateVersion);

            int count = statement.executeUpdate();
            if (count == 0) {
                throw new SQLException(String.format("Failed during createPosition for hostId %s positionId %s with aggregate version %d. It might already exist.", hostId, positionId, newAggregateVersion));
            }
        } catch (SQLException e) {
            logger.error("SQLException during createPosition for hostId {} positionId {} aggregateVersion {}: {}", hostId, positionId, newAggregateVersion, e.getMessage(), e);
            throw e;
        } catch (Exception e) {
            logger.error("Exception during createPosition for hostId {} positionId {} aggregateVersion {}: {}", hostId, positionId, newAggregateVersion, e.getMessage(), e);
            throw e;
        }
    }

    private boolean queryPositionExists(Connection conn, String hostId, String positionId) throws SQLException {
        final String sql = "SELECT COUNT(*) FROM position_t WHERE host_id = ? AND position_id = ?";
        try (PreparedStatement pst = conn.prepareStatement(sql)) {
            pst.setObject(1, UUID.fromString(hostId));
            pst.setString(2, positionId);
            try (ResultSet rs = pst.executeQuery()) {
                return rs.next() && rs.getInt(1) > 0;
            }
        }
    }

    @Override
    public void updatePosition(Connection conn, Map<String, Object> event) throws SQLException, Exception {
        final String updatePosition =
            """
            UPDATE position_t SET position_desc = ?, inherit_to_ancestor = ?, inherit_to_sibling = ?,
            update_user = ?, update_ts = ?, aggregate_version = ?
            WHERE host_id = ? AND position_id = ? AND aggregate_version = ?
            """;

        Map<String, Object> map = (Map<String, Object>)event.get(PortalConstants.DATA);
        String hostId = (String)map.get("hostId");
        String positionId = (String)map.get("positionId");
        long oldAggregateVersion = SqlUtil.getOldAggregateVersion(event);
        long newAggregateVersion = SqlUtil.getNewAggregateVersion(event);

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
            statement.setLong(6, newAggregateVersion);

            statement.setObject(7, UUID.fromString((String)event.get(Constants.HOST)));
            statement.setString(8, positionId);
            statement.setLong(9, oldAggregateVersion);

            int count = statement.executeUpdate();
            if (count == 0) {
                if (queryPositionExists(conn, hostId, positionId)) {
                    throw new ConcurrencyException(String.format("Optimistic concurrency conflict during updatePosition for hostId %s positionId %s. Expected version %d but found a different version %d.", hostId, positionId, oldAggregateVersion, newAggregateVersion));
                } else {
                    throw new SQLException(String.format("No record found during updatePosition for hostId %s positionId %s.", hostId, positionId));
                }
            }
        } catch (SQLException e) {
            logger.error("SQLException during updatePosition for hostId {} positionId {} (old: {}) -> (new: {}): {}", hostId, positionId, oldAggregateVersion, newAggregateVersion, e.getMessage(), e);
            throw e;
        } catch (Exception e) {
            logger.error("Exception during updatePosition for hostId {} positionId {} (old: {}) -> (new: {}): {}", hostId, positionId, oldAggregateVersion, newAggregateVersion, e.getMessage(), e);
            throw e;
        }
    }

    @Override
    public void deletePosition(Connection conn, Map<String, Object> event) throws SQLException, Exception {
        final String deleteGroup = "DELETE from position_t WHERE host_id = ? AND position_id = ? AND aggregate_version = ?";
        Map<String, Object> map = (Map<String, Object>)event.get(PortalConstants.DATA);
        String hostId = (String)map.get("hostId");
        String positionId = (String)map.get("positionId");
        long oldAggregateVersion = SqlUtil.getOldAggregateVersion(event);

        try (PreparedStatement statement = conn.prepareStatement(deleteGroup)) {
            statement.setObject(1, UUID.fromString(hostId));
            statement.setString(2, positionId);
            statement.setLong(3, oldAggregateVersion);

            int count = statement.executeUpdate();
            if (count == 0) {
                if (queryPositionExists(conn, hostId, positionId)) {
                    throw new ConcurrencyException(String.format("Optimistic concurrency conflict during deletePosition for hostId %s positionId %s aggregateVersion %d but found a different version or already updated. ", hostId, positionId, oldAggregateVersion));
                } else {
                    throw new SQLException(String.format("No record found during deletePosition for hostId %s positionId %s. It might have been already deleted.", hostId, positionId)) ;
                }
            }
        } catch (SQLException e) {
            logger.error("SQLException during deletePosition for hostId {} positionId {} aggregateVersion {}: {}", hostId, positionId, oldAggregateVersion, e.getMessage(), e);
            throw e;
        } catch (Exception e) {
            logger.error("Exception during deletePosition for hostId {} positionId {} aggregateVersion {}: {}", hostId, positionId, oldAggregateVersion, e.getMessage(), e);
            throw e;
        }
    }


    @Override
    public Result<String> queryPosition(int offset, int limit, String filtersJson, String globalFilter, String sortingJson, String hostId) {
        Result<String> result;
        List<Map<String, Object>> filters = parseJsonList(filtersJson);
        List<Map<String, Object>> sorting = parseJsonList(sortingJson);

        String s =
            """
            SELECT COUNT(*) OVER () AS total,
            host_id, position_id, position_desc, inherit_to_ancestor, inherit_to_sibling,
            update_user, update_ts, aggregate_version
            FROM position_t
            WHERE host_id = ?
            """;

        List<Object> parameters = new ArrayList<>();
        parameters.add(UUID.fromString(hostId));

        String[] searchColumns = {"position_id", "position_desc"};
        String sqlBuilder = s + dynamicFilter(Arrays.asList("host_id"), filters, null, parameters) +
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
    public Result<String> queryPositionPermission(int offset, int limit, String filtersJson, String globalFilter, String sortingJson, String hostId) {
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

        List<Map<String, Object>> filters = parseJsonList(filtersJson);
        List<Map<String, Object>> sorting = parseJsonList(sortingJson);

        String s =
            """
                SELECT COUNT(*) OVER () AS total,
                pp.host_id, pp.position_id, p.inherit_to_ancestor, p.inherit_to_sibling,
                av.api_version_id, av.api_id, av.api_version, ae.endpoint_id, ae.endpoint,
                pp.aggregate_version, pp.update_user, pp.update_ts
                FROM position_permission_t pp
                JOIN position_t p ON pp.position_id = p.position_id
                JOIN api_endpoint_t ae ON pp.host_id = ae.host_id AND pp.endpoint_id = ae.endpoint_id
                JOIN api_version_t av ON ae.host_id = av.host_id AND ae.api_version_id = av.api_version_id
                WHERE pp.host_id = ?
            """;

        List<Object> parameters = new ArrayList<>();
        parameters.add(UUID.fromString(hostId));

        String[] searchColumns = {"pp.position_id", "ae.endpoint"};
        String sqlBuilder = s + dynamicFilter(Arrays.asList("pp.host_id", "av.api_version_id", "ae.endpoint_id"), filters, columnMap, parameters) +
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
    public Result<String> queryPositionUser(int offset, int limit, String filtersJson, String globalFilter, String sortingJson, String hostId) {
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

        List<Map<String, Object>> filters = parseJsonList(filtersJson);
        List<Map<String, Object>> sorting = parseJsonList(sortingJson);

        String s =
           """
                SELECT COUNT(*) OVER () AS total,
                ep.host_id, ep.position_id, ep.position_type,
                ep.start_ts, ep.end_ts, u.user_id,
                ep.aggregate_version, ep.update_user, ep.update_ts,
                u.email, u.user_type, e.employee_id AS entity_id,
                e.manager_id, u.first_name, u.last_name
                FROM user_t u
                INNER JOIN
                    employee_t e ON u.user_id = e.user_id AND u.user_type = 'E'
                INNER JOIN
                    employee_position_t ep ON ep.employee_id = e.employee_id
                WHERE ep.host_id = ?
            """;

        List<Object> parameters = new ArrayList<>();
        parameters.add(UUID.fromString(hostId));

        String[] searchColumns = {"ep.position_id", "u.email", "u.first_name", "u.last_name"};
        String sqlBuilder = s + dynamicFilter(Arrays.asList("ep.host_id", "u.user_id"), filters, columnMap, parameters) +
                globalFilter(globalFilter, searchColumns, parameters) +
                dynamicSorting("ep.position_id, u.user_id", sorting, columnMap) +
                "\nLIMIT ? OFFSET ?";

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
                    map.put("startTs", resultSet.getDate("start_ts"));
                    map.put("endTs", resultSet.getString("end_ts"));
                    map.put("userId", resultSet.getObject("user_id", UUID.class));
                    map.put("entityId", resultSet.getString("entity_id"));
                    map.put("email", resultSet.getString("email"));
                    map.put("firstName", resultSet.getString("first_name"));
                    map.put("lastName", resultSet.getString("last_name"));
                    map.put("userType", resultSet.getString("user_type"));
                    map.put("updateUser", resultSet.getString("update_user"));
                    map.put("updateTs", resultSet.getObject("update_ts") != null ? resultSet.getObject("update_ts", OffsetDateTime.class) : null);
                    map.put("aggregateVersion", resultSet.getLong("aggregate_version"));
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
    public void createPositionPermission(Connection conn, Map<String, Object> event) throws SQLException, Exception {
        final String insertGroup =
            """
                INSERT INTO position_permission_t (host_id, position_id, endpoint_id, update_user, update_ts, aggregate_version)
                VALUES (?, ?, ?, ?, ?,  ?)
            """;

        Map<String, Object> map = (Map<String, Object>)event.get(PortalConstants.DATA);
        String hostId = (String)map.get("hostId");
        String positionId = (String)map.get("positionId");
        String endpointId = (String)map.get("endpointId");
        long newAggregateVersion = SqlUtil.getNewAggregateVersion(event);
        try (PreparedStatement statement = conn.prepareStatement(insertGroup)) {
            statement.setObject(1, UUID.fromString(hostId));
            statement.setString(2, positionId);
            statement.setObject(3, UUID.fromString(endpointId));
            statement.setString(4, (String)event.get(Constants.USER));
            statement.setObject(5, OffsetDateTime.parse((String)event.get(CloudEventV1.TIME)));
            statement.setLong(6, newAggregateVersion);

            int count = statement.executeUpdate();
            if (count == 0) {
                throw new SQLException(String.format("Failed during createPositionPermission for hostId %s positionId %s endpointId %s with aggregateVersion %d. It might already exist.", hostId, positionId, endpointId, newAggregateVersion));
            }
        } catch (SQLException e) {
            logger.error("SQLException during createPositionPermission for hostId {} positionId {} endpointId {} aggregateVersion {}: {}", hostId, positionId, endpointId, newAggregateVersion, e.getMessage(), e);
            throw e;
        } catch (Exception e) {
            logger.error("Exception during createPositionPermission for hostId {} positionId {} endpointId {} aggregateVersion {}: {}", hostId, positionId, endpointId, newAggregateVersion, e.getMessage(), e);
            throw e;
        }
    }

    private boolean queryPositionPermissionExists(Connection conn, String hostId, String positionId, String endpointId) throws SQLException {
        final String sql =
                """
                    SELECT COUNT(*) FROM position_permission_t
                    WHERE host_id = ?
                    AND position_id = ?
                    AND endpoint_id = ?
                """;
        try (PreparedStatement statement = conn.prepareStatement(sql)) {
            statement.setObject(1, UUID.fromString(hostId));
            statement.setString(2, positionId);
            statement.setObject(3, UUID.fromString(endpointId));

            try (ResultSet rs = statement.executeQuery()) {
                return rs.next() && rs.getInt(1) > 0;
            }
        }
    }

    @Override
    public void deletePositionPermission(Connection conn, Map<String, Object> event) throws SQLException, Exception {
        final String deleteGroup =
            """
                DELETE FROM position_permission_t
                WHERE host_id = ?
                AND position_id = ?
                AND endpoint_id = ?
                AND aggregate_version = ?
            """;
        Map<String, Object> map = (Map<String, Object>)event.get(PortalConstants.DATA);
        String hostId = (String)map.get("hostId");
        String positionId = (String)map.get("positionId");
        String endpointId = (String)map.get("endpointId");
        long oldAggregateVersion = SqlUtil.getOldAggregateVersion(event);

        try (PreparedStatement statement = conn.prepareStatement(deleteGroup)) {
            statement.setObject(1, UUID.fromString(hostId));
            statement.setString(2, positionId);
            statement.setObject(3, UUID.fromString(endpointId));
            statement.setLong(4, oldAggregateVersion);

            int count = statement.executeUpdate();
            if (count == 0) {
                if (queryPositionPermissionExists(conn, hostId, positionId, endpointId)) {
                    throw new ConcurrencyException(String.format("Optimistic concurrency conflict during deletePositionPermission for hostId %s positionId %s endpointId %s with aggregateVersion %d but found a different version or already updated.", hostId, positionId, endpointId, oldAggregateVersion));
                } else {
                    throw new SQLException(String.format("No record found during deletePositionPermission for hostId %s positionId %s endpointId %s. It might have been already deleted.", hostId, positionId, endpointId));
                }
            }
        } catch (SQLException e) {
            logger.error("SQLException during deletePositionPermission for hostId {} positionId {} endpointId {} aggregateVersion {}: {}", hostId, positionId, endpointId, oldAggregateVersion, e.getMessage(), e);
            throw e;
        } catch (Exception e) {
            logger.error("Exception during deletePositionPermission for hostId {} positionId {} endpointId {} aggregateVersion {}: {}", hostId, positionId, endpointId, oldAggregateVersion, e.getMessage(), e);
            throw e;
        }
    }

    @Override
    public void createPositionUser(Connection conn, Map<String, Object> event) throws SQLException, Exception {
        final String insertGroup =
            """
            INSERT INTO position_user_t
            (host_id, position_id, user_id, start_ts, end_ts, update_user, update_ts, aggregate_version)
            VALUES (?, ?, ?, ?, ?,  ?, ?, ?)
            """;

        Map<String, Object> map = (Map<String, Object>)event.get(PortalConstants.DATA);
        String hostId = (String)map.get("hostId");
        String positionId = (String)map.get("positionId");
        String userId = (String)map.get("userId");
        long newAggregateVersion = SqlUtil.getNewAggregateVersion(event);

        try (PreparedStatement statement = conn.prepareStatement(insertGroup)) {
            statement.setObject(1, UUID.fromString(hostId));
            statement.setString(2, positionId);
            statement.setObject(3, UUID.fromString(userId));
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
            statement.setLong(8, newAggregateVersion);

            int count = statement.executeUpdate();
            if (count == 0) {
                throw new SQLException(String.format("Failed during createPositionUser for hostId %s positionId %s userId %s with aggregateVersion %d. It might already exist.", hostId, positionId, userId, newAggregateVersion));
            }
        } catch (SQLException e) {
            logger.error("SQLException during createPositionUser for hostId {} positionId {} userId {} and aggregateVersion {}: {}", hostId, positionId, userId, newAggregateVersion, e.getMessage(), e);
            throw e;
        } catch (Exception e) {
            logger.error("Exception during createPositionUser for hostId {} positionId {} userId {} and aggregateVersion {}: {}", hostId, positionId, userId, newAggregateVersion, e.getMessage(), e);
            throw e;
        }
    }

    private boolean queryPositionUserExists(Connection conn, String hostId, String positionId, String userId) throws SQLException {
        final String sql =
                """
                SELECT COUNT(*) FROM position_user_t WHERE host_id = ? AND position_id = ? AND user_id = ?
                """;
        try (PreparedStatement pst = conn.prepareStatement(sql)) {
            pst.setObject(1, UUID.fromString(hostId));
            pst.setString(2, positionId);
            pst.setObject(3, UUID.fromString(userId));
            try (ResultSet rs = pst.executeQuery()) {
                return rs.next() && rs.getInt(1) > 0;
            }
        }
    }

    @Override
    public void updatePositionUser(Connection conn, Map<String, Object> event) throws SQLException, Exception {
        final String updateGroup =
                """
                UPDATE position_user_t SET start_ts = ?, end_ts = ?, update_user = ?, update_ts = ?, aggregate_version = ?
                WHERE host_id = ? AND position_id = ? AND user_id = ? AND aggregate_version = ?
                """;

        Map<String, Object> map = (Map<String, Object>)event.get(PortalConstants.DATA);
        String hostId = (String)map.get("hostId");
        String positionId = (String)map.get("positionId");
        String userId = (String)map.get("userId");
        long oldAggregateVersion = SqlUtil.getOldAggregateVersion(event);
        long newAggregateVersion = SqlUtil.getNewAggregateVersion(event);
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
            statement.setLong(5, newAggregateVersion);
            statement.setObject(6, UUID.fromString(hostId));
            statement.setString(7, positionId);
            statement.setObject(8, UUID.fromString(userId));
            statement.setLong(9, oldAggregateVersion);

            int count = statement.executeUpdate();
            if (count == 0) {
                if (queryPositionUserExists(conn, hostId, positionId, userId)) {
                    throw new ConcurrencyException(String.format("Optimistic concurrency conflict during updatePositionUser for hostId %s positionId %s userId %s. Expected version %d but found a different version %d.", hostId, positionId, userId, oldAggregateVersion, newAggregateVersion));
                } else {
                    throw new SQLException(String.format("No record found during updatePositionUser for hostId %s positionId %s userId %s.", hostId, positionId, userId));
                }
            }
        } catch (SQLException e) {
            logger.error("SQLException during updatePositionUser for hostId {} positionId {} userId {} (old: {}) -> (new: {}): {}", hostId, positionId, userId, oldAggregateVersion, newAggregateVersion, e.getMessage(), e);
            throw e;
        } catch (Exception e) {
            logger.error("Exception during updatePositionUser for hostId {} positionId {} userId {} (old: {}) -> (new: {}): {}", hostId, positionId, userId, oldAggregateVersion, newAggregateVersion, e.getMessage(), e);
            throw e;
        }
    }

    @Override
    public void deletePositionUser(Connection conn, Map<String, Object> event) throws SQLException, Exception {
        final String deleteGroup =
            """
                DELETE from position_user_t
                WHERE host_id = ? AND position_id = ? AND user_id = ? AND aggregate_version = ?
            """;
        Map<String, Object> map = (Map<String, Object>)event.get(PortalConstants.DATA);
        String hostId = (String)map.get("hostId");
        String positionId = (String)map.get("positionId");
        String userId = (String)map.get("userId");
        long oldAggregateVersion = SqlUtil.getOldAggregateVersion(event);
        try (PreparedStatement statement = conn.prepareStatement(deleteGroup)) {
            statement.setObject(1, UUID.fromString(hostId));
            statement.setString(2, positionId);
            statement.setObject(3, UUID.fromString((String)event.get(Constants.USER)));
            statement.setLong(4, oldAggregateVersion);

            int count = statement.executeUpdate();
            if (count == 0) {
                if (queryPositionUserExists(conn, hostId, positionId, userId)) {
                    throw new ConcurrencyException(String.format("Optimistic concurrency conflict during deletePositionUser for hostId %s positionId %s userId %s aggregateVersion %d but found a different version or already updated. ", hostId, positionId, userId, oldAggregateVersion));
                } else {
                    throw new SQLException(String.format("No record found during deletePositionUser for hostId %s positionId %s userId %s. It might have been already deleted.", hostId, positionId, userId));
                }
            }
        } catch (SQLException e) {
            logger.error("SQLException during deletePositionUser for hostId {} positionId {} userId {} aggregateVersion {}: {}", hostId, positionId, userId, oldAggregateVersion,  e.getMessage(), e);
            throw e;
        } catch (Exception e) {
            logger.error("Exception during deletePositionUser for hostId {} positionId {} userId {} aggregateVersion {}: {}", hostId, positionId, userId, oldAggregateVersion, e.getMessage(), e);
            throw e;
        }
    }

    @Override
    public Result<String> queryPositionRowFilter(int offset, int limit, String filtersJson, String globalFilter, String sortingJson, String hostId) {
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

        List<Map<String, Object>> filters = parseJsonList(filtersJson);
        List<Map<String, Object>> sorting = parseJsonList(sortingJson);

        String s =
            """
            SELECT COUNT(*) OVER () AS total, o.host_id, o.position_id, p.endpoint_id, ae.endpoint,
            av.api_version_id, av.api_id, av.api_version, p.col_name, p.operator, p.col_value,
            p.update_user, p.update_ts, p.aggregate_version
            FROM position_row_filter_t p
            JOIN position_t o ON o.host_id = p.host_id AND o.position_id = p.position_id
            JOIN api_endpoint_t ae ON ae.host_id = p.host_id AND ae.endpoint_id = p.endpoint_id
            JOIN api_version_t av ON av.host_id = ae.host_id AND av.api_version_id = ae.api_version_id
            WHERE p.host_id = ?
            """;

        List<Object> parameters = new ArrayList<>();
        parameters.add(UUID.fromString(hostId));

        String[] searchColumns = {"o.position_id", "ae.endpoint", "p.col_name", "p.col_value"};
        String sqlBuilder = s + dynamicFilter(Arrays.asList("o.host_id", "p.endpoint_id", "av.api_version_id"), filters, columnMap, parameters) +
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
    public void createPositionRowFilter(Connection conn, Map<String, Object> event) throws SQLException, Exception {
        final String insertGroup =
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
                    aggregate_version
                ) VALUES (?, ?, ?, ?, ?,  ?, ?, ?, ?)
            """;

        Map<String, Object> map = (Map<String, Object>)event.get(PortalConstants.DATA);
        String hostId = (String)map.get("hostId");
        String positionId = (String)map.get("positionId");
        String endpointId = (String)map.get("endpointId");
        String colName = (String)map.get("colName");
        long newAggregateVersion = SqlUtil.getNewAggregateVersion(event);

        try (PreparedStatement statement = conn.prepareStatement(insertGroup)) {
            statement.setObject(1, UUID.fromString(hostId));
            statement.setString(2, positionId);
            statement.setObject(3, UUID.fromString(endpointId));
            statement.setString(4, colName);
            statement.setString(5, (String)map.get("operator"));
            statement.setString(6, (String)map.get("colValue"));
            statement.setString(7, (String)event.get(Constants.USER));
            statement.setObject(8, OffsetDateTime.parse((String)event.get(CloudEventV1.TIME)));
            statement.setLong(9, newAggregateVersion);

            int count = statement.executeUpdate();
            if (count == 0) {
                throw new SQLException(String.format("Failed during createPositionRowFilter for hostId %s positionId %s endpointId %s colName %s with aggregate version %d. It might already exist.", hostId, positionId, endpointId, colName, newAggregateVersion));
            }
        } catch (SQLException e) {
            logger.error("SQLException during createPositionRowFilter for hostId {} positionId {} endpointId {} colName {} aggregateVersion {}: {}", hostId, positionId, endpointId, colName, newAggregateVersion, e.getMessage(), e);
            throw e;
        } catch (Exception e) {
            logger.error("Exception during createPositionRowFilter for hostId {} positionId {} endpointId {} colName {} aggregateVersion {}: {}", hostId, positionId, endpointId, colName, newAggregateVersion, e.getMessage(), e);
            throw e;
        }
    }

    private boolean queryPositionRowFilterExists(Connection conn, String hostId, String positionId, String endpointId, String colName) throws SQLException {
        final String sql =
                """
                SELECT COUNT(*) FROM position_row_filter_t WHERE host_id = ? AND position_id = ? AND endpoint_id = ? AND col_name = ?
                """;
        try (PreparedStatement pst = conn.prepareStatement(sql)) {
            pst.setObject(1, UUID.fromString(hostId));
            pst.setString(2, positionId);
            pst.setObject(3, UUID.fromString(endpointId));
            pst.setString(4, colName);
            try (ResultSet rs = pst.executeQuery()) {
                return rs.next() && rs.getInt(1) > 0;
            }
        }
    }

    @Override
    public void updatePositionRowFilter(Connection conn, Map<String, Object> event) throws SQLException, Exception {
        final String updateGroup =
            """
                UPDATE position_row_filter_t
                SET
                    operator = ?,
                    col_value = ?,
                    update_user = ?,
                    update_ts = ?,
                    aggregate_version = ?
                WHERE
                    host_id = ?
                    AND position_id = ?
                    AND endpoint_id = ?
                    AND col_name = ?
                    AND aggregate_version = ?
            """;

        Map<String, Object> map = (Map<String, Object>)event.get(PortalConstants.DATA);
        String hostId = (String)map.get("hostId");
        String positionId = (String)map.get("positionId");
        String endpointId = (String)map.get("endpointId");
        String colName = (String)map.get("colName");
        long oldAggregateVersion = SqlUtil.getOldAggregateVersion(event);
        long newAggregateVersion = SqlUtil.getNewAggregateVersion(event);
        try (PreparedStatement statement = conn.prepareStatement(updateGroup)) {
            statement.setString(1, (String)map.get("operator"));
            statement.setString(2, (String)map.get("colValue"));
            statement.setString(3, (String)event.get(Constants.USER));
            statement.setObject(4, OffsetDateTime.parse((String)event.get(CloudEventV1.TIME)));
            statement.setLong(5, newAggregateVersion);
            statement.setObject(6, UUID.fromString(hostId));
            statement.setString(7, positionId);
            statement.setObject(8, UUID.fromString(endpointId));
            statement.setString(9, colName);
            statement.setLong(10, oldAggregateVersion);

            int count = statement.executeUpdate();
            if (count == 0) {
                if (queryPositionRowFilterExists(conn, hostId, positionId, endpointId, colName)) {
                    throw new ConcurrencyException(String.format("Optimistic concurrency conflict during updatePositionRowFilter for hostId %s positionId %s endpointId %s colName %s. Expected aggregateVersion %d but found a different version %d.", hostId, positionId, endpointId, colName, oldAggregateVersion, newAggregateVersion));
                } else {
                    throw new SQLException(String.format("No record found during updatePositionRowFilter for hostId %s positionId %s endpointId %s colName %s.", hostId, positionId, endpointId, colName));
                }
            }
        } catch (SQLException e) {
            logger.error("SQLException during updatePositionRowFilter for hostId {} positionId {} endpointId {} colName {} (old: {}) -> (new: {}): {}", hostId, positionId, endpointId, colName, oldAggregateVersion, newAggregateVersion, e.getMessage(), e);
            throw e;
        } catch (Exception e) {
            logger.error("Exception during updatePositionRowFilter for hostId {} positionId {} endpointId {} colName {} (old: {}) -> (new: {}): {}", hostId, positionId, endpointId, colName, oldAggregateVersion, newAggregateVersion, e.getMessage(), e);
            throw e;
        }
    }

    @Override
    public void deletePositionRowFilter(Connection conn, Map<String, Object> event) throws SQLException, Exception {
        final String deleteGroup =
                """
                    DELETE FROM position_row_filter_t
                    WHERE host_id = ?
                    AND position_id = ?
                    AND endpoint_id = ?
                    AND col_name = ?
                    AND aggregate_version = ?
                """;
        Map<String, Object> map = (Map<String, Object>)event.get(PortalConstants.DATA);
        String hostId = (String)map.get("hostId");
        String positionId = (String)map.get("positionId");
        String endpointId = (String)map.get("endpointId");
        String colName = (String)map.get("colName");
        long oldAggregateVersion = SqlUtil.getOldAggregateVersion(event);
        try (PreparedStatement statement = conn.prepareStatement(deleteGroup)) {
            statement.setObject(1, UUID.fromString(hostId));
            statement.setString(2, positionId);
            statement.setObject(3, UUID.fromString(endpointId));
            statement.setString(4, colName);
            statement.setLong(5, oldAggregateVersion);

            int count = statement.executeUpdate();
            if (count == 0) {
                if (queryPositionRowFilterExists(conn, hostId, positionId, endpointId, colName)) {
                    throw new ConcurrencyException(String.format("Optimistic concurrency conflict during deletePositionRowFilter for hostId %s positionId %s endpointId %s colName %s aggregateVersion %d but found a different version or already updated.", hostId, positionId, endpointId, colName, oldAggregateVersion));
                } else {
                    throw new SQLException(String.format("No record found during deletePositionRowFilter for hostId %s positionId %s endpointId %s colName %s. It might have been already deleted.", hostId, positionId, endpointId, colName));
                }
            }
        } catch (SQLException e) {
            logger.error("SQLException during deletePositionRowFilter for hostId {} positionId {} endpointId {} colName {} aggregateVersion {}: {}", hostId, positionId, endpointId, colName, oldAggregateVersion, e.getMessage(), e);
            throw e;
        } catch (Exception e) {
            logger.error("Exception during deletePositionRowFilter for hostId {} positionId {} endpointId {} colName {} aggregateVersion {}: {}", hostId, positionId, endpointId, colName, oldAggregateVersion, e.getMessage(), e);
            throw e;
        }
    }

    @Override
    public Result<String> queryPositionColFilter(int offset, int limit, String filtersJson, String globalFilter, String sortingJson, String hostId) {
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

        List<Map<String, Object>> filters = parseJsonList(filtersJson);
        List<Map<String, Object>> sorting = parseJsonList(sortingJson);

        String s =
            """
            SELECT COUNT(*) OVER () AS total,
            o.host_id, o.position_id, av.api_version_id, av.api_id, av.api_version,
            ae.endpoint_id, ae.endpoint, cf.columns, cf.aggregate_version,
            cf.update_user, cf.update_ts
            FROM position_col_filter_t cf
            JOIN position_t o ON o.position_id = cf.position_id
            JOIN api_endpoint_t ae ON cf.host_id = ae.host_id AND cf.endpoint_id = ae.endpoint_id
            JOIN api_version_t av ON ae.host_id = av.host_id AND ae.api_version_id = av.api_version_id
            WHERE cf.host_id = ?
            """;

        List<Object> parameters = new ArrayList<>();
        parameters.add(UUID.fromString(hostId));

        String[] searchColumns = {"o.position_id", "ae.endpoint", "cf.columns"};
        String sqlBuilder = s + dynamicFilter(Arrays.asList("o.host_id", "av.api_version_id", "ae.endpoint_id"), filters, columnMap, parameters) +
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
    public void createPositionColFilter(Connection conn, Map<String, Object> event) throws SQLException, Exception {
        final String insertGroup =
                """
                    INSERT INTO position_col_filter_t (
                        host_id,
                        position_id,
                        endpoint_id,
                        columns,
                        update_user,
                        update_ts,
                        aggregate_version
                    ) VALUES (?, ?, ?, ?, ?,  ?, ?)
                """;

        Map<String, Object> map = (Map<String, Object>)event.get(PortalConstants.DATA);
        String hostId = (String)map.get("hostId");
        String positionId = (String)map.get("positionId");
        String endpointId = (String)map.get("endpointId");
        long newAggregateVersion = SqlUtil.getNewAggregateVersion(event);
        try (PreparedStatement statement = conn.prepareStatement(insertGroup)) {
            statement.setObject(1, UUID.fromString(hostId));
            statement.setString(2, positionId);
            statement.setObject(3, UUID.fromString(endpointId));
            statement.setString(4, (String)map.get("columns"));
            statement.setString(5, (String)event.get(Constants.USER));
            statement.setObject(6, OffsetDateTime.parse((String)event.get(CloudEventV1.TIME)));
            statement.setLong(7, newAggregateVersion);

            int count = statement.executeUpdate();
            if (count == 0) {
                throw new SQLException(String.format("Failed during createPositionColFilter for hostId %s positionId %s endpointId %s with aggregate version %d. It might already exist.", hostId, positionId, endpointId, newAggregateVersion));
            }
        } catch (SQLException e) {
            logger.error("SQLException during createPositionColFilter for hostId {} positionId {} endpointId {} aggregateVersion {}: {}", hostId, positionId, endpointId, newAggregateVersion, e.getMessage(), e);
            throw e;
        } catch (Exception e) {
            logger.error("Exception during createPositionColFilter for hostId {} positionId {} endpointId {} aggregateVersion {}: {}", hostId, positionId, endpointId, newAggregateVersion, e.getMessage(), e);
            throw e;
        }
    }

    private boolean queryPositionColFilterExists(Connection conn, String hostId, String positionId, String endpointId) throws SQLException {
        final String sql =
                """
                SELECT COUNT(*) FROM position_col_filter_t WHERE host_id = ? AND position_id = ? AND endpoint_id = ?
                """;
        try (PreparedStatement pst = conn.prepareStatement(sql)) {
            pst.setObject(1, UUID.fromString(hostId));
            pst.setString(2, positionId);
            pst.setObject(3, UUID.fromString(endpointId));
            try (ResultSet rs = pst.executeQuery()) {
                return rs.next() && rs.getInt(1) > 0;
            }
        }
    }

    @Override
    public void updatePositionColFilter(Connection conn, Map<String, Object> event) throws SQLException, Exception {
        final String updateGroup =
                """
                    UPDATE position_col_filter_t
                    SET
                        columns = ?,
                        update_user = ?,
                        update_ts = ?,
                        aggregate_version = ?
                    WHERE
                        host_id = ?
                        AND position_id = ?
                        AND endpoint_id = ?
                        AND aggregate_version = ?
                """;

        Map<String, Object> map = (Map<String, Object>)event.get(PortalConstants.DATA);
        String hostId = (String)map.get("hostId");
        String positionId = (String)map.get("positionId");
        String endpointId = (String)map.get("endpointId");
        long oldAggregateVersion = SqlUtil.getOldAggregateVersion(event);
        long newAggregateVersion = SqlUtil.getNewAggregateVersion(event);
        try (PreparedStatement statement = conn.prepareStatement(updateGroup)) {
            statement.setString(1, (String)map.get("columns"));
            statement.setString(2, (String)event.get(Constants.USER));
            statement.setObject(3, OffsetDateTime.parse((String)event.get(CloudEventV1.TIME)));
            statement.setLong(4, newAggregateVersion);
            statement.setObject(5, UUID.fromString(hostId));
            statement.setString(6, positionId);
            statement.setObject(7, UUID.fromString(endpointId));
            statement.setLong(8, oldAggregateVersion);

            int count = statement.executeUpdate();
            if (count == 0) {
                if (queryPositionColFilterExists(conn, hostId, positionId, endpointId)) {
                    throw new ConcurrencyException(String.format("Optimistic concurrency conflict during updatePositionColFilter for hostId %s positionId %s endpointId %s. Expected version %d but found a different version %d.", hostId, positionId, endpointId, oldAggregateVersion, newAggregateVersion));
                } else {
                    throw new SQLException(String.format("No record found during updatePositionColFilter for hostId %s positionId %s endpointId %s.", hostId, positionId, endpointId));
                }
            }
        } catch (SQLException e) {
            logger.error("SQLException during updatePositionColFilter for hostId {} positionId {} endpointId {} (old: {}) -> (new: {}): {}", hostId, positionId, endpointId, oldAggregateVersion, newAggregateVersion, e.getMessage(), e);
            throw e;
        } catch (Exception e) {
            logger.error("Exception during updatePositionColFilter for hostId {} positionId {} endpointId {} (old: {}) -> (new: {}): {}", hostId, positionId, endpointId, oldAggregateVersion, newAggregateVersion, e.getMessage(), e);
            throw e;
        }
    }

    @Override
    public void deletePositionColFilter(Connection conn, Map<String, Object> event) throws SQLException, Exception {
        final String deleteGroup =
                """
                    DELETE FROM position_col_filter_t
                    WHERE host_id = ?
                    AND position_id = ?
                    AND endpoint_id = ?
                    AND aggregate_version = ?
                """;
        Map<String, Object> map = (Map<String, Object>)event.get(PortalConstants.DATA);
        String hostId = (String)map.get("hostId");
        String positionId = (String)map.get("positionId");
        String endpointId = (String)map.get("endpointId");
        long oldAggregateVersion = SqlUtil.getOldAggregateVersion(event);
        try (PreparedStatement statement = conn.prepareStatement(deleteGroup)) {
            statement.setObject(1, UUID.fromString(hostId));
            statement.setString(2, positionId);
            statement.setObject(3, UUID.fromString(endpointId));
            statement.setLong(4, oldAggregateVersion);

            int count = statement.executeUpdate();
            if (count == 0) {
                if (queryPositionColFilterExists(conn, hostId, positionId, endpointId)) {
                    throw new ConcurrencyException(String.format("Optimistic concurrency conflict during deletePositionColFilter for hostId %s positionId %s endpointId %s aggregateVersion %d but found a different version or already updated.", hostId, positionId, endpointId, oldAggregateVersion));
                } else {
                    throw new SQLException(String.format("No record found during deletePositionColFilter for hostId %s positionId %s endpointId %s. It might have been already deleted.", hostId, positionId, endpointId));
                }
            }
        } catch (SQLException e) {
            logger.error("SQLException during deletePositionColFilter for hostId {} positionId {} endpointId {} aggregateVersion {}: {}", hostId, positionId, endpointId, oldAggregateVersion, e.getMessage(), e);
            throw e;
        } catch (Exception e) {
            logger.error("Exception during deletePositionColFilter for hostId {} positionId {} endpointId {} aggregateVersion {}: {}", hostId, positionId, endpointId, oldAggregateVersion, e.getMessage(), e);
            throw e;
        }
    }

    @Override
    public void createAttribute(Connection conn, Map<String, Object> event) throws SQLException, Exception {
        final String insertAttribute =
            """
            INSERT INTO attribute_t (host_id, attribute_id, attribute_type,
            attribute_desc, update_user, update_ts, aggregate_version)
            VALUES (?, ?, ?, ?, ?,  ?, ?)
            """;

        Map<String, Object> map = (Map<String, Object>)event.get(PortalConstants.DATA);
        String hostId = (String)map.get("hostId");
        String attributeId = (String)map.get("attributeId");
        long newAggregateVersion = SqlUtil.getNewAggregateVersion(event);
        try (PreparedStatement statement = conn.prepareStatement(insertAttribute)) {
            statement.setObject(1, UUID.fromString(hostId));
            statement.setString(2, attributeId);
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
            statement.setLong(7, newAggregateVersion);

            int count = statement.executeUpdate();
            if (count == 0) {
                throw new SQLException(String.format("Failed during createAttribute for hostId %s attributeId %s with aggregate version %d. It might already exist.", hostId, attributeId, newAggregateVersion));
            }
        } catch (SQLException e) {
            logger.error("SQLException during createAttribute for hostId {} attributeId {} aggregateVersion {}: {}", hostId, attributeId, newAggregateVersion, e.getMessage(), e);
            throw e;
        } catch (Exception e) {
            logger.error("Exception during createAttribute for hostId {} attributeId {} aggregateVersion {}: {}", hostId, attributeId, newAggregateVersion, e.getMessage(), e);
            throw e;
        }
    }

    private boolean queryAttributeExists(Connection conn, String hostId, String attributeId) throws SQLException {
        final String sql = "SELECT COUNT(*) FROM attribute_t WHERE host_id = ? AND attribute_id = ?";
        try (PreparedStatement pst = conn.prepareStatement(sql)) {
            pst.setObject(1, UUID.fromString(hostId));
            pst.setString(2, attributeId);
            try (ResultSet rs = pst.executeQuery()) {
                return rs.next() && rs.getInt(1) > 0;
            }
        }
    }

    @Override
    public void updateAttribute(Connection conn, Map<String, Object> event) throws SQLException, Exception {
        final String updateAttribute =
                """
                UPDATE attribute_t SET attribute_desc = ?, attribute_type = ?,
                update_user = ?, update_ts = ?, aggregate_version = ?
                WHERE host_id = ? AND attribute_id = ? AND aggregate_version = ?
                """;

        Map<String, Object> map = (Map<String, Object>)event.get(PortalConstants.DATA);
        String hostId = (String)map.get("hostId");
        String attributeId = (String)map.get("attributeId");
        long oldAggregateVersion = SqlUtil.getOldAggregateVersion(event);
        long newAggregateVersion = SqlUtil.getNewAggregateVersion(event);
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
            statement.setLong(5, newAggregateVersion);
            statement.setObject(6, UUID.fromString(hostId));
            statement.setString(7, attributeId);
            statement.setLong(8, oldAggregateVersion);

            int count = statement.executeUpdate();
            if (count == 0) {
                if (queryAttributeExists(conn, hostId, attributeId)) {
                    throw new ConcurrencyException(String.format("Optimistic concurrency conflict during updateAttribute for hostId %s attributeId %s. Expected version %d but found a different version %d.", hostId, attributeId, oldAggregateVersion, newAggregateVersion));
                } else {
                    throw new SQLException(String.format("No record found during updateAttribute for hostId %s attributeId %s.", hostId, attributeId));
                }
            }
        } catch (SQLException e) {
            logger.error("SQLException during updateAttribute for hostId {} attributeId {} (old: {}) -> (new: {}): {}", hostId, attributeId, oldAggregateVersion, newAggregateVersion, e.getMessage(), e);
            throw e;
        } catch (Exception e) {
            logger.error("Exception during updateAttribute for hostId {} attributeId {} (old: {}) -> (new: {}): {}", hostId, attributeId, oldAggregateVersion, newAggregateVersion, e.getMessage(), e);
            throw e;
        }
    }

    @Override
    public void deleteAttribute(Connection conn, Map<String, Object> event) throws SQLException, Exception {
        final String deleteGroup = "DELETE from attribute_t WHERE host_id = ? AND attribute_id = ? AND aggregate_version = ?";
        Map<String, Object> map = (Map<String, Object>)event.get(PortalConstants.DATA);
        String hostId = (String)map.get("hostId");
        String attributeId = (String)map.get("attributeId");
        long oldAggregateVersion = SqlUtil.getOldAggregateVersion(event);
        try (PreparedStatement statement = conn.prepareStatement(deleteGroup)) {
            statement.setObject(1, UUID.fromString(hostId));
            statement.setString(2, attributeId);
            statement.setLong(3, oldAggregateVersion);

            int count = statement.executeUpdate();
            if (count == 0) {
                if (queryAttributeExists(conn, hostId, attributeId)) {
                    throw new ConcurrencyException(String.format("Optimistic concurrency conflict during deleteAttribute for hostId %s attributeId %s aggregateVersion %d but found a different version or already updated. ", hostId, attributeId, oldAggregateVersion));
                } else {
                    throw new SQLException(String.format("No record found during deleteAttribute for hostId %s attributeId %s. It might have been already deleted.", hostId, attributeId)) ;
                }
            }
        } catch (SQLException e) {
            logger.error("SQLException during deleteAttribute for hostId {} attributeId {} aggregateVersion {}: {}", hostId, attributeId, oldAggregateVersion, e.getMessage(), e);
            throw e;
        } catch (Exception e) {
            logger.error("Exception during deleteAttribute for hostId {} attributeId {} aggregateVersion {}: {}", hostId, attributeId, oldAggregateVersion, e.getMessage(), e);
            throw e;
        }
    }

    @Override
    public Result<String> queryAttribute(int offset, int limit, String filtersJson, String globalFilter, String sortingJson, String hostId) {
        Result<String> result;
        List<Map<String, Object>> filters = parseJsonList(filtersJson);
        List<Map<String, Object>> sorting = parseJsonList(sortingJson);

        String s =
            """
            SELECT COUNT(*) OVER () AS total,
            host_id, attribute_id, attribute_type, attribute_desc,
            update_user, update_ts, aggregate_version
            FROM attribute_t
            WHERE host_id = ?
            """;

        List<Object> parameters = new ArrayList<>();
        parameters.add(UUID.fromString(hostId));
        String[] searchColumns = {"attribute_id", "attribute_desc"};
        String sqlBuilder = s + dynamicFilter(Arrays.asList("host_id"), filters, null, parameters) +
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
    public Result<String> queryAttributePermission(int offset, int limit, String filtersJson, String globalFilter, String sortingJson, String hostId) {
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

        List<Map<String, Object>> filters = parseJsonList(filtersJson);
        List<Map<String, Object>> sorting = parseJsonList(sortingJson);

        String s =
            """
                SELECT COUNT(*) OVER () AS total,
                ap.host_id, ap.attribute_id, a.attribute_type, ap.attribute_value,
                av.api_version_id, av.api_id, av.api_version, ap.endpoint_id, ae.endpoint,
                ap.aggregate_version, ap.update_user, ap.update_ts
                FROM attribute_permission_t ap
                JOIN attribute_t a ON ap.attribute_id = a.attribute_id
                JOIN api_endpoint_t ae ON ap.host_id = ae.host_id AND ap.endpoint_id = ae.endpoint_id
                JOIN api_version_t av ON ae.host_id = av.host_id AND ae.api_version_id = av.api_version_id
                AND ap.host_id = ?
            """;

        List<Object> parameters = new ArrayList<>();
        parameters.add(UUID.fromString(hostId));

        String[] searchColumns = {"ap.attribute_id", "ap.attribute_value", "ae.endpoint"};
        String sqlBuilder = s + dynamicFilter(Arrays.asList("ap.host_id", "av.api_version_id", "ap.endpoint_id"), filters, columnMap, parameters) +
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
    public Result<String> queryAttributeUser(int offset, int limit, String filtersJson, String globalFilter, String sortingJson, String hostId) {
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
                e.manager_id, u.first_name, u.last_name
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

        String[] searchColumns = {"a.attribute_id", "a.attribute_value", "u.email", "u.first_name", "u.last_name"};
        String sqlBuilder = s + dynamicFilter(Arrays.asList("a.host_id", "u.user_id"), filters, columnMap, parameters) +
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
                    map.put("startTs", resultSet.getDate("start_ts"));
                    map.put("endTs", resultSet.getString("end_ts"));
                    map.put("userId", resultSet.getObject("user_id", UUID.class));
                    map.put("entityId", resultSet.getString("entity_id"));
                    map.put("email", resultSet.getString("email"));
                    map.put("firstName", resultSet.getString("first_name"));
                    map.put("lastName", resultSet.getString("last_name"));
                    map.put("userType", resultSet.getString("user_type"));
                    map.put("updateUser", resultSet.getString("update_user"));
                    map.put("updateTs", resultSet.getObject("update_ts") != null ? resultSet.getObject("update_ts", OffsetDateTime.class) : null);
                    map.put("aggregateVersion", resultSet.getLong("aggregate_version"));
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
    public void createAttributePermission(Connection conn, Map<String, Object> event) throws SQLException, Exception {
        final String insertGroup =
            """
                INSERT INTO attribute_permission_t (host_id, attribute_id, attribute_value, endpoint_id, update_user, update_ts, aggregate_version)
                VALUES (?, ?, ?, ?, ?,  ?, ?)
            """;

        Map<String, Object> map = (Map<String, Object>)event.get(PortalConstants.DATA);
        String hostId = (String)map.get("hostId");
        String attributeId = (String)map.get("attributeId");
        String endpointId = (String)map.get("endpointId");
        long newAggregateVersion = SqlUtil.getNewAggregateVersion(event);
        try (PreparedStatement statement = conn.prepareStatement(insertGroup)) {
            statement.setObject(1, UUID.fromString(hostId));
            statement.setString(2, attributeId);
            statement.setString(3, (String)map.get("attributeValue"));
            statement.setObject(4, UUID.fromString(endpointId));
            statement.setString(5, (String)event.get(Constants.USER));
            statement.setObject(6, OffsetDateTime.parse((String)event.get(CloudEventV1.TIME)));
            statement.setLong(7, newAggregateVersion);

            int count = statement.executeUpdate();
            if (count == 0) {
                throw new SQLException(String.format("Failed during createAttributePermission for hostId %s attributeId %s endpointId %s with aggregateVersion %d. It might already exist.", hostId, attributeId, endpointId, newAggregateVersion));
            }
        } catch (SQLException e) {
            logger.error("SQLException during createAttributePermission for hostId {} attributeId {} endpointId {} aggregateVersion {}: {}", hostId, attributeId, endpointId, newAggregateVersion, e.getMessage(), e);
            throw e;
        } catch (Exception e) {
            logger.error("Exception during createAttributePermission for hostId {} attributeId {} endpointId {} aggregateVersion {}: {}", hostId, attributeId, endpointId, newAggregateVersion, e.getMessage(), e);
            throw e;
        }
    }

    private boolean queryAttributePermissionExists(Connection conn, String hostId, String attributeId, String endpointId) throws SQLException {
        final String sql =
                """
                    SELECT COUNT(*) FROM attribute_permission_t
                    WHERE host_id = ?
                    AND attribute_id = ?
                    AND endpoint_id = ?
                """;
        try (PreparedStatement statement = conn.prepareStatement(sql)) {
            statement.setObject(1, UUID.fromString(hostId));
            statement.setString(2, attributeId);
            statement.setObject(3, UUID.fromString(endpointId));

            try (ResultSet rs = statement.executeQuery()) {
                return rs.next() && rs.getInt(1) > 0;
            }
        }
    }

    @Override
    public void updateAttributePermission(Connection conn, Map<String, Object> event) throws SQLException, Exception {
        final String updateGroup = """
                UPDATE attribute_permission_t SET attribute_value = ?, update_user = ?, update_ts = ?, aggregate_version = ?
                WHERE host_id = ? AND attribute_id = ? AND endpoint_id = ? AND aggregate_version ?
                """;

        Map<String, Object> map = (Map<String, Object>)event.get(PortalConstants.DATA);
        String hostId = (String)map.get("hostId");
        String attributeId = (String)map.get("attributeId");
        String endpointId = (String)map.get("endpointId");
        long newAggregateVersion = SqlUtil.getNewAggregateVersion(event);
        long oldAggregateVersion = SqlUtil.getOldAggregateVersion(event);

        try (PreparedStatement statement = conn.prepareStatement(updateGroup)) {
            statement.setString(1, (String)map.get("attributeValue"));
            statement.setString(2, (String)event.get(Constants.USER));
            statement.setObject(3, OffsetDateTime.parse((String)event.get(CloudEventV1.TIME)));
            statement.setLong(4, newAggregateVersion);

            statement.setObject(5, UUID.fromString(hostId));
            statement.setString(6, attributeId);
            statement.setObject(7, UUID.fromString(endpointId));
            statement.setLong(8, oldAggregateVersion);

            int count = statement.executeUpdate();
            if (count == 0) {
                if (queryAttributePermissionExists(conn, hostId, attributeId, endpointId)) {
                    throw new ConcurrencyException(String.format("Optimistic concurrency conflict during updateAttributePermission for hostId %s attributeId %s endpointId %s. Expected version %d but found a different version %d.", hostId, attributeId, endpointId, oldAggregateVersion, newAggregateVersion));
                } else {
                    throw new SQLException(String.format("No record found during updateAttributePermission for hostId %s attributeId %s endpointId %s.", hostId, attributeId, endpointId));
                }
            }
        } catch (SQLException e) {
            logger.error("SQLException during updateAttributePermission for hostId {} attributeId {} endpointId {} (old: {}) -> (new: {}): {}", hostId, attributeId, endpointId, oldAggregateVersion, newAggregateVersion, e.getMessage(), e);
            throw e;
        } catch (Exception e) {
            logger.error("Exception during updateAttributePermission for hostId {} attributeId {} endpointId {} (old: {}) -> (new: {}): {}", hostId, attributeId, endpointId, oldAggregateVersion, newAggregateVersion, e.getMessage(), e);
            throw e;
        }
    }

    @Override
    public void deleteAttributePermission(Connection conn, Map<String, Object> event) throws SQLException, Exception {
        final String deleteGroup =
                """
                    DELETE FROM attribute_permission_t
                    WHERE host_id = ?
                    AND attribute_id = ?
                    AND endpoint_id = ?
                    AND aggregate_version = ?
                """;
        Map<String, Object> map = (Map<String, Object>)event.get(PortalConstants.DATA);
        String hostId = (String)map.get("hostId");
        String attributeId = (String)map.get("attributeId");
        String endpointId = (String)map.get("endpointId");
        long oldAggregateVersion = SqlUtil.getOldAggregateVersion(event);
        try (PreparedStatement statement = conn.prepareStatement(deleteGroup)) {
            statement.setObject(1, UUID.fromString(hostId));
            statement.setString(2, attributeId);
            statement.setObject(3, UUID.fromString(endpointId));
            statement.setLong(4, oldAggregateVersion);

            int count = statement.executeUpdate();
            if (count == 0) {
                if (queryAttributePermissionExists(conn, hostId, attributeId, endpointId)) {
                    throw new ConcurrencyException(String.format("Optimistic concurrency conflict during deleteAttributePermission for hostId %s attributeId %s endpointId %s with aggregateVersion %d but found a different version or already updated.", hostId, attributeId, endpointId, oldAggregateVersion));
                } else {
                    throw new SQLException(String.format("No record found during deleteAttributePermission for hostId %s attributeId %s endpointId %s. It might have been already deleted.", hostId, attributeId, endpointId));
                }
            }
        } catch (SQLException e) {
            logger.error("SQLException during deleteAttributePermission for hostId {} attributeId {} endpointId {} aggregateVersion {}: {}", hostId, attributeId, endpointId, oldAggregateVersion, e.getMessage(), e);
            throw e;
        } catch (Exception e) {
            logger.error("Exception during deleteAttributePermission for hostId {} attributeId {} endpointId {} aggregateVersion {}: {}", hostId, attributeId, endpointId, oldAggregateVersion, e.getMessage(), e);
            throw e;
        }
    }

    @Override
    public void createAttributeUser(Connection conn, Map<String, Object> event) throws SQLException, Exception {
        final String insertGroup =
            """
            INSERT INTO attribute_user_t
            (host_id, attribute_id, attribute_value, user_id, start_ts, end_ts, update_user, update_ts, aggregate_version)
            VALUES (?, ?, ?, ?, ?,  ?, ?, ?, ?)
            """;

        Map<String, Object> map = (Map<String, Object>)event.get(PortalConstants.DATA);
        String hostId = (String)map.get("hostId");
        String attributeId = (String)map.get("attributeId");
        String userId = (String)map.get("userId");
        long newAggregateVersion = SqlUtil.getNewAggregateVersion(event);
        try (PreparedStatement statement = conn.prepareStatement(insertGroup)) {
            statement.setObject(1, UUID.fromString(hostId));
            statement.setString(2, attributeId);
            statement.setString(3, (String)map.get("attributeValue"));
            statement.setObject(4, UUID.fromString(userId));
            String startTs = (String)map.get("startTs");
            if(startTs != null && !startTs.isEmpty())
                statement.setObject(5, OffsetDateTime.parse(startTs));
            else
                statement.setNull(5, NULL);
            String endTs = (String)map.get("endTs");
            if (endTs != null && !endTs.isEmpty()) {
                statement.setObject(6, OffsetDateTime.parse(endTs));
            } else {
                statement.setNull(6, NULL);
            }
            statement.setString(7, (String)event.get(Constants.USER));
            statement.setObject(8, OffsetDateTime.parse((String)event.get(CloudEventV1.TIME)));
            statement.setLong(9, newAggregateVersion);

            int count = statement.executeUpdate();
            if (count == 0) {
                throw new SQLException(String.format("Failed during createAttributeUser for hostId %s attributeId %s userId %s with aggregateVersion %d. It might already exist.", hostId, attributeId, userId, newAggregateVersion));
            }
        } catch (SQLException e) {
            logger.error("SQLException during createAttributeUser for hostId {} attributeId {} userId {} and aggregateVersion {}: {}", hostId, attributeId, userId, newAggregateVersion, e.getMessage(), e);
            throw e;
        } catch (Exception e) {
            logger.error("Exception during createAttributeUser for hostId {} attributeId {} userId {} and aggregateVersion {}: {}", hostId, attributeId, userId, newAggregateVersion, e.getMessage(), e);
            throw e;
        }
    }

    private boolean queryAttributeUserExists(Connection conn, String hostId, String attributeId, String userId) throws SQLException {
        final String sql =
                """
                SELECT COUNT(*) FROM attribute_user_t WHERE host_id = ? AND attribute_id = ? AND user_id = ?
                """;
        try (PreparedStatement pst = conn.prepareStatement(sql)) {
            pst.setObject(1, UUID.fromString(hostId));
            pst.setString(2, attributeId);
            pst.setObject(3, UUID.fromString(userId));
            try (ResultSet rs = pst.executeQuery()) {
                return rs.next() && rs.getInt(1) > 0;
            }
        }
    }

    @Override
    public void updateAttributeUser(Connection conn, Map<String, Object> event) throws SQLException, Exception {
        final String updateAttributeUser =
                """
                UPDATE attribute_user_t
                SET attribute_value = ?, start_ts = ?, end_ts = ?,
                update_user = ?, update_ts = ?, aggregate_version = ?
                WHERE host_id = ? AND attribute_id = ? AND user_id = ? AND aggregate_version = ?
                """;

        Map<String, Object> map = (Map<String, Object>)event.get(PortalConstants.DATA);
        String hostId = (String)map.get("hostId");
        String attributeId = (String)map.get("attributeId");
        String userId = (String)map.get("userId");
        long oldAggregateVersion = SqlUtil.getOldAggregateVersion(event);
        long newAggregateVersion = SqlUtil.getNewAggregateVersion(event);
        try (PreparedStatement statement = conn.prepareStatement(updateAttributeUser)) {
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
            statement.setLong(6, newAggregateVersion);
            statement.setObject(7, UUID.fromString(hostId));
            statement.setString(8, attributeId);
            statement.setObject(9, UUID.fromString(userId));
            statement.setLong(10, oldAggregateVersion);

            int count = statement.executeUpdate();
            if (count == 0) {
                if (queryAttributeUserExists(conn, hostId, attributeId, userId)) {
                    throw new ConcurrencyException(String.format("Optimistic concurrency conflict during updateAttributeUser for hostId %s attributeId %s userId %s. Expected version %d but found a different version %d.", hostId, attributeId, userId, oldAggregateVersion, newAggregateVersion));
                } else {
                    throw new SQLException(String.format("No record found during updateAttributeUser for hostId %s attributeId %s userId %s.", hostId, attributeId, userId));
                }
            }
        } catch (SQLException e) {
            logger.error("SQLException during updateAttributeUser for hostId {} attributeId {} userId {} (old: {}) -> (new: {}): {}", hostId, attributeId, userId, oldAggregateVersion, newAggregateVersion, e.getMessage(), e);
            throw e;
        } catch (Exception e) {
            logger.error("Exception during updateAttributeUser for hostId {} attributeId {} userId {} (old: {}) -> (new: {}): {}", hostId, attributeId, userId, oldAggregateVersion, newAggregateVersion, e.getMessage(), e);
            throw e;
        }
    }

    @Override
    public void deleteAttributeUser(Connection conn, Map<String, Object> event) throws SQLException, Exception {
        final String deleteGroup =
        """
        DELETE from attribute_user_t
        WHERE host_id = ? AND attribute_id = ? AND user_id = ? AND aggregate_version = ?
        """;
        Map<String, Object> map = (Map<String, Object>)event.get(PortalConstants.DATA);
        String hostId = (String)map.get("hostId");
        String attributeId = (String)map.get("attributeId");
        String userId = (String)map.get("userId");
        long oldAggregateVersion = SqlUtil.getOldAggregateVersion(event);
        try (PreparedStatement statement = conn.prepareStatement(deleteGroup)) {
            statement.setObject(1, UUID.fromString(hostId));
            statement.setString(2, attributeId);
            statement.setObject(3, UUID.fromString(userId));
            statement.setLong(4, oldAggregateVersion);

            int count = statement.executeUpdate();
            if (count == 0) {
                if (queryAttributeUserExists(conn, hostId, attributeId, userId)) {
                    throw new ConcurrencyException(String.format("Optimistic concurrency conflict during deleteAttributeUser for hostId %s attributeId %s userId %s aggregateVersion %d but found a different version or already updated. ", hostId, attributeId, userId, oldAggregateVersion));
                } else {
                    throw new SQLException(String.format("No record found during deleteAttributeUser for hostId %s attributeId %s userId %s. It might have been already deleted.", hostId, attributeId, userId));
                }
            }
        } catch (SQLException e) {
            logger.error("SQLException during deleteAttributeUser for hostId {} attributeId {} userId {} aggregateVersion {}: {}", hostId, attributeId, userId, oldAggregateVersion,  e.getMessage(), e);
            throw e;
        } catch (Exception e) {
            logger.error("Exception during deleteAttributeUser for hostId {} attributeId {} userId {} aggregateVersion {}: {}", hostId, attributeId, userId, oldAggregateVersion, e.getMessage(), e);
            throw e;
        }
    }

    @Override
    public Result<String> queryAttributeRowFilter(int offset, int limit, String filtersJson, String globalFilter, String sortingJson, String hostId) {
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

        List<Map<String, Object>> filters = parseJsonList(filtersJson);
        List<Map<String, Object>> sorting = parseJsonList(sortingJson);

        String s =
            """
                SELECT COUNT(*) OVER () AS total, a.host_id, a.attribute_id, a.attribute_type,\s
                p.attribute_value, p.endpoint_id, ae.endpoint,
                av.api_version_id, av.api_id, av.api_version, p.col_name, p.operator, p.col_value,
                p.update_user, p.update_ts, p.aggregate_version
                FROM attribute_row_filter_t p
                JOIN attribute_t a ON a.host_id = p.host_id AND a.attribute_id = p.attribute_id
                JOIN api_endpoint_t ae ON ae.host_id = p.host_id AND ae.endpoint_id = p.endpoint_id
                JOIN api_version_t av ON av.host_id = ae.host_id AND av.api_version_id = ae.api_version_id
                WHERE p.host_id = ?
            """;

        List<Object> parameters = new ArrayList<>();
        parameters.add(UUID.fromString(hostId));

        String[] searchColumns = {"a.attribute_id", "p.attribute_value", "ae.endpoint", "p.col_name", "p.col_value"};
        String sqlBuilder = s + dynamicFilter(Arrays.asList("a.host_id", "p.endpoint_id", "av.api_version_id"), filters, columnMap, parameters) +
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
    public void createAttributeRowFilter(Connection conn, Map<String, Object> event) throws SQLException, Exception {
        final String insertGroup =
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
                        aggregate_version
                    ) VALUES(?, ?, ?, ?, ?,  ?, ?, ?, ?, ?)
                """;

        Map<String, Object> map = (Map<String, Object>)event.get(PortalConstants.DATA);
        String hostId = (String)map.get("hostId");
        String attributeId = (String)map.get("attributeId");
        String endpointId = (String)map.get("endpointId");
        String colName = (String)map.get("colName");
        long newAggregateVersion = SqlUtil.getNewAggregateVersion(event);

        try (PreparedStatement statement = conn.prepareStatement(insertGroup)) {
            statement.setObject(1, UUID.fromString(hostId));
            statement.setString(2, attributeId);
            statement.setString(3, (String)map.get("attributeValue"));
            statement.setObject(4, UUID.fromString(endpointId));
            statement.setString(5, colName);
            statement.setString(6, (String)map.get("operator"));
            statement.setString(7, (String)map.get("colValue"));
            statement.setString(8, (String)event.get(Constants.USER));
            statement.setObject(9, OffsetDateTime.parse((String)event.get(CloudEventV1.TIME)));
            statement.setLong(10, newAggregateVersion);

            int count = statement.executeUpdate();
            if (count == 0) {
                throw new SQLException(String.format("Failed during createAttributeRowFilter for hostId %s attributeId %s endpointId %s colName %s with aggregate version %d. It might already exist.", hostId, attributeId, endpointId, colName, newAggregateVersion));
            }
        } catch (SQLException e) {
            logger.error("SQLException during createAttributeRowFilter for hostId {} attributeId {} endpointId {} colName {} aggregateVersion {}: {}", hostId, attributeId, endpointId, colName, newAggregateVersion, e.getMessage(), e);
            throw e;
        } catch (Exception e) {
            logger.error("Exception during createAttributeRowFilter for hostId {} attributeId {} endpointId {} colName {} aggregateVersion {}: {}", hostId, attributeId, endpointId, colName, newAggregateVersion, e.getMessage(), e);
            throw e;
        }
    }

    private boolean queryAttributeRowFilterExists(Connection conn, String hostId, String attributeId, String endpointId, String colName) throws SQLException {
        final String sql =
                """
                SELECT COUNT(*) FROM attribute_row_filter_t WHERE host_id = ? AND attribute_id = ? AND endpoint_id = ? AND col_name = ?
                """;
        try (PreparedStatement pst = conn.prepareStatement(sql)) {
            pst.setObject(1, UUID.fromString(hostId));
            pst.setString(2, attributeId);
            pst.setObject(3, UUID.fromString(endpointId));
            pst.setString(4, colName);
            try (ResultSet rs = pst.executeQuery()) {
                return rs.next() && rs.getInt(1) > 0;
            }
        }
    }

    @Override
    public void updateAttributeRowFilter(Connection conn, Map<String, Object> event) throws SQLException, Exception {
        final String updateGroup =
                """
                        UPDATE attribute_row_filter_t
                        SET
                            attribute_value = ?,
                            operator = ?,
                            col_value = ?,
                            update_user = ?,
                            update_ts = ?,
                            aggregate_version = ?
                        WHERE
                            host_id = ?
                            AND attribute_id = ?
                            AND endpoint_id = ?
                            AND col_name = ?
                            AND aggregate_version = ?
                """;

        Map<String, Object> map = (Map<String, Object>)event.get(PortalConstants.DATA);
        String hostId = (String)map.get("hostId");
        String attributeId = (String)map.get("attributeId");
        String endpointId = (String)map.get("endpointId");
        String colName = (String)map.get("colName");
        long oldAggregateVersion = SqlUtil.getOldAggregateVersion(event);
        long newAggregateVersion = SqlUtil.getNewAggregateVersion(event);

        try (PreparedStatement statement = conn.prepareStatement(updateGroup)) {
            statement.setString(1, (String)map.get("attributeValue"));
            statement.setString(2, (String)map.get("operator"));
            statement.setString(3, (String)map.get("colValue"));
            statement.setString(4, (String)event.get(Constants.USER));
            statement.setObject(5, OffsetDateTime.parse((String)event.get(CloudEventV1.TIME)));
            statement.setLong(6, newAggregateVersion);
            statement.setObject(7, UUID.fromString(hostId));
            statement.setString(8, attributeId);
            statement.setObject(9, UUID.fromString(endpointId));
            statement.setString(10, colName);
            statement.setLong(11, oldAggregateVersion);


            int count = statement.executeUpdate();
            if (count == 0) {
                if (queryAttributeRowFilterExists(conn, hostId, attributeId, endpointId, colName)) {
                    throw new ConcurrencyException(String.format("Optimistic concurrency conflict during updateAttributeRowFilter for hostId %s attributeId %s endpointId %s colName %s. Expected aggregateVersion %d but found a different version %d.", hostId, attributeId, endpointId, colName, oldAggregateVersion, newAggregateVersion));
                } else {
                    throw new SQLException(String.format("No record found during updateAttributeRowFilter for hostId %s attributeId %s endpointId %s colName %s.", hostId, attributeId, endpointId, colName));
                }
            }
        } catch (SQLException e) {
            logger.error("SQLException during updateAttributeRowFilter for hostId {} attributeId {} endpointId {} colName {} (old: {}) -> (new: {}): {}", hostId, attributeId, endpointId, colName, oldAggregateVersion, newAggregateVersion, e.getMessage(), e);
            throw e;
        } catch (Exception e) {
            logger.error("Exception during updateAttributeRowFilter for hostId {} attributeId {} endpointId {} colName {} (old: {}) -> (new: {}): {}", hostId, attributeId, endpointId, colName, oldAggregateVersion, newAggregateVersion, e.getMessage(), e);
            throw e;
        }
    }

    @Override
    public void deleteAttributeRowFilter(Connection conn, Map<String, Object> event) throws SQLException, Exception {
        final String deleteGroup =
            """
                DELETE FROM attribute_row_filter_t
                WHERE host_id = ?
                AND attribute_id = ?
                AND endpoint_id = ?
                AND col_name = ?
                AND aggregate_version = ?
            """;
        Map<String, Object> map = (Map<String, Object>)event.get(PortalConstants.DATA);
        String hostId = (String)map.get("hostId");
        String attributeId = (String)map.get("attributeId");
        String endpointId = (String)map.get("endpointId");
        String colName = (String)map.get("colName");
        long oldAggregateVersion = SqlUtil.getOldAggregateVersion(event);
        try (PreparedStatement statement = conn.prepareStatement(deleteGroup)) {
            statement.setObject(1, UUID.fromString(hostId));
            statement.setString(2, attributeId);
            statement.setObject(3, UUID.fromString(endpointId));
            statement.setString(4, colName);
            statement.setLong(5, oldAggregateVersion);

            int count = statement.executeUpdate();
            if (count == 0) {
                if (queryAttributeRowFilterExists(conn, hostId, attributeId, endpointId, colName)) {
                    throw new ConcurrencyException(String.format("Optimistic concurrency conflict during deleteAttributeRowFilter for hostId %s attributeId %s endpointId %s colName %s aggregateVersion %d but found a different version or already updated.", hostId, attributeId, endpointId, colName, oldAggregateVersion));
                } else {
                    throw new SQLException(String.format("No record found during deleteAttributeRowFilter for hostId %s attributeId %s endpointId %s colName %s. It might have been already deleted.", hostId, attributeId, endpointId, colName));
                }
            }
        } catch (SQLException e) {
            logger.error("SQLException during deleteAttributeRowFilter for hostId {} attributeId {} endpointId {} colName {} aggregateVersion {}: {}", hostId, attributeId, endpointId, colName, oldAggregateVersion, e.getMessage(), e);
            throw e;
        } catch (Exception e) {
            logger.error("Exception during deleteAttributeRowFilter for hostId {} attributeId {} endpointId {} colName {} aggregateVersion {}: {}", hostId, attributeId, endpointId, colName, oldAggregateVersion, e.getMessage(), e);
            throw e;
        }
    }

    @Override
    public Result<String> queryAttributeColFilter(int offset, int limit, String filtersJson, String globalFilter, String sortingJson, String hostId) {
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

        List<Map<String, Object>> filters = parseJsonList(filtersJson);
        List<Map<String, Object>> sorting = parseJsonList(sortingJson);

        String s =
            """
            SELECT COUNT(*) OVER () AS total,
            a.host_id, a.attribute_id, a.attribute_type, cf.attribute_value,
            av.api_version_id, av.api_id, av.api_version, ae.endpoint_id, ae.endpoint, cf.columns, cf.aggregate_version,
            cf.update_user, cf.update_ts
            FROM attribute_col_filter_t cf
            JOIN attribute_t a ON a.attribute_id = cf.attribute_id
            JOIN api_endpoint_t ae ON cf.host_id = ae.host_id AND cf.endpoint_id = ae.endpoint_id
            JOIN api_version_t av ON ae.host_id = av.host_id AND ae.api_version_id = av.api_version_id
            WHERE cf.host_id = ?
            """;

        List<Object> parameters = new ArrayList<>();
        parameters.add(UUID.fromString(hostId));

        String[] searchColumns = {"a.attribute_id", "cf.attribute_value", "ae.endpoint", "cf.columns"};
        String sqlBuilder = s + dynamicFilter(Arrays.asList("a.host_id", "av.api_version_id", "ae.endpoint_id"), filters, columnMap, parameters) +
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
    public void createAttributeColFilter(Connection conn, Map<String, Object> event) throws SQLException, Exception {
        final String insertGroup =
                """
                    INSERT INTO attribute_col_filter_t (
                        host_id,
                        attribute_id,
                        attribute_value,
                        endpoint_id,
                        columns,
                        update_user,
                        update_ts,
                        aggregate_version
                    ) VALUES (?, ?, ?, ?, ?,  ?, ?, ?)
                """;

        Map<String, Object> map = (Map<String, Object>)event.get(PortalConstants.DATA);
        String hostId = (String)map.get("hostId");
        String attributeId = (String)map.get("attributeId");
        String endpointId = (String)map.get("endpointId");
        long newAggregateVersion = SqlUtil.getNewAggregateVersion(event);
        try (PreparedStatement statement = conn.prepareStatement(insertGroup)) {
            statement.setObject(1, UUID.fromString(hostId));
            statement.setString(2, attributeId);
            statement.setString(3, (String)map.get("attributeValue"));
            statement.setObject(4, UUID.fromString(endpointId));
            statement.setString(5, (String)map.get("columns"));
            statement.setString(6, (String)event.get(Constants.USER));
            statement.setObject(7, OffsetDateTime.parse((String)event.get(CloudEventV1.TIME)));
            statement.setLong(8, newAggregateVersion);

            int count = statement.executeUpdate();
            if (count == 0) {
                throw new SQLException(String.format("Failed during createAttributeColFilter for hostId %s attributeId %s endpointId %s with aggregate version %d. It might already exist.", hostId, attributeId, endpointId, newAggregateVersion));
            }
        } catch (SQLException e) {
            logger.error("SQLException during createAttributeColFilter for hostId {} attributeId {} endpointId {} aggregateVersion {}: {}", hostId, attributeId, endpointId, newAggregateVersion, e.getMessage(), e);
            throw e;
        } catch (Exception e) {
            logger.error("Exception during createAttributeColFilter for hostId {} attributeId {} endpointId {} aggregateVersion {}: {}", hostId, attributeId, endpointId, newAggregateVersion, e.getMessage(), e);
            throw e;
        }
    }

    private boolean queryAttributeColFilterExists(Connection conn, String hostId, String attributeId, String endpointId) throws SQLException {
        final String sql =
                """
                SELECT COUNT(*) FROM attribute_col_filter_t WHERE host_id = ? AND attribute_id = ? AND endpoint_id = ?
                """;
        try (PreparedStatement pst = conn.prepareStatement(sql)) {
            pst.setObject(1, UUID.fromString(hostId));
            pst.setString(2, attributeId);
            pst.setObject(3, UUID.fromString(endpointId));
            try (ResultSet rs = pst.executeQuery()) {
                return rs.next() && rs.getInt(1) > 0;
            }
        }
    }

    @Override
    public void updateAttributeColFilter(Connection conn, Map<String, Object> event) throws SQLException, Exception {
        final String updateGroup =
                """
                    UPDATE attribute_col_filter_t
                    SET
                        attribute_value = ?,
                        columns = ?,
                        update_user = ?,
                        update_ts = ?
                        aggregate_version = ?
                    WHERE
                        host_id = ?
                        AND attribute_id = ?
                        AND endpoint_id = ?
                        AND aggregate_version = ?
                """;

        Map<String, Object> map = (Map<String, Object>)event.get(PortalConstants.DATA);
        String hostId = (String)map.get("hostId");
        String attributeId = (String)map.get("attributeId");
        String endpointId = (String)map.get("endpointId");
        long oldAggregateVersion = SqlUtil.getOldAggregateVersion(event);
        long newAggregateVersion = SqlUtil.getNewAggregateVersion(event);
        try (PreparedStatement statement = conn.prepareStatement(updateGroup)) {
            statement.setString(1, (String)map.get("attributeValue"));
            statement.setString(2, (String)map.get("columns"));
            statement.setString(3, (String)event.get(Constants.USER));
            statement.setObject(4, OffsetDateTime.parse((String)event.get(CloudEventV1.TIME)));
            statement.setLong(5, newAggregateVersion);
            statement.setObject(6, UUID.fromString(hostId));
            statement.setString(7, attributeId);
            statement.setObject(8, UUID.fromString(endpointId));
            statement.setLong(9, oldAggregateVersion);


            int count = statement.executeUpdate();
            if (count == 0) {
                if (queryAttributeColFilterExists(conn, hostId, attributeId, endpointId)) {
                    throw new ConcurrencyException(String.format("Optimistic concurrency conflict during updateAttributeColFilter for hostId %s attributeId %s endpointId %s. Expected version %d but found a different version %d.", hostId, attributeId, endpointId, oldAggregateVersion, newAggregateVersion));
                } else {
                    throw new SQLException(String.format("No record found during updateAttributeColFilter for hostId %s attributeId %s endpointId %s.", hostId, attributeId, endpointId));
                }
            }
        } catch (SQLException e) {
            logger.error("SQLException during updateAttributeColFilter for hostId {} attributeId {} endpointId {} (old: {}) -> (new: {}): {}", hostId, attributeId, endpointId, oldAggregateVersion, newAggregateVersion, e.getMessage(), e);
            throw e;
        } catch (Exception e) {
            logger.error("Exception during updateAttributeColFilter for hostId {} attributeId {} endpointId {} (old: {}) -> (new: {}): {}", hostId, attributeId, endpointId, oldAggregateVersion, newAggregateVersion, e.getMessage(), e);
            throw e;
        }
    }

    @Override
    public void deleteAttributeColFilter(Connection conn, Map<String, Object> event) throws SQLException, Exception {
        final String deleteGroup =
                """
                    DELETE FROM attribute_col_filter_t
                    WHERE host_id = ?
                    AND attribute_id = ?
                    AND endpoint_id = ?
                    AND aggregate_version = ?
                """;
        Map<String, Object> map = (Map<String, Object>)event.get(PortalConstants.DATA);
        String hostId = (String)map.get("hostId");
        String attributeId = (String)map.get("attributeId");
        String endpointId = (String)map.get("endpointId");
        long oldAggregateVersion = SqlUtil.getOldAggregateVersion(event);
        try (PreparedStatement statement = conn.prepareStatement(deleteGroup)) {
            statement.setObject(1, UUID.fromString(hostId));
            statement.setString(2, attributeId);
            statement.setObject(3, UUID.fromString(endpointId));
            statement.setLong(4, oldAggregateVersion);

            int count = statement.executeUpdate();
            if (count == 0) {
                if (queryAttributeColFilterExists(conn, hostId, attributeId, endpointId)) {
                    throw new ConcurrencyException(String.format("Optimistic concurrency conflict during deleteAttributeColFilter for hostId %s attributeId %s endpointId %s aggregateVersion %d but found a different version or already updated.", hostId, attributeId, endpointId, oldAggregateVersion));
                } else {
                    throw new SQLException(String.format("No record found during deleteAttributeColFilter for hostId %s attributeId %s endpointId %s. It might have been already deleted.", hostId, attributeId, endpointId));
                }
            }
        } catch (SQLException e) {
            logger.error("SQLException during deleteAttributeColFilter for hostId {} attributeId {} endpointId {} aggregateVersion {}: {}", hostId, attributeId, endpointId, oldAggregateVersion, e.getMessage(), e);
            throw e;
        } catch (Exception e) {
            logger.error("Exception during deleteAttributeColFilter for hostId {} attributeId {} endpointId {} aggregateVersion {}: {}", hostId, attributeId, endpointId, oldAggregateVersion, e.getMessage(), e);
            throw e;
        }
    }
}
