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
                "VALUES (?, ?, ?, ?, ?, ?)";

        Map<String, Object> map = SqlUtil.extractEventData(event);
        String roleId = (String)map.get("roleId");
        long newAggregateVersion = SqlUtil.getNewAggregateVersion(event);
        try (PreparedStatement statement = conn.prepareStatement(insertRole)) {
            statement.setObject(1, UUID.fromString((String)event.get(Constants.HOST)));
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
                throw new SQLException("Failed to insert role " + roleId + " with aggregate version " + newAggregateVersion + " - possibly already exists.");
            }
        } catch (SQLException e) {
            logger.error("SQLException during createRole for id {} version {}: {}", roleId, newAggregateVersion, e.getMessage(), e);
            throw e;
        } catch (Exception e) {
            logger.error("Exception during createRole for id {} version {}: {}", roleId, newAggregateVersion, e.getMessage(), e);
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
            statement.setObject(5, UUID.fromString((String)event.get(Constants.HOST)));
            statement.setString(6, roleId);
            statement.setLong(7, oldAggregateVersion);

            int count = statement.executeUpdate();
            if (count == 0) {
                if (queryRoleExists(conn, (String)event.get(Constants.HOST), roleId)) {
                    throw new ConcurrencyException("Optimistic concurrency conflict for role " + roleId + ". Expected version " + oldAggregateVersion + " but found a different version " + newAggregateVersion + ".");
                } else {
                    throw new SQLException("No record found to update for role " + roleId + ".");
                }
            }
        } catch (SQLException e) {
            logger.error("SQLException during updateRole for id {} (old: {}) -> (new: {}): {}", roleId, oldAggregateVersion, newAggregateVersion, e.getMessage(), e);
            throw e;
        } catch (Exception e) {
            logger.error("Exception during updateRole for id {} (old: {}) -> (new: {}): {}", roleId, oldAggregateVersion, newAggregateVersion, e.getMessage(), e);
            throw e;
        }
    }


    @Override
    public void deleteRole(Connection conn, Map<String, Object> event) throws SQLException, Exception {
        final String deleteRole = "DELETE from role_t WHERE host_id = ? AND role_id = ? AND aggregate_version = ?";
        Map<String, Object> map = SqlUtil.extractEventData(event);
        String roleId = (String)map.get("roleId");
        long oldAggregateVersion = SqlUtil.getOldAggregateVersion(event);
        try (PreparedStatement statement = conn.prepareStatement(deleteRole)) {
            statement.setObject(1, UUID.fromString((String)event.get(Constants.HOST)));
            statement.setString(2, roleId);
            statement.setLong(3, oldAggregateVersion);
            int count = statement.executeUpdate();
            if (count == 0) {
                // If 0 rows were affected, it means either:
                // 1. The record did not exist (it was already deleted, or never existed).
                // 2. The record existed, but its aggregate_version did not match the expectedVersion (concurrency conflict).

                // To differentiate, we can perform a quick existence check:
                if (queryRoleExists(conn, (String)event.get(Constants.HOST), roleId)) {
                    // Record exists but version didn't match -> CONCURRENCY CONFLICT
                    logger.warn("Optimistic concurrency conflict during deleteRole for id {}. aggregate version {} but found a different version.", roleId, oldAggregateVersion);
                    throw new ConcurrencyException("Optimistic concurrency conflict for role " + roleId + ". aggregate version " + oldAggregateVersion + " but found a different version or already updated.");
                } else {
                    // Record does not exist -> Already deleted or never existed. This is often an acceptable state.
                    logger.warn("No record found to delete for role {} with aggregate version {}. It might have been already deleted.", roleId, oldAggregateVersion);
                    // You might choose to throw a more specific "NotFoundException" here if necessary for logic.
                    throw new SQLException("No record found to delete for role " + roleId + ". It might have been already deleted.");
                }
            }
        } catch (SQLException e) {
            logger.error("SQLException during deleteRole for id {} aggregateVersion {}: {}", roleId, oldAggregateVersion, e.getMessage(), e);
            throw e;
        } catch (Exception e) {
            logger.error("Exception during deleteRole for id {} aggregateVersion {}: {}", roleId, oldAggregateVersion, e.getMessage(), e);
            throw e;
        }
    }

    @Override
    public Result<String> queryRole(int offset, int limit, String hostId, String roleId, String roleDesc) {
        Result<String> result;
        StringBuilder sqlBuilder = new StringBuilder();
        sqlBuilder.append("SELECT COUNT(*) OVER () AS total, host_id, role_id, role_desc, update_user, update_ts, aggregate_version " +
                "FROM role_t " +
                "WHERE host_id = ?\n");


        List<Object> parameters = new ArrayList<>();
        parameters.add(UUID.fromString(hostId));


        StringBuilder whereClause = new StringBuilder();

        SqlUtil.addCondition(whereClause, parameters, "role_id", roleId);
        SqlUtil.addCondition(whereClause, parameters, "role_desc", roleDesc);


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
    public Result<String> queryRolePermission(int offset, int limit, String hostId, String roleId, String apiVersionId, String apiId, String apiVersion, String endpointId, String endpoint) {
        Result<String> result;
        String s =
                """
                SELECT COUNT(*) OVER () AS total,
                r.host_id, r.role_id, av.api_version_id, av.api_id, av.api_version, rp.endpoint_id, ae.endpoint, rp.aggregate_version
                FROM role_permission_t rp
                JOIN role_t r ON r.role_id = rp.role_id
                JOIN api_endpoint_t ae ON rp.host_id = ae.host_id AND rp.endpoint_id = ae.endpoint_id
                JOIN api_version_t av ON ae.host_id = av.host_id AND ae.api_version_id = av.api_version_id
                AND r.host_id = ?
                """;
        StringBuilder sqlBuilder = new StringBuilder(s);

        List<Object> parameters = new ArrayList<>();
        parameters.add(UUID.fromString(hostId));

        StringBuilder whereClause = new StringBuilder();

        SqlUtil.addCondition(whereClause, parameters, "r.role_id", roleId);
        SqlUtil.addCondition(whereClause, parameters, "av.api_version_id", apiVersionId);
        SqlUtil.addCondition(whereClause, parameters, "av.api_id", apiId);
        SqlUtil.addCondition(whereClause, parameters, "av.api_version", apiVersion);
        SqlUtil.addCondition(whereClause, parameters, "rp.endpoint_id", endpointId);
        SqlUtil.addCondition(whereClause, parameters, "ae.endpoint", endpoint);

        if (!whereClause.isEmpty()) {
            sqlBuilder.append("AND ").append(whereClause);
        }

        sqlBuilder.append(" ORDER BY r.role_id, av.api_id, av.api_version, ae.endpoint\n" +
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
                    map.put("apiVersionId", resultSet.getString("api_version_id"));
                    map.put("apiId", resultSet.getString("api_id"));
                    map.put("apiVersion", resultSet.getString("api_version"));
                    map.put("endpointId", resultSet.getObject("endpoint_id", UUID.class));
                    map.put("endpoint", resultSet.getString("endpoint"));
                    map.put("aggregateVersion", resultSet.getLong("aggregate_version"));
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
        String s =
                """
                SELECT COUNT(*) OVER () AS total,
                r.host_id, r.role_id, r.start_ts, r.end_ts,
                u.user_id, u.email, u.user_type,
                CASE
                    WHEN u.user_type = 'C' THEN c.customer_id
                    WHEN u.user_type = 'E' THEN e.employee_id
                    ELSE NULL -- Handle other cases if needed
                END AS entity_id,
                e.manager_id, u.first_name, u.last_name, r.aggregate_version
                FROM user_t u
                LEFT JOIN
                    customer_t c ON u.user_id = c.user_id AND u.user_type = 'C'
                LEFT JOIN
                    employee_t e ON u.user_id = e.user_id AND u.user_type = 'E'
                INNER JOIN
                    role_user_t r ON r.user_id = u.user_id
                AND r.host_id = ?
                """;

        StringBuilder sqlBuilder = new StringBuilder(s);

        List<Object> parameters = new ArrayList<>();
        parameters.add(UUID.fromString(hostId));


        StringBuilder whereClause = new StringBuilder();

        SqlUtil.addCondition(whereClause, parameters, "r.role_id", roleId);
        SqlUtil.addCondition(whereClause, parameters, "u.user_id", userId != null ? UUID.fromString(userId) : null);
        SqlUtil.addCondition(whereClause, parameters, "entity_id", entityId);
        SqlUtil.addCondition(whereClause, parameters, "u.email", email);
        SqlUtil.addCondition(whereClause, parameters, "u.first_name", firstName);
        SqlUtil.addCondition(whereClause, parameters, "u.last_name", lastName);
        SqlUtil.addCondition(whereClause, parameters, "u.user_type", userType);

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
                    map.put("startTs", resultSet.getObject("start_ts") != null ? resultSet.getObject("start_ts", OffsetDateTime.class) : null);
                    map.put("endTs", resultSet.getObject("end_ts") != null ? resultSet.getObject("end_ts", OffsetDateTime.class) : null);
                    map.put("userId", resultSet.getObject("user_id", UUID.class));
                    map.put("entityId", resultSet.getString("entity_id"));
                    map.put("email", resultSet.getString("email"));
                    map.put("firstName", resultSet.getString("first_name"));
                    map.put("lastName", resultSet.getString("last_name"));
                    map.put("userType", resultSet.getString("user_type"));
                    map.put("managerId", resultSet.getObject("manager_id", UUID.class));
                    map.put("aggregateVersion", resultSet.getLong("aggregate_version"));
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
                    VALUES (
                        ?,
                        ?,
                        (SELECT e.endpoint_id
                         FROM api_endpoint_t e
                         JOIN api_version_t v ON e.host_id = v.host_id
                                             AND e.api_version_id = v.api_version_id
                         WHERE e.host_id = ?
                           AND v.api_id = ?
                           AND v.api_version = ?
                           AND e.endpoint = ?
                        ),
                        ?,
                        ?,
                        ?
                    )
                """;

        Map<String, Object> map = SqlUtil.extractEventData(event);
        String roleId = (String)map.get("roleId");
        long newAggregateVersion = SqlUtil.getNewAggregateVersion(event);
        try (PreparedStatement statement = conn.prepareStatement(insertRole)) {
            statement.setObject(1, UUID.fromString((String)event.get(Constants.HOST)));
            statement.setString(2, roleId);
            statement.setObject(3, UUID.fromString((String)event.get(Constants.HOST)));
            statement.setString(4, (String)map.get("apiId"));
            statement.setString(5, (String)map.get("apiVersion"));
            statement.setString(6, (String)map.get("endpoint"));
            statement.setString(7, (String)event.get(Constants.USER));
            statement.setObject(8, OffsetDateTime.parse((String)event.get(CloudEventV1.TIME)));
            statement.setLong(9, newAggregateVersion);

            int count = statement.executeUpdate();
            if (count == 0) {
                throw new SQLException("Failed to insert role permission " + roleId + " with aggregate version " + newAggregateVersion +  " for endpoint " + map.get("endpoint") + ". It might already exist.");
            }
        } catch (SQLException e) {
            logger.error("SQLException during createRolePermission for id {} aggregateVersion {}: {}", roleId, newAggregateVersion, e.getMessage(), e);
            throw e;
        } catch (Exception e) {
            logger.error("Exception during createRolePermission for id {} aggregateVersion {}: {}", roleId, newAggregateVersion, e.getMessage(), e);
            throw e;
        }
    }

    // Helper to check if a role exists (to differentiate 'not found' from 'conflict')
    private boolean queryRolePermissionExists(Connection conn, String hostId, String roleId, String apiId, String apiVersion, String endpoint) throws SQLException {
        final String sql =
                """
                    SELECT COUNT(*) FROM role_permission_t rp
                    WHERE rp.host_id = ?
                    AND rp.role_id = ?
                    AND rp.endpoint_id IN (
                        SELECT e.endpoint_id
                        FROM api_endpoint_t e
                        JOIN api_version_t v ON e.host_id = v.host_id
                                            AND e.api_version_id = v.api_version_id
                        WHERE e.host_id = ?
                        AND v.api_id = ?
                        AND v.api_version = ?
                        AND e.endpoint = ?
                    )
                """;
        try (PreparedStatement statement = conn.prepareStatement(sql)) {
            statement.setObject(1, UUID.fromString(hostId));
            statement.setString(2, roleId);
            statement.setObject(3, UUID.fromString(hostId));
            statement.setString(4, apiId);
            statement.setString(5, apiVersion);
            statement.setString(6, endpoint);

            try (ResultSet rs = statement.executeQuery()) {
                return rs.next() && rs.getInt(1) > 0;
            }
        }
    }


    @Override
    public void deleteRolePermission(Connection conn, Map<String, Object> event) throws SQLException, Exception {
        final String deleteRole =
                """
                    DELETE FROM role_permission_t rp
                    WHERE rp.host_id = ?
                      AND rp.role_id = ?
                      AND rp.aggregate_version = ?
                      AND rp.endpoint_id IN (
                        SELECT e.endpoint_id
                        FROM api_endpoint_t e
                        JOIN api_version_t v ON e.host_id = v.host_id
                                            AND e.api_version_id = v.api_version_id
                        WHERE e.host_id = ?
                          AND v.api_id = ?
                          AND v.api_version = ?
                          AND e.endpoint = ?
                      )
                """;
        Result<String> result;
        Map<String, Object> map = SqlUtil.extractEventData(event);
        String roleId = (String)map.get("roleId");
        long oldAggregateVersion = SqlUtil.getOldAggregateVersion(event);
        try (PreparedStatement statement = conn.prepareStatement(deleteRole)) {
            statement.setObject(1, UUID.fromString((String)event.get(Constants.HOST)));
            statement.setString(2, roleId);
            statement.setLong(3, oldAggregateVersion);
            statement.setObject(4, UUID.fromString((String)event.get(Constants.HOST)));
            statement.setString(5, (String)map.get("apiId"));
            statement.setString(6, (String)map.get("apiVersion"));
            statement.setString(7, (String)map.get("endpoint"));

            int count = statement.executeUpdate();
            if (count == 0) {
                if (queryRolePermissionExists(conn, (String)event.get(Constants.HOST), roleId, (String)map.get("apiId"), (String)map.get("apiVersion"), (String)map.get("endpoint"))) {
                    // Record exists but version didn't match -> CONCURRENCY CONFLICT
                    logger.warn("Optimistic concurrency conflict during deleteRolePermission for id {}. aggregate version {} but found a different version.", roleId, oldAggregateVersion);
                    throw new ConcurrencyException("Optimistic concurrency conflict during deleteRolePermission for role " + roleId + ". aggregate version " + oldAggregateVersion + " but found a different version or already updated.");
                } else {
                    // Record does not exist -> Already deleted or never existed. This is often an acceptable state.
                    logger.warn("No record found during deleteRolePermission for role {} with aggregate version {}. It might have been already deleted.", roleId, oldAggregateVersion);
                    // You might choose to throw a more specific "NotFoundException" here if necessary for logic.
                    throw new SQLException("No record found during deleteRolePermission for role " + roleId + ". It might have been already deleted.");
                }
            }
        } catch (SQLException e) {
            logger.error("SQLException during deleteRolePermission for id {} aggregateVersion {}: {}", roleId, oldAggregateVersion, e.getMessage(), e);
            throw e;
        } catch (Exception e) {
            logger.error("Exception during deleteRolePermission for id {} aggregateVersion {}: {}", roleId, oldAggregateVersion, e.getMessage(), e);
            throw e;
        }
    }

    @Override
    public void createRoleUser(Connection conn, Map<String, Object> event) throws SQLException, Exception {
        final String insertRole = "INSERT INTO role_user_t (host_id, role_id, user_id, start_ts, " +
                "end_ts, update_user, update_ts, aggregate_version) " +
                "VALUES (?, ?, ?, ?, ?,  ?, ?, ?)";

        Map<String, Object> map = SqlUtil.extractEventData(event);
        String roleId = (String)map.get("roleId");
        String userId = (String)event.get(Constants.USER);
        long newAggregateVersion = SqlUtil.getNewAggregateVersion(event);
        try (PreparedStatement statement = conn.prepareStatement(insertRole)) {
            statement.setObject(1, UUID.fromString((String)event.get(Constants.HOST)));
            statement.setString(2, roleId);
            statement.setObject(3, UUID.fromString((String)event.get(Constants.USER)));

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
            statement.setString(6, userId);
            statement.setObject(7,  OffsetDateTime.parse((String)event.get(CloudEventV1.TIME)));
            statement.setLong(8, newAggregateVersion);

            int count = statement.executeUpdate();
            if (count == 0) {
                throw new SQLException("Failed to insert role " + roleId + " for user " + userId + " with aggregate version " + newAggregateVersion + ".");
            }
        } catch (SQLException e) {
            logger.error("SQLException during createRoleUser for roleId {} userId {} and aggregateVersion {}: {}", roleId, userId, newAggregateVersion, e.getMessage(), e);
            throw e;
        } catch (Exception e) {
            logger.error("Exception during createRoleUser for roleId {} userId {} and aggregateVersion {}: {}", roleId, userId, newAggregateVersion, e.getMessage(), e);
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
        String roleId = (String)map.get("roleId");
        String userId = (String)event.get(Constants.USER);
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
            statement.setObject(6, UUID.fromString((String)event.get(Constants.HOST)));
            statement.setString(7, roleId);
            statement.setObject(8, UUID.fromString(userId));
            statement.setLong(9, oldAggregateVersion);

            int count = statement.executeUpdate();
            if (count == 0) {
                if (queryRoleUserExists(conn, (String)event.get(Constants.HOST), roleId, userId)) {
                    throw new ConcurrencyException("Optimistic concurrency conflict for role " + roleId + " user " + userId + ". Expected version " + oldAggregateVersion + " but found a different version " + newAggregateVersion + ".");
                } else {
                    throw new SQLException("No record found to update for role " + roleId + " user " + userId + ".");
                }
            }
        } catch (SQLException e) {
            logger.error("SQLException during updateRoleUser for roleId {} userId {} (old: {}) -> (new: {}): {}", roleId, userId, oldAggregateVersion, newAggregateVersion, e.getMessage(), e);
            throw e;
        } catch (Exception e) {
            logger.error("Exception during updateRoleUser for roleId {} userId {} (old: {}) -> (new: {}): {}", roleId, userId, oldAggregateVersion, newAggregateVersion, e.getMessage(), e);
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
        String roleId = (String)map.get("roleId");
        String userId = (String)event.get(Constants.USER);
        long oldAggregateVersion = SqlUtil.getOldAggregateVersion(event);
        try (PreparedStatement statement = conn.prepareStatement(deleteRole)) {
            statement.setObject(1, UUID.fromString((String)event.get(Constants.HOST)));
            statement.setString(2, roleId);
            statement.setObject(3, UUID.fromString(userId));
            statement.setLong(4, oldAggregateVersion);

            int count = statement.executeUpdate();
            if (count == 0) {
                if (queryRoleUserExists(conn, (String)event.get(Constants.HOST), roleId, userId)) {
                    logger.warn("Optimistic concurrency conflict during deleteRoleUser for roleId {} userId {} aggregateVersion {} but found a different version.", roleId, userId, oldAggregateVersion);
                    throw new ConcurrencyException("Optimistic concurrency conflict during deleteRoleUser for roleId " + roleId + " userId " + userId + " aggregateVersion " + oldAggregateVersion + " but found a different version or already updated.");
                } else {
                    // Record does not exist -> Already deleted or never existed. This is often an acceptable state.
                    logger.warn("No record found during deleteRoleUser for roleId {} userId {} with aggregate version {}. It might have been already deleted.", roleId, userId, oldAggregateVersion);
                    throw new SQLException("No record found during deleteRoleUser for roleId " + roleId + " userId " + userId + ". It might have been already deleted.");
                }
            }
        } catch (SQLException e) {
            logger.error("SQLException during deleteRoleUser for roleId {} userId {} aggregateVersion {}: {}", roleId, userId, oldAggregateVersion,  e.getMessage(), e);
            throw e;
        } catch (Exception e) {
            logger.error("Exception during deleteRoleUser for roleId {} userId {} aggregateVersion {}: {}", roleId, userId, oldAggregateVersion, e.getMessage(), e);
            throw e;
        }
    }

    @Override
    public Result<String> queryRoleRowFilter(int offset, int limit, String hostId, String roleId, String apiVersionId, String apiId, String apiVersion, String endpointId, String endpoint) {
        Result<String> result;
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

        StringBuilder sqlBuilder = new StringBuilder(s);

        List<Object> parameters = new ArrayList<>();
        parameters.add(UUID.fromString(hostId));

        StringBuilder whereClause = new StringBuilder();

        SqlUtil.addCondition(whereClause, parameters, "p.role_id", roleId);
        SqlUtil.addCondition(whereClause, parameters, "av.api_version_id", apiVersionId);
        SqlUtil.addCondition(whereClause, parameters, "av.api_id", apiId);
        SqlUtil.addCondition(whereClause, parameters, "av.api_version", apiVersion);
        SqlUtil.addCondition(whereClause, parameters, "p.endpoint_id", endpointId);
        SqlUtil.addCondition(whereClause, parameters, "ae.endpoint", endpoint);

        if (!whereClause.isEmpty()) {
            sqlBuilder.append("AND ").append(whereClause);
        }

        sqlBuilder.append(" ORDER BY p.role_id, av.api_id, av.api_version, ae.endpoint, p.col_name\n" +
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
        String roleId = (String)map.get("roleId");
        String endpointId = (String)map.get("endpointId");
        String colName = (String)map.get("colName");
        long oldAggregateVersion = SqlUtil.getOldAggregateVersion(event);
        try (PreparedStatement statement = conn.prepareStatement(deleteRole)) {
            statement.setObject(1, UUID.fromString((String)event.get(Constants.HOST)));
            statement.setString(2, roleId);
            statement.setObject(3, UUID.fromString(endpointId));
            statement.setString(4, colName);
            statement.setLong(5, oldAggregateVersion);

            int count = statement.executeUpdate();
            if (count == 0) {
                if (queryRoleRowFilterExists(conn, (String)event.get(Constants.HOST), roleId, endpointId, colName)) {
                    String s = String.format("Optimistic concurrency conflict during deleteRoleRowFilter for roleId %s endpointId %s colName %s aggregateVersion %d but found a different version.", roleId, endpointId, colName, oldAggregateVersion);
                    logger.warn(s);
                    throw new ConcurrencyException(s);
                } else {
                    String s = String.format("No record found during deleteRoleRowFilter for roleId %s endpointId %s colName %s with aggregateVersion %d. It might have been already deleted.", roleId, endpointId, colName, oldAggregateVersion);
                    logger.warn(s);
                    throw new SQLException(s);
                }
            }
        } catch (SQLException e) {
            logger.error("SQLException during deleteRoleRowFilter for roleId {} endpointId {} colName {} aggregateVersion {}: {}", roleId, endpointId, colName, oldAggregateVersion, e.getMessage(), e);
            throw e;
        } catch (Exception e) {
            logger.error("Exception during deleteRoleRowFilter for roleId {} endpointId {} colName {} aggregateVersion {}: {}", roleId, endpointId, colName, oldAggregateVersion, e.getMessage(), e);
            throw e;
        }
    }

    @Override
    public void createRoleRowFilter(Connection conn, Map<String, Object> event) throws SQLException, Exception {
        final String insertRole = """
                INSERT INTO role_row_filter_t (
                    host_id,
                    role_id,
                    endpoint_id,  -- Now using the resolved endpoint_id
                    col_name,
                    operator,
                    col_value,
                    update_user,
                    update_ts,
                    aggregate_version
                )
                SELECT
                    ?,              -- host_id parameter
                    ?,              -- role_id parameter
                    e.endpoint_id,  -- Resolved from the join
                    ?,              -- col_name parameter
                    ?,              -- operator parameter
                    ?,              -- col_value parameter
                    ?,              -- update_user parameter
                    ?,              -- update_ts parameter (or use DEFAULT for CURRENT_TIMESTAMP)
                    ?               -- aggregate_version parameter
                FROM
                    api_endpoint_t e
                JOIN
                    api_version_t v ON e.host_id = v.host_id
                                   AND e.api_version_id = v.api_version_id
                WHERE
                    e.host_id = ?                  -- Same as the first host_id parameter
                    AND v.api_id = ?               -- api_id parameter
                    AND v.api_version = ?          -- api_version parameter
                    AND e.endpoint = ?;            -- endpoint parameter
                """;

        Map<String, Object> map = SqlUtil.extractEventData(event);
        String roleId = (String)map.get("roleId");
        String endpoint = (String)map.get("endpoint");
        long newAggregateVersion = SqlUtil.getNewAggregateVersion(event);
        try (PreparedStatement statement = conn.prepareStatement(insertRole)) {
            statement.setObject(1, UUID.fromString((String)event.get(Constants.HOST)));
            statement.setString(2, roleId);
            statement.setString(3, (String)map.get("colName"));
            statement.setString(4, (String)map.get("operator"));
            statement.setString(5, (String)map.get("colValue"));
            statement.setString(6, (String)event.get(Constants.USER));
            statement.setObject(7, OffsetDateTime.parse((String)event.get(CloudEventV1.TIME)));
            statement.setLong(8, newAggregateVersion);
            statement.setObject(9, UUID.fromString((String)event.get(Constants.HOST)));
            statement.setString(10, (String)map.get("apiId"));
            statement.setString(11, (String)map.get("apiVersion"));
            statement.setString(12, endpoint);

            int count = statement.executeUpdate();
            if (count == 0) {
                String s = String.format("Failed to insert roleId %s row filter for endpoint %s with aggregate version %d. It might already exist.", roleId, endpoint, newAggregateVersion);
                throw new SQLException(s);
            }
        } catch (SQLException e) {
            logger.error("SQLException during createRoleRowFilter for roleId {} endpoint {} aggregateVersion {}: {}", roleId, endpoint, newAggregateVersion, e.getMessage(), e);
            throw e;
        } catch (Exception e) {
            logger.error("Exception during createRoleRowFilter for roleId {} endpoint {} aggregateVersion {}: {}", roleId, endpoint, newAggregateVersion, e.getMessage(), e);
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
                            update_ts = ?
                            aggregate_version = ?
                        WHERE
                            host_id = ?
                            AND role_id = ?
                            AND col_name = ?
                            AND aggregate_version = ?
                            AND endpoint_id IN (
                                SELECT e.endpoint_id
                                FROM api_endpoint_t e
                                JOIN api_version_t v ON e.host_id = v.host_id
                                                    AND e.api_version_id = v.api_version_id
                                WHERE e.host_id = ?
                                  AND v.api_id = ?
                                  AND v.api_version = ?
                                  AND e.endpoint = ?
                            )
                """;

        Map<String, Object> map = (Map<String, Object>)event.get(PortalConstants.DATA);
        String roleId = (String)map.get("roleId");
        long oldAggregateVersion = SqlUtil.getOldAggregateVersion(event);
        long newAggregateVersion = SqlUtil.getNewAggregateVersion(event);

        try (PreparedStatement statement = conn.prepareStatement(updateRole)) {
            statement.setString(1, (String)map.get("operator"));
            statement.setString(2, (String)map.get("colValue"));
            statement.setString(3, (String)event.get(Constants.USER));
            statement.setObject(4, OffsetDateTime.parse((String)event.get(CloudEventV1.TIME)));
            statement.setLong(5, newAggregateVersion);
            statement.setObject(6, UUID.fromString((String)event.get(Constants.HOST)));
            statement.setString(7, roleId);
            statement.setString(8, (String)map.get("colName"));
            statement.setLong(9, oldAggregateVersion);
            statement.setObject(10, UUID.fromString((String)event.get(Constants.HOST)));
            statement.setString(11, (String)map.get("apiId"));
            statement.setString(12, (String)map.get("apiVersion"));
            statement.setString(13, (String)map.get("endpoint"));

            int count = statement.executeUpdate();
            if (count == 0) {
                throw new SQLException("no record is updated for role row filter " + roleId + " with colName " + map.get("colName") + " and aggregate version " + oldAggregateVersion + ". It might not exist or the version might have changed.");
            }
        } catch (SQLException e) {
            logger.error("SQLException during updateRoleRowFilter for id {} (old: {}) -> (new: {}): {}", roleId, oldAggregateVersion, newAggregateVersion, e.getMessage(), e);
            throw e;
        } catch (Exception e) {
            logger.error("Exception during updateRoleRowFilter for id {} (old: {}) -> (new: {}): {}", roleId, oldAggregateVersion, newAggregateVersion, e.getMessage(), e);
            throw e;
        }
    }

    @Override
    public Result<String> queryRoleColFilter(int offset, int limit, String hostId, String roleId, String apiId, String apiVersion, String endpoint) {
        Result<String> result;
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

        StringBuilder sqlBuilder = new StringBuilder(s);

        List<Object> parameters = new ArrayList<>();
        parameters.add(UUID.fromString(hostId));

        StringBuilder whereClause = new StringBuilder();

        SqlUtil.addCondition(whereClause, parameters, "r.role_id", roleId);
        SqlUtil.addCondition(whereClause, parameters, "av.api_version_id", apiId);
        SqlUtil.addCondition(whereClause, parameters, "av.api_id", apiId);
        SqlUtil.addCondition(whereClause, parameters, "av.api_version", apiVersion);
        SqlUtil.addCondition(whereClause, parameters, "ae.endpoint_id", endpoint);
        SqlUtil.addCondition(whereClause, parameters, "ae.endpoint", endpoint);


        if (!whereClause.isEmpty()) {
            sqlBuilder.append("AND ").append(whereClause);
        }

        sqlBuilder.append(" ORDER BY r.role_id, av.api_id, av.api_version, ae.endpoint\n" +
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
                        endpoint_id,  -- Using resolved endpoint_id instead of api_id/api_version/endpoint
                        columns,
                        update_user,
                        update_ts,
                        aggregate_version
                    )
                    SELECT
                        ?,              -- host_id parameter
                        ?,              -- role_id parameter
                        e.endpoint_id,  -- Resolved from the join
                        ?,              -- columns parameter
                        ?,              -- update_user parameter
                        ?,              -- update_ts parameter (or use DEFAULT for CURRENT_TIMESTAMP)
                        ?               -- aggregate_version parameter
                    FROM
                        api_endpoint_t e
                    JOIN
                        api_version_t v ON e.host_id = v.host_id
                                       AND e.api_version_id = v.api_version_id
                    WHERE
                        e.host_id = ?                  -- Same as the first host_id parameter
                        AND v.api_id = ?               -- api_id parameter
                        AND v.api_version = ?          -- api_version parameter
                        AND e.endpoint = ?;            -- endpoint parameter
                """;

        Map<String, Object> map = (Map<String, Object>)event.get(PortalConstants.DATA);
        String roleId = (String)map.get("roleId");
        String endpoint = (String)map.get("endpoint");
        long newAggregateVersion = SqlUtil.getNewAggregateVersion(event);
        try (PreparedStatement statement = conn.prepareStatement(insertRole)) {
            statement.setObject(1, UUID.fromString((String)event.get(Constants.HOST)));
            statement.setString(2, roleId);
            statement.setString(3, (String)map.get("columns"));
            statement.setString(4, (String)event.get(Constants.USER));
            statement.setObject(5, OffsetDateTime.parse((String)event.get(CloudEventV1.TIME)));
            statement.setLong(6, newAggregateVersion);
            statement.setObject(7, UUID.fromString((String)event.get(Constants.HOST)));
            statement.setString(8, (String)map.get("apiId"));
            statement.setString(9, (String)map.get("apiVersion"));
            statement.setString(10, endpoint);

            int count = statement.executeUpdate();
            if (count == 0) {
                throw new SQLException(String.format("Failed to insert roleId %s col filter for endpoint %s with aggregate version %d. It might already exist.", roleId, endpoint, newAggregateVersion));
            }
        } catch (SQLException e) {
            logger.error("SQLException during createRoleColFilter for roleId {} endpoint {} aggregateVersion {}: {}", roleId, endpoint, newAggregateVersion, e.getMessage(), e);
            throw e;
        } catch (Exception e) {
            logger.error("Exception during createRoleColFilter for roleId {} endpoint {} aggregateVersion {}: {}", roleId, endpoint, newAggregateVersion, e.getMessage(), e);
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
                    DELETE FROM role_col_filter_t rcf
                    WHERE rcf.host_id = ?
                      AND rcf.role_id = ?
                      AND rcf.endpoint_id IN (
                        SELECT e.endpoint_id
                        FROM api_endpoint_t e
                        JOIN api_version_t v ON e.host_id = v.host_id
                                            AND e.api_version_id = v.api_version_id
                        WHERE e.host_id = ?          -- Same host_id as above
                          AND v.api_id = ?           -- Your api_id parameter
                          AND v.api_version = ?      -- Your api_version parameter
                          AND e.endpoint = ?         -- Your endpoint parameter
                      );

                DELETE from role_col_filter_t WHERE host_id = ? AND role_id = ? AND api_id = ? AND api_version = ? AND endpoint = ?
                """;

        Map<String, Object> map = (Map<String, Object>)event.get(PortalConstants.DATA);
        String roleId = (String)map.get("roleId");
        try (PreparedStatement statement = conn.prepareStatement(deleteRole)) {
            statement.setObject(1, UUID.fromString((String)event.get(Constants.HOST)));
            statement.setString(2, roleId);
            statement.setObject(3, UUID.fromString((String)event.get(Constants.HOST)));
            statement.setString(4, (String)map.get("apiId"));
            statement.setString(5, (String)map.get("apiVersion"));
            statement.setString(6, (String)map.get("endpoint"));

            int count = statement.executeUpdate();
            if (count == 0) {
                throw new SQLException("no record is deleted for role col filter " + roleId);
            }
        } catch (SQLException e) {
            logger.error("SQLException during deleteRoleColFilter for id {}: {}", roleId, e.getMessage(), e);
            throw e;
        } catch (Exception e) {
            logger.error("Exception during deleteRoleColFilter for id {}: {}", roleId, e.getMessage(), e);
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
                        WHERE
                            host_id = ?
                            AND role_id = ?
                            AND endpoint_id IN (
                                SELECT e.endpoint_id
                                FROM api_endpoint_t e
                                JOIN api_version_t v ON e.host_id = v.host_id
                                                    AND e.api_version_id = v.api_version_id
                                WHERE e.host_id = ?
                                  AND v.api_id = ?
                                  AND v.api_version = ?
                                  AND e.endpoint = ?
                            )
                """;

        Map<String, Object> map = (Map<String, Object>)event.get(PortalConstants.DATA);
        String roleId = (String)map.get("roleId");
        long oldAggregateVersion = SqlUtil.getOldAggregateVersion(event);
        long newAggregateVersion = SqlUtil.getNewAggregateVersion(event);

        try (PreparedStatement statement = conn.prepareStatement(updateRole)) {
            statement.setString(1, (String)map.get("columns"));
            statement.setString(2, (String)event.get(Constants.USER));
            statement.setObject(3, OffsetDateTime.parse((String)event.get(CloudEventV1.TIME)));
            statement.setObject(4, UUID.fromString((String)event.get(Constants.HOST)));
            statement.setString(5, roleId);
            statement.setObject(6, UUID.fromString((String)event.get(Constants.HOST)));
            statement.setString(7, (String)map.get("apiId"));
            statement.setString(8, (String)map.get("apiVersion"));
            statement.setString(9, (String)map.get("endpoint"));

            int count = statement.executeUpdate();
            if (count == 0) {
                throw new SQLException("no record is updated for role col filter " + roleId);
            }
        } catch (SQLException e) {
            logger.error("SQLException during updateRoleColFilter for id {}: {}", roleId, e.getMessage(), e);
            throw e;
        } catch (Exception e) {
            logger.error("Exception during updateRoleColFilter for id {}: {}", roleId, e.getMessage(), e);
            throw e;
        }
    }

    @Override
    public void createGroup(Connection conn, Map<String, Object> event) throws SQLException, Exception {
        final String insertGroup = "INSERT INTO group_t (host_id, group_id, group_desc, update_user, update_ts) " +
                "VALUES (?, ?, ?, ?, ?)";

        Map<String, Object> map = (Map<String, Object>)event.get(PortalConstants.DATA);
        String groupId = (String)map.get("groupId");
        try (PreparedStatement statement = conn.prepareStatement(insertGroup)) {
            statement.setObject(1, UUID.fromString((String)event.get(Constants.HOST)));
            statement.setString(2, groupId);
            String groupDesc = (String)map.get("groupDesc");
            if (groupDesc != null && !groupDesc.isEmpty())
                statement.setString(3, groupDesc);
            else
                statement.setNull(3, NULL);

            statement.setString(4, (String)event.get(Constants.USER));
            statement.setObject(5, OffsetDateTime.parse((String)event.get(CloudEventV1.TIME)));

            int count = statement.executeUpdate();
            if (count == 0) {
                throw new SQLException("Failed to insert group " + groupId);
            }
        } catch (SQLException e) {
            logger.error("SQLException during createGroup for id {}: {}", groupId, e.getMessage(), e);
            throw e;
        } catch (Exception e) {
            logger.error("Exception during createGroup for id {}: {}", groupId, e.getMessage(), e);
            throw e;
        }
    }

    @Override
    public void updateGroup(Connection conn, Map<String, Object> event) throws SQLException, Exception {
        final String updateGroup = "UPDATE group_t SET group_desc = ?, update_user = ?, update_ts = ? " +
                "WHERE host_id = ? AND group_id = ?";

        Map<String, Object> map = (Map<String, Object>)event.get(PortalConstants.DATA);
        String groupId = (String)map.get("groupId");
        try (PreparedStatement statement = conn.prepareStatement(updateGroup)) {
            String groupDesc = (String)map.get("groupDesc");
            if(groupDesc != null && !groupDesc.isEmpty()) {
                statement.setString(1, groupDesc);
            } else {
                statement.setNull(1, NULL);
            }
            statement.setString(2, (String)event.get(Constants.USER));
            statement.setObject(3, OffsetDateTime.parse((String)event.get(CloudEventV1.TIME)));
            statement.setObject(4, UUID.fromString((String)event.get(Constants.HOST)));
            statement.setString(5, groupId);

            int count = statement.executeUpdate();
            if (count == 0) {
                throw new SQLException("no record is updated for group " + groupId);
            }
        } catch (SQLException e) {
            logger.error("SQLException during updateGroup for id {}: {}", groupId, e.getMessage(), e);
            throw e;
        } catch (Exception e) {
            logger.error("Exception during updateGroup for id {}: {}", groupId, e.getMessage(), e);
            throw e;
        }
    }

    @Override
    public void deleteGroup(Connection conn, Map<String, Object> event) throws SQLException, Exception {
        final String deleteGroup = "DELETE from group_t WHERE host_id = ? AND group_id = ?";
        Map<String, Object> map = (Map<String, Object>)event.get(PortalConstants.DATA);
        String groupId = (String)map.get("groupId");
        try (PreparedStatement statement = conn.prepareStatement(deleteGroup)) {
            statement.setObject(1, UUID.fromString((String)event.get(Constants.HOST)));
            statement.setString(2, groupId);
            int count = statement.executeUpdate();
            if (count == 0) {
                throw new SQLException("no record is deleted for group " + groupId);
            }
        } catch (SQLException e) {
            logger.error("SQLException during deleteGroup for id {}: {}", groupId, e.getMessage(), e);
            throw e;
        } catch (Exception e) {
            logger.error("Exception during deleteGroup for id {}: {}", groupId, e.getMessage(), e);
            throw e;
        }
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
        SqlUtil.addCondition(whereClause, parameters, "group_id", groupId);
        SqlUtil.addCondition(whereClause, parameters, "group_desc", groupDesc);


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

        SqlUtil.addCondition(whereClause, parameters, "g.group_id", groupId);
        SqlUtil.addCondition(whereClause, parameters, "p.api_id", apiId);
        SqlUtil.addCondition(whereClause, parameters, "p.api_version", apiVersion);
        SqlUtil.addCondition(whereClause, parameters, "p.endpoint", endpoint);

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

        SqlUtil.addCondition(whereClause, parameters, "g.group_id", groupId);
        SqlUtil.addCondition(whereClause, parameters, "u.user_id", userId != null ? UUID.fromString(userId) : null);
        SqlUtil.addCondition(whereClause, parameters, "entity_id", entityId);
        SqlUtil.addCondition(whereClause, parameters, "u.email", email);
        SqlUtil.addCondition(whereClause, parameters, "u.first_name", firstName);
        SqlUtil.addCondition(whereClause, parameters, "u.last_name", lastName);
        SqlUtil.addCondition(whereClause, parameters, "u.user_type", userType);

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
                    map.put("startTs", resultSet.getObject("start_ts") != null ? resultSet.getObject("start_ts", OffsetDateTime.class) : null);
                    map.put("endTs", resultSet.getObject("end_ts") != null ? resultSet.getObject("end_ts", OffsetDateTime.class) : null);
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
    public void createGroupPermission(Connection conn, Map<String, Object> event) throws SQLException, Exception {
        final String insertGroup =
                """
                    INSERT INTO group_permission_t (host_id, group_id, endpoint_id, update_user, update_ts)
                    VALUES (
                        ?,
                        ?,
                        (SELECT e.endpoint_id
                         FROM api_endpoint_t e
                         JOIN api_version_t v ON e.host_id = v.host_id
                                             AND e.api_version_id = v.api_version_id
                         WHERE e.host_id = ?
                           AND v.api_id = ?
                           AND v.api_version = ?
                           AND e.endpoint = ?
                        ),
                        ?,
                        ?
                    )
                """;


        Map<String, Object> map = (Map<String, Object>)event.get(PortalConstants.DATA);
        String groupId = (String)map.get("groupId");
        try (PreparedStatement statement = conn.prepareStatement(insertGroup)) {
            statement.setObject(1, UUID.fromString((String)event.get(Constants.HOST)));
            statement.setString(2, groupId);
            statement.setObject(3, UUID.fromString((String)event.get(Constants.HOST)));
            statement.setString(4, (String)map.get("apiId"));
            statement.setString(5, (String)map.get("apiVersion"));
            statement.setString(6, (String)map.get("endpoint"));
            statement.setString(7, (String)event.get(Constants.USER));
            statement.setObject(8, OffsetDateTime.parse((String)event.get(CloudEventV1.TIME)));

            int count = statement.executeUpdate();
            if (count == 0) {
                throw new SQLException("Failed to insert group permission " + groupId);
            }
        } catch (SQLException e) {
            logger.error("SQLException during createGroupPermission for id {}: {}", groupId, e.getMessage(), e);
            throw e;
        } catch (Exception e) {
            logger.error("Exception during createGroupPermission for id {}: {}", groupId, e.getMessage(), e);
            throw e;
        }
    }
    @Override
    public void deleteGroupPermission(Connection conn, Map<String, Object> event) throws SQLException, Exception {
        final String deleteGroup =
                """
                    DELETE FROM group_permission_t gp
                    WHERE gp.host_id = ?
                      AND gp.group_id = ?
                      AND gp.endpoint_id IN (
                        SELECT e.endpoint_id
                        FROM api_endpoint_t e
                        JOIN api_version_t v ON e.host_id = v.host_id
                                            AND e.api_version_id = v.api_version_id
                        WHERE e.host_id = ?
                          AND v.api_id = ?
                          AND v.api_version = ?
                          AND e.endpoint = ?
                      )
                """;

        Map<String, Object> map = (Map<String, Object>)event.get(PortalConstants.DATA);
        String groupId = (String)map.get("groupId");
        try (PreparedStatement statement = conn.prepareStatement(deleteGroup)) {
            statement.setObject(1, UUID.fromString((String)event.get(Constants.HOST)));
            statement.setString(2, groupId);
            statement.setObject(3, UUID.fromString((String)event.get(Constants.HOST)));
            statement.setString(4, (String)map.get("apiId"));
            statement.setString(5, (String)map.get("apiVersion"));
            statement.setString(6, (String)map.get("endpoint"));

            int count = statement.executeUpdate();
            if (count == 0) {
                throw new SQLException("no record is deleted for group permission " + groupId);
            }
        } catch (SQLException e) {
            logger.error("SQLException during deleteGroupPermission for id {}: {}", groupId, e.getMessage(), e);
            throw e;
        } catch (Exception e) {
            logger.error("Exception during deleteGroupPermission for id {}: {}", groupId, e.getMessage(), e);
            throw e;
        }
    }
    @Override
    public void createGroupUser(Connection conn, Map<String, Object> event) throws SQLException, Exception {
        final String insertGroup = "INSERT INTO group_user_t (host_id, group_id, user_id, start_ts, end_ts, update_user, update_ts) " +
                "VALUES (?, ?, ?, ?, ?,  ?, ?)";

        Map<String, Object> map = (Map<String, Object>)event.get(PortalConstants.DATA);
        String groupId = (String)map.get("groupId");
        try (PreparedStatement statement = conn.prepareStatement(insertGroup)) {
            statement.setObject(1, UUID.fromString((String)event.get(Constants.HOST)));
            statement.setString(2, groupId);
            statement.setObject(3, UUID.fromString((String)event.get(Constants.USER)));
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
                throw new SQLException("Failed to insert group user " + groupId);
            }
        } catch (SQLException e) {
            logger.error("SQLException during createGroupUser for id {}: {}", groupId, e.getMessage(), e);
            throw e;
        } catch (Exception e) {
            logger.error("Exception during createGroupUser for id {}: {}", groupId, e.getMessage(), e);
            throw e;
        }
    }
    @Override
    public void updateGroupUser(Connection conn, Map<String, Object> event) throws SQLException, Exception {
        final String updateGroup = "UPDATE group_user_t SET start_ts = ?, end_ts = ?, update_user = ?, update_ts = ? " +
                "WHERE host_id = ? AND group_id = ? AND user_id = ?";

        Map<String, Object> map = (Map<String, Object>)event.get(PortalConstants.DATA);
        String groupId = (String)map.get("groupId");
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
            statement.setObject(5, UUID.fromString((String)event.get(Constants.HOST)));
            statement.setString(6, groupId);
            statement.setObject(7, UUID.fromString((String)event.get(Constants.USER)));

            int count = statement.executeUpdate();
            if (count == 0) {
                throw new SQLException("no record is updated for group user " + groupId);
            }
        } catch (SQLException e) {
            logger.error("SQLException during updateGroupUser for id {}: {}", groupId, e.getMessage(), e);
            throw e;
        } catch (Exception e) {
            logger.error("Exception during updateGroupUser for id {}: {}", groupId, e.getMessage(), e);
            throw e;
        }
    }
    @Override
    public void deleteGroupUser(Connection conn, Map<String, Object> event) throws SQLException, Exception {
        final String deleteGroup = "DELETE from group_user_t WHERE host_id = ? AND group_id = ? AND user_id = ?";
        Map<String, Object> map = (Map<String, Object>)event.get(PortalConstants.DATA);
        String groupId = (String)map.get("groupId");
        try (PreparedStatement statement = conn.prepareStatement(deleteGroup)) {
            statement.setObject(1, UUID.fromString((String)event.get(Constants.HOST)));
            statement.setString(2, groupId);
            statement.setObject(3, UUID.fromString((String)event.get(Constants.USER)));

            int count = statement.executeUpdate();
            if (count == 0) {
                throw new SQLException("no record is deleted for group user " + groupId);
            }
        } catch (SQLException e) {
            logger.error("SQLException during deleteGroupUser for id {}: {}", groupId, e.getMessage(), e);
            throw e;
        } catch (Exception e) {
            logger.error("Exception during deleteGroupUser for id {}: {}", groupId, e.getMessage(), e);
            throw e;
        }
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

        SqlUtil.addCondition(whereClause, parameters, "g.group_id", GroupId);
        SqlUtil.addCondition(whereClause, parameters, "p.api_id", apiId);
        SqlUtil.addCondition(whereClause, parameters, "p.api_version", apiVersion);
        SqlUtil.addCondition(whereClause, parameters, "p.endpoint", endpoint);

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
    public void createGroupRowFilter(Connection conn, Map<String, Object> event) throws SQLException, Exception {
        final String insertGroup =
                """
                    INSERT INTO group_row_filter_t (
                        host_id,
                        group_id,
                        endpoint_id,  -- Using resolved endpoint_id instead of direct API references
                        col_name,
                        operator,
                        col_value,
                        update_user,
                        update_ts
                    )
                    SELECT
                        ?,              -- host_id parameter (1st)
                        ?,              -- group_id parameter
                        e.endpoint_id,  -- Resolved from join
                        ?,              -- col_name parameter
                        ?,              -- operator parameter
                        ?,              -- col_value parameter
                        ?,              -- update_user parameter
                        ?               -- update_ts parameter (or use DEFAULT)
                    FROM
                        api_endpoint_t e
                    JOIN
                        api_version_t v ON e.host_id = v.host_id
                                       AND e.api_version_id = v.api_version_id
                    WHERE
                        e.host_id = ?                  -- Same as first host_id parameter
                        AND v.api_id = ?               -- api_id parameter
                        AND v.api_version = ?          -- api_version parameter
                        AND e.endpoint = ?             -- endpoint parameter

                """;

        Map<String, Object> map = (Map<String, Object>)event.get(PortalConstants.DATA);
        String groupId = (String)map.get("groupId");
        try (PreparedStatement statement = conn.prepareStatement(insertGroup)) {
            statement.setObject(1, UUID.fromString((String)event.get(Constants.HOST)));
            statement.setString(2, groupId);
            statement.setString(3, (String)map.get("colName"));
            statement.setString(4, (String)map.get("operator"));
            statement.setString(5, (String)map.get("colValue"));
            statement.setString(6, (String)event.get(Constants.USER));
            statement.setObject(7, OffsetDateTime.parse((String)event.get(CloudEventV1.TIME)));
            statement.setObject(8, UUID.fromString((String)event.get(Constants.HOST)));
            statement.setString(9, (String)map.get("apiId"));
            statement.setString(10, (String)map.get("apiVersion"));
            statement.setString(11, (String)map.get("endpoint"));

            int count = statement.executeUpdate();
            if (count == 0) {
                throw new SQLException("Failed to insert group row filter " + groupId);
            }
        } catch (SQLException e) {
            logger.error("SQLException during createGroupRowFilter for id {}: {}", groupId, e.getMessage(), e);
            throw e;
        } catch (Exception e) {
            logger.error("Exception during createGroupRowFilter for id {}: {}", groupId, e.getMessage(), e);
            throw e;
        }
    }

    @Override
    public void updateGroupRowFilter(Connection conn, Map<String, Object> event) throws SQLException, Exception {
        final String updateGroup =
                """
                    UPDATE group_row_filter_t
                    SET\s
                        operator = ?,
                        col_value = ?,
                        update_user = ?,
                        update_ts = ?
                    WHERE\s
                        host_id = ?
                        AND group_id = ?
                        AND col_name = ?
                        AND endpoint_id IN (
                            SELECT e.endpoint_id
                            FROM api_endpoint_t e
                            JOIN api_version_t v ON e.host_id = v.host_id\s
                                                AND e.api_version_id = v.api_version_id
                            WHERE e.host_id = ?
                              AND v.api_id = ?
                              AND v.api_version = ?
                              AND e.endpoint = ?
                        )
                """;

        Map<String, Object> map = (Map<String, Object>)event.get(PortalConstants.DATA);
        String groupId = (String)map.get("groupId");
        try (PreparedStatement statement = conn.prepareStatement(updateGroup)) {
            statement.setString(1, (String)map.get("operator"));
            statement.setString(2, (String)map.get("colValue"));
            statement.setString(3, (String)event.get(Constants.USER));
            statement.setObject(4, OffsetDateTime.parse((String)event.get(CloudEventV1.TIME)));
            statement.setObject(5, UUID.fromString((String)event.get(Constants.HOST)));
            statement.setString(6, groupId);
            statement.setString(7, (String)map.get("colName"));
            statement.setObject(8, UUID.fromString((String)event.get(Constants.HOST)));
            statement.setString(9, (String)map.get("apiId"));
            statement.setString(10, (String)map.get("apiVersion"));
            statement.setString(11, (String)map.get("endpoint"));

            int count = statement.executeUpdate();
            if (count == 0) {
                throw new SQLException("no record is updated for group row filter " + groupId);
            }
        } catch (SQLException e) {
            logger.error("SQLException during updateGroupRowFilter for id {}: {}", groupId, e.getMessage(), e);
            throw e;
        } catch (Exception e) {
            logger.error("Exception during updateGroupRowFilter for id {}: {}", groupId, e.getMessage(), e);
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
                      AND col_name = ?
                      AND endpoint_id IN (
                          SELECT e.endpoint_id
                          FROM api_endpoint_t e
                          JOIN api_version_t v ON e.host_id = v.host_id\s
                                              AND e.api_version_id = v.api_version_id
                          WHERE e.host_id = ?
                            AND v.api_id = ?
                            AND v.api_version = ?
                            AND e.endpoint = ?
                      )
                """;
        Map<String, Object> map = (Map<String, Object>)event.get(PortalConstants.DATA);
        String groupId = (String)map.get("groupId");
        try (PreparedStatement statement = conn.prepareStatement(deleteGroup)) {
            statement.setObject(1, UUID.fromString((String)event.get(Constants.HOST)));
            statement.setString(2, groupId);
            statement.setString(3, (String)map.get("colName"));
            statement.setObject(4, UUID.fromString((String)event.get(Constants.HOST)));
            statement.setString(5, (String)map.get("apiId"));
            statement.setString(6, (String)map.get("apiVersion"));
            statement.setString(7, (String)map.get("endpoint"));

            int count = statement.executeUpdate();
            if (count == 0) {
                throw new SQLException("no record is deleted for group row filter " + groupId);
            }
        } catch (SQLException e) {
            logger.error("SQLException during deleteGroupRowFilter for id {}: {}", groupId, e.getMessage(), e);
            throw e;
        } catch (Exception e) {
            logger.error("Exception during deleteGroupRowFilter for id {}: {}", groupId, e.getMessage(), e);
            throw e;
        }
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

        SqlUtil.addCondition(whereClause, parameters, "g.group_id", GroupId);
        SqlUtil.addCondition(whereClause, parameters, "p.api_id", apiId);
        SqlUtil.addCondition(whereClause, parameters, "p.api_version", apiVersion);
        SqlUtil.addCondition(whereClause, parameters, "p.endpoint", endpoint);

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
    public void createGroupColFilter(Connection conn, Map<String, Object> event) throws SQLException, Exception {
        final String insertGroup =
                """
                    INSERT INTO group_col_filter_t (
                        host_id,
                        group_id,
                        endpoint_id,  -- Using resolved endpoint_id instead of direct API references
                        columns,
                        update_user,
                        update_ts
                    )
                    SELECT
                        ?,              -- host_id parameter (1st)
                        ?,              -- group_id parameter
                        e.endpoint_id,  -- Resolved from join
                        ?,              -- columns parameter
                        ?,              -- update_user parameter
                        ?               -- update_ts parameter (or use DEFAULT)
                    FROM
                        api_endpoint_t e
                    JOIN
                        api_version_t v ON e.host_id = v.host_id
                                       AND e.api_version_id = v.api_version_id
                    WHERE
                        e.host_id = ?                  -- Same as first host_id parameter
                        AND v.api_id = ?               -- api_id parameter
                        AND v.api_version = ?          -- api_version parameter
                        AND e.endpoint = ?            -- endpoint parameter
                """;

        Map<String, Object> map = (Map<String, Object>)event.get(PortalConstants.DATA);
        String groupId = (String)map.get("groupId");
        try (PreparedStatement statement = conn.prepareStatement(insertGroup)) {
            statement.setObject(1, UUID.fromString((String)event.get(Constants.HOST)));
            statement.setString(2, groupId);
            statement.setString(3, (String)map.get("columns"));
            statement.setString(4, (String)event.get(Constants.USER));
            statement.setObject(5, OffsetDateTime.parse((String)event.get(CloudEventV1.TIME)));
            statement.setObject(6, UUID.fromString((String)event.get(Constants.HOST)));
            statement.setString(7, (String)map.get("apiId"));
            statement.setString(8, (String)map.get("apiVersion"));
            statement.setString(9, (String)map.get("endpoint"));

            int count = statement.executeUpdate();
            if (count == 0) {
                throw new SQLException("Failed to insert group col filter " + groupId);
            }
        } catch (SQLException e) {
            logger.error("SQLException during createGroupColFilter for id {}: {}", groupId, e.getMessage(), e);
            throw e;
        } catch (Exception e) {
            logger.error("Exception during createGroupColFilter for id {}: {}", groupId, e.getMessage(), e);
            throw e;
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
                        update_ts = ?
                    WHERE
                        host_id = ?
                        AND group_id = ?
                        AND endpoint_id IN (
                            SELECT e.endpoint_id
                            FROM api_endpoint_t e
                            JOIN api_version_t v ON e.host_id = v.host_id
                                                AND e.api_version_id = v.api_version_id
                            WHERE e.host_id = ?
                              AND v.api_id = ?
                              AND v.api_version = ?
                              AND e.endpoint = ?
                        )
                """;

        Map<String, Object> map = (Map<String, Object>)event.get(PortalConstants.DATA);
        String groupId = (String)map.get("groupId");
        try (PreparedStatement statement = conn.prepareStatement(updateGroup)) {
            statement.setString(1, (String)map.get("columns"));
            statement.setString(2, (String)event.get(Constants.USER));
            statement.setObject(3, OffsetDateTime.parse((String)event.get(CloudEventV1.TIME)));
            statement.setObject(4, UUID.fromString((String)event.get(Constants.HOST)));
            statement.setString(5, groupId);
            statement.setObject(6, UUID.fromString((String)event.get(Constants.HOST)));
            statement.setString(7, (String)map.get("apiId"));
            statement.setString(8, (String)map.get("apiVersion"));
            statement.setString(9, (String)map.get("endpoint"));

            int count = statement.executeUpdate();
            if (count == 0) {
                throw new SQLException("no record is updated for group col filter " + groupId);
            }
        } catch (SQLException e) {
            logger.error("SQLException during updateGroupColFilter for id {}: {}", groupId, e.getMessage(), e);
            throw e;
        } catch (Exception e) {
            logger.error("Exception during updateGroupColFilter for id {}: {}", groupId, e.getMessage(), e);
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
                      AND endpoint_id IN (
                          SELECT e.endpoint_id
                          FROM api_endpoint_t e
                          JOIN api_version_t v ON e.host_id = v.host_id\s
                                              AND e.api_version_id = v.api_version_id
                          WHERE e.host_id = ?
                            AND v.api_id = ?
                            AND v.api_version = ?
                            AND e.endpoint = ?
                      )
                """;
        Map<String, Object> map = (Map<String, Object>)event.get(PortalConstants.DATA);
        String groupId = (String)map.get("groupId");
        try (PreparedStatement statement = conn.prepareStatement(deleteGroup)) {
            statement.setObject(1, UUID.fromString((String)event.get(Constants.HOST)));
            statement.setString(2, groupId);
            statement.setObject(3, UUID.fromString((String)event.get(Constants.HOST)));
            statement.setString(4, (String)map.get("apiId"));
            statement.setString(5, (String)map.get("apiVersion"));
            statement.setString(6, (String)map.get("endpoint"));

            int count = statement.executeUpdate();
            if (count == 0) {
                throw new SQLException("no record is deleted for group col filter " + groupId);
            }
        } catch (SQLException e) {
            logger.error("SQLException during deleteGroupColFilter for id {}: {}", groupId, e.getMessage(), e);
            throw e;
        } catch (Exception e) {
            logger.error("Exception during deleteGroupColFilter for id {}: {}", groupId, e.getMessage(), e);
            throw e;
        }
    }


    @Override
    public void createPosition(Connection conn, Map<String, Object> event) throws SQLException, Exception {
        final String insertPosition = "INSERT INTO position_t (host_id, position_id, position_desc, " +
                "inherit_to_ancestor, inherit_to_sibling, update_user, update_ts) " +
                "VALUES (?, ?, ?, ?, ?, ?, ?)";

        Map<String, Object> map = (Map<String, Object>)event.get(PortalConstants.DATA);
        String positionId = (String)map.get("positionId");
        try (PreparedStatement statement = conn.prepareStatement(insertPosition)) {
            statement.setObject(1, UUID.fromString((String)event.get(Constants.HOST)));
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

            int count = statement.executeUpdate();
            if (count == 0) {
                throw new SQLException("Failed to insert position " + positionId);
            }
        } catch (SQLException e) {
            logger.error("SQLException during createPosition for id {}: {}", positionId, e.getMessage(), e);
            throw e;
        } catch (Exception e) {
            logger.error("Exception during createPosition for id {}: {}", positionId, e.getMessage(), e);
            throw e;
        }
    }

    @Override
    public void updatePosition(Connection conn, Map<String, Object> event) throws SQLException, Exception {
        final String updatePosition = "UPDATE position_t SET position_desc = ?, inherit_to_ancestor = ?, inherit_to_sibling = ?, " +
                "update_user = ?, update_ts = ? " +
                "WHERE host_id = ? AND position_id = ?";

        Map<String, Object> map = (Map<String, Object>)event.get(PortalConstants.DATA);
        String positionId = (String)map.get("positionId");
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

            statement.setObject(6, UUID.fromString((String)event.get(Constants.HOST)));
            statement.setString(7, positionId);

            int count = statement.executeUpdate();
            if (count == 0) {
                throw new SQLException("no record is updated for position " + positionId);
            }
        } catch (SQLException e) {
            logger.error("SQLException during updatePosition for id {}: {}", positionId, e.getMessage(), e);
            throw e;
        } catch (Exception e) {
            logger.error("Exception during updatePosition for id {}: {}", positionId, e.getMessage(), e);
            throw e;
        }
    }

    @Override
    public void deletePosition(Connection conn, Map<String, Object> event) throws SQLException, Exception {
        final String deleteGroup = "DELETE from position_t WHERE host_id = ? AND position_id = ?";
        Map<String, Object> map = (Map<String, Object>)event.get(PortalConstants.DATA);
        String positionId = (String)map.get("positionId");
        try (PreparedStatement statement = conn.prepareStatement(deleteGroup)) {
            statement.setObject(1, UUID.fromString((String)event.get(Constants.HOST)));
            statement.setString(2, positionId);
            int count = statement.executeUpdate();
            if (count == 0) {
                throw new SQLException("no record is deleted for position " + positionId);
            }
        } catch (SQLException e) {
            logger.error("SQLException during deletePosition for id {}: {}", positionId, e.getMessage(), e);
            throw e;
        } catch (Exception e) {
            logger.error("Exception during deletePosition for id {}: {}", positionId, e.getMessage(), e);
            throw e;
        }
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
        SqlUtil.addCondition(whereClause, parameters, "position_id", positionId);
        SqlUtil.addCondition(whereClause, parameters, "position_desc", positionDesc);
        SqlUtil.addCondition(whereClause, parameters, "inherit_to_ancestor", inheritToAncestor);
        SqlUtil.addCondition(whereClause, parameters, "inherit_to_sibling", inheritToSibling);


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

        SqlUtil.addCondition(whereClause, parameters, "o.position_id", positionId);
        SqlUtil.addCondition(whereClause, parameters, "o.inherit_to_ancestor", inheritToAncestor);
        SqlUtil.addCondition(whereClause, parameters, "o.inherit_to_sibling", inheritToSibling);
        SqlUtil.addCondition(whereClause, parameters, "p.api_id", apiId);
        SqlUtil.addCondition(whereClause, parameters, "p.api_version", apiVersion);
        SqlUtil.addCondition(whereClause, parameters, "p.endpoint", endpoint);

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

        SqlUtil.addCondition(whereClause, parameters, "ep.position_id", positionId);
        SqlUtil.addCondition(whereClause, parameters, "ep.position_type", positionType);
        SqlUtil.addCondition(whereClause, parameters, "u.user_id", userId != null ? UUID.fromString(userId) : null);
        SqlUtil.addCondition(whereClause, parameters, "entity_id", entityId);
        SqlUtil.addCondition(whereClause, parameters, "u.email", email);
        SqlUtil.addCondition(whereClause, parameters, "u.first_name", firstName);
        SqlUtil.addCondition(whereClause, parameters, "u.last_name", lastName);
        SqlUtil.addCondition(whereClause, parameters, "u.user_type", userType);

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
    public void createPositionPermission(Connection conn, Map<String, Object> event) throws SQLException, Exception {
        final String insertGroup =
                """
                    INSERT INTO position_permission_t (host_id, position_id, endpoint_id, update_user, update_ts)
                    VALUES (
                        ?,
                        ?,
                        (SELECT e.endpoint_id
                         FROM api_endpoint_t e
                         JOIN api_version_t v ON e.host_id = v.host_id
                                             AND e.api_version_id = v.api_version_id
                         WHERE e.host_id = ?
                           AND v.api_id = ?
                           AND v.api_version = ?
                           AND e.endpoint = ?
                        ),
                        ?,
                        ?
                    )
                """;

        Map<String, Object> map = (Map<String, Object>)event.get(PortalConstants.DATA);
        String positionId = (String)map.get("positionId");
        try (PreparedStatement statement = conn.prepareStatement(insertGroup)) {
            statement.setObject(1, UUID.fromString((String)event.get(Constants.HOST)));
            statement.setString(2, positionId);
            statement.setObject(3, UUID.fromString((String)event.get(Constants.HOST)));
            statement.setString(4, (String)map.get("apiId"));
            statement.setString(5, (String)map.get("apiVersion"));
            statement.setString(6, (String)map.get("endpoint"));
            statement.setString(7, (String)event.get(Constants.USER));
            statement.setObject(8, OffsetDateTime.parse((String)event.get(CloudEventV1.TIME)));

            int count = statement.executeUpdate();
            if (count == 0) {
                throw new SQLException("Failed to insert position permission " + positionId);
            }
        } catch (SQLException e) {
            logger.error("SQLException during createPositionPermission for id {}: {}", positionId, e.getMessage(), e);
            throw e;
        } catch (Exception e) {
            logger.error("Exception during createPositionPermission for id {}: {}", positionId, e.getMessage(), e);
            throw e;
        }
    }

    @Override
    public void deletePositionPermission(Connection conn, Map<String, Object> event) throws SQLException, Exception {
        final String deleteGroup =
                """
                    DELETE FROM position_permission_t gp
                    WHERE gp.host_id = ?
                      AND gp.position_id = ?
                      AND gp.endpoint_id IN (
                        SELECT e.endpoint_id
                        FROM api_endpoint_t e
                        JOIN api_version_t v ON e.host_id = v.host_id
                                            AND e.api_version_id = v.api_version_id
                        WHERE e.host_id = ?
                          AND v.api_id = ?
                          AND v.api_version = ?
                          AND e.endpoint = ?
                      )
                """;
        Map<String, Object> map = (Map<String, Object>)event.get(PortalConstants.DATA);
        String positionId = (String)map.get("positionId");
        try (PreparedStatement statement = conn.prepareStatement(deleteGroup)) {
            statement.setObject(1, UUID.fromString((String)event.get(Constants.HOST)));
            statement.setString(2, positionId);
            statement.setObject(3, UUID.fromString((String)event.get(Constants.HOST)));
            statement.setString(4, (String)map.get("apiId"));
            statement.setString(5, (String)map.get("apiVersion"));
            statement.setString(6, (String)map.get("endpoint"));

            int count = statement.executeUpdate();
            if (count == 0) {
                throw new SQLException("no record is deleted for position permission " + positionId);
            }
        } catch (SQLException e) {
            logger.error("SQLException during deletePositionPermission for id {}: {}", positionId, e.getMessage(), e);
            throw e;
        } catch (Exception e) {
            logger.error("Exception during deletePositionPermission for id {}: {}", positionId, e.getMessage(), e);
            throw e;
        }
    }

    @Override
    public void createPositionUser(Connection conn, Map<String, Object> event) throws SQLException, Exception {
        final String insertGroup = "INSERT INTO position_user_t (host_id, position_id, user_id, start_ts, end_ts, update_user, update_ts) " +
                "VALUES (?, ?, ?, ?, ?,  ?, ?)";

        Map<String, Object> map = (Map<String, Object>)event.get(PortalConstants.DATA);
        String positionId = (String)map.get("positionId");
        try (PreparedStatement statement = conn.prepareStatement(insertGroup)) {
            statement.setObject(1, UUID.fromString((String)event.get(Constants.HOST)));
            statement.setString(2, positionId);
            statement.setObject(3, UUID.fromString((String)event.get(Constants.USER)));
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
                throw new SQLException("Failed to insert position user " + positionId);
            }
        } catch (SQLException e) {
            logger.error("SQLException during createPositionUser for id {}: {}", positionId, e.getMessage(), e);
            throw e;
        } catch (Exception e) {
            logger.error("Exception during createPositionUser for id {}: {}", positionId, e.getMessage(), e);
            throw e;
        }
    }

    @Override
    public void updatePositionUser(Connection conn, Map<String, Object> event) throws SQLException, Exception {
        final String updateGroup = "UPDATE position_user_t SET start_ts = ?, end_ts = ?, update_user = ?, update_ts = ? " +
                "WHERE host_id = ? AND position_id = ? AND user_id = ?";

        Map<String, Object> map = (Map<String, Object>)event.get(PortalConstants.DATA);
        String positionId = (String)map.get("positionId");
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
            statement.setObject(5, UUID.fromString((String)event.get(Constants.HOST)));
            statement.setString(6, positionId);
            statement.setObject(7, UUID.fromString((String)event.get(Constants.USER)));

            int count = statement.executeUpdate();
            if (count == 0) {
                throw new SQLException("no record is updated for position user " + positionId);
            }
        } catch (SQLException e) {
            logger.error("SQLException during updatePositionUser for id {}: {}", positionId, e.getMessage(), e);
            throw e;
        } catch (Exception e) {
            logger.error("Exception during updatePositionUser for id {}: {}", positionId, e.getMessage(), e);
            throw e;
        }
    }

    @Override
    public void deletePositionUser(Connection conn, Map<String, Object> event) throws SQLException, Exception {
        final String deleteGroup = "DELETE from position_user_t WHERE host_id = ? AND position_id = ? AND user_id = ?";
        Map<String, Object> map = (Map<String, Object>)event.get(PortalConstants.DATA);
        String positionId = (String)map.get("positionId");
        try (PreparedStatement statement = conn.prepareStatement(deleteGroup)) {
            statement.setObject(1, UUID.fromString((String)event.get(Constants.HOST)));
            statement.setString(2, positionId);
            statement.setObject(3, UUID.fromString((String)event.get(Constants.USER)));

            int count = statement.executeUpdate();
            if (count == 0) {
                throw new SQLException("no record is deleted for position user " + positionId);
            }
        } catch (SQLException e) {
            logger.error("SQLException during deletePositionUser for id {}: {}", positionId, e.getMessage(), e);
            throw e;
        } catch (Exception e) {
            logger.error("Exception during deletePositionUser for id {}: {}", positionId, e.getMessage(), e);
            throw e;
        }
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

        SqlUtil.addCondition(whereClause, parameters, "o.position_id", PositionId);
        SqlUtil.addCondition(whereClause, parameters, "p.api_id", apiId);
        SqlUtil.addCondition(whereClause, parameters, "p.api_version", apiVersion);
        SqlUtil.addCondition(whereClause, parameters, "p.endpoint", endpoint);

        if (!whereClause.isEmpty()) {
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
    public void createPositionRowFilter(Connection conn, Map<String, Object> event) throws SQLException, Exception {
        final String insertGroup =
                """
                    INSERT INTO position_row_filter_t (
                        host_id,
                        position_id,
                        endpoint_id,  -- Using resolved endpoint_id instead of direct API references
                        col_name,
                        operator,
                        col_value,
                        update_user,
                        update_ts
                    )
                    SELECT
                        ?,              -- host_id parameter (1st)
                        ?,              -- position_id parameter
                        e.endpoint_id,  -- Resolved from join
                        ?,              -- col_name parameter
                        ?,              -- operator parameter
                        ?,              -- col_value parameter
                        ?,              -- update_user parameter
                        ?               -- update_ts parameter (or use DEFAULT)
                    FROM
                        api_endpoint_t e
                    JOIN
                        api_version_t v ON e.host_id = v.host_id
                                       AND e.api_version_id = v.api_version_id
                    WHERE
                        e.host_id = ?                  -- Same as first host_id parameter
                        AND v.api_id = ?               -- api_id parameter
                        AND v.api_version = ?          -- api_version parameter
                        AND e.endpoint = ?            -- endpoint parameter
                """;

        Map<String, Object> map = (Map<String, Object>)event.get(PortalConstants.DATA);
        String positionId = (String)map.get("positionId");
        try (PreparedStatement statement = conn.prepareStatement(insertGroup)) {
            statement.setObject(1, UUID.fromString((String)event.get(Constants.HOST)));
            statement.setString(2, positionId);
            statement.setString(3, (String)map.get("colName"));
            statement.setString(4, (String)map.get("operator"));
            statement.setString(5, (String)map.get("colValue"));
            statement.setString(6, (String)event.get(Constants.USER));
            statement.setObject(7, OffsetDateTime.parse((String)event.get(CloudEventV1.TIME)));
            statement.setObject(8, UUID.fromString((String)event.get(Constants.HOST)));
            statement.setString(9, (String)map.get("apiId"));
            statement.setString(10, (String)map.get("apiVersion"));
            statement.setString(11, (String)map.get("endpoint"));

            int count = statement.executeUpdate();
            if (count == 0) {
                throw new SQLException("Failed to insert position row filter " + positionId);
            }
        } catch (SQLException e) {
            logger.error("SQLException during createPositionRowFilter for id {}: {}", positionId, e.getMessage(), e);
            throw e;
        } catch (Exception e) {
            logger.error("Exception during createPositionRowFilter for id {}: {}", positionId, e.getMessage(), e);
            throw e;
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
                        update_ts = ?
                    WHERE
                        host_id = ?
                        AND position_id = ?
                        AND col_name = ?
                        AND endpoint_id IN (
                            SELECT e.endpoint_id
                            FROM api_endpoint_t e
                            JOIN api_version_t v ON e.host_id = v.host_id
                                                AND e.api_version_id = v.api_version_id
                            WHERE e.host_id = ?
                              AND v.api_id = ?
                              AND v.api_version = ?
                              AND e.endpoint = ?
                        )
                """;

        Map<String, Object> map = (Map<String, Object>)event.get(PortalConstants.DATA);
        String positionId = (String)map.get("positionId");
        try (PreparedStatement statement = conn.prepareStatement(updateGroup)) {
            statement.setString(1, (String)map.get("operator"));
            statement.setString(2, (String)map.get("colValue"));
            statement.setString(3, (String)event.get(Constants.USER));
            statement.setObject(4, OffsetDateTime.parse((String)event.get(CloudEventV1.TIME)));
            statement.setObject(5, UUID.fromString((String)event.get(Constants.HOST)));
            statement.setString(6, positionId);
            statement.setString(7, (String)map.get("colName"));
            statement.setObject(8, UUID.fromString((String)event.get(Constants.HOST)));

            statement.setString(9, (String)map.get("apiId"));
            statement.setString(10, (String)map.get("apiVersion"));
            statement.setString(11, (String)map.get("endpoint"));

            int count = statement.executeUpdate();
            if (count == 0) {
                throw new SQLException("no record is updated for position row filter " + positionId);
            }
        } catch (SQLException e) {
            logger.error("SQLException during updatePositionRowFilter for id {}: {}", positionId, e.getMessage(), e);
            throw e;
        } catch (Exception e) {
            logger.error("Exception during updatePositionRowFilter for id {}: {}", positionId, e.getMessage(), e);
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
                      AND col_name = ?
                      AND endpoint_id IN (
                          SELECT e.endpoint_id
                          FROM api_endpoint_t e
                          JOIN api_version_t v ON e.host_id = v.host_id
                                              AND e.api_version_id = v.api_version_id
                          WHERE e.host_id = ?
                            AND v.api_id = ?
                            AND v.api_version = ?
                            AND e.endpoint = ?
                      )
                """;
        Map<String, Object> map = (Map<String, Object>)event.get(PortalConstants.DATA);
        String positionId = (String)map.get("positionId");
        try (PreparedStatement statement = conn.prepareStatement(deleteGroup)) {
            statement.setObject(1, UUID.fromString((String)event.get(Constants.HOST)));
            statement.setString(2, positionId);
            statement.setString(3, (String)map.get("colName"));
            statement.setObject(4, UUID.fromString((String)event.get(Constants.HOST)));
            statement.setString(5, (String)map.get("apiId"));
            statement.setString(6, (String)map.get("apiVersion"));
            statement.setString(7, (String)map.get("endpoint"));

            int count = statement.executeUpdate();
            if (count == 0) {
                throw new SQLException("no record is deleted for position row filter " + positionId);
            }
        } catch (SQLException e) {
            logger.error("SQLException during deletePositionRowFilter for id {}: {}", positionId, e.getMessage(), e);
            throw e;
        } catch (Exception e) {
            logger.error("Exception during deletePositionRowFilter for id {}: {}", positionId, e.getMessage(), e);
            throw e;
        }
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

        SqlUtil.addCondition(whereClause, parameters, "o.position_id", PositionId);
        SqlUtil.addCondition(whereClause, parameters, "p.api_id", apiId);
        SqlUtil.addCondition(whereClause, parameters, "p.api_version", apiVersion);
        SqlUtil.addCondition(whereClause, parameters, "p.endpoint", endpoint);

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
    public void createPositionColFilter(Connection conn, Map<String, Object> event) throws SQLException, Exception {
        final String insertGroup =
                """
                    INSERT INTO position_col_filter_t (
                        host_id,
                        position_id,
                        endpoint_id,  -- Using resolved endpoint_id instead of direct API references
                        columns,
                        update_user,
                        update_ts
                    )
                    SELECT
                        ?,              -- host_id parameter (1st)
                        ?,              -- position_id parameter
                        e.endpoint_id,  -- Resolved from join
                        ?,              -- columns parameter
                        ?,              -- update_user parameter
                        ?               -- update_ts parameter (or use DEFAULT)
                    FROM
                        api_endpoint_t e
                    JOIN
                        api_version_t v ON e.host_id = v.host_id
                                       AND e.api_version_id = v.api_version_id
                    WHERE
                        e.host_id = ?                  -- Same as first host_id parameter
                        AND v.api_id = ?               -- api_id parameter
                        AND v.api_version = ?          -- api_version parameter
                        AND e.endpoint = ?             -- endpoint parameter
                """;

        Map<String, Object> map = (Map<String, Object>)event.get(PortalConstants.DATA);
        String positionId = (String)map.get("positionId");
        try (PreparedStatement statement = conn.prepareStatement(insertGroup)) {
            statement.setObject(1, UUID.fromString((String)event.get(Constants.HOST)));
            statement.setString(2, positionId);
            statement.setString(3, (String)map.get("columns"));
            statement.setString(4, (String)event.get(Constants.USER));
            statement.setObject(5, OffsetDateTime.parse((String)event.get(CloudEventV1.TIME)));
            statement.setObject(6, UUID.fromString((String)event.get(Constants.HOST)));
            statement.setString(7, (String)map.get("apiId"));
            statement.setString(8, (String)map.get("apiVersion"));
            statement.setString(9, (String)map.get("endpoint"));

            int count = statement.executeUpdate();
            if (count == 0) {
                throw new SQLException("Failed to insert position col filter " + positionId);
            }
        } catch (SQLException e) {
            logger.error("SQLException during createPositionColFilter for id {}: {}", positionId, e.getMessage(), e);
            throw e;
        } catch (Exception e) {
            logger.error("Exception during createPositionColFilter for id {}: {}", positionId, e.getMessage(), e);
            throw e;
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
                        update_ts = ?
                    WHERE
                        host_id = ?
                        AND position_id = ?
                        AND endpoint_id IN (
                            SELECT e.endpoint_id
                            FROM api_endpoint_t e
                            JOIN api_version_t v ON e.host_id = v.host_id
                                                AND e.api_version_id = v.api_version_id
                            WHERE e.host_id = ?
                              AND v.api_id = ?
                              AND v.api_version = ?
                              AND e.endpoint = ?
                        )
                """;

        Map<String, Object> map = (Map<String, Object>)event.get(PortalConstants.DATA);
        String positionId = (String)map.get("positionId");
        try (PreparedStatement statement = conn.prepareStatement(updateGroup)) {
            statement.setString(1, (String)map.get("columns"));
            statement.setString(2, (String)event.get(Constants.USER));
            statement.setObject(3, OffsetDateTime.parse((String)event.get(CloudEventV1.TIME)));
            statement.setObject(4, UUID.fromString((String)event.get(Constants.HOST)));
            statement.setString(5, positionId);
            statement.setObject(6, UUID.fromString((String)event.get(Constants.HOST)));
            statement.setString(7, (String)map.get("apiId"));
            statement.setString(8, (String)map.get("apiVersion"));
            statement.setString(9, (String)map.get("endpoint"));

            int count = statement.executeUpdate();
            if (count == 0) {
                throw new SQLException("no record is updated for position col filter " + positionId);
            }
        } catch (SQLException e) {
            logger.error("SQLException during updatePositionColFilter for id {}: {}", positionId, e.getMessage(), e);
            throw e;
        } catch (Exception e) {
            logger.error("Exception during updatePositionColFilter for id {}: {}", positionId, e.getMessage(), e);
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
                      AND endpoint_id IN (
                          SELECT e.endpoint_id
                          FROM api_endpoint_t e
                          JOIN api_version_t v ON e.host_id = v.host_id
                                              AND e.api_version_id = v.api_version_id
                          WHERE e.host_id = ?
                            AND v.api_id = ?
                            AND v.api_version = ?
                            AND e.endpoint = ?
                      )
                """;
        Map<String, Object> map = (Map<String, Object>)event.get(PortalConstants.DATA);
        String positionId = (String)map.get("positionId");
        try (PreparedStatement statement = conn.prepareStatement(deleteGroup)) {
            statement.setObject(1, UUID.fromString((String)event.get(Constants.HOST)));
            statement.setString(2, positionId);
            statement.setObject(3, UUID.fromString((String)event.get(Constants.HOST)));
            statement.setString(4, (String)map.get("apiId"));
            statement.setString(5, (String)map.get("apiVersion"));
            statement.setString(6, (String)map.get("endpoint"));

            int count = statement.executeUpdate();
            if (count == 0) {
                throw new SQLException("no record is deleted for position col filter " + positionId);
            }
        } catch (SQLException e) {
            logger.error("SQLException during deletePositionColFilter for id {}: {}", positionId, e.getMessage(), e);
            throw e;
        } catch (Exception e) {
            logger.error("Exception during deletePositionColFilter for id {}: {}", positionId, e.getMessage(), e);
            throw e;
        }
    }

    @Override
    public void createAttribute(Connection conn, Map<String, Object> event) throws SQLException, Exception {
        final String insertAttribute = "INSERT INTO attribute_t (host_id, attribute_id, attribute_type, " +
                "attribute_desc, update_user, update_ts) " +
                "VALUES (?, ?, ?, ?, ?, ?)";

        Map<String, Object> map = (Map<String, Object>)event.get(PortalConstants.DATA);
        String attributeId = (String)map.get("attributeId");
        try (PreparedStatement statement = conn.prepareStatement(insertAttribute)) {
            statement.setObject(1, UUID.fromString((String)event.get(Constants.HOST)));
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

            int count = statement.executeUpdate();
            if (count == 0) {
                throw new SQLException("Failed to insert attribute " + attributeId);
            }
        } catch (SQLException e) {
            logger.error("SQLException during createAttribute for id {}: {}", attributeId, e.getMessage(), e);
            throw e;
        } catch (Exception e) {
            logger.error("Exception during createAttribute for id {}: {}", attributeId, e.getMessage(), e);
            throw e;
        }
    }

    @Override
    public void updateAttribute(Connection conn, Map<String, Object> event) throws SQLException, Exception {
        final String updateAttribute = "UPDATE attribute_t SET attribute_desc = ?, attribute_type = ?," +
                "update_user = ?, update_ts = ? " +
                "WHERE host_id = ? AND attribute_id = ?";

        Map<String, Object> map = (Map<String, Object>)event.get(PortalConstants.DATA);
        String attributeId = (String)map.get("attributeId");
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

            statement.setObject(5, UUID.fromString((String)event.get(Constants.HOST)));
            statement.setString(6, attributeId);

            int count = statement.executeUpdate();
            if (count == 0) {
                throw new SQLException("no record is updated for attribute " + attributeId);
            }
        } catch (SQLException e) {
            logger.error("SQLException during updateAttribute for id {}: {}", attributeId, e.getMessage(), e);
            throw e;
        } catch (Exception e) {
            logger.error("Exception during updateAttribute for id {}: {}", attributeId, e.getMessage(), e);
            throw e;
        }
    }

    @Override
    public void deleteAttribute(Connection conn, Map<String, Object> event) throws SQLException, Exception {
        final String deleteGroup = "DELETE from attribute_t WHERE host_id = ? AND attribute_id = ?";
        Map<String, Object> map = (Map<String, Object>)event.get(PortalConstants.DATA);
        String attributeId = (String)map.get("attributeId");
        try (PreparedStatement statement = conn.prepareStatement(deleteGroup)) {
            statement.setObject(1, UUID.fromString((String)event.get(Constants.HOST)));
            statement.setString(2, attributeId);
            int count = statement.executeUpdate();
            if (count == 0) {
                throw new SQLException("no record is deleted for attribute " + attributeId);
            }
        } catch (SQLException e) {
            logger.error("SQLException during deleteAttribute for id {}: {}", attributeId, e.getMessage(), e);
            throw e;
        } catch (Exception e) {
            logger.error("Exception during deleteAttribute for id {}: {}", attributeId, e.getMessage(), e);
            throw e;
        }
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
        SqlUtil.addCondition(whereClause, parameters, "attribute_id", attributeId);
        SqlUtil.addCondition(whereClause, parameters, "attribute_type", attributeType);
        SqlUtil.addCondition(whereClause, parameters, "attribute_desc", attributeDesc);

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

        SqlUtil.addCondition(whereClause, parameters, "a.attribute_id", attributeId);
        SqlUtil.addCondition(whereClause, parameters, "a.attribute_type", attributeType);
        SqlUtil.addCondition(whereClause, parameters, "a.attribute_value", attributeValue);
        SqlUtil.addCondition(whereClause, parameters, "p.api_id", apiId);
        SqlUtil.addCondition(whereClause, parameters, "p.api_version", apiVersion);
        SqlUtil.addCondition(whereClause, parameters, "p.endpoint", endpoint);

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

        SqlUtil.addCondition(whereClause, parameters, "a.attribute_id", attributeId);
        SqlUtil.addCondition(whereClause, parameters, "a.attribute_type", attributeType);
        SqlUtil.addCondition(whereClause, parameters, "a.attribute_value", attributeValue);
        SqlUtil.addCondition(whereClause, parameters, "u.user_id", userId != null ? UUID.fromString(userId) : null);
        SqlUtil.addCondition(whereClause, parameters, "entity_id", entityId);
        SqlUtil.addCondition(whereClause, parameters, "u.email", email);
        SqlUtil.addCondition(whereClause, parameters, "u.first_name", firstName);
        SqlUtil.addCondition(whereClause, parameters, "u.last_name", lastName);
        SqlUtil.addCondition(whereClause, parameters, "u.user_type", userType);

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
    public void createAttributePermission(Connection conn, Map<String, Object> event) throws SQLException, Exception {
        final String insertGroup =
                """
                    INSERT INTO attribute_permission_t (host_id, attribute_id, attribute_value, endpoint_id, update_user, update_ts)
                    VALUES (
                        ?,
                        ?,
                        ?,
                        (SELECT e.endpoint_id
                         FROM api_endpoint_t e
                         JOIN api_version_t v ON e.host_id = v.host_id
                                             AND e.api_version_id = v.api_version_id
                         WHERE e.host_id = ?
                           AND v.api_id = ?
                           AND v.api_version = ?
                           AND e.endpoint = ?
                        ),
                        ?,
                        ?
                    )
                """;

        Map<String, Object> map = (Map<String, Object>)event.get(PortalConstants.DATA);
        String attributeId = (String)map.get("attributeId");
        try (PreparedStatement statement = conn.prepareStatement(insertGroup)) {
            statement.setObject(1, UUID.fromString((String)event.get(Constants.HOST)));
            statement.setString(2, attributeId);
            statement.setString(3, (String)map.get("attributeValue"));
            statement.setObject(4, UUID.fromString((String)event.get(Constants.HOST)));
            statement.setString(5, (String)map.get("apiId"));
            statement.setString(6, (String)map.get("apiVersion"));
            statement.setString(7, (String)map.get("endpoint"));
            statement.setString(8, (String)event.get(Constants.USER));
            statement.setObject(9, OffsetDateTime.parse((String)event.get(CloudEventV1.TIME)));

            int count = statement.executeUpdate();
            if (count == 0) {
                throw new SQLException("Failed to insert attribute permission " + attributeId);
            }
        } catch (SQLException e) {
            logger.error("SQLException during createAttributePermission for id {}: {}", attributeId, e.getMessage(), e);
            throw e;
        } catch (Exception e) {
            logger.error("Exception during createAttributePermission for id {}: {}", attributeId, e.getMessage(), e);
            throw e;
        }
    }

    @Override
    public void updateAttributePermission(Connection conn, Map<String, Object> event) throws SQLException, Exception {
        final String updateGroup = """
                UPDATE attribute_permission_t
                SET
                    attribute_value = ?,
                    update_user = ?,
                    update_ts = ?
                WHERE (host_id, attribute_id, endpoint_id) IN (
                    SELECT
                        ?,                      -- host_id
                        ?,                      -- attribute_id
                        e.endpoint_id
                    FROM
                        api_endpoint_t e
                    JOIN
                        api_version_t v ON e.host_id = v.host_id
                                       AND e.api_version_id = v.api_version_id
                    WHERE
                        e.host_id = ?
                        AND v.api_id = ?
                        AND v.api_version = ?
                        AND e.endpoint = ?
                )

                UPDATE attribute_permission_t SET attribute_value = ?, update_user = ?, update_ts = ?
                WHERE host_id = ? AND attribute_id = ? AND api_id = ? AND api_version = ? AND endpoint = ?
                """;

        Map<String, Object> map = (Map<String, Object>)event.get(PortalConstants.DATA);
        String attributeId = (String)map.get("attributeId");
        try (PreparedStatement statement = conn.prepareStatement(updateGroup)) {
            statement.setString(1, (String)map.get("attributeValue"));
            statement.setString(2, (String)event.get(Constants.USER));
            statement.setObject(3, OffsetDateTime.parse((String)event.get(CloudEventV1.TIME)));

            statement.setObject(4, UUID.fromString((String)event.get(Constants.HOST)));
            statement.setString(5, attributeId);
            statement.setObject(6, UUID.fromString((String)event.get(Constants.HOST)));

            statement.setString(7, (String)map.get("apiId"));
            statement.setString(8, (String)map.get("apiVersion"));
            statement.setString(9, (String)map.get("endpoint"));

            int count = statement.executeUpdate();
            if (count == 0) {
                throw new SQLException("no record is updated for attribute permission " + attributeId);
            }
        } catch (SQLException e) {
            logger.error("SQLException during updateAttributePermission for id {}: {}", attributeId, e.getMessage(), e);
            throw e;
        } catch (Exception e) {
            logger.error("Exception during updateAttributePermission for id {}: {}", attributeId, e.getMessage(), e);
            throw e;
        }
    }

    @Override
    public void deleteAttributePermission(Connection conn, Map<String, Object> event) throws SQLException, Exception {
        final String deleteGroup =
                """
                    DELETE FROM attribute_permission_t gp
                    WHERE gp.host_id = ?
                      AND gp.attribute_id = ?
                      AND gp.endpoint_id IN (
                        SELECT e.endpoint_id
                        FROM api_endpoint_t e
                        JOIN api_version_t v ON e.host_id = v.host_id
                                            AND e.api_version_id = v.api_version_id
                        WHERE e.host_id = ?
                          AND v.api_id = ?
                          AND v.api_version = ?
                          AND e.endpoint = ?
                      )
                """;
        Map<String, Object> map = (Map<String, Object>)event.get(PortalConstants.DATA);
        String attributeId = (String)map.get("attributeId");
        try (PreparedStatement statement = conn.prepareStatement(deleteGroup)) {
            statement.setObject(1, UUID.fromString((String)event.get(Constants.HOST)));
            statement.setString(2, attributeId);
            statement.setObject(3, UUID.fromString((String)event.get(Constants.HOST)));
            statement.setString(4, (String)map.get("apiId"));
            statement.setString(5, (String)map.get("apiVersion"));
            statement.setString(6, (String)map.get("endpoint"));

            int count = statement.executeUpdate();
            if (count == 0) {
                throw new SQLException("no record is deleted for attribute permission " + attributeId);
            }
        } catch (SQLException e) {
            logger.error("SQLException during deleteAttributePermission for id {}: {}", attributeId, e.getMessage(), e);
            throw e;
        } catch (Exception e) {
            logger.error("Exception during deleteAttributePermission for id {}: {}", attributeId, e.getMessage(), e);
            throw e;
        }
    }

    @Override
    public void createAttributeUser(Connection conn, Map<String, Object> event) throws SQLException, Exception {
        final String insertGroup = "INSERT INTO attribute_user_t (host_id, attribute_id, attribute_value, user_id, start_ts, end_ts, update_user, update_ts) " +
                "VALUES (?, ?, ?, ?, ?,  ?, ?, ?)";

        Map<String, Object> map = (Map<String, Object>)event.get(PortalConstants.DATA);
        String attributeId = (String)map.get("attributeId");
        try (PreparedStatement statement = conn.prepareStatement(insertGroup)) {
            statement.setObject(1, UUID.fromString((String)event.get(Constants.HOST)));
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
        } catch (SQLException e) {
            logger.error("SQLException during createAttributeUser for id {}: {}", attributeId, e.getMessage(), e);
            throw e;
        } catch (Exception e) {
            logger.error("Exception during createAttributeUser for id {}: {}", attributeId, e.getMessage(), e);
            throw e;
        }
    }

    @Override
    public void updateAttributeUser(Connection conn, Map<String, Object> event) throws SQLException, Exception {
        final String updateGroup = "UPDATE attribute_user_t SET attribute_value = ?, start_ts = ?, end_ts = ?, update_user = ?, update_ts = ? " +
                "WHERE host_id = ? AND attribute_id = ? AND user_id = ?";

        Map<String, Object> map = (Map<String, Object>)event.get(PortalConstants.DATA);
        String attributeId = (String)map.get("attributeId");
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
            statement.setObject(6, UUID.fromString((String)event.get(Constants.HOST)));
            statement.setString(7, attributeId);
            statement.setObject(8, UUID.fromString((String)event.get(Constants.USER)));

            int count = statement.executeUpdate();
            if (count == 0) {
                throw new SQLException("no record is updated for attribute user " + attributeId);
            }
        } catch (SQLException e) {
            logger.error("SQLException during updateAttributeUser for id {}: {}", attributeId, e.getMessage(), e);
            throw e;
        } catch (Exception e) {
            logger.error("Exception during updateAttributeUser for id {}: {}", attributeId, e.getMessage(), e);
            throw e;
        }
    }

    @Override
    public void deleteAttributeUser(Connection conn, Map<String, Object> event) throws SQLException, Exception {
        final String deleteGroup = "DELETE from attribute_user_t WHERE host_id = ? AND attribute_id = ? AND user_id = ?";
        Map<String, Object> map = (Map<String, Object>)event.get(PortalConstants.DATA);
        String attributeId = (String)map.get("attributeId");
        try (PreparedStatement statement = conn.prepareStatement(deleteGroup)) {
            statement.setObject(1, UUID.fromString((String)event.get(Constants.HOST)));
            statement.setString(2, attributeId);
            statement.setObject(3, UUID.fromString((String)event.get(Constants.USER)));

            int count = statement.executeUpdate();
            if (count == 0) {
                throw new SQLException("no record is deleted for attribute user " + attributeId);
            }
        } catch (SQLException e) {
            logger.error("SQLException during deleteAttributeUser for id {}: {}", attributeId, e.getMessage(), e);
            throw e;
        } catch (Exception e) {
            logger.error("Exception during deleteAttributeUser for id {}: {}", attributeId, e.getMessage(), e);
            throw e;
        }
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

        SqlUtil.addCondition(whereClause, parameters, "a.attribute_id", attributeId);
        SqlUtil.addCondition(whereClause, parameters, "p.attribute_value", attributeValue);
        SqlUtil.addCondition(whereClause, parameters, "p.api_id", apiId);
        SqlUtil.addCondition(whereClause, parameters, "p.api_version", apiVersion);
        SqlUtil.addCondition(whereClause, parameters, "p.endpoint", endpoint);

        if (!whereClause.isEmpty()) {
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
    public void createAttributeRowFilter(Connection conn, Map<String, Object> event) throws SQLException, Exception {
        final String insertGroup =
                """
                    INSERT INTO attribute_row_filter_t (
                        host_id,
                        attribute_id,
                        attribute_value,
                        endpoint_id,  -- Using resolved endpoint_id instead of direct API references
                        col_name,
                        operator,
                        col_value,
                        update_user,
                        update_ts
                    )
                    SELECT
                        ?,              -- host_id parameter (1st)
                        ?,              -- attribute_id parameter
                        ?,              -- attribute_value parameter
                        e.endpoint_id,  -- Resolved from join
                        ?,              -- col_name parameter
                        ?,              -- operator parameter
                        ?,              -- col_value parameter
                        ?,              -- update_user parameter
                        ?               -- update_ts parameter (or use DEFAULT)
                    FROM
                        api_endpoint_t e
                    JOIN
                        api_version_t v ON e.host_id = v.host_id
                                       AND e.api_version_id = v.api_version_id
                    WHERE
                        e.host_id = ?                  -- Same as first host_id parameter
                        AND v.api_id = ?               -- api_id parameter
                        AND v.api_version = ?          -- api_version parameter
                        AND e.endpoint = ?             -- endpoint parameter
                """;

        Map<String, Object> map = (Map<String, Object>)event.get(PortalConstants.DATA);
        String attributeId = (String)map.get("attributeId");
        try (PreparedStatement statement = conn.prepareStatement(insertGroup)) {
            statement.setObject(1, UUID.fromString((String)event.get(Constants.HOST)));
            statement.setString(2, attributeId);
            statement.setString(3, (String)map.get("attributeValue"));
            statement.setString(4, (String)map.get("colName"));
            statement.setString(5, (String)map.get("operator"));
            statement.setString(6, (String)map.get("colValue"));
            statement.setString(7, (String)event.get(Constants.USER));
            statement.setObject(8, OffsetDateTime.parse((String)event.get(CloudEventV1.TIME)));
            statement.setObject(9, UUID.fromString((String)event.get(Constants.HOST)));
            statement.setString(10, (String)map.get("apiId"));
            statement.setString(11, (String)map.get("apiVersion"));
            statement.setString(12, (String)map.get("endpoint"));

            int count = statement.executeUpdate();
            if (count == 0) {
                throw new SQLException("Failed to insert attribute row filter " + attributeId);
            }
        } catch (SQLException e) {
            logger.error("SQLException during createAttributeRowFilter for id {}: {}", attributeId, e.getMessage(), e);
            throw e;
        } catch (Exception e) {
            logger.error("Exception during createAttributeRowFilter for id {}: {}", attributeId, e.getMessage(), e);
            throw e;
        }
    }

    @Override
    public void updateAttributeRowFilter(Connection conn, Map<String, Object> event) throws SQLException, Exception {
        final String updateGroup =
                """
                        UPDATE attribute_row_filter_t arf
                        SET
                            attribute_value = ?,
                            endpoint_id = (
                                SELECT e.endpoint_id
                                FROM api_endpoint_t e
                                JOIN api_version_t v ON e.host_id = v.host_id
                                                   AND e.api_version_id = v.api_version_id
                                WHERE e.host_id = arf.host_id
                                  AND v.api_id = ?
                                  AND v.api_version = ?
                                  AND e.endpoint = ?
                            ),
                            col_name = ?,
                            operator = ?,
                            col_value = ?,
                            update_user = ?,
                            update_ts = ?
                        WHERE
                            arf.host_id = ?
                            AND arf.attribute_id = ?
                """;

        Map<String, Object> map = (Map<String, Object>)event.get(PortalConstants.DATA);
        String attributeId = (String)map.get("attributeId");
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
            statement.setObject(10, UUID.fromString((String)event.get(Constants.HOST)));
            statement.setString(11, attributeId);

            int count = statement.executeUpdate();
            if (count == 0) {
                throw new SQLException("no record is updated for attribute row filter " + attributeId);
            }
        } catch (SQLException e) {
            logger.error("SQLException during updateAttributeRowFilter for id {}: {}", attributeId, e.getMessage(), e);
            throw e;
        } catch (Exception e) {
            logger.error("Exception during updateAttributeRowFilter for id {}: {}", attributeId, e.getMessage(), e);
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
                      AND endpoint_id IN (
                          SELECT e.endpoint_id
                          FROM api_endpoint_t e
                          JOIN api_version_t v ON e.host_id = v.host_id
                                              AND e.api_version_id = v.api_version_id
                          WHERE e.host_id = ?
                            AND v.api_id = ?
                            AND v.api_version = ?
                            AND e.endpoint = ?
                      )
                """;
        Map<String, Object> map = (Map<String, Object>)event.get(PortalConstants.DATA);
        String attributeId = (String)map.get("attributeId");
        try (PreparedStatement statement = conn.prepareStatement(deleteGroup)) {
            statement.setObject(1, UUID.fromString((String)event.get(Constants.HOST)));
            statement.setString(2, attributeId);
            statement.setObject(3, UUID.fromString((String)event.get(Constants.HOST)));
            statement.setString(4, (String)map.get("apiId"));
            statement.setString(5, (String)map.get("apiVersion"));
            statement.setString(6, (String)map.get("endpoint"));

            int count = statement.executeUpdate();
            if (count == 0) {
                throw new SQLException("no record is deleted for attribute row filter " + attributeId);
            }
        } catch (SQLException e) {
            logger.error("SQLException during deleteAttributeRowFilter for id {}: {}", attributeId, e.getMessage(), e);
            throw e;
        } catch (Exception e) {
            logger.error("Exception during deleteAttributeRowFilter for id {}: {}", attributeId, e.getMessage(), e);
            throw e;
        }
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

        SqlUtil.addCondition(whereClause, parameters, "a.attribute_id", attributeId);
        SqlUtil.addCondition(whereClause, parameters, "p.attribute_value", attributeValue);
        SqlUtil.addCondition(whereClause, parameters, "p.api_id", apiId);
        SqlUtil.addCondition(whereClause, parameters, "p.api_version", apiVersion);
        SqlUtil.addCondition(whereClause, parameters, "p.endpoint", endpoint);

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
    public void createAttributeColFilter(Connection conn, Map<String, Object> event) throws SQLException, Exception {
        final String insertGroup =
                """
                    INSERT INTO attribute_col_filter_t (
                        host_id,
                        attribute_id,
                        attribute_value,
                        endpoint_id,  -- Using resolved endpoint_id instead of direct API references
                        columns,
                        update_user,
                        update_ts
                    )
                    SELECT
                        ?,              -- host_id parameter (1st)
                        ?,              -- attribute_id parameter
                        ?,              -- attribute_value parameter
                        e.endpoint_id,  -- Resolved from join
                        ?,              -- columns parameter
                        ?,              -- update_user parameter
                        ?               -- update_ts parameter (or use DEFAULT)
                    FROM
                        api_endpoint_t e
                    JOIN
                        api_version_t v ON e.host_id = v.host_id
                                       AND e.api_version_id = v.api_version_id
                    WHERE
                        e.host_id = ?                  -- Same as first host_id parameter
                        AND v.api_id = ?               -- api_id parameter
                        AND v.api_version = ?          -- api_version parameter
                        AND e.endpoint = ?             -- endpoint parameter
                """;

        Map<String, Object> map = (Map<String, Object>)event.get(PortalConstants.DATA);
        String attributeId = (String)map.get("attributeId");
        try (PreparedStatement statement = conn.prepareStatement(insertGroup)) {
            statement.setObject(1, UUID.fromString((String)event.get(Constants.HOST)));
            statement.setString(2, attributeId);
            statement.setString(3, (String)map.get("attributeValue"));
            statement.setString(4, (String)map.get("columns"));
            statement.setString(5, (String)event.get(Constants.USER));
            statement.setObject(6, OffsetDateTime.parse((String)event.get(CloudEventV1.TIME)));
            statement.setObject(7, UUID.fromString((String)event.get(Constants.HOST)));
            statement.setString(8, (String)map.get("apiId"));
            statement.setString(9, (String)map.get("apiVersion"));
            statement.setString(10, (String)map.get("endpoint"));

            int count = statement.executeUpdate();
            if (count == 0) {
                throw new SQLException("Failed to insert attribute col filter " + attributeId);
            }
        } catch (SQLException e) {
            logger.error("SQLException during createAttributeColFilter for id {}: {}", attributeId, e.getMessage(), e);
            throw e;
        } catch (Exception e) {
            logger.error("Exception during createAttributeColFilter for id {}: {}", attributeId, e.getMessage(), e);
            throw e;
        }
    }

    @Override
    public void updateAttributeColFilter(Connection conn, Map<String, Object> event) throws SQLException, Exception {
        final String updateGroup =
                """
                    UPDATE attribute_col_filter_t acf
                    SET
                        attribute_value = ?,
                        endpoint_id = (
                            SELECT e.endpoint_id
                            FROM api_endpoint_t e
                            JOIN api_version_t v ON e.host_id = v.host_id
                                               AND e.api_version_id = v.api_version_id
                            WHERE e.host_id = acf.host_id
                              AND v.api_id = ?
                              AND v.api_version = ?
                              AND e.endpoint = ?
                        ),
                        columns = ?,
                        update_user = ?,
                        update_ts = ?
                    WHERE
                        acf.host_id = ?
                        AND acf.attribute_id = ?
                """;

        Map<String, Object> map = (Map<String, Object>)event.get(PortalConstants.DATA);
        String attributeId = (String)map.get("attributeId");
        try (PreparedStatement statement = conn.prepareStatement(updateGroup)) {
            statement.setString(1, (String)map.get("attributeValue"));
            statement.setString(2, (String)map.get("apiId"));
            statement.setString(3, (String)map.get("apiVersion"));
            statement.setString(4, (String)map.get("endpoint"));
            statement.setString(5, (String)map.get("columns"));
            statement.setString(6, (String)event.get(Constants.USER));
            statement.setObject(7, OffsetDateTime.parse((String)event.get(CloudEventV1.TIME)));
            statement.setObject(8, UUID.fromString((String)event.get(Constants.HOST)));
            statement.setString(9, attributeId);

            int count = statement.executeUpdate();
            if (count == 0) {
                throw new SQLException("no record is updated for attribute col filter " + attributeId);
            }
        } catch (SQLException e) {
            logger.error("SQLException during updateAttributeColFilter for id {}: {}", attributeId, e.getMessage(), e);
            throw e;
        } catch (Exception e) {
            logger.error("Exception during updateAttributeColFilter for id {}: {}", attributeId, e.getMessage(), e);
            throw e;
        }
    }

    @Override
    public void deleteAttributeColFilter(Connection conn, Map<String, Object> event) throws SQLException, Exception {
        final String deleteGroup =
                """
                    DELETE FROM attribute_col_filter_t acf
                    WHERE
                        acf.host_id = ?
                        AND acf.attribute_id = ?
                        AND acf.endpoint_id IN (
                            SELECT e.endpoint_id
                            FROM api_endpoint_t e
                            JOIN api_version_t v ON e.host_id = v.host_id
                                               AND e.api_version_id = v.api_version_id
                            WHERE e.host_id = acf.host_id
                              AND v.api_id = ?
                              AND v.api_version = ?
                              AND e.endpoint = ?
                        )
                """;
        Map<String, Object> map = (Map<String, Object>)event.get(PortalConstants.DATA);
        String attributeId = (String)map.get("attributeId");
        try (PreparedStatement statement = conn.prepareStatement(deleteGroup)) {
            statement.setObject(1, UUID.fromString((String)map.get("hostId")));
            statement.setString(2, attributeId);
            statement.setString(3, (String)map.get("apiId"));
            statement.setString(4, (String)map.get("apiVersion"));
            statement.setString(5, (String)map.get("endpoint"));

            int count = statement.executeUpdate();
            if (count == 0) {
                throw new SQLException("no record is deleted for attribute col filter " + attributeId);
            }
        } catch (SQLException e) {
            logger.error("SQLException during deleteAttributeColFilter for id {}: {}", attributeId, e.getMessage(), e);
            throw e;
        } catch (Exception e) {
            logger.error("Exception during deleteAttributeColFilter for id {}: {}", attributeId, e.getMessage(), e);
            throw e;
        }
    }
}
