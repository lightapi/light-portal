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

public class HostOrgPersistenceImpl implements HostOrgPersistence {
    private static final Logger logger = LoggerFactory.getLogger(HostOrgPersistenceImpl.class);
    private static final String SQL_EXCEPTION = PortalDbProvider.SQL_EXCEPTION;
    private static final String GENERIC_EXCEPTION = PortalDbProvider.GENERIC_EXCEPTION;
    private static final String OBJECT_NOT_FOUND = PortalDbProvider.OBJECT_NOT_FOUND;

    private final NotificationService notificationService;

    public HostOrgPersistenceImpl(NotificationService notificationService) {
        this.notificationService = notificationService;
    }

    @Override
    public void createOrg(Connection conn, Map<String, Object> event) throws SQLException, Exception {
        final String insertOrg = "INSERT INTO org_t (domain, org_name, org_desc, org_owner, update_user, update_ts) " +
                "VALUES (?, ?, ?, ?, ?,  ?)";
        final String insertHost = "INSERT INTO host_t(host_id, domain, sub_domain, host_desc, host_owner, update_user, update_ts) " +
                "VALUES (?, ?, ?, ?, ?, ?, ?)";
        final String insertRole = "INSERT INTO role_t (host_id, role_id, role_desc, update_user, update_ts) " +
                "VALUES (?, ?, ?, ?, ?)";
        final String insertRoleUser = "INSERT INTO role_user_t (host_id, role_id, user_id, update_user, update_ts) " +
                "VALUES (?, ?, ?, ?, ?)";
        final String updateUserHost = "UPDATE user_host_t SET host_id = ?, update_user = ?, update_ts = ? WHERE user_id = ?";

        Map<String, Object> map = (Map<String, Object>)event.get(PortalConstants.DATA);
        String domain = (String)map.get("domain"); // For logging/exceptions
        String hostId = (String)event.get(Constants.HOST); // For logging/exceptions
        String orgOwner = (String)map.get("orgOwner"); // For logging/exceptions
        String hostOwner = (String)map.get("hostOwner"); // For logging/exceptions

        try (PreparedStatement statement = conn.prepareStatement(insertOrg)) {
            statement.setString(1, domain);
            statement.setString(2, (String)map.get("orgName"));
            statement.setString(3, (String)map.get("orgDesc"));
            statement.setString(4, orgOwner);  // org owner is the user id in the eventId
            statement.setString(5, (String)event.get(Constants.USER));
            statement.setObject(6, OffsetDateTime.parse((String)event.get(CloudEventV1.TIME)));
            int count = statement.executeUpdate();
            if (count == 0) {
                throw new SQLException("Failed to insert the org " + domain);
            }
            try (PreparedStatement hostStatement = conn.prepareStatement(insertHost)) {
                hostStatement.setObject(1, UUID.fromString(hostId));
                hostStatement.setString(2, domain);
                hostStatement.setString(3, (String)map.get("subDomain"));
                hostStatement.setString(4, (String)map.get("hostDesc"));
                hostStatement.setString(5, hostOwner); // host owner can be another person selected by the org owner.
                hostStatement.setString(6, (String)event.get(Constants.USER));
                hostStatement.setObject(7, OffsetDateTime.parse((String)event.get(CloudEventV1.TIME)));
                hostStatement.executeUpdate();
            }
            // create user, org-admin and host-admin roles for the hostId by default.
            try (PreparedStatement roleStatement = conn.prepareStatement(insertRole)) {
                roleStatement.setObject(1, UUID.fromString(hostId));
                roleStatement.setString(2, "user");
                roleStatement.setString(3, "user role");
                roleStatement.setString(4, (String)event.get(Constants.USER));
                roleStatement.setObject(5, OffsetDateTime.parse((String)event.get(CloudEventV1.TIME)));
                roleStatement.executeUpdate();
            }
            try (PreparedStatement roleStatement = conn.prepareStatement(insertRole)) {
                roleStatement.setObject(1, UUID.fromString(hostId));
                roleStatement.setString(2, "org-admin");
                roleStatement.setString(3, "org-admin role");
                roleStatement.setString(4, (String)event.get(Constants.USER));
                roleStatement.setObject(5, OffsetDateTime.parse((String)event.get(CloudEventV1.TIME)));
                roleStatement.executeUpdate();
            }
            try (PreparedStatement roleStatement = conn.prepareStatement(insertRole)) {
                roleStatement.setObject(1, UUID.fromString(hostId));
                roleStatement.setString(2, "host-admin");
                roleStatement.setString(3, "host-admin role");
                roleStatement.setString(4, (String)event.get(Constants.USER));
                roleStatement.setObject(5, OffsetDateTime.parse((String)event.get(CloudEventV1.TIME)));
                roleStatement.executeUpdate();
            }
            // insert role user to user for the host
            try (PreparedStatement roleUserStatement = conn.prepareStatement(insertRoleUser)) {
                roleUserStatement.setObject(1, UUID.fromString(hostId));
                roleUserStatement.setString(2, "user");
                roleUserStatement.setString(3, orgOwner);
                roleUserStatement.setString(4, (String)event.get(Constants.USER));
                roleUserStatement.setObject(5, OffsetDateTime.parse((String)event.get(CloudEventV1.TIME)));
                roleUserStatement.executeUpdate();
            }
            // insert role org-admin to user for the host
            try (PreparedStatement roleUserStatement = conn.prepareStatement(insertRoleUser)) {
                roleUserStatement.setObject(1, UUID.fromString(hostId));
                roleUserStatement.setString(2, "org-admin");
                roleUserStatement.setString(3, orgOwner);
                roleUserStatement.setString(4, (String)event.get(Constants.USER));
                roleUserStatement.setObject(5, OffsetDateTime.parse((String)event.get(CloudEventV1.TIME)));
                roleUserStatement.executeUpdate();
            }
            // insert host-admin to user for the host
            try (PreparedStatement roleUserStatement = conn.prepareStatement(insertRoleUser)) {
                roleUserStatement.setObject(1, UUID.fromString(hostId));
                roleUserStatement.setString(2, "host-admin");
                roleUserStatement.setString(3, hostOwner);
                roleUserStatement.setString(4, (String)event.get(Constants.USER));
                roleUserStatement.setObject(5, OffsetDateTime.parse((String)event.get(CloudEventV1.TIME)));
                roleUserStatement.executeUpdate();
            }
            // switch the current user to the hostId by updating to same user pointing to two hosts.
            try (PreparedStatement userHostStatement = conn.prepareStatement(updateUserHost)) {
                userHostStatement.setObject(1, UUID.fromString(hostId));
                userHostStatement.setString(2, (String)event.get(Constants.USER));
                userHostStatement.setObject(3, OffsetDateTime.parse((String)event.get(CloudEventV1.TIME)));
                userHostStatement.setString(4, orgOwner);

                userHostStatement.executeUpdate();
            }
            notificationService.insertNotification(event, true, null);
        } catch (SQLException e) {
            logger.error("SQLException during createOrg for domain {}: {}", domain, e.getMessage(), e);
            notificationService.insertNotification(event, false, e.getMessage());
            throw e; // Re-throw SQLException
        } catch (Exception e) {
            logger.error("Exception during createOrg for domain {}: {}", domain, e.getMessage(), e);
            notificationService.insertNotification(event, false, e.getMessage());
            throw e; // Re-throw generic Exception
        }
    }

    @Override
    public void updateOrg(Connection conn, Map<String, Object> event) throws SQLException, Exception {
        final String updateOrgSql = "UPDATE org_t SET org_name = ?, org_desc = ?, org_owner = ?, " +
                "update_user = ?, update_ts = ? " +
                "WHERE domain = ?";

        Map<String, Object> map = (Map<String, Object>)event.get(PortalConstants.DATA);
        String domain = (String)map.get("domain"); // For logging/exceptions

        try (PreparedStatement statement = conn.prepareStatement(updateOrgSql)) {
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
            statement.setString(6, domain);

            int count = statement.executeUpdate();
            if (count == 0) {
                // no record is updated, write an error notification.
                throw new SQLException("no record is updated for org " + domain);
            }
            notificationService.insertNotification(event, true, null);
        } catch (SQLException e) {
            logger.error("SQLException during updateOrg for domain {}: {}", domain, e.getMessage(), e);
            notificationService.insertNotification(event, false, e.getMessage());
            throw e; // Re-throw SQLException
        } catch (Exception e) {
            logger.error("Exception during updateOrg for domain {}: {}", domain, e.getMessage(), e);
            notificationService.insertNotification(event, false, e.getMessage());
            throw e; // Re-throw generic Exception
        }
    }

    @Override
    public void deleteOrg(Connection conn, Map<String, Object> event) throws SQLException, Exception {
        final String deleteOrgSql = "DELETE FROM org_t WHERE domain = ?";
        Map<String, Object> map = (Map<String, Object>)event.get(PortalConstants.DATA);
        String domain = (String)map.get("domain"); // For logging/exceptions

        try (PreparedStatement statement = conn.prepareStatement(deleteOrgSql)) {
            statement.setString(1, domain);
            int count = statement.executeUpdate();
            if (count == 0) {
                throw new SQLException("no record is deleted for org " + domain);
            }
            notificationService.insertNotification(event, true, null);
        } catch (SQLException e) {
            logger.error("SQLException during deleteOrg for domain {}: {}", domain, e.getMessage(), e);
            notificationService.insertNotification(event, false, e.getMessage());
            throw e; // Re-throw SQLException
        } catch (Exception e) {
            logger.error("Exception during deleteOrg for domain {}: {}", domain, e.getMessage(), e);
            notificationService.insertNotification(event, false, e.getMessage());
            throw e; // Re-throw generic Exception
        }
    }

    @Override
    public void createHost(Connection conn, Map<String, Object> event) throws SQLException, Exception {
        final String insertHost = "INSERT INTO host_t (host_id, domain, sub_domain, host_desc, host_owner, update_user, update_ts) " +
                "VALUES (?, ?, ?, ?, ?,  ?, ?)";
        Map<String, Object> map = (Map<String, Object>)event.get(PortalConstants.DATA);
        String hostId = (String)event.get(Constants.HOST); // For logging/exceptions
        String domain = (String)map.get("domain"); // For logging/exceptions

        try (PreparedStatement statement = conn.prepareStatement(insertHost)) {
            statement.setObject(1, UUID.fromString(hostId));
            statement.setString(2, domain);
            statement.setString(3, (String)map.get("subDomain"));
            statement.setString(4, (String)map.get("hostDesc"));
            statement.setString(5, (String)map.get("hostOwner"));
            statement.setString(6, (String)event.get(Constants.USER));
            statement.setObject(7, OffsetDateTime.parse((String)event.get(CloudEventV1.TIME)));
            int count = statement.executeUpdate();
            if (count == 0) {
                throw new SQLException("Failed to insert the host " + domain);
            }
            notificationService.insertNotification(event, true, null);
        } catch (SQLException e) {
            logger.error("SQLException during createHost for hostId {}: {}", hostId, e.getMessage(), e);
            notificationService.insertNotification(event, false, e.getMessage());
            throw e; // Re-throw SQLException
        } catch (Exception e) {
            logger.error("Exception during createHost for hostId {}: {}", hostId, e.getMessage(), e);
            notificationService.insertNotification(event, false, e.getMessage());
            throw e; // Re-throw generic Exception
        }
    }

    @Override
    public void updateHost(Connection conn, Map<String, Object> event) throws SQLException, Exception {
        final String updateHostSql = "UPDATE host_t SET domain = ?, sub_domain = ?, host_desc = ?, host_owner = ?, " +
                "update_user = ?, update_ts = ? " +
                "WHERE host_id = ?";

        Map<String, Object> map = (Map<String, Object>)event.get(PortalConstants.DATA);
        String hostId = (String)event.get(Constants.HOST); // For logging/exceptions

        try (PreparedStatement statement = conn.prepareStatement(updateHostSql)) {
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
            statement.setObject(7, UUID.fromString(hostId));

            int count = statement.executeUpdate();
            if (count == 0) {
                // no record is updated, write an error notification.
                throw new SQLException("no record is updated for host " + hostId);
            }
            notificationService.insertNotification(event, true, null);
        } catch (SQLException e) {
            logger.error("SQLException during updateHost for hostId {}: {}", hostId, e.getMessage(), e);
            notificationService.insertNotification(event, false, e.getMessage());
            throw e; // Re-throw SQLException
        } catch (Exception e) {
            logger.error("Exception during updateHost for hostId {}: {}", hostId, e.getMessage(), e);
            notificationService.insertNotification(event, false, e.getMessage());
            throw e; // Re-throw generic Exception
        }
    }

    @Override
    public void deleteHost(Connection conn, Map<String, Object> event) throws SQLException, Exception {
        final String deleteHostSql = "DELETE from host_t WHERE host_id = ?";
        Map<String, Object> map = (Map<String, Object>)event.get(PortalConstants.DATA);
        String hostId = (String)event.get(Constants.HOST); // For logging/exceptions

        try (PreparedStatement statement = conn.prepareStatement(deleteHostSql)) {
            statement.setObject(1, UUID.fromString(hostId));
            int count = statement.executeUpdate();
            if (count == 0) {
                throw new SQLException("no record is deleted for host " + hostId);
            }
            notificationService.insertNotification(event, true, null);
        } catch (SQLException e) {
            logger.error("SQLException during deleteHost for hostId {}: {}", hostId, e.getMessage(), e);
            notificationService.insertNotification(event, false, e.getMessage());
            throw e; // Re-throw SQLException
        } catch (Exception e) {
            logger.error("Exception during deleteHost for hostId {}: {}", hostId, e.getMessage(), e);
            notificationService.insertNotification(event, false, e.getMessage());
            throw e; // Re-throw generic Exception
        }
    }

    @Override
    public void switchHost(Connection conn, Map<String, Object> event) throws SQLException, Exception {
        final String updateUserHost = "UPDATE user_host_t SET host_id = ?, update_user = ?, update_ts = ? WHERE user_id = ?";
        String userId = (String)event.get(Constants.USER); // For logging/exceptions

        try (PreparedStatement statement = conn.prepareStatement(updateUserHost)) {
            statement.setObject(1, UUID.fromString((String)event.get(Constants.HOST)));
            statement.setString(2, userId);
            statement.setObject(3, OffsetDateTime.parse((String)event.get(CloudEventV1.TIME)));
            statement.setObject(4, UUID.fromString(userId)); // user_id from event itself, not from data map.
            int count = statement.executeUpdate();
            if (count == 0) {
                throw new SQLException("no record is updated for user " + userId);
            }
            notificationService.insertNotification(event, true, null);
        } catch (SQLException e) {
            logger.error("SQLException during switchHost for userId {}: {}", userId, e.getMessage(), e);
            notificationService.insertNotification(event, false, e.getMessage());
            throw e; // Re-throw SQLException
        } catch (Exception e) {
            logger.error("Exception during switchHost for userId {}: {}", userId, e.getMessage(), e);
            notificationService.insertNotification(event, false, e.getMessage());
            throw e; // Re-throw generic Exception
        }
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
        // Original query seems to select from 'host_t' but refer to 'org_owner' and 'host_domain' directly which may not be in 'host_t'.
        // Assuming 'org_owner' might exist as 'host_owner' and 'host_domain' is derivable.
        // For now, retaining the original query for direct modification as per instructions, but this might be a data model mismatch.
        // Re-aligning with current method signature and interface for `queryHostByOwner` which expects `Map<String, Object>`
        final String queryHostByOwner = "SELECT host_id, domain, sub_domain, host_desc, host_owner, update_user, update_ts " +
                "FROM host_t WHERE host_owner = ?"; // Changed org_owner to host_owner for host_t table

        Result<Map<String, Object>> result;
        try (final Connection conn = ds.getConnection()) {
            Map<String, Object> map = new HashMap<>();
            try (PreparedStatement statement = conn.prepareStatement(queryHostByOwner)) {
                statement.setString(1, owner);
                try (ResultSet resultSet = statement.executeQuery()) {
                    if (resultSet.next()) {
                        map.put("hostId", resultSet.getObject("host_id", UUID.class));
                        map.put("domain", resultSet.getString("domain"));
                        map.put("subDomain", resultSet.getString("sub_domain"));
                        map.put("hostDesc", resultSet.getString("host_desc"));
                        map.put("hostOwner", resultSet.getString("host_owner"));
                        map.put("updateUser", resultSet.getString("update_user"));
                        map.put("updateTs", resultSet.getObject("update_ts") != null ? resultSet.getObject("update_ts", OffsetDateTime.class) : null);
                        // Original had "jwk" which is not in host_t. Removed it to prevent SQLException.
                        // map.put("jwk", resultSet.getString("jwk"));
                    }
                }
            }
            if (map.isEmpty()) // Changed map.size() == 0 to map.isEmpty() for clarity
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

        SqlUtil.addCondition(whereClause, parameters, "domain", domain);
        SqlUtil.addCondition(whereClause, parameters, "org_name", orgName);
        SqlUtil.addCondition(whereClause, parameters, "org_desc", orgDesc);
        SqlUtil.addCondition(whereClause, parameters, "org_owner", orgOwner);

        if (!whereClause.isEmpty()) {
            sqlBuilder.append("AND ").append(whereClause);
        }

        sqlBuilder.append(" ORDER BY domain\n" +
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
                    map.put("updateTs", resultSet.getObject("update_ts") != null ? resultSet.getObject("update_ts", OffsetDateTime.class) : null);
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

        SqlUtil.addCondition(whereClause, parameters, "host_id", hostId != null ? UUID.fromString(hostId) : null);
        SqlUtil.addCondition(whereClause, parameters, "domain", domain);
        SqlUtil.addCondition(whereClause, parameters, "sub_domain", subDomain);
        SqlUtil.addCondition(whereClause, parameters, "host_desc", hostDesc);
        SqlUtil.addCondition(whereClause, parameters, "host_owner", hostOwner);

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
                    map.put("updateTs", resultSet.getObject("update_ts") != null ? resultSet.getObject("update_ts", OffsetDateTime.class) : null);
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
        SqlUtil.addCondition(whereClause, parameters, "domain", domain);
        SqlUtil.addCondition(whereClause, parameters, "sub_domain", subDomain);
        SqlUtil.addCondition(whereClause, parameters, "host_desc", hostDesc);

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
                    map.put("updateTs", resultSet.getObject("update_ts") != null ? resultSet.getObject("update_ts", OffsetDateTime.class) : null);
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
}
