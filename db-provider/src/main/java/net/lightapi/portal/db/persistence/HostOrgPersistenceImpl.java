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
import static java.sql.Types.NULL;
import static net.lightapi.portal.db.util.SqlUtil.*;

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
        final String insertOrg = "INSERT INTO org_t (domain, org_name, org_desc, org_owner, update_user, update_ts, aggregate_version) " +
                "VALUES (?, ?, ?, ?, ?,  ?, ?)";
        final String insertHost = "INSERT INTO host_t(host_id, domain, sub_domain, host_desc, host_owner, update_user, update_ts, aggregate_version) " +
                "VALUES (?, ?, ?, ?, ?,  ?, ?, ?)";
        final String insertRole = "INSERT INTO role_t (host_id, role_id, role_desc, update_user, update_ts, aggregate_version) " +
                "VALUES (?, ?, ?, ?, ?,  ?)";
        final String insertRoleUser = "INSERT INTO role_user_t (host_id, role_id, user_id, update_user, update_ts, aggregate_version) " +
                "VALUES (?, ?, ?, ?, ?,  ?)";
        final String updateUserHost = "UPDATE user_host_t SET host_id = ?, update_user = ?, update_ts = ? WHERE user_id = ?";

        Map<String, Object> map = SqlUtil.extractEventData(event);
        String domain = (String)map.get("domain");
        String hostId = (String)map.get("hostId"); // enriched in the service.
        String orgOwner = (String)map.get("orgOwner");
        String hostOwner = (String)map.get("hostOwner");
        long newAggregateVersion = SqlUtil.getNewAggregateVersion(event);

        try (PreparedStatement statement = conn.prepareStatement(insertOrg)) {
            statement.setString(1, domain);
            statement.setString(2, (String)map.get("orgName"));
            statement.setString(3, (String)map.get("orgDesc"));
            statement.setObject(4, UUID.fromString(orgOwner));
            statement.setString(5, (String)event.get(Constants.USER));
            statement.setObject(6, OffsetDateTime.parse((String)event.get(CloudEventV1.TIME)));
            statement.setLong(7, newAggregateVersion);
            int count = statement.executeUpdate();
            if (count == 0) {
                throw new SQLException("Failed to insert the org " + domain);
            }
            try (PreparedStatement hostStatement = conn.prepareStatement(insertHost)) {
                hostStatement.setObject(1, UUID.fromString(hostId));
                hostStatement.setString(2, domain);
                hostStatement.setString(3, (String)map.get("subDomain"));
                hostStatement.setString(4, (String)map.get("hostDesc"));
                hostStatement.setObject(5, UUID.fromString(hostOwner));
                hostStatement.setString(6, (String)event.get(Constants.USER));
                hostStatement.setObject(7, OffsetDateTime.parse((String)event.get(CloudEventV1.TIME)));
                hostStatement.setLong(8, newAggregateVersion);
                hostStatement.executeUpdate();
            }
            // create user, org-admin and host-admin roles for the hostId by default.
            try (PreparedStatement roleStatement = conn.prepareStatement(insertRole)) {
                roleStatement.setObject(1, UUID.fromString(hostId));
                roleStatement.setString(2, "user");
                roleStatement.setString(3, "user role");
                roleStatement.setString(4, (String)event.get(Constants.USER));
                roleStatement.setObject(5, OffsetDateTime.parse((String)event.get(CloudEventV1.TIME)));
                roleStatement.setLong(6, newAggregateVersion);
                roleStatement.executeUpdate();
            }
            try (PreparedStatement roleStatement = conn.prepareStatement(insertRole)) {
                roleStatement.setObject(1, UUID.fromString(hostId));
                roleStatement.setString(2, "org-admin");
                roleStatement.setString(3, "org-admin role");
                roleStatement.setString(4, (String)event.get(Constants.USER));
                roleStatement.setObject(5, OffsetDateTime.parse((String)event.get(CloudEventV1.TIME)));
                roleStatement.setLong(6, newAggregateVersion);
                roleStatement.executeUpdate();
            }
            try (PreparedStatement roleStatement = conn.prepareStatement(insertRole)) {
                roleStatement.setObject(1, UUID.fromString(hostId));
                roleStatement.setString(2, "host-admin");
                roleStatement.setString(3, "host-admin role");
                roleStatement.setString(4, (String)event.get(Constants.USER));
                roleStatement.setObject(5, OffsetDateTime.parse((String)event.get(CloudEventV1.TIME)));
                roleStatement.setLong(6, newAggregateVersion);
                roleStatement.executeUpdate();
            }
            // insert role user to user for the host
            try (PreparedStatement roleUserStatement = conn.prepareStatement(insertRoleUser)) {
                roleUserStatement.setObject(1, UUID.fromString(hostId));
                roleUserStatement.setString(2, "user");
                roleUserStatement.setObject(3, UUID.fromString(orgOwner));
                roleUserStatement.setString(4, (String)event.get(Constants.USER));
                roleUserStatement.setObject(5, OffsetDateTime.parse((String)event.get(CloudEventV1.TIME)));
                roleUserStatement.setLong(6, newAggregateVersion);
                roleUserStatement.executeUpdate();
            }
            // insert role org-admin to user for the host
            try (PreparedStatement roleUserStatement = conn.prepareStatement(insertRoleUser)) {
                roleUserStatement.setObject(1, UUID.fromString(hostId));
                roleUserStatement.setString(2, "org-admin");
                roleUserStatement.setObject(3, UUID.fromString(orgOwner));
                roleUserStatement.setString(4, (String)event.get(Constants.USER));
                roleUserStatement.setObject(5, OffsetDateTime.parse((String)event.get(CloudEventV1.TIME)));
                roleUserStatement.setLong(6, newAggregateVersion);
                roleUserStatement.executeUpdate();
            }
            // insert host-admin to user for the host
            try (PreparedStatement roleUserStatement = conn.prepareStatement(insertRoleUser)) {
                roleUserStatement.setObject(1, UUID.fromString(hostId));
                roleUserStatement.setString(2, "host-admin");
                roleUserStatement.setObject(3, UUID.fromString(hostOwner));
                roleUserStatement.setString(4, (String)event.get(Constants.USER));
                roleUserStatement.setObject(5, OffsetDateTime.parse((String)event.get(CloudEventV1.TIME)));
                roleUserStatement.setLong(6, newAggregateVersion);
                roleUserStatement.executeUpdate();
            }
            // switch the current user to the hostId by updating to same user pointing to two hosts.
            try (PreparedStatement userHostStatement = conn.prepareStatement(updateUserHost)) {
                userHostStatement.setObject(1, UUID.fromString(hostId));
                userHostStatement.setString(2, (String)event.get(Constants.USER));
                userHostStatement.setObject(3, OffsetDateTime.parse((String)event.get(CloudEventV1.TIME)));
                userHostStatement.setObject(4, UUID.fromString(orgOwner));

                userHostStatement.executeUpdate();
            }
        } catch (SQLException e) {
            logger.error("SQLException during createOrg for domain {} aggregateVersion {}: {}", domain, newAggregateVersion, e.getMessage(), e);
            throw e;
        } catch (Exception e) {
            logger.error("Exception during createOrg for domain {} aggregateVersion {}: {}", domain, newAggregateVersion, e.getMessage(), e);
            throw e;
        }
    }

    private boolean queryOrgExists(Connection conn, String domain) throws SQLException {
        final String sql =
                """
                SELECT COUNT(*) FROM org_t WHERE domain = ?
                """;
        try (PreparedStatement pst = conn.prepareStatement(sql)) {
            pst.setString(1, domain);
            try (ResultSet rs = pst.executeQuery()) {
                return rs.next() && rs.getInt(1) > 0;
            }
        }
    }

    @Override
    public void updateOrg(Connection conn, Map<String, Object> event) throws SQLException, Exception {
        final String updateOrgSql = "UPDATE org_t SET org_name = ?, org_desc = ?, org_owner = ?, " +
                "update_user = ?, update_ts = ?, aggregate_version = ? " +
                "WHERE domain = ? AND aggregate_version = ?";

        Map<String, Object> map = (Map<String, Object>)event.get(PortalConstants.DATA);
        String domain = (String)map.get("domain");
        long oldAggregateVersion = SqlUtil.getOldAggregateVersion(event);
        long newAggregateVersion = SqlUtil.getNewAggregateVersion(event);

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
                statement.setObject(3, UUID.fromString(orgOwner));
            } else {
                statement.setNull(3, NULL);
            }
            statement.setString(4, (String)event.get(Constants.USER));
            statement.setObject(5, OffsetDateTime.parse((String)event.get(CloudEventV1.TIME)));
            statement.setLong(6, newAggregateVersion);
            statement.setString(7, domain);
            statement.setLong(8, oldAggregateVersion);

            int count = statement.executeUpdate();
            if (count == 0) {
                if (queryOrgExists(conn, domain)) {
                    throw new ConcurrencyException("Optimistic concurrency conflict during updateOrg for domain " + domain + ". Expected version " + oldAggregateVersion + " but found a different version " + newAggregateVersion + ".");
                } else {
                    throw new SQLException("No record found during updateOrg for domain " + domain + ".");
                }
            }
        } catch (SQLException e) {
            logger.error("SQLException during updateOrg for domain {} (old: {}) -> (new: {}): {}", domain, oldAggregateVersion, newAggregateVersion, e.getMessage(), e);
            throw e;
        } catch (Exception e) {
            logger.error("Exception during updateOrg for domain {} (old: {}) -> (new: {}): {}", domain, oldAggregateVersion, newAggregateVersion, e.getMessage(), e);
            throw e;
        }
    }

    @Override
    public void deleteOrg(Connection conn, Map<String, Object> event) throws SQLException, Exception {
        final String deleteOrgSql = "DELETE FROM org_t WHERE domain = ? AND aggregate_version = ?";
        Map<String, Object> map = (Map<String, Object>)event.get(PortalConstants.DATA);
        String domain = (String)map.get("domain"); // For logging/exceptions
        long oldAggregateVersion = SqlUtil.getOldAggregateVersion(event);

        try (PreparedStatement statement = conn.prepareStatement(deleteOrgSql)) {
            statement.setString(1, domain);
            statement.setLong(2, oldAggregateVersion);
            int count = statement.executeUpdate();
            if (count == 0) {
                if (queryOrgExists(conn, domain)) {
                    throw new ConcurrencyException("Optimistic concurrency conflict during deleteOrg for domain " + domain + " aggregateVersion " + oldAggregateVersion + " but found a different version or already updated.");
                } else {
                    throw new SQLException("No record found during deleteOrg for domain " + domain + ". It might have been already deleted.");
                }
            }
        } catch (SQLException e) {
            logger.error("SQLException during deleteOrg for domain {} aggregateVersion {}: {}", domain, oldAggregateVersion, e.getMessage(), e);
            throw e; // Re-throw SQLException
        } catch (Exception e) {
            logger.error("Exception during deleteOrg for domain {} aggregateVersion {}: {}", domain, oldAggregateVersion, e.getMessage(), e);
            throw e; // Re-throw generic Exception
        }
    }

    @Override
    public void createHost(Connection conn, Map<String, Object> event) throws SQLException, Exception {
        final String insertHost =
                """
                INSERT INTO host_t (host_id, domain, sub_domain, host_desc, host_owner, update_user, update_ts, aggregate_version)
                VALUES (?, ?, ?, ?, ?,  ?, ?, ?)
                """;
        Map<String, Object> map = (Map<String, Object>)event.get(PortalConstants.DATA);
        String hostId = (String)map.get("hostId"); // enriched in the service.
        String domain = (String)map.get("domain");
        String hostOwner = (String)map.get("hostOwner");
        long newAggregateVersion = SqlUtil.getNewAggregateVersion(event);

        try (PreparedStatement statement = conn.prepareStatement(insertHost)) {
            statement.setObject(1, UUID.fromString(hostId));
            statement.setString(2, domain);
            statement.setString(3, (String)map.get("subDomain"));
            statement.setString(4, (String)map.get("hostDesc"));
            statement.setObject(5, UUID.fromString(hostOwner));
            statement.setString(6, (String)event.get(Constants.USER));
            statement.setObject(7, OffsetDateTime.parse((String)event.get(CloudEventV1.TIME)));
            statement.setLong(8, newAggregateVersion);
            int count = statement.executeUpdate();
            if (count == 0) {
                throw new SQLException(String.format("Failed during createHost for hostId %s with aggregateVersion %d", hostId, newAggregateVersion));
            }
        } catch (SQLException e) {
            logger.error("SQLException during createHost for hostId {} aggregateVersion {}: {}", hostId, newAggregateVersion, e.getMessage(), e);
            throw e; // Re-throw SQLException
        } catch (Exception e) {
            logger.error("Exception during createHost for hostId {} aggregateVersion {}: {}", hostId, newAggregateVersion, e.getMessage(), e);
            throw e; // Re-throw generic Exception
        }
    }

    private boolean queryHostExists(Connection conn, String hostId) throws SQLException {
        final String sql =
                """
                SELECT COUNT(*) FROM host_t WHERE host_id = ?
                """;
        try (PreparedStatement pst = conn.prepareStatement(sql)) {
            pst.setObject(1, UUID.fromString(hostId));
            try (ResultSet rs = pst.executeQuery()) {
                return rs.next() && rs.getInt(1) > 0;
            }
        }
    }

    @Override
    public void updateHost(Connection conn, Map<String, Object> event) throws SQLException, Exception {
        final String updateHostSql =
                """
                UPDATE host_t SET host_desc = ?, host_owner = ?,
                update_user = ?, update_ts = ?, aggregate_version = ?
                WHERE host_id = ? AND aggregate_version = ?
                """;

        Map<String, Object> map = (Map<String, Object>)event.get(PortalConstants.DATA);
        String hostId = (String)map.get("currentHostId");
        long oldAggregateVersion = SqlUtil.getOldAggregateVersion(event);
        long newAggregateVersion = SqlUtil.getNewAggregateVersion(event);

        try (PreparedStatement statement = conn.prepareStatement(updateHostSql)) {
            String hostDesc = (String)map.get("hostDesc");
            if (hostDesc != null && !hostDesc.isEmpty()) {
                statement.setString(1, hostDesc);
            } else {
                statement.setNull(1, NULL);
            }
            String hostOwner = (String)map.get("hostOwner");
            if (hostOwner != null && !hostOwner.isEmpty()) {
                statement.setObject(2, UUID.fromString(hostOwner));
            } else {
                statement.setNull(2, NULL);
            }
            statement.setString(3, (String)event.get(Constants.USER));
            statement.setObject(4, OffsetDateTime.parse((String)event.get(CloudEventV1.TIME)));
            statement.setLong(5, newAggregateVersion);
            statement.setObject(6, UUID.fromString(hostId));
            statement.setLong(7, oldAggregateVersion);

            int count = statement.executeUpdate();
            if (count == 0) {
                if (queryHostExists(conn, hostId)) {
                    throw new ConcurrencyException("Optimistic concurrency conflict during updateHost for hostId " + hostId + ". Expected version " + oldAggregateVersion + " but found a different version " + newAggregateVersion + ".");
                } else {
                    throw new SQLException("No record found during updateHost for hostId " + hostId + ".");
                }
            }
        } catch (SQLException e) {
            logger.error("SQLException during updateHost for hostId {} (old: {}) -> (new: {}): {}", hostId, oldAggregateVersion, newAggregateVersion, e.getMessage(), e);
            throw e;
        } catch (Exception e) {
            logger.error("Exception during updateHost for hostId {} (old: {}) -> (new: {}): {}", hostId, oldAggregateVersion, newAggregateVersion, e.getMessage(), e);
            throw e;
        }
    }

    @Override
    public void deleteHost(Connection conn, Map<String, Object> event) throws SQLException, Exception {
        final String deleteHostSql = "DELETE from host_t WHERE host_id = ? AND aggregate_version = ?";
        Map<String, Object> map = (Map<String, Object>)event.get(PortalConstants.DATA);
        String hostId = (String)map.get("currentHostId");
        long oldAggregateVersion = SqlUtil.getOldAggregateVersion(event);

        try (PreparedStatement statement = conn.prepareStatement(deleteHostSql)) {
            statement.setObject(1, UUID.fromString(hostId));
            statement.setLong(2, oldAggregateVersion);
            int count = statement.executeUpdate();
            if (count == 0) {
                if (queryHostExists(conn, hostId)) {
                    throw new ConcurrencyException("Optimistic concurrency conflict during deleteHost for hostId " + hostId + " aggregateVersion " + oldAggregateVersion + " but found a different version or already updated.");
                } else {
                    throw new SQLException("No record found during deleteHost for hostId " + hostId + ". It might have been already deleted.");
                }
            }
        } catch (SQLException e) {
            logger.error("SQLException during deleteHost for hostId {} aggregateVersion {}: {}", hostId, oldAggregateVersion, e.getMessage(), e);
            throw e;
        } catch (Exception e) {
            logger.error("Exception during deleteHost for hostId {}: {} aggregateVersion {}", hostId, oldAggregateVersion, e.getMessage(), e);
            throw e;
        }
    }

    @Override
    public void switchUserHost(Connection conn, Map<String, Object> event) throws SQLException, Exception {
        final String deactivateCurrentUserHost =
                """
                UPDATE user_host_t
                SET current = false,
                update_user = ?,
                update_ts = ?
                WHERE user_id = ?
                AND current = true
                """;
        final String activateNewHost =
                """
                UPDATE user_host_t
                SET current = true,
                update_user = ?,
                update_ts = ?,
                aggregate_version = ?
                WHERE user_id = ?
                AND aggregate_version = ?
                AND host_id = ?
                """;
        final String updateEmployeeHost =
                """
                UPDATE employee_t
                SET host_id = ?,
                update_user = ?,
                update_ts = ?
                WHERE user_id = ?
                """;
        final String updateCustomerHost =
                """
                UPDATE customer_t
                SET host_id = ?,
                update_user = ?,
                update_ts = ?
                WHERE user_id = ?
                """;

        Map<String, Object> map = (Map<String, Object>)event.get(PortalConstants.DATA);
        String hostId = (String)map.get("hostId");
        String userId = (String)event.get(Constants.USER);
        long oldAggregateVersion = SqlUtil.getOldAggregateVersion(event);
        long newAggregateVersion = SqlUtil.getNewAggregateVersion(event);
        String updateTs = (String) event.get(CloudEventV1.TIME);

        try (PreparedStatement statement = conn.prepareStatement(deactivateCurrentUserHost)) {
            statement.setString(1, userId);
            statement.setObject(2, OffsetDateTime.parse(updateTs));
            statement.setObject(3, UUID.fromString(userId));
            int count = statement.executeUpdate();
            if (count > 0) {
                logger.debug("Deactivated {} current host(s) for user {}", count, userId);
            }
            // There is a chance that there is no current host for the user, so there is concurrency check here.
        } catch (SQLException e) {
            logger.error("SQLException during switchHost for hostId {} userId {} (old: {}) -> (new: {}): {}", hostId, userId, oldAggregateVersion, newAggregateVersion, e.getMessage(), e);
            throw e;
        } catch (Exception e) {
            logger.error("Exception during switchHost for hoarIs {} userId {} (old: {}) -> (new: {}): {}", hostId, userId, oldAggregateVersion, newAggregateVersion, e.getMessage(), e);
            throw e;
        }
        try (PreparedStatement activateStmt = conn.prepareStatement(activateNewHost)) {
            activateStmt.setString(1, userId);
            activateStmt.setObject(2, OffsetDateTime.parse(updateTs));
            activateStmt.setLong(3, newAggregateVersion);
            activateStmt.setObject(4, UUID.fromString(userId));
            activateStmt.setLong(5, oldAggregateVersion);
            activateStmt.setObject(6, UUID.fromString(hostId));

            int activateCount = activateStmt.executeUpdate();

            if (activateCount == 0) {
                if (queryUserHostExists(conn, hostId, userId)) {
                    throw new ConcurrencyException("Optimistic concurrency conflict during switchHost for hostId " + hostId + " userId " + userId + ". Expected version " + oldAggregateVersion + " but found a different version.");
                } else {
                    throw new SQLException("No record found during switchHost for hostId " + hostId + " userId " + userId + ".");
                }
            } else {
                // User switched to a new hostId. Update the employee_t and customer_t
                try (PreparedStatement updateEmployeeHostStmt = conn.prepareStatement(updateEmployeeHost)) {
                    updateEmployeeHostStmt.setObject(1, UUID.fromString(hostId));
                    updateEmployeeHostStmt.setString(2, userId);
                    updateEmployeeHostStmt.setObject(3, OffsetDateTime.parse(updateTs));
                    updateEmployeeHostStmt.setObject(4, UUID.fromString(userId));
                    int count = updateEmployeeHostStmt.executeUpdate();
                    if (count > 0) {
                        logger.debug("Updated employee_t for to switch user {} to host {}", userId, hostId);
                    }
                }
                try (PreparedStatement updateCustomerHostStmt = conn.prepareStatement(updateCustomerHost)) {
                    updateCustomerHostStmt.setObject(1, UUID.fromString(hostId));
                    updateCustomerHostStmt.setString(2, userId);
                    updateCustomerHostStmt.setObject(3, OffsetDateTime.parse(updateTs));
                    updateCustomerHostStmt.setObject(4, UUID.fromString(userId));
                    int count = updateCustomerHostStmt.executeUpdate();
                    if (count > 0) {
                        logger.debug("Updated customer_t for to switch user {} to host {}", userId, hostId);
                    }
                }
            }
            logger.debug("Activated host {} for user {}", hostId, userId);
        } catch (SQLException e) {
            logger.error("SQLException during switchHost for hostId {} userId {} (old: {}) -> (new: {}): {}", hostId, userId, oldAggregateVersion, newAggregateVersion, e.getMessage(), e);
            throw e;
        } catch (Exception e) {
            logger.error("Exception during switchHost for hostId {} userId {} (old: {}) -> (new: {}): {}", hostId, userId, oldAggregateVersion, newAggregateVersion, e.getMessage(), e);
            throw e;
        }
    }

    // detect if this is the first host for the user. If yes, set the current to true.
    private boolean queryUserHostExists(Connection conn, String userId) throws SQLException {
        final String sql =
                """
                SELECT COUNT(*) FROM user_host_t WHERE user_id = ?
                """;
        try (PreparedStatement pst = conn.prepareStatement(sql)) {
            pst.setObject(1, UUID.fromString(userId));
            try (ResultSet rs = pst.executeQuery()) {
                return rs.next() && rs.getInt(1) > 0;
            }
        }
    }

    // detect if there is an entry of host_id and user_id mapping in user_host_t table.
    private boolean queryUserHostExists(Connection conn, String hostId, String userId) throws SQLException {
        final String sql =
                """
                SELECT COUNT(*) FROM user_host_t WHERE host_id = ? AND user_id = ?
                """;
        try (PreparedStatement pst = conn.prepareStatement(sql)) {
            pst.setObject(1, UUID.fromString(hostId));
            pst.setObject(2, UUID.fromString(userId));
            try (ResultSet rs = pst.executeQuery()) {
                return rs.next() && rs.getInt(1) > 0;
            }
        }
    }

    @Override
    public void createUserHost(Connection conn, Map<String, Object> event) throws SQLException, Exception {
        final String insertHost =
                """
                INSERT INTO user_host_t (host_id, user_id, current, update_user, update_ts, aggregate_version)
                VALUES (?, ?, ?, ?, ?,  ?)
                """;
        Map<String, Object> map = (Map<String, Object>)event.get(PortalConstants.DATA);
        String hostId = (String)map.get("hostId");
        String userId = (String)map.get("userId");
        long newAggregateVersion = SqlUtil.getNewAggregateVersion(event);

        try (PreparedStatement statement = conn.prepareStatement(insertHost)) {

            statement.setObject(1, UUID.fromString(hostId));
            statement.setObject(2, UUID.fromString(userId));
            if (queryUserHostExists(conn, userId)) {
                statement.setBoolean(3, false);
            } else {
                statement.setBoolean(3, true);
            }
            statement.setString(4, (String)event.get(Constants.USER));
            statement.setObject(5, OffsetDateTime.parse((String)event.get(CloudEventV1.TIME)));
            statement.setLong(6, newAggregateVersion);
            int count = statement.executeUpdate();
            if (count == 0) {
                throw new SQLException(String.format("Failed during createUserHost for hostId %s userId %s with aggregateVersion %d", hostId, userId, newAggregateVersion));
            }
        } catch (SQLException e) {
            logger.error("SQLException during createUserHost for hostId {} userId {} aggregateVersion {}: {}", hostId, userId, newAggregateVersion, e.getMessage(), e);
            throw e; // Re-throw SQLException
        } catch (Exception e) {
            logger.error("Exception during createUserHost for hostId {} userId {} aggregateVersion {}: {}", hostId, userId, newAggregateVersion, e.getMessage(), e);
            throw e; // Re-throw generic Exception
        }
    }

    @Override
    public void deleteUserHost(Connection conn, Map<String, Object> event) throws SQLException, Exception {
        final String insertHost =
                """
                DELETE FROM user_host_t WHERE host_id = ? AND user_id = ? AND aggregate_version = ?
                """;
        Map<String, Object> map = (Map<String, Object>)event.get(PortalConstants.DATA);
        String hostId = (String)map.get("hostId");
        String userId = (String)map.get("userId");
        long aggregateVersion = SqlUtil.getOldAggregateVersion(event);

        try (PreparedStatement statement = conn.prepareStatement(insertHost)) {

            statement.setObject(1, UUID.fromString(hostId));
            statement.setObject(2, UUID.fromString(userId));
            statement.setLong(3, aggregateVersion);
            int count = statement.executeUpdate();
            if (count == 0) {
                if (queryUserHostExists(conn, hostId, userId)) {
                    throw new ConcurrencyException("Optimistic concurrency conflict during deleteUserHost for hostId " + hostId + " userId " + userId + " aggregateVersion " + aggregateVersion + " but found a different version or already updated.");
                } else {
                    throw new SQLException("No record found during deleteUserHost for hostId " + hostId + " userId " + userId + ". It might have been already deleted.");
                }
            }
        } catch (SQLException e) {
            logger.error("SQLException during deleteUserHost for hostId {} userId {} aggregateVersion {}: {}", hostId, userId, aggregateVersion, e.getMessage(), e);
            throw e; // Re-throw SQLException
        } catch (Exception e) {
            logger.error("Exception during deleteUserHost for hostId {} userId {} aggregateVersion {}: {}", hostId, userId, aggregateVersion, e.getMessage(), e);
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
                result = Success.of(JsonMapper.toJson(Collections.singletonMap("domain",domain)));
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
                "update_user, update_ts, aggregate_version FROM host_t WHERE host_id = ?";
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
                        map.put("hostOwner", resultSet.getObject("host_owner", UUID.class));
                        map.put("updateUser", resultSet.getString("update_user"));
                        map.put("updateTs", resultSet.getObject("update_ts") != null ? resultSet.getObject("update_ts", OffsetDateTime.class) : null);
                        map.put("aggregateVersion", resultSet.getLong("aggregate_version"));
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
        final String queryHostByOwner = "SELECT host_id, domain, sub_domain, host_desc, host_owner, update_user, update_ts, aggregate_version " +
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
                        map.put("hostOwner", resultSet.getObject("host_owner", UUID.class));
                        map.put("updateUser", resultSet.getString("update_user"));
                        map.put("updateTs", resultSet.getObject("update_ts") != null ? resultSet.getObject("update_ts", OffsetDateTime.class) : null);
                        map.put("aggregateVersion", resultSet.getLong("aggregate_version"));
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
    public Result<String> getOrg(int offset, int limit, String filtersJson, String globalFilter, String sortingJson) {
        Result<String> result = null;
        List<Map<String, Object>> filters = parseJsonList(filtersJson);
        List<Map<String, Object>> sorting = parseJsonList(sortingJson);

        String s =
                """
                SELECT COUNT(*) OVER () AS total,
                domain, org_name, org_desc, org_owner, update_user, update_ts, aggregate_version
                FROM org_t
                WHERE 1=1
                """;
        StringBuilder sqlBuilder = new StringBuilder(s);

        List<Object> parameters = new ArrayList<>();

        StringBuilder whereClause = new StringBuilder();

        // Material React Table Filters (Dynamic Filters)
        for (Map<String, Object> filter : filters) {
            String filterId = (String) filter.get("id");
            String dbColumnName = camelToSnake(filterId);
            Object filterValue = filter.get("value");
            if (filterId != null && filterValue != null && !filterValue.toString().isEmpty()) {
                whereClause.append(" AND ").append(dbColumnName).append(" ILIKE ?");
                parameters.add("%" + filterValue + "%");
            }
        }

        // Global Filter (Search across multiple columns)
        if (globalFilter != null && !globalFilter.isEmpty()) {
            whereClause.append(" AND (");
            // Define columns to search for global filter (e.g., org_name, org_desc, etc.)
            String[] globalSearchColumns = {"domain", "org_name", "org_desc"};
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
            orderByClause.append(" ORDER BY domain");
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
            orderByClause.append(sortExpressions.isEmpty() ? "domain" : String.join(", ", sortExpressions));
        }
        sqlBuilder.append(orderByClause);

        // Pagination
        sqlBuilder.append("\nLIMIT ? OFFSET ?");

        parameters.add(limit);
        parameters.add(offset);

        String sql = sqlBuilder.toString();
        if(logger.isTraceEnabled()) logger.trace("sql = {}", sql);

        int total = 0;
        List<Map<String, Object>> orgs = new ArrayList<>();

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
                    map.put("domain", resultSet.getString("domain"));
                    map.put("orgName", resultSet.getString("org_name"));
                    map.put("orgDesc", resultSet.getString("org_desc"));
                    map.put("orgOwner", resultSet.getObject("org_owner", UUID.class));
                    map.put("updateUser", resultSet.getString("update_user"));
                    map.put("updateTs", resultSet.getObject("update_ts") != null ? resultSet.getObject("update_ts", OffsetDateTime.class) : null);
                    map.put("aggregateVersion", resultSet.getLong("aggregate_version"));
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
    public Result<String> getHost(int offset, int limit, String filtersJson, String globalFilter, String sortingJson) {
        Result<String> result = null;
        List<Map<String, Object>> filters = parseJsonList(filtersJson);
        List<Map<String, Object>> sorting = parseJsonList(sortingJson);

        String s =
                """
                SELECT COUNT(*) OVER () AS total,
                host_id, domain, sub_domain, host_desc, host_owner, update_user, update_ts, aggregate_version
                FROM host_t
                WHERE 1=1
                """;
        StringBuilder sqlBuilder = new StringBuilder(s);


        List<Object> parameters = new ArrayList<>();

        StringBuilder whereClause = new StringBuilder();

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
            String[] globalSearchColumns = {"domain, sub_domain, host_desc"};
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
            orderByClause.append(" ORDER BY domain");
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
            orderByClause.append(sortExpressions.isEmpty() ? "domain" : String.join(", ", sortExpressions));
        }
        sqlBuilder.append(orderByClause);

        // Pagination
        sqlBuilder.append("\nLIMIT ? OFFSET ?");

        parameters.add(limit);
        parameters.add(offset);

        String sql = sqlBuilder.toString();
        if(logger.isTraceEnabled()) logger.trace("sql: {}", sql);
        int total = 0;
        List<Map<String, Object>> hosts = new ArrayList<>();

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
                    map.put("currentHostId", resultSet.getObject("host_id", UUID.class));
                    map.put("hostId", resultSet.getObject("host_id", UUID.class));
                    map.put("domain", resultSet.getString("domain"));
                    map.put("subDomain", resultSet.getString("sub_domain"));
                    map.put("hostDesc", resultSet.getString("host_desc"));
                    map.put("hostOwner", resultSet.getObject("host_owner", UUID.class));
                    map.put("updateUser", resultSet.getString("update_user"));
                    map.put("updateTs", resultSet.getObject("update_ts") != null ? resultSet.getObject("update_ts", OffsetDateTime.class) : null);
                    map.put("aggregateVersion", resultSet.getLong("aggregate_version"));
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
    public Result<String> getUserHost(int offset, int limit, String filtersJson, String globalFilter, String sortingJson) {
        Result<String> result = null;
        final Map<String, String> columnMap = Map.of(
                "hostId", "uh.host_id",
                "domain", "h.domain",
                "subDomain", "h.sub_domain",
                "userId", "uh.user_id",
                "email", "u.email",
                "firstName", "u.first_name",
                "lastName", "u.last_name",
                "current", "uh.current"
        );
        List<Map<String, Object>> filters = parseJsonList(filtersJson);
        List<Map<String, Object>> sorting = parseJsonList(sortingJson);


        String s =
                """
                SELECT COUNT(*) OVER () AS total,
                uh.host_id, h.domain, h.sub_domain, uh.user_id,
                u.email, u.first_name, u.last_name, uh.current,
                uh.update_user, uh.update_ts, uh.aggregate_version
                FROM user_host_t uh
                INNER JOIN host_t h ON uh.host_id = h.host_id
                INNER JOIN user_t u ON uh.user_id = u.user_id
                WHERE 1=1
                """;
        StringBuilder sqlBuilder = new StringBuilder(s);


        List<Object> parameters = new ArrayList<>();

        StringBuilder whereClause = new StringBuilder();

        // Material React Table Filters (Dynamic Filters) ---
        for (Map<String, Object> filter : filters) {
            String filterId = (String) filter.get("id"); // Column name
            String dbColumnName = mapToDbColumn(columnMap, filterId);
            Object filterValue = filter.get("value");    // Value to filter by
            if (filterId != null && filterValue != null && !filterValue.toString().isEmpty()) {
                if(dbColumnName.equals("uh.user_id") || dbColumnName.equals("uh.host_id")) {
                    whereClause.append(" AND ").append(dbColumnName).append(" = ?");
                    parameters.add(UUID.fromString(filterValue.toString()));
                } else {
                    whereClause.append(" AND ").append(dbColumnName).append(" ILIKE ?");
                    parameters.add("%" + filterValue + "%");
                }
            }
        }

        // Global Filter (Search across multiple columns)
        if (globalFilter != null && !globalFilter.isEmpty()) {
            whereClause.append(" AND (");
            // Define columns to search for global filter (e.g., table_name, table_desc)
            String[] globalSearchColumns = {"h.domain", "h.sub_domain", "u.email", "u.first_name", "u.last_name"};
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
            orderByClause.append(" ORDER BY u.email");
        } else {
            orderByClause.append(" ORDER BY ");
            List<String> sortExpressions = new ArrayList<>();
            for (Map<String, Object> sort : sorting) {
                String sortId = (String) sort.get("id");
                String dbColumnName = mapToDbColumn(columnMap, sortId);
                Boolean isDesc = (Boolean) sort.get("desc"); // 'desc' is typically a boolean or "true"/"false" string
                if (sortId != null && !sortId.isEmpty()) {
                    String direction = (isDesc != null && isDesc) ? "DESC" : "ASC";
                    // Quote column name to handle SQL keywords or mixed case
                    sortExpressions.add(dbColumnName + " " + direction);
                }
            }
            // Use default if dynamic sort failed to produce anything
            orderByClause.append(sortExpressions.isEmpty() ? "u.email" : String.join(", ", sortExpressions));
        }
        sqlBuilder.append(orderByClause);

        // Pagination
        sqlBuilder.append("\nLIMIT ? OFFSET ?");

        parameters.add(limit);
        parameters.add(offset);

        String sql = sqlBuilder.toString();
        if(logger.isTraceEnabled()) logger.trace("sql: {}", sql);

        int total = 0;
        List<Map<String, Object>> userHosts = new ArrayList<>();

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
                    map.put("userId", resultSet.getObject("user_id", UUID.class));
                    map.put("email", resultSet.getString("email"));
                    map.put("firstName", resultSet.getString("first_name"));
                    map.put("lastName", resultSet.getString("last_name"));
                    map.put("current", resultSet.getBoolean("current"));
                    map.put("updateUser", resultSet.getString("update_user"));
                    map.put("updateTs", resultSet.getObject("update_ts") != null ? resultSet.getObject("update_ts", OffsetDateTime.class) : null);
                    map.put("aggregateVersion", resultSet.getLong("aggregate_version"));
                    userHosts.add(map);
                }
            }

            Map<String, Object> resultMap = new HashMap<>();
            resultMap.put("total", total);
            resultMap.put("userHosts", userHosts);
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
        String s =
                """
                SELECT host_id, domain, sub_domain, host_desc, host_owner, update_user, update_ts, aggregate_version
                FROM host_t
                WHERE 1=1
                """;
        StringBuilder sqlBuilder = new StringBuilder(s);

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
                    map.put("hostOwner", resultSet.getObject("host_owner", UUID.class));
                    map.put("updateUser", resultSet.getString("update_user"));
                    map.put("updateTs", resultSet.getObject("update_ts") != null ? resultSet.getObject("update_ts", OffsetDateTime.class) : null);
                    map.put("aggregateVersion", resultSet.getLong("aggregate_version"));
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
