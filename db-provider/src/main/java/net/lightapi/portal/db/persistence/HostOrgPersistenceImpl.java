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
        // --- UPDATED SQL FOR UPSERT ---
        // This UPSERT handles:
        // 1. First time insert (no conflict).
        // 2. Re-creation/Update (conflict on domain) -> UPDATE existing row, respecting version monotonicity.
        final String upsertOrg =
                """
                INSERT INTO org_t (domain, org_name, org_desc, org_owner, aggregate_version, active, update_user, update_ts)
                VALUES (?, ?, ?, ?, ?, TRUE, ?, ?)
                ON CONFLICT (domain) DO UPDATE
                SET org_name = EXCLUDED.org_name,
                    org_desc = EXCLUDED.org_desc,
                    org_owner = EXCLUDED.org_owner,
                    aggregate_version = EXCLUDED.aggregate_version,
                    active = TRUE,
                    update_user = EXCLUDED.update_user,
                    update_ts = EXCLUDED.update_ts
                WHERE org_t.aggregate_version < EXCLUDED.aggregate_version
                """; // <<< CRITICAL: Monotonicity check in WHERE clause

        Map<String, Object> map = SqlUtil.extractEventData(event);
        String domain = (String)map.get("domain");
        String orgOwner = (String)map.get("orgOwner");
        long newAggregateVersion = SqlUtil.getNewAggregateVersion(event);

        // Parameters 1-7 (7 placeholders in the VALUES clause, excluding the hardcoded TRUE for active)
        try (PreparedStatement statement = conn.prepareStatement(upsertOrg)) {
            // --- SET VALUES (1 to 7) ---
            int i = 1;
            statement.setString(i++, domain); // 1. domain (PK)
            statement.setString(i++, (String)map.get("orgName")); // 2. org_name

            // 3. org_desc (Assuming it's required and present based on DDL NOT NULL)
            String orgDesc = (String)map.get("orgDesc");
            if (orgDesc != null && !orgDesc.isEmpty()) {
                statement.setString(i++, orgDesc);
            } else {
                statement.setNull(i++, Types.VARCHAR);
            }

            statement.setObject(i++, UUID.fromString(orgOwner)); // 4. org_owner
            statement.setLong(i++, newAggregateVersion); // 5. aggregate_version (New Version)
            // 6. active is TRUE in SQL
            statement.setString(i++, (String)event.get(Constants.USER)); // 6. update_user
            statement.setObject(i++, OffsetDateTime.parse((String)event.get(CloudEventV1.TIME))); // 7. update_ts

            // --- Execute UPSERT ---
            int count = statement.executeUpdate();
            if (count == 0) {
                // count=0 means the ON CONFLICT clause was hit, BUT the monotonicity WHERE clause failed.
                logger.warn("Org creation/update skipped for domain {} aggregateVersion {}. A newer or same version already exists in the projection.", domain, newAggregateVersion);
            } else {
                logger.info("Org {} successfully inserted or updated to aggregateVersion {}.", domain, newAggregateVersion);
            }
        } catch (SQLException e) {
            logger.error("SQLException during UPSERT for Org {} aggregateVersion {}: {}", domain, newAggregateVersion, e.getMessage(), e);
            throw e; // Re-throw for transaction management
        } catch (Exception e) {
            logger.error("Exception during UPSERT for Org {} aggregateVersion {}: {}", domain, newAggregateVersion, e.getMessage(), e);
            throw e; // Re-throw for transaction management
        }
    }

    /*
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
    */

    @Override
    public void updateOrg(Connection conn, Map<String, Object> event) throws SQLException, Exception {
        // --- UPDATED SQL FOR MONOTONICITY ---
        // Sets new version and checks that the current DB version is strictly LESS than the incoming new version.
        // This is the read model's idempotent check.
        final String updateOrgSql = "UPDATE org_t SET org_name = ?, org_desc = ?, org_owner = ?, " +
                "update_user = ?, update_ts = ?, aggregate_version = ?, active = TRUE " +
                "WHERE domain = ? AND aggregate_version < ?"; // <<< CRITICAL: Changed '= ?' to '< ?'

        Map<String, Object> map = SqlUtil.extractEventData(event); // Assuming extractEventData is the helper to get PortalConstants.DATA
        String domain = (String)map.get("domain");

        // oldAggregateVersion is needed only for logging the context of the original command, not the SQL check.
        long oldAggregateVersion = SqlUtil.getOldAggregateVersion(event);
        long newAggregateVersion = SqlUtil.getNewAggregateVersion(event); // The new version from the event

        try (PreparedStatement statement = conn.prepareStatement(updateOrgSql)) {
            int i = 1;

            // 1. org_name
            String orgName = (String)map.get("orgName");
            if (orgName != null && !orgName.isEmpty()) {
                statement.setString(i++, orgName);
            } else {
                statement.setNull(i++, NULL);
            }

            // 2. org_desc
            String orgDesc = (String)map.get("orgDesc");
            if (orgDesc != null && !orgDesc.isEmpty()) {
                statement.setString(i++, orgDesc);
            } else {
                statement.setNull(i++, NULL);
            }

            // 3. org_owner
            String orgOwner = (String)map.get("orgOwner");
            if (orgOwner != null && !orgOwner.isEmpty()) {
                statement.setObject(i++, UUID.fromString(orgOwner));
            } else {
                statement.setNull(i++, NULL);
            }

            // 4. update_user
            statement.setString(i++, (String)event.get(Constants.USER));

            // 5. update_ts
            statement.setObject(i++, OffsetDateTime.parse((String)event.get(CloudEventV1.TIME)));

            // 6. aggregate_version (NEW version in SET clause)
            statement.setLong(i++, newAggregateVersion);

            // 7. domain (in WHERE clause)
            statement.setString(i++, domain);

            // 8. aggregate_version (MONOTONICITY check in WHERE clause)
            statement.setLong(i, newAggregateVersion); // Condition: WHERE aggregate_version < newAggregateVersion

            int count = statement.executeUpdate();
            if (count == 0) {
                // count=0 means the record was either not found OR aggregate_version >= newAggregateVersion.
                // We IGNORE the failure and log a warning, as this is the desired idempotent/monotonic behavior.
                logger.warn("Org update skipped for domain {} (new version {}). Record not found or a newer version already exists.", domain, newAggregateVersion);
            }
            // NO THROW on count == 0. The method is now idempotent.

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
        // --- UPDATED SQL FOR SOFT DELETE + MONOTONICITY ---
        // Updates the 'active' flag to FALSE and sets the new version IF the current DB version is older than the incoming event's version.
        final String softDeleteOrgSql =
                """
                UPDATE org_t SET active = false, update_user = ?, update_ts = ?, aggregate_version = ?
                WHERE domain = ? AND aggregate_version < ?
                """; // <<< CRITICAL: Changed from DELETE to UPDATE, and used aggregate_version < ?

        Map<String, Object> map = SqlUtil.extractEventData(event); // Assuming extractEventData is the helper to get PortalConstants.DATA
        String domain = (String)map.get("domain"); // The domain PK

        // newAggregateVersion is the version of the incoming Delete event (the target version).
        long newAggregateVersion = SqlUtil.getNewAggregateVersion(event);

        try (PreparedStatement statement = conn.prepareStatement(softDeleteOrgSql)) {
            int i = 1;

            // 1. update_user
            statement.setString(i++, (String)event.get(Constants.USER));

            // 2. update_ts
            statement.setObject(i++, OffsetDateTime.parse((String)event.get(CloudEventV1.TIME)));

            // 3. aggregate_version (NEW version in SET clause)
            statement.setLong(i++, newAggregateVersion);

            // 4. domain (in WHERE clause)
            statement.setString(i++, domain);

            // 5. aggregate_version (MONOTONICITY check in WHERE clause)
            statement.setLong(i, newAggregateVersion); // Condition: WHERE aggregate_version < newAggregateVersion

            int count = statement.executeUpdate();
            if (count == 0) {
                // count=0 means the record was either not found OR aggregate_version >= newAggregateVersion.
                // We IGNORE the failure and log a warning, as this is the desired idempotent/monotonic behavior.
                logger.warn("deleteOrg skipped for domain {} aggregateVersion {}. Record not found or a newer version already exists.", domain, newAggregateVersion);
            }
            // NO THROW on count == 0. The method is now idempotent.

        } catch (SQLException e) {
            logger.error("SQLException during deleteOrg for domain {} aggregateVersion {}: {}", domain, newAggregateVersion, e.getMessage(), e);
            throw e; // Re-throw SQLException
        } catch (Exception e) {
            logger.error("Exception during deleteOrg for domain {} aggregateVersion {}: {}", domain, newAggregateVersion, e.getMessage(), e);
            throw e; // Re-throw generic Exception
        }
    }

    @Override
    public void createHost(Connection conn, Map<String, Object> event) throws SQLException, Exception {
        // Use UPSERT: INSERT ON CONFLICT DO UPDATE
        // This handles:
        // 1. First time insert (no conflict).
        // 2. Re-creation/Update (conflict on host_id) -> UPDATE the existing row (setting active=TRUE and new version).

        final String upsertHost =
                """
                INSERT INTO host_t (host_id, domain, sub_domain, host_desc, host_owner, update_user, update_ts, aggregate_version, active)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, TRUE)
                ON CONFLICT (host_id) DO UPDATE
                SET domain = EXCLUDED.domain,
                    sub_domain = EXCLUDED.sub_domain,
                    host_desc = EXCLUDED.host_desc,
                    host_owner = EXCLUDED.host_owner,
                    update_user = EXCLUDED.update_user,
                    update_ts = EXCLUDED.update_ts,
                    aggregate_version = EXCLUDED.aggregate_version,
                    active = TRUE
                WHERE host_t.aggregate_version < EXCLUDED.aggregate_version
                """; // <<< CRITICAL: Added WHERE to ensure we only update if the incoming event is newer

        Map<String, Object> map = SqlUtil.extractEventData(event);
        String hostId = (String)map.get("hostId"); // enriched in the service.
        String domain = (String)map.get("domain");
        String hostOwner = (String)map.get("hostOwner");
        long newAggregateVersion = SqlUtil.getNewAggregateVersion(event);

        // Parameters 1-8 (8 placeholders in the VALUES clause)
        try (PreparedStatement statement = conn.prepareStatement(upsertHost)) {
            // --- SET VALUES (1 to 8) ---
            int i = 1;
            statement.setObject(i++, UUID.fromString(hostId)); // 1. host_id (PK)
            statement.setString(i++, domain); // 2. domain
            statement.setString(i++, (String)map.get("subDomain")); // 3. sub_domain

            // 4. host_desc
            if (map.containsKey("hostDesc")) {
                statement.setString(i++, (String) map.get("hostDesc"));
            } else {
                statement.setNull(i++, Types.VARCHAR);
            }

            statement.setObject(i++, UUID.fromString(hostOwner)); // 5. host_owner
            statement.setString(i++, (String)event.get(Constants.USER)); // 6. update_user
            statement.setObject(i++, OffsetDateTime.parse((String)event.get(CloudEventV1.TIME))); // 7. update_ts
            statement.setLong(i++, newAggregateVersion); // 8. aggregate_version
            // active is TRUE in SQL

            // --- Execute UPSERT ---
            int count = statement.executeUpdate();
            if (count == 0) {
                // count=0 means the ON CONFLICT clause was hit, BUT the monotonicity WHERE clause failed.
                logger.warn("createHost skipped for hostId {} aggregateVersion {}. A newer or same version already exists in the projection.", hostId, newAggregateVersion);
            } else {
                logger.info("Host {} successfully inserted or updated to aggregateVersion {}.", hostId, newAggregateVersion);
            }
        } catch (SQLException e) {
            // Log the critical unique constraint violation if it happens on the secondary key (domain, sub_domain)
            // This is a data integrity error from upstream if it happens on insert.
            logger.error("SQLException during createHost UPSERT for hostId {} aggregateVersion {}: {}", hostId, newAggregateVersion, e.getMessage(), e);
            throw e;
        } catch (Exception e) {
            logger.error("Exception during createHost UPSERT for hostId {} aggregateVersion {}: {}", hostId, newAggregateVersion, e.getMessage(), e);
            throw e;
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
        // We will attempt to update the record IF the incoming event is newer than the current projection version.
        // We also explicitly set active = TRUE (assuming an update implies the host is now active).
        final String sql =
                """
                UPDATE host_t SET host_desc = ?, host_owner = ?, update_user = ?, update_ts = ?,
                aggregate_version = ?, active = TRUE
                WHERE host_id = ? AND aggregate_version < ?
                """; // <<< CRITICAL: Changed '= ?' to '< ?' in the WHERE clause AND added active=TRUE

        Map<String, Object> map = SqlUtil.extractEventData(event); // Assuming extractEventData is the helper to get PortalConstants.DATA
        String hostId = (String) map.get("hostId");

        long newAggregateVersion = SqlUtil.getNewAggregateVersion(event); // The new version from the event

        try (PreparedStatement statement = conn.prepareStatement(sql)) {
            int i = 1;

            // 1. host_desc
            String hostDesc = (String)map.get("hostDesc");
            if (hostDesc != null && !hostDesc.isEmpty()) {
                statement.setString(i++, hostDesc);
            } else {
                statement.setNull(i++, NULL);
            }

            // 2. host_owner
            String hostOwner = (String)map.get("hostOwner");
            if (hostOwner != null && !hostOwner.isEmpty()) {
                statement.setObject(i++, UUID.fromString(hostOwner));
            } else {
                statement.setNull(i++, NULL);
            }

            // 3. update_user
            statement.setString(i++, (String)event.get(Constants.USER));

            // 4. update_ts
            statement.setObject(i++, OffsetDateTime.parse((String)event.get(CloudEventV1.TIME)));

            // 5. aggregate_version (NEW version in SET clause)
            statement.setLong(i++, newAggregateVersion);

            // 6. host_id (in WHERE clause)
            statement.setObject(i++, UUID.fromString(hostId));

            // 7. aggregate_version (MONOTONICITY check in WHERE clause)
            statement.setLong(i, newAggregateVersion); // Condition: WHERE aggregate_version < newAggregateVersion

            int count = statement.executeUpdate();
            if (count == 0) {
                // count=0 means the record was either not found OR aggregate_version >= newAggregateVersion.
                // We IGNORE the failure and log a warning, as this is the desired idempotent/monotonic behavior.
                logger.warn("updateHost skipped for hostId {} aggregateVersion {}. Record not found or a newer version already exists.", hostId, newAggregateVersion);
            }
            // NO THROW on count == 0. The method is now idempotent.

        } catch (SQLException e) {
            logger.error("SQLException during updateHost for hostId {} aggregateVersion {}: {}", hostId, newAggregateVersion, e.getMessage(), e);
            throw e;
        } catch (Exception e) {
            logger.error("Exception during updateHost for hostId {} aggregateVersion {}: {}", hostId, newAggregateVersion, e.getMessage(), e);
            throw e;
        }
    }

    @Override
    public void deleteHost(Connection conn, Map<String, Object> event) throws SQLException, Exception {
        // --- UPDATED SQL FOR SOFT DELETE + MONOTONICITY ---
        // Updates the 'active' flag to FALSE and sets the new version IF the current DB version is older than the incoming event's version.
        final String softDeleteHostSql =
                """
                UPDATE host_t SET active = false, update_user = ?, update_ts = ?, aggregate_version = ?
                WHERE host_id = ? AND aggregate_version < ?
                """; // <<< CRITICAL: Changed from DELETE to UPDATE, and used aggregate_version < ?

        Map<String, Object> map = SqlUtil.extractEventData(event); // Assuming extractEventData is the helper to get PortalConstants.DATA
        String hostId = (String) map.get("hostId");

        // newAggregateVersion is the version of the incoming Delete event (the target version).
        long newAggregateVersion = SqlUtil.getNewAggregateVersion(event);

        try (PreparedStatement statement = conn.prepareStatement(softDeleteHostSql)) {
            int i = 1;

            // 1. update_user
            statement.setString(i++, (String)event.get(Constants.USER));

            // 2. update_ts
            statement.setObject(i++, OffsetDateTime.parse((String)event.get(CloudEventV1.TIME)));

            // 3. aggregate_version (NEW version in SET clause)
            statement.setLong(i++, newAggregateVersion);

            // 4. host_id (in WHERE clause)
            statement.setObject(i++, UUID.fromString(hostId));

            // 5. aggregate_version (MONOTONICITY check in WHERE clause)
            statement.setLong(i, newAggregateVersion); // Condition: WHERE aggregate_version < newAggregateVersion

            int count = statement.executeUpdate();
            if (count == 0) {
                // If 0 rows updated, it means the record was either not found OR aggregate_version >= newAggregateVersion.
                // We IGNORE the failure and log a warning, as this is the desired idempotent/monotonic behavior.
                logger.warn("deleteHost skipped for hostId {} aggregateVersion {}. Record not found or a newer version already exists.", hostId, newAggregateVersion);
            }
            // NO THROW on count == 0. The method is now idempotent.

        } catch (SQLException e) {
            logger.error("SQLException during deleteHost for hostId {} aggregateVersion {}: {}", hostId, newAggregateVersion, e.getMessage(), e);
            throw e; // Re-throw SQLException
        } catch (Exception e) {
            logger.error("Exception during deleteHost for hostId {} aggregateVersion {}: {}", hostId, newAggregateVersion, e.getMessage(), e);
            throw e; // Re-throw generic Exception
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
        // --- UPDATED SQL FOR UPSERT ---
        // This UPSERT handles:
        // 1. First time insert (no conflict).
        // 2. Re-creation/Update (conflict on host_id, user_id) -> UPDATE existing row.
        final String upsertUserHost =
                """
                INSERT INTO user_host_t (host_id, user_id, current, aggregate_version, active, update_user, update_ts)
                VALUES (?, ?, ?, ?, TRUE, ?, ?)
                ON CONFLICT (host_id, user_id) DO UPDATE
                SET current = EXCLUDED.current,
                    aggregate_version = EXCLUDED.aggregate_version,
                    active = TRUE,
                    update_user = EXCLUDED.update_user,
                    update_ts = EXCLUDED.update_ts
                WHERE user_host_t.aggregate_version < EXCLUDED.aggregate_version
                """; // <<< CRITICAL: Added WHERE to enforce monotonicity

        Map<String, Object> map = SqlUtil.extractEventData(event);
        String hostId = (String)map.get("hostId");
        String userId = (String)map.get("userId");
        long newAggregateVersion = SqlUtil.getNewAggregateVersion(event);

        // We assume the event payload NOW CONTAINS the final state of 'current' for this (user, host) pair.
        // The complex 'queryUserHostExists' logic has been moved UPSTREAM to the command handler.
        boolean currentFlagValue = (boolean)map.getOrDefault("current", false); // Assume command handler provided 'current' flag value

        // Parameters 1-7 (7 placeholders in the VALUES clause)
        try (PreparedStatement statement = conn.prepareStatement(upsertUserHost)) {
            // --- SET VALUES (1 to 7) ---
            int i = 1;
            statement.setObject(i++, UUID.fromString(hostId)); // 1. host_id
            statement.setObject(i++, UUID.fromString(userId)); // 2. user_id
            statement.setBoolean(i++, currentFlagValue); // 3. current (Value provided by event)
            statement.setLong(i++, newAggregateVersion); // 4. aggregate_version
            // 5. active is hardcoded to TRUE in SQL
            statement.setString(i++, (String) event.get(Constants.USER)); // 6. update_user
            statement.setObject(i, OffsetDateTime.parse((String) event.get(CloudEventV1.TIME))); // 7. update_ts

            // --- Execute UPSERT ---
            int count = statement.executeUpdate();
            if (count == 0) {
                // count=0 means the ON CONFLICT clause was hit, BUT the WHERE clause (aggregate_version < EXCLUDED.aggregate_version) failed.
                logger.warn("Creation/Update skipped for hostId {} userId {} aggregateVersion {}. A newer or same version already exists in the projection.", hostId, userId, newAggregateVersion);
            } else {
                logger.info("UserHost mapping {}/{} successfully inserted or updated to aggregateVersion {}.", hostId, userId, newAggregateVersion);
            }
        } catch (SQLException e) {
            logger.error("SQLException during UPSERT for hostId {} userId {} aggregateVersion {}: {}", hostId, userId, newAggregateVersion, e.getMessage(), e);
            throw e; // Re-throw for transaction management
        } catch (Exception e) {
            logger.error("Exception during UPSERT for hostId {} userId {} aggregateVersion {}: {}", hostId, userId, newAggregateVersion, e.getMessage(), e);
            throw e; // Re-throw for transaction management
        }
    }

    @Override
    public void deleteUserHost(Connection conn, Map<String, Object> event) throws SQLException, Exception {
        // --- UPDATED SQL FOR SOFT DELETE + MONOTONICITY ---
        // Sets the 'active' flag to FALSE and sets the new version IF the current DB version is older than the incoming event's version.
        final String softDeleteUserHostSql =
                """
                UPDATE user_host_t SET active = false, update_user = ?, update_ts = ?, aggregate_version = ?
                WHERE host_id = ? AND user_id = ? AND aggregate_version < ?
                """; // <<< CRITICAL: Changed from DELETE to UPDATE, and used aggregate_version < ?

        Map<String, Object> map = SqlUtil.extractEventData(event); // Assuming extractEventData is the helper to get PortalConstants.DATA
        String hostId = (String) map.get("hostId");
        String userId = (String) map.get("userId");

        // newAggregateVersion is the version of the incoming Delete event (the target version).
        long newAggregateVersion = SqlUtil.getNewAggregateVersion(event);

        try (PreparedStatement statement = conn.prepareStatement(softDeleteUserHostSql)) {
            int i = 1;

            // 1. update_user
            statement.setString(i++, (String)event.get(Constants.USER));

            // 2. update_ts
            statement.setObject(i++, OffsetDateTime.parse((String)event.get(CloudEventV1.TIME)));

            // 3. aggregate_version (NEW version in SET clause)
            statement.setLong(i++, newAggregateVersion);

            // 4. host_id (in WHERE clause)
            statement.setObject(i++, UUID.fromString(hostId));

            // 5. user_id (in WHERE clause)
            statement.setObject(i++, UUID.fromString(userId));

            // 6. aggregate_version (MONOTONICITY check in WHERE clause)
            statement.setLong(i, newAggregateVersion); // Condition: WHERE aggregate_version < newAggregateVersion

            int count = statement.executeUpdate();
            if (count == 0) {
                // If 0 rows updated, it means the record was either not found OR aggregate_version >= newAggregateVersion.
                // We IGNORE the failure and log a warning, as this is the desired idempotent/monotonic behavior.
                logger.warn("deleteUserHost skipped for hostId {} userId {} aggregateVersion {}. Record not found or a newer version already exists.", hostId, userId, newAggregateVersion);
            }
            // NO THROW on count == 0. The method is now idempotent.

        } catch (SQLException e) {
            logger.error("SQLException during deleteUserHost for hostId {} userId {} aggregateVersion {}: {}", hostId, userId, newAggregateVersion, e.getMessage(), e);
            throw e; // Re-throw SQLException
        } catch (Exception e) {
            logger.error("Exception during deleteUserHost for hostId {} userId {} aggregateVersion {}: {}", hostId, userId, newAggregateVersion, e.getMessage(), e);
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
    public Result<String> queryHostById(String hostId) {
        final String queryHostById = "SELECT host_id, domain, sub_domain, host_desc, host_owner, " +
                "update_user, update_ts, aggregate_version, active FROM host_t WHERE host_id = ?";
        Result<String> result;
        try (final Connection conn = ds.getConnection()) {
            Map<String, Object> map = new HashMap<>();
            try (PreparedStatement statement = conn.prepareStatement(queryHostById)) {
                statement.setObject(1, UUID.fromString(hostId));
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
                        map.put("active", resultSet.getBoolean("active"));
                    }
                }
            }
            if (map.isEmpty())
                result = Failure.of(new Status(OBJECT_NOT_FOUND, "queryHostById", hostId));
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
                domain, org_name, org_desc, org_owner, update_user,
                update_ts, aggregate_version, active
                FROM org_t
                WHERE 1=1
                """;

        List<Object> parameters = new ArrayList<>();

        String[] searchColumns = {"domain", "org_name", "org_desc"};
        String sqlBuilder = s + dynamicFilter(Arrays.asList("org_owner"), Arrays.asList(searchColumns), filters, null, parameters) +
                globalFilter(globalFilter, searchColumns, parameters) +
                dynamicSorting("domain", sorting, null) +
                "\nLIMIT ? OFFSET ?";

        parameters.add(limit);
        parameters.add(offset);
        if(logger.isTraceEnabled()) logger.trace("sql = {}", sqlBuilder);
        int total = 0;
        List<Map<String, Object>> orgs = new ArrayList<>();

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
                    map.put("domain", resultSet.getString("domain"));
                    map.put("orgName", resultSet.getString("org_name"));
                    map.put("orgDesc", resultSet.getString("org_desc"));
                    map.put("orgOwner", resultSet.getObject("org_owner", UUID.class));
                    map.put("updateUser", resultSet.getString("update_user"));
                    map.put("updateTs", resultSet.getObject("update_ts") != null ? resultSet.getObject("update_ts", OffsetDateTime.class) : null);
                    map.put("aggregateVersion", resultSet.getLong("aggregate_version"));
                    map.put("active", resultSet.getBoolean("active"));
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
    public Result<String> getOrgByDomain(String domain) {
        Result<String> result;
        String sql =
            """
            SELECT domain, org_name, org_desc, org_owner, aggregate_version,
            active, update_user, update_ts
            FROM org_t
            WHERE domain = ?
            """;
        try (final Connection conn = ds.getConnection()) {
            Map<String, Object> map = new HashMap<>();
            try (PreparedStatement statement = conn.prepareStatement(sql)) {
                statement.setString(1, domain);
                try (ResultSet resultSet = statement.executeQuery()) {
                    if (resultSet.next()) {
                        map.put("domain", resultSet.getString("domain"));
                        map.put("orgName", resultSet.getString("org_name"));
                        map.put("orgDesc", resultSet.getString("org_desc"));
                        map.put("orgOwner", resultSet.getObject("org_owner", UUID.class));
                        map.put("updateUser", resultSet.getString("update_user"));
                        map.put("updateTs", resultSet.getObject("update_ts") != null ? resultSet.getObject("update_ts", OffsetDateTime.class) : null);
                        map.put("aggregateVersion", resultSet.getLong("aggregate_version"));
                        map.put("active", resultSet.getBoolean("active"));
                    }
                }
            }
            if (map.isEmpty())
                result = Failure.of(new Status(OBJECT_NOT_FOUND, "getOrgByDomain", domain));
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
    public Result<String> getHost(int offset, int limit, String filtersJson, String globalFilter, String sortingJson) {
        Result<String> result = null;
        List<Map<String, Object>> filters = parseJsonList(filtersJson);
        List<Map<String, Object>> sorting = parseJsonList(sortingJson);

        String s =
                """
                SELECT COUNT(*) OVER () AS total,
                host_id, domain, sub_domain, host_desc, host_owner, update_user, update_ts, aggregate_version, active
                FROM host_t
                WHERE 1=1
                """;
        List<Object> parameters = new ArrayList<>();

        String[] searchColumns = {"domain", "sub_domain", "host_desc"};
        String sqlBuilder = s + dynamicFilter(Arrays.asList("host_id"), Arrays.asList(searchColumns), filters, null, parameters) +
                globalFilter(globalFilter, searchColumns, parameters) +
                dynamicSorting("domain, sub_domain", sorting, null) +
                "\nLIMIT ? OFFSET ?";

        parameters.add(limit);
        parameters.add(offset);

        if(logger.isTraceEnabled()) logger.trace("sql: {}", sqlBuilder);
        int total = 0;
        List<Map<String, Object>> hosts = new ArrayList<>();

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
                    map.put("hostId", resultSet.getObject("host_id", UUID.class));
                    map.put("domain", resultSet.getString("domain"));
                    map.put("subDomain", resultSet.getString("sub_domain"));
                    map.put("hostDesc", resultSet.getString("host_desc"));
                    map.put("hostOwner", resultSet.getObject("host_owner", UUID.class));
                    map.put("updateUser", resultSet.getString("update_user"));
                    map.put("updateTs", resultSet.getObject("update_ts") != null ? resultSet.getObject("update_ts", OffsetDateTime.class) : null);
                    map.put("aggregateVersion", resultSet.getLong("aggregate_version"));
                    map.put("active", resultSet.getBoolean("active"));
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
                uh.update_user, uh.update_ts, uh.aggregate_version, uh.active
                FROM user_host_t uh
                INNER JOIN host_t h ON uh.host_id = h.host_id
                INNER JOIN user_t u ON uh.user_id = u.user_id
                WHERE 1=1
                """;
        List<Object> parameters = new ArrayList<>();

        String[] searchColumns = {"h.domain", "h.sub_domain", "u.email", "u.first_name", "u.last_name"};
        String sqlBuilder = s + dynamicFilter(Arrays.asList("uh.host_id", "uh.user_id"), Arrays.asList(searchColumns), filters, null, parameters) +
                globalFilter(globalFilter, searchColumns, parameters) +
                dynamicSorting("u.email", sorting, null) +
                "\nLIMIT ? OFFSET ?";

        parameters.add(limit);
        parameters.add(offset);

        if(logger.isTraceEnabled()) logger.trace("sql: {}", sqlBuilder);

        int total = 0;
        List<Map<String, Object>> userHosts = new ArrayList<>();

        try (Connection connection = ds.getConnection();
            PreparedStatement preparedStatement = connection.prepareStatement(sqlBuilder)) {
            populateParameters(preparedStatement, parameters);
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
                    map.put("active", resultSet.getBoolean("active"));
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

    @Override
    public String getHostId(String domain, String subDomain) {
        final String sql = "SELECT host_id FROM host_t WHERE domain = ? AND sub_domain = ?";
        String hostId = null;
        try (Connection connection = ds.getConnection();
            PreparedStatement statement = connection.prepareStatement(sql)) {
            statement.setString(1, domain);
            statement.setString(2, subDomain);
            try (ResultSet resultSet = statement.executeQuery()) {
                if(resultSet.next()){
                    hostId = resultSet.getString(1);
                }
            }
        } catch (SQLException e) {
            logger.error("SQLException:", e);
        } catch (Exception e) {
            logger.error("Exception:", e);
        }
        return hostId;
    }
}
