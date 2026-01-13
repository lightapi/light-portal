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

import java.sql.*;
import java.time.OffsetDateTime;
import java.util.*;

import static com.networknt.db.provider.SqlDbStartupHook.ds;
import static java.sql.Types.NULL;
import static net.lightapi.portal.db.util.SqlUtil.*;

public class AuthPersistenceImpl implements AuthPersistence {
    private static final Logger logger = LoggerFactory.getLogger(AuthPersistenceImpl.class);
    private static final String SQL_EXCEPTION = PortalDbProvider.SQL_EXCEPTION;
    private static final String GENERIC_EXCEPTION = PortalDbProvider.GENERIC_EXCEPTION;
    private static final String OBJECT_NOT_FOUND = PortalDbProvider.OBJECT_NOT_FOUND;

    private final NotificationService notificationService;

    public AuthPersistenceImpl(NotificationService notificationService) {
        this.notificationService = notificationService;
    }

    @Override
    public void createApp(Connection conn, Map<String, Object> event) throws SQLException, Exception {
        // Use UPSERT: INSERT ON CONFLICT DO UPDATE
        // This handles:
        // 1. First time insert (no conflict).
        // 2. Re-creation (conflict on host_id, app_id) -> UPDATE the existing row (setting active=TRUE and new version).

        final String sql =
                """
                INSERT INTO app_t(host_id, app_id, app_name, app_desc, is_kafka_app, operation_owner, delivery_owner, update_user, update_ts, aggregate_version, active)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, TRUE)
                ON CONFLICT (host_id, app_id) DO UPDATE
                SET app_name = EXCLUDED.app_name,
                    app_desc = EXCLUDED.app_desc,
                    is_kafka_app = EXCLUDED.is_kafka_app,
                    operation_owner = EXCLUDED.operation_owner,
                    delivery_owner = EXCLUDED.delivery_owner,
                    update_user = EXCLUDED.update_user,
                    update_ts = EXCLUDED.update_ts,
                    aggregate_version = EXCLUDED.aggregate_version,
                    active = TRUE
                WHERE app_t.aggregate_version < EXCLUDED.aggregate_version AND app_t.active = FALSE
                """; // <<< CRITICAL: Added WHERE to ensure we only update if the incoming event is newer

        Map<String, Object> map = SqlUtil.extractEventData(event);
        String hostId = (String)map.get("hostId");
        String appId = (String)map.get("appId");
        long newAggregateVersion = SqlUtil.getNewAggregateVersion(event);
        try (PreparedStatement statement = conn.prepareStatement(sql)) {
            statement.setObject(1, UUID.fromString(hostId));
            statement.setString(2, appId);
            statement.setString(3, (String) map.get("appName"));
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
                statement.setObject(6, UUID.fromString((String) map.get("operationOwner")));
            } else {
                statement.setNull(6, Types.OTHER);
            }
            if (map.containsKey("deliveryOwner")) {
                statement.setObject(7, UUID.fromString((String) map.get("deliveryOwner")));
            } else {
                statement.setNull(7, Types.OTHER);
            }
            statement.setString(8, (String) event.get(Constants.USER));
            statement.setObject(9, OffsetDateTime.parse((String) event.get(CloudEventV1.TIME)));
            statement.setLong(10, newAggregateVersion);

            int count = statement.executeUpdate();
            if (count == 0) {
                // count=0 means the ON CONFLICT clause was hit, BUT the WHERE clause (aggregate_version < EXCLUDED.aggregate_version) failed.
                // This is the desired idempotent/out-of-order protection behavior. Log and ignore.
                logger.warn("Creation/Reactivation skipped for hostId {} appId {} aggregateVersion {}. A newer or same version already exists.", hostId, appId, newAggregateVersion);            }
        } catch (SQLException e) {
            logger.error("SQLException during createApp for hostId {} appId {} aggregateVersion {}: {}", hostId, appId, newAggregateVersion, e.getMessage(), e);
            throw e;
        } catch (Exception e) {
            logger.error("Exception during createApp for hostId {} appId {} aggregateVersion {}: {}", hostId, appId, newAggregateVersion, e.getMessage(), e);
            throw e;
        }
    }

    @Override
    public void updateApp(Connection conn, Map<String, Object> event) throws SQLException, Exception {
        // We will attempt to update the record IF the incoming event is newer than the current projection version.
        // We also explicitly set active = TRUE if the record exists, as an UPDATE event implies the app is active.
        final String sql =
                """
                UPDATE app_t SET app_name = ?, app_desc = ?, is_kafka_app = ?, operation_owner = ?,
                delivery_owner = ?, update_user = ?, update_ts = ?, aggregate_version = ?, active = TRUE
                WHERE host_id = ? AND app_id = ? AND aggregate_version < ?
                """; // <<< CRITICAL: Added aggregate_version < ? to enforce monotonicity
        Map<String, Object> map = SqlUtil.extractEventData(event);
        String hostId = (String) map.get("hostId");
        String appId = (String) map.get("appId");
        long newAggregateVersion = SqlUtil.getNewAggregateVersion(event);

        try (PreparedStatement statement = conn.prepareStatement(sql)) {
            statement.setString(1, (String) map.get("appName"));

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
                statement.setObject(4, UUID.fromString((String) map.get("operationOwner")));
            } else {
                statement.setNull(4, Types.OTHER);
            }
            if (map.containsKey("deliveryOwner")) {
                statement.setObject(5, UUID.fromString((String) map.get("deliveryOwner")));
            } else {
                statement.setNull(5, Types.VARCHAR);
            }
            statement.setString(6, (String) event.get(Constants.USER));
            statement.setObject(7, OffsetDateTime.parse((String) event.get(CloudEventV1.TIME)));
            statement.setLong(8, newAggregateVersion);
            statement.setObject(9, UUID.fromString(hostId));
            statement.setString(10, appId);
            statement.setLong(11, newAggregateVersion);

            int count = statement.executeUpdate();
            if (count == 0) {
                // If 0 rows updated, it means the record was either not found OR aggregate_version >= newAggregateVersion.
                // We IGNORE the failure and log a warning, as this is the desired idempotent/monotonic behavior.
                logger.warn("Update skipped for hostId {} appId {} aggregateVersion {}. Record not found or a newer version already exists.", hostId, appId, newAggregateVersion);
            }
        } catch (SQLException e) {
            logger.error("SQLException during updateApp for hostId {} appId {} aggregateVersion {}: {}", hostId, appId, newAggregateVersion, e.getMessage(), e);
            throw e;
        } catch (Exception e) {
            logger.error("Exception during updateApp for hostId {} appId {} aggregateVersion {}: {}", hostId, appId, newAggregateVersion, e.getMessage(), e);
            throw e;
        }
    }

    @Override
    public void deleteApp(Connection conn, Map<String, Object> event) throws SQLException, Exception {
        // The delete event's sequence number is the NEW version for the soft-deleted state.
        // We UPDATE to set active=false IF the incoming event is newer than the current projection version.
        final String sql =
                """
                UPDATE app_t SET active = false, update_user = ?, update_ts = ?, aggregate_version = ?
                WHERE host_id = ? AND app_id = ? AND aggregate_version < ?
                """; // <<< CRITICAL: Added aggregate_version < ? to enforce monotonicity

        Map<String, Object> map = SqlUtil.extractEventData(event);
        String hostId = (String)map.get("hostId");
        String appId = (String) map.get("appId");
        long newAggregateVersion = SqlUtil.getNewAggregateVersion(event);

        try (PreparedStatement statement = conn.prepareStatement(sql)) {
            statement.setString(1, (String) event.get(Constants.USER));
            statement.setObject(2, OffsetDateTime.parse((String) event.get(CloudEventV1.TIME)));
            statement.setLong(3, newAggregateVersion );
            statement.setObject(4, UUID.fromString(hostId));
            statement.setString(5, appId);
            // WHERE monotonicity check: use the new version as the check value
            statement.setLong(6, newAggregateVersion); // Condition is WHERE aggregate_version < newAggregateVersion

            int count = statement.executeUpdate();
            if (count == 0) {
                // If 0 rows updated, it means the record was either not found OR aggregate_version >= newAggregateVersion.
                // We IGNORE the failure and log a warning, as this is the desired idempotent/monotonic behavior.
                logger.warn("Deletion skipped for hostId {} appId {} aggregateVersion {}. Record not found or a newer version already exists.", hostId, appId, newAggregateVersion);            }
        } catch (SQLException e) {
            logger.error("SQLException during deleteApp for appId {} aggregateVersion {}: {}", appId, newAggregateVersion, e.getMessage(), e);
            throw e;
        } catch (Exception e) {
            logger.error("Exception during deleteApp for appId {} aggregateVersion {}: {}", appId, newAggregateVersion, e.getMessage(), e);
            throw e;
        }
    }

    @Override
    public Result<String> getAppIdLabel(String hostId) {
        Result<String> result = null;
        String sql = "SELECT app_id, app_name FROM app_t WHERE host_id = ? AND active = TRUE";
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
    public void createClient(Connection conn, Map<String, Object> event) throws SQLException, Exception {
        // --- UPDATED SQL FOR UPSERT ---
        final String upsertClient =
                """
                INSERT INTO auth_client_t (
                    host_id, client_id, client_name, app_id, api_id,
                    client_type, client_profile, client_secret, client_scope, custom_claim,
                    redirect_uri, authenticate_class, deref_client_id, update_user, update_ts, aggregate_version
                )
                VALUES (
                    ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?
                )
                ON CONFLICT (host_id, client_id) DO UPDATE -- <<< CONFLICT KEY is PRIMARY KEY
                SET
                    client_name = EXCLUDED.client_name,
                    app_id = EXCLUDED.app_id,
                    api_id = EXCLUDED.api_id,
                    client_type = EXCLUDED.client_type,
                    client_profile = EXCLUDED.client_profile,
                    client_secret = EXCLUDED.client_secret,
                    client_scope = EXCLUDED.client_scope,
                    custom_claim = EXCLUDED.custom_claim,
                    redirect_uri = EXCLUDED.redirect_uri,
                    authenticate_class = EXCLUDED.authenticate_class,
                    deref_client_id = EXCLUDED.deref_client_id,
                    update_user = EXCLUDED.update_user,
                    update_ts = EXCLUDED.update_ts,
                    aggregate_version = EXCLUDED.aggregate_version
                -- CRITICAL: Only update if the incoming event's version is newer
                WHERE auth_client_t.aggregate_version < EXCLUDED.aggregate_version
                AND auth_client_t.active = FALSE
                """;

        Map<String, Object> map = SqlUtil.extractEventData(event);
        String hostId = (String) map.get("hostId");
        String clientId = (String) map.get("clientId");
        long newAggregateVersion = SqlUtil.getNewAggregateVersion(event);

        // Parameters 1-16 (16 placeholders in the VALUES clause)
        try (PreparedStatement statement = conn.prepareStatement(upsertClient)) {
            // --- SET VALUES (1 to 16) ---
            int i = 1;
            statement.setObject(i++, UUID.fromString(hostId)); // 1. host_id
            statement.setObject(i++, UUID.fromString(clientId)); // 2. client_id

            statement.setString(i++, (String) map.get("clientName")); // 3. client_name

            String appId = (String) map.get("appId");
            if (appId != null && !appId.isEmpty()) {
                statement.setString(i++, appId); // 4. app_id
            } else {
                statement.setNull(i++, Types.VARCHAR);
            }

            String apiId = (String) map.get("apiId");
            if (apiId != null && !apiId.isEmpty()) {
                statement.setString(i++, apiId); // 5. api_id
            } else {
                statement.setNull(i++, Types.VARCHAR);
            }

            statement.setString(i++, (String) map.get("clientType")); // 6. client_type
            statement.setString(i++, (String) map.get("clientProfile")); // 7. client_profile
            statement.setString(i++, (String) map.get("clientSecretEncrypted")); // 8. client_secret

            String clientScope = (String) map.get("clientScope");
            if (clientScope != null && !clientScope.isEmpty()) {
                statement.setString(i++, clientScope); // 9. client_scope
            } else {
                statement.setNull(i++, Types.VARCHAR);
            }

            String customClaim = (String) map.get("customClaim");
            if (customClaim != null && !customClaim.isEmpty()) {
                statement.setString(i++, customClaim); // 10. custom_claim
            } else {
                statement.setNull(i++, Types.VARCHAR);
            }

            String redirectUri = (String) map.get("redirectUri");
            if (redirectUri != null && !redirectUri.isEmpty()) {
                statement.setString(i++, redirectUri); // 11. redirect_uri
            } else {
                statement.setNull(i++, Types.VARCHAR);
            }

            String authenticateClass = (String) map.get("authenticateClass");
            if (authenticateClass != null && !authenticateClass.isEmpty()) {
                statement.setString(i++, authenticateClass); // 12. authenticate_class
            } else {
                statement.setNull(i++, Types.VARCHAR);
            }

            String deRefClientId = (String) map.get("deRefClientId");
            if (deRefClientId != null && !deRefClientId.isEmpty()) {
                statement.setObject(i++, UUID.fromString(deRefClientId)); // 13. deref_client_id
            } else {
                statement.setNull(i++, Types.OTHER);
            }

            statement.setString(i++, (String) event.get(Constants.USER)); // 14. update_user
            statement.setObject(i++, OffsetDateTime.parse((String) event.get(CloudEventV1.TIME))); // 15. update_ts
            statement.setLong(i++, newAggregateVersion); // 16. aggregate_version (The New Version)

            // --- Execute UPSERT ---
            int count = statement.executeUpdate();
            if (count == 0) {
                // count=0 means the ON CONFLICT clause was hit, BUT the monotonicity WHERE clause failed.
                // (i.e., DB version >= incoming version). This is the desired idempotent/out-of-order protection behavior.
                logger.warn("Client creation/update skipped for clientId {} aggregateVersion {}. A newer or same version already exists in the projection.", clientId, newAggregateVersion);
            } else {
                logger.info("Client {} successfully inserted or updated to aggregateVersion {}.", clientId, newAggregateVersion);
            }
        } catch (SQLException e) {
            logger.error("SQLException during UPSERT for clientId {} aggregateVersion {}: {}", clientId, newAggregateVersion, e.getMessage(), e);
            throw e; // Re-throw for transaction management
        } catch (Exception e) {
            logger.error("Exception during UPSERT for clientId {} aggregateVersion {}: {}", clientId, newAggregateVersion, e.getMessage(), e);
            throw e; // Re-throw for transaction management
        }
    }

    @Override
    public void updateClient(Connection conn, Map<String, Object> event) throws SQLException, Exception {
        // --- UPDATED SQL FOR MONOTONICITY CHECK ---
        // SQL now includes aggregate_version in SET clause and uses a monotonicity check in the WHERE clause.
        final String updateClient =
                """
                UPDATE auth_client_t SET app_id = ?, api_id = ?, client_name = ?,
                client_type = ?, client_profile = ?,
                client_scope = ?, custom_claim = ?, redirect_uri = ?, authenticate_class = ?,
                deref_client_id = ?, update_user = ?, update_ts = ?, aggregate_version = ?
                WHERE host_id = ? AND client_id = ? AND aggregate_version < ?
                """; // <<< CRITICAL: Changed '= ?' to '< ?' in the final aggregate_version check.

        Map<String, Object> map = SqlUtil.extractEventData(event); // Assuming extractEventData is the helper to get PortalConstants.DATA
        String hostId = (String) map.get("hostId");
        String clientId = (String) map.get("clientId");

        long newAggregateVersion = SqlUtil.getNewAggregateVersion(event);

        try (PreparedStatement statement = conn.prepareStatement(updateClient)) {
            // --- SET CLAUSE PARAMETERS (1 to 13) ---
            int i = 1;

            // 1. app_id
            String appId = (String) map.get("appId");
            if (appId != null && !appId.isEmpty()) {
                statement.setString(i++, appId);
            } else {
                statement.setNull(i++, NULL);
            }

            // 2. api_id
            String apiId = (String) map.get("apiId");
            if (apiId != null && !apiId.isEmpty()) {
                statement.setString(i++, apiId);
            } else {
                statement.setNull(i++, NULL);
            }

            // 3. client_name
            statement.setString(i++, (String) map.get("clientName"));

            // 4. client_type
            statement.setString(i++, (String) map.get("clientType"));

            // 5. client_profile
            statement.setString(i++, (String) map.get("clientProfile"));

            // 6. client_scope
            String clientScope = (String) map.get("clientScope");
            if (clientScope != null && !clientScope.isEmpty()) {
                statement.setString(i++, clientScope);
            } else {
                statement.setNull(i++, NULL);
            }

            // 7. custom_claim
            String customClaim = (String) map.get("customClaim");
            if (customClaim != null && !customClaim.isEmpty()) {
                statement.setString(i++, customClaim);
            } else {
                statement.setNull(i++, NULL);
            }

            // 8. redirect_uri
            String redirectUri = (String) map.get("redirectUri");
            if (redirectUri != null && !redirectUri.isEmpty()) {
                statement.setString(i++, redirectUri);
            } else {
                statement.setNull(i++, NULL);
            }

            // 9. authenticate_class
            String authenticateClass = (String) map.get("authenticateClass");
            if (authenticateClass != null && !authenticateClass.isEmpty()) {
                statement.setString(i++, authenticateClass);
            } else {
                statement.setNull(i++, NULL);
            }

            // 10. deref_client_id
            String deRefClientId = (String) map.get("deRefClientId");
            if (deRefClientId != null && !deRefClientId.isEmpty()) {
                statement.setObject(i++, UUID.fromString(deRefClientId));
            } else {
                statement.setNull(i++, Types.OTHER);
            }

            // 11. update_user
            statement.setString(i++, (String) event.get(Constants.USER));

            // 12. update_ts
            statement.setObject(i++, OffsetDateTime.parse((String) event.get(CloudEventV1.TIME)));

            // 13. aggregate_version (New Version in SET clause)
            statement.setLong(i++, newAggregateVersion);

            // --- WHERE CLAUSE PARAMETERS (15, 16, 17) ---

            // 14. host_id
            statement.setObject(i++, UUID.fromString(hostId));

            // 15. client_id
            statement.setObject(i++, UUID.fromString(clientId));

            // 16. aggregate_version (Monotonicity Check: aggregate_version < newAggregateVersion)
            statement.setLong(i, newAggregateVersion); // Check against the new version

            int count = statement.executeUpdate();
            if (count == 0) {
                // If 0 rows updated, it means the record was either not found OR aggregate_version >= newAggregateVersion.
                // We IGNORE the failure and log a warning, as this is the desired idempotent/monotonic behavior.
                logger.warn("Client update skipped for clientId {} (new version {}). Record not found or a newer version already exists in the projection.", clientId, newAggregateVersion);
            }
            // NO THROW on count == 0. The method is now idempotent.
        } catch (SQLException e) {
            logger.error("SQLException during updateClient for clientId {} (new: {}): {}", clientId, newAggregateVersion, e.getMessage(), e);
            throw e;
        } catch (Exception e) {
            logger.error("Exception during updateClient for clientId {} (new: {}): {}", clientId, newAggregateVersion, e.getMessage(), e);
            throw e;
        }
    }

    @Override
    public void deleteClient(Connection conn, Map<String, Object> event) throws SQLException, Exception {
        // --- UPDATED SQL FOR SOFT DELETE + MONOTONICITY CHECK ---
        // SQL updates the 'active' flag and aggregate_version IF the current DB version is older than the incoming event's version.
        final String softDeleteClient =
                """
                UPDATE auth_client_t SET active = FALSE, update_user = ?, update_ts = ?, aggregate_version = ?
                WHERE host_id = ? AND client_id = ? AND aggregate_version < ?
                """; // <<< CRITICAL: Sets active=FALSE and uses aggregate_version < ?

        Map<String, Object> map = SqlUtil.extractEventData(event); // Assuming extractEventData is the helper to get PortalConstants.DATA
        String hostId = (String) map.get("hostId");
        String clientId = (String) map.get("clientId");
        long newAggregateVersion = SqlUtil.getNewAggregateVersion(event); // The new version for the soft-deleted state

        try (PreparedStatement statement = conn.prepareStatement(softDeleteClient)) {
            // --- SET CLAUSE PARAMETERS (1 to 3) ---
            int i = 1;
            statement.setString(i++, (String) event.get(Constants.USER)); // 1. update_user
            statement.setObject(i++, OffsetDateTime.parse((String) event.get(CloudEventV1.TIME))); // 2. update_ts
            statement.setLong(i++, newAggregateVersion); // 3. aggregate_version (New Version in SET clause)

            // --- WHERE CLAUSE PARAMETERS (4, 5, 6) ---
            statement.setObject(i++, UUID.fromString(hostId)); // 4. host_id
            statement.setObject(i++, UUID.fromString(clientId)); // 5. client_id
            statement.setLong(i, newAggregateVersion); // 6. aggregate_version (Monotonicity Check: aggregate_version < newAggregateVersion)

            int count = statement.executeUpdate();
            if (count == 0) {
                // If 0 rows updated, it means the record was either not found OR aggregate_version >= newAggregateVersion.
                // We IGNORE the failure and log a warning, as this is the desired idempotent/monotonic behavior.
                logger.warn("Deletion skipped for hostId {} clientId {} aggregateVersion {}. Record not found or a newer version already exists.", hostId, clientId, newAggregateVersion);
            }
            // NO THROW on count == 0. The method is now idempotent.
        } catch (SQLException e) {
            logger.error("SQLException during deleteClient for hostId {} clientId {} aggregateVersion {}: {}", hostId, clientId, newAggregateVersion, e.getMessage(), e);
            throw e;
        } catch (Exception e) {
            logger.error("Exception during deleteClient for hostId {} clientId {} aggregateVersion {}: {}", hostId, clientId, newAggregateVersion, e.getMessage(), e);
            throw e;
        }
    }

    /**
     * This method is called by the oauth-kafka for the client authentication. The client must be active in order to
     * be used.
     * @param clientId client id
     * @return Result<String> the result
     */
    @Override
    public Result<String> queryClientByClientId(String clientId) {
        Result<String> result;
        String sql =
                """
                SELECT host_id, app_id, api_id, client_name, client_id, client_type,
                client_profile, client_secret, client_scope, custom_claim, redirect_uri,
                authenticate_class, deref_client_id, update_user, update_ts, aggregate_version
                FROM auth_client_t
                WHERE active = TRUE AND client_id = ?
                """;
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
                        map.put("aggregateVersion", resultSet.getLong("aggregate_version"));
                    }
                }
            }
            if (map.isEmpty())
                result = Failure.of(new Status(OBJECT_NOT_FOUND, "queryClientByClientId", clientId));
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

    /**
     * This is the method for the UI to refresh the client before updating it. So this one returns the
     * active flag without condition.
     *
     * @param hostId host id
     * @param clientId client id
     * @return Result<String> result
     */
    @Override
    public Result<String> getClientById(String hostId, String clientId) {
        if (logger.isTraceEnabled()) logger.trace("getClientById: hostId = {} clientId = {}", hostId, clientId);
        Result<String> result;
        String sql =
                """
                SELECT host_id, app_id, api_id, client_name, client_id, client_type,
                client_profile, client_secret, client_scope, custom_claim, redirect_uri,
                authenticate_class, deref_client_id, update_user, update_ts, aggregate_version, active
                FROM auth_client_t
                WHERE host_id = ? AND client_id = ?
                """;
        try (final Connection conn = ds.getConnection()) {
            Map<String, Object> map = new HashMap<>();
            try (PreparedStatement statement = conn.prepareStatement(sql)) {
                statement.setObject(1, UUID.fromString(hostId));
                statement.setObject(2, UUID.fromString(clientId));
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
                        map.put("aggregateVersion", resultSet.getLong("aggregate_version"));
                        map.put("active", resultSet.getBoolean("active"));
                    }
                }
            }
            if (map.isEmpty())
                result = Failure.of(new Status(OBJECT_NOT_FOUND, "getClientById", hostId + "|" + clientId));
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
                """
                SELECT c.host_id, a.provider_id, a.client_id, c.client_type, c.client_profile,\s
                c.client_secret, c.client_scope, c.custom_claim, c.redirect_uri,\s
                c.authenticate_class, c.deref_client_id, a.aggregate_version
                FROM auth_client_t c, auth_provider_client_t a
                WHERE c.host_id = a.host_id AND c.client_id = a.client_id
                AND a.provider_id = ?
                AND a.client_id = ?
                """;
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
                        map.put("aggregateVersion", resultSet.getLong("aggregate_version"));
                    }
                }
            }
            if (map.isEmpty())
                result = Failure.of(new Status(OBJECT_NOT_FOUND, "client", "providerId " + providerId + "clientId " + clientId));
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
                """
                SELECT host_id, app_id, client_id, client_type, client_profile, client_scope, custom_claim,
                redirect_uri, authenticate_class, deref_client_id, update_user, update_ts, aggregate_version
                FROM auth_client_t c
                WHERE host_id = ? AND app_id = ?
                """;
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
                        map.put("aggregateVersion", resultSet.getLong("aggregate_version"));
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

    /**
     * As the provider_id is generated based on UUID. There should not have any duplications and provider name cannot be
     * used as a key for the conflict to recover from the soft delete. So it doesn't support the upsert here to bring back
     * deleted record. If you want to recover a deleted record, please use the update.
     * @param conn Connection
     * @param event Event object
     * @throws SQLException SQL exception
     * @throws Exception generic exception
     */
    @Override
    public void createAuthProvider(Connection conn, Map<String, Object> event) throws SQLException, Exception {

        // Use UPSERT based on the Primary Key (provider_id): INSERT ON CONFLICT DO UPDATE
        // This handles:
        // 1. First time insert (no conflict).
        // 2. Re-creation (conflict on provider_id) -> UPDATE the existing soft-deleted row (setting active=TRUE and new version).

        final String sql =
                """
                INSERT INTO auth_provider_t (
                    provider_id,
                    host_id,
                    provider_name,
                    provider_desc,
                    operation_owner,
                    delivery_owner,
                    jwk,
                    update_user,
                    update_ts,
                    aggregate_version,
                    active
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, TRUE)
                ON CONFLICT (provider_id) DO UPDATE
                SET host_id = EXCLUDED.host_id,
                    provider_name = EXCLUDED.provider_name,
                    provider_desc = EXCLUDED.provider_desc,
                    operation_owner = EXCLUDED.operation_owner,
                    delivery_owner = EXCLUDED.delivery_owner,
                    jwk = EXCLUDED.jwk,
                    update_user = EXCLUDED.update_user,
                    update_ts = EXCLUDED.update_ts,
                    aggregate_version = EXCLUDED.aggregate_version,
                    active = TRUE
                -- OCC/IDM: Only update if the incoming event is newer
                WHERE auth_provider_t.active = FALSE
                AND auth_provider_t.aggregate_version < EXCLUDED.aggregate_version
                """;

        // Note: Assuming SqlUtil.extractEventData(event) is the correct utility based on other methods.
        Map<String, Object> map = SqlUtil.extractEventData(event);

        String providerId = (String)map.get("providerId");
        String hostId = (String)map.get("hostId");
        long newAggregateVersion = SqlUtil.getNewAggregateVersion(event);

        try (PreparedStatement statement = conn.prepareStatement(sql)) {
            // INSERT values (10 placeholders + active=TRUE in SQL, total 10 dynamic values)
            int i = 1;
            // 1: provider_id
            statement.setString(i++, providerId);
            // 2: host_id
            statement.setObject(i++, UUID.fromString(hostId));
            // 3: provider_name
            statement.setString(i++, (String)map.get("providerName"));

            // 4: provider_desc
            if (map.containsKey("providerDesc")) {
                statement.setString(i++, (String)map.get("providerDesc"));
            } else {
                statement.setNull(i++, Types.VARCHAR);
            }

            // 5: operation_owner
            if (map.containsKey("operationOwner")) {
                statement.setObject(i++, UUID.fromString((String)map.get("operationOwner")));
            } else {
                statement.setNull(i++, Types.OTHER);
            }

            // 6: delivery_owner
            if (map.containsKey("deliveryOwner")) {
                statement.setObject(i++, UUID.fromString((String)map.get("deliveryOwner")));
            } else {
                statement.setNull(i++, Types.OTHER);
            }

            // 7: jwk
            statement.setString(i++, (String)map.get("jwk")); // Required

            // 8: update_user
            statement.setString(i++, (String)event.get(Constants.USER));
            // 9: update_ts
            statement.setObject(i++, OffsetDateTime.parse((String)event.get(CloudEventV1.TIME)));
            // 10: aggregate_version
            statement.setLong(i++, newAggregateVersion);

            int count = statement.executeUpdate();
            if (count == 0) {
                // count=0 means the ON CONFLICT clause was hit, BUT the WHERE clause (aggregate_version < EXCLUDED.aggregate_version) failed.
                // This is the desired idempotent/out-of-order protection behavior. Log and ignore.
                logger.warn("Creation/Reactivation skipped for providerId {} aggregateVersion {}. A newer or same version already exists.", providerId, newAggregateVersion);
            } else {
                // Insert keys into auth_provider_key_t
                String keySql =
                    """
                    INSERT INTO auth_provider_key_t(provider_id, kid, public_key, private_key, key_type, update_user, update_ts)
                    VALUES (?, ?, ?, ?, ?,  ?, ?)
                    """;

                try (PreparedStatement keyStatement = conn.prepareStatement(keySql)) {
                    Map<String, Object> keys = (Map<String, Object>) map.get("keys");

                    keyStatement.setString(1, providerId);

                    Map<String, Object> lcMap = (Map<String, Object>) keys.get("LC");
                    // add long live current key
                    keyStatement.setString(2, (String) lcMap.get("kid"));
                    keyStatement.setString(3, (String) lcMap.get("publicKey"));
                    keyStatement.setString(4, (String) lcMap.get("privateKey"));
                    keyStatement.setString(5, (String) lcMap.get("keyType"));
                    keyStatement.setString(6, (String) event.get(Constants.USER));
                    keyStatement.setObject(7, OffsetDateTime.parse((String) event.get(CloudEventV1.TIME)));
                    keyStatement.executeUpdate();

                    // add long live previous key
                    Map<String, Object> lpMap = (Map<String, Object>) keys.get("LP");
                    keyStatement.setString(2, (String) lpMap.get("kid"));
                    keyStatement.setString(3, (String) lpMap.get("publicKey"));
                    keyStatement.setString(4, (String) lpMap.get("privateKey"));
                    keyStatement.setString(5, (String) lpMap.get("keyType"));
                    keyStatement.setString(6, (String) event.get(Constants.USER));
                    keyStatement.setObject(7, OffsetDateTime.parse((String) event.get(CloudEventV1.TIME)));
                    keyStatement.executeUpdate();

                    // add token current key
                    Map<String, Object> tcMap = (Map<String, Object>) keys.get("TC");
                    keyStatement.setString(2, (String) tcMap.get("kid"));
                    keyStatement.setString(3, (String) tcMap.get("publicKey"));
                    keyStatement.setString(4, (String) tcMap.get("privateKey"));
                    keyStatement.setString(5, (String) tcMap.get("keyType"));
                    keyStatement.setString(6, (String) event.get(Constants.USER));
                    keyStatement.setObject(7, OffsetDateTime.parse((String) event.get(CloudEventV1.TIME)));
                    keyStatement.executeUpdate();

                    // add token previous key
                    Map<String, Object> tpMap = (Map<String, Object>) keys.get("TP");
                    keyStatement.setString(2, (String) tpMap.get("kid"));
                    keyStatement.setString(3, (String) tpMap.get("publicKey"));
                    keyStatement.setString(4, (String) tpMap.get("privateKey"));
                    keyStatement.setString(5, (String) tpMap.get("keyType"));
                    keyStatement.setString(6, (String) event.get(Constants.USER));
                    keyStatement.setObject(7, OffsetDateTime.parse((String) event.get(CloudEventV1.TIME)));
                    keyStatement.executeUpdate();
                } catch (SQLException ex) {
                    logger.error("SQLException during createAuthProvider key insert for provider id {}: {}", providerId, ex.getMessage(), ex);
                    throw new SQLException("Failed to insert the auth provider key with provider id " + providerId, ex);
                }
            }
        } catch (SQLException e) {
            logger.error("SQLException during createAuthProvider for providerId {} aggregateVersion {}: {}", providerId, newAggregateVersion, e.getMessage(), e);
            throw e;
        } catch (Exception e) {
            logger.error("Exception during createAuthProvider for providerId {} aggregateVersion {}: {}", providerId, newAggregateVersion, e.getMessage(), e);
            throw e;
        }
    }

    @Override
    public void rotateAuthProvider(Connection conn, Map<String, Object> event) throws SQLException, Exception {
        // update the auth_provider_t table with the new jwk
        final String sqlJwk = "UPDATE auth_provider_t SET jwk = ?, update_user = ?, update_ts = ?, aggregate_version = ?, active = TRUE" +
                "WHERE provider_id = ? AND aggregate_version < ?";
        // insert the new key into auth_provider_key_t
        final String sqlInsert = "INSERT INTO auth_provider_key_t(provider_id, kid, public_key, private_key, key_type, update_user, update_ts) " +
                "VALUES (?, ?, ?, ?, ?,  ?, ?, ?)";
        // update the existing key in auth_provider_key_t
        final String sqlUpdate = "UPDATE auth_provider_key_t SET key_type = ?, update_user = ?, update_ts = ?" +
                "WHERE provider_id = ? AND kid = ?";
        // delete the old key from auth_provider_key_t
        final String sqlDelete = "DELETE FROM auth_provider_key_t WHERE provider_id = ? AND kid = ?";

        Map<String, Object> map = (Map<String, Object>) event.get(PortalConstants.DATA);
        String providerId = (String) map.get("providerId");
        long newAggregateVersion = SqlUtil.getNewAggregateVersion(event);

        try (PreparedStatement statement = conn.prepareStatement(sqlJwk)) {
            String jwk = (String) map.get("jwk");
            statement.setString(1, jwk);
            statement.setString(2, (String) event.get(Constants.USER));
            statement.setObject(3, OffsetDateTime.parse((String) event.get(CloudEventV1.TIME)));
            statement.setLong(4, newAggregateVersion);
            statement.setString(4, providerId);
            statement.setLong(5, newAggregateVersion);

            int count = statement.executeUpdate();
            if (count == 0) {
                // If 0 rows updated, it means the record was either not found OR aggregate_version >= newAggregateVersion.
                // We IGNORE the failure and log a warning, as this is the desired idempotent/monotonic behavior.
                logger.warn("rotateAuthProvider skipped for providerId {} aggregateVersion {}. Record not found or a newer version already exists.", providerId, newAggregateVersion);
            }

            try (PreparedStatement statementInsert = conn.prepareStatement(sqlInsert)) {
                Map<String, Object> insertMap = (Map<String, Object>) map.get("insert");
                statementInsert.setString(1, providerId);
                statementInsert.setString(2, (String) insertMap.get("kid"));
                statementInsert.setString(3, (String) insertMap.get("publicKey"));
                statementInsert.setString(4, (String) insertMap.get("privateKey"));
                statementInsert.setString(5, (String) insertMap.get("keyType"));
                statementInsert.setString(6, (String) event.get(Constants.USER));
                statementInsert.setObject(7, OffsetDateTime.parse((String) event.get(CloudEventV1.TIME)));

                count = statementInsert.executeUpdate();
                if (count == 0) {
                    throw new SQLException("Failed to insert the auth provider key with provider id " + providerId);
                }
            }
            try (PreparedStatement statementUpdate = conn.prepareStatement(sqlUpdate)) {
                Map<String, Object> updateMap = (Map<String, Object>) map.get("update");
                statementUpdate.setString(1, (String) updateMap.get("keyType"));
                statementUpdate.setString(2, (String) event.get(Constants.USER));
                statementUpdate.setObject(3, OffsetDateTime.parse((String) event.get(CloudEventV1.TIME)));
                statementUpdate.setString(4, providerId);
                statementUpdate.setString(5, (String) updateMap.get("kid"));
                count = statementUpdate.executeUpdate();
                if (count == 0) {
                    throw new SQLException("Failed to update the auth provider key with provider id " + providerId);
                }
            }
            try (PreparedStatement statementDelete = conn.prepareStatement(sqlDelete)) {
                Map<String, Object> deleteMap = (Map<String, Object>) map.get("delete");
                statementDelete.setString(1, providerId);
                statementDelete.setString(2, (String) deleteMap.get("kid"));
                count = statementDelete.executeUpdate();
                if (count == 0) {
                    throw new SQLException("Failed to delete the auth provider key with provider id " + providerId);
                }
            }
        } catch (SQLException e) {
            logger.error("SQLException during rotateAuthProvider for id {}: {}", providerId, e.getMessage(), e);
            throw e;
        } catch (Exception e) {
            logger.error("Exception during rotateAuthProvider for id {}: {}", providerId, e.getMessage(), e);
            throw e;
        }
    }

    @Override
    public void updateAuthProvider(Connection conn, Map<String, Object> event) throws SQLException, Exception {
        // We attempt to update the record IF the incoming event's aggregate_version is greater than the current projection's version.
        // This enforces Idempotence (IDM) and Optimistic Concurrency Control (OCC) by ensuring version monotonicity.
        // We explicitly set active = TRUE as an UPDATE event implies the auth provider should be active.
        final String sql =
                """
                UPDATE auth_provider_t
                SET host_id = ?,
                    provider_name = ?,
                    provider_desc = ?,
                    operation_owner = ?,
                    delivery_owner = ?,
                    jwk = ?,
                    update_user = ?,
                    update_ts = ?,
                    aggregate_version = ?,
                    active = TRUE
                WHERE provider_id = ?
                  AND aggregate_version < ?
                """; // <<< CRITICAL: Added aggregate_version < ? to enforce monotonicity (OCC/IDM)

        // Note: Assuming SqlUtil.extractEventData(event) is the correct utility based on other methods.
        Map<String, Object> map = SqlUtil.extractEventData(event);

        String providerId = (String)map.get("providerId");
        long newAggregateVersion = SqlUtil.getNewAggregateVersion(event);

        try (PreparedStatement statement = conn.prepareStatement(sql)) {
            // SET values (9 dynamic values + active = TRUE in SQL)
            int i = 1;

            // 1: host_id
            String hostId = (String)map.get("hostId");
            statement.setObject(i++, UUID.fromString(hostId)); // Required

            // 2: provider_name
            statement.setString(i++, (String)map.get("providerName")); // Required

            // 3: provider_desc
            if (map.containsKey("providerDesc")) {
                statement.setString(i++, (String)map.get("providerDesc"));
            } else {
                statement.setNull(i++, Types.VARCHAR);
            }

            // 4: operation_owner
            if (map.containsKey("operationOwner")) {
                statement.setObject(i++, UUID.fromString((String)map.get("operationOwner")));
            } else {
                statement.setNull(i++, Types.OTHER);
            }

            // 5: delivery_owner
            if (map.containsKey("deliveryOwner")) {
                statement.setObject(i++, UUID.fromString((String)map.get("deliveryOwner")));
            } else {
                statement.setNull(i++, Types.OTHER);
            }

            // 6: jwk
            statement.setString(i++, (String)map.get("jwk")); // Required

            // 7: update_user
            statement.setString(i++, (String)event.get(Constants.USER));
            // 8: update_ts
            statement.setObject(i++, OffsetDateTime.parse((String)event.get(CloudEventV1.TIME)));
            // 9: aggregate_version
            statement.setLong(i++, newAggregateVersion);

            // WHERE conditions (2 placeholders)
            // 10: provider_id (PK)
            statement.setString(i++, providerId);
            // 11: aggregate_version < ? (new version for OCC/IDM check)
            statement.setLong(i++, newAggregateVersion);

            int count = statement.executeUpdate();
            if (count == 0) {
                // If 0 rows updated, it means the record was either not found
                // OR aggregate_version >= newAggregateVersion (OCC/IDM check failed).
                // We IGNORE the failure and log a warning, as this is the desired idempotent/monotonic behavior.
                logger.warn("Update skipped for providerId {} aggregateVersion {}. Record not found or a newer/same version already exists.", providerId, newAggregateVersion);
            }
        } catch (SQLException e) {
            logger.error("SQLException during updateAuthProvider for providerId {} aggregateVersion {}: {}", providerId, newAggregateVersion, e.getMessage(), e);
            throw e;
        } catch (Exception e) {
            logger.error("Exception during updateAuthProvider for providerId {} aggregateVersion {}: {}", providerId, newAggregateVersion, e.getMessage(), e);
            throw e;
        }
    }


    @Override
    public void deleteAuthProvider(Connection conn, Map<String, Object> event) throws SQLException, Exception {
        // Use UPDATE to implement Soft Delete (setting active = FALSE).
        // OCC/IDM is enforced by checking aggregate_version < newAggregateVersion.
        final String sql =
                """
                UPDATE auth_provider_t
                SET active = FALSE,
                    update_user = ?,
                    update_ts = ?,
                    aggregate_version = ?
                WHERE provider_id = ?
                  AND aggregate_version < ?
                """; // <<< CRITICAL: Added aggregate_version < ? to enforce monotonicity (OCC/IDM)

        // Note: Assuming SqlUtil.extractEventData(event) is the correct utility based on other methods.
        Map<String, Object> map = SqlUtil.extractEventData(event);

        String providerId = (String)map.get("providerId");
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
            // 4: provider_id
            statement.setString(4, providerId);
            // 5: aggregate_version < ? (new version for OCC/IDM check)
            statement.setLong(5, newAggregateVersion);

            int count = statement.executeUpdate();
            if (count == 0) {
                // If 0 rows updated, it means:
                // 1. The record was not found (already deleted or never existed).
                // 2. The OCC/IDM check failed (aggregate_version >= newAggregateVersion).
                // We IGNORE the failure and log a warning, as this is the desired idempotent/monotonic behavior.
                logger.warn("Soft delete skipped for providerId {} aggregateVersion {}. Record not found or a newer/same version already exists.", providerId, newAggregateVersion);
            } else {
                // delete all keys from auth_provider_key_t
                String sqlDelete = "DELETE FROM auth_provider_key_t WHERE provider_id = ?";
                try (PreparedStatement statementDelete = conn.prepareStatement(sqlDelete)) {
                    statementDelete.setString(1, providerId);
                    statementDelete.executeUpdate();
                }
            }
        } catch (SQLException e) {
            logger.error("SQLException during deleteAuthProvider for providerId {} aggregateVersion {}: {}", providerId, newAggregateVersion, e.getMessage(), e);
            throw e;
        } catch (Exception e) {
            logger.error("Exception during deleteAuthProvider for providerId {} aggregateVersion {}: {}", providerId, newAggregateVersion, e.getMessage(), e);
            throw e;
        }
    }

    @Override
    public Result<String> queryProviderKey(String providerId) {
        Result<String> result = null;
        String sql =
                """
                SELECT provider_id, kid, public_key, private_key, key_type, update_user, update_ts
                FROM auth_provider_key_t
                WHERE provider_id = ?
                """;

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
                    map.put("updateTs", resultSet.getObject("update_ts") != null ? resultSet.getObject("update_ts", OffsetDateTime.class) : null);
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
    public void createAuthProviderApi(Connection conn, Map<String, Object> event) throws SQLException, Exception {
        // Use UPSERT based on the Primary Key (host_id, api_id, provider_id): INSERT ON CONFLICT DO UPDATE
        // This handles:
        // 1. First time insert (no conflict).
        // 2. Re-creation (conflict on PK) -> UPDATE the existing soft-deleted row (setting active=TRUE and new version).

        final String sql =
                """
                INSERT INTO auth_provider_api_t (
                    host_id,
                    api_id,
                    provider_id,
                    update_user,
                    update_ts,
                    aggregate_version,
                    active
                ) VALUES (?, ?, ?, ?, ?, ?, TRUE)
                ON CONFLICT (host_id, api_id, provider_id) DO UPDATE
                SET update_user = EXCLUDED.update_user,
                    update_ts = EXCLUDED.update_ts,
                    aggregate_version = EXCLUDED.aggregate_version,
                    active = TRUE
                -- OCC/IDM: Only update if the incoming event is newer
                WHERE auth_provider_api_t.aggregate_version < EXCLUDED.aggregate_version
                AND auth_provider_api_t.active = FALSE
                """;

        // Note: Assuming SqlUtil.extractEventData(event) is the correct utility based on other methods.
        Map<String, Object> map = SqlUtil.extractEventData(event);

        String hostId = (String)map.get("hostId");
        String apiId = (String)map.get("apiId");
        String providerId = (String)map.get("providerId");
        long newAggregateVersion = SqlUtil.getNewAggregateVersion(event);

        try (PreparedStatement statement = conn.prepareStatement(sql)) {
            // INSERT values (6 placeholders + active=TRUE in SQL, total 6 dynamic values)
            int i = 1;
            // 1: host_id (PK part)
            statement.setObject(i++, UUID.fromString(hostId));
            // 2: api_id (PK part)
            statement.setString(i++, apiId);
            // 3: provider_id (PK part)
            statement.setString(i++, providerId);

            // 4: update_user
            statement.setString(i++, (String)event.get(Constants.USER));
            // 5: update_ts
            statement.setObject(i++, OffsetDateTime.parse((String)event.get(CloudEventV1.TIME)));
            // 6: aggregate_version
            statement.setLong(i++, newAggregateVersion);

            int count = statement.executeUpdate();
            if (count == 0) {
                // count=0 means the ON CONFLICT clause was hit, BUT the WHERE clause (aggregate_version < EXCLUDED.aggregate_version) failed.
                // This is the desired idempotent/out-of-order protection behavior. Log and ignore.
                logger.warn("Creation/Reactivation skipped for hostId {} apiId {} providerId {} aggregateVersion {}. A newer or same version already exists.", hostId, apiId, providerId, newAggregateVersion);
            }
        } catch (SQLException e) {
            logger.error("SQLException during createAuthProviderApi for hostId {} apiId {} providerId {} aggregateVersion {}: {}", hostId, apiId, providerId, newAggregateVersion, e.getMessage(), e);
            throw e;
        } catch (Exception e) {
            logger.error("Exception during createAuthProviderApi for hostId {} apiId {} providerId {} aggregateVersion {}: {}", hostId, apiId, providerId, newAggregateVersion, e.getMessage(), e);
            throw e;
        }
    }

    @Override
    public void updateAuthProviderApi(Connection conn, Map<String, Object> event) throws SQLException, Exception {
        // We attempt to update the record IF the incoming event's aggregate_version is greater than the current projection's version.
        // This enforces Idempotence (IDM) and Optimistic Concurrency Control (OCC) by ensuring version monotonicity.
        // We explicitly set active = TRUE as an UPDATE event implies the assignment should be active.
        final String sql =
                """
                UPDATE auth_provider_api_t
                SET update_user = ?,
                    update_ts = ?,
                    aggregate_version = ?,
                    active = TRUE
                WHERE host_id = ?
                  AND api_id = ?
                  AND provider_id = ?
                  AND aggregate_version < ?
                """; // <<< CRITICAL: Added aggregate_version < ? to enforce monotonicity (OCC/IDM)

        // Note: Assuming SqlUtil.extractEventData(event) is the correct utility based on other methods.
        Map<String, Object> map = SqlUtil.extractEventData(event);

        String hostId = (String)map.get("hostId");
        String apiId = (String)map.get("apiId");
        String providerId = (String)map.get("providerId");
        long newAggregateVersion = SqlUtil.getNewAggregateVersion(event);

        try (PreparedStatement statement = conn.prepareStatement(sql)) {
            // SET values (3 dynamic values + active = TRUE in SQL)
            int i = 1;
            // 1: update_user
            statement.setString(i++, (String)event.get(Constants.USER));
            // 2: update_ts
            statement.setObject(i++, OffsetDateTime.parse((String)event.get(CloudEventV1.TIME)));
            // 3: aggregate_version
            statement.setLong(i++, newAggregateVersion);

            // WHERE conditions (4 placeholders)
            // 4: host_id (PK part)
            statement.setObject(i++, UUID.fromString(hostId));
            // 5: api_id (PK part)
            statement.setString(i++, apiId);
            // 6: provider_id (PK part)
            statement.setString(i++, providerId);
            // 7: aggregate_version < ? (new version for OCC/IDM check)
            statement.setLong(i++, newAggregateVersion);

            int count = statement.executeUpdate();
            if (count == 0) {
                // If 0 rows updated, it means the record was either not found
                // OR aggregate_version >= newAggregateVersion (OCC/IDM check failed).
                // We IGNORE the failure and log a warning, as this is the desired idempotent/monotonic behavior.
                logger.warn("Update skipped for hostId {} apiId {} providerId {} aggregateVersion {}. Record not found or a newer/same version already exists.", hostId, apiId, providerId, newAggregateVersion);
            }
        } catch (SQLException e) {
            logger.error("SQLException during updateAuthProviderApi for hostId {} apiId {} providerId {} aggregateVersion {}: {}", hostId, apiId, providerId, newAggregateVersion, e.getMessage(), e);
            throw e;
        } catch (Exception e) {
            logger.error("Exception during updateAuthProviderApi for hostId {} apiId {} providerId {} aggregateVersion {}: {}", hostId, apiId, providerId, newAggregateVersion, e.getMessage(), e);
            throw e;
        }
    }

    @Override
    public void deleteAuthProviderApi(Connection conn, Map<String, Object> event) throws SQLException, Exception {
        // Use UPDATE to implement Soft Delete (setting active = FALSE).
        // OCC/IDM is enforced by checking aggregate_version < newAggregateVersion.
        final String sql =
                """
                UPDATE auth_provider_api_t
                SET active = FALSE,
                    update_user = ?,
                    update_ts = ?,
                    aggregate_version = ?
                WHERE host_id = ?
                  AND api_id = ?
                  AND provider_id = ?
                  AND aggregate_version < ?
                """; // <<< CRITICAL: Added aggregate_version < ? to enforce monotonicity (OCC/IDM)

        // Note: Assuming SqlUtil.extractEventData(event) is the correct utility based on other methods.
        Map<String, Object> map = SqlUtil.extractEventData(event);

        String hostId = (String)map.get("hostId");
        String apiId = (String)map.get("apiId");
        String providerId = (String)map.get("providerId");
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
            // 4: host_id (PK part)
            statement.setObject(4, UUID.fromString(hostId));
            // 5: api_id (PK part)
            statement.setString(5, apiId);
            // 6: provider_id (PK part)
            statement.setString(6, providerId);
            // 7: aggregate_version < ? (new version for OCC/IDM check)
            statement.setLong(7, newAggregateVersion);

            int count = statement.executeUpdate();
            if (count == 0) {
                // If 0 rows updated, it means:
                // 1. The record was not found (already deleted or never existed).
                // 2. The OCC/IDM check failed (aggregate_version >= newAggregateVersion).
                // We IGNORE the failure and log a warning, as this is the desired idempotent/monotonic behavior.
                logger.warn("Soft delete skipped for hostId {} apiId {} providerId {} aggregateVersion {}. Record not found or a newer/same version already exists.", hostId, apiId, providerId, newAggregateVersion);
            }
        } catch (SQLException e) {
            logger.error("SQLException during deleteAuthProviderApi for hostId {} apiId {} providerId {} aggregateVersion {}: {}", hostId, apiId, providerId, newAggregateVersion, e.getMessage(), e);
            throw e;
        } catch (Exception e) {
            logger.error("Exception during deleteAuthProviderApi for hostId {} apiId {} providerId {} aggregateVersion {}: {}", hostId, apiId, providerId, newAggregateVersion, e.getMessage(), e);
            throw e;
        }
    }

    @Override
    public Result<String> queryAuthProviderApi(int offset, int limit, String filtersJson, String globalFilter, String sortingJson, boolean active, String hostId) {
        Result<String> result = null;

        // Assuming helper methods parseJsonList, dynamicFilter, globalFilter, dynamicSorting,
        // populateParameters, JsonMapper, Success, Failure, Status, SQL_EXCEPTION, GENERIC_EXCEPTION,
        // and Constants are available in the scope.
        List<Map<String, Object>> filters = SqlUtil.parseJsonList(filtersJson);
        List<Map<String, Object>> sorting = SqlUtil.parseJsonList(sortingJson);

        String s =
                """
                SELECT COUNT(*) OVER () AS total,
                host_id, api_id, provider_id, aggregate_version,
                active, update_user, update_ts
                FROM auth_provider_api_t
                WHERE host_id = ?
                """;
        List<Object> parameters = new ArrayList<>();
        parameters.add(UUID.fromString(hostId));

        String activeClause = SqlUtil.buildMultiTableActiveClause(active);
        String[] searchColumns = {"api_id"};
        // Note: Dynamic filtering assumes a column prefix 't.' might be needed if joins were present,
        // but it's simpler to assume it works without prefix or adjusts internally based on the template.
        String sqlBuilder = s + activeClause +
                SqlUtil.dynamicFilter(Arrays.asList("host_id"), Arrays.asList(searchColumns), filters, null, parameters) +
                SqlUtil.globalFilter(globalFilter, searchColumns, parameters) +
                SqlUtil.dynamicSorting("provider_id, api_id", sorting, null) +
                "\nLIMIT ? OFFSET ?";

        parameters.add(limit);
        parameters.add(offset);

        int total = 0;
        List<Map<String, Object>> authProviderApis = new ArrayList<>();

        try (Connection connection = ds.getConnection();
             PreparedStatement preparedStatement = connection.prepareStatement(sqlBuilder)) {
            SqlUtil.populateParameters(preparedStatement, parameters);
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
                    map.put("providerId", resultSet.getString("provider_id"));
                    map.put("aggregateVersion", resultSet.getLong("aggregate_version"));
                    map.put("active", resultSet.getBoolean("active"));
                    map.put("updateUser", resultSet.getString("update_user"));
                    map.put("updateTs", resultSet.getObject("update_ts") != null ? resultSet.getObject("update_ts", OffsetDateTime.class) : null);
                    authProviderApis.add(map);
                }
            }

            Map<String, Object> resultMap = new HashMap<>();
            resultMap.put("total", total);
            resultMap.put("authProviderApis", authProviderApis);
            result = Success.of(JsonMapper.toJson(resultMap)); // Assuming JsonMapper and Success are available

        } catch (SQLException e) {
            logger.error("SQLException:", e);
            result = Failure.of(new Status(SQL_EXCEPTION, e.getMessage())); // Assuming Status and exception constants are available
        } catch (Exception e) {
            logger.error("Exception:", e);
            result = Failure.of(new Status(GENERIC_EXCEPTION, e.getMessage()));
        }
        return result;
    }

    @Override
    public void createAuthProviderClient(Connection conn, Map<String, Object> event) throws SQLException, Exception {
        // Use UPSERT based on the Primary Key (host_id, client_id, provider_id): INSERT ON CONFLICT DO UPDATE
        // This handles:
        // 1. First time insert (no conflict).
        // 2. Re-creation (conflict on PK) -> UPDATE the existing soft-deleted row (setting active=TRUE and new version).

        final String sql =
                """
                INSERT INTO auth_provider_client_t (
                    host_id,
                    client_id,
                    provider_id,
                    update_user,
                    update_ts,
                    aggregate_version,
                    active
                ) VALUES (?, ?, ?, ?, ?, ?, TRUE)
                ON CONFLICT (host_id, client_id, provider_id) DO UPDATE
                SET update_user = EXCLUDED.update_user,
                    update_ts = EXCLUDED.update_ts,
                    aggregate_version = EXCLUDED.aggregate_version,
                    active = TRUE
                -- OCC/IDM: Only update if the incoming event is newer
                WHERE auth_provider_client_t.aggregate_version < EXCLUDED.aggregate_version
                AND auth_provider_client_t.active = FALSE
                """;

        // Note: Assuming SqlUtil.extractEventData(event) is the correct utility based on other methods.
        Map<String, Object> map = SqlUtil.extractEventData(event);

        String hostId = (String)map.get("hostId");
        String clientId = (String)map.get("clientId");
        String providerId = (String)map.get("providerId");
        long newAggregateVersion = SqlUtil.getNewAggregateVersion(event);

        try (PreparedStatement statement = conn.prepareStatement(sql)) {
            // INSERT values (6 placeholders + active=TRUE in SQL, total 6 dynamic values)
            int i = 1;
            // 1: host_id (PK part)
            statement.setObject(i++, UUID.fromString(hostId));
            // 2: client_id (PK part)
            statement.setObject(i++, UUID.fromString(clientId));
            // 3: provider_id (PK part)
            statement.setString(i++, providerId);

            // 4: update_user
            statement.setString(i++, (String)event.get(Constants.USER));
            // 5: update_ts
            statement.setObject(i++, OffsetDateTime.parse((String)event.get(CloudEventV1.TIME)));
            // 6: aggregate_version
            statement.setLong(i++, newAggregateVersion);

            int count = statement.executeUpdate();
            if (count == 0) {
                // count=0 means the ON CONFLICT clause was hit, BUT the WHERE clause (aggregate_version < EXCLUDED.aggregate_version) failed.
                // This is the desired idempotent/out-of-order protection behavior. Log and ignore.
                logger.warn("Creation/Reactivation skipped for hostId {} clientId {} providerId {} aggregateVersion {}. A newer or same version already exists.", hostId, clientId, providerId, newAggregateVersion);
            }
        } catch (SQLException e) {
            logger.error("SQLException during createAuthProviderClient for hostId {} clientId {} providerId {} aggregateVersion {}: {}", hostId, clientId, providerId, newAggregateVersion, e.getMessage(), e);
            throw e;
        } catch (Exception e) {
            logger.error("Exception during createAuthProviderClient for hostId {} clientId {} providerId {} aggregateVersion {}: {}", hostId, clientId, providerId, newAggregateVersion, e.getMessage(), e);
            throw e;
        }
    }

    @Override
    public void updateAuthProviderClient(Connection conn, Map<String, Object> event) throws SQLException, Exception {
        // We attempt to update the record IF the incoming event's aggregate_version is greater than the current projection's version.
        // This enforces Idempotence (IDM) and Optimistic Concurrency Control (OCC) by ensuring version monotonicity.
        // We explicitly set active = TRUE as an UPDATE event implies the assignment should be active.
        final String sql =
                """
                UPDATE auth_provider_client_t
                SET update_user = ?,
                    update_ts = ?,
                    aggregate_version = ?,
                    active = TRUE
                WHERE host_id = ?
                  AND client_id = ?
                  AND provider_id = ?
                  AND aggregate_version < ?
                """; // <<< CRITICAL: Added aggregate_version < ? to enforce monotonicity (OCC/IDM)

        // Note: Assuming SqlUtil.extractEventData(event) is the correct utility based on other methods.
        Map<String, Object> map = SqlUtil.extractEventData(event);

        String hostId = (String)map.get("hostId");
        String clientId = (String)map.get("clientId");
        String providerId = (String)map.get("providerId");
        long newAggregateVersion = SqlUtil.getNewAggregateVersion(event);

        try (PreparedStatement statement = conn.prepareStatement(sql)) {
            // SET values (3 dynamic values + active = TRUE in SQL)
            int i = 1;
            // 1: update_user
            statement.setString(i++, (String)event.get(Constants.USER));
            // 2: update_ts
            statement.setObject(i++, OffsetDateTime.parse((String)event.get(CloudEventV1.TIME)));
            // 3: aggregate_version
            statement.setLong(i++, newAggregateVersion);

            // WHERE conditions (4 placeholders)
            // 4: host_id (PK part)
            statement.setObject(i++, UUID.fromString(hostId));
            // 5: client_id (PK part)
            statement.setObject(i++, UUID.fromString(clientId));
            // 6: provider_id (PK part)
            statement.setString(i++, providerId);
            // 7: aggregate_version < ? (new version for OCC/IDM check)
            statement.setLong(i++, newAggregateVersion);

            int count = statement.executeUpdate();
            if (count == 0) {
                // If 0 rows updated, it means the record was either not found
                // OR aggregate_version >= newAggregateVersion (OCC/IDM check failed).
                // We IGNORE the failure and log a warning, as this is the desired idempotent/monotonic behavior.
                logger.warn("Update skipped for hostId {} clientId {} providerId {} aggregateVersion {}. Record not found or a newer/same version already exists.", hostId, clientId, providerId, newAggregateVersion);
            }
        } catch (SQLException e) {
            logger.error("SQLException during updateAuthProviderClient for hostId {} clientId {} providerId {} aggregateVersion {}: {}", hostId, clientId, providerId, newAggregateVersion, e.getMessage(), e);
            throw e;
        } catch (Exception e) {
            logger.error("Exception during updateAuthProviderClient for hostId {} clientId {} providerId {} aggregateVersion {}: {}", hostId, clientId, providerId, newAggregateVersion, e.getMessage(), e);
            throw e;
        }
    }

    @Override
    public void deleteAuthProviderClient(Connection conn, Map<String, Object> event) throws SQLException, Exception {
        // Use UPDATE to implement Soft Delete (setting active = FALSE).
        // OCC/IDM is enforced by checking aggregate_version < newAggregateVersion.
        final String sql =
                """
                UPDATE auth_provider_client_t
                SET active = FALSE,
                    update_user = ?,
                    update_ts = ?,
                    aggregate_version = ?
                WHERE host_id = ?
                  AND client_id = ?
                  AND provider_id = ?
                  AND aggregate_version < ?
                """; // <<< CRITICAL: Added aggregate_version < ? to enforce monotonicity (OCC/IDM)

        // Note: Assuming SqlUtil.extractEventData(event) is the correct utility based on other methods.
        Map<String, Object> map = SqlUtil.extractEventData(event);

        String hostId = (String)map.get("hostId");
        String clientId = (String)map.get("clientId");
        String providerId = (String)map.get("providerId");
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
            // 4: host_id (PK part)
            statement.setObject(4, UUID.fromString(hostId));
            // 5: client_id (PK part)
            statement.setObject(5, UUID.fromString(clientId));
            // 6: provider_id (PK part)
            statement.setString(6, providerId);
            // 7: aggregate_version < ? (new version for OCC/IDM check)
            statement.setLong(7, newAggregateVersion);

            int count = statement.executeUpdate();
            if (count == 0) {
                // If 0 rows updated, it means:
                // 1. The record was not found (already deleted or never existed).
                // 2. The OCC/IDM check failed (aggregate_version >= newAggregateVersion).
                // We IGNORE the failure and log a warning, as this is the desired idempotent/monotonic behavior.
                logger.warn("Soft delete skipped for hostId {} clientId {} providerId {} aggregateVersion {}. Record not found or a newer/same version already exists.", hostId, clientId, providerId, newAggregateVersion);
            }
        } catch (SQLException e) {
            logger.error("SQLException during deleteAuthProviderClient for hostId {} clientId {} providerId {} aggregateVersion {}: {}", hostId, clientId, providerId, newAggregateVersion, e.getMessage(), e);
            throw e;
        } catch (Exception e) {
            logger.error("Exception during deleteAuthProviderClient for hostId {} clientId {} providerId {} aggregateVersion {}: {}", hostId, clientId, providerId, newAggregateVersion, e.getMessage(), e);
            throw e;
        }
    }

    @Override
    public Result<String> queryAuthProviderClient(int offset, int limit, String filtersJson, String globalFilter, String sortingJson, boolean active, String hostId) {
        Result<String> result = null;

        // Assuming helper methods parseJsonList, dynamicFilter, globalFilter, dynamicSorting,
        // populateParameters, JsonMapper, Success, Failure, Status, SQL_EXCEPTION, GENERIC_EXCEPTION,
        // and Constants are available in the scope.
        List<Map<String, Object>> filters = SqlUtil.parseJsonList(filtersJson);
        List<Map<String, Object>> sorting = SqlUtil.parseJsonList(sortingJson);

        String s =
                """
                SELECT COUNT(*) OVER () AS total,
                host_id, client_id, provider_id, aggregate_version,
                active, update_user, update_ts
                FROM auth_provider_client_t
                WHERE host_id = ?
                """;
        List<Object> parameters = new ArrayList<>();
        parameters.add(UUID.fromString(hostId));

        String activeClause = SqlUtil.buildMultiTableActiveClause(active);
        String[] searchColumns = {"provider_id"};
        // Note: Dynamic filtering/sorting columns changed to match table structure.
        String sqlBuilder = s + activeClause +
                SqlUtil.dynamicFilter(Arrays.asList("host_id", "client_id"), Arrays.asList(searchColumns), filters, null, parameters) +
                SqlUtil.globalFilter(globalFilter, searchColumns, parameters) +
                SqlUtil.dynamicSorting("provider_id, client_id", sorting, null) +
                "\nLIMIT ? OFFSET ?";

        parameters.add(limit);
        parameters.add(offset);

        int total = 0;
        List<Map<String, Object>> authProviderClients = new ArrayList<>();

        try (Connection connection = ds.getConnection(); // Assuming ds.getConnection() is available
            PreparedStatement preparedStatement = connection.prepareStatement(sqlBuilder)) {
            SqlUtil.populateParameters(preparedStatement, parameters);
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
                    map.put("providerId", resultSet.getString("provider_id"));
                    map.put("aggregateVersion", resultSet.getLong("aggregate_version"));
                    map.put("active", resultSet.getBoolean("active"));
                    map.put("updateUser", resultSet.getString("update_user"));
                    map.put("updateTs", resultSet.getObject("update_ts") != null ? resultSet.getObject("update_ts", OffsetDateTime.class) : null);
                    authProviderClients.add(map);
                }
            }

            Map<String, Object> resultMap = new HashMap<>();
            resultMap.put("total", total);
            resultMap.put("authProviderClients", authProviderClients);
            result = Success.of(JsonMapper.toJson(resultMap)); // Assuming JsonMapper and Success are available

        } catch (SQLException e) {
            logger.error("SQLException:", e);
            result = Failure.of(new Status(SQL_EXCEPTION, e.getMessage())); // Assuming Status and exception constants are available
        } catch (Exception e) {
            logger.error("Exception:", e);
            result = Failure.of(new Status(GENERIC_EXCEPTION, e.getMessage()));
        }
        return result;
    }

    @Override
    public Result<String> queryApp(int offset, int limit, String filtersJson, String globalFilter, String sortingJson, boolean active, String hostId) {
        Result<String> result = null;

        List<Map<String, Object>> filters = parseJsonList(filtersJson);
        List<Map<String, Object>> sorting = parseJsonList(sortingJson);

        String s =
                """
                SELECT COUNT(*) OVER () AS total,
                host_id, app_id, app_name, app_desc, is_kafka_app, operation_owner,
                delivery_owner, update_user, update_ts, active, aggregate_version
                FROM app_t
                WHERE host_id = ?
                """;
        List<Object> parameters = new ArrayList<>();
        parameters.add(UUID.fromString(hostId));

        String activeClause = SqlUtil.buildMultiTableActiveClause(active);
        String[] searchColumns = {"app_id", "app_name", "app_desc"};
        String sqlBuilder = s + activeClause +
                dynamicFilter(Arrays.asList("host_id"), Arrays.asList(searchColumns), filters, null, parameters) +
                globalFilter(globalFilter, searchColumns, parameters) +
                dynamicSorting("app_id", sorting, null) +
                "\nLIMIT ? OFFSET ?";

        parameters.add(limit);
        parameters.add(offset);

        int total = 0;
        List<Map<String, Object>> apps = new ArrayList<>();

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
                    map.put("appId", resultSet.getString("app_id"));
                    map.put("appName", resultSet.getString("app_name"));
                    map.put("appDesc", resultSet.getString("app_desc"));
                    map.put("isKafkaApp", resultSet.getBoolean("is_kafka_app"));
                    map.put("operationOwner", resultSet.getObject("operation_owner", UUID.class));
                    map.put("deliveryOwner", resultSet.getObject("delivery_owner", UUID.class));
                    map.put("updateUser", resultSet.getString("update_user"));
                    map.put("updateTs", resultSet.getObject("update_ts") != null ? resultSet.getObject("update_ts", OffsetDateTime.class) : null);
                    map.put("active", resultSet.getBoolean("active"));
                    map.put("aggregateVersion", resultSet.getLong("aggregate_version"));
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
    public Result<String> queryAppById(String hostId, String appId) {
        Result<String> result = null;
        String sql =
                """
                SELECT host_id, app_id, app_name, app_desc, is_kafka_app, operation_owner,
                delivery_owner, aggregate_version, active, update_user, update_ts
                FROM app_t
                WHERE host_id = ? AND app_id = ?
                """;
        try (final Connection conn = ds.getConnection()) {
            Map<String, Object> map = new HashMap<>();
            try (PreparedStatement statement = conn.prepareStatement(sql)) {
                statement.setObject(1, UUID.fromString(hostId));
                statement.setString(2, appId);
                try (ResultSet resultSet = statement.executeQuery()) {
                    if (resultSet.next()) {
                        map.put("hostId", resultSet.getObject("host_id", UUID.class));
                        map.put("appId", resultSet.getString("app_id"));
                        map.put("appName", resultSet.getString("app_name"));
                        map.put("appDesc", resultSet.getString("app_desc"));
                        map.put("isKafkaApp", resultSet.getBoolean("is_kafka_app"));
                        map.put("operationOwner", resultSet.getObject("operation_owner", UUID.class));
                        map.put("deliveryOwner", resultSet.getObject("delivery_owner", UUID.class));
                        map.put("updateUser", resultSet.getString("update_user"));
                        map.put("updateTs", resultSet.getObject("update_ts") != null ? resultSet.getObject("update_ts", OffsetDateTime.class) : null);
                        map.put("aggregateVersion", resultSet.getLong("aggregate_version"));
                        map.put("active", resultSet.getBoolean("active"));
                    }
                }
            }
            if (map.isEmpty())
                result = Failure.of(new Status(OBJECT_NOT_FOUND, "queryAppByAppId", hostId + "|" + appId));
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
    public Result<String> queryClient(int offset, int limit, String filtersJson, String globalFilter, String sortingJson, boolean active, String hostId) {
        Result<String> result = null;
        List<Map<String, Object>> filters = parseJsonList(filtersJson);
        List<Map<String, Object>> sorting = parseJsonList(sortingJson);

        String s =
                """
                SELECT COUNT(*) OVER () AS total,
                client_id, host_id, app_id, api_id, client_name, client_type, client_profile,
                client_scope, custom_claim, redirect_uri, authenticate_class, deref_client_id,
                update_user, update_ts, aggregate_version, active
                FROM auth_client_t
                WHERE host_id = ?
                """;

        List<Object> parameters = new ArrayList<>();
        parameters.add(UUID.fromString(hostId));

        String activeClause = SqlUtil.buildMultiTableActiveClause(active);
        String[] searchColumns = {"app_id", "api_id", "client_name", "client_scope", "custom_claim", "redirect_uri"};
        String sqlBuilder = s + activeClause +
                dynamicFilter(Arrays.asList("client_id", "host_id"), Arrays.asList(searchColumns), filters, null, parameters) +
                globalFilter(globalFilter, searchColumns, parameters) +
                dynamicSorting("client_id", sorting, null) +
                "\nLIMIT ? OFFSET ?";

        parameters.add(limit);
        parameters.add(offset);

        int total = 0;
        List<Map<String, Object>> clients = new ArrayList<>();

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
                    map.put("aggregateVersion", resultSet.getLong("aggregate_version"));
                    map.put("active", resultSet.getBoolean("active"));
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
    public Result<String> getClientIdLabel(String hostId) {
        Result<String> result = null;
        String sql = "SELECT client_id, client_name FROM auth_client_t WHERE host_id = ? AND active = TRUE";
        List<Map<String, Object>> labels = new ArrayList<>();
        try (Connection connection = ds.getConnection();
             PreparedStatement preparedStatement = connection.prepareStatement(sql)) {
            preparedStatement.setObject(1, UUID.fromString(hostId));
            try (ResultSet resultSet = preparedStatement.executeQuery()) {
                while (resultSet.next()) {
                    Map<String, Object> map = new HashMap<>();
                    map.put("id", resultSet.getString("client_id"));
                    map.put("label", resultSet.getString("client_name"));
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
    public void createRefreshToken(Connection conn, Map<String, Object> event) throws SQLException, Exception {
        final String insertRefreshToken = "INSERT INTO auth_refresh_token_t (refresh_token, host_id, provider_id, user_id, entity_id, user_type, " +
                "email, roles, groups, positions, attributes, client_id, scope, csrf, custom_claim, update_user, update_ts, aggregate_version) " +
                "VALUES (?, ?, ?, ?, ?,   ?, ?, ?, ?, ?,   ?, ?, ?, ?, ?, ?, ?, ?)";
        Map<String, Object> map = SqlUtil.extractEventData(event);
        String refreshToken = (String) map.get("refreshToken");
        long aggregateVersion = SqlUtil.getNewAggregateVersion(event);
        try (PreparedStatement statement = conn.prepareStatement(insertRefreshToken)) {
            statement.setObject(1, UUID.fromString(refreshToken));
            statement.setObject(2, UUID.fromString((String) map.get("hostId")));
            statement.setString(3, (String) map.get("providerId"));
            statement.setObject(4, UUID.fromString((String) map.get("userId")));
            statement.setString(5, (String) map.get("entityId"));
            statement.setString(6, (String) map.get("userType"));
            statement.setString(7, (String) map.get("email"));
            String roles = (String) map.get("roles");
            if (roles != null && !roles.isEmpty())
                statement.setString(8, roles);
            else
                statement.setNull(8, NULL);
            String groups = (String) map.get("groups");
            if (groups != null && !groups.isEmpty())
                statement.setString(9, groups);
            else
                statement.setNull(9, NULL);
            String positions = (String) map.get("positions");
            if (positions != null && !positions.isEmpty())
                statement.setString(10, positions);
            else
                statement.setNull(10, NULL);
            String attributes = (String) map.get("attributes");
            if (attributes != null && !attributes.isEmpty())
                statement.setString(11, attributes);
            else
                statement.setNull(11, NULL);
            String clientId = (String) map.get("clientId");
            if (clientId != null && !clientId.isEmpty())
                statement.setObject(12, UUID.fromString(clientId));
            else
                statement.setNull(12, NULL);
            String scope = (String) map.get("scope");
            if (scope != null && !scope.isEmpty())
                statement.setString(13, scope);
            else
                statement.setNull(13, NULL);
            String csrf = (String) map.get("csrf");
            if (csrf != null && !csrf.isEmpty())
                statement.setString(14, csrf);
            else
                statement.setNull(14, NULL);

            String customClaim = (String) map.get("customClaim");
            if (customClaim != null && !customClaim.isEmpty())
                statement.setString(15, customClaim);
            else
                statement.setNull(15, NULL);

            statement.setString(16, (String) event.get(Constants.USER));
            statement.setObject(17, OffsetDateTime.parse((String) event.get(CloudEventV1.TIME)));
            statement.setLong(18, aggregateVersion);
            int count = statement.executeUpdate();
            if (count == 0) {
                throw new SQLException(String.format("Failed to insert refresh token %s with aggregateVersion %d", refreshToken, aggregateVersion));
            }
        } catch (SQLException e) {
            logger.error("SQLException during createRefreshToken for refreshToken {} aggregateVersion {}: {}", refreshToken, aggregateVersion, e.getMessage(), e);
            throw e;
        } catch (Exception e) {
            logger.error("Exception during createRefreshToken for refreshToken {} aggregateVersion {}: {}", refreshToken, aggregateVersion, e.getMessage(), e);
            throw e;
        }
    }

    @Override
    public void deleteRefreshToken(Connection conn, Map<String, Object> event) throws SQLException, Exception {
        final String deleteRefreshToken = "DELETE from auth_refresh_token_t WHERE refresh_token = ? AND user_id = ? AND aggregate_version = ?";
        Map<String, Object> map = SqlUtil.extractEventData(event);
        String refreshToken = (String) map.get("refreshToken");
        long oldAggregateVersion = SqlUtil.getOldAggregateVersion(event);
        try (PreparedStatement statement = conn.prepareStatement(deleteRefreshToken)) {
            statement.setObject(1, UUID.fromString(refreshToken));
            statement.setObject(2, UUID.fromString((String) map.get("userId")));
            statement.setLong(3, oldAggregateVersion);
            int count = statement.executeUpdate();
            if (count == 0) {
                throw new SQLException(String.format("no record is deleted for refresh token %s", refreshToken));
            }
        } catch (SQLException e) {
            logger.error("SQLException during deleteRefreshToken for refreshToken {} aggregateVersion {}: {}", refreshToken, oldAggregateVersion, e.getMessage(), e);
            throw e;
        } catch (Exception e) {
            logger.error("Exception during deleteRefreshToken for refreshToken {} aggregateVersion {}: {}", refreshToken, oldAggregateVersion, e.getMessage(), e);
            throw e;
        }
    }

    @Override
    public Result<String> getRefreshToken(int offset, int limit, String filtersJson, String globalFilter, String sortingJson, boolean active, String hostId) {
        Result<String> result = null;
        final Map<String, String> columnMap = new HashMap<>(Map.of(
                "hostId", "r.host_id",
                "refreshToken", "r.refresh_token",
                "userId", "r.user_id",
                "userType", "r.user_type",
                "entityId", "r.entity_id",
                "email", "r.email",
                "firstName", "u.first_name",
                "lastName", "u.lastName",
                "clientId", "r.client_id",
                "appId", "a.app_id"
        ));
        columnMap.put("appName", "a.app_name");
        columnMap.put("scope", "r.scope");
        columnMap.put("roles", "r.roles");
        columnMap.put("groups", "r.groups");
        columnMap.put("positions", "r.positions");
        columnMap.put("attributes", "r.attributes");
        columnMap.put("csrf", "r.csrf");
        columnMap.put("customClaim", "r.custom_claim");
        columnMap.put("updateUser", "r.update_user");
        columnMap.put("updateTs", "r.update_ts");
        columnMap.put("aggregateVersion", "r.aggregate_version");

        List<Map<String, Object>> filters = parseJsonList(filtersJson);
        List<Map<String, Object>> sorting = parseJsonList(sortingJson);

        String s =
                """
                SELECT COUNT(*) OVER () AS total,
                r.host_id, r.refresh_token, r.user_id, r.user_type, r.entity_id, r.email, u.first_name, u.last_name,
                r.client_id, a.app_id, a.app_name, r.scope, r.roles, r.groups, r.positions, r.attributes, r.csrf,
                r.custom_claim, r.update_user, r.update_ts, r.aggregate_version
                FROM auth_refresh_token_t r, user_t u, app_t a, auth_client_t c
                WHERE r.user_id = u.user_id AND r.client_id = c.client_id AND a.app_id = c.app_id
                AND r.host_id = ?
                """;

        List<Object> parameters = new ArrayList<>();
        parameters.add(UUID.fromString(hostId));

        String activeClause = SqlUtil.buildMultiTableActiveClause(active, "r", "u", "a", "c");
        String[] searchColumns = {"r.entity_id", "r.email", "u.first_name", "u.last_name", "a.app_name", "r.roles", "r.groups", "r.positions", "r.attributes", "r.scope"};
        String sqlBuilder = s + activeClause +
                dynamicFilter(Arrays.asList("r.host_id", "r.refresh_token", "r.user_id", "r.client_id"), Arrays.asList(searchColumns), filters, columnMap, parameters) +
                globalFilter(globalFilter, searchColumns, parameters) +
                dynamicSorting("r.user_id", sorting, columnMap) +
                "\nLIMIT ? OFFSET ?";

        parameters.add(limit);
        parameters.add(offset);

        int total = 0;
        List<Map<String, Object>> tokens = new ArrayList<>();

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
                    map.put("aggregateVersion", resultSet.getLong("aggregate_version"));
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
                """
                SELECT refresh_token, host_id, provider_id, user_id, entity_id, user_type, email, roles, groups,
                positions, attributes, client_id, scope, csrf, custom_claim, update_user, update_ts, aggregate_version
                FROM auth_refresh_token_t
                WHERE refresh_token = ?
                """;
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
                        map.put("updateUser", resultSet.getString("update_user"));
                        map.put("updateTs", resultSet.getObject("update_ts") != null ? resultSet.getObject("update_ts", OffsetDateTime.class) : null);
                        map.put("aggregateVersion", resultSet.getLong("aggregate_version"));
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
    public void createAuthCode(Connection conn, Map<String, Object> event) throws SQLException, Exception {

        final String insertAuthCode = "INSERT INTO auth_code_t(host_id, provider_id, auth_code, user_id, entity_id, user_type, email, roles," +
                "redirect_uri, scope, remember, code_challenge, challenge_method, update_user, update_ts, aggregate_version) " +
                "VALUES (?, ?, ?, ?, ?, ?, ?,  ?, ?, ?, ?, ?, ?, ?, ?, ?)";
        Map<String, Object> map = (Map<String, Object>) event.get(PortalConstants.DATA);
        String authCode = (String) map.get("authCode");
        long newAggregateVersion = SqlUtil.getNewAggregateVersion(event);
        try (PreparedStatement statement = conn.prepareStatement(insertAuthCode)) {
            statement.setObject(1, UUID.fromString((String) map.get("hostId")));
            statement.setString(2, (String) map.get("providerId"));
            statement.setString(3, authCode);
            if (map.containsKey("userId")) {
                statement.setObject(4, UUID.fromString((String) map.get("userId")));
            } else {
                statement.setNull(4, Types.OTHER);
            }
            if (map.containsKey("entityId")) {
                statement.setString(5, (String) map.get("entityId"));
            } else {
                statement.setNull(5, Types.VARCHAR);
            }
            if (map.containsKey("userType")) {
                statement.setString(6, (String) map.get("userType"));
            } else {
                statement.setNull(6, Types.VARCHAR);
            }
            if (map.containsKey("email")) {
                statement.setString(7, (String) map.get("email"));
            } else {
                statement.setNull(7, Types.VARCHAR);
            }
            if (map.containsKey("roles")) {
                statement.setString(8, (String) map.get("roles"));
            } else {
                statement.setNull(8, Types.VARCHAR);
            }
            if (map.containsKey("redirectUri")) {
                statement.setString(9, (String) map.get("redirectUri"));
            } else {
                statement.setNull(9, Types.VARCHAR);
            }
            if (map.containsKey("scope")) {
                statement.setString(10, (String) map.get("scope"));
            } else {
                statement.setNull(10, Types.VARCHAR);
            }
            if (map.containsKey("remember")) {
                statement.setString(11, (String) map.get("remember"));
            } else {
                statement.setNull(11, Types.CHAR);
            }
            if (map.containsKey("codeChallenge")) {
                statement.setString(12, (String) map.get("codeChallenge"));
            } else {
                statement.setNull(12, Types.VARCHAR);
            }
            if (map.containsKey("challengeMethod")) {
                statement.setString(13, (String) map.get("challengeMethod"));
            } else {
                statement.setNull(13, Types.VARCHAR);
            }
            statement.setString(14, (String) event.get(Constants.USER));
            statement.setObject(15, OffsetDateTime.parse((String) event.get(CloudEventV1.TIME)));
            statement.setLong(16, newAggregateVersion);
            int count = statement.executeUpdate();
            if (count == 0) {
                throw new SQLException("Failed to insert the auth code with authCode " + authCode + " aggregateVersion " + newAggregateVersion);
            }
        } catch (SQLException e) {
            logger.error("SQLException during createAuthCode for authCode {} aggregateVersion {}: {}", authCode, newAggregateVersion, e.getMessage(), e);
            throw e;
        } catch (Exception e) {
            logger.error("Exception during createAuthCode for authCode {} aggregateVersion {}: {}", authCode, newAggregateVersion, e.getMessage(), e);
            throw e;
        }
    }

    @Override
    public void deleteAuthCode(Connection conn, Map<String, Object> event) throws SQLException, Exception {
        final String deleteAuthCode = "DELETE FROM auth_code_t WHERE host_id = ? AND auth_code = ? AND aggregate_version = ?";
        Map<String, Object> map = SqlUtil.extractEventData(event);
        String authCode = (String) map.get("authCode");
        long oldAggregateVersion = SqlUtil.getOldAggregateVersion(event);
        try (PreparedStatement statement = conn.prepareStatement(deleteAuthCode)) {
            statement.setObject(1, UUID.fromString((String) map.get("hostId")));
            statement.setString(2, authCode);
            statement.setLong(3, oldAggregateVersion);
            int count = statement.executeUpdate();
            if (count == 0) {
                // there shouldn't be any conflict in aggregate version, so if no record is deleted, it means the auth code doesn't exist
                throw new SQLException(String.format("no record is deleted for auth code %s", authCode));
            }
        } catch (SQLException e) {
            logger.error("SQLException during deleteAuthCode for authCode {} aggregateVersion {}: {}", authCode, oldAggregateVersion, e.getMessage(), e);
            throw e;
        } catch (Exception e) {
            logger.error("Exception during deleteAuthCode for authCode {} aggregateVersion {}: {}", authCode, oldAggregateVersion, e.getMessage(), e);
            throw e;
        }
    }

    @Override
    public Result<String> queryAuthCode(String authCode) {
        final String sql = "SELECT host_id, provider_id, auth_code, user_id, entity_id, user_type, email, " +
                "roles, redirect_uri, scope, remember, code_challenge, challenge_method, aggregate_version " +
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
                    map.put("aggregateVersion", resultSet.getLong("aggregate_version"));
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
    public Result<String> getAuthCode(int offset, int limit, String filtersJson, String globalFilter, String sortingJson, boolean active, String hostId) {
        Result<String> result = null;
        List<Map<String, Object>> filters = parseJsonList(filtersJson);
        List<Map<String, Object>> sorting = parseJsonList(sortingJson);

        String s =
                """
                SELECT COUNT(*) OVER () AS total,
                host_id, auth_code, user_id, entity_id, user_type, email, roles, redirect_uri, scope, remember,
                code_challenge, challenge_method, update_user, update_ts, aggregate_version
                FROM auth_code_t
                WHERE host_id = ?
                """;

        List<Object> parameters = new ArrayList<>();
        parameters.add(UUID.fromString(hostId));

        String activeClause = SqlUtil.buildMultiTableActiveClause(active);
        String[] searchColumns = {"entity_id", "email", "roles", "redirect_uri", "scope"};
        String sqlBuilder = s + activeClause +
                dynamicFilter(Arrays.asList("host_id", "user_id"), Arrays.asList(searchColumns), filters, null, parameters) +
                globalFilter(globalFilter, searchColumns, parameters) +
                dynamicSorting("update_ts", sorting, null) +
                "\nLIMIT ? OFFSET ?";

        parameters.add(limit);
        parameters.add(offset);

        int total = 0;
        List<Map<String, Object>> authCodes = new ArrayList<>();

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
                    map.put("updateTs", resultSet.getObject("update_ts") != null ? resultSet.getObject("update_ts", OffsetDateTime.class) : null);
                    map.put("aggregateVersion", resultSet.getLong("aggregate_version"));

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
        final String sql =
                """
                SELECT host_id, provider_id, provider_name, provider_desc, operation_owner, delivery_owner,
                jwk, aggregate_version, update_user, update_ts, active
                FROM auth_provider_t
                WHERE provider_id = ?
                """;
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
                        map.put("providerDesc", resultSet.getString("provider_desc"));
                        map.put("jwk", resultSet.getString("jwk"));
                        map.put("aggregateVersion", resultSet.getLong("aggregate_version"));
                        map.put("updateUser", resultSet.getString("update_user"));
                        map.put("updateTs", resultSet.getObject("update_ts") != null ? resultSet.getObject("update_ts", OffsetDateTime.class) : null);
                        map.put("active", resultSet.getBoolean("active"));
                        map.put("operationOwner", resultSet.getObject("operation_owner", UUID.class));
                        map.put("deliveryOwner", resultSet.getObject("delivery_owner", UUID.class));
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
    public Result<String> getProviderIdLabel(String hostId) {
        Result<String> result = null;
        String sql = "SELECT provider_id, provider_name FROM auth_provider_t WHERE host_id = ? AND active = true";
        List<Map<String, Object>> labels = new ArrayList<>();
        try (Connection connection = ds.getConnection();
             PreparedStatement preparedStatement = connection.prepareStatement(sql)) {
            preparedStatement.setObject(1, UUID.fromString(hostId));
            try (ResultSet resultSet = preparedStatement.executeQuery()) {
                while (resultSet.next()) {
                    Map<String, Object> map = new HashMap<>();
                    map.put("id", resultSet.getString("provider_id"));
                    map.put("label", resultSet.getString("provider_name"));
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
    public String queryProviderByName(String hostId, String providerName) {
        final String sql =
                """
                SELECT provider_id
                FROM auth_provider_t
                WHERE host_id = ? AND provider_name = ?
                """;
        String providerId = null;
        try (final Connection conn = ds.getConnection()) {
            Map<String, Object> map = new HashMap<>();
            try (PreparedStatement statement = conn.prepareStatement(sql)) {
                statement.setObject(1, UUID.fromString(hostId));
                statement.setString(2, providerName);
                try (ResultSet resultSet = statement.executeQuery()) {
                    if (resultSet.next()) {
                        providerId = resultSet.getString("provider_id");
                    }
                }
            }
        } catch (SQLException e) {
            logger.error("SQLException:", e);
        } catch (Exception e) {
            logger.error("Exception:", e);
        }
        return providerId;
    }

    @Override
    public Result<String> queryProvider(int offset, int limit, String filtersJson, String globalFilter, String sortingJson, boolean active, String hostId) {
        Result<String> result = null;
        List<Map<String, Object>> filters = parseJsonList(filtersJson);
        List<Map<String, Object>> sorting = parseJsonList(sortingJson);

        String s =
                """
                SELECT COUNT(*) OVER () AS total,
                host_id, provider_id, provider_name, provider_desc, operation_owner,
                delivery_owner, jwk, update_user, update_ts, aggregate_version, active
                FROM auth_provider_t
                WHERE host_id = ?
                """;

        List<Object> parameters = new ArrayList<>();
        parameters.add(UUID.fromString(hostId));

        String activeClause = SqlUtil.buildMultiTableActiveClause(active);
        String[] searchColumns = {"provider_name", "provider_desc"};
        String sqlBuilder = s + activeClause +
                dynamicFilter(Arrays.asList("host_id"), Arrays.asList(searchColumns), filters, null, parameters) +
                globalFilter(globalFilter, searchColumns, parameters) +
                dynamicSorting("provider_id", sorting, null) +
                "\nLIMIT ? OFFSET ?";

        parameters.add(limit);
        parameters.add(offset);

        int total = 0;
        List<Map<String, Object>> providers = new ArrayList<>();

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
                    map.put("providerId", resultSet.getString("provider_id"));
                    map.put("providerName", resultSet.getString("provider_name"));
                    map.put("providerDesc", resultSet.getString("provider_desc"));
                    map.put("operationOwner", resultSet.getObject("operation_owner", UUID.class));
                    map.put("deliveryOwner", resultSet.getObject("delivery_owner", UUID.class));
                    map.put("jwk", resultSet.getString("jwk"));
                    map.put("updateUser", resultSet.getString("update_user"));
                    // handling date properly
                    map.put("updateTs", resultSet.getObject("update_ts") != null ? resultSet.getObject("update_ts", OffsetDateTime.class) : null);
                    map.put("aggregateVersion", resultSet.getLong("aggregate_version"));
                    map.put("active", resultSet.getBoolean("active"));
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
    public Result<Map<String, Object>> queryCurrentProviderKey(String providerId) {
        final String queryConfigById =
                """
                SELECT provider_id, kid, public_key, private_key,
                key_type, update_user, update_ts
                FROM auth_provider_key_t
                WHERE provider_id = ? AND key_type = 'TC'
                """;
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
    public Result<Map<String, Object>> queryLongLiveProviderKey(String providerId) {
        final String queryConfigById =
                """
                SELECT provider_id, kid, public_key, private_key,
                key_type, update_user, update_ts
                FROM auth_provider_key_t
                WHERE provider_id = ? AND key_type = 'LC'
                """;
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
    public void createRefToken(Connection conn, Map<String, Object> event) throws SQLException, Exception {
        final String insertRefreshToken =
                """
                INSERT INTO auth_ref_token_t (ref_token, host_id, jwt, client_id, update_user, update_ts, aggregate_version)
                VALUES (?, ?, ?, ?, ?,   ?, ?)
                """;
        Map<String, Object> map = SqlUtil.extractEventData(event);
        String refToken = (String) map.get("refToken");
        long aggregateVersion = SqlUtil.getNewAggregateVersion(event);
        try (PreparedStatement statement = conn.prepareStatement(insertRefreshToken)) {
            statement.setString(1, refToken);
            statement.setObject(2, UUID.fromString((String) map.get("hostId")));
            statement.setString(3, (String) map.get("jwt"));
            statement.setObject(4, UUID.fromString((String)map.get("clientId")));
            statement.setString(5, (String) event.get(Constants.USER));
            statement.setObject(6, OffsetDateTime.parse((String) event.get(CloudEventV1.TIME)));
            statement.setLong(7, aggregateVersion);
            int count = statement.executeUpdate();
            if (count == 0) {
                throw new SQLException(String.format("Failed during createRefToken for refToken %s with aggregateVersion %d", refToken, aggregateVersion));
            }
        } catch (SQLException e) {
            logger.error("SQLException during createRefToken for refToken {} aggregateVersion {}: {}", refToken, aggregateVersion, e.getMessage(), e);
            throw e;
        } catch (Exception e) {
            logger.error("Exception during createRefToken for refToken {} aggregateVersion {}: {}", refToken, aggregateVersion, e.getMessage(), e);
            throw e;
        }
    }

    @Override
    public void deleteRefToken(Connection conn, Map<String, Object> event) throws SQLException, Exception {
        final String deleteRefToken = "DELETE from auth_ref_token_t WHERE ref_token = ? AND aggregate_version = ?";
        Map<String, Object> map = SqlUtil.extractEventData(event);
        String refToken = (String) map.get("refToken");
        long oldAggregateVersion = SqlUtil.getOldAggregateVersion(event);
        try (PreparedStatement statement = conn.prepareStatement(deleteRefToken)) {
            statement.setString(1, refToken);
            statement.setLong(2, oldAggregateVersion);
            int count = statement.executeUpdate();
            if (count == 0) {
                throw new SQLException(String.format("no record is deleted for ref token %s", refToken));
            }
        } catch (SQLException e) {
            logger.error("SQLException during deleteRefToken for refToken {} aggregateVersion {}: {}", refToken, oldAggregateVersion, e.getMessage(), e);
            throw e;
        } catch (Exception e) {
            logger.error("Exception during deleteRefToken for refToken {} aggregateVersion {}: {}", refToken, oldAggregateVersion, e.getMessage(), e);
            throw e;
        }
    }

    @Override
    public Result<String> getRefToken(int offset, int limit, String filtersJson, String globalFilter, String sortingJson, boolean active, String hostId) {
        Result<String> result = null;
        final Map<String, String> columnMap = new HashMap<>(Map.of(
                "hostId", "r.host_id",
                "refToken", "r.ref_token",
                "jwtToken", "r.jwt_token",
                "clientId", "r.client_id",
                "clientName", "c.client_name",
                "updateUser", "r.update_user",
                "updateTs", "r.update_ts",
                "aggregateVersion", "r.aggregate_version"
        ));

        List<Map<String, Object>> filters = parseJsonList(filtersJson);
        List<Map<String, Object>> sorting = parseJsonList(sortingJson);

        String s =
                """
                SELECT COUNT(*) OVER () AS total,
                r.host_id, r.ref_token, r.jwt_token, r.client_id, c.client_name, r.update_user, r.update_ts, r.aggregate_version
                FROM auth_ref_token_t r, auth_client_t c
                WHERE r.client_id = c.client_id
                AND r.host_id = ?
                """;

        List<Object> parameters = new ArrayList<>();
        parameters.add(UUID.fromString(hostId));

        String activeClause = SqlUtil.buildMultiTableActiveClause(active, "r", "c");
        String[] searchColumns = {"c.client_name"};
        String sqlBuilder = s + activeClause +
                dynamicFilter(Arrays.asList("r.host_id", "r.client_id"), Arrays.asList(searchColumns), filters, columnMap, parameters) +
                globalFilter(globalFilter, searchColumns, parameters) +
                dynamicSorting("r.client_id", sorting, columnMap) +
                "\nLIMIT ? OFFSET ?";

        parameters.add(limit);
        parameters.add(offset);

        int total = 0;
        List<Map<String, Object>> tokens = new ArrayList<>();

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
                    map.put("refToken", resultSet.getString("ref_token"));
                    map.put("jwtToken", resultSet.getString("jwt_token"));
                    map.put("clientId", resultSet.getObject("client_id", UUID.class));
                    map.put("clientName", resultSet.getString("client_name"));
                    map.put("updateUser", resultSet.getString("update_user"));
                    map.put("updateTs", resultSet.getObject("update_ts") != null ? resultSet.getObject("update_ts", OffsetDateTime.class) : null);
                    map.put("aggregateVersion", resultSet.getLong("aggregate_version"));
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
    public Result<String> queryRefToken(String refToken) {
        Result<String> result = null;
        String sql =
                """
                SELECT ref_token, host_id, jwt_token, client_id, update_user, update_ts, aggregate_version
                FROM auth_ref_token_t
                WHERE ref_token = ?
                """;
        try (final Connection conn = ds.getConnection()) {
            Map<String, Object> map = new HashMap<>();
            try (PreparedStatement statement = conn.prepareStatement(sql)) {
                statement.setString(1, refToken);
                try (ResultSet resultSet = statement.executeQuery()) {
                    if (resultSet.next()) {
                        map.put("refToken", resultSet.getString("ref_token"));
                        map.put("hostId", resultSet.getObject("host_id", UUID.class));
                        map.put("jwtToken", resultSet.getString("jwt_token"));
                        map.put("clientId", resultSet.getObject("client_id", UUID.class));
                        map.put("updateUser", resultSet.getString("update_user"));
                        map.put("updateTs", resultSet.getObject("update_ts") != null ? resultSet.getObject("update_ts", OffsetDateTime.class) : null);
                        map.put("aggregateVersion", resultSet.getLong("aggregate_version"));
                    }
                }
            }
            if (map.isEmpty())
                result = Failure.of(new Status(OBJECT_NOT_FOUND, "ref token", refToken));
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
}
