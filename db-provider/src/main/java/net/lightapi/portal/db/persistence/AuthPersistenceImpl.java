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

import javax.validation.Valid;
import java.sql.*;
import java.time.OffsetDateTime;
import java.util.*;

import static com.networknt.db.provider.SqlDbStartupHook.ds;
import static java.sql.Types.NULL;
import static net.lightapi.portal.db.util.SqlUtil.addCondition;

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
        final String sql = "INSERT INTO app_t(host_id, app_id, app_name, app_desc, is_kafka_app, operation_owner, delivery_owner, update_user, update_ts, aggregate_version) " +
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
        Map<String, Object> map = SqlUtil.extractEventData(event);
        String appId = (String) map.get("appId");
        long newAggregateVersion = SqlUtil.getNewAggregateVersion(event);
        try (PreparedStatement statement = conn.prepareStatement(sql)) {
            statement.setObject(1, UUID.fromString((String) event.get(Constants.HOST)));
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
                statement.setNull(7, Types.VARCHAR);
            }
            statement.setString(8, (String) event.get(Constants.USER));
            statement.setObject(9, OffsetDateTime.parse((String) event.get(CloudEventV1.TIME)));

            int count = statement.executeUpdate();
            if (count == 0) {
                throw new SQLException("Failed to insert the app with appId " + appId + " with aggregate version " + newAggregateVersion);
            }
        } catch (SQLException e) {
            logger.error("SQLException during createApp for appId {} aggregateVersion {}: {}", appId, newAggregateVersion, e.getMessage(), e);
            throw e;
        } catch (Exception e) {
            logger.error("Exception during createApp for appId {} aggregateVersion {}: {}", appId, newAggregateVersion, e.getMessage(), e);
            throw e;
        }
    }

    private boolean queryAppExists(Connection conn, String hostId, String appId) throws SQLException {
        final String sql =
                """
                SELECT COUNT(*) FROM app_t WHERE host_id = ? AND app_id = ?
                """;
        try (PreparedStatement pst = conn.prepareStatement(sql)) {
            pst.setObject(1, UUID.fromString(hostId));
            pst.setString(2, appId);
            try (ResultSet rs = pst.executeQuery()) {
                return rs.next() && rs.getInt(1) > 0;
            }
        }
    }

    @Override
    public void updateApp(Connection conn, Map<String, Object> event) throws SQLException, Exception {
        final String sql =
                """
                UPDATE app_t SET app_name = ?, app_desc = ?, is_kafka_app = ?, operation_owner = ?,
                delivery_owner = ?, update_user = ?, update_ts = ?, aggregate_version = ?
                WHERE host_id = ? AND app_id = ? AND aggregate_version = ?
                """;
        Map<String, Object> map = SqlUtil.extractEventData(event);
        String appId = (String) map.get("appId");
        long oldAggregateVersion = SqlUtil.getOldAggregateVersion(event);
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
            statement.setObject(9, UUID.fromString((String) map.get("hostId")));
            statement.setString(10, appId);
            statement.setLong(11, oldAggregateVersion);

            int count = statement.executeUpdate();
            if (count == 0) {
                if (queryAppExists(conn, (String)event.get(Constants.HOST), appId)) {
                    throw new ConcurrencyException("Optimistic concurrency conflict for appId " + appId + ". Expected version " + oldAggregateVersion + " but found a different version " + newAggregateVersion + ".");
                } else {
                    throw new SQLException("No record found to update for appId " + appId + ".");
                }
            }
        } catch (SQLException e) {
            logger.error("SQLException during updateApp for appId {} (old: {}) -> (new: {}): {}", appId, oldAggregateVersion, newAggregateVersion, e.getMessage(), e);
            throw e;
        } catch (Exception e) {
            logger.error("Exception during updateApp for appId {} (old: {}) -> (new: {}): {}", appId, oldAggregateVersion, newAggregateVersion, e.getMessage(), e);
            throw e;
        }
    }

    @Override
    public void deleteApp(Connection conn, Map<String, Object> event) throws SQLException, Exception {
        final String sql =
                """
                DELETE FROM app_t WHERE host_id = ? AND app_id = ? AND aggregate_version = ?
                """;

        Map<String, Object> map = SqlUtil.extractEventData(event);
        String appId = (String) map.get("appId");
        long oldAggregateVersion = SqlUtil.getOldAggregateVersion(event);

        try (PreparedStatement statement = conn.prepareStatement(sql)) {
            statement.setObject(1, UUID.fromString((String) map.get("hostId")));
            statement.setString(2, appId);
            statement.setLong(3, oldAggregateVersion);

            int count = statement.executeUpdate();
            if (count == 0) {
                if (queryAppExists(conn, (String)event.get(Constants.HOST), appId)) {
                    String s = String.format("Optimistic concurrency conflict during deleteApp for appId %s aggregateVersion %d but found a different version.", appId, oldAggregateVersion);
                    logger.warn(s);
                    throw new ConcurrencyException(s);
                } else {
                    String s = String.format("No record found during deleteApp for appId %s. It might have been already deleted.", appId);
                    logger.warn(s);
                    throw new SQLException(s);
                }
            }
        } catch (SQLException e) {
            logger.error("SQLException during deleteApp for appId {} aggregateVersion {}: {}", appId, oldAggregateVersion, e.getMessage(), e);
            throw e;
        } catch (Exception e) {
            logger.error("Exception during deleteApp for appId {} aggregateVersion {}: {}", appId, oldAggregateVersion, e.getMessage(), e);
            throw e;
        }
    }

    @Override
    public Result<String> getAppIdLabel(String hostId) {
        Result<String> result = null;
        String sql = "SELECT app_id, app_name FROM app_t WHERE host_id = ?";
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
        final String insertClient =
                """
                INSERT INTO auth_client_t (host_id, app_id, api_id, client_name, client_id,
                client_type, client_profile, client_secret, client_scope, custom_claim, redirect_uri,
                authenticate_class, deref_client_id, update_user, update_ts, aggregate_version)
                VALUES (?, ?, ?, ?, ?,   ?, ?, ?, ?, ?,   ?, ?, ?, ?, ?,  ?)
                """;

        Map<String, Object> map = SqlUtil.extractEventData(event);
        String clientId = (String) map.get("clientId");
        long newAggregateVersion = SqlUtil.getNewAggregateVersion(event);
        try (PreparedStatement statement = conn.prepareStatement(insertClient)) {
            statement.setObject(1, UUID.fromString((String) event.get(Constants.HOST)));
            String appId = (String) map.get("appId");
            if (appId != null && !appId.isEmpty()) {
                statement.setString(2, appId);
            } else {
                statement.setNull(2, NULL);
            }
            String apiId = (String) map.get("apiId");
            if (apiId != null && !apiId.isEmpty()) {
                statement.setString(3, apiId);
            } else {
                statement.setNull(3, NULL);
            }
            statement.setString(4, (String) map.get("clientName"));
            statement.setObject(5, UUID.fromString(clientId));
            statement.setString(6, (String) map.get("clientType"));
            statement.setString(7, (String) map.get("clientProfile"));
            statement.setString(8, (String) map.get("clientSecret"));

            String clientScope = (String) map.get("clientScope");
            if (clientScope != null && !clientScope.isEmpty()) {
                statement.setString(9, clientScope);
            } else {
                statement.setNull(9, NULL);
            }
            String customClaim = (String) map.get("customClaim");
            if (customClaim != null && !customClaim.isEmpty()) {
                statement.setString(10, customClaim);
            } else {
                statement.setNull(10, NULL);
            }
            String redirectUri = (String) map.get("redirectUri");
            if (redirectUri != null && !redirectUri.isEmpty()) {
                statement.setString(11, redirectUri);
            } else {
                statement.setNull(11, NULL);
            }
            String authenticateClass = (String) map.get("authenticateClass");
            if (authenticateClass != null && !authenticateClass.isEmpty()) {
                statement.setString(12, authenticateClass);
            } else {
                statement.setNull(12, NULL);
            }
            String deRefClientId = (String) map.get("deRefClientId");
            if (deRefClientId != null && !deRefClientId.isEmpty()) {
                statement.setObject(13, UUID.fromString(deRefClientId));
            } else {
                statement.setNull(13, NULL);
            }
            statement.setString(14, (String) event.get(Constants.USER));
            statement.setObject(15, OffsetDateTime.parse((String) event.get(CloudEventV1.TIME)));
            statement.setLong(16, newAggregateVersion);
            int count = statement.executeUpdate();
            if (count == 0) {
                throw new SQLException(String.format("Failed to insert client %s with aggregateVersion %d", clientId, newAggregateVersion));
            }
        } catch (SQLException e) {
            logger.error("SQLException during createClient for clientId {} aggregateVersion {}: {}", clientId, newAggregateVersion, e.getMessage(), e);
            throw e;
        } catch (Exception e) {
            logger.error("Exception during createClient for clientId {} aggregateVersion {}: {}", clientId, newAggregateVersion, e.getMessage(), e);
            throw e;
        }
    }


    private boolean queryClientExists(Connection conn, String hostId, String clientId) throws SQLException {
        final String sql =
                """
                SELECT COUNT(*) FROM auth_client_t WHERE host_id = ? AND client_id = ?
                """;
        try (PreparedStatement pst = conn.prepareStatement(sql)) {
            pst.setObject(1, UUID.fromString(hostId));
            pst.setString(2, clientId);
            try (ResultSet rs = pst.executeQuery()) {
                return rs.next() && rs.getInt(1) > 0;
            }
        }
    }

    @Override
    public void updateClient(Connection conn, Map<String, Object> event) throws SQLException, Exception {
        final String updateClient =
                """
                UPDATE auth_client_t SET app_id = ?, api_id = ?, client_name = ?,
                client_type = ?, client_profile = ?,
                client_scope = ?, custom_claim = ?, redirect_uri = ?, authenticate_class = ?,
                deref_client_id = ?, update_user = ?, update_ts = ?, aggregate_version = ?
                WHERE host_id = ? AND client_id = ? AND aggregate_version = ?
                """;

        Map<String, Object> map = (Map<String, Object>) event.get(PortalConstants.DATA);
        String clientId = (String) map.get("clientId");
        long oldAggregateVersion = SqlUtil.getOldAggregateVersion(event);
        long newAggregateVersion = SqlUtil.getNewAggregateVersion(event);

        try (PreparedStatement statement = conn.prepareStatement(updateClient)) {
            String appId = (String) map.get("appId");
            if (appId != null && !appId.isEmpty()) {
                statement.setString(1, appId);
            } else {
                statement.setNull(1, NULL);
            }
            String apiId = (String) map.get("apiId");
            if (apiId != null && !apiId.isEmpty()) {
                statement.setString(2, apiId);
            } else {
                statement.setNull(2, NULL);
            }
            statement.setString(3, (String) map.get("clientName"));
            statement.setString(4, (String) map.get("clientType"));
            statement.setString(5, (String) map.get("clientProfile"));
            String clientScope = (String) map.get("clientScope");
            if (clientScope != null && !clientScope.isEmpty()) {
                statement.setString(6, clientScope);
            } else {
                statement.setNull(6, NULL);
            }
            String customClaim = (String) map.get("customClaim");
            if (customClaim != null && !customClaim.isEmpty()) {
                statement.setString(7, customClaim);
            } else {
                statement.setNull(7, NULL);
            }
            String redirectUri = (String) map.get("redirectUri");
            if (redirectUri != null && !redirectUri.isEmpty()) {
                statement.setString(8, redirectUri);
            } else {
                statement.setNull(8, NULL);
            }
            String authenticateClass = (String) map.get("authenticateClass");
            if (authenticateClass != null && !authenticateClass.isEmpty()) {
                statement.setString(9, authenticateClass);
            } else {
                statement.setNull(9, NULL);
            }
            String deRefClientId = (String) map.get("deRefClientId");
            if (deRefClientId != null && !deRefClientId.isEmpty()) {
                statement.setObject(10, UUID.fromString(deRefClientId));
            } else {
                statement.setNull(10, Types.OTHER);
            }
            statement.setString(11, (String) event.get(Constants.USER));
            statement.setObject(12, OffsetDateTime.parse((String) event.get(CloudEventV1.TIME)));
            statement.setLong(13, newAggregateVersion);
            statement.setObject(14, UUID.fromString((String) map.get("hostId")));
            statement.setObject(15, UUID.fromString(clientId));
            statement.setLong(16, oldAggregateVersion);
            int count = statement.executeUpdate();
            if (count == 0) {
                if (queryClientExists(conn, (String)event.get(Constants.HOST), clientId)) {
                    throw new ConcurrencyException("Optimistic concurrency conflict for client " + clientId + ". Expected version " + oldAggregateVersion + " but found a different version " + newAggregateVersion + ".");
                } else {
                    throw new SQLException("No record found to update for client " + clientId + ".");
                }
            }
        } catch (SQLException e) {
            logger.error("SQLException during updateClient for clientId {} (old: {}) -> (new: {}): {}", clientId, oldAggregateVersion, newAggregateVersion, e.getMessage(), e);
            throw e;
        } catch (Exception e) {
            logger.error("Exception during updateClient for clientId {} (old: {}) -> (new: {}): {}", clientId, oldAggregateVersion, newAggregateVersion, e.getMessage(), e);
            throw e;
        }
    }

    @Override
    public void deleteClient(Connection conn, Map<String, Object> event) throws SQLException, Exception {
        final String deleteClient =
                """
                DELETE from auth_client_t WHERE host_id = ? AND client_id = ? AND aggregate_version = ?
                """;
        Map<String, Object> map = (Map<String, Object>) event.get(PortalConstants.DATA);
        String clientId = (String) map.get("clientId");
        long oldAggregateVersion = SqlUtil.getOldAggregateVersion(event);

        try (PreparedStatement statement = conn.prepareStatement(deleteClient)) {
            statement.setObject(1, UUID.fromString((String) map.get("hostId")));
            statement.setObject(2, UUID.fromString(clientId));
            statement.setLong(3, oldAggregateVersion);
            int count = statement.executeUpdate();
            if (count == 0) {
                if (queryClientExists(conn, (String)event.get(Constants.HOST), clientId)) {
                    throw new ConcurrencyException("Optimistic concurrency conflict during deleteClient for clientId " + clientId + " aggregateVersion " + oldAggregateVersion + " but found a different version or already updated.");
                } else {
                    throw new SQLException("No record found during deleteClient for clientId " + clientId + ". It might have been already deleted.");
                }
            }
        } catch (SQLException e) {
            logger.error("SQLException during deleteClient for clientId {} aggregateVersion {}: {}", clientId, oldAggregateVersion, e.getMessage(), e);
            throw e;
        } catch (Exception e) {
            logger.error("Exception during deleteClient for clientId {} aggregateVersion {}: {}", clientId, oldAggregateVersion, e.getMessage(), e);
            throw e;
        }
    }

    @Override
    public Result<String> queryClientByClientId(String clientId) {
        if (logger.isTraceEnabled()) logger.trace("queryClientByClientId: clientId = {}", clientId);
        Result<String> result;
        String sql =
                """
                SELECT host_id, app_id, api_id, client_name, client_id, client_type,
                client_profile, client_secret, client_scope, custom_claim, redirect_uri,
                authenticate_class, deref_client_id, update_user, update_ts, aggregate_version
                FROM auth_client_t
                WHERE client_id = ?
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
                result = Failure.of(new Status(OBJECT_NOT_FOUND, "application with clientId ", clientId));
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

    @Override
    public void createAuthProvider(Connection conn, Map<String, Object> event) throws SQLException, Exception {
        final String sql =
                """
                INSERT INTO auth_provider_t(host_id, provider_id, provider_name, provider_desc,
                operation_owner, delivery_owner, jwk, update_user, update_ts, aggregate_version)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """;
        Map<String, Object> map = (Map<String, Object>) event.get(PortalConstants.DATA);
        String providerId = (String) map.get("providerId");
        long newAggregateVersion = SqlUtil.getNewAggregateVersion(event);

        try (PreparedStatement statement = conn.prepareStatement(sql)) {
            statement.setObject(1, UUID.fromString((String) map.get("hostId")));
            statement.setString(2, providerId);
            statement.setString(3, (String) map.get("providerName"));

            if (map.containsKey("providerDesc")) {
                statement.setString(4, (String) map.get("providerDesc"));
            } else {
                statement.setNull(4, Types.VARCHAR);
            }
            if (map.containsKey("operationOwner")) {
                statement.setObject(5, UUID.fromString((String) map.get("operationOwner")));
            } else {
                statement.setNull(5, Types.OTHER);
            }
            if (map.containsKey("deliveryOwner")) {
                statement.setObject(6, UUID.fromString((String) map.get("deliveryOwner")));
            } else {
                statement.setNull(6, Types.OTHER);
            }
            if (map.containsKey("jwk")) {
                statement.setString(7, (String) map.get("jwk"));
            } else {
                statement.setNull(7, Types.VARCHAR);
            }
            statement.setString(8, (String) event.get(Constants.USER));
            statement.setObject(9, OffsetDateTime.parse((String) event.get(CloudEventV1.TIME)));
            statement.setLong(10, newAggregateVersion);

            int count = statement.executeUpdate();
            if (count == 0) {
                throw new SQLException("Failed to insert the auth provider " + providerId + " with aggregate version " + newAggregateVersion);
            }

            // Insert keys into auth_provider_key_t
            String keySql =
                    """
                    INSERT INTO auth_provider_key_t(provider_id, kid, public_key, private_key, key_type, update_user, update_ts, aggregate_version)
                    VALUES (?, ?, ?, ?, ?,  ?, ?, ?)
                    """;

            try (PreparedStatement keyStatement = conn.prepareStatement(keySql)) {
                Map<String, Object> keys = (Map<String, Object>) map.get("keys");

                keyStatement.setString(1, providerId);

                Map<String, Object> lcMap = (Map<String, Object>) keys.get("LC");
                // add long live current key
                keyStatement.setString(2, (String) lcMap.get("kid"));
                keyStatement.setString(3, (String) lcMap.get("publicKey"));
                keyStatement.setString(4, (String) lcMap.get("privateKey"));
                keyStatement.setString(5, "LC");
                keyStatement.setString(6, (String) event.get(Constants.USER));
                keyStatement.setObject(7, OffsetDateTime.parse((String) event.get(CloudEventV1.TIME)));
                keyStatement.setLong(8, newAggregateVersion);
                keyStatement.executeUpdate();

                // add long live previous key
                Map<String, Object> lpMap = (Map<String, Object>) keys.get("LP");
                keyStatement.setString(2, (String) lpMap.get("kid"));
                keyStatement.setString(3, (String) lpMap.get("publicKey"));
                keyStatement.setString(4, (String) lpMap.get("privateKey"));
                keyStatement.setString(5, "LP");
                keyStatement.setString(6, (String) event.get(Constants.USER));
                keyStatement.setObject(7, OffsetDateTime.parse((String) event.get(CloudEventV1.TIME)));
                keyStatement.setLong(8, newAggregateVersion);
                keyStatement.executeUpdate();

                // add token current key
                Map<String, Object> tcMap = (Map<String, Object>) keys.get("TC");
                keyStatement.setString(2, (String) tcMap.get("kid"));
                keyStatement.setString(3, (String) tcMap.get("publicKey"));
                keyStatement.setString(4, (String) tcMap.get("privateKey"));
                keyStatement.setString(5, "TC");
                keyStatement.setString(6, (String) event.get(Constants.USER));
                keyStatement.setObject(7, OffsetDateTime.parse((String) event.get(CloudEventV1.TIME)));
                keyStatement.setLong(8, newAggregateVersion);
                keyStatement.executeUpdate();

                // add token previous key
                Map<String, Object> tpMap = (Map<String, Object>) keys.get("TP");
                keyStatement.setString(2, (String) tpMap.get("kid"));
                keyStatement.setString(3, (String) tpMap.get("publicKey"));
                keyStatement.setString(4, (String) tpMap.get("privateKey"));
                keyStatement.setString(5, "TP");
                keyStatement.setString(6, (String) event.get(Constants.USER));
                keyStatement.setObject(7, OffsetDateTime.parse((String) event.get(CloudEventV1.TIME)));
                keyStatement.setLong(8, newAggregateVersion);
                keyStatement.executeUpdate();

            } catch (SQLException ex) {
                logger.error("SQLException during createAuthProvider key insert for provider id {}: {}", providerId, ex.getMessage(), ex);
                throw new SQLException("Failed to insert the auth provider key with provider id " + providerId, ex);
            }
        } catch (SQLException e) {
            logger.error("SQLException during createAuthProvider for id {}: {}", providerId, e.getMessage(), e);
            throw e;
        } catch (Exception e) {
            logger.error("Exception during createAuthProvider for id {}: {}", providerId, e.getMessage(), e);
            throw e;
        }
    }

    @Override
    public void rotateAuthProvider(Connection conn, Map<String, Object> event) throws SQLException, Exception {
        // update the auth_provider_t table with the new jwk
        final String sqlJwk = "UPDATE auth_provider_t SET jwk = ?, update_user = ?, update_ts = ?, aggregate_version = ?" +
                "WHERE provider_id = ? AND aggregate_version = ?";
        // insert the new key into auth_provider_key_t
        final String sqlInsert = "INSERT INTO auth_provider_key_t(provider_id, kid, public_key, private_key, key_type, update_user, update_ts, aggregate_version) " +
                "VALUES (?, ?, ?, ?, ?,  ?, ?, ?)";
        // update the existing key in auth_provider_key_t
        final String sqlUpdate = "UPDATE auth_provider_key_t SET key_type = ?, update_user = ?, update_ts = ?, aggregate_version = ?" +
                "WHERE provider_id = ? AND kid = ? AND aggregate_version = ?";
        // delete the old key from auth_provider_key_t
        final String sqlDelete = "DELETE FROM auth_provider_key_t WHERE provider_id = ? AND kid = ? AND aggregate_version = ?";

        Map<String, Object> map = (Map<String, Object>) event.get(PortalConstants.DATA);
        String providerId = (String) map.get("providerId");
        long oldAggregateVersion = SqlUtil.getOldAggregateVersion(event);
        long newAggregateVersion = SqlUtil.getNewAggregateVersion(event);

        try (PreparedStatement statement = conn.prepareStatement(sqlJwk)) {
            String jwk = (String) map.get("jwk");
            statement.setString(1, jwk);
            statement.setString(2, (String) event.get(Constants.USER));
            statement.setObject(3, OffsetDateTime.parse((String) event.get(CloudEventV1.TIME)));
            statement.setLong(4, newAggregateVersion);
            statement.setString(4, providerId);
            statement.setLong(5, oldAggregateVersion);

            int count = statement.executeUpdate();
            if (count == 0) {

                throw new SQLException("Failed to update the jwk for auth provider with id " + providerId);
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
                statementInsert.setLong(8, newAggregateVersion);

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
                statementUpdate.setLong(4, newAggregateVersion);
                statementUpdate.setString(5, providerId);
                statementUpdate.setString(6, (String) updateMap.get("kid"));
                statementUpdate.setLong(7, oldAggregateVersion);
                count = statementUpdate.executeUpdate();
                if (count == 0) {
                    throw new SQLException("Failed to update the auth provider key with provider id " + providerId);
                }
            }
            try (PreparedStatement statementDelete = conn.prepareStatement(sqlDelete)) {
                Map<String, Object> deleteMap = (Map<String, Object>) map.get("delete");
                statementDelete.setString(1, providerId);
                statementDelete.setString(2, (String) deleteMap.get("kid"));
                statementDelete.setLong(3, oldAggregateVersion);
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

    private boolean queryAuthProviderExists(Connection conn, String hostId, String providerId) throws SQLException {
        final String sql =
                """
                SELECT COUNT(*) FROM auth_provider_t WHERE host_id = ? AND provider_id = ?
                """;
        try (PreparedStatement pst = conn.prepareStatement(sql)) {
            pst.setObject(1, UUID.fromString(hostId));
            pst.setString(2, providerId);
            try (ResultSet rs = pst.executeQuery()) {
                return rs.next() && rs.getInt(1) > 0;
            }
        }
    }

    @Override
    public void updateAuthProvider(Connection conn, Map<String, Object> event) throws SQLException, Exception {
        final String sql =
                """
                UPDATE auth_provider_t SET provider_name = ?, provider_desc = ?, operation_owner = ?,
                delivery_owner = ?, update_user = ?, update_ts = ?, aggregate_version = ?
                WHERE host_id = ? AND provider_id = ? AND aggregate_version = ?
                """;
        Map<String, Object> map = (Map<String, Object>) event.get(PortalConstants.DATA);
        String providerId = (String) map.get("providerId");
        long oldAggregateVersion = SqlUtil.getOldAggregateVersion(event);
        long newAggregateVersion = SqlUtil.getNewAggregateVersion(event);

        try (PreparedStatement statement = conn.prepareStatement(sql)) {
            statement.setString(1, (String) map.get("providerName"));
            if (map.containsKey("providerDesc")) {
                statement.setString(2, (String) map.get("providerDesc"));
            } else {
                statement.setNull(2, Types.VARCHAR);
            }
            if (map.containsKey("operationOwner")) {
                statement.setObject(3, UUID.fromString((String) map.get("operationOwner")));
            } else {
                statement.setNull(3, Types.OTHER);
            }
            if (map.containsKey("deliveryOwner")) {
                statement.setObject(4, UUID.fromString((String) map.get("deliveryOwner")));
            } else {
                statement.setNull(4, Types.OTHER);
            }
            statement.setString(5, (String) event.get(Constants.USER));
            statement.setObject(6, OffsetDateTime.parse((String) event.get(CloudEventV1.TIME)));
            statement.setLong(7, newAggregateVersion);
            statement.setObject(8, UUID.fromString((String) map.get("hostId")));
            statement.setString(9, providerId);
            statement.setLong(10, oldAggregateVersion);

            int count = statement.executeUpdate();
            if (count == 0) {
                if (queryAuthProviderExists(conn, (String)event.get(Constants.HOST), providerId)) {
                    throw new ConcurrencyException("Optimistic concurrency conflict for provider " + providerId + ". Expected version " + oldAggregateVersion + " but found a different version " + newAggregateVersion + ".");
                } else {
                    throw new SQLException("No record found to update for provider " + providerId + ".");
                }
            }
        } catch (SQLException e) {
            logger.error("SQLException during updateAuthProvider for providerId {} (old: {}) -> (new: {}): {}", providerId, oldAggregateVersion, newAggregateVersion, e.getMessage(), e);
            throw e;
        } catch (Exception e) {
            logger.error("Exception during updateAuthProvider for providerId {} (old: {}) -> (new: {}): {}", providerId, oldAggregateVersion, newAggregateVersion, e.getMessage(), e);
            throw e;
        }
    }

    @Override
    public void deleteAuthProvider(Connection conn, Map<String, Object> event) throws SQLException, Exception {
        final String sql = "DELETE FROM auth_provider_t WHERE host_id = ? and provider_id = ? AND aggregate_version = ?";
        Map<String, Object> map = (Map<String, Object>) event.get(PortalConstants.DATA);
        String providerId = (String) map.get("providerId");
        long oldAggregateVersion = SqlUtil.getOldAggregateVersion(event);
        try (PreparedStatement statement = conn.prepareStatement(sql)) {
            statement.setObject(1, UUID.fromString((String) map.get("hostId")));
            statement.setString(2, providerId);
            statement.setLong(3, oldAggregateVersion);

            int count = statement.executeUpdate();
            if (count == 0) {
                if (queryAuthProviderExists(conn, (String)event.get(Constants.HOST), providerId)) {
                    throw new ConcurrencyException("Optimistic concurrency conflict during deleteAuthProvider for provider " + providerId + " aggregateVersion " + oldAggregateVersion + " but found a different version or already updated.");
                } else {
                    throw new SQLException("No record found to update for provider " + providerId + ".");
                }
            }
        } catch (SQLException e) {
            logger.error("SQLException during deleteAuthProvider for providerId {} aggregateVersion {}: {}", providerId, oldAggregateVersion, e.getMessage(), e);
            throw e;
        } catch (Exception e) {
            logger.error("Exception during deleteAuthProvider for providerId {} aggregateVersion {}: {}", providerId, oldAggregateVersion, e.getMessage(), e);
            throw e;
        }
    }

    @Override
    public Result<String> queryProviderKey(String providerId) {
        Result<String> result = null;
        String sql =
                """
                SELECT provider_id, kid, public_key, private_key, key_type, update_user, update_ts, aggregate_version
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
                    map.put("aggregateVersion", resultSet.getLong("aggregate_version"));
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
    public Result<String> queryApp(int offset, int limit, String hostId, String appId, String appName, String appDesc,
                                   Boolean isKafkaApp, String operationOwner, String deliveryOwner) {
        Result<String> result = null;
        String s =
                """
                SELECT COUNT(*) OVER () AS total,
                host_id, app_id, app_name, app_desc, is_kafka_app, operation_owner, delivery_owner, aggregate_version
                FROM app_t
                WHERE host_id = ?
                """;
        StringBuilder sqlBuilder = new StringBuilder(s);

        List<Object> parameters = new ArrayList<>();
        parameters.add(UUID.fromString(hostId));

        StringBuilder whereClause = new StringBuilder();

        addCondition(whereClause, parameters, "app_id", appId);
        addCondition(whereClause, parameters, "app_name", appName);
        addCondition(whereClause, parameters, "app_desc", appDesc);
        addCondition(whereClause, parameters, "is_kafka_app", isKafkaApp);
        addCondition(whereClause, parameters, "operation_owner", operationOwner != null ? UUID.fromString(operationOwner) : null);
        addCondition(whereClause, parameters, "delivery_owner", deliveryOwner != null ? UUID.fromString(deliveryOwner) : null);

        if (!whereClause.isEmpty()) {
            sqlBuilder.append("AND ").append(whereClause);
        }

        sqlBuilder.append(" ORDER BY app_id\n" +
                "LIMIT ? OFFSET ?");

        parameters.add(limit);
        parameters.add(offset);

        String sql = sqlBuilder.toString();
        int total = 0;
        List<Map<String, Object>> apps = new ArrayList<>();

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
                    map.put("hostId", resultSet.getObject("host_id", UUID.class));
                    map.put("appId", resultSet.getString("app_id"));
                    map.put("appName", resultSet.getString("app_name"));
                    map.put("appDesc", resultSet.getString("app_desc"));
                    map.put("isKafkaApp", resultSet.getBoolean("is_kafka_app"));
                    map.put("operationOwner", resultSet.getObject("operation_owner", UUID.class));
                    map.put("deliveryOwner", resultSet.getObject("delivery_owner", UUID.class));
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
    public Result<String> queryClient(int offset, int limit, String hostId, String appId, String apiId,
                                      String clientId, String clientName,
                                      String clientType, String clientProfile, String clientScope,
                                      String customClaim, String redirectUri, String authenticateClass,
                                      String deRefClientId) {
        Result<String> result = null;
        String s =
                """
                SELECT COUNT(*) OVER () AS total,
                client_id, host_id, app_id, api_id, client_name, client_type, client_profile,
                client_scope, custom_claim, redirect_uri, authenticate_class, deref_client_id,
                update_user, update_ts, aggregate_version
                FROM auth_client_t
                WHERE host_id = ?
                """;

        StringBuilder sqlBuilder = new StringBuilder(s);

        List<Object> parameters = new ArrayList<>();
        parameters.add(UUID.fromString(hostId));

        StringBuilder whereClause = new StringBuilder();

        addCondition(whereClause, parameters, "app_id", appId);
        addCondition(whereClause, parameters, "api_id", apiId);
        addCondition(whereClause, parameters, "client_id", clientId != null ? UUID.fromString(clientId) : null);
        addCondition(whereClause, parameters, "client_name", clientName);
        addCondition(whereClause, parameters, "client_type", clientType);
        addCondition(whereClause, parameters, "client_profile", clientProfile);
        addCondition(whereClause, parameters, "client_scope", clientScope);
        addCondition(whereClause, parameters, "custom_claim", customClaim);
        addCondition(whereClause, parameters, "redirect_uri", redirectUri);
        addCondition(whereClause, parameters, "authenticate_class", authenticateClass);
        addCondition(whereClause, parameters, "deref_client_id", deRefClientId != null ? UUID.fromString(deRefClientId) : null);

        if (!whereClause.isEmpty()) {
            sqlBuilder.append("AND ").append(whereClause);
        }

        sqlBuilder.append(" ORDER BY client_id\n" +
                "LIMIT ? OFFSET ?");

        parameters.add(limit);
        parameters.add(offset);

        String sql = sqlBuilder.toString();
        int total = 0;
        List<Map<String, Object>> clients = new ArrayList<>();

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
    public Result<String> listRefreshToken(int offset, int limit, String refreshToken, String hostId, String userId, String entityId,
                                           String email, String firstName, String lastName, String clientId, String appId,
                                           String appName, String scope, String userType, String roles, String groups, String positions,
                                           String attributes, String csrf, String customClaim, String updateUser, Timestamp updateTs) {
        Result<String> result = null;
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

        StringBuilder sqlBuilder = new StringBuilder(s);

        List<Object> parameters = new ArrayList<>();
        parameters.add(UUID.fromString(hostId));

        StringBuilder whereClause = new StringBuilder();

        addCondition(whereClause, parameters, "r.refresh_token", refreshToken != null ? UUID.fromString(refreshToken) : null);
        addCondition(whereClause, parameters, "r.user_id", userId != null ? UUID.fromString(userId) : null);
        addCondition(whereClause, parameters, "r.user_type", userType);
        addCondition(whereClause, parameters, "u.entity_id", entityId);
        addCondition(whereClause, parameters, "u.email", email);
        addCondition(whereClause, parameters, "r.first_name", firstName);
        addCondition(whereClause, parameters, "r.last_name", lastName);
        addCondition(whereClause, parameters, "r.client_id", clientId != null ? UUID.fromString(clientId) : null);
        addCondition(whereClause, parameters, "a.app_id", appId);
        addCondition(whereClause, parameters, "a.app_name", appName);
        addCondition(whereClause, parameters, "r.scope", scope);
        addCondition(whereClause, parameters, "r.roles", roles);
        addCondition(whereClause, parameters, "r.groups", groups);
        addCondition(whereClause, parameters, "r.positions", positions);
        addCondition(whereClause, parameters, "r.attributes", attributes);
        addCondition(whereClause, parameters, "r.csrf", csrf);
        addCondition(whereClause, parameters, "r.custom_claim", customClaim);
        addCondition(whereClause, parameters, "r.update_user", updateUser);
        addCondition(whereClause, parameters, "r.update_ts", updateTs);


        if (!whereClause.isEmpty()) {
            sqlBuilder.append("AND ").append(whereClause);
        }

        sqlBuilder.append(" ORDER BY r.user_id\n" +
                "LIMIT ? OFFSET ?");

        parameters.add(limit);
        parameters.add(offset);

        String sql = sqlBuilder.toString();
        int total = 0;
        List<Map<String, Object>> tokens = new ArrayList<>();

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
    public Result<String> listAuthCode(int offset, int limit, String hostId, String authCode, String userId,
                                       String entityId, String userType, String email, String roles, String groups, String positions,
                                       String attributes, String redirectUri, String scope, String remember, String codeChallenge,
                                       String challengeMethod, String updateUser, Timestamp updateTs) {
        Result<String> result = null;
        String s =
                """
                SELECT COUNT(*) OVER () AS total,
                host_id, auth_code, user_id, entity_id, user_type, email, roles, redirect_uri, scope, remember,
                code_challenge, challenge_method, update_user, update_ts, aggregate_version
                FROM auth_code_t
                WHERE host_id = ?
                """;

        StringBuilder sqlBuilder = new StringBuilder();

        List<Object> parameters = new ArrayList<>();
        parameters.add(UUID.fromString(hostId));

        StringBuilder whereClause = new StringBuilder();

        addCondition(whereClause, parameters, "auth_code", authCode);
        addCondition(whereClause, parameters, "user_id", userId != null ? UUID.fromString(userId) : null);
        addCondition(whereClause, parameters, "entity_id", entityId);
        addCondition(whereClause, parameters, "user_type", userType);
        addCondition(whereClause, parameters, "email", email);
        addCondition(whereClause, parameters, "roles", roles);
        addCondition(whereClause, parameters, "redirect_uri", redirectUri);
        addCondition(whereClause, parameters, "scope", scope);
        addCondition(whereClause, parameters, "remember", remember);
        addCondition(whereClause, parameters, "code_challenge", codeChallenge);
        addCondition(whereClause, parameters, "challenge_method", challengeMethod);


        if (!whereClause.isEmpty()) {
            sqlBuilder.append("AND ").append(whereClause);
        }

        sqlBuilder.append(" ORDER BY update_ts\n" +
                "LIMIT ? OFFSET ?");

        parameters.add(limit);
        parameters.add(offset);

        String sql = sqlBuilder.toString();
        int total = 0;
        List<Map<String, Object>> authCodes = new ArrayList<>();

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
                SELECT host_id, provider_id, provider_name, jwk, aggregate_version
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
                        map.put("jwk", resultSet.getString("jwk"));
                        map.put("aggregateVersion", resultSet.getLong("aggregate_version"));
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
    public Result<String> queryProvider(int offset, int limit, String hostId, String providerId, String providerName, String providerDesc,
                                        String operationOwner, String deliveryOwner, String jwk, String updateUser, Timestamp updateTs) {
        Result<String> result = null;
        String s =
                """
                SELECT COUNT(*) OVER () AS total,
                host_id, provider_id, provider_name, provider_desc, operation_owner,
                delivery_owner, jwk, update_user, update_ts, aggregate_version
                FROM auth_provider_t
                WHERE host_id = ?
                """;
        StringBuilder sqlBuilder = new StringBuilder(s);

        List<Object> parameters = new ArrayList<>();
        parameters.add(UUID.fromString(hostId));

        StringBuilder whereClause = new StringBuilder();

        addCondition(whereClause, parameters, "provider_id", providerId);
        addCondition(whereClause, parameters, "provider_name", providerName);
        addCondition(whereClause, parameters, "provider_desc", providerDesc);
        addCondition(whereClause, parameters, "operation_owner", operationOwner != null ? UUID.fromString(operationOwner) : null);
        addCondition(whereClause, parameters, "delivery_owner", deliveryOwner != null ? UUID.fromString(deliveryOwner) : null);
        addCondition(whereClause, parameters, "jwk", jwk);

        if (!whereClause.isEmpty()) {
            sqlBuilder.append("AND ").append(whereClause);
        }

        sqlBuilder.append(" ORDER BY provider_id\n" +
                "LIMIT ? OFFSET ?");

        parameters.add(limit);
        parameters.add(offset);

        String sql = sqlBuilder.toString();
        int total = 0;
        List<Map<String, Object>> providers = new ArrayList<>();

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
                SELECT provider_id, kid, public_key, private_key,\s
                key_type, update_user, update_ts, aggregate_version
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
                        map.put("aggregateVersion", resultSet.getLong("aggregate_version"));
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
                key_type, update_user, update_ts, aggregate_version
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
                        map.put("aggregateVersion", resultSet.getLong("aggregate_version"));
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
    public Result<String> listRefToken(int offset, int limit, String refToken, String hostId, String clientId, String clientName, String updateUser, Timestamp updateTs) {
        Result<String> result = null;
        String s =
                """
                SELECT COUNT(*) OVER () AS total,
                r.host_id, r.ref_token, r.jwt_token, r.client_id, c.client_name, r.update_user, r.update_ts, r.aggregate_version
                FROM auth_ref_token_t r, auth_client_t c
                WHERE r.client_id = c.client_id
                AND r.host_id = ?
                """;

        StringBuilder sqlBuilder = new StringBuilder(s);

        List<Object> parameters = new ArrayList<>();
        parameters.add(UUID.fromString(hostId));

        StringBuilder whereClause = new StringBuilder();

        addCondition(whereClause, parameters, "r.ref_token", refToken);
        addCondition(whereClause, parameters, "r.client_id", clientId != null ? UUID.fromString(clientId) : null);
        addCondition(whereClause, parameters, "c.client_name", clientName);
        addCondition(whereClause, parameters, "r.update_user", updateUser);
        addCondition(whereClause, parameters, "r.update_ts", updateTs);


        if (!whereClause.isEmpty()) {
            sqlBuilder.append("AND ").append(whereClause);
        }

        sqlBuilder.append(" ORDER BY r.client_id\n" +
                "LIMIT ? OFFSET ?");

        parameters.add(limit);
        parameters.add(offset);

        String sql = sqlBuilder.toString();
        int total = 0;
        List<Map<String, Object>> tokens = new ArrayList<>();

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
