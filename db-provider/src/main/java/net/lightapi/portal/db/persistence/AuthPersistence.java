package net.lightapi.portal.db.persistence;

import com.networknt.monad.Result;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.Map;

public interface AuthPersistence {
    // App
    void createApp(Connection conn, Map<String, Object> event) throws SQLException, Exception;
    void updateApp(Connection conn, Map<String, Object> event) throws SQLException, Exception;
    void deleteApp(Connection conn, Map<String, Object> event) throws SQLException, Exception;
    Result<String> queryApp(int offset, int limit, String hostId, String appId, String appName, String appDesc, Boolean isKafkaApp, String operationOwner, String deliveryOwner);
    Result<String> getAppIdLabel(String hostId);

    // Client
    void createClient(Connection conn, Map<String, Object> event) throws SQLException, Exception;
    void updateClient(Connection conn, Map<String, Object> event) throws SQLException, Exception;
    void deleteClient(Connection conn, Map<String, Object> event) throws SQLException, Exception;
    Result<String> queryClient(int offset, int limit, String hostId, String appId, String apiId, String clientId, String clientName, String clientType, String clientProfile, String clientScope, String customClaim, String redirectUri, String authenticateClass, String deRefClientId);
    Result<String> queryClientByClientId(String clientId);
    Result<String> queryClientByProviderClientId(String providerId, String clientId);
    Result<String> queryClientByHostAppId(String host_id, String appId);

    // AuthProvider
    void createAuthProvider(Connection conn, Map<String, Object> event) throws SQLException, Exception;
    void updateAuthProvider(Connection conn, Map<String, Object> event) throws SQLException, Exception;
    void deleteAuthProvider(Connection conn, Map<String, Object> event) throws SQLException, Exception;
    void rotateAuthProvider(Connection conn, Map<String, Object> event) throws SQLException, Exception; // Treated as an update operation
    Result<Map<String, Object>> queryProviderById(String providerId);
    Result<String> queryProvider(int offset, int limit, String hostId, String providerId, String providerName, String providerDesc, String operationOwner, String deliveryOwner, String jwk, String updateUser, Timestamp updateTs);
    Result<String> queryProviderKey(String providerId);
    Result<Map<String, Object>> queryCurrentProviderKey(String providerId);
    Result<Map<String, Object>> queryLongLiveProviderKey(String providerId);

    // AuthCode
    void createAuthCode(Connection conn, Map<String, Object> event) throws SQLException, Exception;
    void deleteAuthCode(Connection conn, Map<String, Object> event) throws SQLException, Exception;
    Result<String> queryAuthCode(String authCode);
    Result<String> listAuthCode(int offset, int limit, String hostId, String authCode, String userId, String entityId, String userType, String email, String roles, String groups, String positions, String attributes, String redirectUri, String scope, String remember, String codeChallenge, String challengeMethod, String updateUser, Timestamp updateTs);

    // RefreshToken
    void createRefreshToken(Connection conn, Map<String, Object> event) throws SQLException, Exception;
    void deleteRefreshToken(Connection conn, Map<String, Object> event) throws SQLException, Exception;
    Result<String> listRefreshToken(int offset, int limit, String refreshToken, String hostId, String userId, String entityId, String email, String firstName, String lastName, String clientId, String appId, String appName, String scope, String userType, String roles, String groups, String positions, String attributes, String csrf, String customClaim, String updateUser, Timestamp updateTs);
    Result<String> queryRefreshToken(String refreshToken);

    // RefToken
    void createRefToken(Connection conn, Map<String, Object> event) throws SQLException, Exception;
    void deleteRefToken(Connection conn, Map<String, Object> event) throws SQLException, Exception;
    Result<String> listRefToken(int offset, int limit, String refToken, String hostId, String clientId, String clientName, String updateUser, Timestamp updateTs);
    Result<String> queryRefToken(String refToken);

}
