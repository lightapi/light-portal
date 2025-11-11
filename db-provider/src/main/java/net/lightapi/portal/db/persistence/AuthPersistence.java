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
    Result<String> queryApp(int offset, int limit, String filters, String globalFilter, String sorting, String hostId);
    Result<String> queryAppById(String hostId, String appId);
    Result<String> getAppIdLabel(String hostId);


    // Client
    void createClient(Connection conn, Map<String, Object> event) throws SQLException, Exception;
    void updateClient(Connection conn, Map<String, Object> event) throws SQLException, Exception;
    void deleteClient(Connection conn, Map<String, Object> event) throws SQLException, Exception;
    Result<String> queryClient(int offset, int limit, String filters, String globalFilter, String sorting, String hostId);
    Result<String> queryClientByClientId(String clientId);
    Result<String> getClientById(String hostId, String clientId);
    Result<String> queryClientByProviderClientId(String providerId, String clientId);
    Result<String> queryClientByHostAppId(String host_id, String appId);

    // AuthProvider
    void createAuthProvider(Connection conn, Map<String, Object> event) throws SQLException, Exception;
    void updateAuthProvider(Connection conn, Map<String, Object> event) throws SQLException, Exception;
    void deleteAuthProvider(Connection conn, Map<String, Object> event) throws SQLException, Exception;
    void rotateAuthProvider(Connection conn, Map<String, Object> event) throws SQLException, Exception; // Treated as an update operation
    Result<Map<String, Object>> queryProviderById(String providerId);
    String queryProviderByName(String hostId, String providerName);
    Result<String> queryProvider(int offset, int limit, String filters, String globalFilter, String sorting, String hostId);
    Result<String> queryProviderKey(String providerId);
    Result<Map<String, Object>> queryCurrentProviderKey(String providerId);
    Result<Map<String, Object>> queryLongLiveProviderKey(String providerId);

    // AuthCode
    void createAuthCode(Connection conn, Map<String, Object> event) throws SQLException, Exception;
    void deleteAuthCode(Connection conn, Map<String, Object> event) throws SQLException, Exception;
    Result<String> queryAuthCode(String authCode);
    Result<String> getAuthCode(int offset, int limit, String filters, String globalFilter, String sorting, String hostId);

    // RefreshToken
    void createRefreshToken(Connection conn, Map<String, Object> event) throws SQLException, Exception;
    void deleteRefreshToken(Connection conn, Map<String, Object> event) throws SQLException, Exception;
    Result<String> getRefreshToken(int offset, int limit, String filters, String globalFilter, String sorting, String hostId);
    Result<String> queryRefreshToken(String refreshToken);

    // RefToken
    void createRefToken(Connection conn, Map<String, Object> event) throws SQLException, Exception;
    void deleteRefToken(Connection conn, Map<String, Object> event) throws SQLException, Exception;
    Result<String> getRefToken(int offset, int limit, String filters, String globalFilter, String sorting, String hostId);
    Result<String> queryRefToken(String refToken);

}
