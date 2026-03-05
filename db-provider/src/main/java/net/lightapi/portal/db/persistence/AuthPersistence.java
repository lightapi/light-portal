package net.lightapi.portal.db.persistence;

import com.networknt.monad.Result;
import net.lightapi.portal.db.PortalPersistenceException;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.Map;

public interface AuthPersistence {
    // App
    void createApp(Connection conn, Map<String, Object> event) throws PortalPersistenceException;
    void updateApp(Connection conn, Map<String, Object> event) throws PortalPersistenceException;
    void deleteApp(Connection conn, Map<String, Object> event) throws PortalPersistenceException;
    Result<String> queryApp(int offset, int limit, String filters, String globalFilter, String sorting, boolean active, String hostId);
    Result<String> queryAppById(String hostId, String appId);
    Result<String> getAppIdLabel(String hostId);


    // Client
    void createClient(Connection conn, Map<String, Object> event) throws PortalPersistenceException;
    void updateClient(Connection conn, Map<String, Object> event) throws PortalPersistenceException;
    void deleteClient(Connection conn, Map<String, Object> event) throws PortalPersistenceException;
    Result<String> queryClient(int offset, int limit, String filters, String globalFilter, String sorting, boolean active, String hostId);
    Result<String> queryClientByClientId(String clientId);
    Result<String> getClientById(String hostId, String clientId);
    Result<String> queryClientByProviderClientId(String providerId, String clientId);
    Result<String> queryClientByHostAppId(String host_id, String appId);
    Result<String> getClientIdLabel(String hostId);

    // AuthProvider
    void createAuthProvider(Connection conn, Map<String, Object> event) throws PortalPersistenceException;
    void updateAuthProvider(Connection conn, Map<String, Object> event) throws PortalPersistenceException;
    void deleteAuthProvider(Connection conn, Map<String, Object> event) throws PortalPersistenceException;
    void rotateAuthProvider(Connection conn, Map<String, Object> event) throws PortalPersistenceException; // Treated as an update operation
    Result<Map<String, Object>> queryProviderById(String providerId);
    Result<String> getProviderIdLabel(String hostId);
    String queryProviderByName(String hostId, String providerName);
    Result<String> queryProvider(int offset, int limit, String filters, String globalFilter, String sorting, boolean active, String hostId);
    Result<String> queryProviderKey(String providerId);
    Result<Map<String, Object>> queryCurrentProviderKey(String providerId);
    Result<Map<String, Object>> queryLongLiveProviderKey(String providerId);

    void createAuthProviderApi(Connection conn, Map<String, Object> event) throws PortalPersistenceException;
    void updateAuthProviderApi(Connection conn, Map<String, Object> event) throws PortalPersistenceException;
    void deleteAuthProviderApi(Connection conn, Map<String, Object> event) throws PortalPersistenceException;
    Result<String> queryAuthProviderApi(int offset, int limit, String filters, String globalFilter, String sorting, boolean active, String hostId);

    void createAuthProviderClient(Connection conn, Map<String, Object> event) throws PortalPersistenceException;
    void updateAuthProviderClient(Connection conn, Map<String, Object> event) throws PortalPersistenceException;
    void deleteAuthProviderClient(Connection conn, Map<String, Object> event) throws PortalPersistenceException;
    Result<String> queryAuthProviderClient(int offset, int limit, String filters, String globalFilter, String sorting, boolean active, String hostId);

    // AuthCode
    void createAuthCode(Connection conn, Map<String, Object> event) throws PortalPersistenceException;
    void deleteAuthCode(Connection conn, Map<String, Object> event) throws PortalPersistenceException;
    Result<String> queryAuthCode(String authCode);
    Result<String> getAuthCode(int offset, int limit, String filters, String globalFilter, String sorting, boolean active, String hostId);

    // RefreshToken
    void createRefreshToken(Connection conn, Map<String, Object> event) throws PortalPersistenceException;
    void deleteRefreshToken(Connection conn, Map<String, Object> event) throws PortalPersistenceException;
    Result<String> getRefreshToken(int offset, int limit, String filters, String globalFilter, String sorting, boolean active, String hostId);
    Result<String> queryRefreshToken(String refreshToken);

    // RefToken
    void createRefToken(Connection conn, Map<String, Object> event) throws PortalPersistenceException;
    void deleteRefToken(Connection conn, Map<String, Object> event) throws PortalPersistenceException;
    Result<String> getRefToken(int offset, int limit, String filters, String globalFilter, String sorting, boolean active, String hostId);
    Result<String> queryRefToken(String refToken);

    // ClientToken
    void createClientToken(Connection conn, Map<String, Object> event) throws PortalPersistenceException;
    void deleteClientToken(Connection conn, Map<String, Object> event) throws PortalPersistenceException;
    Result<String> queryClientToken(int offset, int limit, String filters, String globalFilter, String sorting, boolean active, String hostId);
}
