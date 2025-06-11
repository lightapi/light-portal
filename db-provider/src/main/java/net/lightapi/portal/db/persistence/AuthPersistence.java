package net.lightapi.portal.db.persistence;

import com.networknt.monad.Result;
import java.sql.Timestamp;
import java.util.Map;

public interface AuthPersistence {
    // App
    Result<String> createApp(Map<String, Object> event);
    Result<String> updateApp(Map<String, Object> event);
    Result<String> deleteApp(Map<String, Object> event);
    Result<String> queryApp(int offset, int limit, String hostId, String appId, String appName, String appDesc, Boolean isKafkaApp, String operationOwner, String deliveryOwner);
    Result<String> getAppIdLabel(String hostId);

    // Client
    Result<String> createClient(Map<String, Object> event);
    Result<String> updateClient(Map<String, Object> event);
    Result<String> deleteClient(Map<String, Object> event);
    Result<String> queryClient(int offset, int limit, String hostId, String appId, String apiId, String clientId, String clientName, String clientType, String clientProfile, String clientScope, String customClaim, String redirectUri, String authenticateClass, String deRefClientId);
    Result<String> queryClientByClientId(String clientId);
    Result<String> queryClientByProviderClientId(String providerId, String clientId);
    Result<String> queryClientByHostAppId(String host_id, String appId);

    // AuthProvider
    Result<String> createAuthProvider(Map<String, Object> event);
    Result<String> updateAuthProvider(Map<String, Object> event);
    Result<String> deleteAuthProvider(Map<String, Object> event);
    Result<String> rotateAuthProvider(Map<String, Object> event);
    Result<Map<String, Object>> queryProviderById(String providerId);
    Result<String> queryProvider(int offset, int limit, String hostId, String providerId, String providerName, String providerDesc, String operationOwner, String deliveryOwner, String jwk, String updateUser, Timestamp updateTs);
    Result<String> queryProviderKey(String providerId);
    Result<Map<String, Object>> queryCurrentProviderKey(String providerId);
    Result<Map<String, Object>> queryLongLiveProviderKey(String providerId);

    // AuthCode
    Result<String> createAuthCode(Map<String, Object> event);
    Result<String> deleteAuthCode(Map<String, Object> event);
    Result<String> queryAuthCode(String authCode);
    Result<String> listAuthCode(int offset, int limit, String hostId, String authCode, String userId, String entityId, String userType, String email, String roles, String groups, String positions, String attributes, String redirectUri, String scope, String remember, String codeChallenge, String challengeMethod, String updateUser, Timestamp updateTs);

    // RefreshToken
    Result<String> createRefreshToken(Map<String, Object> event);
    Result<String> deleteRefreshToken(Map<String, Object> event);
    Result<String> listRefreshToken(int offset, int limit, String refreshToken, String hostId, String userId, String entityId, String email, String firstName, String lastName, String clientId, String appId, String appName, String scope, String userType, String roles, String groups, String positions, String attributes, String csrf, String customClaim, String updateUser, Timestamp updateTs);
    Result<String> queryRefreshToken(String refreshToken);
}
