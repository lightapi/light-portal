package net.lightapi.portal.db;

import com.networknt.monad.Result;
import io.cloudevents.CloudEvent;
import net.lightapi.portal.db.persistence.*;
import net.lightapi.portal.db.util.NotificationService;
import net.lightapi.portal.db.util.NotificationServiceImpl;
import net.lightapi.portal.validation.FilterCriterion;
import net.lightapi.portal.validation.SortCriterion;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.SQLException; // Added import
import java.sql.Timestamp;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class PortalDbProviderImpl implements PortalDbProvider {
    public static final Logger logger = LoggerFactory.getLogger(PortalDbProviderImpl.class);
    // These constants can be moved to a shared project constants class if they are widely used.
    public static final String SQL_EXCEPTION = "ERR10017";
    public static final String GENERIC_EXCEPTION = "ERR10014";
    public static final String OBJECT_NOT_FOUND = "ERR11637";


    private final ReferenceDataPersistence referenceDataPersistence;
    private final UserPersistence userPersistence;
    private final AuthPersistence authPersistence;
    private final ApiServicePersistence apiServicePersistence;
    private final HostOrgPersistence hostOrgPersistence;
    private final ConfigPersistence configPersistence;
    private final InstanceDeploymentPersistence instanceDeploymentPersistence;
    private final AccessControlPersistence accessControlPersistence;
    private final CategoryPersistence categoryPersistence;
    private final TagPersistence tagPersistence;
    private final SchemaPersistence schemaPersistence;
    private final SchedulePersistence schedulePersistence;
    private final RulePersistence rulePersistence;
    private final NotificationDataPersistence notificationDataPersistence;
    private final EventPersistence eventPersistence;

    public PortalDbProviderImpl() {
        NotificationService notificationService = new NotificationServiceImpl();

        this.referenceDataPersistence = new ReferenceDataPersistenceImpl(notificationService);
        this.userPersistence = new UserPersistenceImpl(notificationService);
        this.authPersistence = new AuthPersistenceImpl(notificationService);
        this.apiServicePersistence = new ApiServicePersistenceImpl(notificationService);
        this.hostOrgPersistence = new HostOrgPersistenceImpl(notificationService);
        this.configPersistence = new ConfigPersistenceImpl(notificationService);
        this.instanceDeploymentPersistence = new InstanceDeploymentPersistenceImpl(notificationService);
        this.accessControlPersistence = new AccessControlPersistenceImpl(notificationService);
        this.categoryPersistence = new CategoryPersistenceImpl(notificationService);
        this.tagPersistence = new TagPersistenceImpl(notificationService);
        this.schemaPersistence = new SchemaPersistenceImpl(notificationService);
        this.schedulePersistence = new SchedulePersistenceImpl(notificationService);
        this.rulePersistence = new RulePersistenceImpl(notificationService);
        this.notificationDataPersistence = new NotificationDataPersistenceImpl();
        this.eventPersistence = new EventPersistenceImpl();
    }

    // --- Reference ---
    @Override public void createRefTable(Connection connection, Map<String, Object> event) throws SQLException, Exception { referenceDataPersistence.createRefTable(connection, event); }
    @Override public void updateRefTable(Connection connection, Map<String, Object> event) throws SQLException, Exception { referenceDataPersistence.updateRefTable(connection, event); }
    @Override public void deleteRefTable(Connection connection, Map<String, Object> event) throws SQLException, Exception { referenceDataPersistence.deleteRefTable(connection, event); }
    @Override public Result<String> getRefTable(int offset, int limit, String hostId, String tableId, String tableName, String tableDesc, Boolean active, Boolean editable) { return referenceDataPersistence.getRefTable(offset, limit, hostId, tableId, tableName, tableDesc, active, editable); }
    @Override public Result<String> getRefTableById(String tableId) { return referenceDataPersistence.getRefTableById(tableId); }
    @Override public Result<String> getRefTableLabel(String hostId) { return referenceDataPersistence.getRefTableLabel(hostId); }
    @Override public void createRefValue(Connection connection, Map<String, Object> event) throws SQLException, Exception { referenceDataPersistence.createRefValue(connection, event); }
    @Override public void updateRefValue(Connection connection, Map<String, Object> event) throws SQLException, Exception { referenceDataPersistence.updateRefValue(connection, event); }
    @Override public void deleteRefValue(Connection connection, Map<String, Object> event) throws SQLException, Exception { referenceDataPersistence.deleteRefValue(connection, event); }
    @Override public Result<String> getRefValue(int offset, int limit, String valueId, String tableId, String valueCode, String valueDesc, Integer displayOrder, Boolean active) { return referenceDataPersistence.getRefValue(offset, limit, valueId, tableId, valueCode, valueDesc, displayOrder, active); }
    @Override public Result<String> getRefValueById(String valueId) { return referenceDataPersistence.getRefValueById(valueId); }
    @Override public Result<String> getRefValueLabel(String tableId) { return referenceDataPersistence.getRefValueLabel(tableId); }
    @Override public void createRefLocale(Connection connection, Map<String, Object> event) throws SQLException, Exception { referenceDataPersistence.createRefLocale(connection, event); }
    @Override public void updateRefLocale(Connection connection, Map<String, Object> event) throws SQLException, Exception { referenceDataPersistence.updateRefLocale(connection, event); }
    @Override public void deleteRefLocale(Connection connection, Map<String, Object> event) throws SQLException, Exception { referenceDataPersistence.deleteRefLocale(connection, event); }
    @Override public Result<String> getRefLocale(int offset, int limit, String valueId, String valueCode, String valueDesc, String language, String valueLabel) { return referenceDataPersistence.getRefLocale(offset, limit, valueId, valueCode, valueDesc, language, valueLabel); }
    @Override public void createRefRelationType(Connection connection, Map<String, Object> event) throws SQLException, Exception { referenceDataPersistence.createRefRelationType(connection, event); }
    @Override public void updateRefRelationType(Connection connection, Map<String, Object> event) throws SQLException, Exception { referenceDataPersistence.updateRefRelationType(connection, event); }
    @Override public void deleteRefRelationType(Connection connection, Map<String, Object> event) throws SQLException, Exception { referenceDataPersistence.deleteRefRelationType(connection, event); }
    @Override public Result<String> getRefRelationType(int offset, int limit, String relationId, String relationName, String relationDesc) { return referenceDataPersistence.getRefRelationType(offset, limit, relationId, relationName, relationDesc); }
    @Override public void createRefRelation(Connection connection, Map<String, Object> event) throws SQLException, Exception { referenceDataPersistence.createRefRelation(connection, event); }
    @Override public void updateRefRelation(Connection connection, Map<String, Object> event) throws SQLException, Exception { referenceDataPersistence.updateRefRelation(connection, event); }
    @Override public void deleteRefRelation(Connection connection, Map<String, Object> event) throws SQLException, Exception { referenceDataPersistence.deleteRefRelation(connection, event); }
    @Override public Result<String> getRefRelation(int offset, int limit, String relationId, String relationName, String valueIdFrom, String valueCodeFrom, String valueIdTo, String valueCodeTo, Boolean active) { return referenceDataPersistence.getRefRelation(offset, limit, relationId, relationName, valueIdFrom, valueCodeFrom, valueIdTo, valueCodeTo, active); }


    // --- User ---
    @Override public Result<String> loginUserByEmail(String email) { return userPersistence.loginUserByEmail(email); }
    @Override public Result<String> queryUserByEmail(String email) { return userPersistence.queryUserByEmail(email); }
    @Override public Result<String> queryUserById(String userId) { return userPersistence.queryUserById(userId); }
    @Override public Result<String> queryUserByTypeEntityId(String userType, String entityId) { return userPersistence.queryUserByTypeEntityId(userType, entityId); }
    @Override public Result<String> queryUserByWallet(String cryptoType, String cryptoAddress) { return userPersistence.queryUserByWallet(cryptoType, cryptoAddress); }
    @Override public Result<String> queryUserByHostId(int offset, int limit, String hostId, String email, String language, String userType, String entityId, String referralId, String managerId, String firstName, String lastName, String phoneNumber, String gender, String birthday, String country, String province, String city, String address, String postCode, Boolean verified, Boolean locked) { return userPersistence.queryUserByHostId(offset, limit, hostId, email, language, userType, entityId, referralId, managerId, firstName, lastName, phoneNumber, gender, birthday, country, province, city, address, postCode, verified, locked); }
    @Override public void createUser(Connection connection, Map<String, Object> event) throws SQLException, Exception { userPersistence.createUser(connection, event); }
    @Override public void onboardUser(Connection connection, Map<String, Object> event) throws SQLException, Exception { userPersistence.onboardUser(connection, event); }
    @Override public Result<Long> queryNonceByUserId(String userId) { return userPersistence.queryNonceByUserId(userId); }
    @Override public void confirmUser(Connection connection, Map<String, Object> event) throws SQLException, Exception { userPersistence.confirmUser(connection, event); }
    @Override public void verifyUser(Connection connection, Map<String, Object> event) throws SQLException, Exception { userPersistence.verifyUser(connection, event); }
    @Override public void createSocialUser(Connection connection, Map<String, Object> event) throws SQLException, Exception { userPersistence.createSocialUser(connection, event); }
    @Override public void updateUser(Connection connection, Map<String, Object> event) throws SQLException, Exception { userPersistence.updateUser(connection, event); }
    @Override public void deleteUser(Connection connection, Map<String, Object> event) throws SQLException, Exception { userPersistence.deleteUser(connection, event); }
    @Override public void forgetPassword(Connection connection, Map<String, Object> event) throws SQLException, Exception { userPersistence.forgetPassword(connection, event); }
    @Override public void resetPassword(Connection connection, Map<String, Object> event) throws SQLException, Exception { userPersistence.resetPassword(connection, event); }
    @Override public void changePassword(Connection connection, Map<String, Object> event) throws SQLException, Exception { userPersistence.changePassword(connection, event); }
    @Override public Result<String> queryUserLabel(String hostId) { return userPersistence.queryUserLabel(hostId); }
    @Override public Result<String> queryEmailByWallet(String cryptoType, String cryptoAddress) { return userPersistence.queryEmailByWallet(cryptoType, cryptoAddress); }
    @Override public void sendPrivateMessage(Connection connection, Map<String, Object> event) throws SQLException, Exception { userPersistence.sendPrivateMessage(connection, event); }
    @Override public void updatePayment(Connection connection, Map<String, Object> event) throws SQLException, Exception { userPersistence.updatePayment(connection, event); }
    @Override public void deletePayment(Connection connection, Map<String, Object> event) throws SQLException, Exception { userPersistence.deletePayment(connection, event); }
    @Override public void createOrder(Connection connection, Map<String, Object> event) throws SQLException, Exception { userPersistence.createOrder(connection, event); }
    @Override public void cancelOrder(Connection connection, Map<String, Object> event) throws SQLException, Exception { userPersistence.cancelOrder(connection, event); }
    @Override public void deliverOrder(Connection connection, Map<String, Object> event) throws SQLException, Exception { userPersistence.deliverOrder(connection, event); }
    @Override public Result<String> queryNotification(int offset, int limit, String hostId, String userId, Long nonce, String eventClass, Boolean successFlag, Timestamp processTs, String eventJson, String error) { return notificationDataPersistence.queryNotification(offset, limit, hostId, userId, nonce, eventClass, successFlag, processTs, eventJson, error); }

    // --- Auth ---
    @Override public void createApp(Connection connection, Map<String, Object> event) throws SQLException, Exception { authPersistence.createApp(connection, event); }
    @Override public void updateApp(Connection connection, Map<String, Object> event) throws SQLException, Exception { authPersistence.updateApp(connection, event); }
    @Override public void deleteApp(Connection connection, Map<String, Object> event) throws SQLException, Exception { authPersistence.deleteApp(connection, event); }
    @Override public Result<String> queryApp(int offset, int limit, String hostId, String appId, String appName, String appDesc, Boolean isKafkaApp, String operationOwner, String deliveryOwner) { return authPersistence.queryApp(offset, limit, hostId, appId, appName, appDesc, isKafkaApp, operationOwner, deliveryOwner); }
    @Override public Result<String> getAppIdLabel(String hostId) { return authPersistence.getAppIdLabel(hostId); }
    @Override public void createClient(Connection connection, Map<String, Object> event) throws SQLException, Exception { authPersistence.createClient(connection, event); }
    @Override public void updateClient(Connection connection, Map<String, Object> event) throws SQLException, Exception { authPersistence.updateClient(connection, event); }
    @Override public void deleteClient(Connection connection, Map<String, Object> event) throws SQLException, Exception { authPersistence.deleteClient(connection, event); }
    @Override public Result<String> queryClient(int offset, int limit, String hostId, String appId, String apiId, String clientId, String clientName, String clientType, String clientProfile, String clientScope, String customClaim, String redirectUri, String authenticateClass, String deRefClientId) { return authPersistence.queryClient(offset, limit, hostId, appId, apiId, clientId, clientName, clientType, clientProfile, clientScope, customClaim, redirectUri, authenticateClass, deRefClientId); }
    @Override public Result<String> queryClientByClientId(String clientId) { return authPersistence.queryClientByClientId(clientId); }
    @Override public Result<String> queryClientByProviderClientId(String providerId, String clientId) { return authPersistence.queryClientByProviderClientId(providerId, clientId); }
    @Override public Result<String> queryClientByHostAppId(String host_id, String appId) { return authPersistence.queryClientByHostAppId(host_id, appId); }
    @Override public void createAuthProvider(Connection connection, Map<String, Object> event) throws SQLException, Exception { authPersistence.createAuthProvider(connection, event); }
    @Override public void updateAuthProvider(Connection connection, Map<String, Object> event) throws SQLException, Exception { authPersistence.updateAuthProvider(connection, event); }
    @Override public void deleteAuthProvider(Connection connection, Map<String, Object> event) throws SQLException, Exception { authPersistence.deleteAuthProvider(connection, event); }
    @Override public void rotateAuthProvider(Connection connection, Map<String, Object> event) throws SQLException, Exception { authPersistence.rotateAuthProvider(connection, event); }
    @Override public Result<Map<String, Object>> queryProviderById(String providerId) { return authPersistence.queryProviderById(providerId); }
    @Override public Result<String> queryProvider(int offset, int limit, String hostId, String providerId, String providerName, String providerDesc, String operationOwner, String deliveryOwner, String jwk, String updateUser, Timestamp updateTs) { return authPersistence.queryProvider(offset, limit, hostId, providerId, providerName, providerDesc, operationOwner, deliveryOwner, jwk, updateUser, updateTs); }
    @Override public Result<String> queryProviderKey(String providerId) { return authPersistence.queryProviderKey(providerId); }
    @Override public Result<Map<String, Object>> queryCurrentProviderKey(String providerId) { return authPersistence.queryCurrentProviderKey(providerId); }
    @Override public Result<Map<String, Object>> queryLongLiveProviderKey(String providerId) { return authPersistence.queryLongLiveProviderKey(providerId); }
    @Override public void createAuthCode(Connection connection, Map<String, Object> event) throws SQLException, Exception { authPersistence.createAuthCode(connection, event); }
    @Override public void deleteAuthCode(Connection connection, Map<String, Object> event) throws SQLException, Exception { authPersistence.deleteAuthCode(connection, event); }
    @Override public Result<String> queryAuthCode(String authCode) { return authPersistence.queryAuthCode(authCode); }
    @Override public Result<String> listAuthCode(int offset, int limit, String hostId, String authCode, String userId, String entityId, String userType, String email, String roles, String groups, String positions, String attributes, String redirectUri, String scope, String remember, String codeChallenge, String challengeMethod, String updateUser, Timestamp updateTs) { return authPersistence.listAuthCode(offset, limit, hostId, authCode, userId, entityId, userType, email, roles, groups, positions, attributes, redirectUri, scope, remember, codeChallenge, challengeMethod, updateUser, updateTs); }
    @Override public void createRefreshToken(Connection connection, Map<String, Object> event) throws SQLException, Exception { authPersistence.createRefreshToken(connection, event); }
    @Override public void deleteRefreshToken(Connection connection, Map<String, Object> event) throws SQLException, Exception { authPersistence.deleteRefreshToken(connection, event); }
    @Override public Result<String> listRefreshToken(int offset, int limit, String refreshToken, String hostId, String userId, String entityId, String email, String firstName, String lastName, String clientId, String appId, String appName, String scope, String userType, String roles, String groups, String positions, String attributes, String csrf, String customClaim, String updateUser, Timestamp updateTs) { return authPersistence.listRefreshToken(offset, limit, refreshToken, hostId, userId, entityId, email, firstName, lastName, clientId, appId, appName, scope, userType, roles, groups, positions, attributes, csrf, customClaim, updateUser, updateTs); }
    @Override public Result<String> queryRefreshToken(String refreshToken) { return authPersistence.queryRefreshToken(refreshToken); }

    @Override public void createRefToken(Connection connection, Map<String, Object> event) throws SQLException, Exception {authPersistence.createRefToken(connection, event); }
    @Override public void deleteRefToken(Connection connection, Map<String, Object> event) throws SQLException, Exception { authPersistence.deleteRefToken(connection, event); }
    @Override public Result<String> listRefToken(int offset, int limit, String refToken, String hostId, String clientId, String clientName, String updateUser, Timestamp updateTs) { return authPersistence.listRefToken(offset, limit, refToken, hostId, clientId, clientName, updateUser, updateTs); }
    @Override public Result<String> queryRefToken(String refToken) {return authPersistence.queryRefToken(refToken); };

    // --- ApiService ---
    @Override public void createService(Connection connection, Map<String, Object> event) throws SQLException, Exception { apiServicePersistence.createService(connection, event); }
    @Override public void updateService(Connection connection, Map<String, Object> event) throws SQLException, Exception { apiServicePersistence.updateService(connection, event); }
    @Override public void deleteService(Connection connection, Map<String, Object> event) throws SQLException, Exception { apiServicePersistence.deleteService(connection, event); }
    @Override public Result<String> queryService(int offset, int limit, String hostId, String apiId, String apiName, String apiDesc, String operationOwner, String deliveryOwner, String region, String businessGroup, String lob, String platform, String capability, String gitRepo, String apiTags, String apiStatus) { return apiServicePersistence.queryService(offset, limit, hostId, apiId, apiName, apiDesc, operationOwner, deliveryOwner, region, businessGroup, lob, platform, capability, gitRepo, apiTags, apiStatus); }
    @Override public Result<String> queryApiLabel(String hostId) { return apiServicePersistence.queryApiLabel(hostId); }
    @Override public void createServiceVersion(Connection connection, Map<String, Object> event, List<Map<String, Object>> endpoints) throws SQLException, Exception { apiServicePersistence.createServiceVersion(connection, event, endpoints); }
    @Override public void updateServiceVersion(Connection connection, Map<String, Object> event, List<Map<String, Object>> endpoints) throws SQLException, Exception { apiServicePersistence.updateServiceVersion(connection, event, endpoints); }
    @Override public void deleteServiceVersion(Connection connection, Map<String, Object> event) throws SQLException, Exception { apiServicePersistence.deleteServiceVersion(connection, event); }
    @Override public Result<String> queryServiceVersion(String hostId, String apiId) { return apiServicePersistence.queryServiceVersion(hostId, apiId); }
    @Override public Result<String> getApiVersionIdLabel(String hostId) { return apiServicePersistence.getApiVersionIdLabel(hostId); }
    @Override public Result<String> queryApiVersionLabel(String hostId, String apiId) { return apiServicePersistence.queryApiVersionLabel(hostId, apiId); }
    @Override public void updateServiceSpec(Connection connection, Map<String, Object> event, List<Map<String, Object>> endpoints) throws SQLException, Exception { apiServicePersistence.updateServiceSpec(connection, event, endpoints); }
    @Override public Result<String> queryServiceEndpoint(int offset, int limit, String hostId, String apiVersionId, String apiId, String apiVersion, String endpoint, String method, String path, String desc) { return apiServicePersistence.queryServiceEndpoint(offset, limit, hostId, apiVersionId, apiId, apiVersion, endpoint, method, path, desc); }
    @Override public Result<String> queryEndpointLabel(String hostId, String apiId, String apiVersion) { return apiServicePersistence.queryEndpointLabel(hostId, apiId, apiVersion); }
    @Override public Result<String> queryEndpointScope(String hostId, String endpointId) { return apiServicePersistence.queryEndpointScope(hostId, endpointId); }
    @Override public void createEndpointRule(Connection connection, Map<String, Object> event) throws SQLException, Exception { apiServicePersistence.createEndpointRule(connection, event); }
    @Override public void deleteEndpointRule(Connection connection, Map<String, Object> event) throws SQLException, Exception { apiServicePersistence.deleteEndpointRule(connection, event); }
    @Override public Result<String> queryEndpointRule(String hostId, String apiId, String apiVersion, String endpoint) { return apiServicePersistence.queryEndpointRule(hostId, apiId, apiVersion, endpoint); }
    @Override public Result<String> queryServiceRule(String hostId, String apiId, String apiVersion) { return apiServicePersistence.queryServiceRule(hostId, apiId, apiVersion); }
    @Override public Result<String> queryServicePermission(String hostId, String apiId, String apiVersion) { return apiServicePersistence.queryServicePermission(hostId, apiId, apiVersion); }
    @Override public Result<List<String>> queryServiceFilter(String hostId, String apiId, String apiVersion) { return apiServicePersistence.queryServiceFilter(hostId, apiId, apiVersion); }
    @Override public Result<String> getServiceIdLabel(String hostId) { return apiServicePersistence.getServiceIdLabel(hostId); }

    // --- HostOrg ---
    @Override public void createOrg(Connection connection, Map<String, Object> event) throws SQLException, Exception { hostOrgPersistence.createOrg(connection, event); }
    @Override public void updateOrg(Connection connection, Map<String, Object> event) throws SQLException, Exception { hostOrgPersistence.updateOrg(connection, event); }
    @Override public void deleteOrg(Connection connection, Map<String, Object> event) throws SQLException, Exception { hostOrgPersistence.deleteOrg(connection, event); }
    @Override public Result<String> getOrg(int offset, int limit, String domain, String orgName, String orgDesc, String orgOwner) { return hostOrgPersistence.getOrg(offset, limit, domain, orgName, orgDesc, orgOwner); }
    @Override public void createHost(Connection connection, Map<String, Object> event) throws SQLException, Exception { hostOrgPersistence.createHost(connection, event); }
    @Override public void updateHost(Connection connection, Map<String, Object> event) throws SQLException, Exception { hostOrgPersistence.updateHost(connection, event); }
    @Override public void deleteHost(Connection connection, Map<String, Object> event) throws SQLException, Exception { hostOrgPersistence.deleteHost(connection, event); }
    @Override public void switchHost(Connection connection, Map<String, Object> event) throws SQLException, Exception { hostOrgPersistence.switchHost(connection, event); }
    @Override public Result<String> queryHostDomainById(String hostId) { return hostOrgPersistence.queryHostDomainById(hostId); }
    @Override public Result<String> queryHostById(String id) { return hostOrgPersistence.queryHostById(id); }
    @Override public Result<Map<String, Object>> queryHostByOwner(String owner) { return hostOrgPersistence.queryHostByOwner(owner); }
    @Override public Result<String> getHost(int offset, int limit, String hostId, String domain, String subDomain, String hostDesc, String hostOwner) { return hostOrgPersistence.getHost(offset, limit, hostId, domain, subDomain, hostDesc, hostOwner); }
    @Override public Result<String> getHostByDomain(String domain, String subDomain, String hostDesc) { return hostOrgPersistence.getHostByDomain(domain, subDomain, hostDesc); }
    @Override public Result<String> getHostLabel() { return hostOrgPersistence.getHostLabel(); }

    // --- Config ---
    @Override public void createConfig(Connection connection, Map<String, Object> event) throws SQLException, Exception { configPersistence.createConfig(connection, event); }
    @Override public void updateConfig(Connection connection, Map<String, Object> event) throws SQLException, Exception { configPersistence.updateConfig(connection, event); }
    @Override public void deleteConfig(Connection connection, Map<String, Object> event) throws SQLException, Exception { configPersistence.deleteConfig(connection, event); }
    @Override public Result<String> getConfig(int offset, int limit, String configId, String configName, String configPhase, String configType, String light4jVersion, String classPath, String configDesc) { return configPersistence.getConfig(offset, limit, configId, configName, configPhase, configType, light4jVersion, classPath, configDesc); }
    @Override public Result<String> queryConfigById(String configId) { return configPersistence.queryConfigById(configId); }
    @Override public Result<String> getConfigIdLabel() { return configPersistence.getConfigIdLabel(); }
    @Override public Result<String> getConfigIdApiAppLabel(String resourceType) { return configPersistence.getConfigIdApiAppLabel(resourceType); }
    @Override public void createConfigProperty(Connection connection, Map<String, Object> event) throws SQLException, Exception { configPersistence.createConfigProperty(connection, event); }
    @Override public void updateConfigProperty(Connection connection, Map<String, Object> event) throws SQLException, Exception { configPersistence.updateConfigProperty(connection, event); }
    @Override public void deleteConfigProperty(Connection connection, Map<String, Object> event) throws SQLException, Exception { configPersistence.deleteConfigProperty(connection, event); }
    @Override public Result<String> getConfigProperty(int offset, int limit, String configId, String configName, String propertyId, String propertyName, String propertyType, String light4jVersion, Integer displayOrder, Boolean required, String propertyDesc, String propertyValue, String valueType, String resourceType) { return configPersistence.getConfigProperty(offset, limit, configId, configName, propertyId, propertyName, propertyType, light4jVersion, displayOrder, required, propertyDesc, propertyValue, valueType, resourceType); }
    @Override public Result<String> queryConfigPropertyById(String configId) { return configPersistence.queryConfigPropertyById(configId); }
    @Override public Result<String> queryConfigPropertyByPropertyId(String configId, String propertyId) { return configPersistence.queryConfigPropertyByPropertyId(configId, propertyId); }
    @Override public Result<String> getPropertyIdLabel(String configId) { return configPersistence.getPropertyIdLabel(configId); }
    @Override public Result<String> getPropertyIdApiAppLabel(String configId, String resourceType) { return configPersistence.getPropertyIdApiAppLabel(configId, resourceType); }
    @Override public void createConfigEnvironment(Connection connection, Map<String, Object> event) throws SQLException, Exception { configPersistence.createConfigEnvironment(connection, event); }
    @Override public void updateConfigEnvironment(Connection connection, Map<String, Object> event) throws SQLException, Exception { configPersistence.updateConfigEnvironment(connection, event); }
    @Override public void deleteConfigEnvironment(Connection connection, Map<String, Object> event) throws SQLException, Exception { configPersistence.deleteConfigEnvironment(connection, event); }
    @Override public Result<String> getConfigEnvironment(int offset, int limit, String hostId, String environment, String configId, String configName, String propertyId, String propertyName, String propertyValue) { return configPersistence.getConfigEnvironment(offset, limit, hostId, environment, configId, configName, propertyId, propertyName, propertyValue); }
    @Override public void createConfigInstance(Connection connection, Map<String, Object> event) throws SQLException, Exception { configPersistence.createConfigInstance(connection, event); }
    @Override public void updateConfigInstance(Connection connection, Map<String, Object> event) throws SQLException, Exception { configPersistence.updateConfigInstance(connection, event); }
    @Override public void deleteConfigInstance(Connection connection, Map<String, Object> event) throws SQLException, Exception { configPersistence.deleteConfigInstance(connection, event); }
    @Override public Result<String> getConfigInstance(int offset, int limit, String hostId, String instanceId, String instanceName, String configId, String configName, String propertyId, String propertyName, String propertyValue) { return configPersistence.getConfigInstance(offset, limit, hostId, instanceId, instanceName, configId, configName, propertyId, propertyName, propertyValue); }
    @Override public void createConfigInstanceApi(Connection connection, Map<String, Object> event) throws SQLException, Exception { configPersistence.createConfigInstanceApi(connection, event); }
    @Override public void updateConfigInstanceApi(Connection connection, Map<String, Object> event) throws SQLException, Exception { configPersistence.updateConfigInstanceApi(connection, event); }
    @Override public void deleteConfigInstanceApi(Connection connection, Map<String, Object> event) throws SQLException, Exception { configPersistence.deleteConfigInstanceApi(connection, event); }
    @Override public Result<String> getConfigInstanceApi(int offset, int limit, String hostId, String instanceApiId, String instanceId, String instanceName, String apiVersionId, String apiId, String apiVersion, String configId, String configName, String propertyId, String propertyName, String propertyValue) { return configPersistence.getConfigInstanceApi(offset, limit, hostId, instanceApiId, instanceId, instanceName, apiVersionId, apiId, apiVersion, configId, configName, propertyId, propertyName, propertyValue); }
    @Override public void createConfigInstanceApp(Connection connection, Map<String, Object> event) throws SQLException, Exception { configPersistence.createConfigInstanceApp(connection, event); }
    @Override public void updateConfigInstanceApp(Connection connection, Map<String, Object> event) throws SQLException, Exception { configPersistence.updateConfigInstanceApp(connection, event); }
    @Override public void deleteConfigInstanceApp(Connection connection, Map<String, Object> event) throws SQLException, Exception { configPersistence.deleteConfigInstanceApp(connection, event); }
    @Override public Result<String> getConfigInstanceApp(int offset, int limit, String hostId, String instanceAppId, String instanceId, String instanceName, String appId, String appVersion, String configId, String configName, String propertyId, String propertyName, String propertyValue) { return configPersistence.getConfigInstanceApp(offset, limit, hostId, instanceAppId, instanceId, instanceName, appId, appVersion, configId, configName, propertyId, propertyName, propertyValue); }
    @Override public void createConfigInstanceAppApi(Connection connection, Map<String, Object> event) throws SQLException, Exception { configPersistence.createConfigInstanceAppApi(connection, event); }
    @Override public void updateConfigInstanceAppApi(Connection connection, Map<String, Object> event) throws SQLException, Exception { configPersistence.updateConfigInstanceAppApi(connection, event); }
    @Override public void deleteConfigInstanceAppApi(Connection connection, Map<String, Object> event) throws SQLException, Exception { configPersistence.deleteConfigInstanceAppApi(connection, event); }
    @Override public Result<String> getConfigInstanceAppApi(int offset, int limit, String hostId, String instanceAppId, String instanceApiId, String instanceId, String instanceName, String appId, String appVersion, String apiVersionId, String apiId, String apiVersion, String configId, String configName, String propertyId, String propertyName, String propertyValue) { return configPersistence.getConfigInstanceAppApi(offset, limit, hostId, instanceAppId, instanceApiId, instanceId, instanceName, appId, appVersion, apiVersionId, apiId, apiVersion, configId, configName, propertyId, propertyName, propertyValue); }
    @Override public void createConfigDeploymentInstance(Connection connection, Map<String, Object> event) throws SQLException, Exception { configPersistence.createConfigDeploymentInstance(connection, event); }
    @Override public void updateConfigDeploymentInstance(Connection connection, Map<String, Object> event) throws SQLException, Exception { configPersistence.updateConfigDeploymentInstance(connection, event); }
    @Override public void deleteConfigDeploymentInstance(Connection connection, Map<String, Object> event) throws SQLException, Exception { configPersistence.deleteConfigDeploymentInstance(connection, event); }
    @Override public Result<String> getConfigDeploymentInstance(int offset, int limit, String hostId, String deploymentInstanceId, String instanceId, String instanceName, String serviceId, String ipAddress, Integer portNumber, String configId, String configName, String propertyId, String propertyName, String propertyValue) { return configPersistence.getConfigDeploymentInstance(offset, limit, hostId, deploymentInstanceId, instanceId, instanceName, serviceId, ipAddress, portNumber, configId, configName, propertyId, propertyName, propertyValue); }
    @Override public void createConfigProduct(Connection connection, Map<String, Object> event) throws SQLException, Exception { configPersistence.createConfigProduct(connection, event); }
    @Override public void updateConfigProduct(Connection connection, Map<String, Object> event) throws SQLException, Exception { configPersistence.updateConfigProduct(connection, event); }
    @Override public void deleteConfigProduct(Connection connection, Map<String, Object> event) throws SQLException, Exception { configPersistence.deleteConfigProduct(connection, event); }
    @Override public Result<String> getConfigProduct(int offset, int limit, String productId, String configId, String configName, String propertyId, String propertyName, String propertyValue) { return configPersistence.getConfigProduct(offset, limit, productId, configId, configName, propertyId, propertyName, propertyValue); }
    @Override public void createConfigProductVersion(Connection connection, Map<String, Object> event) throws SQLException, Exception { configPersistence.createConfigProductVersion(connection, event); }
    @Override public void updateConfigProductVersion(Connection connection, Map<String, Object> event) throws SQLException, Exception { configPersistence.updateConfigProductVersion(connection, event); }
    @Override public void deleteConfigProductVersion(Connection connection, Map<String, Object> event) throws SQLException, Exception { configPersistence.deleteConfigProductVersion(connection, event); }
    @Override public Result<String> getConfigProductVersion(int offset, int limit, String hostId, String productId, String productVersion, String configId, String configName, String propertyId, String propertyName, String propertyValue) { return configPersistence.getConfigProductVersion(offset, limit, hostId, productId, productVersion, configId, configName, propertyId, propertyName, propertyValue); }
    @Override public void createConfigInstanceFile(Connection connection, Map<String, Object> event) throws SQLException, Exception { configPersistence.createConfigInstanceFile(connection, event); }
    @Override public void updateConfigInstanceFile(Connection connection, Map<String, Object> event) throws SQLException, Exception { configPersistence.updateConfigInstanceFile(connection, event); }
    @Override public void deleteConfigInstanceFile(Connection connection, Map<String, Object> event) throws SQLException, Exception { configPersistence.deleteConfigInstanceFile(connection, event); }
    @Override public Result<String> getConfigInstanceFile(int offset, int limit, String hostId, String instanceFileId, String instanceId, String instanceName, String fileType, String fileName, String fileValue, String fileDesc, String expirationTs) { return configPersistence.getConfigInstanceFile(offset, limit, hostId, instanceFileId, instanceId, instanceName, fileType, fileName, fileValue, fileDesc, expirationTs); }
    @Override public void commitConfigInstance(Connection connection, Map<String, Object> event) throws SQLException, Exception { configPersistence.commitConfigInstance(connection, event); }
    @Override public void rollbackConfigInstance(Connection connection, Map<String, Object> event) throws SQLException, Exception { configPersistence.rollbackConfigInstance(connection, event); }

    // --- InstanceDeployment ---
    @Override public void createInstance(Connection connection, Map<String, Object> event) throws SQLException, Exception { instanceDeploymentPersistence.createInstance(connection, event); }
    @Override public void updateInstance(Connection connection, Map<String, Object> event) throws SQLException, Exception { instanceDeploymentPersistence.updateInstance(connection, event); }
    @Override public void deleteInstance(Connection connection, Map<String, Object> event) throws SQLException, Exception { instanceDeploymentPersistence.deleteInstance(connection, event); }
    @Override public Result<String> getInstance(int offset, int limit, String hostId, String instanceId, String instanceName, String productVersionId, String productId, String productVersion, String serviceId, Boolean current, Boolean readonly, String environment, String serviceDesc, String instanceDesc, String zone,  String region, String lob, String resourceName, String businessName, String envTag, String topicClassification) { return instanceDeploymentPersistence.getInstance(offset, limit, hostId, instanceId, instanceName, productVersionId, productId, productVersion, serviceId, current, readonly, environment, serviceDesc, instanceDesc, zone, region, lob, resourceName, businessName, envTag, topicClassification); }
    @Override public Result<String> getInstanceLabel(String hostId) { return instanceDeploymentPersistence.getInstanceLabel(hostId); }
    @Override public void createInstanceApi(Connection connection, Map<String, Object> event) throws SQLException, Exception { instanceDeploymentPersistence.createInstanceApi(connection, event); }
    @Override public void updateInstanceApi(Connection connection, Map<String, Object> event) throws SQLException, Exception { instanceDeploymentPersistence.updateInstanceApi(connection, event); }
    @Override public void deleteInstanceApi(Connection connection, Map<String, Object> event) throws SQLException, Exception { instanceDeploymentPersistence.deleteInstanceApi(connection, event); }
    @Override public Result<String> getInstanceApi(int offset, int limit, String hostId, String instanceApiId, String instanceId, String instanceName, String productId, String productVersion, String apiVersionId, String apiId, String apiVersion, Boolean active) { return instanceDeploymentPersistence.getInstanceApi(offset, limit, hostId, instanceApiId, instanceId, instanceName, productId, productVersion, apiVersionId, apiId, apiVersion, active); }
    @Override public Result<String> getInstanceApiLabel(String hostId, String instanceId) { return instanceDeploymentPersistence.getInstanceApiLabel(hostId, instanceId); }
    @Override public void createInstanceApiPathPrefix(Connection connection, Map<String, Object> event) throws SQLException, Exception { instanceDeploymentPersistence.createInstanceApiPathPrefix(connection, event); }
    @Override public void updateInstanceApiPathPrefix(Connection connection, Map<String, Object> event) throws SQLException, Exception { instanceDeploymentPersistence.updateInstanceApiPathPrefix(connection, event); }
    @Override public void deleteInstanceApiPathPrefix(Connection connection, Map<String, Object> event) throws SQLException, Exception { instanceDeploymentPersistence.deleteInstanceApiPathPrefix(connection, event); }
    @Override public Result<String> getInstanceApiPathPrefix(int offset, int limit, String hostId, String instanceApiId, String instanceId, String instanceName, String productId, String productVersion, String apiVersionId, String apiId, String apiVersion, String pathPrefix) { return instanceDeploymentPersistence.getInstanceApiPathPrefix(offset, limit, hostId, instanceApiId, instanceId, instanceName, productId, productVersion, apiVersionId, apiId, apiVersion, pathPrefix); }
    @Override public void createInstanceApp(Connection connection, Map<String, Object> event) throws SQLException, Exception { instanceDeploymentPersistence.createInstanceApp(connection, event); }
    @Override public void updateInstanceApp(Connection connection, Map<String, Object> event) throws SQLException, Exception { instanceDeploymentPersistence.updateInstanceApp(connection, event); }
    @Override public void deleteInstanceApp(Connection connection, Map<String, Object> event) throws SQLException, Exception { instanceDeploymentPersistence.deleteInstanceApp(connection, event); }
    @Override public Result<String> getInstanceApp(int offset, int limit, String hostId, String instanceAppId, String instanceId, String instanceName, String productId, String productVersion, String appId, String appVersion, Boolean active) { return instanceDeploymentPersistence.getInstanceApp(offset, limit, hostId, instanceAppId, instanceId, instanceName, productId, productVersion, appId, appVersion, active); }
    @Override public Result<String> getInstanceAppLabel(String hostId, String instanceId) { return instanceDeploymentPersistence.getInstanceAppLabel(hostId, instanceId); }
    @Override public void createInstanceAppApi(Connection connection, Map<String, Object> event) throws SQLException, Exception { instanceDeploymentPersistence.createInstanceAppApi(connection, event); }
    @Override public void updateInstanceAppApi(Connection connection, Map<String, Object> event) throws SQLException, Exception { instanceDeploymentPersistence.updateInstanceAppApi(connection, event); }
    @Override public void deleteInstanceAppApi(Connection connection, Map<String, Object> event) throws SQLException, Exception { instanceDeploymentPersistence.deleteInstanceAppApi(connection, event); }
    @Override public Result<String> getInstanceAppApi(int offset, int limit, String hostId, String instanceAppId, String instanceApiId, String instanceId, String instanceName, String productId, String productVersion, String appId, String appVersion, String apiVersionId, String apiId, String apiVersion, Boolean active) { return instanceDeploymentPersistence.getInstanceAppApi(offset, limit, hostId, instanceAppId, instanceApiId, instanceId, instanceName, productId, productVersion, appId, appVersion, apiVersionId, apiId, apiVersion, active); }
    @Override public void createProduct(Connection connection, Map<String, Object> event) throws SQLException, Exception { instanceDeploymentPersistence.createProduct(connection, event); }
    @Override public void updateProduct(Connection connection, Map<String, Object> event) throws SQLException, Exception { instanceDeploymentPersistence.updateProduct(connection, event); }
    @Override public void deleteProduct(Connection connection, Map<String, Object> event) throws SQLException, Exception { instanceDeploymentPersistence.deleteProduct(connection, event); }
    @Override public Result<String> getProduct(int offset, int limit, String hostId, String productVersionId, String productId, String productVersion, String light4jVersion, Boolean breakCode, Boolean breakConfig, String releaseNote, String versionDesc, String releaseType, Boolean current, String versionStatus) { return instanceDeploymentPersistence.getProduct(offset, limit, hostId, productVersionId, productId, productVersion, light4jVersion, breakCode, breakConfig, releaseNote, versionDesc, releaseType, current, versionStatus); }
    @Override public Result<String> getProductIdLabel(String hostId) { return instanceDeploymentPersistence.getProductIdLabel(hostId); }
    @Override public Result<String> getProductVersionLabel(String hostId, String productId) { return instanceDeploymentPersistence.getProductVersionLabel(hostId, productId); }
    @Override public Result<String> getProductVersionIdLabel(String hostId) { return instanceDeploymentPersistence.getProductVersionIdLabel(hostId); }
    @Override public void createProductVersionEnvironment(Connection connection, Map<String, Object> event) throws SQLException, Exception { instanceDeploymentPersistence.createProductVersionEnvironment(connection, event); }
    @Override public void deleteProductVersionEnvironment(Connection connection, Map<String, Object> event) throws SQLException, Exception { instanceDeploymentPersistence.deleteProductVersionEnvironment(connection, event); }
    @Override public Result<String> getProductVersionEnvironment(int offset, int limit, String hostId, String productVersionId, String productId, String productVersion, String systemEnv, String runtimeEnv) { return instanceDeploymentPersistence.getProductVersionEnvironment(offset, limit, hostId, productVersionId, productId, productVersion, systemEnv, runtimeEnv); }
    @Override public void createProductVersionPipeline(Connection connection, Map<String, Object> event) throws SQLException, Exception { instanceDeploymentPersistence.createProductVersionPipeline(connection, event); }
    @Override public void deleteProductVersionPipeline(Connection connection, Map<String, Object> event) throws SQLException, Exception { instanceDeploymentPersistence.deleteProductVersionPipeline(connection, event); }
    @Override public Result<String> getProductVersionPipeline(int offset, int limit, String hostId, String productVersionId, String productId, String productVersion, String pipelineId, String pipelineName, String pipelineVersion) { return instanceDeploymentPersistence.getProductVersionPipeline(offset, limit, hostId, productVersionId, productId, productVersion, pipelineId, pipelineName, pipelineVersion); }
    @Override public void createProductVersionConfig(Connection connection, Map<String, Object> event) throws SQLException, Exception { instanceDeploymentPersistence.createProductVersionConfig(connection, event); }
    @Override public void deleteProductVersionConfig(Connection connection, Map<String, Object> event) throws SQLException, Exception { instanceDeploymentPersistence.deleteProductVersionConfig(connection, event); }
    @Override public Result<String> getProductVersionConfig(int offset, int limit, List<SortCriterion> sorting, List<FilterCriterion> filtering, String globalFilter, String hostId, String productVersionId, String productId, String productVersion, String configId, String configName) { return instanceDeploymentPersistence.getProductVersionConfig(offset, limit, sorting, filtering, globalFilter, hostId, productVersionId, productId, productVersion, configId, configName); }
    @Override public void createProductVersionConfigProperty(Connection connection, Map<String, Object> event) throws SQLException, Exception { instanceDeploymentPersistence.createProductVersionConfigProperty(connection, event); }
    @Override public void deleteProductVersionConfigProperty(Connection connection, Map<String, Object> event) throws SQLException, Exception { instanceDeploymentPersistence.deleteProductVersionConfigProperty(connection, event); }
    @Override public Result<String> getProductVersionConfigProperty(int offset, int limit, String hostId, String productVersionId, String productId, String productVersion, String configId, String configName, String propertyId, String propertyName) { return instanceDeploymentPersistence.getProductVersionConfigProperty(offset, limit, hostId, productVersionId, productId, productVersion, configId, configName, propertyId, propertyName); }
    @Override public void createPipeline(Connection connection, Map<String, Object> event) throws SQLException, Exception { instanceDeploymentPersistence.createPipeline(connection, event); }
    @Override public void updatePipeline(Connection connection, Map<String, Object> event) throws SQLException, Exception { instanceDeploymentPersistence.updatePipeline(connection, event); }
    @Override public void deletePipeline(Connection connection, Map<String, Object> event) throws SQLException, Exception { instanceDeploymentPersistence.deletePipeline(connection, event); }
    @Override public Result<String> getPipeline(int offset, int limit, String hostId, String pipelineId, String platformId, String platformName, String platformVersion, String pipelineVersion, String pipelineName, Boolean current, String endpoint, String versionStatus, String systemEnv, String runtimeEnv, String requestSchema, String responseSchema) { return instanceDeploymentPersistence.getPipeline(offset, limit, hostId, pipelineId, platformId, platformName, platformVersion, pipelineVersion, pipelineName, current, endpoint, versionStatus, systemEnv, runtimeEnv, requestSchema, responseSchema); }
    @Override public Result<String> getPipelineLabel(String hostId) { return instanceDeploymentPersistence.getPipelineLabel(hostId); }
    @Override public void createInstancePipeline(Connection connection, Map<String, Object> event) throws SQLException, Exception { instanceDeploymentPersistence.createInstancePipeline(connection, event); }
    @Override public void updateInstancePipeline(Connection connection, Map<String, Object> event) throws SQLException, Exception { instanceDeploymentPersistence.updateInstancePipeline(connection, event); }
    @Override public void deleteInstancePipeline(Connection connection, Map<String, Object> event) throws SQLException, Exception { instanceDeploymentPersistence.deleteInstancePipeline(connection, event); }
    @Override public Result<String> getInstancePipeline(int offset, int limit, String hostId, String instanceId, String instanceName, String productId, String productVersion, String pipelineId, String platformName, String platformVersion, String pipelineName, String pipelineVersion) { return instanceDeploymentPersistence.getInstancePipeline(offset, limit, hostId, instanceId, instanceName, productId, productVersion, pipelineId, platformName, platformVersion, pipelineName, pipelineVersion); }
    @Override public void createPlatform(Connection connection, Map<String, Object> event) throws SQLException, Exception { instanceDeploymentPersistence.createPlatform(connection, event); }
    @Override public void updatePlatform(Connection connection, Map<String, Object> event) throws SQLException, Exception { instanceDeploymentPersistence.updatePlatform(connection, event); }
    @Override public void deletePlatform(Connection connection, Map<String, Object> event) throws SQLException, Exception { instanceDeploymentPersistence.deletePlatform(connection, event); }
    @Override public Result<String> getPlatform(int offset, int limit, String hostId, String platformId, String platformName, String platformVersion, String clientType, String clientUrl, String credentials, String proxyUrl, Integer proxyPort, String handlerClass, String consoleUrl, String environment, String zone, String region, String lob) { return instanceDeploymentPersistence.getPlatform(offset, limit, hostId, platformId, platformName, platformVersion, clientType, clientUrl, credentials, proxyUrl, proxyPort, handlerClass, consoleUrl, environment, zone, region, lob); }
    @Override public Result<String> getPlatformLabel(String hostId) { return instanceDeploymentPersistence.getPlatformLabel(hostId); }
    @Override public void createDeploymentInstance(Connection connection, Map<String, Object> event) throws SQLException, Exception { instanceDeploymentPersistence.createDeploymentInstance(connection, event); }
    @Override public void updateDeploymentInstance(Connection connection, Map<String, Object> event) throws SQLException, Exception { instanceDeploymentPersistence.updateDeploymentInstance(connection, event); }
    @Override public void deleteDeploymentInstance(Connection connection, Map<String, Object> event) throws SQLException, Exception { instanceDeploymentPersistence.deleteDeploymentInstance(connection, event); }
    @Override public Result<String> getDeploymentInstance(int offset, int limit, String hostId, String instanceId, String instanceName, String deploymentInstanceId, String serviceId, String ipAddress, Integer portNumber, String systemEnv, String runtimeEnv, String pipelineId, String pipelineName, String pipelineVersion, String deployStatus) { return instanceDeploymentPersistence.getDeploymentInstance(offset, limit, hostId, instanceId, instanceName, deploymentInstanceId, serviceId, ipAddress, portNumber, systemEnv, runtimeEnv, pipelineId, pipelineName, pipelineVersion, deployStatus); }
    @Override public Result<String> getDeploymentInstancePipeline(String hostId, String instanceId, String systemEnv, String runtimeEnv) { return instanceDeploymentPersistence.getDeploymentInstancePipeline(hostId, instanceId, systemEnv, runtimeEnv); }
    @Override public Result<String> getDeploymentInstanceLabel(String hostId, String instanceId) { return instanceDeploymentPersistence.getDeploymentInstanceLabel(hostId, instanceId); }
    @Override public void createDeployment(Connection connection, Map<String, Object> event) throws SQLException, Exception { instanceDeploymentPersistence.createDeployment(connection, event); }
    @Override public void updateDeployment(Connection connection, Map<String, Object> event) throws SQLException, Exception { instanceDeploymentPersistence.updateDeployment(connection, event); }
    @Override public void updateDeploymentJobId(Connection connection, Map<String, Object> event) throws SQLException, Exception { instanceDeploymentPersistence.updateDeploymentJobId(connection, event); }
    @Override public void updateDeploymentStatus(Connection connection, Map<String, Object> event) throws SQLException, Exception { instanceDeploymentPersistence.updateDeploymentStatus(connection, event); }
    @Override public void deleteDeployment(Connection connection, Map<String, Object> event) throws SQLException, Exception { instanceDeploymentPersistence.deleteDeployment(connection, event); }
    @Override public Result<String> getDeployment(int offset, int limit, String hostId, String deploymentId, String deploymentInstanceId, String serviceId, String deploymentStatus, String deploymentType, String platformJobId) { return instanceDeploymentPersistence.getDeployment(offset, limit, hostId, deploymentId, deploymentInstanceId, serviceId, deploymentStatus, deploymentType, platformJobId); }

    // --- AccessControl ---
    @Override public void createRole(Connection connection, Map<String, Object> event) throws SQLException, Exception { accessControlPersistence.createRole(connection, event); }
    @Override public void updateRole(Connection connection, Map<String, Object> event) throws SQLException, Exception { accessControlPersistence.updateRole(connection, event); }
    @Override public void deleteRole(Connection connection, Map<String, Object> event) throws SQLException, Exception { accessControlPersistence.deleteRole(connection, event); }
    @Override public Result<String> queryRole(int offset, int limit, String hostId, String roleId, String roleDesc) { return accessControlPersistence.queryRole(offset, limit, hostId, roleId, roleDesc); }
    @Override public Result<String> queryRoleLabel(String hostId) { return accessControlPersistence.queryRoleLabel(hostId); }
    @Override public void createRolePermission(Connection connection, Map<String, Object> event) throws SQLException, Exception { accessControlPersistence.createRolePermission(connection, event); }
    @Override public void deleteRolePermission(Connection connection, Map<String, Object> event) throws SQLException, Exception { accessControlPersistence.deleteRolePermission(connection, event); }
    @Override public Result<String> queryRolePermission(int offset, int limit, String hostId, String roleId, String apiVersionId, String apiId, String apiVersion, String endpointId, String endpoint) { return accessControlPersistence.queryRolePermission(offset, limit, hostId, roleId, apiVersionId, apiId, apiVersion, endpointId, endpoint); }
    @Override public void createRoleUser(Connection connection, Map<String, Object> event) throws SQLException, Exception { accessControlPersistence.createRoleUser(connection, event); }
    @Override public void updateRoleUser(Connection connection, Map<String, Object> event) throws SQLException, Exception { accessControlPersistence.updateRoleUser(connection, event); }
    @Override public void deleteRoleUser(Connection connection, Map<String, Object> event) throws SQLException, Exception { accessControlPersistence.deleteRoleUser(connection, event); }
    @Override public Result<String> queryRoleUser(int offset, int limit, String hostId, String roleId, String userId, String entityId, String email, String firstName, String lastName, String userType) { return accessControlPersistence.queryRoleUser(offset, limit, hostId, roleId, userId, entityId, email, firstName, lastName, userType); }
    @Override public void createRoleRowFilter(Connection connection, Map<String, Object> event) throws SQLException, Exception { accessControlPersistence.createRoleRowFilter(connection, event); }
    @Override public void updateRoleRowFilter(Connection connection, Map<String, Object> event) throws SQLException, Exception { accessControlPersistence.updateRoleRowFilter(connection, event); }
    @Override public void deleteRoleRowFilter(Connection connection, Map<String, Object> event) throws SQLException, Exception { accessControlPersistence.deleteRoleRowFilter(connection, event); }
    @Override public Result<String> queryRoleRowFilter(int offset, int limit, String hostId, String roleId, String apiVersionId, String apiId, String apiVersion, String endpointId, String endpoint) { return accessControlPersistence.queryRoleRowFilter(offset, limit, hostId, roleId, apiVersionId, apiId, apiVersion, endpointId, endpoint); }
    @Override public void createRoleColFilter(Connection connection, Map<String, Object> event) throws SQLException, Exception { accessControlPersistence.createRoleColFilter(connection, event); }
    @Override public void updateRoleColFilter(Connection connection, Map<String, Object> event) throws SQLException, Exception { accessControlPersistence.updateRoleColFilter(connection, event); }
    @Override public void deleteRoleColFilter(Connection connection, Map<String, Object> event) throws SQLException, Exception { accessControlPersistence.deleteRoleColFilter(connection, event); }
    @Override public Result<String> queryRoleColFilter(int offset, int limit, String hostId, String roleId, String apiId, String apiVersion, String endpoint) { return accessControlPersistence.queryRoleColFilter(offset, limit, hostId, roleId, apiId, apiVersion, endpoint); }
    @Override public void createGroup(Connection connection, Map<String, Object> event) throws SQLException, Exception { accessControlPersistence.createGroup(connection, event); }
    @Override public void updateGroup(Connection connection, Map<String, Object> event) throws SQLException, Exception { accessControlPersistence.updateGroup(connection, event); }
    @Override public void deleteGroup(Connection connection, Map<String, Object> event) throws SQLException, Exception { accessControlPersistence.deleteGroup(connection, event); }
    @Override public Result<String> queryGroup(int offset, int limit, String hostId, String groupId, String groupDesc) { return accessControlPersistence.queryGroup(offset, limit, hostId, groupId, groupDesc); }
    @Override public Result<String> queryGroupLabel(String hostId) { return accessControlPersistence.queryGroupLabel(hostId); }
    @Override public void createGroupPermission(Connection connection, Map<String, Object> event) throws SQLException, Exception { accessControlPersistence.createGroupPermission(connection, event); }
    @Override public void deleteGroupPermission(Connection connection, Map<String, Object> event) throws SQLException, Exception { accessControlPersistence.deleteGroupPermission(connection, event); }
    @Override public Result<String> queryGroupPermission(int offset, int limit, String hostId, String groupId, String apiId, String apiVersion, String endpoint) { return accessControlPersistence.queryGroupPermission(offset, limit, hostId, groupId, apiId, apiVersion, endpoint); }
    @Override public void createGroupUser(Connection connection, Map<String, Object> event) throws SQLException, Exception { accessControlPersistence.createGroupUser(connection, event); }
    @Override public void updateGroupUser(Connection connection, Map<String, Object> event) throws SQLException, Exception { accessControlPersistence.updateGroupUser(connection, event); }
    @Override public void deleteGroupUser(Connection connection, Map<String, Object> event) throws SQLException, Exception { accessControlPersistence.deleteGroupUser(connection, event); }
    @Override public Result<String> queryGroupUser(int offset, int limit, String hostId, String groupId, String userId, String entityId, String email, String firstName, String lastName, String userType) { return accessControlPersistence.queryGroupUser(offset, limit, hostId, groupId, userId, entityId, email, firstName, lastName, userType); }
    @Override public void createGroupRowFilter(Connection connection, Map<String, Object> event) throws SQLException, Exception { accessControlPersistence.createGroupRowFilter(connection, event); }
    @Override public void updateGroupRowFilter(Connection connection, Map<String, Object> event) throws SQLException, Exception { accessControlPersistence.updateGroupRowFilter(connection, event); }
    @Override public void deleteGroupRowFilter(Connection connection, Map<String, Object> event) throws SQLException, Exception { accessControlPersistence.deleteGroupRowFilter(connection, event); }
    @Override public Result<String> queryGroupRowFilter(int offset, int limit, String hostId, String GroupId, String apiId, String apiVersion, String endpoint) { return accessControlPersistence.queryGroupRowFilter(offset, limit, hostId, GroupId, apiId, apiVersion, endpoint); }
    @Override public void createGroupColFilter(Connection connection, Map<String, Object> event) throws SQLException, Exception { accessControlPersistence.createGroupColFilter(connection, event); }
    @Override public void updateGroupColFilter(Connection connection, Map<String, Object> event) throws SQLException, Exception { accessControlPersistence.updateGroupColFilter(connection, event); }
    @Override public void deleteGroupColFilter(Connection connection, Map<String, Object> event) throws SQLException, Exception { accessControlPersistence.deleteGroupColFilter(connection, event); }
    @Override public Result<String> queryGroupColFilter(int offset, int limit, String hostId, String GroupId, String apiId, String apiVersion, String endpoint) { return accessControlPersistence.queryGroupColFilter(offset, limit, hostId, GroupId, apiId, apiVersion, endpoint); }
    @Override public void createPosition(Connection connection, Map<String, Object> event) throws SQLException, Exception { accessControlPersistence.createPosition(connection, event); }
    @Override public void updatePosition(Connection connection, Map<String, Object> event) throws SQLException, Exception { accessControlPersistence.updatePosition(connection, event); }
    @Override public void deletePosition(Connection connection, Map<String, Object> event) throws SQLException, Exception { accessControlPersistence.deletePosition(connection, event); }
    @Override public Result<String> queryPosition(int offset, int limit, String hostId, String positionId, String positionDesc, String inheritToAncestor, String inheritToSibling) { return accessControlPersistence.queryPosition(offset, limit, hostId, positionId, positionDesc, inheritToAncestor, inheritToSibling); }
    @Override public Result<String> queryPositionLabel(String hostId) { return accessControlPersistence.queryPositionLabel(hostId); }
    @Override public void createPositionPermission(Connection connection, Map<String, Object> event) throws SQLException, Exception { accessControlPersistence.createPositionPermission(connection, event); }
    @Override public void deletePositionPermission(Connection connection, Map<String, Object> event) throws SQLException, Exception { accessControlPersistence.deletePositionPermission(connection, event); }
    @Override public Result<String> queryPositionPermission(int offset, int limit, String hostId, String positionId, String inheritToAncestor, String inheritToSibling, String apiId, String apiVersion, String endpoint) { return accessControlPersistence.queryPositionPermission(offset, limit, hostId, positionId, inheritToAncestor, inheritToSibling, apiId, apiVersion, endpoint); }
    @Override public void createPositionUser(Connection connection, Map<String, Object> event) throws SQLException, Exception { accessControlPersistence.createPositionUser(connection, event); }
    @Override public void updatePositionUser(Connection connection, Map<String, Object> event) throws SQLException, Exception { accessControlPersistence.updatePositionUser(connection, event); }
    @Override public void deletePositionUser(Connection connection, Map<String, Object> event) throws SQLException, Exception { accessControlPersistence.deletePositionUser(connection, event); }
    @Override public Result<String> queryPositionUser(int offset, int limit, String hostId, String positionId, String positionType, String inheritToAncestor, String inheritToSibling, String userId, String entityId, String email, String firstName, String lastName, String userType) { return accessControlPersistence.queryPositionUser(offset, limit, hostId, positionId, positionType, inheritToAncestor, inheritToSibling, userId, entityId, email, firstName, lastName, userType); }
    @Override public void createPositionRowFilter(Connection connection, Map<String, Object> event) throws SQLException, Exception { accessControlPersistence.createPositionRowFilter(connection, event); }
    @Override public void updatePositionRowFilter(Connection connection, Map<String, Object> event) throws SQLException, Exception { accessControlPersistence.updatePositionRowFilter(connection, event); }
    @Override public void deletePositionRowFilter(Connection connection, Map<String, Object> event) throws SQLException, Exception { accessControlPersistence.deletePositionRowFilter(connection, event); }
    @Override public Result<String> queryPositionRowFilter(int offset, int limit, String hostId, String PositionId, String apiId, String apiVersion, String endpoint) { return accessControlPersistence.queryPositionRowFilter(offset, limit, hostId, PositionId, apiId, apiVersion, endpoint); }
    @Override public void createPositionColFilter(Connection connection, Map<String, Object> event) throws SQLException, Exception { accessControlPersistence.createPositionColFilter(connection, event); }
    @Override public void updatePositionColFilter(Connection connection, Map<String, Object> event) throws SQLException, Exception { accessControlPersistence.updatePositionColFilter(connection, event); }
    @Override public void deletePositionColFilter(Connection connection, Map<String, Object> event) throws SQLException, Exception { accessControlPersistence.deletePositionColFilter(connection, event); }
    @Override public Result<String> queryPositionColFilter(int offset, int limit, String hostId, String PositionId, String apiId, String apiVersion, String endpoint) { return accessControlPersistence.queryPositionColFilter(offset, limit, hostId, PositionId, apiId, apiVersion, endpoint); }
    @Override public void createAttribute(Connection connection, Map<String, Object> event) throws SQLException, Exception { accessControlPersistence.createAttribute(connection, event); }
    @Override public void updateAttribute(Connection connection, Map<String, Object> event) throws SQLException, Exception { accessControlPersistence.updateAttribute(connection, event); }
    @Override public void deleteAttribute(Connection connection, Map<String, Object> event) throws SQLException, Exception { accessControlPersistence.deleteAttribute(connection, event); }
    @Override public Result<String> queryAttribute(int offset, int limit, String hostId, String attributeId, String attributeType, String attributeDesc) { return accessControlPersistence.queryAttribute(offset, limit, hostId, attributeId, attributeType, attributeDesc); }
    @Override public Result<String> queryAttributeLabel(String hostId) { return accessControlPersistence.queryAttributeLabel(hostId); }
    @Override public void createAttributePermission(Connection connection, Map<String, Object> event) throws SQLException, Exception { accessControlPersistence.createAttributePermission(connection, event); }
    @Override public void updateAttributePermission(Connection connection, Map<String, Object> event) throws SQLException, Exception { accessControlPersistence.updateAttributePermission(connection, event); }
    @Override public void deleteAttributePermission(Connection connection, Map<String, Object> event) throws SQLException, Exception { accessControlPersistence.deleteAttributePermission(connection, event); }
    @Override public Result<String> queryAttributePermission(int offset, int limit, String hostId, String attributeId, String attributeType, String attributeValue, String apiId, String apiVersion, String endpoint) { return accessControlPersistence.queryAttributePermission(offset, limit, hostId, attributeId, attributeType, attributeValue, apiId, apiVersion, endpoint); }
    @Override public void createAttributeUser(Connection connection, Map<String, Object> event) throws SQLException, Exception { accessControlPersistence.createAttributeUser(connection, event); }
    @Override public void updateAttributeUser(Connection connection, Map<String, Object> event) throws SQLException, Exception { accessControlPersistence.updateAttributeUser(connection, event); }
    @Override public void deleteAttributeUser(Connection connection, Map<String, Object> event) throws SQLException, Exception { accessControlPersistence.deleteAttributeUser(connection, event); }
    @Override public Result<String> queryAttributeUser(int offset, int limit, String hostId, String attributeId, String attributeType, String attributeValue, String userId, String entityId, String email, String firstName, String lastName, String userType) { return accessControlPersistence.queryAttributeUser(offset, limit, hostId, attributeId, attributeType, attributeValue, userId, entityId, email, firstName, lastName, userType); }
    @Override public void createAttributeRowFilter(Connection connection, Map<String, Object> event) throws SQLException, Exception { accessControlPersistence.createAttributeRowFilter(connection, event); }
    @Override public void updateAttributeRowFilter(Connection connection, Map<String, Object> event) throws SQLException, Exception { accessControlPersistence.updateAttributeRowFilter(connection, event); }
    @Override public void deleteAttributeRowFilter(Connection connection, Map<String, Object> event) throws SQLException, Exception { accessControlPersistence.deleteAttributeRowFilter(connection, event); }
    @Override public Result<String> queryAttributeRowFilter(int offset, int limit, String hostId, String attributeId, String attributeValue, String apiId, String apiVersion, String endpoint) { return accessControlPersistence.queryAttributeRowFilter(offset, limit, hostId, attributeId, attributeValue, apiId, apiVersion, endpoint); }
    @Override public void createAttributeColFilter(Connection connection, Map<String, Object> event) throws SQLException, Exception { accessControlPersistence.createAttributeColFilter(connection, event); }
    @Override public void updateAttributeColFilter(Connection connection, Map<String, Object> event) throws SQLException, Exception { accessControlPersistence.updateAttributeColFilter(connection, event); }
    @Override public void deleteAttributeColFilter(Connection connection, Map<String, Object> event) throws SQLException, Exception { accessControlPersistence.deleteAttributeColFilter(connection, event); }
    @Override public Result<String> queryAttributeColFilter(int offset, int limit, String hostId, String attributeId, String attributeValue, String apiId, String apiVersion, String endpoint) { return accessControlPersistence.queryAttributeColFilter(offset, limit, hostId, attributeId, attributeValue, apiId, apiVersion, endpoint); }

    // --- Category ---
    @Override public void createCategory(Connection connection, Map<String, Object> event) throws SQLException, Exception { categoryPersistence.createCategory(connection, event); }
    @Override public void updateCategory(Connection connection, Map<String, Object> event) throws SQLException, Exception { categoryPersistence.updateCategory(connection, event); }
    @Override public void deleteCategory(Connection connection, Map<String, Object> event) throws SQLException, Exception { categoryPersistence.deleteCategory(connection, event); }
    @Override public Result<String> getCategory(int offset, int limit, String hostId, String categoryId, String entityType, String categoryName, String categoryDesc, String parentCategoryId, String parentCategoryName, Integer sortOrder) { return categoryPersistence.getCategory(offset, limit, hostId, categoryId, entityType, categoryName, categoryDesc, parentCategoryId, parentCategoryName, sortOrder); }
    @Override public Result<String> getCategoryLabel(String hostId) { return categoryPersistence.getCategoryLabel(hostId); }
    @Override public Result<String> getCategoryById(String categoryId) { return categoryPersistence.getCategoryById(categoryId); }
    @Override public Result<String> getCategoryByName(String hostId, String categoryName) { return categoryPersistence.getCategoryByName(hostId, categoryName); }
    @Override public Result<String> getCategoryByType(String hostId, String entityType) { return categoryPersistence.getCategoryByType(hostId, entityType); }
    @Override public Result<String> getCategoryTree(String hostId, String entityType) { return categoryPersistence.getCategoryTree(hostId, entityType); }

    // --- Tag ---
    @Override public void createTag(Connection connection, Map<String, Object> event) throws SQLException, Exception { tagPersistence.createTag(connection, event); }
    @Override public void updateTag(Connection connection, Map<String, Object> event) throws SQLException, Exception { tagPersistence.updateTag(connection, event); }
    @Override public void deleteTag(Connection connection, Map<String, Object> event) throws SQLException, Exception { tagPersistence.deleteTag(connection, event); }
    @Override public Result<String> getTag(int offset, int limit, String hostId, String tagId, String entityType, String tagName, String tagDesc) { return tagPersistence.getTag(offset, limit, hostId, tagId, entityType, tagName, tagDesc); }
    @Override public Result<String> getTagLabel(String hostId) { return tagPersistence.getTagLabel(hostId); }
    @Override public Result<String> getTagById(String tagId) { return tagPersistence.getTagById(tagId); }
    @Override public Result<String> getTagByName(String hostId, String tagName) { return tagPersistence.getTagByName(hostId, tagName); }
    @Override public Result<String> getTagByType(String hostId, String entityType) { return tagPersistence.getTagByType(hostId, entityType); }

    // --- Schema ---
    @Override public void createSchema(Connection connection, Map<String, Object> event) throws SQLException, Exception { schemaPersistence.createSchema(connection, event); }
    @Override public void updateSchema(Connection connection, Map<String, Object> event) throws SQLException, Exception { schemaPersistence.updateSchema(connection, event); }
    @Override public void deleteSchema(Connection connection, Map<String, Object> event) throws SQLException, Exception { schemaPersistence.deleteSchema(connection, event); }
    @Override public Result<String> getSchema(int offset, int limit, String hostId, String schemaId, String schemaVersion, String schemaType, String specVersion, String schemaSource, String schemaName, String schemaDesc, String schemaBody, String schemaOwner, String schemaStatus, String example, String commentStatus) { return schemaPersistence.getSchema(offset, limit, hostId, schemaId, schemaVersion, schemaType, specVersion, schemaSource, schemaName, schemaDesc, schemaBody, schemaOwner, schemaStatus, example, commentStatus); }
    @Override public Result<String> getSchemaLabel(String hostId) { return schemaPersistence.getSchemaLabel(hostId); }
    @Override public Result<String> getSchemaById(String schemaId) { return schemaPersistence.getSchemaById(schemaId); }
    @Override public Result<String> getSchemaByCategoryId(String categoryId) { return schemaPersistence.getSchemaByCategoryId(categoryId); }
    @Override public Result<String> getSchemaByTagId(String tagId) { return schemaPersistence.getSchemaByTagId(tagId); }

    // --- Schedule ---
    @Override public void createSchedule(Connection connection, Map<String, Object> event) throws SQLException, Exception { schedulePersistence.createSchedule(connection, event); }
    @Override public void updateSchedule(Connection connection, Map<String, Object> event) throws SQLException, Exception { schedulePersistence.updateSchedule(connection, event); }
    @Override public void deleteSchedule(Connection connection, Map<String, Object> event) throws SQLException, Exception { schedulePersistence.deleteSchedule(connection, event); }
    @Override public Result<String> getSchedule(int offset, int limit, String hostId, String scheduleId, String scheduleName, String frequencyUnit, Integer frequencyTime, String startTs, String eventTopic, String eventType, String eventData) { return schedulePersistence.getSchedule(offset, limit, hostId, scheduleId, scheduleName, frequencyUnit, frequencyTime, startTs, eventTopic, eventType, eventData); }
    @Override public Result<String> getScheduleLabel(String hostId) { return schedulePersistence.getScheduleLabel(hostId); }
    @Override public Result<String> getScheduleById(String scheduleId) { return schedulePersistence.getScheduleById(scheduleId); }

    // --- Rule ---
    @Override public void createRule(Connection connection, Map<String, Object> event) throws SQLException, Exception { rulePersistence.createRule(connection, event); }
    @Override public void updateRule(Connection connection, Map<String, Object> event) throws SQLException, Exception { rulePersistence.updateRule(connection, event); }
    @Override public void deleteRule(Connection connection, Map<String, Object> event) throws SQLException, Exception { rulePersistence.deleteRule(connection, event); }
    @Override public Result<List<Map<String, Object>>> queryRuleByGroup(String groupId) { return rulePersistence.queryRuleByGroup(groupId); }
    @Override public Result<String> queryRule(int offset, int limit, String hostId, String ruleId, String ruleName,
                                              String ruleVersion, String ruleType, String ruleGroup, String ruleDesc,
                                              String ruleBody, String ruleOwner) { return rulePersistence.queryRule(offset, limit, hostId, ruleId, ruleName, ruleVersion, ruleType, ruleGroup, ruleDesc, ruleBody, ruleOwner); }
    @Override public Result<Map<String, Object>> queryRuleById(String ruleId) { return rulePersistence.queryRuleById(ruleId); }
    @Override public Result<String> queryRuleByType(String ruleType) { return rulePersistence.queryRuleByType(ruleType); }
    @Override public Result<List<Map<String, Object>>> queryRuleByHostApiId(String hostId, String apiId, String apiVersion) { return rulePersistence.queryRuleByHostApiId(hostId, apiId, apiVersion); }

    // --- Event ---
    @Override public Result<String> insertEventStore(CloudEvent[] events) { return eventPersistence.insertEventStore(events); }

    // --- Product / Instance Applicable Properties
    @Override
    public Result<String> getApplicableConfigPropertiesForInstance(int offset, int limit, String hostId, String instanceId, Set<String> resourceTypes, Set<String> configTypes, Set<String> propertyTypes) { return configPersistence.getApplicableConfigPropertiesForInstance(offset, limit, hostId, instanceId, resourceTypes, configTypes, propertyTypes); }
    @Override
    public Result<String> getApplicableConfigPropertiesForInstanceApi(int offset, int limit, String hostId, String instanceApiId) { return configPersistence.getApplicableConfigPropertiesForInstanceApi(offset, limit, hostId, instanceApiId); }
    @Override
    public Result<String> getApplicableConfigPropertiesForInstanceApp(int offset, int limit, String hostId, String instanceAppId) { return configPersistence.getApplicableConfigPropertiesForInstanceApp(offset, limit, hostId, instanceAppId); }
    @Override
    public Result<String> getApplicableConfigPropertiesForInstanceAppApi(int offset, int limit, String hostId, String instanceAppId, String instanceApiId) { return configPersistence.getApplicableConfigPropertiesForInstanceAppApi(offset, limit, hostId, instanceAppId, instanceApiId); }
}
