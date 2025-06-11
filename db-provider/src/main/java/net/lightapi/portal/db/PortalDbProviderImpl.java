package net.lightapi.portal.db;

import com.networknt.monad.Result;
import net.lightapi.portal.db.persistence.*;
import net.lightapi.portal.db.util.NotificationService;
import net.lightapi.portal.db.util.NotificationServiceImpl;
import net.lightapi.portal.validation.FilterCriterion;
import net.lightapi.portal.validation.SortCriterion;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Timestamp;
import java.util.List;
import java.util.Map;

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
    }

    // --- Reference ---
    @Override public Result<String> createRefTable(Map<String, Object> event) { return referenceDataPersistence.createRefTable(event); }
    @Override public Result<String> updateRefTable(Map<String, Object> event) { return referenceDataPersistence.updateRefTable(event); }
    @Override public Result<String> deleteRefTable(Map<String, Object> event) { return referenceDataPersistence.deleteRefTable(event); }
    @Override public Result<String> getRefTable(int offset, int limit, String hostId, String tableId, String tableName, String tableDesc, Boolean active, Boolean editable) { return referenceDataPersistence.getRefTable(offset, limit, hostId, tableId, tableName, tableDesc, active, editable); }
    @Override public Result<String> getRefTableById(String tableId) { return referenceDataPersistence.getRefTableById(tableId); }
    @Override public Result<String> getRefTableLabel(String hostId) { return referenceDataPersistence.getRefTableLabel(hostId); }
    @Override public Result<String> createRefValue(Map<String, Object> event) { return referenceDataPersistence.createRefValue(event); }
    @Override public Result<String> updateRefValue(Map<String, Object> event) { return referenceDataPersistence.updateRefValue(event); }
    @Override public Result<String> deleteRefValue(Map<String, Object> event) { return referenceDataPersistence.deleteRefValue(event); }
    @Override public Result<String> getRefValue(int offset, int limit, String valueId, String tableId, String valueCode, String valueDesc, Integer displayOrder, Boolean active) { return referenceDataPersistence.getRefValue(offset, limit, valueId, tableId, valueCode, valueDesc, displayOrder, active); }
    @Override public Result<String> getRefValueById(String valueId) { return referenceDataPersistence.getRefValueById(valueId); }
    @Override public Result<String> getRefValueLabel(String tableId) { return referenceDataPersistence.getRefValueLabel(tableId); }
    @Override public Result<String> createRefLocale(Map<String, Object> event) { return referenceDataPersistence.createRefLocale(event); }
    @Override public Result<String> updateRefLocale(Map<String, Object> event) { return referenceDataPersistence.updateRefLocale(event); }
    @Override public Result<String> deleteRefLocale(Map<String, Object> event) { return referenceDataPersistence.deleteRefLocale(event); }
    @Override public Result<String> getRefLocale(int offset, int limit, String valueId, String valueCode, String valueDesc, String language, String valueLabel) { return referenceDataPersistence.getRefLocale(offset, limit, valueId, valueCode, valueDesc, language, valueLabel); }
    @Override public Result<String> createRefRelationType(Map<String, Object> event) { return referenceDataPersistence.createRefRelationType(event); }
    @Override public Result<String> updateRefRelationType(Map<String, Object> event) { return referenceDataPersistence.updateRefRelationType(event); }
    @Override public Result<String> deleteRefRelationType(Map<String, Object> event) { return referenceDataPersistence.deleteRefRelationType(event); }
    @Override public Result<String> getRefRelationType(int offset, int limit, String relationId, String relationName, String relationDesc) { return referenceDataPersistence.getRefRelationType(offset, limit, relationId, relationName, relationDesc); }
    @Override public Result<String> createRefRelation(Map<String, Object> event) { return referenceDataPersistence.createRefRelation(event); }
    @Override public Result<String> updateRefRelation(Map<String, Object> event) { return referenceDataPersistence.updateRefRelation(event); }
    @Override public Result<String> deleteRefRelation(Map<String, Object> event) { return referenceDataPersistence.deleteRefRelation(event); }
    @Override public Result<String> getRefRelation(int offset, int limit, String relationId, String relationName, String valueIdFrom, String valueCodeFrom, String valueIdTo, String valueCodeTo, Boolean active) { return referenceDataPersistence.getRefRelation(offset, limit, relationId, relationName, valueIdFrom, valueCodeFrom, valueIdTo, valueCodeTo, active); }


    // --- User ---
    @Override public Result<String> loginUserByEmail(String email) { return userPersistence.loginUserByEmail(email); }
    @Override public Result<String> queryUserByEmail(String email) { return userPersistence.queryUserByEmail(email); }
    @Override public Result<String> queryUserById(String userId) { return userPersistence.queryUserById(userId); }
    @Override public Result<String> queryUserByTypeEntityId(String userType, String entityId) { return userPersistence.queryUserByTypeEntityId(userType, entityId); }
    @Override public Result<String> queryUserByWallet(String cryptoType, String cryptoAddress) { return userPersistence.queryUserByWallet(cryptoType, cryptoAddress); }
    @Override public Result<String> queryUserByHostId(int offset, int limit, String hostId, String email, String language, String userType, String entityId, String referralId, String managerId, String firstName, String lastName, String phoneNumber, String gender, String birthday, String country, String province, String city, String address, String postCode, Boolean verified, Boolean locked) { return userPersistence.queryUserByHostId(offset, limit, hostId, email, language, userType, entityId, referralId, managerId, firstName, lastName, phoneNumber, gender, birthday, country, province, city, address, postCode, verified, locked); }
    @Override public Result<String> createUser(Map<String, Object> event) { return userPersistence.createUser(event); }
    @Override public Result<Long> queryNonceByUserId(String userId) { return userPersistence.queryNonceByUserId(userId); }
    @Override public Result<String> confirmUser(Map<String, Object> event) { return userPersistence.confirmUser(event); }
    @Override public Result<String> verifyUser(Map<String, Object> event) { return userPersistence.verifyUser(event); }
    @Override public Result<String> createSocialUser(Map<String, Object> event) { return userPersistence.createSocialUser(event); }
    @Override public Result<String> updateUser(Map<String, Object> event) { return userPersistence.updateUser(event); }
    @Override public Result<String> deleteUser(Map<String, Object> event) { return userPersistence.deleteUser(event); }
    @Override public Result<String> forgetPassword(Map<String, Object> event) { return userPersistence.forgetPassword(event); }
    @Override public Result<String> resetPassword(Map<String, Object> event) { return userPersistence.resetPassword(event); }
    @Override public Result<String> changePassword(Map<String, Object> event) { return userPersistence.changePassword(event); }
    @Override public Result<String> queryUserLabel(String hostId) { return userPersistence.queryUserLabel(hostId); }
    @Override public Result<String> queryEmailByWallet(String cryptoType, String cryptoAddress) { return userPersistence.queryEmailByWallet(cryptoType, cryptoAddress); }
    @Override public Result<String> sendPrivateMessage(Map<String, Object> event) { return userPersistence.sendPrivateMessage(event); }
    @Override public Result<String> updatePayment(Map<String, Object> event) { return userPersistence.updatePayment(event); }
    @Override public Result<String> deletePayment(Map<String, Object> event) { return userPersistence.deletePayment(event); }
    @Override public Result<String> createOrder(Map<String, Object> event) { return userPersistence.createOrder(event); }
    @Override public Result<String> cancelOrder(Map<String, Object> event) { return userPersistence.cancelOrder(event); }
    @Override public Result<String> deliverOrder(Map<String, Object> event) { return userPersistence.deliverOrder(event); }
    @Override public Result<String> queryNotification(int offset, int limit, String hostId, String userId, Long nonce, String eventClass, Boolean successFlag, Timestamp processTs, String eventJson, String error) { return notificationDataPersistence.queryNotification(offset, limit, hostId, userId, nonce, eventClass, successFlag, processTs, eventJson, error); }

    // --- Auth ---
    @Override public Result<String> createApp(Map<String, Object> event) { return authPersistence.createApp(event); }
    @Override public Result<String> updateApp(Map<String, Object> event) { return authPersistence.updateApp(event); }
    @Override public Result<String> deleteApp(Map<String, Object> event) { return authPersistence.deleteApp(event); }
    @Override public Result<String> queryApp(int offset, int limit, String hostId, String appId, String appName, String appDesc, Boolean isKafkaApp, String operationOwner, String deliveryOwner) { return authPersistence.queryApp(offset, limit, hostId, appId, appName, appDesc, isKafkaApp, operationOwner, deliveryOwner); }
    @Override public Result<String> getAppIdLabel(String hostId) { return authPersistence.getAppIdLabel(hostId); }
    @Override public Result<String> createClient(Map<String, Object> event) { return authPersistence.createClient(event); }
    @Override public Result<String> updateClient(Map<String, Object> event) { return authPersistence.updateClient(event); }
    @Override public Result<String> deleteClient(Map<String, Object> event) { return authPersistence.deleteClient(event); }
    @Override public Result<String> queryClient(int offset, int limit, String hostId, String appId, String apiId, String clientId, String clientName, String clientType, String clientProfile, String clientScope, String customClaim, String redirectUri, String authenticateClass, String deRefClientId) { return authPersistence.queryClient(offset, limit, hostId, appId, apiId, clientId, clientName, clientType, clientProfile, clientScope, customClaim, redirectUri, authenticateClass, deRefClientId); }
    @Override public Result<String> queryClientByClientId(String clientId) { return authPersistence.queryClientByClientId(clientId); }
    @Override public Result<String> queryClientByProviderClientId(String providerId, String clientId) { return authPersistence.queryClientByProviderClientId(providerId, clientId); }
    @Override public Result<String> queryClientByHostAppId(String host_id, String appId) { return authPersistence.queryClientByHostAppId(host_id, appId); }
    @Override public Result<String> createAuthProvider(Map<String, Object> event) { return authPersistence.createAuthProvider(event); }
    @Override public Result<String> updateAuthProvider(Map<String, Object> event) { return authPersistence.updateAuthProvider(event); }
    @Override public Result<String> deleteAuthProvider(Map<String, Object> event) { return authPersistence.deleteAuthProvider(event); }
    @Override public Result<String> rotateAuthProvider(Map<String, Object> event) { return authPersistence.rotateAuthProvider(event); }
    @Override public Result<Map<String, Object>> queryProviderById(String providerId) { return authPersistence.queryProviderById(providerId); }
    @Override public Result<String> queryProvider(int offset, int limit, String hostId, String providerId, String providerName, String providerDesc, String operationOwner, String deliveryOwner, String jwk, String updateUser, Timestamp updateTs) { return authPersistence.queryProvider(offset, limit, hostId, providerId, providerName, providerDesc, operationOwner, deliveryOwner, jwk, updateUser, updateTs); }
    @Override public Result<String> queryProviderKey(String providerId) { return authPersistence.queryProviderKey(providerId); }
    @Override public Result<Map<String, Object>> queryCurrentProviderKey(String providerId) { return authPersistence.queryCurrentProviderKey(providerId); }
    @Override public Result<Map<String, Object>> queryLongLiveProviderKey(String providerId) { return authPersistence.queryLongLiveProviderKey(providerId); }
    @Override public Result<String> createAuthCode(Map<String, Object> event) { return authPersistence.createAuthCode(event); }
    @Override public Result<String> deleteAuthCode(Map<String, Object> event) { return authPersistence.deleteAuthCode(event); }
    @Override public Result<String> queryAuthCode(String authCode) { return authPersistence.queryAuthCode(authCode); }
    @Override public Result<String> listAuthCode(int offset, int limit, String hostId, String authCode, String userId, String entityId, String userType, String email, String roles, String groups, String positions, String attributes, String redirectUri, String scope, String remember, String codeChallenge, String challengeMethod, String updateUser, Timestamp updateTs) { return authPersistence.listAuthCode(offset, limit, hostId, authCode, userId, entityId, userType, email, roles, groups, positions, attributes, redirectUri, scope, remember, codeChallenge, challengeMethod, updateUser, updateTs); }
    @Override public Result<String> createRefreshToken(Map<String, Object> event) { return authPersistence.createRefreshToken(event); }
    @Override public Result<String> deleteRefreshToken(Map<String, Object> event) { return authPersistence.deleteRefreshToken(event); }
    @Override public Result<String> listRefreshToken(int offset, int limit, String refreshToken, String hostId, String userId, String entityId, String email, String firstName, String lastName, String clientId, String appId, String appName, String scope, String userType, String roles, String groups, String positions, String attributes, String csrf, String customClaim, String updateUser, Timestamp updateTs) { return authPersistence.listRefreshToken(offset, limit, refreshToken, hostId, userId, entityId, email, firstName, lastName, clientId, appId, appName, scope, userType, roles, groups, positions, attributes, csrf, customClaim, updateUser, updateTs); }
    @Override public Result<String> queryRefreshToken(String refreshToken) { return authPersistence.queryRefreshToken(refreshToken); }

    // --- ApiService ---
    @Override public Result<String> createService(Map<String, Object> event) { return apiServicePersistence.createService(event); }
    @Override public Result<String> updateService(Map<String, Object> event) { return apiServicePersistence.updateService(event); }
    @Override public Result<String> deleteService(Map<String, Object> event) { return apiServicePersistence.deleteService(event); }
    @Override public Result<String> queryService(int offset, int limit, String hostId, String apiId, String apiName, String apiDesc, String operationOwner, String deliveryOwner, String region, String businessGroup, String lob, String platform, String capability, String gitRepo, String apiTags, String apiStatus) { return apiServicePersistence.queryService(offset, limit, hostId, apiId, apiName, apiDesc, operationOwner, deliveryOwner, region, businessGroup, lob, platform, capability, gitRepo, apiTags, apiStatus); }
    @Override public Result<String> queryApiLabel(String hostId) { return apiServicePersistence.queryApiLabel(hostId); }
    @Override public Result<String> createServiceVersion(Map<String, Object> event, List<Map<String, Object>> endpoints) { return apiServicePersistence.createServiceVersion(event, endpoints); }
    @Override public Result<String> updateServiceVersion(Map<String, Object> event, List<Map<String, Object>> endpoints) { return apiServicePersistence.updateServiceVersion(event, endpoints); }
    @Override public Result<String> deleteServiceVersion(Map<String, Object> event) { return apiServicePersistence.deleteServiceVersion(event); }
    @Override public Result<String> queryServiceVersion(String hostId, String apiId) { return apiServicePersistence.queryServiceVersion(hostId, apiId); }
    @Override public Result<String> getApiVersionIdLabel(String hostId) { return apiServicePersistence.getApiVersionIdLabel(hostId); }
    @Override public Result<String> queryApiVersionLabel(String hostId, String apiId) { return apiServicePersistence.queryApiVersionLabel(hostId, apiId); }
    @Override public Result<String> updateServiceSpec(Map<String, Object> event, List<Map<String, Object>> endpoints) { return apiServicePersistence.updateServiceSpec(event, endpoints); }
    @Override public Result<String> queryServiceEndpoint(int offset, int limit, String hostId, String apiVersionId, String apiId, String apiVersion, String endpoint, String method, String path, String desc) { return apiServicePersistence.queryServiceEndpoint(offset, limit, hostId, apiVersionId, apiId, apiVersion, endpoint, method, path, desc); }
    @Override public Result<String> queryEndpointLabel(String hostId, String apiId, String apiVersion) { return apiServicePersistence.queryEndpointLabel(hostId, apiId, apiVersion); }
    @Override public Result<String> queryEndpointScope(String hostId, String endpointId) { return apiServicePersistence.queryEndpointScope(hostId, endpointId); }
    @Override public Result<String> createEndpointRule(Map<String, Object> event) { return apiServicePersistence.createEndpointRule(event); }
    @Override public Result<String> deleteEndpointRule(Map<String, Object> event) { return apiServicePersistence.deleteEndpointRule(event); }
    @Override public Result<String> queryEndpointRule(String hostId, String apiId, String apiVersion, String endpoint) { return apiServicePersistence.queryEndpointRule(hostId, apiId, apiVersion, endpoint); }
    @Override public Result<String> queryServiceRule(String hostId, String apiId, String apiVersion) { return apiServicePersistence.queryServiceRule(hostId, apiId, apiVersion); }
    @Override public Result<String> queryServicePermission(String hostId, String apiId, String apiVersion) { return apiServicePersistence.queryServicePermission(hostId, apiId, apiVersion); }
    @Override public Result<List<String>> queryServiceFilter(String hostId, String apiId, String apiVersion) { return apiServicePersistence.queryServiceFilter(hostId, apiId, apiVersion); }
    @Override public Result<String> getServiceIdLabel(String hostId) { return apiServicePersistence.getServiceIdLabel(hostId); }

    // --- HostOrg ---
    @Override public Result<String> createOrg(Map<String, Object> event) { return hostOrgPersistence.createOrg(event); }
    @Override public Result<String> updateOrg(Map<String, Object> event) { return hostOrgPersistence.updateOrg(event); }
    @Override public Result<String> deleteOrg(Map<String, Object> event) { return hostOrgPersistence.deleteOrg(event); }
    @Override public Result<String> getOrg(int offset, int limit, String domain, String orgName, String orgDesc, String orgOwner) { return hostOrgPersistence.getOrg(offset, limit, domain, orgName, orgDesc, orgOwner); }
    @Override public Result<String> createHost(Map<String, Object> event) { return hostOrgPersistence.createHost(event); }
    @Override public Result<String> updateHost(Map<String, Object> event) { return hostOrgPersistence.updateHost(event); }
    @Override public Result<String> deleteHost(Map<String, Object> event) { return hostOrgPersistence.deleteHost(event); }
    @Override public Result<String> switchHost(Map<String, Object> event) { return hostOrgPersistence.switchHost(event); }
    @Override public Result<String> queryHostDomainById(String hostId) { return hostOrgPersistence.queryHostDomainById(hostId); }
    @Override public Result<String> queryHostById(String id) { return hostOrgPersistence.queryHostById(id); }
    @Override public Result<Map<String, Object>> queryHostByOwner(String owner) { return hostOrgPersistence.queryHostByOwner(owner); }
    @Override public Result<String> getHost(int offset, int limit, String hostId, String domain, String subDomain, String hostDesc, String hostOwner) { return hostOrgPersistence.getHost(offset, limit, hostId, domain, subDomain, hostDesc, hostOwner); }
    @Override public Result<String> getHostByDomain(String domain, String subDomain, String hostDesc) { return hostOrgPersistence.getHostByDomain(domain, subDomain, hostDesc); }
    @Override public Result<String> getHostLabel() { return hostOrgPersistence.getHostLabel(); }

    // --- Config ---
    @Override public Result<String> createConfig(Map<String, Object> event) { return configPersistence.createConfig(event); }
    @Override public Result<String> updateConfig(Map<String, Object> event) { return configPersistence.updateConfig(event); }
    @Override public Result<String> deleteConfig(Map<String, Object> event) { return configPersistence.deleteConfig(event); }
    @Override public Result<String> getConfig(int offset, int limit, String configId, String configName, String configPhase, String configType, String light4jVersion, String classPath, String configDesc) { return configPersistence.getConfig(offset, limit, configId, configName, configPhase, configType, light4jVersion, classPath, configDesc); }
    @Override public Result<String> queryConfigById(String configId) { return configPersistence.queryConfigById(configId); }
    @Override public Result<String> getConfigIdLabel() { return configPersistence.getConfigIdLabel(); }
    @Override public Result<String> getConfigIdApiAppLabel(String resourceType) { return configPersistence.getConfigIdApiAppLabel(resourceType); }
    @Override public Result<String> createConfigProperty(Map<String, Object> event) { return configPersistence.createConfigProperty(event); }
    @Override public Result<String> updateConfigProperty(Map<String, Object> event) { return configPersistence.updateConfigProperty(event); }
    @Override public Result<String> deleteConfigProperty(Map<String, Object> event) { return configPersistence.deleteConfigProperty(event); }
    @Override public Result<String> getConfigProperty(int offset, int limit, String configId, String configName, String propertyId, String propertyName, String propertyType, String light4jVersion, Integer displayOrder, Boolean required, String propertyDesc, String propertyValue, String valueType, String resourceType) { return configPersistence.getConfigProperty(offset, limit, configId, configName, propertyId, propertyName, propertyType, light4jVersion, displayOrder, required, propertyDesc, propertyValue, valueType, resourceType); }
    @Override public Result<String> queryConfigPropertyById(String configId) { return configPersistence.queryConfigPropertyById(configId); }
    @Override public Result<String> queryConfigPropertyByPropertyId(String configId, String propertyId) { return configPersistence.queryConfigPropertyByPropertyId(configId, propertyId); }
    @Override public Result<String> getPropertyIdLabel(String configId) { return configPersistence.getPropertyIdLabel(configId); }
    @Override public Result<String> getPropertyIdApiAppLabel(String configId, String resourceType) { return configPersistence.getPropertyIdApiAppLabel(configId, resourceType); }
    @Override public Result<String> createConfigEnvironment(Map<String, Object> event) { return configPersistence.createConfigEnvironment(event); }
    @Override public Result<String> updateConfigEnvironment(Map<String, Object> event) { return configPersistence.updateConfigEnvironment(event); }
    @Override public Result<String> deleteConfigEnvironment(Map<String, Object> event) { return configPersistence.deleteConfigEnvironment(event); }
    @Override public Result<String> getConfigEnvironment(int offset, int limit, String hostId, String environment, String configId, String configName, String propertyId, String propertyName, String propertyValue) { return configPersistence.getConfigEnvironment(offset, limit, hostId, environment, configId, configName, propertyId, propertyName, propertyValue); }
    @Override public Result<String> createConfigInstance(Map<String, Object> event) { return configPersistence.createConfigInstance(event); }
    @Override public Result<String> updateConfigInstance(Map<String, Object> event) { return configPersistence.updateConfigInstance(event); }
    @Override public Result<String> deleteConfigInstance(Map<String, Object> event) { return configPersistence.deleteConfigInstance(event); }
    @Override public Result<String> getConfigInstance(int offset, int limit, String hostId, String instanceId, String instanceName, String configId, String configName, String propertyId, String propertyName, String propertyValue) { return configPersistence.getConfigInstance(offset, limit, hostId, instanceId, instanceName, configId, configName, propertyId, propertyName, propertyValue); }
    @Override public Result<String> createConfigInstanceApi(Map<String, Object> event) { return configPersistence.createConfigInstanceApi(event); }
    @Override public Result<String> updateConfigInstanceApi(Map<String, Object> event) { return configPersistence.updateConfigInstanceApi(event); }
    @Override public Result<String> deleteConfigInstanceApi(Map<String, Object> event) { return configPersistence.deleteConfigInstanceApi(event); }
    @Override public Result<String> getConfigInstanceApi(int offset, int limit, String hostId, String instanceApiId, String instanceId, String instanceName, String apiVersionId, String apiId, String apiVersion, String configId, String configName, String propertyId, String propertyName, String propertyValue) { return configPersistence.getConfigInstanceApi(offset, limit, hostId, instanceApiId, instanceId, instanceName, apiVersionId, apiId, apiVersion, configId, configName, propertyId, propertyName, propertyValue); }
    @Override public Result<String> createConfigInstanceApp(Map<String, Object> event) { return configPersistence.createConfigInstanceApp(event); }
    @Override public Result<String> updateConfigInstanceApp(Map<String, Object> event) { return configPersistence.updateConfigInstanceApp(event); }
    @Override public Result<String> deleteConfigInstanceApp(Map<String, Object> event) { return configPersistence.deleteConfigInstanceApp(event); }
    @Override public Result<String> getConfigInstanceApp(int offset, int limit, String hostId, String instanceAppId, String instanceId, String instanceName, String appId, String appVersion, String configId, String configName, String propertyId, String propertyName, String propertyValue) { return configPersistence.getConfigInstanceApp(offset, limit, hostId, instanceAppId, instanceId, instanceName, appId, appVersion, configId, configName, propertyId, propertyName, propertyValue); }
    @Override public Result<String> createConfigInstanceAppApi(Map<String, Object> event) { return configPersistence.createConfigInstanceAppApi(event); }
    @Override public Result<String> updateConfigInstanceAppApi(Map<String, Object> event) { return configPersistence.updateConfigInstanceAppApi(event); }
    @Override public Result<String> deleteConfigInstanceAppApi(Map<String, Object> event) { return configPersistence.deleteConfigInstanceAppApi(event); }
    @Override public Result<String> getConfigInstanceAppApi(int offset, int limit, String hostId, String instanceAppId, String instanceApiId, String instanceId, String instanceName, String appId, String appVersion, String apiVersionId, String apiId, String apiVersion, String configId, String configName, String propertyId, String propertyName, String propertyValue) { return configPersistence.getConfigInstanceAppApi(offset, limit, hostId, instanceAppId, instanceApiId, instanceId, instanceName, appId, appVersion, apiVersionId, apiId, apiVersion, configId, configName, propertyId, propertyName, propertyValue); }
    @Override public Result<String> createConfigDeploymentInstance(Map<String, Object> event) { return configPersistence.createConfigDeploymentInstance(event); }
    @Override public Result<String> updateConfigDeploymentInstance(Map<String, Object> event) { return configPersistence.updateConfigDeploymentInstance(event); }
    @Override public Result<String> deleteConfigDeploymentInstance(Map<String, Object> event) { return configPersistence.deleteConfigDeploymentInstance(event); }
    @Override public Result<String> getConfigDeploymentInstance(int offset, int limit, String hostId, String deploymentInstanceId, String instanceId, String instanceName, String serviceId, String ipAddress, Integer portNumber, String configId, String configName, String propertyId, String propertyName, String propertyValue) { return configPersistence.getConfigDeploymentInstance(offset, limit, hostId, deploymentInstanceId, instanceId, instanceName, serviceId, ipAddress, portNumber, configId, configName, propertyId, propertyName, propertyValue); }
    @Override public Result<String> createConfigProduct(Map<String, Object> event) { return configPersistence.createConfigProduct(event); }
    @Override public Result<String> updateConfigProduct(Map<String, Object> event) { return configPersistence.updateConfigProduct(event); }
    @Override public Result<String> deleteConfigProduct(Map<String, Object> event) { return configPersistence.deleteConfigProduct(event); }
    @Override public Result<String> getConfigProduct(int offset, int limit, String productId, String configId, String configName, String propertyId, String propertyName, String propertyValue) { return configPersistence.getConfigProduct(offset, limit, productId, configId, configName, propertyId, propertyName, propertyValue); }
    @Override public Result<String> createConfigProductVersion(Map<String, Object> event) { return configPersistence.createConfigProductVersion(event); }
    @Override public Result<String> updateConfigProductVersion(Map<String, Object> event) { return configPersistence.updateConfigProductVersion(event); }
    @Override public Result<String> deleteConfigProductVersion(Map<String, Object> event) { return configPersistence.deleteConfigProductVersion(event); }
    @Override public Result<String> getConfigProductVersion(int offset, int limit, String hostId, String productId, String productVersion, String configId, String configName, String propertyId, String propertyName, String propertyValue) { return configPersistence.getConfigProductVersion(offset, limit, hostId, productId, productVersion, configId, configName, propertyId, propertyName, propertyValue); }
    @Override public Result<String> createConfigInstanceFile(Map<String, Object> event) { return configPersistence.createConfigInstanceFile(event); }
    @Override public Result<String> updateConfigInstanceFile(Map<String, Object> event) { return configPersistence.updateConfigInstanceFile(event); }
    @Override public Result<String> deleteConfigInstanceFile(Map<String, Object> event) { return configPersistence.deleteConfigInstanceFile(event); }
    @Override public Result<String> getConfigInstanceFile(int offset, int limit, String hostId, String instanceFileId, String instanceId, String instanceName, String fileType, String fileName, String fileValue, String fileDesc, String expirationTs) { return configPersistence.getConfigInstanceFile(offset, limit, hostId, instanceFileId, instanceId, instanceName, fileType, fileName, fileValue, fileDesc, expirationTs); }
    @Override public Result<String> commitConfigInstance(Map<String, Object> event) { return configPersistence.commitConfigInstance(event); }
    @Override public Result<String> rollbackConfigInstance(Map<String, Object> event) { return configPersistence.rollbackConfigInstance(event); }

    // --- InstanceDeployment ---
    @Override public Result<String> createInstance(Map<String, Object> event) { return instanceDeploymentPersistence.createInstance(event); }
    @Override public Result<String> updateInstance(Map<String, Object> event) { return instanceDeploymentPersistence.updateInstance(event); }
    @Override public Result<String> deleteInstance(Map<String, Object> event) { return instanceDeploymentPersistence.deleteInstance(event); }
    @Override public Result<String> getInstance(int offset, int limit, String hostId, String instanceId, String instanceName, String productVersionId, String productId, String productVersion, String serviceId, Boolean current, Boolean readonly, String environment, String serviceDesc, String instanceDesc, String zone,  String region, String lob, String resourceName, String businessName, String envTag, String topicClassification) { return instanceDeploymentPersistence.getInstance(offset, limit, hostId, instanceId, instanceName, productVersionId, productId, productVersion, serviceId, current, readonly, environment, serviceDesc, instanceDesc, zone, region, lob, resourceName, businessName, envTag, topicClassification); }
    @Override public Result<String> getInstanceLabel(String hostId) { return instanceDeploymentPersistence.getInstanceLabel(hostId); }
    @Override public Result<String> createInstanceApi(Map<String, Object> event) { return instanceDeploymentPersistence.createInstanceApi(event); }
    @Override public Result<String> updateInstanceApi(Map<String, Object> event) { return instanceDeploymentPersistence.updateInstanceApi(event); }
    @Override public Result<String> deleteInstanceApi(Map<String, Object> event) { return instanceDeploymentPersistence.deleteInstanceApi(event); }
    @Override public Result<String> getInstanceApi(int offset, int limit, String hostId, String instanceApiId, String instanceId, String instanceName, String productId, String productVersion, String apiVersionId, String apiId, String apiVersion, Boolean active) { return instanceDeploymentPersistence.getInstanceApi(offset, limit, hostId, instanceApiId, instanceId, instanceName, productId, productVersion, apiVersionId, apiId, apiVersion, active); }
    @Override public Result<String> getInstanceApiLabel(String hostId, String instanceId) { return instanceDeploymentPersistence.getInstanceApiLabel(hostId, instanceId); }
    @Override public Result<String> createInstanceApiPathPrefix(Map<String, Object> event) { return instanceDeploymentPersistence.createInstanceApiPathPrefix(event); }
    @Override public Result<String> updateInstanceApiPathPrefix(Map<String, Object> event) { return instanceDeploymentPersistence.updateInstanceApiPathPrefix(event); }
    @Override public Result<String> deleteInstanceApiPathPrefix(Map<String, Object> event) { return instanceDeploymentPersistence.deleteInstanceApiPathPrefix(event); }
    @Override public Result<String> getInstanceApiPathPrefix(int offset, int limit, String hostId, String instanceApiId, String instanceId, String instanceName, String productId, String productVersion, String apiVersionId, String apiId, String apiVersion, String pathPrefix) { return instanceDeploymentPersistence.getInstanceApiPathPrefix(offset, limit, hostId, instanceApiId, instanceId, instanceName, productId, productVersion, apiVersionId, apiId, apiVersion, pathPrefix); }
    @Override public Result<String> createInstanceApp(Map<String, Object> event) { return instanceDeploymentPersistence.createInstanceApp(event); }
    @Override public Result<String> updateInstanceApp(Map<String, Object> event) { return instanceDeploymentPersistence.updateInstanceApp(event); }
    @Override public Result<String> deleteInstanceApp(Map<String, Object> event) { return instanceDeploymentPersistence.deleteInstanceApp(event); }
    @Override public Result<String> getInstanceApp(int offset, int limit, String hostId, String instanceAppId, String instanceId, String instanceName, String productId, String productVersion, String appId, String appVersion, Boolean active) { return instanceDeploymentPersistence.getInstanceApp(offset, limit, hostId, instanceAppId, instanceId, instanceName, productId, productVersion, appId, appVersion, active); }
    @Override public Result<String> getInstanceAppLabel(String hostId, String instanceId) { return instanceDeploymentPersistence.getInstanceAppLabel(hostId, instanceId); }
    @Override public Result<String> createInstanceAppApi(Map<String, Object> event) { return instanceDeploymentPersistence.createInstanceAppApi(event); }
    @Override public Result<String> updateInstanceAppApi(Map<String, Object> event) { return instanceDeploymentPersistence.updateInstanceAppApi(event); }
    @Override public Result<String> deleteInstanceAppApi(Map<String, Object> event) { return instanceDeploymentPersistence.deleteInstanceAppApi(event); }
    @Override public Result<String> getInstanceAppApi(int offset, int limit, String hostId, String instanceAppId, String instanceApiId, String instanceId, String instanceName, String productId, String productVersion, String appId, String appVersion, String apiVersionId, String apiId, String apiVersion, Boolean active) { return instanceDeploymentPersistence.getInstanceAppApi(offset, limit, hostId, instanceAppId, instanceApiId, instanceId, instanceName, productId, productVersion, appId, appVersion, apiVersionId, apiId, apiVersion, active); }
    @Override public Result<String> createProduct(Map<String, Object> event) { return instanceDeploymentPersistence.createProduct(event); }
    @Override public Result<String> updateProduct(Map<String, Object> event) { return instanceDeploymentPersistence.updateProduct(event); }
    @Override public Result<String> deleteProduct(Map<String, Object> event) { return instanceDeploymentPersistence.deleteProduct(event); }
    @Override public Result<String> getProduct(int offset, int limit, String hostId, String productVersionId, String productId, String productVersion, String light4jVersion, Boolean breakCode, Boolean breakConfig, String releaseNote, String versionDesc, String releaseType, Boolean current, String versionStatus) { return instanceDeploymentPersistence.getProduct(offset, limit, hostId, productVersionId, productId, productVersion, light4jVersion, breakCode, breakConfig, releaseNote, versionDesc, releaseType, current, versionStatus); }
    @Override public Result<String> getProductIdLabel(String hostId) { return instanceDeploymentPersistence.getProductIdLabel(hostId); }
    @Override public Result<String> getProductVersionLabel(String hostId, String productId) { return instanceDeploymentPersistence.getProductVersionLabel(hostId, productId); }
    @Override public Result<String> getProductVersionIdLabel(String hostId) { return instanceDeploymentPersistence.getProductVersionIdLabel(hostId); }
    @Override public Result<String> createProductVersionEnvironment(Map<String, Object> event) { return instanceDeploymentPersistence.createProductVersionEnvironment(event); }
    @Override public Result<String> deleteProductVersionEnvironment(Map<String, Object> event) { return instanceDeploymentPersistence.deleteProductVersionEnvironment(event); }
    @Override public Result<String> getProductVersionEnvironment(int offset, int limit, String hostId, String productVersionId, String productId, String productVersion, String systemEnv, String runtimeEnv) { return instanceDeploymentPersistence.getProductVersionEnvironment(offset, limit, hostId, productVersionId, productId, productVersion, systemEnv, runtimeEnv); }
    @Override public Result<String> createProductVersionPipeline(Map<String, Object> event) { return instanceDeploymentPersistence.createProductVersionPipeline(event); }
    @Override public Result<String> deleteProductVersionPipeline(Map<String, Object> event) { return instanceDeploymentPersistence.deleteProductVersionPipeline(event); }
    @Override public Result<String> getProductVersionPipeline(int offset, int limit, String hostId, String productVersionId, String productId, String productVersion, String pipelineId, String pipelineName, String pipelineVersion) { return instanceDeploymentPersistence.getProductVersionPipeline(offset, limit, hostId, productVersionId, productId, productVersion, pipelineId, pipelineName, pipelineVersion); }
    @Override public Result<String> createProductVersionConfig(Map<String, Object> event) { return instanceDeploymentPersistence.createProductVersionConfig(event); }
    @Override public Result<String> deleteProductVersionConfig(Map<String, Object> event) { return instanceDeploymentPersistence.deleteProductVersionConfig(event); }
    @Override public Result<String> getProductVersionConfig(int offset, int limit, List<SortCriterion> sorting, List<FilterCriterion> filtering, String globalFilter, String hostId, String productVersionId, String productId, String productVersion, String configId, String configName) { return instanceDeploymentPersistence.getProductVersionConfig(offset, limit, sorting, filtering, globalFilter, hostId, productVersionId, productId, productVersion, configId, configName); }
    @Override public Result<String> createProductVersionConfigProperty(Map<String, Object> event) { return instanceDeploymentPersistence.createProductVersionConfigProperty(event); }
    @Override public Result<String> deleteProductVersionConfigProperty(Map<String, Object> event) { return instanceDeploymentPersistence.deleteProductVersionConfigProperty(event); }
    @Override public Result<String> getProductVersionConfigProperty(int offset, int limit, String hostId, String productVersionId, String productId, String productVersion, String configId, String configName, String propertyId, String propertyName) { return instanceDeploymentPersistence.getProductVersionConfigProperty(offset, limit, hostId, productVersionId, productId, productVersion, configId, configName, propertyId, propertyName); }
    @Override public Result<String> createPipeline(Map<String, Object> event) { return instanceDeploymentPersistence.createPipeline(event); }
    @Override public Result<String> updatePipeline(Map<String, Object> event) { return instanceDeploymentPersistence.updatePipeline(event); }
    @Override public Result<String> deletePipeline(Map<String, Object> event) { return instanceDeploymentPersistence.deletePipeline(event); }
    @Override public Result<String> getPipeline(int offset, int limit, String hostId, String pipelineId, String platformId, String platformName, String platformVersion, String pipelineVersion, String pipelineName, Boolean current, String endpoint, String versionStatus, String systemEnv, String runtimeEnv, String requestSchema, String responseSchema) { return instanceDeploymentPersistence.getPipeline(offset, limit, hostId, pipelineId, platformId, platformName, platformVersion, pipelineVersion, pipelineName, current, endpoint, versionStatus, systemEnv, runtimeEnv, requestSchema, responseSchema); }
    @Override public Result<String> getPipelineLabel(String hostId) { return instanceDeploymentPersistence.getPipelineLabel(hostId); }
    @Override public Result<String> createInstancePipeline(Map<String, Object> event) { return instanceDeploymentPersistence.createInstancePipeline(event); }
    @Override public Result<String> updateInstancePipeline(Map<String, Object> event) { return instanceDeploymentPersistence.updateInstancePipeline(event); }
    @Override public Result<String> deleteInstancePipeline(Map<String, Object> event) { return instanceDeploymentPersistence.deleteInstancePipeline(event); }
    @Override public Result<String> getInstancePipeline(int offset, int limit, String hostId, String instanceId, String instanceName, String productId, String productVersion, String pipelineId, String platformName, String platformVersion, String pipelineName, String pipelineVersion) { return instanceDeploymentPersistence.getInstancePipeline(offset, limit, hostId, instanceId, instanceName, productId, productVersion, pipelineId, platformName, platformVersion, pipelineName, pipelineVersion); }
    @Override public Result<String> createPlatform(Map<String, Object> event) { return instanceDeploymentPersistence.createPlatform(event); }
    @Override public Result<String> updatePlatform(Map<String, Object> event) { return instanceDeploymentPersistence.updatePlatform(event); }
    @Override public Result<String> deletePlatform(Map<String, Object> event) { return instanceDeploymentPersistence.deletePlatform(event); }
    @Override public Result<String> getPlatform(int offset, int limit, String hostId, String platformId, String platformName, String platformVersion, String clientType, String clientUrl, String credentials, String proxyUrl, Integer proxyPort, String handlerClass, String consoleUrl, String environment, String zone, String region, String lob) { return instanceDeploymentPersistence.getPlatform(offset, limit, hostId, platformId, platformName, platformVersion, clientType, clientUrl, credentials, proxyUrl, proxyPort, handlerClass, consoleUrl, environment, zone, region, lob); }
    @Override public Result<String> getPlatformLabel(String hostId) { return instanceDeploymentPersistence.getPlatformLabel(hostId); }
    @Override public Result<String> createDeploymentInstance(Map<String, Object> event) { return instanceDeploymentPersistence.createDeploymentInstance(event); }
    @Override public Result<String> updateDeploymentInstance(Map<String, Object> event) { return instanceDeploymentPersistence.updateDeploymentInstance(event); }
    @Override public Result<String> deleteDeploymentInstance(Map<String, Object> event) { return instanceDeploymentPersistence.deleteDeploymentInstance(event); }
    @Override public Result<String> getDeploymentInstance(int offset, int limit, String hostId, String instanceId, String instanceName, String deploymentInstanceId, String serviceId, String ipAddress, Integer portNumber, String systemEnv, String runtimeEnv, String pipelineId, String pipelineName, String pipelineVersion, String deployStatus) { return instanceDeploymentPersistence.getDeploymentInstance(offset, limit, hostId, instanceId, instanceName, deploymentInstanceId, serviceId, ipAddress, portNumber, systemEnv, runtimeEnv, pipelineId, pipelineName, pipelineVersion, deployStatus); }
    @Override public Result<String> getDeploymentInstancePipeline(String hostId, String instanceId, String systemEnv, String runtimeEnv) { return instanceDeploymentPersistence.getDeploymentInstancePipeline(hostId, instanceId, systemEnv, runtimeEnv); }
    @Override public Result<String> getDeploymentInstanceLabel(String hostId, String instanceId) { return instanceDeploymentPersistence.getDeploymentInstanceLabel(hostId, instanceId); }
    @Override public Result<String> createDeployment(Map<String, Object> event) { return instanceDeploymentPersistence.createDeployment(event); }
    @Override public Result<String> updateDeployment(Map<String, Object> event) { return instanceDeploymentPersistence.updateDeployment(event); }
    @Override public Result<String> updateDeploymentJobId(Map<String, Object> event) { return instanceDeploymentPersistence.updateDeploymentJobId(event); }
    @Override public Result<String> updateDeploymentStatus(Map<String, Object> event) { return instanceDeploymentPersistence.updateDeploymentStatus(event); }
    @Override public Result<String> deleteDeployment(Map<String, Object> event) { return instanceDeploymentPersistence.deleteDeployment(event); }
    @Override public Result<String> getDeployment(int offset, int limit, String hostId, String deploymentId, String deploymentInstanceId, String serviceId, String deploymentStatus, String deploymentType, String platformJobId) { return instanceDeploymentPersistence.getDeployment(offset, limit, hostId, deploymentId, deploymentInstanceId, serviceId, deploymentStatus, deploymentType, platformJobId); }

    // --- AccessControl ---
    @Override public Result<String> createRole(Map<String, Object> event) { return accessControlPersistence.createRole(event); }
    @Override public Result<String> updateRole(Map<String, Object> event) { return accessControlPersistence.updateRole(event); }
    @Override public Result<String> deleteRole(Map<String, Object> event) { return accessControlPersistence.deleteRole(event); }
    @Override public Result<String> queryRole(int offset, int limit, String hostId, String roleId, String roleDesc) { return accessControlPersistence.queryRole(offset, limit, hostId, roleId, roleDesc); }
    @Override public Result<String> queryRoleLabel(String hostId) { return accessControlPersistence.queryRoleLabel(hostId); }
    @Override public Result<String> createRolePermission(Map<String, Object> event) { return accessControlPersistence.createRolePermission(event); }
    @Override public Result<String> deleteRolePermission(Map<String, Object> event) { return accessControlPersistence.deleteRolePermission(event); }
    @Override public Result<String> queryRolePermission(int offset, int limit, String hostId, String roleId, String apiId, String apiVersion, String endpoint) { return accessControlPersistence.queryRolePermission(offset, limit, hostId, roleId, apiId, apiVersion, endpoint); }
    @Override public Result<String> createRoleUser(Map<String, Object> event) { return accessControlPersistence.createRoleUser(event); }
    @Override public Result<String> updateRoleUser(Map<String, Object> event) { return accessControlPersistence.updateRoleUser(event); }
    @Override public Result<String> deleteRoleUser(Map<String, Object> event) { return accessControlPersistence.deleteRoleUser(event); }
    @Override public Result<String> queryRoleUser(int offset, int limit, String hostId, String roleId, String userId, String entityId, String email, String firstName, String lastName, String userType) { return accessControlPersistence.queryRoleUser(offset, limit, hostId, roleId, userId, entityId, email, firstName, lastName, userType); }
    @Override public Result<String> createRoleRowFilter(Map<String, Object> event) { return accessControlPersistence.createRoleRowFilter(event); }
    @Override public Result<String> updateRoleRowFilter(Map<String, Object> event) { return accessControlPersistence.updateRoleRowFilter(event); }
    @Override public Result<String> deleteRoleRowFilter(Map<String, Object> event) { return accessControlPersistence.deleteRoleRowFilter(event); }
    @Override public Result<String> queryRoleRowFilter(int offset, int limit, String hostId, String roleId, String apiId, String apiVersion, String endpoint) { return accessControlPersistence.queryRoleRowFilter(offset, limit, hostId, roleId, apiId, apiVersion, endpoint); }
    @Override public Result<String> createRoleColFilter(Map<String, Object> event) { return accessControlPersistence.createRoleColFilter(event); }
    @Override public Result<String> updateRoleColFilter(Map<String, Object> event) { return accessControlPersistence.updateRoleColFilter(event); }
    @Override public Result<String> deleteRoleColFilter(Map<String, Object> event) { return accessControlPersistence.deleteRoleColFilter(event); }
    @Override public Result<String> queryRoleColFilter(int offset, int limit, String hostId, String roleId, String apiId, String apiVersion, String endpoint) { return accessControlPersistence.queryRoleColFilter(offset, limit, hostId, roleId, apiId, apiVersion, endpoint); }
    @Override public Result<String> createGroup(Map<String, Object> event) { return accessControlPersistence.createGroup(event); }
    @Override public Result<String> updateGroup(Map<String, Object> event) { return accessControlPersistence.updateGroup(event); }
    @Override public Result<String> deleteGroup(Map<String, Object> event) { return accessControlPersistence.deleteGroup(event); }
    @Override public Result<String> queryGroup(int offset, int limit, String hostId, String groupId, String groupDesc) { return accessControlPersistence.queryGroup(offset, limit, hostId, groupId, groupDesc); }
    @Override public Result<String> queryGroupLabel(String hostId) { return accessControlPersistence.queryGroupLabel(hostId); }
    @Override public Result<String> createGroupPermission(Map<String, Object> event) { return accessControlPersistence.createGroupPermission(event); }
    @Override public Result<String> deleteGroupPermission(Map<String, Object> event) { return accessControlPersistence.deleteGroupPermission(event); }
    @Override public Result<String> queryGroupPermission(int offset, int limit, String hostId, String groupId, String apiId, String apiVersion, String endpoint) { return accessControlPersistence.queryGroupPermission(offset, limit, hostId, groupId, apiId, apiVersion, endpoint); }
    @Override public Result<String> createGroupUser(Map<String, Object> event) { return accessControlPersistence.createGroupUser(event); }
    @Override public Result<String> updateGroupUser(Map<String, Object> event) { return accessControlPersistence.updateGroupUser(event); }
    @Override public Result<String> deleteGroupUser(Map<String, Object> event) { return accessControlPersistence.deleteGroupUser(event); }
    @Override public Result<String> queryGroupUser(int offset, int limit, String hostId, String groupId, String userId, String entityId, String email, String firstName, String lastName, String userType) { return accessControlPersistence.queryGroupUser(offset, limit, hostId, groupId, userId, entityId, email, firstName, lastName, userType); }
    @Override public Result<String> createGroupRowFilter(Map<String, Object> event) { return accessControlPersistence.createGroupRowFilter(event); }
    @Override public Result<String> updateGroupRowFilter(Map<String, Object> event) { return accessControlPersistence.updateGroupRowFilter(event); }
    @Override public Result<String> deleteGroupRowFilter(Map<String, Object> event) { return accessControlPersistence.deleteGroupRowFilter(event); }
    @Override public Result<String> queryGroupRowFilter(int offset, int limit, String hostId, String GroupId, String apiId, String apiVersion, String endpoint) { return accessControlPersistence.queryGroupRowFilter(offset, limit, hostId, GroupId, apiId, apiVersion, endpoint); }
    @Override public Result<String> createGroupColFilter(Map<String, Object> event) { return accessControlPersistence.createGroupColFilter(event); }
    @Override public Result<String> updateGroupColFilter(Map<String, Object> event) { return accessControlPersistence.updateGroupColFilter(event); }
    @Override public Result<String> deleteGroupColFilter(Map<String, Object> event) { return accessControlPersistence.deleteGroupColFilter(event); }
    @Override public Result<String> queryGroupColFilter(int offset, int limit, String hostId, String GroupId, String apiId, String apiVersion, String endpoint) { return accessControlPersistence.queryGroupColFilter(offset, limit, hostId, GroupId, apiId, apiVersion, endpoint); }
    @Override public Result<String> createPosition(Map<String, Object> event) { return accessControlPersistence.createPosition(event); }
    @Override public Result<String> updatePosition(Map<String, Object> event) { return accessControlPersistence.updatePosition(event); }
    @Override public Result<String> deletePosition(Map<String, Object> event) { return accessControlPersistence.deletePosition(event); }
    @Override public Result<String> queryPosition(int offset, int limit, String hostId, String positionId, String positionDesc, String inheritToAncestor, String inheritToSibling) { return accessControlPersistence.queryPosition(offset, limit, hostId, positionId, positionDesc, inheritToAncestor, inheritToSibling); }
    @Override public Result<String> queryPositionLabel(String hostId) { return accessControlPersistence.queryPositionLabel(hostId); }
    @Override public Result<String> createPositionPermission(Map<String, Object> event) { return accessControlPersistence.createPositionPermission(event); }
    @Override public Result<String> deletePositionPermission(Map<String, Object> event) { return accessControlPersistence.deletePositionPermission(event); }
    @Override public Result<String> queryPositionPermission(int offset, int limit, String hostId, String positionId, String inheritToAncestor, String inheritToSibling, String apiId, String apiVersion, String endpoint) { return accessControlPersistence.queryPositionPermission(offset, limit, hostId, positionId, inheritToAncestor, inheritToSibling, apiId, apiVersion, endpoint); }
    @Override public Result<String> createPositionUser(Map<String, Object> event) { return accessControlPersistence.createPositionUser(event); }
    @Override public Result<String> updatePositionUser(Map<String, Object> event) { return accessControlPersistence.updatePositionUser(event); }
    @Override public Result<String> deletePositionUser(Map<String, Object> event) { return accessControlPersistence.deletePositionUser(event); }
    @Override public Result<String> queryPositionUser(int offset, int limit, String hostId, String positionId, String positionType, String inheritToAncestor, String inheritToSibling, String userId, String entityId, String email, String firstName, String lastName, String userType) { return accessControlPersistence.queryPositionUser(offset, limit, hostId, positionId, positionType, inheritToAncestor, inheritToSibling, userId, entityId, email, firstName, lastName, userType); }
    @Override public Result<String> createPositionRowFilter(Map<String, Object> event) { return accessControlPersistence.createPositionRowFilter(event); }
    @Override public Result<String> updatePositionRowFilter(Map<String, Object> event) { return accessControlPersistence.updatePositionRowFilter(event); }
    @Override public Result<String> deletePositionRowFilter(Map<String, Object> event) { return accessControlPersistence.deletePositionRowFilter(event); }
    @Override public Result<String> queryPositionRowFilter(int offset, int limit, String hostId, String PositionId, String apiId, String apiVersion, String endpoint) { return accessControlPersistence.queryPositionRowFilter(offset, limit, hostId, PositionId, apiId, apiVersion, endpoint); }
    @Override public Result<String> createPositionColFilter(Map<String, Object> event) { return accessControlPersistence.createPositionColFilter(event); }
    @Override public Result<String> updatePositionColFilter(Map<String, Object> event) { return accessControlPersistence.updatePositionColFilter(event); }
    @Override public Result<String> deletePositionColFilter(Map<String, Object> event) { return accessControlPersistence.deletePositionColFilter(event); }
    @Override public Result<String> queryPositionColFilter(int offset, int limit, String hostId, String PositionId, String apiId, String apiVersion, String endpoint) { return accessControlPersistence.queryPositionColFilter(offset, limit, hostId, PositionId, apiId, apiVersion, endpoint); }
    @Override public Result<String> createAttribute(Map<String, Object> event) { return accessControlPersistence.createAttribute(event); }
    @Override public Result<String> updateAttribute(Map<String, Object> event) { return accessControlPersistence.updateAttribute(event); }
    @Override public Result<String> deleteAttribute(Map<String, Object> event) { return accessControlPersistence.deleteAttribute(event); }
    @Override public Result<String> queryAttribute(int offset, int limit, String hostId, String attributeId, String attributeType, String attributeDesc) { return accessControlPersistence.queryAttribute(offset, limit, hostId, attributeId, attributeType, attributeDesc); }
    @Override public Result<String> queryAttributeLabel(String hostId) { return accessControlPersistence.queryAttributeLabel(hostId); }
    @Override public Result<String> createAttributePermission(Map<String, Object> event) { return accessControlPersistence.createAttributePermission(event); }
    @Override public Result<String> updateAttributePermission(Map<String, Object> event) { return accessControlPersistence.updateAttributePermission(event); }
    @Override public Result<String> deleteAttributePermission(Map<String, Object> event) { return accessControlPersistence.deleteAttributePermission(event); }
    @Override public Result<String> queryAttributePermission(int offset, int limit, String hostId, String attributeId, String attributeType, String attributeValue, String apiId, String apiVersion, String endpoint) { return accessControlPersistence.queryAttributePermission(offset, limit, hostId, attributeId, attributeType, attributeValue, apiId, apiVersion, endpoint); }
    @Override public Result<String> createAttributeUser(Map<String, Object> event) { return accessControlPersistence.createAttributeUser(event); }
    @Override public Result<String> updateAttributeUser(Map<String, Object> event) { return accessControlPersistence.updateAttributeUser(event); }
    @Override public Result<String> deleteAttributeUser(Map<String, Object> event) { return accessControlPersistence.deleteAttributeUser(event); }
    @Override public Result<String> queryAttributeUser(int offset, int limit, String hostId, String attributeId, String attributeType, String attributeValue, String userId, String entityId, String email, String firstName, String lastName, String userType) { return accessControlPersistence.queryAttributeUser(offset, limit, hostId, attributeId, attributeType, attributeValue, userId, entityId, email, firstName, lastName, userType); }
    @Override public Result<String> createAttributeRowFilter(Map<String, Object> event) { return accessControlPersistence.createAttributeRowFilter(event); }
    @Override public Result<String> updateAttributeRowFilter(Map<String, Object> event) { return accessControlPersistence.updateAttributeRowFilter(event); }
    @Override public Result<String> deleteAttributeRowFilter(Map<String, Object> event) { return accessControlPersistence.deleteAttributeRowFilter(event); }
    @Override public Result<String> queryAttributeRowFilter(int offset, int limit, String hostId, String attributeId, String attributeValue, String apiId, String apiVersion, String endpoint) { return accessControlPersistence.queryAttributeRowFilter(offset, limit, hostId, attributeId, attributeValue, apiId, apiVersion, endpoint); }
    @Override public Result<String> createAttributeColFilter(Map<String, Object> event) { return accessControlPersistence.createAttributeColFilter(event); }
    @Override public Result<String> updateAttributeColFilter(Map<String, Object> event) { return accessControlPersistence.updateAttributeColFilter(event); }
    @Override public Result<String> deleteAttributeColFilter(Map<String, Object> event) { return accessControlPersistence.deleteAttributeColFilter(event); }
    @Override public Result<String> queryAttributeColFilter(int offset, int limit, String hostId, String attributeId, String attributeValue, String apiId, String apiVersion, String endpoint) { return accessControlPersistence.queryAttributeColFilter(offset, limit, hostId, attributeId, attributeValue, apiId, apiVersion, endpoint); }

    // --- Category ---
    @Override public Result<String> createCategory(Map<String, Object> event) { return categoryPersistence.createCategory(event); }
    @Override public Result<String> updateCategory(Map<String, Object> event) { return categoryPersistence.updateCategory(event); }
    @Override public Result<String> deleteCategory(Map<String, Object> event) { return categoryPersistence.deleteCategory(event); }
    @Override public Result<String> getCategory(int offset, int limit, String hostId, String categoryId, String entityType, String categoryName, String categoryDesc, String parentCategoryId, String parentCategoryName, Integer sortOrder) { return categoryPersistence.getCategory(offset, limit, hostId, categoryId, entityType, categoryName, categoryDesc, parentCategoryId, parentCategoryName, sortOrder); }
    @Override public Result<String> getCategoryLabel(String hostId) { return categoryPersistence.getCategoryLabel(hostId); }
    @Override public Result<String> getCategoryById(String categoryId) { return categoryPersistence.getCategoryById(categoryId); }
    @Override public Result<String> getCategoryByName(String hostId, String categoryName) { return categoryPersistence.getCategoryByName(hostId, categoryName); }
    @Override public Result<String> getCategoryByType(String hostId, String entityType) { return categoryPersistence.getCategoryByType(hostId, entityType); }
    @Override public Result<String> getCategoryTree(String hostId, String entityType) { return categoryPersistence.getCategoryTree(hostId, entityType); }

    // --- Tag ---
    @Override public Result<String> createTag(Map<String, Object> event) { return tagPersistence.createTag(event); }
    @Override public Result<String> updateTag(Map<String, Object> event) { return tagPersistence.updateTag(event); }
    @Override public Result<String> deleteTag(Map<String, Object> event) { return tagPersistence.deleteTag(event); }
    @Override public Result<String> getTag(int offset, int limit, String hostId, String tagId, String entityType, String tagName, String tagDesc) { return tagPersistence.getTag(offset, limit, hostId, tagId, entityType, tagName, tagDesc); }
    @Override public Result<String> getTagLabel(String hostId) { return tagPersistence.getTagLabel(hostId); }
    @Override public Result<String> getTagById(String tagId) { return tagPersistence.getTagById(tagId); }
    @Override public Result<String> getTagByName(String hostId, String tagName) { return tagPersistence.getTagByName(hostId, tagName); }
    @Override public Result<String> getTagByType(String hostId, String entityType) { return tagPersistence.getTagByType(hostId, entityType); }

    // --- Schema ---
    @Override public Result<String> createSchema(Map<String, Object> event) { return schemaPersistence.createSchema(event); }
    @Override public Result<String> updateSchema(Map<String, Object> event) { return schemaPersistence.updateSchema(event); }
    @Override public Result<String> deleteSchema(Map<String, Object> event) { return schemaPersistence.deleteSchema(event); }
    @Override public Result<String> getSchema(int offset, int limit, String hostId, String schemaId, String schemaVersion, String schemaType, String specVersion, String schemaSource, String schemaName, String schemaDesc, String schemaBody, String schemaOwner, String schemaStatus, String example, String commentStatus) { return schemaPersistence.getSchema(offset, limit, hostId, schemaId, schemaVersion, schemaType, specVersion, schemaSource, schemaName, schemaDesc, schemaBody, schemaOwner, schemaStatus, example, commentStatus); }
    @Override public Result<String> getSchemaLabel(String hostId) { return schemaPersistence.getSchemaLabel(hostId); }
    @Override public Result<String> getSchemaById(String schemaId) { return schemaPersistence.getSchemaById(schemaId); }
    @Override public Result<String> getSchemaByCategoryId(String categoryId) { return schemaPersistence.getSchemaByCategoryId(categoryId); }
    @Override public Result<String> getSchemaByTagId(String tagId) { return schemaPersistence.getSchemaByTagId(tagId); }

    // --- Schedule ---
    @Override public Result<String> createSchedule(Map<String, Object> event) { return schedulePersistence.createSchedule(event); }
    @Override public Result<String> updateSchedule(Map<String, Object> event) { return schedulePersistence.updateSchedule(event); }
    @Override public Result<String> deleteSchedule(Map<String, Object> event) { return schedulePersistence.deleteSchedule(event); }
    @Override public Result<String> getSchedule(int offset, int limit, String hostId, String scheduleId, String scheduleName, String frequencyUnit, Integer frequencyTime, String startTs, String eventTopic, String eventType, String eventData) { return schedulePersistence.getSchedule(offset, limit, hostId, scheduleId, scheduleName, frequencyUnit, frequencyTime, startTs, eventTopic, eventType, eventData); }
    @Override public Result<String> getScheduleLabel(String hostId) { return schedulePersistence.getScheduleLabel(hostId); }
    @Override public Result<String> getScheduleById(String scheduleId) { return schedulePersistence.getScheduleById(scheduleId); }

    // --- Rule ---
    @Override public Result<String> createRule(Map<String, Object> event) { return rulePersistence.createRule(event); }
    @Override public Result<String> updateRule(Map<String, Object> event) { return rulePersistence.updateRule(event); }
    @Override public Result<String> deleteRule(Map<String, Object> event) { return rulePersistence.deleteRule(event); }
    @Override public Result<List<Map<String, Object>>> queryRuleByHostGroup(String hostId, String groupId) { return rulePersistence.queryRuleByHostGroup(hostId, groupId); }
    @Override public Result<String> queryRule(int offset, int limit, String hostId, String ruleId, String ruleName,
                             String ruleVersion, String ruleType, String ruleGroup, String ruleDesc,
                             String ruleBody, String ruleOwner, String common) { return rulePersistence.queryRule(offset, limit, hostId, ruleId, ruleName, ruleVersion, ruleType, ruleGroup, ruleDesc, ruleBody, ruleOwner, common); }
    @Override public Result<Map<String, Object>> queryRuleById(String ruleId) { return rulePersistence.queryRuleById(ruleId); }
    @Override public Result<String> queryRuleByHostType(String hostId, String ruleType) { return rulePersistence.queryRuleByHostType(hostId, ruleType); }
    @Override public Result<List<Map<String, Object>>> queryRuleByHostApiId(String hostId, String apiId, String apiVersion) { return rulePersistence.queryRuleByHostApiId(hostId, apiId, apiVersion); }

}
