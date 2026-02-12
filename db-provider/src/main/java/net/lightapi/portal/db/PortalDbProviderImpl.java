package net.lightapi.portal.db;

import com.networknt.monad.Result;
import io.cloudevents.CloudEvent;
import net.lightapi.portal.db.persistence.*;
import net.lightapi.portal.db.util.NotificationService;
import net.lightapi.portal.db.util.NotificationServiceImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.SQLException; // Added import
import java.time.OffsetDateTime;
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
    private final GenAIPersistence genAIPersistence;

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
        this.genAIPersistence = new GenAIPersistenceImpl();
    }

    // --- Reference ---
    @Override public void createRefTable(Connection connection, Map<String, Object> event) throws PortalPersistenceException { referenceDataPersistence.createRefTable(connection, event); }
    @Override public void updateRefTable(Connection connection, Map<String, Object> event) throws PortalPersistenceException { referenceDataPersistence.updateRefTable(connection, event); }
    @Override public void deleteRefTable(Connection connection, Map<String, Object> event) throws PortalPersistenceException { referenceDataPersistence.deleteRefTable(connection, event); }
    @Override public Result<String> getRefTable(int offset, int limit, String filters, String globalFilter, String sorting, boolean active, String hostId) { return referenceDataPersistence.getRefTable(offset, limit, filters, globalFilter, sorting, active, hostId); }
    @Override public Result<String> getRefTableById(String tableId) { return referenceDataPersistence.getRefTableById(tableId); }
    @Override public Result<String> getRefTableLabel(String hostId) { return referenceDataPersistence.getRefTableLabel(hostId); }
    @Override public void createRefValue(Connection connection, Map<String, Object> event) throws PortalPersistenceException { referenceDataPersistence.createRefValue(connection, event); }
    @Override public void updateRefValue(Connection connection, Map<String, Object> event) throws PortalPersistenceException { referenceDataPersistence.updateRefValue(connection, event); }
    @Override public void deleteRefValue(Connection connection, Map<String, Object> event) throws PortalPersistenceException { referenceDataPersistence.deleteRefValue(connection, event); }
    @Override public Result<String> getRefValue(int offset, int limit, String filters, String globalFilter, String sorting, boolean active) { return referenceDataPersistence.getRefValue(offset, limit, filters, globalFilter, sorting, active); }
    @Override public Result<String> getRefValueById(String valueId) { return referenceDataPersistence.getRefValueById(valueId); }
    @Override public Result<String> getRefValueLabel(String tableId) { return referenceDataPersistence.getRefValueLabel(tableId); }
    @Override public void createRefLocale(Connection connection, Map<String, Object> event) throws PortalPersistenceException { referenceDataPersistence.createRefLocale(connection, event); }
    @Override public void updateRefLocale(Connection connection, Map<String, Object> event) throws PortalPersistenceException { referenceDataPersistence.updateRefLocale(connection, event); }
    @Override public void deleteRefLocale(Connection connection, Map<String, Object> event) throws PortalPersistenceException { referenceDataPersistence.deleteRefLocale(connection, event); }
    @Override public Result<String> getRefLocale(int offset, int limit, String filters, String globalFilter, String sorting, boolean active) { return referenceDataPersistence.getRefLocale(offset, limit, filters, globalFilter, sorting, active); }
    @Override public void createRefRelationType(Connection connection, Map<String, Object> event) throws PortalPersistenceException { referenceDataPersistence.createRefRelationType(connection, event); }
    @Override public void updateRefRelationType(Connection connection, Map<String, Object> event) throws PortalPersistenceException { referenceDataPersistence.updateRefRelationType(connection, event); }
    @Override public void deleteRefRelationType(Connection connection, Map<String, Object> event) throws PortalPersistenceException { referenceDataPersistence.deleteRefRelationType(connection, event); }
    @Override public Result<String> getRefRelationType(int offset, int limit, String filters, String globalFilter, String sorting, boolean active) { return referenceDataPersistence.getRefRelationType(offset, limit, filters, globalFilter, sorting, active); }
    @Override public void createRefRelation(Connection connection, Map<String, Object> event) throws PortalPersistenceException { referenceDataPersistence.createRefRelation(connection, event); }
    @Override public void updateRefRelation(Connection connection, Map<String, Object> event) throws PortalPersistenceException { referenceDataPersistence.updateRefRelation(connection, event); }
    @Override public void deleteRefRelation(Connection connection, Map<String, Object> event) throws PortalPersistenceException { referenceDataPersistence.deleteRefRelation(connection, event); }
    @Override public Result<String> getRefRelation(int offset, int limit, String filters, String globalFilter, String sorting, boolean active) { return referenceDataPersistence.getRefRelation(offset, limit, filters, globalFilter, sorting, active); }
    @Override public Result<String> getToValueCode(String relationName, String fromValueCode) { return referenceDataPersistence.getToValueCode(relationName, fromValueCode); };

    // --- User ---
    @Override public Result<String> loginUserByEmail(String email) { return userPersistence.loginUserByEmail(email); }
    @Override public Result<String> queryUserByEmail(String email) { return userPersistence.queryUserByEmail(email); }
    @Override public Result<String> queryUserById(String userId) { return userPersistence.queryUserById(userId); }
    @Override public Result<String> getUserById(String userId) { return userPersistence.getUserById(userId); }
    @Override public Result<String> queryUserByTypeEntityId(String userType, String entityId) { return userPersistence.queryUserByTypeEntityId(userType, entityId); }
    @Override public Result<String> queryUserByWallet(String cryptoType, String cryptoAddress) { return userPersistence.queryUserByWallet(cryptoType, cryptoAddress); }
    @Override public Result<String> queryUserByHostId(int offset, int limit, String filters, String globalFilter, String sorting, boolean active) { return userPersistence.queryUserByHostId(offset, limit, filters, globalFilter, sorting, active); }
    @Override public Result<String> getHostsByUserId(String userId) {return userPersistence.getHostsByUserId(userId); }
    @Override public Result<String> getHostLabelByUserId(String userId) {return userPersistence.getHostLabelByUserId(userId); }
    @Override public void createUser(Connection connection, Map<String, Object> event) throws PortalPersistenceException { userPersistence.createUser(connection, event); }
    @Override public void onboardUser(Connection connection, Map<String, Object> event) throws PortalPersistenceException { userPersistence.onboardUser(connection, event); }
    @Override public long queryNonceByUserId(String userId) { return userPersistence.queryNonceByUserId(userId); }
    @Override public void confirmUser(Connection connection, Map<String, Object> event) throws PortalPersistenceException { userPersistence.confirmUser(connection, event); }
    @Override public void verifyUser(Connection connection, Map<String, Object> event) throws PortalPersistenceException { userPersistence.verifyUser(connection, event); }
    @Override public void createSocialUser(Connection connection, Map<String, Object> event) throws PortalPersistenceException { userPersistence.createSocialUser(connection, event); }
    @Override public void updateUser(Connection connection, Map<String, Object> event) throws PortalPersistenceException { userPersistence.updateUser(connection, event); }
    @Override public void deleteUser(Connection connection, Map<String, Object> event) throws PortalPersistenceException { userPersistence.deleteUser(connection, event); }
    @Override public void forgetPassword(Connection connection, Map<String, Object> event) throws PortalPersistenceException { userPersistence.forgetPassword(connection, event); }
    @Override public void resetPassword(Connection connection, Map<String, Object> event) throws PortalPersistenceException { userPersistence.resetPassword(connection, event); }
    @Override public void changePassword(Connection connection, Map<String, Object> event) throws PortalPersistenceException { userPersistence.changePassword(connection, event); }
    @Override public Result<String> queryUserLabel(String hostId) { return userPersistence.queryUserLabel(hostId); }
    @Override public Result<String> getUserLabelNotInHost(String hostId) { return userPersistence.getUserLabelNotInHost(hostId); }
    @Override public Result<String> queryEmailByWallet(String cryptoType, String cryptoAddress) { return userPersistence.queryEmailByWallet(cryptoType, cryptoAddress); }
    @Override public void sendPrivateMessage(Connection connection, Map<String, Object> event) throws PortalPersistenceException { userPersistence.sendPrivateMessage(connection, event); }
    @Override public void updatePayment(Connection connection, Map<String, Object> event) throws PortalPersistenceException { userPersistence.updatePayment(connection, event); }
    @Override public void deletePayment(Connection connection, Map<String, Object> event) throws PortalPersistenceException { userPersistence.deletePayment(connection, event); }
    @Override public void createOrder(Connection connection, Map<String, Object> event) throws PortalPersistenceException { userPersistence.createOrder(connection, event); }
    @Override public void cancelOrder(Connection connection, Map<String, Object> event) throws PortalPersistenceException { userPersistence.cancelOrder(connection, event); }
    @Override public void deliverOrder(Connection connection, Map<String, Object> event) throws PortalPersistenceException { userPersistence.deliverOrder(connection, event); }
    @Override public Result<String> queryNotification(int offset, int limit, String hostId, String userId, Long nonce, String eventClass, Boolean successFlag, OffsetDateTime processTs, String eventJson, String error) { return notificationDataPersistence.queryNotification(offset, limit, hostId, userId, nonce, eventClass, successFlag, processTs, eventJson, error); }

    // --- Auth ---
    @Override public void createApp(Connection connection, Map<String, Object> event) throws PortalPersistenceException { authPersistence.createApp(connection, event); }
    @Override public void updateApp(Connection connection, Map<String, Object> event) throws PortalPersistenceException { authPersistence.updateApp(connection, event); }
    @Override public void deleteApp(Connection connection, Map<String, Object> event) throws PortalPersistenceException { authPersistence.deleteApp(connection, event); }
    @Override public Result<String> queryApp(int offset, int limit, String filters, String globalFilter, String sorting, boolean active, String hostId) { return authPersistence.queryApp(offset, limit, filters, globalFilter, sorting, active, hostId); }
    @Override public Result<String>  queryAppById(java.lang.String hostId, java.lang.String appId) { return authPersistence.queryAppById(hostId, appId); }
    @Override public Result<String> getAppIdLabel(String hostId) { return authPersistence.getAppIdLabel(hostId); }
    @Override public void createClient(Connection connection, Map<String, Object> event) throws PortalPersistenceException { authPersistence.createClient(connection, event); }
    @Override public void updateClient(Connection connection, Map<String, Object> event) throws PortalPersistenceException { authPersistence.updateClient(connection, event); }
    @Override public void deleteClient(Connection connection, Map<String, Object> event) throws PortalPersistenceException { authPersistence.deleteClient(connection, event); }
    @Override public Result<String> queryClient(int offset, int limit, String filters, String globalFilter, String sorting, boolean active, String hostId) { return authPersistence.queryClient(offset, limit, filters, globalFilter, sorting, active, hostId); }
    @Override public Result<String> queryClientByClientId(String clientId) { return authPersistence.queryClientByClientId(clientId); }
    @Override public Result<String> getClientById(String hostId, String clientId) { return authPersistence.getClientById(hostId, clientId); }
    @Override public Result<String> queryClientByProviderClientId(String providerId, String clientId) { return authPersistence.queryClientByProviderClientId(providerId, clientId); }
    @Override public Result<String> queryClientByHostAppId(String host_id, String appId) { return authPersistence.queryClientByHostAppId(host_id, appId); }
    @Override public Result<String> getClientIdLabel(String hostId) { return authPersistence.getClientIdLabel(hostId); }
    @Override public void createAuthProvider(Connection connection, Map<String, Object> event) throws PortalPersistenceException { authPersistence.createAuthProvider(connection, event); }
    @Override public void updateAuthProvider(Connection connection, Map<String, Object> event) throws PortalPersistenceException { authPersistence.updateAuthProvider(connection, event); }
    @Override public void deleteAuthProvider(Connection connection, Map<String, Object> event) throws PortalPersistenceException { authPersistence.deleteAuthProvider(connection, event); }
    @Override public void rotateAuthProvider(Connection connection, Map<String, Object> event) throws PortalPersistenceException { authPersistence.rotateAuthProvider(connection, event); }
    @Override public Result<Map<String, Object>> queryProviderById(String providerId) { return authPersistence.queryProviderById(providerId); }
    @Override public Result<String> getProviderIdLabel(String hostId) { return authPersistence.getProviderIdLabel(hostId); }
    @Override public String queryProviderByName(String hostId, String providerName) { return authPersistence.queryProviderByName(hostId, providerName); }
    @Override public Result<String> queryProvider(int offset, int limit, String filters, String globalFilter, String sorting, boolean active, String hostId) { return authPersistence.queryProvider(offset, limit, filters, globalFilter, sorting, active, hostId); }
    @Override public Result<String> queryProviderKey(String providerId) { return authPersistence.queryProviderKey(providerId); }
    @Override public Result<Map<String, Object>> queryCurrentProviderKey(String providerId) { return authPersistence.queryCurrentProviderKey(providerId); }
    @Override public Result<Map<String, Object>> queryLongLiveProviderKey(String providerId) { return authPersistence.queryLongLiveProviderKey(providerId); }
    @Override public void createAuthProviderApi(Connection conn, Map<String, Object> event) throws PortalPersistenceException { authPersistence.createAuthProviderApi(conn, event); }
    @Override public void updateAuthProviderApi(Connection conn, Map<String, Object> event) throws PortalPersistenceException { authPersistence.updateAuthProviderApi(conn, event); }
    @Override public void deleteAuthProviderApi(Connection conn, Map<String, Object> event) throws PortalPersistenceException { authPersistence.deleteAuthProviderApi(conn, event); }
    @Override public Result<String> queryAuthProviderApi(int offset, int limit, String filters, String globalFilter, String sorting, boolean active, String hostId) { return authPersistence.queryAuthProviderApi(offset, limit, filters, globalFilter, sorting, active, hostId); }

    @Override public void createAuthProviderClient(Connection conn, Map<String, Object> event) throws PortalPersistenceException { authPersistence.createAuthProviderClient(conn, event); }
    @Override public void updateAuthProviderClient(Connection conn, Map<String, Object> event) throws PortalPersistenceException { authPersistence.updateAuthProviderClient(conn, event); }
    @Override public void deleteAuthProviderClient(Connection conn, Map<String, Object> event) throws PortalPersistenceException { authPersistence.deleteAuthProviderClient(conn, event); }
    @Override public Result<String> queryAuthProviderClient(int offset, int limit, String filters, String globalFilter, String sorting, boolean active, String hostId) { return authPersistence.queryAuthProviderClient(offset, limit, filters, globalFilter, sorting, active, hostId); }

    @Override public void createAuthCode(Connection connection, Map<String, Object> event) throws PortalPersistenceException { authPersistence.createAuthCode(connection, event); }
    @Override public void deleteAuthCode(Connection connection, Map<String, Object> event) throws PortalPersistenceException { authPersistence.deleteAuthCode(connection, event); }
    @Override public Result<String> queryAuthCode(String authCode) { return authPersistence.queryAuthCode(authCode); }
    @Override public Result<String> getAuthCode(int offset, int limit, String filters, String globalFilter, String sorting, boolean active, String hostId) { return authPersistence.getAuthCode(offset, limit, filters, globalFilter, sorting, active, hostId); }
    @Override public void createRefreshToken(Connection connection, Map<String, Object> event) throws PortalPersistenceException { authPersistence.createRefreshToken(connection, event); }
    @Override public void deleteRefreshToken(Connection connection, Map<String, Object> event) throws PortalPersistenceException { authPersistence.deleteRefreshToken(connection, event); }
    @Override public Result<String> getRefreshToken(int offset, int limit, String filters, String globalFilter, String sorting, boolean active, String hostId) { return authPersistence.getRefreshToken(offset, limit, filters, globalFilter, sorting, active, hostId); }
    @Override public Result<String> queryRefreshToken(String refreshToken) { return authPersistence.queryRefreshToken(refreshToken); }

    @Override public void createRefToken(Connection connection, Map<String, Object> event) throws PortalPersistenceException {authPersistence.createRefToken(connection, event); }
    @Override public void deleteRefToken(Connection connection, Map<String, Object> event) throws PortalPersistenceException { authPersistence.deleteRefToken(connection, event); }
    @Override public Result<String> getRefToken(int offset, int limit, String filters, String globalFilter, String sorting, boolean active, String hostId) { return authPersistence.getRefToken(offset, limit, filters, globalFilter, sorting, active, hostId); }
    @Override public Result<String> queryRefToken(String refToken) {return authPersistence.queryRefToken(refToken); };

    // --- ApiService ---
    @Override public void createApi(Connection connection, Map<String, Object> event) throws PortalPersistenceException { apiServicePersistence.createApi(connection, event); }
    @Override public void updateApi(Connection connection, Map<String, Object> event) throws PortalPersistenceException { apiServicePersistence.updateApi(connection, event); }
    @Override public void deleteApi(Connection connection, Map<String, Object> event) throws PortalPersistenceException { apiServicePersistence.deleteApi(connection, event); }
    @Override public Result<String> queryApi(int offset, int limit, String filters, String globalFilter, String sorting, boolean active, String hostId) { return apiServicePersistence.queryApi(offset, limit, filters, globalFilter, sorting, active, hostId); }
    @Override public Result<String> queryApiLabel(String hostId) { return apiServicePersistence.queryApiLabel(hostId); }
    @Override public Result<String> getApiById(String hostId, String apiId) { return apiServicePersistence.getApiById(hostId, apiId); }
    @Override public Result<String> getApiVersionById(String hostId, String apiVersionId) { return apiServicePersistence.getApiVersionById(hostId, apiVersionId); }
    @Override public String queryApiVersionId(String hostId, String apiId, String apiVersion) { return apiServicePersistence.queryApiVersionId(hostId, apiId, apiVersion); }
    @Override public Map<String, Object> getEndpointIdMap(String hostId, String apiVersionId) { return apiServicePersistence.getEndpointIdMap(hostId, apiVersionId); }

    @Override public void createApiVersion(Connection connection, Map<String, Object> event) throws PortalPersistenceException { apiServicePersistence.createApiVersion(connection, event); }
    @Override public void updateApiVersion(Connection connection, Map<String, Object> event) throws PortalPersistenceException { apiServicePersistence.updateApiVersion(connection, event); }
    @Override public void deleteApiVersion(Connection connection, Map<String, Object> event) throws PortalPersistenceException { apiServicePersistence.deleteApiVersion(connection, event); }
    @Override public Result<String> queryApiVersion(String hostId, String apiId) { return apiServicePersistence.queryApiVersion(hostId, apiId); }
    @Override public Result<String> getApiVersionIdLabel(String hostId) { return apiServicePersistence.getApiVersionIdLabel(hostId); }
    @Override public Result<String> queryApiVersionLabel(String hostId, String apiId) { return apiServicePersistence.queryApiVersionLabel(hostId, apiId); }
    @Override public void updateApiVersionSpec(Connection connection, Map<String, Object> event) throws PortalPersistenceException { apiServicePersistence.updateApiVersionSpec(connection, event); }
    @Override public Result<String> queryApiEndpoint(int offset, int limit, String filters, String globalFilter, String sorting, boolean active, String hostId) { return apiServicePersistence.queryApiEndpoint(offset, limit, filters, globalFilter, sorting, active, hostId); }
    @Override public Result<String> queryEndpointLabel(String hostId, String apiVersionId) { return apiServicePersistence.queryEndpointLabel(hostId, apiVersionId); }
    @Override public Result<String> queryApiEndpointScope(String hostId, String endpointId) { return apiServicePersistence.queryApiEndpointScope(hostId, endpointId); }
    @Override public void createApiEndpointRule(Connection connection, Map<String, Object> event) throws PortalPersistenceException { apiServicePersistence.createApiEndpointRule(connection, event); }
    @Override public void deleteApiEndpointRule(Connection connection, Map<String, Object> event) throws PortalPersistenceException { apiServicePersistence.deleteApiEndpointRule(connection, event); }
    @Override public Result<String> queryApiEndpointRule(String hostId, String endpointId) { return apiServicePersistence.queryApiEndpointRule(hostId, endpointId); }
    @Override public Result<String> queryServiceRule(String hostId, String apiId, String apiVersion) { return apiServicePersistence.queryServiceRule(hostId, apiId, apiVersion); }

    @Override public Result<String> queryApiPermission(String hostId, String apiId, String apiVersion) { return apiServicePersistence.queryApiPermission(hostId, apiId, apiVersion); }
    @Override public Result<List<String>> queryApiFilter(String hostId, String apiId, String apiVersion) { return apiServicePersistence.queryApiFilter(hostId, apiId, apiVersion); }
    @Override public Result<String> getServiceIdLabel(String hostId) { return apiServicePersistence.getServiceIdLabel(hostId); }

    // --- HostOrg ---
    @Override public void createOrg(Connection connection, Map<String, Object> event) throws PortalPersistenceException { hostOrgPersistence.createOrg(connection, event); }
    @Override public void updateOrg(Connection connection, Map<String, Object> event) throws PortalPersistenceException { hostOrgPersistence.updateOrg(connection, event); }
    @Override public void deleteOrg(Connection connection, Map<String, Object> event) throws PortalPersistenceException { hostOrgPersistence.deleteOrg(connection, event); }
    @Override public Result<String> getOrg(int offset, int limit, String filters, String globalFilter, String sorting, boolean active) { return hostOrgPersistence.getOrg(offset, limit, filters, globalFilter, sorting, active); }
    @Override public void createHost(Connection connection, Map<String, Object> event) throws PortalPersistenceException { hostOrgPersistence.createHost(connection, event); }
    @Override public void updateHost(Connection connection, Map<String, Object> event) throws PortalPersistenceException { hostOrgPersistence.updateHost(connection, event); }
    @Override public void deleteHost(Connection connection, Map<String, Object> event) throws PortalPersistenceException { hostOrgPersistence.deleteHost(connection, event); }
    @Override public void switchUserHost(Connection connection, Map<String, Object> event) throws PortalPersistenceException { hostOrgPersistence.switchUserHost(connection, event); }
    @Override public void createUserHost(Connection connection, Map<String, Object> event) throws PortalPersistenceException { hostOrgPersistence.createUserHost(connection, event); }
    @Override public void deleteUserHost(Connection connection, Map<String, Object> event) throws PortalPersistenceException { hostOrgPersistence.deleteUserHost(connection, event); }
    @Override public Result<String> queryHostDomainById(String hostId) { return hostOrgPersistence.queryHostDomainById(hostId); }
    @Override public Result<String> queryHostById(String id) { return hostOrgPersistence.queryHostById(id); }
    @Override public Result<Map<String, Object>> queryHostByOwner(String owner) { return hostOrgPersistence.queryHostByOwner(owner); }
    @Override public Result<String> getHost(int offset, int limit, String filters, String globalFilter, String sorting, boolean active) { return hostOrgPersistence.getHost(offset, limit, filters, globalFilter, sorting, active); }
    @Override public Result<String> getUserHost(int offset, int limit, String filters, String globalFilter, String sorting, boolean active) { return hostOrgPersistence.getUserHost(offset, limit, filters, globalFilter, sorting, active); }
    @Override public Result<String> getHostByDomain(String domain, String subDomain, String hostDesc) { return hostOrgPersistence.getHostByDomain(domain, subDomain, hostDesc); }
    @Override public Result<String> getHostLabel() { return hostOrgPersistence.getHostLabel(); }
    @Override public Result<String> getOrgByDomain(String domain) { return hostOrgPersistence.getOrgByDomain(domain); }
    @Override public String getHostId(String domain, String subDomain) { return hostOrgPersistence.getHostId(domain, subDomain); }

    // --- Config ---
    @Override public void createConfig(Connection connection, Map<String, Object> event) throws PortalPersistenceException { configPersistence.createConfig(connection, event); }
    @Override public void updateConfig(Connection connection, Map<String, Object> event) throws PortalPersistenceException { configPersistence.updateConfig(connection, event); }
    @Override public void deleteConfig(Connection connection, Map<String, Object> event) throws PortalPersistenceException { configPersistence.deleteConfig(connection, event); }
    @Override public Result<String> getConfig(int offset, int limit, String filters, String globalFilter, String sorting, boolean active) { return configPersistence.getConfig(offset, limit, filters, globalFilter, sorting, active); }
    @Override public Result<String> queryConfigById(String configId) { return configPersistence.queryConfigById(configId); }
    @Override public Result<String> getConfigIdLabel() { return configPersistence.getConfigIdLabel(); }
    @Override public String queryConfigId(String configName) { return configPersistence.queryConfigId(configName); }
    @Override public Result<String> getConfigIdApiAppLabel(String resourceType) { return configPersistence.getConfigIdApiAppLabel(resourceType); }
    @Override public void createConfigProperty(Connection connection, Map<String, Object> event) throws PortalPersistenceException { configPersistence.createConfigProperty(connection, event); }
    @Override public void updateConfigProperty(Connection connection, Map<String, Object> event) throws PortalPersistenceException { configPersistence.updateConfigProperty(connection, event); }
    @Override public void deleteConfigProperty(Connection connection, Map<String, Object> event) throws PortalPersistenceException { configPersistence.deleteConfigProperty(connection, event); }
    @Override public Result<String> getConfigProperty(int offset, int limit, String filters, String globalFilter, String sorting, boolean active) { return configPersistence.getConfigProperty(offset, limit, filters, globalFilter, sorting, active); }
    @Override public Result<String> queryConfigPropertyById(String configId) { return configPersistence.queryConfigPropertyById(configId); }
    @Override public String queryPropertyId(String configName, String propertyName) { return configPersistence.queryPropertyId(configName, propertyName); }
    @Override public Result<String> getPropertyById(String propertyId) { return configPersistence.getPropertyById(propertyId); }
    @Override public String getPropertyId(String configId, String propertyName) { return configPersistence.getPropertyId(configId, propertyName); }
    @Override public Result<String> queryConfigPropertyByPropertyId(String configId, String propertyId) { return configPersistence.queryConfigPropertyByPropertyId(configId, propertyId); }
    @Override public Result<String> getPropertyIdLabel(String configId) { return configPersistence.getPropertyIdLabel(configId); }
    @Override public Result<String> getPropertyIdApiAppLabel(String configId, String resourceType) { return configPersistence.getPropertyIdApiAppLabel(configId, resourceType); }
    @Override public void createConfigEnvironment(Connection connection, Map<String, Object> event) throws PortalPersistenceException { configPersistence.createConfigEnvironment(connection, event); }
    @Override public void updateConfigEnvironment(Connection connection, Map<String, Object> event) throws PortalPersistenceException { configPersistence.updateConfigEnvironment(connection, event); }
    @Override public void deleteConfigEnvironment(Connection connection, Map<String, Object> event) throws PortalPersistenceException { configPersistence.deleteConfigEnvironment(connection, event); }
    @Override public Result<String> getConfigEnvironment(int offset, int limit, String filters, String globalFilter, String sorting, boolean active, String hostId) { return configPersistence.getConfigEnvironment(offset, limit, filters, globalFilter, sorting, active, hostId); }
    @Override public Result<String> getConfigEnvironmentById(String hostId, String environmentId, String propertyId) { return configPersistence.getConfigEnvironmentById(hostId, environmentId, propertyId); }
    @Override public void createConfigInstance(Connection connection, Map<String, Object> event) throws PortalPersistenceException { configPersistence.createConfigInstance(connection, event); }
    @Override public void updateConfigInstance(Connection connection, Map<String, Object> event) throws PortalPersistenceException { configPersistence.updateConfigInstance(connection, event); }
    @Override public void deleteConfigInstance(Connection connection, Map<String, Object> event) throws PortalPersistenceException { configPersistence.deleteConfigInstance(connection, event); }
    @Override public Result<String> getConfigInstance(int offset, int limit, String filters, String globalFilter, String sorting, boolean active, String hostId) { return configPersistence.getConfigInstance(offset, limit, filters, globalFilter, sorting, active, hostId); }
    @Override public Result<String> getConfigInstanceById(String hostId, String instanceId, String propertyId) { return configPersistence.getConfigInstanceById(hostId, instanceId, propertyId); }
    @Override public void createConfigInstanceApi(Connection connection, Map<String, Object> event) throws PortalPersistenceException { configPersistence.createConfigInstanceApi(connection, event); }
    @Override public void updateConfigInstanceApi(Connection connection, Map<String, Object> event) throws PortalPersistenceException { configPersistence.updateConfigInstanceApi(connection, event); }
    @Override public void deleteConfigInstanceApi(Connection connection, Map<String, Object> event) throws PortalPersistenceException { configPersistence.deleteConfigInstanceApi(connection, event); }
    @Override public Result<String> getConfigInstanceApi(int offset, int limit, String filters, String globalFilter, String sorting, boolean active, String hostId) { return configPersistence.getConfigInstanceApi(offset, limit, filters, globalFilter, sorting, active, hostId); }
    @Override public Result<String> getConfigInstanceApiById(String hostId, String instanceApiId, String propertyId) { return configPersistence.getConfigInstanceApiById(hostId, instanceApiId, propertyId); }
    @Override public void createConfigInstanceApp(Connection connection, Map<String, Object> event) throws PortalPersistenceException { configPersistence.createConfigInstanceApp(connection, event); }
    @Override public void updateConfigInstanceApp(Connection connection, Map<String, Object> event) throws PortalPersistenceException { configPersistence.updateConfigInstanceApp(connection, event); }
    @Override public void deleteConfigInstanceApp(Connection connection, Map<String, Object> event) throws PortalPersistenceException { configPersistence.deleteConfigInstanceApp(connection, event); }
    @Override public Result<String> getConfigInstanceApp(int offset, int limit, String filters, String globalFilter, String sorting, boolean active, String hostId) { return configPersistence.getConfigInstanceApp(offset, limit, filters, globalFilter, sorting, active, hostId); }
    @Override public Result<String> getConfigInstanceAppById(String hostId, String instanceAppId, String propertyId) { return configPersistence.getConfigInstanceAppById(hostId, instanceAppId, propertyId); }
    @Override public void createConfigInstanceAppApi(Connection connection, Map<String, Object> event) throws PortalPersistenceException { configPersistence.createConfigInstanceAppApi(connection, event); }
    @Override public void updateConfigInstanceAppApi(Connection connection, Map<String, Object> event) throws PortalPersistenceException { configPersistence.updateConfigInstanceAppApi(connection, event); }
    @Override public void deleteConfigInstanceAppApi(Connection connection, Map<String, Object> event) throws PortalPersistenceException { configPersistence.deleteConfigInstanceAppApi(connection, event); }
    @Override public Result<String> getConfigInstanceAppApi(int offset, int limit, String filters, String globalFilter, String sorting, boolean active, String hostId) { return configPersistence.getConfigInstanceAppApi(offset, limit,filters, globalFilter, sorting, active, hostId); }
    @Override public Result<String> getConfigInstanceAppApiById(String hostId, String instanceAppId, String instanceApiId, String propertyId) { return configPersistence.getConfigInstanceAppApiById(hostId, instanceAppId, instanceApiId, propertyId); }
    @Override public void createConfigDeploymentInstance(Connection connection, Map<String, Object> event) throws PortalPersistenceException { configPersistence.createConfigDeploymentInstance(connection, event); }
    @Override public void updateConfigDeploymentInstance(Connection connection, Map<String, Object> event) throws PortalPersistenceException { configPersistence.updateConfigDeploymentInstance(connection, event); }
    @Override public void deleteConfigDeploymentInstance(Connection connection, Map<String, Object> event) throws PortalPersistenceException { configPersistence.deleteConfigDeploymentInstance(connection, event); }
    @Override public Result<String> getConfigDeploymentInstance(int offset, int limit, String filters, String globalFilter, String sorting, boolean active, String hostId) { return configPersistence.getConfigDeploymentInstance(offset, limit, filters, globalFilter, sorting, active, hostId); }
    @Override public Result<String> getConfigDeploymentInstanceById(String hostId, String deploymentInstanceId, String propertyId) { return configPersistence.getConfigDeploymentInstanceById(hostId, deploymentInstanceId, propertyId); }
    @Override public void createConfigProduct(Connection connection, Map<String, Object> event) throws PortalPersistenceException { configPersistence.createConfigProduct(connection, event); }
    @Override public void updateConfigProduct(Connection connection, Map<String, Object> event) throws PortalPersistenceException { configPersistence.updateConfigProduct(connection, event); }
    @Override public void deleteConfigProduct(Connection connection, Map<String, Object> event) throws PortalPersistenceException { configPersistence.deleteConfigProduct(connection, event); }
    @Override public Result<String> getConfigProduct(int offset, int limit, String filters, String globalFilter, String sorting, boolean active) { return configPersistence.getConfigProduct(offset, limit, filters, globalFilter, sorting, active); }
    @Override public Result<String> getConfigProductById(String productId, String propertyId) { return configPersistence.getConfigProductById(productId, propertyId); }
    @Override public void createConfigProductVersion(Connection connection, Map<String, Object> event) throws PortalPersistenceException { configPersistence.createConfigProductVersion(connection, event); }
    @Override public void updateConfigProductVersion(Connection connection, Map<String, Object> event) throws PortalPersistenceException { configPersistence.updateConfigProductVersion(connection, event); }
    @Override public void deleteConfigProductVersion(Connection connection, Map<String, Object> event) throws PortalPersistenceException { configPersistence.deleteConfigProductVersion(connection, event); }
    @Override public Result<String> getConfigProductVersion(int offset, int limit, String filters, String globalFilter, String sorting, boolean active, String hostId) { return configPersistence.getConfigProductVersion(offset, limit, filters, globalFilter, sorting, active, hostId); }
    @Override public Result<String> getConfigProductVersionById(String hostId, String productVersionId, String propertyId) { return configPersistence.getConfigProductVersionById(hostId, productVersionId, propertyId); }
    @Override public void createConfigInstanceFile(Connection connection, Map<String, Object> event) throws PortalPersistenceException { configPersistence.createConfigInstanceFile(connection, event); }
    @Override public void updateConfigInstanceFile(Connection connection, Map<String, Object> event) throws PortalPersistenceException { configPersistence.updateConfigInstanceFile(connection, event); }
    @Override public void deleteConfigInstanceFile(Connection connection, Map<String, Object> event) throws PortalPersistenceException { configPersistence.deleteConfigInstanceFile(connection, event); }
    @Override public Result<String> getConfigInstanceFile(int offset, int limit, String filters, String globalFilter, String sorting, boolean active, String hostId) { return configPersistence.getConfigInstanceFile(offset, limit, filters, globalFilter, sorting, active, hostId); }
    @Override public Result<String> getConfigInstanceFileById(String hostId, String instanceFileId) { return configPersistence.getConfigInstanceFileById(hostId, instanceFileId); }
    @Override public void createConfigSnapshot(Connection connection, Map<String, Object> event) throws PortalPersistenceException { configPersistence.createConfigSnapshot(connection, event); }
    @Override public void updateConfigSnapshot(Connection connection, Map<String, Object> event) throws PortalPersistenceException { configPersistence.updateConfigSnapshot(connection, event); }
    @Override public void deleteConfigSnapshot(Connection connection, Map<String, Object> event) throws PortalPersistenceException { configPersistence.deleteConfigSnapshot(connection, event); }
    @Override public Result<String> getConfigSnapshot(int offset, int limit, String filters, String globalFilter, String sorting, String hostId) { return configPersistence.getConfigSnapshot(offset, limit, filters, globalFilter, sorting, hostId); }

    // --- InstanceDeployment ---
    @Override public void createInstance(Connection connection, Map<String, Object> event) throws PortalPersistenceException { instanceDeploymentPersistence.createInstance(connection, event); }
    @Override public void updateInstance(Connection connection, Map<String, Object> event) throws PortalPersistenceException { instanceDeploymentPersistence.updateInstance(connection, event); }
    @Override public void deleteInstance(Connection connection, Map<String, Object> event) throws PortalPersistenceException { instanceDeploymentPersistence.deleteInstance(connection, event); }
    @Override public void lockInstance(Connection conn, Map<String, Object> event) throws PortalPersistenceException { instanceDeploymentPersistence.lockInstance(conn, event); }
    @Override public void unlockInstance(Connection conn, Map<String, Object> event) throws PortalPersistenceException { instanceDeploymentPersistence.unlockInstance(conn, event); }
    @Override public void cloneInstance(Connection conn, Map<String, Object> event) throws PortalPersistenceException { instanceDeploymentPersistence.cloneInstance(conn, event); }
    @Override public void promoteInstance(Connection conn, Map<String, Object> event) throws PortalPersistenceException { instanceDeploymentPersistence.promoteInstance(conn, event); }
    @Override public Result<String> getInstance(int offset, int limit, String filters, String globalFilter, String sorting, boolean active, String hostId) { return instanceDeploymentPersistence.getInstance(offset, limit, filters, globalFilter, sorting, active, hostId); }
    @Override public Result<String> getInstanceLabel(String hostId) { return instanceDeploymentPersistence.getInstanceLabel(hostId); }
    @Override public Result<String> getInstanceById(String hostId, String instanceId) { return instanceDeploymentPersistence.getInstanceById(hostId, instanceId); }
    @Override public String getInstanceId(String hostId, String serviceId, String envTag, String productVersionId) { return instanceDeploymentPersistence.getInstanceId(hostId, serviceId, envTag, productVersionId); }

    @Override public void createInstanceApi(Connection connection, Map<String, Object> event) throws PortalPersistenceException { instanceDeploymentPersistence.createInstanceApi(connection, event); }
    @Override public void deleteInstanceApi(Connection connection, Map<String, Object> event) throws PortalPersistenceException { instanceDeploymentPersistence.deleteInstanceApi(connection, event); }
    @Override public Result<String> getInstanceApi(int offset, int limit, String filters, String globalFilter, String sorting, boolean active, String hostId) { return instanceDeploymentPersistence.getInstanceApi(offset, limit, filters, globalFilter, sorting, active, hostId); }
    @Override public Result<String> getInstanceApiLabel(String hostId, String instanceId) { return instanceDeploymentPersistence.getInstanceApiLabel(hostId, instanceId); }
    @Override public Result<String> getInstanceApiById(String hostId, String instanceApiId) { return instanceDeploymentPersistence.getInstanceApiById(hostId, instanceApiId); }
    @Override public String getInstanceApiId(String hostId, String instanceId, String apiVersionId) { return instanceDeploymentPersistence.getInstanceApiId(hostId, instanceId, apiVersionId); }
    @Override public void createInstanceApiPathPrefix(Connection connection, Map<String, Object> event) throws PortalPersistenceException { instanceDeploymentPersistence.createInstanceApiPathPrefix(connection, event); }
    @Override public void updateInstanceApiPathPrefix(Connection connection, Map<String, Object> event) throws PortalPersistenceException { instanceDeploymentPersistence.updateInstanceApiPathPrefix(connection, event); }
    @Override public void deleteInstanceApiPathPrefix(Connection connection, Map<String, Object> event) throws PortalPersistenceException { instanceDeploymentPersistence.deleteInstanceApiPathPrefix(connection, event); }
    @Override public Result<String> getInstanceApiPathPrefix(int offset, int limit, String filters, String globalFilter, String sorting, boolean active, String hostId) { return instanceDeploymentPersistence.getInstanceApiPathPrefix(offset, limit, filters, globalFilter, sorting, active, hostId); }
    @Override public Result<String> getInstanceApiPathPrefixById(String hostId, String instanceApiId, String pathPrefix) { return instanceDeploymentPersistence.getInstanceApiPathPrefixById(hostId, instanceApiId, pathPrefix); }
    @Override public void createInstanceApp(Connection connection, Map<String, Object> event) throws PortalPersistenceException { instanceDeploymentPersistence.createInstanceApp(connection, event); }
    @Override public void deleteInstanceApp(Connection connection, Map<String, Object> event) throws PortalPersistenceException { instanceDeploymentPersistence.deleteInstanceApp(connection, event); }
    @Override public Result<String> getInstanceApp(int offset, int limit, String filters, String globalFilter, String sorting, boolean active, String hostId) { return instanceDeploymentPersistence.getInstanceApp(offset, limit, filters, globalFilter, sorting, active, hostId); }
    @Override public Result<String> getInstanceAppById(String hostId, String instanceAppId) { return instanceDeploymentPersistence.getInstanceAppById(hostId, instanceAppId); }
    @Override public String getInstanceAppId(String hostId, String instanceId, String appId, String appVersion) { return instanceDeploymentPersistence.getInstanceAppId(hostId, instanceId, appId, appVersion); }
    @Override public Result<String> getInstanceAppLabel(String hostId, String instanceId) { return instanceDeploymentPersistence.getInstanceAppLabel(hostId, instanceId); }
    @Override public void createInstanceAppApi(Connection connection, Map<String, Object> event) throws PortalPersistenceException { instanceDeploymentPersistence.createInstanceAppApi(connection, event); }
    @Override public void deleteInstanceAppApi(Connection connection, Map<String, Object> event) throws PortalPersistenceException { instanceDeploymentPersistence.deleteInstanceAppApi(connection, event); }
    @Override public Result<String> getInstanceAppApi(int offset, int limit, String filters, String globalFilter, String sorting, boolean active, String hostId) { return instanceDeploymentPersistence.getInstanceAppApi(offset, limit, filters, globalFilter, sorting, active, hostId); }
    @Override public Result<String> getInstanceAppApiById(String hostId, String instanceAppId, String instanceApiId) { return instanceDeploymentPersistence.getInstanceAppApiById(hostId, instanceAppId, instanceApiId); }
    @Override public void createProduct(Connection connection, Map<String, Object> event) throws PortalPersistenceException { instanceDeploymentPersistence.createProduct(connection, event); }
    @Override public void updateProduct(Connection connection, Map<String, Object> event) throws PortalPersistenceException { instanceDeploymentPersistence.updateProduct(connection, event); }
    @Override public void deleteProduct(Connection connection, Map<String, Object> event) throws PortalPersistenceException { instanceDeploymentPersistence.deleteProduct(connection, event); }
    @Override public Result<String> getProduct(int offset, int limit, String filters, String globalFilter, String sorting, boolean active, String hostId) { return instanceDeploymentPersistence.getProduct(offset, limit, filters, globalFilter, sorting, active, hostId); }
    @Override public Result<String> getProductVersion(String hostId, String productVersionId) {return instanceDeploymentPersistence.getProductVersion(hostId, productVersionId); }
    @Override public Result<String> getProductIdLabel(String hostId) { return instanceDeploymentPersistence.getProductIdLabel(hostId); }
    @Override public Result<String> getProductVersionLabel(String hostId, String productId) { return instanceDeploymentPersistence.getProductVersionLabel(hostId, productId); }
    @Override public Result<String> getProductVersionIdLabel(String hostId) { return instanceDeploymentPersistence.getProductVersionIdLabel(hostId); }
    @Override public String getProductVersionId(String hostId, String productId, String productVersion) { return instanceDeploymentPersistence.getProductVersionId(hostId, productId, productVersion); }
    @Override public String queryProductVersionId(String hostId, String productId, String light4jVersion) { return instanceDeploymentPersistence.queryProductVersionId(hostId, productId, light4jVersion); };
    @Override public void createProductVersionEnvironment(Connection connection, Map<String, Object> event) throws PortalPersistenceException { instanceDeploymentPersistence.createProductVersionEnvironment(connection, event); }
    @Override public void updateProductVersionEnvironment(Connection conn, Map<String, Object> event) throws PortalPersistenceException { instanceDeploymentPersistence.updateProductVersionEnvironment(conn, event); }
    @Override public void deleteProductVersionEnvironment(Connection connection, Map<String, Object> event) throws PortalPersistenceException { instanceDeploymentPersistence.deleteProductVersionEnvironment(connection, event); }
    @Override public Result<String> getProductVersionEnvironment(int offset, int limit, String filters, String globalFilter, String sorting, boolean active, String hostId) { return instanceDeploymentPersistence.getProductVersionEnvironment(offset, limit, filters, globalFilter, sorting, active, hostId); }
    @Override public Result<String> getProductVersionEnvironmentById(String hostId, String productVersionId, String systemEnv, String runtimeEnv) { return instanceDeploymentPersistence.getProductVersionEnvironmentById(hostId, productVersionId, systemEnv, runtimeEnv); }
    @Override public void createProductVersionPipeline(Connection connection, Map<String, Object> event) throws PortalPersistenceException { instanceDeploymentPersistence.createProductVersionPipeline(connection, event); }
    @Override public void deleteProductVersionPipeline(Connection connection, Map<String, Object> event) throws PortalPersistenceException { instanceDeploymentPersistence.deleteProductVersionPipeline(connection, event); }
    @Override public Result<String> getProductVersionPipeline(int offset, int limit, String filters, String globalFilter, String sorting, boolean active, String hostId) { return instanceDeploymentPersistence.getProductVersionPipeline(offset, limit, filters, globalFilter, sorting, active, hostId); }
    @Override public void createProductVersionConfig(Connection connection, Map<String, Object> event) throws PortalPersistenceException { instanceDeploymentPersistence.createProductVersionConfig(connection, event); }
    @Override public void deleteProductVersionConfig(Connection connection, Map<String, Object> event) throws PortalPersistenceException { instanceDeploymentPersistence.deleteProductVersionConfig(connection, event); }
    @Override public Result<String> getProductVersionConfig(int offset, int limit, String filters, String globalFilter, String sorting, boolean active, String hostId) { return instanceDeploymentPersistence.getProductVersionConfig(offset, limit, filters, globalFilter, sorting, active, hostId); }
    @Override public void createProductVersionConfigProperty(Connection connection, Map<String, Object> event) throws PortalPersistenceException { instanceDeploymentPersistence.createProductVersionConfigProperty(connection, event); }
    @Override public void deleteProductVersionConfigProperty(Connection connection, Map<String, Object> event) throws PortalPersistenceException { instanceDeploymentPersistence.deleteProductVersionConfigProperty(connection, event); }
    @Override public Result<String> getProductVersionConfigProperty(int offset, int limit, String filters, String globalFilter, String sorting, boolean active, String hostId) { return instanceDeploymentPersistence.getProductVersionConfigProperty(offset, limit, filters, globalFilter, sorting, active, hostId); }
    @Override public void createPipeline(Connection connection, Map<String, Object> event) throws PortalPersistenceException { instanceDeploymentPersistence.createPipeline(connection, event); }
    @Override public void updatePipeline(Connection connection, Map<String, Object> event) throws PortalPersistenceException { instanceDeploymentPersistence.updatePipeline(connection, event); }
    @Override public void deletePipeline(Connection connection, Map<String, Object> event) throws PortalPersistenceException { instanceDeploymentPersistence.deletePipeline(connection, event); }
    @Override public Result<String> getPipeline(int offset, int limit, String filters, String globalFilter, String sorting, boolean active, String hostId) { return instanceDeploymentPersistence.getPipeline(offset, limit, filters, globalFilter, sorting, active, hostId); }
    @Override public Result<String> getPipelineLabel(String hostId) { return instanceDeploymentPersistence.getPipelineLabel(hostId); }
    @Override public Result<String> getPipelineById(String hostId, String pipelineId) { return instanceDeploymentPersistence.getPipelineById(hostId, pipelineId); }
    @Override public String getPipelineId(String hostId, String platformId, String pipelineName, String pipelineVersion) { return instanceDeploymentPersistence.getPipelineId(hostId, platformId, pipelineName, pipelineVersion); }
    @Override public void createPlatform(Connection connection, Map<String, Object> event) throws PortalPersistenceException { instanceDeploymentPersistence.createPlatform(connection, event); }
    @Override public void updatePlatform(Connection connection, Map<String, Object> event) throws PortalPersistenceException { instanceDeploymentPersistence.updatePlatform(connection, event); }
    @Override public void deletePlatform(Connection connection, Map<String, Object> event) throws PortalPersistenceException { instanceDeploymentPersistence.deletePlatform(connection, event); }
    @Override public Result<String> getPlatform(int offset, int limit, String filters, String globalFilter, String sorting, boolean active, String hostId) { return instanceDeploymentPersistence.getPlatform(offset, limit, filters, globalFilter, sorting, active, hostId); }
    @Override public Result<String> getPlatformLabel(String hostId) { return instanceDeploymentPersistence.getPlatformLabel(hostId); }
    @Override public Result<String> getPlatformById(String hostId, String platformId) { return instanceDeploymentPersistence.getPlatformById(hostId, platformId); }
    @Override public String getPlatformId(String hostId, String platformName, String platformVersion) { return instanceDeploymentPersistence.getPlatformId(hostId, platformName, platformVersion); }

    @Override public void createDeploymentInstance(Connection connection, Map<String, Object> event) throws PortalPersistenceException { instanceDeploymentPersistence.createDeploymentInstance(connection, event); }
    @Override public void updateDeploymentInstance(Connection connection, Map<String, Object> event) throws PortalPersistenceException { instanceDeploymentPersistence.updateDeploymentInstance(connection, event); }
    @Override public void deleteDeploymentInstance(Connection connection, Map<String, Object> event) throws PortalPersistenceException { instanceDeploymentPersistence.deleteDeploymentInstance(connection, event); }
    @Override public Result<String> getDeploymentInstance(int offset, int limit, String filters, String globalFilter, String sorting, boolean active, String hostId) { return instanceDeploymentPersistence.getDeploymentInstance(offset, limit, filters, globalFilter, sorting, active, hostId); }
    @Override public Result<String> getDeploymentInstanceById(String hostId, String deploymentInstanceId) { return instanceDeploymentPersistence.getDeploymentInstanceById(hostId, deploymentInstanceId); }
    @Override public Result<String> getDeploymentInstancePipeline(String hostId, String instanceId, String systemEnv, String runtimeEnv) { return instanceDeploymentPersistence.getDeploymentInstancePipeline(hostId, instanceId, systemEnv, runtimeEnv); }
    @Override public Result<String> getDeploymentInstanceLabel(String hostId, String instanceId) { return instanceDeploymentPersistence.getDeploymentInstanceLabel(hostId, instanceId); }
    @Override public String getDeploymentInstanceId(String hostId, String instanceId, String serviceId) { return instanceDeploymentPersistence.getDeploymentInstanceId(hostId, instanceId, serviceId); }
    @Override public void createDeployment(Connection connection, Map<String, Object> event) throws PortalPersistenceException { instanceDeploymentPersistence.createDeployment(connection, event); }
    @Override public void updateDeployment(Connection connection, Map<String, Object> event) throws PortalPersistenceException { instanceDeploymentPersistence.updateDeployment(connection, event); }
    @Override public void updateDeploymentJobId(Connection connection, Map<String, Object> event) throws PortalPersistenceException { instanceDeploymentPersistence.updateDeploymentJobId(connection, event); }
    @Override public void updateDeploymentStatus(Connection connection, Map<String, Object> event) throws PortalPersistenceException { instanceDeploymentPersistence.updateDeploymentStatus(connection, event); }
    @Override public void deleteDeployment(Connection connection, Map<String, Object> event) throws PortalPersistenceException { instanceDeploymentPersistence.deleteDeployment(connection, event); }
    @Override public Result<String> getDeployment(int offset, int limit, String filters, String globalFilter, String sorting, boolean active, String hostId) { return instanceDeploymentPersistence.getDeployment(offset, limit, filters, globalFilter, sorting, active, hostId); }
    @Override public Result<String> getDeploymentById(String hostId, String deploymentId) { return instanceDeploymentPersistence.getDeploymentById(hostId, deploymentId); }

    // --- AccessControl ---
    @Override public void createRole(Connection connection, Map<String, Object> event) throws PortalPersistenceException { accessControlPersistence.createRole(connection, event); }
    @Override public void updateRole(Connection connection, Map<String, Object> event) throws PortalPersistenceException { accessControlPersistence.updateRole(connection, event); }
    @Override public void deleteRole(Connection connection, Map<String, Object> event) throws PortalPersistenceException { accessControlPersistence.deleteRole(connection, event); }
    @Override public Result<String> queryRole(int offset, int limit, String filters, String globalFilter, String sorting, boolean active, String hostId) { return accessControlPersistence.queryRole(offset, limit, filters, globalFilter, sorting, active, hostId); }
    @Override public Result<String> queryRoleLabel(String hostId) { return accessControlPersistence.queryRoleLabel(hostId); }
    @Override public Result<String> getRoleById(String hostId, String roleId) { return accessControlPersistence.getRoleById(hostId, roleId); }
    @Override public void createRolePermission(Connection connection, Map<String, Object> event) throws PortalPersistenceException { accessControlPersistence.createRolePermission(connection, event); }
    @Override public void deleteRolePermission(Connection connection, Map<String, Object> event) throws PortalPersistenceException { accessControlPersistence.deleteRolePermission(connection, event); }
    @Override public Result<String> queryRolePermission(int offset, int limit, String filters, String globalFilter, String sorting, boolean active, String hostId) { return accessControlPersistence.queryRolePermission(offset, limit, filters, globalFilter, sorting, active, hostId); }
    @Override public void createRoleUser(Connection connection, Map<String, Object> event) throws PortalPersistenceException { accessControlPersistence.createRoleUser(connection, event); }
    @Override public void updateRoleUser(Connection connection, Map<String, Object> event) throws PortalPersistenceException { accessControlPersistence.updateRoleUser(connection, event); }
    @Override public void deleteRoleUser(Connection connection, Map<String, Object> event) throws PortalPersistenceException { accessControlPersistence.deleteRoleUser(connection, event); }
    @Override public Result<String> queryRoleUser(int offset, int limit, String filters, String globalFilter, String sorting, boolean active, String hostId) { return accessControlPersistence.queryRoleUser(offset, limit, filters, globalFilter, sorting, active, hostId); }
    @Override public Result<String> getRoleUserById(String hostId, String roleId, String userId) { return accessControlPersistence.getRoleUserById(hostId, roleId, userId); }
    @Override public void createRoleRowFilter(Connection connection, Map<String, Object> event) throws PortalPersistenceException { accessControlPersistence.createRoleRowFilter(connection, event); }
    @Override public void updateRoleRowFilter(Connection connection, Map<String, Object> event) throws PortalPersistenceException { accessControlPersistence.updateRoleRowFilter(connection, event); }
    @Override public void deleteRoleRowFilter(Connection connection, Map<String, Object> event) throws PortalPersistenceException { accessControlPersistence.deleteRoleRowFilter(connection, event); }
    @Override public Result<String> queryRoleRowFilter(int offset, int limit, String filters, String globalFilter, String sorting, boolean active, String hostId) { return accessControlPersistence.queryRoleRowFilter(offset, limit, filters, globalFilter, sorting, active, hostId); }
    @Override public Result<String> getRoleRowFilterById(String hostId, String roleId, String endpointId, String colName) { return accessControlPersistence.getRoleRowFilterById(hostId, roleId, endpointId, colName); }
    @Override public void createRoleColFilter(Connection connection, Map<String, Object> event) throws PortalPersistenceException { accessControlPersistence.createRoleColFilter(connection, event); }
    @Override public void updateRoleColFilter(Connection connection, Map<String, Object> event) throws PortalPersistenceException { accessControlPersistence.updateRoleColFilter(connection, event); }
    @Override public void deleteRoleColFilter(Connection connection, Map<String, Object> event) throws PortalPersistenceException { accessControlPersistence.deleteRoleColFilter(connection, event); }
    @Override public Result<String> queryRoleColFilter(int offset, int limit, String filters, String globalFilter, String sorting, boolean active, String hostId) { return accessControlPersistence.queryRoleColFilter(offset, limit, filters, globalFilter, sorting, active, hostId); }
    @Override public Result<String> getRoleColFilterById(String hostId, String roleId, String endpointId) { return accessControlPersistence.getRoleColFilterById(hostId, roleId, endpointId); }
    @Override public void createGroup(Connection connection, Map<String, Object> event) throws PortalPersistenceException { accessControlPersistence.createGroup(connection, event); }
    @Override public void updateGroup(Connection connection, Map<String, Object> event) throws PortalPersistenceException { accessControlPersistence.updateGroup(connection, event); }
    @Override public void deleteGroup(Connection connection, Map<String, Object> event) throws PortalPersistenceException { accessControlPersistence.deleteGroup(connection, event); }
    @Override public Result<String> queryGroup(int offset, int limit, String filters, String globalFilter, String sorting, boolean active, String hostId) { return accessControlPersistence.queryGroup(offset, limit, filters, globalFilter, sorting, active, hostId); }
    @Override public Result<String> queryGroupLabel(String hostId) { return accessControlPersistence.queryGroupLabel(hostId); }
    @Override public Result<String> getGroupById(String hostId, String groupId) { return accessControlPersistence.getGroupById(hostId, groupId); }
    @Override public void createGroupPermission(Connection connection, Map<String, Object> event) throws PortalPersistenceException { accessControlPersistence.createGroupPermission(connection, event); }
    @Override public void deleteGroupPermission(Connection connection, Map<String, Object> event) throws PortalPersistenceException { accessControlPersistence.deleteGroupPermission(connection, event); }
    @Override public Result<String> queryGroupPermission(int offset, int limit, String filters, String globalFilter, String sorting, boolean active, String hostId) { return accessControlPersistence.queryGroupPermission(offset, limit, filters, globalFilter, sorting, active, hostId); }
    @Override public void createGroupUser(Connection connection, Map<String, Object> event) throws PortalPersistenceException { accessControlPersistence.createGroupUser(connection, event); }
    @Override public void updateGroupUser(Connection connection, Map<String, Object> event) throws PortalPersistenceException { accessControlPersistence.updateGroupUser(connection, event); }
    @Override public void deleteGroupUser(Connection connection, Map<String, Object> event) throws PortalPersistenceException { accessControlPersistence.deleteGroupUser(connection, event); }
    @Override public Result<String> queryGroupUser(int offset, int limit, String filters, String globalFilter, String sorting, boolean active, String hostId) { return accessControlPersistence.queryGroupUser(offset, limit, filters, globalFilter, sorting, active, hostId); }
    @Override public Result<String> getGroupUserById(String hostId, String groupId, String userId) { return accessControlPersistence.getGroupUserById(hostId, groupId, userId); }
    @Override public void createGroupRowFilter(Connection connection, Map<String, Object> event) throws PortalPersistenceException { accessControlPersistence.createGroupRowFilter(connection, event); }
    @Override public void updateGroupRowFilter(Connection connection, Map<String, Object> event) throws PortalPersistenceException { accessControlPersistence.updateGroupRowFilter(connection, event); }
    @Override public void deleteGroupRowFilter(Connection connection, Map<String, Object> event) throws PortalPersistenceException { accessControlPersistence.deleteGroupRowFilter(connection, event); }
    @Override public Result<String> queryGroupRowFilter(int offset, int limit, String filters, String globalFilter, String sorting, boolean active, String hostId) { return accessControlPersistence.queryGroupRowFilter(offset, limit, filters, globalFilter, sorting, active, hostId); }
    @Override public Result<String> getGroupRowFilterById(String hostId, String groupId, String endpointId, String colName) { return accessControlPersistence.getGroupRowFilterById(hostId, groupId, endpointId, colName); }
    @Override public void createGroupColFilter(Connection connection, Map<String, Object> event) throws PortalPersistenceException { accessControlPersistence.createGroupColFilter(connection, event); }
    @Override public void updateGroupColFilter(Connection connection, Map<String, Object> event) throws PortalPersistenceException { accessControlPersistence.updateGroupColFilter(connection, event); }
    @Override public void deleteGroupColFilter(Connection connection, Map<String, Object> event) throws PortalPersistenceException { accessControlPersistence.deleteGroupColFilter(connection, event); }
    @Override public Result<String> queryGroupColFilter(int offset, int limit, String filters, String globalFilter, String sorting, boolean active, String hostId) { return accessControlPersistence.queryGroupColFilter(offset, limit, filters, globalFilter, sorting, active, hostId); }
    @Override public Result<String> getGroupColFilterById(String hostId, String groupId, String endpointId) { return accessControlPersistence.getGroupColFilterById(hostId, groupId, endpointId); }
    @Override public void createPosition(Connection connection, Map<String, Object> event) throws PortalPersistenceException { accessControlPersistence.createPosition(connection, event); }
    @Override public void updatePosition(Connection connection, Map<String, Object> event) throws PortalPersistenceException { accessControlPersistence.updatePosition(connection, event); }
    @Override public void deletePosition(Connection connection, Map<String, Object> event) throws PortalPersistenceException { accessControlPersistence.deletePosition(connection, event); }
    @Override public Result<String> queryPosition(int offset, int limit, String filters, String globalFilter, String sorting, boolean active, String hostId) { return accessControlPersistence.queryPosition(offset, limit, filters, globalFilter, sorting, active, hostId); }
    @Override public Result<String> queryPositionLabel(String hostId) { return accessControlPersistence.queryPositionLabel(hostId); }
    @Override public Result<String> getPositionById(String hostId, String positionId) {return accessControlPersistence.getPositionById(hostId, positionId); }
    @Override public void createPositionPermission(Connection connection, Map<String, Object> event) throws PortalPersistenceException { accessControlPersistence.createPositionPermission(connection, event); }
    @Override public void deletePositionPermission(Connection connection, Map<String, Object> event) throws PortalPersistenceException { accessControlPersistence.deletePositionPermission(connection, event); }
    @Override public Result<String> queryPositionPermission(int offset, int limit, String filters, String globalFilter, String sorting, boolean active, String hostId) { return accessControlPersistence.queryPositionPermission(offset, limit, filters, globalFilter, sorting, active, hostId); }
    @Override public void createPositionUser(Connection connection, Map<String, Object> event) throws PortalPersistenceException { accessControlPersistence.createPositionUser(connection, event); }
    @Override public void updatePositionUser(Connection connection, Map<String, Object> event) throws PortalPersistenceException { accessControlPersistence.updatePositionUser(connection, event); }
    @Override public void deletePositionUser(Connection connection, Map<String, Object> event) throws PortalPersistenceException { accessControlPersistence.deletePositionUser(connection, event); }
    @Override public Result<String> queryPositionUser(int offset, int limit, String filters, String globalFilter, String sorting, boolean active, String hostId) { return accessControlPersistence.queryPositionUser(offset, limit, filters, globalFilter, sorting, active, hostId); }
    @Override public Result<String> getPositionUserById(String hostId, String positionId, String employeeId) { return accessControlPersistence.getPositionUserById(hostId, positionId, employeeId); }
    @Override public void createPositionRowFilter(Connection connection, Map<String, Object> event) throws PortalPersistenceException { accessControlPersistence.createPositionRowFilter(connection, event); }
    @Override public void updatePositionRowFilter(Connection connection, Map<String, Object> event) throws PortalPersistenceException { accessControlPersistence.updatePositionRowFilter(connection, event); }
    @Override public void deletePositionRowFilter(Connection connection, Map<String, Object> event) throws PortalPersistenceException { accessControlPersistence.deletePositionRowFilter(connection, event); }
    @Override public Result<String> queryPositionRowFilter(int offset, int limit, String filters, String globalFilter, String sorting, boolean active, String hostId) { return accessControlPersistence.queryPositionRowFilter(offset, limit, filters, globalFilter, sorting, active, hostId); }
    @Override public Result<String> getPositionRowFilterById(String hostId, String positionId, String endpointId, String colName) { return accessControlPersistence.getPositionRowFilterById(hostId, positionId, endpointId, colName); }
    @Override public void createPositionColFilter(Connection connection, Map<String, Object> event) throws PortalPersistenceException { accessControlPersistence.createPositionColFilter(connection, event); }
    @Override public void updatePositionColFilter(Connection connection, Map<String, Object> event) throws PortalPersistenceException { accessControlPersistence.updatePositionColFilter(connection, event); }
    @Override public void deletePositionColFilter(Connection connection, Map<String, Object> event) throws PortalPersistenceException { accessControlPersistence.deletePositionColFilter(connection, event); }
    @Override public Result<String> queryPositionColFilter(int offset, int limit, String filters, String globalFilter, String sorting, boolean active, String hostId) { return accessControlPersistence.queryPositionColFilter(offset, limit, filters, globalFilter, sorting, active, hostId); }
    @Override public Result<String> getPositionColFilterById(String hostId, String positionId, String endpointId) { return accessControlPersistence.getPositionColFilterById(hostId, positionId, endpointId); }
    @Override public void createAttribute(Connection connection, Map<String, Object> event) throws PortalPersistenceException { accessControlPersistence.createAttribute(connection, event); }
    @Override public void updateAttribute(Connection connection, Map<String, Object> event) throws PortalPersistenceException { accessControlPersistence.updateAttribute(connection, event); }
    @Override public void deleteAttribute(Connection connection, Map<String, Object> event) throws PortalPersistenceException { accessControlPersistence.deleteAttribute(connection, event); }
    @Override public Result<String> queryAttribute(int offset, int limit, String filters, String globalFilter, String sorting, boolean active, String hostId) { return accessControlPersistence.queryAttribute(offset, limit, filters, globalFilter, sorting, active, hostId); }
    @Override public Result<String> queryAttributeLabel(String hostId) { return accessControlPersistence.queryAttributeLabel(hostId); }
    @Override public Result<String> getAttributeById(String hostId, String attributeId) { return accessControlPersistence.getAttributeById(hostId, attributeId); }
    @Override public void createAttributePermission(Connection connection, Map<String, Object> event) throws PortalPersistenceException { accessControlPersistence.createAttributePermission(connection, event); }
    @Override public void updateAttributePermission(Connection connection, Map<String, Object> event) throws PortalPersistenceException { accessControlPersistence.updateAttributePermission(connection, event); }
    @Override public void deleteAttributePermission(Connection connection, Map<String, Object> event) throws PortalPersistenceException { accessControlPersistence.deleteAttributePermission(connection, event); }
    @Override public Result<String> queryAttributePermission(int offset, int limit, String filters, String globalFilter, String sorting, boolean active, String hostId) { return accessControlPersistence.queryAttributePermission(offset, limit, filters, globalFilter, sorting, active, hostId); }
    @Override public Result<String> getAttributePermissionById(String hostId, String attributeId, String endpointId) { return accessControlPersistence.getAttributePermissionById(hostId, attributeId, endpointId); }
    @Override public void createAttributeUser(Connection connection, Map<String, Object> event) throws PortalPersistenceException { accessControlPersistence.createAttributeUser(connection, event); }
    @Override public void updateAttributeUser(Connection connection, Map<String, Object> event) throws PortalPersistenceException { accessControlPersistence.updateAttributeUser(connection, event); }
    @Override public void deleteAttributeUser(Connection connection, Map<String, Object> event) throws PortalPersistenceException { accessControlPersistence.deleteAttributeUser(connection, event); }
    @Override public Result<String> queryAttributeUser(int offset, int limit, String filters, String globalFilter, String sorting, boolean active, String hostId) { return accessControlPersistence.queryAttributeUser(offset, limit, filters, globalFilter, sorting, active, hostId); }
    @Override public Result<String> getAttributeUserById(String hostId, String attributeId, String userId) { return accessControlPersistence.getAttributeUserById(hostId, attributeId, userId); }
    @Override public void createAttributeRowFilter(Connection connection, Map<String, Object> event) throws PortalPersistenceException { accessControlPersistence.createAttributeRowFilter(connection, event); }
    @Override public void updateAttributeRowFilter(Connection connection, Map<String, Object> event) throws PortalPersistenceException { accessControlPersistence.updateAttributeRowFilter(connection, event); }
    @Override public void deleteAttributeRowFilter(Connection connection, Map<String, Object> event) throws PortalPersistenceException { accessControlPersistence.deleteAttributeRowFilter(connection, event); }
    @Override public Result<String> queryAttributeRowFilter(int offset, int limit, String filters, String globalFilter, String sorting, boolean active, String hostId) { return accessControlPersistence.queryAttributeRowFilter(offset, limit, filters, globalFilter, sorting, active, hostId); }
    @Override public Result<String> getAttributeRowFilterById(String hostId, String attributeId, String endpointId, String colName) { return accessControlPersistence.getAttributeRowFilterById(hostId, attributeId, endpointId, colName); }
    @Override public void createAttributeColFilter(Connection connection, Map<String, Object> event) throws PortalPersistenceException { accessControlPersistence.createAttributeColFilter(connection, event); }
    @Override public void updateAttributeColFilter(Connection connection, Map<String, Object> event) throws PortalPersistenceException { accessControlPersistence.updateAttributeColFilter(connection, event); }
    @Override public void deleteAttributeColFilter(Connection connection, Map<String, Object> event) throws PortalPersistenceException { accessControlPersistence.deleteAttributeColFilter(connection, event); }
    @Override public Result<String> queryAttributeColFilter(int offset, int limit, String filters, String globalFilter, String sorting, boolean active, String hostId) { return accessControlPersistence.queryAttributeColFilter(offset, limit, filters, globalFilter, sorting, active, hostId); }
    @Override public Result<String> getAttributeColFilterById(String hostId, String attributeId, String endpointId) { return accessControlPersistence.getAttributeColFilterById(hostId, attributeId, endpointId); }

    // --- Category ---
    @Override public void createCategory(Connection connection, Map<String, Object> event) throws PortalPersistenceException { categoryPersistence.createCategory(connection, event); }
    @Override public void updateCategory(Connection connection, Map<String, Object> event) throws PortalPersistenceException { categoryPersistence.updateCategory(connection, event); }
    @Override public void deleteCategory(Connection connection, Map<String, Object> event) throws PortalPersistenceException { categoryPersistence.deleteCategory(connection, event); }
    @Override public void createEntityCategory(Connection conn, Map<String, Object> event) throws PortalPersistenceException { categoryPersistence.createEntityCategory(conn, event); }
    @Override public void updateEntityCategory(Connection conn, Map<String, Object> event) throws PortalPersistenceException { categoryPersistence.updateEntityCategory(conn, event); }
    @Override public void deleteEntityCategory(Connection conn, Map<String, Object> event) throws PortalPersistenceException { categoryPersistence.deleteEntityCategory(conn, event); }
    @Override public Result<String> getCategory(int offset, int limit, String filters, String globalFilter, String sorting, boolean active, String hostId) { return categoryPersistence.getCategory(offset, limit, filters, globalFilter, sorting, active, hostId); }
    @Override public Result<String> getCategoryLabel(String hostId) { return categoryPersistence.getCategoryLabel(hostId); }
    @Override public Result<String> getCategoryById(String categoryId) { return categoryPersistence.getCategoryById(categoryId); }
    @Override public Result<String> getCategoryByName(String hostId, String categoryName) { return categoryPersistence.getCategoryByName(hostId, categoryName); }
    @Override public Result<String> getCategoryByType(String hostId, String entityType) { return categoryPersistence.getCategoryByType(hostId, entityType); }
    @Override public Result<String> getCategoryTree(String hostId, String entityType) { return categoryPersistence.getCategoryTree(hostId, entityType); }

    // --- Tag ---
    @Override public void createTag(Connection connection, Map<String, Object> event) throws PortalPersistenceException { tagPersistence.createTag(connection, event); }
    @Override public void updateTag(Connection connection, Map<String, Object> event) throws PortalPersistenceException { tagPersistence.updateTag(connection, event); }
    @Override public void deleteTag(Connection connection, Map<String, Object> event) throws PortalPersistenceException { tagPersistence.deleteTag(connection, event); }
    @Override public void createEntityTag(Connection conn, Map<String, Object> event) throws PortalPersistenceException { tagPersistence.createEntityTag(conn, event); }
    @Override public void updateEntityTag(Connection conn, Map<String, Object> event) throws PortalPersistenceException { tagPersistence.updateEntityTag(conn, event); }
    @Override public void deleteEntityTag(Connection conn, Map<String, Object> event) throws PortalPersistenceException { tagPersistence.deleteEntityTag(conn, event); }
    @Override public Result<String> getTag(int offset, int limit, String filters, String globalFilter, String sorting, boolean active, String hostId) { return tagPersistence.getTag(offset, limit, filters, globalFilter, sorting, active, hostId); }
    @Override public Result<String> getTagLabel(String hostId) { return tagPersistence.getTagLabel(hostId); }
    @Override public Result<String> getTagById(String tagId) { return tagPersistence.getTagById(tagId); }
    @Override public Result<String> getTagByName(String hostId, String tagName) { return tagPersistence.getTagByName(hostId, tagName); }
    @Override public Result<String> getTagByType(String hostId, String entityType) { return tagPersistence.getTagByType(hostId, entityType); }

    // --- Schema ---
    @Override public void createSchema(Connection connection, Map<String, Object> event) throws PortalPersistenceException { schemaPersistence.createSchema(connection, event); }
    @Override public void updateSchema(Connection connection, Map<String, Object> event) throws PortalPersistenceException { schemaPersistence.updateSchema(connection, event); }
    @Override public void deleteSchema(Connection connection, Map<String, Object> event) throws PortalPersistenceException { schemaPersistence.deleteSchema(connection, event); }
    @Override public Result<String> getSchema(int offset, int limit, String filters, String globalFilter, String sorting, boolean active, String hostId) { return schemaPersistence.getSchema(offset, limit, filters, globalFilter, sorting, active, hostId); }
    @Override public Result<String> getSchemaLabel(String hostId) { return schemaPersistence.getSchemaLabel(hostId); }
    @Override public Result<String> getSchemaById(String schemaId) { return schemaPersistence.getSchemaById(schemaId); }
    @Override public Result<String> getSchemaByCategoryId(String categoryId) { return schemaPersistence.getSchemaByCategoryId(categoryId); }
    @Override public Result<String> getSchemaByTagId(String tagId) { return schemaPersistence.getSchemaByTagId(tagId); }

    // --- Schedule ---
    @Override public void createSchedule(Connection connection, Map<String, Object> event) throws PortalPersistenceException { schedulePersistence.createSchedule(connection, event); }
    @Override public void updateSchedule(Connection connection, Map<String, Object> event) throws PortalPersistenceException { schedulePersistence.updateSchedule(connection, event); }
    @Override public void deleteSchedule(Connection connection, Map<String, Object> event) throws PortalPersistenceException { schedulePersistence.deleteSchedule(connection, event); }
    @Override public Result<String> getSchedule(int offset, int limit, String filters, String globalFilter, String sorting, boolean active, String hostId) { return schedulePersistence.getSchedule(offset, limit, filters, globalFilter, sorting, active, hostId); }
    @Override public Result<String> getScheduleLabel(String hostId) { return schedulePersistence.getScheduleLabel(hostId); }
    @Override public Result<String> getScheduleById(String scheduleId) { return schedulePersistence.getScheduleById(scheduleId); }
    @Override public int acquireLock(String instanceId, int lockId, OffsetDateTime lockTimeout) throws PortalPersistenceException { return schedulePersistence.acquireLock(instanceId, lockId, lockTimeout);}
    @Override public int renewLock(String instanceId, int lockId) throws PortalPersistenceException { return schedulePersistence.renewLock(instanceId, lockId); }
    @Override public int releaseLock(String instanceId, int lockId) throws PortalPersistenceException { return schedulePersistence.releaseLock(instanceId, lockId); }
    @Override public Result<List<Map<String, Object>>> pollTasks(OffsetDateTime nextRunTs) { return schedulePersistence.pollTasks(nextRunTs); }
    @Override public Result<String> executeTask(Map<String, Object> taskData, long executionTimeMillis) { return schedulePersistence.executeTask(taskData, executionTimeMillis); }

    // --- Rule ---
    @Override public void createRule(Connection connection, Map<String, Object> event) throws PortalPersistenceException { rulePersistence.createRule(connection, event); }
    @Override public void updateRule(Connection connection, Map<String, Object> event) throws PortalPersistenceException { rulePersistence.updateRule(connection, event); }
    @Override public void deleteRule(Connection connection, Map<String, Object> event) throws PortalPersistenceException { rulePersistence.deleteRule(connection, event); }
    @Override public Result<List<Map<String, Object>>> queryRuleByGroup(String groupId) { return rulePersistence.queryRuleByGroup(groupId); }
    @Override public Result<String> queryRule(int offset, int limit, String filters, String globalFilter, String sorting, boolean active, String hostId) { return rulePersistence.queryRule(offset, limit, filters, globalFilter, sorting, active, hostId); }
    @Override public Result<Map<String, Object>> queryRuleById(String ruleId) { return rulePersistence.queryRuleById(ruleId); }
    @Override public Result<String> queryRuleByType(String ruleType) { return rulePersistence.queryRuleByType(ruleType); }
    @Override public Result<List<Map<String, Object>>> queryRuleByHostApiId(String hostId, String apiId, String apiVersion) { return rulePersistence.queryRuleByHostApiId(hostId, apiId, apiVersion); }

    // --- Event ---
    @Override public Result<String> insertEventStore(CloudEvent[] events) { return eventPersistence.insertEventStore(events); }
    @Override public int getMaxAggregateVersion(String aggregateId) { return eventPersistence.getMaxAggregateVersion(aggregateId); }

    // --- Product / Instance Applicable Properties
    @Override
    public Result<String> getApplicableConfigPropertiesForInstance(int offset, int limit, String hostId, String instanceId, Set<String> resourceTypes, Set<String> configTypes, Set<String> propertyTypes,Set<String> configPhases) { return configPersistence.getApplicableConfigPropertiesForInstance(offset, limit, hostId, instanceId, resourceTypes, configTypes, propertyTypes,configPhases); }

    @Override
    public Result<String> getApplicableConfigPropertiesForInstanceApi(int offset, int limit, String hostId, String instanceApiId) { return configPersistence.getApplicableConfigPropertiesForInstanceApi(offset, limit, hostId, instanceApiId); }

    @Override
    public Result<String> getApplicableConfigPropertiesForInstanceApp(int offset, int limit, String hostId, String instanceAppId) { return configPersistence.getApplicableConfigPropertiesForInstanceApp(offset, limit, hostId, instanceAppId); }

    @Override
    public Result<String> getApplicableConfigPropertiesForInstanceAppApi(int offset, int limit, String hostId, String instanceAppId, String instanceApiId) { return configPersistence.getApplicableConfigPropertiesForInstanceAppApi(offset, limit, hostId, instanceAppId, instanceApiId); }

    @Override
    public Result<String> getAllAggregatedInstanceRuntimeConfigs(String hostId, String instanceId) { return configPersistence.getAllAggregatedInstanceRuntimeConfigs(hostId, instanceId); }

    @Override
    public Result<String> getPromotableInstanceConfigs(String hostId, String instanceId,Set<String> propertyNames,Set<String> apiUids){return configPersistence.getPromotableInstanceConfigs(hostId,instanceId,propertyNames,apiUids);}

    // --- GenAI Agent ---
    @Override public void createAgentDefinition(Connection conn, Map<String, Object> event) throws PortalPersistenceException { genAIPersistence.createAgentDefinition(conn, event); }
    @Override public void updateAgentDefinition(Connection conn, Map<String, Object> event) throws PortalPersistenceException { genAIPersistence.updateAgentDefinition(conn, event); }
    @Override public void deleteAgentDefinition(Connection conn, Map<String, Object> event) throws PortalPersistenceException { genAIPersistence.deleteAgentDefinition(conn, event); }
    @Override public Result<String> queryAgentDefinition(int offset, int limit, String filters, String globalFilter, String sorting, boolean active, String hostId) { return genAIPersistence.queryAgentDefinition(offset, limit, filters, globalFilter, sorting, active, hostId); }
    @Override public Result<String> getAgentDefinitionById(String hostId, String agentDefId) { return genAIPersistence.getAgentDefinitionById(hostId, agentDefId); }

    // --- Workflow Definition ---
    @Override public void createWorkflowDefinition(Connection conn, Map<String, Object> event) throws PortalPersistenceException { genAIPersistence.createWorkflowDefinition(conn, event); }
    @Override public void updateWorkflowDefinition(Connection conn, Map<String, Object> event) throws PortalPersistenceException { genAIPersistence.updateWorkflowDefinition(conn, event); }
    @Override public void deleteWorkflowDefinition(Connection conn, Map<String, Object> event) throws PortalPersistenceException { genAIPersistence.deleteWorkflowDefinition(conn, event); }
    @Override public Result<String> queryWorkflowDefinition(int offset, int limit, String filters, String globalFilter, String sorting, boolean active, String hostId) { return genAIPersistence.queryWorkflowDefinition(offset, limit, filters, globalFilter, sorting, active, hostId); }
    @Override public Result<String> getWorkflowDefinitionById(String hostId, String wfDefId) { return genAIPersistence.getWorkflowDefinitionById(hostId, wfDefId); }

    // --- Worklist ---
    @Override public void createWorklist(Connection conn, Map<String, Object> event) throws PortalPersistenceException { genAIPersistence.createWorklist(conn, event); }
    @Override public void updateWorklist(Connection conn, Map<String, Object> event) throws PortalPersistenceException { genAIPersistence.updateWorklist(conn, event); }
    @Override public void deleteWorklist(Connection conn, Map<String, Object> event) throws PortalPersistenceException { genAIPersistence.deleteWorklist(conn, event); }
    @Override public Result<String> queryWorklist(int offset, int limit, String filters, String globalFilter, String sorting, boolean active, String hostId) { return genAIPersistence.queryWorklist(offset, limit, filters, globalFilter, sorting, active, hostId); }
    @Override public Result<String> getWorklistById(String hostId, String assigneeId, String categoryId) { return genAIPersistence.getWorklistById(hostId, assigneeId, categoryId); }

    // --- WorklistColumn ---
    @Override public void createWorklistColumn(Connection conn, Map<String, Object> event) throws PortalPersistenceException { genAIPersistence.createWorklistColumn(conn, event); }
    @Override public void updateWorklistColumn(Connection conn, Map<String, Object> event) throws PortalPersistenceException { genAIPersistence.updateWorklistColumn(conn, event); }
    @Override public void deleteWorklistColumn(Connection conn, Map<String, Object> event) throws PortalPersistenceException { genAIPersistence.deleteWorklistColumn(conn, event); }
    @Override public Result<String> queryWorklistColumn(int offset, int limit, String filters, String globalFilter, String sorting, boolean active, String hostId) { return genAIPersistence.queryWorklistColumn(offset, limit, filters, globalFilter, sorting, active, hostId); }
    @Override public Result<String> getWorklistColumnById(String hostId, String assigneeId, String categoryId, int sequenceId) { return genAIPersistence.getWorklistColumnById(hostId, assigneeId, categoryId, sequenceId); }

    // --- ProcessInfo ---
    @Override public void createProcessInfo(Connection conn, Map<String, Object> event) throws PortalPersistenceException { genAIPersistence.createProcessInfo(conn, event); }
    @Override public void updateProcessInfo(Connection conn, Map<String, Object> event) throws PortalPersistenceException { genAIPersistence.updateProcessInfo(conn, event); }
    @Override public void deleteProcessInfo(Connection conn, Map<String, Object> event) throws PortalPersistenceException { genAIPersistence.deleteProcessInfo(conn, event); }
    @Override public Result<String> queryProcessInfo(int offset, int limit, String filters, String globalFilter, String sorting, boolean active, String hostId) { return genAIPersistence.queryProcessInfo(offset, limit, filters, globalFilter, sorting, active, hostId); }
    @Override public Result<String> getProcessInfoById(String hostId, String processId) { return genAIPersistence.getProcessInfoById(hostId, processId); }

    // --- TaskInfo ---
    @Override public void createTaskInfo(Connection conn, Map<String, Object> event) throws PortalPersistenceException { genAIPersistence.createTaskInfo(conn, event); }
    @Override public void updateTaskInfo(Connection conn, Map<String, Object> event) throws PortalPersistenceException { genAIPersistence.updateTaskInfo(conn, event); }
    @Override public void deleteTaskInfo(Connection conn, Map<String, Object> event) throws PortalPersistenceException { genAIPersistence.deleteTaskInfo(conn, event); }
    @Override public Result<String> queryTaskInfo(int offset, int limit, String filters, String globalFilter, String sorting, boolean active, String hostId) { return genAIPersistence.queryTaskInfo(offset, limit, filters, globalFilter, sorting, active, hostId); }
    @Override public Result<String> getTaskInfoById(String hostId, String taskId) { return genAIPersistence.getTaskInfoById(hostId, taskId); }

    // --- TaskAssignment ---
    @Override public void createTaskAssignment(Connection conn, Map<String, Object> event) throws PortalPersistenceException { genAIPersistence.createTaskAssignment(conn, event); }
    @Override public void updateTaskAssignment(Connection conn, Map<String, Object> event) throws PortalPersistenceException { genAIPersistence.updateTaskAssignment(conn, event); }
    @Override public void deleteTaskAssignment(Connection conn, Map<String, Object> event) throws PortalPersistenceException { genAIPersistence.deleteTaskAssignment(conn, event); }
    @Override public Result<String> queryTaskAssignment(int offset, int limit, String filters, String globalFilter, String sorting, boolean active, String hostId) { return genAIPersistence.queryTaskAssignment(offset, limit, filters, globalFilter, sorting, active, hostId); }
    @Override public Result<String> getTaskAssignmentById(String hostId, String taskAsstId) { return genAIPersistence.getTaskAssignmentById(hostId, taskAsstId); }

    // --- AuditLog ---
    @Override public void createAuditLog(Connection conn, Map<String, Object> event) throws PortalPersistenceException { genAIPersistence.createAuditLog(conn, event); }
    @Override public Result<String> queryAuditLog(int offset, int limit, String filters, String globalFilter, String sorting, String hostId) { return genAIPersistence.queryAuditLog(offset, limit, filters, globalFilter, sorting, hostId); }
    @Override public Result<String> getAuditLogById(String hostId, String auditLogId) { return genAIPersistence.getAuditLogById(hostId, auditLogId); }

    // --- Skill ---
    @Override public void createSkill(Connection conn, Map<String, Object> event) throws PortalPersistenceException { genAIPersistence.createSkill(conn, event); }
    @Override public void updateSkill(Connection conn, Map<String, Object> event) throws PortalPersistenceException { genAIPersistence.updateSkill(conn, event); }
    @Override public void deleteSkill(Connection conn, Map<String, Object> event) throws PortalPersistenceException { genAIPersistence.deleteSkill(conn, event); }
    @Override public Result<String> querySkill(int offset, int limit, String filters, String globalFilter, String sorting, boolean active, String hostId) { return genAIPersistence.querySkill(offset, limit, filters, globalFilter, sorting, active, hostId); }
    @Override public Result<String> getSkillById(String hostId, String skillId) { return genAIPersistence.getSkillById(hostId, skillId); }

    // --- SkillParam ---
    @Override public void createSkillParam(Connection conn, Map<String, Object> event) throws PortalPersistenceException { genAIPersistence.createSkillParam(conn, event); }
    @Override public void updateSkillParam(Connection conn, Map<String, Object> event) throws PortalPersistenceException { genAIPersistence.updateSkillParam(conn, event); }
    @Override public void deleteSkillParam(Connection conn, Map<String, Object> event) throws PortalPersistenceException { genAIPersistence.deleteSkillParam(conn, event); }
    @Override public Result<String> querySkillParam(int offset, int limit, String filters, String globalFilter, String sorting, boolean active, String hostId) { return genAIPersistence.querySkillParam(offset, limit, filters, globalFilter, sorting, active, hostId); }
    @Override public Result<String> getSkillParamById(String hostId, String paramId) { return genAIPersistence.getSkillParamById(hostId, paramId); }

    // --- SkillDependency ---
    @Override public void createSkillDependency(Connection conn, Map<String, Object> event) throws PortalPersistenceException { genAIPersistence.createSkillDependency(conn, event); }
    @Override public void updateSkillDependency(Connection conn, Map<String, Object> event) throws PortalPersistenceException { genAIPersistence.updateSkillDependency(conn, event); }
    @Override public void deleteSkillDependency(Connection conn, Map<String, Object> event) throws PortalPersistenceException { genAIPersistence.deleteSkillDependency(conn, event); }
    @Override public Result<String> querySkillDependency(int offset, int limit, String filters, String globalFilter, String sorting, boolean active, String hostId) { return genAIPersistence.querySkillDependency(offset, limit, filters, globalFilter, sorting, active, hostId); }
    @Override public Result<String> getSkillDependencyById(String hostId, String skillId, String dependsOnSkillId) { return genAIPersistence.getSkillDependencyById(hostId, skillId, dependsOnSkillId); }

    // --- AgentSkill ---
    @Override public void createAgentSkill(Connection conn, Map<String, Object> event) throws PortalPersistenceException { genAIPersistence.createAgentSkill(conn, event); }
    @Override public void updateAgentSkill(Connection conn, Map<String, Object> event) throws PortalPersistenceException { genAIPersistence.updateAgentSkill(conn, event); }
    @Override public void deleteAgentSkill(Connection conn, Map<String, Object> event) throws PortalPersistenceException { genAIPersistence.deleteAgentSkill(conn, event); }
    @Override public Result<String> queryAgentSkill(int offset, int limit, String filters, String globalFilter, String sorting, boolean active, String hostId) { return genAIPersistence.queryAgentSkill(offset, limit, filters, globalFilter, sorting, active, hostId); }
    @Override public Result<String> getAgentSkillById(String hostId, String agentDefId, String skillId) { return genAIPersistence.getAgentSkillById(hostId, agentDefId, skillId); }

    // --- AgentSessionHistory ---
    @Override public void createAgentSessionHistory(Connection conn, Map<String, Object> event) throws PortalPersistenceException { genAIPersistence.createAgentSessionHistory(conn, event); }
    @Override public void deleteAgentSessionHistory(Connection conn, Map<String, Object> event) throws PortalPersistenceException { genAIPersistence.deleteAgentSessionHistory(conn, event); }
    @Override public Result<String> queryAgentSessionHistory(int offset, int limit, String filters, String globalFilter, String sorting, boolean active, String hostId) { return genAIPersistence.queryAgentSessionHistory(offset, limit, filters, globalFilter, sorting, active, hostId); }
    @Override public Result<String> getAgentSessionHistoryById(String hostId, String sessionHistoryId) { return genAIPersistence.getAgentSessionHistoryById(hostId, sessionHistoryId); }

    // --- SessionMemory ---
    @Override public void createSessionMemory(Connection conn, Map<String, Object> event) throws PortalPersistenceException { genAIPersistence.createSessionMemory(conn, event); }
    @Override public void updateSessionMemory(Connection conn, Map<String, Object> event) throws PortalPersistenceException { genAIPersistence.updateSessionMemory(conn, event); }
    @Override public void deleteSessionMemory(Connection conn, Map<String, Object> event) throws PortalPersistenceException { genAIPersistence.deleteSessionMemory(conn, event); }
    @Override public Result<String> querySessionMemory(int offset, int limit, String filters, String globalFilter, String sorting, boolean active, String hostId) { return genAIPersistence.querySessionMemory(offset, limit, filters, globalFilter, sorting, active, hostId); }
    @Override public Result<String> getSessionMemoryById(String hostId, String memId) { return genAIPersistence.getSessionMemoryById(hostId, memId); }

    // --- UserMemory ---
    @Override public void createUserMemory(Connection conn, Map<String, Object> event) throws PortalPersistenceException { genAIPersistence.createUserMemory(conn, event); }
    @Override public void updateUserMemory(Connection conn, Map<String, Object> event) throws PortalPersistenceException { genAIPersistence.updateUserMemory(conn, event); }
    @Override public void deleteUserMemory(Connection conn, Map<String, Object> event) throws PortalPersistenceException { genAIPersistence.deleteUserMemory(conn, event); }
    @Override public Result<String> queryUserMemory(int offset, int limit, String filters, String globalFilter, String sorting, boolean active, String hostId) { return genAIPersistence.queryUserMemory(offset, limit, filters, globalFilter, sorting, active, hostId); }
    @Override public Result<String> getUserMemoryById(String hostId, String memId) { return genAIPersistence.getUserMemoryById(hostId, memId); }

    // --- AgentMemory ---
    @Override public void createAgentMemory(Connection conn, Map<String, Object> event) throws PortalPersistenceException { genAIPersistence.createAgentMemory(conn, event); }
    @Override public void updateAgentMemory(Connection conn, Map<String, Object> event) throws PortalPersistenceException { genAIPersistence.updateAgentMemory(conn, event); }
    @Override public void deleteAgentMemory(Connection conn, Map<String, Object> event) throws PortalPersistenceException { genAIPersistence.deleteAgentMemory(conn, event); }
    @Override public Result<String> queryAgentMemory(int offset, int limit, String filters, String globalFilter, String sorting, boolean active, String hostId) { return genAIPersistence.queryAgentMemory(offset, limit, filters, globalFilter, sorting, active, hostId); }
    @Override public Result<String> getAgentMemoryById(String hostId, String memId) { return genAIPersistence.getAgentMemoryById(hostId, memId); }

    // --- OrgMemory ---
    @Override public void createOrgMemory(Connection conn, Map<String, Object> event) throws PortalPersistenceException { genAIPersistence.createOrgMemory(conn, event); }
    @Override public void updateOrgMemory(Connection conn, Map<String, Object> event) throws PortalPersistenceException { genAIPersistence.updateOrgMemory(conn, event); }
    @Override public void deleteOrgMemory(Connection conn, Map<String, Object> event) throws PortalPersistenceException { genAIPersistence.deleteOrgMemory(conn, event); }
    @Override public Result<String> queryOrgMemory(int offset, int limit, String filters, String globalFilter, String sorting, boolean active, String hostId) { return genAIPersistence.queryOrgMemory(offset, limit, filters, globalFilter, sorting, active, hostId); }
    @Override public Result<String> getOrgMemoryById(String hostId, String memId) { return genAIPersistence.getOrgMemoryById(hostId, memId); }
}
