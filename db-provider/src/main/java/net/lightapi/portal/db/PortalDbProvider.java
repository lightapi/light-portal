package net.lightapi.portal.db;

import com.networknt.config.JsonMapper;
import com.networknt.db.provider.DbProvider;
import com.networknt.monad.Result;
import com.networknt.utility.Constants;
import com.networknt.utility.UuidUtil;
import io.cloudevents.CloudEvent;
import io.cloudevents.core.builder.CloudEventBuilder;
import io.cloudevents.core.v1.CloudEventV1;
import net.lightapi.portal.PortalConstants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.SQLException;
import java.time.OffsetDateTime;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Interface class provide the contract for different database implementation for the portal. Mainly, the data is
 * saved in the database. However, for some temp date like the oauth code, it is saved in the memory. The Kafka
 * event will be used to sync the data between the memory caches.
 *
 * @author Steve Hu
 */
public interface PortalDbProvider extends DbProvider {
    String SQL_EXCEPTION = "ERR10017";
    String GENERIC_EXCEPTION = "ERR10014";
    String OBJECT_NOT_FOUND = "ERR11637";

    // RefTable
    void createRefTable(Connection conn, Map<String, Object> event) throws SQLException, Exception;
    void updateRefTable(Connection conn, Map<String, Object> event) throws SQLException, Exception;
    void deleteRefTable(Connection conn, Map<String, Object> event) throws SQLException, Exception;
    Result<String> getRefTable(int offset, int limit, String filters, String globalFilter, String sorting, boolean active, String hostId);
    Result<String> getRefTableById(String tableId);
    Result<String> getRefTableLabel(String hostId);

    // RefValue
    void createRefValue(Connection conn, Map<String, Object> event) throws SQLException, Exception;
    void updateRefValue(Connection conn, Map<String, Object> event) throws SQLException, Exception;
    void deleteRefValue(Connection conn, Map<String, Object> event) throws SQLException, Exception;
    Result<String> getRefValue(int offset, int limit, String filters, String globalFilter, String sorting, boolean active);
    Result<String> getRefValueById(String valueId);
    Result<String> getRefValueLabel(String tableId);

    // RefLocale
    void createRefLocale(Connection conn, Map<String, Object> event) throws SQLException, Exception;
    void updateRefLocale(Connection conn, Map<String, Object> event) throws SQLException, Exception;
    void deleteRefLocale(Connection conn, Map<String, Object> event) throws SQLException, Exception;
    Result<String> getRefLocale(int offset, int limit, String filters, String globalFilter, String sorting, boolean active);

    // RefRelationType
    void createRefRelationType(Connection conn, Map<String, Object> event) throws SQLException, Exception;
    void updateRefRelationType(Connection conn, Map<String, Object> event) throws SQLException, Exception;
    void deleteRefRelationType(Connection conn, Map<String, Object> event) throws SQLException, Exception;
    Result<String> getRefRelationType(int offset, int limit, String filters, String globalFilter, String sorting, boolean active);

    // RefRelation
    void createRefRelation(Connection conn, Map<String, Object> event) throws SQLException, Exception;
    void updateRefRelation(Connection conn, Map<String, Object> event) throws SQLException, Exception;
    void deleteRefRelation(Connection conn, Map<String, Object> event) throws SQLException, Exception;
    Result<String> getRefRelation(int offset, int limit, String filters, String globalFilter, String sorting, boolean active);
    Result<String> getToValueCode(String relationName, String fromValueCode);

    // User
    Result<String> loginUserByEmail(String email);
    Result<String> queryUserByEmail(String email);
    Result<String> queryUserById(String id);
    Result<String> getUserById(String userId);
    Result<String> queryUserByTypeEntityId(String userType, String entityId);
    Result<String> queryUserByWallet(String cryptoType, String cryptoAddress);
    Result<String> queryEmailByWallet(String cryptoType, String cryptoAddress);
    Result<String> queryUserByHostId(int offset, int limit, String filters, String globalFilter, String sorting, boolean active);
    Result<String> queryNotification(int offset, int limit, String hostId, String userId, Long nonce, String eventClass, Boolean successFlag,
                                     OffsetDateTime processTs, String eventJson, String error);
    Result<String> getHostsByUserId(String userId);
    Result<String> getHostLabelByUserId(String userId);

    void createUser(Connection conn, Map<String, Object> event) throws SQLException, Exception;
    void onboardUser(Connection conn, Map<String, Object> event) throws SQLException, Exception;
    void confirmUser(Connection conn, Map<String, Object> event) throws SQLException, Exception;
    void verifyUser(Connection conn, Map<String, Object> event) throws SQLException, Exception;
    long queryNonceByUserId(String userId);
    void createSocialUser(Connection conn, Map<String, Object> event) throws SQLException, Exception;
    void updateUser(Connection conn, Map<String, Object> event) throws SQLException, Exception;
    void deleteUser(Connection conn, Map<String, Object> event) throws SQLException, Exception;
    void forgetPassword(Connection conn, Map<String, Object> event) throws SQLException, Exception;
    void resetPassword(Connection conn, Map<String, Object> event) throws SQLException, Exception;
    void changePassword(Connection conn, Map<String, Object> event) throws SQLException, Exception;
    void updatePayment(Connection conn, Map<String, Object> event) throws SQLException, Exception;
    void deletePayment(Connection conn, Map<String, Object> event) throws SQLException, Exception;
    void createOrder(Connection conn, Map<String, Object> event) throws SQLException, Exception;
    void cancelOrder(Connection conn, Map<String, Object> event) throws SQLException, Exception;
    void deliverOrder(Connection conn, Map<String, Object> event) throws SQLException, Exception;
    void sendPrivateMessage(Connection conn, Map<String, Object> event) throws SQLException, Exception;
    Result<String> queryUserLabel(String hostId);
    Result<String> getUserLabelNotInHost(String hostId);

    // RefreshToken
    void createRefreshToken(Connection conn, Map<String, Object> event) throws SQLException, Exception;
    Result<String> queryRefreshToken(String refreshToken);
    void deleteRefreshToken(Connection conn, Map<String, Object> event) throws SQLException, Exception;
    Result<String> getRefreshToken(int offset, int limit, String filters, String globalFilter, String sorting, boolean active, String hostId);
    // RefToken
    void createRefToken(Connection conn, Map<String, Object> event) throws SQLException, Exception;
    void deleteRefToken(Connection conn, Map<String, Object> event) throws SQLException, Exception;
    Result<String> getRefToken(int offset, int limit, String filters, String globalFilter, String sorting, boolean active, String hostId);
    Result<String> queryRefToken(String refToken);

    // AuthCode
    void createAuthCode(Connection conn, Map<String, Object> event) throws SQLException, Exception;
    void deleteAuthCode(Connection conn, Map<String, Object> event) throws SQLException, Exception;
    Result<String> queryAuthCode(String authCode);
    Result<String> getAuthCode(int offset, int limit, String filters, String globalFilter, String sorting, boolean active, String hostId);

    // AuthProvider
    Result<Map<String, Object>> queryProviderById(String providerId);
    Result<String> getProviderIdLabel(String hostId);
    String queryProviderByName(String hostId, String providerName);
    Result<String> queryProvider(int offset, int limit, String filters, String globalFilter, String sorting, boolean active, String hostId);
    void createAuthProvider(Connection conn, Map<String, Object> event) throws SQLException, Exception;
    void rotateAuthProvider(Connection conn, Map<String, Object> event) throws SQLException, Exception;
    void updateAuthProvider(Connection conn, Map<String, Object> event) throws SQLException, Exception;
    void deleteAuthProvider(Connection conn, Map<String, Object> event) throws SQLException, Exception;
    Result<String> queryProviderKey(String providerId);
    Result<Map<String, Object>> queryCurrentProviderKey(String providerId);
    Result<Map<String, Object>> queryLongLiveProviderKey(String providerId);
    void createAuthProviderApi(Connection conn, Map<String, Object> event) throws SQLException, Exception;
    void updateAuthProviderApi(Connection conn, Map<String, Object> event) throws SQLException, Exception;
    void deleteAuthProviderApi(Connection conn, Map<String, Object> event) throws SQLException, Exception;
    Result<String> queryAuthProviderApi(int offset, int limit, String filters, String globalFilter, String sorting, boolean active, String hostId);

    void createAuthProviderClient(Connection conn, Map<String, Object> event) throws SQLException, Exception;
    void updateAuthProviderClient(Connection conn, Map<String, Object> event) throws SQLException, Exception;
    void deleteAuthProviderClient(Connection conn, Map<String, Object> event) throws SQLException, Exception;
    Result<String> queryAuthProviderClient(int offset, int limit, String filters, String globalFilter, String sorting, boolean active, String hostId);

    // App
    Result<String> queryApp(int offset, int limit, String filters, String globalFilter, String sorting, boolean active, String hostId);
    Result<String> queryAppById(String hostId, String appId);
    Result<String> queryClient(int offset, int limit, String filters, String globalFilter, String sorting, boolean active, String hostId);
    void createApp(Connection conn, Map<String, Object> event) throws SQLException, Exception;
    void updateApp(Connection conn, Map<String, Object> event) throws SQLException, Exception;
    void deleteApp(Connection conn, Map<String, Object> event) throws SQLException, Exception;
    Result<String> getAppIdLabel(String hostId);

    // Client
    void createClient(Connection conn, Map<String, Object> event) throws SQLException, Exception;
    void updateClient(Connection conn, Map<String, Object> event) throws SQLException, Exception;
    void deleteClient(Connection conn, Map<String, Object> event) throws SQLException, Exception;
    Result<String> queryClientByClientId(String clientId);
    Result<String> getClientById(String hostId, String clientId);
    Result<String> queryClientByProviderClientId(String providerId, String clientId);
    Result<String> queryClientByHostAppId(String host, String applicationId);
    Result<String> getClientIdLabel(String hostId);

    // Service
    void createApi(Connection conn, Map<String, Object> event) throws SQLException, Exception;
    void updateApi(Connection conn, Map<String, Object> event) throws SQLException, Exception;
    void deleteApi(Connection conn, Map<String, Object> event) throws SQLException, Exception;
    Result<String> queryApi(int offset, int limit, String filters, String globalFilter, String sorting, boolean active, String hostId);
    Result<String> getApiVersionIdLabel(String hostId);
    Result<String> queryApiLabel(String hostId);
    Result<String> getApiById(String hostId, String apiId);
    Result<String> queryApiVersionLabel(String hostId, String apiId);
    Result<String> queryEndpointLabel(String hostId, String apiVersionId);

    // ServiceVersion
    void createApiVersion(Connection conn, Map<String, Object> event) throws SQLException, Exception;
    void updateApiVersion(Connection conn, Map<String, Object> event) throws SQLException, Exception;
    void deleteApiVersion(Connection conn, Map<String, Object> event) throws SQLException, Exception;
    Result<String> queryApiVersion(String hostId, String apiId);
    void updateApiVersionSpec(Connection conn, Map<String, Object> event) throws SQLException, Exception;
    Result<String> getApiVersionById(String hostId, String apiVersionId);
    String queryApiVersionId(String hostId, String apiId, String apiVersion);
    Map<String, Object> getEndpointIdMap(String hostId, String apiVersionId);

    // ServiceEndpoint
    Result<String> queryApiEndpoint(int offset, int limit, String filters, String globalFilter, String sorting, boolean active, String hostId);
    Result<String> queryApiEndpointRule(String hostId, String endpointId);
    Result<String> queryApiEndpointScope(String hostId, String endpointId);

    // EndpointRule
    void createApiEndpointRule(Connection conn, Map<String, Object> event) throws SQLException, Exception;
    void deleteApiEndpointRule(Connection conn, Map<String, Object> event) throws SQLException, Exception;
    Result<String> queryServiceRule(String hostId, String apiId, String apiVersion);

    // Permissions and Filters (Service Specific Aggregations)
    Result<String> queryApiPermission(String hostId, String apiId, String apiVersion);
    Result<List<String>> queryApiFilter(String hostId, String apiId, String apiVersion);
    Result<String> getServiceIdLabel(String hostId);

    // Org
    void createOrg(Connection conn, Map<String, Object> event) throws SQLException, Exception;
    void updateOrg(Connection conn, Map<String, Object> event) throws SQLException, Exception;
    void deleteOrg(Connection conn, Map<String, Object> event) throws SQLException, Exception;

    // Host
    void createHost(Connection conn, Map<String, Object> event) throws SQLException, Exception;
    void updateHost(Connection conn, Map<String, Object> event) throws SQLException, Exception;
    void deleteHost(Connection conn, Map<String, Object> event) throws SQLException, Exception;
    void switchUserHost(Connection conn, Map<String, Object> event) throws SQLException, Exception;
    void createUserHost(Connection conn, Map<String, Object> event) throws SQLException, Exception;
    void deleteUserHost(Connection conn, Map<String, Object> event) throws SQLException, Exception;
    Result<String> queryHostDomainById(String hostId);
    Result<String> queryHostById(String id);
    Result<Map<String, Object>> queryHostByOwner(String owner);
    Result<String> getOrg(int offset, int limit, String filters, String globalFilter, String sorting, boolean active);
    Result<String> getHost(int offset, int limit, String filters, String globalFilter, String sorting, boolean active);
    Result<String> getUserHost(int offset, int limit, String filters, String globalFilter, String sorting, boolean active);
    Result<String> getHostByDomain(String domain, String subDomain, String hostDesc);
    Result<String> getHostLabel();
    Result<String> getOrgByDomain(String domain);
    String getHostId(String domain, String subDomain);

    // Config
    void createConfig(Connection conn, Map<String, Object> event) throws SQLException, Exception;
    void updateConfig(Connection conn, Map<String, Object> event) throws SQLException, Exception;
    void deleteConfig(Connection conn, Map<String, Object> event) throws SQLException, Exception;
    Result<String> getConfig(int offset, int limit, String filters, String globalFilter, String sorting, boolean active);
    Result<String> queryConfigById(String configId);
    Result<String> getConfigIdLabel();
    String queryConfigId(String configName);
    Result<String> getPropertyIdLabel(String configId);
    Result<String> getConfigIdApiAppLabel(String resourceType);
    Result<String> getPropertyIdApiAppLabel(String configId, String resourceType);

    // ConfigProperty
    void createConfigProperty(Connection conn, Map<String, Object> event) throws SQLException, Exception;
    void updateConfigProperty(Connection conn, Map<String, Object> event) throws SQLException, Exception;
    void deleteConfigProperty(Connection conn, Map<String, Object> event) throws SQLException, Exception;

    Result<String> getConfigProperty(int offset, int limit, String filters, String globalFilter, String sorting, boolean active);
    Result<String> queryConfigPropertyById(String configId);
    Result<String> queryConfigPropertyByPropertyId(String configId, String propertyId);
    String queryPropertyId(String configName, String propertyName);
    Result<String> getPropertyById(String propertyId);
    String getPropertyId(String configId, String propertyName);

    // EnvironmentProperty
    void createConfigEnvironment(Connection conn, Map<String, Object> event) throws SQLException, Exception;
    void updateConfigEnvironment(Connection conn, Map<String, Object> event) throws SQLException, Exception;
    void deleteConfigEnvironment(Connection conn, Map<String, Object> event) throws SQLException, Exception;
    Result<String> getConfigEnvironment(int offset, int limit, String filters, String globalFilter, String sorting, boolean active, String hostId);
    Result<String> getConfigEnvironmentById(String hostId, String environmentId, String propertyId);

    // InstanceApiProperty
    void createInstanceApi(Connection conn, Map<String, Object> event) throws SQLException, Exception;
    void deleteInstanceApi(Connection conn, Map<String, Object> event) throws SQLException, Exception;
    Result<String> getInstanceApi(int offset, int limit, String filters, String globalFilter, String sorting, boolean active, String hostId);
    Result<String> getInstanceApiLabel(String hostId, String instanceId);
    Result<String> getInstanceApiById(String hostId, String instanceApiId);
    String getInstanceApiId(String hostId, String instanceId, String apiVersionId);

    // InstanceApiPathPrefix
    void createInstanceApiPathPrefix(Connection conn, Map<String, Object> event) throws SQLException, Exception;
    void updateInstanceApiPathPrefix(Connection conn, Map<String, Object> event) throws SQLException, Exception;
    void deleteInstanceApiPathPrefix(Connection conn, Map<String, Object> event) throws SQLException, Exception;
    Result<String> getInstanceApiPathPrefix(int offset, int limit, String filters, String globalFilter, String sorting, boolean active, String hostId);
    Result<String> getInstanceApiPathPrefixById(String hostId, String instanceApiId, String pathPrefix);

    // InstanceAppApi
    void createInstanceAppApi(Connection conn, Map<String, Object> event) throws SQLException, Exception;
    void deleteInstanceAppApi(Connection conn, Map<String, Object> event) throws SQLException, Exception;
    Result<String> getInstanceAppApi(int offset, int limit, String filters, String globalFilter, String sorting, boolean active, String hostId);
    Result<String> getInstanceAppApiById(String hostId, String instanceAppId, String instanceApiId);

    // ConfigInstanceApi (should be InstanceApiProperty config, perhaps a copy of original method set)
    void createConfigInstanceApi(Connection conn, Map<String, Object> event) throws SQLException, Exception;
    void updateConfigInstanceApi(Connection conn, Map<String, Object> event) throws SQLException, Exception;
    void deleteConfigInstanceApi(Connection conn, Map<String, Object> event) throws SQLException, Exception;
    Result<String> getConfigInstanceApi(int offset, int limit, String filters, String globalFilter, String sorting, boolean active, String hostId);
    Result<String> getConfigInstanceApiById(String hostId, String instanceApiId, String propertyId);

    // InstanceApp
    void createInstanceApp(Connection conn, Map<String, Object> event) throws SQLException, Exception;
    void deleteInstanceApp(Connection conn, Map<String, Object> event) throws SQLException, Exception;
    Result<String> getInstanceApp(int offset, int limit, String filters, String globalFilter, String sorting, boolean active, String hostId);
    Result<String> getInstanceAppLabel(String hostId, String instanceId);
    Result<String> getInstanceAppById(String hostId, String instanceAppId);
    String getInstanceAppId(String hostId, String instanceId, String appId, String appVersion);

    // ConfigInstanceApp (should be InstanceAppProperty config)
    void createConfigInstanceApp(Connection conn, Map<String, Object> event) throws SQLException, Exception;
    void updateConfigInstanceApp(Connection conn, Map<String, Object> event) throws SQLException, Exception;
    void deleteConfigInstanceApp(Connection conn, Map<String, Object> event) throws SQLException, Exception;
    Result<String> getConfigInstanceApp(int offset, int limit, String filters, String globalFilter, String sorting, boolean active, String hostId);
    Result<String> getConfigInstanceAppById(String hostId, String instanceAppId, String propertyId);

    // ConfigInstanceAppApi (should be InstanceAppApiProperty config)
    void createConfigInstanceAppApi(Connection conn, Map<String, Object> event) throws SQLException, Exception;
    void updateConfigInstanceAppApi(Connection conn, Map<String, Object> event) throws SQLException, Exception;
    void deleteConfigInstanceAppApi(Connection conn, Map<String, Object> event) throws SQLException, Exception;
    Result<String> getConfigInstanceAppApi(int offset, int limit, String filters, String globalFilter, String sorting, boolean active, String hostId);
    Result<String> getConfigInstanceAppApiById(String hostId, String instanceAppId, String instanceApiId, String propertyId);

    // InstanceProperty (was ConfigInstance)
    void createConfigInstance(Connection conn, Map<String, Object> event) throws SQLException, Exception;
    void updateConfigInstance(Connection conn, Map<String, Object> event) throws SQLException, Exception;
    void deleteConfigInstance(Connection conn, Map<String, Object> event) throws SQLException, Exception;

    void createConfigSnapshot(Connection conn, Map<String, Object> event) throws SQLException, Exception;
    void updateConfigSnapshot(Connection conn, Map<String, Object> event) throws SQLException, Exception;
    void deleteConfigSnapshot(Connection conn, Map<String, Object> event) throws SQLException, Exception;
    Result<String> getConfigSnapshot(int offset, int limit, String filters, String globalFilter, String sorting, String hostId);

    Result<String> getConfigInstance(int offset, int limit, String filters, String globalFilter, String sorting, boolean active, String hostId);
    Result<String> getConfigInstanceById(String hostId, String instanceId, String propertyId);

    // InstanceFile (was ConfigInstanceFile)
    void createConfigInstanceFile(Connection conn, Map<String, Object> event) throws SQLException, Exception;
    void updateConfigInstanceFile(Connection conn, Map<String, Object> event) throws SQLException, Exception;
    void deleteConfigInstanceFile(Connection conn, Map<String, Object> event) throws SQLException, Exception;
    Result<String> getConfigInstanceFile(int offset, int limit, String filters, String globalFilter, String sorting, boolean active, String hostId);
    Result<String> getConfigInstanceFileById(String hostId, String instanceFileId);

    // DeploymentInstanceProperty (was ConfigDeploymentInstance)
    void createConfigDeploymentInstance(Connection conn, Map<String, Object> event) throws SQLException, Exception;
    void updateConfigDeploymentInstance(Connection conn, Map<String, Object> event) throws SQLException, Exception;
    void deleteConfigDeploymentInstance(Connection conn, Map<String, Object> event) throws SQLException, Exception;
    Result<String> getConfigDeploymentInstance(int offset, int limit, String filters, String globalFilter, String sorting, boolean active, String hostId);
    Result<String> getConfigDeploymentInstanceById(String hostId, String deploymentInstanceId, String propertyId);

    // ProductProperty (was ConfigProduct)
    void createConfigProduct(Connection conn, Map<String, Object> event) throws SQLException, Exception;
    void updateConfigProduct(Connection conn, Map<String, Object> event) throws SQLException, Exception;
    void deleteConfigProduct(Connection conn, Map<String, Object> event) throws SQLException, Exception;
    Result<String> getConfigProduct(int offset, int limit, String filters, String globalFilter, String sorting, boolean active);
    Result<String> getConfigProductById(String productId, String propertyId);

    // ProductVersionProperty (was ConfigProductVersion)
    void createConfigProductVersion(Connection conn, Map<String, Object> event) throws SQLException, Exception;
    void updateConfigProductVersion(Connection conn, Map<String, Object> event) throws SQLException, Exception;
    void deleteConfigProductVersion(Connection conn, Map<String, Object> event) throws SQLException, Exception;
    Result<String> getConfigProductVersion(int offset, int limit, String filters, String globalFilter, String sorting, boolean active, String hostId);
    Result<String> getConfigProductVersionById(String hostId, String productVersionId, String propertyId);

    // Rule
    void createRule(Connection conn, Map<String, Object> event) throws SQLException, Exception;
    void updateRule(Connection conn, Map<String, Object> event) throws SQLException, Exception;
    void deleteRule(Connection conn, Map<String, Object> event) throws SQLException, Exception;
    Result<List<Map<String, Object>>> queryRuleByGroup(String groupId);
    Result<String> queryRule(int offset, int limit, String filters, String globalFilter, String sorting, boolean active, String hostId);
    Result<Map<String, Object>> queryRuleById(String ruleId);
    Result<String> queryRuleByType(String ruleType);
    Result<List<Map<String, Object>>> queryRuleByHostApiId(String hostId, String apiId, String apiVersion);

    // Role
    void createRole(Connection conn, Map<String, Object> event) throws SQLException, Exception;
    void updateRole(Connection conn, Map<String, Object> event) throws SQLException, Exception;
    void deleteRole(Connection conn, Map<String, Object> event) throws SQLException, Exception;
    Result<String> queryRole(int offset, int limit, String filters, String globalFilter, String sorting, boolean active, String hostId);
    Result<String> queryRoleLabel(String hostId);
    Result<String> getRoleById(String hostId, String roleId);

    // RolePermission
    Result<String> queryRolePermission(int offset, int limit, String filters, String globalFilter, String sorting, boolean active, String hostId);
    void createRolePermission(Connection conn, Map<String, Object> event) throws SQLException, Exception;
    void deleteRolePermission(Connection conn, Map<String, Object> event) throws SQLException, Exception;

    // RoleUser
    Result<String> queryRoleUser(int offset, int limit, String filters, String globalFilter, String sorting, boolean active, String hostId);
    void createRoleUser(Connection conn, Map<String, Object> event) throws SQLException, Exception;
    void updateRoleUser(Connection conn, Map<String, Object> event) throws SQLException, Exception;
    void deleteRoleUser(Connection conn, Map<String, Object> event) throws SQLException, Exception;
    Result<String> getRoleUserById(String hostId, String roleId, String userId);

    // RoleRowFilter
    Result<String> queryRoleRowFilter(int offset, int limit, String filters, String globalFilter, String sorting, boolean active, String hostId);
    void createRoleRowFilter(Connection conn, Map<String, Object> event) throws SQLException, Exception;
    void updateRoleRowFilter(Connection conn, Map<String, Object> event) throws SQLException, Exception;
    void deleteRoleRowFilter(Connection conn, Map<String, Object> event) throws SQLException, Exception;
    Result<String> getRoleRowFilterById(String hostId, String roleId, String endpointId, String colName);

    // RoleColFilter
    Result<String> queryRoleColFilter(int offset, int limit, String filters, String globalFilter, String sorting, boolean active, String hostId);
    void createRoleColFilter(Connection conn, Map<String, Object> event) throws SQLException, Exception;
    void updateRoleColFilter(Connection conn, Map<String, Object> event) throws SQLException, Exception;
    void deleteRoleColFilter(Connection conn, Map<String, Object> event) throws SQLException, Exception;
    Result<String> getRoleColFilterById(String hostId, String roleId, String endpointId);

    // Group
    void createGroup(Connection conn, Map<String, Object> event) throws SQLException, Exception;
    void updateGroup(Connection conn, Map<String, Object> event) throws SQLException, Exception;
    void deleteGroup(Connection conn, Map<String, Object> event) throws SQLException, Exception;
    Result<String> queryGroup(int offset, int limit, String filters, String globalFilter, String sorting, boolean active, String hostId);
    Result<String> queryGroupLabel(String hostId);
    Result<String> getGroupById(String hostId, String groupId);

    // GroupPermission
    Result<String> queryGroupPermission(int offset, int limit, String filters, String globalFilter, String sorting, boolean active, String hostId);
    void createGroupPermission(Connection conn, Map<String, Object> event) throws SQLException, Exception;
    void deleteGroupPermission(Connection conn, Map<String, Object> event) throws SQLException, Exception;

    // GroupUser
    Result<String> queryGroupUser(int offset, int limit, String filters, String globalFilter, String sorting, boolean active, String hostId);
    void createGroupUser(Connection conn, Map<String, Object> event) throws SQLException, Exception;
    void updateGroupUser(Connection conn, Map<String, Object> event) throws SQLException, Exception;
    void deleteGroupUser(Connection conn, Map<String, Object> event) throws SQLException, Exception;
    Result<String> getGroupUserById(String hostId, String groupId, String userId);

    // GroupRowFilter
    Result<String> queryGroupRowFilter(int offset, int limit, String filters, String globalFilter, String sorting, boolean active, String hostId);
    void createGroupRowFilter(Connection conn, Map<String, Object> event) throws SQLException, Exception;
    void updateGroupRowFilter(Connection conn, Map<String, Object> event) throws SQLException, Exception;
    void deleteGroupRowFilter(Connection conn, Map<String, Object> event) throws SQLException, Exception;
    Result<String> getGroupRowFilterById(String hostId, String groupId, String endpointId, String colName);

    // GroupColFilter
    Result<String> queryGroupColFilter(int offset, int limit, String filters, String globalFilter, String sorting, boolean active, String hostId);
    void createGroupColFilter(Connection conn, Map<String, Object> event) throws SQLException, Exception;
    void updateGroupColFilter(Connection conn, Map<String, Object> event) throws SQLException, Exception;
    void deleteGroupColFilter(Connection conn, Map<String, Object> event) throws SQLException, Exception;
    Result<String> getGroupColFilterById(String hostId, String groupId, String endpointId);

    // Position
    void createPosition(Connection conn, Map<String, Object> event) throws SQLException, Exception;
    void updatePosition(Connection conn, Map<String, Object> event) throws SQLException, Exception;
    void deletePosition(Connection conn, Map<String, Object> event) throws SQLException, Exception;
    Result<String> queryPosition(int offset, int limit, String filters, String globalFilter, String sorting, boolean active, String hostId);
    Result<String> queryPositionLabel(String hostId);
    Result<String> getPositionById(String hostId, String positionId);

    // PositionPermission
    Result<String> queryPositionPermission(int offset, int limit, String filters, String globalFilter, String sorting, boolean active, String hostId);
    void createPositionPermission(Connection conn, Map<String, Object> event) throws SQLException, Exception;
    void deletePositionPermission(Connection conn, Map<String, Object> event) throws SQLException, Exception;

    // PositionUser
    Result<String> queryPositionUser(int offset, int limit, String filters, String globalFilter, String sorting, boolean active, String hostId);
    void createPositionUser(Connection conn, Map<String, Object> event) throws SQLException, Exception;
    void updatePositionUser(Connection conn, Map<String, Object> event) throws SQLException, Exception;
    void deletePositionUser(Connection conn, Map<String, Object> event) throws SQLException, Exception;
    Result<String> getPositionUserById(String hostId, String positionId, String employeeId);

    // PositionRowFilter
    Result<String> queryPositionRowFilter(int offset, int limit, String filters, String globalFilter, String sorting, boolean active, String hostId);
    void createPositionRowFilter(Connection conn, Map<String, Object> event) throws SQLException, Exception;
    void updatePositionRowFilter(Connection conn, Map<String, Object> event) throws SQLException, Exception;
    void deletePositionRowFilter(Connection conn, Map<String, Object> event) throws SQLException, Exception;
    Result<String> getPositionRowFilterById(String hostId, String positionId, String endpointId, String colName);

    // PositionColFilter
    Result<String> queryPositionColFilter(int offset, int limit, String filters, String globalFilter, String sorting, boolean active, String hostId);
    void createPositionColFilter(Connection conn, Map<String, Object> event) throws SQLException, Exception;
    void updatePositionColFilter(Connection conn, Map<String, Object> event) throws SQLException, Exception;
    void deletePositionColFilter(Connection conn, Map<String, Object> event) throws SQLException, Exception;
    Result<String> getPositionColFilterById(String hostId, String positionId, String endpointId);

    // Attribute
    void createAttribute(Connection conn, Map<String, Object> event) throws SQLException, Exception;
    void updateAttribute(Connection conn, Map<String, Object> event) throws SQLException, Exception;
    void deleteAttribute(Connection conn, Map<String, Object> event) throws SQLException, Exception;
    Result<String> queryAttribute(int offset, int limit, String filters, String globalFilter, String sorting, boolean active, String hostId);
    Result<String> queryAttributeLabel(String hostId);
    Result<String> getAttributeById(String hostId, String attributeId);

    // AttributePermission
    Result<String> queryAttributePermission(int offset, int limit, String filters, String globalFilter, String sorting, boolean active, String hostId);
    void createAttributePermission(Connection conn, Map<String, Object> event) throws SQLException, Exception;
    void updateAttributePermission(Connection conn, Map<String, Object> event) throws SQLException, Exception;
    void deleteAttributePermission(Connection conn, Map<String, Object> event) throws SQLException, Exception;
    Result<String> getAttributePermissionById(String hostId, String attributeId, String endpointId);

    // AttributeUser
    Result<String> queryAttributeUser(int offset, int limit, String filters, String globalFilter, String sorting, boolean active, String hostId);
    void createAttributeUser(Connection conn, Map<String, Object> event) throws SQLException, Exception;
    void updateAttributeUser(Connection conn, Map<String, Object> event) throws SQLException, Exception;
    void deleteAttributeUser(Connection conn, Map<String, Object> event) throws SQLException, Exception;
    Result<String> getAttributeUserById(String hostId, String attributeId, String userId);

    // AttributeRowFilter
    Result<String> queryAttributeRowFilter(int offset, int limit, String filters, String globalFilter, String sorting, boolean active, String hostId);
    void createAttributeRowFilter(Connection conn, Map<String, Object> event) throws SQLException, Exception;
    void updateAttributeRowFilter(Connection conn, Map<String, Object> event) throws SQLException, Exception;
    void deleteAttributeRowFilter(Connection conn, Map<String, Object> event) throws SQLException, Exception;
    Result<String> getAttributeRowFilterById(String hostId, String attributeId, String endpointId, String colName);

    // AttributeColFilter
    Result<String> queryAttributeColFilter(int offset, int limit, String filters, String globalFilter, String sorting, boolean active, String hostId);
    void createAttributeColFilter(Connection conn, Map<String, Object> event) throws SQLException, Exception;
    void updateAttributeColFilter(Connection conn, Map<String, Object> event) throws SQLException, Exception;
    void deleteAttributeColFilter(Connection conn, Map<String, Object> event) throws SQLException, Exception;
    Result<String> getAttributeColFilterById(String hostId, String attributeId, String endpointId);

    // ProductVersion (renamed from Product in previous step)
    void createProduct(Connection conn, Map<String, Object> event) throws SQLException, Exception;
    void updateProduct(Connection conn, Map<String, Object> event) throws SQLException, Exception;
    void deleteProduct(Connection conn, Map<String, Object> event) throws SQLException, Exception;
    Result<String> getProduct(int offset, int limit, String filters, String globalFilter, String sorting, boolean active, String hostId);
    Result<String> getProductVersion(String hostId, String productVersionId);
    Result<String> getProductIdLabel(String hostId);
    Result<String> getProductVersionLabel(String hostId, String productId);
    Result<String> getProductVersionIdLabel(String hostId);
    String getProductVersionId(String hostId, String productId, String productVersion);
    String queryProductVersionId(String hostId, String productId, String light4jVersion);

    // ProductVersionEnvironment
    void createProductVersionEnvironment(Connection conn, Map<String, Object> event) throws SQLException, Exception;
    void updateProductVersionEnvironment(Connection conn, Map<String, Object> event) throws SQLException, Exception;
    void deleteProductVersionEnvironment(Connection conn, Map<String, Object> event) throws SQLException, Exception;
    Result<String> getProductVersionEnvironment(int offset, int limit, String filters, String globalFilter, String sorting, boolean active,  String hostId);
    Result<String> getProductVersionEnvironmentById(String hostId, String productVersionId, String systemEnv, String runtimeEnv);

    // ProductVersionPipeline
    void createProductVersionPipeline(Connection conn, Map<String, Object> event) throws SQLException, Exception;
    void deleteProductVersionPipeline(Connection conn, Map<String, Object> event) throws SQLException, Exception;
    Result<String> getProductVersionPipeline(int offset, int limit, String filters, String globalFilter, String sorting, boolean active, String hostId);

    // ProductVersionConfig
    void createProductVersionConfig(Connection conn, Map<String, Object> event) throws SQLException, Exception;
    void deleteProductVersionConfig(Connection conn, Map<String, Object> event) throws SQLException, Exception;
    Result<String> getProductVersionConfig(int offset, int limit, String filters, String globalFilter, String sorting, boolean active, String hostId);

    // ProductVersionConfigProperty
    void createProductVersionConfigProperty(Connection conn, Map<String, Object> event) throws SQLException, Exception;
    void deleteProductVersionConfigProperty(Connection conn, Map<String, Object> event) throws SQLException, Exception;
    Result<String> getProductVersionConfigProperty(int offset, int limit, String filters, String globalFilter, String sorting, boolean active, String hostId);

    // Instance
    void createInstance(Connection conn, Map<String, Object> event) throws SQLException, Exception;
    void updateInstance(Connection conn, Map<String, Object> event) throws SQLException, Exception;
    void deleteInstance(Connection conn, Map<String, Object> event) throws SQLException, Exception;
    void lockInstance(Connection conn, Map<String, Object> event) throws Exception;
    void unlockInstance(Connection conn, Map<String, Object> event) throws Exception;
    void cloneInstance(Connection conn, Map<String, Object> event) throws Exception;
    void promoteInstance(Connection conn, Map<String, Object> event) throws Exception;
    Result<String> getInstance(int offset, int limit, String filters, String globalFilter, String sorting, boolean active, String hostId);
    Result<String> getInstanceLabel(String hostId);
    Result<String> getInstanceById(String hostId, String instanceId);
    String getInstanceId(String hostId, String serviceId, String envTag, String productVersionId);

    // Pipeline
    void createPipeline(Connection conn, Map<String, Object> event) throws SQLException, Exception;
    void updatePipeline(Connection conn, Map<String, Object> event) throws SQLException, Exception;
    void deletePipeline(Connection conn, Map<String, Object> event) throws SQLException, Exception;
    Result<String> getPipeline(int offset, int limit, String filters, String globalFilter, String sorting, boolean active, String hostId);
    Result<String> getPipelineLabel(String hostId);
    Result<String> getPipelineById(String hostId, String pipelineId);
    String getPipelineId(String hostId, String platformId, String pipelineName, String pipelineVersion);

    // Platform
    void createPlatform(Connection conn, Map<String, Object> event) throws SQLException, Exception;
    void updatePlatform(Connection conn, Map<String, Object> event) throws SQLException, Exception;
    void deletePlatform(Connection conn, Map<String, Object> event) throws SQLException, Exception;
    Result<String> getPlatform(int offset, int limit, String filters, String globalFilter, String sorting, boolean active, String hostId);
    Result<String> getPlatformLabel(String hostId);
    Result<String> getPlatformById(String hostId, String platformId);
    String getPlatformId(String hostId, String platformName, String platformVersion);

    // DeploymentInstance
    void createDeploymentInstance(Connection conn, Map<String, Object> event) throws SQLException, Exception;
    void updateDeploymentInstance(Connection conn, Map<String, Object> event) throws SQLException, Exception;
    void deleteDeploymentInstance(Connection conn, Map<String, Object> event) throws SQLException, Exception;
    Result<String> getDeploymentInstance(int offset, int limit, String filters, String globalFilter, String sorting, boolean active, String hostId);
    Result<String> getDeploymentInstancePipeline(String hostId, String instanceId, String systemEnv, String runtimeEnv);
    Result<String> getDeploymentInstanceLabel(String hostId, String instanceId);
    Result<String> getDeploymentInstanceById(String hostId, String deploymentInstanceId);
    String getDeploymentInstanceId(String hostId, String instanceId, String serviceId);

    // Deployment
    void createDeployment(Connection conn, Map<String, Object> event) throws SQLException, Exception;
    void updateDeployment(Connection conn, Map<String, Object> event) throws SQLException, Exception;
    void updateDeploymentJobId(Connection conn, Map<String, Object> event) throws SQLException, Exception;
    void updateDeploymentStatus(Connection conn, Map<String, Object> event) throws SQLException, Exception;
    void deleteDeployment(Connection conn, Map<String, Object> event) throws SQLException, Exception; // Changed from Result<String> to void
    Result<String> getDeployment(int offset, int limit, String filters, String globalFilter, String sorting, boolean active, String hostId);
    Result<String> getDeploymentById(String hostId, String deploymentId);

    // Category
    void createCategory(Connection conn, Map<String, Object> event) throws SQLException, Exception;
    void updateCategory(Connection conn, Map<String, Object> event) throws SQLException, Exception;
    void deleteCategory(Connection conn, Map<String, Object> event) throws SQLException, Exception;
    void createEntityCategory(Connection conn, Map<String, Object> event) throws SQLException, Exception;
    void updateEntityCategory(Connection conn, Map<String, Object> event) throws SQLException, Exception;
    void deleteEntityCategory(Connection conn, Map<String, Object> event) throws SQLException, Exception;

    Result<String> getCategory(int offset, int limit, String filters, String globalFilter, String sorting, boolean active, String hostId);
    Result<String> getCategoryLabel(String hostId);
    Result<String> getCategoryById(String categoryId);
    Result<String> getCategoryByName(String hostId, String categoryName);
    Result<String> getCategoryByType(String hostId, String entityType);
    Result<String> getCategoryTree(String hostId, String entityType);

    // Schema
    void createSchema(Connection conn, Map<String, Object> event) throws SQLException, Exception;
    void updateSchema(Connection conn, Map<String, Object> event) throws SQLException, Exception;
    void deleteSchema(Connection conn, Map<String, Object> event) throws SQLException, Exception;
    Result<String> getSchema(int offset, int limit, String filters, String globalFilter, String sorting, boolean active, String hostId);
    Result<String> getSchemaLabel(String hostId);
    Result<String> getSchemaById(String schemaId);
    Result<String> getSchemaByCategoryId(String categoryId);
    Result<String> getSchemaByTagId(String tagId);

    // Tag
    void createTag(Connection conn, Map<String, Object> event) throws SQLException, Exception;
    void updateTag(Connection conn, Map<String, Object> event) throws SQLException, Exception;
    void deleteTag(Connection conn, Map<String, Object> event) throws SQLException, Exception;

    void createEntityTag(Connection conn, Map<String, Object> event) throws SQLException, Exception;
    void updateEntityTag(Connection conn, Map<String, Object> event) throws SQLException, Exception;
    void deleteEntityTag(Connection conn, Map<String, Object> event) throws SQLException, Exception;

    Result<String> getTag(int offset, int limit, String filters, String globalFilter, String sorting, boolean active, String hostId);
    Result<String> getTagLabel(String hostId);
    Result<String> getTagById(String tagId);
    Result<String> getTagByName(String hostId, String tagName);
    Result<String> getTagByType(String hostId, String entityType);

    // Schedule
    void createSchedule(Connection conn, Map<String, Object> event) throws SQLException, Exception;
    void updateSchedule(Connection conn, Map<String, Object> event) throws SQLException, Exception;
    void deleteSchedule(Connection conn, Map<String, Object> event) throws SQLException, Exception;
    Result<String> getSchedule(int offset, int limit, String filters, String globalFilter, String sorting, boolean active, String hostId);
    Result<String> getScheduleLabel(String hostId);
    Result<String> getScheduleById(String scheduleId);
    int acquireLock(String instanceId, int lockId, OffsetDateTime lockTimeout) throws Exception;
    int renewLock(String instanceId, int lockId) throws Exception;
    int releaseLock(String instanceId, int lockId) throws Exception;
    Result<List<Map<String, Object>>> pollTasks(OffsetDateTime nextRunTs);
    Result<String> executeTask(Map<String, Object> taskData, long executionTimeMillis);

    // Event Store
    Result<String> insertEventStore(CloudEvent[] events);
    int getMaxAggregateVersion(String aggregateId);

    /**
     * Builds the CloudEvent object array from the provided map, eventType, aggregateId,
     * aggregateType, userId, host, and nonce. This allows light-portal components to
     * directly push the event into kafka without calling the command handler API endpoint.
     */
    default CloudEvent[] buildCloudEvent(Map<String, Object> map, String eventType, String aggregateId,
                                       String aggregateType, String userId, String host) {

        long nonce = queryNonceByUserId(userId);
        if(logger.isTraceEnabled()) logger.trace("nonce = {}", nonce);

        CloudEventBuilder eventTemplate = CloudEventBuilder.v1()
                .withSource(PortalConstants.EVENT_SOURCE)
                .withType(eventType);

        String data = JsonMapper.toJson(map);
        return new CloudEvent[]{eventTemplate.newBuilder()
                .withId(UuidUtil.getUUID().toString())
                .withTime(OffsetDateTime.now())
                .withSubject(aggregateId)
                .withExtension(Constants.USER, userId)
                .withExtension(PortalConstants.NONCE, nonce)
                .withExtension(Constants.HOST, host)
                .withExtension(PortalConstants.AGGREGATE_TYPE, aggregateType)
                .withExtension(PortalConstants.EVENT_AGGREGATE_VERSION, (Number)map.get(PortalConstants.NEW_AGGREGATE_VERSION))
                .withData("application/json", data.getBytes(StandardCharsets.UTF_8))
                .build()};
    }
    // Product / Instance Applicable Properties
    Result<String> getApplicableConfigPropertiesForInstance(int offset, int limit, String hostId, String instanceId, Set<String> resourceTypes, Set<String> configTypes, Set<String> propertyTypes);
    Result<String> getApplicableConfigPropertiesForInstanceApi(int offset, int limit, String hostId, String instanceApiId);
    Result<String> getApplicableConfigPropertiesForInstanceApp(int offset, int limit, String hostId, String instanceAppId);
    Result<String> getApplicableConfigPropertiesForInstanceAppApi(int offset, int limit, String hostId, String instanceAppId, String instanceApiId);

    // Aggregations
    Result<String> getAllAggregatedInstanceRuntimeConfigs(String hostId, String instanceId);
    Result<String> getPromotableInstanceConfigs(String hostId, String instanceId,Set<String> propertyNames,Set<String> apiUids);

    /**
     * Handles an event by dispatching it to the appropriate persistence method.
     * This logic is shared between various event consumers (Kafka, Postgres).
     *
     * @param conn  The database connection (active transaction)
     * @param event The event map
     * @throws SQLException if a database error occurs
     * @throws Exception    if an unexpected error occurs
     */
    default void handleEvent(Connection conn, Map<String, Object> event) throws SQLException, Exception {
        Logger logger = LoggerFactory.getLogger(PortalDbProvider.class);
        String eventType = (String) event.get(CloudEventV1.TYPE);
        if (logger.isTraceEnabled()) logger.trace("Event type {} event {}", eventType, JsonMapper.toJson(event));
        switch (eventType) {
            // --- User Events ---
            case PortalConstants.USER_CREATED_EVENT: createUser(conn, event); break;
            case PortalConstants.USER_ONBOARDED_EVENT: onboardUser(conn, event); break;
            case PortalConstants.SOCIAL_USER_CREATED_EVENT: createSocialUser(conn, event); break;
            case PortalConstants.USER_CONFIRMED_EVENT: confirmUser(conn, event); break;
            case PortalConstants.USER_VERIFIED_EVENT: verifyUser(conn, event); break;
            case PortalConstants.USER_UPDATED_EVENT: updateUser(conn, event); break;
            case PortalConstants.PASSWORD_FORGOT_EVENT: forgetPassword(conn, event); break;
            case PortalConstants.PASSWORD_RESET_EVENT: resetPassword(conn, event); break;
            case PortalConstants.PASSWORD_CHANGED_EVENT: changePassword(conn, event); break;
            case PortalConstants.USER_DELETED_EVENT: deleteUser(conn, event); break;

            // --- Org Events ---
            case PortalConstants.ORG_CREATED_EVENT: createOrg(conn, event); break;
            case PortalConstants.ORG_UPDATED_EVENT: updateOrg(conn, event); break;
            case PortalConstants.ORG_DELETED_EVENT: deleteOrg(conn, event); break;

            // --- Host Events ---
            case PortalConstants.HOST_CREATED_EVENT: createHost(conn, event); break;
            case PortalConstants.HOST_UPDATED_EVENT: updateHost(conn, event); break;
            case PortalConstants.HOST_DELETED_EVENT: deleteHost(conn, event); break;
            case PortalConstants.USER_HOST_SWITCHED_EVENT: switchUserHost(conn, event); break;
            case PortalConstants.USER_HOST_CREATED_EVENT: createUserHost(conn, event); break;
            case PortalConstants.USER_HOST_DELETED_EVENT: deleteUserHost(conn, event); break;

            // --- Payment Events ---
            case PortalConstants.PAYMENT_UPDATED_EVENT: updatePayment(conn, event); break;
            case PortalConstants.PAYMENT_DELETED_EVENT: deletePayment(conn, event); break;

            // --- Message Events ---
            case PortalConstants.PRIVATE_MESSAGE_SENT_EVENT: sendPrivateMessage(conn, event); break;

            // --- Attribute Events ---
            case PortalConstants.ATTRIBUTE_CREATED_EVENT: createAttribute(conn, event); break;
            case PortalConstants.ATTRIBUTE_UPDATED_EVENT: updateAttribute(conn, event); break;
            case PortalConstants.ATTRIBUTE_DELETED_EVENT: deleteAttribute(conn, event); break;
            case PortalConstants.ATTRIBUTE_PERMISSION_CREATED_EVENT: createAttributePermission(conn, event); break;
            case PortalConstants.ATTRIBUTE_PERMISSION_UPDATED_EVENT: updateAttributePermission(conn, event); break;
            case PortalConstants.ATTRIBUTE_PERMISSION_DELETED_EVENT: deleteAttributePermission(conn, event); break;
            case PortalConstants.ATTRIBUTE_USER_CREATED_EVENT: createAttributeUser(conn, event); break;
            case PortalConstants.ATTRIBUTE_USER_UPDATED_EVENT: updateAttributeUser(conn, event); break;
            case PortalConstants.ATTRIBUTE_USER_DELETED_EVENT: deleteAttributeUser(conn, event); break;
            case PortalConstants.ATTRIBUTE_ROW_FILTER_CREATED_EVENT: createAttributeRowFilter(conn, event); break;
            case PortalConstants.ATTRIBUTE_ROW_FILTER_UPDATED_EVENT: updateAttributeRowFilter(conn, event); break;
            case PortalConstants.ATTRIBUTE_ROW_FILTER_DELETED_EVENT: deleteAttributeRowFilter(conn, event); break;
            case PortalConstants.ATTRIBUTE_COL_FILTER_CREATED_EVENT: createAttributeColFilter(conn, event); break;
            case PortalConstants.ATTRIBUTE_COL_FILTER_UPDATED_EVENT: updateAttributeColFilter(conn, event); break;
            case PortalConstants.ATTRIBUTE_COL_FILTER_DELETED_EVENT: deleteAttributeColFilter(conn, event); break;

            // --- Group Events ---
            case PortalConstants.GROUP_CREATED_EVENT: createGroup(conn, event); break;
            case PortalConstants.GROUP_UPDATED_EVENT: updateGroup(conn, event); break;
            case PortalConstants.GROUP_DELETED_EVENT: deleteGroup(conn, event); break;
            case PortalConstants.GROUP_PERMISSION_CREATED_EVENT: createGroupPermission(conn, event); break;
            case PortalConstants.GROUP_PERMISSION_DELETED_EVENT: deleteGroupPermission(conn, event); break;
            case PortalConstants.GROUP_USER_CREATED_EVENT: createGroupUser(conn, event); break;
            case PortalConstants.GROUP_USER_UPDATED_EVENT: updateGroupUser(conn, event); break;
            case PortalConstants.GROUP_USER_DELETED_EVENT: deleteGroupUser(conn, event); break;
            case PortalConstants.GROUP_ROW_FILTER_CREATED_EVENT: createGroupRowFilter(conn, event); break;
            case PortalConstants.GROUP_ROW_FILTER_UPDATED_EVENT: updateGroupRowFilter(conn, event); break;
            case PortalConstants.GROUP_ROW_FILTER_DELETED_EVENT: deleteGroupRowFilter(conn, event); break;
            case PortalConstants.GROUP_COL_FILTER_CREATED_EVENT: createGroupColFilter(conn, event); break;
            case PortalConstants.GROUP_COL_FILTER_UPDATED_EVENT: updateGroupColFilter(conn, event); break;
            case PortalConstants.GROUP_COL_FILTER_DELETED_EVENT: deleteGroupColFilter(conn, event); break;

            // --- Role Events ---
            case PortalConstants.ROLE_CREATED_EVENT: createRole(conn, event); break;
            case PortalConstants.ROLE_UPDATED_EVENT: updateRole(conn, event); break;
            case PortalConstants.ROLE_DELETED_EVENT: deleteRole(conn, event); break;
            case PortalConstants.ROLE_PERMISSION_CREATED_EVENT: createRolePermission(conn, event); break;
            case PortalConstants.ROLE_PERMISSION_DELETED_EVENT: deleteRolePermission(conn, event); break;
            case PortalConstants.ROLE_USER_CREATED_EVENT: createRoleUser(conn, event); break;
            case PortalConstants.ROLE_USER_UPDATED_EVENT: updateRoleUser(conn, event); break;
            case PortalConstants.ROLE_USER_DELETED_EVENT: deleteRoleUser(conn, event); break;
            case PortalConstants.ROLE_ROW_FILTER_CREATED_EVENT: createRoleRowFilter(conn, event); break;
            case PortalConstants.ROLE_ROW_FILTER_UPDATED_EVENT: updateRoleRowFilter(conn, event); break;
            case PortalConstants.ROLE_ROW_FILTER_DELETED_EVENT: deleteRoleRowFilter(conn, event); break;
            case PortalConstants.ROLE_COL_FILTER_CREATED_EVENT: createRoleColFilter(conn, event); break;
            case PortalConstants.ROLE_COL_FILTER_UPDATED_EVENT: updateRoleColFilter(conn, event); break;
            case PortalConstants.ROLE_COL_FILTER_DELETED_EVENT: deleteRoleColFilter(conn, event); break;

            // --- Position Events ---
            case PortalConstants.POSITION_CREATED_EVENT: createPosition(conn, event); break;
            case PortalConstants.POSITION_UPDATED_EVENT: updatePosition(conn, event); break;
            case PortalConstants.POSITION_DELETED_EVENT: deletePosition(conn, event); break;
            case PortalConstants.POSITION_PERMISSION_CREATED_EVENT: createPositionPermission(conn, event); break;
            case PortalConstants.POSITION_PERMISSION_DELETED_EVENT: deletePositionPermission(conn, event); break;
            case PortalConstants.POSITION_USER_CREATED_EVENT: createPositionUser(conn, event); break;
            case PortalConstants.POSITION_USER_UPDATED_EVENT: updatePositionUser(conn, event); break;
            case PortalConstants.POSITION_USER_DELETED_EVENT: deletePositionUser(conn, event); break;
            case PortalConstants.POSITION_ROW_FILTER_CREATED_EVENT: createPositionRowFilter(conn, event); break;
            case PortalConstants.POSITION_ROW_FILTER_UPDATED_EVENT: updatePositionRowFilter(conn, event); break;
            case PortalConstants.POSITION_ROW_FILTER_DELETED_EVENT: deletePositionRowFilter(conn, event); break;
            case PortalConstants.POSITION_COL_FILTER_CREATED_EVENT: createPositionColFilter(conn, event); break;
            case PortalConstants.POSITION_COL_FILTER_UPDATED_EVENT: updatePositionColFilter(conn, event); break;
            case PortalConstants.POSITION_COL_FILTER_DELETED_EVENT: deletePositionColFilter(conn, event); break;

            // --- Rule Events ---
            case PortalConstants.RULE_CREATED_EVENT: createRule(conn, event); break;
            case PortalConstants.RULE_UPDATED_EVENT: updateRule(conn, event); break;
            case PortalConstants.RULE_DELETED_EVENT: deleteRule(conn, event); break;

            // --- Schema Events ---
            case PortalConstants.SCHEMA_CREATED_EVENT: createSchema(conn, event); break;
            case PortalConstants.SCHEMA_UPDATED_EVENT: updateSchema(conn, event); break;
            case PortalConstants.SCHEMA_DELETED_EVENT: deleteSchema(conn, event); break;

            // --- Schedule Events ---
            case PortalConstants.SCHEDULE_CREATED_EVENT: createSchedule(conn, event); break;
            case PortalConstants.SCHEDULE_UPDATED_EVENT: updateSchedule(conn, event); break;
            case PortalConstants.SCHEDULE_DELETED_EVENT: deleteSchedule(conn, event); break;

            // --- Category Events ---
            case PortalConstants.CATEGORY_CREATED_EVENT: createCategory(conn, event); break;
            case PortalConstants.CATEGORY_UPDATED_EVENT: updateCategory(conn, event); break;
            case PortalConstants.CATEGORY_DELETED_EVENT: deleteCategory(conn, event); break;

            // --- Tag Events ---
            case PortalConstants.TAG_CREATED_EVENT: createTag(conn, event); break;
            case PortalConstants.TAG_UPDATED_EVENT: updateTag(conn, event); break;
            case PortalConstants.TAG_DELETED_EVENT: deleteTag(conn, event); break;

            // --- Service Events ---
            case PortalConstants.API_CREATED_EVENT: createApi(conn, event); break;
            case PortalConstants.API_UPDATED_EVENT: updateApi(conn, event); break;
            case PortalConstants.API_DELETED_EVENT: deleteApi(conn, event); break;
            case PortalConstants.API_VERSION_CREATED_EVENT: createApiVersion(conn, event); break;
            case PortalConstants.API_VERSION_UPDATED_EVENT: updateApiVersion(conn, event); break;
            case PortalConstants.API_VERSION_DELETED_EVENT: deleteApiVersion(conn, event); break;
            case PortalConstants.API_ENDPOINT_RULE_CREATED_EVENT: createApiEndpointRule(conn, event); break;
            case PortalConstants.API_ENDPOINT_RULE_DELETED_EVENT: deleteApiEndpointRule(conn, event); break;
            case PortalConstants.API_VERSION_SPEC_UPDATED_EVENT: updateApiVersionSpec(conn, event); break;

            // --- Auth Events ---
            case PortalConstants.AUTH_REFRESH_TOKEN_CREATED_EVENT: createRefreshToken(conn, event); break;
            case PortalConstants.AUTH_REFRESH_TOKEN_DELETED_EVENT: deleteRefreshToken(conn, event); break;
            case PortalConstants.AUTH_CODE_CREATED_EVENT: createAuthCode(conn, event); break;
            case PortalConstants.AUTH_CODE_DELETED_EVENT: deleteAuthCode(conn, event); break;
            case PortalConstants.AUTH_PROVIDER_CREATED_EVENT: createAuthProvider(conn, event); break;
            case PortalConstants.AUTH_PROVIDER_ROTATED_EVENT: rotateAuthProvider(conn, event); break;
            case PortalConstants.AUTH_PROVIDER_UPDATED_EVENT: updateAuthProvider(conn, event); break;
            case PortalConstants.AUTH_PROVIDER_DELETED_EVENT: deleteAuthProvider(conn, event); break;
            case PortalConstants.AUTH_PROVIDER_API_CREATED_EVENT: createAuthProviderApi(conn, event); break;
            case PortalConstants.AUTH_PROVIDER_API_DELETED_EVENT: deleteAuthProviderApi(conn, event); break;
            case PortalConstants.AUTH_PROVIDER_CLIENT_CREATED_EVENT: createAuthProviderClient(conn, event); break;
            case PortalConstants.AUTH_PROVIDER_CLIENT_DELETED_EVENT: deleteAuthProviderClient(conn, event); break;

            // --- Product Events ---
            case PortalConstants.PRODUCT_VERSION_CREATED_EVENT: createProduct(conn, event); break;
            case PortalConstants.PRODUCT_VERSION_UPDATED_EVENT: updateProduct(conn, event); break;
            case PortalConstants.PRODUCT_VERSION_DELETED_EVENT: deleteProduct(conn, event); break;
            case PortalConstants.PRODUCT_VERSION_ENVIRONMENT_CREATED_EVENT: createProductVersionEnvironment(conn, event); break;
            case PortalConstants.PRODUCT_VERSION_ENVIRONMENT_UPDATED_EVENT: updateProductVersionEnvironment(conn, event); break;
            case PortalConstants.PRODUCT_VERSION_ENVIRONMENT_DELETED_EVENT: deleteProductVersionEnvironment(conn, event); break;
            case PortalConstants.PRODUCT_VERSION_PIPELINE_CREATED_EVENT: createProductVersionPipeline(conn, event); break;
            case PortalConstants.PRODUCT_VERSION_PIPELINE_DELETED_EVENT: deleteProductVersionPipeline(conn, event); break;
            case PortalConstants.PRODUCT_VERSION_CONFIG_CREATED_EVENT: createProductVersionConfig(conn, event); break;
            case PortalConstants.PRODUCT_VERSION_CONFIG_DELETED_EVENT: deleteProductVersionConfig(conn, event); break;
            case PortalConstants.PRODUCT_VERSION_CONFIG_PROPERTY_CREATED_EVENT: createProductVersionConfigProperty(conn, event); break;
            case PortalConstants.PRODUCT_VERSION_CONFIG_PROPERTY_DELETED_EVENT: deleteProductVersionConfigProperty(conn, event); break;

            // --- Pipeline Events ---
            case PortalConstants.PIPELINE_CREATED_EVENT: createPipeline(conn, event); break;
            case PortalConstants.PIPELINE_UPDATED_EVENT: updatePipeline(conn, event); break;
            case PortalConstants.PIPELINE_DELETED_EVENT: deletePipeline(conn, event); break;

            // --- Platform Events ---
            case PortalConstants.PLATFORM_CREATED_EVENT: createPlatform(conn, event); break;
            case PortalConstants.PLATFORM_UPDATED_EVENT: updatePlatform(conn, event); break;
            case PortalConstants.PLATFORM_DELETED_EVENT: deletePlatform(conn, event); break;

            // --- Instance Events ---
            case PortalConstants.INSTANCE_CREATED_EVENT: createInstance(conn, event); break;
            case PortalConstants.INSTANCE_UPDATED_EVENT: updateInstance(conn, event); break;
            case PortalConstants.INSTANCE_DELETED_EVENT: deleteInstance(conn, event); break;
            case PortalConstants.INSTANCE_LOCKED_EVENT: lockInstance(conn, event); break;
            case PortalConstants.INSTANCE_UNLOCKED_EVENT: unlockInstance(conn, event); break;
            case PortalConstants.INSTANCE_CLONED_EVENT: cloneInstance(conn, event); break;
            case PortalConstants.INSTANCE_PROMOTED_EVENT: promoteInstance(conn, event); break;
            case PortalConstants.INSTANCE_API_CREATED_EVENT: createInstanceApi(conn, event); break;
            case PortalConstants.INSTANCE_API_DELETED_EVENT: deleteInstanceApi(conn, event); break;
            case PortalConstants.INSTANCE_APP_CREATED_EVENT: createInstanceApp(conn, event); break;
            case PortalConstants.INSTANCE_APP_DELETED_EVENT: deleteInstanceApp(conn, event); break;
            case PortalConstants.INSTANCE_APP_API_CREATED_EVENT: createInstanceAppApi(conn, event); break;
            case PortalConstants.INSTANCE_APP_API_DELETED_EVENT: deleteInstanceAppApi(conn, event); break;
            case PortalConstants.INSTANCE_API_PATH_PREFIX_CREATED_EVENT: createInstanceApiPathPrefix(conn, event); break;
            case PortalConstants.INSTANCE_API_PATH_PREFIX_UPDATED_EVENT: updateInstanceApiPathPrefix(conn, event); break;
            case PortalConstants.INSTANCE_API_PATH_PREFIX_DELETED_EVENT: deleteInstanceApiPathPrefix(conn, event); break;

            // --- Deployment Events ---
            case PortalConstants.DEPLOYMENT_CREATED_EVENT: createDeployment(conn, event); break;
            case PortalConstants.DEPLOYMENT_UPDATED_EVENT: updateDeployment(conn, event); break;
            case PortalConstants.DEPLOYMENT_JOB_ID_UPDATED_EVENT: updateDeploymentJobId(conn, event); break;
            case PortalConstants.DEPLOYMENT_STATUS_UPDATED_EVENT: updateDeploymentStatus(conn, event); break;
            case PortalConstants.DEPLOYMENT_DELETED_EVENT: deleteDeployment(conn, event); break;

            // --- Deployment Instance Events ---
            case PortalConstants.DEPLOYMENT_INSTANCE_CREATED_EVENT: createDeploymentInstance(conn, event); break;
            case PortalConstants.DEPLOYMENT_INSTANCE_UPDATED_EVENT: updateDeploymentInstance(conn, event); break;
            case PortalConstants.DEPLOYMENT_INSTANCE_DELETED_EVENT: deleteDeploymentInstance(conn, event); break;

            // --- Config Events ---
            case PortalConstants.CONFIG_CREATED_EVENT: createConfig(conn, event); break;
            case PortalConstants.CONFIG_UPDATED_EVENT: updateConfig(conn, event); break;
            case PortalConstants.CONFIG_DELETED_EVENT: deleteConfig(conn, event); break;
            case PortalConstants.CONFIG_PROPERTY_CREATED_EVENT: createConfigProperty(conn, event); break;
            case PortalConstants.CONFIG_PROPERTY_UPDATED_EVENT: updateConfigProperty(conn, event); break;
            case PortalConstants.CONFIG_PROPERTY_DELETED_EVENT: deleteConfigProperty(conn, event); break;
            case PortalConstants.CONFIG_ENVIRONMENT_CREATED_EVENT: createConfigEnvironment(conn, event); break;
            case PortalConstants.CONFIG_ENVIRONMENT_UPDATED_EVENT: updateConfigEnvironment(conn, event); break;
            case PortalConstants.CONFIG_ENVIRONMENT_DELETED_EVENT: deleteConfigEnvironment(conn, event); break;
            case PortalConstants.CONFIG_INSTANCE_API_CREATED_EVENT: createConfigInstanceApi(conn, event); break;
            case PortalConstants.CONFIG_INSTANCE_API_UPDATED_EVENT: updateConfigInstanceApi(conn, event); break;
            case PortalConstants.CONFIG_INSTANCE_API_DELETED_EVENT: deleteConfigInstanceApi(conn, event); break;
            case PortalConstants.CONFIG_INSTANCE_APP_CREATED_EVENT: createConfigInstanceApp(conn, event); break;
            case PortalConstants.CONFIG_INSTANCE_APP_UPDATED_EVENT: updateConfigInstanceApp(conn, event); break;
            case PortalConstants.CONFIG_INSTANCE_APP_DELETED_EVENT: deleteConfigInstanceApp(conn, event); break;
            case PortalConstants.CONFIG_INSTANCE_APP_API_CREATED_EVENT: createConfigInstanceAppApi(conn, event); break;
            case PortalConstants.CONFIG_INSTANCE_APP_API_UPDATED_EVENT: updateConfigInstanceAppApi(conn, event); break;
            case PortalConstants.CONFIG_INSTANCE_APP_API_DELETED_EVENT: deleteConfigInstanceAppApi(conn, event); break;
            case PortalConstants.CONFIG_INSTANCE_FILE_CREATED_EVENT: createConfigInstanceFile(conn, event); break;
            case PortalConstants.CONFIG_INSTANCE_FILE_UPDATED_EVENT: updateConfigInstanceFile(conn, event); break;
            case PortalConstants.CONFIG_INSTANCE_FILE_DELETED_EVENT: deleteConfigInstanceFile(conn, event); break;
            case PortalConstants.CONFIG_DEPLOYMENT_INSTANCE_CREATED_EVENT: createConfigDeploymentInstance(conn, event); break;
            case PortalConstants.CONFIG_DEPLOYMENT_INSTANCE_UPDATED_EVENT: updateConfigDeploymentInstance(conn, event); break;
            case PortalConstants.CONFIG_DEPLOYMENT_INSTANCE_DELETED_EVENT: deleteConfigDeploymentInstance(conn, event); break;
            case PortalConstants.CONFIG_INSTANCE_CREATED_EVENT: createConfigInstance(conn, event); break;
            case PortalConstants.CONFIG_INSTANCE_UPDATED_EVENT: updateConfigInstance(conn, event); break;
            case PortalConstants.CONFIG_INSTANCE_DELETED_EVENT: deleteConfigInstance(conn, event); break;
            case PortalConstants.CONFIG_PRODUCT_CREATED_EVENT: createConfigProduct(conn, event); break;
            case PortalConstants.CONFIG_PRODUCT_UPDATED_EVENT: updateConfigProduct(conn, event); break;
            case PortalConstants.CONFIG_PRODUCT_DELETED_EVENT: deleteConfigProduct(conn, event); break;
            case PortalConstants.CONFIG_PRODUCT_VERSION_CREATED_EVENT: createConfigProductVersion(conn, event); break;
            case PortalConstants.CONFIG_PRODUCT_VERSION_UPDATED_EVENT: updateConfigProductVersion(conn, event); break;
            case PortalConstants.CONFIG_PRODUCT_VERSION_DELETED_EVENT: deleteConfigProductVersion(conn, event); break;

            // --- App Events ---
            case PortalConstants.APP_CREATED_EVENT: createApp(conn, event); break;
            case PortalConstants.APP_UPDATED_EVENT: updateApp(conn, event); break;
            case PortalConstants.APP_DELETED_EVENT: deleteApp(conn, event); break;

            // --- Client Events ---
            case PortalConstants.CLIENT_CREATED_EVENT: createClient(conn, event); break;
            case PortalConstants.CLIENT_UPDATED_EVENT: updateClient(conn, event); break;
            case PortalConstants.CLIENT_DELETED_EVENT: deleteClient(conn, event); break;

            // --- Reference Table Events ---
            case PortalConstants.REF_TABLE_CREATED_EVENT: createRefTable(conn, event); break;
            case PortalConstants.REF_TABLE_UPDATED_EVENT: updateRefTable(conn, event); break;
            case PortalConstants.REF_TABLE_DELETED_EVENT: deleteRefTable(conn, event); break;
            case PortalConstants.REF_VALUE_CREATED_EVENT: createRefValue(conn, event); break;
            case PortalConstants.REF_VALUE_UPDATED_EVENT: updateRefValue(conn, event); break;
            case PortalConstants.REF_VALUE_DELETED_EVENT: deleteRefValue(conn, event); break;
            case PortalConstants.REF_LOCALE_CREATED_EVENT: createRefLocale(conn, event); break;
            case PortalConstants.REF_LOCALE_UPDATED_EVENT: updateRefLocale(conn, event); break;
            case PortalConstants.REF_LOCALE_DELETED_EVENT: deleteRefLocale(conn, event); break;
            case PortalConstants.REF_RELATION_TYPE_CREATED_EVENT: createRefRelationType(conn, event); break;
            case PortalConstants.REF_RELATION_TYPE_UPDATED_EVENT: updateRefRelationType(conn, event); break;
            case PortalConstants.REF_RELATION_TYPE_DELETED_EVENT: deleteRefRelationType(conn, event); break;
            case PortalConstants.REF_RELATION_CREATED_EVENT: createRefRelation(conn, event); break;
            case PortalConstants.REF_RELATION_UPDATED_EVENT: updateRefRelation(conn, event); break;
            case PortalConstants.REF_RELATION_DELETED_EVENT: deleteRefRelation(conn, event); break;

            // --- Default Case ---
            default:
                if (logger.isDebugEnabled()) logger.debug("Unhandled event type: {}", eventType);
        }
    }
}
