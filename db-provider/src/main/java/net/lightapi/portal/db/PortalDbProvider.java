package net.lightapi.portal.db;

import com.networknt.config.JsonMapper;
import com.networknt.db.provider.DbProvider;
import com.networknt.monad.Result;
import com.networknt.utility.Constants;
import com.networknt.utility.UuidUtil;
import io.cloudevents.CloudEvent;
import io.cloudevents.core.builder.CloudEventBuilder;
import net.lightapi.portal.PortalConstants;
import net.lightapi.portal.validation.FilterCriterion;
import net.lightapi.portal.validation.SortCriterion;

import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Timestamp;
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
    Result<String> getRefTable(int offset, int limit, String filters, String globalFilter, String sorting, String hostId);
    Result<String> getRefTableById(String tableId);
    Result<String> getRefTableLabel(String hostId);

    // RefValue
    void createRefValue(Connection conn, Map<String, Object> event) throws SQLException, Exception;
    void updateRefValue(Connection conn, Map<String, Object> event) throws SQLException, Exception;
    void deleteRefValue(Connection conn, Map<String, Object> event) throws SQLException, Exception;
    Result<String> getRefValue(int offset, int limit, String filters, String globalFilter, String sorting);
    Result<String> getRefValueById(String valueId);
    Result<String> getRefValueLabel(String tableId);

    // RefLocale
    void createRefLocale(Connection conn, Map<String, Object> event) throws SQLException, Exception;
    void updateRefLocale(Connection conn, Map<String, Object> event) throws SQLException, Exception;
    void deleteRefLocale(Connection conn, Map<String, Object> event) throws SQLException, Exception;
    Result<String> getRefLocale(int offset, int limit, String filters, String globalFilter, String sorting);

    // RefRelationType
    void createRefRelationType(Connection conn, Map<String, Object> event) throws SQLException, Exception;
    void updateRefRelationType(Connection conn, Map<String, Object> event) throws SQLException, Exception;
    void deleteRefRelationType(Connection conn, Map<String, Object> event) throws SQLException, Exception;
    Result<String> getRefRelationType(int offset, int limit, String filters, String globalFilter, String sorting);

    // RefRelation
    void createRefRelation(Connection conn, Map<String, Object> event) throws SQLException, Exception;
    void updateRefRelation(Connection conn, Map<String, Object> event) throws SQLException, Exception;
    void deleteRefRelation(Connection conn, Map<String, Object> event) throws SQLException, Exception;
    Result<String> getRefRelation(int offset, int limit, String filters, String globalFilter, String sorting);
    Result<String> getToValueCode(String relationName, String fromValueCode);

    // User
    Result<String> loginUserByEmail(String email);
    Result<String> queryUserByEmail(String email);
    Result<String> queryUserById(String id);
    Result<String> queryUserByTypeEntityId(String userType, String entityId);
    Result<String> queryUserByWallet(String cryptoType, String cryptoAddress);
    Result<String> queryEmailByWallet(String cryptoType, String cryptoAddress);
    Result<String> queryUserByHostId(int offset, int limit, String filters, String globalFilter, String sorting);
    Result<String> queryNotification(int offset, int limit, String hostId, String userId, Long nonce, String eventClass, Boolean successFlag,
                                     Timestamp processTs, String eventJson, String error);
    Result<String> getHostsByUserId(String userId);
    Result<String> getHostLabelByUserId(String userId);

    void createUser(Connection conn, Map<String, Object> event) throws SQLException, Exception;
    void onboardUser(Connection conn, Map<String, Object> event) throws SQLException, Exception;
    void confirmUser(Connection conn, Map<String, Object> event) throws SQLException, Exception;
    void verifyUser(Connection conn, Map<String, Object> event) throws SQLException, Exception;
    Result<Long> queryNonceByUserId(String userId);
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
    Result<String> listRefreshToken(int offset, int limit, String refreshToken, String hostId, String userId, String entityId,
                                    String email, String firstName, String lastName, String clientId, String appId,
                                    String appName, String scope, String userType, String roles, String groups, String positions,
                                    String attributes, String csrf, String customClaim, String updateUser, Timestamp updateTs);
    // RefToken
    void createRefToken(Connection conn, Map<String, Object> event) throws SQLException, Exception;
    void deleteRefToken(Connection conn, Map<String, Object> event) throws SQLException, Exception;
    Result<String> listRefToken(int offset, int limit, String refToken, String hostId, String clientId, String clientName, String updateUser, Timestamp updateTs);
    Result<String> queryRefToken(String refToken);

    // AuthCode
    void createAuthCode(Connection conn, Map<String, Object> event) throws SQLException, Exception;
    void deleteAuthCode(Connection conn, Map<String, Object> event) throws SQLException, Exception;
    Result<String> queryAuthCode(String authCode);
    Result<String> listAuthCode(int offset, int limit, String hostId, String authCode, String userId,
                                String entityId, String userType, String email, String roles, String groups, String positions,
                                String attributes, String redirectUri, String scope, String remember, String codeChallenge,
                                String challengeMethod, String updateUser, Timestamp updateTs);

    // AuthProvider
    Result<Map<String, Object>> queryProviderById(String providerId);
    Result<String> queryProvider(int offset, int limit, String hostId, String providerId, String providerName, String providerDesc, String operationOwner, String deliveryOwner, String jwk, String updateUser, Timestamp updateTs);
    void createAuthProvider(Connection conn, Map<String, Object> event) throws SQLException, Exception;
    void rotateAuthProvider(Connection conn, Map<String, Object> event) throws SQLException, Exception;
    void updateAuthProvider(Connection conn, Map<String, Object> event) throws SQLException, Exception;
    void deleteAuthProvider(Connection conn, Map<String, Object> event) throws SQLException, Exception;
    Result<String> queryProviderKey(String providerId);
    Result<Map<String, Object>> queryCurrentProviderKey(String providerId);
    Result<Map<String, Object>> queryLongLiveProviderKey(String providerId);

    // App
    Result<String> queryApp(int offset, int limit, String hostId, String appId, String appName, String appDesc, Boolean isKafkaApp, String operationOwner, String deliveryOwner);
    Result<String> queryClient(int offset, int limit, String hostId, String appId, String apiId, String clientId, String clientName, String clientType, String clientProfile, String clientScope, String customClaim, String redirectUri, String authenticateClass, String deRefClientId);
    void createApp(Connection conn, Map<String, Object> event) throws SQLException, Exception;
    void updateApp(Connection conn, Map<String, Object> event) throws SQLException, Exception;
    void deleteApp(Connection conn, Map<String, Object> event) throws SQLException, Exception;
    Result<String> getAppIdLabel(String hostId);

    // Client
    void createClient(Connection conn, Map<String, Object> event) throws SQLException, Exception;
    void updateClient(Connection conn, Map<String, Object> event) throws SQLException, Exception;
    void deleteClient(Connection conn, Map<String, Object> event) throws SQLException, Exception;
    Result<String> queryClientByClientId(String clientId);
    Result<String> queryClientByProviderClientId(String providerId, String clientId);
    Result<String> queryClientByHostAppId(String host, String applicationId);

    // Service
    void createService(Connection conn, Map<String, Object> event) throws SQLException, Exception;
    void updateService(Connection conn, Map<String, Object> event) throws SQLException, Exception;
    void deleteService(Connection conn, Map<String, Object> event) throws SQLException, Exception;
    Result<String> queryService(int offset, int limit, String hostId, String apiId, String apiName,
                                String apiDesc, String operationOwner, String deliveryOwner, String region, String businessGroup,
                                String lob, String platform, String capability, String gitRepo, String apiTags, String apiStatus);
    Result<String> getApiVersionIdLabel(String hostId);
    Result<String> queryApiLabel(String hostId);
    Result<String> queryApiVersionLabel(String hostId, String apiId);
    Result<String> queryEndpointLabel(String hostId, String apiVersionId);

    // ServiceVersion
    void createServiceVersion(Connection conn, Map<String, Object> event) throws SQLException, Exception;
    void updateServiceVersion(Connection conn, Map<String, Object> event) throws SQLException, Exception;
    void deleteServiceVersion(Connection conn, Map<String, Object> event) throws SQLException, Exception;
    Result<String> queryServiceVersion(String hostId, String apiId);
    void updateServiceSpec(Connection conn, Map<String, Object> event) throws SQLException, Exception;

    // ServiceEndpoint
    Result<String> queryServiceEndpoint(int offset, int limit, String hostId, String apiVersionId, String apiId, String apiVersion,
                                        String endpoint, String method, String path, String desc);
    Result<String> queryEndpointRule(String hostId, String apiId, String apiVersion, String endpoint);
    Result<String> queryEndpointScope(String hostId, String endpointId);

    // EndpointRule
    void createEndpointRule(Connection conn, Map<String, Object> event) throws SQLException, Exception;
    void deleteEndpointRule(Connection conn, Map<String, Object> event) throws SQLException, Exception;
    Result<String> queryServiceRule(String hostId, String apiId, String apiVersion);

    // Permissions and Filters (Service Specific Aggregations)
    Result<String> queryServicePermission(String hostId, String apiId, String apiVersion);
    Result<List<String>> queryServiceFilter(String hostId, String apiId, String apiVersion);
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
    Result<String> getOrg(int offset, int limit, String filters, String globalFilter, String sorting);
    Result<String> getHost(int offset, int limit, String filters, String globalFilter, String sorting);
    Result<String> getUserHost(int offset, int limit, String filters, String globalFilter, String sorting);
    Result<String> getHostByDomain(String domain, String subDomain, String hostDesc);
    Result<String> getHostLabel();

    // Config
    void createConfig(Connection conn, Map<String, Object> event) throws SQLException, Exception;
    void updateConfig(Connection conn, Map<String, Object> event) throws SQLException, Exception;
    void deleteConfig(Connection conn, Map<String, Object> event) throws SQLException, Exception;
    Result<String> getConfig(int offset, int limit, String filters, String globalFilter, String sorting);
    Result<String> queryConfigById(String configId);
    Result<String> getConfigIdLabel();
    Result<String> getPropertyIdLabel(String configId);
    Result<String> getConfigIdApiAppLabel(String resourceType);
    Result<String> getPropertyIdApiAppLabel(String configId, String resourceType);

    // ConfigProperty
    void createConfigProperty(Connection conn, Map<String, Object> event) throws SQLException, Exception;
    void updateConfigProperty(Connection conn, Map<String, Object> event) throws SQLException, Exception;
    void deleteConfigProperty(Connection conn, Map<String, Object> event) throws SQLException, Exception;

    Result<String> getConfigProperty(int offset, int limit, String filters, String globalFilter, String sorting);

    Result<String> queryConfigPropertyById(String configId);
    Result<String> queryConfigPropertyByPropertyId(String configId, String propertyId);

    // EnvironmentProperty
    void createConfigEnvironment(Connection conn, Map<String, Object> event) throws SQLException, Exception;
    void updateConfigEnvironment(Connection conn, Map<String, Object> event) throws SQLException, Exception;
    void deleteConfigEnvironment(Connection conn, Map<String, Object> event) throws SQLException, Exception;
    Result<String> getConfigEnvironment(int offset, int limit, String filters, String globalFilter, String sorting, String hostId);

    // InstanceApiProperty
    void createInstanceApi(Connection conn, Map<String, Object> event) throws SQLException, Exception;
    void updateInstanceApi(Connection conn, Map<String, Object> event) throws SQLException, Exception;
    void deleteInstanceApi(Connection conn, Map<String, Object> event) throws SQLException, Exception;
    Result<String> getInstanceApi(int offset, int limit, String hostId, String instanceApiId, String instanceId, String instanceName,
                                  String productId, String productVersion, String apiVersionId, String apiId, String apiVersion,
                                  Boolean active);
    Result<String> getInstanceApiLabel(String hostId, String instanceId);

    // InstanceApiPathPrefix
    void createInstanceApiPathPrefix(Connection conn, Map<String, Object> event) throws SQLException, Exception;
    void updateInstanceApiPathPrefix(Connection conn, Map<String, Object> event) throws SQLException, Exception;
    void deleteInstanceApiPathPrefix(Connection conn, Map<String, Object> event) throws SQLException, Exception;
    Result<String> getInstanceApiPathPrefix(int offset, int limit, String hostId, String instanceApiId, String instanceId,
                                            String instanceName, String productId, String productVersion, String apiVersionId,
                                            String apiId, String apiVersion, String pathPrefix);

    // InstanceAppApi
    void createInstanceAppApi(Connection conn, Map<String, Object> event) throws SQLException, Exception;
    void updateInstanceAppApi(Connection conn, Map<String, Object> event) throws SQLException, Exception;
    void deleteInstanceAppApi(Connection conn, Map<String, Object> event) throws SQLException, Exception;
    Result<String> getInstanceAppApi(int offset, int limit, String hostId, String instanceAppId, String instanceApiId,
                                     String instanceId, String instanceName, String productId, String productVersion,
                                     String appId, String appVersion, String apiVersionId, String apiId,
                                     String apiVersion, Boolean active);

    // ConfigInstanceApi (should be InstanceApiProperty config, perhaps a copy of original method set)
    void createConfigInstanceApi(Connection conn, Map<String, Object> event) throws SQLException, Exception;
    void updateConfigInstanceApi(Connection conn, Map<String, Object> event) throws SQLException, Exception;
    void deleteConfigInstanceApi(Connection conn, Map<String, Object> event) throws SQLException, Exception;
    Result<String> getConfigInstanceApi(int offset, int limit, String filters, String globalFilter, String sorting, String hostId);

    // InstanceApp
    void createInstanceApp(Connection conn, Map<String, Object> event) throws SQLException, Exception;
    void updateInstanceApp(Connection conn, Map<String, Object> event) throws SQLException, Exception;
    void deleteInstanceApp(Connection conn, Map<String, Object> event) throws SQLException, Exception;
    Result<String> getInstanceApp(int offset, int limit, String hostId, String instanceAppId, String instanceId, String instanceName,
                                  String productId, String productVersion, String appId, String appVersion, Boolean active);
    Result<String> getInstanceAppLabel(String hostId, String instanceId);

    // ConfigInstanceApp (should be InstanceAppProperty config)
    void createConfigInstanceApp(Connection conn, Map<String, Object> event) throws SQLException, Exception;
    void updateConfigInstanceApp(Connection conn, Map<String, Object> event) throws SQLException, Exception;
    void deleteConfigInstanceApp(Connection conn, Map<String, Object> event) throws SQLException, Exception;
    Result<String> getConfigInstanceApp(int offset, int limit, String filters, String globalFilter, String sorting, String hostId);

    // ConfigInstanceAppApi (should be InstanceAppApiProperty config)
    void createConfigInstanceAppApi(Connection conn, Map<String, Object> event) throws SQLException, Exception;
    void updateConfigInstanceAppApi(Connection conn, Map<String, Object> event) throws SQLException, Exception;
    void deleteConfigInstanceAppApi(Connection conn, Map<String, Object> event) throws SQLException, Exception;
    Result<String> getConfigInstanceAppApi(int offset, int limit, String filters, String globalFilter, String sorting, String hostId);

    // InstanceProperty (was ConfigInstance)
    void createConfigInstance(Connection conn, Map<String, Object> event) throws SQLException, Exception;
    void updateConfigInstance(Connection conn, Map<String, Object> event) throws SQLException, Exception;
    void deleteConfigInstance(Connection conn, Map<String, Object> event) throws SQLException, Exception;
    void commitConfigInstance(Connection conn, Map<String, Object> event) throws SQLException, Exception;
    void rollbackConfigInstance(Connection conn, Map<String, Object> event) throws SQLException, Exception; // Changed signature
    Result<String> getConfigInstance(int offset, int limit, String filters, String globalFilter, String sorting, String hostId);

    // InstanceFile (was ConfigInstanceFile)
    void createConfigInstanceFile(Connection conn, Map<String, Object> event) throws SQLException, Exception;
    void updateConfigInstanceFile(Connection conn, Map<String, Object> event) throws SQLException, Exception;
    void deleteConfigInstanceFile(Connection conn, Map<String, Object> event) throws SQLException, Exception;
    Result<String> getConfigInstanceFile(int offset, int limit, String filters, String globalFilter, String sorting, String hostId);

    // DeploymentInstanceProperty (was ConfigDeploymentInstance)
    void createConfigDeploymentInstance(Connection conn, Map<String, Object> event) throws SQLException, Exception;
    void updateConfigDeploymentInstance(Connection conn, Map<String, Object> event) throws SQLException, Exception;
    void deleteConfigDeploymentInstance(Connection conn, Map<String, Object> event) throws SQLException, Exception;
    Result<String> getConfigDeploymentInstance(int offset, int limit, String filters, String globalFilter, String sorting, String hostId);

    // ProductProperty (was ConfigProduct)
    void createConfigProduct(Connection conn, Map<String, Object> event) throws SQLException, Exception;
    void updateConfigProduct(Connection conn, Map<String, Object> event) throws SQLException, Exception;
    void deleteConfigProduct(Connection conn, Map<String, Object> event) throws SQLException, Exception;
    Result<String> getConfigProduct(int offset, int limit, String filters, String globalFilter, String sorting);

    // ProductVersionProperty (was ConfigProductVersion)
    void createConfigProductVersion(Connection conn, Map<String, Object> event) throws SQLException, Exception;
    void updateConfigProductVersion(Connection conn, Map<String, Object> event) throws SQLException, Exception;
    void deleteConfigProductVersion(Connection conn, Map<String, Object> event) throws SQLException, Exception;
    Result<String> getConfigProductVersion(int offset, int limit, String filters, String globalFilter, String sorting, String hostId);

    // Rule
    void createRule(Connection conn, Map<String, Object> event) throws SQLException, Exception;
    void updateRule(Connection conn, Map<String, Object> event) throws SQLException, Exception;
    void deleteRule(Connection conn, Map<String, Object> event) throws SQLException, Exception;
    Result<List<Map<String, Object>>> queryRuleByGroup(String groupId);
    Result<String> queryRule(int offset, int limit, String hostId, String ruleId, String ruleName,
                             String ruleVersion, String ruleType, String ruleGroup, String ruleDesc,
                             String ruleBody, String ruleOwner);
    Result<Map<String, Object>> queryRuleById(String ruleId);
    Result<String> queryRuleByType(String ruleType);
    Result<List<Map<String, Object>>> queryRuleByHostApiId(String hostId, String apiId, String apiVersion);

    // Role
    void createRole(Connection conn, Map<String, Object> event) throws SQLException, Exception;
    void updateRole(Connection conn, Map<String, Object> event) throws SQLException, Exception;
    void deleteRole(Connection conn, Map<String, Object> event) throws SQLException, Exception;
    Result<String> queryRole(int offset, int limit, String hostId, String roleId, String roleDesc);
    Result<String> queryRoleLabel(String hostId);

    // RolePermission
    Result<String> queryRolePermission(int offset, int limit, String hostId, String roleId, String apiVersionId, String apiId, String apiVersion, String endpointId, String endpoint);
    void createRolePermission(Connection conn, Map<String, Object> event) throws SQLException, Exception;
    void deleteRolePermission(Connection conn, Map<String, Object> event) throws SQLException, Exception;

    // RoleUser
    Result<String> queryRoleUser(int offset, int limit, String hostId, String roleId, String userId, String entityId, String email, String firstName, String lastName, String userType);
    void createRoleUser(Connection conn, Map<String, Object> event) throws SQLException, Exception;
    void updateRoleUser(Connection conn, Map<String, Object> event) throws SQLException, Exception;
    void deleteRoleUser(Connection conn, Map<String, Object> event) throws SQLException, Exception;

    // RoleRowFilter
    Result<String> queryRoleRowFilter(int offset, int limit, String hostId, String roleId, String apiVersionId, String apiId, String apiVersion, String endpointId, String endpoint);
    void createRoleRowFilter(Connection conn, Map<String, Object> event) throws SQLException, Exception;
    void updateRoleRowFilter(Connection conn, Map<String, Object> event) throws SQLException, Exception;
    void deleteRoleRowFilter(Connection conn, Map<String, Object> event) throws SQLException, Exception;

    // RoleColFilter
    Result<String> queryRoleColFilter(int offset, int limit, String hostId, String roleId, String apiVersionId, String apiId, String apiVersion, String endpointId, String endpoint);
    void createRoleColFilter(Connection conn, Map<String, Object> event) throws SQLException, Exception;
    void updateRoleColFilter(Connection conn, Map<String, Object> event) throws SQLException, Exception;
    void deleteRoleColFilter(Connection conn, Map<String, Object> event) throws SQLException, Exception;

    // Group
    void createGroup(Connection conn, Map<String, Object> event) throws SQLException, Exception;
    void updateGroup(Connection conn, Map<String, Object> event) throws SQLException, Exception;
    void deleteGroup(Connection conn, Map<String, Object> event) throws SQLException, Exception;
    Result<String> queryGroup(int offset, int limit, String hostId, String groupId, String groupDesc);
    Result<String> queryGroupLabel(String hostId);

    // GroupPermission
    Result<String> queryGroupPermission(int offset, int limit, String hostId, String groupId, String apiVersionId, String apiId, String apiVersion, String endpointId, String endpoint);
    void createGroupPermission(Connection conn, Map<String, Object> event) throws SQLException, Exception;
    void deleteGroupPermission(Connection conn, Map<String, Object> event) throws SQLException, Exception;

    // GroupUser
    Result<String> queryGroupUser(int offset, int limit, String hostId, String groupId, String userId, String entityId, String email, String firstName, String lastName, String userType);
    void createGroupUser(Connection conn, Map<String, Object> event) throws SQLException, Exception;
    void updateGroupUser(Connection conn, Map<String, Object> event) throws SQLException, Exception;
    void deleteGroupUser(Connection conn, Map<String, Object> event) throws SQLException, Exception;

    // GroupRowFilter
    Result<String> queryGroupRowFilter(int offset, int limit, String hostId, String groupId, String apiVersionId, String apiId, String apiVersion, String endpointId, String endpoint);
    void createGroupRowFilter(Connection conn, Map<String, Object> event) throws SQLException, Exception;
    void updateGroupRowFilter(Connection conn, Map<String, Object> event) throws SQLException, Exception;
    void deleteGroupRowFilter(Connection conn, Map<String, Object> event) throws SQLException, Exception;

    // GroupColFilter
    Result<String> queryGroupColFilter(int offset, int limit, String hostId, String groupId, String apiVersionId, String apiId, String apiVersion, String endpointId, String endpoint);
    void createGroupColFilter(Connection conn, Map<String, Object> event) throws SQLException, Exception;
    void updateGroupColFilter(Connection conn, Map<String, Object> event) throws SQLException, Exception;
    void deleteGroupColFilter(Connection conn, Map<String, Object> event) throws SQLException, Exception;

    // Position
    void createPosition(Connection conn, Map<String, Object> event) throws SQLException, Exception;
    void updatePosition(Connection conn, Map<String, Object> event) throws SQLException, Exception;
    void deletePosition(Connection conn, Map<String, Object> event) throws SQLException, Exception;
    Result<String> queryPosition(int offset, int limit, String hostId, String positionId, String positionDesc, String inheritToAncestor, String inheritToSibling);
    Result<String> queryPositionLabel(String hostId);

    // PositionPermission
    Result<String> queryPositionPermission(int offset, int limit, String hostId, String positionId, String inheritToAncestor, String inheritToSibling, String apiVersionId, String apiId, String apiVersion, String endpointId, String endpoint);
    void createPositionPermission(Connection conn, Map<String, Object> event) throws SQLException, Exception;
    void deletePositionPermission(Connection conn, Map<String, Object> event) throws SQLException, Exception;

    // PositionUser
    Result<String> queryPositionUser(int offset, int limit, String hostId, String positionId, String positionType, String inheritToAncestor, String inheritToSibling, String userId, String entityId, String email, String firstName, String lastName, String userType);
    void createPositionUser(Connection conn, Map<String, Object> event) throws SQLException, Exception;
    void updatePositionUser(Connection conn, Map<String, Object> event) throws SQLException, Exception;
    void deletePositionUser(Connection conn, Map<String, Object> event) throws SQLException, Exception;

    // PositionRowFilter
    Result<String> queryPositionRowFilter(int offset, int limit, String hostId, String positionId, String apiVersionId, String apiId, String apiVersion, String endpointId, String endpoint);
    void createPositionRowFilter(Connection conn, Map<String, Object> event) throws SQLException, Exception;
    void updatePositionRowFilter(Connection conn, Map<String, Object> event) throws SQLException, Exception;
    void deletePositionRowFilter(Connection conn, Map<String, Object> event) throws SQLException, Exception;

    // PositionColFilter
    Result<String> queryPositionColFilter(int offset, int limit, String hostId, String positionId, String apiVersionId, String apiId, String apiVersion, String endpointId, String endpoint);
    void createPositionColFilter(Connection conn, Map<String, Object> event) throws SQLException, Exception;
    void updatePositionColFilter(Connection conn, Map<String, Object> event) throws SQLException, Exception;
    void deletePositionColFilter(Connection conn, Map<String, Object> event) throws SQLException, Exception;

    // Attribute
    void createAttribute(Connection conn, Map<String, Object> event) throws SQLException, Exception;
    void updateAttribute(Connection conn, Map<String, Object> event) throws SQLException, Exception;
    void deleteAttribute(Connection conn, Map<String, Object> event) throws SQLException, Exception;
    Result<String> queryAttribute(int offset, int limit, String hostId, String attributeId, String attributeType, String attributeDesc);
    Result<String> queryAttributeLabel(String hostId);

    // AttributePermission
    Result<String> queryAttributePermission(int offset, int limit, String hostId, String attributeId, String attributeType, String attributeValue, String apiVersionId, String apiId, String apiVersion, String endpointId, String endpoint);
    void createAttributePermission(Connection conn, Map<String, Object> event) throws SQLException, Exception;
    void updateAttributePermission(Connection conn, Map<String, Object> event) throws SQLException, Exception;
    void deleteAttributePermission(Connection conn, Map<String, Object> event) throws SQLException, Exception;

    // AttributeUser
    Result<String> queryAttributeUser(int offset, int limit, String hostId, String attributeId, String attributeType, String attributeValue, String userId, String entityId, String email, String firstName, String lastName, String userType);
    void createAttributeUser(Connection conn, Map<String, Object> event) throws SQLException, Exception;
    void updateAttributeUser(Connection conn, Map<String, Object> event) throws SQLException, Exception;
    void deleteAttributeUser(Connection conn, Map<String, Object> event) throws SQLException, Exception;

    // AttributeRowFilter
    Result<String> queryAttributeRowFilter(int offset, int limit, String hostId, String attributeId, String attributeValue, String apiVersionId, String apiId, String apiVersion, String endpointId, String endpoint);
    void createAttributeRowFilter(Connection conn, Map<String, Object> event) throws SQLException, Exception;
    void updateAttributeRowFilter(Connection conn, Map<String, Object> event) throws SQLException, Exception;
    void deleteAttributeRowFilter(Connection conn, Map<String, Object> event) throws SQLException, Exception;

    // AttributeColFilter
    Result<String> queryAttributeColFilter(int offset, int limit, String hostId, String attributeId, String attributeValue, String apiVersionId, String apiId, String apiVersion, String endpointId, String endpoint);
    void createAttributeColFilter(Connection conn, Map<String, Object> event) throws SQLException, Exception;
    void updateAttributeColFilter(Connection conn, Map<String, Object> event) throws SQLException, Exception;
    void deleteAttributeColFilter(Connection conn, Map<String, Object> event) throws SQLException, Exception;

    // ProductVersion (renamed from Product in previous step)
    void createProduct(Connection conn, Map<String, Object> event) throws SQLException, Exception;
    void updateProduct(Connection conn, Map<String, Object> event) throws SQLException, Exception;
    void deleteProduct(Connection conn, Map<String, Object> event) throws SQLException, Exception;
    Result<String> getProduct(int offset, int limit, String hostId, String productVersionId, String productId, String productVersion, String light4jVersion,
                              Boolean breakCode, Boolean breakConfig, String releaseNote,
                              String versionDesc, String releaseType, Boolean current, String versionStatus);
    Result<String> getProductIdLabel(String hostId);
    Result<String> getProductVersionLabel(String hostId, String productId);
    Result<String> getProductVersionIdLabel(String hostId);

    // ProductVersionEnvironment
    void createProductVersionEnvironment(Connection conn, Map<String, Object> event) throws SQLException, Exception;
    void deleteProductVersionEnvironment(Connection conn, Map<String, Object> event) throws SQLException, Exception;
    Result<String> getProductVersionEnvironment(int offset, int limit, String hostId, String productVersionId,
                                                String productId, String productVersion, String systemEnv, String runtimeEnv);

    // ProductVersionPipeline
    void createProductVersionPipeline(Connection conn, Map<String, Object> event) throws SQLException, Exception;
    void deleteProductVersionPipeline(Connection conn, Map<String, Object> event) throws SQLException, Exception;
    Result<String> getProductVersionPipeline(int offset, int limit, String hostId, String productVersionId,
                                             String productId, String productVersion, String pipelineId,
                                             String pipelineName, String pipelineVersion);

    // ProductVersionConfig
    void createProductVersionConfig(Connection conn, Map<String, Object> event) throws SQLException, Exception;
    void deleteProductVersionConfig(Connection conn, Map<String, Object> event) throws SQLException, Exception;
    Result<String> getProductVersionConfig(int offset, int limit, List<SortCriterion> sorting, List<FilterCriterion> filtering, String globalFilter,
                                           String hostId, String productVersionId, String productId, String productVersion, String configId,
                                           String configName);

    // ProductVersionConfigProperty
    void createProductVersionConfigProperty(Connection conn, Map<String, Object> event) throws SQLException, Exception;
    void deleteProductVersionConfigProperty(Connection conn, Map<String, Object> event) throws SQLException, Exception;
    Result<String> getProductVersionConfigProperty(int offset, int limit, String hostId, String productVersionId,
                                                   String productId, String productVersion, String configId,
                                                   String configName, String propertyId, String propertyName);

    // Instance
    void createInstance(Connection conn, Map<String, Object> event) throws SQLException, Exception;
    void updateInstance(Connection conn, Map<String, Object> event) throws SQLException, Exception;
    void deleteInstance(Connection conn, Map<String, Object> event) throws SQLException, Exception;
    void lockInstance(Connection conn, Map<String, Object> event) throws Exception;
    void unlockInstance(Connection conn, Map<String, Object> event) throws Exception;
    void cloneInstance(Connection conn, Map<String, Object> event) throws Exception;
    Result<String> getInstance(int offset, int limit, String hostId, String instanceId, String instanceName,
                               String productVersionId, String productId, String productVersion, String serviceId, Boolean current,
                               Boolean readonly, String environment, String serviceDesc, String instanceDesc, String zone,
                               String region, String lob, String resourceName, String businessName, String envTag,
                               String topicClassification);
    Result<String> getInstanceLabel(String hostId);

    // Pipeline
    void createPipeline(Connection conn, Map<String, Object> event) throws SQLException, Exception;
    void updatePipeline(Connection conn, Map<String, Object> event) throws SQLException, Exception;
    void deletePipeline(Connection conn, Map<String, Object> event) throws SQLException, Exception;
    Result<String> getPipeline(int offset, int limit, String hostId, String pipelineId, String platformId,
                               String platformName, String platformVersion, String pipelineVersion,
                               String pipelineName, Boolean current, String endpoint, String versionStatus,
                               String systemEnv, String runtimeEnv, String requestSchema, String responseSchema);
    Result<String> getPipelineLabel(String hostId);

    // InstancePipeline
    void createInstancePipeline(Connection conn, Map<String, Object> event) throws SQLException, Exception;
    void updateInstancePipeline(Connection conn, Map<String, Object> event) throws SQLException, Exception;
    void deleteInstancePipeline(Connection conn, Map<String, Object> event) throws SQLException, Exception;
    Result<String> getInstancePipeline(int offset, int limit, String hostId, String instanceId, String instanceName,
                                       String productId, String productVersion, String pipelineId, String platformName,
                                       String platformVersion, String pipelineName, String pipelineVersion);

    // Platform
    void createPlatform(Connection conn, Map<String, Object> event) throws SQLException, Exception;
    void updatePlatform(Connection conn, Map<String, Object> event) throws SQLException, Exception;
    void deletePlatform(Connection conn, Map<String, Object> event) throws SQLException, Exception;
    Result<String> getPlatform(int offset, int limit, String hostId, String platformId, String platformName, String platformVersion,
                               String clientType, String clientUrl, String credentials, String proxyUrl, Integer proxyPort,
                               String handlerClass, String consoleUrl, String environment, String zone, String region, String lob);
    Result<String> getPlatformLabel(String hostId);

    // DeploymentInstance
    void createDeploymentInstance(Connection conn, Map<String, Object> event) throws SQLException, Exception;
    void updateDeploymentInstance(Connection conn, Map<String, Object> event) throws SQLException, Exception;
    void deleteDeploymentInstance(Connection conn, Map<String, Object> event) throws SQLException, Exception;
    Result<String> getDeploymentInstance(int offset, int limit, String hostId, String instanceId, String instanceName, String deploymentInstanceId,
                                         String serviceId, String ipAddress, Integer portNumber, String systemEnv, String runtimeEnv,
                                         String pipelineId, String pipelineName, String pipelineVersion, String deployStatus);
    Result<String> getDeploymentInstancePipeline(String hostId, String instanceId, String systemEnv, String runtimeEnv);
    Result<String> getDeploymentInstanceLabel(String hostId, String instanceId);

    // Deployment
    void createDeployment(Connection conn, Map<String, Object> event) throws SQLException, Exception;
    void updateDeployment(Connection conn, Map<String, Object> event) throws SQLException, Exception;
    void updateDeploymentJobId(Connection conn, Map<String, Object> event) throws SQLException, Exception;
    void updateDeploymentStatus(Connection conn, Map<String, Object> event) throws SQLException, Exception;
    void deleteDeployment(Connection conn, Map<String, Object> event) throws SQLException, Exception; // Changed from Result<String> to void
    Result<String> getDeployment(int offset, int limit, String hostId, String deploymentId,
                                 String deploymentInstanceId, String serviceId, String deploymentStatus,
                                 String deploymentType, String platformJobId);

    // Category
    void createCategory(Connection conn, Map<String, Object> event) throws SQLException, Exception;
    void updateCategory(Connection conn, Map<String, Object> event) throws SQLException, Exception;
    void deleteCategory(Connection conn, Map<String, Object> event) throws SQLException, Exception;
    Result<String> getCategory(int offset, int limit, String hostId, String categoryId, String entityType, String categoryName, String categoryDesc,
                               String parentCategoryId, String parentCategoryName, Integer sortOrder);
    Result<String> getCategoryLabel(String hostId);
    Result<String> getCategoryById(String categoryId);
    Result<String> getCategoryByName(String hostId, String categoryName);
    Result<String> getCategoryByType(String hostId, String entityType);
    Result<String> getCategoryTree(String hostId, String entityType);

    // Schema
    void createSchema(Connection conn, Map<String, Object> event) throws SQLException, Exception;
    void updateSchema(Connection conn, Map<String, Object> event) throws SQLException, Exception;
    void deleteSchema(Connection conn, Map<String, Object> event) throws SQLException, Exception;
    Result<String> getSchema(int offset, int limit, String hostId, String schemaId, String schemaVersion, String schemaType,
                             String specVersion, String schemaSource, String schemaName, String schemaDesc, String schemaBody,
                             String schemaOwner, String schemaStatus, String example, String commentStatus);
    Result<String> getSchemaLabel(String hostId);
    Result<String> getSchemaById(String schemaId);
    Result<String> getSchemaByCategoryId(String categoryId);
    Result<String> getSchemaByTagId(String tagId);

    // Tag
    void createTag(Connection conn, Map<String, Object> event) throws SQLException, Exception;
    void updateTag(Connection conn, Map<String, Object> event) throws SQLException, Exception;
    void deleteTag(Connection conn, Map<String, Object> event) throws SQLException, Exception;
    Result<String> getTag(int offset, int limit, String hostId, String tagId, String entityType, String tagName, String tagDesc);
    Result<String> getTagLabel(String hostId);
    Result<String> getTagById(String tagId);
    Result<String> getTagByName(String hostId, String tagName);
    Result<String> getTagByType(String hostId, String entityType);

    // Schedule
    void createSchedule(Connection conn, Map<String, Object> event) throws SQLException, Exception;
    void updateSchedule(Connection conn, Map<String, Object> event) throws SQLException, Exception;
    void deleteSchedule(Connection conn, Map<String, Object> event) throws SQLException, Exception;
    Result<String> getSchedule(int offset, int limit, String hostId, String scheduleId, String scheduleName, String frequencyUnit,
                               Integer frequencyTime, String startTs, String eventTopic, String eventType, String eventData);
    Result<String> getScheduleLabel(String hostId);
    Result<String> getScheduleById(String scheduleId);

    // Event Store
    Result<String> insertEventStore(CloudEvent[] events);

    /**
     * Builds the CloudEvent object array from the provided map, eventType, aggregateId,
     * aggregateType, userId, host, and nonce. This allows light-portal components to
     * directly push the event into kafka without calling the command handler API endpoint.
     */
    default CloudEvent[] buildCloudEvent(Map<String, Object> map, String eventType, String aggregateId,
                                       String aggregateType, String userId, String host) {

        Number nonce;
        Result<Long> nonceResult = queryNonceByUserId(userId);
        if(nonceResult.isFailure()) {
            if(nonceResult.getError().getStatusCode() != 404) {
                logger.error("Failed to query nonce for user: {} with error code {}", userId, nonceResult.getError().getCode());
                throw new IllegalStateException("Failed to query nonce for user: " + userId);
            } else {
                // this is a brand-new user that is created or onboarded.
                nonce = 1;
            }
        } else {
            nonce = nonceResult.getResult();
        }
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
}
