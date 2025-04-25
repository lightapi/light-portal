package net.lightapi.portal.db;

import com.networknt.db.provider.DbProvider;
import com.networknt.monad.Result;
import net.lightapi.portal.client.*;
import net.lightapi.portal.config.*;
import net.lightapi.portal.oauth.*;
import net.lightapi.portal.user.*;
import net.lightapi.portal.attribute.*;
import net.lightapi.portal.group.*;
import net.lightapi.portal.position.*;
import net.lightapi.portal.role.*;
import net.lightapi.portal.service.*;
import net.lightapi.portal.rule.*;
import net.lightapi.portal.host.*;
import net.lightapi.portal.product.*;
import net.lightapi.portal.instance.*;
import net.lightapi.portal.deployment.*;

import java.sql.Timestamp;
import java.util.List;
import java.util.Map;

/**
 * Interface class provide the contract for different database implementation for the portal. Mainly, the data is
 * saved in the database. However, for some temp date like the oauth code, it is saved in the memory. The Kafka
 * event will be used to sync the data between the memory caches.
 *
 * @author Steve Hu
 */
public interface PortalDbProvider extends DbProvider {

    // ref table
    Result<String> createRefTable(Map<String, Object> event);
    Result<String> updateRefTable(Map<String, Object> event);
    Result<String> deleteRefTable(Map<String, Object> event);
    Result<String> getRefTable(int offset, int limit, String hostId, String tableId, String tableName,
                               String tableDesc, Boolean active, Boolean editable);
    Result<String> getRefTableById(String tableId);
    Result<String> getRefTableLabel(String hostId);
    Result<String> createRefValue(Map<String, Object> event);
    Result<String> updateRefValue(Map<String, Object> event);
    Result<String> deleteRefValue(Map<String, Object> event);
    Result<String> getRefValue(int offset, int limit, String valueId, String tableId, String valueCode, String valueDesc,
            Integer displayOrder, Boolean active);
    Result<String> getRefValueById(String valueId);
    Result<String> getRefValueLabel(String tableId);
    Result<String> createRefLocale(Map<String, Object> event);
    Result<String> updateRefLocale(Map<String, Object> event);
    Result<String> deleteRefLocale(Map<String, Object> event);
    Result<String> getRefLocale(int offset, int limit, String valueId, String language, String valueLabel);
    Result<String> createRefRelationType(Map<String, Object> event);
    Result<String> updateRefRelationType(Map<String, Object> event);
    Result<String> deleteRefRelationType(Map<String, Object> event);
    Result<String> getRefRelationType(int offset, int limit, String relationId, String relationName,
            String relationDesc);
    Result<String> createRefRelation(Map<String, Object> event);
    Result<String> updateRefRelation(Map<String, Object> event);
    Result<String> deleteRefRelation(Map<String, Object> event);
    Result<String> getRefRelation(int offset, int limit, String relationId, String valueIdFrom, String valueIdTo,
            Boolean active);

    Result<String> loginUserByEmail(String email);
    Result<String> queryUserByEmail(String email);
    Result<String> queryUserById(String id);
    Result<String> queryUserByTypeEntityId(String userType, String entityId);
    Result<String> queryUserByWallet(String cryptoType, String cryptoAddress);
    Result<String> queryEmailByWallet(String cryptoType, String cryptoAddress);
    Result<String> queryUserByHostId(int offset, int limit, String hostId, String email, String language, String userType,
                                     String entityId, String referralId, String managerId, String firstName, String lastName,
                                     String phoneNumber, String gender, String birthday, String country, String province, String city,
                                     String address, String postCode, Boolean verified, Boolean locked);
    Result<String> queryNotification(int offset, int limit, String hostId, String userId, Long nonce, String eventClass, Boolean successFlag,
                                     Timestamp processTs, String eventJson, String error);

    Result<String> createUser(Map<String, Object> event);
    Result<String> confirmUser(Map<String, Object> event);
    Result<String> verifyUser(Map<String, Object> event);
    Result<Long> queryNonceByUserId(String userId);
    Result<String> createSocialUser(Map<String, Object> event);
    Result<String> updateUser(Map<String, Object> event);
    Result<String> deleteUser(Map<String, Object> event);
    Result<String> forgetPassword(Map<String, Object> event);
    Result<String> resetPassword(Map<String, Object> event);
    Result<String> changePassword(Map<String, Object> event);
    Result<String> updatePayment(Map<String, Object> event);
    Result<String> deletePayment(Map<String, Object> event);
    Result<String> createOrder(Map<String, Object> event);
    Result<String> cancelOrder(Map<String, Object> event);
    Result<String> deliverOrder(Map<String, Object> event);
    Result<String> sendPrivateMessage(Map<String, Object> event);
    Result<String> queryUserLabel(String hostId);

    Result<String> createRefreshToken(Map<String, Object> event);
    Result<String> queryRefreshToken(String refreshToken);
    Result<String> deleteRefreshToken(Map<String, Object> event);
    Result<String> listRefreshToken(int offset, int limit, String refreshToken, String hostId, String userId, String entityId,
                                    String email, String firstName, String lastName, String clientId, String appId,
                                    String appName, String scope, String userType, String roles, String groups, String positions,
                                    String attributes, String csrf, String customClaim, String updateUser, Timestamp updateTs);

    Result<String> createAuthCode(Map<String, Object> event);
    Result<String> deleteAuthCode(Map<String, Object> event);
    Result<String> queryAuthCode(String authCode);
    Result<String> listAuthCode(int offset, int limit, String hostId, String authCode, String userId,
                                       String entityId, String userType, String email, String roles, String groups, String positions,
                                       String attributes, String redirectUri, String scope, String remember, String codeChallenge,
                                       String challengeMethod, String updateUser, Timestamp updateTs);
    Result<Map<String, Object>> queryProviderById(String providerId);
    Result<String> queryProvider(int offset, int limit, String hostId, String providerId, String providerName, String providerDesc, String operationOwner, String deliveryOwner, String jwk, String updateUser, Timestamp updateTs);
    Result<String> createAuthProvider(Map<String, Object> event);
    Result<String> rotateAuthProvider(Map<String, Object> event);
    Result<String> updateAuthProvider(Map<String, Object> event);
    Result<String> deleteAuthProvider(Map<String, Object> event);
    Result<String> queryProviderKey(String providerId);
    Result<Map<String, Object>> queryCurrentProviderKey(String providerId);
    Result<Map<String, Object>> queryLongLiveProviderKey(String providerId);

    Result<String> queryApp(int offset, int limit, String hostId, String appId, String appName, String appDesc, Boolean isKafkaApp, String operationOwner, String deliveryOwner);
    Result<String> queryClient(int offset, int limit, String hostId, String appId, String apiId, String clientId, String clientName, String clientType, String clientProfile, String clientScope, String customClaim, String redirectUri, String authenticateClass, String deRefClientId);
    Result<String> createApp(Map<String, Object> event);
    Result<String> updateApp(Map<String, Object> event);
    Result<String> deleteApp(Map<String, Object> event);
    Result<String> getAppIdLabel(String hostId);
    Result<String> createClient(Map<String, Object> event);
    Result<String> updateClient(Map<String, Object> event);
    Result<String> deleteClient(Map<String, Object> event);
    Result<String> queryClientByClientId(String clientId);
    Result<String> queryClientByProviderClientId(String providerId, String clientId);
    Result<String> queryClientByHostAppId(String host, String applicationId);

    Result<String> createService(Map<String, Object> event);
    Result<String> updateService(Map<String, Object> event);
    Result<String> deleteService(Map<String, Object> event);
    Result<String> queryService(int offset, int limit, String hostId, String apiId, String apiName,
                                String apiDesc, String operationOwner, String deliveryOwner, String region, String businessGroup,
                                String lob, String platform, String capability, String gitRepo, String apiTags, String apiStatus);
    Result<String> queryApiLabel(String hostId);
    Result<String> queryApiVersionLabel(String hostId, String apiId);
    Result<String> queryEndpointLabel(String hostId, String apiId, String apiVersion);

    Result<String> createServiceVersion(Map<String, Object> event, List<Map<String, Object>> endpoints);
    Result<String> updateServiceVersion(Map<String, Object> event, List<Map<String, Object>> endpoints);
    Result<String> deleteServiceVersion(Map<String, Object> event);
    Result<String> queryServiceVersion(String hostId, String apiId);
    Result<String> updateServiceSpec(Map<String, Object> event, List<Map<String, Object>> endpoints);
    Result<String> queryServiceEndpoint(int offset, int limit, String hostId, String apiId, String apiVersion, String endpoint, String method, String path, String desc);
    Result<String> queryEndpointRule(String hostId, String apiId, String apiVersion, String endpoint);
    Result<String> queryEndpointScope(String hostId, String apiId, String apiVersion, String endpoint);
    Result<String> createEndpointRule(Map<String, Object> event);
    Result<String> deleteEndpointRule(Map<String, Object> event);
    Result<String> queryServiceRule(String hostId, String apiId, String apiVersion);
    Result<String> queryServicePermission(String hostId, String apiId, String apiVersion);
    Result<List<String>> queryServiceFilter(String hostId, String apiId, String apiVersion);
    Result<String> getServiceIdLabel(String hostId);

    Result<String> createOrg(Map<String, Object> event);
    Result<String> updateOrg(Map<String, Object> event);
    Result<String> deleteOrg(Map<String, Object> event);
    Result<String> createHost(Map<String, Object> event);
    Result<String> updateHost(Map<String, Object> event);
    Result<String> deleteHost(Map<String, Object> event);
    Result<String> switchHost(Map<String, Object> event);
    Result<String> queryHostDomainById(String hostId);
    Result<String> queryHostById(String id);
    Result<Map<String, Object>> queryHostByOwner(String owner);
    Result<String> getOrg(int offset, int limit, String domain, String orgName, String orgDesc, String orgOwner);
    Result<String> getHost(int offset, int limit, String hostId, String domain, String subDomain, String hostDesc, String hostOwner);
    Result<String> getHostByDomain(String domain, String subDomain, String hostDesc);
    Result<String> getHostLabel();

    Result<String> createConfig(Map<String, Object> event);
    Result<String> updateConfig(Map<String, Object> event);
    Result<String> deleteConfig(Map<String, Object> event);
    Result<String> getConfig(int offset, int limit, String configId, String configName, String configPhase,
                             String configType, String light4jVersion, String classPath, String configDesc);
    Result<String> queryConfigById(String configId);
    Result<String> getConfigIdLabel();
    Result<String> getPropertyIdLabel(String configId);
    Result<String> getConfigIdApiAppLabel(String resourceType);
    Result<String> getPropertyNameApiAppLabel(String configId, String resourceType);

    Result<String> createConfigProperty(Map<String, Object> event);
    Result<String> updateConfigProperty(Map<String, Object> event);
    Result<String> deleteConfigProperty(Map<String, Object> event);
    Result<String> getConfigProperty(int offset, int limit, String configId, String propertyId, String configName, String propertyName,
                                                  String propertyType, String light4jVersion, Integer displayOrder, Boolean required,
                                                  String propertyDesc, String propertyValue, String valueType, String propertyFile,
                                                  String resourceType);
    Result<String> queryConfigPropertyById(String configId);
    Result<String> queryConfigPropertyByIdName(String configId, String propertyName);

    Result<String> createConfigEnvironment(Map<String, Object> event);
    Result<String> updateConfigEnvironment(Map<String, Object> event);
    Result<String> deleteConfigEnvironment(Map<String, Object> event);
    Result<String> getConfigEnvironment(int offset, int limit, String hostId, String environment, String configId, String configName,
                                                  String propertyId, String propertyName, String propertyValue, String propertyFile);

    Result<String> createInstanceApi(Map<String, Object> event);
    Result<String> updateInstanceApi(Map<String, Object> event);
    Result<String> deleteInstanceApi(Map<String, Object> event);
    Result<String> getInstanceApi(int offset, int limit, String hostId, String instanceApiId, String instanceId, String instanceName,
                                  String productId, String productVersion, String apiVersionId, String apiId, String apiVersion,
                                  Boolean active);
    Result<String> getInstanceApiLabel(String hostId);

    Result<String> createConfigInstanceApi(Map<String, Object> event);
    Result<String> updateConfigInstanceApi(Map<String, Object> event);
    Result<String> deleteConfigInstanceApi(Map<String, Object> event);
    Result<String> getConfigInstanceApi(int offset, int limit, String hostId, String instanceId, String instanceName,
                                        String apiId, String apiVersion, String configId, String configName, String propertyId,
                                        String propertyName, String propertyValue, String propertyFile);

    Result<String> createInstanceApp(Map<String, Object> event);
    Result<String> updateInstanceApp(Map<String, Object> event);
    Result<String> deleteInstanceApp(Map<String, Object> event);
    Result<String> getInstanceApp(int offset, int limit, String hostId, String instanceAppId, String instanceId, String instanceName,
                                  String productId, String productVersion, String appId, String appVersion, Boolean active);
    Result<String> getInstanceAppLabel(String hostId);

    Result<String> createConfigInstanceApp(Map<String, Object> event);
    Result<String> updateConfigInstanceApp(Map<String, Object> event);
    Result<String> deleteConfigInstanceApp(Map<String, Object> event);
    Result<String> getConfigInstanceApp(int offset, int limit, String hostId, String instanceId, String instanceName,
                                        String appId, String appVersion,String configId, String configName,
                                        String propertyId, String propertyName, String propertyValue, String propertyFile);

    Result<String> createConfigInstance(Map<String, Object> event);
    Result<String> updateConfigInstance(Map<String, Object> event);
    Result<String> deleteConfigInstance(Map<String, Object> event);
    Result<String> getConfigInstance(int offset, int limit, String hostId, String instanceId,
                                                     String configId, String configName, String propertyId,
                                                     String propertyName, String propertyValue, String propertyFile);

    Result<String> createConfigProduct(Map<String, Object> event);
    Result<String> updateConfigProduct(Map<String, Object> event);
    Result<String> deleteConfigProduct(Map<String, Object> event);
    Result<String> getConfigProduct(int offset, int limit, String productId,
                                                  String configId, String configName, String propertyId,
                                                  String propertyName, String propertyValue, String propertyFile);

    Result<String> createConfigProductVersion(Map<String, Object> event);
    Result<String> updateConfigProductVersion(Map<String, Object> event);
    Result<String> deleteConfigProductVersion(Map<String, Object> event);
    Result<String> getConfigProductVersion(int offset, int limit, String hostId, String productId, String productVersion,
                                                 String configId, String configName, String propertyId,
                                                 String propertyName, String propertyValue, String propertyFile);
    //Result<String> queryConfig();
    //Result<String> queryCert();
    //Result<String> queryFile();

    Result<String> createRule(Map<String, Object> event);
    Result<String> updateRule(Map<String, Object> event);
    Result<String> deleteRule(Map<String, Object> event);
    Result<List<Map<String, Object>>> queryRuleByHostGroup(String hostId, String groupId);
    Result<String> queryRule(int offset, int limit, String hostId, String ruleId, String ruleName,
                             String ruleVersion, String ruleType, String ruleGroup, String ruleDesc,
                             String ruleBody, String ruleOwner, String common);
    Result<Map<String, Object>> queryRuleById(String ruleId);
    Result<String> queryRuleByHostType(String hostId, String ruleType);
    Result<List<Map<String, Object>>> queryRuleByHostApiId(String hostId, String apiId, String apiVersion);

    Result<String> createRole(Map<String, Object> event);
    Result<String> updateRole(Map<String, Object> event);
    Result<String> deleteRole(Map<String, Object> event);
    Result<String> queryRole(int offset, int limit, String hostId, String roleId, String roleDesc);
    Result<String> queryRoleLabel(String hostId);
    Result<String> queryRolePermission(int offset, int limit, String hostId, String roleId, String apiId, String apiVersion, String endpoint);
    Result<String> queryRoleUser(int offset, int limit, String hostId, String roleId, String userId, String entityId, String email, String firstName, String lastName, String userType);
    Result<String> createRolePermission(Map<String, Object> event);
    Result<String> deleteRolePermission(Map<String, Object> event);
    Result<String> createRoleUser(Map<String, Object> event);
    Result<String> updateRoleUser(Map<String, Object> event);
    Result<String> deleteRoleUser(Map<String, Object> event);
    Result<String> queryRoleRowFilter(int offset, int limit, String hostId, String roleId, String apiId, String apiVersion, String endpoint);
    Result<String> createRoleRowFilter(Map<String, Object> event);
    Result<String> updateRoleRowFilter(Map<String, Object> event);
    Result<String> deleteRoleRowFilter(Map<String, Object> event);
    Result<String> queryRoleColFilter(int offset, int limit, String hostId, String roleId, String apiId, String apiVersion, String endpoint);
    Result<String> createRoleColFilter(Map<String, Object> event);
    Result<String> updateRoleColFilter(Map<String, Object> event);
    Result<String> deleteRoleColFilter(Map<String, Object> event);

    Result<String> createGroup(Map<String, Object> event);
    Result<String> updateGroup(Map<String, Object> event);
    Result<String> deleteGroup(Map<String, Object> event);
    Result<String> queryGroup(int offset, int limit, String hostId, String groupId, String groupDesc);
    Result<String> queryGroupLabel(String hostId);
    Result<String> queryGroupPermission(int offset, int limit, String hostId, String groupId, String apiId, String apiVersion, String endpoint);
    Result<String> queryGroupUser(int offset, int limit, String hostId, String groupId, String userId, String entityId, String email, String firstName, String lastName, String userType);
    Result<String> createGroupPermission(Map<String, Object> event);
    Result<String> deleteGroupPermission(Map<String, Object> event);
    Result<String> createGroupUser(Map<String, Object> event);
    Result<String> updateGroupUser(Map<String, Object> event);
    Result<String> deleteGroupUser(Map<String, Object> event);
    Result<String> queryGroupRowFilter(int offset, int limit, String hostId, String groupId, String apiId, String apiVersion, String endpoint);
    Result<String> createGroupRowFilter(Map<String, Object> event);
    Result<String> updateGroupRowFilter(Map<String, Object> event);
    Result<String> deleteGroupRowFilter(Map<String, Object> event);
    Result<String> queryGroupColFilter(int offset, int limit, String hostId, String groupId, String apiId, String apiVersion, String endpoint);
    Result<String> createGroupColFilter(Map<String, Object> event);
    Result<String> updateGroupColFilter(Map<String, Object> event);
    Result<String> deleteGroupColFilter(Map<String, Object> event);


    Result<String> createPosition(Map<String, Object> event);
    Result<String> updatePosition(Map<String, Object> event);
    Result<String> deletePosition(Map<String, Object> event);
    Result<String> queryPosition(int offset, int limit, String hostId, String positionId, String positionDesc, String inheritToAncestor, String inheritToSibling);
    Result<String> queryPositionLabel(String hostId);
    Result<String> queryPositionPermission(int offset, int limit, String hostId, String positionId, String inheritToAncestor, String inheritToSibling, String apiId, String apiVersion, String endpoint);
    Result<String> queryPositionUser(int offset, int limit, String hostId, String positionId, String positionType, String inheritToAncestor, String inheritToSibling, String userId, String entityId, String email, String firstName, String lastName, String userType);
    Result<String> createPositionPermission(Map<String, Object> event);
    Result<String> deletePositionPermission(Map<String, Object> event);
    Result<String> createPositionUser(Map<String, Object> event);
    Result<String> updatePositionUser(Map<String, Object> event);
    Result<String> deletePositionUser(Map<String, Object> event);
    Result<String> queryPositionRowFilter(int offset, int limit, String hostId, String positionId, String apiId, String apiVersion, String endpoint);
    Result<String> createPositionRowFilter(Map<String, Object> event);
    Result<String> updatePositionRowFilter(Map<String, Object> event);
    Result<String> deletePositionRowFilter(Map<String, Object> event);
    Result<String> queryPositionColFilter(int offset, int limit, String hostId, String positionId, String apiId, String apiVersion, String endpoint);
    Result<String> createPositionColFilter(Map<String, Object> event);
    Result<String> updatePositionColFilter(Map<String, Object> event);
    Result<String> deletePositionColFilter(Map<String, Object> event);

    Result<String> createAttribute(Map<String, Object> event);
    Result<String> updateAttribute(Map<String, Object> event);
    Result<String> deleteAttribute(Map<String, Object> event);
    Result<String> queryAttribute(int offset, int limit, String hostId, String attributeId, String attributeType, String attributeDesc);
    Result<String> queryAttributeLabel(String hostId);
    Result<String> queryAttributePermission(int offset, int limit, String hostId, String attributeId, String attributeType, String attributeValue, String apiId, String apiVersion, String endpoint);
    Result<String> queryAttributeUser(int offset, int limit, String hostId, String attributeId, String attributeType, String attributeValue, String userId, String entityId, String email, String firstName, String lastName, String userType);
    Result<String> createAttributePermission(Map<String, Object> event);
    Result<String> updateAttributePermission(Map<String, Object> event);
    Result<String> deleteAttributePermission(Map<String, Object> event);
    Result<String> createAttributeUser(Map<String, Object> event);
    Result<String> updateAttributeUser(Map<String, Object> event);
    Result<String> deleteAttributeUser(Map<String, Object> event);
    Result<String> queryAttributeRowFilter(int offset, int limit, String hostId, String attributeId, String attributeValue, String apiId, String apiVersion, String endpoint);
    Result<String> createAttributeRowFilter(Map<String, Object> event);
    Result<String> updateAttributeRowFilter(Map<String, Object> event);
    Result<String> deleteAttributeRowFilter(Map<String, Object> event);
    Result<String> queryAttributeColFilter(int offset, int limit, String hostId, String attributeId, String attributeValue, String apiId, String apiVersion, String endpoint);
    Result<String> createAttributeColFilter(Map<String, Object> event);
    Result<String> updateAttributeColFilter(Map<String, Object> event);
    Result<String> deleteAttributeColFilter(Map<String, Object> event);

    Result<String> createProduct(Map<String, Object> event);
    Result<String> updateProduct(Map<String, Object> event);
    Result<String> deleteProduct(Map<String, Object> event);
    Result<String> getProduct(int offset, int limit, String hostId, String productId, String productVersion, String light4jVersion,
                              Boolean breakCode, Boolean breakConfig, String releaseNote,
                              String versionDesc, String releaseType, Boolean current, String versionStatus);
    Result<String> getProductIdLabel(String hostId);
    Result<String> getProductVersionLabel(String hostId, String productId);
    Result<String> createInstance(Map<String, Object> event);
    Result<String> updateInstance(Map<String, Object> event);
    Result<String> deleteInstance(Map<String, Object> event);
    Result<String> getInstance(int offset, int limit, String hostId, String instanceId, String instanceName,
                               String productVersionId, String productId, String productVersion, String serviceId, Boolean current,
                               Boolean readonly, String environment, String serviceDesc, String instanceDesc, String zone,
                               String region, String lob, String resourceName, String businessName,
                               String topicClassification);
    Result<String> getInstanceLabel(String hostId);


    Result<String> createPipeline(Map<String, Object> event);
    Result<String> updatePipeline(Map<String, Object> event);
    Result<String> deletePipeline(Map<String, Object> event);
    Result<String> getPipeline(int offset, int limit, String hostId, String pipelineId, String platformId, String endpoint,
                               String requestSchema, String responseSchema);
    Result<String> getPipelineLabel(String hostId);

    Result<String> createInstancePipeline(Map<String, Object> event);
    Result<String> updateInstancePipeline(Map<String, Object> event);
    Result<String> deleteInstancePipeline(Map<String, Object> event);
    Result<String> getInstancePipeline(int offset, int limit, String hostId, String instanceId, String instanceName,
                                       String productId, String productVersion, String pipelineId, String platformName,
                                       String platformVersion, String pipelineName, String pipelineVersion);

    Result<String> createPlatform(Map<String, Object> event);
    Result<String> updatePlatform(Map<String, Object> event);
    Result<String> deletePlatform(Map<String, Object> event);
    Result<String> getPlatform(int offset, int limit, String hostId, String platformId, String platformName, String platformVersion,
                               String clientType, String clientUrl, String credentials, String proxyUrl, Integer proxyPort,
                               String environment, String systemEnv, String runtimeEnv, String zone, String region, String lob);
    Result<String> getPlatformLabel(String hostId);
    Result<String> createDeployment(Map<String, Object> event);
    Result<String> updateDeployment(Map<String, Object> event);
    Result<String> updateDeploymentJobId(Map<String, Object> event);
    Result<String> updateDeploymentStatus(Map<String, Object> event);
    Result<String> deleteDeployment(Map<String, Object> event);
    Result<String> getDeployment(int offset, int limit, String hostId, String deploymentId, String instanceId, String deploymentStatus,
                                 String deploymentType, String platformJobId);

    Result<String> createCategory(Map<String, Object> event);
    Result<String> updateCategory(Map<String, Object> event);
    Result<String> deleteCategory(Map<String, Object> event);
    Result<String> getCategory(int offset, int limit, String hostId, String categoryId, String entityType, String categoryName, String categoryDesc,
                               String parentCategoryId, String parentCategoryName, Integer sortOrder);
    Result<String> getCategoryLabel(String hostId);
    Result<String> getCategoryById(String categoryId);
    Result<String> getCategoryByName(String hostId, String categoryName);
    Result<String> getCategoryByType(String hostId, String entityType);
    Result<String> getCategoryTree(String hostId, String entityType);

    Result<String> createSchema(Map<String, Object> event);
    Result<String> updateSchema(Map<String, Object> event);
    Result<String> deleteSchema(Map<String, Object> event);
    Result<String> getSchema(int offset, int limit, String hostId, String schemaId, String schemaVersion, String schemaType,
                             String specVersion, String schemaSource, String schemaName, String schemaDesc, String schemaBody,
                             String schemaOwner, String schemaStatus, String example, String commentStatus);
    Result<String> getSchemaLabel(String hostId);
    Result<String> getSchemaById(String schemaId);
    Result<String> getSchemaByCategoryId(String categoryId);
    Result<String> getSchemaByTagId(String tagId);

    Result<String> createTag(Map<String, Object> event);
    Result<String> updateTag(Map<String, Object> event);
    Result<String> deleteTag(Map<String, Object> event);
    Result<String> getTag(int offset, int limit, String hostId, String tagId, String entityType, String tagName, String tagDesc);
    Result<String> getTagLabel(String hostId);
    Result<String> getTagById(String tagId);
    Result<String> getTagByName(String hostId, String tagName);
    Result<String> getTagByType(String hostId, String entityType);

    Result<String> createSchedule(Map<String, Object> event);
    Result<String> updateSchedule(Map<String, Object> event);
    Result<String> deleteSchedule(Map<String, Object> event);
    Result<String> getSchedule(int offset, int limit, String hostId, String scheduleId, String scheduleName, String frequencyUnit,
                             Integer frequencyTime, String startTs, String eventTopic, String eventType, String eventData);
    Result<String> getScheduleLabel(String hostId);
    Result<String> getScheduleById(String scheduleId);

}
