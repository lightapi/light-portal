package net.lightapi.portal.db;

import com.networknt.db.provider.DbProvider;
import com.networknt.monad.Result;
import net.lightapi.portal.client.ClientCreatedEvent;
import net.lightapi.portal.client.ClientDeletedEvent;
import net.lightapi.portal.client.ClientUpdatedEvent;
import net.lightapi.portal.market.*;
import net.lightapi.portal.oauth.*;
import net.lightapi.portal.user.*;
import net.lightapi.portal.attribute.*;
import net.lightapi.portal.group.*;
import net.lightapi.portal.position.*;
import net.lightapi.portal.role.*;
import net.lightapi.portal.service.*;
import net.lightapi.portal.rule.*;


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
    Result<String> queryRefTable(int offset, int limit, String hostId, String tableName, String tableDesc, String active, String editable, String common);

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

    Result<String> createUser(UserCreatedEvent event);
    Result<String> confirmUser(UserConfirmedEvent event);
    Result<String> verifyUser(UserVerifiedEvent event);
    Result<Integer> queryNonceByUserId(String userId);
    Result<String> createSocialUser(SocialUserCreatedEvent event);
    Result<String> updateUser(UserUpdatedEvent event);
    Result<String> deleteUser(UserDeletedEvent event);
    Result<String> updateUserRoles(UserRolesUpdatedEvent event);
    Result<String> forgetPassword(PasswordForgotEvent event);
    Result<String> resetPassword(PasswordResetEvent event);
    Result<String> changePassword(PasswordChangedEvent event);
    Result<String> updatePayment(PaymentUpdatedEvent event);
    Result<String> deletePayment(PaymentDeletedEvent event);
    Result<String> createOrder(OrderCreatedEvent event);
    Result<String> cancelOrder(OrderCancelledEvent event);
    Result<String> deliverOrder(OrderDeliveredEvent event);
    Result<String> sendPrivateMessage(PrivateMessageSentEvent event);
    Result<String> queryUserLabel(String hostId);

    Result<String> createRefreshToken(AuthRefreshTokenCreatedEvent event);
    Result<String> queryRefreshToken(String refreshToken);
    Result<String> deleteRefreshToken(AuthRefreshTokenDeletedEvent event);
    Result<String> listRefreshToken(int offset, int limit, String refreshToken, String hostId, String userId, String entityId,
                                    String email, String firstName, String lastName, String clientId, String appId,
                                    String appName, String scope, String userType, String roles, String groups, String positions,
                                    String attributes, String csrf, String customClaim, String updateUser, Timestamp updateTs);

    Result<String> createAuthCode(AuthCodeCreatedEvent event);
    Result<String> deleteAuthCode(AuthCodeDeletedEvent event);
    Result<String> queryAuthCode(String hostId, String authCode);
    Result<String> listAuthCode(int offset, int limit, String hostId, String authCode, String userId,
                                       String entityId, String userType, String email, String roles, String groups, String positions,
                                       String attributes, String redirectUri, String scope, String remember, String codeChallenge,
                                       String challengeMethod, String updateUser, Timestamp updateTs);
    Result<Map<String, Object>> queryProviderById(String providerId);
    Result<String> queryProvider(int offset, int limit, String hostId, String providerId, String providerName, String providerDesc, String operationOwner, String deliveryOwner, String jwk, String updateUser, Timestamp updateTs);
    Result<String> createAuthProvider(AuthProviderCreatedEvent event);
    Result<String> rotateAuthProvider(AuthProviderRotatedEvent event);
    Result<String> updateAuthProvider(AuthProviderUpdatedEvent event);
    Result<String> deleteAuthProvider(AuthProviderDeletedEvent event);

    Result<String> queryProviderKey(int offset, int limit, String hostId, String providerId, String kid,
                                    String key_type, String updateUser, Timestamp updateTs);

    Result<String> queryApp(int offset, int limit, String hostId, String appId, String appName, String appDesc, Boolean isKafkaApp, String operationOwner, String deliveryOwner);
    Result<String> queryClient(int offset, int limit, String hostId, String appId, String clientId, String clientType, String clientProfile, String clientScope, String customClaim, String redirectUri, String authenticateClass, String deRefClientId);
    Result<String> createClient(ClientCreatedEvent event);
    Result<String> updateClient(ClientUpdatedEvent event);
    Result<String> deleteClient(ClientDeletedEvent event);
    Result<Map<String, Object>> queryClientByClientId(String clientId);
    Result<String> queryClientByProviderClientId(String providerId, String clientId);
    Result<Map<String, Object>> queryClientByHostAppId(String host, String applicationId);

    Result<String> createService(ServiceCreatedEvent event);
    Result<String> updateService(ServiceUpdatedEvent event);
    Result<String> deleteService(ServiceDeletedEvent event);
    Result<String> queryService(int offset, int limit, String hostId, String apiId, String apiName,
                                String apiDesc, String operationOwner, String deliveryOwner, String region, String businessGroup,
                                String lob, String platform, String capability, String gitRepo, String apiTags, String apiStatus);
    Result<String> queryApiLabel(String hostId);
    Result<String> queryApiVersionLabel(String hostId, String apiId);
    Result<String> queryEndpointLabel(String hostId, String apiId, String apiVersion);

    Result<String> createServiceVersion(ServiceVersionCreatedEvent event);
    Result<String> updateServiceVersion(ServiceVersionUpdatedEvent event);
    Result<String> deleteServiceVersion(ServiceVersionDeletedEvent event);
    Result<String> queryServiceVersion(String hostId, String apiId);
    Result<String> updateServiceSpec(ServiceSpecUpdatedEvent event, List<Map<String, Object>> endpoints);
    Result<String> queryServiceEndpoint(int offset, int limit, String hostId, String apiId, String apiVersion, String endpoint, String method, String path, String desc);
    Result<String> queryEndpointRule(String hostId, String apiId, String apiVersion, String endpoint);
    Result<String> queryEndpointScope(String hostId, String apiId, String apiVersion, String endpoint);
    Result<String> createEndpointRule(EndpointRuleCreatedEvent event);
    Result<String> deleteEndpointRule(EndpointRuleDeletedEvent event);
    Result<String> queryServiceRule(String hostId, String apiId, String apiVersion);
    Result<String> queryServicePermission(String hostId, String apiId, String apiVersion);
    Result<List<String>> queryServiceFilter(String hostId, String apiId, String apiVersion);


    Result<String> createHost(HostCreatedEvent event);
    Result<String> updateHost(HostUpdatedEvent event);
    Result<String> deleteHost(HostDeletedEvent event);
    Result<String> queryHostDomainById(String hostId);
    Result<Map<String, Object>> queryHostById(String id);
    Result<Map<String, Object>> queryHostByOwner(String owner);
    Result<List<Map<String, Object>>> listHost();
    Result<String> getHost(int limit, int offset);

    Result<String> createConfig(ConfigCreatedEvent event);
    Result<String> updateConfig(ConfigUpdatedEvent event);
    Result<String> deleteConfig(ConfigDeletedEvent event);
    Result<Map<String, Object>> queryConfig();
    Result<Map<String, Object>> queryConfigById(String configId);
    Result<Map<String, Object>> queryCurrentProviderKey(String hostId);
    Result<Map<String, Object>> queryLongLiveProviderKey(String hostId);

    Result<String> createRule(RuleCreatedEvent event);
    Result<String> updateRule(RuleUpdatedEvent event);
    Result<String> deleteRule(RuleDeletedEvent event);
    Result<List<Map<String, Object>>> queryRuleByHostGroup(String hostId, String groupId);
    Result<String> queryRule(int offset, int limit, String hostId, String ruleId, String ruleName,
                             String ruleVersion, String ruleType, String ruleGroup, String ruleDesc,
                             String ruleBody, String ruleOwner, String common);
    Result<Map<String, Object>> queryRuleById(String ruleId);
    Result<String> queryRuleByHostType(String hostId, String ruleType);
    Result<List<Map<String, Object>>> queryRuleByHostApiId(String hostId, String apiId, String apiVersion);

    Result<String> createRole(RoleCreatedEvent event);
    Result<String> updateRole(RoleUpdatedEvent event);
    Result<String> deleteRole(RoleDeletedEvent event);
    Result<String> queryRole(int offset, int limit, String hostId, String roleId, String roleDesc);
    Result<String> queryRoleLabel(String hostId);
    Result<String> queryRolePermission(int offset, int limit, String hostId, String roleId, String apiId, String apiVersion, String endpoint);
    Result<String> queryRoleUser(int offset, int limit, String hostId, String roleId, String userId, String entityId, String email, String firstName, String lastName, String userType);
    Result<String> createRolePermission(RolePermissionCreatedEvent event);
    Result<String> deleteRolePermission(RolePermissionDeletedEvent event);
    Result<String> createRoleUser(RoleUserCreatedEvent event);
    Result<String> updateRoleUser(RoleUserUpdatedEvent event);
    Result<String> deleteRoleUser(RoleUserDeletedEvent event);
    Result<String> queryRoleRowFilter(int offset, int limit, String hostId, String roleId, String apiId, String apiVersion, String endpoint);
    Result<String> createRoleRowFilter(RoleRowFilterCreatedEvent event);
    Result<String> updateRoleRowFilter(RoleRowFilterUpdatedEvent event);
    Result<String> deleteRoleRowFilter(RoleRowFilterDeletedEvent event);
    Result<String> queryRoleColFilter(int offset, int limit, String hostId, String roleId, String apiId, String apiVersion, String endpoint);
    Result<String> createRoleColFilter(RoleColFilterCreatedEvent event);
    Result<String> updateRoleColFilter(RoleColFilterUpdatedEvent event);
    Result<String> deleteRoleColFilter(RoleColFilterDeletedEvent event);

    Result<String> createGroup(GroupCreatedEvent event);
    Result<String> updateGroup(GroupUpdatedEvent event);
    Result<String> deleteGroup(GroupDeletedEvent event);
    Result<String> queryGroup(int offset, int limit, String hostId, String groupId, String groupDesc);
    Result<String> queryGroupLabel(String hostId);
    Result<String> queryGroupPermission(int offset, int limit, String hostId, String groupId, String apiId, String apiVersion, String endpoint);
    Result<String> queryGroupUser(int offset, int limit, String hostId, String groupId, String userId, String entityId, String email, String firstName, String lastName, String userType);
    Result<String> createGroupPermission(GroupPermissionCreatedEvent event);
    Result<String> deleteGroupPermission(GroupPermissionDeletedEvent event);
    Result<String> createGroupUser(GroupUserCreatedEvent event);
    Result<String> updateGroupUser(GroupUserUpdatedEvent event);
    Result<String> deleteGroupUser(GroupUserDeletedEvent event);
    Result<String> queryGroupRowFilter(int offset, int limit, String hostId, String groupId, String apiId, String apiVersion, String endpoint);
    Result<String> createGroupRowFilter(GroupRowFilterCreatedEvent event);
    Result<String> updateGroupRowFilter(GroupRowFilterUpdatedEvent event);
    Result<String> deleteGroupRowFilter(GroupRowFilterDeletedEvent event);
    Result<String> queryGroupColFilter(int offset, int limit, String hostId, String groupId, String apiId, String apiVersion, String endpoint);
    Result<String> createGroupColFilter(GroupColFilterCreatedEvent event);
    Result<String> updateGroupColFilter(GroupColFilterUpdatedEvent event);
    Result<String> deleteGroupColFilter(GroupColFilterDeletedEvent event);


    Result<String> createPosition(PositionCreatedEvent event);
    Result<String> updatePosition(PositionUpdatedEvent event);
    Result<String> deletePosition(PositionDeletedEvent event);
    Result<String> queryPosition(int offset, int limit, String hostId, String positionId, String positionDesc, String inheritToAncestor, String inheritToSibling);
    Result<String> queryPositionLabel(String hostId);
    Result<String> queryPositionPermission(int offset, int limit, String hostId, String positionId, String inheritToAncestor, String inheritToSibling, String apiId, String apiVersion, String endpoint);
    Result<String> queryPositionUser(int offset, int limit, String hostId, String positionId, String positionType, String inheritToAncestor, String inheritToSibling, String userId, String entityId, String email, String firstName, String lastName, String userType);
    Result<String> createPositionPermission(PositionPermissionCreatedEvent event);
    Result<String> deletePositionPermission(PositionPermissionDeletedEvent event);
    Result<String> createPositionUser(PositionUserCreatedEvent event);
    Result<String> updatePositionUser(PositionUserUpdatedEvent event);
    Result<String> deletePositionUser(PositionUserDeletedEvent event);
    Result<String> queryPositionRowFilter(int offset, int limit, String hostId, String positionId, String apiId, String apiVersion, String endpoint);
    Result<String> createPositionRowFilter(PositionRowFilterCreatedEvent event);
    Result<String> updatePositionRowFilter(PositionRowFilterUpdatedEvent event);
    Result<String> deletePositionRowFilter(PositionRowFilterDeletedEvent event);
    Result<String> queryPositionColFilter(int offset, int limit, String hostId, String positionId, String apiId, String apiVersion, String endpoint);
    Result<String> createPositionColFilter(PositionColFilterCreatedEvent event);
    Result<String> updatePositionColFilter(PositionColFilterUpdatedEvent event);
    Result<String> deletePositionColFilter(PositionColFilterDeletedEvent event);

    Result<String> createAttribute(AttributeCreatedEvent event);
    Result<String> updateAttribute(AttributeUpdatedEvent event);
    Result<String> deleteAttribute(AttributeDeletedEvent event);
    Result<String> queryAttribute(int offset, int limit, String hostId, String attributeId, String attributeType, String attributeDesc);
    Result<String> queryAttributeLabel(String hostId);
    Result<String> queryAttributePermission(int offset, int limit, String hostId, String attributeId, String attributeType, String attributeValue, String apiId, String apiVersion, String endpoint);
    Result<String> queryAttributeUser(int offset, int limit, String hostId, String attributeId, String attributeType, String attributeValue, String userId, String entityId, String email, String firstName, String lastName, String userType);
    Result<String> createAttributePermission(AttributePermissionCreatedEvent event);
    Result<String> updateAttributePermission(AttributePermissionUpdatedEvent event);
    Result<String> deleteAttributePermission(AttributePermissionDeletedEvent event);
    Result<String> createAttributeUser(AttributeUserCreatedEvent event);
    Result<String> updateAttributeUser(AttributeUserUpdatedEvent event);
    Result<String> deleteAttributeUser(AttributeUserDeletedEvent event);
    Result<String> queryAttributeRowFilter(int offset, int limit, String hostId, String attributeId, String attributeValue, String apiId, String apiVersion, String endpoint);
    Result<String> createAttributeRowFilter(AttributeRowFilterCreatedEvent event);
    Result<String> updateAttributeRowFilter(AttributeRowFilterUpdatedEvent event);
    Result<String> deleteAttributeRowFilter(AttributeRowFilterDeletedEvent event);
    Result<String> queryAttributeColFilter(int offset, int limit, String hostId, String attributeId, String attributeValue, String apiId, String apiVersion, String endpoint);
    Result<String> createAttributeColFilter(AttributeColFilterCreatedEvent event);
    Result<String> updateAttributeColFilter(AttributeColFilterUpdatedEvent event);
    Result<String> deleteAttributeColFilter(AttributeColFilterDeletedEvent event);

}
