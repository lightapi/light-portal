package net.lightapi.portal.db;

import com.networknt.db.provider.DbProvider;
import com.networknt.monad.Result;
import net.lightapi.portal.market.*;
import net.lightapi.portal.user.*;

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
    Result<String> queryUserByWallet(String cryptoType, String cryptoAddress);
    Result<String> queryEmailByWallet(String cryptoType, String cryptoAddress);
    Result<String> createUser(UserCreatedEvent event);
    Result<String> confirmUser(UserConfirmedEvent event);
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

    Result<String> createRefreshToken(MarketTokenCreatedEvent event);
    Result<String> queryRefreshToken(String refreshToken);
    Result<String> deleteRefreshToken(MarketTokenDeletedEvent event);

    Result<String> createClient(MarketClientCreatedEvent event);
    Result<String> updateClient(MarketClientUpdatedEvent event);
    Result<String> deleteClient(MarketClientDeletedEvent event);
    Result<Map<String, Object>> queryClientByClientId(String clientId);
    Result<Map<String, Object>> queryClientByHostAppId(String host, String applicationId);

    Result<String> createService(MarketServiceCreatedEvent event);
    Result<String> updateService(MarketServiceUpdatedEvent event);
    Result<String> deleteService(MarketServiceDeletedEvent event);
    Result<String> queryService(int offset, int limit, String hostId, String apiId, String apiName,
                                String apiDesc, String operationOwner, String deliveryOwner, String region, String businessGroup,
                                String lob, String platform, String capability, String gitRepo, String apiTags, String apiStatus);
    Result<String> createServiceVersion(ServiceVersionCreatedEvent event);
    Result<String> updateServiceVersion(ServiceVersionUpdatedEvent event);
    Result<String> deleteServiceVersion(ServiceVersionDeletedEvent event);
    Result<String> queryServiceVersion(String hostId, String apiId);
    Result<String> updateServiceSpec(ServiceSpecUpdatedEvent event, List<Map<String, Object>> endpoints);
    Result<String> queryServiceEndpoint(String hostId, String apiId, String apiVersion);
    Result<String> queryEndpointRule(String hostId, String apiId, String apiVersion, String endpoint);
    Result<String> queryEndpointScope(String hostId, String apiId, String apiVersion, String endpoint);
    Result<String> createEndpointRule(EndpointRuleCreatedEvent event);
    Result<String> deleteEndpointRule(EndpointRuleDeletedEvent event);

    Result<String> createMarketCode(MarketCodeCreatedEvent event);
    Result<String> deleteMarketCode(MarketCodeDeletedEvent event);
    Result<String> queryMarketCode(String hostId, String authCode);

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
    Result<Map<String, Object>> queryCurrentHostKey(String hostId);
    Result<Map<String, Object>> queryLongLiveHostKey(String hostId);

    Result<String> createRule(RuleCreatedEvent event);
    Result<String> updateRule(RuleUpdatedEvent event);
    Result<String> deleteRule(RuleDeletedEvent event);
    Result<List<Map<String, Object>>> queryRuleByHostGroup(String hostId, String groupId);
    Result<String> queryRule(int offset, int limit, String hostId, String ruleId, String ruleName,
                             String ruleVersion, String ruleType, String ruleGroup, String ruleDesc,
                             String ruleBody, String ruleOwner, String common);
    Result<Map<String, Object>> queryRuleById(String ruleId);
    Result<String> queryRuleByHostType(String hostId, String ruleType);
    Result<String> createApiRule(ApiRuleCreatedEvent event);
    Result<String> deleteApiRule(ApiRuleDeletedEvent event);
    Result<List<Map<String, Object>>> queryRuleByHostApiId(String hostId, String apiId, String apiVersion);

    Result<String> createRole(RoleCreatedEvent event);
    Result<String> updateRole(RoleUpdatedEvent event);
    Result<String> deleteRole(RoleDeletedEvent event);
    Result<String> queryRole(int offset, int limit, String hostId, String roleId, String roleDesc);

    Result<String> createGroup(GroupCreatedEvent event);
    Result<String> updateGroup(GroupUpdatedEvent event);
    Result<String> deleteGroup(GroupDeletedEvent event);
    Result<String> queryGroup(int offset, int limit, String hostId, String groupId, String groupDesc);

    Result<String> createPosition(PositionCreatedEvent event);
    Result<String> updatePosition(PositionUpdatedEvent event);
    Result<String> deletePosition(PositionDeletedEvent event);
    Result<String> queryPosition(int offset, int limit, String hostId, String positionId, String positionDesc, String inheritToAncestor, String inheritToSibling);

    Result<String> createAttribute(AttributeCreatedEvent event);
    Result<String> updateAttribute(AttributeUpdatedEvent event);
    Result<String> deleteAttribute(AttributeDeletedEvent event);
    Result<String> queryAttribute(int offset, int limit, String hostId, String attributeId, String attributeType, String attributeDesc);

}
