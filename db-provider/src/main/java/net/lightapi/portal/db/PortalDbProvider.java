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

    Result<String> queryUserByEmail(String email);
    Result<String> queryUserById(String id);
    Result<String> queryUserByWallet(String wallet);
    Result<String> queryEmailByWallet(String wallet);
    Result<String> createUser(UserCreatedEvent event);
    Result<String> confirmUser(UserConfirmedEvent event);
    Result<Integer> queryNonceByEmail(String email);
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

    Result<String> createClient(MarketClientCreatedEvent event);
    Result<String> updateClient(MarketClientUpdatedEvent event);
    Result<String> deleteClient(MarketClientDeletedEvent event);
    Result<Map<String, Object>> queryClientByClientId(String clientId);
    Result<Map<String, Object>> queryClientByHostAppId(String host, String applicationId);

    Result<String> createService(MarketServiceCreatedEvent event);
    Result<String> updateService(MarketServiceUpdatedEvent event);
    Result<String> deleteService(MarketServiceDeletedEvent event);

    Result<String> createMarketCode(MarketCodeCreatedEvent event);
    Result<String> deleteMarketCode(MarketCodeDeletedEvent event);
    Result<String> queryMarketCode(String hostId, String authCode);

    Result<String> createHost(HostCreatedEvent event);
    Result<String> updateHost(HostUpdatedEvent event);
    Result<String> deleteHost(HostDeletedEvent event);
    Result<Map<String, Object>> queryHostByHost(String host);
    Result<Map<String, Object>> queryHostById(String id);
    Result<Map<String, Object>> queryHostByOwner(String owner);

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
    Result<List<Map<String, Object>>> queryRuleByHost(String hostId);
    Result<Map<String, Object>> queryRuleById(String ruleId);
    Result<List<Map<String, Object>>> queryRuleByHostType(String hostId, String ruleType);
    Result<String> createApiRule(ApiRuleCreatedEvent event);
    Result<String> deleteApiRule(ApiRuleDeletedEvent event);
    Result<List<Map<String, Object>>> queryRuleByHostApiId(String hostId, String apiId);

}
