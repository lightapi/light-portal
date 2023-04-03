package net.lightapi.portal.db;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.networknt.config.Config;
import com.networknt.monad.Result;
import com.networknt.service.SingletonServiceFactory;
import net.lightapi.portal.market.*;
import net.lightapi.portal.user.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

/**
 * Interface class provide the contract for different database implementation for the portal. Mainly, the data is
 * saved in the database. However, for some temp date like the oauth code, it is saved in the memory. The Kafka
 * event will be used to sync the data between the memory caches.
 *
 * @author Steve Hu
 */
public interface DbProvider {
    static Logger logger = LoggerFactory.getLogger(DbProvider.class);

    /**
     * Default method to provide the DbProvider instance from Singleton Service Factory.
     * If no instance found in factory, it creates a SqlDbProviderImpl, puts it in factory and returns it.
     *
     * @return instance of the provider
     */
    static DbProvider getInstance() {

        DbProvider provider = SingletonServiceFactory.getBean(DbProvider.class);
        if (provider == null) {
            logger.warn("No config server provider configured in service.yml; defaulting to VaultProviderImpl");
            provider = new SqlProviderImpl();
            SingletonServiceFactory.setBean(DbProvider.class.getName(), provider);
        }
        return provider;
    }

    // Get a Jackson JSON Object Mapper, usable for object serialization
    public static final ObjectMapper mapper = Config.getInstance().getMapper();

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
    Result<String> queryMarketCode(String authCode);
}
