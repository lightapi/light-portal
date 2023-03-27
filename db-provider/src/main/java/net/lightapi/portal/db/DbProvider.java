package net.lightapi.portal.db;

import com.networknt.service.SingletonServiceFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Interface class provide the contract for different database implementation for the portal.
 * @author Steve Hu
 */
public interface DbProvider {
    static Logger logger = LoggerFactory.getLogger(DbProvider.class);

    /**
     * Default method to provide the DbProvider instance from Singleton Service Factory.
     * If no instance found in factory, it creates a SqlDbProviderImpl, puts it in factory and returns it.
     * @return instance of the provider
     */
    static DbProvider getInstance(){

        DbProvider provider = SingletonServiceFactory.getBean(DbProvider.class);
        if(provider == null){
            logger.warn("No config server provider configured in service.yml; defaulting to VaultProviderImpl");
            provider = new PostgresProviderImpl();
            SingletonServiceFactory.setBean(DbProvider.class.getName(), provider);
        }
        return provider;
    }

}
