package net.lightapi.portal.db;

import com.networknt.server.ShutdownHookProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A shutdown hook for the PostgreSQL event consumer that sets the done flag
 * and shuts down the executor service cleanly.
 */
public class DbEventConsumerShutdownHook implements ShutdownHookProvider {
    private static final Logger logger = LoggerFactory.getLogger(DbEventConsumerShutdownHook.class);

    @Override
    public void onShutdown() {
        logger.info("DbEventConsumerShutdownHook begins");
        DbEventConsumerStartupHook.done = true;
        DbEventConsumerStartupHook.executor.shutdown();
        logger.info("DbEventConsumerShutdownHook ends");
    }
}
