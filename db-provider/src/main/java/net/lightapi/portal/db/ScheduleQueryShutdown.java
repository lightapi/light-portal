package net.lightapi.portal.db;

import com.networknt.server.ShutdownHookProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This is the startup hook for schedule-query to start the ScheduleQueryStreams.
 *
 * @author Steve Hu
 */
public class ScheduleQueryShutdown implements ShutdownHookProvider {
    private static final Logger logger = LoggerFactory.getLogger(ScheduleQueryShutdown.class);

    @Override
    public void onShutdown() {
        logger.info("ScheduleQueryShutdown onShutdown begins.");
        // Stop the scheduler and lock manager
        if(ScheduleQueryStartup.scheduler != null) {
            ScheduleQueryStartup.scheduler.stop();
        }
        if(ScheduleQueryStartup.lockManager != null) {
            ScheduleQueryStartup.lockManager.stop();
        }
        logger.info("ScheduleQueryShutdown onShutdown ends.");
    }
}
