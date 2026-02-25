package net.lightapi.portal.db;

import com.networknt.server.ServerConfig;
import com.networknt.server.StartupHookProvider;
import com.networknt.utility.NetUtils;
import net.lightapi.portal.db.schedule.DatabaseTaskScheduler;
import net.lightapi.portal.db.schedule.LockManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ScheduleQueryStartup implements StartupHookProvider {
    private static final Logger logger = LoggerFactory.getLogger(ScheduleQueryStartup.class);

    public static LockManager lockManager = null;
    public static DatabaseTaskScheduler scheduler = null;

    @Override
    public void onStartup() {
        logger.info("ScheduleQueryStartup onStartup begins.");

        int port = ServerConfig.load().getHttpsPort();
        String ip = NetUtils.getLocalAddressByDatagram();
        String instanceId = ip + ":" + port;
        logger.info("Instance ID for leader election: {}", instanceId);

        // Initialize and start LockManager for leader election
        lockManager = new LockManager(instanceId);
        lockManager.start();

        // Initialize and start DatabaseTaskScheduler
        scheduler = new DatabaseTaskScheduler(lockManager);
        scheduler.start();

        logger.info("ScheduleQueryStartup onStartup ends.");
    }

}
