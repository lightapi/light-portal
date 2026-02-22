package net.lightapi.portal.db.schedule;

import com.networknt.db.provider.DbProvider;
import com.networknt.monad.Result;
import com.networknt.service.SingletonServiceFactory;
import com.networknt.utility.TimeUtil;
import net.lightapi.portal.db.PortalDbProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.time.OffsetDateTime;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * DatabaseTaskScheduler polls the 'schedule_t' table and executes due tasks.
 * It only runs tasks if the current instance is the leader.
 * Execution is done in a transaction, updating the schedule state and inserting into
 * event_store_t and outbox_message_t.
 */
public class DatabaseTaskScheduler {
    private static final Logger logger = LoggerFactory.getLogger(DatabaseTaskScheduler.class);
    private static final PortalDbProvider dbProvider = (PortalDbProvider) SingletonServiceFactory.getBean(DbProvider.class);
    private final LockManager lockManager;
    private ScheduledExecutorService scheduler;

    public DatabaseTaskScheduler(LockManager lockManager) {
        this.lockManager = lockManager;
    }

    public void start() {
        scheduler = Executors.newSingleThreadScheduledExecutor(Thread.ofVirtual().factory());
        // Poll every 1 second to check for due tasks
        scheduler.scheduleAtFixedRate(this::pollAndExecuteTasks, 0, 1, TimeUnit.SECONDS);
        logger.info("DatabaseTaskScheduler started.");
    }

    public void stop() {
        if (scheduler != null) {
            scheduler.shutdown();
            try {
                if (!scheduler.awaitTermination(5, TimeUnit.SECONDS)) {
                    scheduler.shutdownNow();
                }
            } catch (InterruptedException e) {
                scheduler.shutdownNow();
                Thread.currentThread().interrupt();
            }
        }
        logger.info("DatabaseTaskScheduler stopped.");
    }

    private void pollAndExecuteTasks() {
        if (!lockManager.isLeader()) {
            return;
        }
        Result<List<Map<String, Object>>> result = dbProvider.pollTasks(OffsetDateTime.now());
        if(result.isFailure()) {
            logger.error(result.getError().toString());
        } else {
            List<Map<String, Object>> tasks = result.getResult();
            for(Map<String, Object> task : tasks) {
                try {
                    processSchedule(task);
                } catch (Exception e) {
                    logger.error("Error processing schedule {}", task.get("schedule_id"), e);
                }
            }
        }
    }

    private void processSchedule(Map<String, Object> task) throws Exception {
        String scheduleId = (String)task.get("schedule_id");
        String frequencyUnitStr = (String)task.get("frequency_unit");
        int frequencyTime = (Integer)task.get("frequency_time");
        OffsetDateTime startTs = (OffsetDateTime) task.get("start_ts");

        if (startTs == null) {
            logger.warn("Schedule {} has null start_ts. Skipping.", scheduleId);
            return;
        }

        long startTsMillis = startTs.toInstant().toEpochMilli();
        long currentMillis = Instant.now().toEpochMilli();

        TimeUnit timeUnit;
        try {
            timeUnit = TimeUnit.valueOf(frequencyUnitStr.toUpperCase());
        } catch (IllegalArgumentException e) {
            logger.warn("Invalid frequency unit {} for schedule {}", frequencyUnitStr, scheduleId);
            return;
        }

        // Logic adapted from AbstractTaskHandler.java
        long start = TimeUtil.nextStartTimestamp(timeUnit, startTsMillis);
        long current = TimeUtil.nextStartTimestamp(timeUnit, currentMillis);

        if (current - start >= 0) {
            long freq = TimeUtil.oneTimeUnitMillisecond(timeUnit) * frequencyTime;
            if (freq > 0 && (current - start) % freq == 0) {
                if (logger.isDebugEnabled()) {
                    logger.debug("Schedule {} is due for execution. current={}, start={}, freq={}", scheduleId, current, start, freq);
                }
                executeTask(task, current);
            }
        }
    }

    private void executeTask(Map<String, Object> task, long executionTimeMillis) throws Exception {
        // Start a new virtual thread for each task execution to ensure high concurrency and isolation
        Thread.ofVirtual().start(() -> {
            try {
                performTransactionalUpdate(task, executionTimeMillis);
            } catch (Exception e) {
                logger.error("Error in task execution thread for schedule {}", task.get("schedule_id"), e);
            }
        });
    }

    private void performTransactionalUpdate(Map<String, Object> task, long executionTimeMillis) throws Exception {
        String scheduleId = (String) task.get("schedule_id");
        Result<String> result = dbProvider.executeTask(task, executionTimeMillis);
        if(result.isFailure()) {
            logger.error(result.getError().toString());
        } else {
            logger.info("Insert a new event and update the schedule for eventId {} and scheduleId {}", result.getResult(), scheduleId);
        }
    }
}
