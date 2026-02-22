package net.lightapi.portal.db.schedule;

import com.networknt.db.provider.DbProvider;
import com.networknt.service.SingletonServiceFactory;
import net.lightapi.portal.db.PortalDbProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.OffsetDateTime;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * LockManager handles leader election using a database table 'scheduler_lock'.
 * One and only one instance will be the leader at any time.
 */
public class LockManager {
    private static final Logger logger = LoggerFactory.getLogger(LockManager.class);
    private static final int LOCK_ID = 1;
    private static final long HEARTBEAT_INTERVAL_MS = 10000; // 10 seconds
    private static final long LOCK_TIMEOUT_SECOND = 30; // 30 seconds
    private static final PortalDbProvider dbProvider = (PortalDbProvider) SingletonServiceFactory.getBean(DbProvider.class);

    private final String instanceId;
    private final AtomicBoolean isLeader = new AtomicBoolean(false);
    private ScheduledExecutorService scheduler;

    public LockManager(String instanceId) {
        this.instanceId = instanceId;
    }

    public void start() {
        scheduler = Executors.newSingleThreadScheduledExecutor(Thread.ofVirtual().factory());
        scheduler.scheduleAtFixedRate(this::tryAcquireOrRenewLock, 0, HEARTBEAT_INTERVAL_MS, TimeUnit.MILLISECONDS);
        logger.info("LockManager started for instance: {}", instanceId);
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
        if (isLeader.get()) {
            releaseLock();
        }
        isLeader.set(false);
        logger.info("LockManager stopped for instance: {}", instanceId);
    }

    public boolean isLeader() {
        return isLeader.get();
    }

    private void tryAcquireOrRenewLock() {
        try {
            if (isLeader.get()) {
                renewLock();
            } else {
                acquireLock();
            }
        } catch (Exception e) {
            logger.error("Error managing lock for instance: {}", instanceId, e);
            // If we encounter a DB error, we might have lost leadership.
            // Safer to set to false and let the next cycle re-acquire.
            if (isLeader.get()) {
                logger.warn("Potential leadership loss due to DB error for instance: {}", instanceId);
                isLeader.set(false);
            }
        }
    }

    private void acquireLock() throws Exception {
        int updated = dbProvider.acquireLock(instanceId, LOCK_ID, OffsetDateTime.now().minusSeconds(LOCK_TIMEOUT_SECOND));
        // if(logger.isTraceEnabled()) logger.trace("updated = {}", updated);
        if (updated > 0) {
            if (isLeader.compareAndSet(false, true)) {
                logger.info("Instance {} acquired lock and became leader", instanceId);
            }
        } else {
            if (isLeader.get()) {
                logger.warn("Instance {} lost lock leadership", instanceId);
                isLeader.set(false);
            }
        }
    }

    private void renewLock() throws Exception {
        int updated = dbProvider.renewLock(instanceId, LOCK_ID);
        // if(logger.isTraceEnabled()) logger.trace("updated = {}", updated);
        if (updated == 0) {
            logger.warn("Instance {} failed to renew lock, lost leadership", instanceId);
            isLeader.set(false);
        }
    }

    private void releaseLock() {
        try {
            int updated = dbProvider.releaseLock(instanceId, LOCK_ID);
            if(logger.isTraceEnabled()) logger.trace("updated = {}", updated);
            if(updated > 0)
                logger.info("Instance {} released lock", instanceId);
            else
                logger.warn("Instance {} failed to release lock", instanceId);
        } catch (Exception e) {
            logger.error("Error releasing lock for instance: {}", instanceId, e);
        }
    }
}
