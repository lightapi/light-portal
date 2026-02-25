package net.lightapi.portal.db.persistence;

import com.networknt.monad.Result;
import net.lightapi.portal.db.PortalPersistenceException;

import java.sql.Connection; // Added import
import java.sql.SQLException; // Added import
import java.sql.Timestamp;
import java.time.OffsetDateTime;
import java.util.List;
import java.util.Map;

public interface SchedulePersistence {
    void createSchedule(Connection conn, Map<String, Object> event) throws PortalPersistenceException;
    void updateSchedule(Connection conn, Map<String, Object> event) throws PortalPersistenceException;
    void deleteSchedule(Connection conn, Map<String, Object> event) throws PortalPersistenceException;
    Result<String> getSchedule(int offset, int limit, String filters, String globalFilter, String sorting, boolean active, String hostId);
    Result<String> getScheduleLabel(String hostId);
    Result<String> getScheduleById(String scheduleId);
    int acquireLock(String instanceId, int lockId, OffsetDateTime lockTimeout) throws PortalPersistenceException;
    int renewLock(String instanceId, int lockId) throws PortalPersistenceException;
    int releaseLock(String instanceId, int lockId) throws PortalPersistenceException;
    Result<List<Map<String, Object>>> pollTasks(OffsetDateTime nextRunTs);
    Result<String> executeTask(Map<String, Object> taskData, long executionTimeMillis);

}
