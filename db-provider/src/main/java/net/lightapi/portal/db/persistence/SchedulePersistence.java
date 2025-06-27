package net.lightapi.portal.db.persistence;

import com.networknt.monad.Result;
import java.sql.Connection; // Added import
import java.sql.SQLException; // Added import
import java.util.Map;

public interface SchedulePersistence {
    void createSchedule(Connection conn, Map<String, Object> event) throws SQLException, Exception;
    void updateSchedule(Connection conn, Map<String, Object> event) throws SQLException, Exception;
    void deleteSchedule(Connection conn, Map<String, Object> event) throws SQLException, Exception;
    Result<String> getSchedule(int offset, int limit, String hostId, String scheduleId, String scheduleName, String frequencyUnit, Integer frequencyTime, String startTs, String eventTopic, String eventType, String eventData);
    Result<String> getScheduleLabel(String hostId);
    Result<String> getScheduleById(String scheduleId);
}
