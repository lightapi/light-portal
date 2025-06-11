package net.lightapi.portal.db.persistence;

import com.networknt.config.JsonMapper;
import com.networknt.db.provider.SqlDbStartupHook;
import com.networknt.monad.Failure;
import com.networknt.monad.Result;
import com.networknt.monad.Success;
import com.networknt.status.Status;
import net.lightapi.portal.db.PortalDbProvider;
import net.lightapi.portal.db.util.SqlUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;
import java.time.OffsetDateTime;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

public class NotificationDataPersistenceImpl implements NotificationDataPersistence {
    private static final Logger logger = LoggerFactory.getLogger(NotificationDataPersistenceImpl.class);
    private static final String SQL_EXCEPTION = PortalDbProvider.SQL_EXCEPTION;
    private static final String GENERIC_EXCEPTION = PortalDbProvider.GENERIC_EXCEPTION;

    @Override
    public Result<String> queryNotification(int offset, int limit, String hostId, String userId, Long nonce, String eventClass, Boolean successFlag, Timestamp processTs, String eventJson, String error) {
        StringBuilder sqlBuilder = new StringBuilder();
        sqlBuilder.append("""
                SELECT COUNT(*) OVER () AS total,
                       host_id, user_id, nonce, event_class, is_processed, process_ts, event_json, error
                FROM notification_t
                """);

        List<Object> parameters = new ArrayList<>();
        StringBuilder whereClause = new StringBuilder();

        // hostId is mandatory for this query as per original logic
        whereClause.append(" WHERE host_id = ?");
        parameters.add(UUID.fromString(hostId));

        SqlUtil.addCondition(whereClause, parameters, "user_id", userId != null ? UUID.fromString(userId) : null);
        SqlUtil.addCondition(whereClause, parameters, "nonce", nonce);
        SqlUtil.addCondition(whereClause, parameters, "event_class", eventClass);
        SqlUtil.addCondition(whereClause, parameters, "is_processed", successFlag);
        // SqlUtil.addCondition(whereClause, parameters, "process_ts", processTs); // Timestamp needs careful handling with LIKE or range
        SqlUtil.addCondition(whereClause, parameters, "event_json", eventJson); // LIKE for JSON string might be slow
        SqlUtil.addCondition(whereClause, parameters, "error", error);

        if (whereClause.toString().startsWith(" WHERE ")) {
            sqlBuilder.append(whereClause);
        } else if (whereClause.length() > 0) {
            sqlBuilder.append(" AND ").append(whereClause.substring(" AND ".length()));
        }

        sqlBuilder.append("""
                 ORDER BY process_ts DESC
                 LIMIT ? OFFSET ?
                """);
        parameters.add(limit);
        parameters.add(offset);

        String sql = sqlBuilder.toString();
        if(logger.isTraceEnabled()) logger.trace("sql = {}", sql);
        int total = 0;
        List<Map<String, Object>> notifications = new ArrayList<>();

        try (Connection connection = SqlDbStartupHook.ds.getConnection();
             PreparedStatement preparedStatement = connection.prepareStatement(sql)) {
            for (int i = 0; i < parameters.size(); i++) {
                preparedStatement.setObject(i + 1, parameters.get(i));
            }

            boolean isFirstRow = true;
            try (ResultSet resultSet = preparedStatement.executeQuery()) {
                while (resultSet.next()) {
                    Map<String, Object> map = new HashMap<>();
                    if (isFirstRow) {
                        total = resultSet.getInt("total");
                        isFirstRow = false;
                    }
                    map.put("hostId", resultSet.getObject("host_id", UUID.class));
                    map.put("userId", resultSet.getObject("user_id", UUID.class));
                    map.put("nonce", resultSet.getLong("nonce"));
                    map.put("eventClass", resultSet.getString("event_class"));
                    map.put("processFlag", resultSet.getBoolean("is_processed")); // Renamed from successFlag to match DB
                    map.put("processTs", resultSet.getObject("process_ts") != null ? resultSet.getObject("process_ts", OffsetDateTime.class) : null);
                    map.put("eventJson", resultSet.getString("event_json"));
                    map.put("error", resultSet.getString("error"));
                    notifications.add(map);
                }
            }
            Map<String, Object> resultMap = new HashMap<>();
            resultMap.put("total", total);
            resultMap.put("notifications", notifications);
            return Success.of(JsonMapper.toJson(resultMap));
        } catch (SQLException e) {
            logger.error("SQLException:", e);
            return Failure.of(new Status(SQL_EXCEPTION, e.getMessage()));
        } catch (Exception e) {
            logger.error("Exception:", e);
            return Failure.of(new Status(GENERIC_EXCEPTION, e.getMessage()));
        }
    }
}
