package net.lightapi.portal.db.persistence;

import com.networknt.config.JsonMapper;
import com.networknt.db.provider.SqlDbStartupHook;
import com.networknt.monad.Failure;
import com.networknt.monad.Result;
import com.networknt.monad.Success;
import com.networknt.status.Status;
import com.networknt.utility.Constants;
import io.cloudevents.core.v1.CloudEventV1;
import net.lightapi.portal.PortalConstants;
import net.lightapi.portal.db.PortalDbProvider; // For shared constants initially
import net.lightapi.portal.db.util.NotificationService;
import net.lightapi.portal.db.util.SqlUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;
import java.time.OffsetDateTime;
import java.util.*;

import static com.networknt.db.provider.SqlDbStartupHook.ds;
import static net.lightapi.portal.db.util.SqlUtil.addCondition;

public class TagPersistenceImpl implements TagPersistence {
    private static final Logger logger = LoggerFactory.getLogger(TagPersistenceImpl.class);
    // Consider moving these to a shared constants class if they are truly general
    private static final String SQL_EXCEPTION = PortalDbProvider.SQL_EXCEPTION;
    private static final String GENERIC_EXCEPTION = PortalDbProvider.GENERIC_EXCEPTION;
    private static final String OBJECT_NOT_FOUND = PortalDbProvider.OBJECT_NOT_FOUND;

    private final NotificationService notificationService;

    public TagPersistenceImpl(NotificationService notificationService) {
        this.notificationService = notificationService;
    }
    @Override
    public Result<String> createTag(Map<String, Object> event) {
        final String sql = "INSERT INTO tag_t(host_id, tag_id, entity_type, tag_name, " +
                "tag_desc, update_user, update_ts) " +
                "VALUES (?, ?, ?, ?, ?, ?, ?)";
        Result<String> result;
        Map<String, Object> map = (Map<String, Object>)event.get(PortalConstants.DATA);
        String tagId = (String) map.get("tagId"); // Get tagId for return/logging/error

        try (Connection conn = ds.getConnection()) {
            conn.setAutoCommit(false);
            try (PreparedStatement statement = conn.prepareStatement(sql)) {
                String hostId = (String)map.get("hostId");
                if (hostId != null && !hostId.isEmpty()) {
                    statement.setObject(1, UUID.fromString(hostId));
                } else {
                    statement.setNull(1, Types.OTHER);
                }

                statement.setObject(2, UUID.fromString(tagId)); // Required
                statement.setString(3, (String)map.get("entityType")); // Required
                statement.setString(4, (String)map.get("tagName")); // Required

                String tagDesc = (String)map.get("tagDesc");
                if (tagDesc != null && !tagDesc.isBlank()) {
                    statement.setString(5, tagDesc);
                } else {
                    statement.setNull(5, Types.VARCHAR);
                }

                statement.setString(6, (String)event.get(Constants.USER));
                statement.setObject(7, OffsetDateTime.parse((String)event.get(CloudEventV1.TIME)));

                int count = statement.executeUpdate();
                if (count == 0) {
                    throw new SQLException("failed to insert the tag with id " + tagId);
                }
                conn.commit();
                result =  Success.of(tagId); // Return tagId
                notificationService.insertNotification(event, true, null);

            } catch (SQLException e) {
                logger.error("SQLException:", e);
                conn.rollback();
                notificationService.insertNotification(event, false, e.getMessage());
                result = Failure.of(new Status(SQL_EXCEPTION, e.getMessage()));
            } catch (Exception e) { // Catch other potential runtime exceptions
                logger.error("Exception:", e);
                conn.rollback();
                notificationService.insertNotification(event, false, e.getMessage());
                result = Failure.of(new Status(GENERIC_EXCEPTION, e.getMessage()));
            }
        } catch (SQLException e) {
            logger.error("SQLException:", e);
            result = Failure.of(new Status(SQL_EXCEPTION, e.getMessage()));
        }
        return result;
    }

    @Override
    public Result<String> updateTag(Map<String, Object> event) {
        final String sql = "UPDATE tag_t SET tag_name = ?, tag_desc = ?, update_user = ?, update_ts = ? WHERE tag_id = ?";
        Result<String> result;
        Map<String, Object> map = (Map<String, Object>)event.get(PortalConstants.DATA);
        String tagId = (String) map.get("tagId");

        try (Connection conn = ds.getConnection()) {
            conn.setAutoCommit(false);
            try (PreparedStatement statement = conn.prepareStatement(sql)) {
                statement.setString(1, (String)map.get("tagName"));
                String tagDesc = (String)map.get("tagDesc");
                if (tagDesc != null && !tagDesc.isBlank()) {
                    statement.setString(2, tagDesc);
                } else {
                    statement.setNull(2, Types.VARCHAR);
                }
                statement.setString(3, (String)event.get(Constants.USER));
                statement.setObject(4, OffsetDateTime.parse((String)event.get(CloudEventV1.TIME)));
                statement.setObject(5, UUID.fromString(tagId));

                int count = statement.executeUpdate();
                if (count == 0) {
                    throw new SQLException("failed to update the tag with id " + map.get("tagId"));
                }
                conn.commit();
                result =  Success.of((String)map.get("tagId")); // Return tagId
                notificationService.insertNotification(event, true, null);

            } catch (SQLException e) {
                logger.error("SQLException:", e);
                conn.rollback();
                notificationService.insertNotification(event, false, e.getMessage());
                result = Failure.of(new Status(SQL_EXCEPTION, e.getMessage()));
            } catch (Exception e) { // Catch other potential runtime exceptions
                logger.error("Exception:", e);
                conn.rollback();
                notificationService.insertNotification(event, false, e.getMessage());
                result = Failure.of(new Status(GENERIC_EXCEPTION, e.getMessage()));
            }
        } catch (SQLException e) {
            logger.error("SQLException:", e);
            result = Failure.of(new Status(SQL_EXCEPTION, e.getMessage()));
        }
        return result;
    }

    @Override
    public Result<String> deleteTag(Map<String, Object> event) {
        final String sql = "DELETE FROM tag_t WHERE tag_id = ?";
        Result<String> result;
        Map<String, Object> map = (Map<String, Object>)event.get(PortalConstants.DATA);
        String tagId = (String) map.get("tagId");
        try (Connection conn = ds.getConnection()) {
            conn.setAutoCommit(false);
            try (PreparedStatement statement = conn.prepareStatement(sql)) {
                statement.setObject(1, UUID.fromString(tagId));

                int count = statement.executeUpdate();
                if (count == 0) {
                    throw new SQLException("failed to delete the tag with id " + map.get("tagId"));
                }
                conn.commit();
                result =  Success.of(tagId); // Return tagId
                notificationService.insertNotification(event, true, null);

            } catch (SQLException e) {
                logger.error("SQLException:", e);
                conn.rollback();
                notificationService.insertNotification(event, false, e.getMessage());
                result = Failure.of(new Status(SQL_EXCEPTION, e.getMessage()));
            } catch (Exception e) { // Catch other potential runtime exceptions
                logger.error("Exception:", e);
                conn.rollback();
                notificationService.insertNotification(event, false, e.getMessage());
                result = Failure.of(new Status(GENERIC_EXCEPTION, e.getMessage()));
            }
        } catch (SQLException e) {
            logger.error("SQLException:", e);
            result = Failure.of(new Status(SQL_EXCEPTION, e.getMessage()));
        }
        return result;
    }

    @Override
    public Result<String> getTag(int offset, int limit, String hostId, String tagId, String entityType, String tagName, String tagDesc) {
        Result<String> result = null;
        String s =
                """
                        SELECT COUNT(*) OVER () AS total,
                        tag_id, host_id, entity_type, tag_name, tag_desc, update_user, update_ts
                        FROM tag_t
                """;
        StringBuilder sqlBuilder = new StringBuilder();
        sqlBuilder.append(s);

        List<Object> parameters = new ArrayList<>();
        StringBuilder whereClause = new StringBuilder();

        if (hostId != null && !hostId.isEmpty()) {
            // Manually construct the OR group for host_id
            whereClause.append("WHERE (host_id = ? OR host_id IS NULL)");
            parameters.add(UUID.fromString(hostId));
        } else {
            // Only add 'host_id IS NULL' if hostId parameter is NOT provided
            // This means we ONLY want global tables in this case.
            // If hostId WAS provided, the '(cond OR NULL)' handles both cases.
            whereClause.append("WHERE host_id IS NULL");
        }

        addCondition(whereClause, parameters, "tag_id", tagId != null ? UUID.fromString(tagId) : null);
        addCondition(whereClause, parameters, "entity_type", entityType);
        addCondition(whereClause, parameters, "tag_name", tagName);
        addCondition(whereClause, parameters, "tag_desc", tagDesc);

        if (!whereClause.isEmpty()) {
            sqlBuilder.append(whereClause);
        }

        sqlBuilder.append(" ORDER BY tag_name\n" +
                "LIMIT ? OFFSET ?");

        parameters.add(limit);
        parameters.add(offset);

        String sql = sqlBuilder.toString();
        if(logger.isTraceEnabled()) logger.trace("sql: {}", sql);
        int total = 0;
        List<Map<String, Object>> tags = new ArrayList<>();

        try (Connection connection = ds.getConnection();
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
                    map.put("tagId", resultSet.getObject("tag_id", UUID.class));
                    map.put("hostId", resultSet.getObject("host_id", UUID.class));
                    map.put("entityType", resultSet.getString("entity_type"));
                    map.put("tagName", resultSet.getString("tag_name"));
                    map.put("tagDesc", resultSet.getString("tag_desc"));
                    map.put("updateUser", resultSet.getString("update_user"));
                    map.put("updateTs", resultSet.getObject("update_ts") != null ? resultSet.getObject("update_ts", OffsetDateTime.class) : null);

                    tags.add(map);
                }
            }

            Map<String, Object> resultMap = new HashMap<>();
            resultMap.put("total", total);
            resultMap.put("tags", tags);
            result = Success.of(JsonMapper.toJson(resultMap));

        } catch (SQLException e) {
            logger.error("SQLException:", e);
            result = Failure.of(new Status(SQL_EXCEPTION, e.getMessage()));
        } catch (Exception e) {
            logger.error("Exception:", e);
            result = Failure.of(new Status(GENERIC_EXCEPTION, e.getMessage()));
        }
        return result;
    }

    @Override
    public Result<String> getTagLabel(String hostId) {
        Result<String> result = null;
        String sql = "SELECT tag_id, tag_name FROM tag_t WHERE host_id = ? OR host_id IS NULL";
        List<Map<String, Object>> labels = new ArrayList<>();
        try (Connection connection = ds.getConnection();
             PreparedStatement preparedStatement = connection.prepareStatement(sql)) {
            preparedStatement.setObject(1, UUID.fromString(hostId));
            try (ResultSet resultSet = preparedStatement.executeQuery()) {
                while (resultSet.next()) {
                    Map<String, Object> map = new HashMap<>();
                    map.put("id", resultSet.getString("tag_id"));
                    map.put("label", resultSet.getString("tag_name"));
                    labels.add(map);
                }
            }
            result = Success.of(JsonMapper.toJson(labels));
        } catch (SQLException e) {
            logger.error("SQLException:", e);
            result = Failure.of(new Status(SQL_EXCEPTION, e.getMessage()));
        } catch (Exception e) {
            logger.error("Exception:", e);
            result = Failure.of(new Status(GENERIC_EXCEPTION, e.getMessage()));
        }
        return result;
    }

    @Override
    public Result<String> getTagById(String tagId) {
        Result<String> result = null;
        String sql = "SELECT tag_id, host_id, entity_type, tag_name, tag_desc, update_user, update_ts FROM tag_t WHERE tag_id = ?";
        Map<String, Object> map = null;
        try (Connection conn = ds.getConnection()) {
            conn.setAutoCommit(false); // Although not strictly needed for SELECT, keeping template consistent
            try (PreparedStatement statement = conn.prepareStatement(sql)) {
                statement.setString(1, tagId);
                try (ResultSet resultSet = statement.executeQuery()) {
                    if (resultSet.next()) {
                        map = new HashMap<>();
                        map.put("tagId", resultSet.getObject("tag_id", UUID.class));
                        map.put("hostId", resultSet.getObject("host_id", UUID.class));
                        map.put("entityType", resultSet.getString("entity_type"));
                        map.put("tagName", resultSet.getString("tag_name"));
                        map.put("tagDesc", resultSet.getString("tag_desc"));
                        map.put("updateUser", resultSet.getString("update_user"));
                        map.put("updateTs", resultSet.getObject("update_ts") != null ? resultSet.getObject("update_ts", OffsetDateTime.class) : null);
                    }
                }
                if (map != null && !map.isEmpty()) {
                    result = Success.of(JsonMapper.toJson(map));
                } else {
                    result = Success.of(null); // Or perhaps Failure.of(NOT_FOUND Status) if tag must exist
                }
            }
        } catch (SQLException e) {
            logger.error("SQLException:", e);
            result = Failure.of(new Status(SQL_EXCEPTION, e.getMessage()));
        } catch (Exception e) {
            logger.error("Exception:", e);
            result = Failure.of(new Status(GENERIC_EXCEPTION, e.getMessage()));
        }
        return result;
    }


    @Override
    public Result<String> getTagByName(String hostId, String tagName) {
        Result<String> result = null;
        StringBuilder sqlBuilder = new StringBuilder();
        sqlBuilder.append("SELECT tag_id, host_id, entity_type, tag_name, tag_desc, update_user, update_ts\n" +
                "FROM tag_t\n" +
                "WHERE tag_name = ?\n"); // Filter by tagName

        if (hostId != null && !hostId.isEmpty()) {
            sqlBuilder.append("AND (host_id = ? OR host_id IS NULL)"); // Tenant-specific OR Global
        } else {
            sqlBuilder.append("AND host_id IS NULL"); // Only Global if hostId is null/empty
        }

        String sql = sqlBuilder.toString();
        Map<String, Object> map = null;
        try (Connection conn = ds.getConnection()) {
            conn.setAutoCommit(false); // Although not strictly needed for SELECT, keeping template consistent
            try (PreparedStatement preparedStatement = conn.prepareStatement(sql)) {

                int parameterIndex = 1;
                preparedStatement.setString(parameterIndex++, tagName); // 1. tagName

                if (hostId != null && !hostId.isEmpty()) {
                    preparedStatement.setObject(parameterIndex++, UUID.fromString(hostId)); // 2. hostId (for tenant-specific OR global)
                }

                try (ResultSet resultSet = preparedStatement.executeQuery()) {
                    if (resultSet.next()) {
                        map = new HashMap<>();
                        map.put("tagId", resultSet.getObject("tag_id", UUID.class));
                        map.put("hostId", resultSet.getObject("host_id", UUID.class));
                        map.put("entityType", resultSet.getString("entity_type"));
                        map.put("tagName", resultSet.getString("tag_name"));
                        map.put("tagDesc", resultSet.getString("tag_desc"));
                        map.put("updateUser", resultSet.getString("update_user"));
                        map.put("updateTs", resultSet.getObject("update_ts") != null ? resultSet.getObject("update_ts", OffsetDateTime.class) : null);
                    }
                }
                if (map != null && !map.isEmpty()) {
                    result = Success.of(JsonMapper.toJson(map));
                } else {
                    result = Failure.of(new Status(OBJECT_NOT_FOUND, tagName));
                }
            }
        } catch (SQLException e) {
            logger.error("SQLException:", e);
            result = Failure.of(new Status(SQL_EXCEPTION, e.getMessage()));
        } catch (Exception e) {
            logger.error("Exception:", e);
            result = Failure.of(new Status(GENERIC_EXCEPTION, e.getMessage()));
        }
        return result;
    }


    @Override
    public Result<String> getTagByType(String hostId, String entityType) {
        Result<String> result = null;
        StringBuilder sqlBuilder = new StringBuilder();
        sqlBuilder.append("SELECT tag_id, host_id, entity_type, tag_name, tag_desc, update_user, update_ts\n" +
                "FROM tag_t\n" +
                "WHERE entity_type = ?\n"); // Filter by entityType

        if (hostId != null && !hostId.isEmpty()) {
            sqlBuilder.append("AND (host_id = ? OR host_id IS NULL)"); // Tenant-specific OR Global
        } else {
            sqlBuilder.append("AND host_id IS NULL"); // Only Global if hostId is null/empty
        }

        String sql = sqlBuilder.toString();
        List<Map<String, Object>> tags = new ArrayList<>();
        try (Connection conn = ds.getConnection(); PreparedStatement preparedStatement = conn.prepareStatement(sql)) {
            int parameterIndex = 1;
            preparedStatement.setString(parameterIndex++, entityType); // 1. entityType

            if (hostId != null && !hostId.isEmpty()) {
                preparedStatement.setObject(parameterIndex++, UUID.fromString(hostId)); // 2. hostId (for tenant-specific OR global)
            }

            try (ResultSet resultSet = preparedStatement.executeQuery()) {
                while (resultSet.next()) {
                    Map<String, Object> map = new HashMap<>();
                    map.put("tagId", resultSet.getObject("tag_id", UUID.class));
                    map.put("hostId", resultSet.getObject("host_id", UUID.class));
                    map.put("entityType", resultSet.getString("entity_type"));
                    map.put("tagName", resultSet.getString("tag_name"));
                    map.put("tagDesc", resultSet.getString("tag_desc"));
                    map.put("updateUser", resultSet.getString("update_user"));
                    map.put("updateTs", resultSet.getObject("update_ts") != null ? resultSet.getObject("update_ts", OffsetDateTime.class) : null);

                    tags.add(map);
                }
            }
            Map<String, Object> resultMap = new HashMap<>();
            resultMap.put("tags", tags);
            result = Success.of(JsonMapper.toJson(resultMap));
        } catch (SQLException e) {
            logger.error("SQLException:", e);
            result = Failure.of(new Status(SQL_EXCEPTION, e.getMessage()));
        } catch (Exception e) {
            logger.error("Exception:", e);
            result = Failure.of(new Status(GENERIC_EXCEPTION, e.getMessage()));
        }
        return result;
    }

}
