package net.lightapi.portal.db.persistence;

import com.networknt.config.JsonMapper;
import com.networknt.monad.Failure;
import com.networknt.monad.Result;
import com.networknt.monad.Success;
import com.networknt.status.Status;
import com.networknt.utility.Constants;
import io.cloudevents.core.v1.CloudEventV1;
import net.lightapi.portal.PortalConstants;
import net.lightapi.portal.db.ConcurrencyException;
import net.lightapi.portal.db.PortalDbProvider; // For shared constants initially
import net.lightapi.portal.db.util.NotificationService;
import net.lightapi.portal.db.util.SqlUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.time.OffsetDateTime;
import java.util.*;

import static com.networknt.db.provider.SqlDbStartupHook.ds;
import static net.lightapi.portal.db.util.SqlUtil.*;

public class TagPersistenceImpl implements TagPersistence {
    private static final Logger logger = LoggerFactory.getLogger(TagPersistenceImpl.class);
    private static final String SQL_EXCEPTION = PortalDbProvider.SQL_EXCEPTION;
    private static final String GENERIC_EXCEPTION = PortalDbProvider.GENERIC_EXCEPTION;
    private static final String OBJECT_NOT_FOUND = PortalDbProvider.OBJECT_NOT_FOUND;

    private final NotificationService notificationService;

    public TagPersistenceImpl(NotificationService notificationService) {
        this.notificationService = notificationService;
    }

    @Override
    public void createTag(Connection conn, Map<String, Object> event) throws SQLException, Exception {
        final String sql =
                """
                INSERT INTO tag_t(host_id, tag_id, entity_type, tag_name,
                tag_desc, update_user, update_ts, aggregate_version)
                VALUES (?, ?, ?, ?, ?, ?, ?)
                """;
        Map<String, Object> map = (Map<String, Object>)event.get(PortalConstants.DATA);
        String tagId = (String) map.get("tagId");
        long newAggregateVersion = SqlUtil.getNewAggregateVersion(event);

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
            statement.setLong(8, newAggregateVersion);

            int count = statement.executeUpdate();
            if (count == 0) {
                throw new SQLException(String.format("Failed during createTag for tagId %s with aggregateVersion %d", tagId, newAggregateVersion));
            }
        } catch (SQLException e) {
            logger.error("SQLException during createTag for tagId {} aggregateVersion {}: {}", tagId, newAggregateVersion, e.getMessage(), e);
            throw e;
        } catch (Exception e) {
            logger.error("Exception during createTag for tagId {} aggregateVersion {}: {}", tagId, newAggregateVersion, e.getMessage(), e);
            throw e;
        }
    }

    private boolean queryTagExists(Connection conn, String tagId) throws SQLException {
        final String sql =
                """
                SELECT COUNT(*) FROM tag_t WHERE tag_id = ?
                """;
        try (PreparedStatement pst = conn.prepareStatement(sql)) {
            pst.setObject(1, UUID.fromString(tagId));
            try (ResultSet rs = pst.executeQuery()) {
                return rs.next() && rs.getInt(1) > 0;
            }
        }
    }

    @Override
    public void updateTag(Connection conn, Map<String, Object> event) throws SQLException, Exception {
        final String sql =
                """
               UPDATE tag_t
               SET tag_name = ?, tag_desc = ?, update_user = ?, update_ts = ?, aggregate_version = ?
               WHERE tag_id = ? AND aggregate_version = ?
               """;
        Map<String, Object> map = (Map<String, Object>)event.get(PortalConstants.DATA);
        String tagId = (String) map.get("tagId");
        long oldAggregateVersion = SqlUtil.getOldAggregateVersion(event);
        long newAggregateVersion = SqlUtil.getNewAggregateVersion(event);

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
            statement.setLong(5, newAggregateVersion);
            statement.setObject(6, UUID.fromString(tagId));
            statement.setLong(7, oldAggregateVersion);

            int count = statement.executeUpdate();
            if (count == 0) {
                if (queryTagExists(conn, tagId)) {
                    throw new ConcurrencyException("Optimistic concurrency conflict during updateTag for tagId " + tagId + ". Expected version " + oldAggregateVersion + " but found a different version " + newAggregateVersion + ".");
                } else {
                    throw new SQLException("No record found during updateTag for tagId " + tagId + ".");
                }
            }
        } catch (SQLException e) {
            logger.error("SQLException during updateTag for tagId {} (old: {}) -> (new: {}): {}", tagId, oldAggregateVersion, newAggregateVersion, e.getMessage(), e);
            throw e;
        } catch (Exception e) {
            logger.error("Exception during updateTag for tagId {} (old: {}) -> (new: {}): {}", tagId, oldAggregateVersion, newAggregateVersion, e.getMessage(), e);
            throw e;
        }
    }

    @Override
    public void deleteTag(Connection conn, Map<String, Object> event) throws SQLException, Exception {
        final String sql = "DELETE FROM tag_t WHERE tag_id = ? AND aggregate_version = ?";
        Map<String, Object> map = (Map<String, Object>)event.get(PortalConstants.DATA);
        String tagId = (String) map.get("tagId"); // For logging/exceptions
        long oldAggregateVersion = SqlUtil.getOldAggregateVersion(event);

        try (PreparedStatement statement = conn.prepareStatement(sql)) {
            statement.setObject(1, UUID.fromString(tagId));
            statement.setLong(2, oldAggregateVersion);

            int count = statement.executeUpdate();
            if (count == 0) {
                if (queryTagExists(conn, tagId)) {
                    throw new ConcurrencyException("Optimistic concurrency conflict during deleteTag for tagId " + tagId + " aggregateVersion " + oldAggregateVersion + " but found a different version or already updated.");
                } else {
                    throw new SQLException("No record found during deleteTag for tagId " + tagId + ". It might have been already deleted.");
                }
            }
        } catch (SQLException e) {
            logger.error("SQLException during deleteTag for tagId {} aggregateVersion {}: {}", tagId, oldAggregateVersion, e.getMessage(), e);
            throw e;
        } catch (Exception e) {
            logger.error("Exception during deleteTag for tagId {} aggregateVersion {}: {}", tagId, oldAggregateVersion, e.getMessage(), e);
            throw e;
        }
    }

    @Override
    public Result<String> getTag(int offset, int limit, String filtersJson, String globalFilter, String sortingJson, String hostId) {
        Result<String> result = null;
        List<Map<String, Object>> filters = parseJsonList(filtersJson);
        List<Map<String, Object>> sorting = parseJsonList(sortingJson);

        String s =
            """
                SELECT COUNT(*) OVER () AS total,
                tag_id, host_id, entity_type, tag_name, tag_desc,
                update_user, update_ts, aggregate_version, active
                FROM tag_t
            """;

        List<Object> parameters = new ArrayList<>();

        if (hostId != null && !hostId.isEmpty()) {
            // Manually construct the OR group for host_id
            s = s + "WHERE (host_id = ? OR host_id IS NULL)";
            parameters.add(UUID.fromString(hostId));
        } else {
            // Only add 'host_id IS NULL' if hostId parameter is NOT provided
            // This means we ONLY want global tables in this case.
            // If hostId WAS provided, the '(cond OR NULL)' handles both cases.
            s = s + "WHERE host_id IS NULL";
        }

        String[] searchColumns = {"tag_name", "tag_desc"};
        String sqlBuilder = s + dynamicFilter(Arrays.asList("host_id", "tag_id"), Arrays.asList(searchColumns), filters, null, parameters) +
                globalFilter(globalFilter, searchColumns, parameters) +
                dynamicSorting("tag_name", sorting, null) +
                "\nLIMIT ? OFFSET ?";

        parameters.add(limit);
        parameters.add(offset);
        int total = 0;
        List<Map<String, Object>> tags = new ArrayList<>();

        try (Connection connection = ds.getConnection();
            PreparedStatement preparedStatement = connection.prepareStatement(sqlBuilder)) {
            populateParameters(preparedStatement, parameters);
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
                    map.put("aggregateVersion", resultSet.getLong("aggregate_version"));
                    map.put("active", resultSet.getBoolean("active"));
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
        String sql = "SELECT tag_id, host_id, entity_type, tag_name, tag_desc, update_user, update_ts, aggregate_version FROM tag_t WHERE tag_id = ?";
        Map<String, Object> map = null;
        try (Connection conn = ds.getConnection()) {
            // No setAutoCommit(false) needed for SELECT
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
                        map.put("aggregateVersion", resultSet.getLong("aggregate_version"));
                    }
                }
                if (map != null && !map.isEmpty()) {
                    result = Success.of(JsonMapper.toJson(map));
                } else {
                    // Record not found
                    result = Failure.of(new Status(OBJECT_NOT_FOUND, "tag", tagId)); // Consistent with AccessControlPersistence
                }
            }
        } catch (SQLException e) {
            logger.error("SQLException getting tag by id {}:", tagId, e);
            result = Failure.of(new Status(SQL_EXCEPTION, e.getMessage()));
        } catch (Exception e) {
            logger.error("Unexpected exception getting tag by id {}:", tagId, e);
            result = Failure.of(new Status(GENERIC_EXCEPTION, e.getMessage()));
        }
        return result;
    }

    @Override
    public Result<String> getTagByName(String hostId, String tagName) {
        Result<String> result = null;
        String s =
                """
                SELECT tag_id, host_id, entity_type, tag_name, tag_desc, update_user, update_ts, aggregate_version
                FROM tag_t
                WHERE tag_name = ?
                """;
        StringBuilder sqlBuilder = new StringBuilder(s);

        if (hostId != null && !hostId.isEmpty()) {
            sqlBuilder.append("AND (host_id = ? OR host_id IS NULL)"); // Tenant-specific OR Global
        } else {
            sqlBuilder.append("AND host_id IS NULL"); // Only Global if hostId is null/empty
        }

        String sql = sqlBuilder.toString();
        Map<String, Object> map = null;
        try (Connection conn = ds.getConnection()) {
            // No setAutoCommit(false) for read-only query
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
                        map.put("aggregateVersion", resultSet.getLong("aggregate_version"));
                    }
                }
                if (map != null && !map.isEmpty()) {
                    result = Success.of(JsonMapper.toJson(map));
                } else {
                    result = Failure.of(new Status(OBJECT_NOT_FOUND, "tag", tagName)); // Consistent with AccessControlPersistence
                }
            }
        } catch (SQLException e) {
            logger.error("SQLException getting tag by name {}:", tagName, e);
            result = Failure.of(new Status(SQL_EXCEPTION, e.getMessage()));
        } catch (Exception e) {
            logger.error("Unexpected exception getting tag by name {}:", tagName, e);
            result = Failure.of(new Status(GENERIC_EXCEPTION, e.getMessage()));
        }
        return result;
    }

    @Override
    public Result<String> getTagByType(String hostId, String entityType) {
        Result<String> result = null;
        String s =
                """
                SELECT tag_id, host_id, entity_type, tag_name, tag_desc, update_user, update_ts, aggregate_version
                FROM tag_t
                WHERE entity_type = ?
                """;

        StringBuilder sqlBuilder = new StringBuilder(s);
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
                    map.put("aggregateVersion", resultSet.getLong("aggregate_version"));

                    tags.add(map);
                }
            }
            Map<String, Object> resultMap = new HashMap<>();
            resultMap.put("tags", tags);
            result = Success.of(JsonMapper.toJson(resultMap));
        } catch (SQLException e) {
            logger.error("SQLException getting tags by type {}:", entityType, e);
            result = Failure.of(new Status(SQL_EXCEPTION, e.getMessage()));
        } catch (Exception e) {
            logger.error("Unexpected exception getting tags by type {}:", entityType, e);
            result = Failure.of(new Status(GENERIC_EXCEPTION, e.getMessage()));
        }
        return result;
    }

}
