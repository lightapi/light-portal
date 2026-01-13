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
        // Use UPSERT based on the Primary Key (tag_id): INSERT ON CONFLICT DO UPDATE
        // This handles:
        // 1. First time insert (no conflict).
        // 2. Re-creation (conflict on tag_id) -> UPDATE the existing soft-deleted row (setting active=TRUE and new version).

        final String sql =
                """
                INSERT INTO tag_t(
                    host_id,
                    tag_id,
                    entity_type,
                    tag_name,
                    tag_desc,
                    update_user,
                    update_ts,
                    aggregate_version,
                    active
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, TRUE)
                ON CONFLICT (tag_id) DO UPDATE
                SET host_id = EXCLUDED.host_id,
                    entity_type = EXCLUDED.entity_type,
                    tag_name = EXCLUDED.tag_name,
                    tag_desc = EXCLUDED.tag_desc,
                    update_user = EXCLUDED.update_user,
                    update_ts = EXCLUDED.update_ts,
                    aggregate_version = EXCLUDED.aggregate_version,
                    active = TRUE
                -- OCC/IDM: Only update if the incoming event is newer
                WHERE tag_t.aggregate_version < EXCLUDED.aggregate_version
                AND tag_t.active = FALSE
                """;

        // Note: The original code uses a non-standard map retrieval: Map<String, Object> map = (Map<String, Object>)event.get(PortalConstants.DATA);
        // Assuming SqlUtil.extractEventData(event) is the correct utility based on other methods.
        Map<String, Object> map = SqlUtil.extractEventData(event);
        String hostId = (String)map.get("hostId");
        String tagId = (String) map.get("tagId");
        long newAggregateVersion = SqlUtil.getNewAggregateVersion(event);

        try (PreparedStatement statement = conn.prepareStatement(sql)) {
            // INSERT values (8 placeholders + active=TRUE in SQL, total 8 dynamic values)
            int i = 1;

            // 1: host_id
            if (hostId != null && !hostId.isEmpty()) {
                statement.setObject(i++, UUID.fromString(hostId));
            } else {
                statement.setNull(i++, Types.OTHER);
            }

            // 2: tag_id
            statement.setObject(i++, UUID.fromString(tagId)); // Required
            // 3: entity_type
            statement.setString(i++, (String)map.get("entityType")); // Required
            // 4: tag_name
            statement.setString(i++, (String)map.get("tagName")); // Required

            // 5: tag_desc
            String tagDesc = (String)map.get("tagDesc");
            if (tagDesc != null && !tagDesc.isBlank()) {
                statement.setString(i++, tagDesc);
            } else {
                statement.setNull(i++, Types.VARCHAR);
            }

            // 6: update_user
            statement.setString(i++, (String)event.get(Constants.USER));
            // 7: update_ts
            statement.setObject(i++, OffsetDateTime.parse((String)event.get(CloudEventV1.TIME)));
            // 8: aggregate_version
            statement.setLong(i++, newAggregateVersion);

            int count = statement.executeUpdate();
            if (count == 0) {
                // count=0 means the ON CONFLICT clause was hit, BUT the WHERE clause (aggregate_version < EXCLUDED.aggregate_version) failed.
                // This is the desired idempotent/out-of-order protection behavior. Log and ignore.
                logger.warn("Creation/Reactivation skipped for tagId {} aggregateVersion {}. A newer or same version already exists.", tagId, newAggregateVersion);
            }
        } catch (SQLException e) {
            logger.error("SQLException during createTag for tagId {} aggregateVersion {}: {}", tagId, newAggregateVersion, e.getMessage(), e);
            throw e;
        } catch (Exception e) {
            logger.error("Exception during createTag for tagId {} aggregateVersion {}: {}", tagId, newAggregateVersion, e.getMessage(), e);
            throw e;
        }
    }

    @Override
    public void updateTag(Connection conn, Map<String, Object> event) throws SQLException, Exception {
        // We attempt to update the record IF the incoming event's aggregate_version is greater than the current projection's version.
        // This enforces Idempotence (IDM) and Optimistic Concurrency Control (OCC) by ensuring version monotonicity.
        // We explicitly set active = TRUE as an UPDATE event implies the tag should be active.
        final String sql =
                """
                UPDATE tag_t
                SET tag_name = ?,
                    tag_desc = ?,
                    update_user = ?,
                    update_ts = ?,
                    aggregate_version = ?,
                    active = TRUE
                WHERE tag_id = ?
                  AND aggregate_version < ?
                """; // <<< CRITICAL: Changed aggregate_version = ? to aggregate_version < ? to enforce monotonicity (OCC/IDM)

        // Note: The original code uses a non-standard map retrieval: Map<String, Object> map = (Map<String, Object>)event.get(PortalConstants.DATA);
        // Assuming SqlUtil.extractEventData(event) is the correct utility based on other methods.
        Map<String, Object> map = SqlUtil.extractEventData(event);
        String tagId = (String) map.get("tagId");
        long newAggregateVersion = SqlUtil.getNewAggregateVersion(event);

        try (PreparedStatement statement = conn.prepareStatement(sql)) {
            // SET values (5 dynamic values + active = TRUE in SQL)
            int i = 1;
            // 1: tag_name
            statement.setString(i++, (String)map.get("tagName"));
            // 2: tag_desc
            String tagDesc = (String)map.get("tagDesc");
            if (tagDesc != null && !tagDesc.isBlank()) {
                statement.setString(i++, tagDesc);
            } else {
                statement.setNull(i++, Types.VARCHAR);
            }
            // 3: update_user
            statement.setString(i++, (String)event.get(Constants.USER));
            // 4: update_ts
            statement.setObject(i++, OffsetDateTime.parse((String)event.get(CloudEventV1.TIME)));
            // 5: aggregate_version
            statement.setLong(i++, newAggregateVersion);

            // WHERE conditions (2 placeholders)
            // 6: tag_id
            statement.setObject(i++, UUID.fromString(tagId));
            // 7: aggregate_version < ? (new version for OCC/IDM check)
            statement.setLong(i++, newAggregateVersion);

            int count = statement.executeUpdate();
            if (count == 0) {
                // If 0 rows updated, it means the record was either not found
                // OR aggregate_version >= newAggregateVersion (OCC/IDM check failed).
                // We IGNORE the failure and log a warning, as this is the desired idempotent/monotonic behavior.
                logger.warn("Update skipped for tagId {} aggregateVersion {}. Record not found or a newer/same version already exists.", tagId, newAggregateVersion);
            }
        } catch (SQLException e) {
            logger.error("SQLException during updateTag for tagId {} aggregateVersion {}: {}", tagId, newAggregateVersion, e.getMessage(), e);
            throw e;
        } catch (Exception e) {
            logger.error("Exception during updateTag for tagId {} aggregateVersion {}: {}", tagId, newAggregateVersion, e.getMessage(), e);
            throw e;
        }
    }

    @Override
    public void deleteTag(Connection conn, Map<String, Object> event) throws SQLException, Exception {
        // Use UPDATE to implement Soft Delete (setting active = FALSE).
        // OCC/IDM is enforced by checking aggregate_version < newAggregateVersion.
        final String sql =
                """
                UPDATE tag_t
                SET active = FALSE,
                    update_user = ?,
                    update_ts = ?,
                    aggregate_version = ?
                WHERE tag_id = ?
                  AND aggregate_version < ?
                """; // <<< CRITICAL: Added aggregate_version < ? to enforce monotonicity (OCC/IDM)

        // Note: The original code uses a non-standard map retrieval: Map<String, Object> map = (Map<String, Object>)event.get(PortalConstants.DATA);
        // Assuming SqlUtil.extractEventData(event) is the correct utility based on other methods.
        Map<String, Object> map = SqlUtil.extractEventData(event);
        String tagId = (String) map.get("tagId");
        // A delete event represents a state change, so it should have a new, incremented version.
        long newAggregateVersion = SqlUtil.getNewAggregateVersion(event);

        try (PreparedStatement statement = conn.prepareStatement(sql)) {
            // SET values (3 placeholders)
            // 1: update_user
            statement.setString(1, (String)event.get(Constants.USER));
            // 2: update_ts
            statement.setObject(2, OffsetDateTime.parse((String)event.get(CloudEventV1.TIME)));
            // 3: aggregate_version (the new version)
            statement.setLong(3, newAggregateVersion);

            // WHERE conditions (2 placeholders)
            // 4: tag_id
            statement.setObject(4, UUID.fromString(tagId));
            // 5: aggregate_version < ? (new version for OCC/IDM check)
            statement.setLong(5, newAggregateVersion);

            int count = statement.executeUpdate();
            if (count == 0) {
                // If 0 rows updated, it means:
                // 1. The record was not found (already deleted or never existed).
                // 2. The OCC/IDM check failed (aggregate_version >= newAggregateVersion).
                // We IGNORE the failure and log a warning, as this is the desired idempotent/monotonic behavior.
                logger.warn("Soft delete skipped for tagId {} aggregateVersion {}. Record not found or a newer/same version already exists.", tagId, newAggregateVersion);
            }
        } catch (SQLException e) {
            logger.error("SQLException during deleteTag for tagId {} aggregateVersion {}: {}", tagId, newAggregateVersion, e.getMessage(), e);
            throw e;
        } catch (Exception e) {
            logger.error("Exception during deleteTag for tagId {} aggregateVersion {}: {}", tagId, newAggregateVersion, e.getMessage(), e);
            throw e;
        }
    }

    @Override
    public void createEntityTag(Connection conn, Map<String, Object> event) throws SQLException, Exception {
        // Use UPSERT based on the Primary Key (entity_id, entity_type, tag_id): INSERT ON CONFLICT DO UPDATE
        // This handles:
        // 1. First time insert (no conflict).
        // 2. Re-creation (conflict on entity_id, entity_type, tag_id) -> UPDATE the existing soft-deleted row (setting active=TRUE and new version).

        final String sql =
                """
                INSERT INTO entity_tag_t (
                    entity_id,
                    entity_type,
                    tag_id,
                    update_user,
                    update_ts,
                    aggregate_version,
                    active
                ) VALUES (?, ?, ?, ?, ?, ?, TRUE)
                ON CONFLICT (entity_id, entity_type, tag_id) DO UPDATE
                SET update_user = EXCLUDED.update_user,
                    update_ts = EXCLUDED.update_ts,
                    aggregate_version = EXCLUDED.aggregate_version,
                    active = TRUE
                -- OCC/IDM: Only update if the incoming event is newer
                WHERE entity_tag_t.aggregate_version < EXCLUDED.aggregate_version
                """;

        // Note: Assuming SqlUtil.extractEventData(event) is the correct utility based on other methods.
        Map<String, Object> map = SqlUtil.extractEventData(event);

        String entityId = (String)map.get("entityId");
        String entityType = (String)map.get("entityType");
        String tagId = (String)map.get("tagId");
        long newAggregateVersion = SqlUtil.getNewAggregateVersion(event);

        try (PreparedStatement statement = conn.prepareStatement(sql)) {
            // INSERT values (6 placeholders + active=TRUE in SQL, total 6 dynamic values)
            int i = 1;
            // 1: entity_id
            statement.setObject(i++, UUID.fromString(entityId));
            // 2: entity_type
            statement.setString(i++, entityType);
            // 3: tag_id
            statement.setObject(i++, UUID.fromString(tagId));

            // 4: update_user
            statement.setString(i++, (String)event.get(Constants.USER));
            // 5: update_ts
            statement.setObject(i++, OffsetDateTime.parse((String)event.get(CloudEventV1.TIME)));
            // 6: aggregate_version
            statement.setLong(i++, newAggregateVersion);

            int count = statement.executeUpdate();
            if (count == 0) {
                // count=0 means the ON CONFLICT clause was hit, BUT the WHERE clause (aggregate_version < EXCLUDED.aggregate_version) failed.
                // This is the desired idempotent/out-of-order protection behavior. Log and ignore.
                logger.warn("Creation/Reactivation skipped for entityId {} entityType {} tagId {} aggregateVersion {}. A newer or same version already exists.", entityId, entityType, tagId, newAggregateVersion);
            }
        } catch (SQLException e) {
            logger.error("SQLException during createEntityTag for entityId {} entityType {} tagId {} aggregateVersion {}: {}", entityId, entityType, tagId, newAggregateVersion, e.getMessage(), e);
            throw e;
        } catch (Exception e) {
            logger.error("Exception during createEntityTag for entityId {} entityType {} tagId {} aggregateVersion {}: {}", entityId, entityType, tagId, newAggregateVersion, e.getMessage(), e);
            throw e;
        }
    }

    @Override
    public void updateEntityTag(Connection conn, Map<String, Object> event) throws SQLException, Exception {
        // We attempt to update the record IF the incoming event's aggregate_version is greater than the current projection's version.
        // This enforces Idempotence (IDM) and Optimistic Concurrency Control (OCC) by ensuring version monotonicity.
        // We explicitly set active = TRUE as an UPDATE event implies the assignment should be active.
        final String sql =
                """
                UPDATE entity_tag_t
                SET update_user = ?,
                    update_ts = ?,
                    aggregate_version = ?,
                    active = TRUE
                WHERE entity_id = ?
                  AND entity_type = ?
                  AND tag_id = ?
                  AND aggregate_version < ?
                """; // <<< CRITICAL: Added aggregate_version < ? to enforce monotonicity (OCC/IDM)

        // Note: Assuming SqlUtil.extractEventData(event) is the correct utility based on other methods.
        Map<String, Object> map = SqlUtil.extractEventData(event);

        String entityId = (String)map.get("entityId");
        String entityType = (String)map.get("entityType");
        String tagId = (String)map.get("tagId");
        long newAggregateVersion = SqlUtil.getNewAggregateVersion(event);

        try (PreparedStatement statement = conn.prepareStatement(sql)) {
            // SET values (3 dynamic values + active = TRUE in SQL)
            int i = 1;
            // 1: update_user
            statement.setString(i++, (String)event.get(Constants.USER));
            // 2: update_ts
            statement.setObject(i++, OffsetDateTime.parse((String)event.get(CloudEventV1.TIME)));
            // 3: aggregate_version
            statement.setLong(i++, newAggregateVersion);

            // WHERE conditions (4 placeholders)
            // 4: entity_id
            statement.setObject(i++, UUID.fromString(entityId));
            // 5: entity_type
            statement.setString(i++, entityType);
            // 6: tag_id
            statement.setObject(i++, UUID.fromString(tagId));
            // 7: aggregate_version < ? (new version for OCC/IDM check)
            statement.setLong(i++, newAggregateVersion);

            int count = statement.executeUpdate();
            if (count == 0) {
                // If 0 rows updated, it means the record was either not found
                // OR aggregate_version >= newAggregateVersion (OCC/IDM check failed).
                // We IGNORE the failure and log a warning, as this is the desired idempotent/monotonic behavior.
                logger.warn("Update skipped for entityId {} entityType {} tagId {} aggregateVersion {}. Record not found or a newer/same version already exists.", entityId, entityType, tagId, newAggregateVersion);
            }
        } catch (SQLException e) {
            logger.error("SQLException during updateEntityTag for entityId {} entityType {} tagId {} aggregateVersion {}: {}", entityId, entityType, tagId, newAggregateVersion, e.getMessage(), e);
            throw e;
        } catch (Exception e) {
            logger.error("Exception during updateEntityTag for entityId {} entityType {} tagId {} aggregateVersion {}: {}", entityId, entityType, tagId, newAggregateVersion, e.getMessage(), e);
            throw e;
        }
    }

    @Override
    public void deleteEntityTag(Connection conn, Map<String, Object> event) throws SQLException, Exception {
        // Use UPDATE to implement Soft Delete (setting active = FALSE).
        // OCC/IDM is enforced by checking aggregate_version < newAggregateVersion.
        final String sql =
                """
                UPDATE entity_tag_t
                SET active = FALSE,
                    update_user = ?,
                    update_ts = ?,
                    aggregate_version = ?
                WHERE entity_id = ?
                  AND entity_type = ?
                  AND tag_id = ?
                  AND aggregate_version < ?
                """; // <<< CRITICAL: Added aggregate_version < ? to enforce monotonicity (OCC/IDM)

        // Note: Assuming SqlUtil.extractEventData(event) is the correct utility based on other methods.
        Map<String, Object> map = SqlUtil.extractEventData(event);

        String entityId = (String)map.get("entityId");
        String entityType = (String)map.get("entityType");
        String tagId = (String)map.get("tagId");
        // A delete event represents a state change, so it should have a new, incremented version.
        long newAggregateVersion = SqlUtil.getNewAggregateVersion(event);

        try (PreparedStatement statement = conn.prepareStatement(sql)) {
            // SET values (3 placeholders)
            // 1: update_user
            statement.setString(1, (String)event.get(Constants.USER));
            // 2: update_ts
            statement.setObject(2, OffsetDateTime.parse((String)event.get(CloudEventV1.TIME)));
            // 3: aggregate_version (the new version)
            statement.setLong(3, newAggregateVersion);

            // WHERE conditions (4 placeholders)
            // 4: entity_id
            statement.setObject(4, UUID.fromString(entityId));
            // 5: entity_type
            statement.setString(5, entityType);
            // 6: tag_id
            statement.setObject(6, UUID.fromString(tagId));
            // 7: aggregate_version < ? (new version for OCC/IDM check)
            statement.setLong(7, newAggregateVersion);

            int count = statement.executeUpdate();
            if (count == 0) {
                // If 0 rows updated, it means:
                // 1. The record was not found (already deleted or never existed).
                // 2. The OCC/IDM check failed (aggregate_version >= newAggregateVersion).
                // We IGNORE the failure and log a warning, as this is the desired idempotent/monotonic behavior.
                logger.warn("Soft delete skipped for entityId {} entityType {} tagId {} aggregateVersion {}. Record not found or a newer/same version already exists.", entityId, entityType, tagId, newAggregateVersion);
            }
        } catch (SQLException e) {
            logger.error("SQLException during deleteEntityTag for entityId {} entityType {} tagId {} aggregateVersion {}: {}", entityId, entityType, tagId, newAggregateVersion, e.getMessage(), e);
            throw e;
        } catch (Exception e) {
            logger.error("Exception during deleteEntityTag for entityId {} entityType {} tagId {} aggregateVersion {}: {}", entityId, entityType, tagId, newAggregateVersion, e.getMessage(), e);
            throw e;
        }
    }

    @Override
    public Result<String> getTag(int offset, int limit, String filtersJson, String globalFilter, String sortingJson, boolean active, String hostId) {
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

        String activeClause = SqlUtil.buildMultiTableActiveClause(active);
        String[] searchColumns = {"tag_name", "tag_desc"};
        String sqlBuilder = s + activeClause +
                dynamicFilter(Arrays.asList("host_id", "tag_id"), Arrays.asList(searchColumns), filters, null, parameters) +
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
        String sql = "SELECT tag_id, tag_name FROM tag_t WHERE (host_id = ? OR host_id IS NULL) AND active = TRUE";
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
                WHERE entity_type = ? AND active = TRUE
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
