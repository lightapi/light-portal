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
import net.lightapi.portal.db.PortalDbProvider;
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

public class CategoryPersistenceImpl implements CategoryPersistence {
    private static final Logger logger = LoggerFactory.getLogger(CategoryPersistenceImpl.class);
    private static final String SQL_EXCEPTION = PortalDbProvider.SQL_EXCEPTION;
    private static final String GENERIC_EXCEPTION = PortalDbProvider.GENERIC_EXCEPTION;
    private static final String OBJECT_NOT_FOUND = PortalDbProvider.OBJECT_NOT_FOUND;

    private final NotificationService notificationService;

    public CategoryPersistenceImpl(NotificationService notificationService) {
        this.notificationService = notificationService;
    }

    @Override
    public void createCategory(Connection conn, Map<String, Object> event) throws SQLException, Exception {
        // Use UPSERT based on the Primary Key (category_id): INSERT ON CONFLICT DO UPDATE
        // This handles:
        // 1. First time insert (no conflict).
        // 2. Re-creation (conflict on category_id) -> UPDATE the existing soft-deleted row (setting active=TRUE and new version).

        final String sql =
                """
                INSERT INTO category_t(
                    host_id,
                    category_id,
                    entity_type,
                    category_name,
                    category_desc,
                    parent_category_id,
                    sort_order,
                    update_user,
                    update_ts,
                    aggregate_version,
                    active
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, TRUE)
                ON CONFLICT (category_id) DO UPDATE
                SET host_id = EXCLUDED.host_id,
                    entity_type = EXCLUDED.entity_type,
                    category_name = EXCLUDED.category_name,
                    category_desc = EXCLUDED.category_desc,
                    parent_category_id = EXCLUDED.parent_category_id,
                    sort_order = EXCLUDED.sort_order,
                    update_user = EXCLUDED.update_user,
                    update_ts = EXCLUDED.update_ts,
                    aggregate_version = EXCLUDED.aggregate_version,
                    active = TRUE
                -- OCC/IDM: Only update if the incoming event is newer
                WHERE category_t.aggregate_version < EXCLUDED.aggregate_version
                """;

        // Note: The original code uses a non-standard map retrieval: Map<String, Object> map = (Map<String, Object>)event.get(PortalConstants.DATA);
        // Assuming SqlUtil.extractEventData(event) is the correct utility based on other methods.
        Map<String, Object> map = SqlUtil.extractEventData(event);
        String hostId = (String)map.get("hostId");
        String categoryId = (String)map.get("categoryId");
        long newAggregateVersion = SqlUtil.getNewAggregateVersion(event);

        try (PreparedStatement statement = conn.prepareStatement(sql)) {
            // INSERT values (10 placeholders + active=TRUE in SQL, total 10 dynamic values)
            int i = 1;

            // 1: host_id
            if (hostId != null && !hostId.isBlank()) {
                statement.setObject(i++, UUID.fromString(hostId));
            } else {
                statement.setNull(i++, Types.OTHER);
            }

            // 2: category_id
            statement.setObject(i++, UUID.fromString(categoryId));
            // 3: entity_type
            statement.setString(i++, (String)map.get("entityType"));
            // 4: category_name
            statement.setString(i++, (String)map.get("categoryName"));

            // 5: category_desc
            String categoryDesc = (String)map.get("categoryDesc");
            if (categoryDesc != null && !categoryDesc.isBlank()) {
                statement.setString(i++, categoryDesc);
            } else {
                statement.setNull(i++, Types.VARCHAR);
            }

            // 6: parent_category_id
            String parentCategoryId = (String)map.get("parentCategoryId");
            if (parentCategoryId != null && !parentCategoryId.isBlank()) {
                statement.setObject(i++, UUID.fromString(parentCategoryId));
            } else {
                statement.setNull(i++, Types.OTHER);
            }

            // 7: sort_order
            Number sortOrder = (Number)map.get("sortOrder");
            if (sortOrder != null) {
                statement.setInt(i++, sortOrder.intValue());
            } else {
                statement.setNull(i++, Types.INTEGER);
            }

            // 8: update_user
            statement.setString(i++, (String)event.get(Constants.USER));
            // 9: update_ts
            statement.setObject(i++, OffsetDateTime.parse((String)event.get(CloudEventV1.TIME)));
            // 10: aggregate_version
            statement.setLong(i++, newAggregateVersion);

            int count = statement.executeUpdate();
            if (count == 0) {
                // count=0 means the ON CONFLICT clause was hit, BUT the WHERE clause (aggregate_version < EXCLUDED.aggregate_version) failed.
                // This is the desired idempotent/out-of-order protection behavior. Log and ignore.
                logger.warn("Creation/Reactivation skipped for categoryId {} aggregateVersion {}. A newer or same version already exists.", categoryId, newAggregateVersion);
            }
        } catch (SQLException e) {
            logger.error("SQLException during createCategory for id {} aggregateVersion {}: {}", categoryId, newAggregateVersion, e.getMessage(), e);
            throw e;
        } catch (Exception e) { // Catch other potential runtime exceptions
            logger.error("Exception during createCategory for id {} aggregateVersion {}: {}", categoryId, newAggregateVersion, e.getMessage(), e);
            throw e;
        }
    }

    @Override
    public void updateCategory(Connection conn, Map<String, Object> event) throws SQLException, Exception {
        // We attempt to update the record IF the incoming event's aggregate_version is greater than the current projection's version.
        // This enforces Idempotence (IDM) and Optimistic Concurrency Control (OCC) by ensuring version monotonicity.
        // We explicitly set active = TRUE as an UPDATE event implies the category should be active.
        final String sql =
                """
                UPDATE category_t
                SET category_name = ?,
                    category_desc = ?,
                    parent_category_id = ?,
                    sort_order = ?,
                    update_user = ?,
                    update_ts = ?,
                    aggregate_version = ?,
                    active = TRUE
                WHERE category_id = ?
                  AND aggregate_version < ?
                """; // <<< CRITICAL: Changed aggregate_version = ? to aggregate_version < ? to enforce monotonicity (OCC/IDM)

        // Note: Assuming SqlUtil.extractEventData(event) is the correct utility based on other methods.
        Map<String, Object> map = SqlUtil.extractEventData(event);
        String categoryId = (String)map.get("categoryId");
        long newAggregateVersion = SqlUtil.getNewAggregateVersion(event);

        try (PreparedStatement statement = conn.prepareStatement(sql)) {
            // SET values (7 dynamic values + active = TRUE in SQL)
            int i = 1;
            // 1: category_name
            statement.setString(i++, (String)map.get("categoryName"));

            // 2: category_desc
            String categoryDesc = (String)map.get("categoryDesc");
            if (categoryDesc != null && !categoryDesc.isBlank()) {
                statement.setString(i++, categoryDesc);
            } else {
                statement.setNull(i++, Types.VARCHAR);
            }
            // 3: parent_category_id
            String parentCategoryId = (String)map.get("parentCategoryId");
            if (parentCategoryId != null && !parentCategoryId.isBlank()) {
                statement.setObject(i++, UUID.fromString(parentCategoryId));
            } else {
                statement.setNull(i++, Types.OTHER);
            }
            // 4: sort_order
            Number sortOrder = (Number)map.get("sortOrder");
            if (sortOrder != null) {
                statement.setInt(i++, sortOrder.intValue());
            } else {
                statement.setNull(i++, Types.INTEGER);
            }
            // 5: update_user
            statement.setString(i++, (String)event.get(Constants.USER));
            // 6: update_ts
            statement.setObject(i++, OffsetDateTime.parse((String)event.get(CloudEventV1.TIME)));
            // 7: aggregate_version
            statement.setLong(i++, newAggregateVersion);

            // WHERE conditions (2 placeholders)
            // 8: category_id
            statement.setObject(i++, UUID.fromString(categoryId));
            // 9: aggregate_version < ? (new version for OCC/IDM check)
            statement.setLong(i++, newAggregateVersion);

            int count = statement.executeUpdate();
            if (count == 0) {
                // If 0 rows updated, it means the record was either not found
                // OR aggregate_version >= newAggregateVersion (OCC/IDM check failed).
                // We IGNORE the failure and log a warning, as this is the desired idempotent/monotonic behavior.
                logger.warn("Update skipped for categoryId {} aggregateVersion {}. Record not found or a newer/same version already exists.", categoryId, newAggregateVersion);
            }
        } catch (SQLException e) {
            logger.error("SQLException during updateCategory for id {} aggregateVersion {}: {}", categoryId, newAggregateVersion, e.getMessage(), e);
            throw e;
        } catch (Exception e) { // Catch other potential runtime exceptions
            logger.error("Exception during updateCategory for id {} aggregateVersion {}: {}", categoryId, newAggregateVersion, e.getMessage(), e);
            throw e;
        }
    }

    @Override
    public void deleteCategory(Connection conn, Map<String, Object> event) throws SQLException, Exception {
        // Use UPDATE to implement Soft Delete (setting active = FALSE).
        // OCC/IDM is enforced by checking aggregate_version < newAggregateVersion.
        final String sql =
                """
                UPDATE category_t
                SET active = FALSE,
                    update_user = ?,
                    update_ts = ?,
                    aggregate_version = ?
                WHERE category_id = ?
                  AND aggregate_version < ?
                """; // <<< CRITICAL: Added aggregate_version < ? to enforce monotonicity (OCC/IDM)

        // Note: The original code uses a non-standard map retrieval: Map<String, Object> map = (Map<String, Object>)event.get(PortalConstants.DATA);
        // Assuming SqlUtil.extractEventData(event) is the correct utility based on other methods.
        Map<String, Object> map = SqlUtil.extractEventData(event);
        String categoryId = (String) map.get("categoryId");
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
            // 4: category_id
            statement.setObject(4, UUID.fromString(categoryId));
            // 5: aggregate_version < ? (new version for OCC/IDM check)
            statement.setLong(5, newAggregateVersion);

            int count = statement.executeUpdate();
            if (count == 0) {
                // If 0 rows updated, it means:
                // 1. The record was not found (already deleted or never existed).
                // 2. The OCC/IDM check failed (aggregate_version >= newAggregateVersion).
                // We IGNORE the failure and log a warning, as this is the desired idempotent/monotonic behavior.
                logger.warn("Soft delete skipped for categoryId {} aggregateVersion {}. Record not found or a newer/same version already exists.", categoryId, newAggregateVersion);
            }
        } catch (SQLException e) {
            logger.error("SQLException during deleteCategory for id {} aggregateVersion {}: {}", categoryId, newAggregateVersion, e.getMessage(), e);
            throw e;
        } catch (Exception e) {
            logger.error("Exception during deleteCategory for id {} aggregateVersion {}: {}", categoryId, newAggregateVersion, e.getMessage(), e);
            throw e;
        }
    }

    @Override
    public void createEntityCategory(Connection conn, Map<String, Object> event) throws SQLException, Exception {
        // Use UPSERT based on the Primary Key (entity_id, entity_type, category_id): INSERT ON CONFLICT DO UPDATE
        // This handles:
        // 1. First time insert (no conflict).
        // 2. Re-creation (conflict on entity_id, entity_type, category_id) -> UPDATE the existing soft-deleted row (setting active=TRUE and new version).

        final String sql =
                """
                INSERT INTO entity_category_t (
                    entity_id,
                    entity_type,
                    category_id,
                    update_user,
                    update_ts,
                    aggregate_version,
                    active
                ) VALUES (?, ?, ?, ?, ?, ?, TRUE)
                ON CONFLICT (entity_id, entity_type, category_id) DO UPDATE
                SET update_user = EXCLUDED.update_user,
                    update_ts = EXCLUDED.update_ts,
                    aggregate_version = EXCLUDED.aggregate_version,
                    active = TRUE
                -- OCC/IDM: Only update if the incoming event is newer
                WHERE entity_category_t.aggregate_version < EXCLUDED.aggregate_version
                """;

        // Note: Assuming SqlUtil.extractEventData(event) is the correct utility based on other methods.
        Map<String, Object> map = SqlUtil.extractEventData(event);

        String entityId = (String)map.get("entityId");
        String entityType = (String)map.get("entityType");
        String categoryId = (String)map.get("categoryId");
        long newAggregateVersion = SqlUtil.getNewAggregateVersion(event);

        try (PreparedStatement statement = conn.prepareStatement(sql)) {
            // INSERT values (6 placeholders + active=TRUE in SQL, total 6 dynamic values)
            int i = 1;
            // 1: entity_id
            statement.setObject(i++, UUID.fromString(entityId));
            // 2: entity_type
            statement.setString(i++, entityType);
            // 3: category_id
            statement.setObject(i++, UUID.fromString(categoryId));

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
                logger.warn("Creation/Reactivation skipped for entityId {} entityType {} categoryId {} aggregateVersion {}. A newer or same version already exists.", entityId, entityType, categoryId, newAggregateVersion);
            }
        } catch (SQLException e) {
            logger.error("SQLException during createEntityCategory for entityId {} entityType {} categoryId {} aggregateVersion {}: {}", entityId, entityType, categoryId, newAggregateVersion, e.getMessage(), e);
            throw e;
        } catch (Exception e) {
            logger.error("Exception during createEntityCategory for entityId {} entityType {} categoryId {} aggregateVersion {}: {}", entityId, entityType, categoryId, newAggregateVersion, e.getMessage(), e);
            throw e;
        }
    }

    @Override
    public void updateEntityCategory(Connection conn, Map<String, Object> event) throws SQLException, Exception {
        // We attempt to update the record IF the incoming event's aggregate_version is greater than the current projection's version.
        // This enforces Idempotence (IDM) and Optimistic Concurrency Control (OCC) by ensuring version monotonicity.
        // We explicitly set active = TRUE as an UPDATE event implies the assignment should be active.
        final String sql =
                """
                UPDATE entity_category_t
                SET update_user = ?,
                    update_ts = ?,
                    aggregate_version = ?,
                    active = TRUE
                WHERE entity_id = ?
                  AND entity_type = ?
                  AND category_id = ?
                  AND aggregate_version < ?
                """; // <<< CRITICAL: Added aggregate_version < ? to enforce monotonicity (OCC/IDM)

        // Note: Assuming SqlUtil.extractEventData(event) is the correct utility based on other methods.
        Map<String, Object> map = SqlUtil.extractEventData(event);

        String entityId = (String)map.get("entityId");
        String entityType = (String)map.get("entityType");
        String categoryId = (String)map.get("categoryId");
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
            // 6: category_id
            statement.setObject(i++, UUID.fromString(categoryId));
            // 7: aggregate_version < ? (new version for OCC/IDM check)
            statement.setLong(i++, newAggregateVersion);

            int count = statement.executeUpdate();
            if (count == 0) {
                // If 0 rows updated, it means the record was either not found
                // OR aggregate_version >= newAggregateVersion (OCC/IDM check failed).
                // We IGNORE the failure and log a warning, as this is the desired idempotent/monotonic behavior.
                logger.warn("Update skipped for entityId {} entityType {} categoryId {} aggregateVersion {}. Record not found or a newer/same version already exists.", entityId, entityType, categoryId, newAggregateVersion);
            }
        } catch (SQLException e) {
            logger.error("SQLException during updateEntityCategory for entityId {} entityType {} categoryId {} aggregateVersion {}: {}", entityId, entityType, categoryId, newAggregateVersion, e.getMessage(), e);
            throw e;
        } catch (Exception e) {
            logger.error("Exception during updateEntityCategory for entityId {} entityType {} categoryId {} aggregateVersion {}: {}", entityId, entityType, categoryId, newAggregateVersion, e.getMessage(), e);
            throw e;
        }
    }

    @Override
    public void deleteEntityCategory(Connection conn, Map<String, Object> event) throws SQLException, Exception {
        // Use UPDATE to implement Soft Delete (setting active = FALSE).
        // OCC/IDM is enforced by checking aggregate_version < newAggregateVersion.
        final String sql =
                """
                UPDATE entity_category_t
                SET active = FALSE,
                    update_user = ?,
                    update_ts = ?,
                    aggregate_version = ?
                WHERE entity_id = ?
                  AND entity_type = ?
                  AND category_id = ?
                  AND aggregate_version < ?
                """; // <<< CRITICAL: Added aggregate_version < ? to enforce monotonicity (OCC/IDM)

        // Note: Assuming SqlUtil.extractEventData(event) is the correct utility based on other methods.
        Map<String, Object> map = SqlUtil.extractEventData(event);

        String entityId = (String)map.get("entityId");
        String entityType = (String)map.get("entityType");
        String categoryId = (String)map.get("categoryId");
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
            // 6: category_id
            statement.setObject(6, UUID.fromString(categoryId));
            // 7: aggregate_version < ? (new version for OCC/IDM check)
            statement.setLong(7, newAggregateVersion);

            int count = statement.executeUpdate();
            if (count == 0) {
                // If 0 rows updated, it means:
                // 1. The record was not found (already deleted or never existed).
                // 2. The OCC/IDM check failed (aggregate_version >= newAggregateVersion).
                // We IGNORE the failure and log a warning, as this is the desired idempotent/monotonic behavior.
                logger.warn("Soft delete skipped for entityId {} entityType {} categoryId {} aggregateVersion {}. Record not found or a newer/same version already exists.", entityId, entityType, categoryId, newAggregateVersion);
            }
        } catch (SQLException e) {
            logger.error("SQLException during deleteEntityCategory for entityId {} entityType {} categoryId {} aggregateVersion {}: {}", entityId, entityType, categoryId, newAggregateVersion, e.getMessage(), e);
            throw e;
        } catch (Exception e) {
            logger.error("Exception during deleteEntityCategory for entityId {} entityType {} categoryId {} aggregateVersion {}: {}", entityId, entityType, categoryId,  newAggregateVersion, e.getMessage(), e);
            throw e;
        }
    }

    @Override
    public Result<String> getCategory(int offset, int limit, String filtersJson, String globalFilter, String sortingJson, String hostId) {
        Result<String> result = null;
        final Map<String, String> columnMap = new HashMap<>(Map.of(
                "hostId", "cat.host_id",
                "categoryId", "cat.category_id",
                "entityType", "cat.entity_type",
                "categoryName", "cat.category_name",
                "categoryDesc", "cat.category_desc",
                "parentCategoryId", "cat.parent_category_id",
                "sortOrder", "cat.sort_order",
                "updateUser", "cat.update_user",
                "updateTs", "cat.update_ts",
                "active", "cat.active"
        ));
        columnMap.put("aggregateVersion", "cat.aggregate_version");
        columnMap.put("parentCategoryName", "parent_category_name");

        List<Map<String, Object>> filters = parseJsonList(filtersJson);
        List<Map<String, Object>> sorting = parseJsonList(sortingJson);

        String s =
            """
            SELECT COUNT(*) OVER () AS total,
            cat.category_id, cat.host_id, cat.entity_type,
            cat.category_name, cat.category_desc, cat.parent_category_id,
            cat.sort_order, cat.update_user, cat.update_ts, cat.active,
            cat.aggregate_version, parent_cat.category_name AS parent_category_name,
            FROM category_t cat
            LEFT JOIN category_t parent_cat ON cat.parent_category_id = parent_cat.category_id
            WHERE
            """;

        List<Object> parameters = new ArrayList<>();
        // --- Handle host_id condition first ---
        if (hostId != null && !hostId.isEmpty()) {
            s = s + "cat.host_id = ? OR cat.host_id IS NULL";
            parameters.add(UUID.fromString(hostId));
        } else {
            s = s + "cat.host_id IS NULL";
        }

        String[] searchColumns = {"cat.category_name", "cat.category_desc", "e.endpoint_desc"};
        String sqlBuilder = s + dynamicFilter(Arrays.asList("cat.host_id", "cat.category_id", "cat.parent_category_id"), Arrays.asList(searchColumns), filters, columnMap, parameters) +
                globalFilter(globalFilter, searchColumns, parameters) +
                dynamicSorting("cat.category_name", sorting, columnMap) +
                "\nLIMIT ? OFFSET ?";

        parameters.add(limit);
        parameters.add(offset);

        int total = 0;
        List<Map<String, Object>> categories = new ArrayList<>();

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
                    map.put("categoryId", resultSet.getObject("category_id", UUID.class));
                    map.put("hostId", resultSet.getObject("host_id", UUID.class));
                    map.put("entityType", resultSet.getString("entity_type"));
                    map.put("categoryName", resultSet.getString("category_name"));
                    map.put("categoryDesc", resultSet.getString("category_desc"));
                    map.put("parentCategoryId", resultSet.getObject("parent_category_id", UUID.class));
                    map.put("parentCategoryName", resultSet.getString("parent_category_name")); // Get parent category name from join
                    map.put("sortOrder", resultSet.getInt("sort_order"));
                    map.put("updateUser", resultSet.getString("update_user"));
                    map.put("updateTs", resultSet.getObject("update_ts") != null ? resultSet.getObject("update_ts", OffsetDateTime.class) : null);
                    map.put("aggregateVersion", resultSet.getLong("aggregate_version"));
                    map.put("active", resultSet.getBoolean("active"));
                    categories.add(map);
                }
            }

            Map<String, Object> resultMap = new HashMap<>();
            resultMap.put("total", total);
            resultMap.put("categories", categories);
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

    /**
     * Helper method to add a condition string and parameter to lists
     * if the value is not null.
     * @param conditions List of condition strings (e.g., "col = ?")
     * @param parameters List of parameters for PreparedStatement
     * @param columnName Database column name
     * @param columnValue Column value from method parameters
     */
    private void addConditionToList(List<String> conditions, List<Object> parameters, String columnName, Object columnValue) {
        if (columnValue != null) {
            // Treat empty strings as no filter for VARCHAR columns if desired
            if (columnValue instanceof String && ((String) columnValue).isEmpty()) {
                return; // Skip empty string filters
            }
            conditions.add(columnName + " LIKE ?");
            parameters.add("%" + columnValue + "%");
        }
    }

    @Override
    public Result<String> getCategoryLabel(String hostId) {
        Result<String> result = null;
        String sql = "SELECT category_id, category_name FROM category_t WHERE host_id = ? OR host_id IS NULL";
        List<Map<String, Object>> labels = new ArrayList<>();
        try (Connection connection = ds.getConnection();
             PreparedStatement preparedStatement = connection.prepareStatement(sql)) {
            preparedStatement.setObject(1, UUID.fromString(hostId));
            try (ResultSet resultSet = preparedStatement.executeQuery()) {
                while (resultSet.next()) {
                    Map<String, Object> map = new HashMap<>();
                    map.put("id", resultSet.getString("category_id"));
                    map.put("label", resultSet.getString("category_name"));
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
    public Result<String> getCategoryById(String categoryId) {
        Result<String> result = null;
        String sql = "SELECT category_id, host_id, entity_type, category_name, category_desc, parent_category_id, " +
                "sort_order, update_user, update_ts, aggregate_version FROM category_t WHERE category_id = ?";
        Map<String, Object> map = null;
        try (Connection connection = ds.getConnection();
             PreparedStatement preparedStatement = connection.prepareStatement(sql)) {
            preparedStatement.setObject(1, UUID.fromString(categoryId));
            try (ResultSet resultSet = preparedStatement.executeQuery()) {
                if (resultSet.next()) {
                    map = new HashMap<>();
                    map.put("categoryId", resultSet.getObject("category_id", UUID.class));
                    map.put("hostId", resultSet.getObject("host_id", UUID.class));
                    map.put("entityType", resultSet.getString("entity_type"));
                    map.put("categoryName", resultSet.getString("category_name"));
                    map.put("categoryDesc", resultSet.getString("category_desc"));
                    map.put("parentCategoryId", resultSet.getObject("parent_category_id", UUID.class));
                    map.put("sortOrder", resultSet.getInt("sort_order"));
                    map.put("updateUser", resultSet.getString("update_user"));
                    map.put("updateTs", resultSet.getObject("update_ts") != null ? resultSet.getObject("update_ts", OffsetDateTime.class) : null);
                    map.put("aggregateVersion", resultSet.getLong("aggregate_version"));
                }
            }
            if (map != null && !map.isEmpty()) {
                result = Success.of(JsonMapper.toJson(map));
            } else {
                result = Failure.of(new Status(OBJECT_NOT_FOUND, "category", categoryId));
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
    public Result<String> getCategoryByName(String hostId, String categoryName) {
        Result<String> result = null;
        String s =
                """
                SELECT category_id, host_id, entity_type, category_name, category_desc, parent_category_id,
                sort_order, update_user, update_ts, aggregate_version
                FROM category_t
                WHERE category_name = ?
                """;
        StringBuilder sqlBuilder = new StringBuilder(s);

        if (hostId != null && !hostId.isEmpty()) {
            sqlBuilder.append("AND (host_id = ? OR host_id IS NULL)"); // Tenant-specific OR Global
        } else {
            sqlBuilder.append("AND host_id IS NULL"); // Only Global if hostId is null/empty
        }

        String sql = sqlBuilder.toString();
        if(logger.isTraceEnabled()) logger.trace("sql = {}", sql);
        Map<String, Object> map = null;
        try (Connection connection = ds.getConnection();
             PreparedStatement preparedStatement = connection.prepareStatement(sql)) {

            int parameterIndex = 1;
            preparedStatement.setString(parameterIndex++, categoryName); // 1. categoryName

            if (hostId != null && !hostId.isEmpty()) {
                preparedStatement.setObject(parameterIndex++, UUID.fromString(hostId)); // 2. hostId (for tenant-specific OR global)
            }

            try (ResultSet resultSet = preparedStatement.executeQuery()) {
                if (resultSet.next()) {
                    map = new HashMap<>();
                    map.put("categoryId", resultSet.getObject("category_id", UUID.class));
                    map.put("hostId", resultSet.getObject("host_id", UUID.class));
                    map.put("entityType", resultSet.getString("entity_type"));
                    map.put("categoryName", resultSet.getString("category_name"));
                    map.put("categoryDesc", resultSet.getString("category_desc"));
                    map.put("parentCategoryId", resultSet.getObject("parent_category_id", UUID.class));
                    map.put("sortOrder", resultSet.getInt("sort_order"));
                    map.put("updateUser", resultSet.getString("update_user"));
                    map.put("updateTs", resultSet.getObject("update_ts") != null ? resultSet.getObject("update_ts", OffsetDateTime.class) : null);
                    map.put("aggregateVersion", resultSet.getLong("aggregate_version"));
                }
            }
            if (map != null && !map.isEmpty()) {
                result = Success.of(JsonMapper.toJson(map));
            } else {
                result = Failure.of(new Status(OBJECT_NOT_FOUND, "category by name", categoryName + " and hostId " + hostId));
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
    public Result<String> getCategoryByType(String hostId, String entityType) {
        Result<String> result = null;
        String s =
                """
                SELECT category_id, host_id, entity_type, category_name, category_desc, parent_category_id,
                sort_order, update_user, update_ts, aggregate_version
                FROM category_t
                WHERE entity_type = ?
                """;
        StringBuilder sqlBuilder = new StringBuilder(s);

        if (hostId != null && !hostId.isEmpty()) {
            sqlBuilder.append("AND (host_id = ? OR host_id IS NULL)"); // Tenant-specific OR Global
        } else {
            sqlBuilder.append("AND host_id IS NULL"); // Only Global if hostId is null/empty
        }

        String sql = sqlBuilder.toString();
        List<Map<String, Object>> categories = new ArrayList<>();
        try (Connection connection = ds.getConnection();
             PreparedStatement preparedStatement = connection.prepareStatement(sql)) {

            int parameterIndex = 1;
            preparedStatement.setString(parameterIndex++, entityType); // 1. entityType
            if (hostId != null && !hostId.isEmpty()) {
                preparedStatement.setObject(parameterIndex++, UUID.fromString(hostId)); // 2. hostId (for tenant-specific OR global)
            }

            try (ResultSet resultSet = preparedStatement.executeQuery()) {
                while (resultSet.next()) {
                    Map<String, Object> map = new HashMap<>();
                    map.put("categoryId", resultSet.getObject("category_id", UUID.class));
                    map.put("hostId", resultSet.getObject("host_id", UUID.class));
                    map.put("entityType", resultSet.getString("entity_type"));
                    map.put("categoryName", resultSet.getString("category_name"));
                    map.put("categoryDesc", resultSet.getString("category_desc"));
                    map.put("parentCategoryId", resultSet.getObject("parent_category_id", UUID.class));
                    map.put("sortOrder", resultSet.getInt("sort_order"));
                    map.put("updateUser", resultSet.getString("update_user"));
                    map.put("updateTs", resultSet.getObject("update_ts") != null ? resultSet.getObject("update_ts", OffsetDateTime.class) : null);
                    map.put("aggregateVersion", resultSet.getLong("aggregate_version"));

                    categories.add(map);
                }
            }

            Map<String, Object> resultMap = new HashMap<>();
            resultMap.put("categories", categories);
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
    public Result<String> getCategoryTree(String hostId, String entityType) {
        Result<String> result = null;
        String s =
                """
                SELECT cat.category_id, cat.host_id, cat.entity_type, cat.category_name, cat.category_desc, cat.parent_category_id,
                cat.sort_order, cat.update_user, cat.update_ts, cat.aggregate_version
                FROM category_t cat
                WHERE cat.entity_type = ?
                """;
        StringBuilder sqlBuilder = new StringBuilder(s);

        if (hostId != null && !hostId.isEmpty()) {
            sqlBuilder.append("AND (cat.host_id = ? OR cat.host_id IS NULL)"); // Tenant-specific OR Global
        } else {
            sqlBuilder.append("AND cat.host_id IS NULL"); // Only Global if hostId is null/empty
        }

        sqlBuilder.append(" ORDER BY cat.sort_order, cat.category_name"); // Order for tree consistency

        String sql = sqlBuilder.toString();
        List<Map<String, Object>> categoryList = new ArrayList<>(); // Flat list initially

        try (Connection connection = ds.getConnection();
             PreparedStatement preparedStatement = connection.prepareStatement(sql)) {

            int parameterIndex = 1;
            preparedStatement.setString(parameterIndex++, entityType); // 1. entityType
            if (hostId != null && !hostId.isEmpty()) {
                preparedStatement.setObject(parameterIndex++, UUID.fromString(hostId)); // 2. hostId (for tenant-specific OR global)
            }

            try (ResultSet resultSet = preparedStatement.executeQuery()) {
                while (resultSet.next()) {
                    Map<String, Object> map = new HashMap<>();
                    map.put("categoryId", resultSet.getObject("category_id", UUID.class));
                    map.put("hostId", resultSet.getObject("host_id", UUID.class));
                    map.put("entityType", resultSet.getString("entity_type"));
                    map.put("categoryName", resultSet.getString("category_name"));
                    map.put("categoryDesc", resultSet.getString("category_desc"));
                    map.put("parentCategoryId", resultSet.getObject("parent_category_id", UUID.class));
                    map.put("sortOrder", resultSet.getInt("sort_order"));
                    map.put("updateUser", resultSet.getString("update_user"));
                    map.put("updateTs", resultSet.getObject("update_ts") != null ? resultSet.getObject("update_ts", OffsetDateTime.class) : null);
                    map.put("aggregateVersion", resultSet.getLong("aggregate_version"));
                    map.put("children", new ArrayList<>()); // Initialize children list for tree structure

                    categoryList.add(map);
                }
            }

            // Build the category tree structure
            List<Map<String, Object>> categoryTree = buildCategoryTree(categoryList);

            Map<String, Object> resultMap = new HashMap<>();
            resultMap.put("categories", categoryTree);
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

    /**
     * Helper method to build the category tree from a flat list of categories.
     *
     * @param categoryList Flat list of category maps.
     * @return List of root category maps representing the tree structure.
     */
    private List<Map<String, Object>> buildCategoryTree(List<Map<String, Object>> categoryList) {
        Map<String, Map<String, Object>> categoryLookup = new HashMap<>(); // For quick lookup by categoryId
        List<Map<String, Object>> rootCategories = new ArrayList<>();

        // 1. Populate the lookup map for efficient access by categoryId
        for (Map<String, Object> category : categoryList) {
            String categoryId = (String) category.get("categoryId");
            if (categoryId != null) { // Ensure categoryId is not null for lookup
                categoryLookup.put(categoryId, category);
            }
        }

        // 2. Iterate again to build the tree structure
        for (Map<String, Object> category : categoryList) {
            String parentCategoryId = (String) category.get("parentCategoryId");
            if (parentCategoryId != null && !parentCategoryId.isEmpty()) {
                // If it has a parent, add it as a child to the parent category
                Map<String, Object> parentCategory = categoryLookup.get(parentCategoryId);
                if (parentCategory != null) { // Check if parentCategory exists in the lookup
                    ((List<Map<String, Object>>) parentCategory.get("children")).add(category);
                } else {
                    logger.warn("Parent category with ID '{}' not found for categoryId: '{}'. Adding to root.", parentCategoryId, category.get("categoryId"));
                    // Handle missing parent category by adding the current category to the root
                    rootCategories.add(category);
                }
            } else {
                // If no parent, it's a root category
                rootCategories.add(category);
            }
        }
        return rootCategories;
    }

}
