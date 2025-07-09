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
        final String sql = "INSERT INTO category_t(host_id, category_id, entity_type, category_name, " +
                "category_desc, parent_category_id, sort_order, update_user, update_ts, aggregate_version) " +
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
        Map<String, Object> map = (Map<String, Object>)event.get(PortalConstants.DATA);
        String categoryId = (String)map.get("categoryId");
        long newAggregateVersion = SqlUtil.getNewAggregateVersion(event);

        try (PreparedStatement statement = conn.prepareStatement(sql)) {
            String hostId = (String)map.get("hostId");
            if (hostId != null && !hostId.isBlank()) {
                statement.setObject(1, UUID.fromString(hostId));
            } else {
                statement.setNull(1, Types.OTHER);
            }

            statement.setObject(2, UUID.fromString(categoryId));
            statement.setString(3, (String)map.get("entityType"));
            statement.setString(4, (String)map.get("categoryName"));

            String categoryDesc = (String)map.get("categoryDesc");
            if (categoryDesc != null && !categoryDesc.isBlank()) {
                statement.setString(5, categoryDesc);
            } else {
                statement.setNull(5, Types.VARCHAR);
            }
            String parentCategoryId = (String)map.get("parentCategoryId");
            if (parentCategoryId != null && !parentCategoryId.isBlank()) {
                statement.setObject(6, UUID.fromString(parentCategoryId));
            } else {
                statement.setNull(6, Types.OTHER);
            }
            Number sortOrder = (Number)map.get("sortOrder");
            if (sortOrder != null) {
                statement.setInt(7, sortOrder.intValue());
            } else {
                statement.setNull(7, Types.INTEGER);
            }
            statement.setString(8, (String)event.get(Constants.USER));
            statement.setObject(9, OffsetDateTime.parse((String)event.get(CloudEventV1.TIME)));
            statement.setLong(10, newAggregateVersion);

            int count = statement.executeUpdate();
            if (count == 0) {
                throw new SQLException("Failed to insert the category id " + categoryId + " with aggregate version " + newAggregateVersion + ".");
            }
        } catch (SQLException e) {
            logger.error("SQLException during createCategory for id {} aggregateVersion {}: {}", categoryId, newAggregateVersion, e.getMessage(), e);
            throw e;
        } catch (Exception e) { // Catch other potential runtime exceptions
            logger.error("Exception during createCategory for id {} aggregateVersion {}: {}", categoryId, newAggregateVersion, e.getMessage(), e);
            throw e;
        }
    }

    private boolean queryCategoryExists(Connection conn, String categoryId) throws SQLException {
        final String sql =
                """
                SELECT COUNT(*) FROM category_t WHERE category_id = ?
                """;
        try (PreparedStatement pst = conn.prepareStatement(sql)) {
            pst.setObject(1, UUID.fromString(categoryId));
            try (ResultSet rs = pst.executeQuery()) {
                return rs.next() && rs.getInt(1) > 0;
            }
        }
    }

    @Override
    public void updateCategory(Connection conn, Map<String, Object> event) throws SQLException, Exception {
        final String sql =
                """
                UPDATE category_t SET category_name = ?, category_desc = ?, parent_category_id = ?,
                sort_order = ?, update_user = ?, update_ts = ?, aggregate_version = ?
                WHERE category_id = ? AND aggregate_version = ?
                """;
        Map<String, Object> map = (Map<String, Object>)event.get(PortalConstants.DATA);
        String categoryId = (String)map.get("categoryId");
        long oldAggregateVersion = SqlUtil.getOldAggregateVersion(event);
        long newAggregateVersion = SqlUtil.getNewAggregateVersion(event);

        try (PreparedStatement statement = conn.prepareStatement(sql)) {
            statement.setString(1, (String)map.get("categoryName"));

            String categoryDesc = (String)map.get("categoryDesc");
            if (categoryDesc != null && !categoryDesc.isBlank()) {
                statement.setString(2, categoryDesc);
            } else {
                statement.setNull(2, Types.VARCHAR);
            }
            String parentCategoryId = (String)map.get("parentCategoryId");
            if (parentCategoryId != null && !parentCategoryId.isBlank()) {
                statement.setObject(3, UUID.fromString(parentCategoryId));
            } else {
                statement.setNull(3, Types.OTHER);
            }
            Number sortOrder = (Number)map.get("sortOrder");
            if (sortOrder != null) {
                statement.setInt(4, sortOrder.intValue());
            } else {
                statement.setNull(4, Types.INTEGER);
            }
            statement.setString(5, (String)event.get(Constants.USER));
            statement.setObject(6, OffsetDateTime.parse((String)event.get(CloudEventV1.TIME)));
            statement.setLong(7, newAggregateVersion);
            statement.setObject(8, UUID.fromString(categoryId));
            statement.setLong(9, oldAggregateVersion);

            int count = statement.executeUpdate();
            if (count == 0) {
                if (queryCategoryExists(conn, categoryId)) {
                    throw new ConcurrencyException("Optimistic concurrency conflict for category " + categoryId + ". Expected version " + oldAggregateVersion + " but found a different version " + newAggregateVersion + ".");
                } else {
                    throw new SQLException("No record found to update for category " + categoryId + ".");
                }
            }
        } catch (SQLException e) {
            logger.error("SQLException during updateCategory for id {} (old: {}) -> (new: {}): {}", categoryId, oldAggregateVersion, newAggregateVersion, e.getMessage(), e);
            throw e;
        } catch (Exception e) { // Catch other potential runtime exceptions
            logger.error("Exception during updateCategory for id {} (old: {}) -> (new: {}): {}", categoryId, oldAggregateVersion, newAggregateVersion, e.getMessage(), e);
            throw e;
        }
    }

    @Override
    public void deleteCategory(Connection conn, Map<String, Object> event) throws SQLException, Exception {
        final String sql = "DELETE FROM category_t WHERE category_id = ? AND aggregate_version = ?";
        Map<String, Object> map = (Map<String, Object>)event.get(PortalConstants.DATA);
        String categoryId = (String) map.get("categoryId");
        long oldAggregateVersion = SqlUtil.getOldAggregateVersion(event);
        try (PreparedStatement statement = conn.prepareStatement(sql)) {
            statement.setObject(1, UUID.fromString(categoryId));
            int count = statement.executeUpdate();
            if (count == 0) {
                if (queryCategoryExists(conn, categoryId)) {
                    throw new ConcurrencyException("Optimistic concurrency conflict during deleteCategory for category " + categoryId + " aggregateVersion " + oldAggregateVersion + " but found a different version or already updated.");
                } else {
                    throw new SQLException("No record found during deleteCategory for category " + categoryId + ". It might have been already deleted.");
                }
            }
        } catch (SQLException e) {
            logger.error("SQLException during deleteCategory for id {} aggregateVersion {}: {}", categoryId, oldAggregateVersion, e.getMessage(), e);
            throw e;
        } catch (Exception e) {
            logger.error("Exception during deleteCategory for id {} aggregateVersion {}: {}", categoryId, oldAggregateVersion, e.getMessage(), e);
            throw e;
        }
    }

    @Override
    public Result<String> getCategory(int offset, int limit, String hostId, String categoryId, String entityType,
                                      String categoryName, String categoryDesc, String parentCategoryId,
                                      String parentCategoryName, Integer sortOrder) {
        Result<String> result = null;
        String s =
                """
                SELECT COUNT(*) OVER () AS total,
                cat.category_id, cat.host_id, cat.entity_type, cat.category_name, cat.category_desc, cat.parent_category_id,
                cat.sort_order, cat.update_user, cat.update_ts, cat.aggregate_version,
                parent_cat.category_name AS parent_category_name
                FROM category_t cat
                LEFT JOIN category_t parent_cat ON cat.parent_category_id = parent_cat.category_id
                WHERE
                """;

        StringBuilder sqlBuilder = new StringBuilder(s);
        List<Object> parameters = new ArrayList<>();
        // Use a separate list to build condition strings to manage AND correctly
        List<String> conditions = new ArrayList<>();

        // --- Handle host_id condition first ---
        if (hostId != null && !hostId.isEmpty()) {
            conditions.add("(cat.host_id = ? OR cat.host_id IS NULL)");
            parameters.add(UUID.fromString(hostId));
        } else {
            conditions.add("cat.host_id IS NULL");
            // No parameter for IS NULL
        }

        // --- Add other conditions using the helper ---
        addConditionToList(conditions, parameters, "cat.category_id", categoryId != null ? UUID.fromString(categoryId) : null);
        addConditionToList(conditions, parameters, "cat.entity_type", entityType);
        addConditionToList(conditions, parameters, "cat.category_name", categoryName);
        addConditionToList(conditions, parameters, "cat.category_desc", categoryDesc); // Consider LIKE here if needed
        addConditionToList(conditions, parameters, "cat.parent_category_id", parentCategoryId != null ? UUID.fromString(parentCategoryId) : null);
        // parentCategoryName is derived, not filtered here
        addConditionToList(conditions, parameters, "cat.sort_order", sortOrder);

        // --- Join conditions with AND ---
        sqlBuilder.append(String.join(" AND ", conditions));

        // --- Add ORDER BY, LIMIT, OFFSET ---
        sqlBuilder.append(" ORDER BY cat.category_name\n" +
                "LIMIT ? OFFSET ?");

        parameters.add(limit);
        parameters.add(offset);

        String sql = sqlBuilder.toString();
        // if(logger.isTraceEnabled()) logger.trace("sql = {}", sql);
        int total = 0;
        List<Map<String, Object>> categories = new ArrayList<>();

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
