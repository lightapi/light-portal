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


public class SchemaPersistenceImpl implements SchemaPersistence {

    private static final Logger logger = LoggerFactory.getLogger(SchemaPersistenceImpl.class);
    // Consider moving these to a shared constants class if they are truly general
    private static final String SQL_EXCEPTION = PortalDbProvider.SQL_EXCEPTION;
    private static final String GENERIC_EXCEPTION = PortalDbProvider.GENERIC_EXCEPTION;
    private static final String OBJECT_NOT_FOUND = PortalDbProvider.OBJECT_NOT_FOUND;

    private final NotificationService notificationService;

    public SchemaPersistenceImpl(NotificationService notificationService) {
        this.notificationService = notificationService;
    }

    @Override
    public void createSchema(Connection conn, Map<String, Object> event) throws SQLException, Exception {
        final String sql =
                """
                INSERT INTO schema_t(host_id, schema_id, schema_version, schema_type, spec_version, schema_source, schema_name,
                schema_desc, schema_body, schema_owner, schema_status, example, comment_status, update_user, update_ts, aggregate_version)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """;

        final String insertSchemaCategorySql = "INSERT INTO entity_category_t (entity_id, entity_type, category_id) VALUES (?, ?, ?)";
        final String insertSchemaTagSql = "INSERT INTO entity_tag_t (entity_id, entity_type, tag_id) VALUES (?, ?, ?)";

        Map<String, Object> map = (Map<String, Object>)event.get(PortalConstants.DATA);
        String schemaId = (String) map.get("schemaId"); // Get schemaId for return/logging/error
        long newAggregateVersion = SqlUtil.getNewAggregateVersion(event);
        List<String> categoryIds = (List<String>) map.get("categoryId"); // Get categoryIds from event data if present
        List<String> tagIds = (List<String>) map.get("tagIds"); // Get tagIds from event data if present

        try (PreparedStatement statement = conn.prepareStatement(sql)) {
            String hostId = (String)map.get("hostId");
            if (hostId != null && !hostId.isBlank()) {
                statement.setObject(1, UUID.fromString(hostId));
            } else {
                statement.setNull(1, Types.OTHER);
            }

            statement.setString(2, schemaId); // Required
            statement.setString(3, (String)map.get("schemaVersion")); // Required
            statement.setString(4, (String)map.get("schemaType")); // Required
            statement.setString(5, (String)map.get("specVersion")); // Required
            statement.setString(6, (String)map.get("schemaSource")); // Required
            statement.setString(7, (String)map.get("schemaName")); // Required

            String schemaDesc = (String)map.get("schemaDesc");
            if (schemaDesc != null && !schemaDesc.isBlank()) {
                statement.setString(8, schemaDesc);
            } else {
                statement.setNull(8, Types.VARCHAR);
            }
            statement.setString(9, (String)map.get("schemaBody")); // Required
            statement.setObject(10, UUID.fromString((String)map.get("schemaOwner"))); // Required
            statement.setString(11, (String)map.get("schemaStatus")); // Required

            String example = (String)map.get("example");
            if (example != null && !example.isBlank()) {
                statement.setString(12, example);
            } else {
                statement.setNull(12, Types.VARCHAR);
            }
            statement.setString(13, (String)map.get("commentStatus")); // Required
            statement.setString(14, (String)event.get(Constants.USER));
            statement.setObject(15, OffsetDateTime.parse((String)event.get(CloudEventV1.TIME)));
            statement.setLong(16, newAggregateVersion);

            int count = statement.executeUpdate();
            if (count == 0) {
                throw new SQLException(String.format("Failed during createSchema schemaId %s with aggregateVersion %d", schemaId, newAggregateVersion));
            }

            // Insert into entity_categories_t if categoryId is present
            if (categoryIds != null && !categoryIds.isEmpty()) {
                try (PreparedStatement insertCategoryStatement = conn.prepareStatement(insertSchemaCategorySql)) {
                    for (String categoryId : categoryIds) {
                        insertCategoryStatement.setString(1, schemaId);
                        insertCategoryStatement.setString(2, "schema"); // entity_type = "schema"
                        insertCategoryStatement.setObject(3, UUID.fromString(categoryId));
                        insertCategoryStatement.addBatch(); // Batch inserts for efficiency
                    }
                    insertCategoryStatement.executeBatch(); // Execute batch insert
                }
            }
            // Insert into entity_tag_t if tagIds are present
            if (tagIds != null && !tagIds.isEmpty()) {
                try (PreparedStatement insertTagStatement = conn.prepareStatement(insertSchemaTagSql)) {
                    for (String tagId : tagIds) {
                        insertTagStatement.setString(1, schemaId);
                        insertTagStatement.setString(2, "schema"); // entity_type = "schema"
                        insertTagStatement.setObject(3, UUID.fromString(tagId));
                        insertTagStatement.addBatch(); // Batch inserts for efficiency
                    }
                    insertTagStatement.executeBatch(); // Execute batch insert
                }
            }
        } catch (SQLException e) {
            logger.error("SQLException during createSchema for schemaId {} aggregateVersion {}: {}", schemaId, newAggregateVersion, e.getMessage(), e);
            throw e;
        } catch (Exception e) {
            logger.error("Exception during createSchema for schemaId {} aggregateVersion {}: {}", schemaId, newAggregateVersion, e.getMessage(), e);
            throw e;
        }
    }

    private boolean querySchemaExists(Connection conn, String schemaId) throws SQLException {
        final String sql =
                """
                SELECT COUNT(*) FROM schema_t WHERE schema_id = ?
                """;
        try (PreparedStatement pst = conn.prepareStatement(sql)) {
            pst.setObject(1, UUID.fromString(schemaId));
            try (ResultSet rs = pst.executeQuery()) {
                return rs.next() && rs.getInt(1) > 0;
            }
        }
    }

    @Override
    public void updateSchema(Connection conn, Map<String, Object> event) throws SQLException, Exception {
        final String sql =
                """
                UPDATE schema_t SET schema_version = ?, schema_type = ?, spec_version = ?,
                schema_source = ?, schema_name = ?, schema_desc = ?, schema_body = ?,
                schema_owner = ?, schema_status = ?, example = ?, comment_status = ?,
                update_user = ?, update_ts = ?, aggregate_version = ?
                WHERE schema_id = ? AND aggregate_version = ?
                """;
        final String deleteSchemaCategorySql = "DELETE FROM entity_category_t WHERE entity_id = ? AND entity_type = ?";
        final String insertSchemaCategorySql = "INSERT INTO entity_category_t (entity_id, entity_type, category_id) VALUES (?, ?, ?)";
        final String deleteSchemaTagSql = "DELETE FROM entity_tag_t WHERE entity_id = ? AND entity_type = ?";
        final String insertSchemaTagSql = "INSERT INTO entity_tag_t (entity_id, entity_type, tag_id) VALUES (?, ?, ?)";

        Map<String, Object> map = (Map<String, Object>)event.get(PortalConstants.DATA);
        String schemaId = (String) map.get("schemaId"); // For logging/exceptions
        long oldAggregateVersion = SqlUtil.getOldAggregateVersion(event);
        long newAggregateVersion = SqlUtil.getNewAggregateVersion(event);
        List<String> categoryIds = (List<String>) map.get("categoryId");
        List<String> tagIds = (List<String>) map.get("tagIds");

        try (PreparedStatement statement = conn.prepareStatement(sql)) {
            statement.setString(1, (String)map.get("schemaVersion"));
            statement.setString(2, (String)map.get("schemaType"));
            statement.setString(3, (String)map.get("specVersion"));
            statement.setString(4, (String)map.get("schemaSource"));
            statement.setString(5, (String)map.get("schemaName"));
            String schemaDesc = (String)map.get("schemaDesc");
            if (schemaDesc != null && !schemaDesc.isBlank()) {
                statement.setString(6, schemaDesc);
            } else {
                statement.setNull(6, Types.VARCHAR);
            }
            statement.setString(7, (String)map.get("schemaBody"));
            statement.setObject(8, UUID.fromString((String)map.get("schemaOwner")));
            statement.setString(9, (String)map.get("schemaStatus"));
            String example = (String)map.get("example");
            if (example != null && !example.isBlank()) {
                statement.setString(10, example);
            } else {
                statement.setNull(10, Types.VARCHAR);
            }
            statement.setString(11, (String)map.get("commentStatus"));
            statement.setString(12, (String)event.get(Constants.USER));
            statement.setObject(13, OffsetDateTime.parse((String)event.get(CloudEventV1.TIME)));
            statement.setString(14, schemaId);

            int count = statement.executeUpdate();
            if (count == 0) {
                if (querySchemaExists(conn, schemaId)) {
                    throw new ConcurrencyException("Optimistic concurrency conflict during updateSchema for schemaId " + schemaId + ". Expected version " + oldAggregateVersion + " but found a different version " + newAggregateVersion + ".");
                } else {
                    throw new SQLException("No record found during updateSchema for schemaId " + schemaId + ".");
                }
            }
            // --- Replace Category Associations ---
            // 1. Delete existing links for this schema
            try (PreparedStatement deleteCategoryStatement = conn.prepareStatement(deleteSchemaCategorySql)) {
                deleteCategoryStatement.setString(1, schemaId);
                deleteCategoryStatement.setString(2, "schema"); // entity_type = "schema"
                deleteCategoryStatement.executeUpdate(); // Execute delete (no need to check count for DELETE)
            }

            // 2. Insert new links if categoryIds are provided in the event
            if (categoryIds != null && !categoryIds.isEmpty()) {
                try (PreparedStatement insertCategoryStatement = conn.prepareStatement(insertSchemaCategorySql)) {
                    for (String categoryId : categoryIds) {
                        insertCategoryStatement.setString(1, schemaId);
                        insertCategoryStatement.setString(2, "schema"); // entity_type = "schema"
                        insertCategoryStatement.setObject(3, UUID.fromString(categoryId));
                        insertCategoryStatement.addBatch(); // Batch inserts for efficiency
                    }
                    insertCategoryStatement.executeBatch(); // Execute batch insert
                }
            }
            // --- End Replace Category Associations ---

            // --- Replace Tag Associations ---
            // 1. Delete existing links for this schema
            try (PreparedStatement deleteTagStatement = conn.prepareStatement(deleteSchemaTagSql)) {
                deleteTagStatement.setString(1, schemaId);
                deleteTagStatement.setString(2, "schema"); // entity_type = "schema"
                deleteTagStatement.executeUpdate();
            }

            // 2. Insert new links if tagIds are provided in the event
            if (tagIds != null && !tagIds.isEmpty()) {
                try (PreparedStatement insertTagStatement = conn.prepareStatement(insertSchemaTagSql)) {
                    for (String tagId : tagIds) {
                        insertTagStatement.setString(1, schemaId);
                        insertTagStatement.setString(2, "schema"); // entity_type = "schema"
                        insertTagStatement.setObject(3, UUID.fromString(tagId));
                        insertTagStatement.addBatch(); // Batch inserts for efficiency
                    }
                    insertTagStatement.executeBatch(); // Execute batch insert
                }
            }
        } catch (SQLException e) {
            logger.error("SQLException during updateSchema for schemaId {} (old: {}) -> (new: {}): {}", schemaId, oldAggregateVersion, newAggregateVersion, e.getMessage(), e);
            throw e;
        } catch (Exception e) {
            logger.error("Exception during updateSchema for schemaId {} (old: {}) -> (new: {}): {}", schemaId, oldAggregateVersion, newAggregateVersion, e.getMessage(), e);
            throw e;
        }
    }

    @Override
    public void deleteSchema(Connection conn, Map<String, Object> event) throws SQLException, Exception {
        final String sql = "DELETE FROM schema_t WHERE schema_id = ? AND aggregate_version = ?";
        Map<String, Object> map = (Map<String, Object>)event.get(PortalConstants.DATA);
        String schemaId = (String) map.get("schemaId");
        long oldAggregateVersion = SqlUtil.getOldAggregateVersion(event);

        try (PreparedStatement statement = conn.prepareStatement(sql)) {
            statement.setString(1, schemaId);

            int count = statement.executeUpdate();
            if (count == 0) {
                if (querySchemaExists(conn, schemaId)) {
                    throw new ConcurrencyException("Optimistic concurrency conflict during deleteSchema for schemaId " + schemaId + " aggregateVersion " + oldAggregateVersion + " but found a different version or already updated.");
                } else {
                    throw new SQLException("No record found during deleteSchema for schemaId " + schemaId + ". It might have been already deleted.");
                }
            }
        } catch (SQLException e) {
            logger.error("SQLException during deleteSchema for schemaId {} aggregateVersion {}: {}", schemaId, oldAggregateVersion, e.getMessage(), e);
            throw e;
        } catch (Exception e) {
            logger.error("Exception during deleteSchema for schemaId {} aggregateVersion {}: {}", schemaId, oldAggregateVersion, e.getMessage(), e);
            throw e;
        }
    }

    @Override
    public Result<String> getSchema(int offset, int limit, String filtersJson, String globalFilter, String sortingJson, String hostId) {
        Result<String> result;
        List<Map<String, Object>> filters = parseJsonList(filtersJson);
        List<Map<String, Object>> sorting = parseJsonList(sortingJson);

        String s =
                """
                SELECT COUNT(*) OVER () AS total,
                schema_id, host_id, schema_version, schema_type, spec_version, schema_source, schema_name, schema_desc, schema_body,
                schema_owner, schema_status, example, comment_status, update_user, update_ts, aggregate_version, active
                FROM schema_t
                WHERE host_id = ?
                """;

        List<Object> parameters = new ArrayList<>();
        parameters.add(UUID.fromString(hostId));

        String[] searchColumns = {"schema_name", "schema_desc", "schema_body", "example"};
        String sqlBuilder = s + dynamicFilter(Arrays.asList("host_id", "product_version_id"), Arrays.asList(searchColumns), filters, null, parameters) +
                globalFilter(globalFilter, searchColumns, parameters) +
                dynamicSorting("schema_name", sorting, null) +
                "\nLIMIT ? OFFSET ?";

        parameters.add(limit);
        parameters.add(offset);

        int total = 0;
        List<Map<String, Object>> schemas = new ArrayList<>();

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
                    map.put("schemaId", resultSet.getString("schema_id"));
                    map.put("hostId", resultSet.getObject("host_id", UUID.class));
                    map.put("schemaVersion", resultSet.getString("schema_version"));
                    map.put("schemaType", resultSet.getString("schema_type"));
                    map.put("specVersion", resultSet.getString("spec_version"));
                    map.put("schemaSource", resultSet.getString("schema_source"));
                    map.put("schemaName", resultSet.getString("schema_name"));
                    map.put("schemaDesc", resultSet.getString("schema_desc"));
                    // schemaBody is usually not returned in get list query for performance reasons
                    // map.put("schemaBody", resultSet.getString("schema_body"));
                    map.put("schemaOwner", resultSet.getObject("schema_owner", UUID.class));
                    map.put("schemaStatus", resultSet.getString("schema_status"));
                    map.put("example", resultSet.getString("example"));
                    map.put("commentStatus", resultSet.getString("comment_status"));
                    map.put("updateUser", resultSet.getString("update_user"));
                    map.put("updateTs", resultSet.getObject("update_ts") != null ? resultSet.getObject("update_ts", OffsetDateTime.class) : null);
                    map.put("aggregateVersion", resultSet.getLong("aggregate_version"));
                    map.put("active", resultSet.getBoolean("active"));
                    schemas.add(map);
                }
            }

            Map<String, Object> resultMap = new HashMap<>();
            resultMap.put("total", total);
            resultMap.put("schemas", schemas);
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
    public Result<String> getSchemaLabel(String hostId) {
        Result<String> result = null;
        StringBuilder sqlBuilder = new StringBuilder();
        sqlBuilder.append("SELECT schema_id, schema_name FROM schema_t WHERE 1=1 "); // Base query

        if (hostId != null && !hostId.isEmpty()) {
            sqlBuilder.append("AND (host_id = ? OR host_id IS NULL)"); // Tenant-specific OR Global
        } else {
            sqlBuilder.append("AND host_id IS NULL"); // Only Global if hostId is null/empty
        }

        String sql = sqlBuilder.toString();
        List<Map<String, Object>> labels = new ArrayList<>();
        try (Connection connection = ds.getConnection();
             PreparedStatement preparedStatement = connection.prepareStatement(sql)) {

            if (hostId != null && !hostId.isEmpty()) {
                preparedStatement.setObject(1, UUID.fromString(hostId)); // Set hostId parameter if provided
            }

            try (ResultSet resultSet = preparedStatement.executeQuery()) {
                while (resultSet.next()) {
                    Map<String, Object> map = new HashMap<>();
                    map.put("id", resultSet.getString("schema_id"));
                    map.put("label", resultSet.getString("schema_name"));
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
    public Result<String> getSchemaById(String schemaId) {
        Result<String> result = null;
        String sql =
                """
                SELECT schema_id, host_id, schema_version, schema_type, spec_version, schema_source,
                schema_name, schema_desc, schema_body, schema_owner, schema_status, example,
                comment_status, update_user, update_ts, aggregate_version
                FROM schema_t WHERE schema_id = ?
                """;
        Map<String, Object> map = null;
        try (Connection conn = ds.getConnection()) {
            // No setAutoCommit(false) for read-only query
            try (PreparedStatement statement = conn.prepareStatement(sql)) {
                statement.setString(1, schemaId);
                try (ResultSet resultSet = statement.executeQuery()) {
                    if (resultSet.next()) {
                        map = new HashMap<>(); // Create map only if found
                        map.put("schemaId", resultSet.getString("schema_id"));
                        map.put("hostId", resultSet.getObject("host_id", UUID.class));
                        map.put("schemaVersion", resultSet.getString("schema_version"));
                        map.put("schemaType", resultSet.getString("schema_type"));
                        map.put("specVersion", resultSet.getString("spec_version"));
                        map.put("schemaSource", resultSet.getString("schema_source"));
                        map.put("schemaName", resultSet.getString("schema_name"));
                        map.put("schemaDesc", resultSet.getString("schema_desc"));
                        map.put("schemaBody", resultSet.getString("schema_body"));
                        map.put("schemaOwner", resultSet.getObject("schema_owner", UUID.class));
                        map.put("schemaStatus", resultSet.getString("schema_status"));
                        map.put("example", resultSet.getString("example"));
                        map.put("commentStatus", resultSet.getString("comment_status"));
                        map.put("updateUser", resultSet.getString("update_user"));
                        map.put("updateTs", resultSet.getObject("update_ts") != null ? resultSet.getObject("update_ts", OffsetDateTime.class) : null);
                        map.put("aggregateVersion", resultSet.getLong("aggregate_version"));
                    }
                }
                // Check if map was populated (i.e., record found)
                if (map != null && !map.isEmpty()) {
                    result = Success.of(JsonMapper.toJson(map));
                } else {
                    // Record not found
                    result = Failure.of(new Status(OBJECT_NOT_FOUND, "schema", schemaId)); // Consistent with AccessControlPersistence
                }
            }
        } catch (SQLException e) {
            logger.error("SQLException getting schema by id {}:", schemaId, e);
            result = Failure.of(new Status(SQL_EXCEPTION, e.getMessage()));
        } catch (Exception e) {
            logger.error("Unexpected exception getting schema by id {}:", schemaId, e);
            result = Failure.of(new Status(GENERIC_EXCEPTION, e.getMessage()));
        }
        return result;
    }

    @Override
    public Result<String> getSchemaByCategoryId(String categoryId) {
        Result<String> result = null;
        String sqlBuilder =
                """
                SELECT schema_t.schema_id, schema_t.host_id, schema_t.schema_version, schema_t.schema_type,
                schema_t.spec_version, schema_t.schema_source, schema_t.schema_name, schema_t.schema_desc,
                schema_t.schema_body, schema_t.schema_owner, schema_t.schema_status, schema_t.example,
                schema_t.comment_status, schema_t.update_user, schema_t.update_ts, schema_t.aggregate_version
                FROM schema_t
                INNER JOIN entity_category_t ON schema_t.schema_id = entity_category_t.entity_id
                WHERE entity_type = 'schema' AND entity_category_t.category_id = ?
                """;

        List<Map<String, Object>> schemas = new ArrayList<>();
        try (Connection connection = ds.getConnection();
             PreparedStatement preparedStatement = connection.prepareStatement(sqlBuilder)) {

            preparedStatement.setObject(1, UUID.fromString(categoryId));

            try (ResultSet resultSet = preparedStatement.executeQuery()) {
                while (resultSet.next()) {
                    Map<String, Object> map = new HashMap<>();
                    map.put("schemaId", resultSet.getString("schema_id"));
                    map.put("hostId", resultSet.getObject("host_id", UUID.class));
                    map.put("schemaVersion", resultSet.getString("schema_version"));
                    map.put("schemaType", resultSet.getString("schema_type"));
                    map.put("specVersion", resultSet.getString("spec_version"));
                    map.put("schemaSource", resultSet.getString("schema_source"));
                    map.put("schemaName", resultSet.getString("schema_name"));
                    map.put("schemaDesc", resultSet.getString("schema_desc"));
                    map.put("schemaBody", resultSet.getString("schema_body"));
                    map.put("schemaOwner", resultSet.getObject("schema_owner", UUID.class));
                    map.put("schemaStatus", resultSet.getString("schema_status"));
                    map.put("example", resultSet.getString("example"));
                    map.put("commentStatus", resultSet.getString("comment_status"));
                    map.put("updateUser", resultSet.getString("update_user"));
                    map.put("updateTs", resultSet.getObject("update_ts") != null ? resultSet.getObject("update_ts", OffsetDateTime.class) : null);
                    map.put("aggregateVersion", resultSet.getLong("aggregate_version"));

                    schemas.add(map);
                }
            }

            Map<String, Object> resultMap = new HashMap<>();
            resultMap.put("schemas", schemas);
            result = Success.of(JsonMapper.toJson(resultMap));

        } catch (SQLException e) {
            logger.error("SQLException getting schemas by categoryId {}:", categoryId, e);
            result = Failure.of(new Status(SQL_EXCEPTION, e.getMessage()));
        } catch (Exception e) {
            logger.error("Exception getting schemas by categoryId {}:", categoryId, e);
            result = Failure.of(new Status(GENERIC_EXCEPTION, e.getMessage()));
        }
        return result;
    }

    @Override
    public Result<String> getSchemaByTagId(String tagId) {
        Result<String> result = null;
        String sqlBuilder =
                """
                SELECT schema_t.schema_id, schema_t.host_id, schema_t.schema_version, schema_t.schema_type,
                schema_t.spec_version, schema_t.schema_source, schema_t.schema_name, schema_t.schema_desc,
                schema_t.schema_body, schema_t.schema_owner, schema_t.schema_status, schema_t.example,
                schema_t.comment_status, schema_t.update_user, schema_t.update_ts, schema_t.aggregate_version
                FROM schema_t
                INNER JOIN entity_tag_t ON schema_t.schema_id = entity_tag_t.entity_id
                WHERE entity_type = 'schema' AND entity_tag_t.tag_id = ?
                """;

        List<Map<String, Object>> schemas = new ArrayList<>();
        try (Connection connection = ds.getConnection();
             PreparedStatement preparedStatement = connection.prepareStatement(sqlBuilder)) {

            preparedStatement.setObject(1, UUID.fromString(tagId));

            try (ResultSet resultSet = preparedStatement.executeQuery()) {
                while (resultSet.next()) {
                    Map<String, Object> map = new HashMap<>();
                    map.put("schemaId", resultSet.getString("schema_id"));
                    map.put("hostId", resultSet.getObject("host_id", UUID.class));
                    map.put("schemaVersion", resultSet.getString("schema_version"));
                    map.put("schemaType", resultSet.getString("schema_type"));
                    map.put("specVersion", resultSet.getString("spec_version"));
                    map.put("schemaSource", resultSet.getString("schema_source"));
                    map.put("schemaName", resultSet.getString("schema_name"));
                    map.put("schemaDesc", resultSet.getString("schema_desc"));
                    map.put("schemaBody", resultSet.getString("schema_body"));
                    map.put("schemaOwner", resultSet.getObject("schema_owner", UUID.class));
                    map.put("schemaStatus", resultSet.getString("schema_status"));
                    map.put("example", resultSet.getString("example"));
                    map.put("commentStatus", resultSet.getString("comment_status"));
                    map.put("updateUser", resultSet.getString("update_user"));
                    map.put("updateTs", resultSet.getObject("update_ts") != null ? resultSet.getObject("update_ts", OffsetDateTime.class) : null);
                    map.put("aggregateVersion", resultSet.getLong("aggregate_version"));

                    schemas.add(map);
                }
            }

            Map<String, Object> resultMap = new HashMap<>();
            resultMap.put("schemas", schemas);
            result = Success.of(JsonMapper.toJson(resultMap));

        } catch (SQLException e) {
            logger.error("SQLException getting schemas by tagId {}:", tagId, e);
            result = Failure.of(new Status(SQL_EXCEPTION, e.getMessage()));
        } catch (Exception e) {
            logger.error("Exception getting schemas by tagId {}:", tagId, e);
            result = Failure.of(new Status(GENERIC_EXCEPTION, e.getMessage()));
        }
        return result;
    }

}
