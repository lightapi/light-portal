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

import java.sql.*;
import java.time.OffsetDateTime;
import java.util.*;

import static com.networknt.db.provider.SqlDbStartupHook.ds;
import static java.sql.Types.NULL;
import static net.lightapi.portal.db.util.SqlUtil.addCondition;

public class RulePersistenceImpl implements RulePersistence {

    private static final Logger logger = LoggerFactory.getLogger(RulePersistenceImpl.class);
    private static final String SQL_EXCEPTION = PortalDbProvider.SQL_EXCEPTION;
    private static final String GENERIC_EXCEPTION = PortalDbProvider.GENERIC_EXCEPTION;
    private static final String OBJECT_NOT_FOUND = PortalDbProvider.OBJECT_NOT_FOUND;

    private final NotificationService notificationService;

    public RulePersistenceImpl(NotificationService notificationService) {
        this.notificationService = notificationService;
    }

    @Override
    public void createRule(Connection conn, Map<String, Object> event) throws SQLException, Exception {
        final String insertRule =
                """
                INSERT INTO rule_t (rule_id, host_id, rule_name, rule_version, rule_type,
                rule_group, rule_desc, rule_body, rule_owner, common,
                update_user, update_ts, aggregate_version)
                VALUES (?, ?, ?, ?, ?,   ?, ?, ?, ?, ?,  ?, ?, ?)
                """;
        final String insertCategorySql = "INSERT INTO entity_category_t (entity_id, entity_type, category_id) VALUES (?, ?, ?)";
        final String insertTagSql = "INSERT INTO entity_tag_t (entity_id, entity_type, tag_id) VALUES (?, ?, ?)";

        Map<String, Object> map = (Map<String, Object>)event.get(PortalConstants.DATA);
        String ruleId = (String)map.get("ruleId");
        List<String> categoryIds = (List<String>) map.get("categoryId");
        List<String> tagIds = (List<String>) map.get("tagIds");
        long newAggregateVersion = SqlUtil.getNewAggregateVersion(event);

        try {
            try (PreparedStatement statement = conn.prepareStatement(insertRule)) {
                statement.setString(1, ruleId);
                String hostId = (String)map.get("hostId");
                if (hostId != null && !hostId.isBlank()) {
                    statement.setObject(2, UUID.fromString(hostId));
                } else {
                    statement.setNull(2, Types.OTHER);
                }
                statement.setString(3, (String)map.get("ruleName"));
                statement.setString(4, (String)map.get("ruleVersion"));
                statement.setString(5, (String)map.get("ruleType"));
                String ruleGroup = (String)map.get("ruleGroup");
                if (ruleGroup != null && !ruleGroup.isEmpty())
                    statement.setString(6, ruleGroup);
                else
                    statement.setNull(6, NULL);

                String ruleDesc = (String)map.get("ruleDesc");
                if (ruleDesc != null && !ruleDesc.isEmpty())
                    statement.setString(7, ruleDesc);
                else
                    statement.setNull(7, NULL);
                statement.setString(8, (String)map.get("ruleBody"));
                statement.setString(9, (String)map.get("ruleOwner"));
                statement.setString(10, (String)map.get("common"));
                statement.setString(11, (String)event.get(Constants.USER));
                statement.setObject(12, OffsetDateTime.parse((String)event.get(CloudEventV1.TIME)));
                statement.setLong(13, newAggregateVersion);
                int count = statement.executeUpdate();
                if (count == 0) {
                    throw new SQLException(String.format("Failed during createRule ruleId %s with aggregateVersion %d", ruleId, newAggregateVersion));
                }
            }
            // Insert into entity_categories_t if categoryId is present
            if (categoryIds != null && !categoryIds.isEmpty()) {
                try (PreparedStatement insertCategoryStatement = conn.prepareStatement(insertCategorySql)) {
                    for (String categoryId : categoryIds) {
                        insertCategoryStatement.setString(1, ruleId);
                        insertCategoryStatement.setString(2, "rule"); // entity_type = "rule"
                        insertCategoryStatement.setObject(3, UUID.fromString(categoryId));
                        insertCategoryStatement.addBatch();
                    }
                    insertCategoryStatement.executeBatch();
                }
            }
            // Insert into entity_tag_t if tagIds are present
            if (tagIds != null && !tagIds.isEmpty()) {
                try (PreparedStatement insertTagStatement = conn.prepareStatement(insertTagSql)) {
                    for (String tagId : tagIds) {
                        insertTagStatement.setString(1, ruleId);
                        insertTagStatement.setString(2, "schema"); // entity_type = "rule"
                        insertTagStatement.setObject(3, UUID.fromString(tagId));
                        insertTagStatement.addBatch();
                    }
                    insertTagStatement.executeBatch();
                }
            }
        } catch (SQLException e) {
            logger.error("SQLException during createRule for ruleId {} aggregateVersion {}: {}", ruleId, newAggregateVersion, e.getMessage(), e);
            throw e;
        } catch (Exception e) {
            logger.error("Exception during createRule for ruleId {} aggregateVersion {}: {}", ruleId, newAggregateVersion, e.getMessage(), e);
            throw e;
        }
    }

    private boolean queryRuleExists(Connection conn, String ruleId) throws SQLException {
        final String sql =
                """
                SELECT COUNT(*) FROM rule_t WHERE rule_id = ?
                """;
        try (PreparedStatement pst = conn.prepareStatement(sql)) {
            pst.setObject(1, UUID.fromString(ruleId));
            try (ResultSet rs = pst.executeQuery()) {
                return rs.next() && rs.getInt(1) > 0;
            }
        }
    }

    @Override
    public void updateRule(Connection conn, Map<String, Object> event) throws SQLException, Exception {
        final String updateRule =
                """
                UPDATE rule_t SET rule_name = ?, rule_version = ?, rule_type = ?, rule_group = ?, rule_desc = ?,
                rule_body = ?, rule_owner = ?, common = ?, update_user = ?, update_ts = ?, aggregate_version = ?
                WHERE rule_id = ? AND aggregate_version = ?
                """;

        final String deleteCategorySql = "DELETE FROM entity_category_t WHERE entity_id = ? AND entity_type = ?";
        final String insertCategorySql = "INSERT INTO entity_category_t (entity_id, entity_type, category_id) VALUES (?, ?, ?)";
        final String deleteTagSql = "DELETE FROM entity_tag_t WHERE entity_id = ? AND entity_type = ?";
        final String insertTagSql = "INSERT INTO entity_tag_t (entity_id, entity_type, tag_id) VALUES (?, ?, ?)";

        Map<String, Object> map = (Map<String, Object>)event.get(PortalConstants.DATA);
        String ruleId = (String)map.get("ruleId");
        long oldAggregateVersion = SqlUtil.getOldAggregateVersion(event);
        long newAggregateVersion = SqlUtil.getNewAggregateVersion(event);
        List<String> categoryIds = (List<String>) map.get("categoryId");
        List<String> tagIds = (List<String>) map.get("tagIds");

        try (PreparedStatement statement = conn.prepareStatement(updateRule)) {
            String ruleName = (String)map.get("ruleName");
            if(ruleName != null && !ruleName.isEmpty()) {
                statement.setString(1, ruleName);
            } else {
                statement.setNull(1, NULL);
            }
            String ruleVersion = (String)map.get("ruleVersion");
            if (ruleVersion != null && !ruleVersion.isEmpty()) {
                statement.setString(2, ruleVersion);
            } else {
                statement.setNull(2, NULL);
            }
            String ruleType = (String)map.get("ruleType");
            if (ruleType != null && !ruleType.isEmpty()) {
                statement.setString(3, ruleType);
            } else {
                statement.setNull(3, NULL);
            }
            String ruleGroup = (String)map.get("ruleGroup");
            if (ruleGroup != null && !ruleGroup.isEmpty()) {
                statement.setString(4, ruleGroup);
            } else {
                statement.setNull(4, NULL);
            }
            String ruleDesc = (String)map.get("ruleDesc");
            if (ruleDesc != null && !ruleDesc.isEmpty()) {
                statement.setString(5, ruleDesc);
            } else {
                statement.setNull(5, NULL);
            }
            String ruleBody = (String)map.get("ruleBody");
            if(ruleBody != null && !ruleBody.isEmpty()) {
                statement.setString(6, ruleBody);
            } else {
                statement.setNull(6, NULL);
            }
            String ruleOwner = (String)map.get("ruleOwner");
            if(ruleOwner != null && !ruleOwner.isEmpty()) {
                statement.setString(7, ruleOwner);
            } else {
                statement.setNull(7, NULL);
            }
            String common = (String)map.get("common");
            if(common != null && !common.isEmpty()) {
                statement.setString(8, common);
            } else {
                statement.setNull(8, NULL);
            }
            statement.setString(9, (String)event.get(Constants.USER));
            statement.setObject(10, OffsetDateTime.parse((String)event.get(CloudEventV1.TIME)));
            statement.setLong(11, newAggregateVersion);
            statement.setString(12, ruleId);
            statement.setLong(13, oldAggregateVersion);

            int count = statement.executeUpdate();
            if (count == 0) {
                if (queryRuleExists(conn, ruleId)) {
                    throw new ConcurrencyException("Optimistic concurrency conflict during updateRule for ruleId " + ruleId + ". Expected version " + oldAggregateVersion + " but found a different version " + newAggregateVersion + ".");
                } else {
                    throw new SQLException("No record found during updateRule for schemaId " + ruleId + ".");
                }
            }
            // --- Replace Category Associations ---
            // 1. Delete existing links for this schema
            try (PreparedStatement deleteCategoryStatement = conn.prepareStatement(deleteCategorySql)) {
                deleteCategoryStatement.setString(1, ruleId);
                deleteCategoryStatement.setString(2, "rule");
                deleteCategoryStatement.executeUpdate();
            }

            // 2. Insert new links if categoryIds are provided in the event
            if (categoryIds != null && !categoryIds.isEmpty()) {
                try (PreparedStatement insertCategoryStatement = conn.prepareStatement(insertCategorySql)) {
                    for (String categoryId : categoryIds) {
                        insertCategoryStatement.setString(1, ruleId);
                        insertCategoryStatement.setString(2, "rule");
                        insertCategoryStatement.setObject(3, UUID.fromString(categoryId));
                        insertCategoryStatement.addBatch();
                    }
                    insertCategoryStatement.executeBatch();
                }
            }
            // --- End Replace Category Associations ---

            // --- Replace Tag Associations ---
            // 1. Delete existing links for this schema
            try (PreparedStatement deleteTagStatement = conn.prepareStatement(deleteTagSql)) {
                deleteTagStatement.setString(1, ruleId);
                deleteTagStatement.setString(2, "rule");
                deleteTagStatement.executeUpdate();
            }

            // 2. Insert new links if tagIds are provided in the event
            if (tagIds != null && !tagIds.isEmpty()) {
                try (PreparedStatement insertTagStatement = conn.prepareStatement(insertTagSql)) {
                    for (String tagId : tagIds) {
                        insertTagStatement.setString(1, ruleId);
                        insertTagStatement.setString(2, "rule");
                        insertTagStatement.setObject(3, UUID.fromString(tagId));
                        insertTagStatement.addBatch();
                    }
                    insertTagStatement.executeBatch();
                }
            }

        } catch (SQLException e) {
            logger.error("SQLException during updateRule for ruleId {} (old: {}) -> (new: {}): {}", ruleId, oldAggregateVersion, newAggregateVersion, e.getMessage(), e);
            throw e;
        } catch (Exception e) {
            logger.error("Exception during updateRule for ruleId {} (old: {}) -> (new: {}): {}", ruleId, oldAggregateVersion, newAggregateVersion, e.getMessage(), e);
            throw e;
        }
    }

    @Override
    public void deleteRule(Connection conn, Map<String, Object> event) throws SQLException, Exception {
        final String deleteRule = "DELETE from rule_t WHERE rule_id = ? AND aggregate_version = ?";
        Map<String, Object> map = (Map<String, Object>)event.get(PortalConstants.DATA);
        String ruleId = (String)map.get("ruleId");
        String hostId = (String)event.get(Constants.HOST);
        long oldAggregateVersion = SqlUtil.getOldAggregateVersion(event);

        try {
            try (PreparedStatement statement = conn.prepareStatement(deleteRule)) {
                statement.setString(1, ruleId);
                statement.setLong(2, oldAggregateVersion);

                int count = statement.executeUpdate();
                if (count == 0) {
                    if (queryRuleExists(conn, ruleId)) {
                        throw new ConcurrencyException("Optimistic concurrency conflict during deleteRule for ruleId " + ruleId + " aggregateVersion " + oldAggregateVersion + " but found a different version or already updated.");
                    } else {
                        throw new SQLException("No record found during deleteRule for ruleId " + ruleId + ". It might have been already deleted.");
                    }
                }
            }
        } catch (SQLException e) {
            logger.error("SQLException during deleteRule for ruleId {} aggregateVersion {}: {}", ruleId, oldAggregateVersion, e.getMessage(), e);
            throw e;
        } catch (Exception e) {
            logger.error("Exception during deleteRule for ruleId {} aggregateVersion {}: {}", ruleId, oldAggregateVersion, e.getMessage(), e);
            throw e;
        }
    }

    @Override
    public Result<List<Map<String, Object>>> queryRuleByGroup(String groupId) {
        Result<List<Map<String, Object>>> result;
        String sql =
                """
                SELECT rule_id, host_id, rule_name, rule_version, rule_type, rule_group,
                rule_desc, rule_body, rule_owner, update_user, update_ts, aggregate_version
                FROM rule_t WHERE rule_group = ?
                ORDER BY update_ts DESC
                """;
        try (final Connection conn = ds.getConnection()) {
            List<Map<String, Object>> list = new ArrayList<>();
            try (PreparedStatement statement = conn.prepareStatement(sql)) {
                statement.setString(1, groupId);
                try (ResultSet resultSet = statement.executeQuery()) {
                    while (resultSet.next()) {
                        Map<String, Object> map = new HashMap<>();
                        map.put("ruleId", resultSet.getString("rule_id"));
                        map.put("hostId", resultSet.getObject("host_id", UUID.class));
                        map.put("ruleName", resultSet.getString("rule_name"));
                        map.put("ruleVersion", resultSet.getString("rule_version"));
                        map.put("ruleType", resultSet.getString("rule_type"));
                        map.put("ruleGroup", resultSet.getBoolean("rule_group"));
                        map.put("ruleDesc", resultSet.getString("rule_desc"));
                        map.put("ruleBody", resultSet.getString("rule_body"));
                        map.put("ruleOwner", resultSet.getString("rule_owner"));
                        map.put("updateUser", resultSet.getString("update_user"));
                        map.put("updateTs", resultSet.getObject("update_ts") != null ? resultSet.getObject("update_ts", OffsetDateTime.class) : null);
                        map.put("aggregateVersion", resultSet.getLong("aggregate_version"));
                        list.add(map);
                    }
                }
            }
            if (list.isEmpty())
                result = Failure.of(new Status(OBJECT_NOT_FOUND, "rule with rule group ", groupId));
            else
                result = Success.of(list);
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
    public Result<String> queryRule(int offset, int limit, String hostId, String ruleId, String ruleName,
                                    String ruleVersion, String ruleType, String ruleGroup, String ruleDesc,
                                    String ruleBody, String ruleOwner) {
        Result<String> result;
        String s =
                """
                SELECT COUNT(*) OVER () AS total,
                host_id, rule_id, rule_name, rule_version, rule_type, rule_group, rule_desc, rule_body, rule_owner,
                update_user, update_ts, aggregate_version
                FROM rule_t
                WHERE 1 = 1
                """;
        StringBuilder sqlBuilder = new StringBuilder(s);

        List<Object> parameters = new ArrayList<>();
        StringBuilder whereClause = new StringBuilder();

        addCondition(whereClause, parameters, "host_id", hostId != null ? UUID.fromString(hostId) : null);
        addCondition(whereClause, parameters, "rule_id", ruleId);
        addCondition(whereClause, parameters, "rule_name", ruleName);
        addCondition(whereClause, parameters, "rule_version", ruleVersion);
        addCondition(whereClause, parameters, "rule_type", ruleType);
        addCondition(whereClause, parameters, "rule_group", ruleGroup);
        addCondition(whereClause, parameters, "rule_desc", ruleDesc);
        addCondition(whereClause, parameters, "rule_body", ruleBody);
        addCondition(whereClause, parameters, "rule_owner", ruleOwner);

        if (!whereClause.isEmpty()) {
            sqlBuilder.append("AND ").append(whereClause);
        }
        sqlBuilder.append(" ORDER BY rule_id\n" +
                "LIMIT ? OFFSET ?");

        parameters.add(limit);
        parameters.add(offset);
        String sql = sqlBuilder.toString();
        int total = 0;
        List<Map<String, Object>> rules = new ArrayList<>();

        try (final Connection conn = ds.getConnection(); PreparedStatement preparedStatement = conn.prepareStatement(sql)) {
            for (int i = 0; i < parameters.size(); i++) {
                preparedStatement.setObject(i + 1, parameters.get(i));
            }
            boolean isFirstRow = true;

            try (ResultSet resultSet = preparedStatement.executeQuery()) {
                while (resultSet.next()) {
                    Map<String, Object> map = new HashMap<>();
                    // only get the total once as it is the same for all rows.
                    if (isFirstRow) {
                        total = resultSet.getInt("total");
                        isFirstRow = false;
                    }
                    map.put("hostId", resultSet.getObject("host_id", UUID.class));
                    map.put("ruleId", resultSet.getString("rule_id"));
                    map.put("ruleName", resultSet.getString("rule_name"));
                    map.put("ruleVersion", resultSet.getString("rule_version"));
                    map.put("ruleType", resultSet.getString("rule_type"));
                    map.put("ruleGroup", resultSet.getBoolean("rule_group"));
                    map.put("ruleDesc", resultSet.getString("rule_desc"));
                    map.put("ruleBody", resultSet.getString("rule_body"));
                    map.put("ruleOwner", resultSet.getString("rule_owner"));
                    map.put("updateUser", resultSet.getString("update_user"));
                    map.put("updateTs", resultSet.getObject("update_ts") != null ? resultSet.getObject("update_ts", OffsetDateTime.class) : null);
                    map.put("aggregateVersion", resultSet.getLong("aggregate_version"));
                    rules.add(map);
                }
            }
            Map<String, Object> resultMap = new HashMap<>();
            resultMap.put("total", total);
            resultMap.put("rules", rules);
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
    public Result<Map<String, Object>> queryRuleById(String ruleId) {
        Result<Map<String, Object>> result;
        String sql =
                """
                SELECT rule_id, host_id, rule_name, rule_version,
                rule_type, rule_desc, rule_body, rule_owner,
                update_user, update_ts, aggregate_version
                FROM rule_t WHERE rule_id = ?
                """;
        try (final Connection conn = ds.getConnection()) {
            Map<String, Object> map = new HashMap<>();
            try (PreparedStatement statement = conn.prepareStatement(sql)) {
                statement.setString(1, ruleId);
                try (ResultSet resultSet = statement.executeQuery()) {
                    if (resultSet.next()) {
                        map.put("ruleId", resultSet.getString("rule_id"));
                        map.put("hostId", resultSet.getObject("host_id", UUID.class));
                        map.put("ruleName", resultSet.getString("rule_name"));
                        map.put("ruleVersion", resultSet.getString("rule_version"));
                        map.put("ruleType", resultSet.getString("rule_type"));
                        map.put("ruleDesc", resultSet.getString("rule_desc"));
                        map.put("ruleBody", resultSet.getString("rule_body"));
                        map.put("ruleOwner", resultSet.getString("rule_owner"));
                        map.put("updateUser", resultSet.getString("update_user"));
                        map.put("updateTs", resultSet.getObject("update_ts") != null ? resultSet.getObject("update_ts", OffsetDateTime.class) : null);
                        map.put("aggregateVersion", resultSet.getLong("aggregate_version"));
                    }
                }
            }
            if (map.isEmpty())
                result = Failure.of(new Status(OBJECT_NOT_FOUND, "rule with ruleId ", ruleId));
            else
                result = Success.of(map);
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
    public Result<String> queryRuleByType(String ruleType) {
        Result<String> result;
        String sql =
                """
                SELECT rule_id
                FROM rule_t
                WHERE rule_type = ?
                """;
        try (final Connection conn = ds.getConnection()) {
            List<Map<String, Object>> list = new ArrayList<>();
            try (PreparedStatement statement = conn.prepareStatement(sql)) {
                statement.setString(1, ruleType);

                try (ResultSet resultSet = statement.executeQuery()) {
                    while (resultSet.next()) {
                        Map<String, Object> map = new HashMap<>();
                        map.put("id", resultSet.getString("rule_id"));
                        map.put("label", resultSet.getString("rule_id"));
                        list.add(map);
                    }
                }
            }
            if (list.isEmpty())
                result = Failure.of(new Status(OBJECT_NOT_FOUND, "rule with rule type ", ruleType));
            else
                result = Success.of(JsonMapper.toJson(list));
        } catch (SQLException e) {
            logger.error("SQLException:", e);
            result = Failure.of(new Status(SQL_EXCEPTION, e.getMessage()));
        } catch (Exception e) {
            logger.error("Exception:", e);
            result = Failure.of(new Status(GENERIC_EXCEPTION, e.getMessage()));
        }
        return result;
    }

    // TODO this needs to be changed as the host_id can be null for global rules.
    @Override
    public Result<List<Map<String, Object>>> queryRuleByHostApiId(String hostId, String apiId, String apiVersion) {
        Result<List<Map<String, Object>>> result;
        String sql = """
            SELECT r.host_id, r.rule_id, r.rule_type, ae.endpoint, r.rule_body
            FROM rule_t r
            INNER JOIN api_endpoint_rule_t aer ON aer.rule_id = r.rule_id AND aer.host_id = r.host_id
            INNER JOIN api_endpoint_t ae ON ae.endpoint_id = aer.endpoint_id AND ae.host_id = aer.host_id
            INNER JOIN api_version_t av ON ae.api_version_id = av.api_version_id AND ae.host_id = av.host_id
            WHERE r.host_id = ?
              AND av.api_id = ?
              AND av.api_version = ?
            """;
        try (final Connection conn = ds.getConnection()) {
            List<Map<String, Object>> list = new ArrayList<>();
            try (PreparedStatement statement = conn.prepareStatement(sql)) {
                statement.setObject(1, UUID.fromString(hostId));
                statement.setString(2, apiId);
                statement.setString(3, apiVersion);
                try (ResultSet resultSet = statement.executeQuery()) {
                    while (resultSet.next()) {
                        Map<String, Object> map = new HashMap<>();
                        map.put("hostId", resultSet.getObject("host_id", UUID.class));
                        map.put("ruleId", resultSet.getString("rule_id"));
                        map.put("ruleType", resultSet.getString("rule_type"));
                        map.put("endpoint", resultSet.getString("endpoint"));
                        map.put("ruleBody", resultSet.getString("rule_body"));
                        list.add(map);
                    }
                }
            }
            if (list.isEmpty())
                result = Failure.of(new Status(OBJECT_NOT_FOUND, "rule with hostId " + hostId + " apiId " + apiId + " apiVersion " + apiVersion));
            else
                result = Success.of(list);
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
