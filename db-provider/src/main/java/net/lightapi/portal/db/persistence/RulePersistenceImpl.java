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
import static net.lightapi.portal.db.util.SqlUtil.*;

public class RulePersistenceImpl implements RulePersistence {

    private static final Logger logger = LoggerFactory.getLogger(RulePersistenceImpl.class);
    private static final String SQL_EXCEPTION = PortalDbProvider.SQL_EXCEPTION;
    private static final String GENERIC_EXCEPTION = PortalDbProvider.GENERIC_EXCEPTION;
    private static final String OBJECT_NOT_FOUND = PortalDbProvider.OBJECT_NOT_FOUND;

    private final NotificationService notificationService;

    public RulePersistenceImpl(NotificationService notificationService) {
        this.notificationService = notificationService;
    }

    /*
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
    */

    @Override
    public void createRule(Connection conn, Map<String, Object> event) throws SQLException, Exception {
        // Use UPSERT based on the Primary Key (rule_id): INSERT ON CONFLICT DO UPDATE
        // This handles:
        // 1. First time insert (no conflict).
        // 2. Re-creation (conflict on rule_id) -> UPDATE the existing soft-deleted row (setting active=TRUE and new version).

        final String sql =
                """
                INSERT INTO rule_t (
                    rule_id,
                    host_id,
                    rule_name,
                    rule_version,
                    rule_type,
                    rule_group,
                    rule_desc,
                    rule_body,
                    rule_owner,
                    common,
                    update_user,
                    update_ts,
                    aggregate_version,
                    active
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, TRUE)
                ON CONFLICT (rule_id) DO UPDATE
                SET host_id = EXCLUDED.host_id,
                    rule_name = EXCLUDED.rule_name,
                    rule_version = EXCLUDED.rule_version,
                    rule_type = EXCLUDED.rule_type,
                    rule_group = EXCLUDED.rule_group,
                    rule_desc = EXCLUDED.rule_desc,
                    rule_body = EXCLUDED.rule_body,
                    rule_owner = EXCLUDED.rule_owner,
                    common = EXCLUDED.common,
                    update_user = EXCLUDED.update_user,
                    update_ts = EXCLUDED.update_ts,
                    aggregate_version = EXCLUDED.aggregate_version,
                    active = TRUE
                -- OCC/IDM: Only update if the incoming event is newer
                WHERE rule_t.aggregate_version < EXCLUDED.aggregate_version
                AND rule_t.active = FALSE
                """;

        // Note: Assuming SqlUtil.extractEventData(event) is the correct utility based on other methods.
        Map<String, Object> map = SqlUtil.extractEventData(event);

        String ruleId = (String)map.get("ruleId");
        long newAggregateVersion = SqlUtil.getNewAggregateVersion(event);

        try (PreparedStatement statement = conn.prepareStatement(sql)) {
            // INSERT values (13 placeholders + active=TRUE in SQL, total 13 dynamic values)
            int i = 1;
            // 1: rule_id
            statement.setString(i++, ruleId);

            // 2: host_id
            String hostId = (String)map.get("hostId");
            if (hostId != null && !hostId.isBlank()) {
                statement.setObject(i++, UUID.fromString(hostId));
            } else {
                statement.setNull(i++, Types.OTHER);
            }

            // 3: rule_name
            statement.setString(i++, (String)map.get("ruleName"));
            // 4: rule_version
            statement.setString(i++, (String)map.get("ruleVersion"));
            // 5: rule_type
            statement.setString(i++, (String)map.get("ruleType"));

            // 6: rule_group
            if (map.containsKey("ruleGroup")) {
                statement.setString(i++, (String)map.get("ruleGroup"));
            } else {
                statement.setNull(i++, Types.VARCHAR);
            }

            // 7: rule_desc
            if (map.containsKey("ruleDesc")) {
                statement.setString(i++, (String)map.get("ruleDesc"));
            } else {
                statement.setNull(i++, Types.VARCHAR);
            }

            // 8: rule_body
            statement.setString(i++, (String)map.get("ruleBody"));
            // 9: rule_owner
            statement.setString(i++, (String)map.get("ruleOwner"));

            // 10: common
            String common = (String)map.get("common");
            if (common != null && !common.isBlank()) {
                statement.setString(i++, common);
            } else {
                statement.setString(i++, "N"); // Default to 'N' based on table DDL
            }

            // 11: update_user
            statement.setString(i++, (String)event.get(Constants.USER));
            // 12: update_ts
            statement.setObject(i++, OffsetDateTime.parse((String)event.get(CloudEventV1.TIME)));
            // 13: aggregate_version
            statement.setLong(i++, newAggregateVersion);

            int count = statement.executeUpdate();
            if (count == 0) {
                // count=0 means the ON CONFLICT clause was hit, BUT the WHERE clause (aggregate_version < EXCLUDED.aggregate_version) failed.
                // This is the desired idempotent/out-of-order protection behavior. Log and ignore.
                logger.warn("Creation/Reactivation skipped for ruleId {} aggregateVersion {}. A newer or same version already exists.", ruleId, newAggregateVersion);
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

    /*
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
    */

    @Override
    public void updateRule(Connection conn, Map<String, Object> event) throws SQLException, Exception {
        // We attempt to update the record IF the incoming event's aggregate_version is greater than the current projection's version.
        // This enforces Idempotence (IDM) and Optimistic Concurrency Control (OCC) by ensuring version monotonicity.
        // We explicitly set active = TRUE as an UPDATE event implies the rule should be active.
        final String sql =
                """
                UPDATE rule_t
                SET host_id = ?,
                    rule_name = ?,
                    rule_version = ?,
                    rule_type = ?,
                    rule_group = ?,
                    rule_desc = ?,
                    rule_body = ?,
                    rule_owner = ?,
                    common = ?,
                    update_user = ?,
                    update_ts = ?,
                    aggregate_version = ?,
                    active = TRUE
                WHERE rule_id = ?
                  AND aggregate_version < ?
                """; // <<< CRITICAL: Added aggregate_version < ? to enforce monotonicity (OCC/IDM)

        // Note: Assuming SqlUtil.extractEventData(event) is the correct utility based on other methods.
        Map<String, Object> map = SqlUtil.extractEventData(event);

        String ruleId = (String)map.get("ruleId");
        long newAggregateVersion = SqlUtil.getNewAggregateVersion(event);

        try (PreparedStatement statement = conn.prepareStatement(sql)) {
            // SET values (12 dynamic values + active = TRUE in SQL)
            int i = 1;

            // 1: host_id
            String hostId = (String)map.get("hostId");
            if (hostId != null && !hostId.isBlank()) {
                statement.setObject(i++, UUID.fromString(hostId));
            } else {
                statement.setNull(i++, Types.OTHER);
            }

            // 2: rule_name
            statement.setString(i++, (String)map.get("ruleName"));
            // 3: rule_version
            statement.setString(i++, (String)map.get("ruleVersion"));
            // 4: rule_type
            statement.setString(i++, (String)map.get("ruleType"));

            // 5: rule_group
            if (map.containsKey("ruleGroup")) {
                statement.setString(i++, (String)map.get("ruleGroup"));
            } else {
                statement.setNull(i++, Types.VARCHAR);
            }

            // 6: rule_desc
            if (map.containsKey("ruleDesc")) {
                statement.setString(i++, (String)map.get("ruleDesc"));
            } else {
                statement.setNull(i++, Types.VARCHAR);
            }

            // 7: rule_body
            statement.setString(i++, (String)map.get("ruleBody"));
            // 8: rule_owner
            statement.setString(i++, (String)map.get("ruleOwner"));

            // 9: common
            String common = (String)map.get("common");
            if (common != null && !common.isBlank()) {
                statement.setString(i++, common);
            } else {
                statement.setString(i++, "N"); // Default to 'N' based on table DDL
            }

            // 10: update_user
            statement.setString(i++, (String)event.get(Constants.USER));
            // 11: update_ts
            statement.setObject(i++, OffsetDateTime.parse((String)event.get(CloudEventV1.TIME)));
            // 12: aggregate_version
            statement.setLong(i++, newAggregateVersion);

            // WHERE conditions (2 placeholders)
            // 13: rule_id (PK)
            statement.setString(i++, ruleId);
            // 14: aggregate_version < ? (new version for OCC/IDM check)
            statement.setLong(i++, newAggregateVersion);

            int count = statement.executeUpdate();
            if (count == 0) {
                // If 0 rows updated, it means the record was either not found
                // OR aggregate_version >= newAggregateVersion (OCC/IDM check failed).
                // We IGNORE the failure and log a warning, as this is the desired idempotent/monotonic behavior.
                logger.warn("Update skipped for ruleId {} aggregateVersion {}. Record not found or a newer/same version already exists.", ruleId, newAggregateVersion);
            }
        } catch (SQLException e) {
            logger.error("SQLException during updateRule for ruleId {} aggregateVersion {}: {}", ruleId, newAggregateVersion, e.getMessage(), e);
            throw e;
        } catch (Exception e) {
            logger.error("Exception during updateRule for ruleId {} aggregateVersion {}: {}", ruleId, newAggregateVersion, e.getMessage(), e);
            throw e;
        }
    }

    /*
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
    */

    @Override
    public void deleteRule(Connection conn, Map<String, Object> event) throws SQLException, Exception {
        // Use UPDATE to implement Soft Delete (setting active = FALSE).
        // OCC/IDM is enforced by checking aggregate_version < newAggregateVersion.
        final String sql =
                """
                UPDATE rule_t
                SET active = FALSE,
                    update_user = ?,
                    update_ts = ?,
                    aggregate_version = ?
                WHERE rule_id = ?
                  AND aggregate_version < ?
                """; // <<< CRITICAL: Added aggregate_version < ? to enforce monotonicity (OCC/IDM)

        // Note: Assuming SqlUtil.extractEventData(event) is the correct utility based on other methods.
        Map<String, Object> map = SqlUtil.extractEventData(event);

        String ruleId = (String)map.get("ruleId");
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
            // 4: rule_id
            statement.setString(4, ruleId);
            // 5: aggregate_version < ? (new version for OCC/IDM check)
            statement.setLong(5, newAggregateVersion);

            int count = statement.executeUpdate();
            if (count == 0) {
                // If 0 rows updated, it means:
                // 1. The record was not found (already deleted or never existed).
                // 2. The OCC/IDM check failed (aggregate_version >= newAggregateVersion).
                // We IGNORE the failure and log a warning, as this is the desired idempotent/monotonic behavior.
                logger.warn("Soft delete skipped for ruleId {} aggregateVersion {}. Record not found or a newer/same version already exists.", ruleId, newAggregateVersion);
            }
        } catch (SQLException e) {
            logger.error("SQLException during deleteRule for ruleId {} aggregateVersion {}: {}", ruleId, newAggregateVersion, e.getMessage(), e);
            throw e;
        } catch (Exception e) {
            logger.error("Exception during deleteRule for ruleId {} aggregateVersion {}: {}", ruleId, newAggregateVersion, e.getMessage(), e);
            throw e;
        }
    }

    @Override
    public Result<List<Map<String, Object>>> queryRuleByGroup(String groupId) {
        Result<List<Map<String, Object>>> result;
        String sql =
                """
                SELECT rule_id, host_id, rule_name, rule_version, rule_type, rule_group,
                rule_desc, rule_body, rule_owner, update_user, update_ts, aggregate_version, active
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
                        map.put("active", resultSet.getBoolean("active"));
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
    public Result<String> queryRule(int offset, int limit, String filtersJson, String globalFilter, String sortingJson, String hostId) {
        Result<String> result;

        List<Map<String, Object>> filters = parseJsonList(filtersJson);
        List<Map<String, Object>> sorting = parseJsonList(sortingJson);

        String s =
                """
                SELECT COUNT(*) OVER () AS total,
                host_id, rule_id, rule_name, rule_version, rule_type,
                rule_group, rule_desc, rule_body, rule_owner,
                update_user, update_ts, aggregate_version, active
                FROM rule_t
                WHERE
                """;

        List<Object> parameters = new ArrayList<>();

        if (hostId != null && !hostId.isEmpty()) {
            s = s + " (host_id = ? OR host_id IS NULL)";
            parameters.add(UUID.fromString(hostId));
        } else {
            s = s + " host_id IS NULL";
        }

        String[] searchColumns = {"rule_name", "rule_desc"};
        String sqlBuilder = s + dynamicFilter(Arrays.asList("host_id"), Arrays.asList(searchColumns), filters, null, parameters) +
                globalFilter(globalFilter, searchColumns, parameters) +
                dynamicSorting("rule_id, rule_version", sorting, null) +
                "\nLIMIT ? OFFSET ?";

        parameters.add(limit);
        parameters.add(offset);

        int total = 0;
        List<Map<String, Object>> rules = new ArrayList<>();

        try (final Connection conn = ds.getConnection(); PreparedStatement preparedStatement = conn.prepareStatement(sqlBuilder)) {
            populateParameters(preparedStatement, parameters);
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
                    map.put("active", resultSet.getBoolean("active"));
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
                update_user, update_ts, aggregate_version, active
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
                        map.put("active", resultSet.getBoolean("active"));
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
