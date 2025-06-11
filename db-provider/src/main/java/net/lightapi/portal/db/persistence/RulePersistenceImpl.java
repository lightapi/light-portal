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
import static java.sql.Types.NULL;
import static net.lightapi.portal.db.util.SqlUtil.addCondition;

public class RulePersistenceImpl implements RulePersistence {

    private static final Logger logger = LoggerFactory.getLogger(RulePersistenceImpl.class);
    // Consider moving these to a shared constants class if they are truly general
    private static final String SQL_EXCEPTION = PortalDbProvider.SQL_EXCEPTION;
    private static final String GENERIC_EXCEPTION = PortalDbProvider.GENERIC_EXCEPTION;
    private static final String OBJECT_NOT_FOUND = PortalDbProvider.OBJECT_NOT_FOUND;

    private final NotificationService notificationService;

    public RulePersistenceImpl(NotificationService notificationService) {
        this.notificationService = notificationService;
    }
    @Override
    public Result<String> createRule(Map<String, Object> event) {
        final String insertRule = "INSERT INTO rule_t (rule_id, rule_name, rule_version, rule_type, rule_group, " +
                "rule_desc, rule_body, rule_owner, common, update_user, update_ts) " +
                "VALUES (?, ?, ?, ?, ?,   ?, ?, ?, ?, ?,  ?)";
        final String insertHostRule = "INSERT INTO rule_host_t (host_id, rule_id, update_user, update_ts) " +
                "VALUES (?, ?, ?, ?)";

        Result<String> result = null;
        Map<String, Object> map = (Map<String, Object>)event.get(PortalConstants.DATA);
        try (Connection conn = ds.getConnection()) {
            conn.setAutoCommit(false);
            // no duplicate record, insert the user into database and write a success notification.
            try {
                try (PreparedStatement statement = conn.prepareStatement(insertRule)) {
                    statement.setString(1, (String)map.get("ruleId"));
                    statement.setString(2, (String)map.get("ruleName"));
                    statement.setString(3, (String)map.get("ruleVersion"));
                    statement.setString(4, (String)map.get("ruleType"));
                    String ruleGroup = (String)map.get("ruleGroup");
                    if (ruleGroup != null && !ruleGroup.isEmpty())
                        statement.setString(5, ruleGroup);
                    else
                        statement.setNull(5, NULL);
                    String ruleDesc = (String)map.get("ruleDesc");
                    if (ruleDesc != null && !ruleDesc.isEmpty())
                        statement.setString(6, ruleDesc);
                    else
                        statement.setNull(6, NULL);
                    statement.setString(7, (String)map.get("ruleBody"));
                    statement.setString(8, (String)map.get("ruleOwner"));
                    statement.setString(9, (String)map.get("common"));
                    statement.setString(10, (String)event.get(Constants.USER));
                    statement.setObject(11, OffsetDateTime.parse((String)event.get(CloudEventV1.TIME)));
                    int count = statement.executeUpdate();
                    if (count == 0) {
                        throw new SQLException("failed to insert the rule " + map.get("ruleId"));
                    }
                }
                try (PreparedStatement statement = conn.prepareStatement(insertHostRule)) {
                    statement.setObject(1, UUID.fromString((String)event.get(Constants.HOST)));
                    statement.setString(2, (String)map.get("ruleId"));
                    statement.setString(3, (String)event.get(Constants.USER));
                    statement.setObject(4, OffsetDateTime.parse((String)event.get(CloudEventV1.TIME)));
                    int count = statement.executeUpdate();
                    if (count == 0) {
                        throw new SQLException("failed to insert the host_rule for host " + event.get(Constants.HOST) + " rule " + map.get("ruleId"));
                    }
                }
                conn.commit();
                result = Success.of((String)map.get("ruleId"));
                notificationService.insertNotification(event, true, null);
            } catch (SQLException e) {
                logger.error("SQLException:", e);
                conn.rollback();
                notificationService.insertNotification(event, false, e.getMessage());
                result = Failure.of(new Status(SQL_EXCEPTION, e.getMessage()));
            } catch (Exception e) {
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
    public Result<String> updateRule(Map<String, Object> event) {
        final String updateRule = "UPDATE rule_t SET rule_name = ?, rule_version = ?, rule_type = ?, rule_group = ?, rule_desc = ?, " +
                "rule_body = ?, rule_owner = ?, common = ?, update_user = ?, update_ts = ? " +
                "WHERE rule_id = ?";

        Result<String> result = null;
        Map<String, Object> map = (Map<String, Object>)event.get(PortalConstants.DATA);
        try (Connection conn = ds.getConnection()) {
            conn.setAutoCommit(false);

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
                statement.setString(11, (String)map.get("ruleId"));

                int count = statement.executeUpdate();
                if (count == 0) {
                    throw new SQLException("no record is updated for rule " + map.get("ruleId"));
                }
                conn.commit();
                result = Success.of((String)map.get("ruleId"));
                notificationService.insertNotification(event, true, null);
            } catch (SQLException e) {
                logger.error("SQLException:", e);
                conn.rollback();
                notificationService.insertNotification(event, false, e.getMessage());
                result = Failure.of(new Status(SQL_EXCEPTION, e.getMessage()));
            } catch (Exception e) {
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
    public Result<String> deleteRule(Map<String, Object> event) {
        final String deleteRule = "DELETE from rule_t WHERE rule_id = ?";
        final String deleteHostRule = "DELETE from rule_host_t WHERE host_id = ? AND rule_id = ?";
        Result<String> result;
        Map<String, Object> map = (Map<String, Object>)event.get(PortalConstants.DATA);
        try (Connection conn = ds.getConnection()) {
            conn.setAutoCommit(false);
            try {
                try (PreparedStatement statement = conn.prepareStatement(deleteRule)) {
                    statement.setString(1, (String)map.get("ruleId"));
                    int count = statement.executeUpdate();
                    if (count == 0) {
                        throw new SQLException("no record is deleted for rule " + map.get("ruleId"));
                    }
                }
                try (PreparedStatement statement = conn.prepareStatement(deleteHostRule)) {
                    statement.setObject(1, (String)event.get(Constants.HOST));
                    statement.setString(2, (String)map.get("ruleId"));
                    int count = statement.executeUpdate();
                    if (count == 0) {
                        throw new SQLException("no record is deleted for host " + event.get(Constants.HOST) + " rule " + map.get("ruleId"));
                    }
                }
                conn.commit();
                result = Success.of((String)event.get(Constants.USER));
                notificationService.insertNotification(event, true, null);
            } catch (SQLException e) {
                logger.error("SQLException:", e);
                conn.rollback();
                notificationService.insertNotification(event, false, e.getMessage());
                result = Failure.of(new Status(SQL_EXCEPTION, e.getMessage()));
            } catch (Exception e) {
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
    public Result<List<Map<String, Object>>> queryRuleByHostGroup(String hostId, String groupId) {
        Result<List<Map<String, Object>>> result;
        String sql = "SELECT rule_id, host_id, rule_type, rule_group, rule_visibility, rule_description, rule_body, rule_owner " +
                "update_user, update_ts " +
                "FROM rule_t WHERE host_id = ? AND rule_group = ?";
        try (final Connection conn = ds.getConnection()) {
            List<Map<String, Object>> list = new ArrayList<>();
            try (PreparedStatement statement = conn.prepareStatement(sql)) {
                statement.setObject(1, UUID.fromString(hostId));
                statement.setString(2, groupId);
                try (ResultSet resultSet = statement.executeQuery()) {
                    while (resultSet.next()) {
                        Map<String, Object> map = new HashMap<>();
                        map.put("ruleId", resultSet.getString("rule_id"));
                        map.put("hostId", resultSet.getObject("host_id", UUID.class));
                        map.put("ruleType", resultSet.getString("rule_type"));
                        map.put("ruleGroup", resultSet.getBoolean("rule_group"));
                        map.put("ruleVisibility", resultSet.getString("rule_visibility"));
                        map.put("ruleDescription", resultSet.getString("rule_description"));
                        map.put("ruleBody", resultSet.getString("rule_body"));
                        map.put("ruleOwner", resultSet.getString("rule_owner"));
                        map.put("updateUser", resultSet.getString("update_user"));
                        map.put("updateTs", resultSet.getObject("update_ts") != null ? resultSet.getObject("update_ts", OffsetDateTime.class) : null);
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
                                    String ruleBody, String ruleOwner, String common) {
        Result<String> result;
        String sql;
        List<Object> parameters = new ArrayList<>();
        if(common == null || common.equalsIgnoreCase("N")) {
            StringBuilder sqlBuilder = new StringBuilder();
            sqlBuilder.append("SELECT COUNT(*) OVER () AS total, h.host_id, r.rule_id, r.rule_name, r.rule_version, " +
                    "r.rule_type, r.rule_group, r.common, r.rule_desc, r.rule_body, r.rule_owner, " +
                    "r.update_user, r.update_ts " +
                    "FROM rule_t r, rule_host_t h " +
                    "WHERE r.rule_id = h.rule_id " +
                    "AND h.host_id = ?\n");
            parameters.add(UUID.fromString(hostId));

            StringBuilder whereClause = new StringBuilder();

            addCondition(whereClause, parameters, "r.rule_id", ruleId);
            addCondition(whereClause, parameters, "r.rule_name", ruleName);
            addCondition(whereClause, parameters, "r.rule_version", ruleVersion);
            addCondition(whereClause, parameters, "r.rule_type", ruleType);
            addCondition(whereClause, parameters, "r.rule_group", ruleGroup);
            addCondition(whereClause, parameters, "r.rule_desc", ruleDesc);
            addCondition(whereClause, parameters, "r.rule_body", ruleBody);
            addCondition(whereClause, parameters, "r.rule_owner", ruleOwner);

            if (!whereClause.isEmpty()) {
                sqlBuilder.append("AND ").append(whereClause);
            }
            sqlBuilder.append(" ORDER BY rule_id\n" +
                    "LIMIT ? OFFSET ?");

            parameters.add(limit);
            parameters.add(offset);
            sql = sqlBuilder.toString();
        } else {
            StringBuilder sqlBuilder = new StringBuilder();
            sqlBuilder.append("SELECT \n" +
                    "                        COUNT(*) OVER () AS total,\n" +
                    "                        host_id,\n" +
                    "                        rule_id,\n" +
                    "                        rule_name,\n" +
                    "                        rule_version,\n" +
                    "                        rule_type,\n" +
                    "                        rule_group,\n" +
                    "                        common,\n" +
                    "                        rule_desc,\n" +
                    "                        rule_body,\n" +
                    "                        rule_owner,\n" +
                    "                        update_user,\n" +
                    "                        update_ts\n" +
                    "                    FROM (\n" +
                    "                       SELECT \n" +
                    "                        h.host_id,\n" +
                    "                        r.rule_id,\n" +
                    "                        r.rule_name,\n" +
                    "                        r.rule_version,\n" +
                    "                        r.rule_type,\n" +
                    "                        r.rule_group,\n" +
                    "                        r.common,\n" +
                    "                        r.rule_desc,\n" +
                    "                        r.rule_body,\n" +
                    "                        r.rule_owner,\n" +
                    "                        r.update_user,\n" +
                    "                        r.update_ts\n" +
                    "                    FROM rule_t r\n" +
                    "                    JOIN rule_host_t h ON r.rule_id = h.rule_id\n" +
                    "                    WHERE h.host_id = ?\n");
            parameters.add(UUID.fromString(hostId));
            StringBuilder whereClause = new StringBuilder();

            addCondition(whereClause, parameters, "r.rule_id", ruleId);
            addCondition(whereClause, parameters, "r.rule_name", ruleName);
            addCondition(whereClause, parameters, "r.rule_version", ruleVersion);
            addCondition(whereClause, parameters, "r.rule_type", ruleType);
            addCondition(whereClause, parameters, "r.rule_group", ruleGroup);
            addCondition(whereClause, parameters, "r.rule_desc", ruleDesc);
            addCondition(whereClause, parameters, "r.rule_body", ruleBody);
            addCondition(whereClause, parameters, "r.rule_owner", ruleOwner);
            if (!whereClause.isEmpty()) {
                sqlBuilder.append("AND ").append(whereClause);
            }

            sqlBuilder.append("                    \n" +
                    "                    UNION ALL\n" +
                    "                    \n" +
                    "                   SELECT\n" +
                    "                        h.host_id,\n" +
                    "                        r.rule_id,\n" +
                    "                        r.rule_name,\n" +
                    "                        r.rule_version,\n" +
                    "                        r.rule_type,\n" +
                    "                        r.rule_group,\n" +
                    "                        r.common,\n" +
                    "                        r.rule_desc,\n" +
                    "                        r.rule_body,\n" +
                    "                        r.rule_owner,\n" +
                    "                        r.update_user,\n" +
                    "                        r.update_ts\n" +
                    "                    FROM rule_t r\n" +
                    "                    JOIN rule_host_t h ON r.rule_id = h.rule_id\n" +
                    "                    WHERE r.common = 'Y'\n" +
                    "                      AND h.host_id != ?\n" +
                    "                       AND  NOT EXISTS (\n" +
                    "                         SELECT 1\n" +
                    "                        FROM rule_host_t eh\n" +
                    "                         WHERE eh.rule_id = r.rule_id\n" +
                    "                         AND eh.host_id=?\n" +
                    "                     )\n");
            parameters.add(UUID.fromString(hostId));
            parameters.add(UUID.fromString(hostId));


            StringBuilder whereClauseCommon = new StringBuilder();
            addCondition(whereClauseCommon, parameters, "r.rule_id", ruleId);
            addCondition(whereClauseCommon, parameters, "r.rule_name", ruleName);
            addCondition(whereClauseCommon, parameters, "r.rule_version", ruleVersion);
            addCondition(whereClauseCommon, parameters, "r.rule_type", ruleType);
            addCondition(whereClauseCommon, parameters, "r.rule_group", ruleGroup);
            addCondition(whereClauseCommon, parameters, "r.rule_desc", ruleDesc);
            addCondition(whereClauseCommon, parameters, "r.rule_body", ruleBody);
            addCondition(whereClauseCommon, parameters, "r.rule_owner", ruleOwner);

            if (!whereClauseCommon.isEmpty()) {
                sqlBuilder.append("AND ").append(whereClauseCommon);
            }


            sqlBuilder.append("                 ) AS combined_rules\n");

            sqlBuilder.append(" ORDER BY rule_id\n" +
                    "LIMIT ? OFFSET ?");

            parameters.add(limit);
            parameters.add(offset);

            sql = sqlBuilder.toString();
        }

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
                    map.put("common", resultSet.getString("common"));
                    map.put("ruleDesc", resultSet.getString("rule_desc"));
                    map.put("ruleBody", resultSet.getString("rule_body"));
                    map.put("ruleOwner", resultSet.getString("rule_owner"));
                    map.put("updateUser", resultSet.getString("update_user"));
                    map.put("updateTs", resultSet.getObject("update_ts") != null ? resultSet.getObject("update_ts", OffsetDateTime.class) : null);
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
        String sql = "SELECT rule_id, host_id, rule_type, rule_group, rule_visibility, rule_description, rule_body, rule_owner " +
                "update_user, update_ts " +
                "FROM rule_t WHERE rule_id = ?";
        try (final Connection conn = ds.getConnection()) {
            Map<String, Object> map = new HashMap<>();
            try (PreparedStatement statement = conn.prepareStatement(sql)) {
                statement.setString(1, ruleId);
                try (ResultSet resultSet = statement.executeQuery()) {
                    if (resultSet.next()) {
                        map.put("ruleId", resultSet.getString("rule_id"));
                        map.put("hostId", resultSet.getObject("host_id", UUID.class));
                        map.put("ruleType", resultSet.getString("rule_type"));
                        map.put("ruleGroup", resultSet.getBoolean("rule_group"));
                        map.put("ruleVisibility", resultSet.getString("rule_visibility"));
                        map.put("ruleDescription", resultSet.getString("rule_description"));
                        map.put("ruleBody", resultSet.getString("rule_body"));
                        map.put("ruleOwner", resultSet.getString("rule_owner"));
                        map.put("updateUser", resultSet.getString("update_user"));
                        map.put("updateTs", resultSet.getObject("update_ts") != null ? resultSet.getObject("update_ts", OffsetDateTime.class) : null);
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
    public Result<String> queryRuleByHostType(String hostId, String ruleType) {
        Result<String> result;
        String sql = "SELECT r.rule_id\n" +
                "FROM rule_t r, rule_host_t h\n" +
                "WHERE r.rule_id = h.rule_id\n" +
                "AND h.host_id = ?\n" +
                "AND r.rule_type = ?\n" +
                "UNION\n" +
                "SELECT r.rule_id r\n" +
                "FROM rule_t r, rule_host_t h\n" +
                "WHERE h.host_id != ?\n" +
                "AND r.rule_type = ?\n" +
                "AND r.common = 'Y'";
        try (final Connection conn = ds.getConnection()) {
            List<Map<String, Object>> list = new ArrayList<>();
            try (PreparedStatement statement = conn.prepareStatement(sql)) {
                statement.setObject(1, UUID.fromString(hostId));
                statement.setString(2, ruleType);
                statement.setObject(3, UUID.fromString(hostId));
                statement.setString(4, ruleType);

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
                result = Failure.of(new Status(OBJECT_NOT_FOUND, "rule with host id and rule type ", hostId  + "|" + ruleType));
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

    @Override
    public Result<List<Map<String, Object>>> queryRuleByHostApiId(String hostId, String apiId, String apiVersion) {
        Result<List<Map<String, Object>>> result;
        String sql = "SELECT h.host_id, r.rule_id, r.rule_type, a.endpoint, r.rule_body\n" +
                "FROM rule_t r, rule_host_t h, api_endpoint_rule_t a \n" +
                "WHERE r.rule_id = h.rule_id\n" +
                "AND h.host_id = a.host_id\n" +
                "AND h.host_id = ?\n" +
                "AND a.api_id = ?\n" +
                "AND a.api_version = ?";
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
