package net.lightapi.portal.db.persistence;

import com.networknt.monad.Result;

import java.sql.Connection; // Added import
import java.sql.SQLException; // Added import
import java.util.List;
import java.util.Map;

public interface RulePersistence {
    void createRule(Connection conn, Map<String, Object> event) throws SQLException, Exception;
    void updateRule(Connection conn, Map<String, Object> event) throws SQLException, Exception;
    void deleteRule(Connection conn, Map<String, Object> event) throws SQLException, Exception;
    Result<List<Map<String, Object>>> queryRuleByGroup(String groupId);
    Result<String> queryRule(int offset, int limit, String hostId, String ruleId, String ruleName,
                             String ruleVersion, String ruleType, String ruleGroup, String ruleDesc,
                             String ruleBody, String ruleOwner);
    Result<Map<String, Object>> queryRuleById(String ruleId);
    Result<String> queryRuleByType(String ruleType);
    Result<List<Map<String, Object>>> queryRuleByHostApiId(String hostId, String apiId, String apiVersion);

}
