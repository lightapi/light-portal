package net.lightapi.portal.db.persistence;

import com.networknt.monad.Result;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Map;

public interface GenAIPersistence {
    // AgentDefinition
    void createAgentDefinition(Connection conn, Map<String, Object> event) throws SQLException;
    void updateAgentDefinition(Connection conn, Map<String, Object> event) throws SQLException;
    void deleteAgentDefinition(Connection conn, Map<String, Object> event) throws SQLException;
    Result<String> queryAgentDefinition(int offset, int limit, String filters, String globalFilter, String sorting, boolean active, String hostId);
    Result<String> getAgentDefinitionById(String hostId, String agentDefId);
}
