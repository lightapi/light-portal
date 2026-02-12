package net.lightapi.portal.db.persistence;

import com.networknt.monad.Result;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Map;

public interface GenAIPersistence {
    // AgentDefinition
    void createAgentDefinition(Connection conn, Map<String, Object> event) throws Exception;
    void updateAgentDefinition(Connection conn, Map<String, Object> event) throws Exception;
    void deleteAgentDefinition(Connection conn, Map<String, Object> event) throws Exception;
    Result<String> queryAgentDefinition(int offset, int limit, String filters, String globalFilter, String sorting, boolean active, String hostId);
    Result<String> getAgentDefinitionById(String hostId, String agentDefId);

    // WorkflowDefinition
    void createWorkflowDefinition(Connection conn, Map<String, Object> event) throws Exception;
    void updateWorkflowDefinition(Connection conn, Map<String, Object> event) throws Exception;
    void deleteWorkflowDefinition(Connection conn, Map<String, Object> event) throws Exception;
    Result<String> queryWorkflowDefinition(int offset, int limit, String filters, String globalFilter, String sorting, boolean active, String hostId);
    Result<String> getWorkflowDefinitionById(String hostId, String wfDefId);
}
