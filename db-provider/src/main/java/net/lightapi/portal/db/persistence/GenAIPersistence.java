package net.lightapi.portal.db.persistence;

import com.networknt.monad.Result;
import net.lightapi.portal.db.PortalPersistenceException;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Map;

public interface GenAIPersistence {
    // AgentDefinition
    void createAgentDefinition(Connection conn, Map<String, Object> event) throws PortalPersistenceException;
    void updateAgentDefinition(Connection conn, Map<String, Object> event) throws PortalPersistenceException;
    void deleteAgentDefinition(Connection conn, Map<String, Object> event) throws PortalPersistenceException;
    Result<String> queryAgentDefinition(int offset, int limit, String filters, String globalFilter, String sorting, boolean active, String hostId);
    Result<String> getAgentDefinitionById(String hostId, String agentDefId);

    // WorkflowDefinition
    void createWorkflowDefinition(Connection conn, Map<String, Object> event) throws PortalPersistenceException;
    void updateWorkflowDefinition(Connection conn, Map<String, Object> event) throws PortalPersistenceException;
    void deleteWorkflowDefinition(Connection conn, Map<String, Object> event) throws PortalPersistenceException;
    Result<String> queryWorkflowDefinition(int offset, int limit, String filters, String globalFilter, String sorting, boolean active, String hostId);
    Result<String> getWorkflowDefinitionById(String hostId, String wfDefId);

    // Worklist
    void createWorklist(Connection conn, Map<String, Object> event) throws PortalPersistenceException;
    void updateWorklist(Connection conn, Map<String, Object> event) throws PortalPersistenceException;
    void deleteWorklist(Connection conn, Map<String, Object> event) throws PortalPersistenceException;
    Result<String> queryWorklist(int offset, int limit, String filters, String globalFilter, String sorting, boolean active, String hostId);
    Result<String> getWorklistById(String hostId, String assigneeId, String categoryId);

    // WorklistColumn
    void createWorklistColumn(Connection conn, Map<String, Object> event) throws PortalPersistenceException;
    void updateWorklistColumn(Connection conn, Map<String, Object> event) throws PortalPersistenceException;
    void deleteWorklistColumn(Connection conn, Map<String, Object> event) throws PortalPersistenceException;
    Result<String> queryWorklistColumn(int offset, int limit, String filters, String globalFilter, String sorting, boolean active, String hostId);
    Result<String> getWorklistColumnById(String hostId, String assigneeId, String categoryId, int sequenceId);

    // ProcessInfo
    void createProcessInfo(Connection conn, Map<String, Object> event) throws PortalPersistenceException;
    void updateProcessInfo(Connection conn, Map<String, Object> event) throws PortalPersistenceException;
    void deleteProcessInfo(Connection conn, Map<String, Object> event) throws PortalPersistenceException;
    Result<String> queryProcessInfo(int offset, int limit, String filters, String globalFilter, String sorting, boolean active, String hostId);
    Result<String> getProcessInfoById(String hostId, String processId);

    // TaskInfo
    void createTaskInfo(Connection conn, Map<String, Object> event) throws PortalPersistenceException;
    void updateTaskInfo(Connection conn, Map<String, Object> event) throws PortalPersistenceException;
    void deleteTaskInfo(Connection conn, Map<String, Object> event) throws PortalPersistenceException;
    Result<String> queryTaskInfo(int offset, int limit, String filters, String globalFilter, String sorting, boolean active, String hostId);
    Result<String> getTaskInfoById(String hostId, String taskId);

    // TaskAssignment
    void createTaskAssignment(Connection conn, Map<String, Object> event) throws PortalPersistenceException;
    void updateTaskAssignment(Connection conn, Map<String, Object> event) throws PortalPersistenceException;
    void deleteTaskAssignment(Connection conn, Map<String, Object> event) throws PortalPersistenceException;
    Result<String> queryTaskAssignment(int offset, int limit, String filters, String globalFilter, String sorting, boolean active, String hostId);
    Result<String> getTaskAssignmentById(String hostId, String taskAsstId);

    // AuditLog
    void createAuditLog(Connection conn, Map<String, Object> event) throws PortalPersistenceException;
    Result<String> queryAuditLog(int offset, int limit, String filters, String globalFilter, String sorting, String hostId);
    Result<String> getAuditLogById(String hostId, String auditLogId);

    // Skill
    void createSkill(Connection conn, Map<String, Object> event) throws PortalPersistenceException;
    void updateSkill(Connection conn, Map<String, Object> event) throws PortalPersistenceException;
    void deleteSkill(Connection conn, Map<String, Object> event) throws PortalPersistenceException;
    Result<String> querySkill(int offset, int limit, String filters, String globalFilter, String sorting, boolean active, String hostId);
    Result<String> getSkillById(String hostId, String skillId);

    // SkillParam
    void createSkillParam(Connection conn, Map<String, Object> event) throws PortalPersistenceException;
    void updateSkillParam(Connection conn, Map<String, Object> event) throws PortalPersistenceException;
    void deleteSkillParam(Connection conn, Map<String, Object> event) throws PortalPersistenceException;
    Result<String> querySkillParam(int offset, int limit, String filters, String globalFilter, String sorting, boolean active, String hostId);
    Result<String> getSkillParamById(String hostId, String paramId);

    // SkillDependency
    void createSkillDependency(Connection conn, Map<String, Object> event) throws PortalPersistenceException;
    void updateSkillDependency(Connection conn, Map<String, Object> event) throws PortalPersistenceException;
    void deleteSkillDependency(Connection conn, Map<String, Object> event) throws PortalPersistenceException;
    Result<String> querySkillDependency(int offset, int limit, String filters, String globalFilter, String sorting, boolean active, String hostId);
    Result<String> getSkillDependencyById(String hostId, String skillId, String dependsOnSkillId);

    // AgentSkill
    void createAgentSkill(Connection conn, Map<String, Object> event) throws PortalPersistenceException;
    void updateAgentSkill(Connection conn, Map<String, Object> event) throws PortalPersistenceException;
    void deleteAgentSkill(Connection conn, Map<String, Object> event) throws PortalPersistenceException;
    Result<String> queryAgentSkill(int offset, int limit, String filters, String globalFilter, String sorting, boolean active, String hostId);
    Result<String> getAgentSkillById(String hostId, String agentDefId, String skillId);

    // AgentSessionHistory
    void createAgentSessionHistory(Connection conn, Map<String, Object> event) throws PortalPersistenceException;
    void deleteAgentSessionHistory(Connection conn, Map<String, Object> event) throws PortalPersistenceException;
    Result<String> queryAgentSessionHistory(int offset, int limit, String filters, String globalFilter, String sorting, boolean active, String hostId);
    Result<String> getAgentSessionHistoryById(String hostId, String sessionHistoryId);

    // SessionMemory
    void createSessionMemory(Connection conn, Map<String, Object> event) throws PortalPersistenceException;
    void updateSessionMemory(Connection conn, Map<String, Object> event) throws PortalPersistenceException;
    void deleteSessionMemory(Connection conn, Map<String, Object> event) throws PortalPersistenceException;
    Result<String> querySessionMemory(int offset, int limit, String filters, String globalFilter, String sorting, boolean active, String hostId);
    Result<String> getSessionMemoryById(String hostId, String memId);

    // UserMemory
    void createUserMemory(Connection conn, Map<String, Object> event) throws PortalPersistenceException;
    void updateUserMemory(Connection conn, Map<String, Object> event) throws PortalPersistenceException;
    void deleteUserMemory(Connection conn, Map<String, Object> event) throws PortalPersistenceException;
    Result<String> queryUserMemory(int offset, int limit, String filters, String globalFilter, String sorting, boolean active, String hostId);
    Result<String> getUserMemoryById(String hostId, String memId);

    // AgentMemory
    void createAgentMemory(Connection conn, Map<String, Object> event) throws PortalPersistenceException;
    void updateAgentMemory(Connection conn, Map<String, Object> event) throws PortalPersistenceException;
    void deleteAgentMemory(Connection conn, Map<String, Object> event) throws PortalPersistenceException;
    Result<String> queryAgentMemory(int offset, int limit, String filters, String globalFilter, String sorting, boolean active, String hostId);
    Result<String> getAgentMemoryById(String hostId, String memId);

    // OrgMemory
    void createOrgMemory(Connection conn, Map<String, Object> event) throws PortalPersistenceException;
    void updateOrgMemory(Connection conn, Map<String, Object> event) throws PortalPersistenceException;
    void deleteOrgMemory(Connection conn, Map<String, Object> event) throws PortalPersistenceException;
    Result<String> queryOrgMemory(int offset, int limit, String filters, String globalFilter, String sorting, boolean active, String hostId);
    Result<String> getOrgMemoryById(String hostId, String memId);
}
