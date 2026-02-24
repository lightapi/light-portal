package net.lightapi.portal.db.persistence;

import com.networknt.config.JsonMapper;
import com.networknt.monad.Failure;
import com.networknt.monad.Result;
import com.networknt.monad.Success;
import com.networknt.status.Status;
import com.networknt.utility.Constants;
import io.cloudevents.core.v1.CloudEventV1;
import net.lightapi.portal.db.PortalDbProvider;
import net.lightapi.portal.db.PortalPersistenceException;
import net.lightapi.portal.db.util.SqlUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;
import java.time.OffsetDateTime;
import java.util.*;

import static com.networknt.db.provider.SqlDbStartupHook.ds;
import static net.lightapi.portal.db.util.SqlUtil.*;

public class GenAIPersistenceImpl implements GenAIPersistence {
    private static final Logger logger = LoggerFactory.getLogger(GenAIPersistenceImpl.class);
    private static final String SQL_EXCEPTION = PortalDbProvider.SQL_EXCEPTION;
    private static final String GENERIC_EXCEPTION = PortalDbProvider.GENERIC_EXCEPTION;
    private static final String OBJECT_NOT_FOUND = PortalDbProvider.OBJECT_NOT_FOUND;

    @Override
    public void createAgentDefinition(Connection conn, Map<String, Object> event) throws PortalPersistenceException {
        final String sql =
                """
                INSERT INTO agent_definition_t (host_id, agent_def_id, agent_name, model_provider, model_name, api_key_ref, temperature, max_tokens, update_user, update_ts, aggregate_version, active)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, TRUE)
                ON CONFLICT (host_id, agent_def_id) DO UPDATE
                SET agent_name = EXCLUDED.agent_name,
                    model_provider = EXCLUDED.model_provider,
                    model_name = EXCLUDED.model_name,
                    api_key_ref = EXCLUDED.api_key_ref,
                    temperature = EXCLUDED.temperature,
                    max_tokens = EXCLUDED.max_tokens,
                    update_user = EXCLUDED.update_user,
                    update_ts = EXCLUDED.update_ts,
                    aggregate_version = EXCLUDED.aggregate_version,
                    active = TRUE
                WHERE agent_definition_t.aggregate_version < EXCLUDED.aggregate_version
                AND agent_definition_t.active = FALSE
                """;
        Map<String, Object> map = SqlUtil.extractEventData(event);
        String agentDefId = (String)map.get("agentDefId");
        String hostId = (String)map.get("hostId");
        long newAggregateVersion = SqlUtil.getNewAggregateVersion(event);

        try (PreparedStatement statement = conn.prepareStatement(sql)) {
            statement.setObject(1, UUID.fromString(hostId));
            statement.setObject(2, UUID.fromString(agentDefId));
            statement.setString(3, (String)map.get("agentName"));
            statement.setString(4, (String)map.get("modelProvider"));
            statement.setString(5, (String)map.get("modelName"));
            statement.setString(6, (String)map.get("apiKeyRef"));
            if(map.get("temperature") != null) {
                statement.setDouble(7, ((Number)map.get("temperature")).doubleValue());
            } else {
                statement.setDouble(7, 0.7);
            }
            if(map.get("maxTokens") != null) {
                statement.setInt(8, ((Number)map.get("maxTokens")).intValue());
            } else {
                statement.setNull(8, Types.INTEGER);
            }
            statement.setString(9, (String)event.get(Constants.USER));
            statement.setObject(10, OffsetDateTime.parse((String)event.get(CloudEventV1.TIME)));
            statement.setLong(11, newAggregateVersion);

            int count = statement.executeUpdate();
            if (count == 0) {
                logger.warn("Creation skipped for hostId {} agentDefId {} aggregateVersion {}. A newer or same version already exists.", hostId, agentDefId, newAggregateVersion);
            }
        } catch (SQLException e) {
            logger.error("SQLException during createAgentDefinition for hostId {} agentDefId {} aggregateVersion {}: {}", hostId, agentDefId, newAggregateVersion, e.getMessage(), e);
            throw new PortalPersistenceException("Persistence Error", e);
        } catch (Exception e) {
            logger.error("Exception during createAgentDefinition for hostId {} agentDefId {} aggregateVersion {}: {}", hostId, agentDefId, newAggregateVersion, e.getMessage(), e);
            throw new PortalPersistenceException("Persistence Error", e);
        }
    }

    @Override
    public void updateAgentDefinition(Connection conn, Map<String, Object> event) throws PortalPersistenceException {
        final String sql =
            """
            UPDATE agent_definition_t
            SET agent_name=?, model_provider=?, model_name=?, api_key_ref=?, temperature=?, max_tokens=?, update_user=?, update_ts=?, aggregate_version=?, active=TRUE
            WHERE host_id=? AND agent_def_id=? AND aggregate_version < ?
            """;
        Map<String, Object> map = SqlUtil.extractEventData(event);
        String agentDefId = (String)map.get("agentDefId");
        String hostId = (String)map.get("hostId");
        long newAggregateVersion = SqlUtil.getNewAggregateVersion(event);

        try (PreparedStatement statement = conn.prepareStatement(sql)) {
            statement.setString(1, (String)map.get("agentName"));
            statement.setString(2, (String)map.get("modelProvider"));
            statement.setString(3, (String)map.get("modelName"));
            statement.setString(4, (String)map.get("apiKeyRef"));
            if(map.get("temperature") != null) {
                statement.setDouble(5, ((Number)map.get("temperature")).doubleValue());
            } else {
                statement.setDouble(5, 0.7);
            }
            if(map.get("maxTokens") != null) {
                statement.setInt(6, ((Number)map.get("maxTokens")).intValue());
            } else {
                statement.setNull(6, Types.INTEGER);
            }
            statement.setString(7, (String)event.get(Constants.USER));
            statement.setObject(8, OffsetDateTime.parse((String)event.get(CloudEventV1.TIME)));
            statement.setLong(9, newAggregateVersion);
            statement.setObject(10, UUID.fromString(hostId));
            statement.setObject(11, UUID.fromString(agentDefId));
            statement.setLong(12, newAggregateVersion);

            int count = statement.executeUpdate();
            if (count == 0) {
                logger.warn("Update skipped for hostId {} agentDefId {} aggregateVersion {}. Record not found or a newer/same version already exists.", hostId, agentDefId, newAggregateVersion);
            }
        } catch (SQLException e) {
            logger.error("SQLException during updateAgentDefinition for hostId {} agentDefId {} aggregateVersion {}: {}", hostId, agentDefId, newAggregateVersion, e.getMessage(), e);
            throw new PortalPersistenceException("Persistence Error", e);
        } catch (Exception e) {
            logger.error("Exception during updateAgentDefinition for hostId {} agentDefId {} aggregateVersion {}: {}", hostId, agentDefId, newAggregateVersion, e.getMessage(), e);
            throw new PortalPersistenceException("Persistence Error", e);
        }
    }

    @Override
    public void deleteAgentDefinition(Connection conn, Map<String, Object> event) throws PortalPersistenceException {
        final String sql =
            """
            UPDATE agent_definition_t
            SET active = FALSE,
                update_user = ?,
                update_ts = ?,
                aggregate_version = ?
            WHERE host_id=? AND agent_def_id=? AND aggregate_version < ?
            """;
        Map<String, Object> map = SqlUtil.extractEventData(event);
        String agentDefId = (String)map.get("agentDefId");
        String hostId = (String)map.get("hostId");
        long newAggregateVersion = SqlUtil.getNewAggregateVersion(event);

        try (PreparedStatement statement = conn.prepareStatement(sql)) {
            statement.setString(1, (String)event.get(Constants.USER));
            statement.setObject(2, OffsetDateTime.parse((String)event.get(CloudEventV1.TIME)));
            statement.setLong(3, newAggregateVersion);
            statement.setObject(4, UUID.fromString(hostId));
            statement.setObject(5, UUID.fromString(agentDefId));
            statement.setLong(6, newAggregateVersion);

            int count = statement.executeUpdate();
            if (count == 0) {
                logger.warn("Delete skipped for hostId {} agentDefId {} aggregateVersion {}. Record not found or a newer/same version already exists.", hostId, agentDefId, newAggregateVersion);
            }
        } catch (SQLException e) {
            logger.error("SQLException during deleteAgentDefinition for hostId {} agentDefId {} aggregateVersion {}: {}", hostId, agentDefId, newAggregateVersion, e.getMessage(), e);
            throw new PortalPersistenceException("Persistence Error", e);
        } catch (Exception e) {
            logger.error("Exception during deleteAgentDefinition for hostId {} agentDefId {} aggregateVersion {}: {}", hostId, agentDefId, newAggregateVersion, e.getMessage(), e);
            throw new PortalPersistenceException("Persistence Error", e);
        }
    }

    @Override
    public Result<String> queryAgentDefinition(int offset, int limit, String filtersJson, String globalFilter, String sortingJson, boolean active, String hostId) {
        Result<String> result;
        List<Map<String, Object>> filters = parseJsonList(filtersJson);
        List<Map<String, Object>> sorting = parseJsonList(sortingJson);
        String s =
            """
            SELECT COUNT(*) OVER () AS total, host_id, agent_def_id, agent_name, model_provider, model_name,
            api_key_ref, temperature, max_tokens, update_user, update_ts, aggregate_version, active
            FROM agent_definition_t
            WHERE host_id = ?
            """;
        List<Object> parameters = new ArrayList<>();
        parameters.add(UUID.fromString(hostId));
        String activeClause = SqlUtil.buildMultiTableActiveClause(active);
        String[] searchColumns = {"agent_name", "model_provider", "model_name"};
        String sqlBuilder = s + activeClause +
                dynamicFilter(Arrays.asList("host_id"), Arrays.asList(searchColumns), filters, null, parameters) +
                globalFilter(globalFilter, searchColumns, parameters) +
                dynamicSorting("agent_name", sorting, null) +
                "\nLIMIT ? OFFSET ?";

        parameters.add(limit);
        parameters.add(offset);
        if(logger.isTraceEnabled()) logger.trace("queryAgentDefinition sql: {}", sqlBuilder);
        int total = 0;
        List<Map<String, Object>> agents = new ArrayList<>();
        try (final Connection conn = ds.getConnection(); PreparedStatement preparedStatement = conn.prepareStatement(sqlBuilder)) {
            populateParameters(preparedStatement, parameters);
            boolean isFirstRow = true;
            try (ResultSet resultSet = preparedStatement.executeQuery()) {
                while (resultSet.next()) {
                    Map<String, Object> map = new HashMap<>();
                    if (isFirstRow) {
                        total = resultSet.getInt("total");
                        isFirstRow = false;
                    }
                    map.put("hostId", resultSet.getObject("host_id", UUID.class));
                    map.put("agentDefId", resultSet.getObject("agent_def_id", UUID.class));
                    map.put("agentName", resultSet.getString("agent_name"));
                    map.put("modelProvider", resultSet.getString("model_provider"));
                    map.put("modelName", resultSet.getString("model_name"));
                    map.put("apiKeyRef", resultSet.getString("api_key_ref"));
                    map.put("temperature", resultSet.getDouble("temperature"));
                    map.put("maxTokens", resultSet.getObject("max_tokens") != null ? resultSet.getInt("max_tokens") : null);
                    map.put("updateUser", resultSet.getString("update_user"));
                    map.put("updateTs", resultSet.getObject("update_ts", OffsetDateTime.class));
                    map.put("aggregateVersion", resultSet.getLong("aggregate_version"));
                    map.put("active", resultSet.getBoolean("active"));
                    agents.add(map);
                }
            }
            Map<String, Object> resultMap = new HashMap<>();
            resultMap.put("total", total);
            resultMap.put("agents", agents);
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
    public Result<String> getAgentDefinitionById(String hostId, String agentDefId) {
        final String sql = "SELECT host_id, agent_def_id, agent_name, model_provider, model_name, api_key_ref, temperature, max_tokens, update_user, update_ts, aggregate_version, active FROM agent_definition_t WHERE host_id = ? AND agent_def_id = ?";
        Result<String> result;
        Map<String, Object> map = new HashMap<>();
        try (Connection conn = ds.getConnection(); PreparedStatement statement = conn.prepareStatement(sql)) {
            statement.setObject(1, UUID.fromString(hostId));
            statement.setObject(2, UUID.fromString(agentDefId));
            try (ResultSet resultSet = statement.executeQuery()) {
                if (resultSet.next()) {
                    map.put("hostId", resultSet.getObject("host_id", UUID.class));
                    map.put("agentDefId", resultSet.getObject("agent_def_id", UUID.class));
                    map.put("agentName", resultSet.getString("agent_name"));
                    map.put("modelProvider", resultSet.getString("model_provider"));
                    map.put("modelName", resultSet.getString("model_name"));
                    map.put("apiKeyRef", resultSet.getString("api_key_ref"));
                    map.put("temperature", resultSet.getDouble("temperature"));
                    map.put("maxTokens", resultSet.getObject("max_tokens") != null ? resultSet.getInt("max_tokens") : null);
                    map.put("updateUser", resultSet.getString("update_user"));
                    map.put("updateTs", resultSet.getObject("update_ts", OffsetDateTime.class));
                    map.put("aggregateVersion", resultSet.getLong("aggregate_version"));
                    map.put("active", resultSet.getBoolean("active"));
                    result = Success.of(JsonMapper.toJson(map));
                } else {
                    result = Failure.of(new Status(OBJECT_NOT_FOUND, "agent_definition", agentDefId));
                }
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
    public void createWorkflowDefinition(Connection conn, Map<String, Object> event) throws PortalPersistenceException {
        final String sql =
                """
                INSERT INTO wf_definition_t (host_id, wf_def_id, namespace, name, version, definition, update_user, update_ts, aggregate_version, active)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, TRUE)
                ON CONFLICT (host_id, wf_def_id) DO UPDATE
                SET namespace = EXCLUDED.namespace,
                    name = EXCLUDED.name,
                    version = EXCLUDED.version,
                    definition = EXCLUDED.definition,
                    update_user = EXCLUDED.update_user,
                    update_ts = EXCLUDED.update_ts,
                    aggregate_version = EXCLUDED.aggregate_version,
                    active = TRUE
                WHERE wf_definition_t.aggregate_version < EXCLUDED.aggregate_version
                AND wf_definition_t.active = FALSE
                """;
        Map<String, Object> map = SqlUtil.extractEventData(event);
        String wfDefId = (String)map.get("wfDefId");
        String hostId = (String)map.get("hostId");
        long newAggregateVersion = SqlUtil.getNewAggregateVersion(event);

        try (PreparedStatement statement = conn.prepareStatement(sql)) {
            statement.setObject(1, UUID.fromString(hostId));
            statement.setObject(2, UUID.fromString(wfDefId));
            statement.setString(3, (String)map.get("namespace"));
            statement.setString(4, (String)map.get("name"));
            statement.setString(5, (String)map.get("version"));
            statement.setString(6, (String)map.get("definition"));
            statement.setString(7, (String)event.get(Constants.USER));
            statement.setObject(8, OffsetDateTime.parse((String)event.get(CloudEventV1.TIME)));
            statement.setLong(9, newAggregateVersion);

            int count = statement.executeUpdate();
            if (count == 0) {
                logger.warn("Creation skipped for hostId {} wfDefId {} aggregateVersion {}. A newer or same version already exists.", hostId, wfDefId, newAggregateVersion);
            }
        } catch (SQLException e) {
            logger.error("SQLException during createWorkflowDefinition for hostId {} wfDefId {} aggregateVersion {}: {}", hostId, wfDefId, newAggregateVersion, e.getMessage(), e);
            throw new PortalPersistenceException("Persistence Error", e);
        } catch (Exception e) {
            logger.error("Exception during createWorkflowDefinition for hostId {} wfDefId {} aggregateVersion {}: {}", hostId, wfDefId, newAggregateVersion, e.getMessage(), e);
            throw new PortalPersistenceException("Persistence Error", e);
        }
    }

    @Override
    public void updateWorkflowDefinition(Connection conn, Map<String, Object> event) throws PortalPersistenceException {

        final String sql =
                """
                UPDATE wf_definition_t
                SET namespace=?, name=?, version=?, definition=?, update_user=?, update_ts=?, aggregate_version=?, active=TRUE
                WHERE host_id=? AND wf_def_id=? AND aggregate_version < ?
                """;
        Map<String, Object> map = SqlUtil.extractEventData(event);
        String wfDefId = (String)map.get("wfDefId");
        String hostId = (String)map.get("hostId");
        long newAggregateVersion = SqlUtil.getNewAggregateVersion(event);

        try (PreparedStatement statement = conn.prepareStatement(sql)) {
            statement.setString(1, (String)map.get("namespace"));
            statement.setString(2, (String)map.get("name"));
            statement.setString(3, (String)map.get("version"));
            statement.setString(4, (String)map.get("definition"));
            statement.setString(5, (String)event.get(Constants.USER));
            statement.setObject(6, OffsetDateTime.parse((String)event.get(CloudEventV1.TIME)));
            statement.setLong(7, newAggregateVersion);
            statement.setObject(8, UUID.fromString(hostId));
            statement.setObject(9, UUID.fromString(wfDefId));
            statement.setLong(10, newAggregateVersion);

            int count = statement.executeUpdate();
            if (count == 0) {
                logger.warn("Update skipped for hostId {} wfDefId {} aggregateVersion {}. Record not found or a newer/same version already exists.", hostId, wfDefId, newAggregateVersion);
            }
        } catch (SQLException e) {
            logger.error("SQLException during updateWorkflowDefinition for hostId {} wfDefId {} aggregateVersion {}: {}", hostId, wfDefId, newAggregateVersion, e.getMessage(), e);
            throw new PortalPersistenceException("Persistence Error", e);
        } catch (Exception e) {
            logger.error("Exception during updateWorkflowDefinition for hostId {} wfDefId {} aggregateVersion {}: {}", hostId, wfDefId, newAggregateVersion, e.getMessage(), e);
            throw new PortalPersistenceException("Persistence Error", e);
        }
    }

    @Override
    public void deleteWorkflowDefinition(Connection conn, Map<String, Object> event) throws PortalPersistenceException {
        final String sql =
                """
                UPDATE wf_definition_t
                SET active = FALSE,
                    update_user = ?,
                    update_ts = ?,
                    aggregate_version = ?
                WHERE host_id=? AND wf_def_id=? AND aggregate_version < ?
                """;
        Map<String, Object> map = SqlUtil.extractEventData(event);
        String wfDefId = (String)map.get("wfDefId");
        String hostId = (String)map.get("hostId");
        long newAggregateVersion = SqlUtil.getNewAggregateVersion(event);

        try (PreparedStatement statement = conn.prepareStatement(sql)) {
            statement.setString(1, (String)event.get(Constants.USER));
            statement.setObject(2, OffsetDateTime.parse((String)event.get(CloudEventV1.TIME)));
            statement.setLong(3, newAggregateVersion);
            statement.setObject(4, UUID.fromString(hostId));
            statement.setObject(5, UUID.fromString(wfDefId));
            statement.setLong(6, newAggregateVersion);

            int count = statement.executeUpdate();
            if (count == 0) {
                logger.warn("Delete skipped for hostId {} wfDefId {} aggregateVersion {}. Record not found or a newer/same version already exists.", hostId, wfDefId, newAggregateVersion);
            }
        } catch (SQLException e) {
            logger.error("SQLException during deleteWorkflowDefinition for hostId {} wfDefId {} aggregateVersion {}: {}", hostId, wfDefId, newAggregateVersion, e.getMessage(), e);
            throw new PortalPersistenceException("Persistence Error", e);
        } catch (Exception e) {
            logger.error("Exception during deleteWorkflowDefinition for hostId {} wfDefId {} aggregateVersion {}: {}", hostId, wfDefId, newAggregateVersion, e.getMessage(), e);
            throw new PortalPersistenceException("Persistence Error", e);
        }
    }

    @Override
    public Result<String> queryWorkflowDefinition(int offset, int limit, String filtersJson, String globalFilter, String sortingJson, boolean active, String hostId) {
        Result<String> result;
        List<Map<String, Object>> filters = parseJsonList(filtersJson);
        List<Map<String, Object>> sorting = parseJsonList(sortingJson);
        String s =
                """
                SELECT COUNT(*) OVER () AS total, host_id, wf_def_id, namespace,
                name, version, definition, update_user,
                update_ts, aggregate_version, active
                FROM wf_definition_t WHERE host_id = ?
                """;
        List<Object> parameters = new ArrayList<>();
        parameters.add(UUID.fromString(hostId));
        String activeClause = SqlUtil.buildMultiTableActiveClause(active);
        String[] searchColumns = {"namespace", "name", "version"};
        String sqlBuilder = s + activeClause +
                dynamicFilter(Arrays.asList("host_id"), Arrays.asList(searchColumns), filters, null, parameters) +
                globalFilter(globalFilter, searchColumns, parameters) +
                dynamicSorting("name", sorting, null) +
                "\nLIMIT ? OFFSET ?";
        parameters.add(limit);
        parameters.add(offset);
        if(logger.isTraceEnabled()) logger.trace("queryWorkflowDefinition sql: {}", sqlBuilder);
        int total = 0;
        List<Map<String, Object>> workflows = new ArrayList<>();
        try (final Connection conn = ds.getConnection(); PreparedStatement preparedStatement = conn.prepareStatement(sqlBuilder)) {
            populateParameters(preparedStatement, parameters);
            boolean isFirstRow = true;
            try (ResultSet resultSet = preparedStatement.executeQuery()) {
                while (resultSet.next()) {
                    Map<String, Object> map = new HashMap<>();
                    if (isFirstRow) {
                        total = resultSet.getInt("total");
                        isFirstRow = false;
                    }
                    map.put("hostId", resultSet.getObject("host_id", UUID.class));
                    map.put("wfDefId", resultSet.getObject("wf_def_id", UUID.class));
                    map.put("namespace", resultSet.getString("namespace"));
                    map.put("name", resultSet.getString("name"));
                    map.put("version", resultSet.getString("version"));
                    map.put("definition", resultSet.getString("definition"));
                    map.put("updateUser", resultSet.getString("update_user"));
                    map.put("updateTs", resultSet.getObject("update_ts", OffsetDateTime.class));
                    map.put("aggregateVersion", resultSet.getLong("aggregate_version"));
                    map.put("active", resultSet.getBoolean("active"));
                    workflows.add(map);
                }
            }
            Map<String, Object> resultMap = new HashMap<>();
            resultMap.put("total", total);
            resultMap.put("workflows", workflows);
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
    public Result<String> getWorkflowDefinitionById(String hostId, String wfDefId) {
        final String sql = "SELECT host_id, wf_def_id, namespace, name, version, definition, update_user, update_ts, aggregate_version, active FROM wf_definition_t WHERE host_id = ? AND wf_def_id = ?";
        Result<String> result;
        Map<String, Object> map = new HashMap<>();
        try (Connection conn = ds.getConnection(); PreparedStatement statement = conn.prepareStatement(sql)) {
            statement.setObject(1, UUID.fromString(hostId));
            statement.setObject(2, UUID.fromString(wfDefId));
            try (ResultSet resultSet = statement.executeQuery()) {
                if (resultSet.next()) {
                    map.put("hostId", resultSet.getObject("host_id", UUID.class));
                    map.put("wfDefId", resultSet.getObject("wf_def_id", UUID.class));
                    map.put("namespace", resultSet.getString("namespace"));
                    map.put("name", resultSet.getString("name"));
                    map.put("version", resultSet.getString("version"));
                    map.put("definition", resultSet.getString("definition"));
                    map.put("updateUser", resultSet.getString("update_user"));
                    map.put("updateTs", resultSet.getObject("update_ts", OffsetDateTime.class));
                    map.put("aggregateVersion", resultSet.getLong("aggregate_version"));
                    map.put("active", resultSet.getBoolean("active"));
                    result = Success.of(JsonMapper.toJson(map));
                } else {
                    result = Failure.of(new Status(OBJECT_NOT_FOUND, "workflow_definition", wfDefId));
                }
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
    public void createWorklist(Connection conn, Map<String, Object> event) throws PortalPersistenceException {
        final String sql =
                """
                INSERT INTO worklist_t (host_id, assignee_id, category_id, status_code, app_id, update_user, update_ts, aggregate_version, active)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, TRUE)
                ON CONFLICT (host_id, assignee_id, category_id) DO UPDATE
                SET status_code = EXCLUDED.status_code,
                    app_id = EXCLUDED.app_id,
                    update_user = EXCLUDED.update_user,
                    update_ts = EXCLUDED.update_ts,
                    aggregate_version = EXCLUDED.aggregate_version,
                    active = TRUE
                WHERE worklist_t.aggregate_version < EXCLUDED.aggregate_version
                AND worklist_t.active = FALSE
                """;
        Map<String, Object> map = SqlUtil.extractEventData(event);
        String hostId = (String)map.get("hostId");
        String assigneeId = (String)map.get("assigneeId");
        String categoryId = (String)map.get("categoryId");
        long newAggregateVersion = SqlUtil.getNewAggregateVersion(event);

        try (PreparedStatement statement = conn.prepareStatement(sql)) {
            statement.setObject(1, UUID.fromString(hostId));
            statement.setString(2, assigneeId);
            statement.setString(3, categoryId);

            if (map.get("statusCode") != null) {
                statement.setString(4, (String)map.get("statusCode"));
            } else {
                statement.setString(4, "Active");
            }
            if (map.get("appId") != null) {
                statement.setString(5, (String)map.get("appId"));
            } else {
                statement.setString(5, "global");
            }

            statement.setString(6, (String)event.get(Constants.USER));
            statement.setObject(7, OffsetDateTime.parse((String)event.get(CloudEventV1.TIME)));
            statement.setLong(8, newAggregateVersion);

            int count = statement.executeUpdate();
            if (count == 0) {
                logger.warn("Creation skipped for hostId {} assigneeId {} categoryId {} aggregateVersion {}. A newer or same version already exists.", hostId, assigneeId, categoryId, newAggregateVersion);
            }
        } catch (SQLException e) {
            logger.error("SQLException during createWorklist for hostId {} assigneeId {} categoryId {} aggregateVersion {}: {}", hostId, assigneeId, categoryId, newAggregateVersion, e.getMessage(), e);
            throw new PortalPersistenceException("Persistence Error", e);
        } catch (Exception e) {
            logger.error("Exception during createWorklist for hostId {} assigneeId {} categoryId {} aggregateVersion {}: {}", hostId, assigneeId, categoryId, newAggregateVersion, e.getMessage(), e);
            throw new PortalPersistenceException("Persistence Error", e);
        }
    }

    @Override
    public void updateWorklist(Connection conn, Map<String, Object> event) throws PortalPersistenceException {
        final String sql =
                """
                UPDATE worklist_t
                SET status_code=?, app_id=?, update_user=?, update_ts=?, aggregate_version=?, active=TRUE
                WHERE host_id=? AND assignee_id=? AND category_id=? AND aggregate_version < ?
                """;
        Map<String, Object> map = SqlUtil.extractEventData(event);
        String hostId = (String)map.get("hostId");
        String assigneeId = (String)map.get("assigneeId");
        String categoryId = (String)map.get("categoryId");
        long newAggregateVersion = SqlUtil.getNewAggregateVersion(event);

        try (PreparedStatement statement = conn.prepareStatement(sql)) {
            if (map.get("statusCode") != null) {
                statement.setString(1, (String)map.get("statusCode"));
            } else {
                statement.setString(1, "Active");
            }
            if (map.get("appId") != null) {
                statement.setString(2, (String)map.get("appId"));
            } else {
                statement.setString(2, "global");
            }
            statement.setString(3, (String)event.get(Constants.USER));
            statement.setObject(4, OffsetDateTime.parse((String)event.get(CloudEventV1.TIME)));
            statement.setLong(5, newAggregateVersion);
            statement.setObject(6, UUID.fromString(hostId));
            statement.setString(7, assigneeId);
            statement.setString(8, categoryId);
            statement.setLong(9, newAggregateVersion);

            int count = statement.executeUpdate();
            if (count == 0) {
                logger.warn("Update skipped for hostId {} assigneeId {} categoryId {} aggregateVersion {}. Record not found or a newer/same version already exists.", hostId, assigneeId, categoryId, newAggregateVersion);
            }
        } catch (SQLException e) {
            logger.error("SQLException during updateWorklist for hostId {} assigneeId {} categoryId {} aggregateVersion {}: {}", hostId, assigneeId, categoryId, newAggregateVersion, e.getMessage(), e);
            throw new PortalPersistenceException("Persistence Error", e);
        } catch (Exception e) {
            logger.error("Exception during updateWorklist for hostId {} assigneeId {} categoryId {} aggregateVersion {}: {}", hostId, assigneeId, categoryId, newAggregateVersion, e.getMessage(), e);
            throw new PortalPersistenceException("Persistence Error", e);
        }
    }

    @Override
    public void deleteWorklist(Connection conn, Map<String, Object> event) throws PortalPersistenceException {


        final String sql =
                """
                UPDATE worklist_t
                SET active = FALSE,
                    update_user = ?,
                    update_ts = ?,
                    aggregate_version = ?
                WHERE host_id=? AND assignee_id=? AND category_id=? AND aggregate_version < ?
                """;
        Map<String, Object> map = SqlUtil.extractEventData(event);
        String hostId = (String)map.get("hostId");
        String assigneeId = (String)map.get("assigneeId");
        String categoryId = (String)map.get("categoryId");
        long newAggregateVersion = SqlUtil.getNewAggregateVersion(event);

        try (PreparedStatement statement = conn.prepareStatement(sql)) {
            statement.setString(1, (String)event.get(Constants.USER));
            statement.setObject(2, OffsetDateTime.parse((String)event.get(CloudEventV1.TIME)));
            statement.setLong(3, newAggregateVersion);
            statement.setObject(4, UUID.fromString(hostId));
            statement.setString(5, assigneeId);
            statement.setString(6, categoryId);
            statement.setLong(7, newAggregateVersion);

            int count = statement.executeUpdate();
            if (count == 0) {
                logger.warn("Delete skipped for hostId {} assigneeId {} categoryId {} aggregateVersion {}. Record not found or a newer/same version already exists.", hostId, assigneeId, categoryId, newAggregateVersion);
            }
        } catch (SQLException e) {
            logger.error("SQLException during deleteWorklist for hostId {} assigneeId {} categoryId {} aggregateVersion {}: {}", hostId, assigneeId, categoryId, newAggregateVersion, e.getMessage(), e);
            throw new PortalPersistenceException("Persistence Error", e);
        } catch (Exception e) {
            logger.error("Exception during deleteWorklist for hostId {} assigneeId {} categoryId {} aggregateVersion {}: {}", hostId, assigneeId, categoryId, newAggregateVersion, e.getMessage(), e);
            throw new PortalPersistenceException("Persistence Error", e);
        }
    }

    @Override
    public Result<String> queryWorklist(int offset, int limit, String filtersJson, String globalFilter, String sortingJson, boolean active, String hostId) {
        Result<String> result;
        List<Map<String, Object>> filters = parseJsonList(filtersJson);
        List<Map<String, Object>> sorting = parseJsonList(sortingJson);
        String s =
                """
                SELECT COUNT(*) OVER () AS total, host_id, assignee_id, category_id,
                status_code, app_id, update_user,
                update_ts, aggregate_version, active
                FROM worklist_t WHERE host_id = ?
                """;
        List<Object> parameters = new ArrayList<>();
        parameters.add(UUID.fromString(hostId));
        String activeClause = SqlUtil.buildMultiTableActiveClause(active);
        String[] searchColumns = {"assignee_id", "category_id", "status_code", "app_id"};
        String sqlBuilder = s + activeClause +
                dynamicFilter(Arrays.asList("host_id"), Arrays.asList(searchColumns), filters, null, parameters) +
                globalFilter(globalFilter, searchColumns, parameters) +
                dynamicSorting("assignee_id", sorting, null) +
                "\nLIMIT ? OFFSET ?";
        parameters.add(limit);
        parameters.add(offset);
        if(logger.isTraceEnabled()) logger.trace("queryWorklist sql: {}", sqlBuilder);
        int total = 0;
        List<Map<String, Object>> worklists = new ArrayList<>();
        try (final Connection conn = ds.getConnection(); PreparedStatement preparedStatement = conn.prepareStatement(sqlBuilder)) {
            populateParameters(preparedStatement, parameters);
            boolean isFirstRow = true;
            try (ResultSet resultSet = preparedStatement.executeQuery()) {
                while (resultSet.next()) {
                    Map<String, Object> map = new HashMap<>();
                    if (isFirstRow) {
                        total = resultSet.getInt("total");
                        isFirstRow = false;
                    }
                    map.put("hostId", resultSet.getObject("host_id", UUID.class));
                    map.put("assigneeId", resultSet.getString("assignee_id"));
                    map.put("categoryId", resultSet.getString("category_id"));
                    map.put("statusCode", resultSet.getString("status_code"));
                    map.put("appId", resultSet.getString("app_id"));
                    map.put("updateUser", resultSet.getString("update_user"));
                    map.put("updateTs", resultSet.getObject("update_ts", OffsetDateTime.class));
                    map.put("aggregateVersion", resultSet.getLong("aggregate_version"));
                    map.put("active", resultSet.getBoolean("active"));
                    worklists.add(map);
                }
            }
            Map<String, Object> resultMap = new HashMap<>();
            resultMap.put("total", total);
            resultMap.put("worklists", worklists);
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
    public Result<String> getWorklistById(String hostId, String assigneeId, String categoryId) {
        final String sql = "SELECT host_id, assignee_id, category_id, status_code, app_id, update_user, update_ts, aggregate_version, active FROM worklist_t WHERE host_id = ? AND assignee_id = ? AND category_id = ?";
        Result<String> result;
        Map<String, Object> map = new HashMap<>();
        try (Connection conn = ds.getConnection(); PreparedStatement statement = conn.prepareStatement(sql)) {
            statement.setObject(1, UUID.fromString(hostId));
            statement.setString(2, assigneeId);
            statement.setString(3, categoryId);
            try (ResultSet resultSet = statement.executeQuery()) {
                if (resultSet.next()) {
                    map.put("hostId", resultSet.getObject("host_id", UUID.class));
                    map.put("assigneeId", resultSet.getString("assignee_id"));
                    map.put("categoryId", resultSet.getString("category_id"));
                    map.put("statusCode", resultSet.getString("status_code"));
                    map.put("appId", resultSet.getString("app_id"));
                    map.put("updateUser", resultSet.getString("update_user"));
                    map.put("updateTs", resultSet.getObject("update_ts", OffsetDateTime.class));
                    map.put("aggregateVersion", resultSet.getLong("aggregate_version"));
                    map.put("active", resultSet.getBoolean("active"));
                    result = Success.of(JsonMapper.toJson(map));
                } else {
                    result = Failure.of(new Status(OBJECT_NOT_FOUND, "worklist", assigneeId + "|" + categoryId));
                }
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
    public void createWorklistColumn(Connection conn, Map<String, Object> event) throws PortalPersistenceException {
        final String sql =
                """
                INSERT INTO worklist_column_t (host_id, assignee_id, category_id, sequence_id, column_id, update_user, update_ts, aggregate_version, active)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, TRUE)
                ON CONFLICT (host_id, assignee_id, category_id, sequence_id) DO UPDATE
                SET column_id = EXCLUDED.column_id,
                    update_user = EXCLUDED.update_user,
                    update_ts = EXCLUDED.update_ts,
                    aggregate_version = EXCLUDED.aggregate_version,
                    active = TRUE
                WHERE worklist_column_t.aggregate_version < EXCLUDED.aggregate_version
                AND worklist_column_t.active = FALSE
                """;
        Map<String, Object> map = SqlUtil.extractEventData(event);
        String hostId = (String)map.get("hostId");
        String assigneeId = (String)map.get("assigneeId");
        String categoryId = (String)map.get("categoryId");
        Integer sequenceId = (Integer)map.get("sequenceId");
        long newAggregateVersion = SqlUtil.getNewAggregateVersion(event);

        try (PreparedStatement statement = conn.prepareStatement(sql)) {
            statement.setObject(1, UUID.fromString(hostId));
            statement.setString(2, assigneeId);
            statement.setString(3, categoryId);
            statement.setInt(4, sequenceId);
            statement.setString(5, (String)map.get("columnId"));
            statement.setString(6, (String)event.get(Constants.USER));
            statement.setObject(7, OffsetDateTime.parse((String)event.get(CloudEventV1.TIME)));
            statement.setLong(8, newAggregateVersion);

            int count = statement.executeUpdate();
            if (count == 0) {
                logger.warn("Creation skipped for hostId {} assigneeId {} categoryId {} sequenceId {} aggregateVersion {}. A newer or same version already exists.", hostId, assigneeId, categoryId, sequenceId, newAggregateVersion);
            }
        } catch (SQLException e) {
            logger.error("SQLException during createWorklistColumn for hostId {} assigneeId {} categoryId {} sequenceId {} aggregateVersion {}: {}", hostId, assigneeId, categoryId, sequenceId, newAggregateVersion, e.getMessage(), e);
            throw new PortalPersistenceException("Persistence Error", e);
        } catch (Exception e) {
            logger.error("Exception during createWorklistColumn for hostId {} assigneeId {} categoryId {} sequenceId {} aggregateVersion {}: {}", hostId, assigneeId, categoryId, sequenceId, newAggregateVersion, e.getMessage(), e);
            throw new PortalPersistenceException("Persistence Error", e);
        }
    }

    @Override
    public void updateWorklistColumn(Connection conn, Map<String, Object> event) throws PortalPersistenceException {
        final String sql =
                """
                UPDATE worklist_column_t
                SET column_id=?, update_user=?, update_ts=?, aggregate_version=?, active=TRUE
                WHERE host_id=? AND assignee_id=? AND category_id=? AND sequence_id=? AND aggregate_version < ?
                """;
        Map<String, Object> map = SqlUtil.extractEventData(event);
        String hostId = (String)map.get("hostId");
        String assigneeId = (String)map.get("assigneeId");
        String categoryId = (String)map.get("categoryId");
        Integer sequenceId = (Integer)map.get("sequenceId");
        long newAggregateVersion = SqlUtil.getNewAggregateVersion(event);

        try (PreparedStatement statement = conn.prepareStatement(sql)) {
            statement.setString(1, (String)map.get("columnId"));
            statement.setString(2, (String)event.get(Constants.USER));
            statement.setObject(3, OffsetDateTime.parse((String)event.get(CloudEventV1.TIME)));
            statement.setLong(4, newAggregateVersion);
            statement.setObject(5, UUID.fromString(hostId));
            statement.setString(6, assigneeId);
            statement.setString(7, categoryId);
            statement.setInt(8, sequenceId);
            statement.setLong(9, newAggregateVersion);

            int count = statement.executeUpdate();
            if (count == 0) {
                logger.warn("Update skipped for hostId {} assigneeId {} categoryId {} sequenceId {} aggregateVersion {}. Record not found or a newer/same version already exists.", hostId, assigneeId, categoryId, sequenceId, newAggregateVersion);
            }
        } catch (SQLException e) {
            logger.error("SQLException during updateWorklistColumn for hostId {} assigneeId {} categoryId {} sequenceId {} aggregateVersion {}: {}", hostId, assigneeId, categoryId, sequenceId, newAggregateVersion, e.getMessage(), e);
            throw new PortalPersistenceException("Persistence Error", e);
        } catch (Exception e) {
            logger.error("Exception during updateWorklistColumn for hostId {} assigneeId {} categoryId {} sequenceId {} aggregateVersion {}: {}", hostId, assigneeId, categoryId, sequenceId, newAggregateVersion, e.getMessage(), e);
            throw new PortalPersistenceException("Persistence Error", e);
        }
    }

    @Override
    public void deleteWorklistColumn(Connection conn, Map<String, Object> event) throws PortalPersistenceException {
        final String sql =
                """
                UPDATE worklist_column_t
                SET active = FALSE,
                    update_user = ?,
                    update_ts = ?,
                    aggregate_version = ?
                WHERE host_id=? AND assignee_id=? AND category_id=? AND sequence_id=? AND aggregate_version < ?
                """;
        Map<String, Object> map = SqlUtil.extractEventData(event);
        String hostId = (String)map.get("hostId");
        String assigneeId = (String)map.get("assigneeId");
        String categoryId = (String)map.get("categoryId");
        Integer sequenceId = (Integer)map.get("sequenceId");
        long newAggregateVersion = SqlUtil.getNewAggregateVersion(event);

        try (PreparedStatement statement = conn.prepareStatement(sql)) {
            statement.setString(1, (String)event.get(Constants.USER));
            statement.setObject(2, OffsetDateTime.parse((String)event.get(CloudEventV1.TIME)));
            statement.setLong(3, newAggregateVersion);
            statement.setObject(4, UUID.fromString(hostId));
            statement.setString(5, assigneeId);
            statement.setString(6, categoryId);
            statement.setInt(7, sequenceId);
            statement.setLong(8, newAggregateVersion);

            int count = statement.executeUpdate();
            if (count == 0) {
                logger.warn("Delete skipped for hostId {} assigneeId {} categoryId {} sequenceId {} aggregateVersion {}. Record not found or a newer/same version already exists.", hostId, assigneeId, categoryId, sequenceId, newAggregateVersion);
            }
        } catch (SQLException e) {
            logger.error("SQLException during deleteWorklistColumn for hostId {} assigneeId {} categoryId {} sequenceId {} aggregateVersion {}: {}", hostId, assigneeId, categoryId, sequenceId, newAggregateVersion, e.getMessage(), e);
            throw new PortalPersistenceException("Persistence Error", e);
        } catch (Exception e) {
            logger.error("Exception during deleteWorklistColumn for hostId {} assigneeId {} categoryId {} sequenceId {} aggregateVersion {}: {}", hostId, assigneeId, categoryId, sequenceId, newAggregateVersion, e.getMessage(), e);
            throw new PortalPersistenceException("Persistence Error", e);
        }
    }

    @Override
    public Result<String> queryWorklistColumn(int offset, int limit, String filtersJson, String globalFilter, String sortingJson, boolean active, String hostId) {
        Result<String> result;
        List<Map<String, Object>> filters = parseJsonList(filtersJson);
        List<Map<String, Object>> sorting = parseJsonList(sortingJson);
        String s =
                """
                SELECT COUNT(*) OVER () AS total, host_id, assignee_id, category_id, sequence_id,
                column_id, update_user,
                update_ts, aggregate_version, active
                FROM worklist_column_t WHERE host_id = ?
                """;
        List<Object> parameters = new ArrayList<>();
        parameters.add(UUID.fromString(hostId));
        String activeClause = SqlUtil.buildMultiTableActiveClause(active);
        String[] searchColumns = {"assignee_id", "category_id", "column_id"};
        String sqlBuilder = s + activeClause +
                dynamicFilter(Arrays.asList("host_id"), Arrays.asList(searchColumns), filters, null, parameters) +
                globalFilter(globalFilter, searchColumns, parameters) +
                dynamicSorting("sequence_id", sorting, null) +
                "\nLIMIT ? OFFSET ?";
        parameters.add(limit);
        parameters.add(offset);
        if(logger.isTraceEnabled()) logger.trace("queryWorklistColumn sql: {}", sqlBuilder);
        int total = 0;
        List<Map<String, Object>> worklistColumns = new ArrayList<>();
        try (final Connection conn = ds.getConnection(); PreparedStatement preparedStatement = conn.prepareStatement(sqlBuilder)) {
            populateParameters(preparedStatement, parameters);
            boolean isFirstRow = true;
            try (ResultSet resultSet = preparedStatement.executeQuery()) {
                while (resultSet.next()) {
                    Map<String, Object> map = new HashMap<>();
                    if (isFirstRow) {
                        total = resultSet.getInt("total");
                        isFirstRow = false;
                    }
                    map.put("hostId", resultSet.getObject("host_id", UUID.class));
                    map.put("assigneeId", resultSet.getString("assignee_id"));
                    map.put("categoryId", resultSet.getString("category_id"));
                    map.put("sequenceId", resultSet.getInt("sequence_id"));
                    map.put("columnId", resultSet.getString("column_id"));
                    map.put("updateUser", resultSet.getString("update_user"));
                    map.put("updateTs", resultSet.getObject("update_ts", OffsetDateTime.class));
                    map.put("aggregateVersion", resultSet.getLong("aggregate_version"));
                    map.put("active", resultSet.getBoolean("active"));
                    worklistColumns.add(map);
                }
            }
            Map<String, Object> resultMap = new HashMap<>();
            resultMap.put("total", total);
            resultMap.put("worklistColumns", worklistColumns);
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
    public Result<String> getWorklistColumnById(String hostId, String assigneeId, String categoryId, int sequenceId) {
        final String sql = "SELECT host_id, assignee_id, category_id, sequence_id, column_id, update_user, update_ts, aggregate_version, active FROM worklist_column_t WHERE host_id = ? AND assignee_id = ? AND category_id = ? AND sequence_id = ?";
        Result<String> result;
        Map<String, Object> map = new HashMap<>();
        try (Connection conn = ds.getConnection(); PreparedStatement statement = conn.prepareStatement(sql)) {
            statement.setObject(1, UUID.fromString(hostId));
            statement.setString(2, assigneeId);
            statement.setString(3, categoryId);
            statement.setInt(4, sequenceId);
            try (ResultSet resultSet = statement.executeQuery()) {
                if (resultSet.next()) {
                    map.put("hostId", resultSet.getObject("host_id", UUID.class));
                    map.put("assigneeId", resultSet.getString("assignee_id"));
                    map.put("categoryId", resultSet.getString("category_id"));
                    map.put("sequenceId", resultSet.getInt("sequence_id"));
                    map.put("columnId", resultSet.getString("column_id"));
                    map.put("updateUser", resultSet.getString("update_user"));
                    map.put("updateTs", resultSet.getObject("update_ts", OffsetDateTime.class));
                    map.put("aggregateVersion", resultSet.getLong("aggregate_version"));
                    map.put("active", resultSet.getBoolean("active"));
                    result = Success.of(JsonMapper.toJson(map));
                } else {
                    result = Failure.of(new Status(OBJECT_NOT_FOUND, "worklist_column", assigneeId + "|" + categoryId + "|" + sequenceId));
                }
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
    public void createProcessInfo(Connection conn, Map<String, Object> event) throws PortalPersistenceException {
        final String sql =
                """
                INSERT INTO process_info_t (host_id, process_id, wf_def_id, wf_instance_id, app_id, process_type, status_code, started_ts, ex_trigger_ts, custom_status_code, completed_ts, result_code, source_id, branch_code, rr_code, party_id, party_name, counter_party_id, counter_party_name, txn_id, txn_name, product_id, product_name, product_type, group_name, subgroup_name, event_start_ts, event_end_ts, event_other_ts, event_other, risk, risk_scale, price, price_scale, product_qy, currency_code, ex_ref_id, ex_ref_code, product_qy_scale, parent_process_id, deadline_ts, parent_group_id, process_subtype_code, owning_group_name, update_user, update_ts, aggregate_version, active)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, TRUE)
                ON CONFLICT (host_id, process_id) DO UPDATE
                SET wf_def_id = EXCLUDED.wf_def_id,
                    wf_instance_id = EXCLUDED.wf_instance_id,
                    app_id = EXCLUDED.app_id,
                    process_type = EXCLUDED.process_type,
                    status_code = EXCLUDED.status_code,
                    started_ts = EXCLUDED.started_ts,
                    ex_trigger_ts = EXCLUDED.ex_trigger_ts,
                    custom_status_code = EXCLUDED.custom_status_code,
                    completed_ts = EXCLUDED.completed_ts,
                    result_code = EXCLUDED.result_code,
                    source_id = EXCLUDED.source_id,
                    branch_code = EXCLUDED.branch_code,
                    rr_code = EXCLUDED.rr_code,
                    party_id = EXCLUDED.party_id,
                    party_name = EXCLUDED.party_name,
                    counter_party_id = EXCLUDED.counter_party_id,
                    counter_party_name = EXCLUDED.counter_party_name,
                    txn_id = EXCLUDED.txn_id,
                    txn_name = EXCLUDED.txn_name,
                    product_id = EXCLUDED.product_id,
                    product_name = EXCLUDED.product_name,
                    product_type = EXCLUDED.product_type,
                    group_name = EXCLUDED.group_name,
                    subgroup_name = EXCLUDED.subgroup_name,
                    event_start_ts = EXCLUDED.event_start_ts,
                    event_end_ts = EXCLUDED.event_end_ts,
                    event_other_ts = EXCLUDED.event_other_ts,
                    event_other = EXCLUDED.event_other,
                    risk = EXCLUDED.risk,
                    risk_scale = EXCLUDED.risk_scale,
                    price = EXCLUDED.price,
                    price_scale = EXCLUDED.price_scale,
                    product_qy = EXCLUDED.product_qy,
                    currency_code = EXCLUDED.currency_code,
                    ex_ref_id = EXCLUDED.ex_ref_id,
                    ex_ref_code = EXCLUDED.ex_ref_code,
                    product_qy_scale = EXCLUDED.product_qy_scale,
                    parent_process_id = EXCLUDED.parent_process_id,
                    deadline_ts = EXCLUDED.deadline_ts,
                    parent_group_id = EXCLUDED.parent_group_id,
                    process_subtype_code = EXCLUDED.process_subtype_code,
                    owning_group_name = EXCLUDED.owning_group_name,
                    update_user = EXCLUDED.update_user,
                    update_ts = EXCLUDED.update_ts,
                    aggregate_version = EXCLUDED.aggregate_version,
                    active = TRUE
                WHERE process_info_t.aggregate_version < EXCLUDED.aggregate_version
                AND process_info_t.active = FALSE
                """;
        Map<String, Object> map = SqlUtil.extractEventData(event);
        String hostId = (String)map.get("hostId");
        String processId = (String)map.get("processId");
        long newAggregateVersion = SqlUtil.getNewAggregateVersion(event);

        try (PreparedStatement statement = conn.prepareStatement(sql)) {
            int i = 1;
            statement.setObject(i++, UUID.fromString(hostId));
            statement.setObject(i++, UUID.fromString(processId));
            statement.setObject(i++, UUID.fromString((String)map.get("wfDefId")));
            statement.setString(i++, (String)map.get("wfInstanceId"));
            statement.setString(i++, (String)map.get("appId"));
            statement.setString(i++, (String)map.get("processType"));
            statement.setString(i++, (String)map.get("statusCode"));
            statement.setObject(i++, map.get("startedTs") != null ? OffsetDateTime.parse((String)map.get("startedTs")) : OffsetDateTime.now());
            statement.setObject(i++, map.get("exTriggerTs") != null ? OffsetDateTime.parse((String)map.get("exTriggerTs")) : null);
            statement.setString(i++, (String)map.get("customStatusCode"));
            statement.setObject(i++, map.get("completedTs") != null ? OffsetDateTime.parse((String)map.get("completedTs")) : null);
            statement.setString(i++, (String)map.get("resultCode"));
            statement.setString(i++, (String)map.get("sourceId"));
            statement.setString(i++, (String)map.get("branchCode"));
            statement.setString(i++, (String)map.get("rrCode"));
            statement.setString(i++, (String)map.get("partyId"));
            statement.setString(i++, (String)map.get("partyName"));
            statement.setString(i++, (String)map.get("counterPartyId"));
            statement.setString(i++, (String)map.get("counterPartyName"));
            statement.setString(i++, (String)map.get("txnId"));
            statement.setString(i++, (String)map.get("txnName"));
            statement.setString(i++, (String)map.get("productId"));
            statement.setString(i++, (String)map.get("productName"));
            statement.setString(i++, (String)map.get("productType"));
            statement.setString(i++, (String)map.get("groupName"));
            statement.setString(i++, (String)map.get("subgroupName"));
            statement.setObject(i++, map.get("eventStartTs") != null ? OffsetDateTime.parse((String)map.get("eventStartTs")) : null);
            statement.setObject(i++, map.get("eventEndTs") != null ? OffsetDateTime.parse((String)map.get("eventEndTs")) : null);
            statement.setObject(i++, map.get("eventOtherTs") != null ? OffsetDateTime.parse((String)map.get("eventOtherTs")) : null);
            statement.setString(i++, (String)map.get("eventOther"));
            setBigDecimalOrNull(statement, i++, map.get("risk"));
            setIntegerOrNull(statement, i++, map.get("riskScale"));
            setBigDecimalOrNull(statement, i++, map.get("price"));
            setIntegerOrNull(statement, i++, map.get("priceScale"));
            setBigDecimalOrNull(statement, i++, map.get("productQy"));
            statement.setString(i++, (String)map.get("currencyCode"));
            statement.setString(i++, (String)map.get("exRefId"));
            statement.setString(i++, (String)map.get("exRefCode"));
            setIntegerOrNull(statement, i++, map.get("productQyScale"));
            statement.setString(i++, (String)map.get("parentProcessId"));
            statement.setObject(i++, map.get("deadlineTs") != null ? OffsetDateTime.parse((String)map.get("deadlineTs")) : null);
            setBigDecimalOrNull(statement, i++, map.get("parentGroupId"));
            statement.setString(i++, (String)map.get("processSubtypeCode"));
            statement.setString(i++, (String)map.get("owningGroupName"));

            statement.setString(i++, (String)event.get(Constants.USER));
            statement.setObject(i++, OffsetDateTime.parse((String)event.get(CloudEventV1.TIME)));
            statement.setLong(i++, newAggregateVersion);

            int count = statement.executeUpdate();
            if (count == 0) {
                logger.warn("Creation skipped for hostId {} processId {} aggregateVersion {}. A newer or same version already exists.", hostId, processId, newAggregateVersion);
            }
        } catch (SQLException e) {
            logger.error("SQLException during createProcessInfo for hostId {} processId {} aggregateVersion {}: {}", hostId, processId, newAggregateVersion, e.getMessage(), e);
            throw new PortalPersistenceException("Persistence Error", e);
        } catch (Exception e) {
            logger.error("Exception during createProcessInfo for hostId {} processId {} aggregateVersion {}: {}", hostId, processId, newAggregateVersion, e.getMessage(), e);
            throw new PortalPersistenceException("Persistence Error", e);
        }
    }

    @Override
    public void updateProcessInfo(Connection conn, Map<String, Object> event) throws PortalPersistenceException {
        final String sql =
                """
                UPDATE process_info_t
                SET wf_def_id=?, wf_instance_id=?, app_id=?, process_type=?, status_code=?, started_ts=?, ex_trigger_ts=?, custom_status_code=?, completed_ts=?, result_code=?, source_id=?, branch_code=?, rr_code=?, party_id=?, party_name=?, counter_party_id=?, counter_party_name=?, txn_id=?, txn_name=?, product_id=?, product_name=?, product_type=?, group_name=?, subgroup_name=?, event_start_ts=?, event_end_ts=?, event_other_ts=?, event_other=?, risk=?, risk_scale=?, price=?, price_scale=?, product_qy=?, currency_code=?, ex_ref_id=?, ex_ref_code=?, product_qy_scale=?, parent_process_id=?, deadline_ts=?, parent_group_id=?, process_subtype_code=?, owning_group_name=?, update_user=?, update_ts=?, aggregate_version=?, active=TRUE
                WHERE host_id=? AND process_id=? AND aggregate_version < ?
                """;
        Map<String, Object> map = SqlUtil.extractEventData(event);
        String hostId = (String)map.get("hostId");
        String processId = (String)map.get("processId");
        long newAggregateVersion = SqlUtil.getNewAggregateVersion(event);

        try (PreparedStatement statement = conn.prepareStatement(sql)) {
            int i = 1;
            statement.setObject(i++, UUID.fromString((String)map.get("wfDefId")));
            statement.setString(i++, (String)map.get("wfInstanceId"));
            statement.setString(i++, (String)map.get("appId"));
            statement.setString(i++, (String)map.get("processType"));
            statement.setString(i++, (String)map.get("statusCode"));
            statement.setObject(i++, OffsetDateTime.parse((String)map.get("startedTs")));
            statement.setObject(i++, map.get("exTriggerTs") != null ? OffsetDateTime.parse((String)map.get("exTriggerTs")) : null);
            statement.setString(i++, (String)map.get("customStatusCode"));
            statement.setObject(i++, map.get("completedTs") != null ? OffsetDateTime.parse((String)map.get("completedTs")) : null);
            statement.setString(i++, (String)map.get("resultCode"));
            statement.setString(i++, (String)map.get("sourceId"));
            statement.setString(i++, (String)map.get("branchCode"));
            statement.setString(i++, (String)map.get("rrCode"));
            statement.setString(i++, (String)map.get("partyId"));
            statement.setString(i++, (String)map.get("partyName"));
            statement.setString(i++, (String)map.get("counterPartyId"));
            statement.setString(i++, (String)map.get("counterPartyName"));
            statement.setString(i++, (String)map.get("txnId"));
            statement.setString(i++, (String)map.get("txnName"));
            statement.setString(i++, (String)map.get("productId"));
            statement.setString(i++, (String)map.get("productName"));
            statement.setString(i++, (String)map.get("productType"));
            statement.setString(i++, (String)map.get("groupName"));
            statement.setString(i++, (String)map.get("subgroupName"));
            statement.setObject(i++, map.get("eventStartTs") != null ? OffsetDateTime.parse((String)map.get("eventStartTs")) : null);
            statement.setObject(i++, map.get("eventEndTs") != null ? OffsetDateTime.parse((String)map.get("eventEndTs")) : null);
            statement.setObject(i++, map.get("eventOtherTs") != null ? OffsetDateTime.parse((String)map.get("eventOtherTs")) : null);
            statement.setString(i++, (String)map.get("eventOther"));
            setBigDecimalOrNull(statement, i++, map.get("risk"));
            setIntegerOrNull(statement, i++, map.get("riskScale"));
            setBigDecimalOrNull(statement, i++, map.get("price"));
            setIntegerOrNull(statement, i++, map.get("priceScale"));
            setBigDecimalOrNull(statement, i++, map.get("productQy"));
            statement.setString(i++, (String)map.get("currencyCode"));
            statement.setString(i++, (String)map.get("exRefId"));
            statement.setString(i++, (String)map.get("exRefCode"));
            setIntegerOrNull(statement, i++, map.get("productQyScale"));
            statement.setString(i++, (String)map.get("parentProcessId"));
            statement.setObject(i++, map.get("deadlineTs") != null ? OffsetDateTime.parse((String)map.get("deadlineTs")) : null);
            setBigDecimalOrNull(statement, i++, map.get("parentGroupId"));
            statement.setString(i++, (String)map.get("processSubtypeCode"));
            statement.setString(i++, (String)map.get("owningGroupName"));

            statement.setString(i++, (String)event.get(Constants.USER));
            statement.setObject(i++, OffsetDateTime.parse((String)event.get(CloudEventV1.TIME)));
            statement.setLong(i++, newAggregateVersion);

            statement.setObject(i++, UUID.fromString(hostId));
            statement.setObject(i++, UUID.fromString(processId));
            statement.setLong(i++, newAggregateVersion);

            int count = statement.executeUpdate();
            if (count == 0) {
                logger.warn("Update skipped for hostId {} processId {} aggregateVersion {}. Record not found or a newer/same version already exists.", hostId, processId, newAggregateVersion);
            }
        } catch (SQLException e) {
            logger.error("SQLException during updateProcessInfo for hostId {} processId {} aggregateVersion {}: {}", hostId, processId, newAggregateVersion, e.getMessage(), e);
            throw new PortalPersistenceException("Persistence Error", e);
        } catch (Exception e) {
            logger.error("Exception during updateProcessInfo for hostId {} processId {} aggregateVersion {}: {}", hostId, processId, newAggregateVersion, e.getMessage(), e);
            throw new PortalPersistenceException("Persistence Error", e);
        }
    }

    @Override
    public void deleteProcessInfo(Connection conn, Map<String, Object> event) throws PortalPersistenceException {
        final String sql =
                """
                UPDATE process_info_t
                SET active = FALSE,
                    update_user = ?,
                    update_ts = ?,
                    aggregate_version = ?
                WHERE host_id=? AND process_id=? AND aggregate_version < ?
                """;
        Map<String, Object> map = SqlUtil.extractEventData(event);
        String hostId = (String)map.get("hostId");
        String processId = (String)map.get("processId");
        long newAggregateVersion = SqlUtil.getNewAggregateVersion(event);

        try (PreparedStatement statement = conn.prepareStatement(sql)) {
            statement.setString(1, (String)event.get(Constants.USER));
            statement.setObject(2, OffsetDateTime.parse((String)event.get(CloudEventV1.TIME)));
            statement.setLong(3, newAggregateVersion);
            statement.setObject(4, UUID.fromString(hostId));
            statement.setObject(5, UUID.fromString(processId));
            statement.setLong(6, newAggregateVersion);

            int count = statement.executeUpdate();
            if (count == 0) {
                logger.warn("Delete skipped for hostId {} processId {} aggregateVersion {}. Record not found or a newer/same version already exists.", hostId, processId, newAggregateVersion);
            }
        } catch (SQLException e) {
            logger.error("SQLException during deleteProcessInfo for hostId {} processId {} aggregateVersion {}: {}", hostId, processId, newAggregateVersion, e.getMessage(), e);
            throw new PortalPersistenceException("Persistence Error", e);
        } catch (Exception e) {
            logger.error("Exception during deleteProcessInfo for hostId {} processId {} aggregateVersion {}: {}", hostId, processId, newAggregateVersion, e.getMessage(), e);
            throw new PortalPersistenceException("Persistence Error", e);
        }
    }

    @Override
    public Result<String> queryProcessInfo(int offset, int limit, String filtersJson, String globalFilter, String sortingJson, boolean active, String hostId) {
        Result<String> result;
        List<Map<String, Object>> filters = parseJsonList(filtersJson);
        List<Map<String, Object>> sorting = parseJsonList(sortingJson);
        String s =
                """
                SELECT COUNT(*) OVER () AS total, host_id, process_id, wf_def_id, wf_instance_id, app_id, process_type, status_code, started_ts, ex_trigger_ts, custom_status_code, completed_ts, result_code, source_id, branch_code, rr_code, party_id, party_name, counter_party_id, counter_party_name, txn_id, txn_name, product_id, product_name, product_type, group_name, subgroup_name, event_start_ts, event_end_ts, event_other_ts, event_other, risk, risk_scale, price, price_scale, product_qy, currency_code, ex_ref_id, ex_ref_code, product_qy_scale, parent_process_id, deadline_ts, parent_group_id, process_subtype_code, owning_group_name, update_user, update_ts, aggregate_version, active
                FROM process_info_t WHERE host_id = ?
                """;
        List<Object> parameters = new ArrayList<>();
        parameters.add(UUID.fromString(hostId));
        String activeClause = SqlUtil.buildMultiTableActiveClause(active);
        String[] searchColumns = {"process_type", "status_code", "party_name", "product_name"};
        String sqlBuilder = s + activeClause +
                dynamicFilter(Arrays.asList("host_id"), Arrays.asList(searchColumns), filters, null, parameters) +
                globalFilter(globalFilter, searchColumns, parameters) +
                dynamicSorting("started_ts", sorting, null) +
                "\nLIMIT ? OFFSET ?";
        parameters.add(limit);
        parameters.add(offset);
        if(logger.isTraceEnabled()) logger.trace("queryProcessInfo sql: {}", sqlBuilder);
        int total = 0;
        List<Map<String, Object>> processInfos = new ArrayList<>();
        try (final Connection conn = ds.getConnection(); PreparedStatement preparedStatement = conn.prepareStatement(sqlBuilder)) {
            populateParameters(preparedStatement, parameters);
            boolean isFirstRow = true;
            try (ResultSet resultSet = preparedStatement.executeQuery()) {
                while (resultSet.next()) {
                    Map<String, Object> map = new HashMap<>();
                    if (isFirstRow) {
                        total = resultSet.getInt("total");
                        isFirstRow = false;
                    }
                    map.put("hostId", resultSet.getObject("host_id", UUID.class));
                    map.put("processId", resultSet.getObject("process_id", UUID.class));
                    map.put("wfDefId", resultSet.getObject("wf_def_id", UUID.class));
                    map.put("wfInstanceId", resultSet.getString("wf_instance_id"));
                    map.put("appId", resultSet.getString("app_id"));
                    map.put("processType", resultSet.getString("process_type"));
                    map.put("statusCode", resultSet.getString("status_code"));
                    map.put("startedTs", resultSet.getObject("started_ts", OffsetDateTime.class));
                    map.put("exTriggerTs", resultSet.getObject("ex_trigger_ts", OffsetDateTime.class));
                    map.put("customStatusCode", resultSet.getString("custom_status_code"));
                    map.put("completedTs", resultSet.getObject("completed_ts", OffsetDateTime.class));
                    map.put("resultCode", resultSet.getString("result_code"));
                    map.put("sourceId", resultSet.getString("source_id"));
                    map.put("branchCode", resultSet.getString("branch_code"));
                    map.put("rrCode", resultSet.getString("rr_code"));
                    map.put("partyId", resultSet.getString("party_id"));
                    map.put("partyName", resultSet.getString("party_name"));
                    map.put("counterPartyId", resultSet.getString("counter_party_id"));
                    map.put("counterPartyName", resultSet.getString("counter_party_name"));
                    map.put("txnId", resultSet.getString("txn_id"));
                    map.put("txnName", resultSet.getString("txn_name"));
                    map.put("productId", resultSet.getString("product_id"));
                    map.put("productName", resultSet.getString("product_name"));
                    map.put("productType", resultSet.getString("product_type"));
                    map.put("groupName", resultSet.getString("group_name"));
                    map.put("subgroupName", resultSet.getString("subgroup_name"));
                    map.put("eventStartTs", resultSet.getObject("event_start_ts", OffsetDateTime.class));
                    map.put("eventEndTs", resultSet.getObject("event_end_ts", OffsetDateTime.class));
                    map.put("eventOtherTs", resultSet.getObject("event_other_ts", OffsetDateTime.class));
                    map.put("eventOther", resultSet.getString("event_other"));
                    map.put("risk", resultSet.getBigDecimal("risk"));
                    map.put("riskScale", resultSet.getObject("risk_scale"));
                    map.put("price", resultSet.getBigDecimal("price"));
                    map.put("priceScale", resultSet.getObject("price_scale"));
                    map.put("productQy", resultSet.getBigDecimal("product_qy"));
                    map.put("currencyCode", resultSet.getString("currency_code"));
                    map.put("exRefId", resultSet.getString("ex_ref_id"));
                    map.put("exRefCode", resultSet.getString("ex_ref_code"));
                    map.put("productQyScale", resultSet.getObject("product_qy_scale"));
                    map.put("parentProcessId", resultSet.getString("parent_process_id"));
                    map.put("deadlineTs", resultSet.getObject("deadline_ts", OffsetDateTime.class));
                    map.put("parentGroupId", resultSet.getBigDecimal("parent_group_id"));
                    map.put("processSubtypeCode", resultSet.getString("process_subtype_code"));
                    map.put("owningGroupName", resultSet.getString("owning_group_name"));
                    map.put("updateUser", resultSet.getString("update_user"));
                    map.put("updateTs", resultSet.getObject("update_ts", OffsetDateTime.class));
                    map.put("aggregateVersion", resultSet.getLong("aggregate_version"));
                    map.put("active", resultSet.getBoolean("active"));
                    processInfos.add(map);
                }
            }
            Map<String, Object> resultMap = new HashMap<>();
            resultMap.put("total", total);
            resultMap.put("processInfos", processInfos);
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
    public Result<String> getProcessInfoById(String hostId, String processId) {
        final String sql = "SELECT host_id, process_id, wf_def_id, wf_instance_id, app_id, process_type, status_code, started_ts, ex_trigger_ts, custom_status_code, completed_ts, result_code, source_id, branch_code, rr_code, party_id, party_name, counter_party_id, counter_party_name, txn_id, txn_name, product_id, product_name, product_type, group_name, subgroup_name, event_start_ts, event_end_ts, event_other_ts, event_other, risk, risk_scale, price, price_scale, product_qy, currency_code, ex_ref_id, ex_ref_code, product_qy_scale, parent_process_id, deadline_ts, parent_group_id, process_subtype_code, owning_group_name, update_user, update_ts, aggregate_version, active FROM process_info_t WHERE host_id = ? AND process_id = ?";
        Result<String> result;
        Map<String, Object> map = new HashMap<>();
        try (Connection conn = ds.getConnection(); PreparedStatement statement = conn.prepareStatement(sql)) {
            statement.setObject(1, UUID.fromString(hostId));
            statement.setObject(2, UUID.fromString(processId));
            try (ResultSet resultSet = statement.executeQuery()) {
                if (resultSet.next()) {
                    map.put("hostId", resultSet.getObject("host_id", UUID.class));
                    map.put("processId", resultSet.getObject("process_id", UUID.class));
                    map.put("wfDefId", resultSet.getObject("wf_def_id", UUID.class));
                    map.put("wfInstanceId", resultSet.getString("wf_instance_id"));
                    map.put("appId", resultSet.getString("app_id"));
                    map.put("processType", resultSet.getString("process_type"));
                    map.put("statusCode", resultSet.getString("status_code"));
                    map.put("startedTs", resultSet.getObject("started_ts", OffsetDateTime.class));
                    map.put("exTriggerTs", resultSet.getObject("ex_trigger_ts", OffsetDateTime.class));
                    map.put("customStatusCode", resultSet.getString("custom_status_code"));
                    map.put("completedTs", resultSet.getObject("completed_ts", OffsetDateTime.class));
                    map.put("resultCode", resultSet.getString("result_code"));
                    map.put("sourceId", resultSet.getString("source_id"));
                    map.put("branchCode", resultSet.getString("branch_code"));
                    map.put("rrCode", resultSet.getString("rr_code"));
                    map.put("partyId", resultSet.getString("party_id"));
                    map.put("partyName", resultSet.getString("party_name"));
                    map.put("counterPartyId", resultSet.getString("counter_party_id"));
                    map.put("counterPartyName", resultSet.getString("counter_party_name"));
                    map.put("txnId", resultSet.getString("txn_id"));
                    map.put("txnName", resultSet.getString("txn_name"));
                    map.put("productId", resultSet.getString("product_id"));
                    map.put("productName", resultSet.getString("product_name"));
                    map.put("productType", resultSet.getString("product_type"));
                    map.put("groupName", resultSet.getString("group_name"));
                    map.put("subgroupName", resultSet.getString("subgroup_name"));
                    map.put("eventStartTs", resultSet.getObject("event_start_ts", OffsetDateTime.class));
                    map.put("eventEndTs", resultSet.getObject("event_end_ts", OffsetDateTime.class));
                    map.put("eventOtherTs", resultSet.getObject("event_other_ts", OffsetDateTime.class));
                    map.put("eventOther", resultSet.getString("event_other"));
                    map.put("risk", resultSet.getBigDecimal("risk"));
                    map.put("riskScale", resultSet.getObject("risk_scale"));
                    map.put("price", resultSet.getBigDecimal("price"));
                    map.put("priceScale", resultSet.getObject("price_scale"));
                    map.put("productQy", resultSet.getBigDecimal("product_qy"));
                    map.put("currencyCode", resultSet.getString("currency_code"));
                    map.put("exRefId", resultSet.getString("ex_ref_id"));
                    map.put("exRefCode", resultSet.getString("ex_ref_code"));
                    map.put("productQyScale", resultSet.getObject("product_qy_scale"));
                    map.put("parentProcessId", resultSet.getString("parent_process_id"));
                    map.put("deadlineTs", resultSet.getObject("deadline_ts", OffsetDateTime.class));
                    map.put("parentGroupId", resultSet.getBigDecimal("parent_group_id"));
                    map.put("processSubtypeCode", resultSet.getString("process_subtype_code"));
                    map.put("owningGroupName", resultSet.getString("owning_group_name"));
                    map.put("updateUser", resultSet.getString("update_user"));
                    map.put("updateTs", resultSet.getObject("update_ts", OffsetDateTime.class));
                    map.put("aggregateVersion", resultSet.getLong("aggregate_version"));
                    map.put("active", resultSet.getBoolean("active"));
                    result = Success.of(JsonMapper.toJson(map));
                } else {
                    result = Failure.of(new Status(OBJECT_NOT_FOUND, "process_info", processId));
                }
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
    public void createTaskInfo(Connection conn, Map<String, Object> event) throws PortalPersistenceException {
        final String sql =
                """
                INSERT INTO task_info_t (host_id, task_id, agent_def_id, task_type, process_id, wf_instance_id, wf_task_id, status_code, started_ts, locked, priority, completed_ts, completed_user, result_code, locking_user, locking_role, deadline_ts, lock_group, update_user, update_ts, aggregate_version, active)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, TRUE)
                ON CONFLICT (host_id, task_id) DO UPDATE
                SET agent_def_id = EXCLUDED.agent_def_id,
                    task_type = EXCLUDED.task_type,
                    process_id = EXCLUDED.process_id,
                    wf_instance_id = EXCLUDED.wf_instance_id,
                    wf_task_id = EXCLUDED.wf_task_id,
                    status_code = EXCLUDED.status_code,
                    started_ts = EXCLUDED.started_ts,
                    locked = EXCLUDED.locked,
                    priority = EXCLUDED.priority,
                    completed_ts = EXCLUDED.completed_ts,
                    completed_user = EXCLUDED.completed_user,
                    result_code = EXCLUDED.result_code,
                    locking_user = EXCLUDED.locking_user,
                    locking_role = EXCLUDED.locking_role,
                    deadline_ts = EXCLUDED.deadline_ts,
                    lock_group = EXCLUDED.lock_group,
                    update_user = EXCLUDED.update_user,
                    update_ts = EXCLUDED.update_ts,
                    aggregate_version = EXCLUDED.aggregate_version,
                    active = TRUE
                WHERE task_info_t.aggregate_version < EXCLUDED.aggregate_version
                AND task_info_t.active = FALSE
                """;
        Map<String, Object> map = SqlUtil.extractEventData(event);
        String hostId = (String)map.get("hostId");
        String taskId = (String)map.get("taskId");
        long newAggregateVersion = SqlUtil.getNewAggregateVersion(event);

        try (PreparedStatement statement = conn.prepareStatement(sql)) {
            int i = 1;
            statement.setObject(i++, UUID.fromString(hostId));
            statement.setObject(i++, UUID.fromString(taskId));
            statement.setObject(i++, UUID.fromString((String)map.get("agentDefId")));
            statement.setString(i++, (String)map.get("taskType"));
            statement.setObject(i++, UUID.fromString((String)map.get("processId")));
            statement.setString(i++, (String)map.get("wfInstanceId"));
            statement.setString(i++, (String)map.get("wfTaskId"));
            statement.setString(i++, (String)map.get("statusCode"));
            statement.setObject(i++, map.get("startedTs") != null ? OffsetDateTime.parse((String)map.get("startedTs")) : OffsetDateTime.now());
            statement.setString(i++, (String)map.get("locked"));
            setIntegerOrNull(statement, i++, map.get("priority"));
            statement.setObject(i++, map.get("completedTs") != null ? OffsetDateTime.parse((String)map.get("completedTs")) : null);
            statement.setString(i++, (String)map.get("completedUser"));
            statement.setString(i++, (String)map.get("resultCode"));
            statement.setString(i++, (String)map.get("lockingUser"));
            statement.setString(i++, (String)map.get("lockingRole"));
            statement.setObject(i++, map.get("deadlineTs") != null ? OffsetDateTime.parse((String)map.get("deadlineTs")) : null);
            statement.setString(i++, (String)map.get("lockGroup"));

            statement.setString(i++, (String)event.get(Constants.USER));
            statement.setObject(i++, OffsetDateTime.parse((String)event.get(CloudEventV1.TIME)));
            statement.setLong(i++, newAggregateVersion);

            int count = statement.executeUpdate();
            if (count == 0) {
                logger.warn("Creation skipped for hostId {} taskId {} aggregateVersion {}. A newer or same version already exists.", hostId, taskId, newAggregateVersion);
            }
        } catch (SQLException e) {
            logger.error("SQLException during createTaskInfo for hostId {} taskId {} aggregateVersion {}: {}", hostId, taskId, newAggregateVersion, e.getMessage(), e);
            throw new PortalPersistenceException("Persistence Error", e);
        } catch (Exception e) {
            logger.error("Exception during createTaskInfo for hostId {} taskId {} aggregateVersion {}: {}", hostId, taskId, newAggregateVersion, e.getMessage(), e);
            throw new PortalPersistenceException("Persistence Error", e);
        }
    }

    @Override
    public void updateTaskInfo(Connection conn, Map<String, Object> event) throws PortalPersistenceException {
        final String sql =
                """
                UPDATE task_info_t
                SET agent_def_id=?, task_type=?, process_id=?, wf_instance_id=?, wf_task_id=?, status_code=?, started_ts=?, locked=?, priority=?, completed_ts=?, completed_user=?, result_code=?, locking_user=?, locking_role=?, deadline_ts=?, lock_group=?, update_user=?, update_ts=?, aggregate_version=?, active=TRUE
                WHERE host_id=? AND task_id=? AND aggregate_version < ?
                """;
        Map<String, Object> map = SqlUtil.extractEventData(event);
        String hostId = (String)map.get("hostId");
        String taskId = (String)map.get("taskId");
        long newAggregateVersion = SqlUtil.getNewAggregateVersion(event);

        try (PreparedStatement statement = conn.prepareStatement(sql)) {
            int i = 1;
            statement.setObject(i++, UUID.fromString((String)map.get("agentDefId")));
            statement.setString(i++, (String)map.get("taskType"));
            statement.setObject(i++, UUID.fromString((String)map.get("processId")));
            statement.setString(i++, (String)map.get("wfInstanceId"));
            statement.setString(i++, (String)map.get("wfTaskId"));
            statement.setString(i++, (String)map.get("statusCode"));
            statement.setObject(i++, OffsetDateTime.parse((String)map.get("startedTs")));
            statement.setString(i++, (String)map.get("locked"));
            setIntegerOrNull(statement, i++, map.get("priority"));
            statement.setObject(i++, map.get("completedTs") != null ? OffsetDateTime.parse((String)map.get("completedTs")) : null);
            statement.setString(i++, (String)map.get("completedUser"));
            statement.setString(i++, (String)map.get("resultCode"));
            statement.setString(i++, (String)map.get("lockingUser"));
            statement.setString(i++, (String)map.get("lockingRole"));
            statement.setObject(i++, map.get("deadlineTs") != null ? OffsetDateTime.parse((String)map.get("deadlineTs")) : null);
            statement.setString(i++, (String)map.get("lockGroup"));

            statement.setString(i++, (String)event.get(Constants.USER));
            statement.setObject(i++, OffsetDateTime.parse((String)event.get(CloudEventV1.TIME)));
            statement.setLong(i++, newAggregateVersion);

            statement.setObject(i++, UUID.fromString(hostId));
            statement.setObject(i++, UUID.fromString(taskId));
            statement.setLong(i++, newAggregateVersion);

            int count = statement.executeUpdate();
            if (count == 0) {
                logger.warn("Update skipped for hostId {} taskId {} aggregateVersion {}. Record not found or a newer/same version already exists.", hostId, taskId, newAggregateVersion);
            }
        } catch (SQLException e) {
            logger.error("SQLException during updateTaskInfo for hostId {} taskId {} aggregateVersion {}: {}", hostId, taskId, newAggregateVersion, e.getMessage(), e);
            throw new PortalPersistenceException("Persistence Error", e);
        } catch (Exception e) {
            logger.error("Exception during updateTaskInfo for hostId {} taskId {} aggregateVersion {}: {}", hostId, taskId, newAggregateVersion, e.getMessage(), e);
            throw new PortalPersistenceException("Persistence Error", e);
        }
    }

    @Override
    public void deleteTaskInfo(Connection conn, Map<String, Object> event) throws PortalPersistenceException {
        final String sql =
                """
                UPDATE task_info_t
                SET active = FALSE,
                    update_user = ?,
                    update_ts = ?,
                    aggregate_version = ?
                WHERE host_id=? AND task_id=? AND aggregate_version < ?
                """;
        Map<String, Object> map = SqlUtil.extractEventData(event);
        String hostId = (String)map.get("hostId");
        String taskId = (String)map.get("taskId");
        long newAggregateVersion = SqlUtil.getNewAggregateVersion(event);

        try (PreparedStatement statement = conn.prepareStatement(sql)) {
            statement.setString(1, (String)event.get(Constants.USER));
            statement.setObject(2, OffsetDateTime.parse((String)event.get(CloudEventV1.TIME)));
            statement.setLong(3, newAggregateVersion);
            statement.setObject(4, UUID.fromString(hostId));
            statement.setObject(5, UUID.fromString(taskId));
            statement.setLong(6, newAggregateVersion);

            int count = statement.executeUpdate();
            if (count == 0) {
                logger.warn("Delete skipped for hostId {} taskId {} aggregateVersion {}. Record not found or a newer/same version already exists.", hostId, taskId, newAggregateVersion);
            }
        } catch (SQLException e) {
            logger.error("SQLException during deleteTaskInfo for hostId {} taskId {} aggregateVersion {}: {}", hostId, taskId, newAggregateVersion, e.getMessage(), e);
            throw new PortalPersistenceException("Persistence Error", e);
        } catch (Exception e) {
            logger.error("Exception during deleteTaskInfo for hostId {} taskId {} aggregateVersion {}: {}", hostId, taskId, newAggregateVersion, e.getMessage(), e);
            throw new PortalPersistenceException("Persistence Error", e);
        }
    }

    @Override
    public Result<String> queryTaskInfo(int offset, int limit, String filtersJson, String globalFilter, String sortingJson, boolean active, String hostId) {
        Result<String> result;
        List<Map<String, Object>> filters = parseJsonList(filtersJson);
        List<Map<String, Object>> sorting = parseJsonList(sortingJson);
        String s =
                """
                SELECT COUNT(*) OVER () AS total, host_id, task_id, agent_def_id, task_type, process_id, wf_instance_id, wf_task_id, status_code, started_ts, locked, priority, completed_ts, completed_user, result_code, locking_user, locking_role, deadline_ts, lock_group, update_user, update_ts, aggregate_version, active
                FROM task_info_t WHERE host_id = ?
                """;
        List<Object> parameters = new ArrayList<>();
        parameters.add(UUID.fromString(hostId));
        String activeClause = SqlUtil.buildMultiTableActiveClause(active);
        String[] searchColumns = {"task_type", "status_code", "locking_user"};
        String sqlBuilder = s + activeClause +
                dynamicFilter(Arrays.asList("host_id", "process_id"), Arrays.asList(searchColumns), filters, null, parameters) +
                globalFilter(globalFilter, searchColumns, parameters) +
                dynamicSorting("started_ts", sorting, null) +
                "\nLIMIT ? OFFSET ?";
        parameters.add(limit);
        parameters.add(offset);
        if(logger.isTraceEnabled()) logger.trace("queryTaskInfo sql: {}", sqlBuilder);
        int total = 0;
        List<Map<String, Object>> taskInfos = new ArrayList<>();
        try (final Connection conn = ds.getConnection(); PreparedStatement preparedStatement = conn.prepareStatement(sqlBuilder)) {
            populateParameters(preparedStatement, parameters);
            boolean isFirstRow = true;
            try (ResultSet resultSet = preparedStatement.executeQuery()) {
                while (resultSet.next()) {
                    Map<String, Object> map = new HashMap<>();
                    if (isFirstRow) {
                        total = resultSet.getInt("total");
                        isFirstRow = false;
                    }
                    map.put("hostId", resultSet.getObject("host_id", UUID.class));
                    map.put("taskId", resultSet.getObject("task_id", UUID.class));
                    map.put("agentDefId", resultSet.getObject("agent_def_id", UUID.class));
                    map.put("taskType", resultSet.getString("task_type"));
                    map.put("processId", resultSet.getObject("process_id", UUID.class));
                    map.put("wfInstanceId", resultSet.getString("wf_instance_id"));
                    map.put("wfTaskId", resultSet.getString("wf_task_id"));
                    map.put("statusCode", resultSet.getString("status_code"));
                    map.put("startedTs", resultSet.getObject("started_ts", OffsetDateTime.class));
                    map.put("locked", resultSet.getString("locked"));
                    map.put("priority", resultSet.getObject("priority") != null ? resultSet.getInt("priority") : null);
                    map.put("completedTs", resultSet.getObject("completed_ts", OffsetDateTime.class));
                    map.put("completedUser", resultSet.getString("completed_user"));
                    map.put("resultCode", resultSet.getString("result_code"));
                    map.put("lockingUser", resultSet.getString("locking_user"));
                    map.put("lockingRole", resultSet.getString("locking_role"));
                    map.put("deadlineTs", resultSet.getObject("deadline_ts", OffsetDateTime.class));
                    map.put("lockGroup", resultSet.getString("lock_group"));
                    map.put("updateUser", resultSet.getString("update_user"));
                    map.put("updateTs", resultSet.getObject("update_ts", OffsetDateTime.class));
                    map.put("aggregateVersion", resultSet.getLong("aggregate_version"));
                    map.put("active", resultSet.getBoolean("active"));
                    taskInfos.add(map);
                }
            }
            Map<String, Object> resultMap = new HashMap<>();
            resultMap.put("total", total);
            resultMap.put("taskInfos", taskInfos);
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
    public Result<String> getTaskInfoById(String hostId, String taskId) {
        final String sql = "SELECT host_id, task_id, agent_def_id, task_type, process_id, wf_instance_id, wf_task_id, status_code, started_ts, locked, priority, completed_ts, completed_user, result_code, locking_user, locking_role, deadline_ts, lock_group, update_user, update_ts, aggregate_version, active FROM task_info_t WHERE host_id = ? AND task_id = ?";
        Result<String> result;
        Map<String, Object> map = new HashMap<>();
        try (Connection conn = ds.getConnection(); PreparedStatement statement = conn.prepareStatement(sql)) {
            statement.setObject(1, UUID.fromString(hostId));
            statement.setObject(2, UUID.fromString(taskId));
            try (ResultSet resultSet = statement.executeQuery()) {
                if (resultSet.next()) {
                    map.put("hostId", resultSet.getObject("host_id", UUID.class));
                    map.put("taskId", resultSet.getObject("task_id", UUID.class));
                    map.put("agentDefId", resultSet.getObject("agent_def_id", UUID.class));
                    map.put("taskType", resultSet.getString("task_type"));
                    map.put("processId", resultSet.getObject("process_id", UUID.class));
                    map.put("wfInstanceId", resultSet.getString("wf_instance_id"));
                    map.put("wfTaskId", resultSet.getString("wf_task_id"));
                    map.put("statusCode", resultSet.getString("status_code"));
                    map.put("startedTs", resultSet.getObject("started_ts", OffsetDateTime.class));
                    map.put("locked", resultSet.getString("locked"));
                    map.put("priority", resultSet.getObject("priority") != null ? resultSet.getInt("priority") : null);
                    map.put("completedTs", resultSet.getObject("completed_ts", OffsetDateTime.class));
                    map.put("completedUser", resultSet.getString("completed_user"));
                    map.put("resultCode", resultSet.getString("result_code"));
                    map.put("lockingUser", resultSet.getString("locking_user"));
                    map.put("lockingRole", resultSet.getString("locking_role"));
                    map.put("deadlineTs", resultSet.getObject("deadline_ts", OffsetDateTime.class));
                    map.put("lockGroup", resultSet.getString("lock_group"));
                    map.put("updateUser", resultSet.getString("update_user"));
                    map.put("updateTs", resultSet.getObject("update_ts", OffsetDateTime.class));
                    map.put("aggregateVersion", resultSet.getLong("aggregate_version"));
                    map.put("active", resultSet.getBoolean("active"));
                    result = Success.of(JsonMapper.toJson(map));
                } else {
                    result = Failure.of(new Status(OBJECT_NOT_FOUND, "task_info", taskId));
                }
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
    public void createTaskAssignment(Connection conn, Map<String, Object> event) throws PortalPersistenceException {
        final String sql =
                """
                INSERT INTO task_asst_t (host_id, task_asst_id, task_id, assigned_ts, assignee_id, reason_code, unassigned_ts, unassigned_reason, category_code, update_user, update_ts, aggregate_version, active)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, TRUE)
                ON CONFLICT (host_id, task_asst_id) DO UPDATE
                SET task_id = EXCLUDED.task_id,
                    assigned_ts = EXCLUDED.assigned_ts,
                    assignee_id = EXCLUDED.assignee_id,
                    reason_code = EXCLUDED.reason_code,
                    unassigned_ts = EXCLUDED.unassigned_ts,
                    unassigned_reason = EXCLUDED.unassigned_reason,
                    category_code = EXCLUDED.category_code,
                    update_user = EXCLUDED.update_user,
                    update_ts = EXCLUDED.update_ts,
                    aggregate_version = EXCLUDED.aggregate_version,
                    active = TRUE
                WHERE task_asst_t.aggregate_version < EXCLUDED.aggregate_version
                AND task_asst_t.active = FALSE
                """;
        Map<String, Object> map = SqlUtil.extractEventData(event);
        String hostId = (String)map.get("hostId");
        String taskAsstId = (String)map.get("taskAsstId");
        long newAggregateVersion = SqlUtil.getNewAggregateVersion(event);

        try (PreparedStatement statement = conn.prepareStatement(sql)) {
            int i = 1;
            statement.setObject(i++, UUID.fromString(hostId));
            statement.setObject(i++, UUID.fromString(taskAsstId));
            statement.setObject(i++, UUID.fromString((String)map.get("taskId")));
            statement.setObject(i++, map.get("assignedTs") != null ? OffsetDateTime.parse((String)map.get("assignedTs")) : OffsetDateTime.now());
            statement.setString(i++, (String)map.get("assigneeId"));
            statement.setString(i++, (String)map.get("reasonCode"));
            statement.setObject(i++, map.get("unassignedTs") != null ? OffsetDateTime.parse((String)map.get("unassignedTs")) : null);
            statement.setString(i++, (String)map.get("unassignedReason"));
            statement.setString(i++, (String)map.get("categoryCode"));

            statement.setString(i++, (String)event.get(Constants.USER));
            statement.setObject(i++, OffsetDateTime.parse((String)event.get(CloudEventV1.TIME)));
            statement.setLong(i++, newAggregateVersion);

            int count = statement.executeUpdate();
            if (count == 0) {
                logger.warn("Creation skipped for hostId {} taskAsstId {} aggregateVersion {}. A newer or same version already exists.", hostId, taskAsstId, newAggregateVersion);
            }
        } catch (SQLException e) {
            logger.error("SQLException during createTaskAssignment for hostId {} taskAsstId {} aggregateVersion {}: {}", hostId, taskAsstId, newAggregateVersion, e.getMessage(), e);
            throw new PortalPersistenceException("Persistence Error", e);
        } catch (Exception e) {
            logger.error("Exception during createTaskAssignment for hostId {} taskAsstId {} aggregateVersion {}: {}", hostId, taskAsstId, newAggregateVersion, e.getMessage(), e);
            throw new PortalPersistenceException("Persistence Error", e);
        }
    }

    @Override
    public void updateTaskAssignment(Connection conn, Map<String, Object> event) throws PortalPersistenceException {
        final String sql =
                """
                UPDATE task_asst_t
                SET task_id=?, assigned_ts=?, assignee_id=?, reason_code=?, unassigned_ts=?, unassigned_reason=?, category_code=?, update_user=?, update_ts=?, aggregate_version=?, active=TRUE
                WHERE host_id=? AND task_asst_id=? AND aggregate_version < ?
                """;
        Map<String, Object> map = SqlUtil.extractEventData(event);
        String hostId = (String)map.get("hostId");
        String taskAsstId = (String)map.get("taskAsstId");
        long newAggregateVersion = SqlUtil.getNewAggregateVersion(event);

        try (PreparedStatement statement = conn.prepareStatement(sql)) {
            int i = 1;
            statement.setObject(i++, UUID.fromString((String)map.get("taskId")));
            statement.setObject(i++, OffsetDateTime.parse((String)map.get("assignedTs")));
            statement.setString(i++, (String)map.get("assigneeId"));
            statement.setString(i++, (String)map.get("reasonCode"));
            statement.setObject(i++, map.get("unassignedTs") != null ? OffsetDateTime.parse((String)map.get("unassignedTs")) : null);
            statement.setString(i++, (String)map.get("unassignedReason"));
            statement.setString(i++, (String)map.get("categoryCode"));

            statement.setString(i++, (String)event.get(Constants.USER));
            statement.setObject(i++, OffsetDateTime.parse((String)event.get(CloudEventV1.TIME)));
            statement.setLong(i++, newAggregateVersion);

            statement.setObject(i++, UUID.fromString(hostId));
            statement.setObject(i++, UUID.fromString(taskAsstId));
            statement.setLong(i++, newAggregateVersion);

            int count = statement.executeUpdate();
            if (count == 0) {
                logger.warn("Update skipped for hostId {} taskAsstId {} aggregateVersion {}. Record not found or a newer/same version already exists.", hostId, taskAsstId, newAggregateVersion);
            }
        } catch (SQLException e) {
            logger.error("SQLException during updateTaskAssignment for hostId {} taskAsstId {} aggregateVersion {}: {}", hostId, taskAsstId, newAggregateVersion, e.getMessage(), e);
            throw new PortalPersistenceException("Persistence Error", e);
        } catch (Exception e) {
            logger.error("Exception during updateTaskAssignment for hostId {} taskAsstId {} aggregateVersion {}: {}", hostId, taskAsstId, newAggregateVersion, e.getMessage(), e);
            throw new PortalPersistenceException("Persistence Error", e);
        }
    }

    @Override
    public void deleteTaskAssignment(Connection conn, Map<String, Object> event) throws PortalPersistenceException {
        final String sql =
                """
                UPDATE task_asst_t
                SET active = FALSE,
                    update_user = ?,
                    update_ts = ?,
                    aggregate_version = ?
                WHERE host_id=? AND task_asst_id=? AND aggregate_version < ?
                """;
        Map<String, Object> map = SqlUtil.extractEventData(event);
        String hostId = (String)map.get("hostId");
        String taskAsstId = (String)map.get("taskAsstId");
        long newAggregateVersion = SqlUtil.getNewAggregateVersion(event);

        try (PreparedStatement statement = conn.prepareStatement(sql)) {
            statement.setString(1, (String)event.get(Constants.USER));
            statement.setObject(2, OffsetDateTime.parse((String)event.get(CloudEventV1.TIME)));
            statement.setLong(3, newAggregateVersion);
            statement.setObject(4, UUID.fromString(hostId));
            statement.setObject(5, UUID.fromString(taskAsstId));
            statement.setLong(6, newAggregateVersion);

            int count = statement.executeUpdate();
            if (count == 0) {
                logger.warn("Delete skipped for hostId {} taskAsstId {} aggregateVersion {}. Record not found or a newer/same version already exists.", hostId, taskAsstId, newAggregateVersion);
            }
        } catch (SQLException e) {
            logger.error("SQLException during deleteTaskAssignment for hostId {} taskAsstId {} aggregateVersion {}: {}", hostId, taskAsstId, newAggregateVersion, e.getMessage(), e);
            throw new PortalPersistenceException("Persistence Error", e);
        } catch (Exception e) {
            logger.error("Exception during deleteTaskAssignment for hostId {} taskAsstId {} aggregateVersion {}: {}", hostId, taskAsstId, newAggregateVersion, e.getMessage(), e);
            throw new PortalPersistenceException("Persistence Error", e);
        }
    }

    @Override
    public Result<String> queryTaskAssignment(int offset, int limit, String filtersJson, String globalFilter, String sortingJson, boolean active, String hostId) {
        Result<String> result;
        List<Map<String, Object>> filters = parseJsonList(filtersJson);
        List<Map<String, Object>> sorting = parseJsonList(sortingJson);
        String s =
                """
                SELECT COUNT(*) OVER () AS total, host_id, task_asst_id, task_id, assigned_ts, assignee_id, reason_code, unassigned_ts, unassigned_reason, category_code, update_user, update_ts, aggregate_version, active
                FROM task_asst_t WHERE host_id = ?
                """;
        List<Object> parameters = new ArrayList<>();
        parameters.add(UUID.fromString(hostId));
        String activeClause = SqlUtil.buildMultiTableActiveClause(active);
        String[] searchColumns = {"assignee_id", "reason_code", "category_code"};
        String sqlBuilder = s + activeClause +
                dynamicFilter(Arrays.asList("host_id", "task_id"), Arrays.asList(searchColumns), filters, null, parameters) +
                globalFilter(globalFilter, searchColumns, parameters) +
                dynamicSorting("assigned_ts", sorting, null) +
                "\nLIMIT ? OFFSET ?";
        parameters.add(limit);
        parameters.add(offset);
        if(logger.isTraceEnabled()) logger.trace("queryTaskAssignment sql: {}", sqlBuilder);
        int total = 0;
        List<Map<String, Object>> taskAssignments = new ArrayList<>();
        try (final Connection conn = ds.getConnection(); PreparedStatement preparedStatement = conn.prepareStatement(sqlBuilder)) {
            populateParameters(preparedStatement, parameters);
            boolean isFirstRow = true;
            try (ResultSet resultSet = preparedStatement.executeQuery()) {
                while (resultSet.next()) {
                    Map<String, Object> map = new HashMap<>();
                    if (isFirstRow) {
                        total = resultSet.getInt("total");
                        isFirstRow = false;
                    }
                    map.put("hostId", resultSet.getObject("host_id", UUID.class));
                    map.put("taskAsstId", resultSet.getObject("task_asst_id", UUID.class));
                    map.put("taskId", resultSet.getObject("task_id", UUID.class));
                    map.put("assignedTs", resultSet.getObject("assigned_ts", OffsetDateTime.class));
                    map.put("assigneeId", resultSet.getString("assignee_id"));
                    map.put("reasonCode", resultSet.getString("reason_code"));
                    map.put("unassignedTs", resultSet.getObject("unassigned_ts", OffsetDateTime.class));
                    map.put("unassignedReason", resultSet.getString("unassigned_reason"));
                    map.put("categoryCode", resultSet.getString("category_code"));
                    map.put("updateUser", resultSet.getString("update_user"));
                    map.put("updateTs", resultSet.getObject("update_ts", OffsetDateTime.class));
                    map.put("aggregateVersion", resultSet.getLong("aggregate_version"));
                    map.put("active", resultSet.getBoolean("active"));
                    taskAssignments.add(map);
                }
            }
            Map<String, Object> resultMap = new HashMap<>();
            resultMap.put("total", total);
            resultMap.put("taskAssignments", taskAssignments);
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
    public Result<String> getTaskAssignmentById(String hostId, String taskAsstId) {
        final String sql = "SELECT host_id, task_asst_id, task_id, assigned_ts, assignee_id, reason_code, unassigned_ts, unassigned_reason, category_code, update_user, update_ts, aggregate_version, active FROM task_asst_t WHERE host_id = ? AND task_asst_id = ?";
        Result<String> result;
        Map<String, Object> map = new HashMap<>();
        try (Connection conn = ds.getConnection(); PreparedStatement statement = conn.prepareStatement(sql)) {
            statement.setObject(1, UUID.fromString(hostId));
            statement.setObject(2, UUID.fromString(taskAsstId));
            try (ResultSet resultSet = statement.executeQuery()) {
                if (resultSet.next()) {
                    map.put("hostId", resultSet.getObject("host_id", UUID.class));
                    map.put("taskAsstId", resultSet.getObject("task_asst_id", UUID.class));
                    map.put("taskId", resultSet.getObject("task_id", UUID.class));
                    map.put("assignedTs", resultSet.getObject("assigned_ts", OffsetDateTime.class));
                    map.put("assigneeId", resultSet.getString("assignee_id"));
                    map.put("reasonCode", resultSet.getString("reason_code"));
                    map.put("unassignedTs", resultSet.getObject("unassigned_ts", OffsetDateTime.class));
                    map.put("unassignedReason", resultSet.getString("unassigned_reason"));
                    map.put("categoryCode", resultSet.getString("category_code"));
                    map.put("updateUser", resultSet.getString("update_user"));
                    map.put("updateTs", resultSet.getObject("update_ts", OffsetDateTime.class));
                    map.put("aggregateVersion", resultSet.getLong("aggregate_version"));
                    map.put("active", resultSet.getBoolean("active"));
                    result = Success.of(JsonMapper.toJson(map));
                } else {
                    result = Failure.of(new Status(OBJECT_NOT_FOUND, "task_assignment", taskAsstId));
                }
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
    public void createAuditLog(Connection conn, Map<String, Object> event) throws PortalPersistenceException {
        final String sql =
                """
                INSERT INTO audit_log_t (host_id, audit_log_id, source_type_id, correlation_id, user_id, event_ts, success, message0, message1, message2, message3, message, user_comment)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """;
        Map<String, Object> map = SqlUtil.extractEventData(event);
        String hostId = (String)map.get("hostId");
        String auditLogId = (String)map.get("auditLogId");

        try (PreparedStatement statement = conn.prepareStatement(sql)) {
            int i = 1;
            statement.setObject(i++, UUID.fromString(hostId));
            statement.setObject(i++, UUID.fromString(auditLogId));
            statement.setString(i++, (String)map.get("sourceTypeId"));
            statement.setString(i++, (String)map.get("correlationId"));
            statement.setString(i++, (String)map.get("userId"));
            statement.setObject(i++, map.get("eventTs") != null ? OffsetDateTime.parse((String)map.get("eventTs")) : OffsetDateTime.now());
            statement.setString(i++, (String)map.get("success"));
            statement.setString(i++, (String)map.get("message0"));
            statement.setString(i++, (String)map.get("message1"));
            statement.setString(i++, (String)map.get("message2"));
            statement.setString(i++, (String)map.get("message3"));
            statement.setString(i++, (String)map.get("message"));
            statement.setString(i++, (String)map.get("userComment"));

            statement.executeUpdate();
        } catch (SQLException e) {
            logger.error("SQLException during createAuditLog for hostId {} auditLogId {}: {}", hostId, auditLogId, e.getMessage(), e);
            throw new PortalPersistenceException("Persistence Error", e);
        } catch (Exception e) {
            logger.error("Exception during createAuditLog for hostId {} auditLogId {}: {}", hostId, auditLogId, e.getMessage(), e);
            throw new PortalPersistenceException("Persistence Error", e);
        }
    }

    @Override
    public Result<String> queryAuditLog(int offset, int limit, String filtersJson, String globalFilter, String sortingJson, String hostId) {
        Result<String> result;
        List<Map<String, Object>> filters = parseJsonList(filtersJson);
        List<Map<String, Object>> sorting = parseJsonList(sortingJson);
        String s =
                """
                SELECT COUNT(*) OVER () AS total, host_id, audit_log_id, source_type_id, correlation_id, user_id, event_ts, success, message0, message1, message2, message3, message, user_comment
                FROM audit_log_t WHERE host_id = ?
                """;
        List<Object> parameters = new ArrayList<>();
        parameters.add(UUID.fromString(hostId));
        String[] searchColumns = {"source_type_id", "correlation_id", "user_id", "success", "message", "user_comment"};
        String sqlBuilder = s +
                dynamicFilter(Arrays.asList("host_id", "audit_log_id"), Arrays.asList(searchColumns), filters, null, parameters) +
                globalFilter(globalFilter, searchColumns, parameters) +
                dynamicSorting("event_ts DESC", sorting, null) +
                "\nLIMIT ? OFFSET ?";
        parameters.add(limit);
        parameters.add(offset);
        if(logger.isTraceEnabled()) logger.trace("queryAuditLog sql: {}", sqlBuilder);
        int total = 0;
        List<Map<String, Object>> auditLogs = new ArrayList<>();
        try (final Connection conn = ds.getConnection(); PreparedStatement preparedStatement = conn.prepareStatement(sqlBuilder)) {
            populateParameters(preparedStatement, parameters);
            boolean isFirstRow = true;
            try (ResultSet resultSet = preparedStatement.executeQuery()) {
                while (resultSet.next()) {
                    Map<String, Object> map = new HashMap<>();
                    if (isFirstRow) {
                        total = resultSet.getInt("total");
                        isFirstRow = false;
                    }
                    map.put("hostId", resultSet.getObject("host_id", UUID.class));
                    map.put("auditLogId", resultSet.getObject("audit_log_id", UUID.class));
                    map.put("sourceTypeId", resultSet.getString("source_type_id"));
                    map.put("correlationId", resultSet.getString("correlation_id"));
                    map.put("userId", resultSet.getString("user_id"));
                    map.put("eventTs", resultSet.getObject("event_ts", OffsetDateTime.class));
                    map.put("success", resultSet.getString("success"));
                    map.put("message0", resultSet.getString("message0"));
                    map.put("message1", resultSet.getString("message1"));
                    map.put("message2", resultSet.getString("message2"));
                    map.put("message3", resultSet.getString("message3"));
                    map.put("message", resultSet.getString("message"));
                    map.put("userComment", resultSet.getString("user_comment"));
                    auditLogs.add(map);
                }
            }
            Map<String, Object> resultMap = new HashMap<>();
            resultMap.put("total", total);
            resultMap.put("auditLogs", auditLogs);
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
    public Result<String> getAuditLogById(String hostId, String auditLogId) {
        final String sql = "SELECT host_id, audit_log_id, source_type_id, correlation_id, user_id, event_ts, success, message0, message1, message2, message3, message, user_comment FROM audit_log_t WHERE host_id = ? AND audit_log_id = ?";
        Result<String> result;
        Map<String, Object> map = new HashMap<>();
        try (Connection conn = ds.getConnection(); PreparedStatement statement = conn.prepareStatement(sql)) {
            statement.setObject(1, UUID.fromString(hostId));
            statement.setObject(2, UUID.fromString(auditLogId));
            try (ResultSet resultSet = statement.executeQuery()) {
                if (resultSet.next()) {
                    map.put("hostId", resultSet.getObject("host_id", UUID.class));
                    map.put("auditLogId", resultSet.getObject("audit_log_id", UUID.class));
                    map.put("sourceTypeId", resultSet.getString("source_type_id"));
                    map.put("correlationId", resultSet.getString("correlation_id"));
                    map.put("userId", resultSet.getString("user_id"));
                    map.put("eventTs", resultSet.getObject("event_ts", OffsetDateTime.class));
                    map.put("success", resultSet.getString("success"));
                    map.put("message0", resultSet.getString("message0"));
                    map.put("message1", resultSet.getString("message1"));
                    map.put("message2", resultSet.getString("message2"));
                    map.put("message3", resultSet.getString("message3"));
                    map.put("message", resultSet.getString("message"));
                    map.put("userComment", resultSet.getString("user_comment"));
                    result = Success.of(JsonMapper.toJson(map));
                } else {
                    result = Failure.of(new Status(OBJECT_NOT_FOUND, "audit_log", auditLogId));
                }
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
    public void createTool(Connection conn, Map<String, Object> event) throws PortalPersistenceException {
        final String sql =
                """
                INSERT INTO tool_t (host_id, tool_id, name, description, implementation_type, implementation_class, mcp_server_name, api_endpoint, api_method, script_content, version, update_user, update_ts, aggregate_version, active)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, TRUE)
                ON CONFLICT (host_id, tool_id) DO UPDATE
                SET name = EXCLUDED.name,
                    description = EXCLUDED.description,
                    implementation_type = EXCLUDED.implementation_type,
                    implementation_class = EXCLUDED.implementation_class,
                    mcp_server_name = EXCLUDED.mcp_server_name,
                    api_endpoint = EXCLUDED.api_endpoint,
                    api_method = EXCLUDED.api_method,
                    script_content = EXCLUDED.script_content,
                    version = EXCLUDED.version,
                    update_user = EXCLUDED.update_user,
                    update_ts = EXCLUDED.update_ts,
                    aggregate_version = EXCLUDED.aggregate_version,
                    active = TRUE
                WHERE tool_t.aggregate_version < EXCLUDED.aggregate_version
                AND tool_t.active = FALSE
                """;
        Map<String, Object> map = SqlUtil.extractEventData(event);
        String hostId = (String)map.get("hostId");
        String toolId = (String)map.get("toolId");
        long newAggregateVersion = SqlUtil.getNewAggregateVersion(event);
        try (PreparedStatement statement = conn.prepareStatement(sql)) {
            int i = 1;
            statement.setObject(i++, UUID.fromString(hostId));
            statement.setObject(i++, UUID.fromString(toolId));
            statement.setString(i++, (String)map.get("name"));
            statement.setString(i++, (String)map.get("description"));
            statement.setString(i++, (String)map.get("implementationType"));
            statement.setString(i++, (String)map.get("implementationClass"));
            statement.setString(i++, (String)map.get("mcpServerName"));
            statement.setString(i++, (String)map.get("apiEndpoint"));
            statement.setString(i++, (String)map.get("apiMethod"));
            statement.setString(i++, (String)map.get("scriptContent"));
            if(map.get("version") != null) {
                statement.setString(i++, (String)map.get("version"));
            } else {
                statement.setString(i++, "1.0.0");
            }
            statement.setString(i++, (String)event.get(Constants.USER));
            statement.setObject(i++, OffsetDateTime.parse((String)event.get(CloudEventV1.TIME)));
            statement.setLong(i++, newAggregateVersion);

            int count = statement.executeUpdate();
            if (count == 0) {
                logger.warn("Creation skipped for hostId {} toolId {} aggregateVersion {}. A newer or same version already exists.", hostId, toolId, newAggregateVersion);
            }
        } catch (SQLException e) {
            logger.error("SQLException during createTool for hostId {} toolId {} aggregateVersion {}: {}", hostId, toolId, newAggregateVersion, e.getMessage(), e);
            throw new PortalPersistenceException("Persistence Error", e);
        } catch (Exception e) {
            logger.error("Exception during createTool for hostId {} toolId {} aggregateVersion {}: {}", hostId, toolId, newAggregateVersion, e.getMessage(), e);
            throw new PortalPersistenceException("Persistence Error", e);
        }
    }

    @Override
    public void updateTool(Connection conn, Map<String, Object> event) throws PortalPersistenceException {
        final String sql =
                """
                UPDATE tool_t
                SET name=?, description=?, implementation_type=?, implementation_class=?, mcp_server_name=?, api_endpoint=?, api_method=?, script_content=?, version=?, update_user=?, update_ts=?, aggregate_version=?, active=TRUE
                WHERE host_id=? AND tool_id=? AND aggregate_version < ?
                """;
        Map<String, Object> map = SqlUtil.extractEventData(event);
        String hostId = (String)map.get("hostId");
        String toolId = (String)map.get("toolId");
        long newAggregateVersion = SqlUtil.getNewAggregateVersion(event);
        try (PreparedStatement statement = conn.prepareStatement(sql)) {
            int i = 1;
            statement.setString(i++, (String)map.get("name"));
            statement.setString(i++, (String)map.get("description"));
            statement.setString(i++, (String)map.get("implementationType"));
            statement.setString(i++, (String)map.get("implementationClass"));
            statement.setString(i++, (String)map.get("mcpServerName"));
            statement.setString(i++, (String)map.get("apiEndpoint"));
            statement.setString(i++, (String)map.get("apiMethod"));
            statement.setString(i++, (String)map.get("scriptContent"));
            if(map.get("version") != null) {
                statement.setString(i++, (String)map.get("version"));
            } else {
                statement.setString(i++, "1.0.0");
            }
            statement.setString(i++, (String)event.get(Constants.USER));
            statement.setObject(i++, OffsetDateTime.parse((String)event.get(CloudEventV1.TIME)));
            statement.setLong(i++, newAggregateVersion);
            statement.setObject(i++, UUID.fromString(hostId));
            statement.setObject(i++, UUID.fromString(toolId));
            statement.setLong(i++, newAggregateVersion);

            int count = statement.executeUpdate();
            if (count == 0) {
                logger.warn("Update skipped for hostId {} toolId {} aggregateVersion {}. Record not found or a newer/same version already exists.", hostId, toolId, newAggregateVersion);
            }
        } catch (SQLException e) {
            logger.error("SQLException during updateTool for hostId {} toolId {} aggregateVersion {}: {}", hostId, toolId, newAggregateVersion, e.getMessage(), e);
            throw new PortalPersistenceException("Persistence Error", e);
        } catch (Exception e) {
            logger.error("Exception during updateTool for hostId {} toolId {} aggregateVersion {}: {}", hostId, toolId, newAggregateVersion, e.getMessage(), e);
            throw new PortalPersistenceException("Persistence Error", e);
        }
    }

    @Override
    public void deleteTool(Connection conn, Map<String, Object> event) throws PortalPersistenceException {
        final String sql =
                """
                UPDATE tool_t
                SET active = FALSE,
                    update_user = ?,
                    update_ts = ?,
                    aggregate_version = ?
                WHERE host_id=? AND tool_id=? AND aggregate_version < ?
                """;
        Map<String, Object> map = SqlUtil.extractEventData(event);
        String hostId = (String)map.get("hostId");
        String toolId = (String)map.get("toolId");
        long newAggregateVersion = SqlUtil.getNewAggregateVersion(event);
        try (PreparedStatement statement = conn.prepareStatement(sql)) {
            statement.setString(1, (String)event.get(Constants.USER));
            statement.setObject(2, OffsetDateTime.parse((String)event.get(CloudEventV1.TIME)));
            statement.setLong(3, newAggregateVersion);
            statement.setObject(4, UUID.fromString(hostId));
            statement.setObject(5, UUID.fromString(toolId));
            statement.setLong(6, newAggregateVersion);

            int count = statement.executeUpdate();
            if (count == 0) {
                logger.warn("Delete skipped for hostId {} toolId {} aggregateVersion {}. Record not found or a newer/same version already exists.", hostId, toolId, newAggregateVersion);
            }
        } catch (SQLException e) {
            logger.error("SQLException during deleteTool for hostId {} toolId {} aggregateVersion {}: {}", hostId, toolId, newAggregateVersion, e.getMessage(), e);
            throw new PortalPersistenceException("Persistence Error", e);
        } catch (Exception e) {
            logger.error("Exception during deleteTool for hostId {} toolId {} aggregateVersion {}: {}", hostId, toolId, newAggregateVersion, e.getMessage(), e);
            throw new PortalPersistenceException("Persistence Error", e);
        }
    }

    @Override
    public Result<String> queryTool(int offset, int limit, String filtersJson, String globalFilter, String sortingJson, boolean active, String hostId) {
        Result<String> result;
        List<Map<String, Object>> filters = parseJsonList(filtersJson);
        List<Map<String, Object>> sorting = parseJsonList(sortingJson);
        String s =
                """
                SELECT COUNT(*) OVER () AS total, host_id, tool_id, name, description, implementation_type, implementation_class, mcp_server_name, api_endpoint, api_method, script_content, version, update_user, update_ts, aggregate_version, active
                FROM tool_t WHERE host_id = ?
                """;
        List<Object> parameters = new ArrayList<>();
        parameters.add(UUID.fromString(hostId));
        String activeClause = SqlUtil.buildMultiTableActiveClause(active);
        String[] searchColumns = {"name", "description"};
        String sqlBuilder = s + activeClause +
                dynamicFilter(Arrays.asList("host_id", "tool_id"), Arrays.asList(searchColumns), filters, null, parameters) +
                globalFilter(globalFilter, searchColumns, parameters) +
                dynamicSorting("name", sorting, null) +
                "\nLIMIT ? OFFSET ?";
        parameters.add(limit);
        parameters.add(offset);
        if(logger.isTraceEnabled()) logger.trace("queryTool sql: {}", sqlBuilder);
        int total = 0;
        List<Map<String, Object>> tools = new ArrayList<>();
        try (final Connection conn = ds.getConnection(); PreparedStatement preparedStatement = conn.prepareStatement(sqlBuilder)) {
            populateParameters(preparedStatement, parameters);
            boolean isFirstRow = true;
            try (ResultSet resultSet = preparedStatement.executeQuery()) {
                while (resultSet.next()) {
                    Map<String, Object> map = new HashMap<>();
                    if (isFirstRow) {
                        total = resultSet.getInt("total");
                        isFirstRow = false;
                    }
                    map.put("hostId", resultSet.getObject("host_id", UUID.class));
                    map.put("toolId", resultSet.getObject("tool_id", UUID.class));
                    map.put("name", resultSet.getString("name"));
                    map.put("description", resultSet.getString("description"));
                    map.put("implementationType", resultSet.getString("implementation_type"));
                    map.put("implementationClass", resultSet.getString("implementation_class"));
                    map.put("mcpServerName", resultSet.getString("mcp_server_name"));
                    map.put("apiEndpoint", resultSet.getString("api_endpoint"));
                    map.put("apiMethod", resultSet.getString("api_method"));
                    map.put("scriptContent", resultSet.getString("script_content"));
                    map.put("version", resultSet.getString("version"));
                    map.put("updateUser", resultSet.getString("update_user"));
                    map.put("updateTs", resultSet.getObject("update_ts", OffsetDateTime.class));
                    map.put("aggregateVersion", resultSet.getLong("aggregate_version"));
                    map.put("active", resultSet.getBoolean("active"));
                    tools.add(map);
                }
            }
            Map<String, Object> resultMap = new HashMap<>();
            resultMap.put("total", total);
            resultMap.put("tools", tools);
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
    public Result<String> getToolById(String hostId, String toolId) {
        final String sql = "SELECT host_id, tool_id, name, description, implementation_type, implementation_class, mcp_server_name, api_endpoint, api_method, script_content, version, update_user, update_ts, aggregate_version, active FROM tool_t WHERE host_id = ? AND tool_id = ?";
        Result<String> result;
        Map<String, Object> map = new HashMap<>();
        try (Connection conn = ds.getConnection(); PreparedStatement statement = conn.prepareStatement(sql)) {
            statement.setObject(1, UUID.fromString(hostId));
            statement.setObject(2, UUID.fromString(toolId));
            try (ResultSet resultSet = statement.executeQuery()) {
                if (resultSet.next()) {
                    map.put("hostId", resultSet.getObject("host_id", UUID.class));
                    map.put("toolId", resultSet.getObject("tool_id", UUID.class));
                    map.put("name", resultSet.getString("name"));
                    map.put("description", resultSet.getString("description"));
                    map.put("implementationType", resultSet.getString("implementation_type"));
                    map.put("implementationClass", resultSet.getString("implementation_class"));
                    map.put("mcpServerName", resultSet.getString("mcp_server_name"));
                    map.put("apiEndpoint", resultSet.getString("api_endpoint"));
                    map.put("apiMethod", resultSet.getString("api_method"));
                    map.put("scriptContent", resultSet.getString("script_content"));
                    map.put("version", resultSet.getString("version"));
                    map.put("updateUser", resultSet.getString("update_user"));
                    map.put("updateTs", resultSet.getObject("update_ts", OffsetDateTime.class));
                    map.put("aggregateVersion", resultSet.getLong("aggregate_version"));
                    map.put("active", resultSet.getBoolean("active"));
                    result = Success.of(JsonMapper.toJson(map));
                } else {
                    result = Failure.of(new Status(OBJECT_NOT_FOUND, "tool", toolId));
                }
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
    public Result<String> getToolLabel(String hostId) {
        final String sql = "SELECT tool_id, name FROM tool_t WHERE host_id = ? AND active = TRUE";
        Result<String> result;
        List<Map<String, String>> labels = new ArrayList<>();
        try (Connection conn = ds.getConnection(); PreparedStatement statement = conn.prepareStatement(sql)) {
            statement.setObject(1, UUID.fromString(hostId));
            try (ResultSet resultSet = statement.executeQuery()) {
                while (resultSet.next()) {
                    Map<String, String> map = new HashMap<>();
                    map.put("value", resultSet.getObject("tool_id", UUID.class).toString());
                    map.put("label", resultSet.getString("name"));
                    labels.add(map);
                }
                result = Success.of(JsonMapper.toJson(labels));
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
    public void createSkill(Connection conn, Map<String, Object> event) throws PortalPersistenceException {
        final String sql =
                """
                INSERT INTO skill_t (host_id, skill_id, parent_skill_id, name, description, content_markdown, version, update_user, update_ts, aggregate_version, active)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, TRUE)
                ON CONFLICT (host_id, skill_id) DO UPDATE
                SET parent_skill_id = EXCLUDED.parent_skill_id,
                    name = EXCLUDED.name,
                    description = EXCLUDED.description,
                    content_markdown = EXCLUDED.content_markdown,
                    version = EXCLUDED.version,
                    update_user = EXCLUDED.update_user,
                    update_ts = EXCLUDED.update_ts,
                    aggregate_version = EXCLUDED.aggregate_version,
                    active = TRUE
                WHERE skill_t.aggregate_version < EXCLUDED.aggregate_version
                AND skill_t.active = FALSE
                """;
        Map<String, Object> map = SqlUtil.extractEventData(event);
        String hostId = (String)map.get("hostId");
        String skillId = (String)map.get("skillId");
        long newAggregateVersion = SqlUtil.getNewAggregateVersion(event);

        try (PreparedStatement statement = conn.prepareStatement(sql)) {
            int i = 1;
            statement.setObject(i++, UUID.fromString(hostId));
            statement.setObject(i++, UUID.fromString(skillId));
            setUuidOrNull(statement, i++, map.get("parentSkillId"));
            statement.setString(i++, (String)map.get("name"));
            statement.setString(i++, (String)map.get("description"));
            statement.setString(i++, (String)map.get("contentMarkdown"));
            if(map.get("version") != null) {
                statement.setString(i++, (String)map.get("version"));
            } else {
                statement.setString(i++, "1.0.0");
            }
            statement.setString(i++, (String)event.get(Constants.USER));
            statement.setObject(i++, OffsetDateTime.parse((String)event.get(CloudEventV1.TIME)));
            statement.setLong(i++, newAggregateVersion);

            int count = statement.executeUpdate();
            if (count == 0) {
                logger.warn("Creation skipped for hostId {} skillId {} aggregateVersion {}. A newer or same version already exists.", hostId, skillId, newAggregateVersion);
            }
        } catch (SQLException e) {
            logger.error("SQLException during createSkill for hostId {} skillId {} aggregateVersion {}: {}", hostId, skillId, newAggregateVersion, e.getMessage(), e);
            throw new PortalPersistenceException("Persistence Error", e);
        } catch (Exception e) {
            logger.error("Exception during createSkill for hostId {} skillId {} aggregateVersion {}: {}", hostId, skillId, newAggregateVersion, e.getMessage(), e);
            throw new PortalPersistenceException("Persistence Error", e);
        }
    }

    @Override
    public void updateSkill(Connection conn, Map<String, Object> event) throws PortalPersistenceException {
        final String sql =
                """
                UPDATE skill_t
                SET parent_skill_id=?, name=?, description=?, content_markdown=?, version=?, update_user=?, update_ts=?, aggregate_version=?, active=TRUE
                WHERE host_id=? AND skill_id=? AND aggregate_version < ?
                """;
        Map<String, Object> map = SqlUtil.extractEventData(event);
        String hostId = (String)map.get("hostId");
        String skillId = (String)map.get("skillId");
        long newAggregateVersion = SqlUtil.getNewAggregateVersion(event);

        try (PreparedStatement statement = conn.prepareStatement(sql)) {
            int i = 1;
            setUuidOrNull(statement, i++, map.get("parentSkillId"));
            statement.setString(i++, (String)map.get("name"));
            statement.setString(i++, (String)map.get("description"));
            statement.setString(i++, (String)map.get("contentMarkdown"));
            if(map.get("version") != null) {
                statement.setString(i++, (String)map.get("version"));
            } else {
                statement.setString(i++, "1.0.0");
            }
            statement.setString(i++, (String)event.get(Constants.USER));
            statement.setObject(i++, OffsetDateTime.parse((String)event.get(CloudEventV1.TIME)));
            statement.setLong(i++, newAggregateVersion);
            statement.setObject(i++, UUID.fromString(hostId));
            statement.setObject(i++, UUID.fromString(skillId));
            statement.setLong(i++, newAggregateVersion);

            int count = statement.executeUpdate();
            if (count == 0) {
                logger.warn("Update skipped for hostId {} skillId {} aggregateVersion {}. Record not found or a newer/same version already exists.", hostId, skillId, newAggregateVersion);
            }
        } catch (SQLException e) {
            logger.error("SQLException during updateSkill for hostId {} skillId {} aggregateVersion {}: {}", hostId, skillId, newAggregateVersion, e.getMessage(), e);
            throw new PortalPersistenceException("Persistence Error", e);
        } catch (Exception e) {
            logger.error("Exception during updateSkill for hostId {} skillId {} aggregateVersion {}: {}", hostId, skillId, newAggregateVersion, e.getMessage(), e);
            throw new PortalPersistenceException("Persistence Error", e);
        }
    }

    @Override
    public void deleteSkill(Connection conn, Map<String, Object> event) throws PortalPersistenceException {
        final String sql =
                """
                UPDATE skill_t
                SET active = FALSE,
                    update_user = ?,
                    update_ts = ?,
                    aggregate_version = ?
                WHERE host_id=? AND skill_id=? AND aggregate_version < ?
                """;
        Map<String, Object> map = SqlUtil.extractEventData(event);
        String hostId = (String)map.get("hostId");
        String skillId = (String)map.get("skillId");
        long newAggregateVersion = SqlUtil.getNewAggregateVersion(event);

        try (PreparedStatement statement = conn.prepareStatement(sql)) {
            statement.setString(1, (String)event.get(Constants.USER));
            statement.setObject(2, OffsetDateTime.parse((String)event.get(CloudEventV1.TIME)));
            statement.setLong(3, newAggregateVersion);
            statement.setObject(4, UUID.fromString(hostId));
            statement.setObject(5, UUID.fromString(skillId));
            statement.setLong(6, newAggregateVersion);

            int count = statement.executeUpdate();
            if (count == 0) {
                logger.warn("Delete skipped for hostId {} skillId {} aggregateVersion {}. Record not found or a newer/same version already exists.", hostId, skillId, newAggregateVersion);
            }
        } catch (SQLException e) {
            logger.error("SQLException during deleteSkill for hostId {} skillId {} aggregateVersion {}: {}", hostId, skillId, newAggregateVersion, e.getMessage(), e);
            throw new PortalPersistenceException("Persistence Error", e);
        } catch (Exception e) {
            logger.error("Exception during deleteSkill for hostId {} skillId {} aggregateVersion {}: {}", hostId, skillId, newAggregateVersion, e.getMessage(), e);
            throw new PortalPersistenceException("Persistence Error", e);
        }
    }

    @Override
    public Result<String> querySkill(int offset, int limit, String filtersJson, String globalFilter, String sortingJson, boolean active, String hostId) {
        Result<String> result;
        List<Map<String, Object>> filters = parseJsonList(filtersJson);
        List<Map<String, Object>> sorting = parseJsonList(sortingJson);
        String s =
                """
                SELECT COUNT(*) OVER () AS total, host_id, skill_id, parent_skill_id, name, description, content_markdown, version, update_user, update_ts, aggregate_version, active
                FROM skill_t WHERE host_id = ?
                """;
        List<Object> parameters = new ArrayList<>();
        parameters.add(UUID.fromString(hostId));
        String activeClause = SqlUtil.buildMultiTableActiveClause(active);
        String[] searchColumns = {"name", "description"};
        String sqlBuilder = s + activeClause +
                dynamicFilter(Arrays.asList("host_id", "skill_id", "parent_skill_id"), Arrays.asList(searchColumns), filters, null, parameters) +
                globalFilter(globalFilter, searchColumns, parameters) +
                dynamicSorting("name", sorting, null) +
                "\nLIMIT ? OFFSET ?";
        parameters.add(limit);
        parameters.add(offset);
        if(logger.isTraceEnabled()) logger.trace("querySkill sql: {}", sqlBuilder);
        int total = 0;
        List<Map<String, Object>> skills = new ArrayList<>();
        try (final Connection conn = ds.getConnection(); PreparedStatement preparedStatement = conn.prepareStatement(sqlBuilder)) {
            populateParameters(preparedStatement, parameters);
            boolean isFirstRow = true;
            try (ResultSet resultSet = preparedStatement.executeQuery()) {
                while (resultSet.next()) {
                    Map<String, Object> map = new HashMap<>();
                    if (isFirstRow) {
                        total = resultSet.getInt("total");
                        isFirstRow = false;
                    }
                    map.put("hostId", resultSet.getObject("host_id", UUID.class));
                    map.put("skillId", resultSet.getObject("skill_id", UUID.class));
                    map.put("parentSkillId", resultSet.getObject("parent_skill_id", UUID.class));
                    map.put("name", resultSet.getString("name"));
                    map.put("description", resultSet.getString("description"));
                    map.put("contentMarkdown", resultSet.getString("content_markdown"));
                    map.put("version", resultSet.getString("version"));
                    map.put("updateUser", resultSet.getString("update_user"));
                    map.put("updateTs", resultSet.getObject("update_ts", OffsetDateTime.class));
                    map.put("aggregateVersion", resultSet.getLong("aggregate_version"));
                    map.put("active", resultSet.getBoolean("active"));
                    skills.add(map);
                }
            }
            Map<String, Object> resultMap = new HashMap<>();
            resultMap.put("total", total);
            resultMap.put("skills", skills);
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
    public Result<String> getSkillById(String hostId, String skillId) {
        final String sql = "SELECT host_id, skill_id, parent_skill_id, name, description, content_markdown, version, update_user, update_ts, aggregate_version, active FROM skill_t WHERE host_id = ? AND skill_id = ?";
        Result<String> result;
        Map<String, Object> map = new HashMap<>();
        try (Connection conn = ds.getConnection(); PreparedStatement statement = conn.prepareStatement(sql)) {
            statement.setObject(1, UUID.fromString(hostId));
            statement.setObject(2, UUID.fromString(skillId));
            try (ResultSet resultSet = statement.executeQuery()) {
                if (resultSet.next()) {
                    map.put("hostId", resultSet.getObject("host_id", UUID.class));
                    map.put("skillId", resultSet.getObject("skill_id", UUID.class));
                    map.put("parentSkillId", resultSet.getObject("parent_skill_id", UUID.class));
                    map.put("name", resultSet.getString("name"));
                    map.put("description", resultSet.getString("description"));
                    map.put("contentMarkdown", resultSet.getString("content_markdown"));
                    map.put("version", resultSet.getString("version"));
                    map.put("updateUser", resultSet.getString("update_user"));
                    map.put("updateTs", resultSet.getObject("update_ts", OffsetDateTime.class));
                    map.put("aggregateVersion", resultSet.getLong("aggregate_version"));
                    map.put("active", resultSet.getBoolean("active"));
                    result = Success.of(JsonMapper.toJson(map));
                } else {
                    result = Failure.of(new Status(OBJECT_NOT_FOUND, "skill", skillId));
                }
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
public void createToolParam(Connection conn, Map<String, Object> event) throws PortalPersistenceException {
    final String sql =
            """
            INSERT INTO tool_param_t (host_id, param_id, tool_id, name, param_type, required, default_value, description, validation_schema, order_index, update_user, update_ts, aggregate_version, active)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, TRUE)
            ON CONFLICT (host_id, param_id) DO UPDATE
            SET tool_id = EXCLUDED.tool_id,
                name = EXCLUDED.name,
                param_type = EXCLUDED.param_type,
                required = EXCLUDED.required,
                default_value = EXCLUDED.default_value,
                description = EXCLUDED.description,
                validation_schema = EXCLUDED.validation_schema,
                order_index = EXCLUDED.order_index,
                update_user = EXCLUDED.update_user,
                update_ts = EXCLUDED.update_ts,
                aggregate_version = EXCLUDED.aggregate_version,
                active = TRUE
            WHERE tool_param_t.aggregate_version < EXCLUDED.aggregate_version
            AND tool_param_t.active = FALSE
            """;
    Map<String, Object> map = SqlUtil.extractEventData(event);
    String hostId = (String)map.get("hostId");
    String paramId = (String)map.get("paramId");
    long newAggregateVersion = SqlUtil.getNewAggregateVersion(event);

    try (PreparedStatement statement = conn.prepareStatement(sql)) {
        int i = 1;
        statement.setObject(i++, UUID.fromString(hostId));
        statement.setObject(i++, UUID.fromString(paramId));
        statement.setObject(i++, UUID.fromString((String)map.get("toolId")));
        statement.setString(i++, (String)map.get("name"));
        statement.setString(i++, (String)map.get("paramType"));

        if(map.get("required") != null) {
            statement.setBoolean(i++, (Boolean)map.get("required"));
        } else {
            statement.setBoolean(i++, true);
        }

        if(map.get("defaultValue") != null) {
            statement.setObject(i++, JsonMapper.toJson(map.get("defaultValue")), Types.OTHER);
        } else {
            statement.setNull(i++, Types.OTHER);
        }

        statement.setString(i++, (String)map.get("description"));

        if(map.get("validationSchema") != null) {
            statement.setObject(i++, JsonMapper.toJson(map.get("validationSchema")), Types.OTHER);
        } else {
            statement.setNull(i++, Types.OTHER);
        }

        setIntegerOrNull(statement, i++, map.get("orderIndex"));
        statement.setString(i++, (String)event.get(Constants.USER));
        statement.setObject(i++, OffsetDateTime.parse((String)event.get(CloudEventV1.TIME)));
        statement.setLong(i++, newAggregateVersion);

        int count = statement.executeUpdate();
        if (count == 0) {
            logger.warn("Creation skipped for hostId {} paramId {} aggregateVersion {}. A newer or same version already exists.", hostId, paramId, newAggregateVersion);
        }
    } catch (SQLException e) {
        logger.error("SQLException during createToolParam for hostId {} paramId {} aggregateVersion {}: {}", hostId, paramId, newAggregateVersion, e.getMessage(), e);
        throw new PortalPersistenceException("Persistence Error", e);
    } catch (Exception e) {
        logger.error("Exception during createToolParam for hostId {} paramId {} aggregateVersion {}: {}", hostId, paramId, newAggregateVersion, e.getMessage(), e);
        throw new PortalPersistenceException("Persistence Error", e);
    }
}

    @Override
public void updateToolParam(Connection conn, Map<String, Object> event) throws PortalPersistenceException {
    final String sql =
            """
            UPDATE tool_param_t
            SET tool_id=?, name=?, param_type=?, required=?, default_value=?, description=?, validation_schema=?, order_index=?, update_user=?, update_ts=?, aggregate_version=?, active=TRUE
            WHERE host_id=? AND param_id=? AND aggregate_version < ?
            """;
    Map<String, Object> map = SqlUtil.extractEventData(event);
    String hostId = (String)map.get("hostId");
    String paramId = (String)map.get("paramId");
    long newAggregateVersion = SqlUtil.getNewAggregateVersion(event);

    try (PreparedStatement statement = conn.prepareStatement(sql)) {
        int i = 1;
        statement.setObject(i++, UUID.fromString((String)map.get("toolId")));
        statement.setString(i++, (String)map.get("name"));
        statement.setString(i++, (String)map.get("paramType"));

        if(map.get("required") != null) {
            statement.setBoolean(i++, (Boolean)map.get("required"));
        } else {
            statement.setBoolean(i++, true);
        }

        if(map.get("defaultValue") != null) {
            statement.setObject(i++, JsonMapper.toJson(map.get("defaultValue")), Types.OTHER);
        } else {
            statement.setNull(i++, Types.OTHER);
        }

        statement.setString(i++, (String)map.get("description"));

        if(map.get("validationSchema") != null) {
            statement.setObject(i++, JsonMapper.toJson(map.get("validationSchema")), Types.OTHER);
        } else {
            statement.setNull(i++, Types.OTHER);
        }

        setIntegerOrNull(statement, i++, map.get("orderIndex"));
        statement.setString(i++, (String)event.get(Constants.USER));
        statement.setObject(i++, OffsetDateTime.parse((String)event.get(CloudEventV1.TIME)));
        statement.setLong(i++, newAggregateVersion);
        statement.setObject(i++, UUID.fromString(hostId));
        statement.setObject(i++, UUID.fromString(paramId));
        statement.setLong(i++, newAggregateVersion);

        int count = statement.executeUpdate();
        if (count == 0) {
            logger.warn("Update skipped for hostId {} paramId {} aggregateVersion {}. Record not found or a newer/same version already exists.", hostId, paramId, newAggregateVersion);
        }
    } catch (SQLException e) {
        logger.error("SQLException during updateToolParam for hostId {} paramId {} aggregateVersion {}: {}", hostId, paramId, newAggregateVersion, e.getMessage(), e);
        throw new PortalPersistenceException("Persistence Error", e);
    } catch (Exception e) {
        logger.error("Exception during updateToolParam for hostId {} paramId {} aggregateVersion {}: {}", hostId, paramId, newAggregateVersion, e.getMessage(), e);
        throw new PortalPersistenceException("Persistence Error", e);
    }
}

    @Override
public void deleteToolParam(Connection conn, Map<String, Object> event) throws PortalPersistenceException {
    final String sql =
            """
            UPDATE tool_param_t
            SET active = FALSE,
                update_user = ?,
                update_ts = ?,
                aggregate_version = ?
            WHERE host_id=? AND param_id=? AND aggregate_version < ?
            """;
    Map<String, Object> map = SqlUtil.extractEventData(event);
    String hostId = (String)map.get("hostId");
    String paramId = (String)map.get("paramId");
    long newAggregateVersion = SqlUtil.getNewAggregateVersion(event);

    try (PreparedStatement statement = conn.prepareStatement(sql)) {
        statement.setString(1, (String)event.get(Constants.USER));
        statement.setObject(2, OffsetDateTime.parse((String)event.get(CloudEventV1.TIME)));
        statement.setLong(3, newAggregateVersion);
        statement.setObject(4, UUID.fromString(hostId));
        statement.setObject(5, UUID.fromString(paramId));
        statement.setLong(6, newAggregateVersion);

        int count = statement.executeUpdate();
        if (count == 0) {
            logger.warn("Delete skipped for hostId {} paramId {} aggregateVersion {}. Record not found or a newer/same version already exists.", hostId, paramId, newAggregateVersion);
        }
    } catch (SQLException e) {
        logger.error("SQLException during deleteToolParam for hostId {} paramId {} aggregateVersion {}: {}", hostId, paramId, newAggregateVersion, e.getMessage(), e);
        throw new PortalPersistenceException("Persistence Error", e);
    } catch (Exception e) {
        logger.error("Exception during deleteToolParam for hostId {} paramId {} aggregateVersion {}: {}", hostId, paramId, newAggregateVersion, e.getMessage(), e);
        throw new PortalPersistenceException("Persistence Error", e);
    }
}

    @Override
public Result<String> queryToolParam(int offset, int limit, String filtersJson, String globalFilter, String sortingJson, boolean active, String hostId) {
    Result<String> result;
    List<Map<String, Object>> filters = parseJsonList(filtersJson);
    List<Map<String, Object>> sorting = parseJsonList(sortingJson);
    String s =
            """
            SELECT COUNT(*) OVER () AS total, host_id, param_id, tool_id, name, param_type, required, default_value, description, validation_schema, order_index, update_user, update_ts, aggregate_version, active
            FROM tool_param_t WHERE host_id = ?
            """;
    List<Object> parameters = new ArrayList<>();
    parameters.add(UUID.fromString(hostId));
    String activeClause = SqlUtil.buildMultiTableActiveClause(active);
    String[] searchColumns = {"name", "param_type", "description"};
    String sqlBuilder = s + activeClause +
            dynamicFilter(Arrays.asList("host_id", "param_id", "tool_id"), Arrays.asList(searchColumns), filters, null, parameters) +
            globalFilter(globalFilter, searchColumns, parameters) +
            dynamicSorting("order_index", sorting, null) +
            "\nLIMIT ? OFFSET ?";
    parameters.add(limit);
    parameters.add(offset);
    if(logger.isTraceEnabled()) logger.trace("queryToolParam sql: {}", sqlBuilder);
    int total = 0;
    List<Map<String, Object>> toolParams = new ArrayList<>();
    try (final Connection conn = ds.getConnection(); PreparedStatement preparedStatement = conn.prepareStatement(sqlBuilder)) {
        populateParameters(preparedStatement, parameters);
        boolean isFirstRow = true;
        try (ResultSet resultSet = preparedStatement.executeQuery()) {
            while (resultSet.next()) {
                Map<String, Object> map = new HashMap<>();
                if (isFirstRow) {
                    total = resultSet.getInt("total");
                    isFirstRow = false;
                }
                map.put("hostId", resultSet.getObject("host_id", UUID.class));
                map.put("paramId", resultSet.getObject("param_id", UUID.class));
                map.put("toolId", resultSet.getObject("tool_id", UUID.class));
                map.put("name", resultSet.getString("name"));
                map.put("paramType", resultSet.getString("param_type"));
                map.put("required", resultSet.getBoolean("required"));
                map.put("defaultValue", resultSet.getString("default_value"));
                map.put("description", resultSet.getString("description"));
                map.put("validationSchema", resultSet.getString("validation_schema"));
                map.put("orderIndex", resultSet.getObject("order_index") != null ? resultSet.getInt("order_index") : null);
                map.put("updateUser", resultSet.getString("update_user"));
                map.put("updateTs", resultSet.getObject("update_ts", OffsetDateTime.class));
                map.put("aggregateVersion", resultSet.getLong("aggregate_version"));
                map.put("active", resultSet.getBoolean("active"));
                toolParams.add(map);
            }
        }
        Map<String, Object> resultMap = new HashMap<>();
        resultMap.put("total", total);
        resultMap.put("toolParams", toolParams);
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
public Result<String> getToolParamById(String hostId, String paramId) {
    final String sql = "SELECT host_id, param_id, tool_id, name, param_type, required, default_value, description, validation_schema, order_index, update_user, update_ts, aggregate_version, active FROM tool_param_t WHERE host_id = ? AND param_id = ?";
    Result<String> result;
    Map<String, Object> map = new HashMap<>();
    try (Connection conn = ds.getConnection(); PreparedStatement statement = conn.prepareStatement(sql)) {
        statement.setObject(1, UUID.fromString(hostId));
        statement.setObject(2, UUID.fromString(paramId));
        try (ResultSet resultSet = statement.executeQuery()) {
            if (resultSet.next()) {
                map.put("hostId", resultSet.getObject("host_id", UUID.class));
                map.put("paramId", resultSet.getObject("param_id", UUID.class));
                map.put("toolId", resultSet.getObject("tool_id", UUID.class));
                map.put("name", resultSet.getString("name"));
                map.put("paramType", resultSet.getString("param_type"));
                map.put("required", resultSet.getBoolean("required"));
                map.put("defaultValue", resultSet.getString("default_value"));
                map.put("description", resultSet.getString("description"));
                map.put("validationSchema", resultSet.getString("validation_schema"));
                map.put("orderIndex", resultSet.getObject("order_index") != null ? resultSet.getInt("order_index") : null);
                map.put("updateUser", resultSet.getString("update_user"));
                map.put("updateTs", resultSet.getObject("update_ts", OffsetDateTime.class));
                map.put("aggregateVersion", resultSet.getLong("aggregate_version"));
                map.put("active", resultSet.getBoolean("active"));
                result = Success.of(JsonMapper.toJson(map));
            } else {
                result = Failure.of(new Status(OBJECT_NOT_FOUND, "tool_param", paramId));
            }
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

    // --- SkillTool ---

    @Override
    public void createSkillTool(Connection conn, Map<String, Object> event) throws PortalPersistenceException {
        final String sql =
                """
                INSERT INTO skill_tool_t (host_id, skill_id, tool_id, config, access_level, update_user, update_ts, aggregate_version, active)
                VALUES (?, ?, ?, ?::jsonb, ?, ?, ?, ?, TRUE)
                ON CONFLICT (host_id, skill_id, tool_id) DO UPDATE
                SET config = EXCLUDED.config,
                    access_level = EXCLUDED.access_level,
                    update_user = EXCLUDED.update_user,
                    update_ts = EXCLUDED.update_ts,
                    aggregate_version = EXCLUDED.aggregate_version,
                    active = TRUE
                WHERE skill_tool_t.aggregate_version < EXCLUDED.aggregate_version
                AND skill_tool_t.active = FALSE
                """;
        Map<String, Object> map = SqlUtil.extractEventData(event);
        String hostId = (String)map.get("hostId");
        String skillId = (String)map.get("skillId");
        String toolId = (String)map.get("toolId");
        long newAggregateVersion = SqlUtil.getNewAggregateVersion(event);

        try (PreparedStatement statement = conn.prepareStatement(sql)) {
            int i = 1;
            statement.setObject(i++, UUID.fromString(hostId));
            statement.setObject(i++, UUID.fromString(skillId));
            statement.setObject(i++, UUID.fromString(toolId));

            String config = (String)map.get("config");
            statement.setString(i++, config == null || config.isEmpty() ? "{}" : config);

            String accessLevel = (String)map.get("accessLevel");
            statement.setString(i++, accessLevel == null ? "read" : accessLevel);

            statement.setString(i++, (String)event.get(Constants.USER));
            statement.setObject(i++, OffsetDateTime.parse((String)event.get(CloudEventV1.TIME)));
            statement.setLong(i++, newAggregateVersion);

            int count = statement.executeUpdate();
            if (count == 0) {
                logger.warn("Creation skipped for hostId {} skillId {} toolId {} aggregateVersion {}. A newer or same version already exists.", hostId, skillId, toolId, newAggregateVersion);
            }
        } catch (SQLException e) {
            logger.error("SQLException during createSkillTool for hostId {} skillId {} toolId {} aggregateVersion {}: {}", hostId, skillId, toolId, newAggregateVersion, e.getMessage(), e);
            throw new PortalPersistenceException("Persistence Error", e);
        } catch (Exception e) {
            logger.error("Exception during createSkillTool for hostId {} skillId {} toolId {} aggregateVersion {}: {}", hostId, skillId, toolId, newAggregateVersion, e.getMessage(), e);
            throw new PortalPersistenceException("Persistence Error", e);
        }
    }

    @Override
    public void updateSkillTool(Connection conn, Map<String, Object> event) throws PortalPersistenceException {
        final String sql =
                """
                UPDATE skill_tool_t
                SET config=?::jsonb, access_level=?, update_user=?, update_ts=?, aggregate_version=?, active=TRUE
                WHERE host_id=? AND skill_id=? AND tool_id=? AND aggregate_version < ?
                """;
        Map<String, Object> map = SqlUtil.extractEventData(event);
        String hostId = (String)map.get("hostId");
        String skillId = (String)map.get("skillId");
        String toolId = (String)map.get("toolId");
        long newAggregateVersion = SqlUtil.getNewAggregateVersion(event);

        try (PreparedStatement statement = conn.prepareStatement(sql)) {
            int i = 1;
            String config = (String)map.get("config");
            statement.setString(i++, config == null || config.isEmpty() ? "{}" : config);

            String accessLevel = (String)map.get("accessLevel");
            statement.setString(i++, accessLevel == null ? "read" : accessLevel);

            statement.setString(i++, (String)event.get(Constants.USER));
            statement.setObject(i++, OffsetDateTime.parse((String)event.get(CloudEventV1.TIME)));
            statement.setLong(i++, newAggregateVersion);
            statement.setObject(i++, UUID.fromString(hostId));
            statement.setObject(i++, UUID.fromString(skillId));
            statement.setObject(i++, UUID.fromString(toolId));
            statement.setLong(i++, newAggregateVersion);

            int count = statement.executeUpdate();
            if (count == 0) {
                logger.warn("Update skipped for hostId {} skillId {} toolId {} aggregateVersion {}. Record not found or a newer/same version already exists.", hostId, skillId, toolId, newAggregateVersion);
            }
        } catch (SQLException e) {
            logger.error("SQLException during updateSkillTool for hostId {} skillId {} toolId {} aggregateVersion {}: {}", hostId, skillId, toolId, newAggregateVersion, e.getMessage(), e);
            throw new PortalPersistenceException("Persistence Error", e);
        } catch (Exception e) {
            logger.error("Exception during updateSkillTool for hostId {} skillId {} toolId {} aggregateVersion {}: {}", hostId, skillId, toolId, newAggregateVersion, e.getMessage(), e);
            throw new PortalPersistenceException("Persistence Error", e);
        }
    }

    @Override
    public void deleteSkillTool(Connection conn, Map<String, Object> event) throws PortalPersistenceException {
        final String sql =
                """
                UPDATE skill_tool_t
                SET active = FALSE,
                    update_user = ?,
                    update_ts = ?,
                    aggregate_version = ?
                WHERE host_id=? AND skill_id=? AND tool_id=? AND aggregate_version < ?
                """;
        Map<String, Object> map = SqlUtil.extractEventData(event);
        String hostId = (String)map.get("hostId");
        String skillId = (String)map.get("skillId");
        String toolId = (String)map.get("toolId");
        long newAggregateVersion = SqlUtil.getNewAggregateVersion(event);

        try (PreparedStatement statement = conn.prepareStatement(sql)) {
            statement.setString(1, (String)event.get(Constants.USER));
            statement.setObject(2, OffsetDateTime.parse((String)event.get(CloudEventV1.TIME)));
            statement.setLong(3, newAggregateVersion);
            statement.setObject(4, UUID.fromString(hostId));
            statement.setObject(5, UUID.fromString(skillId));
            statement.setObject(6, UUID.fromString(toolId));
            statement.setLong(7, newAggregateVersion);

            int count = statement.executeUpdate();
            if (count == 0) {
                logger.warn("Delete skipped for hostId {} skillId {} toolId {} aggregateVersion {}. Record not found or a newer/same version already exists.", hostId, skillId, toolId, newAggregateVersion);
            }
        } catch (SQLException e) {
            logger.error("SQLException during deleteSkillTool for hostId {} skillId {} toolId {} aggregateVersion {}: {}", hostId, skillId, toolId, newAggregateVersion, e.getMessage(), e);
            throw new PortalPersistenceException("Persistence Error", e);
        } catch (Exception e) {
            logger.error("Exception during deleteSkillTool for hostId {} skillId {} toolId {} aggregateVersion {}: {}", hostId, skillId, toolId, newAggregateVersion, e.getMessage(), e);
            throw new PortalPersistenceException("Persistence Error", e);
        }
    }

    @Override
    public Result<String> querySkillTool(int offset, int limit, String filtersJson, String globalFilter, String sortingJson, boolean active, String hostId) {
        Result<String> result;
        List<Map<String, Object>> filters = parseJsonList(filtersJson);
        List<Map<String, Object>> sorting = parseJsonList(sortingJson);

        String s =
                """
                SELECT COUNT(*) OVER () AS total, host_id, skill_id, tool_id, config, access_level, update_user, update_ts, aggregate_version, active
                FROM skill_tool_t WHERE host_id = ?
                """;
        List<Object> parameters = new ArrayList<>();
        parameters.add(UUID.fromString(hostId));

        String activeClause = SqlUtil.buildMultiTableActiveClause(active);
        String[] searchColumns = {"access_level", "config"};
        String sqlBuilder = s + activeClause +
                dynamicFilter(Arrays.asList("host_id", "skill_id", "tool_id"), Arrays.asList(searchColumns), filters, null, parameters) +
                globalFilter(globalFilter, searchColumns, parameters) +
                dynamicSorting("update_ts", sorting, null) +
                "\nLIMIT ? OFFSET ?";

        parameters.add(limit);
        parameters.add(offset);

        if(logger.isTraceEnabled()) logger.trace("querySkillTool sql: {}", sqlBuilder);

        int total = 0;
        List<Map<String, Object>> items = new ArrayList<>();
        try (final Connection conn = ds.getConnection(); PreparedStatement preparedStatement = conn.prepareStatement(sqlBuilder)) {
            populateParameters(preparedStatement, parameters);
            boolean isFirstRow = true;
            try (ResultSet resultSet = preparedStatement.executeQuery()) {
                while (resultSet.next()) {
                    Map<String, Object> map = new HashMap<>();
                    if (isFirstRow) {
                        total = resultSet.getInt("total");
                        isFirstRow = false;
                    }
                    map.put("hostId", resultSet.getObject("host_id", UUID.class));
                    map.put("skillId", resultSet.getObject("skill_id", UUID.class));
                    map.put("toolId", resultSet.getObject("tool_id", UUID.class));
                    map.put("config", resultSet.getString("config"));
                    map.put("accessLevel", resultSet.getString("access_level"));
                    map.put("updateUser", resultSet.getString("update_user"));
                    map.put("updateTs", resultSet.getObject("update_ts", OffsetDateTime.class));
                    map.put("aggregateVersion", resultSet.getLong("aggregate_version"));
                    map.put("active", resultSet.getBoolean("active"));
                    items.add(map);
                }
            }
            Map<String, Object> resultMap = new HashMap<>();
            resultMap.put("total", total);
            resultMap.put("skillTools", items);
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
    public Result<String> getSkillToolById(String hostId, String skillId, String toolId) {
        Result<String> result;
        final String sql = "SELECT * FROM skill_tool_t WHERE host_id = ? AND skill_id = ? AND tool_id = ?";
        try (final Connection conn = ds.getConnection(); PreparedStatement preparedStatement = conn.prepareStatement(sql)) {
            preparedStatement.setObject(1, UUID.fromString(hostId));
            preparedStatement.setObject(2, UUID.fromString(skillId));
            preparedStatement.setObject(3, UUID.fromString(toolId));
            try (ResultSet resultSet = preparedStatement.executeQuery()) {
                if (resultSet.next()) {
                    Map<String, Object> map = new HashMap<>();
                    map.put("hostId", resultSet.getObject("host_id", UUID.class));
                    map.put("skillId", resultSet.getObject("skill_id", UUID.class));
                    map.put("toolId", resultSet.getObject("tool_id", UUID.class));
                    map.put("config", resultSet.getString("config"));
                    map.put("accessLevel", resultSet.getString("access_level"));
                    map.put("updateUser", resultSet.getString("update_user"));
                    map.put("updateTs", resultSet.getObject("update_ts", OffsetDateTime.class));
                    map.put("aggregateVersion", resultSet.getLong("aggregate_version"));
                    map.put("active", resultSet.getBoolean("active"));
                    result = Success.of(JsonMapper.toJson(map));
                } else {
                    result = Failure.of(new Status(OBJECT_NOT_FOUND, "skill_tool_t", "hostId " + hostId + " skillId " + skillId + " toolId " + toolId));
                }
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
    public void createSkillDependency(Connection conn, Map<String, Object> event) throws PortalPersistenceException {
        final String sql =
                """
                INSERT INTO skill_dependency_t (host_id, skill_id, depends_on_skill_id, required, update_user, update_ts, aggregate_version, active)
                VALUES (?, ?, ?, ?, ?, ?, ?, TRUE)
                ON CONFLICT (host_id, skill_id, depends_on_skill_id) DO UPDATE
                SET required = EXCLUDED.required,
                    update_user = EXCLUDED.update_user,
                    update_ts = EXCLUDED.update_ts,
                    aggregate_version = EXCLUDED.aggregate_version,
                    active = TRUE
                WHERE skill_dependency_t.aggregate_version < EXCLUDED.aggregate_version
                AND skill_dependency_t.active = FALSE
                """;
        Map<String, Object> map = SqlUtil.extractEventData(event);
        String hostId = (String)map.get("hostId");
        String skillId = (String)map.get("skillId");
        String dependsOnSkillId = (String)map.get("dependsOnSkillId");
        long newAggregateVersion = SqlUtil.getNewAggregateVersion(event);

        try (PreparedStatement statement = conn.prepareStatement(sql)) {
            int i = 1;
            statement.setObject(i++, UUID.fromString(hostId));
            statement.setObject(i++, UUID.fromString(skillId));
            statement.setObject(i++, UUID.fromString(dependsOnSkillId));

            if(map.get("required") != null) {
                statement.setBoolean(i++, (Boolean)map.get("required"));
            } else {
                statement.setBoolean(i++, true);
            }

            statement.setString(i++, (String)event.get(Constants.USER));
            statement.setObject(i++, OffsetDateTime.parse((String)event.get(CloudEventV1.TIME)));
            statement.setLong(i++, newAggregateVersion);

            int count = statement.executeUpdate();
            if (count == 0) {
                logger.warn("Creation skipped for hostId {} skillId {} dependsOnSkillId {} aggregateVersion {}. A newer or same version already exists.", hostId, skillId, dependsOnSkillId, newAggregateVersion);
            }
        } catch (SQLException e) {
            logger.error("SQLException during createSkillDependency for hostId {} skillId {} dependsOnSkillId {} aggregateVersion {}: {}", hostId, skillId, dependsOnSkillId, newAggregateVersion, e.getMessage(), e);
            throw new PortalPersistenceException("Persistence Error", e);
        } catch (Exception e) {
            logger.error("Exception during createSkillDependency for hostId {} skillId {} dependsOnSkillId {} aggregateVersion {}: {}", hostId, skillId, dependsOnSkillId, newAggregateVersion, e.getMessage(), e);
            throw new PortalPersistenceException("Persistence Error", e);
        }
    }

    @Override
    public void updateSkillDependency(Connection conn, Map<String, Object> event) throws PortalPersistenceException {
        final String sql =
                """
                UPDATE skill_dependency_t
                SET required=?, update_user=?, update_ts=?, aggregate_version=?, active=TRUE
                WHERE host_id=? AND skill_id=? AND depends_on_skill_id=? AND aggregate_version < ?
                """;
        Map<String, Object> map = SqlUtil.extractEventData(event);
        String hostId = (String)map.get("hostId");
        String skillId = (String)map.get("skillId");
        String dependsOnSkillId = (String)map.get("dependsOnSkillId");
        long newAggregateVersion = SqlUtil.getNewAggregateVersion(event);

        try (PreparedStatement statement = conn.prepareStatement(sql)) {
            int i = 1;
            if(map.get("required") != null) {
                statement.setBoolean(i++, (Boolean)map.get("required"));
            } else {
                statement.setBoolean(i++, true);
            }
            statement.setString(i++, (String)event.get(Constants.USER));
            statement.setObject(i++, OffsetDateTime.parse((String)event.get(CloudEventV1.TIME)));
            statement.setLong(i++, newAggregateVersion);
            statement.setObject(i++, UUID.fromString(hostId));
            statement.setObject(i++, UUID.fromString(skillId));
            statement.setObject(i++, UUID.fromString(dependsOnSkillId));
            statement.setLong(i++, newAggregateVersion);

            int count = statement.executeUpdate();
            if (count == 0) {
                logger.warn("Update skipped for hostId {} skillId {} dependsOnSkillId {} aggregateVersion {}. Record not found or a newer/same version already exists.", hostId, skillId, dependsOnSkillId, newAggregateVersion);
            }
        } catch (SQLException e) {
            logger.error("SQLException during updateSkillDependency for hostId {} skillId {} dependsOnSkillId {} aggregateVersion {}: {}", hostId, skillId, dependsOnSkillId, newAggregateVersion, e.getMessage(), e);
            throw new PortalPersistenceException("Persistence Error", e);
        } catch (Exception e) {
            logger.error("Exception during updateSkillDependency for hostId {} skillId {} dependsOnSkillId {} aggregateVersion {}: {}", hostId, skillId, dependsOnSkillId, newAggregateVersion, e.getMessage(), e);
            throw new PortalPersistenceException("Persistence Error", e);
        }
    }

    @Override
    public void deleteSkillDependency(Connection conn, Map<String, Object> event) throws PortalPersistenceException {
        final String sql =
                """
                UPDATE skill_dependency_t
                SET active = FALSE,
                    update_user = ?,
                    update_ts = ?,
                    aggregate_version = ?
                WHERE host_id=? AND skill_id=? AND depends_on_skill_id=? AND aggregate_version < ?
                """;
        Map<String, Object> map = SqlUtil.extractEventData(event);
        String hostId = (String)map.get("hostId");
        String skillId = (String)map.get("skillId");
        String dependsOnSkillId = (String)map.get("dependsOnSkillId");
        long newAggregateVersion = SqlUtil.getNewAggregateVersion(event);

        try (PreparedStatement statement = conn.prepareStatement(sql)) {
            statement.setString(1, (String)event.get(Constants.USER));
            statement.setObject(2, OffsetDateTime.parse((String)event.get(CloudEventV1.TIME)));
            statement.setLong(3, newAggregateVersion);
            statement.setObject(4, UUID.fromString(hostId));
            statement.setObject(5, UUID.fromString(skillId));
            statement.setObject(6, UUID.fromString(dependsOnSkillId));
            statement.setLong(7, newAggregateVersion);

            int count = statement.executeUpdate();
            if (count == 0) {
                logger.warn("Delete skipped for hostId {} skillId {} dependsOnSkillId {} aggregateVersion {}. Record not found or a newer/same version already exists.", hostId, skillId, dependsOnSkillId, newAggregateVersion);
            }
        } catch (SQLException e) {
            logger.error("SQLException during deleteSkillDependency for hostId {} skillId {} dependsOnSkillId {} aggregateVersion {}: {}", hostId, skillId, dependsOnSkillId, newAggregateVersion, e.getMessage(), e);
            throw new PortalPersistenceException("Persistence Error", e);
        } catch (Exception e) {
            logger.error("Exception during deleteSkillDependency for hostId {} skillId {} dependsOnSkillId {} aggregateVersion {}: {}", hostId, skillId, dependsOnSkillId, newAggregateVersion, e.getMessage(), e);
            throw new PortalPersistenceException("Persistence Error", e);
        }
    }

    @Override
    public Result<String> querySkillDependency(int offset, int limit, String filtersJson, String globalFilter, String sortingJson, boolean active, String hostId) {
        Result<String> result;
        List<Map<String, Object>> filters = parseJsonList(filtersJson);
        List<Map<String, Object>> sorting = parseJsonList(sortingJson);
        String s =
                """
                SELECT COUNT(*) OVER () AS total, host_id, skill_id, depends_on_skill_id, required, update_user, update_ts, aggregate_version, active
                FROM skill_dependency_t WHERE host_id = ?
                """;
        List<Object> parameters = new ArrayList<>();
        parameters.add(UUID.fromString(hostId));
        String activeClause = SqlUtil.buildMultiTableActiveClause(active);
        String[] searchColumns = {"skill_id", "depends_on_skill_id"};
        String sqlBuilder = s + activeClause +
                dynamicFilter(Arrays.asList("host_id", "skill_id", "depends_on_skill_id"), Collections.emptyList(), filters, null, parameters) +
                globalFilter(globalFilter, searchColumns, parameters) +
                dynamicSorting("skill_id", sorting, null) +
                "\nLIMIT ? OFFSET ?";
        parameters.add(limit);
        parameters.add(offset);
        if(logger.isTraceEnabled()) logger.trace("querySkillDependency sql: {}", sqlBuilder);
        int total = 0;
        List<Map<String, Object>> skillDependencies = new ArrayList<>();
        try (final Connection conn = ds.getConnection(); PreparedStatement preparedStatement = conn.prepareStatement(sqlBuilder)) {
            populateParameters(preparedStatement, parameters);
            boolean isFirstRow = true;
            try (ResultSet resultSet = preparedStatement.executeQuery()) {
                while (resultSet.next()) {
                    Map<String, Object> map = new HashMap<>();
                    if (isFirstRow) {
                        total = resultSet.getInt("total");
                        isFirstRow = false;
                    }
                    map.put("hostId", resultSet.getObject("host_id", UUID.class));
                    map.put("skillId", resultSet.getObject("skill_id", UUID.class));
                    map.put("dependsOnSkillId", resultSet.getObject("depends_on_skill_id", UUID.class));
                    map.put("required", resultSet.getBoolean("required"));
                    map.put("updateUser", resultSet.getString("update_user"));
                    map.put("updateTs", resultSet.getObject("update_ts", OffsetDateTime.class));
                    map.put("aggregateVersion", resultSet.getLong("aggregate_version"));
                    map.put("active", resultSet.getBoolean("active"));
                    skillDependencies.add(map);
                }
            }
            Map<String, Object> resultMap = new HashMap<>();
            resultMap.put("total", total);
            resultMap.put("skillDependencies", skillDependencies);
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
    public Result<String> getSkillDependencyById(String hostId, String skillId, String dependsOnSkillId) {
        final String sql = "SELECT host_id, skill_id, depends_on_skill_id, required, update_user, update_ts, aggregate_version, active FROM skill_dependency_t WHERE host_id = ? AND skill_id = ? AND depends_on_skill_id = ?";
        Result<String> result;
        Map<String, Object> map = new HashMap<>();
        try (Connection conn = ds.getConnection(); PreparedStatement statement = conn.prepareStatement(sql)) {
            statement.setObject(1, UUID.fromString(hostId));
            statement.setObject(2, UUID.fromString(skillId));
            statement.setObject(3, UUID.fromString(dependsOnSkillId));
            try (ResultSet resultSet = statement.executeQuery()) {
                if (resultSet.next()) {
                    map.put("hostId", resultSet.getObject("host_id", UUID.class));
                    map.put("skillId", resultSet.getObject("skill_id", UUID.class));
                    map.put("dependsOnSkillId", resultSet.getObject("depends_on_skill_id", UUID.class));
                    map.put("required", resultSet.getBoolean("required"));
                    map.put("updateUser", resultSet.getString("update_user"));
                    map.put("updateTs", resultSet.getObject("update_ts", OffsetDateTime.class));
                    map.put("aggregateVersion", resultSet.getLong("aggregate_version"));
                    map.put("active", resultSet.getBoolean("active"));
                    result = Success.of(JsonMapper.toJson(map));
                } else {
                    result = Failure.of(new Status(OBJECT_NOT_FOUND, "skill_dependency", skillId + "|" + dependsOnSkillId));
                }
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
    public void createAgentSkill(Connection conn, Map<String, Object> event) throws PortalPersistenceException {
        final String sql =
                """
                INSERT INTO agent_skill_t (host_id, agent_def_id, skill_id, config, priority, sequence_id, update_user, update_ts, aggregate_version, active)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, TRUE)
                ON CONFLICT (host_id, agent_def_id, skill_id) DO UPDATE
                SET config = EXCLUDED.config,
                    priority = EXCLUDED.priority,
                    sequence_id = EXCLUDED.sequence_id,
                    update_user = EXCLUDED.update_user,
                    update_ts = EXCLUDED.update_ts,
                    aggregate_version = EXCLUDED.aggregate_version,
                    active = TRUE
                WHERE agent_skill_t.aggregate_version < EXCLUDED.aggregate_version
                AND agent_skill_t.active = FALSE
                """;
        Map<String, Object> map = SqlUtil.extractEventData(event);
        String hostId = (String)map.get("hostId");
        String agentDefId = (String)map.get("agentDefId");
        String skillId = (String)map.get("skillId");
        long newAggregateVersion = SqlUtil.getNewAggregateVersion(event);

        try (PreparedStatement statement = conn.prepareStatement(sql)) {
            int i = 1;
            statement.setObject(i++, UUID.fromString(hostId));
            statement.setObject(i++, UUID.fromString(agentDefId));
            statement.setObject(i++, UUID.fromString(skillId));

            if(map.get("config") != null) {
                statement.setObject(i++, JsonMapper.toJson(map.get("config")), Types.OTHER);
            } else {
                statement.setObject(i++, "{}", Types.OTHER);
            }

            setIntegerOrNull(statement, i++, map.get("priority"));
            setIntegerOrNull(statement, i++, map.get("sequenceId"));

            statement.setString(i++, (String)event.get(Constants.USER));
            statement.setObject(i++, OffsetDateTime.parse((String)event.get(CloudEventV1.TIME)));
            statement.setLong(i++, newAggregateVersion);

            int count = statement.executeUpdate();
            if (count == 0) {
                logger.warn("Creation skipped for hostId {} agentDefId {} skillId {} aggregateVersion {}. A newer or same version already exists.", hostId, agentDefId, skillId, newAggregateVersion);
            }
        } catch (SQLException e) {
            logger.error("SQLException during createAgentSkill for hostId {} agentDefId {} skillId {} aggregateVersion {}: {}", hostId, agentDefId, skillId, newAggregateVersion, e.getMessage(), e);
            throw new PortalPersistenceException("Persistence Error", e);
        } catch (Exception e) {
            logger.error("Exception during createAgentSkill for hostId {} agentDefId {} skillId {} aggregateVersion {}: {}", hostId, agentDefId, skillId, newAggregateVersion, e.getMessage(), e);
            throw new PortalPersistenceException("Persistence Error", e);
        }
    }

    @Override
    public void updateAgentSkill(Connection conn, Map<String, Object> event) throws PortalPersistenceException {
        final String sql =
                """
                UPDATE agent_skill_t
                SET config=?, priority=?, sequence_id=?, update_user=?, update_ts=?, aggregate_version=?, active=TRUE
                WHERE host_id=? AND agent_def_id=? AND skill_id=? AND aggregate_version < ?
                """;
        Map<String, Object> map = SqlUtil.extractEventData(event);
        String hostId = (String)map.get("hostId");
        String agentDefId = (String)map.get("agentDefId");
        String skillId = (String)map.get("skillId");
        long newAggregateVersion = SqlUtil.getNewAggregateVersion(event);

        try (PreparedStatement statement = conn.prepareStatement(sql)) {
            int i = 1;
            if(map.get("config") != null) {
                statement.setObject(i++, JsonMapper.toJson(map.get("config")), Types.OTHER);
            } else {
                statement.setObject(i++, "{}", Types.OTHER);
            }
            setIntegerOrNull(statement, i++, map.get("priority"));
            setIntegerOrNull(statement, i++, map.get("sequenceId"));

            statement.setString(i++, (String)event.get(Constants.USER));
            statement.setObject(i++, OffsetDateTime.parse((String)event.get(CloudEventV1.TIME)));
            statement.setLong(i++, newAggregateVersion);
            statement.setObject(i++, UUID.fromString(hostId));
            statement.setObject(i++, UUID.fromString(agentDefId));
            statement.setObject(i++, UUID.fromString(skillId));
            statement.setLong(i++, newAggregateVersion);

            int count = statement.executeUpdate();
            if (count == 0) {
                logger.warn("Update skipped for hostId {} agentDefId {} skillId {} aggregateVersion {}. Record not found or a newer/same version already exists.", hostId, agentDefId, skillId, newAggregateVersion);
            }
        } catch (SQLException e) {
            logger.error("SQLException during updateAgentSkill for hostId {} agentDefId {} skillId {} aggregateVersion {}: {}", hostId, agentDefId, skillId, newAggregateVersion, e.getMessage(), e);
            throw new PortalPersistenceException("Persistence Error", e);
        } catch (Exception e) {
            logger.error("Exception during updateAgentSkill for hostId {} agentDefId {} skillId {} aggregateVersion {}: {}", hostId, agentDefId, skillId, newAggregateVersion, e.getMessage(), e);
            throw new PortalPersistenceException("Persistence Error", e);
        }
    }

    @Override
    public void deleteAgentSkill(Connection conn, Map<String, Object> event) throws PortalPersistenceException {
        final String sql =
                """
                UPDATE agent_skill_t
                SET active = FALSE,
                    update_user = ?,
                    update_ts = ?,
                    aggregate_version = ?
                WHERE host_id=? AND agent_def_id=? AND skill_id=? AND aggregate_version < ?
                """;
        Map<String, Object> map = SqlUtil.extractEventData(event);
        String hostId = (String)map.get("hostId");
        String agentDefId = (String)map.get("agentDefId");
        String skillId = (String)map.get("skillId");
        long newAggregateVersion = SqlUtil.getNewAggregateVersion(event);

        try (PreparedStatement statement = conn.prepareStatement(sql)) {
            statement.setString(1, (String)event.get(Constants.USER));
            statement.setObject(2, OffsetDateTime.parse((String)event.get(CloudEventV1.TIME)));
            statement.setLong(3, newAggregateVersion);
            statement.setObject(4, UUID.fromString(hostId));
            statement.setObject(5, UUID.fromString(agentDefId));
            statement.setObject(6, UUID.fromString(skillId));
            statement.setLong(7, newAggregateVersion);

            int count = statement.executeUpdate();
            if (count == 0) {
                logger.warn("Delete skipped for hostId {} agentDefId {} skillId {} aggregateVersion {}. Record not found or a newer/same version already exists.", hostId, agentDefId, skillId, newAggregateVersion);
            }
        } catch (SQLException e) {
            logger.error("SQLException during deleteAgentSkill for hostId {} agentDefId {} skillId {} aggregateVersion {}: {}", hostId, agentDefId, skillId, newAggregateVersion, e.getMessage(), e);
            throw new PortalPersistenceException("Persistence Error", e);
        } catch (Exception e) {
            logger.error("Exception during deleteAgentSkill for hostId {} agentDefId {} skillId {} aggregateVersion {}: {}", hostId, agentDefId, skillId, newAggregateVersion, e.getMessage(), e);
            throw new PortalPersistenceException("Persistence Error", e);
        }
    }

    @Override
    public Result<String> queryAgentSkill(int offset, int limit, String filtersJson, String globalFilter, String sortingJson, boolean active, String hostId) {
        Result<String> result;
        List<Map<String, Object>> filters = parseJsonList(filtersJson);
        List<Map<String, Object>> sorting = parseJsonList(sortingJson);
        String s =
                """
                SELECT COUNT(*) OVER () AS total, host_id, agent_def_id, skill_id, config, priority, sequence_id, update_user, update_ts, aggregate_version, active
                FROM agent_skill_t WHERE host_id = ?
                """;
        List<Object> parameters = new ArrayList<>();
        parameters.add(UUID.fromString(hostId));
        String activeClause = SqlUtil.buildMultiTableActiveClause(active);
        String[] searchColumns = {"agent_def_id", "skill_id"};
        String sqlBuilder = s + activeClause +
                dynamicFilter(Arrays.asList("host_id", "agent_def_id", "skill_id"), Collections.emptyList(), filters, null, parameters) +
                globalFilter(globalFilter, searchColumns, parameters) +
                dynamicSorting("sequence_id", sorting, null) +
                "\nLIMIT ? OFFSET ?";
        parameters.add(limit);
        parameters.add(offset);
        if(logger.isTraceEnabled()) logger.trace("queryAgentSkill sql: {}", sqlBuilder);
        int total = 0;
        List<Map<String, Object>> agentSkills = new ArrayList<>();
        try (final Connection conn = ds.getConnection(); PreparedStatement preparedStatement = conn.prepareStatement(sqlBuilder)) {
            populateParameters(preparedStatement, parameters);
            boolean isFirstRow = true;
            try (ResultSet resultSet = preparedStatement.executeQuery()) {
                while (resultSet.next()) {
                    Map<String, Object> map = new HashMap<>();
                    if (isFirstRow) {
                        total = resultSet.getInt("total");
                        isFirstRow = false;
                    }
                    map.put("hostId", resultSet.getObject("host_id", UUID.class));
                    map.put("agentDefId", resultSet.getObject("agent_def_id", UUID.class));
                    map.put("skillId", resultSet.getObject("skill_id", UUID.class));
                    map.put("config", resultSet.getString("config"));
                    map.put("priority", resultSet.getObject("priority") != null ? resultSet.getInt("priority") : null);
                    map.put("timeoutSeconds", resultSet.getObject("timeout_seconds") != null ? resultSet.getInt("timeout_seconds") : null);
                    map.put("maxRetries", resultSet.getObject("max_retries") != null ? resultSet.getInt("max_retries") : null);
                    map.put("sequenceId", resultSet.getObject("sequence_id") != null ? resultSet.getInt("sequence_id") : null);
                    map.put("updateUser", resultSet.getString("update_user"));
                    map.put("updateTs", resultSet.getObject("update_ts", OffsetDateTime.class));
                    map.put("aggregateVersion", resultSet.getLong("aggregate_version"));
                    map.put("active", resultSet.getBoolean("active"));
                    agentSkills.add(map);
                }
            }
            Map<String, Object> resultMap = new HashMap<>();
            resultMap.put("total", total);
            resultMap.put("agentSkills", agentSkills);
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
    public Result<String> getAgentSkillById(String hostId, String agentDefId, String skillId) {
        final String sql = "SELECT host_id, agent_def_id, skill_id, config, priority, sequence_id, update_user, update_ts, aggregate_version, active FROM agent_skill_t WHERE host_id = ? AND agent_def_id = ? AND skill_id = ?";
        Result<String> result;
        Map<String, Object> map = new HashMap<>();
        try (Connection conn = ds.getConnection(); PreparedStatement statement = conn.prepareStatement(sql)) {
            statement.setObject(1, UUID.fromString(hostId));
            statement.setObject(2, UUID.fromString(agentDefId));
            statement.setObject(3, UUID.fromString(skillId));
            try (ResultSet resultSet = statement.executeQuery()) {
                if (resultSet.next()) {
                    map.put("hostId", resultSet.getObject("host_id", UUID.class));
                    map.put("agentDefId", resultSet.getObject("agent_def_id", UUID.class));
                    map.put("skillId", resultSet.getObject("skill_id", UUID.class));
                    map.put("config", resultSet.getString("config"));
                    map.put("priority", resultSet.getObject("priority") != null ? resultSet.getInt("priority") : null);
                    map.put("timeoutSeconds", resultSet.getObject("timeout_seconds") != null ? resultSet.getInt("timeout_seconds") : null);
                    map.put("maxRetries", resultSet.getObject("max_retries") != null ? resultSet.getInt("max_retries") : null);
                    map.put("sequenceId", resultSet.getObject("sequence_id") != null ? resultSet.getInt("sequence_id") : null);
                    map.put("updateUser", resultSet.getString("update_user"));
                    map.put("updateTs", resultSet.getObject("update_ts", OffsetDateTime.class));
                    map.put("aggregateVersion", resultSet.getLong("aggregate_version"));
                    map.put("active", resultSet.getBoolean("active"));
                    result = Success.of(JsonMapper.toJson(map));
                } else {
                    result = Failure.of(new Status(OBJECT_NOT_FOUND, "agent_skill", agentDefId + "|" + skillId));
                }
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
    public void createAgentSessionHistory(Connection conn, Map<String, Object> event) throws PortalPersistenceException {
        final String sql =
                """
                INSERT INTO agent_session_history_t (host_id, session_history_id, process_id, task_id, role, content, tool_call_id, create_ts, create_user, active)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, TRUE)
                """;
        Map<String, Object> map = SqlUtil.extractEventData(event);
        String hostId = (String)map.get("hostId");
        String sessionHistoryId = (String)map.get("sessionHistoryId");

        try (PreparedStatement statement = conn.prepareStatement(sql)) {
            int i = 1;
            statement.setObject(i++, UUID.fromString(hostId));
            statement.setObject(i++, UUID.fromString(sessionHistoryId));
            statement.setObject(i++, UUID.fromString((String)map.get("processId")));
            setUuidOrNull(statement, i++, map.get("taskId"));
            statement.setString(i++, (String)map.get("role"));
            statement.setString(i++, (String)map.get("content"));
            statement.setString(i++, (String)map.get("toolCallId"));
            statement.setObject(i++, map.get("createTs") != null ? OffsetDateTime.parse((String)map.get("createTs")) : OffsetDateTime.now());
            statement.setString(i++, (String)event.get(Constants.USER));

            statement.executeUpdate();
        } catch (SQLException e) {
            logger.error("SQLException during createAgentSessionHistory for hostId {} sessionHistoryId {}: {}", hostId, sessionHistoryId, e.getMessage(), e);
            throw new PortalPersistenceException("Persistence Error", e);
        } catch (Exception e) {
            logger.error("Exception during createAgentSessionHistory for hostId {} sessionHistoryId {}: {}", hostId, sessionHistoryId, e.getMessage(), e);
            throw new PortalPersistenceException("Persistence Error", e);
        }
    }

    @Override
    public void deleteAgentSessionHistory(Connection conn, Map<String, Object> event) throws PortalPersistenceException {
        final String sql =
                """
                UPDATE agent_session_history_t
                SET active = FALSE
                WHERE host_id=? AND session_history_id=?
                """;
        Map<String, Object> map = SqlUtil.extractEventData(event);
        String hostId = (String)map.get("hostId");
        String sessionHistoryId = (String)map.get("sessionHistoryId");

        try (PreparedStatement statement = conn.prepareStatement(sql)) {
            statement.setObject(1, UUID.fromString(hostId));
            statement.setObject(2, UUID.fromString(sessionHistoryId));

            int count = statement.executeUpdate();
            if (count == 0) {
                logger.warn("Delete skipped for hostId {} sessionHistoryId {}. Record not found.", hostId, sessionHistoryId);
            }
        } catch (SQLException e) {
            logger.error("SQLException during deleteAgentSessionHistory for hostId {} sessionHistoryId {}: {}", hostId, sessionHistoryId, e.getMessage(), e);
            throw new PortalPersistenceException("Persistence Error", e);
        } catch (Exception e) {
            logger.error("Exception during deleteAgentSessionHistory for hostId {} sessionHistoryId {}: {}", hostId, sessionHistoryId, e.getMessage(), e);
            throw new PortalPersistenceException("Persistence Error", e);
        }
    }

    @Override
    public Result<String> queryAgentSessionHistory(int offset, int limit, String filtersJson, String globalFilter, String sortingJson, boolean active, String hostId) {
        Result<String> result;
        List<Map<String, Object>> filters = parseJsonList(filtersJson);
        List<Map<String, Object>> sorting = parseJsonList(sortingJson);
        String s =
                """
                SELECT COUNT(*) OVER () AS total, host_id, session_history_id, process_id, task_id, role, content, tool_call_id, create_ts, create_user, active
                FROM agent_session_history_t WHERE host_id = ?
                """;
        List<Object> parameters = new ArrayList<>();
        parameters.add(UUID.fromString(hostId));
        String activeClause = SqlUtil.buildMultiTableActiveClause(active);
        String[] searchColumns = {"role", "content", "tool_call_id"};
        String sqlBuilder = s + activeClause +
                dynamicFilter(Arrays.asList("host_id", "session_history_id", "process_id", "task_id"), Collections.emptyList(), filters, null, parameters) +
                globalFilter(globalFilter, searchColumns, parameters) +
                dynamicSorting("create_ts", sorting, null) +
                "\nLIMIT ? OFFSET ?";
        parameters.add(limit);
        parameters.add(offset);
        if(logger.isTraceEnabled()) logger.trace("queryAgentSessionHistory sql: {}", sqlBuilder);
        int total = 0;
        List<Map<String, Object>> histories = new ArrayList<>();
        try (final Connection conn = ds.getConnection(); PreparedStatement preparedStatement = conn.prepareStatement(sqlBuilder)) {
            populateParameters(preparedStatement, parameters);
            boolean isFirstRow = true;
            try (ResultSet resultSet = preparedStatement.executeQuery()) {
                while (resultSet.next()) {
                    Map<String, Object> map = new HashMap<>();
                    if (isFirstRow) {
                        total = resultSet.getInt("total");
                        isFirstRow = false;
                    }
                    map.put("hostId", resultSet.getObject("host_id", UUID.class));
                    map.put("sessionHistoryId", resultSet.getObject("session_history_id", UUID.class));
                    map.put("processId", resultSet.getObject("process_id", UUID.class));
                    map.put("taskId", resultSet.getObject("task_id", UUID.class));
                    map.put("role", resultSet.getString("role"));
                    map.put("content", resultSet.getString("content"));
                    map.put("toolCallId", resultSet.getString("tool_call_id"));
                    map.put("createTs", resultSet.getObject("create_ts", OffsetDateTime.class));
                    map.put("createUser", resultSet.getString("create_user"));
                    map.put("active", resultSet.getBoolean("active"));
                    histories.add(map);
                }
            }
            Map<String, Object> resultMap = new HashMap<>();
            resultMap.put("total", total);
            resultMap.put("histories", histories);
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
    public Result<String> getAgentSessionHistoryById(String hostId, String sessionHistoryId) {
        final String sql = "SELECT host_id, session_history_id, process_id, task_id, role, content, tool_call_id, create_ts, create_user, active FROM agent_session_history_t WHERE host_id = ? AND session_history_id = ?";
        Result<String> result;
        Map<String, Object> map = new HashMap<>();
        try (Connection conn = ds.getConnection(); PreparedStatement statement = conn.prepareStatement(sql)) {
            statement.setObject(1, UUID.fromString(hostId));
            statement.setObject(2, UUID.fromString(sessionHistoryId));
            try (ResultSet resultSet = statement.executeQuery()) {
                if (resultSet.next()) {
                    map.put("hostId", resultSet.getObject("host_id", UUID.class));
                    map.put("sessionHistoryId", resultSet.getObject("session_history_id", UUID.class));
                    map.put("processId", resultSet.getObject("process_id", UUID.class));
                    map.put("taskId", resultSet.getObject("task_id", UUID.class));
                    map.put("role", resultSet.getString("role"));
                    map.put("content", resultSet.getString("content"));
                    map.put("toolCallId", resultSet.getString("tool_call_id"));
                    map.put("createTs", resultSet.getObject("create_ts", OffsetDateTime.class));
                    map.put("createUser", resultSet.getString("create_user"));
                    map.put("active", resultSet.getBoolean("active"));
                    result = Success.of(JsonMapper.toJson(map));
                } else {
                    result = Failure.of(new Status(OBJECT_NOT_FOUND, "agent_session_history", sessionHistoryId));
                }
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
    public void createSessionMemory(Connection conn, Map<String, Object> event) throws PortalPersistenceException {
        final String sql =
                """
                INSERT INTO session_memory_t (host_id, mem_id, session_id, agent_def_id, user_id, content, embedding, importance_score, expires_at, metadata, update_user, update_ts, aggregate_version, active)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, TRUE)
                ON CONFLICT (host_id, mem_id) DO UPDATE
                SET session_id = EXCLUDED.session_id,
                    agent_def_id = EXCLUDED.agent_def_id,
                    user_id = EXCLUDED.user_id,
                    content = EXCLUDED.content,
                    embedding = EXCLUDED.embedding,
                    importance_score = EXCLUDED.importance_score,
                    expires_at = EXCLUDED.expires_at,
                    metadata = EXCLUDED.metadata,
                    update_user = EXCLUDED.update_user,
                    update_ts = EXCLUDED.update_ts,
                    aggregate_version = EXCLUDED.aggregate_version,
                    active = TRUE
                WHERE session_memory_t.aggregate_version < EXCLUDED.aggregate_version
                AND session_memory_t.active = FALSE
                """;
        Map<String, Object> map = SqlUtil.extractEventData(event);
        String hostId = (String)map.get("hostId");
        String memId = (String)map.get("memId");
        long newAggregateVersion = SqlUtil.getNewAggregateVersion(event);

        try (PreparedStatement statement = conn.prepareStatement(sql)) {
            int i = 1;
            statement.setObject(i++, UUID.fromString(hostId));
            statement.setObject(i++, UUID.fromString(memId));
            statement.setObject(i++, UUID.fromString((String)map.get("sessionId")));
            statement.setObject(i++, UUID.fromString((String)map.get("agentDefId")));
            setUuidOrNull(statement, i++, map.get("userId"));
            statement.setString(i++, (String)map.get("content"));

            // Embedding is VECTOR type, handle as object or custom type string if necessary
            if(map.get("embedding") != null) {
                statement.setObject(i++, map.get("embedding"), Types.OTHER);
            } else {
                statement.setNull(i++, Types.OTHER);
            }

            setFloatOrNull(statement, i++, map.get("importanceScore"));
            statement.setObject(i++, map.get("expiresAt") != null ? OffsetDateTime.parse((String)map.get("expiresAt")) : OffsetDateTime.now().plusHours(1));

            if(map.get("metadata") != null) {
                statement.setObject(i++, JsonMapper.toJson(map.get("metadata")), Types.OTHER);
            } else {
                statement.setNull(i++, Types.OTHER);
            }

            statement.setString(i++, (String)event.get(Constants.USER));
            statement.setObject(i++, OffsetDateTime.parse((String)event.get(CloudEventV1.TIME)));
            statement.setLong(i++, newAggregateVersion);

            int count = statement.executeUpdate();
            if (count == 0) {
                logger.warn("Creation skipped for hostId {} memId {} aggregateVersion {}. A newer or same version already exists.", hostId, memId, newAggregateVersion);
            }
        } catch (SQLException e) {
            logger.error("SQLException during createSessionMemory for hostId {} memId {} aggregateVersion {}: {}", hostId, memId, newAggregateVersion, e.getMessage(), e);
            throw new PortalPersistenceException("Persistence Error", e);
        } catch (Exception e) {
            logger.error("Exception during createSessionMemory for hostId {} memId {} aggregateVersion {}: {}", hostId, memId, newAggregateVersion, e.getMessage(), e);
            throw new PortalPersistenceException("Persistence Error", e);
        }
    }

    @Override
    public void updateSessionMemory(Connection conn, Map<String, Object> event) throws PortalPersistenceException {
        final String sql =
                """
                UPDATE session_memory_t
                SET session_id=?, agent_def_id=?, user_id=?, content=?, embedding=?, importance_score=?, expires_at=?, metadata=?, update_user=?, update_ts=?, aggregate_version=?, active=TRUE
                WHERE host_id=? AND mem_id=? AND aggregate_version < ?
                """;
        Map<String, Object> map = SqlUtil.extractEventData(event);
        String hostId = (String)map.get("hostId");
        String memId = (String)map.get("memId");
        long newAggregateVersion = SqlUtil.getNewAggregateVersion(event);

        try (PreparedStatement statement = conn.prepareStatement(sql)) {
            int i = 1;
            statement.setObject(i++, UUID.fromString((String)map.get("sessionId")));
            statement.setObject(i++, UUID.fromString((String)map.get("agentDefId")));
            setUuidOrNull(statement, i++, map.get("userId"));
            statement.setString(i++, (String)map.get("content"));

            if(map.get("embedding") != null) {
                statement.setObject(i++, map.get("embedding"), Types.OTHER);
            } else {
                statement.setNull(i++, Types.OTHER);
            }

            setFloatOrNull(statement, i++, map.get("importanceScore"));
            statement.setObject(i++, map.get("expiresAt") != null ? OffsetDateTime.parse((String)map.get("expiresAt")) : null);

            if(map.get("metadata") != null) {
                statement.setObject(i++, JsonMapper.toJson(map.get("metadata")), Types.OTHER);
            } else {
                statement.setNull(i++, Types.OTHER);
            }

            statement.setString(i++, (String)event.get(Constants.USER));
            statement.setObject(i++, OffsetDateTime.parse((String)event.get(CloudEventV1.TIME)));
            statement.setLong(i++, newAggregateVersion);
            statement.setObject(i++, UUID.fromString(hostId));
            statement.setObject(i++, UUID.fromString(memId));
            statement.setLong(i++, newAggregateVersion);

            int count = statement.executeUpdate();
            if (count == 0) {
                logger.warn("Update skipped for hostId {} memId {} aggregateVersion {}. Record not found or a newer/same version already exists.", hostId, memId, newAggregateVersion);
            }
        } catch (SQLException e) {
            logger.error("SQLException during updateSessionMemory for hostId {} memId {} aggregateVersion {}: {}", hostId, memId, newAggregateVersion, e.getMessage(), e);
            throw new PortalPersistenceException("Persistence Error", e);
        } catch (Exception e) {
            logger.error("Exception during updateSessionMemory for hostId {} memId {} aggregateVersion {}: {}", hostId, memId, newAggregateVersion, e.getMessage(), e);
            throw new PortalPersistenceException("Persistence Error", e);
        }
    }

    @Override
    public void deleteSessionMemory(Connection conn, Map<String, Object> event) throws PortalPersistenceException {
        final String sql =
                """
                UPDATE session_memory_t
                SET active = FALSE,
                    update_user = ?,
                    update_ts = ?,
                    aggregate_version = ?
                WHERE host_id=? AND mem_id=? AND aggregate_version < ?
                """;
        Map<String, Object> map = SqlUtil.extractEventData(event);
        String hostId = (String)map.get("hostId");
        String memId = (String)map.get("memId");
        long newAggregateVersion = SqlUtil.getNewAggregateVersion(event);

        try (PreparedStatement statement = conn.prepareStatement(sql)) {
            statement.setString(1, (String)event.get(Constants.USER));
            statement.setObject(2, OffsetDateTime.parse((String)event.get(CloudEventV1.TIME)));
            statement.setLong(3, newAggregateVersion);
            statement.setObject(4, UUID.fromString(hostId));
            statement.setObject(5, UUID.fromString(memId));
            statement.setLong(6, newAggregateVersion);

            int count = statement.executeUpdate();
            if (count == 0) {
                logger.warn("Delete skipped for hostId {} memId {} aggregateVersion {}. Record not found or a newer/same version already exists.", hostId, memId, newAggregateVersion);
            }
        } catch (SQLException e) {
            logger.error("SQLException during deleteSessionMemory for hostId {} memId {} aggregateVersion {}: {}", hostId, memId, newAggregateVersion, e.getMessage(), e);
            throw new PortalPersistenceException("Persistence Error", e);
        } catch (Exception e) {
            logger.error("Exception during deleteSessionMemory for hostId {} memId {} aggregateVersion {}: {}", hostId, memId, newAggregateVersion, e.getMessage(), e);
            throw new PortalPersistenceException("Persistence Error", e);
        }
    }

    @Override
    public Result<String> querySessionMemory(int offset, int limit, String filtersJson, String globalFilter, String sortingJson, boolean active, String hostId) {
        Result<String> result;
        List<Map<String, Object>> filters = parseJsonList(filtersJson);
        List<Map<String, Object>> sorting = parseJsonList(sortingJson);
        String s =
                """
                SELECT COUNT(*) OVER () AS total, host_id, mem_id, session_id, agent_def_id, user_id, content, importance_score, expires_at, metadata, update_user, update_ts, aggregate_version, active
                FROM session_memory_t WHERE host_id = ?
                """;
        List<Object> parameters = new ArrayList<>();
        parameters.add(UUID.fromString(hostId));
        String activeClause = SqlUtil.buildMultiTableActiveClause(active);
        String[] searchColumns = {"content", "metadata::text"};
        String sqlBuilder = s + activeClause +
                dynamicFilter(Arrays.asList("host_id", "mem_id", "session_id", "agent_def_id", "user_id"), Collections.emptyList(), filters, null, parameters) +
                globalFilter(globalFilter, searchColumns, parameters) +
                dynamicSorting("update_ts DESC", sorting, null) +
                "\nLIMIT ? OFFSET ?";
        parameters.add(limit);
        parameters.add(offset);
        if(logger.isTraceEnabled()) logger.trace("querySessionMemory sql: {}", sqlBuilder);
        int total = 0;
        List<Map<String, Object>> memories = new ArrayList<>();
        try (final Connection conn = ds.getConnection(); PreparedStatement preparedStatement = conn.prepareStatement(sqlBuilder)) {
            populateParameters(preparedStatement, parameters);
            boolean isFirstRow = true;
            try (ResultSet resultSet = preparedStatement.executeQuery()) {
                while (resultSet.next()) {
                    Map<String, Object> map = new HashMap<>();
                    if (isFirstRow) {
                        total = resultSet.getInt("total");
                        isFirstRow = false;
                    }
                    map.put("hostId", resultSet.getObject("host_id", UUID.class));
                    map.put("memId", resultSet.getObject("mem_id", UUID.class));
                    map.put("sessionId", resultSet.getObject("session_id", UUID.class));
                    map.put("agentDefId", resultSet.getObject("agent_def_id", UUID.class));
                    map.put("userId", resultSet.getObject("user_id", UUID.class));
                    map.put("content", resultSet.getString("content"));
                    map.put("importanceScore", resultSet.getFloat("importance_score"));
                    map.put("expiresAt", resultSet.getObject("expires_at", OffsetDateTime.class));
                    map.put("metadata", resultSet.getString("metadata"));
                    map.put("updateUser", resultSet.getString("update_user"));
                    map.put("updateTs", resultSet.getObject("update_ts", OffsetDateTime.class));
                    map.put("aggregateVersion", resultSet.getLong("aggregate_version"));
                    map.put("active", resultSet.getBoolean("active"));
                    memories.add(map);
                }
            }
            Map<String, Object> resultMap = new HashMap<>();
            resultMap.put("total", total);
            resultMap.put("memories", memories);
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
    public Result<String> getSessionMemoryById(String hostId, String memId) {
        final String sql = "SELECT host_id, mem_id, session_id, agent_def_id, user_id, content, importance_score, expires_at, metadata, update_user, update_ts, aggregate_version, active FROM session_memory_t WHERE host_id = ? AND mem_id = ?";
        Result<String> result;
        Map<String, Object> map = new HashMap<>();
        try (Connection conn = ds.getConnection(); PreparedStatement statement = conn.prepareStatement(sql)) {
            statement.setObject(1, UUID.fromString(hostId));
            statement.setObject(2, UUID.fromString(memId));
            try (ResultSet resultSet = statement.executeQuery()) {
                if (resultSet.next()) {
                    map.put("hostId", resultSet.getObject("host_id", UUID.class));
                    map.put("memId", resultSet.getObject("mem_id", UUID.class));
                    map.put("sessionId", resultSet.getObject("session_id", UUID.class));
                    map.put("agentDefId", resultSet.getObject("agent_def_id", UUID.class));
                    map.put("userId", resultSet.getObject("user_id", UUID.class));
                    map.put("content", resultSet.getString("content"));
                    map.put("importanceScore", resultSet.getFloat("importance_score"));
                    map.put("expiresAt", resultSet.getObject("expires_at", OffsetDateTime.class));
                    map.put("metadata", resultSet.getString("metadata"));
                    map.put("updateUser", resultSet.getString("update_user"));
                    map.put("updateTs", resultSet.getObject("update_ts", OffsetDateTime.class));
                    map.put("aggregateVersion", resultSet.getLong("aggregate_version"));
                    map.put("active", resultSet.getBoolean("active"));
                    result = Success.of(JsonMapper.toJson(map));
                } else {
                    result = Failure.of(new Status(OBJECT_NOT_FOUND, "session_memory", memId));
                }
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
    public void createUserMemory(Connection conn, Map<String, Object> event) throws PortalPersistenceException {
        final String sql =
                """
                INSERT INTO user_memory_t (host_id, mem_id, user_id, agent_def_id, content, embedding, memory_type, importance_score, access_count, last_accessed, metadata, update_user, update_ts, aggregate_version, active)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, TRUE)
                ON CONFLICT (host_id, mem_id) DO UPDATE
                SET user_id = EXCLUDED.user_id,
                    agent_def_id = EXCLUDED.agent_def_id,
                    content = EXCLUDED.content,
                    embedding = EXCLUDED.embedding,
                    memory_type = EXCLUDED.memory_type,
                    importance_score = EXCLUDED.importance_score,
                    access_count = EXCLUDED.access_count,
                    last_accessed = EXCLUDED.last_accessed,
                    metadata = EXCLUDED.metadata,
                    update_user = EXCLUDED.update_user,
                    update_ts = EXCLUDED.update_ts,
                    aggregate_version = EXCLUDED.aggregate_version,
                    active = TRUE
                WHERE user_memory_t.aggregate_version < EXCLUDED.aggregate_version
                AND user_memory_t.active = FALSE
                """;
        Map<String, Object> map = SqlUtil.extractEventData(event);
        String hostId = (String)map.get("hostId");
        String memId = (String)map.get("memId");
        long newAggregateVersion = SqlUtil.getNewAggregateVersion(event);

        try (PreparedStatement statement = conn.prepareStatement(sql)) {
            int i = 1;
            statement.setObject(i++, UUID.fromString(hostId));
            statement.setObject(i++, UUID.fromString(memId));
            statement.setObject(i++, UUID.fromString((String)map.get("userId")));
            setUuidOrNull(statement, i++, map.get("agentDefId"));
            statement.setString(i++, (String)map.get("content"));

            if(map.get("embedding") != null) {
                statement.setObject(i++, map.get("embedding"), Types.OTHER);
            } else {
                statement.setNull(i++, Types.OTHER);
            }

            statement.setString(i++, (String)map.get("memoryType"));
            setFloatOrNull(statement, i++, map.get("importanceScore"));
            setIntegerOrNull(statement, i++, map.get("accessCount"));
            statement.setObject(i++, map.get("lastAccessed") != null ? OffsetDateTime.parse((String)map.get("lastAccessed")) : OffsetDateTime.now());

            if(map.get("metadata") != null) {
                statement.setObject(i++, JsonMapper.toJson(map.get("metadata")), Types.OTHER);
            } else {
                statement.setNull(i++, Types.OTHER);
            }

            statement.setString(i++, (String)event.get(Constants.USER));
            statement.setObject(i++, OffsetDateTime.parse((String)event.get(CloudEventV1.TIME)));
            statement.setLong(i++, newAggregateVersion);

            int count = statement.executeUpdate();
            if (count == 0) {
                logger.warn("Creation skipped for hostId {} memId {} aggregateVersion {}. A newer or same version already exists.", hostId, memId, newAggregateVersion);
            }
        } catch (SQLException e) {
            logger.error("SQLException during createUserMemory for hostId {} memId {} aggregateVersion {}: {}", hostId, memId, newAggregateVersion, e.getMessage(), e);
            throw new PortalPersistenceException("Persistence Error", e);
        } catch (Exception e) {
            logger.error("Exception during createUserMemory for hostId {} memId {} aggregateVersion {}: {}", hostId, memId, newAggregateVersion, e.getMessage(), e);
            throw new PortalPersistenceException("Persistence Error", e);
        }
    }

    @Override
    public void updateUserMemory(Connection conn, Map<String, Object> event) throws PortalPersistenceException {
        final String sql =
                """
                UPDATE user_memory_t
                SET user_id=?, agent_def_id=?, content=?, embedding=?, memory_type=?, importance_score=?, access_count=?, last_accessed=?, metadata=?, update_user=?, update_ts=?, aggregate_version=?, active=TRUE
                WHERE host_id=? AND mem_id=? AND aggregate_version < ?
                """;
        Map<String, Object> map = SqlUtil.extractEventData(event);
        String hostId = (String)map.get("hostId");
        String memId = (String)map.get("memId");
        long newAggregateVersion = SqlUtil.getNewAggregateVersion(event);

        try (PreparedStatement statement = conn.prepareStatement(sql)) {
            int i = 1;
            statement.setObject(i++, UUID.fromString((String)map.get("userId")));
            setUuidOrNull(statement, i++, map.get("agentDefId"));
            statement.setString(i++, (String)map.get("content"));

            if(map.get("embedding") != null) {
                statement.setObject(i++, map.get("embedding"), Types.OTHER);
            } else {
                statement.setNull(i++, Types.OTHER);
            }

            statement.setString(i++, (String)map.get("memoryType"));
            setFloatOrNull(statement, i++, map.get("importanceScore"));
            setIntegerOrNull(statement, i++, map.get("accessCount"));
            statement.setObject(i++, map.get("lastAccessed") != null ? OffsetDateTime.parse((String)map.get("lastAccessed")) : null);

            if(map.get("metadata") != null) {
                statement.setObject(i++, JsonMapper.toJson(map.get("metadata")), Types.OTHER);
            } else {
                statement.setNull(i++, Types.OTHER);
            }

            statement.setString(i++, (String)event.get(Constants.USER));
            statement.setObject(i++, OffsetDateTime.parse((String)event.get(CloudEventV1.TIME)));
            statement.setLong(i++, newAggregateVersion);
            statement.setObject(i++, UUID.fromString(hostId));
            statement.setObject(i++, UUID.fromString(memId));
            statement.setLong(i++, newAggregateVersion);

            int count = statement.executeUpdate();
            if (count == 0) {
                logger.warn("Update skipped for hostId {} memId {} aggregateVersion {}. Record not found or a newer/same version already exists.", hostId, memId, newAggregateVersion);
            }
        } catch (SQLException e) {
            logger.error("SQLException during updateUserMemory for hostId {} memId {} aggregateVersion {}: {}", hostId, memId, newAggregateVersion, e.getMessage(), e);
            throw new PortalPersistenceException("Persistence Error", e);
        } catch (Exception e) {
            logger.error("Exception during updateUserMemory for hostId {} memId {} aggregateVersion {}: {}", hostId, memId, newAggregateVersion, e.getMessage(), e);
            throw new PortalPersistenceException("Persistence Error", e);
        }
    }

    @Override
    public void deleteUserMemory(Connection conn, Map<String, Object> event) throws PortalPersistenceException {
        final String sql =
                """
                UPDATE user_memory_t
                SET active = FALSE,
                    update_user = ?,
                    update_ts = ?,
                    aggregate_version = ?
                WHERE host_id=? AND mem_id=? AND aggregate_version < ?
                """;
        Map<String, Object> map = SqlUtil.extractEventData(event);
        String hostId = (String)map.get("hostId");
        String memId = (String)map.get("memId");
        long newAggregateVersion = SqlUtil.getNewAggregateVersion(event);

        try (PreparedStatement statement = conn.prepareStatement(sql)) {
            statement.setString(1, (String)event.get(Constants.USER));
            statement.setObject(2, OffsetDateTime.parse((String)event.get(CloudEventV1.TIME)));
            statement.setLong(3, newAggregateVersion);
            statement.setObject(4, UUID.fromString(hostId));
            statement.setObject(5, UUID.fromString(memId));
            statement.setLong(6, newAggregateVersion);

            int count = statement.executeUpdate();
            if (count == 0) {
                logger.warn("Delete skipped for hostId {} memId {} aggregateVersion {}. Record not found or a newer/same version already exists.", hostId, memId, newAggregateVersion);
            }
        } catch (SQLException e) {
            logger.error("SQLException during deleteUserMemory for hostId {} memId {} aggregateVersion {}: {}", hostId, memId, newAggregateVersion, e.getMessage(), e);
            throw new PortalPersistenceException("Persistence Error", e);
        } catch (Exception e) {
            logger.error("Exception during deleteUserMemory for hostId {} memId {} aggregateVersion {}: {}", hostId, memId, newAggregateVersion, e.getMessage(), e);
            throw new PortalPersistenceException("Persistence Error", e);
        }
    }

    @Override
    public Result<String> queryUserMemory(int offset, int limit, String filtersJson, String globalFilter, String sortingJson, boolean active, String hostId) {
        Result<String> result;
        List<Map<String, Object>> filters = parseJsonList(filtersJson);
        List<Map<String, Object>> sorting = parseJsonList(sortingJson);
        String s =
                """
                SELECT COUNT(*) OVER () AS total, host_id, mem_id, user_id, agent_def_id, content, memory_type, importance_score, access_count, last_accessed, metadata, update_user, update_ts, aggregate_version, active
                FROM user_memory_t WHERE host_id = ?
                """;
        List<Object> parameters = new ArrayList<>();
        parameters.add(UUID.fromString(hostId));
        String activeClause = SqlUtil.buildMultiTableActiveClause(active);
        String[] searchColumns = {"content", "memory_type", "metadata::text"};
        String sqlBuilder = s + activeClause +
                dynamicFilter(Arrays.asList("host_id", "mem_id", "user_id", "agent_def_id"), Collections.emptyList(), filters, null, parameters) +
                globalFilter(globalFilter, searchColumns, parameters) +
                dynamicSorting("last_accessed DESC", sorting, null) +
                "\nLIMIT ? OFFSET ?";
        parameters.add(limit);
        parameters.add(offset);
        if(logger.isTraceEnabled()) logger.trace("queryUserMemory sql: {}", sqlBuilder);
        int total = 0;
        List<Map<String, Object>> memories = new ArrayList<>();
        try (final Connection conn = ds.getConnection(); PreparedStatement preparedStatement = conn.prepareStatement(sqlBuilder)) {
            populateParameters(preparedStatement, parameters);
            boolean isFirstRow = true;
            try (ResultSet resultSet = preparedStatement.executeQuery()) {
                while (resultSet.next()) {
                    Map<String, Object> map = new HashMap<>();
                    if (isFirstRow) {
                        total = resultSet.getInt("total");
                        isFirstRow = false;
                    }
                    map.put("hostId", resultSet.getObject("host_id", UUID.class));
                    map.put("memId", resultSet.getObject("mem_id", UUID.class));
                    map.put("userId", resultSet.getObject("user_id", UUID.class));
                    map.put("agentDefId", resultSet.getObject("agent_def_id", UUID.class));
                    map.put("content", resultSet.getString("content"));
                    map.put("memoryType", resultSet.getString("memory_type"));
                    map.put("importanceScore", resultSet.getFloat("importance_score"));
                    map.put("accessCount", resultSet.getObject("access_count") != null ? resultSet.getInt("access_count") : null);
                    map.put("lastAccessed", resultSet.getObject("last_accessed", OffsetDateTime.class));
                    map.put("metadata", resultSet.getString("metadata"));
                    map.put("updateUser", resultSet.getString("update_user"));
                    map.put("updateTs", resultSet.getObject("update_ts", OffsetDateTime.class));
                    map.put("aggregateVersion", resultSet.getLong("aggregate_version"));
                    map.put("active", resultSet.getBoolean("active"));
                    memories.add(map);
                }
            }
            Map<String, Object> resultMap = new HashMap<>();
            resultMap.put("total", total);
            resultMap.put("memories", memories);
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
    public void createOrgMemory(Connection conn, Map<String, Object> event) throws PortalPersistenceException {
        final String sql =
                """
                INSERT INTO org_memory_t (host_id, mem_id, domain, source, content, embedding, chunk_index, document_id, metadata, update_user, update_ts, aggregate_version, active)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, TRUE)
                ON CONFLICT (host_id, mem_id) DO UPDATE
                SET domain = EXCLUDED.domain,
                    source = EXCLUDED.source,
                    content = EXCLUDED.content,
                    embedding = EXCLUDED.embedding,
                    chunk_index = EXCLUDED.chunk_index,
                    document_id = EXCLUDED.document_id,
                    metadata = EXCLUDED.metadata,
                    update_user = EXCLUDED.update_user,
                    update_ts = EXCLUDED.update_ts,
                    aggregate_version = EXCLUDED.aggregate_version,
                    active = TRUE
                WHERE org_memory_t.aggregate_version < EXCLUDED.aggregate_version
                AND org_memory_t.active = FALSE
                """;
        Map<String, Object> map = SqlUtil.extractEventData(event);
        String hostId = (String)map.get("hostId");
        String memId = (String)map.get("memId");
        long newAggregateVersion = SqlUtil.getNewAggregateVersion(event);

        try (PreparedStatement statement = conn.prepareStatement(sql)) {
            int i = 1;
            statement.setObject(i++, UUID.fromString(hostId));
            statement.setObject(i++, UUID.fromString(memId));
            statement.setString(i++, (String)map.get("domain"));
            statement.setString(i++, (String)map.get("source"));
            statement.setString(i++, (String)map.get("content"));

            if(map.get("embedding") != null) {
                statement.setObject(i++, map.get("embedding"), Types.OTHER);
            } else {
                statement.setNull(i++, Types.OTHER);
            }

            setIntegerOrNull(statement, i++, map.get("chunkIndex"));
            setUuidOrNull(statement, i++, map.get("documentId"));

            if(map.get("metadata") != null) {
                statement.setObject(i++, JsonMapper.toJson(map.get("metadata")), Types.OTHER);
            } else {
                statement.setNull(i++, Types.OTHER);
            }

            statement.setString(i++, (String)event.get(Constants.USER));
            statement.setObject(i++, OffsetDateTime.parse((String)event.get(CloudEventV1.TIME)));
            statement.setLong(i++, newAggregateVersion);

            int count = statement.executeUpdate();
            if (count == 0) {
                logger.warn("Creation skipped for hostId {} memId {} aggregateVersion {}. A newer or same version already exists.", hostId, memId, newAggregateVersion);
            }
        } catch (SQLException e) {
            logger.error("SQLException during createOrgMemory for hostId {} memId {} aggregateVersion {}: {}", hostId, memId, newAggregateVersion, e.getMessage(), e);
            throw new PortalPersistenceException("Persistence Error", e);
        } catch (Exception e) {
            logger.error("Exception during createOrgMemory for hostId {} memId {} aggregateVersion {}: {}", hostId, memId, newAggregateVersion, e.getMessage(), e);
            throw new PortalPersistenceException("Persistence Error", e);
        }
    }

    @Override
    public void updateOrgMemory(Connection conn, Map<String, Object> event) throws PortalPersistenceException {
        final String sql =
                """
                UPDATE org_memory_t
                SET domain=?, source=?, content=?, embedding=?, chunk_index=?, document_id=?, metadata=?, update_user=?, update_ts=?, aggregate_version=?, active=TRUE
                WHERE host_id=? AND mem_id=? AND aggregate_version < ?
                """;
        Map<String, Object> map = SqlUtil.extractEventData(event);
        String hostId = (String)map.get("hostId");
        String memId = (String)map.get("memId");
        long newAggregateVersion = SqlUtil.getNewAggregateVersion(event);

        try (PreparedStatement statement = conn.prepareStatement(sql)) {
            int i = 1;
            statement.setString(i++, (String)map.get("domain"));
            statement.setString(i++, (String)map.get("source"));
            statement.setString(i++, (String)map.get("content"));

            if(map.get("embedding") != null) {
                statement.setObject(i++, map.get("embedding"), Types.OTHER);
            } else {
                statement.setNull(i++, Types.OTHER);
            }

            setIntegerOrNull(statement, i++, map.get("chunkIndex"));
            setUuidOrNull(statement, i++, map.get("documentId"));

            if(map.get("metadata") != null) {
                statement.setObject(i++, JsonMapper.toJson(map.get("metadata")), Types.OTHER);
            } else {
                statement.setNull(i++, Types.OTHER);
            }

            statement.setString(i++, (String)event.get(Constants.USER));
            statement.setObject(i++, OffsetDateTime.parse((String)event.get(CloudEventV1.TIME)));
            statement.setLong(i++, newAggregateVersion);
            statement.setObject(i++, UUID.fromString(hostId));
            statement.setObject(i++, UUID.fromString(memId));
            statement.setLong(i++, newAggregateVersion);

            int count = statement.executeUpdate();
            if (count == 0) {
                logger.warn("Update skipped for hostId {} memId {} aggregateVersion {}. Record not found or a newer/same version already exists.", hostId, memId, newAggregateVersion);
            }
        } catch (SQLException e) {
            logger.error("SQLException during updateOrgMemory for hostId {} memId {} aggregateVersion {}: {}", hostId, memId, newAggregateVersion, e.getMessage(), e);
            throw new PortalPersistenceException("Persistence Error", e);
        } catch (Exception e) {
            logger.error("Exception during updateOrgMemory for hostId {} memId {} aggregateVersion {}: {}", hostId, memId, newAggregateVersion, e.getMessage(), e);
            throw new PortalPersistenceException("Persistence Error", e);
        }
    }

    @Override
    public void deleteOrgMemory(Connection conn, Map<String, Object> event) throws PortalPersistenceException {
        final String sql =
                """
                UPDATE org_memory_t
                SET active = FALSE,
                    update_user = ?,
                    update_ts = ?,
                    aggregate_version = ?
                WHERE host_id=? AND mem_id=? AND aggregate_version < ?
                """;
        Map<String, Object> map = SqlUtil.extractEventData(event);
        String hostId = (String)map.get("hostId");
        String memId = (String)map.get("memId");
        long newAggregateVersion = SqlUtil.getNewAggregateVersion(event);

        try (PreparedStatement statement = conn.prepareStatement(sql)) {
            statement.setString(1, (String)event.get(Constants.USER));
            statement.setObject(2, OffsetDateTime.parse((String)event.get(CloudEventV1.TIME)));
            statement.setLong(3, newAggregateVersion);
            statement.setObject(4, UUID.fromString(hostId));
            statement.setObject(5, UUID.fromString(memId));
            statement.setLong(6, newAggregateVersion);

            int count = statement.executeUpdate();
            if (count == 0) {
                logger.warn("Delete skipped for hostId {} memId {} aggregateVersion {}. Record not found or a newer/same version already exists.", hostId, memId, newAggregateVersion);
            }
        } catch (SQLException e) {
            logger.error("SQLException during deleteOrgMemory for hostId {} memId {} aggregateVersion {}: {}", hostId, memId, newAggregateVersion, e.getMessage(), e);
            throw new PortalPersistenceException("Persistence Error", e);
        } catch (Exception e) {
            logger.error("Exception during deleteOrgMemory for hostId {} memId {} aggregateVersion {}: {}", hostId, memId, newAggregateVersion, e.getMessage(), e);
            throw new PortalPersistenceException("Persistence Error", e);
        }
    }

    @Override
    public Result<String> queryOrgMemory(int offset, int limit, String filtersJson, String globalFilter, String sortingJson, boolean active, String hostId) {
        Result<String> result;
        List<Map<String, Object>> filters = parseJsonList(filtersJson);
        List<Map<String, Object>> sorting = parseJsonList(sortingJson);
        String s =
                """
                SELECT COUNT(*) OVER () AS total, host_id, mem_id, domain, source, content, chunk_index, document_id, metadata, update_user, update_ts, aggregate_version, active
                FROM org_memory_t WHERE host_id = ?
                """;
        List<Object> parameters = new ArrayList<>();
        parameters.add(UUID.fromString(hostId));
        String activeClause = SqlUtil.buildMultiTableActiveClause(active);
        String[] searchColumns = {"domain", "source", "content", "metadata::text"};
        String sqlBuilder = s + activeClause +
                dynamicFilter(Arrays.asList("host_id", "mem_id", "document_id"), Arrays.asList("domain", "source"), filters, null, parameters) +
                globalFilter(globalFilter, searchColumns, parameters) +
                dynamicSorting("update_ts DESC", sorting, null) +
                "\nLIMIT ? OFFSET ?";
        parameters.add(limit);
        parameters.add(offset);
        if(logger.isTraceEnabled()) logger.trace("queryOrgMemory sql: {}", sqlBuilder);
        int total = 0;
        List<Map<String, Object>> memories = new ArrayList<>();
        try (final Connection conn = ds.getConnection(); PreparedStatement preparedStatement = conn.prepareStatement(sqlBuilder)) {
            populateParameters(preparedStatement, parameters);
            boolean isFirstRow = true;
            try (ResultSet resultSet = preparedStatement.executeQuery()) {
                while (resultSet.next()) {
                    Map<String, Object> map = new HashMap<>();
                    if (isFirstRow) {
                        total = resultSet.getInt("total");
                        isFirstRow = false;
                    }
                    map.put("hostId", resultSet.getObject("host_id", UUID.class));
                    map.put("memId", resultSet.getObject("mem_id", UUID.class));
                    map.put("domain", resultSet.getString("domain"));
                    map.put("source", resultSet.getString("source"));
                    map.put("content", resultSet.getString("content"));
                    map.put("chunkIndex", resultSet.getObject("chunk_index") != null ? resultSet.getInt("chunk_index") : null);
                    map.put("documentId", resultSet.getObject("document_id", UUID.class));
                    map.put("metadata", resultSet.getString("metadata"));
                    map.put("updateUser", resultSet.getString("update_user"));
                    map.put("updateTs", resultSet.getObject("update_ts", OffsetDateTime.class));
                    map.put("aggregateVersion", resultSet.getLong("aggregate_version"));
                    map.put("active", resultSet.getBoolean("active"));
                    memories.add(map);
                }
            }
            Map<String, Object> resultMap = new HashMap<>();
            resultMap.put("total", total);
            resultMap.put("memories", memories);
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
    public Result<String> getOrgMemoryById(String hostId, String memId) {
        final String sql = "SELECT host_id, mem_id, domain, source, content, chunk_index, document_id, metadata, update_user, update_ts, aggregate_version, active FROM org_memory_t WHERE host_id = ? AND mem_id = ?";
        Result<String> result;
        Map<String, Object> map = new HashMap<>();
        try (Connection conn = ds.getConnection(); PreparedStatement statement = conn.prepareStatement(sql)) {
            statement.setObject(1, UUID.fromString(hostId));
            statement.setObject(2, UUID.fromString(memId));
            try (ResultSet resultSet = statement.executeQuery()) {
                if (resultSet.next()) {
                    map.put("hostId", resultSet.getObject("host_id", UUID.class));
                    map.put("memId", resultSet.getObject("mem_id", UUID.class));
                    map.put("domain", resultSet.getString("domain"));
                    map.put("source", resultSet.getString("source"));
                    map.put("content", resultSet.getString("content"));
                    map.put("chunkIndex", resultSet.getObject("chunk_index") != null ? resultSet.getInt("chunk_index") : null);
                    map.put("documentId", resultSet.getObject("document_id", UUID.class));
                    map.put("metadata", JsonMapper.string2Map(resultSet.getString("metadata")));
                    map.put("updateUser", resultSet.getString("update_user"));
                    map.put("updateTs", resultSet.getObject("update_ts", OffsetDateTime.class));
                    map.put("aggregateVersion", resultSet.getLong("aggregate_version"));
                    map.put("active", resultSet.getBoolean("active"));
                    result = Success.of(JsonMapper.toJson(map));
                } else {
                    result = Failure.of(new Status(OBJECT_NOT_FOUND, "org_memory", memId));
                }
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
    public Result<String> getAgentDefinitionLabel(String hostId) {
        String sql = "SELECT agent_def_id, agent_name FROM agent_definition_t WHERE host_id = ? AND active = TRUE";
        return getLabels(sql, hostId, "agent_def_id", "agent_name");
    }

    @Override
    public Result<String> getWfDefinitionLabel(String hostId) {
        String sql = "SELECT wf_def_id, name FROM wf_definition_t WHERE host_id = ? AND active = TRUE";
        return getLabels(sql, hostId, "wf_def_id", "name");
    }

    @Override
    public Result<String> getWorklistLabel(String hostId) {
        String sql = "SELECT assignee_id || ':' || category_id as id, assignee_id || ' (' || category_id || ')' as label FROM worklist_t WHERE host_id = ? AND active = TRUE";
        return getLabels(sql, hostId, "id", "label");
    }

    @Override
    public Result<String> getProcessInfoLabel(String hostId) {
        String sql = "SELECT process_id, process_type FROM process_info_t WHERE host_id = ? AND active = TRUE";
        return getLabels(sql, hostId, "process_id", "process_type");
    }

    @Override
    public Result<String> getTaskInfoLabel(String hostId) {
        String sql = "SELECT task_id, task_type FROM task_info_t WHERE host_id = ? AND active = TRUE";
        return getLabels(sql, hostId, "task_id", "task_type");
    }

    @Override
    public Result<String> getSkillLabel(String hostId) {
        String sql = "SELECT skill_id, name FROM skill_t WHERE host_id = ? AND active = TRUE";
        return getLabels(sql, hostId, "skill_id", "name");
    }

    @Override
    public void createAgentMemory(Connection conn, Map<String, Object> event) throws PortalPersistenceException {
        final String sql =
                """
                INSERT INTO agent_memory_t (host_id, mem_id, agent_def_id, content, embedding, memory_type, metadata, update_user, update_ts, aggregate_version, active)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, TRUE)
                ON CONFLICT (host_id, mem_id) DO UPDATE
                SET agent_def_id = EXCLUDED.agent_def_id,
                    content = EXCLUDED.content,
                    embedding = EXCLUDED.embedding,
                    memory_type = EXCLUDED.memory_type,
                    metadata = EXCLUDED.metadata,
                    update_user = EXCLUDED.update_user,
                    update_ts = EXCLUDED.update_ts,
                    aggregate_version = EXCLUDED.aggregate_version,
                    active = TRUE
                WHERE agent_memory_t.aggregate_version < EXCLUDED.aggregate_version
                AND agent_memory_t.active = FALSE
                """;
        Map<String, Object> map = SqlUtil.extractEventData(event);
        String hostId = (String)map.get("hostId");
        String memId = (String)map.get("memId");
        long newAggregateVersion = SqlUtil.getNewAggregateVersion(event);

        try (PreparedStatement statement = conn.prepareStatement(sql)) {
            int i = 1;
            statement.setObject(i++, UUID.fromString(hostId));
            statement.setObject(i++, UUID.fromString(memId));
            statement.setObject(i++, UUID.fromString((String)map.get("agentDefId")));
            statement.setString(i++, (String)map.get("content"));

            if(map.get("embedding") != null) {
                statement.setObject(i++, map.get("embedding"), Types.OTHER);
            } else {
                statement.setNull(i++, Types.OTHER);
            }

            statement.setString(i++, (String)map.get("memoryType"));

            if(map.get("metadata") != null) {
                statement.setObject(i++, JsonMapper.toJson(map.get("metadata")), Types.OTHER);
            } else {
                statement.setNull(i++, Types.OTHER);
            }

            statement.setString(i++, (String)event.get(Constants.USER));
            statement.setObject(i++, OffsetDateTime.parse((String)event.get(CloudEventV1.TIME)));
            statement.setLong(i++, newAggregateVersion);

            int count = statement.executeUpdate();
            if (count == 0) {
                logger.warn("Creation skipped for hostId {} memId {} aggregateVersion {}. A newer or same version already exists.", hostId, memId, newAggregateVersion);
            }
        } catch (SQLException e) {
            logger.error("SQLException during createAgentMemory for hostId {} memId {} aggregateVersion {}: {}", hostId, memId, newAggregateVersion, e.getMessage(), e);
            throw new PortalPersistenceException("Persistence Error", e);
        } catch (Exception e) {
            logger.error("Exception during createAgentMemory for hostId {} memId {} aggregateVersion {}: {}", hostId, memId, newAggregateVersion, e.getMessage(), e);
            throw new PortalPersistenceException("Persistence Error", e);
        }
    }

    @Override
    public void updateAgentMemory(Connection conn, Map<String, Object> event) throws PortalPersistenceException {
        final String sql =
                """
                UPDATE agent_memory_t
                SET agent_def_id=?, content=?, embedding=?, memory_type=?, metadata=?, update_user=?, update_ts=?, aggregate_version=?, active=TRUE
                WHERE host_id=? AND mem_id=? AND aggregate_version < ?
                """;
        Map<String, Object> map = SqlUtil.extractEventData(event);
        String hostId = (String)map.get("hostId");
        String memId = (String)map.get("memId");
        long newAggregateVersion = SqlUtil.getNewAggregateVersion(event);

        try (PreparedStatement statement = conn.prepareStatement(sql)) {
            int i = 1;
            statement.setObject(i++, UUID.fromString((String)map.get("agentDefId")));
            statement.setString(i++, (String)map.get("content"));

            if(map.get("embedding") != null) {
                statement.setObject(i++, map.get("embedding"), Types.OTHER);
            } else {
                statement.setNull(i++, Types.OTHER);
            }

            statement.setString(i++, (String)map.get("memoryType"));

            if(map.get("metadata") != null) {
                statement.setObject(i++, JsonMapper.toJson(map.get("metadata")), Types.OTHER);
            } else {
                statement.setNull(i++, Types.OTHER);
            }

            statement.setString(i++, (String)event.get(Constants.USER));
            statement.setObject(i++, OffsetDateTime.parse((String)event.get(CloudEventV1.TIME)));
            statement.setLong(i++, newAggregateVersion);
            statement.setObject(i++, UUID.fromString(hostId));
            statement.setObject(i++, UUID.fromString(memId));
            statement.setLong(i++, newAggregateVersion);

            int count = statement.executeUpdate();
            if (count == 0) {
                logger.warn("Update skipped for hostId {} memId {} aggregateVersion {}. Record not found or a newer/same version already exists.", hostId, memId, newAggregateVersion);
            }
        } catch (SQLException e) {
            logger.error("SQLException during updateAgentMemory for hostId {} memId {} aggregateVersion {}: {}", hostId, memId, newAggregateVersion, e.getMessage(), e);
            throw new PortalPersistenceException("Persistence Error", e);
        } catch (Exception e) {
            logger.error("Exception during updateAgentMemory for hostId {} memId {} aggregateVersion {}: {}", hostId, memId, newAggregateVersion, e.getMessage(), e);
            throw new PortalPersistenceException("Persistence Error", e);
        }
    }

    @Override
    public void deleteAgentMemory(Connection conn, Map<String, Object> event) throws PortalPersistenceException {
        final String sql =
                """
                UPDATE agent_memory_t
                SET active = FALSE,
                    update_user = ?,
                    update_ts = ?,
                    aggregate_version = ?
                WHERE host_id=? AND mem_id=? AND aggregate_version < ?
                """;
        Map<String, Object> map = SqlUtil.extractEventData(event);
        String hostId = (String)map.get("hostId");
        String memId = (String)map.get("memId");
        long newAggregateVersion = SqlUtil.getNewAggregateVersion(event);

        try (PreparedStatement statement = conn.prepareStatement(sql)) {
            statement.setString(1, (String)event.get(Constants.USER));
            statement.setObject(2, OffsetDateTime.parse((String)event.get(CloudEventV1.TIME)));
            statement.setLong(3, newAggregateVersion);
            statement.setObject(4, UUID.fromString(hostId));
            statement.setObject(5, UUID.fromString(memId));
            statement.setLong(6, newAggregateVersion);

            int count = statement.executeUpdate();
            if (count == 0) {
                logger.warn("Delete skipped for hostId {} memId {} aggregateVersion {}. Record not found or a newer/same version already exists.", hostId, memId, newAggregateVersion);
            }
        } catch (SQLException e) {
            logger.error("SQLException during deleteAgentMemory for hostId {} memId {} aggregateVersion {}: {}", hostId, memId, newAggregateVersion, e.getMessage(), e);
            throw new PortalPersistenceException("Persistence Error", e);
        } catch (Exception e) {
            logger.error("Exception during deleteAgentMemory for hostId {} memId {} aggregateVersion {}: {}", hostId, memId, newAggregateVersion, e.getMessage(), e);
            throw new PortalPersistenceException("Persistence Error", e);
        }
    }

    @Override
    public Result<String> queryAgentMemory(int offset, int limit, String filtersJson, String globalFilter, String sortingJson, boolean active, String hostId) {
        Result<String> result;
        List<Map<String, Object>> filters = parseJsonList(filtersJson);
        List<Map<String, Object>> sorting = parseJsonList(sortingJson);
        String s =
                """
                SELECT COUNT(*) OVER () AS total, host_id, mem_id, agent_def_id, content, memory_type, metadata, update_user, update_ts, aggregate_version, active
                FROM agent_memory_t WHERE host_id = ?
                """;
        List<Object> parameters = new ArrayList<>();
        parameters.add(UUID.fromString(hostId));
        String activeClause = SqlUtil.buildMultiTableActiveClause(active);
        String[] searchColumns = {"content", "memory_type", "metadata::text"};
        String sqlBuilder = s + activeClause +
                dynamicFilter(Arrays.asList("host_id", "mem_id", "agent_def_id"), Collections.emptyList(), filters, null, parameters) +
                globalFilter(globalFilter, searchColumns, parameters) +
                dynamicSorting("update_ts DESC", sorting, null) +
                "\nLIMIT ? OFFSET ?";
        parameters.add(limit);
        parameters.add(offset);
        if(logger.isTraceEnabled()) logger.trace("queryAgentMemory sql: {}", sqlBuilder);
        int total = 0;
        List<Map<String, Object>> memories = new ArrayList<>();
        try (final Connection conn = ds.getConnection(); PreparedStatement preparedStatement = conn.prepareStatement(sqlBuilder)) {
            populateParameters(preparedStatement, parameters);
            boolean isFirstRow = true;
            try (ResultSet resultSet = preparedStatement.executeQuery()) {
                while (resultSet.next()) {
                    Map<String, Object> map = new HashMap<>();
                    if (isFirstRow) {
                        total = resultSet.getInt("total");
                        isFirstRow = false;
                    }
                    map.put("hostId", resultSet.getObject("host_id", UUID.class));
                    map.put("memId", resultSet.getObject("mem_id", UUID.class));
                    map.put("agentDefId", resultSet.getObject("agent_def_id", UUID.class));
                    map.put("content", resultSet.getString("content"));
                    map.put("memoryType", resultSet.getString("memory_type"));
                    map.put("metadata", resultSet.getString("metadata"));
                    map.put("updateUser", resultSet.getString("update_user"));
                    map.put("updateTs", resultSet.getObject("update_ts", OffsetDateTime.class));
                    map.put("aggregateVersion", resultSet.getLong("aggregate_version"));
                    map.put("active", resultSet.getBoolean("active"));
                    memories.add(map);
                }
            }
            Map<String, Object> resultMap = new HashMap<>();
            resultMap.put("total", total);
            resultMap.put("memories", memories);
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
    public Result<String> getAgentMemoryById(String hostId, String memId) {
        final String sql = "SELECT host_id, mem_id, agent_def_id, content, memory_type, metadata, update_user, update_ts, aggregate_version, active FROM agent_memory_t WHERE host_id = ? AND mem_id = ?";
        Result<String> result;
        Map<String, Object> map = new HashMap<>();
        try (Connection conn = ds.getConnection(); PreparedStatement statement = conn.prepareStatement(sql)) {
            statement.setObject(1, UUID.fromString(hostId));
            statement.setObject(2, UUID.fromString(memId));
            try (ResultSet resultSet = statement.executeQuery()) {
                if (resultSet.next()) {
                    map.put("hostId", resultSet.getObject("host_id", UUID.class));
                    map.put("memId", resultSet.getObject("mem_id", UUID.class));
                    map.put("agentDefId", resultSet.getObject("agent_def_id", UUID.class));
                    map.put("content", resultSet.getString("content"));
                    map.put("memoryType", resultSet.getString("memory_type"));
                    map.put("metadata", resultSet.getString("metadata"));
                    map.put("updateUser", resultSet.getString("update_user"));
                    map.put("updateTs", resultSet.getObject("update_ts", OffsetDateTime.class));
                    map.put("aggregateVersion", resultSet.getLong("aggregate_version"));
                    map.put("active", resultSet.getBoolean("active"));
                    result = Success.of(JsonMapper.toJson(map));
                } else {
                    result = Failure.of(new Status(OBJECT_NOT_FOUND, "agent_memory", memId));
                }
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
    public Result<String> getUserMemoryById(String hostId, String memId) {
        final String sql = "SELECT host_id, mem_id, user_id, agent_def_id, content, memory_type, importance_score, access_count, last_accessed, metadata, update_user, update_ts, aggregate_version, active FROM user_memory_t WHERE host_id = ? AND mem_id = ?";
        Result<String> result;
        Map<String, Object> map = new HashMap<>();
        try (Connection conn = ds.getConnection(); PreparedStatement statement = conn.prepareStatement(sql)) {
            statement.setObject(1, UUID.fromString(hostId));
            statement.setObject(2, UUID.fromString(memId));
            try (ResultSet resultSet = statement.executeQuery()) {
                if (resultSet.next()) {
                    map.put("hostId", resultSet.getObject("host_id", UUID.class));
                    map.put("memId", resultSet.getObject("mem_id", UUID.class));
                    map.put("userId", resultSet.getObject("user_id", UUID.class));
                    map.put("agentDefId", resultSet.getObject("agent_def_id", UUID.class));
                    map.put("content", resultSet.getString("content"));
                    map.put("memoryType", resultSet.getString("memory_type"));
                    map.put("importanceScore", resultSet.getFloat("importance_score"));
                    map.put("accessCount", resultSet.getObject("access_count") != null ? resultSet.getInt("access_count") : null);
                    map.put("lastAccessed", resultSet.getObject("last_accessed", OffsetDateTime.class));
                    map.put("metadata", resultSet.getString("metadata"));
                    map.put("updateUser", resultSet.getString("update_user"));
                    map.put("updateTs", resultSet.getObject("update_ts", OffsetDateTime.class));
                    map.put("aggregateVersion", resultSet.getLong("aggregate_version"));
                    map.put("active", resultSet.getBoolean("active"));
                    result = Success.of(JsonMapper.toJson(map));
                } else {
                    result = Failure.of(new Status(OBJECT_NOT_FOUND, "user_memory", memId));
                }
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

    private Result<String> getLabels(String sql, String hostId, String idCol, String labelCol) {
        List<Map<String, Object>> labels = new ArrayList<>();
        try (Connection connection = ds.getConnection();
             PreparedStatement preparedStatement = connection.prepareStatement(sql)) {
            preparedStatement.setObject(1, UUID.fromString(hostId));
            try (ResultSet resultSet = preparedStatement.executeQuery()) {
                while (resultSet.next()) {
                    Map<String, Object> map = new HashMap<>();
                    map.put("id", resultSet.getString(idCol));
                    map.put("label", resultSet.getString(labelCol));
                    labels.add(map);
                }
            }
            return Success.of(JsonMapper.toJson(labels));
        } catch (SQLException e) {
            logger.error("SQLException:", e);
            return Failure.of(new Status(SQL_EXCEPTION, e.getMessage()));
        } catch (Exception e) {
            logger.error("Exception:", e);
            return Failure.of(new Status(GENERIC_EXCEPTION, e.getMessage()));
        }
    }
}
