package net.lightapi.portal.db.persistence;

import com.networknt.config.JsonMapper;
import com.networknt.monad.Failure;
import com.networknt.monad.Result;
import com.networknt.monad.Success;
import com.networknt.status.Status;
import com.networknt.utility.Constants;
import io.cloudevents.core.v1.CloudEventV1;
import net.lightapi.portal.db.PortalDbProvider;
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
    public void createAgentDefinition(Connection conn, Map<String, Object> event) throws SQLException {
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
            throw e;
        } catch (Exception e) {
            logger.error("Exception during createAgentDefinition for hostId {} agentDefId {} aggregateVersion {}: {}", hostId, agentDefId, newAggregateVersion, e.getMessage(), e);
            throw e;
        }
    }

    @Override
    public void updateAgentDefinition(Connection conn, Map<String, Object> event) throws SQLException {
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
            throw e;
        } catch (Exception e) {
            logger.error("Exception during updateAgentDefinition for hostId {} agentDefId {} aggregateVersion {}: {}", hostId, agentDefId, newAggregateVersion, e.getMessage(), e);
            throw e;
        }
    }

    @Override
    public void deleteAgentDefinition(Connection conn, Map<String, Object> event) throws SQLException {
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
            throw e;
        } catch (Exception e) {
            logger.error("Exception during deleteAgentDefinition for hostId {} agentDefId {} aggregateVersion {}: {}", hostId, agentDefId, newAggregateVersion, e.getMessage(), e);
            throw e;
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
}
