package net.lightapi.portal.db.persistence;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.networknt.config.JsonMapper;
import com.networknt.monad.Failure;
import com.networknt.monad.Result;
import com.networknt.monad.Success;
import com.networknt.status.Status;
import com.networknt.utility.CollectionUtils;
import com.networknt.utility.Constants;
import com.networknt.utility.UuidUtil;
import io.cloudevents.core.v1.CloudEventV1;
import net.lightapi.portal.PortalConstants;
import net.lightapi.portal.db.ConcurrencyException;
import net.lightapi.portal.db.PortalDbProvider;
import net.lightapi.portal.db.model.DbConsumablePromotableInstance;
import net.lightapi.portal.db.util.NotificationService;
import net.lightapi.portal.db.util.SqlUtil;
import net.lightapi.portal.validation.FilterCriterion;
import net.lightapi.portal.validation.SortCriterion;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;
import java.time.OffsetDateTime;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.networknt.db.provider.SqlDbStartupHook.ds;
import static net.lightapi.portal.db.util.SqlUtil.*;

public class InstanceDeploymentPersistenceImpl implements InstanceDeploymentPersistence {
    private static final Logger logger = LoggerFactory.getLogger(InstanceDeploymentPersistenceImpl.class);
    private static final String SQL_EXCEPTION = PortalDbProvider.SQL_EXCEPTION;
    private static final String GENERIC_EXCEPTION = PortalDbProvider.GENERIC_EXCEPTION;
    private static final String OBJECT_NOT_FOUND = PortalDbProvider.OBJECT_NOT_FOUND;

    private final NotificationService notificationService;

    public InstanceDeploymentPersistenceImpl(NotificationService notificationService) {
        this.notificationService = notificationService;
    }

    @Override
    public void createInstance(Connection conn, Map<String, Object> event) throws SQLException, Exception {
        final String sql =
                """
                INSERT INTO instance_t(host_id, instance_id, instance_name, product_version_id,
                service_id, current, readonly, environment, service_desc, instance_desc, zone, region, lob,
                resource_name, business_name, env_tag, topic_classification, update_user, update_ts, aggregate_version)
                VALUES (?, ?, ?, ?, ?,  ?, ?, ?, ?, ?,  ?, ?, ?, ?, ?,  ?, ?, ?, ?, ?)
                """;
        final String sqlUpdateCurrent =
                """
                UPDATE instance_t SET current = false
                WHERE host_id = ?
                AND service_id = ?
                AND instance_id != ?
                """;
        // SQL query for fetching property IDs
        final String sqlFetchPropertyIds =
                """
                SELECT DISTINCT cp.property_id, cp.property_name,c.config_name
                FROM config_property_t cp
                JOIN config_t c ON cp.config_id = c.config_id
                WHERE CONCAT(c.config_name, '.', cp.property_name) IN ('serviceId', 'environment')
                """;

        String[] tableNames = {"environment", "zone", "region", "lob"};
        Map<String, Object> map = (Map<String, Object>)event.get(PortalConstants.DATA);
        String hostId = (String)event.get(Constants.HOST);
        String instanceId = (String)map.get("instanceId");
        String serviceId = (String)map.get("serviceId");
        final String user = (String) event.get(Constants.USER);
        final String updateTs = (String) event.get(CloudEventV1.TIME);
        long newAggregateVersion = SqlUtil.getNewAggregateVersion(event);

        Result<String> refDataResult = fetchReferenceData(hostId, tableNames);
        Map<String, Set<String>> refData = parseReferenceData(refDataResult.getResult());
        // Validate input parameters
        validateParameter("environment", (String)map.get("environment"), refData.get("environment"));
        validateParameter("zone", (String)map.get("zone"), refData.get("zone"));
        validateParameter("region", (String)map.get("region"), refData.get("region"));
        validateParameter("lob", (String)map.get("lob"), refData.get("lob"));

        // Fetch property IDs
        Map<String, List<Map<String, Object>>> properties = new HashMap<>();
        try (PreparedStatement stmtFetchPropertyIds = conn.prepareStatement(sqlFetchPropertyIds);
             ResultSet rs = stmtFetchPropertyIds.executeQuery()) {
            while (rs.next()) {
                UUID propertyId = rs.getObject("property_id", UUID.class);
                String propertyName = rs.getString("property_name");
                String configName = rs.getString("config_name");

                Map<String, Object> propertyMap = new HashMap<>();
                propertyMap.put("propertyId", propertyId);
                propertyMap.put("propertyName", propertyName);
                properties.computeIfAbsent(configName, k -> new ArrayList<>()).add(propertyMap);
            }
        }

        try (PreparedStatement statement = conn.prepareStatement(sql)) {
            statement.setObject(1, UUID.fromString(hostId));
            statement.setObject(2, UUID.fromString(instanceId));
            statement.setString(3, (String)map.get("instanceName"));
            statement.setObject(4, UUID.fromString((String)map.get("productVersionId")));
            statement.setString(5, serviceId);
            Boolean current = (Boolean)map.get("current");
            if (current != null) {
                statement.setBoolean(6, current);
            } else {
                statement.setNull(6, Types.BOOLEAN);
            }
            Boolean readonly = (Boolean)map.get("readonly");
            if (readonly != null) {
                statement.setBoolean(7, readonly);
            } else {
                statement.setNull(7, Types.BOOLEAN);
            }
            String environment = (String)map.get("environment");
            if (environment != null && !environment.isEmpty()) {
                statement.setString(8, environment);
            } else {
                statement.setNull(8, Types.VARCHAR);
            }
            String serviceDesc = (String)map.get("serviceDesc");
            if (serviceDesc != null && !serviceDesc.isEmpty()) {
                statement.setString(9, serviceDesc);
            } else {
                statement.setNull(9, Types.VARCHAR);
            }
            String instanceDesc = (String)map.get("instanceDesc");
            if(instanceDesc != null && !instanceDesc.isEmpty()) {
                statement.setString(10, instanceDesc);
            } else {
                statement.setNull(10, Types.VARCHAR);
            }
            String zone = (String)map.get("zone");
            if(zone != null && !zone.isEmpty()) {
                statement.setString(11, zone);
            } else {
                statement.setNull(11, Types.VARCHAR);
            }
            String region = (String)map.get("region");
            if(region != null && !region.isEmpty()) {
                statement.setString(12, region);
            } else {
                statement.setNull(12, Types.VARCHAR);
            }
            String lob = (String)map.get("lob");
            if(lob != null && !lob.isEmpty()) {
                statement.setString(13, lob);
            } else {
                statement.setNull(13, Types.VARCHAR);
            }
            String resourceName = (String)map.get("resourceName");
            if(resourceName != null && !resourceName.isEmpty()) {
                statement.setString(14, resourceName);
            } else {
                statement.setNull(14, Types.VARCHAR);
            }
            String businessName = (String)map.get("businessName");
            if(businessName != null && !businessName.isEmpty()) {
                statement.setString(15, businessName);
            } else {
                statement.setNull(15, Types.VARCHAR);
            }
            String envTag = (String)map.get("envTag");
            if(envTag != null && !envTag.isEmpty()) {
                statement.setString(16, envTag);
            } else {
                statement.setNull(16, Types.VARCHAR);
            }
            String topicClassification = (String)map.get("topicClassification");
            if(topicClassification != null && !topicClassification.isEmpty()) {
                statement.setString(17, topicClassification);
            } else {
                statement.setNull(17, Types.VARCHAR);
            }

            statement.setString(18, (String)event.get(Constants.USER));
            statement.setObject(19, OffsetDateTime.parse((String)event.get(CloudEventV1.TIME)));
            statement.setLong(20, newAggregateVersion);

            int count = statement.executeUpdate();
            if (count == 0) {
                throw new SQLException(String.format("Failed during createInstance for hostId %s instanceId %s with aggregateVersion %d", hostId, instanceId, newAggregateVersion));
            }
            // try to update current to false for others if current is true.
            if(current != null && current) {
                try (PreparedStatement statementUpdate = conn.prepareStatement(sqlUpdateCurrent)) {
                    statementUpdate.setObject(1, UUID.fromString(hostId));
                    statementUpdate.setString(2, serviceId);
                    statementUpdate.setObject(3, UUID.fromString(instanceId));
                    statementUpdate.executeUpdate();
                }
            }
        } catch (SQLException e) {
            logger.error("SQLException during createInstance for hostId {} instanceId {} aggregateVersion {}: {}", hostId, instanceId, newAggregateVersion, e.getMessage(), e);
            throw e;
        } catch (Exception e) {
            logger.error("Exception during createInstance for hostId {} instanceId {} aggregateVersion {}: {}", hostId, instanceId, newAggregateVersion, e.getMessage(), e);
            throw e;
        }
        upsertPlatformProductInstanceProperties(hostId,instanceId,properties,user);
    }

    private Result<String> fetchReferenceData(String hostId, String[] tableNames) {
        Result<String> result = null;
        String sql = """
                    WITH params AS MATERIALIZED (
                        SELECT
                            ?::UUID as host_id,
                            ?::VARCHAR[] as table_names
                    )
                    SELECT
                        rt.host_id,
                        rt.table_id,
                        rt.table_name,
                        rv.value_code
                    FROM
                        ref_table_t rt
                    JOIN
                        ref_value_t rv ON rt.table_id = rv.table_id
                    JOIN
                        params p ON rt.host_id = p.host_id
                    WHERE
                        (array_length(p.table_names, 1) IS NULL OR rt.table_name = ANY(p.table_names))
                        AND rt.active = TRUE
                """;
        UUID hostIdUUID = hostId != null ? UUID.fromString(hostId) : null;
        try (Connection connection = ds.getConnection();
             PreparedStatement preparedStatement = connection.prepareStatement(sql)) {
            preparedStatement.setObject(1, hostIdUUID);
            preparedStatement.setObject(2, SqlUtil.createArrayLiteral(List.of(tableNames)));
            boolean isFirstRow = true;
            List<Map<String, Object>> refValues = new ArrayList<>();
            int total = 0;
            try (ResultSet resultSet = preparedStatement.executeQuery()) {
                while (resultSet.next()) {
                    Map<String, Object> map = new HashMap<>();
                    if (isFirstRow) {
                        total = resultSet.getInt("total");
                        isFirstRow = false;
                    }
                    map.put("tableId", resultSet.getObject("table_id", UUID.class));
                    map.put("hostId", resultSet.getObject("host_id", UUID.class));
                    map.put("tableName", resultSet.getString("table_name"));
                    map.put("value_code", resultSet.getString("value_code"));
                    refValues.add(map);
                }
                Map<String, Object> resultMap = new HashMap<>();
                resultMap.put("refValues", refValues); // Use a descriptive key for the list
                result = Success.of(JsonMapper.toJson(resultMap));
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

    private Map<String, Set<String>> parseReferenceData(String jsonResult) {
        Map<String, Set<String>> refData = new HashMap<>();
        try {
            ObjectMapper mapper = new ObjectMapper();
            JsonNode root = mapper.readTree(jsonResult);
            JsonNode refValues = root.get("refValues");
            if (refValues != null && refValues.isArray()) {
                for (JsonNode node : refValues) {
                    String tableName = node.get("tableName").asText();
                    String valueCode = node.get("value_code").asText();
                    refData.computeIfAbsent(tableName, k -> new HashSet<>()).add(valueCode);
                }
            }
        } catch (JsonProcessingException e) {
            logger.error("Error parsing reference data JSON: {}", e.getMessage());
        }
        return refData;
    }
    private void validateParameter(String paramName, String value, Set<String> validValues) throws IllegalArgumentException {
        if (value != null && !value.isEmpty() && validValues != null && !validValues.contains(value)) {
            throw new IllegalArgumentException("Invalid " + paramName + ": " + value);
        }
    }

    private void upsertPlatformProductInstanceProperties(
            String hostId,String instanceId,
            Map<String, List<Map<String, Object>>> properties,String user) throws SQLException {
        if (CollectionUtils.isEmpty(properties)) {
            return;
        }

        final String sqlUpsertInstanceProperty = """
        WITH params AS MATERIALIZED (
            SELECT
                CAST(v.host_id as UUID) as host_id,
                CAST(v.instance_id as UUID) as instance_id,
                CAST(v.config_name as VARCHAR) as config_name,
                CAST(v.property_name as VARCHAR) as property_name,
                CAST(v.property_value as TEXT) as property_value,
                CAST(v.update_user as VARCHAR) as update_user,
                CURRENT_TIMESTAMP as update_ts,
                CAST(1 as BIGINT) as aggregate_version
            FROM (
                values
                    (?, ?, ?, ?, ?, ?)
            ) as v(host_id, instance_id, config_name, property_name, property_value, update_user)
        )
        INSERT INTO instance_property_t
            (host_id, instance_id, property_id, property_value, aggregate_version, update_user, update_ts)
        SELECT
            params.host_id as host_id,
            params.instance_id as instance_id,
            config_property.property_id as property_id,
            params.property_value as property_value,
            params.aggregate_version as aggregate_version,
            params.update_user as update_user,
            params.update_ts as update_ts
        FROM
            params
            JOIN config_t as config
                ON config.config_name = params.config_name
            JOIN config_property_t as config_property
                ON config_property.property_name = params.property_name
                    AND config_property.config_id = config.config_id
        ON CONFLICT
        ON CONSTRAINT instance_property_pk
        DO UPDATE
            SET
                property_value = excluded.property_value,
                aggregate_version = instance_property_t.aggregate_version + 1,
                update_user = excluded.update_user,
                update_ts = excluded.update_ts
            WHERE
                instance_property_t.host_id = excluded.host_id
                AND instance_property_t.instance_id = excluded.instance_id
                AND instance_property_t.property_id = excluded.property_id;
        """;

        try (Connection connection = ds.getConnection();
             PreparedStatement preparedStatement = connection.prepareStatement(sqlUpsertInstanceProperty)) {
            for (Map.Entry<String, List<Map<String, Object>>> entry : properties.entrySet()) {
                String configName = entry.getKey();
                List<Map<String, Object>> propertiesMap = entry.getValue();
                for (Map<String, Object> propertyMap : propertiesMap) {
                    preparedStatement.setObject(1, hostId);
                    preparedStatement.setString(2, instanceId);
                    preparedStatement.setString(3, configName);
                    preparedStatement.setString(4, (String) propertyMap.get("propertyName"));
                    preparedStatement.setObject(5, propertyMap.get("propertyId")); // Assuming we want to store the UUID
                    preparedStatement.setString(6, user);
                    preparedStatement.addBatch();
                }
            }

            preparedStatement.executeBatch();
        } catch (SQLException e) {
            logger.error("SQLException during createInstance for hostId {} instanceId {} : {}", hostId, instanceId, e.getMessage(), e);
            throw e;
        } catch (Exception e) {
            logger.error("Exception during createInstance for hostId {} instanceId {} : {}", hostId, instanceId,  e.getMessage(), e);
            throw e;
        }
    }

    private boolean queryInstanceExists(Connection conn, String hostId, String instanceId) throws SQLException {
        final String sql =
                """
                SELECT COUNT(*) FROM instance_t WHERE host_id = ? AND instance_id = ?
                """;
        try (PreparedStatement pst = conn.prepareStatement(sql)) {
            pst.setObject(1, UUID.fromString(hostId));
            pst.setObject(2, UUID.fromString(instanceId));
            try (ResultSet rs = pst.executeQuery()) {
                return rs.next() && rs.getInt(1) > 0;
            }
        }
    }

    @Override
    public void updateInstance(Connection conn, Map<String, Object> event) throws SQLException, Exception {
        final String sql =
                """
                UPDATE instance_t SET instance_name = ?,
                current = ?, environment = ?, service_desc = ?, instance_desc = ?,
                zone = ?, region = ?, lob = ?, resource_name = ?, business_name = ?,
                topic_classification = ?, update_user = ?, update_ts = ?, aggregate_version = ?
                WHERE host_id = ? and instance_id = ? AND aggregate_version = ? and product_version_id = ? and service_id = ? and env_tag = ?
                """;
        final String sqlUpdateCurrent =
                """
                UPDATE instance_t SET current = false
                WHERE host_id = ?
                AND service_id = ?
                AND instance_id != ?
                """;

        Map<String, Object> map = (Map<String, Object>)event.get(PortalConstants.DATA);
        String hostId = (String)event.get(Constants.HOST);
        String instanceId = (String)map.get("instanceId");
        String serviceId = (String)map.get("serviceId");
        long oldAggregateVersion = SqlUtil.getOldAggregateVersion(event);
        long newAggregateVersion = SqlUtil.getNewAggregateVersion(event);

        try (PreparedStatement statement = conn.prepareStatement(sql)) {
            statement.setString(1, (String)map.get("instanceName"));
            Boolean current = (Boolean)map.get("current");
            if (current != null) {
                statement.setBoolean(2, current);
            } else {
                statement.setNull(2, Types.BOOLEAN);
            }
            String environment = (String)map.get("environment");
            if (environment != null && !environment.isEmpty()) {
                statement.setString(3, environment);
            } else {
                statement.setNull(3, Types.VARCHAR);
            }
            String serviceDesc = (String)map.get("serviceDesc");
            if (serviceDesc != null && !serviceDesc.isEmpty()) {
                statement.setString(4, serviceDesc);
            } else {
                statement.setNull(4, Types.VARCHAR);
            }
            String instanceDesc = (String)map.get("instanceDesc");
            if (instanceDesc != null && !instanceDesc.isEmpty()) {
                statement.setString(5, instanceDesc);
            } else {
                statement.setNull(5, Types.VARCHAR);
            }
            String zone = (String)map.get("zone");
            if (zone != null && !zone.isEmpty()) {
                statement.setString(6, zone);
            } else {
                statement.setNull(6, Types.VARCHAR);
            }
            String region = (String)map.get("region");
            if (region != null && !region.isEmpty()) {
                statement.setString(7, region);
            } else {
                statement.setNull(7, Types.VARCHAR);
            }
            String lob = (String)map.get("lob");
            if (lob != null && !lob.isEmpty()) {
                statement.setString(8, lob);
            } else {
                statement.setNull(8, Types.VARCHAR);
            }
            String resourceName = (String)map.get("resourceName");
            if (resourceName != null && !resourceName.isEmpty()) {
                statement.setString(9, resourceName);
            } else {
                statement.setNull(9, Types.VARCHAR);
            }
            String businessName = (String)map.get("businessName");
            if (businessName != null && !businessName.isEmpty()) {
                statement.setString(10, businessName);
            } else {
                statement.setNull(10, Types.VARCHAR);
            }
            String topicClassification = (String)map.get("topicClassification");
            if (topicClassification != null && !topicClassification.isEmpty()) {
                statement.setString(11, topicClassification);
            } else {
                statement.setNull(11, Types.VARCHAR);
            }
            statement.setString(12, (String)event.get(Constants.USER));
            statement.setObject(13, OffsetDateTime.parse((String)event.get(CloudEventV1.TIME)));
            statement.setLong(14, newAggregateVersion);
            statement.setObject(15, UUID.fromString(hostId));
            statement.setObject(16, UUID.fromString(instanceId));
            statement.setLong(17, oldAggregateVersion);
            statement.setObject(18, UUID.fromString((String) map.get("productVersionId")));
            statement.setString(19, serviceId);
            String envTag = (String)map.get("envTag");
            statement.setString(20, envTag);

            int count = statement.executeUpdate();
            if (count == 0) {
                if (queryInstanceExists(conn, hostId, instanceId)) {
                    throw new ConcurrencyException("Optimistic concurrency conflict during updateInstance for hostId " + hostId + " instanceId " + instanceId + ". Expected version " + oldAggregateVersion + " but found a different version " + newAggregateVersion + ".");
                } else {
                    throw new SQLException("No record found during updateInstance for hostId " + hostId + " instanceId " + instanceId + ".");
                }
            }
            // try to update current to false for others if current is true.
            if(current != null && current) {
                try (PreparedStatement statementUpdate = conn.prepareStatement(sqlUpdateCurrent)) {
                    statementUpdate.setObject(1, UUID.fromString(hostId));
                    statementUpdate.setString(2, serviceId);
                    statementUpdate.setObject(3, UUID.fromString(instanceId));
                    statementUpdate.executeUpdate();
                }
            }
        } catch (SQLException e) {
            logger.error("SQLException during updateInstance for hostId {} instanceId {} (old: {}) -> (new: {}): {}", hostId, instanceId, oldAggregateVersion, newAggregateVersion, e.getMessage(), e);
            throw e;
        } catch (Exception e) {
            logger.error("Exception during updateInstance for hostId {} instanceId {} (old: {}) -> (new: {}): {}", hostId, instanceId, oldAggregateVersion, newAggregateVersion, e.getMessage(), e);
            throw e;
        }
    }

    @Override
    public void deleteInstance(Connection conn, Map<String, Object> event) throws SQLException, Exception {
        final String sql = "DELETE FROM instance_t WHERE host_id = ? AND instance_id = ? AND aggregate_version = ?";
        Map<String, Object> map = (Map<String, Object>)event.get(PortalConstants.DATA);
        String hostId = (String)event.get(Constants.HOST);
        String instanceId = (String)map.get("instanceId");
        long oldAggregateVersion = SqlUtil.getOldAggregateVersion(event);

        try (PreparedStatement statement = conn.prepareStatement(sql)) {
            statement.setObject(1, UUID.fromString(hostId));
            statement.setObject(2, UUID.fromString(instanceId));

            int count = statement.executeUpdate();
            if (count == 0) {
                if (queryInstanceExists(conn, hostId, instanceId)) {
                    throw new ConcurrencyException("Optimistic concurrency conflict during deleteInstance for hostId " + hostId + " instanceId " + instanceId + " aggregateVersion " + oldAggregateVersion + " but found a different version or already updated.");
                } else {
                    throw new SQLException("No record found during deleteInstance for hostId " + hostId + " instanceId " + instanceId + ". It might have been already deleted.");
                }
            }
        } catch (SQLException e) {
            logger.error("SQLException during deleteInstance for hostId {} instanceId {} aggregateVersion {}: {}", hostId, instanceId, oldAggregateVersion, e.getMessage(), e);
            throw e;
        } catch (Exception e) {
            logger.error("Exception during deleteInstance for hostId {} instanceId {} aggregateVersion {}: {}", hostId, instanceId, oldAggregateVersion, e.getMessage(), e);
            throw e;
        }
    }

    @Override
    public void lockInstance(Connection conn, Map<String, Object> event) throws Exception {
        String sql = "UPDATE instance_t SET readonly = true, aggregate_version = ? " +
            "WHERE host_id = ? AND instance_id = ? AND aggregate_version = ?";
        Map<String, Object> map = (Map<String, Object>)event.get(PortalConstants.DATA);
        String hostId = (String)event.get(Constants.HOST);
        String instanceId = (String)map.get("instanceId");
        long oldAggregateVersion = SqlUtil.getOldAggregateVersion(event);

        try (PreparedStatement pstmt = conn.prepareStatement(sql)) {
            pstmt.setLong(1, SqlUtil.getNewAggregateVersion(event));
            pstmt.setObject(2, UUID.fromString(hostId));
            pstmt.setObject(3, UUID.fromString(instanceId));
            pstmt.setLong(4, oldAggregateVersion);

            int affectedRows = pstmt.executeUpdate();
            if (affectedRows == 0) {
                throw new SQLException("Locking instance failed, no rows affected.");
            }
        } catch (SQLException e) {
            logger.error("SQLException during lockInstance for hostId {} instanceId {} aggregateVersion {}: {}", hostId, instanceId, oldAggregateVersion, e.getMessage(), e);
            throw e;
        } catch (Exception e) {
            logger.error("Exception during lockInstance for hostId {} instanceId {} aggregateVersion {}: {}", hostId, instanceId, oldAggregateVersion, e.getMessage(), e);
            throw e;
        }
    }

    @Override
    public void unlockInstance(Connection conn, Map<String, Object> event) throws Exception {
        String sql = "UPDATE instance_t SET readonly = false, aggregate_version = ? " +
            "WHERE host_id = ? AND instance_id = ? AND aggregate_version = ?";
        Map<String, Object> map = (Map<String, Object>)event.get(PortalConstants.DATA);
        String hostId = (String)event.get(Constants.HOST);
        String instanceId = (String)map.get("instanceId");
        long oldAggregateVersion = SqlUtil.getOldAggregateVersion(event);

        try (PreparedStatement pstmt = conn.prepareStatement(sql)) {
            pstmt.setLong(1, SqlUtil.getNewAggregateVersion(event));
            pstmt.setObject(2, UUID.fromString(hostId));
            pstmt.setObject(3, UUID.fromString(instanceId));
            pstmt.setLong(4, oldAggregateVersion);

            int affectedRows = pstmt.executeUpdate();
            if (affectedRows == 0) {
                throw new SQLException("Unlocking instance failed, no rows affected.");
            }
        } catch (SQLException e) {
            logger.error("SQLException during unlockInstance for hostId {} instanceId {} aggregateVersion {}: {}", hostId, instanceId, oldAggregateVersion, e.getMessage(), e);
            throw e;
        } catch (Exception e) {
            logger.error("Exception during unlockInstance for hostId {} instanceId {} aggregateVersion {}: {}", hostId, instanceId, oldAggregateVersion, e.getMessage(), e);
            throw e;
        }
    }

    @SuppressWarnings("unchecked")
    @Override
    public void cloneInstance(Connection conn, Map<String, Object> event) throws Exception {
        final Map<String, Object> map = (Map<String, Object>) event.get(PortalConstants.DATA);
        final String hostId = (String) event.get(Constants.HOST);
        final String sourceInstanceId = (String) map.get("sourceInstanceId");
        final String targetInstanceId = (String) map.get("targetInstanceId");
        long oldAggregateVersion = SqlUtil.getOldAggregateVersion(event);

        transact(
            conn,
            connection -> {
                try {
                    // increment the aggregate version of the target instance to lock it during the clone operation
                    tryIncrementAggregateVersionOfInstance(conn, hostId, targetInstanceId, oldAggregateVersion, SqlUtil.getNewAggregateVersion(event));
                    Map<UUID, UUID> idMapping = getIdMappingForClone(connection, hostId, sourceInstanceId);
                    deleteDependentsOfTargetInstance(connection, hostId, targetInstanceId);
                    cloneFirstLevelDependentsOfTargetInstance(connection, hostId, sourceInstanceId, targetInstanceId, idMapping);
                    cloneSecondLevelDependentsOfTargetInstance(connection, hostId, sourceInstanceId, targetInstanceId, idMapping);
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            }
        );
    }

    @SuppressWarnings("unchecked")
    @Override
    public void promoteInstance(Connection conn, Map<String, Object> event) throws Exception {
        final Map<String, Object> map = (Map<String, Object>) event.get(PortalConstants.DATA);

        final String hostId = (String) event.get(Constants.HOST);
        final String instanceId = (String) map.get("instanceId");
        long oldAggregateVersion = SqlUtil.getOldAggregateVersion(event);
        final String user = (String) event.get(Constants.USER);
        final String updateTs = (String) event.get(CloudEventV1.TIME);
        final Map<String, Object> promotableConfigs = (Map<String, Object>) map.get("promotableConfigs");

        transact(
            conn,
            connection -> {
                try {
                    tryIncrementAggregateVersionOfInstance(conn, hostId, instanceId, oldAggregateVersion, SqlUtil.getNewAggregateVersion(event));
                    promoteConfigs(conn, new DbConsumablePromotableInstance(hostId, instanceId, promotableConfigs, user, updateTs));
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            }
        );
    }

    private void tryIncrementAggregateVersionOfInstance(Connection conn, String hostId,
        String instanceId, long oldAggregateVersion, long newAggregateVersion) throws SQLException {

        final String sql = "UPDATE instance_t SET aggregate_version = ? " +
            "WHERE host_id = ? AND instance_id = ? AND aggregate_version = ?";

        try (PreparedStatement pstmt = conn.prepareStatement(sql)) {
            pstmt.setLong(1, newAggregateVersion);
            pstmt.setObject(2, UUID.fromString(hostId));
            pstmt.setObject(3, UUID.fromString(instanceId));
            pstmt.setLong(4, oldAggregateVersion);

            int affectedRows = pstmt.executeUpdate();
            if (affectedRows == 0) {
                throw new SQLException("Incrementing aggregate version of instance failed, no rows affected.");
            }
        } catch (SQLException e) {
            logger.error("SQLException on increment of aggregate version for hostId {} instanceId {} aggregateVersion {}: {}",
                hostId, instanceId, oldAggregateVersion, e.getMessage(), e);
            throw e;
        } catch (Exception e) {
            logger.error("Exception on increment of aggregate version for hostId {} instanceId {} aggregateVersion {}: {}",
                hostId, instanceId, oldAggregateVersion, e.getMessage(), e);
            throw e;
        }
    }

    private void deleteDependentsOfTargetInstance(Connection connection, String hostId, String targetInstanceId)
        throws SQLException {
        final String sql =
            """
            WITH params AS (
              SELECT
                ?::UUID AS host_id,
                ?::UUID AS target_instance_id
            ),
            delete_target_instance_properties AS (
              DELETE FROM instance_property_t
              WHERE (host_id, instance_id, property_id)
              IN (
                SELECT ip.host_id, ip.instance_id, ip.property_id
                FROM instance_property_t ip
                JOIN config_property_t cp ON cp.property_id = ip.property_id
                JOIN config_t c ON c.config_id = cp.config_id
                WHERE CONCAT( c.config_name, '.', cp.property_name ) NOT IN ('server.serviceId', 'server.environment')
              )
            ),
            delete_target_instance_files AS (
              DELETE FROM instance_file_t
              WHERE (host_id, instance_id)
              IN (SELECT host_id, target_instance_id FROM params)
            ),
            delete_target_instance_app_api AS (
              DELETE FROM instance_app_api_t
              WHERE (host_id, instance_app_id, instance_api_id)
              IN (
                SELECT iappapi.host_id, iappapi.instance_app_id, iappapi.instance_api_id
                FROM instance_app_api_t iappapi
                JOIN instance_app_t iapp ON iapp.host_id = iappapi.host_id AND iapp.instance_app_id = iappapi.instance_app_id
                JOIN instance_api_t iapi ON iapi.host_id = iappapi.host_id AND iapi.instance_api_id = iappapi.instance_api_id
                JOIN params ON params.host_id = iapp.host_id AND params.host_id = iapi.host_id AND params.host_id = iappapi.host_id
                AND params.target_instance_id = iapp.instance_id AND params.target_instance_id = iapi.instance_id
              )
            ),
            delete_target_instance_app AS (
              DELETE FROM instance_app_t
              WHERE (host_id, instance_id)
              IN (SELECT host_id, target_instance_id FROM params)
            ),
            delete_target_instance_api AS (
              DELETE FROM instance_api_t
              WHERE (host_id, instance_id)
              IN (SELECT host_id, target_instance_id FROM params)
            )
            SELECT 'Delete from target instance completed' AS result
            """;

        try (PreparedStatement pstmt = connection.prepareStatement(sql)) {
            pstmt.setObject(1, UUID.fromString(hostId));
            pstmt.setObject(2, UUID.fromString(targetInstanceId));
            pstmt.execute();
        }
    }

    private void cloneFirstLevelDependentsOfTargetInstance(Connection connection, String hostId,
        String sourceInstanceId, String targetInstanceId, Map<UUID, UUID> idMapping) throws SQLException {

        final String sql = """
            WITH params AS (
              SELECT
                ?::UUID AS host_id,
                ?::UUID AS source_instance_id,
                ?::UUID AS target_instance_id
            ),
            id_mapping (old_id, new_id) AS MATERIALIZED ( %s ),
            instance_api_insert AS (
              INSERT INTO instance_api_t (host_id, instance_api_id, instance_id, api_version_id, active, update_user, update_ts)
              SELECT
                ia.host_id,
                m.new_id,
                p.target_instance_id,
                ia.api_version_id,
                ia.active,
                ia.update_user,
                ia.update_ts
              FROM instance_api_t ia
              JOIN params p ON p.host_id = ia.host_id AND p.source_instance_id = ia.instance_id
              JOIN id_mapping m ON ia.instance_api_id = m.old_id
              RETURNING *
            ),
            instance_app_insert AS (
              INSERT INTO instance_app_t (host_id, instance_app_id, instance_id, app_id, app_version, active, update_user, update_ts)
              SELECT
                iapp.host_id,
                m.new_id,
                p.target_instance_id,
                iapp.app_id,
                iapp.app_version,
                iapp.active,
                iapp.update_user,
                iapp.update_ts
              FROM instance_app_t iapp
              JOIN params p ON p.host_id = iapp.host_id AND p.source_instance_id = iapp.instance_id
              JOIN id_mapping m ON iapp.instance_app_id = m.old_id
              RETURNING *
            ),
            instance_property_insert AS (
              INSERT INTO instance_property_t (host_id, instance_id, property_id, property_value, update_user, update_ts)
              SELECT
                ip.host_id,
                p.target_instance_id,
                ip.property_id,
                ip.property_value,
                ip.update_user,
                ip.update_ts
              FROM instance_property_t ip
              JOIN params p ON ip.host_id = p.host_id AND p.source_instance_id = ip.instance_id
              JOIN config_property_t cp ON cp.property_id = ip.property_id
              JOIN config_t c ON c.config_id = cp.config_id
              WHERE CONCAT( c.config_name, '.', cp.property_name ) NOT IN ('server.serviceId', 'server.environment')
              RETURNING *
            ),
            instance_file_insert AS (
              INSERT INTO instance_file_t (host_id, instance_file_id, instance_id, file_type, file_name, file_value, file_desc, expiration_ts, update_user, update_ts)
              SELECT
                ifile.host_id,
                m.new_id,
                p.target_instance_id,
                ifile.file_type,
                ifile.file_name,
                ifile.file_value,
                ifile.file_desc,
                ifile.expiration_ts,
                ifile.update_user,
                ifile.update_ts
              FROM instance_file_t ifile
              JOIN params p ON ifile.host_id = p.host_id AND p.source_instance_id = ifile.instance_id
              JOIN id_mapping m ON ifile.instance_file_id = m.old_id
              RETURNING *
            )
            SELECT 'Insert first level dependents completed' AS result
            """;

        String idMappingValuesExpression = generateIdMappingValuesExpression(idMapping);
        String formattedSql = String.format(sql, idMappingValuesExpression);

        try (PreparedStatement pstmt = connection.prepareStatement(formattedSql)) {
            pstmt.setObject(1, UUID.fromString(hostId));
            pstmt.setObject(2, UUID.fromString(sourceInstanceId));
            pstmt.setObject(3, UUID.fromString(targetInstanceId));
            pstmt.execute();
        }
    }

    private void cloneSecondLevelDependentsOfTargetInstance(Connection connection, String hostId,
        String sourceInstanceId, String targetInstanceId, Map<UUID, UUID> idMapping) throws SQLException {
        final String sql = """
            WITH params AS (
              SELECT
                ?::UUID AS host_id,
                ?::UUID AS source_instance_id,
                ?::UUID AS target_instance_id
            ),
            id_mapping (old_id, new_id) AS MATERIALIZED ( %s ),
            instance_app_api_insert AS MATERIALIZED (
              INSERT INTO instance_app_api_t (host_id, instance_app_id, instance_api_id, active, update_user, update_ts)
              SELECT
                iappapi.host_id,
                m_app.new_id,
                m_api.new_id,
                iappapi.active,
                iappapi.update_user,
                iappapi.update_ts
              FROM instance_app_api_t iappapi
              JOIN instance_app_t iapp ON iapp.host_id = iappapi.host_id AND iapp.instance_app_id = iappapi.instance_app_id
              JOIN instance_api_t iapi ON iapi.host_id = iappapi.host_id AND iapi.instance_api_id = iappapi.instance_api_id
              JOIN params p ON p.host_id = iapp.host_id AND p.host_id = iapi.host_id AND p.host_id = iappapi.host_id
              AND p.source_instance_id = iapp.instance_id AND p.source_instance_id = iapi.instance_id
              JOIN id_mapping m_app ON iappapi.instance_app_id = m_app.old_id
              JOIN id_mapping m_api ON iappapi.instance_api_id = m_api.old_id
              RETURNING *
            ),
            instance_api_path_prefix_insert AS (
              INSERT INTO instance_api_path_prefix_t (host_id, instance_api_id, path_prefix, update_user, update_ts)
              SELECT
                iapipath.host_id,
                m.new_id,
                iapipath.path_prefix,
                iapipath.update_user,
                iapipath.update_ts
              FROM instance_api_path_prefix_t iapipath
              JOIN instance_api_t ia ON ia.host_id = iapipath.host_id AND ia.instance_api_id = iapipath.instance_api_id
              JOIN params p ON iapipath.host_id = p.host_id AND ia.host_id = p.host_id AND p.source_instance_id = ia.instance_id
              JOIN id_mapping m ON iapipath.instance_api_id = m.old_id
              RETURNING *
            ),
            instance_api_property_insert AS (
              INSERT INTO instance_api_property_t (host_id, instance_api_id, property_id, property_value, update_user, update_ts)
              SELECT
                iap.host_id,
                m.new_id,
                iap.property_id,
                iap.property_value,
                iap.update_user,
                iap.update_ts
              FROM instance_api_property_t iap
              JOIN instance_api_t ia ON ia.host_id = iap.host_id AND ia.instance_api_id = iap.instance_api_id
              JOIN params p ON iap.host_id = p.host_id AND ia.host_id = p.host_id AND p.source_instance_id = ia.instance_id
              JOIN id_mapping m ON iap.instance_api_id = m.old_id
              RETURNING *
            ),
            instance_app_property_insert AS (
              INSERT INTO instance_app_property_t (host_id, instance_app_id, property_id, property_value, update_user, update_ts)
              SELECT
                iap.host_id,
                m.new_id,
                iap.property_id,
                iap.property_value,
                iap.update_user,
                iap.update_ts
              FROM instance_app_property_t iap
              JOIN instance_app_t ia ON ia.host_id = iap.host_id AND ia.instance_app_id = iap.instance_app_id
              JOIN params p ON iap.host_id = p.host_id AND ia.host_id = p.host_id AND p.source_instance_id = ia.instance_id
              JOIN id_mapping m ON iap.instance_app_id = m.old_id
              RETURNING *
            ),
            instance_app_api_property_insert AS (
              INSERT INTO instance_app_api_property_t (host_id, instance_app_id, instance_api_id, property_id, property_value, update_user, update_ts)
              SELECT
                iaap.host_id,
                m_app.new_id,
                m_api.new_id,
                iaap.property_id,
                iaap.property_value,
                iaap.update_user,
                iaap.update_ts
              FROM instance_app_api_property_t iaap
              JOIN instance_app_api_t iappapi ON iappapi.instance_api_id = iaap.instance_api_id
              AND iappapi.instance_app_id = iaap.instance_app_id AND iappapi.host_id = iaap.host_id
              JOIN instance_app_t iapp ON iapp.host_id = iappapi.host_id AND iapp.instance_app_id = iappapi.instance_app_id
              JOIN instance_api_t iapi ON iapi.host_id = iappapi.host_id AND iapi.instance_api_id = iappapi.instance_api_id
              JOIN params p ON p.host_id = iapp.host_id AND p.host_id = iapi.host_id AND p.host_id = iappapi.host_id
              AND p.host_id = iaap.host_id AND p.source_instance_id = iapp.instance_id AND p.source_instance_id = iapi.instance_id
              JOIN id_mapping m_app ON iaap.instance_app_id = m_app.old_id
              JOIN id_mapping m_api ON iaap.instance_api_id = m_api.old_id
              RETURNING *
            )
            SELECT 'Insert second level dependents completed' AS result
            """;

        String idMappingValuesExpression = generateIdMappingValuesExpression(idMapping);
        String formattedSql = String.format(sql, idMappingValuesExpression);

        try (PreparedStatement pstmt = connection.prepareStatement(formattedSql)) {
            pstmt.setObject(1, UUID.fromString(hostId));
            pstmt.setObject(2, UUID.fromString(sourceInstanceId));
            pstmt.setObject(3, UUID.fromString(targetInstanceId));
            pstmt.execute();
        }
    }

    private String generateIdMappingValuesExpression(Map<UUID, UUID> idMapping) {
        String EMPTY_VALUES_SQL = "SELECT NULL::UUID, NULL::UUID WHERE FALSE";
        List<String> valuePairs = idMapping.entrySet()
            .stream()
            .map(entry -> String.format("('%s'::uuid, '%s'::uuid)", entry.getKey(), entry.getValue()))
            .toList();

        if (valuePairs.isEmpty()) {
            return EMPTY_VALUES_SQL;
        }

        return "VALUES" + " " + String.join(", ", valuePairs);
    }

    @SuppressWarnings("unchecked")
    private Map<UUID, UUID> getIdMappingForClone(Connection connection, String hostId, String instanceId) throws SQLException {
        Map<String, Object> retrievedIdsMap = retrieveAllIds(connection, hostId, instanceId);
        Set<UUID> retrievedIds = Stream.of(retrievedIdsMap.get("instanceApiIds"), retrievedIdsMap.get("instanceAppIds"), retrievedIdsMap.get("instanceFileIds"))
            .filter(Objects::nonNull)
            .flatMap(list -> ((List<UUID>) list).stream())
            .filter(Objects::nonNull)
            .collect(Collectors.toSet());

        return retrievedIds.stream()
            .collect(Collectors.toMap(
                id -> id,
                id -> UuidUtil.getUUID(),
                (existing, replacement) -> existing
            ));
    }

    private Map<String, Object> retrieveAllIds(Connection conn, String hostId, String instanceId) throws SQLException {
        final String sql =
            """
                SELECT
                  i.host_id, i.instance_id AS source_instance_id,
                  array_agg(DISTINCT iapi.instance_api_id) AS instance_api_ids,
                  array_agg(DISTINCT iapp.instance_app_id) AS instance_app_ids,
                  array_agg(DISTINCT ifile.instance_file_id) AS instance_file_ids
                FROM instance_t i
                LEFT JOIN instance_api_t iapi ON i.host_id = iapi.host_id AND i.instance_id = iapi.instance_id
                LEFT JOIN instance_app_t iapp ON i.host_id = iapp.host_id AND i.instance_id = iapp.instance_id
                LEFT JOIN instance_file_t ifile ON i.host_id = ifile.host_id AND i.instance_id = ifile.instance_id
                WHERE i.host_id = ? AND i.instance_id = ?
                GROUP BY i.host_id, i.instance_id
                """;

        Map<String, Object> result = new HashMap<>();
        try (PreparedStatement pstmt = conn.prepareStatement(sql)) {
            pstmt.setObject(1, UUID.fromString(hostId));
            pstmt.setObject(2, UUID.fromString(instanceId));

            try (ResultSet rs = pstmt.executeQuery()) {
                if (rs.next()) {
                    result.put("hostId", rs.getObject("host_id", UUID.class));
                    result.put("sourceInstanceId", rs.getObject("source_instance_id", UUID.class));
                    Array instanceApiIdsArray = rs.getArray("instance_api_ids");
                    if (instanceApiIdsArray != null) {
                        UUID[] instanceApiIds = (UUID[]) instanceApiIdsArray.getArray();
                        result.put("instanceApiIds", Arrays.asList(instanceApiIds));
                    } else {
                        result.put("instanceApiIds", Collections.emptyList());
                    }
                    Array instanceAppIdsArray = rs.getArray("instance_app_ids");
                    if (instanceAppIdsArray != null) {
                        UUID[] instanceAppIds = (UUID[]) instanceAppIdsArray.getArray();
                        result.put("instanceAppIds", Arrays.asList(instanceAppIds));
                    } else {
                        result.put("instanceAppIds", Collections.emptyList());
                    }
                    Array instanceFileIdsArray = rs.getArray("instance_file_ids");
                    if (instanceFileIdsArray != null) {
                        UUID[] instanceFileIds = (UUID[]) instanceFileIdsArray.getArray();
                        result.put("instanceFileIds", Arrays.asList(instanceFileIds));
                    } else {
                        result.put("instanceFileIds", Collections.emptyList());
                    }
                }
            }
        }

        return result;
    }

    @SuppressWarnings("unchecked")
    private static void promoteConfigs(Connection conn, DbConsumablePromotableInstance promotableInstance) throws SQLException {
        var instanceConfigs = promotableInstance.getInstanceConfigs();
        if (Objects.nonNull(instanceConfigs) && !instanceConfigs.isEmpty()) {
            final String sql =
                """
                WITH params AS MATERIALIZED (
                    SELECT
                        v.host_id::uuid as host_id,
                        v.instance_id::uuid as instance_id,
                        v.property_name::VARCHAR as property_name,
                        v.property_value::VARCHAR as property_value,
                        v.update_user::VARCHAR as update_user,
                        v.update_ts::TIMESTAMP as update_ts
                    FROM (
                        VALUES (?, ?, ?, ?, ?, ?)
                    ) as v(host_id, instance_id, property_name, property_value, update_user, update_ts)
                ),
                property_info AS (
                  SELECT DISTINCT cp.property_id, p.property_name
                  FROM config_property_t cp
                  JOIN config_t c ON cp.config_id = c.config_id
                  JOIN params p ON CONCAT(c.config_name, '.', cp.property_name) = p.property_name
                )
                INSERT INTO instance_property_t (host_id, instance_id, property_id, property_value, aggregate_version, update_user, update_ts)
                SELECT p.host_id, p.instance_id, pi.property_id, p.property_value, 1 AS aggregate_version, p.update_user, p.update_ts
                FROM params p
                JOIN property_info pi ON pi.property_name = p.property_name
                ON CONFLICT (host_id, instance_id, property_id)
                DO UPDATE SET
                  property_value = EXCLUDED.property_value,
                  aggregate_version = instance_property_t.aggregate_version + 1,
                  update_user = EXCLUDED.update_user,
                  update_ts = EXCLUDED.update_ts
                """;

            try (PreparedStatement preparedStatement = conn.prepareStatement(sql)) {
                for (Map<String, String> entry : instanceConfigs) {
                    preparedStatement.setObject(1, promotableInstance.getHostId());
                    preparedStatement.setObject(2, promotableInstance.getInstanceId());
                    preparedStatement.setString(3, entry.get(DbConsumablePromotableInstance.PROPERTY_NAME_KEY));
                    preparedStatement.setString(4, entry.get(DbConsumablePromotableInstance.PROPERTY_VALUE_KEY));
                    preparedStatement.setString(5, promotableInstance.getUpdateUser());
                    preparedStatement.setObject(6, promotableInstance.getUpdateTimestamp());
                    preparedStatement.addBatch();
                }
                preparedStatement.executeBatch();
            }
        }

        var apis = promotableInstance.getApis();
        if (Objects.nonNull(apis) && !apis.isEmpty()) {
            final String sql =
                """
                WITH params AS MATERIALIZED (
                    SELECT
                        v.host_id::uuid as host_id,
                        v.api_id::VARCHAR as api_id,
                        v.api_version::VARCHAR as api_version,
                        v.api_name::VARCHAR as api_name,
                        v.api_status::VARCHAR as api_status,
                        v.api_type::VARCHAR as api_type,
                        v.api_service_id::VARCHAR as api_service_id,
                        v.new_api_version_id::uuid as new_api_version_id,
                        v.update_user::VARCHAR as update_user,
                        v.update_ts::TIMESTAMP as update_ts
                    FROM (
                        values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    ) as v(host_id, api_id, api_version, api_name, api_status, api_type, api_service_id, new_api_version_id, update_user, update_ts)
                ),
                inserted_apis AS (
                    INSERT INTO api_t (host_id, api_id, api_name, api_status, update_user, update_ts)
                    SELECT host_id, api_id, api_name, api_status, update_user, update_ts FROM params
                    ON CONFLICT (host_id, api_id) DO NOTHING
                    RETURNING host_id, api_id
                )
                INSERT INTO api_version_t (host_id, api_version_id, api_id, api_version, api_type, service_id, update_user, update_ts)
                SELECT p.host_id, p.new_api_version_id, p.api_id, p.api_version, p.api_type, p.api_service_id, p.update_user, p.update_ts
                FROM params p
                WHERE NOT EXISTS (
                    SELECT 1 FROM api_version_t av WHERE av.host_id = p.host_id AND av.api_id = p.api_id AND av.api_version = p.api_version
                )
                ON CONFLICT (host_id, api_id, api_version) DO NOTHING
                """;

            try (PreparedStatement preparedStatement = conn.prepareStatement(sql)) {
                for (Map<String, String> entry : apis) {
                    preparedStatement.setObject(1, promotableInstance.getHostId());
                    preparedStatement.setString(2, entry.get(DbConsumablePromotableInstance.API_ID_KEY));
                    preparedStatement.setString(3, entry.get(DbConsumablePromotableInstance.API_VERSION_KEY));
                    preparedStatement.setString(4, entry.get(DbConsumablePromotableInstance.API_NAME_KEY));
                    preparedStatement.setString(5, entry.get(DbConsumablePromotableInstance.API_STATUS_KEY));
                    preparedStatement.setString(6, entry.get(DbConsumablePromotableInstance.API_TYPE_KEY));
                    preparedStatement.setString(7, entry.get(DbConsumablePromotableInstance.API_SERVICE_ID_KEY));
                    preparedStatement.setObject(8, UUID.fromString(entry.get(DbConsumablePromotableInstance.GENERATED_ID_KEY)));
                    preparedStatement.setString(9, promotableInstance.getUpdateUser());
                    preparedStatement.setObject(10, promotableInstance.getUpdateTimestamp());
                    preparedStatement.addBatch();
                }
                preparedStatement.executeBatch();
            }
        }

        var instanceApis = promotableInstance.getInstanceApis();
        if (Objects.nonNull(instanceApis) && !instanceApis.isEmpty()) {
            final String sql = """
                WITH params AS MATERIALIZED (
                    SELECT
                        v.host_id::uuid as host_id,
                        v.instance_id::uuid as instance_id,
                        v.new_instance_api_id::uuid as new_instance_api_id,
                        v.api_id::VARCHAR as api_id,
                        v.api_version::VARCHAR as api_version,
                        v.path_prefix::VARCHAR[] as path_prefix,
                        v.update_user::VARCHAR as update_user,
                        v.update_ts::TIMESTAMP as update_ts
                    FROM (
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                    ) as v(host_id, instance_id, new_instance_api_id, api_id, api_version, path_prefix, update_user, update_ts)
                ),
                api_version_lookup AS (
                    SELECT av.host_id, av.api_version_id
                    FROM api_version_t av
                    JOIN params p ON p.host_id = av.host_id AND p.api_id = av.api_id AND p.api_version = av.api_version
                ),
                inserted_instance_api AS (
                    INSERT INTO instance_api_t (host_id, instance_api_id, instance_id, api_version_id, active, update_user, update_ts)
                    SELECT p.host_id, p.new_instance_api_id, p.instance_id, av.api_version_id, TRUE, p.update_user, p.update_ts
                    FROM params p
                    JOIN api_version_lookup av ON av.host_id = p.host_id
                    WHERE NOT EXISTS (
                        SELECT 1 FROM instance_api_t ia JOIN api_version_t av ON ia.api_version_id = av.api_version_id
                        WHERE ia.host_id = p.host_id AND ia.instance_id = p.instance_id AND av.api_id = p.api_id AND av.api_version = p.api_version
                    )
                    ON CONFLICT (host_id, instance_id, api_version_id)
                    DO NOTHING
                    RETURNING host_id, instance_id, instance_api_id
                )
                INSERT INTO instance_api_path_prefix_t (host_id, instance_api_id, path_prefix, update_user, update_ts)
                SELECT i.host_id, i.instance_api_id, unnest(p.path_prefix), p.update_user, p.update_ts
                FROM inserted_instance_api i
                JOIN params p ON p.host_id = i.host_id AND p.instance_id = i.instance_id
                WHERE NOT EXISTS (
                    SELECT 1 FROM instance_api_path_prefix_t iapp
                    JOIN instance_api_t ia ON iapp.instance_api_id = ia.instance_api_id
                    JOIN api_version_t av ON ia.api_version_id = av.api_version_id
                    WHERE iapp.host_id = i.host_id
                      AND av.api_id = p.api_id
                      AND av.api_version = p.api_version
                      AND iapp.path_prefix = ANY(p.path_prefix)
                )
                ON CONFLICT (host_id, instance_api_id, path_prefix) DO NOTHING
                """;

            try (PreparedStatement preparedStatement = conn.prepareStatement(sql)) {
                for (Map<String, Object> entry : instanceApis) {
                    preparedStatement.setObject(1, promotableInstance.getHostId());
                    preparedStatement.setObject(2, promotableInstance.getInstanceId());
                    preparedStatement.setObject(3, UUID.fromString((String) entry.get(DbConsumablePromotableInstance.GENERATED_ID_KEY)));
                    preparedStatement.setString(4, (String) entry.get(DbConsumablePromotableInstance.API_ID_KEY));
                    preparedStatement.setString(5, (String) entry.get(DbConsumablePromotableInstance.API_VERSION_KEY));
                    preparedStatement.setString(6, SqlUtil.createArrayLiteral((List<String>) entry.get(DbConsumablePromotableInstance.API_PATH_PREFIXES_KEY)));
                    preparedStatement.setString(7, promotableInstance.getUpdateUser());
                    preparedStatement.setObject(8, promotableInstance.getUpdateTimestamp());
                    preparedStatement.addBatch();
                }
                preparedStatement.executeBatch();
            }
        }

        var instanceApps = promotableInstance.getInstanceApps();
        if (!instanceApps.isEmpty()) {
            final String sql = """
                WITH params AS MATERIALIZED (
                    SELECT
                        v.host_id::uuid as host_id,
                        v.instance_id::uuid as instance_id,
                        v.new_instance_app_id::uuid as new_instance_app_id,
                        v.app_id::VARCHAR as app_id,
                        v.app_name::VARCHAR as app_name,
                        v.app_version::VARCHAR as app_version,
                        v.update_user::VARCHAR as update_user,
                        v.update_ts::TIMESTAMP as update_ts
                    FROM (
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                    ) as v(host_id, instance_id, new_instance_app_id, app_id, app_name, app_version, update_user, update_ts)
                ),
                inserted_app AS (
                    INSERT INTO app_t (host_id, app_id, app_name, update_user, update_ts)
                    SELECT p.host_id, p.app_id, p.app_name, p.update_user, p.update_ts
                    FROM params p
                    WHERE NOT EXISTS (
                        SELECT 1 FROM app_t a WHERE a.host_id = p.host_id AND a.app_id = p.app_id
                    )
                    ON CONFLICT (host_id, app_id)
                    DO NOTHING
                    RETURNING host_id, app_id
                )
                INSERT INTO instance_app_t (host_id, instance_app_id, instance_id, app_id, app_version, active, update_user, update_ts)
                SELECT p.host_id, p.new_instance_app_id, p.instance_id, p.app_id, p.app_version, TRUE, p.update_user, p.update_ts
                FROM params p
                WHERE NOT EXISTS (
                    SELECT 1 FROM instance_app_t ia
                    WHERE ia.host_id = p.host_id AND ia.instance_id = p.instance_id AND ia.app_id = p.app_id AND ia.app_version = p.app_version
                )
                ON CONFLICT (host_id, instance_id, app_id, app_version)
                DO NOTHING
                """;

            try (PreparedStatement preparedStatement = conn.prepareStatement(sql)) {
                for (Map<String, String> entry : instanceApps) {
                    preparedStatement.setObject(1, promotableInstance.getHostId());
                    preparedStatement.setObject(2, promotableInstance.getInstanceId());
                    preparedStatement.setObject(3, UUID.fromString(entry.get(DbConsumablePromotableInstance.GENERATED_ID_KEY)));
                    preparedStatement.setString(4, entry.get(DbConsumablePromotableInstance.APP_ID_KEY));
                    preparedStatement.setString(5, entry.get(DbConsumablePromotableInstance.APP_NAME_KEY));
                    preparedStatement.setString(6, entry.get(DbConsumablePromotableInstance.APP_VERSION_KEY));
                    preparedStatement.setString(7, promotableInstance.getUpdateUser());
                    preparedStatement.setObject(8, promotableInstance.getUpdateTimestamp());
                    preparedStatement.addBatch();
                }
                preparedStatement.executeBatch();
            }
        }

        var instanceAppApis = promotableInstance.getInstanceAppApis();
        if (!instanceAppApis.isEmpty()) {
            final String sql = """
                WITH params AS MATERIALIZED (
                    SELECT
                        v.host_id::uuid as host_id,
                        v.instance_id::uuid as instance_id,
                        v.api_id::VARCHAR as api_id,
                        v.api_version::VARCHAR as api_version,
                        v.app_id::VARCHAR as app_id,
                        v.update_user::VARCHAR as update_user,
                        v.update_ts::TIMESTAMP as update_ts
                    FROM (
                        VALUES (?, ?, ?, ?, ?, ?, ?)
                    ) as v(host_id, instance_id, api_id, api_version, app_id, update_user, update_ts)
                ),
                instance_api_lookup AS (
                    SELECT DISTINCT ia.host_id, ia.instance_id, ia.instance_api_id
                    FROM instance_api_t ia
                    JOIN api_version_t av ON ia.api_version_id = av.api_version_id
                    JOIN params p ON ia.host_id = p.host_id
                      AND ia.instance_id = p.instance_id
                      AND av.api_id = p.api_id
                      AND av.api_version = p.api_version
                ),
                instance_app_lookup AS (
                    SELECT DISTINCT ia.host_id, ia.instance_id, ia.instance_app_id
                    FROM instance_app_t ia
                    JOIN params p ON ia.host_id = p.host_id
                      AND ia.instance_id = p.instance_id
                      AND ia.app_id = p.app_id
                )
                INSERT INTO instance_app_api_t (host_id, instance_app_id, instance_api_id, active, update_user, update_ts)
                SELECT p.host_id, iapp.instance_app_id, iapi.instance_api_id, TRUE, p.update_user, p.update_ts
                FROM params p
                JOIN instance_api_lookup iapi ON p.host_id = iapi.host_id AND p.instance_id = iapi.instance_id
                JOIN instance_app_lookup iapp ON p.host_id = iapp.host_id AND p.instance_id = iapp.instance_id
                WHERE NOT EXISTS (
                    SELECT 1
                    FROM instance_app_api_t iaa
                    WHERE iaa.host_id = p.host_id
                      AND iaa.instance_app_id = iapp.instance_app_id
                      AND iaa.instance_api_id = iapi.instance_api_id
                )
                ON CONFLICT (host_id, instance_app_id, instance_api_id) DO NOTHING
                """;

            try (PreparedStatement preparedStatement = conn.prepareStatement(sql)) {
                for (Map<String, String> entry : instanceAppApis) {
                    preparedStatement.setObject(1, promotableInstance.getHostId());
                    preparedStatement.setObject(2, promotableInstance.getInstanceId());
                    preparedStatement.setString(3, entry.get(DbConsumablePromotableInstance.API_ID_KEY));
                    preparedStatement.setString(4, entry.get(DbConsumablePromotableInstance.API_VERSION_KEY));
                    preparedStatement.setString(5, entry.get(DbConsumablePromotableInstance.APP_ID_KEY));
                    preparedStatement.setString(6, promotableInstance.getUpdateUser());
                    preparedStatement.setObject(7, promotableInstance.getUpdateTimestamp());
                    preparedStatement.addBatch();
                }
                preparedStatement.executeBatch();
            }
        }

        var instanceApiConfigs = promotableInstance.getInstanceApiConfigs();
        if (!instanceApiConfigs.isEmpty()) {
            final String deleteSql = """
                DELETE FROM instance_api_property_t WHERE (host_id, instance_api_id) IN (
                    SELECT ia.host_id, ia.instance_api_id
                    FROM instance_api_t ia
                    JOIN api_version_t av ON ia.host_id = av.host_id AND ia.api_version_id = av.api_version_id
                    WHERE ia.host_id = ? AND ia.instance_id = ? AND av.api_id = ? AND av.api_version = ?
                )
                """;

            try (PreparedStatement deletePreparedStatement = conn.prepareStatement(deleteSql)) {
                for (Map<String, String> entry : instanceApiConfigs) {
                    deletePreparedStatement.setObject(1, promotableInstance.getHostId());
                    deletePreparedStatement.setObject(2, promotableInstance.getInstanceId());
                    deletePreparedStatement.setString(3, entry.get(DbConsumablePromotableInstance.API_ID_KEY));
                    deletePreparedStatement.setString(4, entry.get(DbConsumablePromotableInstance.API_VERSION_KEY));
                    deletePreparedStatement.addBatch();
                }
                deletePreparedStatement.executeBatch();
            }

            final String upsertSql = """
                WITH params AS MATERIALIZED (
                    SELECT
                        v.host_id::uuid as host_id,
                        v.instance_id::uuid as instance_id,
                        v.api_id::VARCHAR as api_id,
                        v.api_version::VARCHAR as api_version,
                        v.property_name::VARCHAR as property_name,
                        v.property_value::VARCHAR as property_value,
                        v.update_user::VARCHAR as update_user,
                        v.update_ts::TIMESTAMP as update_ts
                    FROM (
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                    ) as v(host_id, instance_id, api_id, api_version, property_name, property_value, update_user, update_ts)
                ),
                property_info AS (
                    SELECT DISTINCT cp.property_id, p.property_name
                    FROM config_property_t cp
                    JOIN config_t c ON cp.config_id = c.config_id
                    JOIN params p ON CONCAT(c.config_name, '.', cp.property_name) = p.property_name
                    WHERE cp.resource_type IN ('api', 'api|app_api', 'all')
                ),
                instance_api_info AS (
                    SELECT DISTINCT ia.instance_api_id, av.api_id, av.api_version
                    FROM instance_api_t ia
                    JOIN api_version_t av ON ia.host_id = av.host_id AND ia.api_version_id = av.api_version_id
                    JOIN params p ON ia.host_id = p.host_id AND ia.instance_id = p.instance_id AND av.api_id = p.api_id AND av.api_version = p.api_version
                )
                INSERT INTO instance_api_property_t (host_id, instance_api_id, property_id, property_value, aggregate_version, update_user, update_ts)
                SELECT p.host_id, iai.instance_api_id, pi.property_id, p.property_value, 1 AS aggregate_version, p.update_user, p.update_ts
                FROM params p
                JOIN property_info pi ON p.property_name = pi.property_name
                JOIN instance_api_info iai ON iai.api_id = p.api_id AND iai.api_version = p.api_version
                ON CONFLICT (host_id, instance_api_id, property_id)
                DO UPDATE SET
                    property_value = EXCLUDED.property_value,
                    aggregate_version = instance_api_property_t.aggregate_version + 1,
                    update_user = EXCLUDED.update_user,
                    update_ts = EXCLUDED.update_ts
                """;

            try (PreparedStatement upsertPreparedStatement = conn.prepareStatement(upsertSql)) {
                for (Map<String, String> entry : instanceApiConfigs) {
                    upsertPreparedStatement.setObject(1, promotableInstance.getHostId());
                    upsertPreparedStatement.setObject(2, promotableInstance.getInstanceId());
                    upsertPreparedStatement.setString(3, entry.get(DbConsumablePromotableInstance.API_ID_KEY));
                    upsertPreparedStatement.setString(4, entry.get(DbConsumablePromotableInstance.API_VERSION_KEY));
                    upsertPreparedStatement.setString(5, entry.get(DbConsumablePromotableInstance.PROPERTY_NAME_KEY));
                    upsertPreparedStatement.setString(6, entry.get(DbConsumablePromotableInstance.PROPERTY_VALUE_KEY));
                    upsertPreparedStatement.setString(7, promotableInstance.getUpdateUser());
                    upsertPreparedStatement.setObject(8, promotableInstance.getUpdateTimestamp());
                    upsertPreparedStatement.addBatch();
                }
                upsertPreparedStatement.executeBatch();
            }
        }

        var instanceAppConfigs = promotableInstance.getInstanceAppConfigs();
        if (!instanceAppConfigs.isEmpty()) {
            final String deleteSql = """
                DELETE FROM instance_app_property_t
                WHERE (host_id, instance_app_id) IN (
                    SELECT ia.host_id, ia.instance_app_id
                    FROM instance_app_t ia
                    WHERE ia.host_id = ?
                    AND ia.instance_id = ?
                    AND ia.app_id = ?
                )
                """;

            try (PreparedStatement deletePreparedStatement = conn.prepareStatement(deleteSql)) {
                for (Map<String, String> entry : instanceAppConfigs) {
                    deletePreparedStatement.setObject(1, promotableInstance.getHostId());
                    deletePreparedStatement.setObject(2, promotableInstance.getInstanceId());
                    deletePreparedStatement.setString(3, entry.get(DbConsumablePromotableInstance.APP_ID_KEY));
                    deletePreparedStatement.addBatch();
                }
                deletePreparedStatement.executeBatch();
            }

            final String upsertSql = """
                WITH params AS MATERIALIZED (
                    SELECT
                        v.host_id::uuid as host_id,
                        v.instance_id::uuid as instance_id,
                        v.app_id::VARCHAR as app_id,
                        v.property_name::VARCHAR as property_name,
                        v.property_value::VARCHAR as property_value,
                        v.update_user::VARCHAR as update_user,
                        v.update_ts::TIMESTAMP as update_ts
                    FROM (
                        VALUES (?, ?, ?, ?, ?, ?, ?)
                    ) as v(host_id, instance_id, app_id, property_name, property_value, update_user, update_ts)
                ),
                property_info AS (
                    SELECT DISTINCT cp.property_id, p.property_name
                    FROM config_property_t cp
                    JOIN config_t c ON cp.config_id = c.config_id
                    JOIN params p ON CONCAT(c.config_name, '.', cp.property_name) = p.property_name
                    WHERE cp.resource_type IN ('app', 'app|app_api', 'all')
                ),
                instance_app_info AS (
                    SELECT DISTINCT ia.instance_app_id, ia.app_id
                    FROM instance_app_t ia
                    JOIN params p ON ia.host_id = p.host_id AND ia.instance_id = p.instance_id AND ia.app_id = p.app_id
                )
                INSERT INTO instance_app_property_t (host_id, instance_app_id, property_id, property_value, aggregate_version, update_user, update_ts)
                SELECT p.host_id, iai.instance_app_id, pi.property_id, p.property_value, 1 AS aggregate_version, p.update_user, p.update_ts
                FROM params p
                JOIN property_info pi ON pi.property_name = p.property_name
                JOIN instance_app_info iai ON iai.app_id = p.app_id
                ON CONFLICT (host_id, instance_app_id, property_id)
                DO UPDATE SET
                    property_value = EXCLUDED.property_value,
                    aggregate_version = instance_app_property_t.aggregate_version + 1,
                    update_user = EXCLUDED.update_user,
                    update_ts = EXCLUDED.update_ts
                """;

            try (PreparedStatement upsertPreparedStatement = conn.prepareStatement(upsertSql)) {
                for (Map<String, String> entry : instanceAppConfigs) {
                    upsertPreparedStatement.setObject(1, promotableInstance.getHostId());
                    upsertPreparedStatement.setObject(2, promotableInstance.getInstanceId());
                    upsertPreparedStatement.setString(3, entry.get(DbConsumablePromotableInstance.APP_ID_KEY));
                    upsertPreparedStatement.setString(4, entry.get(DbConsumablePromotableInstance.PROPERTY_NAME_KEY));
                    upsertPreparedStatement.setString(5, entry.get(DbConsumablePromotableInstance.PROPERTY_VALUE_KEY));
                    upsertPreparedStatement.setString(6, promotableInstance.getUpdateUser());
                    upsertPreparedStatement.setObject(7, promotableInstance.getUpdateTimestamp());
                    upsertPreparedStatement.addBatch();
                }
                upsertPreparedStatement.executeBatch();
            }
        }

        var instanceAppApiConfigs = promotableInstance.getInstanceAppApiConfigs();
        if (!instanceAppApiConfigs.isEmpty()) {
            final String deleteSql = """
                DELETE FROM instance_app_api_property_t
                WHERE (host_id, instance_app_id, instance_api_id) IN (
                    SELECT iaa.host_id, iaa.instance_app_id, iaa.instance_api_id
                    FROM instance_app_api_t iaa
                    JOIN instance_app_t ia ON iaa.host_id = ia.host_id AND iaa.instance_app_id = ia.instance_app_id
                    JOIN instance_api_t iai ON iaa.host_id = iai.host_id AND iaa.instance_api_id = iai.instance_api_id
                    JOIN api_version_t av ON iai.host_id = av.host_id AND iai.api_version_id = av.api_version_id
                    WHERE ia.host_id = ?
                    AND ia.instance_id = ?
                    AND ia.app_id = ?
                    AND av.api_id = ?
                    AND av.api_version = ?
                )
                """;

            try (PreparedStatement deletePreparedStatement = conn.prepareStatement(deleteSql)) {
                for (Map<String, String> entry : instanceAppApiConfigs) {
                    deletePreparedStatement.setObject(1, promotableInstance.getHostId());
                    deletePreparedStatement.setObject(2, promotableInstance.getInstanceId());
                    deletePreparedStatement.setString(3, entry.get(DbConsumablePromotableInstance.APP_ID_KEY));
                    deletePreparedStatement.setString(4, entry.get(DbConsumablePromotableInstance.API_ID_KEY));
                    deletePreparedStatement.setString(5, entry.get(DbConsumablePromotableInstance.API_VERSION_KEY));
                    deletePreparedStatement.addBatch();
                }
                deletePreparedStatement.executeBatch();
            }

            final String upsertSql = """
                WITH params AS MATERIALIZED (
                    SELECT
                        v.host_id::uuid as host_id,
                        v.instance_id::uuid as instance_id,
                        v.app_id::VARCHAR as app_id,
                        v.api_id::VARCHAR as api_id,
                        v.api_version::VARCHAR as api_version,
                        v.property_name::VARCHAR as property_name,
                        v.property_value::VARCHAR as property_value,
                        v.update_user::VARCHAR as update_user,
                        v.update_ts::TIMESTAMP as update_ts
                    FROM (
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                    ) as v(host_id, instance_id, app_id, api_id, api_version, property_name, property_value, update_user, update_ts)
                ),
                property_info AS (
                    SELECT DISTINCT cp.property_id, p.property_name
                    FROM config_property_t cp
                    JOIN config_t c ON cp.config_id = c.config_id
                    JOIN params p ON CONCAT(c.config_name, '.', cp.property_name) = p.property_name
                    WHERE cp.resource_type IN ('app_api', 'api|app_api', 'app|app_api', 'all')
                ),
                instance_app_api_info AS (
                    SELECT DISTINCT iaa.instance_app_id, iaa.instance_api_id, ia.app_id, av.api_id, av.api_version
                    FROM instance_app_api_t iaa
                    JOIN instance_app_t ia ON iaa.host_id = ia.host_id AND iaa.instance_app_id = ia.instance_app_id
                    JOIN instance_api_t iai ON iaa.host_id = iai.host_id AND iaa.instance_api_id = iai.instance_api_id
                    JOIN api_version_t av ON iai.host_id = av.host_id AND iai.api_version_id = av.api_version_id
                    JOIN params p ON ia.host_id = p.host_id AND ia.instance_id = p.instance_id AND ia.app_id = p.app_id
                    AND av.api_id = p.api_id AND av.api_version = p.api_version
                )
                INSERT INTO instance_app_api_property_t (host_id, instance_app_id, instance_api_id, property_id, property_value, aggregate_version, update_user, update_ts)
                SELECT p.host_id, iaai.instance_app_id, iaai.instance_api_id, pi.property_id, p.property_value, 1 AS aggregate_version, p.update_user, p.update_ts
                FROM params p
                JOIN property_info pi ON pi.property_name = p.property_name
                JOIN instance_app_api_info iaai ON iaai.api_id = p.api_id AND iaai.api_version = p.api_version AND iaai.app_id = p.app_id
                ON CONFLICT (host_id, instance_app_id, instance_api_id, property_id)
                DO UPDATE SET
                    property_value = EXCLUDED.property_value,
                    aggregate_version = instance_app_api_property_t.aggregate_version + 1,
                    update_user = EXCLUDED.update_user,
                    update_ts = EXCLUDED.update_ts
                """;

            try (PreparedStatement upsertPreparedStatement = conn.prepareStatement(upsertSql)) {
                for (Map<String, String> entry : instanceAppApiConfigs) {
                    upsertPreparedStatement.setObject(1, promotableInstance.getHostId());
                    upsertPreparedStatement.setObject(2, promotableInstance.getInstanceId());
                    upsertPreparedStatement.setString(3, entry.get(DbConsumablePromotableInstance.APP_ID_KEY));
                    upsertPreparedStatement.setString(4, entry.get(DbConsumablePromotableInstance.API_ID_KEY));
                    upsertPreparedStatement.setString(5, entry.get(DbConsumablePromotableInstance.API_VERSION_KEY));
                    upsertPreparedStatement.setString(6, entry.get(DbConsumablePromotableInstance.PROPERTY_NAME_KEY));
                    upsertPreparedStatement.setString(7, entry.get(DbConsumablePromotableInstance.PROPERTY_VALUE_KEY));
                    upsertPreparedStatement.setString(8, promotableInstance.getUpdateUser());
                    upsertPreparedStatement.setObject(9, promotableInstance.getUpdateTimestamp());
                    upsertPreparedStatement.addBatch();
                }
                upsertPreparedStatement.executeBatch();
            }
        }
    }

    @Override
    public Result<String> getInstance(int offset, int limit, String filtersJson, String globalFilter, String sortingJson, String hostId) {
        Result<String> result = null;

        final Map<String, String> columnMap = new HashMap<>(Map.of(
                "hostId", "i.host_id",
                "categoryId", "i.instance_id",
                "entityType", "i.instance_name",
                "categoryName", "i.product_version_id",
                "categoryDesc", "pv.product_id",
                "parentCategoryId", "pv.product_version",
                "sortOrder", "i.service_id",
                "updateUser", "i.current",
                "updateTs", "i.readonly",
                "active", "i.environment"
        ));
        columnMap.put("serviceDesc", "i.service_desc");
        columnMap.put("instanceDesc", "i.instance_desc");
        columnMap.put("zone", "i.zone");
        columnMap.put("region", "i.region");
        columnMap.put("lob", "i.lob");
        columnMap.put("resourceName", "i.resource_name");
        columnMap.put("businessName", "i.business_name");
        columnMap.put("envTag", "i.env_tag");
        columnMap.put("topicClassification", "i.topic_classification");
        columnMap.put("updateUser", "i.update_user");
        columnMap.put("updateTs", "i.update_ts");
        columnMap.put("aggregateVersion", "i.aggregate_version");
        columnMap.put("active", "i.active");

        List<Map<String, Object>> filters = parseJsonList(filtersJson);
        List<Map<String, Object>> sorting = parseJsonList(sortingJson);

        String s =
            """
                SELECT COUNT(*) OVER () AS total,
                i.host_id, i.instance_id, i.instance_name, i.product_version_id, pv.product_id, pv.product_version,
                i.service_id, i.current, i.readonly, i.environment, i.service_desc, i.instance_desc, i.zone, i.region,
                i.lob, i.resource_name, i.business_name, i.env_tag, i.topic_classification, i.update_user, i.update_ts,
                i.aggregate_version, i.active
                FROM instance_t i
                INNER JOIN product_version_t pv ON pv.product_version_id = i.product_version_id
                WHERE i.host_id = ?
            """;

        List<Object> parameters = new ArrayList<>();
        parameters.add(UUID.fromString(hostId));

        String[] searchColumns = {"i.instance_name", "i.service_desc", "i.instance_desc", "i.zone", "i.region", "i.lob", "i.resource_name", "i.business_name"};
        String sqlBuilder = s + dynamicFilter(Arrays.asList("i.host_id", "i.instance_id", "i.product_version_id"), Arrays.asList(searchColumns), filters, columnMap, parameters) +
                globalFilter(globalFilter, searchColumns, parameters) +
                dynamicSorting("i.instance_id", sorting, columnMap) +
                "\nLIMIT ? OFFSET ?";

        parameters.add(limit);
        parameters.add(offset);
        int total = 0;
        List<Map<String, Object>> instances = new ArrayList<>();

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
                    map.put("hostId", resultSet.getObject("host_id", UUID.class));
                    map.put("instanceId", resultSet.getObject("instance_id", UUID.class));
                    map.put("instanceName", resultSet.getString("instance_name"));
                    map.put("productVersionId", resultSet.getObject("product_version_id", UUID.class));
                    map.put("productId", resultSet.getString("product_id"));
                    map.put("productVersion", resultSet.getString("product_version"));
                    map.put("serviceId", resultSet.getString("service_id"));
                    map.put("current", resultSet.getBoolean("current"));
                    map.put("readonly", resultSet.getBoolean("readonly"));
                    map.put("environment", resultSet.getString("environment"));
                    map.put("serviceDesc", resultSet.getString("service_desc"));
                    map.put("instanceDesc", resultSet.getString("instance_desc"));
                    map.put("zone", resultSet.getString("zone"));
                    map.put("region", resultSet.getString("region"));
                    map.put("lob", resultSet.getString("lob"));
                    map.put("resourceName", resultSet.getString("resource_name"));
                    map.put("businessName", resultSet.getString("business_name"));
                    map.put("envTag", resultSet.getString("env_tag"));
                    map.put("topicClassification", resultSet.getString("topic_classification"));
                    map.put("updateUser", resultSet.getString("update_user"));
                    map.put("updateTs", resultSet.getObject("update_ts") != null ? resultSet.getObject("update_ts", OffsetDateTime.class) : null);
                    map.put("aggregateVersion", resultSet.getLong("aggregate_version"));
                    map.put("active", resultSet.getBoolean("active"));
                    instances.add(map);
                }
            }
            Map<String, Object> resultMap = new HashMap<>();
            resultMap.put("total", total);
            resultMap.put("instances", instances);
            result = Success.of(JsonMapper.toJson(resultMap));
        } catch (SQLException e) {
            logger.error("SQLException:", e);
            result = Failure.of(new Status(SQL_EXCEPTION, e.getMessage()));
        }  catch (Exception e) {
            logger.error("Exception:", e);
            result = Failure.of(new Status(GENERIC_EXCEPTION, e.getMessage()));
        }
        return result;
    }

    @Override
    public Result<String> getInstanceLabel(String hostId) {
        Result<String> result = null;
        String sql =
                """
                SELECT i.instance_id, i.instance_name, pv.product_id, pv.product_version
                FROM instance_t i
                INNER JOIN product_version_t pv ON pv.product_version_id = i.product_version_id
                WHERE i.host_id = ?
                """;
        List<Map<String, Object>> labels = new ArrayList<>();
        try (Connection connection = ds.getConnection();
             PreparedStatement preparedStatement = connection.prepareStatement(sql)) {
            preparedStatement.setObject(1, UUID.fromString(hostId));
            try (ResultSet resultSet = preparedStatement.executeQuery()) {
                while (resultSet.next()) {
                    Map<String, Object> map = new HashMap<>();
                    map.put("id", resultSet.getString("instance_id"));
                    map.put("label", resultSet.getString("instance_name") + "|" +
                            resultSet.getString("product_id") + "|" + resultSet.getString("product_version"));
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
    public void createInstanceApi(Connection conn, Map<String, Object> event) throws SQLException, Exception {
        // Use UPSERT based on the Primary Key (host_id, instance_api_id): INSERT ON CONFLICT DO UPDATE
        // This handles:
        // 1. First time insert (no conflict).
        // 2. Re-creation (conflict on PK) -> UPDATE the existing soft-deleted row (setting active=TRUE and new version).

        final String sql =
                """
                INSERT INTO instance_api_t(
                    host_id,
                    instance_api_id,
                    instance_id,
                    api_version_id,
                    active,
                    update_user,
                    update_ts,
                    aggregate_version
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT (host_id, instance_api_id) DO UPDATE
                SET instance_id = EXCLUDED.instance_id,
                    api_version_id = EXCLUDED.api_version_id,
                    active = EXCLUDED.active,
                    update_user = EXCLUDED.update_user,
                    update_ts = EXCLUDED.update_ts,
                    aggregate_version = EXCLUDED.aggregate_version
                -- OCC/IDM: Only update if the incoming event is newer
                WHERE instance_api_t.aggregate_version < EXCLUDED.aggregate_version
                """;

        // Note: The original code uses a non-standard map retrieval: Map<String, Object> map = (Map<String, Object>)event.get(PortalConstants.DATA);
        // Assuming SqlUtil.extractEventData(event) is the correct utility based on other methods.
        Map<String, Object> map = SqlUtil.extractEventData(event);
        String hostId = (String)map.get("hostId"); // Assuming hostId is in the data map, not just event metadata
        String instanceApiId = (String)map.get("instanceApiId");
        String instanceId = (String)map.get("instanceId");
        String apiVersionId = (String)map.get("apiVersionId");
        long newAggregateVersion = SqlUtil.getNewAggregateVersion(event);

        try (PreparedStatement statement = conn.prepareStatement(sql)) {
            // INSERT values (8 placeholders)
            int i = 1;
            // 1: host_id (Required, part of PK)
            statement.setObject(i++, UUID.fromString(hostId));
            // 2: instance_api_id (Required, part of PK)
            statement.setObject(i++, UUID.fromString(instanceApiId));
            // 3: instance_id (Required)
            statement.setObject(i++, UUID.fromString(instanceId));
            // 4: api_version_id (Required)
            statement.setObject(i++, UUID.fromString(apiVersionId));

            // 5: active (Default TRUE in DDL, using payload value or TRUE)
            Boolean active = (Boolean)map.getOrDefault("active", Boolean.TRUE);
            statement.setBoolean(i++, active);

            // 6: update_user
            statement.setString(i++, (String)event.get(Constants.USER));
            // 7: update_ts
            statement.setObject(i++, OffsetDateTime.parse((String)event.get(CloudEventV1.TIME)));
            // 8: aggregate_version
            statement.setLong(i++, newAggregateVersion);

            int count = statement.executeUpdate();
            if (count == 0) {
                // count=0 means the ON CONFLICT clause was hit, BUT the WHERE clause (aggregate_version < EXCLUDED.aggregate_version) failed.
                // This is the desired idempotent/out-of-order protection behavior. Log and ignore.
                logger.warn("Creation/Reactivation skipped for hostId {} instanceApiId {} aggregateVersion {}. A newer or same version already exists.", hostId, instanceApiId, newAggregateVersion);
            }
        } catch (SQLException e) {
            logger.error("SQLException during createInstanceApi for hostId {} instanceApiId {} aggregateVersion {}: {}", hostId, instanceApiId, newAggregateVersion, e.getMessage(), e);
            throw e;
        } catch (Exception e) {
            logger.error("Exception during createInstanceApi for hostId {} instanceApiId {} aggregateVersion {}: {}", hostId, instanceApiId, newAggregateVersion, e.getMessage(), e);
            throw e;
        }
    }

    @Override
    public void updateInstanceApi(Connection conn, Map<String, Object> event) throws SQLException, Exception {
        // We attempt to update the record IF the incoming event's aggregate_version is greater than the current projection's version.
        // This enforces Idempotence (IDM) and Optimistic Concurrency Control (OCC) by ensuring version monotonicity.
        // Note: The 'active' state is updated based on the event payload (if present).
        final String sql =
                """
                UPDATE instance_api_t
                SET instance_id = ?,
                    api_version_id = ?,
                    active = ?,
                    update_user = ?,
                    update_ts = ?,
                    aggregate_version = ?
                WHERE host_id = ?
                  AND instance_api_id = ?
                  AND aggregate_version < ?
                """; // <<< CRITICAL: Changed aggregate_version = ? to aggregate_version < ? to enforce monotonicity (OCC/IDM)

        // Note: The original code uses a non-standard map retrieval: Map<String, Object> map = (Map<String, Object>)event.get(PortalConstants.DATA);
        // Assuming SqlUtil.extractEventData(event) is the correct utility based on other methods.
        Map<String, Object> map = SqlUtil.extractEventData(event);
        String hostId = (String)map.get("hostId"); // Getting hostId from map data as in other methods
        String instanceApiId = (String)map.get("instanceApiId");
        long newAggregateVersion = SqlUtil.getNewAggregateVersion(event);

        try (PreparedStatement statement = conn.prepareStatement(sql)) {
            // SET values (6 dynamic values)
            int i = 1;
            // 1. instance_id
            statement.setObject(i++, UUID.fromString((String)map.get("instanceId")));
            // 2. api_version_id
            statement.setObject(i++, UUID.fromString((String)map.get("apiVersionId")));

            // 3. active (Default TRUE in DDL, using payload value or TRUE)
            Boolean active = (Boolean)map.getOrDefault("active", Boolean.TRUE);
            statement.setBoolean(i++, active);

            // 4. update_user
            statement.setString(i++, (String)event.get(Constants.USER));
            // 5. update_ts
            statement.setObject(i++, OffsetDateTime.parse((String)event.get(CloudEventV1.TIME)));
            // 6. aggregate_version
            statement.setLong(i++, newAggregateVersion);

            // WHERE conditions (3 placeholders)
            // 7. host_id
            statement.setObject(i++, UUID.fromString(hostId));
            // 8. instance_api_id
            statement.setObject(i++, UUID.fromString(instanceApiId));
            // 9. aggregate_version < ? (new version for OCC/IDM check)
            statement.setLong(i++, newAggregateVersion);

            int count = statement.executeUpdate();
            if (count == 0) {
                // If 0 rows updated, it means the record was either not found
                // OR aggregate_version >= newAggregateVersion (OCC/IDM check failed).
                // We IGNORE the failure and log a warning, as this is the desired idempotent/monotonic behavior.
                logger.warn("Update skipped for hostId {} instanceApiId {} aggregateVersion {}. Record not found or a newer/same version already exists.", hostId, instanceApiId, newAggregateVersion);
            }
        } catch (SQLException e) {
            logger.error("SQLException during updateInstanceApi for hostId {} instanceApiId {} aggregateVersion {}: {}", hostId, instanceApiId, newAggregateVersion, e.getMessage(), e);
            throw e;
        } catch (Exception e) {
            logger.error("Exception during updateInstanceApi for hostId {} instanceApiId {} aggregateVersion {}: {}", hostId, instanceApiId, newAggregateVersion, e.getMessage(), e);
            throw e;
        }
    }

    @Override
    public void deleteInstanceApi(Connection conn, Map<String, Object> event) throws SQLException, Exception {
        // Use UPDATE to implement Soft Delete (setting active = FALSE).
        // OCC/IDM is enforced by checking aggregate_version < newAggregateVersion.
        final String sql =
                """
                UPDATE instance_api_t
                SET active = FALSE,
                    update_user = ?,
                    update_ts = ?,
                    aggregate_version = ?
                WHERE host_id = ?
                  AND instance_api_id = ?
                  AND aggregate_version < ?
                """; // <<< CRITICAL: Added aggregate_version < ? to enforce monotonicity (OCC/IDM)

        // Note: The original code retrieves hostId from event metadata,
        // while the data map is used for instanceApiId.
        Map<String, Object> map = SqlUtil.extractEventData(event);
        String hostId = (String)event.get(Constants.HOST);
        String instanceApiId = (String)map.get("instanceApiId");
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

            // WHERE conditions (3 placeholders)
            // 4: host_id
            statement.setObject(4, UUID.fromString(hostId));
            // 5: instance_api_id
            statement.setObject(5, UUID.fromString(instanceApiId));
            // 6: aggregate_version < ? (new version for OCC/IDM check)
            statement.setLong(6, newAggregateVersion);

            int count = statement.executeUpdate();
            if (count == 0) {
                // If 0 rows updated, it means:
                // 1. The record was not found (already deleted or never existed).
                // 2. The OCC/IDM check failed (aggregate_version >= newAggregateVersion).
                // We IGNORE the failure and log a warning, as this is the desired idempotent/monotonic behavior.
                logger.warn("Soft delete skipped for hostId {} instanceApiId {} aggregateVersion {}. Record not found or a newer/same version already exists.", hostId, instanceApiId, newAggregateVersion);
            }
        } catch (SQLException e) {
            logger.error("SQLException during deleteInstanceApi for hostId {} instanceApiId {} aggregateVersion {}: {}", hostId, instanceApiId, newAggregateVersion, e.getMessage(), e);
            throw e;
        } catch (Exception e) {
            logger.error("Exception during deleteInstanceApi for hostId {} instanceApiId {} aggregateVersion {}: {}", hostId, instanceApiId, newAggregateVersion, e.getMessage(), e);
            throw e;
        }
    }

    @Override
    public Result<String> getInstanceApi(int offset, int limit, String filtersJson, String globalFilter, String sortingJson, String hostId) {
        Result<String> result = null;
        final Map<String, String> columnMap = new HashMap<>(Map.of(
                "hostId", "ia.host_id",
                "instanceApiId", "ia.instance_api_id",
                "instanceId", "ia.instance_id",
                "instanceName", "i.instance_name",
                "productId", "pv.product_id",
                "productVersion", "pv.product_version",
                "apiVersionId", "ia.api_version_id",
                "apiId", "av.api_id",
                "apiVersion", "av.api_version",
                "active", "ia.active"
        ));
        columnMap.put("updateUser", "ia.update_user");
        columnMap.put("updateTs", "ia.update_ts");
        columnMap.put("aggregateVersion", "ia.aggregate_version");

        List<Map<String, Object>> filters = parseJsonList(filtersJson);
        List<Map<String, Object>> sorting = parseJsonList(sortingJson);

        String s =
            """
                SELECT COUNT(*) OVER () AS total,
                ia.host_id, ia.instance_api_id, ia.instance_id, i.instance_name, pv.product_id,
                pv.product_version, ia.api_version_id, av.api_id, av.api_version, ia.active,
                ia.update_user, ia.update_ts, ia.aggregate_version
                FROM instance_api_t ia
                INNER JOIN instance_t i ON ia.instance_id = i.instance_id
                INNER JOIN product_version_t pv ON i.product_version_id = pv.product_version_id
                INNER JOIN api_version_t av ON ia.api_version_id = av.api_version_id
                WHERE ia.host_id = ?
            """;

        List<Object> parameters = new ArrayList<>();
        parameters.add(UUID.fromString(hostId));

        String[] searchColumns = {"i.instance_name"};
        String sqlBuilder = s + dynamicFilter(Arrays.asList("ia.host_id", "ia.instance_api_id", "ia.instance_id", "ia.api_version_id"), Arrays.asList(searchColumns), filters, columnMap, parameters) +
                globalFilter(globalFilter, searchColumns, parameters) +
                dynamicSorting("instance_id, api_version_id", sorting, columnMap) +
                "\nLIMIT ? OFFSET ?";

        parameters.add(limit);
        parameters.add(offset);
        int total = 0;
        List<Map<String, Object>> instanceApis = new ArrayList<>();

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
                    map.put("hostId", resultSet.getObject("host_id", UUID.class));
                    map.put("instanceApiId", resultSet.getObject("instance_api_id", UUID.class));
                    map.put("instanceId", resultSet.getObject("instance_id", UUID.class));
                    map.put("instanceName", resultSet.getString("instance_name"));
                    map.put("productId", resultSet.getString("product_id"));
                    map.put("productVersion", resultSet.getString("product_version"));
                    map.put("apiVersionId", resultSet.getObject("api_version_id", UUID.class));
                    map.put("apiId", resultSet.getString("api_id"));
                    map.put("apiVersion", resultSet.getString("api_version"));
                    map.put("active", resultSet.getBoolean("active"));
                    map.put("updateUser", resultSet.getString("update_user"));
                    map.put("updateTs", resultSet.getObject("update_ts") != null ? resultSet.getObject("update_ts", OffsetDateTime.class) : null);
                    map.put("aggregateVersion", resultSet.getLong("aggregate_version"));
                    instanceApis.add(map);
                }
            }

            Map<String, Object> resultMap = new HashMap<>();
            resultMap.put("total", total);
            resultMap.put("instanceApis", instanceApis);
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

    public Result<String> getInstanceApiLabel(String hostId, String instanceId) {
        Result<String> result = null;
        String sql =
                """
                SELECT ia.instance_api_id, i.instance_name, av.api_id, av.api_version
                FROM instance_api_t ia
                INNER JOIN instance_t i ON i.instance_id = ia.instance_id
                INNER JOIN api_version_t av ON av.api_version_id = ia.api_version_id
                WHERE ia.host_id = ?
                """;
        if(instanceId != null && !instanceId.isEmpty()) sql += " AND ia.instance_id = ?";
        List<Map<String, Object>> labels = new ArrayList<>();
        try (Connection connection = ds.getConnection();
             PreparedStatement preparedStatement = connection.prepareStatement(sql)) {
            preparedStatement.setObject(1, UUID.fromString(hostId));
            if(instanceId != null && !instanceId.isEmpty()) {
                preparedStatement.setObject(2, UUID.fromString(instanceId));
            }
            try (ResultSet resultSet = preparedStatement.executeQuery()) {
                while (resultSet.next()) {
                    Map<String, Object> map = new HashMap<>();
                    map.put("id", resultSet.getString("instance_api_id"));
                    map.put("label", resultSet.getString("api_id") + "|" + resultSet.getString("api_version") +  "|" + resultSet.getString("instance_name"));
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
    public void createInstanceApiPathPrefix(Connection conn, Map<String, Object> event) throws SQLException, Exception {
        // Use UPSERT based on the Primary Key (host_id, instance_api_id, path_prefix): INSERT ON CONFLICT DO UPDATE
        // This handles:
        // 1. First time insert (no conflict).
        // 2. Re-creation (conflict on PK) -> UPDATE the existing soft-deleted row (setting active=TRUE and new version).

        final String sql =
                """
                INSERT INTO instance_api_path_prefix_t(
                    host_id,
                    instance_api_id,
                    path_prefix,
                    update_user,
                    update_ts,
                    aggregate_version,
                    active
                ) VALUES (?, ?, ?, ?, ?, ?, TRUE)
                ON CONFLICT (host_id, instance_api_id, path_prefix) DO UPDATE
                SET update_user = EXCLUDED.update_user,
                    update_ts = EXCLUDED.update_ts,
                    aggregate_version = EXCLUDED.aggregate_version,
                    active = TRUE
                -- OCC/IDM: Only update if the incoming event is newer
                WHERE instance_api_path_prefix_t.aggregate_version < EXCLUDED.aggregate_version
                """;

        // Note: The original code uses a non-standard map retrieval: Map<String, Object> map = (Map<String, Object>)event.get(PortalConstants.DATA);
        // Assuming SqlUtil.extractEventData(event) is the correct utility based on other methods.
        Map<String, Object> map = SqlUtil.extractEventData(event);
        String hostId = (String)map.get("hostId"); // Getting hostId from map data (or event metadata, adjusted for safety)
        String instanceApiId = (String)map.get("instanceApiId");
        String pathPrefix = (String)map.get("pathPrefix");
        long newAggregateVersion = SqlUtil.getNewAggregateVersion(event);

        try (PreparedStatement statement = conn.prepareStatement(sql)) {
            // INSERT values (6 placeholders + active=TRUE in SQL, total 6 dynamic values)
            int i = 1;
            // 1. host_id (Required, part of PK)
            statement.setObject(i++, UUID.fromString(hostId));
            // 2. instance_api_id (Required, part of PK)
            statement.setObject(i++, UUID.fromString(instanceApiId));
            // 3. path_prefix (Required, part of PK)
            statement.setString(i++, pathPrefix);

            // 4. update_user
            statement.setString(i++, (String)event.get(Constants.USER));
            // 5. update_ts
            statement.setObject(i++, OffsetDateTime.parse((String)event.get(CloudEventV1.TIME)));
            // 6. aggregate_version
            statement.setLong(i++, newAggregateVersion);

            int count = statement.executeUpdate();
            if (count == 0) {
                // count=0 means the ON CONFLICT clause was hit, BUT the WHERE clause (aggregate_version < EXCLUDED.aggregate_version) failed.
                // This is the desired idempotent/out-of-order protection behavior. Log and ignore.
                logger.warn("Creation/Reactivation skipped for hostId {} instanceApiId {} pathPrefix {} aggregateVersion {}. A newer or same version already exists.", hostId, instanceApiId, pathPrefix, newAggregateVersion);
            }
        } catch (SQLException e) {
            logger.error("SQLException during createInstanceApiPathPrefix for hostId {} instanceApiId {} pathPrefix {} aggregateVersion {}: {}", hostId, instanceApiId, pathPrefix, newAggregateVersion, e.getMessage(), e);
            throw e;
        } catch (Exception e) {
            logger.error("Exception during createInstanceApiPathPrefix for hostId {} instanceApiId {} pathPrefix {} aggregateVersion {}: {}", hostId, instanceApiId, pathPrefix, newAggregateVersion, e.getMessage(), e);
            throw e;
        }
    }

    @Override
    public void updateInstanceApiPathPrefix(Connection conn, Map<String, Object> event) throws SQLException, Exception {
        // We attempt to update the record IF the incoming event's aggregate_version is greater than the current projection's version.
        // This enforces Idempotence (IDM) and Optimistic Concurrency Control (OCC) by ensuring version monotonicity.
        // We explicitly set active = TRUE as an UPDATE event implies the path prefix should be active.
        final String sql =
                """
                UPDATE instance_api_path_prefix_t
                SET update_user = ?,
                    update_ts = ?,
                    aggregate_version = ?,
                    active = TRUE
                WHERE host_id = ?
                  AND instance_api_id = ?
                  AND path_prefix = ?
                  AND aggregate_version < ?
                """; // <<< CRITICAL: Changed aggregate_version = ? to aggregate_version < ? to enforce monotonicity (OCC/IDM)
        // Note: Path_prefix removed from SET as it's part of the PK and shouldn't be updated.

        // Note: The original code retrieves hostId from event metadata,
        // while the data map is used for instanceApiId and pathPrefix.
        Map<String, Object> map = SqlUtil.extractEventData(event);
        String hostId = (String)map.get("hostId"); // Assuming hostId is in the data map, consistent with other methods
        String instanceApiId = (String)map.get("instanceApiId");
        String pathPrefix = (String)map.get("pathPrefix");
        long newAggregateVersion = SqlUtil.getNewAggregateVersion(event);

        try (PreparedStatement statement = conn.prepareStatement(sql)) {
            // SET values (3 dynamic values + active = TRUE in SQL)
            int i = 1;
            // 1. update_user
            statement.setString(i++, (String)event.get(Constants.USER));
            // 2. update_ts
            statement.setObject(i++, OffsetDateTime.parse((String)event.get(CloudEventV1.TIME)));
            // 3. aggregate_version
            statement.setLong(i++, newAggregateVersion);

            // WHERE conditions (4 placeholders)
            // 4. host_id
            statement.setObject(i++, UUID.fromString(hostId));
            // 5. instance_api_id
            statement.setObject(i++, UUID.fromString(instanceApiId));
            // 6. path_prefix
            statement.setString(i++, pathPrefix);
            // 7. aggregate_version < ? (new version for OCC/IDM check)
            statement.setLong(i++, newAggregateVersion);

            int count = statement.executeUpdate();
            if (count == 0) {
                // If 0 rows updated, it means the record was either not found
                // OR aggregate_version >= newAggregateVersion (OCC/IDM check failed).
                // We IGNORE the failure and log a warning, as this is the desired idempotent/monotonic behavior.
                logger.warn("Update skipped for hostId {} instanceApiId {} pathPrefix {} aggregateVersion {}. Record not found or a newer/same version already exists.", hostId, instanceApiId, pathPrefix, newAggregateVersion);
            }
        } catch (SQLException e) {
            logger.error("SQLException during updateInstanceApiPathPrefix for hostId {} instanceApiId {} pathPrefix {} aggregateVersion {}: {}", hostId, instanceApiId, pathPrefix, newAggregateVersion, e.getMessage(), e);
            throw e;
        } catch (Exception e) {
            logger.error("Exception during updateInstanceApiPathPrefix for hostId {} instanceApiId {} pathPrefix {} aggregateVersion {}: {}", hostId, instanceApiId, pathPrefix, newAggregateVersion, e.getMessage(), e);
            throw e;
        }
    }


    /**
     * Performs a soft delete on an instance_api_path_prefix record using an idempotent/monotonic pattern.
     * This method implements:
     * 1.  **Soft Delete:** Sets the 'active' flag to false.
     * 2.  **IDM:** Explicitly sets 'update_user' and 'update_ts' from the event metadata.
     * 3.  **OCC/Idempotency:** Uses `aggregate_version < ?` to ensure the operation only succeeds if it
     *     represents a new state, preventing re-processing of old events or concurrent conflicts.
     *
     * @param conn  The database connection.
     * @param event The event map containing the data and metadata for the operation.
     * @throws SQLException If a database access error occurs.
     * @throws Exception    For other generic errors.
     */
    @Override
    public void deleteInstanceApiPathPrefix(Connection conn, Map<String, Object> event) throws SQLException, Exception {
        // SQL statement updated to match the idempotent soft-delete pattern.
        final String sql =
                """
                UPDATE instance_api_path_prefix_t
                SET active = FALSE,
                    update_user = ?,
                    update_ts = ?,
                    aggregate_version = ?
                WHERE host_id = ?
                  AND instance_api_id = ?
                  AND path_prefix = ?
                  AND aggregate_version < ?
                """;

        // Extract data and metadata from the event.
        Map<String, Object> map = SqlUtil.extractEventData(event);
        String hostId = (String) event.get(Constants.HOST);
        String instanceApiId = (String) map.get("instanceApiId");
        String pathPrefix = (String) map.get("pathPrefix");
        long newAggregateVersion = SqlUtil.getNewAggregateVersion(event);

        try (PreparedStatement statement = conn.prepareStatement(sql)) {
            // SET clause placeholders (3)
            // 1: update_user from event metadata
            statement.setString(1, (String) event.get(Constants.USER));
            // 2: update_ts from event metadata
            statement.setObject(2, OffsetDateTime.parse((String) event.get(CloudEventV1.TIME)));
            // 3: aggregate_version (the new version)
            statement.setLong(3, newAggregateVersion);

            // WHERE clause placeholders (4)
            // 4: host_id from primary key
            statement.setObject(4, UUID.fromString(hostId));
            // 5: instance_api_id from primary key
            statement.setObject(5, UUID.fromString(instanceApiId));
            // 6: path_prefix from primary key
            statement.setString(6, pathPrefix);
            // 7: aggregate_version < ? for OCC/Idempotency check
            statement.setLong(7, newAggregateVersion);

            int count = statement.executeUpdate();
            if (count == 0) {
                // If 0 rows were updated, we log a warning and continue.
                // This indicates the record was already deleted, never existed, or a newer version is already in the database.
                // This is the expected behavior for an idempotent operation.
                logger.warn("Soft delete skipped for InstanceApiPathPrefix with hostId {}, instanceApiId {}, pathPrefix {}, newAggregateVersion {}. Record not found or a newer version already exists.",
                        hostId, instanceApiId, pathPrefix, newAggregateVersion);
            }
        } catch (SQLException e) {
            logger.error("SQLException during soft delete of InstanceApiPathPrefix for hostId {} instanceApiId {} pathPrefix {}: {}",
                    hostId, instanceApiId, pathPrefix, e.getMessage(), e);
            throw e;
        } catch (Exception e) {
            logger.error("Exception during soft delete of InstanceApiPathPrefix for hostId {} instanceApiId {} pathPrefix {}: {}",
                    hostId, instanceApiId, pathPrefix, e.getMessage(), e);
            throw e;
        }
    }


    @Override
    public Result<String> getInstanceApiPathPrefix(int offset, int limit, String filtersJson, String globalFilter, String sortingJson, String hostId) {
        Result<String> result = null;
        final Map<String, String> columnMap = new HashMap<>(Map.of(
                "hostId", "iapp.host_id",
                "instanceApiId", "iapp.instance_api_id",
                "instanceId", "iai.instance_id",
                "instanceName", "i.instance_name",
                "productId", "pv.product_id",
                "productVersion", "pv.product_version",
                "apiVersionId", "iai.api_version_id",
                "apiId", "av.api_id",
                "apiVersion", "av.api_version",
                "pathPrefix", "iapp.path_prefix"
        ));
        columnMap.put("updateUser", "iapp.update_user");
        columnMap.put("updateTs", "iapp.update_ts");
        columnMap.put("aggregateVersion", "iapp.aggregate_version");
        columnMap.put("active", "iapp.active");

        List<Map<String, Object>> filters = parseJsonList(filtersJson);
        List<Map<String, Object>> sorting = parseJsonList(sortingJson);

        String s =
            """
                SELECT COUNT(*) OVER () AS total,
                iapp.host_id, iapp.instance_api_id, iai.instance_id, i.instance_name,
                pv.product_id, pv.product_version, iai.api_version_id, av.api_id,
                av.api_version, iapp.path_prefix, iapp.update_user, iapp.update_ts,
                iapp.aggregate_version, iapp.active
                FROM instance_api_path_prefix_t iapp
                INNER JOIN instance_api_t iai ON iapp.instance_api_id = iai.instance_api_id
                INNER JOIN instance_t i ON i.instance_id = iai.instance_id
                INNER JOIN product_version_t pv ON pv.product_version_id = i.product_version_id
                INNER JOIN api_version_t av ON av.api_version_id = iai.api_version_id
                INNER JOIN api_t ai ON ai.api_id = av.api_id
                WHERE iapp.host_id = ?
            """;

        List<Object> parameters = new ArrayList<>();
        parameters.add(UUID.fromString(hostId));

        String[] searchColumns = {"i.instance_name", "iapp.path_prefix"};
        String sqlBuilder = s + dynamicFilter(Arrays.asList("iapp.host_id", "iapp.instance_api_id", "iai.instance_id", "iai.api_version_id"), Arrays.asList(searchColumns), filters, columnMap, parameters) +
                globalFilter(globalFilter, searchColumns, parameters) +
                dynamicSorting("iai.instance_id, iapp.instance_api_id, iapp.path_prefix", sorting, columnMap) +
                "\nLIMIT ? OFFSET ?";

        parameters.add(limit);
        parameters.add(offset);
        int total = 0;
        List<Map<String, Object>> instanceApiPathPrefixes = new ArrayList<>();

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
                    map.put("hostId", resultSet.getObject("host_id", UUID.class));
                    map.put("instanceApiId", resultSet.getObject("instance_api_id", UUID.class));
                    map.put("instanceId", resultSet.getObject("instance_id", UUID.class));
                    map.put("instanceName", resultSet.getString("instance_name"));
                    map.put("productId", resultSet.getString("product_id"));
                    map.put("productVersion", resultSet.getString("product_version"));
                    map.put("apiVersionId", resultSet.getObject("api_version_id", UUID.class));
                    map.put("apiId", resultSet.getString("api_id"));
                    map.put("apiVersion", resultSet.getString("api_version"));
                    map.put("pathPrefix", resultSet.getString("path_prefix"));
                    map.put("updateUser", resultSet.getString("update_user"));
                    map.put("updateTs", resultSet.getObject("update_ts") != null ? resultSet.getObject("update_ts", OffsetDateTime.class) : null);
                    map.put("aggregateVersion", resultSet.getLong("aggregate_version"));
                    map.put("active", resultSet.getBoolean("active"));
                    instanceApiPathPrefixes.add(map);
                }
            }

            Map<String, Object> resultMap = new HashMap<>();
            resultMap.put("total", total);
            resultMap.put("instanceApiPathPrefixes", instanceApiPathPrefixes);
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

    /**
     * Creates or reactivates an instance_app_api record using an idempotent UPSERT pattern.
     * This method implements:
     * 1.  **Idempotent Create/Update (Upsert):** Uses `INSERT ... ON CONFLICT DO UPDATE`. This will
     *     create the record on the first valid event, or update and reactivate it if a soft-deleted
     *     record with an older version already exists.
     * 2.  **IDM:** Explicitly sets 'update_user' and 'update_ts' from the event metadata.
     * 3.  **OCC/Monotonicity:** The `WHERE` clause in the `DO UPDATE` part ensures that an update
     *     only occurs if the incoming event's version is strictly greater than the existing record's version.
     *
     * @param conn  The database connection.
     * @param event The event map containing the data and metadata for the operation.
     * @throws SQLException If a database access error occurs.
     * @throws Exception    For other generic errors.
     */
    @Override
    public void createInstanceAppApi(Connection conn, Map<String, Object> event) throws SQLException, Exception {
        final String sql =
                """
                INSERT INTO instance_app_api_t(
                    host_id,
                    instance_app_id,
                    instance_api_id,
                    update_user,
                    update_ts,
                    aggregate_version,
                    active
                ) VALUES (?, ?, ?, ?, ?, ?, TRUE)
                ON CONFLICT (host_id, instance_app_id, instance_api_id) DO UPDATE
                SET update_user = EXCLUDED.update_user,
                    update_ts = EXCLUDED.update_ts,
                    aggregate_version = EXCLUDED.aggregate_version,
                    active = TRUE
                WHERE instance_app_api_t.aggregate_version < EXCLUDED.aggregate_version
                """;

        Map<String, Object> map = SqlUtil.extractEventData(event);
        String hostId = (String) event.get(Constants.HOST);
        String instanceAppId = (String) map.get("instanceAppId");
        String instanceApiId = (String) map.get("instanceApiId");
        long newAggregateVersion = SqlUtil.getNewAggregateVersion(event);

        try (PreparedStatement statement = conn.prepareStatement(sql)) {
            // Set parameters for the INSERT part of the statement
            int i = 1;
            // 1. host_id (PK)
            statement.setObject(i++, UUID.fromString(hostId));
            // 2. instance_app_id (PK)
            statement.setObject(i++, UUID.fromString(instanceAppId));
            // 3. instance_api_id (PK)
            statement.setObject(i++, UUID.fromString(instanceApiId));

            // 4. update_user (IDM)
            statement.setString(i++, (String) event.get(Constants.USER));
            // 5. update_ts (IDM)
            statement.setObject(i++, OffsetDateTime.parse((String) event.get(CloudEventV1.TIME)));
            // 6. aggregate_version (OCC)
            statement.setLong(i++, newAggregateVersion);

            int count = statement.executeUpdate();

            if (count == 0) {
                // A count of 0 is a valid, idempotent outcome. It means the ON CONFLICT clause was triggered,
                // but the WHERE condition (version check) was not met.
                logger.warn("Creation/Reactivation skipped for InstanceAppApi with hostId {}, instanceAppId {}, instanceApiId {}. A newer or same version already exists.",
                        hostId, instanceAppId, instanceApiId);
            }
        } catch (SQLException e) {
            logger.error("SQLException during createInstanceAppApi for hostId {} instanceAppId {} instanceApiId {}: {}",
                    hostId, instanceAppId, instanceApiId, e.getMessage(), e);
            throw e;
        } catch (Exception e) {
            logger.error("Exception during createInstanceAppApi for hostId {} instanceAppId {} instanceApiId {}: {}",
                    hostId, instanceAppId, instanceApiId, e.getMessage(), e);
            throw e;
        }
    }

    /**
     * Updates an instance_app_api record using an idempotent/monotonic pattern.
     * This method implements:
     * 1.  **Soft Delete Principle:** An update implicitly reactivates the record by setting `active = TRUE`.
     * 2.  **IDM:** Explicitly sets 'update_user' and 'update_ts' from the event metadata.
     * 3.  **OCC/Idempotency:** Uses `aggregate_version < ?` to ensure the operation only succeeds if it
     *     represents a new state, preventing re-processing of old events or concurrent conflicts.
     *
     * @param conn  The database connection.
     * @param event The event map containing the data and metadata for the operation.
     * @throws SQLException If a database access error occurs.
     * @throws Exception    For other generic errors.
     */
    @Override
    public void updateInstanceAppApi(Connection conn, Map<String, Object> event) throws SQLException, Exception {
        // SQL statement updated to match the idempotent update pattern.
        final String sql =
                """
                UPDATE instance_app_api_t
                SET update_user = ?,
                    update_ts = ?,
                    aggregate_version = ?,
                    active = TRUE
                WHERE host_id = ?
                  AND instance_app_id = ?
                  AND instance_api_id = ?
                  AND aggregate_version < ?
                """;

        Map<String, Object> map = SqlUtil.extractEventData(event);
        String hostId = (String) event.get(Constants.HOST);
        String instanceAppId = (String) map.get("instanceAppId");
        String instanceApiId = (String) map.get("instanceApiId");
        long newAggregateVersion = SqlUtil.getNewAggregateVersion(event);

        try (PreparedStatement statement = conn.prepareStatement(sql)) {
            // SET clause placeholders (3)
            int i = 1;
            // 1. update_user (IDM)
            statement.setString(i++, (String) event.get(Constants.USER));
            // 2. update_ts (IDM)
            statement.setObject(i++, OffsetDateTime.parse((String) event.get(CloudEventV1.TIME)));
            // 3. aggregate_version (the new version)
            statement.setLong(i++, newAggregateVersion);

            // WHERE clause placeholders (4)
            // 4. host_id (PK)
            statement.setObject(i++, UUID.fromString(hostId));
            // 5. instance_app_id (PK)
            statement.setObject(i++, UUID.fromString(instanceAppId));
            // 6. instance_api_id (PK)
            statement.setObject(i++, UUID.fromString(instanceApiId));
            // 7. aggregate_version < ? for OCC/Idempotency check
            statement.setLong(i++, newAggregateVersion);

            int count = statement.executeUpdate();

            if (count == 0) {
                // If 0 rows were updated, it's a valid idempotent outcome.
                // It means the record was not found or a newer/same version already exists.
                logger.warn("Update skipped for InstanceAppApi with hostId {}, instanceAppId {}, instanceApiId {}. Record not found or a newer version already exists.",
                        hostId, instanceAppId, instanceApiId);
            }
        } catch (SQLException e) {
            logger.error("SQLException during updateInstanceAppApi for hostId {} instanceAppId {} instanceApiId {}: {}",
                    hostId, instanceAppId, instanceApiId, e.getMessage(), e);
            throw e;
        } catch (Exception e) {
            logger.error("Exception during updateInstanceAppApi for hostId {} instanceAppId {} instanceApiId {}: {}",
                    hostId, instanceAppId, instanceApiId, e.getMessage(), e);
            throw e;
        }
    }


    /**
     * Performs a soft delete on an instance_app_api record using an idempotent/monotonic pattern.
     * This method implements:
     * 1.  **Soft Delete:** Sets the 'active' flag to false.
     * 2.  **IDM:** Explicitly sets 'update_user' and 'update_ts' from the event metadata.
     * 3.  **OCC/Idempotency:** Uses `aggregate_version < ?` to ensure the operation only succeeds if it
     *     represents a new state, preventing re-processing of old events or concurrent conflicts.
     *
     * @param conn  The database connection.
     * @param event The event map containing the data and metadata for the operation.
     * @throws SQLException If a database access error occurs.
     * @throws Exception    For other generic errors.
     */
    @Override
    public void deleteInstanceAppApi(Connection conn, Map<String, Object> event) throws SQLException, Exception {
        // SQL statement updated to match the idempotent soft-delete pattern.
        final String sql =
                """
                UPDATE instance_app_api_t
                SET active = FALSE,
                    update_user = ?,
                    update_ts = ?,
                    aggregate_version = ?
                WHERE host_id = ?
                  AND instance_app_id = ?
                  AND instance_api_id = ?
                  AND aggregate_version < ?
                """;

        // Extract data and metadata from the event.
        Map<String, Object> map = SqlUtil.extractEventData(event);
        String hostId = (String) event.get(Constants.HOST);
        String instanceAppId = (String) map.get("instanceAppId");
        String instanceApiId = (String) map.get("instanceApiId");
        long newAggregateVersion = SqlUtil.getNewAggregateVersion(event);

        try (PreparedStatement statement = conn.prepareStatement(sql)) {
            // SET clause placeholders (3)
            int i = 1;
            // 1: update_user from event metadata
            statement.setString(i++, (String) event.get(Constants.USER));
            // 2: update_ts from event metadata
            statement.setObject(i++, OffsetDateTime.parse((String) event.get(CloudEventV1.TIME)));
            // 3: aggregate_version (the new version)
            statement.setLong(i++, newAggregateVersion);

            // WHERE clause placeholders (4)
            // 4: host_id from primary key
            statement.setObject(i++, UUID.fromString(hostId));
            // 5: instance_app_id from primary key
            statement.setObject(i++, UUID.fromString(instanceAppId));
            // 6: instance_api_id from primary key
            statement.setObject(i++, UUID.fromString(instanceApiId));
            // 7: aggregate_version < ? for OCC/Idempotency check
            statement.setLong(i++, newAggregateVersion);

            int count = statement.executeUpdate();
            if (count == 0) {
                // If 0 rows were updated, we log a warning and continue.
                // This indicates the record was already deleted, never existed, or a newer version is already in the database.
                // This is the expected behavior for an idempotent operation.
                logger.warn("Soft delete skipped for InstanceAppApi with hostId {}, instanceAppId {}, instanceApiId {}, newAggregateVersion {}. Record not found or a newer version already exists.",
                        hostId, instanceAppId, instanceApiId, newAggregateVersion);
            }
        } catch (SQLException e) {
            logger.error("SQLException during soft delete of InstanceAppApi for hostId {} instanceAppId {} instanceApiId {}: {}",
                    hostId, instanceAppId, instanceApiId, e.getMessage(), e);
            throw e;
        } catch (Exception e) {
            logger.error("Exception during soft delete of InstanceAppApi for hostId {} instanceAppId {} instanceApiId {}: {}",
                    hostId, instanceAppId, instanceApiId, e.getMessage(), e);
            throw e;
        }
    }

    @Override
    public Result<String> getInstanceAppApi(int offset, int limit, String filtersJson, String globalFilter, String sortingJson, String hostId) {
        Result<String> result = null;
        final Map<String, String> columnMap = new HashMap<>(Map.of(
                "hostId", "iaa.host_id",
                "instanceAppId", "iaa.instance_app_id",
                "appId", "iap.app_id",
                "appVersion", "iap.app_version",
                "instanceApiId", "iaa.instance_api_id",
                "instanceId", "iai.instance_id",
                "instanceName", "i.instance_name",
                "productId", "pv.product_id",
                "productVersion", "pv.product_version",
                "apiVersionId", "iai.api_version_id"
        ));
        columnMap.put("apiId", "av.api_id");
        columnMap.put("apiVersion", "av.api_version");
        columnMap.put("active", "iaa.active");
        columnMap.put("updateUser", "iaa.update_user");
        columnMap.put("updateTs", "iaa.update_ts");
        columnMap.put("aggregateVersion", "iaa.aggregate_version");

        List<Map<String, Object>> filters = parseJsonList(filtersJson);
        List<Map<String, Object>> sorting = parseJsonList(sortingJson);

        String s =
            """
                SELECT COUNT(*) OVER () AS total,
                iaa.host_id, iaa.instance_app_id, iap.app_id, iap.app_version,
                iaa.instance_api_id, iai.instance_id, i.instance_name, pv.product_id,
                pv.product_version, iai.api_version_id, av.api_id, av.api_version, iaa.active,
                iaa.update_user, iaa.update_ts, iaa.aggregate_version
                FROM instance_app_api_t iaa
                INNER JOIN instance_app_t iap ON iaa.instance_app_id = iap.instance_app_id
                INNER JOIN app_t a ON iap.app_id = a.app_id
                INNER JOIN instance_api_t iai ON iaa.instance_api_id = iai.instance_api_id
                INNER JOIN instance_t i ON i.instance_id = iai.instance_id
                INNER JOIN product_version_t pv ON pv.product_version_id = i.product_version_id
                INNER JOIN api_version_t av ON av.api_version_id = iai.api_version_id
                INNER JOIN api_t ai ON ai.api_id = av.api_id
                WHERE iaa.host_id = ?
            """;

        List<Object> parameters = new ArrayList<>();
        parameters.add(UUID.fromString(hostId));

        String[] searchColumns = {"i.instance_name"};
        String sqlBuilder = s + dynamicFilter(Arrays.asList("iaa.host_id", "iaa.instance_app_id", "iaa.instance_api_id", "iai.instance_id", "iai.api_version_id"), Arrays.asList(searchColumns), filters, columnMap, parameters) +
                globalFilter(globalFilter, searchColumns, parameters) +
                dynamicSorting("i.instance_name, iap.app_id, av.api_id", sorting, columnMap) +
                "\nLIMIT ? OFFSET ?";

        parameters.add(limit);
        parameters.add(offset);

        int total = 0;
        List<Map<String, Object>> instanceApis = new ArrayList<>();

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
                    map.put("hostId", resultSet.getObject("host_id", UUID.class));
                    map.put("instanceAppId", resultSet.getObject("instance_app_id", UUID.class));
                    map.put("instanceApiId", resultSet.getObject("instance_api_id", UUID.class));
                    map.put("instanceId", resultSet.getObject("instance_id", UUID.class));
                    map.put("instanceName", resultSet.getString("instance_name"));
                    map.put("productId", resultSet.getString("product_id"));
                    map.put("productVersion", resultSet.getString("product_version"));
                    map.put("appId", resultSet.getString("app_id"));
                    map.put("appVersion", resultSet.getString("app_version"));
                    map.put("apiVersionId", resultSet.getObject("api_version_id", UUID.class));
                    map.put("apiId", resultSet.getString("api_id"));
                    map.put("apiVersion", resultSet.getString("api_version"));
                    map.put("active", resultSet.getBoolean("active"));
                    map.put("updateUser", resultSet.getString("update_user"));
                    map.put("updateTs", resultSet.getObject("update_ts") != null ? resultSet.getObject("update_ts", OffsetDateTime.class) : null);
                    map.put("aggregateVersion", resultSet.getLong("aggregate_version"));
                    instanceApis.add(map);
                }
            }

            Map<String, Object> resultMap = new HashMap<>();
            resultMap.put("total", total);
            resultMap.put("instanceAppApis", instanceApis);
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

    /**
     * Creates or reactivates an instance_app_t record using an idempotent UPSERT pattern.
     * This method implements:
     * 1.  **Idempotent Create/Update (Upsert):** Uses `INSERT ... ON CONFLICT DO UPDATE`. This will
     *     create the record on the first valid event, or update and reactivate it if a soft-deleted
     *     record with an older version already exists.
     * 2.  **IDM:** Explicitly sets 'update_user' and 'update_ts' from the event metadata.
     * 3.  **OCC/Monotonicity:** The `WHERE` clause in the `DO UPDATE` part ensures that an update
     *     only occurs if the incoming event's version is strictly greater than the existing record's version.
     *
     * @param conn  The database connection.
     * @param event The event map containing the data and metadata for the operation.
     * @throws SQLException If a database access error occurs.
     * @throws Exception    For other generic errors.
     */
    @Override
    public void createInstanceApp(Connection conn, Map<String, Object> event) throws SQLException, Exception {
        final String sql =
                """
                INSERT INTO instance_app_t(
                    host_id,
                    instance_app_id,
                    instance_id,
                    app_id,
                    app_version,
                    update_user,
                    update_ts,
                    aggregate_version,
                    active
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, TRUE)
                ON CONFLICT (host_id, instance_app_id) DO UPDATE
                SET instance_id = EXCLUDED.instance_id,
                    app_id = EXCLUDED.app_id,
                    app_version = EXCLUDED.app_version,
                    update_user = EXCLUDED.update_user,
                    update_ts = EXCLUDED.update_ts,
                    aggregate_version = EXCLUDED.aggregate_version,
                    active = TRUE
                WHERE instance_app_t.aggregate_version < EXCLUDED.aggregate_version
                """;

        Map<String, Object> map = SqlUtil.extractEventData(event);
        String hostId = (String) event.get(Constants.HOST);
        String instanceAppId = (String) map.get("instanceAppId");
        long newAggregateVersion = SqlUtil.getNewAggregateVersion(event);

        try (PreparedStatement statement = conn.prepareStatement(sql)) {
            // Set parameters for the INSERT part of the statement
            int i = 1;
            // 1. host_id (PK)
            statement.setObject(i++, UUID.fromString(hostId));
            // 2. instance_app_id (PK)
            statement.setObject(i++, UUID.fromString(instanceAppId));

            // 3. instance_id
            statement.setObject(i++, UUID.fromString((String) map.get("instanceId")));
            // 4. app_id
            statement.setString(i++, (String) map.get("appId"));
            // 5. app_version
            statement.setString(i++, (String) map.get("appVersion"));

            // 6. update_user (IDM)
            statement.setString(i++, (String) event.get(Constants.USER));
            // 7. update_ts (IDM)
            statement.setObject(i++, OffsetDateTime.parse((String) event.get(CloudEventV1.TIME)));
            // 8. aggregate_version (OCC)
            statement.setLong(i++, newAggregateVersion);

            int count = statement.executeUpdate();

            if (count == 0) {
                // A count of 0 is a valid, idempotent outcome. It means the ON CONFLICT clause was triggered,
                // but the WHERE condition (version check) was not met.
                logger.warn("Creation/Reactivation skipped for InstanceApp with hostId {}, instanceAppId {}. A newer or same version already exists.",
                        hostId, instanceAppId);
            }
        } catch (SQLException e) {
            logger.error("SQLException during createInstanceApp for hostId {} instanceAppId {}: {}",
                    hostId, instanceAppId, e.getMessage(), e);
            throw e;
        } catch (Exception e) {
            logger.error("Exception during createInstanceApp for hostId {} instanceAppId {}: {}",
                    hostId, instanceAppId, e.getMessage(), e);
            throw e;
        }
    }

    private boolean queryInstanceAppExists(Connection conn, String hostId, String instanceAppId) throws SQLException {
        final String sql =
                """
                SELECT COUNT(*) FROM instance_app_t WHERE host_id = ? AND instance_app_id = ?
                """;
        try (PreparedStatement pst = conn.prepareStatement(sql)) {
            pst.setObject(1, UUID.fromString(hostId));
            pst.setObject(2, UUID.fromString(instanceAppId));
            try (ResultSet rs = pst.executeQuery()) {
                return rs.next() && rs.getInt(1) > 0;
            }
        }
    }

    /**
     * Updates an instance_app_t record using an idempotent/monotonic pattern.
     * This method implements:
     * 1.  **Soft Delete Principle:** An update implicitly reactivates the record by setting `active = TRUE`.
     * 2.  **IDM:** Explicitly sets 'update_user' and 'update_ts' from the event metadata.
     * 3.  **OCC/Idempotency:** Uses `aggregate_version < ?` to ensure the operation only succeeds if it
     *     represents a new state, preventing re-processing of old events or concurrent conflicts.
     *
     * @param conn  The database connection.
     * @param event The event map containing the data and metadata for the operation.
     * @throws SQLException If a database access error occurs.
     * @throws Exception    For other generic errors.
     */
    @Override
    public void updateInstanceApp(Connection conn, Map<String, Object> event) throws SQLException, Exception {
        // SQL statement updated to match the idempotent update pattern.
        // Includes mutable fields from the event data in the SET clause.
        final String sql =
                """
                UPDATE instance_app_t
                SET instance_id = ?,
                    app_id = ?,
                    app_version = ?,
                    update_user = ?,
                    update_ts = ?,
                    aggregate_version = ?,
                    active = TRUE
                WHERE host_id = ?
                  AND instance_app_id = ?
                  AND aggregate_version < ?
                """;

        Map<String, Object> map = SqlUtil.extractEventData(event);
        String hostId = (String) event.get(Constants.HOST);
        String instanceAppId = (String) map.get("instanceAppId");
        long newAggregateVersion = SqlUtil.getNewAggregateVersion(event);

        try (PreparedStatement statement = conn.prepareStatement(sql)) {
            // SET clause placeholders (6)
            int i = 1;
            // 1. instance_id
            statement.setObject(i++, UUID.fromString((String) map.get("instanceId")));
            // 2. app_id
            statement.setString(i++, (String) map.get("appId"));
            // 3. app_version
            statement.setString(i++, (String) map.get("appVersion"));
            // 4. update_user (IDM)
            statement.setString(i++, (String) event.get(Constants.USER));
            // 5. update_ts (IDM)
            statement.setObject(i++, OffsetDateTime.parse((String) event.get(CloudEventV1.TIME)));
            // 6. aggregate_version (the new version)
            statement.setLong(i++, newAggregateVersion);

            // WHERE clause placeholders (3)
            // 7. host_id (PK)
            statement.setObject(i++, UUID.fromString(hostId));
            // 8. instance_app_id (PK)
            statement.setObject(i++, UUID.fromString(instanceAppId));
            // 9. aggregate_version < ? for OCC/Idempotency check
            statement.setLong(i++, newAggregateVersion);

            int count = statement.executeUpdate();

            if (count == 0) {
                // If 0 rows were updated, it's a valid idempotent outcome.
                // It means the record was not found or a newer/same version already exists.
                logger.warn("Update skipped for InstanceApp with hostId {}, instanceAppId {}. Record not found or a newer version already exists.",
                        hostId, instanceAppId);
            }
        } catch (SQLException e) {
            logger.error("SQLException during updateInstanceApp for hostId {} instanceAppId {}: {}",
                    hostId, instanceAppId, e.getMessage(), e);
            throw e;
        } catch (Exception e) {
            logger.error("Exception during updateInstanceApp for hostId {} instanceAppId {}: {}",
                    hostId, instanceAppId, e.getMessage(), e);
            throw e;
        }
    }

    /**
     * Performs a soft delete on an instance_app_t record using an idempotent/monotonic pattern.
     * This method implements:
     * 1.  **Soft Delete:** Sets the 'active' flag to false.
     * 2.  **IDM:** Explicitly sets 'update_user' and 'update_ts' from the event metadata.
     * 3.  **OCC/Idempotency:** Uses `aggregate_version < ?` to ensure the operation only succeeds if it
     *     represents a new state, preventing re-processing of old events or concurrent conflicts.
     *
     * @param conn  The database connection.
     * @param event The event map containing the data and metadata for the operation.
     * @throws SQLException If a database access error occurs.
     * @throws Exception    For other generic errors.
     */
    @Override
    public void deleteInstanceApp(Connection conn, Map<String, Object> event) throws SQLException, Exception {
        // SQL statement updated to match the idempotent soft-delete pattern.
        final String sql =
                """
                UPDATE instance_app_t
                SET active = FALSE,
                    update_user = ?,
                    update_ts = ?,
                    aggregate_version = ?
                WHERE host_id = ?
                  AND instance_app_id = ?
                  AND aggregate_version < ?
                """;

        // Extract data and metadata from the event.
        Map<String, Object> map = SqlUtil.extractEventData(event);
        String hostId = (String) event.get(Constants.HOST);
        String instanceAppId = (String) map.get("instanceAppId");
        long newAggregateVersion = SqlUtil.getNewAggregateVersion(event);

        try (PreparedStatement statement = conn.prepareStatement(sql)) {
            // SET clause placeholders (3)
            int i = 1;
            // 1: update_user from event metadata
            statement.setString(i++, (String) event.get(Constants.USER));
            // 2: update_ts from event metadata
            statement.setObject(i++, OffsetDateTime.parse((String) event.get(CloudEventV1.TIME)));
            // 3: aggregate_version (the new version)
            statement.setLong(i++, newAggregateVersion);

            // WHERE clause placeholders (3)
            // 4: host_id from primary key
            statement.setObject(i++, UUID.fromString(hostId));
            // 5: instance_app_id from primary key
            statement.setObject(i++, UUID.fromString(instanceAppId));
            // 6: aggregate_version < ? for OCC/Idempotency check
            statement.setLong(i++, newAggregateVersion);

            int count = statement.executeUpdate();
            if (count == 0) {
                // If 0 rows were updated, we log a warning and continue.
                // This indicates the record was already deleted, never existed, or a newer version is already in the database.
                // This is the expected behavior for an idempotent operation.
                logger.warn("Soft delete skipped for InstanceApp with hostId {}, instanceAppId {}, newAggregateVersion {}. Record not found or a newer version already exists.",
                        hostId, instanceAppId, newAggregateVersion);
            }
        } catch (SQLException e) {
            logger.error("SQLException during soft delete of InstanceApp for hostId {} instanceAppId {}: {}",
                    hostId, instanceAppId, e.getMessage(), e);
            throw e;
        } catch (Exception e) {
            logger.error("Exception during soft delete of InstanceApp for hostId {} instanceAppId {}: {}",
                    hostId, instanceAppId, e.getMessage(), e);
            throw e;
        }
    }

    @Override
    public Result<String> getInstanceApp(int offset, int limit, String filtersJson, String globalFilter, String sortingJson, String hostId) {
        Result<String> result = null;
        final Map<String, String> columnMap = new HashMap<>(Map.of(
                "hostId", "ia.host_id",
                "instanceAppId", "ia.instance_app_id",
                "instanceId", "ia.instance_id",
                "instanceName", "i.instance_name",
                "productId", "pv.product_id",
                "productVersion", "pv.product_version",
                "appId", "ia.app_id",
                "appVersion", "ia.app_version",
                "active", "ia.active",
                "updateUser", "ia.update_user"
        ));
        columnMap.put("updateTs", "ia.update_ts");
        columnMap.put("aggregateVersion", "ia.aggregate_version");

        List<Map<String, Object>> filters = parseJsonList(filtersJson);
        List<Map<String, Object>> sorting = parseJsonList(sortingJson);

        String s =
            """
                SELECT COUNT(*) OVER () AS total,
                ia.host_id, ia.instance_app_id, ia.instance_id, i.instance_name, pv.product_version_id,
                pv.product_id, pv.product_version, ia.app_id, ia.app_version, ia.active, ia.update_user,
                ia.update_ts, ia.aggregate_version
                FROM instance_app_t ia
                INNER JOIN instance_t i ON ia.instance_id = i.instance_id
                INNER JOIN product_version_t pv ON i.product_version_id = pv.product_version_id
                WHERE ia.host_id = ?
            """;

        List<Object> parameters = new ArrayList<>();
        parameters.add(UUID.fromString(hostId));

        String[] searchColumns = {"i.instance_name"};
        String sqlBuilder = s + dynamicFilter(Arrays.asList("ia.host_id", "ia.instance_app_id", "ia.instance_id", "pv.product_version_id"), Arrays.asList(searchColumns), filters, columnMap, parameters) +
                globalFilter(globalFilter, searchColumns, parameters) +
                dynamicSorting("instance_id, app_id, app_version", sorting, columnMap) +
                "\nLIMIT ? OFFSET ?";

        parameters.add(limit);
        parameters.add(offset);

        int total = 0;
        List<Map<String, Object>> instanceApps = new ArrayList<>();

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
                    map.put("hostId", resultSet.getObject("host_id", UUID.class));
                    map.put("instanceAppId", resultSet.getObject("instance_app_id", UUID.class));
                    map.put("instanceId", resultSet.getObject("instance_id", UUID.class));
                    map.put("instanceName", resultSet.getString("instance_name"));
                    map.put("productVersionId", resultSet.getObject("productVersionId", UUID.class));
                    map.put("productId", resultSet.getString("product_id"));
                    map.put("productVersion", resultSet.getString("product_version"));
                    map.put("appId", resultSet.getString("app_id"));
                    map.put("appVersion", resultSet.getString("app_version"));
                    map.put("active", resultSet.getBoolean("active"));
                    map.put("updateUser", resultSet.getString("update_user"));
                    map.put("updateTs", resultSet.getObject("update_ts") != null ? resultSet.getObject("update_ts", OffsetDateTime.class) : null);
                    map.put("aggregateVersion", resultSet.getLong("aggregate_version"));
                    instanceApps.add(map);
                }
            }

            Map<String, Object> resultMap = new HashMap<>();
            resultMap.put("total", total);
            resultMap.put("instanceApps", instanceApps);
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

    public Result<String> getInstanceAppLabel(String hostId, String instanceId) {
        Result<String> result = null;
        String sql =
                """
                        SELECT ia.instance_app_id, i.instance_name, ia.app_id, ia.app_version
                        FROM instance_app_t ia
                        INNER JOIN instance_t i ON i.instance_id = ia.instance_id
                        WHERE ia.host_id = ?
                """;
        if(instanceId != null && !instanceId.isEmpty()) {
            sql += " AND ia.instance_id = ?";
        }
        List<Map<String, Object>> labels = new ArrayList<>();
        try (Connection connection = ds.getConnection();
             PreparedStatement preparedStatement = connection.prepareStatement(sql)) {
            preparedStatement.setObject(1, UUID.fromString(hostId));
            if(instanceId != null && !instanceId.isEmpty()) {
                preparedStatement.setObject(2, UUID.fromString(instanceId));
            }
            try (ResultSet resultSet = preparedStatement.executeQuery()) {
                while (resultSet.next()) {
                    Map<String, Object> map = new HashMap<>();
                    map.put("id", resultSet.getString("instance_app_id"));
                    map.put("label", resultSet.getString("app_id") + "|" + resultSet.getString("app_version") + "|" + resultSet.getString("instance_name"));
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
    public void createProduct(Connection conn, Map<String, Object> event) throws SQLException, Exception {
        // --- 1. PRIMARY UPSERT SQL (Creates or Reactivates the specific product version) ---
        // PRIMARY KEY is assumed to be (host_id, product_version_id)
        final String upsertSql =
                """
                INSERT INTO product_version_t(host_id, product_version_id, product_id, product_version,
                light4j_version, break_code, break_config, release_note, version_desc, release_type, current,
                version_status, update_user, update_ts, aggregate_version, active)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, TRUE)
                ON CONFLICT (host_id, product_version_id) DO UPDATE
                SET product_id = EXCLUDED.product_id,
                    product_version = EXCLUDED.product_version,
                    light4j_version = EXCLUDED.light4j_version,
                    break_code = EXCLUDED.break_code,
                    break_config = EXCLUDED.break_config,
                    release_note = EXCLUDED.release_note,
                    version_desc = EXCLUDED.version_desc,
                    release_type = EXCLUDED.release_type,
                    current = EXCLUDED.current,
                    version_status = EXCLUDED.version_status,
                    update_user = EXCLUDED.update_user,
                    update_ts = EXCLUDED.update_ts,
                    aggregate_version = EXCLUDED.aggregate_version,
                    active = TRUE
                WHERE product_version_t.aggregate_version < EXCLUDED.aggregate_version
                """;

        // --- 2. UPDATE SQL (To set all other versions of the same product to current = false) ---
        // This update needs to be idempotent.
        final String updateOthersSql =
                """
                UPDATE product_version_t SET current = false, update_user = ?, update_ts = ?
                WHERE host_id = ?
                AND product_id = ?
                AND product_version_id != ?
                AND current = TRUE
                """; // <<< CRITICAL: Only set current=false if it is true (for idempotency)

        Map<String, Object> map = SqlUtil.extractEventData(event);
        String hostId = (String)event.get(Constants.HOST);
        String productVersionId = (String)map.get("productVersionId");
        String productId = (String)map.get("productId");
        String productVersion = (String)map.get("productVersion");
        Boolean current = (Boolean)map.getOrDefault("current", false); // Default to false if not provided
        long newAggregateVersion = SqlUtil.getNewAggregateVersion(event);
        String updateTsStr = (String)event.get(CloudEventV1.TIME);
        String updateUser = (String)event.get(Constants.USER);
        OffsetDateTime updateTs = OffsetDateTime.parse(updateTsStr);

        try (PreparedStatement statement = conn.prepareStatement(upsertSql)) {
            int i = 1;

            // --- PRIMARY UPSERT PARAMETERS ---
            statement.setObject(i++, UUID.fromString(hostId)); // 1. host_id
            statement.setObject(i++, UUID.fromString(productVersionId)); // 2. product_version_id (PK)
            statement.setString(i++, productId); // 3. product_id
            statement.setString(i++, productVersion); // 4. product_version
            statement.setString(i++, (String)map.get("light4jVersion")); // 5. light4j_version

            // 6. break_code
            if (map.containsKey("breakCode")) {
                statement.setBoolean(i++, (Boolean) map.get("breakCode"));
            } else {
                statement.setNull(i++, Types.BOOLEAN);
            }

            // 7. break_config
            if (map.containsKey("breakConfig")) {
                statement.setBoolean(i++, (Boolean) map.get("breakConfig"));
            } else {
                statement.setNull(i++, Types.BOOLEAN);
            }

            // 8. release_note
            if (map.containsKey("releaseNote")) {
                statement.setString(i++, (String) map.get("releaseNote"));
            } else {
                statement.setNull(i++, Types.VARCHAR);
            }

            // 9. version_desc
            if (map.containsKey("versionDesc")) {
                statement.setString(i++, (String) map.get("versionDesc"));
            } else {
                statement.setNull(i++, Types.VARCHAR);
            }

            statement.setString(i++, (String)map.get("releaseType")); // 10. release_type
            statement.setBoolean(i++, current); // 11. current (NOTE: This value is in the event)
            statement.setString(i++, (String)map.get("versionStatus")); // 12. version_status
            statement.setString(i++, updateUser); // 13. update_user
            statement.setObject(i++, updateTs); // 14. update_ts
            statement.setLong(i++, newAggregateVersion); // 15. aggregate_version

            int count = statement.executeUpdate();
            if (count == 0) {
                // UPSERT monotonicity failed. Log and ignore.
                logger.warn("Product Version creation/update skipped for productId {} version {}. A newer or same version already exists.", productId, productVersion);
            }

            // --- STATE MANAGEMENT: Set others to current=false if this new one is true ---
            if(current && count > 0) {
                // If the new version is set to current=TRUE, ensure all others are set to FALSE.
                try (PreparedStatement statementUpdate = conn.prepareStatement(updateOthersSql)) {
                    int j = 1;
                    statementUpdate.setString(j++, updateUser);
                    statementUpdate.setObject(j++, updateTs);
                    statementUpdate.setObject(j++, UUID.fromString(hostId));
                    statementUpdate.setString(j++, productId);
                    statementUpdate.setObject(j, UUID.fromString(productVersionId)); // The ID of the currently active version

                    statementUpdate.executeUpdate();
                }
            }

        } catch (SQLException e) {
            logger.error("SQLException during createProduct (version) UPSERT for productId {} version {} aggregateVersion {}: {}", productId, productVersion, newAggregateVersion, e.getMessage(), e);
            throw e;
        } catch (Exception e) {
            logger.error("Exception during createProduct (version) for productId {} version {} aggregateVersion {}: {}", productId, productVersion, newAggregateVersion, e.getMessage(), e);
            throw e;
        }
    }

    @Override
    public void updateProduct(Connection conn, Map<String, Object> event) throws SQLException, Exception {
        // --- 1. PRIMARY UPDATE SQL (Monotonicity Check) ---
        // Changed WHERE aggregate_version = ? to aggregate_version < ?
        final String sql =
                """
                UPDATE product_version_t SET light4j_version = ?, break_code = ?, break_config = ?,
                release_note = ?, version_desc = ?, release_type = ?, current = ?, version_status = ?,
                update_user = ?, update_ts = ?, aggregate_version = ?
                WHERE host_id = ? AND product_version_id = ? AND aggregate_version < ?
                """; // <<< CRITICAL: Changed '= ?' to '< ?' for idempotent update

        // --- 2. SECONDARY UPDATE SQL (Set others to current=false) ---
        // This remains the same, but it should also update update_user/update_ts for consistency.
        final String updateOthersSql = "UPDATE product_version_t SET current = false, update_user = ?, update_ts = ? \n" + // Added update_user/update_ts
                "WHERE host_id = ?\n" +
                "AND product_id = ?\n" +
                "AND product_version_id != ?"; // Use product_version_id for uniqueness

        Map<String, Object> map = (Map<String, Object>)event.get(PortalConstants.DATA);
        String hostId = (String)event.get(Constants.HOST);
        String productId = (String)map.get("productId");
        // String productVersion = (String)map.get("productVersion"); // Unused in new logic
        String productVersionId = (String)map.get("productVersionId");
        Boolean current = (Boolean)map.getOrDefault("current", false); // Use getOrDefault for safety
        long oldAggregateVersion = SqlUtil.getOldAggregateVersion(event);
        long newAggregateVersion = SqlUtil.getNewAggregateVersion(event);

        String updateTsStr = (String)event.get(CloudEventV1.TIME);
        String updateUser = (String)event.get(Constants.USER);
        OffsetDateTime updateTs = OffsetDateTime.parse(updateTsStr);

        try (PreparedStatement statement = conn.prepareStatement(sql)) {
            int i = 1;

            // --- PRIMARY UPDATE PARAMETERS (1 to 14) ---
            statement.setString(i++, (String)map.get("light4jVersion")); // 1. light4j_version

            if (map.containsKey("breakCode")) {
                statement.setBoolean(i++, (Boolean) map.get("breakCode"));
            } else {
                statement.setNull(i++, Types.BOOLEAN);
            }

            if (map.containsKey("breakConfig")) {
                statement.setBoolean(i++, (Boolean) map.get("breakConfig"));
            } else {
                statement.setNull(i++, Types.BOOLEAN);
            }

            if (map.containsKey("releaseNote")) {
                statement.setString(i++, (String) map.get("releaseNote"));
            } else {
                statement.setNull(i++, Types.VARCHAR);
            }

            if (map.containsKey("versionDesc")) {
                statement.setString(i++, (String) map.get("versionDesc"));
            } else {
                statement.setNull(i++, Types.VARCHAR);
            }

            statement.setString(i++, (String)map.get("releaseType")); // 6. release_type
            statement.setBoolean(i++, current); // 7. current
            statement.setString(i++, (String)map.get("versionStatus")); // 8. version_status
            statement.setString(i++, updateUser); // 9. update_user
            statement.setObject(i++, updateTs); // 10. update_ts
            statement.setLong(i++, newAggregateVersion); // 11. aggregate_version (New Version in SET)

            statement.setObject(i++, UUID.fromString(hostId)); // 12. host_id (in WHERE clause)
            statement.setObject(i++, UUID.fromString(productVersionId)); // 13. product_version_id (in WHERE clause)
            statement.setLong(i, newAggregateVersion); // 14. Monotonicity Check: aggregate_version < newAggregateVersion

            int count = statement.executeUpdate();
            if (count == 0) {
                // If 0 rows updated, it means the record was either not found OR aggregate_version >= newAggregateVersion.
                // We IGNORE the failure and log a warning, as this is the desired idempotent/monotonic behavior.
                logger.warn("Product Version update skipped for hostId {} productVersionId {} (new version {}). Record not found or a newer version already exists.", hostId, productVersionId, newAggregateVersion);
            }

            // --- STATE MANAGEMENT: Set others to current=false if this new one is true ---
            if(current && count > 0) {
                // If the new version is set to current=TRUE, ensure all others are set to FALSE.
                try (PreparedStatement statementUpdate = conn.prepareStatement(updateOthersSql)) {
                    int j = 1;
                    statementUpdate.setString(j++, updateUser); // 1. update_user
                    statementUpdate.setObject(j++, updateTs); // 2. update_ts
                    statementUpdate.setObject(j++, UUID.fromString(hostId)); // 3. host_id
                    statementUpdate.setString(j++, productId); // 4. product_id
                    statementUpdate.setObject(j, UUID.fromString(productVersionId)); // 5. product_version_id (the ID of the version being SET to TRUE)

                    statementUpdate.executeUpdate();
                }
            }

        } catch (SQLException e) {
            logger.error("SQLException during updateProduct for hostId {} productVersionId {} (old: {}) -> (new: {}): {}", hostId, productVersionId, oldAggregateVersion, newAggregateVersion, e.getMessage(), e);
            throw e;
        } catch (Exception e) {
            logger.error("Exception during updateProduct for hostId {} productVersionId {} (old: {}) -> (new: {}): {}", hostId, productVersionId, oldAggregateVersion, newAggregateVersion, e.getMessage(), e);
            throw e;
        }
    }

    /**
     * This method should only be called if the current flag is false. The service handler should validte it.
     *
     */
    @Override
    public void deleteProduct(Connection conn, Map<String, Object> event) throws SQLException, Exception {
        // --- UPDATED SQL FOR SOFT DELETE + MONOTONICITY ---
        // Updates the 'active' flag to FALSE and sets the new version IF the current DB version is older than the incoming event's version.
        final String softDeleteProductSql =
                """
                UPDATE product_version_t SET active = false, update_user = ?, update_ts = ?, aggregate_version = ?
                WHERE host_id = ? AND product_version_id = ? AND aggregate_version < ?
                """; // <<< CRITICAL: Changed from DELETE to UPDATE, and used aggregate_version < ?

        Map<String, Object> map = SqlUtil.extractEventData(event); // Assuming extractEventData is the helper to get PortalConstants.DATA
        String hostId = (String)event.get(Constants.HOST); // HostId from CloudEvent extension
        String productVersionId = (String)map.get("productVersionId");

        // productId is not needed for the SQL
        // String productId = (String)map.get("productId");

        // oldAggregateVersion is the version the command handler operated on (for context/logging).
        long oldAggregateVersion = SqlUtil.getOldAggregateVersion(event);
        // newAggregateVersion is the version of the incoming Delete event (the target version).
        long newAggregateVersion = SqlUtil.getNewAggregateVersion(event);

        String updateTsStr = (String)event.get(CloudEventV1.TIME);
        String updateUser = (String)event.get(Constants.USER);
        OffsetDateTime updateTs = OffsetDateTime.parse(updateTsStr);

        try (PreparedStatement statement = conn.prepareStatement(softDeleteProductSql)) {
            int i = 1;

            // 1. update_user
            statement.setString(i++, updateUser);

            // 2. update_ts
            statement.setObject(i++, updateTs);

            // 3. aggregate_version (NEW version in SET clause)
            statement.setLong(i++, newAggregateVersion);

            // 4. host_id (in WHERE clause)
            statement.setObject(i++, UUID.fromString(hostId));

            // 5. product_version_id (in WHERE clause)
            statement.setObject(i++, UUID.fromString(productVersionId));

            // 6. aggregate_version (MONOTONICITY check in WHERE clause)
            statement.setLong(i, newAggregateVersion); // Condition: WHERE aggregate_version < newAggregateVersion

            int count = statement.executeUpdate();
            if (count == 0) {
                // If 0 rows updated, it means the record was either not found OR aggregate_version >= newAggregateVersion.
                // We IGNORE the failure and log a warning, as this is the desired idempotent/monotonic behavior.
                logger.warn("Product Version soft-delete skipped for hostId {} productVersionId {} (new version {}). Record not found or a newer version already exists.", hostId, productVersionId, newAggregateVersion);
            }
            // NO THROW on count == 0. The method is now idempotent.

        } catch (SQLException e) {
            logger.error("SQLException during softDeleteProduct for hostId {} productVersionId {} aggregateVersion {}: {}", hostId, productVersionId, newAggregateVersion, e.getMessage(), e);
            throw e; // Re-throw SQLException
        } catch (Exception e) {
            logger.error("Exception during softDeleteProduct for hostId {} productVersionId {} aggregateVersion {}: {}", hostId, productVersionId, newAggregateVersion, e.getMessage(), e);
            throw e; // Re-throw generic Exception
        }
    }

    @Override
    public Result<String> getProduct(int offset, int limit, String filtersJson, String globalFilter, String  sortingJson, String hostId) {
        List<Map<String, Object>> filters = parseJsonList(filtersJson);
        List<Map<String, Object>> sorting = parseJsonList(sortingJson);

        Result<String> result = null;
        String s = """
                SELECT COUNT(*) OVER () AS total,
                host_id, product_version_id, product_id, product_version,
                light4j_version, break_code, break_config, release_note,
                version_desc, release_type, current, version_status,
                update_user, update_ts, aggregate_version, active
                FROM product_version_t
                WHERE host_id = ?
                """;

        List<Object> parameters = new ArrayList<>();
        parameters.add(UUID.fromString(hostId));

        String[] searchColumns = {"release_note", "version_desc", "org_desc"};
        String sqlBuilder = s + dynamicFilter(Arrays.asList("host_id", "product_version_id"), Arrays.asList(searchColumns), filters, null, parameters) +
                globalFilter(globalFilter, searchColumns, parameters) +
                dynamicSorting("product_id, product_version", sorting, null) +
                "\nLIMIT ? OFFSET ?";

        parameters.add(limit);
        parameters.add(offset);
        int total = 0;
        List<Map<String, Object>> products = new ArrayList<>();

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
                    map.put("hostId", resultSet.getObject("host_id", UUID.class));
                    map.put("productVersionId", resultSet.getObject("product_version_id", UUID.class));
                    map.put("productId", resultSet.getString("product_id"));
                    map.put("productVersion", resultSet.getString("product_version"));
                    map.put("light4jVersion", resultSet.getString("light4j_version"));
                    map.put("breakCode", resultSet.getBoolean("break_code"));
                    map.put("breakConfig", resultSet.getBoolean("break_config"));
                    map.put("releaseNote", resultSet.getString("release_note"));
                    map.put("versionDesc", resultSet.getString("version_desc"));
                    map.put("releaseType", resultSet.getString("release_type"));
                    map.put("current", resultSet.getBoolean("current"));
                    map.put("versionStatus", resultSet.getString("version_status"));
                    map.put("updateUser", resultSet.getString("update_user"));
                    // handling date properly
                    map.put("updateTs", resultSet.getObject("update_ts") != null ? resultSet.getObject("update_ts", OffsetDateTime.class) : null);
                    map.put("aggregateVersion", resultSet.getLong("aggregate_version"));
                    map.put("active", resultSet.getBoolean("active"));
                    products.add(map);
                }
            }

            Map<String, Object> resultMap = new HashMap<>();
            resultMap.put("total", total);
            resultMap.put("products", products);
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
    public Result<String> getProductVersion(String hostId, String productVersionId) {
        Result<String> result;
        String sql =
                """
                SELECT host_id, product_version_id, product_id, product_version,
                light4j_version, break_code, break_config, release_note,
                version_desc, release_type, current, version_status,
                update_user, update_ts, aggregate_version, active
                FROM product_version_t
                WHERE host_id = ? AND product_version_id = ?
                """;
        try (final Connection conn = ds.getConnection()) {
            Map<String, Object> map = new HashMap<>();
            try (PreparedStatement statement = conn.prepareStatement(sql)) {
                statement.setObject(1, UUID.fromString(hostId));
                statement.setObject(2, UUID.fromString(productVersionId));
                try (ResultSet resultSet = statement.executeQuery()) {
                    if (resultSet.next()) {
                        map.put("hostId", resultSet.getObject("host_id", UUID.class));
                        map.put("productVersionId", resultSet.getObject("product_version_id", UUID.class));
                        map.put("productId", resultSet.getString("product_id"));
                        map.put("productVersion", resultSet.getString("product_version"));
                        map.put("light4jVersion", resultSet.getString("light4j_version"));
                        map.put("breakCode", resultSet.getBoolean("break_code"));
                        map.put("breakConfig", resultSet.getBoolean("break_config"));
                        map.put("releaseNote", resultSet.getString("release_note"));
                        map.put("versionDesc", resultSet.getString("version_desc"));
                        map.put("releaseType", resultSet.getString("release_type"));
                        map.put("current", resultSet.getBoolean("current"));
                        map.put("versionStatus", resultSet.getString("version_status"));
                        map.put("updateUser", resultSet.getString("update_user"));
                        map.put("updateTs", resultSet.getObject("update_ts") != null ? resultSet.getObject("update_ts", OffsetDateTime.class) : null);
                        map.put("aggregateVersion", resultSet.getLong("aggregate_version"));
                        map.put("active", resultSet.getBoolean("active"));
                    }
                }
            }
            if (map.isEmpty())
                result = Failure.of(new Status(OBJECT_NOT_FOUND, "getProductVersion", productVersionId));
            else
                result = Success.of(JsonMapper.toJson(map));
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
    public Result<String> getProductIdLabel(String hostId) {
        Result<String> result = null;
        String sql = "SELECT DISTINCT product_id FROM product_version_t WHERE host_id = ?";
        List<Map<String, Object>> labels = new ArrayList<>();
        try (Connection connection = ds.getConnection();
             PreparedStatement preparedStatement = connection.prepareStatement(sql)) {
            preparedStatement.setObject(1, UUID.fromString(hostId));
            try (ResultSet resultSet = preparedStatement.executeQuery()) {
                while (resultSet.next()) {
                    Map<String, Object> map = new HashMap<>();
                    map.put("id", resultSet.getString("product_id"));
                    map.put("label", resultSet.getString("product_id"));
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
    public Result<String> getProductVersionLabel(String hostId, String productId) {
        Result<String> result = null;
        String sql = "SELECT product_version FROM product_version_t WHERE host_id = ? AND product_id = ?";
        List<Map<String, Object>> versions = new ArrayList<>();
        try (Connection connection = ds.getConnection();
             PreparedStatement preparedStatement = connection.prepareStatement(sql)) {
            preparedStatement.setObject(1, UUID.fromString(hostId));
            preparedStatement.setString(2, productId);
            try (ResultSet resultSet = preparedStatement.executeQuery()) {
                while (resultSet.next()) {
                    Map<String, Object> map = new HashMap<>();
                    String id = resultSet.getString("product_version");
                    map.put("id", id);
                    map.put("label", id);
                    versions.add(map);
                }
            }
            result = Success.of(JsonMapper.toJson(versions));
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
    public Result<String> getProductVersionIdLabel(String hostId) {
        Result<String> result = null;
        String sql = "SELECT DISTINCT product_version_id, product_id, product_version FROM product_version_t WHERE host_id = ?";
        List<Map<String, Object>> labels = new ArrayList<>();
        try (Connection connection = ds.getConnection();
             PreparedStatement preparedStatement = connection.prepareStatement(sql)) {
            preparedStatement.setObject(1, UUID.fromString(hostId));
            try (ResultSet resultSet = preparedStatement.executeQuery()) {
                while (resultSet.next()) {
                    Map<String, Object> map = new HashMap<>();
                    map.put("id", resultSet.getString("product_version_id"));
                    map.put("label", resultSet.getString("product_id") + "|" + resultSet.getString("product_version"));
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
    public String getProductVersionId(String hostId, String productId, String productVersion) {
        final String sql = "SELECT product_version_id FROM product_version_t WHERE host_id = ? AND product_id = ? AND product_version = ?";
        String productVersionId = null;
        try (Connection connection = ds.getConnection();
             PreparedStatement statement = connection.prepareStatement(sql)) {
            statement.setObject(1, UUID.fromString(hostId));
            statement.setString(2, productId);
            statement.setString(3, productVersion);
            try (ResultSet resultSet = statement.executeQuery()) {
                if(resultSet.next()){
                    productVersionId = resultSet.getString(1);
                }
            }
        } catch (SQLException e) {
            logger.error("SQLException:", e);
        } catch (Exception e) {
            logger.error("Exception:", e);
        }
        return productVersionId;
    }

    @Override
    public String queryProductVersionId(String hostId, String productId, String light4jVersion) {
        final String sql = "SELECT product_version_id FROM product_version_t WHERE host_id = ? AND product_id = ? AND light4j_version = ?";
        String productVersionId = null;
        try (Connection connection = ds.getConnection();
             PreparedStatement statement = connection.prepareStatement(sql)) {
            statement.setObject(1, UUID.fromString(hostId));
            statement.setString(2, productId);
            statement.setString(3, light4jVersion);
            try (ResultSet resultSet = statement.executeQuery()) {
                if(resultSet.next()){
                    productVersionId = resultSet.getString(1);
                }
            }
        } catch (SQLException e) {
            logger.error("SQLException:", e);
        } catch (Exception e) {
            logger.error("Exception:", e);
        }
        return productVersionId;
    }

    @Override
    public void createProductVersionEnvironment(Connection conn, Map<String, Object> event) throws SQLException, Exception {
        // --- UPDATED SQL FOR UPSERT ---
        // Assuming the Unique Constraint is (host_id, product_version_id, system_env, runtime_env)
        final String upsertSql =
                """
                INSERT INTO product_version_environment_t(host_id, product_version_id,
                system_env, runtime_env, update_user, update_ts, aggregate_version, active)
                VALUES (?, ?, ?, ?, ?, ?, ?, TRUE)
                ON CONFLICT (host_id, product_version_id, system_env, runtime_env) DO UPDATE
                SET update_user = EXCLUDED.update_user,
                    update_ts = EXCLUDED.update_ts,
                    aggregate_version = EXCLUDED.aggregate_version,
                    active = TRUE
                WHERE product_version_environment_t.aggregate_version < EXCLUDED.aggregate_version
                """; // <<< CRITICAL: ON CONFLICT on the composite key and monotonicity check

        Map<String, Object> map = SqlUtil.extractEventData(event);
        String hostId = (String)event.get(Constants.HOST);
        String productVersionId = (String)map.get("productVersionId");
        String systemEnv = (String)map.get("systemEnv");
        String runtimeEnv = (String)map.get("runtimeEnv");
        long newAggregateVersion = SqlUtil.getNewAggregateVersion(event);

        try (PreparedStatement statement = conn.prepareStatement(upsertSql)) {
            int i = 1;
            // --- SET VALUES (1 to 7) ---
            statement.setObject(i++, UUID.fromString(hostId)); // 1. host_id (Part of PK)
            statement.setObject(i++, UUID.fromString(productVersionId)); // 2. product_version_id (Part of PK)
            statement.setString(i++, systemEnv); // 3. system_env (Part of PK)
            statement.setString(i++, runtimeEnv); // 4. runtime_env (Part of PK)
            statement.setString(i++, (String)event.get(Constants.USER)); // 5. update_user
            statement.setObject(i++, OffsetDateTime.parse((String)event.get(CloudEventV1.TIME))); // 6. update_ts
            statement.setLong(i++, newAggregateVersion); // 7. aggregate_version (New Version)

            // --- Execute UPSERT ---
            int count = statement.executeUpdate();
            if (count == 0) {
                // count=0 means the ON CONFLICT clause was hit, BUT the WHERE clause failed (version was newer or same).
                logger.warn("Environment creation/reactivation skipped for hostId {} productVersionId {}. A newer or same version already exists in the projection.", hostId, productVersionId);
            } else {
                logger.info("Product Version Environment successfully inserted or updated to aggregateVersion {}.", newAggregateVersion);
            }

        } catch (SQLException e) {
            logger.error("SQLException during UPSERT for hostId {} productVersionId {} systemEnv {} runtimeEnv {} aggregateVersion {}: {}", hostId, productVersionId, systemEnv, runtimeEnv, newAggregateVersion, e.getMessage(), e);
            throw e; // Re-throw SQLException
        } catch (Exception e) {
            logger.error("Exception during UPSERT for hostId {} productVersionId {} systemEnv {} runtimeEnv {} aggregateVersion {}: {}", hostId, productVersionId, systemEnv, runtimeEnv, newAggregateVersion, e.getMessage(), e);
            throw e; // Re-throw generic Exception
        }
    }

    @Override
    public void deleteProductVersionEnvironment(Connection conn, Map<String, Object> event) throws SQLException, Exception {
        // --- UPDATED SQL FOR SOFT DELETE + MONOTONICITY ---
        // Updates the 'active' flag to FALSE and sets the new version IF the current DB version is older than the incoming event's version.
        final String softDeleteSql =
                """
                UPDATE product_version_environment_t SET active = false, update_user = ?, update_ts = ?, aggregate_version = ?
                WHERE host_id = ? AND product_version_id = ? AND system_env = ? AND runtime_env = ? AND aggregate_version < ?
                """; // <<< CRITICAL: Changed from DELETE to UPDATE, and used aggregate_version < ?

        Map<String, Object> map = SqlUtil.extractEventData(event);
        String hostId = (String)event.get(Constants.HOST);
        String productVersionId = (String)map.get("productVersionId");
        String systemEnv = (String)map.get("systemEnv");
        String runtimeEnv = (String)map.get("runtimeEnv");

        long newAggregateVersion = SqlUtil.getNewAggregateVersion(event);

        String updateTsStr = (String)event.get(CloudEventV1.TIME);
        String updateUser = (String)event.get(Constants.USER);
        OffsetDateTime updateTs = OffsetDateTime.parse(updateTsStr);

        try (PreparedStatement statement = conn.prepareStatement(softDeleteSql)) {
            int i = 1;

            // 1. update_user
            statement.setString(i++, updateUser);

            // 2. update_ts
            statement.setObject(i++, updateTs);

            // 3. aggregate_version (NEW version in SET clause)
            statement.setLong(i++, newAggregateVersion);

            // 4. host_id (in WHERE clause)
            statement.setObject(i++, UUID.fromString(hostId));

            // 5. product_version_id (in WHERE clause)
            statement.setObject(i++, UUID.fromString(productVersionId));

            // 6. system_env (in WHERE clause)
            statement.setString(i++, systemEnv);

            // 7. runtime_env (in WHERE clause)
            statement.setString(i++, runtimeEnv);

            // 8. aggregate_version (MONOTONICITY check in WHERE clause)
            statement.setLong(i, newAggregateVersion); // Condition: WHERE aggregate_version < newAggregateVersion

            int count = statement.executeUpdate();
            if (count == 0) {
                // If 0 rows updated, it means the record was either not found OR aggregate_version >= newAggregateVersion.
                // We IGNORE the failure and log a warning, as this is the desired idempotent/monotonic behavior.
                logger.warn("Environment soft-delete skipped for hostId {} productVersionId {} (new version {}). Record not found or a newer version already exists.", hostId, productVersionId, newAggregateVersion);
            }
            // NO THROW on count == 0. The method is now idempotent.
        } catch (SQLException e) {
            logger.error("SQLException during softDeleteEnvironment for hostId {} productVersionId {} aggregateVersion {}: {}", hostId, productVersionId, newAggregateVersion, e.getMessage(), e);
            throw e; // Re-throw SQLException
        } catch (Exception e) {
            logger.error("Exception during softDeleteEnvironment for hostId {} productVersionId {} aggregateVersion {}: {}", hostId, productVersionId, newAggregateVersion, e.getMessage(), e);
            throw e; // Re-throw generic Exception
        }
    }


    @Override
    public Result<String> getProductVersionEnvironment(int offset, int limit, String filtersJson, String globalFilter, String sortingJson, String hostId) {
        Result<String> result = null;
        final Map<String, String> columnMap = new HashMap<>(Map.of(
                "hostId", "pve.host_id",
                "productVersionId", "pve.product_version_id",
                "productId", "pv.product_id",
                "productVersion", "pv.product_version",
                "systemEnv", "pve.system_env",
                "runtimeEnv", "pve.runtime_env",
                "updateUser", "pve.update_user",
                "updateTs", "pve.update_ts",
                "aggregateVersion", "pve.aggregate_version",
                "active", "pve.active"
        ));

        List<Map<String, Object>> filters = parseJsonList(filtersJson);
        List<Map<String, Object>> sorting = parseJsonList(sortingJson);

        String s =
                """
                SELECT COUNT(*) OVER () AS total,
                pve.host_id, pve.product_version_id, pv.product_id, pv.product_version,
                pve.system_env, pve.runtime_env, pve.update_user,
                pve.update_ts, pve.aggregate_version, pve.active
                FROM product_version_environment_t pve
                INNER JOIN product_version_t pv ON pv.product_version_id = pve.product_version_id
                WHERE pve.host_id = ?
                """;

        List<Object> parameters = new ArrayList<>();
        parameters.add(UUID.fromString(hostId));

        String[] searchColumns = {"pve.system_env", "pve.runtime_env"};
        String sqlBuilder = s + dynamicFilter(Arrays.asList("pve.host_id", "pve.product_version_id"), Arrays.asList(searchColumns), filters, columnMap, parameters) +
                globalFilter(globalFilter, searchColumns, parameters) +
                dynamicSorting("pv.product_id, pv.product_version, pve.system_env, pve.runtime_env", sorting, columnMap) +
                "\nLIMIT ? OFFSET ?";

        parameters.add(limit);
        parameters.add(offset);

        int total = 0;
        List<Map<String, Object>> productEnvironments = new ArrayList<>();

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
                    map.put("hostId", resultSet.getObject("host_id", UUID.class));
                    map.put("productVersionId", resultSet.getObject("product_version_id", UUID.class));
                    map.put("productId", resultSet.getString("product_id"));
                    map.put("productVersion", resultSet.getString("product_version"));
                    map.put("systemEnv", resultSet.getString("system_env"));
                    map.put("runtimeEnv", resultSet.getString("runtime_env"));
                    map.put("updateUser", resultSet.getString("update_user"));
                    map.put("updateTs", resultSet.getObject("update_ts") != null ? resultSet.getObject("update_ts", OffsetDateTime.class) : null);
                    map.put("aggregateVersion", resultSet.getLong("aggregate_version"));
                    map.put("active", resultSet.getBoolean("active"));
                    productEnvironments.add(map);
                }
            }
            Map<String, Object> resultMap = new HashMap<>();
            resultMap.put("total", total);
            resultMap.put("productEnvironments", productEnvironments);
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
    public void createProductVersionPipeline(Connection conn, Map<String, Object> event) throws SQLException, Exception {
        // --- UPDATED SQL FOR UPSERT ---
        // Assuming the Unique Constraint is (host_id, product_version_id, pipeline_id)
        final String upsertSql =
                """
                INSERT INTO product_version_pipeline_t(host_id, product_version_id,
                pipeline_id, update_user, update_ts, aggregate_version, active)
                VALUES (?, ?, ?, ?, ?, ?, TRUE)
                ON CONFLICT (host_id, product_version_id, pipeline_id) DO UPDATE
                SET update_user = EXCLUDED.update_user,
                    update_ts = EXCLUDED.update_ts,
                    aggregate_version = EXCLUDED.aggregate_version,
                    active = TRUE
                WHERE product_version_pipeline_t.aggregate_version < EXCLUDED.aggregate_version
                """; // <<< CRITICAL: ON CONFLICT on the composite key and monotonicity check

        Map<String, Object> map = SqlUtil.extractEventData(event);
        String hostId = (String)event.get(Constants.HOST);
        String productVersionId = (String)map.get("productVersionId");
        String pipelineId = (String)map.get("pipelineId");
        long newAggregateVersion = SqlUtil.getNewAggregateVersion(event);
        String updateUser = (String)event.get(Constants.USER);
        OffsetDateTime updateTs = OffsetDateTime.parse((String)event.get(CloudEventV1.TIME));

        try (PreparedStatement statement = conn.prepareStatement(upsertSql)) {
            int i = 1;
            // --- SET VALUES (1 to 6) ---
            statement.setObject(i++, UUID.fromString(hostId)); // 1. host_id (Part of PK)
            statement.setObject(i++, UUID.fromString(productVersionId)); // 2. product_version_id (Part of PK)
            statement.setObject(i++, UUID.fromString(pipelineId)); // 3. pipeline_id (Part of PK)
            statement.setString(i++, updateUser); // 4. update_user
            statement.setObject(i++, updateTs); // 5. update_ts
            statement.setLong(i++, newAggregateVersion); // 6. aggregate_version (New Version)

            // --- Execute UPSERT ---
            int count = statement.executeUpdate();
            if (count == 0) {
                // count=0 means the ON CONFLICT clause was hit, BUT the WHERE clause failed (version was newer or same).
                logger.warn("Pipeline creation/reactivation skipped for hostId {} productVersionId {} pipelineId {}. A newer or same version already exists in the projection.", hostId, productVersionId, pipelineId);
            } else {
                logger.info("Product Version Pipeline successfully inserted or updated to aggregateVersion {}.", newAggregateVersion);
            }

        } catch (SQLException e) {
            logger.error("SQLException during UPSERT for hostId {} productVersionId {} pipelineId {} aggregateVersion {}: {}", hostId, productVersionId, pipelineId, newAggregateVersion, e.getMessage(), e);
            throw e; // Re-throw SQLException
        } catch (Exception e) {
            logger.error("Exception during UPSERT for hostId {} productVersionId {} pipelineId {} aggregateVersion {}: {}", hostId, productVersionId, pipelineId, newAggregateVersion, e.getMessage(), e);
            throw e; // Re-throw generic Exception
        }
    }

    @Override
    public void deleteProductVersionPipeline(Connection conn, Map<String, Object> event) throws SQLException, Exception {
        // --- UPDATED SQL FOR SOFT DELETE + MONOTONICITY ---
        // Updates the 'active' flag to FALSE and sets the new version IF the current DB version is older than the incoming event's version.
        final String softDeleteSql =
                """
                UPDATE product_version_pipeline_t SET active = false, update_user = ?, update_ts = ?, aggregate_version = ?
                WHERE host_id = ? AND product_version_id = ? AND pipeline_id = ? AND aggregate_version < ?
                """; // <<< CRITICAL: Changed from DELETE to UPDATE, and used aggregate_version < ?

        Map<String, Object> map = SqlUtil.extractEventData(event); // Assuming extractEventData is the helper to get PortalConstants.DATA
        String hostId = (String)event.get(Constants.HOST); // HostId from CloudEvent extension
        String productVersionId = (String)map.get("productVersionId");
        String pipelineId = (String)map.get("pipelineId");

        // Old version is for logging context
        long oldAggregateVersion = SqlUtil.getOldAggregateVersion(event);
        // New version is the version of the incoming Delete event (the target version).
        long newAggregateVersion = SqlUtil.getNewAggregateVersion(event);

        String updateUser = (String)event.get(Constants.USER);
        OffsetDateTime updateTs = OffsetDateTime.parse((String)event.get(CloudEventV1.TIME));

        try (PreparedStatement statement = conn.prepareStatement(softDeleteSql)) {
            int i = 1;

            // 1. update_user
            statement.setString(i++, updateUser);

            // 2. update_ts
            statement.setObject(i++, updateTs);

            // 3. aggregate_version (NEW version in SET clause)
            statement.setLong(i++, newAggregateVersion);

            // 4. host_id (in WHERE clause)
            statement.setObject(i++, UUID.fromString(hostId));

            // 5. product_version_id (in WHERE clause)
            statement.setObject(i++, UUID.fromString(productVersionId));

            // 6. pipeline_id (in WHERE clause)
            statement.setObject(i++, UUID.fromString(pipelineId));

            // 7. aggregate_version (MONOTONICITY check in WHERE clause)
            statement.setLong(i, newAggregateVersion); // Condition: WHERE aggregate_version < newAggregateVersion

            int count = statement.executeUpdate();
            if (count == 0) {
                // If 0 rows updated, it means the record was either not found OR aggregate_version >= newAggregateVersion.
                // We IGNORE the failure and log a warning, as this is the desired idempotent/monotonic behavior.
                logger.warn("Pipeline soft-delete skipped for hostId {} productVersionId {} pipelineId {}. Record not found or a newer version already exists.", hostId, productVersionId, pipelineId);
            }
            // NO THROW on count == 0. The method is now idempotent.
            notificationService.insertNotification(event, true, null); // Insert success notification
        } catch (SQLException e) {
            logger.error("SQLException during softDeleteProductVersionPipeline for productVersionId {} pipelineId {} new version {}: {}", productVersionId, pipelineId, newAggregateVersion, e.getMessage(), e);
            notificationService.insertNotification(event, false, e.getMessage()); // Insert failure notification
            throw e; // Re-throw SQLException
        } catch (Exception e) {
            logger.error("Exception during softDeleteProductVersionPipeline for productVersionId {} pipelineId {} new version {}: {}", productVersionId, pipelineId, newAggregateVersion, e.getMessage(), e);
            notificationService.insertNotification(event, false, e.getMessage()); // Insert failure notification
            throw e; // Re-throw generic Exception
        }
    }

    @Override
    public Result<String> getProductVersionPipeline(int offset, int limit, String filtersJson, String globalFilter, String sortingJson, String hostId) {
        Result<String> result = null;
        final Map<String, String> columnMap = new HashMap<>(Map.of(
                "hostId", "pvp.host_id",
                "productVersionId", "pvp.product_version_id",
                "productId", "pv.product_id",
                "productVersion", "pv.product_version",
                "pipelineId", "pvp.pipeline_id",
                "pipelineName", "p.pipeline_name",
                "pipelineVersion", "p.pipeline_version",
                "updateUser", "pvp.update_user",
                "updateTs", "pvp.update_ts",
                "aggregateVersion", "pvp.aggregate_version"
        ));
        columnMap.put("active", "a.app_name");

        List<Map<String, Object>> filters = parseJsonList(filtersJson);
        List<Map<String, Object>> sorting = parseJsonList(sortingJson);

        String s =
            """
            SELECT COUNT(*) OVER () AS total,
            pvp.host_id, pvp.product_version_id, pv.product_id, pv.product_version,
            pvp.pipeline_id, p.pipeline_name, p.pipeline_version, pvp.update_user,
            pvp.update_ts, pvp.aggregate_version, pvp.active
            FROM product_version_pipeline_t pvp
            INNER JOIN product_version_t pv ON pv.product_version_id = pvp.product_version_id
            INNER JOIN pipeline_t p ON p.pipeline_id = pvp.pipeline_id
            WHERE pvp.host_id = ?
            """;

        List<Object> parameters = new ArrayList<>();
        parameters.add(UUID.fromString(hostId));

        String[] searchColumns = {"p.pipeline_name"};
        String sqlBuilder = s + dynamicFilter(Arrays.asList("pvp.host_id", "pvp.product_version_id", "pvp.pipeline_id"), Arrays.asList(searchColumns), filters, columnMap, parameters) +
                globalFilter(globalFilter, searchColumns, parameters) +
                dynamicSorting("pv.product_id, pv.product_version, p.pipeline_name, p.pipeline_version", sorting, columnMap) +
                "\nLIMIT ? OFFSET ?";

        parameters.add(limit);
        parameters.add(offset);
        int total = 0;
        List<Map<String, Object>> productPipelines = new ArrayList<>();

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
                    map.put("hostId", resultSet.getObject("host_id", UUID.class));
                    map.put("productVersionId", resultSet.getObject("product_version_id", UUID.class));
                    map.put("productId", resultSet.getString("product_id"));
                    map.put("productVersion", resultSet.getString("product_version"));
                    map.put("pipelineId", resultSet.getObject("pipeline_id", UUID.class));
                    map.put("pipelineName", resultSet.getString("pipeline_name"));
                    map.put("pipelineVersion", resultSet.getString("pipeline_version"));
                    map.put("updateUser", resultSet.getString("update_user"));
                    map.put("updateTs", resultSet.getObject("update_ts") != null ? resultSet.getObject("update_ts", OffsetDateTime.class) : null);
                    map.put("aggregateVersion", resultSet.getLong("aggregate_version"));
                    map.put("active", resultSet.getBoolean("active"));
                    productPipelines.add(map);
                }
            }
            Map<String, Object> resultMap = new HashMap<>();
            resultMap.put("total", total);
            resultMap.put("productPipelines", productPipelines);
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
    public void createProductVersionConfig(Connection conn, Map<String, Object> event) throws SQLException, Exception {
        // --- UPDATED SQL FOR UPSERT ---
        // Assuming the Unique Constraint is (host_id, product_version_id, config_id)
        final String upsertSql =
                """
                INSERT INTO product_version_config_t(host_id, product_version_id,
                config_id, update_user, update_ts, aggregate_version, active)
                VALUES (?, ?, ?, ?, ?, ?, TRUE)
                ON CONFLICT (host_id, product_version_id, config_id) DO UPDATE
                SET update_user = EXCLUDED.update_user,
                    update_ts = EXCLUDED.update_ts,
                    aggregate_version = EXCLUDED.aggregate_version,
                    active = TRUE
                WHERE product_version_config_t.aggregate_version < EXCLUDED.aggregate_version
                """; // <<< CRITICAL: ON CONFLICT on the composite key and monotonicity check

        Map<String, Object> map = SqlUtil.extractEventData(event);
        String hostId = (String)event.get(Constants.HOST);
        String productVersionId = (String)map.get("productVersionId");
        String configId = (String)map.get("configId");
        long newAggregateVersion = SqlUtil.getNewAggregateVersion(event);
        String updateUser = (String)event.get(Constants.USER);
        OffsetDateTime updateTs = OffsetDateTime.parse((String)event.get(CloudEventV1.TIME));


        try (PreparedStatement statement = conn.prepareStatement(upsertSql)) {
            int i = 1;
            // --- SET VALUES (1 to 6) ---
            statement.setObject(i++, UUID.fromString(hostId)); // 1. host_id (Part of PK)
            statement.setObject(i++, UUID.fromString(productVersionId)); // 2. product_version_id (Part of PK)
            statement.setObject(i++, UUID.fromString(configId)); // 3. config_id (Part of PK)
            statement.setString(i++, updateUser); // 4. update_user
            statement.setObject(i++, updateTs); // 5. update_ts
            statement.setLong(i++, newAggregateVersion); // 6. aggregate_version (New Version)

            // --- Execute UPSERT ---
            int count = statement.executeUpdate();
            if (count == 0) {
                // count=0 means the ON CONFLICT clause was hit, BUT the WHERE clause failed (version was newer or same).
                logger.warn("Config creation/reactivation skipped for hostId {} productVersionId {} configId {}. A newer or same version already exists in the projection.", hostId, productVersionId, configId);
            } else {
                logger.info("Product Version Config successfully inserted or updated to aggregateVersion {}.", newAggregateVersion);
            }

        } catch (SQLException e) {
            logger.error("SQLException during UPSERT for hostId {} productVersionId {} configId {} aggregateVersion {}: {}", hostId, productVersionId, configId, newAggregateVersion, e.getMessage(), e);
            throw e; // Re-throw SQLException
        } catch (Exception e) {
            logger.error("Exception during UPSERT for hostId {} productVersionId {} configId {} aggregateVersion {}: {}", hostId, productVersionId, configId, newAggregateVersion, e.getMessage(), e);
            throw e; // Re-throw generic Exception
        }
    }

    @Override
    public void deleteProductVersionConfig(Connection conn, Map<String, Object> event) throws SQLException, Exception {
        // --- UPDATED SQL FOR SOFT DELETE + MONOTONICITY ---
        // Updates the 'active' flag to FALSE and sets the new version IF the current DB version is older than the incoming event's version.
        final String softDeleteSql =
                """
                UPDATE product_version_config_t SET active = false, update_user = ?, update_ts = ?, aggregate_version = ?
                WHERE host_id = ? AND product_version_id = ? AND config_id = ? AND aggregate_version < ?
                """; // <<< CRITICAL: Changed from DELETE to UPDATE, and used aggregate_version < ?

        Map<String, Object> map = SqlUtil.extractEventData(event); // Assuming extractEventData is the helper to get PortalConstants.DATA
        String hostId = (String)event.get(Constants.HOST); // HostId from CloudEvent extension
        String productVersionId = (String)map.get("productVersionId");
        String configId = (String)map.get("configId");

        long oldAggregateVersion = SqlUtil.getOldAggregateVersion(event);
        // New version is the version of the incoming Delete event (the target version).
        long newAggregateVersion = SqlUtil.getNewAggregateVersion(event);

        String updateUser = (String)event.get(Constants.USER);
        OffsetDateTime updateTs = OffsetDateTime.parse((String)event.get(CloudEventV1.TIME));

        try (PreparedStatement statement = conn.prepareStatement(softDeleteSql)) {
            int i = 1;

            // 1. update_user
            statement.setString(i++, updateUser);

            // 2. update_ts
            statement.setObject(i++, updateTs);

            // 3. aggregate_version (NEW version in SET clause)
            statement.setLong(i++, newAggregateVersion);

            // 4. host_id (in WHERE clause)
            statement.setObject(i++, UUID.fromString(hostId));

            // 5. product_version_id (in WHERE clause)
            statement.setObject(i++, UUID.fromString(productVersionId));

            // 6. config_id (in WHERE clause)
            statement.setObject(i++, UUID.fromString(configId));

            // 7. aggregate_version (MONOTONICITY check in WHERE clause)
            statement.setLong(i, newAggregateVersion); // Condition: WHERE aggregate_version < newAggregateVersion

            int count = statement.executeUpdate();
            if (count == 0) {
                // If 0 rows updated, it means the record was either not found OR aggregate_version >= newAggregateVersion.
                // We IGNORE the failure and log a warning, as this is the desired idempotent/monotonic behavior.
                logger.warn("Config soft-delete skipped for hostId {} productVersionId {} configId {}. Record not found or a newer version already exists.", hostId, productVersionId, configId);
            }
            // NO THROW on count == 0. The method is now idempotent.
            // Assuming notificationService is available in the current context
            notificationService.insertNotification(event, true, null);

        } catch (SQLException e) {
            logger.error("SQLException during softDeleteProductVersionConfig for hostId {} productVersionId {} configId {} new version {}: {}", hostId, productVersionId, configId, newAggregateVersion, e.getMessage(), e);
            notificationService.insertNotification(event, false, e.getMessage());
            throw e; // Re-throw SQLException
        } catch (Exception e) {
            logger.error("Exception during softDeleteProductVersionConfig for hostId {} productVersionId {} configId {} new version {}: {}", hostId, productVersionId, configId, newAggregateVersion, e.getMessage(), e);
            notificationService.insertNotification(event, false, e.getMessage());
            throw e; // Re-throw generic Exception
        }
    }

    @Override
    public Result<String> getProductVersionConfig(int offset, int limit, String filtersJson, String globalFilter, String sortingJson, String hostId) {
        final Map<String, String> columnMap = new HashMap<>(Map.of(
                "hostId", "pvc.host_id",
                "productVersionId", "pvc.product_version_id",
                "productId", "pv.product_id",
                "productVersion", "pv.product_version",
                "configId", "pvc.config_id",
                "configName", "c.config_name",
                "updateUser", "pvc.update_user",
                "updateTs", "pvc.update_ts",
                "active", "pvc.active"
        ));

        List<Map<String, Object>> filters = parseJsonList(filtersJson);
        List<Map<String, Object>> sorting = parseJsonList(sortingJson);

        String s =
                """
                SELECT COUNT(*) OVER () AS total,
                pvc.host_id, pvc.product_version_id, pv.product_id, pv.product_version,
                pvc.config_id, c.config_name, pvc.update_user, pvc.update_ts, pvc.aggregate_version, pvc.active
                FROM product_version_config_t pvc
                INNER JOIN product_version_t pv ON pv.host_id = pvc.host_id AND pv.product_version_id = pvc.product_version_id
                INNER JOIN config_t c ON c.config_id = pvc.config_id
                WHERE pvc.host_id = ?
                """;

        List<Object> parameters = new ArrayList<>();
        parameters.add(UUID.fromString(hostId));

        String[] searchColumns = {"c.config_name"};
        String sqlBuilder = s + dynamicFilter(Arrays.asList("pvc.host_id", "pvc.product_version_id", "pvc.config_id"), Arrays.asList(searchColumns), filters, columnMap, parameters) +
                globalFilter(globalFilter, searchColumns, parameters) +
                dynamicSorting("pv.product_id, pv.product_version, c.config_name", sorting, columnMap) +
                "\nLIMIT ? OFFSET ?";

        parameters.add(limit);
        parameters.add(offset);

        int total = 0;
        List<Map<String, Object>> productConfigs = new ArrayList<>();

        try (Connection connection = ds.getConnection();
            PreparedStatement preparedStatement = connection.prepareStatement(sqlBuilder)) {
            populateParameters(preparedStatement, parameters);
            try (ResultSet resultSet = preparedStatement.executeQuery()) {
                boolean isFirstRow = true;
                while (resultSet.next()) {
                    if (isFirstRow) {
                        // The total is the same for every row, so we only need to read it once.
                        total = resultSet.getInt("total");
                        isFirstRow = false;
                    }
                    Map<String, Object> map = new HashMap<>();
                    map.put("hostId", resultSet.getObject("host_id", UUID.class).toString());
                    map.put("productVersionId", resultSet.getObject("product_version_id", UUID.class).toString());
                    map.put("productId", resultSet.getString("product_id"));
                    map.put("productVersion", resultSet.getString("product_version"));
                    map.put("configId", resultSet.getObject("config_id", UUID.class).toString());
                    map.put("configName", resultSet.getString("config_name"));
                    map.put("updateUser", resultSet.getString("update_user"));
                    map.put("updateTs", resultSet.getObject("update_ts") != null ? resultSet.getObject("update_ts", OffsetDateTime.class).toString() : null);
                    map.put("aggregateVersion", resultSet.getLong("aggregate_version"));
                    map.put("active", resultSet.getBoolean("active"));
                    productConfigs.add(map);
                }
            }
            Map<String, Object> resultMap = new HashMap<>();
            resultMap.put("total", total);
            resultMap.put("productConfigs", productConfigs);
            return Success.of(JsonMapper.toJson(resultMap));
        } catch (SQLException e) {
            logger.error("SQLException:", e);
            return Failure.of(new Status(SQL_EXCEPTION, e.getMessage()));
        } catch (Exception e) {
            logger.error("Exception:", e);
            return Failure.of(new Status(GENERIC_EXCEPTION, e.getMessage()));
        }
    }

    @Override
    public void createProductVersionConfigProperty(Connection conn, Map<String, Object> event) throws SQLException, Exception {
        // --- UPDATED SQL FOR UPSERT ---
        // Assuming the Unique Constraint is (host_id, product_version_id, property_id)
        final String upsertSql =
                """
                INSERT INTO product_version_config_property_t(host_id, product_version_id,
                property_id, update_user, update_ts, aggregate_version, active)
                VALUES (?, ?, ?, ?, ?, ?, TRUE)
                ON CONFLICT (host_id, product_version_id, property_id) DO UPDATE
                SET update_user = EXCLUDED.update_user,
                    update_ts = EXCLUDED.update_ts,
                    aggregate_version = EXCLUDED.aggregate_version,
                    active = TRUE
                WHERE product_version_config_property_t.aggregate_version < EXCLUDED.aggregate_version
                """; // <<< CRITICAL: ON CONFLICT on the composite key and monotonicity check

        Map<String, Object> map = SqlUtil.extractEventData(event);
        String hostId = (String)event.get(Constants.HOST);
        String productVersionId = (String)map.get("productVersionId");
        String propertyId = (String)map.get("propertyId");
        long newAggregateVersion = SqlUtil.getNewAggregateVersion(event);
        String updateUser = (String)event.get(Constants.USER);
        OffsetDateTime updateTs = OffsetDateTime.parse((String)event.get(CloudEventV1.TIME));


        try (PreparedStatement statement = conn.prepareStatement(upsertSql)) {
            int i = 1;
            // --- SET VALUES (1 to 6) ---
            statement.setObject(i++, UUID.fromString(hostId)); // 1. host_id (Part of PK)
            statement.setObject(i++, UUID.fromString(productVersionId)); // 2. product_version_id (Part of PK)
            statement.setObject(i++, UUID.fromString(propertyId)); // 3. property_id (Part of PK)
            statement.setString(i++, updateUser); // 4. update_user
            statement.setObject(i++, updateTs); // 5. update_ts
            statement.setLong(i++, newAggregateVersion); // 6. aggregate_version (New Version)

            // --- Execute UPSERT ---
            int count = statement.executeUpdate();
            if (count == 0) {
                // count=0 means the ON CONFLICT clause was hit, BUT the WHERE clause failed (version was newer or same).
                logger.warn("Property creation/reactivation skipped for hostId {} productVersionId {} propertyId {}. A newer or same version already exists in the projection.", hostId, productVersionId, propertyId);
            } else {
                logger.info("Product Version Config Property successfully inserted or updated to aggregateVersion {}.", newAggregateVersion);
            }

        } catch (SQLException e) {
            logger.error("SQLException during UPSERT for hostId {} productVersionId {} propertyId {} aggregateVersion {}: {}", hostId, productVersionId, propertyId, newAggregateVersion, e.getMessage(), e);
            throw e; // Re-throw SQLException
        } catch (Exception e) {
            logger.error("Exception during UPSERT for hostId {} productVersionId {} propertyId {} aggregateVersion {}: {}", hostId, productVersionId, propertyId, newAggregateVersion, e.getMessage(), e);
            throw e; // Re-throw generic Exception
        }
    }

    @Override
    public void deleteProductVersionConfigProperty(Connection conn, Map<String, Object> event) throws SQLException, Exception {
        // --- UPDATED SQL FOR SOFT DELETE + MONOTONICITY ---
        // Updates the 'active' flag to FALSE and sets the new version IF the current DB version is older than the incoming event's version.
        final String softDeleteSql =
                """
                UPDATE product_version_config_property_t SET active = false, update_user = ?, update_ts = ?, aggregate_version = ?
                WHERE host_id = ? AND product_version_id = ? AND property_id = ? AND aggregate_version < ?
                """; // <<< CRITICAL: Changed from DELETE to UPDATE, and used aggregate_version < ?

        Map<String, Object> map = SqlUtil.extractEventData(event); // Assuming extractEventData is the helper to get PortalConstants.DATA
        String hostId = (String)event.get(Constants.HOST); // HostId from CloudEvent extension
        String productVersionId = (String)map.get("productVersionId");
        String propertyId = (String)map.get("propertyId");

        long oldAggregateVersion = SqlUtil.getOldAggregateVersion(event);
        // New version is the version of the incoming Delete event (the target version).
        long newAggregateVersion = SqlUtil.getNewAggregateVersion(event);

        String updateUser = (String)event.get(Constants.USER);
        OffsetDateTime updateTs = OffsetDateTime.parse((String)event.get(CloudEventV1.TIME));

        try (PreparedStatement statement = conn.prepareStatement(softDeleteSql)) {
            int i = 1;

            // 1. update_user
            statement.setString(i++, updateUser);

            // 2. update_ts
            statement.setObject(i++, updateTs);

            // 3. aggregate_version (NEW version in SET clause)
            statement.setLong(i++, newAggregateVersion);

            // 4. host_id (in WHERE clause)
            statement.setObject(i++, UUID.fromString(hostId));

            // 5. product_version_id (in WHERE clause)
            statement.setObject(i++, UUID.fromString(productVersionId));

            // 6. property_id (in WHERE clause)
            statement.setObject(i++, UUID.fromString(propertyId));

            // 7. aggregate_version (MONOTONICITY check in WHERE clause)
            statement.setLong(i, newAggregateVersion); // Condition: WHERE aggregate_version < newAggregateVersion

            int count = statement.executeUpdate();
            if (count == 0) {
                // If 0 rows updated, it means the record was either not found OR aggregate_version >= newAggregateVersion.
                // We IGNORE the failure and log a warning, as this is the desired idempotent/monotonic behavior.
                logger.warn("Property soft-delete skipped for hostId {} productVersionId {} propertyId {}. Record not found or a newer version already exists.", hostId, productVersionId, propertyId);
            }
            // NO THROW on count == 0. The method is now idempotent.
            // Assuming notificationService is available in the current context
            notificationService.insertNotification(event, true, null);

        } catch (SQLException e) {
            logger.error("SQLException during softDeleteProductVersionConfigProperty for hostId {} productVersionId {} propertyId {} new version {}: {}", hostId, productVersionId, propertyId, newAggregateVersion, e.getMessage(), e);
            notificationService.insertNotification(event, false, e.getMessage());
            throw e; // Re-throw SQLException
        } catch (Exception e) {
            logger.error("Exception during softDeleteProductVersionConfigProperty for hostId {} productVersionId {} propertyId {} new version {}: {}", hostId, productVersionId, propertyId, newAggregateVersion, e.getMessage(), e);
            notificationService.insertNotification(event, false, e.getMessage());
            throw e; // Re-throw generic Exception
        }
    }

    @Override
    public Result<String> getProductVersionConfigProperty(int offset, int limit, String filtersJson, String globalFilter, String sortingJson, String hostId) {
        Result<String> result = null;
        final Map<String, String> columnMap = new HashMap<>(Map.of(
                "hostId", "pvcp.host_id",
                "productVersionId", "pvcp.product_version_id",
                "productId", "pv.product_id",
                "productVersion", "pv.product_version",
                "configId", "cp.config_id",
                "configName", "c.config_name",
                "propertyId", "pvcp.property_id",
                "propertyName", "cp.property_name",
                "updateUser", "pvcp.update_user",
                "updateTs", "pvcp.update_ts"
        ));
        columnMap.put("aggregateVersion", "pvcp.aggregate_version");
        columnMap.put("active", "pvcp.active");

        List<Map<String, Object>> filters = parseJsonList(filtersJson);
        List<Map<String, Object>> sorting = parseJsonList(sortingJson);

        String s =
            """
            SELECT COUNT(*) OVER () AS total,
            pvcp.host_id, pvcp.product_version_id, pv.product_id, pv.product_version,
            cp.config_id, c.config_name, pvcp.property_id, cp.property_name, pvcp.update_user, pvcp.update_ts, pvcp.aggregate_version
            FROM product_version_config_property_t pvcp
            INNER JOIN product_version_t pv ON pv.host_id = pvcp.host_id AND pv.product_version_id = pvcp.product_version_id
            INNER JOIN config_property_t cp ON cp.property_id = pvcp.property_id
            INNER JOIN config_t c ON c.config_id = cp.config_id
            WHERE pvcp.host = ?
            """;

        List<Object> parameters = new ArrayList<>();
        parameters.add(UUID.fromString(hostId));

        String[] searchColumns = {"c.config_name", "cp.property_name"};
        String sqlBuilder = s + dynamicFilter(Arrays.asList("pvcp.host_id", "pvcp.product_version_id", "cp.config_id", "pvcp.property_id"), Arrays.asList(searchColumns), filters, columnMap, parameters) +
                globalFilter(globalFilter, searchColumns, parameters) +
                dynamicSorting("pv.product_id, pv.product_version, c.config_name, cp.property_name", sorting, columnMap) +
                "\nLIMIT ? OFFSET ?";

        parameters.add(limit);
        parameters.add(offset);

        int total = 0;
        List<Map<String, Object>> productProperties = new ArrayList<>();

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
                    map.put("hostId", resultSet.getObject("host_id", UUID.class));
                    map.put("productVersionId", resultSet.getObject("product_version_id", UUID.class));
                    map.put("productId", resultSet.getString("product_id"));
                    map.put("productVersion", resultSet.getString("product_version"));
                    map.put("configId", resultSet.getObject("config_id", UUID.class));
                    map.put("configName", resultSet.getString("config_name"));
                    map.put("propertyId", resultSet.getObject("property_id", UUID.class));
                    map.put("propertyName", resultSet.getString("property_name"));
                    map.put("updateUser", resultSet.getString("update_user"));
                    map.put("updateTs", resultSet.getObject("update_ts") != null ? resultSet.getObject("update_ts", OffsetDateTime.class) : null);
                    map.put("aggregateVersion", resultSet.getLong("aggregate_version"));
                    map.put("active", resultSet.getBoolean("active"));
                    productProperties.add(map);
                }
            }
            Map<String, Object> resultMap = new HashMap<>();
            resultMap.put("total", total);
            resultMap.put("productProperties", productProperties);
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

    /**
     * Creates or updates a pipeline record using an idempotent UPSERT pattern, and manages the 'current' flag.
     * This method implements:
     * 1.  **Idempotent Create/Update (Upsert):** Uses `INSERT ... ON CONFLICT DO UPDATE` to create or update the pipeline.
     * 2.  **IDM:** Explicitly sets 'update_user' and 'update_ts' from the event metadata.
     * 3.  **OCC/Monotonicity:** Ensures updates only occur if the event version is newer than the database version.
     * 4.  **State Management:** If the upserted pipeline is marked as 'current', an additional idempotent UPDATE
     *     is performed to set all other pipelines with the same name to 'current = false'.
     *
     * @param conn  The database connection.
     * @param event The event map containing the data and metadata for the operation.
     * @throws SQLException If a database access error occurs.
     * @throws Exception    For other generic errors.
     */
    @Override
    public void createPipeline(Connection conn, Map<String, Object> event) throws SQLException, Exception {
        // --- 1. PRIMARY UPSERT SQL (Creates or Reactivates the specific pipeline) ---
        final String upsertSql =
                """
                INSERT INTO pipeline_t(host_id, pipeline_id, platform_id, pipeline_version, pipeline_name,
                current, endpoint, version_status, system_env, runtime_env, request_schema,
                response_schema, update_user, update_ts, aggregate_version, active)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, TRUE)
                ON CONFLICT (host_id, pipeline_id) DO UPDATE
                SET platform_id = EXCLUDED.platform_id,
                    pipeline_version = EXCLUDED.pipeline_version,
                    pipeline_name = EXCLUDED.pipeline_name,
                    current = EXCLUDED.current,
                    endpoint = EXCLUDED.endpoint,
                    version_status = EXCLUDED.version_status,
                    system_env = EXCLUDED.system_env,
                    runtime_env = EXCLUDED.runtime_env,
                    request_schema = EXCLUDED.request_schema,
                    response_schema = EXCLUDED.response_schema,
                    update_user = EXCLUDED.update_user,
                    update_ts = EXCLUDED.update_ts,
                    aggregate_version = EXCLUDED.aggregate_version,
                    active = TRUE
                WHERE pipeline_t.aggregate_version < EXCLUDED.aggregate_version
                """;

        // --- 2. UPDATE SQL (To set all other versions of the same pipeline to current = false) ---
        final String updateOthersSql =
                """
                UPDATE pipeline_t SET current = false, update_user = ?, update_ts = ?
                WHERE host_id = ?
                AND pipeline_name = ?
                AND pipeline_id != ?
                AND current = TRUE
                """;

        Map<String, Object> map = SqlUtil.extractEventData(event);
        String hostId = (String) event.get(Constants.HOST);
        String pipelineId = (String) map.get("pipelineId");
        String pipelineName = (String) map.get("pipelineName");
        Boolean current = (Boolean) map.getOrDefault("current", false);
        long newAggregateVersion = SqlUtil.getNewAggregateVersion(event);
        String updateUser = (String) event.get(Constants.USER);
        OffsetDateTime updateTs = OffsetDateTime.parse((String) event.get(CloudEventV1.TIME));

        try (PreparedStatement statement = conn.prepareStatement(upsertSql)) {
            int i = 1;

            // --- PRIMARY UPSERT PARAMETERS ---
            statement.setObject(i++, UUID.fromString(hostId)); // 1. host_id
            statement.setObject(i++, UUID.fromString(pipelineId)); // 2. pipeline_id (PK)
            statement.setObject(i++, UUID.fromString((String) map.get("platformId"))); // 3. platform_id
            statement.setString(i++, (String) map.get("pipelineVersion")); // 4. pipeline_version
            statement.setString(i++, pipelineName); // 5. pipeline_name
            statement.setBoolean(i++, current); // 6. current
            statement.setString(i++, (String) map.get("endpoint")); // 7. endpoint
            statement.setString(i++, (String) map.get("versionStatus")); // 8. version_status
            statement.setString(i++, (String) map.get("systemEnv")); // 9. system_env

            // 10. runtime_env (optional)
            if (map.containsKey("runtimeEnv")) {
                statement.setString(i++, (String) map.get("runtimeEnv"));
            } else {
                statement.setNull(i++, Types.VARCHAR);
            }

            statement.setString(i++, (String) map.get("requestSchema")); // 11. request_schema
            statement.setString(i++, (String) map.get("responseSchema")); // 12. response_schema
            statement.setString(i++, updateUser); // 13. update_user
            statement.setObject(i++, updateTs); // 14. update_ts
            statement.setLong(i++, newAggregateVersion); // 15. aggregate_version

            int count = statement.executeUpdate();
            if (count == 0) {
                logger.warn("Pipeline creation/update skipped for hostId {} pipelineId {}. A newer or same version already exists.", hostId, pipelineId);
            }

            // --- STATE MANAGEMENT: Set others to current=false if this new one is true ---
            if (current && count > 0) {
                try (PreparedStatement statementUpdate = conn.prepareStatement(updateOthersSql)) {
                    int j = 1;
                    statementUpdate.setString(j++, updateUser);
                    statementUpdate.setObject(j++, updateTs);
                    statementUpdate.setObject(j++, UUID.fromString(hostId));
                    statementUpdate.setString(j++, pipelineName);
                    statementUpdate.setObject(j, UUID.fromString(pipelineId));
                    statementUpdate.executeUpdate();
                }
            }

        } catch (SQLException e) {
            logger.error("SQLException during createPipeline UPSERT for hostId {} pipelineId {} aggregateVersion {}: {}", hostId, pipelineId, newAggregateVersion, e.getMessage(), e);
            throw e;
        } catch (Exception e) {
            logger.error("Exception during createPipeline for hostId {} pipelineId {} aggregateVersion {}: {}", hostId, pipelineId, newAggregateVersion, e.getMessage(), e);
            throw e;
        }
    }


    /**
     * Updates a pipeline record using an idempotent/monotonic pattern and manages the 'current' flag.
     * This method implements:
     * 1.  **Soft Delete Principle:** An update implicitly reactivates the record by setting `active = TRUE`.
     * 2.  **IDM:** Explicitly sets 'update_user' and 'update_ts' from the event metadata.
     * 3.  **OCC/Idempotency:** Uses `aggregate_version < ?` to ensure the operation only succeeds if it
     *     represents a new state, preventing re-processing of old events or concurrent conflicts.
     * 4.  **State Management:** If the updated pipeline is marked as 'current', an additional idempotent UPDATE
     *     is performed to set all other pipelines with the same name to 'current = false'.
     *
     * @param conn  The database connection.
     * @param event The event map containing the data and metadata for the operation.
     * @throws SQLException If a database access error occurs.
     * @throws Exception    For other generic errors.
     */
    @Override
    public void updatePipeline(Connection conn, Map<String, Object> event) throws SQLException, Exception {
        // --- 1. PRIMARY UPDATE SQL (Updates the specific pipeline) ---
        final String updateSql =
                """
                UPDATE pipeline_t
                SET platform_id = ?,
                    pipeline_version = ?,
                    pipeline_name = ?,
                    current = ?,
                    endpoint = ?,
                    version_status = ?,
                    system_env = ?,
                    runtime_env = ?,
                    request_schema = ?,
                    response_schema = ?,
                    update_user = ?,
                    update_ts = ?,
                    aggregate_version = ?,
                    active = TRUE
                WHERE host_id = ?
                  AND pipeline_id = ?
                  AND aggregate_version < ?
                """;

        // --- 2. UPDATE SQL (To set all other versions of the same pipeline to current = false) ---
        final String updateOthersSql =
                """
                UPDATE pipeline_t SET current = false, update_user = ?, update_ts = ?
                WHERE host_id = ?
                  AND pipeline_name = ?
                  AND pipeline_id != ?
                  AND current = TRUE
                """;

        Map<String, Object> map = SqlUtil.extractEventData(event);
        String hostId = (String) event.get(Constants.HOST);
        String pipelineId = (String) map.get("pipelineId");
        String pipelineName = (String) map.get("pipelineName");
        Boolean current = (Boolean) map.getOrDefault("current", false);
        long newAggregateVersion = SqlUtil.getNewAggregateVersion(event);
        String updateUser = (String) event.get(Constants.USER);
        OffsetDateTime updateTs = OffsetDateTime.parse((String) event.get(CloudEventV1.TIME));

        try (PreparedStatement statement = conn.prepareStatement(updateSql)) {
            // SET clause placeholders (13)
            int i = 1;
            statement.setObject(i++, UUID.fromString((String) map.get("platformId")));
            statement.setString(i++, (String) map.get("pipelineVersion"));
            statement.setString(i++, pipelineName);
            statement.setBoolean(i++, current);
            statement.setString(i++, (String) map.get("endpoint"));
            statement.setString(i++, (String) map.get("versionStatus"));
            statement.setString(i++, (String) map.get("systemEnv"));

            if (map.containsKey("runtimeEnv")) {
                statement.setString(i++, (String) map.get("runtimeEnv"));
            } else {
                statement.setNull(i++, Types.VARCHAR);
            }

            statement.setString(i++, (String) map.get("requestSchema"));
            statement.setString(i++, (String) map.get("responseSchema"));
            statement.setString(i++, updateUser);
            statement.setObject(i++, updateTs);
            statement.setLong(i++, newAggregateVersion);

            // WHERE clause placeholders (3)
            statement.setObject(i++, UUID.fromString(hostId));
            statement.setObject(i++, UUID.fromString(pipelineId));
            statement.setLong(i++, newAggregateVersion);

            int count = statement.executeUpdate();
            if (count == 0) {
                logger.warn("Update skipped for Pipeline with hostId {} pipelineId {}. Record not found or a newer version already exists.",
                        hostId, pipelineId);
            }

            // --- STATE MANAGEMENT: Set others to current=false if this one is true ---
            if (current && count > 0) {
                try (PreparedStatement statementUpdate = conn.prepareStatement(updateOthersSql)) {
                    int j = 1;
                    statementUpdate.setString(j++, updateUser);
                    statementUpdate.setObject(j++, updateTs);
                    statementUpdate.setObject(j++, UUID.fromString(hostId));
                    statementUpdate.setString(j++, pipelineName);
                    statementUpdate.setObject(j, UUID.fromString(pipelineId));
                    statementUpdate.executeUpdate();
                }
            }
        } catch (SQLException e) {
            logger.error("SQLException during updatePipeline for hostId {} pipelineId {}: {}",
                    hostId, pipelineId, e.getMessage(), e);
            throw e;
        } catch (Exception e) {
            logger.error("Exception during updatePipeline for hostId {} pipelineId {}: {}",
                    hostId, pipelineId, e.getMessage(), e);
            throw e;
        }
    }

    /**
     * Performs a soft delete on a pipeline_t record using an idempotent/monotonic pattern.
     * This method implements:
     * 1.  **Soft Delete:** Sets the 'active' flag to false.
     * 2.  **IDM:** Explicitly sets 'update_user' and 'update_ts' from the event metadata.
     * 3.  **OCC/Idempotency:** Uses `aggregate_version < ?` to ensure the operation only succeeds if it
     *     represents a new state, preventing re-processing of old events or concurrent conflicts.
     *
     * @param conn  The database connection.
     * @param event The event map containing the data and metadata for the operation.
     * @throws SQLException If a database access error occurs.
     * @throws Exception    For other generic errors.
     */
    @Override
    public void deletePipeline(Connection conn, Map<String, Object> event) throws SQLException, Exception {
        // SQL statement updated to match the idempotent soft-delete pattern.
        final String sql =
                """
                UPDATE pipeline_t
                SET active = FALSE,
                    update_user = ?,
                    update_ts = ?,
                    aggregate_version = ?
                WHERE host_id = ?
                  AND pipeline_id = ?
                  AND aggregate_version < ?
                """;

        // Extract data and metadata from the event.
        Map<String, Object> map = SqlUtil.extractEventData(event);
        String hostId = (String) event.get(Constants.HOST);
        String pipelineId = (String) map.get("pipelineId");
        long newAggregateVersion = SqlUtil.getNewAggregateVersion(event);

        try (PreparedStatement statement = conn.prepareStatement(sql)) {
            // SET clause placeholders (3)
            int i = 1;
            // 1: update_user from event metadata
            statement.setString(i++, (String) event.get(Constants.USER));
            // 2: update_ts from event metadata
            statement.setObject(i++, OffsetDateTime.parse((String) event.get(CloudEventV1.TIME)));
            // 3: aggregate_version (the new version)
            statement.setLong(i++, newAggregateVersion);

            // WHERE clause placeholders (3)
            // 4: host_id from primary key
            statement.setObject(i++, UUID.fromString(hostId));
            // 5: pipeline_id from primary key
            statement.setObject(i++, UUID.fromString(pipelineId));
            // 6: aggregate_version < ? for OCC/Idempotency check
            statement.setLong(i++, newAggregateVersion);

            int count = statement.executeUpdate();
            if (count == 0) {
                // If 0 rows were updated, we log a warning and continue.
                // This indicates the record was already deleted, never existed, or a newer version is already in the database.
                // This is the expected behavior for an idempotent operation.
                logger.warn("Soft delete skipped for Pipeline with hostId {}, pipelineId {}, newAggregateVersion {}. Record not found or a newer version already exists.",
                        hostId, pipelineId, newAggregateVersion);
            }
        } catch (SQLException e) {
            logger.error("SQLException during soft delete of Pipeline for hostId {} pipelineId {}: {}",
                    hostId, pipelineId, e.getMessage(), e);
            throw e;
        } catch (Exception e) {
            logger.error("Exception during soft delete of Pipeline for hostId {} pipelineId {}: {}",
                    hostId, pipelineId, e.getMessage(), e);
            throw e;
        }
    }

    @Override
    public Result<String> getPipeline(int offset, int limit, String filtersJson, String globalFilter, String sortingJson, String hostId) {
        Result<String> result = null;

        final Map<String, String> columnMap = new HashMap<>(Map.of(
                "hostId", "p.host_id",
                "pipelineId", "p.pipeline_id",
                "platformId", "p.platform_id",
                "platformName", "pf.platform_name",
                "platformVersion", "pf.platform_version",
                "pipelineVersion", "p.pipeline_version",
                "pipelineName", "p.pipeline_name",
                "current", "p.current",
                "endpoint", "p.endpoint",
                "versionStatus", "p.version_status"
        ));
        columnMap.put("systemEnv", "p.system_env");
        columnMap.put("runtimeEnv", "p.runtime_env");
        columnMap.put("requestSchema", "p.request_schema");
        columnMap.put("responseSchema", "p.response_schema");
        columnMap.put("updateUser", "p.update_user");
        columnMap.put("updateTs", "p.update_ts");
        columnMap.put("aggregateVersion", "p.aggregate_version");
        columnMap.put("active", "p.active");

        List<Map<String, Object>> filters = parseJsonList(filtersJson);
        List<Map<String, Object>> sorting = parseJsonList(sortingJson);

        String s =
            """
            SELECT COUNT(*) OVER () AS total,
            p.host_id, p.pipeline_id, p.platform_id, pf.platform_name, pf.platform_version,
            p.pipeline_version, p.pipeline_name, p.current, p.endpoint, p.version_status,
            p.system_env, p.runtime_env, p.request_schema, p.response_schema,
            p.update_user, p.update_ts, p.aggregate_version, p.active
            FROM pipeline_t p
            INNER JOIN platform_t pf ON pf.platform_id = p.platform_id
            WHERE 1=1
            """;

        List<Object> parameters = new ArrayList<>();

        String[] searchColumns = {"pf.platform_name", "p.pipeline_name", "p.endpoint"};
        String sqlBuilder = s + dynamicFilter(Arrays.asList("p.host_id", "p.pipeline_id", "p.platform_id"), Arrays.asList(searchColumns), filters, columnMap, parameters) +
                globalFilter(globalFilter, searchColumns, parameters) +
                dynamicSorting("p.pipeline_id", sorting, columnMap) +
                "\nLIMIT ? OFFSET ?";
        parameters.add(limit);
        parameters.add(offset);
        int total = 0;
        List<Map<String, Object>> pipelines = new ArrayList<>();

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
                    map.put("hostId", resultSet.getObject("host_id", UUID.class));
                    map.put("pipelineId", resultSet.getObject("pipeline_id", UUID.class));
                    map.put("platformId", resultSet.getObject("platform_id", UUID.class));
                    map.put("platformName", resultSet.getString("platform_name"));
                    map.put("platformVersion", resultSet.getString("platform_version"));
                    map.put("pipelineVersion", resultSet.getString("pipeline_version"));
                    map.put("pipelineName", resultSet.getString("pipeline_name"));
                    map.put("current", resultSet.getBoolean("current"));
                    map.put("endpoint", resultSet.getString("endpoint"));
                    map.put("versionStatus", resultSet.getString("version_status"));
                    map.put("systemEnv", resultSet.getString("system_env"));
                    map.put("runtimeEnv", resultSet.getString("runtime_env"));
                    map.put("requestSchema", resultSet.getString("request_schema"));
                    map.put("responseSchema", resultSet.getString("response_schema"));
                    map.put("updateUser", resultSet.getString("update_user"));
                    map.put("updateTs", resultSet.getObject("update_ts") != null ? resultSet.getObject("update_ts", OffsetDateTime.class) : null);
                    map.put("aggregateVersion", resultSet.getLong("aggregate_version"));
                    map.put("active", resultSet.getBoolean("active"));
                    pipelines.add(map);
                }
            }
            Map<String, Object> resultMap = new HashMap<>();
            resultMap.put("total", total);
            resultMap.put("pipelines", pipelines);
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
    public Result<String> getPipelineLabel(String hostId) {
        Result<String> result = null;
        String sql = "SELECT pipeline_id, pipeline_name, pipeline_version FROM pipeline_t WHERE host_id = ?";
        List<Map<String, Object>> labels = new ArrayList<>();
        try (Connection connection = ds.getConnection();
             PreparedStatement preparedStatement = connection.prepareStatement(sql)) {
            preparedStatement.setObject(1, UUID.fromString(hostId));
            try (ResultSet resultSet = preparedStatement.executeQuery()) {
                while (resultSet.next()) {
                    Map<String, Object> map = new HashMap<>();
                    String pipelineId = resultSet.getObject("pipeline_id", UUID.class).toString();
                    String pipelineName = resultSet.getString("pipeline_name");
                    String pipelineVersion = resultSet.getString("pipeline_version");
                    map.put("id", pipelineId);
                    map.put("label", pipelineName + "|" + pipelineVersion);
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

    /**
     * Creates or reactivates a platform_t record using an idempotent UPSERT pattern.
     * This method implements:
     * 1.  **Idempotent Create/Update (Upsert):** Uses `INSERT ... ON CONFLICT DO UPDATE`. This will
     *     create the record on the first valid event, or update and reactivate it if a soft-deleted
     *     record with an older version already exists.
     * 2.  **IDM:** Explicitly sets 'update_user' and 'update_ts' from the event metadata.
     * 3.  **OCC/Monotonicity:** The `WHERE` clause in the `DO UPDATE` part ensures that an update
     *     only occurs if the incoming event's version is strictly greater than the existing record's version.
     *
     * @param conn  The database connection.
     * @param event The event map containing the data and metadata for the operation.
     * @throws SQLException If a database access error occurs.
     * @throws Exception    For other generic errors.
     */
    @Override
    public void createPlatform(Connection conn, Map<String, Object> event) throws SQLException, Exception {
        final String sql =
                """
                INSERT INTO platform_t(host_id, platform_id, platform_name, platform_version,
                client_type, handler_class, client_url, credentials, proxy_url, proxy_port, console_url,
                environment, zone, region, lob, update_user, update_ts, aggregate_version, active)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, TRUE)
                ON CONFLICT (host_id, platform_id) DO UPDATE
                SET platform_name = EXCLUDED.platform_name,
                    platform_version = EXCLUDED.platform_version,
                    client_type = EXCLUDED.client_type,
                    handler_class = EXCLUDED.handler_class,
                    client_url = EXCLUDED.client_url,
                    credentials = EXCLUDED.credentials,
                    proxy_url = EXCLUDED.proxy_url,
                    proxy_port = EXCLUDED.proxy_port,
                    console_url = EXCLUDED.console_url,
                    environment = EXCLUDED.environment,
                    zone = EXCLUDED.zone,
                    region = EXCLUDED.region,
                    lob = EXCLUDED.lob,
                    update_user = EXCLUDED.update_user,
                    update_ts = EXCLUDED.update_ts,
                    aggregate_version = EXCLUDED.aggregate_version,
                    active = TRUE
                WHERE platform_t.aggregate_version < EXCLUDED.aggregate_version
                """;

        Map<String, Object> map = SqlUtil.extractEventData(event);
        String hostId = (String) event.get(Constants.HOST);
        String platformId = (String) map.get("platformId");
        long newAggregateVersion = SqlUtil.getNewAggregateVersion(event);

        try (PreparedStatement statement = conn.prepareStatement(sql)) {
            // Set parameters for the INSERT part of the statement
            int i = 1;
            statement.setObject(i++, UUID.fromString(hostId));
            statement.setObject(i++, UUID.fromString(platformId));
            statement.setString(i++, (String) map.get("platformName"));
            statement.setString(i++, (String) map.get("platformVersion"));
            statement.setString(i++, (String) map.get("clientType"));
            statement.setString(i++, (String) map.get("handlerClass"));
            statement.setString(i++, (String) map.get("clientUrl"));
            statement.setString(i++, (String) map.get("credentials"));

            // Handle optional fields
            statement.setString(i++, (String) map.get("proxyUrl"));
            if (map.get("proxyPort") != null) {
                statement.setInt(i++, (Integer) map.get("proxyPort"));
            } else {
                statement.setNull(i++, Types.INTEGER);
            }
            statement.setString(i++, (String) map.get("consoleUrl"));
            statement.setString(i++, (String) map.get("environment"));
            statement.setString(i++, (String) map.get("zone"));
            statement.setString(i++, (String) map.get("region"));
            statement.setString(i++, (String) map.get("lob"));

            // IDM and OCC fields
            statement.setString(i++, (String) event.get(Constants.USER));
            statement.setObject(i++, OffsetDateTime.parse((String) event.get(CloudEventV1.TIME)));
            statement.setLong(i++, newAggregateVersion);

            int count = statement.executeUpdate();

            if (count == 0) {
                // A count of 0 is a valid, idempotent outcome.
                logger.warn("Creation/Reactivation skipped for Platform with hostId {}, platformId {}. A newer or same version already exists.",
                        hostId, platformId);
            }
        } catch (SQLException e) {
            logger.error("SQLException during createPlatform for hostId {} platformId {}: {}",
                    hostId, platformId, e.getMessage(), e);
            throw e;
        } catch (Exception e) {
            logger.error("Exception during createPlatform for hostId {} platformId {}: {}",
                    hostId, platformId, e.getMessage(), e);
            throw e;
        }
    }


    /**
     * Updates a platform_t record using an idempotent/monotonic pattern.
     * This method implements:
     * 1.  **Soft Delete Principle:** An update implicitly reactivates the record by setting `active = TRUE`.
     * 2.  **IDM:** Explicitly sets 'update_user' and 'update_ts' from the event metadata.
     * 3.  **OCC/Idempotency:** Uses `aggregate_version < ?` to ensure the operation only succeeds if it
     *     represents a new state, preventing re-processing of old events or concurrent conflicts.
     *
     * @param conn  The database connection.
     * @param event The event map containing the data and metadata for the operation.
     * @throws SQLException If a database access error occurs.
     * @throws Exception    For other generic errors.
     */
    @Override
    public void updatePlatform(Connection conn, Map<String, Object> event) throws SQLException, Exception {
        // SQL statement updated to match the idempotent update pattern.
        final String sql =
                """
                UPDATE platform_t
                SET platform_name = ?,
                    platform_version = ?,
                    client_type = ?,
                    handler_class = ?,
                    client_url = ?,
                    credentials = ?,
                    proxy_url = ?,
                    proxy_port = ?,
                    console_url = ?,
                    environment = ?,
                    zone = ?,
                    region = ?,
                    lob = ?,
                    update_user = ?,
                    update_ts = ?,
                    aggregate_version = ?,
                    active = TRUE
                WHERE host_id = ?
                  AND platform_id = ?
                  AND aggregate_version < ?
                """;

        Map<String, Object> map = SqlUtil.extractEventData(event);
        String hostId = (String) event.get(Constants.HOST);
        String platformId = (String) map.get("platformId");
        long newAggregateVersion = SqlUtil.getNewAggregateVersion(event);

        try (PreparedStatement statement = conn.prepareStatement(sql)) {
            // SET clause placeholders (16)
            int i = 1;
            statement.setString(i++, (String) map.get("platformName"));
            statement.setString(i++, (String) map.get("platformVersion"));
            statement.setString(i++, (String) map.get("clientType"));
            statement.setString(i++, (String) map.get("handlerClass"));
            statement.setString(i++, (String) map.get("clientUrl"));
            statement.setString(i++, (String) map.get("credentials"));

            // Handle optional fields
            statement.setString(i++, (String) map.get("proxyUrl"));
            if (map.get("proxyPort") != null) {
                statement.setInt(i++, (Integer) map.get("proxyPort"));
            } else {
                statement.setNull(i++, Types.INTEGER);
            }
            statement.setString(i++, (String) map.get("consoleUrl"));
            statement.setString(i++, (String) map.get("environment"));
            statement.setString(i++, (String) map.get("zone"));
            statement.setString(i++, (String) map.get("region"));
            statement.setString(i++, (String) map.get("lob"));

            // IDM and OCC fields
            statement.setString(i++, (String) event.get(Constants.USER));
            statement.setObject(i++, OffsetDateTime.parse((String) event.get(CloudEventV1.TIME)));
            statement.setLong(i++, newAggregateVersion);

            // WHERE clause placeholders (3)
            statement.setObject(i++, UUID.fromString(hostId));
            statement.setObject(i++, UUID.fromString(platformId));
            statement.setLong(i++, newAggregateVersion);

            int count = statement.executeUpdate();

            if (count == 0) {
                // If 0 rows were updated, it's a valid idempotent outcome.
                logger.warn("Update skipped for Platform with hostId {}, platformId {}. Record not found or a newer version already exists.",
                        hostId, platformId);
            }
        } catch (SQLException e) {
            logger.error("SQLException during updatePlatform for hostId {} platformId {}: {}",
                    hostId, platformId, e.getMessage(), e);
            throw e;
        } catch (Exception e) {
            logger.error("Exception during updatePlatform for hostId {} platformId {}: {}",
                    hostId, platformId, e.getMessage(), e);
            throw e;
        }
    }

    /**
     * Performs a soft delete on a platform_t record using an idempotent/monotonic pattern.
     * This method implements:
     * 1.  **Soft Delete:** Sets the 'active' flag to false.
     * 2.  **IDM:** Explicitly sets 'update_user' and 'update_ts' from the event metadata.
     * 3.  **OCC/Idempotency:** Uses `aggregate_version < ?` to ensure the operation only succeeds if it
     *     represents a new state, preventing re-processing of old events or concurrent conflicts.
     *
     * @param conn  The database connection.
     * @param event The event map containing the data and metadata for the operation.
     * @throws SQLException If a database access error occurs.
     * @throws Exception    For other generic errors.
     */
    @Override
    public void deletePlatform(Connection conn, Map<String, Object> event) throws SQLException, Exception {
        // SQL statement updated to match the idempotent soft-delete pattern.
        final String sql =
                """
                UPDATE platform_t
                SET active = FALSE,
                    update_user = ?,
                    update_ts = ?,
                    aggregate_version = ?
                WHERE host_id = ?
                  AND platform_id = ?
                  AND aggregate_version < ?
                """;

        // Extract data and metadata from the event.
        Map<String, Object> map = SqlUtil.extractEventData(event);
        String hostId = (String) event.get(Constants.HOST);
        String platformId = (String) map.get("platformId");
        long newAggregateVersion = SqlUtil.getNewAggregateVersion(event);

        try (PreparedStatement statement = conn.prepareStatement(sql)) {
            // SET clause placeholders (3)
            int i = 1;
            // 1: update_user from event metadata
            statement.setString(i++, (String) event.get(Constants.USER));
            // 2: update_ts from event metadata
            statement.setObject(i++, OffsetDateTime.parse((String) event.get(CloudEventV1.TIME)));
            // 3: aggregate_version (the new version)
            statement.setLong(i++, newAggregateVersion);

            // WHERE clause placeholders (3)
            // 4: host_id from primary key
            statement.setObject(i++, UUID.fromString(hostId));
            // 5: platform_id from primary key
            statement.setObject(i++, UUID.fromString(platformId));
            // 6: aggregate_version < ? for OCC/Idempotency check
            statement.setLong(i++, newAggregateVersion);

            int count = statement.executeUpdate();
            if (count == 0) {
                // If 0 rows were updated, we log a warning and continue.
                // This indicates the record was already deleted, never existed, or a newer version is already in the database.
                // This is the expected behavior for an idempotent operation.
                logger.warn("Soft delete skipped for Platform with hostId {}, platformId {}, newAggregateVersion {}. Record not found or a newer version already exists.",
                        hostId, platformId, newAggregateVersion);
            }
        } catch (SQLException e) {
            logger.error("SQLException during soft delete of Platform for hostId {} platformId {}: {}",
                    hostId, platformId, e.getMessage(), e);
            throw e;
        } catch (Exception e) {
            logger.error("Exception during soft delete of Platform for hostId {} platformId {}: {}",
                    hostId, platformId, e.getMessage(), e);
            throw e;
        }
    }


    @Override
    public Result<String> getPlatform(int offset, int limit, String filtersJson, String globalFilter, String sortingJson, String hostId) {
        Result<String> result = null;

        List<Map<String, Object>> filters = parseJsonList(filtersJson);
        List<Map<String, Object>> sorting = parseJsonList(sortingJson);

        String s =
            """
            SELECT COUNT(*) OVER () AS total,
            host_id, platform_id, platform_name, platform_version, client_type, client_url,
            credentials, proxy_url, proxy_port, handler_class, console_url, environment,
            zone, region, lob, update_user, update_ts, aggregate_version, active
            FROM platform_t
            WHERE host_id = ?
            """;

        List<Object> parameters = new ArrayList<>();
        parameters.add(UUID.fromString(hostId));

        String[] searchColumns = {"platform_name"};
        String sqlBuilder = s + dynamicFilter(Arrays.asList("host_id", "platform_id"), Arrays.asList(searchColumns), filters, null, parameters) +
                globalFilter(globalFilter, searchColumns, parameters) +
                dynamicSorting("platform_id", sorting, null) +
                "\nLIMIT ? OFFSET ?";

        parameters.add(limit);
        parameters.add(offset);
        int total = 0;
        List<Map<String, Object>> platforms = new ArrayList<>();

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
                    map.put("hostId", resultSet.getObject("host_id", UUID.class));
                    map.put("platformId", resultSet.getObject("platform_id", UUID.class));
                    map.put("platformName", resultSet.getString("platform_name"));
                    map.put("platformVersion", resultSet.getString("platform_version"));
                    map.put("clientType", resultSet.getString("client_type"));
                    map.put("clientUrl", resultSet.getString("client_url"));
                    map.put("credentials", resultSet.getString("credentials"));
                    map.put("proxyUrl", resultSet.getString("proxy_url"));
                    map.put("proxyPort", resultSet.getInt("proxy_port"));
                    map.put("handlerClass", resultSet.getString("handler_class"));
                    map.put("consoleUrl", resultSet.getString("console_url"));
                    map.put("environment", resultSet.getString("environment"));
                    map.put("zone", resultSet.getString("zone"));
                    map.put("region", resultSet.getString("region"));
                    map.put("lob", resultSet.getString("lob"));
                    map.put("updateUser", resultSet.getString("update_user"));
                    map.put("updateTs", resultSet.getObject("update_ts") != null ? resultSet.getObject("update_ts", OffsetDateTime.class) : null);
                    map.put("aggregateVersion", resultSet.getLong("aggregate_version"));
                    map.put("active", resultSet.getBoolean("active"));
                    platforms.add(map);
                }
            }

            Map<String, Object> resultMap = new HashMap<>();
            resultMap.put("total", total);
            resultMap.put("platforms", platforms);
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
    public Result<String> getPlatformLabel(String hostId) {
        Result<String> result = null;
        String sql = "SELECT platform_id, platform_name FROM platform_t WHERE host_id = ?";
        List<Map<String, Object>> labels = new ArrayList<>();
        try (Connection connection = ds.getConnection();
             PreparedStatement preparedStatement = connection.prepareStatement(sql)) {
            preparedStatement.setObject(1, UUID.fromString(hostId));
            try (ResultSet resultSet = preparedStatement.executeQuery()) {
                while (resultSet.next()) {
                    Map<String, Object> map = new HashMap<>();
                    map.put("id", resultSet.getString("platform_id"));
                    map.put("label", resultSet.getString("platform_name"));
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

    /**
     * Creates or reactivates a deployment_instance_t record using an idempotent UPSERT pattern.
     * This method implements:
     * 1.  **Idempotent Create/Update (Upsert):** Uses `INSERT ... ON CONFLICT DO UPDATE`. This will
     *     create the record on the first valid event, or update and reactivate it if a soft-deleted
     *     record with an older version already exists.
     * 2.  **IDM:** Explicitly sets 'update_user' and 'update_ts' from the event metadata.
     * 3.  **OCC/Monotonicity:** The `WHERE` clause in the `DO UPDATE` part ensures that an update
     *     only occurs if the incoming event's version is strictly greater than the existing record's version.
     *
     * @param conn  The database connection.
     * @param event The event map containing the data and metadata for the operation.
     * @throws SQLException If a database access error occurs.
     * @throws Exception    For other generic errors.
     */
    @Override
    public void createDeploymentInstance(Connection conn, Map<String, Object> event) throws SQLException, Exception {
        final String sql =
                """
                INSERT INTO deployment_instance_t(host_id, deployment_instance_id, instance_id,
                service_id, ip_address, port_number, system_env, runtime_env, pipeline_id,
                update_user, update_ts, aggregate_version, active)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, TRUE)
                ON CONFLICT (host_id, deployment_instance_id) DO UPDATE
                SET instance_id = EXCLUDED.instance_id,
                    service_id = EXCLUDED.service_id,
                    ip_address = EXCLUDED.ip_address,
                    port_number = EXCLUDED.port_number,
                    system_env = EXCLUDED.system_env,
                    runtime_env = EXCLUDED.runtime_env,
                    pipeline_id = EXCLUDED.pipeline_id,
                    update_user = EXCLUDED.update_user,
                    update_ts = EXCLUDED.update_ts,
                    aggregate_version = EXCLUDED.aggregate_version,
                    active = TRUE
                WHERE deployment_instance_t.aggregate_version < EXCLUDED.aggregate_version
                """;

        Map<String, Object> map = SqlUtil.extractEventData(event);
        String hostId = (String) event.get(Constants.HOST);
        String deploymentInstanceId = (String) map.get("deploymentInstanceId");
        long newAggregateVersion = SqlUtil.getNewAggregateVersion(event);

        try (PreparedStatement statement = conn.prepareStatement(sql)) {
            // Set parameters for the INSERT part of the statement
            int i = 1;
            statement.setObject(i++, UUID.fromString(hostId));
            statement.setObject(i++, UUID.fromString(deploymentInstanceId));
            statement.setObject(i++, UUID.fromString((String) map.get("instanceId")));
            statement.setString(i++, (String) map.get("serviceId"));
            statement.setString(i++, (String) map.get("ipAddress"));

            if (map.get("portNumber") != null) {
                statement.setInt(i++, (Integer) map.get("portNumber"));
            } else {
                statement.setNull(i++, Types.INTEGER);
            }

            statement.setString(i++, (String) map.get("systemEnv"));
            statement.setString(i++, (String) map.get("runtimeEnv"));
            statement.setObject(i++, UUID.fromString((String) map.get("pipelineId")));

            // IDM and OCC fields
            statement.setString(i++, (String) event.get(Constants.USER));
            statement.setObject(i++, OffsetDateTime.parse((String) event.get(CloudEventV1.TIME)));
            statement.setLong(i++, newAggregateVersion);

            int count = statement.executeUpdate();

            if (count == 0) {
                // A count of 0 is a valid, idempotent outcome.
                logger.warn("Creation/Reactivation skipped for DeploymentInstance with hostId {}, deploymentInstanceId {}. A newer or same version already exists.",
                        hostId, deploymentInstanceId);
            }
        } catch (SQLException e) {
            logger.error("SQLException during createDeploymentInstance for hostId {} deploymentInstanceId {}: {}",
                    hostId, deploymentInstanceId, e.getMessage(), e);
            throw e;
        } catch (Exception e) {
            logger.error("Exception during createDeploymentInstance for hostId {} deploymentInstanceId {}: {}",
                    hostId, deploymentInstanceId, e.getMessage(), e);
            throw e;
        }
    }

    /**
     * Updates a deployment_instance_t record using an idempotent/monotonic pattern.
     * This method implements:
     * 1.  **Soft Delete Principle:** An update implicitly reactivates the record by setting `active = TRUE`.
     * 2.  **IDM:** Explicitly sets 'update_user' and 'update_ts' from the event metadata.
     * 3.  **OCC/Idempotency:** Uses `aggregate_version < ?` to ensure the operation only succeeds if it
     *     represents a new state, preventing re-processing of old events or concurrent conflicts.
     *
     * @param conn  The database connection.
     * @param event The event map containing the data and metadata for the operation.
     * @throws SQLException If a database access error occurs.
     * @throws Exception    For other generic errors.
     */
    @Override
    public void updateDeploymentInstance(Connection conn, Map<String, Object> event) throws SQLException, Exception {
        // SQL statement updated to match the idempotent update pattern.
        final String sql =
                """
                UPDATE deployment_instance_t
                SET service_id = ?,
                    ip_address = ?,
                    port_number = ?,
                    system_env = ?,
                    runtime_env = ?,
                    pipeline_id = ?,
                    update_user = ?,
                    update_ts = ?,
                    aggregate_version = ?,
                    active = TRUE
                WHERE host_id = ?
                  AND deployment_instance_id = ?
                  AND aggregate_version < ?
                """;

        Map<String, Object> map = SqlUtil.extractEventData(event);
        String hostId = (String) event.get(Constants.HOST);
        String deploymentInstanceId = (String) map.get("deploymentInstanceId");
        long newAggregateVersion = SqlUtil.getNewAggregateVersion(event);

        try (PreparedStatement statement = conn.prepareStatement(sql)) {
            // SET clause placeholders (9)
            int i = 1;
            statement.setString(i++, (String) map.get("serviceId"));
            statement.setString(i++, (String) map.get("ipAddress"));

            if (map.get("portNumber") != null) {
                statement.setInt(i++, (Integer) map.get("portNumber"));
            } else {
                statement.setNull(i++, Types.INTEGER);
            }

            statement.setString(i++, (String) map.get("systemEnv"));
            statement.setString(i++, (String) map.get("runtimeEnv"));
            statement.setObject(i++, UUID.fromString((String) map.get("pipelineId")));

            // IDM and OCC fields
            statement.setString(i++, (String) event.get(Constants.USER));
            statement.setObject(i++, OffsetDateTime.parse((String) event.get(CloudEventV1.TIME)));
            statement.setLong(i++, newAggregateVersion);

            // WHERE clause placeholders (3)
            statement.setObject(i++, UUID.fromString(hostId));
            statement.setObject(i++, UUID.fromString(deploymentInstanceId));
            statement.setLong(i++, newAggregateVersion);

            int count = statement.executeUpdate();

            if (count == 0) {
                // If 0 rows were updated, it's a valid idempotent outcome.
                logger.warn("Update skipped for DeploymentInstance with hostId {}, deploymentInstanceId {}. Record not found or a newer version already exists.",
                        hostId, deploymentInstanceId);
            }
        } catch (SQLException e) {
            logger.error("SQLException during updateDeploymentInstance for hostId {} deploymentInstanceId {}: {}",
                    hostId, deploymentInstanceId, e.getMessage(), e);
            throw e;
        } catch (Exception e) {
            logger.error("Exception during updateDeploymentInstance for hostId {} deploymentInstanceId {}: {}",
                    hostId, deploymentInstanceId, e.getMessage(), e);
            throw e;
        }
    }


    /**
     * Performs a soft delete on a deployment_instance_t record using an idempotent/monotonic pattern.
     * This method implements:
     * 1.  **Soft Delete:** Sets the 'active' flag to false.
     * 2.  **IDM:** Explicitly sets 'update_user' and 'update_ts' from the event metadata.
     * 3.  **OCC/Idempotency:** Uses `aggregate_version < ?` to ensure the operation only succeeds if it
     *     represents a new state, preventing re-processing of old events or concurrent conflicts.
     *
     * @param conn  The database connection.
     * @param event The event map containing the data and metadata for the operation.
     * @throws SQLException If a database access error occurs.
     * @throws Exception    For other generic errors.
     */
    @Override
    public void deleteDeploymentInstance(Connection conn, Map<String, Object> event) throws SQLException, Exception {
        // SQL statement updated to match the idempotent soft-delete pattern.
        final String sql =
                """
                UPDATE deployment_instance_t
                SET active = FALSE,
                    update_user = ?,
                    update_ts = ?,
                    aggregate_version = ?
                WHERE host_id = ?
                  AND deployment_instance_id = ?
                  AND aggregate_version < ?
                """;

        // Extract data and metadata from the event.
        Map<String, Object> map = SqlUtil.extractEventData(event);
        String hostId = (String) event.get(Constants.HOST);
        String deploymentInstanceId = (String) map.get("deploymentInstanceId");
        long newAggregateVersion = SqlUtil.getNewAggregateVersion(event);

        try (PreparedStatement statement = conn.prepareStatement(sql)) {
            // SET clause placeholders (3)
            int i = 1;
            // 1: update_user from event metadata
            statement.setString(i++, (String) event.get(Constants.USER));
            // 2: update_ts from event metadata
            statement.setObject(i++, OffsetDateTime.parse((String) event.get(CloudEventV1.TIME)));
            // 3: aggregate_version (the new version)
            statement.setLong(i++, newAggregateVersion);

            // WHERE clause placeholders (3)
            // 4: host_id from primary key
            statement.setObject(i++, UUID.fromString(hostId));
            // 5: deployment_instance_id from primary key
            statement.setObject(i++, UUID.fromString(deploymentInstanceId));
            // 6: aggregate_version < ? for OCC/Idempotency check
            statement.setLong(i++, newAggregateVersion);

            int count = statement.executeUpdate();
            if (count == 0) {
                // If 0 rows were updated, we log a warning and continue.
                // This is the expected behavior for an idempotent operation.
                logger.warn("Soft delete skipped for DeploymentInstance with hostId {}, deploymentInstanceId {}, newAggregateVersion {}. Record not found or a newer version already exists.",
                        hostId, deploymentInstanceId, newAggregateVersion);
            }
        } catch (SQLException e) {
            logger.error("SQLException during soft delete of DeploymentInstance for hostId {} deploymentInstanceId {}: {}",
                    hostId, deploymentInstanceId, e.getMessage(), e);
            throw e;
        } catch (Exception e) {
            logger.error("Exception during soft delete of DeploymentInstance for hostId {} deploymentInstanceId {}: {}",
                    hostId, deploymentInstanceId, e.getMessage(), e);
            throw e;
        }
    }


    @Override
    public Result<String> getDeploymentInstance(int offset, int limit, String filtersJson, String globalFilter, String sortingJson, String hostId) {
        Result<String> result = null;
        final Map<String, String> columnMap = new HashMap<>(Map.of(
                "hostId", "di.host_id",
                "instanceId", "di.instance_id",
                "instanceName", "i.instance_name",
                "deploymentInstanceId", "di.deployment_instance_id",
                "serviceId", "di.service_id",
                "ipAddress", "di.ip_address",
                "portNumber", "di.port_number",
                "systemEnv", "di.system_env",
                "runtimeEnv", "di.runtime_env",
                "pipelineId", "di.pipeline_id"
        ));
        columnMap.put("pipelineName", "p.pipeline_name");
        columnMap.put("pipelineVersion", "p.pipeline_version");
        columnMap.put("deployStatus", "di.deploy_status");
        columnMap.put("updateUser", "di.update_user");
        columnMap.put("updateTs", "di.update_ts");
        columnMap.put("aggregateVersion", "di.aggregate_version");
        columnMap.put("active", "di.active");

        List<Map<String, Object>> filters = parseJsonList(filtersJson);
        List<Map<String, Object>> sorting = parseJsonList(sortingJson);

        String s =
            """
            SELECT COUNT(*) OVER () AS total,
            di.host_id, di.instance_id, i.instance_name, di.deployment_instance_id, di.service_id, di.ip_address,
            di.port_number, di.system_env, di.runtime_env, di.pipeline_id, p.pipeline_name, p.pipeline_version,
            di.deploy_status, di.update_user, di.update_ts, di.aggregate_version, di.active
            FROM deployment_instance_t di
            INNER JOIN instance_t i ON i.instance_id = di.instance_id
            INNER JOIN pipeline_t p ON p.pipeline_id = di.pipeline_id
            WHERE di.host_id = ?
            """;

        List<Object> parameters = new ArrayList<>();
        parameters.add(UUID.fromString(hostId));
        String[] searchColumns = {"i.instance_name", "p.pipeline_name"};
        String sqlBuilder = s + dynamicFilter(Arrays.asList("di.host_id", "di.instance_id", "di.deployment_instance_id", "di.pipeline_id"), Arrays.asList(searchColumns), filters, columnMap, parameters) +
                globalFilter(globalFilter, searchColumns, parameters) +
                dynamicSorting("di.host_id, di.deployment_instance_id", sorting, columnMap) +
                "\nLIMIT ? OFFSET ?";

        parameters.add(limit);
        parameters.add(offset);

        int total = 0;
        List<Map<String, Object>> deploymentInstances = new ArrayList<>();

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
                    map.put("hostId", resultSet.getObject("host_id", UUID.class));
                    map.put("instanceId", resultSet.getObject("instance_id", UUID.class));
                    map.put("instanceName", resultSet.getString("instance_name"));
                    map.put("deploymentInstanceId", resultSet.getObject("deployment_instance_id", UUID.class));
                    map.put("serviceId", resultSet.getString("service_id"));
                    map.put("ipAddress", resultSet.getString("ip_address"));

                    // Handle nullable Integer for port_number from ResultSet
                    int portNum = resultSet.getInt("port_number");
                    if (resultSet.wasNull()) {
                        map.put("portNumber", null);
                    } else {
                        map.put("portNumber", portNum);
                    }

                    map.put("systemEnv", resultSet.getString("system_env"));
                    map.put("runtimeEnv", resultSet.getString("runtime_env"));
                    map.put("pipelineId", resultSet.getObject("pipeline_id", UUID.class));
                    map.put("pipelineName", resultSet.getString("pipeline_name"));
                    map.put("pipelineVersion", resultSet.getString("pipeline_version"));
                    map.put("deployStatus", resultSet.getString("deploy_status"));
                    map.put("updateUser", resultSet.getString("update_user"));
                    map.put("updateTs", resultSet.getObject("update_ts") != null ? resultSet.getObject("update_ts", OffsetDateTime.class) : null);
                    map.put("aggregateVersion", resultSet.getLong("aggregate_version"));
                    map.put("active", resultSet.getBoolean("active"));
                    deploymentInstances.add(map); // Changed variable name
                }
            }

            Map<String, Object> resultMap = new HashMap<>();
            resultMap.put("total", total);
            resultMap.put("deploymentInstances", deploymentInstances);
            result = Success.of(JsonMapper.toJson(resultMap));
        } catch (SQLException e) {
            logger.error("SQLException:", e);
            result = Failure.of(new Status(SQL_EXCEPTION, e.getMessage()));
        } catch (Exception e) { // Catching potential UUID parsing errors or other runtime issues
            logger.error("Exception:", e);
            result = Failure.of(new Status(GENERIC_EXCEPTION, e.getMessage()));
        }
        return result;
    }

    @Override
    public Result<String> getDeploymentInstancePipeline(String hostId, String instanceId, String systemEnv, String runtimeEnv) {
        final String s =
                """
                WITH InstanceProductVersion AS (
                    -- Get the product_version_id for the given instance_id
                    SELECT product_version_id
                    FROM instance_t
                    WHERE host_id = ? AND instance_id = ? -- Parameter 2: instance_id (e.g., '0196e658-8a14-72a8-802f-1fea0de8843a')
                ),
                ProductPipelines AS (
                    -- Get all pipeline_ids associated with that product_version_id
                    SELECT pvp.pipeline_id
                    FROM product_version_pipeline_t pvp
                    JOIN InstanceProductVersion ipv ON pvp.product_version_id = ipv.product_version_id
                )
                -- Query 1: Exact match for system_env AND runtime_env
                SELECT
                    p.*,
                    1 AS preference -- Higher preference for exact runtime_env match
                FROM pipeline_t p
                JOIN ProductPipelines pp ON p.pipeline_id = pp.pipeline_id
                WHERE p.system_env = ?     -- Parameter 3: system_env (e.g., 'VM Ubuntu 24.04' or 'Kubernetes')
                  AND p.runtime_env = ?    -- Parameter 4: runtime_env (e.g., 'OpenJDK 21')
                  AND p.current = true

                UNION ALL

                -- Query 2: Match for system_env AND runtime_env IS NULL
                SELECT
                    p.*,
                    2 AS preference -- Lower preference for NULL runtime_env
                FROM pipeline_t p
                JOIN ProductPipelines pp ON p.pipeline_id = pp.pipeline_id
                WHERE p.system_env = ?     -- Parameter 5: system_env (same as Parameter 2)
                  AND p.runtime_env IS NULL
                  AND p.current = true

                ORDER BY preference ASC, pipeline_id -- Ensure deterministic order if multiple pipelines have same preference
                LIMIT 1
                """;
        Result<String> result = null;
        String pipelineId = null;
        try (Connection connection = ds.getConnection();
             PreparedStatement preparedStatement = connection.prepareStatement(s)) {
            preparedStatement.setObject(1, UUID.fromString(hostId));
            preparedStatement.setObject(2, UUID.fromString(instanceId));
            preparedStatement.setString(3, systemEnv);
            preparedStatement.setString(4, runtimeEnv);
            preparedStatement.setString(5, systemEnv); // Reusing system_env for the second query
            try (ResultSet resultSet = preparedStatement.executeQuery()) {
                if (resultSet.next()) {
                    pipelineId = resultSet.getString("pipeline_id");
                }
            }
            if (pipelineId == null) {
                throw new SQLException("No pipeline found for the given parameters.");
            }
            result = Success.of(pipelineId);
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
    public Result<String> getDeploymentInstanceLabel(String hostId, String instanceId) {
        Result<String> result = null;
        String sql = instanceId == null ? "SELECT deployment_instance_id, service_id FROM deployment_instance_t WHERE host_id = ?" :
                "SELECT deployment_instance_id, service_id FROM deployment_instance_t WHERE host_id = ? AND instance_id = ?";
        List<Map<String, Object>> labels = new ArrayList<>();
        try (Connection connection = ds.getConnection();
             PreparedStatement preparedStatement = connection.prepareStatement(sql)) {
            preparedStatement.setObject(1, UUID.fromString(hostId));
            if (instanceId != null) {
                preparedStatement.setObject(2, UUID.fromString(instanceId));
            }
            try (ResultSet resultSet = preparedStatement.executeQuery()) {
                while (resultSet.next()) {
                    Map<String, Object> map = new HashMap<>();
                    map.put("id", resultSet.getString("deployment_instance_id"));
                    map.put("label", resultSet.getString("service_id"));
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

    /**
     * Creates or reactivates a deployment_t record using an idempotent UPSERT pattern.
     * This method implements:
     * 1.  **Idempotent Create/Update (Upsert):** Uses `INSERT ... ON CONFLICT DO UPDATE`. This will
     *     create the record on the first valid event, or update and reactivate it if a soft-deleted
     *     record with an older version already exists.
     * 2.  **IDM:** Explicitly sets 'update_user' and 'update_ts' from the event metadata.
     * 3.  **OCC/Monotonicity:** The `WHERE` clause in the `DO UPDATE` part ensures that an update
     *     only occurs if the incoming event's version is strictly greater than the existing record's version.
     *
     * @param conn  The database connection.
     * @param event The event map containing the data and metadata for the operation.
     * @throws SQLException If a database access error occurs.
     * @throws Exception    For other generic errors.
     */
    @Override
    public void createDeployment(Connection conn, Map<String, Object> event) throws SQLException, Exception {
        final String sql =
                """
                INSERT INTO deployment_t(host_id, deployment_id, deployment_instance_id,
                deployment_status, deployment_type, schedule_ts, update_user, update_ts, aggregate_version, active)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, TRUE)
                ON CONFLICT (host_id, deployment_id) DO UPDATE
                SET deployment_instance_id = EXCLUDED.deployment_instance_id,
                    deployment_status = EXCLUDED.deployment_status,
                    deployment_type = EXCLUDED.deployment_type,
                    schedule_ts = EXCLUDED.schedule_ts,
                    update_user = EXCLUDED.update_user,
                    update_ts = EXCLUDED.update_ts,
                    aggregate_version = EXCLUDED.aggregate_version,
                    active = TRUE
                WHERE deployment_t.aggregate_version < EXCLUDED.aggregate_version
                """;

        Map<String, Object> map = SqlUtil.extractEventData(event);
        String hostId = (String) event.get(Constants.HOST);
        String deploymentId = (String) map.get("deploymentId");
        long newAggregateVersion = SqlUtil.getNewAggregateVersion(event);

        try (PreparedStatement statement = conn.prepareStatement(sql)) {
            // Set parameters for the INSERT part of the statement
            int i = 1;
            statement.setObject(i++, UUID.fromString(hostId));
            statement.setObject(i++, UUID.fromString(deploymentId));
            statement.setObject(i++, UUID.fromString((String) map.get("deploymentInstanceId")));
            statement.setString(i++, (String) map.get("deploymentStatus"));
            statement.setString(i++, (String) map.get("deploymentType"));

            // Use schedule_ts from data if present, otherwise default to event time
            OffsetDateTime scheduleTs = map.get("scheduleTs") != null
                    ? OffsetDateTime.parse((String) map.get("scheduleTs"))
                    : OffsetDateTime.parse((String) event.get(CloudEventV1.TIME));
            statement.setObject(i++, scheduleTs);

            // IDM and OCC fields
            statement.setString(i++, (String) event.get(Constants.USER));
            statement.setObject(i++, OffsetDateTime.parse((String) event.get(CloudEventV1.TIME)));
            statement.setLong(i++, newAggregateVersion);

            int count = statement.executeUpdate();

            if (count == 0) {
                // A count of 0 is a valid, idempotent outcome.
                logger.warn("Creation/Reactivation skipped for Deployment with hostId {}, deploymentId {}. A newer or same version already exists.",
                        hostId, deploymentId);
            }
        } catch (SQLException e) {
            logger.error("SQLException during createDeployment for hostId {} deploymentId {}: {}",
                    hostId, deploymentId, e.getMessage(), e);
            throw e;
        } catch (Exception e) {
            logger.error("Exception during createDeployment for hostId {} deploymentId {}: {}",
                    hostId, deploymentId, e.getMessage(), e);
            throw e;
        }
    }

    /**
     * Updates a deployment_t record using an idempotent/monotonic pattern.
     * This method implements:
     * 1.  **Soft Delete Principle:** An update implicitly reactivates the record by setting `active = TRUE`.
     * 2.  **IDM:** Explicitly sets 'update_user' and 'update_ts' from the event metadata.
     * 3.  **OCC/Idempotency:** Uses `aggregate_version < ?` to ensure the operation only succeeds if it
     *     represents a new state, preventing re-processing of old events or concurrent conflicts.
     *
     * @param conn  The database connection.
     * @param event The event map containing the data and metadata for the operation.
     * @throws SQLException If a database access error occurs.
     * @throws Exception    For other generic errors.
     */
    @Override
    public void updateDeployment(Connection conn, Map<String, Object> event) throws SQLException, Exception {
        // SQL statement updated to match the idempotent update pattern.
        final String sql =
                """
                UPDATE deployment_t
                SET deployment_status = ?,
                    deployment_type = ?,
                    schedule_ts = ?,
                    update_user = ?,
                    update_ts = ?,
                    aggregate_version = ?,
                    active = TRUE
                WHERE host_id = ?
                  AND deployment_id = ?
                  AND aggregate_version < ?
                """;

        Map<String, Object> map = SqlUtil.extractEventData(event);
        String hostId = (String) event.get(Constants.HOST);
        String deploymentId = (String) map.get("deploymentId");
        long newAggregateVersion = SqlUtil.getNewAggregateVersion(event);

        try (PreparedStatement statement = conn.prepareStatement(sql)) {
            // SET clause placeholders (6)
            int i = 1;
            statement.setString(i++, (String) map.get("deploymentStatus"));
            statement.setString(i++, (String) map.get("deploymentType"));

            // Use schedule_ts from data if present, otherwise default to event time
            OffsetDateTime scheduleTs = map.get("scheduleTs") != null
                    ? OffsetDateTime.parse((String) map.get("scheduleTs"))
                    : OffsetDateTime.parse((String) event.get(CloudEventV1.TIME));
            statement.setObject(i++, scheduleTs);

            // IDM and OCC fields
            statement.setString(i++, (String) event.get(Constants.USER));
            statement.setObject(i++, OffsetDateTime.parse((String) event.get(CloudEventV1.TIME)));
            statement.setLong(i++, newAggregateVersion);

            // WHERE clause placeholders (3)
            statement.setObject(i++, UUID.fromString(hostId));
            statement.setObject(i++, UUID.fromString(deploymentId));
            statement.setLong(i++, newAggregateVersion);

            int count = statement.executeUpdate();

            if (count == 0) {
                // If 0 rows were updated, it's a valid idempotent outcome.
                logger.warn("Update skipped for Deployment with hostId {}, deploymentId {}. Record not found or a newer version already exists.",
                        hostId, deploymentId);
            }
        } catch (SQLException e) {
            logger.error("SQLException during updateDeployment for hostId {} deploymentId {}: {}",
                    hostId, deploymentId, e.getMessage(), e);
            throw e;
        } catch (Exception e) {
            logger.error("Exception during updateDeployment for hostId {} deploymentId {}: {}",
                    hostId, deploymentId, e.getMessage(), e);
            throw e;
        }
    }


    /**
     * Updates the platform_job_id of a deployment_t record using an idempotent/monotonic pattern.
     * This method implements:
     * 1.  **Soft Delete Principle:** An update implicitly reactivates the record by setting `active = TRUE`.
     * 2.  **IDM:** Explicitly sets 'update_user' and 'update_ts' from the event metadata.
     * 3.  **OCC/Idempotency:** Uses `aggregate_version < ?` to ensure the operation only succeeds if it
     *     represents a new state, preventing re-processing of old events or concurrent conflicts.
     *
     * @param conn  The database connection.
     * @param event The event map containing the data and metadata for the operation.
     * @throws SQLException If a database access error occurs.
     * @throws Exception    For other generic errors.
     */
    @Override
    public void updateDeploymentJobId(Connection conn, Map<String, Object> event) throws SQLException, Exception {
        // SQL statement updated to match the idempotent update pattern.
        final String sql =
                """
                UPDATE deployment_t
                SET platform_job_id = ?,
                    update_user = ?,
                    update_ts = ?,
                    aggregate_version = ?,
                    active = TRUE
                WHERE host_id = ?
                  AND deployment_id = ?
                  AND aggregate_version < ?
                """;

        Map<String, Object> map = SqlUtil.extractEventData(event);
        String hostId = (String) event.get(Constants.HOST);
        String deploymentId = (String) map.get("deploymentId");
        long newAggregateVersion = SqlUtil.getNewAggregateVersion(event);

        try (PreparedStatement statement = conn.prepareStatement(sql)) {
            // SET clause placeholders (4)
            int i = 1;
            statement.setString(i++, (String) map.get("platformJobId"));

            // IDM and OCC fields
            statement.setString(i++, (String) event.get(Constants.USER));
            statement.setObject(i++, OffsetDateTime.parse((String) event.get(CloudEventV1.TIME)));
            statement.setLong(i++, newAggregateVersion);

            // WHERE clause placeholders (3)
            statement.setObject(i++, UUID.fromString(hostId));
            statement.setObject(i++, UUID.fromString(deploymentId));
            statement.setLong(i++, newAggregateVersion);

            int count = statement.executeUpdate();

            if (count == 0) {
                // If 0 rows were updated, it's a valid idempotent outcome.
                logger.warn("Update of platformJobId skipped for Deployment with hostId {}, deploymentId {}. Record not found or a newer version already exists.",
                        hostId, deploymentId);
                // We might not want to send a success notification if nothing happened.
                // Depending on requirements, you could insert a "skipped" notification or do nothing.
                notificationService.insertNotification(event, false, "Update skipped: A newer or same version already exists.");
            } else {
                notificationService.insertNotification(event, true, null);
            }
        } catch (SQLException e) {
            logger.error("SQLException during updateDeploymentJobId for hostId {} deploymentId {}: {}",
                    hostId, deploymentId, e.getMessage(), e);
            notificationService.insertNotification(event, false, e.getMessage());
            throw e;
        } catch (Exception e) {
            logger.error("Exception during updateDeploymentJobId for hostId {} deploymentId {}: {}",
                    hostId, deploymentId, e.getMessage(), e);
            notificationService.insertNotification(event, false, e.getMessage());
            throw e;
        }
    }


    /**
     * Updates the deployment_status of a deployment_t record using an idempotent/monotonic pattern.
     * This method implements:
     * 1.  **Soft Delete Principle:** An update implicitly reactivates the record by setting `active = TRUE`.
     * 2.  **IDM:** Explicitly sets 'update_user' and 'update_ts' from the event metadata.
     * 3.  **OCC/Idempotency:** Uses `aggregate_version < ?` to ensure the operation only succeeds if it
     *     represents a new state, preventing re-processing of old events or concurrent conflicts.
     *
     * @param conn  The database connection.
     * @param event The event map containing the data and metadata for the operation.
     * @throws SQLException If a database access error occurs.
     * @throws Exception    For other generic errors.
     */
    @Override
    public void updateDeploymentStatus(Connection conn, Map<String, Object> event) throws SQLException, Exception {
        // SQL statement updated to match the idempotent update pattern.
        final String sql =
                """
                UPDATE deployment_t
                SET deployment_status = ?,
                    update_user = ?,
                    update_ts = ?,
                    aggregate_version = ?,
                    active = TRUE
                WHERE host_id = ?
                  AND deployment_id = ?
                  AND aggregate_version < ?
                """;

        Map<String, Object> map = SqlUtil.extractEventData(event);
        String hostId = (String) event.get(Constants.HOST);
        String deploymentId = (String) map.get("deploymentId");
        long newAggregateVersion = SqlUtil.getNewAggregateVersion(event);

        try (PreparedStatement statement = conn.prepareStatement(sql)) {
            // SET clause placeholders (4)
            int i = 1;
            statement.setString(i++, (String) map.get("deploymentStatus"));

            // IDM and OCC fields
            statement.setString(i++, (String) event.get(Constants.USER));
            statement.setObject(i++, OffsetDateTime.parse((String) event.get(CloudEventV1.TIME)));
            statement.setLong(i++, newAggregateVersion);

            // WHERE clause placeholders (3)
            statement.setObject(i++, UUID.fromString(hostId));
            statement.setObject(i++, UUID.fromString(deploymentId));
            statement.setLong(i++, newAggregateVersion);

            int count = statement.executeUpdate();

            if (count == 0) {
                // If 0 rows were updated, it's a valid idempotent outcome.
                logger.warn("Update of deploymentStatus skipped for Deployment with hostId {}, deploymentId {}. Record not found or a newer version already exists.",
                        hostId, deploymentId);
                // Send a "skipped" or "failed" notification as the desired state was not actively set by this event.
                notificationService.insertNotification(event, false, "Update skipped: A newer or same version already exists.");
            } else {
                notificationService.insertNotification(event, true, null);
            }
        } catch (SQLException e) {
            logger.error("SQLException during updateDeploymentStatus for hostId {} deploymentId {}: {}",
                    hostId, deploymentId, e.getMessage(), e);
            notificationService.insertNotification(event, false, e.getMessage());
            throw e;
        } catch (Exception e) {
            logger.error("Exception during updateDeploymentStatus for hostId {} deploymentId {}: {}",
                    hostId, deploymentId, e.getMessage(), e);
            notificationService.insertNotification(event, false, e.getMessage());
            throw e;
        }
    }

    /**
     * Performs a soft delete on a deployment_t record using an idempotent/monotonic pattern.
     * This method implements:
     * 1.  **Soft Delete:** Sets the 'active' flag to false.
     * 2.  **IDM:** Explicitly sets 'update_user' and 'update_ts' from the event metadata.
     * 3.  **OCC/Idempotency:** Uses `aggregate_version < ?` to ensure the operation only succeeds if it
     *     represents a new state, preventing re-processing of old events or concurrent conflicts.
     *
     * @param conn  The database connection.
     * @param event The event map containing the data and metadata for the operation.
     * @throws SQLException If a database access error occurs.
     * @throws Exception    For other generic errors.
     */
    @Override
    public void deleteDeployment(Connection conn, Map<String, Object> event) throws SQLException, Exception {
        // SQL statement updated to match the idempotent soft-delete pattern.
        final String sql =
                """
                UPDATE deployment_t
                SET active = FALSE,
                    update_user = ?,
                    update_ts = ?,
                    aggregate_version = ?
                WHERE host_id = ?
                  AND deployment_id = ?
                  AND aggregate_version < ?
                """;

        // Extract data and metadata from the event.
        Map<String, Object> map = SqlUtil.extractEventData(event);
        String hostId = (String) event.get(Constants.HOST);
        String deploymentId = (String) map.get("deploymentId");
        long newAggregateVersion = SqlUtil.getNewAggregateVersion(event);

        try (PreparedStatement statement = conn.prepareStatement(sql)) {
            // SET clause placeholders (3)
            int i = 1;
            // 1: update_user from event metadata
            statement.setString(i++, (String) event.get(Constants.USER));
            // 2: update_ts from event metadata
            statement.setObject(i++, OffsetDateTime.parse((String) event.get(CloudEventV1.TIME)));
            // 3: aggregate_version (the new version)
            statement.setLong(i++, newAggregateVersion);

            // WHERE clause placeholders (3)
            // 4: host_id from primary key
            statement.setObject(i++, UUID.fromString(hostId));
            // 5: deployment_id from primary key
            statement.setObject(i++, UUID.fromString(deploymentId));
            // 6: aggregate_version < ? for OCC/Idempotency check
            statement.setLong(i++, newAggregateVersion);

            int count = statement.executeUpdate();
            if (count == 0) {
                // If 0 rows were updated, we log a warning and continue.
                // This indicates the record was already deleted, never existed, or a newer version is already in the database.
                // This is the expected behavior for an idempotent operation.
                logger.warn("Soft delete skipped for Deployment with hostId {}, deploymentId {}, newAggregateVersion {}. Record not found or a newer version already exists.",
                        hostId, deploymentId, newAggregateVersion);
            }
        } catch (SQLException e) {
            logger.error("SQLException during soft delete of Deployment for hostId {} deploymentId {}: {}",
                    hostId, deploymentId, e.getMessage(), e);
            throw e;
        } catch (Exception e) {
            logger.error("Exception during soft delete of Deployment for hostId {} deploymentId {}: {}",
                    hostId, deploymentId, e.getMessage(), e);
            throw e;
        }
    }

    @Override
    public Result<String> getDeployment(int offset, int limit, String filtersJson, String globalFilter, String sortingJson, String hostId) {
        Result<String> result = null;
        final Map<String, String> columnMap = new HashMap<>(Map.of(
                "hostId", "d.host_id",
                "deploymentId", "d.deployment_id",
                "deploymentInstanceId", "d.deployment_instance_id",
                "serviceId", "di.service_id",
                "deploymentStatus", "d.deployment_status",
                "deploymentType", "d.deployment_type",
                "scheduleTs", "d.schedule_ts",
                "platformJobId", "d.platform_job_id",
                "updateUser", "d.update_user",
                "updateTs", "d.update_ts"
        ));
        columnMap.put("aggregateVersion", "d.aggregate_version");
        columnMap.put("updateUser", "d.active");

        List<Map<String, Object>> filters = parseJsonList(filtersJson);
        List<Map<String, Object>> sorting = parseJsonList(sortingJson);

        String s =
            """
            SELECT COUNT(*) OVER () AS total,
            d.host_id, d.deployment_id, d.deployment_instance_id, di.service_id, d.deployment_status,
            d.deployment_type, d.schedule_ts, d.platform_job_id, d.update_user,
            d.update_ts, d.aggregate_version, d.active
            FROM deployment_t d
            INNER JOIN deployment_instance_t di ON di.deployment_instance_id = d.deployment_instance_id
            WHERE d.host_id = ?
            """;

        List<Object> parameters = new ArrayList<>();
        parameters.add(UUID.fromString(hostId));

        String[] searchColumns = {"di.service_id"};
        String sqlBuilder = s + dynamicFilter(Arrays.asList("d.host_id", "d.deployment_id", "d.deployment_instance_id"), Arrays.asList(searchColumns), filters, columnMap, parameters) +
                globalFilter(globalFilter, searchColumns, parameters) +
                dynamicSorting("d.deployment_id", sorting, columnMap) +
                "\nLIMIT ? OFFSET ?";

        parameters.add(limit);
        parameters.add(offset);
        int total = 0;
        List<Map<String, Object>> deployments = new ArrayList<>();

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
                    map.put("hostId", resultSet.getObject("host_id", UUID.class));
                    map.put("deploymentId", resultSet.getObject("deployment_id", UUID.class));
                    map.put("deploymentInstanceId", resultSet.getObject("deployment_instance_id", UUID.class));
                    map.put("serviceId", resultSet.getString("service_id"));
                    map.put("deploymentStatus", resultSet.getString("deployment_status"));
                    map.put("deploymentType", resultSet.getString("deployment_type"));
                    map.put("scheduleTs", resultSet.getObject("schedule_ts") != null ? resultSet.getObject("schedule_ts", OffsetDateTime.class) : null);
                    map.put("platformJobId", resultSet.getString("platform_job_id"));
                    map.put("updateUser", resultSet.getString("update_user"));
                    map.put("updateTs", resultSet.getObject("update_ts") != null ? resultSet.getObject("update_ts", OffsetDateTime.class) : null);
                    map.put("aggregateVersion", resultSet.getLong("aggregate_version"));
                    map.put("active", resultSet.getBoolean("active"));
                    deployments.add(map);
                }
            }

            Map<String, Object> resultMap = new HashMap<>();
            resultMap.put("total", total);
            resultMap.put("deployments", deployments);
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
}
