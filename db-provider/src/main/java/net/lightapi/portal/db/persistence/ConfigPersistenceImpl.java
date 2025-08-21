package net.lightapi.portal.db.persistence;

import com.networknt.config.JsonMapper;
import com.networknt.monad.Failure;
import com.networknt.monad.Result;
import com.networknt.monad.Success;
import com.networknt.status.Status;
import com.networknt.utility.Constants;
import com.networknt.utility.UuidUtil;
import io.cloudevents.core.v1.CloudEventV1;
import net.lightapi.portal.PortalConstants;
import net.lightapi.portal.db.ConcurrencyException;
import net.lightapi.portal.db.PortalDbProvider;
import net.lightapi.portal.db.util.NotificationService;
import net.lightapi.portal.db.util.SqlUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.sql.Array;
import java.time.OffsetDateTime;
import java.util.*;

import static com.networknt.db.provider.SqlDbStartupHook.ds;

public class ConfigPersistenceImpl implements ConfigPersistence {
    private static final Logger logger = LoggerFactory.getLogger(ConfigPersistenceImpl.class);
    private static final String SQL_EXCEPTION = PortalDbProvider.SQL_EXCEPTION;
    private static final String GENERIC_EXCEPTION = PortalDbProvider.GENERIC_EXCEPTION;
    private static final String OBJECT_NOT_FOUND = PortalDbProvider.OBJECT_NOT_FOUND;

    private final NotificationService notificationService;

    public ConfigPersistenceImpl(NotificationService notificationService) {
        this.notificationService = notificationService;
    }

    @Override
    public void createConfig(Connection conn, Map<String, Object> event) throws SQLException, Exception {
        final String sql =
                """
                INSERT INTO config_t(config_id, config_name, config_phase, config_type, light4j_version,
                class_path, config_desc, update_user, update_ts, aggregate_version)
                VALUES (?, ?, ?, ?, ?,  ?, ?, ?, ?, ?)
                """;
        Map<String, Object> map = (Map<String, Object>)event.get(PortalConstants.DATA);
        String configId = (String)map.get("configId"); // For logging/exceptions
        long newAggregateVersion = SqlUtil.getNewAggregateVersion(event);

        try (PreparedStatement statement = conn.prepareStatement(sql)) {
            statement.setObject(1, UUID.fromString(configId));
            statement.setString(2, (String)map.get("configName"));
            statement.setString(3, (String)map.get("configPhase"));
            statement.setString(4, (String)map.get("configType"));

            if (map.containsKey("light4jVersion")) {
                statement.setString(5, (String) map.get("light4jVersion"));
            } else {
                statement.setNull(5, Types.VARCHAR);
            }

            if (map.containsKey("classPath")) {
                statement.setString(6, (String) map.get("classPath"));
            } else {
                statement.setNull(6, Types.VARCHAR);
            }

            if (map.containsKey("configDesc")) {
                statement.setString(7, (String) map.get("configDesc"));
            } else {
                statement.setNull(7, Types.VARCHAR);
            }
            statement.setString(8, (String)event.get(Constants.USER));
            statement.setObject(9, OffsetDateTime.parse((String)event.get(CloudEventV1.TIME)));
            statement.setLong(10, newAggregateVersion);

            int count = statement.executeUpdate();
            if (count == 0) {
                throw new SQLException("Failed to insert the config with id " + configId + " with aggregateVersion " + newAggregateVersion + ".");
            }
        } catch (SQLException e) {
            logger.error("SQLException during createConfig for id {} aggregateVersion {}: {}", configId, newAggregateVersion, e.getMessage(), e);
            throw e;
        } catch (Exception e) {
            logger.error("Exception during createConfig for id {} aggregateVersion {}: {}", configId, newAggregateVersion, e.getMessage(), e);
            throw e;
        }
    }

    private boolean queryConfigExists(Connection conn, String configId) throws SQLException {
        final String sql =
                """
                SELECT COUNT(*) FROM config_t WHERE config_id = ?
                """;
        try (PreparedStatement pst = conn.prepareStatement(sql)) {
            pst.setObject(1, UUID.fromString(configId));
            try (ResultSet rs = pst.executeQuery()) {
                return rs.next() && rs.getInt(1) > 0;
            }
        }
    }

    @Override
    public void updateConfig(Connection conn, Map<String, Object> event) throws SQLException, Exception {
        final String sql =
                """
                UPDATE config_t SET config_name = ?, config_phase = ?, config_type = ?, light4j_version = ?,
                class_path = ?, config_desc = ?, update_user = ?, update_ts = ?, aggregate_version = ?
                WHERE config_id = ? AND aggregate_version = ?
                """;
        Map<String, Object> map = (Map<String, Object>)event.get(PortalConstants.DATA);
        String configId = (String)map.get("configId");
        long oldAggregateVersion = SqlUtil.getOldAggregateVersion(event);
        long newAggregateVersion = SqlUtil.getNewAggregateVersion(event);

        try (PreparedStatement statement = conn.prepareStatement(sql)) {
            statement.setString(1, (String)map.get("configName"));
            statement.setString(2, (String)map.get("configPhase"));
            statement.setString(3, (String)map.get("configType"));

            if (map.containsKey("light4jVersion")) {
                statement.setString(4, (String) map.get("light4jVersion"));
            } else {
                statement.setNull(4, Types.VARCHAR);
            }

            if (map.containsKey("classPath")) {
                statement.setString(5, (String) map.get("classPath"));
            } else {
                statement.setNull(5, Types.VARCHAR);
            }

            if (map.containsKey("configDesc")) {
                statement.setString(6, (String) map.get("configDesc"));
            } else {
                statement.setNull(6, Types.VARCHAR);
            }
            statement.setString(7, (String)event.get(Constants.USER));
            statement.setObject(8, OffsetDateTime.parse((String)event.get(CloudEventV1.TIME)));
            statement.setLong(9, newAggregateVersion);
            statement.setObject(10, UUID.fromString(configId));
            statement.setLong(11, oldAggregateVersion);

            int count = statement.executeUpdate();
            if (count == 0) {
                if (queryConfigExists(conn, configId)) {
                    throw new ConcurrencyException("Optimistic concurrency conflict for config " + configId + ". Expected version " + oldAggregateVersion + " but found a different version " + newAggregateVersion + ".");
                } else {
                    throw new SQLException("No record found to update for config " + configId + ".");
                }
            }
        } catch (SQLException e) {
            logger.error("SQLException during updateConfig for id {} (old: {}) -> (new: {}): {}", configId, oldAggregateVersion, newAggregateVersion, e.getMessage(), e);
            throw e;
        } catch (Exception e) {
            logger.error("Exception during updateConfig for id {} (old: {}) -> (new: {}): {}", configId, oldAggregateVersion, newAggregateVersion, e.getMessage(), e);
            throw e;
        }
    }

    @Override
    public void deleteConfig(Connection conn, Map<String, Object> event) throws SQLException, Exception {
        final String sql = "DELETE FROM config_t WHERE config_id = ? AND aggregate_version = ?";
        Map<String, Object> map = (Map<String, Object>)event.get(PortalConstants.DATA);
        String configId = (String)map.get("configId");
        long oldAggregateVersion = SqlUtil.getOldAggregateVersion(event);

        try (PreparedStatement statement = conn.prepareStatement(sql)) {
            statement.setObject(1, UUID.fromString(configId));
            statement.setLong(2, oldAggregateVersion);
            int count = statement.executeUpdate();
            if (count == 0) {
                if (queryConfigExists(conn, configId)) {
                    throw new ConcurrencyException("Optimistic concurrency conflict during deleteConfig for config " + configId + " aggregateVersion " + oldAggregateVersion + " but found a different version or already updated.");
                } else {
                    throw new SQLException("No record found during deleteConfig for config " + configId + ". It might have been already deleted.");
                }
            }
        } catch (SQLException e) {
            logger.error("SQLException during deleteConfig for id {}: {}", configId, e.getMessage(), e);
            throw e;
        } catch (Exception e) {
            logger.error("Exception during deleteConfig for id {}: {}", configId, e.getMessage(), e);
            throw e;
        }
    }

    @Override
    public Result<String> getConfig(int offset, int limit, String configId, String configName, String configPhase,
                                    String configType, String light4jVersion, String classPath, String configDesc) {
        Result<String> result = null;
        String s =
                """
                SELECT COUNT(*) OVER () AS total,
                config_id, config_name, config_phase, config_type, light4j_version,
                class_path, config_desc, update_user, update_ts, aggregate_version
                FROM config_t
                WHERE 1=1
                """;

        StringBuilder sqlBuilder = new StringBuilder(s);
        List<Object> parameters = new ArrayList<>();

        StringBuilder whereClause = new StringBuilder();
        SqlUtil.addCondition(whereClause, parameters, "config_id", configId != null ? UUID.fromString(configId) : null);
        SqlUtil.addCondition(whereClause, parameters, "config_name", configName);
        SqlUtil.addCondition(whereClause, parameters, "config_phase", configPhase);
        SqlUtil.addCondition(whereClause, parameters, "config_type", configType);
        SqlUtil.addCondition(whereClause, parameters, "light4j_version", light4jVersion);
        SqlUtil.addCondition(whereClause, parameters, "class_path", classPath);
        SqlUtil.addCondition(whereClause, parameters, "config_desc", configDesc);

        if (!whereClause.isEmpty()) {
            sqlBuilder.append("AND ").append(whereClause);
        }

        sqlBuilder.append(" ORDER BY config_id\n" +
                "LIMIT ? OFFSET ?");

        parameters.add(limit);
        parameters.add(offset);

        String sql = sqlBuilder.toString();
        int total = 0;
        List<Map<String, Object>> configs = new ArrayList<>();

        try (Connection connection = ds.getConnection();
             PreparedStatement preparedStatement = connection.prepareStatement(sql)) {

            for (int i = 0; i < parameters.size(); i++) {
                preparedStatement.setObject(i + 1, parameters.get(i));
            }

            boolean isFirstRow = true;
            try (ResultSet resultSet = preparedStatement.executeQuery()) {
                while (resultSet.next()) {
                    Map<String, Object> map = new HashMap<>();
                    if (isFirstRow) {
                        total = resultSet.getInt("total");
                        isFirstRow = false;
                    }
                    map.put("configId", resultSet.getObject("config_id", UUID.class));
                    map.put("configName", resultSet.getString("config_name"));
                    map.put("configPhase", resultSet.getString("config_phase"));
                    map.put("configType", resultSet.getString("config_type"));
                    map.put("light4jVersion", resultSet.getString("light4j_version"));
                    map.put("classPath", resultSet.getString("class_path"));
                    map.put("configDesc", resultSet.getString("config_desc"));
                    map.put("updateUser", resultSet.getString("update_user"));
                    map.put("updateTs", resultSet.getObject("update_ts") != null ? resultSet.getObject("update_ts", OffsetDateTime.class) : null);
                    map.put("aggregateVersion", resultSet.getLong("aggregate_version"));

                    configs.add(map);
                }
            }

            Map<String, Object> resultMap = new HashMap<>();
            resultMap.put("total", total);
            resultMap.put("configs", configs);
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
    public Result<String> queryConfigById(String configId) {
        final String queryConfigById =
                """
                SELECT config_id, config_name, config_phase, config_type, light4j_version,
                class_path, config_desc, update_user, update_ts, aggregate_version
                FROM config_t WHERE config_id = ?
                """;
        Result<String> result;
        Map<String, Object> config = new HashMap<>();

        try (Connection conn = ds.getConnection();
             PreparedStatement statement = conn.prepareStatement(queryConfigById)) {

            statement.setObject(1, UUID.fromString(configId));

            try (ResultSet resultSet = statement.executeQuery()) {
                if (resultSet.next()) {
                    config.put("configId", resultSet.getObject("config_id", UUID.class));
                    config.put("configName", resultSet.getString("config_name"));
                    config.put("configPhase", resultSet.getString("config_phase"));
                    config.put("configType", resultSet.getString("config_type"));
                    config.put("light4jVersion", resultSet.getString("light4j_version"));
                    config.put("classPath", resultSet.getString("class_path"));
                    config.put("configDesc", resultSet.getString("config_desc"));
                    config.put("updateUser", resultSet.getString("update_user"));
                    config.put("updateTs", resultSet.getObject("update_ts") != null ? resultSet.getObject("update_ts", OffsetDateTime.class) : null);
                    config.put("aggregateVersion", resultSet.getLong("aggregate_version"));
                    result = Success.of(JsonMapper.toJson(config));
                } else {
                    result = Failure.of(new Status(OBJECT_NOT_FOUND, "config", configId));
                }
            }

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
    public Result<String> getConfigIdLabel() {
        final String sql = "SELECT config_id, config_name FROM config_t ORDER BY config_name";
        Result<String> result;
        try (final Connection conn = ds.getConnection()) {
            List<Map<String, Object>> list = new ArrayList<>();
            try (PreparedStatement statement = conn.prepareStatement(sql)) {
                try (ResultSet resultSet = statement.executeQuery()) {
                    while (resultSet.next()) {
                        Map<String, Object> map = new HashMap<>();
                        map.put("id", resultSet.getObject("config_id", UUID.class));
                        map.put("label", resultSet.getString("config_name"));
                        list.add(map);
                    }
                }
            }
            if (list.isEmpty())
                result = Failure.of(new Status(OBJECT_NOT_FOUND, "configId", "any"));
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
    public Result<String> getConfigIdApiAppLabel(String resourceType) {
        final String sql =
                """
                SELECT distinct c.config_id, c.config_name
                FROM config_t c, config_property_t p
                WHERE c.config_id = p.config_id
                AND (p.value_type = 'map' or p.value_type = 'list')
                AND p.resource_type LIKE ?
                ORDER BY config_name
                """;
        Result<String> result;
        try (final Connection conn = ds.getConnection()) {
            List<Map<String, Object>> list = new ArrayList<>();
            try (PreparedStatement statement = conn.prepareStatement(sql)) {
                statement.setString(1, "%" + resourceType + "%");
                try (ResultSet resultSet = statement.executeQuery()) {
                    while (resultSet.next()) {
                        Map<String, Object> map = new HashMap<>();
                        map.put("id", resultSet.getString("config_id"));
                        map.put("label", resultSet.getString("config_name"));
                        list.add(map);
                    }
                }
            }
            if (list.isEmpty())
                result = Failure.of(new Status(OBJECT_NOT_FOUND, "configId", "any"));
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
    public Result<String> getPropertyIdLabel(String configId) {
        final String sql = "SELECT property_id, property_name FROM config_property_t WHERE config_id = ? ORDER BY display_order";
        Result<String> result;
        try (final Connection conn = ds.getConnection()) {
            List<Map<String, Object>> list = new ArrayList<>();
            try (PreparedStatement statement = conn.prepareStatement(sql)) {
                statement.setObject(1, UUID.fromString(configId));
                try (ResultSet resultSet = statement.executeQuery()) {
                    while (resultSet.next()) {
                        Map<String, Object> map = new HashMap<>();
                        map.put("id", resultSet.getString("property_id"));
                        map.put("label", resultSet.getString("property_name"));
                        list.add(map);
                    }
                }
            }
            if (list.isEmpty())
                result = Failure.of(new Status(OBJECT_NOT_FOUND, "config property", configId));
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
    public Result<String> getPropertyIdApiAppLabel(String configId, String resourceType) {
        final String sql =
                """
                SELECT property_id, property_name
                FROM config_property_t
                WHERE config_id = ?
                AND (value_type = 'map' or value_type = 'list')
                AND resource_type LIKE ?
                ORDER BY display_order
                """;
        Result<String> result;
        try (final Connection conn = ds.getConnection()) {
            List<Map<String, Object>> list = new ArrayList<>();
            try (PreparedStatement statement = conn.prepareStatement(sql)) {
                statement.setObject(1, UUID.fromString(configId));
                statement.setString(2, "%" + resourceType + "%");
                try (ResultSet resultSet = statement.executeQuery()) {
                    while (resultSet.next()) {
                        Map<String, Object> map = new HashMap<>();
                        map.put("id", resultSet.getString("property_id"));
                        map.put("label", resultSet.getString("property_name"));
                        list.add(map);
                    }
                }
            }
            if (list.isEmpty())
                result = Failure.of(new Status(OBJECT_NOT_FOUND, "config property", configId));
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
    public void createConfigProperty(Connection conn, Map<String, Object> event) throws SQLException, Exception {
        final String sql =
                """
                INSERT INTO config_property_t (config_id, property_id, property_name, property_type,
                property_value, resource_type, value_type, display_order, required, property_desc,
                light4j_version, update_user, update_ts, aggregate_version)
                VALUES (?, ?, ?, ?, ?,  ?, ?, ?, ?, ?,  ?, ?, ?, ?)
                """;
        Map<String, Object> map = (Map<String, Object>)event.get(PortalConstants.DATA);
        String configId = (String)map.get("configId");
        String propertyId = (String)map.get("propertyId");
        String propertyName = (String)map.get("propertyName");
        long newAggregateVersion = SqlUtil.getNewAggregateVersion(event);

        try (PreparedStatement statement = conn.prepareStatement(sql)) {
            statement.setObject(1, UUID.fromString(configId));
            statement.setObject(2, UUID.fromString(propertyId));
            statement.setString(3, propertyName);
            statement.setString(4, (String)map.get("propertyType"));

            // Handle property_value (required)
            if (map.containsKey("propertyValue")) {
                statement.setString(5, (String) map.get("propertyValue"));
            } else {
                statement.setNull(5, Types.VARCHAR); // Or throw exception if it's truly required, but DB default is not set.
            }

            // Handle resource_type (optional)
            if (map.containsKey("resourceType")) {
                statement.setString(6, (String) map.get("resourceType"));
            } else {
                statement.setString(6, "none");
            }

            // Handle value_type (optional)
            if (map.containsKey("valueType")) {
                statement.setString(7, (String) map.get("valueType"));
            } else {
                statement.setNull(7, Types.VARCHAR);
            }

            // Handle display_order (optional)
            if (map.containsKey("displayOrder")) {
                statement.setInt(8, Integer.parseInt(map.get("displayOrder").toString()));
            } else {
                statement.setNull(8, Types.INTEGER);
            }

            // Handle required (optional)
            if (map.containsKey("required")) {
                statement.setBoolean(9, Boolean.parseBoolean(map.get("required").toString()));
            } else {
                statement.setBoolean(9, false);
            }

            // Handle property_desc (optional)
            if (map.containsKey("propertyDesc")) {
                statement.setString(10, (String) map.get("propertyDesc"));
            } else {
                statement.setNull(10, Types.VARCHAR);
            }

            // Handle light4j_version (optional)
            if(map.containsKey("light4jVersion")) {
                statement.setString(11, (String) map.get("light4jVersion"));
            } else {
                statement.setNull(11, Types.VARCHAR);
            }

            statement.setString(12, (String)event.get(Constants.USER));
            statement.setObject(13, OffsetDateTime.parse((String)event.get(CloudEventV1.TIME)));
            statement.setLong(14, newAggregateVersion);

            int count = statement.executeUpdate();
            if (count == 0) {
                throw new SQLException("Failed to insert the config property with id " + propertyId + " and aggregateVersion " + newAggregateVersion + ".");
            }
        } catch (SQLException e) {
            logger.error("SQLException during createConfigProperty for propertyId {} aggregateVersion {}: {}", propertyId, newAggregateVersion, e.getMessage(), e);
            throw e;
        } catch (Exception e) {
            logger.error("Exception during createConfigProperty for propertyId {} aggregateVersion {}: {}", propertyId, newAggregateVersion, e.getMessage(), e);
            throw e;
        }
    }

    private boolean queryConfigPropertyExists(Connection conn, String propertyId) throws SQLException {
        final String sql =
                """
                SELECT COUNT(*) FROM config_property_t WHERE property_id = ?
                """;
        try (PreparedStatement pst = conn.prepareStatement(sql)) {
            pst.setObject(1, UUID.fromString(propertyId));
            try (ResultSet rs = pst.executeQuery()) {
                return rs.next() && rs.getInt(1) > 0;
            }
        }
    }

    @Override
    public void updateConfigProperty(Connection conn, Map<String, Object> event) throws SQLException, Exception {
        final String sql =
                """
                UPDATE config_property_t SET property_name = ?, property_type = ?, property_value = ?,
                resource_type = ?, value_type = ?, display_order = ?, required = ?, property_desc = ?,
                light4j_version = ?, update_user = ?, update_ts = ?, aggregate_version = ?
                WHERE config_id = ? AND property_id = ? AND aggregate_version = ?
                """;

        Map<String, Object> map = (Map<String, Object>)event.get(PortalConstants.DATA);
        String configId = (String)map.get("configId");
        String propertyId = (String)map.get("propertyId");
        long oldAggregateVersion = SqlUtil.getOldAggregateVersion(event);
        long newAggregateVersion = SqlUtil.getNewAggregateVersion(event);

        try (PreparedStatement statement = conn.prepareStatement(sql)) {
            // Set the update values from the event and the parsed JSON
            statement.setString(1, (String)map.get("propertyName"));
            statement.setString(2, (String)map.get("propertyType"));

            // Handle property_value (optional in update, but check in map)
            if (map.containsKey("propertyValue")) {
                statement.setString(3, (String) map.get("propertyValue"));
            } else {
                statement.setNull(3, Types.VARCHAR); // Or keep existing value if you prefer
            }

            // Handle resource_type
            if (map.containsKey("resourceType")) {
                statement.setString(4, (String) map.get("resourceType"));
            } else {
                statement.setNull(4, Types.VARCHAR); // Could set to 'none' or a DB default, or keep existing.
            }

            // Handle value_type
            if (map.containsKey("valueType")) {
                statement.setString(5, (String) map.get("valueType"));
            } else {
                statement.setNull(5, Types.VARCHAR);
            }

            // Handle display_order
            if (map.containsKey("displayOrder")) {
                statement.setInt(6, Integer.parseInt(map.get("displayOrder").toString()));
            } else {
                statement.setNull(6, Types.INTEGER);
            }

            // Handle required
            if (map.containsKey("required")) {
                statement.setBoolean(7, Boolean.parseBoolean(map.get("required").toString()));
            } else {
                statement.setNull(7, Types.BOOLEAN); //or statement.setBoolean(7, false);
            }

            // Handle property_desc
            if (map.containsKey("propertyDesc")) {
                statement.setString(8, (String) map.get("propertyDesc"));
            } else {
                statement.setNull(8, Types.VARCHAR);
            }

            // Handle light4j_version
            if (map.containsKey("light4jVersion")) {
                statement.setString(9, (String) map.get("light4jVersion"));
            } else {
                statement.setNull(9, Types.VARCHAR);
            }

            statement.setString(10, (String)event.get(Constants.USER));
            statement.setObject(11, OffsetDateTime.parse((String)event.get(CloudEventV1.TIME)));
            statement.setLong(12, newAggregateVersion);

            // WHERE clause: Crucial for updating the correct row!
            statement.setObject(13, UUID.fromString(configId));
            statement.setObject(14, UUID.fromString(propertyId));
            statement.setLong(15, oldAggregateVersion);


            int count = statement.executeUpdate();
            if (count == 0) {
                if (queryConfigPropertyExists(conn, propertyId)) {
                    throw new ConcurrencyException("Optimistic concurrency conflict for config property " + propertyId + ". Expected version " + oldAggregateVersion + " but found a different version " + newAggregateVersion + ".");
                } else {
                    throw new SQLException("No record found to update for config property " + propertyId + ".");
                }
            }
        } catch (SQLException e) {
            logger.error("SQLException during updateConfigProperty for propertyId {} (old: {}) -> (new: {}): {}", propertyId, oldAggregateVersion, newAggregateVersion, e.getMessage(), e);
            throw e;
        } catch (Exception e) {
            logger.error("Exception during updateConfigProperty for propertyId {} (old: {}) -> (new: {}): {}", propertyId, oldAggregateVersion, newAggregateVersion, e.getMessage(), e);
            throw e;
        }
    }

    @Override
    public void deleteConfigProperty(Connection conn, Map<String, Object> event) throws SQLException, Exception {
        final String sql = "DELETE FROM config_property_t WHERE config_id = ? AND property_id = ? AND aggregate_version = ?";
        Map<String, Object> map = (Map<String, Object>)event.get(PortalConstants.DATA);
        String configId = (String)map.get("configId");
        String propertyId = (String)map.get("propertyId");
        long oldAggregateVersion = SqlUtil.getOldAggregateVersion(event);
        try (PreparedStatement statement = conn.prepareStatement(sql)) {
            statement.setObject(1, UUID.fromString(configId));
            statement.setObject(2, UUID.fromString(propertyId));
            statement.setLong(3, oldAggregateVersion);

            int count = statement.executeUpdate();
            if (count == 0) {
                if (queryConfigPropertyExists(conn, propertyId)) {
                    throw new ConcurrencyException("Optimistic concurrency conflict for config property " + propertyId + " aggregateVersion " + oldAggregateVersion + " but found a different version or already updated.");
                } else {
                    throw new SQLException("No record found during deleteConfigProperty for propertyId " + propertyId + ". It might have been already deleted.");
                }
            }
        } catch (SQLException e) {
            logger.error("SQLException during deleteConfigProperty for propertyId {} aggregateVersion {}: {}", propertyId, oldAggregateVersion, e.getMessage(), e);
            throw e;
        } catch (Exception e) {
            logger.error("Exception during deleteConfigProperty for propertyId {} aggregateVersion {}: {}", propertyId, oldAggregateVersion, e.getMessage(), e);
            throw e;
        }
    }

    @Override
    public Result<String> getConfigProperty(int offset, int limit, String configId, String configName, String propertyId,
                                            String propertyName, String propertyType, String light4jVersion, Integer displayOrder,
                                            Boolean required, String propertyDesc, String propertyValue, String valueType,
                                            String resourceType) {
        Result<String> result = null;

        String s =
                """
                SELECT COUNT(*) OVER () AS total,
                cp.config_id, cp.property_id, cp.property_name, cp.property_type, cp.light4j_version,
                cp.display_order, cp.required, cp.property_desc, cp.property_value, cp.value_type,
                cp.resource_type, cp.update_user, cp.update_ts, c.config_name, cp.aggregate_version
                FROM config_property_t cp
                JOIN config_t c ON cp.config_id = c.config_id
                WHERE 1=1
                """;

        StringBuilder sqlBuilder = new StringBuilder(s);
        List<Object> parameters = new ArrayList<>();

        StringBuilder whereClause = new StringBuilder();
        SqlUtil.addCondition(whereClause, parameters, "cp.config_id", configId != null ? UUID.fromString(configId) : null);
        SqlUtil.addCondition(whereClause, parameters, "c.config_name", configName);
        SqlUtil.addCondition(whereClause, parameters, "cp.property_id", propertyId != null ? UUID.fromString(propertyId) : null);
        SqlUtil.addCondition(whereClause, parameters, "cp.property_name", propertyName);
        SqlUtil.addCondition(whereClause, parameters, "cp.property_type", propertyType);
        SqlUtil.addCondition(whereClause, parameters, "cp.light4j_version", light4jVersion);
        SqlUtil.addCondition(whereClause, parameters, "cp.display_order", displayOrder);
        SqlUtil.addCondition(whereClause, parameters, "cp.required", required);
        SqlUtil.addCondition(whereClause, parameters, "cp.property_desc", propertyDesc);
        SqlUtil.addCondition(whereClause, parameters, "cp.property_value", propertyValue);
        SqlUtil.addCondition(whereClause, parameters, "cp.value_type", valueType);
        SqlUtil.addCondition(whereClause, parameters, "cp.resource_type", resourceType);


        if (!whereClause.isEmpty()) {
            sqlBuilder.append("AND ").append(whereClause);
        }

        sqlBuilder.append(" ORDER BY cp.config_id, cp.display_order\n" +
                "LIMIT ? OFFSET ?");

        parameters.add(limit);
        parameters.add(offset);

        String sql = sqlBuilder.toString();
        int total = 0;
        List<Map<String, Object>> configProperties = new ArrayList<>();

        try (Connection connection = ds.getConnection();
             PreparedStatement preparedStatement = connection.prepareStatement(sql)) {

            for (int i = 0; i < parameters.size(); i++) {
                preparedStatement.setObject(i + 1, parameters.get(i));
            }

            boolean isFirstRow = true;
            try (ResultSet resultSet = preparedStatement.executeQuery()) {
                while (resultSet.next()) {
                    Map<String, Object> map = new HashMap<>();
                    if (isFirstRow) {
                        total = resultSet.getInt("total");
                        isFirstRow = false;
                    }
                    map.put("configId", resultSet.getObject("config_id", UUID.class));
                    map.put("configName", resultSet.getString("config_name"));
                    map.put("propertyId", resultSet.getObject("property_id", UUID.class));
                    map.put("propertyName", resultSet.getString("property_name"));
                    map.put("propertyType", resultSet.getString("property_type"));
                    map.put("light4jVersion", resultSet.getString("light4j_version"));
                    map.put("displayOrder", resultSet.getInt("display_order"));
                    map.put("required", resultSet.getBoolean("required"));
                    map.put("propertyDesc", resultSet.getString("property_desc"));
                    map.put("propertyValue", resultSet.getString("property_value"));
                    map.put("valueType", resultSet.getString("value_type"));
                    map.put("resourceType", resultSet.getString("resource_type"));
                    map.put("updateUser", resultSet.getString("update_user"));
                    map.put("updateTs", resultSet.getObject("update_ts") != null ? resultSet.getObject("update_ts", OffsetDateTime.class) : null);
                    map.put("aggregateVersion", resultSet.getLong("aggregate_version"));

                    configProperties.add(map);
                }
            }

            Map<String, Object> resultMap = new HashMap<>();
            resultMap.put("total", total);
            resultMap.put("configProperties", configProperties);  // Changed key name
            result = Success.of(JsonMapper.toJson(resultMap));

        } catch (SQLException e) {
            logger.error("SQLException:", e);
            result = Failure.of(new Status("SQL_EXCEPTION", e.getMessage()));
        } catch (Exception e) {
            logger.error("Exception:", e);
            result = Failure.of(new Status("GENERIC_EXCEPTION", e.getMessage()));
        }
        return result;
    }

    @Override
    public Result<String> queryConfigPropertyById(String configId) {
        Result<String> result = null;

        String sql =
                """
                SELECT cp.config_id, cp.property_id, cp.property_name, cp.property_type, cp.light4j_version,
                cp.display_order, cp.required, cp.property_desc, cp.property_value, cp.value_type,
                cp.resource_type, cp.update_user, cp.update_ts, c.config_name, cp.aggregate_version
                FROM config_property_t cp
                JOIN config_t c ON cp.config_id = c.config_id
                WHERE cp.config_id = ?
                """;

        try (Connection connection = ds.getConnection();
             PreparedStatement preparedStatement = connection.prepareStatement(sql)) {

            preparedStatement.setObject(1, UUID.fromString(configId));

            List<Map<String, Object>> configProperties = new ArrayList<>();
            try (ResultSet resultSet = preparedStatement.executeQuery()) {
                while (resultSet.next()) {
                    Map<String, Object> map = new HashMap<>();
                    map.put("configId", resultSet.getObject("config_id", UUID.class));
                    map.put("configName", resultSet.getString("config_name"));
                    map.put("propertyId", resultSet.getObject("property_id", UUID.class));
                    map.put("propertyName", resultSet.getString("property_name"));
                    map.put("propertyType", resultSet.getString("property_type"));
                    map.put("light4jVersion", resultSet.getString("light4j_version"));
                    map.put("displayOrder", resultSet.getInt("display_order"));
                    map.put("required", resultSet.getBoolean("required"));
                    map.put("propertyDesc", resultSet.getString("property_desc"));
                    map.put("propertyValue", resultSet.getString("property_value"));
                    map.put("valueType", resultSet.getString("value_type"));
                    map.put("resourceType", resultSet.getString("resource_type"));
                    map.put("updateUser", resultSet.getString("update_user"));
                    map.put("updateTs", resultSet.getObject("update_ts") != null ? resultSet.getObject("update_ts", OffsetDateTime.class) : null);
                    map.put("aggregateVersion", resultSet.getLong("aggregate_version"));
                    configProperties.add(map);
                }
            }
            if (configProperties.isEmpty()) {
                result = Failure.of(new Status(OBJECT_NOT_FOUND, "config property", "configId = " + configId ));
            } else {
                result = Success.of(JsonMapper.toJson(configProperties)); // Return the list of properties as JSON
            }

        } catch (SQLException e) {
            logger.error("SQLException:", e);
            result = Failure.of(new Status("SQL_EXCEPTION", e.getMessage()));
        } catch (Exception e) {
            logger.error("Exception:", e);
            result = Failure.of(new Status("GENERIC_EXCEPTION", e.getMessage()));
        }
        return result;
    }

    @Override
    public Result<String> queryConfigPropertyByPropertyId(String configId, String propertyId) {
        Result<String> result = null;

        String sql =
                """
                SELECT cp.config_id, cp.property_id, cp.property_name, cp.property_type, cp.light4j_version,
                cp.display_order, cp.required, cp.property_desc, cp.property_value, cp.value_type,
                cp.resource_type, cp.update_user, cp.update_ts, c.config_name, cp.aggregate_version
                FROM config_property_t cp
                INNER JOIN config_t c ON cp.config_id = c.config_id
                WHERE cp.config_id = ?
                AND cp.property_id = ?
                """;


        try (Connection connection = ds.getConnection();
             PreparedStatement preparedStatement = connection.prepareStatement(sql)) {

            preparedStatement.setObject(1, UUID.fromString(configId));
            preparedStatement.setObject(2, UUID.fromString(propertyId));

            Map<String, Object> map = new HashMap<>();
            try (ResultSet resultSet = preparedStatement.executeQuery()) {
                if (resultSet.next()) {
                    map.put("configId", resultSet.getObject("config_id", UUID.class));
                    map.put("configName", resultSet.getString("config_name"));
                    map.put("propertyId", resultSet.getObject("property_id", UUID.class));
                    map.put("propertyName", resultSet.getString("property_name"));
                    map.put("propertyType", resultSet.getString("property_type"));
                    map.put("light4jVersion", resultSet.getString("light4j_version"));
                    map.put("displayOrder", resultSet.getInt("display_order"));
                    map.put("required", resultSet.getBoolean("required"));
                    map.put("propertyDesc", resultSet.getString("property_desc"));
                    map.put("propertyValue", resultSet.getString("property_value"));
                    map.put("valueType", resultSet.getString("value_type"));
                    map.put("resourceType", resultSet.getString("resource_type"));
                    map.put("updateUser", resultSet.getString("update_user"));
                    map.put("updateTs", resultSet.getObject("update_ts") != null ? resultSet.getObject("update_ts", OffsetDateTime.class) : null);
                    map.put("aggregateVersion", resultSet.getLong("aggregate_version"));
                }
            }

            if (map.isEmpty()) {
                result = Failure.of(new Status(OBJECT_NOT_FOUND, "config property", "configId = " + configId + " propertyId = " + propertyId));
            } else {
                result = Success.of(JsonMapper.toJson(map));
            }
        } catch (SQLException e) {
            logger.error("SQLException:", e);
            result = Failure.of(new Status("SQL_EXCEPTION", e.getMessage()));
        } catch (Exception e) {
            logger.error("Exception:", e);
            result = Failure.of(new Status("GENERIC_EXCEPTION", e.getMessage()));
        }
        return result;
    }

    @Override
    public void createConfigEnvironment(Connection conn, Map<String, Object> event) throws SQLException, Exception {
        final String sql = "INSERT INTO environment_property_t (host_id, environment, property_id, " +
                "property_value, update_user, update_ts, aggregate_version) " +
                "VALUES (?, ?, ?, ?, ?,  ?, ?)";
        Map<String, Object> map = (Map<String, Object>)event.get(PortalConstants.DATA);
        String hostId = (String)map.get("hostId");
        String environment = (String)map.get("environment");
        String propertyId = (String)map.get("propertyId");
        long newAggregateVersion = SqlUtil.getNewAggregateVersion(event);

        try (PreparedStatement statement = conn.prepareStatement(sql)) {
            statement.setObject(1, UUID.fromString(hostId));
            statement.setString(2, environment);
            statement.setObject(3, UUID.fromString(propertyId));

            // Handle property_value (optional)
            if (map.containsKey("propertyValue")) {
                statement.setString(4, (String) map.get("propertyValue"));
            } else {
                statement.setNull(4, Types.VARCHAR);
            }

            statement.setString(5, (String)event.get(Constants.USER));
            statement.setObject(6, OffsetDateTime.parse((String)event.get(CloudEventV1.TIME)));
            statement.setLong(7, newAggregateVersion);

            int count = statement.executeUpdate();
            if (count == 0) {
                throw new SQLException(String.format("Failed to insert environment property for environment %s propertyId %s with aggregateVersion %d", environment, propertyId, newAggregateVersion));
            }
        } catch (SQLException e) {
            logger.error("SQLException during createConfigEnvironment for hostId {} environment {} propertyId {} aggregateVersion {}: {}", hostId, environment, propertyId, newAggregateVersion, e.getMessage(), e);
            throw e;
        } catch (Exception e) {
            logger.error("Exception during createConfigEnvironment for hostId {} environment {} propertyId {} aggregateVersion {}: {}", hostId, environment, propertyId, newAggregateVersion, e.getMessage(), e);
            throw e;
        }
    }

    private boolean queryConfigEnvironmentExists(Connection conn, String hostId, String environment, String propertyId) throws SQLException {
        final String sql =
                """
                SELECT COUNT(*) FROM environment_property_t WHERE host_id = ? AND environment = ? AND property_id = ?
                """;
        try (PreparedStatement pst = conn.prepareStatement(sql)) {
            pst.setObject(1, UUID.fromString(hostId));
            pst.setString(2, environment);
            pst.setObject(3, UUID.fromString(propertyId));
            try (ResultSet rs = pst.executeQuery()) {
                return rs.next() && rs.getInt(1) > 0;
            }
        }
    }

    @Override
    public void updateConfigEnvironment(Connection conn, Map<String, Object> event) throws SQLException, Exception {
        final String sql =
                """
                UPDATE environment_property_t SET property_value = ?, update_user = ?, update_ts = ?, aggregate_version = ?
                WHERE host_id = ? AND environment = ? AND property_id = ? AND aggregate_version = ?
                """;
        Map<String, Object> map = (Map<String, Object>)event.get(PortalConstants.DATA);
        String hostId = (String)map.get("hostId");
        String environment = (String)map.get("environment");
        String propertyId = (String)map.get("propertyId");
        long oldAggregateVersion = SqlUtil.getOldAggregateVersion(event);
        long newAggregateVersion = SqlUtil.getNewAggregateVersion(event);

        try (PreparedStatement statement = conn.prepareStatement(sql)) {

            if (map.containsKey("propertyValue")) {
                statement.setString(1, (String) map.get("propertyValue"));
            } else {
                statement.setNull(1, Types.VARCHAR); // Or keep existing
            }

            statement.setString(2, (String)event.get(Constants.USER));
            statement.setObject(3, OffsetDateTime.parse((String)event.get(CloudEventV1.TIME)));
            statement.setLong(4, newAggregateVersion);

            // WHERE clause parameters
            statement.setObject(5, UUID.fromString(hostId));
            statement.setString(6, environment);
            statement.setObject(7, UUID.fromString(propertyId));
            statement.setLong(8, oldAggregateVersion);

            int count = statement.executeUpdate();
            if (count == 0) {
                if (queryConfigEnvironmentExists(conn, (String)event.get(Constants.HOST), environment, propertyId)) {
                    throw new ConcurrencyException("Optimistic concurrency conflict for environment config hostId " + hostId + " environment " + environment + " propertyId " + propertyId + ". Expected version " + oldAggregateVersion + " but found a different version " + newAggregateVersion + ".");
                } else {
                    throw new SQLException("No record found to update for environment config hostId " + hostId + " environment " + environment + " propertyId " + propertyId + ".");
                }
            }
        } catch (SQLException e) {
            logger.error("SQLException during updateConfigEnvironment for hostId {} environment {} propertyId {} (old: {}) -> (new: {}): {}", hostId, environment, propertyId, oldAggregateVersion, newAggregateVersion, e.getMessage(), e);
            throw e;
        } catch (Exception e) {
            logger.error("Exception during updateConfigEnvironment for hostId {} environment {} propertyId {} (old: {}) -> (new: {}): {}", hostId, environment, propertyId, oldAggregateVersion, newAggregateVersion, e.getMessage(), e);
            throw e;
        }
    }

    @Override
    public void deleteConfigEnvironment(Connection conn, Map<String, Object> event) throws SQLException, Exception {
        final String sql = "DELETE FROM environment_property_t WHERE host_id = ? AND environment = ? AND property_id = ? AND aggregate_version = ?";
        Map<String, Object> map = (Map<String, Object>)event.get(PortalConstants.DATA);
        String hostId = (String)map.get("hostId");
        String environment = (String)map.get("environment");
        String propertyId = (String)map.get("propertyId");
        long oldAggregateVersion = SqlUtil.getOldAggregateVersion(event);

        try (PreparedStatement statement = conn.prepareStatement(sql)) {
            statement.setObject(1, UUID.fromString(hostId));
            statement.setString(2, environment);
            statement.setObject(3, UUID.fromString(propertyId));
            statement.setLong(4, oldAggregateVersion);

            int count = statement.executeUpdate();
            if (count == 0) {
                if (queryConfigEnvironmentExists(conn, (String)event.get(Constants.HOST), environment, propertyId)) {
                    throw new ConcurrencyException("Optimistic concurrency conflict during deleteConfigEnvironment for hostId " + hostId + " environment " + environment + " propertyId " + propertyId + " aggregateVersion " + oldAggregateVersion + " but found a different version or already updated.");
                } else {
                    throw new SQLException("No record found during deleteConfigEnvironment for hostId " + hostId + " environment " + environment + " propertyId " + propertyId + ". It might have been already deleted.");
                }
            }
        } catch (SQLException e) {
            logger.error("SQLException during deleteConfigEnvironment for hostId {} environment {} propertyId {} aggregateVersion {}: {}", hostId, environment, propertyId, oldAggregateVersion, e.getMessage(), e);
            throw e;
        } catch (Exception e) {
            logger.error("Exception during deleteConfigEnvironment for hostId {} environment {} propertyId {} aggregateVersion {}: {}", hostId, environment, propertyId, oldAggregateVersion, e.getMessage(), e);
            throw e;
        }
    }

    @Override
    public Result<String> getConfigEnvironment(int offset, int limit, String hostId, String environment, String configId, String configName,
                                               String propertyId, String propertyName, String propertyValue) {
        Result<String> result = null;
        String s = """
                SELECT COUNT(*) OVER () AS total,
                ep.host_id, ep.environment, c.config_id, c.config_name,
                ep.property_id, p.property_name, ep.property_value,
                ep.update_user, ep.update_ts, ep.aggregate_version
                FROM environment_property_t ep
                JOIN config_property_t p ON ep.property_id = p.property_id
                JOIN config_t c ON p.config_id = c.config_id
                WHERE 1=1
                """;

        StringBuilder sqlBuilder = new StringBuilder();
        sqlBuilder.append(s).append("\n");

        List<Object> parameters = new ArrayList<>();

        StringBuilder whereClause = new StringBuilder();
        SqlUtil.addCondition(whereClause, parameters, "ep.host_id", hostId != null ? UUID.fromString(hostId) : null);
        SqlUtil.addCondition(whereClause, parameters, "ep.environment", environment);
        SqlUtil.addCondition(whereClause, parameters, "ep.property_id", propertyId != null ? UUID.fromString(propertyId) : null);
        SqlUtil.addCondition(whereClause, parameters, "c.config_id", configId != null ? UUID.fromString(configId) : null);
        SqlUtil.addCondition(whereClause, parameters, "c.config_name", configName);
        SqlUtil.addCondition(whereClause, parameters, "p.property_name", propertyName);
        SqlUtil.addCondition(whereClause, parameters, "ep.property_value", propertyValue);


        if (!whereClause.isEmpty()) {
            sqlBuilder.append("AND ").append(whereClause);
        }

        sqlBuilder.append(" ORDER BY ep.environment, c.config_id, p.display_order\n" +
                "LIMIT ? OFFSET ?");

        parameters.add(limit);
        parameters.add(offset);

        String sql = sqlBuilder.toString();
        int total = 0;
        List<Map<String, Object>> configEnvironments = new ArrayList<>();

        try (Connection connection = ds.getConnection();
             PreparedStatement preparedStatement = connection.prepareStatement(sql)) {

            for (int i = 0; i < parameters.size(); i++) {
                preparedStatement.setObject(i + 1, parameters.get(i));
            }

            boolean isFirstRow = true;
            try (ResultSet resultSet = preparedStatement.executeQuery()) {
                while (resultSet.next()) {
                    Map<String, Object> map = new HashMap<>();
                    if (isFirstRow) {
                        total = resultSet.getInt("total");
                        isFirstRow = false;
                    }
                    map.put("hostId", resultSet.getObject("host_id", UUID.class));
                    map.put("environment", resultSet.getString("environment"));
                    map.put("configId", resultSet.getObject("config_id", UUID.class));
                    map.put("configName", resultSet.getString("config_name"));
                    map.put("propertyName", resultSet.getString("property_name"));
                    map.put("propertyValue", resultSet.getString("property_value"));
                    map.put("propertyId", resultSet.getString("property_id"));
                    map.put("updateUser", resultSet.getString("update_user"));
                    map.put("updateTs", resultSet.getObject("update_ts") != null ? resultSet.getObject("update_ts", OffsetDateTime.class) : null);
                    map.put("aggregateVersion", resultSet.getLong("aggregate_version"));

                    configEnvironments.add(map);
                }
            }

            Map<String, Object> resultMap = new HashMap<>();
            resultMap.put("total", total);
            resultMap.put("configEnvironments", configEnvironments);
            result = Success.of(JsonMapper.toJson(resultMap));

        } catch (SQLException e) {
            logger.error("SQLException:", e);
            result = Failure.of(new Status("SQL_EXCEPTION", e.getMessage()));
        } catch (Exception e) {
            logger.error("Exception:", e);
            result = Failure.of(new Status("GENERIC_EXCEPTION", e.getMessage()));
        }
        return result;
    }

    @Override
    public void createConfigInstanceApi(Connection conn, Map<String, Object> event) throws SQLException, Exception {
        final String sql =
                """
                INSERT INTO instance_api_property_t (host_id, instance_api_id, property_id,
                property_value, update_user, update_ts, aggregate_version) VALUES (?, ?, ?, ?, ?,  ?, ?)
                """;
        Map<String, Object> map = (Map<String, Object>)event.get(PortalConstants.DATA);
        String hostId = (String)event.get(Constants.HOST);
        String instanceApiId = (String)map.get("instanceApiId");
        String propertyId = (String)map.get("propertyId");
        long newAggregateVersion = SqlUtil.getNewAggregateVersion(event);

        try (PreparedStatement statement = conn.prepareStatement(sql)) {
            statement.setObject(1, UUID.fromString(hostId));
            statement.setObject(2, UUID.fromString(instanceApiId));
            statement.setObject(3, UUID.fromString(propertyId));
            if (map.containsKey("propertyValue")) {
                statement.setString(4, (String)map.get("propertyValue"));
            } else {
                statement.setNull(4, Types.VARCHAR);
            }
            statement.setString(5, (String)event.get(Constants.USER));
            statement.setObject(6, OffsetDateTime.parse((String)event.get(CloudEventV1.TIME)));
            statement.setLong(7, newAggregateVersion);

            int count = statement.executeUpdate();
            if (count == 0) {
                throw new SQLException(String.format("Failed to insert instance api property hostId %s instanceApiId %s propertyId %s with aggregateVersion %d", hostId, instanceApiId, propertyId, newAggregateVersion));
            }
        } catch (SQLException e) {
            logger.error("SQLException during createConfigInstanceApi for hostId {} instanceApiId {} propertyId {} aggregateVersion {}: {}", hostId, instanceApiId, propertyId, newAggregateVersion, e.getMessage(), e);
            throw e;
        } catch (Exception e) {
            logger.error("Exception during createConfigInstanceApi for hostId {} instanceApiId {} propertyId {} aggregateVersion {}: {}", hostId, instanceApiId, propertyId, newAggregateVersion, e.getMessage(), e);
            throw e;
        }
    }

    private boolean queryConfigInstanceApiExists(Connection conn, String hostId, String instanceApiId, String propertyId) throws SQLException {
        final String sql =
                """
                SELECT COUNT(*) FROM instance_api_property_t WHERE host_id = ? AND instance_api_id = ? AND property_id = ?
                """;
        try (PreparedStatement pst = conn.prepareStatement(sql)) {
            pst.setObject(1, UUID.fromString(hostId));
            pst.setString(2, instanceApiId);
            pst.setObject(3, UUID.fromString(propertyId));
            try (ResultSet rs = pst.executeQuery()) {
                return rs.next() && rs.getInt(1) > 0;
            }
        }
    }

    @Override
    public void updateConfigInstanceApi(Connection conn, Map<String, Object> event) throws SQLException, Exception {
        final String sql =
                """
                UPDATE instance_api_property_t SET
                property_value = ?, update_user = ?, update_ts = ?, aggregate_version" +
                WHERE host_id = ? AND instance_api_id = ? AND property_id = ?
                AND aggregate_version = ?
                """;
        Map<String, Object> map = (Map<String, Object>)event.get(PortalConstants.DATA);
        String hostId = (String)event.get(Constants.HOST);
        String instanceApiId = (String)map.get("instanceApiId");
        String propertyId = (String)map.get("propertyId");
        long oldAggregateVersion = SqlUtil.getOldAggregateVersion(event);
        long newAggregateVersion = SqlUtil.getNewAggregateVersion(event);

        try (PreparedStatement statement = conn.prepareStatement(sql)) {
            if (map.containsKey("propertyValue")) {
                statement.setString(1, (String)map.get("propertyValue"));
            } else {
                statement.setNull(1, Types.VARCHAR);
            }
            statement.setString(2, (String)event.get(Constants.USER));
            statement.setObject(3, OffsetDateTime.parse((String)event.get(CloudEventV1.TIME)));
            statement.setLong(4, newAggregateVersion);
            statement.setObject(5, UUID.fromString(hostId));
            statement.setObject(6, UUID.fromString(instanceApiId));
            statement.setObject(7, UUID.fromString(propertyId));
            statement.setLong(8, oldAggregateVersion);

            int count = statement.executeUpdate();
            if (count == 0) {
                if (queryConfigInstanceApiExists(conn, hostId, instanceApiId, propertyId)) {
                    throw new ConcurrencyException("Optimistic concurrency conflict for config instance api hostId " + hostId + " instanceApiId " + instanceApiId + " propertyId " + propertyId + ". Expected version " + oldAggregateVersion + " but found a different version " + newAggregateVersion + ".");
                } else {
                    throw new SQLException("No record found to update for config instance api hostId " + hostId + " instanceApiId " + instanceApiId + " propertyId " + propertyId + ".");
                }
            }
        } catch (SQLException e) {
            logger.error("SQLException during updateConfigInstanceApi for hostId {} instanceApiId {} propertyId {} (old: {}) -> (new: {}): {}", hostId, instanceApiId, propertyId, oldAggregateVersion, newAggregateVersion, e.getMessage(), e);
            throw e;
        } catch (Exception e) {
            logger.error("Exception during updateConfigInstanceApi for hostId {} instanceApiId {} propertyId {} (old: {}) -> (new: {}): {}", hostId, instanceApiId, propertyId, oldAggregateVersion, newAggregateVersion, e.getMessage(), e);
            throw e;
        }
    }

    @Override
    public void deleteConfigInstanceApi(Connection conn, Map<String, Object> event) throws SQLException, Exception {
        final String sql =
                """
                DELETE FROM instance_api_property_t
                WHERE host_id = ? AND instance_api_id = ? AND property_id = ? AND aggregate_version = ?
                """;
        Map<String, Object> map = (Map<String, Object>)event.get(PortalConstants.DATA);
        String hostId = (String)event.get(Constants.HOST);
        String instanceApiId = (String)map.get("instanceApiId");
        String propertyId = (String)map.get("propertyId");
        long oldAggregateVersion = SqlUtil.getOldAggregateVersion(event);

        try (PreparedStatement statement = conn.prepareStatement(sql)) {
            statement.setObject(1, UUID.fromString(hostId));
            statement.setObject(2, UUID.fromString(instanceApiId));
            statement.setObject(3, UUID.fromString(propertyId));
            statement.setLong(4, oldAggregateVersion);

            int count = statement.executeUpdate();
            if (count == 0) {
                if (queryConfigInstanceApiExists(conn, hostId, instanceApiId, propertyId)) {
                    throw new ConcurrencyException("Optimistic concurrency conflict during deleteConfigInstanceApi hostId " + hostId + " instanceApiId " + instanceApiId + " propertyId " + propertyId + " aggregateVersion " + oldAggregateVersion +  " but found a different version or already updated.");
                } else {
                    throw new SQLException("No record found during deleteConfigInstanceApi hostId " + hostId + " instanceApiId " + instanceApiId + " propertyId " + propertyId + ". It might have been already deleted.");
                }
        }
        } catch (SQLException e) {
            logger.error("SQLException during deleteConfigInstanceApi for hostId {} instanceApiId {} propertyId {} aggregateVersion {}: {}", hostId, instanceApiId, propertyId, oldAggregateVersion, e.getMessage(), e);
            throw e;
        } catch (Exception e) {
            logger.error("Exception during deleteConfigInstanceApi for hostId {} instanceApiId {} propertyId {} aggregateVersion {}: {}", hostId, instanceApiId, propertyId, oldAggregateVersion, e.getMessage(), e);
            throw e;
        }
    }

    @Override
    public Result<String> getConfigInstanceApi(int offset, int limit, String hostId, String instanceApiId, String instanceId,
                                               String instanceName, String apiVersionId, String apiId, String apiVersion,
                                               String configId, String configName, String propertyId, String propertyName,
                                               String propertyValue) {
        Result<String> result = null;
        String s =
                """
                SELECT COUNT(*) OVER () AS total,
                iap.host_id, iap.instance_api_id, ia.instance_id, i.instance_name, ia.api_version_id, av.api_id, av.api_version, ia.active,
                ia.update_user, ia.update_ts, p.config_id, c.config_name, iap.property_id, p.property_name, iap.property_value, iap.aggregate_version,
                p.required, p.property_desc, p.property_type, p.resource_type, p.value_type, c.config_type, c.config_desc, c.class_path
                FROM instance_api_t ia
                INNER JOIN api_version_t av ON av.api_version_id = ia.api_version_id
                INNER JOIN instance_t i ON ia.host_id =i.host_id AND ia.instance_id = i.instance_id
                INNER JOIN instance_api_property_t iap ON ia.host_id = iap.host_id AND ia.instance_api_id = iap.instance_api_id
                INNER JOIN config_property_t p ON iap.property_id = p.property_id
                INNER JOIN config_t c ON p.config_id = c.config_id
                WHERE 1=1
                """;
        StringBuilder sqlBuilder = new StringBuilder();
        sqlBuilder.append(s).append("\n");

        List<Object> parameters = new ArrayList<>();

        StringBuilder whereClause = new StringBuilder();
        SqlUtil.addCondition(whereClause, parameters, "iap.host_id", hostId != null ? UUID.fromString(hostId) : null);
        SqlUtil.addCondition(whereClause, parameters, "iap.instance_api_id", instanceApiId != null ? UUID.fromString(instanceApiId) : null);
        SqlUtil.addCondition(whereClause, parameters, "ia.instance_id", instanceId != null ? UUID.fromString(instanceId) : null);
        SqlUtil.addCondition(whereClause, parameters, "i.instance_name", instanceName);
        SqlUtil.addCondition(whereClause, parameters, "ia.api_version_id", apiVersionId != null ? UUID.fromString(apiVersionId) : null);
        SqlUtil.addCondition(whereClause, parameters, "av.api_id", apiId);
        SqlUtil.addCondition(whereClause, parameters, "av.api_version", apiVersion);
        SqlUtil.addCondition(whereClause, parameters, "p.config_id", configId != null ? UUID.fromString(configId) : null);
        SqlUtil.addCondition(whereClause, parameters, "c.config_name", configName);
        SqlUtil.addCondition(whereClause, parameters, "iap.property_id", propertyId != null ? UUID.fromString(propertyId) : null);
        SqlUtil.addCondition(whereClause, parameters, "p.property_name", propertyName);
        SqlUtil.addCondition(whereClause, parameters, "iap.property_value", propertyValue);


        if (!whereClause.isEmpty()) {
            sqlBuilder.append("AND ").append(whereClause);
        }

        sqlBuilder.append(" ORDER BY iap.host_id, ia.instance_id, av.api_id, av.api_version, p.config_id, p.display_order\n" +
                "LIMIT ? OFFSET ?");

        parameters.add(limit);
        parameters.add(offset);

        String sql = sqlBuilder.toString();
        int total = 0;
        List<Map<String, Object>> instanceApis = new ArrayList<>();

        try (Connection connection = ds.getConnection();
             PreparedStatement preparedStatement = connection.prepareStatement(sql)) {

            for (int i = 0; i < parameters.size(); i++) {
                preparedStatement.setObject(i + 1, parameters.get(i));
            }

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
                    map.put("apiVersionId", resultSet.getObject("api_version_id", UUID.class));
                    map.put("apiId", resultSet.getString("api_id"));
                    map.put("apiVersion", resultSet.getString("api_version"));
                    map.put("configId", resultSet.getObject("config_id", UUID.class));
                    map.put("configName", resultSet.getString("config_name"));
                    map.put("propertyId", resultSet.getObject("property_id", UUID.class));
                    map.put("propertyName", resultSet.getString("property_name"));
                    map.put("propertyValue", resultSet.getString("property_value"));
                    map.put("required", resultSet.getBoolean("required"));
                    map.put("propertyDesc", resultSet.getString("property_desc"));
                    map.put("propertyType", resultSet.getString("property_type"));
                    map.put("resourceType", resultSet.getString("resource_type"));
                    map.put("valueType", resultSet.getString("value_type"));
                    map.put("configType", resultSet.getString("config_type"));
                    map.put("configDesc", resultSet.getString("config_desc"));
                    map.put("classPath", resultSet.getString("class_path"));
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
            result = Failure.of(new Status("SQL_EXCEPTION", e.getMessage()));
        } catch (Exception e) {
            logger.error("Exception:", e);
            result = Failure.of(new Status("GENERIC_EXCEPTION", e.getMessage()));
        }
        return result;
    }

    @Override
    public void createConfigInstanceApp(Connection conn, Map<String, Object> event) throws SQLException, Exception {
        final String sql = "INSERT INTO instance_app_property_t (host_id, instance_app_id, property_id, property_value, update_user, update_ts, aggregate_version) " +
                "VALUES (?, ?, ?, ?, ?,  ?, ?)";
        Map<String, Object> map = (Map<String, Object>)event.get(PortalConstants.DATA);
        String hostId = (String)event.get(Constants.HOST);
        String instanceAppId = (String)map.get("instanceAppId");
        String propertyId = (String)map.get("propertyId");
        long newAggregateVersion = SqlUtil.getNewAggregateVersion(event);

        try (PreparedStatement statement = conn.prepareStatement(sql)) {
            statement.setObject(1, UUID.fromString(hostId));
            statement.setObject(2, UUID.fromString(instanceAppId));
            statement.setObject(3, UUID.fromString(propertyId));
            if (map.containsKey("propertyValue")) {
                statement.setString(4, (String)map.get("propertyValue"));
            } else {
                statement.setNull(4, Types.VARCHAR);
            }
            statement.setString(5, (String)event.get(Constants.USER));
            statement.setObject(6, OffsetDateTime.parse((String)event.get(CloudEventV1.TIME)));
            statement.setLong(7, newAggregateVersion);

            int count = statement.executeUpdate();
            if (count == 0) {
                throw new SQLException(String.format("Failed during createConfigInstanceApp hostId %s instanceAppId %s propertyId %s with aggregateVersion %d", hostId, instanceAppId, propertyId, newAggregateVersion));
            }
        } catch (SQLException e) {
            logger.error("SQLException during createConfigInstanceApp for hostId {} instanceAppId {} propertyId {} aggregateVersion {}: {}", hostId, instanceAppId, propertyId, newAggregateVersion, e.getMessage(), e);
            throw e;
        } catch (Exception e) {
            logger.error("Exception during createConfigInstanceApp for hostId {} instanceAppId {} propertyId {} aggregateVersion {}: {}", hostId, instanceAppId, propertyId, newAggregateVersion, e.getMessage(), e);
            throw e;
        }
    }

    private boolean queryConfigInstanceAppExists(Connection conn, String hostId, String instanceAppId, String propertyId) throws SQLException {
        final String sql =
                """
                SELECT COUNT(*) FROM instance_app_property_t WHERE host_id = ? AND instance_app_id = ? AND property_id = ?
                """;
        try (PreparedStatement pst = conn.prepareStatement(sql)) {
            pst.setObject(1, UUID.fromString(hostId));
            pst.setObject(2, UUID.fromString(instanceAppId));
            pst.setObject(3, UUID.fromString(propertyId));
            try (ResultSet rs = pst.executeQuery()) {
                return rs.next() && rs.getInt(1) > 0;
            }
        }
    }


    @Override
    public void updateConfigInstanceApp(Connection conn, Map<String, Object> event) throws SQLException, Exception {
        final String sql =
                """
                UPDATE instance_app_property_t SET
                property_value = ?, update_user = ?, update_ts = ?, aggregate_version = ?
                WHERE host_id = ? AND instance_app_id = ? AND property_id = ? AND aggregate_version = ?
                """;

        Map<String, Object> map = (Map<String, Object>)event.get(PortalConstants.DATA);
        String hostId = (String)event.get(Constants.HOST);
        String instanceAppId = (String)map.get("instanceAppId");
        String propertyId = (String)map.get("propertyId");
        long oldAggregateVersion = SqlUtil.getOldAggregateVersion(event);
        long newAggregateVersion = SqlUtil.getNewAggregateVersion(event);

        try (PreparedStatement statement = conn.prepareStatement(sql)) {
            if (map.containsKey("propertyValue")) {
                statement.setString(1, (String)map.get("propertyValue"));
            } else {
                statement.setNull(1, Types.VARCHAR);
            }
            statement.setString(2, (String)event.get(Constants.USER));
            statement.setObject(3, OffsetDateTime.parse((String)event.get(CloudEventV1.TIME)));
            statement.setLong(4, newAggregateVersion);
            statement.setObject(4, UUID.fromString(hostId));
            statement.setObject(5, UUID.fromString(instanceAppId));
            statement.setObject(6, UUID.fromString(propertyId));
            statement.setLong(7, oldAggregateVersion);

            int count = statement.executeUpdate();
            if (count == 0) {
                if (queryConfigInstanceAppExists(conn, hostId, instanceAppId, propertyId)) {
                    throw new ConcurrencyException("Optimistic concurrency conflict during updateConfigInstanceApp for hostId " + hostId + " instanceAppId " + instanceAppId + " propertyId " + propertyId + ". Expected version " + oldAggregateVersion + " but found a different version " + newAggregateVersion + ".");
                } else {
                    throw new SQLException("No record found during updateConfigInstanceApp for hostId " + hostId + " instanceAppId " + instanceAppId + " propertyId " + propertyId + ".");
                }
            }
        } catch (SQLException e) {
            logger.error("SQLException during updateConfigInstanceApp for hostId {} instanceAppId {} propertyId {} (old: {}) -> (new: {}): {}", hostId, instanceAppId, propertyId, oldAggregateVersion, newAggregateVersion, e.getMessage(), e);
            throw e;
        } catch (Exception e) {
            logger.error("Exception during updateConfigInstanceApp for hostId {} instanceAppId {} propertyId {} (old: {}) -> (new: {}): {}", hostId, instanceAppId, propertyId, oldAggregateVersion, newAggregateVersion, e.getMessage(), e);
            throw e;
        }
    }

    @Override
    public void deleteConfigInstanceApp(Connection conn, Map<String, Object> event) throws SQLException, Exception {
        final String sql =
                """
                DELETE FROM instance_app_property_t
                WHERE host_id = ? AND instance_app_id = ? AND property_id = ? AND aggregate_version = ?
                """;
        Map<String, Object> map = (Map<String, Object>)event.get(PortalConstants.DATA);
        String hostId = (String)event.get(Constants.HOST);
        String instanceAppId = (String)map.get("instanceAppId");
        String propertyId = (String)map.get("propertyId");
        long oldAggregateVersion = SqlUtil.getOldAggregateVersion(event);

        try (PreparedStatement statement = conn.prepareStatement(sql)) {
            statement.setObject(1, UUID.fromString(hostId));
            statement.setObject(2, UUID.fromString(instanceAppId));
            statement.setObject(3, UUID.fromString(propertyId));
            statement.setLong(4, oldAggregateVersion);

            int count = statement.executeUpdate();
            if (count == 0) {
                if (queryConfigInstanceAppExists(conn, hostId, instanceAppId, propertyId)) {
                    throw new ConcurrencyException("Optimistic concurrency conflict during deleteConfigInstanceApp for hostId " + hostId + " instanceAppId " + instanceAppId + " propertyId " + propertyId + " aggregateVersion " + oldAggregateVersion + " but found a different version or already updated.");
                } else {
                    throw new SQLException("No record found during deleteConfigInstanceApp for hostId " + hostId + " instanceAppId " + instanceAppId + " propertyId " + propertyId + ". It might have been already deleted.");
                }
            }
        } catch (SQLException e) {
            logger.error("SQLException during deleteConfigInstanceApp for hostId {} instanceAppId {} propertyId {} aggregateVersion {}: {}", hostId, instanceAppId, propertyId, e.getMessage(), oldAggregateVersion, e);
            throw e;
        } catch (Exception e) {
            logger.error("Exception during deleteConfigInstanceApp for hostId {} instanceAppId {} propertyId {} aggregateVersion {}: {}", hostId, instanceAppId, propertyId, oldAggregateVersion, e.getMessage(), e);
            throw e;
        }
    }

    @Override
    public Result<String> getConfigInstanceApp(int offset, int limit, String hostId, String instanceAppId, String instanceId,
                                               String instanceName, String appId, String appVersion, String configId, String configName,
                                               String propertyId, String propertyName, String propertyValue) {
        Result<String> result = null;
        String s =
                """
                SELECT COUNT(*) OVER () AS total,
                iap.host_id, iap.instance_app_id, ia.instance_id, i.instance_name, ia.app_id, ia.app_version, ia.active,
                p.config_id, c.config_name, iap.property_id, p.property_name, iap.property_value, iap.aggregate_version,
                p.required, p.property_desc, p.property_type, p.resource_type, p.value_type, c.config_type, c.config_desc, c.class_path,
                ia.update_user, ia.update_ts
                FROM instance_app_t ia
                INNER JOIN instance_t i ON ia.host_id =i.host_id AND ia.instance_id = i.instance_id
                INNER JOIN instance_app_property_t iap ON ia.host_id = iap.host_id AND ia.instance_app_id = iap.instance_app_id
                INNER JOIN config_property_t p ON p.property_id = iap.property_id
                INNER JOIN config_t c ON p.config_id = c.config_id
                WHERE 1=1
                """;
        StringBuilder sqlBuilder = new StringBuilder();
        sqlBuilder.append(s).append("\n");

        List<Object> parameters = new ArrayList<>();

        StringBuilder whereClause = new StringBuilder();
        SqlUtil.addCondition(whereClause, parameters, "iap.host_id", hostId != null ? UUID.fromString(hostId) : null);
        SqlUtil.addCondition(whereClause, parameters, "iap.instance_app_id", instanceAppId != null ? UUID.fromString(instanceAppId) : null);
        SqlUtil.addCondition(whereClause, parameters, "ia.instance_id", instanceId != null ? UUID.fromString(instanceId) : null);
        SqlUtil.addCondition(whereClause, parameters, "i.instance_name", instanceName);
        SqlUtil.addCondition(whereClause, parameters, "ia.app_id", appId);
        SqlUtil.addCondition(whereClause, parameters, "ia.app_version", appVersion);
        SqlUtil.addCondition(whereClause, parameters, "p.config_id", configId != null ? UUID.fromString(configId) : null);
        SqlUtil.addCondition(whereClause, parameters, "c.config_name", configName);
        SqlUtil.addCondition(whereClause, parameters, "iap.property_id", propertyId != null ? UUID.fromString(propertyId) : null);
        SqlUtil.addCondition(whereClause, parameters, "p.property_name", propertyName);
        SqlUtil.addCondition(whereClause, parameters, "iap.property_value", propertyValue);


        if (!whereClause.isEmpty()) {
            sqlBuilder.append("AND ").append(whereClause);
        }

        sqlBuilder.append(" ORDER BY iap.host_id, ia.instance_id, ia.app_id, ia.app_version, p.config_id, p.property_name\n" +
                "LIMIT ? OFFSET ?");


        parameters.add(limit);
        parameters.add(offset);

        String sql = sqlBuilder.toString();
        int total = 0;
        List<Map<String, Object>> instanceApps = new ArrayList<>();

        try (Connection connection = ds.getConnection();
             PreparedStatement preparedStatement = connection.prepareStatement(sql)) {

            for (int i = 0; i < parameters.size(); i++) {
                preparedStatement.setObject(i + 1, parameters.get(i));
            }

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
                    map.put("appId", resultSet.getString("app_id"));
                    map.put("appVersion", resultSet.getString("app_version"));
                    map.put("configId", resultSet.getObject("config_id", UUID.class));
                    map.put("configName", resultSet.getString("config_name"));
                    map.put("propertyId", resultSet.getObject("property_id", UUID.class));
                    map.put("propertyName", resultSet.getString("property_name"));
                    map.put("propertyValue", resultSet.getString("property_value"));
                    map.put("required", resultSet.getBoolean("required"));
                    map.put("propertyDesc", resultSet.getString("property_desc"));
                    map.put("propertyType", resultSet.getString("property_type"));
                    map.put("resourceType", resultSet.getString("resource_type"));
                    map.put("valueType", resultSet.getString("value_type"));
                    map.put("configType", resultSet.getString("config_type"));
                    map.put("configDesc", resultSet.getString("config_desc"));
                    map.put("classPath", resultSet.getString("class_path"));
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
            result = Failure.of(new Status("SQL_EXCEPTION", e.getMessage()));
        } catch (Exception e) {
            logger.error("Exception:", e);
            result = Failure.of(new Status("GENERIC_EXCEPTION", e.getMessage()));
        }
        return result;
    }

    @Override
    public void createConfigInstanceAppApi(Connection conn, Map<String, Object> event) throws SQLException, Exception {
        final String sql =
                """
                INSERT INTO instance_app_api_property_t (host_id, instance_app_id, instance_api_id,
                property_id, property_value, update_user, update_ts, aggregate_version)
                VALUES (?, ?, ?, ?, ?,  ?, ?, ?)
                """;
        Map<String, Object> map = (Map<String, Object>)event.get(PortalConstants.DATA);
        String hostId = (String)event.get(Constants.HOST);
        String instanceAppId = (String)map.get("instanceAppId");
        String instanceApiId = (String)map.get("instanceApiId");
        String propertyId = (String)map.get("propertyId");
        long newAggregateVersion = SqlUtil.getNewAggregateVersion(event);

        try (PreparedStatement statement = conn.prepareStatement(sql)) {
            statement.setObject(1, UUID.fromString(hostId));
            statement.setObject(2, UUID.fromString(instanceAppId));
            statement.setObject(3, UUID.fromString(instanceApiId));
            statement.setObject(4, UUID.fromString(propertyId));
            if (map.containsKey("propertyValue")) {
                statement.setString(5, (String)map.get("propertyValue"));
            } else {
                statement.setNull(5, Types.VARCHAR);
            }
            statement.setString(6, (String)event.get(Constants.USER));
            statement.setObject(7, OffsetDateTime.parse((String)event.get(CloudEventV1.TIME)));
            statement.setLong(8, newAggregateVersion);

            int count = statement.executeUpdate();
            if (count == 0) {
                throw new SQLException(String.format("Failed during createConfigInstanceAppApi for hostId %s instanceAppId %s instanceApiId %s propertyId %s with aggregateVersion %d", hostId, instanceAppId, instanceApiId, propertyId, newAggregateVersion));
            }
        } catch (SQLException e) {
            logger.error("SQLException during createConfigInstanceAppApi for hostId {} instanceAppId {} instanceApiId {} propertyId {} aggregateVersion {}: {}", hostId, instanceAppId, instanceApiId, propertyId, newAggregateVersion, e.getMessage(), e);
            throw e;
        } catch (Exception e) {
            logger.error("Exception during createConfigInstanceAppApi for hostId {} instanceAppId {} instanceApiId {} propertyId {} aggregateVersion {}: {}", hostId, instanceAppId, instanceApiId, propertyId, newAggregateVersion, e.getMessage(), e);
            throw e;
        }
    }

    private boolean queryConfigInstanceAppApiExists(Connection conn, String hostId, String instanceAppId, String instanceApiId, String propertyId) throws SQLException {
        final String sql =
                """
                SELECT COUNT(*) FROM instance_app_api_property_t WHERE host_id = ? AND instance_app_id = ? AND instance_api_id = ? AND property_id = ?
                """;
        try (PreparedStatement pst = conn.prepareStatement(sql)) {
            pst.setObject(1, UUID.fromString(hostId));
            pst.setObject(2, UUID.fromString(instanceAppId));
            pst.setObject(3, UUID.fromString(instanceApiId));
            pst.setObject(4, UUID.fromString(propertyId));

            try (ResultSet rs = pst.executeQuery()) {
                return rs.next() && rs.getInt(1) > 0;
            }
        }
    }

    @Override
    public void updateConfigInstanceAppApi(Connection conn, Map<String, Object> event) throws SQLException, Exception {
        final String sql =
                """
                UPDATE instance_app_api_property_t SET
                property_value = ?, update_user = ?, update_ts = ?, aggregate_version = ?
                WHERE host_id = ? AND instance_app_id = ? AND instance_api_id = ? AND property_id = ? AND aggregate_version = ?
                """;

        Map<String, Object> map = (Map<String, Object>)event.get(PortalConstants.DATA);
        String hostId = (String)event.get(Constants.HOST);
        String instanceAppId = (String)map.get("instanceAppId");
        String instanceApiId = (String)map.get("instanceApiId");
        String propertyId = (String)map.get("propertyId");
        long oldAggregateVersion = SqlUtil.getOldAggregateVersion(event);
        long newAggregateVersion = SqlUtil.getNewAggregateVersion(event);

        try (PreparedStatement statement = conn.prepareStatement(sql)) {
            if (map.containsKey("propertyValue")) {
                statement.setString(1, (String)map.get("propertyValue"));
            } else {
                statement.setNull(1, Types.VARCHAR);
            }
            statement.setString(2, (String)event.get(Constants.USER));
            statement.setObject(3, OffsetDateTime.parse((String)event.get(CloudEventV1.TIME)));
            statement.setLong(4, newAggregateVersion);
            statement.setObject(5, UUID.fromString(hostId));
            statement.setObject(6, UUID.fromString(instanceAppId));
            statement.setObject(7, UUID.fromString(instanceApiId));
            statement.setObject(8, UUID.fromString(propertyId));
            statement.setLong(9, oldAggregateVersion);

            int count = statement.executeUpdate();
            if (count == 0) {
                if (queryConfigInstanceAppApiExists(conn, hostId, instanceAppId, instanceApiId, propertyId)) {
                    throw new ConcurrencyException("Optimistic concurrency conflict during updateConfigInstanceAppApi for hostId " + hostId + " instanceAppId " + instanceAppId + " instanceApiId " + instanceApiId + " propertyId " + propertyId + ". Expected version " + oldAggregateVersion + " but found a different version " + newAggregateVersion + ".");
                } else {
                    throw new SQLException("No record found during updateConfigInstanceAppApi for hostId " + hostId + "  instanceAppId " + instanceAppId + " instanceApiId " + instanceApiId + " propertyId " + propertyId + ".");
                }
            }
        } catch (SQLException e) {
            logger.error("SQLException during updateConfigInstanceAppApi for hostId {} instanceAppId {} instanceApiId {} propertyId {} (old: {}) -> (new: {}): {}", hostId, instanceAppId, instanceApiId, propertyId, oldAggregateVersion, newAggregateVersion, e.getMessage(), e);
            throw e;
        } catch (Exception e) {
            logger.error("Exception during updateConfigInstanceAppApi for hostId {} instanceAppId {} instanceApiId {} propertyId {} (old: {}) -> (new: {}): {}", hostId, instanceAppId, instanceApiId, propertyId, oldAggregateVersion, newAggregateVersion, e.getMessage(), e);
            throw e;
        }
    }

    @Override
    public void deleteConfigInstanceAppApi(Connection conn, Map<String, Object> event) throws SQLException, Exception {
        final String sql =
                """
                DELETE FROM instance_app_api_property_t
                WHERE host_id = ? AND instance_app_id = ? AND instance_api_id = ? AND property_id = ? AND aggregate_version = ?
                """;
        Map<String, Object> map = (Map<String, Object>)event.get(PortalConstants.DATA);
        String hostId = (String)event.get(Constants.HOST);
        String instanceAppId = (String)map.get("instanceAppId");
        String instanceApiId = (String)map.get("instanceApiId");
        String propertyId = (String)map.get("propertyId");
        long oldAggregateVersion = SqlUtil.getOldAggregateVersion(event);

        try (PreparedStatement statement = conn.prepareStatement(sql)) {
            statement.setObject(1, UUID.fromString(hostId));
            statement.setObject(2, UUID.fromString(instanceAppId));
            statement.setObject(3, UUID.fromString(instanceApiId));
            statement.setObject(4, UUID.fromString(propertyId));

            int count = statement.executeUpdate();
            if (count == 0) {
                if (queryConfigInstanceAppApiExists(conn, hostId, instanceAppId, instanceApiId, propertyId)) {
                    throw new ConcurrencyException("Optimistic concurrency conflict during deleteConfigInstanceAppApi for hostId " + hostId + " instanceAppId " + instanceAppId + " instanceApiId " + instanceApiId + " propertyId " + propertyId + " aggregateVersion " + oldAggregateVersion + " but found a different version or already updated.");
                } else {
                    throw new SQLException("No record found during deleteConfigInstanceAppApi for hostId " + hostId + " instanceAppId " + instanceAppId + " instanceApiId " + instanceApiId + " propertyId " + propertyId + ". It might have been already deleted.");
                }
            }
        } catch (SQLException e) {
            logger.error("SQLException during deleteConfigInstanceAppApi for hostId {} instanceAppId {} instanceApiId {} propertyId {} aggregateVersion {}: {}", hostId, instanceAppId, instanceApiId, propertyId, oldAggregateVersion, e.getMessage(), e);
            throw e;
        } catch (Exception e) {
            logger.error("Exception during deleteConfigInstanceAppApi for hostId {} instanceAppId {} instanceApiId {} propertyId {} aggregateVersion {}: {}", hostId, instanceAppId, instanceApiId, propertyId, oldAggregateVersion, e.getMessage(), e);
            throw e;
        }
    }

    @Override
    public Result<String> getConfigInstanceAppApi(int offset, int limit, String hostId, String instanceAppId, String instanceApiId, String instanceId,
                                                  String instanceName, String appId, String appVersion, String apiVersionId, String apiId, String apiVersion,
                                                  String configId, String configName, String propertyId, String propertyName, String propertyValue) {
        Result<String> result = null;
        String s =
                """
                SELECT COUNT(*) OVER () AS total,
                iaap.host_id, iaap.instance_app_id, iaap.instance_api_id, i.instance_id, i.instance_name, iap.app_id, iap.app_version,
                iai.api_version_id, av.api_id, av.api_version, p.config_id, c.config_name, iaap.property_id,
                p.property_name, iaap.property_value, p.required, p.property_desc, p.property_type, p.resource_type, p.value_type,
                c.config_type, c.config_desc, c.class_path,
                iaap.update_user, iaap.update_ts, iaap.aggregate_version
                FROM instance_app_t iap
                INNER JOIN instance_t i ON iap.host_id =i.host_id AND iap.instance_id = i.instance_id
                INNER JOIN instance_app_api_property_t iaap ON iaap.host_id = iap.host_id AND iaap.instance_app_id = iap.instance_app_id
                INNER JOIN instance_api_t iai ON iai.host_id = iaap.host_id AND iai.instance_api_id = iaap.instance_api_id
                INNER JOIN api_version_t av ON av.host_id = iai.host_id AND av.api_version_id = iai.api_version_id
                INNER JOIN config_property_t p ON p.property_id = iaap.property_id
                INNER JOIN config_t c ON p.config_id = c.config_id
                WHERE 1=1
                """;
        StringBuilder sqlBuilder = new StringBuilder();
        sqlBuilder.append(s).append("\n");

        List<Object> parameters = new ArrayList<>();

        StringBuilder whereClause = new StringBuilder();
        SqlUtil.addCondition(whereClause, parameters, "iaap.host_id", hostId != null ? UUID.fromString(hostId) : null);
        SqlUtil.addCondition(whereClause, parameters, "iaap.instance_app_id", instanceAppId != null ? UUID.fromString(instanceAppId) : null);
        SqlUtil.addCondition(whereClause, parameters, "iaap.instance_api_id", instanceApiId != null ? UUID.fromString(instanceApiId) : null);
        SqlUtil.addCondition(whereClause, parameters, "i.instance_id", instanceId != null ? UUID.fromString(instanceId) : null);
        SqlUtil.addCondition(whereClause, parameters, "i.instance_name", instanceName);
        SqlUtil.addCondition(whereClause, parameters, "iap.app_id", appId);
        SqlUtil.addCondition(whereClause, parameters, "iap.app_version", appVersion);
        SqlUtil.addCondition(whereClause, parameters, "iai.api_version_id", apiVersionId != null ? UUID.fromString(apiVersionId) : null);
        SqlUtil.addCondition(whereClause, parameters, "av.api_id", apiId);
        SqlUtil.addCondition(whereClause, parameters, "av.api_version", apiVersion);
        SqlUtil.addCondition(whereClause, parameters, "p.config_id", configId != null ? UUID.fromString(configId) : null);
        SqlUtil.addCondition(whereClause, parameters, "c.config_name", configName);
        SqlUtil.addCondition(whereClause, parameters, "iaap.property_id", propertyId != null ? UUID.fromString(propertyId) : null);
        SqlUtil.addCondition(whereClause, parameters, "p.property_name", propertyName);
        SqlUtil.addCondition(whereClause, parameters, "iaap.property_value", propertyValue);

        if (!whereClause.isEmpty()) {
            sqlBuilder.append("AND ").append(whereClause);
        }

        sqlBuilder.append(" ORDER BY iaap.host_id, i.instance_id, iap.app_id, iap.app_version, av.api_id, av.api_version, p.config_id, p.property_name\n" +
                "LIMIT ? OFFSET ?");


        parameters.add(limit);
        parameters.add(offset);

        String sql = sqlBuilder.toString();
        int total = 0;
        List<Map<String, Object>> instanceAppApis = new ArrayList<>();

        try (Connection connection = ds.getConnection();
             PreparedStatement preparedStatement = connection.prepareStatement(sql)) {

            for (int i = 0; i < parameters.size(); i++) {
                preparedStatement.setObject(i + 1, parameters.get(i));
            }

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
                    map.put("appId", resultSet.getString("app_id"));
                    map.put("appVersion", resultSet.getString("app_version"));
                    map.put("apiVersionId", resultSet.getObject("api_version_id", UUID.class));
                    map.put("apiId", resultSet.getString("api_id"));
                    map.put("apiVersion", resultSet.getString("api_version"));
                    map.put("configId", resultSet.getObject("config_id", UUID.class));
                    map.put("configName", resultSet.getString("config_name"));
                    map.put("propertyId", resultSet.getObject("property_id", UUID.class));
                    map.put("propertyName", resultSet.getString("property_name"));
                    map.put("propertyValue", resultSet.getString("property_value"));
                    map.put("required", resultSet.getBoolean("required"));
                    map.put("propertyDesc", resultSet.getString("property_desc"));
                    map.put("propertyType", resultSet.getString("property_type"));
                    map.put("resourceType", resultSet.getString("resource_type"));
                    map.put("valueType", resultSet.getString("value_type"));
                    map.put("configType", resultSet.getString("config_type"));
                    map.put("configDesc", resultSet.getString("config_desc"));
                    map.put("classPath", resultSet.getString("class_path"));
                    map.put("updateUser", resultSet.getString("update_user"));
                    map.put("updateTs", resultSet.getObject("update_ts") != null ? resultSet.getObject("update_ts", OffsetDateTime.class) : null);
                    map.put("aggregateVersion", resultSet.getLong("aggregate_version"));
                    instanceAppApis.add(map);
                }
            }
            Map<String, Object> resultMap = new HashMap<>();
            resultMap.put("total", total);
            resultMap.put("instanceAppApis", instanceAppApis);
            result = Success.of(JsonMapper.toJson(resultMap));

        } catch (SQLException e) {
            logger.error("SQLException:", e);
            result = Failure.of(new Status("SQL_EXCEPTION", e.getMessage()));
        } catch (Exception e) {
            logger.error("Exception:", e);
            result = Failure.of(new Status("GENERIC_EXCEPTION", e.getMessage()));
        }
        return result;
    }

    @Override
    public void createConfigInstance(Connection conn, Map<String, Object> event) throws SQLException, Exception {
        final String sql =
                """
                INSERT INTO instance_property_t (host_id, instance_id, property_id,
                property_value, update_user, update_ts, aggregate_version) VALUES (?, ?, ?, ?, ?, ?, ?)
                """;
        Map<String, Object> map = (Map<String, Object>)event.get(PortalConstants.DATA);
        String hostId = (String)event.get(Constants.HOST);
        String instanceId = (String)map.get("instanceId");
        String propertyId = (String)map.get("propertyId");
        long newAggregateVersion = SqlUtil.getNewAggregateVersion(event);

        try (PreparedStatement statement = conn.prepareStatement(sql)) {
            statement.setObject(1, UUID.fromString(hostId));
            statement.setObject(2, UUID.fromString(instanceId));
            statement.setObject(3, UUID.fromString(propertyId));

            // Handle 'property_value' (optional)
            if (map.containsKey("propertyValue")) {
                statement.setString(4, (String) map.get("propertyValue"));
            } else {
                statement.setNull(4, Types.VARCHAR);
            }
            statement.setString(5, (String)event.get(Constants.USER));
            statement.setObject(6, OffsetDateTime.parse((String)event.get(CloudEventV1.TIME)));
            statement.setLong(7, newAggregateVersion);

            int count = statement.executeUpdate();
            if (count == 0) {
                throw new SQLException(String.format("Failed during createConfigInstance for hostId %s instanceId %s propertyId %s with aggregateVersion %d", hostId, instanceId, propertyId, newAggregateVersion));
            }
        } catch (SQLException e) {
            logger.error("SQLException during createConfigInstance for hostId {} instanceId {} propertyId {} aggregateVersion {}: {}", hostId, instanceId, propertyId, newAggregateVersion, e.getMessage(), e);
            throw e;
        } catch (Exception e) {
            logger.error("Exception during createConfigInstance for hostId {} instanceId {} propertyId {} aggregateVersion {}: {}", hostId, instanceId, propertyId, newAggregateVersion, e.getMessage(), e);
            throw e;
        }
    }

    private boolean queryConfigInstanceExists(Connection conn, String hostId, String instanceId, String propertyId) throws SQLException {
        final String sql =
                """
                SELECT COUNT(*) FROM instance_property_t WHERE host_id = ? AND instance_id = ? AND property_id = ?
                """;
        try (PreparedStatement pst = conn.prepareStatement(sql)) {
            pst.setObject(1, UUID.fromString(hostId));
            pst.setObject(2, UUID.fromString(instanceId));
            pst.setObject(3, UUID.fromString(propertyId));
            try (ResultSet rs = pst.executeQuery()) {
                return rs.next() && rs.getInt(1) > 0;
            }
        }
    }

    @Override
    public void updateConfigInstance(Connection conn, Map<String, Object> event) throws SQLException, Exception {
        final String sql =
                """
                UPDATE instance_property_t SET property_value = ?, update_user = ?, update_ts = ?, aggregate_version = ?
                WHERE host_id = ? AND instance_id = ? AND property_id = ? AND aggregate_version = ?
                """;
        Map<String, Object> map = (Map<String, Object>)event.get(PortalConstants.DATA);
        String hostId = (String)event.get(Constants.HOST);
        String instanceId = (String)map.get("instanceId");
        String propertyId = (String)map.get("propertyId");
        long oldAggregateVersion = SqlUtil.getOldAggregateVersion(event);
        long newAggregateVersion = SqlUtil.getNewAggregateVersion(event);

        try (PreparedStatement statement = conn.prepareStatement(sql)) {

            // Handle 'property_value' (optional)
            if (map.containsKey("propertyValue")) {
                statement.setString(1, (String) map.get("propertyValue"));
            } else {
                statement.setNull(1, Types.VARCHAR); // Or keep existing
            }

            statement.setString(2, (String)event.get(Constants.USER));
            statement.setObject(3, OffsetDateTime.parse((String)event.get(CloudEventV1.TIME)));
            statement.setLong(4, newAggregateVersion);

            // WHERE clause parameters
            statement.setObject(4, UUID.fromString(hostId));
            statement.setObject(5, UUID.fromString(instanceId));
            statement.setObject(6, UUID.fromString(propertyId));
            statement.setLong(7, oldAggregateVersion);

            int count = statement.executeUpdate();
            if (count == 0) {
                if (queryConfigInstanceExists(conn, hostId, instanceId, propertyId)) {
                    throw new ConcurrencyException("Optimistic concurrency conflict during updateConfigInstance for hostId " + hostId + " instanceId " + instanceId + " propertyId " + propertyId + ". Expected version " + oldAggregateVersion + " but found a different version " + newAggregateVersion + ".");
                } else {
                    throw new SQLException("No record found during updateConfigInstance for hostId " + hostId + " instanceId " + instanceId + " propertyId " + propertyId + ".");
                }
            }
        } catch (SQLException e) {
            logger.error("SQLException during updateConfigInstance for hostId {} instanceId {} propertyId {} (old: {}) -> (new: {}): {}", hostId, instanceId, propertyId, oldAggregateVersion, newAggregateVersion, e.getMessage(), e);
            throw e;
        } catch (Exception e) {
            logger.error("Exception during updateConfigInstance for hostId {} instanceId {} propertyId {} (old: {}) -> (new: {}): {}", hostId, instanceId, propertyId, oldAggregateVersion, newAggregateVersion, e.getMessage(), e);
            throw e;
        }
    }

    @Override
    public void deleteConfigInstance(Connection conn, Map<String, Object> event) throws SQLException, Exception {
        final String sql = "DELETE FROM instance_property_t WHERE host_id = ? AND instance_id = ? AND property_id = ? AND aggregate_version = ?";
        Map<String, Object> map = (Map<String, Object>)event.get(PortalConstants.DATA);
        String hostId = (String)event.get(Constants.HOST);
        String instanceId = (String)map.get("instanceId");
        String propertyId = (String)map.get("propertyId");
        long oldAggregateVersion = SqlUtil.getOldAggregateVersion(event);

        try (PreparedStatement statement = conn.prepareStatement(sql)) {
            statement.setObject(1, UUID.fromString(hostId));
            statement.setObject(2, UUID.fromString(instanceId));
            statement.setObject(3, UUID.fromString(propertyId));
            statement.setLong(4, oldAggregateVersion);

            int count = statement.executeUpdate();
            if (count == 0) {
                if (queryConfigInstanceExists(conn, hostId, instanceId, propertyId)) {
                    throw new ConcurrencyException("Optimistic concurrency conflict during deleteConfigInstance for hostId " + hostId + " instanceId " + instanceId + " propertyId " + propertyId + " aggregateVersion " + oldAggregateVersion + " but found a different version or already updated.");
                } else {
                    throw new SQLException("No record found during deleteConfigInstance for hostId " + hostId + " instanceId " + instanceId + " propertyId " + propertyId + ". It might have been already deleted.");
                }
            }
        } catch (SQLException e) {
            logger.error("SQLException during deleteConfigInstance for hostId {} instanceId {} propertyId {} aggregateVersion {}: {}", hostId, instanceId, propertyId, oldAggregateVersion, e.getMessage(), e);
            throw e;
        } catch (Exception e) {
            logger.error("Exception during deleteConfigInstance for hostId {} instanceId {} propertyId {} aggregateVersion {}: {}", hostId, instanceId, propertyId, oldAggregateVersion, e.getMessage(), e);
            throw e;
        }
    }

    @Override
    public void commitConfigInstance(Connection conn, Map<String, Object> event) throws SQLException, Exception {

        // 1. Extract Input Parameters
        UUID hostId = UUID.fromString((String) event.get("hostId"));
        UUID instanceId = UUID.fromString((String) event.get("instanceId"));
        String snapshotType = (String) event.getOrDefault("snapshotType", "USER_SAVE");
        String description = (String) event.get("description");
        UUID userId = UUID.fromString((String) event.get(Constants.USER));              // User who triggered the event
        UUID deploymentId = event.get("deploymentId") != null ? UUID.fromString((String) event.get("deploymentId")) : null;

        UUID snapshotId = UuidUtil.getUUID();

        try {
            // 2. Derive Scope IDs
            // Query instance_t and potentially product_version_t based on hostId, instanceId
            DerivedScope scope = deriveScopeInfo(conn, hostId, instanceId);
            if (scope == null) {
                // Not rolling back here, caller does. Just throw.
                throw new SQLException(new Status(OBJECT_NOT_FOUND, "Instance not found for hostId/instanceId: " + hostId + "/" + instanceId).toString());
            }

            // 3. Insert Snapshot Metadata
            insertSnapshotMetadata(conn, snapshotId, snapshotType, description, userId, deploymentId, hostId, scope);

            // 4 & 5. Aggregate and Insert Effective Config
            insertEffectiveConfigSnapshot(conn, snapshotId, hostId, instanceId, scope);

            // 6. Snapshot Individual Override Tables
            // Use INSERT ... SELECT ... for efficiency
            snapshotInstanceProperties(conn, snapshotId, hostId, instanceId);
            snapshotInstanceApiProperties(conn, snapshotId, hostId, instanceId);
            snapshotInstanceAppProperties(conn, snapshotId, hostId, instanceId);
            snapshotInstanceAppApiProperties(conn, snapshotId, hostId, instanceId);
            snapshotEnvironmentProperties(conn, snapshotId, hostId, scope.environment());
            snapshotProductVersionProperties(conn, snapshotId, hostId, scope.productVersionId());
            snapshotProductProperties(conn, snapshotId, scope.productId());
            // Add others as needed

            logger.info("Successfully prepared config snapshot: {}", snapshotId);
            notificationService.insertNotification(event, true, null); // Notify success after all operations in this method
        } catch (SQLException e) {
            logger.error("SQLException during snapshot creation for instance {}: {}", instanceId, e.getMessage(), e);
            notificationService.insertNotification(event, false, e.getMessage());
            throw e;
        } catch (Exception e) { // Catch other potential errors (e.g., during scope derivation)
            logger.error("Exception during snapshot creation for instance {}: {}", instanceId, e.getMessage(), e);
            notificationService.insertNotification(event, false, e.getMessage());
            throw e;
        }
    }

    // Placeholder for derived scope data structure
    private record DerivedScope(String environment, String productId, String productVersion, UUID productVersionId, String serviceId) {

    }

    private DerivedScope deriveScopeInfo(Connection conn, UUID hostId, UUID instanceId) throws SQLException {
        String sql =
                """
                SELECT i.environment, i.service_id, pv.product_id, pv.product_version, pv.product_version_id
                FROM instance_t i
                LEFT JOIN product_version_t pv ON i.host_id = pv.host_id AND i.product_version_id = pv.product_version_id
                WHERE i.host_id = ? AND i.instance_id = ?
                """;
        try (PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setObject(1, hostId);
            ps.setObject(2, instanceId);
            try (ResultSet rs = ps.executeQuery()) {
                if (rs.next()) {
                    return new DerivedScope(
                            rs.getString("environment"),
                            rs.getString("product_id"),
                            rs.getString("product_version"),
                            rs.getObject("product_version_id", UUID.class),
                            rs.getString("service_id")
                    );
                } else {
                    return null; // Instance not found
                }
            }
        }
    }

    private void insertSnapshotMetadata(Connection conn, UUID snapshotId, String snapshotType, String description,
                                        UUID userId, UUID deploymentId, UUID hostId, DerivedScope scope) throws SQLException {
        String sql = """
            INSERT INTO config_snapshot_t
            (snapshot_id, snapshot_ts, snapshot_type, description, user_id, deployment_id,
             scope_host_id, scope_environment, scope_product_id, scope_service_id)
            VALUES (?, CURRENT_TIMESTAMP, ?, ?, ?, ?, ?, ?, ?, ?)
            """;
        try (PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setObject(1, snapshotId);
            ps.setString(2, snapshotType);
            ps.setString(3, description);
            ps.setObject(4, userId);         // setObject handles null correctly
            ps.setObject(5, deploymentId);   // setObject handles null correctly
            ps.setObject(6, hostId);
            ps.setString(7, scope.environment());
            ps.setString(8, scope.productId());
            ps.setString(9, scope.serviceId());
            ps.executeUpdate();
        }
    }

    private void insertEffectiveConfigSnapshot(Connection conn, UUID snapshotId, UUID hostId, UUID instanceId, DerivedScope scope) throws SQLException {
        final String selectSql =
                """
                    WITH
                    -- Parameters derived *before* running this query:
                    -- p_host_id UUID
                    -- p_instance_id UUID
                    -- v_product_version_id UUID (derived from p_instance_id)
                    -- v_environment VARCHAR(16) (derived from p_instance_id)
                    -- v_product_id VARCHAR(8) (derived from v_product_version_id)
                    -- Find relevant instance_api_ids and instance_app_ids for the target instance
                    RelevantInstanceApis AS (
                        SELECT instance_api_id
                        FROM instance_api_t
                        WHERE host_id = ? -- p_host_id 1
                          AND instance_id = ? -- p_instance_id 2
                    ),
                    RelevantInstanceApps AS (
                        SELECT instance_app_id
                        FROM instance_app_t
                        WHERE host_id = ? -- p_host_id 3
                          AND instance_id = ? -- p_instance_id 4
                    ),
                    -- Pre-process Instance App API properties with merging logic
                    Merged_Instance_App_Api_Properties AS (
                        SELECT
                            iaap.property_id,
                            CASE cp.value_type
                                WHEN 'map' THEN COALESCE(jsonb_merge_agg(iaap.property_value::jsonb), '{}'::jsonb)::text
                                WHEN 'list' THEN COALESCE((SELECT jsonb_agg(elem ORDER BY iaa.update_ts) -- Order elements based on when they were added via the link table? Or property update_ts? Assuming property update_ts. Check data model if linking time matters more.
                                                            FROM jsonb_array_elements(sub.property_value::jsonb) elem
                                                            WHERE jsonb_typeof(sub.property_value::jsonb) = 'array'
                                                          ), '[]'::jsonb)::text -- Requires subquery if ordering elements
                                 -- Subquery approach for ordering list elements by property timestamp:
                                 /*
                                  COALESCE(
                                     (SELECT jsonb_agg(elem ORDER BY prop.update_ts)
                                      FROM instance_app_api_property_t prop,
                                           jsonb_array_elements(prop.property_value::jsonb) elem
                                      WHERE prop.host_id = iaap.host_id
                                        AND prop.instance_app_id = iaap.instance_app_id
                                        AND prop.instance_api_id = iaap.instance_api_id
                                        AND prop.property_id = iaap.property_id
                                        AND jsonb_typeof(prop.property_value::jsonb) = 'array'
                                     ), '[]'::jsonb
                                  )::text
                                 */
                                ELSE MAX(iaap.property_value) -- For simple types, MAX can work if only one entry expected, otherwise need timestamp logic
                                -- More robust for simple types: Pick latest based on timestamp
                                /*
                                 (SELECT property_value
                                  FROM instance_app_api_property_t latest
                                  WHERE latest.host_id = iaap.host_id
                                    AND latest.instance_app_id = iaap.instance_app_id
                                    AND latest.instance_api_id = iaap.instance_api_id
                                    AND latest.property_id = iaap.property_id
                                  ORDER BY latest.update_ts DESC LIMIT 1)
                                */
                            END AS effective_value
                        FROM instance_app_api_property_t iaap
                        JOIN config_property_t cp ON iaap.property_id = cp.property_id
                        JOIN instance_app_api_t iaa ON iaa.host_id = iaap.host_id AND iaa.instance_app_id = iaap.instance_app_id AND iaa.instance_api_id = iaap.instance_api_id -- Join to potentially use its timestamp for ordering lists
                        WHERE iaap.host_id = ? -- p_host_id 5
                          AND iaap.instance_app_id IN (SELECT instance_app_id FROM RelevantInstanceApps)
                          AND iaap.instance_api_id IN (SELECT instance_api_id FROM RelevantInstanceApis)
                        GROUP BY iaap.host_id, iaap.instance_app_id, iaap.instance_api_id, iaap.property_id, cp.value_type -- Group to aggregate/merge
                    ),
                    -- Pre-process Instance API properties
                    Merged_Instance_Api_Properties AS (
                        SELECT
                            iap.property_id,
                            CASE cp.value_type
                                WHEN 'map' THEN COALESCE(jsonb_merge_agg(iap.property_value::jsonb), '{}'::jsonb)::text
                                WHEN 'list' THEN COALESCE((SELECT jsonb_agg(elem ORDER BY prop.update_ts) FROM instance_api_property_t prop, jsonb_array_elements(prop.property_value::jsonb) elem WHERE prop.host_id = iap.host_id AND prop.instance_api_id = iap.instance_api_id AND prop.property_id = iap.property_id AND jsonb_typeof(prop.property_value::jsonb) = 'array'), '[]'::jsonb)::text
                                ELSE (SELECT property_value FROM instance_api_property_t latest WHERE latest.host_id = iap.host_id AND latest.instance_api_id = iap.instance_api_id AND latest.property_id = iap.property_id ORDER BY latest.update_ts DESC LIMIT 1)
                            END AS effective_value
                        FROM instance_api_property_t iap
                        JOIN config_property_t cp ON iap.property_id = cp.property_id
                        WHERE iap.host_id = ? -- p_host_id 6
                          AND iap.instance_api_id IN (SELECT instance_api_id FROM RelevantInstanceApis)
                        GROUP BY iap.host_id, iap.instance_api_id, iap.property_id, cp.value_type
                    ),
                    -- Pre-process Instance App properties
                    Merged_Instance_App_Properties AS (
                         SELECT
                            iapp.property_id,
                            CASE cp.value_type
                                WHEN 'map' THEN COALESCE(jsonb_merge_agg(iapp.property_value::jsonb), '{}'::jsonb)::text
                                WHEN 'list' THEN COALESCE((SELECT jsonb_agg(elem ORDER BY prop.update_ts) FROM instance_app_property_t prop, jsonb_array_elements(prop.property_value::jsonb) elem WHERE prop.host_id = iapp.host_id AND prop.instance_app_id = iapp.instance_app_id AND prop.property_id = iapp.property_id AND jsonb_typeof(prop.property_value::jsonb) = 'array'), '[]'::jsonb)::text
                                ELSE (SELECT property_value FROM instance_app_property_t latest WHERE latest.host_id = iapp.host_id AND latest.instance_app_id = iapp.instance_app_id AND latest.property_id = iapp.property_id ORDER BY latest.update_ts DESC LIMIT 1)
                            END AS effective_value
                        FROM instance_app_property_t iapp
                        JOIN config_property_t cp ON iapp.property_id = cp.property_id
                        WHERE iapp.host_id = ? -- p_host_id 7
                          AND iapp.instance_app_id IN (SELECT instance_app_id FROM RelevantInstanceApps)
                        GROUP BY iapp.host_id, iapp.instance_app_id, iapp.property_id, cp.value_type
                    ),
                    -- Combine all levels with priority
                    AllOverrides AS (
                        -- Priority 10: Instance App API (highest) - Requires aggregating the merged results if multiple app/api combos apply to the instance
                        SELECT
                            m_iaap.property_id,
                            -- Need final merge/latest logic here if multiple app/api combos apply to the SAME instance_id and define the SAME property_id
                            -- Assuming for now we take the first one found or need more complex logic if merge is needed *again* at this stage
                            -- For simplicity, let's assume we just take MAX effective value if multiple rows exist per property_id for the instance
                            MAX(m_iaap.effective_value) as property_value, -- This MAX might not be right for JSON, need specific logic if merging across app/api combos is needed here
                            10 AS priority_level
                        FROM Merged_Instance_App_Api_Properties m_iaap
                        -- No additional instance filter needed if CTEs were already filtered by RelevantInstanceApps/Apis linked to p_instance_id
                        GROUP BY m_iaap.property_id -- Group to handle multiple app/api links potentially setting the same property for the instance
                        UNION ALL
                        -- Priority 20: Instance API
                        SELECT
                            m_iap.property_id,
                            MAX(m_iap.effective_value) as property_value, -- Similar merge concern as above
                            20 AS priority_level
                        FROM Merged_Instance_Api_Properties m_iap
                        GROUP BY m_iap.property_id
                        UNION ALL
                        -- Priority 30: Instance App
                        SELECT
                            m_iapp.property_id,
                            MAX(m_iapp.effective_value) as property_value, -- Similar merge concern
                            30 AS priority_level
                        FROM Merged_Instance_App_Properties m_iapp
                        GROUP BY m_iapp.property_id
                        UNION ALL
                        -- Priority 40: Instance
                        SELECT
                            ip.property_id,
                            ip.property_value,
                            40 AS priority_level
                        FROM instance_property_t ip
                        WHERE ip.host_id = ? -- p_host_id 8
                          AND ip.instance_id = ? -- p_instance_id 9
                        UNION ALL
                        -- Priority 50: Product Version
                        SELECT
                            pvp.property_id,
                            pvp.property_value,
                            50 AS priority_level
                        FROM product_version_property_t pvp
                        JOIN product_version_t pv ON pv.host_id = pvp.host_id AND pv.product_version_id = pvp.product_version_id
                        WHERE pvp.host_id = ?  -- pvp.host_id 10
                        AND pv.product_id = ? AND pv.product_version = ?  -- pv.product_id 11, pv.product_version 12
                        UNION ALL
                        -- Priority 60: Environment
                        SELECT
                            ep.property_id,
                            ep.property_value,
                            60 AS priority_level
                        FROM environment_property_t ep
                        WHERE ep.host_id = ? -- p_host_id 13
                          AND ep.environment = ? -- v_environment 14
                        UNION ALL
                        -- Priority 70: Product (Host independent)
                        SELECT
                            pp.property_id,
                            pp.property_value,
                            70 AS priority_level
                        FROM product_property_t pp
                        WHERE pp.product_id = ? -- v_product_id 15
                        UNION ALL
                        -- Priority 100: Default values
                        SELECT
                            cp.property_id,
                            cp.property_value, -- Default value
                            100 AS priority_level
                        FROM config_property_t cp
                        -- Optimization: Filter defaults to only those applicable to the product version?
                        -- JOIN product_version_config_property_t pvcp ON cp.property_id = pvcp.property_id
                        -- WHERE pvcp.host_id = ? AND pvcp.product_version_id = ? -- p_host_id 15 AND v_product_version_id 16
                    ),
                    RankedOverrides AS (
                        SELECT
                            ao.property_id,
                            ao.property_value,
                            ao.priority_level,
                            ROW_NUMBER() OVER (PARTITION BY ao.property_id ORDER BY ao.priority_level ASC) as rn
                        FROM AllOverrides ao
                        WHERE ao.property_value IS NOT NULL -- Exclude levels where the value was NULL (unless NULL is a valid override)
                    )
                    -- Final Selection for Snapshot Table
                    SELECT
                        -- snapshot_id needs to be added here or during INSERT
                        cp.config_id, -- Need config_id and property_name for the target table
                        cp.property_name,
                        c.config_phase,
                        ro.property_value,
                        cp.property_type,
                        cp.value_type,
                        ro.priority_level -- Include priority level for source_level mapping
                    FROM RankedOverrides ro
                    JOIN config_property_t cp ON ro.property_id = cp.property_id
                    JOIN config_t c ON cp.config_id = c.config_id
                    WHERE ro.rn = 1;

                """;

        String insertSql = """
            INSERT INTO config_snapshot_property_t
            (snapshot_property_id, snapshot_id, config_phase, config_id, property_id, property_name,
             property_type, property_value, value_type, source_level)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """;
        // Prepare the aggregation query
        try (PreparedStatement selectStmt = conn.prepareStatement(selectSql);
             PreparedStatement insertStmt = conn.prepareStatement(insertSql)) {

            // Set ALL parameters for the AGGREGATE_EFFECTIVE_CONFIG_SQL query
            int paramIndex = 1;
            // Parameters for RelevantInstanceApis CTE
            selectStmt.setObject(paramIndex++, hostId);      // 1: p_host_id for RelevantInstanceApis
            selectStmt.setObject(paramIndex++, instanceId);  // 2: p_instance_id for RelevantInstanceApis
            // Parameters for RelevantInstanceApps CTE
            selectStmt.setObject(paramIndex++, hostId);      // 3: p_host_id for RelevantInstanceApps
            selectStmt.setObject(paramIndex++, instanceId);  // 4: p_instance_id for RelevantInstanceApps
            // Parameters for Merged_Instance_App_Api_Properties CTE
            selectStmt.setObject(paramIndex++, hostId);      // 5: p_host_id for Merged_Instance_App_Api_Properties
            // Parameters for Merged_Instance_Api_Properties CTE
            selectStmt.setObject(paramIndex++, hostId);      // 6: p_host_id for Merged_Instance_Api_Properties
            // Parameters for Merged_Instance_App_Properties CTE
            selectStmt.setObject(paramIndex++, hostId);      // 7: p_host_id for Merged_Instance_App_Properties
            // Parameters for AllOverrides CTE (Instance)
            selectStmt.setObject(paramIndex++, hostId);      // 8: p_host_id for Instance
            selectStmt.setObject(paramIndex++, instanceId);  // 9: p_instance_id for Instance
            // Parameters for AllOverrides CTE (Product Version)
            selectStmt.setObject(paramIndex++, hostId);      // 10: pvp.host_id for Product Version
            selectStmt.setString(paramIndex++, scope.productId());  // 11: pv.product_id for Product Version
            selectStmt.setString(paramIndex++, scope.productVersion()); // 12: pv.product_version for Product Version
            // Parameters for AllOverrides CTE (Environment)
            selectStmt.setObject(paramIndex++, hostId);      // 13: ep.host_id for Environment
            selectStmt.setString(paramIndex++, scope.environment()); // 14: v_environment for Environment
            // Parameters for AllOverrides CTE (Product)
            selectStmt.setString(paramIndex++, scope.productId());   // 15: pp.product_id for Product

            try (ResultSet rs = selectStmt.executeQuery()) {
                int batchCount = 0;
                while (rs.next()) {
                    insertStmt.setObject(1, UuidUtil.getUUID()); // snapshot_property_id
                    insertStmt.setObject(2, snapshotId);
                    insertStmt.setString(3, rs.getString("config_phase"));
                    insertStmt.setObject(4, rs.getObject("config_id", UUID.class)); // config_id from select query
                    insertStmt.setObject(5, rs.getObject("property_id", UUID.class));
                    insertStmt.setString(6, rs.getString("property_name"));
                    insertStmt.setString(7, rs.getString("property_type"));
                    insertStmt.setString(8, rs.getString("property_value"));
                    insertStmt.setString(9, rs.getString("value_type"));
                    insertStmt.setString(10, mapPriorityToSourceLevel(rs.getInt("priority_level"))); // Map numeric priority back to level name

                    insertStmt.addBatch();
                    batchCount++;

                    if (batchCount % 100 == 0) { // Execute batch periodically
                        insertStmt.executeBatch();
                    }
                }
                if (batchCount % 100 != 0) { // Execute remaining batch
                    insertStmt.executeBatch();
                }
            }
        }
    }

    // Helper to map priority back to source level name
    private String mapPriorityToSourceLevel(int priority) {
        return switch (priority) {
            case 10 -> "instance_app_api";
            case 20 -> "instance_api";
            case 30 -> "instance_app";
            case 40 -> "instance";
            case 50 -> "product_version";
            case 60 -> "environment";
            case 70 -> "product";
            case 100 -> "default";
            default -> "unknown";
        };
    }

    // --- Methods for Snapshotting Individual Override Tables ---

    private void snapshotInstanceProperties(Connection conn, UUID snapshotId, UUID hostId, UUID instanceId) throws SQLException {
        String sql = """
            INSERT INTO snapshot_instance_property_t
            (snapshot_id, host_id, instance_id, property_id, property_value, update_user, update_ts)
            SELECT ?, host_id, instance_id, property_id, property_value, update_user, update_ts
            FROM instance_property_t
            WHERE host_id = ? AND instance_id = ?
            """;
        try (PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setObject(1, snapshotId);
            ps.setObject(2, hostId);
            ps.setObject(3, instanceId);
            ps.executeUpdate();
        }
    }

    private void snapshotInstanceApiProperties(Connection conn, UUID snapshotId, UUID hostId, UUID instanceId) throws SQLException {
        // Find relevant instance_api_ids first
        List<UUID> apiIds = findRelevantInstanceApiIds(conn, hostId, instanceId);
        if (apiIds.isEmpty()) return; // No API overrides for this instance

        String sql = """
            INSERT INTO snapshot_instance_api_property_t
            (snapshot_id, host_id, instance_api_id, property_id, property_value, update_user, update_ts)
            SELECT ?, host_id, instance_api_id, property_id, property_value, update_user, update_ts
            FROM instance_api_property_t
            WHERE host_id = ? AND instance_api_id = ANY(?) -- Use ANY with array for multiple IDs
            """;
        try (PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setObject(1, snapshotId);
            ps.setObject(2, hostId);
            // Create a SQL Array from the List of UUIDs
            Array sqlArray = conn.createArrayOf("uuid", apiIds.toArray()); // Use lowercase "uuid" for PostgreSQL
            ps.setArray(3, sqlArray);
            ps.executeUpdate();
            sqlArray.free(); // Release array resources
        }
    }

    private void snapshotInstanceAppProperties(Connection conn, UUID snapshotId, UUID hostId, UUID instanceId) throws SQLException {
        // Find relevant instance_app_ids first
        List<UUID> appIds = findRelevantInstanceAppIds(conn, hostId, instanceId);
        if (appIds.isEmpty()) return; // No App overrides for this instance

        String sql = """
                INSERT INTO snapshot_instance_app_property_t
                (snapshot_id, host_id, instance_app_id, property_id, property_value, update_user, update_ts)
                SELECT ?, host_id, instance_app_id, property_id, property_value, update_user, update_ts
                FROM instance_app_property_t
                WHERE host_id = ? AND instance_app_id = ANY(?) -- Parameter is a SQL Array of relevant instance_app_ids
            """;
        try (PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setObject(1, snapshotId);
            ps.setObject(2, hostId);
            // Create a SQL Array from the List of UUIDs
            Array sqlArray = conn.createArrayOf("uuid", appIds.toArray());
            ps.setArray(3, sqlArray);
            ps.executeUpdate();
            sqlArray.free(); // Release array resources
        }
    }

    private void snapshotInstanceAppApiProperties(Connection conn, UUID snapshotId, UUID hostId, UUID instanceId) throws SQLException {
        List<UUID> apiIds = findRelevantInstanceApiIds(conn, hostId, instanceId);
        List<UUID> appIds = findRelevantInstanceAppIds(conn, hostId, instanceId);

        if (appIds.isEmpty() || apiIds.isEmpty()) return; // Nothing to snapshot if either list is empty

        String sql = """
                INSERT INTO snapshot_instance_app_api_property_t
                (snapshot_id, host_id, instance_app_id, instance_api_id, property_id, property_value, update_user, update_ts)
                SELECT ?, host_id, instance_app_id, instance_api_id, property_id, property_value, update_user, update_ts
                FROM instance_app_api_property_t
                WHERE host_id = ?
                  AND instance_app_id = ANY(?) -- SQL Array of relevant instance_app_ids
                  AND instance_api_id = ANY(?) -- SQL Array of relevant instance_api_ids
            """;
        try (PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setObject(1, snapshotId);
            ps.setObject(2, hostId);
            // Create a SQL Array from the List of UUIDs
            Array sqlAppArray = conn.createArrayOf("uuid", appIds.toArray());
            ps.setArray(3, sqlAppArray);
            Array sqlApiArray = conn.createArrayOf("uuid", apiIds.toArray());
            ps.setArray(4, sqlApiArray);
            ps.executeUpdate();
            sqlAppArray.free(); // Release array resources
            sqlApiArray.free(); // Release array resources
        }
    }

    private void snapshotEnvironmentProperties(Connection conn, UUID snapshotId, UUID hostId, String environment) throws SQLException {
        if (environment == null || environment.isEmpty()) return; // No environment scope
        String sql = """
             INSERT INTO snapshot_environment_property_t
             (snapshot_id, host_id, environment, property_id, property_value, update_user, update_ts)
             SELECT ?, host_id, environment, property_id, property_value, update_user, update_ts
             FROM environment_property_t
             WHERE host_id = ? AND environment = ?
             """;
        try (PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setObject(1, snapshotId);
            ps.setObject(2, hostId);
            ps.setString(3, environment);
            ps.executeUpdate();
        }
    }

    private void snapshotProductVersionProperties(Connection conn, UUID snapshotId, UUID hostId, UUID productVersionId) throws SQLException {
        if (productVersionId == null) return;
        String sql = """
              INSERT INTO snapshot_product_version_property_t
              (snapshot_id, host_id, product_version_id, property_id, property_value, update_user, update_ts)
              SELECT ?, host_id, product_version_id, property_id, property_value, update_user, update_ts
              FROM product_version_property_t
              WHERE host_id = ? AND product_version_id = ?
              """;
        try (PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setObject(1, snapshotId);
            ps.setObject(2, hostId);
            ps.setObject(3, productVersionId);
            ps.executeUpdate();
        }
    }

    private void snapshotProductProperties(Connection conn, UUID snapshotId, String productId) throws SQLException {
        if (productId == null || productId.isEmpty()) return;
        String sql = """
               INSERT INTO snapshot_product_property_t
               (snapshot_id, product_id, property_id, property_value, update_user, update_ts)
               SELECT ?, product_id, property_id, property_value, update_user, update_ts
               FROM product_property_t
               WHERE product_id = ?
               """;
        try (PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setObject(1, snapshotId);
            ps.setString(2, productId);
            ps.executeUpdate();
        }
    }

    // --- Helper method to find associated instance_api_ids ---
    private List<UUID> findRelevantInstanceApiIds(Connection conn, UUID hostId, UUID instanceId) throws SQLException {
        List<UUID> ids = new ArrayList<>();
        String sql = "SELECT instance_api_id FROM instance_api_t WHERE host_id = ? AND instance_id = ?";
        try (PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setObject(1, hostId);
            ps.setObject(2, instanceId);
            try (ResultSet rs = ps.executeQuery()) {
                while(rs.next()) {
                    ids.add(rs.getObject("instance_api_id", UUID.class));
                }
            }
        }
        return ids;
    }

    // --- Helper method to find associated instance_app_ids ---
    private List<UUID> findRelevantInstanceAppIds(Connection conn, UUID hostId, UUID instanceId) throws SQLException {
        List<UUID> ids = new ArrayList<>();
        String sql = "SELECT instance_app_id FROM instance_app_t WHERE host_id = ? AND instance_id = ?";
        try (PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setObject(1, hostId);
            ps.setObject(2, instanceId);
            try (ResultSet rs = ps.executeQuery()) {
                while(rs.next()) {
                    ids.add(rs.getObject("instance_app_id", UUID.class));
                }
            }
        }
        return ids;
    }

    @Override
    public void rollbackConfigInstance(Connection conn, Map<String, Object> event) throws SQLException, Exception {
        final String DELETE_INSTANCE_PROPS_SQL = "DELETE FROM instance_property_t WHERE host_id = ? AND instance_id = ?";
        final String DELETE_INSTANCE_API_PROPS_SQL = "DELETE FROM instance_api_property_t WHERE host_id = ? AND instance_api_id = ANY(?)";
        final String DELETE_INSTANCE_APP_PROPS_SQL = "DELETE FROM instance_app_property_t WHERE host_id = ? AND instance_app_id = ANY(?)";
        final String DELETE_INSTANCE_APP_API_PROPS_SQL = "DELETE FROM instance_app_api_property_t WHERE host_id = ? AND instance_app_id = ANY(?) AND instance_api_id = ANY(?)";
        final String DELETE_ENVIRONMENT_PROPS_SQL = "DELETE FROM environment_property_t WHERE host_id = ? AND environment = ?";
        final String DELETE_PRODUCT_VERSION_PROPS_SQL = "DELETE FROM product_version_property_t WHERE host_id = ? AND product_version_id = ?";
        final String DELETE_PRODUCT_PROPS_SQL = "DELETE FROM product_property_t WHERE product_id = ?";

        // INSERT ... SELECT Statements (From SNAPSHOT tables to LIVE tables)
        final String INSERT_INSTANCE_PROPS_SQL = """
        INSERT INTO instance_property_t
        (host_id, instance_id, property_id, property_value, update_user, update_ts)
        SELECT host_id, instance_id, property_id, property_value, update_user, ? AS update_user_new, ? AS update_ts_new
        FROM snapshot_instance_property_t
        WHERE snapshot_id = ? AND host_id = ? AND instance_id = ?
        """;
        final String INSERT_INSTANCE_API_PROPS_SQL = """
        INSERT INTO instance_api_property_t
        (host_id, instance_api_id, property_id, property_value, update_user, update_ts)
        SELECT host_id, instance_api_id, property_id, property_value, update_user, ? AS update_user_new, ? AS update_ts_new
        FROM snapshot_instance_api_property_t
        WHERE snapshot_id = ? AND host_id = ? AND instance_api_id = ANY(?)
        """;
        final String INSERT_INSTANCE_APP_PROPS_SQL = """
        INSERT INTO instance_app_property_t
        (host_id, instance_app_id, property_id, property_value, update_user, update_ts)
        SELECT host_id, instance_app_id, property_id, property_value, update_user, ? AS update_user_new, ? AS update_ts_new
        FROM snapshot_instance_app_property_t
        WHERE snapshot_id = ? AND host_id = ? AND instance_app_id = ANY(?)
        """;
        final String INSERT_INSTANCE_APP_API_PROPS_SQL = """
        INSERT INTO instance_app_api_property_t
        (host_id, instance_app_id, instance_api_id, property_id, property_value, update_user, update_ts)
        SELECT host_id, instance_app_id, instance_api_id, property_id, property_value, update_user, ? AS update_user_new, ? AS update_ts_new
        FROM snapshot_instance_app_api_property_t
        WHERE snapshot_id = ? AND host_id = ? AND instance_app_id = ANY(?) AND instance_api_id = ANY(?)
        """;
        final String INSERT_ENVIRONMENT_PROPS_SQL = """
        INSERT INTO environment_property_t
        (host_id, environment, property_id, property_value, update_user, update_ts)
        SELECT host_id, environment, property_id, property_value, update_user, ? AS update_user_new, ? AS update_ts_new
        FROM snapshot_environment_property_t
        WHERE snapshot_id = ? AND host_id = ? AND environment = ?
        """;
        final String INSERT_PRODUCT_VERSION_PROPS_SQL = """
        INSERT INTO product_version_property_t
        (host_id, product_version_id, property_id, property_value, update_user, update_ts)
        SELECT host_id, product_version_id, property_id, property_value, update_user, ? AS update_user_new, ? AS update_ts_new
        FROM snapshot_product_version_property_t
        WHERE snapshot_id = ? AND host_id = ? AND product_version_id = ?
        """;
        final String INSERT_PRODUCT_PROPS_SQL = """
        INSERT INTO product_property_t
        (product_id, property_id, property_value, update_user, update_ts)
        SELECT product_id, property_id, property_value, update_user, ? AS update_user_new, ? AS update_ts_new
        FROM snapshot_product_property_t
        WHERE snapshot_id = ? AND product_id = ?
        """;


        // 1. Extract Input Parameters
        UUID snapshotId = UUID.fromString((String) event.get("snapshotId"));
        UUID hostId = UUID.fromString((String) event.get("hostId"));
        UUID instanceId = UUID.fromString((String) event.get("instanceId"));
        String eventUser = (String) event.get(Constants.USER); // User who triggered the event
        OffsetDateTime eventTime = OffsetDateTime.parse((String)event.get(CloudEventV1.TIME));


        List<UUID> currentApiIds = null;
        List<UUID> currentAppIds = null;
        DerivedScope scope = null;

        try {
            // --- Pre-computation: Find CURRENT associated IDs for DELETE scope ---
            currentApiIds = findRelevantInstanceApiIds(conn, hostId, instanceId);
            currentAppIds = findRelevantInstanceAppIds(conn, hostId, instanceId);
            scope = deriveScopeInfo(conn, hostId, instanceId); // Get current scope to match for environment/product deletes

            logger.info("Starting rollback for instance {} (host {}) to snapshot {}", instanceId, hostId, snapshotId);

            // --- Execute Deletes from LIVE tables ---
            executeDelete(conn, DELETE_INSTANCE_PROPS_SQL, hostId, instanceId);

            if (!currentApiIds.isEmpty()) {
                executeDeleteWithArray(conn, DELETE_INSTANCE_API_PROPS_SQL, hostId, currentApiIds);
            }
            if (!currentAppIds.isEmpty()) {
                executeDeleteWithArray(conn, DELETE_INSTANCE_APP_PROPS_SQL, hostId, currentAppIds);
            }
            if (!currentApiIds.isEmpty() && !currentAppIds.isEmpty()) { // Only delete if both existing.
                executeDeleteWithTwoArrays(conn, DELETE_INSTANCE_APP_API_PROPS_SQL, hostId, currentAppIds, currentApiIds);
            }
            if (scope != null && scope.environment() != null) {
                executeDeleteEnvironmentProps(conn, DELETE_ENVIRONMENT_PROPS_SQL, hostId, scope.environment());
            }
            if (scope != null && scope.productVersionId() != null) {
                executeDeleteProductVersionProps(conn, DELETE_PRODUCT_VERSION_PROPS_SQL, hostId, scope.productVersionId());
            }
            if (scope != null && scope.productId() != null) {
                executeDeleteProductProps(conn, DELETE_PRODUCT_PROPS_SQL, scope.productId());
            }


            // --- Execute Inserts from SNAPSHOT tables ---
            executeInsertSelect(conn, INSERT_INSTANCE_PROPS_SQL, snapshotId, hostId, instanceId, eventUser, eventTime);

            if (!currentApiIds.isEmpty()) { // Only insert if corresponding API instances existed in snapshot area
                executeInsertSelectWithArray(conn, INSERT_INSTANCE_API_PROPS_SQL, snapshotId, hostId, currentApiIds, eventUser, eventTime);
            }
            if (!currentAppIds.isEmpty()) { // Only insert if corresponding App instances existed in snapshot area
                executeInsertSelectWithArray(conn, INSERT_INSTANCE_APP_PROPS_SQL, snapshotId, hostId, currentAppIds, eventUser, eventTime);
            }
            if (!currentApiIds.isEmpty() && !currentAppIds.isEmpty()) {
                executeInsertSelectWithTwoArrays(conn, INSERT_INSTANCE_APP_API_PROPS_SQL, snapshotId, hostId, currentAppIds, currentApiIds, eventUser, eventTime);
            }
            if (scope != null && scope.environment() != null) {
                executeInsertSelectEnvironmentProps(conn, INSERT_ENVIRONMENT_PROPS_SQL, snapshotId, hostId, scope.environment(), eventUser, eventTime);
            }
            if (scope != null && scope.productVersionId() != null) {
                executeInsertSelectProductVersionProps(conn, INSERT_PRODUCT_VERSION_PROPS_SQL, snapshotId, hostId, scope.productVersionId(), eventUser, eventTime);
            }
            if (scope != null && scope.productId() != null) {
                executeInsertSelectProductProps(conn, INSERT_PRODUCT_PROPS_SQL, snapshotId, scope.productId(), eventUser, eventTime);
            }

            logger.info("Successfully rolled back instance {} (host {}) to snapshot {}", instanceId, hostId, snapshotId);
            notificationService.insertNotification(event, true, null); // Notify success
        } catch (SQLException e) {
            logger.error("SQLException during rollback for instance {} to snapshot {}: {}", instanceId, snapshotId, e.getMessage(), e);
            notificationService.insertNotification(event, false, e.getMessage());
            throw e;
        } catch (Exception e) { // Catch other potential errors
            logger.error("Exception during rollback for instance {} to snapshot {}: {}", instanceId, snapshotId, e.getMessage(), e);
            notificationService.insertNotification(event, false, e.getMessage());
            throw e;
        }
    }

    private void executeDelete(Connection conn, String sql, UUID hostId, UUID instanceId) throws SQLException {
        try (PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setObject(1, hostId);
            ps.setObject(2, instanceId);
            int rowsAffected = ps.executeUpdate();
            logger.debug("Deleted {} rows from {} for instance {}", rowsAffected, getTableNameFromDeleteSql(sql), instanceId);
        }
    }

    private void executeDeleteWithArray(Connection conn, String sql, UUID hostId, List<UUID> idList) throws SQLException {
        if (idList == null || idList.isEmpty()) return; // Nothing to delete if list is empty
        try (PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setObject(1, hostId);
            Array sqlArray = conn.createArrayOf("uuid", idList.toArray());
            ps.setArray(2, sqlArray);
            int rowsAffected = ps.executeUpdate();
            logger.debug("Deleted {} rows from {} for {} IDs", rowsAffected, getTableNameFromDeleteSql(sql), idList.size());
            sqlArray.free();
        }
    }

    private void executeDeleteWithTwoArrays(Connection conn, String sql, UUID hostId, List<UUID> idList1, List<UUID> idList2) throws SQLException {
        if (idList1 == null || idList1.isEmpty() || idList2 == null || idList2.isEmpty()) return;
        try (PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setObject(1, hostId);
            Array sqlArray1 = conn.createArrayOf("uuid", idList1.toArray());
            Array sqlArray2 = conn.createArrayOf("uuid", idList2.toArray());
            ps.setArray(2, sqlArray1);
            ps.setArray(3, sqlArray2);
            int rowsAffected = ps.executeUpdate();
            logger.debug("Deleted {} rows from {} for {}x{} IDs", rowsAffected, getTableNameFromDeleteSql(sql), idList1.size(), idList2.size());
            sqlArray1.free();
            sqlArray2.free();
        }
    }

    private void executeDeleteEnvironmentProps(Connection conn, String sql, UUID hostId, String environment) throws SQLException {
        try (PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setObject(1, hostId);
            ps.setString(2, environment);
            int rowsAffected = ps.executeUpdate();
            logger.debug("Deleted {} rows from {} for host {} environment {}", rowsAffected, getTableNameFromDeleteSql(sql), hostId, environment);
        }
    }

    private void executeDeleteProductVersionProps(Connection conn, String sql, UUID hostId, UUID productVersionId) throws SQLException {
        try (PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setObject(1, hostId);
            ps.setObject(2, productVersionId);
            int rowsAffected = ps.executeUpdate();
            logger.debug("Deleted {} rows from {} for host {} productVersionId {}", rowsAffected, getTableNameFromDeleteSql(sql), hostId, productVersionId);
        }
    }

    private void executeDeleteProductProps(Connection conn, String sql, String productId) throws SQLException {
        try (PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setString(1, productId);
            int rowsAffected = ps.executeUpdate();
            logger.debug("Deleted {} rows from {} for productId {}", rowsAffected, getTableNameFromDeleteSql(sql), productId);
        }
    }

    private void executeInsertSelect(Connection conn, String sql, UUID snapshotId, UUID hostId, UUID instanceId, String updateUser, OffsetDateTime updateTs) throws SQLException {
        try (PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setObject(1, snapshotId);
            ps.setString(2, updateUser); // Update_user_new
            ps.setObject(3, updateTs);   // Update_ts_new
            ps.setObject(4, hostId);
            ps.setObject(5, instanceId);
            int rowsAffected = ps.executeUpdate();
            logger.debug("Inserted {} rows into {} from snapshot {}", rowsAffected, getTableNameFromInsertSql(sql), snapshotId);
        }
    }

    private void executeInsertSelectWithArray(Connection conn, String sql, UUID snapshotId, UUID hostId, List<UUID> idList, String updateUser, OffsetDateTime updateTs) throws SQLException {
        if (idList == null || idList.isEmpty()) return; // No scope to insert for
        try (PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setObject(1, snapshotId);
            ps.setString(2, updateUser); // Update_user_new
            ps.setObject(3, updateTs);   // Update_ts_new
            ps.setObject(4, hostId);
            Array sqlArray = conn.createArrayOf("uuid", idList.toArray());
            ps.setArray(5, sqlArray);
            int rowsAffected = ps.executeUpdate();
            logger.debug("Inserted {} rows into {} from snapshot {} for {} IDs", rowsAffected, getTableNameFromInsertSql(sql), snapshotId, idList.size());
            sqlArray.free();
        }
    }

    private void executeInsertSelectWithTwoArrays(Connection conn, String sql, UUID snapshotId, UUID hostId, List<UUID> idList1, List<UUID> idList2, String updateUser, OffsetDateTime updateTs) throws SQLException {
        if (idList1 == null || idList1.isEmpty() || idList2 == null || idList2.isEmpty()) return;
        try (PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setObject(1, snapshotId);
            ps.setString(2, updateUser); // Update_user_new
            ps.setObject(3, updateTs);   // Update_ts_new
            ps.setObject(4, hostId);
            Array sqlArray1 = conn.createArrayOf("uuid", idList1.toArray());
            Array sqlArray2 = conn.createArrayOf("uuid", idList2.toArray());
            ps.setArray(5, sqlArray1);
            ps.setArray(6, sqlArray2);
            int rowsAffected = ps.executeUpdate();
            logger.debug("Inserted {} rows into {} from snapshot {} for {}x{} IDs", rowsAffected, getTableNameFromInsertSql(sql), snapshotId, idList1.size(), idList2.size());
            sqlArray1.free();
            sqlArray2.free();
        }
    }

    private void executeInsertSelectEnvironmentProps(Connection conn, String sql, UUID snapshotId, UUID hostId, String environment, String updateUser, OffsetDateTime updateTs) throws SQLException {
        try (PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setObject(1, snapshotId);
            ps.setString(2, updateUser); // Update_user_new
            ps.setObject(3, updateTs);   // Update_ts_new
            ps.setObject(4, hostId);
            ps.setString(5, environment);
            int rowsAffected = ps.executeUpdate();
            logger.debug("Inserted {} rows into {} from snapshot {} for host {} environment {}", rowsAffected, getTableNameFromInsertSql(sql), snapshotId, hostId, environment);
        }
    }

    private void executeInsertSelectProductVersionProps(Connection conn, String sql, UUID snapshotId, UUID hostId, UUID productVersionId, String updateUser, OffsetDateTime updateTs) throws SQLException {
        try (PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setObject(1, snapshotId);
            ps.setString(2, updateUser); // Update_user_new
            ps.setObject(3, updateTs);   // Update_ts_new
            ps.setObject(4, hostId);
            ps.setObject(5, productVersionId);
            int rowsAffected = ps.executeUpdate();
            logger.debug("Inserted {} rows into {} from snapshot {} for host {} productVersionId {}", rowsAffected, getTableNameFromInsertSql(sql), snapshotId, hostId, productVersionId);
        }
    }

    private void executeInsertSelectProductProps(Connection conn, String sql, UUID snapshotId, String productId, String updateUser, OffsetDateTime updateTs) throws SQLException {
        try (PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setObject(1, snapshotId);
            ps.setString(2, updateUser); // Update_user_new
            ps.setObject(3, updateTs);   // Update_ts_new
            ps.setString(4, productId);
            int rowsAffected = ps.executeUpdate();
            logger.debug("Inserted {} rows into {} from snapshot {} for productId {}", rowsAffected, getTableNameFromInsertSql(sql), snapshotId, productId);
        }
    }


    // --- Optional: Helper to get table name from SQL for logging ---
    private String getTableNameFromDeleteSql(String sql) {
        // Simple parsing, might need adjustment
        try { return sql.split("FROM ")[1].split(" ")[0]; } catch (Exception e) { return "[unknown table]"; }
    }
    private String getTableNameFromInsertSql(String sql) {
        try { return sql.split("INTO ")[1].split("\\s+")[0]; } catch (Exception e) { return "[unknown table]"; }
    }

    @Override
    public Result<String> getConfigInstance(int offset, int limit, String hostId, String instanceId,
                                            String instanceName, String configId, String configName,
                                            String propertyId, String propertyName, String propertyValue) {
        Result<String> result = null;
        String s =
                """
                SELECT COUNT(*) OVER () AS total,
                ip.host_id, ip.instance_id, i.instance_name, p.config_id, c.config_name, ip.property_id,
                p.property_name, ip.property_value, p.required, p.property_desc, p.property_type, p.resource_type, p.value_type, c.config_type,
                c.config_desc, c.class_path, ip.update_user, ip.update_ts, ip.aggregate_version
                FROM instance_property_t ip
                INNER JOIN config_property_t p ON p.property_id = ip.property_id
                INNER JOIN instance_t i ON i.instance_id = ip.instance_id
                INNER JOIN config_t c ON p.config_id = c.config_id
                WHERE 1=1
                """;
        StringBuilder sqlBuilder = new StringBuilder();
        sqlBuilder.append(s).append("\n");

        List<Object> parameters = new ArrayList<>();

        StringBuilder whereClause = new StringBuilder();
        SqlUtil.addCondition(whereClause, parameters, "ip.host_id", hostId != null ? UUID.fromString(hostId) : null);
        SqlUtil.addCondition(whereClause, parameters, "ip.instance_id", instanceId != null ? UUID.fromString(instanceId) : null);
        SqlUtil.addCondition(whereClause, parameters, "i.instance_name", instanceName);
        SqlUtil.addCondition(whereClause, parameters, "p.config_id", configId != null ? UUID.fromString(configId) : null);
        SqlUtil.addCondition(whereClause, parameters, "c.config_name", configName); // Filter by config_name
        SqlUtil.addCondition(whereClause, parameters, "p.property_id", propertyId != null ? UUID.fromString(propertyId) : null);
        SqlUtil.addCondition(whereClause, parameters, "ip.property_name", propertyName);
        SqlUtil.addCondition(whereClause, parameters, "ip.property_value", propertyValue);

        if (!whereClause.isEmpty()) {
            sqlBuilder.append("AND ").append(whereClause);
        }

        sqlBuilder.append(" ORDER BY ip.host_id, ip.instance_id, p.config_id, p.display_order\n" +
                "LIMIT ? OFFSET ?");

        parameters.add(limit);
        parameters.add(offset);

        String sql = sqlBuilder.toString();
        int total = 0;
        List<Map<String, Object>> instanceProperties = new ArrayList<>();

        try (Connection connection = ds.getConnection();
             PreparedStatement preparedStatement = connection.prepareStatement(sql)) {

            for (int i = 0; i < parameters.size(); i++) {
                preparedStatement.setObject(i + 1, parameters.get(i));
            }

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
                    map.put("configId", resultSet.getObject("config_id", UUID.class));
                    map.put("configName", resultSet.getString("config_name"));
                    map.put("propertyId", resultSet.getObject("property_id", UUID.class));
                    map.put("propertyName", resultSet.getString("property_name"));
                    map.put("propertyValue", resultSet.getString("property_value"));
                    map.put("required", resultSet.getBoolean("required"));
                    map.put("propertyDesc", resultSet.getString("property_desc"));
                    map.put("propertyType", resultSet.getString("property_type"));
                    map.put("resourceType", resultSet.getString("resource_type"));
                    map.put("valueType", resultSet.getString("value_type"));
                    map.put("configType", resultSet.getString("config_type"));
                    map.put("configDesc", resultSet.getString("config_desc"));
                    map.put("classPath", resultSet.getString("class_path"));
                    map.put("updateUser", resultSet.getString("update_user"));
                    map.put("updateTs", resultSet.getObject("update_ts") != null ? resultSet.getObject("update_ts", OffsetDateTime.class) : null);
                    map.put("aggregateVersion", resultSet.getInt("aggregate_version"));
                    instanceProperties.add(map);
                }
            }

            Map<String, Object> resultMap = new HashMap<>();
            resultMap.put("total", total);
            resultMap.put("instanceProperties", instanceProperties);
            result = Success.of(JsonMapper.toJson(resultMap));

        } catch (SQLException e) {
            logger.error("SQLException:", e);
            result = Failure.of(new Status("SQL_EXCEPTION", e.getMessage()));
        } catch (Exception e) {
            logger.error("Exception:", e);
            result = Failure.of(new Status("GENERIC_EXCEPTION", e.getMessage()));
        }
        return result;
    }

    @Override
    public void createConfigInstanceFile(Connection conn, Map<String, Object> event) throws SQLException, Exception {
        final String sql = "INSERT INTO instance_file_t (host_id, instance_file_id, instance_id, file_type, " +
                "file_name, file_value, file_desc, expiration_ts, update_user, update_ts, aggregate_version) " +
                "VALUES (?, ?, ?, ?, ?,  ?, ?, ?, ?, ?,  ?)";
        Map<String, Object> map = (Map<String, Object>)event.get(PortalConstants.DATA);
        String hostId = (String)event.get(Constants.HOST);
        String instanceFileId = (String)map.get("instanceFileId");
        String instanceId = (String)map.get("instanceId");
        long newAggregateVersion = SqlUtil.getNewAggregateVersion(event);

        try (PreparedStatement statement = conn.prepareStatement(sql)) {
            statement.setObject(1, UUID.fromString(hostId));
            statement.setObject(2, UUID.fromString(instanceFileId));
            statement.setObject(3, UUID.fromString(instanceId));
            statement.setString(4, (String)map.get("fileType"));
            statement.setString(5, (String)map.get("fileName"));
            statement.setString(6, (String)map.get("fileValue"));
            statement.setString(7, (String)map.get("fileDesc"));
            String expirationTs = (String)map.get("expirationTs");
            if(expirationTs != null) {
                statement.setObject(8, OffsetDateTime.parse((String) map.get("expirationTs")));
            } else {
                statement.setNull(8, Types.TIMESTAMP);
            }
            statement.setString(9, (String)event.get(Constants.USER));
            statement.setObject(10, OffsetDateTime.parse((String)event.get(CloudEventV1.TIME)));
            statement.setLong(11, newAggregateVersion);

            int count = statement.executeUpdate();
            if (count == 0) {
                throw new SQLException(String.format("Failed during createConfigInstanceFile for hostId  %s instanceFileId %s with aggregateVersion %d", hostId, instanceFileId, newAggregateVersion));
            }
        } catch (SQLException e) {
            logger.error("SQLException during createConfigInstanceFile for hostId {} instanceFileId {} instanceId {} aggregateVersion {}: {}", hostId, instanceFileId, instanceId, newAggregateVersion, e.getMessage(), e);
            throw e;
        } catch (Exception e) {
            logger.error("Exception during createConfigInstanceFile for hostId {} instanceFileId {} instanceId {} aggregateVersion {}: {}", hostId, instanceFileId, instanceId, newAggregateVersion, e.getMessage(), e);
            throw e;
        }
    }

    private boolean queryConfigInstanceFileExists(Connection conn, String hostId, String instanceFileId) throws SQLException {
        final String sql =
                """
                SELECT COUNT(*) FROM instance_file_t WHERE host_id = ? AND instance_file_id = ?
                """;
        try (PreparedStatement pst = conn.prepareStatement(sql)) {
            pst.setObject(1, UUID.fromString(hostId));
            pst.setObject(2, UUID.fromString(instanceFileId));
            try (ResultSet rs = pst.executeQuery()) {
                return rs.next() && rs.getInt(1) > 0;
            }
        }
    }

    @Override
    public void updateConfigInstanceFile(Connection conn, Map<String, Object> event) throws SQLException, Exception {
        final String sql =
                """
                UPDATE instance_file_t SET file_type = ?, file_name = ?, file_value = ?,
                file_desc = ?, expiration_ts = ?, update_user = ?, update_ts = ?, aggregate_version = ?
                WHERE host_id = ? AND instance_file_id = ? AND aggregate_version = ?
                """;
        Map<String, Object> map = (Map<String, Object>)event.get(PortalConstants.DATA);
        String hostId = (String)event.get(Constants.HOST);
        String instanceFileId = (String)map.get("instanceFileId");
        long oldAggregateVersion = SqlUtil.getOldAggregateVersion(event);
        long newAggregateVersion = SqlUtil.getNewAggregateVersion(event);

        try (PreparedStatement statement = conn.prepareStatement(sql)) {

            String fileType = (String)map.get("fileType");
            if (fileType != null) {
                statement.setString(1, fileType);
            } else {
                statement.setNull(1, Types.VARCHAR);
            }
            String fileName = (String)map.get("fileName");
            if (fileName != null) {
                statement.setString(2, fileName);
            } else {
                statement.setNull(2, Types.VARCHAR);
            }
            String fileValue = (String)map.get("fileValue");
            if (fileValue != null) {
                statement.setString(3, fileValue);
            } else {
                statement.setNull(3, Types.VARCHAR);
            }
            String fileDesc = (String)map.get("fileDesc");
            if (fileDesc != null) {
                statement.setString(4, fileDesc);
            } else {
                statement.setNull(4, Types.VARCHAR);
            }
            String expirationTs = (String)map.get("expirationTs");
            if (expirationTs != null) {
                statement.setObject(5, OffsetDateTime.parse(expirationTs));
            } else {
                statement.setNull(5, Types.TIMESTAMP);
            }

            statement.setString(6, (String)event.get(Constants.USER));
            statement.setObject(7, OffsetDateTime.parse((String)event.get(CloudEventV1.TIME)));
            statement.setLong(8, newAggregateVersion);

            statement.setObject(9, UUID.fromString(hostId));
            statement.setObject(10, UUID.fromString(instanceFileId));
            statement.setLong(11, oldAggregateVersion);

            int count = statement.executeUpdate();
            if (count == 0) {
                if (queryConfigInstanceFileExists(conn, hostId, instanceFileId)) {
                    throw new ConcurrencyException("Optimistic concurrency conflict during updateConfigInstanceFile for hostId " + hostId + " instanceFileId " + instanceFileId + ". Expected version " + oldAggregateVersion + " but found a different version " + newAggregateVersion + ".");
                } else {
                    throw new SQLException("No record found during updateConfigInstanceFile for hostId " + hostId + " instanceFileId " + instanceFileId + ".");
                }
            }
        } catch (SQLException e) {
            logger.error("SQLException during updateConfigInstanceFile for hostId {} instanceFileId {} (old: {}) -> (new: {}): {}", hostId, instanceFileId, oldAggregateVersion, newAggregateVersion,  e.getMessage(), e);
            throw e;
        } catch (Exception e) {
            logger.error("Exception during updateConfigInstanceFile for hostId {} instanceFileId {} (old: {}) -> (new: {}): {}", hostId, instanceFileId, oldAggregateVersion, newAggregateVersion, e.getMessage(), e);
            throw e;
        }
    }

    @Override
    public void deleteConfigInstanceFile(Connection conn, Map<String, Object> event) throws SQLException, Exception {
        final String sql = "DELETE FROM instance_file_t WHERE host_id = ? AND instance_file_id = ? AND aggregate_version = ?";
        Map<String, Object> map = (Map<String, Object>)event.get(PortalConstants.DATA);
        String hostId = (String)event.get(Constants.HOST);
        String instanceFileId = (String)map.get("instanceFileId");
        long oldAggregateVersion = SqlUtil.getOldAggregateVersion(event);

        try (PreparedStatement statement = conn.prepareStatement(sql)) {
            statement.setObject(1, UUID.fromString(hostId));
            statement.setObject(2, UUID.fromString(instanceFileId));
            statement.setLong(3, oldAggregateVersion);

            int count = statement.executeUpdate();
            if (count == 0) {
                if (queryConfigInstanceFileExists(conn, hostId, instanceFileId)) {
                    throw new ConcurrencyException("Optimistic concurrency conflict during deleteConfigInstanceFile for hostId " + hostId + " instanceFileId " + instanceFileId + " aggregateVersion " + oldAggregateVersion + " but found a different version or already updated.");
                } else {
                    throw new SQLException("No record found during deleteConfigInstanceFile for hostId " + hostId + " instanceFileId " + instanceFileId + ". It might have been already deleted.");
                }
            }
        } catch (SQLException e) {
            logger.error("SQLException during deleteConfigInstanceFile for hostId {} instanceFileId {} aggregateVersion {}: {}", hostId, instanceFileId, oldAggregateVersion, e.getMessage(), e);
            throw e;
        } catch (Exception e) {
            logger.error("Exception during deleteConfigInstanceFile for hostId {} instanceFileId {} aggregateVersion {}: {}", hostId, instanceFileId, oldAggregateVersion, e.getMessage(), e);
            throw e;
        }
    }

    @Override
    public Result<String> getConfigInstanceFile(int offset, int limit, String hostId, String instanceFileId, String instanceId,
                                                String instanceName, String fileType, String fileName, String fileValue, String fileDesc,
                                                String expirationTs) {
        Result<String> result = null;
        String s =
                """
                SELECT COUNT(*) OVER () AS total,
                ift.host_id, ift.instance_file_id, ift.instance_id, i.instance_name,
                ift.file_type, ift.file_name, ift.file_value, ift.file_desc,
                ift.expiration_ts, ift.update_user, ift.update_ts, ift.aggregate_version
                FROM instance_file_t ift
                INNER JOIN instance_t i ON i.instance_id = ift.instance_id
                WHERE 1=1
                """;

        StringBuilder sqlBuilder = new StringBuilder();
        sqlBuilder.append(s).append("\n");

        List<Object> parameters = new ArrayList<>();

        StringBuilder whereClause = new StringBuilder();
        SqlUtil.addCondition(whereClause, parameters, "ift.host_id", hostId != null ? UUID.fromString(hostId) : null);
        SqlUtil.addCondition(whereClause, parameters, "ift.instance_file_id", instanceFileId != null ? UUID.fromString(instanceFileId) : null);
        SqlUtil.addCondition(whereClause, parameters, "ift.instance_id", instanceId != null ? UUID.fromString(instanceId) : null);
        SqlUtil.addCondition(whereClause, parameters, "i.instance_name", instanceName);
        SqlUtil.addCondition(whereClause, parameters, "ift.file_type", fileType);
        SqlUtil.addCondition(whereClause, parameters, "ift.file_name", fileName);
        SqlUtil.addCondition(whereClause, parameters, "ift.file_value", fileValue);
        SqlUtil.addCondition(whereClause, parameters, "ift.file_desc", fileDesc);
        SqlUtil.addCondition(whereClause, parameters, "ift.expiration_ts", expirationTs);

        if (!whereClause.isEmpty()) {
            sqlBuilder.append("AND ").append(whereClause);
        }

        sqlBuilder.append(" ORDER BY ift.host_id, ift.instance_file_id, ift.instance_id\n" +
                "LIMIT ? OFFSET ?");

        parameters.add(limit);
        parameters.add(offset);

        String sql = sqlBuilder.toString();
        int total = 0;
        List<Map<String, Object>> instanceFiles = new ArrayList<>();

        try (Connection connection = ds.getConnection();
             PreparedStatement preparedStatement = connection.prepareStatement(sql)) {

            for (int i = 0; i < parameters.size(); i++) {
                preparedStatement.setObject(i + 1, parameters.get(i));
            }

            boolean isFirstRow = true;
            try (ResultSet resultSet = preparedStatement.executeQuery()) {
                while (resultSet.next()) {
                    Map<String, Object> map = new HashMap<>();
                    if (isFirstRow) {
                        total = resultSet.getInt("total");
                        isFirstRow = false;
                    }
                    map.put("hostId", resultSet.getObject("host_id", UUID.class));
                    map.put("instanceFileId", resultSet.getObject("instance_file_id", UUID.class));
                    map.put("instanceId", resultSet.getObject("instance_id", UUID.class));
                    map.put("instanceName", resultSet.getString("instance_name"));
                    map.put("fileType", resultSet.getString("file_type"));
                    map.put("fileName", resultSet.getString("file_name"));
                    map.put("fileValue", resultSet.getString("file_value"));
                    map.put("fileDesc", resultSet.getString("file_desc"));
                    map.put("expirationTs", resultSet.getObject("expiration_ts") != null ? resultSet.getObject("expiration_ts", OffsetDateTime.class) : null);
                    map.put("updateUser", resultSet.getString("update_user"));
                    map.put("updateTs", resultSet.getObject("update_ts") != null ? resultSet.getObject("update_ts", OffsetDateTime.class) : null);
                    map.put("aggregateVersion", resultSet.getInt("aggregate_version"));

                    instanceFiles.add(map);
                }
            }

            Map<String, Object> resultMap = new HashMap<>();
            resultMap.put("total", total);
            resultMap.put("instanceFiles", instanceFiles);
            result = Success.of(JsonMapper.toJson(resultMap));

        } catch (SQLException e) {
            logger.error("SQLException:", e);
            result = Failure.of(new Status("SQL_EXCEPTION", e.getMessage()));
        } catch (Exception e) {
            logger.error("Exception:", e);
            result = Failure.of(new Status("GENERIC_EXCEPTION", e.getMessage()));
        }
        return result;

    }

    @Override
    public void createConfigDeploymentInstance(Connection conn, Map<String, Object> event) throws SQLException, Exception {
        final String sql =
                """
                INSERT INTO deployment_instance_property_t (host_id, deployment_instance_id, property_id,
                property_value, update_user, update_ts, aggregate_version)
                VALUES (?, ?, ?, ?, ?,  ?, ?)
                """;
        Map<String, Object> map = (Map<String, Object>)event.get(PortalConstants.DATA);
        String hostId = (String)event.get(Constants.HOST);
        String deploymentInstanceId = (String)map.get("deploymentInstanceId");
        String propertyId = (String)map.get("propertyId");
        long newAggregateVersion = SqlUtil.getNewAggregateVersion(event);

        try (PreparedStatement statement = conn.prepareStatement(sql)) {
            statement.setObject(1, UUID.fromString(hostId));
            statement.setObject(2, UUID.fromString(deploymentInstanceId));
            statement.setObject(3, UUID.fromString(propertyId));
            String propertyValue = (String) map.get("propertyValue");
            if(propertyValue != null && !propertyValue.isEmpty()) {
                statement.setString(4, propertyValue);
            } else {
                statement.setNull(4, Types.VARCHAR);
            }
            statement.setString(5, (String)event.get(Constants.USER));
            statement.setObject(6, OffsetDateTime.parse((String)event.get(CloudEventV1.TIME)));
            statement.setLong(7, newAggregateVersion);

            int count = statement.executeUpdate();
            if (count == 0) {
                throw new SQLException(String.format("Failed during createConfigDeploymentInstance for hostId %s deploymentInstanceId %s propertyId %s with aggregateVersion %d", hostId, deploymentInstanceId, propertyId, newAggregateVersion));
            }
        } catch (SQLException e) {
            logger.error("SQLException during createConfigDeploymentInstance for hostId {} deploymentInstanceId {} propertyId {} aggregateVersion {}: {}", hostId, deploymentInstanceId, propertyId, newAggregateVersion, e.getMessage(), e);
            throw e;
        } catch (Exception e) {
            logger.error("Exception during createConfigDeploymentInstance for hostId {} deploymentInstanceId {} propertyId {} aggregateVersion {}: {}", hostId, deploymentInstanceId, propertyId, newAggregateVersion, e.getMessage(), e);
            throw e;
        }
    }

    private boolean queryConfigDeploymentInstanceExists(Connection conn, String hostId, String deploymentInstanceId, String propertyId) throws SQLException {
        final String sql =
                """
                SELECT COUNT(*) FROM deployment_instance_property_t WHERE host_id = ? AND deployment_instance_id = ? AND property_id = ?
                """;
        try (PreparedStatement pst = conn.prepareStatement(sql)) {
            pst.setObject(1, UUID.fromString(hostId));
            pst.setObject(2, UUID.fromString(deploymentInstanceId));
            pst.setObject(3, UUID.fromString(propertyId));
            try (ResultSet rs = pst.executeQuery()) {
                return rs.next() && rs.getInt(1) > 0;
            }
        }
    }

    @Override
    public void updateConfigDeploymentInstance(Connection conn, Map<String, Object> event) throws SQLException, Exception {
        final String sql =
                """
                UPDATE deployment_instance_property_t SET property_value = ?,
                update_user = ?, update_ts = ?, aggregate_version = ?
                WHERE host_id = ? AND deployment_instance_id = ? AND property_id = ?
                AND aggregate_version = ?
                """;
        Map<String, Object> map = (Map<String, Object>)event.get(PortalConstants.DATA);
        String hostId = (String)event.get(Constants.HOST);
        String deploymentInstanceId = (String)map.get("deploymentInstanceId");
        String propertyId = (String)map.get("propertyId");
        long oldAggregateVersion = SqlUtil.getOldAggregateVersion(event);
        long newAggregateVersion = SqlUtil.getNewAggregateVersion(event);

        try (PreparedStatement statement = conn.prepareStatement(sql)) {

            String propertyValue = (String)map.get("propertyValue");
            if (propertyValue != null && !propertyValue.isEmpty()) {
                statement.setString(1, propertyValue);
            } else {
                statement.setNull(1, Types.VARCHAR);
            }
            statement.setString(2, (String)event.get(Constants.USER));
            statement.setObject(3, OffsetDateTime.parse((String)event.get(CloudEventV1.TIME)));
            statement.setLong(4, newAggregateVersion);

            statement.setObject(5, UUID.fromString(hostId));
            statement.setObject(6, UUID.fromString(deploymentInstanceId));
            statement.setObject(7, UUID.fromString(propertyId));
            statement.setLong(8, oldAggregateVersion);

            int count = statement.executeUpdate();
            if (count == 0) {
                if (queryConfigDeploymentInstanceExists(conn, hostId, deploymentInstanceId, propertyId)) {
                    throw new ConcurrencyException("Optimistic concurrency conflict during updateConfigDeploymentInstance for hostId " + hostId + " deploymentInstanceId " + deploymentInstanceId + " propertyId " + propertyId + ". Expected version " + oldAggregateVersion + " but found a different version " + newAggregateVersion + ".");
                } else {
                    throw new SQLException("No record found during updateConfigDeploymentInstance for hostId " + hostId + " deploymentInstanceId " + deploymentInstanceId + " propertyId " + propertyId + ".");
                }
            }
        } catch (SQLException e) {
            logger.error("SQLException during updateConfigDeploymentInstance for hostId {} deploymentInstanceId {} propertyId {} (old: {}) -> (new: {}): {}", hostId, deploymentInstanceId, propertyId, oldAggregateVersion, newAggregateVersion, e.getMessage(), e);
            throw e;
        } catch (Exception e) {
            logger.error("Exception during updateConfigDeploymentInstance for hostId {} deploymentInstanceId {} propertyId {} (old: {}) -> (new: {}): {}", hostId, deploymentInstanceId, propertyId, oldAggregateVersion, newAggregateVersion, e.getMessage(), e);
            throw e;
        }
    }

    @Override
    public void deleteConfigDeploymentInstance(Connection conn, Map<String, Object> event) throws SQLException, Exception {
        final String sql = "DELETE FROM deployment_instance_property_t " +
                "WHERE host_id = ? AND deployment_instance_id = ? AND property_id = ? AND aggregate_version = ?";
        Map<String, Object> map = (Map<String, Object>)event.get(PortalConstants.DATA);
        String hostId = (String)event.get(Constants.HOST);
        String deploymentInstanceId = (String)map.get("deploymentInstanceId");
        String propertyId = (String)map.get("propertyId");
        long oldAggregateVersion = SqlUtil.getOldAggregateVersion(event);

        try (PreparedStatement statement = conn.prepareStatement(sql)) {
            statement.setObject(1, UUID.fromString(hostId));
            statement.setObject(2, UUID.fromString(deploymentInstanceId));
            statement.setObject(3, UUID.fromString(propertyId));
            statement.setLong(4, oldAggregateVersion);

            int count = statement.executeUpdate();
            if (count == 0) {
                if (queryConfigDeploymentInstanceExists(conn, hostId, deploymentInstanceId, propertyId)) {
                    throw new ConcurrencyException("Optimistic concurrency conflict during deleteConfigDeploymentInstance for hostId " + hostId + " deploymentInstanceId " + deploymentInstanceId + " propertyId " + propertyId + " aggregateVersion " + oldAggregateVersion + " but found a different version or already updated.");
                } else {
                    throw new SQLException("No record found during deleteConfigDeploymentInstance for hostId " + hostId + " deploymentInstanceId " + deploymentInstanceId + " propertyId " + propertyId + ". It might have been already deleted.");
                }
            }
        } catch (SQLException e) {
            logger.error("SQLException during deleteConfigDeploymentInstance for hostId {} deploymentInstanceId {} propertyId {} aggregateVersion {}: {}", hostId, deploymentInstanceId, propertyId, oldAggregateVersion, e.getMessage(), e);
            throw e;
        } catch (Exception e) {
            logger.error("Exception during deleteConfigDeploymentInstance for hostId {} deploymentInstanceId {} propertyId {} aggregateVersion {}: {}", hostId, deploymentInstanceId, propertyId, oldAggregateVersion, e.getMessage(), e);
            throw e;
        }
    }

    @Override
    public Result<String> getConfigDeploymentInstance(int offset, int limit, String hostId, String deploymentInstanceId, String instanceId,
                                                      String instanceName, String serviceId, String ipAddress, Integer portNumber, String configId,
                                                      String configName, String propertyId, String propertyName, String propertyValue) {
        Result<String> result = null;
        String s =
                """
                SELECT COUNT(*) OVER () AS total,
                dip.host_id, dip.deployment_instance_id, di.instance_id, i.instance_name,
                di.service_id, di.ip_address, di.port_number, cp.config_id, c.config_name,
                dip.property_id, cp.property_name, dip.property_value, dip.update_user, dip.update_ts, dip.aggregate_version
                FROM deployment_instance_property_t dip
                INNER JOIN deployment_instance_t di ON di.host_id = dip.host_id
                AND di.deployment_instance_id = dip.deployment_instance_id
                INNER JOIN instance_t i ON i.host_id = di.host_id
                AND i.instance_id = di.instance_id
                INNER JOIN config_property_t cp ON dip.property_id = cp.property_id
                INNER JOIN config_t c ON c.config_id = cp.config_id
                WHERE 1=1
                """;

        StringBuilder sqlBuilder = new StringBuilder();
        sqlBuilder.append(s).append("\n");

        List<Object> parameters = new ArrayList<>();

        StringBuilder whereClause = new StringBuilder();
        SqlUtil.addCondition(whereClause, parameters, "dip.host_id", hostId != null ? UUID.fromString(hostId) : null);
        SqlUtil.addCondition(whereClause, parameters, "dip.deployment_instance_id", deploymentInstanceId != null ? UUID.fromString(deploymentInstanceId) : null);
        SqlUtil.addCondition(whereClause, parameters, "di.instance_id", instanceId != null ? UUID.fromString(instanceId) : null);
        SqlUtil.addCondition(whereClause, parameters, "i.instance_name", instanceName);
        SqlUtil.addCondition(whereClause, parameters, "di.service_id", serviceId);
        SqlUtil.addCondition(whereClause, parameters, "di.ip_address", ipAddress);
        SqlUtil.addCondition(whereClause, parameters, "di.port_number", portNumber);
        SqlUtil.addCondition(whereClause, parameters, "cp.config_id", configId != null ? UUID.fromString(configId) : null);
        SqlUtil.addCondition(whereClause, parameters, "c.config_name", configName);
        SqlUtil.addCondition(whereClause, parameters, "dip.property_id", propertyId != null ? UUID.fromString(propertyId) : null);
        SqlUtil.addCondition(whereClause, parameters, "cp.property_name", propertyName);
        SqlUtil.addCondition(whereClause, parameters, "dip.property_value", propertyValue);

        if (!whereClause.isEmpty()) {
            sqlBuilder.append("AND ").append(whereClause);
        }

        sqlBuilder.append(" ORDER BY dip.host_id, di.service_id\n" +
                "LIMIT ? OFFSET ?");

        parameters.add(limit);
        parameters.add(offset);

        String sql = sqlBuilder.toString();
        int total = 0;
        List<Map<String, Object>> deploymentInstances = new ArrayList<>();

        try (Connection connection = ds.getConnection();
             PreparedStatement preparedStatement = connection.prepareStatement(sql)) {

            for (int i = 0; i < parameters.size(); i++) {
                preparedStatement.setObject(i + 1, parameters.get(i));
            }

            boolean isFirstRow = true;
            try (ResultSet resultSet = preparedStatement.executeQuery()) {
                while (resultSet.next()) {
                    Map<String, Object> map = new HashMap<>();
                    if (isFirstRow) {
                        total = resultSet.getInt("total");
                        isFirstRow = false;
                    }
                    map.put("hostId", resultSet.getObject("host_id", UUID.class));
                    map.put("deploymentInstanceId", resultSet.getObject("deployment_instance_id", UUID.class));
                    map.put("instanceId", resultSet.getObject("instance_id", UUID.class));
                    map.put("instanceName", resultSet.getString("instance_name"));
                    map.put("serviceId", resultSet.getString("service_id"));
                    map.put("ipAddress", resultSet.getString("ip_address"));
                    map.put("portNumber", resultSet.getInt("port_number"));
                    map.put("configId", resultSet.getObject("config_id", UUID.class));
                    map.put("configName", resultSet.getString("config_name"));
                    map.put("propertyId", resultSet.getObject("property_id", UUID.class));
                    map.put("propertyName", resultSet.getString("property_name"));
                    map.put("propertyValue", resultSet.getString("property_value"));
                    map.put("updateUser", resultSet.getString("update_user"));
                    map.put("updateTs", resultSet.getObject("update_ts") != null ? resultSet.getObject("update_ts", OffsetDateTime.class) : null);
                    map.put("aggregateVersion", resultSet.getInt("aggregate_version"));

                    deploymentInstances.add(map);
                }
            }

            Map<String, Object> resultMap = new HashMap<>();
            resultMap.put("total", total);
            resultMap.put("deploymentInstances", deploymentInstances);
            result = Success.of(JsonMapper.toJson(resultMap));

        } catch (SQLException e) {
            logger.error("SQLException:", e);
            result = Failure.of(new Status("SQL_EXCEPTION", e.getMessage()));
        } catch (Exception e) {
            logger.error("Exception:", e);
            result = Failure.of(new Status("GENERIC_EXCEPTION", e.getMessage()));
        }
        return result;

    }

    @Override
    public void createConfigProduct(Connection conn, Map<String, Object> event) throws SQLException, Exception {
        final String sql =
                """
                INSERT INTO product_property_t (product_id, property_id, property_value, update_user, update_ts, aggregate_version) VALUES (?, ?, ?, ?, ?, ?)
                """;

        Map<String, Object> map = (Map<String, Object>)event.get(PortalConstants.DATA);
        String productId = (String)map.get("productId");
        String propertyId = (String)map.get("propertyId");
        long newAggregateVersion = SqlUtil.getNewAggregateVersion(event);

        try (PreparedStatement statement = conn.prepareStatement(sql)) {
            statement.setString(1, productId);
            statement.setObject(2, UUID.fromString(propertyId));

            if (map.containsKey("propertyValue")) {
                statement.setString(3, (String) map.get("propertyValue"));
            } else {
                statement.setNull(3, Types.VARCHAR);
            }

            statement.setString(4, (String)event.get(Constants.USER));
            statement.setObject(5, OffsetDateTime.parse((String)event.get(CloudEventV1.TIME)));
            statement.setLong(6, newAggregateVersion);

            int count = statement.executeUpdate();
            if (count == 0) {
                throw new SQLException(String.format("Failed during createConfigProduct for productId %s  propertyId %s with aggregateVersion %d", productId, propertyId, newAggregateVersion));
            }
        } catch (SQLException e) {
            logger.error("SQLException during createConfigProduct for productId {} propertyId {} aggregateVersion {}: {}", productId, propertyId, newAggregateVersion, e.getMessage(), e);
            throw e;
        } catch (Exception e) {
            logger.error("Exception during createConfigProduct for productId {} propertyId {} aggregateVersion {}: {}", productId, propertyId, newAggregateVersion, e.getMessage(), e);
            throw e;
        }
    }

    private boolean queryConfigProductExists(Connection conn, String productId, String propertyId) throws SQLException {
        final String sql =
                """
                SELECT COUNT(*) FROM product_property_t WHERE product_id = ? AND property_id = ?
                """;
        try (PreparedStatement pst = conn.prepareStatement(sql)) {
            pst.setString(1, productId);
            pst.setObject(2, UUID.fromString(propertyId));
            try (ResultSet rs = pst.executeQuery()) {
                return rs.next() && rs.getInt(1) > 0;
            }
        }
    }

    @Override
    public void updateConfigProduct(Connection conn, Map<String, Object> event) throws SQLException, Exception {
        final String sql =
                """
                UPDATE product_property_t SET property_value = ?, update_user = ?, update_ts = ?, aggregate_version = ?
                WHERE product_id = ? AND property_id = ? AND aggregate_version = ?
                """;
        Map<String, Object> map = (Map<String, Object>)event.get(PortalConstants.DATA);
        String productId = (String)map.get("productId");
        String propertyId = (String)map.get("propertyId");
        long oldAggregateVersion = SqlUtil.getOldAggregateVersion(event);
        long newAggregateVersion = SqlUtil.getNewAggregateVersion(event);

        try (PreparedStatement statement = conn.prepareStatement(sql)) {

            // Handle 'property_value' (optional)
            if (map.containsKey("propertyValue")) {
                statement.setString(1, (String) map.get("propertyValue"));
            } else {
                statement.setNull(1, Types.VARCHAR);
            }

            statement.setString(2, (String)event.get(Constants.USER));
            statement.setObject(3, OffsetDateTime.parse((String)event.get(CloudEventV1.TIME)));
            statement.setLong(4, newAggregateVersion);

            // WHERE clause parameters
            statement.setString(5, productId);
            statement.setObject(6, UUID.fromString(propertyId));
            statement.setLong(7, oldAggregateVersion);

            int count = statement.executeUpdate();
            if (count == 0) {
                if (queryConfigProductExists(conn, productId, propertyId)) {
                    throw new ConcurrencyException("Optimistic concurrency conflict during updateConfigProduct for productId " + productId + " propertyId " + propertyId + ". Expected version " + oldAggregateVersion + " but found a different version " + newAggregateVersion + ".");
                } else {
                    throw new SQLException("No record found during updateConfigProduct for productId " + productId + " propertyId " + propertyId + ".");
                }
            }
        } catch (SQLException e) {
            logger.error("SQLException during updateConfigProduct for productId {} propertyId {} (old: {}) -> (new: {}): {}", productId, propertyId, oldAggregateVersion, newAggregateVersion, e.getMessage(), e);
            throw e;
        } catch (Exception e) {
            logger.error("Exception during updateConfigProduct for productId {} propertyId {} (old: {}) -> (new: {}): {}", productId, propertyId, oldAggregateVersion, newAggregateVersion, e.getMessage(), e);
            throw e;
        }
    }

    @Override
    public void deleteConfigProduct(Connection conn, Map<String, Object> event) throws SQLException, Exception {
        final String sql = "DELETE FROM product_property_t WHERE product_id = ? AND property_id = ? AND aggregate_version = ?";
        Map<String, Object> map = (Map<String, Object>)event.get(PortalConstants.DATA);
        String productId = (String)map.get("productId");
        String propertyId = (String)map.get("propertyId");
        long oldAggregateVersion = SqlUtil.getOldAggregateVersion(event);

        try (PreparedStatement statement = conn.prepareStatement(sql)) {
            statement.setString(1, productId);
            statement.setObject(2, UUID.fromString(propertyId));
            statement.setLong(3, oldAggregateVersion);

            int count = statement.executeUpdate();
            if (count == 0) {
                if (queryConfigProductExists(conn, productId, propertyId)) {
                    throw new ConcurrencyException("Optimistic concurrency conflict during deleteConfigProduct for productId " + productId + " propertyId " + propertyId + " aggregateVersion " + oldAggregateVersion + " but found a different version or already updated.");
                } else {
                    throw new SQLException("No record found during deleteConfigProduct for productId " + productId + " propertyId " + propertyId + ". It might have been already deleted.");
                }
            }
        } catch (SQLException e) {
            logger.error("SQLException during deleteConfigProduct for productId {} propertyId {} aggregateVersion {}: {}", productId, propertyId, oldAggregateVersion, e.getMessage(), e);
            throw e;
        } catch (Exception e) {
            logger.error("Exception during deleteConfigProduct for productId {} propertyId {} aggregateVersion {}: {}", productId, propertyId, oldAggregateVersion, e.getMessage(), e);
            throw e;
        }
    }

    @Override
    public Result<String> getConfigProduct(int offset, int limit, String productId,
                                           String configId, String configName, String propertyId,
                                           String propertyName, String propertyValue) {
        Result<String> result = null;
        String s =
                """
                        SELECT COUNT(*) OVER () AS total,
                        pp.product_id, p.config_id, pp.property_id, p.property_name, pp.property_value,
                        pp.update_user, pp.update_ts, c.config_name, pp.aggregate_version
                        FROM product_property_t pp
                        INNER JOIN config_property_t p ON p.property_id = pp.property_id
                        INNER JOIN config_t c ON p.config_id = c.config_id
                        WHERE 1=1
                """;
        StringBuilder sqlBuilder = new StringBuilder();
        sqlBuilder.append(s).append("\n");

        List<Object> parameters = new ArrayList<>();

        StringBuilder whereClause = new StringBuilder();
        SqlUtil.addCondition(whereClause, parameters, "pp.product_id", productId);
        SqlUtil.addCondition(whereClause, parameters, "p.config_id", configId != null ? UUID.fromString(configId) : null);
        SqlUtil.addCondition(whereClause, parameters, "c.config_name", configName);
        SqlUtil.addCondition(whereClause, parameters, "pp.property_id", propertyId != null ? UUID.fromString(propertyId) : null);
        SqlUtil.addCondition(whereClause, parameters, "p.property_name", propertyName);
        SqlUtil.addCondition(whereClause, parameters, "pp.property_value", propertyValue);

        if (!whereClause.isEmpty()) {
            sqlBuilder.append("AND ").append(whereClause);
        }

        sqlBuilder.append(" ORDER BY pp.product_id, p.config_id, p.property_name\n" +
                "LIMIT ? OFFSET ?");

        parameters.add(limit);
        parameters.add(offset);

        String sql = sqlBuilder.toString();
        int total = 0;
        List<Map<String, Object>> productProperties = new ArrayList<>();

        try (Connection connection = ds.getConnection();
             PreparedStatement preparedStatement = connection.prepareStatement(sql)) {

            for (int i = 0; i < parameters.size(); i++) {
                preparedStatement.setObject(i + 1, parameters.get(i));
            }

            boolean isFirstRow = true;
            try (ResultSet resultSet = preparedStatement.executeQuery()) {
                while (resultSet.next()) {
                    Map<String, Object> map = new HashMap<>();
                    if (isFirstRow) {
                        total = resultSet.getInt("total");
                        isFirstRow = false;
                    }
                    map.put("productId", resultSet.getString("product_id"));
                    map.put("configId", resultSet.getObject("config_id", UUID.class));
                    map.put("configName", resultSet.getString("config_name"));
                    map.put("propertyId", resultSet.getObject("property_id", UUID.class));
                    map.put("propertyName", resultSet.getString("property_name"));
                    map.put("propertyValue", resultSet.getString("property_value"));
                    map.put("updateUser", resultSet.getString("update_user"));
                    map.put("updateTs", resultSet.getObject("update_ts") != null ? resultSet.getObject("update_ts", OffsetDateTime.class) : null);
                    map.put("aggregateVersion", resultSet.getInt("aggregate_version"));

                    productProperties.add(map);
                }
            }

            Map<String, Object> resultMap = new HashMap<>();
            resultMap.put("total", total);
            resultMap.put("productProperties", productProperties);
            result = Success.of(JsonMapper.toJson(resultMap));

        } catch (SQLException e) {
            logger.error("SQLException:", e);
            result = Failure.of(new Status("SQL_EXCEPTION", e.getMessage()));
        } catch (Exception e) {
            logger.error("Exception:", e);
            result = Failure.of(new Status("GENERIC_EXCEPTION", e.getMessage()));
        }
        return result;
    }

    @Override
    public void createConfigProductVersion(Connection conn, Map<String, Object> event) throws SQLException, Exception {
        final String sql =
                """
                INSERT INTO product_version_property_t (host_id, product_version_id,
                property_id, property_value, update_user, update_ts, aggregate_version)
                VALUES (?, ?, ?, ?, ?,  ?, ?)
                """;
        Map<String, Object> map = (Map<String, Object>)event.get(PortalConstants.DATA);
        String hostId = (String)event.get(Constants.HOST);
        String productVersionId = (String)map.get("productVersionId");
        String propertyId = (String)map.get("propertyId");
        long newAggregateVersion = SqlUtil.getNewAggregateVersion(event);

        try (PreparedStatement statement = conn.prepareStatement(sql)) {
            statement.setObject(1, UUID.fromString(hostId));
            statement.setObject(2, UUID.fromString(productVersionId));
            statement.setObject(3, UUID.fromString(propertyId));

            // Handle 'property_value' (optional)
            if (map.containsKey("propertyValue")) {
                statement.setString(4, (String) map.get("propertyValue"));
            } else {
                statement.setNull(4, Types.VARCHAR);
            }

            statement.setString(5, (String)event.get(Constants.USER));
            statement.setObject(6, OffsetDateTime.parse((String)event.get(CloudEventV1.TIME)));
            statement.setLong(7, newAggregateVersion);

            int count = statement.executeUpdate();
            if (count == 0) {
                throw new SQLException(String.format("Failed during createConfigProductVersion for hostId %s productVersionId %s propertyId %s with aggregateVersion %d", hostId, productVersionId, propertyId, newAggregateVersion));
            }
        } catch (SQLException e) {
            logger.error("SQLException during createConfigProductVersion for hostId {} productVersionId {} propertyId {} aggregateVersion {}: {}", hostId, productVersionId, propertyId, newAggregateVersion, e.getMessage(), e);
            throw e;
        } catch (Exception e) {
            logger.error("Exception during createConfigProductVersion for hostId {} productVersionId {} propertyId {} aggregateVersion {}: {}", hostId, productVersionId, propertyId, newAggregateVersion, e.getMessage(), e);
            throw e;
        }
    }

    private boolean queryConfigProductVersionExists(Connection conn, String hostId, String productVersionId, String propertyId) throws SQLException {
        final String sql =
                """
                SELECT COUNT(*) FROM product_version_property_t WHERE host_id = ? AND product_version_id = ? AND property_id = ?
                """;
        try (PreparedStatement pst = conn.prepareStatement(sql)) {
            pst.setObject(1, UUID.fromString(hostId));
            pst.setObject(2, UUID.fromString(productVersionId));
            pst.setObject(3, UUID.fromString(propertyId));

            try (ResultSet rs = pst.executeQuery()) {
                return rs.next() && rs.getInt(1) > 0;
            }
        }
    }

    @Override
    public void updateConfigProductVersion(Connection conn, Map<String, Object> event) throws SQLException, Exception {
        final String sql =
                """
                UPDATE product_version_property_t SET property_value = ?, update_user = ?, update_ts = ?, aggregate_version = ?
                WHERE host_id = ? AND product_version_id = ? AND property_id = ? AND aggregate_version = ?
                """;
        Map<String, Object> map = (Map<String, Object>)event.get(PortalConstants.DATA);
        String hostId = (String)event.get(Constants.HOST);
        String productVersionId = (String)map.get("productVersionId");
        String propertyId = (String)map.get("propertyId");
        long oldAggregateVersion = SqlUtil.getOldAggregateVersion(event);
        long newAggregateVersion = SqlUtil.getNewAggregateVersion(event);

        try (PreparedStatement statement = conn.prepareStatement(sql)) {

            // Handle 'property_value' (optional)
            if (map.containsKey("propertyValue")) {
                statement.setString(1, (String) map.get("propertyValue"));
            } else {
                statement.setNull(1, Types.VARCHAR);
            }

            statement.setString(2, (String)event.get(Constants.USER));
            statement.setObject(3, OffsetDateTime.parse((String)event.get(CloudEventV1.TIME)));
            statement.setLong(4, newAggregateVersion);

            // WHERE clause parameters
            statement.setObject(5, UUID.fromString(hostId));
            statement.setObject(6, UUID.fromString(productVersionId));
            statement.setObject(7, UUID.fromString(propertyId));
            statement.setLong(8, oldAggregateVersion);

            int count = statement.executeUpdate();
            if (count == 0) {
                if (queryConfigProductVersionExists(conn, hostId, productVersionId, propertyId)) {
                    throw new ConcurrencyException("Optimistic concurrency conflict during updateConfigProductVersion for hostId " + hostId + " productVersionId " + productVersionId + " propertyId " + propertyId + ". Expected version " + oldAggregateVersion + " but found a different version " + newAggregateVersion + ".");
                } else {
                    throw new SQLException("No record found during updateConfigProductVersion for hostId " + hostId + " productVersionId " + productVersionId + " propertyId " + propertyId + ".");
                }
            }
        } catch (SQLException e) {
            logger.error("SQLException during updateConfigProductVersion for hostId {} productVersionId {} propertyId {} (old: {}) -> (new: {}): {}", hostId, productVersionId, propertyId, oldAggregateVersion, newAggregateVersion, e.getMessage(), e);
            throw e;
        } catch (Exception e) {
            logger.error("Exception during updateConfigProductVersion for hostId {} productVersionId {} propertyId {} (old: {}) -> (new: {}): {}", hostId, productVersionId, propertyId, oldAggregateVersion, newAggregateVersion, e.getMessage(), e);
            throw e;
        }
    }

    @Override
    public void deleteConfigProductVersion(Connection conn, Map<String, Object> event) throws SQLException, Exception {
        final String sql = "DELETE FROM product_version_property_t WHERE host_id = ? AND product_version_id = ? " +
                "AND property_id = ? AND aggregate_version = ?";
        Map<String, Object> map = (Map<String, Object>)event.get(PortalConstants.DATA);
        String hostId = (String)event.get(Constants.HOST);
        String productVersionId = (String)map.get("productVersionId");
        String propertyId = (String)map.get("propertyId");
        long oldAggregateVersion = SqlUtil.getOldAggregateVersion(event);

        try (PreparedStatement statement = conn.prepareStatement(sql)) {
            statement.setObject(1, UUID.fromString(hostId));
            statement.setObject(2, UUID.fromString(productVersionId));
            statement.setObject(3, UUID.fromString(propertyId));
            statement.setLong(4, oldAggregateVersion);

            int count = statement.executeUpdate();
            if (count == 0) {
                if (queryConfigProductVersionExists(conn, hostId, productVersionId, propertyId)) {
                    throw new ConcurrencyException("Optimistic concurrency conflict during deleteConfigProductVersion for hostId " + hostId + " productVersionId " + productVersionId + " propertyId " + propertyId + " aggregateVersion " + oldAggregateVersion + " but found a different version or already updated.");
                } else {
                    throw new SQLException("No record found during deleteConfigProductVersion for hostId " + hostId + " productVersionId " + productVersionId + " propertyId " + propertyId + ". It might have been already deleted.");
                }
            }
        } catch (SQLException e) {
            logger.error("SQLException during deleteConfigProductVersion for hostId {} productVersionId {} propertyId {} aggregateVersion {}: {}", hostId, productVersionId, propertyId, oldAggregateVersion, e.getMessage(), e);
            throw e;
        } catch (Exception e) {
            logger.error("Exception during deleteConfigProductVersion for hostId {} productVersionId {} propertyId {} aggregateVersion {}: {}", hostId, productVersionId, propertyId, oldAggregateVersion, e.getMessage(), e);
            throw e;
        }
    }

    @Override
    public Result<String> getConfigProductVersion(int offset, int limit, String hostId, String productId, String productVersion,
                                                  String configId, String configName, String propertyId,
                                                  String propertyName, String propertyValue) {
        Result<String> result = null;
        String s =
                """
                SELECT COUNT(*) OVER () AS total,
                pvp.host_id, pvp.product_version_id, pv.product_id, pv.product_version, p.config_id, pvp.property_id,
                p.property_name, pvp.property_value, pvp.update_user, pvp.update_ts, c.config_name, pvp.aggregate_version
                FROM product_version_property_t pvp
                INNER JOIN product_version_t pv ON pv.product_version_id = pvp.product_version_id
                INNER JOIN config_property_t p ON p.property_id = pvp.property_id
                INNER JOIN config_t c ON p.config_id = c.config_id
                WHERE 1=1
                """;
        StringBuilder sqlBuilder = new StringBuilder();
        sqlBuilder.append(s).append("\n");

        List<Object> parameters = new ArrayList<>();

        StringBuilder whereClause = new StringBuilder();
        SqlUtil.addCondition(whereClause, parameters, "pvp.host_id", hostId != null ? UUID.fromString(hostId) : null);
        SqlUtil.addCondition(whereClause, parameters, "pv.product_version_id", productVersion != null ? UUID.fromString(productVersion) : null); // Assuming productVersion can map to product_version_id
        SqlUtil.addCondition(whereClause, parameters, "pv.product_id", productId);
        SqlUtil.addCondition(whereClause, parameters, "pv.product_version", productVersion);
        SqlUtil.addCondition(whereClause, parameters, "p.config_id", configId != null ? UUID.fromString(configId) : null);
        SqlUtil.addCondition(whereClause, parameters, "c.config_name", configName);
        SqlUtil.addCondition(whereClause, parameters, "pvp.property_id", propertyId != null ? UUID.fromString(propertyId) : null);
        SqlUtil.addCondition(whereClause, parameters, "p.property_name", propertyName);
        SqlUtil.addCondition(whereClause, parameters, "pvp.property_value", propertyValue);


        if (!whereClause.isEmpty()) {
            sqlBuilder.append("AND ").append(whereClause);
        }

        sqlBuilder.append(" ORDER BY pvp.host_id, pv.product_id, pv.product_version, p.config_id, p.display_order\n" +
                "LIMIT ? OFFSET ?");

        parameters.add(limit);
        parameters.add(offset);

        String sql = sqlBuilder.toString();
        int total = 0;
        List<Map<String, Object>> productVersionProperties = new ArrayList<>();

        try (Connection connection = ds.getConnection();
             PreparedStatement preparedStatement = connection.prepareStatement(sql)) {

            for (int i = 0; i < parameters.size(); i++) {
                preparedStatement.setObject(i + 1, parameters.get(i));
            }

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
                    map.put("propertyValue", resultSet.getString("property_value"));
                    map.put("updateUser", resultSet.getString("update_user"));
                    map.put("updateTs", resultSet.getObject("update_ts") != null ? resultSet.getObject("update_ts", OffsetDateTime.class) : null);
                    map.put("aggregateVersion", resultSet.getInt("aggregate_version"));

                    productVersionProperties.add(map);
                }
            }

            Map<String, Object> resultMap = new HashMap<>();
            resultMap.put("total", total);
            resultMap.put("productVersionProperties", productVersionProperties);
            result = Success.of(JsonMapper.toJson(resultMap));

        } catch (SQLException e) {
            logger.error("SQLException:", e);
            result = Failure.of(new Status("SQL_EXCEPTION", e.getMessage()));
        } catch (Exception e) {
            logger.error("Exception:", e);
            result = Failure.of(new Status("GENERIC_EXCEPTION", e.getMessage()));
        }
        return result;
    }

    @Override
    public Result<String> getApplicableConfigPropertiesForInstance(
            int offset, int limit,
            String hostId, String instanceId,
            Set<String> resourceTypes, Set<String> configTypes, Set<String> propertyTypes
    ) {

        Result<String> result;
        String sql =
                """
                    WITH applicable_instance_config_properties AS (
                        SELECT DISTINCT pvc.config_id, pvcp.property_id, i.host_id, i.instance_id, i.product_version_id
                        FROM instance_t i
                        JOIN product_version_config_t pvc ON i.host_id = pvc.host_id AND i.product_version_id = pvc.product_version_id
                        JOIN product_version_config_property_t pvcp ON i.host_id = pvcp.host_id AND i.product_version_id = pvcp.product_version_id
                        WHERE i.host_id = ? AND i.instance_id = ?
                    ),
                    property_values AS (
                        SELECT
                            cp.property_id,
                            CASE
                                WHEN ep.property_value IS NOT NULL THEN ep.property_value
                                WHEN pvp.property_value IS NOT NULL THEN pvp.property_value
                                WHEN pp.property_value IS NOT NULL THEN pp.property_value
                                ELSE cp.property_value
                            END AS effective_property_value,
                            CASE
                                WHEN ep.property_value IS NOT NULL THEN 'environment_property'
                                WHEN pvp.property_value IS NOT NULL THEN 'product_version_property'
                                WHEN pp.property_value IS NOT NULL THEN 'product_property'
                                ELSE 'config_property'
                            END AS property_source_type,
                            CASE
                                WHEN ep.property_value IS NOT NULL THEN COALESCE( ep.environment, '' )
                                WHEN pvp.property_value IS NOT NULL THEN CONCAT( COALESCE( pv.product_id, '' ), '-', COALESCE( pv.product_version, '' ) )
                                WHEN pp.property_value IS NOT NULL THEN COALESCE ( pp.product_id, '' )
                                ELSE 'global'
                            END AS property_source
                        FROM config_property_t cp
                        JOIN applicable_instance_config_properties aicp ON aicp.config_id = cp.config_id AND aicp.property_id = cp.property_id
                        JOIN product_version_t pv ON aicp.host_id = pv.host_id AND aicp.product_version_id = pv.product_version_id
                        LEFT JOIN product_property_t pp ON cp.property_id = pp.property_id AND pv.product_id = pp.product_id
                        LEFT JOIN product_version_property_t pvp ON cp.property_id = pvp.property_id AND aicp.host_id = pvp.host_id AND aicp.product_version_id = pvp.product_version_id
                        LEFT JOIN environment_property_t ep ON cp.property_id = ep.property_id AND aicp.host_id = ep.host_id
                    )
                    SELECT
                        COUNT(*) OVER () AS total,
                        ac.host_id, ac.instance_id,
                        c.config_id, c.config_name, c.config_phase, c.config_type, c.class_path, c.config_desc,
                        cp.property_id, cp.property_name, cp.property_type, cp.display_order, cp.required, cp.property_desc, cp.value_type, cp.resource_type,
                        pv.effective_property_value AS property_value, pv.property_source, pv.property_source_type
                    FROM config_t c
                    JOIN config_property_t cp ON c.config_id = cp.config_id
                    JOIN applicable_instance_config_properties aicp ON c.config_id = aicp.config_id AND cp.config_id = aicp.config_id AND cp.property_id = aicp.property_id
                    LEFT JOIN property_values pv ON cp.property_id = pv.property_id
                    WHERE 1 = 1
                        AND ( array_length(?, 1) IS NULL OR cp.resource_type = ANY(?) )
                        AND ( array_length(?, 1) IS NULL OR c.config_type = ANY(?) )
                        AND ( array_length(?, 1) IS NULL OR cp.property_type = ANY(?) )
                    ORDER BY c.config_name, cp.property_name, cp.display_order
                    LIMIT ? OFFSET ?
                """;

        int total = 0;
        List<Map<String, Object>> instanceApplicableProperties = new ArrayList<>();

        try (Connection connection = ds.getConnection();
             PreparedStatement preparedStatement = connection.prepareStatement(sql)) {

            preparedStatement.setObject(1, hostId != null ? UUID.fromString(hostId) : null);
            preparedStatement.setObject(2, instanceId != null ? UUID.fromString(instanceId) : null);

            if (resourceTypes == null || resourceTypes.isEmpty()) {
                preparedStatement.setNull(3, Types.ARRAY);
                preparedStatement.setNull(4, Types.ARRAY);
            } else {
                Array sqlArray = connection.createArrayOf("VARCHAR", resourceTypes.toArray());
                preparedStatement.setArray(3, sqlArray);
                preparedStatement.setArray(4, sqlArray);
            }

            if (configTypes == null || configTypes.isEmpty()) {
                preparedStatement.setNull(5, Types.ARRAY);
                preparedStatement.setNull(6, Types.ARRAY);
            } else {
                Array sqlArray = connection.createArrayOf("VARCHAR", configTypes.toArray());
                preparedStatement.setArray(5, sqlArray);
                preparedStatement.setArray(6, sqlArray);
            }

            if (propertyTypes == null || propertyTypes.isEmpty()) {
                preparedStatement.setNull(7, Types.ARRAY);
                preparedStatement.setNull(8, Types.ARRAY);
            } else {
                Array sqlArray = connection.createArrayOf("VARCHAR", propertyTypes.toArray());
                preparedStatement.setArray(7, sqlArray);
                preparedStatement.setArray(8, sqlArray);
            }

            preparedStatement.setObject(9, limit);
            preparedStatement.setObject(10, offset);

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
                    map.put("configId", resultSet.getObject("config_id", UUID.class));
                    map.put("configName", resultSet.getString("config_name"));
                    map.put("configPhase", resultSet.getString("config_phase"));
                    map.put("configType", resultSet.getString("config_type"));
                    map.put("classPath", resultSet.getString("class_path"));
                    map.put("configDesc", resultSet.getString("config_desc"));
                    map.put("propertyId", resultSet.getObject("property_id", UUID.class));
                    map.put("propertyName", resultSet.getString("property_name"));
                    map.put("displayOrder", resultSet.getInt("display_order"));
                    map.put("required", resultSet.getBoolean("required"));
                    map.put("propertyDesc", resultSet.getString("property_desc"));
                    map.put("propertyValue", resultSet.getString("property_value"));
                    map.put("propertySource", resultSet.getString("property_source"));
                    map.put("propertySourceType", resultSet.getString("property_source_type"));
                    map.put("valueType", resultSet.getString("value_type"));
                    map.put("resourceType", resultSet.getString("resource_type"));

                    instanceApplicableProperties.add(map);
                }
            }

            Map<String, Object> resultMap = new HashMap<>();
            resultMap.put("total", total);
            resultMap.put("instanceApplicableProperties", instanceApplicableProperties);
            result = Success.of(JsonMapper.toJson(resultMap));

        } catch (SQLException e) {
            logger.error("SQLException:", e);
            result = Failure.of(new Status("SQL_EXCEPTION", e.getMessage()));
        } catch (Exception e) {
            logger.error("Exception:", e);
            result = Failure.of(new Status("GENERIC_EXCEPTION", e.getMessage()));
        }
        return result;
    }

    @Override
    public Result<String> getApplicableConfigPropertiesForInstanceApi(
        int offset, int limit, String hostId, String instanceApiId
    ) {

        Result<String> result;
        String sql =
            """
                WITH applicable_instance_config_properties AS (
                    SELECT DISTINCT pvc.config_id, pvcp.property_id, i.host_id, i.instance_id, i.product_version_id
                    FROM instance_api_t ia
                    JOIN instance_t i ON ia.instance_id = i.instance_id AND ia.host_id = i.host_id
                    JOIN product_version_config_t pvc ON i.host_id = pvc.host_id AND i.product_version_id = pvc.product_version_id
                    JOIN product_version_config_property_t pvcp ON i.host_id = pvcp.host_id AND i.product_version_id = pvcp.product_version_id
                    WHERE ia.host_id = ? AND ia.instance_api_id = ?
                ),
                property_values AS (
                    SELECT
                        cp.property_id,
                        CASE
                            WHEN ep.property_value IS NOT NULL THEN ep.property_value
                            WHEN pvp.property_value IS NOT NULL THEN pvp.property_value
                            WHEN pp.property_value IS NOT NULL THEN pp.property_value
                            ELSE cp.property_value
                        END AS effective_property_value,
                        CASE
                            WHEN ep.property_value IS NOT NULL THEN 'environment_property'
                            WHEN pvp.property_value IS NOT NULL THEN 'product_version_property'
                            WHEN pp.property_value IS NOT NULL THEN 'product_property'
                            ELSE 'config_property'
                        END AS property_source_type,
                        CASE
                            WHEN ep.property_value IS NOT NULL THEN COALESCE( ep.environment, '' )
                            WHEN pvp.property_value IS NOT NULL THEN CONCAT( COALESCE( pv.product_id, '' ), '-', COALESCE( pv.product_version, '' ) )
                            WHEN pp.property_value IS NOT NULL THEN COALESCE ( pp.product_id, '' )
                            ELSE 'global'
                        END AS property_source
                    FROM config_property_t cp
                    JOIN applicable_instance_config_properties aicp ON aicp.config_id = cp.config_id AND aicp.property_id = cp.property_id
                    JOIN product_version_t pv ON aicp.host_id = pv.host_id AND aicp.product_version_id = pv.product_version_id
                    LEFT JOIN product_property_t pp ON cp.property_id = pp.property_id AND pv.product_id = pp.product_id
                    LEFT JOIN product_version_property_t pvp ON cp.property_id = pvp.property_id AND aicp.host_id = pvp.host_id AND aicp.product_version_id = pvp.product_version_id
                    LEFT JOIN environment_property_t ep ON cp.property_id = ep.property_id AND aicp.host_id = ep.host_id
                )
                SELECT
                    COUNT(*) OVER () AS total,
                    ac.host_id, ac.instance_id,
                    c.config_id, c.config_name, c.config_phase, c.config_type, c.class_path, c.config_desc,
                    cp.property_id, cp.property_name, cp.property_type, cp.display_order, cp.required, cp.property_desc, cp.value_type, cp.resource_type,
                    pv.effective_property_value AS property_value, pv.property_source, pv.property_source_type
                FROM config_t c
                JOIN config_property_t cp ON c.config_id = cp.config_id
                JOIN applicable_instance_config_properties aicp ON c.config_id = aicp.config_id AND cp.config_id = aicp.config_id AND cp.property_id = aicp.property_id
                LEFT JOIN property_values pv ON cp.property_id = pv.property_id
                WHERE 1 = 1 AND cp.resource_type IN ('api', 'api|app_api', 'all')
                ORDER BY c.config_name, cp.property_name, cp.display_order
                LIMIT ? OFFSET ?
            """;

        int total = 0;
        List<Map<String, Object>> instanceApplicableProperties = new ArrayList<>();

        try (Connection connection = ds.getConnection();
             PreparedStatement preparedStatement = connection.prepareStatement(sql)) {

            preparedStatement.setObject(1, hostId != null ? UUID.fromString(hostId) : null);
            preparedStatement.setObject(2, instanceApiId != null ? UUID.fromString(instanceApiId) : null);

            preparedStatement.setObject(3, limit);
            preparedStatement.setObject(4, offset);

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
                    map.put("configId", resultSet.getObject("config_id", UUID.class));
                    map.put("configName", resultSet.getString("config_name"));
                    map.put("configPhase", resultSet.getString("config_phase"));
                    map.put("configType", resultSet.getString("config_type"));
                    map.put("classPath", resultSet.getString("class_path"));
                    map.put("configDesc", resultSet.getString("config_desc"));
                    map.put("propertyId", resultSet.getObject("property_id", UUID.class));
                    map.put("propertyName", resultSet.getString("property_name"));
                    map.put("displayOrder", resultSet.getInt("display_order"));
                    map.put("required", resultSet.getBoolean("required"));
                    map.put("propertyDesc", resultSet.getString("property_desc"));
                    map.put("propertyValue", resultSet.getString("property_value"));
                    map.put("propertySource", resultSet.getString("property_source"));
                    map.put("propertySourceType", resultSet.getString("property_source_type"));
                    map.put("valueType", resultSet.getString("value_type"));
                    map.put("resourceType", resultSet.getString("resource_type"));

                    instanceApplicableProperties.add(map);
                }
            }

            Map<String, Object> resultMap = new HashMap<>();
            resultMap.put("total", total);
            resultMap.put("instanceApiApplicableProperties", instanceApplicableProperties);
            result = Success.of(JsonMapper.toJson(resultMap));

        } catch (SQLException e) {
            logger.error("SQLException:", e);
            result = Failure.of(new Status("SQL_EXCEPTION", e.getMessage()));
        } catch (Exception e) {
            logger.error("Exception:", e);
            result = Failure.of(new Status("GENERIC_EXCEPTION", e.getMessage()));
        }
        return result;
    }

    @Override
    public Result<String> getApplicableConfigPropertiesForInstanceApp(
        int offset, int limit, String hostId, String instanceAppId
    ) {

        Result<String> result;
        String sql =
            """
                WITH applicable_instance_config_properties AS (
                    SELECT DISTINCT pvc.config_id, pvcp.property_id, i.host_id, i.instance_id, i.product_version_id
                    FROM instance_app_t ia
                    JOIN instance_t i ON ia.instance_id = i.instance_id AND ia.host_id = i.host_id
                    JOIN product_version_config_t pvc ON i.host_id = pvc.host_id AND i.product_version_id = pvc.product_version_id
                    JOIN product_version_config_property_t pvcp ON i.host_id = pvcp.host_id AND i.product_version_id = pvcp.product_version_id
                    WHERE ia.host_id = ? AND ia.instance_app_id = ?
                ),
                property_values AS (
                    SELECT
                        cp.property_id,
                        CASE
                            WHEN ep.property_value IS NOT NULL THEN ep.property_value
                            WHEN pvp.property_value IS NOT NULL THEN pvp.property_value
                            WHEN pp.property_value IS NOT NULL THEN pp.property_value
                            ELSE cp.property_value
                        END AS effective_property_value,
                        CASE
                            WHEN ep.property_value IS NOT NULL THEN 'environment_property'
                            WHEN pvp.property_value IS NOT NULL THEN 'product_version_property'
                            WHEN pp.property_value IS NOT NULL THEN 'product_property'
                            ELSE 'config_property'
                        END AS property_source_type,
                        CASE
                            WHEN ep.property_value IS NOT NULL THEN COALESCE( ep.environment, '' )
                            WHEN pvp.property_value IS NOT NULL THEN CONCAT( COALESCE( pv.product_id, '' ), '-', COALESCE( pv.product_version, '' ) )
                            WHEN pp.property_value IS NOT NULL THEN COALESCE ( pp.product_id, '' )
                            ELSE 'global'
                        END AS property_source
                    FROM config_property_t cp
                    JOIN applicable_instance_config_properties aicp ON aicp.config_id = cp.config_id AND aicp.property_id = cp.property_id
                    JOIN product_version_t pv ON aicp.host_id = pv.host_id AND aicp.product_version_id = pv.product_version_id
                    LEFT JOIN product_property_t pp ON cp.property_id = pp.property_id AND pv.product_id = pp.product_id
                    LEFT JOIN product_version_property_t pvp ON cp.property_id = pvp.property_id AND aicp.host_id = pvp.host_id AND aicp.product_version_id = pvp.product_version_id
                    LEFT JOIN environment_property_t ep ON cp.property_id = ep.property_id AND aicp.host_id = ep.host_id
                )
                SELECT
                    COUNT(*) OVER () AS total,
                    ac.host_id, ac.instance_id,
                    c.config_id, c.config_name, c.config_phase, c.config_type, c.class_path, c.config_desc,
                    cp.property_id, cp.property_name, cp.property_type, cp.display_order, cp.required, cp.property_desc, cp.value_type, cp.resource_type,
                    pv.effective_property_value AS property_value, pv.property_source, pv.property_source_type
                FROM config_t c
                JOIN config_property_t cp ON c.config_id = cp.config_id
                JOIN applicable_instance_config_properties aicp ON c.config_id = aicp.config_id AND cp.config_id = aicp.config_id AND cp.property_id = aicp.property_id
                LEFT JOIN property_values pv ON cp.property_id = pv.property_id
                WHERE 1 = 1 AND cp.resource_type IN ('app', 'app|app_api', 'all')
                ORDER BY c.config_name, cp.property_name, cp.display_order
                LIMIT ? OFFSET ?
            """;

        int total = 0;
        List<Map<String, Object>> instanceApplicableProperties = new ArrayList<>();

        try (Connection connection = ds.getConnection();
             PreparedStatement preparedStatement = connection.prepareStatement(sql)) {

            preparedStatement.setObject(1, hostId != null ? UUID.fromString(hostId) : null);
            preparedStatement.setObject(2, instanceAppId != null ? UUID.fromString(instanceAppId) : null);

            preparedStatement.setObject(3, limit);
            preparedStatement.setObject(4, offset);

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
                    map.put("configId", resultSet.getObject("config_id", UUID.class));
                    map.put("configName", resultSet.getString("config_name"));
                    map.put("configPhase", resultSet.getString("config_phase"));
                    map.put("configType", resultSet.getString("config_type"));
                    map.put("classPath", resultSet.getString("class_path"));
                    map.put("configDesc", resultSet.getString("config_desc"));
                    map.put("propertyId", resultSet.getObject("property_id", UUID.class));
                    map.put("propertyName", resultSet.getString("property_name"));
                    map.put("displayOrder", resultSet.getInt("display_order"));
                    map.put("required", resultSet.getBoolean("required"));
                    map.put("propertyDesc", resultSet.getString("property_desc"));
                    map.put("propertyValue", resultSet.getString("property_value"));
                    map.put("propertySource", resultSet.getString("property_source"));
                    map.put("propertySourceType", resultSet.getString("property_source_type"));
                    map.put("valueType", resultSet.getString("value_type"));
                    map.put("resourceType", resultSet.getString("resource_type"));

                    instanceApplicableProperties.add(map);
                }
            }

            Map<String, Object> resultMap = new HashMap<>();
            resultMap.put("total", total);
            resultMap.put("instanceAppApplicableProperties", instanceApplicableProperties);
            result = Success.of(JsonMapper.toJson(resultMap));

        } catch (SQLException e) {
            logger.error("SQLException:", e);
            result = Failure.of(new Status("SQL_EXCEPTION", e.getMessage()));
        } catch (Exception e) {
            logger.error("Exception:", e);
            result = Failure.of(new Status("GENERIC_EXCEPTION", e.getMessage()));
        }
        return result;
    }

    @Override
    public Result<String> getApplicableConfigPropertiesForInstanceAppApi(
        int offset, int limit, String hostId, String instanceAppId, String instanceApiId
    ) {

        Result<String> result;
        String sql =
            """
                WITH applicable_instance_config_properties AS (
                    SELECT DISTINCT pvc.config_id, pvcp.property_id, i.host_id, i.instance_id, i.product_version_id
                    FROM instance_app_api_t iappapi
                    JOIN instance_app_t iapp ON iappapi.instance_app_id = iapp.instance_app_id AND iappapi.host_id = iapp.host_id
                    JOIN instance_api_t iapi ON iappapi.instance_api_id = iapi.instance_api_id AND iappapi.host_id = iapi.host_id
                    JOIN instance_t i ON iapp.instance_id = i.instance_id AND iapp.host_id = i.host_id AND iapi.instance_id = i.instance_id AND iapi.host_id = i.host_id
                    JOIN product_version_config_t pvc ON i.host_id = pvc.host_id AND i.product_version_id = pvc.product_version_id
                    JOIN product_version_config_property_t pvcp ON i.host_id = pvcp.host_id AND i.product_version_id = pvcp.product_version_id
                    WHERE iappapi.host_id = ? AND iappapi.instance_api_id = ? AND iappapi.instance_app_id = ?
                ),
                property_values AS (
                    SELECT
                        cp.property_id,
                        CASE
                            WHEN ep.property_value IS NOT NULL THEN ep.property_value
                            WHEN pvp.property_value IS NOT NULL THEN pvp.property_value
                            WHEN pp.property_value IS NOT NULL THEN pp.property_value
                            ELSE cp.property_value
                        END AS effective_property_value,
                        CASE
                            WHEN ep.property_value IS NOT NULL THEN 'environment_property'
                            WHEN pvp.property_value IS NOT NULL THEN 'product_version_property'
                            WHEN pp.property_value IS NOT NULL THEN 'product_property'
                            ELSE 'config_property'
                        END AS property_source_type,
                        CASE
                            WHEN ep.property_value IS NOT NULL THEN COALESCE( ep.environment, '' )
                            WHEN pvp.property_value IS NOT NULL THEN CONCAT( COALESCE( pv.product_id, '' ), '-', COALESCE( pv.product_version, '' ) )
                            WHEN pp.property_value IS NOT NULL THEN COALESCE ( pp.product_id, '' )
                            ELSE 'global'
                        END AS property_source
                    FROM config_property_t cp
                    JOIN applicable_instance_config_properties aicp ON aicp.config_id = cp.config_id AND aicp.property_id = cp.property_id
                    JOIN product_version_t pv ON aicp.host_id = pv.host_id AND aicp.product_version_id = pv.product_version_id
                    LEFT JOIN product_property_t pp ON cp.property_id = pp.property_id AND pv.product_id = pp.product_id
                    LEFT JOIN product_version_property_t pvp ON cp.property_id = pvp.property_id AND aicp.host_id = pvp.host_id AND aicp.product_version_id = pvp.product_version_id
                    LEFT JOIN environment_property_t ep ON cp.property_id = ep.property_id AND aicp.host_id = ep.host_id
                )
                SELECT
                    COUNT(*) OVER () AS total,
                    ac.host_id, ac.instance_id,
                    c.config_id, c.config_name, c.config_phase, c.config_type, c.class_path, c.config_desc,
                    cp.property_id, cp.property_name, cp.property_type, cp.display_order, cp.required, cp.property_desc, cp.value_type, cp.resource_type,
                    pv.effective_property_value AS property_value, pv.property_source, pv.property_source_type
                FROM config_t c
                JOIN config_property_t cp ON c.config_id = cp.config_id
                JOIN applicable_instance_config_properties aicp ON c.config_id = aicp.config_id AND cp.config_id = aicp.config_id AND cp.property_id = aicp.property_id
                LEFT JOIN property_values pv ON cp.property_id = pv.property_id
                WHERE 1 = 1 AND cp.resource_type IN ('app_api', 'api|app_api', 'app|app_api', 'all')
                ORDER BY c.config_name, cp.property_name, cp.display_order
                LIMIT ? OFFSET ?
            """;

        int total = 0;
        List<Map<String, Object>> instanceApplicableProperties = new ArrayList<>();

        try (Connection connection = ds.getConnection();
             PreparedStatement preparedStatement = connection.prepareStatement(sql)) {

            preparedStatement.setObject(1, hostId != null ? UUID.fromString(hostId) : null);
            preparedStatement.setObject(2, instanceApiId != null ? UUID.fromString(instanceApiId) : null);
            preparedStatement.setObject(3, instanceAppId != null ? UUID.fromString(instanceAppId) : null);

            preparedStatement.setObject(4, limit);
            preparedStatement.setObject(5, offset);

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
                    map.put("configId", resultSet.getObject("config_id", UUID.class));
                    map.put("configName", resultSet.getString("config_name"));
                    map.put("configPhase", resultSet.getString("config_phase"));
                    map.put("configType", resultSet.getString("config_type"));
                    map.put("classPath", resultSet.getString("class_path"));
                    map.put("configDesc", resultSet.getString("config_desc"));
                    map.put("propertyId", resultSet.getObject("property_id", UUID.class));
                    map.put("propertyName", resultSet.getString("property_name"));
                    map.put("displayOrder", resultSet.getInt("display_order"));
                    map.put("required", resultSet.getBoolean("required"));
                    map.put("propertyDesc", resultSet.getString("property_desc"));
                    map.put("propertyValue", resultSet.getString("property_value"));
                    map.put("propertySource", resultSet.getString("property_source"));
                    map.put("propertySourceType", resultSet.getString("property_source_type"));
                    map.put("valueType", resultSet.getString("value_type"));
                    map.put("resourceType", resultSet.getString("resource_type"));

                    instanceApplicableProperties.add(map);
                }
            }

            Map<String, Object> resultMap = new HashMap<>();
            resultMap.put("total", total);
            resultMap.put("instanceAppApiApplicableProperties", instanceApplicableProperties);
            result = Success.of(JsonMapper.toJson(resultMap));

        } catch (SQLException e) {
            logger.error("SQLException:", e);
            result = Failure.of(new Status("SQL_EXCEPTION", e.getMessage()));
        } catch (Exception e) {
            logger.error("Exception:", e);
            result = Failure.of(new Status("GENERIC_EXCEPTION", e.getMessage()));
        }
        return result;
    }

    @Override
    public Result<String> getAllAggregatedInstanceRuntimeConfigs(String hostId, String instanceId) {
        Result<String> result;
        final String instanceRuntimeConfigsSql = """
            WITH params as MATERIALIZED (
                SELECT
                    CAST(v.host_id as UUID) as host_id,
                    CAST(v.instance_id as UUID) as instance_id
                FROM (
                    values
                        (?, ?)
                ) as v(host_id, instance_id)
            )
            SELECT
                ip.host_id,
                ip.instance_id,
                c.config_id,
                c.config_name,
                ip.property_id,
                CASE
                    WHEN cp.property_type IN ('File', 'Cert') THEN cp.property_name
                    ELSE CONCAT( c.config_name, '.', cp.property_name )
                END as property_name,
                ip.property_value,
                cp.value_type,
                cp.property_type,
                CAST (NULL as UUID) as instance_api_id,
                CAST (NULL as UUID) as instance_app_id,
                CAST ('instance_property' as VARCHAR) as property_source_type
            FROM
                instance_property_t ip
                JOIN params p ON ip.host_id = p.host_id AND ip.instance_id = p.instance_id
                JOIN config_property_t cp ON cp.property_id = ip.property_id
                JOIN config_t c ON c.config_id = cp.config_id AND c.config_phase = 'R'
            UNION ALL
            SELECT
                ia.host_id,
                ia.instance_id,
                c.config_id,
                c.config_name,
                iap.property_id,
                CASE
                    WHEN cp.property_type IN ('File', 'Cert') THEN cp.property_name
                    ELSE CONCAT( c.config_name, '.', cp.property_name )
                END as property_name,
                iap.property_value,
                cp.value_type,
                cp.property_type,
                ia.instance_api_id as instance_api_id,
                CAST (NULL as UUID) as instance_app_id,
                CAST ('instance_api_property' as VARCHAR) as property_source_type
            FROM
                instance_api_property_t iap
                JOIN instance_api_t ia ON iap.host_id = ia.host_id
                    AND iap.instance_api_id = ia.instance_api_id
                    AND ia.active = true
                JOIN params p ON ia.host_id = p.host_id AND ia.instance_id = p.instance_id
                JOIN config_property_t cp ON cp.property_id = iap.property_id
                JOIN config_t c ON c.config_id = cp.config_id AND c.config_phase = 'R'
            UNION ALL
            SELECT
                ia.host_id,
                ia.instance_id,
                c.config_id,
                c.config_name,
                iap.property_id,
                CASE
                    WHEN cp.property_type IN ('File', 'Cert') THEN cp.property_name
                    ELSE CONCAT( c.config_name, '.', cp.property_name )
                END as property_name,
                iap.property_value,
                cp.value_type,
                cp.property_type,
                CAST (NULL as UUID) as instance_api_id,
                ia.instance_app_id as instance_app_id,
                CAST ('instance_app_property' as VARCHAR) as property_source_type
            FROM
                instance_app_property_t iap
                JOIN instance_app_t ia ON iap.host_id = ia.host_id
                    AND iap.instance_app_id = ia.instance_app_id
                    AND ia.active = true
                JOIN params p ON ia.host_id = p.host_id AND ia.instance_id = p.instance_id
                JOIN config_property_t cp ON cp.property_id = iap.property_id
                JOIN config_t c ON c.config_id = cp.config_id AND c.config_phase = 'R'
            UNION ALL
            SELECT
                iappapiprop.host_id,
                iapp.instance_id,
                c.config_id,
                c.config_name,
                iappapiprop.property_id,
                CASE
                    WHEN cp.property_type IN ('File', 'Cert') THEN cp.property_name
                    ELSE CONCAT( c.config_name, '.', cp.property_name )
                END as property_name,
                iappapiprop.property_value,
                cp.value_type,
                cp.property_type,
                iappapiprop.instance_api_id as instance_api_id,
                iappapiprop.instance_app_id as instance_app_id,
                CAST ('instance_app_api_property' as VARCHAR) as property_source_type
            FROM
                instance_app_api_property_t iappapiprop
                JOIN instance_app_api_t iappapi ON iappapiprop.host_id = iappapi.host_id
                    AND iappapiprop.instance_api_id = iappapi.instance_api_id
                    AND iappapiprop.instance_app_id = iappapi.instance_app_id
                    AND iappapi.active = true
                JOIN instance_app_t iapp ON iapp.host_id = iappapi.host_id
                    AND iapp.instance_app_id = iappapi.instance_app_id
                    AND iapp.active = true
                JOIN instance_api_t iapi ON iapi.host_id = iappapi.host_id
                    AND iapi.instance_api_id = iappapi.instance_api_id
                    AND iapi.active = true
                JOIN params p ON iappapi.host_id = p.host_id
                    AND iapp.host_id = p.host_id
                    AND iapi.host_id = p.host_id
                    AND iapp.instance_id = p.instance_id
                    AND iapi.instance_id = p.instance_id
                JOIN config_property_t cp ON cp.property_id = iappapiprop.property_id
                JOIN config_t c ON c.config_id = cp.config_id AND c.config_phase = 'R'
            """;

        final String instanceCustomFilesSql = """
            WITH params as MATERIALIZED (
                SELECT
                    CAST(v.host_id as UUID) as host_id,
                    CAST(v.instance_id as UUID) as instance_id
                FROM (
                    values
                        (?, ?)
                ) as v(host_id, instance_id)
            )
            SELECT f.host_id, f.instance_id, f.instance_file_id, f.file_name, f.file_value, f.file_type
            FROM instance_file_t f
            JOIN params p on f.host_id = p.host_id AND f.instance_id = p.instance_id
            """;

        List<Map<String, Object>> runtimeConfigs = new ArrayList<>();
        List<Map<String, Object>> customFiles = new ArrayList<>();

        try (Connection connection = ds.getConnection();
             PreparedStatement instanceRuntimeConfigsPs = connection.prepareStatement(instanceRuntimeConfigsSql);
             PreparedStatement instanceCustomFilesPs = connection.prepareStatement(instanceCustomFilesSql);
             ) {

            instanceRuntimeConfigsPs.setObject(1, hostId != null ? UUID.fromString(hostId) : null);
            instanceRuntimeConfigsPs.setObject(2, instanceId != null ? UUID.fromString(instanceId) : null);

            try (ResultSet resultSet = instanceRuntimeConfigsPs.executeQuery()) {
                while (resultSet.next()) {
                    Map<String, Object> map = new HashMap<>();
                    map.put("hostId", resultSet.getObject("host_id", UUID.class));
                    map.put("instanceId", resultSet.getObject("instance_id", UUID.class));
                    map.put("instanceApiId", resultSet.getObject("instance_api_id", UUID.class));
                    map.put("instanceAppId", resultSet.getObject("instance_app_id", UUID.class));
                    map.put("configId", resultSet.getObject("config_id", UUID.class));
                    map.put("configName", resultSet.getString("config_name"));
                    map.put("propertyId", resultSet.getObject("property_id", UUID.class));
                    map.put("propertyName", resultSet.getString("property_name"));
                    map.put("propertyValue", resultSet.getString("property_value"));
                    map.put("propertySourceType", resultSet.getString("property_source_type"));
                    map.put("valueType", resultSet.getString("value_type"));
                    map.put("propertyType", resultSet.getString("property_type"));
                    runtimeConfigs.add(map);
                }
            }

            try (ResultSet resultSet = instanceCustomFilesPs.executeQuery()) {
                while (resultSet.next()) {
                    Map<String, Object> map = new HashMap<>();
                    map.put("hostId", resultSet.getObject("host_id", UUID.class));
                    map.put("instanceId", resultSet.getObject("instance_id", UUID.class));
                    map.put("instanceFileId", resultSet.getObject("instance_file_id", UUID.class));
                    map.put("fileName", resultSet.getString("file_name"));
                    map.put("fileValue", resultSet.getString("file_value"));
                    map.put("fileType", resultSet.getString("file_type"));
                    customFiles.add(map);
                }
            }

            Map<String, Object> resultMap = new HashMap<>();
            resultMap.put("runtimeConfigs", runtimeConfigs);
            resultMap.put("customFiles", customFiles);
            result = Success.of(JsonMapper.toJson(resultMap));
        } catch (SQLException e) {
            logger.error("SQLException:", e);
            result = Failure.of(new Status("SQL_EXCEPTION", e.getMessage()));
        } catch (Exception e) {
            logger.error("Exception:", e);
            result = Failure.of(new Status("GENERIC_EXCEPTION", e.getMessage()));
        }
        return result;
    }
    @Override
    public Result<String> getPromotableInstanceConfigs(String hostId, String instanceId,Set<String> propertyNames,Set<String> apiUids){
        Result<String> result;
        String instanceConfigsSql = """
                WITH params AS MATERIALIZED (
                    SELECT
                        CAST(v.instance_id as UUID) as instance_id,
                        CAST(v.hostId as UUID) as hostId,
                        v.property_names::VARCHAR[] as property_names
                    FROM (
                        values
                            (?, ?, ?)
                    ) as v(instance_id, hostId, property_names)
                ),
                instance_properties AS (
                    SELECT
                        CASE
                            WHEN config.config_phase = 'D' OR cp.property_type IN ('File', 'Cert') THEN cp.property_name
                            ELSE CONCAT(config.config_name, '.', cp.property_name)
                        END as property_name,
                        CASE
                            WHEN cp.property_type IN ('File', 'Cert') THEN ip.property_value
                            ELSE ip.property_value
                        END as property_value,
                        cp.value_type as property_value_type,
                        cp.property_type as property_type,
                        'instance_property' as property_source_type
                    FROM
                        instance_property_t as ip
                        JOIN config_property_t as cp ON cp.property_id = ip.property_id
                        JOIN config_t as config ON config.config_id = cp.config_id
                        JOIN instance_t as instance ON instance.instance_id = ip.instance_id
                        JOIN params ON params.instance_id = instance.instance_id
                            AND params.instance_id = ip.instance_id
                            AND params.hostId = instance.host_id
                )
                SELECT property_name, property_value, property_value_type, property_type, property_source_type
                FROM instance_properties
                    JOIN params ON
                        array_length(params.property_names, 1) IS NULL
                            OR instance_properties.property_name = ANY(params.property_names)
                ORDER BY
                    property_name
                """;


        String subresourceConfigsSql = """
                WITH params AS MATERIALIZED (
                    SELECT
                        CAST(v.instance_id as UUID) as instance_id,
                        v.api_uids::VARCHAR[] as api_uids
                    FROM (
                        values
                            (?, ?)
                    ) as v(instance_id, api_uids)
                ),
                instance_api_path_prefix AS (
                    SELECT
                        STRING_AGG(
                            instance_api_path_prefix.path_prefix,
                            ', '
                            ORDER BY instance_api_path_prefix.path_prefix
                        ) as api_path_prefixes,
                        instance_api.instance_api_id
                    FROM
                        instance_api_t as instance_api
                        JOIN instance_t as instance
                            ON instance.instance_id = instance_api.instance_id
                        JOIN params
                            ON params.instance_id = instance_api.instance_id
                                AND params.instance_id = instance.instance_id
                                AND (
                                    array_length(params.api_uids, 1) IS NULL
                                    OR CONCAT(instance_api.api_id, '-', instance_api.api_version) = ANY(params.api_uids)
                                )
                        LEFT JOIN instance_api_path_prefix_t as instance_api_path_prefix
                            ON instance_api.instance_api_id = instance_api_path_prefix.instance_api_id
                    GROUP BY
                        instance_api.instance_api_id
                ),
                configuration_properties AS (
                    SELECT
                        config_property.property_id as property_id,
                        CASE
                            WHEN config.config_phase = 'D' OR config_property.property_type IN ('File', 'Cert') THEN config_property.property_name
                            ELSE CONCAT( config.config_name, '.', config_property.property_name )
                        END as property_name,
                        config_property.value_type as property_value_type,
                        config_property.property_type as property_type
                    FROM
                        config_property_t as config_property
                        JOIN config_t as config ON config.config_id = config_property.config_id
                    WHERE config_property.property_type = 'Config'
                )
                SELECT
                    configuration_properties.property_id as property_id,
                    configuration_properties.property_name as property_name,
                    configuration_properties.property_value_type as property_value_type,
                    configuration_properties.property_type as property_type,
                    'instance_api_property' as property_source_type,
                    instance_api_property.property_value as property_value,
                    instance_api_property.instance_api_id as instance_api_id,
                    CONCAT(instance_api.api_id, '-', instance_api.api_version) as api_uid,
                    instance_api.api_id as api_id,
                    instance_api.api_version as api_version_value,
                    instance_api_path_prefix.api_path_prefixes as api_path_prefixes,
                    CAST( NULL as UUID) as instance_app_id,
                    CAST( NULL as VARCHAR) as app_id
                FROM
                    instance_api_property_t AS instance_api_property
                    JOIN configuration_properties ON configuration_properties.property_id = instance_api_property.property_id
                    JOIN instance_api_t AS instance_api ON instance_api.instance_api_id = instance_api_property.instance_api_id
                    JOIN params
                        ON params.instance_id = instance_api.instance_id
                        AND (
                            array_length(params.api_uids, 1) IS NULL
                            OR CONCAT(instance_api.api_id, '-', instance_api.api_version) = ANY(params.api_uids)
                        )
                    LEFT JOIN instance_api_path_prefix
                        ON instance_api_path_prefix.instance_api_id = instance_api.instance_api_id
                UNION ALL
                SELECT
                    configuration_properties.property_id as property_id,
                    configuration_properties.property_name as property_name,
                    configuration_properties.property_value_type as property_value_type,
                    configuration_properties.property_type as property_type,
                    'instance_app_property' as property_source_type,
                    instance_app_property.property_value as property_value,
                    CAST( NULL as UUID) as instance_api_id,
                    CAST( NULL as VARCHAR) as api_uid,
                    CAST( NULL as VARCHAR) as api_id,
                    CAST( NULL as VARCHAR) as api_version_value,
                    CAST( NULL as VARCHAR) as api_path_prefixes,
                    instance_app_property.instance_app_id as instance_app_id,
                    instance_app.app_id as app_id
                FROM
                    instance_app_property_t AS instance_app_property
                    JOIN configuration_properties ON configuration_properties.property_id = instance_app_property.property_id
                    JOIN instance_app_t AS instance_app ON instance_app.instance_app_id = instance_app_property.instance_app_id
                    JOIN params
                        ON params.instance_id = instance_app.instance_id
                UNION ALL
                SELECT
                    configuration_properties.property_id as property_id,
                    configuration_properties.property_name as property_name,
                    configuration_properties.property_value_type as property_value_type,
                    configuration_properties.property_type as property_type,
                    'instance_app_api_property' as property_source_type,
                    instance_app_api_property.property_value as property_value,
                    instance_app_api_property.instance_api_id as instance_api_id,
                    CONCAT(instance_api.api_id, '-', instance_api.api_version) as api_uid,
                    instance_api.api_id as api_id,
                    instance_api.api_version as api_version_value,
                    instance_api_path_prefix.api_path_prefixes as api_path_prefixes,
                    instance_app_api_property.instance_app_id as instance_app_id,
                    instance_app.app_id as app_id
                FROM
                    instance_app_api_property_t AS instance_app_api_property
                    JOIN configuration_properties ON configuration_properties.property_id = instance_app_api_property.property_id
                    JOIN instance_app_api_t AS instance_app_api
                        ON instance_app_api.instance_api_id = instance_app_api_property.instance_api_id
                        AND instance_app_api.instance_app_id = instance_app_api_property.instance_app_id
                    JOIN instance_app_t AS instance_app
                        ON instance_app.instance_app_id = instance_app_api_property.instance_app_id
                        AND instance_app.instance_app_id = instance_app_api.instance_app_id
                    JOIN instance_api_t as instance_api
                        ON instance_api.instance_api_id = instance_app_api_property.instance_api_id
                        AND instance_api.instance_api_id = instance_app_api.instance_api_id
                    JOIN params
                        ON params.instance_id = instance_app.instance_id
                        AND params.instance_id = instance_api.instance_id
                        AND (
                            array_length(params.api_uids, 1) IS NULL
                            OR CONCAT(instance_api.api_id, '-', instance_api.api_version) = ANY(params.api_uids)
                        )
                    LEFT JOIN instance_api_path_prefix
                        ON instance_api_path_prefix.instance_api_id = instance_api.instance_api_id
                """;

        List<Map<String, Object>> instanceConfigs = new ArrayList<>();
        List<Map<String, Object>> subresourceConfigs = new ArrayList<>();

        try (Connection connection = ds.getConnection();
             PreparedStatement instanceConfigsPs = connection.prepareStatement(instanceConfigsSql);
             PreparedStatement subresourceConfigsPs = connection.prepareStatement(subresourceConfigsSql);
        ) {

            instanceConfigsPs.setObject(1, instanceId != null ? UUID.fromString(instanceId) : null);
            instanceConfigsPs.setObject(2, hostId != null ? UUID.fromString(hostId) : null);
            instanceConfigsPs.setObject(3, propertyNames != null ? propertyNames : null);


            try (ResultSet resultSet = instanceConfigsPs.executeQuery()) {
                while (resultSet.next()) {
                    Map<String, Object> map = new HashMap<>();
                    map.put("hostId", resultSet.getObject("host_id", UUID.class));
                    map.put("instanceId", resultSet.getObject("instance_id", UUID.class));
                    map.put("propertyId", resultSet.getObject("property_id", UUID.class));
                    map.put("propertyName", resultSet.getString("property_name"));
                    map.put("propertyValue", resultSet.getString("property_value"));
                    map.put("propertySourceType", resultSet.getString("property_source_type"));
                    map.put("valueType", resultSet.getString("value_type"));
                    map.put("propertyType", resultSet.getString("property_type"));
                    instanceConfigs.add(map);
                }
            }

            subresourceConfigsPs.setObject(1, instanceId != null ? UUID.fromString(instanceId) : null);
            subresourceConfigsPs.setObject(2, apiUids != null ? apiUids : null);

            try (ResultSet resultSet = subresourceConfigsPs.executeQuery()) {
                while (resultSet.next()) {
                    Map<String, Object> map = new HashMap<>();
                    map.put("hostId", resultSet.getObject("host_id", UUID.class));
                    map.put("instanceId", resultSet.getObject("instance_id", UUID.class));
                    map.put("propertyId", resultSet.getObject("property_id", UUID.class));
                    map.put("propertyName", resultSet.getString("property_name"));
                    map.put("propertyValue", resultSet.getString("property_value"));
                    map.put("propertySourceType", resultSet.getString("property_source_type"));
                    map.put("valueType", resultSet.getString("value_type"));
                    map.put("propertyType", resultSet.getString("property_type"));
                    map.put("instanceApiId", resultSet.getString("instance_api_id"));
                    map.put("apiVersionId", resultSet.getObject("api_version_id", UUID.class));
                    map.put("apiId", resultSet.getString("api_id"));
                    map.put("apiVersion", resultSet.getString("api_version"));
                    map.put("apiPathPrefixes", resultSet.getString("api_path_prefixes"));
                    map.put("instanceAppId", resultSet.getObject("instance_app_id", UUID.class));
                    map.put("appId", resultSet.getObject("app_id", UUID.class));
                    subresourceConfigs.add(map);
                }
            }

            Map<String, Object> resultMap = new HashMap<>();
            resultMap.put("instanceConfigs", instanceConfigs);
            resultMap.put("subresourceConfigs", subresourceConfigs);
            result = Success.of(JsonMapper.toJson(resultMap));
        } catch (SQLException e) {
            logger.error("SQLException:", e);
            result = Failure.of(new Status("SQL_EXCEPTION", e.getMessage()));
        } catch (Exception e) {
            logger.error("Exception:", e);
            result = Failure.of(new Status("GENERIC_EXCEPTION", e.getMessage()));
        }
        return result;
    }
}
