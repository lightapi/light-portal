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
import net.lightapi.portal.db.PortalDbProvider;
import net.lightapi.portal.db.util.NotificationService;
import net.lightapi.portal.db.util.SqlUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;
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
    public Result<String> createConfig(Map<String, Object> event) {
        final String sql = "INSERT INTO config_t(config_id, config_name, config_phase, config_type, light4j_version, " +
                "class_path, config_desc, update_user, update_ts) " +
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)";
        Result<String> result;
        Map<String, Object> map = (Map<String, Object>)event.get(PortalConstants.DATA);
        try (Connection conn = ds.getConnection()) {
            conn.setAutoCommit(false);
            try (PreparedStatement statement = conn.prepareStatement(sql)) {
                statement.setObject(1, UUID.fromString((String)map.get("configId")));
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


                int count = statement.executeUpdate();
                if (count == 0) {
                    throw new SQLException("failed to insert the config with id " + map.get("configId"));
                }
                conn.commit();
                result = Success.of((String)map.get("configId"));
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
    public Result<String> updateConfig(Map<String, Object> event) {
        final String sql = "UPDATE config_t SET config_name = ?, config_phase = ?, config_type = ?, " +
                "light4j_version = ?, class_path = ?, config_desc = ?, update_user = ?, update_ts = ? " +
                "WHERE config_id = ?";
        Result<String> result;
        Map<String, Object> map = (Map<String, Object>)event.get(PortalConstants.DATA);
        try (Connection conn = ds.getConnection()) {
            conn.setAutoCommit(false);
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
                statement.setObject(9, UUID.fromString((String)map.get("configId")));

                int count = statement.executeUpdate();
                if (count == 0) {
                    throw new SQLException("failed to update config with id " + map.get("configId"));
                }
                conn.commit();
                result = Success.of((String)map.get("configId"));
                notificationService.insertNotification(event, true, null);
            }  catch (SQLException e) {
                logger.error("SQLException:", e);
                conn.rollback();
                notificationService.insertNotification(event, false, e.getMessage());
                result = Failure.of(new Status(SQL_EXCEPTION, e.getMessage()));
            }  catch (Exception e) {
                logger.error("Exception:", e);
                conn.rollback();
                notificationService.insertNotification(event, false, e.getMessage());
                result = Failure.of(new Status(GENERIC_EXCEPTION, e.getMessage()));
            }
        }  catch (SQLException e) {
            logger.error("SQLException:", e);
            result = Failure.of(new Status(SQL_EXCEPTION, e.getMessage()));
        }
        return result;
    }


    @Override
    public Result<String> deleteConfig(Map<String, Object> event) {
        final String sql = "DELETE FROM config_t WHERE config_id = ?";
        Result<String> result;
        Map<String, Object> map = (Map<String, Object>)event.get(PortalConstants.DATA);
        try (Connection conn = ds.getConnection()) {
            conn.setAutoCommit(false);
            try (PreparedStatement statement = conn.prepareStatement(sql)) {
                statement.setObject(1, UUID.fromString((String)map.get("configId")));

                int count = statement.executeUpdate();
                if (count == 0) {
                    throw new SQLException("failed to delete config with id " + map.get("configId"));
                }
                conn.commit();
                result = Success.of((String)map.get("configId"));
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
        }  catch (SQLException e) {
            logger.error("SQLException:", e);
            result = Failure.of(new Status(SQL_EXCEPTION, e.getMessage()));
        }
        return result;
    }


    @Override
    public Result<String> getConfig(int offset, int limit, String configId, String configName, String configPhase,
                                    String configType, String light4jVersion, String classPath, String configDesc) {
        Result<String> result = null;
        String s =
                """
                SELECT COUNT(*) OVER () AS total,
                config_id, config_name, config_phase, config_type, light4j_version,
                class_path, config_desc, update_user, update_ts
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
                    // handling date properly
                    map.put("updateTs", resultSet.getObject("update_ts") != null ? resultSet.getObject("update_ts", OffsetDateTime.class) : null);

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
        final String queryConfigById = "SELECT config_id, config_name, config_phase, config_type, light4j_version, " +
                "class_path, config_desc, update_user, update_ts FROM config_t WHERE config_id = ?";
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
                    // handling date properly
                    config.put("updateTs", resultSet.getObject("update_ts") != null ? resultSet.getObject("update_ts", OffsetDateTime.class) : null);
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
    public Result<String> createConfigProperty(Map<String, Object> event) {
        final String sql = "INSERT INTO config_property_t (config_id, property_id, property_name, property_type, " +
                "property_value, resource_type, value_type, display_order, required, property_desc, " +
                "light4j_version, update_user, update_ts) " +
                "VALUES (?, ?, ?, ?, ?,  ?, ?, ?, ?, ?,  ?, ?, ?)";
        Result<String> result;
        Map<String, Object> map = (Map<String, Object>)event.get(PortalConstants.DATA);
        try (Connection conn = ds.getConnection()) {
            conn.setAutoCommit(false); // Start transaction
            try (PreparedStatement statement = conn.prepareStatement(sql)) {
                statement.setObject(1, UUID.fromString((String)map.get("configId")));
                statement.setObject(2, UUID.fromString((String)map.get("propertyId")));
                statement.setString(3, (String)map.get("propertyName"));
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


                int count = statement.executeUpdate();
                if (count == 0) {
                    throw new SQLException("Failed to insert the config property with id " + map.get("configId") + " and name " + map.get("propertyName"));
                }
                conn.commit(); // Commit transaction
                result = Success.of((String)map.get("configId"));
                // Assuming notificationService.insertNotification is a method you have for handling notifications
                notificationService.insertNotification(event, true, null);

            } catch (SQLException e) {
                logger.error("SQLException:", e);
                conn.rollback(); // Rollback transaction on error
                notificationService.insertNotification(event, false, e.getMessage());
                result = Failure.of(new Status("SQL_EXCEPTION", e.getMessage())); // Use Status
            } catch (Exception e) {
                logger.error("Exception:", e);
                conn.rollback(); // Rollback transaction on error
                notificationService.insertNotification(event, false, e.getMessage());

                result = Failure.of(new Status("GENERIC_EXCEPTION", e.getMessage()));  // Use Status
            }
        } catch (SQLException e) {
            logger.error("SQLException:", e);
            result = Failure.of(new Status("SQL_EXCEPTION", e.getMessage())); // Use Status
        }
        return result;
    }

    @Override
    public Result<String> updateConfigProperty(Map<String, Object> event) {
        final String sql = "UPDATE config_property_t SET property_name = ?, property_type = ?, property_value = ?, " +
                "resource_type = ?, value_type = ?, display_order = ?, required = ?, property_desc = ?, " +
                "light4j_version = ?, update_user = ?, update_ts = ? " +
                "WHERE config_id = ? AND property_id = ?";

        Result<String> result;
        Map<String, Object> map = (Map<String, Object>)event.get(PortalConstants.DATA);
        try (Connection conn = ds.getConnection()) {
            conn.setAutoCommit(false);
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

                // WHERE clause: Crucial for updating the correct row!
                statement.setObject(12, UUID.fromString((String)map.get("configId")));
                statement.setString(13, (String)map.get("propertyId"));


                int count = statement.executeUpdate();
                if (count == 0) {
                    // No rows were updated.  This could mean the config_id and property_name
                    // combination doesn't exist, or it could be a concurrency issue.
                    throw new SQLException("Failed to update config property.  No rows affected for config_id: " + map.get("configId") + " and property_id: " + map.get("propertyId"));
                }

                conn.commit();
                result = Success.of((String)map.get("configId"));
                notificationService.insertNotification(event, true, null);

            } catch (SQLException e) {
                logger.error("SQLException:", e);
                conn.rollback();
                notificationService.insertNotification(event, false, e.getMessage());
                result = Failure.of(new Status("SQL_EXCEPTION", e.getMessage()));
            } catch (Exception e) {
                logger.error("Exception:", e);
                conn.rollback();
                notificationService.insertNotification(event, false, e.getMessage());
                result = Failure.of(new Status("GENERIC_EXCEPTION", e.getMessage()));
            }
        } catch (SQLException e) {
            logger.error("SQLException:", e);
            result = Failure.of(new Status("SQL_EXCEPTION", e.getMessage()));
        }
        return result;
    }

    @Override
    public Result<String> deleteConfigProperty(Map<String, Object> event) {
        final String sql = "DELETE FROM config_property_t WHERE config_id = ? AND property_id = ?";
        Result<String> result;
        Map<String, Object> map = (Map<String, Object>)event.get(PortalConstants.DATA);
        try (Connection conn = ds.getConnection()) {
            conn.setAutoCommit(false);
            try (PreparedStatement statement = conn.prepareStatement(sql)) {
                statement.setObject(1, UUID.fromString((String)map.get("configId")));
                statement.setObject(2, UUID.fromString((String)map.get("propertyId")));

                int count = statement.executeUpdate();
                if (count == 0) {
                    throw new SQLException("Failed to delete config property. No rows affected for config_id: " + map.get("configId") + " and property_id: " + map.get("propertyId"));
                }
                conn.commit();
                result = Success.of((String)map.get("configId"));
                notificationService.insertNotification(event, true, null);

            } catch (SQLException e) {
                logger.error("SQLException:", e);
                conn.rollback();
                notificationService.insertNotification(event, false, e.getMessage());
                result = Failure.of(new Status("SQL_EXCEPTION", e.getMessage()));
            } catch (Exception e) {
                logger.error("Exception:", e);
                conn.rollback();
                notificationService.insertNotification(event, false, e.getMessage());
                result = Failure.of(new Status("GENERIC_EXCEPTION", e.getMessage()));
            }
        } catch (SQLException e) {
            logger.error("SQLException:", e);
            result = Failure.of(new Status("SQL_EXCEPTION", e.getMessage()));
        }
        return result;
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
                cp.resource_type, cp.update_user, cp.update_ts, c.config_name
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
                cp.resource_type, cp.update_user, cp.update_ts, c.config_name
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
                cp.resource_type, cp.update_user, cp.update_ts, c.config_name
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
    public Result<String> createConfigEnvironment(Map<String, Object> event) {
        final String sql = "INSERT INTO environment_property_t (host_id, environment, property_id, " +
                "property_value, update_user, update_ts) " +
                "VALUES (?, ?, ?, ?, ?,  ?)";
        Result<String> result;
        Map<String, Object> map = (Map<String, Object>)event.get(PortalConstants.DATA);
        try (Connection conn = ds.getConnection()) {
            conn.setAutoCommit(false);
            try (PreparedStatement statement = conn.prepareStatement(sql)) {
                statement.setObject(1, UUID.fromString((String)map.get("hostId")));
                statement.setString(2, (String)map.get("environment"));
                statement.setObject(3, UUID.fromString((String)map.get("propertyId")));

                // Handle property_value (optional)
                if (map.containsKey("propertyValue")) {
                    statement.setString(4, (String) map.get("propertyValue"));
                } else {
                    statement.setNull(4, Types.VARCHAR);
                }

                statement.setString(5, (String)event.get(Constants.USER));
                statement.setObject(6, OffsetDateTime.parse((String)event.get(CloudEventV1.TIME)));

                int count = statement.executeUpdate();
                if (count == 0) {
                    throw new SQLException("Failed to insert environment property for environment: " + map.get("environment") +
                            ", host_id: " + map.get("hostId") + ", property_id: " + map.get("propertyId"));
                }
                conn.commit();
                result = Success.of((String)map.get("configId"));
                notificationService.insertNotification(event, true, null);


            } catch (SQLException e) {
                logger.error("SQLException:", e);
                conn.rollback();
                notificationService.insertNotification(event, false, e.getMessage());
                result = Failure.of(new Status("SQL_EXCEPTION", e.getMessage()));
            } catch (Exception e) {
                logger.error("Exception:", e);
                conn.rollback();
                notificationService.insertNotification(event, false, e.getMessage());

                result = Failure.of(new Status("GENERIC_EXCEPTION", e.getMessage()));
            }
        } catch (SQLException e) {
            logger.error("SQLException:", e);
            result = Failure.of(new Status("SQL_EXCEPTION", e.getMessage()));
        }
        return result;
    }

    @Override
    public Result<String> updateConfigEnvironment(Map<String, Object> event) {
        final String sql = "UPDATE environment_property_t SET property_value = ?, update_user = ?, update_ts = ? " +
                "WHERE host_id = ? AND environment = ? AND property_id = ?";
        Result<String> result;
        Map<String, Object> map = (Map<String, Object>)event.get(PortalConstants.DATA);
        try (Connection conn = ds.getConnection()) {
            conn.setAutoCommit(false);
            try (PreparedStatement statement = conn.prepareStatement(sql)) {

                if (map.containsKey("propertyValue")) {
                    statement.setString(1, (String) map.get("propertyValue"));
                } else {
                    statement.setNull(1, Types.VARCHAR); // Or keep existing
                }

                statement.setString(2, (String)event.get(Constants.USER));
                statement.setObject(3, OffsetDateTime.parse((String)event.get(CloudEventV1.TIME)));

                // WHERE clause parameters (from the event, *not* the JSON)
                statement.setObject(4, UUID.fromString((String)map.get("hostId")));
                statement.setString(5, (String)map.get("environment"));
                statement.setObject(6, UUID.fromString((String)map.get("propertyId")));

                int count = statement.executeUpdate();
                if (count == 0) {
                    throw new SQLException("Failed to update environment property. No rows affected for environment: " + map.get("environment") +
                            ", property_id: " + map.get("propertyId"));
                }
                conn.commit();
                result = Success.of((String)map.get("environment"));
                notificationService.insertNotification(event, true, null);

            } catch (SQLException e) {
                logger.error("SQLException:", e);
                conn.rollback();
                notificationService.insertNotification(event, false, e.getMessage());
                result = Failure.of(new Status("SQL_EXCEPTION", e.getMessage()));
            } catch (Exception e) {
                logger.error("Exception:", e);
                conn.rollback();
                notificationService.insertNotification(event, false, e.getMessage());
                result = Failure.of(new Status("GENERIC_EXCEPTION", e.getMessage()));
            }
        } catch (SQLException e) {
            logger.error("SQLException:", e);
            result = Failure.of(new Status("SQL_EXCEPTION", e.getMessage()));
        }
        return result;
    }


    @Override
    public Result<String> deleteConfigEnvironment(Map<String, Object> event) {
        final String sql = "DELETE FROM environment_property_t WHERE host_id = ? AND environment = ? AND property_id = ?";
        Result<String> result;
        Map<String, Object> map = (Map<String, Object>)event.get(PortalConstants.DATA);
        try (Connection conn = ds.getConnection()) {
            conn.setAutoCommit(false);
            try (PreparedStatement statement = conn.prepareStatement(sql)) {
                statement.setString(1, (String)map.get("hostId"));
                statement.setString(2, (String)map.get("environment"));
                statement.setObject(3, UUID.fromString((String)map.get("propertyId")));

                int count = statement.executeUpdate();
                if (count == 0) {
                    throw new SQLException("Failed to delete environment property. No rows affected for hostId: " + map.get("hostId") + ", environment: " + map.get("environment") +
                            ", property_id: " + map.get("propertyId"));
                }
                conn.commit();
                result = Success.of((String)map.get("hostId: " + map.get("hostId") + ", environment: " + map.get("environment") +
                        ", property_id: " + map.get("propertyId")));
                notificationService.insertNotification(event, true, null);
            } catch (SQLException e) {
                logger.error("SQLException:", e);
                conn.rollback();
                notificationService.insertNotification(event, false, e.getMessage());
                result = Failure.of(new Status("SQL_EXCEPTION", e.getMessage()));
            } catch (Exception e) {
                logger.error("Exception:", e);
                conn.rollback();
                notificationService.insertNotification(event, false, e.getMessage());
                result = Failure.of(new Status("GENERIC_EXCEPTION", e.getMessage()));
            }
        } catch (SQLException e) {
            logger.error("SQLException:", e);
            result = Failure.of(new Status("SQL_EXCEPTION", e.getMessage()));
        }
        return result;
    }

    @Override
    public Result<String> getConfigEnvironment(int offset, int limit, String hostId, String environment, String configId, String configName,
                                               String propertyId, String propertyName, String propertyValue) {
        Result<String> result = null;
        String s = """
                SELECT COUNT(*) OVER () AS total,
                ep.host_id, ep.environment, c.config_id, c.config_name,
                ep.property_id, p.property_name, ep.property_value,
                ep.update_user, ep.update_ts
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
    public Result<String> createConfigInstanceApi(Map<String, Object> event) {
        final String sql = "INSERT INTO instance_api_property_t (host_id, instance_api_id, property_id, " +
                "property_value, update_user, update_ts) VALUES (?, ?, ?, ?, ?,  ?)";
        Result<String> result;
        Map<String, Object> map = (Map<String, Object>)event.get(PortalConstants.DATA);
        try (Connection conn = ds.getConnection()) {
            conn.setAutoCommit(false);
            try (PreparedStatement statement = conn.prepareStatement(sql)) {
                statement.setObject(1, UUID.fromString((String)event.get(Constants.HOST)));
                statement.setObject(2, UUID.fromString((String)map.get("instanceApiId")));
                statement.setObject(3, UUID.fromString((String)map.get("propertyId")));
                if (map.containsKey("propertyValue")) {
                    statement.setString(4, (String)map.get("propertyValue"));
                } else {
                    statement.setNull(4, Types.VARCHAR);
                }
                statement.setString(5, (String)event.get(Constants.USER));
                statement.setObject(6, OffsetDateTime.parse((String)event.get(CloudEventV1.TIME)));

                int count = statement.executeUpdate();
                if (count == 0) {
                    throw new SQLException("Failed to insert instance API for host_id: " + event.get(Constants.HOST) +
                            ", instance_api_id: " + map.get("instanceApiId"));
                }
                conn.commit();
                result = Success.of(event.get(Constants.HOST) + "|" + map.get("instanceApiId"));
                notificationService.insertNotification(event, true, null);

            } catch (SQLException e) {
                logger.error("SQLException:", e);
                conn.rollback();
                notificationService.insertNotification(event, false, e.getMessage());
                result = Failure.of(new Status("SQL_EXCEPTION", e.getMessage()));
            } catch (Exception e) {
                logger.error("Exception:", e);
                conn.rollback();
                notificationService.insertNotification(event, false, e.getMessage());
                result = Failure.of(new Status("GENERIC_EXCEPTION", e.getMessage()));
            }
        } catch (SQLException e) {
            logger.error("SQLException:", e);
            result = Failure.of(new Status("SQL_EXCEPTION", e.getMessage()));
        }
        return result;
    }

    @Override
    public Result<String> updateConfigInstanceApi(Map<String, Object> event) {
        final String sql = "UPDATE instance_api_property_t SET " +
                "property_value = ?, update_user = ?, update_ts = ? " +
                "WHERE host_id = ? AND instance_api_id = ? AND property_id = ?";
        Result<String> result;
        Map<String, Object> map = (Map<String, Object>)event.get(PortalConstants.DATA);
        try (Connection conn = ds.getConnection()) {
            conn.setAutoCommit(false);
            try (PreparedStatement statement = conn.prepareStatement(sql)) {
                if (map.containsKey("propertyValue")) {
                    statement.setString(1, (String)map.get("propertyValue"));
                } else {
                    statement.setNull(1, Types.VARCHAR);
                }
                statement.setString(2, (String)event.get(Constants.USER));
                statement.setObject(3, OffsetDateTime.parse((String)event.get(CloudEventV1.TIME)));
                statement.setObject(4, UUID.fromString((String)event.get(Constants.HOST)));
                statement.setObject(5, UUID.fromString((String)map.get("instanceApiId")));
                statement.setObject(6, UUID.fromString((String)map.get("propertyId")));

                int count = statement.executeUpdate();
                if (count == 0) {
                    throw new SQLException("Failed to update instance API. No rows affected for host_id: " + event.get(Constants.HOST) +
                            ", instance_api_id: " + map.get("instanceApiId") +
                            ", property_id: " + map.get("propertyId"));
                }
                conn.commit();
                result = Success.of(event.get(Constants.HOST) + "|" + map.get("instanceApiId") + "|" + "|" + map.get("propertyId"));
                notificationService.insertNotification(event, true, null);

            } catch (SQLException e) {
                logger.error("SQLException:", e);
                conn.rollback();
                notificationService.insertNotification(event, false, e.getMessage());
                result = Failure.of(new Status("SQL_EXCEPTION", e.getMessage()));
            } catch (Exception e) {
                logger.error("Exception:", e);
                conn.rollback();
                notificationService.insertNotification(event, false, e.getMessage());
                result = Failure.of(new Status("GENERIC_EXCEPTION", e.getMessage()));
            }
        } catch (SQLException e) {
            logger.error("SQLException:", e);
            result = Failure.of(new Status("SQL_EXCEPTION", e.getMessage()));
        }
        return result;
    }

    @Override
    public Result<String> deleteConfigInstanceApi(Map<String, Object> event) {
        final String sql = "DELETE FROM instance_api_property_t " +
                "WHERE host_id = ? AND instance_api_id = ? AND property_id = ?";
        Result<String> result;
        Map<String, Object> map = (Map<String, Object>)event.get(PortalConstants.DATA);
        try (Connection conn = ds.getConnection()) {
            conn.setAutoCommit(false);
            try (PreparedStatement statement = conn.prepareStatement(sql)) {
                statement.setObject(1, UUID.fromString((String)event.get(Constants.HOST)));
                statement.setObject(2, UUID.fromString((String)map.get("instanceApiId")));
                statement.setObject(3, UUID.fromString((String)map.get("propertyId")));

                int count = statement.executeUpdate();
                if (count == 0) {
                    throw new SQLException("Failed to delete instance API. No rows affected for host_id: " + event.get(Constants.HOST) +
                            ", instance_api_id: " + map.get("instanceApiId"));
                }
                conn.commit();
                result = Success.of(event.get(Constants.HOST) + "|" + map.get("instanceApiId"));
                notificationService.insertNotification(event, true, null);

            } catch (SQLException e) {
                logger.error("SQLException:", e);
                conn.rollback();
                notificationService.insertNotification(event, false, e.getMessage());
                result = Failure.of(new Status("SQL_EXCEPTION", e.getMessage()));
            } catch (Exception e) {
                logger.error("Exception:", e);
                conn.rollback();
                notificationService.insertNotification(event, false, e.getMessage());
                result = Failure.of(new Status("GENERIC_EXCEPTION", e.getMessage()));
            }
        } catch (SQLException e) {
            logger.error("SQLException:", e);
            result = Failure.of(new Status("SQL_EXCEPTION", e.getMessage()));
        }
        return result;
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
                ia.update_user, ia.update_ts, p.config_id, c.config_name, iap.property_id, p.property_name, iap.property_value
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
                    map.put("updateUser", resultSet.getString("update_user"));
                    map.put("updateTs", resultSet.getObject("update_ts") != null ? resultSet.getObject("update_ts", OffsetDateTime.class) : null);
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
    public Result<String> createConfigInstanceApp(Map<String, Object> event) {
        final String sql = "INSERT INTO instance_app_property_t (host_id, instance_app_id, property_id, property_value, update_user, update_ts) " +
                "VALUES (?, ?, ?, ?, ?,  ?)";
        Result<String> result;
        Map<String, Object> map = (Map<String, Object>)event.get(PortalConstants.DATA);
        try (Connection conn = ds.getConnection()) {
            conn.setAutoCommit(false);
            try (PreparedStatement statement = conn.prepareStatement(sql)) {
                statement.setObject(1, UUID.fromString((String)event.get(Constants.HOST)));
                statement.setObject(2, UUID.fromString((String)map.get("instanceAppId")));
                statement.setObject(3, UUID.fromString((String)map.get("propertyId")));
                if (map.containsKey("propertyValue")) {
                    statement.setString(4, (String)map.get("propertyValue"));
                } else {
                    statement.setNull(4, Types.VARCHAR);
                }
                statement.setString(5, (String)event.get(Constants.USER));
                statement.setObject(6, OffsetDateTime.parse((String)event.get(CloudEventV1.TIME)));

                int count = statement.executeUpdate();
                if (count == 0) {
                    throw new SQLException("Failed to insert instance app for host_id: " + event.get(Constants.HOST) +
                            ", instance_app_id: " + map.get("instanceAppId"));
                }
                conn.commit();
                result = Success.of(event.get(Constants.HOST) + "|" + map.get("instanceAppId"));
                notificationService.insertNotification(event, true, null);


            } catch (SQLException e) {
                logger.error("SQLException:", e);
                conn.rollback();
                notificationService.insertNotification(event, false, e.getMessage());
                result = Failure.of(new Status("SQL_EXCEPTION", e.getMessage()));
            } catch (Exception e) {
                logger.error("Exception:", e);
                conn.rollback();
                notificationService.insertNotification(event, false, e.getMessage());
                result = Failure.of(new Status("GENERIC_EXCEPTION", e.getMessage()));
            }
        } catch (SQLException e) {
            logger.error("SQLException:", e);
            result = Failure.of(new Status("SQL_EXCEPTION", e.getMessage()));
        }
        return result;
    }


    @Override
    public Result<String> updateConfigInstanceApp(Map<String, Object> event) {
        final String sql = "UPDATE instance_app_property_t SET " +
                "property_value = ?, update_user = ?, update_ts = ? " +
                "WHERE host_id = ? AND instance_app_id = ? AND property_id = ?";

        Result<String> result;
        Map<String, Object> map = (Map<String, Object>)event.get(PortalConstants.DATA);
        try (Connection conn = ds.getConnection()) {
            conn.setAutoCommit(false);
            try (PreparedStatement statement = conn.prepareStatement(sql)) {
                if (map.containsKey("propertyValue")) {
                    statement.setString(1, (String)map.get("propertyValue"));
                } else {
                    statement.setNull(1, Types.VARCHAR);
                }
                statement.setString(2, (String)event.get(Constants.USER));
                statement.setObject(3, OffsetDateTime.parse((String)event.get(CloudEventV1.TIME)));
                statement.setObject(4, UUID.fromString((String)event.get(Constants.HOST)));
                statement.setObject(5, UUID.fromString((String)map.get("instanceAppId")));
                statement.setObject(6, UUID.fromString((String)map.get("propertyId")));

                int count = statement.executeUpdate();
                if (count == 0) {
                    throw new SQLException("Failed to update instance app.  No rows affected for host_id: " + event.get(Constants.HOST) +
                            ", instance_app_id: " + map.get("instanceAppId") +
                            ", property_id: " + map.get("propertyId"));
                }
                conn.commit();
                result = Success.of(event.get(Constants.HOST) + "|" + map.get("instanceAppId")  + "|" +  map.get("propertyId"));
                notificationService.insertNotification(event, true, null);

            } catch (SQLException e) {
                logger.error("SQLException:", e);
                conn.rollback();
                notificationService.insertNotification(event, false, e.getMessage());
                result = Failure.of(new Status("SQL_EXCEPTION", e.getMessage()));
            } catch (Exception e) {
                logger.error("Exception:", e);
                conn.rollback();
                notificationService.insertNotification(event, false, e.getMessage());
                result = Failure.of(new Status("GENERIC_EXCEPTION", e.getMessage()));
            }
        } catch (SQLException e) {
            logger.error("SQLException:", e);
            result = Failure.of(new Status("SQL_EXCEPTION", e.getMessage()));
        }
        return result;
    }


    @Override
    public Result<String> deleteConfigInstanceApp(Map<String, Object> event) {
        final String sql = "DELETE FROM instance_app_property_t " +
                "WHERE host_id = ? AND instance_app_id = ? AND property_id = ?";
        Result<String> result;
        Map<String, Object> map = (Map<String, Object>)event.get(PortalConstants.DATA);
        try (Connection conn = ds.getConnection()) {
            conn.setAutoCommit(false);
            try (PreparedStatement statement = conn.prepareStatement(sql)) {
                statement.setObject(1, UUID.fromString((String)event.get(Constants.HOST)));
                statement.setObject(2, UUID.fromString((String)map.get("instanceAppId")));
                statement.setObject(3, UUID.fromString((String)map.get("propertyId")));

                int count = statement.executeUpdate();
                if (count == 0) {
                    throw new SQLException("Failed to delete instance app. No rows affected for host_id: " + event.get(Constants.HOST) +
                            ", instance_app_id: " + map.get("instanceAppId"));
                }
                conn.commit();
                result = Success.of(event.get(Constants.HOST) + "|" + map.get("instanceAppId"));
                notificationService.insertNotification(event, true, null);

            } catch (SQLException e) {
                logger.error("SQLException:", e);
                conn.rollback();
                notificationService.insertNotification(event, false, e.getMessage());
                result = Failure.of(new Status("SQL_EXCEPTION", e.getMessage()));
            } catch (Exception e) {
                logger.error("Exception:", e);
                conn.rollback();
                notificationService.insertNotification(event, false, e.getMessage());
                result = Failure.of(new Status("GENERIC_EXCEPTION", e.getMessage()));
            }
        } catch (SQLException e) {
            logger.error("SQLException:", e);
            result = Failure.of(new Status("SQL_EXCEPTION", e.getMessage()));
        }
        return result;
    }

    @Override
    public Result<String> getConfigInstanceApp(int offset, int limit, String hostId, String instanceAppId, String instanceId,
                                               String instanceName, String appId, String appVersion, String configId, String configName,
                                               String propertyId, String propertyName, String propertyValue) {
        Result<String> result = null;
        String s =
                """
                SELECT COUNT(*) OVER () AS total,
                iap.host_id, iap.instance_app_id, ia.instance_id, i.instance_name, ia.app_id, ia.app_version, ia.active,\s
                ia.update_user, ia.update_ts, p.config_id, c.config_name, iap.property_id, p.property_name, iap.property_value
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
                    map.put("updateUser", resultSet.getString("update_user"));
                    map.put("updateTs", resultSet.getObject("update_ts") != null ? resultSet.getObject("update_ts", OffsetDateTime.class) : null);
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
    public Result<String> createConfigInstanceAppApi(Map<String, Object> event) {
        final String sql = "INSERT INTO instance_app_api_property_t (host_id, instance_app_id, instance_api_id, property_id, property_value, update_user, update_ts) " +
                "VALUES (?, ?, ?, ?, ?,  ?, ?)";
        Result<String> result;
        Map<String, Object> map = (Map<String, Object>)event.get(PortalConstants.DATA);
        try (Connection conn = ds.getConnection()) {
            conn.setAutoCommit(false);
            try (PreparedStatement statement = conn.prepareStatement(sql)) {
                statement.setObject(1, UUID.fromString((String)event.get(Constants.HOST)));
                statement.setObject(2, UUID.fromString((String)map.get("instanceAppId")));
                statement.setObject(3, UUID.fromString((String)map.get("instanceApiId")));
                statement.setObject(4, UUID.fromString((String)map.get("propertyId")));
                if (map.containsKey("propertyValue")) {
                    statement.setString(5, (String)map.get("propertyValue"));
                } else {
                    statement.setNull(5, Types.VARCHAR);
                }
                statement.setString(6, (String)event.get(Constants.USER));
                statement.setObject(7, OffsetDateTime.parse((String)event.get(CloudEventV1.TIME)));

                int count = statement.executeUpdate();
                if (count == 0) {
                    throw new SQLException("Failed to insert instance app api for host_id: " + event.get(Constants.HOST) +
                            ", instance_app_id: " + map.get("instanceAppId") + ", instance_api_id: " + map.get("instanceApiId"));
                }
                conn.commit();
                result = Success.of(event.get(Constants.HOST) + "|" + map.get("instanceAppId") + "|" + map.get("instanceApiId"));
                notificationService.insertNotification(event, true, null);
            } catch (SQLException e) {
                logger.error("SQLException:", e);
                conn.rollback();
                notificationService.insertNotification(event, false, e.getMessage());
                result = Failure.of(new Status("SQL_EXCEPTION", e.getMessage()));
            } catch (Exception e) {
                logger.error("Exception:", e);
                conn.rollback();
                notificationService.insertNotification(event, false, e.getMessage());
                result = Failure.of(new Status("GENERIC_EXCEPTION", e.getMessage()));
            }
        } catch (SQLException e) {
            logger.error("SQLException:", e);
            result = Failure.of(new Status("SQL_EXCEPTION", e.getMessage()));
        }
        return result;
    }


    @Override
    public Result<String> updateConfigInstanceAppApi(Map<String, Object> event) {
        final String sql = "UPDATE instance_app_api_property_t SET " +
                "property_value = ?, update_user = ?, update_ts = ? " +
                "WHERE host_id = ? AND instance_app_id = ? AND instance_api_id = ? AND property_id = ?";

        Result<String> result;
        Map<String, Object> map = (Map<String, Object>)event.get(PortalConstants.DATA);
        try (Connection conn = ds.getConnection()) {
            conn.setAutoCommit(false);
            try (PreparedStatement statement = conn.prepareStatement(sql)) {
                if (map.containsKey("propertyValue")) {
                    statement.setString(1, (String)map.get("propertyValue"));
                } else {
                    statement.setNull(1, Types.VARCHAR);
                }
                statement.setString(2, (String)event.get(Constants.USER));
                statement.setObject(3, OffsetDateTime.parse((String)event.get(CloudEventV1.TIME)));
                statement.setObject(4, UUID.fromString((String)event.get(Constants.HOST)));
                statement.setObject(5, UUID.fromString((String)map.get("instanceAppId")));
                statement.setObject(6, UUID.fromString((String)map.get("instanceApiId")));
                statement.setObject(7, UUID.fromString((String)map.get("propertyId")));

                int count = statement.executeUpdate();
                if (count == 0) {
                    throw new SQLException("Failed to update instance app api.  No rows affected for host_id: " + event.get(Constants.HOST) +
                            ", instance_app_id: " + map.get("instanceAppId") + ", instance_api_id: " + map.get("instanceApiId") +
                            ", property_id: " + map.get("propertyId"));
                }
                conn.commit();
                result = Success.of(event.get(Constants.HOST) + "|" + map.get("instanceAppId")  + "|" +  map.get("instanceApiId") + "|" + map.get("propertyId"));
                notificationService.insertNotification(event, true, null);

            } catch (SQLException e) {
                logger.error("SQLException:", e);
                conn.rollback();
                notificationService.insertNotification(event, false, e.getMessage());
                result = Failure.of(new Status("SQL_EXCEPTION", e.getMessage()));
            } catch (Exception e) {
                logger.error("Exception:", e);
                conn.rollback();
                notificationService.insertNotification(event, false, e.getMessage());
                result = Failure.of(new Status("GENERIC_EXCEPTION", e.getMessage()));
            }
        } catch (SQLException e) {
            logger.error("SQLException:", e);
            result = Failure.of(new Status("SQL_EXCEPTION", e.getMessage()));
        }
        return result;
    }


    @Override
    public Result<String> deleteConfigInstanceAppApi(Map<String, Object> event) {
        final String sql = "DELETE FROM instance_app_api_property_t " +
                "WHERE host_id = ? AND instance_app_id = ? AND instance_api_id = ? AND property_id = ?";
        Result<String> result;
        Map<String, Object> map = (Map<String, Object>)event.get(PortalConstants.DATA);
        try (Connection conn = ds.getConnection()) {
            conn.setAutoCommit(false);
            try (PreparedStatement statement = conn.prepareStatement(sql)) {
                statement.setObject(1, UUID.fromString((String)event.get(Constants.HOST)));
                statement.setObject(2, UUID.fromString((String)map.get("instanceAppId")));
                statement.setObject(3, UUID.fromString((String)map.get("instanceApiId")));
                statement.setObject(4, UUID.fromString((String)map.get("propertyId")));

                int count = statement.executeUpdate();
                if (count == 0) {
                    throw new SQLException("Failed to delete instance app api. No rows affected for host_id: " + event.get(Constants.HOST) +
                            ", instance_app_id: " + map.get("instanceAppId")  + ", instance_api_id: " + map.get("instanceApiId"));
                }
                conn.commit();
                result = Success.of(event.get(Constants.HOST) + "|" + map.get("instanceAppId") + "|" + map.get("instanceApiId"));
                notificationService.insertNotification(event, true, null);
            } catch (SQLException e) {
                logger.error("SQLException:", e);
                conn.rollback();
                notificationService.insertNotification(event, false, e.getMessage());
                result = Failure.of(new Status("SQL_EXCEPTION", e.getMessage()));
            } catch (Exception e) {
                logger.error("Exception:", e);
                conn.rollback();
                notificationService.insertNotification(event, false, e.getMessage());
                result = Failure.of(new Status("GENERIC_EXCEPTION", e.getMessage()));
            }
        } catch (SQLException e) {
            logger.error("SQLException:", e);
            result = Failure.of(new Status("SQL_EXCEPTION", e.getMessage()));
        }
        return result;
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
                p.property_name, iaap.property_value, iaap.update_user, iaap.update_ts
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
                    map.put("updateUser", resultSet.getString("update_user"));
                    map.put("updateTs", resultSet.getObject("update_ts") != null ? resultSet.getObject("update_ts", OffsetDateTime.class) : null);
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
    public Result<String> createConfigInstance(Map<String, Object> event) {
        // The table is now instance_property_t, NOT instance_t
        final String sql = "INSERT INTO instance_property_t (host_id, instance_id, property_id, " +
                "property_value, update_user, update_ts) VALUES (?, ?, ?, ?, ?, ?)";
        Result<String> result;
        Map<String, Object> map = (Map<String, Object>)event.get(PortalConstants.DATA);
        try (Connection conn = ds.getConnection()) {
            conn.setAutoCommit(false);
            try (PreparedStatement statement = conn.prepareStatement(sql)) {
                statement.setObject(1, UUID.fromString((String)event.get(Constants.HOST)));
                statement.setObject(2, UUID.fromString((String)map.get("instanceId")));
                statement.setObject(3, UUID.fromString((String)map.get("propertyId")));

                // Handle 'property_value' (optional)
                if (map.containsKey("propertyValue")) {
                    statement.setString(4, (String) map.get("propertyValue"));
                } else {
                    statement.setNull(4, Types.VARCHAR);
                }
                statement.setString(5, (String)event.get(Constants.USER));
                statement.setObject(6, OffsetDateTime.parse((String)event.get(CloudEventV1.TIME)));

                int count = statement.executeUpdate();
                if (count == 0) {
                    throw new SQLException("Failed to insert instance property for host_id: " + event.get(Constants.HOST) +
                            ", instance_id: " + map.get("instanceId") + ", property_id: " + map.get("propertyId"));
                }
                conn.commit();
                result = Success.of(event.get(Constants.HOST) + "|" + map.get("instanceId") + "|" + map.get("propertyId"));
                notificationService.insertNotification(event, true, null);


            } catch (SQLException e) {
                logger.error("SQLException:", e);
                conn.rollback();
                notificationService.insertNotification(event, false, e.getMessage());
                result = Failure.of(new Status("SQL_EXCEPTION", e.getMessage()));
            } catch (Exception e) {
                logger.error("Exception:", e);
                conn.rollback();
                notificationService.insertNotification(event, false, e.getMessage());
                result = Failure.of(new Status("GENERIC_EXCEPTION", e.getMessage()));
            }
        } catch (SQLException e) {
            logger.error("SQLException:", e);
            result = Failure.of(new Status("SQL_EXCEPTION", e.getMessage()));
        }
        return result;
    }


    @Override
    public Result<String> updateConfigInstance(Map<String, Object> event) {
        final String sql = "UPDATE instance_property_t SET property_value = ?, update_user = ?, update_ts = ? " +
                "WHERE host_id = ? AND instance_id = ? AND property_id = ?";
        Result<String> result;
        Map<String, Object> map = (Map<String, Object>)event.get(PortalConstants.DATA);
        try (Connection conn = ds.getConnection()) {
            conn.setAutoCommit(false);
            try (PreparedStatement statement = conn.prepareStatement(sql)) {

                // Handle 'property_value' (optional)
                if (map.containsKey("propertyValue")) {
                    statement.setString(1, (String) map.get("propertyValue"));
                } else {
                    statement.setNull(1, Types.VARCHAR); // Or keep existing
                }

                statement.setString(2, (String)event.get(Constants.USER));
                statement.setObject(3, OffsetDateTime.parse((String)event.get(CloudEventV1.TIME)));

                // WHERE clause parameters (from the event, NOT the JSON)
                statement.setObject(4, UUID.fromString((String)event.get(Constants.HOST)));
                statement.setObject(5, UUID.fromString((String)map.get("instanceId")));
                statement.setObject(6, UUID.fromString((String)map.get("propertyId")));

                int count = statement.executeUpdate();
                if (count == 0) {
                    throw new SQLException("Failed to update instance property. No rows affected for host_id: " + event.get(Constants.HOST) +
                            ", instance_id: " + map.get("instanceId") + ", property_id: " + map.get("propertyId"));
                }
                conn.commit();
                result = Success.of(event.get(Constants.HOST) + "|" + map.get("instanceId") + "|" + map.get("propertyId"));

                notificationService.insertNotification(event, true, null);

            } catch (SQLException e) {
                logger.error("SQLException:", e);
                conn.rollback();
                notificationService.insertNotification(event, false, e.getMessage());
                result = Failure.of(new Status("SQL_EXCEPTION", e.getMessage()));
            } catch (Exception e) {
                logger.error("Exception:", e);
                conn.rollback();
                notificationService.insertNotification(event, false, e.getMessage());
                result = Failure.of(new Status("GENERIC_EXCEPTION", e.getMessage()));
            }
        } catch (SQLException e) {
            logger.error("SQLException:", e);
            result = Failure.of(new Status("SQL_EXCEPTION", e.getMessage()));
        }
        return result;
    }


    @Override
    public Result<String> deleteConfigInstance(Map<String, Object> event) {
        final String sql = "DELETE FROM instance_property_t WHERE host_id = ? AND instance_id = ? AND property_id = ?";
        Result<String> result;
        Map<String, Object> map = (Map<String, Object>)event.get(PortalConstants.DATA);
        try (Connection conn = ds.getConnection()) {
            conn.setAutoCommit(false);
            try (PreparedStatement statement = conn.prepareStatement(sql)) {
                statement.setObject(1, UUID.fromString((String)event.get(Constants.HOST)));
                statement.setObject(2, UUID.fromString((String)map.get("instanceId")));
                statement.setObject(3, UUID.fromString((String)map.get("propertyId")));

                int count = statement.executeUpdate();
                if (count == 0) {
                    throw new SQLException("Failed to delete instance property. No rows affected for host_id: " + event.get(Constants.HOST) +
                            ", instance_id: " + map.get("instanceId") + ", property_id: " + map.get("propertyId"));
                }
                conn.commit();
                result = Success.of(event.get(Constants.HOST) + "|" + map.get("instanceId") + "|" + map.get("propertyId"));
                notificationService.insertNotification(event, true, null);

            } catch (SQLException e) {
                logger.error("SQLException:", e);
                conn.rollback();
                notificationService.insertNotification(event, false, e.getMessage());
                result = Failure.of(new Status("SQL_EXCEPTION", e.getMessage()));
            } catch (Exception e) {
                logger.error("Exception:", e);
                conn.rollback();
                notificationService.insertNotification(event, false, e.getMessage());
                result = Failure.of(new Status("GENERIC_EXCEPTION", e.getMessage()));
            }
        } catch (SQLException e) {
            logger.error("SQLException:", e);
            result = Failure.of(new Status("SQL_EXCEPTION", e.getMessage()));
        }
        return result;
    }

    @Override
    public Result<String> commitConfigInstance(Map<String, Object> event) {

        // 1. Extract Input Parameters
        UUID hostId = UUID.fromString((String) event.get("hostId"));
        UUID instanceId = UUID.fromString((String) event.get("instanceId"));
        String snapshotType = (String) event.getOrDefault("snapshotType", "USER_SAVE");
        String description = (String) event.get("description");
        UUID userId = UUID.fromString((String) event.get("userId"));              // either userId or deploymentId is passed here.
        UUID deploymentId = event.get("deploymentId") != null ? UUID.fromString((String) event.get("deploymentId")) : null;

        UUID snapshotId = UuidUtil.getUUID();

        Connection connection = null;
        try {
            connection = ds.getConnection();
            connection.setAutoCommit(false); // Start Transaction

            // 2. Derive Scope IDs
            // Query instance_t and potentially product_version_t based on hostId, instanceId
            DerivedScope scope = deriveScopeInfo(connection, hostId, instanceId);
            if (scope == null) {
                connection.rollback(); // Rollback if instance not found
                return Failure.of(new Status(OBJECT_NOT_FOUND, "Instance not found for hostId/instanceId."));
            }

            // 3. Insert Snapshot Metadata
            insertSnapshotMetadata(connection, snapshotId, snapshotType, description, userId, deploymentId, hostId, scope);

            // 4 & 5. Aggregate and Insert Effective Config
            insertEffectiveConfigSnapshot(connection, snapshotId, hostId, instanceId, scope);

            // 6. Snapshot Individual Override Tables
            // Use INSERT ... SELECT ... for efficiency
            snapshotInstanceProperties(connection, snapshotId, hostId, instanceId);
            snapshotInstanceApiProperties(connection, snapshotId, hostId, instanceId);
            snapshotInstanceAppProperties(connection, snapshotId, hostId, instanceId);
            snapshotInstanceAppApiProperties(connection, snapshotId, hostId, instanceId);
            snapshotEnvironmentProperties(connection, snapshotId, hostId, scope.environment());
            snapshotProductVersionProperties(connection, snapshotId, hostId, scope.productVersionId());
            snapshotProductProperties(connection, snapshotId, scope.productId());
            // Add others as needed

            // 7. Commit Transaction
            connection.commit();
            logger.info("Successfully created config snapshot: {}", snapshotId);
            return Success.of(snapshotId.toString());

        } catch (SQLException e) {
            logger.error("SQLException during snapshot creation for instance {}: {}", instanceId, e.getMessage(), e);
            if (connection != null) {
                try {
                    connection.rollback();
                } catch (SQLException ex) {
                    logger.error("Error rolling back transaction:", ex);
                }
            }
            return Failure.of(new Status(SQL_EXCEPTION, "Database error during snapshot creation."));
        } catch (Exception e) { // Catch other potential errors (e.g., during scope derivation)
            logger.error("Exception during snapshot creation for instance {}: {}", instanceId, e.getMessage(), e);
            if (connection != null) {
                try { connection.rollback(); } catch (SQLException ex) { logger.error("Error rolling back transaction:", ex); }
            }
            return Failure.of(new Status(GENERIC_EXCEPTION, "Unexpected error during snapshot creation."));
        } finally {
            if (connection != null) {
                try {
                    connection.setAutoCommit(true); // Restore default behavior
                    connection.close();
                } catch (SQLException e) {
                    logger.error("Error closing connection:", e);
                }
            }
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
                        c.config_phase,
                        cfg.config_name || '.' || cp.property_name AS property_key,
                        ro.property_value,
                        cp.property_type,
                        cp.value_type
                        -- Include ro.priority_level AS source_priority if storing provenance
                    FROM RankedOverrides ro
                    JOIN config_property_t cp ON ro.property_id = cp.property_id
                    JOIN config_t cfg ON cp.config_id = cfg.config_id
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
            selectStmt.setObject(paramIndex++, hostId);      // 1
            selectStmt.setObject(paramIndex++, instanceId);  // 2
            selectStmt.setObject(paramIndex++, hostId);      // 3
            selectStmt.setObject(paramIndex++, instanceId);  // 4
            selectStmt.setObject(paramIndex++, hostId);      // 5
            selectStmt.setObject(paramIndex++, hostId);      // 6
            selectStmt.setObject(paramIndex++, hostId);      // 7
            selectStmt.setString(paramIndex++, scope.environment()); // 6
            selectStmt.setString(paramIndex++, scope.productId());
            selectStmt.setString(paramIndex++, scope.serviceId());
            selectStmt.setObject(paramIndex++, hostId);
            selectStmt.setObject(paramIndex++, instanceId);
            selectStmt.setObject(paramIndex++, hostId);
            selectStmt.setObject(paramIndex++, instanceId);
            selectStmt.setObject(paramIndex++, hostId);
            selectStmt.setObject(paramIndex++, instanceId);
            selectStmt.setObject(paramIndex++, hostId);
            selectStmt.setObject(paramIndex++, instanceId);
            selectStmt.setObject(paramIndex++, hostId);
            selectStmt.setObject(paramIndex++, instanceId);
            selectStmt.setObject(paramIndex++, hostId);
            selectStmt.setObject(paramIndex++, instanceId);
            selectStmt.setObject(paramIndex++, hostId);
            selectStmt.setObject(paramIndex++, instanceId);


            try (ResultSet rs = selectStmt.executeQuery()) {
                int batchCount = 0;
                while (rs.next()) {
                    insertStmt.setObject(1, UuidUtil.getUUID()); // snapshot_property_id
                    insertStmt.setObject(2, snapshotId);
                    insertStmt.setString(3, rs.getString("config_phase"));
                    insertStmt.setObject(4, rs.getObject("config_id", UUID.class));
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
            Array sqlArray = conn.createArrayOf("UUID", apiIds.toArray());
            ps.setArray(3, sqlArray);
            ps.executeUpdate();
            sqlArray.free(); // Release array resources
        }
    }

    private void snapshotInstanceAppProperties(Connection conn, UUID snapshotId, UUID hostId, UUID instanceId) throws SQLException {
        // Find relevant instance_api_ids first
        List<UUID> appIds = findRelevantInstanceAppIds(conn, hostId, instanceId);
        if (appIds.isEmpty()) return; // No API overrides for this instance

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
            Array sqlArray = conn.createArrayOf("UUID", appIds.toArray());
            ps.setArray(3, sqlArray);
            ps.executeUpdate();
            sqlArray.free(); // Release array resources
        }
    }

    private void snapshotInstanceAppApiProperties(Connection conn, UUID snapshotId, UUID hostId, UUID instanceId) throws SQLException {
        List<UUID> apiIds = findRelevantInstanceApiIds(conn, hostId, instanceId);
        List<UUID> appIds = findRelevantInstanceAppIds(conn, hostId, instanceId);

        if (appIds.isEmpty()) return; // No API overrides for this instance

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
            Array sqlAppArray = conn.createArrayOf("UUID", appIds.toArray());
            ps.setArray(3, sqlAppArray);
            Array sqlApiArray = conn.createArrayOf("UUID", apiIds.toArray());
            ps.setArray(4, sqlApiArray);
            ps.executeUpdate();
            sqlAppArray.free(); // Release array resources
            sqlApiArray.free(); // Release array resources
        }
    }

    // Similar methods for snapshotInstanceAppProperties, snapshotInstanceAppApiProperties...
    // These will need helper methods like findRelevantInstanceApiIds/findRelevantInstanceAppIds

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
    public Result<String> rollbackConfigInstance(Map<String, Object> event) {
        final String DELETE_INSTANCE_PROPS_SQL = "DELETE FROM instance_property_t WHERE host_id = ? AND instance_id = ?";
        final String DELETE_INSTANCE_API_PROPS_SQL = "DELETE FROM instance_api_property_t WHERE host_id = ? AND instance_api_id = ANY(?)";
        final String DELETE_INSTANCE_APP_PROPS_SQL = "DELETE FROM instance_app_property_t WHERE host_id = ? AND instance_app_id = ANY(?)";
        final String DELETE_INSTANCE_APP_API_PROPS_SQL = "DELETE FROM instance_app_api_property_t WHERE host_id = ? AND instance_app_id = ANY(?) AND instance_api_id = ANY(?)";

        // INSERT ... SELECT Statements (From SNAPSHOT tables to LIVE tables)
        final String INSERT_INSTANCE_PROPS_SQL = """
        INSERT INTO instance_property_t
        (host_id, instance_id, property_id, property_value, update_user, update_ts)
        SELECT host_id, instance_id, property_id, property_value, update_user, update_ts
        FROM snapshot_instance_property_t
        WHERE snapshot_id = ? AND host_id = ? AND instance_id = ?
        """;
        final String INSERT_INSTANCE_API_PROPS_SQL = """
        INSERT INTO instance_api_property_t
        (host_id, instance_api_id, property_id, property_value, update_user, update_ts)
        SELECT host_id, instance_api_id, property_id, property_value, update_user, update_ts
        FROM snapshot_instance_api_property_t
        WHERE snapshot_id = ? AND host_id = ? AND instance_api_id = ANY(?)
        """;
        final String INSERT_INSTANCE_APP_PROPS_SQL = """
        INSERT INTO instance_app_property_t
        (host_id, instance_app_id, property_id, property_value, update_user, update_ts)
        SELECT host_id, instance_app_id, property_id, property_value, update_user, update_ts
        FROM snapshot_instance_app_property_t
        WHERE snapshot_id = ? AND host_id = ? AND instance_app_id = ANY(?)
        """;
        final String INSERT_INSTANCE_APP_API_PROPS_SQL = """
        INSERT INTO instance_app_api_property_t
        (host_id, instance_app_id, instance_api_id, property_id, property_value, update_user, update_ts)
        SELECT host_id, instance_app_id, instance_api_id, property_id, property_value, update_user, update_ts
        FROM snapshot_instance_app_api_property_t
        WHERE snapshot_id = ? AND host_id = ? AND instance_app_id = ANY(?) AND instance_api_id = ANY(?)
        """;

        // 1. Extract Input Parameters
        UUID snapshotId = (UUID) event.get("snapshotId");
        UUID hostId = (UUID) event.get("hostId");
        UUID instanceId = (UUID) event.get("instanceId");
        UUID userId = (UUID) event.get("userId"); // For potential auditing
        String description = (String) event.get("rollbackDescription"); // Optional reason

        Connection connection = null;
        List<UUID> currentApiIds = null;
        List<UUID> currentAppIds = null;

        try {
            connection = ds.getConnection();
            connection.setAutoCommit(false); // Start Transaction

            // --- Pre-computation: Find CURRENT associated IDs for DELETE scope ---
            // It's generally safer to delete based on current relationships and then
            // insert based on snapshot relationships if they could have diverged.
            currentApiIds = findRelevantInstanceApiIds(connection, hostId, instanceId);
            currentAppIds = findRelevantInstanceAppIds(connection, hostId, instanceId);
            // Note: InstanceAppApi requires both lists.

            logger.info("Starting rollback for instance {} (host {}) to snapshot {}", instanceId, hostId, snapshotId);

            // --- Execute Deletes from LIVE tables ---
            executeDelete(connection, DELETE_INSTANCE_PROPS_SQL, hostId, instanceId);

            if (!currentApiIds.isEmpty()) {
                executeDeleteWithArray(connection, DELETE_INSTANCE_API_PROPS_SQL, hostId, currentApiIds);
                // Also delete AppApi props related to these APIs if apps also exist
                if (!currentAppIds.isEmpty()) {
                    executeDeleteWithTwoArrays(connection, DELETE_INSTANCE_APP_API_PROPS_SQL, hostId, currentAppIds, currentApiIds);
                }
            }

            if (!currentAppIds.isEmpty()) {
                executeDeleteWithArray(connection, DELETE_INSTANCE_APP_PROPS_SQL, hostId, currentAppIds);
                // AppApi props deletion might have already happened above if APIs existed.
                // If only apps existed but no APIs, delete AppApi here (redundant if handled above)
                // Generally safe to run the AppApi delete again if needed, targeting only appIds.
                // For simplicity, we assume the AppApi delete targeting both arrays covers necessary cases.
            }


            // --- Execute Inserts from SNAPSHOT tables ---
            executeInsertSelect(connection, INSERT_INSTANCE_PROPS_SQL, snapshotId, hostId, instanceId);

            // For array-based inserts, we need the IDs *from the snapshot time*
            // However, the SELECT inside the INSERT query implicitly filters by snapshot_id AND the array condition,
            // so it should correctly only insert relationships that existed in the snapshot.
            // We still use the *current* IDs to DEFINE the overall scope of instance being affected,
            // but the INSERT...SELECT filters correctly based on snapshot content.
            if (!currentApiIds.isEmpty()) { // Use currentApiIds to decide IF we run the insert query
                executeInsertSelectWithArray(connection, INSERT_INSTANCE_API_PROPS_SQL, snapshotId, hostId, currentApiIds);
                if (!currentAppIds.isEmpty()) {
                    executeInsertSelectWithTwoArrays(connection, INSERT_INSTANCE_APP_API_PROPS_SQL, snapshotId, hostId, currentAppIds, currentApiIds);
                }
            }
            if (!currentAppIds.isEmpty()) { // Use currentAppIds to decide IF we run the insert query
                executeInsertSelectWithArray(connection, INSERT_INSTANCE_APP_PROPS_SQL, snapshotId, hostId, currentAppIds);
                // Redundant AppApi insert if handled above? No, the INSERT uses the AppId filter.
                // If only apps existed at snapshot time, this covers it.
            }

            // --- Optional: Audit Logging ---
            // logRollbackActivity(connection, snapshotId, hostId, instanceId, userId, description);


            // --- Commit Transaction ---
            connection.commit();
            logger.info("Successfully rolled back instance {} (host {}) to snapshot {}", instanceId, hostId, snapshotId);
            return Success.of("Rollback successful to snapshot " + snapshotId);

        } catch (SQLException e) {
            logger.error("SQLException during rollback for instance {} to snapshot {}: {}", instanceId, snapshotId, e.getMessage(), e);
            if (connection != null) {
                try {
                    connection.rollback();
                    logger.warn("Transaction rolled back for instance {} snapshot {}", instanceId, snapshotId);
                } catch (SQLException ex) {
                    logger.error("Error rolling back transaction:", ex);
                }
            }
            return Failure.of(new Status(SQL_EXCEPTION, "Database error during rollback operation."));
        } catch (Exception e) { // Catch other potential errors
            logger.error("Exception during rollback for instance {} to snapshot {}: {}", instanceId, snapshotId, e.getMessage(), e);
            if (connection != null) {
                try { connection.rollback(); } catch (SQLException ex) { logger.error("Error rolling back transaction:", ex); }
            }
            return Failure.of(new Status(GENERIC_EXCEPTION, "Unexpected error during rollback operation."));
        } finally {
            if (connection != null) {
                try {
                    connection.setAutoCommit(true); // Restore default behavior
                    connection.close();
                } catch (SQLException e) {
                    logger.error("Error closing connection:", e);
                }
            }
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
            Array sqlArray = conn.createArrayOf("UUID", idList.toArray());
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
            Array sqlArray1 = conn.createArrayOf("UUID", idList1.toArray());
            Array sqlArray2 = conn.createArrayOf("UUID", idList2.toArray());
            ps.setArray(2, sqlArray1);
            ps.setArray(3, sqlArray2);
            int rowsAffected = ps.executeUpdate();
            logger.debug("Deleted {} rows from {} for {}x{} IDs", rowsAffected, getTableNameFromDeleteSql(sql), idList1.size(), idList2.size());
            sqlArray1.free();
            sqlArray2.free();
        }
    }


    private void executeInsertSelect(Connection conn, String sql, UUID snapshotId, UUID hostId, UUID instanceId) throws SQLException {
        try (PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setObject(1, snapshotId);
            ps.setObject(2, hostId);
            ps.setObject(3, instanceId);
            int rowsAffected = ps.executeUpdate();
            logger.debug("Inserted {} rows into {} from snapshot {}", rowsAffected, getTableNameFromInsertSql(sql), snapshotId);
        }
    }

    private void executeInsertSelectWithArray(Connection conn, String sql, UUID snapshotId, UUID hostId, List<UUID> idList) throws SQLException {
        if (idList == null || idList.isEmpty()) return; // No scope to insert for
        try (PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setObject(1, snapshotId);
            ps.setObject(2, hostId);
            Array sqlArray = conn.createArrayOf("UUID", idList.toArray());
            ps.setArray(3, sqlArray);
            int rowsAffected = ps.executeUpdate();
            logger.debug("Inserted {} rows into {} from snapshot {} for {} IDs", rowsAffected, getTableNameFromInsertSql(sql), snapshotId, idList.size());
            sqlArray.free();
        }
    }

    private void executeInsertSelectWithTwoArrays(Connection conn, String sql, UUID snapshotId, UUID hostId, List<UUID> idList1, List<UUID> idList2) throws SQLException {
        if (idList1 == null || idList1.isEmpty() || idList2 == null || idList2.isEmpty()) return;
        try (PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setObject(1, snapshotId);
            ps.setObject(2, hostId);
            Array sqlArray1 = conn.createArrayOf("UUID", idList1.toArray());
            Array sqlArray2 = conn.createArrayOf("UUID", idList2.toArray());
            ps.setArray(3, sqlArray1);
            ps.setArray(4, sqlArray2);
            int rowsAffected = ps.executeUpdate();
            logger.debug("Inserted {} rows into {} from snapshot {} for {}x{} IDs", rowsAffected, getTableNameFromInsertSql(sql), snapshotId, idList1.size(), idList2.size());
            sqlArray1.free();
            sqlArray2.free();
        }
    }

    // --- Optional: Helper to get table name from SQL for logging ---
    private String getTableNameFromDeleteSql(String sql) {
        // Simple parsing, might need adjustment
        try { return sql.split("FROM ")[1].split(" ")[0]; } catch (Exception e) { return "[unknown table]"; }
    }
    private String getTableNameFromInsertSql(String sql) {
        try { return sql.split("INTO ")[1].split(" ")[0]; } catch (Exception e) { return "[unknown table]"; }
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
                p.property_name, ip.property_value, ip.update_user, ip.update_ts
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
                    map.put("updateUser", resultSet.getString("update_user"));
                    map.put("updateTs", resultSet.getObject("update_ts") != null ? resultSet.getObject("update_ts", OffsetDateTime.class) : null);

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
    public Result<String> createConfigInstanceFile(Map<String, Object> event) {
        final String sql = "INSERT INTO instance_file_t (host_id, instance_file_id, instance_id, file_type, " +
                "file_name, file_value, file_desc, expiration_ts, update_user, update_ts) " +
                "VALUES (?, ?, ?, ?, ?,  ?, ?, ?, ?, ?)";
        Result<String> result;
        Map<String, Object> map = (Map<String, Object>)event.get(PortalConstants.DATA);
        try (Connection conn = ds.getConnection()) {
            conn.setAutoCommit(false);
            try (PreparedStatement statement = conn.prepareStatement(sql)) {
                statement.setObject(1, UUID.fromString((String)event.get(Constants.HOST)));
                statement.setObject(2, UUID.fromString((String)map.get("instanceFileId")));
                statement.setObject(3, UUID.fromString((String)map.get("instanceId")));
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

                int count = statement.executeUpdate();
                if (count == 0) {
                    throw new SQLException("Failed to insert instance file for host_id: " + event.get(Constants.HOST) +
                            ", instance_file_id: " + map.get("instanceFileId") + ", instance_id: " + map.get("instanceId"));
                }
                conn.commit();
                result = Success.of(event.get(Constants.HOST) + "|" + map.get("instanceFileId") + "|" + map.get("instanceId"));
                notificationService.insertNotification(event, true, null);
            } catch (SQLException e) {
                logger.error("SQLException:", e);
                conn.rollback();
                notificationService.insertNotification(event, false, e.getMessage());
                result = Failure.of(new Status("SQL_EXCEPTION", e.getMessage()));
            } catch (Exception e) {
                logger.error("Exception:", e);
                conn.rollback();
                notificationService.insertNotification(event, false, e.getMessage());
                result = Failure.of(new Status("GENERIC_EXCEPTION", e.getMessage()));
            }
        } catch (SQLException e) {
            logger.error("SQLException:", e);
            result = Failure.of(new Status("SQL_EXCEPTION", e.getMessage()));
        }
        return result;

    }


    @Override
    public Result<String> updateConfigInstanceFile(Map<String, Object> event) {
        final String sql = "UPDATE instance_file_t SET file_type = ?, file_name = ?, file_value = ?, " +
                "file_desc = ?, expiration_ts = ?, update_user = ?, update_ts = ? " +
                "WHERE host_id = ? AND instance_file_id = ?";
        Result<String> result;
        Map<String, Object> map = (Map<String, Object>)event.get(PortalConstants.DATA);
        try (Connection conn = ds.getConnection()) {
            conn.setAutoCommit(false);
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

                statement.setObject(8, UUID.fromString((String)event.get(Constants.HOST)));
                statement.setObject(9, UUID.fromString((String)map.get("instanceFileId")));

                int count = statement.executeUpdate();
                if (count == 0) {
                    throw new SQLException("Failed to update instance file. No rows affected for host_id: " + event.get(Constants.HOST) +
                            ", instance_file_id: " + map.get("instanceFileId"));
                }
                conn.commit();
                result = Success.of(event.get(Constants.HOST) + "|" + map.get("instanceFileId"));

                notificationService.insertNotification(event, true, null);

            } catch (SQLException e) {
                logger.error("SQLException:", e);
                conn.rollback();
                notificationService.insertNotification(event, false, e.getMessage());
                result = Failure.of(new Status("SQL_EXCEPTION", e.getMessage()));
            } catch (Exception e) {
                logger.error("Exception:", e);
                conn.rollback();
                notificationService.insertNotification(event, false, e.getMessage());
                result = Failure.of(new Status("GENERIC_EXCEPTION", e.getMessage()));
            }
        } catch (SQLException e) {
            logger.error("SQLException:", e);
            result = Failure.of(new Status("SQL_EXCEPTION", e.getMessage()));
        }
        return result;

    }

    @Override
    public Result<String> deleteConfigInstanceFile(Map<String, Object> event) {
        final String sql = "DELETE FROM instance_file_t WHERE host_id = ? AND instance_file_id = ?";
        Result<String> result;
        Map<String, Object> map = (Map<String, Object>)event.get(PortalConstants.DATA);
        try (Connection conn = ds.getConnection()) {
            conn.setAutoCommit(false);
            try (PreparedStatement statement = conn.prepareStatement(sql)) {
                statement.setObject(1, UUID.fromString((String)event.get(Constants.HOST)));
                statement.setObject(2, UUID.fromString((String)map.get("instanceFileId")));

                int count = statement.executeUpdate();
                if (count == 0) {
                    throw new SQLException("Failed to delete instance file. No rows affected for host_id: " + event.get(Constants.HOST) +
                            ", instance_file_id: " + map.get("instanceFileId"));
                }
                conn.commit();
                result = Success.of(event.get(Constants.HOST) + "|" + map.get("instanceFileId"));
                notificationService.insertNotification(event, true, null);

            } catch (SQLException e) {
                logger.error("SQLException:", e);
                conn.rollback();
                notificationService.insertNotification(event, false, e.getMessage());
                result = Failure.of(new Status("SQL_EXCEPTION", e.getMessage()));
            } catch (Exception e) {
                logger.error("Exception:", e);
                conn.rollback();
                notificationService.insertNotification(event, false, e.getMessage());
                result = Failure.of(new Status("GENERIC_EXCEPTION", e.getMessage()));
            }
        } catch (SQLException e) {
            logger.error("SQLException:", e);
            result = Failure.of(new Status("SQL_EXCEPTION", e.getMessage()));
        }
        return result;

    }


    @Override
    public Result<String> getConfigInstanceFile(int offset, int limit, String hostId, String instanceFileId, String instanceId,
                                                String instanceName, String fileType, String fileName, String fileValue, String fileDesc,
                                                String expirationTs) {
        Result<String> result = null;
        String s =
                """
                SELECT COUNT(*) OVER () AS total,
                ift.host_id, ift.instance_file_id, ift.instance_id, i.instance_name,\s
                ift.file_type, ift.file_name, ift.file_value, ift.file_desc,\s
                ift.expiration_ts, ift.update_user, ift.update_ts
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
    public Result<String> createConfigDeploymentInstance(Map<String, Object> event) {
        final String sql = "INSERT INTO deployment_instance_property_t (host_id, deployment_instance_id, property_id, " +
                "property_value, update_user, update_ts) " +
                "VALUES (?, ?, ?, ?, ?,  ?)";
        Result<String> result;
        Map<String, Object> map = (Map<String, Object>)event.get(PortalConstants.DATA);
        try (Connection conn = ds.getConnection()) {
            conn.setAutoCommit(false);
            try (PreparedStatement statement = conn.prepareStatement(sql)) {
                statement.setObject(1, UUID.fromString((String)event.get(Constants.HOST)));
                statement.setObject(2, UUID.fromString((String)map.get("deploymentInstanceId")));
                statement.setObject(3, UUID.fromString((String)map.get("propertyId")));
                String propertyValue = (String) map.get("propertyValue");
                if(propertyValue != null && !propertyValue.isEmpty()) {
                    statement.setString(4, (String) map.get("propertyValue"));
                } else {
                    statement.setNull(4, Types.VARCHAR);
                }
                statement.setString(5, (String)event.get(Constants.USER));
                statement.setObject(6, OffsetDateTime.parse((String)event.get(CloudEventV1.TIME)));

                int count = statement.executeUpdate();
                if (count == 0) {
                    throw new SQLException("Failed to insert deployment instance property for host_id: " + event.get(Constants.HOST) +
                            ", deployment_instance_id: " + map.get("deploymentInstanceId") + ", property_id: " + map.get("propertyId"));
                }
                conn.commit();
                result = Success.of(event.get(Constants.HOST) + "|" + map.get("deploymentInstanceId") + "|" + map.get("propertyId"));
                notificationService.insertNotification(event, true, null);
            } catch (SQLException e) {
                logger.error("SQLException:", e);
                conn.rollback();
                notificationService.insertNotification(event, false, e.getMessage());
                result = Failure.of(new Status("SQL_EXCEPTION", e.getMessage()));
            } catch (Exception e) {
                logger.error("Exception:", e);
                conn.rollback();
                notificationService.insertNotification(event, false, e.getMessage());
                result = Failure.of(new Status("GENERIC_EXCEPTION", e.getMessage()));
            }
        } catch (SQLException e) {
            logger.error("SQLException:", e);
            result = Failure.of(new Status("SQL_EXCEPTION", e.getMessage()));
        }
        return result;
    }

    @Override
    public Result<String> updateConfigDeploymentInstance(Map<String, Object> event) {
        final String sql = "UPDATE deployment_instance_property_t SET property_value = ?, " +
                "update_user = ?, update_ts = ? " +
                "WHERE host_id = ? AND deployment_instance_id = ? AND property_id = ?";
        Result<String> result;
        Map<String, Object> map = (Map<String, Object>)event.get(PortalConstants.DATA);
        try (Connection conn = ds.getConnection()) {
            conn.setAutoCommit(false);
            try (PreparedStatement statement = conn.prepareStatement(sql)) {

                String propertyValue = (String)map.get("propertyValue");
                if (propertyValue != null && !propertyValue.isEmpty()) {
                    statement.setString(1, propertyValue);
                } else {
                    statement.setNull(1, Types.VARCHAR);
                }
                statement.setString(2, (String)event.get(Constants.USER));
                statement.setObject(3, OffsetDateTime.parse((String)event.get(CloudEventV1.TIME)));

                statement.setObject(4, UUID.fromString((String)event.get(Constants.HOST)));
                statement.setObject(5, UUID.fromString((String)map.get("deploymentInstanceId")));
                statement.setObject(6, UUID.fromString((String)map.get("propertyId")));

                int count = statement.executeUpdate();
                if (count == 0) {
                    throw new SQLException("Failed to update deployment instance property. No rows affected for host_id: " + event.get(Constants.HOST) +
                            ", deployment_instance_id: " + map.get("depoloymentInstanceId") + ", property_id: " + map.get("propertyId"));
                }
                conn.commit();
                result = Success.of(event.get(Constants.HOST) + "|" + map.get("deploymentInstanceId") + "|" + map.get("propertyId"));
                notificationService.insertNotification(event, true, null);
            } catch (SQLException e) {
                logger.error("SQLException:", e);
                conn.rollback();
                notificationService.insertNotification(event, false, e.getMessage());
                result = Failure.of(new Status("SQL_EXCEPTION", e.getMessage()));
            } catch (Exception e) {
                logger.error("Exception:", e);
                conn.rollback();
                notificationService.insertNotification(event, false, e.getMessage());
                result = Failure.of(new Status("GENERIC_EXCEPTION", e.getMessage()));
            }
        } catch (SQLException e) {
            logger.error("SQLException:", e);
            result = Failure.of(new Status("SQL_EXCEPTION", e.getMessage()));
        }
        return result;
    }

    @Override
    public Result<String> deleteConfigDeploymentInstance(Map<String, Object> event) {
        final String sql = "DELETE FROM deployment_instance_property_t " +
                "WHERE host_id = ? AND deployment_instance_id = ? AND property_id = ?";
        Result<String> result;
        Map<String, Object> map = (Map<String, Object>)event.get(PortalConstants.DATA);
        try (Connection conn = ds.getConnection()) {
            conn.setAutoCommit(false);
            try (PreparedStatement statement = conn.prepareStatement(sql)) {
                statement.setObject(1, UUID.fromString((String)event.get(Constants.HOST)));
                statement.setObject(2, UUID.fromString((String)map.get("deploymentInstanceId")));
                statement.setObject(3, UUID.fromString((String)map.get("propertyId")));

                int count = statement.executeUpdate();
                if (count == 0) {
                    throw new SQLException("Failed to delete deployment instance property. No rows affected for host_id: " + event.get(Constants.HOST) +
                            ", deployment_instance_id: " + map.get("deploymentInstanceId") + ", property_id: " + map.get("propertyId"));
                }
                conn.commit();
                result = Success.of(event.get(Constants.HOST) + "|" + map.get("deploymentInstanceId") + "|" + map.get("propertyId"));
                notificationService.insertNotification(event, true, null);
            } catch (SQLException e) {
                logger.error("SQLException:", e);
                conn.rollback();
                notificationService.insertNotification(event, false, e.getMessage());
                result = Failure.of(new Status("SQL_EXCEPTION", e.getMessage()));
            } catch (Exception e) {
                logger.error("Exception:", e);
                conn.rollback();
                notificationService.insertNotification(event, false, e.getMessage());
                result = Failure.of(new Status("GENERIC_EXCEPTION", e.getMessage()));
            }
        } catch (SQLException e) {
            logger.error("SQLException:", e);
            result = Failure.of(new Status("SQL_EXCEPTION", e.getMessage()));
        }
        return result;

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
                dip.property_id, cp.property_name, dip.property_value, dip.update_user, dip.update_ts
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
    public Result<String> createConfigProduct(Map<String, Object> event) {
        final String sql = "INSERT INTO product_property_t (product_id, property_id, property_value, update_user, update_ts) VALUES (?, ?, ?, ?, ?)";
        Result<String> result;
        Map<String, Object> map = (Map<String, Object>)event.get(PortalConstants.DATA);
        try (Connection conn = ds.getConnection()) {
            conn.setAutoCommit(false);
            try (PreparedStatement statement = conn.prepareStatement(sql)) {
                statement.setString(1, (String)map.get("productId"));
                statement.setObject(2, UUID.fromString((String)map.get("propertyId")));

                if (map.containsKey("propertyValue")) {
                    statement.setString(3, (String) map.get("propertyValue"));
                } else {
                    statement.setNull(3, Types.VARCHAR);
                }

                statement.setString(4, (String)event.get(Constants.USER));
                statement.setObject(5, OffsetDateTime.parse((String)event.get(CloudEventV1.TIME)));

                int count = statement.executeUpdate();
                if (count == 0) {
                    throw new SQLException("Failed to insert product property for product_id: " + map.get("productId") +
                            ", property_id: " + map.get("propertyId"));
                }
                conn.commit();
                result = Success.of(map.get("productId") + "|" + map.get("propertyId"));
                notificationService.insertNotification(event, true, null);

            } catch (SQLException e) {
                logger.error("SQLException:", e);
                conn.rollback();
                notificationService.insertNotification(event, false, e.getMessage());
                result = Failure.of(new Status("SQL_EXCEPTION", e.getMessage()));
            } catch (Exception e) {
                logger.error("Exception:", e);
                conn.rollback();
                notificationService.insertNotification(event, false, e.getMessage());
                result = Failure.of(new Status("GENERIC_EXCEPTION", e.getMessage()));
            }
        } catch (SQLException e) {
            logger.error("SQLException:", e);
            result = Failure.of(new Status("SQL_EXCEPTION", e.getMessage()));
        }
        return result;
    }

    @Override
    public Result<String> updateConfigProduct(Map<String, Object> event) {
        final String sql = "UPDATE product_property_t SET property_value = ?, update_user = ?, update_ts = ? " +
                "WHERE product_id = ? AND property_id = ?";
        Result<String> result;
        Map<String, Object> map = (Map<String, Object>)event.get(PortalConstants.DATA);
        try (Connection conn = ds.getConnection()) {
            conn.setAutoCommit(false);
            try (PreparedStatement statement = conn.prepareStatement(sql)) {

                // Handle 'property_value' (optional)
                if (map.containsKey("propertyValue")) {
                    statement.setString(1, (String) map.get("propertyValue"));
                } else {
                    statement.setNull(1, Types.VARCHAR);
                }

                statement.setString(2, (String)event.get(Constants.USER));
                statement.setObject(3, OffsetDateTime.parse((String)event.get(CloudEventV1.TIME)));

                // WHERE clause parameters (from the event, NOT the JSON)
                statement.setString(4, (String)map.get("productId"));
                statement.setObject(5, UUID.fromString((String)map.get("propertyId")));

                int count = statement.executeUpdate();
                if (count == 0) {
                    throw new SQLException("Failed to update product property. No rows affected for product_id: " + map.get("productId") +
                            ", property_id: " + map.get("propertyId"));
                }
                conn.commit();
                result = Success.of(map.get("productId") + "|" + map.get("propertyId"));
                notificationService.insertNotification(event, true, null);

            } catch (SQLException e) {
                logger.error("SQLException:", e);
                conn.rollback();
                notificationService.insertNotification(event, false, e.getMessage());
                result = Failure.of(new Status("SQL_EXCEPTION", e.getMessage()));
            } catch (Exception e) {
                logger.error("Exception:", e);
                conn.rollback();
                notificationService.insertNotification(event, false, e.getMessage());
                result = Failure.of(new Status("GENERIC_EXCEPTION", e.getMessage()));
            }
        } catch (SQLException e) {
            logger.error("SQLException:", e);
            result = Failure.of(new Status("SQL_EXCEPTION", e.getMessage()));
        }
        return result;
    }

    @Override
    public Result<String> deleteConfigProduct(Map<String, Object> event) {
        final String sql = "DELETE FROM product_property_t WHERE product_id = ? AND property_id = ?";
        Result<String> result;
        Map<String, Object> map = (Map<String, Object>)event.get(PortalConstants.DATA);
        try (Connection conn = ds.getConnection()) {
            conn.setAutoCommit(false);
            try (PreparedStatement statement = conn.prepareStatement(sql)) {
                statement.setString(1, (String)map.get("productId"));
                statement.setObject(2, UUID.fromString((String)map.get("propertyId")));

                int count = statement.executeUpdate();
                if (count == 0) {
                    throw new SQLException("Failed to delete product property. No rows affected for product_id: " + map.get("productId") +
                            ", property_id: " + map.get("propertyId"));
                }
                conn.commit();
                result = Success.of(map.get("productId") + "|" + map.get("propertyId"));
                notificationService.insertNotification(event, true, null);

            } catch (SQLException e) {
                logger.error("SQLException:", e);
                conn.rollback();
                notificationService.insertNotification(event, false, e.getMessage());
                result = Failure.of(new Status("SQL_EXCEPTION", e.getMessage()));
            } catch (Exception e) {
                logger.error("Exception:", e);
                conn.rollback();
                notificationService.insertNotification(event, false, e.getMessage());
                result = Failure.of(new Status("GENERIC_EXCEPTION", e.getMessage()));
            }
        } catch (SQLException e) {
            logger.error("SQLException:", e);
            result = Failure.of(new Status("SQL_EXCEPTION", e.getMessage()));
        }
        return result;
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
                        pp.update_user, pp.update_ts, c.config_name
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
    public Result<String> createConfigProductVersion(Map<String, Object> event) {
        final String sql = "INSERT INTO product_version_property_t (host_id, product_version_id, " +
                "property_id, property_value, update_user, update_ts) " +
                "VALUES (?, ?, ?, ?, ?,  ?)";
        Result<String> result;
        Map<String, Object> map = (Map<String, Object>)event.get(PortalConstants.DATA);
        try (Connection conn = ds.getConnection()) {
            conn.setAutoCommit(false);
            try (PreparedStatement statement = conn.prepareStatement(sql)) {
                statement.setObject(1, UUID.fromString((String)event.get(Constants.HOST)));
                statement.setObject(2, UUID.fromString((String)map.get("productVersionId")));
                statement.setObject(3, UUID.fromString((String)map.get("propertyId")));

                // Handle 'property_value' (optional)
                if (map.containsKey("propertyValue")) {
                    statement.setString(4, (String) map.get("propertyValue"));
                } else {
                    statement.setNull(4, Types.VARCHAR);
                }

                statement.setString(5, (String)event.get(Constants.USER));
                statement.setObject(6, OffsetDateTime.parse((String)event.get(CloudEventV1.TIME)));

                int count = statement.executeUpdate();
                if (count == 0) {
                    throw new SQLException("Failed to insert product version property for host_id: " + event.get(Constants.HOST) +
                            ", product_version_id: " + map.get("productVersionId") +
                            ", property_id: " + map.get("propertyId"));
                }
                conn.commit();
                result = Success.of(event.get(Constants.HOST) + "|" + map.get("productVersionId") + "|" + map.get("propertyId"));
                notificationService.insertNotification(event, true, null);

            } catch (SQLException e) {
                logger.error("SQLException:", e);
                conn.rollback();
                notificationService.insertNotification(event, false, e.getMessage());
                result = Failure.of(new Status("SQL_EXCEPTION", e.getMessage()));
            } catch (Exception e) {
                logger.error("Exception:", e);
                conn.rollback();
                notificationService.insertNotification(event, false, e.getMessage());
                result = Failure.of(new Status("GENERIC_EXCEPTION", e.getMessage()));
            }
        } catch (SQLException e) {
            logger.error("SQLException:", e);
            result = Failure.of(new Status("SQL_EXCEPTION", e.getMessage()));
        }
        return result;
    }

    @Override
    public Result<String> updateConfigProductVersion(Map<String, Object> event) {
        final String sql = "UPDATE product_version_property_t SET property_value = ?, update_user = ?, update_ts = ? " +
                "WHERE host_id = ? AND product_version_id = ? AND property_id = ?";
        Result<String> result;
        Map<String, Object> map = (Map<String, Object>)event.get(PortalConstants.DATA);
        try (Connection conn = ds.getConnection()) {
            conn.setAutoCommit(false);
            try (PreparedStatement statement = conn.prepareStatement(sql)) {

                // Handle 'property_value' (optional)
                if (map.containsKey("propertyValue")) {
                    statement.setString(1, (String) map.get("propertyValue"));
                } else {
                    statement.setNull(1, Types.VARCHAR); // Or keep existing
                }

                statement.setString(2, (String)event.get(Constants.USER));
                statement.setObject(3, OffsetDateTime.parse((String)event.get(CloudEventV1.TIME)));

                // WHERE clause parameters (from the event, NOT the JSON)
                statement.setObject(4, UUID.fromString((String)event.get(Constants.HOST)));
                statement.setObject(5, UUID.fromString((String)map.get("productVersionId")));
                statement.setObject(6, UUID.fromString((String)map.get("propertyId")));

                int count = statement.executeUpdate();
                if (count == 0) {
                    throw new SQLException("Failed to update product version property. No rows affected for host_id: " + event.get(Constants.HOST) +
                            ", product_version_id: " + map.get("productVersionId") +
                            ", property_id: " + map.get("propertyId"));
                }
                conn.commit();
                result = Success.of(event.get(Constants.HOST) + "|" + map.get("productVersionId") + "|" + map.get("propertyId"));
                notificationService.insertNotification(event, true, null);

            } catch (SQLException e) {
                logger.error("SQLException:", e);
                conn.rollback();
                notificationService.insertNotification(event, false, e.getMessage());
                result = Failure.of(new Status("SQL_EXCEPTION", e.getMessage()));
            } catch (Exception e) {
                logger.error("Exception:", e);
                conn.rollback();
                notificationService.insertNotification(event, false, e.getMessage());
                result = Failure.of(new Status("GENERIC_EXCEPTION", e.getMessage()));
            }
        } catch (SQLException e) {
            logger.error("SQLException:", e);
            result = Failure.of(new Status("SQL_EXCEPTION", e.getMessage()));
        }
        return result;
    }

    @Override
    public Result<String> deleteConfigProductVersion(Map<String, Object> event) {
        final String sql = "DELETE FROM product_version_property_t WHERE host_id = ? AND product_version_id = ? " +
                "AND property_id = ?";
        Result<String> result;
        Map<String, Object> map = (Map<String, Object>)event.get(PortalConstants.DATA);
        try (Connection conn = ds.getConnection()) {
            conn.setAutoCommit(false);
            try (PreparedStatement statement = conn.prepareStatement(sql)) {
                statement.setObject(1, UUID.fromString((String)event.get(Constants.HOST)));
                statement.setObject(2, UUID.fromString((String)map.get("productVersionId")));
                statement.setObject(3, UUID.fromString((String)map.get("propertyId")));

                int count = statement.executeUpdate();
                if (count == 0) {
                    throw new SQLException("Failed to delete product version property. No rows affected for host_id: " + event.get(Constants.HOST) +
                            ", product_version_id: " + map.get("productVersionId") +
                            ", property_id: " + map.get("propertyId"));
                }
                conn.commit();
                result = Success.of(event.get(Constants.HOST) + "|" + map.get("productVersionId") + "|" + map.get("propertyId"));
                notificationService.insertNotification(event, true, null);
            } catch (SQLException e) {
                logger.error("SQLException:", e);
                conn.rollback();
                notificationService.insertNotification(event, false, e.getMessage());
                result = Failure.of(new Status("SQL_EXCEPTION", e.getMessage()));
            } catch (Exception e) {
                logger.error("Exception:", e);
                conn.rollback();
                notificationService.insertNotification(event, false, e.getMessage());
                result = Failure.of(new Status("GENERIC_EXCEPTION", e.getMessage()));
            }
        } catch (SQLException e) {
            logger.error("SQLException:", e);
            result = Failure.of(new Status("SQL_EXCEPTION", e.getMessage()));
        }
        return result;
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
                p.property_name, pvp.property_value, pvp.update_user, pvp.update_ts, c.config_name
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
        SqlUtil.addCondition(whereClause, parameters, "pv.product_version_id", productVersion != null ? UUID.fromString(productVersion) : null);
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


}
