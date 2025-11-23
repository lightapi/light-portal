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
import static net.lightapi.portal.db.util.SqlUtil.*;

public class ConfigPersistenceImpl implements ConfigPersistence {
    private static final Logger logger = LoggerFactory.getLogger(ConfigPersistenceImpl.class);
    private static final String SQL_EXCEPTION = PortalDbProvider.SQL_EXCEPTION;
    private static final String GENERIC_EXCEPTION = PortalDbProvider.GENERIC_EXCEPTION;
    private static final String OBJECT_NOT_FOUND = PortalDbProvider.OBJECT_NOT_FOUND;

    private final NotificationService notificationService;

    public ConfigPersistenceImpl(NotificationService notificationService) {
        this.notificationService = notificationService;
    }

    /**
     * Creates or reactivates a config_t record using an idempotent UPSERT pattern.
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
    public void createConfig(Connection conn, Map<String, Object> event) throws SQLException, Exception {
        final String sql =
                """
                INSERT INTO config_t(config_id, config_name, config_phase, config_type, light4j_version,
                class_path, config_desc, update_user, update_ts, aggregate_version, active)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, TRUE)
                ON CONFLICT (config_id) DO UPDATE
                SET config_name = EXCLUDED.config_name,
                    config_phase = EXCLUDED.config_phase,
                    config_type = EXCLUDED.config_type,
                    light4j_version = EXCLUDED.light4j_version,
                    class_path = EXCLUDED.class_path,
                    config_desc = EXCLUDED.config_desc,
                    update_user = EXCLUDED.update_user,
                    update_ts = EXCLUDED.update_ts,
                    aggregate_version = EXCLUDED.aggregate_version,
                    active = TRUE
                WHERE config_t.aggregate_version < EXCLUDED.aggregate_version
                """;

        Map<String, Object> map = SqlUtil.extractEventData(event);
        String configId = (String) map.get("configId");
        long newAggregateVersion = SqlUtil.getNewAggregateVersion(event);

        try (PreparedStatement statement = conn.prepareStatement(sql)) {
            // Set parameters for the INSERT part of the statement
            int i = 1;
            statement.setObject(i++, UUID.fromString(configId));
            statement.setString(i++, (String) map.get("configName"));
            statement.setString(i++, (String) map.get("configPhase"));
            statement.setString(i++, (String) map.get("configType"));

            // Handle optional fields
            statement.setString(i++, (String) map.get("light4jVersion"));
            statement.setString(i++, (String) map.get("classPath"));
            statement.setString(i++, (String) map.get("configDesc"));

            // IDM and OCC fields
            statement.setString(i++, (String) event.get(Constants.USER));
            statement.setObject(i++, OffsetDateTime.parse((String) event.get(CloudEventV1.TIME)));
            statement.setLong(i++, newAggregateVersion);

            int count = statement.executeUpdate();

            if (count == 0) {
                // A count of 0 is a valid, idempotent outcome.
                logger.warn("Creation/Reactivation skipped for Config with configId {}. A newer or same version already exists.", configId);
            }
        } catch (SQLException e) {
            logger.error("SQLException during createConfig for configId {}: {}",
                    configId, e.getMessage(), e);
            throw e;
        } catch (Exception e) {
            logger.error("Exception during createConfig for configId {}: {}",
                    configId, e.getMessage(), e);
            throw e;
        }
    }


    /**
     * Updates a config_t record using an idempotent/monotonic pattern.
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
    public void updateConfig(Connection conn, Map<String, Object> event) throws SQLException, Exception {
        // SQL statement updated to match the idempotent update pattern.
        final String sql =
                """
                UPDATE config_t
                SET config_name = ?,
                    config_phase = ?,
                    config_type = ?,
                    light4j_version = ?,
                    class_path = ?,
                    config_desc = ?,
                    update_user = ?,
                    update_ts = ?,
                    aggregate_version = ?,
                    active = TRUE
                WHERE config_id = ?
                  AND aggregate_version < ?
                """;

        Map<String, Object> map = SqlUtil.extractEventData(event);
        String configId = (String) map.get("configId");
        long newAggregateVersion = SqlUtil.getNewAggregateVersion(event);

        try (PreparedStatement statement = conn.prepareStatement(sql)) {
            // SET clause placeholders (9)
            int i = 1;
            statement.setString(i++, (String) map.get("configName"));
            statement.setString(i++, (String) map.get("configPhase"));
            statement.setString(i++, (String) map.get("configType"));

            // Handle optional fields
            statement.setString(i++, (String) map.get("light4jVersion"));
            statement.setString(i++, (String) map.get("classPath"));
            statement.setString(i++, (String) map.get("configDesc"));

            // IDM and OCC fields
            statement.setString(i++, (String) event.get(Constants.USER));
            statement.setObject(i++, OffsetDateTime.parse((String) event.get(CloudEventV1.TIME)));
            statement.setLong(i++, newAggregateVersion);

            // WHERE clause placeholders (2)
            statement.setObject(i++, UUID.fromString(configId));
            statement.setLong(i++, newAggregateVersion);

            int count = statement.executeUpdate();

            if (count == 0) {
                // If 0 rows were updated, it's a valid idempotent outcome.
                logger.warn("Update skipped for Config with configId {}. Record not found or a newer version already exists.", configId);
            }
        } catch (SQLException e) {
            logger.error("SQLException during updateConfig for configId {}: {}",
                    configId, e.getMessage(), e);
            throw e;
        } catch (Exception e) {
            logger.error("Exception during updateConfig for configId {}: {}",
                    configId, e.getMessage(), e);
            throw e;
        }
    }

    /**
     * Performs a soft delete on a config_t record using an idempotent/monotonic pattern.
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
    public void deleteConfig(Connection conn, Map<String, Object> event) throws SQLException, Exception {
        // SQL statement updated to match the idempotent soft-delete pattern.
        final String sql =
                """
                UPDATE config_t
                SET active = FALSE,
                    update_user = ?,
                    update_ts = ?,
                    aggregate_version = ?
                WHERE config_id = ?
                  AND aggregate_version < ?
                """;

        // Extract data and metadata from the event.
        Map<String, Object> map = SqlUtil.extractEventData(event);
        String configId = (String) map.get("configId");
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

            // WHERE clause placeholders (2)
            // 4: config_id from primary key
            statement.setObject(i++, UUID.fromString(configId));
            // 5: aggregate_version < ? for OCC/Idempotency check
            statement.setLong(i++, newAggregateVersion);

            int count = statement.executeUpdate();
            if (count == 0) {
                // If 0 rows were updated, we log a warning and continue.
                // This indicates the record was already deleted, never existed, or a newer version is already in the database.
                // This is the expected behavior for an idempotent operation.
                logger.warn("Soft delete skipped for Config with configId {}, newAggregateVersion {}. Record not found or a newer version already exists.",
                        configId, newAggregateVersion);
            }
        } catch (SQLException e) {
            logger.error("SQLException during soft delete of Config for configId {}: {}",
                    configId, e.getMessage(), e);
            throw e;
        } catch (Exception e) {
            logger.error("Exception during soft delete of Config for configId {}: {}",
                    configId, e.getMessage(), e);
            throw e;
        }
    }

    @Override
    public Result<String> getConfig(int offset, int limit, String filtersJson, String globalFilter, String sortingJson) {
        Result<String> result = null;
        List<Map<String, Object>> filters = parseJsonList(filtersJson);
        List<Map<String, Object>> sorting = parseJsonList(sortingJson);
        String s =
                """
                SELECT COUNT(*) OVER () AS total,
                config_id, config_name, config_phase, config_type, light4j_version,
                class_path, config_desc, update_user, update_ts, aggregate_version, active
                FROM config_t
                WHERE 1=1
                """;

        List<Object> parameters = new ArrayList<>();
        String[] searchColumns = {"config_name", "config_desc"};
        String sqlBuilder = s + dynamicFilter(Arrays.asList("config_id"), Arrays.asList(searchColumns), filters, null, parameters) +
                globalFilter(globalFilter, searchColumns, parameters) +
                dynamicSorting("config_id", sorting, null) +
                "\nLIMIT ? OFFSET ?";

        parameters.add(limit);
        parameters.add(offset);

        int total = 0;
        List<Map<String, Object>> configs = new ArrayList<>();

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
                    map.put("active", resultSet.getBoolean("active"));

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
                class_path, config_desc, update_user, update_ts, aggregate_version, active
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
                    config.put("active", resultSet.getBoolean("active"));
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
    public String queryConfigId(String configName) {
        final String sql =
                """
                SELECT config_id
                FROM config_t WHERE config_name = ?
                """;
        String configId = null;
        try (Connection connection = ds.getConnection();
             PreparedStatement statement = connection.prepareStatement(sql)) {
            statement.setString(1, configName);
            try (ResultSet resultSet = statement.executeQuery()) {
                if(resultSet.next()){
                    configId = resultSet.getString(1);
                }
            }
        } catch (SQLException e) {
            logger.error("SQLException:", e);
        } catch (Exception e) {
            logger.error("Exception:", e);
        }
        return configId;
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

    /**
     * This is used in the import and configId is not available for enrichment.
     * @param configName config name
     * @param propertyName property name
     * @return propertyId
     */
    @Override
    public String queryPropertyId(String configName, String propertyName) {
        final String sql =
            """
            SELECT cp.property_id
            FROM config_property_t cp
            JOIN config_t c ON cp.config_id = c.config_id
            WHERE c.config_name = ?
            AND cp.property_name = ?
            """;
        String propertyId = null;
        try (Connection connection = ds.getConnection();
             PreparedStatement statement = connection.prepareStatement(sql)) {
            statement.setString(1, configName);
            statement.setString(2, propertyName);
            try (ResultSet resultSet = statement.executeQuery()) {
                if(resultSet.next()){
                    propertyId = resultSet.getString(1);
                }
            }
        } catch (SQLException e) {
            logger.error("SQLException:", e);
        } catch (Exception e) {
            logger.error("Exception:", e);
        }
        return propertyId;
    }

    /**
     * This is used by the handler to query the propertyId in case the same property is deleted and added back.
     * @param configId configId
     * @param propertyName propertyName
     * @return propertyId
     */
    @Override
    public String getPropertyId(String configId, String propertyName) {
        final String sql =
                """
                SELECT property_id
                FROM config_property_t
                WHERE config_id = ?
                AND property_name = ?
                """;
        String propertyId = null;
        try (Connection connection = ds.getConnection();
            PreparedStatement statement = connection.prepareStatement(sql)) {
            statement.setObject(1, UUID.fromString(configId));
            statement.setString(2, propertyName);
            try (ResultSet resultSet = statement.executeQuery()) {
                if(resultSet.next()){
                    propertyId = resultSet.getString(1);
                }
            }
        } catch (SQLException e) {
            logger.error("SQLException:", e);
        } catch (Exception e) {
            logger.error("Exception:", e);
        }
        return propertyId;
    }

    @Override
    public Result<String> getPropertyById(String propertyId) {
        final String sql =
                """
                SELECT config_id, property_id, property_name, property_type, light4j_version,
                display_order, required, property_desc, property_value, value_type, resource_type,
                update_user, update_ts, aggregate_version, active
                FROM config_property_t WHERE property_id = ?
                """;
        Result<String> result;
        Map<String, Object> map = new HashMap<>();

        try (Connection conn = ds.getConnection();
            PreparedStatement statement = conn.prepareStatement(sql)) {
            statement.setObject(1, UUID.fromString(propertyId));
            try (ResultSet resultSet = statement.executeQuery()) {
                if (resultSet.next()) {
                    map.put("configId", resultSet.getObject("config_id", UUID.class));
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
                    map.put("active", resultSet.getBoolean("active"));
                    result = Success.of(JsonMapper.toJson(map));
                } else {
                    result = Failure.of(new Status(OBJECT_NOT_FOUND, "property", propertyId));
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

    /**
     * Creates or reactivates a config_property_t record using an idempotent UPSERT pattern.
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
    public void createConfigProperty(Connection conn, Map<String, Object> event) throws SQLException, Exception {
        final String sql =
                """
                INSERT INTO config_property_t (config_id, property_id, property_name, property_type,
                property_value, resource_type, value_type, display_order, required, property_desc,
                light4j_version, update_user, update_ts, aggregate_version, active)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, TRUE)
                ON CONFLICT (property_id) DO UPDATE
                SET config_id = EXCLUDED.config_id,
                    property_name = EXCLUDED.property_name,
                    property_type = EXCLUDED.property_type,
                    property_value = EXCLUDED.property_value,
                    resource_type = EXCLUDED.resource_type,
                    value_type = EXCLUDED.value_type,
                    display_order = EXCLUDED.display_order,
                    required = EXCLUDED.required,
                    property_desc = EXCLUDED.property_desc,
                    light4j_version = EXCLUDED.light4j_version,
                    update_user = EXCLUDED.update_user,
                    update_ts = EXCLUDED.update_ts,
                    aggregate_version = EXCLUDED.aggregate_version,
                    active = TRUE
                WHERE config_property_t.aggregate_version < EXCLUDED.aggregate_version
                """;

        Map<String, Object> map = SqlUtil.extractEventData(event);
        String propertyId = (String) map.get("propertyId");
        long newAggregateVersion = SqlUtil.getNewAggregateVersion(event);

        try (PreparedStatement statement = conn.prepareStatement(sql)) {
            // Set parameters for the INSERT part of the statement
            int i = 1;
            statement.setObject(i++, UUID.fromString((String) map.get("configId")));
            statement.setObject(i++, UUID.fromString(propertyId));
            statement.setString(i++, (String) map.get("propertyName"));
            statement.setString(i++, (String) map.get("propertyType"));
            statement.setString(i++, (String) map.get("propertyValue"));
            statement.setString(i++, (String) map.get("resourceType"));
            statement.setString(i++, (String) map.get("valueType"));

            if (map.get("displayOrder") != null) {
                statement.setInt(i++, (Integer) map.get("displayOrder"));
            } else {
                statement.setNull(i++, Types.INTEGER);
            }

            if (map.get("required") != null) {
                statement.setBoolean(i++, (Boolean) map.get("required"));
            } else {
                statement.setBoolean(i++, false); // Default to false if not present
            }

            statement.setString(i++, (String) map.get("propertyDesc"));
            statement.setString(i++, (String) map.get("light4jVersion"));

            // IDM and OCC fields
            statement.setString(i++, (String) event.get(Constants.USER));
            statement.setObject(i++, OffsetDateTime.parse((String) event.get(CloudEventV1.TIME)));
            statement.setLong(i++, newAggregateVersion);

            int count = statement.executeUpdate();

            if (count == 0) {
                // A count of 0 is a valid, idempotent outcome.
                logger.warn("Creation/Reactivation skipped for ConfigProperty with propertyId {}. A newer or same version already exists.", propertyId);
            }
        } catch (SQLException e) {
            logger.error("SQLException during createConfigProperty for propertyId {}: {}",
                    propertyId, e.getMessage(), e);
            throw e;
        } catch (Exception e) {
            logger.error("Exception during createConfigProperty for propertyId {}: {}",
                    propertyId, e.getMessage(), e);
            throw e;
        }
    }

    /**
     * Updates a config_property_t record using an idempotent/monotonic pattern.
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
    public void updateConfigProperty(Connection conn, Map<String, Object> event) throws SQLException, Exception {
        final String sql =
                """
                UPDATE config_property_t
                SET property_name = ?,
                    property_type = ?,
                    property_value = ?,
                    resource_type = ?,
                    value_type = ?,
                    display_order = ?,
                    required = ?,
                    property_desc = ?,
                    light4j_version = ?,
                    update_user = ?,
                    update_ts = ?,
                    aggregate_version = ?,
                    active = TRUE
                WHERE property_id = ?
                  AND aggregate_version < ?
                """;

        Map<String, Object> map = SqlUtil.extractEventData(event);
        String propertyId = (String) map.get("propertyId");
        long newAggregateVersion = SqlUtil.getNewAggregateVersion(event);

        try (PreparedStatement statement = conn.prepareStatement(sql)) {
            // SET clause placeholders (12)
            int i = 1;
            statement.setString(i++, (String) map.get("propertyName"));
            statement.setString(i++, (String) map.get("propertyType"));
            statement.setString(i++, (String) map.get("propertyValue"));
            statement.setString(i++, (String) map.get("resourceType"));
            statement.setString(i++, (String) map.get("valueType"));

            if (map.get("displayOrder") != null) {
                statement.setInt(i++, (Integer) map.get("displayOrder"));
            } else {
                statement.setNull(i++, Types.INTEGER);
            }

            if (map.get("required") != null) {
                statement.setBoolean(i++, (Boolean) map.get("required"));
            } else {
                statement.setBoolean(i++, false); // Default to false if not present
            }

            statement.setString(i++, (String) map.get("propertyDesc"));
            statement.setString(i++, (String) map.get("light4jVersion"));

            // IDM and OCC fields
            statement.setString(i++, (String) event.get(Constants.USER));
            statement.setObject(i++, OffsetDateTime.parse((String) event.get(CloudEventV1.TIME)));
            statement.setLong(i++, newAggregateVersion);

            // WHERE clause placeholders (2)
            statement.setObject(i++, UUID.fromString(propertyId));
            statement.setLong(i++, newAggregateVersion);

            int count = statement.executeUpdate();

            if (count == 0) {
                // If 0 rows were updated, it's a valid idempotent outcome.
                logger.warn("Update skipped for ConfigProperty with propertyId {}. Record not found or a newer version already exists.", propertyId);
            }
        } catch (SQLException e) {
            logger.error("SQLException during updateConfigProperty for propertyId {}: {}",
                    propertyId, e.getMessage(), e);
            throw e;
        } catch (Exception e) {
            logger.error("Exception during updateConfigProperty for propertyId {}: {}",
                    propertyId, e.getMessage(), e);
            throw e;
        }
    }

    /**
     * Performs a soft delete on a config_property_t record using an idempotent/monotonic pattern.
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
    public void deleteConfigProperty(Connection conn, Map<String, Object> event) throws SQLException, Exception {
        // SQL statement updated to match the idempotent soft-delete pattern.
        final String sql =
                """
                UPDATE config_property_t
                SET active = FALSE,
                    update_user = ?,
                    update_ts = ?,
                    aggregate_version = ?
                WHERE property_id = ?
                  AND aggregate_version < ?
                """;

        // Extract data and metadata from the event.
        Map<String, Object> map = SqlUtil.extractEventData(event);
        String propertyId = (String) map.get("propertyId");
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

            // WHERE clause placeholders (2)
            // 4: property_id from primary key
            statement.setObject(i++, UUID.fromString(propertyId));
            // 5: aggregate_version < ? for OCC/Idempotency check
            statement.setLong(i++, newAggregateVersion);

            int count = statement.executeUpdate();
            if (count == 0) {
                // If 0 rows were updated, we log a warning and continue.
                // This indicates the record was already deleted, never existed, or a newer version is already in the database.
                // This is the expected behavior for an idempotent operation.
                logger.warn("Soft delete skipped for ConfigProperty with propertyId {}, newAggregateVersion {}. Record not found or a newer version already exists.",
                        propertyId, newAggregateVersion);
            }
        } catch (SQLException e) {
            logger.error("SQLException during soft delete of ConfigProperty for propertyId {}: {}",
                    propertyId, e.getMessage(), e);
            throw e;
        } catch (Exception e) {
            logger.error("Exception during soft delete of ConfigProperty for propertyId {}: {}",
                    propertyId, e.getMessage(), e);
            throw e;
        }
    }

    @Override
    public Result<String> getConfigProperty(int offset, int limit, String filtersJson, String globalFilter, String sortingJson) {
        Result<String> result = null;
        final Map<String, String> columnMap = new HashMap<>(Map.of(
                "configId", "cp.config_id",
                "propertyId", "cp.property_id",
                "propertyName", "cp.property_name",
                "propertyType", "cp.property_type",
                "light4jVersion", "cp.light4j_version",
                "displayOrder", "cp.display_order",
                "required", "cp.required",
                "propertyDesc", "cp.property_desc",
                "propertyValue", "cp.property_value"
        ));
        columnMap.put("valueType", "cp.value_type");
        columnMap.put("resourceType", "cp.resource_type");
        columnMap.put("updateUser", "cp.update_user");
        columnMap.put("updateTs", "cp.update_ts");
        columnMap.put("aggregateVersion", "cp.aggregate_version");
        columnMap.put("configName", "c.config_name");
        columnMap.put("active", "cp.active");


        List<Map<String, Object>> filters = parseJsonList(filtersJson);
        List<Map<String, Object>> sorting = parseJsonList(sortingJson);

        String s =
                """
                SELECT COUNT(*) OVER () AS total,
                cp.config_id, cp.property_id, cp.property_name, cp.property_type, cp.light4j_version,
                cp.display_order, cp.required, cp.property_desc, cp.property_value, cp.value_type,
                cp.resource_type, cp.update_user, cp.update_ts, c.config_name, cp.aggregate_version, cp.active
                FROM config_property_t cp
                JOIN config_t c ON cp.config_id = c.config_id
                WHERE 1=1
                """;

        List<Object> parameters = new ArrayList<>();
        String[] searchColumns = {"cp.property_name", "cp.property_desc", "c.config_name"};
        String sqlBuilder = s + dynamicFilter(Arrays.asList("cp.config_id", "cp.property_id"), Arrays.asList(searchColumns), filters, columnMap, parameters) +
                globalFilter(globalFilter, searchColumns, parameters) +
                dynamicSorting("cp.config_id, cp.display_order", sorting, columnMap) +
                "\nLIMIT ? OFFSET ?";

        parameters.add(limit);
        parameters.add(offset);

        int total = 0;
        List<Map<String, Object>> configProperties = new ArrayList<>();

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
                    map.put("active", resultSet.getBoolean("active"));

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

    /**
     * Creates or reactivates an environment_property_t record using an idempotent UPSERT pattern.
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
    public void createConfigEnvironment(Connection conn, Map<String, Object> event) throws SQLException, Exception {
        final String sql =
                """
                INSERT INTO environment_property_t (
                    host_id,
                    environment,
                    property_id,
                    property_value,
                    update_user,
                    update_ts,
                    aggregate_version,
                    active
                ) VALUES (?, ?, ?, ?, ?, ?, ?, TRUE)
                ON CONFLICT (host_id, environment, property_id) DO UPDATE
                SET property_value = EXCLUDED.property_value,
                    update_user = EXCLUDED.update_user,
                    update_ts = EXCLUDED.update_ts,
                    aggregate_version = EXCLUDED.aggregate_version,
                    active = TRUE
                WHERE environment_property_t.aggregate_version < EXCLUDED.aggregate_version
                """;

        Map<String, Object> map = SqlUtil.extractEventData(event);
        String hostId = (String) map.get("hostId");
        String environment = (String) map.get("environment");
        String propertyId = (String) map.get("propertyId");
        long newAggregateVersion = SqlUtil.getNewAggregateVersion(event);

        try (PreparedStatement statement = conn.prepareStatement(sql)) {
            // Set parameters for the INSERT part of the statement
            int i = 1;
            // Primary Key fields
            statement.setObject(i++, UUID.fromString(hostId));
            statement.setString(i++, environment);
            statement.setObject(i++, UUID.fromString(propertyId));

            // Mutable field
            statement.setString(i++, (String) map.get("propertyValue"));

            // IDM and OCC fields
            statement.setString(i++, (String) event.get(Constants.USER));
            statement.setObject(i++, OffsetDateTime.parse((String) event.get(CloudEventV1.TIME)));
            statement.setLong(i++, newAggregateVersion);

            int count = statement.executeUpdate();

            if (count == 0) {
                // A count of 0 is a valid, idempotent outcome.
                logger.warn("Creation/Reactivation skipped for ConfigEnvironment with hostId {}, environment {}, propertyId {}. A newer or same version already exists.",
                        hostId, environment, propertyId);
            }
        } catch (SQLException e) {
            logger.error("SQLException during createConfigEnvironment for hostId {} environment {} propertyId {}: {}",
                    hostId, environment, propertyId, e.getMessage(), e);
            throw e;
        } catch (Exception e) {
            logger.error("Exception during createConfigEnvironment for hostId {} environment {} propertyId {}: {}",
                    hostId, environment, propertyId, e.getMessage(), e);
            throw e;
        }
    }


    /**
     * Updates an environment_property_t record using an idempotent/monotonic pattern.
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
    public void updateConfigEnvironment(Connection conn, Map<String, Object> event) throws SQLException, Exception {
        // SQL statement updated to match the idempotent update pattern.
        final String sql =
                """
                UPDATE environment_property_t
                SET property_value = ?,
                    update_user = ?,
                    update_ts = ?,
                    aggregate_version = ?,
                    active = TRUE
                WHERE host_id = ?
                  AND environment = ?
                  AND property_id = ?
                  AND aggregate_version < ?
                """;

        Map<String, Object> map = SqlUtil.extractEventData(event);
        String hostId = (String) map.get("hostId");
        String environment = (String) map.get("environment");
        String propertyId = (String) map.get("propertyId");
        long newAggregateVersion = SqlUtil.getNewAggregateVersion(event);

        try (PreparedStatement statement = conn.prepareStatement(sql)) {
            // SET clause placeholders (4)
            int i = 1;
            statement.setString(i++, (String) map.get("propertyValue"));

            // IDM and OCC fields
            statement.setString(i++, (String) event.get(Constants.USER));
            statement.setObject(i++, OffsetDateTime.parse((String) event.get(CloudEventV1.TIME)));
            statement.setLong(i++, newAggregateVersion);

            // WHERE clause placeholders (4)
            statement.setObject(i++, UUID.fromString(hostId));
            statement.setString(i++, environment);
            statement.setObject(i++, UUID.fromString(propertyId));
            statement.setLong(i++, newAggregateVersion);

            int count = statement.executeUpdate();

            if (count == 0) {
                // If 0 rows were updated, it's a valid idempotent outcome.
                logger.warn("Update skipped for ConfigEnvironment with hostId {}, environment {}, propertyId {}. Record not found or a newer version already exists.",
                        hostId, environment, propertyId);
            }
        } catch (SQLException e) {
            logger.error("SQLException during updateConfigEnvironment for hostId {} environment {} propertyId {}: {}",
                    hostId, environment, propertyId, e.getMessage(), e);
            throw e;
        } catch (Exception e) {
            logger.error("Exception during updateConfigEnvironment for hostId {} environment {} propertyId {}: {}",
                    hostId, environment, propertyId, e.getMessage(), e);
            throw e;
        }
    }

    /**
     * Performs a soft delete on an environment_property_t record using an idempotent/monotonic pattern.
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
    public void deleteConfigEnvironment(Connection conn, Map<String, Object> event) throws SQLException, Exception {
        // SQL statement updated to match the idempotent soft-delete pattern.
        final String sql =
                """
                UPDATE environment_property_t
                SET active = FALSE,
                    update_user = ?,
                    update_ts = ?,
                    aggregate_version = ?
                WHERE host_id = ?
                  AND environment = ?
                  AND property_id = ?
                  AND aggregate_version < ?
                """;

        // Extract data and metadata from the event.
        Map<String, Object> map = SqlUtil.extractEventData(event);
        String hostId = (String) map.get("hostId");
        String environment = (String) map.get("environment");
        String propertyId = (String) map.get("propertyId");
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
            // 5: environment from primary key
            statement.setString(i++, environment);
            // 6: property_id from primary key
            statement.setObject(i++, UUID.fromString(propertyId));
            // 7: aggregate_version < ? for OCC/Idempotency check
            statement.setLong(i++, newAggregateVersion);

            int count = statement.executeUpdate();
            if (count == 0) {
                // If 0 rows were updated, we log a warning and continue.
                // This indicates the record was already deleted, never existed, or a newer version is already in the database.
                // This is the expected behavior for an idempotent operation.
                logger.warn("Soft delete skipped for ConfigEnvironment with hostId {}, environment {}, propertyId {}, newAggregateVersion {}. Record not found or a newer version already exists.",
                        hostId, environment, propertyId, newAggregateVersion);
            }
        } catch (SQLException e) {
            logger.error("SQLException during soft delete of ConfigEnvironment for hostId {} environment {} propertyId {}: {}",
                    hostId, environment, propertyId, e.getMessage(), e);
            throw e;
        } catch (Exception e) {
            logger.error("Exception during soft delete of ConfigEnvironment for hostId {} environment {} propertyId {}: {}",
                    hostId, environment, propertyId, e.getMessage(), e);
            throw e;
        }
    }

    @Override
    public Result<String> getConfigEnvironment(int offset, int limit, String filtersJson, String globalFilter, String sortingJson, String hostId) {
        Result<String> result = null;
        final Map<String, String> columnMap = new HashMap<>(Map.of(
                "hostId", "ep.host_id",
                "environment", "ep.environment",
                "configId", "c.config_id",
                "configName", "c.config_name",
                "propertyId", "ep.property_id",
                "propertyName", "p.property_name",
                "propertyValue", "ep.property_value",
                "updateUser", "ep.update_user",
                "updateTs", "ep.update_ts",
                "aggregateVersion", "ep.aggregate_version"
        ));
        columnMap.put("active", "ep.active");

        List<Map<String, Object>> filters = parseJsonList(filtersJson);
        List<Map<String, Object>> sorting = parseJsonList(sortingJson);

        String s = """
                SELECT COUNT(*) OVER () AS total,
                ep.host_id, ep.environment, c.config_id, c.config_name,
                ep.property_id, p.property_name, ep.property_value,
                ep.update_user, ep.update_ts, ep.aggregate_version, ep.active
                FROM environment_property_t ep
                JOIN config_property_t p ON ep.property_id = p.property_id
                JOIN config_t c ON p.config_id = c.config_id
                WHERE ep.host_id = ?
                """;

        List<Object> parameters = new ArrayList<>();
        parameters.add(UUID.fromString(hostId));

        String[] searchColumns = {"c.config_name", "p.property_name", "ep.propertyValue"};
        String sqlBuilder = s + dynamicFilter(Arrays.asList("ep.host_id", "c.config_id", "ep.property_id"), Arrays.asList(searchColumns), filters, columnMap, parameters) +
                globalFilter(globalFilter, searchColumns, parameters) +
                dynamicSorting("ep.environment, c.config_id, p.display_order", sorting, columnMap) +
                "\nLIMIT ? OFFSET ?";

        parameters.add(limit);
        parameters.add(offset);

        int total = 0;
        List<Map<String, Object>> configEnvironments = new ArrayList<>();

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
                    map.put("environment", resultSet.getString("environment"));
                    map.put("configId", resultSet.getObject("config_id", UUID.class));
                    map.put("configName", resultSet.getString("config_name"));
                    map.put("propertyName", resultSet.getString("property_name"));
                    map.put("propertyValue", resultSet.getString("property_value"));
                    map.put("propertyId", resultSet.getString("property_id"));
                    map.put("updateUser", resultSet.getString("update_user"));
                    map.put("updateTs", resultSet.getObject("update_ts") != null ? resultSet.getObject("update_ts", OffsetDateTime.class) : null);
                    map.put("aggregateVersion", resultSet.getLong("aggregate_version"));
                    map.put("active", resultSet.getBoolean("active"));
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
    public Result<String> getConfigEnvironmentById(String hostId, String environmentId, String propertyId) {
        final String sql =
                """
                SELECT ep.host_id, ep.environment, p.config_id, ep.property_id, ep.property_value,
                ep.aggregate_version, ep.active, ep.update_user, ep.update_ts
                FROM environment_property_t ep
                JOIN config_property_t p ON ep.property_id = p.property_id
                WHERE ep.host_id = ? AND ep.environment = ? AND ep.property_id = ?
                """;
        Result<String> result;
        Map<String, Object> map = new HashMap<>();

        String searchId = hostId + ":" + environmentId + ":" + propertyId;

        try (Connection conn = ds.getConnection();
             PreparedStatement statement = conn.prepareStatement(sql)) {

            // Set WHERE clause parameters
            statement.setObject(1, UUID.fromString(hostId));
            statement.setString(2, environmentId);
            statement.setObject(3, UUID.fromString(propertyId));

            try (ResultSet resultSet = statement.executeQuery()) {
                if (resultSet.next()) {
                    map.put("hostId", resultSet.getObject("host_id", UUID.class));
                    map.put("environment", resultSet.getString("environment"));
                    map.put("configId", resultSet.getObject("config_id", UUID.class));
                    map.put("propertyId", resultSet.getObject("property_id", UUID.class));
                    map.put("propertyValue", resultSet.getString("property_value"));
                    map.put("aggregateVersion", resultSet.getLong("aggregate_version"));
                    map.put("active", resultSet.getBoolean("active"));
                    map.put("updateUser", resultSet.getString("update_user"));
                    map.put("updateTs", resultSet.getObject("update_ts") != null ? resultSet.getObject("update_ts", OffsetDateTime.class) : null);
                    result = Success.of(JsonMapper.toJson(map));
                } else {
                    // Assuming OBJECT_NOT_FOUND and Status are available
                    result = Failure.of(new Status(OBJECT_NOT_FOUND, "environment_property", searchId));
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

    /**
     * Creates or reactivates an instance_api_property_t record using an idempotent UPSERT pattern.
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
    public void createConfigInstanceApi(Connection conn, Map<String, Object> event) throws SQLException, Exception {
        final String sql =
                """
                INSERT INTO instance_api_property_t (
                    host_id,
                    instance_api_id,
                    property_id,
                    property_value,
                    update_user,
                    update_ts,
                    aggregate_version,
                    active
                ) VALUES (?, ?, ?, ?, ?, ?, ?, TRUE)
                ON CONFLICT (host_id, instance_api_id, property_id) DO UPDATE
                SET property_value = EXCLUDED.property_value,
                    update_user = EXCLUDED.update_user,
                    update_ts = EXCLUDED.update_ts,
                    aggregate_version = EXCLUDED.aggregate_version,
                    active = TRUE
                WHERE instance_api_property_t.aggregate_version < EXCLUDED.aggregate_version
                """;

        Map<String, Object> map = SqlUtil.extractEventData(event);
        String hostId = (String) event.get(Constants.HOST);
        String instanceApiId = (String) map.get("instanceApiId");
        String propertyId = (String) map.get("propertyId");
        long newAggregateVersion = SqlUtil.getNewAggregateVersion(event);

        try (PreparedStatement statement = conn.prepareStatement(sql)) {
            // Set parameters for the INSERT part of the statement
            int i = 1;
            // Primary Key fields
            statement.setObject(i++, UUID.fromString(hostId));
            statement.setObject(i++, UUID.fromString(instanceApiId));
            statement.setObject(i++, UUID.fromString(propertyId));

            // Mutable field
            statement.setString(i++, (String) map.get("propertyValue"));

            // IDM and OCC fields
            statement.setString(i++, (String) event.get(Constants.USER));
            statement.setObject(i++, OffsetDateTime.parse((String) event.get(CloudEventV1.TIME)));
            statement.setLong(i++, newAggregateVersion);

            int count = statement.executeUpdate();

            if (count == 0) {
                // A count of 0 is a valid, idempotent outcome.
                logger.warn("Creation/Reactivation skipped for ConfigInstanceApi with hostId {}, instanceApiId {}, propertyId {}. A newer or same version already exists.",
                        hostId, instanceApiId, propertyId);
            }
        } catch (SQLException e) {
            logger.error("SQLException during createConfigInstanceApi for hostId {} instanceApiId {} propertyId {}: {}",
                    hostId, instanceApiId, propertyId, e.getMessage(), e);
            throw e;
        } catch (Exception e) {
            logger.error("Exception during createConfigInstanceApi for hostId {} instanceApiId {} propertyId {}: {}",
                    hostId, instanceApiId, propertyId, e.getMessage(), e);
            throw e;
        }
    }

    /**
     * Updates an instance_api_property_t record using an idempotent/monotonic pattern.
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
    public void updateConfigInstanceApi(Connection conn, Map<String, Object> event) throws SQLException, Exception {
        // SQL statement updated to match the idempotent update pattern.
        final String sql =
                """
                UPDATE instance_api_property_t
                SET property_value = ?,
                    update_user = ?,
                    update_ts = ?,
                    aggregate_version = ?,
                    active = TRUE
                WHERE host_id = ?
                  AND instance_api_id = ?
                  AND property_id = ?
                  AND aggregate_version < ?
                """;

        Map<String, Object> map = SqlUtil.extractEventData(event);
        String hostId = (String) event.get(Constants.HOST);
        String instanceApiId = (String) map.get("instanceApiId");
        String propertyId = (String) map.get("propertyId");
        long newAggregateVersion = SqlUtil.getNewAggregateVersion(event);

        try (PreparedStatement statement = conn.prepareStatement(sql)) {
            // SET clause placeholders (4)
            int i = 1;
            statement.setString(i++, (String) map.get("propertyValue"));

            // IDM and OCC fields
            statement.setString(i++, (String) event.get(Constants.USER));
            statement.setObject(i++, OffsetDateTime.parse((String) event.get(CloudEventV1.TIME)));
            statement.setLong(i++, newAggregateVersion);

            // WHERE clause placeholders (4)
            statement.setObject(i++, UUID.fromString(hostId));
            statement.setObject(i++, UUID.fromString(instanceApiId));
            statement.setObject(i++, UUID.fromString(propertyId));
            statement.setLong(i++, newAggregateVersion);

            int count = statement.executeUpdate();

            if (count == 0) {
                // If 0 rows were updated, it's a valid idempotent outcome.
                logger.warn("Update skipped for ConfigInstanceApi with hostId {}, instanceApiId {}, propertyId {}. Record not found or a newer version already exists.",
                        hostId, instanceApiId, propertyId);
            }
        } catch (SQLException e) {
            logger.error("SQLException during updateConfigInstanceApi for hostId {} instanceApiId {} propertyId {}: {}",
                    hostId, instanceApiId, propertyId, e.getMessage(), e);
            throw e;
        } catch (Exception e) {
            logger.error("Exception during updateConfigInstanceApi for hostId {} instanceApiId {} propertyId {}: {}",
                    hostId, instanceApiId, propertyId, e.getMessage(), e);
            throw e;
        }
    }

    /**
     * Performs a soft delete on an instance_api_property_t record using an idempotent/monotonic pattern.
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
    public void deleteConfigInstanceApi(Connection conn, Map<String, Object> event) throws SQLException, Exception {
        // SQL statement updated to match the idempotent soft-delete pattern.
        final String sql =
                """
                UPDATE instance_api_property_t
                SET active = FALSE,
                    update_user = ?,
                    update_ts = ?,
                    aggregate_version = ?
                WHERE host_id = ?
                  AND instance_api_id = ?
                  AND property_id = ?
                  AND aggregate_version < ?
                """;

        // Extract data and metadata from the event.
        Map<String, Object> map = SqlUtil.extractEventData(event);
        String hostId = (String) event.get(Constants.HOST);
        String instanceApiId = (String) map.get("instanceApiId");
        String propertyId = (String) map.get("propertyId");
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
            // 5: instance_api_id from primary key
            statement.setObject(i++, UUID.fromString(instanceApiId));
            // 6: property_id from primary key
            statement.setObject(i++, UUID.fromString(propertyId));
            // 7: aggregate_version < ? for OCC/Idempotency check
            statement.setLong(i++, newAggregateVersion);

            int count = statement.executeUpdate();
            if (count == 0) {
                // If 0 rows were updated, we log a warning and continue.
                // This is the expected behavior for an idempotent operation.
                logger.warn("Soft delete skipped for ConfigInstanceApi with hostId {}, instanceApiId {}, propertyId {}, newAggregateVersion {}. Record not found or a newer version already exists.",
                        hostId, instanceApiId, propertyId, newAggregateVersion);
            }
        } catch (SQLException e) {
            logger.error("SQLException during soft delete of ConfigInstanceApi for hostId {} instanceApiId {} propertyId {}: {}",
                    hostId, instanceApiId, propertyId, e.getMessage(), e);
            throw e;
        } catch (Exception e) {
            logger.error("Exception during soft delete of ConfigInstanceApi for hostId {} instanceApiId {} propertyId {}: {}",
                    hostId, instanceApiId, propertyId, e.getMessage(), e);
            throw e;
        }
    }

    @Override
    public Result<String> getConfigInstanceApi(int offset, int limit, String filtersJson, String globalFilter, String sortingJson, String hostId) {
        Result<String> result = null;
        final Map<String, String> columnMap = new HashMap<>(Map.of(
                "hostId", "iap.host_id",
                "instanceApiId", "iap.instance_api_id",
                "instanceId", "ia.instance_id",
                "instanceName", "i.instance_name",
                "apiVersionId", "ia.api_version_id",
                "apiId", "av.api_id",
                "apiVersion", "av.api_version",
                "active", "ia.active",
                "updateUser", "ia.update_user"
        ));
        columnMap.put("updateTs", "ia.update_ts");
        columnMap.put("configId", "p.config_id");
        columnMap.put("configName", "c.config_name");
        columnMap.put("propertyId", "iap.property_id");
        columnMap.put("propertyName", "p.property_name");
        columnMap.put("propertyValue", "iap.property_value");
        columnMap.put("required", "p.required");
        columnMap.put("propertyDesc", "p.property_desc");
        columnMap.put("propertyType", "p.property_type");
        columnMap.put("resourceType", "p.resource_type");
        columnMap.put("valueType", "p.value_type");
        columnMap.put("configType", "c.config_type");
        columnMap.put("configDesc", "c.config_desc");
        columnMap.put("classPath", "c.class_path");

        List<Map<String, Object>> filters = parseJsonList(filtersJson);
        List<Map<String, Object>> sorting = parseJsonList(sortingJson);

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
                WHERE iap.host_id = ?
                """;
        List<Object> parameters = new ArrayList<>();
        parameters.add(UUID.fromString(hostId));

        String[] searchColumns = {"i.instance_name", "c.config_name", "p.property_name", "p.property_desc", "c.config_desc"};
        String sqlBuilder = s + dynamicFilter(Arrays.asList("iap.host_id", "iap.instance_api_id", "ia.instance_id", "ia.api_version_id", "p.config_id", "iap.property_id"), Arrays.asList(searchColumns), filters, columnMap, parameters) +
                globalFilter(globalFilter, searchColumns, parameters) +
                dynamicSorting("iap.host_id, ia.instance_id, av.api_id, av.api_version, p.config_id, p.display_order", sorting, columnMap) +
                "\nLIMIT ? OFFSET ?";

        parameters.add(limit);
        parameters.add(offset);

        if(logger.isTraceEnabled()) logger.trace("sql = {}", sqlBuilder);
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
    public Result<String> getConfigInstanceApiById(String hostId, String instanceApiId, String propertyId) {
        final String sql =
                """
                SELECT host_id, instance_api_id, property_id, property_value,
                aggregate_version, active, update_user, update_ts
                FROM instance_api_property_t
                WHERE host_id = ? AND instance_api_id = ? AND property_id = ?
                """;
        Result<String> result;
        Map<String, Object> map = new HashMap<>();

        String searchId = hostId + ":" + instanceApiId + ":" + propertyId;

        try (Connection conn = ds.getConnection();
             PreparedStatement statement = conn.prepareStatement(sql)) {

            // Set WHERE clause parameters
            statement.setObject(1, UUID.fromString(hostId));
            statement.setObject(2, UUID.fromString(instanceApiId));
            statement.setObject(3, UUID.fromString(propertyId));

            try (ResultSet resultSet = statement.executeQuery()) {
                if (resultSet.next()) {
                    map.put("hostId", resultSet.getObject("host_id", UUID.class));
                    map.put("instanceApiId", resultSet.getObject("instance_api_id", UUID.class));
                    map.put("propertyId", resultSet.getObject("property_id", UUID.class));
                    map.put("propertyValue", resultSet.getString("property_value"));
                    map.put("aggregateVersion", resultSet.getLong("aggregate_version"));
                    map.put("active", resultSet.getBoolean("active"));
                    map.put("updateUser", resultSet.getString("update_user"));
                    map.put("updateTs", resultSet.getObject("update_ts") != null ? resultSet.getObject("update_ts", OffsetDateTime.class) : null);

                    result = Success.of(JsonMapper.toJson(map));
                } else {
                    result = Failure.of(new Status(OBJECT_NOT_FOUND, "instance_api_property", searchId));
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

    /**
     * Creates or reactivates an instance_app_property_t record using an idempotent UPSERT pattern.
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
    public void createConfigInstanceApp(Connection conn, Map<String, Object> event) throws SQLException, Exception {
        final String sql =
                """
                INSERT INTO instance_app_property_t (
                    host_id,
                    instance_app_id,
                    property_id,
                    property_value,
                    update_user,
                    update_ts,
                    aggregate_version,
                    active
                ) VALUES (?, ?, ?, ?, ?, ?, ?, TRUE)
                ON CONFLICT (host_id, instance_app_id, property_id) DO UPDATE
                SET property_value = EXCLUDED.property_value,
                    update_user = EXCLUDED.update_user,
                    update_ts = EXCLUDED.update_ts,
                    aggregate_version = EXCLUDED.aggregate_version,
                    active = TRUE
                WHERE instance_app_property_t.aggregate_version < EXCLUDED.aggregate_version
                """;

        Map<String, Object> map = SqlUtil.extractEventData(event);
        String hostId = (String) event.get(Constants.HOST);
        String instanceAppId = (String) map.get("instanceAppId");
        String propertyId = (String) map.get("propertyId");
        long newAggregateVersion = SqlUtil.getNewAggregateVersion(event);

        try (PreparedStatement statement = conn.prepareStatement(sql)) {
            // Set parameters for the INSERT part of the statement
            int i = 1;
            // Primary Key fields
            statement.setObject(i++, UUID.fromString(hostId));
            statement.setObject(i++, UUID.fromString(instanceAppId));
            statement.setObject(i++, UUID.fromString(propertyId));

            // Mutable field
            statement.setString(i++, (String) map.get("propertyValue"));

            // IDM and OCC fields
            statement.setString(i++, (String) event.get(Constants.USER));
            statement.setObject(i++, OffsetDateTime.parse((String) event.get(CloudEventV1.TIME)));
            statement.setLong(i++, newAggregateVersion);

            int count = statement.executeUpdate();

            if (count == 0) {
                // A count of 0 is a valid, idempotent outcome.
                logger.warn("Creation/Reactivation skipped for ConfigInstanceApp with hostId {}, instanceAppId {}, propertyId {}. A newer or same version already exists.",
                        hostId, instanceAppId, propertyId);
            }
        } catch (SQLException e) {
            logger.error("SQLException during createConfigInstanceApp for hostId {} instanceAppId {} propertyId {}: {}",
                    hostId, instanceAppId, propertyId, e.getMessage(), e);
            throw e;
        } catch (Exception e) {
            logger.error("Exception during createConfigInstanceApp for hostId {} instanceAppId {} propertyId {}: {}",
                    hostId, instanceAppId, propertyId, e.getMessage(), e);
            throw e;
        }
    }


    /**
     * Updates an instance_app_property_t record using an idempotent/monotonic pattern.
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
    public void updateConfigInstanceApp(Connection conn, Map<String, Object> event) throws SQLException, Exception {
        // SQL statement updated to match the idempotent update pattern.
        final String sql =
                """
                UPDATE instance_app_property_t
                SET property_value = ?,
                    update_user = ?,
                    update_ts = ?,
                    aggregate_version = ?,
                    active = TRUE
                WHERE host_id = ?
                  AND instance_app_id = ?
                  AND property_id = ?
                  AND aggregate_version < ?
                """;

        Map<String, Object> map = SqlUtil.extractEventData(event);
        String hostId = (String) event.get(Constants.HOST);
        String instanceAppId = (String) map.get("instanceAppId");
        String propertyId = (String) map.get("propertyId");
        long newAggregateVersion = SqlUtil.getNewAggregateVersion(event);

        try (PreparedStatement statement = conn.prepareStatement(sql)) {
            // SET clause placeholders (4)
            int i = 1;
            statement.setString(i++, (String) map.get("propertyValue"));

            // IDM and OCC fields
            statement.setString(i++, (String) event.get(Constants.USER));
            statement.setObject(i++, OffsetDateTime.parse((String) event.get(CloudEventV1.TIME)));
            statement.setLong(i++, newAggregateVersion);

            // WHERE clause placeholders (4)
            statement.setObject(i++, UUID.fromString(hostId));
            statement.setObject(i++, UUID.fromString(instanceAppId));
            statement.setObject(i++, UUID.fromString(propertyId));
            statement.setLong(i++, newAggregateVersion);

            int count = statement.executeUpdate();

            if (count == 0) {
                // If 0 rows were updated, it's a valid idempotent outcome.
                logger.warn("Update skipped for ConfigInstanceApp with hostId {}, instanceAppId {}, propertyId {}. Record not found or a newer version already exists.",
                        hostId, instanceAppId, propertyId);
            }
        } catch (SQLException e) {
            logger.error("SQLException during updateConfigInstanceApp for hostId {} instanceAppId {} propertyId {}: {}",
                    hostId, instanceAppId, propertyId, e.getMessage(), e);
            throw e;
        } catch (Exception e) {
            logger.error("Exception during updateConfigInstanceApp for hostId {} instanceAppId {} propertyId {}: {}",
                    hostId, instanceAppId, propertyId, e.getMessage(), e);
            throw e;
        }
    }

    /**
     * Performs a soft delete on an instance_app_property_t record using an idempotent/monotonic pattern.
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
    public void deleteConfigInstanceApp(Connection conn, Map<String, Object> event) throws SQLException, Exception {
        // SQL statement updated to match the idempotent soft-delete pattern.
        final String sql =
                """
                UPDATE instance_app_property_t
                SET active = FALSE,
                    update_user = ?,
                    update_ts = ?,
                    aggregate_version = ?
                WHERE host_id = ?
                  AND instance_app_id = ?
                  AND property_id = ?
                  AND aggregate_version < ?
                """;

        // Extract data and metadata from the event.
        Map<String, Object> map = SqlUtil.extractEventData(event);
        String hostId = (String) event.get(Constants.HOST);
        String instanceAppId = (String) map.get("instanceAppId");
        String propertyId = (String) map.get("propertyId");
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
            // 6: property_id from primary key
            statement.setObject(i++, UUID.fromString(propertyId));
            // 7: aggregate_version < ? for OCC/Idempotency check
            statement.setLong(i++, newAggregateVersion);

            int count = statement.executeUpdate();
            if (count == 0) {
                // If 0 rows were updated, we log a warning and continue.
                // This is the expected behavior for an idempotent operation.
                logger.warn("Soft delete skipped for ConfigInstanceApp with hostId {}, instanceAppId {}, propertyId {}, newAggregateVersion {}. Record not found or a newer version already exists.",
                        hostId, instanceAppId, propertyId, newAggregateVersion);
            }
        } catch (SQLException e) {
            logger.error("SQLException during soft delete of ConfigInstanceApp for hostId {} instanceAppId {} propertyId {}: {}",
                    hostId, instanceAppId, propertyId, e.getMessage(), e);
            throw e;
        } catch (Exception e) {
            logger.error("Exception during soft delete of ConfigInstanceApp for hostId {} instanceAppId {} propertyId {}: {}",
                    hostId, instanceAppId, propertyId, e.getMessage(), e);
            throw e;
        }
    }

    @Override
    public Result<String> getConfigInstanceApp(int offset, int limit, String filtersJson, String globalFilter, String sortingJson, String hostId) {
        Result<String> result = null;
        final Map<String, String> columnMap = new HashMap<>(Map.of(
                "hostId", "iap.host_id",
                "instanceAppId", "iap.instance_app_id",
                "instanceId", "ia.instance_id",
                "instanceName", "i.instance_name",
                "appId", "ia.app_id",
                "appVersion", "ia.app_version",
                "configId", "p.config_id",
                "configName", "c.config_name",
                "propertyId", "iap.property_id",
                "propertyName", "p.property_name"
                ));
        columnMap.put("propertyValue", "iap.property_value");
        columnMap.put("required", "p.required");
        columnMap.put("propertyDesc", "p.property_desc");
        columnMap.put("propertyType", "p.property_type");
        columnMap.put("resourceType", "p.resource_type");
        columnMap.put("valueType", "p.value_type");
        columnMap.put("configType", "c.config_type");
        columnMap.put("configDesc", "c.config_desc");
        columnMap.put("classPath", "c.class_path");
        columnMap.put("updateUser", "ia.update_user");
        columnMap.put("updateTs", "ia.update_ts");
        columnMap.put("aggregateVersion", "iap.aggregate_version");

        List<Map<String, Object>> filters = parseJsonList(filtersJson);
        List<Map<String, Object>> sorting = parseJsonList(sortingJson);

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
                WHERE iap.host_id = ?
                """;

        List<Object> parameters = new ArrayList<>();
        parameters.add(UUID.fromString(hostId));

        String[] searchColumns = {"i.instance_name", "c.config_name", "p.property_name", "p.property_desc", "c.config_desc"};
        String sqlBuilder = s + dynamicFilter(Arrays.asList("iap.host_id", "iap.instance_app_id", "ia.instance_id", "p.config_id", "iap.property_id"), Arrays.asList(searchColumns), filters, columnMap, parameters) +
                globalFilter(globalFilter, searchColumns, parameters) +
                dynamicSorting("iap.host_id, ia.instance_id, ia.app_id, ia.app_version, p.config_id, p.property_name", sorting, columnMap) +
                // Pagination
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
    public Result<String> getConfigInstanceAppById(String hostId, String instanceAppId, String propertyId) {
        final String sql =
                """
                SELECT host_id, instance_app_id, property_id, property_value,
                aggregate_version, active, update_user, update_ts
                FROM instance_app_property_t
                WHERE host_id = ? AND instance_app_id = ? AND property_id = ?
                """;
        Result<String> result;
        Map<String, Object> map = new HashMap<>();

        String searchId = hostId + ":" + instanceAppId + ":" + propertyId;

        try (Connection conn = ds.getConnection();
             PreparedStatement statement = conn.prepareStatement(sql)) {

            statement.setObject(1, UUID.fromString(hostId));
            statement.setObject(2, UUID.fromString(instanceAppId));
            statement.setObject(3, UUID.fromString(propertyId));

            try (ResultSet resultSet = statement.executeQuery()) {
                if (resultSet.next()) {
                    map.put("hostId", resultSet.getObject("host_id", UUID.class));
                    map.put("instanceAppId", resultSet.getObject("instance_app_id", UUID.class));
                    map.put("propertyId", resultSet.getObject("property_id", UUID.class));
                    map.put("propertyValue", resultSet.getString("property_value"));
                    map.put("aggregateVersion", resultSet.getLong("aggregate_version"));
                    map.put("active", resultSet.getBoolean("active"));
                    map.put("updateUser", resultSet.getString("update_user"));
                    map.put("updateTs", resultSet.getObject("update_ts") != null ? resultSet.getObject("update_ts", OffsetDateTime.class) : null);

                    result = Success.of(JsonMapper.toJson(map));
                } else {
                    result = Failure.of(new Status(OBJECT_NOT_FOUND, "instance_app_property", searchId));
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

    /**
     * Creates or reactivates an instance_app_api_property_t record using an idempotent UPSERT pattern.
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
    public void createConfigInstanceAppApi(Connection conn, Map<String, Object> event) throws SQLException, Exception {
        final String sql =
                """
                INSERT INTO instance_app_api_property_t (
                    host_id,
                    instance_app_id,
                    instance_api_id,
                    property_id,
                    property_value,
                    update_user,
                    update_ts,
                    aggregate_version,
                    active
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, TRUE)
                ON CONFLICT (host_id, instance_app_id, instance_api_id, property_id) DO UPDATE
                SET property_value = EXCLUDED.property_value,
                    update_user = EXCLUDED.update_user,
                    update_ts = EXCLUDED.update_ts,
                    aggregate_version = EXCLUDED.aggregate_version,
                    active = TRUE
                WHERE instance_app_api_property_t.aggregate_version < EXCLUDED.aggregate_version
                """;

        Map<String, Object> map = SqlUtil.extractEventData(event);
        String hostId = (String) event.get(Constants.HOST);
        String instanceAppId = (String) map.get("instanceAppId");
        String instanceApiId = (String) map.get("instanceApiId");
        String propertyId = (String) map.get("propertyId");
        long newAggregateVersion = SqlUtil.getNewAggregateVersion(event);

        try (PreparedStatement statement = conn.prepareStatement(sql)) {
            // Set parameters for the INSERT part of the statement
            int i = 1;
            // Primary Key fields
            statement.setObject(i++, UUID.fromString(hostId));
            statement.setObject(i++, UUID.fromString(instanceAppId));
            statement.setObject(i++, UUID.fromString(instanceApiId));
            statement.setObject(i++, UUID.fromString(propertyId));

            // Mutable field
            statement.setString(i++, (String) map.get("propertyValue"));

            // IDM and OCC fields
            statement.setString(i++, (String) event.get(Constants.USER));
            statement.setObject(i++, OffsetDateTime.parse((String) event.get(CloudEventV1.TIME)));
            statement.setLong(i++, newAggregateVersion);

            int count = statement.executeUpdate();

            if (count == 0) {
                // A count of 0 is a valid, idempotent outcome.
                logger.warn("Creation/Reactivation skipped for ConfigInstanceAppApi with hostId {}, instanceAppId {}, instanceApiId {}, propertyId {}. A newer or same version already exists.",
                        hostId, instanceAppId, instanceApiId, propertyId);
            }
        } catch (SQLException e) {
            logger.error("SQLException during createConfigInstanceAppApi for hostId {} instanceAppId {} instanceApiId {} propertyId {}: {}",
                    hostId, instanceAppId, instanceApiId, propertyId, e.getMessage(), e);
            throw e;
        } catch (Exception e) {
            logger.error("Exception during createConfigInstanceAppApi for hostId {} instanceAppId {} instanceApiId {} propertyId {}: {}",
                    hostId, instanceAppId, instanceApiId, propertyId, e.getMessage(), e);
            throw e;
        }
    }

    /**
     * Updates an instance_app_api_property_t record using an idempotent/monotonic pattern.
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
    public void updateConfigInstanceAppApi(Connection conn, Map<String, Object> event) throws SQLException, Exception {
        // SQL statement updated to match the idempotent update pattern.
        final String sql =
                """
                UPDATE instance_app_api_property_t
                SET property_value = ?,
                    update_user = ?,
                    update_ts = ?,
                    aggregate_version = ?,
                    active = TRUE
                WHERE host_id = ?
                  AND instance_app_id = ?
                  AND instance_api_id = ?
                  AND property_id = ?
                  AND aggregate_version < ?
                """;

        Map<String, Object> map = SqlUtil.extractEventData(event);
        String hostId = (String) event.get(Constants.HOST);
        String instanceAppId = (String) map.get("instanceAppId");
        String instanceApiId = (String) map.get("instanceApiId");
        String propertyId = (String) map.get("propertyId");
        long newAggregateVersion = SqlUtil.getNewAggregateVersion(event);

        try (PreparedStatement statement = conn.prepareStatement(sql)) {
            // SET clause placeholders (4)
            int i = 1;
            statement.setString(i++, (String) map.get("propertyValue"));

            // IDM and OCC fields
            statement.setString(i++, (String) event.get(Constants.USER));
            statement.setObject(i++, OffsetDateTime.parse((String) event.get(CloudEventV1.TIME)));
            statement.setLong(i++, newAggregateVersion);

            // WHERE clause placeholders (5)
            statement.setObject(i++, UUID.fromString(hostId));
            statement.setObject(i++, UUID.fromString(instanceAppId));
            statement.setObject(i++, UUID.fromString(instanceApiId));
            statement.setObject(i++, UUID.fromString(propertyId));
            statement.setLong(i++, newAggregateVersion);

            int count = statement.executeUpdate();

            if (count == 0) {
                // If 0 rows were updated, it's a valid idempotent outcome.
                logger.warn("Update skipped for ConfigInstanceAppApi with hostId {}, instanceAppId {}, instanceApiId {}, propertyId {}. Record not found or a newer version already exists.",
                        hostId, instanceAppId, instanceApiId, propertyId);
            }
        } catch (SQLException e) {
            logger.error("SQLException during updateConfigInstanceAppApi for hostId {} instanceAppId {} instanceApiId {} propertyId {}: {}",
                    hostId, instanceAppId, instanceApiId, propertyId, e.getMessage(), e);
            throw e;
        } catch (Exception e) {
            logger.error("Exception during updateConfigInstanceAppApi for hostId {} instanceAppId {} instanceApiId {} propertyId {}: {}",
                    hostId, instanceAppId, instanceApiId, propertyId, e.getMessage(), e);
            throw e;
        }
    }

    /**
     * Performs a soft delete on an instance_app_api_property_t record using an idempotent/monotonic pattern.
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
    public void deleteConfigInstanceAppApi(Connection conn, Map<String, Object> event) throws SQLException, Exception {
        // SQL statement updated to match the idempotent soft-delete pattern.
        final String sql =
                """
                UPDATE instance_app_api_property_t
                SET active = FALSE,
                    update_user = ?,
                    update_ts = ?,
                    aggregate_version = ?
                WHERE host_id = ?
                  AND instance_app_id = ?
                  AND instance_api_id = ?
                  AND property_id = ?
                  AND aggregate_version < ?
                """;

        // Extract data and metadata from the event.
        Map<String, Object> map = SqlUtil.extractEventData(event);
        String hostId = (String) event.get(Constants.HOST);
        String instanceAppId = (String) map.get("instanceAppId");
        String instanceApiId = (String) map.get("instanceApiId");
        String propertyId = (String) map.get("propertyId");
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

            // WHERE clause placeholders (5)
            // 4: host_id from primary key
            statement.setObject(i++, UUID.fromString(hostId));
            // 5: instance_app_id from primary key
            statement.setObject(i++, UUID.fromString(instanceAppId));
            // 6: instance_api_id from primary key
            statement.setObject(i++, UUID.fromString(instanceApiId));
            // 7: property_id from primary key
            statement.setObject(i++, UUID.fromString(propertyId));
            // 8: aggregate_version < ? for OCC/Idempotency check
            statement.setLong(i++, newAggregateVersion);

            int count = statement.executeUpdate();
            if (count == 0) {
                // If 0 rows were updated, we log a warning and continue.
                // This is the expected behavior for an idempotent operation.
                logger.warn("Soft delete skipped for ConfigInstanceAppApi with hostId {}, instanceAppId {}, instanceApiId {}, propertyId {}, newAggregateVersion {}. Record not found or a newer version already exists.",
                        hostId, instanceAppId, instanceApiId, propertyId, newAggregateVersion);
            }
        } catch (SQLException e) {
            logger.error("SQLException during soft delete of ConfigInstanceAppApi for hostId {} instanceAppId {} instanceApiId {} propertyId {}: {}",
                    hostId, instanceAppId, instanceApiId, propertyId, e.getMessage(), e);
            throw e;
        } catch (Exception e) {
            logger.error("Exception during soft delete of ConfigInstanceAppApi for hostId {} instanceAppId {} instanceApiId {} propertyId {}: {}",
                    hostId, instanceAppId, instanceApiId, propertyId, e.getMessage(), e);
            throw e;
        }
    }

    @Override
    public Result<String> getConfigInstanceAppApi(int offset, int limit, String filtersJson, String globalFilter, String sortingJson, String hostId) {
        Result<String> result = null;
        final Map<String, String> columnMap = new HashMap<>(Map.of(
                "hostId", "iap.host_id",
                "instanceAppId", "iap.instance_app_id",
                "instanceApiId", "iaap.instance_api_id",
                "instanceId", "i.instance_id",
                "instanceName", "i.instance_name",
                "appId", "ia.app_id",
                "appVersion", "ia.app_version",
                "apiVersionId", "iai.api_version_id",
                "apiId", "av.api_id",
                "apiVersion", "av.api_version"
                ));
        columnMap.put("configId", "p.config_id");
        columnMap.put("configName", "c.config_name");
        columnMap.put("propertyId", "iap.property_id");
        columnMap.put("propertyName", "p.property_name");
        columnMap.put("propertyValue", "iap.property_value");
        columnMap.put("required", "p.required");
        columnMap.put("propertyDesc", "p.property_desc");
        columnMap.put("propertyType", "p.property_type");
        columnMap.put("resourceType", "p.resource_type");
        columnMap.put("valueType", "p.value_type");
        columnMap.put("configType", "c.config_type");
        columnMap.put("configDesc", "c.config_desc");
        columnMap.put("classPath", "c.class_path");
        columnMap.put("updateUser", "ia.update_user");
        columnMap.put("updateTs", "ia.update_ts");
        columnMap.put("aggregateVersion", "iap.aggregate_version");

        List<Map<String, Object>> filters = parseJsonList(filtersJson);
        List<Map<String, Object>> sorting = parseJsonList(sortingJson);

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
                WHERE iaap.host_id = ?
                """;

        List<Object> parameters = new ArrayList<>();
        parameters.add(UUID.fromString(hostId));

        String[] searchColumns = {"i.instance_name", "c.config_name", "p.property_name", "p.property_desc", "c.config_desc"};
        String sqlBuilder = s + dynamicFilter(Arrays.asList("iaap.host_id", "iaap.instance_app_id", "iaap.instance_api_id", "ia.instance_id", "iai.api_version_id", "p.config_id", "iaap.property_id"), Arrays.asList(searchColumns), filters, columnMap, parameters) +
                globalFilter(globalFilter, searchColumns, parameters) +
                dynamicSorting("iaap.host_id, i.instance_id, iap.app_id, iap.app_version, av.api_id, av.api_version, p.config_id, p.property_name", sorting, columnMap) +
                // Pagination
                "\nLIMIT ? OFFSET ?";

        parameters.add(limit);
        parameters.add(offset);

        int total = 0;
        List<Map<String, Object>> instanceAppApis = new ArrayList<>();

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
    public Result<String> getConfigInstanceAppApiById(String hostId, String instanceAppId, String instanceApiId, String propertyId) {
        final String sql =
                """
                SELECT host_id, instance_app_id, instance_api_id, property_id, property_value,
                aggregate_version, active, update_user, update_ts
                FROM instance_app_api_property_t
                WHERE host_id = ? AND instance_app_id = ? AND instance_api_id = ? AND property_id = ?
                """;
        Result<String> result;
        Map<String, Object> map = new HashMap<>();

        String searchId = hostId + ":" + instanceAppId + ":" + instanceApiId + ":" + propertyId;

        try (Connection conn = ds.getConnection();
             PreparedStatement statement = conn.prepareStatement(sql)) {

            statement.setObject(1, UUID.fromString(hostId));
            statement.setObject(2, UUID.fromString(instanceAppId));
            statement.setObject(3, UUID.fromString(instanceApiId));
            statement.setObject(4, UUID.fromString(propertyId));

            try (ResultSet resultSet = statement.executeQuery()) {
                if (resultSet.next()) {
                    map.put("hostId", resultSet.getObject("host_id", UUID.class));
                    map.put("instanceAppId", resultSet.getObject("instance_app_id", UUID.class));
                    map.put("instanceApiId", resultSet.getObject("instance_api_id", UUID.class));
                    map.put("propertyId", resultSet.getObject("property_id", UUID.class));
                    map.put("propertyValue", resultSet.getString("property_value"));
                    map.put("aggregateVersion", resultSet.getLong("aggregate_version"));
                    map.put("active", resultSet.getBoolean("active"));
                    map.put("updateUser", resultSet.getString("update_user"));
                    map.put("updateTs", resultSet.getObject("update_ts") != null ? resultSet.getObject("update_ts", OffsetDateTime.class) : null);

                    result = Success.of(JsonMapper.toJson(map));
                } else {
                    result = Failure.of(new Status(OBJECT_NOT_FOUND, "instance_app_api_property", searchId));
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

    /**
     * Creates or reactivates an instance_property_t record using an idempotent UPSERT pattern.
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
    public void createConfigInstance(Connection conn, Map<String, Object> event) throws SQLException, Exception {
        final String sql =
                """
                INSERT INTO instance_property_t (
                    host_id,
                    instance_id,
                    property_id,
                    property_value,
                    update_user,
                    update_ts,
                    aggregate_version,
                    active
                ) VALUES (?, ?, ?, ?, ?, ?, ?, TRUE)
                ON CONFLICT (host_id, instance_id, property_id) DO UPDATE
                SET property_value = EXCLUDED.property_value,
                    update_user = EXCLUDED.update_user,
                    update_ts = EXCLUDED.update_ts,
                    aggregate_version = EXCLUDED.aggregate_version,
                    active = TRUE
                WHERE instance_property_t.aggregate_version < EXCLUDED.aggregate_version
                """;

        Map<String, Object> map = SqlUtil.extractEventData(event);
        String hostId = (String) event.get(Constants.HOST);
        String instanceId = (String) map.get("instanceId");
        String propertyId = (String) map.get("propertyId");
        long newAggregateVersion = SqlUtil.getNewAggregateVersion(event);

        try (PreparedStatement statement = conn.prepareStatement(sql)) {
            // Set parameters for the INSERT part of the statement
            int i = 1;
            // Primary Key fields
            statement.setObject(i++, UUID.fromString(hostId));
            statement.setObject(i++, UUID.fromString(instanceId));
            statement.setObject(i++, UUID.fromString(propertyId));

            // Mutable field
            statement.setString(i++, (String) map.get("propertyValue"));

            // IDM and OCC fields
            statement.setString(i++, (String) event.get(Constants.USER));
            statement.setObject(i++, OffsetDateTime.parse((String) event.get(CloudEventV1.TIME)));
            statement.setLong(i++, newAggregateVersion);

            int count = statement.executeUpdate();

            if (count == 0) {
                // A count of 0 is a valid, idempotent outcome.
                logger.warn("Creation/Reactivation skipped for ConfigInstance with hostId {}, instanceId {}, propertyId {}. A newer or same version already exists.",
                        hostId, instanceId, propertyId);
            }
        } catch (SQLException e) {
            logger.error("SQLException during createConfigInstance for hostId {} instanceId {} propertyId {}: {}",
                    hostId, instanceId, propertyId, e.getMessage(), e);
            throw e;
        } catch (Exception e) {
            logger.error("Exception during createConfigInstance for hostId {} instanceId {} propertyId {}: {}",
                    hostId, instanceId, propertyId, e.getMessage(), e);
            throw e;
        }
    }

    /**
     * Updates an instance_property_t record using an idempotent/monotonic pattern.
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
    public void updateConfigInstance(Connection conn, Map<String, Object> event) throws SQLException, Exception {
        // SQL statement updated to match the idempotent update pattern.
        final String sql =
                """
                UPDATE instance_property_t
                SET property_value = ?,
                    update_user = ?,
                    update_ts = ?,
                    aggregate_version = ?,
                    active = TRUE
                WHERE host_id = ?
                  AND instance_id = ?
                  AND property_id = ?
                  AND aggregate_version < ?
                """;

        Map<String, Object> map = SqlUtil.extractEventData(event);
        String hostId = (String) event.get(Constants.HOST);
        String instanceId = (String) map.get("instanceId");
        String propertyId = (String) map.get("propertyId");
        long newAggregateVersion = SqlUtil.getNewAggregateVersion(event);

        try (PreparedStatement statement = conn.prepareStatement(sql)) {
            // SET clause placeholders (4)
            int i = 1;
            statement.setString(i++, (String) map.get("propertyValue"));

            // IDM and OCC fields
            statement.setString(i++, (String) event.get(Constants.USER));
            statement.setObject(i++, OffsetDateTime.parse((String) event.get(CloudEventV1.TIME)));
            statement.setLong(i++, newAggregateVersion);

            // WHERE clause placeholders (4)
            statement.setObject(i++, UUID.fromString(hostId));
            statement.setObject(i++, UUID.fromString(instanceId));
            statement.setObject(i++, UUID.fromString(propertyId));
            statement.setLong(i++, newAggregateVersion);

            int count = statement.executeUpdate();

            if (count == 0) {
                // If 0 rows were updated, it's a valid idempotent outcome.
                logger.warn("Update skipped for ConfigInstance with hostId {}, instanceId {}, propertyId {}. Record not found or a newer version already exists.",
                        hostId, instanceId, propertyId);
            }
        } catch (SQLException e) {
            logger.error("SQLException during updateConfigInstance for hostId {} instanceId {} propertyId {}: {}",
                    hostId, instanceId, propertyId, e.getMessage(), e);
            throw e;
        } catch (Exception e) {
            logger.error("Exception during updateConfigInstance for hostId {} instanceId {} propertyId {}: {}",
                    hostId, instanceId, propertyId, e.getMessage(), e);
            throw e;
        }
    }

    /**
     * Performs a soft delete on an instance_property_t record using an idempotent/monotonic pattern.
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
    public void deleteConfigInstance(Connection conn, Map<String, Object> event) throws SQLException, Exception {
        // SQL statement updated to match the idempotent soft-delete pattern.
        final String sql =
                """
                UPDATE instance_property_t
                SET active = FALSE,
                    update_user = ?,
                    update_ts = ?,
                    aggregate_version = ?
                WHERE host_id = ?
                  AND instance_id = ?
                  AND property_id = ?
                  AND aggregate_version < ?
                """;

        // Extract data and metadata from the event.
        Map<String, Object> map = SqlUtil.extractEventData(event);
        String hostId = (String) event.get(Constants.HOST);
        String instanceId = (String) map.get("instanceId");
        String propertyId = (String) map.get("propertyId");
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
            // 5: instance_id from primary key
            statement.setObject(i++, UUID.fromString(instanceId));
            // 6: property_id from primary key
            statement.setObject(i++, UUID.fromString(propertyId));
            // 7: aggregate_version < ? for OCC/Idempotency check
            statement.setLong(i++, newAggregateVersion);

            int count = statement.executeUpdate();
            if (count == 0) {
                // If 0 rows were updated, we log a warning and continue.
                // This is the expected behavior for an idempotent operation.
                logger.warn("Soft delete skipped for ConfigInstance with hostId {}, instanceId {}, propertyId {}, newAggregateVersion {}. Record not found or a newer version already exists.",
                        hostId, instanceId, propertyId, newAggregateVersion);
            }
        } catch (SQLException e) {
            logger.error("SQLException during soft delete of ConfigInstance for hostId {} instanceId {} propertyId {}: {}",
                    hostId, instanceId, propertyId, e.getMessage(), e);
            throw e;
        } catch (Exception e) {
            logger.error("Exception during soft delete of ConfigInstance for hostId {} instanceId {} propertyId {}: {}",
                    hostId, instanceId, propertyId, e.getMessage(), e);
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
    public Result<String> getConfigInstance(int offset, int limit, String filtersJson, String globalFilter, String sortingJson, String hostId) {
        Result<String> result = null;
        final Map<String, String> columnMap = new HashMap<>(Map.of(
                "hostId", "ip.host_id",
                "instanceId", "ip.instance_id",
                "instanceName", "i.instance_name",
                "configId", "p.config_id",
                "configName", "c.config_name",
                "propertyId", "ip.property_id",
                "propertyName", "ip.property_name",
                "propertyValue", "ip.property_value",
                "required", "p.required",
                "propertyDesc", "p.property_desc"
        ));
        columnMap.put("propertyType", "p.property_type");
        columnMap.put("resourceType", "p.resource_type");
        columnMap.put("valueType", "p.value_type");
        columnMap.put("configType", "c.config_type");
        columnMap.put("configDesc", "c.config_desc");
        columnMap.put("classPath", "c.class_path");
        columnMap.put("updateUser", "ip.update_user");
        columnMap.put("updateTs", "ip.update_ts");
        columnMap.put("aggregateVersion", "ip.aggregate_version");

        List<Map<String, Object>> filters = parseJsonList(filtersJson);
        List<Map<String, Object>> sorting = parseJsonList(sortingJson);

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
                WHERE ip.host_id = ?
                """;
        List<Object> parameters = new ArrayList<>();
        parameters.add(UUID.fromString(hostId));


        String[] searchColumns = {"i.instance_name", "c.config_name", "p.property_name", "p.property_desc", "c.config_desc"};
        String sqlBuilder = s + dynamicFilter(Arrays.asList("ip.host_id", "ip.instance_id", "p.config_id", "ip.property_id"), Arrays.asList(searchColumns), filters, columnMap, parameters) +
                globalFilter(globalFilter, searchColumns, parameters) +
                dynamicSorting("ip.host_id, ip.instance_id, p.config_id, p.display_order", sorting, columnMap) +
                "\nLIMIT ? OFFSET ?";

        parameters.add(limit);
        parameters.add(offset);

        int total = 0;
        List<Map<String, Object>> instanceProperties = new ArrayList<>();

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
    public Result<String> getConfigInstanceById(String hostId, String instanceId, String propertyId) {
        final String sql =
                """
                SELECT host_id, instance_id, property_id, property_value,
                aggregate_version, active, update_user, update_ts
                FROM instance_property_t
                WHERE host_id = ? AND instance_id = ? AND property_id = ?
                """;
        Result<String> result;
        Map<String, Object> map = new HashMap<>();

        String searchId = hostId + ":" + instanceId + ":" + propertyId;

        try (Connection conn = ds.getConnection();
             PreparedStatement statement = conn.prepareStatement(sql)) {

            // Set WHERE clause parameters
            statement.setObject(1, UUID.fromString(hostId));
            statement.setObject(2, UUID.fromString(instanceId));
            statement.setObject(3, UUID.fromString(propertyId));

            try (ResultSet resultSet = statement.executeQuery()) {
                if (resultSet.next()) {
                    map.put("hostId", resultSet.getObject("host_id", UUID.class));
                    map.put("instanceId", resultSet.getObject("instance_id", UUID.class));
                    map.put("propertyId", resultSet.getObject("property_id", UUID.class));
                    map.put("propertyValue", resultSet.getString("property_value"));
                    map.put("aggregateVersion", resultSet.getLong("aggregate_version"));
                    map.put("active", resultSet.getBoolean("active"));
                    map.put("updateUser", resultSet.getString("update_user"));
                    map.put("updateTs", resultSet.getObject("update_ts") != null ? resultSet.getObject("update_ts", OffsetDateTime.class) : null);

                    result = Success.of(JsonMapper.toJson(map));
                } else {
                    result = Failure.of(new Status(OBJECT_NOT_FOUND, "instance_property", searchId));
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

    /**
     * Creates or reactivates an instance_file_t record using an idempotent UPSERT pattern.
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
    public void createConfigInstanceFile(Connection conn, Map<String, Object> event) throws SQLException, Exception {
        final String sql =
                """
                INSERT INTO instance_file_t (
                    host_id,
                    instance_file_id,
                    instance_id,
                    file_type,
                    file_name,
                    file_value,
                    file_desc,
                    expiration_ts,
                    update_user,
                    update_ts,
                    aggregate_version,
                    active
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, TRUE)
                ON CONFLICT (host_id, instance_file_id) DO UPDATE
                SET instance_id = EXCLUDED.instance_id,
                    file_type = EXCLUDED.file_type,
                    file_name = EXCLUDED.file_name,
                    file_value = EXCLUDED.file_value,
                    file_desc = EXCLUDED.file_desc,
                    expiration_ts = EXCLUDED.expiration_ts,
                    update_user = EXCLUDED.update_user,
                    update_ts = EXCLUDED.update_ts,
                    aggregate_version = EXCLUDED.aggregate_version,
                    active = TRUE
                WHERE instance_file_t.aggregate_version < EXCLUDED.aggregate_version
                """;

        Map<String, Object> map = SqlUtil.extractEventData(event);
        String hostId = (String) event.get(Constants.HOST);
        String instanceFileId = (String) map.get("instanceFileId");
        long newAggregateVersion = SqlUtil.getNewAggregateVersion(event);

        try (PreparedStatement statement = conn.prepareStatement(sql)) {
            // Set parameters for the INSERT part of the statement
            int i = 1;
            statement.setObject(i++, UUID.fromString(hostId));
            statement.setObject(i++, UUID.fromString(instanceFileId));
            statement.setObject(i++, UUID.fromString((String) map.get("instanceId")));
            statement.setString(i++, (String) map.get("fileType"));
            statement.setString(i++, (String) map.get("fileName"));
            statement.setString(i++, (String) map.get("fileValue"));
            statement.setString(i++, (String) map.get("fileDesc"));

            // Handle optional expiration_ts
            if (map.get("expirationTs") != null) {
                statement.setObject(i++, OffsetDateTime.parse((String) map.get("expirationTs")));
            } else {
                statement.setNull(i++, Types.TIMESTAMP_WITH_TIMEZONE);
            }

            // IDM and OCC fields
            statement.setString(i++, (String) event.get(Constants.USER));
            statement.setObject(i++, OffsetDateTime.parse((String) event.get(CloudEventV1.TIME)));
            statement.setLong(i++, newAggregateVersion);

            int count = statement.executeUpdate();

            if (count == 0) {
                // A count of 0 is a valid, idempotent outcome.
                logger.warn("Creation/Reactivation skipped for ConfigInstanceFile with hostId {}, instanceFileId {}. A newer or same version already exists.",
                        hostId, instanceFileId);
            }
        } catch (SQLException e) {
            logger.error("SQLException during createConfigInstanceFile for hostId {} instanceFileId {}: {}",
                    hostId, instanceFileId, e.getMessage(), e);
            throw e;
        } catch (Exception e) {
            logger.error("Exception during createConfigInstanceFile for hostId {} instanceFileId {}: {}",
                    hostId, instanceFileId, e.getMessage(), e);
            throw e;
        }
    }

    @Override
    public void updateConfigInstanceFile(Connection conn, Map<String, Object> event) throws SQLException, Exception {
        // We attempt to update the record IF the incoming event's aggregate_version is greater than the current projection's version.
        // This enforces Idempotence (IDM) and Optimistic Concurrency Control (OCC) by ensuring version monotonicity.
        // We explicitly set active = TRUE as an UPDATE event implies the file should be active.
        final String sql =
                """
                UPDATE instance_file_t
                SET file_type = ?,
                    file_name = ?,
                    file_value = ?,
                    file_desc = ?,
                    expiration_ts = ?,
                    update_user = ?,
                    update_ts = ?,
                    aggregate_version = ?,
                    active = TRUE
                WHERE host_id = ?
                  AND instance_file_id = ?
                  AND aggregate_version < ?
                """; // <<< CRITICAL: Changed aggregate_version = ? to aggregate_version < ? to enforce monotonicity (OCC/IDM)

        // Note: The original code retrieves hostId from event metadata/constants,
        // while the data map is used for instanceFileId.
        Map<String, Object> map = SqlUtil.extractEventData(event);
        String hostId = (String)map.get("hostId"); // Getting hostId from map data (or event metadata, adjusted for safety)
        String instanceFileId = (String)map.get("instanceFileId");
        long oldAggregateVersion = SqlUtil.getOldAggregateVersion(event);
        long newAggregateVersion = SqlUtil.getNewAggregateVersion(event);

        try (PreparedStatement statement = conn.prepareStatement(sql)) {
            // SET values (8 dynamic values + active = TRUE in SQL)
            int i = 1;
            // 1. file_type
            String fileType = (String)map.get("fileType");
            if (fileType != null) {
                statement.setString(i++, fileType);
            } else {
                statement.setNull(i++, Types.VARCHAR);
            }
            // 2. file_name
            String fileName = (String)map.get("fileName");
            if (fileName != null) {
                statement.setString(i++, fileName);
            } else {
                statement.setNull(i++, Types.VARCHAR);
            }
            // 3. file_value
            String fileValue = (String)map.get("fileValue");
            if (fileValue != null) {
                statement.setString(i++, fileValue);
            } else {
                statement.setNull(i++, Types.VARCHAR);
            }
            // 4. file_desc
            String fileDesc = (String)map.get("fileDesc");
            if (fileDesc != null) {
                statement.setString(i++, fileDesc);
            } else {
                statement.setNull(i++, Types.VARCHAR);
            }
            // 5. expiration_ts
            String expirationTs = (String)map.get("expirationTs");
            if (expirationTs != null) {
                statement.setObject(i++, OffsetDateTime.parse(expirationTs));
            } else {
                statement.setNull(i++, Types.TIMESTAMP_WITH_TIMEZONE); // Use TIMESTAMP_WITH_TIMEZONE
            }

            // 6. update_user
            statement.setString(i++, (String)event.get(Constants.USER));
            // 7. update_ts
            statement.setObject(i++, OffsetDateTime.parse((String)event.get(CloudEventV1.TIME)));
            // 8. aggregate_version
            statement.setLong(i++, newAggregateVersion);

            // WHERE conditions (3 placeholders)
            // 9. host_id
            statement.setObject(i++, UUID.fromString(hostId));
            // 10. instance_file_id
            statement.setObject(i++, UUID.fromString(instanceFileId));
            // 11. aggregate_version < ? (new version for OCC/IDM check)
            statement.setLong(i++, newAggregateVersion);

            int count = statement.executeUpdate();
            if (count == 0) {
                // If 0 rows updated, it means the record was either not found
                // OR aggregate_version >= newAggregateVersion (OCC/IDM check failed).
                // We IGNORE the failure and log a warning, as this is the desired idempotent/monotonic behavior.
                logger.warn("Update skipped for hostId {} instanceFileId {} (old: {}) -> (new: {}). Record not found or a newer/same version already exists.", hostId, instanceFileId, oldAggregateVersion, newAggregateVersion);
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
        // Use UPDATE to implement Soft Delete (setting active = FALSE).
        // OCC/IDM is enforced by checking aggregate_version < newAggregateVersion.
        final String sql =
                """
                UPDATE instance_file_t
                SET active = FALSE,
                    update_user = ?,
                    update_ts = ?,
                    aggregate_version = ?
                WHERE host_id = ?
                  AND instance_file_id = ?
                  AND aggregate_version < ?
                """; // <<< CRITICAL: Added aggregate_version < ? to enforce monotonicity (OCC/IDM)

        // Note: The original code retrieves hostId from event metadata/constants,
        // while the data map is used for instanceFileId.
        Map<String, Object> map = SqlUtil.extractEventData(event);
        String hostId = (String)event.get(Constants.HOST);
        String instanceFileId = (String)map.get("instanceFileId");
        // A delete event represents a state change, so it should have a new, incremented version.
        long newAggregateVersion = SqlUtil.getNewAggregateVersion(event);
        // oldAggregateVersion is kept for logging context from the original method.
        long oldAggregateVersion = SqlUtil.getOldAggregateVersion(event);

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
            // 5: instance_file_id
            statement.setObject(5, UUID.fromString(instanceFileId));
            // 6: aggregate_version < ? (new version for OCC/IDM check)
            statement.setLong(6, newAggregateVersion);

            int count = statement.executeUpdate();
            if (count == 0) {
                // If 0 rows updated, it means:
                // 1. The record was not found (already deleted or never existed).
                // 2. The OCC/IDM check failed (aggregate_version >= newAggregateVersion).
                // We IGNORE the failure and log a warning, as this is the desired idempotent/monotonic behavior.
                logger.warn("Soft delete skipped for hostId {} instanceFileId {} (old: {}) -> (new: {}). Record not found or a newer/same version already exists.", hostId, instanceFileId, oldAggregateVersion, newAggregateVersion);
            }
        } catch (SQLException e) {
            logger.error("SQLException during deleteConfigInstanceFile for hostId {} instanceFileId {} (old: {}) -> (new: {}): {}", hostId, instanceFileId, oldAggregateVersion, newAggregateVersion, e.getMessage(), e);
            throw e;
        } catch (Exception e) {
            logger.error("Exception during deleteConfigInstanceFile for hostId {} instanceFileId {} (old: {}) -> (new: {}): {}", hostId, instanceFileId, oldAggregateVersion, newAggregateVersion, e.getMessage(), e);
            throw e;
        }
    }

    @Override
    public Result<String> getConfigInstanceFile(int offset, int limit, String filtersJson, String globalFilter, String sortingJson, String hostId) {
        Result<String> result = null;
        final Map<String, String> columnMap = new HashMap<>(Map.of(
                "hostId", "ift.host_id",
                "instanceFileId", "ift.instance_file_id",
                "instanceId", "ift.instance_id",
                "instanceName", "i.instance_name",
                "fileType", "ift.file_type",
                "fileName", "ift.file_name",
                "fileValue", "ift.file_value",
                "fileDesc", "ift.file_desc",
                "expirationTs", "ift.expiration_ts",
                "updateUser", "ift.update_user"
        ));
        columnMap.put("updateTs", "ift.update_ts");
        columnMap.put("aggregateVersion", "ift.aggregate_version");

        List<Map<String, Object>> filters = parseJsonList(filtersJson);
        List<Map<String, Object>> sorting = parseJsonList(sortingJson);

        String s =
                """
                SELECT COUNT(*) OVER () AS total,
                ift.host_id, ift.instance_file_id, ift.instance_id, i.instance_name,
                ift.file_type, ift.file_name, ift.file_value, ift.file_desc,
                ift.expiration_ts, ift.update_user, ift.update_ts, ift.aggregate_version
                FROM instance_file_t ift
                INNER JOIN instance_t i ON i.instance_id = ift.instance_id
                WHERE ift.host_id = ?
                """;

        List<Object> parameters = new ArrayList<>();
        parameters.add(UUID.fromString(hostId));

        String[] searchColumns = {"i.instance_name", "ift.file_name", "ift.file_desc"};
        String sqlBuilder = s + dynamicFilter(Arrays.asList("ift.host_id", "ift.instance_file_id", "ift.instance_id"), Arrays.asList(searchColumns), filters, columnMap, parameters) +
                globalFilter(globalFilter, searchColumns, parameters) +
                dynamicSorting("ift.host_id, ift.instance_file_id, ift.instance_id", sorting, columnMap) +
                "\nLIMIT ? OFFSET ?";

        parameters.add(limit);
        parameters.add(offset);

        int total = 0;
        List<Map<String, Object>> instanceFiles = new ArrayList<>();

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
    public Result<String> getConfigInstanceFileById(String hostId, String instanceFileId) {
        final String sql =
                """
                SELECT host_id, instance_file_id, instance_id, file_type, file_name, file_value,
                file_desc, expiration_ts, aggregate_version, active, update_user, update_ts
                FROM instance_file_t
                WHERE host_id = ? AND instance_file_id = ?
                """;
        Result<String> result;
        Map<String, Object> map = new HashMap<>();

        String searchId = hostId + ":" + instanceFileId;

        try (Connection conn = ds.getConnection();
             PreparedStatement statement = conn.prepareStatement(sql)) {

            statement.setObject(1, UUID.fromString(hostId));
            statement.setObject(2, UUID.fromString(instanceFileId));

            try (ResultSet resultSet = statement.executeQuery()) {
                if (resultSet.next()) {
                    map.put("hostId", resultSet.getObject("host_id", UUID.class));
                    map.put("instanceFileId", resultSet.getObject("instance_file_id", UUID.class));
                    map.put("instanceId", resultSet.getObject("instance_id", UUID.class));
                    map.put("fileType", resultSet.getString("file_type"));
                    map.put("fileName", resultSet.getString("file_name"));
                    map.put("fileValue", resultSet.getString("file_value"));
                    map.put("fileDesc", resultSet.getString("file_desc"));
                    map.put("expirationTs", resultSet.getObject("expiration_ts") != null ? resultSet.getObject("expiration_ts", OffsetDateTime.class) : null);
                    map.put("aggregateVersion", resultSet.getLong("aggregate_version"));
                    map.put("active", resultSet.getBoolean("active"));
                    map.put("updateUser", resultSet.getString("update_user"));
                    map.put("updateTs", resultSet.getObject("update_ts") != null ? resultSet.getObject("update_ts", OffsetDateTime.class) : null);

                    result = Success.of(JsonMapper.toJson(map));
                } else {
                    result = Failure.of(new Status(OBJECT_NOT_FOUND, "instance_file", searchId));
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
    public void createConfigDeploymentInstance(Connection conn, Map<String, Object> event) throws SQLException, Exception {
        // Use UPSERT based on the Primary Key (host_id, deployment_instance_id, property_id): INSERT ON CONFLICT DO UPDATE
        // This handles:
        // 1. First time insert (no conflict).
        // 2. Re-creation (conflict on PK) -> UPDATE the existing soft-deleted row (setting active=TRUE and new version).

        final String sql =
                """
                INSERT INTO deployment_instance_property_t(
                    host_id,
                    deployment_instance_id,
                    property_id,
                    property_value,
                    update_user,
                    update_ts,
                    aggregate_version,
                    active
                ) VALUES (?, ?, ?, ?, ?, ?, ?, TRUE)
                ON CONFLICT (host_id, deployment_instance_id, property_id) DO UPDATE
                SET property_value = EXCLUDED.property_value,
                    update_user = EXCLUDED.update_user,
                    update_ts = EXCLUDED.update_ts,
                    aggregate_version = EXCLUDED.aggregate_version,
                    active = TRUE
                -- OCC/IDM: Only update if the incoming event is newer
                WHERE deployment_instance_property_t.aggregate_version < EXCLUDED.aggregate_version
                """;

        // Note: The original code retrieves hostId from event metadata/constants,
        // while the data map is used for deploymentInstanceId and propertyId.
        Map<String, Object> map = SqlUtil.extractEventData(event);
        String hostId = (String)map.get("hostId"); // Assuming hostId is in the data map, consistent with other methods
        String deploymentInstanceId = (String)map.get("deploymentInstanceId");
        String propertyId = (String)map.get("propertyId");
        long newAggregateVersion = SqlUtil.getNewAggregateVersion(event);

        try (PreparedStatement statement = conn.prepareStatement(sql)) {
            // INSERT values (7 placeholders + active=TRUE in SQL, total 7 dynamic values)
            int i = 1;
            // 1. host_id (Required, part of PK)
            statement.setObject(i++, UUID.fromString(hostId));
            // 2. deployment_instance_id (Required, part of PK)
            statement.setObject(i++, UUID.fromString(deploymentInstanceId));
            // 3. property_id (Required, part of PK)
            statement.setObject(i++, UUID.fromString(propertyId));

            // 4. property_value
            String propertyValue = (String) map.get("propertyValue");
            if(propertyValue != null && !propertyValue.isEmpty()) {
                statement.setString(i++, propertyValue);
            } else {
                statement.setNull(i++, Types.VARCHAR); // Using TEXT/VARCHAR
            }

            // 5. update_user
            statement.setString(i++, (String)event.get(Constants.USER));
            // 6. update_ts
            statement.setObject(i++, OffsetDateTime.parse((String)event.get(CloudEventV1.TIME)));
            // 7. aggregate_version
            statement.setLong(i++, newAggregateVersion);

            int count = statement.executeUpdate();
            if (count == 0) {
                // count=0 means the ON CONFLICT clause was hit, BUT the WHERE clause (aggregate_version < EXCLUDED.aggregate_version) failed.
                // This is the desired idempotent/out-of-order protection behavior. Log and ignore.
                logger.warn("Creation/Reactivation skipped for hostId {} deploymentInstanceId {} propertyId {} aggregateVersion {}. A newer or same version already exists.", hostId, deploymentInstanceId, propertyId, newAggregateVersion);
            }
        } catch (SQLException e) {
            logger.error("SQLException during createConfigDeploymentInstance for hostId {} deploymentInstanceId {} propertyId {} aggregateVersion {}: {}", hostId, deploymentInstanceId, propertyId, newAggregateVersion, e.getMessage(), e);
            throw e;
        } catch (Exception e) {
            logger.error("Exception during createConfigDeploymentInstance for hostId {} deploymentInstanceId {} propertyId {} aggregateVersion {}: {}", hostId, deploymentInstanceId, propertyId, newAggregateVersion, e.getMessage(), e);
            throw e;
        }
    }

    @Override
    public void updateConfigDeploymentInstance(Connection conn, Map<String, Object> event) throws SQLException, Exception {
        // We attempt to update the record IF the incoming event's aggregate_version is greater than the current projection's version.
        // This enforces Idempotence (IDM) and Optimistic Concurrency Control (OCC) by ensuring version monotonicity.
        // We explicitly set active = TRUE as an UPDATE event implies the property should be active.
        final String sql =
                """
                UPDATE deployment_instance_property_t
                SET property_value = ?,
                    update_user = ?,
                    update_ts = ?,
                    aggregate_version = ?,
                    active = TRUE
                WHERE host_id = ?
                  AND deployment_instance_id = ?
                  AND property_id = ?
                  AND aggregate_version < ?
                """; // <<< CRITICAL: Changed aggregate_version = ? to aggregate_version < ? to enforce monotonicity (OCC/IDM)

        // Note: The original code retrieves hostId from event metadata/constants,
        // while the data map is used for deploymentInstanceId and propertyId.
        Map<String, Object> map = SqlUtil.extractEventData(event);
        String hostId = (String)map.get("hostId"); // Assuming hostId is in the data map, consistent with other methods
        String deploymentInstanceId = (String)map.get("deploymentInstanceId");
        String propertyId = (String)map.get("propertyId");
        long oldAggregateVersion = SqlUtil.getOldAggregateVersion(event);
        long newAggregateVersion = SqlUtil.getNewAggregateVersion(event);

        try (PreparedStatement statement = conn.prepareStatement(sql)) {
            // SET values (4 dynamic values + active = TRUE in SQL)
            int i = 1;
            // 1. property_value
            String propertyValue = (String)map.get("propertyValue");
            if (propertyValue != null && !propertyValue.isEmpty()) {
                statement.setString(i++, propertyValue);
            } else {
                statement.setNull(i++, Types.VARCHAR); // Using TEXT/VARCHAR
            }
            // 2. update_user
            statement.setString(i++, (String)event.get(Constants.USER));
            // 3. update_ts
            statement.setObject(i++, OffsetDateTime.parse((String)event.get(CloudEventV1.TIME)));
            // 4. aggregate_version
            statement.setLong(i++, newAggregateVersion);

            // WHERE conditions (4 placeholders)
            // 5. host_id
            statement.setObject(i++, UUID.fromString(hostId));
            // 6. deployment_instance_id
            statement.setObject(i++, UUID.fromString(deploymentInstanceId));
            // 7. property_id
            statement.setObject(i++, UUID.fromString(propertyId));
            // 8. aggregate_version < ? (new version for OCC/IDM check)
            statement.setLong(i++, newAggregateVersion);

            int count = statement.executeUpdate();
            if (count == 0) {
                // If 0 rows updated, it means the record was either not found
                // OR aggregate_version >= newAggregateVersion (OCC/IDM check failed).
                // We IGNORE the failure and log a warning, as this is the desired idempotent/monotonic behavior.
                logger.warn("Update skipped for hostId {} deploymentInstanceId {} propertyId {} (old: {}) -> (new: {}). Record not found or a newer/same version already exists.", hostId, deploymentInstanceId, propertyId, oldAggregateVersion, newAggregateVersion);
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
        // Use UPDATE to implement Soft Delete (setting active = FALSE).
        // OCC/IDM is enforced by checking aggregate_version < newAggregateVersion.
        final String sql =
                """
                UPDATE deployment_instance_property_t
                SET active = FALSE,
                    update_user = ?,
                    update_ts = ?,
                    aggregate_version = ?
                WHERE host_id = ?
                  AND deployment_instance_id = ?
                  AND property_id = ?
                  AND aggregate_version < ?
                """; // <<< CRITICAL: Added aggregate_version < ? to enforce monotonicity (OCC/IDM)

        // Note: The original code retrieves hostId from event metadata/constants,
        // while the data map is used for deploymentInstanceId and propertyId.
        Map<String, Object> map = SqlUtil.extractEventData(event);
        String hostId = (String)event.get(Constants.HOST);
        String deploymentInstanceId = (String)map.get("deploymentInstanceId");
        String propertyId = (String)map.get("propertyId");
        // A delete event represents a state change, so it should have a new, incremented version.
        long newAggregateVersion = SqlUtil.getNewAggregateVersion(event);
        // oldAggregateVersion is kept for logging context from the original method.
        long oldAggregateVersion = SqlUtil.getOldAggregateVersion(event);

        try (PreparedStatement statement = conn.prepareStatement(sql)) {
            // SET values (3 placeholders)
            // 1: update_user
            statement.setString(1, (String)event.get(Constants.USER));
            // 2: update_ts
            statement.setObject(2, OffsetDateTime.parse((String)event.get(CloudEventV1.TIME)));
            // 3: aggregate_version (the new version)
            statement.setLong(3, newAggregateVersion);

            // WHERE conditions (4 placeholders)
            // 4: host_id
            statement.setObject(4, UUID.fromString(hostId));
            // 5: deployment_instance_id
            statement.setObject(5, UUID.fromString(deploymentInstanceId));
            // 6: property_id
            statement.setObject(6, UUID.fromString(propertyId));
            // 7: aggregate_version < ? (new version for OCC/IDM check)
            statement.setLong(7, newAggregateVersion);

            int count = statement.executeUpdate();
            if (count == 0) {
                // If 0 rows updated, it means:
                // 1. The record was not found (already deleted or never existed).
                // 2. The OCC/IDM check failed (aggregate_version >= newAggregateVersion).
                // We IGNORE the failure and log a warning, as this is the desired idempotent/monotonic behavior.
                logger.warn("Soft delete skipped for hostId {} deploymentInstanceId {} propertyId {} (old: {}) -> (new: {}). Record not found or a newer/same version already exists.", hostId, deploymentInstanceId, propertyId, oldAggregateVersion, newAggregateVersion);
            }
        } catch (SQLException e) {
            logger.error("SQLException during deleteConfigDeploymentInstance for hostId {} deploymentInstanceId {} propertyId {} (old: {}) -> (new: {}): {}", hostId, deploymentInstanceId, propertyId, oldAggregateVersion, newAggregateVersion, e.getMessage(), e);
            throw e;
        } catch (Exception e) {
            logger.error("Exception during deleteConfigDeploymentInstance for hostId {} deploymentInstanceId {} propertyId {} (old: {}) -> (new: {}): {}", hostId, deploymentInstanceId, propertyId, oldAggregateVersion, newAggregateVersion, e.getMessage(), e);
            throw e;
        }
    }

    @Override
    public Result<String> getConfigDeploymentInstance(int offset, int limit, String filtersJson, String globalFilter, String sortingJson, String hostId) {
        Result<String> result = null;
        final Map<String, String> columnMap = new HashMap<>(Map.of(
                "hostId", "dip.host_id",
                "deploymentInstanceId", "dip.deployment_instance_id",
                "instanceId", "di.instance_id",
                "instanceName", "i.instance_name",
                "serviceId", "di.service_id",
                "ipAddress", "di.ip_address",
                "portNumber", "di.port_number",
                "configId", "cp.config_id",
                "configName", "c.config_name",
                "propertyId", "dip.property_id"
        ));
        columnMap.put("propertyName", "cp.property_name");
        columnMap.put("propertyValue", "dip.property_value");
        columnMap.put("updateUser", "dip.update_user");
        columnMap.put("updateTs", "dip.update_ts");
        columnMap.put("aggregateVersion", "dip.aggregate_version");

        List<Map<String, Object>> filters = parseJsonList(filtersJson);
        List<Map<String, Object>> sorting = parseJsonList(sortingJson);

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
                WHERE dip.host_id = ?
                """;

        List<Object> parameters = new ArrayList<>();
        parameters.add(UUID.fromString(hostId));

        String[] searchColumns = {"i.instance_name", "c.config_name", "cp.property_name"};
        String sqlBuilder = s + dynamicFilter(Arrays.asList("dip.host_id", "dip.deployment_instance_id", "di.instance_id", "cp.config_id", "dip.property_id"), Arrays.asList(searchColumns), filters, columnMap, parameters) +
                globalFilter(globalFilter, searchColumns, parameters) +
                dynamicSorting("dip.host_id, di.service_id", sorting, columnMap) +
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
    public Result<String> getConfigDeploymentInstanceById(String hostId, String deploymentInstanceId, String propertyId) {
        final String sql =
                """
                SELECT host_id, deployment_instance_id, property_id, property_value,
                aggregate_version, active, update_user, update_ts
                FROM deployment_instance_property_t
                WHERE host_id = ? AND deployment_instance_id = ? AND property_id = ?
                """;
        Result<String> result;
        Map<String, Object> map = new HashMap<>();

        String searchId = hostId + ":" + deploymentInstanceId + ":" + propertyId;

        try (Connection conn = ds.getConnection();
             PreparedStatement statement = conn.prepareStatement(sql)) {

            statement.setObject(1, UUID.fromString(hostId));
            statement.setObject(2, UUID.fromString(deploymentInstanceId));
            statement.setObject(3, UUID.fromString(propertyId));

            try (ResultSet resultSet = statement.executeQuery()) {
                if (resultSet.next()) {
                    map.put("hostId", resultSet.getObject("host_id", UUID.class));
                    map.put("deploymentInstanceId", resultSet.getObject("deployment_instance_id", UUID.class));
                    map.put("propertyId", resultSet.getObject("property_id", UUID.class));
                    map.put("propertyValue", resultSet.getString("property_value"));
                    map.put("aggregateVersion", resultSet.getLong("aggregate_version"));
                    map.put("active", resultSet.getBoolean("active"));
                    map.put("updateUser", resultSet.getString("update_user"));
                    map.put("updateTs", resultSet.getObject("update_ts") != null ? resultSet.getObject("update_ts", OffsetDateTime.class) : null);

                    result = Success.of(JsonMapper.toJson(map));
                } else {
                    result = Failure.of(new Status(OBJECT_NOT_FOUND, "deployment_instance_property", searchId));
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
    public void createConfigProduct(Connection conn, Map<String, Object> event) throws SQLException, Exception {
        // Use UPSERT based on the Primary Key (product_id, property_id): INSERT ON CONFLICT DO UPDATE
        // This handles:
        // 1. First time insert (no conflict).
        // 2. Re-creation (conflict on PK) -> UPDATE the existing soft-deleted row (setting active=TRUE and new version).

        final String sql =
                """
                INSERT INTO product_property_t (
                    product_id,
                    property_id,
                    property_value,
                    update_user,
                    update_ts,
                    aggregate_version,
                    active
                ) VALUES (?, ?, ?, ?, ?, ?, TRUE)
                ON CONFLICT (product_id, property_id) DO UPDATE
                SET property_value = EXCLUDED.property_value,
                    update_user = EXCLUDED.update_user,
                    update_ts = EXCLUDED.update_ts,
                    aggregate_version = EXCLUDED.aggregate_version,
                    active = TRUE
                -- OCC/IDM: Only update if the incoming event is newer
                WHERE product_property_t.aggregate_version < EXCLUDED.aggregate_version
                """;

        // Note: The original code uses a non-standard map retrieval: Map<String, Object> map = (Map<String, Object>)event.get(PortalConstants.DATA);
        // Assuming SqlUtil.extractEventData(event) is the correct utility based on other methods.
        Map<String, Object> map = SqlUtil.extractEventData(event);
        String productId = (String)map.get("productId");
        String propertyId = (String)map.get("propertyId");
        long newAggregateVersion = SqlUtil.getNewAggregateVersion(event);

        try (PreparedStatement statement = conn.prepareStatement(sql)) {
            // INSERT values (6 placeholders + active=TRUE in SQL, total 6 dynamic values)
            int i = 1;
            // 1. product_id (Required, part of PK)
            statement.setString(i++, productId);
            // 2. property_id (Required, part of PK)
            statement.setObject(i++, UUID.fromString(propertyId));

            // 3. property_value
            if (map.containsKey("propertyValue")) {
                statement.setString(i++, (String) map.get("propertyValue"));
            } else {
                statement.setNull(i++, Types.VARCHAR); // Using TEXT/VARCHAR
            }

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
                logger.warn("Creation/Reactivation skipped for productId {} propertyId {} aggregateVersion {}. A newer or same version already exists.", productId, propertyId, newAggregateVersion);
            }
        } catch (SQLException e) {
            logger.error("SQLException during createConfigProduct for productId {} propertyId {} aggregateVersion {}: {}", productId, propertyId, newAggregateVersion, e.getMessage(), e);
            throw e;
        } catch (Exception e) {
            logger.error("Exception during createConfigProduct for productId {} propertyId {} aggregateVersion {}: {}", productId, propertyId, newAggregateVersion, e.getMessage(), e);
            throw e;
        }
    }

    @Override
    public void updateConfigProduct(Connection conn, Map<String, Object> event) throws SQLException, Exception {
        // We attempt to update the record IF the incoming event's aggregate_version is greater than the current projection's version.
        // This enforces Idempotence (IDM) and Optimistic Concurrency Control (OCC) by ensuring version monotonicity.
        // We explicitly set active = TRUE as an UPDATE event implies the property should be active.
        final String sql =
                """
                UPDATE product_property_t
                SET property_value = ?,
                    update_user = ?,
                    update_ts = ?,
                    aggregate_version = ?,
                    active = TRUE
                WHERE product_id = ?
                  AND property_id = ?
                  AND aggregate_version < ?
                """; // <<< CRITICAL: Changed aggregate_version = ? to aggregate_version < ? to enforce monotonicity (OCC/IDM)

        // Note: The original code uses a non-standard map retrieval: Map<String, Object> map = (Map<String, Object>)event.get(PortalConstants.DATA);
        // Assuming SqlUtil.extractEventData(event) is the correct utility based on other methods.
        Map<String, Object> map = SqlUtil.extractEventData(event);
        String productId = (String)map.get("productId");
        String propertyId = (String)map.get("propertyId");
        long oldAggregateVersion = SqlUtil.getOldAggregateVersion(event);
        long newAggregateVersion = SqlUtil.getNewAggregateVersion(event);

        try (PreparedStatement statement = conn.prepareStatement(sql)) {
            // SET values (4 dynamic values + active = TRUE in SQL)
            int i = 1;
            // 1. property_value
            if (map.containsKey("propertyValue")) {
                statement.setString(i++, (String) map.get("propertyValue"));
            } else {
                statement.setNull(i++, Types.VARCHAR); // Using TEXT/VARCHAR
            }
            // 2. update_user
            statement.setString(i++, (String)event.get(Constants.USER));
            // 3. update_ts
            statement.setObject(i++, OffsetDateTime.parse((String)event.get(CloudEventV1.TIME)));
            // 4. aggregate_version
            statement.setLong(i++, newAggregateVersion);

            // WHERE conditions (3 placeholders)
            // 5. product_id
            statement.setString(i++, productId);
            // 6. property_id
            statement.setObject(i++, UUID.fromString(propertyId));
            // 7. aggregate_version < ? (new version for OCC/IDM check)
            statement.setLong(i++, newAggregateVersion);

            int count = statement.executeUpdate();
            if (count == 0) {
                // If 0 rows updated, it means the record was either not found
                // OR aggregate_version >= newAggregateVersion (OCC/IDM check failed).
                // We IGNORE the failure and log a warning, as this is the desired idempotent/monotonic behavior.
                logger.warn("Update skipped for productId {} propertyId {} (old: {}) -> (new: {}). Record not found or a newer/same version already exists.", productId, propertyId, oldAggregateVersion, newAggregateVersion);
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
        // Use UPDATE to implement Soft Delete (setting active = FALSE).
        // OCC/IDM is enforced by checking aggregate_version < newAggregateVersion.
        final String sql =
                """
                UPDATE product_property_t
                SET active = FALSE,
                    update_user = ?,
                    update_ts = ?,
                    aggregate_version = ?
                WHERE product_id = ?
                  AND property_id = ?
                  AND aggregate_version < ?
                """; // <<< CRITICAL: Added aggregate_version < ? to enforce monotonicity (OCC/IDM)

        // Note: The original code uses a non-standard map retrieval: Map<String, Object> map = (Map<String, Object>)event.get(PortalConstants.DATA);
        // Assuming SqlUtil.extractEventData(event) is the correct utility based on other methods.
        Map<String, Object> map = SqlUtil.extractEventData(event);
        String productId = (String)map.get("productId");
        String propertyId = (String)map.get("propertyId");
        // A delete event represents a state change, so it should have a new, incremented version.
        long newAggregateVersion = SqlUtil.getNewAggregateVersion(event);
        // oldAggregateVersion is kept for logging context from the original method.
        long oldAggregateVersion = SqlUtil.getOldAggregateVersion(event);

        try (PreparedStatement statement = conn.prepareStatement(sql)) {
            // SET values (3 placeholders)
            // 1: update_user
            statement.setString(1, (String)event.get(Constants.USER));
            // 2: update_ts
            statement.setObject(2, OffsetDateTime.parse((String)event.get(CloudEventV1.TIME)));
            // 3: aggregate_version (the new version)
            statement.setLong(3, newAggregateVersion);

            // WHERE conditions (3 placeholders)
            // 4: product_id
            statement.setString(4, productId);
            // 5. property_id
            statement.setObject(5, UUID.fromString(propertyId));
            // 6: aggregate_version < ? (new version for OCC/IDM check)
            statement.setLong(6, newAggregateVersion);

            int count = statement.executeUpdate();
            if (count == 0) {
                // If 0 rows updated, it means:
                // 1. The record was not found (already deleted or never existed).
                // 2. The OCC/IDM check failed (aggregate_version >= newAggregateVersion).
                // We IGNORE the failure and log a warning, as this is the desired idempotent/monotonic behavior.
                logger.warn("Soft delete skipped for productId {} propertyId {} (old: {}) -> (new: {}). Record not found or a newer/same version already exists.", productId, propertyId, oldAggregateVersion, newAggregateVersion);
            }
        } catch (SQLException e) {
            logger.error("SQLException during deleteConfigProduct for productId {} propertyId {} (old: {}) -> (new: {}): {}", productId, propertyId, oldAggregateVersion, newAggregateVersion, e.getMessage(), e);
            throw e;
        } catch (Exception e) {
            logger.error("Exception during deleteConfigProduct for productId {} propertyId {} (old: {}) -> (new: {}): {}", productId, propertyId, oldAggregateVersion, newAggregateVersion, e.getMessage(), e);
            throw e;
        }
    }

    @Override
    public Result<String> getConfigProduct(int offset, int limit, String filtersJson, String globalFilter, String sortingJson) {
        Result<String> result = null;
        final Map<String, String> columnMap = new HashMap<>(Map.of(
                "productId", "pp.product_id",
                "configId", "p.config_id",
                "propertyId", "pp.property_id",
                "propertyName", "p.property_name",
                "propertyValue", "pp.property_value",
                "updateUser", "pp.update_user",
                "updateTs", "pp.update_ts",
                "configName", "c.config_name",
                "aggregateVersion", "pp.aggregate_version"
        ));
        List<Map<String, Object>> filters = parseJsonList(filtersJson);
        List<Map<String, Object>> sorting = parseJsonList(sortingJson);


        String s =
            """
                SELECT COUNT(*) OVER () AS total,
                pp.product_id, p.config_id, pp.property_id, p.property_name, pp.property_value,
                pp.update_user, pp.update_ts, c.config_name, pp.aggregate_version, pp.active
                FROM product_property_t pp
                INNER JOIN config_property_t p ON p.property_id = pp.property_id
                INNER JOIN config_t c ON p.config_id = c.config_id
                WHERE 1=1
            """;

        List<Object> parameters = new ArrayList<>();

        String[] searchColumns = {"p.property_name", "c.config_name"};
        String sqlBuilder = s + dynamicFilter(Arrays.asList("p.config_id", "pp.property_id"), Arrays.asList(searchColumns), filters, columnMap, parameters) +
                globalFilter(globalFilter, searchColumns, parameters) +
                dynamicSorting("pp.product_id, p.config_id, p.property_name", sorting, columnMap) +
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
                    map.put("productId", resultSet.getString("product_id"));
                    map.put("configId", resultSet.getObject("config_id", UUID.class));
                    map.put("configName", resultSet.getString("config_name"));
                    map.put("propertyId", resultSet.getObject("property_id", UUID.class));
                    map.put("propertyName", resultSet.getString("property_name"));
                    map.put("propertyValue", resultSet.getString("property_value"));
                    map.put("updateUser", resultSet.getString("update_user"));
                    map.put("updateTs", resultSet.getObject("update_ts") != null ? resultSet.getObject("update_ts", OffsetDateTime.class) : null);
                    map.put("aggregateVersion", resultSet.getInt("aggregate_version"));
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
            result = Failure.of(new Status("SQL_EXCEPTION", e.getMessage()));
        } catch (Exception e) {
            logger.error("Exception:", e);
            result = Failure.of(new Status("GENERIC_EXCEPTION", e.getMessage()));
        }
        return result;
    }

    @Override
    public Result<String> getConfigProductById(String productId, String propertyId) {
        final String sql =
                """
                SELECT product_id, property_id, property_value,
                aggregate_version, active, update_user, update_ts
                FROM product_property_t
                WHERE product_id = ? AND property_id = ?
                """;
        Result<String> result;
        Map<String, Object> map = new HashMap<>();

        String searchId = productId + ":" + propertyId;

        try (Connection conn = ds.getConnection();
             PreparedStatement statement = conn.prepareStatement(sql)) {

            statement.setString(1, productId);
            statement.setObject(2, UUID.fromString(propertyId));

            try (ResultSet resultSet = statement.executeQuery()) {
                if (resultSet.next()) {
                    map.put("productId", resultSet.getString("product_id"));
                    map.put("propertyId", resultSet.getObject("property_id", UUID.class));
                    map.put("propertyValue", resultSet.getString("property_value"));
                    map.put("aggregateVersion", resultSet.getLong("aggregate_version"));
                    map.put("active", resultSet.getBoolean("active"));
                    map.put("updateUser", resultSet.getString("update_user"));
                    map.put("updateTs", resultSet.getObject("update_ts") != null ? resultSet.getObject("update_ts", OffsetDateTime.class) : null);

                    result = Success.of(JsonMapper.toJson(map));
                } else {
                    result = Failure.of(new Status(OBJECT_NOT_FOUND, "product_property", searchId));
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
    public void createConfigProductVersion(Connection conn, Map<String, Object> event) throws SQLException, Exception {
        // Use UPSERT based on the Primary Key (host_id, product_version_id, property_id): INSERT ON CONFLICT DO UPDATE
        // This handles:
        // 1. First time insert (no conflict).
        // 2. Re-creation (conflict on PK) -> UPDATE the existing soft-deleted row (setting active=TRUE and new version).

        final String sql =
                """
                INSERT INTO product_version_property_t (
                    host_id,
                    product_version_id,
                    property_id,
                    property_value,
                    update_user,
                    update_ts,
                    aggregate_version,
                    active
                ) VALUES (?, ?, ?, ?, ?, ?, ?, TRUE)
                ON CONFLICT (host_id, product_version_id, property_id) DO UPDATE
                SET property_value = EXCLUDED.property_value,
                    update_user = EXCLUDED.update_user,
                    update_ts = EXCLUDED.update_ts,
                    aggregate_version = EXCLUDED.aggregate_version,
                    active = TRUE
                -- OCC/IDM: Only update if the incoming event is newer
                WHERE product_version_property_t.aggregate_version < EXCLUDED.aggregate_version
                """;

        // Note: The original code retrieves hostId from event metadata/constants,
        // while the data map is used for productVersionId and propertyId.
        Map<String, Object> map = SqlUtil.extractEventData(event);
        String hostId = (String)map.get("hostId"); // Assuming hostId is in the data map, consistent with other methods
        String productVersionId = (String)map.get("productVersionId");
        String propertyId = (String)map.get("propertyId");
        long newAggregateVersion = SqlUtil.getNewAggregateVersion(event);

        try (PreparedStatement statement = conn.prepareStatement(sql)) {
            // INSERT values (7 placeholders + active=TRUE in SQL, total 7 dynamic values)
            int i = 1;
            // 1. host_id (Required, part of PK)
            statement.setObject(i++, UUID.fromString(hostId));
            // 2. product_version_id (Required, part of PK)
            statement.setObject(i++, UUID.fromString(productVersionId));
            // 3. property_id (Required, part of PK)
            statement.setObject(i++, UUID.fromString(propertyId));

            // 4. property_value
            if (map.containsKey("propertyValue")) {
                statement.setString(i++, (String) map.get("propertyValue"));
            } else {
                statement.setNull(i++, Types.VARCHAR); // Using TEXT/VARCHAR
            }

            // 5. update_user
            statement.setString(i++, (String)event.get(Constants.USER));
            // 6. update_ts
            statement.setObject(i++, OffsetDateTime.parse((String)event.get(CloudEventV1.TIME)));
            // 7. aggregate_version
            statement.setLong(i++, newAggregateVersion);

            int count = statement.executeUpdate();
            if (count == 0) {
                // count=0 means the ON CONFLICT clause was hit, BUT the WHERE clause (aggregate_version < EXCLUDED.aggregate_version) failed.
                // This is the desired idempotent/out-of-order protection behavior. Log and ignore.
                logger.warn("Creation/Reactivation skipped for hostId {} productVersionId {} propertyId {} aggregateVersion {}. A newer or same version already exists.", hostId, productVersionId, propertyId, newAggregateVersion);
            }
        } catch (SQLException e) {
            logger.error("SQLException during createConfigProductVersion for hostId {} productVersionId {} propertyId {} aggregateVersion {}: {}", hostId, productVersionId, propertyId, newAggregateVersion, e.getMessage(), e);
            throw e;
        } catch (Exception e) {
            logger.error("Exception during createConfigProductVersion for hostId {} productVersionId {} propertyId {} aggregateVersion {}: {}", hostId, productVersionId, propertyId, newAggregateVersion, e.getMessage(), e);
            throw e;
        }
    }

    @Override
    public void updateConfigProductVersion(Connection conn, Map<String, Object> event) throws SQLException, Exception {
        // We attempt to update the record IF the incoming event's aggregate_version is greater than the current projection's version.
        // This enforces Idempotence (IDM) and Optimistic Concurrency Control (OCC) by ensuring version monotonicity.
        // We explicitly set active = TRUE as an UPDATE event implies the property should be active.
        final String sql =
                """
                UPDATE product_version_property_t
                SET property_value = ?,
                    update_user = ?,
                    update_ts = ?,
                    aggregate_version = ?,
                    active = TRUE
                WHERE host_id = ?
                  AND product_version_id = ?
                  AND property_id = ?
                  AND aggregate_version < ?
                """; // <<< CRITICAL: Changed aggregate_version = ? to aggregate_version < ? to enforce monotonicity (OCC/IDM)

        // Note: The original code retrieves hostId from event metadata/constants,
        // while the data map is used for productVersionId and propertyId.
        Map<String, Object> map = SqlUtil.extractEventData(event);
        String hostId = (String)map.get("hostId"); // Assuming hostId is in the data map, consistent with other methods
        String productVersionId = (String)map.get("productVersionId");
        String propertyId = (String)map.get("propertyId");
        long oldAggregateVersion = SqlUtil.getOldAggregateVersion(event);
        long newAggregateVersion = SqlUtil.getNewAggregateVersion(event);

        try (PreparedStatement statement = conn.prepareStatement(sql)) {
            // SET values (4 dynamic values + active = TRUE in SQL)
            int i = 1;
            // 1. property_value
            if (map.containsKey("propertyValue")) {
                statement.setString(i++, (String) map.get("propertyValue"));
            } else {
                statement.setNull(i++, Types.VARCHAR); // Using TEXT/VARCHAR
            }
            // 2. update_user
            statement.setString(i++, (String)event.get(Constants.USER));
            // 3. update_ts
            statement.setObject(i++, OffsetDateTime.parse((String)event.get(CloudEventV1.TIME)));
            // 4. aggregate_version
            statement.setLong(i++, newAggregateVersion);

            // WHERE conditions (4 placeholders)
            // 5. host_id
            statement.setObject(i++, UUID.fromString(hostId));
            // 6. product_version_id
            statement.setObject(i++, UUID.fromString(productVersionId));
            // 7. property_id
            statement.setObject(i++, UUID.fromString(propertyId));
            // 8. aggregate_version < ? (new version for OCC/IDM check)
            statement.setLong(i++, newAggregateVersion);

            int count = statement.executeUpdate();
            if (count == 0) {
                // If 0 rows updated, it means the record was either not found
                // OR aggregate_version >= newAggregateVersion (OCC/IDM check failed).
                // We IGNORE the failure and log a warning, as this is the desired idempotent/monotonic behavior.
                logger.warn("Update skipped for hostId {} productVersionId {} propertyId {} (old: {}) -> (new: {}). Record not found or a newer/same version already exists.", hostId, productVersionId, propertyId, oldAggregateVersion, newAggregateVersion);
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
        // Use UPDATE to implement Soft Delete (setting active = FALSE).
        // OCC/IDM is enforced by checking aggregate_version < newAggregateVersion.
        final String sql =
                """
                UPDATE product_version_property_t
                SET active = FALSE,
                    update_user = ?,
                    update_ts = ?,
                    aggregate_version = ?
                WHERE host_id = ?
                  AND product_version_id = ?
                  AND property_id = ?
                  AND aggregate_version < ?
                """; // <<< CRITICAL: Added aggregate_version < ? to enforce monotonicity (OCC/IDM)

        // Note: The original code retrieves hostId from event metadata/constants,
        // while the data map is used for productVersionId and propertyId.
        Map<String, Object> map = SqlUtil.extractEventData(event);
        String hostId = (String)event.get(Constants.HOST);
        String productVersionId = (String)map.get("productVersionId");
        String propertyId = (String)map.get("propertyId");
        // A delete event represents a state change, so it should have a new, incremented version.
        long newAggregateVersion = SqlUtil.getNewAggregateVersion(event);
        // oldAggregateVersion is kept for logging context from the original method.
        long oldAggregateVersion = SqlUtil.getOldAggregateVersion(event);

        try (PreparedStatement statement = conn.prepareStatement(sql)) {
            // SET values (3 placeholders)
            // 1: update_user
            statement.setString(1, (String)event.get(Constants.USER));
            // 2: update_ts
            statement.setObject(2, OffsetDateTime.parse((String)event.get(CloudEventV1.TIME)));
            // 3: aggregate_version (the new version)
            statement.setLong(3, newAggregateVersion);

            // WHERE conditions (4 placeholders)
            // 4: host_id
            statement.setObject(4, UUID.fromString(hostId));
            // 5. product_version_id
            statement.setObject(5, UUID.fromString(productVersionId));
            // 6. property_id
            statement.setObject(6, UUID.fromString(propertyId));
            // 7: aggregate_version < ? (new version for OCC/IDM check)
            statement.setLong(7, newAggregateVersion);

            int count = statement.executeUpdate();
            if (count == 0) {
                // If 0 rows updated, it means:
                // 1. The record was not found (already deleted or never existed).
                // 2. The OCC/IDM check failed (aggregate_version >= newAggregateVersion).
                // We IGNORE the failure and log a warning, as this is the desired idempotent/monotonic behavior.
                logger.warn("Soft delete skipped for hostId {} productVersionId {} propertyId {} (old: {}) -> (new: {}). Record not found or a newer/same version already exists.", hostId, productVersionId, propertyId, oldAggregateVersion, newAggregateVersion);
            }
        } catch (SQLException e) {
            logger.error("SQLException during deleteConfigProductVersion for hostId {} productVersionId {} propertyId {} (old: {}) -> (new: {}): {}", hostId, productVersionId, propertyId, oldAggregateVersion, newAggregateVersion, e.getMessage(), e);
            throw e;
        } catch (Exception e) {
            logger.error("Exception during deleteConfigProductVersion for hostId {} productVersionId {} propertyId {} (old: {}) -> (new: {}): {}", hostId, productVersionId, propertyId, oldAggregateVersion, newAggregateVersion, e.getMessage(), e);
            throw e;
        }
    }

    @Override
    public Result<String> getConfigProductVersion(int offset, int limit, String filtersJson, String globalFilter, String sortingJson, String hostId) {
        Result<String> result = null;
        final Map<String, String> columnMap = new HashMap<>(Map.of(
                "hostId", "pvp.host_id",
                "productVersionId", "pvp.product_version_id",
                "productId", "pv.product_id",
                "productVersion", "pv.product_version",
                "configId", "p.config_id",
                "configName", "c.config_name",
                "propertyId", "pvp.property_id",
                "propertyName", "p.property_name"
        ));
        columnMap.put("propertyValue", "pvp.property_value");
        columnMap.put("updateUser", "pvp.update_user");
        columnMap.put("updateTs", "pvp.update_ts");
        columnMap.put("configName", "c.config_name");
        columnMap.put("aggregateVersion", "pvp.aggregate_version");
        columnMap.put("active", "pvp.active");

        List<Map<String, Object>> filters = parseJsonList(filtersJson);
        List<Map<String, Object>> sorting = parseJsonList(sortingJson);

        String s =
                """
                SELECT COUNT(*) OVER () AS total,
                pvp.host_id, pvp.product_version_id, pv.product_id, pv.product_version, p.config_id, pvp.property_id,
                p.property_name, pvp.property_value, pvp.update_user, pvp.update_ts, pvp.active
                c.config_name, pvp.aggregate_version
                FROM product_version_property_t pvp
                INNER JOIN product_version_t pv ON pv.product_version_id = pvp.product_version_id
                INNER JOIN config_property_t p ON p.property_id = pvp.property_id
                INNER JOIN config_t c ON p.config_id = c.config_id
                WHERE pvp.host_id = ?
                """;

        List<Object> parameters = new ArrayList<>();
        parameters.add(UUID.fromString(hostId));

        String[] searchColumns = {"p.property_name", "c.config_name"};
        String sqlBuilder = s + dynamicFilter(Arrays.asList("pvp.host_id", "pvp.product_version_id", "p.config_id", "pvp.property_id"), Arrays.asList(searchColumns), filters, columnMap, parameters) +
                globalFilter(globalFilter, searchColumns, parameters) +
                dynamicSorting("pvp.host_id, pv.product_id, pv.product_version, p.config_id, p.display_order", sorting, columnMap) +
                "\nLIMIT ? OFFSET ?";

        parameters.add(limit);
        parameters.add(offset);

        int total = 0;
        List<Map<String, Object>> productVersionProperties = new ArrayList<>();

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
                    map.put("propertyValue", resultSet.getString("property_value"));
                    map.put("updateUser", resultSet.getString("update_user"));
                    map.put("updateTs", resultSet.getObject("update_ts") != null ? resultSet.getObject("update_ts", OffsetDateTime.class) : null);
                    map.put("aggregateVersion", resultSet.getInt("aggregate_version"));
                    map.put("active", resultSet.getBoolean("active"));
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
    public Result<String> getConfigProductVersionById(String hostId, String productVersionId, String propertyId) {
        final String sql =
                """
                SELECT host_id, product_version_id, property_id, property_value,
                aggregate_version, active, update_user, update_ts
                FROM product_version_property_t
                WHERE host_id = ? AND product_version_id = ? AND property_id = ?
                """;
        Result<String> result;
        Map<String, Object> map = new HashMap<>();

        String searchId = hostId + ":" + productVersionId + ":" + propertyId;

        try (Connection conn = ds.getConnection();
             PreparedStatement statement = conn.prepareStatement(sql)) {

            statement.setObject(1, UUID.fromString(hostId));
            statement.setObject(2, UUID.fromString(productVersionId));
            statement.setObject(3, UUID.fromString(propertyId));

            try (ResultSet resultSet = statement.executeQuery()) {
                if (resultSet.next()) {
                    map.put("hostId", resultSet.getObject("host_id", UUID.class));
                    map.put("productVersionId", resultSet.getObject("product_version_id", UUID.class));
                    map.put("propertyId", resultSet.getObject("property_id", UUID.class));
                    map.put("propertyValue", resultSet.getString("property_value"));
                    map.put("aggregateVersion", resultSet.getLong("aggregate_version"));
                    map.put("active", resultSet.getBoolean("active"));
                    map.put("updateUser", resultSet.getString("update_user"));
                    map.put("updateTs", resultSet.getObject("update_ts") != null ? resultSet.getObject("update_ts", OffsetDateTime.class) : null);

                    result = Success.of(JsonMapper.toJson(map));
                } else {
                    result = Failure.of(new Status(OBJECT_NOT_FOUND, "product_version_property", searchId));
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
    public Result<String> getPromotableInstanceConfigs(String hostId, String instanceId, Set<String> propertyNames, Set<String> apiUids) {
        Result<String> result;
        final String instanceConfigsSql = """
                WITH params AS MATERIALIZED (
                    SELECT
                        CAST(v.host_id as UUID) as host_id,
                        CAST(v.instance_id as UUID) as instance_id,
                        v.property_names::VARCHAR[] as property_names
                    FROM (
                        values
                            (?, ?, ?)
                    ) as v(host_id, instance_id, property_names)
                ),
                instance_properties AS (
                    SELECT
                        ip.property_id as property_id,
                        CASE
                            WHEN cp.property_type IN ('File', 'Cert') THEN cp.property_name
                            ELSE CONCAT(config.config_name, '.', cp.property_name)
                        END as property_name,
                        ip.property_value as property_value,
                        cp.value_type as property_value_type,
                        cp.property_type as property_type,
                        'instance_property'::VARCHAR as property_source_type
                    FROM
                        instance_property_t as ip
                        JOIN config_property_t as cp ON cp.property_id = ip.property_id
                        JOIN config_t as config ON config.config_id = cp.config_id AND config.config_phase = 'R'
                        JOIN instance_t as instance ON instance.instance_id = ip.instance_id
                            AND instance.host_id = ip.host_id
                        JOIN params ON params.instance_id = instance.instance_id
                            AND params.instance_id = ip.instance_id
                            AND params.host_id = instance.host_id
                )
                SELECT p.host_id, p.instance_id, ip.property_id, ip.property_name, ip.property_value,
                    ip.property_value_type, ip.property_type, ip.property_source_type
                FROM instance_properties ip
                    JOIN params p ON
                        array_length(p.property_names, 1) IS NULL
                            OR ip.property_name = ANY(p.property_names)
                """;

        final String subresourceConfigsSql = """
            WITH params AS MATERIALIZED (
                SELECT
                    CAST(v.host_id AS UUID) AS host_id,
                    CAST(v.instance_id AS UUID) AS instance_id,
                    v.api_uids::VARCHAR[] AS api_uids
                FROM (
                    VALUES (?, ?, ?)
                ) AS v(host_id, instance_id, api_uids)
            ),
            instance_api_path_prefix AS (
                SELECT
                    STRING_AGG(iapp.path_prefix, ', ' ORDER BY iapp.path_prefix) AS api_path_prefixes,
                    ia.instance_api_id,
                    ia.host_id
                FROM
                    instance_api_t ia
                JOIN api_version_t av ON av.api_version_id = ia.api_version_id AND av.host_id = ia.host_id
                JOIN instance_t i ON i.instance_id = ia.instance_id AND i.host_id = ia.host_id
                JOIN params p ON p.instance_id = ia.instance_id
                    AND p.host_id = ia.host_id
                    AND (array_length(p.api_uids, 1) IS NULL OR av.api_id || '-' || av.api_version = ANY(p.api_uids))
                LEFT JOIN instance_api_path_prefix_t iapp ON ia.instance_api_id = iapp.instance_api_id AND ia.host_id = iapp.host_id
                GROUP BY ia.host_id, ia.instance_api_id
            ),
            configuration_properties AS (
                SELECT
                    cp.property_id,
                    c.config_name || '.' || cp.property_name AS property_name,
                    cp.value_type AS property_value_type,
                    cp.property_type
                FROM
                    config_property_t cp
                JOIN config_t c ON c.config_id = cp.config_id
                WHERE cp.property_type = 'Config'
            )
            SELECT
                p.host_id,
                p.instance_id,
                cp.property_id,
                cp.property_name,
                cp.property_value_type,
                cp.property_type,
                'instance_api_property' AS property_source_type,
                iap.property_value,
                iap.instance_api_id,
                av.api_id || '-' || av.api_version AS api_uid,
                av.api_id,
                av.api_version AS api_version_value,
                iapp.api_path_prefixes,
                NULL::UUID AS instance_app_id,
                NULL::VARCHAR AS app_id
            FROM
                instance_api_property_t iap
            JOIN configuration_properties cp ON cp.property_id = iap.property_id
            JOIN instance_api_t ia ON ia.instance_api_id = iap.instance_api_id AND ia.host_id = iap.host_id
            JOIN api_version_t av ON av.api_version_id = ia.api_version_id AND av.host_id = ia.host_id
            JOIN params p ON p.instance_id = ia.instance_id AND p.host_id = ia.host_id
                AND (array_length(p.api_uids, 1) IS NULL OR av.api_id || '-' || av.api_version = ANY(p.api_uids))
            LEFT JOIN instance_api_path_prefix iapp ON iapp.instance_api_id = ia.instance_api_id AND iapp.host_id = ia.host_id

            UNION ALL

            SELECT
                p.host_id,
                p.instance_id,
                cp.property_id,
                cp.property_name,
                cp.property_value_type,
                cp.property_type,
                'instance_app_property' AS property_source_type,
                iap.property_value,
                NULL::UUID AS instance_api_id,
                NULL::VARCHAR AS api_uid,
                NULL::VARCHAR AS api_id,
                NULL::VARCHAR AS api_version_value,
                NULL::VARCHAR AS api_path_prefixes,
                iap.instance_app_id,
                ia.app_id
            FROM
                instance_app_property_t iap
            JOIN configuration_properties cp ON cp.property_id = iap.property_id
            JOIN instance_app_t ia ON ia.instance_app_id = iap.instance_app_id AND ia.host_id = iap.host_id
            JOIN params p ON p.instance_id = ia.instance_id AND p.host_id = ia.host_id

            UNION ALL

            SELECT
                p.host_id,
                p.instance_id,
                cp.property_id,
                cp.property_name,
                cp.property_value_type,
                cp.property_type,
                'instance_app_api_property' AS property_source_type,
                iaap.property_value,
                iaap.instance_api_id,
                av.api_id || '-' || av.api_version AS api_uid,
                av.api_id,
                av.api_version AS api_version_value,
                iapp.api_path_prefixes,
                iaap.instance_app_id,
                ia.app_id
            FROM
                instance_app_api_property_t iaap
            JOIN configuration_properties cp ON cp.property_id = iaap.property_id
            JOIN instance_app_api_t iaa ON iaa.instance_api_id = iaap.instance_api_id
                AND iaa.instance_app_id = iaap.instance_app_id
                AND iaa.host_id = iaap.host_id
            JOIN instance_app_t ia ON ia.instance_app_id = iaap.instance_app_id
                AND ia.host_id = iaap.host_id
            JOIN instance_api_t iai ON iai.instance_api_id = iaap.instance_api_id
                AND iai.host_id = iaap.host_id
            JOIN api_version_t av ON av.api_version_id = iai.api_version_id
                AND av.host_id = iai.host_id
            JOIN params p ON p.instance_id = ia.instance_id
                AND p.instance_id = iai.instance_id
                AND p.host_id = ia.host_id
                AND (array_length(p.api_uids, 1) IS NULL OR av.api_id || '-' || av.api_version = ANY(p.api_uids))
            LEFT JOIN instance_api_path_prefix iapp ON iapp.instance_api_id = iai.instance_api_id
                AND iapp.host_id = iai.host_id
            """;

        final String instanceCustomFilesSql = """
                WITH params AS MATERIALIZED (
                    SELECT
                        CAST(v.host_id as UUID) as host_id,
                        CAST(v.instance_id as UUID) as instance_id,
                        v.property_names::VARCHAR[] as property_names
                    FROM (
                        values
                            (?, ?, ?)
                    ) as v(host_id, instance_id, property_names)
                )
                SELECT f.host_id, f.instance_id, f.instance_file_id, f.file_name, f.file_value, f.file_type
                FROM instance_file_t f
                JOIN instance_t i ON f.instance_id = i.instance_id AND f.host_id = i.host_id
                JOIN params p on f.host_id = p.host_id AND f.instance_id = p.instance_id
                    AND (array_length(p.property_names, 1) IS NULL OR f.file_name = ANY(p.property_names))
            """;

        List<Map<String, Object>> instanceConfigs = new ArrayList<>();
        List<Map<String, Object>> subresourceConfigs = new ArrayList<>();
        List<Map<String, Object>> instanceCustomFiles = new ArrayList<>();

        try (Connection connection = ds.getConnection();
             PreparedStatement instanceConfigsPs = connection.prepareStatement(instanceConfigsSql);
             PreparedStatement subresourceConfigsPs = connection.prepareStatement(subresourceConfigsSql);
             PreparedStatement instanceCustomFilesPs = connection.prepareStatement(instanceCustomFilesSql)
        ) {
            UUID instanceIdUUID = instanceId != null ? UUID.fromString(instanceId) : null;
            UUID hostIdUUID = hostId != null ? UUID.fromString(hostId) : null;

            instanceConfigsPs.setObject(1, hostIdUUID);
            instanceConfigsPs.setObject(2, instanceIdUUID);
            instanceConfigsPs.setString(3, SqlUtil.createArrayLiteral(propertyNames));


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

            subresourceConfigsPs.setObject(1, hostIdUUID);
            subresourceConfigsPs.setObject(2, instanceIdUUID);
            subresourceConfigsPs.setString(3, SqlUtil.createArrayLiteral(apiUids));

            try (ResultSet resultSet = subresourceConfigsPs.executeQuery()) {
                while (resultSet.next()) {
                    Map<String, Object> map = new HashMap<>();
                    map.put("hostId", resultSet.getObject("host_id", UUID.class));
                    map.put("instanceId", resultSet.getObject("instance_id", UUID.class));
                    map.put("propertyId", resultSet.getObject("property_id", UUID.class));
                    map.put("propertyName", resultSet.getString("property_name"));
                    map.put("propertyValue", resultSet.getString("property_value"));
                    map.put("propertySourceType", resultSet.getString("property_source_type"));
                    map.put("valueType", resultSet.getString("property_value_type"));
                    map.put("propertyType", resultSet.getString("property_type"));
                    map.put("instanceApiId", resultSet.getObject("instance_api_id", UUID.class));
                    map.put("apiUid", resultSet.getString("api_uid"));
                    map.put("apiId", resultSet.getString("api_id"));
                    map.put("apiVersion", resultSet.getString("api_version_value"));
                    map.put("apiPathPrefixes", resultSet.getString("api_path_prefixes"));
                    map.put("instanceAppId", resultSet.getObject("instance_app_id", UUID.class));
                    map.put("appId", resultSet.getString("app_id"));
                    subresourceConfigs.add(map);
                }
            }

            instanceCustomFilesPs.setObject(1, hostIdUUID);
            instanceCustomFilesPs.setObject(2, instanceIdUUID);
            instanceCustomFilesPs.setString(3, SqlUtil.createArrayLiteral(propertyNames));

            try (ResultSet resultSet = instanceCustomFilesPs.executeQuery()) {
                while (resultSet.next()) {
                    Map<String, Object> map = new HashMap<>();
                    map.put("hostId", resultSet.getObject("host_id", UUID.class));
                    map.put("instanceId", resultSet.getObject("instance_id", UUID.class));
                    map.put("instanceFileId", resultSet.getObject("instance_file_id", UUID.class));
                    map.put("fileName", resultSet.getString("file_name"));
                    map.put("fileValue", resultSet.getString("file_value"));
                    map.put("fileType", resultSet.getString("file_type"));
                    instanceCustomFiles.add(map);
                }
            }

            Map<String, Object> resultMap = new HashMap<>();
            resultMap.put("instanceConfigs", instanceConfigs);
            resultMap.put("subresourceConfigs", subresourceConfigs);
            resultMap.put("instanceCustomFiles", instanceCustomFiles);
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
