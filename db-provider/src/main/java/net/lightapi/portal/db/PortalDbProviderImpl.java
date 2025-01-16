package net.lightapi.portal.db;

import com.networknt.config.JsonMapper;
import com.networknt.kafka.common.AvroConverter;
import com.networknt.kafka.common.EventId;
import com.networknt.monad.Failure;
import com.networknt.monad.Result;
import com.networknt.monad.Success;
import com.networknt.security.KeyUtil;
import com.networknt.status.Status;
import com.networknt.utility.HashUtil;
import net.lightapi.portal.market.*;
import net.lightapi.portal.user.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.security.KeyPair;
import java.sql.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.networknt.db.provider.SqlDbStartupHook.cacheManager;
import static com.networknt.db.provider.SqlDbStartupHook.ds;
import static java.sql.Types.NULL;

public class PortalDbProviderImpl implements PortalDbProvider {
    public static final Logger logger = LoggerFactory.getLogger(PortalDbProviderImpl.class);
    public static final String AUTH_CODE_CACHE = "auth_code";
    public static final String SQL_EXCEPTION = "ERR10017";
    public static final String GENERIC_EXCEPTION = "ERR10014";
    public static final String OBJECT_NOT_FOUND = "ERR11637";

    public static final String INSERT_NOTIFICATION = "INSERT INTO notification_t (user_id, nonce, event_class, event_json, process_time, " +
            "process_flag, error) VALUES (?, ?, ?, ?, ?,  ?, ?)";

    @Override
    public Result<String> queryRefTable(int offset, int limit, String hostId, String tableName, String tableDesc, String active, String editable, String common) {
        Result<String> result = null;
        String sql = "SELECT COUNT(*) OVER () AS total,\n" +
                "               rt.table_id,\n" +
                "               rt.table_name,\n" +
                "               rt.table_desc,\n" +
                "               rt.active,\n" +
                "               rt.editable,\n" +
                "               rt.common,\n" +
                "               rht.host_id\n" +
                "        FROM ref_table_t rt\n" +
                "        JOIN ref_host_t rht ON rt.table_id = rht.table_id\n" +
                "        WHERE rht.host_id = ?\n" +
                "        AND rt.active = ?\n" +
                "        AND rt.editable = ?\n" +
                "        AND (\n" +
                "            rt.common = ?\n" +
                "                OR  rht.host_id = ?\n" +
                "        )\n" +
                "        AND (\n" +
                "            ? IS NULL OR ? = '*' OR rt.table_name LIKE '%' || ? || '%'\n" +
                "        )\n" +
                "        AND (\n" +
                "            ? IS NULL OR ? = '*' OR rt.table_desc LIKE '%' || ? || '%'\n" +
                "        )\n" +
                "        ORDER BY rt.table_name\n" +
                "        LIMIT ? OFFSET ?;";

        int total = 0;
        List<Map<String, Object>> tables = new ArrayList<>();

        try (Connection connection = ds.getConnection();
             PreparedStatement preparedStatement = connection.prepareStatement(sql)) {
            preparedStatement.setString(1, hostId);
            preparedStatement.setString(2, active);
            preparedStatement.setString(3, editable);
            preparedStatement.setString(4, common);
            preparedStatement.setString(5, hostId);
            preparedStatement.setString(6, tableName);
            preparedStatement.setString(7, tableName);
            preparedStatement.setString(8, tableName);
            preparedStatement.setString(9, tableDesc);
            preparedStatement.setString(10, tableDesc);
            preparedStatement.setString(11, tableDesc);
            preparedStatement.setInt(12, limit);
            preparedStatement.setInt(13, offset);

            boolean isFirstRow = true;
            try (ResultSet resultSet = preparedStatement.executeQuery()) {
                while (resultSet.next()) {
                    Map<String, Object> map = new HashMap<>();
                    // only get the total once as it is the same for all rows.
                    if (isFirstRow) {
                        total = resultSet.getInt("total");
                        isFirstRow = false;
                    }

                    map.put("tableId", resultSet.getString("table_id"));
                    map.put("tableName", resultSet.getString("table_name"));
                    map.put("tableDesc", resultSet.getString("table_desc"));
                    map.put("active", resultSet.getString("active"));
                    map.put("editable", resultSet.getString("editable"));
                    map.put("common", resultSet.getString("common"));
                    map.put("hostId", resultSet.getString("host_id"));
                    tables.add(map);
                }
            }
            // now, we have the total and the list of tables, we need to put them into a map.
            Map<String, Object> resultMap = new HashMap<>();
            resultMap.put("total", total);
            resultMap.put("tables", tables);
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
    public Result<String> loginUserByEmail(String email) {
        Result<String> result = null;
        String sql = "SELECT\n" +
                "    uh.host_id,\n" +
                "    u.user_id,\n" +
                "    u.email, \n" +
                "    u.user_type,\n" +
                "    u.password,\n" +
                "    u.verified,\n" +
                "    CASE\n" +
                "        WHEN u.user_type = 'E' THEN e.employee_id\n" +
                "        WHEN u.user_type = 'C' THEN c.customer_id\n" +
                "        ELSE NULL\n" +
                "    END AS entity_id,\n" +
                "    CASE WHEN u.user_type = 'E' THEN string_agg(DISTINCT p.position_id, ' ' ORDER BY p.position_id) ELSE NULL END AS positions,\n" +
                "    string_agg(DISTINCT r.role_id, ' ' ORDER BY r.role_id) AS roles,\n" +
                "    string_agg(DISTINCT g.group_id, ' ' ORDER BY g.group_id) AS groups,\n" +
                "     CASE\n" +
                "        WHEN COUNT(DISTINCT at.attribute_id || '^=^' || aut.attribute_value) > 0 THEN string_agg(DISTINCT at.attribute_id || '^=^' || aut.attribute_value, '~' ORDER BY at.attribute_id || '^=^' || aut.attribute_value)\n" +
                "        ELSE NULL\n" +
                "    END AS attributes\n" +
                "FROM\n" +
                "    user_t AS u\n" +
                "LEFT JOIN\n" +
                "    user_host_t AS uh ON u.user_id = uh.user_id\n" +
                "LEFT JOIN\n" +
                "    role_user_t AS ru ON u.user_id = ru.user_id\n" +
                "LEFT JOIN\n" +
                "    role_t AS r ON ru.host_id = r.host_id AND ru.role_id = r.role_id\n" +
                "LEFT JOIN\n" +
                "    attribute_user_t AS aut ON u.user_id = aut.user_id\n" +
                "LEFT JOIN\n" +
                "    attribute_t AS at ON aut.host_id = at.host_id AND aut.attribute_id = at.attribute_id\n" +
                "LEFT JOIN\n" +
                "    group_user_t AS gu ON u.user_id = gu.user_id\n" +
                "LEFT JOIN\n" +
                "    group_t AS g ON gu.host_id = g.host_id AND gu.group_id = g.group_id\n" +
                "LEFT JOIN\n" +
                "    employee_t AS e ON uh.host_id = e.host_id AND u.user_id = e.user_id\n" +
                "LEFT JOIN\n" +
                "    customer_t AS c ON uh.host_id = c.host_id AND u.user_id = c.user_id\n" +
                "LEFT JOIN\n" +
                "    employee_position_t AS ep ON e.host_id = ep.host_id AND e.employee_id = ep.employee_id\n" +
                "LEFT JOIN\n" +
                "    position_t AS p ON ep.host_id = p.host_id AND ep.position_id = p.position_id\n" +
                "WHERE\n" +
                "    u.email = ?\n" +
                "    AND u.locked = FALSE\n" +
                "    AND u.verified = TRUE\n" +
                "GROUP BY\n" +
                "    uh.host_id, u.user_id, u.user_type, e.employee_id, c.customer_id;\n";
        try (final Connection conn = ds.getConnection()) {
            Map<String, Object> map = new HashMap<>();
            try (PreparedStatement statement = conn.prepareStatement(sql)) {
                statement.setString(1, email);
                try (ResultSet resultSet = statement.executeQuery()) {
                    if (resultSet.next()) {
                        map.put("hostId", resultSet.getString("host_id"));
                        map.put("userId", resultSet.getString("user_id"));
                        map.put("email", resultSet.getString("email"));
                        map.put("userType", resultSet.getString("user_type"));
                        map.put("entityId", resultSet.getString("entity_id"));
                        map.put("password", resultSet.getString("password"));
                        map.put("verified", resultSet.getBoolean("verified"));
                        map.put("positions", resultSet.getString("positions"));
                        map.put("roles", resultSet.getString("roles"));
                        map.put("groups", resultSet.getString("groups"));
                        map.put("attributes", resultSet.getString("attributes"));
                    }
                }
            }
            if (map.size() == 0)
                result = Failure.of(new Status(OBJECT_NOT_FOUND, "user", email));
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
    public Result<String> queryUserByEmail(String email) {
        Result<String> result = null;
        String sql =
                "SELECT h.host_id, u.user_id, u.email, u.password, u.language, \n" +
                        "u.first_name, u.last_name, u.user_type, u.phone_number, u.gender,\n" +
                        "u.birthday, u.country, u.province, u.city, u.address,\n" +
                        "u.post_code, u.verified, u.token, u.locked, u.nonce \n" +
                        "FROM user_t u, user_host_t h\n" +
                        "WHERE u.user_id = h.user_id\n" +
                        "AND email = ?";
        try (final Connection conn = ds.getConnection()) {
            Map<String, Object> map = new HashMap<>();
            try (PreparedStatement statement = conn.prepareStatement(sql)) {
                statement.setString(1, email);
                try (ResultSet resultSet = statement.executeQuery()) {
                    if (resultSet.next()) {
                        map.put("hostId", resultSet.getString("host_id"));
                        map.put("userId", resultSet.getString("user_id"));
                        map.put("email", resultSet.getString("email"));
                        map.put("password", resultSet.getString("password"));
                        map.put("language", resultSet.getString("language"));

                        map.put("firstName", resultSet.getString("first_name"));
                        map.put("lastName", resultSet.getString("last_name"));
                        map.put("userType", resultSet.getString("user_type"));
                        map.put("phoneNumber", resultSet.getString("phone_number"));
                        map.put("gender", resultSet.getString("gender"));

                        map.put("birthday", resultSet.getDate("birthday"));
                        map.put("country", resultSet.getString("country"));
                        map.put("province", resultSet.getString("province"));
                        map.put("city", resultSet.getString("city"));
                        map.put("address", resultSet.getString("address"));

                        map.put("postCode", resultSet.getString("post_code"));
                        map.put("verified", resultSet.getBoolean("verified"));
                        map.put("token", resultSet.getString("token"));
                        map.put("locked", resultSet.getBoolean("locked"));
                        map.put("nonce", resultSet.getLong("nonce"));
                    }
                }
            }
            if (map.size() == 0)
                result = Failure.of(new Status(OBJECT_NOT_FOUND, "user", email));
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
    public Result<String> queryUserById(String userId) {
        Result<String> result = null;
        String sql =
                "SELECT h.host_id, u.user_id, u.email, u.password, u.language, \n" +
                        "u.first_name, u.last_name, u.user_type, u.phone_number, u.gender,\n" +
                        "u.birthday, u.country, u.province, u.city, u.address,\n" +
                        "u.post_code, u.verified, u.token, u.locked, u.nonce \n" +
                        "FROM user_t u, user_host_t h\n" +
                        "WHERE u.user_id = h.user_id\n" +
                        "AND u.user_id = ?";
        try (final Connection conn = ds.getConnection()) {
            Map<String, Object> map = new HashMap();
            try (PreparedStatement statement = conn.prepareStatement(sql)) {
                statement.setString(1, userId);
                try (ResultSet resultSet = statement.executeQuery()) {
                    if (resultSet.next()) {
                        map.put("hostId", resultSet.getString("host_id"));
                        map.put("userId", resultSet.getString("user_id"));
                        map.put("email", resultSet.getString("email"));
                        map.put("password", resultSet.getString("password"));
                        map.put("language", resultSet.getString("language"));

                        map.put("firstName", resultSet.getString("first_name"));
                        map.put("lastName", resultSet.getString("last_name"));
                        map.put("userType", resultSet.getString("user_type"));
                        map.put("phoneNumber", resultSet.getString("phone_number"));
                        map.put("gender", resultSet.getString("gender"));

                        map.put("birthday", resultSet.getDate("birthday"));
                        map.put("country", resultSet.getString("country"));
                        map.put("province", resultSet.getString("province"));
                        map.put("city", resultSet.getString("city"));
                        map.put("address", resultSet.getString("address"));

                        map.put("postCode", resultSet.getString("post_code"));
                        map.put("verified", resultSet.getBoolean("verified"));
                        map.put("token", resultSet.getString("token"));
                        map.put("locked", resultSet.getBoolean("locked"));
                        map.put("nonce", resultSet.getLong("nonce"));
                    }
                }
            }
            if (map.size() == 0)
                result = Failure.of(new Status(OBJECT_NOT_FOUND, "user", userId));
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
    public Result<String> queryUserByTypeEntityId(String userType, String entityId) {
        Result<String> result = null;
        String sqlEmployee =
                "SELECT h.host_id, u.user_id, e.employee_id as entity_id, u.email, u.password, \n" +
                        "u.language, u.first_name, u.last_name, u.user_type, u.phone_number, \n" +
                        "u.gender, u.birthday, u.country, u.province, u.city, \n" +
                        "u.address, u.post_code, u.verified, u.token, u.locked, \n" +
                        "u.nonce\n" +
                        "FROM user_t u, user_host_t h, employee_t e\n" +
                        "WHERE u.user_id = h.user_id\n" +
                        "AND h.host_id = e.host_id\n" +
                        "AND h.user_id = e.user_id\n" +
                        "AND e.employee_id = ? \n";
        String sqlCustomer =
                "SELECT h.host_id, u.user_id, c.customer_id as entity_id, u.email, u.password, \n" +
                        "u.language, u.first_name, u.last_name, u.user_type, u.phone_number, \n" +
                        "u.gender, u.birthday, u.country, u.province, u.city, \n" +
                        "u.address, u.post_code, u.verified, u.token, u.locked, \n" +
                        "u.nonce\n" +
                        "FROM user_t u, user_host_t h, customer_t c\n" +
                        "WHERE u.user_id = h.user_id\n" +
                        "AND h.host_id = c.host_id\n" +
                        "AND h.user_id = c.user_id\n" +
                        "AND c.customer_id = ? \n";
        String sql = userType.equals("E") ? sqlEmployee : sqlCustomer;
        try (final Connection conn = ds.getConnection()) {
            Map<String, Object> map = new HashMap<>();
            try (PreparedStatement statement = conn.prepareStatement(sql)) {
                statement.setString(1, entityId);
                try (ResultSet resultSet = statement.executeQuery()) {
                    if (resultSet.next()) {
                        map.put("hostId", resultSet.getString("host_id"));
                        map.put("userId", resultSet.getString("user_id"));
                        map.put("entityId", resultSet.getString("entity_id"));
                        map.put("email", resultSet.getString("email"));
                        map.put("password", resultSet.getString("password"));

                        map.put("language", resultSet.getString("language"));
                        map.put("firstName", resultSet.getString("first_name"));
                        map.put("lastName", resultSet.getString("last_name"));
                        map.put("userType", resultSet.getString("user_type"));
                        map.put("phoneNumber", resultSet.getString("phone_number"));

                        map.put("gender", resultSet.getString("gender"));
                        map.put("birthday", resultSet.getDate("birthday"));
                        map.put("country", resultSet.getString("country"));
                        map.put("province", resultSet.getString("province"));
                        map.put("city", resultSet.getString("city"));

                        map.put("address", resultSet.getString("address"));
                        map.put("postCode", resultSet.getString("post_code"));
                        map.put("verified", resultSet.getBoolean("verified"));
                        map.put("token", resultSet.getString("token"));
                        map.put("locked", resultSet.getBoolean("locked"));

                        map.put("nonce", resultSet.getLong("nonce"));
                    }
                }
            }
            if (map.size() == 0)
                result = Failure.of(new Status(OBJECT_NOT_FOUND, "entityId", entityId));
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
    public Result<String> queryUserByWallet(String cryptoType, String cryptoAddress) {
        Result<String> result = null;
        String sql =
                "SELECT h.host_id, u.user_id, u.email, u.password, u.language, \n" +
                        "u.first_name, u.last_name, u.user_type, u.phone_number, u.gender,\n" +
                        "u.birthday, u.country, u.province, u.city, u.address,\n" +
                        "u.post_code, u.verified, u.token, u.locked, u.nonce \n" +
                        "FROM user_t u, user_host_t h, user_crypto_wallet_t w\n" +
                        "WHERE u.user_id = h.user_id\n" +
                        "AND u.user_id = w.user_id\n" +
                        "AND w.crypto_type = ?\n" +
                        "AND w.crypto_address = ?";
        try (final Connection conn = ds.getConnection()) {
            Map<String, Object> map = new HashMap();
            try (PreparedStatement statement = conn.prepareStatement(sql)) {
                statement.setString(1, cryptoType);
                statement.setString(2, cryptoAddress);
                try (ResultSet resultSet = statement.executeQuery()) {
                    if (resultSet.next()) {
                        map.put("hostId", resultSet.getInt("host_id"));
                        map.put("userId", resultSet.getString("user_id"));
                        map.put("firstName", resultSet.getInt("first_name"));
                        map.put("lastName", resultSet.getString("last_name"));
                        map.put("email", resultSet.getString("email"));
                        map.put("roles", resultSet.getString("roles"));
                        map.put("language", resultSet.getString("language"));
                        map.put("gender", resultSet.getString("gender"));
                        map.put("birthday", resultSet.getString("birthday"));
                        map.put("taijiWallet", resultSet.getString("taiji_wallet"));
                        map.put("country", resultSet.getString("country"));
                        map.put("province", resultSet.getString("province"));
                        map.put("city", resultSet.getString("city"));
                        map.put("postCode", resultSet.getString("post_code"));
                        map.put("address", resultSet.getString("address"));
                        map.put("verified", resultSet.getBoolean("verified"));
                        map.put("token", resultSet.getString("token"));
                        map.put("locked", resultSet.getBoolean("locked"));
                    }
                }
            }
            if (map.isEmpty())
                result = Failure.of(new Status(OBJECT_NOT_FOUND, "user", cryptoType + cryptoAddress));
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
    public Result<String> queryUserByHostId(int offset, int limit, String hostId, String email, String language, String userType,
                                     String entityId, String referralId, String managerId, String firstName, String lastName,
                                     String phoneNumber, String gender, String birthday, String country, String province, String city,
                                     String address, String postCode, Boolean verified, Boolean locked) {
        Result<String> result = null;
        StringBuilder sqlBuilder = new StringBuilder();
        sqlBuilder.append("SELECT COUNT(*) OVER () AS total,\n" +
                "uh.host_id, u.user_id, u.email, u.language, u.first_name, u.last_name, u.user_type, u.phone_number, " +
                "u.gender, u.birthday, u.country, u.province, u.city, u.address, u.post_code, u.verified, u.locked,\n" +
                "COALESCE(c.customer_id, e.employee_id) AS entity_id, c.referral_id, e.manager_id\n" +
                "FROM user_t u\n" +
                "LEFT JOIN user_host_t uh ON u.user_id = uh.user_id\n" +
                "LEFT JOIN customer_t c ON uh.host_id = c.host_id AND u.user_id = c.user_id\n" +
                "LEFT JOIN employee_t e ON uh.host_id = e.host_id AND u.user_id = e.user_id\n" +
                "WHERE uh.host_id = ?\n");

        List<Object> parameters = new ArrayList<>();
        parameters.add(hostId);

        StringBuilder whereClause = new StringBuilder();

        addCondition(whereClause, parameters, "u.email", email);
        addCondition(whereClause, parameters, "u.language", language);
        addCondition(whereClause, parameters, "u.user_type", userType);
        addCondition(whereClause, parameters, "COALESCE(c.customer_id, e.employee_id)", entityId); // Using COALESCE here
        addCondition(whereClause, parameters, "c.referral_id", referralId);
        addCondition(whereClause, parameters, "e.manager_id", managerId);
        addCondition(whereClause, parameters, "u.first_name", firstName);
        addCondition(whereClause, parameters, "u.last_name", lastName);
        addCondition(whereClause, parameters, "u.phone_number", phoneNumber);
        addCondition(whereClause, parameters, "u.gender", gender);
        addCondition(whereClause, parameters, "u.birthday", birthday);
        addCondition(whereClause, parameters, "u.country", country);
        addCondition(whereClause, parameters, "u.province", province);
        addCondition(whereClause, parameters, "u.city", city);
        addCondition(whereClause, parameters, "u.address", address);
        addCondition(whereClause, parameters, "u.post_code", postCode);
        addCondition(whereClause, parameters, "u.verified", verified);
        addCondition(whereClause, parameters, "u.locked", locked);

        if (whereClause.length() > 0) {
            sqlBuilder.append("AND ").append(whereClause);
        }

        sqlBuilder.append(" ORDER BY u.last_name\n" +
                "LIMIT ? OFFSET ?");

        parameters.add(limit);
        parameters.add(offset);

        String sql = sqlBuilder.toString();
        if(logger.isDebugEnabled()) logger.debug("sql = {}", sql);
        int total = 0;
        List<Map<String, Object>> users = new ArrayList<>();

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
                    map.put("hostId", resultSet.getString("host_id"));
                    map.put("userId", resultSet.getString("user_id"));
                    map.put("email", resultSet.getString("email"));
                    map.put("language", resultSet.getString("language"));
                    map.put("userType", resultSet.getString("user_type"));
                    map.put("firstName", resultSet.getString("first_name"));
                    map.put("lastName", resultSet.getString("last_name"));
                    map.put("phoneNumber", resultSet.getString("phone_number"));
                    map.put("gender", resultSet.getString("gender"));
                    // handling date properly
                    map.put("birthday", resultSet.getDate("birthday") != null ? resultSet.getDate("birthday").toString() : null);
                    map.put("country", resultSet.getString("country"));
                    map.put("province", resultSet.getString("province"));
                    map.put("city", resultSet.getString("city"));
                    map.put("address", resultSet.getString("address"));
                    map.put("postCode", resultSet.getString("post_code"));
                    map.put("verified", resultSet.getBoolean("verified"));
                    map.put("locked", resultSet.getBoolean("locked"));
                    map.put("entityId", resultSet.getString("entity_id"));
                    map.put("referralId", resultSet.getString("referral_id"));
                    map.put("managerId", resultSet.getString("manager_id"));

                    users.add(map);
                }
            }

            Map<String, Object> resultMap = new HashMap<>();
            resultMap.put("total", total);
            resultMap.put("users", users);
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
    public Result<String> queryEmailByWallet(String cryptoType, String cryptoAddress) {
        Result<String> result = null;
        String sql = "SELECT email \n" +
                "FROM user_t u, user_crypto_wallet_t w \n" +
                "WHERE u.user_id = w.user_id\n" +
                "AND w.crypto_type = ?\n" +
                "AND w.crypto_address = ?\n";
        try (final Connection conn = ds.getConnection()) {
            String email = null;
            try (PreparedStatement statement = conn.prepareStatement(sql)) {
                statement.setString(1, cryptoType);
                statement.setString(2, cryptoAddress);
                try (ResultSet resultSet = statement.executeQuery()) {
                    if (resultSet.next()) {
                        email = resultSet.getString("email");
                    }
                }
            }
            if (email == null)
                result = Failure.of(new Status(OBJECT_NOT_FOUND, "user email", cryptoType + cryptoAddress));
            else
                result = Success.of(email);
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
     * insert notification into database using different connection and transaction.
     *
     * @param eventId The eventId of the event
     * @param eventClass The event class of the notification
     * @param json   The json string of the event
     * @param flag   The flag of the notification
     * @param error  The error message of the notification
     * @throws SQLException when there is an error in the database access
     */
    public void insertNotification(EventId eventId, String eventClass, String json, boolean flag, String error) throws SQLException {
        try (Connection conn = ds.getConnection();
            PreparedStatement statement = conn.prepareStatement(INSERT_NOTIFICATION)) {
            statement.setString(1, eventId.getId());
            statement.setLong(2, eventId.getNonce());
            statement.setString(3, eventClass);
            statement.setString(4, json);
            statement.setTimestamp(5, new Timestamp(eventId.getTimestamp()));
            statement.setBoolean(6, flag);
            if (error != null) {
                statement.setString(7, error);
            } else {
                statement.setNull(7, NULL);
            }
            statement.executeUpdate();
        } catch (SQLException e) {
            logger.error("SQLException:", e);
            throw new RuntimeException(e);
        }
    }

    /**
     * check the email, user_id, is unique. if not, write an error notification. If yes, insert
     * the user into database and write a success notification.
     *
     * @param event event that is created by user service
     * @return result of email
     */
    @Override
    public Result<String> createUser(UserCreatedEvent event) {
        final String queryEmailEntityId = "SELECT\n" +
                "    u.user_id,\n" +
                "    u.email,\n" +
                "    COALESCE(c.customer_id, e.employee_id) AS entity_id\n" +
                "FROM\n" +
                "    user_t u\n" +
                "LEFT JOIN\n" +
                "    user_host_t uh ON u.user_id = uh.user_id\n" +
                "LEFT JOIN\n" +
                "    customer_t c ON uh.host_id = c.host_id AND u.user_id = c.user_id\n" +
                "LEFT JOIN\n" +
                "    employee_t e ON uh.host_id = e.host_id AND u.user_id = e.user_id\n" +
                "WHERE\n" +
                "    (u.email = ? OR COALESCE(c.customer_id, e.employee_id) = ?)\n" +
                "    AND u.user_type IN ('C', 'E')";

        final String insertUser = "INSERT INTO user_t (user_id, email, password, language, first_name, " +
                "last_name, user_type, phone_number, gender, birthday, " +
                "country, province, city, address, post_code, " +
                "verified, token, locked) " +
                "VALUES (?, ?, ?, ?, ?,   ?, ?, ?, ?, ?,   ?, ?, ?, ?, ?,  ?, ?, ?)";
        final String insertUserHost = "INSERT INTO user_host_t (user_id, host_id) VALUES (?, ?)";
        final String insertCustomer = "INSERT INTO customer_t (host_id, customer_id, user_id, referral_id) " +
                "VALUES (?, ?, ?, ?)";
        final String insertEmployee = "INSERT INTO employee_t (host_id, employee_id, user_id, manager_id) " +
                "VALUES (?, ?, ?, ?)";

        Result<String> result = null;
        Map<String, Object> map = JsonMapper.string2Map(event.getValue());
        try (Connection conn = ds.getConnection()){
            conn.setAutoCommit(false);
            try {
                try (PreparedStatement statement = conn.prepareStatement(queryEmailEntityId)) {
                    statement.setString(1, event.getEmail());
                    statement.setString(2, event.getEntityId());
                    try (ResultSet resultSet = statement.executeQuery()) {
                        if (resultSet.next()) {
                            // found duplicate record, write an error notification.
                            logger.error("entityId {} or email {} already exists in database.", event.getEntityId(), event.getEmail());
                            throw new SQLException(String.format("entityId %s or email %s already exists in database.", event.getEntityId(), event.getEmail()));
                        }
                    }
                }

                // no duplicate record, insert the user into database and write a success notification.
                try (PreparedStatement statement = conn.prepareStatement(insertUser)) {
                    statement.setString(1, event.getUserId());
                    statement.setString(2, event.getEmail());
                    statement.setString(3, event.getPassword());
                    statement.setString(4, event.getLanguage());
                    if (map.get("first_name") != null)
                        statement.setString(5, (String) map.get("first_name"));
                    else
                        statement.setNull(5, NULL);

                    if (map.get("last_name") != null)
                        statement.setString(6, (String) map.get("last_name"));
                    else
                        statement.setNull(6, NULL);

                    statement.setString(7, event.getUserType());

                    if (map.get("phone_number") != null)
                        statement.setString(8, (String) map.get("phone_number"));
                    else
                        statement.setNull(8, NULL);

                    if (map.get("gender") != null) {
                        statement.setString(9, (String) map.get("gender"));
                    } else {
                        statement.setNull(9, NULL);
                    }
                    java.util.Date birthday = (java.util.Date) map.get("birthday");
                    if (birthday != null) {
                        statement.setDate(10, new java.sql.Date(birthday.getTime()));
                    } else {
                        statement.setNull(10, NULL);
                    }
                    Object countryObject = event.get("country");
                    if (countryObject != null) {
                        statement.setString(11, (String) countryObject);
                    } else {
                        statement.setNull(11, NULL);
                    }
                    Object provinceObject = event.get("province");
                    if (provinceObject != null) {
                        statement.setString(12, (String) provinceObject);
                    } else {
                        statement.setNull(12, NULL);
                    }
                    Object cityObject = event.get("city");
                    if (cityObject != null) {
                        statement.setString(13, (String) cityObject);
                    } else {
                        statement.setNull(13, NULL);
                    }
                    Object addressObject = map.get("address");
                    if (addressObject != null) {
                        statement.setString(14, (String) addressObject);
                    } else {
                        statement.setNull(14, NULL);
                    }
                    Object postCodeObject = map.get("post_code");
                    if (postCodeObject != null) {
                        statement.setString(15, (String) postCodeObject);
                    } else {
                        statement.setNull(15, NULL);
                    }
                    statement.setBoolean(16, event.getVerified());
                    statement.setString(17, event.getToken());
                    statement.setBoolean(18, event.getLocked());
                    statement.execute();
                }
                try (PreparedStatement statement = conn.prepareStatement(insertUserHost)) {
                    statement.setString(1, event.getUserId());
                    statement.setString(2, event.getHostId());
                    statement.execute();
                }
                // insert customer or employee based on user_type
                if(event.getUserType().equals("E")) {
                    try (PreparedStatement statement = conn.prepareStatement(insertEmployee)) {
                        statement.setString(1, event.getHostId());
                        statement.setString(2, event.getEntityId());
                        statement.setString(3, event.getUserId());
                        if(map.get("manager_id") != null) {
                            statement.setString(4, (String) map.get("manager_id"));
                        } else {
                            statement.setNull(4, NULL);
                        }
                        statement.execute();
                    }
                } else if(event.getUserType().equals("C")) {
                    try (PreparedStatement statement = conn.prepareStatement(insertCustomer)) {
                        statement.setString(1, event.getHostId());
                        statement.setString(2, event.getEntityId());
                        statement.setString(3, event.getUserId());
                        if(map.get("referral_id") != null) {
                            statement.setString(4, (String) map.get("referral_id"));
                        } else {
                            statement.setNull(4, NULL);
                        }
                        statement.execute();
                    }
                } else {
                    throw new SQLException("user_type is not valid: " + event.getUserType());
                }
                conn.commit();
                result = Success.of(event.getUserId());
                insertNotification(event.getEventId(), event.getClass().getName(), AvroConverter.toJson(event, false), true, null);
            } catch (SQLException e) {
                logger.error("SQLException:", e);
                conn.rollback();
                insertNotification(event.getEventId(), event.getClass().getName(), AvroConverter.toJson(event, false), false, e.getMessage());
                result = Failure.of(new Status(SQL_EXCEPTION, e.getMessage()));
            } catch (Exception e) {
                logger.error("Exception:", e);
                conn.rollback();
                insertNotification(event.getEventId(), event.getClass().getName(), AvroConverter.toJson(event, false), false, e.getMessage());
                result = Failure.of(new Status(GENERIC_EXCEPTION, e.getMessage()));
            }
        } catch (SQLException e) {
            logger.error("SQLException:", e);
            result = Failure.of(new Status(SQL_EXCEPTION, e.getMessage()));
        }
        return result;
    }


    @Override
    public Result<Integer> queryNonceByUserId(String userId){
        final String updateNonceSql = "UPDATE user_t SET nonce = nonce + 1 WHERE user_id = ? RETURNING nonce;";
        Result<Integer> result = null;
        try (Connection connection = ds.getConnection();
             PreparedStatement statement = connection.prepareStatement(updateNonceSql)) {
            Integer nonce = null;
            statement.setString(1, userId);
            try (ResultSet resultSet = statement.executeQuery()) {
                if(resultSet.next()){
                    nonce = resultSet.getInt(1);
                }
            }
            if (nonce == null)
                result = Failure.of(new Status(OBJECT_NOT_FOUND, "user nonce", userId));
            else
                result = Success.of(nonce);

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
     * check the input token with the saved token in user_t table to ensure match. If matched, update the verified to true
     * and nonce in the user_t table and a success notification. If not matched, write an error notification.
     *
     * @param event event that is created by user service
     * @return result of email
     */
    @Override
    public Result<String> confirmUser(UserConfirmedEvent event) {
        final String queryTokenByEmail = "SELECT token FROM user_t WHERE user_id = ? AND token = ?";
        final String updateUserByEmail = "UPDATE user_t SET token = null, verified = true, nonce = ? WHERE user_id = ?";
        Result<String> result;
        try (Connection conn = ds.getConnection()){
            conn.setAutoCommit(false);
            try (PreparedStatement statement = conn.prepareStatement(queryTokenByEmail)) {
                statement.setString(1, event.getEventId().getId());
                statement.setString(2, event.getToken());
                try (ResultSet resultSet = statement.executeQuery()) {
                    if (resultSet.next()) {
                        // found the token record, update user_t for token, verified flog and nonce, write a success notification.
                        try (PreparedStatement updateStatement = conn.prepareStatement(updateUserByEmail)) {
                            updateStatement.setLong(1, event.getEventId().getNonce() + 1);
                            updateStatement.setString(2, event.getEventId().getId());
                            updateStatement.execute();
                        }
                    } else {
                        // record is not found with the email and token. write an error notification.
                        throw new SQLException(String.format("token %s is not matched for userId %s.", event.getToken(), event.getEventId().getId()));
                    }
                }
                conn.commit();
                result = Success.of(event.getEventId().getId());
                insertNotification(event.getEventId(), event.getClass().getName(), AvroConverter.toJson(event, false), true, null);
            } catch (SQLException e) {
                logger.error("SQLException:", e);
                conn.rollback();
                insertNotification(event.getEventId(), event.getClass().getName(), AvroConverter.toJson(event, false), false, e.getMessage());
                result = Failure.of(new Status(SQL_EXCEPTION, e.getMessage()));
            } catch (Exception e) {
                logger.error("Exception:", e);
                conn.rollback();
                insertNotification(event.getEventId(), event.getClass().getName(), AvroConverter.toJson(event, false), false, e.getMessage());
                result = Failure.of(new Status(GENERIC_EXCEPTION, e.getMessage()));
            }
        } catch (SQLException e) {
            logger.error("SQLException:", e);
            result = Failure.of(new Status(SQL_EXCEPTION, e.getMessage()));
        }
        return result;
    }

    /**
     * Update the verified to true and nonce in the user_t table based on the hostId and userId. Write a success notification.
     *
     * @param event UserVerifiedEvent
     * @return  Result of userId
     */
    @Override
    public Result<String> verifyUser(UserVerifiedEvent event) {
        final String updateUserByUserId = "UPDATE user_t SET token = null, verified = true WHERE user_id = ?";
        Result<String> result;
        try (Connection conn = ds.getConnection()){
            conn.setAutoCommit(false);
            try (PreparedStatement statement = conn.prepareStatement(updateUserByUserId)) {
                statement.setString(1, event.getUserId());
                statement.execute();
                conn.commit();
                result = Success.of(event.getUserId());
                insertNotification(event.getEventId(), event.getClass().getName(), AvroConverter.toJson(event, false), true, null);
            } catch (SQLException e) {
                logger.error("SQLException:", e);
                conn.rollback();
                insertNotification(event.getEventId(), event.getClass().getName(), AvroConverter.toJson(event, false), false, e.getMessage());
                result = Failure.of(new Status(SQL_EXCEPTION, e.getMessage()));
            } catch (Exception e) {
                logger.error("Exception:", e);
                conn.rollback();
                insertNotification(event.getEventId(), event.getClass().getName(), AvroConverter.toJson(event, false), false, e.getMessage());
                result = Failure.of(new Status(GENERIC_EXCEPTION, e.getMessage()));
            }
        } catch (SQLException e) {
            logger.error("SQLException:", e);
            result = Failure.of(new Status(SQL_EXCEPTION, e.getMessage()));
        }
        return result;

    }

    /**
     * check the email, user_id is unique. if not, write an error notification. If yes, insert
     * the user into database and write a success notification.
     *
     * @param event event that is created by user service
     * @return result of email
     */
    @Override
    public Result<String> createSocialUser(SocialUserCreatedEvent event) {
        final String queryIdEmail = "SELECT nonce FROM user_t WHERE user_id = ? OR email = ?";
        final String insertUser = "INSERT INTO user_t (host_id, user_id, first_name, last_name, email, roles, language, " +
                "verified, gender, birthday, country, province, city, post_code, address) " +
                "VALUES (?, ?, ?, ?, ?,   ?, ?, ?, ?, ?,   ?, ?, ?, ?, ?)";
        Result<String> result = null;
        Map<String, Object> map = JsonMapper.string2Map(event.getValue());
        try(Connection conn = ds.getConnection()) {
            conn.setAutoCommit(false);
            try {
                try (PreparedStatement statement = conn.prepareStatement(queryIdEmail)) {
                    statement.setString(1, event.getUserId());
                    statement.setString(2, event.getEmail());
                    try (ResultSet resultSet = statement.executeQuery()) {
                        if (resultSet.next()) {
                            // found duplicate record, write an error notification.
                            throw new SQLException(String.format("userId %s or email %s already exists in database.", event.getUserId(), event.getEmail()));
                        }
                    }
                }
                // no duplicate record, insert the user into database and write a success notification.
                try (PreparedStatement statement = conn.prepareStatement(insertUser)) {
                    statement.setString(1, event.getHostId());
                    statement.setString(2, event.getUserId());
                    if (map.get("first_name") != null)
                        statement.setString(3, (String) map.get("first_name"));
                    else
                        statement.setNull(3, NULL);
                    if (map.get("last_name") != null)
                        statement.setString(4, (String) map.get("last_name"));
                    else
                        statement.setNull(4, NULL);
                    statement.setString(5, event.getEmail());
                    statement.setString(6, event.getRoles());
                    statement.setString(7, event.getLanguage());
                    statement.setBoolean(8, event.getVerified());
                    if (map.get("gender") != null) {
                        statement.setString(9, (String) map.get("gender"));
                    } else {
                        statement.setNull(9, NULL);
                    }
                    java.util.Date birthday = (java.util.Date) map.get("birthday");
                    if (birthday != null) {
                        statement.setDate(10, new java.sql.Date(birthday.getTime()));
                    } else {
                        statement.setNull(10, NULL);
                    }
                    Object countryObject = map.get("country");
                    if (countryObject != null) {
                        statement.setString(11, (String) countryObject);
                    } else {
                        statement.setNull(11, NULL);
                    }
                    Object provinceObject = map.get("province");
                    if (provinceObject != null) {
                        statement.setString(12, (String) provinceObject);
                    } else {
                        statement.setNull(12, NULL);
                    }
                    Object cityObject = map.get("city");
                    if (cityObject != null) {
                        statement.setString(13, (String) cityObject);
                    } else {
                        statement.setNull(13, NULL);
                    }
                    Object postCodeObject = map.get("post_code");
                    if (postCodeObject != null) {
                        statement.setString(14, (String) postCodeObject);
                    } else {
                        statement.setNull(14, NULL);
                    }
                    Object addressObject = map.get("address");
                    if (addressObject != null) {
                        statement.setString(15, (String) addressObject);
                    } else {
                        statement.setNull(15, NULL);
                    }
                    statement.execute();
                }
                conn.commit();
                result = Success.of(event.getUserId());
                insertNotification(event.getEventId(), event.getClass().getName(), AvroConverter.toJson(event, false), true, null);
            } catch (SQLException e) {
                logger.error("SQLException:", e);
                conn.rollback();
                insertNotification(event.getEventId(), event.getClass().getName(), AvroConverter.toJson(event, false), false, e.getMessage());
                result = Failure.of(new Status(SQL_EXCEPTION, e.getMessage()));
            } catch (Exception e) {
                logger.error("Exception:", e);
                conn.rollback();
                insertNotification(event.getEventId(), event.getClass().getName(), AvroConverter.toJson(event, false), false, e.getMessage());
                result = Failure.of(new Status(GENERIC_EXCEPTION, e.getMessage()));
            }
        } catch (SQLException e) {
            logger.error("SQLException:", e);
            result = Failure.of(new Status(SQL_EXCEPTION, e.getMessage()));
        }
        return result;
    }

    /**
     * update user if it exists in database.
     *
     * @param event event that is created by user service
     * @return result of email
     */
    @Override
    public Result<String> updateUser(UserUpdatedEvent event) {
        final String updateUser = "UPDATE user_t SET language = ?, first_name = ?, last_name = ?, phone_number = ?," +
                "gender = ?, birthday = ?, country = ?, province = ?, city = ?, address = ?, post_code = ? " +
                "WHERE user_id = ?";
        final String updateCustomer = "UPDATE customer_t SET referral_id = ? WHERE host_id = ? AND customer_id = ?";
        final String updateEmployee = "UPDATE employee_t SET manager_id = ? WHERE host_id = ? AND employee_id = ?";
        Result<String> result = null;
        Map<String, Object> map = JsonMapper.string2Map(event.getValue());
        try (Connection conn = ds.getConnection()){
            conn.setAutoCommit(false);

            // no duplicate record, insert the user into database and write a success notification.
            try (PreparedStatement statement = conn.prepareStatement(updateUser)) {
                statement.setString(1, event.getLanguage());
                if (map.get("first_name") != null)
                    statement.setString(2, (String) map.get("first_name"));
                else
                    statement.setNull(2, NULL);
                if (map.get("last_name") != null)
                    statement.setString(3, (String) map.get("last_name"));
                else
                    statement.setNull(3, NULL);

                if (map.get("phone_number") != null)
                    statement.setString(4, (String) map.get("phone_number"));
                else
                    statement.setNull(4, NULL);

                if(map.get("gender") != null) {
                    statement.setString(5, (String) map.get("gender"));
                } else {
                    statement.setNull(5, NULL);
                }

                java.util.Date birthday = (java.util.Date) map.get("birthday");
                if (birthday != null) {
                    statement.setDate(6, new java.sql.Date(birthday.getTime()));
                } else {
                    statement.setNull(6, NULL);
                }

                String countryObject = event.getCountry();
                if (countryObject != null) {
                    statement.setString(7, countryObject);
                } else {
                    statement.setNull(7, NULL);
                }

                String provinceObject = event.getProvince();
                if (provinceObject != null) {
                    statement.setString(8, provinceObject);
                } else {
                    statement.setNull(8, NULL);
                }

                String cityObject = event.getCity();
                if (cityObject != null) {
                    statement.setString(9, cityObject);
                } else {
                    statement.setNull(9, NULL);
                }

                Object addressObject = map.get("address");
                if (addressObject != null) {
                    statement.setString(10, (String) addressObject);
                } else {
                    statement.setNull(10, NULL);
                }

                Object postCodeObject = map.get("post_code");
                if (postCodeObject != null) {
                    statement.setString(11, (String) postCodeObject);
                } else {
                    statement.setNull(11, NULL);
                }
                statement.setString(12, event.getUserId());
                int count = statement.executeUpdate();
                if (count == 0) {
                    // no record is updated, write an error notification.
                    throw new SQLException(String.format("no record is updated by userId %s", event.getUserId()));
                }
                // TODO there are old country, province and city in the event for maproot, so we need to update them
                // update customer or employee based on user_type
                if(event.getUserType().equals("E")) {
                    try (PreparedStatement updateStatement = conn.prepareStatement(updateEmployee)) {
                        if(map.get("manager_id") != null) {
                            updateStatement.setString(1, (String) map.get("manager_id"));
                        } else {
                            updateStatement.setNull(1, NULL);
                        }
                        updateStatement.setString(2, event.getHostId());
                        updateStatement.setString(3, event.getEntityId());
                        updateStatement.execute();
                    }
                } else if(event.getUserType().equals("C")) {
                    try (PreparedStatement updateStatement = conn.prepareStatement(updateCustomer)) {
                        if(map.get("referral_id") != null) {
                            updateStatement.setString(1, (String) map.get("referral_id"));
                        } else {
                            updateStatement.setNull(1, NULL);
                        }
                        updateStatement.setString(2, event.getHostId());
                        updateStatement.setString(3, event.getEntityId());
                        updateStatement.execute();
                    }
                } else {
                    throw new SQLException("user_type is not valid: " + event.getUserType());
                }
                conn.commit();
                if(logger.isTraceEnabled()) logger.trace("update user success: {}", event.getUserId());
                result = Success.of(event.getUserId());
                insertNotification(event.getEventId(), event.getClass().getName(), AvroConverter.toJson(event, false), true, null);
            } catch (SQLException e) {
                logger.error("SQLException:", e);
                conn.rollback();
                insertNotification(event.getEventId(), event.getClass().getName(), AvroConverter.toJson(event, false), false, e.getMessage());
                result = Failure.of(new Status(SQL_EXCEPTION, e.getMessage()));
            } catch (Exception e) {
                logger.error("Exception:", e);
                conn.rollback();
                insertNotification(event.getEventId(), event.getClass().getName(), AvroConverter.toJson(event, false), false, e.getMessage());
                result = Failure.of(new Status(GENERIC_EXCEPTION, e.getMessage()));
            }
        } catch (SQLException e) {
            logger.error("SQLException:", e);
            result = Failure.of(new Status(SQL_EXCEPTION, e.getMessage()));
        }
        return result;
    }

    /**
     * delete user from user_t table and all other tables related to this user.
     *
     * @param event event that is created by user service
     * @return result of email
     */
    @Override
    public Result<String> deleteUser(UserDeletedEvent event) {
        // delete only user_t, other tables will be cacade deleted by database
        final String deleteUserById = "DELETE from user_t WHERE user_id = ?";
        Result<String> result;
        try (Connection conn = ds.getConnection()) {
            conn.setAutoCommit(false);
            try (PreparedStatement statement = conn.prepareStatement(deleteUserById)) {
                statement.setString(1, event.getUserId());
                int count = statement.executeUpdate();
                if (count == 0) {
                    // no record is deleted, write an error notification.
                    throw new SQLException(String.format("no record is deleted by userId %s", event.getUserId()));
                }
                conn.commit();
                result = Success.of(event.getEventId().getId());
                insertNotification(event.getEventId(), event.getClass().getName(), AvroConverter.toJson(event, false), true, null);
            } catch (SQLException e) {
                logger.error("SQLException:", e);
                conn.rollback();
                insertNotification(event.getEventId(), event.getClass().getName(), AvroConverter.toJson(event, false), false, e.getMessage());
                result = Failure.of(new Status(SQL_EXCEPTION, e.getMessage()));
            } catch (Exception e) {
                logger.error("Exception:", e);
                conn.rollback();
                insertNotification(event.getEventId(), event.getClass().getName(), AvroConverter.toJson(event, false), false, e.getMessage());
                result = Failure.of(new Status(GENERIC_EXCEPTION, e.getMessage()));
            }
        } catch (SQLException e) {
            logger.error("SQLException:", e);
            result = Failure.of(new Status(SQL_EXCEPTION, e.getMessage()));
        }
        return result;
    }

    /**
     * update user roles by email in user_t table
     *
     * @param event event that is created by user service
     * @return result of email
     */
    @Override
    public Result<String> updateUserRoles(UserRolesUpdatedEvent event) {
        final String deleteUserByEmail = "UPDATE user_t SET roles = ?, nonce = ? WHERE email = ?";
        Result<String> result;
        try (Connection conn = ds.getConnection()) {
            conn.setAutoCommit(false);
            try (PreparedStatement statement = conn.prepareStatement(deleteUserByEmail)) {
                statement.setString(1, event.getRoles());
                statement.setLong(2, event.getEventId().getNonce() + 1);
                statement.setString(3, event.getEmail());
                int count = statement.executeUpdate();
                if (count == 0) {
                    // no record is deleted, write an error notification.
                    throw new SQLException(String.format("no roles is updated by email %s", event.getEmail()));
                }
                conn.commit();
                result = Success.of(event.getEventId().getId());
                insertNotification(event.getEventId(), event.getClass().getName(), AvroConverter.toJson(event, false), true, null);
            } catch (SQLException e) {
                logger.error("SQLException:", e);
                conn.rollback();
                insertNotification(event.getEventId(), event.getClass().getName(), AvroConverter.toJson(event, false), false, e.getMessage());
                result = Failure.of(new Status(SQL_EXCEPTION, e.getMessage()));
            } catch (Exception e) {
                logger.error("Exception:", e);
                conn.rollback();
                insertNotification(event.getEventId(), event.getClass().getName(), AvroConverter.toJson(event, false), false, e.getMessage());
                result = Failure.of(new Status(GENERIC_EXCEPTION, e.getMessage()));
            }
        } catch (SQLException e) {
            logger.error("SQLException:", e);
            result = Failure.of(new Status(SQL_EXCEPTION, e.getMessage()));
        }
        return result;
    }

    /**
     * update user_t for the forget password token by email
     *
     * @param event event that is created by user service
     * @return result of email
     */
    @Override
    public Result<String> forgetPassword(PasswordForgotEvent event) {
        final String deleteUserByEmail = "UPDATE user_t SET token = ?, nonce = ? WHERE email = ?";
        Result<String> result;
        try (Connection conn = ds.getConnection()) {
            conn.setAutoCommit(false);
            try (PreparedStatement statement = conn.prepareStatement(deleteUserByEmail)) {
                statement.setString(1, event.getToken());
                statement.setLong(2, event.getEventId().getNonce() + 1);
                statement.setString(3, event.getEmail());
                int count = statement.executeUpdate();
                if (count == 0) {
                    // no record is deleted, write an error notification.
                    throw new SQLException(String.format("no token is updated by email %s", event.getEmail()));
                }
                conn.commit();
                result = Success.of(event.getEventId().getId());
                insertNotification(event.getEventId(), event.getClass().getName(), AvroConverter.toJson(event, false), true, null);
            } catch (SQLException e) {
                logger.error("SQLException:", e);
                conn.rollback();
                insertNotification(event.getEventId(), event.getClass().getName(), AvroConverter.toJson(event, false), false, e.getMessage());
                result = Failure.of(new Status(SQL_EXCEPTION, e.getMessage()));
            } catch (Exception e) {
                logger.error("Exception:", e);
                conn.rollback();
                insertNotification(event.getEventId(), event.getClass().getName(), AvroConverter.toJson(event, false), false, e.getMessage());
                result = Failure.of(new Status(GENERIC_EXCEPTION, e.getMessage()));
            }
        } catch (SQLException e) {
            logger.error("SQLException:", e);
            result = Failure.of(new Status(SQL_EXCEPTION, e.getMessage()));
        }
        return result;
    }

    /**
     * update user_t to reset the password by email
     *
     * @param event event that is created by user service
     * @return result of email
     */
    @Override
    public Result<String> resetPassword(PasswordResetEvent event) {
        final String deleteUserByEmail = "UPDATE user_t SET token = ?, nonce = ? WHERE email = ?";
        Result<String> result;
        try (Connection conn = ds.getConnection()) {
            conn.setAutoCommit(false);
            try (PreparedStatement statement = conn.prepareStatement(deleteUserByEmail)) {
                statement.setString(1, event.getToken());
                statement.setLong(2, event.getEventId().getNonce() + 1);
                statement.setString(3, event.getEmail());
                int count = statement.executeUpdate();
                if (count == 0) {
                    // no record is deleted, write an error notification.
                    throw new SQLException(String.format("no token is updated by email %s", event.getEmail()));
                }
                conn.commit();
                result = Success.of(event.getEventId().getId());
                insertNotification(event.getEventId(), event.getClass().getName(), AvroConverter.toJson(event, false), true, null);
            } catch (SQLException e) {
                logger.error("SQLException:", e);
                conn.rollback();
                insertNotification(event.getEventId(), event.getClass().getName(), AvroConverter.toJson(event, false), false, e.getMessage());
                result = Failure.of(new Status(SQL_EXCEPTION, e.getMessage()));
            } catch (Exception e) {
                logger.error("Exception:", e);
                conn.rollback();
                insertNotification(event.getEventId(), event.getClass().getName(), AvroConverter.toJson(event, false), false, e.getMessage());
                result = Failure.of(new Status(GENERIC_EXCEPTION, e.getMessage()));
            }
        } catch (SQLException e) {
            logger.error("SQLException:", e);
            result = Failure.of(new Status(SQL_EXCEPTION, e.getMessage()));
        }
        return result;
    }

    /**
     * update user_t to change the password by email
     *
     * @param event event that is created by user service
     * @return result of email
     */
    @Override
    public Result<String> changePassword(PasswordChangedEvent event) {
        final String updatePasswordByEmail = "UPDATE user_t SET password = ?, nonce = ? WHERE email = ? AND password = ?";
        Result<String> result;
        try (Connection conn = ds.getConnection()) {
            conn.setAutoCommit(false);
            try (PreparedStatement statement = conn.prepareStatement(updatePasswordByEmail)) {
                statement.setString(1, event.getPassword());
                statement.setLong(2, event.getEventId().getNonce() + 1);
                statement.setString(3, event.getEventId().getId());
                statement.setString(4, event.getOldPassword());
                int count = statement.executeUpdate();
                if (count == 0) {
                    // no record is updated, write an error notification.
                    throw new SQLException(String.format("no password is updated by email %s", event.getEventId().getId()));
                }
                conn.commit();
                result = Success.of(event.getEventId().getId());
                insertNotification(event.getEventId(), event.getClass().getName(), AvroConverter.toJson(event, false), true, null);
            } catch (SQLException e) {
                logger.error("SQLException:", e);
                conn.rollback();
                insertNotification(event.getEventId(), event.getClass().getName(), AvroConverter.toJson(event, false), false, e.getMessage());
                result = Failure.of(new Status(SQL_EXCEPTION, e.getMessage()));
            } catch (Exception e) {
                logger.error("Exception:", e);
                conn.rollback();
                insertNotification(event.getEventId(), event.getClass().getName(), AvroConverter.toJson(event, false), false, e.getMessage());
                result = Failure.of(new Status(GENERIC_EXCEPTION, e.getMessage()));
            }
        } catch (SQLException e) {
            logger.error("SQLException:", e);
            result = Failure.of(new Status(SQL_EXCEPTION, e.getMessage()));
        }
        return result;
    }

    @Override
    public Result<String> updatePayment(PaymentUpdatedEvent event) {
        return null;
    }

    @Override
    public Result<String> deletePayment(PaymentDeletedEvent event) {
        return null;
    }

    @Override
    public Result<String> createOrder(OrderCreatedEvent event) {
        return null;
    }

    @Override
    public Result<String> cancelOrder(OrderCancelledEvent event) {
        return null;
    }

    @Override
    public Result<String> deliverOrder(OrderDeliveredEvent event) {
        return null;
    }

    /**
     * send private message to user. Update the nonce of the from user and insert a message
     * to message_t table. Send a notification to the from user about the event processing result.
     *
     * @param event event that is created by user service
     * @return result of email
     */
    @Override
    public Result<String> sendPrivateMessage(PrivateMessageSentEvent event) {
        final String insertMessage = "INSERT INTO message_t (from_id, nonce, to_email, subject, content, send_time) VALUES (?, ?, ?, ?, ?, ?)";
        Result<String> result;
        try (Connection conn = ds.getConnection()) {
            conn.setAutoCommit(false);
            try (PreparedStatement statement = conn.prepareStatement(insertMessage)) {
                statement.setString(1, event.getFromId());
                statement.setLong(2, event.getEventId().getNonce());
                statement.setString(3, event.getToEmail());
                statement.setString(4, event.getSubject());
                statement.setString(5, event.getContent());
                statement.setTimestamp(6, new Timestamp(event.getEventId().getTimestamp()));
                statement.executeUpdate();

                conn.commit();
                result = Success.of(event.getEventId().getId());
                insertNotification(event.getEventId(), event.getClass().getName(), AvroConverter.toJson(event, false), true, null);
            } catch (SQLException e) {
                logger.error("SQLException:", e);
                conn.rollback();
                insertNotification(event.getEventId(), event.getClass().getName(), AvroConverter.toJson(event, false), false, e.getMessage());
                result = Failure.of(new Status(SQL_EXCEPTION, e.getMessage()));
            } catch (Exception e) {
                logger.error("Exception:", e);
                conn.rollback();
                insertNotification(event.getEventId(), event.getClass().getName(), AvroConverter.toJson(event, false), false, e.getMessage());
                result = Failure.of(new Status(GENERIC_EXCEPTION, e.getMessage()));
            }
        } catch (SQLException e) {
            logger.error("SQLException:", e);
            result = Failure.of(new Status(SQL_EXCEPTION, e.getMessage()));
        }
        return result;

    }

    @Override
    public Result<String> queryUserLabel(String hostId) {
        Result<String> result = null;
        String sql = "SELECT u.user_id, u.email FROM user_t u, user_host_t h WHERE u.user_id = h.user_id AND h.host_id = ?";
        List<Map<String, Object>> labels = new ArrayList<>();
        try (Connection connection = ds.getConnection();
             PreparedStatement preparedStatement = connection.prepareStatement(sql)) {
            preparedStatement.setString(1, hostId);
            try (ResultSet resultSet = preparedStatement.executeQuery()) {
                while (resultSet.next()) {
                    Map<String, Object> map = new HashMap<>();
                    map.put("id", resultSet.getString("user_id"));
                    map.put("label", resultSet.getString("email"));
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
    public Result<String> createRefreshToken(MarketTokenCreatedEvent event) {
        final String insertUser = "INSERT INTO refresh_token_t (refresh_token, host_id, user_id, client_id, scope, " +
                "user_type, roles, csrf, custom_claim) " +
                "VALUES (?, ?, ?, ?, ?,   ?, ?, ?, ?)";
        Result<String> result = null;
        Map<String, Object> map = JsonMapper.string2Map(event.getValue());
        try (Connection conn = ds.getConnection()) {
            conn.setAutoCommit(false);
            // no duplicate record, insert the user into database and write a success notification.
            try (PreparedStatement statement = conn.prepareStatement(insertUser)) {
                statement.setString(1, event.getRefreshToken());
                statement.setString(2, event.getHostId());
                statement.setString(3, event.getUserId());
                if (map.get("clientId") != null)
                    statement.setString(4, (String) map.get("clientId"));
                else
                    statement.setNull(4, NULL);

                if (map.get("scope") != null)
                    statement.setString(5, (String) map.get("scope"));
                else
                    statement.setNull(5, NULL);

                if (map.get("userType") != null)
                    statement.setString(6, (String) map.get("userType"));
                else
                    statement.setNull(6, NULL);

                if (map.get("roles") != null)
                    statement.setString(7, (String) map.get("roles"));
                else
                    statement.setNull(7, NULL);

                if (map.get("csrf") != null)
                    statement.setString(8, (String) map.get("csrf"));
                else
                    statement.setNull(8, NULL);

                if (map.get("customClaim") != null)
                    statement.setString(9, (String) map.get("customClaim"));
                else
                    statement.setNull(9, NULL);
                int count = statement.executeUpdate();
                if (count == 0) {
                    // no record is inserted, write an error notification.
                    throw new SQLException(String.format("no record is inserted for refresh token %s", event.getRefreshToken()));
                } else {
                }
                conn.commit();
                result = Success.of(event.getRefreshToken());
                insertNotification(event.getEventId(), event.getClass().getName(), AvroConverter.toJson(event, false), true, null);
            } catch (SQLException e) {
                logger.error("SQLException:", e);
                conn.rollback();
                insertNotification(event.getEventId(), event.getClass().getName(), AvroConverter.toJson(event, false), false, e.getMessage());
                result = Failure.of(new Status(SQL_EXCEPTION, e.getMessage()));
            } catch (Exception e) {
                logger.error("Exception:", e);
                conn.rollback();
                insertNotification(event.getEventId(), event.getClass().getName(), AvroConverter.toJson(event, false), false, e.getMessage());
                result = Failure.of(new Status(GENERIC_EXCEPTION, e.getMessage()));
            }
        } catch (SQLException e) {
            logger.error("SQLException:", e);
            result = Failure.of(new Status(SQL_EXCEPTION, e.getMessage()));
        }
        return result;
    }

    @Override
    public Result<String> deleteRefreshToken(MarketTokenDeletedEvent event) {
        final String deleteApp = "DELETE from refresh_token_t WHERE refresh_token = ? AND host_id = ? AND user_id = ?";
        Result<String> result;
        try (Connection conn = ds.getConnection()) {
            conn.setAutoCommit(false);
            try (PreparedStatement statement = conn.prepareStatement(deleteApp)) {
                statement.setString(1, event.getRefreshToken());
                statement.setString(2, event.getHostId());
                statement.setString(3, event.getUserId());
                int count = statement.executeUpdate();
                if (count == 0) {
                    // no record is deleted, write an error notification.
                    throw new SQLException(String.format("no record is deleted for refresh token %s", event.getRefreshToken()));
                }
                conn.commit();
                result = Success.of(event.getEventId().getId());
                insertNotification(event.getEventId(), event.getClass().getName(), AvroConverter.toJson(event, false), true, null);
            } catch (SQLException e) {
                logger.error("SQLException:", e);
                conn.rollback();
                insertNotification(event.getEventId(), event.getClass().getName(), AvroConverter.toJson(event, false), false, e.getMessage());
                result = Failure.of(new Status(SQL_EXCEPTION, e.getMessage()));
            } catch (Exception e) {
                logger.error("Exception:", e);
                conn.rollback();
                insertNotification(event.getEventId(), event.getClass().getName(), AvroConverter.toJson(event, false), false, e.getMessage());
                result = Failure.of(new Status(GENERIC_EXCEPTION, e.getMessage()));
            }
        } catch (SQLException e) {
            logger.error("SQLException:", e);
            result = Failure.of(new Status(SQL_EXCEPTION, e.getMessage()));
        }
        return result;
    }

    @Override
    public Result<String> queryRefreshToken(String refreshToken) {
        Result<String> result = null;
        String sql =
                "SELECT refresh_token, host_id, user_id, client_id, scope, user_type, roles, csrf, custom_claim\n" +
                        "FROM refresh_token_t\n" +
                        "WHERE refresh_token = ?\n";
        try (final Connection conn = ds.getConnection()) {
            Map<String, Object> map = new HashMap<>();
            try (PreparedStatement statement = conn.prepareStatement(sql)) {
                statement.setString(1, refreshToken);
                try (ResultSet resultSet = statement.executeQuery()) {
                    if (resultSet.next()) {
                        map.put("refreshToken", resultSet.getString("refresh_token"));
                        map.put("hostId", resultSet.getString("host_id"));
                        map.put("userId", resultSet.getString("user_id"));
                        map.put("clientId", resultSet.getString("client_id"));
                        map.put("scope", resultSet.getString("scope"));
                        map.put("userType", resultSet.getString("user_type"));
                        map.put("roles", resultSet.getString("roles"));
                        map.put("csrf", resultSet.getString("csrf"));
                        map.put("customClaim", resultSet.getString("custom_claim"));
                    }
                }
            }
            if (map.isEmpty())
                result = Failure.of(new Status(OBJECT_NOT_FOUND, "refresh token", refreshToken));
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
    public Result<String> createClient(MarketClientCreatedEvent event) {
        final String insertUser = "INSERT INTO app_t (host_id, app_id, app_name, app_desc, " +
                "is_kafka_app, client_id, client_type, client_profile, client_secret, client_scope, custom_claim, " +
                "redirect_uri, authenticate_class, deref_client_id, operation_owner, delivery_owner, update_user, update_ts) " +
                "VALUES (?, ?, ?, ?, ?,   ?, ?, ?, ?, ?,   ?, ?, ?, ?, ?,   ?, ?, ?)";
        Result<String> result = null;
        Map<String, Object> map = JsonMapper.string2Map(event.getValue());
        try (Connection conn = ds.getConnection()) {
            conn.setAutoCommit(false);
            // no duplicate record, insert the user into database and write a success notification.
            try (PreparedStatement statement = conn.prepareStatement(insertUser)) {
                statement.setString(1, event.getHostId());
                statement.setString(2, event.getAppId());
                statement.setString(3, (String) map.get("appName"));
                if (map.get("appDesc") != null)
                    statement.setString(4, (String) map.get("appDesc"));
                else
                    statement.setNull(4, NULL);
                if (map.get("isKafkaApp") != null)
                    statement.setBoolean(5, (Boolean) map.get("isKafkaApp"));
                else
                    statement.setNull(5, NULL);
                statement.setString(6, (String) map.get("clientId"));
                statement.setString(7, (String) map.get("clientType"));
                statement.setString(8, (String) map.get("clientProfile"));
                statement.setString(9, (String) map.get("clientSecret"));
                if (map.get("clientScope") != null) {
                    statement.setString(10, (String) map.get("clientScope"));
                } else {
                    statement.setNull(10, NULL);
                }
                if (map.get("customClaim") != null) {
                    statement.setString(11, (String) map.get("customClaim"));
                } else {
                    statement.setNull(11, NULL);
                }
                if (map.get("redirectUri") != null) {
                    statement.setString(12, (String) map.get("redirectUri"));
                } else {
                    statement.setNull(12, NULL);
                }
                if (map.get("authenticateClass") != null) {
                    statement.setString(13, (String) map.get("authenticateClass"));
                } else {
                    statement.setNull(13, NULL);
                }
                if (map.get("derefClientId") != null) {
                    statement.setString(14, (String) map.get("derefClientId"));
                } else {
                    statement.setNull(14, NULL);
                }
                if (map.get("operationOwner") != null) {
                    statement.setString(15, (String) map.get("operationOwner"));
                } else {
                    statement.setNull(15, NULL);
                }
                if (map.get("deliveryOwner") != null) {
                    statement.setString(16, (String) map.get("deliveryOwner"));
                } else {
                    statement.setNull(16, NULL);
                }
                statement.setString(17, event.getEventId().getId());
                statement.setTimestamp(18, new Timestamp(System.currentTimeMillis()));
                int count = statement.executeUpdate();
                if (count == 0) {
                    throw new SQLException(String.format("no record is inserted for app %s", event.getAppId()));
                }
                conn.commit();
                result = Success.of(event.getAppId());
                insertNotification(event.getEventId(), event.getClass().getName(), AvroConverter.toJson(event, false), true, null);
            } catch (SQLException e) {
                logger.error("SQLException:", e);
                conn.rollback();
                insertNotification(event.getEventId(), event.getClass().getName(), AvroConverter.toJson(event, false), false, e.getMessage());
                result = Failure.of(new Status(SQL_EXCEPTION, e.getMessage()));
            } catch (Exception e) {
                logger.error("Exception:", e);
                conn.rollback();
                insertNotification(event.getEventId(), event.getClass().getName(), AvroConverter.toJson(event, false), false, e.getMessage());
                result = Failure.of(new Status(GENERIC_EXCEPTION, e.getMessage()));
            }
        } catch (SQLException e) {
            logger.error("SQLException:", e);
            result = Failure.of(new Status(SQL_EXCEPTION, e.getMessage()));
        }
        return result;
    }

    @Override
    public Result<String> updateClient(MarketClientUpdatedEvent event) {
        final String updateApplication = "UPDATE app_t SET app_name = ?, app_desc = ?, is_kafka_app = ?, " +
                "client_type = ?, client_profile = ?, client_scope = ?, custom_claim = ?, redirect_uri = ?, authenticate_class = ?, " +
                "deref_client_id = ?, operation_owner = ?, delivery_owner = ?, update_user = ?, update_ts = ? " +
                "WHERE host_id = ? AND app_id = ?";

        Result<String> result = null;
        Map<String, Object> map = JsonMapper.string2Map(event.getValue());
        try (Connection conn = ds.getConnection()) {
            conn.setAutoCommit(false);

            try (PreparedStatement statement = conn.prepareStatement(updateApplication)) {
                if (map.get("appName") != null) {
                    statement.setString(1, (String) map.get("appName"));
                } else {
                    statement.setNull(1, NULL);
                }
                if (map.get("appDesc") != null) {
                    statement.setString(2, (String) map.get("appDesc"));
                } else {
                    statement.setNull(2, NULL);
                }
                if (map.get("isKafkaApp") != null) {
                    statement.setBoolean(3, (Boolean) map.get("isKafkaApp"));
                } else {
                    statement.setNull(3, NULL);
                }
                if (map.get("clientType") != null) {
                    statement.setString(4, (String) map.get("clientType"));
                } else {
                    statement.setNull(4, NULL);
                }
                if (map.get("clientProfile") != null) {
                    statement.setString(5, (String) map.get("clientProfile"));
                } else {
                    statement.setNull(5, NULL);
                }
                if (map.get("clientScope") != null) {
                    statement.setString(6, (String) map.get("clientScope"));
                } else {
                    statement.setNull(6, NULL);
                }
                if (map.get("customClaim") != null)
                    statement.setString(7, (String) map.get("customClaim"));
                else
                    statement.setNull(7, NULL);
                if (map.get("redirectUri") != null)
                    statement.setString(8, (String) map.get("redirectUri"));
                else
                    statement.setNull(8, NULL);
                if (map.get("authenticateClass") != null) {
                    statement.setString(9, (String) map.get("authenticateClass"));
                } else {
                    statement.setNull(9, NULL);
                }
                if (map.get("derefClientId") != null) {
                    statement.setString(10, (String) map.get("derefClientId"));
                } else {
                    statement.setNull(10, NULL);
                }
                if (map.get("operationOwner") != null) {
                    statement.setString(11, (String) map.get("operationOwner"));
                } else {
                    statement.setNull(11, NULL);
                }
                if (map.get("deliveryOwner") != null) {
                    statement.setString(12, (String) map.get("deliveryOwner"));
                } else {
                    statement.setNull(12, NULL);
                }
                statement.setString(13, event.getEventId().getId());
                statement.setTimestamp(14, new Timestamp(System.currentTimeMillis()));
                statement.setString(15, event.getHostId());
                statement.setString(16, event.getAppId());

                int count = statement.executeUpdate();
                if (count == 0) {
                    // no record is updated, write an error notification.
                    throw new SQLException(String.format("no record is updated for app %s", event.getAppId()));
                }
                conn.commit();
                result = Success.of(event.getAppId());
                insertNotification(event.getEventId(), event.getClass().getName(), AvroConverter.toJson(event, false), true, null);
            } catch (SQLException e) {
                logger.error("SQLException:", e);
                conn.rollback();
                insertNotification(event.getEventId(), event.getClass().getName(), AvroConverter.toJson(event, false), false, e.getMessage());
                result = Failure.of(new Status(SQL_EXCEPTION, e.getMessage()));
            } catch (Exception e) {
                logger.error("Exception:", e);
                conn.rollback();
                insertNotification(event.getEventId(), event.getClass().getName(), AvroConverter.toJson(event, false), false, e.getMessage());
                result = Failure.of(new Status(GENERIC_EXCEPTION, e.getMessage()));
            }
        } catch (SQLException e) {
            logger.error("SQLException:", e);
            result = Failure.of(new Status(SQL_EXCEPTION, e.getMessage()));
        }
        return result;
    }

    @Override
    public Result<String> deleteClient(MarketClientDeletedEvent event) {
        final String deleteApp = "DELETE from app_t WHERE host_id = ? AND app_id = ?";
        // TODO delete all other tables related to this user.
        Result<String> result;
        try (Connection conn = ds.getConnection()) {
            conn.setAutoCommit(false);
            try (PreparedStatement statement = conn.prepareStatement(deleteApp)) {
                statement.setString(1, event.getHostId());
                statement.setString(2, event.getAppId());
                int count = statement.executeUpdate();
                if (count == 0) {
                    // no record is deleted, write an error notification.
                    throw new SQLException(String.format("no record is deleted for app %s", event.getAppId()));
                }
                conn.commit();
                result = Success.of(event.getEventId().getId());
                insertNotification(event.getEventId(), event.getClass().getName(), AvroConverter.toJson(event, false), true, null);
            } catch (SQLException e) {
                logger.error("SQLException:", e);
                conn.rollback();
                insertNotification(event.getEventId(), event.getClass().getName(), AvroConverter.toJson(event, false), false, e.getMessage());
                result = Failure.of(new Status(SQL_EXCEPTION, e.getMessage()));
            } catch (Exception e) {
                logger.error("Exception:", e);
                conn.rollback();
                insertNotification(event.getEventId(), event.getClass().getName(), AvroConverter.toJson(event, false), false, e.getMessage());
                result = Failure.of(new Status(GENERIC_EXCEPTION, e.getMessage()));
            }
        } catch (SQLException e) {
            logger.error("SQLException:", e);
            result = Failure.of(new Status(SQL_EXCEPTION, e.getMessage()));
        }
        return result;
    }

    @Override
    public Result<Map<String, Object>> queryClientByClientId(String clientId) {
        Result<Map<String, Object>> result;
        String sql =
                "SELECT host_id, app_id, app_name, app_desc, is_kafka_app, client_id, " +
                        "client_type, client_profile, client_secret, client_scope, custom_claim, redirect_uri, authenticate_class, " +
                        "deref_client_id, operation_owner, delivery_owner, update_user, update_ts " +
                        "FROM app_t WHERE client_id = ?";
        try (final Connection conn = ds.getConnection()) {
            Map<String, Object> map = new HashMap<>();
            try (PreparedStatement statement = conn.prepareStatement(sql)) {
                statement.setString(1, clientId);
                try (ResultSet resultSet = statement.executeQuery()) {
                    if (resultSet.next()) {
                        map.put("hostId", resultSet.getString("host_id"));
                        map.put("appId", resultSet.getString("app_id"));
                        map.put("appDesc", resultSet.getString("app_desc"));
                        map.put("isKafkaApp", resultSet.getBoolean("is_kafka_app"));
                        map.put("clientId", resultSet.getString("client_id"));
                        map.put("clientType", resultSet.getString("client_type"));
                        map.put("clientProfile", resultSet.getString("client_profile"));
                        map.put("clientSecret", resultSet.getString("client_secret"));
                        map.put("clientScope", resultSet.getString("client_scope"));
                        map.put("customClaim", resultSet.getString("custom_claim"));
                        map.put("redirectUri", resultSet.getString("redirect_uri"));
                        map.put("authenticateClass", resultSet.getString("authenticate_class"));
                        map.put("derefClientId", resultSet.getString("deref_client_id"));
                        map.put("operationOwner", resultSet.getString("operation_owner"));
                        map.put("deliveryOwner", resultSet.getString("delivery_owner"));
                        map.put("updateUser", resultSet.getString("update_user"));
                        map.put("updateTs", resultSet.getTimestamp("update_ts"));
                    }
                }
            }
            if (map.size() == 0)
                result = Failure.of(new Status(OBJECT_NOT_FOUND, "application with clientId ", clientId));
            else
                result = Success.of(map);
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
    public Result<Map<String, Object>> queryClientByHostAppId(String host_id, String applicationId) {
        Result<Map<String, Object>> result;
        String sql =
                "SELECT host_id, app_id, app_name, app_desc, is_kafka_app, client_id, " +
                        "client_type, client_profile, client_secret, client_scope, custom_claim, redirect_uri, authenticate_class, " +
                        "deref_client_id, operation_owner, delivery_owner, update_user, update_ts " +
                        "FROM app_t WHERE host = ? AND app_id = ?";
        try (final Connection conn = ds.getConnection()) {
            Map<String, Object> map = new HashMap<>();
            try (PreparedStatement statement = conn.prepareStatement(sql)) {
                statement.setString(1, host_id);
                statement.setString(2, applicationId);
                try (ResultSet resultSet = statement.executeQuery()) {
                    if (resultSet.next()) {
                        map.put("hostId", resultSet.getString("host_id"));
                        map.put("appId", resultSet.getString("app_id"));
                        map.put("appDesc", resultSet.getString("app_desc"));
                        map.put("isKafkaApp", resultSet.getBoolean("is_kafka_app"));
                        map.put("clientId", resultSet.getString("client_id"));
                        map.put("clientType", resultSet.getString("client_type"));
                        map.put("clientProfile", resultSet.getString("client_profile"));
                        map.put("clientSecret", resultSet.getString("client_secret"));
                        map.put("clientScope", resultSet.getString("client_scope"));
                        map.put("customClaim", resultSet.getString("custom_claim"));
                        map.put("redirectUri", resultSet.getString("redirect_uri"));
                        map.put("authenticateClass", resultSet.getString("authenticate_class"));
                        map.put("derefClientId", resultSet.getString("deref_client_id"));
                        map.put("operationOwner", resultSet.getString("operation_owner"));
                        map.put("deliveryOwner", resultSet.getString("delivery_owner"));
                        map.put("updateUser", resultSet.getString("update_user"));
                        map.put("updateTs", resultSet.getTimestamp("update_ts"));
                    }
                }
            }
            if (map.size() == 0)
                result = Failure.of(new Status(OBJECT_NOT_FOUND, "application with applicationId ", applicationId));
            else
                result = Success.of(map);
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
    public Result<String> createService(MarketServiceCreatedEvent event) {
        final String insertUser = "INSERT INTO api_t (host_id, api_id, api_name, " +
                "api_desc, operation_owner, delivery_owner, region, business_group, " +
                "lob, platform, capability, git_repo, api_tags, " +
                "api_status, update_user, update_ts) " +
                "VALUES (?, ?, ?, ?, ?,   ?, ?, ?, ?, ?,   ?, ?, ?, ?, ?,   ?)";
        Result<String> result = null;
        Map<String, Object> map = JsonMapper.string2Map(event.getValue());
        try (Connection conn = ds.getConnection()) {
            conn.setAutoCommit(false);
            // no duplicate record, insert the user into database and write a success notification.
            try (PreparedStatement statement = conn.prepareStatement(insertUser)) {
                statement.setString(1, event.getHostId());
                statement.setString(2, event.getApiId());
                statement.setString(3, (String) map.get("apiName"));
                if (map.get("apiDesc") != null)
                    statement.setString(4, (String) map.get("apiDesc"));
                else
                    statement.setNull(4, NULL);

                if (map.get("operationOwner") != null)
                    statement.setString(5, (String) map.get("operationOwner"));
                else
                    statement.setNull(5, NULL);

                if (map.get("deliveryOwner") != null)
                    statement.setString(6, (String) map.get("deliveryOwner"));
                else
                    statement.setNull(6, NULL);

                if (map.get("region") != null)
                    statement.setInt(7, (Integer) map.get("region"));
                else
                    statement.setNull(7, NULL);

                if (map.get("businessGroup") != null)
                    statement.setInt(8, (Integer) map.get("businessGroup"));
                else
                    statement.setNull(8, NULL);

                if (map.get("lob") != null)
                    statement.setInt(9, (Integer) map.get("lob"));
                else
                    statement.setNull(9, NULL);

                if (map.get("platform") != null)
                    statement.setInt(10, (Integer) map.get("platform"));
                else
                    statement.setNull(10, NULL);

                if (map.get("capability") != null)
                    statement.setInt(11, (Integer) map.get("capability"));
                else
                    statement.setNull(11, NULL);

                if (map.get("gitRepo") != null)
                    statement.setString(12, (String) map.get("gitRepo"));
                else
                    statement.setNull(12, NULL);

                if (map.get("apiTags") != null)
                    statement.setString(13, (String) map.get("apiTags"));
                else
                    statement.setNull(13, NULL);

                if (map.get("apiStatus") != null)
                    statement.setString(14, (String) map.get("apiStatus"));
                else
                    statement.setNull(14, NULL);

                statement.setString(15, event.getEventId().getId());
                statement.setTimestamp(16, new Timestamp(System.currentTimeMillis()));
                int count = statement.executeUpdate();
                if (count == 0) {
                    throw new SQLException(String.format("no record is inserted for api %s", event.getApiId()));
                }
                conn.commit();
                result = Success.of(event.getApiId());
                insertNotification(event.getEventId(), event.getClass().getName(), AvroConverter.toJson(event, false), true, null);
            } catch (SQLException e) {
                logger.error("SQLException:", e);
                conn.rollback();
                insertNotification(event.getEventId(), event.getClass().getName(), AvroConverter.toJson(event, false), false, e.getMessage());
                result = Failure.of(new Status(SQL_EXCEPTION, e.getMessage()));
            } catch (Exception e) {
                logger.error("Exception:", e);
                conn.rollback();
                result = Failure.of(new Status(GENERIC_EXCEPTION, e.getMessage()));
            }
        } catch (SQLException e) {
            logger.error("SQLException:", e);
            result = Failure.of(new Status(SQL_EXCEPTION, e.getMessage()));
        }
        return result;
    }

    @Override
    public Result<String> updateService(MarketServiceUpdatedEvent event) {
        final String updateApi = "UPDATE api_t SET api_name = ?, api_desc = ? " +
                "operation_owner = ?, delivery_owner = ?, region = ?, business_group = ?, lob = ?, platform = ?, " +
                "capability = ?, git_repo = ?, api_tags = ?, api_status = ?,  update_user = ?, update_ts = ? " +
                "WHERE host_id = ? AND api_id = ?";

        Result<String> result = null;
        Map<String, Object> map = JsonMapper.string2Map(event.getValue());
        try (Connection conn = ds.getConnection()) {
            conn.setAutoCommit(false);

            try (PreparedStatement statement = conn.prepareStatement(updateApi)) {

                if (map.get("apiName") != null) {
                    statement.setString(1, (String) map.get("apiName"));
                } else {
                    statement.setNull(1, NULL);
                }

                if (map.get("apiDesc") != null) {
                    statement.setString(2, (String) map.get("apiDesc"));
                } else {
                    statement.setNull(2, NULL);
                }

                if (map.get("operationOwner") != null) {
                    statement.setString(3, (String) map.get("operationOwner"));
                } else {
                    statement.setNull(3, NULL);
                }

                if (map.get("deliveryOwner") != null) {
                    statement.setString(4, (String) map.get("deliveryOwner"));
                } else {
                    statement.setNull(4, NULL);
                }

                if (map.get("region") != null)
                    statement.setInt(5, (Integer) map.get("region"));
                else
                    statement.setNull(5, NULL);

                if (map.get("businessGroup") != null)
                    statement.setInt(6, (Integer) map.get("businessGroup"));
                else
                    statement.setNull(6, NULL);

                if (map.get("lob") != null) {
                    statement.setInt(7, (Integer) map.get("lob"));
                } else {
                    statement.setNull(7, NULL);
                }
                if (map.get("platform") != null) {
                    statement.setInt(8, (Integer) map.get("platform"));
                } else {
                    statement.setNull(8, NULL);
                }
                if (map.get("capability") != null) {
                    statement.setInt(9, (Integer) map.get("capability"));
                } else {
                    statement.setNull(9, NULL);
                }
                if (map.get("gitRepo") != null) {
                    statement.setString(10, (String) map.get("gitRepo"));
                } else {
                    statement.setNull(10, NULL);
                }
                if (map.get("apiTags") != null) {
                    statement.setString(11, (String) map.get("apiTags"));
                } else {
                    statement.setNull(11, NULL);
                }
                if (map.get("apiStatus") != null) {
                    statement.setString(12, (String) map.get("apiStatus"));
                } else {
                    statement.setNull(12, NULL);
                }
                statement.setString(13, event.getEventId().getId());
                statement.setTimestamp(14, new Timestamp(event.getEventId().getTimestamp()));
                statement.setString(15, event.getHostId());
                statement.setString(16, event.getApiId());

                int count = statement.executeUpdate();
                if (count == 0) {
                    // no record is updated, write an error notification.
                    throw new SQLException(String.format("no record is updated for api %s", event.getApiId()));
                }
                conn.commit();
                result = Success.of(event.getApiId());
                insertNotification(event.getEventId(), event.getClass().getName(), AvroConverter.toJson(event, false), true, null);
            } catch (SQLException e) {
                logger.error("SQLException:", e);
                conn.rollback();
                insertNotification(event.getEventId(), event.getClass().getName(), AvroConverter.toJson(event, false), false, e.getMessage());
                result = Failure.of(new Status(SQL_EXCEPTION, e.getMessage()));
            } catch (Exception e) {
                logger.error("Exception:", e);
                conn.rollback();
                insertNotification(event.getEventId(), event.getClass().getName(), AvroConverter.toJson(event, false), false, e.getMessage());
                result = Failure.of(new Status(GENERIC_EXCEPTION, e.getMessage()));
            }
        } catch (SQLException e) {
            logger.error("SQLException:", e);
            result = Failure.of(new Status(SQL_EXCEPTION, e.getMessage()));
        }
        return result;

    }

    @Override
    public Result<String> deleteService(MarketServiceDeletedEvent event) {
        final String deleteApplication = "DELETE from api_t WHERE host_id = ? AND api_id = ?";
        Result<String> result;
        try (Connection conn = ds.getConnection()) {
            conn.setAutoCommit(false);
            try (PreparedStatement statement = conn.prepareStatement(deleteApplication)) {
                statement.setString(1, event.getHostId());
                statement.setString(2, event.getApiId());
                int count = statement.executeUpdate();
                if (count == 0) {
                    // no record is deleted, write an error notification.
                    throw new SQLException(String.format("no record is deleted for api %s", event.getApiId()));
                }
                conn.commit();
                result = Success.of(event.getEventId().getId());
                insertNotification(event.getEventId(), event.getClass().getName(), AvroConverter.toJson(event, false), true, null);
            } catch (SQLException e) {
                logger.error("SQLException:", e);
                conn.rollback();
                insertNotification(event.getEventId(), event.getClass().getName(), AvroConverter.toJson(event, false), false, e.getMessage());
                result = Failure.of(new Status(SQL_EXCEPTION, e.getMessage()));
            } catch (Exception e) {
                logger.error("Exception:", e);
                conn.rollback();
                insertNotification(event.getEventId(), event.getClass().getName(), AvroConverter.toJson(event, false), false, e.getMessage());
                result = Failure.of(new Status(GENERIC_EXCEPTION, e.getMessage()));
            }
        } catch (SQLException e) {
            logger.error("SQLException:", e);
            result = Failure.of(new Status(SQL_EXCEPTION, e.getMessage()));
        }
        return result;
    }

    @Override
    public Result<String> queryService(int offset, int limit, String hostId, String apiId, String apiName,
                                       String apiDesc, String operationOwner, String deliveryOwner, String region, String businessGroup,
                                       String lob, String platform, String capability, String gitRepo, String apiTags, String apiStatus) {
        Result<String> result = null;
        StringBuilder sqlBuilder = new StringBuilder();
        sqlBuilder.append("SELECT COUNT(*) OVER () AS total,\n" +
                "host_id, api_id, api_name,\n" +
                "api_desc, operation_owner, delivery_owner, region, business_group,\n" +
                "lob, platform, capability, git_repo, api_tags, api_status\n" +
                "FROM api_t\n" +
                "WHERE host_id = ?\n");


        List<Object> parameters = new ArrayList<>();
        parameters.add(hostId);

        StringBuilder whereClause = new StringBuilder();

        addCondition(whereClause, parameters, "api_id", apiId);
        addCondition(whereClause, parameters, "api_name", apiName);
        addCondition(whereClause, parameters, "api_desc", apiDesc);
        addCondition(whereClause, parameters, "operation_owner", operationOwner);
        addCondition(whereClause, parameters, "delivery_owner", deliveryOwner);
        addCondition(whereClause, parameters, "region", region);
        addCondition(whereClause, parameters, "business_group", businessGroup);
        addCondition(whereClause, parameters, "lob", lob);
        addCondition(whereClause, parameters, "platform", platform);
        addCondition(whereClause, parameters, "capability", capability);
        addCondition(whereClause, parameters, "git_repo", gitRepo);
        addCondition(whereClause, parameters, "api_tags", apiTags);
        addCondition(whereClause, parameters, "api_status", apiStatus);

        if (whereClause.length() > 0) {
            sqlBuilder.append("AND ").append(whereClause);
        }


        sqlBuilder.append(" ORDER BY api_id\n" +
                "LIMIT ? OFFSET ?");

        parameters.add(limit);
        parameters.add(offset);
        String sql = sqlBuilder.toString();
        int total = 0;
        List<Map<String, Object>> services = new ArrayList<>();

        try (Connection connection = ds.getConnection();
             PreparedStatement preparedStatement = connection.prepareStatement(sql)) {

            for (int i = 0; i < parameters.size(); i++) {
                preparedStatement.setObject(i + 1, parameters.get(i));
            }


            boolean isFirstRow = true;
            try (ResultSet resultSet = preparedStatement.executeQuery()) {
                while (resultSet.next()) {
                    Map<String, Object> map = new HashMap<>();
                    // only get the total once as it is the same for all rows.
                    if (isFirstRow) {
                        total = resultSet.getInt("total");
                        isFirstRow = false;
                    }

                    map.put("hostId", resultSet.getString("host_id"));
                    map.put("apiId", resultSet.getString("api_id"));
                    map.put("apiName", resultSet.getString("api_name"));
                    map.put("apiDesc", resultSet.getString("api_desc"));
                    map.put("operationOwner", resultSet.getString("operation_owner"));
                    map.put("deliveryOwner", resultSet.getString("delivery_owner"));
                    map.put("region", resultSet.getString("region"));
                    map.put("businessGroup", resultSet.getString("business_group"));
                    map.put("lob", resultSet.getString("lob"));
                    map.put("platform", resultSet.getString("platform"));
                    map.put("capability", resultSet.getString("capability"));
                    map.put("gitRepo", resultSet.getString("git_repo"));
                    map.put("apiTags", resultSet.getString("api_tags"));
                    map.put("apiStatus", resultSet.getString("api_status"));
                    services.add(map);
                }
            }
            // now, we have the total and the list of tables, we need to put them into a map.
            Map<String, Object> resultMap = new HashMap<>();
            resultMap.put("total", total);
            resultMap.put("services", services);
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
    public Result<String> queryApiLabel(String hostId) {
        Result<String> result = null;
        String sql = "SELECT api_id, api_name FROM api_t WHERE host_id = ?";
        List<Map<String, Object>> labels = new ArrayList<>();
        try (Connection connection = ds.getConnection();
             PreparedStatement preparedStatement = connection.prepareStatement(sql)) {
            preparedStatement.setString(1, hostId);
            try (ResultSet resultSet = preparedStatement.executeQuery()) {
                while (resultSet.next()) {
                    Map<String, Object> map = new HashMap<>();
                    map.put("id", resultSet.getString("api_id"));
                    map.put("label", resultSet.getString("api_name"));
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
    public Result<String> queryApiVersionLabel(String hostId, String apiId) {
        Result<String> result = null;
        String sql = "SELECT api_version FROM api_version_t WHERE host_id = ? AND api_id = ?";
        List<Map<String, Object>> versions = new ArrayList<>();
        try (Connection connection = ds.getConnection();
             PreparedStatement preparedStatement = connection.prepareStatement(sql)) {
            preparedStatement.setString(1, hostId);
            preparedStatement.setString(2, apiId);
            try (ResultSet resultSet = preparedStatement.executeQuery()) {
                while (resultSet.next()) {
                    Map<String, Object> map = new HashMap<>();
                    String id = resultSet.getString("api_version");
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
    public Result<String> queryEndpointLabel(String hostId, String apiId, String apiVersion) {
        Result<String> result = null;
        String sql = "SELECT endpoint FROM api_endpoint_t WHERE host_id = ? AND api_id = ? AND api_version = ?";
        List<Map<String, Object>> labels = new ArrayList<>();
        try (Connection connection = ds.getConnection();
             PreparedStatement preparedStatement = connection.prepareStatement(sql)) {
            preparedStatement.setString(1, hostId);
            preparedStatement.setString(2, apiId);
            preparedStatement.setString(3, apiVersion);
            try (ResultSet resultSet = preparedStatement.executeQuery()) {
                while (resultSet.next()) {
                    Map<String, Object> map = new HashMap<>();
                    String id = resultSet.getString("endpoint");
                    map.put("id", id);
                    map.put("label", id);
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
    public Result<String> createServiceVersion(ServiceVersionCreatedEvent event) {
        final String insertUser = "INSERT INTO api_version_t (host_id, api_id, api_version, api_type, service_id, api_version_desc, " +
                "spec_link, spec, update_user, update_ts) " +
                "VALUES (?, ?, ?, ?, ?,   ?, ?, ?, ?, ?)";
        Result<String> result = null;
        Map<String, Object> map = JsonMapper.string2Map(event.getValue());
        try (Connection conn = ds.getConnection()) {
            conn.setAutoCommit(false);
            // no duplicate record, insert the user into database and write a success notification.
            try (PreparedStatement statement = conn.prepareStatement(insertUser)) {
                statement.setString(1, event.getHostId());
                statement.setString(2, event.getApiId());
                statement.setString(3, event.getApiVersion());

                if (map.get("apiType") != null)
                    statement.setString(4, (String) map.get("apiType"));
                else
                    statement.setNull(4, NULL);

                if (map.get("serviceId") != null)
                    statement.setString(5, (String) map.get("serviceId"));
                else
                    statement.setNull(5, NULL);

                if (map.get("apiVersionDesc") != null)
                    statement.setString(6, (String) map.get("apiVersionDesc"));
                else
                    statement.setNull(6, NULL);

                if (map.get("specLink") != null)
                    statement.setString(7, (String) map.get("specLink"));
                else
                    statement.setNull(7, NULL);

                if (map.get("spec") != null)
                    statement.setInt(8, (Integer) map.get("spec"));
                else
                    statement.setNull(8, NULL);
                statement.setString(9, event.getEventId().getId());
                statement.setTimestamp(10, new Timestamp(System.currentTimeMillis()));
                int count = statement.executeUpdate();
                if (count == 0) {
                    throw new SQLException(String.format("no record is inserted for api version %s", "hostId " + event.getHostId() + " apiId " + event.getApiId() + " apiVersion " + event.getApiVersion()));
                }
                conn.commit();
                result = Success.of(event.getApiId());
                insertNotification(event.getEventId(), event.getClass().getName(), AvroConverter.toJson(event, false), true, null);
            } catch (SQLException e) {
                logger.error("SQLException:", e);
                conn.rollback();
                insertNotification(event.getEventId(), event.getClass().getName(), AvroConverter.toJson(event, false), false, e.getMessage());
                result = Failure.of(new Status(SQL_EXCEPTION, e.getMessage()));
            } catch (Exception e) {
                logger.error("Exception:", e);
                conn.rollback();
                insertNotification(event.getEventId(), event.getClass().getName(), AvroConverter.toJson(event, false), false, e.getMessage());
                result = Failure.of(new Status(GENERIC_EXCEPTION, e.getMessage()));
            }
        } catch (SQLException e) {
            logger.error("SQLException:", e);
            result = Failure.of(new Status(SQL_EXCEPTION, e.getMessage()));
        }
        return result;
    }

    @Override
    public Result<String> updateServiceVersion(ServiceVersionUpdatedEvent event) {
        final String updateApi = "UPDATE api_version_t SET api_type = ?, service_id = ?, api_version_desc = ?, spec_link = ?,  spec = ?," +
                "update_user = ?, update_ts = ? " +
                "WHERE host_id = ? AND api_id = ? AND api_version = ?";

        Result<String> result = null;
        Map<String, Object> map = JsonMapper.string2Map(event.getValue());
        try (Connection conn = ds.getConnection()) {
            conn.setAutoCommit(false);

            try (PreparedStatement statement = conn.prepareStatement(updateApi)) {

                if (map.get("apiType") != null) {
                    statement.setString(1, (String) map.get("apiType"));
                } else {
                    statement.setNull(1, NULL);
                }

                if (map.get("serviceId") != null) {
                    statement.setString(2, (String) map.get("serviceId"));
                } else {
                    statement.setNull(2, NULL);
                }

                if (map.get("apiVersionDesc") != null) {
                    statement.setString(3, (String) map.get("apiVersionDesc"));
                } else {
                    statement.setNull(3, NULL);
                }

                if (map.get("specLink") != null) {
                    statement.setString(4, (String) map.get("specLink"));
                } else {
                    statement.setNull(4, NULL);
                }

                if (map.get("spec") != null) {
                    statement.setString(5, (String) map.get("spec"));
                } else {
                    statement.setNull(5, NULL);
                }

                statement.setString(6, event.getEventId().getId());
                statement.setTimestamp(7, new Timestamp(event.getEventId().getTimestamp()));
                statement.setString(8, event.getHostId());
                statement.setString(9, event.getApiId());
                statement.setString(10, event.getApiVersion());

                int count = statement.executeUpdate();
                if (count == 0) {
                    throw new SQLException(String.format("no record is updated for api version %s", "hostId " + event.getHostId() + " apiId " + event.getApiId() + " apiVersion " + event.getApiVersion()));
                }
                conn.commit();
                result = Success.of(event.getApiId());
                insertNotification(event.getEventId(), event.getClass().getName(), AvroConverter.toJson(event, false), true, null);
            } catch (SQLException e) {
                logger.error("SQLException:", e);
                conn.rollback();
                insertNotification(event.getEventId(), event.getClass().getName(), AvroConverter.toJson(event, false), false, e.getMessage());
                result = Failure.of(new Status(SQL_EXCEPTION, e.getMessage()));
            } catch (Exception e) {
                logger.error("Exception:", e);
                conn.rollback();
                insertNotification(event.getEventId(), event.getClass().getName(), AvroConverter.toJson(event, false), false, e.getMessage());
                result = Failure.of(new Status(GENERIC_EXCEPTION, e.getMessage()));
            }
        } catch (SQLException e) {
            logger.error("SQLException:", e);
            result = Failure.of(new Status(SQL_EXCEPTION, e.getMessage()));
        }
        return result;

    }

    @Override
    public Result<String> deleteServiceVersion(ServiceVersionDeletedEvent event) {
        final String deleteApplication = "DELETE from api_version_t WHERE host_id = ? AND api_id = ? AND api_version = ?";
        Result<String> result;
        try (Connection conn = ds.getConnection()) {
            conn.setAutoCommit(false);
            try (PreparedStatement statement = conn.prepareStatement(deleteApplication)) {
                statement.setString(1, event.getHostId());
                statement.setString(2, event.getApiId());
                statement.setString(3, event.getApiVersion());
                int count = statement.executeUpdate();
                if (count == 0) {
                    // no record is deleted, write an error notification.
                    throw new SQLException(String.format("no record is deleted for api version %s", "hostId " + event.getHostId() + " apiId " + event.getApiId() + " apiVersion " + event.getApiVersion()));
                }
                conn.commit();
                result = Success.of(event.getEventId().getId());
                insertNotification(event.getEventId(), event.getClass().getName(), AvroConverter.toJson(event, false), true, null);
            } catch (SQLException e) {
                logger.error("SQLException:", e);
                conn.rollback();
                insertNotification(event.getEventId(), event.getClass().getName(), AvroConverter.toJson(event, false), false, e.getMessage());
                result = Failure.of(new Status(SQL_EXCEPTION, e.getMessage()));
            } catch (Exception e) {
                logger.error("Exception:", e);
                conn.rollback();
                insertNotification(event.getEventId(), event.getClass().getName(), AvroConverter.toJson(event, false), false, e.getMessage());
                result = Failure.of(new Status(GENERIC_EXCEPTION, e.getMessage()));
            }

        } catch (SQLException e) {
            logger.error("SQLException:", e);
            result = Failure.of(new Status(SQL_EXCEPTION, e.getMessage()));
        }
        return result;

    }

    @Override
    public Result<String> queryServiceVersion(String hostId, String apiId) {
        Result<String> result = null;
        String sql = "SELECT host_id, api_id, api_version, api_type, service_id,\n" +
                "api_version_desc, spec_link, spec\n" +
                "FROM api_version_t\n" +
                "WHERE host_id = ? AND api_id = ?\n" +
                "ORDER BY api_version";

        List<Map<String, Object>> serviceVersions = new ArrayList<>();

        try (Connection connection = ds.getConnection();
             PreparedStatement preparedStatement = connection.prepareStatement(sql)) {
            preparedStatement.setString(1, hostId);
            preparedStatement.setString(2, apiId);
            try (ResultSet resultSet = preparedStatement.executeQuery()) {
                while (resultSet.next()) {
                    Map<String, Object> map = new HashMap<>();
                    map.put("hostId", resultSet.getString("host_id"));
                    map.put("apiId", resultSet.getString("api_id"));
                    map.put("apiVersion", resultSet.getString("api_version"));
                    map.put("apiType", resultSet.getString("api_type"));
                    map.put("serviceId", resultSet.getString("service_id"));
                    map.put("apiVersionDesc", resultSet.getString("api_version_desc"));
                    map.put("specLink", resultSet.getString("spec_link"));
                    map.put("spec", resultSet.getString("spec"));
                    serviceVersions.add(map);
                }
            }
            result = Success.of(JsonMapper.toJson(serviceVersions));
        } catch (SQLException e) {
            logger.error("SQLException:", e);
            result = Failure.of(new Status(SQL_EXCEPTION, e.getMessage()));
        } catch (Exception e) {
            logger.error("Exception:", e);
            result = Failure.of(new Status(GENERIC_EXCEPTION, e.getMessage()));
        }
        return result;
    }

    private void addCondition(StringBuilder whereClause, List<Object> parameters, String columnName, String value) {
        if (value != null && !value.equals("*")) {
            if (whereClause.length() > 0) {
                whereClause.append(" AND ");
            }
            whereClause.append(columnName);
            whereClause.append(" LIKE '%' || ? || '%'");
            parameters.add(value);

        }
    }

    private void addCondition(StringBuilder whereClause, List<Object> parameters, String columnName, Object value) {
        if (value != null) {
            if (whereClause.length() > 0) {
                whereClause.append(" AND ");
            }
            whereClause.append(columnName).append(" = ?");
            parameters.add(value);
        }
    }

    @Override
    public Result<String> updateServiceSpec(ServiceSpecUpdatedEvent event, List<Map<String, Object>> endpoints) {
        final String updateApiVersion = "UPDATE api_version_t SET spec = ?, " +
                "update_user = ?, update_ts = ? " +
                "WHERE host_id = ? AND api_id = ? AND api_version = ?";
        final String deleteEndpoint = "DELETE FROM api_endpoint_t WHERE host_id = ? AND api_id = ? AND api_version = ?";
        final String insertEndpoint = "INSERT INTO api_endpoint_t (host_id, api_id, api_version, endpoint, http_method, " +
                "endpoint_path, endpoint_name, endpoint_desc, update_user, update_ts) " +
                "VALUES (?,? ,?, ?, ?,  ?, ?, ?, ?, ?)";
        final String insertScope = "INSERT INTO api_endpoint_scope_t (host_id, api_id, api_version, endpoint, scope, scope_desc, " +
                "update_user, update_ts) " +
                "VALUES (?, ?, ?, ?, ?,  ?, ?, ?)";


        Result<String> result = null;
        try (Connection conn = ds.getConnection()) {
            conn.setAutoCommit(false);
            try {
                // update spec
                try (PreparedStatement statement = conn.prepareStatement(updateApiVersion)) {
                    statement.setString(1, event.getSpec());
                    statement.setString(2, event.getEventId().getId());
                    statement.setTimestamp(3, new Timestamp(event.getEventId().getTimestamp()));
                    statement.setString(4, event.getHostId());
                    statement.setString(5, event.getApiId());
                    statement.setString(6, event.getApiVersion());

                    int count = statement.executeUpdate();
                    if (count == 0) {
                        // no record is updated, write an error notification.
                        throw new SQLException(String.format("no record is updated for api version " + " hostId " + event.getHostId() + " apiId " + event.getApiId() + " apiVersion " + event.getApiVersion()));
                    }
                }
                // delete endpoints for the api version. the api_endpoint_scope_t will be deleted by the cascade.
                try (PreparedStatement statement = conn.prepareStatement(deleteEndpoint)) {
                    statement.setString(1, event.getHostId());
                    statement.setString(2, event.getApiId());
                    statement.setString(3, event.getApiVersion());
                    statement.executeUpdate();
                }
                // insert endpoints
                for (Map<String, Object> endpoint : endpoints) {
                    try (PreparedStatement statement = conn.prepareStatement(insertEndpoint)) {
                        statement.setString(1, event.getHostId());
                        statement.setString(2, event.getApiId());
                        statement.setString(3, event.getApiVersion());
                        statement.setString(4, (String) endpoint.get("endpoint"));
                        statement.setString(5, ((String) endpoint.get("httpMethod")).toLowerCase().trim());
                        statement.setString(6, (String) endpoint.get("endpointPath"));

                        if (endpoint.get("endpointName") == null)
                            statement.setNull(7, NULL);
                        else
                            statement.setString(7, (String) endpoint.get("endpointName"));

                        if (endpoint.get("endpointDesc") == null)
                            statement.setNull(8, NULL);
                        else
                            statement.setString(8, (String) endpoint.get("endpointDesc"));

                        statement.setString(9, event.getEventId().getId());
                        statement.setTimestamp(10, new Timestamp(event.getEventId().getTimestamp()));
                        statement.executeUpdate();
                    }
                    // insert scopes
                    List<String> scopes = (List<String>) endpoint.get("scopes");
                    for (String scope : scopes) {
                        String[] scopeDesc = scope.split(":");
                        try (PreparedStatement statement = conn.prepareStatement(insertScope)) {
                            statement.setString(1, event.getHostId());
                            statement.setString(2, event.getApiId());
                            statement.setString(3, event.getApiVersion());
                            statement.setString(4, (String) endpoint.get("endpoint"));
                            statement.setString(5, scopeDesc[0]);
                            if (scopeDesc.length == 1)
                                statement.setNull(6, NULL);
                            else
                                statement.setString(6, scopeDesc[1]);
                            statement.setString(7, event.getEventId().getId());
                            statement.setTimestamp(8, new Timestamp(event.getEventId().getTimestamp()));
                            statement.executeUpdate();
                        }
                    }
                }
                conn.commit();
                result = Success.of(event.getApiId());
                insertNotification(event.getEventId(), event.getClass().getName(), AvroConverter.toJson(event, false), true, null);
            } catch (SQLException e) {
                logger.error("SQLException:", e);
                conn.rollback();
                insertNotification(event.getEventId(), event.getClass().getName(), AvroConverter.toJson(event, false), false, e.getMessage());
                result = Failure.of(new Status(SQL_EXCEPTION, e.getMessage()));
            } catch (Exception e) {
                logger.error("Exception:", e);
                conn.rollback();
                insertNotification(event.getEventId(), event.getClass().getName(), AvroConverter.toJson(event, false), false, e.getMessage());
                result = Failure.of(new Status(GENERIC_EXCEPTION, e.getMessage()));
            }
        } catch (SQLException e) {
            logger.error("SQLException:", e);
            result = Failure.of(new Status(SQL_EXCEPTION, e.getMessage()));
        }
        return result;

    }

    @Override
    public Result<String> queryServiceEndpoint(String hostId, String apiId, String apiVersion) {
        Result<String> result = null;
        String sql = "SELECT host_id, api_id, api_version, endpoint, http_method,\n" +
                "endpoint_path, endpoint_desc\n" +
                "FROM api_endpoint_t\n" +
                "WHERE host_id = ? AND api_id = ? AND api_version = ?\n" +
                "ORDER BY endpoint";

        List<Map<String, Object>> endpoints = new ArrayList<>();

        try (Connection connection = ds.getConnection();
             PreparedStatement preparedStatement = connection.prepareStatement(sql)) {
            preparedStatement.setString(1, hostId);
            preparedStatement.setString(2, apiId);
            preparedStatement.setString(3, apiVersion);
            try (ResultSet resultSet = preparedStatement.executeQuery()) {
                while (resultSet.next()) {
                    Map<String, Object> map = new HashMap<>();
                    map.put("hostId", resultSet.getString("host_id"));
                    map.put("apiId", resultSet.getString("api_id"));
                    map.put("apiVersion", resultSet.getString("api_version"));
                    map.put("endpoint", resultSet.getString("endpoint"));
                    map.put("httpMethod", resultSet.getString("http_method"));
                    map.put("endpointPath", resultSet.getString("endpoint_path"));
                    map.put("endpointDesc", resultSet.getString("endpoint_desc"));
                    endpoints.add(map);
                }
            }
            result = Success.of(JsonMapper.toJson(endpoints));
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
    public Result<String> queryEndpointScope(String hostId, String apiId, String apiVersion, String endpoint) {
        Result<String> result = null;
        String sql = "SELECT host_id, api_id, api_version, endpoint, scope, scope_desc \n" +
                "FROM api_endpoint_scope_t\n" +
                "WHERE host_id = ?\n" +
                "AND api_id = ?\n" +
                "AND api_version = ?\n" +
                "AND endpoint = ?\n" +
                "ORDER BY scope";

        List<Map<String, Object>> scopes = new ArrayList<>();

        try (Connection connection = ds.getConnection();
             PreparedStatement preparedStatement = connection.prepareStatement(sql)) {
            preparedStatement.setString(1, hostId);
            preparedStatement.setString(2, apiId);
            preparedStatement.setString(3, apiVersion);
            preparedStatement.setString(4, endpoint);
            try (ResultSet resultSet = preparedStatement.executeQuery()) {
                while (resultSet.next()) {
                    Map<String, Object> map = new HashMap<>();
                    map.put("hostId", resultSet.getString("host_id"));
                    map.put("apiId", resultSet.getString("api_id"));
                    map.put("apiVersion", resultSet.getString("api_version"));
                    map.put("endpoint", resultSet.getString("endpoint"));
                    map.put("scope", resultSet.getString("scope"));
                    map.put("scopeDesc", resultSet.getString("scope_desc"));
                    scopes.add(map);
                }
            }
            result = Success.of(JsonMapper.toJson(scopes));
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
    public Result<String> queryEndpointRule(String hostId, String apiId, String apiVersion, String endpoint) {
        Result<String> result = null;
        String sql = "SELECT a.host_id, a.api_id, a.api_version, a.endpoint, r.rule_type, a.rule_id\n" +
                "FROM api_endpoint_rule_t a, rule_t r\n" +
                "WHERE a.rule_id = r.rule_id\n" +
                "AND a.host_id = ?\n" +
                "AND a.api_id = ?\n" +
                "AND a.api_version = ?\n" +
                "AND a.endpoint = ?\n" +
                "ORDER BY r.rule_type";

        List<Map<String, Object>> rules = new ArrayList<>();

        try (Connection connection = ds.getConnection();
             PreparedStatement preparedStatement = connection.prepareStatement(sql)) {
            preparedStatement.setString(1, hostId);
            preparedStatement.setString(2, apiId);
            preparedStatement.setString(3, apiVersion);
            preparedStatement.setString(4, endpoint);
            try (ResultSet resultSet = preparedStatement.executeQuery()) {
                while (resultSet.next()) {
                    Map<String, Object> map = new HashMap<>();
                    map.put("hostId", resultSet.getString("host_id"));
                    map.put("apiId", resultSet.getString("api_id"));
                    map.put("apiVersion", resultSet.getString("api_version"));
                    map.put("endpoint", resultSet.getString("endpoint"));
                    map.put("ruleType", resultSet.getString("rule_type"));
                    map.put("ruleId", resultSet.getString("rule_id"));
                    rules.add(map);
                }
            }
            result = Success.of(JsonMapper.toJson(rules));
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
    public Result<String> createEndpointRule(EndpointRuleCreatedEvent event) {
        final String insertUser = "INSERT INTO api_endpoint_rule_t (host_id, api_id, api_version, endpoint, rule_id, " +
                "update_user, update_ts) " +
                "VALUES (?, ?, ?, ?, ?,   ?, ?)";
        Result<String> result = null;
        try (Connection conn = ds.getConnection()) {
            conn.setAutoCommit(false);
            // no duplicate record, insert the user into database and write a success notification.
            try (PreparedStatement statement = conn.prepareStatement(insertUser)) {
                statement.setString(1, event.getHostId());
                statement.setString(2, event.getApiId());
                statement.setString(3, event.getApiVersion());
                statement.setString(4, event.getEndpoint());
                statement.setString(5, event.getRuleId());
                statement.setString(6, event.getEventId().getId());
                statement.setTimestamp(7, new Timestamp(System.currentTimeMillis()));
                int count = statement.executeUpdate();
                if (count == 0) {
                    throw new SQLException(String.format("no record is inserted for api version " + "hostId " + event.getHostId() + " apiId " + event.getApiId() + " apiVersion " + event.getApiVersion()));
                }
                conn.commit();
                result = Success.of(event.getApiId());
                insertNotification(event.getEventId(), event.getClass().getName(), AvroConverter.toJson(event, false), true, null);
            } catch (SQLException e) {
                logger.error("SQLException:", e);
                conn.rollback();
                insertNotification(event.getEventId(), event.getClass().getName(), AvroConverter.toJson(event, false), false, e.getMessage());
                result = Failure.of(new Status(SQL_EXCEPTION, e.getMessage()));
            } catch (Exception e) {
                logger.error("Exception:", e);
                conn.rollback();
                insertNotification(event.getEventId(), event.getClass().getName(), AvroConverter.toJson(event, false), false, e.getMessage());
                result = Failure.of(new Status(GENERIC_EXCEPTION, e.getMessage()));
            }
        } catch (SQLException e) {
            logger.error("SQLException:", e);
            result = Failure.of(new Status(SQL_EXCEPTION, e.getMessage()));
        }
        return result;
    }

    @Override
    public Result<String> deleteEndpointRule(EndpointRuleDeletedEvent event) {
        final String deleteApplication = "DELETE from api_endpoint_rule_t WHERE host_id = ? AND api_id = ? AND api_version = ? AND endpoint = ? AND rule_id = ?";
        Result<String> result;
        try (Connection conn = ds.getConnection()) {
            conn.setAutoCommit(false);
            try (PreparedStatement statement = conn.prepareStatement(deleteApplication)) {
                statement.setString(1, event.getHostId());
                statement.setString(2, event.getApiId());
                statement.setString(3, event.getApiVersion());
                statement.setString(4, event.getEndpoint());
                statement.setString(5, event.getRuleId());

                int count = statement.executeUpdate();
                if (count == 0) {
                    // no record is deleted, write an error notification.
                    throw new SQLException(String.format("no record is deleted for endpoint rule " + "hostId " + event.getHostId() + " apiId " + event.getApiId() + " apiVersion " + event.getApiVersion()));
                }
                conn.commit();
                result = Success.of(event.getEventId().getId());
                insertNotification(event.getEventId(), event.getClass().getName(), AvroConverter.toJson(event, false), true, null);
            } catch (SQLException e) {
                logger.error("SQLException:", e);
                conn.rollback();
                insertNotification(event.getEventId(), event.getClass().getName(), AvroConverter.toJson(event, false), false, e.getMessage());
                result = Failure.of(new Status(SQL_EXCEPTION, e.getMessage()));
            } catch (Exception e) {
                logger.error("Exception:", e);
                conn.rollback();
                insertNotification(event.getEventId(), event.getClass().getName(), AvroConverter.toJson(event, false), false, e.getMessage());
                result = Failure.of(new Status(GENERIC_EXCEPTION, e.getMessage()));
            }

        } catch (SQLException e) {
            logger.error("SQLException:", e);
            result = Failure.of(new Status(SQL_EXCEPTION, e.getMessage()));
        }
        return result;
    }

    @Override
    public Result<String> queryServiceRule(String hostId, String apiId, String apiVersion) {
        Result<String> result = null;
        String sql = "SELECT a.host_id, a.api_id, a.api_version, a.endpoint, r.rule_type, a.rule_id\n" +
                "FROM api_endpoint_rule_t a, rule_t r\n" +
                "WHERE a.rule_id = r.rule_id\n" +
                "AND a.host_id =?\n" +
                "AND a.api_id = ?\n" +
                "AND a.api_version = ?\n" +
                "ORDER BY r.rule_type";
        String sqlRuleBody = "SELECT rule_id, rule_body FROM rule_t WHERE rule_id = ?";
        List<Map<String, Object>> rules = new ArrayList<>();
        Map<String, Object> ruleBodies = new HashMap<>();

        try (Connection connection = ds.getConnection();
             PreparedStatement preparedStatement = connection.prepareStatement(sql)) {
            preparedStatement.setString(1, hostId);
            preparedStatement.setString(2, apiId);
            preparedStatement.setString(3, apiVersion);
            try (ResultSet resultSet = preparedStatement.executeQuery()) {
                while (resultSet.next()) {
                    Map<String, Object> map = new HashMap<>();
                    map.put("hostId", resultSet.getString("host_id"));
                    map.put("apiId", resultSet.getString("api_id"));
                    map.put("apiVersion", resultSet.getString("api_version"));
                    map.put("endpoint", resultSet.getString("endpoint"));
                    map.put("ruleType", resultSet.getString("rule_type"));
                    String ruleId = resultSet.getString("rule_id");
                    map.put("ruleId", ruleId);
                    rules.add(map);

                    // Get rule body if not already cached
                    if (!ruleBodies.containsKey(ruleId)) {
                        String ruleBody = fetchRuleBody(connection, sqlRuleBody, ruleId);
                        // convert the json string to map.
                        Map<String, Object> bodyMap = JsonMapper.string2Map(ruleBody);
                        ruleBodies.put(ruleId, bodyMap);
                    }
                }
            }
            Map<String, Object> combinedResult = new HashMap<>();
            combinedResult.put("rules", rules);
            combinedResult.put("ruleBodies", ruleBodies);
            result = Success.of(JsonMapper.toJson(combinedResult));
        } catch (SQLException e) {
            logger.error("SQLException:", e);
            result = Failure.of(new Status(SQL_EXCEPTION, e.getMessage()));
        } catch (Exception e) {
            logger.error("Exception:", e);
            result = Failure.of(new Status(GENERIC_EXCEPTION, e.getMessage()));
        }
        return result;
    }

    private String fetchRuleBody(Connection connection, String sqlRuleBody, String ruleId) throws SQLException {
        try (PreparedStatement preparedStatement = connection.prepareStatement(sqlRuleBody)) {
            preparedStatement.setString(1, ruleId);
            try (ResultSet resultSet = preparedStatement.executeQuery()) {
                if (resultSet.next()) {
                    return resultSet.getString("rule_body");
                }
            }
        }
        return null; // or throw an exception if you consider this an error state.
    }

    @Override
    public Result<String> queryServicePermission(String hostId, String apiId, String apiVersion) {
        Result<String> result = null;
        String sql = "SELECT\n" +
                "    CASE\n" +
                "        WHEN COUNT(ae.endpoint) > 0 THEN\n" +
                "            JSON_AGG(\n" +
                "                JSON_BUILD_OBJECT(\n" +
                "                    'endpoint', ae.endpoint,\n" +
                "                    'roles', COALESCE((\n" +
                "                        SELECT JSON_ARRAYAGG(\n" +
                "                            JSON_BUILD_OBJECT(\n" +
                "                                'roleId', rp.role_id\n" +
                "                            )\n" +
                "                        )\n" +
                "                        FROM role_permission_t rp\n" +
                "                        WHERE rp.host_id = ?\n" +
                "                        AND rp.api_id = ?\n" +
                "                        AND rp.api_version = ?\n" +
                "                        AND rp.endpoint = ae.endpoint\n" +
                "                    ), '[]'),\n" +
                "                    'positions', COALESCE((\n" +
                "                        SELECT JSON_ARRAYAGG(\n" +
                "                             JSON_BUILD_OBJECT(\n" +
                "                                'positionId', pp.position_id\n" +
                "                             )\n" +
                "                         )\n" +
                "                        FROM position_permission_t pp\n" +
                "                        WHERE pp.host_id = ?\n" +
                "                        AND pp.api_id = ?\n" +
                "                        AND pp.api_version = ?\n" +
                "                        AND pp.endpoint = ae.endpoint\n" +
                "                    ), '[]'),\n" +
                "                    'groups', COALESCE((\n" +
                "                        SELECT JSON_ARRAYAGG(\n" +
                "                            JSON_BUILD_OBJECT(\n" +
                "                               'groupId', gp.group_id\n" +
                "                            )\n" +
                "                        )\n" +
                "                        FROM group_permission_t gp\n" +
                "                        WHERE gp.host_id = ?\n" +
                "                        AND gp.api_id = ?\n" +
                "                        AND gp.api_version = ?\n" +
                "                        AND gp.endpoint = ae.endpoint\n" +
                "                    ), '[]'),\n" +
                "                    'attributes', COALESCE((\n" +
                "                        SELECT JSON_ARRAYAGG(\n" +
                "                            JSON_BUILD_OBJECT(\n" +
                "                                'attribute_id', ap.attribute_id, \n" +
                "                                'attribute_value', ap.attribute_value, \n" +
                "                                'attribute_type', a.attribute_type\n" +
                "                            )\n" +
                "                        )\n" +
                "                        FROM attribute_permission_t ap, attribute_t a\n" +
                "                        WHERE ap.attribute_id = a.attribute_id\n" +
                "                        AND ap.host_id = ?\n" +
                "                        AND ap.api_id = ?\n" +
                "                        AND ap.api_version = ?\n" +
                "                        AND ap.endpoint = ae.endpoint\n" +
                "                    ), '[]'),\n" +
                "                    'users', COALESCE((\n" +
                "                        SELECT JSON_ARRAYAGG(\n" +
                "                            JSON_BUILD_OBJECT(\n" +
                "                                 'userId', user_id,\n" +
                "                                 'startTs', start_ts,\n" +
                "                                 'endTs', end_ts\n" +
                "                            )\n" +
                "                        )\n" +
                "                        FROM user_permission_t up\n" +
                "                        WHERE up.host_id = ?\n" +
                "                        AND up.api_id = ?\n" +
                "                        AND up.api_version = ?\n" +
                "                        AND up.endpoint = ae.endpoint\n" +
                "                    ), '[]')\n" +
                "                )\n" +
                "            )\n" +
                "        ELSE NULL\n" +
                "    END AS permissions\n" +
                "FROM\n" +
                "    api_endpoint_t ae\n" +
                "WHERE\n" +
                "    ae.host_id = ?\n" +
                "    AND ae.api_id = ?\n" +
                "    AND ae.api_version = ?;\n";

        try (Connection connection = ds.getConnection();
             PreparedStatement preparedStatement = connection.prepareStatement(sql)) {
            preparedStatement.setString(1, hostId);
            preparedStatement.setString(2, apiId);
            preparedStatement.setString(3, apiVersion);
            preparedStatement.setString(4, hostId);
            preparedStatement.setString(5, apiId);
            preparedStatement.setString(6, apiVersion);
            preparedStatement.setString(7, hostId);
            preparedStatement.setString(8, apiId);
            preparedStatement.setString(9, apiVersion);
            preparedStatement.setString(10, hostId);
            preparedStatement.setString(11, apiId);
            preparedStatement.setString(12, apiVersion);
            preparedStatement.setString(13, hostId);
            preparedStatement.setString(14, apiId);
            preparedStatement.setString(15, apiVersion);
            preparedStatement.setString(16, hostId);
            preparedStatement.setString(17, apiId);
            preparedStatement.setString(18, apiVersion);

            try (ResultSet resultSet = preparedStatement.executeQuery()) {
                if (resultSet.next()) {
                    String permissionsJson = resultSet.getString("permissions");
                    result = Success.of(permissionsJson);
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
    public Result<List<String>> queryServiceFilter(String hostId, String apiId, String apiVersion) {
        Result<List<String>> result = null;
        String sql = "SELECT\n" +
                "    JSON_BUILD_OBJECT(\n" +
                "        'role_row', JSON_AGG(\n" +
                "            JSON_BUILD_OBJECT(\n" +
                "                'endpoint', endpoint,\n" +
                "                'roleId', role_id,\n" +
                "                'colName', col_name,\n" +
                "                'operator', operator,\n" +
                "                'colValue', col_value\n" +
                "            )\n" +
                "        )\n" +
                "    ) AS result\n" +
                "FROM\n" +
                "    role_row_filter_t\n" +
                "WHERE\n" +
                "    host_id = ?\n" +
                "    AND api_id = ?\n" +
                "    AND api_version = ?\n" +
                "GROUP BY ()\n" +
                "HAVING COUNT(*) > 0 \n" +
                "UNION ALL\n" +
                "SELECT\n" +
                "    JSON_BUILD_OBJECT(\n" +
                "        'role_col', JSON_AGG(\n" +
                "            JSON_BUILD_OBJECT(\n" +
                "                'endpoint', endpoint,\n" +
                "                'roleId', role_id,\n" +
                "                'columns', columns\n" +
                "            )\n" +
                "        )\n" +
                "    ) AS result\n" +
                "FROM\n" +
                "    role_col_filter_t\n" +
                "WHERE\n" +
                "    host_id = ?\n" +
                "    AND api_id = ?\n" +
                "    AND api_version = ?\n" +
                "GROUP BY ()\n" +
                "HAVING COUNT(*) > 0\n" +
                "UNION ALL\n" +
                "SELECT\n" +
                "    JSON_BUILD_OBJECT(\n" +
                "        'group_row', JSON_AGG(\n" +
                "            JSON_BUILD_OBJECT(\n" +
                "                'endpoint', endpoint,\n" +
                "                'groupId', group_id,\n" +
                "                'colName', col_name,\n" +
                "                'operator', operator,\n" +
                "                'colValue', col_value\n" +
                "            )\n" +
                "        )\n" +
                "    ) AS result\n" +
                "FROM\n" +
                "    group_row_filter_t\n" +
                "WHERE\n" +
                "    host_id = ?\n" +
                "    AND api_id = ?\n" +
                "    AND api_version = ?\n" +
                "GROUP BY ()\n" +
                "HAVING COUNT(*) > 0 \n" +
                "UNION ALL\n" +
                "SELECT\n" +
                "    JSON_BUILD_OBJECT(\n" +
                "        'group_col', JSON_AGG(\n" +
                "            JSON_BUILD_OBJECT(\n" +
                "                'endpoint', endpoint,\n" +
                "                'groupId', group_id,\n" +
                "                'columns', columns\n" +
                "            )\n" +
                "        )\n" +
                "    ) AS result\n" +
                "FROM\n" +
                "    group_col_filter_t\n" +
                "WHERE\n" +
                "    host_id = ?\n" +
                "    AND api_id = ?\n" +
                "    AND api_version = ?\n" +
                "GROUP BY ()\n" +
                "HAVING COUNT(*) > 0\n" +
                "UNION ALL\n" +
                "SELECT\n" +
                "    JSON_BUILD_OBJECT(\n" +
                "        'position_row', JSON_AGG(\n" +
                "            JSON_BUILD_OBJECT(\n" +
                "                'endpoint', endpoint,\n" +
                "                'positionId', position_id,\n" +
                "                'colName', col_name,\n" +
                "                'operator', operator,\n" +
                "                'colValue', col_value\n" +
                "            )\n" +
                "        )\n" +
                "    ) AS result\n" +
                "FROM\n" +
                "    position_row_filter_t\n" +
                "WHERE\n" +
                "    host_id = ?\n" +
                "    AND api_id = ?\n" +
                "    AND api_version = ?\n" +
                "GROUP BY ()\n" +
                "HAVING COUNT(*) > 0 \n" +
                "UNION ALL\n" +
                "SELECT\n" +
                "    JSON_BUILD_OBJECT(\n" +
                "        'position_col', JSON_AGG(\n" +
                "            JSON_BUILD_OBJECT(\n" +
                "                'endpoint', endpoint,\n" +
                "                'positionId', position_id,\n" +
                "                'columns', columns\n" +
                "            )\n" +
                "        )\n" +
                "    ) AS result\n" +
                "FROM\n" +
                "    position_col_filter_t\n" +
                "WHERE\n" +
                "    host_id = ?\n" +
                "    AND api_id = ?\n" +
                "    AND api_version = ?\n" +
                "GROUP BY ()\n" +
                "HAVING COUNT(*) > 0\n" +
                "UNION ALL\n" +
                "SELECT\n" +
                "    JSON_BUILD_OBJECT(\n" +
                "        'attribute_row', JSON_AGG(\n" +
                "            JSON_BUILD_OBJECT(\n" +
                "                'endpoint', endpoint,\n" +
                "                'attributeId', attribute_id,\n" +
                "                'attributeValue', attribute_value,\n" +
                "                'colName', col_name,\n" +
                "                'operator', operator,\n" +
                "                'colValue', col_value\n" +
                "            )\n" +
                "        )\n" +
                "    ) AS result\n" +
                "FROM\n" +
                "    attribute_row_filter_t\n" +
                "WHERE\n" +
                "    host_id = ?\n" +
                "    AND api_id = ?\n" +
                "    AND api_version = ?\n" +
                "GROUP BY ()\n" +
                "HAVING COUNT(*) > 0 \n" +
                "UNION ALL\n" +
                "SELECT\n" +
                "    JSON_BUILD_OBJECT(\n" +
                "        'attribute_col', JSON_AGG(\n" +
                "            JSON_BUILD_OBJECT(\n" +
                "                'endpoint', endpoint,\n" +
                "                'attributeId', attribute_id,\n" +
                "                'attributeValue', attribute_value,\n" +
                "                'columns', columns\n" +
                "            )\n" +
                "        )\n" +
                "    ) AS result\n" +
                "FROM\n" +
                "    attribute_col_filter_t\n" +
                "WHERE\n" +
                "    host_id = ?\n" +
                "    AND api_id = ?\n" +
                "    AND api_version = ?\n" +
                "GROUP BY ()\n" +
                "HAVING COUNT(*) > 0\n" +
                "UNION ALL\n" +
                "SELECT\n" +
                "    JSON_BUILD_OBJECT(\n" +
                "        'user_row', JSON_AGG(\n" +
                "            JSON_BUILD_OBJECT(\n" +
                "                'endpoint', endpoint,\n" +
                "                'userId', user_id,\n" +
                "                'startTs', start_ts,\n" +
                "                'endTs', end_ts,\n" +
                "                'colName', col_name,\n" +
                "                'operator', operator,\n" +
                "                'colValue', col_value\n" +
                "            )\n" +
                "        )\n" +
                "    ) AS result\n" +
                "FROM\n" +
                "    user_row_filter_t\n" +
                "WHERE\n" +
                "    host_id = ?\n" +
                "    AND api_id = ?\n" +
                "    AND api_version = ?\n" +
                "GROUP BY ()\n" +
                "HAVING COUNT(*) > 0 \n" +
                "UNION ALL\n" +
                "SELECT\n" +
                "    JSON_BUILD_OBJECT(\n" +
                "        'user_col', JSON_AGG(\n" +
                "            JSON_BUILD_OBJECT(\n" +
                "                'endpoint', endpoint,\n" +
                "                'userId', user_id,\n" +
                "                'startTs', start_ts,\n" +
                "                'endTs', end_ts,\n" +
                "                'columns', columns\n" +
                "            )\n" +
                "        )\n" +
                "    ) AS result\n" +
                "FROM\n" +
                "    user_col_filter_t\n" +
                "WHERE\n" +
                "    host_id = ?\n" +
                "    AND api_id = ?\n" +
                "    AND api_version = ?\n" +
                "GROUP BY ()\n" +
                "HAVING COUNT(*) > 0\n";

        try (Connection connection = ds.getConnection();
             PreparedStatement preparedStatement = connection.prepareStatement(sql)) {
            preparedStatement.setString(1, hostId);
            preparedStatement.setString(2, apiId);
            preparedStatement.setString(3, apiVersion);
            preparedStatement.setString(4, hostId);
            preparedStatement.setString(5, apiId);
            preparedStatement.setString(6, apiVersion);
            preparedStatement.setString(7, hostId);
            preparedStatement.setString(8, apiId);
            preparedStatement.setString(9, apiVersion);
            preparedStatement.setString(10, hostId);
            preparedStatement.setString(11, apiId);
            preparedStatement.setString(12, apiVersion);
            preparedStatement.setString(13, hostId);
            preparedStatement.setString(14, apiId);
            preparedStatement.setString(15, apiVersion);
            preparedStatement.setString(16, hostId);
            preparedStatement.setString(17, apiId);
            preparedStatement.setString(18, apiVersion);
            preparedStatement.setString(19, hostId);
            preparedStatement.setString(20, apiId);
            preparedStatement.setString(21, apiVersion);
            preparedStatement.setString(22, hostId);
            preparedStatement.setString(23, apiId);
            preparedStatement.setString(24, apiVersion);
            preparedStatement.setString(25, hostId);
            preparedStatement.setString(26, apiId);
            preparedStatement.setString(27, apiVersion);
            preparedStatement.setString(28, hostId);
            preparedStatement.setString(29, apiId);
            preparedStatement.setString(30, apiVersion);
            List<String> list = new ArrayList<>();
            try (ResultSet resultSet = preparedStatement.executeQuery()) {
                while (resultSet.next()) {
                    String json = resultSet.getString("result");
                    list.add(json);
                }
            }
            result = Success.of(list);
        } catch (SQLException e) {
            logger.error("SQLException:", e);
            result = Failure.of(new Status(SQL_EXCEPTION, e.getMessage()));
        } catch (Exception e) {
            logger.error("Exception:", e);
            result = Failure.of(new Status(GENERIC_EXCEPTION, e.getMessage()));
        }
        return result;
    }

    public Result<String> createMarketCode(MarketCodeCreatedEvent event) {
        // cache key is based on the hostId and authCode.
        String hostId = event.getHostId();
        String authCode = event.getAuthCode();
        String key = hostId + "|" + authCode;
        if (logger.isTraceEnabled())
            logger.trace("insert into the cache auth_code with key {} value {}", key, event.getValue());
        if (logger.isTraceEnabled())
            logger.trace("estimate the size of the cache auth_code before is " + cacheManager.getSize(AUTH_CODE_CACHE));
        cacheManager.put(AUTH_CODE_CACHE, key, event.getValue());
        if (logger.isTraceEnabled())
            logger.trace("estimate the size of the cache auth_code after is " + cacheManager.getSize(AUTH_CODE_CACHE));
        return Success.of(event.getAuthCode());
    }

    public Result<String> deleteMarketCode(MarketCodeDeletedEvent event) {
        String hostId = event.getHostId();
        String authCode = event.getAuthCode();
        String key = hostId + "|" + authCode;
        if (logger.isTraceEnabled()) logger.trace("insert into the cache auth_code with key {}", key);
        if (logger.isTraceEnabled())
            logger.trace("estimate the size of the cache auth_code before is " + cacheManager.getSize(AUTH_CODE_CACHE));
        cacheManager.delete(AUTH_CODE_CACHE, key);
        if (logger.isTraceEnabled())
            logger.trace("estimate the size of the cache auth_code after is " + cacheManager.getSize(AUTH_CODE_CACHE));
        return Success.of(event.getAuthCode());
    }

    public Result<String> queryMarketCode(String hostId, String authCode) {
        // cache key is based on the hostId and authCode.
        String key = hostId + "|" + authCode;
        if (logger.isTraceEnabled())
            logger.trace("key = {} and estimate the size of the cache auth_code is {}", key, cacheManager.getSize(AUTH_CODE_CACHE));
        String value = (String) cacheManager.get(AUTH_CODE_CACHE, key);
        if (logger.isTraceEnabled()) logger.trace("retrieve cache auth_code with key {} value {}", key, value);
        if (value != null) {
            return Success.of(value);
        } else {
            return Failure.of(new Status(OBJECT_NOT_FOUND, "auth code not found"));
        }
    }

    @Override
    public Result<String> createHost(HostCreatedEvent event) {
        final String insertHost = "INSERT INTO host_t (host_id, host_domain, org_name, org_desc, org_owner, jwk, update_user, update_ts) " +
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?)";
        final String insertHostKey = "INSERT INTO host_key_t (host_id, kid, public_key, private_key, key_type, update_user, update_ts) " +
                "VALUES (?, ?, ?, ?, ?, ?, ?)";

        Result<String> result;
        Map<String, Object> map = JsonMapper.string2Map(event.getValue());
        try (Connection conn = ds.getConnection()) {
            // we create the key pair for the host here so that each environment will have different key pairs. This also avoids to put
            // the keys in the events which might be used to promote from env to env or from one host to another host.
            KeyPair longKeyPair = KeyUtil.generateKeyPair("RSA", 2048);
            String longKeyId = HashUtil.generateUUID();
            KeyPair currKeyPair = KeyUtil.generateKeyPair("RSA", 2048);
            String currKeyId = HashUtil.generateUUID();
            if (logger.isTraceEnabled()) logger.trace("longKeyId is {} currKeyId is {}", longKeyId, currKeyId);
            // prevKey and prevKeyId are null for the first time create the host. They are available during the key rotation.
            String jwk = KeyUtil.generateJwk(longKeyPair.getPublic(), longKeyId, currKeyPair.getPublic(), currKeyId, null, null);
            if (logger.isTraceEnabled()) logger.trace("jwk is {}", jwk);
            conn.setAutoCommit(false);
            try {
                // no duplicate record, insert the user into database and write a success notification.
                try (PreparedStatement statement = conn.prepareStatement(insertHost)) {
                    statement.setString(1, event.getHostId());
                    statement.setString(2, event.getHostDomain());
                    statement.setString(3, event.getName());
                    statement.setString(4, event.getDesc());
                    statement.setString(5, event.getOwner());
                    statement.setString(6, jwk);
                    statement.setString(7, event.getEventId().getId());
                    statement.setTimestamp(8, new Timestamp(event.getEventId().getTimestamp()));
                    int count = statement.executeUpdate();
                    if (count == 0) {
                        throw new SQLException("failed to insert the host " + event.getHostDomain());
                    }
                }
                // insert the long key pair
                try (PreparedStatement statement = conn.prepareStatement(insertHostKey)) {
                    statement.setString(1, event.getHostId());
                    statement.setString(2, longKeyId);
                    statement.setString(3, KeyUtil.serializePublicKey(longKeyPair.getPublic()));
                    statement.setString(4, KeyUtil.serializePrivateKey(longKeyPair.getPrivate()));
                    statement.setString(5, "L");
                    statement.setString(6, event.getEventId().getId());
                    statement.setTimestamp(7, new Timestamp(event.getEventId().getTimestamp()));
                    int count = statement.executeUpdate();
                    if (count == 0) {
                        throw new SQLException("failed to insert the host_key for host " + event.getHostDomain() + " kid " + longKeyId);
                    }
                }
                // insert the current key pair
                try (PreparedStatement statement = conn.prepareStatement(insertHostKey)) {
                    statement.setString(1, event.getHostId());
                    statement.setString(2, currKeyId);
                    statement.setString(3, KeyUtil.serializePublicKey(currKeyPair.getPublic()));
                    statement.setString(4, KeyUtil.serializePrivateKey(currKeyPair.getPrivate()));
                    statement.setString(5, "C");
                    statement.setString(6, event.getEventId().getId());
                    statement.setTimestamp(7, new Timestamp(event.getEventId().getTimestamp()));
                    int count = statement.executeUpdate();
                    if (count == 0) {
                        throw new SQLException("failed to insert the host_key for host " + event.getHostDomain() + " kid " + currKeyId);
                    }
                }
                conn.commit();
                result = Success.of(event.getHostId());
                insertNotification(event.getEventId(), event.getClass().getName(), AvroConverter.toJson(event, false), true, null);
            } catch (SQLException e) {
                logger.error("SQLException:", e);
                conn.rollback();
                insertNotification(event.getEventId(), event.getClass().getName(), AvroConverter.toJson(event, false), false, e.getMessage());
                result = Failure.of(new Status(SQL_EXCEPTION, e.getMessage()));
            } catch (Exception e) {
                logger.error("Exception:", e);
                conn.rollback();
                result = Failure.of(new Status(GENERIC_EXCEPTION, e.getMessage()));
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
    public Result<String> updateHost(HostUpdatedEvent event) {
        final String updateHost = "UPDATE host_t SET org_name = ?, org_desc = ?, org_owner = ?, update_user = ? " +
                "update_ts = ? " +
                "WHERE host_id = ?";

        Result<String> result = null;
        try (Connection conn = ds.getConnection()) {
            conn.setAutoCommit(false);

            try (PreparedStatement statement = conn.prepareStatement(updateHost)) {
                if (event.getName() != null) {
                    statement.setString(1, event.getName());
                } else {
                    statement.setNull(1, NULL);
                }
                if (event.getDesc() != null) {
                    statement.setString(2, event.getDesc());
                } else {
                    statement.setNull(2, NULL);
                }
                if (event.getOwner() != null) {
                    statement.setString(3, event.getOwner());
                } else {
                    statement.setNull(3, NULL);
                }
                statement.setString(4, event.getEventId().getId());
                statement.setTimestamp(5, new Timestamp(event.getEventId().getTimestamp()));
                statement.setString(6, event.getHostId());

                int count = statement.executeUpdate();
                if (count == 0) {
                    // no record is updated, write an error notification.
                    throw new SQLException("no record is updated for host " + event.getHostDomain());
                }
                conn.commit();
                result = Success.of(event.getHostId());
                insertNotification(event.getEventId(), event.getClass().getName(), AvroConverter.toJson(event, false), true, null);
            } catch (SQLException e) {
                logger.error("SQLException:", e);
                conn.rollback();
                insertNotification(event.getEventId(), event.getClass().getName(), AvroConverter.toJson(event, false), false, e.getMessage());
                result = Failure.of(new Status(SQL_EXCEPTION, e.getMessage()));
            } catch (Exception e) {
                logger.error("Exception:", e);
                conn.rollback();
                insertNotification(event.getEventId(), event.getClass().getName(), AvroConverter.toJson(event, false), false, e.getMessage());
                result = Failure.of(new Status(GENERIC_EXCEPTION, e.getMessage()));
            }
        } catch (SQLException e) {
            logger.error("SQLException:", e);
            result = Failure.of(new Status(SQL_EXCEPTION, e.getMessage()));
        }
        return result;
    }

    @Override
    public Result<String> deleteHost(HostDeletedEvent event) {
        final String deleteHost = "DELETE from host_t WHERE host_id = ?";
        final String deleteHostKey = "DELETE from host_key_t WHERE host_id = ?";

        Result<String> result;
        try (Connection conn = ds.getConnection()) {
            conn.setAutoCommit(false);
            try {
                try (PreparedStatement statement = conn.prepareStatement(deleteHostKey)) {
                    statement.setString(1, event.getHostId());
                    int count = statement.executeUpdate();
                    if (count == 0) {
                        // no record is deleted, write an error notification.
                        throw new SQLException("no record is deleted for host " + event.getHostId());
                    }
                }
                try (PreparedStatement statement = conn.prepareStatement(deleteHost)) {
                    statement.setString(1, event.getHostId());
                    int count = statement.executeUpdate();
                    if (count == 0) {
                        throw new SQLException("no record is deleted for host " + event.getHostId());
                    }
                }
                conn.commit();
                result = Success.of(event.getEventId().getId());
                insertNotification(event.getEventId(), event.getClass().getName(), AvroConverter.toJson(event, false), true, null);
            } catch (SQLException e) {
                logger.error("SQLException:", e);
                conn.rollback();
                insertNotification(event.getEventId(), event.getClass().getName(), AvroConverter.toJson(event, false), false, e.getMessage());
                result = Failure.of(new Status(SQL_EXCEPTION, e.getMessage()));
            } catch (Exception e) {
                logger.error("Exception:", e);
                conn.rollback();
                insertNotification(event.getEventId(), event.getClass().getName(), AvroConverter.toJson(event, false), false, e.getMessage());
                result = Failure.of(new Status(GENERIC_EXCEPTION, e.getMessage()));
            }
        } catch (SQLException e) {
            logger.error("SQLException:", e);
            result = Failure.of(new Status(SQL_EXCEPTION, e.getMessage()));
        }
        return result;
    }

    @Override
    public Result<String> queryHostDomainById(String hostId) {
        final String sql = "SELECT host_domain from host_t WHERE host_id = ?";
        Result<String> result;
        String hostDomain = null;
        try (final Connection conn = ds.getConnection(); final PreparedStatement statement = conn.prepareStatement(sql)) {
            statement.setString(1, hostId);
            try (ResultSet resultSet = statement.executeQuery()) {
                if (resultSet.next()) {
                    hostDomain = resultSet.getString("host_domain");
                }
            }
            if (hostDomain == null)
                result = Failure.of(new Status(OBJECT_NOT_FOUND, "host with hostId ", hostId));
            else
                result = Success.of(hostDomain);
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
    public Result<Map<String, Object>> queryHostById(String id) {
        final String queryHostById = "SELECT * from host_t WHERE host_id = ?";
        Result<Map<String, Object>> result;
        try (final Connection conn = ds.getConnection()) {
            Map<String, Object> map = new HashMap<>();
            try (PreparedStatement statement = conn.prepareStatement(queryHostById)) {
                statement.setString(1, id);
                try (ResultSet resultSet = statement.executeQuery()) {
                    if (resultSet.next()) {
                        map.put("hostId", resultSet.getString("host_id"));
                        map.put("hostDomain", resultSet.getString("host_domain"));
                        map.put("orgName", resultSet.getString("org_name"));
                        map.put("orgDesc", resultSet.getString("org_desc"));
                        map.put("orgOwner", resultSet.getString("org_owner"));
                        map.put("jwk", resultSet.getString("jwk"));
                        map.put("updateUser", resultSet.getString("update_user"));
                        map.put("updateTs", resultSet.getTimestamp("update_ts"));
                    }
                }
            }
            if (map.size() == 0)
                result = Failure.of(new Status(OBJECT_NOT_FOUND, "host with id", id));
            else
                result = Success.of(map);
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
    public Result<Map<String, Object>> queryHostByOwner(String owner) {
        final String queryHostByOwner = "SELECT * from host_t WHERE org_owner = ?";
        Result<Map<String, Object>> result;
        try (final Connection conn = ds.getConnection()) {
            Map<String, Object> map = new HashMap<>();
            try (PreparedStatement statement = conn.prepareStatement(queryHostByOwner)) {
                statement.setString(1, owner);
                try (ResultSet resultSet = statement.executeQuery()) {
                    if (resultSet.next()) {
                        map.put("hostId", resultSet.getString("host_id"));
                        map.put("hostDomain", resultSet.getString("host_domain"));
                        map.put("orgName", resultSet.getString("org_name"));
                        map.put("orgDesc", resultSet.getString("org_desc"));
                        map.put("orgOwner", resultSet.getString("org_owner"));
                        map.put("jwk", resultSet.getString("jwk"));
                        map.put("updateUser", resultSet.getString("update_user"));
                        map.put("updateTs", resultSet.getTimestamp("update_ts"));
                    }
                }
            }
            if (map.size() == 0)
                result = Failure.of(new Status(OBJECT_NOT_FOUND, "host with owner ", owner));
            else
                result = Success.of(map);
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
    public Result<List<Map<String, Object>>> listHost() {
        final String listHost = "SELECT host_id, host_domain from host_t";
        Result<List<Map<String, Object>>> result;
        try (final Connection conn = ds.getConnection()) {
            List<Map<String, Object>> list = new ArrayList<>();
            try (PreparedStatement statement = conn.prepareStatement(listHost)) {
                try (ResultSet resultSet = statement.executeQuery()) {
                    while (resultSet.next()) {
                        Map<String, Object> map = new HashMap<>();
                        map.put("id", resultSet.getString("host_id"));
                        map.put("label", resultSet.getString("host_domain"));
                        list.add(map);
                    }
                }
            }
            if (list.isEmpty())
                result = Failure.of(new Status(OBJECT_NOT_FOUND, "host", "any key"));
            else
                result = Success.of(list);
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
    public Result<String> getHost(int limit, int offset) {
        final String getHost = "SELECT COUNT(*) OVER () AS total,\n" +
                "host_id, host_domain, org_name, org_desc, \n" +
                "org_owner, jwk, update_user, update_ts \n" +
                "FROM host_t LIMIT ? OFFSET ?";
        Result<String> result;
        int total = 0;
        List<Map<String, Object>> hosts = new ArrayList<>();
        try (final Connection conn = ds.getConnection()) {
            try (PreparedStatement statement = conn.prepareStatement(getHost)) {
                statement.setInt(1, limit);
                statement.setInt(2, offset);
                boolean isFirstRow = true;
                try (ResultSet resultSet = statement.executeQuery()) {
                    while (resultSet.next()) {
                        Map<String, Object> map = new HashMap<>();
                        if (isFirstRow) {
                            total = resultSet.getInt("total");
                            isFirstRow = false;
                        }
                        map.put("hostId", resultSet.getString("host_id"));
                        map.put("hostDomain", resultSet.getString("host_domain"));
                        map.put("orgName", resultSet.getString("org_name"));
                        map.put("orgDesc", resultSet.getString("org_desc"));
                        map.put("orgOwner", resultSet.getString("org_owner"));
                        map.put("jwk", resultSet.getString("jwk"));
                        map.put("updateUser", resultSet.getString("update_user"));
                        map.put("updateTs", resultSet.getTimestamp("update_ts"));
                        hosts.add(map);
                    }
                }
            }
            if(hosts.isEmpty()) {
                result = Failure.of(new Status(OBJECT_NOT_FOUND, "host", "limit and offset"));
            } else {
                Map<String, Object> resultMap = new HashMap<>();
                resultMap.put("total", total);
                resultMap.put("hosts", hosts);
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

    @Override
    public Result<String> createConfig(ConfigCreatedEvent event) {
        final String insertHost = "INSERT INTO configuration_t (configuration_id, configuration_type, infrastructure_type_id, class_path, configuration_description, update_user, update_ts) " +
                "VALUES (?, ?, ?, ?, ?,   ?, ?)";
        Result<String> result;
        try (Connection conn = ds.getConnection()) {
            conn.setAutoCommit(false);
            // no duplicate record, insert the user into database and write a success notification.
            try (PreparedStatement statement = conn.prepareStatement(insertHost)) {
                statement.setString(1, event.getConfigId());
                statement.setString(2, event.getConfigType());
                statement.setString(3, event.getInfraType());
                statement.setString(4, event.getClassPath());
                statement.setString(5, event.getConfigDesc());
                statement.setString(6, event.getEventId().getId());
                statement.setTimestamp(7, new Timestamp(event.getEventId().getTimestamp()));
                int count = statement.executeUpdate();
                if (count == 0) {
                    throw new SQLException("failed to insert the configuration with id " + event.getConfigId());
                }
                conn.commit();
                result = Success.of(event.getConfigId());
                insertNotification(event.getEventId(), event.getClass().getName(), AvroConverter.toJson(event, false), true, null);
            } catch (SQLException e) {
                logger.error("SQLException:", e);
                conn.rollback();
                insertNotification(event.getEventId(), event.getClass().getName(), AvroConverter.toJson(event, false), false, e.getMessage());
                result = Failure.of(new Status(SQL_EXCEPTION, e.getMessage()));
            } catch (Exception e) {
                logger.error("Exception:", e);
                conn.rollback();
                insertNotification(event.getEventId(), event.getClass().getName(), AvroConverter.toJson(event, false), false, e.getMessage());
                result = Failure.of(new Status(GENERIC_EXCEPTION, e.getMessage()));
            }
        } catch (SQLException e) {
            logger.error("SQLException:", e);
            result = Failure.of(new Status(SQL_EXCEPTION, e.getMessage()));
        }
        return result;
    }

    @Override
    public Result<String> updateConfig(ConfigUpdatedEvent event) {
        final String updateHost = "UPDATE configuration_t SET configuration_type = ?, infrastructure_type_id = ?, class_path = ?, configuration_description = ?, update_user = ? " +
                "update_ts = ? " +
                "WHERE configuration_id = ?";

        Result<String> result = null;
        try (Connection conn = ds.getConnection()) {
            conn.setAutoCommit(false);

            try (PreparedStatement statement = conn.prepareStatement(updateHost)) {
                if (event.getConfigType() != null) {
                    statement.setString(1, event.getConfigType());
                } else {
                    statement.setNull(1, NULL);
                }
                if (event.getInfraType() != null) {
                    statement.setString(2, event.getInfraType());
                } else {
                    statement.setNull(2, NULL);
                }
                if (event.getClassPath() != null) {
                    statement.setString(3, event.getClassPath());
                } else {
                    statement.setNull(3, NULL);
                }
                if (event.getConfigDesc() != null) {
                    statement.setString(4, event.getConfigDesc());
                } else {
                    statement.setNull(4, NULL);
                }
                statement.setString(5, event.getEventId().getId());
                statement.setTimestamp(6, new Timestamp(event.getEventId().getTimestamp()));
                statement.setString(7, event.getConfigId());

                int count = statement.executeUpdate();
                if (count == 0) {
                    throw new SQLException("no record is updated for configuration id " + event.getConfigId());
                }
                conn.commit();
                result = Success.of(event.getConfigId());
                insertNotification(event.getEventId(), event.getClass().getName(), AvroConverter.toJson(event, false), true, null);
            } catch (SQLException e) {
                logger.error("SQLException:", e);
                conn.rollback();
                insertNotification(event.getEventId(), event.getClass().getName(), AvroConverter.toJson(event, false), false, e.getMessage());
                result = Failure.of(new Status(SQL_EXCEPTION, e.getMessage()));
            } catch (Exception e) {
                logger.error("Exception:", e);
                conn.rollback();
                insertNotification(event.getEventId(), event.getClass().getName(), AvroConverter.toJson(event, false), false, e.getMessage());
                result = Failure.of(new Status(GENERIC_EXCEPTION, e.getMessage()));
            }
        } catch (SQLException e) {
            logger.error("SQLException:", e);
            result = Failure.of(new Status(SQL_EXCEPTION, e.getMessage()));
        }
        return result;
    }

    @Override
    public Result<String> deleteConfig(ConfigDeletedEvent event) {
        final String deleteHost = "DELETE from configuration_t WHERE configuration_id = ?";
        Result<String> result;
        try (Connection conn = ds.getConnection()) {
            conn.setAutoCommit(false);
            try (PreparedStatement statement = conn.prepareStatement(deleteHost)) {
                statement.setString(1, event.getConfigId());
                int count = statement.executeUpdate();
                if (count == 0) {
                    throw new SQLException("no record is deleted for configuration id " + event.getConfigId());
                }
                conn.commit();
                result = Success.of(event.getEventId().getId());
                insertNotification(event.getEventId(), event.getClass().getName(), AvroConverter.toJson(event, false), true, null);
            } catch (SQLException e) {
                logger.error("SQLException:", e);
                conn.rollback();
                insertNotification(event.getEventId(), event.getClass().getName(), AvroConverter.toJson(event, false), false, e.getMessage());
                result = Failure.of(new Status(SQL_EXCEPTION, e.getMessage()));
            } catch (Exception e) {
                logger.error("Exception:", e);
                conn.rollback();
                insertNotification(event.getEventId(), event.getClass().getName(), AvroConverter.toJson(event, false), false, e.getMessage());
                result = Failure.of(new Status(GENERIC_EXCEPTION, e.getMessage()));
            }
        } catch (SQLException e) {
            logger.error("SQLException:", e);
            result = Failure.of(new Status(SQL_EXCEPTION, e.getMessage()));
        }
        return result;
    }

    @Override
    public Result<Map<String, Object>> queryConfig() {
        final String queryConfig = "SELECT * from configuration_t";
        Result<Map<String, Object>> result;
        try (final Connection conn = ds.getConnection()) {
            Map<String, Object> map = new HashMap<>();
            try (PreparedStatement statement = conn.prepareStatement(queryConfig)) {
                try (ResultSet resultSet = statement.executeQuery()) {
                    if (resultSet.next()) {
                        map.put("configId", resultSet.getString("configuration_id"));
                        map.put("configType", resultSet.getString("configuration_type"));
                        map.put("infraType", resultSet.getString("infrastructure_type_id"));
                        map.put("classPath", resultSet.getString("class_path"));
                        map.put("configDesc", resultSet.getString("configuration_desc"));
                        map.put("updateUser", resultSet.getString("update_user"));
                        map.put("updateTs", resultSet.getTimestamp("update_ts"));
                    }
                }
            }
            if (map.size() == 0)
                result = Failure.of(new Status(OBJECT_NOT_FOUND, "configuration entry is registered"));
            else
                result = Success.of(map);
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
    public Result<Map<String, Object>> queryConfigById(String configId) {
        final String queryConfigById = "SELECT * from configuration_t WHERE configuration_id = ?";
        Result<Map<String, Object>> result;
        try (final Connection conn = ds.getConnection()) {
            Map<String, Object> map = new HashMap<>();
            try (PreparedStatement statement = conn.prepareStatement(queryConfigById)) {
                statement.setString(1, configId);
                try (ResultSet resultSet = statement.executeQuery()) {
                    if (resultSet.next()) {
                        map.put("configId", resultSet.getString("configuration_id"));
                        map.put("configType", resultSet.getString("configuration_type"));
                        map.put("infraType", resultSet.getString("infrastructure_type_id"));
                        map.put("classPath", resultSet.getString("class_path"));
                        map.put("configDesc", resultSet.getString("configuration_desc"));
                        map.put("updateUser", resultSet.getString("update_user"));
                        map.put("updateTs", resultSet.getTimestamp("update_ts"));
                    }
                }
            }
            if (map.size() == 0)
                result = Failure.of(new Status(OBJECT_NOT_FOUND, "configuration with id ", configId));
            else
                result = Success.of(map);
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
    public Result<Map<String, Object>> queryCurrentHostKey(String hostId) {
        final String queryConfigById = "SELECT * from host_key_t WHERE host_id = ? AND key_type = 'C'";
        Result<Map<String, Object>> result;
        try (final Connection conn = ds.getConnection()) {
            Map<String, Object> map = new HashMap<>();
            try (PreparedStatement statement = conn.prepareStatement(queryConfigById)) {
                statement.setString(1, hostId);
                try (ResultSet resultSet = statement.executeQuery()) {
                    if (resultSet.next()) {
                        map.put("hostId", resultSet.getString("host_id"));
                        map.put("kid", resultSet.getString("kid"));
                        map.put("publicKey", resultSet.getString("public_key"));
                        map.put("privateKey", resultSet.getString("private_key"));
                        map.put("keyType", resultSet.getString("key_type"));
                        map.put("updateUser", resultSet.getString("update_user"));
                        map.put("updateTs", resultSet.getTimestamp("update_ts"));
                    }
                }
            }
            if (map.isEmpty())
                result = Failure.of(new Status(OBJECT_NOT_FOUND, "host key with id", hostId));
            else
                result = Success.of(map);
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
    public Result<Map<String, Object>> queryLongLiveHostKey(String hostId) {
        final String queryConfigById = "SELECT * from host_key_t WHERE host_id = ? AND key_type = 'L'";
        Result<Map<String, Object>> result;
        try (final Connection conn = ds.getConnection()) {
            Map<String, Object> map = new HashMap<>();
            try (PreparedStatement statement = conn.prepareStatement(queryConfigById)) {
                statement.setString(1, hostId);
                try (ResultSet resultSet = statement.executeQuery()) {
                    if (resultSet.next()) {
                        map.put("hostId", resultSet.getString("host_id"));
                        map.put("kid", resultSet.getString("kid"));
                        map.put("publicKey", resultSet.getString("public_key"));
                        map.put("privateKey", resultSet.getString("private_key"));
                        map.put("keyType", resultSet.getString("key_type"));
                        map.put("updateUser", resultSet.getString("update_user"));
                        map.put("updateTs", resultSet.getTimestamp("update_ts"));
                    }
                }
            }
            if (map.isEmpty())
                result = Failure.of(new Status(OBJECT_NOT_FOUND, "host key with id", hostId));
            else
                result = Success.of(map);
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
    public Result<String> createRule(RuleCreatedEvent event) {
        final String insertRule = "INSERT INTO rule_t (rule_id, rule_name, rule_version, rule_type, rule_group, " +
                "rule_desc, rule_body, rule_owner, common, update_user, update_ts) " +
                "VALUES (?, ?, ?, ?, ?,   ?, ?, ?, ?, ?,  ?)";
        final String insertHostRule = "INSERT INTO rule_host_t (host_id, rule_id, update_user, update_ts) " +
                "VALUES (?, ?, ?, ?)";

        Result<String> result = null;
        try (Connection conn = ds.getConnection()) {
            conn.setAutoCommit(false);
            // no duplicate record, insert the user into database and write a success notification.
            try {
                try (PreparedStatement statement = conn.prepareStatement(insertRule)) {
                    statement.setString(1, event.getRuleId());
                    statement.setString(2, event.getRuleName());
                    statement.setString(3, event.getRuleVersion());
                    statement.setString(4, event.getRuleType());
                    if (event.getRuleGroup() != null)
                        statement.setString(5, event.getRuleGroup());
                    else
                        statement.setNull(5, NULL);

                    if (event.getRuleDesc() != null)
                        statement.setString(6, event.getRuleDesc());
                    else
                        statement.setNull(6, NULL);
                    statement.setString(7, event.getRuleBody());
                    statement.setString(8, event.getRuleOwner());
                    statement.setString(9, event.getCommon());
                    statement.setString(10, event.getEventId().getId());
                    statement.setTimestamp(11, new Timestamp(System.currentTimeMillis()));
                    int count = statement.executeUpdate();
                    if (count == 0) {
                        throw new SQLException("failed to insert the rule " + event.getRuleId());
                    }
                }
                try (PreparedStatement statement = conn.prepareStatement(insertHostRule)) {
                    statement.setString(1, event.getHostId());
                    statement.setString(2, event.getRuleId());
                    statement.setString(3, event.getEventId().getId());
                    statement.setTimestamp(4, new Timestamp(System.currentTimeMillis()));
                    int count = statement.executeUpdate();
                    if (count == 0) {
                        throw new SQLException("failed to insert the host_rule for host " + event.getHostId() + " rule " + event.getRuleId());
                    }
                }
                conn.commit();
                result = Success.of(event.getRuleId());
                insertNotification(event.getEventId(), event.getClass().getName(), AvroConverter.toJson(event, false), true, null);
            } catch (SQLException e) {
                logger.error("SQLException:", e);
                conn.rollback();
                insertNotification(event.getEventId(), event.getClass().getName(), AvroConverter.toJson(event, false), false, e.getMessage());
                result = Failure.of(new Status(SQL_EXCEPTION, e.getMessage()));
            } catch (Exception e) {
                logger.error("Exception:", e);
                conn.rollback();
                insertNotification(event.getEventId(), event.getClass().getName(), AvroConverter.toJson(event, false), false, e.getMessage());
                result = Failure.of(new Status(GENERIC_EXCEPTION, e.getMessage()));
            }
        } catch (SQLException e) {
            logger.error("SQLException:", e);
            result = Failure.of(new Status(SQL_EXCEPTION, e.getMessage()));
        }
        return result;
    }

    @Override
    public Result<String> updateRule(RuleUpdatedEvent event) {
        final String updateRule = "UPDATE rule_t SET rule_name = ?, rule_version = ?, rule_type = ?, rule_group = ?, rule_desc = ?, " +
                "rule_body = ?, rule_owner = ?, common = ?, update_user = ?, update_ts = ? " +
                "WHERE rule_id = ?";

        Result<String> result = null;
        try (Connection conn = ds.getConnection()) {
            conn.setAutoCommit(false);

            try (PreparedStatement statement = conn.prepareStatement(updateRule)) {
                if(event.getRuleName() != null) {
                    statement.setString(1, event.getRuleName());
                } else {
                    statement.setNull(1, NULL);
                }
                if (event.getRuleVersion() != null) {
                    statement.setString(2, event.getRuleVersion());
                } else {
                    statement.setNull(2, NULL);
                }
                if (event.getRuleType() != null) {
                    statement.setString(3, event.getRuleType());
                } else {
                    statement.setNull(3, NULL);
                }
                if (event.getRuleGroup() != null) {
                    statement.setString(4, event.getRuleGroup());
                } else {
                    statement.setNull(4, NULL);
                }
                if (event.getRuleDesc() != null) {
                    statement.setString(5, event.getRuleDesc());
                } else {
                    statement.setNull(5, NULL);
                }
                if(event.getRuleBody() != null) {
                    statement.setString(6, event.getRuleBody());
                } else {
                    statement.setNull(6, NULL);
                }
                if(event.getRuleOwner() != null) {
                    statement.setString(7, event.getRuleOwner());
                } else {
                    statement.setNull(7, NULL);
                }
                if(event.getCommon() != null) {
                    statement.setString(8, event.getCommon());
                } else {
                    statement.setNull(8, NULL);
                }
                statement.setString(9, event.getEventId().getId());
                statement.setTimestamp(10, new Timestamp(event.getEventId().getTimestamp()));
                statement.setString(11, event.getRuleId());

                int count = statement.executeUpdate();
                if (count == 0) {
                    throw new SQLException("no record is updated for rule " + event.getRuleId());
                }
                conn.commit();
                result = Success.of(event.getRuleId());
                insertNotification(event.getEventId(), event.getClass().getName(), AvroConverter.toJson(event, false), true, null);
            } catch (SQLException e) {
                logger.error("SQLException:", e);
                conn.rollback();
                insertNotification(event.getEventId(), event.getClass().getName(), AvroConverter.toJson(event, false), false, e.getMessage());
                result = Failure.of(new Status(SQL_EXCEPTION, e.getMessage()));
            } catch (Exception e) {
                logger.error("Exception:", e);
                conn.rollback();
                insertNotification(event.getEventId(), event.getClass().getName(), AvroConverter.toJson(event, false), false, e.getMessage());
                result = Failure.of(new Status(GENERIC_EXCEPTION, e.getMessage()));
            }
        } catch (SQLException e) {
            logger.error("SQLException:", e);
            result = Failure.of(new Status(SQL_EXCEPTION, e.getMessage()));
        }
        return result;

    }

    @Override
    public Result<String> deleteRule(RuleDeletedEvent event) {
        final String deleteRule = "DELETE from rule_t WHERE rule_id = ?";
        final String deleteHostRule = "DELETE from rule_host_t WHERE host_id = ? AND rule_id = ?";
        Result<String> result;
        try (Connection conn = ds.getConnection()) {
            conn.setAutoCommit(false);
            try {
                try (PreparedStatement statement = conn.prepareStatement(deleteRule)) {
                    statement.setString(1, event.getRuleId());
                    int count = statement.executeUpdate();
                    if (count == 0) {
                        throw new SQLException("no record is deleted for rule " + event.getRuleId());
                    }
                }
                try (PreparedStatement statement = conn.prepareStatement(deleteHostRule)) {
                    statement.setString(1, event.getHostId());
                    statement.setString(2, event.getRuleId());
                    int count = statement.executeUpdate();
                    if (count == 0) {
                        throw new SQLException("no record is deleted for host " + event.getHostId() + " rule " + event.getRuleId());
                    }
                }
                conn.commit();
                result = Success.of(event.getEventId().getId());
                insertNotification(event.getEventId(), event.getClass().getName(), AvroConverter.toJson(event, false), true, null);
            } catch (SQLException e) {
                logger.error("SQLException:", e);
                conn.rollback();
                insertNotification(event.getEventId(), event.getClass().getName(), AvroConverter.toJson(event, false), false, e.getMessage());
                result = Failure.of(new Status(SQL_EXCEPTION, e.getMessage()));
            } catch (Exception e) {
                logger.error("Exception:", e);
                conn.rollback();
                insertNotification(event.getEventId(), event.getClass().getName(), AvroConverter.toJson(event, false), false, e.getMessage());
                result = Failure.of(new Status(GENERIC_EXCEPTION, e.getMessage()));
            }
        } catch (SQLException e) {
            logger.error("SQLException:", e);
            result = Failure.of(new Status(SQL_EXCEPTION, e.getMessage()));
        }
        return result;
    }


    @Override
    public Result<List<Map<String, Object>>> queryRuleByHostGroup(String hostId, String groupId) {
        Result<List<Map<String, Object>>> result;
        String sql = "SELECT rule_id, host_id, rule_type, rule_group, rule_visibility, rule_description, rule_body, rule_owner " +
                "update_user, update_ts " +
                "FROM rule_t WHERE host_id = ? AND rule_group = ?";
        try (final Connection conn = ds.getConnection()) {
            List<Map<String, Object>> list = new ArrayList<>();
            try (PreparedStatement statement = conn.prepareStatement(sql)) {
                statement.setString(1, hostId);
                statement.setString(2, groupId);
                try (ResultSet resultSet = statement.executeQuery()) {
                    while (resultSet.next()) {
                        Map<String, Object> map = new HashMap<>();
                        map.put("ruleId", resultSet.getString("rule_id"));
                        map.put("hostId", resultSet.getString("host_id"));
                        map.put("ruleType", resultSet.getString("rule_type"));
                        map.put("ruleGroup", resultSet.getBoolean("rule_group"));
                        map.put("ruleVisibility", resultSet.getString("rule_visibility"));
                        map.put("ruleDescription", resultSet.getString("rule_description"));
                        map.put("ruleBody", resultSet.getString("rule_body"));
                        map.put("ruleOwner", resultSet.getString("rule_owner"));
                        map.put("updateUser", resultSet.getString("update_user"));
                        map.put("updateTs", resultSet.getTimestamp("update_ts"));
                        list.add(map);
                    }
                }
            }
            if (list.isEmpty())
                result = Failure.of(new Status(OBJECT_NOT_FOUND, "rule with rule group ", groupId));
            else
                result = Success.of(list);
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
    public Result<String> queryRule(int offset, int limit, String hostId, String ruleId, String ruleName,
                                    String ruleVersion, String ruleType, String ruleGroup, String ruleDesc,
                                    String ruleBody, String ruleOwner, String common) {
        Result<String> result;
        String sql;
        List<Object> parameters = new ArrayList<>();
        if(common == null || common.equalsIgnoreCase("N")) {
            StringBuilder sqlBuilder = new StringBuilder();
            sqlBuilder.append("SELECT COUNT(*) OVER () AS total, h.host_id, r.rule_id, r.rule_name, r.rule_version, " +
                    "r.rule_type, r.rule_group, r.common, r.rule_desc, r.rule_body, r.rule_owner, " +
                    "r.update_user, r.update_ts " +
                    "FROM rule_t r, rule_host_t h " +
                    "WHERE r.rule_id = h.rule_id " +
                    "AND h.host_id = ?\n");
            parameters.add(hostId);

            StringBuilder whereClause = new StringBuilder();

            addCondition(whereClause, parameters, "r.rule_id", ruleId);
            addCondition(whereClause, parameters, "r.rule_name", ruleName);
            addCondition(whereClause, parameters, "r.rule_version", ruleVersion);
            addCondition(whereClause, parameters, "r.rule_type", ruleType);
            addCondition(whereClause, parameters, "r.rule_group", ruleGroup);
            addCondition(whereClause, parameters, "r.rule_desc", ruleDesc);
            addCondition(whereClause, parameters, "r.rule_body", ruleBody);
            addCondition(whereClause, parameters, "r.rule_owner", ruleOwner);

            if (whereClause.length() > 0) {
                sqlBuilder.append("AND ").append(whereClause);
            }
            sqlBuilder.append(" ORDER BY rule_id\n" +
                    "LIMIT ? OFFSET ?");

            parameters.add(limit);
            parameters.add(offset);
            sql = sqlBuilder.toString();
        } else {
            StringBuilder sqlBuilder = new StringBuilder();
            sqlBuilder.append("SELECT \n" +
                    "                        COUNT(*) OVER () AS total,\n" +
                    "                        host_id,\n" +
                    "                        rule_id,\n" +
                    "                        rule_name,\n" +
                    "                        rule_version,\n" +
                    "                        rule_type,\n" +
                    "                        rule_group,\n" +
                    "                        common,\n" +
                    "                        rule_desc,\n" +
                    "                        rule_body,\n" +
                    "                        rule_owner,\n" +
                    "                        update_user,\n" +
                    "                        update_ts\n" +
                    "                    FROM (\n" +
                    "                       SELECT \n" +
                    "                        h.host_id,\n" +
                    "                        r.rule_id,\n" +
                    "                        r.rule_name,\n" +
                    "                        r.rule_version,\n" +
                    "                        r.rule_type,\n" +
                    "                        r.rule_group,\n" +
                    "                        r.common,\n" +
                    "                        r.rule_desc,\n" +
                    "                        r.rule_body,\n" +
                    "                        r.rule_owner,\n" +
                    "                        r.update_user,\n" +
                    "                        r.update_ts\n" +
                    "                    FROM rule_t r\n" +
                    "                    JOIN rule_host_t h ON r.rule_id = h.rule_id\n" +
                    "                    WHERE h.host_id = ?\n");
            parameters.add(hostId);
            StringBuilder whereClause = new StringBuilder();

            addCondition(whereClause, parameters, "r.rule_id", ruleId);
            addCondition(whereClause, parameters, "r.rule_name", ruleName);
            addCondition(whereClause, parameters, "r.rule_version", ruleVersion);
            addCondition(whereClause, parameters, "r.rule_type", ruleType);
            addCondition(whereClause, parameters, "r.rule_group", ruleGroup);
            addCondition(whereClause, parameters, "r.rule_desc", ruleDesc);
            addCondition(whereClause, parameters, "r.rule_body", ruleBody);
            addCondition(whereClause, parameters, "r.rule_owner", ruleOwner);
            if (whereClause.length() > 0) {
                sqlBuilder.append("AND ").append(whereClause);
            }

            sqlBuilder.append("                    \n" +
                    "                    UNION ALL\n" +
                    "                    \n" +
                    "                   SELECT\n" +
                    "                        h.host_id,\n" +
                    "                        r.rule_id,\n" +
                    "                        r.rule_name,\n" +
                    "                        r.rule_version,\n" +
                    "                        r.rule_type,\n" +
                    "                        r.rule_group,\n" +
                    "                        r.common,\n" +
                    "                        r.rule_desc,\n" +
                    "                        r.rule_body,\n" +
                    "                        r.rule_owner,\n" +
                    "                        r.update_user,\n" +
                    "                        r.update_ts\n" +
                    "                    FROM rule_t r\n" +
                    "                    JOIN rule_host_t h ON r.rule_id = h.rule_id\n" +
                    "                    WHERE r.common = 'Y'\n" +
                    "                      AND h.host_id != ?\n" +
                    "                       AND  NOT EXISTS (\n" +
                    "                         SELECT 1\n" +
                    "                        FROM rule_host_t eh\n" +
                    "                         WHERE eh.rule_id = r.rule_id\n" +
                    "                         AND eh.host_id=?\n" +
                    "                     )\n");
            parameters.add(hostId);
            parameters.add(hostId);


            StringBuilder whereClauseCommon = new StringBuilder();
            addCondition(whereClauseCommon, parameters, "r.rule_id", ruleId);
            addCondition(whereClauseCommon, parameters, "r.rule_name", ruleName);
            addCondition(whereClauseCommon, parameters, "r.rule_version", ruleVersion);
            addCondition(whereClauseCommon, parameters, "r.rule_type", ruleType);
            addCondition(whereClauseCommon, parameters, "r.rule_group", ruleGroup);
            addCondition(whereClauseCommon, parameters, "r.rule_desc", ruleDesc);
            addCondition(whereClauseCommon, parameters, "r.rule_body", ruleBody);
            addCondition(whereClauseCommon, parameters, "r.rule_owner", ruleOwner);

            if (whereClauseCommon.length() > 0) {
                sqlBuilder.append("AND ").append(whereClauseCommon);
            }


            sqlBuilder.append("                 ) AS combined_rules\n");

            sqlBuilder.append("ORDER BY rule_id\n" +
                    "LIMIT ? OFFSET ?");

            parameters.add(limit);
            parameters.add(offset);

            sql = sqlBuilder.toString();
        }

        int total = 0;
        List<Map<String, Object>> rules = new ArrayList<>();

        try (final Connection conn = ds.getConnection(); PreparedStatement preparedStatement = conn.prepareStatement(sql)) {
            for (int i = 0; i < parameters.size(); i++) {
                preparedStatement.setObject(i + 1, parameters.get(i));
            }
            boolean isFirstRow = true;

            try (ResultSet resultSet = preparedStatement.executeQuery()) {
                while (resultSet.next()) {
                    Map<String, Object> map = new HashMap<>();
                    // only get the total once as it is the same for all rows.
                    if (isFirstRow) {
                        total = resultSet.getInt("total");
                        isFirstRow = false;
                    }
                    map.put("hostId", resultSet.getString("host_id"));
                    map.put("ruleId", resultSet.getString("rule_id"));
                    map.put("ruleName", resultSet.getString("rule_name"));
                    map.put("ruleVersion", resultSet.getString("rule_version"));
                    map.put("ruleType", resultSet.getString("rule_type"));
                    map.put("ruleGroup", resultSet.getBoolean("rule_group"));
                    map.put("common", resultSet.getString("common"));
                    map.put("ruleDesc", resultSet.getString("rule_desc"));
                    map.put("ruleBody", resultSet.getString("rule_body"));
                    map.put("ruleOwner", resultSet.getString("rule_owner"));
                    map.put("updateUser", resultSet.getString("update_user"));
                    map.put("updateTs", resultSet.getTimestamp("update_ts"));
                    rules.add(map);
                }
            }
            Map<String, Object> resultMap = new HashMap<>();
            resultMap.put("total", total);
            resultMap.put("rules", rules);
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
    public Result<Map<String, Object>> queryRuleById(String ruleId) {
        Result<Map<String, Object>> result;
        String sql = "SELECT rule_id, host_id, rule_type, rule_group, rule_visibility, rule_description, rule_body, rule_owner " +
                "update_user, update_ts " +
                "FROM rule_t WHERE rule_id = ?";
        try (final Connection conn = ds.getConnection()) {
            Map<String, Object> map = new HashMap<>();
            try (PreparedStatement statement = conn.prepareStatement(sql)) {
                statement.setString(1, ruleId);
                try (ResultSet resultSet = statement.executeQuery()) {
                    if (resultSet.next()) {
                        map.put("ruleId", resultSet.getString("rule_id"));
                        map.put("hostId", resultSet.getString("host_id"));
                        map.put("ruleType", resultSet.getString("rule_type"));
                        map.put("ruleGroup", resultSet.getBoolean("rule_group"));
                        map.put("ruleVisibility", resultSet.getString("rule_visibility"));
                        map.put("ruleDescription", resultSet.getString("rule_description"));
                        map.put("ruleBody", resultSet.getString("rule_body"));
                        map.put("ruleOwner", resultSet.getString("rule_owner"));
                        map.put("updateUser", resultSet.getString("update_user"));
                        map.put("updateTs", resultSet.getTimestamp("update_ts"));
                    }
                }
            }
            if (map.isEmpty())
                result = Failure.of(new Status(OBJECT_NOT_FOUND, "rule with ruleId ", ruleId));
            else
                result = Success.of(map);
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
    public Result<String> queryRuleByHostType(String hostId, String ruleType) {
        Result<String> result;
        String sql = "SELECT r.rule_id\n" +
                "FROM rule_t r, rule_host_t h\n" +
                "WHERE r.rule_id = h.rule_id\n" +
                "AND h.host_id = ?\n" +
                "AND r.rule_type = ?\n" +
                "UNION\n" +
                "SELECT r.rule_id r\n" +
                "FROM rule_t r, rule_host_t h\n" +
                "WHERE h.host_id != ?\n" +
                "AND r.rule_type = ?\n" +
                "AND r.common = 'Y'";
        try (final Connection conn = ds.getConnection()) {
            List<Map<String, Object>> list = new ArrayList<>();
            try (PreparedStatement statement = conn.prepareStatement(sql)) {
                statement.setString(1, hostId);
                statement.setString(2, ruleType);
                statement.setString(3, hostId);
                statement.setString(4, ruleType);

                try (ResultSet resultSet = statement.executeQuery()) {
                    while (resultSet.next()) {
                        Map<String, Object> map = new HashMap<>();
                        map.put("id", resultSet.getString("rule_id"));
                        map.put("label", resultSet.getString("rule_id"));
                        list.add(map);
                    }
                }
            }
            if (list.isEmpty())
                result = Failure.of(new Status(OBJECT_NOT_FOUND, "rule with host id and rule type ", hostId  + "|" + ruleType));
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
    public Result<String> createApiRule(ApiRuleCreatedEvent event) {
        final String insertApiRule = "INSERT INTO api_rule_t (api_id, rule_id, update_user, update_ts) " +
                "VALUES (?, ?, ?, ?)";
        Result<String> result = null;
        List<String> ruleIds = event.getRuleIds();
        String apiId = event.getApiId();
        Timestamp ts = new Timestamp(System.currentTimeMillis());

        try (Connection conn = ds.getConnection()) {
            conn.setAutoCommit(false);
            // no duplicate record, insert the user into database and write a success notification.
            try (PreparedStatement statement = conn.prepareStatement(insertApiRule)) {
                int i = 0;
                for (String ruleId : ruleIds) {
                    statement.setString(1, apiId);
                    statement.setString(2, ruleId);
                    statement.setString(3, event.getEventId().getId());
                    statement.setTimestamp(4, ts);
                    statement.addBatch();
                    i++;
                    if (i % 1000 == 0 || i == ruleIds.size()) {
                        statement.executeBatch();
                    }
                }
                if (ruleIds.isEmpty()) {
                    throw new SQLException("failed to insert the Api rule " + event.getApiId());
                }
                conn.commit();
                result = Success.of(event.getApiId());
                insertNotification(event.getEventId(), event.getClass().getName(), AvroConverter.toJson(event, false), true, null);
            } catch (SQLException e) {
                logger.error("SQLException:", e);
                conn.rollback();
                insertNotification(event.getEventId(), event.getClass().getName(), AvroConverter.toJson(event, false), false, e.getMessage());
                result = Failure.of(new Status(SQL_EXCEPTION, e.getMessage()));
            } catch (Exception e) {
                logger.error("Exception:", e);
                conn.rollback();
                insertNotification(event.getEventId(), event.getClass().getName(), AvroConverter.toJson(event, false), false, e.getMessage());
                result = Failure.of(new Status(GENERIC_EXCEPTION, e.getMessage()));
            }
        } catch (SQLException e) {
            logger.error("SQLException:", e);
            result = Failure.of(new Status(SQL_EXCEPTION, e.getMessage()));
        }
        return result;
    }

    @Override
    public Result<String> deleteApiRule(ApiRuleDeletedEvent event) {
        final String deleteApiRule = "DELETE FROM api_rule_t WHERE api_id = ? AND rule_id = ?";
        Result<String> result = null;
        List<String> ruleIds = event.getRuleIds();
        String apiId = event.getApiId();

        try (Connection conn = ds.getConnection()) {
            conn.setAutoCommit(false);
            // no duplicate record, insert the user into database and write a success notification.
            try (PreparedStatement statement = conn.prepareStatement(deleteApiRule)) {
                int i = 0;
                for (String ruleId : ruleIds) {
                    statement.setString(1, apiId);
                    statement.setString(2, ruleId);
                    statement.addBatch();
                    i++;
                    if (i % 1000 == 0 || i == ruleIds.size()) {
                        statement.executeBatch();
                    }
                }
                if (ruleIds.isEmpty()) {
                    throw new SQLException("failed to delete the Api rule " + event.getApiId());
                }
                conn.commit();
                result = Success.of(event.getApiId());
                insertNotification(event.getEventId(), event.getClass().getName(), AvroConverter.toJson(event, false), true, null);
            } catch (SQLException e) {
                logger.error("SQLException:", e);
                conn.rollback();
                insertNotification(event.getEventId(), event.getClass().getName(), AvroConverter.toJson(event, false), false, e.getMessage());
                result = Failure.of(new Status(SQL_EXCEPTION, e.getMessage()));
            } catch (Exception e) {
                logger.error("Exception:", e);
                conn.rollback();
                insertNotification(event.getEventId(), event.getClass().getName(), AvroConverter.toJson(event, false), false, e.getMessage());
                result = Failure.of(new Status(GENERIC_EXCEPTION, e.getMessage()));
            }
        } catch (SQLException e) {
            logger.error("SQLException:", e);
            result = Failure.of(new Status(SQL_EXCEPTION, e.getMessage()));
        }
        return result;
    }

    @Override
    public Result<List<Map<String, Object>>> queryRuleByHostApiId(String hostId, String apiId, String apiVersion) {
        Result<List<Map<String, Object>>> result;
        String sql = "SELECT h.host_id, r.rule_id, r.rule_type, a.endpoint, r.rule_body\n" +
                "FROM rule_t r, rule_host_t h, api_endpoint_rule_t a \n" +
                "WHERE r.rule_id = h.rule_id\n" +
                "AND h.host_id = a.host_id\n" +
                "AND h.host_id = ?\n" +
                "AND a.api_id = ?\n" +
                "AND a.api_version = ?";
        try (final Connection conn = ds.getConnection()) {
            List<Map<String, Object>> list = new ArrayList<>();
            try (PreparedStatement statement = conn.prepareStatement(sql)) {
                statement.setString(1, hostId);
                statement.setString(2, apiId);
                statement.setString(3, apiVersion);
                try (ResultSet resultSet = statement.executeQuery()) {
                    while (resultSet.next()) {
                        Map<String, Object> map = new HashMap<>();
                        map.put("hostId", resultSet.getString("host_id"));
                        map.put("ruleId", resultSet.getString("rule_id"));
                        map.put("ruleType", resultSet.getString("rule_type"));
                        map.put("endpoint", resultSet.getString("endpoint"));
                        map.put("ruleBody", resultSet.getString("rule_body"));
                        list.add(map);
                    }
                }
            }
            if (list.isEmpty())
                result = Failure.of(new Status(OBJECT_NOT_FOUND, "rule with hostId " + hostId + " apiId " + apiId + " apiVersion " + apiVersion));
            else
                result = Success.of(list);
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
    public Result<String> createRole(RoleCreatedEvent event) {
        final String insertRole = "INSERT INTO role_t (host_id, role_id, role_desc, update_user, update_ts) " +
                "VALUES (?, ?, ?, ?, ?)";

        Result<String> result = null;
        try (Connection conn = ds.getConnection()) {
            conn.setAutoCommit(false);
            // no duplicate record, insert the user into database and write a success notification.
            try (PreparedStatement statement = conn.prepareStatement(insertRole)) {
                statement.setString(1, event.getHostId());
                statement.setString(2, event.getRoleId());
                if (event.getRoleDesc() != null)
                    statement.setString(3, event.getRoleDesc());
                else
                    statement.setNull(3, NULL);

                statement.setString(4, event.getEventId().getId());
                statement.setTimestamp(5, new Timestamp(event.getEventId().getTimestamp()));

                int count = statement.executeUpdate();
                if (count == 0) {
                    throw new SQLException("failed to insert role " + event.getRoleId());
                }
                conn.commit();
                result = Success.of(event.getRoleId());
                insertNotification(event.getEventId(), event.getClass().getName(), AvroConverter.toJson(event, false), true, null);
            } catch (SQLException e) {
                logger.error("SQLException:", e);
                conn.rollback();
                insertNotification(event.getEventId(), event.getClass().getName(), AvroConverter.toJson(event, false), false, e.getMessage());
                result = Failure.of(new Status(SQL_EXCEPTION, e.getMessage()));
            } catch (Exception e) {
                logger.error("Exception:", e);
                conn.rollback();
                insertNotification(event.getEventId(), event.getClass().getName(), AvroConverter.toJson(event, false), false, e.getMessage());
                result = Failure.of(new Status(GENERIC_EXCEPTION, e.getMessage()));
            }
        } catch (SQLException e) {
            logger.error("SQLException:", e);
            result = Failure.of(new Status(SQL_EXCEPTION, e.getMessage()));
        }
        return result;
    }

    @Override
    public Result<String> updateRole(RoleUpdatedEvent event) {
        final String updateRole = "UPDATE role_t SET role_desc = ?, update_user = ?, update_ts = ? " +
                "WHERE host_id = ? AND role_id = ?";

        Result<String> result = null;
        try (Connection conn = ds.getConnection()) {
            conn.setAutoCommit(false);

            try (PreparedStatement statement = conn.prepareStatement(updateRole)) {
                if(event.getRoleDesc() != null) {
                    statement.setString(1, event.getRoleDesc());
                } else {
                    statement.setNull(1, NULL);
                }
                statement.setString(2, event.getEventId().getId());
                statement.setTimestamp(3, new Timestamp(event.getEventId().getTimestamp()));
                statement.setString(4, event.getHostId());
                statement.setString(5, event.getRoleId());

                int count = statement.executeUpdate();
                if (count == 0) {
                    throw new SQLException("no record is updated for role " + event.getRoleId());
                }
                conn.commit();
                result = Success.of(event.getRoleId());
                insertNotification(event.getEventId(), event.getClass().getName(), AvroConverter.toJson(event, false), true, null);
            } catch (SQLException e) {
                logger.error("SQLException:", e);
                conn.rollback();
                insertNotification(event.getEventId(), event.getClass().getName(), AvroConverter.toJson(event, false), false, e.getMessage());
                result = Failure.of(new Status(SQL_EXCEPTION, e.getMessage()));
            } catch (Exception e) {
                logger.error("Exception:", e);
                conn.rollback();
                insertNotification(event.getEventId(), event.getClass().getName(), AvroConverter.toJson(event, false), false, e.getMessage());
                result = Failure.of(new Status(GENERIC_EXCEPTION, e.getMessage()));
            }
        } catch (SQLException e) {
            logger.error("SQLException:", e);
            result = Failure.of(new Status(SQL_EXCEPTION, e.getMessage()));
        }
        return result;

    }

    @Override
    public Result<String> deleteRole(RoleDeletedEvent event) {
        final String deleteRole = "DELETE from role_t WHERE host_id = ? AND role_id = ?";
        Result<String> result;
        try (Connection conn = ds.getConnection()) {
            conn.setAutoCommit(false);
            try (PreparedStatement statement = conn.prepareStatement(deleteRole)) {
                statement.setString(1, event.getHostId());
                statement.setString(2, event.getRoleId());
                int count = statement.executeUpdate();
                if (count == 0) {
                    throw new SQLException("no record is deleted for role " + event.getRoleId());
                }
                conn.commit();
                result = Success.of(event.getEventId().getId());
                insertNotification(event.getEventId(), event.getClass().getName(), AvroConverter.toJson(event, false), true, null);
            } catch (SQLException e) {
                logger.error("SQLException:", e);
                conn.rollback();
                insertNotification(event.getEventId(), event.getClass().getName(), AvroConverter.toJson(event, false), false, e.getMessage());
                result = Failure.of(new Status(SQL_EXCEPTION, e.getMessage()));
            } catch (Exception e) {
                logger.error("Exception:", e);
                conn.rollback();
                insertNotification(event.getEventId(), event.getClass().getName(), AvroConverter.toJson(event, false), false, e.getMessage());
                result = Failure.of(new Status(GENERIC_EXCEPTION, e.getMessage()));
            }
        } catch (SQLException e) {
            logger.error("SQLException:", e);
            result = Failure.of(new Status(SQL_EXCEPTION, e.getMessage()));
        }
        return result;
    }

    @Override
    public Result<String> queryRole(int offset, int limit, String hostId, String roleId, String roleDesc) {
        Result<String> result;
        StringBuilder sqlBuilder = new StringBuilder();
        sqlBuilder.append("SELECT COUNT(*) OVER () AS total, host_id, role_id, role_desc, update_user, update_ts " +
                "FROM role_t " +
                "WHERE host_id = ?\n");


        List<Object> parameters = new ArrayList<>();
        parameters.add(hostId);


        StringBuilder whereClause = new StringBuilder();

        addCondition(whereClause, parameters, "role_id", roleId);
        addCondition(whereClause, parameters, "role_desc", roleDesc);


        if (whereClause.length() > 0) {
            sqlBuilder.append("AND ").append(whereClause);
        }

        sqlBuilder.append(" ORDER BY role_id\n" +
                "LIMIT ? OFFSET ?");

        parameters.add(limit);
        parameters.add(offset);

        String sql = sqlBuilder.toString();
        if(logger.isTraceEnabled()) logger.trace("queryRole sql: {}", sql);
        int total = 0;
        List<Map<String, Object>> roles = new ArrayList<>();


        try (final Connection conn = ds.getConnection(); PreparedStatement preparedStatement = conn.prepareStatement(sql)) {
            for (int i = 0; i < parameters.size(); i++) {
                preparedStatement.setObject(i + 1, parameters.get(i));
            }
            boolean isFirstRow = true;
            try (ResultSet resultSet = preparedStatement.executeQuery()) {
                while (resultSet.next()) {
                    Map<String, Object> map = new HashMap<>();
                    // only get the total once as it is the same for all rows.
                    if (isFirstRow) {
                        total = resultSet.getInt("total");
                        isFirstRow = false;
                    }
                    map.put("hostId", resultSet.getString("host_id"));
                    map.put("roleId", resultSet.getString("role_id"));
                    map.put("roleDesc", resultSet.getString("role_desc"));
                    map.put("updateUser", resultSet.getString("update_user"));
                    map.put("updateTs", resultSet.getTimestamp("update_ts"));
                    roles.add(map);
                }
            }
            Map<String, Object> resultMap = new HashMap<>();
            resultMap.put("total", total);
            resultMap.put("roles", roles);
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
    public Result<String> queryRoleLabel(String hostId) {
        final String sql = "SELECT role_id from role_t WHERE host_id = ?";
        Result<String> result;
        try (final Connection conn = ds.getConnection()) {
            List<Map<String, Object>> list = new ArrayList<>();
            try (PreparedStatement statement = conn.prepareStatement(sql)) {
                statement.setString(1, hostId);
                try (ResultSet resultSet = statement.executeQuery()) {
                    while (resultSet.next()) {
                        Map<String, Object> map = new HashMap<>();
                        String id = resultSet.getString("role_id");
                        map.put("id", id);
                        map.put("label", id);
                        list.add(map);
                    }
                }
            }
            if (list.isEmpty())
                result = Failure.of(new Status(OBJECT_NOT_FOUND, "role", hostId));
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
    public Result<String> queryRolePermission(int offset, int limit, String hostId, String roleId, String apiId, String apiVersion, String endpoint) {
        Result<String> result;
        StringBuilder sqlBuilder = new StringBuilder();
        sqlBuilder.append("SELECT COUNT(*) OVER () AS total, \n" +
                "r.host_id, r.role_id, p.api_id, p.api_version, p.endpoint\n" +
                "FROM role_t r, role_permission_t p\n" +
                "WHERE r.role_id = p.role_id\n" +
                "AND r.host_id = ?\n");


        List<Object> parameters = new ArrayList<>();
        parameters.add(hostId);


        StringBuilder whereClause = new StringBuilder();

        addCondition(whereClause, parameters, "r.role_id", roleId);
        addCondition(whereClause, parameters, "p.api_id", apiId);
        addCondition(whereClause, parameters, "p.api_version", apiVersion);
        addCondition(whereClause, parameters, "p.endpoint", endpoint);

        if (whereClause.length() > 0) {
            sqlBuilder.append("AND ").append(whereClause);
        }

        sqlBuilder.append(" ORDER BY r.role_id, p.api_id, p.api_version, p.endpoint\n" +
                "LIMIT ? OFFSET ?");

        parameters.add(limit);
        parameters.add(offset);

        String sql = sqlBuilder.toString();
        if(logger.isTraceEnabled()) logger.trace("queryRolePermission sql: {}", sql);
        int total = 0;
        List<Map<String, Object>> rolePermissions = new ArrayList<>();


        try (final Connection conn = ds.getConnection(); PreparedStatement preparedStatement = conn.prepareStatement(sql)) {
            for (int i = 0; i < parameters.size(); i++) {
                preparedStatement.setObject(i + 1, parameters.get(i));
            }
            boolean isFirstRow = true;
            try (ResultSet resultSet = preparedStatement.executeQuery()) {
                while (resultSet.next()) {
                    Map<String, Object> map = new HashMap<>();
                    // only get the total once as it is the same for all rows.
                    if (isFirstRow) {
                        total = resultSet.getInt("total");
                        isFirstRow = false;
                    }
                    map.put("hostId", resultSet.getString("host_id"));
                    map.put("roleId", resultSet.getString("role_id"));
                    map.put("apiId", resultSet.getString("api_id"));
                    map.put("apiVersion", resultSet.getString("api_version"));
                    map.put("endpoint", resultSet.getString("endpoint"));
                    rolePermissions.add(map);
                }
            }
            Map<String, Object> resultMap = new HashMap<>();
            resultMap.put("total", total);
            resultMap.put("rolePermissions", rolePermissions);
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
    public Result<String> queryRoleUser(int offset, int limit, String hostId, String roleId, String userId, String entityId, String email, String firstName, String lastName, String userType) {
        Result<String> result;
        StringBuilder sqlBuilder = new StringBuilder();
        sqlBuilder.append("SELECT COUNT(*) OVER () AS total, \n" +
                "r.host_id, r.role_id, r.start_ts, r.end_ts, \n" +
                "u.user_id, u.email, u.user_type, \n" +
                "CASE\n" +
                "    WHEN u.user_type = 'C' THEN c.customer_id\n" +
                "    WHEN u.user_type = 'E' THEN e.employee_id\n" +
                "    ELSE NULL -- Handle other cases if needed\n" +
                "END AS entity_id,\n" +
                "e.manager_id, u.first_name, u.last_name\n" +
                "FROM user_t u\n" +
                "LEFT JOIN\n" +
                "    customer_t c ON u.user_id = c.user_id AND u.user_type = 'C'\n" +
                "LEFT JOIN\n" +
                "    employee_t e ON u.user_id = e.user_id AND u.user_type = 'E'\n" +
                "INNER JOIN\n" +
                "    role_user_t r ON r.user_id = u.user_id\n" +
                "AND r.host_id = ?\n");


        List<Object> parameters = new ArrayList<>();
        parameters.add(hostId);


        StringBuilder whereClause = new StringBuilder();

        addCondition(whereClause, parameters, "r.role_id", roleId);
        addCondition(whereClause, parameters, "u.user_id", userId);
        addCondition(whereClause, parameters, "entity_id", entityId);
        addCondition(whereClause, parameters, "u.email", email);
        addCondition(whereClause, parameters, "u.first_name", firstName);
        addCondition(whereClause, parameters, "u.last_name", lastName);
        addCondition(whereClause, parameters, "u.user_type", userType);

        if (whereClause.length() > 0) {
            sqlBuilder.append("AND ").append(whereClause);
        }

        sqlBuilder.append(" ORDER BY r.role_id, u.user_id\n" +
                "LIMIT ? OFFSET ?");

        parameters.add(limit);
        parameters.add(offset);

        String sql = sqlBuilder.toString();
        if(logger.isTraceEnabled()) logger.trace("queryRoleUser sql: {}", sql);
        int total = 0;
        List<Map<String, Object>> roleUsers = new ArrayList<>();


        try (final Connection conn = ds.getConnection(); PreparedStatement preparedStatement = conn.prepareStatement(sql)) {
            for (int i = 0; i < parameters.size(); i++) {
                preparedStatement.setObject(i + 1, parameters.get(i));
            }
            boolean isFirstRow = true;
            try (ResultSet resultSet = preparedStatement.executeQuery()) {
                while (resultSet.next()) {
                    Map<String, Object> map = new HashMap<>();
                    // only get the total once as it is the same for all rows.
                    if (isFirstRow) {
                        total = resultSet.getInt("total");
                        isFirstRow = false;
                    }
                    map.put("hostId", resultSet.getString("host_id"));
                    map.put("roleId", resultSet.getString("role_id"));
                    map.put("startTs", resultSet.getTimestamp("start_ts"));
                    map.put("endTs", resultSet.getTimestamp("end_ts"));
                    map.put("userId", resultSet.getString("user_id"));
                    map.put("entityId", resultSet.getString("entity_id"));
                    map.put("email", resultSet.getString("email"));
                    map.put("firstName", resultSet.getString("first_name"));
                    map.put("lastName", resultSet.getString("last_name"));
                    map.put("userType", resultSet.getString("user_type"));
                    roleUsers.add(map);
                }
            }
            Map<String, Object> resultMap = new HashMap<>();
            resultMap.put("total", total);
            resultMap.put("roleUsers", roleUsers);
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
    public Result<String> createRolePermission(RolePermissionCreatedEvent event) {
        final String insertRole = "INSERT INTO role_permission_t (host_id, role_id, api_id, api_version, endpoint, update_user, update_ts) " +
                "VALUES (?, ?, ?, ?, ?,  ?, ?)";

        Result<String> result = null;
        try (Connection conn = ds.getConnection()) {
            conn.setAutoCommit(false);
            // no duplicate record, insert the user into database and write a success notification.
            try (PreparedStatement statement = conn.prepareStatement(insertRole)) {
                statement.setString(1, event.getHostId());
                statement.setString(2, event.getRoleId());
                statement.setString(3, event.getApiId());
                statement.setString(4, event.getApiVersion());
                statement.setString(5, event.getEndpoint());
                statement.setString(6, event.getEventId().getId());
                statement.setTimestamp(7, new Timestamp(event.getEventId().getTimestamp()));

                int count = statement.executeUpdate();
                if (count == 0) {
                    throw new SQLException("failed to insert role permission " + event.getRoleId());
                }
                conn.commit();
                result = Success.of(event.getRoleId());
                insertNotification(event.getEventId(), event.getClass().getName(), AvroConverter.toJson(event, false), true, null);
            } catch (SQLException e) {
                logger.error("SQLException:", e);
                conn.rollback();
                insertNotification(event.getEventId(), event.getClass().getName(), AvroConverter.toJson(event, false), false, e.getMessage());
                result = Failure.of(new Status(SQL_EXCEPTION, e.getMessage()));
            } catch (Exception e) {
                logger.error("Exception:", e);
                conn.rollback();
                insertNotification(event.getEventId(), event.getClass().getName(), AvroConverter.toJson(event, false), false, e.getMessage());
                result = Failure.of(new Status(GENERIC_EXCEPTION, e.getMessage()));
            }
        } catch (SQLException e) {
            logger.error("SQLException:", e);
            result = Failure.of(new Status(SQL_EXCEPTION, e.getMessage()));
        }
        return result;

    }

    @Override
    public Result<String> deleteRolePermission(RolePermissionDeletedEvent event) {
        final String deleteRole = "DELETE from role_permission_t WHERE host_id = ? AND role_id = ? AND api_id = ? AND api_version = ? AND endpoint = ?";
        Result<String> result;
        try (Connection conn = ds.getConnection()) {
            conn.setAutoCommit(false);
            try (PreparedStatement statement = conn.prepareStatement(deleteRole)) {
                statement.setString(1, event.getHostId());
                statement.setString(2, event.getRoleId());
                statement.setString(3, event.getApiId());
                statement.setString(4, event.getApiVersion());
                statement.setString(5, event.getEndpoint());

                int count = statement.executeUpdate();
                if (count == 0) {
                    throw new SQLException("no record is deleted for role " + event.getRoleId());
                }
                conn.commit();
                result = Success.of(event.getEventId().getId());
                insertNotification(event.getEventId(), event.getClass().getName(), AvroConverter.toJson(event, false), true, null);
            } catch (SQLException e) {
                logger.error("SQLException:", e);
                conn.rollback();
                insertNotification(event.getEventId(), event.getClass().getName(), AvroConverter.toJson(event, false), false, e.getMessage());
                result = Failure.of(new Status(SQL_EXCEPTION, e.getMessage()));
            } catch (Exception e) {
                logger.error("Exception:", e);
                conn.rollback();
                insertNotification(event.getEventId(), event.getClass().getName(), AvroConverter.toJson(event, false), false, e.getMessage());
                result = Failure.of(new Status(GENERIC_EXCEPTION, e.getMessage()));
            }
        } catch (SQLException e) {
            logger.error("SQLException:", e);
            result = Failure.of(new Status(SQL_EXCEPTION, e.getMessage()));
        }
        return result;
    }

    @Override
    public Result<String> createRoleUser(RoleUserCreatedEvent event) {
        final String insertRole = "INSERT INTO role_user_t (host_id, role_id, user_id, start_ts, end_ts, update_user, update_ts) " +
                "VALUES (?, ?, ?, ?, ?,  ?, ?)";

        Result<String> result = null;
        try (Connection conn = ds.getConnection()) {
            conn.setAutoCommit(false);
            // no duplicate record, insert the user into database and write a success notification.
            try (PreparedStatement statement = conn.prepareStatement(insertRole)) {
                statement.setString(1, event.getHostId());
                statement.setString(2, event.getRoleId());
                statement.setString(3, event.getUserId());

                if(event.getStartTs() != null)
                    statement.setObject(4, event.getStartTs());
                else
                    statement.setNull(4, NULL);

                if (event.getEndTs() != null) {
                    statement.setObject(5, event.getEndTs());
                } else {
                    statement.setNull(5, NULL);
                }
                statement.setString(6, event.getEventId().getId());
                statement.setTimestamp(7, new Timestamp(event.getEventId().getTimestamp()));

                int count = statement.executeUpdate();
                if (count == 0) {
                    throw new SQLException("failed to insert role user " + event.getRoleId());
                }
                conn.commit();
                result = Success.of(event.getRoleId());
                insertNotification(event.getEventId(), event.getClass().getName(), AvroConverter.toJson(event, false), true, null);
            } catch (SQLException e) {
                logger.error("SQLException:", e);
                conn.rollback();
                insertNotification(event.getEventId(), event.getClass().getName(), AvroConverter.toJson(event, false), false, e.getMessage());
                result = Failure.of(new Status(SQL_EXCEPTION, e.getMessage()));
            } catch (Exception e) {
                logger.error("Exception:", e);
                conn.rollback();
                insertNotification(event.getEventId(), event.getClass().getName(), AvroConverter.toJson(event, false), false, e.getMessage());
                result = Failure.of(new Status(GENERIC_EXCEPTION, e.getMessage()));
            }
        } catch (SQLException e) {
            logger.error("SQLException:", e);
            result = Failure.of(new Status(SQL_EXCEPTION, e.getMessage()));
        }
        return result;

    }

    @Override
    public Result<String> updateRoleUser(RoleUserUpdatedEvent event) {
        final String updateRole = "UPDATE role_user_t SET start_ts = ?, end_ts = ?, update_user = ?, update_ts = ? " +
                "WHERE host_id = ? AND role_id = ? AND user_id = ?";

        Result<String> result = null;
        try (Connection conn = ds.getConnection()) {
            conn.setAutoCommit(false);

            try (PreparedStatement statement = conn.prepareStatement(updateRole)) {
                if(event.getStartTs() != null)
                    statement.setObject(1, event.getStartTs());
                else
                    statement.setNull(1, NULL);

                if (event.getEndTs() != null) {
                    statement.setObject(2, event.getEndTs());
                } else {
                    statement.setNull(2, NULL);
                }
                statement.setString(3, event.getEventId().getId());
                statement.setTimestamp(4, new Timestamp(event.getEventId().getTimestamp()));
                statement.setString(5, event.getHostId());
                statement.setString(6, event.getRoleId());
                statement.setString(7, event.getUserId());

                int count = statement.executeUpdate();
                if (count == 0) {
                    throw new SQLException("no record is updated for role user " + event.getRoleId());
                }
                conn.commit();
                result = Success.of(event.getRoleId());
                insertNotification(event.getEventId(), event.getClass().getName(), AvroConverter.toJson(event, false), true, null);
            } catch (SQLException e) {
                logger.error("SQLException:", e);
                conn.rollback();
                insertNotification(event.getEventId(), event.getClass().getName(), AvroConverter.toJson(event, false), false, e.getMessage());
                result = Failure.of(new Status(SQL_EXCEPTION, e.getMessage()));
            } catch (Exception e) {
                logger.error("Exception:", e);
                conn.rollback();
                insertNotification(event.getEventId(), event.getClass().getName(), AvroConverter.toJson(event, false), false, e.getMessage());
                result = Failure.of(new Status(GENERIC_EXCEPTION, e.getMessage()));
            }
        } catch (SQLException e) {
            logger.error("SQLException:", e);
            result = Failure.of(new Status(SQL_EXCEPTION, e.getMessage()));
        }
        return result;

    }

    @Override
    public Result<String> deleteRoleUser(RoleUserDeletedEvent event) {
        final String deleteRole = "DELETE from role_user_t WHERE host_id = ? AND role_id = ? AND user_id = ?";
        Result<String> result;
        try (Connection conn = ds.getConnection()) {
            conn.setAutoCommit(false);
            try (PreparedStatement statement = conn.prepareStatement(deleteRole)) {
                statement.setString(1, event.getHostId());
                statement.setString(2, event.getRoleId());
                statement.setString(3, event.getUserId());

                int count = statement.executeUpdate();
                if (count == 0) {
                    throw new SQLException("no record is deleted for role user " + event.getRoleId());
                }
                conn.commit();
                result = Success.of(event.getEventId().getId());
                insertNotification(event.getEventId(), event.getClass().getName(), AvroConverter.toJson(event, false), true, null);
            } catch (SQLException e) {
                logger.error("SQLException:", e);
                conn.rollback();
                insertNotification(event.getEventId(), event.getClass().getName(), AvroConverter.toJson(event, false), false, e.getMessage());
                result = Failure.of(new Status(SQL_EXCEPTION, e.getMessage()));
            } catch (Exception e) {
                logger.error("Exception:", e);
                conn.rollback();
                insertNotification(event.getEventId(), event.getClass().getName(), AvroConverter.toJson(event, false), false, e.getMessage());
                result = Failure.of(new Status(GENERIC_EXCEPTION, e.getMessage()));
            }
        } catch (SQLException e) {
            logger.error("SQLException:", e);
            result = Failure.of(new Status(SQL_EXCEPTION, e.getMessage()));
        }
        return result;

    }

    @Override
    public Result<String> queryRoleRowFilter(int offset, int limit, String hostId, String roleId, String apiId, String apiVersion, String endpoint) {
        Result<String> result;
        StringBuilder sqlBuilder = new StringBuilder();
        sqlBuilder.append("SELECT COUNT(*) OVER () AS total, \n" +
                "r.host_id, r.role_id, p.api_id, p.api_version, p.endpoint, p.col_name, p.operator, p.col_value\n" +
                "FROM role_t r, role_row_filter_t p\n" +
                "WHERE r.role_id = p.role_id\n" +
                "AND r.host_id = ?\n");

        List<Object> parameters = new ArrayList<>();
        parameters.add(hostId);

        StringBuilder whereClause = new StringBuilder();

        addCondition(whereClause, parameters, "r.role_id", roleId);
        addCondition(whereClause, parameters, "p.api_id", apiId);
        addCondition(whereClause, parameters, "p.api_version", apiVersion);
        addCondition(whereClause, parameters, "p.endpoint", endpoint);

        if (whereClause.length() > 0) {
            sqlBuilder.append("AND ").append(whereClause);
        }

        sqlBuilder.append(" ORDER BY r.role_id, p.api_id, p.api_version, p.endpoint, p.col_name\n" +
                "LIMIT ? OFFSET ?");

        parameters.add(limit);
        parameters.add(offset);

        String sql = sqlBuilder.toString();
        if(logger.isTraceEnabled()) logger.trace("queryRoleRowFilter sql: {}", sql);
        int total = 0;
        List<Map<String, Object>> roleRowFilters = new ArrayList<>();

        try (final Connection conn = ds.getConnection(); PreparedStatement preparedStatement = conn.prepareStatement(sql)) {
            for (int i = 0; i < parameters.size(); i++) {
                preparedStatement.setObject(i + 1, parameters.get(i));
            }
            boolean isFirstRow = true;
            try (ResultSet resultSet = preparedStatement.executeQuery()) {
                while (resultSet.next()) {
                    Map<String, Object> map = new HashMap<>();
                    // only get the total once as it is the same for all rows.
                    if (isFirstRow) {
                        total = resultSet.getInt("total");
                        isFirstRow = false;
                    }
                    map.put("hostId", resultSet.getString("host_id"));
                    map.put("roleId", resultSet.getString("role_id"));
                    map.put("apiId", resultSet.getString("api_id"));
                    map.put("apiVersion", resultSet.getString("api_version"));
                    map.put("endpoint", resultSet.getString("endpoint"));
                    map.put("colName", resultSet.getString("col_name"));
                    map.put("operator", resultSet.getString("operator"));
                    map.put("colValue", resultSet.getString("col_value"));
                    roleRowFilters.add(map);
                }
            }
            Map<String, Object> resultMap = new HashMap<>();
            resultMap.put("total", total);
            resultMap.put("roleRowFilters", roleRowFilters);
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
    public Result<String> deleteRoleRowFilter(RoleRowFilterDeletedEvent event) {
        final String deleteRole = "DELETE from role_row_filter_t WHERE host_id = ? AND role_id = ? AND api_id = ? AND api_version = ? AND endpoint = ? AND col_name = ?";
        Result<String> result;
        try (Connection conn = ds.getConnection()) {
            conn.setAutoCommit(false);
            try (PreparedStatement statement = conn.prepareStatement(deleteRole)) {
                statement.setString(1, event.getHostId());
                statement.setString(2, event.getRoleId());
                statement.setString(3, event.getApiId());
                statement.setString(4, event.getApiVersion());
                statement.setString(5, event.getEndpoint());
                statement.setString(6, event.getColName());

                int count = statement.executeUpdate();
                if (count == 0) {
                    throw new SQLException("no record is deleted for role row filter " + event.getRoleId());
                }
                conn.commit();
                result = Success.of(event.getEventId().getId());
                insertNotification(event.getEventId(), event.getClass().getName(), AvroConverter.toJson(event, false), true, null);
            } catch (SQLException e) {
                logger.error("SQLException:", e);
                conn.rollback();
                insertNotification(event.getEventId(), event.getClass().getName(), AvroConverter.toJson(event, false), false, e.getMessage());
                result = Failure.of(new Status(SQL_EXCEPTION, e.getMessage()));
            } catch (Exception e) {
                logger.error("Exception:", e);
                conn.rollback();
                insertNotification(event.getEventId(), event.getClass().getName(), AvroConverter.toJson(event, false), false, e.getMessage());
                result = Failure.of(new Status(GENERIC_EXCEPTION, e.getMessage()));
            }
        } catch (SQLException e) {
            logger.error("SQLException:", e);
            result = Failure.of(new Status(SQL_EXCEPTION, e.getMessage()));
        }
        return result;
    }

    @Override
    public Result<String> createRoleRowFilter(RoleRowFilterCreatedEvent event) {
        final String insertRole = "INSERT INTO role_row_filter_t (host_id, role_id, api_id, api_version, endpoint, col_name, operator, col_value, update_user, update_ts) " +
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";

        Result<String> result = null;
        try (Connection conn = ds.getConnection()) {
            conn.setAutoCommit(false);
            // no duplicate record, insert the user into database and write a success notification.
            try (PreparedStatement statement = conn.prepareStatement(insertRole)) {
                statement.setString(1, event.getHostId());
                statement.setString(2, event.getRoleId());
                statement.setString(3, event.getApiId());
                statement.setString(4, event.getApiVersion());
                statement.setString(5, event.getEndpoint());
                statement.setString(6, event.getColName());
                statement.setString(7, event.getOperator());
                statement.setString(8, event.getColValue());
                statement.setString(9, event.getEventId().getId());
                statement.setTimestamp(10, new Timestamp(event.getEventId().getTimestamp()));

                int count = statement.executeUpdate();
                if (count == 0) {
                    throw new SQLException("failed to insert role row filter " + event.getRoleId());
                }
                conn.commit();
                result = Success.of(event.getRoleId());
                insertNotification(event.getEventId(), event.getClass().getName(), AvroConverter.toJson(event, false), true, null);
            } catch (SQLException e) {
                logger.error("SQLException:", e);
                conn.rollback();
                insertNotification(event.getEventId(), event.getClass().getName(), AvroConverter.toJson(event, false), false, e.getMessage());
                result = Failure.of(new Status(SQL_EXCEPTION, e.getMessage()));
            } catch (Exception e) {
                logger.error("Exception:", e);
                conn.rollback();
                insertNotification(event.getEventId(), event.getClass().getName(), AvroConverter.toJson(event, false), false, e.getMessage());
                result = Failure.of(new Status(GENERIC_EXCEPTION, e.getMessage()));
            }
        } catch (SQLException e) {
            logger.error("SQLException:", e);
            result = Failure.of(new Status(SQL_EXCEPTION, e.getMessage()));
        }
        return result;
    }

    @Override
    public Result<String> updateRoleRowFilter(RoleRowFilterUpdatedEvent event) {
        final String updateRole = "UPDATE role_row_filter_t SET operator = ?, col_value = ?, update_user = ?, update_ts = ? " +
                "WHERE host_id = ? AND role_id = ? AND api_id = ? AND api_version = ? AND endpoint = ? AND col_name = ?";

        Result<String> result = null;
        try (Connection conn = ds.getConnection()) {
            conn.setAutoCommit(false);

            try (PreparedStatement statement = conn.prepareStatement(updateRole)) {
                statement.setString(1, event.getOperator());
                statement.setString(2, event.getColValue());
                statement.setString(3, event.getEventId().getId());
                statement.setTimestamp(4, new Timestamp(event.getEventId().getTimestamp()));
                statement.setString(5, event.getHostId());
                statement.setString(6, event.getRoleId());
                statement.setString(7, event.getApiId());
                statement.setString(8, event.getApiVersion());
                statement.setString(9, event.getEndpoint());
                statement.setString(10, event.getColName());

                int count = statement.executeUpdate();
                if (count == 0) {
                    throw new SQLException("no record is updated for role row filter " + event.getRoleId());
                }
                conn.commit();
                result = Success.of(event.getRoleId());
                insertNotification(event.getEventId(), event.getClass().getName(), AvroConverter.toJson(event, false), true, null);
            } catch (SQLException e) {
                logger.error("SQLException:", e);
                conn.rollback();
                insertNotification(event.getEventId(), event.getClass().getName(), AvroConverter.toJson(event, false), false, e.getMessage());
                result = Failure.of(new Status(SQL_EXCEPTION, e.getMessage()));
            } catch (Exception e) {
                logger.error("Exception:", e);
                conn.rollback();
                insertNotification(event.getEventId(), event.getClass().getName(), AvroConverter.toJson(event, false), false, e.getMessage());
                result = Failure.of(new Status(GENERIC_EXCEPTION, e.getMessage()));
            }
        } catch (SQLException e) {
            logger.error("SQLException:", e);
            result = Failure.of(new Status(SQL_EXCEPTION, e.getMessage()));
        }
        return result;
    }

    @Override
    public Result<String> queryRoleColFilter(int offset, int limit, String hostId, String roleId, String apiId, String apiVersion, String endpoint) {
        Result<String> result;
        StringBuilder sqlBuilder = new StringBuilder();
        sqlBuilder.append("SELECT COUNT(*) OVER () AS total, \n" +
                "r.host_id, r.role_id, p.api_id, p.api_version, p.endpoint, p.columns\n" +
                "FROM role_t r, role_col_filter_t p\n" +
                "WHERE r.role_id = p.role_id\n" +
                "AND r.host_id = ?\n");

        List<Object> parameters = new ArrayList<>();
        parameters.add(hostId);

        StringBuilder whereClause = new StringBuilder();

        addCondition(whereClause, parameters, "r.role_id", roleId);
        addCondition(whereClause, parameters, "p.api_id", apiId);
        addCondition(whereClause, parameters, "p.api_version", apiVersion);
        addCondition(whereClause, parameters, "p.endpoint", endpoint);

        if (whereClause.length() > 0) {
            sqlBuilder.append("AND ").append(whereClause);
        }

        sqlBuilder.append(" ORDER BY r.role_id, p.api_id, p.api_version, p.endpoint\n" +
                "LIMIT ? OFFSET ?");

        parameters.add(limit);
        parameters.add(offset);

        String sql = sqlBuilder.toString();
        if(logger.isTraceEnabled()) logger.trace("queryRoleColFilter sql: {}", sql);
        int total = 0;
        List<Map<String, Object>> roleColFilters = new ArrayList<>();

        try (final Connection conn = ds.getConnection(); PreparedStatement preparedStatement = conn.prepareStatement(sql)) {
            for (int i = 0; i < parameters.size(); i++) {
                preparedStatement.setObject(i + 1, parameters.get(i));
            }
            boolean isFirstRow = true;
            try (ResultSet resultSet = preparedStatement.executeQuery()) {
                while (resultSet.next()) {
                    Map<String, Object> map = new HashMap<>();
                    // only get the total once as it is the same for all rows.
                    if (isFirstRow) {
                        total = resultSet.getInt("total");
                        isFirstRow = false;
                    }
                    map.put("hostId", resultSet.getString("host_id"));
                    map.put("roleId", resultSet.getString("role_id"));
                    map.put("apiId", resultSet.getString("api_id"));
                    map.put("apiVersion", resultSet.getString("api_version"));
                    map.put("endpoint", resultSet.getString("endpoint"));
                    map.put("columns", resultSet.getString("columns"));
                    roleColFilters.add(map);
                }
            }
            Map<String, Object> resultMap = new HashMap<>();
            resultMap.put("total", total);
            resultMap.put("roleColFilters", roleColFilters);
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
    public Result<String> createRoleColFilter(RoleColFilterCreatedEvent event) {
        final String insertRole = "INSERT INTO role_col_filter_t (host_id, role_id, api_id, api_version, endpoint, columns, update_user, update_ts) " +
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?)";

        Result<String> result = null;
        try (Connection conn = ds.getConnection()) {
            conn.setAutoCommit(false);
            // no duplicate record, insert the user into database and write a success notification.
            try (PreparedStatement statement = conn.prepareStatement(insertRole)) {
                statement.setString(1, event.getHostId());
                statement.setString(2, event.getRoleId());
                statement.setString(3, event.getApiId());
                statement.setString(4, event.getApiVersion());
                statement.setString(5, event.getEndpoint());
                statement.setString(6, event.getColumns());
                statement.setString(7, event.getEventId().getId());
                statement.setTimestamp(8, new Timestamp(event.getEventId().getTimestamp()));

                int count = statement.executeUpdate();
                if (count == 0) {
                    throw new SQLException("failed to insert role col filter " + event.getRoleId());
                }
                conn.commit();
                result = Success.of(event.getRoleId());
                insertNotification(event.getEventId(), event.getClass().getName(), AvroConverter.toJson(event, false), true, null);
            } catch (SQLException e) {
                logger.error("SQLException:", e);
                conn.rollback();
                insertNotification(event.getEventId(), event.getClass().getName(), AvroConverter.toJson(event, false), false, e.getMessage());
                result = Failure.of(new Status(SQL_EXCEPTION, e.getMessage()));
            } catch (Exception e) {
                logger.error("Exception:", e);
                conn.rollback();
                insertNotification(event.getEventId(), event.getClass().getName(), AvroConverter.toJson(event, false), false, e.getMessage());
                result = Failure.of(new Status(GENERIC_EXCEPTION, e.getMessage()));
            }
        } catch (SQLException e) {
            logger.error("SQLException:", e);
            result = Failure.of(new Status(SQL_EXCEPTION, e.getMessage()));
        }
        return result;
    }

    @Override
    public Result<String> deleteRoleColFilter(RoleColFilterDeletedEvent event) {
        final String deleteRole = "DELETE from role_col_filter_t WHERE host_id = ? AND role_id = ? AND api_id = ? AND api_version = ? AND endpoint = ?";
        Result<String> result;
        try (Connection conn = ds.getConnection()) {
            conn.setAutoCommit(false);
            try (PreparedStatement statement = conn.prepareStatement(deleteRole)) {
                statement.setString(1, event.getHostId());
                statement.setString(2, event.getRoleId());
                statement.setString(3, event.getApiId());
                statement.setString(4, event.getApiVersion());
                statement.setString(5, event.getEndpoint());

                int count = statement.executeUpdate();
                if (count == 0) {
                    throw new SQLException("no record is deleted for role col filter " + event.getRoleId());
                }
                conn.commit();
                result = Success.of(event.getEventId().getId());
                insertNotification(event.getEventId(), event.getClass().getName(), AvroConverter.toJson(event, false), true, null);
            } catch (SQLException e) {
                logger.error("SQLException:", e);
                conn.rollback();
                insertNotification(event.getEventId(), event.getClass().getName(), AvroConverter.toJson(event, false), false, e.getMessage());
                result = Failure.of(new Status(SQL_EXCEPTION, e.getMessage()));
            } catch (Exception e) {
                logger.error("Exception:", e);
                conn.rollback();
                insertNotification(event.getEventId(), event.getClass().getName(), AvroConverter.toJson(event, false), false, e.getMessage());
                result = Failure.of(new Status(GENERIC_EXCEPTION, e.getMessage()));
            }
        } catch (SQLException e) {
            logger.error("SQLException:", e);
            result = Failure.of(new Status(SQL_EXCEPTION, e.getMessage()));
        }
        return result;
    }

    @Override
    public Result<String> updateRoleColFilter(RoleColFilterUpdatedEvent event) {
        final String updateRole = "UPDATE role_col_filter_t SET columns = ?, update_user = ?, update_ts = ? " +
                "WHERE host_id = ? AND role_id = ? AND api_id = ? AND api_version = ? AND endpoint = ?";

        Result<String> result = null;
        try (Connection conn = ds.getConnection()) {
            conn.setAutoCommit(false);

            try (PreparedStatement statement = conn.prepareStatement(updateRole)) {
                statement.setString(1, event.getColumns());
                statement.setString(2, event.getEventId().getId());
                statement.setTimestamp(3, new Timestamp(event.getEventId().getTimestamp()));
                statement.setString(4, event.getHostId());
                statement.setString(5, event.getRoleId());
                statement.setString(6, event.getApiId());
                statement.setString(7, event.getApiVersion());
                statement.setString(8, event.getEndpoint());

                int count = statement.executeUpdate();
                if (count == 0) {
                    throw new SQLException("no record is updated for role col filter " + event.getRoleId());
                }
                conn.commit();
                result = Success.of(event.getRoleId());
                insertNotification(event.getEventId(), event.getClass().getName(), AvroConverter.toJson(event, false), true, null);
            } catch (SQLException e) {
                logger.error("SQLException:", e);
                conn.rollback();
                insertNotification(event.getEventId(), event.getClass().getName(), AvroConverter.toJson(event, false), false, e.getMessage());
                result = Failure.of(new Status(SQL_EXCEPTION, e.getMessage()));
            } catch (Exception e) {
                logger.error("Exception:", e);
                conn.rollback();
                insertNotification(event.getEventId(), event.getClass().getName(), AvroConverter.toJson(event, false), false, e.getMessage());
                result = Failure.of(new Status(GENERIC_EXCEPTION, e.getMessage()));
            }
        } catch (SQLException e) {
            logger.error("SQLException:", e);
            result = Failure.of(new Status(SQL_EXCEPTION, e.getMessage()));
        }
        return result;
    }

    @Override
    public Result<String> createGroup(GroupCreatedEvent event) {
        final String insertGroup = "INSERT INTO group_t (host_id, group_id, group_desc, update_user, update_ts) " +
                "VALUES (?, ?, ?, ?, ?)";

        Result<String> result = null;
        try (Connection conn = ds.getConnection()) {
            conn.setAutoCommit(false);
            try (PreparedStatement statement = conn.prepareStatement(insertGroup)) {
                statement.setString(1, event.getHostId());
                statement.setString(2, event.getGroupId());
                if (event.getGroupDesc() != null)
                    statement.setString(3, event.getGroupDesc());
                else
                    statement.setNull(3, NULL);

                statement.setString(4, event.getEventId().getId());
                statement.setTimestamp(5, new Timestamp(event.getEventId().getTimestamp()));

                int count = statement.executeUpdate();
                if (count == 0) {
                    throw new SQLException("failed to insert group " + event.getGroupId());
                }
                conn.commit();
                result = Success.of(event.getGroupId());
                insertNotification(event.getEventId(), event.getClass().getName(), AvroConverter.toJson(event, false), true, null);
            } catch (SQLException e) {
                logger.error("SQLException:", e);
                conn.rollback();
                insertNotification(event.getEventId(), event.getClass().getName(), AvroConverter.toJson(event, false), false, e.getMessage());
                result = Failure.of(new Status(SQL_EXCEPTION, e.getMessage()));
            } catch (Exception e) {
                logger.error("Exception:", e);
                conn.rollback();
                insertNotification(event.getEventId(), event.getClass().getName(), AvroConverter.toJson(event, false), false, e.getMessage());
                result = Failure.of(new Status(GENERIC_EXCEPTION, e.getMessage()));
            }
        } catch (SQLException e) {
            logger.error("SQLException:", e);
            result = Failure.of(new Status(SQL_EXCEPTION, e.getMessage()));
        }
        return result;
    }

    @Override
    public Result<String> updateGroup(GroupUpdatedEvent event) {
        final String updateGroup = "UPDATE group_t SET group_desc = ?, update_user = ?, update_ts = ? " +
                "WHERE host_id = ? AND group_id = ?";

        Result<String> result = null;
        try (Connection conn = ds.getConnection()) {
            conn.setAutoCommit(false);
            try (PreparedStatement statement = conn.prepareStatement(updateGroup)) {
                if(event.getGroupDesc() != null) {
                    statement.setString(1, event.getGroupDesc());
                } else {
                    statement.setNull(1, NULL);
                }
                statement.setString(2, event.getEventId().getId());
                statement.setTimestamp(3, new Timestamp(event.getEventId().getTimestamp()));
                statement.setString(4, event.getHostId());
                statement.setString(5, event.getGroupId());

                int count = statement.executeUpdate();
                if (count == 0) {
                    throw new SQLException("no record is updated for group " + event.getGroupId());
                }
                conn.commit();
                result = Success.of(event.getGroupId());
                insertNotification(event.getEventId(), event.getClass().getName(), AvroConverter.toJson(event, false), true, null);
            } catch (SQLException e) {
                logger.error("SQLException:", e);
                conn.rollback();
                insertNotification(event.getEventId(), event.getClass().getName(), AvroConverter.toJson(event, false), false, e.getMessage());
                result = Failure.of(new Status(SQL_EXCEPTION, e.getMessage()));
            } catch (Exception e) {
                logger.error("Exception:", e);
                conn.rollback();
                insertNotification(event.getEventId(), event.getClass().getName(), AvroConverter.toJson(event, false), false, e.getMessage());
                result = Failure.of(new Status(GENERIC_EXCEPTION, e.getMessage()));
            }
        } catch (SQLException e) {
            logger.error("SQLException:", e);
            result = Failure.of(new Status(SQL_EXCEPTION, e.getMessage()));
        }
        return result;

    }

    @Override
    public Result<String> deleteGroup(GroupDeletedEvent event) {
        final String deleteGroup = "DELETE from group_t WHERE host_id = ? AND group_id = ?";
        Result<String> result;
        try (Connection conn = ds.getConnection()) {
            conn.setAutoCommit(false);
            try (PreparedStatement statement = conn.prepareStatement(deleteGroup)) {
                statement.setString(1, event.getHostId());
                statement.setString(2, event.getGroupId());
                int count = statement.executeUpdate();
                if (count == 0) {
                    throw new SQLException("no record is deleted for group " + event.getGroupId());
                }
                conn.commit();
                result = Success.of(event.getEventId().getId());
                insertNotification(event.getEventId(), event.getClass().getName(), AvroConverter.toJson(event, false), true, null);
            } catch (SQLException e) {
                logger.error("SQLException:", e);
                conn.rollback();
                insertNotification(event.getEventId(), event.getClass().getName(), AvroConverter.toJson(event, false), false, e.getMessage());
                result = Failure.of(new Status(SQL_EXCEPTION, e.getMessage()));
            } catch (Exception e) {
                logger.error("Exception:", e);
                conn.rollback();
                insertNotification(event.getEventId(), event.getClass().getName(), AvroConverter.toJson(event, false), false, e.getMessage());
                result = Failure.of(new Status(GENERIC_EXCEPTION, e.getMessage()));
            }
        } catch (SQLException e) {
            logger.error("SQLException:", e);
            result = Failure.of(new Status(SQL_EXCEPTION, e.getMessage()));
        }
        return result;
    }

    @Override
    public Result<String> queryGroup(int offset, int limit, String hostId, String groupId, String groupDesc) {
        Result<String> result;
        StringBuilder sqlBuilder = new StringBuilder();
        sqlBuilder.append("SELECT COUNT(*) OVER () AS total, host_id, group_id, group_desc, update_user, update_ts " +
                "FROM group_t " +
                "WHERE host_id = ?\n");
        List<Object> parameters = new ArrayList<>();
        parameters.add(hostId);

        StringBuilder whereClause = new StringBuilder();
        addCondition(whereClause, parameters, "group_id", groupId);
        addCondition(whereClause, parameters, "group_desc", groupDesc);


        if (whereClause.length() > 0) {
            sqlBuilder.append("AND ").append(whereClause);
        }

        sqlBuilder.append(" ORDER BY group_id\n" +
                "LIMIT ? OFFSET ?");
        parameters.add(limit);
        parameters.add(offset);

        String sql = sqlBuilder.toString();
        int total = 0;
        List<Map<String, Object>> groups = new ArrayList<>();

        try (final Connection conn = ds.getConnection(); PreparedStatement preparedStatement = conn.prepareStatement(sql)) {
            for (int i = 0; i < parameters.size(); i++) {
                preparedStatement.setObject(i + 1, parameters.get(i));
            }
            boolean isFirstRow = true;
            try (ResultSet resultSet = preparedStatement.executeQuery()) {
                while (resultSet.next()) {
                    Map<String, Object> map = new HashMap<>();
                    // only get the total once as it is the same for all rows.
                    if (isFirstRow) {
                        total = resultSet.getInt("total");
                        isFirstRow = false;
                    }
                    map.put("hostId", resultSet.getString("host_id"));
                    map.put("groupId", resultSet.getString("group_id"));
                    map.put("groupDesc", resultSet.getString("group_desc"));
                    map.put("updateUser", resultSet.getString("update_user"));
                    map.put("updateTs", resultSet.getTimestamp("update_ts"));
                    groups.add(map);
                }
            }
            Map<String, Object> resultMap = new HashMap<>();
            resultMap.put("total", total);
            resultMap.put("groups", groups);
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
    public Result<String> queryGroupLabel(String hostId) {
        final String sql = "SELECT group_id from group_t WHERE host_id = ?";
        Result<String> result;
        try (final Connection conn = ds.getConnection()) {
            List<Map<String, Object>> list = new ArrayList<>();
            try (PreparedStatement statement = conn.prepareStatement(sql)) {
                statement.setString(1, hostId);
                try (ResultSet resultSet = statement.executeQuery()) {
                    while (resultSet.next()) {
                        Map<String, Object> map = new HashMap<>();
                        String id = resultSet.getString("group_id");
                        map.put("id", id);
                        map.put("label", id);
                        list.add(map);
                    }
                }
            }
            if (list.isEmpty())
                result = Failure.of(new Status(OBJECT_NOT_FOUND, "group", hostId));
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
    public Result<String> queryGroupPermission(int offset, int limit, String hostId, String groupId, String apiId, String apiVersion, String endpoint) {
        Result<String> result;
        StringBuilder sqlBuilder = new StringBuilder();
        sqlBuilder.append("SELECT COUNT(*) OVER () AS total, \n" +
                "g.host_id, g.group_id, p.api_id, p.api_version, p.endpoint\n" +
                "FROM group_t g, group_permission_t p\n" +
                "WHERE g.group_id = p.group_id\n" +
                "AND g.host_id = ?\n");


        List<Object> parameters = new ArrayList<>();
        parameters.add(hostId);


        StringBuilder whereClause = new StringBuilder();

        addCondition(whereClause, parameters, "g.group_id", groupId);
        addCondition(whereClause, parameters, "p.api_id", apiId);
        addCondition(whereClause, parameters, "p.api_version", apiVersion);
        addCondition(whereClause, parameters, "p.endpoint", endpoint);

        if (whereClause.length() > 0) {
            sqlBuilder.append("AND ").append(whereClause);
        }

        sqlBuilder.append(" ORDER BY g.group_id, p.api_id, p.api_version, p.endpoint\n" +
                "LIMIT ? OFFSET ?");

        parameters.add(limit);
        parameters.add(offset);

        String sql = sqlBuilder.toString();
        if(logger.isTraceEnabled()) logger.trace("queryGroupPermission sql: {}", sql);
        int total = 0;
        List<Map<String, Object>> groupPermissions = new ArrayList<>();


        try (final Connection conn = ds.getConnection(); PreparedStatement preparedStatement = conn.prepareStatement(sql)) {
            for (int i = 0; i < parameters.size(); i++) {
                preparedStatement.setObject(i + 1, parameters.get(i));
            }
            boolean isFirstRow = true;
            try (ResultSet resultSet = preparedStatement.executeQuery()) {
                while (resultSet.next()) {
                    Map<String, Object> map = new HashMap<>();
                    // only get the total once as it is the same for all rows.
                    if (isFirstRow) {
                        total = resultSet.getInt("total");
                        isFirstRow = false;
                    }
                    map.put("hostId", resultSet.getString("host_id"));
                    map.put("groupId", resultSet.getString("group_id"));
                    map.put("apiId", resultSet.getString("api_id"));
                    map.put("apiVersion", resultSet.getString("api_version"));
                    map.put("endpoint", resultSet.getString("endpoint"));
                    groupPermissions.add(map);
                }
            }
            Map<String, Object> resultMap = new HashMap<>();
            resultMap.put("total", total);
            resultMap.put("groupPermissions", groupPermissions);
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
    public Result<String> queryGroupUser(int offset, int limit, String hostId, String groupId, String userId, String entityId, String email, String firstName, String lastName, String userType) {
        Result<String> result;
        StringBuilder sqlBuilder = new StringBuilder();
        sqlBuilder.append("SELECT COUNT(*) OVER () AS total, \n" +
                "g.host_id, g.group_id, g.start_ts, g.end_ts, \n" +
                "u.user_id, u.email, u.user_type, \n" +
                "CASE\n" +
                "    WHEN u.user_type = 'C' THEN c.customer_id\n" +
                "    WHEN u.user_type = 'E' THEN e.employee_id\n" +
                "    ELSE NULL -- Handle other cases if needed\n" +
                "END AS entity_id,\n" +
                "e.manager_id, u.first_name, u.last_name\n" +
                "FROM user_t u\n" +
                "LEFT JOIN\n" +
                "    customer_t c ON u.user_id = c.user_id AND u.user_type = 'C'\n" +
                "LEFT JOIN\n" +
                "    employee_t e ON u.user_id = e.user_id AND u.user_type = 'E'\n" +
                "INNER JOIN\n" +
                "    group_user_t g ON g.user_id = u.user_id\n" +
                "AND g.host_id = ?\n");

        List<Object> parameters = new ArrayList<>();
        parameters.add(hostId);


        StringBuilder whereClause = new StringBuilder();

        addCondition(whereClause, parameters, "g.group_id", groupId);
        addCondition(whereClause, parameters, "u.user_id", userId);
        addCondition(whereClause, parameters, "entity_id", entityId);
        addCondition(whereClause, parameters, "u.email", email);
        addCondition(whereClause, parameters, "u.first_name", firstName);
        addCondition(whereClause, parameters, "u.last_name", lastName);
        addCondition(whereClause, parameters, "u.user_type", userType);

        if (whereClause.length() > 0) {
            sqlBuilder.append("AND ").append(whereClause);
        }

        sqlBuilder.append(" ORDER BY g.group_id, u.user_id\n" +
                "LIMIT ? OFFSET ?");

        parameters.add(limit);
        parameters.add(offset);

        String sql = sqlBuilder.toString();
        if(logger.isTraceEnabled()) logger.trace("queryGroupUser sql: {}", sql);
        int total = 0;
        List<Map<String, Object>> groupUsers = new ArrayList<>();


        try (final Connection conn = ds.getConnection(); PreparedStatement preparedStatement = conn.prepareStatement(sql)) {
            for (int i = 0; i < parameters.size(); i++) {
                preparedStatement.setObject(i + 1, parameters.get(i));
            }
            boolean isFirstRow = true;
            try (ResultSet resultSet = preparedStatement.executeQuery()) {
                while (resultSet.next()) {
                    Map<String, Object> map = new HashMap<>();
                    // only get the total once as it is the same for all rows.
                    if (isFirstRow) {
                        total = resultSet.getInt("total");
                        isFirstRow = false;
                    }
                    map.put("hostId", resultSet.getString("host_id"));
                    map.put("groupId", resultSet.getString("group_id"));
                    map.put("startTs", resultSet.getTimestamp("start_ts"));
                    map.put("endTs", resultSet.getTimestamp("end_ts"));
                    map.put("userId", resultSet.getString("user_id"));
                    map.put("entityId", resultSet.getString("entity_id"));
                    map.put("email", resultSet.getString("email"));
                    map.put("firstName", resultSet.getString("first_name"));
                    map.put("lastName", resultSet.getString("last_name"));
                    map.put("userType", resultSet.getString("user_type"));
                    groupUsers.add(map);
                }
            }
            Map<String, Object> resultMap = new HashMap<>();
            resultMap.put("total", total);
            resultMap.put("groupUsers", groupUsers);
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
    public Result<String> createGroupPermission(GroupPermissionCreatedEvent event) {
        final String insertGroup = "INSERT INTO group_permission_t (host_id, group_id, api_id, api_version, endpoint, update_user, update_ts) " +
                "VALUES (?, ?, ?, ?, ?,  ?, ?)";

        Result<String> result = null;
        try (Connection conn = ds.getConnection()) {
            conn.setAutoCommit(false);
            // no duplicate record, insert the user into database and write a success notification.
            try (PreparedStatement statement = conn.prepareStatement(insertGroup)) {
                statement.setString(1, event.getHostId());
                statement.setString(2, event.getGroupId());
                statement.setString(3, event.getApiId());
                statement.setString(4, event.getApiVersion());
                statement.setString(5, event.getEndpoint());
                statement.setString(6, event.getEventId().getId());
                statement.setTimestamp(7, new Timestamp(event.getEventId().getTimestamp()));

                int count = statement.executeUpdate();
                if (count == 0) {
                    throw new SQLException("failed to insert group permission " + event.getGroupId());
                }
                conn.commit();
                result = Success.of(event.getGroupId());
                insertNotification(event.getEventId(), event.getClass().getName(), AvroConverter.toJson(event, false), true, null);
            } catch (SQLException e) {
                logger.error("SQLException:", e);
                conn.rollback();
                insertNotification(event.getEventId(), event.getClass().getName(), AvroConverter.toJson(event, false), false, e.getMessage());
                result = Failure.of(new Status(SQL_EXCEPTION, e.getMessage()));
            } catch (Exception e) {
                logger.error("Exception:", e);
                conn.rollback();
                insertNotification(event.getEventId(), event.getClass().getName(), AvroConverter.toJson(event, false), false, e.getMessage());
                result = Failure.of(new Status(GENERIC_EXCEPTION, e.getMessage()));
            }
        } catch (SQLException e) {
            logger.error("SQLException:", e);
            result = Failure.of(new Status(SQL_EXCEPTION, e.getMessage()));
        }
        return result;
    }
    @Override
    public Result<String> deleteGroupPermission(GroupPermissionDeletedEvent event) {
        final String deleteGroup = "DELETE from group_permission_t WHERE host_id = ? AND group_id = ? AND api_id = ? AND api_version = ? AND endpoint = ?";
        Result<String> result;
        try (Connection conn = ds.getConnection()) {
            conn.setAutoCommit(false);
            try (PreparedStatement statement = conn.prepareStatement(deleteGroup)) {
                statement.setString(1, event.getHostId());
                statement.setString(2, event.getGroupId());
                statement.setString(3, event.getApiId());
                statement.setString(4, event.getApiVersion());
                statement.setString(5, event.getEndpoint());

                int count = statement.executeUpdate();
                if (count == 0) {
                    throw new SQLException("no record is deleted for group permission " + event.getGroupId());
                }
                conn.commit();
                result = Success.of(event.getEventId().getId());
                insertNotification(event.getEventId(), event.getClass().getName(), AvroConverter.toJson(event, false), true, null);
            } catch (SQLException e) {
                logger.error("SQLException:", e);
                conn.rollback();
                insertNotification(event.getEventId(), event.getClass().getName(), AvroConverter.toJson(event, false), false, e.getMessage());
                result = Failure.of(new Status(SQL_EXCEPTION, e.getMessage()));
            } catch (Exception e) {
                logger.error("Exception:", e);
                conn.rollback();
                insertNotification(event.getEventId(), event.getClass().getName(), AvroConverter.toJson(event, false), false, e.getMessage());
                result = Failure.of(new Status(GENERIC_EXCEPTION, e.getMessage()));
            }
        } catch (SQLException e) {
            logger.error("SQLException:", e);
            result = Failure.of(new Status(SQL_EXCEPTION, e.getMessage()));
        }
        return result;

    }
    @Override
    public Result<String> createGroupUser(GroupUserCreatedEvent event) {
        final String insertGroup = "INSERT INTO group_user_t (host_id, group_id, user_id, start_ts, end_ts, update_user, update_ts) " +
                "VALUES (?, ?, ?, ?, ?,  ?, ?)";

        Result<String> result = null;
        try (Connection conn = ds.getConnection()) {
            conn.setAutoCommit(false);
            // no duplicate record, insert the user into database and write a success notification.
            try (PreparedStatement statement = conn.prepareStatement(insertGroup)) {
                statement.setString(1, event.getHostId());
                statement.setString(2, event.getGroupId());
                statement.setString(3, event.getUserId());

                if(event.getStartTs() != null)
                    statement.setObject(4, event.getStartTs());
                else
                    statement.setNull(4, NULL);

                if (event.getEndTs() != null) {
                    statement.setObject(5, event.getEndTs());
                } else {
                    statement.setNull(5, NULL);
                }
                statement.setString(6, event.getEventId().getId());
                statement.setTimestamp(7, new Timestamp(event.getEventId().getTimestamp()));

                int count = statement.executeUpdate();
                if (count == 0) {
                    throw new SQLException("failed to insert group user " + event.getGroupId());
                }
                conn.commit();
                result = Success.of(event.getGroupId());
                insertNotification(event.getEventId(), event.getClass().getName(), AvroConverter.toJson(event, false), true, null);
            } catch (SQLException e) {
                logger.error("SQLException:", e);
                conn.rollback();
                insertNotification(event.getEventId(), event.getClass().getName(), AvroConverter.toJson(event, false), false, e.getMessage());
                result = Failure.of(new Status(SQL_EXCEPTION, e.getMessage()));
            } catch (Exception e) {
                logger.error("Exception:", e);
                conn.rollback();
                insertNotification(event.getEventId(), event.getClass().getName(), AvroConverter.toJson(event, false), false, e.getMessage());
                result = Failure.of(new Status(GENERIC_EXCEPTION, e.getMessage()));
            }
        } catch (SQLException e) {
            logger.error("SQLException:", e);
            result = Failure.of(new Status(SQL_EXCEPTION, e.getMessage()));
        }
        return result;

    }
    @Override
    public Result<String> updateGroupUser(GroupUserUpdatedEvent event) {
        final String updateGroup = "UPDATE group_user_t SET start_ts = ?, end_ts = ?, update_user = ?, update_ts = ? " +
                "WHERE host_id = ? AND group_id = ? AND user_id = ?";

        Result<String> result = null;
        try (Connection conn = ds.getConnection()) {
            conn.setAutoCommit(false);

            try (PreparedStatement statement = conn.prepareStatement(updateGroup)) {
                if(event.getStartTs() != null)
                    statement.setObject(1, event.getStartTs());
                else
                    statement.setNull(1, NULL);

                if (event.getEndTs() != null) {
                    statement.setObject(2, event.getEndTs());
                } else {
                    statement.setNull(2, NULL);
                }
                statement.setString(3, event.getEventId().getId());
                statement.setTimestamp(4, new Timestamp(event.getEventId().getTimestamp()));
                statement.setString(5, event.getHostId());
                statement.setString(6, event.getGroupId());
                statement.setString(7, event.getUserId());

                int count = statement.executeUpdate();
                if (count == 0) {
                    throw new SQLException("no record is updated for group user " + event.getGroupId());
                }
                conn.commit();
                result = Success.of(event.getGroupId());
                insertNotification(event.getEventId(), event.getClass().getName(), AvroConverter.toJson(event, false), true, null);
            } catch (SQLException e) {
                logger.error("SQLException:", e);
                conn.rollback();
                insertNotification(event.getEventId(), event.getClass().getName(), AvroConverter.toJson(event, false), false, e.getMessage());
                result = Failure.of(new Status(SQL_EXCEPTION, e.getMessage()));
            } catch (Exception e) {
                logger.error("Exception:", e);
                conn.rollback();
                insertNotification(event.getEventId(), event.getClass().getName(), AvroConverter.toJson(event, false), false, e.getMessage());
                result = Failure.of(new Status(GENERIC_EXCEPTION, e.getMessage()));
            }
        } catch (SQLException e) {
            logger.error("SQLException:", e);
            result = Failure.of(new Status(SQL_EXCEPTION, e.getMessage()));
        }
        return result;

    }
    @Override
    public Result<String> deleteGroupUser(GroupUserDeletedEvent event) {
        final String deleteGroup = "DELETE from group_user_t WHERE host_id = ? AND group_id = ? AND user_id = ?";
        Result<String> result;
        try (Connection conn = ds.getConnection()) {
            conn.setAutoCommit(false);
            try (PreparedStatement statement = conn.prepareStatement(deleteGroup)) {
                statement.setString(1, event.getHostId());
                statement.setString(2, event.getGroupId());
                statement.setString(3, event.getUserId());

                int count = statement.executeUpdate();
                if (count == 0) {
                    throw new SQLException("no record is deleted for group user " + event.getGroupId());
                }
                conn.commit();
                result = Success.of(event.getEventId().getId());
                insertNotification(event.getEventId(), event.getClass().getName(), AvroConverter.toJson(event, false), true, null);
            } catch (SQLException e) {
                logger.error("SQLException:", e);
                conn.rollback();
                insertNotification(event.getEventId(), event.getClass().getName(), AvroConverter.toJson(event, false), false, e.getMessage());
                result = Failure.of(new Status(SQL_EXCEPTION, e.getMessage()));
            } catch (Exception e) {
                logger.error("Exception:", e);
                conn.rollback();
                insertNotification(event.getEventId(), event.getClass().getName(), AvroConverter.toJson(event, false), false, e.getMessage());
                result = Failure.of(new Status(GENERIC_EXCEPTION, e.getMessage()));
            }
        } catch (SQLException e) {
            logger.error("SQLException:", e);
            result = Failure.of(new Status(SQL_EXCEPTION, e.getMessage()));
        }
        return result;

    }

    @Override
    public Result<String> queryGroupRowFilter(int offset, int limit, String hostId, String GroupId, String apiId, String apiVersion, String endpoint) {
        Result<String> result;
        StringBuilder sqlBuilder = new StringBuilder();
        sqlBuilder.append("SELECT COUNT(*) OVER () AS total, \n" +
                "g.host_id, g.group_id, p.api_id, p.api_version, p.endpoint, p.col_name, p.operator, p.col_value\n" +
                "FROM group_t g, group_row_filter_t p\n" +
                "WHERE g.group_id = p.group_id\n" +
                "AND g.host_id = ?\n");

        List<Object> parameters = new ArrayList<>();
        parameters.add(hostId);

        StringBuilder whereClause = new StringBuilder();

        addCondition(whereClause, parameters, "g.group_id", GroupId);
        addCondition(whereClause, parameters, "p.api_id", apiId);
        addCondition(whereClause, parameters, "p.api_version", apiVersion);
        addCondition(whereClause, parameters, "p.endpoint", endpoint);

        if (whereClause.length() > 0) {
            sqlBuilder.append("AND ").append(whereClause);
        }

        sqlBuilder.append(" ORDER BY g.group_id, p.api_id, p.api_version, p.endpoint\n" +
                "LIMIT ? OFFSET ?");

        parameters.add(limit);
        parameters.add(offset);

        String sql = sqlBuilder.toString();
        if(logger.isTraceEnabled()) logger.trace("queryGroupRowFilter sql: {}", sql);
        int total = 0;
        List<Map<String, Object>> groupRowFilters = new ArrayList<>();

        try (final Connection conn = ds.getConnection(); PreparedStatement preparedStatement = conn.prepareStatement(sql)) {
            for (int i = 0; i < parameters.size(); i++) {
                preparedStatement.setObject(i + 1, parameters.get(i));
            }
            boolean isFirstRow = true;
            try (ResultSet resultSet = preparedStatement.executeQuery()) {
                while (resultSet.next()) {
                    Map<String, Object> map = new HashMap<>();
                    // only get the total once as it is the same for all rows.
                    if (isFirstRow) {
                        total = resultSet.getInt("total");
                        isFirstRow = false;
                    }
                    map.put("hostId", resultSet.getString("host_id"));
                    map.put("groupId", resultSet.getString("group_id"));
                    map.put("apiId", resultSet.getString("api_id"));
                    map.put("apiVersion", resultSet.getString("api_version"));
                    map.put("endpoint", resultSet.getString("endpoint"));
                    map.put("colName", resultSet.getString("col_name"));
                    map.put("operator", resultSet.getString("operator"));
                    map.put("colValue", resultSet.getString("col_value"));
                    groupRowFilters.add(map);
                }
            }
            Map<String, Object> resultMap = new HashMap<>();
            resultMap.put("total", total);
            resultMap.put("groupRowFilters", groupRowFilters);
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
    public Result<String> createGroupRowFilter(GroupRowFilterCreatedEvent event) {
        final String insertGroup = "INSERT INTO group_row_filter_t (host_id, group_id, api_id, api_version, endpoint, col_name, operator, col_value, update_user, update_ts) " +
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";

        Result<String> result = null;
        try (Connection conn = ds.getConnection()) {
            conn.setAutoCommit(false);
            // no duplicate record, insert the user into database and write a success notification.
            try (PreparedStatement statement = conn.prepareStatement(insertGroup)) {
                statement.setString(1, event.getHostId());
                statement.setString(2, event.getGroupId());
                statement.setString(3, event.getApiId());
                statement.setString(4, event.getApiVersion());
                statement.setString(5, event.getEndpoint());
                statement.setString(6, event.getColName());
                statement.setString(7, event.getOperator());
                statement.setString(8, event.getColValue());
                statement.setString(9, event.getEventId().getId());
                statement.setTimestamp(10, new Timestamp(event.getEventId().getTimestamp()));

                int count = statement.executeUpdate();
                if (count == 0) {
                    throw new SQLException("failed to insert group row filter " + event.getGroupId());
                }
                conn.commit();
                result = Success.of(event.getGroupId());
                insertNotification(event.getEventId(), event.getClass().getName(), AvroConverter.toJson(event, false), true, null);
            } catch (SQLException e) {
                logger.error("SQLException:", e);
                conn.rollback();
                insertNotification(event.getEventId(), event.getClass().getName(), AvroConverter.toJson(event, false), false, e.getMessage());
                result = Failure.of(new Status(SQL_EXCEPTION, e.getMessage()));
            } catch (Exception e) {
                logger.error("Exception:", e);
                conn.rollback();
                insertNotification(event.getEventId(), event.getClass().getName(), AvroConverter.toJson(event, false), false, e.getMessage());
                result = Failure.of(new Status(GENERIC_EXCEPTION, e.getMessage()));
            }
        } catch (SQLException e) {
            logger.error("SQLException:", e);
            result = Failure.of(new Status(SQL_EXCEPTION, e.getMessage()));
        }
        return result;
    }

    @Override
    public Result<String> updateGroupRowFilter(GroupRowFilterUpdatedEvent event) {
        final String updateGroup = "UPDATE group_row_filter_t SET operator = ?, col_value = ?, update_user = ?, update_ts = ? " +
                "WHERE host_id = ? AND group_id = ? AND api_id = ? AND api_version = ? AND endpoint = ? AND col_name = ?";

        Result<String> result = null;
        try (Connection conn = ds.getConnection()) {
            conn.setAutoCommit(false);

            try (PreparedStatement statement = conn.prepareStatement(updateGroup)) {
                statement.setString(1, event.getOperator());
                statement.setString(2, event.getColValue());
                statement.setString(3, event.getEventId().getId());
                statement.setTimestamp(4, new Timestamp(event.getEventId().getTimestamp()));
                statement.setString(5, event.getHostId());
                statement.setString(6, event.getGroupId());
                statement.setString(7, event.getApiId());
                statement.setString(8, event.getApiVersion());
                statement.setString(9, event.getEndpoint());
                statement.setString(10, event.getColName());

                int count = statement.executeUpdate();
                if (count == 0) {
                    throw new SQLException("no record is updated for group row filter " + event.getGroupId());
                }
                conn.commit();
                result = Success.of(event.getGroupId());
                insertNotification(event.getEventId(), event.getClass().getName(), AvroConverter.toJson(event, false), true, null);
            } catch (SQLException e) {
                logger.error("SQLException:", e);
                conn.rollback();
                insertNotification(event.getEventId(), event.getClass().getName(), AvroConverter.toJson(event, false), false, e.getMessage());
                result = Failure.of(new Status(SQL_EXCEPTION, e.getMessage()));
            } catch (Exception e) {
                logger.error("Exception:", e);
                conn.rollback();
                insertNotification(event.getEventId(), event.getClass().getName(), AvroConverter.toJson(event, false), false, e.getMessage());
                result = Failure.of(new Status(GENERIC_EXCEPTION, e.getMessage()));
            }
        } catch (SQLException e) {
            logger.error("SQLException:", e);
            result = Failure.of(new Status(SQL_EXCEPTION, e.getMessage()));
        }
        return result;
    }

    @Override
    public Result<String> deleteGroupRowFilter(GroupRowFilterDeletedEvent event) {
        final String deleteGroup = "DELETE from group_row_filter_t WHERE host_id = ? AND group_id = ? AND api_id = ? AND api_version = ? AND endpoint = ? AND col_name = ?";
        Result<String> result;
        try (Connection conn = ds.getConnection()) {
            conn.setAutoCommit(false);
            try (PreparedStatement statement = conn.prepareStatement(deleteGroup)) {
                statement.setString(1, event.getHostId());
                statement.setString(2, event.getGroupId());
                statement.setString(3, event.getApiId());
                statement.setString(4, event.getApiVersion());
                statement.setString(5, event.getEndpoint());
                statement.setString(6, event.getColName());

                int count = statement.executeUpdate();
                if (count == 0) {
                    throw new SQLException("no record is deleted for group row filter " + event.getGroupId());
                }
                conn.commit();
                result = Success.of(event.getEventId().getId());
                insertNotification(event.getEventId(), event.getClass().getName(), AvroConverter.toJson(event, false), true, null);
            } catch (SQLException e) {
                logger.error("SQLException:", e);
                conn.rollback();
                insertNotification(event.getEventId(), event.getClass().getName(), AvroConverter.toJson(event, false), false, e.getMessage());
                result = Failure.of(new Status(SQL_EXCEPTION, e.getMessage()));
            } catch (Exception e) {
                logger.error("Exception:", e);
                conn.rollback();
                insertNotification(event.getEventId(), event.getClass().getName(), AvroConverter.toJson(event, false), false, e.getMessage());
                result = Failure.of(new Status(GENERIC_EXCEPTION, e.getMessage()));
            }
        } catch (SQLException e) {
            logger.error("SQLException:", e);
            result = Failure.of(new Status(SQL_EXCEPTION, e.getMessage()));
        }
        return result;
    }

    @Override
    public Result<String> queryGroupColFilter(int offset, int limit, String hostId, String GroupId, String apiId, String apiVersion, String endpoint) {
        Result<String> result;
        StringBuilder sqlBuilder = new StringBuilder();
        sqlBuilder.append("SELECT COUNT(*) OVER () AS total, \n" +
                "g.host_id, g.group_id, p.api_id, p.api_version, p.endpoint, p.columns\n" +
                "FROM group_t g, group_col_filter_t p\n" +
                "WHERE g.group_id = p.group_id\n" +
                "AND g.host_id = ?\n");

        List<Object> parameters = new ArrayList<>();
        parameters.add(hostId);

        StringBuilder whereClause = new StringBuilder();

        addCondition(whereClause, parameters, "g.group_id", GroupId);
        addCondition(whereClause, parameters, "p.api_id", apiId);
        addCondition(whereClause, parameters, "p.api_version", apiVersion);
        addCondition(whereClause, parameters, "p.endpoint", endpoint);

        if (whereClause.length() > 0) {
            sqlBuilder.append("AND ").append(whereClause);
        }

        sqlBuilder.append(" ORDER BY g.group_id, p.api_id, p.api_version, p.endpoint\n" +
                "LIMIT ? OFFSET ?");

        parameters.add(limit);
        parameters.add(offset);

        String sql = sqlBuilder.toString();
        if(logger.isTraceEnabled()) logger.trace("queryGroupColFilter sql: {}", sql);
        int total = 0;
        List<Map<String, Object>> groupColFilters = new ArrayList<>();

        try (final Connection conn = ds.getConnection(); PreparedStatement preparedStatement = conn.prepareStatement(sql)) {
            for (int i = 0; i < parameters.size(); i++) {
                preparedStatement.setObject(i + 1, parameters.get(i));
            }
            boolean isFirstRow = true;
            try (ResultSet resultSet = preparedStatement.executeQuery()) {
                while (resultSet.next()) {
                    Map<String, Object> map = new HashMap<>();
                    // only get the total once as it is the same for all rows.
                    if (isFirstRow) {
                        total = resultSet.getInt("total");
                        isFirstRow = false;
                    }
                    map.put("hostId", resultSet.getString("host_id"));
                    map.put("groupId", resultSet.getString("group_id"));
                    map.put("apiId", resultSet.getString("api_id"));
                    map.put("apiVersion", resultSet.getString("api_version"));
                    map.put("endpoint", resultSet.getString("endpoint"));
                    map.put("columns", resultSet.getString("columns"));
                    groupColFilters.add(map);
                }
            }
            Map<String, Object> resultMap = new HashMap<>();
            resultMap.put("total", total);
            resultMap.put("groupColFilters", groupColFilters);
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
    public Result<String> createGroupColFilter(GroupColFilterCreatedEvent event) {
        final String insertGroup = "INSERT INTO group_col_filter_t (host_id, group_id, api_id, api_version, endpoint, columns, update_user, update_ts) " +
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?)";

        Result<String> result = null;
        try (Connection conn = ds.getConnection()) {
            conn.setAutoCommit(false);
            // no duplicate record, insert the user into database and write a success notification.
            try (PreparedStatement statement = conn.prepareStatement(insertGroup)) {
                statement.setString(1, event.getHostId());
                statement.setString(2, event.getGroupId());
                statement.setString(3, event.getApiId());
                statement.setString(4, event.getApiVersion());
                statement.setString(5, event.getEndpoint());
                statement.setString(6, event.getColumns());
                statement.setString(7, event.getEventId().getId());
                statement.setTimestamp(8, new Timestamp(event.getEventId().getTimestamp()));

                int count = statement.executeUpdate();
                if (count == 0) {
                    throw new SQLException("failed to insert group col filter " + event.getGroupId());
                }
                conn.commit();
                result = Success.of(event.getGroupId());
                insertNotification(event.getEventId(), event.getClass().getName(), AvroConverter.toJson(event, false), true, null);
            } catch (SQLException e) {
                logger.error("SQLException:", e);
                conn.rollback();
                insertNotification(event.getEventId(), event.getClass().getName(), AvroConverter.toJson(event, false), false, e.getMessage());
                result = Failure.of(new Status(SQL_EXCEPTION, e.getMessage()));
            } catch (Exception e) {
                logger.error("Exception:", e);
                conn.rollback();
                insertNotification(event.getEventId(), event.getClass().getName(), AvroConverter.toJson(event, false), false, e.getMessage());
                result = Failure.of(new Status(GENERIC_EXCEPTION, e.getMessage()));
            }
        } catch (SQLException e) {
            logger.error("SQLException:", e);
            result = Failure.of(new Status(SQL_EXCEPTION, e.getMessage()));
        }
        return result;
    }

    @Override
    public Result<String> updateGroupColFilter(GroupColFilterUpdatedEvent event) {
        final String updateGroup = "UPDATE group_col_filter_t SET columns = ?, update_user = ?, update_ts = ? " +
                "WHERE host_id = ? AND group_id = ? AND api_id = ? AND api_version = ? AND endpoint = ?";

        Result<String> result = null;
        try (Connection conn = ds.getConnection()) {
            conn.setAutoCommit(false);

            try (PreparedStatement statement = conn.prepareStatement(updateGroup)) {
                statement.setString(1, event.getColumns());
                statement.setString(2, event.getEventId().getId());
                statement.setTimestamp(3, new Timestamp(event.getEventId().getTimestamp()));
                statement.setString(4, event.getHostId());
                statement.setString(5, event.getGroupId());
                statement.setString(6, event.getApiId());
                statement.setString(7, event.getApiVersion());
                statement.setString(8, event.getEndpoint());

                int count = statement.executeUpdate();
                if (count == 0) {
                    throw new SQLException("no record is updated for group col filter " + event.getGroupId());
                }
                conn.commit();
                result = Success.of(event.getGroupId());
                insertNotification(event.getEventId(), event.getClass().getName(), AvroConverter.toJson(event, false), true, null);
            } catch (SQLException e) {
                logger.error("SQLException:", e);
                conn.rollback();
                insertNotification(event.getEventId(), event.getClass().getName(), AvroConverter.toJson(event, false), false, e.getMessage());
                result = Failure.of(new Status(SQL_EXCEPTION, e.getMessage()));
            } catch (Exception e) {
                logger.error("Exception:", e);
                conn.rollback();
                insertNotification(event.getEventId(), event.getClass().getName(), AvroConverter.toJson(event, false), false, e.getMessage());
                result = Failure.of(new Status(GENERIC_EXCEPTION, e.getMessage()));
            }
        } catch (SQLException e) {
            logger.error("SQLException:", e);
            result = Failure.of(new Status(SQL_EXCEPTION, e.getMessage()));
        }
        return result;
    }

    @Override
    public Result<String> deleteGroupColFilter(GroupColFilterDeletedEvent event) {
        final String deleteGroup = "DELETE from group_col_filter_t WHERE host_id = ? AND group_id = ? AND api_id = ? AND api_version = ? AND endpoint = ?";
        Result<String> result;
        try (Connection conn = ds.getConnection()) {
            conn.setAutoCommit(false);
            try (PreparedStatement statement = conn.prepareStatement(deleteGroup)) {
                statement.setString(1, event.getHostId());
                statement.setString(2, event.getGroupId());
                statement.setString(3, event.getApiId());
                statement.setString(4, event.getApiVersion());
                statement.setString(5, event.getEndpoint());

                int count = statement.executeUpdate();
                if (count == 0) {
                    throw new SQLException("no record is deleted for group col filter " + event.getGroupId());
                }
                conn.commit();
                result = Success.of(event.getEventId().getId());
                insertNotification(event.getEventId(), event.getClass().getName(), AvroConverter.toJson(event, false), true, null);
            } catch (SQLException e) {
                logger.error("SQLException:", e);
                conn.rollback();
                insertNotification(event.getEventId(), event.getClass().getName(), AvroConverter.toJson(event, false), false, e.getMessage());
                result = Failure.of(new Status(SQL_EXCEPTION, e.getMessage()));
            } catch (Exception e) {
                logger.error("Exception:", e);
                conn.rollback();
                insertNotification(event.getEventId(), event.getClass().getName(), AvroConverter.toJson(event, false), false, e.getMessage());
                result = Failure.of(new Status(GENERIC_EXCEPTION, e.getMessage()));
            }
        } catch (SQLException e) {
            logger.error("SQLException:", e);
            result = Failure.of(new Status(SQL_EXCEPTION, e.getMessage()));
        }
        return result;
    }


    @Override
    public Result<String> createPosition(PositionCreatedEvent event) {
        final String insertPosition = "INSERT INTO position_t (host_id, position_id, position_desc, " +
                "inherit_to_ancestor, inherit_to_sibling, update_user, update_ts) " +
                "VALUES (?, ?, ?, ?, ?, ?, ?)";

        Result<String> result = null;
        try (Connection conn = ds.getConnection()) {
            conn.setAutoCommit(false);
            try (PreparedStatement statement = conn.prepareStatement(insertPosition)) {
                statement.setString(1, event.getHostId());
                statement.setString(2, event.getPositionId());
                if (event.getPositionDesc() != null)
                    statement.setString(3, event.getPositionDesc());
                else
                    statement.setNull(3, NULL);
                if(event.getInheritToAncestor() != null)
                    statement.setString(4, event.getInheritToAncestor());
                else
                    statement.setNull(4, NULL);
                if(event.getInheritToSibling() != null)
                    statement.setString(5, event.getInheritToSibling());
                else
                    statement.setNull(5, NULL);

                statement.setString(6, event.getEventId().getId());
                statement.setTimestamp(7, new Timestamp(event.getEventId().getTimestamp()));

                int count = statement.executeUpdate();
                if (count == 0) {
                    throw new SQLException("failed to insert position " + event.getPositionId());
                }
                conn.commit();
                result = Success.of(event.getPositionId());
                insertNotification(event.getEventId(), event.getClass().getName(), AvroConverter.toJson(event, false), true, null);
            } catch (SQLException e) {
                logger.error("SQLException:", e);
                conn.rollback();
                insertNotification(event.getEventId(), event.getClass().getName(), AvroConverter.toJson(event, false), false, e.getMessage());
                result = Failure.of(new Status(SQL_EXCEPTION, e.getMessage()));
            } catch (Exception e) {
                logger.error("Exception:", e);
                conn.rollback();
                insertNotification(event.getEventId(), event.getClass().getName(), AvroConverter.toJson(event, false), false, e.getMessage());
                result = Failure.of(new Status(GENERIC_EXCEPTION, e.getMessage()));
            }
        } catch (SQLException e) {
            logger.error("SQLException:", e);
            result = Failure.of(new Status(SQL_EXCEPTION, e.getMessage()));
        }
        return result;
    }

    @Override
    public Result<String> updatePosition(PositionUpdatedEvent event) {
        final String updatePosition = "UPDATE position_t SET position_desc = ?, inherit_to_ancestor = ?, inherit_to_sibling = ?, " +
                "update_user = ?, update_ts = ? " +
                "WHERE host_id = ? AND position_id = ?";

        Result<String> result = null;
        try (Connection conn = ds.getConnection()) {
            conn.setAutoCommit(false);
            try (PreparedStatement statement = conn.prepareStatement(updatePosition)) {
                if(event.getPositionDesc() != null) {
                    statement.setString(1, event.getPositionDesc());
                } else {
                    statement.setNull(1, NULL);
                }
                if(event.getInheritToAncestor() != null) {
                    statement.setString(2, event.getInheritToAncestor());
                } else {
                    statement.setNull(2, NULL);
                }
                if(event.getInheritToSibling() != null) {
                    statement.setString(3, event.getInheritToSibling());
                } else {
                    statement.setNull(3, NULL);
                }
                statement.setString(4, event.getEventId().getId());
                statement.setTimestamp(5, new Timestamp(event.getEventId().getTimestamp()));

                statement.setString(6, event.getHostId());
                statement.setString(7, event.getPositionId());

                int count = statement.executeUpdate();
                if (count == 0) {
                    throw new SQLException("no record is updated for position " + event.getPositionId());
                }
                conn.commit();
                result = Success.of(event.getPositionId());
                insertNotification(event.getEventId(), event.getClass().getName(), AvroConverter.toJson(event, false), true, null);
            } catch (SQLException e) {
                logger.error("SQLException:", e);
                conn.rollback();
                insertNotification(event.getEventId(), event.getClass().getName(), AvroConverter.toJson(event, false), false, e.getMessage());
                result = Failure.of(new Status(SQL_EXCEPTION, e.getMessage()));
            } catch (Exception e) {
                logger.error("Exception:", e);
                conn.rollback();
                insertNotification(event.getEventId(), event.getClass().getName(), AvroConverter.toJson(event, false), false, e.getMessage());
                result = Failure.of(new Status(GENERIC_EXCEPTION, e.getMessage()));
            }
        } catch (SQLException e) {
            logger.error("SQLException:", e);
            result = Failure.of(new Status(SQL_EXCEPTION, e.getMessage()));
        }
        return result;

    }

    @Override
    public Result<String> deletePosition(PositionDeletedEvent event) {
        final String deleteGroup = "DELETE from position_t WHERE host_id = ? AND position_id = ?";
        Result<String> result;
        try (Connection conn = ds.getConnection()) {
            conn.setAutoCommit(false);
            try (PreparedStatement statement = conn.prepareStatement(deleteGroup)) {
                statement.setString(1, event.getHostId());
                statement.setString(2, event.getPositionId());
                int count = statement.executeUpdate();
                if (count == 0) {
                    throw new SQLException("no record is deleted for position " + event.getPositionId());
                }
                conn.commit();
                result = Success.of(event.getEventId().getId());
                insertNotification(event.getEventId(), event.getClass().getName(), AvroConverter.toJson(event, false), true, null);
            } catch (SQLException e) {
                logger.error("SQLException:", e);
                conn.rollback();
                insertNotification(event.getEventId(), event.getClass().getName(), AvroConverter.toJson(event, false), false, e.getMessage());
                result = Failure.of(new Status(SQL_EXCEPTION, e.getMessage()));
            } catch (Exception e) {
                logger.error("Exception:", e);
                conn.rollback();
                insertNotification(event.getEventId(), event.getClass().getName(), AvroConverter.toJson(event, false), false, e.getMessage());
                result = Failure.of(new Status(GENERIC_EXCEPTION, e.getMessage()));
            }
        } catch (SQLException e) {
            logger.error("SQLException:", e);
            result = Failure.of(new Status(SQL_EXCEPTION, e.getMessage()));
        }
        return result;
    }


    @Override
    public Result<String> queryPosition(int offset, int limit, String hostId, String positionId, String positionDesc, String inheritToAncestor, String inheritToSibling) {
        Result<String> result;
        StringBuilder sqlBuilder = new StringBuilder();
        sqlBuilder.append("SELECT COUNT(*) OVER () AS total, host_id, position_id, position_desc, inherit_to_ancestor, inherit_to_sibling, update_user, update_ts " +
                "FROM position_t " +
                "WHERE host_id = ?\n");
        List<Object> parameters = new ArrayList<>();
        parameters.add(hostId);

        StringBuilder whereClause = new StringBuilder();
        addCondition(whereClause, parameters, "position_id", positionId);
        addCondition(whereClause, parameters, "position_desc", positionDesc);
        addCondition(whereClause, parameters, "inherit_to_ancestor", inheritToAncestor);
        addCondition(whereClause, parameters, "inherit_to_sibling", inheritToSibling);


        if (whereClause.length() > 0) {
            sqlBuilder.append("AND ").append(whereClause);
        }
        sqlBuilder.append(" ORDER BY position_id\n" +
                "LIMIT ? OFFSET ?");
        parameters.add(limit);
        parameters.add(offset);

        String sql = sqlBuilder.toString();

        int total = 0;
        List<Map<String, Object>> positions = new ArrayList<>();

        try (final Connection conn = ds.getConnection(); PreparedStatement preparedStatement = conn.prepareStatement(sql)) {
            for (int i = 0; i < parameters.size(); i++) {
                preparedStatement.setObject(i + 1, parameters.get(i));
            }
            boolean isFirstRow = true;

            try (ResultSet resultSet = preparedStatement.executeQuery()) {
                while (resultSet.next()) {
                    Map<String, Object> map = new HashMap<>();
                    // only get the total once as it is the same for all rows.
                    if (isFirstRow) {
                        total = resultSet.getInt("total");
                        isFirstRow = false;
                    }
                    map.put("hostId", resultSet.getString("host_id"));
                    map.put("positionId", resultSet.getString("position_id"));
                    map.put("positionDesc", resultSet.getString("position_desc"));
                    map.put("inheritToAncestor", resultSet.getString("inherit_to_ancestor"));
                    map.put("inheritToSibling", resultSet.getString("inherit_to_sibling"));
                    map.put("updateUser", resultSet.getString("update_user"));
                    map.put("updateTs", resultSet.getTimestamp("update_ts"));
                    positions.add(map);
                }
            }
            Map<String, Object> resultMap = new HashMap<>();
            resultMap.put("total", total);
            resultMap.put("positions", positions);
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
    public Result<String> queryPositionLabel(String hostId) {
        final String sql = "SELECT position_id from position_t WHERE host_id = ?";
        Result<String> result;
        try (final Connection conn = ds.getConnection()) {
            List<Map<String, Object>> list = new ArrayList<>();
            try (PreparedStatement statement = conn.prepareStatement(sql)) {
                statement.setString(1, hostId);
                try (ResultSet resultSet = statement.executeQuery()) {
                    while (resultSet.next()) {
                        Map<String, Object> map = new HashMap<>();
                        String id = resultSet.getString("position_id");
                        map.put("id", id);
                        map.put("label", id);
                        list.add(map);
                    }
                }
            }
            if (list.isEmpty())
                result = Failure.of(new Status(OBJECT_NOT_FOUND, "position", hostId));
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
    public Result<String> queryPositionPermission(int offset, int limit, String hostId, String positionId, String inheritToAncestor, String inheritToSibling, String apiId, String apiVersion, String endpoint) {
        Result<String> result;
        StringBuilder sqlBuilder = new StringBuilder();
        sqlBuilder.append("SELECT COUNT(*) OVER () AS total, \n" +
                "o.host_id, o.position_id, o.inherit_to_ancestor, o.inherit_to_sibling, " +
                "p.api_id, p.api_version, p.endpoint\n" +
                "FROM position_t o, position_permission_t p\n" +
                "WHERE o.position_id = p.position_id\n" +
                "AND o.host_id = ?\n");


        List<Object> parameters = new ArrayList<>();
        parameters.add(hostId);


        StringBuilder whereClause = new StringBuilder();

        addCondition(whereClause, parameters, "o.position_id", positionId);
        addCondition(whereClause, parameters, "o.inherit_to_ancestor", inheritToAncestor);
        addCondition(whereClause, parameters, "o.inherit_to_sibling", inheritToSibling);
        addCondition(whereClause, parameters, "p.api_id", apiId);
        addCondition(whereClause, parameters, "p.api_version", apiVersion);
        addCondition(whereClause, parameters, "p.endpoint", endpoint);

        if (whereClause.length() > 0) {
            sqlBuilder.append("AND ").append(whereClause);
        }

        sqlBuilder.append(" ORDER BY o.position_id, p.api_id, p.api_version, p.endpoint\n" +
                "LIMIT ? OFFSET ?");

        parameters.add(limit);
        parameters.add(offset);

        String sql = sqlBuilder.toString();
        if(logger.isTraceEnabled()) logger.trace("queryPositionPermission sql: {}", sql);
        int total = 0;
        List<Map<String, Object>> positionPermissions = new ArrayList<>();


        try (final Connection conn = ds.getConnection(); PreparedStatement preparedStatement = conn.prepareStatement(sql)) {
            for (int i = 0; i < parameters.size(); i++) {
                preparedStatement.setObject(i + 1, parameters.get(i));
            }
            boolean isFirstRow = true;
            try (ResultSet resultSet = preparedStatement.executeQuery()) {
                while (resultSet.next()) {
                    Map<String, Object> map = new HashMap<>();
                    // only get the total once as it is the same for all rows.
                    if (isFirstRow) {
                        total = resultSet.getInt("total");
                        isFirstRow = false;
                    }
                    map.put("hostId", resultSet.getString("host_id"));
                    map.put("positionId", resultSet.getString("position_id"));
                    map.put("inheritToAncestor", resultSet.getString("inherit_to_ancestor"));
                    map.put("inheritToSibling", resultSet.getString("inherit_to_sibling"));
                    map.put("apiId", resultSet.getString("api_id"));
                    map.put("apiVersion", resultSet.getString("api_version"));
                    map.put("endpoint", resultSet.getString("endpoint"));
                    positionPermissions.add(map);
                }
            }
            Map<String, Object> resultMap = new HashMap<>();
            resultMap.put("total", total);
            resultMap.put("positionPermissions", positionPermissions);
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
    public Result<String> queryPositionUser(int offset, int limit, String hostId, String positionId, String positionType, String inheritToAncestor, String inheritToSibling, String userId, String entityId, String email, String firstName, String lastName, String userType) {
        Result<String> result;
        StringBuilder sqlBuilder = new StringBuilder();
        sqlBuilder.append("SELECT COUNT(*) OVER () AS total, \n" +
                "ep.host_id, ep.position_id, ep.position_type, \n " +
                "ep.start_ts, ep.end_ts, u.user_id, \n" +
                "u.email, u.user_type, e.employee_id AS entity_id,\n" +
                "e.manager_id, u.first_name, u.last_name\n" +
                "FROM user_t u\n" +
                "INNER JOIN\n" +
                "    employee_t e ON u.user_id = e.user_id AND u.user_type = 'E'\n" +
                "INNER JOIN\n" +
                "    employee_position_t ep ON ep.employee_id = e.employee_id\n" +
                "AND ep.host_id = ?\n");

        List<Object> parameters = new ArrayList<>();
        parameters.add(hostId);


        StringBuilder whereClause = new StringBuilder();

        addCondition(whereClause, parameters, "ep.position_id", positionId);
        addCondition(whereClause, parameters, "ep.position_type", positionType);
        addCondition(whereClause, parameters, "u.user_id", userId);
        addCondition(whereClause, parameters, "entity_id", entityId);
        addCondition(whereClause, parameters, "u.email", email);
        addCondition(whereClause, parameters, "u.first_name", firstName);
        addCondition(whereClause, parameters, "u.last_name", lastName);
        addCondition(whereClause, parameters, "u.user_type", userType);

        if (whereClause.length() > 0) {
            sqlBuilder.append("AND ").append(whereClause);
        }

        sqlBuilder.append(" ORDER BY ep.position_id, u.user_id\n" +
                "LIMIT ? OFFSET ?");

        parameters.add(limit);
        parameters.add(offset);

        String sql = sqlBuilder.toString();
        if(logger.isTraceEnabled()) logger.trace("queryPositionUser sql: {}", sql);
        int total = 0;
        List<Map<String, Object>> positionUsers = new ArrayList<>();


        try (final Connection conn = ds.getConnection(); PreparedStatement preparedStatement = conn.prepareStatement(sql)) {
            for (int i = 0; i < parameters.size(); i++) {
                preparedStatement.setObject(i + 1, parameters.get(i));
            }
            boolean isFirstRow = true;
            try (ResultSet resultSet = preparedStatement.executeQuery()) {
                while (resultSet.next()) {
                    Map<String, Object> map = new HashMap<>();
                    // only get the total once as it is the same for all rows.
                    if (isFirstRow) {
                        total = resultSet.getInt("total");
                        isFirstRow = false;
                    }
                    map.put("hostId", resultSet.getString("host_id"));
                    map.put("positionId", resultSet.getString("position_id"));
                    map.put("positionType", resultSet.getString("position_type"));
                    map.put("startTs", resultSet.getDate("start_ts"));
                    map.put("endTs", resultSet.getString("end_ts"));
                    map.put("userId", resultSet.getString("user_id"));
                    map.put("entityId", resultSet.getString("entity_id"));
                    map.put("email", resultSet.getString("email"));
                    map.put("firstName", resultSet.getString("first_name"));
                    map.put("lastName", resultSet.getString("last_name"));
                    map.put("userType", resultSet.getString("user_type"));
                    positionUsers.add(map);
                }
            }
            Map<String, Object> resultMap = new HashMap<>();
            resultMap.put("total", total);
            resultMap.put("positionUsers", positionUsers);
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
    public Result<String> createPositionPermission(PositionPermissionCreatedEvent event) {
        final String insertGroup = "INSERT INTO position_permission_t (host_id, position_id, api_id, api_version, endpoint, update_user, update_ts) " +
                "VALUES (?, ?, ?, ?, ?,  ?, ?)";

        Result<String> result = null;
        try (Connection conn = ds.getConnection()) {
            conn.setAutoCommit(false);
            // no duplicate record, insert the user into database and write a success notification.
            try (PreparedStatement statement = conn.prepareStatement(insertGroup)) {
                statement.setString(1, event.getHostId());
                statement.setString(2, event.getPositionId());
                statement.setString(3, event.getApiId());
                statement.setString(4, event.getApiVersion());
                statement.setString(5, event.getEndpoint());
                statement.setString(6, event.getEventId().getId());
                statement.setTimestamp(7, new Timestamp(event.getEventId().getTimestamp()));

                int count = statement.executeUpdate();
                if (count == 0) {
                    throw new SQLException("failed to insert position permission " + event.getPositionId());
                }
                conn.commit();
                result = Success.of(event.getPositionId());
                insertNotification(event.getEventId(), event.getClass().getName(), AvroConverter.toJson(event, false), true, null);
            } catch (SQLException e) {
                logger.error("SQLException:", e);
                conn.rollback();
                insertNotification(event.getEventId(), event.getClass().getName(), AvroConverter.toJson(event, false), false, e.getMessage());
                result = Failure.of(new Status(SQL_EXCEPTION, e.getMessage()));
            } catch (Exception e) {
                logger.error("Exception:", e);
                conn.rollback();
                insertNotification(event.getEventId(), event.getClass().getName(), AvroConverter.toJson(event, false), false, e.getMessage());
                result = Failure.of(new Status(GENERIC_EXCEPTION, e.getMessage()));
            }
        } catch (SQLException e) {
            logger.error("SQLException:", e);
            result = Failure.of(new Status(SQL_EXCEPTION, e.getMessage()));
        }
        return result;

    }

    @Override
    public Result<String> deletePositionPermission(PositionPermissionDeletedEvent event) {
        final String deleteGroup = "DELETE from position_permission_t WHERE host_id = ? AND position_id = ? AND api_id = ? AND api_version = ? AND endpoint = ?";
        Result<String> result;
        try (Connection conn = ds.getConnection()) {
            conn.setAutoCommit(false);
            try (PreparedStatement statement = conn.prepareStatement(deleteGroup)) {
                statement.setString(1, event.getHostId());
                statement.setString(2, event.getPositionId());
                statement.setString(3, event.getApiId());
                statement.setString(4, event.getApiVersion());
                statement.setString(5, event.getEndpoint());

                int count = statement.executeUpdate();
                if (count == 0) {
                    throw new SQLException("no record is deleted for position permission " + event.getPositionId());
                }
                conn.commit();
                result = Success.of(event.getEventId().getId());
                insertNotification(event.getEventId(), event.getClass().getName(), AvroConverter.toJson(event, false), true, null);
            } catch (SQLException e) {
                logger.error("SQLException:", e);
                conn.rollback();
                insertNotification(event.getEventId(), event.getClass().getName(), AvroConverter.toJson(event, false), false, e.getMessage());
                result = Failure.of(new Status(SQL_EXCEPTION, e.getMessage()));
            } catch (Exception e) {
                logger.error("Exception:", e);
                conn.rollback();
                insertNotification(event.getEventId(), event.getClass().getName(), AvroConverter.toJson(event, false), false, e.getMessage());
                result = Failure.of(new Status(GENERIC_EXCEPTION, e.getMessage()));
            }
        } catch (SQLException e) {
            logger.error("SQLException:", e);
            result = Failure.of(new Status(SQL_EXCEPTION, e.getMessage()));
        }
        return result;
    }

    @Override
    public Result<String> createPositionUser(PositionUserCreatedEvent event) {
        final String insertGroup = "INSERT INTO position_user_t (host_id, position_id, user_id, start_ts, end_ts, update_user, update_ts) " +
                "VALUES (?, ?, ?, ?, ?,  ?, ?)";

        Result<String> result = null;
        try (Connection conn = ds.getConnection()) {
            conn.setAutoCommit(false);
            // no duplicate record, insert the user into database and write a success notification.
            try (PreparedStatement statement = conn.prepareStatement(insertGroup)) {
                statement.setString(1, event.getHostId());
                statement.setString(2, event.getPositionId());
                statement.setString(3, event.getUserId());

                if(event.getStartTs() != null)
                    statement.setObject(4, event.getStartTs());
                else
                    statement.setNull(4, NULL);

                if (event.getEndTs() != null) {
                    statement.setObject(5, event.getEndTs());
                } else {
                    statement.setNull(5, NULL);
                }
                statement.setString(6, event.getEventId().getId());
                statement.setTimestamp(7, new Timestamp(event.getEventId().getTimestamp()));

                int count = statement.executeUpdate();
                if (count == 0) {
                    throw new SQLException("failed to insert position user " + event.getPositionId());
                }
                conn.commit();
                result = Success.of(event.getPositionId());
                insertNotification(event.getEventId(), event.getClass().getName(), AvroConverter.toJson(event, false), true, null);
            } catch (SQLException e) {
                logger.error("SQLException:", e);
                conn.rollback();
                insertNotification(event.getEventId(), event.getClass().getName(), AvroConverter.toJson(event, false), false, e.getMessage());
                result = Failure.of(new Status(SQL_EXCEPTION, e.getMessage()));
            } catch (Exception e) {
                logger.error("Exception:", e);
                conn.rollback();
                insertNotification(event.getEventId(), event.getClass().getName(), AvroConverter.toJson(event, false), false, e.getMessage());
                result = Failure.of(new Status(GENERIC_EXCEPTION, e.getMessage()));
            }
        } catch (SQLException e) {
            logger.error("SQLException:", e);
            result = Failure.of(new Status(SQL_EXCEPTION, e.getMessage()));
        }
        return result;

    }

    @Override
    public Result<String> updatePositionUser(PositionUserUpdatedEvent event) {
        final String updateGroup = "UPDATE position_user_t SET start_ts = ?, end_ts = ?, update_user = ?, update_ts = ? " +
                "WHERE host_id = ? AND position_id = ? AND user_id = ?";

        Result<String> result = null;
        try (Connection conn = ds.getConnection()) {
            conn.setAutoCommit(false);

            try (PreparedStatement statement = conn.prepareStatement(updateGroup)) {
                if(event.getStartTs() != null)
                    statement.setObject(1, event.getStartTs());
                else
                    statement.setNull(1, NULL);

                if (event.getEndTs() != null) {
                    statement.setObject(2, event.getEndTs());
                } else {
                    statement.setNull(2, NULL);
                }
                statement.setString(3, event.getEventId().getId());
                statement.setTimestamp(4, new Timestamp(event.getEventId().getTimestamp()));
                statement.setString(5, event.getHostId());
                statement.setString(6, event.getPositionId());
                statement.setString(7, event.getUserId());

                int count = statement.executeUpdate();
                if (count == 0) {
                    throw new SQLException("no record is updated for position user " + event.getPositionId());
                }
                conn.commit();
                result = Success.of(event.getPositionId());
                insertNotification(event.getEventId(), event.getClass().getName(), AvroConverter.toJson(event, false), true, null);
            } catch (SQLException e) {
                logger.error("SQLException:", e);
                conn.rollback();
                insertNotification(event.getEventId(), event.getClass().getName(), AvroConverter.toJson(event, false), false, e.getMessage());
                result = Failure.of(new Status(SQL_EXCEPTION, e.getMessage()));
            } catch (Exception e) {
                logger.error("Exception:", e);
                conn.rollback();
                insertNotification(event.getEventId(), event.getClass().getName(), AvroConverter.toJson(event, false), false, e.getMessage());
                result = Failure.of(new Status(GENERIC_EXCEPTION, e.getMessage()));
            }
        } catch (SQLException e) {
            logger.error("SQLException:", e);
            result = Failure.of(new Status(SQL_EXCEPTION, e.getMessage()));
        }
        return result;

    }

    @Override
    public Result<String> deletePositionUser(PositionUserDeletedEvent event) {
        final String deleteGroup = "DELETE from position_user_t WHERE host_id = ? AND position_id = ? AND user_id = ?";
        Result<String> result;
        try (Connection conn = ds.getConnection()) {
            conn.setAutoCommit(false);
            try (PreparedStatement statement = conn.prepareStatement(deleteGroup)) {
                statement.setString(1, event.getHostId());
                statement.setString(2, event.getPositionId());
                statement.setString(3, event.getUserId());

                int count = statement.executeUpdate();
                if (count == 0) {
                    throw new SQLException("no record is deleted for position user " + event.getPositionId());
                }
                conn.commit();
                result = Success.of(event.getEventId().getId());
                insertNotification(event.getEventId(), event.getClass().getName(), AvroConverter.toJson(event, false), true, null);
            } catch (SQLException e) {
                logger.error("SQLException:", e);
                conn.rollback();
                insertNotification(event.getEventId(), event.getClass().getName(), AvroConverter.toJson(event, false), false, e.getMessage());
                result = Failure.of(new Status(SQL_EXCEPTION, e.getMessage()));
            } catch (Exception e) {
                logger.error("Exception:", e);
                conn.rollback();
                insertNotification(event.getEventId(), event.getClass().getName(), AvroConverter.toJson(event, false), false, e.getMessage());
                result = Failure.of(new Status(GENERIC_EXCEPTION, e.getMessage()));
            }
        } catch (SQLException e) {
            logger.error("SQLException:", e);
            result = Failure.of(new Status(SQL_EXCEPTION, e.getMessage()));
        }
        return result;

    }

    @Override
    public Result<String> queryPositionRowFilter(int offset, int limit, String hostId, String PositionId, String apiId, String apiVersion, String endpoint) {
        Result<String> result;
        StringBuilder sqlBuilder = new StringBuilder();
        sqlBuilder.append("SELECT COUNT(*) OVER () AS total, \n" +
                "o.host_id, o.position_id, p.api_id, p.api_version, p.endpoint, p.col_name, p.operator, p.col_value\n" +
                "FROM position_t o, position_row_filter_t p\n" +
                "WHERE o.position_id = p.position_id\n" +
                "AND o.host_id = ?\n");

        List<Object> parameters = new ArrayList<>();
        parameters.add(hostId);

        StringBuilder whereClause = new StringBuilder();

        addCondition(whereClause, parameters, "o.position_id", PositionId);
        addCondition(whereClause, parameters, "p.api_id", apiId);
        addCondition(whereClause, parameters, "p.api_version", apiVersion);
        addCondition(whereClause, parameters, "p.endpoint", endpoint);

        if (whereClause.length() > 0) {
            sqlBuilder.append("AND ").append(whereClause);
        }

        sqlBuilder.append(" ORDER BY o.position_id, p.api_id, p.api_version, p.endpoint\n" +
                "LIMIT ? OFFSET ?");

        parameters.add(limit);
        parameters.add(offset);

        String sql = sqlBuilder.toString();
        if(logger.isTraceEnabled()) logger.trace("queryPositionRowFilter sql: {}", sql);
        int total = 0;
        List<Map<String, Object>> positionRowFilters = new ArrayList<>();

        try (final Connection conn = ds.getConnection(); PreparedStatement preparedStatement = conn.prepareStatement(sql)) {
            for (int i = 0; i < parameters.size(); i++) {
                preparedStatement.setObject(i + 1, parameters.get(i));
            }
            boolean isFirstRow = true;
            try (ResultSet resultSet = preparedStatement.executeQuery()) {
                while (resultSet.next()) {
                    Map<String, Object> map = new HashMap<>();
                    // only get the total once as it is the same for all rows.
                    if (isFirstRow) {
                        total = resultSet.getInt("total");
                        isFirstRow = false;
                    }
                    map.put("hostId", resultSet.getString("host_id"));
                    map.put("positionId", resultSet.getString("position_id"));
                    map.put("apiId", resultSet.getString("api_id"));
                    map.put("apiVersion", resultSet.getString("api_version"));
                    map.put("endpoint", resultSet.getString("endpoint"));
                    map.put("colName", resultSet.getString("col_name"));
                    map.put("operator", resultSet.getString("operator"));
                    map.put("colValue", resultSet.getString("col_value"));
                    positionRowFilters.add(map);
                }
            }
            Map<String, Object> resultMap = new HashMap<>();
            resultMap.put("total", total);
            resultMap.put("positionRowFilters", positionRowFilters);
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
    public Result<String> createPositionRowFilter(PositionRowFilterCreatedEvent event) {
        final String insertGroup = "INSERT INTO position_row_filter_t (host_id, position_id, api_id, api_version, endpoint, col_name, operator, col_value, update_user, update_ts) " +
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";

        Result<String> result = null;
        try (Connection conn = ds.getConnection()) {
            conn.setAutoCommit(false);
            // no duplicate record, insert the user into database and write a success notification.
            try (PreparedStatement statement = conn.prepareStatement(insertGroup)) {
                statement.setString(1, event.getHostId());
                statement.setString(2, event.getPositionId());
                statement.setString(3, event.getApiId());
                statement.setString(4, event.getApiVersion());
                statement.setString(5, event.getEndpoint());
                statement.setString(6, event.getColName());
                statement.setString(7, event.getOperator());
                statement.setString(8, event.getColValue());
                statement.setString(9, event.getEventId().getId());
                statement.setTimestamp(10, new Timestamp(event.getEventId().getTimestamp()));

                int count = statement.executeUpdate();
                if (count == 0) {
                    throw new SQLException("failed to insert position row filter " + event.getPositionId());
                }
                conn.commit();
                result = Success.of(event.getPositionId());
                insertNotification(event.getEventId(), event.getClass().getName(), AvroConverter.toJson(event, false), true, null);
            } catch (SQLException e) {
                logger.error("SQLException:", e);
                conn.rollback();
                insertNotification(event.getEventId(), event.getClass().getName(), AvroConverter.toJson(event, false), false, e.getMessage());
                result = Failure.of(new Status(SQL_EXCEPTION, e.getMessage()));
            } catch (Exception e) {
                logger.error("Exception:", e);
                conn.rollback();
                insertNotification(event.getEventId(), event.getClass().getName(), AvroConverter.toJson(event, false), false, e.getMessage());
                result = Failure.of(new Status(GENERIC_EXCEPTION, e.getMessage()));
            }
        } catch (SQLException e) {
            logger.error("SQLException:", e);
            result = Failure.of(new Status(SQL_EXCEPTION, e.getMessage()));
        }
        return result;
    }

    @Override
    public Result<String> updatePositionRowFilter(PositionRowFilterUpdatedEvent event) {
        final String updateGroup = "UPDATE position_row_filter_t SET operator = ?, col_value = ?, update_user = ?, update_ts = ? " +
                "WHERE host_id = ? AND position_id = ? AND api_id = ? AND api_version = ? AND endpoint = ? AND col_name = ?";

        Result<String> result = null;
        try (Connection conn = ds.getConnection()) {
            conn.setAutoCommit(false);

            try (PreparedStatement statement = conn.prepareStatement(updateGroup)) {
                statement.setString(1, event.getOperator());
                statement.setString(2, event.getColValue());
                statement.setString(3, event.getEventId().getId());
                statement.setTimestamp(4, new Timestamp(event.getEventId().getTimestamp()));
                statement.setString(5, event.getHostId());
                statement.setString(6, event.getPositionId());
                statement.setString(7, event.getApiId());
                statement.setString(8, event.getApiVersion());
                statement.setString(9, event.getEndpoint());
                statement.setString(10, event.getColName());

                int count = statement.executeUpdate();
                if (count == 0) {
                    throw new SQLException("no record is updated for position row filter " + event.getPositionId());
                }
                conn.commit();
                result = Success.of(event.getPositionId());
                insertNotification(event.getEventId(), event.getClass().getName(), AvroConverter.toJson(event, false), true, null);
            } catch (SQLException e) {
                logger.error("SQLException:", e);
                conn.rollback();
                insertNotification(event.getEventId(), event.getClass().getName(), AvroConverter.toJson(event, false), false, e.getMessage());
                result = Failure.of(new Status(SQL_EXCEPTION, e.getMessage()));
            } catch (Exception e) {
                logger.error("Exception:", e);
                conn.rollback();
                insertNotification(event.getEventId(), event.getClass().getName(), AvroConverter.toJson(event, false), false, e.getMessage());
                result = Failure.of(new Status(GENERIC_EXCEPTION, e.getMessage()));
            }
        } catch (SQLException e) {
            logger.error("SQLException:", e);
            result = Failure.of(new Status(SQL_EXCEPTION, e.getMessage()));
        }
        return result;
    }

    @Override
    public Result<String> deletePositionRowFilter(PositionRowFilterDeletedEvent event) {
        final String deleteGroup = "DELETE from position_row_filter_t WHERE host_id = ? AND position_id = ? AND api_id = ? AND api_version = ? AND endpoint = ? AND col_name = ?";
        Result<String> result;
        try (Connection conn = ds.getConnection()) {
            conn.setAutoCommit(false);
            try (PreparedStatement statement = conn.prepareStatement(deleteGroup)) {
                statement.setString(1, event.getHostId());
                statement.setString(2, event.getPositionId());
                statement.setString(3, event.getApiId());
                statement.setString(4, event.getApiVersion());
                statement.setString(5, event.getEndpoint());
                statement.setString(6, event.getColName());

                int count = statement.executeUpdate();
                if (count == 0) {
                    throw new SQLException("no record is deleted for position row filter " + event.getPositionId());
                }
                conn.commit();
                result = Success.of(event.getEventId().getId());
                insertNotification(event.getEventId(), event.getClass().getName(), AvroConverter.toJson(event, false), true, null);
            } catch (SQLException e) {
                logger.error("SQLException:", e);
                conn.rollback();
                insertNotification(event.getEventId(), event.getClass().getName(), AvroConverter.toJson(event, false), false, e.getMessage());
                result = Failure.of(new Status(SQL_EXCEPTION, e.getMessage()));
            } catch (Exception e) {
                logger.error("Exception:", e);
                conn.rollback();
                insertNotification(event.getEventId(), event.getClass().getName(), AvroConverter.toJson(event, false), false, e.getMessage());
                result = Failure.of(new Status(GENERIC_EXCEPTION, e.getMessage()));
            }
        } catch (SQLException e) {
            logger.error("SQLException:", e);
            result = Failure.of(new Status(SQL_EXCEPTION, e.getMessage()));
        }
        return result;
    }

    @Override
    public Result<String> queryPositionColFilter(int offset, int limit, String hostId, String PositionId, String apiId, String apiVersion, String endpoint) {
        Result<String> result;
        StringBuilder sqlBuilder = new StringBuilder();
        sqlBuilder.append("SELECT COUNT(*) OVER () AS total, \n" +
                "o.host_id, o.position_id, p.api_id, p.api_version, p.endpoint, p.columns\n" +
                "FROM position_t o, position_col_filter_t p\n" +
                "WHERE o.position_id = p.position_id\n" +
                "AND o.host_id = ?\n");

        List<Object> parameters = new ArrayList<>();
        parameters.add(hostId);

        StringBuilder whereClause = new StringBuilder();

        addCondition(whereClause, parameters, "o.position_id", PositionId);
        addCondition(whereClause, parameters, "p.api_id", apiId);
        addCondition(whereClause, parameters, "p.api_version", apiVersion);
        addCondition(whereClause, parameters, "p.endpoint", endpoint);

        if (whereClause.length() > 0) {
            sqlBuilder.append("AND ").append(whereClause);
        }

        sqlBuilder.append(" ORDER BY o.position_id, p.api_id, p.api_version, p.endpoint\n" +
                "LIMIT ? OFFSET ?");

        parameters.add(limit);
        parameters.add(offset);

        String sql = sqlBuilder.toString();
        if(logger.isTraceEnabled()) logger.trace("queryPositionColFilter sql: {}", sql);
        int total = 0;
        List<Map<String, Object>> positionColFilters = new ArrayList<>();

        try (final Connection conn = ds.getConnection(); PreparedStatement preparedStatement = conn.prepareStatement(sql)) {
            for (int i = 0; i < parameters.size(); i++) {
                preparedStatement.setObject(i + 1, parameters.get(i));
            }
            boolean isFirstRow = true;
            try (ResultSet resultSet = preparedStatement.executeQuery()) {
                while (resultSet.next()) {
                    Map<String, Object> map = new HashMap<>();
                    // only get the total once as it is the same for all rows.
                    if (isFirstRow) {
                        total = resultSet.getInt("total");
                        isFirstRow = false;
                    }
                    map.put("hostId", resultSet.getString("host_id"));
                    map.put("positionId", resultSet.getString("position_id"));
                    map.put("apiId", resultSet.getString("api_id"));
                    map.put("apiVersion", resultSet.getString("api_version"));
                    map.put("endpoint", resultSet.getString("endpoint"));
                    map.put("columns", resultSet.getString("columns"));
                    positionColFilters.add(map);
                }
            }
            Map<String, Object> resultMap = new HashMap<>();
            resultMap.put("total", total);
            resultMap.put("positionColFilters", positionColFilters);
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
    public Result<String> createPositionColFilter(PositionColFilterCreatedEvent event) {
        final String insertGroup = "INSERT INTO position_col_filter_t (host_id, position_id, api_id, api_version, endpoint, columns, update_user, update_ts) " +
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?)";

        Result<String> result = null;
        try (Connection conn = ds.getConnection()) {
            conn.setAutoCommit(false);
            // no duplicate record, insert the user into database and write a success notification.
            try (PreparedStatement statement = conn.prepareStatement(insertGroup)) {
                statement.setString(1, event.getHostId());
                statement.setString(2, event.getPositionId());
                statement.setString(3, event.getApiId());
                statement.setString(4, event.getApiVersion());
                statement.setString(5, event.getEndpoint());
                statement.setString(6, event.getColumns());
                statement.setString(7, event.getEventId().getId());
                statement.setTimestamp(8, new Timestamp(event.getEventId().getTimestamp()));

                int count = statement.executeUpdate();
                if (count == 0) {
                    throw new SQLException("failed to insert position col filter " + event.getPositionId());
                }
                conn.commit();
                result = Success.of(event.getPositionId());
                insertNotification(event.getEventId(), event.getClass().getName(), AvroConverter.toJson(event, false), true, null);
            } catch (SQLException e) {
                logger.error("SQLException:", e);
                conn.rollback();
                insertNotification(event.getEventId(), event.getClass().getName(), AvroConverter.toJson(event, false), false, e.getMessage());
                result = Failure.of(new Status(SQL_EXCEPTION, e.getMessage()));
            } catch (Exception e) {
                logger.error("Exception:", e);
                conn.rollback();
                insertNotification(event.getEventId(), event.getClass().getName(), AvroConverter.toJson(event, false), false, e.getMessage());
                result = Failure.of(new Status(GENERIC_EXCEPTION, e.getMessage()));
            }
        } catch (SQLException e) {
            logger.error("SQLException:", e);
            result = Failure.of(new Status(SQL_EXCEPTION, e.getMessage()));
        }
        return result;
    }

    @Override
    public Result<String> updatePositionColFilter(PositionColFilterUpdatedEvent event) {
        final String updateGroup = "UPDATE position_col_filter_t SET columns = ?, update_user = ?, update_ts = ? " +
                "WHERE host_id = ? AND position_id = ? AND api_id = ? AND api_version = ? AND endpoint = ?";

        Result<String> result = null;
        try (Connection conn = ds.getConnection()) {
            conn.setAutoCommit(false);

            try (PreparedStatement statement = conn.prepareStatement(updateGroup)) {
                statement.setString(1, event.getColumns());
                statement.setString(2, event.getEventId().getId());
                statement.setTimestamp(3, new Timestamp(event.getEventId().getTimestamp()));
                statement.setString(4, event.getHostId());
                statement.setString(5, event.getPositionId());
                statement.setString(6, event.getApiId());
                statement.setString(7, event.getApiVersion());
                statement.setString(8, event.getEndpoint());

                int count = statement.executeUpdate();
                if (count == 0) {
                    throw new SQLException("no record is updated for position col filter " + event.getPositionId());
                }
                conn.commit();
                result = Success.of(event.getPositionId());
                insertNotification(event.getEventId(), event.getClass().getName(), AvroConverter.toJson(event, false), true, null);
            } catch (SQLException e) {
                logger.error("SQLException:", e);
                conn.rollback();
                insertNotification(event.getEventId(), event.getClass().getName(), AvroConverter.toJson(event, false), false, e.getMessage());
                result = Failure.of(new Status(SQL_EXCEPTION, e.getMessage()));
            } catch (Exception e) {
                logger.error("Exception:", e);
                conn.rollback();
                insertNotification(event.getEventId(), event.getClass().getName(), AvroConverter.toJson(event, false), false, e.getMessage());
                result = Failure.of(new Status(GENERIC_EXCEPTION, e.getMessage()));
            }
        } catch (SQLException e) {
            logger.error("SQLException:", e);
            result = Failure.of(new Status(SQL_EXCEPTION, e.getMessage()));
        }
        return result;
    }

    @Override
    public Result<String> deletePositionColFilter(PositionColFilterDeletedEvent event) {
        final String deleteGroup = "DELETE from position_col_filter_t WHERE host_id = ? AND position_id = ? AND api_id = ? AND api_version = ? AND endpoint = ?";
        Result<String> result;
        try (Connection conn = ds.getConnection()) {
            conn.setAutoCommit(false);
            try (PreparedStatement statement = conn.prepareStatement(deleteGroup)) {
                statement.setString(1, event.getHostId());
                statement.setString(2, event.getPositionId());
                statement.setString(3, event.getApiId());
                statement.setString(4, event.getApiVersion());
                statement.setString(5, event.getEndpoint());

                int count = statement.executeUpdate();
                if (count == 0) {
                    throw new SQLException("no record is deleted for position col filter " + event.getPositionId());
                }
                conn.commit();
                result = Success.of(event.getEventId().getId());
                insertNotification(event.getEventId(), event.getClass().getName(), AvroConverter.toJson(event, false), true, null);
            } catch (SQLException e) {
                logger.error("SQLException:", e);
                conn.rollback();
                insertNotification(event.getEventId(), event.getClass().getName(), AvroConverter.toJson(event, false), false, e.getMessage());
                result = Failure.of(new Status(SQL_EXCEPTION, e.getMessage()));
            } catch (Exception e) {
                logger.error("Exception:", e);
                conn.rollback();
                insertNotification(event.getEventId(), event.getClass().getName(), AvroConverter.toJson(event, false), false, e.getMessage());
                result = Failure.of(new Status(GENERIC_EXCEPTION, e.getMessage()));
            }
        } catch (SQLException e) {
            logger.error("SQLException:", e);
            result = Failure.of(new Status(SQL_EXCEPTION, e.getMessage()));
        }
        return result;
    }

    @Override
    public Result<String> createAttribute(AttributeCreatedEvent event) {
        final String insertAttribute = "INSERT INTO attribute_t (host_id, attribute_id, attribute_type, " +
                "attribute_desc, update_user, update_ts) " +
                "VALUES (?, ?, ?, ?, ?, ?)";

        Result<String> result = null;
        try (Connection conn = ds.getConnection()) {
            conn.setAutoCommit(false);
            try (PreparedStatement statement = conn.prepareStatement(insertAttribute)) {
                statement.setString(1, event.getHostId());
                statement.setString(2, event.getAttributeId());

                if(event.getAttributeType() != null)
                    statement.setString(3, event.getAttributeType());
                else
                    statement.setNull(3, NULL);

                if (event.getAttributeDesc() != null)
                    statement.setString(4, event.getAttributeDesc());
                else
                    statement.setNull(4, NULL);

                statement.setString(5, event.getEventId().getId());
                statement.setTimestamp(6, new Timestamp(event.getEventId().getTimestamp()));

                int count = statement.executeUpdate();
                if (count == 0) {
                    throw new SQLException("failed to insert attribute " + event.getAttributeId());
                }
                conn.commit();
                result = Success.of(event.getAttributeId());
                insertNotification(event.getEventId(), event.getClass().getName(), AvroConverter.toJson(event, false), true, null);
            } catch (SQLException e) {
                logger.error("SQLException:", e);
                conn.rollback();
                insertNotification(event.getEventId(), event.getClass().getName(), AvroConverter.toJson(event, false), false, e.getMessage());
                result = Failure.of(new Status(SQL_EXCEPTION, e.getMessage()));
            } catch (Exception e) {
                logger.error("Exception:", e);
                conn.rollback();
                insertNotification(event.getEventId(), event.getClass().getName(), AvroConverter.toJson(event, false), false, e.getMessage());
                result = Failure.of(new Status(GENERIC_EXCEPTION, e.getMessage()));
            }
        } catch (SQLException e) {
            logger.error("SQLException:", e);
            result = Failure.of(new Status(SQL_EXCEPTION, e.getMessage()));
        }
        return result;
    }

    @Override
    public Result<String> updateAttribute(AttributeUpdatedEvent event) {
        final String updateAttribute = "UPDATE attribute_t SET attribute_desc = ?, attribute_type = ?," +
                "update_user = ?, update_ts = ? " +
                "WHERE host_id = ? AND attribute_id = ?";

        Result<String> result = null;
        try (Connection conn = ds.getConnection()) {
            conn.setAutoCommit(false);
            try (PreparedStatement statement = conn.prepareStatement(updateAttribute)) {
                if(event.getAttributeDesc() != null) {
                    statement.setString(1, event.getAttributeDesc());
                } else {
                    statement.setNull(1, NULL);
                }
                if(event.getAttributeType() != null) {
                    statement.setString(2, event.getAttributeType());
                } else {
                    statement.setNull(2, NULL);
                }
                statement.setString(3, event.getEventId().getId());
                statement.setTimestamp(4, new Timestamp(event.getEventId().getTimestamp()));

                statement.setString(5, event.getHostId());
                statement.setString(6, event.getAttributeId());

                int count = statement.executeUpdate();
                if (count == 0) {
                    throw new SQLException("no record is updated for attribute " + event.getAttributeId());
                }
                conn.commit();
                result = Success.of(event.getAttributeId());
                insertNotification(event.getEventId(), event.getClass().getName(), AvroConverter.toJson(event, false), true, null);
            } catch (SQLException e) {
                logger.error("SQLException:", e);
                conn.rollback();
                insertNotification(event.getEventId(), event.getClass().getName(), AvroConverter.toJson(event, false), false, e.getMessage());
                result = Failure.of(new Status(SQL_EXCEPTION, e.getMessage()));
            } catch (Exception e) {
                logger.error("Exception:", e);
                conn.rollback();
                insertNotification(event.getEventId(), event.getClass().getName(), AvroConverter.toJson(event, false), false, e.getMessage());
                result = Failure.of(new Status(GENERIC_EXCEPTION, e.getMessage()));
            }
        } catch (SQLException e) {
            logger.error("SQLException:", e);
            result = Failure.of(new Status(SQL_EXCEPTION, e.getMessage()));
        }
        return result;

    }

    @Override
    public Result<String> deleteAttribute(AttributeDeletedEvent event) {
        final String deleteGroup = "DELETE from attribute_t WHERE host_id = ? AND attribute_id = ?";
        Result<String> result;
        try (Connection conn = ds.getConnection()) {
            conn.setAutoCommit(false);
            try (PreparedStatement statement = conn.prepareStatement(deleteGroup)) {
                statement.setString(1, event.getHostId());
                statement.setString(2, event.getAttributeId());
                int count = statement.executeUpdate();
                if (count == 0) {
                    throw new SQLException("no record is deleted for attribute " + event.getAttributeId());
                }
                conn.commit();
                result = Success.of(event.getEventId().getId());
                insertNotification(event.getEventId(), event.getClass().getName(), AvroConverter.toJson(event, false), true, null);
            } catch (SQLException e) {
                logger.error("SQLException:", e);
                conn.rollback();
                insertNotification(event.getEventId(), event.getClass().getName(), AvroConverter.toJson(event, false), false, e.getMessage());
                result = Failure.of(new Status(SQL_EXCEPTION, e.getMessage()));
            } catch (Exception e) {
                logger.error("Exception:", e);
                conn.rollback();
                insertNotification(event.getEventId(), event.getClass().getName(), AvroConverter.toJson(event, false), false, e.getMessage());
                result = Failure.of(new Status(GENERIC_EXCEPTION, e.getMessage()));
            }
        } catch (SQLException e) {
            logger.error("SQLException:", e);
            result = Failure.of(new Status(SQL_EXCEPTION, e.getMessage()));
        }
        return result;
    }

    @Override
    public Result<String> queryAttribute(int offset, int limit, String hostId, String attributeId, String attributeType, String attributeDesc) {
        Result<String> result;
        StringBuilder sqlBuilder = new StringBuilder();
        sqlBuilder.append("SELECT COUNT(*) OVER () AS total, host_id, attribute_id, attribute_type, attribute_desc, update_user, update_ts " +
                "FROM attribute_t " +
                "WHERE host_id = ?\n");

        List<Object> parameters = new ArrayList<>();
        parameters.add(hostId);

        StringBuilder whereClause = new StringBuilder();
        addCondition(whereClause, parameters, "attribute_id", attributeId);
        addCondition(whereClause, parameters, "attribute_type", attributeType);
        addCondition(whereClause, parameters, "attribute_desc", attributeDesc);

        if (whereClause.length() > 0) {
            sqlBuilder.append("AND ").append(whereClause);
        }
        sqlBuilder.append(" ORDER BY attribute_id\n" +
                "LIMIT ? OFFSET ?");

        parameters.add(limit);
        parameters.add(offset);

        String sql = sqlBuilder.toString();

        int total = 0;
        List<Map<String, Object>> attributes = new ArrayList<>();
        try (final Connection conn = ds.getConnection(); PreparedStatement preparedStatement = conn.prepareStatement(sql)) {
            for (int i = 0; i < parameters.size(); i++) {
                preparedStatement.setObject(i + 1, parameters.get(i));
            }
            boolean isFirstRow = true;
            try (ResultSet resultSet = preparedStatement.executeQuery()) {
                while (resultSet.next()) {
                    Map<String, Object> map = new HashMap<>();
                    // only get the total once as it is the same for all rows.
                    if (isFirstRow) {
                        total = resultSet.getInt("total");
                        isFirstRow = false;
                    }
                    map.put("hostId", resultSet.getString("host_id"));
                    map.put("attributeId", resultSet.getString("attribute_id"));
                    map.put("attributeType", resultSet.getString("attribute_type"));
                    map.put("attributeDesc", resultSet.getString("attribute_desc"));
                    map.put("updateUser", resultSet.getString("update_user"));
                    map.put("updateTs", resultSet.getTimestamp("update_ts"));
                    attributes.add(map);
                }
            }

            Map<String, Object> resultMap = new HashMap<>();
            resultMap.put("total", total);
            resultMap.put("attributes", attributes);
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
    public Result<String> queryAttributeLabel(String hostId) {
        final String sql = "SELECT attribute_id from attribute_t WHERE host_id = ?";
        Result<String> result;
        try (final Connection conn = ds.getConnection()) {
            List<Map<String, Object>> list = new ArrayList<>();
            try (PreparedStatement statement = conn.prepareStatement(sql)) {
                statement.setString(1, hostId);
                try (ResultSet resultSet = statement.executeQuery()) {
                    while (resultSet.next()) {
                        Map<String, Object> map = new HashMap<>();
                        String id = resultSet.getString("attribute_id");
                        map.put("id", id);
                        map.put("label", id);
                        list.add(map);
                    }
                }
            }
            if (list.isEmpty())
                result = Failure.of(new Status(OBJECT_NOT_FOUND, "attribute", hostId));
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
    public Result<String> queryAttributePermission(int offset, int limit, String hostId, String attributeId, String attributeType, String attributeValue, String apiId, String apiVersion, String endpoint) {
        Result<String> result;
        StringBuilder sqlBuilder = new StringBuilder();
        sqlBuilder.append("SELECT COUNT(*) OVER () AS total, \n" +
                "a.host_id, a.attribute_id, a.attribute_type, p.attribute_value, " +
                "p.api_id, p.api_version, p.endpoint\n" +
                "FROM attribute_t a, attribute_permission_t p\n" +
                "WHERE a.attribute_id = p.attribute_id\n" +
                "AND a.host_id = ?\n");


        List<Object> parameters = new ArrayList<>();
        parameters.add(hostId);


        StringBuilder whereClause = new StringBuilder();

        addCondition(whereClause, parameters, "a.attribute_id", attributeId);
        addCondition(whereClause, parameters, "a.attribute_type", attributeType);
        addCondition(whereClause, parameters, "a.attribute_value", attributeValue);
        addCondition(whereClause, parameters, "p.api_id", apiId);
        addCondition(whereClause, parameters, "p.api_version", apiVersion);
        addCondition(whereClause, parameters, "p.endpoint", endpoint);

        if (whereClause.length() > 0) {
            sqlBuilder.append("AND ").append(whereClause);
        }

        sqlBuilder.append(" ORDER BY a.attribute_id, p.api_id, p.api_version, p.endpoint\n" +
                "LIMIT ? OFFSET ?");

        parameters.add(limit);
        parameters.add(offset);

        String sql = sqlBuilder.toString();
        if(logger.isTraceEnabled()) logger.trace("queryAttributePermission sql: {}", sql);
        int total = 0;
        List<Map<String, Object>> attributePermissions = new ArrayList<>();


        try (final Connection conn = ds.getConnection(); PreparedStatement preparedStatement = conn.prepareStatement(sql)) {
            for (int i = 0; i < parameters.size(); i++) {
                preparedStatement.setObject(i + 1, parameters.get(i));
            }
            boolean isFirstRow = true;
            try (ResultSet resultSet = preparedStatement.executeQuery()) {
                while (resultSet.next()) {
                    Map<String, Object> map = new HashMap<>();
                    // only get the total once as it is the same for all rows.
                    if (isFirstRow) {
                        total = resultSet.getInt("total");
                        isFirstRow = false;
                    }
                    map.put("hostId", resultSet.getString("host_id"));
                    map.put("attributeId", resultSet.getString("attribute_id"));
                    map.put("attributeType", resultSet.getString("attribute_type"));
                    map.put("attributeValue", resultSet.getString("attribute_value"));
                    map.put("apiId", resultSet.getString("api_id"));
                    map.put("apiVersion", resultSet.getString("api_version"));
                    map.put("endpoint", resultSet.getString("endpoint"));
                    attributePermissions.add(map);
                }
            }
            Map<String, Object> resultMap = new HashMap<>();
            resultMap.put("total", total);
            resultMap.put("attributePermissions", attributePermissions);
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
    public Result<String> queryAttributeUser(int offset, int limit, String hostId, String attributeId, String attributeType, String attributeValue, String userId, String entityId, String email, String firstName, String lastName, String userType) {
        Result<String> result;
        StringBuilder sqlBuilder = new StringBuilder();
        sqlBuilder.append("SELECT COUNT(*) OVER () AS total, \n" +
                "a.host_id, a.attribute_id, at.attribute_type, a.attribute_value, \n" +
                "a.start_ts, a.end_ts, \n" +
                "u.user_id, u.email, u.user_type, \n" +
                "CASE\n" +
                "    WHEN u.user_type = 'C' THEN c.customer_id\n" +
                "    WHEN u.user_type = 'E' THEN e.employee_id\n" +
                "    ELSE NULL -- Handle other cases if needed\n" +
                "END AS entity_id,\n" +
                "e.manager_id, u.first_name, u.last_name\n" +
                "FROM user_t u\n" +
                "LEFT JOIN\n" +
                "    customer_t c ON u.user_id = c.user_id AND u.user_type = 'C'\n" +
                "LEFT JOIN\n" +
                "    employee_t e ON u.user_id = e.user_id AND u.user_type = 'E'\n" +
                "INNER JOIN\n" +
                "    attribute_user_t a ON a.user_id = u.user_id\n" +
                "INNER JOIN\n" +
                "    attribute_t at ON at.attribute_id = a.attribute_id\n" +
                "AND a.host_id = ?\n");

        List<Object> parameters = new ArrayList<>();
        parameters.add(hostId);


        StringBuilder whereClause = new StringBuilder();

        addCondition(whereClause, parameters, "a.attribute_id", attributeId);
        addCondition(whereClause, parameters, "a.attribute_type", attributeType);
        addCondition(whereClause, parameters, "a.attribute_value", attributeValue);
        addCondition(whereClause, parameters, "u.user_id", userId);
        addCondition(whereClause, parameters, "entity_id", entityId);
        addCondition(whereClause, parameters, "u.email", email);
        addCondition(whereClause, parameters, "u.first_name", firstName);
        addCondition(whereClause, parameters, "u.last_name", lastName);
        addCondition(whereClause, parameters, "u.user_type", userType);

        if (whereClause.length() > 0) {
            sqlBuilder.append("AND ").append(whereClause);
        }

        sqlBuilder.append(" ORDER BY a.attribute_id, u.user_id\n" +
                "LIMIT ? OFFSET ?");

        parameters.add(limit);
        parameters.add(offset);

        String sql = sqlBuilder.toString();
        if(logger.isTraceEnabled()) logger.trace("queryGroupUser sql: {}", sql);
        int total = 0;
        List<Map<String, Object>> attributeUsers = new ArrayList<>();


        try (final Connection conn = ds.getConnection(); PreparedStatement preparedStatement = conn.prepareStatement(sql)) {
            for (int i = 0; i < parameters.size(); i++) {
                preparedStatement.setObject(i + 1, parameters.get(i));
            }
            boolean isFirstRow = true;
            try (ResultSet resultSet = preparedStatement.executeQuery()) {
                while (resultSet.next()) {
                    Map<String, Object> map = new HashMap<>();
                    // only get the total once as it is the same for all rows.
                    if (isFirstRow) {
                        total = resultSet.getInt("total");
                        isFirstRow = false;
                    }
                    map.put("hostId", resultSet.getString("host_id"));
                    map.put("attributeId", resultSet.getString("attribute_id"));
                    map.put("attributeType", resultSet.getString("attribute_type"));
                    map.put("attributeValue", resultSet.getString("attribute_value"));
                    map.put("startTs", resultSet.getDate("start_ts"));
                    map.put("endTs", resultSet.getString("end_ts"));
                    map.put("userId", resultSet.getString("user_id"));
                    map.put("entityId", resultSet.getString("entity_id"));
                    map.put("email", resultSet.getString("email"));
                    map.put("firstName", resultSet.getString("first_name"));
                    map.put("lastName", resultSet.getString("last_name"));
                    map.put("userType", resultSet.getString("user_type"));
                    attributeUsers.add(map);
                }
            }
            Map<String, Object> resultMap = new HashMap<>();
            resultMap.put("total", total);
            resultMap.put("attributeUsers", attributeUsers);
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
    public Result<String> createAttributePermission(AttributePermissionCreatedEvent event) {
        final String insertGroup = "INSERT INTO attribute_permission_t (host_id, attribute_id, attribute_value, api_id, api_version, endpoint, update_user, update_ts) " +
                "VALUES (?, ?, ?, ?, ?, ?,  ?, ?)";

        Result<String> result = null;
        try (Connection conn = ds.getConnection()) {
            conn.setAutoCommit(false);
            // no duplicate record, insert the user into database and write a success notification.
            try (PreparedStatement statement = conn.prepareStatement(insertGroup)) {
                statement.setString(1, event.getHostId());
                statement.setString(2, event.getAttributeId());
                statement.setString(3, event.getAttributeValue());
                statement.setString(4, event.getApiId());
                statement.setString(5, event.getApiVersion());
                statement.setString(6, event.getEndpoint());
                statement.setString(7, event.getEventId().getId());
                statement.setTimestamp(8, new Timestamp(event.getEventId().getTimestamp()));

                int count = statement.executeUpdate();
                if (count == 0) {
                    throw new SQLException("failed to insert attribute permission " + event.getAttributeId());
                }
                conn.commit();
                result = Success.of(event.getAttributeId());
                insertNotification(event.getEventId(), event.getClass().getName(), AvroConverter.toJson(event, false), true, null);
            } catch (SQLException e) {
                logger.error("SQLException:", e);
                conn.rollback();
                insertNotification(event.getEventId(), event.getClass().getName(), AvroConverter.toJson(event, false), false, e.getMessage());
                result = Failure.of(new Status(SQL_EXCEPTION, e.getMessage()));
            } catch (Exception e) {
                logger.error("Exception:", e);
                conn.rollback();
                insertNotification(event.getEventId(), event.getClass().getName(), AvroConverter.toJson(event, false), false, e.getMessage());
                result = Failure.of(new Status(GENERIC_EXCEPTION, e.getMessage()));
            }
        } catch (SQLException e) {
            logger.error("SQLException:", e);
            result = Failure.of(new Status(SQL_EXCEPTION, e.getMessage()));
        }
        return result;
    }

    @Override
    public Result<String> updateAttributePermission(AttributePermissionUpdatedEvent event) {
        final String updateGroup = "UPDATE attribute_permission_t SET attribute_value = ?, update_user = ?, update_ts = ? " +
                "WHERE host_id = ? AND attribute_id = ? AND api_id = ? AND api_version = ? AND endpoint = ?";

        Result<String> result = null;
        try (Connection conn = ds.getConnection()) {
            conn.setAutoCommit(false);
            try (PreparedStatement statement = conn.prepareStatement(updateGroup)) {
                statement.setString(1, event.getAttributeValue());
                statement.setString(2, event.getEventId().getId());
                statement.setTimestamp(3, new Timestamp(event.getEventId().getTimestamp()));

                statement.setString(4, event.getHostId());
                statement.setString(5, event.getAttributeId());
                statement.setString(6, event.getApiId());
                statement.setString(7, event.getApiVersion());
                statement.setString(8, event.getEndpoint());

                int count = statement.executeUpdate();
                if (count == 0) {
                    throw new SQLException("no record is updated for attribute permission " + event.getAttributeId());
                }
                conn.commit();
                result = Success.of(event.getAttributeId());
                insertNotification(event.getEventId(), event.getClass().getName(), AvroConverter.toJson(event, false), true, null);
            } catch (SQLException e) {
                logger.error("SQLException:", e);
                conn.rollback();
                insertNotification(event.getEventId(), event.getClass().getName(), AvroConverter.toJson(event, false), false, e.getMessage());
                result = Failure.of(new Status(SQL_EXCEPTION, e.getMessage()));
            } catch (Exception e) {
                logger.error("Exception:", e);
                conn.rollback();
                insertNotification(event.getEventId(), event.getClass().getName(), AvroConverter.toJson(event, false), false, e.getMessage());
                result = Failure.of(new Status(GENERIC_EXCEPTION, e.getMessage()));
            }
        } catch (SQLException e) {
            logger.error("SQLException:", e);
            result = Failure.of(new Status(SQL_EXCEPTION, e.getMessage()));
        }
        return result;

    }

    @Override
    public Result<String> deleteAttributePermission(AttributePermissionDeletedEvent event) {
        final String deleteGroup = "DELETE from attribute_permission_t WHERE host_id = ? AND attribute_id = ? " +
                "AND api_id = ? AND api_version = ? AND endpoint = ?";
        Result<String> result;
        try (Connection conn = ds.getConnection()) {
            conn.setAutoCommit(false);
            try (PreparedStatement statement = conn.prepareStatement(deleteGroup)) {
                statement.setString(1, event.getHostId());
                statement.setString(2, event.getAttributeId());
                statement.setString(3, event.getApiId());
                statement.setString(4, event.getApiVersion());
                statement.setString(5, event.getEndpoint());

                int count = statement.executeUpdate();
                if (count == 0) {
                    throw new SQLException("no record is deleted for attribute permission " + event.getAttributeId());
                }
                conn.commit();
                result = Success.of(event.getEventId().getId());
                insertNotification(event.getEventId(), event.getClass().getName(), AvroConverter.toJson(event, false), true, null);
            } catch (SQLException e) {
                logger.error("SQLException:", e);
                conn.rollback();
                insertNotification(event.getEventId(), event.getClass().getName(), AvroConverter.toJson(event, false), false, e.getMessage());
                result = Failure.of(new Status(SQL_EXCEPTION, e.getMessage()));
            } catch (Exception e) {
                logger.error("Exception:", e);
                conn.rollback();
                insertNotification(event.getEventId(), event.getClass().getName(), AvroConverter.toJson(event, false), false, e.getMessage());
                result = Failure.of(new Status(GENERIC_EXCEPTION, e.getMessage()));
            }
        } catch (SQLException e) {
            logger.error("SQLException:", e);
            result = Failure.of(new Status(SQL_EXCEPTION, e.getMessage()));
        }
        return result;

    }

    @Override
    public Result<String> createAttributeUser(AttributeUserCreatedEvent event) {
        final String insertGroup = "INSERT INTO attribute_user_t (host_id, attribute_id, attribute_value, user_id, start_ts, end_ts, update_user, update_ts) " +
                "VALUES (?, ?, ?, ?, ?,  ?, ?, ?)";

        Result<String> result = null;
        try (Connection conn = ds.getConnection()) {
            conn.setAutoCommit(false);
            // no duplicate record, insert the user into database and write a success notification.
            try (PreparedStatement statement = conn.prepareStatement(insertGroup)) {
                statement.setString(1, event.getHostId());
                statement.setString(2, event.getAttributeId());
                statement.setString(3, event.getAttributeValue());

                if(event.getStartTs() != null)
                    statement.setObject(4, event.getStartTs());
                else
                    statement.setNull(4, NULL);

                if (event.getEndTs() != null) {
                    statement.setObject(5, event.getEndTs());
                } else {
                    statement.setNull(5, NULL);
                }
                statement.setString(6, event.getEventId().getId());
                statement.setTimestamp(7, new Timestamp(event.getEventId().getTimestamp()));

                int count = statement.executeUpdate();
                if (count == 0) {
                    throw new SQLException("failed to insert attribute user " + event.getAttributeId());
                }
                conn.commit();
                result = Success.of(event.getAttributeId());
                insertNotification(event.getEventId(), event.getClass().getName(), AvroConverter.toJson(event, false), true, null);
            } catch (SQLException e) {
                logger.error("SQLException:", e);
                conn.rollback();
                insertNotification(event.getEventId(), event.getClass().getName(), AvroConverter.toJson(event, false), false, e.getMessage());
                result = Failure.of(new Status(SQL_EXCEPTION, e.getMessage()));
            } catch (Exception e) {
                logger.error("Exception:", e);
                conn.rollback();
                insertNotification(event.getEventId(), event.getClass().getName(), AvroConverter.toJson(event, false), false, e.getMessage());
                result = Failure.of(new Status(GENERIC_EXCEPTION, e.getMessage()));
            }
        } catch (SQLException e) {
            logger.error("SQLException:", e);
            result = Failure.of(new Status(SQL_EXCEPTION, e.getMessage()));
        }
        return result;

    }

    @Override
    public Result<String> updateAttributeUser(AttributeUserUpdatedEvent event) {
        final String updateGroup = "UPDATE attribute_user_t SET attribute_value = ?, start_ts = ?, end_ts = ?, update_user = ?, update_ts = ? " +
                "WHERE host_id = ? AND attribute_id = ? AND user_id = ?";

        Result<String> result = null;
        try (Connection conn = ds.getConnection()) {
            conn.setAutoCommit(false);

            try (PreparedStatement statement = conn.prepareStatement(updateGroup)) {
                statement.setString(1, event.getAttributeValue());
                if(event.getStartTs() != null)
                    statement.setObject(2, event.getStartTs());
                else
                    statement.setNull(2, NULL);

                if (event.getEndTs() != null) {
                    statement.setObject(3, event.getEndTs());
                } else {
                    statement.setNull(3, NULL);
                }
                statement.setString(4, event.getEventId().getId());
                statement.setTimestamp(5, new Timestamp(event.getEventId().getTimestamp()));
                statement.setString(6, event.getHostId());
                statement.setString(7, event.getAttributeId());
                statement.setString(8, event.getUserId());

                int count = statement.executeUpdate();
                if (count == 0) {
                    throw new SQLException("no record is updated for attribute user " + event.getAttributeId());
                }
                conn.commit();
                result = Success.of(event.getAttributeId());
                insertNotification(event.getEventId(), event.getClass().getName(), AvroConverter.toJson(event, false), true, null);
            } catch (SQLException e) {
                logger.error("SQLException:", e);
                conn.rollback();
                insertNotification(event.getEventId(), event.getClass().getName(), AvroConverter.toJson(event, false), false, e.getMessage());
                result = Failure.of(new Status(SQL_EXCEPTION, e.getMessage()));
            } catch (Exception e) {
                logger.error("Exception:", e);
                conn.rollback();
                insertNotification(event.getEventId(), event.getClass().getName(), AvroConverter.toJson(event, false), false, e.getMessage());
                result = Failure.of(new Status(GENERIC_EXCEPTION, e.getMessage()));
            }
        } catch (SQLException e) {
            logger.error("SQLException:", e);
            result = Failure.of(new Status(SQL_EXCEPTION, e.getMessage()));
        }
        return result;

    }

    @Override
    public Result<String> deleteAttributeUser(AttributeUserDeletedEvent event) {
        final String deleteGroup = "DELETE from attribute_user_t WHERE host_id = ? AND attribute_id = ? AND user_id = ?";
        Result<String> result;
        try (Connection conn = ds.getConnection()) {
            conn.setAutoCommit(false);
            try (PreparedStatement statement = conn.prepareStatement(deleteGroup)) {
                statement.setString(1, event.getHostId());
                statement.setString(2, event.getAttributeId());
                statement.setString(3, event.getUserId());

                int count = statement.executeUpdate();
                if (count == 0) {
                    throw new SQLException("no record is deleted for attribute user " + event.getAttributeId());
                }
                conn.commit();
                result = Success.of(event.getEventId().getId());
                insertNotification(event.getEventId(), event.getClass().getName(), AvroConverter.toJson(event, false), true, null);
            } catch (SQLException e) {
                logger.error("SQLException:", e);
                conn.rollback();
                insertNotification(event.getEventId(), event.getClass().getName(), AvroConverter.toJson(event, false), false, e.getMessage());
                result = Failure.of(new Status(SQL_EXCEPTION, e.getMessage()));
            } catch (Exception e) {
                logger.error("Exception:", e);
                conn.rollback();
                insertNotification(event.getEventId(), event.getClass().getName(), AvroConverter.toJson(event, false), false, e.getMessage());
                result = Failure.of(new Status(GENERIC_EXCEPTION, e.getMessage()));
            }
        } catch (SQLException e) {
            logger.error("SQLException:", e);
            result = Failure.of(new Status(SQL_EXCEPTION, e.getMessage()));
        }
        return result;

    }

    @Override
    public Result<String> queryAttributeRowFilter(int offset, int limit, String hostId, String attributeId, String attributeValue, String apiId, String apiVersion, String endpoint) {
        Result<String> result;
        StringBuilder sqlBuilder = new StringBuilder();
        sqlBuilder.append("SELECT COUNT(*) OVER () AS total, \n" +
                "a.host_id, a.attribute_id, at.attribute_type, p.attribute_value, " +
                "p.api_id, p.api_version, p.endpoint, p.col_name, p.operator, p.col_value\n" +
                "FROM attribute_t a, attribute_row_filter_t p, attribute_user_t at\n" +
                "WHERE a.attribute_id = p.attribute_id\n" +
                "AND a.host_id = ?\n");

        List<Object> parameters = new ArrayList<>();
        parameters.add(hostId);

        StringBuilder whereClause = new StringBuilder();

        addCondition(whereClause, parameters, "a.attribute_id", attributeId);
        addCondition(whereClause, parameters, "p.attribute_value", attributeValue);
        addCondition(whereClause, parameters, "p.api_id", apiId);
        addCondition(whereClause, parameters, "p.api_version", apiVersion);
        addCondition(whereClause, parameters, "p.endpoint", endpoint);

        if (whereClause.length() > 0) {
            sqlBuilder.append("AND ").append(whereClause);
        }

        sqlBuilder.append(" ORDER BY a.attribute_id, p.api_id, p.api_version, p.endpoint\n" +
                "LIMIT ? OFFSET ?");

        parameters.add(limit);
        parameters.add(offset);

        String sql = sqlBuilder.toString();
        if(logger.isTraceEnabled()) logger.trace("queryAttributeRowFilter sql: {}", sql);
        int total = 0;
        List<Map<String, Object>> attributeRowFilters = new ArrayList<>();

        try (final Connection conn = ds.getConnection(); PreparedStatement preparedStatement = conn.prepareStatement(sql)) {
            for (int i = 0; i < parameters.size(); i++) {
                preparedStatement.setObject(i + 1, parameters.get(i));
            }
            boolean isFirstRow = true;
            try (ResultSet resultSet = preparedStatement.executeQuery()) {
                while (resultSet.next()) {
                    Map<String, Object> map = new HashMap<>();
                    // only get the total once as it is the same for all rows.
                    if (isFirstRow) {
                        total = resultSet.getInt("total");
                        isFirstRow = false;
                    }
                    map.put("hostId", resultSet.getString("host_id"));
                    map.put("attributeId", resultSet.getString("attribute_id"));
                    map.put("attributeType", resultSet.getString("attribute_type"));
                    map.put("attributeValue", resultSet.getString("attribute_value"));
                    map.put("apiId", resultSet.getString("api_id"));
                    map.put("apiVersion", resultSet.getString("api_version"));
                    map.put("endpoint", resultSet.getString("endpoint"));
                    map.put("colName", resultSet.getString("col_name"));
                    map.put("operator", resultSet.getString("operator"));
                    map.put("colValue", resultSet.getString("col_value"));
                    attributeRowFilters.add(map);
                }
            }
            Map<String, Object> resultMap = new HashMap<>();
            resultMap.put("total", total);
            resultMap.put("attributeRowFilters", attributeRowFilters);
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
    public Result<String> createAttributeRowFilter(AttributeRowFilterCreatedEvent event) {
        final String insertGroup = "INSERT INTO attribute_row_filter_t (host_id, attribute_id, attribute_value, api_id, api_version, endpoint, col_name, operator, col_value, update_user, update_ts) " +
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";

        Result<String> result = null;
        try (Connection conn = ds.getConnection()) {
            conn.setAutoCommit(false);
            // no duplicate record, insert the user into database and write a success notification.
            try (PreparedStatement statement = conn.prepareStatement(insertGroup)) {
                statement.setString(1, event.getHostId());
                statement.setString(2, event.getAttributeId());
                statement.setString(3, event.getAttributeValue());
                statement.setString(4, event.getApiId());
                statement.setString(5, event.getApiVersion());
                statement.setString(6, event.getEndpoint());
                statement.setString(7, event.getColName());
                statement.setString(8, event.getOperator());
                statement.setString(9, event.getColValue());
                statement.setString(10, event.getEventId().getId());
                statement.setTimestamp(11, new Timestamp(event.getEventId().getTimestamp()));

                int count = statement.executeUpdate();
                if (count == 0) {
                    throw new SQLException("failed to insert attribute row filter " + event.getAttributeId());
                }
                conn.commit();
                result = Success.of(event.getAttributeId());
                insertNotification(event.getEventId(), event.getClass().getName(), AvroConverter.toJson(event, false), true, null);
            } catch (SQLException e) {
                logger.error("SQLException:", e);
                conn.rollback();
                insertNotification(event.getEventId(), event.getClass().getName(), AvroConverter.toJson(event, false), false, e.getMessage());
                result = Failure.of(new Status(SQL_EXCEPTION, e.getMessage()));
            } catch (Exception e) {
                logger.error("Exception:", e);
                conn.rollback();
                insertNotification(event.getEventId(), event.getClass().getName(), AvroConverter.toJson(event, false), false, e.getMessage());
                result = Failure.of(new Status(GENERIC_EXCEPTION, e.getMessage()));
            }
        } catch (SQLException e) {
            logger.error("SQLException:", e);
            result = Failure.of(new Status(SQL_EXCEPTION, e.getMessage()));
        }
        return result;
    }

    @Override
    public Result<String> updateAttributeRowFilter(AttributeRowFilterUpdatedEvent event) {
        final String updateGroup = "UPDATE attribute_row_filter_t SET attribute_value = ?, api_id = ?, api_version = ?, endpoint = ?, col_name = ?, operator = ?, col_value = ?, update_user = ?, update_ts = ? " +
                "WHERE host_id = ? AND attribute_id = ?";

        Result<String> result = null;
        try (Connection conn = ds.getConnection()) {
            conn.setAutoCommit(false);
            try (PreparedStatement statement = conn.prepareStatement(updateGroup)) {
                statement.setString(1, event.getAttributeValue());
                statement.setString(2, event.getApiId());
                statement.setString(3, event.getApiVersion());
                statement.setString(4, event.getEndpoint());
                statement.setString(5, event.getColName());
                statement.setString(6, event.getOperator());
                statement.setString(7, event.getColValue());
                statement.setString(8, event.getEventId().getId());
                statement.setTimestamp(9, new Timestamp(event.getEventId().getTimestamp()));

                statement.setString(10, event.getHostId());
                statement.setString(11, event.getAttributeId());

                int count = statement.executeUpdate();
                if (count == 0) {
                    throw new SQLException("no record is updated for attribute row filter " + event.getAttributeId());
                }
                conn.commit();
                result = Success.of(event.getAttributeId());
                insertNotification(event.getEventId(), event.getClass().getName(), AvroConverter.toJson(event, false), true, null);
            } catch (SQLException e) {
                logger.error("SQLException:", e);
                conn.rollback();
                insertNotification(event.getEventId(), event.getClass().getName(), AvroConverter.toJson(event, false), false, e.getMessage());
                result = Failure.of(new Status(SQL_EXCEPTION, e.getMessage()));
            } catch (Exception e) {
                logger.error("Exception:", e);
                conn.rollback();
                insertNotification(event.getEventId(), event.getClass().getName(), AvroConverter.toJson(event, false), false, e.getMessage());
                result = Failure.of(new Status(GENERIC_EXCEPTION, e.getMessage()));
            }
        } catch (SQLException e) {
            logger.error("SQLException:", e);
            result = Failure.of(new Status(SQL_EXCEPTION, e.getMessage()));
        }
        return result;
    }

    @Override
    public Result<String> deleteAttributeRowFilter(AttributeRowFilterDeletedEvent event) {
        final String deleteGroup = "DELETE from attribute_row_filter_t WHERE host_id = ? AND attribute_id = ? " +
                "AND api_id = ? AND api_version = ? AND endpoint = ?";
        Result<String> result;
        try (Connection conn = ds.getConnection()) {
            conn.setAutoCommit(false);
            try (PreparedStatement statement = conn.prepareStatement(deleteGroup)) {
                statement.setString(1, event.getHostId());
                statement.setString(2, event.getAttributeId());
                statement.setString(3, event.getApiId());
                statement.setString(4, event.getApiVersion());
                statement.setString(5, event.getEndpoint());

                int count = statement.executeUpdate();
                if (count == 0) {
                    throw new SQLException("no record is deleted for attribute row filter " + event.getAttributeId());
                }
                conn.commit();
                result = Success.of(event.getEventId().getId());
                insertNotification(event.getEventId(), event.getClass().getName(), AvroConverter.toJson(event, false), true, null);
            } catch (SQLException e) {
                logger.error("SQLException:", e);
                conn.rollback();
                insertNotification(event.getEventId(), event.getClass().getName(), AvroConverter.toJson(event, false), false, e.getMessage());
                result = Failure.of(new Status(SQL_EXCEPTION, e.getMessage()));
            } catch (Exception e) {
                logger.error("Exception:", e);
                conn.rollback();
                insertNotification(event.getEventId(), event.getClass().getName(), AvroConverter.toJson(event, false), false, e.getMessage());
                result = Failure.of(new Status(GENERIC_EXCEPTION, e.getMessage()));
            }
        } catch (SQLException e) {
            logger.error("SQLException:", e);
            result = Failure.of(new Status(SQL_EXCEPTION, e.getMessage()));
        }
        return result;
    }

    @Override
    public Result<String> queryAttributeColFilter(int offset, int limit, String hostId, String attributeId, String attributeValue, String apiId, String apiVersion, String endpoint) {
        Result<String> result;
        StringBuilder sqlBuilder = new StringBuilder();
        sqlBuilder.append("SELECT COUNT(*) OVER () AS total, \n" +
                "a.host_id, a.attribute_id, a.attribute_type, p.attribute_value, " +
                "p.api_id, p.api_version, p.endpoint, p.columns\n" +
                "FROM attribute_t a, attribute_col_filter_t p\n" +
                "WHERE a.attribute_id = p.attribute_id\n" +
                "AND a.host_id = ?\n");

        List<Object> parameters = new ArrayList<>();
        parameters.add(hostId);

        StringBuilder whereClause = new StringBuilder();

        addCondition(whereClause, parameters, "a.attribute_id", attributeId);
        addCondition(whereClause, parameters, "p.attribute_value", attributeValue);
        addCondition(whereClause, parameters, "p.api_id", apiId);
        addCondition(whereClause, parameters, "p.api_version", apiVersion);
        addCondition(whereClause, parameters, "p.endpoint", endpoint);

        if (whereClause.length() > 0) {
            sqlBuilder.append("AND ").append(whereClause);
        }

        sqlBuilder.append(" ORDER BY a.attribute_id, p.api_id, p.api_version, p.endpoint\n" +
                "LIMIT ? OFFSET ?");

        parameters.add(limit);
        parameters.add(offset);

        String sql = sqlBuilder.toString();
        if(logger.isTraceEnabled()) logger.trace("queryAttributeColFilter sql: {}", sql);
        int total = 0;
        List<Map<String, Object>> attributeColFilters = new ArrayList<>();

        try (final Connection conn = ds.getConnection(); PreparedStatement preparedStatement = conn.prepareStatement(sql)) {
            for (int i = 0; i < parameters.size(); i++) {
                preparedStatement.setObject(i + 1, parameters.get(i));
            }
            boolean isFirstRow = true;
            try (ResultSet resultSet = preparedStatement.executeQuery()) {
                while (resultSet.next()) {
                    Map<String, Object> map = new HashMap<>();
                    // only get the total once as it is the same for all rows.
                    if (isFirstRow) {
                        total = resultSet.getInt("total");
                        isFirstRow = false;
                    }
                    map.put("hostId", resultSet.getString("host_id"));
                    map.put("attributeId", resultSet.getString("attribute_id"));
                    map.put("attributeType", resultSet.getString("attribute_type"));
                    map.put("attributeValue", resultSet.getString("attribute_value"));
                    map.put("apiId", resultSet.getString("api_id"));
                    map.put("apiVersion", resultSet.getString("api_version"));
                    map.put("endpoint", resultSet.getString("endpoint"));
                    map.put("columns", resultSet.getString("columns"));
                    attributeColFilters.add(map);
                }
            }
            Map<String, Object> resultMap = new HashMap<>();
            resultMap.put("total", total);
            resultMap.put("attributeColFilters", attributeColFilters);
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
    public Result<String> createAttributeColFilter(AttributeColFilterCreatedEvent event) {
        final String insertGroup = "INSERT INTO attribute_col_filter_t (host_id, attribute_id, attribute_value, api_id, api_version, endpoint, columns, update_user, update_ts) " +
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)";

        Result<String> result = null;
        try (Connection conn = ds.getConnection()) {
            conn.setAutoCommit(false);
            // no duplicate record, insert the user into database and write a success notification.
            try (PreparedStatement statement = conn.prepareStatement(insertGroup)) {
                statement.setString(1, event.getHostId());
                statement.setString(2, event.getAttributeId());
                statement.setString(3, event.getAttributeValue());
                statement.setString(4, event.getApiId());
                statement.setString(5, event.getApiVersion());
                statement.setString(6, event.getEndpoint());
                statement.setString(7, event.getColumns());
                statement.setString(8, event.getEventId().getId());
                statement.setTimestamp(9, new Timestamp(event.getEventId().getTimestamp()));

                int count = statement.executeUpdate();
                if (count == 0) {
                    throw new SQLException("failed to insert attribute col filter " + event.getAttributeId());
                }
                conn.commit();
                result = Success.of(event.getAttributeId());
                insertNotification(event.getEventId(), event.getClass().getName(), AvroConverter.toJson(event, false), true, null);
            } catch (SQLException e) {
                logger.error("SQLException:", e);
                conn.rollback();
                insertNotification(event.getEventId(), event.getClass().getName(), AvroConverter.toJson(event, false), false, e.getMessage());
                result = Failure.of(new Status(SQL_EXCEPTION, e.getMessage()));
            } catch (Exception e) {
                logger.error("Exception:", e);
                conn.rollback();
                insertNotification(event.getEventId(), event.getClass().getName(), AvroConverter.toJson(event, false), false, e.getMessage());
                result = Failure.of(new Status(GENERIC_EXCEPTION, e.getMessage()));
            }
        } catch (SQLException e) {
            logger.error("SQLException:", e);
            result = Failure.of(new Status(SQL_EXCEPTION, e.getMessage()));
        }
        return result;
    }

    @Override
    public Result<String> updateAttributeColFilter(AttributeColFilterUpdatedEvent event) {
        final String updateGroup = "UPDATE attribute_col_filter_t SET attribute_value = ?, api_id = ?, api_version = ?, endpoint = ?, columns = ?, update_user = ?, update_ts = ? " +
                "WHERE host_id = ? AND attribute_id = ?";

        Result<String> result = null;
        try (Connection conn = ds.getConnection()) {
            conn.setAutoCommit(false);
            try (PreparedStatement statement = conn.prepareStatement(updateGroup)) {
                statement.setString(1, event.getAttributeValue());
                statement.setString(2, event.getApiId());
                statement.setString(3, event.getApiVersion());
                statement.setString(4, event.getEndpoint());
                statement.setString(5, event.getColumns());
                statement.setString(6, event.getEventId().getId());
                statement.setTimestamp(7, new Timestamp(event.getEventId().getTimestamp()));

                statement.setString(8, event.getHostId());
                statement.setString(9, event.getAttributeId());

                int count = statement.executeUpdate();
                if (count == 0) {
                    throw new SQLException("no record is updated for attribute col filter " + event.getAttributeId());
                }
                conn.commit();
                result = Success.of(event.getAttributeId());
                insertNotification(event.getEventId(), event.getClass().getName(), AvroConverter.toJson(event, false), true, null);
            } catch (SQLException e) {
                logger.error("SQLException:", e);
                conn.rollback();
                insertNotification(event.getEventId(), event.getClass().getName(), AvroConverter.toJson(event, false), false, e.getMessage());
                result = Failure.of(new Status(SQL_EXCEPTION, e.getMessage()));
            } catch (Exception e) {
                logger.error("Exception:", e);
                conn.rollback();
                insertNotification(event.getEventId(), event.getClass().getName(), AvroConverter.toJson(event, false), false, e.getMessage());
                result = Failure.of(new Status(GENERIC_EXCEPTION, e.getMessage()));
            }
        } catch (SQLException e) {
            logger.error("SQLException:", e);
            result = Failure.of(new Status(SQL_EXCEPTION, e.getMessage()));
        }
        return result;
    }

    @Override
    public Result<String> deleteAttributeColFilter(AttributeColFilterDeletedEvent event) {
        final String deleteGroup = "DELETE from attribute_col_filter_t WHERE host_id = ? AND attribute_id = ? " +
                "AND api_id = ? AND api_version = ? AND endpoint = ?";
        Result<String> result;
        try (Connection conn = ds.getConnection()) {
            conn.setAutoCommit(false);
            try (PreparedStatement statement = conn.prepareStatement(deleteGroup)) {
                statement.setString(1, event.getHostId());
                statement.setString(2, event.getAttributeId());
                statement.setString(3, event.getApiId());
                statement.setString(4, event.getApiVersion());
                statement.setString(5, event.getEndpoint());

                int count = statement.executeUpdate();
                if (count == 0) {
                    throw new SQLException("no record is deleted for attribute col filter " + event.getAttributeId());
                }
                conn.commit();
                result = Success.of(event.getEventId().getId());
                insertNotification(event.getEventId(), event.getClass().getName(), AvroConverter.toJson(event, false), true, null);
            } catch (SQLException e) {
                logger.error("SQLException:", e);
                conn.rollback();
                insertNotification(event.getEventId(), event.getClass().getName(), AvroConverter.toJson(event, false), false, e.getMessage());
                result = Failure.of(new Status(SQL_EXCEPTION, e.getMessage()));
            } catch (Exception e) {
                logger.error("Exception:", e);
                conn.rollback();
                insertNotification(event.getEventId(), event.getClass().getName(), AvroConverter.toJson(event, false), false, e.getMessage());
                result = Failure.of(new Status(GENERIC_EXCEPTION, e.getMessage()));
            }
        } catch (SQLException e) {
            logger.error("SQLException:", e);
            result = Failure.of(new Status(SQL_EXCEPTION, e.getMessage()));
        }
        return result;
    }

}
