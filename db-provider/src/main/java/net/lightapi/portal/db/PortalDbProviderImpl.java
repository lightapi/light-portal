package net.lightapi.portal.db;

import com.networknt.config.JsonMapper;
import com.networknt.kafka.common.AvroConverter;
import com.networknt.kafka.common.EventId;
import com.networknt.monad.Failure;
import com.networknt.monad.Result;
import com.networknt.monad.Success;
import com.networknt.status.Status;
import net.lightapi.portal.client.*;
import net.lightapi.portal.deployment.*;
import net.lightapi.portal.instance.*;
import net.lightapi.portal.oauth.*;
import net.lightapi.portal.product.*;
import net.lightapi.portal.user.*;
import net.lightapi.portal.attribute.*;
import net.lightapi.portal.group.*;
import net.lightapi.portal.position.*;
import net.lightapi.portal.role.*;
import net.lightapi.portal.rule.*;
import net.lightapi.portal.host.*;
import net.lightapi.portal.service.*;
import net.lightapi.portal.config.*;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.networknt.db.provider.SqlDbStartupHook.ds;
import static java.sql.Types.NULL;

public class PortalDbProviderImpl implements PortalDbProvider {
    public static final Logger logger = LoggerFactory.getLogger(PortalDbProviderImpl.class);
    public static final String SQL_EXCEPTION = "ERR10017";
    public static final String GENERIC_EXCEPTION = "ERR10014";
    public static final String OBJECT_NOT_FOUND = "ERR11637";

    public static final String INSERT_NOTIFICATION = "INSERT INTO notification_t (id, host_id, user_id, nonce, event_class, event_json, process_ts, " +
            "is_processed, error) VALUES (?, ?, ?, ?, ?,  ?, ?, ?, ?)";

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
        /*
        SELECT
        uh.host_id,
        u.user_id,
        u.email,
        u.user_type,
        u.password,
        u.verified,
        CASE
            WHEN u.user_type = 'E' THEN e.employee_id
            WHEN u.user_type = 'C' THEN c.customer_id
            ELSE NULL
        END AS entity_id,
        CASE WHEN u.user_type = 'E' THEN string_agg(DISTINCT p.position_id, ' ' ORDER BY p.position_id) ELSE NULL END AS positions,
        string_agg(DISTINCT r.role_id, ' ' ORDER BY r.role_id) AS roles,
        string_agg(DISTINCT g.group_id, ' ' ORDER BY g.group_id) AS groups,
        CASE
            WHEN COUNT(DISTINCT at.attribute_id || '^=^' || aut.attribute_value) > 0 THEN string_agg(DISTINCT at.attribute_id || '^=^' || aut.attribute_value, '~' ORDER BY at.attribute_id || '^=^' || aut.attribute_value)
            ELSE NULL
        END AS attributes
        FROM
            user_t AS u
        LEFT JOIN
             user_host_t AS uh ON u.user_id = uh.user_id
        LEFT JOIN
            role_user_t AS ru ON u.user_id = ru.user_id
        LEFT JOIN
            role_t AS r ON ru.host_id = r.host_id AND ru.role_id = r.role_id
        LEFT JOIN
            attribute_user_t AS aut ON u.user_id = aut.user_id
        LEFT JOIN
            attribute_t AS at ON aut.host_id = at.host_id AND aut.attribute_id = at.attribute_id
        LEFT JOIN
            group_user_t AS gu ON u.user_id = gu.user_id
        LEFT JOIN
            group_t AS g ON gu.host_id = g.host_id AND gu.group_id = g.group_id
        LEFT JOIN
            employee_t AS e ON uh.host_id = e.host_id AND u.user_id = e.user_id
        LEFT JOIN
            customer_t AS c ON uh.host_id = c.host_id AND u.user_id = c.user_id
        LEFT JOIN
            employee_position_t AS ep ON e.host_id = ep.host_id AND e.employee_id = ep.employee_id
        LEFT JOIN
            position_t AS p ON ep.host_id = p.host_id AND ep.position_id = p.position_id
        WHERE
            u.email = 'steve.hu@networknt.com'
            AND u.locked = FALSE
            AND u.verified = TRUE
        GROUP BY
            uh.host_id, u.user_id, u.user_type, e.employee_id, c.customer_id;         */

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
    public Result<String> queryNotification(int offset, int limit, String hostId, String userId, Long nonce, String eventClass, Boolean successFlag,
                                            Timestamp processTs, String eventJson, String error) {
        Result<String> result = null;
        StringBuilder sqlBuilder = new StringBuilder();
        sqlBuilder.append("SELECT COUNT(*) OVER () AS total,\n" +
                "host_id, user_id, nonce, event_class, is_processed, process_ts, event_json, error\n" +
                "FROM notification_t\n" +
                "WHERE host_id = ?\n");

        List<Object> parameters = new ArrayList<>();
        parameters.add(hostId);

        StringBuilder whereClause = new StringBuilder();

        addCondition(whereClause, parameters, "user_id", userId);
        addCondition(whereClause, parameters, "nonce", nonce);
        addCondition(whereClause, parameters, "event_class", eventClass);
        addCondition(whereClause, parameters, "is_processed", successFlag);
        addCondition(whereClause, parameters, "event_json", eventJson);
        addCondition(whereClause, parameters, "error", error);

        if (whereClause.length() > 0) {
            sqlBuilder.append("AND ").append(whereClause);
        }

        sqlBuilder.append(" ORDER BY process_ts DESC\n" +
                "LIMIT ? OFFSET ?");

        parameters.add(limit);
        parameters.add(offset);

        String sql = sqlBuilder.toString();
        int total = 0;
        List<Map<String, Object>> notifications = new ArrayList<>();

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
                    map.put("nonce", resultSet.getLong("nonce"));
                    map.put("eventClass", resultSet.getString("event_class"));
                    map.put("processFlag", resultSet.getBoolean("is_processed"));
                    // handling date properly
                    map.put("processTs", resultSet.getTimestamp("process_ts") != null ? resultSet.getTimestamp("process_ts").toString() : null);
                    map.put("eventJson", resultSet.getString("event_json"));
                    map.put("error", resultSet.getString("error"));
                    notifications.add(map);
                }
            }


            Map<String, Object> resultMap = new HashMap<>();
            resultMap.put("total", total);
            resultMap.put("notifications", notifications);
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
            statement.setString(2, eventId.getHostId());
            statement.setString(3, eventId.getUserId());
            statement.setLong(4, eventId.getNonce());
            statement.setString(5, eventClass);
            statement.setString(6, json);
            statement.setTimestamp(7, new Timestamp(eventId.getTimestamp()));
            statement.setBoolean(8, flag);
            if (error != null) {
                statement.setString(9, error);
            } else {
                statement.setNull(9, NULL);
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
                statement.setString(1, event.getEventId().getUserId());
                statement.setString(2, event.getToken());
                try (ResultSet resultSet = statement.executeQuery()) {
                    if (resultSet.next()) {
                        // found the token record, update user_t for token, verified flog and nonce, write a success notification.
                        try (PreparedStatement updateStatement = conn.prepareStatement(updateUserByEmail)) {
                            updateStatement.setLong(1, event.getEventId().getNonce() + 1);
                            updateStatement.setString(2, event.getEventId().getUserId());
                            updateStatement.execute();
                        }
                    } else {
                        // record is not found with the email and token. write an error notification.
                        throw new SQLException(String.format("token %s is not matched for userId %s.", event.getToken(), event.getEventId().getUserId()));
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
    public Result<String> createRefreshToken(AuthRefreshTokenCreatedEvent event) {
        final String insertUser = "INSERT INTO auth_refresh_token_t (refresh_token, host_id, provider_id, user_id, entity_id, user_type, " +
                "email, roles, groups, positions, attributes, client_id, scope, csrf, custom_claim, update_user, update_ts) " +
                "VALUES (?, ?, ?, ?, ?,   ?, ?, ?, ?, ?,   ?, ?, ?, ?, ?, ?, ?)";
        Result<String> result = null;
        Map<String, Object> map = JsonMapper.string2Map(event.getValue());
        try (Connection conn = ds.getConnection()) {
            conn.setAutoCommit(false);
            // no duplicate record, insert the user into database and write a success notification.
            try (PreparedStatement statement = conn.prepareStatement(insertUser)) {
                statement.setString(1, event.getRefreshToken());
                statement.setString(2, event.getHostId());
                statement.setString(3, event.getProviderId());
                statement.setString(4, event.getUserId());
                statement.setString(5, (String) map.get("entityId"));
                statement.setString(6, (String) map.get("userType"));
                statement.setString(7, (String) map.get("email"));

                if (map.get("roles") != null)
                    statement.setString(8, (String) map.get("roles"));
                else
                    statement.setNull(8, NULL);

                if (map.get("groups") != null)
                    statement.setString(9, (String) map.get("groups"));
                else
                    statement.setNull(9, NULL);

                if (map.get("positions") != null)
                    statement.setString(10, (String) map.get("positions"));
                else
                    statement.setNull(10, NULL);

                if (map.get("attributes") != null)
                    statement.setString(11, (String) map.get("attributes"));
                else
                    statement.setNull(11, NULL);

                if (map.get("clientId") != null)
                    statement.setString(12, (String) map.get("clientId"));
                else
                    statement.setNull(12, NULL);

                if (map.get("scope") != null)
                    statement.setString(13, (String) map.get("scope"));
                else
                    statement.setNull(13, NULL);

                if (map.get("csrf") != null)
                    statement.setString(14, (String) map.get("csrf"));
                else
                    statement.setNull(14, NULL);

                if (map.get("customClaim") != null)
                    statement.setString(15, (String) map.get("customClaim"));
                else
                    statement.setNull(15, NULL);

                statement.setString(16, event.getEventId().getId());
                statement.setTimestamp(17, new java.sql.Timestamp(event.getEventId().getTimestamp()));
                int count = statement.executeUpdate();
                if (count == 0) {
                    // no record is inserted, write an error notification.
                    throw new SQLException(String.format("no record is inserted for refresh token %s", event.getRefreshToken()));
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
    public Result<String> deleteRefreshToken(AuthRefreshTokenDeletedEvent event) {
        final String deleteApp = "DELETE from auth_refresh_token_t WHERE refresh_token = ? AND user_id = ?";
        Result<String> result;
        try (Connection conn = ds.getConnection()) {
            conn.setAutoCommit(false);
            try (PreparedStatement statement = conn.prepareStatement(deleteApp)) {
                statement.setString(1, event.getRefreshToken());
                statement.setString(2, event.getUserId());
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
    public Result<String> listRefreshToken(int offset, int limit, String refreshToken, String hostId, String userId, String entityId,
                                           String email, String firstName, String lastName, String clientId, String appId,
                                           String appName, String scope, String userType, String roles, String groups, String positions,
                                           String attributes, String csrf, String customClaim, String updateUser, Timestamp updateTs) {
        Result<String> result = null;
        StringBuilder sqlBuilder = new StringBuilder();
        sqlBuilder.append("SELECT COUNT(*) OVER () AS total,\n" +
                "r.host_id, r.refresh_token, r.user_id, r.user_type, r.entity_id, r.email, u.first_name, u.last_name, \n" +
                "r.client_id, a.app_id, a.app_name, r.scope, r.roles, r.groups, r.positions, r.attributes, r.csrf, " +
                "r.custom_claim, r.update_user, r.update_ts \n" +
                "FROM auth_refresh_token_t r, user_t u, app_t a, auth_client_t c\n" +
                "WHERE r.user_id = u.user_id AND r.client_id = c.client_id AND a.app_id = c.app_id\n" +
                "AND r.host_id = ?\n");

        List<Object> parameters = new ArrayList<>();
        parameters.add(hostId);

        StringBuilder whereClause = new StringBuilder();

        addCondition(whereClause, parameters, "r.refresh_token", refreshToken);
        addCondition(whereClause, parameters, "r.user_id", userId);
        addCondition(whereClause, parameters, "r.user_type", userType);
        addCondition(whereClause, parameters, "u.entity_id", entityId);
        addCondition(whereClause, parameters, "u.email", email);
        addCondition(whereClause, parameters, "r.first_name", firstName);
        addCondition(whereClause, parameters, "r.last_name", lastName);
        addCondition(whereClause, parameters, "r.client_id", clientId);
        addCondition(whereClause, parameters, "a.app_id", appId);
        addCondition(whereClause, parameters, "a.app_name", appName);
        addCondition(whereClause, parameters, "r.scope", scope);
        addCondition(whereClause, parameters, "r.roles", roles);
        addCondition(whereClause, parameters, "r.groups", groups);
        addCondition(whereClause, parameters, "r.positions", positions);
        addCondition(whereClause, parameters, "r.attributes", attributes);
        addCondition(whereClause, parameters, "r.csrf", csrf);
        addCondition(whereClause, parameters, "r.custom_claim", customClaim);
        addCondition(whereClause, parameters, "r.update_user", updateUser);
        addCondition(whereClause, parameters, "r.update_ts", updateTs);


        if (whereClause.length() > 0) {
            sqlBuilder.append("AND ").append(whereClause);
        }

        sqlBuilder.append(" ORDER BY r.user_id\n" +
                "LIMIT ? OFFSET ?");

        parameters.add(limit);
        parameters.add(offset);

        String sql = sqlBuilder.toString();
        int total = 0;
        List<Map<String, Object>> tokens = new ArrayList<>();

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
                    map.put("refreshToken", resultSet.getString("refresh_token"));
                    map.put("userId", resultSet.getString("user_id"));
                    map.put("userType", resultSet.getString("user_type"));
                    map.put("entityId", resultSet.getString("entity_id"));
                    map.put("email", resultSet.getString("email"));
                    map.put("firstName", resultSet.getString("first_name"));
                    map.put("lastName", resultSet.getString("last_name"));
                    map.put("clientId", resultSet.getString("client_id"));
                    map.put("appId", resultSet.getString("app_id"));
                    map.put("appName", resultSet.getString("app_name"));
                    map.put("scope", resultSet.getString("scope"));
                    map.put("roles", resultSet.getString("roles"));
                    map.put("groups", resultSet.getString("groups"));
                    map.put("positions", resultSet.getString("positions"));
                    map.put("attributes", resultSet.getString("attributes"));
                    map.put("csrf", resultSet.getString("csrf"));
                    map.put("customClaim", resultSet.getString("custom_claim"));
                    map.put("updateUser", resultSet.getString("update_user"));
                    map.put("updateTs", resultSet.getTimestamp("update_ts"));
                    tokens.add(map);
                }
            }


            Map<String, Object> resultMap = new HashMap<>();
            resultMap.put("total", total);
            resultMap.put("tokens", tokens);
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
    public Result<String> queryRefreshToken(String refreshToken) {
        Result<String> result = null;
        String sql =
                "SELECT refresh_token, host_id, provider_id, user_id, entity_id, user_type, email, roles, groups, " +
                        "positions, attributes, client_id, scope, csrf, custom_claim\n" +
                        "FROM auth_refresh_token_t\n" +
                        "WHERE refresh_token = ?\n";
        try (final Connection conn = ds.getConnection()) {
            Map<String, Object> map = new HashMap<>();
            try (PreparedStatement statement = conn.prepareStatement(sql)) {
                statement.setString(1, refreshToken);
                try (ResultSet resultSet = statement.executeQuery()) {
                    if (resultSet.next()) {
                        map.put("refreshToken", resultSet.getString("refresh_token"));
                        map.put("hostId", resultSet.getString("host_id"));
                        map.put("providerId", resultSet.getString("provider_id"));
                        map.put("userId", resultSet.getString("user_id"));
                        map.put("entityId", resultSet.getString("entity_id"));
                        map.put("userType", resultSet.getString("user_type"));
                        map.put("email", resultSet.getString("email"));
                        map.put("roles", resultSet.getString("roles"));
                        map.put("groups", resultSet.getString("groups"));
                        map.put("positions", resultSet.getString("positions"));
                        map.put("attributes", resultSet.getString("attributes"));
                        map.put("clientId", resultSet.getString("client_id"));
                        map.put("scope", resultSet.getString("scope"));
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
    public Result<String> createAuthCode(AuthCodeCreatedEvent event) {

        final String sql = "INSERT INTO auth_code_t(host_id, provider_id, auth_code, user_id, entity_id, user_type, email, roles," +
                "redirect_uri, scope, remember, code_challenge, challenge_method, update_user, update_ts) " +
                "VALUES (?, ?, ?, ?, ?, ?, ?,  ?, ?, ?, ?, ?, ?, ?, ?)";
        Result<String> result;
        String value = event.getValue();
        Map<String, Object> map = JsonMapper.string2Map(value);
        try (Connection conn = ds.getConnection()) {
            conn.setAutoCommit(false);
            // no duplicate record, insert the user into database and write a success notification.
            try (PreparedStatement statement = conn.prepareStatement(sql)) {
                statement.setString(1, event.getHostId());
                statement.setString(2, event.getProviderId());
                statement.setString(3, event.getAuthCode());
                if(map.containsKey("userId")) {
                    statement.setString(4, (String) map.get("userId"));
                } else {
                    statement.setNull(4, Types.VARCHAR);
                }
                if(map.containsKey("entityId")) {
                    statement.setString(5, (String) map.get("entityId"));
                } else {
                    statement.setNull(5, Types.VARCHAR);
                }
                if(map.containsKey("userType")) {
                    statement.setString(6, (String) map.get("userType"));
                } else {
                    statement.setNull(6, Types.VARCHAR);
                }
                if(map.containsKey("email")) {
                    statement.setString(7, (String) map.get("email"));
                } else {
                    statement.setNull(7, Types.VARCHAR);
                }
                if(map.containsKey("roles")) {
                    statement.setString(8, (String) map.get("roles"));
                } else {
                    statement.setNull(8, Types.VARCHAR);
                }
                if(map.containsKey("redirectUri")) {
                    statement.setString(9, (String) map.get("redirectUri"));
                } else {
                    statement.setNull(9, Types.VARCHAR);
                }
                if(map.containsKey("scope")) {
                    statement.setString(10, (String) map.get("scope"));
                } else {
                    statement.setNull(10, Types.VARCHAR);
                }
                if(map.containsKey("remember")) {
                    statement.setString(11, (String) map.get("remember"));
                } else {
                    statement.setNull(11, Types.CHAR);
                }
                if(map.containsKey("codeChallenge")) {
                    statement.setString(12, (String) map.get("codeChallenge"));
                } else {
                    statement.setNull(12, Types.VARCHAR);
                }
                if(map.containsKey("challengeMethod")) {
                    statement.setString(13, (String) map.get("challengeMethod"));
                } else {
                    statement.setNull(13, Types.VARCHAR);
                }
                statement.setString(14, event.getEventId().getId());
                statement.setTimestamp(15, new Timestamp(event.getEventId().getTimestamp()));
                int count = statement.executeUpdate();
                if (count == 0) {
                    throw new SQLException("failed to insert the auth code with id " + event.getAuthCode());
                }
                conn.commit();
                result = Success.of(event.getAuthCode());
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
    public Result<String> deleteAuthCode(AuthCodeDeletedEvent event) {
        final String sql = "DELETE FROM auth_code_t WHERE host_id = ? AND auth_code = ?";
        Result<String> result;
        try (Connection conn = ds.getConnection()) {
            conn.setAutoCommit(false);
            try (PreparedStatement statement = conn.prepareStatement(sql)) {
                statement.setString(1, event.getHostId());
                statement.setString(2, event.getAuthCode());
                int count = statement.executeUpdate();
                if (count == 0) {
                    // no record is deleted, write an error notification.
                    throw new SQLException(String.format("no record is deleted for auth code " + "hostId " + event.getHostId() + " authCode " + event.getAuthCode()));
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
    public Result<String> queryAuthCode(String authCode) {
        final String sql = "SELECT host_id, provider_id, auth_code, user_id, entity_id, user_type, email, " +
                "roles, redirect_uri, scope, remember, code_challenge, challenge_method " +
                "FROM auth_code_t WHERE auth_code = ?";
        Result<String> result;
        try (Connection connection = ds.getConnection();
             PreparedStatement preparedStatement = connection.prepareStatement(sql)) {
            preparedStatement.setString(1, authCode);
            try (ResultSet resultSet = preparedStatement.executeQuery()) {
                if (resultSet.next()) {
                    Map<String, Object> map = new HashMap<>();
                    map.put("hostId", resultSet.getString("host_id"));
                    map.put("providerId", resultSet.getString("provider_id"));
                    map.put("authCode", resultSet.getString("auth_code"));
                    map.put("userId", resultSet.getString("user_id"));
                    map.put("entityId", resultSet.getString("entity_id"));
                    map.put("userType", resultSet.getString("user_type"));
                    map.put("email", resultSet.getString("email"));
                    map.put("roles", resultSet.getString("roles"));
                    map.put("redirectUri", resultSet.getString("redirect_uri"));
                    map.put("scope", resultSet.getString("scope"));
                    map.put("remember", resultSet.getString("remember"));
                    map.put("codeChallenge", resultSet.getString("code_challenge"));
                    map.put("challengeMethod", resultSet.getString("challenge_method"));
                    result = Success.of(JsonMapper.toJson(map));
                } else {
                    result = Failure.of(new Status(OBJECT_NOT_FOUND, "auth code", authCode));
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
    public Result<String> listAuthCode(int offset, int limit, String hostId, String authCode, String userId,
                                       String entityId, String userType, String email, String roles, String groups, String positions,
                                       String attributes, String redirectUri, String scope, String remember, String codeChallenge,
                                       String challengeMethod, String updateUser, Timestamp updateTs) {
        Result<String> result = null;
        StringBuilder sqlBuilder = new StringBuilder();
        sqlBuilder.append("SELECT COUNT(*) OVER () AS total,\n" +
                "host_id, auth_code, user_id, entity_id, user_type, email, roles, redirect_uri, scope, remember, " +
                "code_challenge, challenge_method, update_user, update_ts\n" +
                "FROM auth_code_t\n" +
                "WHERE host_id = ?\n");


        List<Object> parameters = new ArrayList<>();
        parameters.add(hostId);

        StringBuilder whereClause = new StringBuilder();

        addCondition(whereClause, parameters, "auth_code", authCode);
        addCondition(whereClause, parameters, "user_id", userId);
        addCondition(whereClause, parameters, "entity_id", entityId);
        addCondition(whereClause, parameters, "user_type", userType);
        addCondition(whereClause, parameters, "email", email);
        addCondition(whereClause, parameters, "roles", roles);
        addCondition(whereClause, parameters, "redirect_uri", redirectUri);
        addCondition(whereClause, parameters, "scope", scope);
        addCondition(whereClause, parameters, "remember", remember);
        addCondition(whereClause, parameters, "code_challenge", codeChallenge);
        addCondition(whereClause, parameters, "challenge_method", challengeMethod);

        if (whereClause.length() > 0) {
            sqlBuilder.append("AND ").append(whereClause);
        }

        sqlBuilder.append(" ORDER BY update_ts\n" +
                "LIMIT ? OFFSET ?");

        parameters.add(limit);
        parameters.add(offset);

        String sql = sqlBuilder.toString();
        int total = 0;
        List<Map<String, Object>> authCodes = new ArrayList<>();

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
                    map.put("authCode", resultSet.getString("auth_code"));
                    map.put("userId", resultSet.getString("user_id"));
                    map.put("entityId", resultSet.getString("entity_id"));
                    map.put("userType", resultSet.getString("user_type"));
                    map.put("email", resultSet.getString("email"));
                    map.put("roles", resultSet.getString("roles"));
                    map.put("redirectUri", resultSet.getString("redirect_uri"));
                    map.put("scope", resultSet.getString("scope"));
                    map.put("remember", resultSet.getString("remember"));
                    map.put("codeChallenge", resultSet.getString("code_challenge"));
                    map.put("challengeMethod", resultSet.getString("challenge_method"));
                    map.put("updateUser", resultSet.getString("update_user"));
                    map.put("updateTs", resultSet.getTimestamp("update_ts") != null ? resultSet.getTimestamp("update_ts").toString() : null);

                    authCodes.add(map);
                }
            }

            Map<String, Object> resultMap = new HashMap<>();
            resultMap.put("total", total);
            resultMap.put("codes", authCodes);
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
    public Result<Map<String, Object>> queryProviderById(String providerId) {
        final String sql = "SELECT host_id, provider_id, provider_name, jwk " +
                "from auth_provider_t WHERE provider_id = ?";
        Result<Map<String, Object>> result;
        try (final Connection conn = ds.getConnection()) {
            Map<String, Object> map = new HashMap<>();
            try (PreparedStatement statement = conn.prepareStatement(sql)) {
                statement.setString(1, providerId);
                try (ResultSet resultSet = statement.executeQuery()) {
                    if (resultSet.next()) {
                        map.put("hostId", resultSet.getString("host_id"));
                        map.put("providerId", resultSet.getString("provider_id"));
                        map.put("providerName", resultSet.getString("provider_name"));
                        map.put("jwk", resultSet.getString("jwk"));
                    }
                }
            }
            if (map.isEmpty())
                result = Failure.of(new Status(OBJECT_NOT_FOUND, "auth provider", providerId));
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
    public Result<String> queryProvider(int offset, int limit, String hostId, String providerId, String providerName, String providerDesc,
                                        String operationOwner, String deliveryOwner, String jwk, String updateUser, Timestamp updateTs) {
        Result<String> result = null;
        StringBuilder sqlBuilder = new StringBuilder();
        sqlBuilder.append("SELECT COUNT(*) OVER () AS total,\n" +
                "host_id, provider_id, provider_name, provider_desc, operation_owner, delivery_owner, jwk, update_user, update_ts\n" +
                "FROM auth_provider_t\n" +
                "WHERE host_id = ?\n");

        List<Object> parameters = new ArrayList<>();
        parameters.add(hostId);

        StringBuilder whereClause = new StringBuilder();

        addCondition(whereClause, parameters, "provider_id", providerId);
        addCondition(whereClause, parameters, "provider_name", providerName);
        addCondition(whereClause, parameters, "provider_desc", providerDesc);
        addCondition(whereClause, parameters, "operation_owner", operationOwner);
        addCondition(whereClause, parameters, "delivery_owner", deliveryOwner);
        addCondition(whereClause, parameters, "jwk", jwk);

        if (whereClause.length() > 0) {
            sqlBuilder.append("AND ").append(whereClause);
        }

        sqlBuilder.append(" ORDER BY provider_id\n" +
                "LIMIT ? OFFSET ?");

        parameters.add(limit);
        parameters.add(offset);

        String sql = sqlBuilder.toString();
        int total = 0;
        List<Map<String, Object>> providers = new ArrayList<>();

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
                    map.put("providerId", resultSet.getString("provider_id"));
                    map.put("providerName", resultSet.getString("provider_name"));
                    map.put("providerDesc", resultSet.getString("provider_desc"));
                    map.put("operationOwner", resultSet.getString("operation_owner"));
                    map.put("deliveryOwner", resultSet.getString("delivery_owner"));
                    map.put("jwk", resultSet.getString("jwk"));
                    map.put("updateUser", resultSet.getString("update_user"));
                    // handling date properly
                    map.put("updateTs", resultSet.getTimestamp("update_ts") != null ? resultSet.getTimestamp("update_ts").toString() : null);
                    providers.add(map);
                }
            }

            Map<String, Object> resultMap = new HashMap<>();
            resultMap.put("total", total);
            resultMap.put("providers", providers);
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
    public Result<String> createAuthProvider(AuthProviderCreatedEvent event) {
        final String sql = "INSERT INTO auth_provider_t(host_id, provider_id, provider_name, provider_desc, " +
                "operation_owner, delivery_owner, jwk, update_user, update_ts) " +
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)";
        Result<String> result;
        Timestamp timestamp = new Timestamp(event.getEventId().getTimestamp());
        String value = event.getValue();
        Map<String, Object> map = JsonMapper.string2Map(value);

        try (Connection conn = ds.getConnection()) {
            conn.setAutoCommit(false);
            try (PreparedStatement statement = conn.prepareStatement(sql)) {
                statement.setString(1, event.getHostId());
                statement.setString(2, event.getProviderId());
                statement.setString(3, event.getProviderName());

                if(map.containsKey("providerDesc")) {
                    statement.setString(4, (String)map.get("providerDesc"));
                } else {
                    statement.setNull(4, Types.VARCHAR);
                }
                if(map.containsKey("operationOwner")) {
                    statement.setString(5, (String)map.get("operationOwner"));
                } else {
                    statement.setNull(5, Types.VARCHAR);
                }
                if(map.containsKey("deliveryOwner")) {
                    statement.setString(6, (String)map.get("deliveryOwner"));
                } else {
                    statement.setNull(6, Types.VARCHAR);
                }
                if(map.containsKey("jwk")) {
                    statement.setString(7, (String)map.get("jwk"));
                } else {
                    statement.setNull(7, Types.VARCHAR);
                }
                statement.setString(8, event.getEventId().getId());
                statement.setTimestamp(9, timestamp);

                int count = statement.executeUpdate();
                if (count == 0) {
                    throw new SQLException("failed to insert the auth provider with id " + event.getProviderId());
                }

                // Insert keys into auth_provider_key_t
                String keySql = "INSERT INTO auth_provider_key_t(provider_id, kid, public_key, private_key, key_type, update_user, update_ts) " +
                        "VALUES (?, ?, ?, ?, ?, ?, ?)";

                try (PreparedStatement keyStatement = conn.prepareStatement(keySql)) {
                    Map<String, Object> keys = (Map<String, Object>) map.get("keys");

                    keyStatement.setString(1, event.getProviderId());

                    Map<String, Object> lcMap = (Map<String, Object>) keys.get("LC");
                    // add long live current key
                    keyStatement.setString(2, (String)lcMap.get("kid"));
                    keyStatement.setString(3, (String)lcMap.get("publicKey"));
                    keyStatement.setString(4, (String)lcMap.get("privateKey"));
                    keyStatement.setString(5, "LC");
                    keyStatement.setString(6, event.getEventId().getId());
                    keyStatement.setTimestamp(7, timestamp);
                    keyStatement.executeUpdate();

                    // add long live previous key
                    Map<String, Object> lpMap = (Map<String, Object>) keys.get("LP");
                    keyStatement.setString(2, (String)lpMap.get("kid"));
                    keyStatement.setString(3, (String)lpMap.get("publicKey"));
                    keyStatement.setString(4, (String)lpMap.get("privateKey"));
                    keyStatement.setString(5, "LP");
                    keyStatement.setString(6, event.getEventId().getId());
                    keyStatement.setTimestamp(7, timestamp);
                    keyStatement.executeUpdate();

                    // add token current key
                    Map<String, Object> tcMap = (Map<String, Object>) keys.get("TC");
                    keyStatement.setString(2, (String)tcMap.get("kid"));
                    keyStatement.setString(3, (String)tcMap.get("publicKey"));
                    keyStatement.setString(4, (String)tcMap.get("privateKey"));
                    keyStatement.setString(5, "TC");
                    keyStatement.setString(6, event.getEventId().getId());
                    keyStatement.setTimestamp(7, timestamp);
                    keyStatement.executeUpdate();

                    // add token previous key
                    Map<String, Object> tpMap = (Map<String, Object>) keys.get("TP");
                    keyStatement.setString(2, (String)tpMap.get("kid"));
                    keyStatement.setString(3, (String)tpMap.get("publicKey"));
                    keyStatement.setString(4, (String)tpMap.get("privateKey"));
                    keyStatement.setString(5, "TP");
                    keyStatement.setString(6, event.getEventId().getId());
                    keyStatement.setTimestamp(7, new Timestamp(event.getEventId().getTimestamp()));
                    keyStatement.executeUpdate();

                } catch(SQLException ex) {
                    throw new SQLException("failed to insert the auth provider key with provider id " + event.getProviderId());
                }
                conn.commit();
                result = Success.of(event.getProviderId());
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
    public Result<String> rotateAuthProvider(AuthProviderRotatedEvent event) {
        final String sqlJwk = "UPDATE auth_provider_t SET jwk = ?, update_user = ?, update_ts = ? " +
                "WHERE provider_id = ?";
        final String sqlInsert = "INSERT INTO auth_provider_key_t(provider_id, kid, public_key, private_key, key_type, update_user, update_ts) " +
                "VALUES (?, ?, ?, ?, ?,  ?, ?)";
        final String sqlUpdate = "UPDATE auth_provider_key_t SET key_type = ?, update_user = ?, update_ts = ? " +
                "WHERE provider_id = ? AND kid = ?";
        final String sqlDelete = "DELETE FROM auth_provider_key_t WHERE provider_id = ? AND kid = ?";


        Result<String> result;
        Timestamp timestamp = new Timestamp(event.getEventId().getTimestamp());
        String value = event.getValue();
        Map<String, Object> map = JsonMapper.string2Map(value);

        try (Connection conn = ds.getConnection()) {
            conn.setAutoCommit(false);
            try (PreparedStatement statement = conn.prepareStatement(sqlJwk)) {
                String jwk = (String) map.get("jwk");
                statement.setString(1, jwk);
                statement.setString(2, event.getEventId().getId());
                statement.setTimestamp(3, timestamp);
                statement.setString(4, event.getProviderId());

                int count = statement.executeUpdate();
                if (count == 0) {
                    throw new SQLException("failed to update the jwk for auth provider with id " + event.getProviderId());
                }

                try (PreparedStatement statementInsert = conn.prepareStatement(sqlInsert)) {
                    Map<String, Object> insertMap = (Map<String, Object>) map.get("insert");
                    statementInsert.setString(1, event.getProviderId());
                    statementInsert.setString(2, (String) insertMap.get("kid"));
                    statementInsert.setString(3, (String) insertMap.get("publicKey"));
                    statementInsert.setString(4, (String) insertMap.get("privateKey"));
                    statementInsert.setString(5, (String) insertMap.get("keyType"));
                    statementInsert.setString(6, event.getEventId().getUserId());
                    statementInsert.setTimestamp(7, timestamp);

                    count = statementInsert.executeUpdate();
                    if (count == 0) {
                        throw new SQLException("failed to insert the auth provider key with provider id " + event.getProviderId());
                    }
                }
                try (PreparedStatement statementUpdate = conn.prepareStatement(sqlUpdate)) {
                    Map<String, Object> updateMap = (Map<String, Object>) map.get("update");
                    statementUpdate.setString(1, (String) updateMap.get("keyType"));
                    statementUpdate.setString(2, event.getEventId().getUserId());
                    statementUpdate.setTimestamp(3, timestamp);
                    statementUpdate.setString(4, event.getProviderId());
                    statementUpdate.setString(5, (String) updateMap.get("kid"));
                    count = statementUpdate.executeUpdate();
                    if (count == 0) {
                        throw new SQLException("failed to update the auth provider key with provider id " + event.getProviderId());
                    }
                }
                try (PreparedStatement statementDelete = conn.prepareStatement(sqlDelete)) {
                    Map<String, Object> deleteMap = (Map<String, Object>) map.get("delete");
                    statementDelete.setString(1, event.getProviderId());
                    statementDelete.setString(2, (String) deleteMap.get("kid"));
                    count = statementDelete.executeUpdate();
                    if (count == 0) {
                        throw new SQLException("failed to update the auth provider key with provider id " + event.getProviderId());
                    }
                }
                conn.commit();
                result = Success.of(event.getProviderId());
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
    public Result<String> updateAuthProvider(AuthProviderUpdatedEvent event) {
        final String sql = "UPDATE auth_provider_t SET provider_name = ?, provider_desc = ?, " +
                "operation_owner = ?, delivery_owner = ?, update_user = ?, update_ts = ? " +
                "WHERE host_id = ? and provider_id = ?";
        Result<String> result;
        String value = event.getValue();
        Map<String, Object> map = JsonMapper.string2Map(value);

        try (Connection conn = ds.getConnection()) {
            conn.setAutoCommit(false);
            try (PreparedStatement statement = conn.prepareStatement(sql)) {
                statement.setString(1, event.getProviderName());
                if(map.containsKey("providerDesc")) {
                    statement.setString(2, (String)map.get("providerDesc"));
                } else {
                    statement.setNull(2, Types.VARCHAR);
                }
                if(map.containsKey("operationOwner")) {
                    statement.setString(3, (String)map.get("operationOwner"));
                } else {
                    statement.setNull(3, Types.VARCHAR);
                }
                if(map.containsKey("deliveryOwner")) {
                    statement.setString(4, (String)map.get("deliveryOwner"));
                } else {
                    statement.setNull(4, Types.VARCHAR);
                }
                statement.setString(5, event.getEventId().getId());
                statement.setTimestamp(6, new Timestamp(event.getEventId().getTimestamp()));
                statement.setString(7, event.getHostId());
                statement.setString(8, event.getProviderId());

                int count = statement.executeUpdate();
                if (count == 0) {
                    throw new SQLException("failed to update the auth provider with id " + event.getProviderId());
                }
                conn.commit();
                result = Success.of(event.getProviderId());
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
    public Result<String> deleteAuthProvider(AuthProviderDeletedEvent event) {
        final String sql = "DELETE FROM auth_provider_t WHERE host_id = ? and provider_id = ?";
        Result<String> result;

        try (Connection conn = ds.getConnection()) {
            conn.setAutoCommit(false);
            try (PreparedStatement statement = conn.prepareStatement(sql)) {
                statement.setString(1, event.getHostId());
                statement.setString(2, event.getProviderId());

                int count = statement.executeUpdate();
                if (count == 0) {
                    throw new SQLException("failed to delete the auth provider with id " + event.getProviderId());
                }
                conn.commit();
                result = Success.of(event.getProviderId());
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
    public Result<String> queryProviderKey(String providerId) {
        Result<String> result = null;
        String sql = "SELECT provider_id, kid, public_key, private_key, key_type, update_user, update_ts\n" +
                "FROM auth_provider_key_t\n" +
                "WHERE provider_id = ?\n";

        List<Map<String, Object>> providerKeys = new ArrayList<>();

        try (Connection connection = ds.getConnection();
             PreparedStatement preparedStatement = connection.prepareStatement(sql)) {
            preparedStatement.setString(1, providerId);
            try (ResultSet resultSet = preparedStatement.executeQuery()) {
                while (resultSet.next()) {
                    Map<String, Object> map = new HashMap<>();
                    map.put("providerId", resultSet.getString("provider_id"));
                    map.put("kid", resultSet.getString("kid"));
                    map.put("publicKey", resultSet.getString("public_key"));
                    map.put("privateKey", resultSet.getString("private_key"));
                    map.put("keyType", resultSet.getString("key_type"));
                    map.put("updateUser", resultSet.getString("update_user"));
                    // handling date properly
                    map.put("updateTs", resultSet.getTimestamp("update_ts") != null ? resultSet.getTimestamp("update_ts").toString() : null);
                    providerKeys.add(map);
                }
            }
            result = Success.of(JsonMapper.toJson(providerKeys));
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
    public Result<String> queryApp(int offset, int limit, String hostId, String appId, String appName, String appDesc,
                                   Boolean isKafkaApp, String operationOwner, String deliveryOwner) {
        Result<String> result = null;
        StringBuilder sqlBuilder = new StringBuilder();
        sqlBuilder.append("SELECT COUNT(*) OVER () AS total,\n" +
                "host_id, app_id, app_name, app_desc, is_kafka_app, operation_owner, delivery_owner\n" +
                "FROM app_t\n" +
                "WHERE host_id = ?\n");

        List<Object> parameters = new ArrayList<>();
        parameters.add(hostId);

        StringBuilder whereClause = new StringBuilder();

        addCondition(whereClause, parameters, "app_id", appId);
        addCondition(whereClause, parameters, "app_name", appName);
        addCondition(whereClause, parameters, "app_desc", appDesc);
        addCondition(whereClause, parameters, "is_kafka_app", isKafkaApp);
        addCondition(whereClause, parameters, "operation_owner", operationOwner);
        addCondition(whereClause, parameters, "delivery_owner", deliveryOwner);

        if (whereClause.length() > 0) {
            sqlBuilder.append("AND ").append(whereClause);
        }

        sqlBuilder.append(" ORDER BY app_id\n" +
                "LIMIT ? OFFSET ?");

        parameters.add(limit);
        parameters.add(offset);

        String sql = sqlBuilder.toString();
        int total = 0;
        List<Map<String, Object>> apps = new ArrayList<>();

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
                    map.put("appId", resultSet.getString("app_id"));
                    map.put("appName", resultSet.getString("app_name"));
                    map.put("appDesc", resultSet.getString("app_desc"));
                    map.put("isKafkaApp", resultSet.getBoolean("is_kafka_app"));
                    map.put("operationOwner", resultSet.getString("operation_owner"));
                    map.put("deliveryOwner", resultSet.getString("delivery_owner"));
                    apps.add(map);
                }
            }

            Map<String, Object> resultMap = new HashMap<>();
            resultMap.put("total", total);
            resultMap.put("apps", apps);
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
    public Result<String> queryClient(int offset, int limit, String hostId, String appId, String apiId,
                                      String clientId, String clientName,
                                      String clientType, String clientProfile, String clientScope,
                                      String customClaim, String redirectUri, String authenticateClass,
                                      String deRefClientId) {
        Result<String> result = null;
        StringBuilder sqlBuilder = new StringBuilder();
        sqlBuilder.append("SELECT COUNT(*) OVER () AS total,\n" +
                "client_id, host_id, app_id, api_id, client_name, client_type, client_profile, " +
                "client_scope, custom_claim, " +
                "redirect_uri, authenticate_class, deref_client_id, update_user, update_ts\n" +
                "FROM auth_client_t\n" +
                "WHERE host_id = ?\n");

        List<Object> parameters = new ArrayList<>();
        parameters.add(hostId);

        StringBuilder whereClause = new StringBuilder();

        addCondition(whereClause, parameters, "app_id", appId);
        addCondition(whereClause, parameters, "api_id", apiId);
        addCondition(whereClause, parameters, "client_id", clientId);
        addCondition(whereClause, parameters, "client_name", clientName);
        addCondition(whereClause, parameters, "client_type", clientType);
        addCondition(whereClause, parameters, "client_profile", clientProfile);
        addCondition(whereClause, parameters, "client_scope", clientScope);
        addCondition(whereClause, parameters, "custom_claim", customClaim);
        addCondition(whereClause, parameters, "redirect_uri", redirectUri);
        addCondition(whereClause, parameters, "authenticate_class", authenticateClass);
        addCondition(whereClause, parameters, "deref_client_id", deRefClientId);

        if (whereClause.length() > 0) {
            sqlBuilder.append("AND ").append(whereClause);
        }

        sqlBuilder.append(" ORDER BY client_id\n" +
                "LIMIT ? OFFSET ?");

        parameters.add(limit);
        parameters.add(offset);

        String sql = sqlBuilder.toString();
        int total = 0;
        List<Map<String, Object>> clients = new ArrayList<>();

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
                    map.put("clientId", resultSet.getString("client_id"));
                    map.put("appId", resultSet.getString("app_id"));
                    map.put("apiId", resultSet.getString("api_id"));
                    map.put("clientName", resultSet.getString("client_name"));
                    map.put("clientType", resultSet.getString("client_type"));
                    map.put("clientProfile", resultSet.getString("client_profile"));
                    map.put("clientScope", resultSet.getString("client_scope"));
                    map.put("customClaim", resultSet.getString("custom_claim"));
                    map.put("redirectUri", resultSet.getString("redirect_uri"));
                    map.put("authenticateClass", resultSet.getString("authenticate_class"));
                    map.put("deRefClientId", resultSet.getString("deref_client_id"));
                    map.put("updateUser", resultSet.getString("update_user"));
                    map.put("updateTs", resultSet.getTimestamp("update_ts"));
                    clients.add(map);
                }
            }

            Map<String, Object> resultMap = new HashMap<>();
            resultMap.put("total", total);
            resultMap.put("clients", clients);
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
    public Result<String> createApp(AppCreatedEvent event) {
        final String sql = "INSERT INTO app_t(host_id, app_id, app_name, app_desc, is_kafka_app, operation_owner, delivery_owner, update_user, update_ts) " +
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)";
        Result<String> result;
        Timestamp timestamp = new Timestamp(event.getEventId().getTimestamp());
        String value = event.getValue();
        Map<String, Object> map = JsonMapper.string2Map(value);

        try (Connection conn = ds.getConnection()) {
            conn.setAutoCommit(false);
            try (PreparedStatement statement = conn.prepareStatement(sql)) {
                statement.setString(1, event.getEventId().getHostId());
                statement.setString(2, event.getAppId());
                statement.setString(3, event.getAppName());
                if (map.containsKey("appDesc")) {
                    statement.setString(4, (String) map.get("appDesc"));
                } else {
                    statement.setNull(4, Types.VARCHAR);
                }
                if (map.containsKey("isKafkaApp")) {
                    statement.setBoolean(5, (Boolean) map.get("isKafkaApp"));
                } else {
                    statement.setNull(5, Types.BOOLEAN);
                }
                if (map.containsKey("operationOwner")) {
                    statement.setString(6, (String) map.get("operationOwner"));
                } else {
                    statement.setNull(6, Types.VARCHAR);
                }
                if (map.containsKey("deliveryOwner")) {
                    statement.setString(7, (String) map.get("deliveryOwner"));
                } else {
                    statement.setNull(7, Types.VARCHAR);
                }
                statement.setString(8, event.getEventId().getId());
                statement.setTimestamp(9, timestamp);

                int count = statement.executeUpdate();
                if (count == 0) {
                    throw new SQLException("failed to insert the app with id " + event.getAppId());
                }
                conn.commit();
                result = Success.of(event.getAppId());
                insertNotification(event.getEventId(), event.getClass().getName(), AvroConverter.toJson(event, false), true, null);

            }   catch (SQLException e) {
                logger.error("SQLException:", e);
                conn.rollback();
                insertNotification(event.getEventId(), event.getClass().getName(), AvroConverter.toJson(event, false), false, e.getMessage());
                result = Failure.of(new Status(SQL_EXCEPTION, e.getMessage()));
            }  catch (Exception e) {
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
    public Result<String> updateApp(AppUpdatedEvent event) {
        final String sql = "UPDATE app_t SET app_name = ?, app_desc = ?, is_kafka_app = ?, operation_owner = ?, delivery_owner = ?, update_user = ?, update_ts = ? " +
                "WHERE host_id = ? and app_id = ?";
        Result<String> result;
        Timestamp timestamp = new Timestamp(event.getEventId().getTimestamp());
        String value = event.getValue();
        Map<String, Object> map = JsonMapper.string2Map(value);

        try (Connection conn = ds.getConnection()) {
            conn.setAutoCommit(false);
            try (PreparedStatement statement = conn.prepareStatement(sql)) {
                statement.setString(1, event.getAppName());

                if (map.containsKey("appDesc")) {
                    statement.setString(2, (String) map.get("appDesc"));
                } else {
                    statement.setNull(2, Types.VARCHAR);
                }

                if (map.containsKey("isKafkaApp")) {
                    statement.setBoolean(3, (Boolean) map.get("isKafkaApp"));
                } else {
                    statement.setNull(3, Types.BOOLEAN);
                }

                if (map.containsKey("operationOwner")) {
                    statement.setString(4, (String) map.get("operationOwner"));
                } else {
                    statement.setNull(4, Types.VARCHAR);
                }
                if (map.containsKey("deliveryOwner")) {
                    statement.setString(5, (String) map.get("deliveryOwner"));
                } else {
                    statement.setNull(5, Types.VARCHAR);
                }
                statement.setString(6, event.getEventId().getId());
                statement.setTimestamp(7, timestamp);
                statement.setString(8, event.getEventId().getHostId());
                statement.setString(9, event.getAppId());

                int count = statement.executeUpdate();
                if (count == 0) {
                    throw new SQLException("failed to update the app with id " + event.getAppId());
                }
                conn.commit();
                result = Success.of(event.getAppId());
                insertNotification(event.getEventId(), event.getClass().getName(), AvroConverter.toJson(event, false), true, null);

            }  catch (SQLException e) {
                logger.error("SQLException:", e);
                conn.rollback();
                insertNotification(event.getEventId(), event.getClass().getName(), AvroConverter.toJson(event, false), false, e.getMessage());
                result = Failure.of(new Status(SQL_EXCEPTION, e.getMessage()));
            }   catch (Exception e) {
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
    public Result<String> deleteApp(AppDeletedEvent event) {
        final String sql = "DELETE FROM app_t WHERE host_id = ? AND app_id = ?";
        Result<String> result;

        try (Connection conn = ds.getConnection()) {
            conn.setAutoCommit(false);
            try (PreparedStatement statement = conn.prepareStatement(sql)) {
                statement.setString(1, event.getEventId().getHostId());
                statement.setString(2, event.getAppId());

                int count = statement.executeUpdate();
                if (count == 0) {
                    throw new SQLException("failed to delete the app with id " + event.getAppId());
                }
                conn.commit();
                result = Success.of(event.getAppId());
                insertNotification(event.getEventId(), event.getClass().getName(), AvroConverter.toJson(event, false), true, null);
            }  catch (SQLException e) {
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
    public Result<String> getAppIdLabel(String hostId) {
        Result<String> result = null;
        String sql = "SELECT app_id, app_name FROM app_t WHERE host_id = ?";
        List<Map<String, Object>> labels = new ArrayList<>();
        try (Connection connection = ds.getConnection();
             PreparedStatement preparedStatement = connection.prepareStatement(sql)) {
            preparedStatement.setString(1, hostId);
            try (ResultSet resultSet = preparedStatement.executeQuery()) {
                while (resultSet.next()) {
                    Map<String, Object> map = new HashMap<>();
                    map.put("id", resultSet.getString("app_id"));
                    map.put("label", resultSet.getString("app_name"));
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
    public Result<String> createClient(ClientCreatedEvent event) {
        final String insertUser = "INSERT INTO auth_client_t (host_id, app_id, api_id, client_name, client_id, " +
                "client_type, client_profile, client_secret, client_scope, custom_claim, redirect_uri, " +
                "authenticate_class, deref_client_id, update_user, update_ts) " +
                "VALUES (?, ?, ?, ?, ?,   ?, ?, ?, ?, ?,   ?, ?, ?, ?, ?)";
        Result<String> result = null;
        Map<String, Object> map = JsonMapper.string2Map(event.getValue());
        try (Connection conn = ds.getConnection()) {
            conn.setAutoCommit(false);
            // no duplicate record, insert the user into database and write a success notification.
            try (PreparedStatement statement = conn.prepareStatement(insertUser)) {
                statement.setString(1, event.getEventId().getHostId());
                if (map.get("appId") != null) {
                    statement.setString(2, (String) map.get("appId"));
                } else {
                    statement.setNull(2, NULL);
                }
                if (map.get("apiId") != null) {
                    statement.setString(3, (String) map.get("apiId"));
                } else {
                    statement.setNull(3, NULL);
                }
                statement.setString(4, event.getClientName());
                statement.setString(5, event.getClientId());
                statement.setString(6, event.getClientType());
                statement.setString(7, event.getClientProfile());
                statement.setString(8, event.getClientSecret());
                if (map.get("clientScope") != null) {
                    statement.setString(9, (String) map.get("clientScope"));
                } else {
                    statement.setNull(9, NULL);
                }
                if (map.get("customClaim") != null) {
                    statement.setString(10, (String) map.get("customClaim"));
                } else {
                    statement.setNull(10, NULL);
                }
                if (map.get("redirectUri") != null) {
                    statement.setString(11, (String) map.get("redirectUri"));
                } else {
                    statement.setNull(11, NULL);
                }
                if (map.get("authenticateClass") != null) {
                    statement.setString(12, (String) map.get("authenticateClass"));
                } else {
                    statement.setNull(12, NULL);
                }
                if (map.get("deRefClientId") != null) {
                    statement.setString(13, (String) map.get("deRefClientId"));
                } else {
                    statement.setNull(13, NULL);
                }
                statement.setString(14, event.getEventId().getId());
                statement.setTimestamp(15, new Timestamp(System.currentTimeMillis()));
                int count = statement.executeUpdate();
                if (count == 0) {
                    throw new SQLException(String.format("no record is inserted for client %s", event.getClientId()));
                }
                conn.commit();
                result = Success.of(event.getClientId());
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
    public Result<String> updateClient(ClientUpdatedEvent event) {
        final String updateApplication = "UPDATE auth_client_t SET app_id = ?, api_id = ?, client_name = ?, " +
                "client_type = ?, client_profile = ?, " +
                "client_scope = ?, custom_claim = ?, redirect_uri = ?, authenticate_class = ?, " +
                "deref_client_id = ?, update_user = ?, update_ts = ? " +
                "WHERE host_id = ? AND client_id = ?";

        Result<String> result = null;
        Map<String, Object> map = JsonMapper.string2Map(event.getValue());
        try (Connection conn = ds.getConnection()) {
            conn.setAutoCommit(false);

            try (PreparedStatement statement = conn.prepareStatement(updateApplication)) {
                if (map.get("appId") != null) {
                    statement.setString(1, (String) map.get("appId"));
                } else {
                    statement.setNull(1, NULL);
                }
                if (map.get("apiId") != null) {
                    statement.setString(2, (String) map.get("apiId"));
                } else {
                    statement.setNull(2, NULL);
                }
                statement.setString(3, event.getClientName());
                statement.setString(4, event.getClientType());
                statement.setString(5, event.getClientProfile());
                if (map.get("clientScope") != null) {
                    statement.setString(6, (String) map.get("clientScope"));
                } else {
                    statement.setNull(6, NULL);
                }
                if (map.get("customClaim") != null) {
                    statement.setString(7, (String) map.get("customClaim"));
                } else {
                    statement.setNull(7, NULL);
                }
                if (map.get("redirectUri") != null) {
                    statement.setString(8, (String) map.get("redirectUri"));
                } else {
                    statement.setNull(8, NULL);
                }
                if (map.get("authenticateClass") != null) {
                    statement.setString(9, (String) map.get("authenticateClass"));
                } else {
                    statement.setNull(9, NULL);
                }
                if (map.get("deRefClientId") != null) {
                    statement.setString(10, (String) map.get("deRefClientId"));
                } else {
                    statement.setNull(10, NULL);
                }
                statement.setString(11, event.getEventId().getId());
                statement.setTimestamp(12, new Timestamp(System.currentTimeMillis()));
                statement.setString(13, event.getEventId().getHostId());
                statement.setString(14, event.getClientId());
                int count = statement.executeUpdate();
                if (count == 0) {
                    // no record is updated, write an error notification.
                    throw new SQLException(String.format("no record is updated for client %s", event.getClientId()));
                }
                conn.commit();
                result = Success.of(event.getClientId());
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
    public Result<String> deleteClient(ClientDeletedEvent event) {
        final String deleteApp = "DELETE from auth_client_t WHERE host_id = ? AND client_id = ?";
        Result<String> result;
        try (Connection conn = ds.getConnection()) {
            conn.setAutoCommit(false);
            try (PreparedStatement statement = conn.prepareStatement(deleteApp)) {
                statement.setString(1, event.getEventId().getHostId());
                statement.setString(2, event.getClientId());
                int count = statement.executeUpdate();
                if (count == 0) {
                    // no record is deleted, write an error notification.
                    throw new SQLException(String.format("no record is deleted for client %s", event.getClientId()));
                }
                conn.commit();
                result = Success.of(event.getClientId());
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
                "SELECT host_id, app_id, api_id, client_name, client_id, client_type, client_profile, client_secret, " +
                        "client_scope, custom_claim,\n" +
                        "redirect_uri, authenticate_class, deref_client_id, update_user, update_ts\n" +
                        "FROM auth_client_t \n" +
                        "WHERE client_id = ?";
        try (final Connection conn = ds.getConnection()) {
            Map<String, Object> map = new HashMap<>();
            try (PreparedStatement statement = conn.prepareStatement(sql)) {
                statement.setString(1, clientId);
                try (ResultSet resultSet = statement.executeQuery()) {
                    if (resultSet.next()) {
                        map.put("hostId", resultSet.getString("host_id"));
                        map.put("appId", resultSet.getString("app_id"));
                        map.put("apiId", resultSet.getString("api_id"));
                        map.put("clientName", resultSet.getString("client_name"));
                        map.put("clientId", resultSet.getString("client_id"));
                        map.put("clientType", resultSet.getString("client_type"));
                        map.put("clientProfile", resultSet.getString("client_profile"));
                        map.put("clientSecret", resultSet.getString("client_secret"));
                        map.put("clientScope", resultSet.getString("client_scope"));
                        map.put("customClaim", resultSet.getString("custom_claim"));
                        map.put("redirectUri", resultSet.getString("redirect_uri"));
                        map.put("authenticateClass", resultSet.getString("authenticate_class"));
                        map.put("deRefClientId", resultSet.getString("deref_client_id"));
                        map.put("updateUser", resultSet.getString("update_user"));
                        map.put("updateTs", resultSet.getTimestamp("update_ts"));
                    }
                }
            }
            if (map.isEmpty())
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
    public Result<String> queryClientByProviderClientId(String providerId, String clientId) {
        Result<String> result;
        String sql =
                "SELECT c.host_id, a.provider_id, a.client_id, c.client_type, c.client_profile, c.client_secret, \n" +
                        "c.client_scope, c.custom_claim, c.redirect_uri, c.authenticate_class, c.deref_client_id\n" +
                        "FROM auth_client_t c, auth_provider_client_t a\n" +
                        "WHERE c.host_id = a.host_id AND c.client_id = a.client_id\n" +
                        "AND a.provider_id = ?\n" +
                        "AND a.client_id = ?\n";
        try (final Connection conn = ds.getConnection()) {
            Map<String, Object> map = new HashMap<>();
            try (PreparedStatement statement = conn.prepareStatement(sql)) {
                statement.setString(1, providerId);
                statement.setString(2, clientId);

                try (ResultSet resultSet = statement.executeQuery()) {
                    if (resultSet.next()) {
                        map.put("hostId", resultSet.getString("host_id"));
                        map.put("providerId", resultSet.getString("provider_id"));
                        map.put("clientId", resultSet.getString("client_id"));
                        map.put("clientType", resultSet.getString("client_type"));
                        map.put("clientProfile", resultSet.getString("client_profile"));
                        map.put("clientSecret", resultSet.getString("client_secret"));
                        map.put("clientScope", resultSet.getString("client_scope"));
                        map.put("customClaim", resultSet.getString("custom_claim"));
                        map.put("redirectUri", resultSet.getString("redirect_uri"));
                        map.put("authenticateClass", resultSet.getString("authenticate_class"));
                        map.put("deRefClientId", resultSet.getString("deref_client_id"));
                    }
                }
            }
            if (map.isEmpty())
                result = Failure.of(new Status(OBJECT_NOT_FOUND, "client", "providerId " +  providerId + "clientId " + clientId));
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
    public Result<Map<String, Object>> queryClientByHostAppId(String host_id, String appId) {
        Result<Map<String, Object>> result;
        String sql =
                "SELECT host_id, app_id, client_id, client_type, client_profile, client_scope, custom_claim, \n" +
                        "redirect_uri, authenticate_class, deref_client_id, update_user, update_ts \n" +
                        "FROM auth_client_t c\n" +
                        "WHERE host_id = ? AND app_id = ?";
        try (final Connection conn = ds.getConnection()) {
            Map<String, Object> map = new HashMap<>();
            try (PreparedStatement statement = conn.prepareStatement(sql)) {
                statement.setString(1, host_id);
                statement.setString(2, appId);
                try (ResultSet resultSet = statement.executeQuery()) {
                    if (resultSet.next()) {
                        map.put("hostId", resultSet.getString("host_id"));
                        map.put("appId", resultSet.getString("app_id"));
                        map.put("clientId", resultSet.getString("client_id"));
                        map.put("clientType", resultSet.getString("client_type"));
                        map.put("clientProfile", resultSet.getString("client_profile"));
                        map.put("clientScope", resultSet.getString("client_scope"));
                        map.put("customClaim", resultSet.getString("custom_claim"));
                        map.put("redirectUri", resultSet.getString("redirect_uri"));
                        map.put("authenticateClass", resultSet.getString("authenticate_class"));
                        map.put("deRefClientId", resultSet.getString("deref_client_id"));
                        map.put("updateUser", resultSet.getString("update_user"));
                        map.put("updateTs", resultSet.getTimestamp("update_ts"));
                    }
                }
            }
            if (map.size() == 0)
                result = Failure.of(new Status(OBJECT_NOT_FOUND, "client with appId ", appId));
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
    public Result<String> createService(ServiceCreatedEvent event) {
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
                statement.setString(3, event.getApiName());
                if (map.containsKey("apiDesc")) {
                    String apiDesc = (String) map.get("apiDesc");
                    if (apiDesc != null && !apiDesc.trim().isEmpty()) {
                        statement.setString(4, apiDesc);
                    } else {
                        statement.setNull(4, Types.VARCHAR);
                    }
                } else {
                    statement.setNull(4, Types.VARCHAR);
                }
                if (map.containsKey("operationOwner")) {
                    String operationOwner = (String) map.get("operationOwner");
                    if(operationOwner != null && !operationOwner.trim().isEmpty()) {
                        statement.setString(5, operationOwner);
                    } else {
                        statement.setNull(5, Types.VARCHAR);
                    }
                } else {
                    statement.setNull(5, Types.VARCHAR);
                }
                if (map.containsKey("deliveryOwner")) {
                    String deliveryOwner = (String) map.get("deliveryOwner");
                    if(deliveryOwner != null && !deliveryOwner.trim().isEmpty()) {
                        statement.setString(6, deliveryOwner);
                    } else {
                        statement.setNull(6, Types.VARCHAR);
                    }
                } else {
                    statement.setNull(6, Types.VARCHAR);
                }
                if (map.containsKey("region")) {
                    String region = (String) map.get("region");
                    if(region != null && !region.trim().isEmpty()) {
                        statement.setString(7, region);
                    } else {
                        statement.setNull(7, Types.VARCHAR);
                    }
                } else {
                    statement.setNull(7, Types.VARCHAR);
                }
                if (map.containsKey("businessGroup")) {
                    String businessGroup = (String) map.get("businessGroup");
                    if(businessGroup != null && !businessGroup.trim().isEmpty()) {
                        statement.setString(8, businessGroup);
                    } else {
                        statement.setNull(8, Types.VARCHAR);
                    }
                } else {
                    statement.setNull(8, Types.VARCHAR);
                }
                if (map.containsKey("lob")) {
                    String lob = (String) map.get("lob");
                    if(lob != null && !lob.trim().isEmpty()) {
                        statement.setString(9, lob);
                    } else {
                        statement.setNull(9, Types.VARCHAR);
                    }
                } else {
                    statement.setNull(9, Types.VARCHAR);
                }
                if (map.containsKey("platform")) {
                    String platform = (String) map.get("platform");
                    if(platform != null && !platform.trim().isEmpty()) {
                        statement.setString(10, platform);
                    } else {
                        statement.setNull(10, Types.VARCHAR);
                    }
                } else {
                    statement.setNull(10, Types.VARCHAR);
                }
                if (map.containsKey("capability")) {
                    String capability = (String) map.get("capability");
                    if(capability != null && !capability.trim().isEmpty()) {
                        statement.setString(11, capability);
                    } else {
                        statement.setNull(11, Types.VARCHAR);
                    }
                } else {
                    statement.setNull(11, Types.VARCHAR);
                }
                if (map.containsKey("gitRepo")) {
                    String gitRepo = (String) map.get("gitRepo");
                    if(gitRepo != null && !gitRepo.trim().isEmpty()) {
                        statement.setString(12, gitRepo);
                    } else {
                        statement.setNull(12, Types.VARCHAR);
                    }
                } else {
                    statement.setNull(12, Types.VARCHAR);
                }
                if (map.containsKey("apiTags")) {
                    String apiTags = (String) map.get("apiTags");
                    if(apiTags != null && !apiTags.trim().isEmpty()) {
                        statement.setString(13, apiTags);
                    } else {
                        statement.setNull(13, Types.VARCHAR);
                    }
                } else {
                    statement.setNull(13, Types.VARCHAR);
                }

                statement.setString(14, event.getApiStatus());
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
    public Result<String> updateService(ServiceUpdatedEvent event) {
        final String updateApi = "UPDATE api_t SET api_name = ?, api_desc = ? " +
                "operation_owner = ?, delivery_owner = ?, region = ?, business_group = ?, lob = ?, platform = ?, " +
                "capability = ?, git_repo = ?, api_tags = ?, api_status = ?,  update_user = ?, update_ts = ? " +
                "WHERE host_id = ? AND api_id = ?";

        Result<String> result = null;
        Map<String, Object> map = JsonMapper.string2Map(event.getValue());
        try (Connection conn = ds.getConnection()) {
            conn.setAutoCommit(false);

            try (PreparedStatement statement = conn.prepareStatement(updateApi)) {
                statement.setString(1, event.getApiName());
                if (map.containsKey("apiDesc")) {
                    String apiDesc = (String) map.get("apiDesc");
                    if (apiDesc != null && !apiDesc.trim().isEmpty()) {
                        statement.setString(2, apiDesc);
                    } else {
                        statement.setNull(2, Types.VARCHAR);
                    }
                } else {
                    statement.setNull(2, Types.VARCHAR);
                }
                if (map.containsKey("operationOwner")) {
                    String operationOwner = (String) map.get("operationOwner");
                    if(operationOwner != null && !operationOwner.trim().isEmpty()) {
                        statement.setString(3, operationOwner);
                    } else {
                        statement.setNull(3, Types.VARCHAR);
                    }
                } else {
                    statement.setNull(3, Types.VARCHAR);
                }
                if (map.containsKey("deliveryOwner")) {
                    String deliveryOwner = (String) map.get("deliveryOwner");
                    if(deliveryOwner != null && !deliveryOwner.trim().isEmpty()) {
                        statement.setString(4, deliveryOwner);
                    } else {
                        statement.setNull(4, Types.VARCHAR);
                    }
                } else {
                    statement.setNull(4, Types.VARCHAR);
                }
                if (map.containsKey("region")) {
                    String region = (String) map.get("region");
                    if(region != null && !region.trim().isEmpty()) {
                        statement.setString(5, region);
                    } else {
                        statement.setNull(5, Types.VARCHAR);
                    }
                } else {
                    statement.setNull(5, Types.VARCHAR);
                }
                if (map.containsKey("businessGroup")) {
                    String businessGroup = (String) map.get("businessGroup");
                    if(businessGroup != null && !businessGroup.trim().isEmpty()) {
                        statement.setString(6, businessGroup);
                    } else {
                        statement.setNull(6, Types.VARCHAR);
                    }
                } else {
                    statement.setNull(6, Types.VARCHAR);
                }
                if (map.containsKey("lob")) {
                    String lob = (String) map.get("lob");
                    if(lob != null && !lob.trim().isEmpty()) {
                        statement.setString(7, lob);
                    } else {
                        statement.setNull(7, Types.VARCHAR);
                    }
                } else {
                    statement.setNull(7, Types.VARCHAR);
                }
                if (map.containsKey("platform")) {
                    String platform = (String) map.get("platform");
                    if(platform != null && !platform.trim().isEmpty()) {
                        statement.setString(8, platform);
                    } else {
                        statement.setNull(8, Types.VARCHAR);
                    }
                } else {
                    statement.setNull(8, Types.VARCHAR);
                }
                if (map.containsKey("capability")) {
                    String capability = (String) map.get("capability");
                    if(capability != null && !capability.trim().isEmpty()) {
                        statement.setString(9, capability);
                    } else {
                        statement.setNull(9, Types.VARCHAR);
                    }
                } else {
                    statement.setNull(9, Types.VARCHAR);
                }
                if (map.containsKey("gitRepo")) {
                    String gitRepo = (String) map.get("gitRepo");
                    if(gitRepo != null && !gitRepo.trim().isEmpty()) {
                        statement.setString(10, gitRepo);
                    } else {
                        statement.setNull(10, Types.VARCHAR);
                    }
                } else {
                    statement.setNull(10, Types.VARCHAR);
                }
                if (map.containsKey("apiTags")) {
                    String apiTags = (String) map.get("apiTags");
                    if(apiTags != null && !apiTags.trim().isEmpty()) {
                        statement.setString(11, apiTags);
                    } else {
                        statement.setNull(11, Types.VARCHAR);
                    }
                } else {
                    statement.setNull(11, Types.VARCHAR);
                }
                statement.setString(12, event.getApiStatus());
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
    public Result<String> deleteService(ServiceDeletedEvent event) {
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
    public Result<String> createServiceVersion(ServiceVersionCreatedEvent event, List<Map<String, Object>> endpoints) {
        final String insertUser = "INSERT INTO api_version_t (host_id, api_id, api_version, api_type, service_id, api_version_desc, " +
                "spec_link, spec, update_user, update_ts) " +
                "VALUES (?, ?, ?, ?, ?,   ?, ?, ?, ?, ?)";
        final String insertEndpoint = "INSERT INTO api_endpoint_t (host_id, api_id, api_version, endpoint, http_method, " +
                "endpoint_path, endpoint_name, endpoint_desc, update_user, update_ts) " +
                "VALUES (?,? ,?, ?, ?,  ?, ?, ?, ?, ?)";
        final String insertScope = "INSERT INTO api_endpoint_scope_t (host_id, api_id, api_version, endpoint, scope, scope_desc, " +
                "update_user, update_ts) " +
                "VALUES (?, ?, ?, ?, ?,  ?, ?, ?)";

        Result<String> result = null;
        Map<String, Object> map = JsonMapper.string2Map(event.getValue());
        try (Connection conn = ds.getConnection()) {
            conn.setAutoCommit(false);
            // no duplicate record, insert the user into database and write a success notification.
            try (PreparedStatement statement = conn.prepareStatement(insertUser)) {
                statement.setString(1, event.getHostId());
                statement.setString(2, event.getApiId());
                statement.setString(3, event.getApiVersion());
                statement.setString(4, event.getApiType());
                statement.setString(5, event.getServiceId());

                if (map.containsKey("apiVersionDesc")) {
                    String apiDesc = (String) map.get("apiVersionDesc");
                    if (apiDesc != null && !apiDesc.trim().isEmpty()) {
                        statement.setString(6, apiDesc);
                    } else {
                        statement.setNull(6, Types.VARCHAR);
                    }
                } else {
                    statement.setNull(6, Types.VARCHAR);
                }
                if (map.containsKey("specLink")) {
                    String specLink = (String) map.get("specLink");
                    if (specLink != null && !specLink.trim().isEmpty()) {
                        statement.setString(7, specLink);
                    } else {
                        statement.setNull(7, Types.VARCHAR);
                    }
                } else {
                    statement.setNull(7, Types.VARCHAR);
                }
                if (map.containsKey("spec")) {
                    String spec = (String) map.get("spec");
                    if (spec != null && !spec.trim().isEmpty()) {
                        statement.setString(8, spec);
                    } else {
                        statement.setNull(8, Types.VARCHAR);
                    }
                } else {
                    statement.setNull(8, Types.VARCHAR);
                }
                statement.setString(9, event.getEventId().getId());
                statement.setTimestamp(10, new Timestamp(System.currentTimeMillis()));
                int count = statement.executeUpdate();
                if (count == 0) {
                    throw new SQLException(String.format("no record is inserted for api version %s", "hostId " + event.getHostId() + " apiId " + event.getApiId() + " apiVersion " + event.getApiVersion()));
                }
                if(endpoints != null && !endpoints.isEmpty()) {
                    // insert endpoints
                    for (Map<String, Object> endpoint : endpoints) {
                        try (PreparedStatement statementInsert = conn.prepareStatement(insertEndpoint)) {
                            statementInsert.setString(1, event.getHostId());
                            statementInsert.setString(2, event.getApiId());
                            statementInsert.setString(3, event.getApiVersion());
                            statementInsert.setString(4, (String) endpoint.get("endpoint"));

                            if (endpoint.get("httpMethod") == null)
                                statementInsert.setNull(5, NULL);
                            else
                                statementInsert.setString(5, ((String) endpoint.get("httpMethod")).toLowerCase().trim());

                            if (endpoint.get("endpointPath") == null)
                                statementInsert.setNull(6, NULL);
                            else
                                statementInsert.setString(6, (String) endpoint.get("endpointPath"));

                            if (endpoint.get("endpointName") == null)
                                statementInsert.setNull(7, NULL);
                            else
                                statementInsert.setString(7, (String) endpoint.get("endpointName"));

                            if (endpoint.get("endpointDesc") == null)
                                statementInsert.setNull(8, NULL);
                            else
                                statementInsert.setString(8, (String) endpoint.get("endpointDesc"));

                            statementInsert.setString(9, event.getEventId().getId());
                            statementInsert.setTimestamp(10, new Timestamp(event.getEventId().getTimestamp()));
                            statementInsert.executeUpdate();
                        }
                        // insert scopes
                        List<String> scopes = (List<String>) endpoint.get("scopes");
                        if(scopes != null) {
                            for (String scope : scopes) {
                                String[] scopeDesc = scope.split(":");
                                try (PreparedStatement statementScope = conn.prepareStatement(insertScope)) {
                                    statementScope.setString(1, event.getHostId());
                                    statementScope.setString(2, event.getApiId());
                                    statementScope.setString(3, event.getApiVersion());
                                    statementScope.setString(4, (String) endpoint.get("endpoint"));
                                    statementScope.setString(5, scopeDesc[0]);
                                    if (scopeDesc.length == 1)
                                        statementScope.setNull(6, NULL);
                                    else
                                        statementScope.setString(6, scopeDesc[1]);
                                    statementScope.setString(7, event.getEventId().getId());
                                    statementScope.setTimestamp(8, new Timestamp(event.getEventId().getTimestamp()));
                                    statementScope.executeUpdate();
                                }
                            }
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
    public Result<String> updateServiceVersion(ServiceVersionUpdatedEvent event, List<Map<String, Object>> endpoints) {
        final String updateApi = "UPDATE api_version_t SET api_type = ?, service_id = ?, api_version_desc = ?, spec_link = ?,  spec = ?," +
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
        Map<String, Object> map = JsonMapper.string2Map(event.getValue());
        try (Connection conn = ds.getConnection()) {
            conn.setAutoCommit(false);

            try (PreparedStatement statement = conn.prepareStatement(updateApi)) {
                statement.setString(1, event.getApiType());
                statement.setString(2, event.getServiceId());

                if (map.containsKey("apiVersionDesc")) {
                    String apiDesc = (String) map.get("apiVersionDesc");
                    if (apiDesc != null && !apiDesc.trim().isEmpty()) {
                        statement.setString(3, apiDesc);
                    } else {
                        statement.setNull(3, Types.VARCHAR);
                    }
                } else {
                    statement.setNull(3, Types.VARCHAR);
                }
                if (map.containsKey("specLink")) {
                    String specLink = (String) map.get("specLink");
                    if (specLink != null && !specLink.trim().isEmpty()) {
                        statement.setString(4, specLink);
                    } else {
                        statement.setNull(4, Types.VARCHAR);
                    }
                } else {
                    statement.setNull(4, Types.VARCHAR);
                }
                if (map.containsKey("spec")) {
                    String spec = (String) map.get("spec");
                    if (spec != null && !spec.trim().isEmpty()) {
                        statement.setString(5, spec);
                    } else {
                        statement.setNull(5, Types.VARCHAR);
                    }
                } else {
                    statement.setNull(5, Types.VARCHAR);
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
                if(endpoints != null && !endpoints.isEmpty()) {
                    // delete endpoints for the api version. the api_endpoint_scope_t will be deleted by the cascade.
                    try (PreparedStatement statementDelete = conn.prepareStatement(deleteEndpoint)) {
                        statementDelete.setString(1, event.getHostId());
                        statementDelete.setString(2, event.getApiId());
                        statementDelete.setString(3, event.getApiVersion());
                        statementDelete.executeUpdate();
                    }
                    // insert endpoints
                    for (Map<String, Object> endpoint : endpoints) {
                        try (PreparedStatement statementInsert = conn.prepareStatement(insertEndpoint)) {
                            statementInsert.setString(1, event.getHostId());
                            statementInsert.setString(2, event.getApiId());
                            statementInsert.setString(3, event.getApiVersion());
                            statementInsert.setString(4, (String) endpoint.get("endpoint"));

                            if (endpoint.get("httpMethod") == null)
                                statementInsert.setNull(5, NULL);
                            else
                                statementInsert.setString(5, ((String) endpoint.get("httpMethod")).toLowerCase().trim());

                            if (endpoint.get("endpointPath") == null)
                                statementInsert.setNull(6, NULL);
                            else
                                statementInsert.setString(6, (String) endpoint.get("endpointPath"));

                            if (endpoint.get("endpointName") == null)
                                statementInsert.setNull(7, NULL);
                            else
                                statementInsert.setString(7, (String) endpoint.get("endpointName"));

                            if (endpoint.get("endpointDesc") == null)
                                statementInsert.setNull(8, NULL);
                            else
                                statementInsert.setString(8, (String) endpoint.get("endpointDesc"));

                            statementInsert.setString(9, event.getEventId().getId());
                            statementInsert.setTimestamp(10, new Timestamp(event.getEventId().getTimestamp()));
                            statementInsert.executeUpdate();
                        }
                        // insert scopes
                        List<String> scopes = (List<String>) endpoint.get("scopes");
                        if (scopes != null) {
                            for (String scope : scopes) {
                                String[] scopeDesc = scope.split(":");
                                try (PreparedStatement statementScope = conn.prepareStatement(insertScope)) {
                                    statementScope.setString(1, event.getHostId());
                                    statementScope.setString(2, event.getApiId());
                                    statementScope.setString(3, event.getApiVersion());
                                    statementScope.setString(4, (String) endpoint.get("endpoint"));
                                    statementScope.setString(5, scopeDesc[0]);
                                    if (scopeDesc.length == 1)
                                        statementScope.setNull(6, NULL);
                                    else
                                        statementScope.setString(6, scopeDesc[1]);
                                    statementScope.setString(7, event.getEventId().getId());
                                    statementScope.setTimestamp(8, new Timestamp(event.getEventId().getTimestamp()));
                                    statementScope.executeUpdate();
                                }
                            }
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

                        if (endpoint.get("httpMethod") == null)
                            statement.setNull(5, NULL);
                        else
                            statement.setString(5, ((String) endpoint.get("httpMethod")).toLowerCase().trim());

                        if (endpoint.get("endpointPath") == null)
                            statement.setNull(6, NULL);
                        else
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
                    if(scopes != null) {
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
    public Result<String> queryServiceEndpoint(int offset, int limit, String hostId, String apiId, String apiVersion, String endpoint, String method, String path, String desc) {
        Result<String> result = null;
        StringBuilder sqlBuilder = new StringBuilder();
        sqlBuilder.append("SELECT COUNT(*) OVER () AS total,\n" +
                "host_id, api_id, api_version, endpoint, http_method,\n" +
                "endpoint_path, endpoint_desc\n" +
                "FROM api_endpoint_t\n" +
                "WHERE host_id = ? AND api_id = ? AND api_version = ?\n");

        List<Object> parameters = new ArrayList<>();
        parameters.add(hostId);
        parameters.add(apiId);
        parameters.add(apiVersion);

        StringBuilder whereClause = new StringBuilder();
        addCondition(whereClause, parameters, "endpoint", endpoint);
        addCondition(whereClause, parameters, "http_method", method);
        addCondition(whereClause, parameters, "endpoint_path", path);
        addCondition(whereClause, parameters, "endpoint_desc", desc);

        if(whereClause.length() > 0) {
            sqlBuilder.append(" AND ").append(whereClause);
        }


        sqlBuilder.append(" ORDER BY endpoint\n" +
                "LIMIT ? OFFSET ?");

        parameters.add(limit);
        parameters.add(offset);

        String sql = sqlBuilder.toString();
        int total = 0;
        List<Map<String, Object>> endpoints = new ArrayList<>();


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
                    map.put("apiId", resultSet.getString("api_id"));
                    map.put("apiVersion", resultSet.getString("api_version"));
                    map.put("endpoint", resultSet.getString("endpoint"));
                    map.put("httpMethod", resultSet.getString("http_method"));
                    map.put("endpointPath", resultSet.getString("endpoint_path"));
                    map.put("endpointDesc", resultSet.getString("endpoint_desc"));
                    endpoints.add(map);
                }
            }

            Map<String, Object> resultMap = new HashMap<>();
            resultMap.put("total", total);
            resultMap.put("endpoints", endpoints);
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

    @Override
    public Result<String> getServiceIdLabel(String hostId) {
        Result<String> result = null;
        String sql = "SELECT service_id FROM api_version_t WHERE host_id =  ?";
        List<Map<String, Object>> labels = new ArrayList<>();
        try (Connection connection = ds.getConnection();
             PreparedStatement preparedStatement = connection.prepareStatement(sql)) {
            preparedStatement.setString(1, hostId);
            try (ResultSet resultSet = preparedStatement.executeQuery()) {
                while (resultSet.next()) {
                    Map<String, Object> map = new HashMap<>();
                    map.put("id", resultSet.getString("service_id"));
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

    @Override
    public Result<String> createOrg(OrgCreatedEvent event) {
        final String insertOrg = "INSERT INTO org_t (domain, org_name, org_desc, org_owner, update_user, update_ts) " +
                "VALUES (?, ?, ?, ?, ?,  ?)";
        final String insertHost = "INSERT INTO host_t(host_id, domain, sub_domain, host_desc, host_owner, update_user, update_ts) " +
                "VALUES (?, ?, ?, ?, ?, ?, ?)";
        final String insertRole = "INSERT INTO role_t (host_id, role_id, role_desc, update_user, update_ts) " +
                "VALUES (?, ?, ?, ?, ?)";
        final String insertRoleUser = "INSERT INTO role_user_t (host_id, role_id, user_id, update_user, update_ts) " +
                "VALUES (?, ?, ?, ?, ?)";
        final String updateUserHost = "UPDATE user_host_t SET host_id = ?, update_user = ?, update_ts = ? WHERE user_id = ?";


        Result<String> result;
        try (Connection conn = ds.getConnection()) {
            conn.setAutoCommit(false);
            try (PreparedStatement statement = conn.prepareStatement(insertOrg)) {
                statement.setString(1, event.getDomain());
                statement.setString(2, event.getOrgName());
                statement.setString(3, event.getOrgDesc());
                statement.setString(4, event.getOrgOwner());  // org owner is the user id in the eventId
                statement.setString(5, event.getEventId().getUserId());
                statement.setTimestamp(6, new Timestamp(event.getEventId().getTimestamp()));
                int count = statement.executeUpdate();
                if (count == 0) {
                    throw new SQLException("failed to insert the org " + event.getDomain());
                }
                try (PreparedStatement hostStatement = conn.prepareStatement(insertHost)) {
                    hostStatement.setString(1, event.getHostId());
                    hostStatement.setString(2, event.getDomain());
                    hostStatement.setString(3, event.getSubDomain());
                    hostStatement.setString(4, event.getHostDesc());
                    hostStatement.setString(5, event.getHostOwner()); // host owner can be another person selected by the org owner.
                    hostStatement.setString(6, event.getEventId().getUserId());
                    hostStatement.setTimestamp(7, new Timestamp(event.getEventId().getTimestamp()));
                    hostStatement.executeUpdate();
                }
                // create user, org-admin and host-admin roles for the hostId by default.
                try (PreparedStatement roleStatement = conn.prepareStatement(insertRole)) {
                    roleStatement.setString(1, event.getHostId());
                    roleStatement.setString(2, "user");
                    roleStatement.setString(3, "user role");
                    roleStatement.setString(4, event.getEventId().getUserId());
                    roleStatement.setTimestamp(5, new Timestamp(event.getEventId().getTimestamp()));
                    roleStatement.executeUpdate();
                }
                try (PreparedStatement roleStatement = conn.prepareStatement(insertRole)) {
                    roleStatement.setString(1, event.getHostId());
                    roleStatement.setString(2, "org-admin");
                    roleStatement.setString(3, "org-admin role");
                    roleStatement.setString(4, event.getEventId().getUserId());
                    roleStatement.setTimestamp(5, new Timestamp(event.getEventId().getTimestamp()));
                    roleStatement.executeUpdate();
                }
                try (PreparedStatement roleStatement = conn.prepareStatement(insertRole)) {
                    roleStatement.setString(1, event.getHostId());
                    roleStatement.setString(2, "host-admin");
                    roleStatement.setString(3, "host-admin role");
                    roleStatement.setString(4, event.getEventId().getUserId());
                    roleStatement.setTimestamp(5, new Timestamp(event.getEventId().getTimestamp()));
                    roleStatement.executeUpdate();
                }
                // insert role user to user for the host
                try (PreparedStatement roleUserStatement = conn.prepareStatement(insertRoleUser)) {
                    roleUserStatement.setString(1, event.getHostId());
                    roleUserStatement.setString(2, "user");
                    roleUserStatement.setString(3, event.getOrgOwner());
                    roleUserStatement.setString(4, event.getEventId().getUserId());
                    roleUserStatement.setTimestamp(5, new Timestamp(event.getEventId().getTimestamp()));
                    roleUserStatement.executeUpdate();
                }
                // insert role org-admin to user for the host
                try (PreparedStatement roleUserStatement = conn.prepareStatement(insertRoleUser)) {
                    roleUserStatement.setString(1, event.getHostId());
                    roleUserStatement.setString(2, "org-admin");
                    roleUserStatement.setString(3, event.getOrgOwner());
                    roleUserStatement.setString(4, event.getEventId().getUserId());
                    roleUserStatement.setTimestamp(5, new Timestamp(event.getEventId().getTimestamp()));
                    roleUserStatement.executeUpdate();
                }
                // insert host-admin to user for the host
                try (PreparedStatement roleUserStatement = conn.prepareStatement(insertRoleUser)) {
                    roleUserStatement.setString(1, event.getHostId());
                    roleUserStatement.setString(2, "host-admin");
                    roleUserStatement.setString(3, event.getHostOwner());
                    roleUserStatement.setString(4, event.getEventId().getUserId());
                    roleUserStatement.setTimestamp(5, new Timestamp(event.getEventId().getTimestamp()));
                    roleUserStatement.executeUpdate();
                }
                // switch the current user to the hostId by updating to same user pointing to two hosts.
                try (PreparedStatement userHostStatement = conn.prepareStatement(updateUserHost)) {
                    userHostStatement.setString(1, event.getHostId());
                    userHostStatement.setString(2, event.getEventId().getUserId());
                    userHostStatement.setTimestamp(3, new Timestamp(event.getEventId().getTimestamp()));
                    userHostStatement.setString(4, event.getOrgOwner());

                    userHostStatement.executeUpdate();
                }
                conn.commit();
                result = Success.of(event.getDomain());
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
    public Result<String> updateOrg(OrgUpdatedEvent event) {
        final String updateHost = "UPDATE org_t SET org_name = ?, org_desc = ?, org_owner = ?, " +
                "update_user = ?, update_ts = ? " +
                "WHERE domain = ?";

        Result<String> result = null;
        try (Connection conn = ds.getConnection()) {
            conn.setAutoCommit(false);
            try (PreparedStatement statement = conn.prepareStatement(updateHost)) {
                if (event.getOrgName() != null) {
                    statement.setString(1, event.getOrgName());
                } else {
                    statement.setNull(1, NULL);
                }
                if (event.getOrgDesc() != null) {
                    statement.setString(2, event.getOrgDesc());
                } else {
                    statement.setNull(2, NULL);
                }
                if (event.getOrgOwner() != null) {
                    statement.setString(3, event.getOrgOwner());
                } else {
                    statement.setNull(3, NULL);
                }
                statement.setString(4, event.getEventId().getUserId());
                statement.setTimestamp(5, new Timestamp(event.getEventId().getTimestamp()));
                statement.setString(6, event.getDomain());

                int count = statement.executeUpdate();
                if (count == 0) {
                    // no record is updated, write an error notification.
                    throw new SQLException("no record is updated for org " + event.getDomain());
                }
                conn.commit();
                result = Success.of(event.getDomain());
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
    public Result<String> deleteOrg(OrgDeletedEvent event) {
        final String deleteHost = "DELETE FROM org_t WHERE domain = ?";
        Result<String> result;
        try (Connection conn = ds.getConnection()) {
            conn.setAutoCommit(false);
            try (PreparedStatement statement = conn.prepareStatement(deleteHost)) {
                statement.setString(1, event.getDomain());
                int count = statement.executeUpdate();
                if (count == 0) {
                    throw new SQLException("no record is deleted for org " + event.getDomain());
                }
                conn.commit();
                result = Success.of(event.getDomain());
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
    public Result<String> createHost(HostCreatedEvent event) {
        final String insertHost = "INSERT INTO host_t (host_id, domain, sub_domain, host_desc, host_owner, update_user, update_ts) " +
                "VALUES (?, ?, ?, ?, ?,  ?, ?)";
        Result<String> result;
        try (Connection conn = ds.getConnection()) {
            conn.setAutoCommit(false);
            try (PreparedStatement statement = conn.prepareStatement(insertHost)) {
                statement.setString(1, event.getHostId());
                statement.setString(2, event.getDomain());
                statement.setString(3, event.getSubDomain());
                statement.setString(4, event.getHostDesc());
                statement.setString(5, event.getHostOwner());
                statement.setString(6, event.getEventId().getId());
                statement.setTimestamp(7, new Timestamp(event.getEventId().getTimestamp()));
                int count = statement.executeUpdate();
                if (count == 0) {
                    throw new SQLException("failed to insert the host " + event.getDomain());
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
        }
        return result;
    }

    @Override
    public Result<String> updateHost(HostUpdatedEvent event) {
        final String updateHost = "UPDATE host_t SET domain = ?, sub_domain = ?, host_desc = ?, host_owner = ?, " +
                "update_user = ?, update_ts = ? " +
                "WHERE host_id = ?";

        Result<String> result = null;
        try (Connection conn = ds.getConnection()) {
            conn.setAutoCommit(false);

            try (PreparedStatement statement = conn.prepareStatement(updateHost)) {
                statement.setString(1, event.getDomain());
                if (event.getSubDomain() != null) {
                    statement.setString(2, event.getSubDomain());
                } else {
                    statement.setNull(2, NULL);
                }
                if (event.getHostDesc() != null) {
                    statement.setString(3, event.getHostDesc());
                } else {
                    statement.setNull(3, NULL);
                }
                if (event.getHostOwner() != null) {
                    statement.setString(4, event.getHostOwner());
                } else {
                    statement.setNull(4, NULL);
                }
                statement.setString(5, event.getEventId().getUserId());
                statement.setTimestamp(6, new Timestamp(event.getEventId().getTimestamp()));
                statement.setString(7, event.getHostId());

                int count = statement.executeUpdate();
                if (count == 0) {
                    // no record is updated, write an error notification.
                    throw new SQLException("no record is updated for host " + event.getHostId());
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
        Result<String> result;
        try (Connection conn = ds.getConnection()) {
            conn.setAutoCommit(false);
            try (PreparedStatement statement = conn.prepareStatement(deleteHost)) {
                statement.setString(1, event.getHostId());
                int count = statement.executeUpdate();
                if (count == 0) {
                    throw new SQLException("no record is deleted for host " + event.getHostId());
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
    public Result<String> switchHost(HostSwitchedEvent event) {
        final String updateUserHost = "UPDATE user_host_t SET host_id = ?, update_user = ?, update_ts = ? WHERE user_id = ?";
        Result<String> result;
        try (Connection conn = ds.getConnection()) {
            conn.setAutoCommit(false);
            try (PreparedStatement statement = conn.prepareStatement(updateUserHost)) {
                statement.setString(1, event.getHostId());
                statement.setString(2, event.getEventId().getUserId());
                statement.setTimestamp(3, new Timestamp(event.getEventId().getTimestamp()));
                statement.setString(4, event.getEventId().getUserId());
                int count = statement.executeUpdate();
                if (count == 0) {
                    throw new SQLException("no record is updated for user " + event.getEventId().getUserId());
                }
                conn.commit();
                result = Success.of(event.getEventId().getUserId());
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
        final String sql = "SELECT sub_domain || '.' || domain AS domain FROM host_t WHERE host_id = ?";
        Result<String> result;
        String domain = null;
        try (final Connection conn = ds.getConnection(); final PreparedStatement statement = conn.prepareStatement(sql)) {
            statement.setString(1, hostId);
            try (ResultSet resultSet = statement.executeQuery()) {
                if (resultSet.next()) {
                    domain = resultSet.getString("domain");
                }
            }
            if (domain == null)
                result = Failure.of(new Status(OBJECT_NOT_FOUND, "host domain", hostId));
            else
                result = Success.of(domain);
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
    public Result<String> queryHostById(String id) {
        final String queryHostById = "SELECT host_id, domain, sub_domain, host_desc, host_owner, " +
                "update_user, update_ts FROM host_t WHERE host_id = ?";
        Result<String> result;
        try (final Connection conn = ds.getConnection()) {
            Map<String, Object> map = new HashMap<>();
            try (PreparedStatement statement = conn.prepareStatement(queryHostById)) {
                statement.setString(1, id);
                try (ResultSet resultSet = statement.executeQuery()) {
                    if (resultSet.next()) {
                        map.put("hostId", resultSet.getString("host_id"));
                        map.put("domain", resultSet.getString("domain"));
                        map.put("subDomain", resultSet.getString("sub_domain"));
                        map.put("hostDesc", resultSet.getString("host_desc"));
                        map.put("hostOwner", resultSet.getString("host_owner"));
                        map.put("updateUser", resultSet.getString("update_user"));
                        map.put("updateTs", resultSet.getTimestamp("update_ts"));
                    }
                }
            }
            if (map.isEmpty())
                result = Failure.of(new Status(OBJECT_NOT_FOUND, "host with id", id));
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
    public Result<String> getOrg(int offset, int limit, String domain, String orgName, String orgDesc, String orgOwner) {
        Result<String> result = null;
        StringBuilder sqlBuilder = new StringBuilder();
        sqlBuilder.append("SELECT COUNT(*) OVER () AS total,\n" +
                "domain, org_name, org_desc, org_owner, update_user, update_ts \n" +
                "FROM org_t\n" +
                "WHERE 1=1\n");

        List<Object> parameters = new ArrayList<>();

        StringBuilder whereClause = new StringBuilder();

        addCondition(whereClause, parameters, "domain", domain);
        addCondition(whereClause, parameters, "org_name", orgName);
        addCondition(whereClause, parameters, "org_desc", orgDesc);
        addCondition(whereClause, parameters, "org_owner", orgOwner);

        if (whereClause.length() > 0) {
            sqlBuilder.append("AND ").append(whereClause);
        }

        sqlBuilder.append("ORDER BY domain\n" +
                "LIMIT ? OFFSET ?");

        parameters.add(limit);
        parameters.add(offset);

        String sql = sqlBuilder.toString();
        int total = 0;
        List<Map<String, Object>> orgs = new ArrayList<>();

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
                    map.put("domain", resultSet.getString("domain"));
                    map.put("orgName", resultSet.getString("org_name"));
                    map.put("orgDesc", resultSet.getString("org_desc"));
                    map.put("orgOwner", resultSet.getString("org_owner"));
                    map.put("updateUser", resultSet.getString("update_user"));
                    // handling date properly
                    map.put("updateTs", resultSet.getTimestamp("update_ts") != null ? resultSet.getTimestamp("update_ts").toString() : null);
                    orgs.add(map);
                }
            }


            Map<String, Object> resultMap = new HashMap<>();
            resultMap.put("total", total);
            resultMap.put("orgs", orgs);
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
    public Result<String> getHost(int offset, int limit, String hostId, String domain, String subDomain, String hostDesc, String hostOwner) {
        Result<String> result = null;
        StringBuilder sqlBuilder = new StringBuilder();
        sqlBuilder.append("SELECT COUNT(*) OVER () AS total,\n" +
                "host_id, domain, sub_domain, host_desc, host_owner, update_user, update_ts \n" +
                "FROM host_t\n" +
                "WHERE 1=1\n");


        List<Object> parameters = new ArrayList<>();

        StringBuilder whereClause = new StringBuilder();

        addCondition(whereClause, parameters, "host_id", hostId);
        addCondition(whereClause, parameters, "domain", domain);
        addCondition(whereClause, parameters, "sub_domain", subDomain);
        addCondition(whereClause, parameters, "host_desc", hostDesc);
        addCondition(whereClause, parameters, "host_owner", hostOwner);

        if (whereClause.length() > 0) {
            sqlBuilder.append("AND ").append(whereClause);
        }

        sqlBuilder.append(" ORDER BY domain\n" +
                "LIMIT ? OFFSET ?");


        parameters.add(limit);
        parameters.add(offset);
        String sql = sqlBuilder.toString();
        if(logger.isTraceEnabled()) logger.trace("sql: {}", sql);
        int total = 0;
        List<Map<String, Object>> hosts = new ArrayList<>();

        try (Connection connection = ds.getConnection();
             PreparedStatement preparedStatement = connection.prepareStatement(sql)) {

            for (int i = 0; i < parameters.size(); i++) {
                preparedStatement.setObject(i + 1, parameters.get(i));
            }

            boolean isFirstRow = true;
            try (ResultSet resultSet = preparedStatement.executeQuery()) {
                if(logger.isTraceEnabled()) logger.trace("resultSet: {}", resultSet);
                while (resultSet.next()) {
                    if(logger.isTraceEnabled()) logger.trace("at least there is 1 row here in the resultSet");
                    Map<String, Object> map = new HashMap<>();
                    if (isFirstRow) {
                        total = resultSet.getInt("total");
                        isFirstRow = false;
                    }
                    map.put("hostId", resultSet.getString("host_id"));
                    map.put("domain", resultSet.getString("domain"));
                    map.put("subDomain", resultSet.getString("sub_domain"));
                    map.put("hostDesc", resultSet.getString("host_desc"));
                    map.put("hostOwner", resultSet.getString("host_owner"));
                    map.put("updateUser", resultSet.getString("update_user"));
                    // handling date properly
                    map.put("updateTs", resultSet.getTimestamp("update_ts") != null ? resultSet.getTimestamp("update_ts").toString() : null);
                    hosts.add(map);
                }
            }

            Map<String, Object> resultMap = new HashMap<>();
            resultMap.put("total", total);
            resultMap.put("hosts", hosts);
            if(logger.isTraceEnabled()) logger.trace("resultMap: {}", resultMap);
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
    public Result<String> getHostByDomain(String domain, String subDomain, String hostDesc) {
        Result<String> result = null;
        StringBuilder sqlBuilder = new StringBuilder();
        sqlBuilder.append("SELECT host_id, domain, sub_domain, host_desc, host_owner, update_user, update_ts \n" +
                "FROM host_t\n" +
                "WHERE 1=1\n");

        List<Object> parameters = new ArrayList<>();

        StringBuilder whereClause = new StringBuilder();
        addCondition(whereClause, parameters, "domain", domain);
        addCondition(whereClause, parameters, "sub_domain", subDomain);
        addCondition(whereClause, parameters, "host_desc", hostDesc);

        if (whereClause.length() > 0) {
            sqlBuilder.append("AND ").append(whereClause);
        }

        sqlBuilder.append(" ORDER BY sub_domain");

        String sql = sqlBuilder.toString();
        if(logger.isTraceEnabled()) logger.trace("sql: {}", sql);
        List<Map<String, Object>> hosts = new ArrayList<>();
        try (Connection connection = ds.getConnection();
             PreparedStatement preparedStatement = connection.prepareStatement(sql)) {

            for (int i = 0; i < parameters.size(); i++) {
                preparedStatement.setObject(i + 1, parameters.get(i));
            }

            try (ResultSet resultSet = preparedStatement.executeQuery()) {
                while (resultSet.next()) {
                    Map<String, Object> map = new HashMap<>();
                    map.put("hostId", resultSet.getString("host_id"));
                    map.put("domain", resultSet.getString("domain"));
                    map.put("subDomain", resultSet.getString("sub_domain"));
                    map.put("hostDesc", resultSet.getString("host_desc"));
                    map.put("hostOwner", resultSet.getString("host_owner"));
                    map.put("updateUser", resultSet.getString("update_user"));
                    map.put("updateTs", resultSet.getTimestamp("update_ts") != null ? resultSet.getTimestamp("update_ts").toString() : null);
                    hosts.add(map);
                }
            }

            if(hosts.isEmpty()) {
                result = Failure.of(new Status(OBJECT_NOT_FOUND, "host", "domain, subDomain or hostDesc"));
            } else {
                result = Success.of(JsonMapper.toJson(hosts));
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
    public Result<String> getHostLabel() {
        final String getHostLabel = "SELECT host_id, domain, sub_domain FROM host_t ORDER BY domain, sub_domain";
        Result<String> result;
        List<Map<String, Object>> hosts = new ArrayList<>();
        try (final Connection conn = ds.getConnection()) {
            try (PreparedStatement statement = conn.prepareStatement(getHostLabel)) {
                try (ResultSet resultSet = statement.executeQuery()) {
                    while (resultSet.next()) {
                        Map<String, Object> map = new HashMap<>();
                        map.put("id", resultSet.getString("host_id"));
                        map.put("label", resultSet.getString("sub_domain") + "." + resultSet.getString("domain"));
                        hosts.add(map);
                    }
                }
            }
            if(hosts.isEmpty()) {
                result = Failure.of(new Status(OBJECT_NOT_FOUND, "host", "any key"));
            } else {
                result = Success.of(JsonMapper.toJson(hosts));
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
        final String sql = "INSERT INTO config_t(config_id, config_name, config_phase, config_type, light4j_version, " +
                "class_path, config_desc, update_user, update_ts) " +
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)";
        Result<String> result;
        Timestamp timestamp = new Timestamp(event.getEventId().getTimestamp());
        String value = event.getValue();
        Map<String, Object> map = JsonMapper.string2Map(value);

        try (Connection conn = ds.getConnection()) {
            conn.setAutoCommit(false);
            try (PreparedStatement statement = conn.prepareStatement(sql)) {
                statement.setString(1, event.getConfigId());
                statement.setString(2, event.getConfigName());
                statement.setString(3, event.getConfigPhase());
                statement.setString(4, event.getConfigType());

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
                statement.setString(8, event.getEventId().getId());
                statement.setTimestamp(9, timestamp);


                int count = statement.executeUpdate();
                if (count == 0) {
                    throw new SQLException("failed to insert the config with id " + event.getConfigId());
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
        final String sql = "UPDATE config_t SET config_name = ?, config_phase = ?, config_type = ?, " +
                "light4j_version = ?, class_path = ?, config_desc = ?, update_user = ?, update_ts = ? " +
                "WHERE config_id = ?";
        Result<String> result;
        Timestamp timestamp = new Timestamp(event.getEventId().getTimestamp());
        String value = event.getValue();
        Map<String, Object> map = JsonMapper.string2Map(value);

        try (Connection conn = ds.getConnection()) {
            conn.setAutoCommit(false);
            try (PreparedStatement statement = conn.prepareStatement(sql)) {
                statement.setString(1, event.getConfigName());
                statement.setString(2, event.getConfigPhase());
                statement.setString(3, event.getConfigType());

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
                statement.setString(7, event.getEventId().getId());
                statement.setTimestamp(8, timestamp);
                statement.setString(9, event.getConfigId());

                int count = statement.executeUpdate();
                if (count == 0) {
                    throw new SQLException("failed to update config with id " + event.getConfigId());
                }
                conn.commit();
                result = Success.of(event.getConfigId());
                insertNotification(event.getEventId(), event.getClass().getName(), AvroConverter.toJson(event, false), true, null);
            }  catch (SQLException e) {
                logger.error("SQLException:", e);
                conn.rollback();
                insertNotification(event.getEventId(), event.getClass().getName(), AvroConverter.toJson(event, false), false, e.getMessage());
                result = Failure.of(new Status(SQL_EXCEPTION, e.getMessage()));
            }  catch (Exception e) {
                logger.error("Exception:", e);
                conn.rollback();
                insertNotification(event.getEventId(), event.getClass().getName(), AvroConverter.toJson(event, false), false, e.getMessage());
                result = Failure.of(new Status(GENERIC_EXCEPTION, e.getMessage()));
            }
        }  catch (SQLException e) {
            logger.error("SQLException:", e);
            result = Failure.of(new Status(SQL_EXCEPTION, e.getMessage()));
        }
        return result;
    }


    @Override
    public Result<String> deleteConfig(ConfigDeletedEvent event) {
        final String sql = "DELETE FROM config_t WHERE config_id = ?";
        Result<String> result;

        try (Connection conn = ds.getConnection()) {
            conn.setAutoCommit(false);
            try (PreparedStatement statement = conn.prepareStatement(sql)) {
                statement.setString(1, event.getConfigId());

                int count = statement.executeUpdate();
                if (count == 0) {
                    throw new SQLException("failed to delete config with id " + event.getConfigId());
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
        StringBuilder sqlBuilder = new StringBuilder();
        sqlBuilder.append("SELECT COUNT(*) OVER () AS total,\n" +
                "config_id, config_name, config_phase, config_type, light4j_version, class_path, config_desc, update_user, update_ts\n" +
                "FROM config_t\n" +
                "WHERE 1=1\n");

        List<Object> parameters = new ArrayList<>();

        StringBuilder whereClause = new StringBuilder();
        addCondition(whereClause, parameters, "config_id", configId);
        addCondition(whereClause, parameters, "config_name", configName);
        addCondition(whereClause, parameters, "config_phase", configPhase);
        addCondition(whereClause, parameters, "config_type", configType);
        addCondition(whereClause, parameters, "light4j_version", light4jVersion);
        addCondition(whereClause, parameters, "class_path", classPath);
        addCondition(whereClause, parameters, "config_desc", configDesc);

        if (whereClause.length() > 0) {
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
                    map.put("configId", resultSet.getString("config_id"));
                    map.put("configName", resultSet.getString("config_name"));
                    map.put("configPhase", resultSet.getString("config_phase"));
                    map.put("configType", resultSet.getString("config_type"));
                    map.put("light4jVersion", resultSet.getString("light4j_version"));
                    map.put("classPath", resultSet.getString("class_path"));
                    map.put("configDesc", resultSet.getString("config_desc"));
                    map.put("updateUser", resultSet.getString("update_user"));
                    // handling date properly
                    map.put("updateTs", resultSet.getTimestamp("update_ts") != null ? resultSet.getTimestamp("update_ts").toString() : null);

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

            statement.setString(1, configId);

            try (ResultSet resultSet = statement.executeQuery()) {
                if (resultSet.next()) {
                    config.put("configId", resultSet.getString("config_id"));
                    config.put("configName", resultSet.getString("config_name"));
                    config.put("configPhase", resultSet.getString("config_phase"));
                    config.put("configType", resultSet.getString("config_type"));
                    config.put("light4jVersion", resultSet.getString("light4j_version"));
                    config.put("classPath", resultSet.getString("class_path"));
                    config.put("configDesc", resultSet.getString("config_desc"));
                    config.put("updateUser", resultSet.getString("update_user"));
                    // handling date properly
                    config.put("updateTs", resultSet.getTimestamp("update_ts") != null ? resultSet.getTimestamp("update_ts").toString() : null);
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
    public Result<String> getConfigIdApiAppLabel(String resourceType) {
        final String sql = "SELECT distinct c.config_id, c.config_name \n" +
                "FROM config_t c, config_property_t p \n" +
                "WHERE c.config_id = p.config_id \n" +
                "AND (p.value_type = 'map' or p.value_type = 'list')\n" +
                "AND p.resource_type LIKE ?\n" +
                "ORDER BY config_name\n";
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
    public Result<String> getPropertyNameLabel(String configId) {
        final String sql = "SELECT property_name FROM config_property_t WHERE config_id = ? ORDER BY display_order";
        Result<String> result;
        try (final Connection conn = ds.getConnection()) {
            List<Map<String, Object>> list = new ArrayList<>();
            try (PreparedStatement statement = conn.prepareStatement(sql)) {
                statement.setString(1, configId);
                try (ResultSet resultSet = statement.executeQuery()) {
                    while (resultSet.next()) {
                        Map<String, Object> map = new HashMap<>();
                        map.put("id", resultSet.getString("property_name"));
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
    public Result<String> getPropertyNameApiAppLabel(String configId, String resourceType) {
        final String sql = "SELECT property_name \n" +
                "FROM config_property_t\n" +
                "WHERE config_id = ?\n" +
                "AND (value_type = 'map' or value_type = 'list')\n" +
                "AND resource_type LIKE ? \n" +
                "ORDER BY display_order\n";
        Result<String> result;
        try (final Connection conn = ds.getConnection()) {
            List<Map<String, Object>> list = new ArrayList<>();
            try (PreparedStatement statement = conn.prepareStatement(sql)) {
                statement.setString(1, configId);
                statement.setString(2, "%" + resourceType + "%");
                try (ResultSet resultSet = statement.executeQuery()) {
                    while (resultSet.next()) {
                        Map<String, Object> map = new HashMap<>();
                        map.put("id", resultSet.getString("property_name"));
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
    public Result<String> createConfigProperty(ConfigPropertyCreatedEvent event) {
        final String sql = "INSERT INTO config_property_t (config_id, property_name, property_type, property_value, property_file, " +
                "resource_type, value_type, display_order, required, property_desc, light4j_version, update_user, update_ts) " +
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
        Result<String> result;
        Timestamp timestamp = new Timestamp(event.getEventId().getTimestamp());
        String value = event.getValue();
        Map<String, Object> map = JsonMapper.string2Map(value); // Parse the JSON value

        try (Connection conn = ds.getConnection()) {
            conn.setAutoCommit(false); // Start transaction
            try (PreparedStatement statement = conn.prepareStatement(sql)) {
                statement.setString(1, event.getConfigId());
                statement.setString(2, event.getPropertyName());
                statement.setString(3, event.getPropertyType());

                // Handle property_value (required)
                if (map.containsKey("propertyValue")) {
                    statement.setString(4, (String) map.get("propertyValue"));
                } else {
                    statement.setNull(4, Types.VARCHAR); // Or throw exception if it's truly required, but DB default is not set.
                }

                // Handle property_file (optional)
                if (map.containsKey("propertyFile")) {
                    statement.setString(5, (String) map.get("propertyFile"));
                } else {
                    statement.setNull(5, Types.VARCHAR);
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


                statement.setString(12, event.getEventId().getUserId());
                statement.setTimestamp(13, timestamp);


                int count = statement.executeUpdate();
                if (count == 0) {
                    throw new SQLException("Failed to insert the config property with id " + event.getConfigId() + " and name " + event.getPropertyName());
                }
                conn.commit(); // Commit transaction
                result = Success.of(event.getConfigId());  // Use Success.of
                // Assuming insertNotification is a method you have for handling notifications
                insertNotification(event.getEventId(), event.getClass().getName(), AvroConverter.toJson(event, false), true, null);

            } catch (SQLException e) {
                logger.error("SQLException:", e);
                conn.rollback(); // Rollback transaction on error
                insertNotification(event.getEventId(), event.getClass().getName(), AvroConverter.toJson(event, false), false, e.getMessage());
                result = Failure.of(new Status("SQL_EXCEPTION", e.getMessage())); // Use Status
            } catch (Exception e) {
                logger.error("Exception:", e);
                conn.rollback(); // Rollback transaction on error
                insertNotification(event.getEventId(), event.getClass().getName(), AvroConverter.toJson(event, false), false, e.getMessage());

                result = Failure.of(new Status("GENERIC_EXCEPTION", e.getMessage()));  // Use Status
            }
        } catch (SQLException e) {
            logger.error("SQLException:", e);
            result = Failure.of(new Status("SQL_EXCEPTION", e.getMessage())); // Use Status
        }
        return result;
    }

    @Override
    public Result<String> updateConfigProperty(ConfigPropertyUpdatedEvent event) {
        final String sql = "UPDATE config_property_t SET property_type = ?, property_value = ?, property_file = ?, " +
                "resource_type = ?, value_type = ?, display_order = ?, required = ?, property_desc = ?, " +
                "light4j_version = ?, update_user = ?, update_ts = ? " +
                "WHERE config_id = ? AND property_name = ?";

        Result<String> result;
        Timestamp timestamp = new Timestamp(event.getEventId().getTimestamp());
        String value = event.getValue();
        Map<String, Object> map = JsonMapper.string2Map(value);

        try (Connection conn = ds.getConnection()) {
            conn.setAutoCommit(false);
            try (PreparedStatement statement = conn.prepareStatement(sql)) {
                // Set the update values from the event and the parsed JSON
                statement.setString(1, event.getPropertyType());

                // Handle property_value (optional in update, but check in map)
                if (map.containsKey("propertyValue")) {
                    statement.setString(2, (String) map.get("propertyValue"));
                } else {
                    statement.setNull(2, Types.VARCHAR); // Or keep existing value if you prefer
                }

                // Handle property_file
                if (map.containsKey("propertyFile")) {
                    statement.setString(3, (String) map.get("propertyFile"));
                } else {
                    statement.setNull(3, Types.VARCHAR);
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

                statement.setString(10, event.getEventId().getUserId());
                statement.setTimestamp(11, timestamp);

                // WHERE clause: Crucial for updating the correct row!
                statement.setString(12, event.getConfigId());
                statement.setString(13, event.getPropertyName());


                int count = statement.executeUpdate();
                if (count == 0) {
                    // No rows were updated.  This could mean the config_id and property_name
                    // combination doesn't exist, or it could be a concurrency issue.
                    throw new SQLException("Failed to update config property.  No rows affected for config_id: " + event.getConfigId() + " and property_name: " + event.getPropertyName());
                }

                conn.commit();
                result = Success.of(event.getConfigId());
                insertNotification(event.getEventId(), event.getClass().getName(), AvroConverter.toJson(event, false), true, null);

            } catch (SQLException e) {
                logger.error("SQLException:", e);
                conn.rollback();
                insertNotification(event.getEventId(), event.getClass().getName(), AvroConverter.toJson(event, false), false, e.getMessage());
                result = Failure.of(new Status("SQL_EXCEPTION", e.getMessage()));
            } catch (Exception e) {
                logger.error("Exception:", e);
                conn.rollback();
                insertNotification(event.getEventId(), event.getClass().getName(), AvroConverter.toJson(event, false), false, e.getMessage());
                result = Failure.of(new Status("GENERIC_EXCEPTION", e.getMessage()));
            }
        } catch (SQLException e) {
            logger.error("SQLException:", e);
            result = Failure.of(new Status("SQL_EXCEPTION", e.getMessage()));
        }
        return result;
    }

    @Override
    public Result<String> deleteConfigProperty(ConfigPropertyDeletedEvent event) {
        final String sql = "DELETE FROM config_property_t WHERE config_id = ? AND property_name = ?";
        Result<String> result;

        try (Connection conn = ds.getConnection()) {
            conn.setAutoCommit(false);
            try (PreparedStatement statement = conn.prepareStatement(sql)) {
                statement.setString(1, event.getConfigId());
                statement.setString(2, event.getPropertyName());

                int count = statement.executeUpdate();
                if (count == 0) {
                    throw new SQLException("Failed to delete config property. No rows affected for config_id: " + event.getConfigId() + " and property_name: " + event.getPropertyName());
                }
                conn.commit();
                result = Success.of(event.getConfigId());
                insertNotification(event.getEventId(), event.getClass().getName(), AvroConverter.toJson(event, false), true, null);

            } catch (SQLException e) {
                logger.error("SQLException:", e);
                conn.rollback();
                insertNotification(event.getEventId(), event.getClass().getName(), AvroConverter.toJson(event, false), false, e.getMessage());
                result = Failure.of(new Status("SQL_EXCEPTION", e.getMessage()));
            } catch (Exception e) {
                logger.error("Exception:", e);
                conn.rollback();
                insertNotification(event.getEventId(), event.getClass().getName(), AvroConverter.toJson(event, false), false, e.getMessage());
                result = Failure.of(new Status("GENERIC_EXCEPTION", e.getMessage()));
            }
        } catch (SQLException e) {
            logger.error("SQLException:", e);
            result = Failure.of(new Status("SQL_EXCEPTION", e.getMessage()));
        }
        return result;
    }



    @Override
    public Result<String> getConfigProperty(int offset, int limit, String configId, String configName, String propertyName,
                                            String propertyType, String light4jVersion, Integer displayOrder, Boolean required,
                                            String propertyDesc, String propertyValue, String valueType, String propertyFile,
                                            String resourceType) {
        Result<String> result = null;

        StringBuilder sqlBuilder = new StringBuilder();
        sqlBuilder.append("SELECT COUNT(*) OVER () AS total,\n" +
                "cp.config_id, cp.property_name, cp.property_type, cp.light4j_version, cp.display_order, cp.required, " +
                "cp.property_desc, cp.property_value, cp.value_type, cp.property_file, cp.resource_type, cp.update_user, cp.update_ts, " +
                "c.config_name \n" +
                "FROM config_property_t cp\n" +
                "JOIN config_t c ON cp.config_id = c.config_id\n" +
                "WHERE 1=1\n");

        List<Object> parameters = new ArrayList<>();

        StringBuilder whereClause = new StringBuilder();
        addCondition(whereClause, parameters, "cp.config_id", configId);
        addCondition(whereClause, parameters, "c.config_name", configName);
        addCondition(whereClause, parameters, "cp.property_name", propertyName);
        addCondition(whereClause, parameters, "cp.property_type", propertyType);
        addCondition(whereClause, parameters, "cp.light4j_version", light4jVersion);
        addCondition(whereClause, parameters, "cp.display_order", displayOrder);
        addCondition(whereClause, parameters, "cp.required", required);
        addCondition(whereClause, parameters, "cp.property_desc", propertyDesc);
        addCondition(whereClause, parameters, "cp.property_value", propertyValue);
        addCondition(whereClause, parameters, "cp.value_type", valueType);
        addCondition(whereClause, parameters, "cp.property_file", propertyFile);
        addCondition(whereClause, parameters, "cp.resource_type", resourceType);


        if (whereClause.length() > 0) {
            sqlBuilder.append("AND ").append(whereClause);
        }

        sqlBuilder.append(" ORDER BY cp.config_id, cp.property_name\n" +
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
                    map.put("configId", resultSet.getString("config_id"));
                    map.put("configName", resultSet.getString("config_name")); // Get config_name
                    map.put("propertyName", resultSet.getString("property_name"));
                    map.put("propertyType", resultSet.getString("property_type"));
                    map.put("light4jVersion", resultSet.getString("light4j_version"));
                    map.put("displayOrder", resultSet.getInt("display_order"));  // Could be null
                    map.put("required", resultSet.getBoolean("required"));      // Could be null
                    map.put("propertyDesc", resultSet.getString("property_desc"));
                    map.put("propertyValue", resultSet.getString("property_value"));
                    map.put("valueType", resultSet.getString("value_type"));
                    map.put("propertyFile", resultSet.getString("property_file"));
                    map.put("resourceType", resultSet.getString("resource_type"));
                    map.put("updateUser", resultSet.getString("update_user"));
                    map.put("updateTs", resultSet.getTimestamp("update_ts") != null ? resultSet.getTimestamp("update_ts").toString() : null);

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

        String sql = "SELECT cp.config_id, cp.property_name, cp.property_type, cp.light4j_version, cp.display_order, cp.required, " +
                "cp.property_desc, cp.property_value, cp.value_type, cp.property_file, cp.resource_type, cp.update_user, cp.update_ts, " +
                "c.config_name " +
                "FROM config_property_t cp " +
                "JOIN config_t c ON cp.config_id = c.config_id " +
                "WHERE cp.config_id = ?";

        try (Connection connection = ds.getConnection();
             PreparedStatement preparedStatement = connection.prepareStatement(sql)) {

            preparedStatement.setString(1, configId);

            List<Map<String, Object>> configProperties = new ArrayList<>();
            try (ResultSet resultSet = preparedStatement.executeQuery()) {
                while (resultSet.next()) {
                    Map<String, Object> map = new HashMap<>();
                    map.put("configId", resultSet.getString("config_id"));
                    map.put("configName", resultSet.getString("config_name"));
                    map.put("propertyName", resultSet.getString("property_name"));
                    map.put("propertyType", resultSet.getString("property_type"));
                    map.put("light4jVersion", resultSet.getString("light4j_version"));
                    map.put("displayOrder", resultSet.getInt("display_order"));
                    map.put("required", resultSet.getBoolean("required"));
                    map.put("propertyDesc", resultSet.getString("property_desc"));
                    map.put("propertyValue", resultSet.getString("property_value"));
                    map.put("valueType", resultSet.getString("value_type"));
                    map.put("propertyFile", resultSet.getString("property_file"));
                    map.put("resourceType", resultSet.getString("resource_type"));
                    map.put("updateUser", resultSet.getString("update_user"));
                    map.put("updateTs", resultSet.getTimestamp("update_ts") != null ? resultSet.getTimestamp("update_ts").toString() : null);
                    configProperties.add(map);
                }
            }

            if (configProperties.isEmpty()) {
                // return Failure.of(new Status("CONFIG_PROPERTY_NOT_FOUND", configId)); // Consider a more specific status
                result = Success.of("[]"); // Return an empty JSON array.  This is generally better than a 404.

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
    public Result<String> queryConfigPropertyByIdName(String configId, String propertyName) {
        Result<String> result = null;

        String sql = "SELECT cp.config_id, cp.property_name, cp.property_type, cp.light4j_version, cp.display_order, cp.required, " +
                "cp.property_desc, cp.property_value, cp.value_type, cp.property_file, cp.resource_type, cp.update_user, cp.update_ts, " +
                "c.config_name " +
                "FROM config_property_t cp " +
                "INNER JOIN config_t c ON cp.config_id = c.config_id " +
                "WHERE cp.config_id = ? " +
                "AND cp.property_name = ?";


        try (Connection connection = ds.getConnection();
             PreparedStatement preparedStatement = connection.prepareStatement(sql)) {

            preparedStatement.setString(1, configId);
            preparedStatement.setString(2, propertyName);

            Map<String, Object> map = new HashMap<>();
            try (ResultSet resultSet = preparedStatement.executeQuery()) {
                if (resultSet.next()) {
                    map.put("configId", resultSet.getString("config_id"));
                    map.put("configName", resultSet.getString("config_name"));
                    map.put("propertyName", resultSet.getString("property_name"));
                    map.put("propertyType", resultSet.getString("property_type"));
                    map.put("light4jVersion", resultSet.getString("light4j_version"));
                    map.put("displayOrder", resultSet.getInt("display_order"));
                    map.put("required", resultSet.getBoolean("required"));
                    map.put("propertyDesc", resultSet.getString("property_desc"));
                    map.put("propertyValue", resultSet.getString("property_value"));
                    map.put("valueType", resultSet.getString("value_type"));
                    map.put("propertyFile", resultSet.getString("property_file"));
                    map.put("resourceType", resultSet.getString("resource_type"));
                    map.put("updateUser", resultSet.getString("update_user"));
                    map.put("updateTs", resultSet.getTimestamp("update_ts") != null ? resultSet.getTimestamp("update_ts").toString() : null);
                }
            }

            if (map.isEmpty()) {
                result = Failure.of(new Status(OBJECT_NOT_FOUND, "config property", "configId = " + configId + " propertyName = " + propertyName));
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
    public Result<String> createConfigEnvironment(ConfigEnvironmentCreatedEvent event) {
        final String sql = "INSERT INTO environment_property_t (environment, config_id, property_name, " +
                "property_value, property_file, update_user, update_ts) " +
                "VALUES (?, ?, ?, ?, ?, ?, ?)";
        Result<String> result;
        Timestamp timestamp = new Timestamp(event.getEventId().getTimestamp());
        String value = event.getValue();
        Map<String, Object> map = JsonMapper.string2Map(value);

        try (Connection conn = ds.getConnection()) {
            conn.setAutoCommit(false);
            try (PreparedStatement statement = conn.prepareStatement(sql)) {
                statement.setString(1, event.getEnvironment());
                statement.setString(2, event.getConfigId());
                statement.setString(3, event.getPropertyName());

                // Handle property_value (optional)
                if (map.containsKey("propertyValue")) {
                    statement.setString(4, (String) map.get("propertyValue"));
                } else {
                    statement.setNull(4, Types.VARCHAR);
                }

                // Handle property_file (optional)
                if (map.containsKey("propertyFile")) {
                    statement.setString(5, (String) map.get("propertyFile"));
                } else {
                    statement.setNull(5, Types.VARCHAR);
                }

                statement.setString(6, event.getEventId().getUserId());
                statement.setTimestamp(7, timestamp);

                int count = statement.executeUpdate();
                if (count == 0) {
                    throw new SQLException("Failed to insert environment property for environment: " + event.getEnvironment() +
                            ", config_id: " + event.getConfigId() + ", property_name: " + event.getPropertyName());
                }
                conn.commit();
                result = Success.of(event.getConfigId()); // Or perhaps a composite key, if needed.
                insertNotification(event.getEventId(), event.getClass().getName(), AvroConverter.toJson(event, false), true, null);


            } catch (SQLException e) {
                logger.error("SQLException:", e);
                conn.rollback();
                insertNotification(event.getEventId(), event.getClass().getName(), AvroConverter.toJson(event, false), false, e.getMessage());
                result = Failure.of(new Status("SQL_EXCEPTION", e.getMessage()));
            } catch (Exception e) {
                logger.error("Exception:", e);
                conn.rollback();
                insertNotification(event.getEventId(), event.getClass().getName(), AvroConverter.toJson(event, false), false, e.getMessage());

                result = Failure.of(new Status("GENERIC_EXCEPTION", e.getMessage()));
            }
        } catch (SQLException e) {
            logger.error("SQLException:", e);
            result = Failure.of(new Status("SQL_EXCEPTION", e.getMessage()));
        }
        return result;
    }

    @Override
    public Result<String> updateConfigEnvironment(ConfigEnvironmentUpdatedEvent event) {
        final String sql = "UPDATE environment_property_t SET property_value = ?, property_file = ?, update_user = ?, update_ts = ? " +
                "WHERE environment = ? AND config_id = ? AND property_name = ?";
        Result<String> result;
        Timestamp timestamp = new Timestamp(event.getEventId().getTimestamp());
        String value = event.getValue();
        Map<String, Object> map = JsonMapper.string2Map(value);

        try (Connection conn = ds.getConnection()) {
            conn.setAutoCommit(false);
            try (PreparedStatement statement = conn.prepareStatement(sql)) {

                // Handle property_value (optional)
                if (map.containsKey("propertyValue")) {
                    statement.setString(1, (String) map.get("propertyValue"));
                } else {
                    statement.setNull(1, Types.VARCHAR); // Or keep existing
                }

                // Handle property_file (optional)
                if (map.containsKey("propertyFile")) {
                    statement.setString(2, (String) map.get("propertyFile"));
                } else {
                    statement.setNull(2, Types.VARCHAR); // Or keep existing
                }

                statement.setString(3, event.getEventId().getUserId());
                statement.setTimestamp(4, timestamp);

                // WHERE clause parameters (from the event, *not* the JSON)
                statement.setString(5, event.getEnvironment());
                statement.setString(6, event.getConfigId());
                statement.setString(7, event.getPropertyName());

                int count = statement.executeUpdate();
                if (count == 0) {
                    throw new SQLException("Failed to update environment property. No rows affected for environment: " + event.getEnvironment() +
                            ", config_id: " + event.getConfigId() + ", property_name: " + event.getPropertyName());
                }
                conn.commit();
                result = Success.of(event.getConfigId());  // Or a composite key.
                insertNotification(event.getEventId(), event.getClass().getName(), AvroConverter.toJson(event, false), true, null);

            } catch (SQLException e) {
                logger.error("SQLException:", e);
                conn.rollback();
                insertNotification(event.getEventId(), event.getClass().getName(), AvroConverter.toJson(event, false), false, e.getMessage());
                result = Failure.of(new Status("SQL_EXCEPTION", e.getMessage()));
            } catch (Exception e) {
                logger.error("Exception:", e);
                conn.rollback();
                insertNotification(event.getEventId(), event.getClass().getName(), AvroConverter.toJson(event, false), false, e.getMessage());
                result = Failure.of(new Status("GENERIC_EXCEPTION", e.getMessage()));
            }
        } catch (SQLException e) {
            logger.error("SQLException:", e);
            result = Failure.of(new Status("SQL_EXCEPTION", e.getMessage()));
        }
        return result;
    }


    @Override
    public Result<String> deleteConfigEnvironment(ConfigEnvironmentDeletedEvent event) {
        final String sql = "DELETE FROM environment_property_t WHERE environment = ? AND config_id = ? AND property_name = ?";
        Result<String> result;

        try (Connection conn = ds.getConnection()) {
            conn.setAutoCommit(false);
            try (PreparedStatement statement = conn.prepareStatement(sql)) {
                statement.setString(1, event.getEnvironment());
                statement.setString(2, event.getConfigId());
                statement.setString(3, event.getPropertyName());

                int count = statement.executeUpdate();
                if (count == 0) {
                    throw new SQLException("Failed to delete environment property. No rows affected for environment: " + event.getEnvironment() +
                            ", config_id: " + event.getConfigId() + ", property_name: " + event.getPropertyName());
                }
                conn.commit();
                result = Success.of(event.getConfigId()); // Or a composite key
                insertNotification(event.getEventId(), event.getClass().getName(), AvroConverter.toJson(event, false), true, null);

            } catch (SQLException e) {
                logger.error("SQLException:", e);
                conn.rollback();
                insertNotification(event.getEventId(), event.getClass().getName(), AvroConverter.toJson(event, false), false, e.getMessage());
                result = Failure.of(new Status("SQL_EXCEPTION", e.getMessage()));
            } catch (Exception e) {
                logger.error("Exception:", e);
                conn.rollback();
                insertNotification(event.getEventId(), event.getClass().getName(), AvroConverter.toJson(event, false), false, e.getMessage());
                result = Failure.of(new Status("GENERIC_EXCEPTION", e.getMessage()));
            }
        } catch (SQLException e) {
            logger.error("SQLException:", e);
            result = Failure.of(new Status("SQL_EXCEPTION", e.getMessage()));
        }
        return result;
    }

    @Override
    public Result<String> getConfigEnvironment(int offset, int limit, String environment, String configId, String configName,
                                               String propertyName, String propertyValue, String propertyFile) {
        Result<String> result = null;
        StringBuilder sqlBuilder = new StringBuilder();
        sqlBuilder.append("SELECT COUNT(*) OVER () AS total,\n" +
                "ep.environment, ep.config_id, ep.property_name, ep.property_value, ep.property_file, ep.update_user, ep.update_ts, \n" +
                "c.config_name \n" +  // Include config_name
                "FROM environment_property_t ep\n" +
                "JOIN config_t c ON ep.config_id = c.config_id\n" + // Join with config_t
                "WHERE 1=1\n");

        List<Object> parameters = new ArrayList<>();

        StringBuilder whereClause = new StringBuilder();
        addCondition(whereClause, parameters, "ep.environment", environment);
        addCondition(whereClause, parameters, "ep.config_id", configId);
        addCondition(whereClause, parameters, "c.config_name", configName); // Filter by config_name
        addCondition(whereClause, parameters, "ep.property_name", propertyName);
        addCondition(whereClause, parameters, "ep.property_value", propertyValue);
        addCondition(whereClause, parameters, "ep.property_file", propertyFile);


        if (whereClause.length() > 0) {
            sqlBuilder.append("AND ").append(whereClause);
        }

        sqlBuilder.append(" ORDER BY ep.environment, ep.config_id, ep.property_name\n" +
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
                    map.put("environment", resultSet.getString("environment"));
                    map.put("configId", resultSet.getString("config_id"));
                    map.put("configName", resultSet.getString("config_name")); // Get from joined table
                    map.put("propertyName", resultSet.getString("property_name"));
                    map.put("propertyValue", resultSet.getString("property_value"));
                    map.put("propertyFile", resultSet.getString("property_file"));
                    map.put("updateUser", resultSet.getString("update_user"));
                    map.put("updateTs", resultSet.getTimestamp("update_ts") != null ? resultSet.getTimestamp("update_ts").toString() : null);

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
    public Result<String> createInstanceApi(InstanceApiCreatedEvent event) {
        final String sql = "INSERT INTO instance_api_t(host_id, instance_id, api_id, api_version, active, update_user, update_ts) " +
                "VALUES (?, ?, ?, ?, ?, ?, ?)";
        Result<String> result;
        Timestamp timestamp = new Timestamp(event.getEventId().getTimestamp());
        String value = event.getValue();
        Map<String, Object> map = JsonMapper.string2Map(value);

        try (Connection conn = ds.getConnection()) {
            conn.setAutoCommit(false);
            try (PreparedStatement statement = conn.prepareStatement(sql)) {
                statement.setString(1, event.getEventId().getHostId());
                statement.setString(2, event.getInstanceId());
                statement.setString(3, event.getApiId());
                statement.setString(4, event.getApiVersion());
                if (map.containsKey("active")) {
                    statement.setBoolean(5, (Boolean) map.get("active"));
                } else {
                    statement.setNull(5, Types.BOOLEAN);
                }
                statement.setString(6, event.getEventId().getId());
                statement.setTimestamp(7, timestamp);

                int count = statement.executeUpdate();
                if (count == 0) {
                    throw new SQLException("failed to insert the instance api");
                }
                conn.commit();
                result = Success.of(String.format("Instance API created for instanceId: %s, apiId: %s, apiVersion: %s",
                        event.getInstanceId(), event.getApiId(), event.getApiVersion())); // return some kind of key.
                insertNotification(event.getEventId(), event.getClass().getName(), AvroConverter.toJson(event, false), true, null);

            }   catch (SQLException e) {
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
    public Result<String> updateInstanceApi(InstanceApiUpdatedEvent event) {
        final String sql = "UPDATE instance_api_t SET active = ?, update_user = ?, update_ts = ? " +
                "WHERE host_id = ? and instance_id = ? and api_id = ? and api_version = ?";
        Result<String> result;
        Timestamp timestamp = new Timestamp(event.getEventId().getTimestamp());
        String value = event.getValue();
        Map<String, Object> map = JsonMapper.string2Map(value);

        try (Connection conn = ds.getConnection()) {
            conn.setAutoCommit(false);
            try (PreparedStatement statement = conn.prepareStatement(sql)) {
                if (map.containsKey("active")) {
                    statement.setBoolean(1, (Boolean) map.get("active"));
                } else {
                    statement.setNull(1, Types.BOOLEAN);
                }
                statement.setString(2, event.getEventId().getId());
                statement.setTimestamp(3, timestamp);
                statement.setString(4, event.getEventId().getHostId());
                statement.setString(5, event.getInstanceId());
                statement.setString(6, event.getApiId());
                statement.setString(7, event.getApiVersion());

                int count = statement.executeUpdate();
                if (count == 0) {
                    throw new SQLException("failed to update instance api");
                }
                conn.commit();
                result = Success.of(String.format("Instance API updated for instanceId: %s, apiId: %s, apiVersion: %s",
                        event.getInstanceId(), event.getApiId(), event.getApiVersion()));
                insertNotification(event.getEventId(), event.getClass().getName(), AvroConverter.toJson(event, false), true, null);
            } catch (SQLException e) {
                logger.error("SQLException:", e);
                conn.rollback();
                insertNotification(event.getEventId(), event.getClass().getName(), AvroConverter.toJson(event, false), false, e.getMessage());
                result = Failure.of(new Status(SQL_EXCEPTION, e.getMessage()));
            }  catch (Exception e) {
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
    public Result<String> deleteInstanceApi(InstanceApiDeletedEvent event) {
        final String sql = "DELETE FROM instance_api_t WHERE host_id = ? AND instance_id = ? AND api_id = ? AND api_version = ?";
        Result<String> result;
        try (Connection conn = ds.getConnection()) {
            conn.setAutoCommit(false);
            try (PreparedStatement statement = conn.prepareStatement(sql)) {
                statement.setString(1, event.getEventId().getHostId());
                statement.setString(2, event.getInstanceId());
                statement.setString(3, event.getApiId());
                statement.setString(4, event.getApiVersion());

                int count = statement.executeUpdate();
                if (count == 0) {
                    throw new SQLException("failed to delete the instance api with id " + event.getApiId());
                }
                conn.commit();
                result = Success.of(String.format("Instance API deleted for instanceId: %s, apiId: %s, apiVersion: %s",
                        event.getInstanceId(), event.getApiId(), event.getApiVersion())); //Return some information about the deleted record
                insertNotification(event.getEventId(), event.getClass().getName(), AvroConverter.toJson(event, false), true, null);

            } catch (SQLException e) {
                logger.error("SQLException:", e);
                conn.rollback();
                insertNotification(event.getEventId(), event.getClass().getName(), AvroConverter.toJson(event, false), false, e.getMessage());
                result = Failure.of(new Status(SQL_EXCEPTION, e.getMessage()));
            }  catch (Exception e) {
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
    public Result<String> getInstanceApi(int offset, int limit, String hostId, String instanceId, String apiId, String apiVersion,
                                         Boolean active) {
        Result<String> result = null;
        StringBuilder sqlBuilder = new StringBuilder();
        sqlBuilder.append("SELECT COUNT(*) OVER () AS total,\n" +
                "host_id, instance_id, api_id, api_version, active, update_user, update_ts\n" +
                "FROM instance_api_t\n" +
                "WHERE 1=1\n");

        List<Object> parameters = new ArrayList<>();

        StringBuilder whereClause = new StringBuilder();

        addCondition(whereClause, parameters, "host_id", hostId);
        addCondition(whereClause, parameters, "instance_id", instanceId);
        addCondition(whereClause, parameters, "api_id", apiId);
        addCondition(whereClause, parameters, "api_version", apiVersion);
        addCondition(whereClause, parameters, "active", active);


        if (whereClause.length() > 0) {
            sqlBuilder.append("AND ").append(whereClause);
        }

        sqlBuilder.append("ORDER BY instance_id, api_id, api_version\n" + // Added ordering
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
                    map.put("hostId", resultSet.getString("host_id"));
                    map.put("instanceId", resultSet.getString("instance_id"));
                    map.put("apiId", resultSet.getString("api_id"));
                    map.put("apiVersion", resultSet.getString("api_version"));
                    map.put("active", resultSet.getBoolean("active"));
                    map.put("updateUser", resultSet.getString("update_user"));
                    // handling date properly
                    map.put("updateTs", resultSet.getTimestamp("update_ts") != null ? resultSet.getTimestamp("update_ts").toString() : null);
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

    @Override
    public Result<String> createConfigInstanceApi(ConfigInstanceApiCreatedEvent event) {
        final String sql = "INSERT INTO instance_api_property_t (host_id, instance_id, api_id, api_version, config_id, " +
                "property_name, property_value, update_user, update_ts) VALUES (?, ?, ?, ?, ?,  ?, ?, ?, ?)";
        Result<String> result;
        Timestamp timestamp = new Timestamp(event.getEventId().getTimestamp());
        String value = event.getValue();
        Map<String, Object> map = JsonMapper.string2Map(value);

        try (Connection conn = ds.getConnection()) {
            conn.setAutoCommit(false);
            try (PreparedStatement statement = conn.prepareStatement(sql)) {
                statement.setString(1, event.getEventId().getHostId());
                statement.setString(2, event.getInstanceId());
                statement.setString(3, event.getApiId());
                statement.setString(4, event.getApiVersion());
                statement.setString(5, event.getConfigId());
                statement.setString(6, event.getPropertyName());
                if (map.containsKey("propertyValue")) {
                    statement.setString(7, (String)map.get("propertyValue"));
                } else {
                    statement.setNull(7, Types.VARCHAR);
                }
                statement.setString(8, event.getEventId().getUserId());
                statement.setTimestamp(9, timestamp);

                int count = statement.executeUpdate();
                if (count == 0) {
                    throw new SQLException("Failed to insert instance API for host_id: " + event.getEventId().getHostId() +
                            ", instance_id: " + event.getInstanceId() + ", api_id: " + event.getApiId() + ", api_version: " + event.getApiVersion());
                }
                conn.commit();
                result = Success.of(event.getEventId().getHostId() + "|" + event.getInstanceId() + "|" + event.getApiId() + "|" + event.getApiVersion()); // Composite key
                insertNotification(event.getEventId(), event.getClass().getName(), AvroConverter.toJson(event, false), true, null);

            } catch (SQLException e) {
                logger.error("SQLException:", e);
                conn.rollback();
                insertNotification(event.getEventId(), event.getClass().getName(), AvroConverter.toJson(event, false), false, e.getMessage());
                result = Failure.of(new Status("SQL_EXCEPTION", e.getMessage()));
            } catch (Exception e) {
                logger.error("Exception:", e);
                conn.rollback();
                insertNotification(event.getEventId(), event.getClass().getName(), AvroConverter.toJson(event, false), false, e.getMessage());
                result = Failure.of(new Status("GENERIC_EXCEPTION", e.getMessage()));
            }
        } catch (SQLException e) {
            logger.error("SQLException:", e);
            result = Failure.of(new Status("SQL_EXCEPTION", e.getMessage()));
        }
        return result;
    }

    @Override
    public Result<String> updateConfigInstanceApi(ConfigInstanceApiUpdatedEvent event) {
        final String sql = "UPDATE instance_api_property_t SET " +
                "property_value = ?, update_user = ?, update_ts = ? " +
                "WHERE host_id = ? AND instance_id = ? AND api_id = ? AND api_version = ? AND config_id = ? AND property_name = ?";
        Result<String> result;
        Timestamp timestamp = new Timestamp(event.getEventId().getTimestamp());
        String value = event.getValue();
        Map<String, Object> map = JsonMapper.string2Map(value);

        try (Connection conn = ds.getConnection()) {
            conn.setAutoCommit(false);
            try (PreparedStatement statement = conn.prepareStatement(sql)) {
                if (map.containsKey("propertyValue")) {
                    statement.setString(1, (String)map.get("propertyValue"));
                } else {
                    statement.setNull(1, Types.VARCHAR);
                }
                statement.setString(2, event.getEventId().getUserId());
                statement.setTimestamp(3, timestamp);
                statement.setString(4, event.getEventId().getHostId());
                statement.setString(5, event.getInstanceId());
                statement.setString(6, event.getApiId());
                statement.setString(7, event.getApiVersion());
                statement.setString(8, event.getConfigId());
                statement.setString(9, event.getPropertyName());

                int count = statement.executeUpdate();
                if (count == 0) {
                    throw new SQLException("Failed to update instance API. No rows affected for host_id: " + event.getEventId().getHostId() +
                            ", instance_id: " + event.getInstanceId() + ", api_id: " + event.getApiId() + ", api_version: " + event.getApiVersion() +
                            ", config_id: " + event.getConfigId() + ", property_name: " + event.getPropertyName());
                }
                conn.commit();
                result = Success.of(event.getEventId().getHostId() + "|" + event.getInstanceId() + "|" + event.getApiId() + "|" + event.getApiVersion() + "|" + event.getConfigId() + "|" + event.getPropertyName());
                insertNotification(event.getEventId(), event.getClass().getName(), AvroConverter.toJson(event, false), true, null);

            } catch (SQLException e) {
                logger.error("SQLException:", e);
                conn.rollback();
                insertNotification(event.getEventId(), event.getClass().getName(), AvroConverter.toJson(event, false), false, e.getMessage());
                result = Failure.of(new Status("SQL_EXCEPTION", e.getMessage()));
            } catch (Exception e) {
                logger.error("Exception:", e);
                conn.rollback();
                insertNotification(event.getEventId(), event.getClass().getName(), AvroConverter.toJson(event, false), false, e.getMessage());
                result = Failure.of(new Status("GENERIC_EXCEPTION", e.getMessage()));
            }
        } catch (SQLException e) {
            logger.error("SQLException:", e);
            result = Failure.of(new Status("SQL_EXCEPTION", e.getMessage()));
        }
        return result;
    }

    @Override
    public Result<String> deleteConfigInstanceApi(ConfigInstanceApiDeletedEvent event) {
        final String sql = "DELETE FROM instance_api_property_t " +
                "WHERE host_id = ? AND instance_id = ? AND api_id = ? AND api_version = ? AND config_id = ? AND property_name = ?";
        Result<String> result;

        try (Connection conn = ds.getConnection()) {
            conn.setAutoCommit(false);
            try (PreparedStatement statement = conn.prepareStatement(sql)) {
                statement.setString(1, event.getEventId().getHostId());
                statement.setString(2, event.getInstanceId());
                statement.setString(3, event.getApiId());
                statement.setString(4, event.getApiVersion());
                statement.setString(5, event.getConfigId());
                statement.setString(6, event.getPropertyName());

                int count = statement.executeUpdate();
                if (count == 0) {
                    throw new SQLException("Failed to delete instance API. No rows affected for host_id: " + event.getEventId().getHostId() +
                            ", instance_id: " + event.getInstanceId() + ", api_id: " + event.getApiId() + ", api_version: " + event.getApiVersion());
                }
                conn.commit();
                result = Success.of(event.getEventId().getHostId() + "|" + event.getInstanceId() + "|" + event.getApiId() + "|" + event.getApiVersion()); // Composite key
                insertNotification(event.getEventId(), event.getClass().getName(), AvroConverter.toJson(event, false), true, null);

            } catch (SQLException e) {
                logger.error("SQLException:", e);
                conn.rollback();
                insertNotification(event.getEventId(), event.getClass().getName(), AvroConverter.toJson(event, false), false, e.getMessage());
                result = Failure.of(new Status("SQL_EXCEPTION", e.getMessage()));
            } catch (Exception e) {
                logger.error("Exception:", e);
                conn.rollback();
                insertNotification(event.getEventId(), event.getClass().getName(), AvroConverter.toJson(event, false), false, e.getMessage());
                result = Failure.of(new Status("GENERIC_EXCEPTION", e.getMessage()));
            }
        } catch (SQLException e) {
            logger.error("SQLException:", e);
            result = Failure.of(new Status("SQL_EXCEPTION", e.getMessage()));
        }
        return result;
    }

    @Override
    public Result<String> getConfigInstanceApi(int offset, int limit, String hostId, String instanceId, String instanceName,
                                               String apiId, String apiVersion, String configId, String configName,
                                               String propertyName, String propertyValue, String propertyFile) {
        Result<String> result = null;

        StringBuilder sqlBuilder = new StringBuilder();
        sqlBuilder.append("SELECT COUNT(*) OVER () AS total,\n" +
                "ia.host_id, ia.instance_id, i.instance_name, ia.api_id, ia.api_version, ia.active, ia.update_user, ia.update_ts,\n" +
                "iap.config_id, c.config_name, iap.property_name, iap.property_value, iap.property_file\n" +
                "FROM instance_api_t ia\n" +
                "INNER JOIN instance_t i ON ia.host_id =i.host_id AND ia.instance_id = i.instance_id \n" +
                "INNER JOIN instance_api_property_t iap ON ia.host_id = iap.host_id AND ia.instance_id = iap.instance_id AND ia.api_id = iap.api_id AND ia.api_version = iap.api_version\n" +
                "INNER JOIN config_t c ON iap.config_id = c.config_id\n" +
                "WHERE 1=1\n");

        List<Object> parameters = new ArrayList<>();

        StringBuilder whereClause = new StringBuilder();
        addCondition(whereClause, parameters, "ia.host_id", hostId);
        addCondition(whereClause, parameters, "ia.instance_id", instanceId);
        addCondition(whereClause, parameters, "i.instance_name", instanceName);
        addCondition(whereClause, parameters, "ia.api_id", apiId);
        addCondition(whereClause, parameters, "ia.api_version", apiVersion);
        addCondition(whereClause, parameters, "iap.config_id", configId);
        addCondition(whereClause, parameters, "c.config_name", configName);
        addCondition(whereClause, parameters, "iap.property_name", propertyName);
        addCondition(whereClause, parameters, "iap.property_value", propertyValue);
        addCondition(whereClause, parameters, "iap.property_file", propertyFile);


        if (whereClause.length() > 0) {
            sqlBuilder.append("AND ").append(whereClause);
        }

        sqlBuilder.append(" ORDER BY ia.host_id, ia.instance_id, ia.api_id, ia.api_version, iap.config_id, iap.property_name\n" +
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

                    map.put("hostId", resultSet.getString("host_id"));
                    map.put("instanceId", resultSet.getString("instance_id"));
                    map.put("instanceName", resultSet.getString("instance_name"));
                    map.put("apiId", resultSet.getString("api_id"));
                    map.put("apiVersion", resultSet.getString("api_version"));
                    map.put("configId", resultSet.getString("config_id"));
                    map.put("configName", resultSet.getString("config_name"));
                    map.put("propertyName", resultSet.getString("property_name"));
                    map.put("propertyValue", resultSet.getString("property_value"));
                    map.put("propertyFile", resultSet.getString("property_file"));
                    map.put("updateUser", resultSet.getString("update_user"));
                    map.put("updateTs", resultSet.getTimestamp("update_ts") != null ? resultSet.getTimestamp("update_ts").toString() : null);
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
    public Result<String> createInstanceApp(InstanceAppCreatedEvent event) {
        final String sql = "INSERT INTO instance_app_t(host_id, instance_id, app_id, app_version, active, update_user, update_ts) " +
                "VALUES (?, ?, ?, ?, ?, ?, ?)";
        Result<String> result;
        Timestamp timestamp = new Timestamp(event.getEventId().getTimestamp());
        String value = event.getValue();
        Map<String, Object> map = JsonMapper.string2Map(value);

        try (Connection conn = ds.getConnection()) {
            conn.setAutoCommit(false);
            try (PreparedStatement statement = conn.prepareStatement(sql)) {
                statement.setString(1, event.getEventId().getHostId());
                statement.setString(2, event.getInstanceId());
                statement.setString(3, event.getAppId());
                statement.setString(4, event.getAppVersion());
                if (map.containsKey("active")) {
                    statement.setBoolean(5, (Boolean) map.get("active"));
                } else {
                    statement.setNull(5, Types.BOOLEAN);
                }
                statement.setString(6, event.getEventId().getId());
                statement.setTimestamp(7, timestamp);
                int count = statement.executeUpdate();
                if (count == 0) {
                    throw new SQLException("failed to insert the instance app with event " + event.toString());
                }
                conn.commit();
                result = Success.of(String.format("Instance App created for instanceId: %s, appId: %s, appVersion: %s",
                        event.getInstanceId(), event.getAppId(), event.getAppVersion())); //Return some info about the created instance
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
    public Result<String> updateInstanceApp(InstanceAppUpdatedEvent event) {
        final String sql = "UPDATE instance_app_t SET active = ?, update_user = ?, update_ts = ? " +
                "WHERE host_id = ? and instance_id = ? and app_id = ? and app_version = ?";
        Result<String> result;
        Timestamp timestamp = new Timestamp(event.getEventId().getTimestamp());
        String value = event.getValue();
        Map<String, Object> map = JsonMapper.string2Map(value);

        try (Connection conn = ds.getConnection()) {
            conn.setAutoCommit(false);
            try (PreparedStatement statement = conn.prepareStatement(sql)) {
                if (map.containsKey("active")) {
                    statement.setBoolean(1, (Boolean) map.get("active"));
                } else {
                    statement.setNull(1, Types.BOOLEAN);
                }
                statement.setString(2, event.getEventId().getId());
                statement.setTimestamp(3, timestamp);
                statement.setString(4, event.getEventId().getHostId());
                statement.setString(5, event.getInstanceId());
                statement.setString(6, event.getAppId());
                statement.setString(7, event.getAppVersion());
                int count = statement.executeUpdate();
                if (count == 0) {
                    throw new SQLException("failed to update the instance app with id " + event.getAppId());
                }
                conn.commit();
                result = Success.of(String.format("Instance App updated for instanceId: %s, appId: %s, appVersion: %s",
                        event.getInstanceId(), event.getAppId(), event.getAppVersion())); //Return some info about the updated record.
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
    public Result<String> deleteInstanceApp(InstanceAppDeletedEvent event) {
        final String sql = "DELETE FROM instance_app_t WHERE host_id = ? AND instance_id = ? AND app_id = ? AND app_version = ?";
        Result<String> result;
        try (Connection conn = ds.getConnection()) {
            conn.setAutoCommit(false);
            try (PreparedStatement statement = conn.prepareStatement(sql)) {
                statement.setString(1, event.getEventId().getHostId());
                statement.setString(2, event.getInstanceId());
                statement.setString(3, event.getAppId());
                statement.setString(4, event.getAppVersion());

                int count = statement.executeUpdate();
                if (count == 0) {
                    throw new SQLException("failed to delete the instance app with id " + event.getAppId());
                }
                conn.commit();
                result = Success.of(String.format("Instance app deleted for instanceId: %s, appId: %s, appVersion: %s",
                        event.getInstanceId(), event.getAppId(), event.getAppVersion())); //Return some info about the deleted record
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
    public Result<String> getInstanceApp(int offset, int limit, String hostId, String instanceId, String appId, String appVersion,
                                         Boolean active) {
        Result<String> result = null;
        StringBuilder sqlBuilder = new StringBuilder();
        sqlBuilder.append("SELECT COUNT(*) OVER () AS total,\n" +
                "host_id, instance_id, app_id, app_version, active, update_user, update_ts\n" +
                "FROM instance_app_t\n" +
                "WHERE 1=1\n");

        List<Object> parameters = new ArrayList<>();

        StringBuilder whereClause = new StringBuilder();

        addCondition(whereClause, parameters, "host_id", hostId);
        addCondition(whereClause, parameters, "instance_id", instanceId);
        addCondition(whereClause, parameters, "app_id", appId);
        addCondition(whereClause, parameters, "app_version", appVersion);
        addCondition(whereClause, parameters, "active", active);

        if (whereClause.length() > 0) {
            sqlBuilder.append("AND ").append(whereClause);
        }

        sqlBuilder.append("ORDER BY instance_id, app_id, app_version\n" +
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
                    map.put("hostId", resultSet.getString("host_id"));
                    map.put("instanceId", resultSet.getString("instance_id"));
                    map.put("appId", resultSet.getString("app_id"));
                    map.put("appVersion", resultSet.getString("app_version"));
                    map.put("active", resultSet.getBoolean("active"));
                    map.put("updateUser", resultSet.getString("update_user"));
                    // handling date properly
                    map.put("updateTs", resultSet.getTimestamp("update_ts") != null ? resultSet.getTimestamp("update_ts").toString() : null);
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

    @Override
    public Result<String> createConfigInstanceApp(ConfigInstanceAppCreatedEvent event) {
        final String sql = "INSERT INTO instance_app_property_t (host_id, instance_id, app_id, app_version, config_id, property_name, property_value, update_user, update_ts) " +
                "VALUES (?, ?, ?, ?, ?,  ?, ?, ?, ?)";
        Result<String> result;
        Timestamp timestamp = new Timestamp(event.getEventId().getTimestamp());
        String value = event.getValue(); // 'value' can contain optional fields
        Map<String, Object> map = JsonMapper.string2Map(value);

        try (Connection conn = ds.getConnection()) {
            conn.setAutoCommit(false);
            try (PreparedStatement statement = conn.prepareStatement(sql)) {
                statement.setString(1, event.getEventId().getHostId());
                statement.setString(2, event.getInstanceId());
                statement.setString(3, event.getAppId());
                statement.setString(4, event.getAppVersion());
                statement.setString(5, event.getConfigId());
                statement.setString(6, event.getPropertyName());
                if (map.containsKey("propertyValue")) {
                    statement.setString(7, (String)map.get("propertyValue"));
                } else {
                    statement.setNull(7, Types.VARCHAR);
                }
                statement.setString(8, event.getEventId().getUserId());
                statement.setTimestamp(9, timestamp);

                int count = statement.executeUpdate();
                if (count == 0) {
                    throw new SQLException("Failed to insert instance app for host_id: " + event.getEventId().getHostId() +
                            ", instance_id: " + event.getInstanceId() + ", app_id: " + event.getAppId() + ", app_version: " + event.getAppVersion());
                }
                conn.commit();
                result = Success.of(event.getEventId().getHostId() + "|" + event.getInstanceId() + "|" + event.getAppId() + "|" + event.getAppVersion()); // Composite key
                insertNotification(event.getEventId(), event.getClass().getName(), AvroConverter.toJson(event, false), true, null);


            } catch (SQLException e) {
                logger.error("SQLException:", e);
                conn.rollback();
                insertNotification(event.getEventId(), event.getClass().getName(), AvroConverter.toJson(event, false), false, e.getMessage());
                result = Failure.of(new Status("SQL_EXCEPTION", e.getMessage()));
            } catch (Exception e) {
                logger.error("Exception:", e);
                conn.rollback();
                insertNotification(event.getEventId(), event.getClass().getName(), AvroConverter.toJson(event, false), false, e.getMessage());
                result = Failure.of(new Status("GENERIC_EXCEPTION", e.getMessage()));
            }
        } catch (SQLException e) {
            logger.error("SQLException:", e);
            result = Failure.of(new Status("SQL_EXCEPTION", e.getMessage()));
        }
        return result;
    }


    @Override
    public Result<String> updateConfigInstanceApp(ConfigInstanceAppUpdatedEvent event) {
        final String sql = "UPDATE instance_app_property_t SET " +
                "property_value = ?, update_user = ?, update_ts = ? " +
                "WHERE host_id = ? AND instance_id = ? AND app_id = ? AND app_version = ? AND config_id = ? AND property_name = ?";

        Result<String> result;
        Timestamp timestamp = new Timestamp(event.getEventId().getTimestamp());
        String value = event.getValue();  // 'value' contains fields like 'active'
        Map<String, Object> map = JsonMapper.string2Map(value);

        try (Connection conn = ds.getConnection()) {
            conn.setAutoCommit(false);
            try (PreparedStatement statement = conn.prepareStatement(sql)) {
                if (map.containsKey("propertyValue")) {
                    statement.setString(1, (String)map.get("propertyValue"));
                } else {
                    statement.setNull(1, Types.VARCHAR);
                }
                statement.setString(2, event.getEventId().getUserId());
                statement.setTimestamp(3, timestamp);
                statement.setString(4, event.getEventId().getHostId());
                statement.setString(5, event.getInstanceId());
                statement.setString(6, event.getAppId());
                statement.setString(7, event.getAppVersion());
                statement.setString(8, event.getConfigId());
                statement.setString(9, event.getPropertyName());

                int count = statement.executeUpdate();
                if (count == 0) {
                    throw new SQLException("Failed to update instance app.  No rows affected for host_id: " + event.getEventId().getHostId() +
                            ", instance_id: " + event.getInstanceId() + ", app_id: " + event.getAppId() + ", app_version: " + event.getAppVersion() +
                            ", config_id: " + event.getConfigId() + ", property_name: " + event.getPropertyName());
                }
                conn.commit();
                result = Success.of(event.getEventId().getHostId() + "|" + event.getInstanceId() + "|" + event.getAppId() + "|" + event.getAppVersion() + "|" +  event.getConfigId() + "|" + event.getPropertyName()); // Composite key
                insertNotification(event.getEventId(), event.getClass().getName(), AvroConverter.toJson(event, false), true, null);

            } catch (SQLException e) {
                logger.error("SQLException:", e);
                conn.rollback();
                insertNotification(event.getEventId(), event.getClass().getName(), AvroConverter.toJson(event, false), false, e.getMessage());
                result = Failure.of(new Status("SQL_EXCEPTION", e.getMessage()));
            } catch (Exception e) {
                logger.error("Exception:", e);
                conn.rollback();
                insertNotification(event.getEventId(), event.getClass().getName(), AvroConverter.toJson(event, false), false, e.getMessage());
                result = Failure.of(new Status("GENERIC_EXCEPTION", e.getMessage()));
            }
        } catch (SQLException e) {
            logger.error("SQLException:", e);
            result = Failure.of(new Status("SQL_EXCEPTION", e.getMessage()));
        }
        return result;
    }


    @Override
    public Result<String> deleteConfigInstanceApp(ConfigInstanceAppDeletedEvent event) {
        final String sql = "DELETE FROM instance_app_property_t " +
                "WHERE host_id = ? AND instance_id = ? AND app_id = ? AND app_version = ? AND config_id = ? AND property_name = ?";
        Result<String> result;

        try (Connection conn = ds.getConnection()) {
            conn.setAutoCommit(false);
            try (PreparedStatement statement = conn.prepareStatement(sql)) {
                statement.setString(1, event.getEventId().getHostId());
                statement.setString(2, event.getInstanceId());
                statement.setString(3, event.getAppId());
                statement.setString(4, event.getAppVersion());
                statement.setString(5, event.getConfigId());
                statement.setString(6, event.getPropertyName());

                int count = statement.executeUpdate();
                if (count == 0) {
                    throw new SQLException("Failed to delete instance app. No rows affected for host_id: " + event.getEventId().getHostId() +
                            ", instance_id: " + event.getInstanceId() + ", app_id: " + event.getAppId() + ", app_version: " + event.getAppVersion());
                }
                conn.commit();
                result = Success.of(event.getEventId().getHostId() + "|" + event.getInstanceId() + "|" + event.getAppId() + "|" + event.getAppVersion());  // Composite key
                insertNotification(event.getEventId(), event.getClass().getName(), AvroConverter.toJson(event, false), true, null);

            } catch (SQLException e) {
                logger.error("SQLException:", e);
                conn.rollback();
                insertNotification(event.getEventId(), event.getClass().getName(), AvroConverter.toJson(event, false), false, e.getMessage());
                result = Failure.of(new Status("SQL_EXCEPTION", e.getMessage()));
            } catch (Exception e) {
                logger.error("Exception:", e);
                conn.rollback();
                insertNotification(event.getEventId(), event.getClass().getName(), AvroConverter.toJson(event, false), false, e.getMessage());
                result = Failure.of(new Status("GENERIC_EXCEPTION", e.getMessage()));
            }
        } catch (SQLException e) {
            logger.error("SQLException:", e);
            result = Failure.of(new Status("SQL_EXCEPTION", e.getMessage()));
        }
        return result;
    }

    @Override
    public Result<String> getConfigInstanceApp(int offset, int limit, String hostId, String instanceId, String instanceName,
                                               String appId, String appVersion, String configId, String configName,
                                               String propertyName, String propertyValue, String propertyFile) {
        Result<String> result = null;

        StringBuilder sqlBuilder = new StringBuilder();
        sqlBuilder.append("SELECT COUNT(*) OVER () AS total,\n" +
                "ia.host_id, ia.instance_id, i.instance_name, ia.app_id, ia.app_version, ia.active, ia.update_user, ia.update_ts,\n" +
                "iap.config_id, iap.property_name, iap.property_value, iap.property_file, c.config_name\n" +
                "FROM instance_app_t ia\n" +
                "INNER JOIN instance_t i ON ia.host_id =i.host_id AND ia.instance_id = i.instance_id \n" +
                "INNER JOIN instance_app_property_t iap ON ia.host_id = iap.host_id AND ia.instance_id = iap.instance_id AND ia.app_id = iap.app_id AND ia.app_version = iap.app_version\n" +
                "INNER JOIN config_t c ON iap.config_id = c.config_id\n" +
                "WHERE 1=1\n");

        List<Object> parameters = new ArrayList<>();

        StringBuilder whereClause = new StringBuilder();
        addCondition(whereClause, parameters, "ia.host_id", hostId);
        addCondition(whereClause, parameters, "ia.instance_id", instanceId);
        addCondition(whereClause, parameters, "i.instance_name", instanceName);
        addCondition(whereClause, parameters, "ia.app_id", appId);
        addCondition(whereClause, parameters, "ia.app_version", appVersion);
        addCondition(whereClause, parameters, "iap.config_id", configId);
        addCondition(whereClause, parameters, "c.config_name", configName); // Filter by config_name
        addCondition(whereClause, parameters, "iap.property_name", propertyName);
        addCondition(whereClause, parameters, "iap.property_value", propertyValue);
        addCondition(whereClause, parameters, "iap.property_file", propertyFile);


        if (whereClause.length() > 0) {
            sqlBuilder.append("AND ").append(whereClause);
        }

        sqlBuilder.append(" ORDER BY ia.host_id, ia.instance_id, ia.app_id, ia.app_version, iap.config_id, iap.property_name\n" +
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
                    map.put("hostId", resultSet.getString("host_id"));
                    map.put("instanceId", resultSet.getString("instance_id"));
                    map.put("instanceName", resultSet.getString("instance_name"));
                    map.put("appId", resultSet.getString("app_id"));
                    map.put("appVersion", resultSet.getString("app_version"));
                    map.put("configId", resultSet.getString("config_id"));
                    map.put("configName", resultSet.getString("config_name")); // Get from joined table
                    map.put("propertyName", resultSet.getString("property_name"));
                    map.put("propertyValue", resultSet.getString("property_value"));
                    map.put("propertyFile", resultSet.getString("property_file"));
                    map.put("updateUser", resultSet.getString("update_user"));
                    map.put("updateTs", resultSet.getTimestamp("update_ts") != null ? resultSet.getTimestamp("update_ts").toString() : null);
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
    public Result<String> createConfigInstance(ConfigInstanceCreatedEvent event) {
        // The table is now instance_property_t, NOT instance_t
        final String sql = "INSERT INTO instance_property_t (host_id, instance_id, config_id, property_name, " +
                "property_value, property_file, update_user, update_ts) VALUES (?, ?, ?, ?, ?, ?, ?, ?)";
        Result<String> result;
        Timestamp timestamp = new Timestamp(event.getEventId().getTimestamp());
        String value = event.getValue();  // 'value' contains the property details
        Map<String, Object> map = JsonMapper.string2Map(value);

        try (Connection conn = ds.getConnection()) {
            conn.setAutoCommit(false);
            try (PreparedStatement statement = conn.prepareStatement(sql)) {
                statement.setString(1, event.getEventId().getHostId());
                statement.setString(2, event.getInstanceId());
                statement.setString(3, event.getConfigId()); // Now using configId from the event
                statement.setString(4, event.getPropertyName());

                // Handle 'property_value' (optional)
                if (map.containsKey("propertyValue")) {
                    statement.setString(5, (String) map.get("propertyValue"));
                } else {
                    statement.setNull(5, Types.VARCHAR);
                }

                // Handle 'property_file' (optional)
                if (map.containsKey("propertyFile")) {
                    statement.setString(6, (String) map.get("propertyFile"));
                } else {
                    statement.setNull(6, Types.VARCHAR);
                }

                statement.setString(7, event.getEventId().getUserId());
                statement.setTimestamp(8, timestamp);

                int count = statement.executeUpdate();
                if (count == 0) {
                    throw new SQLException("Failed to insert instance property for host_id: " + event.getEventId().getHostId() +
                            ", instance_id: " + event.getInstanceId() + ", config_id: " + event.getConfigId() +
                            ", property_name: " + event.getPropertyName());
                }
                conn.commit();
                result = Success.of(event.getEventId().getHostId() + "|" + event.getInstanceId() + "|" + event.getConfigId() + "|" + event.getPropertyName()); // Composite key
                insertNotification(event.getEventId(), event.getClass().getName(), AvroConverter.toJson(event, false), true, null);


            } catch (SQLException e) {
                logger.error("SQLException:", e);
                conn.rollback();
                insertNotification(event.getEventId(), event.getClass().getName(), AvroConverter.toJson(event, false), false, e.getMessage());
                result = Failure.of(new Status("SQL_EXCEPTION", e.getMessage()));
            } catch (Exception e) {
                logger.error("Exception:", e);
                conn.rollback();
                insertNotification(event.getEventId(), event.getClass().getName(), AvroConverter.toJson(event, false), false, e.getMessage());
                result = Failure.of(new Status("GENERIC_EXCEPTION", e.getMessage()));
            }
        } catch (SQLException e) {
            logger.error("SQLException:", e);
            result = Failure.of(new Status("SQL_EXCEPTION", e.getMessage()));
        }
        return result;
    }


    @Override
    public Result<String> updateConfigInstance(ConfigInstanceUpdatedEvent event) {
        final String sql = "UPDATE instance_property_t SET property_value = ?, property_file = ?, update_user = ?, update_ts = ? " +
                "WHERE host_id = ? AND instance_id = ? AND config_id = ? AND property_name = ?";
        Result<String> result;
        Timestamp timestamp = new Timestamp(event.getEventId().getTimestamp());
        String value = event.getValue();  // 'value' contains updated property values
        Map<String, Object> map = JsonMapper.string2Map(value);

        try (Connection conn = ds.getConnection()) {
            conn.setAutoCommit(false);
            try (PreparedStatement statement = conn.prepareStatement(sql)) {

                // Handle 'property_value' (optional)
                if (map.containsKey("propertyValue")) {
                    statement.setString(1, (String) map.get("propertyValue"));
                } else {
                    statement.setNull(1, Types.VARCHAR); // Or keep existing
                }

                // Handle 'property_file' (optional)
                if (map.containsKey("propertyFile")) {
                    statement.setString(2, (String) map.get("propertyFile"));
                } else {
                    statement.setNull(2, Types.VARCHAR); // Or keep existing
                }

                statement.setString(3, event.getEventId().getUserId());
                statement.setTimestamp(4, timestamp);

                // WHERE clause parameters (from the event, NOT the JSON)
                statement.setString(5, event.getEventId().getHostId());
                statement.setString(6, event.getInstanceId());
                statement.setString(7, event.getConfigId());
                statement.setString(8, event.getPropertyName());

                int count = statement.executeUpdate();
                if (count == 0) {
                    throw new SQLException("Failed to update instance property. No rows affected for host_id: " + event.getEventId().getHostId() +
                            ", instance_id: " + event.getInstanceId() + ", config_id: " + event.getConfigId() +
                            ", property_name: " + event.getPropertyName());
                }
                conn.commit();
                result = Success.of(event.getEventId().getHostId() + "|" + event.getInstanceId() + "|" + event.getConfigId() + "|" + event.getPropertyName()); // Composite key

                insertNotification(event.getEventId(), event.getClass().getName(), AvroConverter.toJson(event, false), true, null);

            } catch (SQLException e) {
                logger.error("SQLException:", e);
                conn.rollback();
                insertNotification(event.getEventId(), event.getClass().getName(), AvroConverter.toJson(event, false), false, e.getMessage());
                result = Failure.of(new Status("SQL_EXCEPTION", e.getMessage()));
            } catch (Exception e) {
                logger.error("Exception:", e);
                conn.rollback();
                insertNotification(event.getEventId(), event.getClass().getName(), AvroConverter.toJson(event, false), false, e.getMessage());
                result = Failure.of(new Status("GENERIC_EXCEPTION", e.getMessage()));
            }
        } catch (SQLException e) {
            logger.error("SQLException:", e);
            result = Failure.of(new Status("SQL_EXCEPTION", e.getMessage()));
        }
        return result;
    }


    @Override
    public Result<String> deleteConfigInstance(ConfigInstanceDeletedEvent event) {
        final String sql = "DELETE FROM instance_property_t WHERE host_id = ? AND instance_id = ? AND config_id = ? AND property_name = ?";
        Result<String> result;

        try (Connection conn = ds.getConnection()) {
            conn.setAutoCommit(false);
            try (PreparedStatement statement = conn.prepareStatement(sql)) {
                statement.setString(1, event.getEventId().getHostId());
                statement.setString(2, event.getInstanceId());
                statement.setString(3, event.getConfigId());
                statement.setString(4, event.getPropertyName());

                int count = statement.executeUpdate();
                if (count == 0) {
                    throw new SQLException("Failed to delete instance property. No rows affected for host_id: " + event.getEventId().getHostId() +
                            ", instance_id: " + event.getInstanceId() + ", config_id: " + event.getConfigId() +
                            ", property_name: " + event.getPropertyName());
                }
                conn.commit();
                result = Success.of(event.getEventId().getHostId() + "|" + event.getInstanceId() + "|" + event.getConfigId() + "|" + event.getPropertyName()); // Composite key
                insertNotification(event.getEventId(), event.getClass().getName(), AvroConverter.toJson(event, false), true, null);

            } catch (SQLException e) {
                logger.error("SQLException:", e);
                conn.rollback();
                insertNotification(event.getEventId(), event.getClass().getName(), AvroConverter.toJson(event, false), false, e.getMessage());
                result = Failure.of(new Status("SQL_EXCEPTION", e.getMessage()));
            } catch (Exception e) {
                logger.error("Exception:", e);
                conn.rollback();
                insertNotification(event.getEventId(), event.getClass().getName(), AvroConverter.toJson(event, false), false, e.getMessage());
                result = Failure.of(new Status("GENERIC_EXCEPTION", e.getMessage()));
            }
        } catch (SQLException e) {
            logger.error("SQLException:", e);
            result = Failure.of(new Status("SQL_EXCEPTION", e.getMessage()));
        }
        return result;
    }

    @Override
    public Result<String> getConfigInstance(int offset, int limit, String hostId, String instanceId,
                                            String configId, String configName,
                                            String propertyName, String propertyValue, String propertyFile) {
        Result<String> result = null;

        StringBuilder sqlBuilder = new StringBuilder();
        sqlBuilder.append("SELECT COUNT(*) OVER () AS total,\n" +
                "ip.host_id, ip.instance_id, ip.config_id, ip.property_name, ip.property_value, ip.property_file, " +
                "ip.update_user, ip.update_ts, c.config_name \n" +
                "FROM instance_property_t ip\n" +
                "LEFT JOIN config_t c ON ip.config_id = c.config_id\n" +
                "WHERE 1=1\n");

        List<Object> parameters = new ArrayList<>();

        StringBuilder whereClause = new StringBuilder();
        addCondition(whereClause, parameters, "ip.host_id", hostId);
        addCondition(whereClause, parameters, "ip.instance_id", instanceId);
        addCondition(whereClause, parameters, "ip.config_id", configId);
        addCondition(whereClause, parameters, "c.config_name", configName); // Filter by config_name
        addCondition(whereClause, parameters, "ip.property_name", propertyName);
        addCondition(whereClause, parameters, "ip.property_value", propertyValue);
        addCondition(whereClause, parameters, "ip.property_file", propertyFile);

        if (whereClause.length() > 0) {
            sqlBuilder.append("AND ").append(whereClause);
        }

        sqlBuilder.append(" ORDER BY ip.host_id, ip.instance_id, ip.config_id, ip.property_name\n" +
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
                    map.put("hostId", resultSet.getString("host_id"));
                    map.put("instanceId", resultSet.getString("instance_id"));
                    map.put("configId", resultSet.getString("config_id"));
                    map.put("configName", resultSet.getString("config_name")); // Get from joined table
                    map.put("propertyName", resultSet.getString("property_name"));
                    map.put("propertyValue", resultSet.getString("property_value"));
                    map.put("propertyFile", resultSet.getString("property_file"));
                    map.put("updateUser", resultSet.getString("update_user"));
                    map.put("updateTs", resultSet.getTimestamp("update_ts") != null ? resultSet.getTimestamp("update_ts").toString() : null);

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
    public Result<String> createConfigProduct(ConfigProductCreatedEvent event) {
        final String sql = "INSERT INTO product_property_t (product_id, config_id, property_name, property_value, property_file, update_user, update_ts) VALUES (?, ?, ?, ?, ?, ?, ?)";
        Result<String> result;
        Timestamp timestamp = new Timestamp(event.getEventId().getTimestamp());
        String value = event.getValue(); // 'value' contains optional property details
        Map<String, Object> map = JsonMapper.string2Map(value);

        try (Connection conn = ds.getConnection()) {
            conn.setAutoCommit(false);
            try (PreparedStatement statement = conn.prepareStatement(sql)) {
                statement.setString(1, event.getProductId());
                statement.setString(2, event.getConfigId());
                statement.setString(3, event.getPropertyName());

                // Handle 'property_value' (optional)
                if (map.containsKey("propertyValue")) {
                    statement.setString(4, (String) map.get("propertyValue"));
                } else {
                    statement.setNull(4, Types.VARCHAR);
                }

                // Handle 'property_file' (optional)
                if (map.containsKey("propertyFile")) {
                    statement.setString(5, (String) map.get("propertyFile"));
                } else {
                    statement.setNull(5, Types.VARCHAR);
                }

                statement.setString(6, event.getEventId().getUserId());
                statement.setTimestamp(7, timestamp);

                int count = statement.executeUpdate();
                if (count == 0) {
                    throw new SQLException("Failed to insert product property for product_id: " + event.getProductId() +
                            ", config_id: " + event.getConfigId() + ", property_name: " + event.getPropertyName());
                }
                conn.commit();
                result = Success.of(event.getProductId() + "|" + event.getConfigId() + "|" + event.getPropertyName()); // Composite key
                insertNotification(event.getEventId(), event.getClass().getName(), AvroConverter.toJson(event, false), true, null);

            } catch (SQLException e) {
                logger.error("SQLException:", e);
                conn.rollback();
                insertNotification(event.getEventId(), event.getClass().getName(), AvroConverter.toJson(event, false), false, e.getMessage());
                result = Failure.of(new Status("SQL_EXCEPTION", e.getMessage()));
            } catch (Exception e) {
                logger.error("Exception:", e);
                conn.rollback();
                insertNotification(event.getEventId(), event.getClass().getName(), AvroConverter.toJson(event, false), false, e.getMessage());
                result = Failure.of(new Status("GENERIC_EXCEPTION", e.getMessage()));
            }
        } catch (SQLException e) {
            logger.error("SQLException:", e);
            result = Failure.of(new Status("SQL_EXCEPTION", e.getMessage()));
        }
        return result;
    }

    @Override
    public Result<String> updateConfigProduct(ConfigProductUpdatedEvent event) {
        final String sql = "UPDATE product_property_t SET property_value = ?, property_file = ?, update_user = ?, update_ts = ? " +
                "WHERE product_id = ? AND config_id = ? AND property_name = ?";
        Result<String> result;
        Timestamp timestamp = new Timestamp(event.getEventId().getTimestamp());
        String value = event.getValue(); // 'value' contains updated property details
        Map<String, Object> map = JsonMapper.string2Map(value);

        try (Connection conn = ds.getConnection()) {
            conn.setAutoCommit(false);
            try (PreparedStatement statement = conn.prepareStatement(sql)) {

                // Handle 'property_value' (optional)
                if (map.containsKey("propertyValue")) {
                    statement.setString(1, (String) map.get("propertyValue"));
                } else {
                    statement.setNull(1, Types.VARCHAR); // Or keep existing
                }

                // Handle 'property_file' (optional)
                if (map.containsKey("propertyFile")) {
                    statement.setString(2, (String) map.get("propertyFile"));
                } else {
                    statement.setNull(2, Types.VARCHAR); // Or keep existing
                }

                statement.setString(3, event.getEventId().getUserId());
                statement.setTimestamp(4, timestamp);

                // WHERE clause parameters (from the event, NOT the JSON)
                statement.setString(5, event.getProductId());
                statement.setString(6, event.getConfigId());
                statement.setString(7, event.getPropertyName());

                int count = statement.executeUpdate();
                if (count == 0) {
                    throw new SQLException("Failed to update product property. No rows affected for product_id: " + event.getProductId() +
                            ", config_id: " + event.getConfigId() + ", property_name: " + event.getPropertyName());
                }
                conn.commit();
                result = Success.of(event.getProductId() + "|" + event.getConfigId() + "|" + event.getPropertyName()); // Composite Key
                insertNotification(event.getEventId(), event.getClass().getName(), AvroConverter.toJson(event, false), true, null);

            } catch (SQLException e) {
                logger.error("SQLException:", e);
                conn.rollback();
                insertNotification(event.getEventId(), event.getClass().getName(), AvroConverter.toJson(event, false), false, e.getMessage());
                result = Failure.of(new Status("SQL_EXCEPTION", e.getMessage()));
            } catch (Exception e) {
                logger.error("Exception:", e);
                conn.rollback();
                insertNotification(event.getEventId(), event.getClass().getName(), AvroConverter.toJson(event, false), false, e.getMessage());
                result = Failure.of(new Status("GENERIC_EXCEPTION", e.getMessage()));
            }
        } catch (SQLException e) {
            logger.error("SQLException:", e);
            result = Failure.of(new Status("SQL_EXCEPTION", e.getMessage()));
        }
        return result;
    }

    @Override
    public Result<String> deleteConfigProduct(ConfigProductDeletedEvent event) {
        final String sql = "DELETE FROM product_property_t WHERE product_id = ? AND config_id = ? AND property_name = ?";
        Result<String> result;

        try (Connection conn = ds.getConnection()) {
            conn.setAutoCommit(false);
            try (PreparedStatement statement = conn.prepareStatement(sql)) {
                statement.setString(1, event.getProductId());
                statement.setString(2, event.getConfigId());
                statement.setString(3, event.getPropertyName());

                int count = statement.executeUpdate();
                if (count == 0) {
                    throw new SQLException("Failed to delete product property. No rows affected for product_id: " + event.getProductId() +
                            ", config_id: " + event.getConfigId() + ", property_name: " + event.getPropertyName());
                }
                conn.commit();
                result = Success.of(event.getProductId() + "|" + event.getConfigId() + "|" + event.getPropertyName()); // Composite key
                insertNotification(event.getEventId(), event.getClass().getName(), AvroConverter.toJson(event, false), true, null);

            } catch (SQLException e) {
                logger.error("SQLException:", e);
                conn.rollback();
                insertNotification(event.getEventId(), event.getClass().getName(), AvroConverter.toJson(event, false), false, e.getMessage());
                result = Failure.of(new Status("SQL_EXCEPTION", e.getMessage()));
            } catch (Exception e) {
                logger.error("Exception:", e);
                conn.rollback();
                insertNotification(event.getEventId(), event.getClass().getName(), AvroConverter.toJson(event, false), false, e.getMessage());
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
                                           String configId, String configName,
                                           String propertyName, String propertyValue, String propertyFile) {
        Result<String> result = null;

        StringBuilder sqlBuilder = new StringBuilder();
        sqlBuilder.append("SELECT COUNT(*) OVER () AS total,\n" +
                "pp.product_id, pp.config_id, pp.property_name, pp.property_value, pp.property_file, pp.update_user, pp.update_ts, \n" +
                "c.config_name \n" + // Include config_name from config_t
                "FROM product_property_t pp\n" +
                "LEFT JOIN config_t c ON pp.config_id = c.config_id\n" + // Left join with config_t
                "WHERE 1=1\n");

        List<Object> parameters = new ArrayList<>();

        StringBuilder whereClause = new StringBuilder();
        addCondition(whereClause, parameters, "pp.product_id", productId);
        addCondition(whereClause, parameters, "pp.config_id", configId);
        addCondition(whereClause, parameters, "c.config_name", configName); // Filter by config_name
        addCondition(whereClause, parameters, "pp.property_name", propertyName);
        addCondition(whereClause, parameters, "pp.property_value", propertyValue);
        addCondition(whereClause, parameters, "pp.property_file", propertyFile);

        if (whereClause.length() > 0) {
            sqlBuilder.append("AND ").append(whereClause);
        }

        sqlBuilder.append(" ORDER BY pp.product_id, pp.config_id, pp.property_name\n" +
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
                    map.put("configId", resultSet.getString("config_id"));
                    map.put("configName", resultSet.getString("config_name"));
                    map.put("propertyName", resultSet.getString("property_name"));
                    map.put("propertyValue", resultSet.getString("property_value"));
                    map.put("propertyFile", resultSet.getString("property_file"));
                    map.put("updateUser", resultSet.getString("update_user"));
                    map.put("updateTs", resultSet.getTimestamp("update_ts") != null ? resultSet.getTimestamp("update_ts").toString() : null);

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
    public Result<String> createConfigProductVersion(ConfigProductVersionCreatedEvent event) {
        final String sql = "INSERT INTO product_version_property_t (host_id, product_id, product_version, " +
                "config_id, property_name, property_value, property_file, update_user, update_ts) " +
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)";
        Result<String> result;
        Timestamp timestamp = new Timestamp(event.getEventId().getTimestamp());
        String value = event.getValue(); // 'value' contains optional property details
        Map<String, Object> map = JsonMapper.string2Map(value);

        try (Connection conn = ds.getConnection()) {
            conn.setAutoCommit(false);
            try (PreparedStatement statement = conn.prepareStatement(sql)) {
                statement.setString(1, event.getEventId().getHostId());
                statement.setString(2, event.getProductId());
                statement.setString(3, event.getProductVersion());
                statement.setString(4, event.getConfigId());
                statement.setString(5, event.getPropertyName());

                // Handle 'property_value' (optional)
                if (map.containsKey("propertyValue")) {
                    statement.setString(6, (String) map.get("propertyValue"));
                } else {
                    statement.setNull(6, Types.VARCHAR);
                }

                // Handle 'property_file' (optional)
                if (map.containsKey("propertyFile")) {
                    statement.setString(7, (String) map.get("propertyFile"));
                } else {
                    statement.setNull(7, Types.VARCHAR);
                }

                statement.setString(8, event.getEventId().getUserId());
                statement.setTimestamp(9, timestamp);

                int count = statement.executeUpdate();
                if (count == 0) {
                    throw new SQLException("Failed to insert product version property for host_id: " + event.getEventId().getHostId() +
                            ", product_id: " + event.getProductId() + ", product_version: " + event.getProductVersion() +
                            ", config_id: " + event.getConfigId() + ", property_name: " + event.getPropertyName());
                }
                conn.commit();
                result = Success.of(event.getEventId().getHostId() + "|" + event.getProductId() + "|" + event.getProductVersion() + "|" + event.getConfigId() + "|" + event.getPropertyName()); // Composite key
                insertNotification(event.getEventId(), event.getClass().getName(), AvroConverter.toJson(event, false), true, null);

            } catch (SQLException e) {
                logger.error("SQLException:", e);
                conn.rollback();
                insertNotification(event.getEventId(), event.getClass().getName(), AvroConverter.toJson(event, false), false, e.getMessage());
                result = Failure.of(new Status("SQL_EXCEPTION", e.getMessage()));
            } catch (Exception e) {
                logger.error("Exception:", e);
                conn.rollback();
                insertNotification(event.getEventId(), event.getClass().getName(), AvroConverter.toJson(event, false), false, e.getMessage());
                result = Failure.of(new Status("GENERIC_EXCEPTION", e.getMessage()));
            }
        } catch (SQLException e) {
            logger.error("SQLException:", e);
            result = Failure.of(new Status("SQL_EXCEPTION", e.getMessage()));
        }
        return result;
    }

    @Override
    public Result<String> updateConfigProductVersion(ConfigProductVersionUpdatedEvent event) {
        final String sql = "UPDATE product_version_property_t SET property_value = ?, property_file = ?, update_user = ?, update_ts = ? " +
                "WHERE host_id = ? AND product_id = ? AND product_version = ? AND config_id = ? AND property_name = ?";
        Result<String> result;
        Timestamp timestamp = new Timestamp(event.getEventId().getTimestamp());
        String value = event.getValue();  // 'value' contains updated property details
        Map<String, Object> map = JsonMapper.string2Map(value);

        try (Connection conn = ds.getConnection()) {
            conn.setAutoCommit(false);
            try (PreparedStatement statement = conn.prepareStatement(sql)) {

                // Handle 'property_value' (optional)
                if (map.containsKey("propertyValue")) {
                    statement.setString(1, (String) map.get("propertyValue"));
                } else {
                    statement.setNull(1, Types.VARCHAR); // Or keep existing
                }

                // Handle 'property_file' (optional)
                if (map.containsKey("propertyFile")) {
                    statement.setString(2, (String) map.get("propertyFile"));
                } else {
                    statement.setNull(2, Types.VARCHAR); // Or keep existing
                }

                statement.setString(3, event.getEventId().getUserId());
                statement.setTimestamp(4, timestamp);

                // WHERE clause parameters (from the event, NOT the JSON)
                statement.setString(5, event.getEventId().getHostId());
                statement.setString(6, event.getProductId());
                statement.setString(7, event.getProductVersion());
                statement.setString(8, event.getConfigId());
                statement.setString(9, event.getPropertyName());

                int count = statement.executeUpdate();
                if (count == 0) {
                    throw new SQLException("Failed to update product version property. No rows affected for host_id: " + event.getEventId().getHostId() +
                            ", product_id: " + event.getProductId() + ", product_version: " + event.getProductVersion() +
                            ", config_id: " + event.getConfigId() + ", property_name: " + event.getPropertyName());
                }
                conn.commit();
                result = Success.of(event.getEventId().getHostId() + "|" + event.getProductId() + "|" + event.getProductVersion() + "|" + event.getConfigId() + "|" + event.getPropertyName());
                insertNotification(event.getEventId(), event.getClass().getName(), AvroConverter.toJson(event, false), true, null);

            } catch (SQLException e) {
                logger.error("SQLException:", e);
                conn.rollback();
                insertNotification(event.getEventId(), event.getClass().getName(), AvroConverter.toJson(event, false), false, e.getMessage());
                result = Failure.of(new Status("SQL_EXCEPTION", e.getMessage()));
            } catch (Exception e) {
                logger.error("Exception:", e);
                conn.rollback();
                insertNotification(event.getEventId(), event.getClass().getName(), AvroConverter.toJson(event, false), false, e.getMessage());
                result = Failure.of(new Status("GENERIC_EXCEPTION", e.getMessage()));
            }
        } catch (SQLException e) {
            logger.error("SQLException:", e);
            result = Failure.of(new Status("SQL_EXCEPTION", e.getMessage()));
        }
        return result;
    }


    @Override
    public Result<String> deleteConfigProductVersion(ConfigProductVersionDeletedEvent event) {
        final String sql = "DELETE FROM product_version_property_t WHERE host_id = ? AND product_id = ? " +
                "AND product_version = ? AND config_id = ? AND property_name = ?";
        Result<String> result;

        try (Connection conn = ds.getConnection()) {
            conn.setAutoCommit(false);
            try (PreparedStatement statement = conn.prepareStatement(sql)) {
                statement.setString(1, event.getEventId().getHostId());
                statement.setString(2, event.getProductId());
                statement.setString(3, event.getProductVersion());
                statement.setString(4, event.getConfigId());
                statement.setString(5, event.getPropertyName());

                int count = statement.executeUpdate();
                if (count == 0) {
                    throw new SQLException("Failed to delete product version property. No rows affected for host_id: " + event.getEventId().getHostId() +
                            ", product_id: " + event.getProductId() + ", product_version: " + event.getProductVersion() +
                            ", config_id: " + event.getConfigId() + ", property_name: " + event.getPropertyName());
                }
                conn.commit();
                result = Success.of(event.getEventId().getHostId() + "|" + event.getProductId() + "|" + event.getProductVersion() + "|" + event.getConfigId() + "|" + event.getPropertyName()); // Composite key
                insertNotification(event.getEventId(), event.getClass().getName(), AvroConverter.toJson(event, false), true, null);
            } catch (SQLException e) {
                logger.error("SQLException:", e);
                conn.rollback();
                insertNotification(event.getEventId(), event.getClass().getName(), AvroConverter.toJson(event, false), false, e.getMessage());
                result = Failure.of(new Status("SQL_EXCEPTION", e.getMessage()));
            } catch (Exception e) {
                logger.error("Exception:", e);
                conn.rollback();
                insertNotification(event.getEventId(), event.getClass().getName(), AvroConverter.toJson(event, false), false, e.getMessage());
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
                                                  String configId, String configName,
                                                  String propertyName, String propertyValue, String propertyFile) {
        Result<String> result = null;

        StringBuilder sqlBuilder = new StringBuilder();
        sqlBuilder.append("SELECT COUNT(*) OVER () AS total,\n" +
                "pvp.host_id, pvp.product_id, pvp.product_version, pvp.config_id, pvp.property_name, " +
                "pvp.property_value, pvp.property_file, pvp.update_user, pvp.update_ts, \n" +
                "c.config_name \n" + // Include config_name from config_t
                "FROM product_version_property_t pvp\n" +
                "LEFT JOIN config_t c ON pvp.config_id = c.config_id\n" +  // Left join with config_t
                "WHERE 1=1\n");

        List<Object> parameters = new ArrayList<>();

        StringBuilder whereClause = new StringBuilder();
        addCondition(whereClause, parameters, "pvp.host_id", hostId);
        addCondition(whereClause, parameters, "pvp.product_id", productId);
        addCondition(whereClause, parameters, "pvp.product_version", productVersion);
        addCondition(whereClause, parameters, "pvp.config_id", configId);
        addCondition(whereClause, parameters, "c.config_name", configName);  // Filter by config_name
        addCondition(whereClause, parameters, "pvp.property_name", propertyName);
        addCondition(whereClause, parameters, "pvp.property_value", propertyValue);
        addCondition(whereClause, parameters, "pvp.property_file", propertyFile);


        if (whereClause.length() > 0) {
            sqlBuilder.append("AND ").append(whereClause);
        }

        sqlBuilder.append(" ORDER BY pvp.host_id, pvp.product_id, pvp.product_version, pvp.config_id, pvp.property_name\n" +
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
                    map.put("hostId", resultSet.getString("host_id"));
                    map.put("productId", resultSet.getString("product_id"));
                    map.put("productVersion", resultSet.getString("product_version"));
                    map.put("configId", resultSet.getString("config_id"));
                    map.put("configName", resultSet.getString("config_name")); // Get from joined table
                    map.put("propertyName", resultSet.getString("property_name"));
                    map.put("propertyValue", resultSet.getString("property_value"));
                    map.put("propertyFile", resultSet.getString("property_file"));
                    map.put("updateUser", resultSet.getString("update_user"));
                    map.put("updateTs", resultSet.getTimestamp("update_ts") != null ? resultSet.getTimestamp("update_ts").toString() : null);

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
    public Result<Map<String, Object>> queryCurrentProviderKey(String providerId) {
        final String queryConfigById = "SELECT provider_id, kid, public_key, " +
                "private_key, key_type, update_user, update_ts " +
                "FROM auth_provider_key_t WHERE provider_id = ? AND key_type = 'TC'";
        Result<Map<String, Object>> result;
        try (final Connection conn = ds.getConnection()) {
            Map<String, Object> map = new HashMap<>();
            try (PreparedStatement statement = conn.prepareStatement(queryConfigById)) {
                statement.setString(1, providerId);
                try (ResultSet resultSet = statement.executeQuery()) {
                    if (resultSet.next()) {
                        map.put("providerId", resultSet.getString("provider_id"));
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
                result = Failure.of(new Status(OBJECT_NOT_FOUND, "provider key with id", providerId));
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
    public Result<Map<String, Object>> queryLongLiveProviderKey(String providerId) {
        final String queryConfigById = "SELECT provider_id, kid, public_key, " +
                "private_key, key_type, update_user, update_ts " +
                "FROM auth_provider_key_t WHERE provider_id = ? AND key_type = 'LC'";
        Result<Map<String, Object>> result;
        try (final Connection conn = ds.getConnection()) {
            Map<String, Object> map = new HashMap<>();
            try (PreparedStatement statement = conn.prepareStatement(queryConfigById)) {
                statement.setString(1, providerId);
                try (ResultSet resultSet = statement.executeQuery()) {
                    if (resultSet.next()) {
                        map.put("providerId", resultSet.getString("provider_id"));
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
                result = Failure.of(new Status(OBJECT_NOT_FOUND, "provider key with id", providerId));
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

    @Override
    public Result<String> createProduct(ProductCreatedEvent event) {
        final String sql = "INSERT INTO product_version_t(host_id, product_id, product_version, " +
                "light4j_version, break_code, break_config, release_note, version_desc, release_type, current, " +
                "version_status, update_user, update_ts) " +
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
        final String sqlUpdate = "UPDATE product_version_t SET current = false \n" +
                "WHERE host_id = ?\n" +
                "AND product_id = ?\n" +
                "AND product_version != ?";
        Result<String> result;
        Timestamp timestamp = new Timestamp(event.getEventId().getTimestamp());
        try (Connection conn = ds.getConnection()) {
            conn.setAutoCommit(false);
            try (PreparedStatement statement = conn.prepareStatement(sql)) {
                statement.setString(1, event.getEventId().getHostId());
                String value = event.getValue();
                Map<String, Object> map = JsonMapper.string2Map(value);
                statement.setString(2, event.getProductId());
                statement.setString(3, event.getProductVersion());
                statement.setString(4, event.getLight4jVersion());
                if (map.containsKey("breakCode")) {
                    statement.setBoolean(5, (Boolean) map.get("breakCode"));
                } else {
                    statement.setNull(5, Types.BOOLEAN);
                }
                if (map.containsKey("breakConfig")) {
                    statement.setBoolean(6, (Boolean) map.get("breakConfig"));
                } else {
                    statement.setNull(6, Types.BOOLEAN);
                }
                if (map.containsKey("releaseNote")) {
                    statement.setString(7, (String) map.get("releaseNote"));
                } else {
                    statement.setNull(7, Types.VARCHAR);
                }
                if (map.containsKey("versionDesc")) {
                    statement.setString(8, (String) map.get("versionDesc"));
                } else {
                    statement.setNull(8, Types.VARCHAR);
                }
                statement.setString(9, event.getReleaseType());
                statement.setBoolean(10, event.getCurrent());
                statement.setString(11, event.getVersionStatus());
                statement.setString(12, event.getEventId().getId());
                statement.setTimestamp(13, timestamp);

                int count = statement.executeUpdate();
                if (count == 0) {
                    throw new SQLException("failed to insert the product with id " + event.getProductId());
                }
                // try to update current to false for others if current is true.
                if(event.getCurrent()) {
                    try (PreparedStatement statementUpdate = conn.prepareStatement(sqlUpdate)) {
                        statementUpdate.setString(1, event.getEventId().getHostId());
                        statementUpdate.setString(2, event.getProductId());
                        statementUpdate.setString(3, event.getProductVersion());
                        statementUpdate.executeUpdate();
                    }
                }
                conn.commit();
                result = Success.of((String)map.get("productId"));
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
    public Result<String> updateProduct(ProductUpdatedEvent event) {
        final String sql = "UPDATE product_version_t SET light4j_version = ?, break_code = ?, break_config = ?, " +
                "release_note = ?, version_desc = ?, release_type = ?, current = ?, version_status = ?, update_user = ?, update_ts = ? " +
                "WHERE host_id = ? and product_id = ? and product_version = ?";
        final String sqlUpdate = "UPDATE product_version_t SET current = false \n" +
                "WHERE host_id = ?\n" +
                "AND product_id = ?\n" +
                "AND product_version != ?";

        Result<String> result;
        Timestamp timestamp = new Timestamp(event.getEventId().getTimestamp());
        String value = event.getValue();
        Map<String, Object> map = JsonMapper.string2Map(value);

        try (Connection conn = ds.getConnection()) {
            conn.setAutoCommit(false);
            try (PreparedStatement statement = conn.prepareStatement(sql)) {
                statement.setString(1, event.getLight4jVersion());
                if (map.containsKey("breakCode")) {
                    statement.setBoolean(2, (Boolean) map.get("breakCode"));
                } else {
                    statement.setNull(2, Types.BOOLEAN);
                }
                if (map.containsKey("breakConfig")) {
                    statement.setBoolean(3, (Boolean) map.get("breakConfig"));
                } else {
                    statement.setNull(3, Types.BOOLEAN);
                }
                if (map.containsKey("releaseNote")) {
                    statement.setString(4, (String) map.get("releaseNote"));
                } else {
                    statement.setNull(4, Types.VARCHAR);
                }

                if (map.containsKey("versionDesc")) {
                    statement.setString(5, (String) map.get("versionDesc"));
                } else {
                    statement.setNull(5, Types.VARCHAR);
                }
                statement.setString(6, event.getReleaseType());
                statement.setBoolean(7, event.getCurrent());
                statement.setString(8, event.getVersionStatus());
                statement.setString(9, event.getEventId().getId());
                statement.setTimestamp(10, timestamp);
                statement.setString(11, event.getEventId().getHostId());
                statement.setString(12, event.getProductId());
                statement.setString(13, event.getProductVersion());
                int count = statement.executeUpdate();
                if (count == 0) {
                    throw new SQLException("failed to update the product with id " + event.getProductId());
                }
                // try to update current to false for others if current is true.
                if(event.getCurrent()) {
                    try (PreparedStatement statementUpdate = conn.prepareStatement(sqlUpdate)) {
                        statementUpdate.setString(1, event.getEventId().getHostId());
                        statementUpdate.setString(2, event.getProductId());
                        statementUpdate.setString(3, event.getProductVersion());
                        statementUpdate.executeUpdate();
                    }
                }
                conn.commit();
                result = Success.of(event.getProductId());
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
    public Result<String> deleteProduct(ProductDeletedEvent event) {
        final String sql = "DELETE FROM product_version_t WHERE host_id = ? " +
                "AND product_id = ? AND product_version = ?";
        Result<String> result;
        try (Connection conn = ds.getConnection()) {
            conn.setAutoCommit(false);
            try (PreparedStatement statement = conn.prepareStatement(sql)) {
                statement.setString(1, event.getEventId().getHostId());
                statement.setString(2, event.getProductId());
                statement.setString(3, event.getProductVersion());


                int count = statement.executeUpdate();
                if (count == 0) {
                    throw new SQLException("failed to delete the product with id " + event.getProductId());
                }
                conn.commit();
                result = Success.of(event.getProductId());
                insertNotification(event.getEventId(), event.getClass().getName(), AvroConverter.toJson(event, false), true, null);


            } catch (SQLException e) {
                logger.error("SQLException:", e);
                conn.rollback();
                insertNotification(event.getEventId(), event.getClass().getName(), AvroConverter.toJson(event, false), false, e.getMessage());
                result = Failure.of(new Status(SQL_EXCEPTION, e.getMessage()));
            }
            catch (Exception e) {
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
    public Result<String> getProduct(int offset, int limit, String hostId, String productId, String productVersion,
                                     String light4jVersion, Boolean breakCode, Boolean breakConfig, String releaseNote,
                                     String versionDesc, String releaseType, Boolean current, String versionStatus) {
        Result<String> result = null;
        StringBuilder sqlBuilder = new StringBuilder();
        sqlBuilder.append("SELECT COUNT(*) OVER () AS total,\n" +
                "host_id, product_id, product_version, light4j_version, break_code, break_config, release_note,\n" +
                "version_desc, release_type, current, version_status, update_user, update_ts\n" +
                "FROM product_version_t\n" +
                "WHERE 1=1\n");

        List<Object> parameters = new ArrayList<>();

        StringBuilder whereClause = new StringBuilder();
        addCondition(whereClause, parameters, "host_id", hostId);
        addCondition(whereClause, parameters, "product_id", productId);
        addCondition(whereClause, parameters, "product_version", productVersion);
        addCondition(whereClause, parameters, "light4j_version", light4jVersion);
        addCondition(whereClause, parameters, "break_code", breakCode);
        addCondition(whereClause, parameters, "break_config", breakConfig);
        addCondition(whereClause, parameters, "release_note", releaseNote);
        addCondition(whereClause, parameters, "version_desc", versionDesc);
        addCondition(whereClause, parameters, "release_type", releaseType);
        addCondition(whereClause, parameters, "current", current);
        addCondition(whereClause, parameters, "version_status", versionStatus);

        if (whereClause.length() > 0) {
            sqlBuilder.append("AND ").append(whereClause);
        }

        sqlBuilder.append(" ORDER BY product_id, product_version DESC\n" +
                "LIMIT ? OFFSET ?");

        parameters.add(limit);
        parameters.add(offset);

        String sql = sqlBuilder.toString();
        int total = 0;
        List<Map<String, Object>> products = new ArrayList<>();

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
                    map.put("updateTs", resultSet.getTimestamp("update_ts") != null ? resultSet.getTimestamp("update_ts").toString() : null);
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
    public Result<String> getProductIdLabel(String hostId) {
        Result<String> result = null;
        String sql = "SELECT DISTINCT product_id FROM product_version_t WHERE host_id = ?";
        List<Map<String, Object>> labels = new ArrayList<>();
        try (Connection connection = ds.getConnection();
             PreparedStatement preparedStatement = connection.prepareStatement(sql)) {
            preparedStatement.setString(1, hostId);
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
            preparedStatement.setString(1, hostId);
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
    public Result<String> createInstance(InstanceCreatedEvent event) {
        final String sql = "INSERT INTO instance_t(host_id, instance_id, instance_name, product_id, product_version, " +
                "service_id, api_id, api_version, environment, pipeline_id, service_desc, instance_desc, tag_id, update_user, update_ts) " +
                "VALUES (?, ?, ?, ?, ?,  ?, ?, ?, ?, ?,  ?, ?, ?, ?, ?)";
        Result<String> result;
        Timestamp timestamp = new Timestamp(event.getEventId().getTimestamp());
        String value = event.getValue();
        Map<String, Object> map = JsonMapper.string2Map(value);

        try (Connection conn = ds.getConnection()) {
            conn.setAutoCommit(false);
            try (PreparedStatement statement = conn.prepareStatement(sql)) {
                statement.setString(1, event.getEventId().getHostId());
                statement.setString(2, event.getInstanceId());
                statement.setString(3, event.getInstanceName());
                statement.setString(4, event.getProductId());
                statement.setString(5, event.getProductVersion());
                statement.setString(6, event.getServiceId());

                if (map.containsKey("apiId")) {
                    statement.setString(7, (String) map.get("apiId"));
                } else {
                    statement.setNull(7, Types.VARCHAR);
                }
                if (map.containsKey("apiVersion")) {
                    statement.setString(8, (String) map.get("apiVersion"));
                } else {
                    statement.setNull(8, Types.VARCHAR);
                }

                if (map.containsKey("environment")) {
                    statement.setString(9, (String) map.get("environment"));
                } else {
                    statement.setNull(9, Types.VARCHAR);
                }
                statement.setString(10, event.getPipelineId());
                if (map.containsKey("serviceDesc")) {
                    statement.setString(11, (String) map.get("serviceDesc"));
                } else {
                    statement.setNull(11, Types.VARCHAR);
                }
                if(map.containsKey("instanceDesc")) {
                    statement.setString(12, (String) map.get("instanceDesc"));
                } else {
                    statement.setNull(12, Types.VARCHAR);
                }
                if(map.containsKey("tagId")) {
                    statement.setString(13, (String) map.get("tagId"));
                } else {
                    statement.setNull(13, Types.VARCHAR);
                }
                statement.setString(14, event.getEventId().getId());
                statement.setTimestamp(15, timestamp);

                int count = statement.executeUpdate();
                if (count == 0) {
                    throw new SQLException("failed to insert the instance with id " + event.getInstanceId());
                }
                conn.commit();
                result = Success.of(event.getInstanceId());
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
    public Result<String> updateInstance(InstanceUpdatedEvent event) {
        final String sql = "UPDATE instance_t SET instance_name = ?, product_id = ?, product_version = ?, service_id = ?, " +
                "api_id = ?, api_version = ?, environment = ?, pipeline_id = ?, " +
                "service_desc = ?, instance_desc = ?, tag_id = ?, update_user = ?, update_ts = ? " +
                "WHERE host_id = ? and instance_id = ?";
        Result<String> result;
        Timestamp timestamp = new Timestamp(event.getEventId().getTimestamp());
        String value = event.getValue();
        Map<String, Object> map = JsonMapper.string2Map(value);

        try (Connection conn = ds.getConnection()) {
            conn.setAutoCommit(false);
            try (PreparedStatement statement = conn.prepareStatement(sql)) {
                statement.setString(1, event.getInstanceName());
                statement.setString(2, event.getProductId());
                statement.setString(3, event.getProductVersion());
                statement.setString(4, event.getServiceId());
                if (map.containsKey("apiId")) {
                    statement.setString(5, (String) map.get("apiId"));
                } else {
                    statement.setNull(5, Types.VARCHAR);
                }
                if (map.containsKey("apiVersion")) {
                    statement.setString(6, (String) map.get("apiVersion"));
                } else {
                    statement.setNull(6, Types.VARCHAR);
                }
                if (map.containsKey("environment")) {
                    statement.setString(7, (String) map.get("environment"));
                } else {
                    statement.setNull(7, Types.VARCHAR);
                }
                statement.setString(8, event.getPipelineId());
                if (map.containsKey("serviceDesc")) {
                    statement.setString(9, (String) map.get("serviceDesc"));
                } else {
                    statement.setNull(9, Types.VARCHAR);
                }
                if(map.containsKey("instanceDesc")) {
                    statement.setString(10, (String) map.get("instanceDesc"));
                } else {
                    statement.setNull(10, Types.VARCHAR);
                }
                if(map.containsKey("tagId")) {
                    statement.setString(11, (String) map.get("tagId"));
                } else {
                    statement.setNull(11, Types.VARCHAR);
                }
                statement.setString(12, event.getEventId().getId());
                statement.setTimestamp(13, timestamp);
                statement.setString(14, event.getEventId().getHostId());
                statement.setString(15, event.getInstanceId());

                int count = statement.executeUpdate();
                if (count == 0) {
                    throw new SQLException("failed to update the instance with id " + event.getInstanceId());
                }
                conn.commit();
                result = Success.of(event.getInstanceId());
                insertNotification(event.getEventId(), event.getClass().getName(), AvroConverter.toJson(event, false), true, null);


            } catch (SQLException e) {
                logger.error("SQLException:", e);
                conn.rollback();
                insertNotification(event.getEventId(), event.getClass().getName(), AvroConverter.toJson(event, false), false, e.getMessage());
                result = Failure.of(new Status(SQL_EXCEPTION, e.getMessage()));
            }  catch (Exception e) {
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
    public Result<String> deleteInstance(InstanceDeletedEvent event) {
        final String sql = "DELETE FROM instance_t WHERE host_id = ? AND instance_id = ?";
        Result<String> result;

        try (Connection conn = ds.getConnection()) {
            conn.setAutoCommit(false);
            try (PreparedStatement statement = conn.prepareStatement(sql)) {
                statement.setString(1, event.getEventId().getHostId());
                statement.setString(2, event.getInstanceId());

                int count = statement.executeUpdate();
                if (count == 0) {
                    throw new SQLException("failed to delete the instance with id " + event.getInstanceId());
                }
                conn.commit();
                result = Success.of(event.getInstanceId());
                insertNotification(event.getEventId(), event.getClass().getName(), AvroConverter.toJson(event, false), true, null);

            } catch (SQLException e) {
                logger.error("SQLException:", e);
                conn.rollback();
                insertNotification(event.getEventId(), event.getClass().getName(), AvroConverter.toJson(event, false), false, e.getMessage());
                result = Failure.of(new Status(SQL_EXCEPTION, e.getMessage()));
            }
            catch (Exception e) {
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
    public Result<String> getInstance(int offset, int limit, String hostId, String instanceId, String instanceName,
                                      String productId, String productVersion, String serviceId, String apiId, String apiVersion,
                                      String environment, String pipelineId, String serviceDesc, String instanceDesc, String tagId) {
        Result<String> result = null;
        StringBuilder sqlBuilder = new StringBuilder();
        sqlBuilder.append("SELECT COUNT(*) OVER () AS total,\n" +
                "host_id, instance_id, instance_name, product_id, product_version, service_id, api_id, api_version, " +
                "environment, pipeline_id, service_desc, instance_desc, tag_id, update_user, update_ts \n" +
                "FROM instance_t\n" +
                "WHERE 1=1\n");

        List<Object> parameters = new ArrayList<>();

        StringBuilder whereClause = new StringBuilder();

        addCondition(whereClause, parameters, "host_id", hostId);
        addCondition(whereClause, parameters, "instance_id", instanceId);
        addCondition(whereClause, parameters, "instance_name", instanceName);
        addCondition(whereClause, parameters, "product_id", productId);
        addCondition(whereClause, parameters, "product_version", productVersion);
        addCondition(whereClause, parameters, "service_id", serviceId);
        addCondition(whereClause, parameters, "api_id", apiId);
        addCondition(whereClause, parameters, "api_version", apiVersion);
        addCondition(whereClause, parameters, "environment", environment);
        addCondition(whereClause, parameters, "pipeline_id", pipelineId);
        addCondition(whereClause, parameters, "service_desc", serviceDesc);
        addCondition(whereClause, parameters, "instance_desc", instanceDesc);
        addCondition(whereClause, parameters, "tag_id", tagId);


        if (whereClause.length() > 0) {
            sqlBuilder.append("AND ").append(whereClause);
        }

        sqlBuilder.append(" ORDER BY instance_id\n" +
                "LIMIT ? OFFSET ?");

        parameters.add(limit);
        parameters.add(offset);

        String sql = sqlBuilder.toString();
        int total = 0;
        List<Map<String, Object>> instances = new ArrayList<>();

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
                    map.put("instanceId", resultSet.getString("instance_id"));
                    map.put("instanceName", resultSet.getString("instance_name"));
                    map.put("productId", resultSet.getString("product_id"));
                    map.put("productVersion", resultSet.getString("product_version"));
                    map.put("serviceId", resultSet.getString("service_id"));
                    map.put("apiId", resultSet.getString("api_id"));
                    map.put("apiVersion", resultSet.getString("api_version"));
                    map.put("environment", resultSet.getString("environment"));
                    map.put("pipelineId", resultSet.getString("pipeline_id"));
                    map.put("serviceDesc", resultSet.getString("service_desc"));
                    map.put("instanceDesc", resultSet.getString("instance_desc"));
                    map.put("tagId", resultSet.getString("tag_id"));
                    map.put("updateUser", resultSet.getString("update_user"));
                    // handling date properly
                    map.put("updateTs", resultSet.getTimestamp("update_ts") != null ? resultSet.getTimestamp("update_ts").toString() : null);
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
        String sql = "SELECT instance_id, instance_name FROM instance_t WHERE host_id = ?";
        List<Map<String, Object>> labels = new ArrayList<>();
        try (Connection connection = ds.getConnection();
             PreparedStatement preparedStatement = connection.prepareStatement(sql)) {
            preparedStatement.setString(1, hostId);
            try (ResultSet resultSet = preparedStatement.executeQuery()) {
                while (resultSet.next()) {
                    Map<String, Object> map = new HashMap<>();
                    map.put("id", resultSet.getString("instance_id"));
                    map.put("label", resultSet.getString("instance_name"));
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
    public Result<String> createPipeline(PipelineCreatedEvent event) {
        final String sql = "INSERT INTO pipeline_t(host_id, pipeline_id, platform_id, endpoint, request_schema, response_schema, update_user, update_ts) " +
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?)";
        Result<String> result;
        Timestamp timestamp = new Timestamp(event.getEventId().getTimestamp());

        try (Connection conn = ds.getConnection()) {
            conn.setAutoCommit(false);
            try (PreparedStatement statement = conn.prepareStatement(sql)) {
                statement.setString(1, event.getEventId().getHostId());
                statement.setString(2, event.getPipelineId());
                statement.setString(3, event.getPlatformId());
                statement.setString(4, event.getEndpoint());
                statement.setString(5, event.getRequestSchema());
                statement.setString(6, event.getResponseSchema());
                statement.setString(7, event.getEventId().getId());
                statement.setTimestamp(8, timestamp);

                int count = statement.executeUpdate();
                if (count == 0) {
                    throw new SQLException("failed to insert the pipeline with id " + event.getPipelineId());
                }
                conn.commit();
                result = Success.of(event.getPipelineId());
                insertNotification(event.getEventId(), event.getClass().getName(), AvroConverter.toJson(event, false), true, null);
            }   catch (SQLException e) {
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
    public Result<String> updatePipeline(PipelineUpdatedEvent event) {
        final String sql = "UPDATE pipeline_t SET platform_id = ?, endpoint = ?, request_schema = ?, response_schema = ?, update_user = ?, update_ts = ? " +
                "WHERE host_id = ? and pipeline_id = ?";
        Result<String> result;
        Timestamp timestamp = new Timestamp(event.getEventId().getTimestamp());

        try (Connection conn = ds.getConnection()) {
            conn.setAutoCommit(false);
            try (PreparedStatement statement = conn.prepareStatement(sql)) {
                statement.setString(1, event.getPlatformId());
                statement.setString(2, event.getEndpoint());
                statement.setString(3, event.getRequestSchema());
                statement.setString(4, event.getResponseSchema());
                statement.setString(5, event.getEventId().getId());
                statement.setTimestamp(6, timestamp);
                statement.setString(7, event.getEventId().getHostId());
                statement.setString(8, event.getPipelineId());


                int count = statement.executeUpdate();
                if (count == 0) {
                    throw new SQLException("failed to update the pipeline with id " + event.getPipelineId());
                }
                conn.commit();
                result = Success.of(event.getPipelineId());
                insertNotification(event.getEventId(), event.getClass().getName(), AvroConverter.toJson(event, false), true, null);

            }  catch (SQLException e) {
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
    public Result<String> deletePipeline(PipelineDeletedEvent event) {
        final String sql = "DELETE FROM pipeline_t WHERE host_id = ? AND pipeline_id = ?";
        Result<String> result;

        try (Connection conn = ds.getConnection()) {
            conn.setAutoCommit(false);
            try (PreparedStatement statement = conn.prepareStatement(sql)) {
                statement.setString(1, event.getEventId().getHostId());
                statement.setString(2, event.getPipelineId());

                int count = statement.executeUpdate();
                if (count == 0) {
                    throw new SQLException("failed to delete the pipeline with id " + event.getPipelineId());
                }
                conn.commit();
                result = Success.of(event.getPipelineId());
                insertNotification(event.getEventId(), event.getClass().getName(), AvroConverter.toJson(event, false), true, null);
            }   catch (SQLException e) {
                logger.error("SQLException:", e);
                conn.rollback();
                insertNotification(event.getEventId(), event.getClass().getName(), AvroConverter.toJson(event, false), false, e.getMessage());
                result = Failure.of(new Status(SQL_EXCEPTION, e.getMessage()));
            }  catch (Exception e) {
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
    public Result<String> getPipeline(int offset, int limit, String hostId, String pipelineId, String platformId, String endpoint,
                                      String requestSchema, String responseSchema) {
        Result<String> result = null;
        StringBuilder sqlBuilder = new StringBuilder();
        sqlBuilder.append("SELECT COUNT(*) OVER () AS total,\n" +
                "host_id, pipeline_id, platform_id, endpoint, request_schema, response_schema, update_user, update_ts\n" +
                "FROM pipeline_t\n" +
                "WHERE 1=1\n");

        List<Object> parameters = new ArrayList<>();

        StringBuilder whereClause = new StringBuilder();
        addCondition(whereClause, parameters, "host_id", hostId);
        addCondition(whereClause, parameters, "pipeline_id", pipelineId);
        addCondition(whereClause, parameters, "platform_id", platformId);
        addCondition(whereClause, parameters, "endpoint", endpoint);
        addCondition(whereClause, parameters, "request_schema", requestSchema);
        addCondition(whereClause, parameters, "response_schema", responseSchema);


        if (whereClause.length() > 0) {
            sqlBuilder.append("AND ").append(whereClause);
        }

        sqlBuilder.append(" ORDER BY pipeline_id\n" +
                "LIMIT ? OFFSET ?");

        parameters.add(limit);
        parameters.add(offset);

        String sql = sqlBuilder.toString();
        int total = 0;
        List<Map<String, Object>> pipelines = new ArrayList<>();

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
                    map.put("pipelineId", resultSet.getString("pipeline_id"));
                    map.put("platformId", resultSet.getString("platform_id"));
                    map.put("endpoint", resultSet.getString("endpoint"));
                    map.put("requestSchema", resultSet.getString("request_schema"));
                    map.put("responseSchema", resultSet.getString("response_schema"));
                    map.put("updateUser", resultSet.getString("update_user"));
                    // handling date properly
                    map.put("updateTs", resultSet.getTimestamp("update_ts") != null ? resultSet.getTimestamp("update_ts").toString() : null);
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
        String sql = "SELECT pipeline_id, pipeline_id FROM pipeline_t WHERE host_id = ?";
        List<Map<String, Object>> labels = new ArrayList<>();
        try (Connection connection = ds.getConnection();
             PreparedStatement preparedStatement = connection.prepareStatement(sql)) {
            preparedStatement.setString(1, hostId);
            try (ResultSet resultSet = preparedStatement.executeQuery()) {
                while (resultSet.next()) {
                    Map<String, Object> map = new HashMap<>();
                    String pipelineId = resultSet.getString("pipeline_id");
                    map.put("id", pipelineId);
                    map.put("label", pipelineId);
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
    public Result<String> createPlatform(PlatformCreatedEvent event) {
        final String sql = "INSERT INTO platform_t(host_id, platform_id, platform_name, platform_version, " +
                "client_type, client_url, credentials, proxy_url, proxy_port, environment, system_env, runtime_env, " +
                "zone, region, lob, update_user, update_ts) " +
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
        Result<String> result;
        Timestamp timestamp = new Timestamp(event.getEventId().getTimestamp());
        String value = event.getValue();
        Map<String, Object> map = JsonMapper.string2Map(value);
        try (Connection conn = ds.getConnection()) {
            conn.setAutoCommit(false);
            try (PreparedStatement statement = conn.prepareStatement(sql)) {
                statement.setString(1, event.getEventId().getHostId());
                statement.setString(2, event.getPlatformId());
                statement.setString(3, event.getPlatformName());
                statement.setString(4, event.getPlatformVersion());
                statement.setString(5, event.getClientType());
                statement.setString(6, event.getClientUrl());
                statement.setString(7, event.getCredentials());

                if (map.containsKey("proxyUrl")) {
                    statement.setString(8, (String) map.get("proxyUrl"));
                } else {
                    statement.setNull(8, Types.VARCHAR);
                }
                if (map.containsKey("proxyPort")) {
                    statement.setInt(9, (Integer)map.get("proxyPort"));
                } else {
                    statement.setNull(9, Types.INTEGER);
                }
                if (map.containsKey("environment")) {
                    statement.setString(10, (String) map.get("environment"));
                } else {
                    statement.setNull(10, Types.VARCHAR);
                }
                if(map.containsKey("systemEnv")) {
                    statement.setString(11, (String) map.get("systemEnv"));
                } else {
                    statement.setNull(11, Types.VARCHAR);
                }
                if(map.containsKey("runtimeEnv")) {
                    statement.setString(12, (String) map.get("runtimeEnv"));
                } else {
                    statement.setNull(12, Types.VARCHAR);
                }
                if(map.containsKey("zone")) {
                    statement.setString(13, (String) map.get("zone"));
                } else {
                    statement.setNull(13, Types.VARCHAR);
                }
                if(map.containsKey("region")) {
                    statement.setString(14, (String) map.get("region"));
                } else {
                    statement.setNull(14, Types.VARCHAR);
                }
                if(map.containsKey("lob")) {
                    statement.setString(15, (String) map.get("lob"));
                } else {
                    statement.setNull(15, Types.VARCHAR);
                }
                statement.setString(16, event.getEventId().getId());
                statement.setTimestamp(17, timestamp);


                int count = statement.executeUpdate();
                if (count == 0) {
                    throw new SQLException("failed to insert the platform with id " + event.getPlatformId());
                }
                conn.commit();
                result =  Success.of(event.getPlatformId());
                insertNotification(event.getEventId(), event.getClass().getName(), AvroConverter.toJson(event, false), true, null);

            } catch (SQLException e) {
                logger.error("SQLException:", e);
                conn.rollback();
                insertNotification(event.getEventId(), event.getClass().getName(), AvroConverter.toJson(event, false), false, e.getMessage());
                result = Failure.of(new Status(SQL_EXCEPTION, e.getMessage()));
            }  catch (Exception e) {
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
    public Result<String> updatePlatform(PlatformUpdatedEvent event) {
        final String sql = "UPDATE platform_t SET platform_name = ?, platform_version = ?, " +
                "client_type = ?, client_url = ?, credentials = ?, proxy_url = ?, proxy_port = ?, " +
                "environment = ?, system_env = ?, runtime_env = ?, zone = ?, region = ?, lob = ?, " +
                "update_user = ?, update_ts = ? " +
                "WHERE host_id = ? and platform_id = ?";
        Result<String> result;
        Timestamp timestamp = new Timestamp(event.getEventId().getTimestamp());
        String value = event.getValue();
        Map<String, Object> map = JsonMapper.string2Map(value);
        try (Connection conn = ds.getConnection()) {
            conn.setAutoCommit(false);
            try (PreparedStatement statement = conn.prepareStatement(sql)) {
                statement.setString(1, event.getPlatformName());
                statement.setString(2, event.getPlatformVersion());
                statement.setString(3, event.getClientType());
                statement.setString(4, event.getClientUrl());
                statement.setString(5, event.getCredentials());
                if (map.containsKey("proxyUrl")) {
                    statement.setString(6, (String) map.get("proxyUrl"));
                } else {
                    statement.setNull(6, Types.VARCHAR);
                }
                if (map.containsKey("proxyPort")) {
                    statement.setInt(7, (Integer) map.get("proxyPort"));
                } else {
                    statement.setNull(7, Types.INTEGER);
                }
                if (map.containsKey("environment")) {
                    statement.setString(8, (String) map.get("environment"));
                } else {
                    statement.setNull(8, Types.VARCHAR);
                }
                if(map.containsKey("systemEnv")) {
                    statement.setString(9, (String) map.get("systemEnv"));
                } else {
                    statement.setNull(9, Types.VARCHAR);
                }
                if(map.containsKey("runtimeEnv")) {
                    statement.setString(10, (String) map.get("runtimeEnv"));
                } else {
                    statement.setNull(10, Types.VARCHAR);
                }
                if(map.containsKey("zone")) {
                    statement.setString(11, (String) map.get("zone"));
                } else {
                    statement.setNull(11, Types.VARCHAR);
                }
                if(map.containsKey("region")) {
                    statement.setString(12, (String) map.get("region"));
                } else {
                    statement.setNull(12, Types.VARCHAR);
                }
                if(map.containsKey("lob")) {
                    statement.setString(13, (String) map.get("lob"));
                } else {
                    statement.setNull(13, Types.VARCHAR);
                }
                statement.setString(14, event.getEventId().getId());
                statement.setTimestamp(15, timestamp);
                statement.setString(16, event.getEventId().getHostId());
                statement.setString(17, event.getPlatformId());

                int count = statement.executeUpdate();
                if (count == 0) {
                    throw new SQLException("failed to update the platform with id " + event.getPlatformId());
                }
                conn.commit();
                result = Success.of(event.getPlatformId());
                insertNotification(event.getEventId(), event.getClass().getName(), AvroConverter.toJson(event, false), true, null);

            }  catch (SQLException e) {
                logger.error("SQLException:", e);
                conn.rollback();
                insertNotification(event.getEventId(), event.getClass().getName(), AvroConverter.toJson(event, false), false, e.getMessage());
                result = Failure.of(new Status(SQL_EXCEPTION, e.getMessage()));
            }   catch (Exception e) {
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
    public Result<String> deletePlatform(PlatformDeletedEvent event) {
        final String sql = "DELETE FROM platform_t WHERE host_id = ? AND platform_id = ?";
        Result<String> result;

        try (Connection conn = ds.getConnection()) {
            conn.setAutoCommit(false);
            try (PreparedStatement statement = conn.prepareStatement(sql)) {
                statement.setString(1, event.getEventId().getHostId());
                statement.setString(2, event.getPlatformId());

                int count = statement.executeUpdate();
                if (count == 0) {
                    throw new SQLException("failed to delete the platform with id " + event.getPlatformId());
                }
                conn.commit();
                result =  Success.of(event.getPlatformId());
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
    public Result<String> getPlatform(int offset, int limit, String hostId, String platformId, String platformName, String platformVersion,
                                      String clientType, String clientUrl, String credentials, String proxyUrl, Integer proxyPort,
                                      String environment, String systemEnv, String runtimeEnv, String zone, String region, String lob) {
        Result<String> result = null;
        StringBuilder sqlBuilder = new StringBuilder();
        sqlBuilder.append("SELECT COUNT(*) OVER () AS total,\n" +
                "host_id, platform_id, platform_name, platform_version, client_type, client_url, " +
                "credentials, proxy_url, " +
                "proxy_port, environment, system_env, runtime_env, zone, region, lob, update_user, update_ts \n" +
                "FROM platform_t\n" +
                "WHERE 1=1\n");

        List<Object> parameters = new ArrayList<>();


        StringBuilder whereClause = new StringBuilder();
        addCondition(whereClause, parameters, "host_id", hostId);
        addCondition(whereClause, parameters, "platform_id", platformId);
        addCondition(whereClause, parameters, "platform_name", platformName);
        addCondition(whereClause, parameters, "platform_version", platformVersion);
        addCondition(whereClause, parameters, "client_type", clientType);
        addCondition(whereClause, parameters, "client_url", clientUrl);
        addCondition(whereClause, parameters, "credentials", credentials);
        addCondition(whereClause, parameters, "proxy_url", proxyUrl);
        addCondition(whereClause, parameters, "proxy_port", proxyPort);
        addCondition(whereClause, parameters, "environment", environment);
        addCondition(whereClause, parameters, "system_env", systemEnv);
        addCondition(whereClause, parameters, "runtime_env", runtimeEnv);
        addCondition(whereClause, parameters, "zone", zone);
        addCondition(whereClause, parameters, "region", region);
        addCondition(whereClause, parameters, "lob", lob);


        if (whereClause.length() > 0) {
            sqlBuilder.append("AND ").append(whereClause);
        }

        sqlBuilder.append(" ORDER BY platform_id\n" +
                "LIMIT ? OFFSET ?");


        parameters.add(limit);
        parameters.add(offset);

        String sql = sqlBuilder.toString();
        int total = 0;
        List<Map<String, Object>> platforms = new ArrayList<>();

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
                    map.put("platformId", resultSet.getString("platform_id"));
                    map.put("platformName", resultSet.getString("platform_name"));
                    map.put("platformVersion", resultSet.getString("platform_version"));
                    map.put("clientType", resultSet.getString("client_type"));
                    map.put("clientUrl", resultSet.getString("client_url"));
                    map.put("credentials", resultSet.getString("credentials"));
                    map.put("proxyUrl", resultSet.getString("proxy_url"));
                    map.put("proxyPort", resultSet.getInt("proxy_port"));
                    map.put("environment", resultSet.getString("environment"));
                    map.put("systemEnv", resultSet.getString("system_env"));
                    map.put("runtimeEnv", resultSet.getString("runtime_env"));
                    map.put("zone", resultSet.getString("zone"));
                    map.put("region", resultSet.getString("region"));
                    map.put("lob", resultSet.getString("lob"));
                    map.put("updateUser", resultSet.getString("update_user"));
                    // handling date properly
                    map.put("updateTs", resultSet.getTimestamp("update_ts") != null ? resultSet.getTimestamp("update_ts").toString() : null);

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
            preparedStatement.setString(1, hostId);
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

    @Override
    public Result<String> createDeployment(DeploymentCreatedEvent event) {
        final String sql = "INSERT INTO deployment_t(host_id, deployment_id, instance_id, " +
                "deployment_status, deployment_type, schedule_ts, update_user, update_ts) " +
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?)";
        Result<String> result;
        Timestamp timestamp = new Timestamp(event.getEventId().getTimestamp());
        try (Connection conn = ds.getConnection()) {
            conn.setAutoCommit(false);
            try (PreparedStatement statement = conn.prepareStatement(sql)) {
                statement.setString(1, event.getEventId().getHostId());
                statement.setString(2, event.getDeploymentId());
                statement.setString(3, event.getInstanceId());
                statement.setString(4, event.getDeploymentStatus());
                statement.setString(5, event.getDeploymentType());
                statement.setTimestamp(6, event.getScheduleTs() != 0 ? new java.sql.Timestamp(event.getScheduleTs()) : timestamp);
                statement.setString(7, event.getEventId().getId());
                statement.setTimestamp(8, timestamp);

                int count = statement.executeUpdate();
                if (count == 0) {
                    throw new SQLException("failed to insert the deployment with id " + event.getDeploymentId());
                }
                conn.commit();
                result = Success.of(event.getDeploymentId());
                insertNotification(event.getEventId(), event.getClass().getName(), AvroConverter.toJson(event, false), true, null);
            }  catch (SQLException e) {
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
        }  catch (SQLException e) {
            logger.error("SQLException:", e);
            result = Failure.of(new Status(SQL_EXCEPTION, e.getMessage()));
        }
        return result;
    }

    @Override
    public Result<String> updateDeployment(DeploymentUpdatedEvent event) {
        final String sql = "UPDATE deployment_t SET instance_id = ?, deployment_status = ?, deployment_type = ?, " +
                "schedule_ts = ?, update_user = ?, update_ts = ? " +
                "WHERE host_id = ? and deployment_id = ?";
        Result<String> result;
        Timestamp timestamp = new Timestamp(event.getEventId().getTimestamp());

        try (Connection conn = ds.getConnection()) {
            conn.setAutoCommit(false);
            try (PreparedStatement statement = conn.prepareStatement(sql)) {
                statement.setString(1, event.getInstanceId());
                statement.setString(2, event.getDeploymentStatus());
                statement.setString(3, event.getDeploymentType());
                statement.setTimestamp(4, event.getScheduleTs() != 0 ? new java.sql.Timestamp(event.getScheduleTs()) : timestamp);
                statement.setString(5, event.getEventId().getId());
                statement.setTimestamp(6, timestamp);
                statement.setString(7, event.getEventId().getHostId());
                statement.setString(8, event.getDeploymentId());
                int count = statement.executeUpdate();
                if (count == 0) {
                    throw new SQLException("failed to update the deployment with id " + event.getDeploymentId());
                }
                conn.commit();
                result = Success.of(event.getDeploymentId());
                insertNotification(event.getEventId(), event.getClass().getName(), AvroConverter.toJson(event, false), true, null);
            }  catch (SQLException e) {
                logger.error("SQLException:", e);
                conn.rollback();
                insertNotification(event.getEventId(), event.getClass().getName(), AvroConverter.toJson(event, false), false, e.getMessage());
                result = Failure.of(new Status(SQL_EXCEPTION, e.getMessage()));
            }  catch (Exception e) {
                logger.error("Exception:", e);
                conn.rollback();
                insertNotification(event.getEventId(), event.getClass().getName(), AvroConverter.toJson(event, false), false, e.getMessage());
                result = Failure.of(new Status(GENERIC_EXCEPTION, e.getMessage()));
            }
        }   catch (SQLException e) {
            logger.error("SQLException:", e);
            result = Failure.of(new Status(SQL_EXCEPTION, e.getMessage()));
        }
        return result;
    }

    @Override
    public Result<String> deleteDeployment(DeploymentDeletedEvent event) {
        final String sql = "DELETE FROM deployment_t WHERE host_id = ? AND deployment_id = ?";
        Result<String> result;

        try (Connection conn = ds.getConnection()) {
            conn.setAutoCommit(false);
            try (PreparedStatement statement = conn.prepareStatement(sql)) {
                statement.setString(1, event.getEventId().getHostId());
                statement.setString(2, event.getDeploymentId());

                int count = statement.executeUpdate();
                if (count == 0) {
                    throw new SQLException("failed to delete the deployment with id " + event.getDeploymentId());
                }
                conn.commit();
                result = Success.of(event.getDeploymentId());
                insertNotification(event.getEventId(), event.getClass().getName(), AvroConverter.toJson(event, false), true, null);

            }   catch (SQLException e) {
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
        }  catch (SQLException e) {
            logger.error("SQLException:", e);
            result = Failure.of(new Status(SQL_EXCEPTION, e.getMessage()));
        }
        return result;
    }

    @Override
    public Result<String> getDeployment(int offset, int limit, String hostId, String deploymentId,
                                        String instanceId, String deploymentStatus,
                                        String deploymentType, String platformJobId) {
        Result<String> result = null;
        StringBuilder sqlBuilder = new StringBuilder();
        sqlBuilder.append("SELECT COUNT(*) OVER () AS total,\n" +
                "host_id, deployment_id, instance_id, deployment_status, deployment_type, " +
                "schedule_ts, platform_job_id, update_user, update_ts\n" +
                "FROM deployment_t\n" +
                "WHERE 1=1\n");

        List<Object> parameters = new ArrayList<>();

        StringBuilder whereClause = new StringBuilder();

        addCondition(whereClause, parameters, "host_id", hostId);
        addCondition(whereClause, parameters, "deployment_id", deploymentId);
        addCondition(whereClause, parameters, "instance_id", instanceId);
        addCondition(whereClause, parameters, "deployment_status", deploymentStatus);
        addCondition(whereClause, parameters, "deployment_type", deploymentType);
        addCondition(whereClause, parameters, "platform_job_id", platformJobId);

        if (whereClause.length() > 0) {
            sqlBuilder.append("AND ").append(whereClause);
        }

        sqlBuilder.append(" ORDER BY deployment_id\n" +
                "LIMIT ? OFFSET ?");

        parameters.add(limit);
        parameters.add(offset);

        String sql = sqlBuilder.toString();
        int total = 0;
        List<Map<String, Object>> deployments = new ArrayList<>();

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
                    map.put("deploymentId", resultSet.getString("deployment_id"));
                    map.put("instanceId", resultSet.getString("instance_id"));
                    map.put("deploymentStatus", resultSet.getString("deployment_status"));
                    map.put("deploymentType", resultSet.getString("deployment_type"));
                    // handling date properly
                    map.put("scheduleTs", resultSet.getTimestamp("schedule_ts") != null ? resultSet.getTimestamp("schedule_ts").toString() : null);
                    map.put("platformJobId", resultSet.getString("platform_job_id"));
                    map.put("updateUser", resultSet.getString("update_user"));
                    map.put("updateTs", resultSet.getTimestamp("update_ts") != null ? resultSet.getTimestamp("update_ts").toString() : null);

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
