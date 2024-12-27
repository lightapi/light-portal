package net.lightapi.portal.db;

import com.github.benmanes.caffeine.cache.Cache;
import com.networknt.config.JsonMapper;
import com.networknt.kafka.common.AvroConverter;
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

import static java.sql.Types.NULL;
import static com.networknt.db.provider.SqlDbStartupHook.cacheManager;
import static com.networknt.db.provider.SqlDbStartupHook.ds;

public class PortalDbProviderImpl implements PortalDbProvider {
    public static final Logger logger = LoggerFactory.getLogger(PortalDbProviderImpl.class);
    public static final String AUTH_CODE_CACHE = "auth_code";
    public static final String SQL_EXCEPTION = "ERR10017";
    public static final String GENERIC_EXCEPTION = "ERR10014";
    public static final String OBJECT_NOT_FOUND = "ERR11637";

    public static final String INSERT_NOTIFICATION = "INSERT INTO notification_t (user_id, nonce, event_json, process_time, " +
            "process_flag, error) VALUES (?, ?, ?, ?, ?, ?)";
    public static final String UPDATE_NONCE = "UPDATE user_t SET nonce = ? WHERE user_id = ?";

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
                        total =  resultSet.getInt("total");
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
                "    CASE WHEN u.user_type = 'E' THEN string_agg(DISTINCT p.position_name, ' ' ORDER BY p.position_name) ELSE NULL END AS positions,\n" +
                "    string_agg(DISTINCT r.role_name, ' ' ORDER BY r.role_name) AS roles,\n" +
                "    string_agg(DISTINCT g.group_name, ' ' ORDER BY g.group_name) AS groups,\n" +
                "     CASE\n" +
                "        WHEN COUNT(DISTINCT at.attribute_name || '^=^' || aut.attribute_value) > 0 THEN string_agg(DISTINCT at.attribute_name || '^=^' || aut.attribute_value, '~' ORDER BY at.attribute_name || '^=^' || aut.attribute_value)\n" +
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
            if(map.size() == 0)
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
            if(map.size() == 0)
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
            if(map.size() == 0)
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
            if(map.size() == 0)
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
            if(email == null)
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
     * insert notification into database within the same transaction as conn is passed in.
     * @param conn The connection to the database
     * @param userId The userId of the user
     * @param nonce The nonce of the notification
     * @param json The json string of the event
     * @param flag The flag of the notification
     * @param error The error message of the notification
     * @throws SQLException when there is an error in the database access
     */
    public void insertNotification(Connection conn, String userId, long nonce, String json, boolean flag, String error) throws SQLException {
        try (PreparedStatement statement = conn.prepareStatement(INSERT_NOTIFICATION)) {
            statement.setString(1, userId);
            statement.setLong(2, nonce);
            statement.setString(3, json);
            statement.setTimestamp(4, new Timestamp(System.currentTimeMillis()));
            statement.setBoolean(5, flag);
            if(error != null) {
                statement.setString(6, error);
            } else {
                statement.setNull(6, NULL);
            }
            statement.executeUpdate();
        }
    }

    /**
     * update nonce in user_t to reflect the latest event nonce.
     * @param conn The connection to the database
     * @param userId The userId of the user
     * @param nonce The nonce of the notification
     * @throws SQLException when there is an error in the database access
     * @return the number of rows updated
     */
    public int updateNonce(Connection conn, long nonce, String userId) throws SQLException {
        int count = 0;
        try (PreparedStatement statement = conn.prepareStatement(UPDATE_NONCE)) {
            statement.setLong(1, nonce);
            statement.setString(2, userId);
            count = statement.executeUpdate();
        }
        return count;
    }

    /**
     * check the email, user_id, is unique. if not, write an error notification. If yes, insert
     * the user into database and write a success notification.
     *
     * @param event event that is created by user service
     * @return result of email
     *
     */
    @Override
    public Result<String> createUser(UserCreatedEvent event) {
        final String queryIdEmail = "SELECT nonce FROM user_t WHERE user_id = ? OR email = ?";
        final String insertUser = "INSERT INTO user_t (user_id, email, password, language, first_name, " +
                "last_name, user_type, phone_number, gender, birthday, " +
                "country, province, city, address, post_code, " +
                "verified), token, locked " +
                "VALUES (?, ?, ?, ?, ?,   ?, ?, ?, ?, ?,   ?, ?, ?, ?, ?,  ?, ?, ?)";
        final String insertUserHost = "INSERT INTO user_host_t (user_id, host_id) VALUES (?, ?)";

        Result<String> result = null;
        Map<String, Object> map = JsonMapper.string2Map(event.getValue());
        Connection conn = null;
        try {
            conn = ds.getConnection();
            conn.setAutoCommit(false);
            try (PreparedStatement statement = conn.prepareStatement(queryIdEmail)) {
                statement.setString(1, event.getUserId());
                statement.setString(2, event.getEmail());
                try (ResultSet resultSet = statement.executeQuery()) {
                    if (resultSet.next()) {
                        // found duplicate record, write an error notification.
                        insertNotification(conn, event.getUserId(), event.getEventId().getNonce(), AvroConverter.toJson(event, false), false,  "userId or email already exists in database.");
                        return result;
                    }
                }
            }

            // no duplicate record, insert the user into database and write a success notification.
            try (PreparedStatement statement = conn.prepareStatement(insertUser)) {
                statement.setString(1, event.getUserId());
                statement.setString(2, event.getEmail());
                statement.setString(3, event.getPassword());
                statement.setString(4, event.getLanguage());
                if(map.get("first_name") != null)
                    statement.setString(5, (String)map.get("first_name"));
                else
                    statement.setNull(5, NULL);

                if(map.get("last_name") != null)
                    statement.setString(6, (String)map.get("last_name"));
                else
                    statement.setNull(6, NULL);

                if(map.get("user_type") != null)
                    statement.setString(7, (String)map.get("user_type"));
                else
                    statement.setNull(7, NULL);

                if(map.get("phone_number") != null)
                    statement.setString(8, (String)map.get("phone_number"));
                else
                    statement.setNull(8, NULL);

                if(map.get("gender") != null) {
                    statement.setString(9, (String)map.get("gender"));
                } else {
                    statement.setNull(9, NULL);
                }
                java.util.Date birthday = (java.util.Date)map.get("birthday");
                if(birthday != null) {
                    statement.setDate(10, new java.sql.Date(birthday.getTime()));
                } else {
                    statement.setNull(10, NULL);
                }
                Object countryObject = event.get("country");
                if(countryObject != null) {
                    statement.setString(11, (String)countryObject);
                } else {
                    statement.setNull(11, NULL);
                }
                Object provinceObject = event.get("province");
                if(provinceObject != null) {
                    statement.setString(12, (String)provinceObject);
                } else {
                    statement.setNull(12, NULL);
                }
                Object cityObject = event.get("city");
                if(cityObject != null) {
                    statement.setString(13, (String)cityObject);
                } else {
                    statement.setNull(13, NULL);
                }
                Object addressObject = map.get("address");
                if(addressObject != null) {
                    statement.setString(14, (String)addressObject);
                } else {
                    statement.setNull(14, NULL);
                }
                Object postCodeObject = map.get("post_code");
                if(postCodeObject != null) {
                    statement.setString(15, (String)postCodeObject);
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
            insertNotification(conn, event.getEmail(), event.getEventId().getNonce(), AvroConverter.toJson(event, false), true,  null);
            // as this is a brand-new user, there is no nonce to be updated. By default, the nonce is 0.
            conn.commit();
            result = Success.of(event.getUserId());
        } catch (SQLException e) {
            logger.error("SQLException:", e);
            try {
                if(conn != null) conn.rollback();
            } catch (SQLException ex) {
                throw new RuntimeException(ex);
            }
            result = Failure.of(new Status(SQL_EXCEPTION, e.getMessage()));
        } catch (Exception e) {
            logger.error("Exception:", e);
            try {
                if(conn != null) conn.rollback();
            } catch (SQLException ex) {
                throw new RuntimeException(ex);
            }
            result = Failure.of(new Status(GENERIC_EXCEPTION, e.getMessage()));
        } finally {
            try {
                if (conn != null) {
                    conn.close();
                }
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
        return result;
    }

    @Override
    public Result<Integer> queryNonceByUserId(String userId) {
        Result<Integer> result = null;
        String sql = "SELECT nonce FROM user_t WHERE user_id = ?";
        try (final Connection conn = ds.getConnection()) {
            Integer nonce = null;
            try (PreparedStatement statement = conn.prepareStatement(sql)) {
                statement.setString(1, userId);
                try (ResultSet resultSet = statement.executeQuery()) {
                    if (resultSet.next()) {
                        nonce = resultSet.getInt("nonce");
                    }
                }
            }
            if(nonce == null)
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
     *
     */
    @Override
    public Result<String> confirmUser(UserConfirmedEvent event) {
        final String queryTokenByEmail = "SELECT token FROM user_t WHERE email = ? AND token = ?";
        final String updateUserByEmail = "UPDATE user_t SET token = null, verified = true, nonce = ? WHERE email = ?";
        Result<String> result;
        Connection conn = null;
        try {
            conn = ds.getConnection();
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
                        insertNotification(conn, event.getEventId().getId(), event.getEventId().getNonce(), AvroConverter.toJson(event, false), true,null);
                    } else {
                        // record is not found with the email and token. write an error notification.
                        insertNotification(conn, event.getEventId().getId(), event.getEventId().getNonce(), AvroConverter.toJson(event, false), false,  "token " + event.getToken() + " is not matched for email " + event.getEventId().getId());
                    }
                }
            }
            conn.commit();
            result = Success.of(event.getEventId().getId());
        } catch (SQLException e) {
            logger.error("SQLException:", e);
            try {
                if(conn != null) conn.rollback();
            } catch (SQLException ex) {
                throw new RuntimeException(ex);
            }
            result = Failure.of(new Status(SQL_EXCEPTION, e.getMessage()));
        } catch (Exception e) {
            logger.error("Exception:", e);
            try {
                if(conn != null) conn.rollback();
            } catch (SQLException ex) {
                throw new RuntimeException(ex);
            }
            result = Failure.of(new Status(GENERIC_EXCEPTION, e.getMessage()));
        } finally {
            try {
                if (conn != null) {
                    conn.close();
                }
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
        return result;
    }

    /**
     * check the email, user_id is unique. if not, write an error notification. If yes, insert
     * the user into database and write a success notification.
     *
     * @param event event that is created by user service
     * @return result of email
     *
     */
    @Override
    public Result<String> createSocialUser(SocialUserCreatedEvent event) {
        final String queryIdEmail = "SELECT nonce FROM user_t WHERE user_id = ? OR email = ?";
        final String insertUser = "INSERT INTO user_t (host_id, user_id, first_name, last_name, email, roles, language, " +
                "verified, gender, birthday, country, province, city, post_code, address) " +
                "VALUES (?, ?, ?, ?, ?,   ?, ?, ?, ?, ?,   ?, ?, ?, ?, ?)";
        Result<String> result = null;
        Map<String, Object> map = JsonMapper.string2Map(event.getValue());
        Connection conn = null;
        try {
            conn = ds.getConnection();
            conn.setAutoCommit(false);
            try (PreparedStatement statement = conn.prepareStatement(queryIdEmail)) {
                statement.setString(1, event.getUserId());
                statement.setString(2, event.getEmail());
                try (ResultSet resultSet = statement.executeQuery()) {
                    if (resultSet.next()) {
                        // found duplicate record, write an error notification.
                        insertNotification(conn, event.getEmail(), event.getEventId().getNonce(), AvroConverter.toJson(event, false), false,  "userId or email already exists in database.");
                        return result;
                    }
                }
            }
            // no duplicate record, insert the user into database and write a success notification.
            try (PreparedStatement statement = conn.prepareStatement(insertUser)) {
                statement.setString(1, event.getHostId());
                statement.setString(2, event.getUserId());
                if(map.get("first_name") != null)
                    statement.setString(3, (String)map.get("first_name"));
                else
                    statement.setNull(3, NULL);
                if(map.get("last_name") != null)
                    statement.setString(4, (String)map.get("last_name"));
                else
                    statement.setNull(4, NULL);
                statement.setString(5, event.getEmail());
                statement.setString(6, event.getRoles());
                statement.setString(7, event.getLanguage());
                statement.setBoolean(8, event.getVerified());
                if(map.get("gender") != null) {
                    statement.setString(9, (String)map.get("gender"));
                } else {
                    statement.setNull(9, NULL);
                }
                java.util.Date birthday = (java.util.Date)map.get("birthday");
                if(birthday != null) {
                    statement.setDate(10, new java.sql.Date(birthday.getTime()));
                } else {
                    statement.setNull(10, NULL);
                }
                Object countryObject = map.get("country");
                if(countryObject != null) {
                    statement.setString(11, (String)countryObject);
                } else {
                    statement.setNull(11, NULL);
                }
                Object provinceObject = map.get("province");
                if(provinceObject != null) {
                    statement.setString(12, (String)provinceObject);
                } else {
                    statement.setNull(12, NULL);
                }
                Object cityObject = map.get("city");
                if(cityObject != null) {
                    statement.setString(13, (String)cityObject);
                } else {
                    statement.setNull(13, NULL);
                }
                Object postCodeObject = map.get("post_code");
                if(postCodeObject != null) {
                    statement.setString(14, (String)postCodeObject);
                } else {
                    statement.setNull(14, NULL);
                }
                Object addressObject = map.get("address");
                if(addressObject != null) {
                    statement.setString(15, (String)addressObject);
                } else {
                    statement.setNull(15, NULL);
                }
                statement.execute();
            }
            insertNotification(conn, event.getEmail(), event.getEventId().getNonce(), AvroConverter.toJson(event, false), true,  null);
            // as this is a brand-new user, there is no nonce to be updated. By default, the nonce is 0.
            conn.commit();
            result = Success.of(event.getUserId());
        } catch (SQLException e) {
            logger.error("SQLException:", e);
            try {
                if(conn != null) conn.rollback();
            } catch (SQLException ex) {
                throw new RuntimeException(ex);
            }
            result = Failure.of(new Status(SQL_EXCEPTION, e.getMessage()));
        } catch (Exception e) {
            logger.error("Exception:", e);
            try {
                if(conn != null) conn.rollback();
            } catch (SQLException ex) {
                throw new RuntimeException(ex);
            }
            result = Failure.of(new Status(GENERIC_EXCEPTION, e.getMessage()));
        } finally {
            try {
                if (conn != null) {
                    conn.close();
                }
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
        return result;
    }

    /**
     * update user if it exists in database.
     *
     * @param event event that is created by user service
     * @return result of email
     *
     */
    @Override
    public Result<String> updateUser(UserUpdatedEvent event) {
        final String queryUserIdByWallet = "SELECT user_id FROM user_t WHERE taiji_wallet = ?";
        final String updateUser = "UPDATE user_t SET host_id = ?, language = ?, taiji_wallet = ?, country = ?, province = ?, " +
                "city = ?, post_code = ?, address = ?, first_name = ?, last_name = ?, gender = ?, birthday = ?, nonce = ? " +
                "WHERE email = ?";
        Result<String> result = null;
        Map<String, Object> map = JsonMapper.string2Map(event.getValue());
        Connection conn = null;
        try {
            conn = ds.getConnection();
            conn.setAutoCommit(false);
            if(event.getTaijiWallet() != null) {
                try (PreparedStatement statement = conn.prepareStatement(queryUserIdByWallet)) {
                    statement.setString(1, event.getTaijiWallet());
                    try (ResultSet resultSet = statement.executeQuery()) {
                        if (resultSet.next()) {
                            String userId = resultSet.getString(1);
                            // found duplicate record, write an error notification.
                            insertNotification(conn, event.getEmail(), event.getEventId().getNonce(), AvroConverter.toJson(event, false), false,  "wallet already exists in database for userId ." + userId);
                            return result;
                        }
                    }
                }
            }
            // make sure that the user exists in database.

            // no duplicate record, insert the user into database and write a success notification.
            try (PreparedStatement statement = conn.prepareStatement(updateUser)) {
                statement.setString(1, event.getHostId());
                statement.setString(2, event.getLanguage());
                if(event.getTaijiWallet() != null) {
                    statement.setString(3, event.getTaijiWallet());
                } else {
                    statement.setNull(3, NULL);
                }
                if(event.get("country") != null) {
                    statement.setString(4, (String)event.get("country"));
                } else {
                    statement.setNull(4, NULL);
                }
                if(event.get("province") != null) {
                    statement.setString(5, (String)event.get("province"));
                } else {
                    statement.setNull(5, NULL);
                }
                if(event.get("city") != null) {
                    statement.setString(6, (String)event.get("city"));
                } else {
                    statement.setNull(6, NULL);
                }
                if(map.get("post_code") != null) {
                    statement.setString(7, (String)map.get("post_code"));
                } else {
                    statement.setNull(7, NULL);
                }
                if(map.get("address") != null) {
                    statement.setString(8, (String)map.get("address"));
                } else {
                    statement.setNull(8, NULL);
                }
                if(map.get("first_name") != null)
                    statement.setString(9, (String)map.get("first_name"));
                else
                    statement.setNull(9, NULL);
                if(map.get("last_name") != null)
                    statement.setString(10, (String)map.get("last_name"));
                else
                    statement.setNull(10, NULL);
                if(map.get("gender") != null) {
                    statement.setString(11, (String)map.get("gender"));
                } else {
                    statement.setNull(11, NULL);
                }
                java.util.Date birthday = (java.util.Date)map.get("birthday");
                if(birthday != null) {
                    statement.setDate(12, new java.sql.Date(birthday.getTime()));
                } else {
                    statement.setNull(12, NULL);
                }
                statement.setString(13, event.getEmail());
                statement.setLong(14, event.getEventId().getNonce() + 1);
                int count = statement.executeUpdate();
                if(count == 0) {
                    // no record is updated, write an error notification.
                    insertNotification(conn, event.getEmail(), event.getEventId().getNonce(), AvroConverter.toJson(event, false), false,  "no record is updated by email" + event.getEmail());
                    return result;
                } else {
                    insertNotification(conn, event.getEmail(), event.getEventId().getNonce(), AvroConverter.toJson(event, false), true, null);
                }
            }
            // as this is a brand-new user, there is no nonce to be updated. By default, the nonce is 0.
            conn.commit();
            result = Success.of(event.getUserId());
        } catch (SQLException e) {
            logger.error("SQLException:", e);
            try {
                if(conn != null) conn.rollback();
            } catch (SQLException ex) {
                throw new RuntimeException(ex);
            }
            result = Failure.of(new Status(SQL_EXCEPTION, e.getMessage()));
        } catch (Exception e) {
            logger.error("Exception:", e);
            try {
                if(conn != null) conn.rollback();
            } catch (SQLException ex) {
                throw new RuntimeException(ex);
            }
            result = Failure.of(new Status(GENERIC_EXCEPTION, e.getMessage()));
        } finally {
            try {
                if (conn != null) {
                    conn.close();
                }
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
        return result;
    }

    /**
     * delete user from user_t table and all other tables related to this user.
     *
     * @param event event that is created by user service
     * @return result of email
     *
     */
    @Override
    public Result<String> deleteUser(UserDeletedEvent event) {
        final String deleteUserByEmail = "DELETE from user_t WHERE email = ?";
        // TODO delete all other tables related to this user.
        Result<String> result;
        Connection conn = null;
        try {
            conn = ds.getConnection();
            conn.setAutoCommit(false);
            try (PreparedStatement statement = conn.prepareStatement(deleteUserByEmail)) {
                statement.setString(1, event.getEmail());
                int count = statement.executeUpdate();
                if(count == 0) {
                    // no record is deleted, write an error notification.
                    insertNotification(conn, event.getEmail(), event.getEventId().getNonce(), AvroConverter.toJson(event, false), false,  "no record is deleted by email " + event.getEmail());
                } else {
                    // record is deleted, write a success notification.
                    insertNotification(conn, event.getEmail(), event.getEventId().getNonce(), AvroConverter.toJson(event, false), true,  null);
                }
            }
            conn.commit();
            result = Success.of(event.getEventId().getId());
        } catch (SQLException e) {
            logger.error("SQLException:", e);
            try {
                if(conn != null) conn.rollback();
            } catch (SQLException ex) {
                throw new RuntimeException(ex);
            }
            result = Failure.of(new Status(SQL_EXCEPTION, e.getMessage()));
        } catch (Exception e) {
            logger.error("Exception:", e);
            try {
                if(conn != null) conn.rollback();
            } catch (SQLException ex) {
                throw new RuntimeException(ex);
            }
            result = Failure.of(new Status(GENERIC_EXCEPTION, e.getMessage()));
        } finally {
            try {
                if (conn != null) {
                    conn.close();
                }
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
        return result;
    }

    /**
     * update user roles by email in user_t table
     *
     * @param event event that is created by user service
     * @return result of email
     *
     */
    @Override
    public Result<String> updateUserRoles(UserRolesUpdatedEvent event) {
        final String deleteUserByEmail = "UPDATE user_t SET roles = ?, nonce = ? WHERE email = ?";
        Result<String> result;
        Connection conn = null;
        try {
            conn = ds.getConnection();
            conn.setAutoCommit(false);
            try (PreparedStatement statement = conn.prepareStatement(deleteUserByEmail)) {
                statement.setString(1, event.getRoles());
                statement.setLong(2, event.getEventId().getNonce() + 1);
                statement.setString(3, event.getEmail());
                int count = statement.executeUpdate();
                if(count == 0) {
                    // no record is deleted, write an error notification.
                    insertNotification(conn, event.getEmail(), event.getEventId().getNonce(), AvroConverter.toJson(event, false), false,  "no roles is updated by email " + event.getEmail());
                } else {
                    // record is deleted, write a success notification.
                    insertNotification(conn, event.getEmail(), event.getEventId().getNonce(), AvroConverter.toJson(event, false), true,  null);
                }
            }
            conn.commit();
            result = Success.of(event.getEventId().getId());
        } catch (SQLException e) {
            logger.error("SQLException:", e);
            try {
                if(conn != null) conn.rollback();
            } catch (SQLException ex) {
                throw new RuntimeException(ex);
            }
            result = Failure.of(new Status(SQL_EXCEPTION, e.getMessage()));
        } catch (Exception e) {
            logger.error("Exception:", e);
            try {
                if(conn != null) conn.rollback();
            } catch (SQLException ex) {
                throw new RuntimeException(ex);
            }
            result = Failure.of(new Status(GENERIC_EXCEPTION, e.getMessage()));
        } finally {
            try {
                if (conn != null) {
                    conn.close();
                }
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
        return result;
    }

    /**
     * update user_t for the forget password token by email
     *
     * @param event event that is created by user service
     * @return result of email
     *
     */
    @Override
    public Result<String> forgetPassword(PasswordForgotEvent event) {
        final String deleteUserByEmail = "UPDATE user_t SET token = ?, nonce = ? WHERE email = ?";
        Result<String> result;
        Connection conn = null;
        try {
            conn = ds.getConnection();
            conn.setAutoCommit(false);
            try (PreparedStatement statement = conn.prepareStatement(deleteUserByEmail)) {
                statement.setString(1, event.getToken());
                statement.setLong(2, event.getEventId().getNonce() + 1);
                statement.setString(3, event.getEmail());
                int count = statement.executeUpdate();
                if(count == 0) {
                    // no record is deleted, write an error notification.
                    insertNotification(conn, event.getEmail(), event.getEventId().getNonce(), AvroConverter.toJson(event, false), false,  "no token is updated by email " + event.getEmail());
                } else {
                    // record is deleted, write a success notification.
                    insertNotification(conn, event.getEmail(), event.getEventId().getNonce(), AvroConverter.toJson(event, false), true,  null);
                }
            }
            conn.commit();
            result = Success.of(event.getEventId().getId());
        } catch (SQLException e) {
            logger.error("SQLException:", e);
            try {
                if(conn != null) conn.rollback();
            } catch (SQLException ex) {
                throw new RuntimeException(ex);
            }
            result = Failure.of(new Status(SQL_EXCEPTION, e.getMessage()));
        } catch (Exception e) {
            logger.error("Exception:", e);
            try {
                if(conn != null) conn.rollback();
            } catch (SQLException ex) {
                throw new RuntimeException(ex);
            }
            result = Failure.of(new Status(GENERIC_EXCEPTION, e.getMessage()));
        } finally {
            try {
                if (conn != null) {
                    conn.close();
                }
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
        return result;
    }

    /**
     * update user_t to reset the password by email
     *
     * @param event event that is created by user service
     * @return result of email
     *
     */
    @Override
    public Result<String> resetPassword(PasswordResetEvent event) {
        final String deleteUserByEmail = "UPDATE user_t SET token = ?, nonce = ? WHERE email = ?";
        Result<String> result;
        Connection conn = null;
        try {
            conn = ds.getConnection();
            conn.setAutoCommit(false);
            try (PreparedStatement statement = conn.prepareStatement(deleteUserByEmail)) {
                statement.setString(1, event.getToken());
                statement.setLong(2, event.getEventId().getNonce() + 1);
                statement.setString(3, event.getEmail());
                int count = statement.executeUpdate();
                if(count == 0) {
                    // no record is deleted, write an error notification.
                    insertNotification(conn, event.getEmail(), event.getEventId().getNonce(), AvroConverter.toJson(event, false), false,  "no token is updated by email " + event.getEmail());
                } else {
                    // record is deleted, write a success notification.
                    insertNotification(conn, event.getEmail(), event.getEventId().getNonce(), AvroConverter.toJson(event, false), true,  null);
                }
            }
            conn.commit();
            result = Success.of(event.getEventId().getId());
        } catch (SQLException e) {
            logger.error("SQLException:", e);
            try {
                if(conn != null) conn.rollback();
            } catch (SQLException ex) {
                throw new RuntimeException(ex);
            }
            result = Failure.of(new Status(SQL_EXCEPTION, e.getMessage()));
        } catch (Exception e) {
            logger.error("Exception:", e);
            try {
                if(conn != null) conn.rollback();
            } catch (SQLException ex) {
                throw new RuntimeException(ex);
            }
            result = Failure.of(new Status(GENERIC_EXCEPTION, e.getMessage()));
        } finally {
            try {
                if (conn != null) {
                    conn.close();
                }
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
        return result;
    }

    /**
     * update user_t to change the password by email
     *
     * @param event event that is created by user service
     * @return result of email
     *
     */
    @Override
    public Result<String> changePassword(PasswordChangedEvent event) {
        final String updatePasswordByEmail = "UPDATE user_t SET password = ?, nonce = ? WHERE email = ? AND password = ?";
        Result<String> result;
        Connection conn = null;
        try {
            conn = ds.getConnection();
            conn.setAutoCommit(false);
            try (PreparedStatement statement = conn.prepareStatement(updatePasswordByEmail)) {
                statement.setString(1, event.getPassword());
                statement.setLong(2, event.getEventId().getNonce() + 1);
                statement.setString(3, event.getEventId().getId());
                statement.setString(4, event.getOldPassword());
                int count = statement.executeUpdate();
                if(count == 0) {
                    // no record is updated, write an error notification.
                    insertNotification(conn, event.getEventId().getId(), event.getEventId().getNonce(), AvroConverter.toJson(event, false), false,  "no password is updated by email " + event.getEventId().getId());
                } else {
                    // record is deleted, write a success notification.
                    insertNotification(conn, event.getEventId().getId(), event.getEventId().getNonce(), AvroConverter.toJson(event, false), true,  null);
                }
            }
            conn.commit();
            result = Success.of(event.getEventId().getId());
        } catch (SQLException e) {
            logger.error("SQLException:", e);
            try {
                if(conn != null) conn.rollback();
            } catch (SQLException ex) {
                throw new RuntimeException(ex);
            }
            result = Failure.of(new Status(SQL_EXCEPTION, e.getMessage()));
        } catch (Exception e) {
            logger.error("Exception:", e);
            try {
                if(conn != null) conn.rollback();
            } catch (SQLException ex) {
                throw new RuntimeException(ex);
            }
            result = Failure.of(new Status(GENERIC_EXCEPTION, e.getMessage()));
        } finally {
            try {
                if (conn != null) {
                    conn.close();
                }
            } catch (SQLException e) {
                e.printStackTrace();
            }
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
     * @param event event that is created by user service
     * @return result of email
     */
    @Override
    public Result<String> sendPrivateMessage(PrivateMessageSentEvent event) {
        final String insertMessage = "INSERT INTO message_t (from_id, nonce, to_email, subject, content, send_time) VALUES (?, ?, ?, ?, ?, ?)";
        Result<String> result;
        Connection conn = null;
        try {
            conn = ds.getConnection();
            conn.setAutoCommit(false);
            int count = updateNonce(conn, event.getEventId().getNonce() + 1, event.getEventId().getId());
            if(count == 0) {
                // no record is updated, write an error notification.
                insertNotification(conn, event.getEventId().getId(), event.getEventId().getNonce(), AvroConverter.toJson(event, false), false,  "no nonce is updated by email " + event.getEventId().getId());
            } else {
                try (PreparedStatement statement = conn.prepareStatement(insertMessage)) {
                    statement.setString(1, event.getFromId());
                    statement.setLong(2, event.getEventId().getNonce());
                    statement.setString(3, event.getToEmail());
                    statement.setString(4, event.getSubject());
                    statement.setString(5, event.getContent());
                    statement.setTimestamp(6, new Timestamp(event.getTimestamp()));
                    statement.executeUpdate();
                }
                // record is deleted, write a success notification.
                insertNotification(conn, event.getEventId().getId(), event.getEventId().getNonce(), AvroConverter.toJson(event, false), true,  null);
            }
            conn.commit();
            result = Success.of(event.getEventId().getId());
        } catch (SQLException e) {
            logger.error("SQLException:", e);
            try {
                if(conn != null) conn.rollback();
            } catch (SQLException ex) {
                throw new RuntimeException(ex);
            }
            result = Failure.of(new Status(SQL_EXCEPTION, e.getMessage()));
        } catch (Exception e) {
            logger.error("Exception:", e);
            try {
                if(conn != null) conn.rollback();
            } catch (SQLException ex) {
                throw new RuntimeException(ex);
            }
            result = Failure.of(new Status(GENERIC_EXCEPTION, e.getMessage()));
        } finally {
            try {
                if (conn != null) {
                    conn.close();
                }
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
        return result;

    }

    @Override
    public Result<String> createClient(MarketClientCreatedEvent event) {
        final String insertUser = "INSERT INTO app_t (host_id, app_id, app_name, app_desc, " +
                "is_kafka_app, client_id, client_type, client_profile, client_secret, client_scope, custom_claim, " +
                "redirect_uri, authenticate_class, deref_client_id, operation_owner, delivery_owner, update_user, update_timestamp) " +
                "VALUES (?, ?, ?, ?, ?,   ?, ?, ?, ?, ?,   ?, ?, ?, ?, ?,   ?, ?, ?)";
        Result<String> result = null;
        Map<String, Object> map = JsonMapper.string2Map(event.getValue());
        Connection conn = null;
        try {
            conn = ds.getConnection();
            conn.setAutoCommit(false);
            // no duplicate record, insert the user into database and write a success notification.
            try (PreparedStatement statement = conn.prepareStatement(insertUser)) {
                statement.setString(1, event.getHostId());
                statement.setString(2, event.getAppId());
                statement.setString(3, (String)map.get("appName"));
                if(map.get("appDesc") != null)
                    statement.setString(4, (String)map.get("appDesc"));
                else
                    statement.setNull(4, NULL);
                if(map.get("isKafkaApp") != null)
                    statement.setBoolean(5, (Boolean)map.get("isKafkaApp"));
                else
                    statement.setNull(5, NULL);
                statement.setString(6, (String)map.get("clientId"));
                statement.setString(7, (String)map.get("clientType"));
                statement.setString(8, (String)map.get("clientProfile"));
                statement.setString(9, (String)map.get("clientSecret"));
                if(map.get("clientScope") != null) {
                    statement.setString(10, (String)map.get("clientScope"));
                } else {
                    statement.setNull(10, NULL);
                }
                if(map.get("customClaim") != null) {
                    statement.setString(11, (String)map.get("customClaim"));
                } else {
                    statement.setNull(11, NULL);
                }
                if(map.get("redirectUri") != null) {
                    statement.setString(12, (String)map.get("redirectUri"));
                } else {
                    statement.setNull(12, NULL);
                }
                if(map.get("authenticateClass") != null) {
                    statement.setString(13, (String)map.get("authenticateClass"));
                } else {
                    statement.setNull(13, NULL);
                }
                if(map.get("derefClientId") != null) {
                    statement.setString(14, (String)map.get("derefClientId"));
                } else {
                    statement.setNull(14, NULL);
                }
                if(map.get("operationOwner") != null) {
                    statement.setString(15, (String)map.get("operationOwner"));
                } else {
                    statement.setNull(15, NULL);
                }
                if(map.get("deliveryOwner") != null) {
                    statement.setString(16, (String)map.get("deliveryOwner"));
                } else {
                    statement.setNull(16, NULL);
                }
                statement.setString(17, event.getEventId().getId());
                statement.setTimestamp(18, new Timestamp(System.currentTimeMillis()));
                int count = statement.executeUpdate();
                if (count == 0) {
                    insertNotification(conn, event.getEventId().getId(), event.getEventId().getNonce(), AvroConverter.toJson(event, false), false, "failed to insert the app " + event.getAppId());
                } else {
                    insertNotification(conn, event.getEventId().getId(), event.getEventId().getNonce(), AvroConverter.toJson(event, false), true,  null);
                }
            }
            updateNonce(conn, event.getEventId().getNonce() + 1, event.getEventId().getId());
            // as this is a brand-new user, there is no nonce to be updated. By default, the nonce is 0.
            conn.commit();
            result = Success.of(event.getAppId());
        } catch (SQLException e) {
            logger.error("SQLException:", e);
            try {
                if(conn != null) conn.rollback();
            } catch (SQLException ex) {
                throw new RuntimeException(ex);
            }
            result = Failure.of(new Status(SQL_EXCEPTION, e.getMessage()));
        } catch (Exception e) {
            logger.error("Exception:", e);
            try {
                if(conn != null) conn.rollback();
            } catch (SQLException ex) {
                throw new RuntimeException(ex);
            }
            result = Failure.of(new Status(GENERIC_EXCEPTION, e.getMessage()));
        } finally {
            try {
                if (conn != null) {
                    conn.close();
                }
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
        return result;
    }
    @Override
    public Result<String> updateClient(MarketClientUpdatedEvent event) {
        final String updateApplication = "UPDATE app_t SET app_name = ?, app_desc = ?, is_kafka_app = ?, " +
                "client_type = ?, client_profile = ?, client_scope = ?, custom_claim = ?, redirect_uri = ?, authenticate_class = ?, " +
                "deref_client_id = ?, operation_owner = ?, delivery_owner = ?, update_user = ?, update_timestamp = ? " +
                "WHERE host_id = ? AND app_id = ?";

        Result<String> result = null;
        Map<String, Object> map = JsonMapper.string2Map(event.getValue());
        Connection conn = null;
        try {
            conn = ds.getConnection();
            conn.setAutoCommit(false);

            try (PreparedStatement statement = conn.prepareStatement(updateApplication)) {
                if(map.get("appName") != null) {
                    statement.setString(1, (String)map.get("appName"));
                } else {
                    statement.setNull(1, NULL);
                }
                if(map.get("appDesc") != null) {
                    statement.setString(2, (String)map.get("appDesc"));
                } else {
                    statement.setNull(2, NULL);
                }
                if(map.get("isKafkaApp") != null) {
                    statement.setBoolean(3, (Boolean)map.get("isKafkaApp"));
                } else {
                    statement.setNull(3, NULL);
                }
                if(map.get("clientType") != null) {
                    statement.setString(4, (String)map.get("clientType"));
                } else {
                    statement.setNull(4, NULL);
                }
                if(map.get("clientProfile") != null) {
                    statement.setString(5, (String)map.get("clientProfile"));
                } else {
                    statement.setNull(5, NULL);
                }
                if(map.get("clientScope") != null) {
                    statement.setString(6, (String)map.get("clientScope"));
                } else {
                    statement.setNull(6, NULL);
                }
                if(map.get("customClaim") != null)
                    statement.setString(7, (String)map.get("customClaim"));
                else
                    statement.setNull(7, NULL);
                if(map.get("redirectUri") != null)
                    statement.setString(8, (String)map.get("redirectUri"));
                else
                    statement.setNull(8, NULL);
                if(map.get("authenticateClass") != null) {
                    statement.setString(9, (String)map.get("authenticateClass"));
                } else {
                    statement.setNull(9, NULL);
                }
                if(map.get("derefClientId") != null) {
                    statement.setString(10, (String)map.get("derefClientId"));
                } else {
                    statement.setNull(10, NULL);
                }
                if(map.get("operationOwner") != null) {
                    statement.setString(11, (String)map.get("operationOwner"));
                } else {
                    statement.setNull(11, NULL);
                }
                if(map.get("deliveryOwner") != null) {
                    statement.setString(12, (String)map.get("deliveryOwner"));
                } else {
                    statement.setNull(12, NULL);
                }
                statement.setString(13, event.getEventId().getId());
                statement.setTimestamp(14, new Timestamp(System.currentTimeMillis()));
                statement.setString(15, event.getHostId());
                statement.setString(16, event.getAppId());

                int count = statement.executeUpdate();
                if(count == 0) {
                    // no record is updated, write an error notification.
                    insertNotification(conn, event.getEventId().getId(), event.getEventId().getNonce(), AvroConverter.toJson(event, false), false,  "no record is updated by app " + event.getAppId());
                    return result;
                } else {
                    insertNotification(conn, event.getEventId().getId(), event.getEventId().getNonce(), AvroConverter.toJson(event, false), true, null);
                }
                updateNonce(conn, event.getEventId().getNonce() + 1, event.getEventId().getId());
            }
            // as this is a brand-new user, there is no nonce to be updated. By default, the nonce is 0.
            conn.commit();
            result = Success.of(event.getAppId());
        } catch (SQLException e) {
            logger.error("SQLException:", e);
            try {
                if(conn != null) conn.rollback();
            } catch (SQLException ex) {
                throw new RuntimeException(ex);
            }
            result = Failure.of(new Status(SQL_EXCEPTION, e.getMessage()));
        } catch (Exception e) {
            logger.error("Exception:", e);
            try {
                if(conn != null) conn.rollback();
            } catch (SQLException ex) {
                throw new RuntimeException(ex);
            }
            result = Failure.of(new Status(GENERIC_EXCEPTION, e.getMessage()));
        } finally {
            try {
                if (conn != null) {
                    conn.close();
                }
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
        return result;

    }
    @Override
    public Result<String> deleteClient(MarketClientDeletedEvent event) {
        final String deleteApp = "DELETE from app_t WHERE host_id = ? AND app_id = ?";
        // TODO delete all other tables related to this user.
        Result<String> result;
        Connection conn = null;
        try {
            conn = ds.getConnection();
            conn.setAutoCommit(false);
            try (PreparedStatement statement = conn.prepareStatement(deleteApp)) {
                statement.setString(1, event.getHostId());
                statement.setString(2, event.getAppId());
                int count = statement.executeUpdate();
                if(count == 0) {
                    // no record is deleted, write an error notification.
                    insertNotification(conn, event.getEventId().getId(), event.getEventId().getNonce(), AvroConverter.toJson(event, false), false,  "no record is deleted for app " + event.getAppId());
                } else {
                    // record is deleted, write a success notification.
                    insertNotification(conn, event.getEventId().getId(), event.getEventId().getNonce(), AvroConverter.toJson(event, false), true,  null);
                }
            }
            updateNonce(conn, event.getEventId().getNonce() + 1, event.getEventId().getId());
            conn.commit();
            result = Success.of(event.getEventId().getId());
        } catch (SQLException e) {
            logger.error("SQLException:", e);
            try {
                if(conn != null) conn.rollback();
            } catch (SQLException ex) {
                throw new RuntimeException(ex);
            }
            result = Failure.of(new Status(SQL_EXCEPTION, e.getMessage()));
        } catch (Exception e) {
            logger.error("Exception:", e);
            try {
                if(conn != null) conn.rollback();
            } catch (SQLException ex) {
                throw new RuntimeException(ex);
            }
            result = Failure.of(new Status(GENERIC_EXCEPTION, e.getMessage()));
        } finally {
            try {
                if (conn != null) {
                    conn.close();
                }
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
        return result;

    }
    @Override
    public Result<Map<String, Object>> queryClientByClientId(String clientId) {
        Result<Map<String, Object>> result;
        String sql =
                "SELECT host_id, app_id, app_name, app_desc, is_kafka_app, client_id, " +
                        "client_type, client_profile, client_secret, client_scope, custom_claim, redirect_uri, authenticate_class, " +
                        "deref_client_id, operation_owner, delivery_owner, update_user, update_timestamp " +
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
                        map.put("updateTimestamp", resultSet.getTimestamp("update_timestamp"));
                    }
                }
            }
            if(map.size() == 0)
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
                        "deref_client_id, operation_owner, delivery_owner, update_user, update_timestamp " +
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
                        map.put("updateTimestamp", resultSet.getTimestamp("update_timestamp"));
                    }
                }
            }
            if(map.size() == 0)
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
        final String insertUser = "INSERT INTO api_t (host_id, api_id, service_id, api_name, api_type, " +
                "api_desc, operation_owner, delivery_owner, region, business_group, " +
                "lob, platform, capability, git_repo, api_tags, " +
                "api_status, update_user, update_timestamp) " +
                "VALUES (?, ?, ?, ?, ?,   ?, ?, ?, ?, ?,   ?, ?, ?, ?, ?,   ?, ?, ?)";
        Result<String> result = null;
        Map<String, Object> map = JsonMapper.string2Map(event.getValue());
        Connection conn = null;
        try {
            conn = ds.getConnection();
            conn.setAutoCommit(false);
            // no duplicate record, insert the user into database and write a success notification.
            try (PreparedStatement statement = conn.prepareStatement(insertUser)) {
                statement.setString(1, event.getHostId());
                statement.setString(2, event.getApiId());
                statement.setString(3, (String)map.get("serviceId"));
                statement.setString(4, (String)map.get("apiName"));
                statement.setString(5, (String)map.get("apiType"));
                if(map.get("apiDesc") != null)
                    statement.setString(6, (String)map.get("applicationDesc"));
                else
                    statement.setNull(6, NULL);

                if(map.get("operationOwner") != null)
                    statement.setString(7, (String)map.get("operationOwner"));
                else
                    statement.setNull(7, NULL);

                if (map.get("deliveryOwner") != null)
                    statement.setString(8, (String)map.get("deliveryOwner"));
                else
                    statement.setNull(8, NULL);

                if (map.get("region") != null)
                    statement.setInt(9, (Integer)map.get("region"));
                else
                    statement.setNull(9, NULL);

                if (map.get("businessGroup") != null)
                    statement.setInt(10, (Integer)map.get("businessGroup"));
                else
                    statement.setNull(10, NULL);

                if (map.get("lob") != null)
                    statement.setInt(11, (Integer)map.get("lob"));
                else
                    statement.setNull(11, NULL);

                if (map.get("platform") != null)
                    statement.setInt(12, (Integer)map.get("platform"));
                else
                    statement.setNull(12, NULL);

                if (map.get("capability") != null)
                    statement.setInt(13, (Integer)map.get("capability"));
                else
                    statement.setNull(13, NULL);

                if (map.get("gitRepo") != null)
                    statement.setString(14, (String)map.get("gitRepo"));
                else
                    statement.setNull(14, NULL);

                if (map.get("gitRepo") != null)
                    statement.setString(14, (String)map.get("gitRepo"));
                else
                    statement.setNull(14, NULL);

                if (map.get("apiTags") != null)
                    statement.setString(15, (String)map.get("apiTags"));
                else
                    statement.setNull(15, NULL);

                if (map.get("apiStatus") != null)
                    statement.setString(16, (String)map.get("apiStatus"));
                else
                    statement.setNull(16, NULL);

                statement.setString(17, event.getEventId().getId());
                statement.setTimestamp(18, new Timestamp(System.currentTimeMillis()));
                int count = statement.executeUpdate();
                if (count == 0) {
                    insertNotification(conn, event.getEventId().getId(), event.getEventId().getNonce(), AvroConverter.toJson(event, false), false, "failed to insert the api " + event.getApiId());
                } else {
                    insertNotification(conn, event.getEventId().getId(), event.getEventId().getNonce(), AvroConverter.toJson(event, false), true,  null);
                }
            }
            updateNonce(conn, event.getEventId().getNonce() + 1, event.getEventId().getId());
            // as this is a brand-new user, there is no nonce to be updated. By default, the nonce is 0.
            conn.commit();
            result = Success.of(event.getApiId());
        } catch (SQLException e) {
            logger.error("SQLException:", e);
            try {
                if(conn != null) conn.rollback();
            } catch (SQLException ex) {
                throw new RuntimeException(ex);
            }
            result = Failure.of(new Status(SQL_EXCEPTION, e.getMessage()));
        } catch (Exception e) {
            logger.error("Exception:", e);
            try {
                if(conn != null) conn.rollback();
            } catch (SQLException ex) {
                throw new RuntimeException(ex);
            }
            result = Failure.of(new Status(GENERIC_EXCEPTION, e.getMessage()));
        } finally {
            try {
                if (conn != null) {
                    conn.close();
                }
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
        return result;
    }

    @Override
    public Result<String> updateService(MarketServiceUpdatedEvent event) {
        final String updateApi = "UPDATE api_t SET service_id = ?, api_name = ?, api_type = ?, api_desc = ? " +
                "operation_owner = ?, delivery_owner = ?, region = ?, business_group = ?, lob = ?, platform = ?, " +
                "capability = ?, git_repo = ?, api_tags = ?, api_status = ?,  update_user = ?, update_timestamp = ? " +
                "WHERE host_id = ? AND api_id = ?";

        Result<String> result = null;
        Map<String, Object> map = JsonMapper.string2Map(event.getValue());
        Connection conn = null;
        try {
            conn = ds.getConnection();
            conn.setAutoCommit(false);

            try (PreparedStatement statement = conn.prepareStatement(updateApi)) {
                if(map.get("serviceId") != null) {
                    statement.setString(1, (String)map.get("serviceId"));
                } else {
                    statement.setNull(1, NULL);
                }

                if(map.get("apiName") != null) {
                    statement.setString(2, (String)map.get("apiName"));
                } else {
                    statement.setNull(2, NULL);
                }

                if(map.get("apiType") != null) {
                    statement.setString(3, (String)map.get("apiType"));
                } else {
                    statement.setNull(3, NULL);
                }

                if(map.get("apiDesc") != null) {
                    statement.setString(4, (String)map.get("apiDesc"));
                } else {
                    statement.setNull(4, NULL);
                }

                if(map.get("operationOwner") != null) {
                    statement.setString(5, (String)map.get("operationOwner"));
                } else {
                    statement.setNull(5, NULL);
                }

                if(map.get("deliveryOwner") != null) {
                    statement.setString(6, (String)map.get("deliveryOwner"));
                } else {
                    statement.setNull(6, NULL);
                }

                if(map.get("region") != null)
                    statement.setInt(7, (Integer)map.get("region"));
                else
                    statement.setNull(7, NULL);

                if(map.get("businessGroup") != null)
                    statement.setInt(8, (Integer)map.get("businessGroup"));
                else
                    statement.setNull(8, NULL);

                if(map.get("lob") != null) {
                    statement.setInt(9, (Integer)map.get("lob"));
                } else {
                    statement.setNull(9, NULL);
                }
                if(map.get("platform") != null) {
                    statement.setInt(10, (Integer)map.get("platform"));
                } else {
                    statement.setNull(10, NULL);
                }
                if(map.get("capability") != null) {
                    statement.setInt(11, (Integer)map.get("capability"));
                } else {
                    statement.setNull(11, NULL);
                }
                if(map.get("gitRepo") != null) {
                    statement.setString(12, (String)map.get("gitRepo"));
                } else {
                    statement.setNull(12, NULL);
                }
                if(map.get("apiTags") != null) {
                    statement.setString(13, (String)map.get("apiTags"));
                } else {
                    statement.setNull(13, NULL);
                }
                if(map.get("apiStatus") != null) {
                    statement.setString(14, (String)map.get("apiStatus"));
                } else {
                    statement.setNull(14, NULL);
                }
                statement.setString(15, event.getEventId().getId());
                statement.setTimestamp(16, new Timestamp(event.getTimestamp()));
                statement.setString(17, event.getHostId());
                statement.setString(18, event.getApiId());

                int count = statement.executeUpdate();
                if(count == 0) {
                    // no record is updated, write an error notification.
                    insertNotification(conn, event.getEventId().getId(), event.getEventId().getNonce(), AvroConverter.toJson(event, false), false,  "no record is updated by api " + event.getApiId());
                    return result;
                } else {
                    insertNotification(conn, event.getEventId().getId(), event.getEventId().getNonce(), AvroConverter.toJson(event, false), true, null);
                }
                updateNonce(conn, event.getEventId().getNonce() + 1, event.getEventId().getId());
            }
            // as this is a brand-new user, there is no nonce to be updated. By default, the nonce is 0.
            conn.commit();
            result = Success.of(event.getApiId());
        } catch (SQLException e) {
            logger.error("SQLException:", e);
            try {
                if(conn != null) conn.rollback();
            } catch (SQLException ex) {
                throw new RuntimeException(ex);
            }
            result = Failure.of(new Status(SQL_EXCEPTION, e.getMessage()));
        } catch (Exception e) {
            logger.error("Exception:", e);
            try {
                if(conn != null) conn.rollback();
            } catch (SQLException ex) {
                throw new RuntimeException(ex);
            }
            result = Failure.of(new Status(GENERIC_EXCEPTION, e.getMessage()));
        } finally {
            try {
                if (conn != null) {
                    conn.close();
                }
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
        return result;

    }
    @Override
    public Result<String> deleteService(MarketServiceDeletedEvent event) {
        final String deleteApplication = "DELETE from api_t WHERE host_id = ? AND api_id = ?";
        Result<String> result;
        Connection conn = null;
        try {
            conn = ds.getConnection();
            conn.setAutoCommit(false);
            try (PreparedStatement statement = conn.prepareStatement(deleteApplication)) {
                statement.setString(1, event.getHostId());
                statement.setString(2, event.getApiId());
                int count = statement.executeUpdate();
                if(count == 0) {
                    // no record is deleted, write an error notification.
                    insertNotification(conn, event.getEventId().getId(), event.getEventId().getNonce(), AvroConverter.toJson(event, false), false,  "no record is deleted for api " + event.getApiId());
                } else {
                    // record is deleted, write a success notification.
                    insertNotification(conn, event.getEventId().getId(), event.getEventId().getNonce(), AvroConverter.toJson(event, false), true,  null);
                }
            }
            updateNonce(conn, event.getEventId().getNonce() + 1, event.getEventId().getId());
            conn.commit();
            result = Success.of(event.getEventId().getId());
        } catch (SQLException e) {
            logger.error("SQLException:", e);
            try {
                if(conn != null) conn.rollback();
            } catch (SQLException ex) {
                throw new RuntimeException(ex);
            }
            result = Failure.of(new Status(SQL_EXCEPTION, e.getMessage()));
        } catch (Exception e) {
            logger.error("Exception:", e);
            try {
                if(conn != null) conn.rollback();
            } catch (SQLException ex) {
                throw new RuntimeException(ex);
            }
            result = Failure.of(new Status(GENERIC_EXCEPTION, e.getMessage()));
        } finally {
            try {
                if (conn != null) {
                    conn.close();
                }
            } catch (SQLException e) {
                logger.error("SQLException:", e);
            }
        }
        return result;

    }

    @Override
    public Result<String> queryService(int offset, int limit, String hostId, String apiId, String serviceId, String apiName, String apiType,
                                       String apiDesc, String operationOwner, String deliveryOwner, String region, String businessGroup,
                                       String lob, String platform, String capability, String gitRepo, String apiTags, String apiStatus) {
        Result<String> result = null;
        String sql = "SELECT COUNT(*) OVER () AS total,\n" +
                "host_id, api_id, service_id, api_name, api_type,\n" +
                "api_desc, operation_owner, delivery_owner, region, business_group,\n" +
                "lob, platform, capability, git_repo, api_tags, api_status\n" +
                "FROM api_t\n" +
                "WHERE host_id = ?\n" +
                "AND ? IS NULL OR ? = '*' OR api_id LIKE '%' || ? || '%'\n" +
                "AND ? IS NULL OR ? = '*' OR service_id LIKE '%' || ? || '%'\n" +
                "AND ? IS NULL OR ? = '*' OR api_name LIKE '%' || ? || '%'\n" +
                "AND ? IS NULL OR api_type = ?\n" +
                "AND ? IS NULL OR ? = '*' OR api_desc LIKE '%' || ? || '%'\n" +
                "AND ? IS NULL OR ? = '*' OR operation_owner LIKE '%' || ? || '%'\n" +
                "AND ? IS NULL OR ? = '*' OR delivery_owner LIKE '%' || ? || '%'\n" +
                "AND ? IS NULL OR region = ?\n" +
                "AND ? IS NULL OR business_group = ?\n" +
                "AND ? IS NULL OR lob = ?\n" +
                "AND ? IS NULL OR platform = ?\n" +
                "AND ? IS NULL OR capability = ?\n" +
                "AND ? IS NULL OR ? = '*' OR git_repo LIKE '%' || ? || '%'\n" +
                "AND ? IS NULL OR ? = '*' OR api_tags LIKE '%' || ? || '%'\n" +
                "AND ? IS NULL OR api_status = ?\n" +
                "ORDER BY api_id\n" +
                "LIMIT ? OFFSET ?";

        int total = 0;
        List<Map<String, Object>> services = new ArrayList<>();

        try (Connection connection = ds.getConnection();
             PreparedStatement preparedStatement = connection.prepareStatement(sql)) {
            preparedStatement.setString(1, hostId);
            preparedStatement.setString(2, apiId);
            preparedStatement.setString(3, apiId);
            preparedStatement.setString(4, apiId);
            preparedStatement.setString(5, serviceId);
            preparedStatement.setString(6, serviceId);
            preparedStatement.setString(7, serviceId);
            preparedStatement.setString(8, apiName);
            preparedStatement.setString(9, apiName);
            preparedStatement.setString(10, apiName);
            preparedStatement.setString(11, apiType);
            preparedStatement.setString(12, apiType);
            preparedStatement.setString(13, apiDesc);
            preparedStatement.setString(14, apiDesc);
            preparedStatement.setString(15, apiDesc);
            preparedStatement.setString(16, operationOwner);
            preparedStatement.setString(17, operationOwner);
            preparedStatement.setString(18, operationOwner);
            preparedStatement.setString(19, deliveryOwner);
            preparedStatement.setString(20, deliveryOwner);
            preparedStatement.setString(21, deliveryOwner);
            preparedStatement.setString(22, region);
            preparedStatement.setString(23, region);
            preparedStatement.setString(24, businessGroup);
            preparedStatement.setString(25, businessGroup);
            preparedStatement.setString(26, lob);
            preparedStatement.setString(27, lob);
            preparedStatement.setString(28, platform);
            preparedStatement.setString(29, platform);
            preparedStatement.setString(30, capability);
            preparedStatement.setString(31, capability);
            preparedStatement.setString(32, gitRepo);
            preparedStatement.setString(33, gitRepo);
            preparedStatement.setString(34, gitRepo);
            preparedStatement.setString(35, apiTags);
            preparedStatement.setString(36, apiTags);
            preparedStatement.setString(37, apiTags);
            preparedStatement.setString(38, apiStatus);
            preparedStatement.setString(39, apiStatus);
            preparedStatement.setInt(40, limit);
            preparedStatement.setInt(41, offset);

            boolean isFirstRow = true;
            try (ResultSet resultSet = preparedStatement.executeQuery()) {
                while (resultSet.next()) {
                    Map<String, Object> map = new HashMap<>();
                    // only get the total once as it is the same for all rows.
                    if (isFirstRow) {
                        total =  resultSet.getInt("total");
                        isFirstRow = false;
                    }

                    map.put("hostId", resultSet.getString("host_id"));
                    map.put("apiId", resultSet.getString("api_id"));
                    map.put("serviceId", resultSet.getString("service_id"));
                    map.put("apiName", resultSet.getString("api_name"));
                    map.put("apiType", resultSet.getString("api_type"));
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


    public Result<String> createMarketCode(MarketCodeCreatedEvent event) {
        // cache key is based on the hostId and authCode.
        String hostId = event.getHostId();
        String authCode = event.getAuthCode();
        String key = hostId + "|" + authCode;
        if(logger.isTraceEnabled()) logger.trace("insert into the cache auth_code with key {} value {}", key, event.getValue());
        if(logger.isTraceEnabled()) logger.trace("estimate the size of the cache auth_code before is " + cacheManager.getSize(AUTH_CODE_CACHE));
        cacheManager.put(AUTH_CODE_CACHE, key, event.getValue());
        if(logger.isTraceEnabled()) logger.trace("estimate the size of the cache auth_code after is " + cacheManager.getSize(AUTH_CODE_CACHE));
        return Success.of(event.getAuthCode());
    }

    public Result<String> deleteMarketCode(MarketCodeDeletedEvent event) {
        String hostId = event.getHostId();
        String authCode = event.getAuthCode();
        String key = hostId + "|" + authCode;
        if(logger.isTraceEnabled()) logger.trace("insert into the cache auth_code with key {}", key);
        if(logger.isTraceEnabled()) logger.trace("estimate the size of the cache auth_code before is " + cacheManager.getSize(AUTH_CODE_CACHE));
        cacheManager.delete(AUTH_CODE_CACHE, key);
        if(logger.isTraceEnabled()) logger.trace("estimate the size of the cache auth_code after is " + cacheManager.getSize(AUTH_CODE_CACHE));
        return Success.of(event.getAuthCode());
    }

    public Result<String> queryMarketCode(String hostId, String authCode) {
        // cache key is based on the hostId and authCode.
        String key = hostId + "|" + authCode;
        if(logger.isTraceEnabled()) logger.trace("key = {} and estimate the size of the cache auth_code is {}", key, cacheManager.getSize(AUTH_CODE_CACHE));
        String value = (String)cacheManager.get(AUTH_CODE_CACHE, key);
        if(logger.isTraceEnabled()) logger.trace("retrieve cache auth_code with key {} value {}", key, value);
        if(value != null) {
            return Success.of(value);
        } else {
            return Failure.of(new Status(OBJECT_NOT_FOUND, "auth code not found"));
        }
    }

    @Override
    public Result<String> createHost(HostCreatedEvent event) {
        final String insertHost = "INSERT INTO host_t (host_id, host, org_name, org_desc, org_owner, jwk, update_user, update_timestamp) " +
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?)";
        final String insertHostKey = "INSERT INTO host_key_t (host_id, kid, public_key, private_key, key_type, update_user, update_timestamp) " +
                "VALUES (?, ?, ?, ?, ?, ?, ?)";

        Result<String> result;
        Map<String, Object> map = JsonMapper.string2Map(event.getValue());
        Connection conn = null;
        try {
            // we create the key pair for the host here so that each environment will have different key pairs. This also avoids to put
            // the keys in the events which might be used to promote from env to env or from one host to another host.
            KeyPair longKeyPair = KeyUtil.generateKeyPair("RSA", 2048);
            String longKeyId = HashUtil.generateUUID();
            KeyPair currKeyPair = KeyUtil.generateKeyPair("RSA", 2048);
            String currKeyId = HashUtil.generateUUID();
            if(logger.isTraceEnabled()) logger.trace("longKeyId is " + longKeyId + " currKeyId is " + currKeyId);
            // prevKey and prevKeyId are null for the first time create the host. They are available during the key rotation.
            String jwk = KeyUtil.generateJwk(longKeyPair.getPublic(), longKeyId, currKeyPair.getPublic(), currKeyId, null, null);
            if(logger.isTraceEnabled()) logger.trace("jwk is " + jwk);
            conn = ds.getConnection();
            conn.setAutoCommit(false);
            // no duplicate record, insert the user into database and write a success notification.
            try (PreparedStatement statement = conn.prepareStatement(insertHost)) {
                statement.setString(1, event.getHostId());
                statement.setString(2, event.getHost());
                statement.setString(3, event.getName());
                statement.setString(4, event.getDesc());
                statement.setString(5, event.getOwner());
                statement.setString(6, jwk);
                statement.setString(7, event.getEventId().getId());
                statement.setTimestamp(8, new Timestamp(event.getTimestamp()));
                int count = statement.executeUpdate();
                if (count == 0) {
                    insertNotification(conn, event.getEventId().getId(), event.getEventId().getNonce(), AvroConverter.toJson(event, false), false, "failed to insert the host " + event.getHost());
                } else {
                    insertNotification(conn, event.getEventId().getId(), event.getEventId().getNonce(), AvroConverter.toJson(event, false), true,  null);
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
                statement.setTimestamp(7, new Timestamp(event.getTimestamp()));
                int count = statement.executeUpdate();
                if (count == 0) {
                    insertNotification(conn, event.getEventId().getId(), event.getEventId().getNonce(), AvroConverter.toJson(event, false), false, "failed to insert the host_key for host " + event.getHost() + " kid " + longKeyId);
                } else {
                    insertNotification(conn, event.getEventId().getId(), event.getEventId().getNonce(), AvroConverter.toJson(event, false), true,  null);
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
                statement.setTimestamp(7, new Timestamp(event.getTimestamp()));
                int count = statement.executeUpdate();
                if (count == 0) {
                    insertNotification(conn, event.getEventId().getId(), event.getEventId().getNonce(), AvroConverter.toJson(event, false), false, "failed to insert the host_key for host " + event.getHost() + " kid " + longKeyId);
                } else {
                    insertNotification(conn, event.getEventId().getId(), event.getEventId().getNonce(), AvroConverter.toJson(event, false), true,  null);
                }
            }
            updateNonce(conn, event.getEventId().getNonce() + 1, event.getEventId().getId());
            // as this is a brand-new user, there is no nonce to be updated. By default, the nonce is 0.
            conn.commit();
            result = Success.of(event.getHostId());
        } catch (SQLException e) {
            logger.error("SQLException:", e);
            try {
                if(conn != null) conn.rollback();
            } catch (SQLException ex) {
                throw new RuntimeException(ex);
            }
            result = Failure.of(new Status(SQL_EXCEPTION, e.getMessage()));
        } catch (Exception e) {
            logger.error("Exception:", e);
            try {
                if(conn != null) conn.rollback();
            } catch (SQLException ex) {
                throw new RuntimeException(ex);
            }
            result = Failure.of(new Status(GENERIC_EXCEPTION, e.getMessage()));
        } finally {
            try {
                if (conn != null) {
                    conn.close();
                }
            } catch (SQLException e) {
                logger.error("SQLException:", e);
            }
        }
        return result;
    }

    @Override
    public Result<String> updateHost(HostUpdatedEvent event) {
        final String updateHost = "UPDATE host_t SET org_name = ?, org_desc = ?, org_owner = ?, update_user = ? " +
                "update_timestamp = ? " +
                "WHERE host_id = ?";

        Result<String> result = null;
        Connection conn = null;
        try {
            conn = ds.getConnection();
            conn.setAutoCommit(false);

            try (PreparedStatement statement = conn.prepareStatement(updateHost)) {
                if(event.getName() != null) {
                    statement.setString(1, event.getName());
                } else {
                    statement.setNull(1, NULL);
                }
                if(event.getDesc() != null) {
                    statement.setString(2, event.getDesc());
                } else {
                    statement.setNull(2, NULL);
                }
                if(event.getOwner() != null) {
                    statement.setString(3, event.getOwner());
                } else {
                    statement.setNull(3, NULL);
                }
                statement.setString(4, event.getEventId().getId());
                statement.setTimestamp(5, new Timestamp(event.getTimestamp()));
                statement.setString(6, event.getHostId());

                int count = statement.executeUpdate();
                if(count == 0) {
                    // no record is updated, write an error notification.
                    insertNotification(conn, event.getEventId().getId(), event.getEventId().getNonce(), AvroConverter.toJson(event, false), false,  "no record is updated by host " + event.getHost());
                    return result;
                } else {
                    insertNotification(conn, event.getEventId().getId(), event.getEventId().getNonce(), AvroConverter.toJson(event, false), true, null);
                }
                updateNonce(conn, event.getEventId().getNonce() + 1, event.getEventId().getId());
            }
            // as this is a brand-new user, there is no nonce to be updated. By default, the nonce is 0.
            conn.commit();
            result = Success.of(event.getHostId());
        } catch (SQLException e) {
            logger.error("SQLException:", e);
            try {
                if(conn != null) conn.rollback();
            } catch (SQLException ex) {
                throw new RuntimeException(ex);
            }
            result = Failure.of(new Status(SQL_EXCEPTION, e.getMessage()));
        } catch (Exception e) {
            logger.error("Exception:", e);
            try {
                if(conn != null) conn.rollback();
            } catch (SQLException ex) {
                throw new RuntimeException(ex);
            }
            result = Failure.of(new Status(GENERIC_EXCEPTION, e.getMessage()));
        } finally {
            try {
                if (conn != null) {
                    conn.close();
                }
            } catch (SQLException e) {
                logger.error("SQLException:", e);
            }
        }
        return result;
    }

    @Override
    public Result<String> deleteHost(HostDeletedEvent event) {
        final String deleteHost = "DELETE from host_t WHERE host_id = ?";
        final String deleteHostKey = "DELETE from host_key_t WHERE host_id = ?";

        Result<String> result;
        Connection conn = null;
        try {
            conn = ds.getConnection();
            conn.setAutoCommit(false);
            try (PreparedStatement statement = conn.prepareStatement(deleteHostKey)) {
                statement.setString(1, event.getHostId());
                int count = statement.executeUpdate();
                if(count == 0) {
                    // no record is deleted, write an error notification.
                    insertNotification(conn, event.getEventId().getId(), event.getEventId().getNonce(), AvroConverter.toJson(event, false), false,  "no host_key record is deleted for host " + event.getHost());
                } else {
                    // record is deleted, write a success notification.
                    insertNotification(conn, event.getEventId().getId(), event.getEventId().getNonce(), AvroConverter.toJson(event, false), true,  null);
                }
            }
            try (PreparedStatement statement = conn.prepareStatement(deleteHost)) {
                statement.setString(1, event.getHostId());
                int count = statement.executeUpdate();
                if(count == 0) {
                    // no record is deleted, write an error notification.
                    insertNotification(conn, event.getEventId().getId(), event.getEventId().getNonce(), AvroConverter.toJson(event, false), false,  "no host record is deleted for host " + event.getHost());
                } else {
                    // record is deleted, write a success notification.
                    insertNotification(conn, event.getEventId().getId(), event.getEventId().getNonce(), AvroConverter.toJson(event, false), true,  null);
                }
            }
            updateNonce(conn, event.getEventId().getNonce() + 1, event.getEventId().getId());
            conn.commit();
            result = Success.of(event.getEventId().getId());
        } catch (SQLException e) {
            logger.error("SQLException:", e);
            try {
                if(conn != null) conn.rollback();
            } catch (SQLException ex) {
                throw new RuntimeException(ex);
            }
            result = Failure.of(new Status(SQL_EXCEPTION, e.getMessage()));
        } catch (Exception e) {
            logger.error("Exception:", e);
            try {
                if(conn != null) conn.rollback();
            } catch (SQLException ex) {
                throw new RuntimeException(ex);
            }
            result = Failure.of(new Status(GENERIC_EXCEPTION, e.getMessage()));
        } finally {
            try {
                if (conn != null) {
                    conn.close();
                }
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
        return result;
    }

    @Override
    public Result<Map<String, Object>> queryHostByHost(String host) {
        final String queryHostByHost = "SELECT * from host_t WHERE host = ?";
        Result<Map<String, Object>> result;
        try (final Connection conn = ds.getConnection()) {
            Map<String, Object> map = new HashMap<>();
            try (PreparedStatement statement = conn.prepareStatement(queryHostByHost)) {
                statement.setString(1, host);
                try (ResultSet resultSet = statement.executeQuery()) {
                    if (resultSet.next()) {
                        map.put("host_id", resultSet.getString("host_id"));
                        map.put("host", resultSet.getString("host"));
                        map.put("orgName", resultSet.getString("org_name"));
                        map.put("orgDesc", resultSet.getString("org_desc"));
                        map.put("orgOwner", resultSet.getString("org_owner"));
                        map.put("jwk", resultSet.getString("jwk"));
                        map.put("updateUser", resultSet.getString("update_user"));
                        map.put("updateTimestamp", resultSet.getTimestamp("update_timestamp"));
                    }
                }
            }
            if(map.size() == 0)
                result = Failure.of(new Status(OBJECT_NOT_FOUND, "host with host ", host));
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
                        map.put("updateTimestamp", resultSet.getTimestamp("update_timestamp"));
                    }
                }
            }
            if(map.size() == 0)
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
                        map.put("host_id", resultSet.getString("host_id"));
                        map.put("host", resultSet.getString("host"));
                        map.put("orgName", resultSet.getString("org_name"));
                        map.put("orgDesc", resultSet.getString("org_desc"));
                        map.put("orgOwner", resultSet.getString("org_owner"));
                        map.put("jwk", resultSet.getString("jwk"));
                        map.put("updateUser", resultSet.getString("update_user"));
                        map.put("updateTimestamp", resultSet.getTimestamp("update_timestamp"));
                    }
                }
            }
            if(map.size() == 0)
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
            if(list.isEmpty())
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
    public Result<List<Map<String, Object>>> getHost(int limit, int offset) {
        final String getHost = "SELECT * from host_t LIMIT ? OFFSET ?";
        Result<List<Map<String, Object>>> result;
        try (final Connection conn = ds.getConnection()) {
            List<Map<String, Object>> list = new ArrayList<>();
            try (PreparedStatement statement = conn.prepareStatement(getHost)) {
                statement.setInt(1, limit);
                statement.setInt(2, offset);
                try (ResultSet resultSet = statement.executeQuery()) {
                    while (resultSet.next()) {
                        Map<String, Object> map = new HashMap<>();
                        map.put("host_id", resultSet.getString("host_id"));
                        map.put("host", resultSet.getString("host"));
                        map.put("orgName", resultSet.getString("org_name"));
                        map.put("orgDesc", resultSet.getString("org_desc"));
                        map.put("orgOwner", resultSet.getString("org_owner"));
                        map.put("jwk", resultSet.getString("jwk"));
                        map.put("updateUser", resultSet.getString("update_user"));
                        map.put("updateTimestamp", resultSet.getTimestamp("update_timestamp"));
                        list.add(map);
                    }
                }
            }
            if(list.isEmpty())
                result = Failure.of(new Status(OBJECT_NOT_FOUND, "host", "limit and offset"));
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
    public Result<String> createConfig(ConfigCreatedEvent event) {
        final String insertHost = "INSERT INTO configuration_t (configuration_id, configuration_type, infrastructure_type_id, class_path, configuration_description, update_user, update_timestamp) " +
                "VALUES (?, ?, ?, ?, ?,   ?, ?)";
        Result<String> result;
        Connection conn = null;
        try {
            conn = ds.getConnection();
            conn.setAutoCommit(false);
            // no duplicate record, insert the user into database and write a success notification.
            try (PreparedStatement statement = conn.prepareStatement(insertHost)) {
                statement.setString(1, event.getConfigId());
                statement.setString(2, event.getConfigType());
                statement.setString(3, event.getInfraType());
                statement.setString(4, event.getClassPath());
                statement.setString(5, event.getConfigDesc());
                statement.setString(6, event.getEventId().getId());
                statement.setTimestamp(7, new Timestamp(event.getTimestamp()));
                int count = statement.executeUpdate();
                if (count == 0) {
                    insertNotification(conn, event.getEventId().getId(), event.getEventId().getNonce(), AvroConverter.toJson(event, false), false, "failed to insert the configuration with id " + event.getConfigId());
                } else {
                    insertNotification(conn, event.getEventId().getId(), event.getEventId().getNonce(), AvroConverter.toJson(event, false), true,  null);
                }
            }
            updateNonce(conn, event.getEventId().getNonce() + 1, event.getEventId().getId());
            // as this is a brand-new user, there is no nonce to be updated. By default, the nonce is 0.
            conn.commit();
            result = Success.of(event.getConfigId());
        } catch (SQLException e) {
            logger.error("SQLException:", e);
            try {
                if(conn != null) conn.rollback();
            } catch (SQLException ex) {
                throw new RuntimeException(ex);
            }
            result = Failure.of(new Status(SQL_EXCEPTION, e.getMessage()));
        } catch (Exception e) {
            logger.error("Exception:", e);
            try {
                if(conn != null) conn.rollback();
            } catch (SQLException ex) {
                throw new RuntimeException(ex);
            }
            result = Failure.of(new Status(GENERIC_EXCEPTION, e.getMessage()));
        } finally {
            try {
                if (conn != null) {
                    conn.close();
                }
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
        return result;
    }
    @Override
    public Result<String> updateConfig(ConfigUpdatedEvent event) {
        final String updateHost = "UPDATE configuration_t SET configuration_type = ?, infrastructure_type_id = ?, class_path = ?, configuration_description = ?, update_user = ? " +
                "update_timestamp = ? " +
                "WHERE configuration_id = ?";

        Result<String> result = null;
        Connection conn = null;
        try {
            conn = ds.getConnection();
            conn.setAutoCommit(false);

            try (PreparedStatement statement = conn.prepareStatement(updateHost)) {
                if(event.getConfigType() != null) {
                    statement.setString(1, event.getConfigType());
                } else {
                    statement.setNull(1, NULL);
                }
                if(event.getInfraType() != null) {
                    statement.setString(2, event.getInfraType());
                } else {
                    statement.setNull(2, NULL);
                }
                if(event.getClassPath() != null) {
                    statement.setString(3, event.getClassPath());
                } else {
                    statement.setNull(3, NULL);
                }
                if(event.getConfigDesc() != null) {
                    statement.setString(4, event.getConfigDesc());
                } else {
                    statement.setNull(4, NULL);
                }
                statement.setString(5, event.getEventId().getId());
                statement.setTimestamp(6, new Timestamp(event.getTimestamp()));
                statement.setString(7, event.getConfigId());

                int count = statement.executeUpdate();
                if(count == 0) {
                    // no record is updated, write an error notification.
                    insertNotification(conn, event.getEventId().getId(), event.getEventId().getNonce(), AvroConverter.toJson(event, false), false,  "no record is updated by configuration id " + event.getConfigId());
                    return result;
                } else {
                    insertNotification(conn, event.getEventId().getId(), event.getEventId().getNonce(), AvroConverter.toJson(event, false), true, null);
                }
                updateNonce(conn, event.getEventId().getNonce() + 1, event.getEventId().getId());
            }
            // as this is a brand-new user, there is no nonce to be updated. By default, the nonce is 0.
            conn.commit();
            result = Success.of(event.getConfigId());
        } catch (SQLException e) {
            logger.error("SQLException:", e);
            try {
                if(conn != null) conn.rollback();
            } catch (SQLException ex) {
                throw new RuntimeException(ex);
            }
            result = Failure.of(new Status(SQL_EXCEPTION, e.getMessage()));
        } catch (Exception e) {
            logger.error("Exception:", e);
            try {
                if(conn != null) conn.rollback();
            } catch (SQLException ex) {
                throw new RuntimeException(ex);
            }
            result = Failure.of(new Status(GENERIC_EXCEPTION, e.getMessage()));
        } finally {
            try {
                if (conn != null) {
                    conn.close();
                }
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
        return result;
    }
    @Override
    public Result<String> deleteConfig(ConfigDeletedEvent event) {
        final String deleteHost = "DELETE from configuration_t WHERE configuration_id = ?";
        Result<String> result;
        Connection conn = null;
        try {
            conn = ds.getConnection();
            conn.setAutoCommit(false);
            try (PreparedStatement statement = conn.prepareStatement(deleteHost)) {
                statement.setString(1, event.getConfigId());
                int count = statement.executeUpdate();
                if(count == 0) {
                    // no record is deleted, write an error notification.
                    insertNotification(conn, event.getEventId().getId(), event.getEventId().getNonce(), AvroConverter.toJson(event, false), false,  "no configuration record is deleted for id " + event.getConfigId());
                } else {
                    // record is deleted, write a success notification.
                    insertNotification(conn, event.getEventId().getId(), event.getEventId().getNonce(), AvroConverter.toJson(event, false), true,  null);
                }
            }
            updateNonce(conn, event.getEventId().getNonce() + 1, event.getEventId().getId());
            conn.commit();
            result = Success.of(event.getEventId().getId());
        } catch (SQLException e) {
            logger.error("SQLException:", e);
            try {
                if(conn != null) conn.rollback();
            } catch (SQLException ex) {
                throw new RuntimeException(ex);
            }
            result = Failure.of(new Status(SQL_EXCEPTION, e.getMessage()));
        } catch (Exception e) {
            logger.error("Exception:", e);
            try {
                if(conn != null) conn.rollback();
            } catch (SQLException ex) {
                throw new RuntimeException(ex);
            }
            result = Failure.of(new Status(GENERIC_EXCEPTION, e.getMessage()));
        } finally {
            try {
                if (conn != null) {
                    conn.close();
                }
            } catch (SQLException e) {
                e.printStackTrace();
            }
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
                        map.put("updateTimestamp", resultSet.getTimestamp("update_timestamp"));
                    }
                }
            }
            if(map.size() == 0)
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
                        map.put("updateTimestamp", resultSet.getTimestamp("update_timestamp"));
                    }
                }
            }
            if(map.size() == 0)
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
                        map.put("updateTimestamp", resultSet.getTimestamp("update_timestamp"));
                    }
                }
            }
            if(map.isEmpty())
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
                        map.put("updateTimestamp", resultSet.getTimestamp("update_timestamp"));
                    }
                }
            }
            if(map.isEmpty())
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
        final String insertRule = "INSERT INTO rule_t (rule_id, host_id, rule_type, rule_group, rule_visibility, " +
                "rule_description, rule_body, rule_owner, update_user, update_timestamp) " +
                "VALUES (?, ?, ?, ?, ?,   ?, ?, ?, ?, ?)";
        Result<String> result = null;
        Map<String, Object> map = JsonMapper.string2Map(event.getValue());
        Connection conn = null;
        try {
            conn = ds.getConnection();
            conn.setAutoCommit(false);
            // no duplicate record, insert the user into database and write a success notification.
            try (PreparedStatement statement = conn.prepareStatement(insertRule)) {
                statement.setString(1, event.getRuleId());
                statement.setString(2, event.getHostId());
                statement.setString(3, event.getRuleType());
                if(event.getGroupId() != null)
                    statement.setString(4, event.getGroupId());
                else
                    statement.setNull(4, NULL);
                statement.setString(5, event.getVisibility());
                if(event.getDesc() != null)
                    statement.setString(6, event.getDesc());
                else
                    statement.setNull(6, NULL);
                statement.setString(7, event.getValue());
                statement.setString(8, event.getOwner());
                statement.setString(15, event.getEventId().getId());
                statement.setTimestamp(16, new Timestamp(System.currentTimeMillis()));
                int count = statement.executeUpdate();
                if (count == 0) {
                    insertNotification(conn, event.getEventId().getId(), event.getEventId().getNonce(), AvroConverter.toJson(event, false), false, "failed to insert the rule " + event.getRuleId());
                } else {
                    insertNotification(conn, event.getEventId().getId(), event.getEventId().getNonce(), AvroConverter.toJson(event, false), true,  null);
                }
            }
            updateNonce(conn, event.getEventId().getNonce() + 1, event.getEventId().getId());
            // as this is a brand-new user, there is no nonce to be updated. By default, the nonce is 0.
            conn.commit();
            result = Success.of(event.getRuleId());
        } catch (SQLException e) {
            logger.error("SQLException:", e);
            try {
                if(conn != null) conn.rollback();
            } catch (SQLException ex) {
                throw new RuntimeException(ex);
            }
            result = Failure.of(new Status(SQL_EXCEPTION, e.getMessage()));
        } catch (Exception e) {
            logger.error("Exception:", e);
            try {
                if(conn != null) conn.rollback();
            } catch (SQLException ex) {
                throw new RuntimeException(ex);
            }
            result = Failure.of(new Status(GENERIC_EXCEPTION, e.getMessage()));
        } finally {
            try {
                if (conn != null) {
                    conn.close();
                }
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
        return result;
    }

    @Override
    public Result<String> updateRule(RuleUpdatedEvent event) {
        final String updateRule = "UPDATE rule_t SET host_id = ?, rule_type = ?, rule_group = ?, rule_visibility = ? " +
                "rule_description = ?, rule_body = ?, rule_owner = ?, update_user = ?, update_timestamp = ? " +
                "WHERE rule_id = ?";

        Result<String> result = null;
        Map<String, Object> map = JsonMapper.string2Map(event.getValue());
        Connection conn = null;
        try {
            conn = ds.getConnection();
            conn.setAutoCommit(false);

            try (PreparedStatement statement = conn.prepareStatement(updateRule)) {
                if(event.getHostId() != null) {
                    statement.setString(1, event.getHostId());
                } else {
                    statement.setNull(1, NULL);
                }
                if(event.getRuleType() != null) {
                    statement.setString(2, event.getRuleType());
                } else {
                    statement.setNull(2, NULL);
                }
                if(event.getGroupId() != null) {
                    statement.setString(3, event.getGroupId());
                } else {
                    statement.setNull(3, NULL);
                }
                if(event.getVisibility() != null) {
                    statement.setString(4, event.getVisibility());
                } else {
                    statement.setNull(4, NULL);
                }
                if(event.getDesc() != null) {
                    statement.setString(5, event.getDesc());
                } else {
                    statement.setNull(5, NULL);
                }
                if(event.getValue() != null) {
                    statement.setString(6, event.getValue());
                } else {
                    statement.setNull(6, NULL);
                }
                if(event.getOwner() != null)
                    statement.setString(7, event.getOwner());
                else
                    statement.setNull(7, NULL);
                statement.setString(8, event.getEventId().getId());
                statement.setTimestamp(9, new Timestamp(event.getTimestamp()));
                statement.setString(10, event.getRuleId());

                int count = statement.executeUpdate();
                if(count == 0) {
                    // no record is updated, write an error notification.
                    insertNotification(conn, event.getEventId().getId(), event.getEventId().getNonce(), AvroConverter.toJson(event, false), false,  "no record is updated by rule " + event.getRuleId());
                    return result;
                } else {
                    insertNotification(conn, event.getEventId().getId(), event.getEventId().getNonce(), AvroConverter.toJson(event, false), true, null);
                }
                updateNonce(conn, event.getEventId().getNonce() + 1, event.getEventId().getId());
            }
            // as this is a brand-new user, there is no nonce to be updated. By default, the nonce is 0.
            conn.commit();
            result = Success.of(event.getRuleId());
        } catch (SQLException e) {
            logger.error("SQLException:", e);
            try {
                if(conn != null) conn.rollback();
            } catch (SQLException ex) {
                throw new RuntimeException(ex);
            }
            result = Failure.of(new Status(SQL_EXCEPTION, e.getMessage()));
        } catch (Exception e) {
            logger.error("Exception:", e);
            try {
                if(conn != null) conn.rollback();
            } catch (SQLException ex) {
                throw new RuntimeException(ex);
            }
            result = Failure.of(new Status(GENERIC_EXCEPTION, e.getMessage()));
        } finally {
            try {
                if (conn != null) {
                    conn.close();
                }
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
        return result;

    }
    @Override
    public Result<String> deleteRule(RuleDeletedEvent event) {
        final String deleteRule = "DELETE from rule_t WHERE rule_id = ?";
        Result<String> result;
        Connection conn = null;
        try {
            conn = ds.getConnection();
            conn.setAutoCommit(false);
            try (PreparedStatement statement = conn.prepareStatement(deleteRule)) {
                statement.setString(1, event.getRuleId());
                int count = statement.executeUpdate();
                if(count == 0) {
                    // no record is deleted, write an error notification.
                    insertNotification(conn, event.getEventId().getId(), event.getEventId().getNonce(), AvroConverter.toJson(event, false), false,  "no record is deleted for rule " + event.getRuleId());
                } else {
                    // record is deleted, write a success notification.
                    insertNotification(conn, event.getEventId().getId(), event.getEventId().getNonce(), AvroConverter.toJson(event, false), true,  null);
                }
            }
            updateNonce(conn, event.getEventId().getNonce() + 1, event.getEventId().getId());
            conn.commit();
            result = Success.of(event.getEventId().getId());
        } catch (SQLException e) {
            logger.error("SQLException:", e);
            try {
                if(conn != null) conn.rollback();
            } catch (SQLException ex) {
                throw new RuntimeException(ex);
            }
            result = Failure.of(new Status(SQL_EXCEPTION, e.getMessage()));
        } catch (Exception e) {
            logger.error("Exception:", e);
            try {
                if(conn != null) conn.rollback();
            } catch (SQLException ex) {
                throw new RuntimeException(ex);
            }
            result = Failure.of(new Status(GENERIC_EXCEPTION, e.getMessage()));
        } finally {
            try {
                if (conn != null) {
                    conn.close();
                }
            } catch (SQLException e) {
                logger.error("SQLException:", e);
            }
        }
        return result;
    }


    @Override
    public Result<List<Map<String, Object>>> queryRuleByHostGroup(String hostId, String groupId) {
        Result<List<Map<String, Object>>> result;
        String sql = "SELECT rule_id, host_id, rule_type, rule_group, rule_visibility, rule_description, rule_body, rule_owner " +
                        "update_user, update_timestamp " +
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
                        map.put("updateTimestamp", resultSet.getTimestamp("update_timestamp"));
                        list.add(map);
                    }
                }
            }
            if(list.isEmpty())
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
    public Result<List<Map<String, Object>>> queryRuleByHost(String hostId) {
        Result<List<Map<String, Object>>> result;
        String sql = "SELECT rule_id, host_id, rule_type, rule_group, rule_visibility, rule_description, rule_body, rule_owner " +
                "update_user, update_timestamp " +
                "FROM rule_t WHERE host_id = ?";
        try (final Connection conn = ds.getConnection()) {
            List<Map<String, Object>> list = new ArrayList<>();
            try (PreparedStatement statement = conn.prepareStatement(sql)) {
                statement.setString(1, hostId);
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
                        map.put("updateTimestamp", resultSet.getTimestamp("update_timestamp"));
                        list.add(map);
                    }
                }
            }
            if(list.isEmpty())
                result = Failure.of(new Status(OBJECT_NOT_FOUND, "rule with host ", hostId));
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
    public Result<Map<String, Object>> queryRuleById(String ruleId) {
        Result<Map<String, Object>> result;
        String sql = "SELECT rule_id, host_id, rule_type, rule_group, rule_visibility, rule_description, rule_body, rule_owner " +
                "update_user, update_timestamp " +
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
                        map.put("updateTimestamp", resultSet.getTimestamp("update_timestamp"));
                    }
                }
            }
            if(map.isEmpty())
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
    public Result<List<Map<String, Object>>> queryRuleByHostType(String hostId, String ruleType) {
        Result<List<Map<String, Object>>> result;
        String sql = "SELECT rule_id, host_id, rule_type, rule_group, rule_visibility, rule_description, rule_body, rule_owner " +
                "update_user, update_timestamp " +
                "FROM rule_t WHERE host_id = ? AND rule_type = ?";
        try (final Connection conn = ds.getConnection()) {
            List<Map<String, Object>> list = new ArrayList<>();
            try (PreparedStatement statement = conn.prepareStatement(sql)) {
                statement.setString(1, hostId);
                statement.setString(2, ruleType);
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
                        map.put("updateTimestamp", resultSet.getTimestamp("update_timestamp"));
                        list.add(map);
                    }
                }
            }
            if(list.isEmpty())
                result = Failure.of(new Status(OBJECT_NOT_FOUND, "rule with rule type ", ruleType));
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
    public Result<String> createApiRule(ApiRuleCreatedEvent event) {
        final String insertApiRule = "INSERT INTO api_rule_t (api_id, rule_id, update_user, update_timestamp) " +
                "VALUES (?, ?, ?, ?)";
        Result<String> result = null;
        Connection conn = null;
        List<String> ruleIds = event.getRuleIds();
        String apiId = event.getApiId();
        Timestamp ts = new Timestamp(System.currentTimeMillis());

        try {
            conn = ds.getConnection();
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
                    if(i % 1000 == 0 || i == ruleIds.size()) {
                        statement.executeBatch();
                    }
                }
                if (ruleIds.isEmpty()) {
                    insertNotification(conn, event.getEventId().getId(), event.getEventId().getNonce(), AvroConverter.toJson(event, false), false, "failed to insert the Api rule " + event.getApiId());
                } else {
                    insertNotification(conn, event.getEventId().getId(), event.getEventId().getNonce(), AvroConverter.toJson(event, false), true,  null);
                }
            }
            updateNonce(conn, event.getEventId().getNonce() + 1, event.getEventId().getId());
            // as this is a brand-new user, there is no nonce to be updated. By default, the nonce is 0.
            conn.commit();
            result = Success.of(event.getApiId());
        } catch (SQLException e) {
            logger.error("SQLException:", e);
            try {
                if(conn != null) conn.rollback();
            } catch (SQLException ex) {
                throw new RuntimeException(ex);
            }
            result = Failure.of(new Status(SQL_EXCEPTION, e.getMessage()));
        } catch (Exception e) {
            logger.error("Exception:", e);
            try {
                if(conn != null) conn.rollback();
            } catch (SQLException ex) {
                throw new RuntimeException(ex);
            }
            result = Failure.of(new Status(GENERIC_EXCEPTION, e.getMessage()));
        } finally {
            try {
                if (conn != null) {
                    conn.close();
                }
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
        return result;
    }

    @Override
    public Result<String> deleteApiRule(ApiRuleDeletedEvent event) {
        final String deleteApiRule = "DELETE FROM api_rule_t WHERE api_id = ? AND rule_id = ?";
        Result<String> result = null;
        Connection conn = null;
        List<String> ruleIds = event.getRuleIds();
        String apiId = event.getApiId();

        try {
            conn = ds.getConnection();
            conn.setAutoCommit(false);
            // no duplicate record, insert the user into database and write a success notification.
            try (PreparedStatement statement = conn.prepareStatement(deleteApiRule)) {
                int i = 0;
                for (String ruleId : ruleIds) {
                    statement.setString(1, apiId);
                    statement.setString(2, ruleId);
                    statement.addBatch();
                    i++;
                    if(i % 1000 == 0 || i == ruleIds.size()) {
                        statement.executeBatch();
                    }
                }
                if (ruleIds.isEmpty()) {
                    insertNotification(conn, event.getEventId().getId(), event.getEventId().getNonce(), AvroConverter.toJson(event, false), false, "failed to delete the Api rule " + event.getApiId());
                } else {
                    insertNotification(conn, event.getEventId().getId(), event.getEventId().getNonce(), AvroConverter.toJson(event, false), true,  null);
                }
            }
            updateNonce(conn, event.getEventId().getNonce() + 1, event.getEventId().getId());
            // as this is a brand-new user, there is no nonce to be updated. By default, the nonce is 0.
            conn.commit();
            result = Success.of(event.getApiId());
        } catch (SQLException e) {
            logger.error("SQLException:", e);
            try {
                if(conn != null) conn.rollback();
            } catch (SQLException ex) {
                throw new RuntimeException(ex);
            }
            result = Failure.of(new Status(SQL_EXCEPTION, e.getMessage()));
        } catch (Exception e) {
            logger.error("Exception:", e);
            try {
                if(conn != null) conn.rollback();
            } catch (SQLException ex) {
                throw new RuntimeException(ex);
            }
            result = Failure.of(new Status(GENERIC_EXCEPTION, e.getMessage()));
        } finally {
            try {
                if (conn != null) {
                    conn.close();
                }
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
        return result;
    }

    @Override
    public Result<List<Map<String, Object>>> queryRuleByHostApiId(String hostId, String apiId) {
        Result<List<Map<String, Object>>> result;
        String sql = "SELECT rule_id, host_id, rule_type, rule_group, rule_visibility, rule_description, rule_body, rule_owner " +
                "update_user, update_timestamp " +
                "FROM rule_t r, api_rule_t a WHERE r.rule_id = a.rule_id AND r.host_id = ? AND a.api_id = ?";
        try (final Connection conn = ds.getConnection()) {
            List<Map<String, Object>> list = new ArrayList<>();
            try (PreparedStatement statement = conn.prepareStatement(sql)) {
                statement.setString(1, hostId);
                statement.setString(2, apiId);
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
                        map.put("updateTimestamp", resultSet.getTimestamp("update_timestamp"));
                        list.add(map);
                    }
                }
            }
            if(list.isEmpty())
                result = Failure.of(new Status(OBJECT_NOT_FOUND, "rule with rule apiId ", apiId));
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

}
