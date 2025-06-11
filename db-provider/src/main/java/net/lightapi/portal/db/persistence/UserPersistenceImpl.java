package net.lightapi.portal.db.persistence;

import com.networknt.config.JsonMapper;
import com.networknt.db.provider.SqlDbStartupHook;
import com.networknt.monad.Failure;
import com.networknt.monad.Result;
import com.networknt.monad.Success;
import com.networknt.status.Status;
import com.networknt.utility.Constants;
import io.cloudevents.core.v1.CloudEventV1;
import net.lightapi.portal.PortalConstants;
import net.lightapi.portal.db.PortalDbProvider;
import net.lightapi.portal.db.util.NotificationService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;
import java.time.OffsetDateTime;
import java.util.*;

import static com.networknt.db.provider.SqlDbStartupHook.ds;
import static java.sql.Types.NULL;
import static net.lightapi.portal.db.util.SqlUtil.addCondition;

public class UserPersistenceImpl implements UserPersistence {
    private static final Logger logger = LoggerFactory.getLogger(UserPersistenceImpl.class);
    private static final String SQL_EXCEPTION = PortalDbProvider.SQL_EXCEPTION;
    private static final String GENERIC_EXCEPTION = PortalDbProvider.GENERIC_EXCEPTION;
    private static final String OBJECT_NOT_FOUND = PortalDbProvider.OBJECT_NOT_FOUND;

    private final NotificationService notificationService;

    public UserPersistenceImpl(NotificationService notificationService) {
        this.notificationService = notificationService;
    }

    @Override
    public Result<String> createUser(Map<String, Object> event) {
        final String queryEmailEntityId = """
                SELECT u.user_id, u.email, COALESCE(c.customer_id, e.employee_id) AS entity_id
                FROM user_t u
                LEFT JOIN user_host_t uh ON u.user_id = uh.user_id
                LEFT JOIN customer_t c ON uh.host_id = c.host_id AND u.user_id = c.user_id
                LEFT JOIN employee_t e ON uh.host_id = e.host_id AND u.user_id = e.user_id
                WHERE
                    (u.email = ? OR COALESCE(c.customer_id, e.employee_id) = ?)
                    AND u.user_type IN ('C', 'E')
                """;
        final String insertUser = """
                INSERT INTO user_t
                  (user_id, email, password, language, first_name, last_name, user_type,
                   phone_number, gender, birthday, country, province, city, address,
                   post_code, verified, token, locked)
                VALUES
                  (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """;
        final String insertUserHost = """
                INSERT INTO user_host_t (user_id, host_id) VALUES (?, ?)
                """;
        final String insertCustomer = """
                INSERT INTO customer_t (host_id, customer_id, user_id, referral_id) VALUES (?, ?, ?, ?)
                """;
        final String insertEmployee = """
                INSERT INTO employee_t (host_id, employee_id, user_id, manager_id) VALUES (?, ?, ?, ?)
                """;

        Result<String> result;
        Map<String, Object> map = (Map<String, Object>)event.get(PortalConstants.DATA);
        String userId = (String)map.get("userId");

        try (Connection conn = SqlDbStartupHook.ds.getConnection()){
            conn.setAutoCommit(false);
            try {
                try (PreparedStatement statement = conn.prepareStatement(queryEmailEntityId)) {
                    statement.setString(1, (String)map.get("email"));
                    statement.setString(2, (String)map.get("entityId"));
                    try (ResultSet resultSet = statement.executeQuery()) {
                        if (resultSet.next()) {
                            logger.error("entityId {} or email {} already exists in database.", map.get("entityId"), map.get("email"));
                            throw new SQLException(String.format("entityId %s or email %s already exists in database.", map.get("entityId"), map.get("email")));
                        }
                    }
                }

                try (PreparedStatement statement = conn.prepareStatement(insertUser)) {
                    statement.setObject(1, UUID.fromString(userId));
                    statement.setString(2, (String)map.get("email"));
                    statement.setString(3, (String)map.get("password"));
                    statement.setString(4, (String)map.get("language"));
                    // ... (rest of the parameter settings for insertUser)
                    String firstName = (String)map.get("firstName");
                    if (firstName != null && !firstName.isEmpty()) statement.setString(5, firstName); else statement.setNull(5, Types.VARCHAR);
                    String lastName = (String)map.get("lastName");
                    if (lastName != null && !lastName.isEmpty()) statement.setString(6, lastName); else statement.setNull(6, Types.VARCHAR);
                    statement.setString(7, (String)map.get("userType"));
                    String phoneNumber = (String)map.get("phoneNumber");
                    if (phoneNumber != null && !phoneNumber.isEmpty()) statement.setString(8, phoneNumber); else statement.setNull(8, Types.VARCHAR);
                    String gender = (String) map.get("gender");
                    if (gender != null && !gender.isEmpty()) statement.setString(9, gender); else statement.setNull(9, Types.VARCHAR);
                    java.util.Date birthday = (java.util.Date)map.get("birthday"); // Assuming it's passed as java.util.Date
                    if (birthday != null) statement.setDate(10, new java.sql.Date(birthday.getTime())); else statement.setNull(10, Types.DATE);
                    String country = (String)map.get("country");
                    if (country != null && !country.isEmpty()) statement.setString(11, country); else statement.setNull(11, Types.VARCHAR);
                    String province = (String)map.get("province");
                    if (province != null && !province.isEmpty()) statement.setString(12, province); else statement.setNull(12, Types.VARCHAR);
                    String city = (String)map.get("city");
                    if (city != null && !city.isEmpty()) statement.setString(13, city); else statement.setNull(13, Types.VARCHAR);
                    String address = (String)map.get("address");
                    if (address != null && !address.isEmpty()) statement.setString(14, address); else statement.setNull(14, Types.VARCHAR);
                    String postCode = (String)map.get("postCode");
                    if (postCode != null && !postCode.isEmpty()) statement.setString(15, postCode); else statement.setNull(15, Types.VARCHAR);
                    statement.setBoolean(16, (Boolean)map.get("verified"));
                    statement.setString(17, (String)map.get("token"));
                    statement.setBoolean(18, (Boolean)map.get("locked"));
                    statement.execute();
                }
                try (PreparedStatement statement = conn.prepareStatement(insertUserHost)) {
                    statement.setObject(1, UUID.fromString(userId));
                    statement.setObject(2, UUID.fromString((String)map.get("hostId")));
                    statement.execute();
                }
                if("E".equals(map.get("userType"))) {
                    try (PreparedStatement statement = conn.prepareStatement(insertEmployee)) {
                        statement.setObject(1, UUID.fromString((String)map.get("hostId")));
                        statement.setString(2, (String)map.get("entityId"));
                        statement.setObject(3, UUID.fromString(userId));
                        String managerId = (String)map.get("managerId");
                        if(managerId != null && !managerId.isEmpty()) statement.setString(4, managerId); else statement.setNull(4, Types.VARCHAR);
                        statement.execute();
                    }
                } else if("C".equals(map.get("userType"))) {
                    try (PreparedStatement statement = conn.prepareStatement(insertCustomer)) {
                        statement.setObject(1, UUID.fromString((String)map.get("hostId")));
                        statement.setString(2, (String)map.get("entityId"));
                        statement.setObject(3, UUID.fromString(userId));
                        String referralId = (String)map.get("referralId");
                        if(referralId != null && !referralId.isEmpty()) statement.setString(4, referralId); else statement.setNull(4, Types.VARCHAR);
                        statement.execute();
                    }
                } else {
                    throw new SQLException("user_type is not valid: " + map.get("userType"));
                }
                conn.commit();
                result = Success.of(userId);
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
            logger.error("SQLException on getting connection:", e);
            result = Failure.of(new Status(SQL_EXCEPTION, e.getMessage()));
        }
        return result;
    }
    @Override
    public Result<String> loginUserByEmail(String email) {
        Result<String> result;
        String sql = """
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
                    u.email = ?
                    AND u.locked = FALSE
                    AND u.verified = TRUE
                GROUP BY
                    uh.host_id, u.user_id, u.user_type, e.employee_id, c.customer_id;
                """;
        try (final Connection conn = ds.getConnection()) {
            Map<String, Object> map = new HashMap<>();
            try (PreparedStatement statement = conn.prepareStatement(sql)) {
                statement.setString(1, email);
                try (ResultSet resultSet = statement.executeQuery()) {
                    if (resultSet.next()) {
                        map.put("hostId", resultSet.getObject("host_id", UUID.class));
                        map.put("userId", resultSet.getObject("user_id", UUID.class));
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
            if (map.isEmpty())
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
                        map.put("hostId", resultSet.getObject("host_id", UUID.class));
                        map.put("userId", resultSet.getObject("user_id", UUID.class));
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

        String sql = """
                SELECT h.host_id, u.user_id, u.email, u.password, u.language,
                u.first_name, u.last_name, u.user_type, u.phone_number, u.gender,
                u.birthday, u.country, u.province, u.city, u.address,
                u.post_code, u.verified, u.token, u.locked, u.nonce
                FROM user_t u, user_host_t h
                WHERE u.user_id = h.user_id
                AND u.user_id = ?
                """;

        try (final Connection conn = ds.getConnection()) {
            Map<String, Object> map = new HashMap();
            try (PreparedStatement statement = conn.prepareStatement(sql)) {
                statement.setObject(1, UUID.fromString(userId));
                try (ResultSet resultSet = statement.executeQuery()) {
                    if (resultSet.next()) {
                        map.put("hostId", resultSet.getObject("host_id", UUID.class));
                        map.put("userId", resultSet.getObject("user_id", UUID.class));
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

        String sqlEmployee = """
                SELECT h.host_id, u.user_id, e.employee_id as entity_id, u.email, u.password,
                u.language, u.first_name, u.last_name, u.user_type, u.phone_number,
                u.gender, u.birthday, u.country, u.province, u.city,
                u.address, u.post_code, u.verified, u.token, u.locked,
                u.nonce
                FROM user_t u, user_host_t h, employee_t e
                WHERE u.user_id = h.user_id
                AND h.host_id = e.host_id
                AND h.user_id = e.user_id
                AND e.employee_id = ?
                """;

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
                        map.put("hostId", resultSet.getObject("host_id", UUID.class));
                        map.put("userId", resultSet.getObject("user_id", UUID.class));
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
                        map.put("hostId", resultSet.getObject("host_id", UUID.class));
                        map.put("userId", resultSet.getObject("user_id", UUID.class));
                        map.put("firstName", resultSet.getInt("first_name"));
                        map.put("lastName", resultSet.getString("last_name"));
                        map.put("email", resultSet.getString("email"));
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
        parameters.add(UUID.fromString(hostId));

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

        if (!whereClause.isEmpty()) {
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
                    map.put("hostId", resultSet.getObject("host_id", UUID.class));
                    map.put("userId", resultSet.getObject("user_id", UUID.class));
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
        String sql = """
                SELECT email
                FROM user_t u, user_crypto_wallet_t w
                WHERE u.user_id = w.user_id
                AND w.crypto_type = ?
                AND w.crypto_address = ?
                """;
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
    @Override
    public Result<Long> queryNonceByUserId(String userId){
        final String updateNonceSql = "UPDATE user_t SET nonce = nonce + 1 WHERE user_id = ? RETURNING nonce;";
        Result<Long> result = null;
        try (Connection connection = ds.getConnection();
             PreparedStatement statement = connection.prepareStatement(updateNonceSql)) {
            Long nonce = null;
            statement.setObject(1, UUID.fromString(userId));
            try (ResultSet resultSet = statement.executeQuery()) {
                if(resultSet.next()){
                    nonce = (Long)resultSet.getObject(1);
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
    public Result<String> confirmUser(Map<String, Object> event) {
        final String queryTokenByEmail = "SELECT token FROM user_t WHERE user_id = ? AND token = ?";
        final String updateUserByEmail = "UPDATE user_t SET token = null, verified = true, nonce = ? WHERE user_id = ?";
        Result<String> result;
        Map<String, Object> map = (Map<String, Object>)event.get(PortalConstants.DATA);
        try (Connection conn = ds.getConnection()){
            conn.setAutoCommit(false);
            try (PreparedStatement statement = conn.prepareStatement(queryTokenByEmail)) {
                statement.setObject(1, UUID.fromString((String)event.get(Constants.USER)));
                statement.setString(2, (String)map.get("token"));
                try (ResultSet resultSet = statement.executeQuery()) {
                    if (resultSet.next()) {
                        // found the token record, update user_t for token, verified flog and nonce, write a success notification.
                        try (PreparedStatement updateStatement = conn.prepareStatement(updateUserByEmail)) {
                            updateStatement.setLong(1, ((Number)event.get(PortalConstants.NONCE)).longValue() + 1);
                            updateStatement.setObject(2, UUID.fromString((String)event.get(Constants.USER)));
                            updateStatement.execute();
                        }
                    } else {
                        // record is not found with the email and token. write an error notification.
                        throw new SQLException(String.format("token %s is not matched for userId %s.", map.get("token"), event.get(Constants.USER)));
                    }
                }
                conn.commit();
                result = Success.of((String)event.get(Constants.USER));
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

    /**
     * Update the verified to true and nonce in the user_t table based on the hostId and userId. Write a success notification.
     *
     * @param event UserVerifiedEvent
     * @return  Result of userId
     */
    @Override
    public Result<String> verifyUser(Map<String, Object> event) {
        final String updateUserByUserId = "UPDATE user_t SET token = null, verified = true WHERE user_id = ?";
        Result<String> result;
        Map<String, Object> map = (Map<String, Object>)event.get(PortalConstants.DATA);
        try (Connection conn = ds.getConnection()){
            conn.setAutoCommit(false);
            try (PreparedStatement statement = conn.prepareStatement(updateUserByUserId)) {
                statement.setObject(1, UUID.fromString((String)map.get("userId")));
                statement.execute();
                conn.commit();
                result = Success.of((String)map.get("userId"));
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

    /**
     * check the email, user_id is unique. if not, write an error notification. If yes, insert
     * the user into database and write a success notification.
     *
     * @param event event that is created by user service
     * @return result of email
     */
    @Override
    public Result<String> createSocialUser(Map<String, Object> event) {
        final String queryIdEmail = "SELECT nonce FROM user_t WHERE user_id = ? OR email = ?";
        final String insertUser = "INSERT INTO user_t (host_id, user_id, first_name, last_name, email, language, " +
                "verified, gender, birthday, country, province, city, post_code, address) " +
                "VALUES (?, ?, ?, ?, ?,   ?, ?, ?, ?, ?,   ?, ?, ?, ?)";
        Result<String> result = null;
        Map<String, Object> map = (Map<String, Object>)event.get(PortalConstants.DATA);
        try(Connection conn = ds.getConnection()) {
            conn.setAutoCommit(false);
            try {
                try (PreparedStatement statement = conn.prepareStatement(queryIdEmail)) {
                    statement.setObject(1, UUID.fromString((String)map.get("userId")));
                    statement.setString(2, (String)map.get("email"));
                    try (ResultSet resultSet = statement.executeQuery()) {
                        if (resultSet.next()) {
                            // found duplicate record, write an error notification.
                            throw new SQLException(String.format("userId %s or email %s already exists in database.", map.get("userId"), map.get("email")));
                        }
                    }
                }
                // no duplicate record, insert the user into database and write a success notification.
                try (PreparedStatement statement = conn.prepareStatement(insertUser)) {
                    statement.setObject(1, UUID.fromString((String)map.get("hostId")));
                    statement.setObject(2, UUID.fromString((String)map.get("userId")));
                    String firstName = (String)map.get("firstName");
                    if (firstName != null && !firstName.isEmpty())
                        statement.setString(3, firstName);
                    else
                        statement.setNull(3, NULL);
                    String lastName = (String)map.get("lastName");
                    if (lastName != null && !lastName.isEmpty())
                        statement.setString(4, lastName);
                    else
                        statement.setNull(4, NULL);

                    statement.setString(5, (String)map.get("email"));
                    statement.setString(6, (String)map.get("language"));
                    statement.setBoolean(7, (Boolean)map.get("verified"));
                    String gender = (String)map.get("gender");
                    if (gender != null && !gender.isEmpty()) {
                        statement.setString(8, gender);
                    } else {
                        statement.setNull(8, NULL);
                    }
                    java.util.Date birthday = (java.util.Date) map.get("birthday");
                    if (birthday != null) {
                        statement.setDate(9, new java.sql.Date(birthday.getTime()));
                    } else {
                        statement.setNull(9, NULL);
                    }
                    String country = (String)map.get("country");
                    if (country != null && !country.isEmpty()) {
                        statement.setString(10, country);
                    } else {
                        statement.setNull(10, NULL);
                    }
                    String province = (String)map.get("province");
                    if (province != null && !province.isEmpty()) {
                        statement.setString(11, province);
                    } else {
                        statement.setNull(11, NULL);
                    }
                    String city = (String)map.get("city");
                    if (city != null && !city.isEmpty()) {
                        statement.setString(12, city);
                    } else {
                        statement.setNull(12, NULL);
                    }
                    String postCode = (String)map.get("postCode");
                    if (postCode != null && !postCode.isEmpty()) {
                        statement.setString(13, postCode);
                    } else {
                        statement.setNull(13, NULL);
                    }
                    String address = (String)map.get("address");
                    if (address != null && !address.isEmpty()) {
                        statement.setString(14, address);
                    } else {
                        statement.setNull(14, NULL);
                    }
                    statement.execute();
                }
                conn.commit();
                result = Success.of((String)map.get("userId"));
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

    /**
     * update user if it exists in database.
     *
     * @param event event that is created by user service
     * @return result of email
     */
    @Override
    public Result<String> updateUser(Map<String, Object> event) {
        final String updateUser = "UPDATE user_t SET language = ?, first_name = ?, last_name = ?, phone_number = ?," +
                "gender = ?, birthday = ?, country = ?, province = ?, city = ?, address = ?, post_code = ? " +
                "WHERE user_id = ?";
        final String updateCustomer = "UPDATE customer_t SET referral_id = ? WHERE host_id = ? AND customer_id = ?";
        final String updateEmployee = "UPDATE employee_t SET manager_id = ? WHERE host_id = ? AND employee_id = ?";
        Result<String> result = null;
        Map<String, Object> map = (Map<String, Object>)event.get(PortalConstants.DATA);
        try (Connection conn = ds.getConnection()){
            conn.setAutoCommit(false);

            // no duplicate record, insert the user into database and write a success notification.
            try (PreparedStatement statement = conn.prepareStatement(updateUser)) {
                statement.setString(1, (String)map.get("language"));
                String firstName = (String)map.get("firstName");
                if (firstName != null && !firstName.isEmpty())
                    statement.setString(2, firstName);
                else
                    statement.setNull(2, NULL);
                String lastName = (String)map.get("lastName");
                if (lastName != null && !lastName.isEmpty())
                    statement.setString(3, lastName);
                else
                    statement.setNull(3, NULL);
                String phoneNumber = (String)map.get("phoneNumber");
                if (phoneNumber != null && !phoneNumber.isEmpty())
                    statement.setString(4, phoneNumber);
                else
                    statement.setNull(4, NULL);
                String gender = (String)map.get("gender");
                if(gender != null && !gender.isEmpty()) {
                    statement.setString(5, gender);
                } else {
                    statement.setNull(5, NULL);
                }

                java.util.Date birthday = (java.util.Date) map.get("birthday");
                if (birthday != null) {
                    statement.setDate(6, new java.sql.Date(birthday.getTime()));
                } else {
                    statement.setNull(6, NULL);
                }

                String country = (String)map.get("country");
                if (country != null && !country.isEmpty()) {
                    statement.setString(7, country);
                } else {
                    statement.setNull(7, NULL);
                }

                String province = (String)map.get("province");
                if (province != null && !province.isEmpty()) {
                    statement.setString(8, province);
                } else {
                    statement.setNull(8, NULL);
                }

                String city = (String)map.get("city");
                if (city != null && !city.isEmpty()) {
                    statement.setString(9, city);
                } else {
                    statement.setNull(9, NULL);
                }

                String address = (String)map.get("address");
                if (address != null && !address.isEmpty()) {
                    statement.setString(10, address);
                } else {
                    statement.setNull(10, NULL);
                }

                String postCode = (String)map.get("postCode");
                if (postCode != null && !postCode.isEmpty()) {
                    statement.setString(11, postCode);
                } else {
                    statement.setNull(11, NULL);
                }
                statement.setObject(12, UUID.fromString((String)map.get("userId")));
                int count = statement.executeUpdate();
                if (count == 0) {
                    // no record is updated, write an error notification.
                    throw new SQLException(String.format("no record is updated by userId %s", map.get("userId")));
                }
                // TODO there are old country, province and city in the event for maproot, so we need to update them
                // update customer or employee based on user_type
                if(map.get("userType").equals("E")) {
                    try (PreparedStatement updateStatement = conn.prepareStatement(updateEmployee)) {
                        String managerId = (String)map.get("managerId");
                        if(managerId != null && !managerId.isEmpty()) {
                            updateStatement.setString(1, managerId);
                        } else {
                            updateStatement.setNull(1, NULL);
                        }
                        updateStatement.setObject(2, UUID.fromString((String)map.get("hostId")));
                        updateStatement.setString(3, (String)map.get("entityId"));
                        updateStatement.execute();
                    }
                } else if(map.get("userType").equals("C")) {
                    try (PreparedStatement updateStatement = conn.prepareStatement(updateCustomer)) {
                        String referralId = (String)map.get("referralId");
                        if(referralId != null && !referralId.isEmpty()) {
                            updateStatement.setString(1, referralId);
                        } else {
                            updateStatement.setNull(1, NULL);
                        }
                        updateStatement.setObject(2, UUID.fromString((String)map.get("hostId")));
                        updateStatement.setString(3, (String)map.get("entityId"));
                        updateStatement.execute();
                    }
                } else {
                    throw new SQLException("userType is not valid: " + map.get("userType"));
                }
                conn.commit();
                if(logger.isTraceEnabled()) logger.trace("update user success: {}", map.get("userId"));
                result = Success.of((String)map.get("userId"));
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

    /**
     * delete user from user_t table and all other tables related to this user.
     *
     * @param event event that is created by user service
     * @return result of email
     */
    @Override
    public Result<String> deleteUser(Map<String, Object> event) {
        // delete only user_t, other tables will be cacade deleted by database
        final String deleteUserById = "DELETE from user_t WHERE user_id = ?";
        Result<String> result;
        Map<String, Object> map = (Map<String, Object>)event.get(PortalConstants.DATA);
        try (Connection conn = ds.getConnection()) {
            conn.setAutoCommit(false);
            try (PreparedStatement statement = conn.prepareStatement(deleteUserById)) {
                statement.setObject(1, UUID.fromString((String)map.get("userId")));
                int count = statement.executeUpdate();
                if (count == 0) {
                    // no record is deleted, write an error notification.
                    throw new SQLException(String.format("no record is deleted by userId %s", map.get("userId")));
                }
                conn.commit();
                result = Success.of((String)map.get("userId"));
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

    /**
     * update user_t for the forget password token by email
     *
     * @param event event that is created by user service
     * @return result of email
     */
    @Override
    public Result<String> forgetPassword(Map<String, Object> event) {
        final String deleteUserByEmail = "UPDATE user_t SET token = ?, nonce = ? WHERE email = ?";
        Result<String> result;
        Map<String, Object> map = (Map<String, Object>)event.get(PortalConstants.DATA);
        try (Connection conn = ds.getConnection()) {
            conn.setAutoCommit(false);
            try (PreparedStatement statement = conn.prepareStatement(deleteUserByEmail)) {
                statement.setString(1, (String)map.get("token"));
                statement.setLong(2, ((Number)event.get(PortalConstants.NONCE)).longValue() + 1);
                statement.setString(3, (String)map.get("email"));
                int count = statement.executeUpdate();
                if (count == 0) {
                    // no record is deleted, write an error notification.
                    throw new SQLException(String.format("no token is updated by email %s", map.get("email")));
                }
                conn.commit();
                result = Success.of((String)map.get("email"));
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

    /**
     * update user_t to reset the password by email
     *
     * @param event event that is created by user service
     * @return result of email
     */
    @Override
    public Result<String> resetPassword(Map<String, Object> event) {
        final String deleteUserByEmail = "UPDATE user_t SET token = ?, nonce = ? WHERE email = ?";
        Result<String> result;
        Map<String, Object> map = (Map<String, Object>)event.get(PortalConstants.DATA);
        try (Connection conn = ds.getConnection()) {
            conn.setAutoCommit(false);
            try (PreparedStatement statement = conn.prepareStatement(deleteUserByEmail)) {
                statement.setString(1, (String)map.get("token"));
                statement.setLong(2, ((Number)event.get(PortalConstants.NONCE)).longValue() + 1);
                statement.setString(3, (String)map.get("email"));
                int count = statement.executeUpdate();
                if (count == 0) {
                    // no record is deleted, write an error notification.
                    throw new SQLException(String.format("no token is updated by email %s", map.get("email")));
                }
                conn.commit();
                result = Success.of((String)map.get("email"));
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

    /**
     * update user_t to change the password by email
     *
     * @param event event that is created by user service
     * @return result of email
     */
    @Override
    public Result<String> changePassword(Map<String, Object> event) {
        final String updatePasswordByEmail = "UPDATE user_t SET password = ?, nonce = ? WHERE email = ? AND password = ?";
        Result<String> result;
        Map<String, Object> map = (Map<String, Object>)event.get(PortalConstants.DATA);
        try (Connection conn = ds.getConnection()) {
            conn.setAutoCommit(false);
            try (PreparedStatement statement = conn.prepareStatement(updatePasswordByEmail)) {
                statement.setString(1, (String)map.get("password"));
                statement.setLong(2, ((Number)event.get(PortalConstants.NONCE)).longValue() + 1);
                statement.setString(3, (String)map.get("email"));
                statement.setString(4, (String)map.get("oldPassword"));
                int count = statement.executeUpdate();
                if (count == 0) {
                    // no record is updated, write an error notification.
                    throw new SQLException(String.format("no password is updated by email %s", map.get("email")));
                }
                conn.commit();
                result = Success.of((String)map.get("email"));
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
    public Result<String> updatePayment(Map<String, Object> event) {
        return null;
    }

    @Override
    public Result<String> deletePayment(Map<String, Object> event) {
        return null;
    }

    @Override
    public Result<String> createOrder(Map<String, Object> event) {
        return null;
    }

    @Override
    public Result<String> cancelOrder(Map<String, Object> event) {
        return null;
    }

    @Override
    public Result<String> deliverOrder(Map<String, Object> event) {
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
    public Result<String> sendPrivateMessage(Map<String, Object> event) {
        final String insertMessage = "INSERT INTO message_t (from_id, nonce, to_email, subject, content, send_time) VALUES (?, ?, ?, ?, ?, ?)";
        Result<String> result;
        Map<String, Object> map = (Map<String, Object>)event.get(PortalConstants.DATA);
        try (Connection conn = ds.getConnection()) {
            conn.setAutoCommit(false);
            try (PreparedStatement statement = conn.prepareStatement(insertMessage)) {
                statement.setString(1, (String)map.get("fromId"));
                statement.setLong(2, ((Number)event.get(PortalConstants.NONCE)).longValue());
                statement.setString(3, (String)map.get("toEmail"));
                statement.setString(4, (String)map.get("subject"));
                statement.setString(5, (String)map.get("content"));
                statement.setObject(6, OffsetDateTime.parse((String)event.get(CloudEventV1.TIME)));
                statement.executeUpdate();

                conn.commit();
                result = Success.of((String)map.get("fromId"));
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
    public Result<String> queryUserLabel(String hostId) {
        Result<String> result = null;
        String sql = "SELECT u.user_id, u.email FROM user_t u, user_host_t h WHERE u.user_id = h.user_id AND h.host_id = ?";
        List<Map<String, Object>> labels = new ArrayList<>();
        try (Connection connection = ds.getConnection();
             PreparedStatement preparedStatement = connection.prepareStatement(sql)) {
            preparedStatement.setObject(1, UUID.fromString(hostId));
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
    public Result<String> queryNotification(int offset, int limit, String hostId, String userId, Long nonce, String eventClass, Boolean successFlag,
                                            Timestamp processTs, String eventJson, String error) {
        Result<String> result = null;
        StringBuilder sqlBuilder = new StringBuilder();
        sqlBuilder.append("SELECT COUNT(*) OVER () AS total,\n" +
                "host_id, user_id, nonce, event_class, is_processed, process_ts, event_json, error\n" +
                "FROM notification_t\n" +
                "WHERE host_id = ?\n");

        List<Object> parameters = new ArrayList<>();
        parameters.add(UUID.fromString(hostId));

        StringBuilder whereClause = new StringBuilder();

        addCondition(whereClause, parameters, "user_id", userId != null ? UUID.fromString(userId) : null);
        addCondition(whereClause, parameters, "nonce", nonce);
        addCondition(whereClause, parameters, "event_class", eventClass);
        addCondition(whereClause, parameters, "is_processed", successFlag);
        addCondition(whereClause, parameters, "event_json", eventJson);
        addCondition(whereClause, parameters, "error", error);

        if (!whereClause.isEmpty()) {
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
                    map.put("hostId", resultSet.getObject("host_id", UUID.class));
                    map.put("userId", resultSet.getObject("user_id", UUID.class));
                    map.put("nonce", resultSet.getLong("nonce"));
                    map.put("eventClass", resultSet.getString("event_class"));
                    map.put("processFlag", resultSet.getBoolean("is_processed"));
                    // handling date properly
                    map.put("processTs", resultSet.getObject("process_ts") != null ? resultSet.getObject("process_ts", OffsetDateTime.class) : null);
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

}
