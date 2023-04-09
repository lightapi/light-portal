package net.lightapi.portal.db;

import com.github.benmanes.caffeine.cache.Cache;
import com.networknt.config.JsonMapper;
import com.networknt.kafka.common.AvroConverter;
import com.networknt.monad.Failure;
import com.networknt.monad.Result;
import com.networknt.monad.Success;
import com.networknt.status.Status;
import net.lightapi.portal.market.*;
import net.lightapi.portal.user.*;

import java.sql.*;
import java.util.HashMap;
import java.util.Map;

import static java.sql.Types.NULL;
import static com.networknt.db.provider.SqlDbStartupHook.cacheManager;
import static com.networknt.db.provider.SqlDbStartupHook.ds;

public class PortalDbProviderImpl implements PortalDbProvider {
    public static final String SQL_EXCEPTION = "ERR10017";
    public static final String GENERIC_EXCEPTION = "ERR10014";
    public static final String OBJECT_NOT_FOUND = "ERR11637";

    public static final String INSERT_NOTIFICATION = "INSERT INTO notification_t (email, nonce, event_json, process_time, " +
            "process_flag, error) VALUES (?, ?, ?, ?, ?, ?)";
    public static final String UPDATE_NONCE = "UPDATE user_t SET nonce = ? WHERE email = ?";

    @Override
    public Result<String> queryUserByEmail(String email) {
        Result<String> result = null;
        String sql =
                "SELECT host, user_id, first_name, last_name, email, roles, language, gender, birthday, taiji_wallet, " +
                        "country, province, city, post_code, address, verified, token, locked, password, nonce FROM user_t " +
                        "WHERE email = ?";
        try (final Connection conn = ds.getConnection()) {
            Map<String, Object> map = new HashMap();
            try (PreparedStatement statement = conn.prepareStatement(sql)) {
                statement.setString(1, email);
                try (ResultSet resultSet = statement.executeQuery()) {
                    if (resultSet.next()) {
                        map.put("host", resultSet.getString("host"));
                        map.put("userId", resultSet.getString("user_id"));
                        map.put("firstName", resultSet.getString("first_name"));
                        map.put("lastName", resultSet.getString("last_name"));
                        map.put("email", resultSet.getString("email"));
                        map.put("password", resultSet.getString("password"));
                        map.put("language", resultSet.getString("language"));
                        map.put("gender", resultSet.getString("gender"));
                        map.put("birthday", resultSet.getDate("birthday"));
                        map.put("taijiWallet", resultSet.getString("taiji_wallet"));
                        map.put("country", resultSet.getString("country"));
                        map.put("province", resultSet.getString("province"));
                        map.put("city", resultSet.getString("city"));
                        map.put("postCode", resultSet.getString("post_code"));
                        map.put("address", resultSet.getString("address"));
                        map.put("verified", resultSet.getBoolean("verified"));
                        map.put("token", resultSet.getString("token"));
                        map.put("locked", resultSet.getBoolean("locked"));
                        map.put("nonce", resultSet.getLong("nonce"));
                        map.put("roles", resultSet.getString("roles"));
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
                "SELECT host, user_id, first_name, last_name, email, roles, language, gender, birthday, " +
                        "taiji_wallet, country, province, city, post_code, address, verified, token, locked FROM user_t " +
                        "WHERE user_id = ?";
        try (final Connection conn = ds.getConnection()) {
            Map<String, Object> map = new HashMap();
            try (PreparedStatement statement = conn.prepareStatement(sql)) {
                statement.setString(1, userId);
                try (ResultSet resultSet = statement.executeQuery()) {
                    if (resultSet.next()) {
                        map.put("host", resultSet.getInt("host"));
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
    public Result<String> queryUserByWallet(String wallet) {
        Result<String> result = null;
        String sql =
                "SELECT host, user_id, first_name, last_name, email, roles, language, gender, birthday, " +
                        "taiji_wallet, country, province, city, post_code, address, verified, token, locked FROM user_t " +
                        "WHERE taiji_wallet = ?";
        try (final Connection conn = ds.getConnection()) {
            Map<String, Object> map = new HashMap();
            try (PreparedStatement statement = conn.prepareStatement(sql)) {
                statement.setString(1, wallet);
                try (ResultSet resultSet = statement.executeQuery()) {
                    if (resultSet.next()) {
                        map.put("host", resultSet.getInt("host"));
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
                result = Failure.of(new Status(OBJECT_NOT_FOUND, "user", wallet));
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
    public Result<String> queryEmailByWallet(String wallet) {
        Result<String> result = null;
        String sql = "SELECT email FROM user_t WHERE taiji_wallet = ?";
        try (final Connection conn = ds.getConnection()) {
            String email = null;
            try (PreparedStatement statement = conn.prepareStatement(sql)) {
                statement.setString(1, wallet);
                try (ResultSet resultSet = statement.executeQuery()) {
                    if (resultSet.next()) {
                        email = resultSet.getString("email");
                    }
                }
            }
            if(email == null)
                result = Failure.of(new Status(OBJECT_NOT_FOUND, "user email", wallet));
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
     * @param email The email address of the user
     * @param nonce The nonce of the notification
     * @param json The json string of the event
     * @param flag The flag of the notification
     * @param error The error message of the notification
     * @throws SQLException when there is an error in the database access
     */
    public void insertNotification(Connection conn, String email, long nonce, String json, boolean flag, String error) throws SQLException {
        try (PreparedStatement statement = conn.prepareStatement(INSERT_NOTIFICATION)) {
            statement.setString(1, email);
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
     * @param email The email address of the user
     * @param nonce The nonce of the notification
     * @throws SQLException when there is an error in the database access
     * @return the number of rows updated
     */
    public int updateNonce(Connection conn, long nonce, String email) throws SQLException {
        int count = 0;
        try (PreparedStatement statement = conn.prepareStatement(UPDATE_NONCE)) {
            statement.setLong(1, nonce);
            statement.setString(2, email);
            count = statement.executeUpdate();
        }
        return count;
    }

    /**
     * check the email, user_id, taiji_wallet is unique. if not, write an error notification. If yes, insert
     * the user into database and write a success notification.
     *
     * @param event event that is created by user service
     * @return result of email
     *
     */
    @Override
    public Result<String> createUser(UserCreatedEvent event) {
        final String queryIdEmailWallet = "SELECT nonce FROM user_t WHERE user_id = ? OR email = ? OR taiji_wallet = ?";
        final String queryIdEmail = "SELECT nonce FROM user_t WHERE user_id = ? OR email = ?";
        final String insertUser = "INSERT INTO user_t (host, user_id, first_name, last_name, email, roles, language, " +
                "verified, token, gender, password, birthday, country, province, city, post_code, address) " +
                "VALUES (?, ?, ?, ?, ?,   ?, ?, ?, ?, ?,   ?, ?, ?, ?, ?,   ?, ?)";
        Result<String> result = null;
        Map<String, Object> map = JsonMapper.string2Map(event.getValue());
        Connection conn = null;
        try {
            conn = ds.getConnection();
            conn.setAutoCommit(false);
            if(event.getTaijiWallet() != null) {
                try (PreparedStatement statement = conn.prepareStatement(queryIdEmailWallet)) {
                    statement.setString(1, event.getUserId());
                    statement.setString(2, event.getEmail());
                    statement.setString(3, event.getTaijiWallet());
                    try (ResultSet resultSet = statement.executeQuery()) {
                        if (resultSet.next()) {
                            // found duplicate record, write an error notification.
                            insertNotification(conn, event.getEmail(), event.getEventId().getNonce(), AvroConverter.toJson(event, false), false,  "userId or email or wallet already exists in database.");
                            return result;
                        }
                    }
                }
            } else {
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
            }
            // no duplicate record, insert the user into database and write a success notification.
            try (PreparedStatement statement = conn.prepareStatement(insertUser)) {
                statement.setString(1, event.getHost());
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
                statement.setString(9, event.getToken());
                if(map.get("gender") != null) {
                    statement.setString(10, (String)map.get("gender"));
                } else {
                    statement.setNull(10, NULL);
                }
                statement.setString(11, event.getPassword());
                java.util.Date birthday = (java.util.Date)map.get("birthday");
                if(birthday != null) {
                    statement.setDate(12, new java.sql.Date(birthday.getTime()));
                } else {
                    statement.setNull(12, NULL);
                }
                Object countryObject = event.get("country");
                if(countryObject != null) {
                    statement.setString(13, (String)countryObject);
                } else {
                    statement.setNull(13, NULL);
                }
                Object provinceObject = event.get("province");
                if(provinceObject != null) {
                    statement.setString(14, (String)provinceObject);
                } else {
                    statement.setNull(14, NULL);
                }
                Object cityObject = event.get("city");
                if(cityObject != null) {
                    statement.setString(15, (String)cityObject);
                } else {
                    statement.setNull(15, NULL);
                }
                Object postCodeObject = map.get("post_code");
                if(postCodeObject != null) {
                    statement.setString(16, (String)postCodeObject);
                } else {
                    statement.setNull(16, NULL);
                }
                Object addressObject = map.get("address");
                if(addressObject != null) {
                    statement.setString(17, (String)addressObject);
                } else {
                    statement.setNull(17, NULL);
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

    @Override
    public Result<Integer> queryNonceByEmail(String email) {
        Result<Integer> result = null;
        String sql = "SELECT nonce FROM user_t WHERE email = ?";
        try (final Connection conn = ds.getConnection()) {
            Integer nonce = null;
            try (PreparedStatement statement = conn.prepareStatement(sql)) {
                statement.setString(1, email);
                try (ResultSet resultSet = statement.executeQuery()) {
                    if (resultSet.next()) {
                        nonce = resultSet.getInt("nonce");
                    }
                }
            }
            if(nonce == null)
                result = Failure.of(new Status(OBJECT_NOT_FOUND, "user nonce", email));
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
        final String insertUser = "INSERT INTO user_t (host, user_id, first_name, last_name, email, roles, language, " +
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
                statement.setString(1, event.getHost());
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
        final String updateUser = "UPDATE user_t SET host = ?, language = ?, taiji_wallet = ?, country = ?, province = ?, " +
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
                statement.setString(1, event.getHost());
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
        final String insertUser = "INSERT INTO application_t (host, application_id, application_name, application_description, " +
                "is_kafka_application, client_id, client_type, client_profile, client_secret, client_scope, custom_claim, " +
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
                statement.setString(1, event.getHost());
                statement.setString(2, event.getApplicationId());
                statement.setString(3, (String)map.get("applicationName"));
                if(map.get("applicationDescription") != null)
                    statement.setString(4, (String)map.get("applicationDescription"));
                else
                    statement.setNull(4, NULL);
                if(map.get("isKafkaApplication") != null)
                    statement.setBoolean(5, (Boolean)map.get("isKafkaApplication"));
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
                    insertNotification(conn, event.getEventId().getId(), event.getEventId().getNonce(), AvroConverter.toJson(event, false), false, "failed to insert the application " + event.getApplicationId());
                } else {
                    insertNotification(conn, event.getEventId().getId(), event.getEventId().getNonce(), AvroConverter.toJson(event, false), true,  null);
                }
            }
            updateNonce(conn, event.getEventId().getNonce() + 1, event.getEventId().getId());
            // as this is a brand-new user, there is no nonce to be updated. By default, the nonce is 0.
            conn.commit();
            result = Success.of(event.getApplicationId());
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
        final String updateApplication = "UPDATE application_t SET application_name = ?, application_description = ?, is_kafka_application = ?, " +
                "client_type = ?, client_profile = ?, client_scope = ?, custom_claim = ?, redirect_uri = ?, authenticate_class = ?, " +
                "deref_client_id = ?, operation_owner = ?, delivery_owner = ?, update_user = ?, update_timestamp = ? " +
                "WHERE host = ? AND application_id = ?";

        Result<String> result = null;
        Map<String, Object> map = JsonMapper.string2Map(event.getValue());
        Connection conn = null;
        try {
            conn = ds.getConnection();
            conn.setAutoCommit(false);

            try (PreparedStatement statement = conn.prepareStatement(updateApplication)) {
                if(map.get("applicationName") != null) {
                    statement.setString(1, (String)map.get("applicationName"));
                } else {
                    statement.setNull(1, NULL);
                }
                if(map.get("applicationDescription") != null) {
                    statement.setString(2, (String)map.get("applicationDescription"));
                } else {
                    statement.setNull(2, NULL);
                }
                if(map.get("isKafkaApplication") != null) {
                    statement.setBoolean(3, (Boolean)map.get("isKafkaApplication"));
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
                statement.setString(15, event.getHost());
                statement.setString(16, event.getApplicationId());

                int count = statement.executeUpdate();
                if(count == 0) {
                    // no record is updated, write an error notification.
                    insertNotification(conn, event.getEventId().getId(), event.getEventId().getNonce(), AvroConverter.toJson(event, false), false,  "no record is updated by application" + event.getApplicationId());
                    return result;
                } else {
                    insertNotification(conn, event.getEventId().getId(), event.getEventId().getNonce(), AvroConverter.toJson(event, false), true, null);
                }
                updateNonce(conn, event.getEventId().getNonce() + 1, event.getEventId().getId());
            }
            // as this is a brand-new user, there is no nonce to be updated. By default, the nonce is 0.
            conn.commit();
            result = Success.of(event.getApplicationId());
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
        final String deleteApplication = "DELETE from application_t WHERE host = ? AND application_id = ?";
        // TODO delete all other tables related to this user.
        Result<String> result;
        Connection conn = null;
        try {
            conn = ds.getConnection();
            conn.setAutoCommit(false);
            try (PreparedStatement statement = conn.prepareStatement(deleteApplication)) {
                statement.setString(1, event.getHost());
                statement.setString(2, event.getApplicationId());
                int count = statement.executeUpdate();
                if(count == 0) {
                    // no record is deleted, write an error notification.
                    insertNotification(conn, event.getEventId().getId(), event.getEventId().getNonce(), AvroConverter.toJson(event, false), false,  "no record is deleted for application " + event.getApplicationId());
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
                "SELECT host, application_id, application_name, application_description, is_kafka_application, client_id, " +
                        "client_type, client_profile, client_secret, client_scope, custom_claim, redirect_uri, authenticate_class, " +
                        "deref_client_id, operation_owner, delivery_owner, update_user, update_timestamp " +
                        "FROM application_t WHERE client_id = ?";
        try (final Connection conn = ds.getConnection()) {
            Map<String, Object> map = new HashMap<>();
            try (PreparedStatement statement = conn.prepareStatement(sql)) {
                statement.setString(1, clientId);
                try (ResultSet resultSet = statement.executeQuery()) {
                    if (resultSet.next()) {
                        map.put("host", resultSet.getString("host"));
                        map.put("applicationId", resultSet.getString("application_id"));
                        map.put("applicationDescription", resultSet.getString("application_description"));
                        map.put("isKafkaApplication", resultSet.getBoolean("is_kafka_application"));
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
    public Result<Map<String, Object>> queryClientByHostAppId(String host, String applicationId) {
        Result<Map<String, Object>> result;
        String sql =
                "SELECT host, application_id, application_name, application_description, is_kafka_application, client_id, " +
                        "client_type, client_profile, client_secret, client_scope, custom_claim, redirect_uri, authenticate_class, " +
                        "deref_client_id, operation_owner, delivery_owner, update_user, update_timestamp " +
                        "FROM application_t WHERE host = ? AND application_id = ?";
        try (final Connection conn = ds.getConnection()) {
            Map<String, Object> map = new HashMap<>();
            try (PreparedStatement statement = conn.prepareStatement(sql)) {
                statement.setString(1, host);
                statement.setString(2, applicationId);
                try (ResultSet resultSet = statement.executeQuery()) {
                    if (resultSet.next()) {
                        map.put("host", resultSet.getString("host"));
                        map.put("applicationIdd", resultSet.getString("application_id"));
                        map.put("applicationDescription", resultSet.getString("application_description"));
                        map.put("isKafkaApplication", resultSet.getBoolean("is_kafka_application"));
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
                        map.put("update_timestamp", resultSet.getTimestamp("update_timestamp"));
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
        final String insertUser = "INSERT INTO api_t (host, api_id, api_name, service_id, api_description, operation_owner, " +
                "delivery_owner, api_type_id, region_id, lob_id, platform_id, capability_id, api_marketplace_team_id, " +
                "git_repository, update_user, update_timestamp) " +
                "VALUES (?, ?, ?, ?, ?,   ?, ?, ?, ?, ?,   ?, ?, ?, ?, ?,   ?)";
        Result<String> result = null;
        Map<String, Object> map = JsonMapper.string2Map(event.getValue());
        Connection conn = null;
        try {
            conn = ds.getConnection();
            conn.setAutoCommit(false);
            // no duplicate record, insert the user into database and write a success notification.
            try (PreparedStatement statement = conn.prepareStatement(insertUser)) {
                statement.setString(1, event.getHost());
                statement.setString(2, event.getApiId());
                statement.setString(3, (String)map.get("apiName"));
                statement.setString(4, (String)map.get("serviceId"));
                if(map.get("apiDescription") != null)
                    statement.setString(5, (String)map.get("applicationDescription"));
                else
                    statement.setNull(5, NULL);

                if(map.get("operationOwner") != null)
                    statement.setString(6, (String)map.get("operationOwner"));
                else
                    statement.setNull(6, NULL);
                if (map.get("deliveryOwner") != null)
                    statement.setString(7, (String)map.get("deliveryOwner"));
                else
                    statement.setNull(7, NULL);
                if (map.get("apiTypeId") != null)
                    statement.setInt(8, (Integer)map.get("apiTypeId"));
                else
                    statement.setNull(8, NULL);
                if (map.get("regionId") != null)
                    statement.setInt(9, (Integer)map.get("regionId"));
                else
                    statement.setNull(9, NULL);
                if (map.get("lobId") != null)
                    statement.setInt(10, (Integer)map.get("lobId"));
                else
                    statement.setNull(10, NULL);
                if (map.get("platformId") != null)
                    statement.setInt(11, (Integer)map.get("platformId"));
                else
                    statement.setNull(11, NULL);
                if (map.get("capabilityId") != null)
                    statement.setInt(12, (Integer)map.get("capabilityId"));
                else
                    statement.setNull(12, NULL);
                if (map.get("apiMarketplaceTeamId") != null)
                    statement.setInt(13, (Integer)map.get("apiMarketplaceTeamId"));
                else
                    statement.setNull(13, NULL);
                if (map.get("gitRepository") != null)
                    statement.setString(14, (String)map.get("gitRepository"));
                else
                    statement.setNull(14, NULL);
                statement.setString(15, event.getEventId().getId());
                statement.setTimestamp(16, new Timestamp(System.currentTimeMillis()));
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
        final String updateApi = "UPDATE api_t SET api_name = ?, service_id = ?, service_type = ?, api_description = ? " +
                "operation_owner = ?, delivery_owner = ?, api_type_id = ?, region_id = ?, lob_id = ?, platform_id = ?, " +
                "capability_id = ?, api_marketplace_team_id = ?, git_repository = ?, update_user = ?, update_timestamp = ? " +
                "WHERE host = ? AND api_id = ?";

        Result<String> result = null;
        Map<String, Object> map = JsonMapper.string2Map(event.getValue());
        Connection conn = null;
        try {
            conn = ds.getConnection();
            conn.setAutoCommit(false);

            try (PreparedStatement statement = conn.prepareStatement(updateApi)) {
                if(map.get("apiName") != null) {
                    statement.setString(1, (String)map.get("apiName"));
                } else {
                    statement.setNull(1, NULL);
                }
                if(map.get("serviceId") != null) {
                    statement.setString(2, (String)map.get("serviceId"));
                } else {
                    statement.setNull(2, NULL);
                }
                if(map.get("serviceType") != null) {
                    statement.setString(3, (String)map.get("serviceType"));
                } else {
                    statement.setNull(3, NULL);
                }
                if(map.get("apiDescription") != null) {
                    statement.setString(4, (String)map.get("apiDescription"));
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
                if(map.get("apiTypeId") != null)
                    statement.setInt(7, (Integer)map.get("apiTypeId"));
                else
                    statement.setNull(7, NULL);
                if(map.get("regionId") != null)
                    statement.setInt(8, (Integer)map.get("regionId"));
                else
                    statement.setNull(8, NULL);
                if(map.get("lobId") != null) {
                    statement.setInt(9, (Integer)map.get("lobId"));
                } else {
                    statement.setNull(9, NULL);
                }
                if(map.get("platformId") != null) {
                    statement.setInt(10, (Integer)map.get("platformId"));
                } else {
                    statement.setNull(10, NULL);
                }
                if(map.get("capabilityId") != null) {
                    statement.setInt(11, (Integer)map.get("capabilityId"));
                } else {
                    statement.setNull(11, NULL);
                }
                if(map.get("apiMarketplaceTeamId") != null) {
                    statement.setInt(12, (Integer)map.get("apiMarketplaceTeamId"));
                } else {
                    statement.setNull(12, NULL);
                }
                if(map.get("gitRepository") != null) {
                    statement.setString(13, (String)map.get("gitRepository"));
                } else {
                    statement.setNull(13, NULL);
                }
                statement.setString(14, event.getEventId().getId());
                statement.setTimestamp(15, new Timestamp(event.getTimestamp()));
                statement.setString(16, event.getHost());
                statement.setString(17, event.getApiId());

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
        final String deleteApplication = "DELETE from api_t WHERE host = ? AND api_id = ?";
        Result<String> result;
        Connection conn = null;
        try {
            conn = ds.getConnection();
            conn.setAutoCommit(false);
            try (PreparedStatement statement = conn.prepareStatement(deleteApplication)) {
                statement.setString(1, event.getHost());
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
                e.printStackTrace();
            }
        }
        return result;

    }

    public Result<String> createMarketCode(MarketCodeCreatedEvent event) {
        Cache<Object, Object> authCodeCache = cacheManager.getCache("auth_code");
        if(logger.isTraceEnabled()) logger.trace("insert into the cache auth_code with key " + event.getAuthCode() + " value " + event.getValue());
        if(logger.isTraceEnabled()) logger.trace("estimate the size of the cache auth_code before is " + authCodeCache.estimatedSize());
        authCodeCache.put(event.getAuthCode(), event.getValue());
        if(logger.isTraceEnabled()) logger.trace("estimate the size of the cache auth_code after is " + authCodeCache.estimatedSize());
        return Success.of(event.getAuthCode());
    }

    public Result<String> deleteMarketCode(MarketCodeDeletedEvent event) {
        Cache<Object, Object> authCodeCache = cacheManager.getCache("auth_code");
        if(logger.isTraceEnabled()) logger.trace("insert into the cache auth_code with key " + event.getAuthCode() + " value ");
        if(logger.isTraceEnabled()) logger.trace("estimate the size of the cache auth_code before is " + authCodeCache.estimatedSize());
        authCodeCache.invalidate(event.getAuthCode());
        if(logger.isTraceEnabled()) logger.trace("estimate the size of the cache auth_code after is " + authCodeCache.estimatedSize());
        return Success.of(event.getAuthCode());
    }

    public Result<String> queryMarketCode(String authCode) {
        Cache<Object, Object> authCodeCache = cacheManager.getCache("auth_code");
        if(logger.isTraceEnabled()) logger.trace("estimate the size of the cache auth_code is " + authCodeCache.estimatedSize());
        String value = (String)authCodeCache.getIfPresent(authCode);
        if(logger.isTraceEnabled()) logger.trace("retrieve cache auth_code with key " + authCode + " value " + value);
        if(value != null) {
            return Success.of(value);
        } else {
            return Failure.of(new Status(OBJECT_NOT_FOUND, "auth code not found"));
        }
    }

    @Override
    public Result<String> createHost(HostCreatedEvent event) {
        final String insertHost = "INSERT INTO host_t (id, host, org_name, org_desc, org_owner, update_user, update_timestamp) " +
                "VALUES (?, ?, ?, ?, ?,   ?, ?)";
        Result<String> result;
        Map<String, Object> map = JsonMapper.string2Map(event.getValue());
        Connection conn = null;
        try {
            conn = ds.getConnection();
            conn.setAutoCommit(false);
            // no duplicate record, insert the user into database and write a success notification.
            try (PreparedStatement statement = conn.prepareStatement(insertHost)) {
                statement.setString(1, event.getId());
                statement.setString(2, event.getHost());
                statement.setString(3, event.getName());
                statement.setString(4, event.getDesc());
                statement.setString(5, event.getOwner());
                statement.setString(6, event.getEventId().getId());
                statement.setTimestamp(7, new Timestamp(event.getTimestamp()));
                int count = statement.executeUpdate();
                if (count == 0) {
                    insertNotification(conn, event.getEventId().getId(), event.getEventId().getNonce(), AvroConverter.toJson(event, false), false, "failed to insert the host " + event.getHost());
                } else {
                    insertNotification(conn, event.getEventId().getId(), event.getEventId().getNonce(), AvroConverter.toJson(event, false), true,  null);
                }
            }
            updateNonce(conn, event.getEventId().getNonce() + 1, event.getEventId().getId());
            // as this is a brand-new user, there is no nonce to be updated. By default, the nonce is 0.
            conn.commit();
            result = Success.of(event.getHost());
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
    public Result<String> updateHost(HostUpdatedEvent event) {
        final String updateHost = "UPDATE host_t SET org_name = ?, org_desc = ?, org_owner = ?, update_user = ? " +
                "update_timestamp = ? " +
                "WHERE host = ?";

        Result<String> result = null;
        Map<String, Object> map = JsonMapper.string2Map(event.getValue());
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
                statement.setString(6, event.getHost());

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
            result = Success.of(event.getHost());
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
    public Result<String> deleteHost(HostDeletedEvent event) {
        final String deleteHost = "DELETE from host_t WHERE host = ?";
        Result<String> result;
        Connection conn = null;
        try {
            conn = ds.getConnection();
            conn.setAutoCommit(false);
            try (PreparedStatement statement = conn.prepareStatement(deleteHost)) {
                statement.setString(1, event.getHost());
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
                        map.put("id", resultSet.getString("id"));
                        map.put("host", resultSet.getString("host"));
                        map.put("orgName", resultSet.getString("org_name"));
                        map.put("orgDesc", resultSet.getString("org_desc"));
                        map.put("orgOwner", resultSet.getBoolean("org_owner"));
                        map.put("updateUser", resultSet.getString("update_user"));
                        map.put("update_timestamp", resultSet.getTimestamp("update_timestamp"));
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
        final String queryHostById = "SELECT * from host_t WHERE id = ?";
        Result<Map<String, Object>> result;
        try (final Connection conn = ds.getConnection()) {
            Map<String, Object> map = new HashMap<>();
            try (PreparedStatement statement = conn.prepareStatement(queryHostById)) {
                statement.setString(1, id);
                try (ResultSet resultSet = statement.executeQuery()) {
                    if (resultSet.next()) {
                        map.put("id", resultSet.getString("id"));
                        map.put("host", resultSet.getString("host"));
                        map.put("orgName", resultSet.getString("org_name"));
                        map.put("orgDesc", resultSet.getString("org_desc"));
                        map.put("orgOwner", resultSet.getBoolean("org_owner"));
                        map.put("updateUser", resultSet.getString("update_user"));
                        map.put("update_timestamp", resultSet.getTimestamp("update_timestamp"));
                    }
                }
            }
            if(map.size() == 0)
                result = Failure.of(new Status(OBJECT_NOT_FOUND, "host with id ", id));
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
                        map.put("id", resultSet.getString("id"));
                        map.put("host", resultSet.getString("host"));
                        map.put("orgName", resultSet.getString("org_name"));
                        map.put("orgDesc", resultSet.getString("org_desc"));
                        map.put("orgOwner", resultSet.getBoolean("org_owner"));
                        map.put("updateUser", resultSet.getString("update_user"));
                        map.put("update_timestamp", resultSet.getTimestamp("update_timestamp"));
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
}
