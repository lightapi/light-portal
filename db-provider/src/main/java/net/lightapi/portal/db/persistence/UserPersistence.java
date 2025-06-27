package net.lightapi.portal.db.persistence;

import com.networknt.monad.Result;

import java.sql.Connection; // Added import
import java.sql.SQLException; // Added import
import java.sql.Timestamp;
import java.util.Map;

public interface UserPersistence {
    Result<String> loginUserByEmail(String email);
    Result<String> queryUserByEmail(String email);
    Result<String> queryUserById(String userId);
    Result<String> queryUserByTypeEntityId(String userType, String entityId);
    Result<String> queryUserByWallet(String cryptoType, String cryptoAddress);
    Result<String> queryUserByHostId(int offset, int limit, String hostId, String email, String language, String userType, String entityId, String referralId, String managerId, String firstName, String lastName, String phoneNumber, String gender, String birthday, String country, String province, String city, String address, String postCode, Boolean verified, Boolean locked);

    void createUser(Connection conn, Map<String, Object> event) throws SQLException, Exception;
    Result<Long> queryNonceByUserId(String userId);

    void confirmUser(Connection conn, Map<String, Object> event) throws SQLException, Exception;
    void verifyUser(Connection conn, Map<String, Object> event) throws SQLException, Exception;
    void createSocialUser(Connection conn, Map<String, Object> event) throws SQLException, Exception;
    void updateUser(Connection conn, Map<String, Object> event) throws SQLException, Exception;
    void deleteUser(Connection conn, Map<String, Object> event) throws SQLException, Exception;
    void forgetPassword(Connection conn, Map<String, Object> event) throws SQLException, Exception;
    void resetPassword(Connection conn, Map<String, Object> event) throws SQLException, Exception;
    void changePassword(Connection conn, Map<String, Object> event) throws SQLException, Exception;

    Result<String> queryUserLabel(String hostId);
    Result<String> queryEmailByWallet(String cryptoType, String cryptoAddress);

    void sendPrivateMessage(Connection conn, Map<String, Object> event) throws SQLException, Exception;

    // updatePayment, deletePayment, createOrder, cancelOrder, deliverOrder might belong to a separate Order/PaymentPersistence
    void updatePayment(Connection conn, Map<String, Object> event) throws SQLException, Exception;
    void deletePayment(Connection conn, Map<String, Object> event) throws SQLException, Exception;
    void createOrder(Connection conn, Map<String, Object> event) throws SQLException, Exception;
    void cancelOrder(Connection conn, Map<String, Object> event) throws SQLException, Exception;
    void deliverOrder(Connection conn, Map<String, Object> event) throws SQLException, Exception;

    Result<String> queryNotification(int offset, int limit, String hostId, String userId, Long nonce, String eventClass, Boolean successFlag,
                                     Timestamp processTs, String eventJson, String error);

}
