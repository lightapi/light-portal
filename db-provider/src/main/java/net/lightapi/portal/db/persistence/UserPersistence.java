package net.lightapi.portal.db.persistence;

import com.networknt.monad.Result;
import net.lightapi.portal.db.PortalPersistenceException;

import java.sql.Connection; // Added import
import java.sql.SQLException; // Added import
import java.sql.Timestamp;
import java.util.Map;

public interface UserPersistence {
    Result<String> loginUserByEmail(String email);
    Result<String> queryUserByEmail(String email);
    Result<String> queryUserById(String userId);
    Result<String> getUserById(String userId);
    Result<String> queryUserByTypeEntityId(String userType, String entityId);
    Result<String> queryUserByWallet(String cryptoType, String cryptoAddress);
    Result<String> queryUserByHostId(int offset, int limit, String filters, String globalFilter, String sorting, boolean active);
    Result<String> getHostsByUserId(String userId);
    Result<String> getHostLabelByUserId(String userId);

    void createUser(Connection conn, Map<String, Object> event) throws PortalPersistenceException;
    void onboardUser(Connection conn, Map<String, Object> event) throws PortalPersistenceException;
    long queryNonceByUserId(String userId);

    void confirmUser(Connection conn, Map<String, Object> event) throws PortalPersistenceException;
    void verifyUser(Connection conn, Map<String, Object> event) throws PortalPersistenceException;
    void createSocialUser(Connection conn, Map<String, Object> event) throws PortalPersistenceException;
    void updateUser(Connection conn, Map<String, Object> event) throws PortalPersistenceException;
    void deleteUser(Connection conn, Map<String, Object> event) throws PortalPersistenceException;
    void forgetPassword(Connection conn, Map<String, Object> event) throws PortalPersistenceException;
    void resetPassword(Connection conn, Map<String, Object> event) throws PortalPersistenceException;
    void changePassword(Connection conn, Map<String, Object> event) throws PortalPersistenceException;

    Result<String> queryUserLabel(String hostId);
    Result<String> getUserLabelNotInHost(String hostId);
    Result<String> queryEmailByWallet(String cryptoType, String cryptoAddress);

    void sendPrivateMessage(Connection conn, Map<String, Object> event) throws PortalPersistenceException;

    // updatePayment, deletePayment, createOrder, cancelOrder, deliverOrder might belong to a separate Order/PaymentPersistence
    void updatePayment(Connection conn, Map<String, Object> event) throws PortalPersistenceException;
    void deletePayment(Connection conn, Map<String, Object> event) throws PortalPersistenceException;
    void createOrder(Connection conn, Map<String, Object> event) throws PortalPersistenceException;
    void cancelOrder(Connection conn, Map<String, Object> event) throws PortalPersistenceException;
    void deliverOrder(Connection conn, Map<String, Object> event) throws PortalPersistenceException;

    Result<String> queryNotification(int offset, int limit, String hostId, String userId, Long nonce, String eventClass, Boolean successFlag,
                                     Timestamp processTs, String eventJson, String error);

}
