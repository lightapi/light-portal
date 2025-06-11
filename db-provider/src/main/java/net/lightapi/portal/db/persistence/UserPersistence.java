package net.lightapi.portal.db.persistence;

import com.networknt.monad.Result;

import java.sql.Timestamp;
import java.util.Map;

public interface UserPersistence {
    Result<String> loginUserByEmail(String email);
    Result<String> queryUserByEmail(String email);
    Result<String> queryUserById(String userId);
    Result<String> queryUserByTypeEntityId(String userType, String entityId);
    Result<String> queryUserByWallet(String cryptoType, String cryptoAddress);
    Result<String> queryUserByHostId(int offset, int limit, String hostId, String email, String language, String userType, String entityId, String referralId, String managerId, String firstName, String lastName, String phoneNumber, String gender, String birthday, String country, String province, String city, String address, String postCode, Boolean verified, Boolean locked);
    Result<String> createUser(Map<String, Object> event);
    Result<Long> queryNonceByUserId(String userId);
    Result<String> confirmUser(Map<String, Object> event);
    Result<String> verifyUser(Map<String, Object> event);
    Result<String> createSocialUser(Map<String, Object> event);
    Result<String> updateUser(Map<String, Object> event);
    Result<String> deleteUser(Map<String, Object> event);
    Result<String> forgetPassword(Map<String, Object> event);
    Result<String> resetPassword(Map<String, Object> event);
    Result<String> changePassword(Map<String, Object> event);
    Result<String> queryUserLabel(String hostId);
    Result<String> queryEmailByWallet(String cryptoType, String cryptoAddress);
    Result<String> sendPrivateMessage(Map<String, Object> event);
    // updatePayment, deletePayment, createOrder, cancelOrder, deliverOrder might belong to a separate Order/PaymentPersistence
    Result<String> updatePayment(Map<String, Object> event);
    Result<String> deletePayment(Map<String, Object> event);
    Result<String> createOrder(Map<String, Object> event);
    Result<String> cancelOrder(Map<String, Object> event);
    Result<String> deliverOrder(Map<String, Object> event);
    Result<String> queryNotification(int offset, int limit, String hostId, String userId, Long nonce, String eventClass, Boolean successFlag,
                                     Timestamp processTs, String eventJson, String error);

}
