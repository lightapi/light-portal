package net.lightapi.portal.db.persistence;

import com.networknt.monad.Result;
import java.sql.Timestamp;

public interface NotificationDataPersistence {
    Result<String> queryNotification(int offset, int limit, String hostId, String userId, Long nonce, String eventClass, Boolean successFlag, Timestamp processTs, String eventJson, String error);
}
