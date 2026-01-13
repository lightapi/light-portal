package net.lightapi.portal.db.persistence;

import com.networknt.monad.Result;
import java.sql.Timestamp;
import java.time.OffsetDateTime;

public interface NotificationDataPersistence {
    Result<String> queryNotification(int offset, int limit, String hostId, String userId, Long nonce, String eventClass, Boolean successFlag, OffsetDateTime processTs, String eventJson, String error);
}
