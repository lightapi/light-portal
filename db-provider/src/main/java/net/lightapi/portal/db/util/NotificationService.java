package net.lightapi.portal.db.util;

import java.sql.SQLException;
import java.util.Map;

public interface NotificationService {
    void insertNotification(Map<String, Object> event, boolean flag, String error) throws SQLException;
}
