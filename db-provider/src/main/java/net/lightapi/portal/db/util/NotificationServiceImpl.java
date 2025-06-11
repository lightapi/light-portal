package net.lightapi.portal.db.util;

import com.networknt.config.JsonMapper;
import com.networknt.utility.Constants;
import io.cloudevents.core.v1.CloudEventV1;
import net.lightapi.portal.PortalConstants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Types;
import java.time.OffsetDateTime;
import java.util.Map;
import java.util.UUID;

import static com.networknt.db.provider.SqlDbStartupHook.ds;

public class NotificationServiceImpl implements NotificationService {
    private static final Logger logger = LoggerFactory.getLogger(NotificationServiceImpl.class);
    public static final String INSERT_NOTIFICATION_SQL = """
            INSERT INTO notification_t
              (id, host_id, user_id, nonce, event_class, event_json, process_ts, is_processed, error)
            VALUES
              (?, ?, ?, ?, ?,  ?, ?, ?, ?)
            """;

    @Override
    public void insertNotification(Map<String, Object> event, boolean flag, String error) throws SQLException {
        // Ensure ds is initialized. This might happen in a startup hook in a real app.
        if (ds == null) {
            logger.error("Datasource ds is not initialized in NotificationServiceImpl.");
            throw new SQLException("Datasource not available.");
        }
        try (Connection conn = ds.getConnection(); // Obtain connection here
             PreparedStatement statement = conn.prepareStatement(INSERT_NOTIFICATION_SQL)) {

            statement.setObject(1, UUID.fromString((String)event.get(CloudEventV1.ID)));
            statement.setObject(2, UUID.fromString((String)event.get(Constants.HOST)));
            statement.setObject(3, UUID.fromString((String)event.get(Constants.USER)));
            statement.setLong(4, ((Number)event.get(PortalConstants.NONCE)).longValue());
            statement.setString(5, (String)event.get(CloudEventV1.TYPE));
            statement.setString(6, JsonMapper.toJson(event)); // Serialize the whole event map
            statement.setObject(7, OffsetDateTime.parse((String)event.get(CloudEventV1.TIME)));
            statement.setBoolean(8, flag);
            if (error != null && !error.isEmpty()) {
                statement.setString(9, error);
            } else {
                statement.setNull(9, Types.VARCHAR);
            }
            statement.executeUpdate();
        } catch (SQLException e) {
            logger.error("SQLException in insertNotification:", e);
            throw e; // Re-throw to be handled by the caller's transaction logic if any
        }
    }
}
