package net.lightapi.portal.db.persistence;

import com.networknt.config.JsonMapper;
import com.networknt.monad.Failure;
import com.networknt.monad.Result;
import com.networknt.monad.Success;
import com.networknt.status.Status;
import com.networknt.utility.Constants;
import io.cloudevents.core.v1.CloudEventV1;
import net.lightapi.portal.db.PortalDbProvider;
import net.lightapi.portal.db.util.NotificationService;
import net.lightapi.portal.db.util.SqlUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;
import java.time.OffsetDateTime;
import java.util.*;

import static com.networknt.db.provider.SqlDbStartupHook.ds;
import static java.sql.Types.NULL;
import static net.lightapi.portal.db.util.SqlUtil.*;

public class ApiServicePersistenceImpl implements ApiServicePersistence {
    private static final Logger logger = LoggerFactory.getLogger(ApiServicePersistenceImpl.class);
    private static final String SQL_EXCEPTION = PortalDbProvider.SQL_EXCEPTION;
    private static final String GENERIC_EXCEPTION = PortalDbProvider.GENERIC_EXCEPTION;
    private static final String OBJECT_NOT_FOUND = PortalDbProvider.OBJECT_NOT_FOUND;

    private final NotificationService notificationService;

    public ApiServicePersistenceImpl(NotificationService notificationService) {
        this.notificationService = notificationService;
    }

    @Override
    public void createApi(Connection conn, Map<String, Object> event) throws SQLException, Exception {
        // Use UPSERT: INSERT ON CONFLICT DO UPDATE
        // This handles:
        // 1. First time insert (no conflict).
        // 2. Re-creation (conflict on host_id, api_id) -> UPDATE the existing soft-deleted row (setting active=TRUE and new version).

        final String sql =
                """
                INSERT INTO api_t (
                    host_id, api_id, api_name, api_desc, operation_owner,
                    delivery_owner, region, business_group, lob, platform,
                    capability, git_repo, api_tags, api_status, update_user,
                    update_ts, aggregate_version, active
                ) VALUES (?, ?, ?, ?, ?,   ?, ?, ?, ?, ?,   ?, ?, ?, ?, ?,   ?, ?, TRUE)
                ON CONFLICT (host_id, api_id) DO UPDATE
                SET api_name = EXCLUDED.api_name,
                    api_desc = EXCLUDED.api_desc,
                    operation_owner = EXCLUDED.operation_owner,
                    delivery_owner = EXCLUDED.delivery_owner,
                    region = EXCLUDED.region,
                    business_group = EXCLUDED.business_group,
                    lob = EXCLUDED.lob,
                    platform = EXCLUDED.platform,
                    capability = EXCLUDED.capability,
                    git_repo = EXCLUDED.git_repo,
                    api_tags = EXCLUDED.api_tags,
                    api_status = EXCLUDED.api_status,
                    update_user = EXCLUDED.update_user,
                    update_ts = EXCLUDED.update_ts,
                    aggregate_version = EXCLUDED.aggregate_version,
                    active = TRUE
                -- OCC/IDM: Only update if the incoming event is newer
                WHERE api_t.aggregate_version < EXCLUDED.aggregate_version AND api_t.active = FALSE
                """;

        // Note: The original code uses a non-standard map retrieval: Map<String, Object> map = (Map<String, Object>)event.get(PortalConstants.DATA);
        // Assuming SqlUtil.extractEventData(event) is the correct utility based on other methods.
        Map<String, Object> map = SqlUtil.extractEventData(event);

        String hostId = (String) map.get("hostId");
        String apiId = (String) map.get("apiId");
        long newAggregateVersion = SqlUtil.getNewAggregateVersion(event);

        try (PreparedStatement statement = conn.prepareStatement(sql)) {
            // INSERT values (17 placeholders + active=TRUE in SQL, total 17 dynamic values)
            // 1: host_id
            statement.setObject(1, UUID.fromString(hostId));
            // 2: api_id
            statement.setString(2, apiId);
            // 3: api_name
            statement.setString(3, (String) map.get("apiName"));

            // 4: api_desc
            String apiDesc = (String) map.get("apiDesc");
            if (apiDesc != null && !apiDesc.trim().isEmpty()) {
                statement.setString(4, apiDesc);
            } else {
                statement.setNull(4, Types.VARCHAR);
            }
            // 5: operation_owner
            String operationOwner = (String) map.get("operationOwner");
            if (operationOwner != null && !operationOwner.trim().isEmpty()) {
                statement.setObject(5, UUID.fromString(operationOwner));
            } else {
                statement.setNull(5, Types.OTHER);
            }
            // 6: delivery_owner
            String deliveryOwner = (String) map.get("deliveryOwner");
            if (deliveryOwner != null && !deliveryOwner.trim().isEmpty()) {
                statement.setObject(6, UUID.fromString(deliveryOwner));
            } else {
                statement.setNull(6, Types.OTHER);
            }

            // 7: region
            String region = (String) map.get("region");
            if (region != null && !region.trim().isEmpty()) {
                statement.setString(7, region);
            } else {
                statement.setNull(7, Types.VARCHAR);
            }
            // 8: businessGroup
            String businessGroup = (String) map.get("businessGroup");
            if (businessGroup != null && !businessGroup.trim().isEmpty()) {
                statement.setString(8, businessGroup);
            } else {
                statement.setNull(8, Types.VARCHAR);
            }
            // 9: lob
            String lob = (String) map.get("lob");
            if (lob != null && !lob.trim().isEmpty()) {
                statement.setString(9, lob);
            } else {
                statement.setNull(9, Types.VARCHAR);
            }
            // 10: platform
            String platform = (String) map.get("platform");
            if (platform != null && !platform.trim().isEmpty()) {
                statement.setString(10, platform);
            } else {
                statement.setNull(10, Types.VARCHAR);
            }
            // 11: capability
            String capability = (String) map.get("capability");
            if (capability != null && !capability.trim().isEmpty()) {
                statement.setString(11, capability);
            } else {
                statement.setNull(11, Types.VARCHAR);
            }
            // 12: gitRepo
            String gitRepo = (String) map.get("gitRepo");
            if (gitRepo != null && !gitRepo.trim().isEmpty()) {
                statement.setString(12, gitRepo);
            } else {
                statement.setNull(12, Types.VARCHAR);
            }
            // 13: apiTags
            String apiTags = (String) map.get("apiTags");
            if (apiTags != null && !apiTags.trim().isEmpty()) {
                statement.setString(13, apiTags);
            } else {
                statement.setNull(13, Types.VARCHAR);
            }

            // 14: apiStatus
            statement.setString(14, (String) map.get("apiStatus"));
            // 15: update_user
            statement.setString(15, (String) event.get(Constants.USER));
            // 16: update_ts
            statement.setObject(16, OffsetDateTime.parse((String) event.get(CloudEventV1.TIME)));
            // 17: aggregate_version
            statement.setLong(17, newAggregateVersion);

            int count = statement.executeUpdate();
            if (count == 0) {
                // count=0 means the ON CONFLICT clause was hit, BUT the WHERE clause (aggregate_version < EXCLUDED.aggregate_version) failed.
                // This is the desired idempotent/out-of-order protection behavior. Log and ignore.
                logger.warn("Creation/Reactivation skipped for hostId {} apiId {} aggregateVersion {}. A newer or same version already exists.", hostId, apiId, newAggregateVersion);
            }
        } catch (SQLException e) {
            logger.error("SQLException during createService for apiId {} aggregateVersion {}: {}", apiId, newAggregateVersion, e.getMessage(), e);
            throw e;
        } catch (Exception e) {
            logger.error("Exception during createService for apiId {} aggregateVersion {}: {}", apiId, newAggregateVersion, e.getMessage(), e);
            throw e;
        }
    }

    @Override
    public void updateApi(Connection conn, Map<String, Object> event) throws SQLException, Exception {
        // We attempt to update the record IF the incoming event's aggregate_version is greater than the current projection's version.
        // This enforces Idempotence (IDM) and Optimistic Concurrency Control (OCC) by ensuring version monotonicity.
        // We explicitly set active = TRUE as an UPDATE event implies the service should be active.
        final String sql =
                """
                UPDATE api_t
                SET api_name = ?,
                    api_desc = ?,
                    operation_owner = ?,
                    delivery_owner = ?,
                    region = ?,
                    business_group = ?,
                    lob = ?,
                    platform = ?,
                    capability = ?,
                    git_repo = ?,
                    api_tags = ?,
                    api_status = ?,
                    update_user = ?,
                    update_ts = ?,
                    aggregate_version = ?,
                    active = TRUE
                WHERE host_id = ?
                  AND api_id = ?
                  AND aggregate_version < ?
                """; // <<< CRITICAL: Changed aggregate_version = ? to aggregate_version < ? to enforce monotonicity (OCC/IDM)

        // Note: The original code uses a non-standard map retrieval: Map<String, Object> map = (Map<String, Object>)event.get(PortalConstants.DATA);
        // Assuming SqlUtil.extractEventData(event) is the correct utility based on other methods.
        Map<String, Object> map = SqlUtil.extractEventData(event);

        String hostId = (String) map.get("hostId");
        String apiId = (String) map.get("apiId");
        long newAggregateVersion = SqlUtil.getNewAggregateVersion(event);

        try (PreparedStatement statement = conn.prepareStatement(sql)) {
            // SET values (14 dynamic values + active = TRUE in SQL)
            int i = 1;
            // 1: api_name
            statement.setString(i++, (String) map.get("apiName"));

            // 2: api_desc
            if (map.containsKey("apiDesc")) {
                String apiDesc = (String) map.get("apiDesc");
                if (apiDesc != null && !apiDesc.trim().isEmpty()) {
                    statement.setString(i++, apiDesc);
                } else {
                    statement.setNull(i++, Types.VARCHAR);
                }
            } else {
                statement.setNull(i++, Types.VARCHAR);
            }

            // 3: operation_owner
            String operationOwner = (String) map.get("operationOwner");
            if (operationOwner != null && !operationOwner.trim().isEmpty()) {
                statement.setObject(i++, UUID.fromString(operationOwner));
            } else {
                statement.setNull(i++, Types.OTHER);
            }

            // 4: delivery_owner
            String deliveryOwner = (String) map.get("deliveryOwner");
            if (deliveryOwner != null && !deliveryOwner.trim().isEmpty()) {
                statement.setObject(i++, UUID.fromString(deliveryOwner));
            } else {
                statement.setNull(i++, Types.OTHER);
            }

            // 5: region
            if (map.containsKey("region")) {
                String region = (String) map.get("region");
                if (region != null && !region.trim().isEmpty()) {
                    statement.setString(i++, region);
                } else {
                    statement.setNull(i++, Types.VARCHAR);
                }
            } else {
                statement.setNull(i++, Types.VARCHAR);
            }
            // 6: businessGroup
            if (map.containsKey("businessGroup")) {
                String businessGroup = (String) map.get("businessGroup");
                if (businessGroup != null && !businessGroup.trim().isEmpty()) {
                    statement.setString(i++, businessGroup);
                } else {
                    statement.setNull(i++, Types.VARCHAR);
                }
            } else {
                statement.setNull(i++, Types.VARCHAR);
            }
            // 7: lob
            if (map.containsKey("lob")) {
                String lob = (String) map.get("lob");
                if (lob != null && !lob.trim().isEmpty()) {
                    statement.setString(i++, lob);
                } else {
                    statement.setNull(i++, Types.VARCHAR);
                }
            } else {
                statement.setNull(i++, Types.VARCHAR);
            }
            // 8: platform
            if (map.containsKey("platform")) {
                String platform = (String) map.get("platform");
                if (platform != null && !platform.trim().isEmpty()) {
                    statement.setString(i++, platform);
                } else {
                    statement.setNull(i++, Types.VARCHAR);
                }
            } else {
                statement.setNull(i++, Types.VARCHAR);
            }
            // 9: capability
            if (map.containsKey("capability")) {
                String capability = (String) map.get("capability");
                if (capability != null && !capability.trim().isEmpty()) {
                    statement.setString(i++, capability);
                } else {
                    statement.setNull(i++, Types.VARCHAR);
                }
            } else {
                statement.setNull(i++, Types.VARCHAR);
            }
            // 10: gitRepo
            if (map.containsKey("gitRepo")) {
                String gitRepo = (String) map.get("gitRepo");
                if (gitRepo != null && !gitRepo.trim().isEmpty()) {
                    statement.setString(i++, gitRepo);
                } else {
                    statement.setNull(i++, Types.VARCHAR);
                }
            } else {
                statement.setNull(i++, Types.VARCHAR);
            }
            // 11: apiTags
            if (map.containsKey("apiTags")) {
                String apiTags = (String) map.get("apiTags");
                if (apiTags != null && !apiTags.trim().isEmpty()) {
                    statement.setString(i++, apiTags);
                } else {
                    statement.setNull(i++, Types.VARCHAR);
                }
            } else {
                statement.setNull(i++, Types.VARCHAR);
            }
            // 12: apiStatus
            statement.setString(i++, (String) map.get("apiStatus"));
            // 13: update_user
            statement.setString(i++, (String) event.get(Constants.USER));
            // 14: update_ts
            statement.setObject(i++, OffsetDateTime.parse((String) event.get(CloudEventV1.TIME)));
            // 15: aggregate_version
            statement.setLong(i++, newAggregateVersion);

            // WHERE conditions (3 placeholders)
            // 16: host_id
            statement.setObject(i++, UUID.fromString(hostId));
            // 17: api_id
            statement.setString(i++, apiId);
            // 18: aggregate_version < ? (new version for OCC/IDM check)
            statement.setLong(i++, newAggregateVersion);

            int count = statement.executeUpdate();
            if (count == 0) {
                // If 0 rows updated, it means the record was either not found
                // OR aggregate_version >= newAggregateVersion (OCC/IDM check failed).
                // We IGNORE the failure and log a warning, as this is the desired idempotent/monotonic behavior.
                logger.warn("Update skipped for hostId {} apiId {} aggregateVersion {}. Record not found or a newer/same version already exists.", hostId, apiId, newAggregateVersion);
            }
        } catch (SQLException e) {
            logger.error("SQLException during updateService for apiId {} aggregateVersion {}: {}", apiId, newAggregateVersion, e.getMessage(), e);
            throw e;
        } catch (Exception e) {
            logger.error("Exception during updateService for apiId {} aggregateVersion {}: {}", apiId, newAggregateVersion, e.getMessage(), e);
            throw e;
        }
    }

    @Override
    public void deleteApi(Connection conn, Map<String, Object> event) throws SQLException, Exception {
        // Use UPDATE to implement Soft Delete (setting active = FALSE).
        // OCC/IDM is enforced by checking aggregate_version < newAggregateVersion.
        final String sql =
                """
                UPDATE api_t
                SET active = FALSE,
                    update_user = ?,
                    update_ts = ?,
                    aggregate_version = ?
                WHERE host_id = ?
                  AND api_id = ?
                  AND aggregate_version < ?
                """; // <<< CRITICAL: Added aggregate_version < ? to enforce monotonicity (OCC/IDM)

        // Note: The original code uses a non-standard map retrieval: Map<String, Object> map = (Map<String, Object>)event.get(PortalConstants.DATA);
        // Assuming SqlUtil.extractEventData(event) is the correct utility based on other methods.
        Map<String, Object> map = SqlUtil.extractEventData(event);

        String hostId = (String) map.get("hostId");
        String apiId = (String) map.get("apiId");
        // A delete event represents a state change, so it should have a new, incremented version.
        long newAggregateVersion = SqlUtil.getNewAggregateVersion(event);

        try (PreparedStatement statement = conn.prepareStatement(sql)) {
            // SET values (3 placeholders)
            // 1: update_user
            statement.setString(1, (String) event.get(Constants.USER));
            // 2: update_ts
            statement.setObject(2, OffsetDateTime.parse((String) event.get(CloudEventV1.TIME)));
            // 3: aggregate_version (the new version)
            statement.setLong(3, newAggregateVersion);

            // WHERE conditions (3 placeholders)
            // 4: host_id
            statement.setObject(4, UUID.fromString(hostId));
            // 5: api_id
            statement.setString(5, apiId);
            // 6: aggregate_version < ? (new version for OCC/IDM check)
            statement.setLong(6, newAggregateVersion);

            int count = statement.executeUpdate();
            if (count == 0) {
                // If 0 rows updated, it means:
                // 1. The record was not found (already deleted or never existed).
                // 2. The OCC/IDM check failed (aggregate_version >= newAggregateVersion).
                // We IGNORE the failure and log a warning, as this is the desired idempotent/monotonic behavior.
                logger.warn("Soft delete skipped for hostId {} apiId {} aggregateVersion {}. Record not found or a newer/same version already exists.", hostId, apiId, newAggregateVersion);
            }
        } catch (SQLException e) {
            logger.error("SQLException during deleteService for apiId {} aggregateVersion {}: {}", apiId, newAggregateVersion, e.getMessage(), e);
            throw e;
        } catch (Exception e) {
            logger.error("Exception during deleteService for apiId {} aggregateVersion {}: {}", apiId, newAggregateVersion, e.getMessage(), e);
            throw e;
        }
    }

    @Override
    public Result<String> queryApi(int offset, int limit, String filtersJson, String globalFilter, String sortingJson, String hostId) {
        Result<String> result = null;
        List<Map<String, Object>> filters = parseJsonList(filtersJson);
        List<Map<String, Object>> sorting = parseJsonList(sortingJson);

        String s =
                """
                SELECT COUNT(*) OVER () AS total,
                host_id, api_id, api_name,
                api_desc, operation_owner, delivery_owner, region, business_group,
                lob, platform, capability, git_repo, api_tags, api_status,
                update_user, update_ts, aggregate_version, active
                FROM api_t
                WHERE host_id = ?
                """;

        List<Object> parameters = new ArrayList<>();
        parameters.add(UUID.fromString(hostId));

        String[] searchColumns = {"api_id", "api_name", "api_desc"};
        String sqlBuilder = s + dynamicFilter(Arrays.asList("host_id"), Arrays.asList(searchColumns), filters, null, parameters) +
                globalFilter(globalFilter, searchColumns, parameters) +
                dynamicSorting("api_id", sorting, null) +
                "\nLIMIT ? OFFSET ?";

        parameters.add(limit);
        parameters.add(offset);
        int total = 0;
        List<Map<String, Object>> services = new ArrayList<>();

        try (Connection connection = ds.getConnection();
            PreparedStatement preparedStatement = connection.prepareStatement(sqlBuilder)) {
            populateParameters(preparedStatement, parameters);
            boolean isFirstRow = true;
            try (ResultSet resultSet = preparedStatement.executeQuery()) {
                while (resultSet.next()) {
                    Map<String, Object> map = new HashMap<>();
                    // only get the total once as it is the same for all rows.
                    if (isFirstRow) {
                        total = resultSet.getInt("total");
                        isFirstRow = false;
                    }

                    map.put("hostId", resultSet.getObject("host_id", UUID.class));
                    map.put("apiId", resultSet.getString("api_id"));
                    map.put("apiName", resultSet.getString("api_name"));
                    map.put("apiDesc", resultSet.getString("api_desc"));
                    map.put("operationOwner", resultSet.getObject("operation_owner", UUID.class));
                    map.put("deliveryOwner", resultSet.getObject("delivery_owner", UUID.class));
                    map.put("region", resultSet.getString("region"));
                    map.put("businessGroup", resultSet.getString("business_group"));
                    map.put("lob", resultSet.getString("lob"));
                    map.put("platform", resultSet.getString("platform"));
                    map.put("capability", resultSet.getString("capability"));
                    map.put("gitRepo", resultSet.getString("git_repo"));
                    map.put("apiTags", resultSet.getString("api_tags"));
                    map.put("apiStatus", resultSet.getString("api_status"));
                    map.put("updateUser", resultSet.getString("update_user"));
                    map.put("updateTs", resultSet.getObject("update_ts", OffsetDateTime.class));
                    map.put("aggregateVersion", resultSet.getLong("aggregate_version"));
                    map.put("active", resultSet.getBoolean("active"));
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

    @Override
    public Result<String> getApiVersionIdLabel(String hostId) {
        Result<String> result = null;
        String sql =
                """
                SELECT av.api_version_id, av.api_id, a.api_name, av.api_version
                FROM api_version_t av, api_t a
                WHERE av.api_id = a.api_id
                AND av.host_id = ?
                """;

        List<Map<String, Object>> labels = new ArrayList<>();
        try (Connection connection = ds.getConnection();
             PreparedStatement preparedStatement = connection.prepareStatement(sql)) {
            preparedStatement.setObject(1, UUID.fromString(hostId));
            try (ResultSet resultSet = preparedStatement.executeQuery()) {
                while (resultSet.next()) {
                    Map<String, Object> map = new HashMap<>();
                    map.put("id", resultSet.getObject("api_version_id"));
                    map.put("label", resultSet.getString("api_id") + "|" + resultSet.getString("api_version") + "|" + resultSet.getString("api_name"));
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
    public Result<String> queryApiLabel(String hostId) {
        Result<String> result = null;
        String sql = "SELECT api_id, api_name FROM api_t WHERE host_id = ?";
        List<Map<String, Object>> labels = new ArrayList<>();
        try (Connection connection = ds.getConnection();
             PreparedStatement preparedStatement = connection.prepareStatement(sql)) {
            preparedStatement.setObject(1, UUID.fromString(hostId));
            try (ResultSet resultSet = preparedStatement.executeQuery()) {
                while (resultSet.next()) {
                    Map<String, Object> map = new HashMap<>();
                    map.put("id", resultSet.getString("api_id"));
                    map.put("label", resultSet.getString("api_name"));
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
    public Result<String> getApiById(String hostId, String apiId) {
        Result<String> result;
        String sql =
            """
            SELECT host_id, api_id, api_name,
            api_desc, operation_owner, delivery_owner, region, business_group,
            lob, platform, capability, git_repo, api_status,
            update_user, update_ts, aggregate_version, active
            FROM api_t
            WHERE host_id = ? AND api_id = ?
            """;
        try (final Connection conn = ds.getConnection()) {
            Map<String, Object> map = new HashMap<>();
            try (PreparedStatement statement = conn.prepareStatement(sql)) {
                statement.setObject(1, UUID.fromString(hostId));
                statement.setString(2, apiId);
                try (ResultSet resultSet = statement.executeQuery()) {
                    if (resultSet.next()) {
                        map.put("hostId", resultSet.getString("host_id"));
                        map.put("apiId", resultSet.getString("api_id"));
                        map.put("apiName", resultSet.getString("api_name"));
                        map.put("apiDesc", resultSet.getString("api_desc"));
                        map.put("operationOwner", resultSet.getObject("operation_owner", UUID.class));
                        map.put("deliveryOwner", resultSet.getObject("delivery_owner", UUID.class));
                        map.put("region", resultSet.getString("region"));
                        map.put("businessGroup", resultSet.getString("business_group"));
                        map.put("lob", resultSet.getString("lob"));
                        map.put("platform", resultSet.getString("platform"));
                        map.put("capability", resultSet.getString("capability"));
                        map.put("gitRepo", resultSet.getString("git_repo"));
                        map.put("apiStatus", resultSet.getString("api_status"));
                        map.put("updateUser", resultSet.getString("update_user"));
                        map.put("updateTs", resultSet.getObject("update_ts") != null ? resultSet.getObject("update_ts", OffsetDateTime.class) : null);
                        map.put("aggregateVersion", resultSet.getLong("aggregate_version"));
                        map.put("active", resultSet.getBoolean("active"));
                    }
                }
            }
            if (map.isEmpty())
                result = Failure.of(new Status(OBJECT_NOT_FOUND, "getApiById", apiId));
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
    public Result<String> queryApiVersionLabel(String hostId, String apiId) {
        Result<String> result = null;
        String sql = "SELECT api_version FROM api_version_t WHERE host_id = ? AND api_id = ?";
        List<Map<String, Object>> versions = new ArrayList<>();
        try (Connection connection = ds.getConnection();
             PreparedStatement preparedStatement = connection.prepareStatement(sql)) {
            preparedStatement.setObject(1, UUID.fromString(hostId));
            preparedStatement.setString(2, apiId);
            try (ResultSet resultSet = preparedStatement.executeQuery()) {
                while (resultSet.next()) {
                    Map<String, Object> map = new HashMap<>();
                    String id = resultSet.getString("api_version");
                    map.put("id", id);
                    map.put("label", id);
                    versions.add(map);
                }
            }
            result = Success.of(JsonMapper.toJson(versions));
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
    public Result<String> queryEndpointLabel(String hostId, String apiVersionId) {
        Result<String> result = null;
        List<Map<String, Object>> labels = new ArrayList<>();
        if(apiVersionId == null || apiVersionId.isEmpty()) {
            // return an empty array in the case apiVersionId is not selected in the form.
            return Success.of(JsonMapper.toJson(labels));
        }
        String sql = "SELECT endpoint_id, endpoint FROM api_endpoint_t WHERE host_id = ? AND api_version_id = ?";
        try (Connection connection = ds.getConnection();
             PreparedStatement preparedStatement = connection.prepareStatement(sql)) {
            preparedStatement.setObject(1, UUID.fromString(hostId));
            preparedStatement.setObject(2, UUID.fromString(apiVersionId));
            try (ResultSet resultSet = preparedStatement.executeQuery()) {
                while (resultSet.next()) {
                    Map<String, Object> map = new HashMap<>();
                    String id = resultSet.getString("endpoint_id");
                    String endpoint = resultSet.getString("endpoint");
                    map.put("id", id);
                    map.put("label", endpoint);
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
    public void createApiVersion(Connection conn, Map<String, Object> event) throws SQLException, Exception {
        // Use UPSERT: INSERT ON CONFLICT DO UPDATE
        // This handles:
        // 1. First time insert (no conflict).
        // 2. Re-creation (conflict on host_id, api_version_id) -> UPDATE the existing soft-deleted row (setting active=TRUE and new version).

        final String sqlApiVersion =
                """
                INSERT INTO api_version_t (
                    host_id,
                    api_version_id,
                    api_id,
                    api_version,
                    api_type,
                    service_id,
                    api_version_desc,
                    spec_link,
                    spec,
                    update_user,
                    update_ts,
                    aggregate_version,
                    active
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, TRUE)
                ON CONFLICT (host_id, api_version_id) DO UPDATE
                SET api_id = EXCLUDED.api_id,
                    api_version = EXCLUDED.api_version,
                    api_type = EXCLUDED.api_type,
                    service_id = EXCLUDED.service_id,
                    api_version_desc = EXCLUDED.api_version_desc,
                    spec_link = EXCLUDED.spec_link,
                    spec = EXCLUDED.spec,
                    update_user = EXCLUDED.update_user,
                    update_ts = EXCLUDED.update_ts,
                    aggregate_version = EXCLUDED.aggregate_version,
                    active = TRUE
                -- OCC/IDM: Only update if the incoming event is newer
                WHERE api_version_t.aggregate_version < EXCLUDED.aggregate_version
                AND api_version_t.active = FALSE
                """;

        final String sqlEndpoint =
                """
                INSERT INTO api_endpoint_t (
                    host_id,
                    endpoint_id,
                    api_version_id,
                    endpoint,
                    http_method,
                    endpoint_path,
                    endpoint_name,
                    endpoint_desc,
                    update_user,
                    update_ts,
                    active
                ) VALUES (?,? ,?, ?, ?,  ?, ?, ?, ?, ?, TRUE)
                ON CONFLICT (host_id, endpoint_id) DO UPDATE
                SET api_version_id = EXCLUDED.api_version_id,
                    endpoint = EXCLUDED.endpoint,
                    http_method = EXCLUDED.http_method,
                    endpoint_path = EXCLUDED.endpoint,
                    endpoint_name = EXCLUDED.endpoint_name,
                    endpoint_desc = EXCLUDED.endpoint_desc,
                    update_user = EXCLUDED.update_user,
                    update_ts = EXCLUDED.update_ts,
                    active = TRUE
                    -- OCC/IDM: Only update if the endpoint is soft-deleted.
                    WHERE api_endpoint_t.active = FALSE
                """;

        final String sqlScope =
                """
                INSERT INTO api_endpoint_scope_t (
                    host_id,
                    endpoint_id,
                    scope,
                    scope_desc,
                    update_user,
                    update_ts,
                    active
                ) VALUES (?, ?, ?, ?, ?,  ?, TRUE)
                ON CONFLICT (host_id, endpoint_id, scope) DO UPDATE
                SET scope_desc = EXCLUDED.scope_desc,
                    update_user = EXCLUDED.update_user,
                    update_ts = EXCLUDED.update_ts,
                    active = TRUE
                    -- OCC/IDM: Only update if the endpoint scope is soft-deleted.
                    WHERE api_endpoint_scope_t.active = FALSE
                """;

        Map<String, Object> map = SqlUtil.extractEventData(event);

        String hostId = (String)map.get("hostId");
        String apiVersionId = (String)map.get("apiVersionId");
        long newAggregateVersion = SqlUtil.getNewAggregateVersion(event);

        try (PreparedStatement statement = conn.prepareStatement(sqlApiVersion)) {
            // INSERT values (12 placeholders + active=TRUE in SQL, total 12 dynamic values)
            int i = 1;
            // 1: host_id
            statement.setObject(i++, UUID.fromString(hostId));
            // 2: api_version_id
            statement.setObject(i++, UUID.fromString(apiVersionId));
            // 3: api_id
            statement.setString(i++, (String)map.get("apiId"));
            // 4: api_version
            statement.setString(i++, (String)map.get("apiVersion"));
            // 5: api_type
            statement.setString(i++, (String)map.get("apiType"));
            // 6: service_id
            statement.setString(i++, (String)map.get("serviceId"));

            // 7: api_version_desc
            if (map.containsKey("apiVersionDesc")) {
                statement.setString(i++, (String)map.get("apiVersionDesc"));
            } else {
                statement.setNull(i++, Types.VARCHAR);
            }

            // 8: spec_link
            if (map.containsKey("specLink")) {
                statement.setString(i++, (String)map.get("specLink"));
            } else {
                statement.setNull(i++, Types.VARCHAR);
            }

            // 9: spec
            if (map.containsKey("spec")) {
                statement.setString(i++, (String)map.get("spec"));
            } else {
                statement.setNull(i++, Types.CLOB); // Use appropriate large object type if TEXT is large, otherwise VARCHAR/CLOB/TEXT type
            }

            // 10: update_user
            statement.setString(i++, (String)event.get(Constants.USER));
            // 11: update_ts
            statement.setObject(i++, OffsetDateTime.parse((String)event.get(CloudEventV1.TIME)));
            // 12: aggregate_version
            statement.setLong(i++, newAggregateVersion);

            int count = statement.executeUpdate();
            if (count == 0) {
                // count=0 means the ON CONFLICT clause was hit, BUT the WHERE clause (aggregate_version < EXCLUDED.aggregate_version) failed.
                // This is the desired idempotent/out-of-order protection behavior. Log and ignore.
                logger.warn("Creation/Reactivation skipped for hostId {} apiVersionId {} aggregateVersion {}. A newer or same version already exists.", hostId, apiVersionId, newAggregateVersion);
            }

            List<Map<String, Object>> endpoints = (List<Map<String, Object>>) map.get("endpoints");
            if (endpoints != null && !endpoints.isEmpty()) {
                // insert endpoints
                for (Map<String, Object> endpoint : endpoints) {
                    try (PreparedStatement statementInsert = conn.prepareStatement(sqlEndpoint)) {
                        statementInsert.setObject(1, UUID.fromString((String) map.get("hostId")));
                        statementInsert.setObject(2, UUID.fromString((String) endpoint.get("endpointId")));
                        statementInsert.setObject(3, UUID.fromString(apiVersionId));
                        statementInsert.setString(4, (String) endpoint.get("endpoint"));

                        if (endpoint.get("httpMethod") == null)
                            statementInsert.setNull(5, NULL);
                        else
                            statementInsert.setString(5, ((String) endpoint.get("httpMethod")).toLowerCase().trim());

                        if (endpoint.get("endpointPath") == null)
                            statementInsert.setNull(6, NULL);
                        else
                            statementInsert.setString(6, (String) endpoint.get("endpointPath"));

                        if (endpoint.get("endpointName") == null)
                            statementInsert.setNull(7, NULL);
                        else
                            statementInsert.setString(7, (String) endpoint.get("endpointName"));

                        if (endpoint.get("endpointDesc") == null)
                            statementInsert.setNull(8, NULL);
                        else
                            statementInsert.setString(8, (String) endpoint.get("endpointDesc"));

                        statementInsert.setString(9, (String) event.get(Constants.USER));
                        statementInsert.setObject(10, OffsetDateTime.parse((String) event.get(CloudEventV1.TIME)));
                        statementInsert.executeUpdate();
                    }
                    // insert scopes
                    List<String> scopes = (List<String>) endpoint.get("scopes");
                    if (scopes != null && !scopes.isEmpty()) {
                        for (String scope : scopes) {
                            String[] scopeDesc = scope.split(":");
                            try (PreparedStatement statementScope = conn.prepareStatement(sqlScope)) {
                                statementScope.setObject(1, UUID.fromString((String) map.get("hostId")));
                                statementScope.setObject(2, UUID.fromString((String) endpoint.get("endpointId")));
                                statementScope.setString(3, scopeDesc[0]);
                                if (scopeDesc.length == 1)
                                    statementScope.setNull(4, NULL);
                                else
                                    statementScope.setString(4, scopeDesc[1]);
                                statementScope.setString(5, (String) event.get(Constants.USER));
                                statementScope.setObject(6, OffsetDateTime.parse((String) event.get(CloudEventV1.TIME)));
                                statementScope.executeUpdate();
                            }
                        }
                    }
                }
            }
        } catch (SQLException e) {
            logger.error("SQLException during createServiceVersion for hostId {} apiVersionId {} aggregateVersion {}: {}", hostId, apiVersionId, newAggregateVersion, e.getMessage(), e);
            throw e;
        } catch (Exception e) {
            logger.error("Exception during createServiceVersion for hostId {} apiVersionId {} aggregateVersion {}: {}", hostId, apiVersionId, newAggregateVersion, e.getMessage(), e);
            throw e;
        }
    }

    @Override
    public void updateApiVersion(Connection conn, Map<String, Object> event) throws SQLException, Exception {
        // We attempt to update the record IF the incoming event's aggregate_version is greater than the current projection's version.
        // This enforces Idempotence (IDM) and Optimistic Concurrency Control (OCC) by ensuring version monotonicity.
        // We explicitly set active = TRUE as an UPDATE event implies the service version should be active.
        final String sqlApiVersion =
                """
                UPDATE api_version_t
                SET api_id = ?,
                    api_version = ?,
                    api_type = ?,
                    service_id = ?,
                    api_version_desc = ?,
                    spec_link = ?,
                    spec = ?,
                    update_user = ?,
                    update_ts = ?,
                    aggregate_version = ?,
                    active = TRUE
                WHERE host_id = ?
                  AND api_version_id = ?
                  AND aggregate_version < ?
                """; // <<< CRITICAL: Added aggregate_version < ? to enforce monotonicity (OCC/IDM)
        final String sqlDeactivateEndpoint =
                """
                UPDATE api_endpoint_t
                SET active = FALSE,
                update_user = ?,
                update_ts = ?
                WHERE host_id = ?
                AND api_version_id = ?
                """;
        final String sqlDeactivateScope =
                """
                UPDATE api_endpoint_scope_t
                SET active = FALSE,
                update_user = ?,
                update_ts = ?
                WHERE host_id = ?
                AND endpoint_id IN
                (
                    SELECT endpoint_id
                    FROM api_endpoint_t
                    WHERE host_id = ?
                    AND api_version_id = ?
                )
                """;

        final String sqlEndpoint =
                """
                INSERT INTO api_endpoint_t (
                    host_id,
                    endpoint_id,
                    api_version_id,
                    endpoint,
                    http_method,
                    endpoint_path,
                    endpoint_name,
                    endpoint_desc,
                    update_user,
                    update_ts,
                    active
                ) VALUES (?,? ,?, ?, ?,  ?, ?, ?, ?, ?, TRUE)
                ON CONFLICT (host_id, endpoint_id) DO UPDATE
                SET api_version_id = EXCLUDED.api_version_id,
                    endpoint = EXCLUDED.endpoint,
                    http_method = EXCLUDED.http_method,
                    endpoint_path = EXCLUDED.endpoint,
                    endpoint_name = EXCLUDED.endpoint_name,
                    endpoint_desc = EXCLUDED.endpoint_desc,
                    update_user = EXCLUDED.update_user,
                    update_ts = EXCLUDED.update_ts,
                    active = TRUE
                    -- OCC/IDM: Only update if the endpoint is soft-deleted.
                    WHERE api_endpoint_t.active = FALSE
                """;

        final String sqlScope =
                """
                INSERT INTO api_endpoint_scope_t (
                    host_id,
                    endpoint_id,
                    scope,
                    scope_desc,
                    update_user,
                    update_ts,
                    active
                ) VALUES (?, ?, ?, ?, ?,  ?, TRUE)
                ON CONFLICT (host_id, endpoint_id, scope) DO UPDATE
                SET scope_desc = EXCLUDED.scope_desc,
                    update_user = EXCLUDED.update_user,
                    update_ts = EXCLUDED.update_ts,
                    active = TRUE
                    -- OCC/IDM: Only update if the endpoint scope is soft-deleted.
                    WHERE api_endpoint_scope_t.active = FALSE
                """;

        // Note: Assuming SqlUtil.extractEventData(event) is the correct utility based on other methods.
        Map<String, Object> map = SqlUtil.extractEventData(event);

        String hostId = (String)map.get("hostId");
        String apiVersionId = (String)map.get("apiVersionId");
        long newAggregateVersion = SqlUtil.getNewAggregateVersion(event);

        try (PreparedStatement statement = conn.prepareStatement(sqlApiVersion)) {
            // SET values (10 dynamic values + active = TRUE in SQL)
            int i = 1;
            // 1: api_id
            statement.setString(i++, (String)map.get("apiId"));
            // 2: api_version
            statement.setString(i++, (String)map.get("apiVersion"));
            // 3: api_type
            statement.setString(i++, (String)map.get("apiType"));
            // 4: service_id
            statement.setString(i++, (String)map.get("serviceId"));

            // 5: api_version_desc
            if (map.containsKey("apiVersionDesc")) {
                statement.setString(i++, (String)map.get("apiVersionDesc"));
            } else {
                statement.setNull(i++, Types.VARCHAR);
            }

            // 6: spec_link
            if (map.containsKey("specLink")) {
                statement.setString(i++, (String)map.get("specLink"));
            } else {
                statement.setNull(i++, Types.VARCHAR);
            }

            // 7: spec
            if (map.containsKey("spec")) {
                statement.setString(i++, (String)map.get("spec"));
            } else {
                statement.setNull(i++, Types.CLOB); // Use appropriate large object type
            }

            // 8: update_user
            statement.setString(i++, (String)event.get(Constants.USER));
            // 9: update_ts
            statement.setObject(i++, OffsetDateTime.parse((String)event.get(CloudEventV1.TIME)));
            // 10: aggregate_version
            statement.setLong(i++, newAggregateVersion);


            // WHERE conditions (3 placeholders)
            // 11: host_id
            statement.setObject(i++, UUID.fromString(hostId));
            // 12: api_version_id
            statement.setObject(i++, UUID.fromString(apiVersionId));
            // 13: aggregate_version < ? (new version for OCC/IDM check)
            statement.setLong(i++, newAggregateVersion);

            int count = statement.executeUpdate();
            if (count == 0) {
                // If 0 rows updated, it means the record was either not found
                // OR aggregate_version >= newAggregateVersion (OCC/IDM check failed).
                // We IGNORE the failure and log a warning, as this is the desired idempotent/monotonic behavior.
                logger.warn("Update skipped for hostId {} apiVersionId {} aggregateVersion {}. Record not found or a newer/same version already exists.", hostId, apiVersionId, newAggregateVersion);
            }

            List<Map<String, Object>> endpoints = (List<Map<String, Object>>) map.get("endpoints");
            if (endpoints != null && !endpoints.isEmpty()) {
                // deactivate endpoints for the api version.
                try (PreparedStatement statementDelete = conn.prepareStatement(sqlDeactivateEndpoint)) {
                    statementDelete.setString(1, (String) event.get(Constants.USER));
                    statementDelete.setObject(2, OffsetDateTime.parse((String) event.get(CloudEventV1.TIME)));
                    statementDelete.setObject(3, UUID.fromString(hostId));
                    statementDelete.setObject(4, UUID.fromString(apiVersionId));
                    statementDelete.executeUpdate();
                }
                // deactivate scopes for the api version.
                try (PreparedStatement statementDelete = conn.prepareStatement(sqlDeactivateScope)) {
                    statementDelete.setString(1, (String) event.get(Constants.USER));
                    statementDelete.setObject(2, OffsetDateTime.parse((String) event.get(CloudEventV1.TIME)));
                    statementDelete.setObject(3, UUID.fromString((String) map.get("hostId")));
                    statementDelete.setObject(4, UUID.fromString((String) map.get("hostId")));
                    statementDelete.setObject(5, UUID.fromString(apiVersionId));
                    statementDelete.executeUpdate();
                }

                // insert or update endpoints
                for (Map<String, Object> endpoint : endpoints) {
                    try (PreparedStatement statementInsert = conn.prepareStatement(sqlEndpoint)) {
                        statementInsert.setObject(1, UUID.fromString(hostId));
                        statementInsert.setObject(2, UUID.fromString((String) endpoint.get("endpointId")));
                        statementInsert.setObject(3, UUID.fromString(apiVersionId));
                        statementInsert.setString(4, (String) endpoint.get("endpoint"));

                        if (endpoint.get("httpMethod") == null)
                            statementInsert.setNull(5, NULL);
                        else
                            statementInsert.setString(5, ((String) endpoint.get("httpMethod")).toLowerCase().trim());

                        if (endpoint.get("endpointPath") == null)
                            statementInsert.setNull(6, NULL);
                        else
                            statementInsert.setString(6, (String) endpoint.get("endpointPath"));

                        if (endpoint.get("endpointName") == null)
                            statementInsert.setNull(7, NULL);
                        else
                            statementInsert.setString(7, (String) endpoint.get("endpointName"));

                        if (endpoint.get("endpointDesc") == null)
                            statementInsert.setNull(8, NULL);
                        else
                            statementInsert.setString(8, (String) endpoint.get("endpointDesc"));

                        statementInsert.setString(9, (String) event.get(Constants.USER));
                        statementInsert.setObject(10, OffsetDateTime.parse((String) event.get(CloudEventV1.TIME)));
                        statementInsert.executeUpdate();
                    }
                    // insert or update scopes
                    List<String> scopes = (List<String>) endpoint.get("scopes");
                    if (scopes != null && !scopes.isEmpty()) {
                        for (String scope : scopes) {
                            String[] scopeDesc = scope.split(":");
                            try (PreparedStatement statementScope = conn.prepareStatement(sqlScope)) {
                                statementScope.setObject(1, UUID.fromString((String) map.get("hostId")));
                                statementScope.setObject(2, UUID.fromString((String) endpoint.get("endpointId")));
                                statementScope.setString(3, scopeDesc[0]);
                                if (scopeDesc.length == 1)
                                    statementScope.setNull(4, NULL);
                                else
                                    statementScope.setString(4, scopeDesc[1]);
                                statementScope.setString(5, (String) event.get(Constants.USER));
                                statementScope.setObject(6, OffsetDateTime.parse((String) event.get(CloudEventV1.TIME)));
                                statementScope.executeUpdate();
                            }
                        }
                    }
                }
            }
        } catch (SQLException e) {
            logger.error("SQLException during updateServiceVersion for hostId {} apiVersionId {} aggregateVersion {}: {}", hostId, apiVersionId, newAggregateVersion, e.getMessage(), e);
            throw e;
        } catch (Exception e) {
            logger.error("Exception during updateServiceVersion for hostId {} apiVersionId {} aggregateVersion {}: {}", hostId, apiVersionId, newAggregateVersion, e.getMessage(), e);
            throw e;
        }
    }

    @Override
    public void deleteApiVersion(Connection conn, Map<String, Object> event) throws SQLException, Exception {
        // Use UPDATE to implement Soft Delete (setting active = FALSE).
        // OCC/IDM is enforced by checking aggregate_version < newAggregateVersion.
        final String sqlDeactivateApiVersion =
                """
                UPDATE api_version_t
                SET active = FALSE,
                    update_user = ?,
                    update_ts = ?,
                    aggregate_version = ?
                WHERE host_id = ?
                  AND api_version_id = ?
                  AND aggregate_version < ?
                """; // <<< CRITICAL: Added aggregate_version < ? to enforce monotonicity (OCC/IDM)
        final String sqlDeactivateEndpoint =
                """
                UPDATE api_endpoint_t
                SET active = FALSE,
                update_user = ?,
                update_ts = ?
                WHERE host_id = ?
                AND api_version_id = ?
                """;
        final String sqlDeactivateScope =
                """
                UPDATE api_endpoint_scope_t
                SET active = FALSE,
                update_user = ?,
                update_ts = ?
                WHERE host_id = ?
                AND endpoint_id IN
                (
                    SELECT endpoint_id
                    FROM api_endpoint_t
                    WHERE host_id = ?
                    AND api_version_id = ?
                )
                """;

        // Note: Assuming SqlUtil.extractEventData(event) is the correct utility based on other methods.
        Map<String, Object> map = SqlUtil.extractEventData(event);

        String hostId = (String)map.get("hostId");
        String apiVersionId = (String)map.get("apiVersionId");
        // A delete event represents a state change, so it should have a new, incremented version.
        long newAggregateVersion = SqlUtil.getNewAggregateVersion(event);

        try (PreparedStatement statement = conn.prepareStatement(sqlDeactivateApiVersion)) {
            // SET values (3 placeholders)
            // 1: update_user
            statement.setString(1, (String)event.get(Constants.USER));
            // 2: update_ts
            statement.setObject(2, OffsetDateTime.parse((String)event.get(CloudEventV1.TIME)));
            // 3: aggregate_version (the new version)
            statement.setLong(3, newAggregateVersion);

            // WHERE conditions (3 placeholders)
            // 4: host_id
            statement.setObject(4, UUID.fromString(hostId));
            // 5: api_version_id
            statement.setObject(5, UUID.fromString(apiVersionId));
            // 6: aggregate_version < ? (new version for OCC/IDM check)
            statement.setLong(6, newAggregateVersion);

            int count = statement.executeUpdate();
            if (count == 0) {
                // If 0 rows updated, it means:
                // 1. The record was not found (already deleted or never existed).
                // 2. The OCC/IDM check failed (aggregate_version >= newAggregateVersion).
                // We IGNORE the failure and log a warning, as this is the desired idempotent/monotonic behavior.
                logger.warn("Soft delete skipped for hostId {} apiVersionId {} aggregateVersion {}. Record not found or a newer/same version already exists.", hostId, apiVersionId, newAggregateVersion);
            }
            // deactivate endpoints for the api version.
            try (PreparedStatement statementDelete = conn.prepareStatement(sqlDeactivateEndpoint)) {
                statementDelete.setString(1, (String) event.get(Constants.USER));
                statementDelete.setObject(2, OffsetDateTime.parse((String) event.get(CloudEventV1.TIME)));
                statementDelete.setObject(3, UUID.fromString((String) map.get("hostId")));
                statementDelete.setObject(4, UUID.fromString(apiVersionId));
                statementDelete.executeUpdate();
            }
            // deactivate scopes for the api version.
            try (PreparedStatement statementDelete = conn.prepareStatement(sqlDeactivateScope)) {
                statementDelete.setString(1, (String) event.get(Constants.USER));
                statementDelete.setObject(2, OffsetDateTime.parse((String) event.get(CloudEventV1.TIME)));
                statementDelete.setObject(3, UUID.fromString((String) map.get("hostId")));
                statementDelete.setObject(4, UUID.fromString((String) map.get("hostId")));
                statementDelete.setObject(5, UUID.fromString(apiVersionId));
                statementDelete.executeUpdate();
            }
        } catch (SQLException e) {
            logger.error("SQLException during deleteServiceVersion for hostId {} apiVersionId {} aggregateVersion {}: {}", hostId, apiVersionId, newAggregateVersion, e.getMessage(), e);
            throw e;
        } catch (Exception e) {
            logger.error("Exception during deleteServiceVersion for hostId {} apiVersionId {} aggregateVersion {}: {}", hostId, apiVersionId, newAggregateVersion, e.getMessage(), e);
            throw e;
        }
    }

    @Override
    public Result<String> getApiVersionById(String hostId, String apiVersionId) {
        Result<String> result;
        String sql =
            """
            SELECT host_id, api_version_id, api_id, api_version, api_type,
            service_id, api_version_desc, spec_link, spec,
            update_user, update_ts, aggregate_version, active
            FROM api_version_t
            WHERE host_id = ? AND api_version_id = ?
            """;
        try (final Connection conn = ds.getConnection()) {
            Map<String, Object> map = new HashMap<>();
            try (PreparedStatement statement = conn.prepareStatement(sql)) {
                statement.setObject(1, UUID.fromString(hostId));
                statement.setObject(2, UUID.fromString(apiVersionId));
                try (ResultSet resultSet = statement.executeQuery()) {
                    if (resultSet.next()) {
                        map.put("hostId", resultSet.getString("host_id"));
                        map.put("apiVersionId", resultSet.getObject("api_version_id", UUID.class));
                        map.put("apiId", resultSet.getString("api_id"));
                        map.put("apiVersion", resultSet.getString("api_version"));
                        map.put("apiType", resultSet.getString("api_type"));
                        map.put("serviceId", resultSet.getString("service_id"));
                        map.put("apiVersionDesc", resultSet.getString("api_version_desc"));
                        map.put("specLink", resultSet.getString("spec_link"));
                        map.put("spec", resultSet.getString("spec"));
                        map.put("updateUser", resultSet.getString("update_user"));
                        map.put("updateTs", resultSet.getObject("update_ts") != null ? resultSet.getObject("update_ts", OffsetDateTime.class) : null);
                        map.put("aggregateVersion", resultSet.getLong("aggregate_version"));
                        map.put("active", resultSet.getBoolean("active"));
                    }
                }
            }
            if (map.isEmpty())
                result = Failure.of(new Status(OBJECT_NOT_FOUND, "getApiVersionById", apiVersionId));
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
    public String queryApiVersionId(String hostId, String apiId, String apiVersion) {
        Result<String> result;
        String sql =
                """
                SELECT api_version_id
                FROM api_version_t
                WHERE host_id = ? AND api_id = ? AND api_version = ?
                """;
        String apiVersionId = null;
        try (final Connection conn = ds.getConnection()) {
            try (PreparedStatement statement = conn.prepareStatement(sql)) {
                statement.setObject(1, UUID.fromString(hostId));
                statement.setString(2, apiId);
                statement.setString(3, apiVersion);
                try (ResultSet resultSet = statement.executeQuery()) {
                    if (resultSet.next()) {
                        apiVersionId = resultSet.getObject("api_version_id", UUID.class).toString();
                    }
                }
            }
        } catch (SQLException e) {
            logger.error("SQLException:", e);
        } catch (Exception e) {
            logger.error("Exception:", e);
        }
        return apiVersionId;
    }

    @Override
    public Result<String> queryApiVersion(String hostId, String apiId) {
        Result<String> result = null;
        String sql =
                """
                SELECT host_id, api_version_id, api_id, api_version, api_type,
                service_id, api_version_desc, spec_link, spec,
                update_user, update_ts, aggregate_version, active
                FROM api_version_t
                WHERE host_id = ? AND api_id = ?
                ORDER BY api_version
                """;
        List<Map<String, Object>> serviceVersions = new ArrayList<>();

        try (Connection connection = ds.getConnection();
             PreparedStatement preparedStatement = connection.prepareStatement(sql)) {
            preparedStatement.setObject(1, UUID.fromString(hostId));
            preparedStatement.setString(2, apiId);
            try (ResultSet resultSet = preparedStatement.executeQuery()) {
                while (resultSet.next()) {
                    Map<String, Object> map = new HashMap<>();
                    map.put("hostId", resultSet.getObject("host_id", UUID.class));
                    map.put("apiVersionId", resultSet.getObject("api_version_id", UUID.class));
                    map.put("apiId", resultSet.getString("api_id"));
                    map.put("apiVersion", resultSet.getString("api_version"));
                    map.put("apiType", resultSet.getString("api_type"));
                    map.put("serviceId", resultSet.getString("service_id"));
                    map.put("apiVersionDesc", resultSet.getString("api_version_desc"));
                    map.put("specLink", resultSet.getString("spec_link"));
                    map.put("spec", resultSet.getString("spec"));
                    map.put("updateUser", resultSet.getString("update_user"));
                    map.put("updateTs", resultSet.getObject("update_ts", OffsetDateTime.class));
                    map.put("aggregateVersion", resultSet.getLong("aggregate_version"));
                    map.put("active", resultSet.getBoolean("active"));
                    serviceVersions.add(map);
                }
            }
            result = Success.of(JsonMapper.toJson(serviceVersions));
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
    public void updateApiVersionSpec(Connection conn, Map<String, Object> event) throws SQLException, Exception {
        final String sqlUpdateApiVersion =
                """
                UPDATE api_version_t
                SET spec = ?,
                    update_user = ?,
                    update_ts = ?,
                    aggregate_version = ?,
                    active = TRUE
                WHERE host_id = ?
                  AND api_version_id = ?
                  AND aggregate_version < ?
                """; // <<< CRITICAL: Added aggregate_version < ? to enforce monotonicity (OCC/IDM)
        final String sqlDeactivateEndpoint =
                """
                UPDATE api_endpoint_t
                SET active = FALSE,
                update_user = ?,
                update_ts = ?
                WHERE host_id = ?
                AND api_version_id = ?
                """;
        final String sqlDeactivateScope =
                """
                UPDATE api_endpoint_scope_t
                SET active = FALSE,
                update_user = ?,
                update_ts = ?
                WHERE host_id = ?
                AND endpoint_id IN
                (
                    SELECT endpoint_id
                    FROM api_endpoint_t
                    WHERE host_id = ?
                    AND api_version_id = ?
                )
                """;

        final String sqlEndpoint =
                """
                INSERT INTO api_endpoint_t (
                    host_id,
                    endpoint_id,
                    api_version_id,
                    endpoint,
                    http_method,
                    endpoint_path,
                    endpoint_name,
                    endpoint_desc,
                    update_user,
                    update_ts,
                    active
                ) VALUES (?,? ,?, ?, ?,  ?, ?, ?, ?, ?, TRUE)
                ON CONFLICT (host_id, endpoint_id) DO UPDATE
                SET api_version_id = EXCLUDED.api_version_id,
                    endpoint = EXCLUDED.endpoint,
                    http_method = EXCLUDED.http_method,
                    endpoint_path = EXCLUDED.endpoint,
                    endpoint_name = EXCLUDED.endpoint_name,
                    endpoint_desc = EXCLUDED.endpoint_desc,
                    update_user = EXCLUDED.update_user,
                    update_ts = EXCLUDED.update_ts,
                    active = TRUE
                    -- OCC/IDM: Only update if the endpoint is soft-deleted.
                    WHERE api_endpoint_t.active = FALSE
                """;

        final String sqlScope =
                """
                INSERT INTO api_endpoint_scope_t (
                    host_id,
                    endpoint_id,
                    scope,
                    scope_desc,
                    update_user,
                    update_ts,
                    active
                ) VALUES (?, ?, ?, ?, ?,  ?, TRUE)
                ON CONFLICT (host_id, endpoint_id, scope) DO UPDATE
                SET scope_desc = EXCLUDED.scope_desc,
                    update_user = EXCLUDED.update_user,
                    update_ts = EXCLUDED.update_ts,
                    active = TRUE
                    -- OCC/IDM: Only update if the endpoint scope is soft-deleted.
                    WHERE api_endpoint_scope_t.active = FALSE
                """;

        // Note: Assuming SqlUtil.extractEventData(event) is the correct utility based on other methods.
        Map<String, Object> map = SqlUtil.extractEventData(event);

        String hostId = (String)map.get("hostId");
        String apiVersionId = (String)map.get("apiVersionId");
        long newAggregateVersion = SqlUtil.getNewAggregateVersion(event);

        try (PreparedStatement statement = conn.prepareStatement(sqlUpdateApiVersion)) {
            // SET values (10 dynamic values + active = TRUE in SQL)
            int i = 1;
            if (map.containsKey("spec")) {
                statement.setString(i++, (String)map.get("spec"));
            } else {
                statement.setNull(i++, Types.CLOB); // Use appropriate large object type
            }
            statement.setString(i++, (String)event.get(Constants.USER));
            statement.setObject(i++, OffsetDateTime.parse((String)event.get(CloudEventV1.TIME)));
            statement.setLong(i++, newAggregateVersion);
            statement.setObject(i++, UUID.fromString(hostId));
            statement.setObject(i++, UUID.fromString(apiVersionId));
            statement.setLong(i++, newAggregateVersion);

            int count = statement.executeUpdate();
            if (count == 0) {
                // If 0 rows updated, it means the record was either not found
                // OR aggregate_version >= newAggregateVersion (OCC/IDM check failed).
                // We IGNORE the failure and log a warning, as this is the desired idempotent/monotonic behavior.
                logger.warn("Update skipped for hostId {} apiVersionId {} aggregateVersion {}. Record not found or a newer/same version already exists.", hostId, apiVersionId, newAggregateVersion);
            }

            List<Map<String, Object>> endpoints = (List<Map<String, Object>>) map.get("endpoints");
            if (endpoints != null && !endpoints.isEmpty()) {
                // deactivate endpoints for the api version.
                try (PreparedStatement statementDelete = conn.prepareStatement(sqlDeactivateEndpoint)) {
                    statementDelete.setString(1, (String) event.get(Constants.USER));
                    statementDelete.setObject(2, OffsetDateTime.parse((String) event.get(CloudEventV1.TIME)));
                    statementDelete.setObject(3, UUID.fromString((String) map.get("hostId")));
                    statementDelete.setObject(4, UUID.fromString(apiVersionId));
                    statementDelete.executeUpdate();
                }
                // deactivate scopes for the api version.
                try (PreparedStatement statementDelete = conn.prepareStatement(sqlDeactivateScope)) {
                    statementDelete.setString(1, (String) event.get(Constants.USER));
                    statementDelete.setObject(2, OffsetDateTime.parse((String) event.get(CloudEventV1.TIME)));
                    statementDelete.setObject(3, UUID.fromString((String) map.get("hostId")));
                    statementDelete.setObject(4, UUID.fromString((String) map.get("hostId")));
                    statementDelete.setObject(5, UUID.fromString(apiVersionId));
                    statementDelete.executeUpdate();
                }

                // insert or update endpoints
                for (Map<String, Object> endpoint : endpoints) {
                    try (PreparedStatement statementInsert = conn.prepareStatement(sqlEndpoint)) {
                        statementInsert.setObject(1, UUID.fromString((String) map.get("hostId")));
                        statementInsert.setObject(2, UUID.fromString((String) endpoint.get("endpointId")));
                        statementInsert.setObject(3, UUID.fromString(apiVersionId));
                        statementInsert.setString(4, (String) endpoint.get("endpoint"));

                        if (endpoint.get("httpMethod") == null)
                            statementInsert.setNull(5, NULL);
                        else
                            statementInsert.setString(5, ((String) endpoint.get("httpMethod")).toLowerCase().trim());

                        if (endpoint.get("endpointPath") == null)
                            statementInsert.setNull(6, NULL);
                        else
                            statementInsert.setString(6, (String) endpoint.get("endpointPath"));

                        if (endpoint.get("endpointName") == null)
                            statementInsert.setNull(7, NULL);
                        else
                            statementInsert.setString(7, (String) endpoint.get("endpointName"));

                        if (endpoint.get("endpointDesc") == null)
                            statementInsert.setNull(8, NULL);
                        else
                            statementInsert.setString(8, (String) endpoint.get("endpointDesc"));

                        statementInsert.setString(9, (String) event.get(Constants.USER));
                        statementInsert.setObject(10, OffsetDateTime.parse((String) event.get(CloudEventV1.TIME)));
                        statementInsert.executeUpdate();
                    }
                    // insert or update scopes
                    List<String> scopes = (List<String>) endpoint.get("scopes");
                    if (scopes != null && !scopes.isEmpty()) {
                        for (String scope : scopes) {
                            String[] scopeDesc = scope.split(":");
                            try (PreparedStatement statementScope = conn.prepareStatement(sqlScope)) {
                                statementScope.setObject(1, UUID.fromString((String) map.get("hostId")));
                                statementScope.setObject(2, UUID.fromString((String) endpoint.get("endpointId")));
                                statementScope.setString(3, scopeDesc[0]);
                                if (scopeDesc.length == 1)
                                    statementScope.setNull(4, NULL);
                                else
                                    statementScope.setString(4, scopeDesc[1]);
                                statementScope.setString(5, (String) event.get(Constants.USER));
                                statementScope.setObject(6, OffsetDateTime.parse((String) event.get(CloudEventV1.TIME)));
                                statementScope.executeUpdate();
                            }
                        }
                    }
                }
            }

        } catch (SQLException e) {
            logger.error("SQLException during updateServiceVersion for hostId {} apiVersionId {} aggregateVersion {}: {}", hostId, apiVersionId, newAggregateVersion, e.getMessage(), e);
            throw e;
        } catch (Exception e) {
            logger.error("Exception during updateServiceVersion for hostId {} apiVersionId {} aggregateVersion {}: {}", hostId, apiVersionId, newAggregateVersion, e.getMessage(), e);
            throw e;
        }
    }

    @Override
    public Map<String, Object> getEndpointIdMap(String hostId, String apiVersionId) {
        String sql = "SELECT endpoint_id, endpoint FROM api_endpoint_t WHERE host_id = ? AND api_version_id = ?";
        Map<String, Object> map = new HashMap<>();
        try (Connection connection = ds.getConnection();
            PreparedStatement preparedStatement = connection.prepareStatement(sql)) {
            preparedStatement.setObject(1, UUID.fromString(hostId));
            preparedStatement.setObject(2, UUID.fromString(apiVersionId));
            try (ResultSet resultSet = preparedStatement.executeQuery()) {
                while (resultSet.next()) {
                    String endpointId = resultSet.getString("endpoint_id");
                    String endpoint = resultSet.getString("endpoint");
                    map.put(endpoint, endpointId);
                }
            }
        } catch (SQLException e) {
            logger.error("SQLException:", e);
        } catch (Exception e) {
            logger.error("Exception:", e);
        }
        return map;
    }

    @Override
    public Result<String> queryApiEndpoint(int offset, int limit, String filtersJson, String globalFilter, String sortingJson, String hostId) {
        Result<String> result = null;
        final Map<String, String> columnMap = new HashMap<>(Map.of(
                "hostId", "e.host_id",
                "endpointId", "e.endpoint_id",
                "apiVersionId", "e.api_version_id",
                "apiId", "v.api_id",
                "apiVersion", "v.api_version",
                "endpoint", "e.endpoint",
                "httpMethod", "e.http_method",
                "endpointPath", "e.endpoint_path",
                "endpointDesc", "e.endpoint_desc"
        ));
        columnMap.put("active", "e.active");
        columnMap.put("updateUser", "e.update_user");
        columnMap.put("updateTs", "e.updateTs");

        List<Map<String, Object>> filters = parseJsonList(filtersJson);
        List<Map<String, Object>> sorting = parseJsonList(sortingJson);

        String s =
            """
                SELECT COUNT(*) OVER () AS total,
                e.host_id, e.endpoint_id, e.api_version_id, v.api_id,
                v.api_version, e.endpoint, e.http_method, e.endpoint_path,
                e.endpoint_desc, e.active, e.update_user, e.update_ts
                FROM api_endpoint_t e
                INNER JOIN api_version_t v ON e.api_version_id = v.api_version_id
                WHERE e.host_id = ?
            """;

        List<Object> parameters = new ArrayList<>();
        parameters.add(UUID.fromString(hostId));

        String[] searchColumns = {"e.endpoint", "e.endpoint_path", "e.endpoint_desc"};
        String sqlBuilder = s + dynamicFilter(Arrays.asList("e.host_id", "e.endpoint_id", "e.api_version_id"), Arrays.asList(searchColumns), filters, columnMap, parameters) +
                globalFilter(globalFilter, searchColumns, parameters) +
                dynamicSorting("e.endpoint", sorting, columnMap) +
                "\nLIMIT ? OFFSET ?";

        parameters.add(limit);
        parameters.add(offset);
        int total = 0;
        List<Map<String, Object>> endpoints = new ArrayList<>();
        try (Connection connection = ds.getConnection();
            PreparedStatement preparedStatement = connection.prepareStatement(sqlBuilder)) {
            populateParameters(preparedStatement, parameters);
            boolean isFirstRow = true;
            try (ResultSet resultSet = preparedStatement.executeQuery()) {
                while (resultSet.next()) {
                    Map<String, Object> map = new HashMap<>();

                    if (isFirstRow) {
                        total = resultSet.getInt("total");
                        isFirstRow = false;
                    }
                    map.put("hostId", resultSet.getObject("host_id", UUID.class));
                    map.put("endpointId", resultSet.getObject("endpoint_id", UUID.class));
                    map.put("apiVersionId", resultSet.getObject("api_version_id", UUID.class));
                    map.put("apiId", resultSet.getString("api_id"));
                    map.put("apiVersion", resultSet.getString("api_version"));
                    map.put("endpoint", resultSet.getString("endpoint"));
                    map.put("httpMethod", resultSet.getString("http_method"));
                    map.put("endpointPath", resultSet.getString("endpoint_path"));
                    map.put("endpointDesc", resultSet.getString("endpoint_desc"));
                    map.put("updateUser", resultSet.getString("update_user"));
                    map.put("updateTs", resultSet.getObject("update_ts", OffsetDateTime.class));
                    map.put("active", resultSet.getBoolean("active"));
                    endpoints.add(map);
                }
            }
            Map<String, Object> resultMap = new HashMap<>();
            resultMap.put("total", total);
            resultMap.put("endpoints", endpoints);
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
    public Result<String> queryApiEndpointScope(String hostId, String endpointId) {
        Result<String> result = null;
        String sql =
                """
                    SELECT s.host_id, s.endpoint_id, e.endpoint, s.scope,
                    s.scope_desc, s.active, s.update_user, s.update_ts
                    FROM api_endpoint_scope_t s
                    INNER JOIN api_endpoint_t e ON e.host_id = s.host_id AND e.endpoint_id = s.endpoint_id
                    WHERE s.host_id = ?
                    AND s.endpoint_id = ?
                    ORDER BY scope
                """;

        List<Map<String, Object>> scopes = new ArrayList<>();

        try (Connection connection = ds.getConnection();
             PreparedStatement preparedStatement = connection.prepareStatement(sql)) {
            preparedStatement.setObject(1, UUID.fromString(hostId));
            preparedStatement.setObject(2, UUID.fromString(endpointId));
            try (ResultSet resultSet = preparedStatement.executeQuery()) {
                while (resultSet.next()) {
                    Map<String, Object> map = new HashMap<>();
                    map.put("hostId", resultSet.getObject("host_id", UUID.class));
                    map.put("endpointId", resultSet.getObject("endpoint_id", UUID.class));
                    map.put("endpoint", resultSet.getString("endpoint"));
                    map.put("scope", resultSet.getString("scope"));
                    map.put("scopeDesc", resultSet.getString("scope_desc"));
                    map.put("active", resultSet.getBoolean("active"));
                    map.put("updateUser", resultSet.getString("update_user"));
                    map.put("updateTs", resultSet.getObject("update_ts", OffsetDateTime.class));
                    scopes.add(map);
                }
            }
            result = Success.of(JsonMapper.toJson(scopes));
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
    public Result<String> queryApiEndpointRule(String hostId, String endpointId) {
        Result<String> result = null;
        String sql =
            """
                SELECT ae.host_id, ae.endpoint_id, av.api_version_id, a.api_id, av.api_version,
                e.endpoint, r.rule_type, ae.rule_id, ae.aggregate_version, ae.active
                FROM api_endpoint_rule_t ae
                INNER JOIN rule_t r ON ae.rule_id = r.rule_id
                INNER JOIN api_endpoint_t e ON ae.endpoint_id = e.endpoint_id
                INNER JOIN api_version_t av ON e.api_version_id = av.api_version_id
                INNER JOIN api_t a ON av.api_id = a.api_id
                WHERE ae.host_id = ?
                AND ae.endpoint_id = ?
                ORDER BY r.rule_type
            """;

        List<Map<String, Object>> rules = new ArrayList<>();

        try (Connection connection = ds.getConnection();
             PreparedStatement preparedStatement = connection.prepareStatement(sql)) {
            preparedStatement.setObject(1, UUID.fromString(hostId));
            preparedStatement.setObject(2, UUID.fromString(endpointId));
            try (ResultSet resultSet = preparedStatement.executeQuery()) {
                while (resultSet.next()) {
                    Map<String, Object> map = new HashMap<>();
                    map.put("hostId", resultSet.getObject("host_id", UUID.class));
                    map.put("endpointId", resultSet.getObject("endpoint_id", UUID.class));
                    map.put("apiVersionId", resultSet.getObject("api_version_id", UUID.class));
                    map.put("apiId", resultSet.getString("api_id"));
                    map.put("apiVersion", resultSet.getString("api_version"));
                    map.put("endpoint", resultSet.getString("endpoint"));
                    map.put("ruleType", resultSet.getString("rule_type"));
                    map.put("ruleId", resultSet.getString("rule_id"));
                    map.put("aggregateVersion", resultSet.getLong("aggregate_version"));
                    map.put("active", resultSet.getBoolean("active"));
                    rules.add(map);
                }
            }
            result = Success.of(JsonMapper.toJson(rules));
        } catch (SQLException e) {
            logger.error("SQLException:", e);
            result = Failure.of(new Status(SQL_EXCEPTION, e.getMessage()));
        } catch (Exception e) {
            logger.error("Exception:", e);
            result = Failure.of(new Status(GENERIC_EXCEPTION, e.getMessage()));
        }
        return result;

    }

    /*
    @Override
    public void createEndpointRule(Connection conn, Map<String, Object> event) throws SQLException, Exception {
        final String insertUser =
                """
                INSERT INTO api_endpoint_rule_t (host_id, endpoint_id, rule_id,
                update_user, update_ts, aggregate_version)
                VALUES (
                ?,
                (SELECT e.endpoint_id
                 FROM api_endpoint_t e
                 JOIN api_version_t v ON e.host_id = v.host_id
                                     AND e.api_version_id = v.api_version_id
                 WHERE e.host_id = ?
                   AND v.api_id = ?
                   AND v.api_version = ?
                   AND e.endpoint = ?
                ),
                ?,
                ?,
                ?
                )
                """;
        Map<String, Object> map = (Map<String, Object>) event.get(PortalConstants.DATA);
        String endpoint = (String) map.get("endpoint");
        String ruleId = (String) map.get("ruleId");
        long newAggregateVersion = SqlUtil.getNewAggregateVersion(event);

        try (PreparedStatement statement = conn.prepareStatement(insertUser)) {
            statement.setObject(1, UUID.fromString((String) event.get(Constants.HOST)));
            statement.setObject(2, UUID.fromString((String) event.get(Constants.HOST)));
            statement.setString(3, (String) map.get("apiId"));
            statement.setString(4, (String) map.get("apiVersion"));
            statement.setString(5, endpoint);
            statement.setString(6, ruleId);
            statement.setString(7, (String) event.get(Constants.USER));
            statement.setObject(8, OffsetDateTime.parse((String) event.get(CloudEventV1.TIME)));
            statement.setLong(9, newAggregateVersion);
            int count = statement.executeUpdate();
            if (count == 0) {
                throw new SQLException("Failed to insert endpoint " + endpoint + " rule " + ruleId + " with aggregate version " + newAggregateVersion + ".");
            }
        } catch (SQLException e) {
            logger.error("SQLException during createEndpointRule for endpoint {} rule {} aggregateVersion {}: {}", endpoint, ruleId, newAggregateVersion, e.getMessage(), e);
            throw e;
        } catch (Exception e) {
            logger.error("Exception during createEndpointRule for endpoint {} rule {} aggregateVersion {}: {}", endpoint, ruleId, newAggregateVersion, e.getMessage(), e);
            throw e;
        }
    }

     */

    @Override
    public void createApiEndpointRule(Connection conn, Map<String, Object> event) throws SQLException, Exception {
        // Use UPSERT: INSERT ON CONFLICT DO UPDATE
        // This handles:
        // 1. First time insert (no conflict).
        // 2. Re-creation (conflict on host_id, endpoint_id, rule_id) -> UPDATE the existing soft-deleted row (setting active=TRUE and new version).

        final String sql =
                """
                INSERT INTO api_endpoint_rule_t (
                    host_id,
                    endpoint_id,
                    rule_id,
                    update_user,
                    update_ts,
                    aggregate_version,
                    active
                ) VALUES (?, ?, ?, ?, ?, ?, TRUE)
                ON CONFLICT (host_id, endpoint_id, rule_id) DO UPDATE
                SET update_user = EXCLUDED.update_user,
                    update_ts = EXCLUDED.update_ts,
                    aggregate_version = EXCLUDED.aggregate_version,
                    active = TRUE
                -- OCC/IDM: Only update if the incoming event is newer
                WHERE api_endpoint_rule_t.aggregate_version < EXCLUDED.aggregate_version
                AND api_endpoint_rule_t.active = FALSE
                """;

        // Note: Assuming SqlUtil.extractEventData(event) is the correct utility based on other methods.
        Map<String, Object> map = SqlUtil.extractEventData(event);

        String hostId = (String)map.get("hostId");
        String endpointId = (String)map.get("endpointId");
        String ruleId = (String)map.get("ruleId");
        long newAggregateVersion = SqlUtil.getNewAggregateVersion(event);

        try (PreparedStatement statement = conn.prepareStatement(sql)) {
            // INSERT values (6 placeholders + active=TRUE in SQL, total 6 dynamic values)
            int i = 1;
            // 1: host_id
            statement.setObject(i++, UUID.fromString(hostId));
            // 2: endpoint_id
            statement.setObject(i++, UUID.fromString(endpointId));
            // 3: rule_id
            statement.setString(i++, ruleId);

            // 4: update_user
            statement.setString(i++, (String)event.get(Constants.USER));
            // 5: update_ts
            statement.setObject(i++, OffsetDateTime.parse((String)event.get(CloudEventV1.TIME)));
            // 6: aggregate_version
            statement.setLong(i++, newAggregateVersion);

            int count = statement.executeUpdate();
            if (count == 0) {
                // count=0 means the ON CONFLICT clause was hit, BUT the WHERE clause (aggregate_version < EXCLUDED.aggregate_version) failed.
                // This is the desired idempotent/out-of-order protection behavior. Log and ignore.
                logger.warn("Creation/Reactivation skipped for hostId {} endpointId {} ruleId {} aggregateVersion {}. A newer or same version already exists.", hostId, endpointId, ruleId, newAggregateVersion);
            }
        } catch (SQLException e) {
            logger.error("SQLException during createApiEndpointRule for hostId {} endpointId {} ruleId {} aggregateVersion {}: {}", hostId, endpointId, ruleId, newAggregateVersion, e.getMessage(), e);
            throw e;
        } catch (Exception e) {
            logger.error("Exception during createApiEndpointRule for hostId {} endpointId {} ruleId {} aggregateVersion {}: {}", hostId, endpointId, ruleId, newAggregateVersion, e.getMessage(), e);
            throw e;
        }
    }

    @Override
    public void deleteApiEndpointRule(Connection conn, Map<String, Object> event) throws SQLException, Exception {
        // Use UPDATE to implement Soft Delete (setting active = FALSE).
        // OCC/IDM is enforced by checking aggregate_version < newAggregateVersion.
        final String sql =
                """
                UPDATE api_endpoint_rule_t
                SET active = FALSE,
                    update_user = ?,
                    update_ts = ?,
                    aggregate_version = ?
                WHERE host_id = ?
                  AND endpoint_id = ?
                  AND rule_id = ?
                  AND aggregate_version < ?
                """; // <<< CRITICAL: Added aggregate_version < ? to enforce monotonicity (OCC/IDM)

        // Note: Assuming SqlUtil.extractEventData(event) is the correct utility based on other methods.
        Map<String, Object> map = SqlUtil.extractEventData(event);

        String hostId = (String)map.get("hostId");
        String endpointId = (String)map.get("endpointId");
        String ruleId = (String)map.get("ruleId");
        // A delete event represents a state change, so it should have a new, incremented version.
        long newAggregateVersion = SqlUtil.getNewAggregateVersion(event);

        try (PreparedStatement statement = conn.prepareStatement(sql)) {
            // SET values (3 placeholders)
            int i = 1;
            // 1: update_user
            statement.setString(i++, (String)event.get(Constants.USER));
            // 2: update_ts
            statement.setObject(i++, OffsetDateTime.parse((String)event.get(CloudEventV1.TIME)));
            // 3: aggregate_version (the new version)
            statement.setLong(i++, newAggregateVersion);

            // WHERE conditions (4 placeholders)
            // 4: host_id
            statement.setObject(i++, UUID.fromString(hostId));
            // 5: endpoint_id
            statement.setObject(i++, UUID.fromString(endpointId));
            // 6: rule_id
            statement.setString(i++, ruleId);
            // 7: aggregate_version < ? (new version for OCC/IDM check)
            statement.setLong(i++, newAggregateVersion);

            int count = statement.executeUpdate();
            if (count == 0) {
                // If 0 rows updated, it means:
                // 1. The record was not found (already deleted or never existed).
                // 2. The OCC/IDM check failed (aggregate_version >= newAggregateVersion).
                // We IGNORE the failure and log a warning, as this is the desired idempotent/monotonic behavior.
                logger.warn("Soft delete skipped for hostId {} endpointId {} ruleId {} aggregateVersion {}. Record not found or a newer/same version already exists.", hostId, endpointId, ruleId, newAggregateVersion);
            }
        } catch (SQLException e) {
            logger.error("SQLException during deleteApiEndpointRule for hostId {} endpointId {} ruleId {} aggregateVersion {}: {}", hostId, endpointId, ruleId, newAggregateVersion, e.getMessage(), e);
            throw e;
        } catch (Exception e) {
            logger.error("Exception during deleteApiEndpointRule for hostId {} endpointId {} ruleId {} aggregateVersion {}: {}", hostId, endpointId, ruleId, newAggregateVersion, e.getMessage(), e);
            throw e;
        }
    }

    @Override
    public Result<String> queryServiceRule(String hostId, String apiId, String apiVersion) {
        Result<String> result = null;
        String sql =
                """
                SELECT ae.host_id, a.api_id, av.api_version, e.endpoint, r.rule_type, ae.rule_id, ae.aggregate_version
                FROM api_endpoint_rule_t ae
                INNER JOIN rule_t r ON ae.rule_id = r.rule_id
                INNER JOIN api_endpoint_t e ON e.endpoint_id = ae.endpoint_id
                INNER JOIN api_version_t av ON av.api_version_id = e.api_version_id
                INNER JOIN api_t a ON a.api_id = av.api_id
                WHERE a.host_id =?
                AND a.api_id = ?
                AND av.api_version = ?
                ORDER BY r.rule_type
                """;
        String sqlRuleBody = "SELECT rule_id, rule_body FROM rule_t WHERE rule_id = ?";
        List<Map<String, Object>> rules = new ArrayList<>();
        Map<String, Object> ruleBodies = new HashMap<>();

        try (Connection connection = ds.getConnection();
             PreparedStatement preparedStatement = connection.prepareStatement(sql)) {
            preparedStatement.setObject(1, UUID.fromString(hostId));
            preparedStatement.setString(2, apiId);
            preparedStatement.setString(3, apiVersion);
            try (ResultSet resultSet = preparedStatement.executeQuery()) {
                while (resultSet.next()) {
                    Map<String, Object> map = new HashMap<>();
                    map.put("hostId", resultSet.getObject("host_id", UUID.class));
                    map.put("endpointId", resultSet.getObject("endpoint_id", UUID.class));
                    map.put("apiId", resultSet.getString("api_id"));
                    map.put("apiVersion", resultSet.getString("api_version"));
                    map.put("endpoint", resultSet.getString("endpoint"));
                    map.put("ruleType", resultSet.getString("rule_type"));
                    String ruleId = resultSet.getString("rule_id");
                    map.put("ruleId", ruleId);
                    map.put("aggregateVersion", resultSet.getLong("aggregate_version"));
                    rules.add(map);

                    // Get rule body if not already cached
                    if (!ruleBodies.containsKey(ruleId)) {
                        String ruleBody = fetchRuleBody(connection, sqlRuleBody, ruleId);
                        // convert the json string to map.
                        Map<String, Object> bodyMap = JsonMapper.string2Map(ruleBody);
                        ruleBodies.put(ruleId, bodyMap);
                    }
                }
            }
            Map<String, Object> combinedResult = new HashMap<>();
            combinedResult.put("rules", rules);
            combinedResult.put("ruleBodies", ruleBodies);
            result = Success.of(JsonMapper.toJson(combinedResult));
        } catch (SQLException e) {
            logger.error("SQLException:", e);
            result = Failure.of(new Status(SQL_EXCEPTION, e.getMessage()));
        } catch (Exception e) {
            logger.error("Exception:", e);
            result = Failure.of(new Status(GENERIC_EXCEPTION, e.getMessage()));
        }
        return result;
    }

    private String fetchRuleBody(Connection connection, String sqlRuleBody, String ruleId) throws SQLException {
        try (PreparedStatement preparedStatement = connection.prepareStatement(sqlRuleBody)) {
            preparedStatement.setString(1, ruleId);
            try (ResultSet resultSet = preparedStatement.executeQuery()) {
                if (resultSet.next()) {
                    return resultSet.getString("rule_body");
                }
            }
        }
        return null; // or throw an exception if you consider this an error state.
    }

    @Override
    public Result<String> queryApiPermission(String hostId, String apiId, String apiVersion) {
        Result<String> result = null;
        String sql = "SELECT\n" +
                "    CASE\n" +
                "        WHEN COUNT(ae.endpoint) > 0 THEN\n" +
                "            JSON_AGG(\n" +
                "                JSON_BUILD_OBJECT(\n" +
                "                    'endpoint', ae.endpoint,\n" +
                "                    'roles', COALESCE((\n" +
                "                        SELECT JSON_ARRAYAGG(\n" +
                "                            JSON_BUILD_OBJECT(\n" +
                "                                'roleId', rp.role_id\n" +
                "                            )\n" +
                "                        )\n" +
                "                        FROM role_permission_t rp\n" +
                "                        INNER JOIN api_endpoint_t re ON rp.endpoint_id = re.endpoint_id AND rp.host_id = re.host_id\n" + // Added join for correct filtering
                "                        INNER JOIN api_version_t rv ON re.api_version_id = rv.api_version_id AND re.host_id = rv.host_id\n" + // Added join for correct filtering
                "                        WHERE rp.host_id = ?\n" +
                "                        AND rv.api_id = ?\n" + // Filter on v.api_id from api_version_t
                "                        AND rv.api_version = ?\n" + // Filter on v.api_version from api_version_t
                "                        AND re.endpoint = ae.endpoint\n" +
                "                    ), '[]'),\n" +
                "                    'positions', COALESCE((\n" +
                "                        SELECT JSON_ARRAYAGG(\n" +
                "                             JSON_BUILD_OBJECT(\n" +
                "                                'positionId', pp.position_id\n" +
                "                             )\n" +
                "                         )\n" +
                "                        FROM position_permission_t pp\n" +
                "                        INNER JOIN api_endpoint_t pe ON pp.endpoint_id = pe.endpoint_id AND pp.host_id = pe.host_id\n" + // Added join for correct filtering
                "                        INNER JOIN api_version_t pv ON pe.api_version_id = pv.api_version_id AND pe.host_id = pv.host_id\n" + // Added join for correct filtering
                "                        WHERE pp.host_id = ?\n" +
                "                        AND pv.api_id = ?\n" + // Filter on v.api_id from api_version_t
                "                        AND pv.api_version = ?\n" + // Filter on v.api_version from api_version_t
                "                        AND pe.endpoint = ae.endpoint\n" +
                "                    ), '[]'),\n" +
                "                    'groups', COALESCE((\n" +
                "                        SELECT JSON_ARRAYAGG(\n" +
                "                            JSON_BUILD_OBJECT(\n" +
                "                               'groupId', gp.group_id\n" +
                "                            )\n" +
                "                        )\n" +
                "                        FROM group_permission_t gp\n" +
                "                        INNER JOIN api_endpoint_t ge ON gp.endpoint_id = ge.endpoint_id AND gp.host_id = ge.host_id\n" + // Added join for correct filtering
                "                        INNER JOIN api_version_t gv ON ge.api_version_id = gv.api_version_id AND ge.host_id = gv.host_id\n" + // Added join for correct filtering
                "                        WHERE gp.host_id = ?\n" +
                "                        AND gv.api_id = ?\n" + // Filter on v.api_id from api_version_t
                "                        AND gv.api_version = ?\n" + // Filter on v.api_version from api_version_t
                "                        AND ge.endpoint = ae.endpoint\n" +
                "                    ), '[]'),\n" +
                "                    'attributes', COALESCE((\n" +
                "                        SELECT JSON_ARRAYAGG(\n" +
                "                            JSON_BUILD_OBJECT(\n" +
                "                                'attribute_id', ap.attribute_id, \n" +
                "                                'attribute_value', ap.attribute_value, \n" +
                "                                'attribute_type', a.attribute_type\n" +
                "                            )\n" +
                "                        )\n" +
                "                        FROM attribute_permission_t ap, attribute_t a\n" +
                "                        INNER JOIN api_endpoint_t ate ON ap.endpoint_id = ate.endpoint_id AND ap.host_id = ate.host_id\n" + // Added join for correct filtering
                "                        INNER JOIN api_version_t atv ON ate.api_version_id = atv.api_version_id AND ate.host_id = atv.host_id\n" + // Added join for correct filtering
                "                        WHERE ap.attribute_id = a.attribute_id\n" +
                "                        AND ap.host_id = ?\n" +
                "                        AND atv.api_id = ?\n" + // Filter on v.api_id from api_version_t
                "                        AND atv.api_version = ?\n" + // Filter on v.api_version from api_version_t
                "                        AND ate.endpoint = ae.endpoint\n" +
                "                    ), '[]'),\n" +
                "                    'users', COALESCE((\n" +
                "                        SELECT JSON_ARRAYAGG(\n" +
                "                            JSON_BUILD_OBJECT(\n" +
                "                                 'userId', user_id,\n" +
                "                                 'startTs', start_ts,\n" +
                "                                 'endTs', end_ts\n" +
                "                            )\n" +
                "                        )\n" +
                "                        FROM user_permission_t up\n" +
                "                        INNER JOIN api_endpoint_t ue ON up.endpoint_id = ue.endpoint_id AND up.host_id = ue.host_id\n" + // Added join for correct filtering
                "                        INNER JOIN api_version_t uv ON ue.api_version_id = uv.api_version_id AND ue.host_id = uv.host_id\n" + // Added join for correct filtering
                "                        WHERE up.host_id = ?\n" +
                "                        AND uv.api_id = ?\n" + // Filter on v.api_id from api_version_t
                "                        AND uv.api_version = ?\n" + // Filter on v.api_version from api_version_t
                "                        AND ue.endpoint = ae.endpoint\n" +
                "                    ), '[]')\n" +
                "                )\n" +
                "            )\n" +
                "        ELSE NULL\n" +
                "    END AS permissions\n" +
                "FROM\n" +
                "    api_endpoint_t ae\n" +
                "INNER JOIN api_version_t av ON ae.api_version_id = av.api_version_id AND ae.host_id = av.host_id\n" + // Join to filter endpoints by apiId and apiVersion
                "WHERE\n" +
                "    ae.host_id = ?\n" +
                "    AND av.api_id = ?\n" + // Use alias for consistency
                "    AND av.api_version = ?;\n"; // Use alias for consistency

        try (Connection connection = ds.getConnection();
             PreparedStatement preparedStatement = connection.prepareStatement(sql)) {
            // Parameters for roles subquery
            preparedStatement.setObject(1, UUID.fromString(hostId));
            preparedStatement.setString(2, apiId);
            preparedStatement.setString(3, apiVersion);
            // Parameters for positions subquery
            preparedStatement.setObject(4, UUID.fromString(hostId));
            preparedStatement.setString(5, apiId);
            preparedStatement.setString(6, apiVersion);
            // Parameters for groups subquery
            preparedStatement.setObject(7, UUID.fromString(hostId));
            preparedStatement.setString(8, apiId);
            preparedStatement.setString(9, apiVersion);
            // Parameters for attributes subquery
            preparedStatement.setObject(10, UUID.fromString(hostId));
            preparedStatement.setString(11, apiId);
            preparedStatement.setString(12, apiVersion);
            // Parameters for users subquery
            preparedStatement.setObject(13, UUID.fromString(hostId));
            preparedStatement.setString(14, apiId);
            preparedStatement.setString(15, apiVersion);
            // Parameters for main query
            preparedStatement.setObject(16, UUID.fromString(hostId));
            preparedStatement.setString(17, apiId);
            preparedStatement.setString(18, apiVersion);

            try (ResultSet resultSet = preparedStatement.executeQuery()) {
                if (resultSet.next()) {
                    String permissionsJson = resultSet.getString("permissions");
                    result = Success.of(permissionsJson);
                }
            }
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
    public Result<List<String>> queryApiFilter(String hostId, String apiId, String apiVersion) {
        Result<List<String>> result = null;
        // Corrected SQL to use endpoint_id from filters and join to api_endpoint_t and api_version_t
        // to filter by api_id and api_version.
        String sql = "SELECT\n" +
                "    JSON_BUILD_OBJECT(\n" +
                "        'role_row', JSON_AGG(\n" +
                "            JSON_BUILD_OBJECT(\n" +
                "                'endpoint', ae.endpoint,\n" +
                "                'roleId', rrf.role_id,\n" +
                "                'colName', rrf.col_name,\n" +
                "                'operator', rrf.operator,\n" +
                "                'colValue', rrf.col_value\n" +
                "            )\n" +
                "        )\n" +
                "    ) AS result\n" +
                "FROM\n" +
                "    role_row_filter_t rrf\n" +
                "INNER JOIN api_endpoint_t ae ON rrf.endpoint_id = ae.endpoint_id AND rrf.host_id = ae.host_id\n" +
                "INNER JOIN api_version_t av ON ae.api_version_id = av.api_version_id AND ae.host_id = av.host_id\n" +
                "WHERE\n" +
                "    rrf.host_id = ?\n" +
                "    AND av.api_id = ?\n" +
                "    AND av.api_version = ?\n" +
                "GROUP BY 1\n" + // Group by a constant to aggregate all results into one JSON_AGG
                "HAVING COUNT(*) > 0 \n" +
                "UNION ALL\n" +
                "SELECT\n" +
                "    JSON_BUILD_OBJECT(\n" +
                "        'role_col', JSON_AGG(\n" +
                "            JSON_BUILD_OBJECT(\n" +
                "                'endpoint', ae.endpoint,\n" +
                "                'roleId', rcf.role_id,\n" +
                "                'columns', rcf.columns\n" +
                "            )\n" +
                "        )\n" +
                "    ) AS result\n" +
                "FROM\n" +
                "    role_col_filter_t rcf\n" +
                "INNER JOIN api_endpoint_t ae ON rcf.endpoint_id = ae.endpoint_id AND rcf.host_id = ae.host_id\n" +
                "INNER JOIN api_version_t av ON ae.api_version_id = av.api_version_id AND ae.host_id = av.host_id\n" +
                "WHERE\n" +
                "    rcf.host_id = ?\n" +
                "    AND av.api_id = ?\n" +
                "    AND av.api_version = ?\n" +
                "GROUP BY 1\n" +
                "HAVING COUNT(*) > 0\n" +
                "UNION ALL\n" +
                "SELECT\n" +
                "    JSON_BUILD_OBJECT(\n" +
                "        'group_row', JSON_AGG(\n" +
                "            JSON_BUILD_OBJECT(\n" +
                "                'endpoint', ae.endpoint,\n" +
                "                'groupId', grf.group_id,\n" +
                "                'colName', grf.col_name,\n" +
                "                'operator', grf.operator,\n" +
                "                'colValue', grf.col_value\n" +
                "            )\n" +
                "        )\n" +
                "    ) AS result\n" +
                "FROM\n" +
                "    group_row_filter_t grf\n" +
                "INNER JOIN api_endpoint_t ae ON grf.endpoint_id = ae.endpoint_id AND grf.host_id = ae.host_id\n" +
                "INNER JOIN api_version_t av ON ae.api_version_id = av.api_version_id AND ae.host_id = av.host_id\n" +
                "WHERE\n" +
                "    grf.host_id = ?\n" +
                "    AND av.api_id = ?\n" +
                "    AND av.api_version = ?\n" +
                "GROUP BY 1\n" +
                "HAVING COUNT(*) > 0 \n" +
                "UNION ALL\n" +
                "SELECT\n" +
                "    JSON_BUILD_OBJECT(\n" +
                "        'group_col', JSON_AGG(\n" +
                "            JSON_BUILD_OBJECT(\n" +
                "                'endpoint', ae.endpoint,\n" +
                "                'groupId', gcf.group_id,\n" +
                "                'columns', gcf.columns\n" +
                "            )\n" +
                "        )\n" +
                "    ) AS result\n" +
                "FROM\n" +
                "    group_col_filter_t gcf\n" +
                "INNER JOIN api_endpoint_t ae ON gcf.endpoint_id = ae.endpoint_id AND gcf.host_id = ae.host_id\n" +
                "INNER JOIN api_version_t av ON ae.api_version_id = av.api_version_id AND ae.host_id = av.host_id\n" +
                "WHERE\n" +
                "    gcf.host_id = ?\n" +
                "    AND av.api_id = ?\n" +
                "    AND av.api_version = ?\n" +
                "GROUP BY 1\n" +
                "HAVING COUNT(*) > 0\n" +
                "UNION ALL\n" +
                "SELECT\n" +
                "    JSON_BUILD_OBJECT(\n" +
                "        'position_row', JSON_AGG(\n" +
                "            JSON_BUILD_OBJECT(\n" +
                "                'endpoint', ae.endpoint,\n" +
                "                'positionId', prf.position_id,\n" +
                "                'colName', prf.col_name,\n" +
                "                'operator', prf.operator,\n" +
                "                'colValue', prf.col_value\n" +
                "            )\n" +
                "        )\n" +
                "    ) AS result\n" +
                "FROM\n" +
                "    position_row_filter_t prf\n" +
                "INNER JOIN api_endpoint_t ae ON prf.endpoint_id = ae.endpoint_id AND prf.host_id = ae.host_id\n" +
                "INNER JOIN api_version_t av ON ae.api_version_id = av.api_version_id AND ae.host_id = av.host_id\n" +
                "WHERE\n" +
                "    prf.host_id = ?\n" +
                "    AND av.api_id = ?\n" +
                "    AND av.api_version = ?\n" +
                "GROUP BY 1\n" +
                "HAVING COUNT(*) > 0 \n" +
                "UNION ALL\n" +
                "SELECT\n" +
                "    JSON_BUILD_OBJECT(\n" +
                "        'position_col', JSON_AGG(\n" +
                "            JSON_BUILD_OBJECT(\n" +
                "                'endpoint', ae.endpoint,\n" +
                "                'positionId', pcf.position_id,\n" +
                "                'columns', pcf.columns\n" +
                "            )\n" +
                "        )\n" +
                "    ) AS result\n" +
                "FROM\n" +
                "    position_col_filter_t pcf\n" +
                "INNER JOIN api_endpoint_t ae ON pcf.endpoint_id = ae.endpoint_id AND pcf.host_id = ae.host_id\n" +
                "INNER JOIN api_version_t av ON ae.api_version_id = av.api_version_id AND ae.host_id = av.host_id\n" +
                "WHERE\n" +
                "    pcf.host_id = ?\n" +
                "    AND av.api_id = ?\n" +
                "    AND av.api_version = ?\n" +
                "GROUP BY 1\n" +
                "HAVING COUNT(*) > 0\n" +
                "UNION ALL\n" +
                "SELECT\n" +
                "    JSON_BUILD_OBJECT(\n" +
                "        'attribute_row', JSON_AGG(\n" +
                "            JSON_BUILD_OBJECT(\n" +
                "                'endpoint', ae.endpoint,\n" +
                "                'attributeId', arf.attribute_id,\n" +
                "                'attributeValue', arf.attribute_value,\n" +
                "                'colName', arf.col_name,\n" +
                "                'operator', arf.operator,\n" +
                "                'colValue', arf.col_value\n" +
                "            )\n" +
                "        )\n" +
                "    ) AS result\n" +
                "FROM\n" +
                "    attribute_row_filter_t arf\n" +
                "INNER JOIN api_endpoint_t ae ON arf.endpoint_id = ae.endpoint_id AND arf.host_id = ae.host_id\n" +
                "INNER JOIN api_version_t av ON ae.api_version_id = av.api_version_id AND ae.host_id = av.host_id\n" +
                "WHERE\n" +
                "    arf.host_id = ?\n" +
                "    AND av.api_id = ?\n" +
                "    AND av.api_version = ?\n" +
                "GROUP BY 1\n" +
                "HAVING COUNT(*) > 0 \n" +
                "UNION ALL\n" +
                "SELECT\n" +
                "    JSON_BUILD_OBJECT(\n" +
                "        'attribute_col', JSON_AGG(\n" +
                "            JSON_BUILD_OBJECT(\n" +
                "                'endpoint', ae.endpoint,\n" +
                "                'attributeId', acf.attribute_id,\n" +
                "                'attributeValue', acf.attribute_value,\n" +
                "                'columns', acf.columns\n" +
                "            )\n" +
                "        )\n" +
                "    ) AS result\n" +
                "FROM\n" +
                "    attribute_col_filter_t acf\n" +
                "INNER JOIN api_endpoint_t ae ON acf.endpoint_id = ae.endpoint_id AND acf.host_id = ae.host_id\n" +
                "INNER JOIN api_version_t av ON ae.api_version_id = av.api_version_id AND ae.host_id = av.host_id\n" +
                "WHERE\n" +
                "    acf.host_id = ?\n" +
                "    AND av.api_id = ?\n" +
                "    AND av.api_version = ?\n" +
                "GROUP BY 1\n" +
                "HAVING COUNT(*) > 0\n" +
                "UNION ALL\n" +
                "SELECT\n" +
                "    JSON_BUILD_OBJECT(\n" +
                "        'user_row', JSON_AGG(\n" +
                "            JSON_BUILD_OBJECT(\n" +
                "                'endpoint', ae.endpoint,\n" +
                "                'userId', urf.user_id,\n" +
                "                'startTs', urf.start_ts,\n" +
                "                'endTs', urf.end_ts,\n" +
                "                'colName', urf.col_name,\n" +
                "                'operator', urf.operator,\n" +
                "                'colValue', urf.col_value\n" +
                "            )\n" +
                "        )\n" +
                "    ) AS result\n" +
                "FROM\n" +
                "    user_row_filter_t urf\n" +
                "INNER JOIN api_endpoint_t ae ON urf.endpoint_id = ae.endpoint_id AND urf.host_id = ae.host_id\n" +
                "INNER JOIN api_version_t av ON ae.api_version_id = av.api_version_id AND ae.host_id = av.host_id\n" +
                "WHERE\n" +
                "    urf.host_id = ?\n" +
                "    AND av.api_id = ?\n" +
                "    AND av.api_version = ?\n" +
                "GROUP BY 1\n" +
                "HAVING COUNT(*) > 0 \n" +
                "UNION ALL\n" +
                "SELECT\n" +
                "    JSON_BUILD_OBJECT(\n" +
                "        'user_col', JSON_AGG(\n" +
                "            JSON_BUILD_OBJECT(\n" +
                "                'endpoint', ae.endpoint,\n" +
                "                'userId', ucf.user_id,\n" +
                "                'startTs', ucf.start_ts,\n" +
                "                'endTs', ucf.end_ts,\n" +
                "                'columns', ucf.columns\n" +
                "            )\n" +
                "        )\n" +
                "    ) AS result\n" +
                "FROM\n" +
                "    user_col_filter_t ucf\n" +
                "INNER JOIN api_endpoint_t ae ON ucf.endpoint_id = ae.endpoint_id AND ucf.host_id = ae.host_id\n" +
                "INNER JOIN api_version_t av ON ae.api_version_id = av.api_version_id AND ae.host_id = av.host_id\n" +
                "WHERE\n" +
                "    ucf.host_id = ?\n" +
                "    AND av.api_id = ?\n" +
                "    AND av.api_version = ?\n" +
                "GROUP BY 1\n" +
                "HAVING COUNT(*) > 0\n";

        try (Connection connection = ds.getConnection();
             PreparedStatement preparedStatement = connection.prepareStatement(sql)) {
            // Each UNION ALL block needs its set of parameters. There are 8 blocks, each takes hostId, apiId, apiVersion.
            // So, 8 * 3 = 24 parameters in total.
            for (int i = 0; i < 8; i++) {
                preparedStatement.setObject(i * 3 + 1, UUID.fromString(hostId));
                preparedStatement.setString(i * 3 + 2, apiId);
                preparedStatement.setString(i * 3 + 3, apiVersion);
            }

            List<String> list = new ArrayList<>();
            try (ResultSet resultSet = preparedStatement.executeQuery()) {
                while (resultSet.next()) {
                    String json = resultSet.getString("result");
                    list.add(json);
                }
            }
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
    public Result<String> getServiceIdLabel(String hostId) {
        Result<String> result = null;
        String sql = "SELECT service_id FROM api_version_t WHERE host_id =  ?";
        List<Map<String, Object>> labels = new ArrayList<>();
        try (Connection connection = ds.getConnection();
             PreparedStatement preparedStatement = connection.prepareStatement(sql)) {
            preparedStatement.setObject(1, UUID.fromString(hostId));
            try (ResultSet resultSet = preparedStatement.executeQuery()) {
                while (resultSet.next()) {
                    Map<String, Object> map = new HashMap<>();
                    map.put("id", resultSet.getString("service_id"));
                    map.put("label", resultSet.getString("service_id"));
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

}
