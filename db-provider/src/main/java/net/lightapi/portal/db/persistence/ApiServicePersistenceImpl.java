package net.lightapi.portal.db.persistence;

import com.networknt.config.JsonMapper;
import com.networknt.monad.Failure;
import com.networknt.monad.Result;
import com.networknt.monad.Success;
import com.networknt.status.Status;
import com.networknt.utility.Constants;
import io.cloudevents.core.v1.CloudEventV1;
import net.lightapi.portal.PortalConstants;
import net.lightapi.portal.db.ConcurrencyException;
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
import static net.lightapi.portal.db.util.SqlUtil.addCondition;

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
    public void createService(Connection conn, Map<String, Object> event) throws SQLException, Exception {
        final String insertService =
                """
                INSERT INTO api_t (host_id, api_id, api_name,
                api_desc, operation_owner, delivery_owner, region, business_group,
                lob, platform, capability, git_repo, api_tags,
                api_status, update_user, update_ts, aggregate_version)
                VALUES (?, ?, ?, ?, ?,   ?, ?, ?, ?, ?,   ?, ?, ?, ?, ?,   ?, ?)
                """;

        Map<String, Object> map = (Map<String, Object>) event.get(PortalConstants.DATA);
        String apiId = (String) map.get("apiId");
        long newAggregateVersion = SqlUtil.getNewAggregateVersion(event);
        try (PreparedStatement statement = conn.prepareStatement(insertService)) {
            statement.setObject(1, UUID.fromString((String) map.get("hostId")));
            statement.setString(2, apiId);
            statement.setString(3, (String) map.get("apiName"));
            if (map.containsKey("apiDesc")) {
                String apiDesc = (String) map.get("apiDesc");
                if (apiDesc != null && !apiDesc.trim().isEmpty()) {
                    statement.setString(4, apiDesc);
                } else {
                    statement.setNull(4, Types.VARCHAR);
                }
            } else {
                statement.setNull(4, Types.VARCHAR);
            }
            String operationOwner = (String) map.get("operationOwner");
            if (operationOwner != null && !operationOwner.trim().isEmpty()) {
                statement.setObject(5, UUID.fromString(operationOwner));
            } else {
                statement.setNull(5, Types.OTHER);
            }
            String deliveryOwner = (String) map.get("deliveryOwner");
            if (deliveryOwner != null && !deliveryOwner.trim().isEmpty()) {
                statement.setObject(6, UUID.fromString(deliveryOwner));
            } else {
                statement.setNull(6, Types.OTHER);
            }

            if (map.containsKey("region")) {
                String region = (String) map.get("region");
                if (region != null && !region.trim().isEmpty()) {
                    statement.setString(7, region);
                } else {
                    statement.setNull(7, Types.VARCHAR);
                }
            } else {
                statement.setNull(7, Types.VARCHAR);
            }
            if (map.containsKey("businessGroup")) {
                String businessGroup = (String) map.get("businessGroup");
                if (businessGroup != null && !businessGroup.trim().isEmpty()) {
                    statement.setString(8, businessGroup);
                } else {
                    statement.setNull(8, Types.VARCHAR);
                }
            } else {
                statement.setNull(8, Types.VARCHAR);
            }
            if (map.containsKey("lob")) {
                String lob = (String) map.get("lob");
                if (lob != null && !lob.trim().isEmpty()) {
                    statement.setString(9, lob);
                } else {
                    statement.setNull(9, Types.VARCHAR);
                }
            } else {
                statement.setNull(9, Types.VARCHAR);
            }
            if (map.containsKey("platform")) {
                String platform = (String) map.get("platform");
                if (platform != null && !platform.trim().isEmpty()) {
                    statement.setString(10, platform);
                } else {
                    statement.setNull(10, Types.VARCHAR);
                }
            } else {
                statement.setNull(10, Types.VARCHAR);
            }
            if (map.containsKey("capability")) {
                String capability = (String) map.get("capability");
                if (capability != null && !capability.trim().isEmpty()) {
                    statement.setString(11, capability);
                } else {
                    statement.setNull(11, Types.VARCHAR);
                }
            } else {
                statement.setNull(11, Types.VARCHAR);
            }
            if (map.containsKey("gitRepo")) {
                String gitRepo = (String) map.get("gitRepo");
                if (gitRepo != null && !gitRepo.trim().isEmpty()) {
                    statement.setString(12, gitRepo);
                } else {
                    statement.setNull(12, Types.VARCHAR);
                }
            } else {
                statement.setNull(12, Types.VARCHAR);
            }
            if (map.containsKey("apiTags")) {
                String apiTags = (String) map.get("apiTags");
                if (apiTags != null && !apiTags.trim().isEmpty()) {
                    statement.setString(13, apiTags);
                } else {
                    statement.setNull(13, Types.VARCHAR);
                }
            } else {
                statement.setNull(13, Types.VARCHAR);
            }

            statement.setString(14, (String) map.get("apiStatus"));
            statement.setString(15, (String) event.get(Constants.USER));
            statement.setObject(16, OffsetDateTime.parse((String) event.get(CloudEventV1.TIME)));
            statement.setLong(17, newAggregateVersion);
            int count = statement.executeUpdate();
            if (count == 0) {
                throw new SQLException(String.format("Failed to insert apiId %s", apiId + " with aggregate version " + newAggregateVersion));
            }
        } catch (SQLException e) {
            logger.error("SQLException during createService for apiId {} aggregateVersion {}: {}", apiId, newAggregateVersion, e.getMessage(), e);
            throw e;
        } catch (Exception e) {
            logger.error("Exception during createService for apiId {} aggregateVersion {}: {}", apiId, newAggregateVersion, e.getMessage(), e);
            throw e;
        }
    }

    private boolean queryApiExists(Connection conn, String hostId, String apiId) throws SQLException {
        final String sql =
                """
                SELECT COUNT(*) FROM api_t WHERE host_id = ? AND api_id = ?
                """;
        try (PreparedStatement pst = conn.prepareStatement(sql)) {
            pst.setObject(1, UUID.fromString(hostId));
            pst.setString(2, apiId);
            try (ResultSet rs = pst.executeQuery()) {
                return rs.next() && rs.getInt(1) > 0;
            }
        }
    }

    @Override
    public void updateService(Connection conn, Map<String, Object> event) throws SQLException, Exception {
        final String updateApi =
                """
                UPDATE api_t SET api_name = ?, api_desc = ?,
                operation_owner = ?, delivery_owner = ?, region = ?, business_group = ?, lob = ?, platform = ?,
                capability = ?, git_repo = ?, api_tags = ?, api_status = ?, update_user = ?, update_ts = ?, aggregate_version = ?
                WHERE host_id = ? AND api_id = ? AND aggregate_version = ?
                """;

        Map<String, Object> map = (Map<String, Object>) event.get(PortalConstants.DATA);
        String apiId = (String) map.get("apiId");
        long oldAggregateVersion = SqlUtil.getOldAggregateVersion(event);
        long newAggregateVersion = SqlUtil.getNewAggregateVersion(event);
        try (PreparedStatement statement = conn.prepareStatement(updateApi)) {
            statement.setString(1, (String) map.get("apiName"));
            if (map.containsKey("apiDesc")) {
                String apiDesc = (String) map.get("apiDesc");
                if (apiDesc != null && !apiDesc.trim().isEmpty()) {
                    statement.setString(2, apiDesc);
                } else {
                    statement.setNull(2, Types.VARCHAR);
                }
            } else {
                statement.setNull(2, Types.VARCHAR);
            }
            String operationOwner = (String) map.get("operationOwner");
            if (operationOwner != null && !operationOwner.trim().isEmpty()) {
                statement.setObject(3, UUID.fromString(operationOwner));
            } else {
                statement.setNull(3, Types.OTHER);
            }

            String deliveryOwner = (String) map.get("deliveryOwner");
            if (deliveryOwner != null && !deliveryOwner.trim().isEmpty()) {
                statement.setObject(4, UUID.fromString(deliveryOwner));
            } else {
                statement.setNull(4, Types.OTHER);
            }

            if (map.containsKey("region")) {
                String region = (String) map.get("region");
                if (region != null && !region.trim().isEmpty()) {
                    statement.setString(5, region);
                } else {
                    statement.setNull(5, Types.VARCHAR);
                }
            } else {
                statement.setNull(5, Types.VARCHAR);
            }
            if (map.containsKey("businessGroup")) {
                String businessGroup = (String) map.get("businessGroup");
                if (businessGroup != null && !businessGroup.trim().isEmpty()) {
                    statement.setString(6, businessGroup);
                } else {
                    statement.setNull(6, Types.VARCHAR);
                }
            } else {
                statement.setNull(6, Types.VARCHAR);
            }
            if (map.containsKey("lob")) {
                String lob = (String) map.get("lob");
                if (lob != null && !lob.trim().isEmpty()) {
                    statement.setString(7, lob);
                } else {
                    statement.setNull(7, Types.VARCHAR);
                }
            } else {
                statement.setNull(7, Types.VARCHAR);
            }
            if (map.containsKey("platform")) {
                String platform = (String) map.get("platform");
                if (platform != null && !platform.trim().isEmpty()) {
                    statement.setString(8, platform);
                } else {
                    statement.setNull(8, Types.VARCHAR);
                }
            } else {
                statement.setNull(8, Types.VARCHAR);
            }
            if (map.containsKey("capability")) {
                String capability = (String) map.get("capability");
                if (capability != null && !capability.trim().isEmpty()) {
                    statement.setString(9, capability);
                } else {
                    statement.setNull(9, Types.VARCHAR);
                }
            } else {
                statement.setNull(9, Types.VARCHAR);
            }
            if (map.containsKey("gitRepo")) {
                String gitRepo = (String) map.get("gitRepo");
                if (gitRepo != null && !gitRepo.trim().isEmpty()) {
                    statement.setString(10, gitRepo);
                } else {
                    statement.setNull(10, Types.VARCHAR);
                }
            } else {
                statement.setNull(10, Types.VARCHAR);
            }
            if (map.containsKey("apiTags")) {
                String apiTags = (String) map.get("apiTags");
                if (apiTags != null && !apiTags.trim().isEmpty()) {
                    statement.setString(11, apiTags);
                } else {
                    statement.setNull(11, Types.VARCHAR);
                }
            } else {
                statement.setNull(11, Types.VARCHAR);
            }
            statement.setString(12, (String) map.get("apiStatus"));
            statement.setString(13, (String) event.get(Constants.USER));
            statement.setObject(14, OffsetDateTime.parse((String) event.get(CloudEventV1.TIME)));
            statement.setLong(15, newAggregateVersion);
            statement.setObject(16, UUID.fromString((String) map.get("hostId")));
            statement.setString(17, apiId);
            statement.setLong(18, oldAggregateVersion);

            int count = statement.executeUpdate();
            if (count == 0) {
                if (queryApiExists(conn, (String)event.get(Constants.HOST), apiId)) {
                    throw new ConcurrencyException("Optimistic concurrency conflict for api " + apiId + ". Expected version " + oldAggregateVersion + " but found a different version " + newAggregateVersion + ".");
                } else {
                    throw new SQLException("No record found to update for api " + apiId + ".");
                }
            }
        } catch (SQLException e) {
            logger.error("SQLException during updateService for apiId {} (old: {}) -> (new: {}): {}", apiId, oldAggregateVersion, newAggregateVersion, e.getMessage(), e);
            throw e;
        } catch (Exception e) {
            logger.error("Exception during updateService for apiId {} (old: {}) -> (new: {}): {}", apiId, oldAggregateVersion, newAggregateVersion, e.getMessage(), e);
            throw e;
        }
    }

    @Override
    public void deleteService(Connection conn, Map<String, Object> event) throws SQLException, Exception {
        final String deleteApplication = "DELETE from api_t WHERE host_id = ? AND api_id = ? AND aggregate_version = ?";
        Map<String, Object> map = (Map<String, Object>) event.get(PortalConstants.DATA);
        String apiId = (String) map.get("apiId");
        long oldAggregateVersion = SqlUtil.getOldAggregateVersion(event);
        try (PreparedStatement statement = conn.prepareStatement(deleteApplication)) {
            statement.setObject(1, UUID.fromString((String) map.get("hostId")));
            statement.setString(2, apiId);
            statement.setLong(3, oldAggregateVersion);
            int count = statement.executeUpdate();
            if (count == 0) {
                if (queryApiExists(conn, (String)event.get(Constants.HOST), apiId)) {
                    throw new ConcurrencyException("Optimistic concurrency conflict during deleteService for apiId " + apiId + " aggregateVersion " + oldAggregateVersion + " but found a different version or already updated.");
                } else {
                    throw new SQLException("No record found during deleteService for apiId " + apiId + ". It might have been already deleted.");
                }
            }
        } catch (SQLException e) {
            logger.error("SQLException during deleteService for apiId {} aggregateVersion: {}: {}", apiId, oldAggregateVersion, e.getMessage(), e);
            throw e;
        } catch (Exception e) {
            logger.error("Exception during deleteService for apiId {} aggregateVersion {}: {}", apiId, oldAggregateVersion, e.getMessage(), e);
            throw e;
        }
    }

    @Override
    public Result<String> queryService(int offset, int limit, String hostId, String apiId, String apiName,
                                       String apiDesc, String operationOwner, String deliveryOwner, String region, String businessGroup,
                                       String lob, String platform, String capability, String gitRepo, String apiTags, String apiStatus) {
        Result<String> result = null;
        String s =
                """
                SELECT COUNT(*) OVER () AS total,
                host_id, api_id, api_name,
                api_desc, operation_owner, delivery_owner, region, business_group,
                lob, platform, capability, git_repo, api_tags, api_status,
                update_user, update_ts, aggregate_version
                FROM api_t
                WHERE host_id = ?
                """;

        StringBuilder sqlBuilder = new StringBuilder(s);

        List<Object> parameters = new ArrayList<>();
        parameters.add(UUID.fromString(hostId));

        StringBuilder whereClause = new StringBuilder();

        SqlUtil.addCondition(whereClause, parameters, "api_id", apiId);
        SqlUtil.addCondition(whereClause, parameters, "api_name", apiName);
        SqlUtil.addCondition(whereClause, parameters, "api_desc", apiDesc);
        SqlUtil.addCondition(whereClause, parameters, "operation_owner", operationOwner != null ? UUID.fromString(operationOwner) : null);
        SqlUtil.addCondition(whereClause, parameters, "delivery_owner", deliveryOwner != null ? UUID.fromString(deliveryOwner) : null);
        SqlUtil.addCondition(whereClause, parameters, "region", region);
        SqlUtil.addCondition(whereClause, parameters, "business_group", businessGroup);
        SqlUtil.addCondition(whereClause, parameters, "lob", lob);
        SqlUtil.addCondition(whereClause, parameters, "platform", platform);
        SqlUtil.addCondition(whereClause, parameters, "capability", capability);
        SqlUtil.addCondition(whereClause, parameters, "git_repo", gitRepo);
        SqlUtil.addCondition(whereClause, parameters, "api_tags", apiTags);
        SqlUtil.addCondition(whereClause, parameters, "api_status", apiStatus);

        if (!whereClause.isEmpty()) {
            sqlBuilder.append("AND ").append(whereClause);
        }


        sqlBuilder.append(" ORDER BY api_id\n" +
                "LIMIT ? OFFSET ?");

        parameters.add(limit);
        parameters.add(offset);
        String sql = sqlBuilder.toString();
        int total = 0;
        List<Map<String, Object>> services = new ArrayList<>();

        try (Connection connection = ds.getConnection();
             PreparedStatement preparedStatement = connection.prepareStatement(sql)) {

            for (int i = 0; i < parameters.size(); i++) {
                preparedStatement.setObject(i + 1, parameters.get(i));
            }


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
    public Result<String> queryEndpointLabel(String hostId, String apiId, String apiVersion) {
        Result<String> result = null;
        // The original implementation used apiId as apiVersionId. Adjusting to reflect that, assuming it's intentional.
        // If not, a lookup for apiVersionId based on apiId and apiVersion would be needed.
        // Based on the interface, it expects apiVersionId, but the impl used apiId directly. Sticking to original impl's logic here.
        // Assuming apiId here actually refers to apiVersionId as per the method parameter comment.
        String sql = "SELECT endpoint_id FROM api_endpoint_t WHERE host_id = ? AND api_version_id = ?";
        List<Map<String, Object>> labels = new ArrayList<>();
        try (Connection connection = ds.getConnection();
             PreparedStatement preparedStatement = connection.prepareStatement(sql)) {
            preparedStatement.setObject(1, UUID.fromString(hostId));
            preparedStatement.setObject(2, UUID.fromString(apiId)); // This might be an error in original logic if apiId != apiVersionId
            try (ResultSet resultSet = preparedStatement.executeQuery()) {
                while (resultSet.next()) {
                    Map<String, Object> map = new HashMap<>();
                    String id = resultSet.getString("endpoint_id");
                    map.put("id", id);
                    map.put("label", id);
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
    public void createServiceVersion(Connection conn, Map<String, Object> event) throws SQLException, Exception {
        final String insertServiceVersion = "INSERT INTO api_version_t (host_id, api_version_id, api_id, api_version, api_type, service_id, api_version_desc, " +
                "spec_link, spec, update_user, update_ts, aggregate_version) " +
                "VALUES (?, ?, ?, ?, ?,   ?, ?, ?, ?, ?,  ?, ?)";
        final String insertEndpoint = "INSERT INTO api_endpoint_t (host_id, endpoint_id, api_version_id, endpoint, http_method, " +
                "endpoint_path, endpoint_name, endpoint_desc, update_user, update_ts, aggregate_version) " +
                "VALUES (?,? ,?, ?, ?,  ?, ?, ?, ?, ?,  ?)";
        final String insertScope = "INSERT INTO api_endpoint_scope_t (host_id, endpoint_id, scope, scope_desc, " +
                "update_user, update_ts, aggregate_version) " +
                "VALUES (?, ?, ?, ?, ?,  ?, ?)";

        Map<String, Object> map = (Map<String, Object>) event.get(PortalConstants.DATA);
        String apiVersionId = (String) map.get("apiVersionId");
        long newAggregateVersion = SqlUtil.getNewAggregateVersion(event);
        try (PreparedStatement statement = conn.prepareStatement(insertServiceVersion)) {
            statement.setObject(1, UUID.fromString((String) map.get("hostId")));
            statement.setObject(2, UUID.fromString(apiVersionId));
            statement.setString(3, (String) map.get("apiId"));
            statement.setString(4, (String) map.get("apiVersion"));
            statement.setString(5, (String) map.get("apiType"));
            statement.setString(6, (String) map.get("serviceId"));

            if (map.containsKey("apiVersionDesc")) {
                String apiDesc = (String) map.get("apiVersionDesc");
                if (apiDesc != null && !apiDesc.trim().isEmpty()) {
                    statement.setString(7, apiDesc);
                } else {
                    statement.setNull(7, Types.VARCHAR);
                }
            } else {
                statement.setNull(7, Types.VARCHAR);
            }
            if (map.containsKey("specLink")) {
                String specLink = (String) map.get("specLink");
                if (specLink != null && !specLink.trim().isEmpty()) {
                    statement.setString(8, specLink);
                } else {
                    statement.setNull(8, Types.VARCHAR);
                }
            } else {
                statement.setNull(8, Types.VARCHAR);
            }
            if (map.containsKey("spec")) {
                String spec = (String) map.get("spec");
                if (spec != null && !spec.trim().isEmpty()) {
                    statement.setString(9, spec);
                } else {
                    statement.setNull(9, Types.VARCHAR);
                }
            } else {
                statement.setNull(9, Types.VARCHAR);
            }
            statement.setString(10, (String) event.get(Constants.USER));
            statement.setObject(11, OffsetDateTime.parse((String) event.get(CloudEventV1.TIME)));
            statement.setLong(12, newAggregateVersion);
            int count = statement.executeUpdate();
            if (count == 0) {
                throw new SQLException(String.format("Failed to insert api version %s with aggregateVersion %d", apiVersionId, newAggregateVersion));
            }
            List<Map<String, Object>> endpoints = (List<Map<String, Object>>) map.get("endpoints");
            if (endpoints != null && !endpoints.isEmpty()) {
                // insert endpoints
                for (Map<String, Object> endpoint : endpoints) {
                    try (PreparedStatement statementInsert = conn.prepareStatement(insertEndpoint)) {
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
                        statementInsert.setLong(11, newAggregateVersion);
                        statementInsert.executeUpdate();
                    }
                    // insert scopes
                    List<String> scopes = (List<String>) endpoint.get("scopes");
                    if (scopes != null && !scopes.isEmpty()) {
                        for (String scope : scopes) {
                            String[] scopeDesc = scope.split(":");
                            try (PreparedStatement statementScope = conn.prepareStatement(insertScope)) {
                                statementScope.setObject(1, UUID.fromString((String) map.get("hostId")));
                                statementScope.setObject(2, UUID.fromString((String) endpoint.get("endpointId")));
                                statementScope.setString(3, scopeDesc[0]);
                                if (scopeDesc.length == 1)
                                    statementScope.setNull(4, NULL);
                                else
                                    statementScope.setString(4, scopeDesc[1]);
                                statementScope.setString(5, (String) event.get(Constants.USER));
                                statementScope.setObject(6, OffsetDateTime.parse((String) event.get(CloudEventV1.TIME)));
                                statementScope.setLong(7, newAggregateVersion);
                                statementScope.executeUpdate();
                            }
                        }
                    }
                }
            }
        } catch (SQLException e) {
            logger.error("SQLException during createServiceVersion for id {} and aggregateVersion {}: {}", apiVersionId, newAggregateVersion, e.getMessage(), e);
            throw e;
        } catch (Exception e) {
            logger.error("Exception during createServiceVersion for id {} and aggregateVersion {}: {}", apiVersionId, newAggregateVersion, e.getMessage(), e);
            throw e;
        }
    }

    private boolean queryServiceVersionExists(Connection conn, String hostId, String apiVersionId) throws SQLException {
        final String sql =
                """
                SELECT COUNT(*) FROM api_version_t WHERE host_id = ? AND api_version_id = ?
                """;
        try (PreparedStatement pst = conn.prepareStatement(sql)) {
            pst.setObject(1, UUID.fromString(hostId));
            pst.setString(2, apiVersionId);
            try (ResultSet rs = pst.executeQuery()) {
                return rs.next() && rs.getInt(1) > 0;
            }
        }
    }


    @Override
    public void updateServiceVersion(Connection conn, Map<String, Object> event) throws SQLException, Exception {
        final String updateApi = "UPDATE api_version_t SET api_id = ?, api_version = ?, api_type = ?, service_id = ?, " +
                "api_version_desc = ?, spec_link = ?,  spec = ?," +
                "update_user = ?, update_ts = ?, aggregate_version =? " +
                "WHERE host_id = ? AND api_version_id = ? AND aggregate_version = ?";
        final String deleteEndpoint = "DELETE FROM api_endpoint_t WHERE host_id = ? AND api_version_id = ? AND aggregate_version = ?";
        final String insertEndpoint = "INSERT INTO api_endpoint_t (host_id, endpoint_id, api_version_id, endpoint, http_method, " +
                "endpoint_path, endpoint_name, endpoint_desc, update_user, update_ts, aggregate_version) " +
                "VALUES (?,? ,?, ?, ?,  ?, ?, ?, ?, ?,  ?)";
        final String insertScope = "INSERT INTO api_endpoint_scope_t (host_id, endpoint_id, scope, scope_desc, " +
                "update_user, update_ts, aggregate_version) " +
                "VALUES (?, ?, ?, ?, ?,  ?, ?)";

        Map<String, Object> map = (Map<String, Object>) event.get(PortalConstants.DATA);
        String apiVersionId = (String) map.get("apiVersionId");
        long oldAggregateVersion = SqlUtil.getOldAggregateVersion(event);
        long newAggregateVersion = SqlUtil.getNewAggregateVersion(event);
        try (PreparedStatement statement = conn.prepareStatement(updateApi)) {
            statement.setString(1, (String) map.get("apiId"));
            statement.setString(2, (String) map.get("apiVersion"));
            statement.setString(3, (String) map.get("apiType"));
            statement.setString(4, (String) map.get("serviceId"));

            if (map.containsKey("apiVersionDesc")) {
                String apiDesc = (String) map.get("apiVersionDesc");
                if (apiDesc != null && !apiDesc.trim().isEmpty()) {
                    statement.setString(5, apiDesc);
                } else {
                    statement.setNull(5, Types.VARCHAR);
                }
            } else {
                statement.setNull(5, Types.VARCHAR);
            }
            if (map.containsKey("specLink")) {
                String specLink = (String) map.get("specLink");
                if (specLink != null && !specLink.trim().isEmpty()) {
                    statement.setString(6, specLink);
                } else {
                    statement.setNull(6, Types.VARCHAR);
                }
            } else {
                statement.setNull(6, Types.VARCHAR);
            }
            if (map.containsKey("spec")) {
                String spec = (String) map.get("spec");
                if (spec != null && !spec.trim().isEmpty()) {
                    statement.setString(7, spec);
                } else {
                    statement.setNull(7, Types.VARCHAR);
                }
            } else {
                statement.setNull(7, Types.VARCHAR);
            }

            statement.setString(8, (String) event.get(Constants.USER));
            statement.setObject(9, OffsetDateTime.parse((String) event.get(CloudEventV1.TIME)));
            statement.setLong(10, newAggregateVersion);
            statement.setObject(11, UUID.fromString((String) map.get("hostId")));
            statement.setObject(12, UUID.fromString(apiVersionId));
            statement.setLong(13, oldAggregateVersion);

            int count = statement.executeUpdate();
            if (count == 0) {
                if (queryServiceVersionExists(conn, (String)event.get(Constants.HOST), apiVersionId)) {
                    throw new ConcurrencyException("Optimistic concurrency conflict for apiVersionId " + apiVersionId + ". Expected version " + oldAggregateVersion + " but found a different version " + newAggregateVersion + ".");
                } else {
                    throw new SQLException("No record found to update for apiVersionId " + apiVersionId + ".");
                }
            }
            List<Map<String, Object>> endpoints = (List<Map<String, Object>>) map.get("endpoints");
            if (endpoints != null && !endpoints.isEmpty()) {
                // delete endpoints for the api version. the api_endpoint_scope_t will be deleted by the cascade.
                try (PreparedStatement statementDelete = conn.prepareStatement(deleteEndpoint)) {
                    statementDelete.setObject(1, UUID.fromString((String) map.get("hostId")));
                    statementDelete.setObject(2, UUID.fromString(apiVersionId));
                    statementDelete.setLong(3, oldAggregateVersion);
                    statementDelete.executeUpdate();
                }
                // insert endpoints
                for (Map<String, Object> endpoint : endpoints) {
                    try (PreparedStatement statementInsert = conn.prepareStatement(insertEndpoint)) {
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
                        statementInsert.setLong(11, newAggregateVersion);
                        statementInsert.executeUpdate();
                    }
                    // insert scopes
                    List<String> scopes = (List<String>) endpoint.get("scopes");
                    if (scopes != null && !scopes.isEmpty()) {
                        for (String scope : scopes) {
                            String[] scopeDesc = scope.split(":");
                            try (PreparedStatement statementScope = conn.prepareStatement(insertScope)) {
                                statementScope.setObject(1, UUID.fromString((String) map.get("hostId")));
                                statementScope.setObject(2, UUID.fromString((String) endpoint.get("endpointId")));
                                statementScope.setString(3, scopeDesc[0]);
                                if (scopeDesc.length == 1)
                                    statementScope.setNull(4, NULL);
                                else
                                    statementScope.setString(4, scopeDesc[1]);
                                statementScope.setString(5, (String) event.get(Constants.USER));
                                statementScope.setObject(6, OffsetDateTime.parse((String) event.get(CloudEventV1.TIME)));
                                statementScope.setLong(7, newAggregateVersion);
                                statementScope.executeUpdate();
                            }
                        }
                    }
                }
            }
        } catch (SQLException e) {
            logger.error("SQLException during updateServiceVersion for id {} (old: {}) -> (new: {}): {}", apiVersionId, oldAggregateVersion, newAggregateVersion, e.getMessage(), e);
            throw e;
        } catch (Exception e) {
            logger.error("Exception during updateServiceVersion for id {} (old: {}) -> (new: {}): {}", apiVersionId, oldAggregateVersion, newAggregateVersion, e.getMessage(), e);
            throw e;
        }
    }

    @Override
    public void deleteServiceVersion(Connection conn, Map<String, Object> event) throws SQLException, Exception {
        final String deleteApplication = "DELETE from api_version_t WHERE host_id = ? AND api_version_id = ? AND aggregate_version = ?";
        Map<String, Object> map = (Map<String, Object>) event.get(PortalConstants.DATA);
        String apiVersionId = (String) map.get("apiVersionId");
        long oldAggregateVersion = SqlUtil.getOldAggregateVersion(event);
        try (PreparedStatement statement = conn.prepareStatement(deleteApplication)) {
            statement.setObject(1, UUID.fromString((String) map.get("hostId")));
            statement.setObject(2, UUID.fromString(apiVersionId));
            statement.setLong(3, oldAggregateVersion);
            int count = statement.executeUpdate();
            if (count == 0) {
                if (queryServiceVersionExists(conn, (String)event.get(Constants.HOST), apiVersionId)) {
                    throw new ConcurrencyException("Optimistic concurrency conflict during deleteServiceVersion for apiVersionId " + apiVersionId + " aggregateVersion " + oldAggregateVersion + " but found a different version or already updated.");
                } else {
                    throw new SQLException("No record found during deleteServiceVersion for apiVersionId " + apiVersionId + ". It might have been already deleted.");
                }
            }
        } catch (SQLException e) {
            logger.error("SQLException during deleteServiceVersion for id {} aggregateVersion {}: {}", apiVersionId, oldAggregateVersion, e.getMessage(), e);
            throw e;
        } catch (Exception e) {
            logger.error("Exception during deleteServiceVersion for id {} aggregateVersion {}: {}", apiVersionId, oldAggregateVersion, e.getMessage(), e);
            throw e;
        }
    }

    @Override
    public Result<String> queryServiceVersion(String hostId, String apiId) {
        Result<String> result = null;
        String sql =
                """
                SELECT host_id, api_version_id, api_id, api_version, api_type,
                service_id, api_version_desc, spec_link, spec,\s
                update_user, update_ts, aggregate_version
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
    public void updateServiceSpec(Connection conn, Map<String, Object> event) throws SQLException, Exception {
        final String updateApiVersion =
                """
                UPDATE api_version_t SET spec = ?,
                update_user = ?, update_ts = ?, aggregate_version = ?
                WHERE host_id = ? AND api_version_id = ? AND aggregate_version = ?
                """;
        final String deleteEndpoint = "DELETE FROM api_endpoint_t WHERE host_id = ? AND api_version_id = ? AND aggregate_version = ?";
        final String insertEndpoint = "INSERT INTO api_endpoint_t (host_id, endpoint_id, api_version_id, endpoint, http_method, " +
                "endpoint_path, endpoint_name, endpoint_desc, update_user, update_ts, aggregate_version) " +
                "VALUES (?,? ,?, ?, ?,   ?, ?, ?, ?, ?,  ?)";
        final String insertScope = "INSERT INTO api_endpoint_scope_t (host_id, endpoint_id, scope, scope_desc, " +
                "update_user, update_ts, aggregate_version) " +
                "VALUES (?, ?, ?, ?, ?,  ?, ?)";


        Map<String, Object> map = (Map<String, Object>) event.get(PortalConstants.DATA);
        String apiVersionId = (String) map.get("apiVersionId");
        long oldAggregateVersion = SqlUtil.getOldAggregateVersion(event);
        long newAggregateVersion = SqlUtil.getNewAggregateVersion(event);
        try {
            try (PreparedStatement statement = conn.prepareStatement(updateApiVersion)) {
                statement.setString(1, (String) map.get("spec"));
                statement.setString(2, (String) event.get(Constants.USER));
                statement.setObject(3, OffsetDateTime.parse((String) event.get(CloudEventV1.TIME)));
                statement.setLong(4, newAggregateVersion);
                statement.setObject(5, UUID.fromString((String) map.get("hostId")));
                statement.setObject(6, UUID.fromString(apiVersionId));
                statement.setLong(7, oldAggregateVersion);

                int count = statement.executeUpdate();
                if (count == 0) {
                    if (queryServiceVersionExists(conn, (String)event.get(Constants.HOST), apiVersionId)) {
                        throw new ConcurrencyException("Optimistic concurrency conflict for apiVersionId " + apiVersionId + ". Expected version " + oldAggregateVersion + " but found a different version " + newAggregateVersion + ".");
                    } else {
                        throw new SQLException("No record found to update for apiVersionId " + apiVersionId + ".");
                    }
                }
            }
            // delete endpoints for the api version. the api_endpoint_scope_t will be deleted by the cascade.
            try (PreparedStatement statement = conn.prepareStatement(deleteEndpoint)) {
                statement.setObject(1, UUID.fromString((String) map.get("hostId")));
                statement.setObject(2, UUID.fromString(apiVersionId));
                statement.setLong(3, oldAggregateVersion);
                statement.executeUpdate();
            }
            // insert endpoints
            List<Map<String, Object>> endpoints = (List<Map<String, Object>>) map.get("endpoints");
            for (Map<String, Object> endpoint : endpoints) {
                try (PreparedStatement statement = conn.prepareStatement(insertEndpoint)) {
                    statement.setObject(1, UUID.fromString((String) map.get("hostId")));
                    statement.setObject(2, UUID.fromString((String) endpoint.get("endpointId")));
                    statement.setObject(3, UUID.fromString(apiVersionId));
                    statement.setString(4, (String) endpoint.get("endpoint"));

                    if (endpoint.get("httpMethod") == null)
                        statement.setNull(5, NULL);
                    else
                        statement.setString(5, ((String) endpoint.get("httpMethod")).toLowerCase().trim());

                    if (endpoint.get("endpointPath") == null)
                        statement.setNull(6, NULL);
                    else
                        statement.setString(6, (String) endpoint.get("endpointPath"));

                    if (endpoint.get("endpointName") == null)
                        statement.setNull(7, NULL);
                    else
                        statement.setString(7, (String) endpoint.get("endpointName"));

                    if (endpoint.get("endpointDesc") == null)
                        statement.setNull(8, NULL);
                    else
                        statement.setString(8, (String) endpoint.get("endpointDesc"));

                    statement.setString(9, (String) event.get(Constants.USER));
                    statement.setObject(10, OffsetDateTime.parse((String) event.get(CloudEventV1.TIME)));
                    statement.setLong(11, newAggregateVersion);
                    statement.executeUpdate();
                }
                // insert scopes
                List<String> scopes = (List<String>) endpoint.get("scopes");
                if (scopes != null && !scopes.isEmpty()) {
                    for (String scope : scopes) {
                        String[] scopeDesc = scope.split(":");
                        try (PreparedStatement statement = conn.prepareStatement(insertScope)) {
                            statement.setObject(1, UUID.fromString((String) map.get("hostId")));
                            statement.setObject(2, UUID.fromString((String) endpoint.get("endpointId")));
                            statement.setString(3, scopeDesc[0]);
                            if (scopeDesc.length == 1)
                                statement.setNull(4, NULL);
                            else
                                statement.setString(4, scopeDesc[1]);
                            statement.setString(5, (String) event.get(Constants.USER));
                            statement.setObject(6, OffsetDateTime.parse((String) event.get(CloudEventV1.TIME)));
                            statement.setLong(7, newAggregateVersion);
                            statement.executeUpdate();
                        }
                    }
                }
            }

        } catch (SQLException e) {
            logger.error("SQLException during updateServiceSpec for id {} (old: {}) -> (new: {}): {}", apiVersionId, oldAggregateVersion, newAggregateVersion, e.getMessage(), e);
            throw e;
        } catch (Exception e) {
            logger.error("Exception during updateServiceSpec for id {} (old: {}) -> (new: {}): {}", apiVersionId, oldAggregateVersion, newAggregateVersion, e.getMessage(), e);
            throw e;
        }
    }

    @Override
    public Result<String> queryServiceEndpoint(int offset, int limit, String hostId, String apiVersionId, String apiId, String apiVersion,
                                               String endpoint, String method, String path, String desc) {
        Result<String> result = null;
        String s =
                """
                    SELECT COUNT(*) OVER () AS total,
                    e.host_id, e.endpoint_id, e.api_version_id, v.api_id, v.api_version,
                    e.endpoint, e.http_method, e.endpoint_path, e.endpoint_desc, e.aggregate_version
                    FROM api_endpoint_t e
                    INNER JOIN api_version_t v ON e.api_version_id = v.api_version_id
                    WHERE e.host_id = ? AND e.api_version_id = ?
                """;

        StringBuilder sqlBuilder = new StringBuilder();
        List<Object> parameters = new ArrayList<>();
        parameters.add(UUID.fromString(hostId));
        parameters.add(UUID.fromString(apiVersionId)); // ensure apiVersionId is converted to UUID

        StringBuilder whereClause = new StringBuilder();
        addCondition(whereClause, parameters, "v.api_id", apiId);
        addCondition(whereClause, parameters, "v.api_version", apiVersion);
        addCondition(whereClause, parameters, "e.endpoint", endpoint);
        addCondition(whereClause, parameters, "e.http_method", method);
        addCondition(whereClause, parameters, "e.endpoint_path", path);
        addCondition(whereClause, parameters, "e.endpoint_desc", desc);

        if (!whereClause.isEmpty()) {
            sqlBuilder.append(" AND ").append(whereClause);
        }


        sqlBuilder.append(" ORDER BY e.endpoint\n" +
                "LIMIT ? OFFSET ?");

        parameters.add(limit);
        parameters.add(offset);

        String sql = s + sqlBuilder.toString(); // Append the WHERE and ORDER BY clauses to the initial select
        int total = 0;
        List<Map<String, Object>> endpoints = new ArrayList<>();


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
                    map.put("endpointId", resultSet.getObject("endpoint_id", UUID.class));
                    map.put("apiVersionId", resultSet.getObject("api_version_id", UUID.class));
                    map.put("apiId", resultSet.getString("api_id"));
                    map.put("apiVersion", resultSet.getString("api_version"));
                    map.put("endpoint", resultSet.getString("endpoint"));
                    map.put("httpMethod", resultSet.getString("http_method"));
                    map.put("endpointPath", resultSet.getString("endpoint_path"));
                    map.put("endpointDesc", resultSet.getString("endpoint_desc"));
                    map.put("aggregateVersion", resultSet.getLong("aggregate_version"));
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
    public Result<String> queryEndpointScope(String hostId, String endpointId) {
        Result<String> result = null;
        String sql =
                """
                    SELECT s.host_id, s.endpoint_id, e.endpoint, s.scope, s.scope_desc, s.aggregate_version
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
                    map.put("aggregateVersion", resultSet.getLong("aggregate_version"));
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
    public Result<String> queryEndpointRule(String hostId, String apiId, String apiVersion, String endpoint) {
        Result<String> result = null;
        String sql =
                """
                    SELECT ae.host_id, ae.endpoint_id, a.api_id, av.api_version,
                    e.endpoint, r.rule_type, ae.rule_id, ae.aggregate_version
                    FROM api_endpoint_rule_t ae
                    INNER JOIN rule_t r ON ae.rule_id = r.rule_id
                    INNER JOIN api_endpoint_t e ON ae.endpoint_id = e.endpoint_id
                    INNER JOIN api_version_t av ON e.api_version_id = av.api_version_id
                    INNER JOIN api_t a ON av.api_id = a.api_id
                    WHERE ae.host_id = ?
                    AND a.api_id = ?
                    AND av.api_version = ?
                    AND e.endpoint = ?
                    ORDER BY r.rule_type
                """;

        List<Map<String, Object>> rules = new ArrayList<>();

        try (Connection connection = ds.getConnection();
             PreparedStatement preparedStatement = connection.prepareStatement(sql)) {
            preparedStatement.setObject(1, UUID.fromString(hostId));
            preparedStatement.setString(2, apiId);
            preparedStatement.setString(3, apiVersion);
            preparedStatement.setString(4, endpoint);
            try (ResultSet resultSet = preparedStatement.executeQuery()) {
                while (resultSet.next()) {
                    Map<String, Object> map = new HashMap<>();
                    map.put("hostId", resultSet.getObject("host_id", UUID.class));
                    map.put("endpointId", resultSet.getObject("endpoint_id", UUID.class));
                    map.put("apiId", resultSet.getString("api_id"));
                    map.put("apiVersion", resultSet.getString("api_version"));
                    map.put("endpoint", resultSet.getString("endpoint"));
                    map.put("ruleType", resultSet.getString("rule_type"));
                    map.put("ruleId", resultSet.getString("rule_id"));
                    map.put("aggregateVersion", resultSet.getLong("aggregate_version"));
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

    @Override
    public void deleteEndpointRule(Connection conn, Map<String, Object> event) throws SQLException, Exception {
        final String deleteApplication =
                """
                DELETE FROM api_endpoint_rule_t er
                WHERE er.host_id = ?
                  AND er.rule_id = ?
                  AND er.aggregate_version = ?
                  AND er.endpoint_id IN (
                    SELECT e.endpoint_id
                    FROM api_endpoint_t e
                    JOIN api_version_t v ON e.host_id = v.host_id
                                        AND e.api_version_id = v.api_version_id
                    WHERE e.host_id = ?
                      AND v.api_id = ?
                      AND v.api_version = ?
                      AND e.endpoint = ?
                  )
                """;
        Map<String, Object> map = (Map<String, Object>) event.get(PortalConstants.DATA);
        String endpoint = (String) map.get("endpoint");
        String ruleId = (String) map.get("ruleId");
        long oldAggregateVersion = SqlUtil.getOldAggregateVersion(event);
        try (PreparedStatement statement = conn.prepareStatement(deleteApplication)) {
            statement.setObject(1, UUID.fromString((String) event.get(Constants.HOST)));
            statement.setString(2, ruleId);
            statement.setLong(3, SqlUtil.getOldAggregateVersion(event));
            statement.setObject(4, UUID.fromString((String) event.get(Constants.HOST)));
            statement.setString(5, (String) map.get("apiId"));
            statement.setString(6, (String) map.get("apiVersion"));
            statement.setString(7, endpoint);

            int count = statement.executeUpdate();
            if (count == 0) {
                throw new SQLException(String.format("no record is deleted for endpoint rule for endpoint %s, ruleId %s and aggregateVersion", endpoint, ruleId, oldAggregateVersion));
            }
        } catch (SQLException e) {
            logger.error("SQLException during deleteEndpointRule for endpoint {} ruleId {} aggregateVersion {}: {}", endpoint, ruleId, oldAggregateVersion, e.getMessage(), e);
            throw e;
        } catch (Exception e) {
            logger.error("Exception during deleteEndpointRule for endpoint {} ruleId {} aggregateVersion {}: {}", endpoint, ruleId, oldAggregateVersion, e.getMessage(), e);
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
    public Result<String> queryServicePermission(String hostId, String apiId, String apiVersion) {
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
    public Result<List<String>> queryServiceFilter(String hostId, String apiId, String apiVersion) {
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
