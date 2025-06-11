package net.lightapi.portal.db.persistence;

import com.networknt.config.JsonMapper;
import com.networknt.monad.Failure;
import com.networknt.monad.Result;
import com.networknt.monad.Success;
import com.networknt.status.Status;
import com.networknt.utility.Constants;
import io.cloudevents.core.v1.CloudEventV1;
import net.lightapi.portal.PortalConstants;
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
    public Result<String> createService(Map<String, Object> event) {
        final String insertUser = "INSERT INTO api_t (host_id, api_id, api_name, " +
                "api_desc, operation_owner, delivery_owner, region, business_group, " +
                "lob, platform, capability, git_repo, api_tags, " +
                "api_status, update_user, update_ts) " +
                "VALUES (?, ?, ?, ?, ?,   ?, ?, ?, ?, ?,   ?, ?, ?, ?, ?,   ?)";
        Result<String> result = null;
        Map<String, Object> map = (Map<String, Object>)event.get(PortalConstants.DATA);
        try (Connection conn = ds.getConnection()) {
            conn.setAutoCommit(false);
            // no duplicate record, insert the user into database and write a success notification.
            try (PreparedStatement statement = conn.prepareStatement(insertUser)) {
                statement.setObject(1, UUID.fromString((String)map.get("hostId")));
                statement.setString(2, (String)map.get("apiId"));
                statement.setString(3, (String)map.get("apiName"));
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
                String operationOwner = (String)map.get("operationOwner");
                if(operationOwner != null && !operationOwner.trim().isEmpty()) {
                    statement.setObject(5, UUID.fromString(operationOwner));
                } else {
                    statement.setNull(5, Types.OTHER);
                }
                String deliveryOwner = (String) map.get("deliveryOwner");
                if(deliveryOwner != null && !deliveryOwner.trim().isEmpty()) {
                    statement.setObject(6, UUID.fromString(deliveryOwner));
                } else {
                    statement.setNull(6, Types.OTHER);
                }

                if (map.containsKey("region")) {
                    String region = (String) map.get("region");
                    if(region != null && !region.trim().isEmpty()) {
                        statement.setString(7, region);
                    } else {
                        statement.setNull(7, Types.VARCHAR);
                    }
                } else {
                    statement.setNull(7, Types.VARCHAR);
                }
                if (map.containsKey("businessGroup")) {
                    String businessGroup = (String) map.get("businessGroup");
                    if(businessGroup != null && !businessGroup.trim().isEmpty()) {
                        statement.setString(8, businessGroup);
                    } else {
                        statement.setNull(8, Types.VARCHAR);
                    }
                } else {
                    statement.setNull(8, Types.VARCHAR);
                }
                if (map.containsKey("lob")) {
                    String lob = (String) map.get("lob");
                    if(lob != null && !lob.trim().isEmpty()) {
                        statement.setString(9, lob);
                    } else {
                        statement.setNull(9, Types.VARCHAR);
                    }
                } else {
                    statement.setNull(9, Types.VARCHAR);
                }
                if (map.containsKey("platform")) {
                    String platform = (String) map.get("platform");
                    if(platform != null && !platform.trim().isEmpty()) {
                        statement.setString(10, platform);
                    } else {
                        statement.setNull(10, Types.VARCHAR);
                    }
                } else {
                    statement.setNull(10, Types.VARCHAR);
                }
                if (map.containsKey("capability")) {
                    String capability = (String) map.get("capability");
                    if(capability != null && !capability.trim().isEmpty()) {
                        statement.setString(11, capability);
                    } else {
                        statement.setNull(11, Types.VARCHAR);
                    }
                } else {
                    statement.setNull(11, Types.VARCHAR);
                }
                if (map.containsKey("gitRepo")) {
                    String gitRepo = (String) map.get("gitRepo");
                    if(gitRepo != null && !gitRepo.trim().isEmpty()) {
                        statement.setString(12, gitRepo);
                    } else {
                        statement.setNull(12, Types.VARCHAR);
                    }
                } else {
                    statement.setNull(12, Types.VARCHAR);
                }
                if (map.containsKey("apiTags")) {
                    String apiTags = (String) map.get("apiTags");
                    if(apiTags != null && !apiTags.trim().isEmpty()) {
                        statement.setString(13, apiTags);
                    } else {
                        statement.setNull(13, Types.VARCHAR);
                    }
                } else {
                    statement.setNull(13, Types.VARCHAR);
                }

                statement.setString(14, (String)map.get("apiStatus"));
                statement.setString(15, (String)event.get(Constants.USER));
                statement.setObject(16, OffsetDateTime.parse((String)event.get(CloudEventV1.TIME)));
                int count = statement.executeUpdate();
                if (count == 0) {
                    throw new SQLException(String.format("no record is inserted for api %s", map.get("apiId")));
                }
                conn.commit();
                result = Success.of((String)map.get("apiId"));
                notificationService.insertNotification(event, true, null);
            } catch (SQLException e) {
                logger.error("SQLException:", e);
                conn.rollback();
                notificationService.insertNotification(event, false, e.getMessage());
                result = Failure.of(new Status(SQL_EXCEPTION, e.getMessage()));
            } catch (Exception e) {
                logger.error("Exception:", e);
                conn.rollback();
                result = Failure.of(new Status(GENERIC_EXCEPTION, e.getMessage()));
            }
        } catch (SQLException e) {
            logger.error("SQLException:", e);
            result = Failure.of(new Status(SQL_EXCEPTION, e.getMessage()));
        }
        return result;
    }

    @Override
    public Result<String> updateService(Map<String, Object> event) {
        final String updateApi = "UPDATE api_t SET api_name = ?, api_desc = ? " +
                "operation_owner = ?, delivery_owner = ?, region = ?, business_group = ?, lob = ?, platform = ?, " +
                "capability = ?, git_repo = ?, api_tags = ?, api_status = ?,  update_user = ?, update_ts = ? " +
                "WHERE host_id = ? AND api_id = ?";

        Result<String> result = null;
        Map<String, Object> map = (Map<String, Object>)event.get(PortalConstants.DATA);
        try (Connection conn = ds.getConnection()) {
            conn.setAutoCommit(false);

            try (PreparedStatement statement = conn.prepareStatement(updateApi)) {
                statement.setString(1, (String)map.get("apiName"));
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
                if(operationOwner != null && !operationOwner.trim().isEmpty()) {
                    statement.setObject(3, UUID.fromString(operationOwner));
                } else {
                    statement.setNull(3, Types.OTHER);
                }

                String deliveryOwner = (String) map.get("deliveryOwner");
                if(deliveryOwner != null && !deliveryOwner.trim().isEmpty()) {
                    statement.setObject(4, UUID.fromString(deliveryOwner));
                } else {
                    statement.setNull(4, Types.OTHER);
                }

                if (map.containsKey("region")) {
                    String region = (String) map.get("region");
                    if(region != null && !region.trim().isEmpty()) {
                        statement.setString(5, region);
                    } else {
                        statement.setNull(5, Types.VARCHAR);
                    }
                } else {
                    statement.setNull(5, Types.VARCHAR);
                }
                if (map.containsKey("businessGroup")) {
                    String businessGroup = (String) map.get("businessGroup");
                    if(businessGroup != null && !businessGroup.trim().isEmpty()) {
                        statement.setString(6, businessGroup);
                    } else {
                        statement.setNull(6, Types.VARCHAR);
                    }
                } else {
                    statement.setNull(6, Types.VARCHAR);
                }
                if (map.containsKey("lob")) {
                    String lob = (String) map.get("lob");
                    if(lob != null && !lob.trim().isEmpty()) {
                        statement.setString(7, lob);
                    } else {
                        statement.setNull(7, Types.VARCHAR);
                    }
                } else {
                    statement.setNull(7, Types.VARCHAR);
                }
                if (map.containsKey("platform")) {
                    String platform = (String) map.get("platform");
                    if(platform != null && !platform.trim().isEmpty()) {
                        statement.setString(8, platform);
                    } else {
                        statement.setNull(8, Types.VARCHAR);
                    }
                } else {
                    statement.setNull(8, Types.VARCHAR);
                }
                if (map.containsKey("capability")) {
                    String capability = (String) map.get("capability");
                    if(capability != null && !capability.trim().isEmpty()) {
                        statement.setString(9, capability);
                    } else {
                        statement.setNull(9, Types.VARCHAR);
                    }
                } else {
                    statement.setNull(9, Types.VARCHAR);
                }
                if (map.containsKey("gitRepo")) {
                    String gitRepo = (String) map.get("gitRepo");
                    if(gitRepo != null && !gitRepo.trim().isEmpty()) {
                        statement.setString(10, gitRepo);
                    } else {
                        statement.setNull(10, Types.VARCHAR);
                    }
                } else {
                    statement.setNull(10, Types.VARCHAR);
                }
                if (map.containsKey("apiTags")) {
                    String apiTags = (String) map.get("apiTags");
                    if(apiTags != null && !apiTags.trim().isEmpty()) {
                        statement.setString(11, apiTags);
                    } else {
                        statement.setNull(11, Types.VARCHAR);
                    }
                } else {
                    statement.setNull(11, Types.VARCHAR);
                }
                statement.setString(12, (String)map.get("apiStatus"));
                statement.setString(13, (String)event.get(Constants.USER));
                statement.setObject(14, OffsetDateTime.parse((String)event.get(CloudEventV1.TIME)));
                statement.setObject(15, UUID.fromString((String)map.get("hostId")));
                statement.setString(16, (String)map.get("apiId"));

                int count = statement.executeUpdate();
                if (count == 0) {
                    // no record is updated, write an error notification.
                    throw new SQLException(String.format("no record is updated for api %s", map.get("apiId")));
                }
                conn.commit();
                result = Success.of((String)map.get("apiId"));
                notificationService.insertNotification(event, true, null);
            } catch (SQLException e) {
                logger.error("SQLException:", e);
                conn.rollback();
                notificationService.insertNotification(event, false, e.getMessage());
                result = Failure.of(new Status(SQL_EXCEPTION, e.getMessage()));
            } catch (Exception e) {
                logger.error("Exception:", e);
                conn.rollback();
                notificationService.insertNotification(event, false, e.getMessage());
                result = Failure.of(new Status(GENERIC_EXCEPTION, e.getMessage()));
            }
        } catch (SQLException e) {
            logger.error("SQLException:", e);
            result = Failure.of(new Status(SQL_EXCEPTION, e.getMessage()));
        }
        return result;

    }

    @Override
    public Result<String> deleteService(Map<String, Object> event) {
        final String deleteApplication = "DELETE from api_t WHERE host_id = ? AND api_id = ?";
        Result<String> result;
        Map<String, Object> map = (Map<String, Object>)event.get(PortalConstants.DATA);
        try (Connection conn = ds.getConnection()) {
            conn.setAutoCommit(false);
            try (PreparedStatement statement = conn.prepareStatement(deleteApplication)) {
                statement.setObject(1, UUID.fromString((String)map.get("hostId")));
                statement.setString(2, (String)map.get("apiId"));
                int count = statement.executeUpdate();
                if (count == 0) {
                    // no record is deleted, write an error notification.
                    throw new SQLException(String.format("no record is deleted for api %s", map.get("apiId")));
                }
                conn.commit();
                result = Success.of((String)map.get("apiId"));
                notificationService.insertNotification(event, true, null);
            } catch (SQLException e) {
                logger.error("SQLException:", e);
                conn.rollback();
                notificationService.insertNotification(event, false, e.getMessage());
                result = Failure.of(new Status(SQL_EXCEPTION, e.getMessage()));
            } catch (Exception e) {
                logger.error("Exception:", e);
                conn.rollback();
                notificationService.insertNotification(event, false, e.getMessage());
                result = Failure.of(new Status(GENERIC_EXCEPTION, e.getMessage()));
            }
        } catch (SQLException e) {
            logger.error("SQLException:", e);
            result = Failure.of(new Status(SQL_EXCEPTION, e.getMessage()));
        }
        return result;
    }

    @Override
    public Result<String> queryService(int offset, int limit, String hostId, String apiId, String apiName,
                                       String apiDesc, String operationOwner, String deliveryOwner, String region, String businessGroup,
                                       String lob, String platform, String capability, String gitRepo, String apiTags, String apiStatus) {
        Result<String> result = null;
        StringBuilder sqlBuilder = new StringBuilder();
        sqlBuilder.append("SELECT COUNT(*) OVER () AS total,\n" +
                "host_id, api_id, api_name,\n" +
                "api_desc, operation_owner, delivery_owner, region, business_group,\n" +
                "lob, platform, capability, git_repo, api_tags, api_status\n" +
                "FROM api_t\n" +
                "WHERE host_id = ?\n");


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
                    map.put("label", resultSet.getString("api_id") + "|" + resultSet.getString("api_version") + "|"  + resultSet.getString("api_name"));
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
        String sql = "SELECT endpoint_id FROM api_endpoint_t WHERE host_id = ? AND api_version_id = ?";
        List<Map<String, Object>> labels = new ArrayList<>();
        try (Connection connection = ds.getConnection();
             PreparedStatement preparedStatement = connection.prepareStatement(sql)) {
            preparedStatement.setObject(1, UUID.fromString(hostId));
            preparedStatement.setObject(2, UUID.fromString(apiId));
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
    public Result<String> createServiceVersion(Map<String, Object> event, List<Map<String, Object>> endpoints) {
        final String insertUser = "INSERT INTO api_version_t (host_id, api_version_id, api_id, api_version, api_type, service_id, api_version_desc, " +
                "spec_link, spec, update_user, update_ts) " +
                "VALUES (?, ?, ?, ?, ?,   ?, ?, ?, ?, ?,  ?)";
        final String insertEndpoint = "INSERT INTO api_endpoint_t (host_id, endpoint_id, api_version_id, endpoint, http_method, " +
                "endpoint_path, endpoint_name, endpoint_desc, update_user, update_ts) " +
                "VALUES (?,? ,?, ?, ?,  ?, ?, ?, ?, ?)";
        final String insertScope = "INSERT INTO api_endpoint_scope_t (host_id, endpoint_id, scope, scope_desc, " +
                "update_user, update_ts) " +
                "VALUES (?, ?, ?, ?, ?,  ?)";

        Result<String> result = null;
        Map<String, Object> map = (Map<String, Object>)event.get(PortalConstants.DATA);
        try (Connection conn = ds.getConnection()) {
            conn.setAutoCommit(false);
            // no duplicate record, insert the user into database and write a success notification.
            try (PreparedStatement statement = conn.prepareStatement(insertUser)) {
                statement.setObject(1, UUID.fromString((String)map.get("hostId")));
                statement.setObject(2, UUID.fromString((String)map.get("apiVersionId")));
                statement.setString(3, (String)map.get("apiId"));
                statement.setString(4, (String)map.get("apiVersion"));
                statement.setString(5, (String)map.get("apiType"));
                statement.setString(6, (String)map.get("serviceId"));

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
                statement.setString(10, (String)event.get(Constants.USER));
                statement.setObject(11, OffsetDateTime.parse((String)event.get(CloudEventV1.TIME)));
                int count = statement.executeUpdate();
                if (count == 0) {
                    throw new SQLException(String.format("no record is inserted for api version %s", "hostId " + map.get("hostId") + " apiId " + map.get("apiId") + " apiVersion " + map.get("apiVersion")));
                }
                if(endpoints != null && !endpoints.isEmpty()) {
                    // insert endpoints
                    for (Map<String, Object> endpoint : endpoints) {
                        try (PreparedStatement statementInsert = conn.prepareStatement(insertEndpoint)) {
                            statementInsert.setObject(1, UUID.fromString((String)map.get("hostId")));
                            statementInsert.setObject(2, UUID.fromString((String)endpoint.get("endpointId")));
                            statementInsert.setObject(3, UUID.fromString((String)map.get("apiVersionId")));
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

                            statementInsert.setString(9, (String)event.get(Constants.USER));
                            statementInsert.setObject(10, OffsetDateTime.parse((String)event.get(CloudEventV1.TIME)));
                            statementInsert.executeUpdate();
                        }
                        // insert scopes
                        List<String> scopes = (List<String>) endpoint.get("scopes");
                        if(scopes != null && !scopes.isEmpty()) {
                            for (String scope : scopes) {
                                String[] scopeDesc = scope.split(":");
                                try (PreparedStatement statementScope = conn.prepareStatement(insertScope)) {
                                    statementScope.setObject(1, UUID.fromString((String)map.get("hostId")));
                                    statementScope.setObject(2, UUID.fromString((String)endpoint.get("endpointId")));
                                    statementScope.setString(3, scopeDesc[0]);
                                    if (scopeDesc.length == 1)
                                        statementScope.setNull(4, NULL);
                                    else
                                        statementScope.setString(4, scopeDesc[1]);
                                    statementScope.setString(5, (String)event.get(Constants.USER));
                                    statementScope.setObject(6, OffsetDateTime.parse((String)event.get(CloudEventV1.TIME)));
                                    statementScope.executeUpdate();
                                }
                            }
                        }
                    }
                }
                conn.commit();
                result = Success.of((String)map.get("apiId"));
                notificationService.insertNotification(event, true, null);
            } catch (SQLException e) {
                logger.error("SQLException:", e);
                conn.rollback();
                notificationService.insertNotification(event, false, e.getMessage());
                result = Failure.of(new Status(SQL_EXCEPTION, e.getMessage()));
            } catch (Exception e) {
                logger.error("Exception:", e);
                conn.rollback();
                notificationService.insertNotification(event, false, e.getMessage());
                result = Failure.of(new Status(GENERIC_EXCEPTION, e.getMessage()));
            }
        } catch (SQLException e) {
            logger.error("SQLException:", e);
            result = Failure.of(new Status(SQL_EXCEPTION, e.getMessage()));
        }
        return result;
    }

    @Override
    public Result<String> updateServiceVersion(Map<String, Object> event, List<Map<String, Object>> endpoints) {
        final String updateApi = "UPDATE api_version_t SET api_id = ?, api_version = ?, api_type = ?, service_id = ?, " +
                "api_version_desc = ?, spec_link = ?,  spec = ?," +
                "update_user = ?, update_ts = ? " +
                "WHERE host_id = ? AND api_version_id = ?";
        final String deleteEndpoint = "DELETE FROM api_endpoint_t WHERE host_id = ? AND api_version_id = ?";
        final String insertEndpoint = "INSERT INTO api_endpoint_t (host_id, endpoint_id, api_version_id, endpoint, http_method, " +
                "endpoint_path, endpoint_name, endpoint_desc, update_user, update_ts) " +
                "VALUES (?,? ,?, ?, ?,  ?, ?, ?, ?, ?)";
        final String insertScope = "INSERT INTO api_endpoint_scope_t (host_id, endpoint_id, scope, scope_desc, " +
                "update_user, update_ts) " +
                "VALUES (?, ?, ?, ?, ?,  ?)";

        Result<String> result = null;
        Map<String, Object> map = (Map<String, Object>)event.get(PortalConstants.DATA);
        try (Connection conn = ds.getConnection()) {
            conn.setAutoCommit(false);

            try (PreparedStatement statement = conn.prepareStatement(updateApi)) {
                statement.setString(1, (String)map.get("apiId"));
                statement.setString(2, (String)map.get("apiVersion"));
                statement.setString(3, (String)map.get("apiType"));
                statement.setString(4, (String)map.get("serviceId"));

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

                statement.setString(8, (String)event.get(Constants.USER));
                statement.setObject(9, OffsetDateTime.parse((String)event.get(CloudEventV1.TIME)));
                statement.setObject(10, UUID.fromString((String)map.get("hostId")));
                statement.setObject(11, UUID.fromString((String)map.get("apiVersionId")));

                int count = statement.executeUpdate();
                if (count == 0) {
                    throw new SQLException(String.format("no record is updated for api version %s", "hostId " + (event.get(Constants.HOST) + " apiVersionId " + map.get("apiVersionId"))));
                }
                if(endpoints != null && !endpoints.isEmpty()) {
                    // delete endpoints for the api version. the api_endpoint_scope_t will be deleted by the cascade.
                    try (PreparedStatement statementDelete = conn.prepareStatement(deleteEndpoint)) {
                        statementDelete.setObject(1, UUID.fromString((String)map.get("hostId")));
                        statementDelete.setObject(2, UUID.fromString((String)map.get("apiVersionId")));
                        statementDelete.executeUpdate();
                    }
                    // insert endpoints
                    for (Map<String, Object> endpoint : endpoints) {
                        try (PreparedStatement statementInsert = conn.prepareStatement(insertEndpoint)) {
                            statementInsert.setObject(1, UUID.fromString((String)map.get("hostId")));
                            statementInsert.setObject(2, UUID.fromString((String)endpoint.get("endpointId")));
                            statementInsert.setObject(3, UUID.fromString((String)map.get("apiVersionId")));
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

                            statementInsert.setString(9, (String)event.get(Constants.USER));
                            statementInsert.setObject(10, OffsetDateTime.parse((String)event.get(CloudEventV1.TIME)));
                            statementInsert.executeUpdate();
                        }
                        // insert scopes
                        List<String> scopes = (List<String>) endpoint.get("scopes");
                        if (scopes != null && !scopes.isEmpty()) {
                            for (String scope : scopes) {
                                String[] scopeDesc = scope.split(":");
                                try (PreparedStatement statementScope = conn.prepareStatement(insertScope)) {
                                    statementScope.setObject(1, UUID.fromString((String)map.get("hostId")));
                                    statementScope.setObject(2, UUID.fromString((String)endpoint.get("endpointId")));
                                    statementScope.setString(3, scopeDesc[0]);
                                    if (scopeDesc.length == 1)
                                        statementScope.setNull(4, NULL);
                                    else
                                        statementScope.setString(4, scopeDesc[1]);
                                    statementScope.setString(5, (String)event.get(Constants.USER));
                                    statementScope.setObject(6, OffsetDateTime.parse((String)event.get(CloudEventV1.TIME)));
                                    statementScope.executeUpdate();
                                }
                            }
                        }
                    }
                }
                conn.commit();
                result = Success.of((String)map.get("apiId"));
                notificationService.insertNotification(event, true, null);
            } catch (SQLException e) {
                logger.error("SQLException:", e);
                conn.rollback();
                notificationService.insertNotification(event, false, e.getMessage());
                result = Failure.of(new Status(SQL_EXCEPTION, e.getMessage()));
            } catch (Exception e) {
                logger.error("Exception:", e);
                conn.rollback();
                notificationService.insertNotification(event, false, e.getMessage());
                result = Failure.of(new Status(GENERIC_EXCEPTION, e.getMessage()));
            }
        } catch (SQLException e) {
            logger.error("SQLException:", e);
            result = Failure.of(new Status(SQL_EXCEPTION, e.getMessage()));
        }
        return result;

    }

    @Override
    public Result<String> deleteServiceVersion(Map<String, Object> event) {
        final String deleteApplication = "DELETE from api_version_t WHERE host_id = ? AND api_version_id = ?";
        Result<String> result;
        Map<String, Object> map = (Map<String, Object>)event.get(PortalConstants.DATA);
        try (Connection conn = ds.getConnection()) {
            conn.setAutoCommit(false);
            try (PreparedStatement statement = conn.prepareStatement(deleteApplication)) {
                statement.setObject(1, UUID.fromString((String)map.get("hostId")));
                statement.setObject(2, UUID.fromString((String)map.get("apiVersionId")));
                int count = statement.executeUpdate();
                if (count == 0) {
                    // no record is deleted, write an error notification.
                    throw new SQLException(String.format("no record is deleted for api version %s", "hostId " + map.get("hostId") + " apiVersionId " + map.get("apiVersionId")));
                }
                conn.commit();
                result = Success.of((String)event.get(Constants.USER));
                notificationService.insertNotification(event, true, null);
            } catch (SQLException e) {
                logger.error("SQLException:", e);
                conn.rollback();
                notificationService.insertNotification(event, false, e.getMessage());
                result = Failure.of(new Status(SQL_EXCEPTION, e.getMessage()));
            } catch (Exception e) {
                logger.error("Exception:", e);
                conn.rollback();
                notificationService.insertNotification(event, false, e.getMessage());
                result = Failure.of(new Status(GENERIC_EXCEPTION, e.getMessage()));
            }

        } catch (SQLException e) {
            logger.error("SQLException:", e);
            result = Failure.of(new Status(SQL_EXCEPTION, e.getMessage()));
        }
        return result;

    }

    @Override
    public Result<String> queryServiceVersion(String hostId, String apiId) {
        Result<String> result = null;
        String sql = """
                SELECT host_id, api_version_id, api_id, api_version, api_type,
                service_id, api_version_desc, spec_link, spec
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
    public Result<String> updateServiceSpec(Map<String, Object> event, List<Map<String, Object>> endpoints) {
        if(logger.isTraceEnabled()) logger.trace("endpoints = {}", endpoints);
        final String updateApiVersion =
                """
                    UPDATE api_version_t SET spec = ?,
                    update_user = ?, update_ts = ?
                    WHERE host_id = ? AND api_version_id = ?
                """;
        final String deleteEndpoint = "DELETE FROM api_endpoint_t WHERE host_id = ? AND api_version_id = ?";
        final String insertEndpoint = "INSERT INTO api_endpoint_t (host_id, endpoint_id, api_version_id, endpoint, http_method, " +
                "endpoint_path, endpoint_name, endpoint_desc, update_user, update_ts) " +
                "VALUES (?,? ,?, ?, ?,   ?, ?, ?, ?, ?)";
        final String insertScope = "INSERT INTO api_endpoint_scope_t (host_id, endpoint_id, scope, scope_desc, " +
                "update_user, update_ts) " +
                "VALUES (?, ?, ?, ?, ?,  ?)";


        Result<String> result = null;
        Map<String, Object> map = (Map<String, Object>)event.get(PortalConstants.DATA);
        try (Connection conn = ds.getConnection()) {
            conn.setAutoCommit(false);
            try {
                // update spec
                try (PreparedStatement statement = conn.prepareStatement(updateApiVersion)) {
                    statement.setString(1, (String)map.get("spec"));
                    statement.setString(2, (String)event.get(Constants.USER));
                    statement.setObject(3, OffsetDateTime.parse((String)event.get(CloudEventV1.TIME)));
                    statement.setObject(4, UUID.fromString((String)map.get("hostId")));
                    statement.setObject(5, UUID.fromString((String)map.get("apiVersionId")));

                    int count = statement.executeUpdate();
                    if (count == 0) {
                        // no record is updated, write an error notification.
                        throw new SQLException(String.format("no record is updated for api version " + " hostId " + map.get("hostId") + " apiId " + map.get("apiId") + " apiVersion " + map.get("apiVersion")));
                    }
                }
                // delete endpoints for the api version. the api_endpoint_scope_t will be deleted by the cascade.
                try (PreparedStatement statement = conn.prepareStatement(deleteEndpoint)) {
                    statement.setObject(1, UUID.fromString((String)map.get("hostId")));
                    statement.setObject(2, UUID.fromString((String)map.get("apiVersionId")));
                    statement.executeUpdate();
                }
                // insert endpoints
                for (Map<String, Object> endpoint : endpoints) {
                    try (PreparedStatement statement = conn.prepareStatement(insertEndpoint)) {
                        statement.setObject(1, UUID.fromString((String)map.get("hostId")));
                        statement.setObject(2, UUID.fromString((String)endpoint.get("endpointId")));
                        statement.setObject(3, UUID.fromString((String)map.get("apiVersionId")));
                        statement.setString(4, (String)endpoint.get("endpoint"));

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

                        statement.setString(9, (String)event.get(Constants.USER));
                        statement.setObject(10, OffsetDateTime.parse((String)event.get(CloudEventV1.TIME)));
                        statement.executeUpdate();
                    }
                    // insert scopes
                    List<String> scopes = (List<String>) endpoint.get("scopes");
                    if(scopes != null && !scopes.isEmpty()) {
                        for (String scope : scopes) {
                            String[] scopeDesc = scope.split(":");
                            try (PreparedStatement statement = conn.prepareStatement(insertScope)) {
                                statement.setObject(1, UUID.fromString((String)map.get("hostId")));
                                statement.setObject(2, UUID.fromString((String)endpoint.get("endpointId")));
                                statement.setString(3, scopeDesc[0]);
                                if (scopeDesc.length == 1)
                                    statement.setNull(4, NULL);
                                else
                                    statement.setString(4, scopeDesc[1]);
                                statement.setString(5, (String)event.get(Constants.USER));
                                statement.setObject(6, OffsetDateTime.parse((String)event.get(CloudEventV1.TIME)));
                                statement.executeUpdate();
                            }
                        }
                    }
                }
                conn.commit();
                result = Success.of((String)map.get("apiId"));
                notificationService.insertNotification(event, true, null);
            } catch (SQLException e) {
                logger.error("SQLException:", e);
                conn.rollback();
                notificationService.insertNotification(event, false, e.getMessage());
                result = Failure.of(new Status(SQL_EXCEPTION, e.getMessage()));
            } catch (Exception e) {
                logger.error("Exception:", e);
                conn.rollback();
                notificationService.insertNotification(event, false, e.getMessage());
                result = Failure.of(new Status(GENERIC_EXCEPTION, e.getMessage()));
            }
        } catch (SQLException e) {
            logger.error("SQLException:", e);
            result = Failure.of(new Status(SQL_EXCEPTION, e.getMessage()));
        }
        return result;

    }

    @Override
    public Result<String> queryServiceEndpoint(int offset, int limit, String hostId, String apiVersionId, String apiId, String apiVersion,
                                               String endpoint, String method, String path, String desc) {
        Result<String> result = null;
        String s =
                """
                    SELECT COUNT(*) OVER () AS total,
                    e.host_id, e.endpoint_id, e.api_version_id, v.api_id, v.api_version,
                    e.endpoint, e.http_method, e.endpoint_path, e.endpoint_desc
                    FROM api_endpoint_t e
                    INNER JOIN api_version_t v ON e.api_version_id = v.api_version_id
                    WHERE e.host_id = ? AND e.api_version_id = ?
                """;

        StringBuilder sqlBuilder = new StringBuilder();
        List<Object> parameters = new ArrayList<>();
        parameters.add(UUID.fromString(hostId));
        parameters.add(apiVersionId);

        StringBuilder whereClause = new StringBuilder();
        addCondition(whereClause, parameters, "v.api_id", apiId);
        addCondition(whereClause, parameters, "v.api_version", apiVersion);
        addCondition(whereClause, parameters, "e.endpoint", endpoint);
        addCondition(whereClause, parameters, "e.http_method", method);
        addCondition(whereClause, parameters, "e.endpoint_path", path);
        addCondition(whereClause, parameters, "e.endpoint_desc", desc);

        if(!whereClause.isEmpty()) {
            sqlBuilder.append(" AND ").append(whereClause);
        }


        sqlBuilder.append(" ORDER BY e.endpoint\n" +
                "LIMIT ? OFFSET ?");

        parameters.add(limit);
        parameters.add(offset);

        String sql = sqlBuilder.toString();
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
                    SELECT s.host_id, s.endpoint_id, e.endpoint, s.scope, s.scope_desc
                    FROM api_endpoint_scope_t s
                    INNER JOIN api_endpoint_t e ON e.host_id = s.host_id AND e.endpoint_id = s.endpoint_id
                    WHERE host_id = ?
                    AND endpoint_id = ?
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
                    SELECT ae.host_id, ae.endpoint_id, a.api_id, av.api_version, e.endpoint, r.rule_type, ae.rule_id
                    FROM api_endpoint_rule_t ae
                    INNER JOIN rule_t r ON ae.rule_id = r.rule_id
                    INNER JOIN api_endpoint_t e ON ae.endpoint_id = e.endpoint_id
                    INNER JOIN api_version_t av ON e.api_version_id = av.api_version_id
                    INNER JOIN api_t a ON av.api_id = a.api_id\s
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
    public Result<String> createEndpointRule(Map<String, Object> event) {
        final String insertUser =
                """
                INSERT INTO api_endpoint_rule_t (host_id, endpoint_id, rule_id,
                update_user, update_ts)
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
        Result<String> result = null;
        Map<String, Object> map = (Map<String, Object>)event.get(PortalConstants.DATA);
        try (Connection conn = ds.getConnection()) {
            conn.setAutoCommit(false);
            // no duplicate record, insert the user into database and write a success notification.
            try (PreparedStatement statement = conn.prepareStatement(insertUser)) {
                statement.setObject(1, UUID.fromString((String)event.get(Constants.HOST)));
                statement.setObject(2, UUID.fromString((String)event.get(Constants.HOST)));
                statement.setString(3, (String)map.get("apiId"));
                statement.setString(4, (String)map.get("apiVersion"));
                statement.setString(5, (String)map.get("endpoint"));
                statement.setString(6, (String)map.get("ruleId"));
                statement.setString(7, (String)event.get(Constants.USER));
                statement.setObject(8, OffsetDateTime.parse((String)event.get(CloudEventV1.TIME)));
                int count = statement.executeUpdate();
                if (count == 0) {
                    throw new SQLException(String.format("no record is inserted for api version " + "hostId " + event.get(Constants.HOST) + " endpoint " + map.get("endpoint")));
                }
                conn.commit();
                result = Success.of((String)map.get("endpointId"));
                notificationService.insertNotification(event, true, null);
            } catch (SQLException e) {
                logger.error("SQLException:", e);
                conn.rollback();
                notificationService.insertNotification(event, false, e.getMessage());
                result = Failure.of(new Status(SQL_EXCEPTION, e.getMessage()));
            } catch (Exception e) {
                logger.error("Exception:", e);
                conn.rollback();
                notificationService.insertNotification(event, false, e.getMessage());
                result = Failure.of(new Status(GENERIC_EXCEPTION, e.getMessage()));
            }
        } catch (SQLException e) {
            logger.error("SQLException:", e);
            result = Failure.of(new Status(SQL_EXCEPTION, e.getMessage()));
        }
        return result;
    }

    @Override
    public Result<String> deleteEndpointRule(Map<String, Object> event) {
        final String deleteApplication =
                """
                DELETE FROM api_endpoint_rule_t er
                WHERE er.host_id = ?
                  AND er.rule_id = ?
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
        Result<String> result;
        Map<String, Object> map = (Map<String, Object>)event.get(PortalConstants.DATA);
        try (Connection conn = ds.getConnection()) {
            conn.setAutoCommit(false);
            try (PreparedStatement statement = conn.prepareStatement(deleteApplication)) {
                statement.setObject(1, UUID.fromString((String)event.get(Constants.HOST)));
                statement.setString(2, (String)map.get("ruleId"));
                statement.setObject(3, UUID.fromString((String)event.get(Constants.HOST)));
                statement.setString(4, (String)map.get("apiId"));
                statement.setString(5, (String)map.get("apiVersion"));
                statement.setString(6, (String)map.get("endpoint"));

                int count = statement.executeUpdate();
                if (count == 0) {
                    // no record is deleted, write an error notification.
                    throw new SQLException(String.format("no record is deleted for endpoint rule " + "hostId " + event.get(Constants.HOST) + " endpoint " + map.get("endpoint")));
                }
                conn.commit();
                result = Success.of((String)event.get(Constants.USER));
                notificationService.insertNotification(event, true, null);
            } catch (SQLException e) {
                logger.error("SQLException:", e);
                conn.rollback();
                notificationService.insertNotification(event, false, e.getMessage());
                result = Failure.of(new Status(SQL_EXCEPTION, e.getMessage()));
            } catch (Exception e) {
                logger.error("Exception:", e);
                conn.rollback();
                notificationService.insertNotification(event, false, e.getMessage());
                result = Failure.of(new Status(GENERIC_EXCEPTION, e.getMessage()));
            }

        } catch (SQLException e) {
            logger.error("SQLException:", e);
            result = Failure.of(new Status(SQL_EXCEPTION, e.getMessage()));
        }
        return result;
    }

    @Override
    public Result<String> queryServiceRule(String hostId, String apiId, String apiVersion) {
        Result<String> result = null;
        String sql =
                """
                SELECT ae.host_id, a.api_id, av.api_version, e.endpoint, r.rule_type, ae.rule_id
                FROM api_endpoint_rule_t ae
                INNER JOIN rule_t r ON ae.rule_id = r.rule_id
                INNER JOIN api_endpoint_t e ON e.endpoint_id = ae.endpoint_id
                INNER JOIN api_version_t av ON av.api_version_id = e.api_version_id
                INNER JOIN api_t a ON a.api_id = av.api_id
                WHERE a.host_id =?
                AND a.api_id = ?
                AND a.api_version = ?
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
                    map.put("apiId", resultSet.getString("api_id"));
                    map.put("apiVersion", resultSet.getString("api_version"));
                    map.put("endpoint", resultSet.getString("endpoint"));
                    map.put("ruleType", resultSet.getString("rule_type"));
                    String ruleId = resultSet.getString("rule_id");
                    map.put("ruleId", ruleId);
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
                "                        WHERE rp.host_id = ?\n" +
                "                        AND rp.api_id = ?\n" +
                "                        AND rp.api_version = ?\n" +
                "                        AND rp.endpoint = ae.endpoint\n" +
                "                    ), '[]'),\n" +
                "                    'positions', COALESCE((\n" +
                "                        SELECT JSON_ARRAYAGG(\n" +
                "                             JSON_BUILD_OBJECT(\n" +
                "                                'positionId', pp.position_id\n" +
                "                             )\n" +
                "                         )\n" +
                "                        FROM position_permission_t pp\n" +
                "                        WHERE pp.host_id = ?\n" +
                "                        AND pp.api_id = ?\n" +
                "                        AND pp.api_version = ?\n" +
                "                        AND pp.endpoint = ae.endpoint\n" +
                "                    ), '[]'),\n" +
                "                    'groups', COALESCE((\n" +
                "                        SELECT JSON_ARRAYAGG(\n" +
                "                            JSON_BUILD_OBJECT(\n" +
                "                               'groupId', gp.group_id\n" +
                "                            )\n" +
                "                        )\n" +
                "                        FROM group_permission_t gp\n" +
                "                        WHERE gp.host_id = ?\n" +
                "                        AND gp.api_id = ?\n" +
                "                        AND gp.api_version = ?\n" +
                "                        AND gp.endpoint = ae.endpoint\n" +
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
                "                        WHERE ap.attribute_id = a.attribute_id\n" +
                "                        AND ap.host_id = ?\n" +
                "                        AND ap.api_id = ?\n" +
                "                        AND ap.api_version = ?\n" +
                "                        AND ap.endpoint = ae.endpoint\n" +
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
                "                        WHERE up.host_id = ?\n" +
                "                        AND up.api_id = ?\n" +
                "                        AND up.api_version = ?\n" +
                "                        AND up.endpoint = ae.endpoint\n" +
                "                    ), '[]')\n" +
                "                )\n" +
                "            )\n" +
                "        ELSE NULL\n" +
                "    END AS permissions\n" +
                "FROM\n" +
                "    api_endpoint_t ae\n" +
                "WHERE\n" +
                "    ae.host_id = ?\n" +
                "    AND ae.api_id = ?\n" +
                "    AND ae.api_version = ?;\n";

        try (Connection connection = ds.getConnection();
             PreparedStatement preparedStatement = connection.prepareStatement(sql)) {
            preparedStatement.setObject(1, UUID.fromString(hostId));
            preparedStatement.setString(2, apiId);
            preparedStatement.setString(3, apiVersion);
            preparedStatement.setObject(4, UUID.fromString(hostId));
            preparedStatement.setString(5, apiId);
            preparedStatement.setString(6, apiVersion);
            preparedStatement.setObject(7, UUID.fromString(hostId));
            preparedStatement.setString(8, apiId);
            preparedStatement.setString(9, apiVersion);
            preparedStatement.setObject(10, UUID.fromString(hostId));
            preparedStatement.setString(11, apiId);
            preparedStatement.setString(12, apiVersion);
            preparedStatement.setObject(13, UUID.fromString(hostId));
            preparedStatement.setString(14, apiId);
            preparedStatement.setString(15, apiVersion);
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
        String sql = "SELECT\n" +
                "    JSON_BUILD_OBJECT(\n" +
                "        'role_row', JSON_AGG(\n" +
                "            JSON_BUILD_OBJECT(\n" +
                "                'endpoint', endpoint,\n" +
                "                'roleId', role_id,\n" +
                "                'colName', col_name,\n" +
                "                'operator', operator,\n" +
                "                'colValue', col_value\n" +
                "            )\n" +
                "        )\n" +
                "    ) AS result\n" +
                "FROM\n" +
                "    role_row_filter_t\n" +
                "WHERE\n" +
                "    host_id = ?\n" +
                "    AND api_id = ?\n" +
                "    AND api_version = ?\n" +
                "GROUP BY ()\n" +
                "HAVING COUNT(*) > 0 \n" +
                "UNION ALL\n" +
                "SELECT\n" +
                "    JSON_BUILD_OBJECT(\n" +
                "        'role_col', JSON_AGG(\n" +
                "            JSON_BUILD_OBJECT(\n" +
                "                'endpoint', endpoint,\n" +
                "                'roleId', role_id,\n" +
                "                'columns', columns\n" +
                "            )\n" +
                "        )\n" +
                "    ) AS result\n" +
                "FROM\n" +
                "    role_col_filter_t\n" +
                "WHERE\n" +
                "    host_id = ?\n" +
                "    AND api_id = ?\n" +
                "    AND api_version = ?\n" +
                "GROUP BY ()\n" +
                "HAVING COUNT(*) > 0\n" +
                "UNION ALL\n" +
                "SELECT\n" +
                "    JSON_BUILD_OBJECT(\n" +
                "        'group_row', JSON_AGG(\n" +
                "            JSON_BUILD_OBJECT(\n" +
                "                'endpoint', endpoint,\n" +
                "                'groupId', group_id,\n" +
                "                'colName', col_name,\n" +
                "                'operator', operator,\n" +
                "                'colValue', col_value\n" +
                "            )\n" +
                "        )\n" +
                "    ) AS result\n" +
                "FROM\n" +
                "    group_row_filter_t\n" +
                "WHERE\n" +
                "    host_id = ?\n" +
                "    AND api_id = ?\n" +
                "    AND api_version = ?\n" +
                "GROUP BY ()\n" +
                "HAVING COUNT(*) > 0 \n" +
                "UNION ALL\n" +
                "SELECT\n" +
                "    JSON_BUILD_OBJECT(\n" +
                "        'group_col', JSON_AGG(\n" +
                "            JSON_BUILD_OBJECT(\n" +
                "                'endpoint', endpoint,\n" +
                "                'groupId', group_id,\n" +
                "                'columns', columns\n" +
                "            )\n" +
                "        )\n" +
                "    ) AS result\n" +
                "FROM\n" +
                "    group_col_filter_t\n" +
                "WHERE\n" +
                "    host_id = ?\n" +
                "    AND api_id = ?\n" +
                "    AND api_version = ?\n" +
                "GROUP BY ()\n" +
                "HAVING COUNT(*) > 0\n" +
                "UNION ALL\n" +
                "SELECT\n" +
                "    JSON_BUILD_OBJECT(\n" +
                "        'position_row', JSON_AGG(\n" +
                "            JSON_BUILD_OBJECT(\n" +
                "                'endpoint', endpoint,\n" +
                "                'positionId', position_id,\n" +
                "                'colName', col_name,\n" +
                "                'operator', operator,\n" +
                "                'colValue', col_value\n" +
                "            )\n" +
                "        )\n" +
                "    ) AS result\n" +
                "FROM\n" +
                "    position_row_filter_t\n" +
                "WHERE\n" +
                "    host_id = ?\n" +
                "    AND api_id = ?\n" +
                "    AND api_version = ?\n" +
                "GROUP BY ()\n" +
                "HAVING COUNT(*) > 0 \n" +
                "UNION ALL\n" +
                "SELECT\n" +
                "    JSON_BUILD_OBJECT(\n" +
                "        'position_col', JSON_AGG(\n" +
                "            JSON_BUILD_OBJECT(\n" +
                "                'endpoint', endpoint,\n" +
                "                'positionId', position_id,\n" +
                "                'columns', columns\n" +
                "            )\n" +
                "        )\n" +
                "    ) AS result\n" +
                "FROM\n" +
                "    position_col_filter_t\n" +
                "WHERE\n" +
                "    host_id = ?\n" +
                "    AND api_id = ?\n" +
                "    AND api_version = ?\n" +
                "GROUP BY ()\n" +
                "HAVING COUNT(*) > 0\n" +
                "UNION ALL\n" +
                "SELECT\n" +
                "    JSON_BUILD_OBJECT(\n" +
                "        'attribute_row', JSON_AGG(\n" +
                "            JSON_BUILD_OBJECT(\n" +
                "                'endpoint', endpoint,\n" +
                "                'attributeId', attribute_id,\n" +
                "                'attributeValue', attribute_value,\n" +
                "                'colName', col_name,\n" +
                "                'operator', operator,\n" +
                "                'colValue', col_value\n" +
                "            )\n" +
                "        )\n" +
                "    ) AS result\n" +
                "FROM\n" +
                "    attribute_row_filter_t\n" +
                "WHERE\n" +
                "    host_id = ?\n" +
                "    AND api_id = ?\n" +
                "    AND api_version = ?\n" +
                "GROUP BY ()\n" +
                "HAVING COUNT(*) > 0 \n" +
                "UNION ALL\n" +
                "SELECT\n" +
                "    JSON_BUILD_OBJECT(\n" +
                "        'attribute_col', JSON_AGG(\n" +
                "            JSON_BUILD_OBJECT(\n" +
                "                'endpoint', endpoint,\n" +
                "                'attributeId', attribute_id,\n" +
                "                'attributeValue', attribute_value,\n" +
                "                'columns', columns\n" +
                "            )\n" +
                "        )\n" +
                "    ) AS result\n" +
                "FROM\n" +
                "    attribute_col_filter_t\n" +
                "WHERE\n" +
                "    host_id = ?\n" +
                "    AND api_id = ?\n" +
                "    AND api_version = ?\n" +
                "GROUP BY ()\n" +
                "HAVING COUNT(*) > 0\n" +
                "UNION ALL\n" +
                "SELECT\n" +
                "    JSON_BUILD_OBJECT(\n" +
                "        'user_row', JSON_AGG(\n" +
                "            JSON_BUILD_OBJECT(\n" +
                "                'endpoint', endpoint,\n" +
                "                'userId', user_id,\n" +
                "                'startTs', start_ts,\n" +
                "                'endTs', end_ts,\n" +
                "                'colName', col_name,\n" +
                "                'operator', operator,\n" +
                "                'colValue', col_value\n" +
                "            )\n" +
                "        )\n" +
                "    ) AS result\n" +
                "FROM\n" +
                "    user_row_filter_t\n" +
                "WHERE\n" +
                "    host_id = ?\n" +
                "    AND api_id = ?\n" +
                "    AND api_version = ?\n" +
                "GROUP BY ()\n" +
                "HAVING COUNT(*) > 0 \n" +
                "UNION ALL\n" +
                "SELECT\n" +
                "    JSON_BUILD_OBJECT(\n" +
                "        'user_col', JSON_AGG(\n" +
                "            JSON_BUILD_OBJECT(\n" +
                "                'endpoint', endpoint,\n" +
                "                'userId', user_id,\n" +
                "                'startTs', start_ts,\n" +
                "                'endTs', end_ts,\n" +
                "                'columns', columns\n" +
                "            )\n" +
                "        )\n" +
                "    ) AS result\n" +
                "FROM\n" +
                "    user_col_filter_t\n" +
                "WHERE\n" +
                "    host_id = ?\n" +
                "    AND api_id = ?\n" +
                "    AND api_version = ?\n" +
                "GROUP BY ()\n" +
                "HAVING COUNT(*) > 0\n";

        try (Connection connection = ds.getConnection();
             PreparedStatement preparedStatement = connection.prepareStatement(sql)) {
            preparedStatement.setObject(1, UUID.fromString(hostId));
            preparedStatement.setString(2, apiId);
            preparedStatement.setString(3, apiVersion);
            preparedStatement.setObject(4, UUID.fromString(hostId));
            preparedStatement.setString(5, apiId);
            preparedStatement.setString(6, apiVersion);
            preparedStatement.setObject(7, UUID.fromString(hostId));
            preparedStatement.setString(8, apiId);
            preparedStatement.setString(9, apiVersion);
            preparedStatement.setObject(10, UUID.fromString(hostId));
            preparedStatement.setString(11, apiId);
            preparedStatement.setString(12, apiVersion);
            preparedStatement.setObject(13, UUID.fromString(hostId));
            preparedStatement.setString(14, apiId);
            preparedStatement.setString(15, apiVersion);
            preparedStatement.setObject(16, UUID.fromString(hostId));
            preparedStatement.setString(17, apiId);
            preparedStatement.setString(18, apiVersion);
            preparedStatement.setObject(19, UUID.fromString(hostId));
            preparedStatement.setString(20, apiId);
            preparedStatement.setString(21, apiVersion);
            preparedStatement.setObject(22, UUID.fromString(hostId));
            preparedStatement.setString(23, apiId);
            preparedStatement.setString(24, apiVersion);
            preparedStatement.setObject(25, UUID.fromString(hostId));
            preparedStatement.setString(26, apiId);
            preparedStatement.setString(27, apiVersion);
            preparedStatement.setObject(28, UUID.fromString(hostId));
            preparedStatement.setString(29, apiId);
            preparedStatement.setString(30, apiVersion);
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
