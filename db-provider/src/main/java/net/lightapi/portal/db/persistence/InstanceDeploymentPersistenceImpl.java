package net.lightapi.portal.db.persistence;

import com.networknt.config.JsonMapper;
import com.networknt.monad.Failure;
import com.networknt.monad.Result;
import com.networknt.monad.Success;
import com.networknt.status.Status;
import com.networknt.utility.Constants;
import com.networknt.utility.UuidUtil;
import io.cloudevents.core.v1.CloudEventV1;
import net.lightapi.portal.PortalConstants;
import net.lightapi.portal.db.PortalDbProvider;
import net.lightapi.portal.db.util.NotificationService;
import net.lightapi.portal.db.util.SqlUtil;
import net.lightapi.portal.validation.FilterCriterion;
import net.lightapi.portal.validation.SortCriterion;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;
import java.time.OffsetDateTime;
import java.util.*;
import java.util.stream.Collectors;

import static com.networknt.db.provider.SqlDbStartupHook.ds;
import static java.sql.Types.NULL;
import static net.lightapi.portal.db.util.SqlUtil.addCondition;

public class InstanceDeploymentPersistenceImpl implements InstanceDeploymentPersistence {
    private static final Logger logger = LoggerFactory.getLogger(InstanceDeploymentPersistenceImpl.class);
    private static final String SQL_EXCEPTION = PortalDbProvider.SQL_EXCEPTION;
    private static final String GENERIC_EXCEPTION = PortalDbProvider.GENERIC_EXCEPTION;
    private static final String OBJECT_NOT_FOUND = PortalDbProvider.OBJECT_NOT_FOUND;

    private final NotificationService notificationService;

    public InstanceDeploymentPersistenceImpl(NotificationService notificationService) {
        this.notificationService = notificationService;
    }

    @Override
    public Result<String> createInstance(Map<String, Object> event) {
        final String sql =
                """
                INSERT INTO instance_t(host_id, instance_id, instance_name, product_version_id,
                service_id, current, readonly, environment, service_desc, instance_desc, zone, region, lob,
                resource_name, business_name, env_tag, topic_classification, update_user, update_ts)
                VALUES (?, ?, ?, ?, ?,  ?, ?, ?, ?, ?,  ?, ?, ?, ?, ?,  ?, ?, ?, ?)
                """;
        final String sqlUpdateCurrent =
                """
                UPDATE instance_t SET current = false
                WHERE host_id = ?
                AND service_id = ?
                AND instance_id != ?
                """;

        Result<String> result;
        Map<String, Object> map = (Map<String, Object>)event.get(PortalConstants.DATA);
        try (Connection conn = ds.getConnection()) {
            conn.setAutoCommit(false);
            try (PreparedStatement statement = conn.prepareStatement(sql)) {
                statement.setObject(1, UUID.fromString((String)event.get(Constants.HOST)));
                statement.setObject(2, UUID.fromString((String)map.get("instanceId")));
                statement.setString(3, (String)map.get("instanceName"));
                statement.setObject(4, UUID.fromString((String)map.get("productVersionId")));
                statement.setString(5, (String)map.get("serviceId"));
                Boolean current = (Boolean)map.get("current");
                if (current != null) {
                    statement.setBoolean(6, current);
                } else {
                    statement.setNull(6, Types.BOOLEAN);
                }
                Boolean readonly = (Boolean)map.get("readonly");
                if (readonly != null) {
                    statement.setBoolean(7, readonly);
                } else {
                    statement.setNull(7, Types.BOOLEAN);
                }
                String environment = (String)map.get("environment");
                if (environment != null && !environment.isEmpty()) {
                    statement.setString(8, environment);
                } else {
                    statement.setNull(8, Types.VARCHAR);
                }
                String serviceDesc = (String)map.get("serviceDesc");
                if (serviceDesc != null && !serviceDesc.isEmpty()) {
                    statement.setString(9, serviceDesc);
                } else {
                    statement.setNull(9, Types.VARCHAR);
                }
                String instanceDesc = (String)map.get("instanceDesc");
                if(instanceDesc != null && !instanceDesc.isEmpty()) {
                    statement.setString(10, instanceDesc);
                } else {
                    statement.setNull(10, Types.VARCHAR);
                }
                String zone = (String)map.get("zone");
                if(zone != null && !zone.isEmpty()) {
                    statement.setString(11, zone);
                } else {
                    statement.setNull(11, Types.VARCHAR);
                }
                String region = (String)map.get("region");
                if(region != null && !region.isEmpty()) {
                    statement.setString(12, region);
                } else {
                    statement.setNull(12, Types.VARCHAR);
                }
                String lob = (String)map.get("lob");
                if(lob != null && !lob.isEmpty()) {
                    statement.setString(13, lob);
                } else {
                    statement.setNull(13, Types.VARCHAR);
                }
                String resourceName = (String)map.get("resourceName");
                if(resourceName != null && !resourceName.isEmpty()) {
                    statement.setString(14, resourceName);
                } else {
                    statement.setNull(14, Types.VARCHAR);
                }
                String businessName = (String)map.get("businessName");
                if(businessName != null && !businessName.isEmpty()) {
                    statement.setString(15, businessName);
                } else {
                    statement.setNull(15, Types.VARCHAR);
                }
                String envTag = (String)map.get("envTag");
                if(envTag != null && !envTag.isEmpty()) {
                    statement.setString(16, envTag);
                } else {
                    statement.setNull(16, Types.VARCHAR);
                }
                String topicClassification = (String)map.get("topicClassification");
                if(topicClassification != null && !topicClassification.isEmpty()) {
                    statement.setString(17, topicClassification);
                } else {
                    statement.setNull(17, Types.VARCHAR);
                }

                statement.setString(18, (String)event.get(Constants.USER));
                statement.setObject(19, OffsetDateTime.parse((String)event.get(CloudEventV1.TIME)));

                int count = statement.executeUpdate();
                if (count == 0) {
                    throw new SQLException("failed to insert the instance with id " + map.get("instanceId"));
                }
                // try to update current to false for others if current is true.
                if(current != null && current) {
                    try (PreparedStatement statementUpdate = conn.prepareStatement(sqlUpdateCurrent)) {
                        statementUpdate.setObject(1, UUID.fromString((String)event.get(Constants.HOST)));
                        statementUpdate.setString(2, (String)map.get("serviceId"));
                        statementUpdate.setObject(3, UUID.fromString((String)map.get("instanceId")));
                        statementUpdate.executeUpdate();
                    }
                }
                conn.commit();
                result = Success.of((String)map.get("instanceId"));
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
    public Result<String> updateInstance(Map<String, Object> event) {
        final String sql =
                """
                UPDATE instance_t SET instance_name = ?, product_version_id = ?, service_id = ?,
                current = ?, readonly = ?, environment = ?, service_desc = ?, instance_desc = ?,
                zone = ?, region = ?, lob = ?, resource_name = ?, business_name = ?, env_tag = ?,
                topic_classification = ?, update_user = ?, update_ts = ?
                WHERE host_id = ? and instance_id = ?
                """;
        final String sqlUpdateCurrent =
                """
                UPDATE instance_t SET current = false
                WHERE host_id = ?
                AND service_id = ?
                AND instance_id != ?
                """;

        Result<String> result;
        Map<String, Object> map = (Map<String, Object>)event.get(PortalConstants.DATA);
        try (Connection conn = ds.getConnection()) {
            conn.setAutoCommit(false);
            try (PreparedStatement statement = conn.prepareStatement(sql)) {
                statement.setString(1, (String)map.get("instanceName"));
                statement.setObject(2, UUID.fromString((String)map.get("productVersionId")));
                statement.setString(3, (String)map.get("serviceId"));
                Boolean current = (Boolean)map.get("current");
                if (current != null) {
                    statement.setBoolean(4, current);
                } else {
                    statement.setNull(4, Types.BOOLEAN);
                }
                Boolean readonly = (Boolean)map.get("readonly");
                if (readonly != null) {
                    statement.setBoolean(5, readonly);
                } else {
                    statement.setNull(5, Types.BOOLEAN);
                }
                String environment = (String)map.get("environment");
                if (environment != null && !environment.isEmpty()) {
                    statement.setString(6, environment);
                } else {
                    statement.setNull(6, Types.VARCHAR);
                }
                String serviceDesc = (String)map.get("serviceDesc");
                if (serviceDesc != null && !serviceDesc.isEmpty()) {
                    statement.setString(7, serviceDesc);
                } else {
                    statement.setNull(7, Types.VARCHAR);
                }
                String instanceDesc = (String)map.get("instanceDesc");
                if (instanceDesc != null && !instanceDesc.isEmpty()) {
                    statement.setString(8, instanceDesc);
                } else {
                    statement.setNull(8, Types.VARCHAR);
                }
                String zone = (String)map.get("zone");
                if (zone != null && !zone.isEmpty()) {
                    statement.setString(9, zone);
                } else {
                    statement.setNull(9, Types.VARCHAR);
                }
                String region = (String)map.get("region");
                if (region != null && !region.isEmpty()) {
                    statement.setString(10, region);
                } else {
                    statement.setNull(10, Types.VARCHAR);
                }
                String lob = (String)map.get("lob");
                if (lob != null && !lob.isEmpty()) {
                    statement.setString(11, lob);
                } else {
                    statement.setNull(11, Types.VARCHAR);
                }
                String resourceName = (String)map.get("resourceName");
                if (resourceName != null && !resourceName.isEmpty()) {
                    statement.setString(12, resourceName);
                } else {
                    statement.setNull(12, Types.VARCHAR);
                }
                String businessName = (String)map.get("businessName");
                if (businessName != null && !businessName.isEmpty()) {
                    statement.setString(13, businessName);
                } else {
                    statement.setNull(13, Types.VARCHAR);
                }
                String envTag = (String)map.get("envTag");
                if (envTag != null && !envTag.isEmpty()) {
                    statement.setString(14, envTag);
                } else {
                    statement.setNull(14, Types.VARCHAR);
                }
                String topicClassification = (String)map.get("topicClassification");
                if (topicClassification != null && !topicClassification.isEmpty()) {
                    statement.setString(15, topicClassification);
                } else {
                    statement.setNull(15, Types.VARCHAR);
                }
                statement.setString(16, (String)event.get(Constants.USER));
                statement.setObject(17, OffsetDateTime.parse((String)event.get(CloudEventV1.TIME)));
                statement.setObject(18, UUID.fromString((String)event.get(Constants.HOST)));
                statement.setObject(19, UUID.fromString((String)map.get("instanceId")));

                int count = statement.executeUpdate();
                if (count == 0) {
                    throw new SQLException("failed to update the instance with id " + map.get("instanceId"));
                }
                // try to update current to false for others if current is true.
                if(current != null && current) {
                    try (PreparedStatement statementUpdate = conn.prepareStatement(sqlUpdateCurrent)) {
                        statementUpdate.setObject(1, UUID.fromString((String)event.get(Constants.HOST)));
                        statementUpdate.setString(2, (String)map.get("serviceId"));
                        statementUpdate.setObject(3, UUID.fromString((String)map.get("instanceId")));
                        statementUpdate.executeUpdate();
                    }
                }

                conn.commit();
                result = Success.of((String)map.get("instanceId"));
                notificationService.insertNotification(event, true, null);


            } catch (SQLException e) {
                logger.error("SQLException:", e);
                conn.rollback();
                notificationService.insertNotification(event, false, e.getMessage());
                result = Failure.of(new Status(SQL_EXCEPTION, e.getMessage()));
            }  catch (Exception e) {
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
    public Result<String> deleteInstance(Map<String, Object> event) {
        final String sql = "DELETE FROM instance_t WHERE host_id = ? AND instance_id = ?";
        Result<String> result;
        Map<String, Object> map = (Map<String, Object>)event.get(PortalConstants.DATA);
        try (Connection conn = ds.getConnection()) {
            conn.setAutoCommit(false);
            try (PreparedStatement statement = conn.prepareStatement(sql)) {
                statement.setObject(1, UUID.fromString((String)event.get(Constants.HOST)));
                statement.setObject(2, UUID.fromString((String)map.get("instanceId")));

                int count = statement.executeUpdate();
                if (count == 0) {
                    throw new SQLException("failed to delete the instance with id " + map.get("instanceId"));
                }
                conn.commit();
                result = Success.of((String)map.get("instanceId"));
                notificationService.insertNotification(event, true, null);

            } catch (SQLException e) {
                logger.error("SQLException:", e);
                conn.rollback();
                notificationService.insertNotification(event, false, e.getMessage());
                result = Failure.of(new Status(SQL_EXCEPTION, e.getMessage()));
            }
            catch (Exception e) {
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
    public Result<String> getInstance(int offset, int limit, String hostId, String instanceId, String instanceName,
                                      String productVersionId, String productId, String productVersion, String serviceId,
                                      Boolean current, Boolean readonly, String environment, String serviceDesc,
                                      String instanceDesc, String zone,  String region, String lob, String resourceName,
                                      String businessName, String envTag, String topicClassification) {
        Result<String> result = null;
        String s =
                """
                        SELECT COUNT(*) OVER () AS total,
                        i.host_id, i.instance_id, i.instance_name, i.product_version_id, pv.product_id, pv.product_version,
                        i.service_id, i.current, i.readonly, i.environment, i.service_desc, i.instance_desc, i.zone, i.region,
                        i.lob, i.resource_name, i.business_name, i.env_tag, i.topic_classification, i.update_user, i.update_ts
                        FROM instance_t i
                        INNER JOIN product_version_t pv ON pv.product_version_id = i.product_version_id
                        WHERE 1=1
                """;
        StringBuilder sqlBuilder = new StringBuilder();
        sqlBuilder.append(s).append("\n");

        List<Object> parameters = new ArrayList<>();

        StringBuilder whereClause = new StringBuilder();

        addCondition(whereClause, parameters, "i.host_id", hostId != null ? UUID.fromString(hostId) : null);
        addCondition(whereClause, parameters, "i.instance_id", instanceId != null ? UUID.fromString(instanceId) : null);
        addCondition(whereClause, parameters, "i.instance_name", instanceName);
        addCondition(whereClause, parameters, "i.product_version_id", productVersionId != null ? UUID.fromString(productVersionId) : null);
        addCondition(whereClause, parameters, "pv.product_id", productId);
        addCondition(whereClause, parameters, "pv.product_version", productVersion);
        addCondition(whereClause, parameters, "i.service_id", serviceId);
        addCondition(whereClause, parameters, "i.current", current);
        addCondition(whereClause, parameters, "i.readonly", readonly);
        addCondition(whereClause, parameters, "i.environment", environment);
        addCondition(whereClause, parameters, "i.service_desc", serviceDesc);
        addCondition(whereClause, parameters, "i.instance_desc", instanceDesc);
        addCondition(whereClause, parameters, "i.zone", zone);
        addCondition(whereClause, parameters, "i.region", region);
        addCondition(whereClause, parameters, "i.lob", lob);
        addCondition(whereClause, parameters, "i.resource_name", resourceName);
        addCondition(whereClause, parameters, "i.business_name", businessName);
        addCondition(whereClause, parameters, "i.env_tag", envTag);
        addCondition(whereClause, parameters, "i.topic_classification", topicClassification);

        if (!whereClause.isEmpty()) {
            sqlBuilder.append("AND ").append(whereClause);
        }

        sqlBuilder.append(" ORDER BY i.instance_id\n" +
                "LIMIT ? OFFSET ?");

        parameters.add(limit);
        parameters.add(offset);

        String sql = sqlBuilder.toString();
        int total = 0;
        List<Map<String, Object>> instances = new ArrayList<>();

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
                    map.put("instanceId", resultSet.getObject("instance_id", UUID.class));
                    map.put("instanceName", resultSet.getString("instance_name"));
                    map.put("productVersionId", resultSet.getObject("product_version_id", UUID.class));
                    map.put("productId", resultSet.getString("product_id"));
                    map.put("productVersion", resultSet.getString("product_version"));
                    map.put("serviceId", resultSet.getString("service_id"));
                    map.put("current", resultSet.getBoolean("current"));
                    map.put("readonly", resultSet.getBoolean("readonly"));
                    map.put("environment", resultSet.getString("environment"));
                    map.put("serviceDesc", resultSet.getString("service_desc"));
                    map.put("instanceDesc", resultSet.getString("instance_desc"));
                    map.put("zone", resultSet.getString("zone"));
                    map.put("region", resultSet.getString("region"));
                    map.put("lob", resultSet.getString("lob"));
                    map.put("resourceName", resultSet.getString("resource_name"));
                    map.put("businessName", resultSet.getString("business_name"));
                    map.put("envTag", resultSet.getString("env_tag"));
                    map.put("topicClassification", resultSet.getString("topic_classification"));
                    map.put("updateUser", resultSet.getString("update_user"));
                    // handling date properly
                    map.put("updateTs", resultSet.getObject("update_ts") != null ? resultSet.getObject("update_ts", OffsetDateTime.class) : null);
                    instances.add(map);
                }
            }
            Map<String, Object> resultMap = new HashMap<>();
            resultMap.put("total", total);
            resultMap.put("instances", instances);
            result = Success.of(JsonMapper.toJson(resultMap));
        } catch (SQLException e) {
            logger.error("SQLException:", e);
            result = Failure.of(new Status(SQL_EXCEPTION, e.getMessage()));
        }  catch (Exception e) {
            logger.error("Exception:", e);
            result = Failure.of(new Status(GENERIC_EXCEPTION, e.getMessage()));
        }
        return result;
    }

    @Override
    public Result<String> getInstanceLabel(String hostId) {
        Result<String> result = null;
        String sql =
                """
                        SELECT i.instance_id, i.instance_name, pv.product_id, pv.product_version
                        FROM instance_t i
                        INNER JOIN product_version_t pv ON pv.product_version_id = i.product_version_id
                        WHERE i.host_id = ?
                """;
        List<Map<String, Object>> labels = new ArrayList<>();
        try (Connection connection = ds.getConnection();
             PreparedStatement preparedStatement = connection.prepareStatement(sql)) {
            preparedStatement.setObject(1, UUID.fromString(hostId));
            try (ResultSet resultSet = preparedStatement.executeQuery()) {
                while (resultSet.next()) {
                    Map<String, Object> map = new HashMap<>();
                    map.put("id", resultSet.getString("instance_id"));
                    map.put("label", resultSet.getString("instance_name") + "|" +
                            resultSet.getString("product_id") + "|" + resultSet.getString("product_version"));
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
    public Result<String> createInstanceApi(Map<String, Object> event) {
        final String sql = "INSERT INTO instance_api_t(host_id, instance_api_id, instance_id, api_version_id, active, update_user, update_ts) " +
                "VALUES (?, ?, ?, ?, ?, ?, ?)";
        Result<String> result;
        Map<String, Object> map = (Map<String, Object>)event.get(PortalConstants.DATA);
        try (Connection conn = ds.getConnection()) {
            conn.setAutoCommit(false);
            try (PreparedStatement statement = conn.prepareStatement(sql)) {
                statement.setObject(1, UUID.fromString((String)event.get(Constants.HOST)));
                statement.setObject(2, UUID.fromString((String)map.get("instanceApiId")));
                statement.setObject(3, UUID.fromString((String)map.get("instanceId")));
                statement.setObject(4, UUID.fromString((String)map.get("apiVersionId")));
                Boolean active = (Boolean)map.get("active");
                if (active != null) {
                    statement.setBoolean(5, active);
                } else {
                    statement.setNull(5, Types.BOOLEAN);
                }
                statement.setString(6, (String)event.get(Constants.USER));
                statement.setObject(7, OffsetDateTime.parse((String)event.get(CloudEventV1.TIME)));

                int count = statement.executeUpdate();
                if (count == 0) {
                    throw new SQLException("failed to insert the instance api");
                }
                conn.commit();
                result = Success.of(String.format("Instance API created for hostId: %s, instanceApiId: %s, instanceId: %s, apiVersionId: %s",
                        event.get(Constants.HOST), map.get("instanceApiId"), map.get("instanceId"), map.get("apiVersionId")));
                notificationService.insertNotification(event, true, null);

            }   catch (SQLException e) {
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
    public Result<String> updateInstanceApi(Map<String, Object> event) {
        final String sql = "UPDATE instance_api_t SET instance_id = ?, api_version_id = ?, active = ?, update_user = ?, update_ts = ? " +
                "WHERE host_id = ? and instance_api_id = ?";
        Result<String> result;
        Map<String, Object> map = (Map<String, Object>)event.get(PortalConstants.DATA);
        try (Connection conn = ds.getConnection()) {
            conn.setAutoCommit(false);
            try (PreparedStatement statement = conn.prepareStatement(sql)) {
                statement.setObject(1, UUID.fromString((String)map.get("instanceId")));
                statement.setObject(2, UUID.fromString((String)map.get("apiVersionId")));
                Boolean active = (Boolean)map.get("active");
                if (active != null) {
                    statement.setBoolean(3, active);
                } else {
                    statement.setNull(3, Types.BOOLEAN);
                }
                statement.setString(4, (String)event.get(Constants.USER));
                statement.setObject(5, OffsetDateTime.parse((String)event.get(CloudEventV1.TIME)));
                statement.setObject(6, UUID.fromString((String)event.get(Constants.HOST)));
                statement.setObject(7, UUID.fromString((String)map.get("instanceApiId")));

                int count = statement.executeUpdate();
                if (count == 0) {
                    throw new SQLException("failed to update instance api");
                }
                conn.commit();
                result = Success.of(String.format("Instance API updated for hostId: %s, instanceApiId: %s",
                        event.get(Constants.HOST), map.get("instanceApiId")));
                notificationService.insertNotification(event, true, null);
            } catch (SQLException e) {
                logger.error("SQLException:", e);
                conn.rollback();
                notificationService.insertNotification(event, false, e.getMessage());
                result = Failure.of(new Status(SQL_EXCEPTION, e.getMessage()));
            }  catch (Exception e) {
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
    public Result<String> deleteInstanceApi(Map<String, Object> event) {
        final String sql = "DELETE FROM instance_api_t WHERE host_id = ? AND instance_api_id = ?";
        Result<String> result;
        Map<String, Object> map = (Map<String, Object>)event.get(PortalConstants.DATA);
        try (Connection conn = ds.getConnection()) {
            conn.setAutoCommit(false);
            try (PreparedStatement statement = conn.prepareStatement(sql)) {
                statement.setObject(1, UUID.fromString((String)event.get(Constants.HOST)));
                statement.setObject(2, UUID.fromString((String)map.get("instanceApiId")));

                int count = statement.executeUpdate();
                if (count == 0) {
                    throw new SQLException("failed to delete the instance api with instanceApiId " + map.get("instanceApiId"));
                }
                conn.commit();
                result = Success.of(String.format("Instance API deleted for hostId: %s, instanceApiId: %s",
                        event.get(Constants.HOST), map.get("instanceApiId")));
                notificationService.insertNotification(event, true, null);

            } catch (SQLException e) {
                logger.error("SQLException:", e);
                conn.rollback();
                notificationService.insertNotification(event, false, e.getMessage());
                result = Failure.of(new Status(SQL_EXCEPTION, e.getMessage()));
            }  catch (Exception e) {
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
    public Result<String> getInstanceApi(int offset, int limit, String hostId, String instanceApiId, String instanceId, String instanceName,
                                         String productId, String productVersion, String apiVersionId, String apiId, String apiVersion,
                                         Boolean active) {
        Result<String> result = null;
        String s =
                """
                        SELECT COUNT(*) OVER () AS total,
                        ia.host_id, ia.instance_api_id, ia.instance_id, i.instance_name, pv.product_id,
                        pv.product_version, ia.api_version_id, av.api_id, av.api_version, ia.active,
                        ia.update_user, ia.update_ts
                        FROM instance_api_t ia
                        INNER JOIN instance_t i ON ia.instance_id = i.instance_id
                        INNER JOIN product_version_t pv ON i.product_version_id = pv.product_version_id
                        INNER JOIN api_version_t av ON ia.api_version_id = av.api_version_id
                        WHERE 1=1
                """;
        StringBuilder sqlBuilder = new StringBuilder();
        sqlBuilder.append(s).append("\n");

        List<Object> parameters = new ArrayList<>();

        StringBuilder whereClause = new StringBuilder();

        addCondition(whereClause, parameters, "ia.host_id", hostId != null ? UUID.fromString(hostId) : null);
        addCondition(whereClause, parameters, "ia.instance_api_id", instanceApiId != null ? UUID.fromString(instanceApiId) : null);
        addCondition(whereClause, parameters, "ia.instance_id", instanceId != null ? UUID.fromString(instanceId) : null);
        addCondition(whereClause, parameters, "i.instance_name", instanceName);
        addCondition(whereClause, parameters, "pv.product_id", productId);
        addCondition(whereClause, parameters, "pv.product_version", productVersion);
        addCondition(whereClause, parameters, "api_version_id", apiVersionId != null ? UUID.fromString(apiVersionId) : null);
        addCondition(whereClause, parameters, "av.api_id", apiId);
        addCondition(whereClause, parameters, "av.api_version", apiVersion);
        addCondition(whereClause, parameters, "active", active);

        if (!whereClause.isEmpty()) {
            sqlBuilder.append("AND ").append(whereClause);
        }

        sqlBuilder.append(" ORDER BY instance_id, api_version_id\n" + // Added ordering
                "LIMIT ? OFFSET ?");

        parameters.add(limit);
        parameters.add(offset);

        String sql = sqlBuilder.toString();
        int total = 0;
        List<Map<String, Object>> instanceApis = new ArrayList<>();

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
                    map.put("instanceApiId", resultSet.getObject("instance_api_id", UUID.class));
                    map.put("instanceId", resultSet.getObject("instance_id", UUID.class));
                    map.put("instanceName", resultSet.getString("instance_name"));
                    map.put("productId", resultSet.getString("product_id"));
                    map.put("productVersion", resultSet.getString("product_version"));
                    map.put("apiVersionId", resultSet.getObject("api_version_id", UUID.class));
                    map.put("apiId", resultSet.getString("api_id"));
                    map.put("apiVersion", resultSet.getString("api_version"));
                    map.put("active", resultSet.getBoolean("active"));
                    map.put("updateUser", resultSet.getString("update_user"));
                    map.put("updateTs", resultSet.getObject("update_ts") != null ? resultSet.getObject("update_ts", OffsetDateTime.class) : null);
                    instanceApis.add(map);
                }
            }

            Map<String, Object> resultMap = new HashMap<>();
            resultMap.put("total", total);
            resultMap.put("instanceApis", instanceApis);
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

    public Result<String> getInstanceApiLabel(String hostId, String instanceId) {
        Result<String> result = null;
        String sql =
                """
                        SELECT ia.instance_api_id, i.instance_name, av.api_id, av.api_version
                        FROM instance_api_t ia
                        INNER JOIN instance_t i ON i.instance_id = ia.instance_id
                        INNER JOIN api_version_t av ON av.api_version_id = ia.api_version_id
                        WHERE ia.host_id = ?
                """;
        if(instanceId != null) sql += " AND ia.instance_id = ?";
        List<Map<String, Object>> labels = new ArrayList<>();
        try (Connection connection = ds.getConnection();
             PreparedStatement preparedStatement = connection.prepareStatement(sql)) {
            preparedStatement.setObject(1, UUID.fromString(hostId));
            if(instanceId != null) {
                preparedStatement.setObject(2, UUID.fromString(instanceId));
            }
            try (ResultSet resultSet = preparedStatement.executeQuery()) {
                while (resultSet.next()) {
                    Map<String, Object> map = new HashMap<>();
                    map.put("id", resultSet.getString("instance_api_id"));
                    map.put("label", resultSet.getString("api_id") + "|" + resultSet.getString("api_version") +  "|" + resultSet.getString("instance_name"));
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
    public Result<String> createInstanceApiPathPrefix(Map<String, Object> event) {
        final String sql = "INSERT INTO instance_api_path_prefix_t(host_id, instance_api_id, path_prefix, update_user, update_ts) " +
                "VALUES (?, ?, ?, ?, ?)";
        Result<String> result;
        Map<String, Object> map = (Map<String, Object>)event.get(PortalConstants.DATA);
        try (Connection conn = ds.getConnection()) {
            conn.setAutoCommit(false);
            try (PreparedStatement statement = conn.prepareStatement(sql)) {
                statement.setObject(1, UUID.fromString((String)event.get(Constants.HOST)));
                statement.setObject(2, UUID.fromString((String)map.get("instanceApiId")));
                statement.setString(3, (String)map.get("pathPrefix"));
                statement.setString(4, (String)event.get(Constants.USER));
                statement.setObject(5, OffsetDateTime.parse((String)event.get(CloudEventV1.TIME)));

                int count = statement.executeUpdate();
                if (count == 0) {
                    throw new SQLException("failed to insert the instance api path prefix");
                }
                conn.commit();
                result = Success.of(String.format("Instance api path prefix created for hostId: %s, instanceApiId: %s, pathPrefix: %s",
                        event.get(Constants.HOST), map.get("instanceApiId"), map.get("pathPrefix")));
                notificationService.insertNotification(event, true, null);

            }   catch (SQLException e) {
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
    public Result<String> updateInstanceApiPathPrefix(Map<String, Object> event) {
        final String sql = "UPDATE instance_api_path_prefix_t SET path_prefix = ?, update_user = ?, update_ts = ? " +
                "WHERE host_id = ? and instance_api_id = ?";
        Result<String> result;
        Map<String, Object> map = (Map<String, Object>)event.get(PortalConstants.DATA);
        try (Connection conn = ds.getConnection()) {
            conn.setAutoCommit(false);
            try (PreparedStatement statement = conn.prepareStatement(sql)) {
                statement.setString(1, (String)map.get("pathPrefix"));
                statement.setString(2, (String)event.get(Constants.USER));
                statement.setObject(3, OffsetDateTime.parse((String)event.get(CloudEventV1.TIME)));
                statement.setObject(4, UUID.fromString((String)event.get(Constants.HOST)));
                statement.setObject(5, UUID.fromString((String)map.get("instanceApiId")));

                int count = statement.executeUpdate();
                if (count == 0) {
                    throw new SQLException("failed to update instance api path prefix");
                }
                conn.commit();
                result = Success.of(String.format("Instance API updated for hostId: %s, instanceApiId: %s, pathPrefix: %s",
                        event.get(Constants.HOST), map.get("instanceApiId"), map.get("pathPrefix")));
                notificationService.insertNotification(event, true, null);
            } catch (SQLException e) {
                logger.error("SQLException:", e);
                conn.rollback();
                notificationService.insertNotification(event, false, e.getMessage());
                result = Failure.of(new Status(SQL_EXCEPTION, e.getMessage()));
            }  catch (Exception e) {
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
    public Result<String> deleteInstanceApiPathPrefix(Map<String, Object> event) {
        final String sql = "DELETE FROM instance_api_path_prefix_t WHERE host_id = ? AND instance_api_id = ? AND path_prefix = ?";
        Result<String> result;
        Map<String, Object> map = (Map<String, Object>)event.get(PortalConstants.DATA);
        try (Connection conn = ds.getConnection()) {
            conn.setAutoCommit(false);
            try (PreparedStatement statement = conn.prepareStatement(sql)) {
                statement.setObject(1, UUID.fromString((String)event.get(Constants.HOST)));
                statement.setObject(2, UUID.fromString((String)map.get("instanceApiId")));
                statement.setString(3, (String)map.get("pathPrefix"));

                int count = statement.executeUpdate();
                if (count == 0) {
                    throw new SQLException("failed to delete the instance api with instanceApiId " + map.get("instanceApiId"));
                }
                conn.commit();
                result = Success.of(String.format("Instance api path prefix deleted for hostId: %s, instanceApiId: %s, pathPrefix: %s",
                        event.get(Constants.HOST), map.get("instanceApiId"), map.get("pathPrefix")));
                notificationService.insertNotification(event, true, null);

            } catch (SQLException e) {
                logger.error("SQLException:", e);
                conn.rollback();
                notificationService.insertNotification(event, false, e.getMessage());
                result = Failure.of(new Status(SQL_EXCEPTION, e.getMessage()));
            }  catch (Exception e) {
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
    public Result<String> getInstanceApiPathPrefix(int offset, int limit, String hostId, String instanceApiId, String instanceId,
                                                   String instanceName, String productId, String productVersion, String apiVersionId,
                                                   String apiId, String apiVersion, String pathPrefix) {
        Result<String> result = null;
        String s =
                """
                        SELECT COUNT(*) OVER () AS total,
                        iapp.host_id, iapp.instance_api_id, iai.instance_id, i.instance_name,
                        pv.product_id, pv.product_version, iai.api_version_id, av.api_id,
                        av.api_version, iapp.path_prefix, iapp.update_user, iapp.update_ts
                        FROM instance_api_path_prefix_t iapp
                        INNER JOIN instance_api_t iai ON iapp.instance_api_id = iai.instance_api_id
                        INNER JOIN instance_t i ON i.instance_id = iai.instance_id
                        INNER JOIN product_version_t pv ON pv.product_version_id = i.product_version_id
                        INNER JOIN api_version_t av ON av.api_version_id = iai.api_version_id
                        INNER JOIN api_t ai ON ai.api_id = av.api_id
                        WHERE 1=1
                """;
        StringBuilder sqlBuilder = new StringBuilder();
        sqlBuilder.append(s).append("\n");

        List<Object> parameters = new ArrayList<>();

        StringBuilder whereClause = new StringBuilder();

        addCondition(whereClause, parameters, "iapp.host_id", hostId != null ? UUID.fromString(hostId) : null);
        addCondition(whereClause, parameters, "iapp.instance_api_id", instanceApiId != null ? UUID.fromString(instanceApiId) : null);
        addCondition(whereClause, parameters, "iai.instance_id", instanceId != null ? UUID.fromString(instanceId) : null);
        addCondition(whereClause, parameters, "i.instance_name", instanceName);
        addCondition(whereClause, parameters, "pv.product_id", productId);
        addCondition(whereClause, parameters, "pv.product_version", productVersion);
        addCondition(whereClause, parameters, "iai.api_version_id", apiVersionId != null ? UUID.fromString(apiVersionId) : null);
        addCondition(whereClause, parameters, "av.api_id", apiId);
        addCondition(whereClause, parameters, "av.api_version", apiVersion);
        addCondition(whereClause, parameters, "iapp.path_prefix", pathPrefix);


        if (!whereClause.isEmpty()) {
            sqlBuilder.append("AND ").append(whereClause);
        }

        sqlBuilder.append(" ORDER BY iai.instance_id, iapp.instance_api_id, iapp.path_prefix\n" + // Added ordering
                "LIMIT ? OFFSET ?");

        parameters.add(limit);
        parameters.add(offset);

        String sql = sqlBuilder.toString();
        int total = 0;
        List<Map<String, Object>> instanceApiPathPrefixes = new ArrayList<>();

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
                    map.put("instanceApiId", resultSet.getObject("instance_api_id", UUID.class));
                    map.put("instanceId", resultSet.getObject("instance_id", UUID.class));
                    map.put("instanceName", resultSet.getString("instance_name"));
                    map.put("productId", resultSet.getString("product_id"));
                    map.put("productVersion", resultSet.getString("product_version"));
                    map.put("apiVersionId", resultSet.getObject("api_version_id", UUID.class));
                    map.put("apiId", resultSet.getString("api_id"));
                    map.put("apiVersion", resultSet.getString("api_version"));
                    map.put("pathPrefix", resultSet.getString("path_prefix"));
                    map.put("updateUser", resultSet.getString("update_user"));
                    map.put("updateTs", resultSet.getObject("update_ts") != null ? resultSet.getObject("update_ts", OffsetDateTime.class) : null);
                    instanceApiPathPrefixes.add(map);
                }
            }

            Map<String, Object> resultMap = new HashMap<>();
            resultMap.put("total", total);
            resultMap.put("instanceApiPathPrefixes", instanceApiPathPrefixes);
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
    public Result<String> createInstanceAppApi(Map<String, Object> event) {
        final String sql = "INSERT INTO instance_app_api_t(host_id, instance_app_id, instance_api_id, active, update_user, update_ts) " +
                "VALUES (?, ?, ?, ?, ?, ?)";
        Result<String> result;
        Map<String, Object> map = (Map<String, Object>)event.get(PortalConstants.DATA);
        try (Connection conn = ds.getConnection()) {
            conn.setAutoCommit(false);
            try (PreparedStatement statement = conn.prepareStatement(sql)) {
                statement.setObject(1, UUID.fromString((String)event.get(Constants.HOST)));
                statement.setObject(2, UUID.fromString((String)map.get("instanceAppId")));
                statement.setObject(3, UUID.fromString((String)map.get("instanceApiId")));
                Boolean active = (Boolean)map.get("active");
                if (active != null) {
                    statement.setBoolean(4, active);
                } else {
                    statement.setNull(4, Types.BOOLEAN);
                }
                statement.setString(5, (String)event.get(Constants.USER));
                statement.setObject(6, OffsetDateTime.parse((String)event.get(CloudEventV1.TIME)));

                int count = statement.executeUpdate();
                if (count == 0) {
                    throw new SQLException("failed to insert the instance app api");
                }
                conn.commit();
                result = Success.of(String.format("Instance app api created for hostId: %s, instanceAppId: %s, instanceApiId: %s",
                        event.get(Constants.HOST), map.get("instanceAppId"), map.get("instanceApiId")));
                notificationService.insertNotification(event, true, null);
            }   catch (SQLException e) {
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
    public Result<String> updateInstanceAppApi(Map<String, Object> event) {
        final String sql = "UPDATE instance_api_t SET active = ?, update_user = ?, update_ts = ? " +
                "WHERE host_id = ? AND instance_app_id = ? AND instance_api_id = ?";
        Result<String> result;
        Map<String, Object> map = (Map<String, Object>)event.get(PortalConstants.DATA);
        try (Connection conn = ds.getConnection()) {
            conn.setAutoCommit(false);
            try (PreparedStatement statement = conn.prepareStatement(sql)) {
                Boolean active = (Boolean)map.get("active");
                if (active != null) {
                    statement.setBoolean(1, active);
                } else {
                    statement.setNull(1, Types.BOOLEAN);
                }
                statement.setString(2, (String)event.get(Constants.USER));
                statement.setObject(3, OffsetDateTime.parse((String)event.get(CloudEventV1.TIME)));
                statement.setObject(4, UUID.fromString((String)event.get(Constants.HOST)));
                statement.setObject(5, UUID.fromString((String)map.get("instanceAppId")));
                statement.setObject(6, UUID.fromString((String)map.get("instanceApiId")));

                int count = statement.executeUpdate();
                if (count == 0) {
                    throw new SQLException("failed to update instance app api");
                }
                conn.commit();
                result = Success.of(String.format("Instance API updated for hostId: %s, instanceAppId: %s, instanceApiId: %s",
                        event.get(Constants.HOST), map.get("instanceAppId"), map.get("instanceApiId")));
                notificationService.insertNotification(event, true, null);
            } catch (SQLException e) {
                logger.error("SQLException:", e);
                conn.rollback();
                notificationService.insertNotification(event, false, e.getMessage());
                result = Failure.of(new Status(SQL_EXCEPTION, e.getMessage()));
            }  catch (Exception e) {
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
    public Result<String> deleteInstanceAppApi(Map<String, Object> event) {
        final String sql = "DELETE FROM instance_app_api_t WHERE host_id = ? AND instance_app_id = ? AND instance_api_id = ?";
        Result<String> result;
        Map<String, Object> map = (Map<String, Object>)event.get(PortalConstants.DATA);
        try (Connection conn = ds.getConnection()) {
            conn.setAutoCommit(false);
            try (PreparedStatement statement = conn.prepareStatement(sql)) {
                statement.setObject(1, UUID.fromString((String)event.get(Constants.HOST)));
                statement.setObject(2, UUID.fromString((String)map.get("instanceAppId")));
                statement.setObject(3, UUID.fromString((String)map.get("instanceApiId")));

                int count = statement.executeUpdate();
                if (count == 0) {
                    throw new SQLException("failed to delete the instance app api");
                }
                conn.commit();
                result = Success.of(String.format("Instance app api deleted for hostId: %s, instanceAppId: %s, instanceApiId: %s",
                        event.get(Constants.HOST), map.get("instanceAppId"), map.get("instanceApiId")));
                notificationService.insertNotification(event, true, null);

            } catch (SQLException e) {
                logger.error("SQLException:", e);
                conn.rollback();
                notificationService.insertNotification(event, false, e.getMessage());
                result = Failure.of(new Status(SQL_EXCEPTION, e.getMessage()));
            }  catch (Exception e) {
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
    public Result<String> getInstanceAppApi(int offset, int limit, String hostId, String instanceAppId, String instanceApiId,
                                            String instanceId, String instanceName, String productId, String productVersion,
                                            String appId, String appVersion, String apiVersionId, String apiId,
                                            String apiVersion, Boolean active) {
        Result<String> result = null;
        String s =
                """
                                SELECT COUNT(*) OVER () AS total,
                                iaa.host_id, iaa.instance_app_id, iap.app_id, iap.app_version,
                                iaa.instance_api_id, iai.instance_id, i.instance_name, pv.product_id,
                                pv.product_version, iai.api_version_id, av.api_id, av.api_version, iaa.active,
                                iaa.update_user, iaa.update_ts
                                FROM instance_app_api_t iaa
                                INNER JOIN instance_app_t iap ON iaa.instance_app_id = iap.instance_app_id
                                INNER JOIN app_t a ON iap.app_id = a.app_id
                                INNER JOIN instance_api_t iai ON iaa.instance_api_id = iai.instance_api_id
                                INNER JOIN instance_t i ON i.instance_id = iai.instance_id
                                INNER JOIN product_version_t pv ON pv.product_version_id = i.product_version_id
                                INNER JOIN api_version_t av ON av.api_version_id = iai.api_version_id
                                INNER JOIN api_t ai ON ai.api_id = av.api_id
                                WHERE 1=1
                        """;
        StringBuilder sqlBuilder = new StringBuilder();
        sqlBuilder.append(s).append("\n");

        List<Object> parameters = new ArrayList<>();

        StringBuilder whereClause = new StringBuilder();

        addCondition(whereClause, parameters, "iaa.host_id", hostId != null ? UUID.fromString(hostId) : null);
        addCondition(whereClause, parameters, "iaa.instance_app_id", instanceAppId != null ? UUID.fromString(instanceAppId) : null);
        addCondition(whereClause, parameters, "iaa.instance_api_id", instanceApiId != null ? UUID.fromString(instanceApiId) : null);
        addCondition(whereClause, parameters, "iai.instance_id", instanceId != null ? UUID.fromString(instanceId) : null);
        addCondition(whereClause, parameters, "i.instance_name", instanceName);
        addCondition(whereClause, parameters, "pv.product_id", productId);
        addCondition(whereClause, parameters, "pv.product_version", productVersion);
        addCondition(whereClause, parameters, "iap.app_id", appId);
        addCondition(whereClause, parameters, "iap.app_version", appVersion);
        addCondition(whereClause, parameters, "iai.api_version_id", apiVersionId != null ? UUID.fromString(apiVersionId) : null);
        addCondition(whereClause, parameters, "av.api_id", apiId);
        addCondition(whereClause, parameters, "av.api_version", apiVersion);
        addCondition(whereClause, parameters, "active", active);

        if (!whereClause.isEmpty()) {
            sqlBuilder.append("AND ").append(whereClause);
        }

        sqlBuilder.append(" ORDER BY i.instance_name, iap.app_id, av.api_id\n" +
                "LIMIT ? OFFSET ?");

        parameters.add(limit);
        parameters.add(offset);

        String sql = sqlBuilder.toString();
        int total = 0;
        List<Map<String, Object>> instanceApis = new ArrayList<>();

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
                    map.put("instanceAppId", resultSet.getObject("instance_app_id", UUID.class));
                    map.put("instanceApiId", resultSet.getObject("instance_api_id", UUID.class));
                    map.put("instanceId", resultSet.getObject("instance_id", UUID.class));
                    map.put("instanceName", resultSet.getString("instance_name"));
                    map.put("productId", resultSet.getString("product_id"));
                    map.put("productVersion", resultSet.getString("product_version"));
                    map.put("appId", resultSet.getString("app_id"));
                    map.put("appVersion", resultSet.getString("app_version"));
                    map.put("apiVersionId", resultSet.getObject("api_version_id", UUID.class));
                    map.put("apiId", resultSet.getString("api_id"));
                    map.put("apiVersion", resultSet.getString("api_version"));
                    map.put("active", resultSet.getBoolean("active"));
                    map.put("updateUser", resultSet.getString("update_user"));
                    map.put("updateTs", resultSet.getObject("update_ts") != null ? resultSet.getObject("update_ts", OffsetDateTime.class) : null);
                    instanceApis.add(map);
                }
            }

            Map<String, Object> resultMap = new HashMap<>();
            resultMap.put("total", total);
            resultMap.put("instanceAppApis", instanceApis);
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
    public Result<String> createInstanceApp(Map<String, Object> event) {
        final String sql = "INSERT INTO instance_app_t(host_id, instance_app_id, instance_id, app_id, app_version, active, update_user, update_ts) " +
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?)";
        Result<String> result;
        Map<String, Object> map = (Map<String, Object>)event.get(PortalConstants.DATA);
        try (Connection conn = ds.getConnection()) {
            conn.setAutoCommit(false);
            try (PreparedStatement statement = conn.prepareStatement(sql)) {
                statement.setObject(1, UUID.fromString((String)event.get(Constants.HOST)));
                statement.setObject(2, UUID.fromString((String)map.get("instanceAppId")));
                statement.setObject(3, UUID.fromString((String)map.get("instanceId")));
                statement.setString(4, (String)map.get("appId"));
                statement.setString(5, (String)map.get("appVersion"));
                if (map.containsKey("active")) {
                    statement.setBoolean(6, (Boolean) map.get("active"));
                } else {
                    statement.setNull(6, Types.BOOLEAN);
                }
                statement.setString(7, (String)event.get(Constants.USER));
                statement.setObject(8, OffsetDateTime.parse((String)event.get(CloudEventV1.TIME)));
                int count = statement.executeUpdate();
                if (count == 0) {
                    throw new SQLException("failed to insert the instance app with event " + event);
                }
                conn.commit();
                result = Success.of(String.format("Instance App created for hostId: %s, instanceAppId: %s",
                        event.get(Constants.HOST), map.get("instanceAppId")));
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
    public Result<String> updateInstanceApp(Map<String, Object> event) {
        final String sql = "UPDATE instance_app_t SET instance_id = ?, app_id = ?, app_version = ?, active = ?, update_user = ?, update_ts = ? " +
                "WHERE host_id = ? and instance_app_id = ?";
        Result<String> result;
        Map<String, Object> map = (Map<String, Object>)event.get(PortalConstants.DATA);
        try (Connection conn = ds.getConnection()) {
            conn.setAutoCommit(false);
            try (PreparedStatement statement = conn.prepareStatement(sql)) {
                statement.setObject(1, UUID.fromString((String)map.get("instanceId")));
                statement.setString(2, (String)map.get("appId"));
                statement.setString(3, (String)map.get("appVersion"));
                Boolean active = (Boolean)map.get("active");
                if (active != null) {
                    statement.setBoolean(4, active);
                } else {
                    statement.setNull(4, Types.BOOLEAN);
                }
                statement.setString(5, (String)event.get(Constants.USER));
                statement.setObject(6, OffsetDateTime.parse((String)event.get(CloudEventV1.TIME)));
                statement.setObject(7, UUID.fromString((String)event.get(Constants.HOST)));
                statement.setObject(8, UUID.fromString((String)map.get("instanceAppId")));

                int count = statement.executeUpdate();
                if (count == 0) {
                    throw new SQLException("failed to update the instance app with instanceAppId " + map.get("instanceAppId"));
                }
                conn.commit();
                result = Success.of(String.format("Instance App updated for hostId: %s, instanceAppId: %s",
                        event.get(Constants.HOST), map.get("instanceAppId")));
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
    public Result<String> deleteInstanceApp(Map<String, Object> event) {
        final String sql = "DELETE FROM instance_app_t WHERE host_id = ? AND instance_app_id = ?";
        Result<String> result;
        Map<String, Object> map = (Map<String, Object>)event.get(PortalConstants.DATA);
        try (Connection conn = ds.getConnection()) {
            conn.setAutoCommit(false);
            try (PreparedStatement statement = conn.prepareStatement(sql)) {
                statement.setObject(1, UUID.fromString((String)event.get(Constants.HOST)));
                statement.setObject(2, UUID.fromString((String)map.get("instanceAppId")));

                int count = statement.executeUpdate();
                if (count == 0) {
                    throw new SQLException("failed to delete the instance app with instanceAppId " + map.get("instanceAppId"));
                }
                conn.commit();
                result = Success.of(String.format("Instance app deleted for hostId: %s, instanceAppId: %s",
                        event.get(Constants.HOST), map.get("instanceAppId")));
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
    public Result<String> getInstanceApp(int offset, int limit, String hostId, String instanceAppId, String instanceId, String instanceName,
                                         String productId, String productVersion, String appId, String appVersion, Boolean active) {
        Result<String> result = null;
        String s =
                """
                        SELECT COUNT(*) OVER () AS total,
                        ia.host_id, ia.instance_app_id, ia.instance_id, i.instance_name, pv.product_id, pv.product_version,\s
                        ia.app_id, ia.app_version, ia.active, ia.update_user, ia.update_ts
                        FROM instance_app_t ia
                        INNER JOIN instance_t i ON ia.instance_id = i.instance_id
                        INNER JOIN product_version_t pv ON i.product_version_id = pv.product_version_id
                        WHERE 1=1
                """;
        StringBuilder sqlBuilder = new StringBuilder();
        sqlBuilder.append(s).append("\n");

        List<Object> parameters = new ArrayList<>();

        StringBuilder whereClause = new StringBuilder();

        addCondition(whereClause, parameters, "ia.host_id", hostId != null ? UUID.fromString(hostId) : null);
        addCondition(whereClause, parameters, "ia.instance_app_id", instanceAppId != null ? UUID.fromString(instanceAppId) : null);
        addCondition(whereClause, parameters, "ia.instance_id", instanceId != null ? UUID.fromString(instanceId) : null);
        addCondition(whereClause, parameters, "i.instance_name", instanceName);
        addCondition(whereClause, parameters, "pv.product_id", productId);
        addCondition(whereClause, parameters, "pv.product_version", productVersion);
        addCondition(whereClause, parameters, "ia.app_id", appId);
        addCondition(whereClause, parameters, "ia.app_version", appVersion);
        addCondition(whereClause, parameters, "ia.active", active);

        if (!whereClause.isEmpty()) {
            sqlBuilder.append("AND ").append(whereClause);
        }

        sqlBuilder.append(" ORDER BY instance_id, app_id, app_version\n" +
                "LIMIT ? OFFSET ?");

        parameters.add(limit);
        parameters.add(offset);

        String sql = sqlBuilder.toString();
        if(logger.isTraceEnabled()) logger.trace("sql = {}", sql);
        int total = 0;
        List<Map<String, Object>> instanceApps = new ArrayList<>();

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
                    map.put("instanceAppId", resultSet.getObject("instance_app_id", UUID.class));
                    map.put("instanceId", resultSet.getObject("instance_id", UUID.class));
                    map.put("instanceName", resultSet.getString("instance_name"));
                    map.put("productId", resultSet.getString("product_id"));
                    map.put("productVersion", resultSet.getString("product_version"));
                    map.put("appId", resultSet.getString("app_id"));
                    map.put("appVersion", resultSet.getString("app_version"));
                    map.put("active", resultSet.getBoolean("active"));
                    map.put("updateUser", resultSet.getString("update_user"));
                    map.put("updateTs", resultSet.getObject("update_ts") != null ? resultSet.getObject("update_ts", OffsetDateTime.class) : null);
                    instanceApps.add(map);
                }
            }

            Map<String, Object> resultMap = new HashMap<>();
            resultMap.put("total", total);
            resultMap.put("instanceApps", instanceApps);
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

    public Result<String> getInstanceAppLabel(String hostId, String instanceId) {
        Result<String> result = null;
        String sql =
                """
                        SELECT ia.instance_app_id, i.instance_name, ia.app_id, ia.app_version
                        FROM instance_app_t ia
                        INNER JOIN instance_t i ON i.instance_id = ia.instance_id
                        WHERE ia.host_id = ?
                """;
        if(instanceId != null) {
            sql += " AND ia.instance_id = ?";
        }
        List<Map<String, Object>> labels = new ArrayList<>();
        try (Connection connection = ds.getConnection();
             PreparedStatement preparedStatement = connection.prepareStatement(sql)) {
            preparedStatement.setObject(1, UUID.fromString(hostId));
            if(instanceId != null) {
                preparedStatement.setObject(2, UUID.fromString(instanceId));
            }
            try (ResultSet resultSet = preparedStatement.executeQuery()) {
                while (resultSet.next()) {
                    Map<String, Object> map = new HashMap<>();
                    map.put("id", resultSet.getString("instance_app_id"));
                    map.put("label", resultSet.getString("app_id") + "|" + resultSet.getString("app_version") + "|" + resultSet.getString("instance_name"));
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
    public Result<String> createProduct(Map<String, Object> event) {
        final String sql = "INSERT INTO product_version_t(host_id, product_version_id, product_id, product_version, " +
                "light4j_version, break_code, break_config, release_note, version_desc, release_type, current, " +
                "version_status, update_user, update_ts) " +
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
        final String sqlUpdate = "UPDATE product_version_t SET current = false \n" +
                "WHERE host_id = ?\n" +
                "AND product_id = ?\n" +
                "AND product_version != ?";
        Result<String> result;
        Map<String, Object> map = (Map<String, Object>)event.get(PortalConstants.DATA);
        try (Connection conn = ds.getConnection()) {
            conn.setAutoCommit(false);
            try (PreparedStatement statement = conn.prepareStatement(sql)) {
                statement.setObject(1, UUID.fromString((String)event.get(Constants.HOST)));
                statement.setObject(2, UUID.fromString((String)map.get("productVersionId")));
                statement.setString(3, (String)map.get("productId"));
                statement.setString(4, (String)map.get("productVersion"));
                statement.setString(5, (String)map.get("light4jVersion"));
                if (map.containsKey("breakCode")) {
                    statement.setBoolean(6, (Boolean) map.get("breakCode"));
                } else {
                    statement.setNull(6, Types.BOOLEAN);
                }
                if (map.containsKey("breakConfig")) {
                    statement.setBoolean(7, (Boolean) map.get("breakConfig"));
                } else {
                    statement.setNull(7, Types.BOOLEAN);
                }
                if (map.containsKey("releaseNote")) {
                    statement.setString(8, (String) map.get("releaseNote"));
                } else {
                    statement.setNull(8, Types.VARCHAR);
                }
                if (map.containsKey("versionDesc")) {
                    statement.setString(9, (String) map.get("versionDesc"));
                } else {
                    statement.setNull(9, Types.VARCHAR);
                }
                statement.setString(10, (String)map.get("releaseType"));
                statement.setBoolean(11, (Boolean)map.get("current"));
                statement.setString(12, (String)map.get("versionStatus"));
                statement.setString(13, (String)event.get(Constants.USER));
                statement.setObject(14, OffsetDateTime.parse((String)event.get(CloudEventV1.TIME)));

                int count = statement.executeUpdate();
                if (count == 0) {
                    throw new SQLException("failed to insert the product with id " + map.get("productId"));
                }
                // try to update current to false for others if current is true.
                if((Boolean)map.get("current")) {
                    try (PreparedStatement statementUpdate = conn.prepareStatement(sqlUpdate)) {
                        statementUpdate.setObject(1, UUID.fromString((String)event.get(Constants.HOST)));
                        statementUpdate.setString(2, (String)map.get("productId"));
                        statementUpdate.setString(3, (String)map.get("productVersion"));
                        statementUpdate.executeUpdate();
                    }
                }
                conn.commit();
                result = Success.of((String)map.get("productId"));
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
    public Result<String> updateProduct(Map<String, Object> event) {
        final String sql = "UPDATE product_version_t SET light4j_version = ?, break_code = ?, break_config = ?, " +
                "release_note = ?, version_desc = ?, release_type = ?, current = ?, version_status = ?, update_user = ?, update_ts = ? " +
                "WHERE host_id = ? AND product_version_id = ?";
        final String sqlUpdate = "UPDATE product_version_t SET current = false \n" +
                "WHERE host_id = ?\n" +
                "AND product_id = ?\n" +
                "AND product_version != ?";

        Result<String> result;
        Map<String, Object> map = (Map<String, Object>)event.get(PortalConstants.DATA);
        try (Connection conn = ds.getConnection()) {
            conn.setAutoCommit(false);
            try (PreparedStatement statement = conn.prepareStatement(sql)) {
                statement.setString(1, (String)map.get("light4jVersion"));
                if (map.containsKey("breakCode")) {
                    statement.setBoolean(2, (Boolean) map.get("breakCode"));
                } else {
                    statement.setNull(2, Types.BOOLEAN);
                }
                if (map.containsKey("breakConfig")) {
                    statement.setBoolean(3, (Boolean) map.get("breakConfig"));
                } else {
                    statement.setNull(3, Types.BOOLEAN);
                }
                if (map.containsKey("releaseNote")) {
                    statement.setString(4, (String) map.get("releaseNote"));
                } else {
                    statement.setNull(4, Types.VARCHAR);
                }

                if (map.containsKey("versionDesc")) {
                    statement.setString(5, (String) map.get("versionDesc"));
                } else {
                    statement.setNull(5, Types.VARCHAR);
                }
                statement.setString(6, (String)map.get("releaseType"));
                statement.setBoolean(7, (Boolean)map.get("current"));
                statement.setString(8, (String)map.get("versionStatus"));
                statement.setString(9, (String)event.get(Constants.USER));
                statement.setObject(10, OffsetDateTime.parse((String)event.get(CloudEventV1.TIME)));
                statement.setObject(11, UUID.fromString((String)event.get(Constants.HOST)));
                statement.setObject(12, UUID.fromString((String)map.get("productVersionId")));

                int count = statement.executeUpdate();
                if (count == 0) {
                    throw new SQLException("failed to update the product with id " + map.get("productId"));
                }
                // try to update current to false for others if current is true.
                if((Boolean)map.get("current")) {
                    try (PreparedStatement statementUpdate = conn.prepareStatement(sqlUpdate)) {
                        statementUpdate.setObject(1, UUID.fromString((String)event.get(Constants.HOST)));
                        statementUpdate.setString(2, (String)map.get("productId"));
                        statementUpdate.setString(3, (String)map.get("productVersion"));
                        statementUpdate.executeUpdate();
                    }
                }
                conn.commit();
                result = Success.of((String)map.get("productId"));
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
    public Result<String> deleteProduct(Map<String, Object> event) {
        final String sql = "DELETE FROM product_version_t WHERE host_id = ? " +
                "AND product_version_id = ?";
        Result<String> result;
        Map<String, Object> map = (Map<String, Object>)event.get(PortalConstants.DATA);
        try (Connection conn = ds.getConnection()) {
            conn.setAutoCommit(false);
            try (PreparedStatement statement = conn.prepareStatement(sql)) {
                statement.setObject(1, UUID.fromString((String)event.get(Constants.HOST)));
                statement.setObject(2, UUID.fromString((String)map.get("productVersionId")));

                int count = statement.executeUpdate();
                if (count == 0) {
                    throw new SQLException("failed to delete the product with id " + map.get("productId"));
                }
                conn.commit();
                result = Success.of((String)map.get("productId"));
                notificationService.insertNotification(event, true, null);


            } catch (SQLException e) {
                logger.error("SQLException:", e);
                conn.rollback();
                notificationService.insertNotification(event, false, e.getMessage());
                result = Failure.of(new Status(SQL_EXCEPTION, e.getMessage()));
            }
            catch (Exception e) {
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
    public Result<String> getProduct(int offset, int limit, String hostId, String productVersionId, String productId, String productVersion,
                                     String light4jVersion, Boolean breakCode, Boolean breakConfig, String releaseNote,
                                     String versionDesc, String releaseType, Boolean current, String versionStatus) {
        Result<String> result = null;
        String s = """
                SELECT COUNT(*) OVER () AS total,
                host_id, product_version_id, product_id, product_version, light4j_version, break_code, break_config,
                release_note, version_desc, release_type, current, version_status, update_user, update_ts
                FROM product_version_t
                WHERE 1=1
                """;

        StringBuilder sqlBuilder = new StringBuilder(s);
        List<Object> parameters = new ArrayList<>();
        StringBuilder whereClause = new StringBuilder();
        addCondition(whereClause, parameters, "host_id", hostId != null ? UUID.fromString(hostId) : null);
        addCondition(whereClause, parameters, "product_version_id", productVersionId != null ? UUID.fromString(productVersionId) : null);
        addCondition(whereClause, parameters, "product_id", productId);
        addCondition(whereClause, parameters, "product_version", productVersion);
        addCondition(whereClause, parameters, "light4j_version", light4jVersion);
        addCondition(whereClause, parameters, "break_code", breakCode);
        addCondition(whereClause, parameters, "break_config", breakConfig);
        addCondition(whereClause, parameters, "release_note", releaseNote);
        addCondition(whereClause, parameters, "version_desc", versionDesc);
        addCondition(whereClause, parameters, "release_type", releaseType);
        addCondition(whereClause, parameters, "current", current);
        addCondition(whereClause, parameters, "version_status", versionStatus);

        if (!whereClause.isEmpty()) {
            sqlBuilder.append("AND ").append(whereClause);
        }

        sqlBuilder.append(" ORDER BY product_id, product_version DESC\n" +
                "LIMIT ? OFFSET ?");

        parameters.add(limit);
        parameters.add(offset);

        String sql = sqlBuilder.toString();
        int total = 0;
        List<Map<String, Object>> products = new ArrayList<>();

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
                    map.put("productVersionId", resultSet.getObject("product_version_id", UUID.class));
                    map.put("productId", resultSet.getString("product_id"));
                    map.put("productVersion", resultSet.getString("product_version"));
                    map.put("light4jVersion", resultSet.getString("light4j_version"));
                    map.put("breakCode", resultSet.getBoolean("break_code"));
                    map.put("breakConfig", resultSet.getBoolean("break_config"));
                    map.put("releaseNote", resultSet.getString("release_note"));
                    map.put("versionDesc", resultSet.getString("version_desc"));
                    map.put("releaseType", resultSet.getString("release_type"));
                    map.put("current", resultSet.getBoolean("current"));
                    map.put("versionStatus", resultSet.getString("version_status"));
                    map.put("updateUser", resultSet.getString("update_user"));
                    // handling date properly
                    map.put("updateTs", resultSet.getObject("update_ts") != null ? resultSet.getObject("update_ts", OffsetDateTime.class) : null);
                    products.add(map);
                }
            }

            Map<String, Object> resultMap = new HashMap<>();
            resultMap.put("total", total);
            resultMap.put("products", products);
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
    public Result<String> getProductIdLabel(String hostId) {
        Result<String> result = null;
        String sql = "SELECT DISTINCT product_id FROM product_version_t WHERE host_id = ?";
        List<Map<String, Object>> labels = new ArrayList<>();
        try (Connection connection = ds.getConnection();
             PreparedStatement preparedStatement = connection.prepareStatement(sql)) {
            preparedStatement.setObject(1, UUID.fromString(hostId));
            try (ResultSet resultSet = preparedStatement.executeQuery()) {
                while (resultSet.next()) {
                    Map<String, Object> map = new HashMap<>();
                    map.put("id", resultSet.getString("product_id"));
                    map.put("label", resultSet.getString("product_id"));
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
    public Result<String> getProductVersionLabel(String hostId, String productId) {
        Result<String> result = null;
        String sql = "SELECT product_version FROM product_version_t WHERE host_id = ? AND product_id = ?";
        List<Map<String, Object>> versions = new ArrayList<>();
        try (Connection connection = ds.getConnection();
             PreparedStatement preparedStatement = connection.prepareStatement(sql)) {
            preparedStatement.setObject(1, UUID.fromString(hostId));
            preparedStatement.setString(2, productId);
            try (ResultSet resultSet = preparedStatement.executeQuery()) {
                while (resultSet.next()) {
                    Map<String, Object> map = new HashMap<>();
                    String id = resultSet.getString("product_version");
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
    public Result<String> getProductVersionIdLabel(String hostId) {
        Result<String> result = null;
        String sql = "SELECT DISTINCT product_version_id, product_id, product_version FROM product_version_t WHERE host_id = ?";
        List<Map<String, Object>> labels = new ArrayList<>();
        try (Connection connection = ds.getConnection();
             PreparedStatement preparedStatement = connection.prepareStatement(sql)) {
            preparedStatement.setObject(1, UUID.fromString(hostId));
            try (ResultSet resultSet = preparedStatement.executeQuery()) {
                while (resultSet.next()) {
                    Map<String, Object> map = new HashMap<>();
                    map.put("id", resultSet.getString("product_version_id"));
                    map.put("label", resultSet.getString("product_id") + "|" + resultSet.getString("product_version"));
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
    public Result<String> createProductVersionEnvironment(Map<String, Object> event) {
        final String sql = "INSERT INTO product_version_environment_t(host_id, product_version_id, " +
                "system_env, runtime_env, update_user, update_ts) " +
                "VALUES (?, ?, ?, ?, ?, ?)";
        Result<String> result;
        Map<String, Object> map = (Map<String, Object>)event.get(PortalConstants.DATA);
        try (Connection conn = ds.getConnection()) {
            conn.setAutoCommit(false);
            try (PreparedStatement statement = conn.prepareStatement(sql)) {
                statement.setObject(1, UUID.fromString((String)event.get(Constants.HOST)));
                statement.setObject(2, UUID.fromString((String)map.get("productVersionId")));
                statement.setString(3, (String)map.get("systemEnv"));
                statement.setString(4, (String)map.get("runtimeEnv"));
                statement.setString(5, (String)event.get(Constants.USER));
                statement.setObject(6, OffsetDateTime.parse((String)event.get(CloudEventV1.TIME)));

                int count = statement.executeUpdate();
                if (count == 0) {
                    throw new SQLException("failed to insert the product version environment with id " + map.get("productVersionId"));
                }
                conn.commit();
                result = Success.of((String)map.get("productVersionId"));
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
    public Result<String> deleteProductVersionEnvironment(Map<String, Object> event) {
        final String sql = "DELETE FROM product_version_environment_t WHERE host_id = ? " +
                "AND product_version_id = ? AND system_env = ? AND runtime_env = ?";
        Result<String> result;
        Map<String, Object> map = (Map<String, Object>)event.get(PortalConstants.DATA);
        try (Connection conn = ds.getConnection()) {
            conn.setAutoCommit(false);
            try (PreparedStatement statement = conn.prepareStatement(sql)) {
                statement.setObject(1, UUID.fromString((String)event.get(Constants.HOST)));
                statement.setObject(2, UUID.fromString((String)map.get("productVersionId")));
                statement.setString(3, (String)map.get("systemEnv"));
                statement.setString(4, (String)map.get("runtimeEnv"));

                int count = statement.executeUpdate();
                if (count == 0) {
                    throw new SQLException("failed to delete the product version environment with id " + map.get("productVersionId"));
                }
                conn.commit();
                result = Success.of((String)map.get("productVersionId"));
                notificationService.insertNotification(event, true, null);
            } catch (SQLException e) {
                logger.error("SQLException:", e);
                conn.rollback();
                notificationService.insertNotification(event, false, e.getMessage());
                result = Failure.of(new Status(SQL_EXCEPTION, e.getMessage()));
            }
            catch (Exception e) {
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
    public Result<String> getProductVersionEnvironment(int offset, int limit, String hostId, String productVersionId,
                                                       String productId, String productVersion, String systemEnv, String runtimeEnv) {
        Result<String> result = null;
        String s =
                """
                SELECT COUNT(*) OVER () AS total,
                pve.host_id, pve.product_version_id, pv.product_id, pv.product_version,
                pve.system_env, pve.runtime_env, pve.update_user, pve.update_ts
                FROM product_version_environment_t pve
                INNER JOIN product_version_t pv ON pv.product_version_id = pve.product_version_id
                WHERE 1=1
                """;

        StringBuilder sqlBuilder = new StringBuilder(s);
        List<Object> parameters = new ArrayList<>();
        StringBuilder whereClause = new StringBuilder();
        addCondition(whereClause, parameters, "pve.host_id", hostId != null ? UUID.fromString(hostId) : null);
        addCondition(whereClause, parameters, "pve.product_version_id", productVersionId != null ? UUID.fromString(productVersionId) : null);
        addCondition(whereClause, parameters, "pv.product_id", productId);
        addCondition(whereClause, parameters, "pv.product_version", productVersion);
        addCondition(whereClause, parameters, "pve.system_env", systemEnv);
        addCondition(whereClause, parameters, "pve.runtime_env", runtimeEnv);

        if (!whereClause.isEmpty()) {
            sqlBuilder.append("AND ").append(whereClause);
        }

        sqlBuilder.append(" ORDER BY pv.product_id, pv.product_version, pve.system_env, pve.runtime_env DESC\n" +
                "LIMIT ? OFFSET ?");

        parameters.add(limit);
        parameters.add(offset);

        String sql = sqlBuilder.toString();
        int total = 0;
        List<Map<String, Object>> productEnvironments = new ArrayList<>();

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
                    map.put("productVersionId", resultSet.getObject("product_version_id", UUID.class));
                    map.put("productId", resultSet.getString("product_id"));
                    map.put("productVersion", resultSet.getString("product_version"));
                    map.put("systemEnv", resultSet.getString("system_env"));
                    map.put("runtimeEnv", resultSet.getString("runtime_env"));
                    map.put("updateUser", resultSet.getString("update_user"));
                    map.put("updateTs", resultSet.getObject("update_ts") != null ? resultSet.getObject("update_ts", OffsetDateTime.class) : null);
                    productEnvironments.add(map);
                }
            }
            Map<String, Object> resultMap = new HashMap<>();
            resultMap.put("total", total);
            resultMap.put("productEnvironments", productEnvironments);
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
    public Result<String> createProductVersionPipeline(Map<String, Object> event) {
        final String sql = "INSERT INTO product_version_pipeline_t(host_id, product_version_id, " +
                "pipeline_id, update_user, update_ts) " +
                "VALUES (?, ?, ?, ?, ?)";
        Result<String> result;
        Map<String, Object> map = (Map<String, Object>)event.get(PortalConstants.DATA);
        try (Connection conn = ds.getConnection()) {
            conn.setAutoCommit(false);
            try (PreparedStatement statement = conn.prepareStatement(sql)) {
                statement.setObject(1, UUID.fromString((String)event.get(Constants.HOST)));
                statement.setObject(2, UUID.fromString((String)map.get("productVersionId")));
                statement.setObject(3, UUID.fromString((String)map.get("pipelineId")));
                statement.setString(4, (String)event.get(Constants.USER));
                statement.setObject(5, OffsetDateTime.parse((String)event.get(CloudEventV1.TIME)));

                int count = statement.executeUpdate();
                if (count == 0) {
                    throw new SQLException("failed to insert the product version pipeline with id " + map.get("productVersionId"));
                }
                conn.commit();
                result = Success.of((String)map.get("productVersionId"));
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
    public Result<String> deleteProductVersionPipeline(Map<String, Object> event) {
        final String sql = "DELETE FROM product_version_pipeline_t WHERE host_id = ? " +
                "AND product_version_id = ? AND pipeline_id = ?";
        Result<String> result;
        Map<String, Object> map = (Map<String, Object>)event.get(PortalConstants.DATA);
        try (Connection conn = ds.getConnection()) {
            conn.setAutoCommit(false);
            try (PreparedStatement statement = conn.prepareStatement(sql)) {
                statement.setObject(1, UUID.fromString((String)event.get(Constants.HOST)));
                statement.setObject(2, UUID.fromString((String)map.get("productVersionId")));
                statement.setObject(3, UUID.fromString((String)map.get("pipelineId")));

                int count = statement.executeUpdate();
                if (count == 0) {
                    throw new SQLException("failed to delete the product version pipeline with id " + map.get("productVersionId"));
                }
                conn.commit();
                result = Success.of((String)map.get("productVersionId"));
                notificationService.insertNotification(event, true, null);
            } catch (SQLException e) {
                logger.error("SQLException:", e);
                conn.rollback();
                notificationService.insertNotification(event, false, e.getMessage());
                result = Failure.of(new Status(SQL_EXCEPTION, e.getMessage()));
            }
            catch (Exception e) {
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
    public Result<String> getProductVersionPipeline(int offset, int limit, String hostId, String productVersionId,
                                                    String productId, String productVersion, String pipelineId,
                                                    String pipelineName, String pipelineVersion) {
        Result<String> result = null;
        String s =
                """
                SELECT COUNT(*) OVER () AS total,
                pvp.host_id, pvp.product_version_id, pv.product_id, pv.product_version,
                pvp.pipeline_id, p.pipeline_name, p.pipeline_version, pvp.update_user, pvp.update_ts
                FROM product_version_pipeline_t pvp
                INNER JOIN product_version_t pv ON pv.product_version_id = pvp.product_version_id
                INNER JOIN pipeline_t p ON p.pipeline_id = pvp.pipeline_id
                WHERE 1=1
                """;

        StringBuilder sqlBuilder = new StringBuilder(s);
        List<Object> parameters = new ArrayList<>();
        StringBuilder whereClause = new StringBuilder();
        addCondition(whereClause, parameters, "pvp.host_id", hostId != null ? UUID.fromString(hostId) : null);
        addCondition(whereClause, parameters, "pvp.product_version_id", productVersionId != null ? UUID.fromString(productVersionId) : null);
        addCondition(whereClause, parameters, "pv.product_id", productId);
        addCondition(whereClause, parameters, "pv.product_version", productVersion);
        addCondition(whereClause, parameters, "pvp.pipeline_id", pipelineId != null ? UUID.fromString(pipelineId) : null);
        addCondition(whereClause, parameters, "p.pipeline_name", pipelineName);
        addCondition(whereClause, parameters, "p.pipeline_version", pipelineVersion);

        if (!whereClause.isEmpty()) {
            sqlBuilder.append("AND ").append(whereClause);
        }

        sqlBuilder.append(" ORDER BY pv.product_id, pv.product_version, p.pipeline_name, p.pipeline_version DESC\n" +
                "LIMIT ? OFFSET ?");

        parameters.add(limit);
        parameters.add(offset);

        String sql = sqlBuilder.toString();
        int total = 0;
        List<Map<String, Object>> productPipelines = new ArrayList<>();

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
                    map.put("productVersionId", resultSet.getObject("product_version_id", UUID.class));
                    map.put("productId", resultSet.getString("product_id"));
                    map.put("productVersion", resultSet.getString("product_version"));
                    map.put("pipelineId", resultSet.getObject("pipeline_id", UUID.class));
                    map.put("pipelineName", resultSet.getString("pipeline_name"));
                    map.put("pipelineVersion", resultSet.getString("pipeline_version"));
                    map.put("updateUser", resultSet.getString("update_user"));
                    map.put("updateTs", resultSet.getObject("update_ts") != null ? resultSet.getObject("update_ts", OffsetDateTime.class) : null);
                    productPipelines.add(map);
                }
            }
            Map<String, Object> resultMap = new HashMap<>();
            resultMap.put("total", total);
            resultMap.put("productPipelines", productPipelines);
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
    public Result<String> createProductVersionConfig(Map<String, Object> event) {
        final String sql = "INSERT INTO product_version_config_t(host_id, product_version_id, " +
                "config_id, update_user, update_ts) " +
                "VALUES (?, ?, ?, ?, ?)";
        Result<String> result;
        Map<String, Object> map = (Map<String, Object>)event.get(PortalConstants.DATA);
        try (Connection conn = ds.getConnection()) {
            conn.setAutoCommit(false);
            try (PreparedStatement statement = conn.prepareStatement(sql)) {
                statement.setObject(1, UUID.fromString((String)event.get(Constants.HOST)));
                statement.setObject(2, UUID.fromString((String)map.get("productVersionId")));
                statement.setObject(3, UUID.fromString((String)map.get("configId")));
                statement.setString(4, (String)event.get(Constants.USER));
                statement.setObject(5, OffsetDateTime.parse((String)event.get(CloudEventV1.TIME)));

                int count = statement.executeUpdate();
                if (count == 0) {
                    throw new SQLException("failed to insert the product version config with id " + map.get("productVersionId") + "|" + map.get("configId"));
                }
                conn.commit();
                result = Success.of(map.get("productVersionId") + "|" + map.get("configId"));
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
    public Result<String> deleteProductVersionConfig(Map<String, Object> event) {
        final String sql = "DELETE FROM product_version_config_t WHERE host_id = ? " +
                "AND product_version_id = ? AND config_id = ?";
        Result<String> result;
        Map<String, Object> map = (Map<String, Object>)event.get(PortalConstants.DATA);
        try (Connection conn = ds.getConnection()) {
            conn.setAutoCommit(false);
            try (PreparedStatement statement = conn.prepareStatement(sql)) {
                statement.setObject(1, UUID.fromString((String)event.get(Constants.HOST)));
                statement.setObject(2, UUID.fromString((String)map.get("productVersionId")));
                statement.setObject(3, UUID.fromString((String)map.get("configId")));

                int count = statement.executeUpdate();
                if (count == 0) {
                    throw new SQLException("failed to delete the product version config with id " + map.get("productVersionId") + "|" + map.get("configId"));
                }
                conn.commit();
                result = Success.of(map.get("productVersionId") + "|" + map.get("configId"));
                notificationService.insertNotification(event, true, null);
            } catch (SQLException e) {
                logger.error("SQLException:", e);
                conn.rollback();
                notificationService.insertNotification(event, false, e.getMessage());
                result = Failure.of(new Status(SQL_EXCEPTION, e.getMessage()));
            }
            catch (Exception e) {
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
    public Result<String> getProductVersionConfig(int offset, int limit, List<SortCriterion> sorting, List<FilterCriterion> filtering, String globalFilter,
                                                  String hostId, String productVersionId, String productId, String productVersion, String configId,
                                                  String configName) {
        final Map<String, String> API_TO_DB_COLUMN_MAP = Map.of(
                "hostId", "pvc.host_id",
                "productVersionId", "pvc.product_version_id",
                "productId", "pv.product_id",
                "productVersion", "pv.product_version",
                "configId", "pvc.config_id",
                "configName", "c.config_name",
                "updateUser", "pvc.update_user",
                "updateTs", "pvc.update_ts"
        );
        // The base query with the window function for total count is efficient.
        String baseSql =
                """
                SELECT COUNT(*) OVER () AS total,
                pvc.host_id, pvc.product_version_id, pv.product_id, pv.product_version,
                pvc.config_id, c.config_name, pvc.update_user, pvc.update_ts
                FROM product_version_config_t pvc
                INNER JOIN product_version_t pv ON pv.host_id = pvc.host_id AND pv.product_version_id = pvc.product_version_id
                INNER JOIN config_t c ON c.config_id = pvc.config_id
                """;

        StringBuilder sqlBuilder = new StringBuilder(baseSql);
        List<Object> parameters = new ArrayList<>();
        List<String> whereClauses = new ArrayList<>();

        // --- 1. Build WHERE clause from filters ---

        // Add mandatory hostId filter.
        if (hostId != null) {
            whereClauses.add("pvc.host_id = ?");
            parameters.add(UUID.fromString(hostId));
        }

        // Handle dynamic column filters from MRT
        if (filtering != null) {
            for (FilterCriterion filter : filtering) {
                String dbColumn = API_TO_DB_COLUMN_MAP.get(filter.getId());
                if (dbColumn == null) {
                    logger.warn("Invalid filter column requested: {}", filter.getId());
                    continue; // Or return an error
                }
                whereClauses.add(dbColumn + " ILIKE ?"); // Use ILIKE for case-insensitive matching in PostgreSQL
                parameters.add("%" + filter.getValue() + "%");
            }
        }

        // Handle global filter from MRT
        if (globalFilter != null && !globalFilter.isBlank()) {
            List<String> globalSearchableColumns = List.of("pv.product_id", "pv.product_version", "c.config_name");
            String globalWhere = globalSearchableColumns.stream()
                    .map(col -> col + " ILIKE ?")
                    .collect(Collectors.joining(" OR "));

            whereClauses.add("(" + globalWhere + ")");
            for (int i = 0; i < globalSearchableColumns.size(); i++) {
                parameters.add("%" + globalFilter + "%");
            }
        }

        if (!whereClauses.isEmpty()) {
            sqlBuilder.append(" WHERE ").append(String.join(" AND ", whereClauses));
        }

        // --- 2. Build ORDER BY clause ---
        if (sorting != null && !sorting.isEmpty()) {
            String orderByClause = sorting.stream()
                    .map(sortCriterion -> {
                        String dbColumn = API_TO_DB_COLUMN_MAP.get(sortCriterion.getId());
                        if (dbColumn == null) {
                            logger.warn("Invalid sort column requested: {}", sortCriterion.getId());
                            return null; // This will be filtered out
                        }
                        return dbColumn + (sortCriterion.isDesc() ? " DESC" : " ASC");
                    })
                    .filter(Objects::nonNull)
                    .collect(Collectors.joining(", "));

            if (!orderByClause.isEmpty()) {
                sqlBuilder.append(" ORDER BY ").append(orderByClause);
            } else {
                // Fallback to default sort if all provided columns were invalid
                sqlBuilder.append(" ORDER BY pv.product_id, pv.product_version, c.config_name DESC");
            }
        } else {
            // Default sort order when no sorting is provided by the client
            sqlBuilder.append(" ORDER BY pv.product_id, pv.product_version, c.config_name DESC");
        }


        // --- 3. Add Pagination ---
        sqlBuilder.append(" LIMIT ? OFFSET ?");
        parameters.add(limit);
        parameters.add(offset);


        // --- 4. Execute Query ---
        String finalSql = sqlBuilder.toString();
        if(logger.isTraceEnabled()) logger.trace("Final SQL: {}, Parameters: {}", finalSql, parameters);

        int total = 0;
        List<Map<String, Object>> productConfigs = new ArrayList<>();

        try (Connection connection = ds.getConnection();
             PreparedStatement preparedStatement = connection.prepareStatement(finalSql)) {

            for (int i = 0; i < parameters.size(); i++) {
                preparedStatement.setObject(i + 1, parameters.get(i));
            }

            try (ResultSet resultSet = preparedStatement.executeQuery()) {
                boolean isFirstRow = true;
                while (resultSet.next()) {
                    if (isFirstRow) {
                        // The total is the same for every row, so we only need to read it once.
                        total = resultSet.getInt("total");
                        isFirstRow = false;
                    }
                    Map<String, Object> map = new HashMap<>();
                    map.put("hostId", resultSet.getObject("host_id", UUID.class).toString());
                    map.put("productVersionId", resultSet.getObject("product_version_id", UUID.class).toString());
                    map.put("productId", resultSet.getString("product_id"));
                    map.put("productVersion", resultSet.getString("product_version"));
                    map.put("configId", resultSet.getObject("config_id", UUID.class).toString());
                    map.put("configName", resultSet.getString("config_name"));
                    map.put("updateUser", resultSet.getString("update_user"));
                    map.put("updateTs", resultSet.getObject("update_ts") != null ? resultSet.getObject("update_ts", OffsetDateTime.class).toString() : null);
                    productConfigs.add(map);
                }
            }
            Map<String, Object> resultMap = new HashMap<>();
            resultMap.put("total", total);
            resultMap.put("productConfigs", productConfigs);
            return Success.of(JsonMapper.toJson(resultMap));
        } catch (SQLException e) {
            logger.error("SQLException executing query: " + finalSql, e);
            return Failure.of(new Status("ERR10001", "SQL_EXCEPTION", e.getMessage())); // Use your own error codes
        } catch (Exception e) {
            logger.error("Generic exception executing query: " + finalSql, e);
            return Failure.of(new Status("ERR10000", "GENERIC_EXCEPTION", e.getMessage()));
        }
    }


    @Override
    public Result<String> createProductVersionConfigProperty(Map<String, Object> event) {
        final String sql = "INSERT INTO product_version_config_property_t(host_id, product_version_id, " +
                "property_id, update_user, update_ts) " +
                "VALUES (?, ?, ?, ?, ?)";
        Result<String> result;
        Map<String, Object> map = (Map<String, Object>)event.get(PortalConstants.DATA);
        try (Connection conn = ds.getConnection()) {
            conn.setAutoCommit(false);
            try (PreparedStatement statement = conn.prepareStatement(sql)) {
                statement.setObject(1, UUID.fromString((String)event.get(Constants.HOST)));
                statement.setObject(2, UUID.fromString((String)map.get("productVersionId")));
                statement.setObject(3, UUID.fromString((String)map.get("propertyId")));
                statement.setString(4, (String)event.get(Constants.USER));
                statement.setObject(5, OffsetDateTime.parse((String)event.get(CloudEventV1.TIME)));

                int count = statement.executeUpdate();
                if (count == 0) {
                    throw new SQLException("failed to insert the product version config property with id " + map.get("productVersionId") + "|" + map.get("propertyId"));
                }
                conn.commit();
                result = Success.of(map.get("productVersionId") + "|" + map.get("propertyId"));
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
    public Result<String> deleteProductVersionConfigProperty(Map<String, Object> event) {
        final String sql = "DELETE FROM product_version_config_property_t WHERE host_id = ? " +
                "AND product_version_id = ? AND property_id = ?";
        Result<String> result;
        Map<String, Object> map = (Map<String, Object>)event.get(PortalConstants.DATA);
        try (Connection conn = ds.getConnection()) {
            conn.setAutoCommit(false);
            try (PreparedStatement statement = conn.prepareStatement(sql)) {
                statement.setObject(1, UUID.fromString((String)event.get(Constants.HOST)));
                statement.setObject(2, UUID.fromString((String)map.get("productVersionId")));
                statement.setObject(3, UUID.fromString((String)map.get("propertyId")));

                int count = statement.executeUpdate();
                if (count == 0) {
                    throw new SQLException("failed to delete the product version config property with id " + map.get("productVersionId") + "|" + map.get("propertyId"));
                }
                conn.commit();
                result = Success.of(map.get("productVersionId") + "|" + map.get("propertyId"));
                notificationService.insertNotification(event, true, null);
            } catch (SQLException e) {
                logger.error("SQLException:", e);
                conn.rollback();
                notificationService.insertNotification(event, false, e.getMessage());
                result = Failure.of(new Status(SQL_EXCEPTION, e.getMessage()));
            }
            catch (Exception e) {
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
    public Result<String> getProductVersionConfigProperty(int offset, int limit, String hostId, String productVersionId,
                                                          String productId, String productVersion, String configId,
                                                          String configName, String propertyId, String propertyName) {
        Result<String> result = null;
        String s =
                """
                        SELECT COUNT(*) OVER () AS total,
                        pvcp.host_id, pvcp.product_version_id, pv.product_id, pv.product_version,
                        cp.config_id, c.config_name, pvcp.property_id, cp.property_name, pvcp.update_user, pvcp.update_ts
                        FROM product_version_config_property_t pvcp
                        INNER JOIN product_version_t pv ON pv.host_id = pvcp.host_id AND pv.product_version_id = pvcp.product_version_id
                        INNER JOIN config_property_t cp ON cp.property_id = pvcp.property_id
                        INNER JOIN config_t c ON c.config_id = cp.config_id
                        WHERE 1=1
                        """;

        StringBuilder sqlBuilder = new StringBuilder(s);
        List<Object> parameters = new ArrayList<>();
        StringBuilder whereClause = new StringBuilder();
        addCondition(whereClause, parameters, "pvcp.host_id", hostId != null ? UUID.fromString(hostId) : null);
        addCondition(whereClause, parameters, "pvcp.product_version_id", productVersionId != null ? UUID.fromString(productVersionId) : null);
        addCondition(whereClause, parameters, "pv.product_id", productId);
        addCondition(whereClause, parameters, "pv.product_version", productVersion);
        addCondition(whereClause, parameters, "cp.config_id", configId != null ? UUID.fromString(configId) : null);
        addCondition(whereClause, parameters, "c.config_name", configName);
        addCondition(whereClause, parameters, "pvcp.property_id", propertyId != null ? UUID.fromString(propertyId) : null);
        addCondition(whereClause, parameters, "cp.property_name", propertyName);


        if (!whereClause.isEmpty()) {
            sqlBuilder.append("AND ").append(whereClause);
        }

        sqlBuilder.append(" ORDER BY pv.product_id, pv.product_version, c.config_name, cp.property_name DESC\n" +
                "LIMIT ? OFFSET ?");

        parameters.add(limit);
        parameters.add(offset);

        String sql = sqlBuilder.toString();
        int total = 0;
        List<Map<String, Object>> productProperties = new ArrayList<>();

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
                    map.put("productVersionId", resultSet.getObject("product_version_id", UUID.class));
                    map.put("productId", resultSet.getString("product_id"));
                    map.put("productVersion", resultSet.getString("product_version"));
                    map.put("configId", resultSet.getObject("config_id", UUID.class));
                    map.put("configName", resultSet.getString("config_name"));
                    map.put("propertyId", resultSet.getObject("property_id", UUID.class));
                    map.put("propertyName", resultSet.getString("property_name"));
                    map.put("updateUser", resultSet.getString("update_user"));
                    map.put("updateTs", resultSet.getObject("update_ts") != null ? resultSet.getObject("update_ts", OffsetDateTime.class) : null);
                    productProperties.add(map);
                }
            }
            Map<String, Object> resultMap = new HashMap<>();
            resultMap.put("total", total);
            resultMap.put("productProperties", productProperties);
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
    public Result<String> createPipeline(Map<String, Object> event) {
        final String sql = "INSERT INTO pipeline_t(host_id, pipeline_id, platform_id, pipeline_version, pipeline_name, " +
                "current, endpoint, version_status, system_env, runtime_env, request_schema, response_schema, update_user, update_ts) " +
                "VALUES (?, ?, ?, ?, ?,  ?, ?, ?, ?, ?,  ?, ?, ?, ?)";
        final String sqlUpdateCurrent =
                """
                UPDATE pipeline_t
                SET current = false
                WHERE host_id = ?
                AND pipeline_name = ?
                AND pipeline_id != ?
                """;
        Result<String> result;
        Map<String, Object> map = (Map<String, Object>)event.get(PortalConstants.DATA);
        try (Connection conn = ds.getConnection()) {
            conn.setAutoCommit(false);
            try (PreparedStatement statement = conn.prepareStatement(sql)) {
                statement.setObject(1, UUID.fromString((String)event.get(Constants.HOST)));
                statement.setObject(2, UUID.fromString((String)map.get("pipelineId")));
                statement.setObject(3, UUID.fromString((String)map.get("platformId")));
                statement.setString(4, (String)map.get("pipelineVersion"));
                statement.setString(5, (String)map.get("pipelineName"));
                Boolean current = (Boolean)map.get("current");
                if (current != null) {
                    statement.setBoolean(6, current);
                } else {
                    statement.setNull(6, Types.BOOLEAN);
                }

                statement.setString(7, (String)map.get("endpoint"));

                String versionStatus = (String)map.get("versionStatus");
                if (versionStatus != null && !versionStatus.isEmpty()) {
                    statement.setString(8, versionStatus);
                } else {
                    statement.setNull(8, Types.VARCHAR);
                }
                statement.setString(9, (String)map.get("systemEnv"));

                String runtimeEnv = (String)map.get("runtimeEnv");
                if (runtimeEnv != null && !runtimeEnv.isEmpty()) {
                    statement.setString(10, runtimeEnv);
                } else {
                    statement.setNull(10, Types.VARCHAR);
                }
                statement.setString(11, (String)map.get("requestSchema"));
                statement.setString(12, (String)map.get("responseSchema"));
                statement.setString(13, (String)event.get(Constants.USER));
                statement.setObject(14, OffsetDateTime.parse((String)event.get(CloudEventV1.TIME)));

                int count = statement.executeUpdate();
                if (count == 0) {
                    throw new SQLException("failed to insert the pipeline with id " + map.get("pipelineId"));
                }
                // try to update current to false for others if current is true.
                if(current != null && current) {
                    try (PreparedStatement statementUpdate = conn.prepareStatement(sqlUpdateCurrent)) {
                        statementUpdate.setObject(1, UUID.fromString((String)event.get(Constants.HOST)));
                        statementUpdate.setString(2, (String)map.get("pipelineName"));
                        statementUpdate.setObject(3, UUID.fromString((String)map.get("pipelineId")));
                        statementUpdate.executeUpdate();
                    }
                }
                conn.commit();
                result = Success.of((String)map.get("pipelineId"));
                notificationService.insertNotification(event, true, null);
            }   catch (SQLException e) {
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
    public Result<String> updatePipeline(Map<String, Object> event) {
        final String sql = "UPDATE pipeline_t SET platform_id = ?, pipeline_version = ?, pipeline_name = ?, current = ?, " +
                "endpoint = ?, version_status = ?, system_env = ?, runtime_env = ?, request_schema = ?, response_schema = ?, " +
                "update_user = ?, update_ts = ? " +
                "WHERE host_id = ? AND pipeline_id = ?";
        final String sqlUpdateCurrent =
                """
                UPDATE pipeline_t
                SET current = false
                WHERE host_id = ?
                AND pipeline_name = ?
                AND pipeline_id != ?
                """;

        Result<String> result;
        Map<String, Object> map = (Map<String, Object>)event.get(PortalConstants.DATA);
        try (Connection conn = ds.getConnection()) {
            conn.setAutoCommit(false);
            try (PreparedStatement statement = conn.prepareStatement(sql)) {
                statement.setObject(1, UUID.fromString((String)map.get("platformId")));
                statement.setString(2, (String)map.get("pipelineVersion"));
                statement.setString(3, (String)map.get("pipelineName"));
                Boolean current = (Boolean)map.get("current");
                if (current != null) {
                    statement.setBoolean(4, current);
                } else {
                    statement.setNull(4, Types.BOOLEAN);
                }
                statement.setString(5, (String)map.get("endpoint"));
                String versionStatus = (String)map.get("versionStatus");
                if (versionStatus != null && !versionStatus.isEmpty()) {
                    statement.setString(6, versionStatus);
                } else {
                    statement.setNull(6, Types.VARCHAR);
                }
                statement.setString(7, (String)map.get("systemEnv"));
                String runtimeEnv = (String)map.get("runtimeEnv");
                if (runtimeEnv != null && !runtimeEnv.isEmpty()) {
                    statement.setString(8, runtimeEnv);
                } else {
                    statement.setNull(8, Types.VARCHAR);
                }

                statement.setString(9, (String)map.get("requestSchema"));
                statement.setString(10, (String)map.get("responseSchema"));
                statement.setString(11,(String) event.get(Constants.USER));
                statement.setObject(12, OffsetDateTime.parse((String)event.get(CloudEventV1.TIME)));
                statement.setObject(13, UUID.fromString((String)event.get(Constants.HOST)));
                statement.setString(14, (String)map.get("pipelineId"));


                int count = statement.executeUpdate();
                if (count == 0) {
                    throw new SQLException("failed to update the pipeline with id " + map.get("pipelineId"));
                }
                // try to update current to false for others if current is true.
                if(current != null && current) {
                    try (PreparedStatement statementUpdate = conn.prepareStatement(sqlUpdateCurrent)) {
                        statementUpdate.setObject(1, UUID.fromString((String)event.get(Constants.HOST)));
                        statementUpdate.setString(2, (String)map.get("pipelineName"));
                        statementUpdate.setObject(3, UUID.fromString((String)map.get("pipelineId")));
                        statementUpdate.executeUpdate();
                    }
                }
                conn.commit();
                result = Success.of((String)map.get("pipelineId"));
                notificationService.insertNotification(event, true, null);

            }  catch (SQLException e) {
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
    public Result<String> deletePipeline(Map<String, Object> event) {
        final String sql = "DELETE FROM pipeline_t WHERE host_id = ? AND pipeline_id = ?";
        Result<String> result;
        Map<String, Object> map = (Map<String, Object>)event.get(PortalConstants.DATA);
        try (Connection conn = ds.getConnection()) {
            conn.setAutoCommit(false);
            try (PreparedStatement statement = conn.prepareStatement(sql)) {
                statement.setObject(1, UUID.fromString((String)event.get(Constants.HOST)));
                statement.setObject(2, UUID.fromString((String)map.get("pipelineId")));

                int count = statement.executeUpdate();
                if (count == 0) {
                    throw new SQLException("failed to delete the pipeline with id " + map.get("pipelineId"));
                }
                conn.commit();
                result = Success.of((String)map.get("pipelineId"));
                notificationService.insertNotification(event, true, null);
            }   catch (SQLException e) {
                logger.error("SQLException:", e);
                conn.rollback();
                notificationService.insertNotification(event, false, e.getMessage());
                result = Failure.of(new Status(SQL_EXCEPTION, e.getMessage()));
            }  catch (Exception e) {
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
    public Result<String> getPipeline(int offset, int limit, String hostId, String pipelineId, String platformId,
                                      String platformName, String platformVersion, String pipelineVersion,
                                      String pipelineName, Boolean current, String endpoint, String versionStatus,
                                      String systemEnv, String runtimeEnv, String requestSchema, String responseSchema) {
        Result<String> result = null;
        String s =
                """
                SELECT COUNT(*) OVER () AS total,
                p.host_id, p.pipeline_id, p.platform_id, pf.platform_name, pf.platform_version,
                p.pipeline_version, p.pipeline_name, p.current, p.endpoint, p.version_status,
                p.system_env, p.runtime_env, p.request_schema, p.response_schema, p.update_user, p.update_ts
                FROM pipeline_t p
                INNER JOIN platform_t pf ON pf.platform_id = p.platform_id
                WHERE 1=1
                """;

        StringBuilder sqlBuilder = new StringBuilder(s);
        List<Object> parameters = new ArrayList<>();
        StringBuilder whereClause = new StringBuilder();
        addCondition(whereClause, parameters, "p.host_id", hostId != null ? UUID.fromString(hostId) : null);
        addCondition(whereClause, parameters, "p.pipeline_id", pipelineId != null ? UUID.fromString(pipelineId) : null);
        addCondition(whereClause, parameters, "p.platform_id", platformId != null ? UUID.fromString(platformId) : null);
        addCondition(whereClause, parameters, "pf.platform_name", platformName);
        addCondition(whereClause, parameters, "pf.platform_version", platformVersion);
        addCondition(whereClause, parameters, "p.pipeline_version", pipelineVersion);
        addCondition(whereClause, parameters, "p.pipeline_name", pipelineName);
        addCondition(whereClause, parameters, "p.current", current);
        addCondition(whereClause, parameters, "p.endpoint", endpoint);
        addCondition(whereClause, parameters, "p.version_status", versionStatus);
        addCondition(whereClause, parameters, "p.system_env", systemEnv);
        addCondition(whereClause, parameters, "p.runtime_env", runtimeEnv);
        addCondition(whereClause, parameters, "p.request_schema", requestSchema);
        addCondition(whereClause, parameters, "p.response_schema", responseSchema);


        if (!whereClause.isEmpty()) {
            sqlBuilder.append(" AND ").append(whereClause);
        }

        sqlBuilder.append(" ORDER BY p.pipeline_id\n" +
                "LIMIT ? OFFSET ?");

        parameters.add(limit);
        parameters.add(offset);

        String sql = sqlBuilder.toString();
        // if(logger.isTraceEnabled()) logger.trace("sql = {}", sql);
        int total = 0;
        List<Map<String, Object>> pipelines = new ArrayList<>();

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
                    map.put("pipelineId", resultSet.getObject("pipeline_id", UUID.class));
                    map.put("platformId", resultSet.getObject("platform_id", UUID.class));
                    map.put("platformName", resultSet.getString("platform_name"));
                    map.put("platformVersion", resultSet.getString("platform_version"));
                    map.put("pipelineVersion", resultSet.getString("pipeline_version"));
                    map.put("pipelineName", resultSet.getString("pipeline_name"));
                    map.put("current", resultSet.getBoolean("current"));
                    map.put("endpoint", resultSet.getString("endpoint"));
                    map.put("versionStatus", resultSet.getString("version_status"));
                    map.put("systemEnv", resultSet.getString("system_env"));
                    map.put("runtimeEnv", resultSet.getString("runtime_env"));
                    map.put("requestSchema", resultSet.getString("request_schema"));
                    map.put("responseSchema", resultSet.getString("response_schema"));
                    map.put("updateUser", resultSet.getString("update_user"));
                    // handling date properly
                    map.put("updateTs", resultSet.getObject("update_ts") != null ? resultSet.getObject("update_ts", OffsetDateTime.class) : null);
                    pipelines.add(map);
                }
            }
            Map<String, Object> resultMap = new HashMap<>();
            resultMap.put("total", total);
            resultMap.put("pipelines", pipelines);
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
    public Result<String> getPipelineLabel(String hostId) {
        Result<String> result = null;
        String sql = "SELECT pipeline_id, pipeline_name, pipeline_version FROM pipeline_t WHERE host_id = ?";
        List<Map<String, Object>> labels = new ArrayList<>();
        try (Connection connection = ds.getConnection();
             PreparedStatement preparedStatement = connection.prepareStatement(sql)) {
            preparedStatement.setObject(1, UUID.fromString(hostId));
            try (ResultSet resultSet = preparedStatement.executeQuery()) {
                while (resultSet.next()) {
                    Map<String, Object> map = new HashMap<>();
                    String pipelineId = resultSet.getObject("pipeline_id", UUID.class).toString();
                    String pipelineName = resultSet.getString("pipeline_name");
                    String pipelineVersion = resultSet.getString("pipeline_version");
                    map.put("id", pipelineId);
                    map.put("label", pipelineName + "|" + pipelineVersion);
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
    public Result<String> createInstancePipeline(Map<String, Object> event) {
        final String sql = "INSERT INTO instance_pipeline_t(host_id, instance_id, pipeline_id, update_user, update_ts) " +
                "VALUES (?, ?, ?, ?, ?)";
        Result<String> result;
        Map<String, Object> map = (Map<String, Object>)event.get(PortalConstants.DATA);
        try (Connection conn = ds.getConnection()) {
            conn.setAutoCommit(false);
            try (PreparedStatement statement = conn.prepareStatement(sql)) {
                statement.setObject(1, UUID.fromString((String)event.get(Constants.HOST)));
                statement.setObject(2, UUID.fromString((String)map.get("instanceId")));
                statement.setObject(3, UUID.fromString((String)map.get("pipelineId")));
                statement.setString(4, (String)event.get(Constants.USER));
                statement.setObject(5, OffsetDateTime.parse((String)event.get(CloudEventV1.TIME)));

                int count = statement.executeUpdate();
                String key = String.format("hostId: %s, instanceId: %s pipelineId: %s",
                        event.get(Constants.HOST), map.get("instanceId"), map.get("pipelineId"));
                if (count == 0) {
                    throw new SQLException("failed to insert the instance_pipeline_t with id " + key);
                }
                conn.commit();
                result = Success.of(key);
                notificationService.insertNotification(event, true, null);
            }   catch (SQLException e) {
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
    public Result<String> updateInstancePipeline(Map<String, Object> event) {
        final String sql = "UPDATE instance_pipeline_t SET update_user = ?, update_ts = ? " +
                "WHERE host_id = ? and instance_id = ? and pipeline_id = ?";
        Result<String> result;
        Map<String, Object> map = (Map<String, Object>)event.get(PortalConstants.DATA);
        try (Connection conn = ds.getConnection()) {
            conn.setAutoCommit(false);
            try (PreparedStatement statement = conn.prepareStatement(sql)) {
                statement.setString(1,(String) event.get(Constants.USER));
                statement.setObject(2, OffsetDateTime.parse((String)event.get(CloudEventV1.TIME)));
                statement.setObject(3, UUID.fromString((String)event.get(Constants.HOST)));
                statement.setObject(4, UUID.fromString((String)map.get("instanceId")));
                statement.setObject(5, UUID.fromString((String)map.get("pipelineId")));

                int count = statement.executeUpdate();
                String key = String.format("hostId: %s, instanceId: %s pipelineId: %s",
                        event.get(Constants.HOST), map.get("instanceId"), map.get("pipelineId"));

                if (count == 0) {
                    throw new SQLException("failed to update the pipeline with id " + key);
                }
                conn.commit();
                result = Success.of(key);
                notificationService.insertNotification(event, true, null);
            }  catch (SQLException e) {
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
    public Result<String> deleteInstancePipeline(Map<String, Object> event) {
        final String sql = "DELETE FROM instance_pipeline_t WHERE host_id = ? AND instance_id = ? AND pipeline_id = ?";
        Result<String> result;
        Map<String, Object> map = (Map<String, Object>)event.get(PortalConstants.DATA);
        try (Connection conn = ds.getConnection()) {
            conn.setAutoCommit(false);
            try (PreparedStatement statement = conn.prepareStatement(sql)) {
                statement.setObject(1, UUID.fromString((String)event.get(Constants.HOST)));
                statement.setObject(2, UUID.fromString((String)map.get("instanceId")));
                statement.setObject(3, UUID.fromString((String)map.get("pipelineId")));

                int count = statement.executeUpdate();
                String key = String.format("hostId: %s, instanceId: %s pipelineId: %s",
                        event.get(Constants.HOST), map.get("instanceId"), map.get("pipelineId"));

                if (count == 0) {
                    throw new SQLException("failed to delete the pipeline with id " + key);
                }
                conn.commit();
                result = Success.of(key);
                notificationService.insertNotification(event, true, null);
            }   catch (SQLException e) {
                logger.error("SQLException:", e);
                conn.rollback();
                notificationService.insertNotification(event, false, e.getMessage());
                result = Failure.of(new Status(SQL_EXCEPTION, e.getMessage()));
            }  catch (Exception e) {
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
    public Result<String> getInstancePipeline(int offset, int limit, String hostId, String instanceId, String instanceName,
                                              String productId, String productVersion, String pipelineId, String platformName,
                                              String platformVersion, String pipelineName, String pipelineVersion) {
        Result<String> result = null;
        String s =
                """
                        SELECT ip.host_id, ip.instance_id, i.instance_name, pv.product_id,\s
                        pv.product_version, ip.pipeline_id, pf.platform_name, pf.platform_version,\s
                        p.pipeline_name, p.pipeline_version, ip.update_user, ip.update_ts\s
                        FROM instance_pipeline_t ip
                        INNER JOIN instance_t i ON ip.instance_id = i.instance_id
                        INNER JOIN product_version_t pv ON i.product_version_id = pv.product_version_id
                        INNER JOIN pipeline_t p ON p.pipeline_id = ip.pipeline_id
                        INNER JOIN platform_t pf ON p.platform_id = pf.platform_id
                        WHERE 1=1
                """;
        StringBuilder sqlBuilder = new StringBuilder();
        sqlBuilder.append(s).append("\n");

        List<Object> parameters = new ArrayList<>();

        StringBuilder whereClause = new StringBuilder();
        addCondition(whereClause, parameters, "host_id", hostId != null ? UUID.fromString(hostId) : null);
        addCondition(whereClause, parameters, "instance_id", instanceId != null ? UUID.fromString(instanceId) : null);
        addCondition(whereClause, parameters, "instance_name", instanceName);
        addCondition(whereClause, parameters, "product_id", productId);
        addCondition(whereClause, parameters, "product_version", productVersion);
        addCondition(whereClause, parameters, "pipeline_id", pipelineId != null ? UUID.fromString(pipelineId) : null);
        addCondition(whereClause, parameters, "platform_name", platformName);
        addCondition(whereClause, parameters, "platform_version", platformVersion);
        addCondition(whereClause, parameters, "pipeline_name", pipelineName);
        addCondition(whereClause, parameters, "pipeline_version", pipelineVersion);

        if (!whereClause.isEmpty()) {
            sqlBuilder.append("AND ").append(whereClause);
        }

        sqlBuilder.append(" ORDER BY instance_id, pipeline_id\n" +
                "LIMIT ? OFFSET ?");

        parameters.add(limit);
        parameters.add(offset);

        String sql = sqlBuilder.toString();
        int total = 0;
        List<Map<String, Object>> instancePipelines = new ArrayList<>();

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
                    map.put("instanceId", resultSet.getObject("instance_id", UUID.class));
                    map.put("instanceName", resultSet.getString("instance_name"));
                    map.put("productId", resultSet.getString("product_id"));
                    map.put("productVersion", resultSet.getString("product_version"));
                    map.put("pipelineId", resultSet.getString("pipeline_id"));
                    map.put("platformName", resultSet.getString("platform_name"));
                    map.put("platformVersion", resultSet.getString("platform_version"));
                    map.put("pipelineName", resultSet.getString("pipeline_name"));
                    map.put("pipelineVersion", resultSet.getString("pipeline_version"));
                    map.put("updateUser", resultSet.getString("update_user"));
                    map.put("updateTs", resultSet.getObject("update_ts") != null ? resultSet.getObject("update_ts", OffsetDateTime.class) : null);
                    instancePipelines.add(map);
                }
            }
            Map<String, Object> resultMap = new HashMap<>();
            resultMap.put("total", total);
            resultMap.put("instancePipelines", instancePipelines);
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
    public Result<String> createPlatform(Map<String, Object> event) {
        final String sql = "INSERT INTO platform_t(host_id, platform_id, platform_name, platform_version, " +
                "client_type, handler_class, client_url, credentials, proxy_url, proxy_port, console_url, " +
                "environment, zone, region, lob, update_user, update_ts) " +
                "VALUES (?, ?, ?, ?, ?,  ?, ?, ?, ?, ?,  ?, ?, ?, ?, ?,  ?, ?)";
        Result<String> result;
        Map<String, Object> map = (Map<String, Object>)event.get(PortalConstants.DATA);
        try (Connection conn = ds.getConnection()) {
            conn.setAutoCommit(false);
            try (PreparedStatement statement = conn.prepareStatement(sql)) {
                statement.setObject(1, UUID.fromString((String)event.get(Constants.HOST)));
                statement.setObject(2, UUID.fromString((String)map.get("platformId")));
                statement.setString(3, (String)map.get("platformName"));
                statement.setString(4, (String)map.get("platformVersion"));
                statement.setString(5, (String)map.get("clientType"));
                statement.setString(6, (String)map.get("handlerClass"));
                statement.setString(7, (String)map.get("clientUrl"));
                statement.setString(8, (String)map.get("credentials"));

                if (map.containsKey("proxyUrl")) {
                    statement.setString(9, (String) map.get("proxyUrl"));
                } else {
                    statement.setNull(9, Types.VARCHAR);
                }
                if (map.containsKey("proxyPort")) {
                    statement.setInt(10, (Integer)map.get("proxyPort"));
                } else {
                    statement.setNull(10, Types.INTEGER);
                }
                if (map.containsKey("consoleUrl")) {
                    statement.setString(11, (String) map.get("consoleUrl"));
                } else {
                    statement.setNull(11, Types.VARCHAR);
                }
                if (map.containsKey("environment")) {
                    statement.setString(12, (String) map.get("environment"));
                } else {
                    statement.setNull(12, Types.VARCHAR);
                }
                if(map.containsKey("zone")) {
                    statement.setString(13, (String) map.get("zone"));
                } else {
                    statement.setNull(13, Types.VARCHAR);
                }
                if(map.containsKey("region")) {
                    statement.setString(14, (String) map.get("region"));
                } else {
                    statement.setNull(14, Types.VARCHAR);
                }
                if(map.containsKey("lob")) {
                    statement.setString(15, (String) map.get("lob"));
                } else {
                    statement.setNull(15, Types.VARCHAR);
                }
                statement.setString(16, (String)event.get(Constants.USER));
                statement.setObject(17, OffsetDateTime.parse((String)event.get(CloudEventV1.TIME)));


                int count = statement.executeUpdate();
                if (count == 0) {
                    throw new SQLException("failed to insert the platform with id " + map.get("platformId"));
                }
                conn.commit();
                result =  Success.of((String)map.get("platformId"));
                notificationService.insertNotification(event, true, null);

            } catch (SQLException e) {
                logger.error("SQLException:", e);
                conn.rollback();
                notificationService.insertNotification(event, false, e.getMessage());
                result = Failure.of(new Status(SQL_EXCEPTION, e.getMessage()));
            }  catch (Exception e) {
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
    public Result<String> updatePlatform(Map<String, Object> event) {
        final String sql = "UPDATE platform_t SET platform_name = ?, platform_version = ?, " +
                "client_type = ?, handler_class = ?, client_url = ?, credentials = ?, proxy_url = ?, proxy_port = ?, " +
                "console_url = ?, environment = ?, zone = ?, region = ?, lob = ?, " +
                "update_user = ?, update_ts = ? " +
                "WHERE host_id = ? and platform_id = ?";
        Result<String> result;
        Map<String, Object> map = (Map<String, Object>)event.get(PortalConstants.DATA);
        try (Connection conn = ds.getConnection()) {
            conn.setAutoCommit(false);
            try (PreparedStatement statement = conn.prepareStatement(sql)) {
                statement.setString(1, (String)map.get("platformName"));
                statement.setString(2, (String)map.get("platformVersion"));
                statement.setString(3, (String)map.get("clientType"));
                statement.setString(4, (String)map.get("handlerClass"));
                statement.setString(5, (String)map.get("clientUrl"));
                statement.setString(6, (String)map.get("credentials"));
                if (map.containsKey("proxyUrl")) {
                    statement.setString(7, (String) map.get("proxyUrl"));
                } else {
                    statement.setNull(7, Types.VARCHAR);
                }
                if (map.containsKey("proxyPort")) {
                    statement.setInt(8, (Integer) map.get("proxyPort"));
                } else {
                    statement.setNull(8, Types.INTEGER);
                }
                if (map.containsKey("consoleUrl")) {
                    statement.setString(9, (String) map.get("consoleUrl"));
                } else {
                    statement.setNull(9, Types.VARCHAR);
                }
                if (map.containsKey("environment")) {
                    statement.setString(10, (String) map.get("environment"));
                } else {
                    statement.setNull(10, Types.VARCHAR);
                }
                if(map.containsKey("zone")) {
                    statement.setString(11, (String) map.get("zone"));
                } else {
                    statement.setNull(11, Types.VARCHAR);
                }
                if(map.containsKey("region")) {
                    statement.setString(12, (String) map.get("region"));
                } else {
                    statement.setNull(12, Types.VARCHAR);
                }
                if(map.containsKey("lob")) {
                    statement.setString(13, (String) map.get("lob"));
                } else {
                    statement.setNull(13, Types.VARCHAR);
                }
                statement.setString(14, (String)event.get(Constants.USER));
                statement.setObject(15, OffsetDateTime.parse((String)event.get(CloudEventV1.TIME)));
                statement.setObject(16, UUID.fromString((String)event.get(Constants.HOST)));
                statement.setObject(17, UUID.fromString((String)map.get("platformId")));

                int count = statement.executeUpdate();
                if (count == 0) {
                    throw new SQLException("failed to update the platform with id " + map.get("platformId"));
                }
                conn.commit();
                result = Success.of((String)map.get("platformId"));
                notificationService.insertNotification(event, true, null);

            }  catch (SQLException e) {
                logger.error("SQLException:", e);
                conn.rollback();
                notificationService.insertNotification(event, false, e.getMessage());
                result = Failure.of(new Status(SQL_EXCEPTION, e.getMessage()));
            }   catch (Exception e) {
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
    public Result<String> deletePlatform(Map<String, Object> event) {
        final String sql = "DELETE FROM platform_t WHERE host_id = ? AND platform_id = ?";
        Result<String> result;
        Map<String, Object> map = (Map<String, Object>)event.get(PortalConstants.DATA);

        try (Connection conn = ds.getConnection()) {
            conn.setAutoCommit(false);
            try (PreparedStatement statement = conn.prepareStatement(sql)) {
                statement.setObject(1, UUID.fromString((String)event.get(Constants.HOST)));
                statement.setObject(2, UUID.fromString((String)map.get("platformId")));

                int count = statement.executeUpdate();
                if (count == 0) {
                    throw new SQLException("failed to delete the platform with id " + map.get("platformId"));
                }
                conn.commit();
                result =  Success.of((String)map.get("platformId"));
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
    public Result<String> getPlatform(int offset, int limit, String hostId, String platformId, String platformName, String platformVersion,
                                      String clientType, String clientUrl, String credentials, String proxyUrl, Integer proxyPort,
                                      String handlerClass, String consoleUrl, String environment, String zone, String region, String lob) {
        Result<String> result = null;
        String s =
                """
                SELECT COUNT(*) OVER () AS total,
                host_id, platform_id, platform_name, platform_version, client_type, client_url,
                credentials, proxy_url, proxy_port, handler_class, console_url, environment, zone, region, lob, update_user, update_ts
                FROM platform_t
                WHERE 1=1
                """;

        StringBuilder sqlBuilder = new StringBuilder(s);
        List<Object> parameters = new ArrayList<>();

        StringBuilder whereClause = new StringBuilder();
        addCondition(whereClause, parameters, "host_id", hostId != null ? UUID.fromString(hostId) : null);
        addCondition(whereClause, parameters, "platform_id", platformId != null ? UUID.fromString(platformId) : null);
        addCondition(whereClause, parameters, "platform_name", platformName);
        addCondition(whereClause, parameters, "platform_version", platformVersion);
        addCondition(whereClause, parameters, "client_type", clientType);
        addCondition(whereClause, parameters, "handler_class", handlerClass);
        addCondition(whereClause, parameters, "client_url", clientUrl);
        addCondition(whereClause, parameters, "credentials", credentials);
        addCondition(whereClause, parameters, "proxy_url", proxyUrl);
        addCondition(whereClause, parameters, "proxy_port", proxyPort);
        addCondition(whereClause, parameters, "console_url", consoleUrl);
        addCondition(whereClause, parameters, "environment", environment);
        addCondition(whereClause, parameters, "zone", zone);
        addCondition(whereClause, parameters, "region", region);
        addCondition(whereClause, parameters, "lob", lob);

        if (!whereClause.isEmpty()) {
            sqlBuilder.append("AND ").append(whereClause);
        }

        sqlBuilder.append(" ORDER BY platform_id\n" +
                "LIMIT ? OFFSET ?");


        parameters.add(limit);
        parameters.add(offset);

        String sql = sqlBuilder.toString();
        int total = 0;
        List<Map<String, Object>> platforms = new ArrayList<>();

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
                    map.put("platformId", resultSet.getObject("platform_id", UUID.class));
                    map.put("platformName", resultSet.getString("platform_name"));
                    map.put("platformVersion", resultSet.getString("platform_version"));
                    map.put("clientType", resultSet.getString("client_type"));
                    map.put("clientUrl", resultSet.getString("client_url"));
                    map.put("credentials", resultSet.getString("credentials"));
                    map.put("proxyUrl", resultSet.getString("proxy_url"));
                    map.put("proxyPort", resultSet.getInt("proxy_port"));
                    map.put("handlerClass", resultSet.getString("handler_class"));
                    map.put("consoleUrl", resultSet.getString("console_url"));
                    map.put("environment", resultSet.getString("environment"));
                    map.put("zone", resultSet.getString("zone"));
                    map.put("region", resultSet.getString("region"));
                    map.put("lob", resultSet.getString("lob"));
                    map.put("updateUser", resultSet.getString("update_user"));
                    map.put("updateTs", resultSet.getObject("update_ts") != null ? resultSet.getObject("update_ts", OffsetDateTime.class) : null);

                    platforms.add(map);
                }
            }

            Map<String, Object> resultMap = new HashMap<>();
            resultMap.put("total", total);
            resultMap.put("platforms", platforms);
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
    public Result<String> getPlatformLabel(String hostId) {
        Result<String> result = null;
        String sql = "SELECT platform_id, platform_name FROM platform_t WHERE host_id = ?";
        List<Map<String, Object>> labels = new ArrayList<>();
        try (Connection connection = ds.getConnection();
             PreparedStatement preparedStatement = connection.prepareStatement(sql)) {
            preparedStatement.setObject(1, UUID.fromString(hostId));
            try (ResultSet resultSet = preparedStatement.executeQuery()) {
                while (resultSet.next()) {
                    Map<String, Object> map = new HashMap<>();
                    map.put("id", resultSet.getString("platform_id"));
                    map.put("label", resultSet.getString("platform_name"));
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
    public Result<String> createDeploymentInstance(Map<String, Object> event) {
        // deployStatus is not set here but use the default value.
        final String sql = "INSERT INTO deployment_instance_t(host_id, instance_id, deployment_instance_id, " +
                "service_id, ip_address, port_number, system_env, runtime_env, pipeline_id, " +
                "update_user, update_ts) " +
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
        Result<String> result;
        Map<String, Object> map = (Map<String, Object>) event.get(PortalConstants.DATA);
        try (Connection conn = ds.getConnection()) {
            conn.setAutoCommit(false);
            try (PreparedStatement statement = conn.prepareStatement(sql)) {
                statement.setObject(1, UUID.fromString((String) event.get(Constants.HOST)));
                statement.setObject(2, UUID.fromString((String) map.get("instanceId")));
                statement.setObject(3, UUID.fromString((String) map.get("deploymentInstanceId")));
                statement.setString(4, (String) map.get("serviceId"));
                String ipAddress = (String) map.get("ipAddress");
                if(ipAddress != null && !ipAddress.isEmpty()) {
                    statement.setString(5, ipAddress);
                } else {
                    statement.setNull(5, Types.VARCHAR); // Set to SQL NULL if null or empty
                }

                // Handle nullable integer for port_number
                Object portNumberObj = map.get("portNumber");
                if (portNumberObj != null) {
                    if (portNumberObj instanceof String && !((String) portNumberObj).isEmpty()) {
                        statement.setInt(6, Integer.parseInt((String) portNumberObj));
                    } else if (portNumberObj instanceof Number) {
                        statement.setInt(6, ((Number) portNumberObj).intValue());
                    } else {
                        statement.setNull(6, java.sql.Types.INTEGER); // Set to SQL NULL if not a valid number or empty string
                    }
                } else {
                    statement.setNull(6, java.sql.Types.INTEGER); // Set to SQL NULL if null
                }

                statement.setString(7, (String) map.get("systemEnv"));
                statement.setString(8, (String) map.get("runtimeEnv"));
                statement.setObject(9, UUID.fromString((String) map.get("pipelineId")));
                statement.setString(10, (String) event.get(Constants.USER));
                statement.setObject(11, OffsetDateTime.parse((String) event.get(CloudEventV1.TIME)));

                int count = statement.executeUpdate();
                if (count == 0) {
                    throw new SQLException("failed to insert the deployment instance with id " + map.get("deploymentInstanceId"));
                }
                conn.commit();
                result = Success.of((String) map.get("deploymentInstanceId"));
                // Assuming you have a similar notification mechanism for deployment instances
                // insertDeploymentInstanceNotification(event, true, null); // You'd need to create this method
            } catch (SQLException e) {
                logger.error("SQLException:", e);
                conn.rollback();
                // insertDeploymentInstanceNotification(event, false, e.getMessage());
                result = Failure.of(new Status(SQL_EXCEPTION, e.getMessage()));
            } catch (Exception e) {
                logger.error("Exception:", e);
                conn.rollback();
                // insertDeploymentInstanceNotification(event, false, e.getMessage());
                result = Failure.of(new Status(GENERIC_EXCEPTION, e.getMessage()));
            }
        } catch (SQLException e) {
            logger.error("SQLException on getting connection:", e);
            result = Failure.of(new Status(SQL_EXCEPTION, e.getMessage()));
        }
        return result;
    }

    @Override
    public Result<String> updateDeploymentInstance(Map<String, Object> event) {
        // instanceId is FK and deployStatus is not updated here.
        final String sql = "UPDATE deployment_instance_t SET " +
                "service_id = ?, " +
                "ip_address = ?, " +
                "port_number = ?, " +
                "system_env = ?, " +
                "runtime_env = ?, " +
                "pipeline_id = ?, " +
                "update_user = ?, " +
                "update_ts = ? " +
                "WHERE host_id = ? AND deployment_instance_id = ?";

        Result<String> result;
        Map<String, Object> map = (Map<String, Object>) event.get(PortalConstants.DATA);
        String deploymentInstanceId = (String) map.get("deploymentInstanceId"); // For messages

        try (Connection conn = ds.getConnection()) {
            conn.setAutoCommit(false);
            try (PreparedStatement statement = conn.prepareStatement(sql)) {
                // Set parameters in the order they appear in the SQL SET clause, then WHERE clause
                int paramIdx = 1;

                // service_id
                statement.setString(paramIdx++, (String) map.get("serviceId"));

                // ip_address (handles null if map.get("ipAddress") returns null)
                statement.setString(paramIdx++, (String) map.get("ipAddress"));

                // port_number
                Object portNumberObj = map.get("portNumber");
                if (portNumberObj != null) {
                    // Attempt to parse if it's a string, otherwise assume it's a number or handle appropriately
                    if (portNumberObj instanceof String && !((String) portNumberObj).isEmpty()) {
                        statement.setInt(paramIdx++, Integer.parseInt((String) portNumberObj));
                    } else if (portNumberObj instanceof Number) {
                        statement.setInt(paramIdx++, ((Number) portNumberObj).intValue());
                    } else { // If it's an empty string or unexpected type intended to be null for port_number
                        statement.setNull(paramIdx++, java.sql.Types.INTEGER);
                    }
                } else {
                    statement.setNull(paramIdx++, java.sql.Types.INTEGER);
                }

                // system_env
                statement.setString(paramIdx++, (String) map.get("systemEnv"));
                // runtime_env
                statement.setString(paramIdx++, (String) map.get("runtimeEnv"));
                // pipeline_id
                statement.setObject(paramIdx++, UUID.fromString((String) map.get("pipelineId")));

                // update_user
                statement.setString(paramIdx++, (String) event.get(Constants.USER));
                // update_ts
                statement.setObject(paramIdx++, OffsetDateTime.parse((String) event.get(CloudEventV1.TIME)));

                // WHERE clause parameters
                statement.setObject(paramIdx++, UUID.fromString((String) event.get(Constants.HOST)));
                statement.setObject(paramIdx++, UUID.fromString(deploymentInstanceId));

                int count = statement.executeUpdate();
                if (count == 0) {
                    // Rollback before creating Failure, consistent with template's SQLException handling
                    conn.rollback();
                    String errorMessage = "failed to update the deployment instance with id " + deploymentInstanceId + " (not found or no changes)";
                    logger.warn(errorMessage);
                    // insertDeploymentInstanceNotification(event, false, "Deployment instance not found or no effective change.");
                    // The template throws SQLException, which then gets caught. Let's align more closely.
                    // However, "not found" is distinct. For closer alignment to template throwing SQLException for 0 count:
                    throw new SQLException("failed to update the deployment instance with id " + deploymentInstanceId + " (record not found or no changes made)");
                }
                conn.commit();
                result = Success.of(deploymentInstanceId);
                // insertDeploymentInstanceNotification(event, true, null);
            } catch (SQLException e) {
                logger.error("SQLException during updateDeploymentInstance:", e);
                conn.rollback();
                // insertDeploymentInstanceNotification(event, false, e.getMessage());
                result = Failure.of(new Status(SQL_EXCEPTION, e.getMessage()));
            } catch (Exception e) { // Generic catch for other runtime errors like NullPointerException if map keys are missing
                logger.error("Exception during updateDeploymentInstance:", e);
                conn.rollback();
                // insertDeploymentInstanceNotification(event, false, e.getMessage());
                result = Failure.of(new Status(GENERIC_EXCEPTION, e.getMessage()));
            }
        } catch (SQLException e) { // For conn = ds.getConnection() failure
            logger.error("SQLException obtaining connection for updateDeploymentInstance:", e);
            result = Failure.of(new Status(SQL_EXCEPTION, e.getMessage()));
        }
        return result;
    }

    @Override
    public Result<String> deleteDeploymentInstance(Map<String, Object> event) {
        final String sql = "DELETE FROM deployment_instance_t WHERE host_id = ? AND deployment_instance_id = ?";
        Result<String> result;
        Map<String, Object> map = (Map<String, Object>) event.get(PortalConstants.DATA);
        // String deploymentInstanceId = (String) map.get("deploymentInstanceId"); // Not strictly needed if we follow template's success return

        try (Connection conn = ds.getConnection()) {
            conn.setAutoCommit(false);
            try (PreparedStatement statement = conn.prepareStatement(sql)) {
                statement.setObject(1, UUID.fromString((String) event.get(Constants.HOST)));
                statement.setObject(2, UUID.fromString((String) map.get("deploymentInstanceId")));

                int count = statement.executeUpdate();
                if (count == 0) {
                    // Following template: throw SQLException if no rows affected
                    throw new SQLException("failed to delete the deployment instance with id " + map.get("deploymentInstanceId") + " and host " + event.get(Constants.HOST));
                }
                conn.commit();
                // Template returns the ID used in the WHERE clause for update, let's return deploymentInstanceId for delete
                result = Success.of((String) map.get("deploymentInstanceId"));
                // Assuming a similar notification mechanism for deployment instances.
                // You would need to create/adapt this method: insertDeploymentInstanceNotification(event, true, null);
                // For now, using the template's notification method name as a placeholder for structure:
                // notificationService.insertNotification(event, true, null); // If you have a generic one or adapt this.
            } catch (SQLException e) {
                logger.error("SQLException during deleteDeploymentInstance:", e);
                conn.rollback();
                // insertDeploymentInstanceNotification(event, false, e.getMessage());
                result = Failure.of(new Status(SQL_EXCEPTION, e.getMessage()));
            } catch (Exception e) { // Catch other potential runtime errors like NullPointerException or IllegalArgumentException
                logger.error("Exception during deleteDeploymentInstance:", e);
                conn.rollback();
                // insertDeploymentInstanceNotification(event, false, e.getMessage());
                result = Failure.of(new Status(GENERIC_EXCEPTION, e.getMessage()));
            }
        } catch (SQLException e) { // For conn = ds.getConnection() failure
            logger.error("SQLException obtaining connection for deleteDeploymentInstance:", e);
            result = Failure.of(new Status(SQL_EXCEPTION, e.getMessage()));
        }
        return result;
    }

    @Override
    public Result<String> getDeploymentInstance(int offset, int limit, String hostId, String instanceId, String instanceName, String deploymentInstanceId,
                                                String serviceId, String ipAddress, Integer portNumber, String systemEnv, String runtimeEnv,
                                                String pipelineId, String pipelineName, String pipelineVersion, String deployStatus) {
        Result<String> result = null;
        String s =
                """
                SELECT COUNT(*) OVER () AS total,
                di.host_id, di.instance_id, i.instance_name, di.deployment_instance_id, di.service_id, di.ip_address,
                di.port_number, di.system_env, di.runtime_env, di.pipeline_id, p.pipeline_name, p.pipeline_version,
                di.deploy_status, di.update_user, di.update_ts
                FROM deployment_instance_t di
                INNER JOIN instance_t i ON i.instance_id = di.instance_id
                INNER JOIN pipeline_t p ON p.pipeline_id = di.pipeline_id
                WHERE 1=1
                """;

        StringBuilder sqlBuilder = new StringBuilder(s);
        List<Object> parameters = new ArrayList<>();
        StringBuilder whereClause = new StringBuilder();

        // Add conditions based on input parameters
        addCondition(whereClause, parameters, "di.host_id", hostId != null ? UUID.fromString(hostId) : null);
        addCondition(whereClause, parameters, "di.instance_id", instanceId != null ? UUID.fromString(instanceId) : null);
        addCondition(whereClause, parameters, "i.instance_name", instanceName);
        addCondition(whereClause, parameters, "di.deployment_instance_id", deploymentInstanceId != null ? UUID.fromString(deploymentInstanceId) : null);
        addCondition(whereClause, parameters, "di.service_id", serviceId);
        addCondition(whereClause, parameters, "di.ip_address", ipAddress);
        addCondition(whereClause, parameters, "di.port_number", portNumber); // Integer, addCondition should handle it or be adapted
        addCondition(whereClause, parameters, "di.system_env", systemEnv);
        addCondition(whereClause, parameters, "di.runtime_env", runtimeEnv);
        addCondition(whereClause, parameters, "di.pipeline_id", pipelineId != null ? UUID.fromString(pipelineId) : null);
        addCondition(whereClause, parameters, "p.pipeline_name", pipelineName);
        addCondition(whereClause, parameters, "p.pipeline_version", pipelineVersion);
        addCondition(whereClause, parameters, "di.deploy_status", deployStatus);


        if (!whereClause.isEmpty()) {
            sqlBuilder.append("AND ").append(whereClause);
        }

        // It's good practice to order by a consistent key, e.g., the primary key or a creation timestamp.
        // Using deployment_instance_id for ordering.
        sqlBuilder.append(" ORDER BY di.host_id, di.deployment_instance_id\n") // Or just deployment_instance_id if host_id is always filtered
                .append("LIMIT ? OFFSET ?");

        parameters.add(limit);
        parameters.add(offset);

        String sql = sqlBuilder.toString();
        int total = 0;
        List<Map<String, Object>> deploymentInstances = new ArrayList<>(); // Changed variable name

        try (Connection connection = ds.getConnection();
             PreparedStatement preparedStatement = connection.prepareStatement(sql)) {

            for (int i = 0; i < parameters.size(); i++) {
                // Handle null Integer specifically for port_number if addCondition doesn't
                if (parameters.get(i) == null && (i < parameters.size() - 2) /* not limit or offset */ ) {
                    // This check assumes portNumber is the only nullable Integer filter.
                    // A more robust way would be to pass the SQL type to addCondition or have type-specific addCondition.
                    // For now, assuming addCondition correctly handles nulls or setObject works.
                    // If 'port_number' was the parameter and it's null:
                    // String columnNameForNullCheck = ... ; // Need a way to know which column this null is for.
                    // if ("port_number".equals(columnNameForNullCheck)) {
                    //    preparedStatement.setNull(i + 1, java.sql.Types.INTEGER);
                    // } else {
                    //    preparedStatement.setObject(i + 1, null); // Or specific type if known
                    // }
                    // The template's addCondition likely handles setObject(..., null) which works for most types.
                    // For Integers specifically, setObject(i + 1, null) might not map to SQL NULL correctly for all drivers,
                    // setNull(i+1, Types.INTEGER) is safer.
                    // Given the template uses setObject for all, we'll follow that.
                    preparedStatement.setObject(i + 1, null);
                } else {
                    preparedStatement.setObject(i + 1, parameters.get(i));
                }
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
                    map.put("instanceId", resultSet.getObject("instance_id", UUID.class));
                    map.put("instanceName", resultSet.getString("instance_name"));
                    map.put("deploymentInstanceId", resultSet.getObject("deployment_instance_id", UUID.class));
                    map.put("serviceId", resultSet.getString("service_id"));
                    map.put("ipAddress", resultSet.getString("ip_address"));

                    // Handle nullable Integer for port_number from ResultSet
                    int portNum = resultSet.getInt("port_number");
                    if (resultSet.wasNull()) {
                        map.put("portNumber", null);
                    } else {
                        map.put("portNumber", portNum);
                    }

                    map.put("systemEnv", resultSet.getString("system_env"));
                    map.put("runtimeEnv", resultSet.getString("runtime_env"));
                    map.put("pipelineId", resultSet.getObject("pipeline_id", UUID.class));
                    map.put("pipelineName", resultSet.getString("pipeline_name"));
                    map.put("pipelineVersion", resultSet.getString("pipeline_version"));
                    map.put("deployStatus", resultSet.getString("deploy_status"));
                    map.put("updateUser", resultSet.getString("update_user"));
                    map.put("updateTs", resultSet.getObject("update_ts") != null ? resultSet.getObject("update_ts", OffsetDateTime.class) : null);

                    deploymentInstances.add(map); // Changed variable name
                }
            }

            Map<String, Object> resultMap = new HashMap<>();
            resultMap.put("total", total);
            resultMap.put("deploymentInstances", deploymentInstances);
            result = Success.of(JsonMapper.toJson(resultMap));
        } catch (SQLException e) {
            logger.error("SQLException:", e);
            result = Failure.of(new Status(SQL_EXCEPTION, e.getMessage()));
        } catch (Exception e) { // Catching potential UUID parsing errors or other runtime issues
            logger.error("Exception:", e);
            result = Failure.of(new Status(GENERIC_EXCEPTION, e.getMessage()));
        }
        return result;
    }

    @Override
    public Result<String> getDeploymentInstancePipeline(String hostId, String instanceId, String systemEnv, String runtimeEnv) {
        final String s =
                """
                WITH InstanceProductVersion AS (
                    -- Get the product_version_id for the given instance_id
                    SELECT product_version_id
                    FROM instance_t
                    WHERE host_id = ? AND instance_id = ? -- Parameter 2: instance_id (e.g., '0196e658-8a14-72a8-802f-1fea0de8843a')
                ),
                ProductPipelines AS (
                    -- Get all pipeline_ids associated with that product_version_id
                    SELECT pvp.pipeline_id
                    FROM product_version_pipeline_t pvp
                    JOIN InstanceProductVersion ipv ON pvp.product_version_id = ipv.product_version_id
                )
                -- Query 1: Exact match for system_env AND runtime_env
                SELECT
                    p.*,
                    1 AS preference -- Higher preference for exact runtime_env match
                FROM pipeline_t p
                JOIN ProductPipelines pp ON p.pipeline_id = pp.pipeline_id
                WHERE p.system_env = ?     -- Parameter 3: system_env (e.g., 'VM Ubuntu 24.04' or 'Kubernetes')
                  AND p.runtime_env = ?    -- Parameter 4: runtime_env (e.g., 'OpenJDK 21')
                  AND p.current = true

                UNION ALL

                -- Query 2: Match for system_env AND runtime_env IS NULL
                SELECT
                    p.*,
                    2 AS preference -- Lower preference for NULL runtime_env
                FROM pipeline_t p
                JOIN ProductPipelines pp ON p.pipeline_id = pp.pipeline_id
                WHERE p.system_env = ?     -- Parameter 5: system_env (same as Parameter 2)
                  AND p.runtime_env IS NULL
                  AND p.current = true

                ORDER BY preference ASC, pipeline_id -- Ensure deterministic order if multiple pipelines have same preference
                LIMIT 1
                """;
        Result<String> result = null;
        String pipelineId = null;
        try (Connection connection = ds.getConnection();
             PreparedStatement preparedStatement = connection.prepareStatement(s)) {
            preparedStatement.setObject(1, UUID.fromString(hostId));
            preparedStatement.setObject(2, UUID.fromString(instanceId));
            preparedStatement.setString(3, systemEnv);
            preparedStatement.setString(4, runtimeEnv);
            preparedStatement.setString(5, systemEnv); // Reusing system_env for the second query
            try (ResultSet resultSet = preparedStatement.executeQuery()) {
                if (resultSet.next()) {
                    pipelineId = resultSet.getString("pipeline_id");
                }
            }
            if (pipelineId == null) {
                throw new SQLException("No pipeline found for the given parameters.");
            }
            result = Success.of(pipelineId);
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
    public Result<String> getDeploymentInstanceLabel(String hostId, String instanceId) {
        Result<String> result = null;
        String sql = instanceId == null ? "SELECT deployment_instance_id, service_id FROM deployment_instance_t WHERE host_id = ?" :
                "SELECT deployment_instance_id, service_id FROM deployment_instance_t WHERE host_id = ? AND instance_id = ?";
        List<Map<String, Object>> labels = new ArrayList<>();
        try (Connection connection = ds.getConnection();
             PreparedStatement preparedStatement = connection.prepareStatement(sql)) {
            preparedStatement.setObject(1, UUID.fromString(hostId));
            if (instanceId != null) {
                preparedStatement.setObject(2, UUID.fromString(instanceId));
            }
            try (ResultSet resultSet = preparedStatement.executeQuery()) {
                while (resultSet.next()) {
                    Map<String, Object> map = new HashMap<>();
                    map.put("id", resultSet.getString("deployment_instance_id"));
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

    @Override
    public Result<String> createDeployment(Map<String, Object> event) {
        final String sql = "INSERT INTO deployment_t(host_id, deployment_id, deployment_instance_id, " +
                "deployment_status, deployment_type, schedule_ts, update_user, update_ts) " +
                "VALUES (?, ?, ?, ?, ?,  ?, ?, ?)";
        Result<String> result;
        Map<String, Object> map = (Map<String, Object>)event.get(PortalConstants.DATA);
        try (Connection conn = ds.getConnection()) {
            conn.setAutoCommit(false);
            try (PreparedStatement statement = conn.prepareStatement(sql)) {
                statement.setObject(1, UUID.fromString((String)event.get(Constants.HOST)));
                statement.setObject(2, UUID.fromString((String)map.get("deploymentId")));
                statement.setObject(3, UUID.fromString((String)map.get("deploymentInstanceId")));
                statement.setString(4, (String)map.get("deploymentStatus"));
                statement.setString(5, (String)map.get("deploymentType"));
                statement.setObject(6, map.get("scheduleTs") != null ? OffsetDateTime.parse((String)map.get("scheduleTs")) : OffsetDateTime.parse((String)event.get(CloudEventV1.TIME)));
                statement.setString(7, (String)event.get(Constants.USER));
                statement.setObject(8, OffsetDateTime.parse((String)event.get(CloudEventV1.TIME)));

                int count = statement.executeUpdate();
                if (count == 0) {
                    throw new SQLException("failed to insert the deployment with id " + map.get("deploymentId"));
                }
                conn.commit();
                result = Success.of((String)map.get("deploymentId"));
                notificationService.insertNotification(event, true, null);
            }  catch (SQLException e) {
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
        }  catch (SQLException e) {
            logger.error("SQLException:", e);
            result = Failure.of(new Status(SQL_EXCEPTION, e.getMessage()));
        }
        return result;
    }

    @Override
    public Result<String> updateDeployment(Map<String, Object> event) {
        final String sql = "UPDATE deployment_t SET deployment_status = ?, deployment_type = ?, " +
                "schedule_ts = ?, update_user = ?, update_ts = ? " +
                "WHERE host_id = ? and deployment_id = ?";
        Result<String> result;
        Map<String, Object> map = (Map<String, Object>)event.get(PortalConstants.DATA);
        OffsetDateTime offsetDateTime = OffsetDateTime.parse((String)event.get(CloudEventV1.TIME));

        try (Connection conn = ds.getConnection()) {
            conn.setAutoCommit(false);
            try (PreparedStatement statement = conn.prepareStatement(sql)) {
                statement.setString(1, (String)map.get("deploymentStatus"));
                statement.setString(2, (String)map.get("deploymentType"));
                // use the event time if schedule time is not provided. We cannot use now as this event might be replayed.
                statement.setObject(3, map.get("scheduleTs") != null ? OffsetDateTime.parse((String)map.get("scheduleTs")) : OffsetDateTime.parse((String)event.get(CloudEventV1.TIME)));
                statement.setString(4, (String)event.get(Constants.USER));
                statement.setObject(5, OffsetDateTime.parse((String)event.get(CloudEventV1.TIME)));
                statement.setObject(6, UUID.fromString((String)event.get(Constants.HOST)));
                statement.setObject(7, UUID.fromString((String)map.get("deploymentId")));
                int count = statement.executeUpdate();
                if (count == 0) {
                    throw new SQLException("failed to update the deployment with id " + map.get("deploymentId"));
                }
                conn.commit();
                result = Success.of((String)map.get("deploymentId"));
                notificationService.insertNotification(event, true, null);
            }  catch (SQLException e) {
                logger.error("SQLException:", e);
                conn.rollback();
                notificationService.insertNotification(event, false, e.getMessage());
                result = Failure.of(new Status(SQL_EXCEPTION, e.getMessage()));
            }  catch (Exception e) {
                logger.error("Exception:", e);
                conn.rollback();
                notificationService.insertNotification(event, false, e.getMessage());
                result = Failure.of(new Status(GENERIC_EXCEPTION, e.getMessage()));
            }
        }   catch (SQLException e) {
            logger.error("SQLException:", e);
            result = Failure.of(new Status(SQL_EXCEPTION, e.getMessage()));
        }
        return result;
    }

    @Override
    public Result<String> updateDeploymentJobId(Map<String, Object> event) {
        final String sql = "UPDATE deployment_t SET platform_job_id = ?, update_user = ?, update_ts = ? " +
                "WHERE host_id = ? and deployment_id = ?";
        Result<String> result;
        Map<String, Object> map = (Map<String, Object>)event.get(PortalConstants.DATA);
        try (Connection conn = ds.getConnection()) {
            conn.setAutoCommit(false);
            try (PreparedStatement statement = conn.prepareStatement(sql)) {
                statement.setString(1, (String)map.get("platformJobId"));
                statement.setString(2, (String)event.get(Constants.USER));
                statement.setObject(3, OffsetDateTime.parse((String)event.get(CloudEventV1.TIME)));
                statement.setObject(4, UUID.fromString((String)event.get(Constants.HOST)));
                statement.setObject(5, UUID.fromString((String)map.get("deploymentId")));
                int count = statement.executeUpdate();
                if (count == 0) {
                    throw new SQLException("failed to update the platform job id with deploymentId " + map.get("deploymentId"));
                }
                conn.commit();
                result = Success.of((String)map.get("deploymentId"));
                notificationService.insertNotification(event, true, null);
            }  catch (SQLException e) {
                logger.error("SQLException:", e);
                conn.rollback();
                notificationService.insertNotification(event, false, e.getMessage());
                result = Failure.of(new Status(SQL_EXCEPTION, e.getMessage()));
            }  catch (Exception e) {
                logger.error("Exception:", e);
                conn.rollback();
                notificationService.insertNotification(event, false, e.getMessage());
                result = Failure.of(new Status(GENERIC_EXCEPTION, e.getMessage()));
            }
        }   catch (SQLException e) {
            logger.error("SQLException:", e);
            result = Failure.of(new Status(SQL_EXCEPTION, e.getMessage()));
        }
        return result;

    }

    @Override
    public Result<String> updateDeploymentStatus(Map<String, Object> event) {
        final String sql = "UPDATE deployment_t SET deployment_status = ?, update_user = ?, update_ts = ? " +
                "WHERE host_id = ? and deployment_id = ?";
        Result<String> result;
        Map<String, Object> map = (Map<String, Object>)event.get(PortalConstants.DATA);

        try (Connection conn = ds.getConnection()) {
            conn.setAutoCommit(false);
            try (PreparedStatement statement = conn.prepareStatement(sql)) {
                statement.setString(1, (String)map.get("deploymentStatus"));
                statement.setString(2, (String)event.get(Constants.USER));
                statement.setObject(3, OffsetDateTime.parse((String)event.get(CloudEventV1.TIME)));
                statement.setObject(4, UUID.fromString((String)event.get(Constants.HOST)));
                statement.setObject(5, UUID.fromString((String)map.get("deploymentId")));
                int count = statement.executeUpdate();
                if (count == 0) {
                    throw new SQLException("failed to update the deployment status with deploymentId " + map.get("deploymentId"));
                }
                conn.commit();
                result = Success.of((String)map.get("deploymentId"));
                notificationService.insertNotification(event, true, null);
            }  catch (SQLException e) {
                logger.error("SQLException:", e);
                conn.rollback();
                notificationService.insertNotification(event, false, e.getMessage());
                result = Failure.of(new Status(SQL_EXCEPTION, e.getMessage()));
            }  catch (Exception e) {
                logger.error("Exception:", e);
                conn.rollback();
                notificationService.insertNotification(event, false, e.getMessage());
                result = Failure.of(new Status(GENERIC_EXCEPTION, e.getMessage()));
            }
        }   catch (SQLException e) {
            logger.error("SQLException:", e);
            result = Failure.of(new Status(SQL_EXCEPTION, e.getMessage()));
        }
        return result;

    }

    @Override
    public Result<String> deleteDeployment(Map<String, Object> event) {
        final String sql = "DELETE FROM deployment_t WHERE host_id = ? AND deployment_id = ?";
        Result<String> result;
        Map<String, Object> map = (Map<String, Object>)event.get(PortalConstants.DATA);
        try (Connection conn = ds.getConnection()) {
            conn.setAutoCommit(false);
            try (PreparedStatement statement = conn.prepareStatement(sql)) {
                statement.setObject(1, UUID.fromString((String)event.get(Constants.HOST)));
                statement.setObject(2, UUID.fromString((String)map.get("deploymentId")));

                int count = statement.executeUpdate();
                if (count == 0) {
                    throw new SQLException("failed to delete the deployment with id " + map.get("deploymentId"));
                }
                conn.commit();
                result = Success.of((String)map.get("deploymentId"));
                notificationService.insertNotification(event, true, null);

            }   catch (SQLException e) {
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
        }  catch (SQLException e) {
            logger.error("SQLException:", e);
            result = Failure.of(new Status(SQL_EXCEPTION, e.getMessage()));
        }
        return result;
    }

    @Override
    public Result<String> getDeployment(int offset, int limit, String hostId, String deploymentId,
                                        String deploymentInstanceId, String serviceId, String deploymentStatus,
                                        String deploymentType, String platformJobId) {
        Result<String> result = null;
        String s =
                """
                SELECT COUNT(*) OVER () AS total,
                d.host_id, d.deployment_id, d.deployment_instance_id, di.service_id, d.deployment_status,
                d.deployment_type, d.schedule_ts, d.platform_job_id, d.update_user, d.update_ts
                FROM deployment_t d
                INNER JOIN deployment_instance_t di ON di.deployment_instance_id = d.deployment_instance_id
                WHERE 1=1
                """;


        StringBuilder sqlBuilder = new StringBuilder(s);
        List<Object> parameters = new ArrayList<>();
        StringBuilder whereClause = new StringBuilder();

        addCondition(whereClause, parameters, "d.host_id", hostId != null ? UUID.fromString(hostId) : null);
        addCondition(whereClause, parameters, "d.deployment_id", deploymentId != null ? UUID.fromString(deploymentId) : null);
        addCondition(whereClause, parameters, "d.deployment_instance_id", deploymentInstanceId != null ? UUID.fromString(deploymentInstanceId) : null);
        addCondition(whereClause, parameters, "di.service_id", serviceId);
        addCondition(whereClause, parameters, "d.deployment_status", deploymentStatus);
        addCondition(whereClause, parameters, "d.deployment_type", deploymentType);
        addCondition(whereClause, parameters, "d.platform_job_id", platformJobId);

        if (!whereClause.isEmpty()) {
            sqlBuilder.append("AND ").append(whereClause);
        }

        sqlBuilder.append(" ORDER BY d.deployment_id\n" +
                "LIMIT ? OFFSET ?");

        parameters.add(limit);
        parameters.add(offset);

        String sql = sqlBuilder.toString();
        int total = 0;
        List<Map<String, Object>> deployments = new ArrayList<>();

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
                    map.put("deploymentId", resultSet.getObject("deployment_id", UUID.class));
                    map.put("deploymentInstanceId", resultSet.getObject("deployment_instance_id", UUID.class));
                    map.put("serviceId", resultSet.getString("service_id"));
                    map.put("deploymentStatus", resultSet.getString("deployment_status"));
                    map.put("deploymentType", resultSet.getString("deployment_type"));
                    map.put("scheduleTs", resultSet.getObject("schedule_ts") != null ? resultSet.getObject("schedule_ts", OffsetDateTime.class) : null);
                    map.put("platformJobId", resultSet.getString("platform_job_id"));
                    map.put("updateUser", resultSet.getString("update_user"));
                    map.put("updateTs", resultSet.getObject("update_ts") != null ? resultSet.getObject("update_ts", OffsetDateTime.class) : null);

                    deployments.add(map);
                }
            }

            Map<String, Object> resultMap = new HashMap<>();
            resultMap.put("total", total);
            resultMap.put("deployments", deployments);
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

}
