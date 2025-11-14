package net.lightapi.portal.db.persistence;

import com.networknt.monad.Result;
import java.sql.Connection; // Added import
import java.sql.SQLException; // Added import
import java.util.Map;
import java.util.Set;

public interface ConfigPersistence {
    // Config
    void createConfig(Connection conn, Map<String, Object> event) throws SQLException, Exception;
    void updateConfig(Connection conn, Map<String, Object> event) throws SQLException, Exception;
    void deleteConfig(Connection conn, Map<String, Object> event) throws SQLException, Exception;
    Result<String> getConfig(int offset, int limit, String filters, String globalFilter, String sorting);
    Result<String> queryConfigById(String configId);
    String queryConfigId(String configName);
    Result<String> getConfigIdLabel();
    Result<String> getConfigIdApiAppLabel(String resourceType);

    // ConfigProperty
    void createConfigProperty(Connection conn, Map<String, Object> event) throws SQLException, Exception;
    void updateConfigProperty(Connection conn, Map<String, Object> event) throws SQLException, Exception;
    void deleteConfigProperty(Connection conn, Map<String, Object> event) throws SQLException, Exception;
    Result<String> getConfigProperty(int offset, int limit, String filters, String globalFilter, String sorting);
    Result<String> queryConfigPropertyById(String configId); // Gets all properties for a configId
    Result<String> queryConfigPropertyByPropertyId(String configId, String propertyId); // Gets a specific property
    Result<String> getPropertyIdLabel(String configId);
    Result<String> getPropertyIdApiAppLabel(String configId, String resourceType);
    String queryPropertyId(String configName, String propertyName);
    Result<String> getPropertyById(String propertyId);

    // EnvironmentProperty
    void createConfigEnvironment(Connection conn, Map<String, Object> event) throws SQLException, Exception;
    void updateConfigEnvironment(Connection conn, Map<String, Object> event) throws SQLException, Exception;
    void deleteConfigEnvironment(Connection conn, Map<String, Object> event) throws SQLException, Exception;
    Result<String> getConfigEnvironment(int offset, int limit, String filters, String globalFilter, String sorting, String hostId);
    Result<String> getConfigEnvironmentById(String hostId, String environmentId, String propertyId);

    // InstanceProperty (was ConfigInstance)
    void createConfigInstance(Connection conn, Map<String, Object> event) throws SQLException, Exception;
    void updateConfigInstance(Connection conn, Map<String, Object> event) throws SQLException, Exception;
    void deleteConfigInstance(Connection conn, Map<String, Object> event) throws SQLException, Exception;
    Result<String> getConfigInstance(int offset, int limit, String filters, String globalFilter, String sorting, String hostId);
    Result<String> getConfigInstanceById(String hostId, String instanceId, String propertyId);

    // InstanceApiProperty (was ConfigInstanceApi)
    void createConfigInstanceApi(Connection conn, Map<String, Object> event) throws SQLException, Exception;
    void updateConfigInstanceApi(Connection conn, Map<String, Object> event) throws SQLException, Exception;
    void deleteConfigInstanceApi(Connection conn, Map<String, Object> event) throws SQLException, Exception;
    Result<String> getConfigInstanceApi(int offset, int limit, String filters, String globalFilter, String sorting, String hostId);
    Result<String> getConfigInstanceApiById(String hostId, String instanceApiId, String propertyId);

    // InstanceAppProperty (was ConfigInstanceApp)
    void createConfigInstanceApp(Connection conn, Map<String, Object> event) throws SQLException, Exception;
    void updateConfigInstanceApp(Connection conn, Map<String, Object> event) throws SQLException, Exception;
    void deleteConfigInstanceApp(Connection conn, Map<String, Object> event) throws SQLException, Exception;
    Result<String> getConfigInstanceApp(int offset, int limit, String filters, String globalFilter, String sorting, String hostId);
    Result<String> getConfigInstanceAppById(String hostId, String instanceAppId, String propertyId);

    // InstanceAppApiProperty (was ConfigInstanceAppApi)
    void createConfigInstanceAppApi(Connection conn, Map<String, Object> event) throws SQLException, Exception;
    void updateConfigInstanceAppApi(Connection conn, Map<String, Object> event) throws SQLException, Exception;
    void deleteConfigInstanceAppApi(Connection conn, Map<String, Object> event) throws SQLException, Exception;
    Result<String> getConfigInstanceAppApi(int offset, int limit, String filters, String globalFilter, String sorting, String hostId);
    Result<String> getConfigInstanceAppApiById(String hostId, String instanceAppId, String instanceApiId, String propertyId);

    // DeploymentInstanceProperty (was ConfigDeploymentInstance)
    void createConfigDeploymentInstance(Connection conn, Map<String, Object> event) throws SQLException, Exception;
    void updateConfigDeploymentInstance(Connection conn, Map<String, Object> event) throws SQLException, Exception;
    void deleteConfigDeploymentInstance(Connection conn, Map<String, Object> event) throws SQLException, Exception;
    Result<String> getConfigDeploymentInstance(int offset, int limit, String filters, String globalFilter, String sorting, String hostId);
    Result<String> getConfigDeploymentInstanceById(String hostId, String deploymentInstanceId, String propertyId);

    // ProductProperty (was ConfigProduct)
    void createConfigProduct(Connection conn, Map<String, Object> event) throws SQLException, Exception;
    void updateConfigProduct(Connection conn, Map<String, Object> event) throws SQLException, Exception;
    void deleteConfigProduct(Connection conn, Map<String, Object> event) throws SQLException, Exception;
    Result<String> getConfigProduct(int offset, int limit, String filters, String globalFilter, String sorting);
    Result<String> getConfigProductById(String productId, String propertyId);

    // ProductVersionProperty (was ConfigProductVersion)
    void createConfigProductVersion(Connection conn, Map<String, Object> event) throws SQLException, Exception;
    void updateConfigProductVersion(Connection conn, Map<String, Object> event) throws SQLException, Exception;
    void deleteConfigProductVersion(Connection conn, Map<String, Object> event) throws SQLException, Exception;
    Result<String> getConfigProductVersion(int offset, int limit, String filters, String globalFilter, String sorting, String hostId);
    Result<String> getConfigProductVersionById(String hostId, String productVersionId, String propertyId);

    // InstanceFile (was ConfigInstanceFile)
    void createConfigInstanceFile(Connection conn, Map<String, Object> event) throws SQLException, Exception;
    void updateConfigInstanceFile(Connection conn, Map<String, Object> event) throws SQLException, Exception;
    void deleteConfigInstanceFile(Connection conn, Map<String, Object> event) throws SQLException, Exception;
    Result<String> getConfigInstanceFile(int offset, int limit, String filters, String globalFilter, String sorting, String hostId);
    Result<String> getConfigInstanceFileById(String hostId, String instanceFileId);

    // Snapshot
    void commitConfigInstance(Connection conn, Map<String, Object> event) throws SQLException, Exception; // Applied pattern
    void rollbackConfigInstance(Connection conn, Map<String, Object> event) throws SQLException, Exception; // Applied pattern

    // Product / Instance Applicable Properties
    Result<String> getApplicableConfigPropertiesForInstance(int offset, int limit, String hostId, String instanceId, Set<String> resourceTypes, Set<String> configTypes, Set<String> propertyTypes);
    Result<String> getApplicableConfigPropertiesForInstanceApi(int offset, int limit, String hostId, String instanceApiId);
    Result<String> getApplicableConfigPropertiesForInstanceApp(int offset, int limit, String hostId, String instanceAppId);
    Result<String> getApplicableConfigPropertiesForInstanceAppApi(int offset, int limit, String hostId, String instanceAppId, String instanceApiId);

    // Aggregations
    Result<String> getAllAggregatedInstanceRuntimeConfigs(String hostId, String instanceId);

    Result<String> getPromotableInstanceConfigs(String hostId, String instanceId,Set<String> propertyNames,Set<String> apiUids);
}
