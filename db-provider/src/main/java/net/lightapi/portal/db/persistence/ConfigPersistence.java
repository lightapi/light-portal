package net.lightapi.portal.db.persistence;

import com.networknt.monad.Result;
import java.sql.Connection; // Added import
import java.sql.SQLException; // Added import
import java.util.Map;

public interface ConfigPersistence {
    // Config
    void createConfig(Connection conn, Map<String, Object> event) throws SQLException, Exception;
    void updateConfig(Connection conn, Map<String, Object> event) throws SQLException, Exception;
    void deleteConfig(Connection conn, Map<String, Object> event) throws SQLException, Exception;
    Result<String> getConfig(int offset, int limit, String configId, String configName, String configPhase, String configType, String light4jVersion, String classPath, String configDesc);
    Result<String> queryConfigById(String configId);
    Result<String> getConfigIdLabel();
    Result<String> getConfigIdApiAppLabel(String resourceType);

    // ConfigProperty
    void createConfigProperty(Connection conn, Map<String, Object> event) throws SQLException, Exception;
    void updateConfigProperty(Connection conn, Map<String, Object> event) throws SQLException, Exception;
    void deleteConfigProperty(Connection conn, Map<String, Object> event) throws SQLException, Exception;
    Result<String> getConfigProperty(int offset, int limit, String configId, String configName, String propertyId, String propertyName, String propertyType, String light4jVersion, Integer displayOrder, Boolean required, String propertyDesc, String propertyValue, String valueType, String resourceType);
    Result<String> queryConfigPropertyById(String configId); // Gets all properties for a configId
    Result<String> queryConfigPropertyByPropertyId(String configId, String propertyId); // Gets a specific property
    Result<String> getPropertyIdLabel(String configId);
    Result<String> getPropertyIdApiAppLabel(String configId, String resourceType);

    // EnvironmentProperty
    void createConfigEnvironment(Connection conn, Map<String, Object> event) throws SQLException, Exception;
    void updateConfigEnvironment(Connection conn, Map<String, Object> event) throws SQLException, Exception;
    void deleteConfigEnvironment(Connection conn, Map<String, Object> event) throws SQLException, Exception;
    Result<String> getConfigEnvironment(int offset, int limit, String hostId, String environment, String configId, String configName, String propertyId, String propertyName, String propertyValue);

    // InstanceProperty (was ConfigInstance)
    void createConfigInstance(Connection conn, Map<String, Object> event) throws SQLException, Exception;
    void updateConfigInstance(Connection conn, Map<String, Object> event) throws SQLException, Exception;
    void deleteConfigInstance(Connection conn, Map<String, Object> event) throws SQLException, Exception;
    Result<String> getConfigInstance(int offset, int limit, String hostId, String instanceId, String instanceName, String configId, String configName, String propertyId, String propertyName, String propertyValue);

    // InstanceApiProperty (was ConfigInstanceApi)
    void createConfigInstanceApi(Connection conn, Map<String, Object> event) throws SQLException, Exception;
    void updateConfigInstanceApi(Connection conn, Map<String, Object> event) throws SQLException, Exception;
    void deleteConfigInstanceApi(Connection conn, Map<String, Object> event) throws SQLException, Exception;
    Result<String> getConfigInstanceApi(int offset, int limit, String hostId, String instanceApiId, String instanceId, String instanceName, String apiVersionId, String apiId, String apiVersion, String configId, String configName, String propertyId, String propertyName, String propertyValue);

    // InstanceAppProperty (was ConfigInstanceApp)
    void createConfigInstanceApp(Connection conn, Map<String, Object> event) throws SQLException, Exception;
    void updateConfigInstanceApp(Connection conn, Map<String, Object> event) throws SQLException, Exception;
    void deleteConfigInstanceApp(Connection conn, Map<String, Object> event) throws SQLException, Exception;
    Result<String> getConfigInstanceApp(int offset, int limit, String hostId, String instanceAppId, String instanceId, String instanceName, String appId, String appVersion, String configId, String configName, String propertyId, String propertyName, String propertyValue);

    // InstanceAppApiProperty (was ConfigInstanceAppApi)
    void createConfigInstanceAppApi(Connection conn, Map<String, Object> event) throws SQLException, Exception;
    void updateConfigInstanceAppApi(Connection conn, Map<String, Object> event) throws SQLException, Exception;
    void deleteConfigInstanceAppApi(Connection conn, Map<String, Object> event) throws SQLException, Exception;
    Result<String> getConfigInstanceAppApi(int offset, int limit, String hostId, String instanceAppId, String instanceApiId, String instanceId, String instanceName, String appId, String appVersion, String apiVersionId, String apiId, String apiVersion, String configId, String configName, String propertyId, String propertyName, String propertyValue);

    // DeploymentInstanceProperty (was ConfigDeploymentInstance)
    void createConfigDeploymentInstance(Connection conn, Map<String, Object> event) throws SQLException, Exception;
    void updateConfigDeploymentInstance(Connection conn, Map<String, Object> event) throws SQLException, Exception;
    void deleteConfigDeploymentInstance(Connection conn, Map<String, Object> event) throws SQLException, Exception;
    Result<String> getConfigDeploymentInstance(int offset, int limit, String hostId, String deploymentInstanceId, String instanceId, String instanceName, String serviceId, String ipAddress, Integer portNumber, String configId, String configName, String propertyId, String propertyName, String propertyValue);

    // ProductProperty (was ConfigProduct)
    void createConfigProduct(Connection conn, Map<String, Object> event) throws SQLException, Exception;
    void updateConfigProduct(Connection conn, Map<String, Object> event) throws SQLException, Exception;
    void deleteConfigProduct(Connection conn, Map<String, Object> event) throws SQLException, Exception;
    Result<String> getConfigProduct(int offset, int limit, String productId, String configId, String configName, String propertyId, String propertyName, String propertyValue);

    // ProductVersionProperty (was ConfigProductVersion)
    void createConfigProductVersion(Connection conn, Map<String, Object> event) throws SQLException, Exception;
    void updateConfigProductVersion(Connection conn, Map<String, Object> event) throws SQLException, Exception;
    void deleteConfigProductVersion(Connection conn, Map<String, Object> event) throws SQLException, Exception;
    Result<String> getConfigProductVersion(int offset, int limit, String hostId, String productId, String productVersion, String configId, String configName, String propertyId, String propertyName, String propertyValue);

    // InstanceFile (was ConfigInstanceFile)
    void createConfigInstanceFile(Connection conn, Map<String, Object> event) throws SQLException, Exception;
    void updateConfigInstanceFile(Connection conn, Map<String, Object> event) throws SQLException, Exception;
    void deleteConfigInstanceFile(Connection conn, Map<String, Object> event) throws SQLException, Exception;
    Result<String> getConfigInstanceFile(int offset, int limit, String hostId, String instanceFileId, String instanceId, String instanceName, String fileType, String fileName, String fileValue, String fileDesc, String expirationTs);

    // Snapshot
    void commitConfigInstance(Connection conn, Map<String, Object> event) throws SQLException, Exception; // Applied pattern
    void rollbackConfigInstance(Connection conn, Map<String, Object> event) throws SQLException, Exception; // Applied pattern
}
