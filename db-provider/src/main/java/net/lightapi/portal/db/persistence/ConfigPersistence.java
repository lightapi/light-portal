package net.lightapi.portal.db.persistence;

import com.networknt.monad.Result;
import net.lightapi.portal.validation.FilterCriterion;
import net.lightapi.portal.validation.SortCriterion;

import java.sql.Timestamp;
import java.util.List;
import java.util.Map;

public interface ConfigPersistence {
    // Config
    Result<String> createConfig(Map<String, Object> event);
    Result<String> updateConfig(Map<String, Object> event);
    Result<String> deleteConfig(Map<String, Object> event);
    Result<String> getConfig(int offset, int limit, String configId, String configName, String configPhase, String configType, String light4jVersion, String classPath, String configDesc);
    Result<String> queryConfigById(String configId);
    Result<String> getConfigIdLabel();
    Result<String> getConfigIdApiAppLabel(String resourceType);

    // ConfigProperty
    Result<String> createConfigProperty(Map<String, Object> event);
    Result<String> updateConfigProperty(Map<String, Object> event);
    Result<String> deleteConfigProperty(Map<String, Object> event);
    Result<String> getConfigProperty(int offset, int limit, String configId, String configName, String propertyId, String propertyName, String propertyType, String light4jVersion, Integer displayOrder, Boolean required, String propertyDesc, String propertyValue, String valueType, String resourceType);
    Result<String> queryConfigPropertyById(String configId); // Gets all properties for a configId
    Result<String> queryConfigPropertyByPropertyId(String configId, String propertyId); // Gets a specific property
    Result<String> getPropertyIdLabel(String configId);
    Result<String> getPropertyIdApiAppLabel(String configId, String resourceType);

    // EnvironmentProperty
    Result<String> createConfigEnvironment(Map<String, Object> event);
    Result<String> updateConfigEnvironment(Map<String, Object> event);
    Result<String> deleteConfigEnvironment(Map<String, Object> event);
    Result<String> getConfigEnvironment(int offset, int limit, String hostId, String environment, String configId, String configName, String propertyId, String propertyName, String propertyValue);

    // InstanceProperty (was ConfigInstance)
    Result<String> createConfigInstance(Map<String, Object> event);
    Result<String> updateConfigInstance(Map<String, Object> event);
    Result<String> deleteConfigInstance(Map<String, Object> event);
    Result<String> getConfigInstance(int offset, int limit, String hostId, String instanceId, String instanceName, String configId, String configName, String propertyId, String propertyName, String propertyValue);

    // InstanceApiProperty (was ConfigInstanceApi)
    Result<String> createConfigInstanceApi(Map<String, Object> event);
    Result<String> updateConfigInstanceApi(Map<String, Object> event);
    Result<String> deleteConfigInstanceApi(Map<String, Object> event);
    Result<String> getConfigInstanceApi(int offset, int limit, String hostId, String instanceApiId, String instanceId, String instanceName, String apiVersionId, String apiId, String apiVersion, String configId, String configName, String propertyId, String propertyName, String propertyValue);

    // InstanceAppProperty (was ConfigInstanceApp)
    Result<String> createConfigInstanceApp(Map<String, Object> event);
    Result<String> updateConfigInstanceApp(Map<String, Object> event);
    Result<String> deleteConfigInstanceApp(Map<String, Object> event);
    Result<String> getConfigInstanceApp(int offset, int limit, String hostId, String instanceAppId, String instanceId, String instanceName, String appId, String appVersion, String configId, String configName, String propertyId, String propertyName, String propertyValue);

    // InstanceAppApiProperty (was ConfigInstanceAppApi)
    Result<String> createConfigInstanceAppApi(Map<String, Object> event);
    Result<String> updateConfigInstanceAppApi(Map<String, Object> event);
    Result<String> deleteConfigInstanceAppApi(Map<String, Object> event);
    Result<String> getConfigInstanceAppApi(int offset, int limit, String hostId, String instanceAppId, String instanceApiId, String instanceId, String instanceName, String appId, String appVersion, String apiVersionId, String apiId, String apiVersion, String configId, String configName, String propertyId, String propertyName, String propertyValue);

    // DeploymentInstanceProperty (was ConfigDeploymentInstance)
    Result<String> createConfigDeploymentInstance(Map<String, Object> event);
    Result<String> updateConfigDeploymentInstance(Map<String, Object> event);
    Result<String> deleteConfigDeploymentInstance(Map<String, Object> event);
    Result<String> getConfigDeploymentInstance(int offset, int limit, String hostId, String deploymentInstanceId, String instanceId, String instanceName, String serviceId, String ipAddress, Integer portNumber, String configId, String configName, String propertyId, String propertyName, String propertyValue);

    // ProductProperty (was ConfigProduct)
    Result<String> createConfigProduct(Map<String, Object> event);
    Result<String> updateConfigProduct(Map<String, Object> event);
    Result<String> deleteConfigProduct(Map<String, Object> event);
    Result<String> getConfigProduct(int offset, int limit, String productId, String configId, String configName, String propertyId, String propertyName, String propertyValue);

    // ProductVersionProperty (was ConfigProductVersion)
    Result<String> createConfigProductVersion(Map<String, Object> event);
    Result<String> updateConfigProductVersion(Map<String, Object> event);
    Result<String> deleteConfigProductVersion(Map<String, Object> event);
    Result<String> getConfigProductVersion(int offset, int limit, String hostId, String productId, String productVersion, String configId, String configName, String propertyId, String propertyName, String propertyValue);

    // InstanceFile (was ConfigInstanceFile)
    Result<String> createConfigInstanceFile(Map<String, Object> event);
    Result<String> updateConfigInstanceFile(Map<String, Object> event);
    Result<String> deleteConfigInstanceFile(Map<String, Object> event);
    Result<String> getConfigInstanceFile(int offset, int limit, String hostId, String instanceFileId, String instanceId, String instanceName, String fileType, String fileName, String fileValue, String fileDesc, String expirationTs);

    // Snapshot
    Result<String> commitConfigInstance(Map<String, Object> event);
    Result<String> rollbackConfigInstance(Map<String, Object> event);
}
