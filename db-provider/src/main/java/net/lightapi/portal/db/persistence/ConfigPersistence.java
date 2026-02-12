package net.lightapi.portal.db.persistence;

import com.networknt.monad.Result;
import net.lightapi.portal.db.PortalPersistenceException;

import java.sql.Connection; // Added import
import java.sql.SQLException; // Added import
import java.util.Map;
import java.util.Set;

public interface ConfigPersistence {
    // Config
    void createConfig(Connection conn, Map<String, Object> event) throws PortalPersistenceException;
    void updateConfig(Connection conn, Map<String, Object> event) throws PortalPersistenceException;
    void deleteConfig(Connection conn, Map<String, Object> event) throws PortalPersistenceException;
    Result<String> getConfig(int offset, int limit, String filters, String globalFilter, String sorting, boolean active);
    Result<String> queryConfigById(String configId);
    String queryConfigId(String configName);
    Result<String> getConfigIdLabel();
    Result<String> getConfigIdApiAppLabel(String resourceType);

    // ConfigProperty
    void createConfigProperty(Connection conn, Map<String, Object> event) throws PortalPersistenceException;
    void updateConfigProperty(Connection conn, Map<String, Object> event) throws PortalPersistenceException;
    void deleteConfigProperty(Connection conn, Map<String, Object> event) throws PortalPersistenceException;
    Result<String> getConfigProperty(int offset, int limit, String filters, String globalFilter, String sorting, boolean active);
    Result<String> queryConfigPropertyById(String configId); // Gets all properties for a configId
    Result<String> queryConfigPropertyByPropertyId(String configId, String propertyId); // Gets a specific property
    Result<String> getPropertyIdLabel(String configId);
    Result<String> getPropertyIdApiAppLabel(String configId, String resourceType);
    String queryPropertyId(String configName, String propertyName);
    Result<String> getPropertyById(String propertyId);
    String getPropertyId(String configId, String propertyName);

    // EnvironmentProperty
    void createConfigEnvironment(Connection conn, Map<String, Object> event) throws PortalPersistenceException;
    void updateConfigEnvironment(Connection conn, Map<String, Object> event) throws PortalPersistenceException;
    void deleteConfigEnvironment(Connection conn, Map<String, Object> event) throws PortalPersistenceException;
    Result<String> getConfigEnvironment(int offset, int limit, String filters, String globalFilter, String sorting, boolean active, String hostId);
    Result<String> getConfigEnvironmentById(String hostId, String environmentId, String propertyId);

    // InstanceProperty (was ConfigInstance)
    void createConfigInstance(Connection conn, Map<String, Object> event) throws PortalPersistenceException;
    void updateConfigInstance(Connection conn, Map<String, Object> event) throws PortalPersistenceException;
    void deleteConfigInstance(Connection conn, Map<String, Object> event) throws PortalPersistenceException;
    Result<String> getConfigInstance(int offset, int limit, String filters, String globalFilter, String sorting, boolean active, String hostId);
    Result<String> getConfigInstanceById(String hostId, String instanceId, String propertyId);

    // InstanceApiProperty (was ConfigInstanceApi)
    void createConfigInstanceApi(Connection conn, Map<String, Object> event) throws PortalPersistenceException;
    void updateConfigInstanceApi(Connection conn, Map<String, Object> event) throws PortalPersistenceException;
    void deleteConfigInstanceApi(Connection conn, Map<String, Object> event) throws PortalPersistenceException;
    Result<String> getConfigInstanceApi(int offset, int limit, String filters, String globalFilter, String sorting, boolean active, String hostId);
    Result<String> getConfigInstanceApiById(String hostId, String instanceApiId, String propertyId);

    // InstanceAppProperty (was ConfigInstanceApp)
    void createConfigInstanceApp(Connection conn, Map<String, Object> event) throws PortalPersistenceException;
    void updateConfigInstanceApp(Connection conn, Map<String, Object> event) throws PortalPersistenceException;
    void deleteConfigInstanceApp(Connection conn, Map<String, Object> event) throws PortalPersistenceException;
    Result<String> getConfigInstanceApp(int offset, int limit, String filters, String globalFilter, String sorting, boolean active, String hostId);
    Result<String> getConfigInstanceAppById(String hostId, String instanceAppId, String propertyId);

    // InstanceAppApiProperty (was ConfigInstanceAppApi)
    void createConfigInstanceAppApi(Connection conn, Map<String, Object> event) throws PortalPersistenceException;
    void updateConfigInstanceAppApi(Connection conn, Map<String, Object> event) throws PortalPersistenceException;
    void deleteConfigInstanceAppApi(Connection conn, Map<String, Object> event) throws PortalPersistenceException;
    Result<String> getConfigInstanceAppApi(int offset, int limit, String filters, String globalFilter, String sorting, boolean active, String hostId);
    Result<String> getConfigInstanceAppApiById(String hostId, String instanceAppId, String instanceApiId, String propertyId);

    // DeploymentInstanceProperty (was ConfigDeploymentInstance)
    void createConfigDeploymentInstance(Connection conn, Map<String, Object> event) throws PortalPersistenceException;
    void updateConfigDeploymentInstance(Connection conn, Map<String, Object> event) throws PortalPersistenceException;
    void deleteConfigDeploymentInstance(Connection conn, Map<String, Object> event) throws PortalPersistenceException;
    Result<String> getConfigDeploymentInstance(int offset, int limit, String filters, String globalFilter, String sorting, boolean active, String hostId);
    Result<String> getConfigDeploymentInstanceById(String hostId, String deploymentInstanceId, String propertyId);

    // ProductProperty (was ConfigProduct)
    void createConfigProduct(Connection conn, Map<String, Object> event) throws PortalPersistenceException;
    void updateConfigProduct(Connection conn, Map<String, Object> event) throws PortalPersistenceException;
    void deleteConfigProduct(Connection conn, Map<String, Object> event) throws PortalPersistenceException;
    Result<String> getConfigProduct(int offset, int limit, String filters, String globalFilter, String sorting, boolean active);
    Result<String> getConfigProductById(String productId, String propertyId);

    // ProductVersionProperty (was ConfigProductVersion)
    void createConfigProductVersion(Connection conn, Map<String, Object> event) throws PortalPersistenceException;
    void updateConfigProductVersion(Connection conn, Map<String, Object> event) throws PortalPersistenceException;
    void deleteConfigProductVersion(Connection conn, Map<String, Object> event) throws PortalPersistenceException;
    Result<String> getConfigProductVersion(int offset, int limit, String filters, String globalFilter, String sorting, boolean active, String hostId);
    Result<String> getConfigProductVersionById(String hostId, String productVersionId, String propertyId);

    // InstanceFile (was ConfigInstanceFile)
    void createConfigInstanceFile(Connection conn, Map<String, Object> event) throws PortalPersistenceException;
    void updateConfigInstanceFile(Connection conn, Map<String, Object> event) throws PortalPersistenceException;
    void deleteConfigInstanceFile(Connection conn, Map<String, Object> event) throws PortalPersistenceException;
    Result<String> getConfigInstanceFile(int offset, int limit, String filters, String globalFilter, String sorting, boolean active, String hostId);
    Result<String> getConfigInstanceFileById(String hostId, String instanceFileId);

    // Snapshot
    void createConfigSnapshot(Connection conn, Map<String, Object> event) throws PortalPersistenceException;
    void updateConfigSnapshot(Connection conn, Map<String, Object> event) throws PortalPersistenceException;
    void deleteConfigSnapshot(Connection conn, Map<String, Object> event) throws PortalPersistenceException;
    Result<String> getConfigSnapshot(int offset, int limit, String filters, String globalFilter, String sorting, String hostId);

    // Product / Instance Applicable Properties
    Result<String> getApplicableConfigPropertiesForInstance(int offset, int limit, String hostId, String instanceId, Set<String> resourceTypes, Set<String> configTypes, Set<String> propertyTypes,Set<String> configPhases);
    Result<String> getApplicableConfigPropertiesForInstanceApi(int offset, int limit, String hostId, String instanceApiId);
    Result<String> getApplicableConfigPropertiesForInstanceApp(int offset, int limit, String hostId, String instanceAppId);
    Result<String> getApplicableConfigPropertiesForInstanceAppApi(int offset, int limit, String hostId, String instanceAppId, String instanceApiId);

    // Aggregations
    Result<String> getAllAggregatedInstanceRuntimeConfigs(String hostId, String instanceId);

    Result<String> getPromotableInstanceConfigs(String hostId, String instanceId,Set<String> propertyNames,Set<String> apiUids);
}
