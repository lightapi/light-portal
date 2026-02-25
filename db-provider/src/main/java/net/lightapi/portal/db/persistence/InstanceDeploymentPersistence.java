package net.lightapi.portal.db.persistence;

import com.networknt.monad.Result;
import net.lightapi.portal.db.PortalPersistenceException;
import net.lightapi.portal.validation.FilterCriterion;
import net.lightapi.portal.validation.SortCriterion;

import java.sql.Connection; // Added import
import java.sql.SQLException; // Added import
import java.util.List;
import java.util.Map;

public interface InstanceDeploymentPersistence {
    // Instance
    void createInstance(Connection conn, Map<String, Object> event) throws PortalPersistenceException;
    void updateInstance(Connection conn, Map<String, Object> event) throws PortalPersistenceException;
    void deleteInstance(Connection conn, Map<String, Object> event) throws PortalPersistenceException;
    void lockInstance(Connection conn, Map<String, Object> event) throws PortalPersistenceException;
    void unlockInstance(Connection conn, Map<String, Object> event) throws PortalPersistenceException;
    void cloneInstance(Connection conn, Map<String, Object> event) throws PortalPersistenceException;
    void promoteInstance(Connection conn, Map<String, Object> event) throws PortalPersistenceException;
    Result<String> getInstance(int offset, int limit, String filters, String globalFilter, String sorting, boolean active, String hostId);
    Result<String> getInstanceLabel(String hostId);
    Result<String> getInstanceById(String hostId, String instanceId);
    String getInstanceId(String hostId, String serviceId, String envTag, String productVersionId);

    // InstanceApi
    void createInstanceApi(Connection conn, Map<String, Object> event) throws PortalPersistenceException;
    void deleteInstanceApi(Connection conn, Map<String, Object> event) throws PortalPersistenceException;
    Result<String> getInstanceApi(int offset, int limit, String filters, String globalFilter, String sorting, boolean active, String hostId);
    Result<String> getInstanceApiLabel(String hostId, String instanceId);
    Result<String> getInstanceApiById(String hostId, String instanceApiId);
    String getInstanceApiId(String hostId, String instanceId, String apiVersionId);

    // InstanceApiPathPrefix
    void createInstanceApiPathPrefix(Connection conn, Map<String, Object> event) throws PortalPersistenceException;
    void updateInstanceApiPathPrefix(Connection conn, Map<String, Object> event) throws PortalPersistenceException;
    void deleteInstanceApiPathPrefix(Connection conn, Map<String, Object> event) throws PortalPersistenceException;
    Result<String> getInstanceApiPathPrefix(int offset, int limit, String filters, String globalFilter, String sorting, boolean active, String hostId);
    Result<String> getInstanceApiPathPrefixById(String hostId, String instanceApiId, String pathPrefix);

    // InstanceApp
    void createInstanceApp(Connection conn, Map<String, Object> event) throws PortalPersistenceException;
    void deleteInstanceApp(Connection conn, Map<String, Object> event) throws PortalPersistenceException;
    Result<String> getInstanceApp(int offset, int limit, String filters, String globalFilter, String sorting, boolean active, String hostId);
    Result<String> getInstanceAppLabel(String hostId, String instanceId);
    Result<String> getInstanceAppById(String hostId, String instanceAppId);
    String getInstanceAppId(String hostId, String instanceId, String appId, String appVersion);

    // InstanceAppApi
    void createInstanceAppApi(Connection conn, Map<String, Object> event) throws PortalPersistenceException;
    void deleteInstanceAppApi(Connection conn, Map<String, Object> event) throws PortalPersistenceException;
    Result<String> getInstanceAppApi(int offset, int limit, String filters, String globalFilter, String sorting, boolean active, String hostId);
    Result<String> getInstanceAppApiById(String hostId, String instanceAppId, String instanceApiId);

    // ProductVersion
    void createProduct(Connection conn, Map<String, Object> event) throws PortalPersistenceException; // Renamed from createProductVersion in original
    void updateProduct(Connection conn, Map<String, Object> event) throws PortalPersistenceException; // Renamed from updateProductVersion
    void deleteProduct(Connection conn, Map<String, Object> event) throws PortalPersistenceException; // Renamed from deleteProductVersion
    Result<String> getProduct(int offset, int limit, String filters, String globalFilter, String sorting, boolean active, String hostId);
    Result<String> getProductVersion(String hostId, String productVersionId);
    Result<String> getProductIdLabel(String hostId);
    Result<String> getProductVersionLabel(String hostId, String productId);
    Result<String> getProductVersionIdLabel(String hostId);
    String getProductVersionId(String hostId, String productId, String productVersion);
    String queryProductVersionId(String hostId, String productId, String light4jVersion);

    // ProductVersionEnvironment
    void createProductVersionEnvironment(Connection conn, Map<String, Object> event) throws PortalPersistenceException;
    void updateProductVersionEnvironment(Connection conn, Map<String, Object> event) throws PortalPersistenceException;
    void deleteProductVersionEnvironment(Connection conn, Map<String, Object> event) throws PortalPersistenceException;
    Result<String> getProductVersionEnvironment(int offset, int limit, String filters, String globalFilter, String sorting, boolean active, String hostId);
    Result<String> getProductVersionEnvironmentById(String hostId, String productVersionId, String systemEnv, String runtimeEnv);

    // ProductVersionPipeline
    void createProductVersionPipeline(Connection conn, Map<String, Object> event) throws PortalPersistenceException;
    void deleteProductVersionPipeline(Connection conn, Map<String, Object> event) throws PortalPersistenceException;
    Result<String> getProductVersionPipeline(int offset, int limit, String filters, String globalFilter, String sorting, boolean active, String hostId);

    // ProductVersionConfig
    void createProductVersionConfig(Connection conn, Map<String, Object> event) throws PortalPersistenceException;
    void deleteProductVersionConfig(Connection conn, Map<String, Object> event) throws PortalPersistenceException;
    Result<String> getProductVersionConfig(int offset, int limit, String filters, String globalFilter, String sorting, boolean active, String hostId);

    // ProductVersionConfigProperty
    void createProductVersionConfigProperty(Connection conn, Map<String, Object> event) throws PortalPersistenceException;
    void deleteProductVersionConfigProperty(Connection conn, Map<String, Object> event) throws PortalPersistenceException;
    Result<String> getProductVersionConfigProperty(int offset, int limit, String filters, String globalFilter, String sorting, boolean active, String hostId);

    // Pipeline
    void createPipeline(Connection conn, Map<String, Object> event) throws PortalPersistenceException;
    void updatePipeline(Connection conn, Map<String, Object> event) throws PortalPersistenceException;
    void deletePipeline(Connection conn, Map<String, Object> event) throws PortalPersistenceException;
    Result<String> getPipeline(int offset, int limit, String filters, String globalFilter, String sorting, boolean active, String hostId);
    Result<String> getPipelineLabel(String hostId);
    Result<String> getPipelineById(String hostId, String pipelineId);
    String getPipelineId(String hostId, String platformId, String pipelineName, String pipelineVersion);

    // Platform
    void createPlatform(Connection conn, Map<String, Object> event) throws PortalPersistenceException;
    void updatePlatform(Connection conn, Map<String, Object> event) throws PortalPersistenceException;
    void deletePlatform(Connection conn, Map<String, Object> event) throws PortalPersistenceException;
    Result<String> getPlatform(int offset, int limit, String filters, String globalFilter, String sorting, boolean active, String hostId);
    Result<String> getPlatformLabel(String hostId);
    Result<String> getPlatformById(String hostId, String platformId);
    String getPlatformId(String hostId, String platformName, String platformVersion);

    // DeploymentInstance
    void createDeploymentInstance(Connection conn, Map<String, Object> event) throws PortalPersistenceException;
    void updateDeploymentInstance(Connection conn, Map<String, Object> event) throws PortalPersistenceException;
    void deleteDeploymentInstance(Connection conn, Map<String, Object> event) throws PortalPersistenceException;
    Result<String> getDeploymentInstance(int offset, int limit, String filters, String globalFilter, String sorting, boolean active, String hostId);
    Result<String> getDeploymentInstancePipeline(String hostId, String instanceId, String systemEnv, String runtimeEnv);
    Result<String> getDeploymentInstanceLabel(String hostId, String instanceId);
    Result<String> getDeploymentInstanceById(String hostId, String deploymentInstanceId);
    String getDeploymentInstanceId(String hostId, String instanceId, String serviceId);

    // Deployment
    void createDeployment(Connection conn, Map<String, Object> event) throws PortalPersistenceException;
    void updateDeployment(Connection conn, Map<String, Object> event) throws PortalPersistenceException;
    void updateDeploymentJobId(Connection conn, Map<String, Object> event) throws PortalPersistenceException;
    void updateDeploymentStatus(Connection conn, Map<String, Object> event) throws PortalPersistenceException;
    void deleteDeployment(Connection conn, Map<String, Object> event) throws PortalPersistenceException;
    Result<String> getDeployment(int offset, int limit, String filters, String globalFilter, String sorting, boolean active, String hostId);
    Result<String> getDeploymentById(String hostId, String deploymentId);
}
