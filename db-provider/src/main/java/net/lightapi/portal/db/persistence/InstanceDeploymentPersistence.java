package net.lightapi.portal.db.persistence;

import com.networknt.monad.Result;
import net.lightapi.portal.validation.FilterCriterion;
import net.lightapi.portal.validation.SortCriterion;

import java.sql.Connection; // Added import
import java.sql.SQLException; // Added import
import java.util.List;
import java.util.Map;

public interface InstanceDeploymentPersistence {
    // Instance
    void createInstance(Connection conn, Map<String, Object> event) throws SQLException, Exception;
    void updateInstance(Connection conn, Map<String, Object> event) throws SQLException, Exception;
    void deleteInstance(Connection conn, Map<String, Object> event) throws SQLException, Exception;
    Result<String> getInstance(int offset, int limit, String hostId, String instanceId, String instanceName, String productVersionId, String productId, String productVersion, String serviceId, Boolean current, Boolean readonly, String environment, String serviceDesc, String instanceDesc, String zone,  String region, String lob, String resourceName, String businessName, String envTag, String topicClassification);
    Result<String> getInstanceLabel(String hostId);

    // InstanceApi
    void createInstanceApi(Connection conn, Map<String, Object> event) throws SQLException, Exception;
    void updateInstanceApi(Connection conn, Map<String, Object> event) throws SQLException, Exception;
    void deleteInstanceApi(Connection conn, Map<String, Object> event) throws SQLException, Exception;
    Result<String> getInstanceApi(int offset, int limit, String hostId, String instanceApiId, String instanceId, String instanceName, String productId, String productVersion, String apiVersionId, String apiId, String apiVersion, Boolean active);
    Result<String> getInstanceApiLabel(String hostId, String instanceId);

    // InstanceApiPathPrefix
    void createInstanceApiPathPrefix(Connection conn, Map<String, Object> event) throws SQLException, Exception;
    void updateInstanceApiPathPrefix(Connection conn, Map<String, Object> event) throws SQLException, Exception;
    void deleteInstanceApiPathPrefix(Connection conn, Map<String, Object> event) throws SQLException, Exception;
    Result<String> getInstanceApiPathPrefix(int offset, int limit, String hostId, String instanceApiId, String instanceId, String instanceName, String productId, String productVersion, String apiVersionId, String apiId, String apiVersion, String pathPrefix);

    // InstanceApp
    void createInstanceApp(Connection conn, Map<String, Object> event) throws SQLException, Exception;
    void updateInstanceApp(Connection conn, Map<String, Object> event) throws SQLException, Exception;
    void deleteInstanceApp(Connection conn, Map<String, Object> event) throws SQLException, Exception;
    Result<String> getInstanceApp(int offset, int limit, String hostId, String instanceAppId, String instanceId, String instanceName, String productId, String productVersion, String appId, String appVersion, Boolean active);
    Result<String> getInstanceAppLabel(String hostId, String instanceId);

    // InstanceAppApi
    void createInstanceAppApi(Connection conn, Map<String, Object> event) throws SQLException, Exception;
    void updateInstanceAppApi(Connection conn, Map<String, Object> event) throws SQLException, Exception;
    void deleteInstanceAppApi(Connection conn, Map<String, Object> event) throws SQLException, Exception;
    Result<String> getInstanceAppApi(int offset, int limit, String hostId, String instanceAppId, String instanceApiId, String instanceId, String instanceName, String productId, String productVersion, String appId, String appVersion, String apiVersionId, String apiId, String apiVersion, Boolean active);

    // ProductVersion
    void createProduct(Connection conn, Map<String, Object> event) throws SQLException, Exception; // Renamed from createProductVersion in original
    void updateProduct(Connection conn, Map<String, Object> event) throws SQLException, Exception; // Renamed from updateProductVersion
    void deleteProduct(Connection conn, Map<String, Object> event) throws SQLException, Exception; // Renamed from deleteProductVersion
    Result<String> getProduct(int offset, int limit, String hostId, String productVersionId, String productId, String productVersion, String light4jVersion, Boolean breakCode, Boolean breakConfig, String releaseNote, String versionDesc, String releaseType, Boolean current, String versionStatus);
    Result<String> getProductIdLabel(String hostId);
    Result<String> getProductVersionLabel(String hostId, String productId);
    Result<String> getProductVersionIdLabel(String hostId);

    // ProductVersionEnvironment
    void createProductVersionEnvironment(Connection conn, Map<String, Object> event) throws SQLException, Exception;
    void deleteProductVersionEnvironment(Connection conn, Map<String, Object> event) throws SQLException, Exception;
    Result<String> getProductVersionEnvironment(int offset, int limit, String hostId, String productVersionId, String productId, String productVersion, String systemEnv, String runtimeEnv);

    // ProductVersionPipeline
    void createProductVersionPipeline(Connection conn, Map<String, Object> event) throws SQLException, Exception;
    void deleteProductVersionPipeline(Connection conn, Map<String, Object> event) throws SQLException, Exception;
    Result<String> getProductVersionPipeline(int offset, int limit, String hostId, String productVersionId, String productId, String productVersion, String pipelineId, String pipelineName, String pipelineVersion);

    // ProductVersionConfig
    void createProductVersionConfig(Connection conn, Map<String, Object> event) throws SQLException, Exception;
    void deleteProductVersionConfig(Connection conn, Map<String, Object> event) throws SQLException, Exception;
    Result<String> getProductVersionConfig(int offset, int limit, List<SortCriterion> sorting, List<FilterCriterion> filtering, String globalFilter,
                                           String hostId, String productVersionId, String productId, String productVersion, String configId,
                                           String configName);

    // ProductVersionConfigProperty
    void createProductVersionConfigProperty(Connection conn, Map<String, Object> event) throws SQLException, Exception;
    void deleteProductVersionConfigProperty(Connection conn, Map<String, Object> event) throws SQLException, Exception;
    Result<String> getProductVersionConfigProperty(int offset, int limit, String hostId, String productVersionId, String productId, String productVersion, String configId, String configName, String propertyId, String propertyName);

    // Pipeline
    void createPipeline(Connection conn, Map<String, Object> event) throws SQLException, Exception;
    void updatePipeline(Connection conn, Map<String, Object> event) throws SQLException, Exception;
    void deletePipeline(Connection conn, Map<String, Object> event) throws SQLException, Exception;
    Result<String> getPipeline(int offset, int limit, String hostId, String pipelineId, String platformId, String platformName, String platformVersion, String pipelineVersion, String pipelineName, Boolean current, String endpoint, String versionStatus, String systemEnv, String runtimeEnv, String requestSchema, String responseSchema);
    Result<String> getPipelineLabel(String hostId);

    // InstancePipeline
    void createInstancePipeline(Connection conn, Map<String, Object> event) throws SQLException, Exception;
    void updateInstancePipeline(Connection conn, Map<String, Object> event) throws SQLException, Exception;
    void deleteInstancePipeline(Connection conn, Map<String, Object> event) throws SQLException, Exception;
    Result<String> getInstancePipeline(int offset, int limit, String hostId, String instanceId, String instanceName, String productId, String productVersion, String pipelineId, String platformName, String platformVersion, String pipelineName, String pipelineVersion);

    // Platform
    void createPlatform(Connection conn, Map<String, Object> event) throws SQLException, Exception;
    void updatePlatform(Connection conn, Map<String, Object> event) throws SQLException, Exception;
    void deletePlatform(Connection conn, Map<String, Object> event) throws SQLException, Exception;
    Result<String> getPlatform(int offset, int limit, String hostId, String platformId, String platformName, String platformVersion, String clientType, String clientUrl, String credentials, String proxyUrl, Integer proxyPort, String handlerClass, String consoleUrl, String environment, String zone, String region, String lob);
    Result<String> getPlatformLabel(String hostId);

    // DeploymentInstance
    void createDeploymentInstance(Connection conn, Map<String, Object> event) throws SQLException, Exception;
    void updateDeploymentInstance(Connection conn, Map<String, Object> event) throws SQLException, Exception;
    void deleteDeploymentInstance(Connection conn, Map<String, Object> event) throws SQLException, Exception;
    Result<String> getDeploymentInstance(int offset, int limit, String hostId, String instanceId, String instanceName, String deploymentInstanceId, String serviceId, String ipAddress, Integer portNumber, String systemEnv, String runtimeEnv, String pipelineId, String pipelineName, String pipelineVersion, String deployStatus);
    Result<String> getDeploymentInstancePipeline(String hostId, String instanceId, String systemEnv, String runtimeEnv);
    Result<String> getDeploymentInstanceLabel(String hostId, String instanceId);

    // Deployment
    void createDeployment(Connection conn, Map<String, Object> event) throws SQLException, Exception;
    void updateDeployment(Connection conn, Map<String, Object> event) throws SQLException, Exception;
    void updateDeploymentJobId(Connection conn, Map<String, Object> event) throws SQLException, Exception;
    void updateDeploymentStatus(Connection conn, Map<String, Object> event) throws SQLException, Exception;
    void deleteDeployment(Connection conn, Map<String, Object> event) throws SQLException, Exception;
    Result<String> getDeployment(int offset, int limit, String hostId, String deploymentId, String deploymentInstanceId, String serviceId, String deploymentStatus, String deploymentType, String platformJobId);
}
