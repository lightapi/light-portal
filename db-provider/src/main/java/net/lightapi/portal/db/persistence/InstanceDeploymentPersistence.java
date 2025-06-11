package net.lightapi.portal.db.persistence;

import com.networknt.monad.Result;
import net.lightapi.portal.validation.FilterCriterion;
import net.lightapi.portal.validation.SortCriterion;

import java.util.List;
import java.util.Map;

public interface InstanceDeploymentPersistence {
    // Instance
    Result<String> createInstance(Map<String, Object> event);
    Result<String> updateInstance(Map<String, Object> event);
    Result<String> deleteInstance(Map<String, Object> event);
    Result<String> getInstance(int offset, int limit, String hostId, String instanceId, String instanceName, String productVersionId, String productId, String productVersion, String serviceId, Boolean current, Boolean readonly, String environment, String serviceDesc, String instanceDesc, String zone,  String region, String lob, String resourceName, String businessName, String envTag, String topicClassification);
    Result<String> getInstanceLabel(String hostId);

    // InstanceApi
    Result<String> createInstanceApi(Map<String, Object> event);
    Result<String> updateInstanceApi(Map<String, Object> event);
    Result<String> deleteInstanceApi(Map<String, Object> event);
    Result<String> getInstanceApi(int offset, int limit, String hostId, String instanceApiId, String instanceId, String instanceName, String productId, String productVersion, String apiVersionId, String apiId, String apiVersion, Boolean active);
    Result<String> getInstanceApiLabel(String hostId, String instanceId);

    // InstanceApiPathPrefix
    Result<String> createInstanceApiPathPrefix(Map<String, Object> event);
    Result<String> updateInstanceApiPathPrefix(Map<String, Object> event);
    Result<String> deleteInstanceApiPathPrefix(Map<String, Object> event);
    Result<String> getInstanceApiPathPrefix(int offset, int limit, String hostId, String instanceApiId, String instanceId, String instanceName, String productId, String productVersion, String apiVersionId, String apiId, String apiVersion, String pathPrefix);

    // InstanceApp
    Result<String> createInstanceApp(Map<String, Object> event);
    Result<String> updateInstanceApp(Map<String, Object> event);
    Result<String> deleteInstanceApp(Map<String, Object> event);
    Result<String> getInstanceApp(int offset, int limit, String hostId, String instanceAppId, String instanceId, String instanceName, String productId, String productVersion, String appId, String appVersion, Boolean active);
    Result<String> getInstanceAppLabel(String hostId, String instanceId);

    // InstanceAppApi
    Result<String> createInstanceAppApi(Map<String, Object> event);
    Result<String> updateInstanceAppApi(Map<String, Object> event);
    Result<String> deleteInstanceAppApi(Map<String, Object> event);
    Result<String> getInstanceAppApi(int offset, int limit, String hostId, String instanceAppId, String instanceApiId, String instanceId, String instanceName, String productId, String productVersion, String appId, String appVersion, String apiVersionId, String apiId, String apiVersion, Boolean active);

    // ProductVersion
    Result<String> createProduct(Map<String, Object> event); // Renamed from createProductVersion in original
    Result<String> updateProduct(Map<String, Object> event); // Renamed from updateProductVersion
    Result<String> deleteProduct(Map<String, Object> event); // Renamed from deleteProductVersion
    Result<String> getProduct(int offset, int limit, String hostId, String productVersionId, String productId, String productVersion, String light4jVersion, Boolean breakCode, Boolean breakConfig, String releaseNote, String versionDesc, String releaseType, Boolean current, String versionStatus);
    Result<String> getProductIdLabel(String hostId);
    Result<String> getProductVersionLabel(String hostId, String productId);
    Result<String> getProductVersionIdLabel(String hostId);

    // ProductVersionEnvironment
    Result<String> createProductVersionEnvironment(Map<String, Object> event);
    Result<String> deleteProductVersionEnvironment(Map<String, Object> event);
    Result<String> getProductVersionEnvironment(int offset, int limit, String hostId, String productVersionId, String productId, String productVersion, String systemEnv, String runtimeEnv);

    // ProductVersionPipeline
    Result<String> createProductVersionPipeline(Map<String, Object> event);
    Result<String> deleteProductVersionPipeline(Map<String, Object> event);
    Result<String> getProductVersionPipeline(int offset, int limit, String hostId, String productVersionId, String productId, String productVersion, String pipelineId, String pipelineName, String pipelineVersion);

    // ProductVersionConfig
    Result<String> createProductVersionConfig(Map<String, Object> event);
    Result<String> deleteProductVersionConfig(Map<String, Object> event);
    Result<String> getProductVersionConfig(int offset, int limit, List<SortCriterion> sorting, List<FilterCriterion> filtering, String globalFilter,
                                           String hostId, String productVersionId, String productId, String productVersion, String configId,
                                           String configName);

    // ProductVersionConfigProperty
    Result<String> createProductVersionConfigProperty(Map<String, Object> event);
    Result<String> deleteProductVersionConfigProperty(Map<String, Object> event);
    Result<String> getProductVersionConfigProperty(int offset, int limit, String hostId, String productVersionId, String productId, String productVersion, String configId, String configName, String propertyId, String propertyName);

    // Pipeline
    Result<String> createPipeline(Map<String, Object> event);
    Result<String> updatePipeline(Map<String, Object> event);
    Result<String> deletePipeline(Map<String, Object> event);
    Result<String> getPipeline(int offset, int limit, String hostId, String pipelineId, String platformId, String platformName, String platformVersion, String pipelineVersion, String pipelineName, Boolean current, String endpoint, String versionStatus, String systemEnv, String runtimeEnv, String requestSchema, String responseSchema);
    Result<String> getPipelineLabel(String hostId);

    // InstancePipeline
    Result<String> createInstancePipeline(Map<String, Object> event);
    Result<String> updateInstancePipeline(Map<String, Object> event);
    Result<String> deleteInstancePipeline(Map<String, Object> event);
    Result<String> getInstancePipeline(int offset, int limit, String hostId, String instanceId, String instanceName, String productId, String productVersion, String pipelineId, String platformName, String platformVersion, String pipelineName, String pipelineVersion);

    // Platform
    Result<String> createPlatform(Map<String, Object> event);
    Result<String> updatePlatform(Map<String, Object> event);
    Result<String> deletePlatform(Map<String, Object> event);
    Result<String> getPlatform(int offset, int limit, String hostId, String platformId, String platformName, String platformVersion, String clientType, String clientUrl, String credentials, String proxyUrl, Integer proxyPort, String handlerClass, String consoleUrl, String environment, String zone, String region, String lob);
    Result<String> getPlatformLabel(String hostId);

    // DeploymentInstance
    Result<String> createDeploymentInstance(Map<String, Object> event);
    Result<String> updateDeploymentInstance(Map<String, Object> event);
    Result<String> deleteDeploymentInstance(Map<String, Object> event);
    Result<String> getDeploymentInstance(int offset, int limit, String hostId, String instanceId, String instanceName, String deploymentInstanceId, String serviceId, String ipAddress, Integer portNumber, String systemEnv, String runtimeEnv, String pipelineId, String pipelineName, String pipelineVersion, String deployStatus);
    Result<String> getDeploymentInstancePipeline(String hostId, String instanceId, String systemEnv, String runtimeEnv);
    Result<String> getDeploymentInstanceLabel(String hostId, String instanceId);

    // Deployment
    Result<String> createDeployment(Map<String, Object> event);
    Result<String> updateDeployment(Map<String, Object> event);
    Result<String> updateDeploymentJobId(Map<String, Object> event);
    Result<String> updateDeploymentStatus(Map<String, Object> event);
    Result<String> deleteDeployment(Map<String, Object> event);
    Result<String> getDeployment(int offset, int limit, String hostId, String deploymentId, String deploymentInstanceId, String serviceId, String deploymentStatus, String deploymentType, String platformJobId);
}
