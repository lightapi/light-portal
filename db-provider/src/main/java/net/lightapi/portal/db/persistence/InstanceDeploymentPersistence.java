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
    void lockInstance(Connection conn, Map<String, Object> event) throws Exception;
    void unlockInstance(Connection conn, Map<String, Object> event) throws Exception;
    void cloneInstance(Connection conn, Map<String, Object> event) throws Exception;
    void promoteInstance(Connection conn, Map<String, Object> event) throws Exception;
    Result<String> getInstance(int offset, int limit, String filters, String globalFilter, String sortin, String hostId);
    Result<String> getInstanceLabel(String hostId);

    // InstanceApi
    void createInstanceApi(Connection conn, Map<String, Object> event) throws SQLException, Exception;
    void updateInstanceApi(Connection conn, Map<String, Object> event) throws SQLException, Exception;
    void deleteInstanceApi(Connection conn, Map<String, Object> event) throws SQLException, Exception;
    Result<String> getInstanceApi(int offset, int limit, String filters, String globalFilter, String sorting, String hostId);
    Result<String> getInstanceApiLabel(String hostId, String instanceId);

    // InstanceApiPathPrefix
    void createInstanceApiPathPrefix(Connection conn, Map<String, Object> event) throws SQLException, Exception;
    void updateInstanceApiPathPrefix(Connection conn, Map<String, Object> event) throws SQLException, Exception;
    void deleteInstanceApiPathPrefix(Connection conn, Map<String, Object> event) throws SQLException, Exception;
    Result<String> getInstanceApiPathPrefix(int offset, int limit, String filters, String globalFilter, String sorting, String hostId);

    // InstanceApp
    void createInstanceApp(Connection conn, Map<String, Object> event) throws SQLException, Exception;
    void updateInstanceApp(Connection conn, Map<String, Object> event) throws SQLException, Exception;
    void deleteInstanceApp(Connection conn, Map<String, Object> event) throws SQLException, Exception;
    Result<String> getInstanceApp(int offset, int limit, String filters, String globalFilter, String sorting, String hostId);
    Result<String> getInstanceAppLabel(String hostId, String instanceId);

    // InstanceAppApi
    void createInstanceAppApi(Connection conn, Map<String, Object> event) throws SQLException, Exception;
    void updateInstanceAppApi(Connection conn, Map<String, Object> event) throws SQLException, Exception;
    void deleteInstanceAppApi(Connection conn, Map<String, Object> event) throws SQLException, Exception;
    Result<String> getInstanceAppApi(int offset, int limit, String filters, String globalFilter, String sorting, String hostId);

    // ProductVersion
    void createProduct(Connection conn, Map<String, Object> event) throws SQLException, Exception; // Renamed from createProductVersion in original
    void updateProduct(Connection conn, Map<String, Object> event) throws SQLException, Exception; // Renamed from updateProductVersion
    void deleteProduct(Connection conn, Map<String, Object> event) throws SQLException, Exception; // Renamed from deleteProductVersion
    Result<String> getProduct(int offset, int limit, String filters, String globalFilter, String sorting, String hostId);
    Result<String> getProductVersion(String hostId, String productVersionId);
    Result<String> getProductIdLabel(String hostId);
    Result<String> getProductVersionLabel(String hostId, String productId);
    Result<String> getProductVersionIdLabel(String hostId);
    String getProductVersionId(String hostId, String productId, String productVersion);
    String queryProductVersionId(String hostId, String productId, String light4jVersion);

    // ProductVersionEnvironment
    void createProductVersionEnvironment(Connection conn, Map<String, Object> event) throws SQLException, Exception;
    void deleteProductVersionEnvironment(Connection conn, Map<String, Object> event) throws SQLException, Exception;
    Result<String> getProductVersionEnvironment(int offset, int limit, String filters, String globalFilter, String sorting, String hostId);

    // ProductVersionPipeline
    void createProductVersionPipeline(Connection conn, Map<String, Object> event) throws SQLException, Exception;
    void deleteProductVersionPipeline(Connection conn, Map<String, Object> event) throws SQLException, Exception;
    Result<String> getProductVersionPipeline(int offset, int limit, String filters, String globalFilter, String sorting, String hostId);

    // ProductVersionConfig
    void createProductVersionConfig(Connection conn, Map<String, Object> event) throws SQLException, Exception;
    void deleteProductVersionConfig(Connection conn, Map<String, Object> event) throws SQLException, Exception;
    Result<String> getProductVersionConfig(int offset, int limit, String filters, String globalFilter, String sorting, String hostId);

    // ProductVersionConfigProperty
    void createProductVersionConfigProperty(Connection conn, Map<String, Object> event) throws SQLException, Exception;
    void deleteProductVersionConfigProperty(Connection conn, Map<String, Object> event) throws SQLException, Exception;
    Result<String> getProductVersionConfigProperty(int offset, int limit, String filters, String globalFilter, String sorting, String hostId);

    // Pipeline
    void createPipeline(Connection conn, Map<String, Object> event) throws SQLException, Exception;
    void updatePipeline(Connection conn, Map<String, Object> event) throws SQLException, Exception;
    void deletePipeline(Connection conn, Map<String, Object> event) throws SQLException, Exception;
    Result<String> getPipeline(int offset, int limit, String filters, String globalFilter, String sorting, String hostId);
    Result<String> getPipelineLabel(String hostId);

    // Platform
    void createPlatform(Connection conn, Map<String, Object> event) throws SQLException, Exception;
    void updatePlatform(Connection conn, Map<String, Object> event) throws SQLException, Exception;
    void deletePlatform(Connection conn, Map<String, Object> event) throws SQLException, Exception;
    Result<String> getPlatform(int offset, int limit, String filters, String globalFilter, String sorting, String hostId);
    Result<String> getPlatformLabel(String hostId);

    // DeploymentInstance
    void createDeploymentInstance(Connection conn, Map<String, Object> event) throws SQLException, Exception;
    void updateDeploymentInstance(Connection conn, Map<String, Object> event) throws SQLException, Exception;
    void deleteDeploymentInstance(Connection conn, Map<String, Object> event) throws SQLException, Exception;
    Result<String> getDeploymentInstance(int offset, int limit, String filters, String globalFilter, String sorting, String hostId);
    Result<String> getDeploymentInstancePipeline(String hostId, String instanceId, String systemEnv, String runtimeEnv);
    Result<String> getDeploymentInstanceLabel(String hostId, String instanceId);

    // Deployment
    void createDeployment(Connection conn, Map<String, Object> event) throws SQLException, Exception;
    void updateDeployment(Connection conn, Map<String, Object> event) throws SQLException, Exception;
    void updateDeploymentJobId(Connection conn, Map<String, Object> event) throws SQLException, Exception;
    void updateDeploymentStatus(Connection conn, Map<String, Object> event) throws SQLException, Exception;
    void deleteDeployment(Connection conn, Map<String, Object> event) throws SQLException, Exception;
    Result<String> getDeployment(int offset, int limit, String filters, String globalFilter, String sorting, String hostId);
}
