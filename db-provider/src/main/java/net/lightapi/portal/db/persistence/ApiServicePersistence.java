package net.lightapi.portal.db.persistence;

import com.networknt.monad.Result;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;

public interface ApiServicePersistence {
    // Service
    void createApi(Connection conn, Map<String, Object> event) throws SQLException, Exception;
    void updateApi(Connection conn, Map<String, Object> event) throws SQLException, Exception;
    void deleteApi(Connection conn, Map<String, Object> event) throws SQLException, Exception;
    Result<String> queryApi(int offset, int limit, String filters, String globalFilter, String sorting, String hostId);
    Result<String> queryApiLabel(String hostId);
    Result<String> getApiById(String hostId, String apiId);

    // Service Version
    void createApiVersion(Connection conn, Map<String, Object> event) throws SQLException, Exception;
    void createApiEndpoint(Connection conn, Map<String, Object> event) throws SQLException, Exception;
    void createApiEndpointScope(Connection conn, Map<String, Object> event) throws SQLException, Exception;
    void updateApiVersion(Connection conn, Map<String, Object> event) throws SQLException, Exception;
    void updateApiEndpoint(Connection conn, Map<String, Object> event) throws SQLException, Exception;
    void updateApiEndpointScope(Connection conn, Map<String, Object> event) throws SQLException, Exception;
    void deleteApiVersion(Connection conn, Map<String, Object> event) throws SQLException, Exception;
    void deleteApiEndpoint(Connection conn, Map<String, Object> event) throws SQLException, Exception;
    void deleteApiEndpointScope(Connection conn, Map<String, Object> event) throws SQLException, Exception;
    Result<String> getApiVersionById(String hostId, String apiVersionId);
    String queryApiVersionId(String hostId, String apiId, String apiVersion);
    Result<String> queryApiVersion(String hostId, String apiId);
    Result<String> getApiVersionIdLabel(String hostId);
    Result<String> queryApiVersionLabel(String hostId, String apiId);
    void updateApiVersionSpec(Connection conn, Map<String, Object> event) throws SQLException, Exception;

    // Service Endpoint
    Result<String> queryApiEndpoint(int offset, int limit, String filters, String globalFilter, String sorting, String hostId);
    Result<String> queryEndpointLabel(String hostId, String apiVersionId);
    Result<String> queryApiEndpointScope(String hostId, String endpointId);

    // Endpoint Rule
    void createApiEndpointRule(Connection conn, Map<String, Object> event) throws SQLException, Exception;
    void deleteApiEndpointRule(Connection conn, Map<String, Object> event) throws SQLException, Exception;
    Result<String> queryApiEndpointRule(String hostId, String endpointId);
    Result<String> queryServiceRule(String hostId, String apiId, String apiVersion);

    // Permissions and Filters (Aggregate Queries)
    Result<String> queryApiPermission(String hostId, String apiId, String apiVersion);
    Result<List<String>> queryApiFilter(String hostId, String apiId, String apiVersion);
    Result<String> getServiceIdLabel(String hostId);
}
