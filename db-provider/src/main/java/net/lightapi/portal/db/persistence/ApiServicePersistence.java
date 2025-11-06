package net.lightapi.portal.db.persistence;

import com.networknt.monad.Result;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;

public interface ApiServicePersistence {
    // Service
    void createService(Connection conn, Map<String, Object> event) throws SQLException, Exception;
    void updateService(Connection conn, Map<String, Object> event) throws SQLException, Exception;
    void deleteService(Connection conn, Map<String, Object> event) throws SQLException, Exception;
    Result<String> queryService(int offset, int limit, String filters, String globalFilter, String sorting, String hostId);
    Result<String> queryApiLabel(String hostId);

    // Service Version
    void createServiceVersion(Connection conn, Map<String, Object> event) throws SQLException, Exception;
    void updateServiceVersion(Connection conn, Map<String, Object> event) throws SQLException, Exception;
    void deleteServiceVersion(Connection conn, Map<String, Object> event) throws SQLException, Exception;
    Result<String> queryServiceVersion(String hostId, String apiId);
    Result<String> getApiVersionIdLabel(String hostId);
    Result<String> queryApiVersionLabel(String hostId, String apiId);
    void updateServiceSpec(Connection conn, Map<String, Object> event) throws SQLException, Exception;

    // Service Endpoint
    Result<String> queryServiceEndpoint(int offset, int limit, String filters, String globalFilter, String sorting, String hostId);
    Result<String> queryEndpointLabel(String hostId, String apiVersionId);
    Result<String> queryEndpointScope(String hostId, String endpointId);

    // Endpoint Rule
    void createEndpointRule(Connection conn, Map<String, Object> event) throws SQLException, Exception;
    void deleteEndpointRule(Connection conn, Map<String, Object> event) throws SQLException, Exception;
    Result<String> queryEndpointRule(String hostId, String apiId, String apiVersion, String endpoint);
    Result<String> queryServiceRule(String hostId, String apiId, String apiVersion);

    // Permissions and Filters (Aggregate Queries)
    Result<String> queryServicePermission(String hostId, String apiId, String apiVersion);
    Result<List<String>> queryServiceFilter(String hostId, String apiId, String apiVersion);
    Result<String> getServiceIdLabel(String hostId);
}
