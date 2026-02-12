package net.lightapi.portal.db.persistence;

import com.networknt.monad.Result;
import net.lightapi.portal.db.PortalPersistenceException;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;

public interface ApiServicePersistence {
    // Service
    void createApi(Connection conn, Map<String, Object> event) throws PortalPersistenceException;
    void updateApi(Connection conn, Map<String, Object> event) throws PortalPersistenceException;
    void deleteApi(Connection conn, Map<String, Object> event) throws PortalPersistenceException;
    Result<String> queryApi(int offset, int limit, String filters, String globalFilter, String sorting, boolean active, String hostId);
    Result<String> queryApiLabel(String hostId);
    Result<String> getApiById(String hostId, String apiId);

    // Service Version
    void createApiVersion(Connection conn, Map<String, Object> event) throws PortalPersistenceException;
    void updateApiVersion(Connection conn, Map<String, Object> event) throws PortalPersistenceException;
    void deleteApiVersion(Connection conn, Map<String, Object> event) throws PortalPersistenceException;
    Result<String> getApiVersionById(String hostId, String apiVersionId);
    String queryApiVersionId(String hostId, String apiId, String apiVersion);
    Result<String> queryApiVersion(String hostId, String apiId);
    Result<String> getApiVersionIdLabel(String hostId);
    Result<String> queryApiVersionLabel(String hostId, String apiId);
    void updateApiVersionSpec(Connection conn, Map<String, Object> event) throws PortalPersistenceException;
    Map<String, Object> getEndpointIdMap(String hostId, String apiVersionId);

    // Service Endpoint
    Result<String> queryApiEndpoint(int offset, int limit, String filters, String globalFilter, String sorting, boolean active, String hostId);
    Result<String> queryEndpointLabel(String hostId, String apiVersionId);
    Result<String> queryApiEndpointScope(String hostId, String endpointId);

    // Endpoint Rule
    void createApiEndpointRule(Connection conn, Map<String, Object> event) throws PortalPersistenceException;
    void deleteApiEndpointRule(Connection conn, Map<String, Object> event) throws PortalPersistenceException;
    Result<String> queryApiEndpointRule(String hostId, String endpointId);
    Result<String> queryServiceRule(String hostId, String apiId, String apiVersion);

    // Permissions and Filters (Aggregate Queries)
    Result<String> queryApiPermission(String hostId, String apiId, String apiVersion);
    Result<List<String>> queryApiFilter(String hostId, String apiId, String apiVersion);
    Result<String> getServiceIdLabel(String hostId);
}
