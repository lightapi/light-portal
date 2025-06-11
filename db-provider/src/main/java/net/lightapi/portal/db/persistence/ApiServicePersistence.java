package net.lightapi.portal.db.persistence;

import com.networknt.monad.Result;
import java.util.List;
import java.util.Map;

public interface ApiServicePersistence {
    Result<String> createService(Map<String, Object> event);
    Result<String> updateService(Map<String, Object> event);
    Result<String> deleteService(Map<String, Object> event);
    Result<String> queryService(int offset, int limit, String hostId, String apiId, String apiName, String apiDesc, String operationOwner, String deliveryOwner, String region, String businessGroup, String lob, String platform, String capability, String gitRepo, String apiTags, String apiStatus);
    Result<String> queryApiLabel(String hostId);

    Result<String> createServiceVersion(Map<String, Object> event, List<Map<String, Object>> endpoints);
    Result<String> updateServiceVersion(Map<String, Object> event, List<Map<String, Object>> endpoints);
    Result<String> deleteServiceVersion(Map<String, Object> event);
    Result<String> queryServiceVersion(String hostId, String apiId);
    Result<String> getApiVersionIdLabel(String hostId);
    Result<String> queryApiVersionLabel(String hostId, String apiId);
    Result<String> updateServiceSpec(Map<String, Object> event, List<Map<String, Object>> endpoints);

    Result<String> queryServiceEndpoint(int offset, int limit, String hostId, String apiVersionId, String apiId, String apiVersion, String endpoint, String method, String path, String desc);
    Result<String> queryEndpointLabel(String hostId, String apiId, String apiVersion); // This seems to take apiId, not apiVersionId
    Result<String> queryEndpointScope(String hostId, String endpointId);

    Result<String> createEndpointRule(Map<String, Object> event);
    Result<String> deleteEndpointRule(Map<String, Object> event);
    Result<String> queryEndpointRule(String hostId, String apiId, String apiVersion, String endpoint);
    Result<String> queryServiceRule(String hostId, String apiId, String apiVersion);

    Result<String> queryServicePermission(String hostId, String apiId, String apiVersion);
    Result<List<String>> queryServiceFilter(String hostId, String apiId, String apiVersion);
    Result<String> getServiceIdLabel(String hostId);
}
