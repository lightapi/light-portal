package net.lightapi.portal.db.persistence;

import com.networknt.monad.Result;
import java.util.Map;

public interface HostOrgPersistence {
    Result<String> createOrg(Map<String, Object> event);
    Result<String> updateOrg(Map<String, Object> event);
    Result<String> deleteOrg(Map<String, Object> event);
    Result<String> getOrg(int offset, int limit, String domain, String orgName, String orgDesc, String orgOwner);

    Result<String> createHost(Map<String, Object> event);
    Result<String> updateHost(Map<String, Object> event);
    Result<String> deleteHost(Map<String, Object> event);
    Result<String> switchHost(Map<String, Object> event);
    Result<String> queryHostDomainById(String hostId);
    Result<String> queryHostById(String id);
    Result<Map<String, Object>> queryHostByOwner(String owner); // Note: original returns Map
    Result<String> getHost(int offset, int limit, String hostId, String domain, String subDomain, String hostDesc, String hostOwner);
    Result<String> getHostByDomain(String domain, String subDomain, String hostDesc);
    Result<String> getHostLabel();
}
