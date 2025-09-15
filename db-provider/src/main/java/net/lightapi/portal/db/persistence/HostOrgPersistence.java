package net.lightapi.portal.db.persistence;

import com.networknt.monad.Result;
import java.sql.Connection; // Added import
import java.sql.SQLException; // Added import
import java.util.Map;

public interface HostOrgPersistence {
    void createOrg(Connection conn, Map<String, Object> event) throws SQLException, Exception;
    void updateOrg(Connection conn, Map<String, Object> event) throws SQLException, Exception;
    void deleteOrg(Connection conn, Map<String, Object> event) throws SQLException, Exception;
    Result<String> getOrg(int offset, int limit, String domain, String orgName, String orgDesc, String orgOwner);

    void createHost(Connection conn, Map<String, Object> event) throws SQLException, Exception;
    void updateHost(Connection conn, Map<String, Object> event) throws SQLException, Exception;
    void deleteHost(Connection conn, Map<String, Object> event) throws SQLException, Exception;
    void switchHost(Connection conn, Map<String, Object> event) throws SQLException, Exception; // Treated as an update-like operation
    void createUserHost(Connection conn, Map<String, Object> event) throws SQLException, Exception;

    Result<String> queryHostDomainById(String hostId);
    Result<String> queryHostById(String id);
    Result<Map<String, Object>> queryHostByOwner(String owner);
    Result<String> getHost(int offset, int limit, String hostId, String domain, String subDomain, String hostDesc, String hostOwner);
    Result<String> getUserHost(int offset, int limit, String hostId, String domain, String subDomain, String userId, String email, String firstName, String lastName, Boolean current);
    Result<String> getHostByDomain(String domain, String subDomain, String hostDesc);
    Result<String> getHostLabel();
}
