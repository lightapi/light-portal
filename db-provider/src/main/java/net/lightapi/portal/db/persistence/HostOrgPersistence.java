package net.lightapi.portal.db.persistence;

import com.networknt.monad.Result;
import net.lightapi.portal.db.PortalPersistenceException;

import java.sql.Connection; // Added import
import java.sql.SQLException; // Added import
import java.util.Map;

public interface HostOrgPersistence {
    void createOrg(Connection conn, Map<String, Object> event) throws PortalPersistenceException;
    void updateOrg(Connection conn, Map<String, Object> event) throws PortalPersistenceException;
    void deleteOrg(Connection conn, Map<String, Object> event) throws PortalPersistenceException;
    Result<String> getOrg(int offset, int limit, String filters, String globalFilter, String sorting, boolean active);
    Result<String> getOrgByDomain(String domain);

    void createHost(Connection conn, Map<String, Object> event) throws PortalPersistenceException;
    void updateHost(Connection conn, Map<String, Object> event) throws PortalPersistenceException;
    void deleteHost(Connection conn, Map<String, Object> event) throws PortalPersistenceException;
    void switchUserHost(Connection conn, Map<String, Object> event) throws PortalPersistenceException; // Treated as an update-like operation
    void createUserHost(Connection conn, Map<String, Object> event) throws PortalPersistenceException;
    void deleteUserHost(Connection conn, Map<String, Object> event) throws PortalPersistenceException;

    Result<String> queryHostDomainById(String hostId);
    Result<String> queryHostById(String id);
    Result<Map<String, Object>> queryHostByOwner(String owner);
    Result<String> getHost(int offset, int limit, String filters, String globalFilter, String sorting, boolean active);
    Result<String> getUserHost(int offset, int limit, String filters, String globalFilter, String sorting, boolean active);
    Result<String> getHostByDomain(String domain, String subDomain, String hostDesc);
    Result<String> getHostLabel();
    String getHostId(String domain, String subDomain);
}
