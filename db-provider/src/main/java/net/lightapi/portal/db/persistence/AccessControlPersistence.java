package net.lightapi.portal.db.persistence;

import com.networknt.monad.Result;
import net.lightapi.portal.db.PortalPersistenceException;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Map;

public interface AccessControlPersistence {
    // Role
    void createRole(Connection conn, Map<String, Object> event) throws PortalPersistenceException;
    void updateRole(Connection conn, Map<String, Object> event) throws PortalPersistenceException;
    void deleteRole(Connection conn, Map<String, Object> event) throws PortalPersistenceException;
    Result<String> queryRole(int offset, int limit, String filters, String globalFilter, String sorting, boolean active, String hostId);
    Result<String> queryRoleLabel(String hostId);
    Result<String> getRoleById(String hostId, String roleId);

    // RolePermission
    void createRolePermission(Connection conn, Map<String, Object> event) throws PortalPersistenceException;
    void deleteRolePermission(Connection conn, Map<String, Object> event) throws PortalPersistenceException;
    Result<String> queryRolePermission(int offset, int limit, String filters, String globalFilter, String sorting, boolean active, String hostId);

    // RoleUser
    void createRoleUser(Connection conn, Map<String, Object> event) throws PortalPersistenceException;
    void updateRoleUser(Connection conn, Map<String, Object> event) throws PortalPersistenceException;
    void deleteRoleUser(Connection conn, Map<String, Object> event) throws PortalPersistenceException;
    Result<String> queryRoleUser(int offset, int limit, String filters, String globalFilter, String sorting, boolean active, String hostId);
    Result<String> getRoleUserById(String hostId, String roleId, String userId);

    // RoleRowFilter
    void createRoleRowFilter(Connection conn, Map<String, Object> event) throws PortalPersistenceException;
    void updateRoleRowFilter(Connection conn, Map<String, Object> event) throws PortalPersistenceException;
    void deleteRoleRowFilter(Connection conn, Map<String, Object> event) throws PortalPersistenceException;
    Result<String> queryRoleRowFilter(int offset, int limit, String filters, String globalFilter, String sorting, boolean active, String hostId);
    Result<String> getRoleRowFilterById(String hostId, String roleId, String endpointId, String colName);

    // RoleColFilter
    void createRoleColFilter(Connection conn, Map<String, Object> event) throws PortalPersistenceException;
    void updateRoleColFilter(Connection conn, Map<String, Object> event) throws PortalPersistenceException;
    void deleteRoleColFilter(Connection conn, Map<String, Object> event) throws PortalPersistenceException;
    Result<String> queryRoleColFilter(int offset, int limit, String filters, String globalFilter, String sorting, boolean active, String hostId);
    Result<String> getRoleColFilterById(String hostId, String roleId, String endpointId);

    // Group
    void createGroup(Connection conn, Map<String, Object> event) throws PortalPersistenceException;
    void updateGroup(Connection conn, Map<String, Object> event) throws PortalPersistenceException;
    void deleteGroup(Connection conn, Map<String, Object> event) throws PortalPersistenceException;
    Result<String> queryGroup(int offset, int limit, String filters, String globalFilter, String sorting, boolean active, String hostId);
    Result<String> queryGroupLabel(String hostId);
    Result<String> getGroupById(String hostId, String groupId);

    // GroupPermission
    void createGroupPermission(Connection conn, Map<String, Object> event) throws PortalPersistenceException;
    void deleteGroupPermission(Connection conn, Map<String, Object> event) throws PortalPersistenceException;
    Result<String> queryGroupPermission(int offset, int limit, String filters, String globalFilter, String sorting, boolean active, String hostId);

    // GroupUser
    void createGroupUser(Connection conn, Map<String, Object> event) throws PortalPersistenceException;
    void updateGroupUser(Connection conn, Map<String, Object> event) throws PortalPersistenceException;
    void deleteGroupUser(Connection conn, Map<String, Object> event) throws PortalPersistenceException;
    Result<String> queryGroupUser(int offset, int limit, String filters, String globalFilter, String sorting, boolean active, String hostId);
    Result<String> getGroupUserById(String hostId, String groupId, String userId);

    // GroupRowFilter
    void createGroupRowFilter(Connection conn, Map<String, Object> event) throws PortalPersistenceException;
    void updateGroupRowFilter(Connection conn, Map<String, Object> event) throws PortalPersistenceException;
    void deleteGroupRowFilter(Connection conn, Map<String, Object> event) throws PortalPersistenceException;
    Result<String> queryGroupRowFilter(int offset, int limit, String filters, String globalFilter, String sorting, boolean active, String hostId);
    Result<String> getGroupRowFilterById(String hostId, String groupId, String endpointId, String colName);

    // GroupColFilter
    void createGroupColFilter(Connection conn, Map<String, Object> event) throws PortalPersistenceException;
    void updateGroupColFilter(Connection conn, Map<String, Object> event) throws PortalPersistenceException;
    void deleteGroupColFilter(Connection conn, Map<String, Object> event) throws PortalPersistenceException;
    Result<String> queryGroupColFilter(int offset, int limit, String filters, String globalFilter, String sorting, boolean active, String hostId);
    Result<String> getGroupColFilterById(String hostId, String groupId, String endpointId);

    // Position
    void createPosition(Connection conn, Map<String, Object> event) throws PortalPersistenceException;
    void updatePosition(Connection conn, Map<String, Object> event) throws PortalPersistenceException;
    void deletePosition(Connection conn, Map<String, Object> event) throws PortalPersistenceException;
    Result<String> queryPosition(int offset, int limit, String filters, String globalFilter, String sorting, boolean active, String hostId);
    Result<String> queryPositionLabel(String hostId);
    Result<String> getPositionById(String hostId, String positionId);

    // PositionPermission
    void createPositionPermission(Connection conn, Map<String, Object> event) throws PortalPersistenceException;
    void deletePositionPermission(Connection conn, Map<String, Object> event) throws PortalPersistenceException;
    Result<String> queryPositionPermission(int offset, int limit, String filters, String globalFilter, String sorting, boolean active, String hostId);

    // PositionUser
    void createPositionUser(Connection conn, Map<String, Object> event) throws PortalPersistenceException;
    void updatePositionUser(Connection conn, Map<String, Object> event) throws PortalPersistenceException;
    void deletePositionUser(Connection conn, Map<String, Object> event) throws PortalPersistenceException;
    Result<String> queryPositionUser(int offset, int limit, String filters, String globalFilter, String sorting, boolean active, String hostId);
    Result<String> getPositionUserById(String hostId, String positionId, String employeeId);

    // PositionRowFilter
    void createPositionRowFilter(Connection conn, Map<String, Object> event) throws PortalPersistenceException;
    void updatePositionRowFilter(Connection conn, Map<String, Object> event) throws PortalPersistenceException;
    void deletePositionRowFilter(Connection conn, Map<String, Object> event) throws PortalPersistenceException;
    Result<String> queryPositionRowFilter(int offset, int limit, String filters, String globalFilter, String sorting, boolean active, String hostId);
    Result<String> getPositionRowFilterById(String hostId, String positionId, String endpointId, String colName);

    // PositionColFilter
    void createPositionColFilter(Connection conn, Map<String, Object> event) throws PortalPersistenceException;
    void updatePositionColFilter(Connection conn, Map<String, Object> event) throws PortalPersistenceException;
    void deletePositionColFilter(Connection conn, Map<String, Object> event) throws PortalPersistenceException;
    Result<String> queryPositionColFilter(int offset, int limit, String filters, String globalFilter, String sorting, boolean active, String hostId);
    Result<String> getPositionColFilterById(String hostId, String positionId, String endpointId);

    // Attribute
    void createAttribute(Connection conn, Map<String, Object> event) throws PortalPersistenceException;
    void updateAttribute(Connection conn, Map<String, Object> event) throws PortalPersistenceException;
    void deleteAttribute(Connection conn, Map<String, Object> event) throws PortalPersistenceException;
    Result<String> queryAttribute(int offset, int limit, String filters, String globalFilter, String sorting, boolean active, String hostId);
    Result<String> queryAttributeLabel(String hostId);
    Result<String> getAttributeById(String hostId, String attributeId);

    // AttributePermission
    void createAttributePermission(Connection conn, Map<String, Object> event) throws PortalPersistenceException;
    void updateAttributePermission(Connection conn, Map<String, Object> event) throws PortalPersistenceException;
    void deleteAttributePermission(Connection conn, Map<String, Object> event) throws PortalPersistenceException;
    Result<String> queryAttributePermission(int offset, int limit, String filters, String globalFilter, String sorting, boolean active, String hostId);
    Result<String> getAttributePermissionById(String hostId, String attributeId, String endpointId);

    // AttributeUser
    void createAttributeUser(Connection conn, Map<String, Object> event) throws PortalPersistenceException;
    void updateAttributeUser(Connection conn, Map<String, Object> event) throws PortalPersistenceException;
    void deleteAttributeUser(Connection conn, Map<String, Object> event) throws PortalPersistenceException;
    Result<String> queryAttributeUser(int offset, int limit, String filters, String globalFilter, String sorting, boolean active, String hostId);
    Result<String> getAttributeUserById(String hostId, String attributeId, String userId);

    // AttributeRowFilter
    void createAttributeRowFilter(Connection conn, Map<String, Object> event) throws PortalPersistenceException;
    void updateAttributeRowFilter(Connection conn, Map<String, Object> event) throws PortalPersistenceException;
    void deleteAttributeRowFilter(Connection conn, Map<String, Object> event) throws PortalPersistenceException;
    Result<String> queryAttributeRowFilter(int offset, int limit, String filters, String globalFilter, String sorting, boolean active, String hostId);
    Result<String> getAttributeRowFilterById(String hostId, String attributeId, String endpointId, String colName);

    // AttributeColFilter
    void createAttributeColFilter(Connection conn, Map<String, Object> event) throws PortalPersistenceException;
    void updateAttributeColFilter(Connection conn, Map<String, Object> event) throws PortalPersistenceException;
    void deleteAttributeColFilter(Connection conn, Map<String, Object> event) throws PortalPersistenceException;
    Result<String> queryAttributeColFilter(int offset, int limit, String filters, String globalFilter, String sorting, boolean active, String hostId);
    Result<String> getAttributeColFilterById(String hostId, String attributeId, String endpointId);
}
