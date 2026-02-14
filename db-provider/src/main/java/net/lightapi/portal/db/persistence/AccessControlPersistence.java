package net.lightapi.portal.db.persistence;

import com.networknt.monad.Result;
import net.lightapi.portal.db.PortalPersistenceException;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Map;

/**
 * Interface for access control persistence.
 */
public interface AccessControlPersistence {
    // Role
    /**
     * Create role
     * @param conn Connection
     * @param event Map
     * @throws PortalPersistenceException PortalPersistenceException
     */
    void createRole(Connection conn, Map<String, Object> event) throws PortalPersistenceException;

    /**
     * Update role
     * @param conn Connection
     * @param event Map
     * @throws PortalPersistenceException PortalPersistenceException
     */
    void updateRole(Connection conn, Map<String, Object> event) throws PortalPersistenceException;

    /**
     * Delete role
     * @param conn Connection
     * @param event Map
     * @throws PortalPersistenceException PortalPersistenceException
     */
    void deleteRole(Connection conn, Map<String, Object> event) throws PortalPersistenceException;

    /**
     * Query role
     * @param offset int
     * @param limit int
     * @param filters String
     * @param globalFilter String
     * @param sorting String
     * @param active boolean
     * @param hostId String
     * @return Result
     */
    Result<String> queryRole(int offset, int limit, String filters, String globalFilter, String sorting, boolean active, String hostId);

    /**
     * Query role label
     * @param hostId String
     * @return Result
     */
    Result<String> queryRoleLabel(String hostId);

    /**
     * Get role by id
     * @param hostId String
     * @param roleId String
     * @return Result
     */
    Result<String> getRoleById(String hostId, String roleId);

    // RolePermission
    /**
     * Create role permission
     * @param conn Connection
     * @param event Map
     * @throws PortalPersistenceException PortalPersistenceException
     */
    void createRolePermission(Connection conn, Map<String, Object> event) throws PortalPersistenceException;

    /**
     * Delete role permission
     * @param conn Connection
     * @param event Map
     * @throws PortalPersistenceException PortalPersistenceException
     */
    void deleteRolePermission(Connection conn, Map<String, Object> event) throws PortalPersistenceException;

    /**
     * Query role permission
     * @param offset int
     * @param limit int
     * @param filters String
     * @param globalFilter String
     * @param sorting String
     * @param active boolean
     * @param hostId String
     * @return Result
     */
    Result<String> queryRolePermission(int offset, int limit, String filters, String globalFilter, String sorting, boolean active, String hostId);

    // RoleUser
    /**
     * Create role user
     * @param conn Connection
     * @param event Map
     * @throws PortalPersistenceException PortalPersistenceException
     */
    void createRoleUser(Connection conn, Map<String, Object> event) throws PortalPersistenceException;

    /**
     * Update role user
     * @param conn Connection
     * @param event Map
     * @throws PortalPersistenceException PortalPersistenceException
     */
    void updateRoleUser(Connection conn, Map<String, Object> event) throws PortalPersistenceException;

    /**
     * Delete role user
     * @param conn Connection
     * @param event Map
     * @throws PortalPersistenceException PortalPersistenceException
     */
    void deleteRoleUser(Connection conn, Map<String, Object> event) throws PortalPersistenceException;

    /**
     * Query role user
     * @param offset int
     * @param limit int
     * @param filters String
     * @param globalFilter String
     * @param sorting String
     * @param active boolean
     * @param hostId String
     * @return Result
     */
    Result<String> queryRoleUser(int offset, int limit, String filters, String globalFilter, String sorting, boolean active, String hostId);

    /**
     * Get role user by id
     * @param hostId String
     * @param roleId String
     * @param userId String
     * @return Result
     */
    Result<String> getRoleUserById(String hostId, String roleId, String userId);

    // RoleRowFilter
    /**
     * Create role row filter
     * @param conn Connection
     * @param event Map
     * @throws PortalPersistenceException PortalPersistenceException
     */
    void createRoleRowFilter(Connection conn, Map<String, Object> event) throws PortalPersistenceException;

    /**
     * Update role row filter
     * @param conn Connection
     * @param event Map
     * @throws PortalPersistenceException PortalPersistenceException
     */
    void updateRoleRowFilter(Connection conn, Map<String, Object> event) throws PortalPersistenceException;

    /**
     * Delete role row filter
     * @param conn Connection
     * @param event Map
     * @throws PortalPersistenceException PortalPersistenceException
     */
    void deleteRoleRowFilter(Connection conn, Map<String, Object> event) throws PortalPersistenceException;

    /**
     * Query role row filter
     * @param offset int
     * @param limit int
     * @param filters String
     * @param globalFilter String
     * @param sorting String
     * @param active boolean
     * @param hostId String
     * @return Result
     */
    Result<String> queryRoleRowFilter(int offset, int limit, String filters, String globalFilter, String sorting, boolean active, String hostId);

    /**
     * Get role row filter by id
     * @param hostId String
     * @param roleId String
     * @param endpointId String
     * @param colName String
     * @return Result
     */
    Result<String> getRoleRowFilterById(String hostId, String roleId, String endpointId, String colName);

    // RoleColFilter
    /**
     * Create role col filter
     * @param conn Connection
     * @param event Map
     * @throws PortalPersistenceException PortalPersistenceException
     */
    void createRoleColFilter(Connection conn, Map<String, Object> event) throws PortalPersistenceException;

    /**
     * Update role col filter
     * @param conn Connection
     * @param event Map
     * @throws PortalPersistenceException PortalPersistenceException
     */
    void updateRoleColFilter(Connection conn, Map<String, Object> event) throws PortalPersistenceException;

    /**
     * Delete role col filter
     * @param conn Connection
     * @param event Map
     * @throws PortalPersistenceException PortalPersistenceException
     */
    void deleteRoleColFilter(Connection conn, Map<String, Object> event) throws PortalPersistenceException;

    /**
     * Query role col filter
     * @param offset int
     * @param limit int
     * @param filters String
     * @param globalFilter String
     * @param sorting String
     * @param active boolean
     * @param hostId String
     * @return Result
     */
    Result<String> queryRoleColFilter(int offset, int limit, String filters, String globalFilter, String sorting, boolean active, String hostId);

    /**
     * Get role col filter by id
     * @param hostId String
     * @param roleId String
     * @param endpointId String
     * @return Result
     */
    Result<String> getRoleColFilterById(String hostId, String roleId, String endpointId);

    // Group
    /**
     * Create group
     * @param conn Connection
     * @param event Map
     * @throws PortalPersistenceException PortalPersistenceException
     */
    void createGroup(Connection conn, Map<String, Object> event) throws PortalPersistenceException;

    /**
     * Update group
     * @param conn Connection
     * @param event Map
     * @throws PortalPersistenceException PortalPersistenceException
     */
    void updateGroup(Connection conn, Map<String, Object> event) throws PortalPersistenceException;

    /**
     * Delete group
     * @param conn Connection
     * @param event Map
     * @throws PortalPersistenceException PortalPersistenceException
     */
    void deleteGroup(Connection conn, Map<String, Object> event) throws PortalPersistenceException;

    /**
     * Query group
     * @param offset int
     * @param limit int
     * @param filters String
     * @param globalFilter String
     * @param sorting String
     * @param active boolean
     * @param hostId String
     * @return Result
     */
    Result<String> queryGroup(int offset, int limit, String filters, String globalFilter, String sorting, boolean active, String hostId);

    /**
     * Query group label
     * @param hostId String
     * @return Result
     */
    Result<String> queryGroupLabel(String hostId);

    /**
     * Get group by id
     * @param hostId String
     * @param groupId String
     * @return Result
     */
    Result<String> getGroupById(String hostId, String groupId);

    // GroupPermission
    /**
     * Create group permission
     * @param conn Connection
     * @param event Map
     * @throws PortalPersistenceException PortalPersistenceException
     */
    void createGroupPermission(Connection conn, Map<String, Object> event) throws PortalPersistenceException;

    /**
     * Delete group permission
     * @param conn Connection
     * @param event Map
     * @throws PortalPersistenceException PortalPersistenceException
     */
    void deleteGroupPermission(Connection conn, Map<String, Object> event) throws PortalPersistenceException;

    /**
     * Query group permission
     * @param offset int
     * @param limit int
     * @param filters String
     * @param globalFilter String
     * @param sorting String
     * @param active boolean
     * @param hostId String
     * @return Result
     */
    Result<String> queryGroupPermission(int offset, int limit, String filters, String globalFilter, String sorting, boolean active, String hostId);

    // GroupUser
    /**
     * Create group user
     * @param conn Connection
     * @param event Map
     * @throws PortalPersistenceException PortalPersistenceException
     */
    void createGroupUser(Connection conn, Map<String, Object> event) throws PortalPersistenceException;

    /**
     * Update group user
     * @param conn Connection
     * @param event Map
     * @throws PortalPersistenceException PortalPersistenceException
     */
    void updateGroupUser(Connection conn, Map<String, Object> event) throws PortalPersistenceException;

    /**
     * Delete group user
     * @param conn Connection
     * @param event Map
     * @throws PortalPersistenceException PortalPersistenceException
     */
    void deleteGroupUser(Connection conn, Map<String, Object> event) throws PortalPersistenceException;

    /**
     * Query group user
     * @param offset int
     * @param limit int
     * @param filters String
     * @param globalFilter String
     * @param sorting String
     * @param active boolean
     * @param hostId String
     * @return Result
     */
    Result<String> queryGroupUser(int offset, int limit, String filters, String globalFilter, String sorting, boolean active, String hostId);

    /**
     * Get group user by id
     * @param hostId String
     * @param groupId String
     * @param userId String
     * @return Result
     */
    Result<String> getGroupUserById(String hostId, String groupId, String userId);

    // GroupRowFilter
    /**
     * Create group row filter
     * @param conn Connection
     * @param event Map
     * @throws PortalPersistenceException PortalPersistenceException
     */
    void createGroupRowFilter(Connection conn, Map<String, Object> event) throws PortalPersistenceException;

    /**
     * Update group row filter
     * @param conn Connection
     * @param event Map
     * @throws PortalPersistenceException PortalPersistenceException
     */
    void updateGroupRowFilter(Connection conn, Map<String, Object> event) throws PortalPersistenceException;

    /**
     * Delete group row filter
     * @param conn Connection
     * @param event Map
     * @throws PortalPersistenceException PortalPersistenceException
     */
    void deleteGroupRowFilter(Connection conn, Map<String, Object> event) throws PortalPersistenceException;

    /**
     * Query group row filter
     * @param offset int
     * @param limit int
     * @param filters String
     * @param globalFilter String
     * @param sorting String
     * @param active boolean
     * @param hostId String
     * @return Result
     */
    Result<String> queryGroupRowFilter(int offset, int limit, String filters, String globalFilter, String sorting, boolean active, String hostId);

    /**
     * Get group row filter by id
     * @param hostId String
     * @param groupId String
     * @param endpointId String
     * @param colName String
     * @return Result
     */
    Result<String> getGroupRowFilterById(String hostId, String groupId, String endpointId, String colName);

    // GroupColFilter
    /**
     * Create group col filter
     * @param conn Connection
     * @param event Map
     * @throws PortalPersistenceException PortalPersistenceException
     */
    void createGroupColFilter(Connection conn, Map<String, Object> event) throws PortalPersistenceException;

    /**
     * Update group col filter
     * @param conn Connection
     * @param event Map
     * @throws PortalPersistenceException PortalPersistenceException
     */
    void updateGroupColFilter(Connection conn, Map<String, Object> event) throws PortalPersistenceException;

    /**
     * Delete group col filter
     * @param conn Connection
     * @param event Map
     * @throws PortalPersistenceException PortalPersistenceException
     */
    void deleteGroupColFilter(Connection conn, Map<String, Object> event) throws PortalPersistenceException;

    /**
     * Query group col filter
     * @param offset int
     * @param limit int
     * @param filters String
     * @param globalFilter String
     * @param sorting String
     * @param active boolean
     * @param hostId String
     * @return Result
     */
    Result<String> queryGroupColFilter(int offset, int limit, String filters, String globalFilter, String sorting, boolean active, String hostId);

    /**
     * Get group col filter by id
     * @param hostId String
     * @param groupId String
     * @param endpointId String
     * @return Result
     */
    Result<String> getGroupColFilterById(String hostId, String groupId, String endpointId);

    // Position
    /**
     * Create position
     * @param conn Connection
     * @param event Map
     * @throws PortalPersistenceException PortalPersistenceException
     */
    void createPosition(Connection conn, Map<String, Object> event) throws PortalPersistenceException;

    /**
     * Update position
     * @param conn Connection
     * @param event Map
     * @throws PortalPersistenceException PortalPersistenceException
     */
    void updatePosition(Connection conn, Map<String, Object> event) throws PortalPersistenceException;

    /**
     * Delete position
     * @param conn Connection
     * @param event Map
     * @throws PortalPersistenceException PortalPersistenceException
     */
    void deletePosition(Connection conn, Map<String, Object> event) throws PortalPersistenceException;

    /**
     * Query position
     * @param offset int
     * @param limit int
     * @param filters String
     * @param globalFilter String
     * @param sorting String
     * @param active boolean
     * @param hostId String
     * @return Result
     */
    Result<String> queryPosition(int offset, int limit, String filters, String globalFilter, String sorting, boolean active, String hostId);

    /**
     * Query position label
     * @param hostId String
     * @return Result
     */
    Result<String> queryPositionLabel(String hostId);

    /**
     * Get position by id
     * @param hostId String
     * @param positionId String
     * @return Result
     */
    Result<String> getPositionById(String hostId, String positionId);

    // PositionPermission
    /**
     * Create position permission
     * @param conn Connection
     * @param event Map
     * @throws PortalPersistenceException PortalPersistenceException
     */
    void createPositionPermission(Connection conn, Map<String, Object> event) throws PortalPersistenceException;

    /**
     * Delete position permission
     * @param conn Connection
     * @param event Map
     * @throws PortalPersistenceException PortalPersistenceException
     */
    void deletePositionPermission(Connection conn, Map<String, Object> event) throws PortalPersistenceException;

    /**
     * Query position permission
     * @param offset int
     * @param limit int
     * @param filters String
     * @param globalFilter String
     * @param sorting String
     * @param active boolean
     * @param hostId String
     * @return Result
     */
    Result<String> queryPositionPermission(int offset, int limit, String filters, String globalFilter, String sorting, boolean active, String hostId);

    // PositionUser
    /**
     * Create position user
     * @param conn Connection
     * @param event Map
     * @throws PortalPersistenceException PortalPersistenceException
     */
    void createPositionUser(Connection conn, Map<String, Object> event) throws PortalPersistenceException;

    /**
     * Update position user
     * @param conn Connection
     * @param event Map
     * @throws PortalPersistenceException PortalPersistenceException
     */
    void updatePositionUser(Connection conn, Map<String, Object> event) throws PortalPersistenceException;

    /**
     * Delete position user
     * @param conn Connection
     * @param event Map
     * @throws PortalPersistenceException PortalPersistenceException
     */
    void deletePositionUser(Connection conn, Map<String, Object> event) throws PortalPersistenceException;

    /**
     * Query position user
     * @param offset int
     * @param limit int
     * @param filters String
     * @param globalFilter String
     * @param sorting String
     * @param active boolean
     * @param hostId String
     * @return Result
     */
    Result<String> queryPositionUser(int offset, int limit, String filters, String globalFilter, String sorting, boolean active, String hostId);

    /**
     * Get position user by id
     * @param hostId String
     * @param positionId String
     * @param employeeId String
     * @return Result
     */
    Result<String> getPositionUserById(String hostId, String positionId, String employeeId);

    // PositionRowFilter
    /**
     * Create position row filter
     * @param conn Connection
     * @param event Map
     * @throws PortalPersistenceException PortalPersistenceException
     */
    void createPositionRowFilter(Connection conn, Map<String, Object> event) throws PortalPersistenceException;

    /**
     * Update position row filter
     * @param conn Connection
     * @param event Map
     * @throws PortalPersistenceException PortalPersistenceException
     */
    void updatePositionRowFilter(Connection conn, Map<String, Object> event) throws PortalPersistenceException;

    /**
     * Delete position row filter
     * @param conn Connection
     * @param event Map
     * @throws PortalPersistenceException PortalPersistenceException
     */
    void deletePositionRowFilter(Connection conn, Map<String, Object> event) throws PortalPersistenceException;

    /**
     * Query position row filter
     * @param offset int
     * @param limit int
     * @param filters String
     * @param globalFilter String
     * @param sorting String
     * @param active boolean
     * @param hostId String
     * @return Result
     */
    Result<String> queryPositionRowFilter(int offset, int limit, String filters, String globalFilter, String sorting, boolean active, String hostId);

    /**
     * Get position row filter by id
     * @param hostId String
     * @param positionId String
     * @param endpointId String
     * @param colName String
     * @return Result
     */
    Result<String> getPositionRowFilterById(String hostId, String positionId, String endpointId, String colName);

    // PositionColFilter
    /**
     * Create position col filter
     * @param conn Connection
     * @param event Map
     * @throws PortalPersistenceException PortalPersistenceException
     */
    void createPositionColFilter(Connection conn, Map<String, Object> event) throws PortalPersistenceException;

    /**
     * Update position col filter
     * @param conn Connection
     * @param event Map
     * @throws PortalPersistenceException PortalPersistenceException
     */
    void updatePositionColFilter(Connection conn, Map<String, Object> event) throws PortalPersistenceException;

    /**
     * Delete position col filter
     * @param conn Connection
     * @param event Map
     * @throws PortalPersistenceException PortalPersistenceException
     */
    void deletePositionColFilter(Connection conn, Map<String, Object> event) throws PortalPersistenceException;

    /**
     * Query position col filter
     * @param offset int
     * @param limit int
     * @param filters String
     * @param globalFilter String
     * @param sorting String
     * @param active boolean
     * @param hostId String
     * @return Result
     */
    Result<String> queryPositionColFilter(int offset, int limit, String filters, String globalFilter, String sorting, boolean active, String hostId);

    /**
     * Get position col filter by id
     * @param hostId String
     * @param positionId String
     * @param endpointId String
     * @return Result
     */
    Result<String> getPositionColFilterById(String hostId, String positionId, String endpointId);

    // Attribute
    /**
     * Create attribute
     * @param conn Connection
     * @param event Map
     * @throws PortalPersistenceException PortalPersistenceException
     */
    void createAttribute(Connection conn, Map<String, Object> event) throws PortalPersistenceException;

    /**
     * Update attribute
     * @param conn Connection
     * @param event Map
     * @throws PortalPersistenceException PortalPersistenceException
     */
    void updateAttribute(Connection conn, Map<String, Object> event) throws PortalPersistenceException;

    /**
     * Delete attribute
     * @param conn Connection
     * @param event Map
     * @throws PortalPersistenceException PortalPersistenceException
     */
    void deleteAttribute(Connection conn, Map<String, Object> event) throws PortalPersistenceException;

    /**
     * Query attribute
     * @param offset int
     * @param limit int
     * @param filters String
     * @param globalFilter String
     * @param sorting String
     * @param active boolean
     * @param hostId String
     * @return Result
     */
    Result<String> queryAttribute(int offset, int limit, String filters, String globalFilter, String sorting, boolean active, String hostId);

    /**
     * Query attribute label
     * @param hostId String
     * @return Result
     */
    Result<String> queryAttributeLabel(String hostId);

    /**
     * Get attribute by id
     * @param hostId String
     * @param attributeId String
     * @return Result
     */
    Result<String> getAttributeById(String hostId, String attributeId);

    // AttributePermission
    /**
     * Create attribute permission
     * @param conn Connection
     * @param event Map
     * @throws PortalPersistenceException PortalPersistenceException
     */
    void createAttributePermission(Connection conn, Map<String, Object> event) throws PortalPersistenceException;

    /**
     * Update attribute permission
     * @param conn Connection
     * @param event Map
     * @throws PortalPersistenceException PortalPersistenceException
     */
    void updateAttributePermission(Connection conn, Map<String, Object> event) throws PortalPersistenceException;

    /**
     * Delete attribute permission
     * @param conn Connection
     * @param event Map
     * @throws PortalPersistenceException PortalPersistenceException
     */
    void deleteAttributePermission(Connection conn, Map<String, Object> event) throws PortalPersistenceException;

    /**
     * Query attribute permission
     * @param offset int
     * @param limit int
     * @param filters String
     * @param globalFilter String
     * @param sorting String
     * @param active boolean
     * @param hostId String
     * @return Result
     */
    Result<String> queryAttributePermission(int offset, int limit, String filters, String globalFilter, String sorting, boolean active, String hostId);

    /**
     * Get attribute permission by id
     * @param hostId String
     * @param attributeId String
     * @param endpointId String
     * @return Result
     */
    Result<String> getAttributePermissionById(String hostId, String attributeId, String endpointId);

    // AttributeUser
    /**
     * Create attribute user
     * @param conn Connection
     * @param event Map
     * @throws PortalPersistenceException PortalPersistenceException
     */
    void createAttributeUser(Connection conn, Map<String, Object> event) throws PortalPersistenceException;

    /**
     * Update attribute user
     * @param conn Connection
     * @param event Map
     * @throws PortalPersistenceException PortalPersistenceException
     */
    void updateAttributeUser(Connection conn, Map<String, Object> event) throws PortalPersistenceException;

    /**
     * Delete attribute user
     * @param conn Connection
     * @param event Map
     * @throws PortalPersistenceException PortalPersistenceException
     */
    void deleteAttributeUser(Connection conn, Map<String, Object> event) throws PortalPersistenceException;

    /**
     * Query attribute user
     * @param offset int
     * @param limit int
     * @param filters String
     * @param globalFilter String
     * @param sorting String
     * @param active boolean
     * @param hostId String
     * @return Result
     */
    Result<String> queryAttributeUser(int offset, int limit, String filters, String globalFilter, String sorting, boolean active, String hostId);

    /**
     * Get attribute user by id
     * @param hostId String
     * @param attributeId String
     * @param userId String
     * @return Result
     */
    Result<String> getAttributeUserById(String hostId, String attributeId, String userId);

    // AttributeRowFilter
    /**
     * Create attribute row filter
     * @param conn Connection
     * @param event Map
     * @throws PortalPersistenceException PortalPersistenceException
     */
    void createAttributeRowFilter(Connection conn, Map<String, Object> event) throws PortalPersistenceException;

    /**
     * Update attribute row filter
     * @param conn Connection
     * @param event Map
     * @throws PortalPersistenceException PortalPersistenceException
     */
    void updateAttributeRowFilter(Connection conn, Map<String, Object> event) throws PortalPersistenceException;

    /**
     * Delete attribute row filter
     * @param conn Connection
     * @param event Map
     * @throws PortalPersistenceException PortalPersistenceException
     */
    void deleteAttributeRowFilter(Connection conn, Map<String, Object> event) throws PortalPersistenceException;

    /**
     * Query attribute row filter
     * @param offset int
     * @param limit int
     * @param filters String
     * @param globalFilter String
     * @param sorting String
     * @param active boolean
     * @param hostId String
     * @return Result
     */
    Result<String> queryAttributeRowFilter(int offset, int limit, String filters, String globalFilter, String sorting, boolean active, String hostId);

    /**
     * Get attribute row filter by id
     * @param hostId String
     * @param attributeId String
     * @param endpointId String
     * @param colName String
     * @return Result
     */
    Result<String> getAttributeRowFilterById(String hostId, String attributeId, String endpointId, String colName);

    // AttributeColFilter
    /**
     * Create attribute col filter
     * @param conn Connection
     * @param event Map
     * @throws PortalPersistenceException PortalPersistenceException
     */
    void createAttributeColFilter(Connection conn, Map<String, Object> event) throws PortalPersistenceException;

    /**
     * Update attribute col filter
     * @param conn Connection
     * @param event Map
     * @throws PortalPersistenceException PortalPersistenceException
     */
    void updateAttributeColFilter(Connection conn, Map<String, Object> event) throws PortalPersistenceException;

    /**
     * Delete attribute col filter
     * @param conn Connection
     * @param event Map
     * @throws PortalPersistenceException PortalPersistenceException
     */
    void deleteAttributeColFilter(Connection conn, Map<String, Object> event) throws PortalPersistenceException;

    /**
     * Query attribute col filter
     * @param offset int
     * @param limit int
     * @param filters String
     * @param globalFilter String
     * @param sorting String
     * @param active boolean
     * @param hostId String
     * @return Result
     */
    Result<String> queryAttributeColFilter(int offset, int limit, String filters, String globalFilter, String sorting, boolean active, String hostId);

    /**
     * Get attribute col filter by id
     * @param hostId String
     * @param attributeId String
     * @param endpointId String
     * @return Result
     */
    Result<String> getAttributeColFilterById(String hostId, String attributeId, String endpointId);
}

