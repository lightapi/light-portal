package net.lightapi.portal.db.persistence;

import com.networknt.monad.Result;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Map;

public interface AccessControlPersistence {
    // Role
    void createRole(Connection conn, Map<String, Object> event) throws SQLException, Exception;
    void updateRole(Connection conn, Map<String, Object> event) throws SQLException, Exception;
    void deleteRole(Connection conn, Map<String, Object> event) throws SQLException, Exception;
    Result<String> queryRole(int offset, int limit, String filters, String globalFilter, String sorting, String hostId);
    Result<String> queryRoleLabel(String hostId);

    // RolePermission
    void createRolePermission(Connection conn, Map<String, Object> event) throws SQLException, Exception;
    void deleteRolePermission(Connection conn, Map<String, Object> event) throws SQLException, Exception;
    Result<String> queryRolePermission(int offset, int limit, String filters, String globalFilter, String sorting, String hostId);

    // RoleUser
    void createRoleUser(Connection conn, Map<String, Object> event) throws SQLException, Exception;
    void updateRoleUser(Connection conn, Map<String, Object> event) throws SQLException, Exception;
    void deleteRoleUser(Connection conn, Map<String, Object> event) throws SQLException, Exception;
    Result<String> queryRoleUser(int offset, int limit, String filters, String globalFilter, String sorting, String hostId);

    // RoleRowFilter
    void createRoleRowFilter(Connection conn, Map<String, Object> event) throws SQLException, Exception;
    void updateRoleRowFilter(Connection conn, Map<String, Object> event) throws SQLException, Exception;
    void deleteRoleRowFilter(Connection conn, Map<String, Object> event) throws SQLException, Exception;
    Result<String> queryRoleRowFilter(int offset, int limit, String filters, String globalFilter, String sorting, String hostId);

    // RoleColFilter
    void createRoleColFilter(Connection conn, Map<String, Object> event) throws SQLException, Exception;
    void updateRoleColFilter(Connection conn, Map<String, Object> event) throws SQLException, Exception;
    void deleteRoleColFilter(Connection conn, Map<String, Object> event) throws SQLException, Exception;
    Result<String> queryRoleColFilter(int offset, int limit, String filters, String globalFilter, String sorting, String hostId);

    // Group
    void createGroup(Connection conn, Map<String, Object> event) throws SQLException, Exception;
    void updateGroup(Connection conn, Map<String, Object> event) throws SQLException, Exception;
    void deleteGroup(Connection conn, Map<String, Object> event) throws SQLException, Exception;
    Result<String> queryGroup(int offset, int limit, String filters, String globalFilter, String sorting, String hostId);
    Result<String> queryGroupLabel(String hostId);

    // GroupPermission
    void createGroupPermission(Connection conn, Map<String, Object> event) throws SQLException, Exception;
    void deleteGroupPermission(Connection conn, Map<String, Object> event) throws SQLException, Exception;
    Result<String> queryGroupPermission(int offset, int limit, String filters, String globalFilter, String sorting, String hostId);

    // GroupUser
    void createGroupUser(Connection conn, Map<String, Object> event) throws SQLException, Exception;
    void updateGroupUser(Connection conn, Map<String, Object> event) throws SQLException, Exception;
    void deleteGroupUser(Connection conn, Map<String, Object> event) throws SQLException, Exception;
    Result<String> queryGroupUser(int offset, int limit, String filters, String globalFilter, String sorting, String hostId);

    // GroupRowFilter
    void createGroupRowFilter(Connection conn, Map<String, Object> event) throws SQLException, Exception;
    void updateGroupRowFilter(Connection conn, Map<String, Object> event) throws SQLException, Exception;
    void deleteGroupRowFilter(Connection conn, Map<String, Object> event) throws SQLException, Exception;
    Result<String> queryGroupRowFilter(int offset, int limit, String filters, String globalFilter, String sorting, String hostId);

    // GroupColFilter
    void createGroupColFilter(Connection conn, Map<String, Object> event) throws SQLException, Exception;
    void updateGroupColFilter(Connection conn, Map<String, Object> event) throws SQLException, Exception;
    void deleteGroupColFilter(Connection conn, Map<String, Object> event) throws SQLException, Exception;
    Result<String> queryGroupColFilter(int offset, int limit, String filters, String globalFilter, String sorting, String hostId);

    // Position
    void createPosition(Connection conn, Map<String, Object> event) throws SQLException, Exception;
    void updatePosition(Connection conn, Map<String, Object> event) throws SQLException, Exception;
    void deletePosition(Connection conn, Map<String, Object> event) throws SQLException, Exception;
    Result<String> queryPosition(int offset, int limit, String filters, String globalFilter, String sorting, String hostId);
    Result<String> queryPositionLabel(String hostId);

    // PositionPermission
    void createPositionPermission(Connection conn, Map<String, Object> event) throws SQLException, Exception;
    void deletePositionPermission(Connection conn, Map<String, Object> event) throws SQLException, Exception;
    Result<String> queryPositionPermission(int offset, int limit, String filters, String globalFilter, String sorting, String hostId);

    // PositionUser
    void createPositionUser(Connection conn, Map<String, Object> event) throws SQLException, Exception;
    void updatePositionUser(Connection conn, Map<String, Object> event) throws SQLException, Exception;
    void deletePositionUser(Connection conn, Map<String, Object> event) throws SQLException, Exception;
    Result<String> queryPositionUser(int offset, int limit, String filters, String globalFilter, String sorting, String hostId);

    // PositionRowFilter
    void createPositionRowFilter(Connection conn, Map<String, Object> event) throws SQLException, Exception;
    void updatePositionRowFilter(Connection conn, Map<String, Object> event) throws SQLException, Exception;
    void deletePositionRowFilter(Connection conn, Map<String, Object> event) throws SQLException, Exception;
    Result<String> queryPositionRowFilter(int offset, int limit, String filters, String globalFilter, String sorting, String hostId);

    // PositionColFilter
    void createPositionColFilter(Connection conn, Map<String, Object> event) throws SQLException, Exception;
    void updatePositionColFilter(Connection conn, Map<String, Object> event) throws SQLException, Exception;
    void deletePositionColFilter(Connection conn, Map<String, Object> event) throws SQLException, Exception;
    Result<String> queryPositionColFilter(int offset, int limit, String filters, String globalFilter, String sorting, String hostId);

    // Attribute
    void createAttribute(Connection conn, Map<String, Object> event) throws SQLException, Exception;
    void updateAttribute(Connection conn, Map<String, Object> event) throws SQLException, Exception;
    void deleteAttribute(Connection conn, Map<String, Object> event) throws SQLException, Exception;
    Result<String> queryAttribute(int offset, int limit, String filters, String globalFilter, String sorting, String hostId);
    Result<String> queryAttributeLabel(String hostId);

    // AttributePermission
    void createAttributePermission(Connection conn, Map<String, Object> event) throws SQLException, Exception;
    void updateAttributePermission(Connection conn, Map<String, Object> event) throws SQLException, Exception;
    void deleteAttributePermission(Connection conn, Map<String, Object> event) throws SQLException, Exception;
    Result<String> queryAttributePermission(int offset, int limit, String filters, String globalFilter, String sorting, String hostId);

    // AttributeUser
    void createAttributeUser(Connection conn, Map<String, Object> event) throws SQLException, Exception;
    void updateAttributeUser(Connection conn, Map<String, Object> event) throws SQLException, Exception;
    void deleteAttributeUser(Connection conn, Map<String, Object> event) throws SQLException, Exception;
    Result<String> queryAttributeUser(int offset, int limit, String filters, String globalFilter, String sorting, String hostId);

    // AttributeRowFilter
    void createAttributeRowFilter(Connection conn, Map<String, Object> event) throws SQLException, Exception;
    void updateAttributeRowFilter(Connection conn, Map<String, Object> event) throws SQLException, Exception;
    void deleteAttributeRowFilter(Connection conn, Map<String, Object> event) throws SQLException, Exception;
    Result<String> queryAttributeRowFilter(int offset, int limit, String filters, String globalFilter, String sorting, String hostId);

    // AttributeColFilter
    void createAttributeColFilter(Connection conn, Map<String, Object> event) throws SQLException, Exception;
    void updateAttributeColFilter(Connection conn, Map<String, Object> event) throws SQLException, Exception;
    void deleteAttributeColFilter(Connection conn, Map<String, Object> event) throws SQLException, Exception;
    Result<String> queryAttributeColFilter(int offset, int limit, String filters, String globalFilter, String sorting, String hostId);
}
