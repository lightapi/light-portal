package net.lightapi.portal.db.persistence;

import com.networknt.monad.Result;
import java.util.Map;

public interface AccessControlPersistence {
    // Role
    Result<String> createRole(Map<String, Object> event);
    Result<String> updateRole(Map<String, Object> event);
    Result<String> deleteRole(Map<String, Object> event);
    Result<String> queryRole(int offset, int limit, String hostId, String roleId, String roleDesc);
    Result<String> queryRoleLabel(String hostId);

    // RolePermission
    Result<String> createRolePermission(Map<String, Object> event);
    Result<String> deleteRolePermission(Map<String, Object> event);
    Result<String> queryRolePermission(int offset, int limit, String hostId, String roleId, String apiId, String apiVersion, String endpoint);

    // RoleUser
    Result<String> createRoleUser(Map<String, Object> event);
    Result<String> updateRoleUser(Map<String, Object> event);
    Result<String> deleteRoleUser(Map<String, Object> event);
    Result<String> queryRoleUser(int offset, int limit, String hostId, String roleId, String userId, String entityId, String email, String firstName, String lastName, String userType);

    // RoleRowFilter
    Result<String> createRoleRowFilter(Map<String, Object> event);
    Result<String> updateRoleRowFilter(Map<String, Object> event);
    Result<String> deleteRoleRowFilter(Map<String, Object> event);
    Result<String> queryRoleRowFilter(int offset, int limit, String hostId, String roleId, String apiId, String apiVersion, String endpoint);

    // RoleColFilter
    Result<String> createRoleColFilter(Map<String, Object> event);
    Result<String> updateRoleColFilter(Map<String, Object> event);
    Result<String> deleteRoleColFilter(Map<String, Object> event);
    Result<String> queryRoleColFilter(int offset, int limit, String hostId, String roleId, String apiId, String apiVersion, String endpoint);

    // Group
    Result<String> createGroup(Map<String, Object> event);
    Result<String> updateGroup(Map<String, Object> event);
    Result<String> deleteGroup(Map<String, Object> event);
    Result<String> queryGroup(int offset, int limit, String hostId, String groupId, String groupDesc);
    Result<String> queryGroupLabel(String hostId);

    // GroupPermission
    Result<String> createGroupPermission(Map<String, Object> event);
    Result<String> deleteGroupPermission(Map<String, Object> event);
    Result<String> queryGroupPermission(int offset, int limit, String hostId, String groupId, String apiId, String apiVersion, String endpoint);

    // GroupUser
    Result<String> createGroupUser(Map<String, Object> event);
    Result<String> updateGroupUser(Map<String, Object> event);
    Result<String> deleteGroupUser(Map<String, Object> event);
    Result<String> queryGroupUser(int offset, int limit, String hostId, String groupId, String userId, String entityId, String email, String firstName, String lastName, String userType);

    // GroupRowFilter
    Result<String> createGroupRowFilter(Map<String, Object> event);
    Result<String> updateGroupRowFilter(Map<String, Object> event);
    Result<String> deleteGroupRowFilter(Map<String, Object> event);
    Result<String> queryGroupRowFilter(int offset, int limit, String hostId, String GroupId, String apiId, String apiVersion, String endpoint);

    // GroupColFilter
    Result<String> createGroupColFilter(Map<String, Object> event);
    Result<String> updateGroupColFilter(Map<String, Object> event);
    Result<String> deleteGroupColFilter(Map<String, Object> event);
    Result<String> queryGroupColFilter(int offset, int limit, String hostId, String GroupId, String apiId, String apiVersion, String endpoint);

    // Position
    Result<String> createPosition(Map<String, Object> event);
    Result<String> updatePosition(Map<String, Object> event);
    Result<String> deletePosition(Map<String, Object> event);
    Result<String> queryPosition(int offset, int limit, String hostId, String positionId, String positionDesc, String inheritToAncestor, String inheritToSibling);
    Result<String> queryPositionLabel(String hostId);

    // PositionPermission
    Result<String> createPositionPermission(Map<String, Object> event);
    Result<String> deletePositionPermission(Map<String, Object> event);
    Result<String> queryPositionPermission(int offset, int limit, String hostId, String positionId, String inheritToAncestor, String inheritToSibling, String apiId, String apiVersion, String endpoint);

    // PositionUser
    Result<String> createPositionUser(Map<String, Object> event);
    Result<String> updatePositionUser(Map<String, Object> event);
    Result<String> deletePositionUser(Map<String, Object> event);
    Result<String> queryPositionUser(int offset, int limit, String hostId, String positionId, String positionType, String inheritToAncestor, String inheritToSibling, String userId, String entityId, String email, String firstName, String lastName, String userType);

    // PositionRowFilter
    Result<String> createPositionRowFilter(Map<String, Object> event);
    Result<String> updatePositionRowFilter(Map<String, Object> event);
    Result<String> deletePositionRowFilter(Map<String, Object> event);
    Result<String> queryPositionRowFilter(int offset, int limit, String hostId, String PositionId, String apiId, String apiVersion, String endpoint);

    // PositionColFilter
    Result<String> createPositionColFilter(Map<String, Object> event);
    Result<String> updatePositionColFilter(Map<String, Object> event);
    Result<String> deletePositionColFilter(Map<String, Object> event);
    Result<String> queryPositionColFilter(int offset, int limit, String hostId, String PositionId, String apiId, String apiVersion, String endpoint);

    // Attribute
    Result<String> createAttribute(Map<String, Object> event);
    Result<String> updateAttribute(Map<String, Object> event);
    Result<String> deleteAttribute(Map<String, Object> event);
    Result<String> queryAttribute(int offset, int limit, String hostId, String attributeId, String attributeType, String attributeDesc);
    Result<String> queryAttributeLabel(String hostId);

    // AttributePermission
    Result<String> createAttributePermission(Map<String, Object> event);
    Result<String> updateAttributePermission(Map<String, Object> event);
    Result<String> deleteAttributePermission(Map<String, Object> event);
    Result<String> queryAttributePermission(int offset, int limit, String hostId, String attributeId, String attributeType, String attributeValue, String apiId, String apiVersion, String endpoint);

    // AttributeUser
    Result<String> createAttributeUser(Map<String, Object> event);
    Result<String> updateAttributeUser(Map<String, Object> event);
    Result<String> deleteAttributeUser(Map<String, Object> event);
    Result<String> queryAttributeUser(int offset, int limit, String hostId, String attributeId, String attributeType, String attributeValue, String userId, String entityId, String email, String firstName, String lastName, String userType);

    // AttributeRowFilter
    Result<String> createAttributeRowFilter(Map<String, Object> event);
    Result<String> updateAttributeRowFilter(Map<String, Object> event);
    Result<String> deleteAttributeRowFilter(Map<String, Object> event);
    Result<String> queryAttributeRowFilter(int offset, int limit, String hostId, String attributeId, String attributeValue, String apiId, String apiVersion, String endpoint);

    // AttributeColFilter
    Result<String> createAttributeColFilter(Map<String, Object> event);
    Result<String> updateAttributeColFilter(Map<String, Object> event);
    Result<String> deleteAttributeColFilter(Map<String, Object> event);
    Result<String> queryAttributeColFilter(int offset, int limit, String hostId, String attributeId, String attributeValue, String apiId, String apiVersion, String endpoint);
}
