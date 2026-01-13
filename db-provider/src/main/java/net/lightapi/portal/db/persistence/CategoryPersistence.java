package net.lightapi.portal.db.persistence;

import com.networknt.monad.Result;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Map;

public interface CategoryPersistence {
    void createCategory(Connection conn, Map<String, Object> event) throws SQLException, Exception;
    void updateCategory(Connection conn, Map<String, Object> event) throws SQLException, Exception;
    void deleteCategory(Connection conn, Map<String, Object> event) throws SQLException, Exception;
    void createEntityCategory(Connection conn, Map<String, Object> event) throws SQLException, Exception;
    void updateEntityCategory(Connection conn, Map<String, Object> event) throws SQLException, Exception;
    void deleteEntityCategory(Connection conn, Map<String, Object> event) throws SQLException, Exception;
    Result<String> getCategory(int offset, int limit, String filters, String globalFilter, String sorting, boolean active, String hostId);
    Result<String> getCategoryLabel(String hostId);
    Result<String> getCategoryById(String categoryId);
    Result<String> getCategoryByName(String hostId, String categoryName);
    Result<String> getCategoryByType(String hostId, String entityType);
    Result<String> getCategoryTree(String hostId, String entityType);
}
