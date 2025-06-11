package net.lightapi.portal.db.persistence;

import com.networknt.monad.Result;
import java.util.Map;

public interface CategoryPersistence {
    Result<String> createCategory(Map<String, Object> event);
    Result<String> updateCategory(Map<String, Object> event);
    Result<String> deleteCategory(Map<String, Object> event);
    Result<String> getCategory(int offset, int limit, String hostId, String categoryId, String entityType, String categoryName, String categoryDesc, String parentCategoryId, String parentCategoryName, Integer sortOrder);
    Result<String> getCategoryLabel(String hostId);
    Result<String> getCategoryById(String categoryId);
    Result<String> getCategoryByName(String hostId, String categoryName);
    Result<String> getCategoryByType(String hostId, String entityType);
    Result<String> getCategoryTree(String hostId, String entityType);
}
