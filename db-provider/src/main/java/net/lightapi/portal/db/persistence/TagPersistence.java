package net.lightapi.portal.db.persistence;

import com.networknt.monad.Result;
import java.util.Map;

public interface TagPersistence {
    Result<String> createTag(Map<String, Object> event);
    Result<String> updateTag(Map<String, Object> event);
    Result<String> deleteTag(Map<String, Object> event);
    Result<String> getTag(int offset, int limit, String hostId, String tagId, String entityType, String tagName, String tagDesc);
    Result<String> getTagLabel(String hostId);
    Result<String> getTagById(String tagId);
    Result<String> getTagByName(String hostId, String tagName);
    Result<String> getTagByType(String hostId, String entityType);
}
