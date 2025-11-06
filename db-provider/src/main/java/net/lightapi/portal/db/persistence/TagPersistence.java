package net.lightapi.portal.db.persistence;

import com.networknt.monad.Result;
import java.sql.Connection; // Added import
import java.sql.SQLException; // Added import
import java.util.Map;

public interface TagPersistence {
    void createTag(Connection conn, Map<String, Object> event) throws SQLException, Exception;
    void updateTag(Connection conn, Map<String, Object> event) throws SQLException, Exception;
    void deleteTag(Connection conn, Map<String, Object> event) throws SQLException, Exception;
    Result<String> getTag(int offset, int limit, String filters, String globalFilter, String sorting, String hostId);
    Result<String> getTagLabel(String hostId);
    Result<String> getTagById(String tagId);
    Result<String> getTagByName(String hostId, String tagName);
    Result<String> getTagByType(String hostId, String entityType);
}
