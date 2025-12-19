package net.lightapi.portal.db.persistence;

import com.networknt.monad.Result;
import java.sql.Connection; // Added import
import java.sql.SQLException; // Added import
import java.util.Map;

public interface SchemaPersistence {
    void createSchema(Connection conn, Map<String, Object> event) throws SQLException, Exception;
    void updateSchema(Connection conn, Map<String, Object> event) throws SQLException, Exception;
    void deleteSchema(Connection conn, Map<String, Object> event) throws SQLException, Exception;
    Result<String> getSchema(int offset, int limit, String filters, String globalFilter, String sorting, boolean active, String hostId);
    Result<String> getSchemaLabel(String hostId);
    Result<String> getSchemaById(String schemaId);
    Result<String> getSchemaByCategoryId(String categoryId);
    Result<String> getSchemaByTagId(String tagId);
}
