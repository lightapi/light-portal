package net.lightapi.portal.db.persistence;

import com.networknt.monad.Result;
import java.sql.Connection; // Added import
import java.sql.SQLException; // Added import
import java.util.Map;

public interface SchemaPersistence {
    void createSchema(Connection conn, Map<String, Object> event) throws SQLException, Exception;
    void updateSchema(Connection conn, Map<String, Object> event) throws SQLException, Exception;
    void deleteSchema(Connection conn, Map<String, Object> event) throws SQLException, Exception;
    Result<String> getSchema(int offset, int limit, String hostId, String schemaId, String schemaVersion, String schemaType, String specVersion, String schemaSource, String schemaName, String schemaDesc, String schemaBody, String schemaOwner, String schemaStatus, String example, String commentStatus);
    Result<String> getSchemaLabel(String hostId);
    Result<String> getSchemaById(String schemaId);
    Result<String> getSchemaByCategoryId(String categoryId);
    Result<String> getSchemaByTagId(String tagId);
}
