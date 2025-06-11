package net.lightapi.portal.db.persistence;

import com.networknt.monad.Result;
import java.util.Map;

public interface SchemaPersistence {
    Result<String> createSchema(Map<String, Object> event);
    Result<String> updateSchema(Map<String, Object> event);
    Result<String> deleteSchema(Map<String, Object> event);
    Result<String> getSchema(int offset, int limit, String hostId, String schemaId, String schemaVersion, String schemaType, String specVersion, String schemaSource, String schemaName, String schemaDesc, String schemaBody, String schemaOwner, String schemaStatus, String example, String commentStatus);
    Result<String> getSchemaLabel(String hostId);
    Result<String> getSchemaById(String schemaId);
    Result<String> getSchemaByCategoryId(String categoryId);
    Result<String> getSchemaByTagId(String tagId);
}
