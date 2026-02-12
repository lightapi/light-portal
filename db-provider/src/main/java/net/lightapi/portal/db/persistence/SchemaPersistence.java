package net.lightapi.portal.db.persistence;

import com.networknt.monad.Result;
import net.lightapi.portal.db.PortalPersistenceException;

import java.sql.Connection; // Added import
import java.sql.SQLException; // Added import
import java.util.Map;

public interface SchemaPersistence {
    void createSchema(Connection conn, Map<String, Object> event) throws PortalPersistenceException;
    void updateSchema(Connection conn, Map<String, Object> event) throws PortalPersistenceException;
    void deleteSchema(Connection conn, Map<String, Object> event) throws PortalPersistenceException;
    Result<String> getSchema(int offset, int limit, String filters, String globalFilter, String sorting, boolean active, String hostId);
    Result<String> getSchemaLabel(String hostId);
    Result<String> getSchemaById(String schemaId);
    Result<String> getSchemaByCategoryId(String categoryId);
    Result<String> getSchemaByTagId(String tagId);
}
