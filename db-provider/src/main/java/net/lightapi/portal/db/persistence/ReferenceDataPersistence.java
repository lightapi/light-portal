package net.lightapi.portal.db.persistence;

import com.networknt.monad.Result;
import net.lightapi.portal.db.PortalPersistenceException;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Map;

public interface ReferenceDataPersistence {
    void createRefTable(Connection conn, Map<String, Object> event) throws PortalPersistenceException;
    void updateRefTable(Connection conn, Map<String, Object> event) throws PortalPersistenceException;
    void deleteRefTable(Connection conn, Map<String, Object> event) throws PortalPersistenceException;
    Result<String> getRefTable(int offset, int limit, String filters, String globalFilter, String sorting, boolean active, String hostId);
    Result<String> getRefTableById(String tableId);
    Result<String> getRefTableLabel(String hostId);

    void createRefValue(Connection conn, Map<String, Object> event) throws PortalPersistenceException;
    void updateRefValue(Connection conn, Map<String, Object> event) throws PortalPersistenceException;
    void deleteRefValue(Connection conn, Map<String, Object> event) throws PortalPersistenceException;
    Result<String> getRefValue(int offset, int limit, String filters, String globalFilter, String sorting, boolean active);
    Result<String> getRefValueById(String valueId);
    Result<String> getRefValueLabel(String tableId);

    void createRefLocale(Connection conn, Map<String, Object> event) throws PortalPersistenceException;
    void updateRefLocale(Connection conn, Map<String, Object> event) throws PortalPersistenceException;
    void deleteRefLocale(Connection conn, Map<String, Object> event) throws PortalPersistenceException;
    Result<String> getRefLocale(int offset, int limit, String filters, String globalFilter, String sorting, boolean active);

    void createRefRelationType(Connection conn, Map<String, Object> event) throws PortalPersistenceException;
    void updateRefRelationType(Connection conn, Map<String, Object> event) throws PortalPersistenceException;
    void deleteRefRelationType(Connection conn, Map<String, Object> event) throws PortalPersistenceException;
    Result<String> getRefRelationType(int offset, int limit, String filters, String globalFilter, String sorting, boolean active);

    void createRefRelation(Connection conn, Map<String, Object> event) throws PortalPersistenceException;
    void updateRefRelation(Connection conn, Map<String, Object> event) throws PortalPersistenceException;
    void deleteRefRelation(Connection conn, Map<String, Object> event) throws PortalPersistenceException;
    Result<String> getRefRelation(int offset, int limit, String filters, String globalFilter, String sorting, boolean active);
    Result<String> getToValueCode(String relationName, String fromValueCode);
}
