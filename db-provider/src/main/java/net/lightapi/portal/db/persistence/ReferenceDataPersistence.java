package net.lightapi.portal.db.persistence;

import com.networknt.monad.Result;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Map;

public interface ReferenceDataPersistence {
    void createRefTable(Connection conn, Map<String, Object> event) throws SQLException, Exception;
    void updateRefTable(Connection conn, Map<String, Object> event) throws SQLException, Exception;
    void deleteRefTable(Connection conn, Map<String, Object> event) throws SQLException, Exception;
    Result<String> getRefTable(int offset, int limit, String hostId, String tableId, String tableName, String tableDesc, Boolean active, Boolean editable);
    Result<String> getRefTableById(String tableId);
    Result<String> getRefTableLabel(String hostId);

    void createRefValue(Connection conn, Map<String, Object> event) throws SQLException, Exception;
    void updateRefValue(Connection conn, Map<String, Object> event) throws SQLException, Exception;
    void deleteRefValue(Connection conn, Map<String, Object> event) throws SQLException, Exception;
    Result<String> getRefValue(int offset, int limit, String valueId, String tableId, String valueCode, String valueDesc, Integer displayOrder, Boolean active);
    Result<String> getRefValueById(String valueId);
    Result<String> getRefValueLabel(String tableId);

    void createRefLocale(Connection conn, Map<String, Object> event) throws SQLException, Exception;
    void updateRefLocale(Connection conn, Map<String, Object> event) throws SQLException, Exception;
    void deleteRefLocale(Connection conn, Map<String, Object> event) throws SQLException, Exception;
    Result<String> getRefLocale(int offset, int limit, String valueId, String valueCode, String valueDesc, String language, String valueLabel);

    void createRefRelationType(Connection conn, Map<String, Object> event) throws SQLException, Exception;
    void updateRefRelationType(Connection conn, Map<String, Object> event) throws SQLException, Exception;
    void deleteRefRelationType(Connection conn, Map<String, Object> event) throws SQLException, Exception;
    Result<String> getRefRelationType(int offset, int limit, String relationId, String relationName, String relationDesc);

    void createRefRelation(Connection conn, Map<String, Object> event) throws SQLException, Exception;
    void updateRefRelation(Connection conn, Map<String, Object> event) throws SQLException, Exception;
    void deleteRefRelation(Connection conn, Map<String, Object> event) throws SQLException, Exception;
    Result<String> getRefRelation(int offset, int limit, String relationId, String relationName, String valueIdFrom, String valueCodeFrom, String valueIdTo, String valueCodeTo, Boolean active);
}
