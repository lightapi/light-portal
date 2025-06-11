package net.lightapi.portal.db.persistence;

import com.networknt.monad.Result;
import java.util.Map;

public interface ReferenceDataPersistence {
    Result<String> createRefTable(Map<String, Object> event);
    Result<String> updateRefTable(Map<String, Object> event);
    Result<String> deleteRefTable(Map<String, Object> event);
    Result<String> getRefTable(int offset, int limit, String hostId, String tableId, String tableName, String tableDesc, Boolean active, Boolean editable);
    Result<String> getRefTableById(String tableId);
    Result<String> getRefTableLabel(String hostId);

    Result<String> createRefValue(Map<String, Object> event);
    Result<String> updateRefValue(Map<String, Object> event);
    Result<String> deleteRefValue(Map<String, Object> event);
    Result<String> getRefValue(int offset, int limit, String valueId, String tableId, String valueCode, String valueDesc, Integer displayOrder, Boolean active);
    Result<String> getRefValueById(String valueId);
    Result<String> getRefValueLabel(String tableId);

    Result<String> createRefLocale(Map<String, Object> event);
    Result<String> updateRefLocale(Map<String, Object> event);
    Result<String> deleteRefLocale(Map<String, Object> event);
    Result<String> getRefLocale(int offset, int limit, String valueId, String valueCode, String valueDesc, String language, String valueLabel);

    Result<String> createRefRelationType(Map<String, Object> event);
    Result<String> updateRefRelationType(Map<String, Object> event);
    Result<String> deleteRefRelationType(Map<String, Object> event);
    Result<String> getRefRelationType(int offset, int limit, String relationId, String relationName, String relationDesc);

    Result<String> createRefRelation(Map<String, Object> event);
    Result<String> updateRefRelation(Map<String, Object> event);
    Result<String> deleteRefRelation(Map<String, Object> event);
    Result<String> getRefRelation(int offset, int limit, String relationId, String relationName, String valueIdFrom, String valueCodeFrom, String valueIdTo, String valueCodeTo, Boolean active);
}
