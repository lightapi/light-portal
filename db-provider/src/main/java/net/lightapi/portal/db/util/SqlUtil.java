package net.lightapi.portal.db.util;

import com.networknt.config.JsonMapper;
import io.cloudevents.CloudEvent;
import io.cloudevents.core.v1.CloudEventV1;
import net.lightapi.portal.PortalConstants;
import net.lightapi.portal.db.persistence.ReferenceDataPersistenceImpl;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.*;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;

public class SqlUtil {
    private static final Logger logger = LoggerFactory.getLogger(SqlUtil.class);

    private SqlUtil() {
        // Private constructor for utility class
    }

    /**
     * Appends "WHERE " or " AND " to the clause builder based on its current content.
     * @param clauseBuilder StringBuilder for the WHERE clause.
     */
    public static void appendWhereOrAnd(StringBuilder clauseBuilder) {
        if (clauseBuilder.length() == 0) {
            clauseBuilder.append("WHERE ");
        } else {
            clauseBuilder.append(" AND ");
        }
    }

    /**
     * Adds a condition to the WHERE clause for String values, using LIKE for wildcard matching.
     * Manages adding " AND " if other conditions already exist.
     *
     * @param whereClause StringBuilder to append the condition to.
     * @param parameters List to add the parameter value to.
     * @param columnName The database column name.
     * @param value The string value to filter by. Can contain SQL LIKE wildcards ('%', '_').
     *              If value is null or "*", the condition is not added.
     */
    public static void addCondition(StringBuilder whereClause, List<Object> parameters, String columnName, String value) {
        if (value != null && !value.equals("*") && !value.isEmpty()) {
            if (whereClause.length() > 0) {
                whereClause.append(" AND ");
            }
            whereClause.append(columnName);
            if (value.contains("%") || value.contains("_")) {
                whereClause.append(" LIKE ?");
            } else {
                whereClause.append(" = ?"); // Exact match for non-wildcard strings
            }
            parameters.add(value);
        }
    }

    /**
     * Adds a condition to the WHERE clause for general Object values (e.g., UUID, Boolean, Integer).
     * Manages adding " AND " if other conditions already exist.
     *
     * @param whereClause StringBuilder to append the condition to.
     * @param parameters List to add the parameter value to.
     * @param columnName The database column name.
     * @param value The object value to filter by. If null, the condition is not added.
     */
    public static void addCondition(StringBuilder whereClause, List<Object> parameters, String columnName, Object value) {
        if (value != null) {
            if (whereClause.length() > 0) {
                whereClause.append(" AND ");
            }
            whereClause.append(columnName).append(" = ?");
            parameters.add(value);
        }
    }

    /**
     * Helper method to add a condition string and parameter to lists for dynamic query building.
     * Used in getCategory.
     * @param conditions List of condition strings (e.g., "col = ?")
     * @param parameters List of parameters for PreparedStatement
     * @param columnName Database column name
     * @param columnValue Column value from method parameters
     */
    public static void addConditionToList(List<String> conditions, List<Object> parameters, String columnName, Object columnValue) {
        if (columnValue != null) {
            if (columnValue instanceof String && ((String) columnValue).isEmpty()) {
                return; // Skip empty string filters
            }
            // This specific version from original getCategory used LIKE with wildcards
            conditions.add(columnName + " LIKE ?");
            parameters.add("%" + columnValue + "%");
        }
    }

    public static Map<String, Object> extractEventData(Map<String, Object> event) {
        Object data = event.get(PortalConstants.DATA);
        if (data instanceof Map) {
            return (Map<String, Object>) data;
        }
        throw new IllegalArgumentException("CloudEvent data is missing or not a Map.");
    }

    // This is the expected version from the projection. It is the existing version of the aggregate.
    public static long getOldAggregateVersion(Map<String, Object> event) {
        Map<String, Object> dataMap = extractEventData(event);
        if (dataMap.containsKey("aggregateVersion") && dataMap.get("aggregateVersion") instanceof Number) {
            return ((Number) dataMap.get("aggregateVersion")).longValue();
        }
        throw new IllegalArgumentException("CloudEvent data missing 'aggregateVersion' for aggregate versioning.");
    }

    // This is the new version of the aggregate that is being created or updated.
    public static long getNewAggregateVersion(Map<String, Object> event) {
        Map<String, Object> dataMap = extractEventData(event);
        if (dataMap.containsKey("newAggregateVersion") && dataMap.get("newAggregateVersion") instanceof Number) {
            return ((Number) dataMap.get("newAggregateVersion")).longValue();
        }
        // For CREATE events, aggregateVersion is typically 0. For UPDATE/DELETE, it should be present.
        // Differentiate here, or rely on caller to only call this for update/delete events.
        String eventType = (String) event.get(CloudEventV1.TYPE);
        if (eventType != null && eventType.endsWith("CreatedEvent")) {
            return 1L; // For creation events, expected version is 1 (no prior state)
        }
        throw new IllegalArgumentException("CloudEvent data missing 'newAggregateVersion' for optimistic concurrency check.");
    }

    /**
     * Creates a SQL array literal from a list of strings.
     * Trims whitespace, removes duplicates and blank entries.
     * Returns "{}" for null or empty input.
     *
     * @param strings Collection of strings to convert to SQL array literal.
     * @return SQL array literal string (e.g., "{val1,val2}").
     */
    public static String createArrayLiteral(Collection<String> strings) {
        String processedString = strings != null && !strings.isEmpty()
            ? strings.stream()
            .filter(StringUtils::isNotBlank)
            .map(String::trim)
            .distinct()
            .collect(Collectors.joining(","))
            : "";

        return createArrayLiteral(processedString);
    }

    /**
     * Creates a SQL array literal from a comma-separated string.
     * Trims whitespace. Returns "{}" for null or blank input.
     *
     * @param csvString Comma-separated string to convert to SQL array literal.
     * @return SQL array literal string (e.g., "{val1,val2}").
     */
    public static String createArrayLiteral(String csvString) {
        return String.format("{%s}", StringUtils.isNotBlank(csvString) ? csvString.trim() : "");
    }

    /**
     * Executes a series of database operations within a transaction.
     * Commits if successful, rolls back on exception, and ensures connection is closed.
     *
     * @param connection JDBC Connection to use for the transaction.
     * @param callback Consumer that performs database operations using the provided Connection.
     * @throws SQLException if a database access error occurs or the callback throws an exception.
     */
    public static void transact(
        final Connection connection,
        Consumer<Connection> callback
    ) throws SQLException {
        Objects.requireNonNull(connection, "Connection must not be null");
        try {
            connection.setAutoCommit(false);
            callback.accept(connection);
            connection.commit();
        } catch (Exception e) {
            connection.rollback();
            throw e;
        } finally {
            connection.close();
        }
    }

    /**
     * Executes a series of database operations within a transaction and returns a result.
     * Commits if successful, rolls back on exception, and ensures connection is closed.
     *
     * @param connection JDBC Connection to use for the transaction.
     * @param callback Function that performs database operations using the provided Connection and returns a result.
     * @param <T> The type of the result returned by the callback.
     * @return The result from the callback function.
     * @throws SQLException if a database access error occurs or the callback throws an exception.
     */
    public static <T> T transactWithResult(
        final Connection connection,
        Function<Connection, T> callback
    ) throws SQLException {
        Objects.requireNonNull(connection, "Connection must not be null");
        try {
            connection.setAutoCommit(false);
            T result = callback.apply(connection);
            connection.commit();
            return result;
        } catch (Exception e) {
            connection.rollback();
            throw e;
        } finally {
            connection.close();
        }
    }

    public static List<Map<String, Object>> parseJsonList(String json) {
        if (json == null || json.isEmpty() || "[]".equals(json.trim())) {
            return Collections.emptyList();
        }
        return JsonMapper.string2List(json);
    }

    // Utility to convert simple CamelCase to snake_case
    public static String camelToSnake(String camelCase) {
        if (camelCase == null || camelCase.isEmpty()) {
            return camelCase;
        }
        // Simple regex: insert underscore before every capital letter and lowercase the whole string
        return camelCase.replaceAll("([a-z])([A-Z]+)", "$1_$2").toLowerCase(Locale.ROOT);
    }

    public static String mapToDbColumn(Map<String, String> columnMap, String camelCaseName) {
        String dbName = columnMap.get(camelCaseName);
        if (dbName == null) {
            // Log warning or throw error if an unknown column name is used
            logger.warn("Attempted to map unknown column name: {}", camelCaseName);
            // Defaulting to the original name can be dangerous, but we'll stick to a strict lookup
            return camelCaseName;
        }
        return dbName;
    }

    public static StringBuilder dynamicFilter(List<String> uuidColumnNames, List<String> likeColumnNames, List<Map<String, Object>> filters, Map<String, String> columnMap, List<Object> parameters) {
        StringBuilder sb = new StringBuilder();
        // Material React Table Filters (Dynamic Filters) ---
        for (Map<String, Object> filter : filters) {
            String filterId = (String) filter.get("id"); // Column name
            String dbColumnName = columnMap == null ? camelToSnake(filterId) : mapToDbColumn(columnMap, filterId);
            Object filterValue = filter.get("value");    // Value to filter by
            if (filterId != null && filterValue != null && !filterValue.toString().isEmpty()) {
                if(uuidColumnNames.contains(dbColumnName)) {
                    sb.append(" AND ").append(dbColumnName).append(" = ?");
                    parameters.add(UUID.fromString(filterValue.toString()));
                } else if(likeColumnNames.contains(dbColumnName)) {
                    sb.append(" AND ").append(dbColumnName).append(" ILIKE ?");
                    parameters.add("%" + filterValue + "%");
                } else {
                    sb.append(" AND ").append(dbColumnName).append(" = ?");
                    parameters.add(filterValue);
                }
            }
        }
        return sb;
    }

    public static StringBuilder globalFilter(String globalFilter, String[] searchColumns, List<Object> parameters) {
        StringBuilder sb = new StringBuilder();
        // Global Filter (Search across multiple columns)
        if (globalFilter != null && !globalFilter.isEmpty()) {
            sb.append(" AND (");
            // Define columns to search for global filter (e.g., table_name, table_desc)
            List<String> globalConditions = new ArrayList<>();
            for (String col : searchColumns) {
                globalConditions.add(col + " ILIKE ?");
                parameters.add("%" + globalFilter + "%");
            }
            sb.append(String.join(" OR ", globalConditions));
            sb.append(")");
        }
        return sb;
    }

    public static StringBuilder dynamicSorting(String defaultSorting, List<Map<String, Object>> sorting, Map<String, String> columnMap) {
        StringBuilder orderByClause = new StringBuilder();
        if (sorting.isEmpty()) {
            // Default sort if none provided
            orderByClause.append(" ORDER BY ").append(defaultSorting);
        } else {
            orderByClause.append(" ORDER BY ");
            List<String> sortExpressions = new ArrayList<>();
            for (Map<String, Object> sort : sorting) {
                String sortId = (String) sort.get("id");
                String dbColumnName = columnMap == null ? camelToSnake(sortId) : mapToDbColumn(columnMap, sortId);
                Boolean isDesc = (Boolean) sort.get("desc"); // 'desc' is typically a boolean or "true"/"false" string
                if (sortId != null && !sortId.isEmpty()) {
                    String direction = (isDesc != null && isDesc) ? "DESC" : "ASC";
                    // Quote column name to handle SQL keywords or mixed case
                    sortExpressions.add(dbColumnName + " " + direction);
                }
            }
            // Use default if dynamic sort failed to produce anything
            orderByClause.append(sortExpressions.isEmpty() ? defaultSorting : String.join(", ", sortExpressions));
        }
        return orderByClause;
    }

    public static void populateParameters(PreparedStatement preparedStatement, List<Object> parameters) throws SQLException {
        for (int i = 0; i < parameters.size(); i++) {
            // Ensure proper type setting (especially for UUIDs, Booleans, etc.)
            if (parameters.get(i) instanceof UUID) {
                preparedStatement.setObject(i + 1, parameters.get(i));
            } else if (parameters.get(i) instanceof Boolean) {
                preparedStatement.setBoolean(i + 1, (Boolean) parameters.get(i));
            } else {
                preparedStatement.setObject(i + 1, parameters.get(i));
            }
        }

    }

    /**
     * Generates the SQL condition for soft-delete filtering across multiple tables.
     *
     * @param active       If true, enforces strict consistency (all tables must be active).
     *                     If false, checks if the primary entity (first alias) is inactive.
     * @param tableAliases A variable list of table aliases involved in the query.
     *                     IMPORTANT: The first alias provided must be the primary table being queried.
     * @return A SQL string starting with " AND ...", or an empty string if no aliases provided.
     */
    public static String buildMultiTableActiveClause(boolean active, String... tableAliases) {
        StringBuilder sb = new StringBuilder();

        if (tableAliases == null || tableAliases.length == 0) {
            if(active)
                return sb.append(" AND active = true").toString();
            else
                return sb.append(" AND active = false").toString();
        }

        if (active) {
            // Scenario: Active View
            // Requirement: Strict Consistency. To be considered a valid active record in a join,
            // the record itself AND its parents/associations must all be active.
            for (String alias : tableAliases) {
                sb.append(" AND ").append(alias).append(".active = true");
            }
        } else {
            // Scenario: Trash/Deleted View
            // Requirement: We are looking for records that were deleted.
            // We usually only check the status of the *primary* entity (the first arg).
            // The status of related tables (aliases[1..n]) usually doesn't filter the result here.
            sb.append(" AND ").append(tableAliases[0]).append(".active = false");
        }

        return sb.toString();
    }

}
