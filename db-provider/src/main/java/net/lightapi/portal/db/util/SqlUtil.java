package net.lightapi.portal.db.util;

import io.cloudevents.CloudEvent;
import io.cloudevents.core.v1.CloudEventV1;
import net.lightapi.portal.PortalConstants;
import org.apache.commons.lang3.StringUtils;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;

public class SqlUtil {

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
}
