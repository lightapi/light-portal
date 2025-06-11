package net.lightapi.portal.db.util;

import java.util.List;

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
}
