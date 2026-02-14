package net.lightapi.portal.validation;

import java.util.Set;

/**
 * Utility class for validation operations.
 */
public class ValidationUtil {

    private ValidationUtil() {
    }
    /**
     * Checks if a column name is valid (i.e., exists in the allowed set).
     *
     * @param column         The column name to check.
     * @param allowedColumns The set of allowed column names.
     * @return true if valid, false otherwise.
     */
    public static boolean isValidColumn(String column, Set<String> allowedColumns) {
        if (column == null || column.isEmpty()) {
            return false;
        }
        return allowedColumns.contains(column);
    }
}
