package net.lightapi.portal.validation;

import java.util.Set;

public class ValidationUtil {
    public static boolean isValidColumn(String column, Set<String> allowedColumns) {
        if (column == null || column.isEmpty()) {
            return false;
        }
        return allowedColumns.contains(column);
    }
}
