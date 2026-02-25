package net.lightapi.portal.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;

/**
 * Utility class for mapping YAML content.
 */
public final class YamlMapper {

    private static final Logger LOGGER = LoggerFactory.getLogger(YamlMapper.class);

    private YamlMapper() {
        // Suppress instantiation
    }

    /**
     * Parses a YAML string into a Map-
     *
     * @param value The YAML string.
     * @return A Map representation of the YAML.
     * @throws YamlConversionException If parsing fails.
     */
    public static Map<String, Object> toMap(String value) throws YamlConversionException {
        try {
            return YamlSupplier.yaml().load(value);
        } catch (Exception e) {
            LOGGER.warn("Failed to parse map from value: {}", value, e);
            throw new YamlConversionException("Conversion failed to map from value: " + value, e);
        }
    }

    /**
     * Parses a YAML string into a List.
     *
     * @param value The YAML string.
     * @return A List representation of the YAML.
     * @throws YamlConversionException If parsing fails.
     */
    public static List<Object> toList(String value) throws YamlConversionException {
        try {
            return YamlSupplier.yaml().load(value);
        } catch (Exception e) {
            LOGGER.warn("Failed to parse list from value: {}", value, e);
            throw new YamlConversionException("Conversion failed to list from value: " + value, e);
        }
    }

    /**
     * Converts an object to a minified YAML string.
     *
     * @param value The object to convert.
     * @return A minified YAML string.
     * @throws YamlConversionException If conversion fails.
     */
    public static String toMinifiedYamlString(Object value) throws YamlConversionException {
        try {
            return YamlSupplier.yaml().dump(value);
        } catch (Exception e) {
            LOGGER.warn("Failed to convert value to minified YAML string: {}", value, e);
            throw new YamlConversionException("Conversion failed to minified YAML string from value: " + value, e);
        }
    }

    /**
     * Converts an object to a prettified YAML string.
     *
     * @param value The object to convert.
     * @return A prettified YAML string.
     * @throws YamlConversionException If conversion fails.
     */
    public static String toPrettifiedYamlString(Object value) throws YamlConversionException {
        try {
            return YamlSupplier.prettyYaml().dump(value);
        } catch (Exception e) {
            LOGGER.warn("Failed to convert value to pretty YAML string: {}", value, e);
            throw new YamlConversionException("Conversion failed to pretty YAML string from value: " + value, e);
        }
    }

    /**
     * Exception thrown when YAML conversion fails.
     */
    public static class YamlConversionException extends Exception {
        /**
         * Constructor with message and cause.
         *
         * @param message Error message
         * @param cause   Cause of the error
         */
        public YamlConversionException(String message, Throwable cause) {
            super(message, cause);
        }
    }
}
