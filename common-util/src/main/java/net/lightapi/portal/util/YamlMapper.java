package net.lightapi.portal.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;

public final class YamlMapper {

    private static final Logger LOGGER = LoggerFactory.getLogger(YamlMapper.class);

    private YamlMapper() {
        // Suppress instantiation
    }

    public static Map<String, Object> toMap(String value) throws YamlConversionException {
        try {
            return YamlSupplier.yaml().load(value);
        } catch (Exception e) {
            LOGGER.warn("Failed to parse map from value: {}", value, e);
            throw new YamlConversionException("Conversion failed to map from value: " + value, e);
        }
    }

    public static List<Object> toList(String value) throws YamlConversionException {
        try {
            return YamlSupplier.yaml().load(value);
        } catch (Exception e) {
            LOGGER.warn("Failed to parse list from value: {}", value, e);
            throw new YamlConversionException("Conversion failed to list from value: " + value, e);
        }
    }

    public static String toMinifiedYamlString(Object value) throws YamlConversionException {
        try {
            return YamlSupplier.yaml().dump(value);
        } catch (Exception e) {
            LOGGER.warn("Failed to convert value to minified YAML string: {}", value, e);
            throw new YamlConversionException("Conversion failed to minified YAML string from value: " + value, e);
        }
    }

    public static String toPrettifiedYamlString(Object value) throws YamlConversionException {
        try {
            return YamlSupplier.prettyYaml().dump(value);
        } catch (Exception e) {
            LOGGER.warn("Failed to convert value to pretty YAML string: {}", value, e);
            throw new YamlConversionException("Conversion failed to pretty YAML string from value: " + value, e);
        }
    }

    public static class YamlConversionException extends Exception {
        public YamlConversionException(String message, Throwable cause) {
            super(message, cause);
        }
    }
}
