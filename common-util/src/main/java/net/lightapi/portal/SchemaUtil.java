package net.lightapi.portal;

/**
 * A utility class that work with Json and Yaml
 *
 * @author Steve Hu
 */
public class SchemaUtil {
    /**
     * Detect if a string is JSON or not by checking the first char { | [
     * This is not a strict check but only a validation check, so further check is
     * required by loading it with either JSON or YAML.
     *
     * @param s input string
     * @return true if it is possible JSON
     *
     */
    public static boolean isJson(String s) {
        s = s.trim();
        return s.startsWith("{") || s.startsWith("[");
    }

}
