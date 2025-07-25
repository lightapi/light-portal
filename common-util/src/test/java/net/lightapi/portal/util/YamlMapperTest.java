package net.lightapi.portal.util;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

import java.util.Map;
import java.util.List;
import java.math.BigDecimal;
import java.util.HashMap;
import java.util.Arrays;

class YamlMapperTest {

    @Test
    void testToMap() throws YamlMapper.YamlConversionException {
        String yaml = "key1: value1\nkey2: 42\nkey3: true";
        Map<String, Object> result = YamlMapper.toMap(yaml);

        assertEquals(3, result.size());
        assertEquals("value1", result.get("key1"));
        assertEquals(42, result.get("key2"));
        assertEquals(true, result.get("key3"));
    }

    @Test
    void testToList() throws YamlMapper.YamlConversionException {
        String yaml = "- item1\n- 42\n- true";
        List<Object> result = YamlMapper.toList(yaml);

        assertEquals(3, result.size());
        assertEquals("item1", result.get(0));
        assertEquals(42, result.get(1));
        assertEquals(true, result.get(2));
    }

    @Test
    void testToMinifiedYamlString() throws YamlMapper.YamlConversionException {
        Map<String, Object> map = new HashMap<>();
        map.put("key1", "value1");
        map.put("key2", 42);
        map.put("key3", true);

        String result = YamlMapper.toMinifiedYamlString(map);

        assertTrue(result.contains("{key1: value1, key2: 42, key3: true}"));
    }

    @Test
    void testToPrettifiedYamlString() throws YamlMapper.YamlConversionException {
        Map<String, Object> map = new HashMap<>();
        map.put("key1", "value1");
        map.put("key2", 42);
        map.put("key3", true);

        String result = YamlMapper.toPrettifiedYamlString(map);

        assertTrue(result.contains("key1: value1"));
        assertTrue(result.contains("key2: 42"));
        assertTrue(result.contains("key3: true"));
        assertTrue(result.contains("\n"));  // Check for line breaks
    }

    @Test
    void testToMapWithInvalidYaml() {
        String invalidYaml = "key1: value1\nkey2: : invalid";
        assertThrows(YamlMapper.YamlConversionException.class, () -> YamlMapper.toMap(invalidYaml));
    }

    @Test
    void testToListWithInvalidYaml() {
        String invalidYaml = "- item1\n- : invalid";
        assertThrows(YamlMapper.YamlConversionException.class, () -> YamlMapper.toList(invalidYaml));
    }

    @Test
    void testToMinifiedYamlStringWithComplexObject() throws YamlMapper.YamlConversionException {
        Map<String, Object> complexMap = new HashMap<>();
        complexMap.put("string", "value");
        complexMap.put("integer", 42);
        complexMap.put("boolean", true);
        complexMap.put("list", Arrays.asList("item1", "item2"));
        complexMap.put("nested", new HashMap<String, Object>() {{
            put("nestedKey", "nestedValue");
        }});
        complexMap.put("bigDecimal", new BigDecimal("123.456"));

        String result = YamlMapper.toMinifiedYamlString(complexMap);

        assertTrue(result.contains("{"));
        assertTrue(result.contains("}"));
        assertTrue(result.contains("string: value"));
        assertTrue(result.contains("integer: 42"));
        assertTrue(result.contains("boolean: true"));
        assertTrue(result.contains("list: [item1, item2]"));
        assertTrue(result.contains("nested: {nestedKey: nestedValue}"));
        assertTrue(result.contains("bigDecimal: 123.456"));
    }

    @Test
    void testToPrettifiedYamlStringWithComplexObject() throws YamlMapper.YamlConversionException {
        Map<String, Object> complexMap = new HashMap<>();
        complexMap.put("string", "value");
        complexMap.put("integer", 42);
        complexMap.put("boolean", true);
        complexMap.put("list", Arrays.asList("item1", "item2"));
        complexMap.put("nested", new HashMap<String, Object>() {{
            put("nestedKey", "nestedValue");
        }});
        complexMap.put("bigDecimal", new BigDecimal("123.456"));

        String result = YamlMapper.toPrettifiedYamlString(complexMap);

        assertTrue(result.contains("string: value"));
        assertTrue(result.contains("integer: 42"));
        assertTrue(result.contains("boolean: true"));
        assertTrue(result.contains("list:\n- item1\n- item2"));
        assertTrue(result.contains("nested:\n  nestedKey: nestedValue"));
        assertTrue(result.contains("bigDecimal: '123.456'"));
        assertTrue(result.contains("\n"));  // Check for line breaks
    }
}