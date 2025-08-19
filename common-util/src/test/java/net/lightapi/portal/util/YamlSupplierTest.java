package net.lightapi.portal.util;

import org.junit.jupiter.api.Test;
import org.yaml.snakeyaml.Yaml;

import java.lang.reflect.Constructor;

import static org.junit.jupiter.api.Assertions.*;

class YamlSupplierTest {
    @Test
    void testYamlInstanceCreation() {
        Yaml yaml = YamlSupplier.yaml();
        assertNotNull(yaml, "Yaml instance should not be null");
    }

    @Test
    void testYamlInstanceThreadLocal() {
        Yaml yaml1 = YamlSupplier.yaml();
        Yaml yaml2 = YamlSupplier.yaml();
        assertSame(yaml1, yaml2, "Yaml instances should be the same within the same thread");
    }

    @Test
    void testYamlInstancesAcrossThreads() throws InterruptedException {
        Yaml yaml1 = YamlSupplier.yaml();

        Thread thread = new Thread(() -> {
            Yaml yaml2 = YamlSupplier.yaml();
            assertNotSame(yaml1, yaml2, "Yaml instances should be different across threads");
        });

        thread.start();
        thread.join();
    }

    @Test
    void testPrettyYamlInstanceCreation() {
        Yaml yaml = YamlSupplier.prettyYaml();
        assertNotNull(yaml, "Pretty Yaml instance should not be null");
    }

    @Test
    void testPrettyYamlInstanceThreadLocal() {
        Yaml yaml1 = YamlSupplier.prettyYaml();
        Yaml yaml2 = YamlSupplier.prettyYaml();
        assertSame(yaml1, yaml2, "Pretty Yaml instances should be the same within the same thread");
    }

    @Test
    void testPrettyYamlInstancesAcrossThreads() throws InterruptedException {
        Yaml yaml1 = YamlSupplier.prettyYaml();

        Thread thread = new Thread(() -> {
            Yaml yaml2 = YamlSupplier.prettyYaml();
            assertNotSame(yaml1, yaml2, "Pretty Yaml instances should be different across threads");
        });

        thread.start();
        thread.join();
    }

    @Test
    void testPrivateConstructor() throws Exception {
        Constructor<YamlSupplier> constructor = YamlSupplier.class.getDeclaredConstructor();
        constructor.setAccessible(false);

        assertThrows(IllegalAccessException.class, constructor::newInstance,
            "YamlSupplier should not be instantiable");
    }
}