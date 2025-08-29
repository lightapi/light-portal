package net.lightapi.portal.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.yaml.snakeyaml.DumperOptions;
import org.yaml.snakeyaml.Yaml;
import org.yaml.snakeyaml.introspector.BeanAccess;
import org.yaml.snakeyaml.nodes.Tag;
import org.yaml.snakeyaml.representer.Representer;

import java.math.BigDecimal;

/**
 * YamlSupplier is a utility class that provides a thread-local instance of Yaml.
 * This is useful for applications that need to serialize or deserialize YAML data
 * in a thread-safe manner without creating multiple instances of Yaml.
 * <br/>
 * Note: As Snake Yaml is not thread-safe, we use a ThreadLocal to ensure that each thread
 * gets their own Yaml instances.
 */
public final class YamlSupplier {

    private static final Logger LOGGER = LoggerFactory.getLogger(YamlSupplier.class);

    /*
     * ThreadLocal holder for Yaml instance.
     * As Snake Yaml is not thread-safe, we use ThreadLocal to ensure that each thread
     * has its own instance of Yaml, thus avoiding concurrency issues by using
     * thread confinement.
     */
    private static final ThreadLocal<Yaml> YAML_THREAD_LOCAL_HOLDER = ThreadLocal.withInitial(() -> {
        LOGGER.trace("Thread local yaml that dumps minified yaml created from thread {}", Thread.currentThread());
        DumperOptions dumperOptions = new DumperOptions();
        dumperOptions.setDefaultFlowStyle(DumperOptions.FlowStyle.FLOW);
        dumperOptions.setPrettyFlow(false);
        dumperOptions.setSplitLines(false);
        return new Yaml(dumperOptions);
    });

    private static final ThreadLocal<Yaml> PRETTY_YAML_THREAD_LOCAL_HOLDER = ThreadLocal.withInitial(() -> {
        LOGGER.trace("Thread local yaml that dumps prettified yaml created from thread {}", Thread.currentThread());
        DumperOptions options = new DumperOptions();
        options.setDefaultFlowStyle(DumperOptions.FlowStyle.BLOCK);
        options.setIndent(2);
        options.setSplitLines(false);
        options.setPrettyFlow(true);

        Representer representer = new Representer(options);
        representer.addClassTag(BigDecimal.class, Tag.STR);
        // Configure to skip null values during serialization
        representer.getPropertyUtils().setSkipMissingProperties(true);

        Yaml yaml = new Yaml(representer, options);
        // Configure to ignore unknown properties during deserialization
        yaml.setBeanAccess(BeanAccess.FIELD);
        return yaml;

    });

    private YamlSupplier() {
        // suppress instantiation
    }

    /**
     * Returns a thread-local instance of Yaml.
     * This ensures that each thread has its own instance, avoiding concurrency issues.
     *
     * @return a Yaml instance
     */
    public static Yaml yaml() {
        return YAML_THREAD_LOCAL_HOLDER.get();
    }

    public static Yaml prettyYaml() {
        return PRETTY_YAML_THREAD_LOCAL_HOLDER.get();
    }
}
