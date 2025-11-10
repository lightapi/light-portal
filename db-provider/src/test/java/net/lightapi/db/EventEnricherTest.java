package net.lightapi.db;

import com.networknt.db.provider.DbProvider;
import com.networknt.db.provider.SqlDbStartupHook;
import com.networknt.service.SingletonServiceFactory;
import com.networknt.utility.UuidUtil;
import net.lightapi.portal.PortalConstants;
import net.lightapi.portal.db.PortalDbProvider;
import net.lightapi.portal.mutator.EventEnrichmentRegistry;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.OffsetDateTime;
import java.util.HashMap;
import java.util.Map;

@Disabled
public class EventEnricherTest {
    private static final Logger logger = LoggerFactory.getLogger(EventEnricherTest.class);

    public static SqlDbStartupHook sqlDbStartupHook;
    @BeforeAll
    static void init() {
        sqlDbStartupHook = new SqlDbStartupHook();
        sqlDbStartupHook.onStartup();
    }

    @Test
    void testProductVersionConfigEnricher() {
        Map<String, Object> originalEvent = new HashMap<>();
        originalEvent.put("id", UuidUtil.getUUID().toString());
        originalEvent.put("type", "ProductVersionConfigCreatedEvent");
        originalEvent.put("host", "01964b05-552a-7c4b-9184-6857e7f3dc5f"); // can be any host
        originalEvent.put("time", OffsetDateTime.now().toString()); // use current time
        originalEvent.put("user", "0199543e-d115-7ae0-8bf9-cba1bbdad45f");
        originalEvent.put("nonce", 1); // will be replaced by the importer
        originalEvent.put("source", "https://github.com/lightapi/light-portal");
        // originalEvent.put("subject", ""); needs to be enriched.
        originalEvent.put("specversion", "1.0");
        originalEvent.put("aggregatetype", "ProductVersionConfig");
        // originalEvent.put("aggregateversion", 1); needs to be enriched.
        Map<String, Object> data = new HashMap<>();
        originalEvent.put(PortalConstants.DATA, data);
        data.put("hostId", "01964b05-552a-7c4b-9184-6857e7f3dc5f");
        data.put("productId", "lps");
        data.put("light4jVersion", "2.1.38");
        data.put("configName", "access-control");

        EventEnrichmentRegistry registry = EventEnrichmentRegistry.getInstance();
        if(logger.isTraceEnabled())logger.trace("map before enricher {}",originalEvent);
        registry.enrich(originalEvent);
        if(logger.isTraceEnabled())logger.trace("map after enricher {}",originalEvent);
    }

    @Test
    void testProductVersionConfigPropertyEnricher() {
        Map<String, Object> originalEvent = new HashMap<>();
        originalEvent.put("id", UuidUtil.getUUID().toString());
        originalEvent.put("type", "ProductVersionConfigPropertyCreatedEvent");
        originalEvent.put("host", "01964b05-552a-7c4b-9184-6857e7f3dc5f"); // can be default host
        originalEvent.put("time", OffsetDateTime.now().toString()); // use current time
        originalEvent.put("user", "0199543e-d115-7ae0-8bf9-cba1bbdad45f");
        originalEvent.put("nonce", 1); // will be replaced by the importer
        originalEvent.put("source", "https://github.com/lightapi/light-portal");
        // originalEvent.put("subject", ""); needs to be enriched.
        originalEvent.put("specversion", "1.0");
        originalEvent.put("aggregatetype", "ProductVersionConfigProperty");
        // originalEvent.put("aggregateversion", 1); needs to be enriched.
        Map<String, Object> data = new HashMap<>();
        originalEvent.put(PortalConstants.DATA, data);
        data.put("hostId", "01964b05-552a-7c4b-9184-6857e7f3dc5f");
        data.put("productId", "lps");
        data.put("light4jVersion", "2.1.38");
        data.put("configName", "access-control");
        data.put("propertyName", "defaultDeny");

        EventEnrichmentRegistry registry = EventEnrichmentRegistry.getInstance();
        if(logger.isTraceEnabled())logger.trace("map before enricher {}",originalEvent);
        registry.enrich(originalEvent);
        if(logger.isTraceEnabled())logger.trace("map after enricher {}",originalEvent);

    }

}
