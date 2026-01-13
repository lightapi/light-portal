package net.lightapi.portal.mutator;

import com.networknt.service.SingletonServiceFactory;
import jdk.jfr.Event;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class EventEnrichmentRegistry {
    private static final Logger logger = LoggerFactory.getLogger(EventEnrichmentRegistry.class);

    // Maps Event Type (e.g., "ConfigCreatedEvent") to its enricher instance
    private final Map<String, EventEnricher> enricherMap = new ConcurrentHashMap<>();

    // Singleton instance access
    private static final EventEnrichmentRegistry instance = new EventEnrichmentRegistry();

    private EventEnrichmentRegistry() {
        // Use Light-Platform's Service Loader to find all implementations of EventEnricher
        // Note: You must ensure all implementations are listed in your service.yml file.
        EventEnricher[] enrichers = SingletonServiceFactory.getBeans(EventEnricher.class);
        if (enrichers != null)
            Arrays.stream(enrichers).forEach(enricher -> {
                String eventType = enricher.getEventType();
                if (enricherMap.containsKey(eventType)) {
                    logger.error("Duplicate EventEnricher found for event type: {}. Ignoring class: {}", eventType, enricher.getClass().getName());
                } else {
                    enricherMap.put(eventType, enricher);
                    logger.info("Registered EventEnricher for event type: {}", eventType);
                }
            });
    }

    public static EventEnrichmentRegistry getInstance() {
        return instance;
    }

    /**
     * Dispatches the event to the registered enricher for its type.
     * @param event The event to enrich.
     * @return The enriched event.
     */
    public Map<String, Object> enrich(Map<String, Object> event) {
        String eventType = (String)event.get("type");
        EventEnricher enricher = enricherMap.get(eventType);

        if (enricher != null) {
            return enricher.enrich(event);
        }
        // If no specific enricher is found, return the original event
        return event;
    }
}
