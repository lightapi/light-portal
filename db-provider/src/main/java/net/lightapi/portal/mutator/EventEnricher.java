package net.lightapi.portal.mutator;

import java.util.Map;

public interface EventEnricher {

    /**
     * The type of event this enricher is designed to handle.
     * This is used by the EventEnrichmentRegistry to dispatch the correct enricher.
     * @return The String event type (e.g., "ServiceVersionCreatedEvent").
     */
    String getEventType();

    /**
     * Applies enrichment logic to the CloudEvent.
     *
     * @param event The CloudEvent to be mutated/enriched in map format.
     * @return The enriched event (or the original if no changes were made).
     */
    Map<String, Object> enrich(Map<String, Object> event);
}
