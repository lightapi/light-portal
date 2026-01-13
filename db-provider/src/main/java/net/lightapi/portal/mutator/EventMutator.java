package net.lightapi.portal.mutator;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.networknt.config.Config;
import com.networknt.config.JsonMapper;
import com.networknt.utility.UuidUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class EventMutator {
    private static final Logger logger = LoggerFactory.getLogger(EventMutator.class);
    private final ObjectMapper mapper = Config.getInstance().getMapper();

    // Parsed list of rules
    private final List<Map<String, Object>> replacementRules;
    private final List<Map<String, Object>> enrichmentRules;

    // Map to track generated UUIDs for consistent replacement across events (e.g., old user ID -> new user ID)
    private final Map<String, String> generatedIdMap = new HashMap<>();

    public EventMutator(String replacementJson, String enrichmentJson) {
        this.replacementRules = parseRules(replacementJson);
        this.enrichmentRules = parseRules(enrichmentJson);
    }

    public EventMutator(List<Map<String, Object>> replacementRules, List<Map<String, Object>> enrichmentRules) {
        this.replacementRules = replacementRules;
        this.enrichmentRules = enrichmentRules;
    }

    private List<Map<String, Object>> parseRules(String json) {
        if (json == null || json.isEmpty()) return Collections.emptyList();
        try {
            return mapper.readValue(json, new TypeReference<List<Map<String, Object>>>() {});
        } catch (IOException e) {
            logger.error("Failed to parse mutation rules JSON: {}", json, e);
            throw new IllegalArgumentException("Invalid JSON format for mutation rules.", e);
        }
    }

    /**
     * Applies all replacement and enrichment rules to a single CloudEvent.
     * @param json The original CloudEvent in json.
     */
    public String mutate(String json) {
        boolean hasReplacements = replacementRules != null && !replacementRules.isEmpty();
        boolean hasEnrichments = enrichmentRules != null && !enrichmentRules.isEmpty();

        if (!hasReplacements && !hasEnrichments) {
            return json;
        } else {
            Map<String, Object> map = JsonMapper.string2Map(json);

            // 1. Apply Replacements only if replacementRules is not null and not empty
            if (hasReplacements) {
                applyReplacements(map);
            }

            // 2. Apply Enrichments only if enrichmentRules is not null and not empty
            if (hasEnrichments) {
                applyEnrichments(map);
            }

            return JsonMapper.toJson(map);
        }
    }

    // --- Private Mutation Helpers ---
    private void applyReplacements(Map<String, Object> map) {
        for (Map<String, Object> rule : replacementRules) {
            String field = (String)rule.get("fieldName");
            String from = (String)rule.get("fromValue");
            String to = (String)rule.get("toValue");
            if (field == null || from == null || to == null) continue;

            if (map != null && map.containsKey(field) && map.get(field) != null && map.get(field).toString().equals(from)) {
                map.put(field, to);
                logger.debug("Replaced field {} from {} to {}", field, from, to);
            }
        }
    }

    private void applyEnrichments(Map<String, Object> map) {
        assert map != null;
        // enrichment rule execution
        for (Map<String, Object> rule : enrichmentRules) {
            String field = (String)rule.get("fieldName");
            String action = (String)rule.get("action");
            if (field == null || action == null) continue;

            String generatedId = null;

            // id generation
            if ("generateUUID".equalsIgnoreCase(action)) {
                // Generate and cache a new UUID for the whole import run if needed, or always generate new.
                // For simplicity, we assume we generate a new UUID for the field.
                generatedId = UuidUtil.getUUID().toString();
            } else if ("mapGenerate".equalsIgnoreCase(action)) {
                String sourceField = (String)rule.get("sourceField");
                String originalId = null;

                // Get the original ID from a source field in the data payload (e.g., from an 'oldUserId' field)
                if (sourceField != null && map.containsKey(sourceField)) {
                    originalId = map.get(sourceField).toString();
                }

                if (originalId != null) {
                    // Check cache for consistency (e.g., ensure old_user_ID_A always maps to new_user_ID_X)
                    generatedId = generatedIdMap.computeIfAbsent(field + ":" + originalId, k -> UuidUtil.getUUID().toString());
                    logger.debug("Mapped original ID {} to new ID {}", originalId, generatedId);
                } else {
                    // Cannot map, fall back to simple UUID generation if allowed
                    generatedId = UuidUtil.getUUID().toString();
                }
            }

            if (generatedId != null) {
                // Mutate Data Payload
                if (map.containsKey(field)) {
                    map.put(field, generatedId);
                }
                logger.debug("Enriched field {} with new ID {}", field, generatedId);
            }
        }

        // enricher execution for event type
        EventEnrichmentRegistry registry = EventEnrichmentRegistry.getInstance();
        if(logger.isTraceEnabled()) logger.trace("map before enricher {}", map);
        registry.enrich(map);
        if(logger.isTraceEnabled()) logger.trace("map after enricher {}", map);
    }
}
