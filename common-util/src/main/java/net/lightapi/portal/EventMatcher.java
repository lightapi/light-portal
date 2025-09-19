package net.lightapi.portal;

import java.util.Arrays;
import java.util.Collections;
import java.util.Set;
import java.util.stream.Collectors;

public class EventMatcher {
    private final Set<String> exactEventTypes;
    private final Set<String> normalizedAggregateTypes; // Already lowercase
    private final boolean matchAll;

    public EventMatcher(String eventTypesInput, String aggregateTypesInput) {
        // Handle "all" explicitly first
        boolean eventTypesAll = eventTypesInput != null && Arrays.stream(eventTypesInput.split(",")).anyMatch("all"::equalsIgnoreCase);
        boolean aggregateTypesAll = aggregateTypesInput != null && Arrays.stream(aggregateTypesInput.split(",")).anyMatch("all"::equalsIgnoreCase);

        this.matchAll = eventTypesAll || aggregateTypesAll || (eventTypesInput == null && aggregateTypesInput == null); // If neither specified, assume all

        if (this.matchAll) {
            this.exactEventTypes = Collections.emptySet();
            this.normalizedAggregateTypes = Collections.emptySet();
        } else {
            // Process eventTypesInput
            this.exactEventTypes = eventTypesInput != null && !eventTypesInput.isEmpty()
                    ? Arrays.stream(eventTypesInput.split(","))
                    .map(String::trim)
                    .filter(s -> !s.isEmpty())
                    .collect(Collectors.toSet())
                    : Collections.emptySet();

            // Process aggregateTypesInput (normalize to lowercase for DB comparison)
            this.normalizedAggregateTypes = aggregateTypesInput != null && !aggregateTypesInput.isEmpty()
                    ? Arrays.stream(aggregateTypesInput.split(","))
                    .map(String::trim)
                    .filter(s -> !s.isEmpty())
                    .map(String::toLowerCase) // Store lowercase
                    .collect(Collectors.toSet())
                    : Collections.emptySet();
        }
    }

    public boolean isMatchAll() {
        return matchAll;
    }

    public Set<String> getExactEventTypes() {
        return exactEventTypes;
    }

    public Set<String> getNormalizedAggregateTypes() {
        return normalizedAggregateTypes;
    }
}
