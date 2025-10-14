package net.lightapi.portal;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.Arrays;
import java.util.HashSet;

public class EventMatcherTest {
    @Test
    void testMatchAllWhenBothNull() {
        EventMatcher matcher = new EventMatcher(null, null);
        assertTrue(matcher.isMatchAll());
        assertTrue(matcher.getExactEventTypes().isEmpty());
        assertTrue(matcher.getNormalizedAggregateTypes().isEmpty());
    }

    @Test
    void testMatchAllWhenEventTypesIsAll() {
        EventMatcher matcher = new EventMatcher("all", null);
        assertTrue(matcher.isMatchAll());
        assertTrue(matcher.getExactEventTypes().isEmpty());
        assertTrue(matcher.getNormalizedAggregateTypes().isEmpty());
    }

    @Test
    void testMatchAllWhenAggregateTypesIsAll() {
        EventMatcher matcher = new EventMatcher(null, "all");
        assertTrue(matcher.isMatchAll());
        assertTrue(matcher.getExactEventTypes().isEmpty());
        assertTrue(matcher.getNormalizedAggregateTypes().isEmpty());
    }

    @Test
    void testExactEventTypesOnly() {
        String eventTypesInput = "UserCreatedEvent,OrgUpdatedEvent";
        EventMatcher matcher = new EventMatcher(eventTypesInput, null);
        assertFalse(matcher.isMatchAll());
        assertEquals(new HashSet<>(Arrays.asList("UserCreatedEvent", "OrgUpdatedEvent")), matcher.getExactEventTypes());
        assertTrue(matcher.getNormalizedAggregateTypes().isEmpty());
    }

    @Test
    void testAggregateTypesOnly() {
        String aggregateTypesInput = "User,ConfigProperty";
        EventMatcher matcher = new EventMatcher(null, aggregateTypesInput);
        assertFalse(matcher.isMatchAll());
        assertTrue(matcher.getExactEventTypes().isEmpty());
        assertEquals(new HashSet<>(Arrays.asList("User", "ConfigProperty")), matcher.getNormalizedAggregateTypes());
    }

    @Test
    void testMixedEventAndAggregateTypes() {
        String eventTypesInput = "UserCreatedEvent,HostDeletedEvent";
        String aggregateTypesInput = "User,Service"; // "User" will be redundant but harmless
        EventMatcher matcher = new EventMatcher(eventTypesInput, aggregateTypesInput);
        assertFalse(matcher.isMatchAll());
        assertEquals(new HashSet<>(Arrays.asList("UserCreatedEvent", "HostDeletedEvent")), matcher.getExactEventTypes());
        assertEquals(new HashSet<>(Arrays.asList("User", "Service")), matcher.getNormalizedAggregateTypes());
    }

    @Test
    void testEmptyInputStrings() {
        EventMatcher matcher = new EventMatcher("", "");
        assertFalse(matcher.isMatchAll()); // Should not match all by default if explicit empty strings
        assertTrue(matcher.getExactEventTypes().isEmpty());
        assertTrue(matcher.getNormalizedAggregateTypes().isEmpty());
    }

    @Test
    void testInputWithWhitespace() {
        EventMatcher matcher = new EventMatcher("  UserCreatedEvent  ,  OrgUpdatedEvent  ", "  user  ,  host  ");
        assertEquals(new HashSet<>(Arrays.asList("UserCreatedEvent", "OrgUpdatedEvent")), matcher.getExactEventTypes());
        assertEquals(new HashSet<>(Arrays.asList("user", "host")), matcher.getNormalizedAggregateTypes());
    }
}
