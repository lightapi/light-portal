package net.lightapi.portal;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

public class EventTypeUtil {

    private static final Logger logger = LoggerFactory.getLogger(EventTypeUtil.class);

    // Define common event suffixes that indicate the operation
    private static final String[] EVENT_SUFFIXES = {
            "CreatedEvent",
            "UpdatedEvent",
            "DeletedEvent",
            "OnboardedEvent",
            "ConfirmedEvent",
            "VerifiedEvent",
            "ForgotEvent",
            "ResetEvent",
            "ChangedEvent",
            "LockedEvent",
            "UnlockedEvent",
            "CancelledEvent",
            "DeliveredEvent",
            "SwitchedEvent",
            "SentEvent",
            "RotatedEvent",
            "QueriedEvent",
            "ClonedEvent"
    };

    /**
     * Derives the aggregate type from a CloudEvent type string.
     * It assumes the aggregate type is the part of the event type before the operation suffix (e.g., "Config" from "ConfigCreatedEvent").
     *
     * @param eventType The CloudEvent type string (e.g., "ConfigCreatedEvent", "ServiceVersionUpdatedEvent").
     * @return The derived aggregate type (e.g., "Config", "ServiceVersion"), or null if it cannot be derived consistently.
     */
    public static String deriveAggregateTypeFromEventType(String eventType) {
        if (eventType == null || eventType.isEmpty()) {
            logger.warn("Attempted to derive aggregate type from null or empty eventType.");
            return null;
        }

        for (String suffix : EVENT_SUFFIXES) {
            if (eventType.endsWith(suffix)) {
                String aggregateType = eventType.substring(0, eventType.length() - suffix.length());
                // Basic validation: ensure it's not empty after removing suffix
                if (!aggregateType.isEmpty()) {
                    return aggregateType;
                }
            }
        }

        // Handle cases that don't match standard suffixes, or specific exceptions
        // For example, "PlatformQueriedEvent" might conceptually be a "Platform" aggregate.
        // If not found by suffix, you might have specific hardcoded mappings for exceptions:
        if (eventType.equals("PlatformQueriedEvent")) {
            return "Platform"; // Example of a specific mapping if it doesn't fit the suffix pattern
        }
        // ... add more specific mappings if needed ...

        logger.warn("Could not derive aggregate type for eventType: {}", eventType);
        return null; // Return null if aggregate type cannot be determined
    }

    public static String getAggregateId(String eventType, Map<String, Object> dataMap) {
        String aggregateType = deriveAggregateTypeFromEventType(eventType);
        if (aggregateType == null) {
            logger.warn("Cannot determine aggregate type from event type: {}", eventType);
            return null;
        }
        // Define a mapping from aggregate types to their corresponding ID fields in the data map
        return switch (aggregateType) {
            case PortalConstants.AGGREGATE_CONFIG -> {
                String configId = (String) dataMap.get("configId");
                if (configId == null) {
                    logger.warn("configId is null in data map for aggregate type: {}", aggregateType);
                    yield null;
                }
                if(logger.isTraceEnabled()) logger.trace("Derived aggregateId for Config: {}", configId);
                yield configId;
            }
            case PortalConstants.AGGREGATE_CONFIG_PROPERTY -> {
                String propertyId = (String) dataMap.get("propertyId");
                if (propertyId == null) {
                    logger.warn("propertyId is null in data map for aggregate type: {}", aggregateType);
                    yield null;
                }
                if(logger.isTraceEnabled()) logger.trace("Derived aggregateId for ConfigProperty: {}", propertyId);
                yield propertyId;
            }
            case PortalConstants.AGGREGATE_REF_TABLE -> {
                String tableId = (String) dataMap.get("tableId");
                if (tableId == null) {
                    logger.warn("tableId is null in data map for aggregate type: {}", aggregateType);
                    yield null;
                }
                if(logger.isTraceEnabled()) logger.trace("Derived aggregateId for RefTable: {}", tableId);
                yield tableId;
            }
            case PortalConstants.AGGREGATE_REF_VALUE -> {
                String valueId = (String) dataMap.get("valueId");
                if (valueId == null) {
                    logger.warn("valueId is null in data map for aggregate type: {}", aggregateType);
                    yield null;
                }
                if(logger.isTraceEnabled()) logger.trace("Derived aggregateId for RefValue: {}", valueId);
                yield valueId;
            }
            case PortalConstants.AGGREGATE_REF_LOCALE -> {
                String valueId = (String) dataMap.get("valueId");
                String language = (String) dataMap.get("language");
                if (valueId == null || language == null) {
                    logger.warn("valueId or language is null in data map for aggregate type: {}", aggregateType);
                    yield null;
                }
                String localeId = valueId + "|" + language;
                if(logger.isTraceEnabled()) logger.trace("Derived aggregateId for RefLocale: {}", localeId);
                yield localeId;
            }
            case PortalConstants.AGGREGATE_REF_RELATION_TYPE -> {
                String relationId = (String) dataMap.get("relationId");
                if (relationId == null) {
                    logger.warn("relationId is null in data map for aggregate type: {}", aggregateType);
                    yield null;
                }
                if(logger.isTraceEnabled()) logger.trace("Derived aggregateId for RefRelationType: {}", relationId);
                yield relationId;
            }
            case PortalConstants.AGGREGATE_REF_RELATION -> {
                String relationId = (String) dataMap.get("relationId");
                String valueIdFrom = (String) dataMap.get("valueIdFrom");
                String valueIdTo = (String) dataMap.get("valueIdTo");
                if (relationId == null || valueIdFrom == null || valueIdTo == null) {
                    logger.warn("relationId or valueIdFrom or valueIdTo is null in data map for aggregate type: {}", aggregateType);
                    yield null;
                }
                String id = relationId + "|" +valueIdFrom + "|" + valueIdTo;
                if(logger.isTraceEnabled()) logger.trace("Derived aggregateId for RefRelation: {}", id);
                yield id;
            }
            case "ServiceVersion" -> (String) dataMap.get("serviceVersionId");
            case "Service" -> (String) dataMap.get("serviceId");
            case "Instance" -> (String) dataMap.get("instanceId");
            case "Host" -> (String) dataMap.get("hostId");
            case "User" -> (String) dataMap.get("userId");
            case "Role" -> (String) dataMap.get("roleId");
            case "Group" -> (String) dataMap.get("groupId");
            case "Permission" -> (String) dataMap.get("permissionId");
            case "ApiKey" -> (String) dataMap.get("apiKeyId");
            case "AuditLog" -> (String) dataMap.get("auditLogId");
            case "PasswordReset" -> (String) dataMap.get("resetId");
            case "Invitation" -> (String) dataMap.get("invitationId");
            case "Platform" -> "platform"; // Singleton aggregate, fixed ID
            case "ConfigInstance" -> (String) dataMap.get("configInstanceId");
            // Add more cases as needed for other aggregate types
            default -> {
                logger.warn("No aggregate ID mapping defined for aggregate type: {}", aggregateType);
                yield null;
            }
        };
    }
}
