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
                if(logger.isTraceEnabled()) logger.trace("Derived aggregateId for {}: {}", aggregateType, configId);
                yield configId;
            }
            case PortalConstants.AGGREGATE_CONFIG_PROPERTY -> {
                String propertyId = (String) dataMap.get("propertyId");
                if (propertyId == null) {
                    logger.warn("propertyId is null in data map for aggregate type: {}", aggregateType);
                    yield null;
                }
                if(logger.isTraceEnabled()) logger.trace("Derived aggregateId for {}: {}", aggregateType, propertyId);
                yield propertyId;
            }
            case PortalConstants.AGGREGATE_CONFIG_DEPLOYMENT_INSTANCE -> {
                String deploymentInstanceId = (String) dataMap.get("deploymentInstanceId");
                String propertyId = (String) dataMap.get("propertyId");
                if (deploymentInstanceId == null || propertyId == null) {
                    logger.warn("deploymentInstanceId or propertyId is null in data map for aggregate type: {}", aggregateType);
                    yield null;
                }
                String id = deploymentInstanceId + "|" + propertyId;
                if(logger.isTraceEnabled()) logger.trace("Derived aggregateId for {}: {}", aggregateType, id);
                yield id;
            }
            case PortalConstants.AGGREGATE_CONFIG_ENVIRONMENT -> {
                String environment = (String) dataMap.get("environment");
                String propertyId = (String) dataMap.get("propertyId");
                if (environment == null || propertyId == null) {
                    logger.warn("environment or propertyId is null in data map for aggregate type: {}", aggregateType);
                    yield null;
                }
                String id = environment + "|" + propertyId;
                if(logger.isTraceEnabled()) logger.trace("Derived aggregateId for {}: {}", aggregateType, id);
                yield id;
            }
            case PortalConstants.AGGREGATE_CONFIG_INSTANCE -> {
                String instanceId = (String) dataMap.get("instanceId");
                String propertyId = (String) dataMap.get("propertyId");
                if (instanceId == null || propertyId == null) {
                    logger.warn("instanceId or propertyId is null in data map for aggregate type: {}", aggregateType);
                    yield null;
                }
                String id = instanceId + "|" + propertyId;
                if(logger.isTraceEnabled()) logger.trace("Derived aggregateId for {}: {}", aggregateType, id);
                yield id;
            }
            case PortalConstants.AGGREGATE_CONFIG_INSTANCE_API -> {
                String instanceApiId = (String) dataMap.get("instanceApiId");
                String propertyId = (String) dataMap.get("propertyId");
                if (instanceApiId == null || propertyId == null) {
                    logger.warn("instanceApiId or propertyId is null in data map for aggregate type: {}", aggregateType);
                    yield null;
                }
                String id = instanceApiId + "|" + propertyId;
                if(logger.isTraceEnabled()) logger.trace("Derived aggregateId for {}: {}", aggregateType, id);
                yield id;
            }
            case PortalConstants.AGGREGATE_CONFIG_INSTANCE_APP -> {
                String instanceAppId = (String) dataMap.get("instanceAppId");
                String propertyId = (String) dataMap.get("propertyId");
                if (instanceAppId == null || propertyId == null) {
                    logger.warn("instanceAppId or propertyId is null in data map for aggregate type: {}", aggregateType);
                    yield null;
                }
                String id = instanceAppId + "|" + propertyId;
                if(logger.isTraceEnabled()) logger.trace("Derived aggregateId for {}: {}", aggregateType, id);
                yield id;
            }
            case PortalConstants.AGGREGATE_CONFIG_INSTANCE_APP_API -> {
                String instanceAppId = (String) dataMap.get("instanceAppId");
                String instanceApiId = (String) dataMap.get("instanceApiId");
                String propertyId = (String) dataMap.get("propertyId");
                if (instanceAppId == null || instanceApiId == null || propertyId == null) {
                    logger.warn("instanceAppId or instanceApi or propertyId is null in data map for aggregate type: {}", aggregateType);
                    yield null;
                }
                String id = instanceAppId + "|" + instanceApiId + "|" + propertyId;
                if(logger.isTraceEnabled()) logger.trace("Derived aggregateId for {}: {}", aggregateType, id);
                yield id;
            }
            case PortalConstants.AGGREGATE_CONFIG_INSTANCE_FILE -> {
                String instanceFileId = (String) dataMap.get("instanceFileId");
                if (instanceFileId == null) {
                    logger.warn("instanceFileId is null in data map for aggregate type: {}", aggregateType);
                    yield null;
                }
                if(logger.isTraceEnabled()) logger.trace("Derived aggregateId for {}: {}", aggregateType, instanceFileId);
                yield instanceFileId;
            }
            case PortalConstants.AGGREGATE_CONFIG_PRODUCT -> {
                String productId = (String) dataMap.get("productId");
                String propertyId = (String) dataMap.get("propertyId");
                if (productId == null || propertyId == null) {
                    logger.warn("productId or propertyId is null in data map for aggregate type: {}", aggregateType);
                    yield null;
                }
                String id = productId + "|" + propertyId;
                if(logger.isTraceEnabled()) logger.trace("Derived aggregateId for {}: {}", aggregateType, id);
                yield id;
            }
            case PortalConstants.AGGREGATE_CONFIG_PRODUCT_VERSION -> {
                String productVersionId = (String) dataMap.get("productVersionId");
                String propertyId = (String) dataMap.get("propertyId");
                if (productVersionId == null || propertyId == null) {
                    logger.warn("productVersionId or propertyId is null in data map for aggregate type: {}", aggregateType);
                    yield null;
                }
                String id = productVersionId + "|" + propertyId;
                if(logger.isTraceEnabled()) logger.trace("Derived aggregateId for {}: {}", aggregateType, id);
                yield id;
            }
            case PortalConstants.AGGREGATE_REF_TABLE -> {
                String tableId = (String) dataMap.get("tableId");
                if (tableId == null) {
                    logger.warn("tableId is null in data map for aggregate type: {}", aggregateType);
                    yield null;
                }
                if(logger.isTraceEnabled()) logger.trace("Derived aggregateId for {}: {}", aggregateType, tableId);
                yield tableId;
            }
            case PortalConstants.AGGREGATE_REF_VALUE -> {
                String valueId = (String) dataMap.get("valueId");
                if (valueId == null) {
                    logger.warn("valueId is null in data map for aggregate type: {}", aggregateType);
                    yield null;
                }
                if(logger.isTraceEnabled()) logger.trace("Derived aggregateId for {}: {}", aggregateType, valueId);
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
                if(logger.isTraceEnabled()) logger.trace("Derived aggregateId for {}: {}", aggregateType, localeId);
                yield localeId;
            }
            case PortalConstants.AGGREGATE_REF_RELATION_TYPE -> {
                String relationId = (String) dataMap.get("relationId");
                if (relationId == null) {
                    logger.warn("relationId is null in data map for aggregate type: {}", aggregateType);
                    yield null;
                }
                if(logger.isTraceEnabled()) logger.trace("Derived aggregateId for {}: {}", aggregateType, relationId);
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
                if(logger.isTraceEnabled()) logger.trace("Derived aggregateId for {}: {}", aggregateType, id);
                yield id;
            }
            case PortalConstants.AGGREGATE_SERVICE -> {
                String apiId = (String) dataMap.get("apiId");
                if (apiId == null) {
                    logger.warn("apiId is null in data map for aggregate type: {}", aggregateType);
                    yield null;
                }
                if(logger.isTraceEnabled()) logger.trace("Derived aggregateId for {}: {}", aggregateType, apiId);
                yield apiId;
            }
            case PortalConstants.AGGREGATE_SERVICE_VERSION -> {
                String apiVersionId = (String) dataMap.get("apiVersionId");
                if (apiVersionId == null) {
                    logger.warn("apiVersionId is null in data map for aggregate type: {}", aggregateType);
                    yield null;
                }
                if(logger.isTraceEnabled()) logger.trace("Derived aggregateId for {}: {}", aggregateType, apiVersionId);
                yield apiVersionId;
            }
            case PortalConstants.AGGREGATE_SERVICE_SPEC -> {
                String apiVersionId = (String) dataMap.get("apiVersionId");
                if (apiVersionId == null) {
                    logger.warn("apiVersionId is null in data map for aggregate type: {}", aggregateType);
                    yield null;
                }
                if(logger.isTraceEnabled()) logger.trace("Derived aggregateId for {}: {}", aggregateType, apiVersionId);
                yield apiVersionId;
            }
            case PortalConstants.AGGREGATE_ENDPOINT_RULE -> {
                String endpointId = (String) dataMap.get("endpointId");
                String ruleId = (String) dataMap.get("ruleId");
                if (endpointId == null || ruleId == null) {
                    logger.warn("endpointId or ruleId is null in data map for aggregate type: {}", aggregateType);
                    yield null;
                }
                String id = endpointId + "|" + ruleId;
                if(logger.isTraceEnabled()) logger.trace("Derived aggregateId for {}: {}", aggregateType, id);
                yield id;
            }
            case PortalConstants.AGGREGATE_RULE -> {
                String ruleId = (String) dataMap.get("ruleId");
                if (ruleId == null) {
                    logger.warn("ruleId is null in data map for aggregate type: {}", aggregateType);
                    yield null;
                }
                if(logger.isTraceEnabled()) logger.trace("Derived aggregateId for {}: {}", aggregateType, ruleId);
                yield ruleId;
            }
            case PortalConstants.AGGREGATE_ROLE -> {
                String roleId = (String) dataMap.get("roleId");
                if (roleId == null) {
                    logger.warn("roleId is null in data map for aggregate type: {}", aggregateType);
                    yield null;
                }
                if(logger.isTraceEnabled()) logger.trace("Derived aggregateId for {}: {}", aggregateType, roleId);
                yield roleId;
            }
            case PortalConstants.AGGREGATE_ROLE_PERMISSION -> {
                String roleId = (String) dataMap.get("roleId");
                String endpointId = (String) dataMap.get("endpointId");
                if (endpointId == null || roleId == null) {
                    logger.warn("roleId or endpointId is null in data map for aggregate type: {}", aggregateType);
                    yield null;
                }
                String id = roleId + "|" + endpointId;
                if(logger.isTraceEnabled()) logger.trace("Derived aggregateId for {}: {}", aggregateType, id);
                yield id;
            }
            case PortalConstants.AGGREGATE_ROLE_USER -> {
                String roleId = (String) dataMap.get("roleId");
                String userId = (String) dataMap.get("userId");
                if (userId == null || roleId == null) {
                    logger.warn("roleId or userId is null in data map for aggregate type: {}", aggregateType);
                    yield null;
                }
                String id = roleId + "|" + userId;
                if(logger.isTraceEnabled()) logger.trace("Derived aggregateId for {}: {}", aggregateType, id);
                yield id;
            }
            case PortalConstants.AGGREGATE_ROLE_ROW_FILTER -> {
                String roleId = (String) dataMap.get("roleId");
                String endpointId = (String) dataMap.get("endpointId");
                String colName = (String) dataMap.get("colName");
                if (endpointId == null || roleId == null || colName == null) {
                    logger.warn("roleId or endpointId or colName is null in data map for aggregate type: {}", aggregateType);
                    yield null;
                }
                String id = roleId + "|" + endpointId + "|" + colName;
                if(logger.isTraceEnabled()) logger.trace("Derived aggregateId for {}: {}", aggregateType, id);
                yield id;
            }
            case PortalConstants.AGGREGATE_ROLE_COL_FILTER -> {
                String roleId = (String) dataMap.get("roleId");
                String endpointId = (String) dataMap.get("endpointId");
                if (endpointId == null || roleId == null) {
                    logger.warn("roleId or endpointId is null in data map for aggregate type: {}", aggregateType);
                    yield null;
                }
                String id = roleId + "|" + endpointId;
                if(logger.isTraceEnabled()) logger.trace("Derived aggregateId for {}: {}", aggregateType, id);
                yield id;
            }
            case PortalConstants.AGGREGATE_GROUP -> {
                String groupId = (String) dataMap.get("groupId");
                if (groupId == null) {
                    logger.warn("groupId is null in data map for aggregate type: {}", aggregateType);
                    yield null;
                }
                if(logger.isTraceEnabled()) logger.trace("Derived aggregateId for {}: {}", aggregateType, groupId);
                yield groupId;
            }
            case PortalConstants.AGGREGATE_GROUP_PERMISSION -> {
                String groupId = (String) dataMap.get("groupId");
                String endpointId = (String) dataMap.get("endpointId");
                if (endpointId == null || groupId == null) {
                    logger.warn("groupId or endpointId is null in data map for aggregate type: {}", aggregateType);
                    yield null;
                }
                String id = groupId + "|" + endpointId;
                if(logger.isTraceEnabled()) logger.trace("Derived aggregateId for {}: {}", aggregateType, id);
                yield id;
            }
            case PortalConstants.AGGREGATE_GROUP_USER -> {
                String groupId = (String) dataMap.get("groupId");
                String userId = (String) dataMap.get("userId");
                if (userId == null || groupId == null) {
                    logger.warn("groupId or userId is null in data map for aggregate type: {}", aggregateType);
                    yield null;
                }
                String id = groupId + "|" + userId;
                if(logger.isTraceEnabled()) logger.trace("Derived aggregateId for {}: {}", aggregateType, id);
                yield id;
            }
            case PortalConstants.AGGREGATE_GROUP_ROW_FILTER -> {
                String groupId = (String) dataMap.get("groupId");
                String endpointId = (String) dataMap.get("endpointId");
                String colName = (String) dataMap.get("colName");
                if (endpointId == null || groupId == null || colName == null) {
                    logger.warn("groupId or endpointId or colName is null in data map for aggregate type: {}", aggregateType);
                    yield null;
                }
                String id = groupId + "|" + endpointId + "|" + colName;
                if(logger.isTraceEnabled()) logger.trace("Derived aggregateId for {}: {}", aggregateType, id);
                yield id;
            }
            case PortalConstants.AGGREGATE_GROUP_COL_FILTER -> {
                String groupId = (String) dataMap.get("groupId");
                String endpointId = (String) dataMap.get("endpointId");
                if (endpointId == null || groupId == null) {
                    logger.warn("groupId or endpointId is null in data map for aggregate type: {}", aggregateType);
                    yield null;
                }
                String id = groupId + "|" + endpointId;
                if(logger.isTraceEnabled()) logger.trace("Derived aggregateId for {}: {}", aggregateType, id);
                yield id;
            }
            case PortalConstants.AGGREGATE_POSITION -> {
                String positionId = (String) dataMap.get("positionId");
                if (positionId == null) {
                    logger.warn("positionId is null in data map for aggregate type: {}", aggregateType);
                    yield null;
                }
                if(logger.isTraceEnabled()) logger.trace("Derived aggregateId for {}: {}", aggregateType, positionId);
                yield positionId;
            }
            case PortalConstants.AGGREGATE_POSITION_PERMISSION -> {
                String positionId = (String) dataMap.get("positionId");
                String endpointId = (String) dataMap.get("endpointId");
                if (endpointId == null || positionId == null) {
                    logger.warn("positionId or endpointId is null in data map for aggregate type: {}", aggregateType);
                    yield null;
                }
                String id = positionId + "|" + endpointId;
                if(logger.isTraceEnabled()) logger.trace("Derived aggregateId for {}: {}", aggregateType, id);
                yield id;
            }
            case PortalConstants.AGGREGATE_POSITION_USER -> {
                String positionId = (String) dataMap.get("positionId");
                String userId = (String) dataMap.get("userId");
                if (userId == null || positionId == null) {
                    logger.warn("positionId or userId is null in data map for aggregate type: {}", aggregateType);
                    yield null;
                }
                String id = positionId + "|" + userId;
                if(logger.isTraceEnabled()) logger.trace("Derived aggregateId for {}: {}", aggregateType, id);
                yield id;
            }
            case PortalConstants.AGGREGATE_POSITION_ROW_FILTER -> {
                String positionId = (String) dataMap.get("positionId");
                String endpointId = (String) dataMap.get("endpointId");
                String colName = (String) dataMap.get("colName");
                if (endpointId == null || positionId == null || colName == null) {
                    logger.warn("positionId or endpointId or colName is null in data map for aggregate type: {}", aggregateType);
                    yield null;
                }
                String id = positionId + "|" + endpointId + "|" + colName;
                if(logger.isTraceEnabled()) logger.trace("Derived aggregateId for {}: {}", aggregateType, id);
                yield id;
            }
            case PortalConstants.AGGREGATE_POSITION_COL_FILTER -> {
                String positionId = (String) dataMap.get("positionId");
                String endpointId = (String) dataMap.get("endpointId");
                if (endpointId == null || positionId == null) {
                    logger.warn("positionId or endpointId is null in data map for aggregate type: {}", aggregateType);
                    yield null;
                }
                String id = positionId + "|" + endpointId;
                if(logger.isTraceEnabled()) logger.trace("Derived aggregateId for {}: {}", aggregateType, id);
                yield id;
            }
            case PortalConstants.AGGREGATE_ATTRIBUTE -> {
                String attributeId = (String) dataMap.get("attributeId");
                if (attributeId == null) {
                    logger.warn("attributeId is null in data map for aggregate type: {}", aggregateType);
                    yield null;
                }
                if(logger.isTraceEnabled()) logger.trace("Derived aggregateId for {}: {}", aggregateType, attributeId);
                yield attributeId;
            }
            case PortalConstants.AGGREGATE_ATTRIBUTE_PERMISSION -> {
                String attributeId = (String) dataMap.get("attributeId");
                String endpointId = (String) dataMap.get("endpointId");
                if (endpointId == null || attributeId == null) {
                    logger.warn("attributeId or endpointId is null in data map for aggregate type: {}", aggregateType);
                    yield null;
                }
                String id = attributeId + "|" + endpointId;
                if(logger.isTraceEnabled()) logger.trace("Derived aggregateId for {}: {}", aggregateType, id);
                yield id;
            }
            case PortalConstants.AGGREGATE_ATTRIBUTE_USER -> {
                String attributeId = (String) dataMap.get("attributeId");
                String userId = (String) dataMap.get("userId");
                if (userId == null || attributeId == null) {
                    logger.warn("attributeId or userId is null in data map for aggregate type: {}", aggregateType);
                    yield null;
                }
                String id = attributeId + "|" + userId;
                if(logger.isTraceEnabled()) logger.trace("Derived aggregateId for {}: {}", aggregateType, id);
                yield id;
            }
            case PortalConstants.AGGREGATE_ATTRIBUTE_ROW_FILTER -> {
                String attributeId = (String) dataMap.get("attributeId");
                String endpointId = (String) dataMap.get("endpointId");
                String colName = (String) dataMap.get("colName");
                if (endpointId == null || attributeId == null || colName == null) {
                    logger.warn("attributeId or endpointId or colName is null in data map for aggregate type: {}", aggregateType);
                    yield null;
                }
                String id = attributeId + "|" + endpointId + "|" + colName;
                if(logger.isTraceEnabled()) logger.trace("Derived aggregateId for {}: {}", aggregateType, id);
                yield id;
            }
            case PortalConstants.AGGREGATE_ATTRIBUTE_COL_FILTER -> {
                String attributeId = (String) dataMap.get("attributeId");
                String endpointId = (String) dataMap.get("endpointId");
                if (endpointId == null || attributeId == null) {
                    logger.warn("attributeId or endpointId is null in data map for aggregate type: {}", aggregateType);
                    yield null;
                }
                String id = attributeId + "|" + endpointId;
                if(logger.isTraceEnabled()) logger.trace("Derived aggregateId for {}: {}", aggregateType, id);
                yield id;
            }
            case PortalConstants.AGGREGATE_DEPLOYMENT -> {
                String deploymentId = (String) dataMap.get("deploymentId");
                if (deploymentId == null) {
                    logger.warn("deploymentId is null in data map for aggregate type: {}", aggregateType);
                    yield null;
                }
                if(logger.isTraceEnabled()) logger.trace("Derived aggregateId for {}: {}", aggregateType, deploymentId);
                yield deploymentId;
            }
            case PortalConstants.AGGREGATE_DEPLOYMENT_INSTANCE -> {
                String deploymentInstanceId = (String) dataMap.get("deploymentInstanceId");
                if (deploymentInstanceId == null) {
                    logger.warn("deploymentInstanceId is null in data map for aggregate type: {}", aggregateType);
                    yield null;
                }
                if(logger.isTraceEnabled()) logger.trace("Derived aggregateId for {}: {}", aggregateType, deploymentInstanceId);
                yield deploymentInstanceId;
            }
            case PortalConstants.AGGREGATE_PLATFORM -> {
                String platformId = (String) dataMap.get("platformId");
                if (platformId == null) {
                    logger.warn("platformId is null in data map for aggregate type: {}", aggregateType);
                    yield null;
                }
                if(logger.isTraceEnabled()) logger.trace("Derived aggregateId for {}: {}", aggregateType, platformId);
                yield platformId;
            }
            case PortalConstants.AGGREGATE_PIPELINE -> {
                String pipelineId = (String) dataMap.get("pipelineId");
                if (pipelineId == null) {
                    logger.warn("pipelineId is null in data map for aggregate type: {}", aggregateType);
                    yield null;
                }
                if(logger.isTraceEnabled()) logger.trace("Derived aggregateId for {}: {}", aggregateType, pipelineId);
                yield pipelineId;
            }
            case PortalConstants.AGGREGATE_PRODUCT_VERSION -> {
                String productVersionId = (String) dataMap.get("productVersionId");
                if (productVersionId == null) {
                    logger.warn("productVersionId is null in data map for aggregate type: {}", aggregateType);
                    yield null;
                }
                if(logger.isTraceEnabled()) logger.trace("Derived aggregateId for {}: {}", aggregateType, productVersionId);
                yield productVersionId;
            }
            case PortalConstants.AGGREGATE_PRODUCT_VERSION_CONFIG -> {
                String productVersionId = (String) dataMap.get("productVersionId");
                String configId = (String) dataMap.get("configId");
                if (configId == null || productVersionId == null) {
                    logger.warn("productVersionId or configId is null in data map for aggregate type: {}", aggregateType);
                    yield null;
                }
                String id = productVersionId + "|" + configId;
                if(logger.isTraceEnabled()) logger.trace("Derived aggregateId for {}: {}", aggregateType, id);
                yield id;
            }
            case PortalConstants.AGGREGATE_PRODUCT_VERSION_CONFIG_PROPERTY -> {
                String productVersionId = (String) dataMap.get("productVersionId");
                String propertyId = (String) dataMap.get("propertyId");
                if (propertyId == null || productVersionId == null) {
                    logger.warn("productVersionId or propertyId is null in data map for aggregate type: {}", aggregateType);
                    yield null;
                }
                String id = productVersionId + "|" + propertyId;
                if(logger.isTraceEnabled()) logger.trace("Derived aggregateId for {}: {}", aggregateType, id);
                yield id;
            }
            case PortalConstants.AGGREGATE_PRODUCT_VERSION_ENVIRONMENT -> {
                String productVersionId = (String) dataMap.get("productVersionId");
                String systemEnv = (String) dataMap.get("systemEnv");
                String runtimeEnv = (String) dataMap.get("runtimeEnv");
                if (systemEnv == null || productVersionId == null || runtimeEnv == null) {
                    logger.warn("productVersionId or systemEnv or runtimeEnv is null in data map for aggregate type: {}", aggregateType);
                    yield null;
                }
                String id = productVersionId + "|" + systemEnv + "|" + runtimeEnv;
                if(logger.isTraceEnabled()) logger.trace("Derived aggregateId for {}: {}", aggregateType, id);
                yield id;
            }
            case PortalConstants.AGGREGATE_PRODUCT_VERSION_PIPELINE -> {
                String productVersionId = (String) dataMap.get("productVersionId");
                String pipelineId = (String) dataMap.get("pipelineId");
                if (pipelineId == null || productVersionId == null) {
                    logger.warn("productVersionId or pipelineId is null in data map for aggregate type: {}", aggregateType);
                    yield null;
                }
                String id = productVersionId + "|" + pipelineId;
                if(logger.isTraceEnabled()) logger.trace("Derived aggregateId for {}: {}", aggregateType, id);
                yield id;
            }
            case "Instance" -> (String) dataMap.get("instanceId");
            case "Host" -> (String) dataMap.get("hostId");
            case "User" -> (String) dataMap.get("userId");
            case "Permission" -> (String) dataMap.get("permissionId");
            case "ApiKey" -> (String) dataMap.get("apiKeyId");
            case "AuditLog" -> (String) dataMap.get("auditLogId");
            case "PasswordReset" -> (String) dataMap.get("resetId");
            case "Invitation" -> (String) dataMap.get("invitationId");
            // Add more cases as needed for other aggregate types
            default -> {
                logger.warn("No aggregate ID mapping defined for aggregate type: {}", aggregateType);
                yield null;
            }
        };
    }
}
