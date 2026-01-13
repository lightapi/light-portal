package net.lightapi.portal.mutator;

import com.networknt.db.provider.DbProvider;
import net.lightapi.portal.PortalConstants;
import net.lightapi.portal.db.PortalDbProvider;
import com.networknt.service.SingletonServiceFactory;

import java.util.Map;

/**
 * This is used to handle the generated events from the config schema during the build. As the configId and propertyId
 * won't be available in the generated events, we need to enrich them based on the config name and property name only.
 */
public class ProductVersionConfigPropertyEnricher implements EventEnricher {

    public static PortalDbProvider dbProvider = (PortalDbProvider) SingletonServiceFactory.getBean(DbProvider.class);

    @Override
    public String getEventType() {
        return PortalConstants.PRODUCT_VERSION_CONFIG_PROPERTY_CREATED_EVENT;
    }

    @Override
    public Map<String, Object> enrich(Map<String, Object> event) {

        Map<String, Object> data = (Map<String, Object>)event.get(PortalConstants.DATA);
        String hostId = (String)data.get("hostId");
        String configName = (String) data.get("configName");
        String propertyName = (String) data.get("propertyName");
        String productId = (String) data.get("productId");
        String light4jVersion = (String) data.get("light4jVersion");

        if (hostId == null || configName == null || propertyName == null || productId == null || light4jVersion == null) {
            throw new IllegalArgumentException("Missing required lookup keys for " + getEventType());
        }

        // Resolve productVersionId
        String productVersionId = dbProvider.queryProductVersionId(hostId, productId, light4jVersion);

        // Resolve propertyId
        String propertyId = dbProvider.queryPropertyId(configName, propertyName);

        // Build Final Event ---
        // The Aggregate ID (Subject) for the junction table is the composite key.
        String aggregateId = String.format("%s|%s", productVersionId, propertyId);
        int aggregateVersion = dbProvider.getMaxAggregateVersion(aggregateId) + 1;

        event.put("subject", aggregateId);
        event.put("aggregateversion", aggregateVersion);
        data.put("productVersionId", productVersionId);
        data.put("propertyId", propertyId);
        data.put("newAggregateVersion", aggregateVersion);
        return event;
    }
}
