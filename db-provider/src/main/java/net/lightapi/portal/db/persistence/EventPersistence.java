package net.lightapi.portal.db.persistence;

import com.networknt.monad.Result;
import io.cloudevents.CloudEvent;

public interface EventPersistence {
    Result<String> insertEventStore(CloudEvent[] events);
    Result<String> getEventStore(int offset, int limit, String hostId, String id, String userId, Long nonce, String aggregateId, Long aggregateVersion, String aggregateType, String eventType, String payload, String metaData);
    Result<String> exportEventStore(String hostId, String id, String userId, Long nonce, String aggregateId, Long aggregateVersion, String aggregateType, String eventType, String payload, String metaData);
    int getMaxAggregateVersion(String aggregateId);
}
