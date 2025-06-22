package net.lightapi.portal.db.persistence;

import com.networknt.monad.Result;
import io.cloudevents.CloudEvent;

import java.util.Map;

public interface EventPersistence {
    Result<String> insertEventStore(CloudEvent[] events);
}
