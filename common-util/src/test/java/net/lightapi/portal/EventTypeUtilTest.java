package net.lightapi.portal;

import org.junit.jupiter.api.Test;

import static net.lightapi.portal.EventTypeUtil.deriveAggregateTypeFromEventType;

public class EventTypeUtilTest {
    @Test
    public void testEventType2AggregateType() {
        System.out.println("ConfigCreatedEvent -> " + deriveAggregateTypeFromEventType("ConfigCreatedEvent"));
        System.out.println("ConfigPropertyUpdatedEvent -> " + deriveAggregateTypeFromEventType("ConfigPropertyUpdatedEvent"));
        System.out.println("HostSwitchedEvent -> " + deriveAggregateTypeFromEventType("HostSwitchedEvent"));
        System.out.println("PasswordResetEvent -> " + deriveAggregateTypeFromEventType("PasswordResetEvent"));
        System.out.println("ServiceVersionCreatedEvent -> " + deriveAggregateTypeFromEventType("ServiceVersionCreatedEvent"));
        System.out.println("DeploymentJobIdUpdatedEvent -> " + deriveAggregateTypeFromEventType("DeploymentJobIdUpdatedEvent"));
        System.out.println("PlatformQueriedEvent -> " + deriveAggregateTypeFromEventType("PlatformQueriedEvent"));
        System.out.println("UnknownEventType -> " + deriveAggregateTypeFromEventType("UnknownEventType"));
        System.out.println("UserOnboardedEvent -> " + deriveAggregateTypeFromEventType("UserOnboardedEvent"));
    }
}
