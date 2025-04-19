package net.lightapi.db;

import com.networknt.utility.Constants;
import com.networknt.utility.Util;
import com.networknt.utility.UuidUtil;
import io.cloudevents.CloudEvent;
import io.cloudevents.core.builder.CloudEventBuilder;
import io.cloudevents.jackson.JsonFormat;
import net.lightapi.portal.PortalConstants;
import org.junit.jupiter.api.Test;

import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;

public class CloudEventTest {
    private static final JsonFormat jsonFormat = new JsonFormat();

    @Test
    public void testJsonFormat() {
        CloudEventBuilder eventTemplate = CloudEventBuilder.v1()
                .withSource(URI.create("https://github.com/lightapi/light-portal"))
                .withType(PortalConstants.AUTH_CODE_CREATED_EVENT);

        String data = "{\"redirectUri\":\"https://localhost:3000/authorization\",\"authCode\":\"dTT0hjYBS7aZwF0gipqxnA\",\"roles\":\"admin user\",\"hostId\":\"N2CMw0HGQXeLvC1wBfln2A\",\"groups\":\"delete insert select update\",\"entityId\":\"sh35\",\"positions\":\"APIPlatformDelivery\",\"userId\":\"utgdG50vRVOX3mL1Kf83aA\",\"remember\":\"Y\",\"providerId\":\"EF3wqhfWQti2DUVrvYNM7g\",\"attributes\":\"country^=^CAN~peranent_employee^=^true~security_clearance_level^=^2\",\"userType\":\"E\",\"email\":\"steve.hu@lightapi.net\"}";
        CloudEvent event = eventTemplate.newBuilder()
                .withId(UuidUtil.getUUID().toString())
                .withTime(OffsetDateTime.now())
                .withExtension(Constants.USER, "user123")
                .withExtension(PortalConstants.NONCE, 1)
                .withExtension(Constants.HOST, "host123")
                .withData("application/json", data.getBytes(StandardCharsets.UTF_8))
                .build();

        byte[] bytes = jsonFormat.serialize(event);
        String json = new String(bytes, StandardCharsets.UTF_8);
        System.out.println("json = " + json);
    }

    @Test
    public void testMilliSecond2OffsetDateTime() {
        long time = 1740003720000L;
        Instant instant = Instant.ofEpochMilli(time);
        OffsetDateTime odtUtc = instant.atOffset(ZoneOffset.UTC);
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSSXXX");
        String formatted = odtUtc.format(formatter);
        System.out.println("offsetDateTime = " + formatted);
    }
}
