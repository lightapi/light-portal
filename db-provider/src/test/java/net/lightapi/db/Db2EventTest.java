package net.lightapi.db;

import com.networknt.config.JsonMapper;
import com.networknt.db.provider.DbProvider;
import com.networknt.db.provider.SqlDbStartupHook;
import com.networknt.monad.Result;
import com.networknt.service.SingletonServiceFactory;
import com.networknt.utility.Constants;
import com.networknt.utility.UuidUtil;
import io.cloudevents.CloudEvent;
import io.cloudevents.core.builder.CloudEventBuilder;
import io.cloudevents.jackson.JsonFormat;
import net.lightapi.portal.PortalConstants;
import net.lightapi.portal.db.PortalDbProvider;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.nio.charset.StandardCharsets;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.time.format.DateTimeFormatter;
import java.util.List;
import java.util.Map;

@Disabled
public class Db2EventTest {
    public static final Logger logger = LoggerFactory.getLogger(Db2EventTest.class);
    private static final JsonFormat jsonFormat = new JsonFormat();
    public static PortalDbProvider dbProvider;
    public static SqlDbStartupHook sqlDbStartupHook;
    public static final String USER = "01964b05-5532-7c79-8cde-191dcbd421b8";
    public static final String HOST = "01964b05-552a-7c4b-9184-6857e7f3dc5f";

    @BeforeAll
    static void init() {
        sqlDbStartupHook = new SqlDbStartupHook();
        sqlDbStartupHook.onStartup();
        dbProvider = (PortalDbProvider) SingletonServiceFactory.getBean(DbProvider.class);

    }

    @Test
    void testExtractConfigCreatedEvent() {
        long nonce = 68L;
        LocalDateTime now = LocalDateTime.now();
        String formattedDateTime = now.format(DateTimeFormatter.ISO_LOCAL_DATE_TIME);
        String filename = "/home/steve/lightapi/events/config-" + formattedDateTime + ".txt";
        Result<String> result = dbProvider.getConfig(0, Integer.MAX_VALUE, null, null, null, null, null, null, null);
        if(result.isFailure()) {
            System.out.println(result.getError());
        } else {
            String json = result.getResult();
            Map<String, Object> map = JsonMapper.string2Map(json);
            List<Map<String, Object>> configs = (List<Map<String, Object>>)map.get("configs");
            int total = (int)map.get("total");
            System.out.println("total = " + total);
            try(BufferedWriter writer = new BufferedWriter(new FileWriter(filename))) {
                // iterate configs
                for(Map<String, Object> config: configs) {
                    String data = JsonMapper.toJson(config);
                    StringBuilder builder = new StringBuilder();
                    builder.append(USER).append(" ");

                    CloudEventBuilder eventTemplate = CloudEventBuilder.v1()
                            .withSource(PortalConstants.EVENT_SOURCE)
                            .withType(PortalConstants.CONFIG_CREATED_EVENT);
                    CloudEvent event = eventTemplate.newBuilder()
                            .withId(UuidUtil.getUUID().toString())
                            .withTime(OffsetDateTime.now())
                            .withExtension(Constants.USER, USER)
                            .withExtension(PortalConstants.NONCE, ++nonce)
                            .withExtension(Constants.HOST, HOST)
                            .withData("application/json", data.getBytes(StandardCharsets.UTF_8))
                            .build();
                    byte[] eventBytes = jsonFormat.serialize(event);
                    String eventJson = new String(eventBytes, StandardCharsets.UTF_8);
                    builder.append(eventJson).append("\n");
                    writer.write(builder.toString());
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    @Test
    void testExtractConfigPropertyCreatedEvent() {
        long nonce = 140L;
        LocalDateTime now = LocalDateTime.now();
        String formattedDateTime = now.format(DateTimeFormatter.ISO_LOCAL_DATE_TIME);
        String filename = "/home/steve/lightapi/events/config-property-" + formattedDateTime + ".txt";
        Result<String> result = dbProvider.getConfigProperty(0, Integer.MAX_VALUE, null, null, null, null, null, null, null, null, null, null, null, null, null);
        if(result.isFailure()) {
            System.out.println(result.getError());
        } else {
            String json = result.getResult();
            Map<String, Object> map = JsonMapper.string2Map(json);
            List<Map<String, Object>> objects = (List<Map<String, Object>>)map.get("configProperties");
            int total = (int)map.get("total");
            System.out.println("total = " + total);
            try(BufferedWriter writer = new BufferedWriter(new FileWriter(filename))) {
                // iterate configs
                for(Map<String, Object> object: objects) {
                    // add propertyId field as UUID for the primary key when migrating from old schema to new.
                    if(object.get("propertyId") == null) object.put("propertyId", UuidUtil.getUUID().toString());
                    String data = JsonMapper.toJson(object);

                    StringBuilder builder = new StringBuilder();
                    builder.append(USER).append(" ");

                    CloudEventBuilder eventTemplate = CloudEventBuilder.v1()
                            .withSource(PortalConstants.EVENT_SOURCE)
                            .withType(PortalConstants.CONFIG_PROPERTY_CREATED_EVENT);
                    CloudEvent event = eventTemplate.newBuilder()
                            .withId(UuidUtil.getUUID().toString())
                            .withTime(OffsetDateTime.now())
                            .withExtension(Constants.USER, USER)
                            .withExtension(PortalConstants.NONCE, ++nonce)
                            .withExtension(Constants.HOST, HOST)
                            .withData("application/json", data.getBytes(StandardCharsets.UTF_8))
                            .build();
                    byte[] eventBytes = jsonFormat.serialize(event);
                    String eventJson = new String(eventBytes, StandardCharsets.UTF_8);
                    builder.append(eventJson).append("\n");
                    writer.write(builder.toString());
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    @Test
    void testExtractRefTableCreatedEvent() {
        long nonce = 714L;
        LocalDateTime now = LocalDateTime.now();
        String formattedDateTime = now.format(DateTimeFormatter.ISO_LOCAL_DATE_TIME);
        String filename = "/home/steve/lightapi/events/ref-table-" + formattedDateTime + ".txt";
        Result<String> result = dbProvider.getRefTable(0, Integer.MAX_VALUE, null, null, null, null, null, null);
        if(result.isFailure()) {
            System.out.println(result.getError());
        } else {
            String json = result.getResult();
            Map<String, Object> map = JsonMapper.string2Map(json);
            List<Map<String, Object>> objects = (List<Map<String, Object>>)map.get("refTables");
            int total = (int)map.get("total");
            System.out.println("total = " + total);
            try(BufferedWriter writer = new BufferedWriter(new FileWriter(filename))) {
                // iterate configs
                for(Map<String, Object> object: objects) {
                    String data = JsonMapper.toJson(object);

                    StringBuilder builder = new StringBuilder();
                    builder.append(USER).append(" ");

                    CloudEventBuilder eventTemplate = CloudEventBuilder.v1()
                            .withSource(PortalConstants.EVENT_SOURCE)
                            .withType(PortalConstants.REF_TABLE_CREATED_EVENT);
                    CloudEvent event = eventTemplate.newBuilder()
                            .withId(UuidUtil.getUUID().toString())
                            .withTime(OffsetDateTime.now())
                            .withExtension(Constants.USER, USER)
                            .withExtension(PortalConstants.NONCE, ++nonce)
                            .withExtension(Constants.HOST, HOST)
                            .withData("application/json", data.getBytes(StandardCharsets.UTF_8))
                            .build();
                    byte[] eventBytes = jsonFormat.serialize(event);
                    String eventJson = new String(eventBytes, StandardCharsets.UTF_8);
                    builder.append(eventJson).append("\n");
                    writer.write(builder.toString());
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    @Test
    void testExtractRefValueCreatedEvent() {
        long nonce = 748L;
        LocalDateTime now = LocalDateTime.now();
        String formattedDateTime = now.format(DateTimeFormatter.ISO_LOCAL_DATE_TIME);
        String filename = "/home/steve/lightapi/events/ref-value-" + formattedDateTime + ".txt";
        Result<String> result = dbProvider.getRefValue(0, Integer.MAX_VALUE, null, null, null, null, null, null);
        if(result.isFailure()) {
            System.out.println(result.getError());
        } else {
            String json = result.getResult();
            Map<String, Object> map = JsonMapper.string2Map(json);
            List<Map<String, Object>> objects = (List<Map<String, Object>>)map.get("refValues");
            int total = (int)map.get("total");
            System.out.println("total = " + total);
            try(BufferedWriter writer = new BufferedWriter(new FileWriter(filename))) {
                // iterate configs
                for(Map<String, Object> object: objects) {
                    String data = JsonMapper.toJson(object);

                    StringBuilder builder = new StringBuilder();
                    builder.append(USER).append(" ");

                    CloudEventBuilder eventTemplate = CloudEventBuilder.v1()
                            .withSource(PortalConstants.EVENT_SOURCE)
                            .withType(PortalConstants.REF_VALUE_CREATED_EVENT);
                    CloudEvent event = eventTemplate.newBuilder()
                            .withId(UuidUtil.getUUID().toString())
                            .withTime(OffsetDateTime.now())
                            .withExtension(Constants.USER, USER)
                            .withExtension(PortalConstants.NONCE, ++nonce)
                            .withExtension(Constants.HOST, HOST)
                            .withData("application/json", data.getBytes(StandardCharsets.UTF_8))
                            .build();
                    byte[] eventBytes = jsonFormat.serialize(event);
                    String eventJson = new String(eventBytes, StandardCharsets.UTF_8);
                    builder.append(eventJson).append("\n");
                    writer.write(builder.toString());
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    @Test
    void testExtractValueLocaleCreatedEvent() {
        long nonce = 1161L;
        LocalDateTime now = LocalDateTime.now();
        String formattedDateTime = now.format(DateTimeFormatter.ISO_LOCAL_DATE_TIME);
        String filename = "/home/steve/lightapi/events/value-locale-" + formattedDateTime + ".txt";
        Result<String> result = dbProvider.getRefLocale(0, Integer.MAX_VALUE, null, null, null);
        if(result.isFailure()) {
            System.out.println(result.getError());
        } else {
            String json = result.getResult();
            Map<String, Object> map = JsonMapper.string2Map(json);
            List<Map<String, Object>> objects = (List<Map<String, Object>>)map.get("locales");
            int total = (int)map.get("total");
            System.out.println("total = " + total);
            try(BufferedWriter writer = new BufferedWriter(new FileWriter(filename))) {
                // iterate configs
                for(Map<String, Object> object: objects) {
                    String data = JsonMapper.toJson(object);

                    StringBuilder builder = new StringBuilder();
                    builder.append(USER).append(" ");

                    CloudEventBuilder eventTemplate = CloudEventBuilder.v1()
                            .withSource(PortalConstants.EVENT_SOURCE)
                            .withType(PortalConstants.REF_LOCALE_CREATED_EVENT);
                    CloudEvent event = eventTemplate.newBuilder()
                            .withId(UuidUtil.getUUID().toString())
                            .withTime(OffsetDateTime.now())
                            .withExtension(Constants.USER, USER)
                            .withExtension(PortalConstants.NONCE, ++nonce)
                            .withExtension(Constants.HOST, HOST)
                            .withData("application/json", data.getBytes(StandardCharsets.UTF_8))
                            .build();
                    byte[] eventBytes = jsonFormat.serialize(event);
                    String eventJson = new String(eventBytes, StandardCharsets.UTF_8);
                    builder.append(eventJson).append("\n");
                    writer.write(builder.toString());
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    @Test
    void testExtractRefRelationTypeCreatedEvent() {
        long nonce = 1574L;
        LocalDateTime now = LocalDateTime.now();
        String formattedDateTime = now.format(DateTimeFormatter.ISO_LOCAL_DATE_TIME);
        String filename = "/home/steve/lightapi/events/relation-type-" + formattedDateTime + ".txt";
        Result<String> result = dbProvider.getRefRelationType(0, Integer.MAX_VALUE, null, null, null);
        if(result.isFailure()) {
            System.out.println(result.getError());
        } else {
            String json = result.getResult();
            Map<String, Object> map = JsonMapper.string2Map(json);
            List<Map<String, Object>> objects = (List<Map<String, Object>>)map.get("relationTypes");
            int total = (int)map.get("total");
            System.out.println("total = " + total);
            try(BufferedWriter writer = new BufferedWriter(new FileWriter(filename))) {
                // iterate configs
                for(Map<String, Object> object: objects) {
                    String data = JsonMapper.toJson(object);

                    StringBuilder builder = new StringBuilder();
                    builder.append(USER).append(" ");

                    CloudEventBuilder eventTemplate = CloudEventBuilder.v1()
                            .withSource(PortalConstants.EVENT_SOURCE)
                            .withType(PortalConstants.REF_RELATION_TYPE_CREATED_EVENT);
                    CloudEvent event = eventTemplate.newBuilder()
                            .withId(UuidUtil.getUUID().toString())
                            .withTime(OffsetDateTime.now())
                            .withExtension(Constants.USER, USER)
                            .withExtension(PortalConstants.NONCE, ++nonce)
                            .withExtension(Constants.HOST, HOST)
                            .withData("application/json", data.getBytes(StandardCharsets.UTF_8))
                            .build();
                    byte[] eventBytes = jsonFormat.serialize(event);
                    String eventJson = new String(eventBytes, StandardCharsets.UTF_8);
                    builder.append(eventJson).append("\n");
                    writer.write(builder.toString());
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    @Test
    void testExtractRefRelationCreatedEvent() {
        long nonce = 1579L;
        LocalDateTime now = LocalDateTime.now();
        String formattedDateTime = now.format(DateTimeFormatter.ISO_LOCAL_DATE_TIME);
        String filename = "/home/steve/lightapi/events/ref-relation-" + formattedDateTime + ".txt";
        Result<String> result = dbProvider.getRefRelation(0, Integer.MAX_VALUE, null, null, null, null);
        if(result.isFailure()) {
            System.out.println(result.getError());
        } else {
            String json = result.getResult();
            Map<String, Object> map = JsonMapper.string2Map(json);
            List<Map<String, Object>> objects = (List<Map<String, Object>>)map.get("relations");
            int total = (int)map.get("total");
            System.out.println("total = " + total);
            try(BufferedWriter writer = new BufferedWriter(new FileWriter(filename))) {
                // iterate configs
                for(Map<String, Object> object: objects) {
                    String data = JsonMapper.toJson(object);

                    StringBuilder builder = new StringBuilder();
                    builder.append(USER).append(" ");

                    CloudEventBuilder eventTemplate = CloudEventBuilder.v1()
                            .withSource(PortalConstants.EVENT_SOURCE)
                            .withType(PortalConstants.REF_RELATION_CREATED_EVENT);
                    CloudEvent event = eventTemplate.newBuilder()
                            .withId(UuidUtil.getUUID().toString())
                            .withTime(OffsetDateTime.now())
                            .withExtension(Constants.USER, USER)
                            .withExtension(PortalConstants.NONCE, ++nonce)
                            .withExtension(Constants.HOST, HOST)
                            .withData("application/json", data.getBytes(StandardCharsets.UTF_8))
                            .build();
                    byte[] eventBytes = jsonFormat.serialize(event);
                    String eventJson = new String(eventBytes, StandardCharsets.UTF_8);
                    builder.append(eventJson).append("\n");
                    writer.write(builder.toString());
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

}
