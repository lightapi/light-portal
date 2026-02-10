package net.lightapi.db;

import com.networknt.config.JsonMapper;
import com.networknt.db.provider.DbProvider;
import com.networknt.db.provider.SqlDbStartupHook;
import com.networknt.monad.Result;
import com.networknt.service.SingletonServiceFactory;
import com.networknt.utility.UuidUtil;
import com.networknt.utility.Constants;
import io.cloudevents.CloudEvent;
import io.cloudevents.core.builder.CloudEventBuilder;
import net.lightapi.portal.PortalConstants;
import net.lightapi.portal.db.PortalDbProvider;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import java.net.URI;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.OffsetDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import static com.networknt.config.JsonMapper.objectMapper;
import static com.networknt.db.provider.SqlDbStartupHook.ds;
import static org.junit.jupiter.api.Assertions.*;

@Disabled
public class PortalDbProviderImplTest {
    public static PortalDbProvider dbProvider;
    public static SqlDbStartupHook sqlDbStartupHook;
    @BeforeAll
    static void init() {
        sqlDbStartupHook = new SqlDbStartupHook();
        sqlDbStartupHook.onStartup();
        dbProvider = (PortalDbProvider) SingletonServiceFactory.getBean(DbProvider.class);
    }

    public Map<String, Object> createEvent(String hostId, String userId, String aggregateId, String aggregateType, int aggregateVersion, Map<String, Object> dataMap) {
        Map<String, Object> map = new HashMap<>();
        map.put("id", UuidUtil.getUUID().toString());
        map.put("host", hostId);
        map.put("time", OffsetDateTime.now());
        map.put("user", userId);
        map.put("nonce", dbProvider.queryNonceByUserId(userId));
        map.put("source", "https://github.com/lightapi/light-portal");
        map.put("subject", aggregateId);
        map.put("specversion", "1.0");
        map.put("aggregatetype", aggregateType);
        map.put("datacontenttype", "application/json");
        map.put("aggregateversion", aggregateVersion);
        map.put("data", dataMap);
        return map;
    }

    @Test
    void testCloneInstance() throws Exception {
        Map<String, Object> data = Map.of("sourceInstanceId", "019aad40-eaef-73d2-b557-9af202340706");
        Map<String, Object> instanceClonedEvent = createEvent(
                "01964b05-552a-7c4b-9184-6857e7f3dc5f",
                "01964b05-5532-7c79-8cde-191dcbd421b8",
                "019aad40-eaef-73d2-b557-9af202340706",
                "Instance",
                1,
                data);
        Connection conn = SqlDbStartupHook.ds.getConnection();
        dbProvider.cloneInstance(conn, instanceClonedEvent);
    }

    @Test
    void testLoginUserByEmail() {
        Result<String> result = dbProvider.loginUserByEmail("steve.hu@lightapi.net");
        if(result.isFailure()) {
            System.out.println(result.getError());
        } else {
            System.out.println(result.getResult());
        }
    }

    @Test
    void testGetRefTableNoHost() {
        Result<String> result = dbProvider.getRefTable(0, 100, null, null, null, true, null);
        if(result.isFailure()) {
            System.out.println(result.getError());
        } else {
            System.out.println(result.getResult());
        }
    }

    @Test
    void testGetRefTableWithFilters() {
        Result<String> result = dbProvider.getRefTable(0, 100,"[{\"id\":\"tableName\",\"value\":\"env\"}]", null, null, true, "01964b05-552a-7c4b-9184-6857e7f3dc5f");
        if(result.isFailure()) {
            System.out.println(result.getError());
        } else {
            System.out.println(result.getResult());
        }
    }

    @Test
    void testGetRefTableWithGlobalFilter() {
        Result<String> result = dbProvider.getRefTable(0, 100,null, "env", null, true,"01964b05-552a-7c4b-9184-6857e7f3dc5f");
        if(result.isFailure()) {
            System.out.println(result.getError());
        } else {
            System.out.println(result.getResult());
        }
    }

    @Test
    void testGetRefTableWithSorting() {
        Result<String> result = dbProvider.getRefTable(0, 100,null, null, "[{\"id\":\"tableId\",\"desc\":false}]", true, "01964b05-552a-7c4b-9184-6857e7f3dc5f");
        if(result.isFailure()) {
            System.out.println(result.getError());
        } else {
            System.out.println(result.getResult());
        }
    }

    @Test
    void testGetRefValueWithFilters() {
        Result<String> result = dbProvider.getRefValue(0, 100,"[{\"id\":\"tableId\",\"value\":\"01964b05-552e-705a-a193-7a859347a9d5\"}]", null, null, true);
        if(result.isFailure()) {
            System.out.println(result.getError());
        } else {
            System.out.println(result.getResult());
        }
    }

    @Test
    void testGetRefTableLabelWithHost() {
        Result<String> result = dbProvider.getRefTableLabel("01964b05-552a-7c4b-9184-6857e7f3dc5f");
        if(result.isFailure()) {
            System.out.println(result.getError());
        } else {
            System.out.println(result.getResult());
        }
    }

    @Test
    void testQueryService() {
        Result<String> result = dbProvider.queryApi(0, 2, null, null, null, true, "01964b05-552a-7c4b-9184-6857e7f3dc5f");
        if(result.isFailure()) {
            System.out.println(result.getError());
        } else {
            System.out.println(result.getResult());
        }
    }

    @Test
    void testQueryRule() {
        Result<String> result = dbProvider.queryRule(0, 2, null, null, null, true, "01964b05-552a-7c4b-9184-6857e7f3dc5f");
        if(result.isFailure()) {
            System.out.println(result.getError());
        } else {
            System.out.println(result.getResult());
        }
    }

    @Test
    void testGetNextNonce() {
        long nonce = dbProvider.queryNonceByUserId("01964b05-5532-7c79-8cde-191dcbd421b8");
        System.out.println("nonce = " + nonce);
    }

    @Test
    public void testQueryRole()  {
        Result<String> result = dbProvider.queryRole(0, 4, null, null, null, true, "01964b05-552a-7c4b-9184-6857e7f3dc5f");
        if(result.isFailure()) {
            System.out.println(result.getError());
            fail();
        } else {
            System.out.println(result.getResult());
            Map<String, Object> roles = JsonMapper.string2Map(result.getResult());
            assertTrue((Integer)roles.get("total") > 0);
            assertTrue(((List)roles.get("roles")).size() > 0);
        }
    }

    @Test
    public void testQueryRoleUser()  {
        Result<String> result = dbProvider.queryRoleUser(0, 4, null, null, null, true, "01964b05-552a-7c4b-9184-6857e7f3dc5f");
        if(result.isFailure()) {
            System.out.println(result.getError());
            fail();
        } else {
            System.out.println(result.getResult());
            Map<String, Object> entities = JsonMapper.string2Map(result.getResult());
            assertTrue((Integer)entities.get("total") > 0);
        }
    }

    @Test
    public void testQueryGroupUser()  {
        Result<String> result = dbProvider.queryGroupUser(0, 4, null, null, null, true, "01964b05-552a-7c4b-9184-6857e7f3dc5f");
        if(result.isFailure()) {
            System.out.println(result.getError());
            fail();
        } else {
            System.out.println(result.getResult());
            Map<String, Object> entities = JsonMapper.string2Map(result.getResult());
            assertTrue((Integer)entities.get("total") >= 0);
        }
    }

    @Test
    public void testQueryPositionUser()  {
        Result<String> result = dbProvider.queryPositionUser(0, 4, null, null, null, true, "01964b05-552a-7c4b-9184-6857e7f3dc5f");
        if(result.isFailure()) {
            System.out.println(result.getError());
            fail();
        } else {
            System.out.println(result.getResult());
            Map<String, Object> entities = JsonMapper.string2Map(result.getResult());
            assertTrue((Integer)entities.get("total") >= 0);
        }
    }

    @Test
    public void testQueryAttributeUser()  {
        Result<String> result = dbProvider.queryAttributeUser(0, 4, null, null, null, true, "01964b05-552a-7c4b-9184-6857e7f3dc5f");
        if(result.isFailure()) {
            System.out.println(result.getError());
            fail();
        } else {
            System.out.println(result.getResult());
            Map<String, Object> entities = JsonMapper.string2Map(result.getResult());
            assertTrue((Integer)entities.get("total") >= 0);
        }
    }

    @Test
    @Disabled
    public void testQueryGroup() {
        Result<String> result = dbProvider.queryGroup(0, 2, null, null, null,true, "01964b05-552a-7c4b-9184-6857e7f3dc5f");
        if(result.isFailure()) {
            System.out.println(result.getError());
            fail();
        } else {
            System.out.println(result.getResult());
            Map<String, Object> groups = JsonMapper.string2Map(result.getResult());
            assertTrue((Integer)groups.get("total") > 0);
            assertTrue(((List)groups.get("groups")).size() > 0);
        }
    }

    @Test
    @Disabled
    public void testQueryPosition() {
        Result<String> result = dbProvider.queryPosition(0, 2, null, null, null, true, "01964b05-552a-7c4b-9184-6857e7f3dc5f");
        if(result.isFailure()) {
            System.out.println(result.getError());
            fail();
        } else {
            System.out.println(result.getResult());
            Map<String, Object> positions = JsonMapper.string2Map(result.getResult());
            assertTrue((Integer)positions.get("total") > 0);
            assertTrue(((List)positions.get("positions")).size() > 0);
        }
    }

    @Test
    @Disabled
    public void testQueryAttribute() {
        Result<String> result = dbProvider.queryAttribute(0, 2, null, null, null,true, "01964b05-552a-7c4b-9184-6857e7f3dc5f");
        if(result.isFailure()) {
            System.out.println(result.getError());
            fail();
        } else {
            System.out.println(result.getResult());
            Map<String, Object> attributes = JsonMapper.string2Map(result.getResult());
            assertTrue((Integer)attributes.get("total") > 0);
            assertTrue(((List)attributes.get("attributes")).size() > 0);
        }
    }

    @Test
    @Disabled
    public void testQueryServicePermission() {
        Result<String> result = dbProvider.queryApiPermission("01964b05-552a-7c4b-9184-6857e7f3dc5f", "0100", "1.0.0");
        if(result.isFailure()) {
            System.out.println(result.getError());
            fail();
        } else {
            System.out.println(result.getResult());
            List<Map<String, Object>> permissions = JsonMapper.string2List(result.getResult());
            assertTrue(permissions.size() > 0);
        }
    }

    @Test
    @Disabled
    public void testQueryServiceFilter() {
        Result<List<String>> result = dbProvider.queryApiFilter("01964b05-552a-7c4b-9184-6857e7f3dc5f", "0100", "1.0.0");
        if(result.isFailure()) {
            System.out.println(result.getError());
            fail();
        } else {
            System.out.println(result.getResult());
            List<String> filters = result.getResult();
            assertFalse(filters.isEmpty());
        }
    }

    @Test
    void testListUserByHostId() {
        Result<String> result = dbProvider.queryUserByHostId(0, 2, "[{\"id\":\"hostId\",\"value\":\"01964b05-552a-7c4b-9184-6857e7f3dc5f\"}]", null, null, true);
        if(result.isFailure()) {
            System.out.println(result.getError());
        } else {
            System.out.println(result.getResult());
        }
    }

    @Test
    void testQueryApp() {
        Result<String> result = dbProvider.queryApp(0, 2, "[{\"id\":\"active\",\"value\":false}]", null, null, true, "01964b05-552a-7c4b-9184-6857e7f3dc5f");
        if(result.isFailure()) {
            System.out.println(result.getError());
        } else {
            System.out.println(result.getResult());
        }
    }

    @Test
    void testQueryProviderKey() {
        Result<String> result = dbProvider.queryProviderKey("01964b05-552a-7c4b-9184-6857e7f3dc5f");
        if(result.isFailure()) {
            System.out.println(result.getError());
        } else {
            System.out.println(result.getResult());
        }
    }

    @Test
    void testGetHostLabel() {
        Result<String> result = dbProvider.getHostLabel();
        if(result.isFailure()) {
            System.out.println(result.getError());
        } else {
            System.out.println(result.getResult());
        }
    }

    @Test
    void testGetProduct() {
        Result<String> result = dbProvider.getProduct(0, 2, "[{\"id\":\"active\",\"value\":true}]", null, null, true, "01964b05-552a-7c4b-9184-6857e7f3dc5f");
        if(result.isFailure()) {
            System.out.println(result.getError());
        } else {
            System.out.println(result.getResult());
        }
    }

    @Test
    void testGetCategory() {
        Result<String> result = dbProvider.getCategory(0, 2, null, null, null, true, "01964b05-552a-7c4b-9184-6857e7f3dc5f");
        if(result.isFailure()) {
            System.out.println(result.getError());
        } else {
            System.out.println(result.getResult());
        }
    }

    @Test
    void testGetCategoryWithoutHostId() {
        Result<String> result = dbProvider.getCategory(0, 2, null, null, null, true,null);
        if(result.isFailure()) {
            System.out.println(result.getError());
        } else {
            System.out.println(result.getResult());
        }
    }

    @Test
    void testQueryClientByClientId() {
        Result<String> result =  dbProvider.queryClientByClientId("f7d42348-c647-4efb-a52d-4c5787421e72");
        if(result.isFailure()) {
            System.out.println(result.getError());
        } else {
            System.out.println(result.getResult());
        }
    }

    @Test
    void testGetClientById() {
        Result<String> result =  dbProvider.getClientById("01964b05-552a-7c4b-9184-6857e7f3dc5f", "f7d42348-c647-4efb-a52d-4c5787421e72");
        if(result.isFailure()) {
            System.out.println(result.getError());
        } else {
            System.out.println(result.getResult());
        }
    }

    @Test
    void testQueryHostById() {
        Result<String> result =  dbProvider.queryHostById("01964b05-552a-7c4b-9184-6857e7f3dc5f");
        if(result.isFailure()) {
            System.out.println(result.getError());
        } else {
            System.out.println(result.getResult());
        }
    }

    @Test
    void testQueryNotification() {
        Result<String> result =  dbProvider.queryNotification(0, 10, "01964b05-552a-7c4b-9184-6857e7f3dc5f", null, null, null, null, null, null, null);
        if(result.isFailure()) {
            System.out.println(result.getError());
        } else {
            System.out.println(result.getResult());
        }
    }

    @Test
    void testQueryAuthCode() {
        Result<String> result =  dbProvider.queryAuthCode("AZZUf_bod7y2VaMm9Lu3_w");
        if(result.isFailure()) {
            System.out.println(result.getError());
        } else {
            System.out.println(result.getResult());
        }
    }

    @Test
    void testGetTag() {
        Result<String> result = dbProvider.getTag(0, 2, null, null, null, true,"01964b05-552a-7c4b-9184-6857e7f3dc5f");
        if(result.isFailure()) {
            System.out.println(result.getError());
        } else {
            System.out.println(result.getResult());
        }
    }

    @Test
    void testCreateConfigSnapshot() throws Exception {
        Map<String, Object> map = Map.of("hostId", "01964b05-552a-7c4b-9184-6857e7f3dc5f",
                "instanceId", "019aad40-eaef-73d2-b557-9af202340706",
                "snapshotType", "USER_SAVE",
                "description", "Test with user save type",
                "userId", "01964b05-5532-7c79-8cde-191dcbd421b8");

        Connection conn = SqlDbStartupHook.ds.getConnection();
        dbProvider.createConfigSnapshot(conn, map);
    }

    @Test
    @Disabled
    void testOnboardUser() throws Exception {

        Map<String, Object> map = new java.util.HashMap<>(Map.of(
                "hostId", "01964b05-552a-7c4b-9184-6857e7f3dc5f",
                "email", "test@example.com",
                "entityId", "cust123",
                "userType", "C",
                "language", "en",
                "firstName", "John",
                "lastName", "Doe",
                "phoneNumber", "123-456-7890",
                "gender", "M"
        ));

        map.put("birthday", "1980-01-01");
        map.put("country", "US");
        map.put("province", "CA");
        map.put("city", "Los Angeles");
        map.put("postCode", "12345");
        map.put("address", "123 Main St");
        map.put("userId", "01964b05-5532-7c79-8cde-191dcbd421b8");
        map.put("token", UUID.randomUUID().toString());
        map.put("verified", true);
        map.put("locked", false);

        // TODO create a cloud event object and pass it to the method
        Map<String, Object> event = new java.util.HashMap<>(Map.of(
                "hostId", "01964b05-552a-7c4b-9184-6857e7f3dc5f"
        ));

        Connection conn = SqlDbStartupHook.ds.getConnection();
        dbProvider.onboardUser(conn, map);
    }

    @Test
    @Disabled
    void testUpdateServiceSpec() throws Exception {
        // To make it work, you need to query the database and update the aggregateVersion in the following JSON string.
        String s = "{\"datacontenttype\":\"application/json\",\"data\":{\"spec\":\"---\\nopenapi: \\\"3.1.0\\\"\\ninfo:\\n  version: \\\"1.0.0\\\"\\n  title: \\\"Swagger Petstore\\\"\\n  license:\\n    name: \\\"MIT\\\"\\nservers:\\n- url: \\\"http://petstore.swagger.io/v1\\\"\\npaths:\\n  /pets:\\n    get:\\n      summary: \\\"List all pets\\\"\\n      operationId: \\\"listPets\\\"\\n      tags:\\n      - \\\"pets\\\"\\n      parameters:\\n      - name: \\\"limit\\\"\\n        in: \\\"query\\\"\\n        description: \\\"How many items to return at one time (max 100)\\\"\\n        required: false\\n        schema:\\n          type: \\\"integer\\\"\\n          format: \\\"int32\\\"\\n      security:\\n      - petstore_auth:\\n        - \\\"read:pets\\\"\\n      responses:\\n        \\\"200\\\":\\n          description: \\\"An paged array of pets\\\"\\n          headers:\\n            x-next:\\n              description: \\\"A link to the next page of responses\\\"\\n              schema:\\n                type: \\\"string\\\"\\n          content:\\n            application/json:\\n              schema:\\n                type: \\\"array\\\"\\n                items:\\n                  $ref: \\\"#/components/schemas/Pet\\\"\\n              example:\\n              - id: 1\\n                name: \\\"catten\\\"\\n                tag: \\\"cat\\\"\\n              - id: 2\\n                name: \\\"doggy\\\"\\n                tag: \\\"dog\\\"\\n        default:\\n          description: \\\"unexpected error\\\"\\n          content:\\n            application/json:\\n              schema:\\n                $ref: \\\"#/components/schemas/Error\\\"\\n    post:\\n      summary: \\\"Create a pet\\\"\\n      operationId: \\\"createPets\\\"\\n      requestBody:\\n        description: \\\"Pet to add to the store\\\"\\n        required: true\\n        content:\\n          application/json:\\n            schema:\\n              $ref: \\\"#/components/schemas/Pet\\\"\\n      tags:\\n      - \\\"pets\\\"\\n      security:\\n      - petstore_auth:\\n        - \\\"read:pets\\\"\\n        - \\\"write:pets\\\"\\n      responses:\\n        \\\"201\\\":\\n          description: \\\"Null response\\\"\\n        default:\\n          description: \\\"unexpected error\\\"\\n          content:\\n            application/json:\\n              schema:\\n                $ref: \\\"#/components/schemas/Error\\\"\\n  /pets/{petId}:\\n    get:\\n      summary: \\\"Info for a specific pet\\\"\\n      operationId: \\\"showPetById\\\"\\n      tags:\\n      - \\\"pets\\\"\\n      parameters:\\n      - name: \\\"petId\\\"\\n        in: \\\"path\\\"\\n        required: true\\n        description: \\\"The id of the pet to retrieve\\\"\\n        schema:\\n          type: \\\"string\\\"\\n      security:\\n      - petstore_auth:\\n        - \\\"read:pets\\\"\\n      responses:\\n        \\\"200\\\":\\n          description: \\\"Expected response to a valid request\\\"\\n          content:\\n            application/json:\\n              schema:\\n                $ref: \\\"#/components/schemas/Pet\\\"\\n              example:\\n                id: 1\\n                name: \\\"Jessica Right\\\"\\n                tag: \\\"pet\\\"\\n        default:\\n          description: \\\"unexpected error\\\"\\n          content:\\n            application/json:\\n              schema:\\n                $ref: \\\"#/components/schemas/Error\\\"\\n    put:\\n      summary: \\\"Update a pet\\\"\\n      operationId: \\\"updatePets\\\"\\n      parameters:\\n      - name: \\\"petId\\\"\\n        in: \\\"path\\\"\\n        required: true\\n        description: \\\"The id of the pet to update\\\"\\n        schema:\\n          type: \\\"string\\\"\\n      requestBody:\\n        description: \\\"Pet to update\\\"\\n        required: true\\n        content:\\n          application/json:\\n            schema:\\n              $ref: \\\"#/components/schemas/UpdatePet\\\"\\n      tags:\\n      - \\\"pets\\\"\\n      security:\\n      - petstore_auth:\\n        - \\\"read:pets\\\"\\n        - \\\"write:pets\\\"\\n      responses:\\n        \\\"200\\\":\\n          description: \\\"Successfully updated pets\\\"\\n        default:\\n          description: \\\"unexpected error\\\"\\n          content:\\n            application/json:\\n              schema:\\n                $ref: \\\"#/components/schemas/Error\\\"\\n    delete:\\n      summary: \\\"Delete a specific pet\\\"\\n      operationId: \\\"deletePetById\\\"\\n      tags:\\n      - \\\"pets\\\"\\n      parameters:\\n      - name: \\\"petId\\\"\\n        in: \\\"path\\\"\\n        required: true\\n        description: \\\"The id of the pet to delete\\\"\\n        schema:\\n          type: \\\"string\\\"\\n      - name: \\\"key\\\"\\n        in: \\\"header\\\"\\n        required: true\\n        description: \\\"The key header\\\"\\n        schema:\\n          type: \\\"string\\\"\\n      security:\\n      - petstore_auth:\\n        - \\\"write:pets\\\"\\n      responses:\\n        \\\"200\\\":\\n          description: \\\"Expected response to a valid request\\\"\\n          content:\\n            application/json:\\n              schema:\\n                $ref: \\\"#/components/schemas/Pet\\\"\\n              examples:\\n                response:\\n                  value:\\n                    id: 1\\n                    name: \\\"Jessica Right\\\"\\n                    tag: \\\"pet\\\"\\n        default:\\n          description: \\\"unexpected error\\\"\\n          content:\\n            application/json:\\n              schema:\\n                $ref: \\\"#/components/schemas/Error\\\"\\n  /accounts:\\n    get:\\n      summary: \\\"Get a list of accounts\\\"\\n      operationId: \\\"getAccounts\\\"\\n      tags:\\n      - \\\"accounts\\\"\\n      parameters:\\n      - name: \\\"limit\\\"\\n        in: \\\"query\\\"\\n        description: \\\"How many items to return at one time (max 100)\\\"\\n        required: false\\n        schema:\\n          type: \\\"integer\\\"\\n          format: \\\"int32\\\"\\n      security:\\n      - petstore_auth:\\n        - \\\"read:pets\\\"\\n      responses:\\n        \\\"200\\\":\\n          description: \\\"An paged array of accounts\\\"\\n          content:\\n            application/json:\\n              schema:\\n                type: \\\"array\\\"\\n                items:\\n                  $ref: \\\"#/components/schemas/Account\\\"\\n              example:\\n              - accountNo: 123\\n                ownerId: \\\"johndoe\\\"\\n                accountType: \\\"P\\\"\\n                firstName: \\\"John\\\"\\n                lastName: \\\"Doe\\\"\\n                status: \\\"O\\\"\\n              - id: 2\\n                accountNo: 456\\n                ownerId: \\\"johndoe\\\"\\n                accountType: \\\"B\\\"\\n                firstName: \\\"John\\\"\\n                lastName: \\\"Doe\\\"\\n                status: \\\"C\\\"\\n        default:\\n          description: \\\"unexpected error\\\"\\n          content:\\n            application/json:\\n              schema:\\n                $ref: \\\"#/components/schemas/Error\\\"\\n    post:\\n      summary: \\\"Create an account\\\"\\n      operationId: \\\"createAccount\\\"\\n      requestBody:\\n        description: \\\"Account to add to the system\\\"\\n        required: true\\n        content:\\n          application/json:\\n            schema:\\n              $ref: \\\"#/components/schemas/Account\\\"\\n      tags:\\n      - \\\"accounts\\\"\\n      security:\\n      - petstore_auth:\\n        - \\\"read:pets\\\"\\n        - \\\"write:pets\\\"\\n      responses:\\n        \\\"201\\\":\\n          description: \\\"Null response\\\"\\n        default:\\n          description: \\\"unexpected error\\\"\\n          content:\\n            application/json:\\n              schema:\\n                $ref: \\\"#/components/schemas/Error\\\"\\n  /accounts/{accountNo}:\\n    get:\\n      summary: \\\"get account with a specific accountNo\\\"\\n      operationId: \\\"getAccountByNo\\\"\\n      tags:\\n      - \\\"accounts\\\"\\n      parameters:\\n      - name: \\\"accountNo\\\"\\n        in: \\\"path\\\"\\n        required: true\\n        description: \\\"The accountNo of the account to retrieve\\\"\\n        schema:\\n          type: \\\"string\\\"\\n      security:\\n      - petstore_auth:\\n        - \\\"read:pets\\\"\\n      responses:\\n        \\\"200\\\":\\n          description: \\\"Expected response to a valid request\\\"\\n          content:\\n            application/json:\\n              schema:\\n                $ref: \\\"#/components/schemas/Account\\\"\\n              example:\\n                accountNo: 123\\n                ownerId: \\\"johndoe\\\"\\n                accountType: \\\"P\\\"\\n                firstName: \\\"John\\\"\\n                lastName: \\\"Doe\\\"\\n                status: \\\"O\\\"\\n        default:\\n          description: \\\"unexpected error\\\"\\n          content:\\n            application/json:\\n              schema:\\n                $ref: \\\"#/components/schemas/Error\\\"\\n    put:\\n      summary: \\\"Update an account by accountNo\\\"\\n      operationId: \\\"updateAccountByNo\\\"\\n      parameters:\\n      - name: \\\"accountNo\\\"\\n        in: \\\"path\\\"\\n        required: true\\n        description: \\\"The account no of the account to update\\\"\\n        schema:\\n          type: \\\"string\\\"\\n      requestBody:\\n        description: \\\"Account to update\\\"\\n        required: true\\n        content:\\n          application/json:\\n            schema:\\n              $ref: \\\"#/components/schemas/Account\\\"\\n      tags:\\n      - \\\"accounts\\\"\\n      security:\\n      - petstore_auth:\\n        - \\\"read:pets\\\"\\n        - \\\"write:pets\\\"\\n      responses:\\n        \\\"200\\\":\\n          description: \\\"Successfully updated accounts\\\"\\n        default:\\n          description: \\\"unexpected error\\\"\\n          content:\\n            application/json:\\n              schema:\\n                $ref: \\\"#/components/schemas/Error\\\"\\n    delete:\\n      summary: \\\"Delete a specific account by accountNo\\\"\\n      operationId: \\\"deleteAccountByNo\\\"\\n      tags:\\n      - \\\"accounts\\\"\\n      parameters:\\n      - name: \\\"accountNo\\\"\\n        in: \\\"path\\\"\\n        required: true\\n        description: \\\"The no of the account to delete\\\"\\n        schema:\\n          type: \\\"string\\\"\\n      security:\\n      - petstore_auth:\\n        - \\\"write:pets\\\"\\n      responses:\\n        \\\"200\\\":\\n          description: \\\"Expected response to a valid request\\\"\\n          content:\\n            application/json:\\n              schema:\\n                $ref: \\\"#/components/schemas/Account\\\"\\n              examples:\\n                response:\\n                  value:\\n                    accountNo: 123\\n                    ownerId: \\\"johndoe\\\"\\n                    accountType: \\\"P\\\"\\n                    firstName: \\\"John\\\"\\n                    lastName: \\\"Doe\\\"\\n                    status: \\\"O\\\"\\n        default:\\n          description: \\\"unexpected error\\\"\\n          content:\\n            application/json:\\n              schema:\\n                $ref: \\\"#/components/schemas/Error\\\"\\n  /notifications:\\n    get:\\n      summary: \\\"Get Notifications\\\"\\n      operationId: \\\"listNotifications\\\"\\n      tags:\\n      - \\\"notifications\\\"\\n      security:\\n      - petstore_auth:\\n        - \\\"read:pets\\\"\\n      responses:\\n        \\\"200\\\":\\n          description: \\\"A standard notification response in JSON for response interceptor\\\\\\n            \\\\ test\\\"\\n        default:\\n          description: \\\"unexpected error\\\"\\n          content:\\n            application/json:\\n              schema:\\n                $ref: \\\"#/components/schemas/Error\\\"\\n  /flowers:\\n    post:\\n      summary: \\\"The API accept XML and the consumer is using JSON\\\"\\n      operationId: \\\"flowers\\\"\\n      tags:\\n      - \\\"flowers\\\"\\n      security:\\n      - petstore_auth:\\n        - \\\"read:pets\\\"\\n      responses:\\n        \\\"200\\\":\\n          description: \\\"Return an flowers XML as the demo soap service\\\"\\n        default:\\n          description: \\\"unexpected error\\\"\\n          content:\\n            application/json:\\n              schema:\\n                $ref: \\\"#/components/schemas/Error\\\"\\n  /documents:\\n    get:\\n      summary: \\\"The API to get a document in a JSON with base64 content\\\"\\n      operationId: \\\"documents\\\"\\n      tags:\\n      - \\\"documents\\\"\\n      security:\\n      - petstore_auth:\\n        - \\\"read:pets\\\"\\n      responses:\\n        \\\"200\\\":\\n          description: \\\"Return a document with base64 content in JSON format\\\"\\n        default:\\n          description: \\\"unexpected error\\\"\\n          content:\\n            application/json:\\n              schema:\\n                $ref: \\\"#/components/schemas/Error\\\"\\n  /streams:\\n    get:\\n      summary: \\\"The API to get a stream of json response\\\"\\n      operationId: \\\"streams\\\"\\n      tags:\\n      - \\\"streams\\\"\\n      security:\\n      - petstore_auth:\\n        - \\\"read:pets\\\"\\n      responses:\\n        \\\"200\\\":\\n          description: \\\"Return a stream with json content in JSON format\\\"\\n        default:\\n          description: \\\"unexpected error\\\"\\n          content:\\n            application/json:\\n              schema:\\n                $ref: \\\"#/components/schemas/Error\\\"\\ncomponents:\\n  securitySchemes:\\n    petstore_auth:\\n      type: \\\"oauth2\\\"\\n      description: \\\"This API uses OAuth 2 with the client credential grant flow.\\\"\\n      flows:\\n        clientCredentials:\\n          tokenUrl: \\\"https://localhost:6882/token\\\"\\n          scopes:\\n            write:pets: \\\"modify pets in your account\\\"\\n            read:pets: \\\"read your pets\\\"\\n  schemas:\\n    Pet:\\n      allOf:\\n      - $ref: \\\"#/components/schemas/NewPet\\\"\\n      - type: \\\"object\\\"\\n        required:\\n        - \\\"id\\\"\\n        properties:\\n          id:\\n            type: \\\"integer\\\"\\n            format: \\\"int64\\\"\\n    NewPet:\\n      type: \\\"object\\\"\\n      required:\\n      - \\\"name\\\"\\n      properties:\\n        name:\\n          type: \\\"string\\\"\\n        tag:\\n          type: \\\"string\\\"\\n    UpdatePet:\\n      type: \\\"object\\\"\\n      properties:\\n        petAge:\\n          $ref: \\\"#/components/schemas/petAge\\\"\\n        petToys:\\n          $ref: \\\"#/components/schemas/petToys\\\"\\n        ownerEmail:\\n          $ref: \\\"#/components/schemas/ownerEmail\\\"\\n        ownerSsn:\\n          $ref: \\\"#/components/schemas/ownerSsn\\\"\\n      additionalProperties: false\\n    petAge:\\n      maximum: 20\\n      minimum: 1\\n      type: \\\"integer\\\"\\n      description: \\\"current age of the pet\\\"\\n      nullable: false\\n    petToys:\\n      type: \\\"array\\\"\\n      description: \\\"Toys of the pet\\\"\\n      items:\\n        type: \\\"string\\\"\\n    ownerEmail:\\n      maxLength: 65\\n      minLength: 2\\n      pattern: \\\"^[A-Z0-9._%+-]+@[A-Z0-9.-]+\\\\\\\\.[A-Z]{2,6}$\\\"\\n      type: \\\"string\\\"\\n      description: \\\"owner email\\\"\\n    ownerSsn:\\n      pattern: \\\"^\\\\\\\\d{3}-\\\\\\\\d{2}-\\\\\\\\d{4}$\\\"\\n      type: \\\"string\\\"\\n      description: \\\"owner SSN\\\"\\n    Account:\\n      type: \\\"object\\\"\\n      required:\\n      - \\\"accountNo\\\"\\n      - \\\"userId\\\"\\n      - \\\"accountType\\\"\\n      - \\\"status\\\"\\n      properties:\\n        accountNo:\\n          type: \\\"string\\\"\\n        userId:\\n          type: \\\"string\\\"\\n        accountType:\\n          type: \\\"string\\\"\\n        firstName:\\n          type: \\\"string\\\"\\n        lastName:\\n          type: \\\"string\\\"\\n        status:\\n          type: \\\"string\\\"\\n    Error:\\n      type: \\\"object\\\"\\n      required:\\n      - \\\"code\\\"\\n      - \\\"message\\\"\\n      properties:\\n        code:\\n          type: \\\"integer\\\"\\n          format: \\\"int32\\\"\\n        message:\\n          type: \\\"string\\\"\\n\",\"apiId\":\"0100\",\"hostId\":\"01964b05-552a-7c4b-9184-6857e7f3dc5f\",\"apiType\":\"openapi\",\"updateTs\":\"2025-08-19T18:02:24.798737Z\",\"endpoints\":[{\"scopes\":[\"read:pets\"],\"endpoint\":\"/v1/pets@get\",\"endpointId\":\"0198c384-7963-735e-a174-40a55788a35e\",\"httpMethod\":\"get\",\"endpointDesc\":\"List all pets\",\"endpointName\":\"listPets\",\"endpointPath\":\"/pets\"},{\"scopes\":[\"read:pets\",\"write:pets\"],\"endpoint\":\"/v1/pets@post\",\"endpointId\":\"0198c384-7963-7434-a176-55d7c50e4ec8\",\"httpMethod\":\"post\",\"endpointDesc\":\"Create a pet\",\"endpointName\":\"createPets\",\"endpointPath\":\"/pets\"},{\"scopes\":[\"read:pets\"],\"endpoint\":\"/v1/pets/{petId}@get\",\"endpointId\":\"0198c384-7963-7468-a178-a7b5e5fcecf9\",\"httpMethod\":\"get\",\"endpointDesc\":\"Info for a specific pet\",\"endpointName\":\"showPetById\",\"endpointPath\":\"/pets/{petId}\"},{\"scopes\":[\"read:pets\",\"write:pets\"],\"endpoint\":\"/v1/pets/{petId}@put\",\"endpointId\":\"0198c384-7963-7499-a17a-29eac620bebe\",\"httpMethod\":\"put\",\"endpointDesc\":\"Update a pet\",\"endpointName\":\"updatePets\",\"endpointPath\":\"/pets/{petId}\"},{\"scopes\":[\"write:pets\"],\"endpoint\":\"/v1/pets/{petId}@delete\",\"endpointId\":\"0198c384-7963-74bd-a17c-c8ae88485bcd\",\"httpMethod\":\"delete\",\"endpointDesc\":\"Delete a specific pet\",\"endpointName\":\"deletePetById\",\"endpointPath\":\"/pets/{petId}\"},{\"scopes\":[\"read:pets\"],\"endpoint\":\"/v1/accounts@get\",\"endpointId\":\"0198c384-7963-74e4-a17e-02c0f1576da1\",\"httpMethod\":\"get\",\"endpointDesc\":\"Get a list of accounts\",\"endpointName\":\"getAccounts\",\"endpointPath\":\"/accounts\"},{\"scopes\":[\"read:pets\",\"write:pets\"],\"endpoint\":\"/v1/accounts@post\",\"endpointId\":\"0198c384-7963-750f-a180-9a9043a67a24\",\"httpMethod\":\"post\",\"endpointDesc\":\"Create an account\",\"endpointName\":\"createAccount\",\"endpointPath\":\"/accounts\"},{\"scopes\":[\"read:pets\"],\"endpoint\":\"/v1/accounts/{accountNo}@get\",\"endpointId\":\"0198c384-7963-7535-a182-837e6d484c4a\",\"httpMethod\":\"get\",\"endpointDesc\":\"get account with a specific accountNo\",\"endpointName\":\"getAccountByNo\",\"endpointPath\":\"/accounts/{accountNo}\"},{\"scopes\":[\"read:pets\",\"write:pets\"],\"endpoint\":\"/v1/accounts/{accountNo}@put\",\"endpointId\":\"0198c384-7963-7560-a184-dc02fce8c093\",\"httpMethod\":\"put\",\"endpointDesc\":\"Update an account by accountNo\",\"endpointName\":\"updateAccountByNo\",\"endpointPath\":\"/accounts/{accountNo}\"},{\"scopes\":[\"write:pets\"],\"endpoint\":\"/v1/accounts/{accountNo}@delete\",\"endpointId\":\"0198c384-7963-7580-a186-e358d42c33dd\",\"httpMethod\":\"delete\",\"endpointDesc\":\"Delete a specific account by accountNo\",\"endpointName\":\"deleteAccountByNo\",\"endpointPath\":\"/accounts/{accountNo}\"},{\"scopes\":[\"read:pets\"],\"endpoint\":\"/v1/notifications@get\",\"endpointId\":\"0198c384-7963-75a6-a188-166a086e8050\",\"httpMethod\":\"get\",\"endpointDesc\":\"Get Notifications\",\"endpointName\":\"listNotifications\",\"endpointPath\":\"/notifications\"},{\"scopes\":[\"read:pets\"],\"endpoint\":\"/v1/flowers@post\",\"endpointId\":\"0198c384-7963-75d5-a18a-40fb0d532963\",\"httpMethod\":\"post\",\"endpointDesc\":\"The API accept XML and the consumer is using JSON\",\"endpointName\":\"flowers\",\"endpointPath\":\"/flowers\"},{\"scopes\":[\"read:pets\"],\"endpoint\":\"/v1/documents@get\",\"endpointId\":\"0198c384-7963-7629-a18c-f837cb2a2119\",\"httpMethod\":\"get\",\"endpointDesc\":\"The API to get a document in a JSON with base64 content\",\"endpointName\":\"documents\",\"endpointPath\":\"/documents\"},{\"scopes\":[\"read:pets\"],\"endpoint\":\"/v1/streams@get\",\"endpointId\":\"0198c384-7963-765b-a18e-c84a7b5af3be\",\"httpMethod\":\"get\",\"endpointDesc\":\"The API to get a stream of json response\",\"endpointName\":\"streams\",\"endpointPath\":\"/streams\"}],\"serviceId\":\"com.networknt.petstore-1.0.0\",\"apiVersion\":\"1.0.0\",\"updateUser\":\"postgres\",\"apiVersionId\":\"019664ec-c3e4-71f0-9b6c-3c0893ee688e\",\"apiVersionDesc\":\"First Major release\",\"aggregateVersion\":4,\"newAggregateVersion\":5},\"aggregateversion\":\"4\",\"subject\":\"019664ec-c3e4-71f0-9b6c-3c0893ee688e\",\"source\":\"https://github.com/lightapi/light-portal\",\"type\":\"ServiceSpecUpdatedEvent\",\"nonce\":1612,\"host\":\"01964b05-552a-7c4b-9184-6857e7f3dc5f\",\"specversion\":\"1.0\",\"id\":\"0198c384-79c6-7e9d-af45-85e814457d7f\",\"time\":\"2025-08-19T18:08:15.814963813Z\",\"user\":\"01964b05-5532-7c79-8cde-191dcbd421b8\",\"aggregatetype\":\"ServiceSpec\"}";
        Map<String, Object> map = JsonMapper.string2Map(s);
        Connection conn = SqlDbStartupHook.ds.getConnection();
        dbProvider.updateApiVersionSpec(conn, map);
    }

    @Test
    void testGetToValueCodeMultiple() throws Exception {
        Result<String> result = dbProvider.getToValueCode("service_aggregate", "User,Host");
        if(result.isFailure()) {
            System.out.println(result.getError());
        } else {
            System.out.println(result.getResult());
        }
    }

    @Test
    void testGetToValueCodeSingle() throws Exception {
        Result<String> result = dbProvider.getToValueCode("service_aggregate", "Host");
        if(result.isFailure()) {
            System.out.println(result.getError());
        } else {
            System.out.println(result.getResult());
        }
    }

    @Test
    void testGetToValueCodeNull() throws Exception {
        Result<String> result = dbProvider.getToValueCode("service_aggregate", null);
        if(result.isFailure()) {
            System.out.println(result.getError());
        } else {
            System.out.println(result.getResult());
        }
    }

    @Test
    @Disabled
    void testCreateProductVersionConfig() throws Exception {
        // To make it work, you need to query the database and update the aggregateVersion in the following JSON string.
        String s = "{\"datacontenttype\":\"application/json\",\"data\":{\"spec\":\"---\\nopenapi: \\\"3.1.0\\\"\\ninfo:\\n  version: \\\"1.0.0\\\"\\n  title: \\\"Swagger Petstore\\\"\\n  license:\\n    name: \\\"MIT\\\"\\nservers:\\n- url: \\\"http://petstore.swagger.io/v1\\\"\\npaths:\\n  /pets:\\n    get:\\n      summary: \\\"List all pets\\\"\\n      operationId: \\\"listPets\\\"\\n      tags:\\n      - \\\"pets\\\"\\n      parameters:\\n      - name: \\\"limit\\\"\\n        in: \\\"query\\\"\\n        description: \\\"How many items to return at one time (max 100)\\\"\\n        required: false\\n        schema:\\n          type: \\\"integer\\\"\\n          format: \\\"int32\\\"\\n      security:\\n      - petstore_auth:\\n        - \\\"read:pets\\\"\\n      responses:\\n        \\\"200\\\":\\n          description: \\\"An paged array of pets\\\"\\n          headers:\\n            x-next:\\n              description: \\\"A link to the next page of responses\\\"\\n              schema:\\n                type: \\\"string\\\"\\n          content:\\n            application/json:\\n              schema:\\n                type: \\\"array\\\"\\n                items:\\n                  $ref: \\\"#/components/schemas/Pet\\\"\\n              example:\\n              - id: 1\\n                name: \\\"catten\\\"\\n                tag: \\\"cat\\\"\\n              - id: 2\\n                name: \\\"doggy\\\"\\n                tag: \\\"dog\\\"\\n        default:\\n          description: \\\"unexpected error\\\"\\n          content:\\n            application/json:\\n              schema:\\n                $ref: \\\"#/components/schemas/Error\\\"\\n    post:\\n      summary: \\\"Create a pet\\\"\\n      operationId: \\\"createPets\\\"\\n      requestBody:\\n        description: \\\"Pet to add to the store\\\"\\n        required: true\\n        content:\\n          application/json:\\n            schema:\\n              $ref: \\\"#/components/schemas/Pet\\\"\\n      tags:\\n      - \\\"pets\\\"\\n      security:\\n      - petstore_auth:\\n        - \\\"read:pets\\\"\\n        - \\\"write:pets\\\"\\n      responses:\\n        \\\"201\\\":\\n          description: \\\"Null response\\\"\\n        default:\\n          description: \\\"unexpected error\\\"\\n          content:\\n            application/json:\\n              schema:\\n                $ref: \\\"#/components/schemas/Error\\\"\\n  /pets/{petId}:\\n    get:\\n      summary: \\\"Info for a specific pet\\\"\\n      operationId: \\\"showPetById\\\"\\n      tags:\\n      - \\\"pets\\\"\\n      parameters:\\n      - name: \\\"petId\\\"\\n        in: \\\"path\\\"\\n        required: true\\n        description: \\\"The id of the pet to retrieve\\\"\\n        schema:\\n          type: \\\"string\\\"\\n      security:\\n      - petstore_auth:\\n        - \\\"read:pets\\\"\\n      responses:\\n        \\\"200\\\":\\n          description: \\\"Expected response to a valid request\\\"\\n          content:\\n            application/json:\\n              schema:\\n                $ref: \\\"#/components/schemas/Pet\\\"\\n              example:\\n                id: 1\\n                name: \\\"Jessica Right\\\"\\n                tag: \\\"pet\\\"\\n        default:\\n          description: \\\"unexpected error\\\"\\n          content:\\n            application/json:\\n              schema:\\n                $ref: \\\"#/components/schemas/Error\\\"\\n    put:\\n      summary: \\\"Update a pet\\\"\\n      operationId: \\\"updatePets\\\"\\n      parameters:\\n      - name: \\\"petId\\\"\\n        in: \\\"path\\\"\\n        required: true\\n        description: \\\"The id of the pet to update\\\"\\n        schema:\\n          type: \\\"string\\\"\\n      requestBody:\\n        description: \\\"Pet to update\\\"\\n        required: true\\n        content:\\n          application/json:\\n            schema:\\n              $ref: \\\"#/components/schemas/UpdatePet\\\"\\n      tags:\\n      - \\\"pets\\\"\\n      security:\\n      - petstore_auth:\\n        - \\\"read:pets\\\"\\n        - \\\"write:pets\\\"\\n      responses:\\n        \\\"200\\\":\\n          description: \\\"Successfully updated pets\\\"\\n        default:\\n          description: \\\"unexpected error\\\"\\n          content:\\n            application/json:\\n              schema:\\n                $ref: \\\"#/components/schemas/Error\\\"\\n    delete:\\n      summary: \\\"Delete a specific pet\\\"\\n      operationId: \\\"deletePetById\\\"\\n      tags:\\n      - \\\"pets\\\"\\n      parameters:\\n      - name: \\\"petId\\\"\\n        in: \\\"path\\\"\\n        required: true\\n        description: \\\"The id of the pet to delete\\\"\\n        schema:\\n          type: \\\"string\\\"\\n      - name: \\\"key\\\"\\n        in: \\\"header\\\"\\n        required: true\\n        description: \\\"The key header\\\"\\n        schema:\\n          type: \\\"string\\\"\\n      security:\\n      - petstore_auth:\\n        - \\\"write:pets\\\"\\n      responses:\\n        \\\"200\\\":\\n          description: \\\"Expected response to a valid request\\\"\\n          content:\\n            application/json:\\n              schema:\\n                $ref: \\\"#/components/schemas/Pet\\\"\\n              examples:\\n                response:\\n                  value:\\n                    id: 1\\n                    name: \\\"Jessica Right\\\"\\n                    tag: \\\"pet\\\"\\n        default:\\n          description: \\\"unexpected error\\\"\\n          content:\\n            application/json:\\n              schema:\\n                $ref: \\\"#/components/schemas/Error\\\"\\n  /accounts:\\n    get:\\n      summary: \\\"Get a list of accounts\\\"\\n      operationId: \\\"getAccounts\\\"\\n      tags:\\n      - \\\"accounts\\\"\\n      parameters:\\n      - name: \\\"limit\\\"\\n        in: \\\"query\\\"\\n        description: \\\"How many items to return at one time (max 100)\\\"\\n        required: false\\n        schema:\\n          type: \\\"integer\\\"\\n          format: \\\"int32\\\"\\n      security:\\n      - petstore_auth:\\n        - \\\"read:pets\\\"\\n      responses:\\n        \\\"200\\\":\\n          description: \\\"An paged array of accounts\\\"\\n          content:\\n            application/json:\\n              schema:\\n                type: \\\"array\\\"\\n                items:\\n                  $ref: \\\"#/components/schemas/Account\\\"\\n              example:\\n              - accountNo: 123\\n                ownerId: \\\"johndoe\\\"\\n                accountType: \\\"P\\\"\\n                firstName: \\\"John\\\"\\n                lastName: \\\"Doe\\\"\\n                status: \\\"O\\\"\\n              - id: 2\\n                accountNo: 456\\n                ownerId: \\\"johndoe\\\"\\n                accountType: \\\"B\\\"\\n                firstName: \\\"John\\\"\\n                lastName: \\\"Doe\\\"\\n                status: \\\"C\\\"\\n        default:\\n          description: \\\"unexpected error\\\"\\n          content:\\n            application/json:\\n              schema:\\n                $ref: \\\"#/components/schemas/Error\\\"\\n    post:\\n      summary: \\\"Create an account\\\"\\n      operationId: \\\"createAccount\\\"\\n      requestBody:\\n        description: \\\"Account to add to the system\\\"\\n        required: true\\n        content:\\n          application/json:\\n            schema:\\n              $ref: \\\"#/components/schemas/Account\\\"\\n      tags:\\n      - \\\"accounts\\\"\\n      security:\\n      - petstore_auth:\\n        - \\\"read:pets\\\"\\n        - \\\"write:pets\\\"\\n      responses:\\n        \\\"201\\\":\\n          description: \\\"Null response\\\"\\n        default:\\n          description: \\\"unexpected error\\\"\\n          content:\\n            application/json:\\n              schema:\\n                $ref: \\\"#/components/schemas/Error\\\"\\n  /accounts/{accountNo}:\\n    get:\\n      summary: \\\"get account with a specific accountNo\\\"\\n      operationId: \\\"getAccountByNo\\\"\\n      tags:\\n      - \\\"accounts\\\"\\n      parameters:\\n      - name: \\\"accountNo\\\"\\n        in: \\\"path\\\"\\n        required: true\\n        description: \\\"The accountNo of the account to retrieve\\\"\\n        schema:\\n          type: \\\"string\\\"\\n      security:\\n      - petstore_auth:\\n        - \\\"read:pets\\\"\\n      responses:\\n        \\\"200\\\":\\n          description: \\\"Expected response to a valid request\\\"\\n          content:\\n            application/json:\\n              schema:\\n                $ref: \\\"#/components/schemas/Account\\\"\\n              example:\\n                accountNo: 123\\n                ownerId: \\\"johndoe\\\"\\n                accountType: \\\"P\\\"\\n                firstName: \\\"John\\\"\\n                lastName: \\\"Doe\\\"\\n                status: \\\"O\\\"\\n        default:\\n          description: \\\"unexpected error\\\"\\n          content:\\n            application/json:\\n              schema:\\n                $ref: \\\"#/components/schemas/Error\\\"\\n    put:\\n      summary: \\\"Update an account by accountNo\\\"\\n      operationId: \\\"updateAccountByNo\\\"\\n      parameters:\\n      - name: \\\"accountNo\\\"\\n        in: \\\"path\\\"\\n        required: true\\n        description: \\\"The account no of the account to update\\\"\\n        schema:\\n          type: \\\"string\\\"\\n      requestBody:\\n        description: \\\"Account to update\\\"\\n        required: true\\n        content:\\n          application/json:\\n            schema:\\n              $ref: \\\"#/components/schemas/Account\\\"\\n      tags:\\n      - \\\"accounts\\\"\\n      security:\\n      - petstore_auth:\\n        - \\\"read:pets\\\"\\n        - \\\"write:pets\\\"\\n      responses:\\n        \\\"200\\\":\\n          description: \\\"Successfully updated accounts\\\"\\n        default:\\n          description: \\\"unexpected error\\\"\\n          content:\\n            application/json:\\n              schema:\\n                $ref: \\\"#/components/schemas/Error\\\"\\n    delete:\\n      summary: \\\"Delete a specific account by accountNo\\\"\\n      operationId: \\\"deleteAccountByNo\\\"\\n      tags:\\n      - \\\"accounts\\\"\\n      parameters:\\n      - name: \\\"accountNo\\\"\\n        in: \\\"path\\\"\\n        required: true\\n        description: \\\"The no of the account to delete\\\"\\n        schema:\\n          type: \\\"string\\\"\\n      security:\\n      - petstore_auth:\\n        - \\\"write:pets\\\"\\n      responses:\\n        \\\"200\\\":\\n          description: \\\"Expected response to a valid request\\\"\\n          content:\\n            application/json:\\n              schema:\\n                $ref: \\\"#/components/schemas/Account\\\"\\n              examples:\\n                response:\\n                  value:\\n                    accountNo: 123\\n                    ownerId: \\\"johndoe\\\"\\n                    accountType: \\\"P\\\"\\n                    firstName: \\\"John\\\"\\n                    lastName: \\\"Doe\\\"\\n                    status: \\\"O\\\"\\n        default:\\n          description: \\\"unexpected error\\\"\\n          content:\\n            application/json:\\n              schema:\\n                $ref: \\\"#/components/schemas/Error\\\"\\n  /notifications:\\n    get:\\n      summary: \\\"Get Notifications\\\"\\n      operationId: \\\"listNotifications\\\"\\n      tags:\\n      - \\\"notifications\\\"\\n      security:\\n      - petstore_auth:\\n        - \\\"read:pets\\\"\\n      responses:\\n        \\\"200\\\":\\n          description: \\\"A standard notification response in JSON for response interceptor\\\\\\n            \\\\ test\\\"\\n        default:\\n          description: \\\"unexpected error\\\"\\n          content:\\n            application/json:\\n              schema:\\n                $ref: \\\"#/components/schemas/Error\\\"\\n  /flowers:\\n    post:\\n      summary: \\\"The API accept XML and the consumer is using JSON\\\"\\n      operationId: \\\"flowers\\\"\\n      tags:\\n      - \\\"flowers\\\"\\n      security:\\n      - petstore_auth:\\n        - \\\"read:pets\\\"\\n      responses:\\n        \\\"200\\\":\\n          description: \\\"Return an flowers XML as the demo soap service\\\"\\n        default:\\n          description: \\\"unexpected error\\\"\\n          content:\\n            application/json:\\n              schema:\\n                $ref: \\\"#/components/schemas/Error\\\"\\n  /documents:\\n    get:\\n      summary: \\\"The API to get a document in a JSON with base64 content\\\"\\n      operationId: \\\"documents\\\"\\n      tags:\\n      - \\\"documents\\\"\\n      security:\\n      - petstore_auth:\\n        - \\\"read:pets\\\"\\n      responses:\\n        \\\"200\\\":\\n          description: \\\"Return a document with base64 content in JSON format\\\"\\n        default:\\n          description: \\\"unexpected error\\\"\\n          content:\\n            application/json:\\n              schema:\\n                $ref: \\\"#/components/schemas/Error\\\"\\n  /streams:\\n    get:\\n      summary: \\\"The API to get a stream of json response\\\"\\n      operationId: \\\"streams\\\"\\n      tags:\\n      - \\\"streams\\\"\\n      security:\\n      - petstore_auth:\\n        - \\\"read:pets\\\"\\n      responses:\\n        \\\"200\\\":\\n          description: \\\"Return a stream with json content in JSON format\\\"\\n        default:\\n          description: \\\"unexpected error\\\"\\n          content:\\n            application/json:\\n              schema:\\n                $ref: \\\"#/components/schemas/Error\\\"\\ncomponents:\\n  securitySchemes:\\n    petstore_auth:\\n      type: \\\"oauth2\\\"\\n      description: \\\"This API uses OAuth 2 with the client credential grant flow.\\\"\\n      flows:\\n        clientCredentials:\\n          tokenUrl: \\\"https://localhost:6882/token\\\"\\n          scopes:\\n            write:pets: \\\"modify pets in your account\\\"\\n            read:pets: \\\"read your pets\\\"\\n  schemas:\\n    Pet:\\n      allOf:\\n      - $ref: \\\"#/components/schemas/NewPet\\\"\\n      - type: \\\"object\\\"\\n        required:\\n        - \\\"id\\\"\\n        properties:\\n          id:\\n            type: \\\"integer\\\"\\n            format: \\\"int64\\\"\\n    NewPet:\\n      type: \\\"object\\\"\\n      required:\\n      - \\\"name\\\"\\n      properties:\\n        name:\\n          type: \\\"string\\\"\\n        tag:\\n          type: \\\"string\\\"\\n    UpdatePet:\\n      type: \\\"object\\\"\\n      properties:\\n        petAge:\\n          $ref: \\\"#/components/schemas/petAge\\\"\\n        petToys:\\n          $ref: \\\"#/components/schemas/petToys\\\"\\n        ownerEmail:\\n          $ref: \\\"#/components/schemas/ownerEmail\\\"\\n        ownerSsn:\\n          $ref: \\\"#/components/schemas/ownerSsn\\\"\\n      additionalProperties: false\\n    petAge:\\n      maximum: 20\\n      minimum: 1\\n      type: \\\"integer\\\"\\n      description: \\\"current age of the pet\\\"\\n      nullable: false\\n    petToys:\\n      type: \\\"array\\\"\\n      description: \\\"Toys of the pet\\\"\\n      items:\\n        type: \\\"string\\\"\\n    ownerEmail:\\n      maxLength: 65\\n      minLength: 2\\n      pattern: \\\"^[A-Z0-9._%+-]+@[A-Z0-9.-]+\\\\\\\\.[A-Z]{2,6}$\\\"\\n      type: \\\"string\\\"\\n      description: \\\"owner email\\\"\\n    ownerSsn:\\n      pattern: \\\"^\\\\\\\\d{3}-\\\\\\\\d{2}-\\\\\\\\d{4}$\\\"\\n      type: \\\"string\\\"\\n      description: \\\"owner SSN\\\"\\n    Account:\\n      type: \\\"object\\\"\\n      required:\\n      - \\\"accountNo\\\"\\n      - \\\"userId\\\"\\n      - \\\"accountType\\\"\\n      - \\\"status\\\"\\n      properties:\\n        accountNo:\\n          type: \\\"string\\\"\\n        userId:\\n          type: \\\"string\\\"\\n        accountType:\\n          type: \\\"string\\\"\\n        firstName:\\n          type: \\\"string\\\"\\n        lastName:\\n          type: \\\"string\\\"\\n        status:\\n          type: \\\"string\\\"\\n    Error:\\n      type: \\\"object\\\"\\n      required:\\n      - \\\"code\\\"\\n      - \\\"message\\\"\\n      properties:\\n        code:\\n          type: \\\"integer\\\"\\n          format: \\\"int32\\\"\\n        message:\\n          type: \\\"string\\\"\\n\",\"apiId\":\"0100\",\"hostId\":\"01964b05-552a-7c4b-9184-6857e7f3dc5f\",\"apiType\":\"openapi\",\"updateTs\":\"2025-08-19T18:02:24.798737Z\",\"endpoints\":[{\"scopes\":[\"read:pets\"],\"endpoint\":\"/v1/pets@get\",\"endpointId\":\"0198c384-7963-735e-a174-40a55788a35e\",\"httpMethod\":\"get\",\"endpointDesc\":\"List all pets\",\"endpointName\":\"listPets\",\"endpointPath\":\"/pets\"},{\"scopes\":[\"read:pets\",\"write:pets\"],\"endpoint\":\"/v1/pets@post\",\"endpointId\":\"0198c384-7963-7434-a176-55d7c50e4ec8\",\"httpMethod\":\"post\",\"endpointDesc\":\"Create a pet\",\"endpointName\":\"createPets\",\"endpointPath\":\"/pets\"},{\"scopes\":[\"read:pets\"],\"endpoint\":\"/v1/pets/{petId}@get\",\"endpointId\":\"0198c384-7963-7468-a178-a7b5e5fcecf9\",\"httpMethod\":\"get\",\"endpointDesc\":\"Info for a specific pet\",\"endpointName\":\"showPetById\",\"endpointPath\":\"/pets/{petId}\"},{\"scopes\":[\"read:pets\",\"write:pets\"],\"endpoint\":\"/v1/pets/{petId}@put\",\"endpointId\":\"0198c384-7963-7499-a17a-29eac620bebe\",\"httpMethod\":\"put\",\"endpointDesc\":\"Update a pet\",\"endpointName\":\"updatePets\",\"endpointPath\":\"/pets/{petId}\"},{\"scopes\":[\"write:pets\"],\"endpoint\":\"/v1/pets/{petId}@delete\",\"endpointId\":\"0198c384-7963-74bd-a17c-c8ae88485bcd\",\"httpMethod\":\"delete\",\"endpointDesc\":\"Delete a specific pet\",\"endpointName\":\"deletePetById\",\"endpointPath\":\"/pets/{petId}\"},{\"scopes\":[\"read:pets\"],\"endpoint\":\"/v1/accounts@get\",\"endpointId\":\"0198c384-7963-74e4-a17e-02c0f1576da1\",\"httpMethod\":\"get\",\"endpointDesc\":\"Get a list of accounts\",\"endpointName\":\"getAccounts\",\"endpointPath\":\"/accounts\"},{\"scopes\":[\"read:pets\",\"write:pets\"],\"endpoint\":\"/v1/accounts@post\",\"endpointId\":\"0198c384-7963-750f-a180-9a9043a67a24\",\"httpMethod\":\"post\",\"endpointDesc\":\"Create an account\",\"endpointName\":\"createAccount\",\"endpointPath\":\"/accounts\"},{\"scopes\":[\"read:pets\"],\"endpoint\":\"/v1/accounts/{accountNo}@get\",\"endpointId\":\"0198c384-7963-7535-a182-837e6d484c4a\",\"httpMethod\":\"get\",\"endpointDesc\":\"get account with a specific accountNo\",\"endpointName\":\"getAccountByNo\",\"endpointPath\":\"/accounts/{accountNo}\"},{\"scopes\":[\"read:pets\",\"write:pets\"],\"endpoint\":\"/v1/accounts/{accountNo}@put\",\"endpointId\":\"0198c384-7963-7560-a184-dc02fce8c093\",\"httpMethod\":\"put\",\"endpointDesc\":\"Update an account by accountNo\",\"endpointName\":\"updateAccountByNo\",\"endpointPath\":\"/accounts/{accountNo}\"},{\"scopes\":[\"write:pets\"],\"endpoint\":\"/v1/accounts/{accountNo}@delete\",\"endpointId\":\"0198c384-7963-7580-a186-e358d42c33dd\",\"httpMethod\":\"delete\",\"endpointDesc\":\"Delete a specific account by accountNo\",\"endpointName\":\"deleteAccountByNo\",\"endpointPath\":\"/accounts/{accountNo}\"},{\"scopes\":[\"read:pets\"],\"endpoint\":\"/v1/notifications@get\",\"endpointId\":\"0198c384-7963-75a6-a188-166a086e8050\",\"httpMethod\":\"get\",\"endpointDesc\":\"Get Notifications\",\"endpointName\":\"listNotifications\",\"endpointPath\":\"/notifications\"},{\"scopes\":[\"read:pets\"],\"endpoint\":\"/v1/flowers@post\",\"endpointId\":\"0198c384-7963-75d5-a18a-40fb0d532963\",\"httpMethod\":\"post\",\"endpointDesc\":\"The API accept XML and the consumer is using JSON\",\"endpointName\":\"flowers\",\"endpointPath\":\"/flowers\"},{\"scopes\":[\"read:pets\"],\"endpoint\":\"/v1/documents@get\",\"endpointId\":\"0198c384-7963-7629-a18c-f837cb2a2119\",\"httpMethod\":\"get\",\"endpointDesc\":\"The API to get a document in a JSON with base64 content\",\"endpointName\":\"documents\",\"endpointPath\":\"/documents\"},{\"scopes\":[\"read:pets\"],\"endpoint\":\"/v1/streams@get\",\"endpointId\":\"0198c384-7963-765b-a18e-c84a7b5af3be\",\"httpMethod\":\"get\",\"endpointDesc\":\"The API to get a stream of json response\",\"endpointName\":\"streams\",\"endpointPath\":\"/streams\"}],\"serviceId\":\"com.networknt.petstore-1.0.0\",\"apiVersion\":\"1.0.0\",\"updateUser\":\"postgres\",\"apiVersionId\":\"019664ec-c3e4-71f0-9b6c-3c0893ee688e\",\"apiVersionDesc\":\"First Major release\",\"aggregateVersion\":4,\"newAggregateVersion\":5},\"aggregateversion\":\"4\",\"subject\":\"019664ec-c3e4-71f0-9b6c-3c0893ee688e\",\"source\":\"https://github.com/lightapi/light-portal\",\"type\":\"ServiceSpecUpdatedEvent\",\"nonce\":1612,\"host\":\"01964b05-552a-7c4b-9184-6857e7f3dc5f\",\"specversion\":\"1.0\",\"id\":\"0198c384-79c6-7e9d-af45-85e814457d7f\",\"time\":\"2025-08-19T18:08:15.814963813Z\",\"user\":\"01964b05-5532-7c79-8cde-191dcbd421b8\",\"aggregatetype\":\"ServiceSpec\"}";
        Map<String, Object> map = JsonMapper.string2Map(s);
        Connection conn = SqlDbStartupHook.ds.getConnection();
        dbProvider.updateApiVersionSpec(conn, map);
    }

    @Test
    @Disabled
    void testCreateApiVersion() throws Exception {
        // To make it work, you need to query the database and update the aggregateVersion in the following JSON string.
        String s = "{\"datacontenttype\":\"application/json\",\"data\":{\"spec\":\"host: \\\"lightapi.net\\\"\\nservice: \\\"user\\\"\\nschemas:\\n  createUserRequest:\\n    title: \\\"Service\\\"\\n    type: \\\"object\\\"\\n    properties:\\n      hostId:\\n        type: \\\"string\\\"\\n        description: \\\"user host indicator the organization for user\\\"\\n      email:\\n        type: \\\"string\\\"\\n        description: \\\"user email address\\\"\\n      entityId:\\n        type: \\\"string\\\"\\n        description: \\\"a unique entity id for user type\\\"\\n      userType:\\n        title: \\\"User Type\\\"\\n        type: \\\"string\\\"\\n      referralId:\\n        type: \\\"string\\\"\\n        title: \\\"Referral Id or Email\\\"\\n      managerId:\\n        type: \\\"string\\\"\\n        title: \\\"Manager Id or Email\\\"\\n      language:\\n        type: \\\"string\\\"\\n        description: \\\"preferred language of user\\\"\\n        enum:\\n        - \\\"en\\\"\\n      password:\\n        type: \\\"string\\\"\\n        description: \\\"raw password of user\\\"\\n      passwordConfirm:\\n        type: \\\"string\\\"\\n        description: \\\"password confirm of user\\\"\\n      firstName:\\n        type: \\\"string\\\"\\n        description: \\\"firstName of the user\\\"\\n      lastName:\\n        type: \\\"string\\\"\\n        description: \\\"firstName of the user\\\"\\n      phoneNumber:\\n        type: \\\"string\\\"\\n        title: \\\"Phone Number\\\"\\n      gender:\\n        type: \\\"string\\\"\\n        description: \\\"user gender type\\\"\\n      birthday:\\n        type: \\\"string\\\"\\n        format: \\\"date\\\"\\n        description: \\\"birthday of the user\\\"\\n      country:\\n        type: \\\"string\\\"\\n        description: \\\"country name\\\"\\n      province:\\n        type: \\\"string\\\"\\n        description: \\\"province or state name\\\"\\n      city:\\n        type: \\\"string\\\"\\n        description: \\\"city name\\\"\\n      postCode:\\n        type: \\\"string\\\"\\n        description: \\\"postCode or zipCode\\\"\\n      address:\\n        type: \\\"string\\\"\\n        description: \\\"address\\\"\\n    required:\\n    - \\\"hostId\\\"\\n    - \\\"email\\\"\\n    - \\\"entityId\\\"\\n    - \\\"userType\\\"\\n    - \\\"language\\\"\\n    - \\\"password\\\"\\n    - \\\"passwordConfirm\\\"\\n  onboardUserRequest:\\n    title: \\\"Service\\\"\\n    type: \\\"object\\\"\\n    properties:\\n      hostId:\\n        type: \\\"string\\\"\\n        description: \\\"user host indicator the organization for user\\\"\\n      email:\\n        type: \\\"string\\\"\\n        description: \\\"user email address\\\"\\n      entityId:\\n        type: \\\"string\\\"\\n        description: \\\"a unique entity id for user type\\\"\\n      userType:\\n        title: \\\"User Type\\\"\\n        type: \\\"string\\\"\\n      referralId:\\n        type: \\\"string\\\"\\n        title: \\\"Referral Id or Email\\\"\\n      managerId:\\n        type: \\\"string\\\"\\n        title: \\\"Manager Id or Email\\\"\\n      language:\\n        type: \\\"string\\\"\\n        description: \\\"preferred language of user\\\"\\n        enum:\\n        - \\\"en\\\"\\n      firstName:\\n        type: \\\"string\\\"\\n        description: \\\"firstName of the user\\\"\\n      lastName:\\n        type: \\\"string\\\"\\n        description: \\\"firstName of the user\\\"\\n      phoneNumber:\\n        type: \\\"string\\\"\\n        title: \\\"Phone Number\\\"\\n      gender:\\n        type: \\\"string\\\"\\n        description: \\\"user gender type\\\"\\n      birthday:\\n        type: \\\"string\\\"\\n        format: \\\"date\\\"\\n        description: \\\"birthday of the user\\\"\\n      country:\\n        type: \\\"string\\\"\\n        description: \\\"country name\\\"\\n      province:\\n        type: \\\"string\\\"\\n        description: \\\"province or state name\\\"\\n      city:\\n        type: \\\"string\\\"\\n        description: \\\"city name\\\"\\n      postCode:\\n        type: \\\"string\\\"\\n        description: \\\"postCode or zipCode\\\"\\n      address:\\n        type: \\\"string\\\"\\n        description: \\\"address\\\"\\n    required:\\n    - \\\"hostId\\\"\\n    - \\\"email\\\"\\n    - \\\"entityId\\\"\\n    - \\\"userType\\\"\\n    - \\\"language\\\"\\n  createSocialUserRequest:\\n    title: \\\"Service\\\"\\n    type: \\\"object\\\"\\n    properties:\\n      hostId:\\n        type: \\\"string\\\"\\n        description: \\\"user host indicator the organization for user\\\"\\n      email:\\n        type: \\\"string\\\"\\n        description: \\\"user email address\\\"\\n      userId:\\n        type: \\\"string\\\"\\n        description: \\\"a unique user id\\\"\\n      userType:\\n        title: \\\"User Type\\\"\\n        type: \\\"string\\\"\\n      language:\\n        type: \\\"string\\\"\\n        description: \\\"preferred language of user\\\"\\n      firstName:\\n        type: \\\"string\\\"\\n        description: \\\"firstName of the user\\\"\\n      lastName:\\n        type: \\\"string\\\"\\n        description: \\\"firstName of the user\\\"\\n    required:\\n    - \\\"hostId\\\"\\n    - \\\"email\\\"\\n    - \\\"userId\\\"\\n    - \\\"userType\\\"\\n    - \\\"language\\\"\\n  deleteUserByIdRequest:\\n    title: \\\"Service\\\"\\n    type: \\\"object\\\"\\n    properties:\\n      hostId:\\n        type: \\\"string\\\"\\n      userId:\\n        type: \\\"string\\\"\\n    required:\\n    - \\\"hostId\\\"\\n    - \\\"userId\\\"\\n  updateUserByIdRequest:\\n    title: \\\"Service\\\"\\n    type: \\\"object\\\"\\n    properties:\\n      hostId:\\n        type: \\\"string\\\"\\n        description: \\\"user host indicator the organization for user\\\"\\n      email:\\n        type: \\\"string\\\"\\n        description: \\\"email address of the user\\\"\\n      userId:\\n        type: \\\"string\\\"\\n        description: \\\"a unique user id\\\"\\n      entityId:\\n        type: \\\"string\\\"\\n        title: \\\"Entity Id\\\"\\n      userType:\\n        type: \\\"string\\\"\\n        title: \\\"User Type\\\"\\n      referralId:\\n        type: \\\"string\\\"\\n        title: \\\"Referral Id or Email\\\"\\n      managerId:\\n        type: \\\"string\\\"\\n        title: \\\"Manager Id or Email\\\"\\n      language:\\n        type: \\\"string\\\"\\n        description: \\\"preferred language of user\\\"\\n        enum:\\n        - \\\"en\\\"\\n      firstName:\\n        type: \\\"string\\\"\\n        description: \\\"firstName of the user\\\"\\n      lastName:\\n        type: \\\"string\\\"\\n        description: \\\"firstName of the user\\\"\\n      phoneNumber:\\n        type: \\\"string\\\"\\n        title: \\\"Phone Number\\\"\\n      gender:\\n        type: \\\"string\\\"\\n        description: \\\"user gender type\\\"\\n      birthday:\\n        type: \\\"string\\\"\\n        format: \\\"date\\\"\\n        description: \\\"birthday of the user\\\"\\n      country:\\n        type: \\\"string\\\"\\n        description: \\\"country name\\\"\\n      province:\\n        type: \\\"string\\\"\\n        description: \\\"province or state name\\\"\\n      city:\\n        type: \\\"string\\\"\\n        description: \\\"city name\\\"\\n      postCode:\\n        type: \\\"string\\\"\\n        description: \\\"postCode or zipCode\\\"\\n      address:\\n        type: \\\"string\\\"\\n        description: \\\"address\\\"\\n    required:\\n    - \\\"hostId\\\"\\n    - \\\"email\\\"\\n    - \\\"userId\\\"\\n    - \\\"entityId\\\"\\n    - \\\"userType\\\"\\n    - \\\"language\\\"\\n  changePasswordRequest:\\n    title: \\\"Service\\\"\\n    type: \\\"object\\\"\\n    properties:\\n      oldPassword:\\n        type: \\\"string\\\"\\n        description: \\\"the old password\\\"\\n      newPassword:\\n        type: \\\"string\\\"\\n        description: \\\"the new password\\\"\\n      passwordConfirm:\\n        type: \\\"string\\\"\\n        description: \\\"password confirm of user\\\"\\n    required:\\n    - \\\"oldPassword\\\"\\n    - \\\"newPassword\\\"\\n    - \\\"passwordConfirm\\\"\\n  forgetPasswordRequest:\\n    title: \\\"Service\\\"\\n    type: \\\"object\\\"\\n    properties:\\n      email:\\n        type: \\\"string\\\"\\n        description: \\\"email address of the user\\\"\\n    required:\\n    - \\\"email\\\"\\n  resetPasswordRequest:\\n    title: \\\"Service\\\"\\n    type: \\\"object\\\"\\n    properties:\\n      email:\\n        type: \\\"string\\\"\\n        description: \\\"email address of the user\\\"\\n      token:\\n        type: \\\"string\\\"\\n        description: \\\"password reset token received from email\\\"\\n      newPassword:\\n        type: \\\"string\\\"\\n        description: \\\"the new password\\\"\\n      passwordConfirm:\\n        type: \\\"string\\\"\\n        description: \\\"password confirm of user\\\"\\n    required:\\n    - \\\"email\\\"\\n    - \\\"token\\\"\\n    - \\\"newPassword\\\"\\n    - \\\"passwordConfirm\\\"\\n  confirmUserRequest:\\n    title: \\\"Service\\\"\\n    type: \\\"object\\\"\\n    properties:\\n      email:\\n        type: \\\"string\\\"\\n      token:\\n        type: \\\"string\\\"\\n    required:\\n    - \\\"email\\\"\\n    - \\\"token\\\"\\n  verifyUserRequest:\\n    title: \\\"Service\\\"\\n    type: \\\"object\\\"\\n    properties:\\n      hostId:\\n        type: \\\"string\\\"\\n        description: \\\"Host Id\\\"\\n      userId:\\n        type: \\\"string\\\"\\n        description: \\\"User Id\\\"\\n    required:\\n    - \\\"hostId\\\"\\n    - \\\"userId\\\"\\n  lockUserRequest:\\n    title: \\\"Service\\\"\\n    type: \\\"object\\\"\\n    properties:\\n      email:\\n        type: \\\"string\\\"\\n      reason:\\n        type: \\\"string\\\"\\n    required:\\n    - \\\"email\\\"\\n    - \\\"reason\\\"\\n  unlockUserRequest:\\n    title: \\\"Service\\\"\\n    type: \\\"object\\\"\\n    properties:\\n      email:\\n        type: \\\"string\\\"\\n      reason:\\n        type: \\\"string\\\"\\n    required:\\n    - \\\"email\\\"\\n    - \\\"reason\\\"\\n  sendMessageRequest:\\n    title: \\\"Service\\\"\\n    type: \\\"object\\\"\\n    properties:\\n      userId:\\n        type: \\\"string\\\"\\n      subject:\\n        type: \\\"string\\\"\\n      content:\\n        type: \\\"string\\\"\\n    required:\\n    - \\\"userId\\\"\\n    - \\\"subject\\\"\\n    - \\\"content\\\"\\n  updatePaymentRequest:\\n    title: \\\"Service\\\"\\n    type: \\\"object\\\"\\n    properties:\\n      email:\\n        type: \\\"string\\\"\\n        description: \\\"Merchant user email address\\\"\\n      payments:\\n        type: \\\"array\\\"\\n        description: \\\"A list of payments\\\"\\n    required:\\n    - \\\"payments\\\"\\n  deletePaymentRequest:\\n    title: \\\"Service\\\"\\n    type: \\\"object\\\"\\n    properties:\\n      email:\\n        type: \\\"string\\\"\\n        description: \\\"Merchant user email address\\\"\\n    required:\\n    - \\\"email\\\"\\n  paymentNonceRequest:\\n    title: \\\"Service\\\"\\n    type: \\\"object\\\"\\n    properties:\\n      userId:\\n        type: \\\"string\\\"\\n        description: \\\"merchant userId\\\"\\n      nonce:\\n        type: \\\"string\\\"\\n        description: \\\"Payment method nonce\\\"\\n    required:\\n    - \\\"userId\\\"\\n    - \\\"nonce\\\"\\n  createOrderRequest:\\n    title: \\\"Service\\\"\\n    type: \\\"object\\\"\\n    properties:\\n      userId:\\n        type: \\\"string\\\"\\n        description: \\\"merchant userId\\\"\\n      order:\\n        type: \\\"object\\\"\\n        description: \\\"Order object\\\"\\n    required:\\n    - \\\"userId\\\"\\n    - \\\"order\\\"\\n  cancelOrderRequest:\\n    title: \\\"Service\\\"\\n    type: \\\"object\\\"\\n    properties:\\n      email:\\n        type: \\\"string\\\"\\n        description: \\\"Customer email\\\"\\n      merchantUserId:\\n        type: \\\"string\\\"\\n        description: \\\"Merchant User Id\\\"\\n      orderId:\\n        type: \\\"object\\\"\\n        description: \\\"Order Id\\\"\\n    required:\\n    - \\\"email\\\"\\n    - \\\"orderId\\\"\\n  deliverOrderRequest:\\n    title: \\\"Service\\\"\\n    type: \\\"object\\\"\\n    properties:\\n      email:\\n        type: \\\"string\\\"\\n        description: \\\"Merchant email\\\"\\n      customerUserId:\\n        type: \\\"string\\\"\\n        description: \\\"Customer User Id\\\"\\n      orderId:\\n        type: \\\"string\\\"\\n        description: \\\"Order Id\\\"\\n    required:\\n    - \\\"email\\\"\\n    - \\\"customerUserId\\\"\\n    - \\\"orderId\\\"\\n  exportPortalEventRequest:\\n    title: \\\"Export\\\"\\n    type: \\\"object\\\"\\n    properties:\\n      hostId:\\n        type: \\\"string\\\"\\n        description: \\\"Host Id\\\"\\n      portalServices:\\n        type: \\\"string\\\"\\n        description: \\\"Portal services\\\"\\n      aggregateTypes:\\n        type: \\\"string\\\"\\n        description: \\\"Aggregate Types\\\"\\n      eventTypes:\\n        type: \\\"string\\\"\\n        description: \\\"Event Types\\\"\\n      startTs:\\n        type: \\\"string\\\"\\n        description: \\\"Start timestamp\\\"\\n      endTs:\\n        type: \\\"string\\\"\\n        description: \\\"End timestamp\\\"\\n    required:\\n    - \\\"hostId\\\"\\n    - \\\"startTs\\\"\\n  importPortalEventRequest:\\n    title: \\\"Import\\\"\\n    type: \\\"object\\\"\\n    properties:\\n      hostId:\\n        type: \\\"string\\\"\\n        description: \\\"Host Id\\\"\\n      portalServices:\\n        type: \\\"string\\\"\\n        description: \\\"Portal services\\\"\\n      aggregateTypes:\\n        type: \\\"string\\\"\\n        description: \\\"Aggregate Types\\\"\\n      eventTypes:\\n        type: \\\"string\\\"\\n        description: \\\"Event Types\\\"\\n      startTs:\\n        type: \\\"string\\\"\\n        description: \\\"Start timestamp\\\"\\n      endTs:\\n        type: \\\"string\\\"\\n        description: \\\"End timestamp\\\"\\n      replacements:\\n        type: array\\n        title: Replacements\\n        maxItems: 20\\n        items:\\n          type: object\\n          properties:\\n            fieldName:\\n              title: Field Name\\n              type: string\\n            fromValue:\\n              title: From Value\\n              type: string\\n            toValue:\\n              title: To Value\\n              type: string\\n          required:\\n            - fieldName\\n            - fromValue\\n            - toValue\\n      enrichments:\\n        type: array\\n        title: Enrichments\\n        maxItems: 20\\n        items:\\n          type: object\\n          properties:\\n            fieldName:\\n              title: Field Name\\n              type: string\\n            action:\\n              title: Action\\n              type: string\\n            sourceField:\\n              title: Source Field\\n              type: string\\n          required:\\n            - fieldName\\n            - action\\n      eventText:\\n        title: \\\"Event Text\\\"\\n        description: \\\"Event text to be imported\\\"\\n        type: \\\"string\\\"\\n        minLength: 1\\n      eventUrl:\\n        title: \\\"Event URL\\\"\\n        description: \\\"Event URL to be imported\\\"\\n        type: \\\"string\\\"\\n        minLength: 1\\n    required:\\n    - \\\"hostId\\\"\\n    oneOf:\\n    - required:\\n      - \\\"eventText\\\"\\n    - required:\\n      - \\\"eventUrl\\\"\\naction:\\n- name: \\\"createUser\\\"\\n  version: \\\"0.1.0\\\"\\n  handler: \\\"CreateUser\\\"\\n  skipAuth: true\\n  request:\\n    schema:\\n      $ref: \\\"#/schemas/createUserRequest\\\"\\n    example:\\n      hostId: \\\"host123\\\"\\n      email: \\\"test@example.com\\\"\\n      entityId: \\\"cust123\\\"\\n      userType: \\\"C\\\"\\n      language: \\\"en\\\"\\n      password: \\\"password\\\"\\n      passwordConfirm: \\\"password\\\"\\n      firstName: \\\"John\\\"\\n      lastName: \\\"Doe\\\"\\n      phoneNumber: \\\"123-456-7890\\\"\\n      gender: \\\"M\\\"\\n      birthday: \\\"1980-01-01\\\"\\n      country: \\\"US\\\"\\n      province: \\\"CA\\\"\\n      city: \\\"Los Angeles\\\"\\n      postCode: \\\"12345\\\"\\n      address: \\\"123 Main St\\\"\\n- name: \\\"onboardUser\\\"\\n  version: \\\"0.1.0\\\"\\n  handler: \\\"OnboardUser\\\"\\n  scope: \\\"portal.w\\\"\\n  request:\\n    schema:\\n      $ref: \\\"#/schemas/onboardUserRequest\\\"\\n    example:\\n      hostId: \\\"host123\\\"\\n      email: \\\"test@example.com\\\"\\n      entityId: \\\"cust123\\\"\\n      userType: \\\"C\\\"\\n      language: \\\"en\\\"\\n      firstName: \\\"John\\\"\\n      lastName: \\\"Doe\\\"\\n      phoneNumber: \\\"123-456-7890\\\"\\n      gender: \\\"M\\\"\\n      birthday: \\\"1980-01-01\\\"\\n      country: \\\"US\\\"\\n      province: \\\"CA\\\"\\n      city: \\\"Los Angeles\\\"\\n      postCode: \\\"12345\\\"\\n      address: \\\"123 Main St\\\"\\n- name: \\\"createSocialUser\\\"\\n  version: \\\"0.1.0\\\"\\n  handler: \\\"CreateSocialUser\\\"\\n  scope: \\\"portal.w\\\"\\n  request:\\n    schema:\\n      $ref: \\\"#/schemas/createSocialUserRequest\\\"\\n    example:\\n      hostId: \\\"host123\\\"\\n      email: \\\"test@example.com\\\"\\n      userId: \\\"socialuser123\\\"\\n      userType: \\\"C\\\"\\n      language: \\\"en\\\"\\n      firstName: \\\"Jane\\\"\\n      lastName: \\\"Doe\\\"\\n- name: \\\"deleteUserById\\\"\\n  version: \\\"0.1.0\\\"\\n  handler: \\\"DeleteUserById\\\"\\n  scope: \\\"portal.w\\\"\\n  request:\\n    schema:\\n      $ref: \\\"#/schemas/deleteUserByIdRequest\\\"\\n    example:\\n      hostId: \\\"host123\\\"\\n      userId: \\\"user123\\\"\\n- name: \\\"updateUserById\\\"\\n  version: \\\"0.1.0\\\"\\n  handler: \\\"UpdateUserById\\\"\\n  scope: \\\"portal.w\\\"\\n  request:\\n    schema:\\n      $ref: \\\"#/schemas/updateUserByIdRequest\\\"\\n    example:\\n      hostId: \\\"host123\\\"\\n      email: \\\"test@example.com\\\"\\n      userId: \\\"user123\\\"\\n      entityId: \\\"cust123\\\"\\n      userType: \\\"E\\\"\\n      language: \\\"en\\\"\\n      firstName: \\\"Updated First\\\"\\n      lastName: \\\"Updated Last\\\"\\n      phoneNumber: \\\"555-1234\\\"\\n      gender: \\\"F\\\"\\n      birthday: \\\"1990-02-02\\\"\\n      country: \\\"CA\\\"\\n      province: \\\"ON\\\"\\n      city: \\\"Toronto\\\"\\n      postCode: \\\"54321\\\"\\n      address: \\\"345 Fake St\\\"\\n- name: \\\"changePassword\\\"\\n  version: \\\"0.1.0\\\"\\n  handler: \\\"ChangePassword\\\"\\n  scope: \\\"portal.w\\\"\\n  request:\\n    schema:\\n      $ref: \\\"#/schemas/changePasswordRequest\\\"\\n    example:\\n      oldPassword: \\\"oldpassword\\\"\\n      newPassword: \\\"newpassword\\\"\\n      passwordConfirm: \\\"newpassword\\\"\\n- name: \\\"forgetPassword\\\"\\n  version: \\\"0.1.0\\\"\\n  handler: \\\"ForgetPassword\\\"\\n  skipAuth: true\\n  request:\\n    schema:\\n      $ref: \\\"#/schemas/forgetPasswordRequest\\\"\\n    example:\\n      email: \\\"test@example.com\\\"\\n- name: \\\"resetPassword\\\"\\n  version: \\\"0.1.0\\\"\\n  handler: \\\"ResetPassword\\\"\\n  skipAuth: true\\n  request:\\n    schema:\\n      $ref: \\\"#/schemas/resetPasswordRequest\\\"\\n    example:\\n      email: \\\"test@example.com\\\"\\n      token: \\\"reset_token\\\"\\n      newPassword: \\\"newpassword\\\"\\n      passwordConfirm: \\\"newpassword\\\"\\n- name: \\\"confirmUser\\\"\\n  version: \\\"0.1.0\\\"\\n  handler: \\\"ConfirmUser\\\"\\n  skipAuth: true\\n  request:\\n    schema:\\n      $ref: \\\"#/schemas/confirmUserRequest\\\"\\n    example:\\n      email: \\\"test@example.com\\\"\\n      token: \\\"confirm_token\\\"\\n- name: \\\"verifyUser\\\"\\n  version: \\\"0.1.0\\\"\\n  handler: \\\"VerifyUser\\\"\\n  scope: \\\"portal.w\\\"\\n  request:\\n    schema:\\n      $ref: \\\"#/schemas/verifyUserRequest\\\"\\n    example:\\n      hostId: \\\"host123\\\"\\n      userId: \\\"user123\\\"\\n- name: \\\"lockUser\\\"\\n  version: \\\"0.1.0\\\"\\n  handler: \\\"LockUser\\\"\\n  scope: \\\"portal.w\\\"\\n  request:\\n    schema:\\n      $ref: \\\"#/schemas/lockUserRequest\\\"\\n    example:\\n      email: \\\"test@example.com\\\"\\n      reason: \\\"suspicious activity\\\"\\n- name: \\\"unlockUser\\\"\\n  version: \\\"0.1.0\\\"\\n  handler: \\\"UnlockUser\\\"\\n  scope: \\\"portal.w\\\"\\n  request:\\n    schema:\\n      $ref: \\\"#/schemas/unlockUserRequest\\\"\\n    example:\\n      email: \\\"test@example.com\\\"\\n      reason: \\\"resolved\\\"\\n- name: \\\"sendMessage\\\"\\n  version: \\\"0.1.0\\\"\\n  handler: \\\"SendMessage\\\"\\n  scope: \\\"portal.w\\\"\\n  request:\\n    schema:\\n      $ref: \\\"#/schemas/sendMessageRequest\\\"\\n    example:\\n      userId: \\\"user123\\\"\\n      subject: \\\"Welcome Message\\\"\\n      content: \\\"Hi there, welcome to our platform\\\"\\n- name: \\\"updatePayment\\\"\\n  version: \\\"0.1.0\\\"\\n  handler: \\\"UpdatePayment\\\"\\n  scope: \\\"portal.w\\\"\\n  request:\\n    schema:\\n      $ref: \\\"#/schemas/updatePaymentRequest\\\"\\n    example:\\n      email: \\\"test@example.com\\\"\\n      payments:\\n      - card_number: \\\"1234-5678-9012-3456\\\"\\n        expiry_month: \\\"12\\\"\\n        expiry_year: \\\"2025\\\"\\n        cvv: \\\"123\\\"\\n- name: \\\"deletePayment\\\"\\n  version: \\\"0.1.0\\\"\\n  handler: \\\"DeletePayment\\\"\\n  scope: \\\"portal.w\\\"\\n  request:\\n    schema:\\n      $ref: \\\"#/schemas/deletePaymentRequest\\\"\\n    example:\\n      email: \\\"test@example.com\\\"\\n- name: \\\"paymentNonce\\\"\\n  version: \\\"0.1.0\\\"\\n  handler: \\\"PaymentNonce\\\"\\n  scope: \\\"portal.w\\\"\\n  request:\\n    schema:\\n      $ref: \\\"#/schemas/paymentNonceRequest\\\"\\n    example:\\n      userId: \\\"user123\\\"\\n      nonce: \\\"payment_method_nonce\\\"\\n- name: \\\"createOrder\\\"\\n  version: \\\"0.1.0\\\"\\n  handler: \\\"CreateOrder\\\"\\n  scope: \\\"portal.w\\\"\\n  request:\\n    schema:\\n      $ref: \\\"#/schemas/createOrderRequest\\\"\\n    example:\\n      userId: \\\"user123\\\"\\n      order:\\n        items:\\n        - id: \\\"item123\\\"\\n          qty: 2\\n        total: 100\\n- name: \\\"cancelOrder\\\"\\n  version: \\\"0.1.0\\\"\\n  handler: \\\"CancelOrder\\\"\\n  scope: \\\"portal.w\\\"\\n  request:\\n    schema:\\n      $ref: \\\"#/schemas/cancelOrderRequest\\\"\\n    example:\\n      email: \\\"test@example.com\\\"\\n      merchantUserId: \\\"merchant123\\\"\\n      orderId: \\\"order123\\\"\\n- name: \\\"deliverOrder\\\"\\n  version: \\\"0.1.0\\\"\\n  handler: \\\"DeliverOrder\\\"\\n  scope: \\\"portal.w\\\"\\n  request:\\n    schema:\\n      $ref: \\\"#/schemas/deliverOrderRequest\\\"\\n    example:\\n      email: \\\"test@example.com\\\"\\n      customerUserId: \\\"cust123\\\"\\n      orderId: \\\"order123\\\"\\n- name: \\\"exportPortalEvent\\\"\\n  version: \\\"0.1.0\\\"\\n  handler: \\\"ExportPortalEvent\\\"\\n  scope: \\\"portal.r\\\"\\n  request:\\n    schema:\\n      $ref: \\\"#/schemas/exportPortalEventRequest\\\"\\n    example:\\n      hostId: \\\"host123\\\"\\n      portalServices: \\\"service1,service2\\\"\\n- name: \\\"importPortalEvent\\\"\\n  version: \\\"0.1.0\\\"\\n  handler: \\\"ImportPortalEvent\\\"\\n  scope: \\\"portal.w\\\"\\n  request:\\n    schema:\\n      $ref: \\\"#/schemas/importPortalEventRequest\\\"\\n    example:\\n      hostId: \\\"host123\\\"\\n      portalServices: \\\"service1,service2\\\"\\n\",\"apiId\":\"1113\",\"hostId\":\"01964b05-552a-7c4b-9184-6857e7f3dc5f\",\"apiType\":\"hybrid\",\"endpoints\":[{\"endpoint\":\"lightapi.net/user/createUser/0.1.0\",\"endpointId\":\"019bb433-8217-79b9-9f63-b183be23767f\",\"endpointName\":\"CreateUser\"},{\"scopes\":[\"portal.w\"],\"endpoint\":\"lightapi.net/user/onboardUser/0.1.0\",\"endpointId\":\"019bb433-8217-7a1e-9f64-47b6efa8f619\",\"endpointName\":\"OnboardUser\"},{\"scopes\":[\"portal.w\"],\"endpoint\":\"lightapi.net/user/createSocialUser/0.1.0\",\"endpointId\":\"019bb433-8217-7a3b-9f65-78e8e490ef8a\",\"endpointName\":\"CreateSocialUser\"},{\"scopes\":[\"portal.w\"],\"endpoint\":\"lightapi.net/user/deleteUserById/0.1.0\",\"endpointId\":\"019bb433-8217-7a5f-9f66-4b86012ed333\",\"endpointName\":\"DeleteUserById\"},{\"scopes\":[\"portal.w\"],\"endpoint\":\"lightapi.net/user/updateUserById/0.1.0\",\"endpointId\":\"019bb433-8217-7a6c-9f67-093ba19e64f3\",\"endpointName\":\"UpdateUserById\"},{\"scopes\":[\"portal.w\"],\"endpoint\":\"lightapi.net/user/changePassword/0.1.0\",\"endpointId\":\"019bb433-8217-7a75-9f68-b56ff684ccc4\",\"endpointName\":\"ChangePassword\"},{\"endpoint\":\"lightapi.net/user/forgetPassword/0.1.0\",\"endpointId\":\"019bb433-8217-7a7d-9f69-74a7ba9731a1\",\"endpointName\":\"ForgetPassword\"},{\"endpoint\":\"lightapi.net/user/resetPassword/0.1.0\",\"endpointId\":\"019bb433-8217-7a86-9f6a-09b276b140fe\",\"endpointName\":\"ResetPassword\"},{\"endpoint\":\"lightapi.net/user/confirmUser/0.1.0\",\"endpointId\":\"019bb433-8217-7a8e-9f6b-f470081ac82f\",\"endpointName\":\"ConfirmUser\"},{\"scopes\":[\"portal.w\"],\"endpoint\":\"lightapi.net/user/verifyUser/0.1.0\",\"endpointId\":\"019bb433-8217-7a9a-9f6c-6178e7188d96\",\"endpointName\":\"VerifyUser\"},{\"scopes\":[\"portal.w\"],\"endpoint\":\"lightapi.net/user/lockUser/0.1.0\",\"endpointId\":\"019bb433-8217-7aa4-9f6d-66e6fc845360\",\"endpointName\":\"LockUser\"},{\"scopes\":[\"portal.w\"],\"endpoint\":\"lightapi.net/user/unlockUser/0.1.0\",\"endpointId\":\"019bb433-8217-7aad-9f6e-409b5f3a4ece\",\"endpointName\":\"UnlockUser\"},{\"scopes\":[\"portal.w\"],\"endpoint\":\"lightapi.net/user/sendMessage/0.1.0\",\"endpointId\":\"019bb433-8217-7ab5-9f6f-2adc6cc95fb5\",\"endpointName\":\"SendMessage\"},{\"scopes\":[\"portal.w\"],\"endpoint\":\"lightapi.net/user/updatePayment/0.1.0\",\"endpointId\":\"019bb433-8217-7abf-9f70-a700b75207ca\",\"endpointName\":\"UpdatePayment\"},{\"scopes\":[\"portal.w\"],\"endpoint\":\"lightapi.net/user/deletePayment/0.1.0\",\"endpointId\":\"019bb433-8217-7ac7-9f71-496206ea701c\",\"endpointName\":\"DeletePayment\"},{\"scopes\":[\"portal.w\"],\"endpoint\":\"lightapi.net/user/paymentNonce/0.1.0\",\"endpointId\":\"019bb433-8217-7ad1-9f72-bfc7051ce962\",\"endpointName\":\"PaymentNonce\"},{\"scopes\":[\"portal.w\"],\"endpoint\":\"lightapi.net/user/createOrder/0.1.0\",\"endpointId\":\"019bb433-8217-7ada-9f73-4388fadaf041\",\"endpointName\":\"CreateOrder\"},{\"scopes\":[\"portal.w\"],\"endpoint\":\"lightapi.net/user/cancelOrder/0.1.0\",\"endpointId\":\"019bb433-8217-7ae3-9f74-09a710dbb73f\",\"endpointName\":\"CancelOrder\"},{\"scopes\":[\"portal.w\"],\"endpoint\":\"lightapi.net/user/deliverOrder/0.1.0\",\"endpointId\":\"019bb433-8217-7aec-9f75-4834b671da58\",\"endpointName\":\"DeliverOrder\"},{\"scopes\":[\"portal.r\"],\"endpoint\":\"lightapi.net/user/exportPortalEvent/0.1.0\",\"endpointId\":\"019bb433-8217-7af4-9f76-9b9c5618463e\",\"endpointName\":\"ExportPortalEvent\"},{\"scopes\":[\"portal.w\"],\"endpoint\":\"lightapi.net/user/importPortalEvent/0.1.0\",\"endpointId\":\"019bb433-8217-7b00-9f77-7d438d45a3c0\",\"endpointName\":\"ImportPortalEvent\"}],\"serviceId\":\"user-command.lightapi.net\",\"apiVersion\":\"1.0.0\",\"apiVersionId\":\"019bb433-8213-710f-a4eb-53aede5d63c0\",\"apiVersionDesc\":\"user-command hybrid service 1.0.0\",\"aggregateVersion\":0,\"newAggregateVersion\":1},\"aggregateversion\":1,\"subject\":\"019bb433-8213-710f-a4eb-53aede5d63c0\",\"source\":\"https://github.com/lightapi/light-portal\",\"type\":\"ApiVersionCreatedEvent\",\"nonce\":\"2110\",\"host\":\"01964b05-552a-7c4b-9184-6857e7f3dc5f\",\"specversion\":\"1.0\",\"id\":\"019bb433-821e-71ab-8c1f-a18229cf165f\",\"time\":\"2026-01-12T21:53:53.182132065Z\",\"user\":\"01964b05-5532-7c79-8cde-191dcbd421b8\",\"aggregatetype\":\"ApiVersion\"}";
        Map<String, Object> map = JsonMapper.string2Map(s);
        Connection conn = SqlDbStartupHook.ds.getConnection();
        dbProvider.createApiVersion(conn, map);
    }

    @Test
    void testGetProviderClient() {
        Result<String> result = dbProvider.queryAuthProviderClient(0, 10,
                "[{\"id\":\"providerId\",\"value\":\"AZZRJE52eXu3t1hseacnGQ\"},{\"id\":\"active\",\"value\":true}]", null, null,
                true, "01964b05-552a-7c4b-9184-6857e7f3dc5f");
        if(result.isFailure()) {
            System.out.println(result.getError());
        } else {
            System.out.println(result.getResult());
        }
    }

    @Test
    void testQueryUserByHostId() {
        Result<String> result = dbProvider.queryUserByHostId(0, 10,
                "[{\"id\":\"hostId\",\"value\":\"01964b05-552a-7c4b-9184-6857e7f3dc5f\"}]", null, null,
                true);
        if(result.isFailure()) {
            System.out.println(result.getError());
        } else {
            System.out.println(result.getResult());
        }
    }

    @Test
    void testGetDeploymentInstancePipeline() {
        Result<String> result = dbProvider.getDeploymentInstancePipeline("01964b05-552a-7c4b-9184-6857e7f3dc5f",  "019aa354-dfdb-738e-a02a-e75d8f9b81c3", "VM Ubuntu 24.04", "OpenJDK 21");
        if(result.isFailure()) {
            System.out.println(result.getError());
        } else {
            System.out.println(result.getResult());
        }

    }

    @Test
    void testPollTask() {
        Result<List<Map<String, Object>>> result = dbProvider.pollTasks(OffsetDateTime.now());
        if(result.isFailure()) {
            System.out.println(result.getError());
        } else {
            System.out.println(result.getResult());
        }
    }

    @Test
    @Disabled
    void testCreateScheduleWithNextRunTs() throws Exception {
        String scheduleId = UuidUtil.getUUID().toString();
        String hostId = "01964b05-552a-7c4b-9184-6857e7f3dc5f";
        String userId = "stevehu";
        OffsetDateTime startTs = OffsetDateTime.now(ZoneOffset.UTC);
        Map<String, Object> data = new HashMap<>();
        data.put("scheduleId", scheduleId);
        data.put("hostId", hostId);
        data.put("scheduleName", "Test Schedule");
        data.put("frequencyUnit", "DAYS");
        data.put("frequencyTime", 1);
        data.put("startTs", startTs.toString());
        data.put("eventTopic", "test-topic");
        data.put("eventType", "TestCreatedEvent");
        data.put("eventData", "{\"test\":\"data\"}");
        data.put("newAggregateVersion", 1L); // Added for OCC/IDM

        Map<String, Object> event = createEvent(hostId, userId, scheduleId, "Schedule", 1, data);

        try (Connection conn = sqlDbStartupHook.ds.getConnection()) {
            dbProvider.createSchedule(conn, event);
        }

        Result<String> result = dbProvider.getScheduleById(scheduleId);
        assertTrue(result.isSuccess());
        Map<String, Object> schedule = JsonMapper.string2Map(result.getResult());
        assertNotNull(schedule.get("next_run_ts"));

        // OffsetDateTime might lose some precision in DB, so compare as strings or within a small delta
        OffsetDateTime dbNextRunTs = OffsetDateTime.parse((String) schedule.get("next_run_ts"));
        assertEquals(startTs.toInstant().getEpochSecond(), dbNextRunTs.toInstant().getEpochSecond());
    }

    @Test
    void testDbPubSub() throws Exception {
        String aggregateId = UuidUtil.getUUID().toString();
        String hostId = "01964b05-552a-7c4b-9184-6857e7f3dc5f";
        String userId = UuidUtil.getUUID().toString(); // Use unique userId to avoid nonce clash

        // 1. Insert multiple events
        Map<String, Object> data1 = new HashMap<>();
        data1.put("key", "value1");
        CloudEvent event1 = CloudEventBuilder.v1()
                .withId(UUID.randomUUID().toString())
                .withSource(URI.create("test-source"))
                .withType("TestCreatedEvent")
                .withTime(OffsetDateTime.now())
                .withSubject(aggregateId)
                .withExtension(Constants.HOST, hostId)
                .withExtension(Constants.USER, userId)
                .withExtension(PortalConstants.NONCE, 1L)
                .withExtension(PortalConstants.AGGREGATE_TYPE, "Test")
                .withExtension(PortalConstants.EVENT_AGGREGATE_VERSION, 1L)
                .withData("application/json", objectMapper.writeValueAsBytes(data1))
                .build();

        Map<String, Object> data2 = new HashMap<>();
        data2.put("key", "value2");
        CloudEvent event2 = CloudEventBuilder.v1()
                .withId(UUID.randomUUID().toString())
                .withSource(URI.create("test-source"))
                .withType("TestUpdatedEvent")
                .withTime(OffsetDateTime.now())
                .withSubject(aggregateId)
                .withExtension(Constants.HOST, hostId)
                .withExtension(Constants.USER, userId)
                .withExtension(PortalConstants.NONCE, 2L)
                .withExtension(PortalConstants.AGGREGATE_TYPE, "Test")
                .withExtension(PortalConstants.EVENT_AGGREGATE_VERSION, 2L)
                .withData("application/json", objectMapper.writeValueAsBytes(data2))
                .build();

        dbProvider.insertEventStore(new CloudEvent[]{event1, event2});

        // 2. Verify offsets are populated in outbox_message_t
        try (Connection conn = ds.getConnection();
             PreparedStatement pstmt = conn.prepareStatement("SELECT c_offset FROM outbox_message_t WHERE aggregate_id = ? ORDER BY c_offset")) {
            pstmt.setString(1, aggregateId);
            try (ResultSet rs = pstmt.executeQuery()) {
                assertTrue(rs.next());
                long offset1 = rs.getLong(1);
                assertTrue(offset1 >= 1);
                assertTrue(rs.next());
                long offset2 = rs.getLong(1);
                assertEquals(offset1 + 1, offset2);
            }
        }

        // 3. Claim offsets via consumer_offsets (Competing Consumer Pattern)
        String groupId = "test-group-" + aggregateId;
        try (Connection conn = ds.getConnection();
             PreparedStatement pstmt = conn.prepareStatement("INSERT INTO consumer_offsets (group_id, topic_id, partition_id, next_offset) VALUES (?, 1, 0, 1)")) {
            pstmt.setString(1, groupId);
            pstmt.executeUpdate();
        }

        String claimSql =
            "WITH counter_tip AS ( SELECT (next_offset - 1) AS highest_committed_offset FROM log_counter WHERE id = 1 ), " +
            "to_claim AS ( SELECT c.group_id, c.next_offset AS n0, LEAST(?::bigint, GREATEST(0, (SELECT highest_committed_offset FROM counter_tip) - c.next_offset + 1)) AS delta " +
            "FROM consumer_offsets c WHERE c.group_id = ? AND c.topic_id = 1 AND c.partition_id = 0 FOR UPDATE ), " +
            "upd AS ( UPDATE consumer_offsets c SET next_offset = c.next_offset + t.delta FROM to_claim t WHERE c.group_id = t.group_id AND c.topic_id = 1 AND c.partition_id = 0 " +
            "RETURNING t.n0 AS n0, (c.next_offset - 1) AS n1 ) SELECT n0, n1 FROM upd";

        try (Connection conn = ds.getConnection();
             PreparedStatement pstmt = conn.prepareStatement(claimSql)) {
            pstmt.setInt(1, 10);
            pstmt.setString(2, groupId);
            try (ResultSet rs = pstmt.executeQuery()) {
                assertTrue(rs.next());
                long claimedStart = rs.getLong(1);
                long claimedEnd = rs.getLong(2);
                assertTrue(claimedEnd >= claimedStart);
            }
        }
    }
}
