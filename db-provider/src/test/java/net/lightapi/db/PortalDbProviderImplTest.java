package net.lightapi.db;

import com.networknt.config.JsonMapper;
import com.networknt.db.provider.DbProvider;
import com.networknt.db.provider.SqlDbStartupHook;
import com.networknt.monad.Result;
import com.networknt.service.SingletonServiceFactory;
import net.lightapi.portal.db.PortalDbProvider;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;
import java.util.UUID;

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
        Result<String> result = dbProvider.getRefTable(0, 100, null, null, null, null);
        if(result.isFailure()) {
            System.out.println(result.getError());
        } else {
            System.out.println(result.getResult());
        }
    }

    @Test
    void testGetRefTableWithFilters() {
        Result<String> result = dbProvider.getRefTable(0, 100,"[{\"id\":\"tableName\",\"value\":\"env\"}]", null, null, "01964b05-552a-7c4b-9184-6857e7f3dc5f");
        if(result.isFailure()) {
            System.out.println(result.getError());
        } else {
            System.out.println(result.getResult());
        }
    }

    @Test
    void testGetRefTableWithGlobalFilter() {
        Result<String> result = dbProvider.getRefTable(0, 100,null, "env", null, "01964b05-552a-7c4b-9184-6857e7f3dc5f");
        if(result.isFailure()) {
            System.out.println(result.getError());
        } else {
            System.out.println(result.getResult());
        }
    }

    @Test
    void testGetRefTableWithSorting() {
        Result<String> result = dbProvider.getRefTable(0, 100,null, null, "[{\"id\":\"tableId\",\"desc\":false}]", "01964b05-552a-7c4b-9184-6857e7f3dc5f");
        if(result.isFailure()) {
            System.out.println(result.getError());
        } else {
            System.out.println(result.getResult());
        }
    }

    @Test
    void testGetRefValueWithFilters() {
        Result<String> result = dbProvider.getRefValue(0, 100,"[{\"id\":\"tableId\",\"value\":\"01964b05-552e-705a-a193-7a859347a9d5\"}]", null, null);
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
        Result<String> result = dbProvider.queryService(0, 2, "01964b05-552a-7c4b-9184-6857e7f3dc5f", "1234",
                null, null, null, null, null, null, null,
                null, null, null, null, null);
        if(result.isFailure()) {
            System.out.println(result.getError());
        } else {
            System.out.println(result.getResult());
        }
    }

    @Test
    void testQueryRule() {
        Result<String> result = dbProvider.queryRule(0, 2, "01964b05-552a-7c4b-9184-6857e7f3dc5f", null,
                null, null, null, null, null, null, null);
        if(result.isFailure()) {
            System.out.println(result.getError());
        } else {
            System.out.println(result.getResult());
        }
    }

    @Test
    void testGetNextNonce() {
        Result<Long> result = dbProvider.queryNonceByUserId("01964b05-5532-7c79-8cde-191dcbd421b8");
        if(result.isFailure()) {
            System.out.println(result.getError());
        } else {
            System.out.println(result.getResult());
            System.out.println(result.getResult().getClass());
        }
    }

    @Test
    public void testQueryRole()  {
        Result<String> result = dbProvider.queryRole(0, 4, "01964b05-552a-7c4b-9184-6857e7f3dc5f", null, null);
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
    public void testQueryGroup() {
        Result<String> result = dbProvider.queryGroup(0, 2, "01964b05-552a-7c4b-9184-6857e7f3dc5f", "le", null);
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
    public void testQueryPosition() {
        Result<String> result = dbProvider.queryPosition(0, 2, "01964b05-552a-7c4b-9184-6857e7f3dc5f", null, null, "Y", null);
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
    public void testQueryAttribute() {
        Result<String> result = dbProvider.queryAttribute(0, 2, "01964b05-552a-7c4b-9184-6857e7f3dc5f", "ou", null, null);
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
    public void testQueryServicePermission() {
        Result<String> result = dbProvider.queryServicePermission("01964b05-552a-7c4b-9184-6857e7f3dc5f", "0100", "1.0.0");
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
    public void testQueryServiceFilter() {
        Result<List<String>> result = dbProvider.queryServiceFilter("01964b05-552a-7c4b-9184-6857e7f3dc5f", "0100", "1.0.0");
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
        Result<String> result = dbProvider.queryUserByHostId(0, 2, "[{\"id\":\"hostId\",\"value\":\"01964b05-552a-7c4b-9184-6857e7f3dc5f\"}]", null, null);
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
        Result<String> result = dbProvider.getProduct(0, 2, "01964b05-552a-7c4b-9184-6857e7f3dc5f", null, null, null, null, null, null, null, null, null, null, null);
        if(result.isFailure()) {
            System.out.println(result.getError());
        } else {
            System.out.println(result.getResult());
        }
    }

    @Test
    void testGetCategory() {
        Result<String> result = dbProvider.getCategory(0, 2, "01964b05-552a-7c4b-9184-6857e7f3dc5f", null, null, null, null, null, null, null);
        if(result.isFailure()) {
            System.out.println(result.getError());
        } else {
            System.out.println(result.getResult());
        }
    }

    @Test
    void testGetCategoryWithoutHostId() {
        Result<String> result = dbProvider.getCategory(0, 2, null, null, null, null, null, null, null, null);
        if(result.isFailure()) {
            System.out.println(result.getError());
        } else {
            System.out.println(result.getResult());
        }
    }

    @Test
    void testQueryClientByClientId() {
        Result<String> result =  dbProvider.queryClientByClientId("f7d42348-c647-4efb-a52d-4c5787421e70");
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
        Result<String> result = dbProvider.getTag(0, 2, "01964b05-552a-7c4b-9184-6857e7f3dc5f", null, null, null, null);
        if(result.isFailure()) {
            System.out.println(result.getError());
        } else {
            System.out.println(result.getResult());
        }
    }

    @Test
    void testCommitConfigInstance() throws Exception {
        Map<String, Object> map = Map.of("hostId", "01964b05-552a-7c4b-9184-6857e7f3dc5f",
                "instanceId", "0196dbd4-3820-7be4-894a-078f311d0b49",
                "snapshotType", "USER_SAVE",
                "description", "Test with user save type",
                "userId", "01964b05-5532-7c79-8cde-191dcbd421b8");

        Connection conn = SqlDbStartupHook.ds.getConnection();
        dbProvider.commitConfigInstance(conn, map);
    }

    @Test
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
    void testUpdateServiceSpec() throws Exception {
        // To make it work, you need to query the database and update the aggregateVersion in the following JSON string.
        String s = "{\"datacontenttype\":\"application/json\",\"data\":{\"spec\":\"---\\nopenapi: \\\"3.1.0\\\"\\ninfo:\\n  version: \\\"1.0.0\\\"\\n  title: \\\"Swagger Petstore\\\"\\n  license:\\n    name: \\\"MIT\\\"\\nservers:\\n- url: \\\"http://petstore.swagger.io/v1\\\"\\npaths:\\n  /pets:\\n    get:\\n      summary: \\\"List all pets\\\"\\n      operationId: \\\"listPets\\\"\\n      tags:\\n      - \\\"pets\\\"\\n      parameters:\\n      - name: \\\"limit\\\"\\n        in: \\\"query\\\"\\n        description: \\\"How many items to return at one time (max 100)\\\"\\n        required: false\\n        schema:\\n          type: \\\"integer\\\"\\n          format: \\\"int32\\\"\\n      security:\\n      - petstore_auth:\\n        - \\\"read:pets\\\"\\n      responses:\\n        \\\"200\\\":\\n          description: \\\"An paged array of pets\\\"\\n          headers:\\n            x-next:\\n              description: \\\"A link to the next page of responses\\\"\\n              schema:\\n                type: \\\"string\\\"\\n          content:\\n            application/json:\\n              schema:\\n                type: \\\"array\\\"\\n                items:\\n                  $ref: \\\"#/components/schemas/Pet\\\"\\n              example:\\n              - id: 1\\n                name: \\\"catten\\\"\\n                tag: \\\"cat\\\"\\n              - id: 2\\n                name: \\\"doggy\\\"\\n                tag: \\\"dog\\\"\\n        default:\\n          description: \\\"unexpected error\\\"\\n          content:\\n            application/json:\\n              schema:\\n                $ref: \\\"#/components/schemas/Error\\\"\\n    post:\\n      summary: \\\"Create a pet\\\"\\n      operationId: \\\"createPets\\\"\\n      requestBody:\\n        description: \\\"Pet to add to the store\\\"\\n        required: true\\n        content:\\n          application/json:\\n            schema:\\n              $ref: \\\"#/components/schemas/Pet\\\"\\n      tags:\\n      - \\\"pets\\\"\\n      security:\\n      - petstore_auth:\\n        - \\\"read:pets\\\"\\n        - \\\"write:pets\\\"\\n      responses:\\n        \\\"201\\\":\\n          description: \\\"Null response\\\"\\n        default:\\n          description: \\\"unexpected error\\\"\\n          content:\\n            application/json:\\n              schema:\\n                $ref: \\\"#/components/schemas/Error\\\"\\n  /pets/{petId}:\\n    get:\\n      summary: \\\"Info for a specific pet\\\"\\n      operationId: \\\"showPetById\\\"\\n      tags:\\n      - \\\"pets\\\"\\n      parameters:\\n      - name: \\\"petId\\\"\\n        in: \\\"path\\\"\\n        required: true\\n        description: \\\"The id of the pet to retrieve\\\"\\n        schema:\\n          type: \\\"string\\\"\\n      security:\\n      - petstore_auth:\\n        - \\\"read:pets\\\"\\n      responses:\\n        \\\"200\\\":\\n          description: \\\"Expected response to a valid request\\\"\\n          content:\\n            application/json:\\n              schema:\\n                $ref: \\\"#/components/schemas/Pet\\\"\\n              example:\\n                id: 1\\n                name: \\\"Jessica Right\\\"\\n                tag: \\\"pet\\\"\\n        default:\\n          description: \\\"unexpected error\\\"\\n          content:\\n            application/json:\\n              schema:\\n                $ref: \\\"#/components/schemas/Error\\\"\\n    put:\\n      summary: \\\"Update a pet\\\"\\n      operationId: \\\"updatePets\\\"\\n      parameters:\\n      - name: \\\"petId\\\"\\n        in: \\\"path\\\"\\n        required: true\\n        description: \\\"The id of the pet to update\\\"\\n        schema:\\n          type: \\\"string\\\"\\n      requestBody:\\n        description: \\\"Pet to update\\\"\\n        required: true\\n        content:\\n          application/json:\\n            schema:\\n              $ref: \\\"#/components/schemas/UpdatePet\\\"\\n      tags:\\n      - \\\"pets\\\"\\n      security:\\n      - petstore_auth:\\n        - \\\"read:pets\\\"\\n        - \\\"write:pets\\\"\\n      responses:\\n        \\\"200\\\":\\n          description: \\\"Successfully updated pets\\\"\\n        default:\\n          description: \\\"unexpected error\\\"\\n          content:\\n            application/json:\\n              schema:\\n                $ref: \\\"#/components/schemas/Error\\\"\\n    delete:\\n      summary: \\\"Delete a specific pet\\\"\\n      operationId: \\\"deletePetById\\\"\\n      tags:\\n      - \\\"pets\\\"\\n      parameters:\\n      - name: \\\"petId\\\"\\n        in: \\\"path\\\"\\n        required: true\\n        description: \\\"The id of the pet to delete\\\"\\n        schema:\\n          type: \\\"string\\\"\\n      - name: \\\"key\\\"\\n        in: \\\"header\\\"\\n        required: true\\n        description: \\\"The key header\\\"\\n        schema:\\n          type: \\\"string\\\"\\n      security:\\n      - petstore_auth:\\n        - \\\"write:pets\\\"\\n      responses:\\n        \\\"200\\\":\\n          description: \\\"Expected response to a valid request\\\"\\n          content:\\n            application/json:\\n              schema:\\n                $ref: \\\"#/components/schemas/Pet\\\"\\n              examples:\\n                response:\\n                  value:\\n                    id: 1\\n                    name: \\\"Jessica Right\\\"\\n                    tag: \\\"pet\\\"\\n        default:\\n          description: \\\"unexpected error\\\"\\n          content:\\n            application/json:\\n              schema:\\n                $ref: \\\"#/components/schemas/Error\\\"\\n  /accounts:\\n    get:\\n      summary: \\\"Get a list of accounts\\\"\\n      operationId: \\\"getAccounts\\\"\\n      tags:\\n      - \\\"accounts\\\"\\n      parameters:\\n      - name: \\\"limit\\\"\\n        in: \\\"query\\\"\\n        description: \\\"How many items to return at one time (max 100)\\\"\\n        required: false\\n        schema:\\n          type: \\\"integer\\\"\\n          format: \\\"int32\\\"\\n      security:\\n      - petstore_auth:\\n        - \\\"read:pets\\\"\\n      responses:\\n        \\\"200\\\":\\n          description: \\\"An paged array of accounts\\\"\\n          content:\\n            application/json:\\n              schema:\\n                type: \\\"array\\\"\\n                items:\\n                  $ref: \\\"#/components/schemas/Account\\\"\\n              example:\\n              - accountNo: 123\\n                ownerId: \\\"johndoe\\\"\\n                accountType: \\\"P\\\"\\n                firstName: \\\"John\\\"\\n                lastName: \\\"Doe\\\"\\n                status: \\\"O\\\"\\n              - id: 2\\n                accountNo: 456\\n                ownerId: \\\"johndoe\\\"\\n                accountType: \\\"B\\\"\\n                firstName: \\\"John\\\"\\n                lastName: \\\"Doe\\\"\\n                status: \\\"C\\\"\\n        default:\\n          description: \\\"unexpected error\\\"\\n          content:\\n            application/json:\\n              schema:\\n                $ref: \\\"#/components/schemas/Error\\\"\\n    post:\\n      summary: \\\"Create an account\\\"\\n      operationId: \\\"createAccount\\\"\\n      requestBody:\\n        description: \\\"Account to add to the system\\\"\\n        required: true\\n        content:\\n          application/json:\\n            schema:\\n              $ref: \\\"#/components/schemas/Account\\\"\\n      tags:\\n      - \\\"accounts\\\"\\n      security:\\n      - petstore_auth:\\n        - \\\"read:pets\\\"\\n        - \\\"write:pets\\\"\\n      responses:\\n        \\\"201\\\":\\n          description: \\\"Null response\\\"\\n        default:\\n          description: \\\"unexpected error\\\"\\n          content:\\n            application/json:\\n              schema:\\n                $ref: \\\"#/components/schemas/Error\\\"\\n  /accounts/{accountNo}:\\n    get:\\n      summary: \\\"get account with a specific accountNo\\\"\\n      operationId: \\\"getAccountByNo\\\"\\n      tags:\\n      - \\\"accounts\\\"\\n      parameters:\\n      - name: \\\"accountNo\\\"\\n        in: \\\"path\\\"\\n        required: true\\n        description: \\\"The accountNo of the account to retrieve\\\"\\n        schema:\\n          type: \\\"string\\\"\\n      security:\\n      - petstore_auth:\\n        - \\\"read:pets\\\"\\n      responses:\\n        \\\"200\\\":\\n          description: \\\"Expected response to a valid request\\\"\\n          content:\\n            application/json:\\n              schema:\\n                $ref: \\\"#/components/schemas/Account\\\"\\n              example:\\n                accountNo: 123\\n                ownerId: \\\"johndoe\\\"\\n                accountType: \\\"P\\\"\\n                firstName: \\\"John\\\"\\n                lastName: \\\"Doe\\\"\\n                status: \\\"O\\\"\\n        default:\\n          description: \\\"unexpected error\\\"\\n          content:\\n            application/json:\\n              schema:\\n                $ref: \\\"#/components/schemas/Error\\\"\\n    put:\\n      summary: \\\"Update an account by accountNo\\\"\\n      operationId: \\\"updateAccountByNo\\\"\\n      parameters:\\n      - name: \\\"accountNo\\\"\\n        in: \\\"path\\\"\\n        required: true\\n        description: \\\"The account no of the account to update\\\"\\n        schema:\\n          type: \\\"string\\\"\\n      requestBody:\\n        description: \\\"Account to update\\\"\\n        required: true\\n        content:\\n          application/json:\\n            schema:\\n              $ref: \\\"#/components/schemas/Account\\\"\\n      tags:\\n      - \\\"accounts\\\"\\n      security:\\n      - petstore_auth:\\n        - \\\"read:pets\\\"\\n        - \\\"write:pets\\\"\\n      responses:\\n        \\\"200\\\":\\n          description: \\\"Successfully updated accounts\\\"\\n        default:\\n          description: \\\"unexpected error\\\"\\n          content:\\n            application/json:\\n              schema:\\n                $ref: \\\"#/components/schemas/Error\\\"\\n    delete:\\n      summary: \\\"Delete a specific account by accountNo\\\"\\n      operationId: \\\"deleteAccountByNo\\\"\\n      tags:\\n      - \\\"accounts\\\"\\n      parameters:\\n      - name: \\\"accountNo\\\"\\n        in: \\\"path\\\"\\n        required: true\\n        description: \\\"The no of the account to delete\\\"\\n        schema:\\n          type: \\\"string\\\"\\n      security:\\n      - petstore_auth:\\n        - \\\"write:pets\\\"\\n      responses:\\n        \\\"200\\\":\\n          description: \\\"Expected response to a valid request\\\"\\n          content:\\n            application/json:\\n              schema:\\n                $ref: \\\"#/components/schemas/Account\\\"\\n              examples:\\n                response:\\n                  value:\\n                    accountNo: 123\\n                    ownerId: \\\"johndoe\\\"\\n                    accountType: \\\"P\\\"\\n                    firstName: \\\"John\\\"\\n                    lastName: \\\"Doe\\\"\\n                    status: \\\"O\\\"\\n        default:\\n          description: \\\"unexpected error\\\"\\n          content:\\n            application/json:\\n              schema:\\n                $ref: \\\"#/components/schemas/Error\\\"\\n  /notifications:\\n    get:\\n      summary: \\\"Get Notifications\\\"\\n      operationId: \\\"listNotifications\\\"\\n      tags:\\n      - \\\"notifications\\\"\\n      security:\\n      - petstore_auth:\\n        - \\\"read:pets\\\"\\n      responses:\\n        \\\"200\\\":\\n          description: \\\"A standard notification response in JSON for response interceptor\\\\\\n            \\\\ test\\\"\\n        default:\\n          description: \\\"unexpected error\\\"\\n          content:\\n            application/json:\\n              schema:\\n                $ref: \\\"#/components/schemas/Error\\\"\\n  /flowers:\\n    post:\\n      summary: \\\"The API accept XML and the consumer is using JSON\\\"\\n      operationId: \\\"flowers\\\"\\n      tags:\\n      - \\\"flowers\\\"\\n      security:\\n      - petstore_auth:\\n        - \\\"read:pets\\\"\\n      responses:\\n        \\\"200\\\":\\n          description: \\\"Return an flowers XML as the demo soap service\\\"\\n        default:\\n          description: \\\"unexpected error\\\"\\n          content:\\n            application/json:\\n              schema:\\n                $ref: \\\"#/components/schemas/Error\\\"\\n  /documents:\\n    get:\\n      summary: \\\"The API to get a document in a JSON with base64 content\\\"\\n      operationId: \\\"documents\\\"\\n      tags:\\n      - \\\"documents\\\"\\n      security:\\n      - petstore_auth:\\n        - \\\"read:pets\\\"\\n      responses:\\n        \\\"200\\\":\\n          description: \\\"Return a document with base64 content in JSON format\\\"\\n        default:\\n          description: \\\"unexpected error\\\"\\n          content:\\n            application/json:\\n              schema:\\n                $ref: \\\"#/components/schemas/Error\\\"\\n  /streams:\\n    get:\\n      summary: \\\"The API to get a stream of json response\\\"\\n      operationId: \\\"streams\\\"\\n      tags:\\n      - \\\"streams\\\"\\n      security:\\n      - petstore_auth:\\n        - \\\"read:pets\\\"\\n      responses:\\n        \\\"200\\\":\\n          description: \\\"Return a stream with json content in JSON format\\\"\\n        default:\\n          description: \\\"unexpected error\\\"\\n          content:\\n            application/json:\\n              schema:\\n                $ref: \\\"#/components/schemas/Error\\\"\\ncomponents:\\n  securitySchemes:\\n    petstore_auth:\\n      type: \\\"oauth2\\\"\\n      description: \\\"This API uses OAuth 2 with the client credential grant flow.\\\"\\n      flows:\\n        clientCredentials:\\n          tokenUrl: \\\"https://localhost:6882/token\\\"\\n          scopes:\\n            write:pets: \\\"modify pets in your account\\\"\\n            read:pets: \\\"read your pets\\\"\\n  schemas:\\n    Pet:\\n      allOf:\\n      - $ref: \\\"#/components/schemas/NewPet\\\"\\n      - type: \\\"object\\\"\\n        required:\\n        - \\\"id\\\"\\n        properties:\\n          id:\\n            type: \\\"integer\\\"\\n            format: \\\"int64\\\"\\n    NewPet:\\n      type: \\\"object\\\"\\n      required:\\n      - \\\"name\\\"\\n      properties:\\n        name:\\n          type: \\\"string\\\"\\n        tag:\\n          type: \\\"string\\\"\\n    UpdatePet:\\n      type: \\\"object\\\"\\n      properties:\\n        petAge:\\n          $ref: \\\"#/components/schemas/petAge\\\"\\n        petToys:\\n          $ref: \\\"#/components/schemas/petToys\\\"\\n        ownerEmail:\\n          $ref: \\\"#/components/schemas/ownerEmail\\\"\\n        ownerSsn:\\n          $ref: \\\"#/components/schemas/ownerSsn\\\"\\n      additionalProperties: false\\n    petAge:\\n      maximum: 20\\n      minimum: 1\\n      type: \\\"integer\\\"\\n      description: \\\"current age of the pet\\\"\\n      nullable: false\\n    petToys:\\n      type: \\\"array\\\"\\n      description: \\\"Toys of the pet\\\"\\n      items:\\n        type: \\\"string\\\"\\n    ownerEmail:\\n      maxLength: 65\\n      minLength: 2\\n      pattern: \\\"^[A-Z0-9._%+-]+@[A-Z0-9.-]+\\\\\\\\.[A-Z]{2,6}$\\\"\\n      type: \\\"string\\\"\\n      description: \\\"owner email\\\"\\n    ownerSsn:\\n      pattern: \\\"^\\\\\\\\d{3}-\\\\\\\\d{2}-\\\\\\\\d{4}$\\\"\\n      type: \\\"string\\\"\\n      description: \\\"owner SSN\\\"\\n    Account:\\n      type: \\\"object\\\"\\n      required:\\n      - \\\"accountNo\\\"\\n      - \\\"userId\\\"\\n      - \\\"accountType\\\"\\n      - \\\"status\\\"\\n      properties:\\n        accountNo:\\n          type: \\\"string\\\"\\n        userId:\\n          type: \\\"string\\\"\\n        accountType:\\n          type: \\\"string\\\"\\n        firstName:\\n          type: \\\"string\\\"\\n        lastName:\\n          type: \\\"string\\\"\\n        status:\\n          type: \\\"string\\\"\\n    Error:\\n      type: \\\"object\\\"\\n      required:\\n      - \\\"code\\\"\\n      - \\\"message\\\"\\n      properties:\\n        code:\\n          type: \\\"integer\\\"\\n          format: \\\"int32\\\"\\n        message:\\n          type: \\\"string\\\"\\n\",\"apiId\":\"0100\",\"hostId\":\"01964b05-552a-7c4b-9184-6857e7f3dc5f\",\"apiType\":\"openapi\",\"updateTs\":\"2025-08-19T18:02:24.798737Z\",\"endpoints\":[{\"scopes\":[\"read:pets\"],\"endpoint\":\"/v1/pets@get\",\"endpointId\":\"0198c384-7963-735e-a174-40a55788a35e\",\"httpMethod\":\"get\",\"endpointDesc\":\"List all pets\",\"endpointName\":\"listPets\",\"endpointPath\":\"/pets\"},{\"scopes\":[\"read:pets\",\"write:pets\"],\"endpoint\":\"/v1/pets@post\",\"endpointId\":\"0198c384-7963-7434-a176-55d7c50e4ec8\",\"httpMethod\":\"post\",\"endpointDesc\":\"Create a pet\",\"endpointName\":\"createPets\",\"endpointPath\":\"/pets\"},{\"scopes\":[\"read:pets\"],\"endpoint\":\"/v1/pets/{petId}@get\",\"endpointId\":\"0198c384-7963-7468-a178-a7b5e5fcecf9\",\"httpMethod\":\"get\",\"endpointDesc\":\"Info for a specific pet\",\"endpointName\":\"showPetById\",\"endpointPath\":\"/pets/{petId}\"},{\"scopes\":[\"read:pets\",\"write:pets\"],\"endpoint\":\"/v1/pets/{petId}@put\",\"endpointId\":\"0198c384-7963-7499-a17a-29eac620bebe\",\"httpMethod\":\"put\",\"endpointDesc\":\"Update a pet\",\"endpointName\":\"updatePets\",\"endpointPath\":\"/pets/{petId}\"},{\"scopes\":[\"write:pets\"],\"endpoint\":\"/v1/pets/{petId}@delete\",\"endpointId\":\"0198c384-7963-74bd-a17c-c8ae88485bcd\",\"httpMethod\":\"delete\",\"endpointDesc\":\"Delete a specific pet\",\"endpointName\":\"deletePetById\",\"endpointPath\":\"/pets/{petId}\"},{\"scopes\":[\"read:pets\"],\"endpoint\":\"/v1/accounts@get\",\"endpointId\":\"0198c384-7963-74e4-a17e-02c0f1576da1\",\"httpMethod\":\"get\",\"endpointDesc\":\"Get a list of accounts\",\"endpointName\":\"getAccounts\",\"endpointPath\":\"/accounts\"},{\"scopes\":[\"read:pets\",\"write:pets\"],\"endpoint\":\"/v1/accounts@post\",\"endpointId\":\"0198c384-7963-750f-a180-9a9043a67a24\",\"httpMethod\":\"post\",\"endpointDesc\":\"Create an account\",\"endpointName\":\"createAccount\",\"endpointPath\":\"/accounts\"},{\"scopes\":[\"read:pets\"],\"endpoint\":\"/v1/accounts/{accountNo}@get\",\"endpointId\":\"0198c384-7963-7535-a182-837e6d484c4a\",\"httpMethod\":\"get\",\"endpointDesc\":\"get account with a specific accountNo\",\"endpointName\":\"getAccountByNo\",\"endpointPath\":\"/accounts/{accountNo}\"},{\"scopes\":[\"read:pets\",\"write:pets\"],\"endpoint\":\"/v1/accounts/{accountNo}@put\",\"endpointId\":\"0198c384-7963-7560-a184-dc02fce8c093\",\"httpMethod\":\"put\",\"endpointDesc\":\"Update an account by accountNo\",\"endpointName\":\"updateAccountByNo\",\"endpointPath\":\"/accounts/{accountNo}\"},{\"scopes\":[\"write:pets\"],\"endpoint\":\"/v1/accounts/{accountNo}@delete\",\"endpointId\":\"0198c384-7963-7580-a186-e358d42c33dd\",\"httpMethod\":\"delete\",\"endpointDesc\":\"Delete a specific account by accountNo\",\"endpointName\":\"deleteAccountByNo\",\"endpointPath\":\"/accounts/{accountNo}\"},{\"scopes\":[\"read:pets\"],\"endpoint\":\"/v1/notifications@get\",\"endpointId\":\"0198c384-7963-75a6-a188-166a086e8050\",\"httpMethod\":\"get\",\"endpointDesc\":\"Get Notifications\",\"endpointName\":\"listNotifications\",\"endpointPath\":\"/notifications\"},{\"scopes\":[\"read:pets\"],\"endpoint\":\"/v1/flowers@post\",\"endpointId\":\"0198c384-7963-75d5-a18a-40fb0d532963\",\"httpMethod\":\"post\",\"endpointDesc\":\"The API accept XML and the consumer is using JSON\",\"endpointName\":\"flowers\",\"endpointPath\":\"/flowers\"},{\"scopes\":[\"read:pets\"],\"endpoint\":\"/v1/documents@get\",\"endpointId\":\"0198c384-7963-7629-a18c-f837cb2a2119\",\"httpMethod\":\"get\",\"endpointDesc\":\"The API to get a document in a JSON with base64 content\",\"endpointName\":\"documents\",\"endpointPath\":\"/documents\"},{\"scopes\":[\"read:pets\"],\"endpoint\":\"/v1/streams@get\",\"endpointId\":\"0198c384-7963-765b-a18e-c84a7b5af3be\",\"httpMethod\":\"get\",\"endpointDesc\":\"The API to get a stream of json response\",\"endpointName\":\"streams\",\"endpointPath\":\"/streams\"}],\"serviceId\":\"com.networknt.petstore-1.0.0\",\"apiVersion\":\"1.0.0\",\"updateUser\":\"postgres\",\"apiVersionId\":\"019664ec-c3e4-71f0-9b6c-3c0893ee688e\",\"apiVersionDesc\":\"First Major release\",\"aggregateVersion\":4,\"newAggregateVersion\":5},\"aggregateversion\":\"4\",\"subject\":\"019664ec-c3e4-71f0-9b6c-3c0893ee688e\",\"source\":\"https://github.com/lightapi/light-portal\",\"type\":\"ServiceSpecUpdatedEvent\",\"nonce\":1612,\"host\":\"01964b05-552a-7c4b-9184-6857e7f3dc5f\",\"specversion\":\"1.0\",\"id\":\"0198c384-79c6-7e9d-af45-85e814457d7f\",\"time\":\"2025-08-19T18:08:15.814963813Z\",\"user\":\"01964b05-5532-7c79-8cde-191dcbd421b8\",\"aggregatetype\":\"ServiceSpec\"}";
        Map<String, Object> map = JsonMapper.string2Map(s);
        Connection conn = SqlDbStartupHook.ds.getConnection();
        dbProvider.updateServiceSpec(conn, map);
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

}
