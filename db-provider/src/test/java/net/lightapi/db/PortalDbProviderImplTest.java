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

import java.util.List;
import java.util.Map;

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
        Result<String> result = dbProvider.getRefTable(0, 100, null, null, null, null, null, null);
        if(result.isFailure()) {
            System.out.println(result.getError());
        } else {
            System.out.println(result.getResult());
        }
    }

    @Test
    void testGetRefTableWithHost() {
        Result<String> result = dbProvider.getRefTable(0, 100, "01964b05-552a-7c4b-9184-6857e7f3dc5f", null, null, null, null, null);
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
                null, null, null, null, null, null, null,
                "Y");
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
        Result<String> result = dbProvider.queryUserByHostId(0, 2, "01964b05-552a-7c4b-9184-6857e7f3dc5f", null, null,
                null, null,null, null, null, null, null, null,
                null, null, null, null, null, null, null, null);
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
        Result<String> result = dbProvider.getProduct(0, 2, "01964b05-552a-7c4b-9184-6857e7f3dc5f", null, null, null, null, null, null, null, null, null, null);
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
    void testCommitConfigInstance() {
        Map<String, Object> map = Map.of("hostId", "01964b05-552a-7c4b-9184-6857e7f3dc5f",
                "instanceId", "0196923e-ad58-7f40-8004-3d6fb657bad5",
                "productVersion", "1.0.0",
                "serviceId", "0100",
                "serviceVersion", "1.0.0",
                "apiId", "0100",
                "apiVersion", "1.0.0",
                "configPhase", "R",
                "configType", "R");

        Result<String> result = dbProvider.getTag(0, 2, null, null, null, null, null);
        if(result.isFailure()) {
            System.out.println(result.getError());
        } else {
            System.out.println(result.getResult());
        }
    }
}
