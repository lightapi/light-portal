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
    void testQueryRefTableNoParameter() {
        Result<String> result = dbProvider.queryRefTable(0, 2, "N2CMw0HGQXeLvC1wBfln2A", null, null, "Y", "Y", null);
        if(result.isFailure()) {
            System.out.println(result.getError());
        } else {
            System.out.println(result.getResult());
        }

    }

    @Test
    void testQueryRefTable() {
        Result<String> result = dbProvider.queryRefTable(0, 2, "N2CMw0HGQXeLvC1wBfln2A", null, "t", "Y", "Y", "Y");
        if(result.isFailure()) {
            System.out.println(result.getError());
        } else {
            System.out.println(result.getResult());
        }

    }

    @Test
    void testQueryService() {
        Result<String> result = dbProvider.queryService(0, 2, "N2CMw0HGQXeLvC1wBfln2A", "1234",
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
        Result<String> result = dbProvider.queryRule(0, 2, "N2CMw0HGQXeLvC1wBfln2A", null,
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
        Result<Integer> result = dbProvider.queryNonceByUserId("utgdG50vRVOX3mL1Kf83aA");
        if(result.isFailure()) {
            System.out.println(result.getError());
        } else {
            System.out.println(result.getResult());
        }
    }

    @Test
    public void testQueryRole()  {
        Result<String> result = dbProvider.queryRole(0, 4, "N2CMw0HGQXeLvC1wBfln2A", null, null);
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
        Result<String> result = dbProvider.queryGroup(0, 2, "N2CMw0HGQXeLvC1wBfln2A", "le", null);
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
        Result<String> result = dbProvider.queryPosition(0, 2, "N2CMw0HGQXeLvC1wBfln2A", null, null, "Y", null);
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
        Result<String> result = dbProvider.queryAttribute(0, 2, "N2CMw0HGQXeLvC1wBfln2A", "ou", null, null);
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
        Result<String> result = dbProvider.queryServicePermission("N2CMw0HGQXeLvC1wBfln2A", "0100", "1.0.0");
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
        Result<List<String>> result = dbProvider.queryServiceFilter("N2CMw0HGQXeLvC1wBfln2A", "0100", "1.0.0");
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
        Result<String> result = dbProvider.queryUserByHostId(0, 2, "N2CMw0HGQXeLvC1wBfln2A", null, null,
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
        Result<String> result = dbProvider.queryProviderKey("biAGY0FdRkS38JUtOQymuQ");
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
        Result<String> result = dbProvider.getProduct(0, 2, "N2CMw0HGQXeLvC1wBfln2A", null, null, null, null, null, null, null, null, null, null);
        if(result.isFailure()) {
            System.out.println(result.getError());
        } else {
            System.out.println(result.getResult());
        }
    }

    @Test
    void testGetCategory() {
        Result<String> result = dbProvider.getCategory(0, 2, "N2CMw0HGQXeLvC1wBfln2A", null, null, null, null, null, null, null);
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

}
