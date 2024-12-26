package net.lightapi.db;

import com.networknt.config.Config;
import com.networknt.config.JsonMapper;
import com.networknt.db.provider.DbProvider;
import com.networknt.db.provider.DbProviderConfig;
import com.networknt.db.provider.SqlDbStartupHook;
import com.networknt.monad.Result;
import com.networknt.service.SingletonServiceFactory;
import com.networknt.utility.NioUtils;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import net.lightapi.portal.db.PortalDbProvider;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

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

}
