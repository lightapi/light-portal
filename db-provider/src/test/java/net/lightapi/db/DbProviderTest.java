package net.lightapi.db;

import com.networknt.config.Config;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import net.lightapi.portal.db.DbConfig;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import java.sql.Connection;

public class DbProviderTest {
    @Test
    @Disabled
    public void testDbConnection() {
        DbConfig config = (DbConfig) Config.getInstance().getJsonObjectConfig(DbConfig.CONFIG_NAME, DbConfig.class);

        HikariConfig hikariConfig = new HikariConfig();
        hikariConfig.setDriverClassName(config.getDriverClassName());
        hikariConfig.setUsername(config.getUsername());
        hikariConfig.setPassword(config.getPassword());
        hikariConfig.setJdbcUrl(config.getJdbcUrl());
        hikariConfig.setMaximumPoolSize(config.getMaximumPoolSize());
        HikariDataSource ds = new HikariDataSource(hikariConfig);
        System.out.println(ds);
        try (Connection connection = ds.getConnection()) {
            System.out.println(connection);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
