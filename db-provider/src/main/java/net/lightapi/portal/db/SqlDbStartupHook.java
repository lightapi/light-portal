package net.lightapi.portal.db;

import com.networknt.cache.CacheManager;
import com.networknt.config.Config;
import com.networknt.server.StartupHookProvider;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Start up hook for the SQL provider to create the datasource and initial the cache.
 * It is for both command and query side.
 *
 * @author Steve Hu
 */

public class SqlDbStartupHook implements StartupHookProvider {
    private static final Logger logger = LoggerFactory.getLogger(SqlDbStartupHook.class);
    // shared datasource that can be used to get a database connection.
    static DbConfig config = (DbConfig)Config.getInstance().getJsonObjectConfig(DbConfig.CONFIG_NAME, DbConfig.class);
    public static HikariDataSource ds;
    // key and json cache for the dropdowns.
    public static CacheManager cacheManager;


    @Override
    public void onStartup() {
        logger.info("SqlDbStartupHook begins");
        HikariConfig hikariConfig = new HikariConfig();
        hikariConfig.setDriverClassName(config.getDriverClassName());
        hikariConfig.setUsername(config.getUsername());
        hikariConfig.setPassword(config.getPassword());
        hikariConfig.setJdbcUrl(config.getJdbcUrl());
        if(logger.isTraceEnabled()) logger.trace("jdbcUrl = " + config.getJdbcUrl());
        hikariConfig.setMaximumPoolSize(config.getMaximumPoolSize());
        ds = new HikariDataSource(hikariConfig);
        cacheManager = CacheManager.getInstance();
        logger.info("SqlDbStartupHook ends");

    }
}
