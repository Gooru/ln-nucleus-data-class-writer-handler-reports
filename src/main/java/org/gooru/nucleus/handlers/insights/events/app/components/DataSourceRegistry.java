package org.gooru.nucleus.handlers.insights.events.app.components;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.sql.DataSource;

import org.gooru.nucleus.handlers.insights.events.bootstrap.shutdown.Finalizer;
import org.gooru.nucleus.handlers.insights.events.bootstrap.startup.Initializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;

import io.vertx.core.Vertx;
import io.vertx.core.json.JsonObject;

public class DataSourceRegistry implements Initializer, Finalizer {

    private static final String DEFAULT_DATA_SOURCE = "defaultDataSource";
    private static final String DEFAULT_DATA_SOURCE_TYPE = "nucleus.ds.type";
    private static final String DS_HIKARI = "hikari";
    private static final Logger LOGGER = LoggerFactory.getLogger(DataSourceRegistry.class);
    // All the elements in this array are supposed to be present in config file
    // as keys as we are going to initialize them with the value associated with
    // that key
    private final List<String> datasources = Arrays.asList(DEFAULT_DATA_SOURCE);
    private final Map<String, DataSource> registry = new HashMap<>();
    private volatile boolean initialized;

    private DataSourceRegistry() {
        // TODO Auto-generated constructor stub
    }

    public static DataSourceRegistry getInstance() {
        return Holder.INSTANCE;
    }

    @Override
    public void initializeComponent(Vertx vertx, JsonObject config) {
        // Skip if we are already initialized
        LOGGER.debug("Initialization called upon.");
        if (!initialized) {
            LOGGER.debug("May have to do initialization");
            // We need to do initialization, however, we are running it via
            // verticle instance which is going to run in
            // multiple threads hence we need to be safe for this operation
            synchronized (Holder.INSTANCE) {
                LOGGER.debug("Will initialize after double checking");
                if (!initialized) {
                    LOGGER.debug("Initializing now");
                    for (String datasource : datasources) {
                        JsonObject dbConfig = config.getJsonObject(datasource);
                        if (dbConfig != null) {
                            DataSource ds = initializeDataSource(dbConfig);
                            registry.put(datasource, ds);
                        }
                    }
                    initialized = true;
                }
            }
        }
    }

    public DataSource getDefaultDataSource() {
        return registry.get(DEFAULT_DATA_SOURCE);
    }

    public DataSource getDataSourceByName(String name) {
        if (name != null) {
            return registry.get(name);
        }
        return null;
    }

    private DataSource initializeDataSource(JsonObject dbConfig) {
        // The default DS provider is hikari, so if set explicitly or not set
        // use it, else error out
        String dsType = dbConfig.getString(DEFAULT_DATA_SOURCE_TYPE);
        if (dsType != null && !dsType.equals(DS_HIKARI)) {
            // No support
            throw new IllegalStateException("Unsupported data store type");
        }
        final HikariConfig config = new HikariConfig();

        for (Map.Entry<String, Object> entry : dbConfig) {
            switch (entry.getKey()) {
            case "dataSourceClassName":
                config.setDataSourceClassName((String) entry.getValue());
                break;
            case "jdbcUrl":
                config.setJdbcUrl((String) entry.getValue());
                break;
            case "username":
                config.setUsername((String) entry.getValue());
                break;
            case "password":
                config.setPassword((String) entry.getValue());
                break;
            case "autoCommit":
                config.setAutoCommit((Boolean) entry.getValue());
                break;
            case "connectionTimeout":
                config.setConnectionTimeout((Long) entry.getValue());
                break;
            case "idleTimeout":
                config.setIdleTimeout((Long) entry.getValue());
                break;
            case "maxLifetime":
                config.setMaxLifetime((Long) entry.getValue());
                break;
            case "connectionTestQuery":
                config.setConnectionTestQuery((String) entry.getValue());
                break;
            case "minimumIdle":
                config.setMinimumIdle((Integer) entry.getValue());
                break;
            case "maximumPoolSize":
                config.setMaximumPoolSize((Integer) entry.getValue());
                break;
            case "metricRegistry":
                throw new UnsupportedOperationException(entry.getKey());
            case "healthCheckRegistry":
                throw new UnsupportedOperationException(entry.getKey());
            case "poolName":
                config.setPoolName((String) entry.getValue());
                break;
            case "isolationInternalQueries":
                config.setIsolateInternalQueries((Boolean) entry.getValue());
                break;
            case "allowPoolSuspension":
                config.setAllowPoolSuspension((Boolean) entry.getValue());
                break;
            case "readOnly":
                config.setReadOnly((Boolean) entry.getValue());
                break;
            case "registerMBeans":
                config.setRegisterMbeans((Boolean) entry.getValue());
                break;
            case "catalog":
                config.setCatalog((String) entry.getValue());
                break;
            case "connectionInitSql":
                config.setConnectionInitSql((String) entry.getValue());
                break;
            case "driverClassName":
                config.setDriverClassName((String) entry.getValue());
                break;
            case "transactionIsolation":
                config.setTransactionIsolation((String) entry.getValue());
                break;
            case "validationTimeout":
                config.setValidationTimeout((Long) entry.getValue());
                break;
            case "leakDetectionThreshold":
                config.setLeakDetectionThreshold((Long) entry.getValue());
                break;
            case "dataSource":
                throw new UnsupportedOperationException(entry.getKey());
            case "threadFactory":
                throw new UnsupportedOperationException(entry.getKey());
            case "datasource":
                for (Map.Entry<String, Object> key : ((JsonObject) entry.getValue())) {
                    config.addDataSourceProperty(key.getKey(), key.getValue());
                }
                break;
            }
        }

        return new HikariDataSource(config);

    }

    @Override
    public void finalizeComponent() {
        for (String datasource : datasources) {
            DataSource ds = registry.get(datasource);
            if (ds != null) {
                if (ds instanceof HikariDataSource) {
                    ((HikariDataSource) ds).close();
                }
            }
        }
    }

    private static final class Holder {
        private static final DataSourceRegistry INSTANCE = new DataSourceRegistry();
    }

}
