package com.etl.etltool.config;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import jakarta.annotation.PreDestroy;
import lombok.extern.slf4j.Slf4j;
import org.springframework.core.annotation.Order;
import org.springframework.stereotype.Component;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * ✅ Singleton DataSource Manager з Graceful Shutdown
 *
 * Призначення:
 * - Гарантує що для кожної БД створюється ТІЛЬКИ ОДИН connection pool
 * - Уникає витоку з'єднань при багатопоточності
 * - Thread-safe через ConcurrentHashMap
 * - Блокує створення нових pools під час shutdown
 *
 * Приклад:
 * - Thread 1 викликає getDataSource("jdbc:postgresql://localhost/db1", "user", "pass")
 * - Thread 2 викликає getDataSource("jdbc:postgresql://localhost/db1", "user", "pass")
 * → Обидва отримають ОДИН і той самий HikariDataSource pool
 */
@Slf4j
@Component
@Order(2) // ✅ Виконується ДРУГИМ при shutdown (після зупинки batch jobs)
public class DataSourceManager {

    // ✅ Thread-safe кеш DataSource
    // Key = "dbUrl|dbUser"
    // Value = HikariDataSource pool
    private final ConcurrentHashMap<String, DataSource> dataSourceCache = new ConcurrentHashMap<>();

    // ✅ Прапорець shutdown стану
    private static final AtomicBoolean shuttingDown = new AtomicBoolean(false);

    /**
     * ✅ Отримати DataSource (або створити новий, якщо не існує)
     *
     * @param dbUrl JDBC URL (напр. jdbc:postgresql://localhost:5433/mydb)
     * @param dbUser Ім'я користувача БД
     * @param dbPassword Пароль БД
     * @return DataSource з connection pool
     * @throws IllegalStateException якщо додаток зупиняється
     */
    public DataSource getDataSource(String dbUrl, String dbUser, String dbPassword) {
        // ✅ ПЕРЕВІРКА 1: Чи не зупиняється додаток?
        if (shuttingDown.get()) {
            log.error("❌ Cannot create DataSource - application is shutting down");
            log.error("❌ Requested URL: {}", dbUrl);
            throw new IllegalStateException("Application shutdown in progress - DataSource creation blocked");
        }

        String cacheKey = buildCacheKey(dbUrl, dbUser);

        log.info("┌─────────────────────────────────────────────────");
        log.info("│ 🔍 DataSource Request");
        log.info("│ URL: {}", dbUrl);
        log.info("│ User: {}", dbUser);
        log.info("│ Cache key: {}", cacheKey);
        log.info("│ Shutdown state: {}", shuttingDown.get() ? "SHUTTING DOWN ⚠️" : "ACTIVE ✅");
        log.info("└─────────────────────────────────────────────────");

        // ✅ computeIfAbsent - атомарна операція:
        // - Якщо ключ існує → повертає існуючий DataSource
        // - Якщо ключа немає → створює новий і додає в cache
        DataSource dataSource = dataSourceCache.computeIfAbsent(cacheKey, key -> {
            // ✅ ПЕРЕВІРКА 2: Додаткова перевірка всередині computeIfAbsent
            // (на випадок race condition між першою перевіркою і викликом цієї lambda)
            if (shuttingDown.get()) {
                log.error("❌ Shutdown detected during pool creation - aborting");
                log.error("❌ This should not happen - indicates race condition");
                throw new IllegalStateException("Shutdown in progress - pool creation aborted");
            }

            log.info("🆕 DataSource NOT in cache - creating new pool...");
            return createDataSource(dbUrl, dbUser, dbPassword);
        });

        if (dataSource != null) {
            log.info("✅ DataSource retrieved from cache (pool: {})", getPoolName(dataSource));
            logPoolStats(dataSource);
        }

        return dataSource;
    }

    /**
     * ✅ Створення нового HikariCP DataSource з перевіркою shutdown
     */
    private DataSource createDataSource(String dbUrl, String dbUser, String dbPassword) {
        log.info("┌═════════════════════════════════════════════════");
        log.info("│ 🔧 CREATING NEW HIKARI CONNECTION POOL");
        log.info("└═════════════════════════════════════════════════");

        // ✅ ПЕРЕВІРКА 3: Фінальна перевірка перед створенням pool
        if (shuttingDown.get()) {
            log.error("❌ Cannot create pool - shutdown in progress");
            throw new IllegalStateException("Shutdown detected in createDataSource()");
        }

        long startTime = System.currentTimeMillis();

        try {
            // Примусова реєстрація JDBC драйверів
            registerJdbcDriver(dbUrl);

            // Налаштування HikariCP
            HikariConfig config = new HikariConfig();
            config.setJdbcUrl(dbUrl);
            config.setUsername(dbUser);
            config.setPassword(dbPassword);

            // ✅ Оптимальні налаштування для ETL
            config.setMaximumPoolSize(10);           // Максимум 10 одночасних з'єднань
            config.setMinimumIdle(2);                // Мінімум 2 idle з'єднання
            config.setConnectionTimeout(30000);      // 30 сек на з'єднання
            config.setIdleTimeout(600000);           // 10 хв idle timeout
            config.setMaxLifetime(1800000);          // 30 хв max lifetime
            config.setLeakDetectionThreshold(60000); // 60 сек - detection витоку з'єднань

            // Унікальна назва pool для логування
            String poolName = String.format("ETL-Pool-%d", System.currentTimeMillis());
            config.setPoolName(poolName);

            // Налаштування для кращої діагностики
            config.setRegisterMbeans(true);          // JMX моніторинг

            log.info("📝 Pool configuration:");
            log.info("   Pool name: {}", poolName);
            log.info("   Max pool size: {}", config.getMaximumPoolSize());
            log.info("   Min idle: {}", config.getMinimumIdle());
            log.info("   Connection timeout: {}ms", config.getConnectionTimeout());
            log.info("   Idle timeout: {}ms", config.getIdleTimeout());
            log.info("   Max lifetime: {}ms", config.getMaxLifetime());

            // Створюємо pool
            log.info("⚙️ Initializing HikariDataSource...");
            HikariDataSource dataSource = new HikariDataSource(config);

            // Тестуємо з'єднання
            testConnection(dataSource);

            long duration = System.currentTimeMillis() - startTime;
            log.info("✅ HikariDataSource created successfully in {}ms", duration);
            log.info("═════════════════════════════════════════════════\n");

            return dataSource;

        } catch (Exception e) {
            log.error("❌ Failed to create DataSource for: {}", dbUrl);
            log.error("❌ Error details: {}", e.getMessage());
            throw new RuntimeException("Failed to create DataSource: " + e.getMessage(), e);
        }
    }

    /**
     * ✅ Реєстрація JDBC драйверів
     */
    private void registerJdbcDriver(String dbUrl) {
        try {
            if (dbUrl.startsWith("jdbc:postgresql:")) {
                log.info("🔧 Registering PostgreSQL driver...");
                Class.forName("org.postgresql.Driver");
                log.info("✅ PostgreSQL driver registered");

            } else if (dbUrl.startsWith("jdbc:mysql:")) {
                log.info("🔧 Registering MySQL driver...");
                Class.forName("com.mysql.cj.jdbc.Driver");
                log.info("✅ MySQL driver registered");

            } else if (dbUrl.startsWith("jdbc:sqlite:")) {
                log.info("🔧 Registering SQLite driver...");
                Class.forName("org.sqlite.JDBC");
                log.info("✅ SQLite driver registered");

            } else {
                log.warn("⚠️ Unknown database type: {}", dbUrl);
            }
        } catch (ClassNotFoundException e) {
            log.error("❌ JDBC Driver not found for: {}", dbUrl, e);
            throw new RuntimeException("JDBC Driver not found: " + e.getMessage(), e);
        }
    }

    /**
     * ✅ Тестування з'єднання
     */
    private void testConnection(DataSource dataSource) {
        log.info("🧪 Testing connection...");
        try (Connection conn = dataSource.getConnection()) {
            if (conn != null && !conn.isClosed()) {
                String dbProduct = conn.getMetaData().getDatabaseProductName();
                String dbVersion = conn.getMetaData().getDatabaseProductVersion();
                log.info("✅ Connection test successful:");
                log.info("   Database: {} {}", dbProduct, dbVersion);
            }
        } catch (SQLException e) {
            log.error("❌ Connection test failed", e);
            throw new RuntimeException("Connection test failed: " + e.getMessage(), e);
        }
    }

    /**
     * ✅ Статистика pool (якщо доступна)
     */
    private void logPoolStats(DataSource dataSource) {
        if (dataSource instanceof HikariDataSource hikari) {
            try {
                log.info("📊 Pool stats:");
                log.info("   Active connections: {}", hikari.getHikariPoolMXBean().getActiveConnections());
                log.info("   Idle connections: {}", hikari.getHikariPoolMXBean().getIdleConnections());
                log.info("   Total connections: {}", hikari.getHikariPoolMXBean().getTotalConnections());
                log.info("   Threads awaiting: {}", hikari.getHikariPoolMXBean().getThreadsAwaitingConnection());
            } catch (Exception e) {
                log.debug("Could not retrieve pool stats: {}", e.getMessage());
            }
        }
    }

    /**
     * ✅ Отримання назви pool
     */
    private String getPoolName(DataSource dataSource) {
        if (dataSource instanceof HikariDataSource hikari) {
            return hikari.getPoolName();
        }
        return "unknown";
    }

    /**
     * ✅ Створення ключа для кешу
     */
    private String buildCacheKey(String dbUrl, String dbUser) {
        return dbUrl + "|" + dbUser;
    }

    /**
     * ✅ Отримати кількість активних pools (для діагностики)
     */
    public int getActivePools() {
        return dataSourceCache.size();
    }

    /**
     * ✅ Отримати інформацію про всі pools (для діагностики)
     */
    public void logAllPools() {
        log.info("═════════════════════════════════════════════════");
        log.info("📊 DATASOURCE CACHE STATUS");
        log.info("═════════════════════════════════════════════════");
        log.info("Total pools: {}", dataSourceCache.size());
        log.info("Shutdown state: {}", shuttingDown.get() ? "SHUTTING DOWN ⚠️" : "ACTIVE ✅");

        if (dataSourceCache.isEmpty()) {
            log.info("No pools in cache");
        } else {
            dataSourceCache.forEach((key, dataSource) -> {
                log.info("───────────────────────────────────────────────");
                log.info("Key: {}", key);
                log.info("Pool name: {}", getPoolName(dataSource));
                logPoolStats(dataSource);
            });
        }
        log.info("═════════════════════════════════════════════════\n");
    }

    /**
     * ✅ Закрити всі pools при shutdown додатку
     *
     * Порядок виконання:
     * 1. BatchJobShutdownManager (@Order(1)) - зупиняє batch jobs
     * 2. DataSourceManager (@Order(2)) - закриває pools (ЦЕЙ МЕТОД)
     */
    @PreDestroy
    public void closeAllPools() {
        // ✅ КРОК 1: Встановлюємо прапорець shutdown
        // Це заблокує всі спроби створити нові pools
        shuttingDown.set(true);
        log.warn("🛑 DataSourceManager marked as SHUTTING DOWN");
        log.warn("🛑 No new connection pools will be created from this point");

        // ✅ КРОК 2: Закриваємо існуючі pools
        log.info("═════════════════════════════════════════════════");
        log.info("🛑 CLOSING ALL CONNECTION POOLS");
        log.info("═════════════════════════════════════════════════");
        log.info("Total pools to close: {}", dataSourceCache.size());

        if (dataSourceCache.isEmpty()) {
            log.info("ℹ️ No pools to close (cache is empty)");
        } else {
            int closed = 0;
            int failed = 0;

            for (var entry : dataSourceCache.entrySet()) {
                String key = entry.getKey();
                DataSource dataSource = entry.getValue();

                try {
                    if (dataSource instanceof HikariDataSource hikari) {
                        String poolName = hikari.getPoolName();

                        log.info("🔒 Closing pool: {}", poolName);
                        log.info("   Key: {}", key);

                        // Логуємо фінальну статистику перед закриттям
                        try {
                            log.info("   Final stats - Active: {}, Idle: {}, Total: {}",
                                    hikari.getHikariPoolMXBean().getActiveConnections(),
                                    hikari.getHikariPoolMXBean().getIdleConnections(),
                                    hikari.getHikariPoolMXBean().getTotalConnections());
                        } catch (Exception e) {
                            log.debug("Could not get final stats: {}", e.getMessage());
                        }

                        // Закриваємо pool
                        hikari.close();
                        closed++;

                        log.info("✅ Pool closed successfully: {}", poolName);
                    }
                } catch (Exception e) {
                    failed++;
                    log.error("❌ Error closing pool for key: {}", key, e);
                }
            }

            log.info("📊 Shutdown summary: {} closed, {} failed", closed, failed);
        }

        // ✅ КРОК 3: Очищаємо cache
        dataSourceCache.clear();
        log.info("🧹 Cache cleared");

        log.info("✅ All connection pools closed");
        log.info("═════════════════════════════════════════════════\n");
    }

    /**
     * ✅ Діагностичний метод: перевірка стану shutdown
     * Корисно для debugging
     */
    public boolean isShuttingDown() {
        return shuttingDown.get();
    }
}