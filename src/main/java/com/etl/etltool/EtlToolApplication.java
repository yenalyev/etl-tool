package com.etl.etltool;

import com.etl.etltool.config.DataSourceManager;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.ConfigurableApplicationContext;

import java.io.File;

@SpringBootApplication
public class EtlToolApplication {

    public static void main(String[] args) {
        // Створюємо папку для бази даних перед запуском Spring
        createStorageDirectory();

        // ✅ Зберігаємо контекст Spring для доступу до beans
        ConfigurableApplicationContext context = SpringApplication.run(EtlToolApplication.class, args);

        // ✅ Реєструємо shutdown hook для graceful shutdown
        registerShutdownHook(context);
    }

    /**
     * Створення директорії для SQLite бази даних
     */
    private static void createStorageDirectory() {
        String home = System.getProperty("user.home");
        File configDir = new File(home + File.separator + ".etl");

        if (!configDir.exists()) {
            if (configDir.mkdirs()) {
                System.out.println(">>> Created storage directory: " + configDir.getAbsolutePath());
            } else {
                System.err.println(">>> Failed to create storage directory: " + configDir.getAbsolutePath());
            }
        } else {
            System.out.println(">>> Storage directory exists: " + configDir.getAbsolutePath());
        }
    }

    /**
     * ✅ Graceful Shutdown Hook
     * Викликається при:
     * - Ctrl+C
     * - kill <PID>
     * - System.exit()
     * - Зупинка через IDE
     */
    private static void registerShutdownHook(ConfigurableApplicationContext context) {
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            System.out.println("\n╔═══════════════════════════════════════════════════╗");
            System.out.println("║  🛑 SHUTTING DOWN ETL TOOL APPLICATION          ║");
            System.out.println("╚═══════════════════════════════════════════════════╝");

            try {
                // ✅ Закриваємо всі connection pools
                DataSourceManager dataSourceManager = context.getBean(DataSourceManager.class);

                System.out.println(">>> Closing database connection pools...");
                dataSourceManager.closeAllPools();
                System.out.println(">>> ✅ All connection pools closed");

            } catch (Exception e) {
                System.err.println(">>> ❌ Error during shutdown: " + e.getMessage());
                e.printStackTrace();
            }

            System.out.println("\n╔═══════════════════════════════════════════════════╗");
            System.out.println("║  ✅ ETL TOOL APPLICATION SHUTDOWN COMPLETE       ║");
            System.out.println("╚═══════════════════════════════════════════════════╝\n");
        }, "etl-shutdown-hook"));

        System.out.println(">>> Shutdown hook registered");
    }
}