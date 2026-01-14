package com.etl.etltool;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import java.io.File;

@SpringBootApplication
public class EtlToolApplication {

    public static void main(String[] args) {
        // Створюємо папку для бази даних перед запуском Spring
        createStorageDirectory();

        SpringApplication.run(EtlToolApplication.class, args);
    }

    private static void createStorageDirectory() {
        String home = System.getProperty("user.home");
        File configDir = new File(home + File.separator + ".etl");
        if (!configDir.exists()) {
            if (configDir.mkdirs()) {
                System.out.println(">>> Created storage directory: " + configDir.getAbsolutePath());
            }
        }
    }
}