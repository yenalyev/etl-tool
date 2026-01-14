package com.etl.etltool.controller;

import com.etl.etltool.dto.FileItem;
import com.etl.etltool.dto.FileListResponse;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import java.io.File;
import java.util.*;
import java.util.stream.Collectors;

@Slf4j
@RestController
@RequestMapping("/api/files")
@CrossOrigin(origins = "*")
public class FileBrowserController {

    @GetMapping("/list")
    public FileListResponse listFiles(@RequestParam(required = false) String path) {
        // 1. Покращена нормалізація
        String normalizedPath = (path == null || path.trim().isEmpty() || path.equals("undefined") || path.equals("null"))
                ? "" : path.replace("\\", "/");

        log.info("Browsing path: [{}]", normalizedPath);

        try {
            // 2. Якщо корінь - повертаємо диски
            if (normalizedPath.isEmpty()) {
                List<FileItem> roots = Arrays.stream(File.listRoots())
                        .map(f -> FileItem.builder()
                                .name(f.toString())
                                .absolutePath(f.getAbsolutePath().replace("\\", "/"))
                                .isDirectory(true)
                                .build())
                        .collect(Collectors.toList());
                return FileListResponse.builder().currentPath("Мій комп'ютер").items(roots).build();
            }

            File directory = new File(normalizedPath);
            if (!directory.exists()) {
                return FileListResponse.builder().error("Шлях не знайдено: " + normalizedPath).build();
            }

            File[] files = directory.listFiles();
            if (files == null) {
                return FileListResponse.builder()
                        .currentPath(normalizedPath)
                        .parentPath(getParentPath(directory))
                        .error("Доступ обмежено системними правами")
                        .items(new ArrayList<>())
                        .build();
            }

            // 3. Фільтрація та мапінг
            List<FileItem> items = Arrays.stream(files)
                    .filter(f -> !f.isHidden())
                    .filter(f -> f.isDirectory() || f.getName().toLowerCase().endsWith(".json"))
                    .map(f -> FileItem.builder()
                            .name(f.getName())
                            .absolutePath(f.getAbsolutePath().replace("\\", "/"))
                            .isDirectory(f.isDirectory())
                            .build())
                    .sorted(Comparator.comparing(FileItem::isDirectory).reversed()
                            .thenComparing(FileItem::getName))
                    .collect(Collectors.toList());

            return FileListResponse.builder()
                    .currentPath(directory.getAbsolutePath().replace("\\", "/"))
                    .parentPath(getParentPath(directory))
                    .items(items)
                    .build();

        } catch (Exception e) {
            log.error("File browser error", e);
            return FileListResponse.builder().error("Error: " + e.getMessage()).build();
        }
    }

    @GetMapping("/get-email")
    public ResponseEntity<?> getServiceAccountEmail(@RequestParam String path) {
        log.info("Reading email from JSON: [{}]", path);
        try {
            File file = new File(path);
            if (!file.exists()) {
                return ResponseEntity.badRequest().body(Map.of("error", "Файл не знайдено"));
            }

            ObjectMapper mapper = new ObjectMapper();
            // Читаємо JSON файл як дерево
            JsonNode rootNode = mapper.readTree(file);

            // Шукаємо поле client_email
            JsonNode emailNode = rootNode.get("client_email");

            if (emailNode != null && !emailNode.asText().isEmpty()) {
                return ResponseEntity.ok(Map.of("email", emailNode.asText()));
            } else {
                return ResponseEntity.badRequest().body(Map.of("error", "Поле client_email не знайдено в JSON"));
            }
        } catch (Exception e) {
            log.error("Error reading JSON file", e);
            return ResponseEntity.internalServerError().body(Map.of("error", "Помилка при зчитуванні файлу: " + e.getMessage()));
        }
    }

    private String getParentPath(File file) {
        File parent = file.getParentFile();
        if (parent == null) return "";
        return parent.getAbsolutePath().replace("\\", "/");
    }
}