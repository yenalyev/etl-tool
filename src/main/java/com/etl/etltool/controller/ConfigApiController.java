package com.etl.etltool.controller;

import com.etl.etltool.core.service.DatabaseService;
import com.etl.etltool.core.service.google.GoogleSheetsService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.List;
import java.util.Map;

@Slf4j
@RestController
@RequestMapping("/api/config")
@RequiredArgsConstructor
@CrossOrigin(origins = "*") // TODO: обмежити в production
public class ConfigApiController {

    private final GoogleSheetsService googleSheetsService;
    private final DatabaseService databaseService;

    // ✨ НОВИЙ ENDPOINT: Отримання списку аркушів
    @GetMapping("/google-sheets/list")
    public ResponseEntity<?> getSheetsList(
            @RequestParam String sheetId,
            @RequestParam String keyPath) {
        try {
            log.info("Fetching sheets list for spreadsheet: {}", sheetId);

            List<String> sheets = googleSheetsService.getSheetNames(sheetId, keyPath);

            return ResponseEntity.ok(Map.of(
                    "success", true,
                    "sheets", sheets
            ));
        } catch (Exception e) {
            log.error("Error fetching sheets list", e);
            return ResponseEntity.status(500).body(Map.of(
                    "success", false,
                    "error", "Помилка отримання списку аркушів: " + e.getMessage()
            ));
        }
    }

    // ✨ ОНОВЛЕНИЙ ENDPOINT: Додали параметр sheetName
    @GetMapping("/google-headers")
    public ResponseEntity<?> getHeaders(
            @RequestParam String sheetId,
            @RequestParam String keyPath,
            @RequestParam(required = false) String sheetName) {
        try {
            log.info("Fetching headers from spreadsheet: {} (sheet: {})",
                    sheetId,
                    sheetName != null ? sheetName : "default");

            List<String> headers = googleSheetsService.fetchFirstRow(sheetId, keyPath, sheetName);

            return ResponseEntity.ok(headers);
        } catch (Exception e) {
            log.error("Error fetching headers", e);
            return ResponseEntity.status(500).body(Map.of(
                    "error", "Помилка отримання заголовків: " + e.getMessage()
            ));
        }
    }

    @GetMapping("/database/columns")
    public ResponseEntity<?> getTableColumns(
            @RequestParam String tableName,
            @RequestParam String url,
            @RequestParam String user,
            @RequestParam String password) {
        try {
            log.info("Fetching columns for table: {}", tableName);

            List<String> columns = databaseService.getColumnNames(url, user, password, tableName);

            return ResponseEntity.ok(columns);
        } catch (Exception e) {
            log.error("Error fetching table columns", e);
            return ResponseEntity.status(500).body(Map.of(
                    "error", "Помилка отримання колонок: " + e.getMessage()
            ));
        }
    }
}