package com.etl.etltool.controller;


import com.etl.etltool.core.service.DatabaseService;
import com.etl.etltool.core.service.google.GoogleSheetsService;
import lombok.RequiredArgsConstructor;
import org.springframework.web.bind.annotation.*;

import java.util.List;

@RestController
@RequestMapping("/api/config")
@RequiredArgsConstructor
public class ConfigApiController {

    private final GoogleSheetsService googleSheetsService;
    // Додай у ConfigApiController
    private final DatabaseService databaseService;

    // 1. Отримання заголовків з Google Sheets для мапінгу
    @GetMapping("/google-headers")
    public List<String> getHeaders(@RequestParam String sheetId, @RequestParam String keyPath) throws Exception {
        // Ми викликаємо сервіс, який поверне лише перший рядок (заголовки)
        return googleSheetsService.fetchFirstRow(sheetId, keyPath);
    }

    @GetMapping("/database/columns")
    public List<String> getTableColumns(@RequestParam String tableName,
                                        @RequestParam String url,
                                        @RequestParam String user,
                                        @RequestParam String password) {
        return databaseService.getColumnNames(url, user, password, tableName);
    }
}
