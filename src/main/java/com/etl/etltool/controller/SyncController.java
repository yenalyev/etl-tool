package com.etl.etltool.controller;

import com.etl.etltool.core.entity.AppConfig;
import com.etl.etltool.core.service.ConfigService;
import com.etl.etltool.core.service.SyncService;
import com.etl.etltool.dto.SyncResponse;
import lombok.RequiredArgsConstructor;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.ResponseBody;

import java.util.List;

@Controller
@RequiredArgsConstructor
public class SyncController {

    private final ConfigService configService;
    private final SyncService syncService;

    @GetMapping("/sync")
    public String syncPage(Model model) {
        model.addAttribute("config", configService.getConfig());
        model.addAttribute("content", "sync :: content"); // Твій фрагмент
        return "layout";
    }

    // Новий метод для AJAX-запиту
    @PostMapping("/api/sync/run")
    @ResponseBody
    public ResponseEntity<SyncResponse> runSyncApi() {
        try {
            AppConfig config = configService.getConfig();

            // Викликаємо сервіс (який ми зараз допишемо)
            int count = syncService.runSync(config);

            return ResponseEntity.ok(SyncResponse.builder()
                    .success(true)
                    .addedCount(count)
                    .message("Синхронізація завершена")
                    .logs(List.of("З'єднання встановлено", "Дані зчитано успішно", "Таблицю " + config.getTargetTableName() + " оновлено"))
                    .build());
        } catch (Exception e) {
            return ResponseEntity.status(500).body(SyncResponse.builder()
                    .success(false)
                    .message("Помилка: " + e.getMessage())
                    .build());
        }
    }
}