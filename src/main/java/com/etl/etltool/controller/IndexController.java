package com.etl.etltool.controller;

import com.etl.etltool.core.entity.AppConfig;
import com.etl.etltool.core.entity.SyncTask;
import com.etl.etltool.core.service.ConfigService;
import com.etl.etltool.core.service.TaskExecutionManager;
import com.etl.etltool.core.service.TaskService;
import com.etl.etltool.core.service.SyncService; // Для запуску
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.*;

@Slf4j
@Controller
@RequiredArgsConstructor
public class IndexController {

    private final ConfigService configService;
    private final TaskService taskService;
    private final SyncService syncService;
    private final TaskExecutionManager executionManager;

    // --- DASHBOARD (Головна сторінка) ---
    @GetMapping("/")
    public String dashboard(Model model) {
        model.addAttribute("tasks", taskService.getAllTasks());
        model.addAttribute("globalConfig", configService.getConfig());
        model.addAttribute("content", "index :: content");
        return "layout";
    }

    // --- GLOBAL SETTINGS (Налаштування БД) ---
    @GetMapping("/settings")
    public String globalSettings(Model model) {
        model.addAttribute("config", configService.getConfig());
        model.addAttribute("content", "settings :: content");
        return "layout";
    }

    @PostMapping("/settings/save")
    public String saveGlobalSettings(@ModelAttribute AppConfig config) {
        configService.saveConfig(config);
        return "redirect:/?saved";
    }

    // --- TASK MANAGEMENT (Робота з задачами) ---

    // Створення нової задачі
    @GetMapping("/task/new")
    public String newTask(Model model) {
        AppConfig globalConfig = configService.getConfig();

        // Перевіряємо, чи налаштована БД перед створенням задачі
        if (globalConfig.getTargetDbUrl() == null || globalConfig.getTargetDbUrl().isEmpty()) {
            return "redirect:/settings?error=setup_db_first";
        }

        model.addAttribute("task", new SyncTask());
        model.addAttribute("globalConfig", globalConfig); // Потрібно для JS (API calls)
        model.addAttribute("content", "task-form :: content");
        return "layout";
    }

    // Редагування існуючої задачі
    @GetMapping("/task/edit/{id}")
    public String editTask(@PathVariable Long id, Model model) {
        SyncTask task = taskService.getTask(id);
        AppConfig globalConfig = configService.getConfig();

        model.addAttribute("task", task);
        model.addAttribute("globalConfig", globalConfig);
        model.addAttribute("content", "task-form :: content");
        return "layout";
    }

    // Збереження задачі
    @PostMapping("/task/save")
    public String saveTask(@ModelAttribute SyncTask task) {
        taskService.saveTask(task);
        return "redirect:/?taskSaved";
    }

    // Видалення задачі
    @GetMapping("/task/delete/{id}")
    public String deleteTask(@PathVariable Long id) {
        taskService.deleteTask(id);
        return "redirect:/?deleted";
    }

//    // Запуск задачі вручну
//    @GetMapping("/task/run/{id}")
//    public String runTaskPage(@PathVariable Long id, Model model) {
//        // 1. Ініціалізуємо і запускаємо задачу асинхронно
//        // (Це не заблокує потік, бо метод сервісу @Async)
//        try {
//            executionManager.initTask(id); // Скидаємо старі логи
//            syncService.runTaskAsync(id);  // "Fire and forget"
//        } catch (Exception e) {
//            return "redirect:/?error=" + e.getMessage();
//        }
//
//        // 2. Передаємо ID на фронт для підключення SSE
//        model.addAttribute("taskId", id);
//
//        // 3. Віддаємо сторінку терміналу
//        return "execution";
//    }
}