package com.etl.etltool.controller;

import com.etl.etltool.core.entity.AppConfig;
import com.etl.etltool.core.service.ConfigService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.ModelAttribute;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestParam;

import java.util.List;

@Slf4j
@Controller
@RequiredArgsConstructor
public class IndexController {

    private final ConfigService configService;

    @GetMapping("/")
    public String showConfig(Model model) {
        // 1. Отримуємо або створюємо конфігурацію
        AppConfig config = configService.getConfig();
        model.addAttribute("config", config);

        // 2. Вказуємо, який фрагмент вставити в layout
        model.addAttribute("content", "index :: content");

        // 3. ПОВЕРТАЄМО LAYOUT (не index!)
        return "layout";
    }

    @PostMapping("/settings/save")
    public String saveSettings(@ModelAttribute AppConfig newConfig) {
        log.info("Received config: {}", newConfig);
        configService.saveConfig(newConfig);
        // Редірект залишається на "/", що знову викличе метод index()
        return "redirect:/?success";
    }

}