package com.etl.etltool.core.service;

import com.etl.etltool.core.entity.AppConfig;
import com.etl.etltool.core.repository.AppConfigRepository;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

/**
 * Сервіс для керування глобальними налаштуваннями ETL-системи.
 * Оскільки це локальний додаток, ми оперуємо одним екземпляром конфігурації (Singleton-style).
 */
@Service
@RequiredArgsConstructor
public class ConfigService {

    private final AppConfigRepository repository;

    /**
     * Отримує поточну конфігурацію або повертає дефолтну, якщо база порожня.
     */
    public AppConfig getConfig() {
        return repository.findFirstByOrderByIdAsc()
                .orElse(AppConfig.builder()
                        .defaultChunkSize(1000)
                        .autoStartEnabled(false)
                        .build());
    }

    /**
     * Зберігає або оновлює конфігурацію.
     */
    @Transactional
    public AppConfig saveConfig(AppConfig newConfig) {
        return repository.findFirstByOrderByIdAsc()
                .map(existing -> updateExistingConfig(existing, newConfig))
                .orElseGet(() -> repository.save(newConfig));
    }

    /**
     * Внутрішній метод для мапінгу полів при оновленні.
     */
    private AppConfig updateExistingConfig(AppConfig existing, AppConfig incoming) {
        existing.setGoogleSheetId(incoming.getGoogleSheetId());
        existing.setServiceAccountKeyPath(incoming.getServiceAccountKeyPath());
        existing.setTargetDbUrl(incoming.getTargetDbUrl());
        existing.setTargetDbUser(incoming.getTargetDbUser());
        existing.setTargetDbPassword(incoming.getTargetDbPassword());
        existing.setDefaultChunkSize(incoming.getDefaultChunkSize());
        existing.setCronExpression(incoming.getCronExpression());
        existing.setAutoStartEnabled(incoming.isAutoStartEnabled());
        existing.setCreateNewTable(incoming.isCreateNewTable());
        existing.setTargetTableName(incoming.getTargetTableName());
        existing.setFieldMappingJson(incoming.getFieldMappingJson());

        return repository.save(existing);
    }
}