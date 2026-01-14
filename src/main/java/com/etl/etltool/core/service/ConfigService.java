package com.etl.etltool.core.service;

import com.etl.etltool.core.entity.AppConfig;
import com.etl.etltool.core.repository.AppConfigRepository;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

@Service
@RequiredArgsConstructor
public class ConfigService {

    private final AppConfigRepository repository;

    public AppConfig getConfig() {
        return repository.findFirstByOrderByIdAsc()
                .orElse(AppConfig.builder()
                        .defaultChunkSize(1000)
                        .autoStartEnabled(false)
                        .build());
    }

    @Transactional
    public AppConfig saveConfig(AppConfig newConfig) {
        return repository.findFirstByOrderByIdAsc()
                .map(existing -> updateExistingConfig(existing, newConfig))
                .orElseGet(() -> repository.save(newConfig));
    }

    private AppConfig updateExistingConfig(AppConfig existing, AppConfig incoming) {
        existing.setGoogleSheetId(incoming.getGoogleSheetId());
        existing.setServiceAccountKeyPath(incoming.getServiceAccountKeyPath());
        existing.setSheetName(incoming.getSheetName());
        existing.setTargetDbUrl(incoming.getTargetDbUrl());
        existing.setTargetDbUser(incoming.getTargetDbUser());

        // оновлюємо пароль ТІЛЬКИ якщо з форми прийшло нове значення.
        // Якщо поле пусте (користувач не вводив новий пароль), залишаємо старий, який вже є в базі.
        if (incoming.getTargetDbPassword() != null && !incoming.getTargetDbPassword().isEmpty()) {
            existing.setTargetDbPassword(incoming.getTargetDbPassword());
        }

        existing.setDefaultChunkSize(incoming.getDefaultChunkSize());
        existing.setCronExpression(incoming.getCronExpression());
        existing.setAutoStartEnabled(incoming.isAutoStartEnabled());
        existing.setCreateNewTable(incoming.isCreateNewTable());
        existing.setTargetTableName(incoming.getTargetTableName());
        existing.setFieldMappingJson(incoming.getFieldMappingJson());

        return repository.save(existing);
    }
}