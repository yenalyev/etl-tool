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
        existing.setServiceAccountKeyPath(incoming.getServiceAccountKeyPath());

        existing.setTargetDbUrl(incoming.getTargetDbUrl());
        existing.setTargetDbUser(incoming.getTargetDbUser());

        // Зберігаємо логіку перевірки пароля
        if (incoming.getTargetDbPassword() != null && !incoming.getTargetDbPassword().isEmpty()) {
            existing.setTargetDbPassword(incoming.getTargetDbPassword());
        }

        existing.setDefaultChunkSize(incoming.getDefaultChunkSize());
        existing.setCronExpression(incoming.getCronExpression());
        existing.setAutoStartEnabled(incoming.isAutoStartEnabled());

        // Поля, пов'язані з таблицями, видалені
        return repository.save(existing);
    }
}