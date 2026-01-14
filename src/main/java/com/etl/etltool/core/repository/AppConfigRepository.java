package com.etl.etltool.core.repository;

import com.etl.etltool.core.entity.AppConfig;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import java.util.Optional;

@Repository
public interface AppConfigRepository extends JpaRepository<AppConfig, Long> {
    // Метод для отримання першого (єдиного) запису налаштувань
    Optional<AppConfig> findFirstByOrderByIdAsc();
}