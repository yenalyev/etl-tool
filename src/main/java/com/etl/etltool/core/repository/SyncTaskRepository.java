package com.etl.etltool.core.repository;

import com.etl.etltool.core.entity.SyncTask;
import org.springframework.data.jpa.repository.JpaRepository;
import java.util.List;

public interface SyncTaskRepository extends JpaRepository<SyncTask, Long> {
    List<SyncTask> findByEnabledTrue();
}