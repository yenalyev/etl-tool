package com.etl.etltool.core.service;

import com.etl.etltool.core.entity.SyncTask;
import com.etl.etltool.core.repository.SyncTaskRepository;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import java.util.List;

@Service
@RequiredArgsConstructor
public class TaskService {

    private final SyncTaskRepository taskRepository;

    public List<SyncTask> getAllTasks() {
        return taskRepository.findAll();
    }

    public SyncTask getTask(Long id) {
        return taskRepository.findById(id).orElseThrow(() -> new RuntimeException("Task not found"));
    }

    @Transactional
    public SyncTask saveTask(SyncTask task) {
        return taskRepository.save(task);
    }

    @Transactional
    public void deleteTask(Long id) {
        taskRepository.deleteById(id);
    }

    public List<SyncTask> getAllActiveTasks() {
        return taskRepository.findByEnabledTrue();
    }
}