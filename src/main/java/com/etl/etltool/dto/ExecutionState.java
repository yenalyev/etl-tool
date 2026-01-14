package com.etl.etltool.dto;

import lombok.Data;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

@Data
public class ExecutionState {
    private volatile boolean running;
    private volatile boolean finished;
    private volatile boolean success;
    private volatile long rowsProcessed;
    private LocalDateTime startTime;
    private LocalDateTime endTime;

    // Синхронізований список для безпечного запису з різних потоків
    private final List<String> logs = Collections.synchronizedList(new ArrayList<>());

    public void addLog(String message) {
        this.logs.add(message);
    }
}