package com.etl.etltool.dto;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.LocalDateTime;

/**
 * DTO для статистики виконань
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class TaskExecutionStats {
    private Long taskId;
    private long totalExecutions;
    private long successfulExecutions;
    private long failedExecutions;
    private LocalDateTime lastExecutionTime;
    private String lastExecutionStatus;

    public double getSuccessRate() {
        if (totalExecutions == 0) return 0.0;
        return (successfulExecutions * 100.0) / totalExecutions;
    }
}
