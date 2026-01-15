package com.etl.etltool.core.repository;

import com.etl.etltool.core.entity.TaskExecutionHistory;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;

import java.time.LocalDateTime;
import java.util.List;
import java.util.Optional;

@Repository
public interface TaskExecutionHistoryRepository extends JpaRepository<TaskExecutionHistory, Long> {

    /**
     * Отримати історію виконань для конкретної задачі
     * Сортування: від найновіших до найстаріших
     */
    List<TaskExecutionHistory> findByTaskIdOrderByStartTimeDesc(Long taskId);

    /**
     * Отримати N останніх виконань для задачі
     */
    @Query("SELECT h FROM TaskExecutionHistory h WHERE h.task.id = :taskId ORDER BY h.startTime DESC")
    List<TaskExecutionHistory> findTopNByTaskId(@Param("taskId") Long taskId,
                                                org.springframework.data.domain.Pageable pageable);

    /**
     * Отримати останнє успішне виконання для задачі
     * Потрібно для порівняння даних
     */
    Optional<TaskExecutionHistory> findFirstByTaskIdAndStatusOrderByStartTimeDesc(
            Long taskId, String status);

    /**
     * Отримати всі виконання за період
     */
    List<TaskExecutionHistory> findByStartTimeBetween(
            LocalDateTime start, LocalDateTime end);

    /**
     * Підрахувати кількість успішних виконань для задачі
     */
    @Query("SELECT COUNT(h) FROM TaskExecutionHistory h WHERE h.task.id = :taskId AND h.status = 'SUCCESS'")
    long countSuccessfulExecutions(@Param("taskId") Long taskId);

    /**
     * Підрахувати кількість невдалих виконань для задачі
     */
    @Query("SELECT COUNT(h) FROM TaskExecutionHistory h WHERE h.task.id = :taskId AND h.status = 'FAILED'")
    long countFailedExecutions(@Param("taskId") Long taskId);

    /**
     * Видалити історію старіше N днів
     * Для автоматичного очищення
     */
    void deleteByStartTimeBefore(LocalDateTime threshold);
}