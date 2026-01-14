package com.etl.etltool.dto;

import lombok.Builder;
import lombok.Data;
import java.util.List;

@Data
@Builder
public class SyncResponse {
    private boolean success;
    private String message;
    private int addedCount;
    private List<String> logs; // Список кроків для виведення в консоль
}