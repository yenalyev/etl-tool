package com.etl.etltool.dto.csv;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class CsvStorageStats {
    private Long taskId;
    private int totalFiles;
    private long totalSizeBytes;
    private String latestFileName;
    private String directoryPath;

    public String getFormattedSize() {
        if (totalSizeBytes < 1024) {
            return totalSizeBytes + " B";
        } else if (totalSizeBytes < 1024 * 1024) {
            return String.format("%.2f KB", totalSizeBytes / 1024.0);
        } else {
            return String.format("%.2f MB", totalSizeBytes / (1024.0 * 1024.0));
        }
    }
}
