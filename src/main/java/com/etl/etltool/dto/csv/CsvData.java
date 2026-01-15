package com.etl.etltool.dto.csv;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.List;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class CsvData {
    private List<String> headers;
    private List<List<Object>> rows;
    private String filePath;
    private long fileSize;
}
