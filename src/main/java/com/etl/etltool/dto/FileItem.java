package com.etl.etltool.dto;


import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class FileItem {
    private String name;
    private String absolutePath;

    @JsonProperty("isDirectory") // Це КРИТИЧНО для JavaScript
    private boolean isDirectory;
}
