package com.etl.etltool.dto;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.List;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class FileListResponse {
    private String currentPath;
    private String parentPath;
    private String error;
    private List<FileItem> items;
}
