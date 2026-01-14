package com.etl.etltool.dto;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class ConnectionRequest {
    private String jsonPath;
    private String spreadsheetId;
}
