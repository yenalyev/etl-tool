package com.etl.etltool.dto;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class DbVerifyRequest {
    private String url;
    private String user;
    private String password;
}
