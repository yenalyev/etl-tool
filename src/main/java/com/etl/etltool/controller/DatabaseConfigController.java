package com.etl.etltool.controller;

import com.etl.etltool.core.service.DatabaseConfigService;
import com.etl.etltool.dto.DbVerifyRequest;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.sql.*;
import java.util.List;

@Slf4j
@RestController
@RequestMapping("/api/config/database")
@RequiredArgsConstructor
@CrossOrigin(origins = "*")
public class DatabaseConfigController {

    private final DatabaseConfigService dbService;

    @PostMapping("/verify")
    public ResponseEntity<?> verify(@RequestBody DbVerifyRequest request) {
        var result = dbService.testConnection(
                request.getUrl(),
                request.getUser(),
                request.getPassword()
        );
        return ResponseEntity.ok(result);
    }

    @PostMapping("/tables")
    public ResponseEntity<List<String>> getTables(@RequestBody DbVerifyRequest request) {
        List<String> tables = dbService.getExistingTables(request.getUrl(), request.getUser(), request.getPassword());
        return ResponseEntity.ok(tables);
    }
}
