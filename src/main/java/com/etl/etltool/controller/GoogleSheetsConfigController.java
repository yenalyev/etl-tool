package com.etl.etltool.controller;

import com.etl.etltool.core.service.google.GoogleSheetsConfigService;
import com.etl.etltool.dto.ConnectionRequest;
import com.etl.etltool.dto.ConnectionResponse;
import lombok.RequiredArgsConstructor;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

@RestController
@RequestMapping("/api/config/google-sheets")
@RequiredArgsConstructor
@CrossOrigin(origins = "*")
public class GoogleSheetsConfigController {

    private final GoogleSheetsConfigService googleSheetsConfigService;

    @PostMapping("/verify")
    public ResponseEntity<ConnectionResponse> verify(@RequestBody ConnectionRequest request) {
        var result = googleSheetsConfigService.checkConnection(
                request.getJsonPath(),
                request.getSpreadsheetId()
        );

        return ResponseEntity.ok(new ConnectionResponse(result.isSuccess(), result.getMessage()));
    }
}
