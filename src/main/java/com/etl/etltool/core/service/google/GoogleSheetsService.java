package com.etl.etltool.core.service.google;

import com.google.api.client.googleapis.javanet.GoogleNetHttpTransport;
import com.google.api.client.json.gson.GsonFactory;
import com.google.api.services.sheets.v4.Sheets;
import com.google.api.services.sheets.v4.SheetsScopes;
import com.google.api.services.sheets.v4.model.Spreadsheet;
import com.google.api.services.sheets.v4.model.ValueRange;
import com.google.auth.http.HttpCredentialsAdapter;
import com.google.auth.oauth2.GoogleCredentials;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.io.FileInputStream;
import java.io.IOException;
import java.security.GeneralSecurityException;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

@Slf4j
@Service
public class GoogleSheetsService {

    private static final String APPLICATION_NAME = "ETL-Tool";
    private static final GsonFactory JSON_FACTORY = GsonFactory.getDefaultInstance();
    private static final String DEFAULT_RANGE = "A1:Z1000";

    // Semaphore для обмеження одночасних запитів
    private final Semaphore apiRateLimiter = new Semaphore(5); // Макс 5 одночасних запитів

    /**
     * Читає дані з Google Sheets з контролемRate Limiting
     */
    public List<List<Object>> readSheet(String spreadsheetId, String sheetName, String jsonPath) throws Exception {
        log.info("Reading Google Sheet: {} (sheet: {})",
                spreadsheetId,
                sheetName != null ? sheetName : "default");

        // КРОК 1: Спроба отримати "дозвіл" на виконання запиту
        // tryAcquire(30, TimeUnit.SECONDS) означає:
        // - Якщо є вільний слот (1 з 5) - отримуємо його миттєво
        // - Якщо всі 5 слотів зайняті - чекаємо максимум 30 секунд
        // - Якщо за 30 сек не звільнився жоден слот - повертає false
        boolean acquired = apiRateLimiter.tryAcquire(30, TimeUnit.SECONDS);

        if (!acquired) {
            // Таймаут - не змогли отримати дозвіл за 30 секунд
            log.error("Failed to acquire API rate limiter permit within 30 seconds for sheet: {}", spreadsheetId);
            throw new RuntimeException("Google API rate limiter timeout - too many concurrent requests");
        }

        try {
            // КРОК 2: Ми отримали дозвіл - можемо виконувати запит
            log.debug("Acquired API rate limiter permit. Available permits: {}",
                    apiRateLimiter.availablePermits());

            Sheets service = getSheetsService(jsonPath);
            String range = buildRange(sheetName, DEFAULT_RANGE);

            log.debug("Fetching range: {}", range);

            // Виконуємо запит до Google API
            ValueRange response = service.spreadsheets().values()
                    .get(spreadsheetId, range)
                    .execute();

            List<List<Object>> values = response.getValues();

            if (values == null || values.isEmpty()) {
                log.warn("No data found in spreadsheet");
                return Collections.emptyList();
            }

            log.info("Successfully read {} rows from Google Sheet", values.size());
            return values;

        } finally {
            // КРОК 3: ЗАВЖДИ звільняємо дозвіл
            // finally гарантує виконання навіть при exception
            // Це критично важливо! Без release() семафор "застряне"
            apiRateLimiter.release();
            log.debug("Released API rate limiter permit. Available permits: {}",
                    apiRateLimiter.availablePermits());
        }
    }

    /**
     * Отримує список всіх аркушів (sheets) у spreadsheet.
     * Використовується для заповнення dropdown у UI.
     */
    public List<String> getSheetNames(String spreadsheetId, String jsonPath) throws Exception {
        log.info("Fetching sheet names for spreadsheet: {}", spreadsheetId);

        Sheets service = getSheetsService(jsonPath);

        // Отримуємо метадані spreadsheet
        Spreadsheet spreadsheet = service.spreadsheets()
                .get(spreadsheetId)
                .setFields("sheets.properties.title") // Оптимізація - тягнемо тільки назви
                .execute();

        if (spreadsheet.getSheets() == null || spreadsheet.getSheets().isEmpty()) {
            log.warn("No sheets found in spreadsheet: {}", spreadsheetId);
            return Collections.emptyList();
        }

        // Витягуємо назви всіх аркушів
        List<String> sheetNames = spreadsheet.getSheets().stream()
                .map(sheet -> sheet.getProperties().getTitle())
                .collect(Collectors.toList());

        log.info("Found {} sheets: {}", sheetNames.size(), sheetNames);
        return sheetNames;
    }

    /**
     * Зчитує перший рядок (заголовки) з вказаного аркуша.
     * Використовується для field mapping у UI.
     */
    public List<String> fetchFirstRow(String spreadsheetId, String jsonPath, String sheetName) throws Exception {
        log.info("Fetching first row from spreadsheet: {} (sheet: {})",
                spreadsheetId,
                sheetName != null ? sheetName : "default");

        Sheets service = getSheetsService(jsonPath);

        // Формуємо діапазон для першого рядка
        String range = buildRange(sheetName, "1:1");

        log.debug("Fetching headers from range: {}", range);

        ValueRange response = service.spreadsheets().values()
                .get(spreadsheetId, range)
                .execute();

        List<List<Object>> values = response.getValues();

        if (values == null || values.isEmpty()) {
            log.warn("No headers found in first row");
            return Collections.emptyList();
        }

        // Перетворюємо об'єкти у рядки
        List<String> headers = values.get(0).stream()
                .map(Object::toString)
                .collect(Collectors.toList());

        log.info("Found {} headers: {}", headers.size(), headers);
        return headers;
    }

    /**
     * Перевантажений метод для зворотної сумісності.
     */
    public List<String> fetchFirstRow(String spreadsheetId, String jsonPath) throws Exception {
        return fetchFirstRow(spreadsheetId, jsonPath, null);
    }

    /**
     * Створює та повертає авторизований клієнт Google Sheets API.
     */
    private Sheets getSheetsService(String jsonPath) throws IOException, GeneralSecurityException {
        log.debug("Initializing Google Sheets service with credentials from: {}", jsonPath);

        GoogleCredentials credentials = GoogleCredentials
                .fromStream(new FileInputStream(jsonPath))
                .createScoped(Collections.singleton(SheetsScopes.SPREADSHEETS_READONLY));

        return new Sheets.Builder(
                GoogleNetHttpTransport.newTrustedTransport(),
                JSON_FACTORY,
                new HttpCredentialsAdapter(credentials))
                .setApplicationName(APPLICATION_NAME)
                .build();
    }

    /**
     * Будує діапазон (range) для Google Sheets API з урахуванням назви аркуша.
     */
    private String buildRange(String sheetName, String cells) {
        if (sheetName == null || sheetName.trim().isEmpty()) {
            return cells;
        }

        if (needsQuoting(sheetName)) {
            String escapedName = sheetName.replace("'", "''");
            return String.format("'%s'!%s", escapedName, cells);
        }

        return String.format("%s!%s", sheetName, cells);
    }

    private String buildRange(String sheetName) {
        return buildRange(sheetName, DEFAULT_RANGE);
    }

    private boolean needsQuoting(String sheetName) {
        if (sheetName == null || sheetName.isEmpty()) {
            return false;
        }
        return sheetName.contains(" ")
                || sheetName.contains("'")
                || sheetName.contains("!")
                || sheetName.contains(":")
                || sheetName.contains(";")
                || sheetName.contains(",");
    }
}