package com.etl.etltool.core.service.google;

import com.etl.etltool.core.entity.AppConfig;
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
import java.util.stream.Collectors;

@Slf4j
@Service
public class GoogleSheetsService {

    private static final String APPLICATION_NAME = "ETL-Tool";
    private static final GsonFactory JSON_FACTORY = GsonFactory.getDefaultInstance();
    private static final String DEFAULT_RANGE = "A1:Z1000"; // Можна винести в properties

    /**
     * Основний метод для читання даних з Google Sheets.
     * Викликається SyncService для ETL процесу.
     */
    public List<List<Object>> readSheet(AppConfig config) throws Exception {
        log.info("Reading Google Sheet: {} (sheet: {})",
                config.getGoogleSheetId(),
                config.getSheetName() != null ? config.getSheetName() : "default");

        Sheets service = getSheetsService(config.getServiceAccountKeyPath());

        // Формуємо діапазон з урахуванням назви аркуша
        String range = buildRange(config.getSheetName(), DEFAULT_RANGE);

        log.debug("Fetching range: {}", range);

        ValueRange response = service.spreadsheets().values()
                .get(config.getGoogleSheetId(), range)
                .execute();

        List<List<Object>> values = response.getValues();

        if (values == null || values.isEmpty()) {
            log.warn("No data found in spreadsheet");
            return Collections.emptyList();
        }

        log.info("Successfully read {} rows from Google Sheet", values.size());
        return values;
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
     * Використовується, якщо назва аркуша не вказана.
     */
    public List<String> fetchFirstRow(String spreadsheetId, String jsonPath) throws Exception {
        return fetchFirstRow(spreadsheetId, jsonPath, null);
    }

    /**
     * Створює та повертає авторизований клієнт Google Sheets API.
     */
    private Sheets getSheetsService(String jsonPath) throws IOException, GeneralSecurityException {
        log.debug("Initializing Google Sheets service with credentials from: {}", jsonPath);

        // Авторизація через Service Account JSON файл
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
     *
     * Приклади:
     * - buildRange(null, "A1:Z10") -> "A1:Z10" (перший аркуш за замовчуванням)
     * - buildRange("Sheet1", "A1:Z10") -> "Sheet1!A1:Z10"
     * - buildRange("My Data", "A1:Z10") -> "'My Data'!A1:Z10" (з лапками, якщо є пробіли)
     * - buildRange("John's Sheet", "A1:Z10") -> "'John''s Sheet'!A1:Z10" (екранування лапок)
     */
    private String buildRange(String sheetName, String cells) {
        // Якщо назва аркуша не вказана - використовуємо перший аркуш
        if (sheetName == null || sheetName.trim().isEmpty()) {
            log.debug("Sheet name not specified, using default (first sheet)");
            return cells;
        }

        // Якщо в назві є спецсимволи або пробіли - оточуємо одинарними лапками
        // та екрануємо існуючі лапки подвоєнням
        if (needsQuoting(sheetName)) {
            String escapedName = sheetName.replace("'", "''");
            String range = String.format("'%s'!%s", escapedName, cells);
            log.debug("Built quoted range: {}", range);
            return range;
        }

        // Якщо назва проста - без лапок
        String range = String.format("%s!%s", sheetName, cells);
        log.debug("Built simple range: {}", range);
        return range;
    }

    /**
     * Перевантажений метод для побудови range з дефолтним діапазоном.
     */
    private String buildRange(String sheetName) {
        return buildRange(sheetName, DEFAULT_RANGE);
    }

    /**
     * Перевіряє, чи потрібно оточувати назву аркуша лапками.
     * Лапки потрібні якщо назва містить:
     * - пробіли
     * - одинарні лапки
     * - спеціальні символи (!:;,)
     */
    private boolean needsQuoting(String sheetName) {
        if (sheetName == null || sheetName.isEmpty()) {
            return false;
        }

        // Перевіряємо на спецсимволи, які вимагають лапки
        return sheetName.contains(" ")
                || sheetName.contains("'")
                || sheetName.contains("!")
                || sheetName.contains(":")
                || sheetName.contains(";")
                || sheetName.contains(",");
    }

    /**
     * Валідує, чи існує вказаний аркуш у spreadsheet.
     * Корисно для додаткової перевірки перед синхронізацією.
     */
    public boolean validateSheetExists(String spreadsheetId, String jsonPath, String sheetName) {
        if (sheetName == null || sheetName.trim().isEmpty()) {
            return true; // Якщо не вказано - використовуємо дефолтний
        }

        try {
            List<String> sheets = getSheetNames(spreadsheetId, jsonPath);
            boolean exists = sheets.contains(sheetName);

            if (!exists) {
                log.warn("Sheet '{}' not found in spreadsheet. Available sheets: {}",
                        sheetName, sheets);
            }

            return exists;
        } catch (Exception e) {
            log.error("Error validating sheet existence: {}", e.getMessage());
            return false;
        }
    }
}