package com.etl.etltool.core.service.google;

import com.etl.etltool.core.entity.AppConfig;
import com.google.api.client.googleapis.javanet.GoogleNetHttpTransport;
import com.google.api.client.json.gson.GsonFactory;
import com.google.api.services.sheets.v4.Sheets;
import com.google.api.services.sheets.v4.SheetsScopes;
import com.google.api.services.sheets.v4.model.ValueRange;
import com.google.auth.http.HttpCredentialsAdapter;
import com.google.auth.oauth2.GoogleCredentials;
import org.springframework.stereotype.Service;

import java.io.FileInputStream;
import java.io.IOException;
import java.security.GeneralSecurityException;
import java.util.Collections;
import java.util.List;

@Service
public class GoogleSheetsService {

    private static final String APPLICATION_NAME = "ETL-Tool";
    private static final GsonFactory JSON_FACTORY = GsonFactory.getDefaultInstance();

    // Метод, який викликатиме SyncService
    public List<List<Object>> readSheet(AppConfig config) throws Exception {
        Sheets service = getSheetsService(config.getServiceAccountKeyPath());

        // Зчитуємо дані (діапазон можна винести в налаштування, зараз A1:Z1000)
        ValueRange response = service.spreadsheets().values()
                .get(config.getGoogleSheetId(), "A1:Z1000")
                .execute();

        return response.getValues();
    }

    private Sheets getSheetsService(String jsonPath) throws IOException, GeneralSecurityException {
        // Авторизація через Service Account JSON файл
        GoogleCredentials credentials = GoogleCredentials.fromStream(new FileInputStream(jsonPath))
                .createScoped(Collections.singleton(SheetsScopes.SPREADSHEETS_READONLY));

        return new Sheets.Builder(
                GoogleNetHttpTransport.newTrustedTransport(),
                JSON_FACTORY,
                new HttpCredentialsAdapter(credentials))
                .setApplicationName(APPLICATION_NAME)
                .build();
    }

    public List<String> fetchFirstRow(String spreadsheetId, String jsonPath) throws Exception {
        // Використовуємо твій існуючий метод авторизації
        Sheets service = getSheetsService(jsonPath);

        // Зчитуємо тільки перший рядок
        ValueRange response = service.spreadsheets().values()
                .get(spreadsheetId, "1:1")
                .execute();

        List<List<Object>> values = response.getValues();
        if (values == null || values.isEmpty()) {
            return Collections.emptyList();
        }

        // Перетворюємо список об'єктів у список рядків
        return values.getFirst().stream()
                .map(Object::toString)
                .toList();
    }
}