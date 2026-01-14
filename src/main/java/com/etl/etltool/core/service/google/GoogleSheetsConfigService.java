package com.etl.etltool.core.service.google;

import com.etl.etltool.dto.ConnectionResult;
import com.google.api.client.googleapis.javanet.GoogleNetHttpTransport;
import com.google.api.client.json.gson.GsonFactory;
import com.google.api.services.sheets.v4.Sheets;
import com.google.api.services.sheets.v4.SheetsScopes;
import com.google.auth.http.HttpCredentialsAdapter;
import com.google.auth.oauth2.GoogleCredentials;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.io.FileInputStream;
import java.io.IOException;
import java.security.GeneralSecurityException;
import java.util.Collections;
import java.util.List;

@Slf4j
@Service
public class GoogleSheetsConfigService {

    private static final String APPLICATION_NAME = "ETL-Tool";
    private static final List<String> SCOPES = Collections.singletonList(SheetsScopes.SPREADSHEETS);
    private static final GsonFactory JSON_FACTORY = GsonFactory.getDefaultInstance();

    /**
     * Створює та повертає клієнт Google Sheets
     */
    private Sheets createSheetsService(String jsonPath) throws IOException, GeneralSecurityException {
        // Зчитуємо файл сервісного акаунту
        FileInputStream in = new FileInputStream(jsonPath);
        GoogleCredentials credentials = GoogleCredentials.fromStream(in)
                .createScoped(SCOPES);

        // Ініціалізуємо транспорт та сервіс
        return new Sheets.Builder(
                GoogleNetHttpTransport.newTrustedTransport(),
                JSON_FACTORY,
                new HttpCredentialsAdapter(credentials))
                .setApplicationName(APPLICATION_NAME)
                .build();
    }

    /**
     * Метод для перевірки з'єднання (той, що ми обговорювали раніше)
     */
    public ConnectionResult checkConnection(String jsonPath, String spreadsheetId) {
        try {
            // Виклик тепер уже існуючого методу
            Sheets sheetsService = createSheetsService(jsonPath);

            // Спроба отримати дані про таблицю для перевірки доступу
            sheetsService.spreadsheets().get(spreadsheetId).execute();

            return new ConnectionResult(true, "Успішно підключено до Google Sheets!");
        } catch (IOException | GeneralSecurityException e) {
            log.error("Помилка автентифікації або доступу: {}", e.getMessage());
            return new ConnectionResult(false, "Помилка: " + e.getMessage());
        }
    }
}
