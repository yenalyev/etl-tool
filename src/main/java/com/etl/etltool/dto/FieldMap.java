package com.etl.etltool.dto;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class FieldMap {
    private String sourceHeader; // Заголовок з Google Sheet (наприклад, "Ціна")
    private String targetColumn; // Назва в БД (наприклад, "price" або "tsina")
}
