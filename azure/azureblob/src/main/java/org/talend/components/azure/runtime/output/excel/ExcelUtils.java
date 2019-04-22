/*
 * Copyright (C) 2006-2019 Talend Inc. - www.talend.com
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */

package org.talend.components.azure.runtime.output.excel;

import java.io.IOException;
import java.io.InputStream;

import org.apache.poi.hssf.usermodel.HSSFWorkbook;
import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.Workbook;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;
import org.talend.components.azure.common.excel.ExcelFormat;

import lombok.NonNull;

public class ExcelUtils {

    public static Workbook createWorkBook(ExcelFormat format) {
        return format == ExcelFormat.EXCEL97 ? new HSSFWorkbook() : new XSSFWorkbook();
    }

    public static Workbook createWorkBook(ExcelFormat format, InputStream is) throws IOException {
        return format == ExcelFormat.EXCEL97 ? new HSSFWorkbook(is) : new XSSFWorkbook(is);
    }

    public static void copyCellsValue(@NonNull Cell source, @NonNull Cell target) {
        target.setCellValue(source.getStringCellValue());
    }
}
