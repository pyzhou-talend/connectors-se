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

package org.talend.components.azure.runtime.converters;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.CellType;
import org.apache.poi.ss.usermodel.CellValue;
import org.apache.poi.ss.usermodel.FormulaEvaluator;
import org.apache.poi.ss.usermodel.Row;
import org.talend.components.azure.common.excel.ExcelFormat;
import org.talend.components.azure.common.excel.ExcelFormatOptions;
import org.talend.components.azure.common.exception.BlobRuntimeException;
import org.talend.sdk.component.api.record.Record;
import org.talend.sdk.component.api.record.Schema;
import org.talend.sdk.component.api.service.record.RecordBuilderFactory;

import lombok.Getter;

// TODO <Row> ???
public class ExcelConverter implements RecordConverter {

    private static RecordBuilderFactory builderFactory;

    private final ExcelFormatOptions excelFormatOptions;

    private Schema schema;

    @Getter
    private List<String> columnNames;

    @Getter
    private List<CellType> columnTypes;

    public ExcelConverter(ExcelFormatOptions excelFormatOptions) {
        this.excelFormatOptions = excelFormatOptions;

    }

    public static ExcelConverter of(ExcelFormatOptions excelFormatOptions, RecordBuilderFactory builderFactory) {
        if (ExcelConverter.builderFactory == null) {
            ExcelConverter.builderFactory = builderFactory;
        }

        return new ExcelConverter(excelFormatOptions);
    }

    /**
     * @param record
     * @return
     */
    @Override
    public Schema inferSchema(Object record) {
        if (schema == null) {
            if (!(record instanceof Row)) {
                throw new IllegalArgumentException("Record must be apache row");
            }
            if (columnNames == null) {
                columnNames = inferSchemaNames((Row) record, false);
            }

            if (columnTypes == null) {
                columnTypes = inferSchemaTypes((Row) record);
            }

            Schema.Builder schemaBuilder = builderFactory.newSchemaBuilder(Schema.Type.RECORD);
            for (int i = 0; i < columnNames.size(); i++) {
                Schema.Entry.Builder entryBuilder = builderFactory.newEntryBuilder();
                entryBuilder.withName(columnNames.get(i));
                CellType cellType = columnTypes.get(i);
                if (cellType == CellType.FORMULA) {
                    Cell cell = ((Row) record).getCell(i);
                    if (excelFormatOptions.getExcelFormat() == ExcelFormat.EXCEL97) {
                        cellType = cell.getCachedFormulaResultType();
                    } else if (excelFormatOptions.getExcelFormat() == ExcelFormat.EXCEL2007) {
                        FormulaEvaluator formulaEvaluator = ((Row) record).getSheet().getWorkbook().getCreationHelper()
                                .createFormulaEvaluator();
                        cellType = formulaEvaluator.evaluateFormulaCell(cell);
                    }
                }
                switch (cellType) {
                case ERROR:
                    throw new BlobRuntimeException("Error cell exists in excel document in the " + i + 1 + " column");
                case STRING:
                    entryBuilder.withType(Schema.Type.STRING);
                    break;
                case NUMERIC:
                    entryBuilder.withType(Schema.Type.DOUBLE);
                    break;
                case BOOLEAN:
                    entryBuilder.withType(Schema.Type.BOOLEAN);
                }
                schemaBuilder.withEntry(entryBuilder.build());
            }
            schema = schemaBuilder.build();
        }
        return schema;
    }

    private List<CellType> inferSchemaTypes(Row record) {
        Iterator<Cell> cellIterator = record.cellIterator();
        List<CellType> cellTypes = new ArrayList<>();

        while (cellIterator.hasNext()) {
            cellTypes.add(cellIterator.next().getCellType());
        }

        return cellTypes;
    }

    public List<String> inferSchemaNames(Row next, boolean isHeader) {
        List<String> columns = new ArrayList<>();
        for (int i = 0;; i++) {
            Cell cell = next.getCell(i);
            if (cell == null)
                break;

            String columnName;
            if (isHeader) {
                columnName = cell.getStringCellValue();
            } else {
                columnName = "field" + i;
            }

            columns.add(columnName);
        }
        columnNames = columns;
        return columns;
    }

    @Override
    public Record toRecord(Object record) {
        if (!(record instanceof Row)) {
            throw new IllegalArgumentException("Record must be apache poi row");
        }

        if (schema == null) {
            inferSchema(record);
        }

        Record.Builder recordBuilder = builderFactory.newRecordBuilder();

        for (int i = 0; i < schema.getEntries().size(); i++) {
            Cell recordCell = ((Row) record).getCell(i);
            switch (schema.getEntries().get(i).getType()) {
            case BOOLEAN:
                recordBuilder.withBoolean(columnNames.get(i), recordCell.getBooleanCellValue());
                break;
            case DOUBLE:
                recordBuilder.withDouble(columnNames.get(i), recordCell.getNumericCellValue());
                break;
            default:
                recordBuilder.withString(columnNames.get(i), recordCell.getStringCellValue());
            }
        }

        return recordBuilder.build();
    }

    @Override
    public Object fromRecord(Record record) {
        return null;
    }
}
