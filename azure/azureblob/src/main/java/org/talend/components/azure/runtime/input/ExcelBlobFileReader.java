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

package org.talend.components.azure.runtime.input;

import java.io.IOException;
import java.io.InputStream;
import java.net.URISyntaxException;
import java.util.LinkedList;

import org.apache.commons.csv.CSVRecord;
import org.apache.poi.hssf.usermodel.HSSFWorkbook;
import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.CellType;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.ss.usermodel.Sheet;
import org.apache.poi.ss.usermodel.Workbook;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;
import org.talend.components.azure.common.excel.ExcelFormat;
import org.talend.components.azure.common.exception.BlobRuntimeException;
import org.talend.components.azure.dataset.AzureBlobDataset;
import org.talend.components.azure.runtime.converters.ExcelConverter;
import org.talend.components.azure.service.AzureBlobComponentServices;
import org.talend.sdk.component.api.record.Record;
import org.talend.sdk.component.api.record.Schema;
import org.talend.sdk.component.api.service.record.RecordBuilderFactory;

import com.microsoft.azure.storage.StorageException;
import com.microsoft.azure.storage.blob.ListBlobItem;

public class ExcelBlobFileReader extends BlobFileReader {

    public ExcelBlobFileReader(AzureBlobDataset config, RecordBuilderFactory recordBuilderFactory,
            AzureBlobComponentServices connectionServices) throws URISyntaxException, StorageException {
        super(config, recordBuilderFactory, connectionServices);
    }

    @Override
    protected ItemRecordIterator initItemRecordIterator(Iterable<ListBlobItem> blobItems) {
        return new ExcelRecordIterator(blobItems);
    }

    private class ExcelRecordIterator extends BlobFileReader.ItemRecordIterator<Row> {

        private LinkedList<Row> rows;

        private ExcelConverter converter;

        public ExcelRecordIterator(Iterable<ListBlobItem> blobItemsList) {
            super(blobItemsList);
            this.rows = new LinkedList<>();
            takeFirstItem();
        }

        @Override
        protected Record convertToRecord(Row next) {
            return converter.toRecord(next);
        }

        @Override
        protected void readItem() {
            if (converter == null) {
                converter = ExcelConverter.of(getConfig().getExcelOptions(), getRecordBuilderFactory());
            }

            try (InputStream input = getCurrentItem().openInputStream()) {
                Workbook wb;
                if (getConfig().getExcelOptions().getExcelFormat() == ExcelFormat.EXCEL97) {
                    wb = new HSSFWorkbook(input);
                } else {
                    wb = new XSSFWorkbook(input);
                }
                Sheet sheet = wb.getSheet(getConfig().getExcelOptions().getSheetName());

                if (getConfig().getExcelOptions().isUseHeader() && getConfig().getExcelOptions().getHeader() >= 1) {

                    Row headerRow = sheet.getRow(getConfig().getExcelOptions().getHeader() - 1);
                    if (converter.getColumnNames() == null) {
                        converter.inferSchemaNames(headerRow, true);
                    }
                }

                for (int i = getConfig().getExcelOptions().getHeader(); i < sheet.getPhysicalNumberOfRows(); i++) { // TODO check
                                                                                                                    // physical is
                                                                                                                    // working
                                                                                                                    // correctly
                    Row row = sheet.getRow(i);
                    rows.add(row);
                }

            } catch (StorageException | IOException e) {
                throw new BlobRuntimeException(e);
            }
        }

        @Override
        protected boolean hasNextRecordTaken() {
            return getConfig().getExcelOptions().isUseFooter() ? rows.size() > getConfig().getExcelOptions().getFooter()
                    : rows.size() > 0;
        }

        @Override
        protected Row takeNextRecord() {
            return rows.poll();
        }
    }
}
