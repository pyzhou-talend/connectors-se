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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.net.URISyntaxException;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.Set;

import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.ss.usermodel.Sheet;
import org.apache.poi.ss.usermodel.Workbook;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.talend.components.azure.common.excel.ExcelFormat;
import org.talend.components.azure.common.exception.BlobRuntimeException;
import org.talend.components.azure.common.service.AzureComponentServices;
import org.talend.components.azure.runtime.output.BlobFileWriter;
import org.talend.components.azure.output.BlobOutputConfiguration;
import org.talend.sdk.component.api.record.Record;

import com.microsoft.azure.storage.StorageException;
import com.microsoft.azure.storage.blob.CloudBlockBlob;
import com.microsoft.azure.storage.blob.DeleteSnapshotsOption;

public class ExcelBlobFileWriter extends BlobFileWriter {

    private final static Logger logger = LoggerFactory.getLogger(ExcelBlobFileWriter.class);

    private BlobOutputConfiguration config;

    private Set<String> tempBlobItemNames;

    private ByteArrayOutputStream bos;

    private static final int EXCEL_97_MAX_ROWS = 65_536;

    private static final int EXCEL_2007_MAX_ROWS = 1_048_576;

    private final String fileExtention;

    public ExcelBlobFileWriter(BlobOutputConfiguration config, AzureComponentServices connectionServices) throws Exception {
        super(config, connectionServices);
        this.config = config;
        ExcelFormat format = config.getDataset().getExcelOptions().getExcelFormat();
        if (format == ExcelFormat.HTML) {
            throw new IllegalArgumentException("HTML excel output is not supported");
        } else if (format == ExcelFormat.EXCEL97) {
            fileExtention = ".xls";
        } else {
            fileExtention = ".xlsx";
        }
        tempBlobItemNames = new LinkedHashSet<>();
    }

    /**
     * Generates a temp file for batch (or part of batch)
     */
    @Override
    public void generateFile() throws URISyntaxException, StorageException {
        String tempName = config.getDataset().getDirectory() + "/temp/" + config.getBlobNameTemplate()
                + System.currentTimeMillis() + fileExtention;

        CloudBlockBlob tempFile = getContainer().getBlockBlobReference(tempName);
        if (tempFile.exists(null, null, AzureComponentServices.getTalendOperationContext())) {
            generateFile();
            return;
        }

        tempBlobItemNames.add(tempName);
        setCurrentItem(tempFile);
    }

    @Override
    public void newBatch() {
        super.newBatch();
        try {
            generateFile();
        } catch (Exception e) {
            throw new BlobRuntimeException(e);
        }
    }

    @Override
    public void flush() throws IOException, StorageException {
        if (getBatch().isEmpty()) {
            return;
        }
        bos = new ByteArrayOutputStream();

        switch (config.getDataset().getExcelOptions().getExcelFormat()) {
        case EXCEL97:
        case EXCEL2007:
            flushBatchToByteArray();
        }

        getCurrentItem().upload(new ByteArrayInputStream(bos.toByteArray()), -1);
        bos.close();
    }

    @Override
    public void complete() throws Exception {
        CloudBlockBlob finalItem = getContainer()
                .getBlockBlobReference(config.getDataset().getDirectory() + "/" + config.getBlobNameTemplate() + fileExtention);

        setCurrentItem(finalItem);

        ByteArrayOutputStream finalByteStream = new ByteArrayOutputStream();
        Workbook finalWorkBook = ExcelUtils.createWorkBook(config.getDataset().getExcelOptions().getExcelFormat());
        Sheet finalMainSheet = finalWorkBook.createSheet(config.getDataset().getExcelOptions().getSheetName());
        int rowCounter = 0;
        Iterator<String> tempBlobNamesIterator = tempBlobItemNames.iterator();
        while (tempBlobNamesIterator.hasNext()) {
            String tempBlobName = tempBlobNamesIterator.next();
            CloudBlockBlob tempBlobItem = getContainer().getBlockBlobReference(tempBlobName);
            if (tempBlobItem.exists()) {
                tempBlobNamesIterator.remove();
            } else {
                logger.warn("Can't find temp blob " + tempBlobName + ". Skipping it");
                continue;
            }

            Workbook tempWorkBook = ExcelUtils.createWorkBook(config.getDataset().getExcelOptions().getExcelFormat(),
                    tempBlobItem.openInputStream());
            Sheet sheet = tempWorkBook.getSheet(config.getDataset().getExcelOptions().getSheetName());
            for (int j = 0; j < sheet.getPhysicalNumberOfRows(); j++) {
                Row nextRow = sheet.getRow(j);
                // TODO check max row size
                Row finalNextRow = finalMainSheet.createRow(rowCounter++);
                for (int k = 0; k < nextRow.getPhysicalNumberOfCells(); k++) {
                    Cell c = nextRow.getCell(k);
                    Cell finalCell = finalNextRow.createCell(k, c.getCellType());
                    ExcelUtils.copyCellsValue(c, finalCell);
                }
            }

            tempBlobItem.deleteIfExists(DeleteSnapshotsOption.NONE, null, null,
                    AzureComponentServices.getTalendOperationContext());
        }

        finalWorkBook.write(finalByteStream);
        try (ByteArrayInputStream finalInputStream = new ByteArrayInputStream(finalByteStream.toByteArray())) {
            finalItem.upload(finalInputStream, -1);
        } finally {
            finalByteStream.close();
            finalWorkBook.close();
        }
    }

    private void flushBatchToByteArray() throws IOException {
        // TODO check batch size is less than max row in excel (otherwise split data)
        Workbook item = ExcelUtils.createWorkBook(config.getDataset().getExcelOptions().getExcelFormat());
        Sheet sheet = item.createSheet(config.getDataset().getExcelOptions().getSheetName());
        for (int i = 0; i < getBatch().size(); i++) {
            Row row = sheet.createRow(i);
            Record currentRecord = getBatch().get(i);
            for (int j = 0; j < currentRecord.getSchema().getEntries().size(); j++) {
                Cell cell = row.createCell(j);
                cell.setCellValue(
                        String.valueOf(currentRecord.get(Object.class, currentRecord.getSchema().getEntries().get(j).getName())));
            }
        }

        item.write(bos);
        item.close();
    }
}
