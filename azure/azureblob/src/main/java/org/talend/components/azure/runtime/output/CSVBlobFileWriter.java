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

package org.talend.components.azure.runtime.output;

import java.io.IOException;
import java.net.URISyntaxException;
import java.util.Iterator;
import java.util.List;

import org.talend.components.azure.common.csv.CSVFormatOptions;
import org.talend.components.azure.common.exception.BlobRuntimeException;
import org.talend.components.azure.common.service.AzureComponentServices;
import org.talend.components.azure.output.BlobOutputConfiguration;
import org.talend.components.azure.runtime.converters.CSVConverter;
import org.talend.components.azure.service.AzureBlobComponentServices;
import org.talend.sdk.component.api.record.Record;

import com.microsoft.azure.storage.StorageException;
import com.microsoft.azure.storage.blob.CloudAppendBlob;

public class CSVBlobFileWriter extends BlobFileWriter {

    private BlobOutputConfiguration config;

    private final CSVFormatOptions configCSV;

    private boolean fileIsEmpty = true;

    public CSVBlobFileWriter(BlobOutputConfiguration config, AzureBlobComponentServices connectionServices) throws Exception {
        super(config, connectionServices);
        this.config = config;
        this.configCSV = config.getDataset().getCsvOptions();

    }

    @Override
    public void generateFile() throws URISyntaxException, StorageException {
        CloudAppendBlob currentItem = getContainer()
                .getAppendBlobReference(config.getDataset().getDirectory() + "/" + config.getBlobNameTemplate() + ".csv");
        // TODO not replace if append
        currentItem.createOrReplace();

        setCurrentItem(currentItem);
    }

    @Override
    public void newBatch() {
        super.newBatch();

        if (getCurrentItem() == null) {
            try {
                generateFile();
            } catch (Exception e) {
                throw new BlobRuntimeException(e);
            }
        }
    }

    // TODO move common implementation to the abstract class
    @Override
    public void flush() throws IOException, StorageException {
        if (getBatch().isEmpty()) {
            return;
        }

        String content = convertBatchToString();
        if (!fileIsEmpty) {
            content = CSVConverter.getRecordDelimiterValue(configCSV) + content;
        } else if (configCSV.isUseHeader() && configCSV.getHeader() > 0) {
            appendHeader();
        }
        ((CloudAppendBlob) getCurrentItem()).appendText(content, "UTF-8", null, null,
                AzureComponentServices.getTalendOperationContext());
        fileIsEmpty = false;
        // TODO charset name

        getBatch().clear();
    }

    private void appendHeader() throws IOException, StorageException {
        // TODO add more lines if needed
        if (getSchema() == null || getSchema().getEntries().size() == 0)
            return;
        StringBuilder headerBuilder = new StringBuilder();
        for (int i = 0; i < configCSV.getHeader() - 1; i++) {
            headerBuilder.append("//header line").append(CSVConverter.getRecordDelimiterValue(configCSV));
        }

        headerBuilder.append(getSchema().getEntries().get(0).getName());
        for (int i = 1; i < getSchema().getEntries().size(); i++) {
            headerBuilder.append(CSVConverter.getFieldDelimiterValue(configCSV))
                    .append(getSchema().getEntries().get(i).getName());
        }
        ((CloudAppendBlob) getCurrentItem())
                .appendText(headerBuilder.toString() + CSVConverter.getRecordDelimiterValue(configCSV));
        fileIsEmpty = false;
    }

    private String convertBatchToString() {
        StringBuilder contentBuilder = new StringBuilder();
        List<Record> batch = getBatch();
        Iterator<Record> recordIterator = batch.iterator();
        if (recordIterator.hasNext()) {
            contentBuilder.append(convertRecordToString(recordIterator.next()));

            while (recordIterator.hasNext()) {
                contentBuilder.append(CSVConverter.getRecordDelimiterValue(configCSV))
                        .append(convertRecordToString(recordIterator.next()));
            }
        }

        return contentBuilder.toString();
    }

    private String convertRecordToString(Record record) {
        StringBuilder stringBuilder = new StringBuilder();

        if (!getSchema().getEntries().isEmpty()) {
            stringBuilder.append(record.get(Object.class, getSchema().getEntries().get(0).getName()));

            for (int i = 1; i < getSchema().getEntries().size(); i++) {
                stringBuilder.append(CSVConverter.getFieldDelimiterValue(configCSV))
                        .append(record.get(Object.class, getSchema().getEntries().get(i).getName()));
            }
        }

        return stringBuilder.toString();
    }

    @Override
    public void complete() {
        // NOOP
    }
}
