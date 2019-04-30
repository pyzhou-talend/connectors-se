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

import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URISyntaxException;
import java.util.Iterator;
import java.util.LinkedList;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.talend.components.azure.common.Encoding;
import org.talend.components.azure.common.exception.BlobRuntimeException;
import org.talend.components.azure.dataset.AzureBlobDataset;
import org.talend.components.azure.runtime.converters.CSVConverter;
import org.talend.components.azure.service.AzureBlobComponentServices;
import org.talend.sdk.component.api.record.Record;
import org.talend.sdk.component.api.service.record.RecordBuilderFactory;

import com.microsoft.azure.storage.StorageException;
import com.microsoft.azure.storage.blob.ListBlobItem;

public class CSVBlobFileReader extends BlobFileReader {

    CSVBlobFileReader(AzureBlobDataset config, RecordBuilderFactory recordBuilderFactory,
            AzureBlobComponentServices connectionServices) throws URISyntaxException, StorageException {
        super(config, recordBuilderFactory, connectionServices);
    }

    @Override
    protected ItemRecordIterator initItemRecordIterator(Iterable<ListBlobItem> blobItems) {
        return new CSVFileRecordIterator(blobItems);
    }

    @Override
    public Record readRecord() {
        return super.readRecord();
    }

    private class CSVFileRecordIterator extends ItemRecordIterator<CSVRecord> {

        private LinkedList<CSVRecord> recordList;

        private CSVFormat format;

        private CSVConverter converter;

        private String encodingValue;

        public CSVFileRecordIterator(Iterable<ListBlobItem> blobItemsList) {
            super(blobItemsList);
            this.encodingValue = getConfig().getCsvOptions().getEncoding() == Encoding.OTHER
                    ? getConfig().getCsvOptions().getCustomEncoding()
                    : getConfig().getCsvOptions().getEncoding().getEncodingValue();

            recordList = new LinkedList<>();

            takeFirstItem();
        }

        @Override
        protected Record convertToRecord(CSVRecord next) {
            if (converter.recordBuilderFactory == null) {
                converter.recordBuilderFactory = getRecordBuilderFactory();
            }
            return converter.toRecord(next);
        }

        @Override
        protected void readItem() {
            if (converter == null) {
                converter = CSVConverter.of(getConfig().getCsvOptions());
            }

            if (format == null) {
                format = converter.getCsvFormat();
            }
            try (InputStream input = getCurrentItem().openInputStream();
                    InputStreamReader inr = new InputStreamReader(input, encodingValue);
                    CSVParser parser = new CSVParser(inr, format)) {
                Iterator<CSVRecord> recordIterator = parser.getRecords().iterator();
                if (getConfig().getCsvOptions().isUseHeader() && getConfig().getCsvOptions().getHeader() >= 1) {
                    for (int i = 0; i < getConfig().getCsvOptions().getHeader() - 1; i++) {
                        // skip extra header lines
                        recordIterator.next();
                    }
                    CSVRecord headerRecord = recordIterator.next();
                    // save schema from first file
                    if (converter.getSchema() == null) {
                        converter.toRecord(headerRecord);
                    }
                }
                while (recordIterator.hasNext()) {
                    recordList.add(recordIterator.next());
                }
            } catch (Exception e) {
                throw new BlobRuntimeException(e);
            }
        }

        @Override
        protected boolean hasNextRecordTaken() {
            return recordList.size() > 0;
        }

        @Override
        protected CSVRecord takeNextRecord() {
            return recordList.poll();
        }
    }
}
