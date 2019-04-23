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
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import org.apache.commons.lang3.StringUtils;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;
import org.talend.components.azure.common.excel.ExcelFormatOptions;
import org.talend.components.azure.common.exception.BlobRuntimeException;
import org.talend.components.azure.common.service.AzureComponentServices;
import org.talend.components.azure.dataset.AzureBlobDataset;
import org.talend.components.azure.service.AzureBlobComponentServices;
import org.talend.sdk.component.api.record.Record;
import org.talend.sdk.component.api.service.record.RecordBuilderFactory;

import com.microsoft.azure.storage.StorageException;
import com.microsoft.azure.storage.blob.ListBlobItem;

public class ExcelHTMLBlobFileReader extends BlobFileReader {

    private ExcelFormatOptions excelConfig;

    public ExcelHTMLBlobFileReader(AzureBlobDataset config, RecordBuilderFactory recordBuilderFactory,
            AzureBlobComponentServices connectionServices) throws URISyntaxException, StorageException {
        super(config, recordBuilderFactory, connectionServices);
        this.excelConfig = config.getExcelOptions();
    }

    @Override
    protected ItemRecordIterator initItemRecordIterator(Iterable<ListBlobItem> blobItems) {
        return new HTMLRecordIterator(blobItems);
    }

    private class HTMLRecordIterator extends ItemRecordIterator<Element> {

        private Iterator<Element> rowIterator;

        private List<String> columns;

        public HTMLRecordIterator(Iterable<ListBlobItem> blobItemsList) {
            super(blobItemsList);
            takeFirstItem();
        }

        @Override
        protected void readItem() {

            try (InputStream input = getCurrentItem().openInputStream()) {
                Document document = Jsoup.parse(input, excelConfig.getEncoding().getEncodingValue(), "");
                Element body = document.body();
                Elements rows = body.getElementsByTag("tr");
                rowIterator = rows.iterator();
            } catch (Exception e) {
                throw new BlobRuntimeException(e);
            }
        }

        @Override
        protected boolean hasNextRecordTaken() {
            return rowIterator.hasNext();
        }

        @Override
        protected Element takeNextRecord() {
            return rowIterator.next();
        }

        @Override
        protected Record convertToRecord(Element row) {
            if (columns == null) {
                columns = inferSchemaInfo(row, true);
            }

            Record.Builder builder = getRecordBuilderFactory().newRecordBuilder();
            Elements rowColumns = row.getElementsByTag("td");
            for (int i = 0; i < rowColumns.size(); i++) {
                builder.withString(columns.get(i), rowColumns.get(i).text());
            }
            return builder.build();
        }

        // TODO move it
        private List<String> inferSchemaInfo(Element row, boolean useDefaultFieldName) {
            List<String> result = new ArrayList<>();
            Set<String> existNames = new HashSet<>();
            int index = 0;
            Elements columns = row.getElementsByTag("td");
            for (int i = 0; i < columns.size(); i++) {
                String fieldName = columns.get(i).ownText();
                if (useDefaultFieldName || StringUtils.isEmpty(fieldName)) {
                    fieldName = "field" + i;
                }

                String finalName = SchemaUtils.correct(fieldName, index++, existNames);
                existNames.add(finalName);

                result.add(finalName);
            }
            return result;
        }
    }
}
