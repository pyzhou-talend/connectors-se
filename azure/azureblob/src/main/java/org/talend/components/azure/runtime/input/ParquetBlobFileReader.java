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

import java.net.URISyntaxException;

import org.talend.components.azure.common.service.AzureComponentServices;
import org.talend.components.azure.dataset.AzureBlobDataset;
import org.talend.sdk.component.api.record.Record;
import org.talend.sdk.component.api.service.record.RecordBuilderFactory;

import com.microsoft.azure.storage.StorageException;
import com.microsoft.azure.storage.blob.ListBlobItem;

public class ParquetBlobFileReader extends BlobFileReader {

    public ParquetBlobFileReader(AzureBlobDataset config, RecordBuilderFactory recordBuilderFactory,
            AzureComponentServices connectionServices) throws URISyntaxException, StorageException {
        super(config, recordBuilderFactory, connectionServices);
    }

    @Override
    protected ItemRecordIterator initItemRecordIterator(Iterable<ListBlobItem> blobItems) {
        return new ParquetRecordIterator(blobItems);
    }

    private class ParquetRecordIterator extends ItemRecordIterator {

        protected ParquetRecordIterator(Iterable<ListBlobItem> blobItems) {
            super(blobItems);
            takeFirstItem();
        }

        @Override
        protected Object takeNextRecord() {
            return null;
        }

        @Override
        protected boolean hasNextRecordTaken() {
            return false;
        }

        @Override
        protected Record convertToRecord(Object next) {
            return null;
        }

        @Override
        protected void readItem() {

        }
    }
}
