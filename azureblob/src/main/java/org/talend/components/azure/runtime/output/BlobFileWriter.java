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
import java.util.LinkedList;
import java.util.List;

import org.talend.components.azure.common.service.AzureComponentServices;
import org.talend.components.azure.output.BlobOutputConfiguration;
import org.talend.sdk.component.api.record.Record;
import org.talend.sdk.component.api.record.Schema;

import com.microsoft.azure.storage.CloudStorageAccount;
import com.microsoft.azure.storage.StorageException;
import com.microsoft.azure.storage.blob.CloudBlob;
import com.microsoft.azure.storage.blob.CloudBlobClient;
import com.microsoft.azure.storage.blob.CloudBlobContainer;

public abstract class BlobFileWriter {

    private CloudStorageAccount connection;

    private List<Record> batch;

    private Schema schema;

    private CloudBlob currentItem = null;

    private final CloudBlobContainer container;

    public BlobFileWriter(BlobOutputConfiguration config, AzureComponentServices connectionServices) throws Exception {
        this.connection = connectionServices.createStorageAccount(config.getDataset().getConnection());
        CloudBlobClient blobClient = connectionServices.createCloudBlobClient(connection,
                AzureComponentServices.DEFAULT_RETRY_POLICY);
        container = blobClient.getContainerReference(config.getDataset().getContainerName());
    }

    public void newBatch() {
        batch = new LinkedList<>();
    }

    public abstract void generateFile() throws URISyntaxException, StorageException;

    public void writeRecord(Record record) {
        if (schema == null) {
            schema = record.getSchema();
        }

        batch.add(record);
    }

    protected List<Record> getBatch() {
        return batch;
    }

    protected CloudBlob getCurrentItem() {
        return currentItem;
    }

    protected void setCurrentItem(CloudBlob currentItem) {
        this.currentItem = currentItem;
    }

    protected CloudBlobContainer getContainer() {
        return container;
    }

    protected Schema getSchema() {
        return schema;
    }

    /**
     * Upload prepared batch
     * 
     * @throws IOException
     * @throws StorageException
     */
    public abstract void flush() throws IOException, StorageException;

    /**
     * Delete temp items, unite all temporarily data to the final item blob file
     */
    public abstract void complete() throws Exception;

}
