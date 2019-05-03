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

package org.talend.components.azure;

import java.net.URISyntaxException;

import org.apache.commons.lang3.RandomStringUtils;
import org.junit.ClassRule;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.talend.components.azure.common.Encoding;
import org.talend.components.azure.common.FileFormat;
import org.talend.components.azure.common.excel.ExcelFormat;
import org.talend.components.azure.common.excel.ExcelFormatOptions;
import org.talend.components.azure.dataset.AzureBlobDataset;
import org.talend.components.azure.datastore.AzureCloudConnection;
import org.talend.components.azure.service.AzureBlobComponentServices;
import org.talend.components.azure.source.BlobInputProperties;
import org.talend.sdk.component.api.service.Service;
import org.talend.sdk.component.junit.SimpleComponentRule;
import org.talend.sdk.component.junit5.WithComponents;

import com.microsoft.azure.storage.CloudStorageAccount;
import com.microsoft.azure.storage.StorageException;
import com.microsoft.azure.storage.blob.CloudBlob;
import com.microsoft.azure.storage.blob.CloudBlobClient;
import com.microsoft.azure.storage.blob.CloudBlobContainer;
import com.microsoft.azure.storage.blob.ListBlobItem;

@WithComponents("org.talend.components.azure")
public class BaseIT {

    @ClassRule
    public static final SimpleComponentRule COMPONENT = new SimpleComponentRule("org.talend.components.azure");

    protected static String containerName;

    protected static CloudStorageAccount storageAccount;

    protected static AzureCloudConnection dataStore;

    @BeforeAll
    public static void initContainer() throws Exception {
        containerName = "test-it-" + RandomStringUtils.randomAlphabetic(10).toLowerCase();
        dataStore = BlobTestUtils.createCloudConnection();
        AzureBlobComponentServices componentService = COMPONENT.findService(AzureBlobComponentServices.class);

        storageAccount = componentService.createStorageAccount(dataStore);
        BlobTestUtils.createStorage(containerName, storageAccount);
    }

    @AfterEach
    public void cleanUpContainer() throws URISyntaxException, StorageException {
        CloudBlobClient blobClient = storageAccount.createCloudBlobClient();
        CloudBlobContainer container = blobClient.getContainerReference(containerName);

        for (ListBlobItem blob : container.listBlobs("", true)) {
            if (blob instanceof CloudBlob) {
                ((CloudBlob) blob).delete();
            }
        }
    }

    @AfterAll
    public static void removeContainer() throws URISyntaxException, StorageException {
        BlobTestUtils.deleteStorage(containerName, storageAccount);
    }
}
