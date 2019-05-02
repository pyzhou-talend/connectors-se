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

package org.talend.components.azure.source;

import java.io.IOException;
import java.net.URISyntaxException;
import java.util.List;

import org.apache.commons.lang3.RandomStringUtils;
import org.junit.Assert;
import org.junit.ClassRule;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.talend.components.azure.BlobTestUtils;
import org.talend.components.azure.common.Encoding;
import org.talend.components.azure.common.FileFormat;
import org.talend.components.azure.common.connection.AzureStorageConnectionAccount;
import org.talend.components.azure.common.excel.ExcelFormat;
import org.talend.components.azure.common.excel.ExcelFormatOptions;
import org.talend.components.azure.dataset.AzureBlobDataset;
import org.talend.components.azure.datastore.AzureCloudConnection;
import org.talend.components.azure.service.AzureBlobComponentServices;
import org.talend.sdk.component.api.record.Record;
import org.talend.sdk.component.api.service.Service;
import org.talend.sdk.component.junit.SimpleComponentRule;
import org.talend.sdk.component.junit5.WithComponents;
import org.talend.sdk.component.maven.MavenDecrypter;
import org.talend.sdk.component.maven.Server;
import org.talend.sdk.component.runtime.manager.chain.Job;

import com.microsoft.azure.storage.CloudStorageAccount;
import com.microsoft.azure.storage.StorageException;
import static org.talend.sdk.component.junit.SimpleFactory.configurationByExample;

@WithComponents("org.talend.components.azure")
public class HTMLInputIT {
    @Service
    private AzureBlobComponentServices componentService;

    @ClassRule
    public static final SimpleComponentRule COMPONENT = new SimpleComponentRule("org.talend.components.azure");

    private static BlobInputProperties blobInputProperties;

    private CloudStorageAccount storageAccount;

    private String containerName;

    @BeforeEach
    public void init() throws Exception {
        containerName = "test-it-" + RandomStringUtils.randomAlphabetic(10).toLowerCase();
        AzureCloudConnection dataStore = BlobTestUtils.createCloudConnection();

        AzureBlobDataset dataset = new AzureBlobDataset();
        dataset.setConnection(dataStore);
        dataset.setFileFormat(FileFormat.EXCEL);
        ExcelFormatOptions excelFormatOptions = new ExcelFormatOptions();
        excelFormatOptions.setExcelFormat(ExcelFormat.HTML);
        excelFormatOptions.setEncoding(Encoding.UFT8);
        dataset.setExcelOptions(excelFormatOptions);

        dataset.setContainerName(containerName);
        blobInputProperties = new BlobInputProperties();
        blobInputProperties.setDataset(dataset);

        storageAccount = componentService.createStorageAccount(blobInputProperties.getDataset().getConnection());
        BlobTestUtils.createStorage(blobInputProperties.getDataset().getContainerName(), storageAccount);
    }
    @Test
    public void testInput1File1Row() throws StorageException, IOException, URISyntaxException {
        final int recordSize = 1;
        final int columnSize = 2;

        blobInputProperties.getDataset().setDirectory("excelHTML");
        BlobTestUtils.uploadTestFile(storageAccount, blobInputProperties, "excelHTML/TestExcelHTML1Row.html", "TestExcelHTML1Row.html");

        String inputConfig = configurationByExample().forInstance(blobInputProperties).configured().toQueryString();
        Job.components().component("azureInput", "Azure://Input?" + inputConfig).component("collector", "test://collector")
                .connections().from("azureInput").to("collector").build().run();
        List<Record> records = COMPONENT.getCollectedData(Record.class);

        Assert.assertEquals("Records amount is different", recordSize, records.size());
        Record firstRecord = records.get(0);
        Assert.assertEquals("Record's schema is different", columnSize, firstRecord.getSchema().getEntries().size());
        Assert.assertEquals("a1", firstRecord.getString("field0"));
        Assert.assertEquals("b1", firstRecord.getString("field1"));
    }

    @Test
    public void testInput1FileMultipleRows() throws StorageException, IOException, URISyntaxException {
        final int recordSize = 5;
        final int columnSize = 2;

        blobInputProperties.getDataset().setDirectory("excelHTML");
        BlobTestUtils.uploadTestFile(storageAccount, blobInputProperties, "excelHTML/TestExcelHTML5Rows.html", "TestExcelHTML5Rows.html");

        String inputConfig = configurationByExample().forInstance(blobInputProperties).configured().toQueryString();
        Job.components().component("azureInput", "Azure://Input?" + inputConfig).component("collector", "test://collector")
                .connections().from("azureInput").to("collector").build().run();
        List<Record> records = COMPONENT.getCollectedData(Record.class);

        Assert.assertEquals("Records amount is different", recordSize, records.size());
        for (int i = 0; i < recordSize; i++) {
            Record record = records.get(i);
            Assert.assertEquals("Record's schema is different", columnSize, record.getSchema().getEntries().size());
            Assert.assertEquals("a" + (i+1), record.getString("field0"));
            Assert.assertEquals("b" + (i+1), record.getString("field1"));
        }
    }

    @Test
    public void testInputMultipleFiles() throws StorageException, IOException, URISyntaxException {
        final int recordSize = 1 + 5;

        BlobTestUtils.uploadTestFile(storageAccount, blobInputProperties, "excelHTML/TestExcelHTML1Row.html", "TestExcelHTML1Row.html");
        BlobTestUtils.uploadTestFile(storageAccount, blobInputProperties, "excelHTML/TestExcelHTML5Rows.html", "TestExcelHTML5Rows.html");

        String inputConfig = configurationByExample().forInstance(blobInputProperties).configured().toQueryString();
        Job.components().component("azureInput", "Azure://Input?" + inputConfig).component("collector", "test://collector")
                .connections().from("azureInput").to("collector").build().run();
        List<Record> records = COMPONENT.getCollectedData(Record.class);

        Assert.assertEquals("Records amount is different", recordSize, records.size());
    }

    @AfterEach
    public void removeContainer() throws URISyntaxException, StorageException {
        BlobTestUtils.deleteStorage(containerName, storageAccount);
    }
}
