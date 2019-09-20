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
package org.talend.components.couchbase.source;

import com.couchbase.client.deps.io.netty.buffer.ByteBuf;
import com.couchbase.client.deps.io.netty.buffer.Unpooled;
import com.couchbase.client.java.Bucket;
import com.couchbase.client.java.document.BinaryDocument;
import com.couchbase.client.java.document.JsonDocument;
import com.couchbase.client.java.document.StringDocument;
import com.couchbase.client.java.document.json.JsonObject;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.talend.components.couchbase.CouchbaseUtilTest;
import org.talend.components.couchbase.TestData;
import org.talend.components.couchbase.dataset.CouchbaseDataSet;
import org.talend.components.couchbase.dataset.DocumentType;
import org.talend.sdk.component.api.record.Record;
import org.talend.sdk.component.api.service.Service;
import org.talend.sdk.component.api.service.record.RecordBuilderFactory;
import org.talend.sdk.component.junit.BaseComponentsHandler;
import org.talend.sdk.component.junit5.Injected;
import org.talend.sdk.component.junit5.WithComponents;
import org.talend.sdk.component.runtime.manager.chain.Job;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.talend.sdk.component.junit.SimpleFactory.configurationByExample;

@Slf4j
@WithComponents("org.talend.components.couchbase")
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@DisplayName("Testing of CouchbaseInput component")
public class CouchbaseInputTest extends CouchbaseUtilTest {

    @Injected
    private BaseComponentsHandler componentsHandler;

    @Service
    private RecordBuilderFactory recordBuilderFactory;

    private void executeJob(CouchbaseInputConfiguration configuration) {
        final String inputConfig = configurationByExample().forInstance(configuration).configured().toQueryString();
        Job.components().component("Couchbase_Input", "Couchbase://Input?" + inputConfig)
                .component("collector", "test://collector").connections().from("Couchbase_Input").to("collector").build().run();
    }

    @Test
    @DisplayName("Check input data")
    void couchbaseInputDataTest() {
        log.info("test couchbaseInputDataTest started");
        String idPrefix = "couchbaseInputDataTest";
        insertTestDataToDB(idPrefix);
        executeJob(getInputConfiguration());

        final List<Record> res = componentsHandler.getCollectedData(Record.class);

        assertNotNull(res);
        List<Record> data = res.stream().filter(record -> record.getString("_meta_id_").startsWith(idPrefix))
                .sorted(Comparator.comparing(r -> r.getString("_meta_id_"))).collect(Collectors.toList());
        assertEquals(2, data.size());

        TestData testData = new TestData();
        assertEquals(testData.getColId() + "1", data.get(0).getString("t_string"));
        assertEquals(testData.getColIntMin(), data.get(0).getInt("t_int_min"));
        assertEquals(testData.getColIntMax(), data.get(0).getInt("t_int_max"));
        assertEquals(testData.getColLongMin(), data.get(0).getLong("t_long_min"));
        assertEquals(testData.getColLongMax(), data.get(0).getLong("t_long_max"));
        assertEquals(testData.getColFloatMin(), data.get(0).getFloat("t_float_min"));
        assertEquals(testData.getColFloatMax(), data.get(0).getFloat("t_float_max"));
        assertEquals(testData.getColDoubleMin(), data.get(0).getDouble("t_double_min"));
        assertEquals(testData.getColDoubleMax(), data.get(0).getDouble("t_double_max"));
        assertEquals(testData.isColBoolean(), data.get(0).getBoolean("t_boolean"));
        assertEquals(testData.getColDateTime().toString(), data.get(0).getDateTime("t_datetime").toString());
        String arrayStrOriginal = "[" + testData.getColList().stream().map(s -> "\"" + s + "\"").collect(Collectors.joining(","))
                + "]";
        assertEquals(arrayStrOriginal, data.get(0).getString("t_array"));

        assertEquals(testData.getColId() + "2", data.get(1).getString("t_string"));
        log.info("test couchbaseInputDataTest finished");
    }

    private void insertTestDataToDB(String idPrefix) {
        Bucket bucket = couchbaseCluster.openBucket(BUCKET_NAME, BUCKET_PASSWORD);
//        bucket.bucketManager().flush();

        List<JsonObject> jsonObjects = createJsonObjects();
        for (int i = 0; i < 2; i++) {
            bucket.insert(JsonDocument.create(generateDocId(idPrefix, i), jsonObjects.get(i)));
        }
        bucket.close();
    }

    private List<JsonObject> createJsonObjects() {
        TestData testData = new TestData();
        List<JsonObject> jsonObjects = new ArrayList<>();
        for (int i = 1; i <= 2; i++) {
            jsonObjects.add(createJsonObject(testData.getColId() + i));
        }
        return jsonObjects;
    }

    private JsonObject createJsonObject(String id) {
        TestData testData = new TestData();
        JsonObject json = JsonObject.create().put("t_string", id).put("t_int_min", testData.getColIntMin())
                .put("t_int_max", testData.getColIntMax()).put("t_long_min", testData.getColLongMin())
                .put("t_long_max", testData.getColLongMax()).put("t_float_min", testData.getColFloatMin())
                .put("t_float_max", testData.getColFloatMax()).put("t_double_min", testData.getColDoubleMin())
                .put("t_double_max", testData.getColDoubleMax()).put("t_boolean", testData.isColBoolean())
                .put("t_datetime", testData.getColDateTime().toString()).put("t_array", testData.getColList());
        return json;
    }

    @Test
    @DisplayName("When input data is null, record will be skipped")
    void firstValueIsNullInInputDBTest() {
        log.info("test firstValueIsNullInInputDBTest started");
        String idPrefix = "firstValueIsNullInInputDBTest";
        Bucket bucket = couchbaseCluster.openBucket(BUCKET_NAME, BUCKET_PASSWORD);
//        bucket.bucketManager().flush();
        JsonObject json = JsonObject.create().put("t_string1", "RRRR1").put("t_string2", "RRRR2").putNull("t_string3");
        bucket.insert(JsonDocument.create(generateDocId(idPrefix, 0), json));
        bucket.close();

        executeJob(getInputConfiguration());

        final List<Record> res = componentsHandler.getCollectedData(Record.class);
        assertNotNull(res);
        List<Record> data = res.stream().filter(record -> record.getString("_meta_id_").startsWith(idPrefix))
                .sorted(Comparator.comparing(r -> r.getString("_meta_id_"))).collect(Collectors.toList());

        Assertions.assertFalse(data.isEmpty());
        assertEquals(2, data.get(0).getSchema().getEntries().size());
        log.info("test firstValueIsNullInInputDBTest finished");
    }

    @Test
    @DisplayName("Execution of customN1QL query")
    void n1qlQueryInputDBTest() {
        log.info("test n1qlQueryInputDBTest started");
        String idPrefix = "n1qlQueryInputDBTest";
        insertTestDataToDB(idPrefix);

        CouchbaseInputConfiguration configurationWithN1ql = getInputConfiguration();
        configurationWithN1ql.setUseN1QLQuery(true);
        configurationWithN1ql.setQuery("SELECT `t_long_max`, `t_string`, `t_double_max` FROM `" + BUCKET_NAME + "` where meta().id like \"" + idPrefix + "%\"");
        executeJob(configurationWithN1ql);

        final List<Record> res = componentsHandler.getCollectedData(Record.class);
        assertNotNull(res);
//        List<Record> data = res.stream().filter(record -> record.getString("id").startsWith(idPrefix))
//                .sorted(Comparator.comparing(r -> r.getString("id"))).collect(Collectors.toList());

        assertEquals(2, res.size());
        assertEquals(3, res.get(0).getSchema().getEntries().size());
        assertEquals(3, res.get(1).getSchema().getEntries().size());
        log.info("test n1qlQueryInputDBTest finished");
    }

    @Test
    @DisplayName("Check input binary data")
    void inputBinaryDocumentTest() {
        log.info("test inputBinaryDocumentTest started");
        String idPrefix = "inputBinaryDocumentTest";
        String docContent = "DocumentContent";

        Bucket bucket = couchbaseCluster.openBucket(BUCKET_NAME, BUCKET_PASSWORD);
        for (int i = 0; i < 2; i++) {
            bucket.insert(
                    createBinaryDocument(generateDocId(idPrefix, i), (docContent + "_" + i).getBytes(StandardCharsets.UTF_8)));
        }
        bucket.close();

        CouchbaseInputConfiguration config = getInputConfiguration();
        config.getDataSet().setDocumentType(DocumentType.BINARY);
        executeJob(config);

        final List<Record> res = componentsHandler.getCollectedData(Record.class);

        assertNotNull(res);
        List<Record> data = res.stream().filter(record -> record.getString("id").startsWith(idPrefix))
                .sorted(Comparator.comparing(r -> r.getString("id"))).collect(Collectors.toList());
        assertEquals(2, data.size());
        for (int i = 0; i < 2; i++) {
            assertEquals(generateDocId(idPrefix, i), data.get(i).getString("id"));
            assertArrayEquals((docContent + "_" + i).getBytes(StandardCharsets.UTF_8), data.get(i).getBytes("content"));
        }
        log.info("test inputBinaryDocumentTest finished");
    }

    @Test
    @DisplayName("Check input string data")
    void inputStringDocumentTest() {
        log.info("test inputStringDocumentTest started");
        String idPrefix = "inputStringDocumentTest";
        String docContent = "DocumentContent";

        Bucket bucket = couchbaseCluster.openBucket(BUCKET_NAME, BUCKET_PASSWORD);
        for (int i = 0; i < 2; i++) {
            bucket.insert(StringDocument.create(generateDocId(idPrefix, i), (docContent + "_" + i)));
        }
        bucket.close();

        CouchbaseInputConfiguration config = getInputConfiguration();
        config.getDataSet().setDocumentType(DocumentType.STRING);
        executeJob(config);

        final List<Record> res = componentsHandler.getCollectedData(Record.class);

        assertNotNull(res);
        List<Record> data = res.stream().filter(record -> record.getString("id").startsWith(idPrefix))
                .sorted(Comparator.comparing(r -> r.getString("id"))).collect(Collectors.toList());
        assertEquals(2, data.size());
        for (int i = 0; i < 2; i++) {
            assertEquals(generateDocId(idPrefix, i), data.get(i).getString("id"));
            assertEquals((docContent + "_" + i), data.get(i).getString("content"));
        }
        log.info("test inputStringDocumentTest finished");
    }

    private BinaryDocument createBinaryDocument(String id, byte[] bytes) {
        ByteBuf toWrite = Unpooled.copiedBuffer(bytes);
        return BinaryDocument.create(id, toWrite);
    }

    private CouchbaseInputConfiguration getInputConfiguration() {
        CouchbaseDataSet couchbaseDataSet = new CouchbaseDataSet();
        couchbaseDataSet.setDatastore(couchbaseDataStore);
        couchbaseDataSet.setBucket(BUCKET_NAME);

        CouchbaseInputConfiguration configuration = new CouchbaseInputConfiguration();
        return configuration.setDataSet(couchbaseDataSet);
    }
}
