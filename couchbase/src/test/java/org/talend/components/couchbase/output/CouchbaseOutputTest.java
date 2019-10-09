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
package org.talend.components.couchbase.output;

import com.couchbase.client.deps.io.netty.util.ReferenceCountUtil;
import com.couchbase.client.java.Bucket;
import com.couchbase.client.java.document.BinaryDocument;
import com.couchbase.client.java.document.JsonDocument;
import com.couchbase.client.java.document.json.JsonObject;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.talend.components.couchbase.CouchbaseUtilTest;
import org.talend.components.couchbase.TestData;
import org.talend.components.couchbase.dataset.CouchbaseDataSet;
import org.talend.components.couchbase.dataset.DocumentType;
import org.talend.sdk.component.api.record.Record;
import org.talend.sdk.component.api.record.Schema;
import org.talend.sdk.component.api.service.Service;
import org.talend.sdk.component.api.service.record.RecordBuilderFactory;
import org.talend.sdk.component.junit.BaseComponentsHandler;
import org.talend.sdk.component.junit5.Injected;
import org.talend.sdk.component.junit5.WithComponents;
import org.talend.sdk.component.runtime.manager.chain.Job;
import org.talend.sdk.component.runtime.record.SchemaImpl;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.talend.components.couchbase.source.CouchbaseInput.META_ID_FIELD;
import static org.talend.sdk.component.junit.SimpleFactory.configurationByExample;

@WithComponents("org.talend.components.couchbase")
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@DisplayName("Testing of CouchbaseOutput component")
public class CouchbaseOutputTest extends CouchbaseUtilTest {

    @Injected
    private BaseComponentsHandler componentsHandler;

    @Service
    private RecordBuilderFactory recordBuilderFactory;

    private final String SIMPLE_OUTPUT_TEST_ID = "simpleOutputTest";

    private List<JsonDocument> retrieveDataFromDatabase(String prefix, int count) {
        Bucket bucket = couchbaseCluster.openBucket(BUCKET_NAME, BUCKET_PASSWORD);
        List<JsonDocument> resultList = new ArrayList<>();
        for (int i = 0; i < count; i++) {
            JsonDocument doc1 = bucket.get(generateDocId(prefix, i));
            doc1.content().put(META_ID_FIELD, generateDocId(prefix, i));
            resultList.add(doc1);
        }
        bucket.close();
        return resultList;
    }

    private void executeJob(CouchbaseOutputConfiguration configuration) {
        final String outputConfig = configurationByExample().forInstance(configuration).configured().toQueryString();
        Job.components().component("Couchbase_Output", "Couchbase://Output?" + outputConfig)
                .component("emitter", "test://emitter").connections().from("emitter").to("Couchbase_Output").build().run();
    }

    @Test
    @DisplayName("Check fields from retrieved data")
    void simpleOutputTest() {
        List<Record> records = createRecords();
        componentsHandler.setInputData(records);
        executeJob(getOutputConfiguration());

        List<JsonDocument> resultList = retrieveDataFromDatabase(SIMPLE_OUTPUT_TEST_ID, 2);
        TestData testData = new TestData();

        assertEquals(new Integer(testData.getColIntMin()), resultList.get(0).content().getInt("t_int_min"));
        assertEquals(new Integer(testData.getColIntMax()), resultList.get(0).content().getInt("t_int_max"));
        assertEquals(new Long(testData.getColLongMin()), resultList.get(0).content().getLong("t_long_min"));
        assertEquals(new Long(testData.getColLongMax()), resultList.get(0).content().getLong("t_long_max"));
        assertEquals(testData.getColFloatMin(), resultList.get(0).content().getNumber("t_float_min").floatValue());
        assertEquals(testData.getColFloatMax(), resultList.get(0).content().getNumber("t_float_max").floatValue());
        assertEquals(testData.getColDoubleMin(), resultList.get(0).content().getDouble("t_double_min"));
        assertEquals(testData.getColDoubleMax(), resultList.get(0).content().getDouble("t_double_max"));
        assertEquals(testData.isColBoolean(), resultList.get(0).content().getBoolean("t_boolean"));
        assertEquals(testData.getColDateTime().toString(), resultList.get(0).content().getString("t_datetime"));
        Assertions.assertArrayEquals(testData.getColList().toArray(),
                resultList.get(0).content().getArray("t_array").toList().toArray());

        assertEquals(2, resultList.size());
    }

    private List<Record> createRecords() {
        List<Record> records = new ArrayList<>();
        for (int i = 0; i < 2; i++) {
            records.add(createRecord(generateDocId(SIMPLE_OUTPUT_TEST_ID, i)));
        }
        return records;
    }

    private Record createRecord(String id) {
        TestData testData = new TestData();

        final Schema.Entry.Builder entryBuilder = recordBuilderFactory.newEntryBuilder();
        SchemaImpl arrayInnerSchema = new SchemaImpl();
        arrayInnerSchema.setType(Schema.Type.STRING);

        Record record = recordBuilderFactory.newRecordBuilder()
                .withString(entryBuilder.withName("t_string").withType(Schema.Type.STRING).build(), id)
                .withInt(entryBuilder.withName("t_int_min").withType(Schema.Type.INT).build(), testData.getColIntMin())
                .withInt(entryBuilder.withName("t_int_max").withType(Schema.Type.INT).build(), testData.getColIntMax())
                .withLong(entryBuilder.withName("t_long_min").withType(Schema.Type.LONG).build(), testData.getColLongMin())
                .withLong(entryBuilder.withName("t_long_max").withType(Schema.Type.LONG).build(), testData.getColLongMax())
                .withFloat(entryBuilder.withName("t_float_min").withType(Schema.Type.FLOAT).build(), testData.getColFloatMin())
                .withFloat(entryBuilder.withName("t_float_max").withType(Schema.Type.FLOAT).build(), testData.getColFloatMax())
                .withDouble(entryBuilder.withName("t_double_min").withType(Schema.Type.DOUBLE).build(),
                        testData.getColDoubleMin())
                .withDouble(entryBuilder.withName("t_double_max").withType(Schema.Type.DOUBLE).build(),
                        testData.getColDoubleMax())
                .withBoolean(entryBuilder.withName("t_boolean").withType(Schema.Type.BOOLEAN).build(), testData.isColBoolean())
                .withDateTime(entryBuilder.withName("t_datetime").withType(Schema.Type.DATETIME).build(),
                        testData.getColDateTime())
                .withArray(
                        entryBuilder.withName("t_array").withType(Schema.Type.ARRAY).withElementSchema(arrayInnerSchema).build(),
                        testData.getColList())
                .build();
        return record;
    }

    @Test
    @DisplayName("Check binary document output")
    void outputBinaryTest() {
        String idPrefix = "outputBinaryDocumentTest";
        String docContent = "DocumentContent";
        int docCount = 2;

        List<Record> records = new ArrayList<>();
        final Schema.Entry.Builder entryBuilder = recordBuilderFactory.newEntryBuilder();
        for (int i = 0; i < docCount; i++) {
            Record record = recordBuilderFactory.newRecordBuilder()
                    .withString(entryBuilder.withName("id").withType(Schema.Type.STRING).build(), generateDocId(idPrefix, i))
                    .withBytes(entryBuilder.withName("content").withType(Schema.Type.BYTES).build(),
                            (docContent + "_" + i).getBytes(StandardCharsets.UTF_8))
                    .build();
            records.add(record);
        }

        componentsHandler.setInputData(records);
        CouchbaseOutputConfiguration configuration = getOutputConfiguration();
        configuration.getDataSet().setDocumentType(DocumentType.BINARY);
        configuration.setIdFieldName("id");
        executeJob(configuration);

        Bucket bucket = couchbaseCluster.openBucket(BUCKET_NAME, BUCKET_PASSWORD);
        List<BinaryDocument> resultList = new ArrayList<>();
        try {
            for (int i = 0; i < docCount; i++) {
                BinaryDocument doc = bucket.get(generateDocId(idPrefix, i), BinaryDocument.class);
                resultList.add(doc);
            }
        } finally {
            bucket.close();
        }

        assertEquals(2, resultList.size());
        for (int i = 0; i < docCount; i++) {
            BinaryDocument doc = resultList.get(i);
            byte[] data = new byte[doc.content().readableBytes()];
            doc.content().readBytes(data);
            ReferenceCountUtil.release(doc.content());
            assertArrayEquals((docContent + "_" + i).getBytes(StandardCharsets.UTF_8), data);
        }
    }

    private List<Record> createRecordsForN1QL(String idPrefix) {
        final Schema.Entry.Builder entryBuilder = recordBuilderFactory.newEntryBuilder();
        return Stream.iterate(0, op -> op + 1).limit(5).map(idx -> recordBuilderFactory.newRecordBuilder()
                .withString(entryBuilder.withName("docId").withType(Schema.Type.STRING).build(), generateDocId(idPrefix, idx))
                .withString(entryBuilder.withName("key1").withType(Schema.Type.STRING).build(), "ZzZ" + idx)
                .withString(entryBuilder.withName("key2").withType(Schema.Type.STRING).build(), "ZzTop" + idx)
                .withString(entryBuilder.withName("key3").withType(Schema.Type.STRING).build(), "KEY_3")
                .withInt(entryBuilder.withName("count").withType(Schema.Type.INT).build(), idx).build())
                .collect(Collectors.toList());
    }

    @Test
    @DisplayName("Simple N1QL query with no parameters")
    void executeSimpleN1QLQueryWithNoParameters() {
        final String N1QL_WITH_NO_PARAMETERS_ID_PREFIX = "n1qlWithNoParametersIdPrefix";
        CouchbaseOutputConfiguration configuration = getOutputConfiguration();
        configuration.setUseN1QLQuery(true);
        String qry = String.format(
                "UPSERT INTO `%s` (KEY, VALUE) VALUES (\"" + generateDocId(N1QL_WITH_NO_PARAMETERS_ID_PREFIX, 0)
                        + "\", {\"key1\": \"masterkey1\", \"key2\": \"masterkey2\", \"key3\": \"masterkey3\", \"count\": 19})",
                BUCKET_NAME);
        configuration.setQuery(qry);
        componentsHandler.setInputData(createRecordsForN1QL(""));
        executeJob(configuration);
        List<JsonDocument> resultList = retrieveDataFromDatabase(N1QL_WITH_NO_PARAMETERS_ID_PREFIX, 1);
        assertEquals(1, resultList.size());
        JsonObject result = resultList.get(0).content();
        assertEquals(generateDocId(N1QL_WITH_NO_PARAMETERS_ID_PREFIX, 0), result.getString(META_ID_FIELD));
        assertEquals("masterkey1", result.getString("key1"));
        assertEquals("masterkey2", result.getString("key2"));
        assertEquals("masterkey3", result.getString("key3"));
        assertEquals(19, result.getInt("count"));
    }

    @Test
    @DisplayName("N1QL query with parameters")
    void executeSimpleN1QLQueryWithParameters() {
        final String N1QL_WITH_PARAMETERS_ID_PREFIX = "N1qlWithParameters";
        CouchbaseOutputConfiguration configuration = getOutputConfiguration();
        configuration.setUseN1QLQuery(true);
        String qry = String.format(
                "INSERT INTO `%s` (KEY, VALUE) VALUES ($id, {\"key1\": $k1, \"key2\": $k2, " + "\"key3\": $k3, \"count\": $cn})",
                BUCKET_NAME);
        configuration.setQuery(qry);
        List<N1QLQueryParameter> params = new ArrayList<>();
        params.add(new N1QLQueryParameter("id", "docId"));
        params.add(new N1QLQueryParameter("k1", "key1"));
        params.add(new N1QLQueryParameter("k2", "key2"));
        params.add(new N1QLQueryParameter("k3", "key3"));
        params.add(new N1QLQueryParameter("cn", "count"));
        configuration.setQueryParams(params);
        componentsHandler.setInputData(createRecordsForN1QL(N1QL_WITH_PARAMETERS_ID_PREFIX));
        executeJob(configuration);
        List<JsonDocument> resultList = retrieveDataFromDatabase(N1QL_WITH_PARAMETERS_ID_PREFIX, 5);
        assertEquals(5, resultList.size());
        Stream.iterate(0, o -> o + 1).limit(5).forEach(idx -> {
            JsonObject result = resultList.get(idx).content();
            assertEquals(generateDocId(N1QL_WITH_PARAMETERS_ID_PREFIX, idx), result.getString(META_ID_FIELD));
            assertEquals("ZzZ" + idx, result.getString("key1"));
            assertEquals("ZzTop" + idx, result.getString("key2"));
            assertEquals("KEY_3", result.getString("key3"));
            assertEquals(idx, result.getInt("count"));
        });
    }

    private List<Record> createPartialUpdateRecords() {
        final Schema.Entry.Builder entryBuilder = recordBuilderFactory.newEntryBuilder();
        List<Record> records = new ArrayList<>();
        Record record1 = recordBuilderFactory.newRecordBuilder()
                .withString(entryBuilder.withName("t_string").withType(Schema.Type.STRING).build(),
                        generateDocId(SIMPLE_OUTPUT_TEST_ID, 0))
                .withInt(entryBuilder.withName("t_int_min").withType(Schema.Type.INT).build(), 1971)
                .withString(entryBuilder.withName("extra_content").withType(Schema.Type.STRING).build(), "path new").build();
        Record record2 = recordBuilderFactory.newRecordBuilder()
                .withString(entryBuilder.withName("t_string").withType(Schema.Type.STRING).build(),
                        generateDocId(SIMPLE_OUTPUT_TEST_ID, 1))
                .withBoolean(entryBuilder.withName("t_boolean").withType(Schema.Type.BOOLEAN).build(), Boolean.FALSE)
                .withString(entryBuilder.withName("extra_content2").withType(Schema.Type.STRING).build(), "path zap").build();
        records.add(record1);
        records.add(record2);

        return records;
    }

    @Test
    @DisplayName("Document partial update")
    void partialUpdate() {
        CouchbaseOutputConfiguration config = getOutputConfiguration();
        config.setPartialUpdate(true);
        componentsHandler.setInputData(createPartialUpdateRecords());
        executeJob(config);
        //
        List<JsonDocument> resultList = retrieveDataFromDatabase(SIMPLE_OUTPUT_TEST_ID, 2);
        assertEquals(2, resultList.size());
        TestData testData = new TestData();
        Stream.iterate(0, o -> o + 1).limit(2).forEach(idx -> {
            // untouched properties
            assertEquals(new Integer(testData.getColIntMax()), resultList.get(idx).content().getInt("t_int_max"));
            assertEquals(new Long(testData.getColLongMin()), resultList.get(idx).content().getLong("t_long_min"));
            assertEquals(new Long(testData.getColLongMax()), resultList.get(idx).content().getLong("t_long_max"));
            assertEquals(testData.getColFloatMin(), resultList.get(idx).content().getDouble("t_float_min"), 1E35);
            assertEquals(testData.getColFloatMax(), resultList.get(idx).content().getDouble("t_float_max"), 1E35);
            assertEquals(testData.getColDoubleMin(), resultList.get(idx).content().getDouble("t_double_min"), 1);
            assertEquals(testData.getColDoubleMax(), resultList.get(idx).content().getDouble("t_double_max"), 1);
            assertEquals(testData.getColDateTime().toString(), resultList.get(idx).content().getString("t_datetime"));
            assertArrayEquals(testData.getColList().toArray(),
                    resultList.get(idx).content().getArray("t_array").toList().toArray());
            // upserted proterties
            if (idx == 0) {
                assertEquals(1971, resultList.get(idx).content().getInt("t_int_min"));
                assertEquals(testData.isColBoolean(), resultList.get(idx).content().getBoolean("t_boolean"));
                assertEquals("path new", resultList.get(idx).content().getString("extra_content"));
                assertNull(resultList.get(idx).content().getString("extra_content2"));
            } else {
                assertEquals(new Integer(testData.getColIntMin()), resultList.get(idx).content().getInt("t_int_min"));
                assertEquals(Boolean.FALSE, resultList.get(idx).content().getBoolean("t_boolean"));
                assertEquals("path zap", resultList.get(idx).content().getString("extra_content2"));
                assertNull(resultList.get(idx).content().getString("extra_content"));
            }
        });
    }

    private CouchbaseOutputConfiguration getOutputConfiguration() {
        CouchbaseDataSet couchbaseDataSet = new CouchbaseDataSet();
        couchbaseDataSet.setBucket(BUCKET_NAME);
        couchbaseDataSet.setDatastore(couchbaseDataStore);

        CouchbaseOutputConfiguration configuration = new CouchbaseOutputConfiguration();
        configuration.setIdFieldName("t_string");
        configuration.setDataSet(couchbaseDataSet);
        return configuration;
    }
}