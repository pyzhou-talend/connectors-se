package org.talend.components.netsuite.source;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.IndexedRecord;
import org.apache.commons.lang3.RandomStringUtils;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.talend.components.netsuite.NetsuiteBaseTest;
import org.talend.components.netsuite.dataset.NetSuiteCommonDataSet;
import org.talend.components.netsuite.dataset.NetsuiteInputDataSet;
import org.talend.components.netsuite.dataset.NetsuiteOutputDataSet;
import org.talend.components.netsuite.dataset.NetsuiteOutputDataSet.DataAction;
import org.talend.components.netsuite.dataset.SearchConditionConfiguration;
import org.talend.components.netsuite.processor.NetsuiteOutputProcessor;
import org.talend.sdk.component.junit5.WithComponents;
import org.talend.sdk.component.runtime.input.Mapper;

@WithComponents("org.talend.components.netsuite")
public class NetsuiteSourceTest extends NetsuiteBaseTest {

    NetsuiteInputDataSet dataSet;

    String randomName = "TestIT_" + RandomStringUtils.randomAlphanumeric(10);

    Schema schema;

    private NetSuiteCommonDataSet commonDataSet;

    @BeforeEach
    public void setup() {
        dataSet = new NetsuiteInputDataSet();
        commonDataSet = new NetSuiteCommonDataSet(dataStore, "Subsidiary");
        dataSet.setBodyFieldsOnly(false);

        dataSet.setSchema(service.getSchema(dataSet.getCommonDataSet()).stream().map(entry -> entry.getName())
                .collect(Collectors.toList()));
        schema = service.getAvroSchema(dataSet.getCommonDataSet());
    }

    @Test
    void testGetAccountRecords() {
        NetsuiteOutputDataSet configuration = new NetsuiteOutputDataSet();
        configuration.setCommonDataSet(commonDataSet);
        configuration.setAction(DataAction.ADD);
        configuration.setBatchSize(1);
        configuration.setSchemaIn(Arrays.asList("Country", "MainAddress", "Name", "State"));

        NetsuiteOutputProcessor processor = new NetsuiteOutputProcessor(configuration, service);
        processor.init();
        IndexedRecord ir = new GenericData.Record(schema);
        ir.put(schema.getField("Country").pos(), "_unitedStates");
        ir.put(schema.getField("MainAddress").pos(),
                "{\"country\": \"_unitedStates\",\"addressee\": \"Anchorage\",\"addr1\": \"Boulevard of Broken Dreams 2\",\"city\": \"Anchorage\",\"zip\": \"99501\"}");
        ir.put(schema.getField("Name").pos(), randomName);
        ir.put(schema.getField("State").pos(), "CA");
        processor.onNext(ir, null, null);
        SearchConditionConfiguration searchCondition = new SearchConditionConfiguration();
        searchCondition.setField("name");
        searchCondition.setOperator("String.contains");
        searchCondition.setValue(randomName);
        searchCondition.setValue2("");
        dataSet.setSearchCondition(Collections.singletonList(searchCondition));
        Mapper mapper = COMPONENT.createMapper(NetsuiteInputMapper.class, dataSet);
        List<IndexedRecord> records = COMPONENT.collectAsList(IndexedRecord.class, mapper, 5);
        assertNotNull(records);
        assertEquals(1, records.size());
        IndexedRecord record = records.get(0);
        assertEquals(randomName, record.get(schema.getField("Name").pos()));
        String id = (String) record.get(schema.getField("InternalId").pos());

        configuration.setAction(DataAction.DELETE);
        configuration.setSchemaIn(Arrays.asList("InternalId"));
        processor = new NetsuiteOutputProcessor(configuration, service);
        processor.init();
        ir = new GenericData.Record(schema);
        ir.put(schema.getField("InternalId").pos(), id);
        processor.onNext(ir, null, null);
    }

}
