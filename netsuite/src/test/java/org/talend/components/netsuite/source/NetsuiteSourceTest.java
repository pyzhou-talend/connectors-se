package org.talend.components.netsuite.source;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

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

import com.netsuite.webservices.v2018_2.lists.accounting.types.AccountType;

@WithComponents("org.talend.components.netsuite")
public class NetsuiteSourceTest extends NetsuiteBaseTest {

    NetsuiteInputDataSet dataSet;

    String randomName = "TestIT_" + RandomStringUtils.randomAlphanumeric(10);

    Schema schema;

    @BeforeEach
    public void setup() {
        dataSet = new NetsuiteInputDataSet();
        commonDataSet = new NetSuiteCommonDataSet();
        commonDataSet.setDataStore(dataStore);
        dataSet.setCommonDataSet(commonDataSet);

    }

    // @Test "no permissions for searching Subsidiary"
    void testGetAccountRecords() {
        commonDataSet.setRecordType("Subsidiary");
        dataSet.setSchema(service.getSchema(commonDataSet).stream().map(entry -> entry.getName()).collect(Collectors.toList()));
        schema = service.getAvroSchema(commonDataSet);
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
        SearchConditionConfiguration searchCondition = new SearchConditionConfiguration("name", "String.contains", randomName,
                "");
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

    @Test
    void testSearchBankAccounts() {
        commonDataSet.setRecordType("Account");
        dataSet.setSchema(service.getSchema(commonDataSet).stream().map(entry -> entry.getName()).collect(Collectors.toList()));
        schema = service.getAvroSchema(commonDataSet);
        SearchConditionConfiguration searchCondition = new SearchConditionConfiguration("Type", "List.anyOf", "Bank", "");
        dataSet.setSearchCondition(Collections.singletonList(searchCondition));

        Mapper mapper = COMPONENT.createMapper(NetsuiteInputMapper.class, dataSet);
        List<IndexedRecord> records = COMPONENT.collectAsList(IndexedRecord.class, mapper);

        assertNotNull(records);
        records.stream().map(record -> (String) record.get(schema.getField("AcctType").pos())).forEach(accType -> {
            assertNotNull(accType);
            assertEquals(AccountType.BANK.value(), accType);
        });
    }

    @Test
    void testSearchCustomRecords() {
        dataStore.setEnableCustomization(true);
        commonDataSet.setRecordType("customrecord398");
        dataSet.setSchema(service.getSchema(commonDataSet).stream().map(entry -> entry.getName()).collect(Collectors.toList()));
        schema = service.getAvroSchema(commonDataSet);
        SearchConditionConfiguration searchCondition = new SearchConditionConfiguration("name", "String.doesNotContain", "TUP",
                "");
        dataSet.setSearchCondition(Collections.singletonList(searchCondition));

        Mapper mapper = COMPONENT.createMapper(NetsuiteInputMapper.class, dataSet);
        List<IndexedRecord> records = COMPONENT.collectAsList(IndexedRecord.class, mapper);

        assertNotNull(records);
        assertTrue(records.size() > 1);
        records.stream().map(record -> (String) record.get(schema.getField("Name").pos())).forEach(name -> {
            assertNotNull(name);
            assertTrue(!name.contains("TUP"));
        });
    }

    @Test
    void testSearchSublistItems() {
        searchSublistItems(false);
    }

    @Test
    void testSearchSublistItemsEmpty() {
        searchSublistItems(true);
    }

    /**
     * In terms of documentation false means get all fields, true no item lists returned
     *
     * @param bodyFieldsOnly
     */
    private void searchSublistItems(final boolean bodyFieldsOnly) {
        dataStore.setEnableCustomization(true);
        service.getClientService(dataStore).setBodyFieldsOnly(bodyFieldsOnly);
        commonDataSet.setRecordType("purchaseOrder");
        dataSet.setSchema(service.getSchema(commonDataSet).stream().map(entry -> entry.getName()).collect(Collectors.toList()));
        schema = service.getAvroSchema(commonDataSet);
        SearchConditionConfiguration searchCondition = new SearchConditionConfiguration("internalId", "List.anyOf", "9", "");
        dataSet.setSearchCondition(Collections.singletonList(searchCondition));

        Mapper mapper = COMPONENT.createMapper(NetsuiteInputMapper.class, dataSet);
        List<IndexedRecord> records = COMPONENT.collectAsList(IndexedRecord.class, mapper);

        assertNotNull(records);
        assertTrue(records.size() == 1);
        String itemList = (String) records.get(0).get(schema.getField("ItemList").pos());
        if (bodyFieldsOnly) {
            assertNull(itemList);
        } else {
            assertNotNull(itemList);
        }
    }
}
