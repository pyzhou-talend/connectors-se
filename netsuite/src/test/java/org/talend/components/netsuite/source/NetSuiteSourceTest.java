package org.talend.components.netsuite.source;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.talend.sdk.component.junit.SimpleFactory.configurationByExample;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.commons.lang3.RandomStringUtils;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.talend.components.netsuite.NetSuiteBaseTest;
import org.talend.components.netsuite.dataset.NetSuiteDataSet;
import org.talend.components.netsuite.dataset.NetSuiteInputProperties;
import org.talend.components.netsuite.dataset.SearchConditionConfiguration;
import org.talend.components.netsuite.test.TestCollector;
import org.talend.sdk.component.api.record.Record;
import org.talend.sdk.component.api.record.Schema;
import org.talend.sdk.component.junit5.WithComponents;
import org.talend.sdk.component.runtime.manager.chain.Job;

import com.netsuite.webservices.v2018_2.lists.accounting.types.AccountType;

@WithComponents("org.talend.components.netsuite")
public class NetSuiteSourceTest extends NetSuiteBaseTest {

    NetSuiteInputProperties inputProperties;

    String randomName = "TestIT_" + RandomStringUtils.randomAlphanumeric(10);

    Schema schema;

    @BeforeEach
    public void setup() {
        inputProperties = new NetSuiteInputProperties();
        dataSet = new NetSuiteDataSet();
        dataSet.setDataStore(dataStore);
        inputProperties.setDataSet(dataSet);
        TestCollector.reset();
    }

    @Test
    void testSearchBankAccounts() {
        dataSet.setRecordType("Account");
        schema = service.getSchema(dataSet);
        dataSet.setSchema(schema.getEntries().stream().map(entry -> entry.getName()).collect(Collectors.toList()));
        SearchConditionConfiguration searchCondition = new SearchConditionConfiguration("Type", "List.anyOf", "Bank", "");
        inputProperties.setSearchCondition(Collections.singletonList(searchCondition));

        String inputConfig = configurationByExample().forInstance(inputProperties).configured().toQueryString();
        Job.components().component("nsEmitter", "NetSuite://Input?" + inputConfig)
                .component("collector", "NetSuiteTest://TestCollector").connections().from("nsEmitter").to("collector").build()
                .run();

        List<Record> records = new ArrayList<>(TestCollector.getData());

        assertNotNull(records);
        assertEquals(AccountType.BANK.value(), records.get(0).getString("AcctType"));
    }

    @Test
    void testSearchCustomRecords() {
        dataStore.setEnableCustomization(true);
        dataSet.setRecordType("customrecord398");
        schema = service.getSchema(dataSet);
        dataSet.setSchema(schema.getEntries().stream().map(entry -> entry.getName()).collect(Collectors.toList()));
        SearchConditionConfiguration searchCondition = new SearchConditionConfiguration("name", "String.doesNotContain", "TUP",
                "");
        inputProperties.setSearchCondition(Collections.singletonList(searchCondition));
        String inputConfig = configurationByExample().forInstance(inputProperties).configured().toQueryString();

        Job.components().component("nsEmitter", "NetSuite://Input?" + inputConfig)
                .component("collector", "NetSuiteTest://TestCollector").connections().from("nsEmitter").to("collector").build()
                .run();

        List<Record> records = new ArrayList<>(TestCollector.getData());

        assertNotNull(records);
        assertTrue(records.size() > 1);
        records.stream().map(record -> record.get(String.class, "Name")).forEach(name -> {
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
        dataSet.setRecordType("purchaseOrder");
        schema = service.getSchema(dataSet);
        dataSet.setSchema(schema.getEntries().stream().map(entry -> entry.getName()).collect(Collectors.toList()));
        SearchConditionConfiguration searchCondition = new SearchConditionConfiguration("internalId", "List.anyOf", "9", "");
        inputProperties.setSearchCondition(Collections.singletonList(searchCondition));

        String inputConfig = configurationByExample().forInstance(inputProperties).configured().toQueryString();

        Job.components().component("nsEmitter", "NetSuite://Input?" + inputConfig)
                .component("collector", "NetSuiteTest://TestCollector").connections().from("nsEmitter").to("collector").build()
                .run();

        List<Record> records = new ArrayList<>(TestCollector.getData());

        assertNotNull(records);
        assertTrue(records.size() == 1);

        String itemList = records.get(0).get(String.class, "ItemList");
        if (bodyFieldsOnly) {
            assertNull(itemList);
        } else {
            assertNotNull(itemList);
        }
    }
}
