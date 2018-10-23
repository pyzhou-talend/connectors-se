package org.talend.components.netsuite.source;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.talend.sdk.component.junit.SimpleFactory.configurationByExample;

import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.commons.lang3.RandomStringUtils;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.talend.components.netsuite.NetsuiteBaseTest;
import org.talend.components.netsuite.dataset.NetSuiteCommonDataSet;
import org.talend.components.netsuite.dataset.NetsuiteInputDataSet;
import org.talend.components.netsuite.dataset.SearchConditionConfiguration;
import org.talend.sdk.component.api.record.Record;
import org.talend.sdk.component.api.record.Schema;
import org.talend.sdk.component.junit5.WithComponents;
import org.talend.sdk.component.runtime.manager.chain.Job;

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

    @Test
    void testSearchBankAccounts() {
        commonDataSet.setRecordType("Account");
        schema = service.getSchema(commonDataSet);
        dataSet.getCommonDataSet()
                .setSchema(schema.getEntries().stream().map(entry -> entry.getName()).collect(Collectors.toList()));
        SearchConditionConfiguration searchCondition = new SearchConditionConfiguration("Type", "List.anyOf", "Bank", "");
        dataSet.setSearchCondition(Collections.singletonList(searchCondition));

        String inputConfig = configurationByExample().forInstance(dataSet).configured().toQueryString();
        Job.components().component("nsEmitter", "Netsuite://Input?" + inputConfig).component("collector", "test://collector")
                .connections().from("nsEmitter").to("collector").build().run();

        List<Record> records = COMPONENT.getCollectedData(Record.class);

        assertNotNull(records);
        assertEquals(AccountType.BANK.value(), records.get(0).getString("AcctType"));
    }

    @Test
    void testSearchCustomRecords() {
        dataStore.setEnableCustomization(true);
        commonDataSet.setRecordType("customrecord398");
        schema = service.getSchema(commonDataSet);
        dataSet.getCommonDataSet()
                .setSchema(schema.getEntries().stream().map(entry -> entry.getName()).collect(Collectors.toList()));
        SearchConditionConfiguration searchCondition = new SearchConditionConfiguration("name", "String.doesNotContain", "TUP",
                "");
        dataSet.setSearchCondition(Collections.singletonList(searchCondition));
        String inputConfig = configurationByExample().forInstance(dataSet).configured().toQueryString();

        Job.components().component("nsEmitter", "Netsuite://Input?" + inputConfig).component("collector", "test://collector")
                .connections().from("nsEmitter").to("collector").build().run();

        List<Record> records = COMPONENT.getCollectedData(Record.class);

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
        commonDataSet.setRecordType("purchaseOrder");
        schema = service.getSchema(commonDataSet);
        dataSet.getCommonDataSet()
                .setSchema(schema.getEntries().stream().map(entry -> entry.getName()).collect(Collectors.toList()));
        SearchConditionConfiguration searchCondition = new SearchConditionConfiguration("internalId", "List.anyOf", "9", "");
        dataSet.setSearchCondition(Collections.singletonList(searchCondition));

        String inputConfig = configurationByExample().forInstance(dataSet).configured().toQueryString();

        Job.components().component("nsEmitter", "Netsuite://Input?" + inputConfig).component("collector", "test://collector")
                .connections().from("nsEmitter").to("collector").build().run();

        List<Record> records = COMPONENT.getCollectedData(Record.class);

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
