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
package org.talend.components.netsuite.source;

import com.netsuite.webservices.v2018_2.lists.accounting.types.AccountType;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.talend.components.netsuite.NetSuiteBaseTest;
import org.talend.components.netsuite.dataset.NetSuiteDataSet;
import org.talend.components.netsuite.dataset.NetSuiteInputProperties;
import org.talend.components.netsuite.dataset.SearchConditionConfiguration;
import org.talend.components.netsuite.test.TestCollector;
import org.talend.sdk.component.api.record.Record;
import org.talend.sdk.component.junit5.WithComponents;

import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

@WithComponents("org.talend.components.netsuite")
public class NetSuiteSourceTest extends NetSuiteBaseTest {

    NetSuiteInputProperties inputProperties;

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
        dataSet.setSchema(service.getSchema(dataSet, null).getEntries().stream().map(entry -> entry.getName())
                .collect(Collectors.toList()));
        inputProperties.setSearchCondition(
                Collections.singletonList(new SearchConditionConfiguration("Type", "List.anyOf", "Bank", "")));

        List<Record> records = buildAndRunEmitterJob(inputProperties);

        assertNotNull(records);
        assertEquals(AccountType.BANK.value(), records.get(0).getString("AcctType"));
    }

    @Test
    void testSearchCustomRecords() {
        clientService.getMetaDataSource().setCustomizationEnabled(true);
        dataSet.setRecordType("customrecord398");
        dataSet.setSchema(service.getSchema(dataSet, null).getEntries().stream().map(entry -> entry.getName())
                .collect(Collectors.toList()));
        inputProperties.setSearchCondition(
                Collections.singletonList(new SearchConditionConfiguration("name", "String.doesNotContain", "TUP", "")));

        List<Record> records = buildAndRunEmitterJob(inputProperties);
        assertTrue(records.size() > 1);
        records.stream().map(record -> record.get(String.class, "Name")).forEach(name -> {
            assertNotNull(name);
            assertTrue(!name.contains("TUP"));
        });
    }

    @Test
    void testSearchSublistItems() {
        assertNotNull(searchSublistItems(false));
    }

    @Test
    void testSearchSublistItemsEmpty() {
        assertNull(searchSublistItems(true));
    }

    /**
     * In terms of documentation false means get all fields, true no item lists returned
     *
     * @param bodyFieldsOnly
     */
    private String searchSublistItems(final boolean bodyFieldsOnly) {
        dataStore.setEnableCustomization(true);
        service.getClientService(dataStore).setBodyFieldsOnly(bodyFieldsOnly);
        dataSet.setRecordType("purchaseOrder");
        dataSet.setSchema(service.getSchema(dataSet, null).getEntries().stream().map(entry -> entry.getName())
                .collect(Collectors.toList()));
        inputProperties.setSearchCondition(
                Collections.singletonList(new SearchConditionConfiguration("internalId", "List.anyOf", "9", "")));
        List<Record> records = buildAndRunEmitterJob(inputProperties);
        assertEquals(1, records.size());

        return records.get(0).get(String.class, "ItemList");
    }

    @Test
    public void testSearchCustomRecordWithCustomFields() {
        clientService.getMetaDataSource().setCustomizationEnabled(true);
        dataSet.setRecordType("customrecordsearch_date_type");
        inputProperties.setSearchCondition(Collections.singletonList(
                new SearchConditionConfiguration("custrecordtemp_value_for_search", "Long.between", "100", "200")));

        List<Record> records = buildAndRunEmitterJob(inputProperties);
        assertEquals(1, records.size());
        assertTrue("FirstRecord".equals(records.get(0).get(String.class, "Name")));

    }
}
