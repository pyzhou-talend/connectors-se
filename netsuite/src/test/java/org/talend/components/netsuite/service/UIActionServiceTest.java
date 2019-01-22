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
package org.talend.components.netsuite.service;

import static java.util.stream.Collectors.toList;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertIterableEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.lang.reflect.Field;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;
import java.util.stream.Stream;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.talend.components.netsuite.NetSuiteBaseTest;
import org.talend.components.netsuite.dataset.NetSuiteDataSet;
import org.talend.components.netsuite.dataset.NetSuiteOutputProperties;
import org.talend.components.netsuite.datastore.NetSuiteDataStore;
import org.talend.components.netsuite.datastore.NetSuiteDataStore.ApiVersion;
import org.talend.components.netsuite.datastore.NetSuiteDataStore.LoginType;
import org.talend.components.netsuite.runtime.model.search.SearchFieldOperatorName;
import org.talend.components.netsuite.runtime.model.search.SearchFieldOperatorType;
import org.talend.components.netsuite.runtime.model.search.SearchFieldOperatorTypeDesc;
import org.talend.components.netsuite.runtime.v2018_2.model.RecordTypeEnum;
import org.talend.sdk.component.api.record.Schema;
import org.talend.sdk.component.api.service.Service;
import org.talend.sdk.component.api.service.completion.SuggestionValues;
import org.talend.sdk.component.api.service.completion.SuggestionValues.Item;
import org.talend.sdk.component.api.service.healthcheck.HealthCheckStatus;
import org.talend.sdk.component.junit5.WithComponents;

import com.netsuite.webservices.v2018_2.platform.common.AccountSearchBasic;
import com.netsuite.webservices.v2018_2.platform.common.CustomRecordSearchBasic;
import com.netsuite.webservices.v2018_2.platform.common.TransactionSearchBasic;
import com.netsuite.webservices.v2018_2.platform.core.types.SearchDate;
import com.netsuite.webservices.v2018_2.platform.core.types.SearchDateFieldOperator;
import com.netsuite.webservices.v2018_2.platform.core.types.SearchMultiSelectFieldOperator;
import com.netsuite.webservices.v2018_2.transactions.purchases.PurchaseOrder;

@WithComponents("org.talend.components.netsuite")
public class UIActionServiceTest extends NetSuiteBaseTest {

    @Service
    private UIActionService uiActionService;

    @BeforeEach
    public void refresh() {
        dataStore.setEnableCustomization(false);
    }

    @Test
    public void testHealthCheck() {
        assertEquals(HealthCheckStatus.Status.OK, uiActionService.validateConnection(dataStore).getStatus());
    }

    @Test
    public void testHealthCheckFailed() {

        NetSuiteDataStore dataStoreWrong = new NetSuiteDataStore();
        dataStoreWrong.setLoginType(LoginType.BASIC);
        dataStoreWrong.setRole(3);
        dataStoreWrong.setAccount(System.getProperty("netsuite.account"));
        dataStoreWrong.setEndpoint(System.getProperty("netsuite.endpoint.url"));
        dataStoreWrong.setEmail("test_junit@talend.com");
        dataStoreWrong.setPassword("wrongPassword");
        dataStoreWrong.setApplicationId(UUID.randomUUID().toString());
        dataStoreWrong.setApiVersion(ApiVersion.V2018_2);
        assertEquals(HealthCheckStatus.Status.KO, uiActionService.validateConnection(dataStoreWrong).getStatus());
    }

    @Test
    public void testGuessSchema() {
        List<String> fields = Arrays.asList(PurchaseOrder.class.getDeclaredFields()).stream()
                .map(field -> field.getName().toLowerCase())
                .filter(field -> !field.equals("customfieldlist") && !field.equals("nullfieldlist")).collect(toList());
        NetSuiteOutputProperties outputDataSet = new NetSuiteOutputProperties();
        NetSuiteDataSet dataSet = new NetSuiteDataSet();
        dataSet.setDataStore(dataStore);
        dataSet.setRecordType("PurchaseOrder");
        outputDataSet.setDataSet(dataSet);
        // For custom fields need to re-create connection (might be a case for refactoring of Service)
        dataStore.setEnableCustomization(true);
        uiActionService.validateConnection(dataStore);

        Schema schema = uiActionService.guessSchema(outputDataSet.getDataSet());
        // In case of customization count of entries in schema must be more than actual class fields.
        assertTrue(fields.size() < schema.getEntries().size());
    }

    @Test
    public void testLoadRecordTypes() {
        List<String> expectedList = Arrays.asList(RecordTypeEnum.values()).stream().map(RecordTypeEnum::getTypeName).sorted()
                .collect(toList());
        uiActionService.validateConnection(dataStore);
        SuggestionValues values = uiActionService.loadRecordTypes(dataStore);
        List<String> actualList = values.getItems().stream().map(Item::getLabel).sorted().collect(toList());

        assertIterableEquals(expectedList, actualList);
    }

    @Test
    public void testLoadCustomRecordTypes() {
        dataStore.setEnableCustomization(true);
        uiActionService.validateConnection(dataStore);
        SuggestionValues values = uiActionService.loadRecordTypes(dataStore);

        // For enabled customization we must have more record types. (even for passing other tests, they must be
        // defined)
        assertTrue(RecordTypeEnum.values().length < values.getItems().size());
    }

    @Test
    public void testLoadFields() {
        List<String> expectedList = Arrays.asList(AccountSearchBasic.class.getDeclaredFields()).stream().map(Field::getName)
                .sorted().collect(toList());

        NetSuiteDataSet commonDataSet = new NetSuiteDataSet();
        commonDataSet.setDataStore(dataStore);
        commonDataSet.setRecordType("Account");

        uiActionService.validateConnection(dataStore);
        SuggestionValues values = uiActionService.loadFields(commonDataSet);
        List<String> actualList = values.getItems().stream().map(Item::getLabel).sorted().collect(toList());
        assertIterableEquals(expectedList, actualList);
    }

    @Test
    public void testLoadCustomSearchFields() {
        List<String> expectedList = Arrays.asList(CustomRecordSearchBasic.class.getDeclaredFields()).stream().map(Field::getName)
                .sorted().collect(toList());

        dataStore.setEnableCustomization(true);
        NetSuiteDataSet commonDataSet = new NetSuiteDataSet();
        commonDataSet.setDataStore(dataStore);
        commonDataSet.setRecordType("customrecordqacomp_custom_recordtype");

        uiActionService.validateConnection(dataStore);
        SuggestionValues values = uiActionService.loadFields(commonDataSet);
        List<String> actualList = values.getItems().stream().map(Item::getLabel).sorted().collect(toList());
        assertIterableEquals(expectedList, actualList);
    }

    @Test
    public void testLoadTransactionSearchFields() {
        List<String> expectedList = Arrays.asList(TransactionSearchBasic.class.getDeclaredFields()).stream().map(Field::getName)
                .sorted().collect(toList());

        NetSuiteDataSet commonDataSet = new NetSuiteDataSet();
        commonDataSet.setDataStore(dataStore);
        commonDataSet.setRecordType("PurchaseOrder");

        uiActionService.validateConnection(dataStore);
        SuggestionValues values = uiActionService.loadFields(commonDataSet);
        List<String> actualList = values.getItems().stream().map(Item::getLabel).sorted().collect(toList());
        assertIterableEquals(expectedList, actualList);
    }

    @Test
    public void testLoadSearchOperator() {
        NetSuiteDataSet dataSet = new NetSuiteDataSet();
        dataSet.setDataStore(dataStore);
        dataSet.setRecordType("Account");

        List<String> expectedList = SearchFieldOperatorTypeDesc
                .createForEnum(SearchFieldOperatorType.MULTI_SELECT, SearchMultiSelectFieldOperator.class).getOperatorNames()
                .stream().map(SearchFieldOperatorName::getQualifiedName).sorted().collect(toList());

        SuggestionValues values = uiActionService.loadOperators(dataSet, "internalId");

        List<String> actualList = values.getItems().stream().map(Item::getLabel).sorted().collect(toList());
        assertIterableEquals(expectedList, actualList);
    }

    @Test
    public void testLoadSearchOperatorForDateType() {
        NetSuiteDataSet dataSet = new NetSuiteDataSet();
        dataSet.setDataStore(dataStore);
        dataSet.setRecordType("PurchaseOrder");

        List<String> expectedList = Stream.concat(
                SearchFieldOperatorTypeDesc.createForEnum(SearchFieldOperatorType.DATE, SearchDateFieldOperator.class)
                        .getOperatorNames().stream(),
                SearchFieldOperatorTypeDesc.createForEnum(SearchFieldOperatorType.PREDEFINED_DATE, SearchDate.class)
                        .getOperatorNames().stream())
                .map(SearchFieldOperatorName::getQualifiedName).sorted().collect(toList());

        SuggestionValues values = uiActionService.loadOperators(dataSet, "billedDate");

        List<String> actualList = values.getItems().stream().map(Item::getLabel).sorted().collect(toList());
        assertIterableEquals(expectedList, actualList);
    }
}
