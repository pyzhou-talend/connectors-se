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
package org.talend.components.netsuite.processor;

import com.netsuite.webservices.v2018_2.lists.accounting.Account;
import com.netsuite.webservices.v2018_2.lists.accounting.types.AccountType;
import com.netsuite.webservices.v2018_2.platform.core.CustomFieldList;
import com.netsuite.webservices.v2018_2.platform.core.RecordRef;
import com.netsuite.webservices.v2018_2.platform.core.RecordRefList;
import com.netsuite.webservices.v2018_2.platform.core.StringCustomFieldRef;
import com.netsuite.webservices.v2018_2.platform.core.types.RecordType;
import com.netsuite.webservices.v2018_2.setup.customization.CustomRecord;
import com.netsuite.webservices.v2018_2.transactions.purchases.PurchaseOrder;
import com.netsuite.webservices.v2018_2.transactions.purchases.PurchaseOrderItem;
import com.netsuite.webservices.v2018_2.transactions.purchases.PurchaseOrderItemList;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.talend.components.netsuite.NetSuiteBaseTest;
import org.talend.components.netsuite.dataset.NetSuiteDataSet;
import org.talend.components.netsuite.dataset.NetSuiteInputProperties;
import org.talend.components.netsuite.dataset.NetSuiteOutputProperties;
import org.talend.components.netsuite.dataset.NetSuiteOutputProperties.DataAction;
import org.talend.components.netsuite.source.NsObjectInputTransducer;
import org.talend.components.netsuite.test.TestCollector;
import org.talend.components.netsuite.test.TestEmitter;
import org.talend.sdk.component.api.record.Record;
import org.talend.sdk.component.junit5.WithComponents;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

@WithComponents("org.talend.components.netsuite")
public class NetSuiteOutputProcessorTest extends NetSuiteBaseTest {

    NetSuiteOutputProperties outputProperties;

    NsObjectInputTransducer inputTransducer;

    private String id;

    @BeforeEach
    public void setup() {
        outputProperties = new NetSuiteOutputProperties();
        dataSet = new NetSuiteDataSet();
        dataSet.setDataStore(dataStore);
        outputProperties.setDataSet(dataSet);
        TestEmitter.reset();
        TestCollector.reset();
    }

    @Test
    public void testCreateUpdateDeleteAccountRecordType() throws IOException {
        dataSet.setRecordType("Account");
        outputProperties.setAction(DataAction.ADD);
        List<String> schemaFields = Arrays.asList("SubsidiaryList", "Description", "AcctName", "AcctType", "InternalId",
                "ExternalId");
        dataSet.setSchema(schemaFields);
        inputTransducer = new NsObjectInputTransducer(clientService, messages, factory, service.getSchema(dataSet, schemaFields),
                "Account", "2018.2");
        Record record = inputTransducer.read(() -> this.prepareAccountRecord(null));
        buildAndRunCollectorJob(outputProperties, record);

        NetSuiteInputProperties inputDataSet = new NetSuiteInputProperties();
        inputDataSet.setDataSet(dataSet);

        final Record insertedRecord = buildAndRunEmitterJob(inputDataSet).stream()
                .filter(r -> r.getString("AcctName").equals(record.getString("AcctName"))).findFirst()
                .orElseThrow(IllegalStateException::new);
        TestCollector.reset();

        Record updateRecord = inputTransducer.read(() -> this.prepareAccountRecord(insertedRecord));
        outputProperties.setAction(DataAction.UPDATE);

        buildAndRunCollectorJob(outputProperties, updateRecord);

        inputDataSet.setSearchCondition(createSearchConditionConfiguration(updateRecord.getString("InternalId")));
        Record resultUpdatedRecord = buildAndRunEmitterJob(inputDataSet).get(0);
        assertEquals(updateRecord.getString("Description"), resultUpdatedRecord.getString("Description"));

        outputProperties.setAction(DataAction.DELETE);
        buildAndRunCollectorJob(outputProperties, updateRecord);

        assertTrue(buildAndRunEmitterJob(inputDataSet).isEmpty());
    }

    @Test
    public void testNativeUpsert() throws IOException {
        createUpsertCustomRecord(true);
    }

    @Test
    public void testCustomUpsert() throws IOException {
        createUpsertCustomRecord(false);
    }

    private void createUpsertCustomRecord(boolean isNativeUpsert) throws IOException {
        clientService.getMetaDataSource().setCustomizationEnabled(true);
        dataSet.setRecordType("customrecordqacomp_custom_recordtype");
        List<String> schemaFields = Arrays.asList("Name", "Custrecord79", "Custrecord80", "InternalId", "ExternalId");
        dataSet.setSchema(schemaFields);
        outputProperties.setUseNativeUpsert(isNativeUpsert);
        inputTransducer = new NsObjectInputTransducer(clientService, messages, factory, service.getSchema(dataSet, schemaFields),
                "customrecordqacomp_custom_recordtype", "2018.2");
        NetSuiteInputProperties inputDataSet = new NetSuiteInputProperties();
        inputDataSet.setDataSet(dataSet);

        Record record = null;
        Record updateRecord = null;
        if (isNativeUpsert) {
            record = inputTransducer.read(() -> this.prepareCustomRecord(null));
            final String preparedCustomField79Value = record.getString("Custrecord79");
            outputProperties.setAction(DataAction.ADD);
            buildAndRunCollectorJob(outputProperties, record);

            Record finalRecord = buildAndRunEmitterJob(inputDataSet).stream()
                    .filter(r -> preparedCustomField79Value.equals(r.getString("Custrecord79"))).findFirst()
                    .orElseThrow(IllegalStateException::new);
            updateRecord = inputTransducer.read(() -> this.prepareCustomRecord(finalRecord));
            record = finalRecord;
        } else {
            updateRecord = inputTransducer.read(() -> this.prepareCustomRecord(null));
        }

        outputProperties.setAction(DataAction.UPSERT);
        buildAndRunCollectorJob(outputProperties, updateRecord);

        final String preparedCustomField80Value = updateRecord.getString("Custrecord80");
        Record resultUpdatedRecord = buildAndRunEmitterJob(inputDataSet).stream()
                .filter(r -> preparedCustomField80Value.equals(r.getString("Custrecord80"))).findFirst()
                .orElseThrow(IllegalStateException::new);

        assertEquals(updateRecord.getString("Name"), resultUpdatedRecord.getString("Name"));

        outputProperties.setAction(DataAction.DELETE);
        if (isNativeUpsert) {
            TestEmitter.addRecord(record);
        }

        buildAndRunCollectorJob(outputProperties, resultUpdatedRecord);

        // Test Search Custom Records.
        // Job.components().component("nsEmitter", "NetSuite://Input?" + inputConfig)
        // .component("collector", "NetSuiteTest://TestCollector").connections().from("nsEmitter").to("collector").build()
        // .run();
        // assertTrue(TestCollector.getData().isEmpty());
    }

    @Test
    public void testCreateVendorBillWithTransactionField() {
        outputProperties.setAction(DataAction.ADD);
        clientService.getMetaDataSource().setCustomizationEnabled(true);
        dataSet.setRecordType("PurchaseOrder");
        List<String> schemaFields = Arrays.asList("Custbody_clarivates_custom", "Custbody111", "Subsidiary", "ItemList",
                "Message", "CustomForm", "Entity", "ExchangeRate", "SupervisorApproval", "InternalId", "ExternalId");
        dataSet.setSchema(schemaFields);

        inputTransducer = new NsObjectInputTransducer(clientService, messages, factory, service.getSchema(dataSet, schemaFields),
                "PurchaseOrder", "2018.2");

        Record record = inputTransducer.read(this::preparePurchaseOrder);

        buildAndRunCollectorJob(outputProperties, record);

        NetSuiteInputProperties inputDataSet = new NetSuiteInputProperties();
        inputDataSet.setDataSet(dataSet);

        String messageStringPrepared = record.getString("Message");
        Record resultRecord = buildAndRunEmitterJob(inputDataSet).stream()
                .filter(r -> messageStringPrepared.equals(r.getString("Message"))).findFirst()
                .orElseThrow(IllegalStateException::new);
        assertEquals(record.getString("Custbody111"), resultRecord.getString("Custbody111"));

        outputProperties.setAction(DataAction.DELETE);
        buildAndRunCollectorJob(outputProperties, resultRecord);

        inputDataSet.setSearchCondition(createSearchConditionConfiguration(resultRecord.getString("InternalId")));
        assertTrue(buildAndRunEmitterJob(inputDataSet).isEmpty());
    }

    private Account prepareAccountRecord(Record record) {
        Account account = new Account();
        RecordRefList subsidiaries = new RecordRefList();
        RecordRef subsidiaryRef = new RecordRef();
        subsidiaryRef.setType(RecordType.SUBSIDIARY);
        subsidiaryRef.setInternalId("1");
        account.setAcctType(AccountType.OTHER_ASSET);
        subsidiaries.getRecordRef().add(subsidiaryRef);
        account.setSubsidiaryList(subsidiaries);
        if (record == null) {
            id = Long.toString(System.currentTimeMillis());
            account.setAcctName("Test account" + id);
            account.setDescription("Test description " + id);
        } else {
            account.setAcctName(record.getString("AcctName"));
            account.setDescription(record.getString("Description") + "- Updated");
            account.setInternalId(record.getString("InternalId"));
            account.setExternalId(record.getString("ExternalId"));
        }
        return account;
    }

    private CustomRecord prepareCustomRecord(Record record) {
        CustomRecord customRecord = new CustomRecord();
        CustomFieldList custFieldList = new CustomFieldList();
        StringCustomFieldRef custField1 = new StringCustomFieldRef();
        StringCustomFieldRef custField2 = new StringCustomFieldRef();
        custField1.setScriptId("custrecord79");
        custField2.setScriptId("custrecord80");
        id = Long.toString(System.currentTimeMillis());
        if (record == null) {
            custField1.setValue("Test " + id);
            custField2.setValue("0.1.0" + id);
            customRecord.setName("Test name " + id);
        } else {
            custField1.setValue(record.getString("Custrecord79"));
            custField2.setValue("1.0.0" + id);
            customRecord.setName(record.getString("Name") + " Updated");
            customRecord.setExternalId(record.getString("InternalId"));
        }
        custFieldList.getCustomField().addAll(Arrays.asList(custField1, custField2));
        customRecord.setCustomFieldList(custFieldList);
        return customRecord;
    }

    private PurchaseOrder preparePurchaseOrder() {

        // Bad practice to hard code internalIds, we had failed tests after truncating environment. Need to consider
        // better way of setupping values.
        String customFormId = "98";
        String vendorId = "5322";
        String employeeId = "5";
        String subsidiaryId = "1";
        String purchaseOrderItemId = "12";
        id = Long.toString(System.currentTimeMillis());
        PurchaseOrder po = new PurchaseOrder();
        po.setSupervisorApproval(true);
        RecordRef ref = new RecordRef();
        ref.setInternalId(customFormId);
        po.setCustomForm(ref);
        ref = new RecordRef();
        ref.setInternalId(vendorId);
        ref.setType(RecordType.VENDOR);
        po.setEntity(ref);
        ref = new RecordRef();
        ref.setInternalId(employeeId);
        ref.setType(RecordType.EMPLOYEE);
        po.setEmployee(ref);
        ref = new RecordRef();
        ref.setInternalId(subsidiaryId);
        ref.setType(RecordType.SUBSIDIARY);
        po.setMessage("Message " + id);
        po.setSubsidiary(ref);
        po.setExchangeRate(1.00);
        CustomFieldList custFieldList = new CustomFieldList();
        StringCustomFieldRef custField1 = new StringCustomFieldRef();
        custField1.setScriptId("custbody111");
        custField1.setValue("SMTH " + subsidiaryId);
        StringCustomFieldRef custField2 = new StringCustomFieldRef();
        custField2.setScriptId("custbody_clarivates_custom");
        custField2.setValue("Integration test");
        custFieldList.getCustomField().addAll(Arrays.asList(custField1, custField2));
        po.setCustomFieldList(custFieldList);
        PurchaseOrderItemList poItemList = new PurchaseOrderItemList();
        PurchaseOrderItem item = new PurchaseOrderItem();
        ref = new RecordRef();
        ref.setInternalId(purchaseOrderItemId);
        ref.setType(RecordType.SERVICE_PURCHASE_ITEM);
        item.setItem(ref);
        poItemList.getItem().addAll(Collections.singletonList(item));
        po.setItemList(poItemList);
        return po;
    }

}