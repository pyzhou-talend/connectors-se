package org.talend.components.netsuite.processor;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.talend.sdk.component.junit.SimpleFactory.configurationByExample;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.talend.components.netsuite.NetsuiteBaseTest;
import org.talend.components.netsuite.dataset.NetSuiteCommonDataSet;
import org.talend.components.netsuite.dataset.NetsuiteInputDataSet;
import org.talend.components.netsuite.dataset.NetsuiteOutputDataSet;
import org.talend.components.netsuite.dataset.NetsuiteOutputDataSet.DataAction;
import org.talend.components.netsuite.dataset.SearchConditionConfiguration;
import org.talend.components.netsuite.runtime.client.NetSuiteClientService;
import org.talend.components.netsuite.source.NsObjectInputTransducer;
import org.talend.sdk.component.api.record.Record;
import org.talend.sdk.component.api.record.Schema;
import org.talend.sdk.component.junit5.WithComponents;
import org.talend.sdk.component.runtime.manager.chain.Job;

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

@WithComponents("org.talend.components.netsuite")
public class NetsuiteOutputProcessorTest extends NetsuiteBaseTest {

    NetsuiteOutputDataSet dataSet;

    // NetsuiteOutputProcessor processor;

    NetSuiteClientService<?> clientService;

    private Schema schema;

    NsObjectInputTransducer inputTransducer;

    private List<Record> resultList;

    private List<Record> rejectList;

    private String id;

    @BeforeEach
    public void setup() {
        dataSet = new NetsuiteOutputDataSet();
        commonDataSet = new NetSuiteCommonDataSet();
        commonDataSet.setDataStore(dataStore);
        dataSet.setCommonDataSet(commonDataSet);
        clientService = service.getClientService(commonDataSet.getDataStore());
    }

    @Test
    public void map() throws IOException {
        commonDataSet.setRecordType("Account");
        dataSet.setAction(DataAction.ADD);
        List<String> schemaFields = Arrays.asList("SubsidiaryList", "Description", "AcctName", "AcctType", "InternalId",
                "ExternalId");
        schema = service.getSchema(commonDataSet);
        inputTransducer = new NsObjectInputTransducer(clientService, factory, schema, schemaFields, "Account");
        Record record = inputTransducer.read(() -> this.prepareAccountRecord(null));
        // resultList = new ArrayList<>();
        rejectList = new ArrayList<>();
        String config = configurationByExample().forInstance(dataSet).configured().toQueryString();
        COMPONENT.setInputData(Collections.singletonList(record));
        Job.components().component("emitter", "test://emitter").component("nsProducer", "Netsuite://Output?" + config)
                .component("collector", "test://collector").connections().from("emitter").to("nsProducer")
                .from("nsProducer", "main").to("collector").build().run();
        List<Record> resultList = COMPONENT.getCollectedData(Record.class);
        assertEquals(1, resultList.size());
        COMPONENT.resetState();

        final Record resultRecord = resultList.get(0);
        Record updateRecord = inputTransducer.read(() -> this.prepareAccountRecord(resultRecord));
        dataSet.setAction(DataAction.UPDATE);
        config = configurationByExample().forInstance(dataSet).configured().toQueryString();

        NetsuiteInputDataSet inputDataSet = new NetsuiteInputDataSet();
        commonDataSet.setSchema(schemaFields);
        inputDataSet.setCommonDataSet(commonDataSet);
        SearchConditionConfiguration search = new SearchConditionConfiguration();
        search.setField("internalId");
        search.setOperator("List.anyOf");
        search.setValue(resultRecord.getString("InternalId"));
        search.setValue2("");
        inputDataSet.setSearchCondition(Collections.singletonList(search));
        String inputConfig = configurationByExample().forInstance(inputDataSet).configured().toQueryString();
        COMPONENT.setInputData(Collections.singletonList(updateRecord));
        Job.components().component("emitter", "test://emitter").component("nsProducer", "Netsuite://Output?" + config)
                .connections().from("emitter").to("nsProducer").build().run();

        Job.components().component("nsEmitter", "Netsuite://Input?" + inputConfig).component("collector", "test://collector")
                .connections().from("nsEmitter").to("collector").build().run();
        resultList = COMPONENT.getCollectedData(Record.class);
        assertEquals(1, resultList.size());
        Record resultUpdatedRecord = resultList.get(0);
        assertEquals(updateRecord.getString("Description"), resultUpdatedRecord.getString("Description"));
        COMPONENT.resetState();

        dataSet.setAction(DataAction.DELETE);
        config = configurationByExample().forInstance(dataSet).configured().toQueryString();

        COMPONENT.setInputData(Collections.singletonList(updateRecord));
        Job.components().component("emitter", "test://emitter").component("nsProducer", "Netsuite://Output?" + config)
                .connections().from("emitter").to("nsProducer").build().run();

        Job.components().component("nsEmitter", "Netsuite://Input?" + inputConfig).component("collector", "test://collector")
                .connections().from("nsEmitter").to("collector").build().run();
        resultList = COMPONENT.getCollectedData(Record.class);
        assertTrue(resultList.isEmpty());
        COMPONENT.resetState();
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
        String internalId = null;
        String externalId = null;
        clientService.getMetaDataSource().setCustomizationEnabled(true);
        commonDataSet.setRecordType("customrecordqacomp_custom_recordtype");
        List<String> schemaFields = Arrays.asList("Name", "Custrecord79", "Custrecord80", "InternalId", "ExternalId");
        commonDataSet.setSchema(schemaFields);
        dataSet.setUseNativeUpsert(isNativeUpsert);
        schema = service.getSchema(commonDataSet);
        inputTransducer = new NsObjectInputTransducer(clientService, factory, schema, schemaFields,
                "customrecordqacomp_custom_recordtype");
        String config = null;
        Record record = null;
        Record updateRecord = null;
        if (isNativeUpsert) {
            record = inputTransducer.read(() -> this.prepareCustomRecord(null));
            dataSet.setAction(DataAction.ADD);
            config = configurationByExample().forInstance(dataSet).configured().toQueryString();
            COMPONENT.setInputData(Collections.singletonList(record));
            Job.components().component("emitter", "test://emitter").component("nsProducer", "Netsuite://Output?" + config)
                    .component("collector", "test://collector").connections().from("emitter").to("nsProducer")
                    .from("nsProducer", "main").to("collector").build().run();
            List<Record> resultList = COMPONENT.getCollectedData(Record.class);
            assertEquals(1, resultList.size());
            Record finalRecord = resultList.get(0);
            externalId = finalRecord.getString("InternalId");
            updateRecord = inputTransducer.read(() -> this.prepareCustomRecord(finalRecord));
            record = finalRecord;
            COMPONENT.resetState();
        } else {
            updateRecord = inputTransducer.read(() -> this.prepareCustomRecord(null));
        }

        dataSet.setAction(DataAction.UPSERT);
        config = configurationByExample().forInstance(dataSet).configured().toQueryString();

        COMPONENT.setInputData(Collections.singletonList(updateRecord));
        Job.components().component("emitter", "test://emitter").component("nsProducer", "Netsuite://Output?" + config)
                .component("collector", "test://collector").connections().from("emitter").to("nsProducer")
                .from("nsProducer", "main").to("collector").build().run();

        resultList = COMPONENT.getCollectedData(Record.class);
        internalId = resultList.get(0).getString("InternalId");
        COMPONENT.resetState();

        NetsuiteInputDataSet inputDataSet = new NetsuiteInputDataSet();
        commonDataSet.setSchema(schemaFields);
        inputDataSet.setCommonDataSet(commonDataSet);
        SearchConditionConfiguration search = new SearchConditionConfiguration();
        search.setField("internalId");
        search.setOperator("List.anyOf");
        search.setValue(internalId);
        search.setValue2("");
        inputDataSet.setSearchCondition(Collections.singletonList(search));
        String inputConfig = configurationByExample().forInstance(inputDataSet).configured().toQueryString();
        Job.components().component("nsEmitter", "Netsuite://Input?" + inputConfig).component("collector", "test://collector")
                .connections().from("nsEmitter").to("collector").build().run();
        resultList = COMPONENT.getCollectedData(Record.class);
        assertEquals(1, resultList.size());
        Record resultUpdatedRecord = resultList.get(0);
        assertEquals(updateRecord.getString("Custrecord80"), resultUpdatedRecord.getString("Custrecord80"));
        COMPONENT.resetState();

        dataSet.setAction(DataAction.DELETE);
        config = configurationByExample().forInstance(dataSet).configured().toQueryString();
        List<Record> recordsToBeDeleted = new ArrayList<>();
        recordsToBeDeleted.add(resultUpdatedRecord);
        if (isNativeUpsert) {
            recordsToBeDeleted.add(record);
        }
        COMPONENT.setInputData(recordsToBeDeleted);
        Job.components().component("emitter", "test://emitter").component("nsProducer", "Netsuite://Output?" + config)
                .connections().from("emitter").to("nsProducer").build().run();

        Job.components().component("nsEmitter", "Netsuite://Input?" + inputConfig).component("collector", "test://collector")
                .connections().from("nsEmitter").to("collector").build().run();
        resultList = COMPONENT.getCollectedData(Record.class);
        assertTrue(resultList.isEmpty());
        COMPONENT.resetState();
    }

    @Test
    public void testCreateVendorBillWithTransactionField() {
        dataSet.setAction(DataAction.ADD);
        clientService.getMetaDataSource().setCustomizationEnabled(true);
        commonDataSet.setRecordType("PurchaseOrder");
        List<String> schemaFields = Arrays.asList("Custbody_clarivates_custom", "Custbody111", "Subsidiary", "ItemList",
                "CustomForm", "Entity", "ExchangeRate", "SupervisorApproval", "InternalId", "ExternalId");

        schema = service.getSchema(commonDataSet);
        inputTransducer = new NsObjectInputTransducer(clientService, factory, schema, schemaFields, "PurchaseOrder");

        // Bad practice to hard code internalIds, we had failed tests after truncating environment. Need to consider
        // better way of setupping values.
        String customFormId = "98";
        String vendorId = "5322";
        String employeeId = "5";
        String subsidiaryId = "1";
        String purchaseOrderItemId = "12";
        Record record = inputTransducer
                .read(() -> preparePurchaseOrder(customFormId, vendorId, employeeId, subsidiaryId, purchaseOrderItemId));
        String config = configurationByExample().forInstance(dataSet).configured().toQueryString();
        COMPONENT.setInputData(Collections.singletonList(record));
        Job.components().component("emitter", "test://emitter").component("nsProducer", "Netsuite://Output?" + config)
                .component("collector", "test://collector").connections().from("emitter").to("nsProducer")
                .from("nsProducer", "main").to("collector").build().run();
        List<Record> resultList = COMPONENT.getCollectedData(Record.class);
        assertEquals(1, resultList.size());
        final Record resultRecord = resultList.get(0);
        assertEquals(record.getString("Custbody111"), resultRecord.getString("Custbody111"));
        COMPONENT.resetState();

        COMPONENT.setInputData(Collections.singletonList(resultRecord));
        dataSet.setAction(DataAction.DELETE);
        config = configurationByExample().forInstance(dataSet).configured().toQueryString();

        Job.components().component("emitter", "test://emitter").component("nsProducer", "Netsuite://Output?" + config)
                .connections().from("emitter").to("nsProducer").build().run();
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
        if (record == null) {
            id = Long.toString(System.currentTimeMillis());
            custField1.setValue("Test " + id);
            custField2.setValue("0.1.0");
            customRecord.setName("Test name " + id);
        } else {
            custField1.setValue(record.getString("Custrecord79"));
            custField2.setValue("1.0.0");
            customRecord.setName(record.getString("Name"));
            customRecord.setExternalId(record.getString("InternalId"));
        }
        custFieldList.getCustomField().addAll(Arrays.asList(custField1, custField2));
        customRecord.setCustomFieldList(custFieldList);
        return customRecord;
    }

    private PurchaseOrder preparePurchaseOrder(String customFormId, String vendorId, String employeeId, String subsidiaryId,
            String purchaseOrderItemId) {
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