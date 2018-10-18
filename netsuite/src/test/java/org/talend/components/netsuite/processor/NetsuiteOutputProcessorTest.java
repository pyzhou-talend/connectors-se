package org.talend.components.netsuite.processor;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.talend.components.netsuite.NetsuiteBaseTest;
import org.talend.components.netsuite.dataset.NetSuiteCommonDataSet;
import org.talend.components.netsuite.dataset.NetsuiteOutputDataSet;
import org.talend.components.netsuite.dataset.NetsuiteOutputDataSet.DataAction;
import org.talend.components.netsuite.runtime.client.NetSuiteClientService;
import org.talend.components.netsuite.source.NsObjectInputTransducer;
import org.talend.sdk.component.api.record.Record;
import org.talend.sdk.component.api.record.Schema;
import org.talend.sdk.component.junit5.WithComponents;

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

    NetsuiteOutputProcessor processor;

    NetSuiteClientService<?> clientService;

    private Schema schema;

    NsObjectInputTransducer inputTransducer;

    private List<Record> resultList;

    private List<Record> rejectList;

    private String id;

    // @BeforeEach
    public void setup() {
        dataSet = new NetsuiteOutputDataSet();
        commonDataSet = new NetSuiteCommonDataSet();
        commonDataSet.setDataStore(dataStore);
        dataSet.setCommonDataSet(commonDataSet);
        clientService = service.getClientService(commonDataSet.getDataStore());
    }

    // @Test
    public void map() throws IOException {
        commonDataSet.setRecordType("Account");
        dataSet.setSchemaIn(Arrays.asList("SubsidiaryList", "Description", "AcctName", "AcctType", "InternalId", "ExternalId"));
        dataSet.setAction(DataAction.ADD);
        processor = new NetsuiteOutputProcessor(dataSet, service, factory);
        processor.init();

        schema = service.getSchema(commonDataSet);
        inputTransducer = new NsObjectInputTransducer(clientService, factory, schema, dataSet.getSchemaIn(), "Account");
        Record ir = inputTransducer.read(this::prepareAccountRecord);

        resultList = new ArrayList<>();
        rejectList = new ArrayList<>();
        processor.onNext(ir, resultList::addAll, rejectList::addAll);

        assertEquals(1, resultList.size());
        ir = resultList.get(0);
        assertEquals(0, rejectList.size());
        resultList.clear();
        rejectList.clear();

        // ir.put(ir.getSchema().getField("AcctName").pos(), "Update " + id);
        // RecordRef accountRef = new RecordRef();
        // accountRef.setInternalId((String) ir.get(ir.getSchema().getField("InternalId").pos()));
        // accountRef.setType(RecordType.ACCOUNT);
        //
        // dataSet.setAction(DataAction.UPDATE);
        // processor.init();
        // processor.onNext(ir, resultList::addAll, rejectList::addAll);
        // assertEquals(1, resultList.size());
        // assertEquals(0, rejectList.size());
        // resultList.clear();
        // rejectList.clear();
        // List<NsReadResponse<Object>> result = clientService.getList(Collections.singletonList(accountRef));
        // Account account = (Account) result.get(0).getRecord();
        // assertEquals("Update " + id, account.getAcctName());
        //
        // dataSet.setAction(DataAction.DELETE);
        // processor.init();
        // processor.onNext(ir, resultList::addAll, rejectList::addAll);
        //
        // assertEquals(1, resultList.size());
        // assertEquals(0, rejectList.size());
    }

    // @Test
    public void testNativeUpsert() throws IOException {
        createUpsertCustomRecord(true);
    }

    // @Test
    public void testCustomUpsert() throws IOException {
        createUpsertCustomRecord(false);
    }

    private void createUpsertCustomRecord(boolean isNativeUpsert) throws IOException {
        String externalId = null;
        String internalId = null;
        clientService.getMetaDataSource().setCustomizationEnabled(true);
        commonDataSet.setRecordType("customrecordqacomp_custom_recordtype");
        dataSet.setSchemaIn(Arrays.asList("Name", "Custrecord79", "Custrecord80", "InternalId", "ExternalId"));
        dataSet.setUseNativeUpsert(isNativeUpsert);
        processor = new NetsuiteOutputProcessor(dataSet, service, factory);
        schema = service.getSchema(commonDataSet);
        inputTransducer = new NsObjectInputTransducer(clientService, factory, schema, dataSet.getSchemaIn(),
                "customrecordqacomp_custom_recordtype");
        Record ir = inputTransducer.read(this::prepareCustomRecord);
        // int internalIdPosition = ir.getSchema().getField("InternalId").pos();
        // resultList = new ArrayList<>();
        // rejectList = new ArrayList<>();
        //
        // if (isNativeUpsert) {
        // dataSet.setAction(DataAction.ADD);
        // processor.init();
        // processor.onNext(ir, resultList::addAll, rejectList::addAll);
        // ir = resultList.get(0);
        // externalId = (String) ir.get(internalIdPosition);
        // ir.put(ir.getSchema().getField("ExternalId").pos(), externalId);
        // ir.put(internalIdPosition, null);
        // resultList.clear();
        // rejectList.clear();
        // }
        //
        // dataSet.setAction(DataAction.UPSERT);
        // processor.init();
        // processor.onNext(ir, resultList::addAll, rejectList::addAll);
        //
        // assertEquals(1, resultList.size());
        // ir = resultList.get(0);
        // assertEquals(0, rejectList.size());
        // resultList.clear();
        // rejectList.clear();
        //
        // ir.put(ir.getSchema().getField("Custrecord79").pos(), "Test Update " + id);
        // if (isNativeUpsert) {
        // internalId = (String) ir.get(internalIdPosition);
        // ir.put(internalIdPosition, null);
        // }
        // processor.onNext(ir, resultList::addAll, rejectList::addAll);
        // assertEquals(1, resultList.size());
        // assertEquals(0, rejectList.size());
        // resultList.clear();
        // rejectList.clear();
        //
        // CustomRecordRef customRecordRef = new CustomRecordRef();
        // customRecordRef.setInternalId(isNativeUpsert ? internalId : (String)
        // ir.get(ir.getSchema().getField("InternalId").pos()));
        // customRecordRef.setScriptId("customrecordqacomp_custom_recordtype");
        // List<NsReadResponse<Object>> result = clientService.getList(Collections.singletonList(customRecordRef));
        // CustomRecord customRecord = (CustomRecord) result.get(0).getRecord();
        // assertEquals("Test Update " + id,
        // customRecord.getCustomFieldList().getCustomField().stream()
        // .filter(element -> element.getScriptId().equalsIgnoreCase("Custrecord79")).findFirst()
        // .map(StringCustomFieldRef.class::cast).get().getValue());
        //
        // dataSet.setAction(DataAction.DELETE);
        // processor.init();
        // processor.onNext(ir, resultList::addAll, rejectList::addAll);
        //
        // assertEquals(1, resultList.size());
        // assertEquals(0, rejectList.size());
        //
        // if (externalId != null) {
        // // Clean up for external record
        // ir.put(ir.getSchema().getField("InternalId").pos(), externalId);
        // processor.onNext(ir, resultList::addAll, rejectList::addAll);
        // }
    }

    // @Test
    public void testCreateVendorBillWithTransactionField() {
        clientService.getMetaDataSource().setCustomizationEnabled(true);
        commonDataSet.setRecordType("PurchaseOrder");
        schema = service.getSchema(commonDataSet);
        dataSet.setSchemaIn(Arrays.asList("Custbody_clarivates_custom", "Custbody111", "Subsidiary", "ItemList", "CustomForm",
                "Entity", "ExchangeRate", "SupervisorApproval", "InternalId", "ExternalId"));
        dataSet.setAction(DataAction.ADD);
        processor = new NetsuiteOutputProcessor(dataSet, service, factory);
        processor.init();
        inputTransducer = new NsObjectInputTransducer(clientService, factory, schema, dataSet.getSchemaIn(), "PurchaseOrder");

        // Bad practice to hard code internalIds, we had failed tests after truncating environment. Need to consider
        // better way of setupping values.
        String customFormId = "98";
        String vendorId = "5322";
        String employeeId = "5";
        String subsidiaryId = "1";
        String purchaseOrderItemId = "12";

        Record ir = inputTransducer
                .read(() -> preparePurchaseOrder(customFormId, vendorId, employeeId, subsidiaryId, purchaseOrderItemId));

        resultList = new ArrayList<>();
        rejectList = new ArrayList<>();
        processor.onNext(ir, resultList::addAll, rejectList::addAll);

        assertEquals(1, resultList.size());
        ir = resultList.get(0);
        assertEquals(0, rejectList.size());
        resultList.clear();
        rejectList.clear();

        dataSet.setAction(DataAction.DELETE);
        processor.init();
        processor.onNext(ir, resultList::addAll, rejectList::addAll);

        assertEquals(1, resultList.size());
        assertEquals(0, rejectList.size());
    }

    private Account prepareAccountRecord() {
        Account record = new Account();
        id = Long.toString(System.currentTimeMillis());
        record.setAcctName("Test account " + id);
        record.setAcctType(AccountType.OTHER_ASSET);
        record.setDescription("Test description " + id);
        RecordRef subsidiaryRef = new RecordRef();
        subsidiaryRef.setType(RecordType.SUBSIDIARY);
        subsidiaryRef.setInternalId("1");
        RecordRefList subsidiaries = new RecordRefList();
        subsidiaries.getRecordRef().add(subsidiaryRef);
        record.setSubsidiaryList(subsidiaries);
        return record;
    }

    private CustomRecord prepareCustomRecord() {
        CustomRecord record = new CustomRecord();
        id = Long.toString(System.currentTimeMillis());
        record.setName("Test name " + id);
        CustomFieldList custFieldList = new CustomFieldList();
        StringCustomFieldRef custField1 = new StringCustomFieldRef();
        custField1.setScriptId("custrecord79");
        custField1.setValue("Test " + id);
        StringCustomFieldRef custField2 = new StringCustomFieldRef();
        custField2.setScriptId("custrecord80");
        custField2.setValue("0.1.0");
        custFieldList.getCustomField().addAll(Arrays.asList(custField1, custField2));
        record.setCustomFieldList(custFieldList);
        return record;
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
        custField1.setValue("SMTH " + id);
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