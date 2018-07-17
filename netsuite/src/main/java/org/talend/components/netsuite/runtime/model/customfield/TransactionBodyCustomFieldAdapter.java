package org.talend.components.netsuite.runtime.model.customfield;

import java.util.HashMap;
import java.util.Map;

import org.talend.components.netsuite.runtime.model.BasicRecordType;
import org.talend.components.netsuite.runtime.model.beans.Beans;

/**
 * Custom field adapter for {@link BasicRecordType#TRANSACTION_BODY_CUSTOM_FIELD} type.
 */
public class TransactionBodyCustomFieldAdapter<T> extends CustomFieldAdapter<T> {

    private static final Map<String, String> RECORD_TYPE_PROPERTY_MAP = new HashMap<>();
    static {
        RECORD_TYPE_PROPERTY_MAP.put("assemblyBuild", "bodyAssemblyBuild");
        RECORD_TYPE_PROPERTY_MAP.put("purchaseOrder", "bodyPurchase");
        RECORD_TYPE_PROPERTY_MAP.put("journalEntry", "bodyJournal");
        RECORD_TYPE_PROPERTY_MAP.put("expenseReport", "bodyExpenseReport");
        RECORD_TYPE_PROPERTY_MAP.put("opportunity", "bodyOpportunity");
        RECORD_TYPE_PROPERTY_MAP.put("itemReceipt", "bodyItemReceipt");
        RECORD_TYPE_PROPERTY_MAP.put("itemFulfillment", "bodyItemFulfillment");
        RECORD_TYPE_PROPERTY_MAP.put("inventoryAdjustment", "bodyInventoryAdjustment");
        RECORD_TYPE_PROPERTY_MAP.put("customerPayment", "bodyCustomerPayment");
        RECORD_TYPE_PROPERTY_MAP.put("vendorPayment", "bodyVendorPayment");
        RECORD_TYPE_PROPERTY_MAP.put("vendorBill", "bodyPurchase");
        RECORD_TYPE_PROPERTY_MAP.put("vendorCredit", "bodyPurchase");
        RECORD_TYPE_PROPERTY_MAP.put("creditMemo", "bodySale");
        RECORD_TYPE_PROPERTY_MAP.put("invoice", "bodySale");
    }

    public TransactionBodyCustomFieldAdapter() {
        super(BasicRecordType.TRANSACTION_BODY_CUSTOM_FIELD);
    }

    @Override
    public boolean appliesTo(String recordType, T field) {
        String propertyName = RECORD_TYPE_PROPERTY_MAP.get(recordType);
        Boolean applies = propertyName != null ? (Boolean) Beans.getSimpleProperty(field, propertyName) : Boolean.FALSE;
        return applies == null ? false : applies.booleanValue();
    }

    @Override
    public CustomFieldRefType apply(T field) {
        return getFieldType(field);
    }

}
