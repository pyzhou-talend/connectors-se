package org.talend.components.netsuite.runtime.model.customfield;

import java.util.HashMap;
import java.util.Map;

import org.talend.components.netsuite.runtime.model.BasicRecordType;
import org.talend.components.netsuite.runtime.model.beans.Beans;

/**
 * Custom field adapter for {@link BasicRecordType#TRANSACTION_COLUMN_CUSTOM_FIELD} type.
 */
public class TransactionColumnCustomFieldAdapter<T> extends CustomFieldAdapter<T> {

    private static final Map<String, String> RECORD_TYPE_PROPERTY_MAP = new HashMap<>();
    static {
        RECORD_TYPE_PROPERTY_MAP.put("expenseCategory", "colExpense");
        RECORD_TYPE_PROPERTY_MAP.put("expenseReport", "colExpenseReport");
        RECORD_TYPE_PROPERTY_MAP.put("assemblyBuild", "colBuild");
        RECORD_TYPE_PROPERTY_MAP.put("itemReceipt", "colItemReceipt");
        RECORD_TYPE_PROPERTY_MAP.put("itemFulfillment", "colItemFulfillment");
        RECORD_TYPE_PROPERTY_MAP.put("purchaseOrder", "colPurchase");
        RECORD_TYPE_PROPERTY_MAP.put("journalEntry", "colJournal");
        RECORD_TYPE_PROPERTY_MAP.put("salesOrder", "colSale");
        RECORD_TYPE_PROPERTY_MAP.put("opportunity", "colOpportunity");
        RECORD_TYPE_PROPERTY_MAP.put("kitItem", "colKitItem");
        RECORD_TYPE_PROPERTY_MAP.put("timeBill", "colTime");
    }

    public TransactionColumnCustomFieldAdapter() {
        super(BasicRecordType.TRANSACTION_COLUMN_CUSTOM_FIELD);
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
