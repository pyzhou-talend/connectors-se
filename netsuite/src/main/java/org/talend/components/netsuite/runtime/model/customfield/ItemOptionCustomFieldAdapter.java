package org.talend.components.netsuite.runtime.model.customfield;

import java.util.HashMap;
import java.util.Map;

import org.talend.components.netsuite.runtime.model.BasicRecordType;
import org.talend.components.netsuite.runtime.model.beans.Beans;

/**
 * Custom field adapter for {@link BasicRecordType#ITEM_OPTION_CUSTOM_FIELD} type.
 */
public class ItemOptionCustomFieldAdapter<T> extends CustomFieldAdapter<T> {

    private static final Map<String, String> recordTypePropertyMap = new HashMap<>();
    static {
        recordTypePropertyMap.put("purchaseOrder", "colPurchase");
        recordTypePropertyMap.put("salesOrder", "colSale");
        recordTypePropertyMap.put("opportunity", "colOpportunity");
        recordTypePropertyMap.put("kitItem", "colKitItem");
    }

    public ItemOptionCustomFieldAdapter() {
        super(BasicRecordType.ITEM_OPTION_CUSTOM_FIELD);
    }

    @Override
    public boolean appliesTo(String recordType, T field) {
        String propertyName = recordTypePropertyMap.get(recordType);
        Boolean applies = propertyName != null ? (Boolean) Beans.getSimpleProperty(field, propertyName) : Boolean.FALSE;
        return applies == null ? false : applies.booleanValue();
    }

    @Override
    public CustomFieldRefType apply(T field) {
        return getFieldType(field);
    }

}
