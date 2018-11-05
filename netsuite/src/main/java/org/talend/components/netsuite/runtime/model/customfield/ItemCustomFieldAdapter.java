/*
 * Copyright (C) 2006-2018 Talend Inc. - www.talend.com
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
package org.talend.components.netsuite.runtime.model.customfield;

import java.util.HashMap;
import java.util.Map;

import org.talend.components.netsuite.runtime.model.BasicRecordType;
import org.talend.components.netsuite.runtime.model.beans.Beans;

/**
 * Custom field adapter for {@link BasicRecordType#ITEM_CUSTOM_FIELD} type.
 */
public class ItemCustomFieldAdapter<T> extends CustomFieldAdapter<T> {

    private static final Map<String, String> RECORD_TYPE_PROPERTY_MAP = new HashMap<>();
    static {
        RECORD_TYPE_PROPERTY_MAP.put("entityGroup", "appliesToGroup");
        RECORD_TYPE_PROPERTY_MAP.put("inventoryItem", "appliesToInventory");
        RECORD_TYPE_PROPERTY_MAP.put("assemblyItem", "appliesToItemAssembly");
        RECORD_TYPE_PROPERTY_MAP.put("kitItem", "appliesToKit");
        RECORD_TYPE_PROPERTY_MAP.put("nonInventoryPurchaseItem", "appliesToNonInventory");
        RECORD_TYPE_PROPERTY_MAP.put("nonInventoryResaleItem", "appliesToNonInventory");
        RECORD_TYPE_PROPERTY_MAP.put("nonInventorySaleItem", "appliesToNonInventory");
        RECORD_TYPE_PROPERTY_MAP.put("otherChargePurchaseItem", "appliesToOtherCharge");
        RECORD_TYPE_PROPERTY_MAP.put("otherChargeResaleItem", "appliesToOtherCharge");
        RECORD_TYPE_PROPERTY_MAP.put("otherChargeSaleItem", "appliesToOtherCharge");
        RECORD_TYPE_PROPERTY_MAP.put("pricingGroup", "appliesToPriceList");
        RECORD_TYPE_PROPERTY_MAP.put("servicePurchaseItem", "appliesToService");
        RECORD_TYPE_PROPERTY_MAP.put("serviceResaleItem", "appliesToService");
        RECORD_TYPE_PROPERTY_MAP.put("serviceSaleItem", "appliesToService");
    }

    public ItemCustomFieldAdapter() {
        super(BasicRecordType.ITEM_CUSTOM_FIELD);
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
