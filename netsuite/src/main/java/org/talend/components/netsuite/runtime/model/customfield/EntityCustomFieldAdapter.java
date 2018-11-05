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
 * Custom field adapter for {@link BasicRecordType#ENTITY_CUSTOM_FIELD} type.
 */
public class EntityCustomFieldAdapter<T> extends CustomFieldAdapter<T> {

    private static final Map<String, String> RECORD_TYPE_PROPERTY_MAP = new HashMap<>();

    static {
        RECORD_TYPE_PROPERTY_MAP.put("contact", "appliesToContact");
        RECORD_TYPE_PROPERTY_MAP.put("customer", "appliesToCustomer");
        RECORD_TYPE_PROPERTY_MAP.put("employee", "appliesToEmployee");
        RECORD_TYPE_PROPERTY_MAP.put("entityGroup", "appliesToGroup");
        RECORD_TYPE_PROPERTY_MAP.put("otherNameCategory", "appliesToOtherName");
        RECORD_TYPE_PROPERTY_MAP.put("partner", "appliesToPartner");
        RECORD_TYPE_PROPERTY_MAP.put("pricingGroup", "appliesToPriceList");
        RECORD_TYPE_PROPERTY_MAP.put("projectTask", "appliesToProject");
        RECORD_TYPE_PROPERTY_MAP.put("message", "appliesToStatement");
        RECORD_TYPE_PROPERTY_MAP.put("note", "appliesToStatement");
        RECORD_TYPE_PROPERTY_MAP.put("vendor", "appliesToVendor");
        RECORD_TYPE_PROPERTY_MAP.put("siteCategory", "appliesToWebSite");
    }

    public EntityCustomFieldAdapter() {
        super(BasicRecordType.ENTITY_CUSTOM_FIELD);
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
