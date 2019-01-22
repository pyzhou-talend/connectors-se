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
package org.talend.components.netsuite.runtime.model.customfield;

import java.util.HashMap;
import java.util.Map;

import org.talend.components.netsuite.runtime.model.BasicRecordType;
import org.talend.components.netsuite.runtime.model.beans.Beans;

/**
 * Custom field adapter for {@link BasicRecordType#CRM_CUSTOM_FIELD} type.
 */
public class CrmCustomFieldAdapter<T> extends CustomFieldAdapter<T> {

    private static final Map<String, String> recordTypePropertyMap = new HashMap<>();
    static {
        recordTypePropertyMap.put("campaign", "appliesToCampaign");
        recordTypePropertyMap.put("supportCase", "appliesToCase");
        recordTypePropertyMap.put("calendarEvent", "appliesToEvent");
        recordTypePropertyMap.put("issue", "appliesToIssue");
        recordTypePropertyMap.put("phoneCall", "appliesToPhoneCall");
        recordTypePropertyMap.put("projectTask", "appliesToProjectTask");
        recordTypePropertyMap.put("solution", "appliesToSolution");
        recordTypePropertyMap.put("task", "appliesToTask");
    }

    public CrmCustomFieldAdapter() {
        super(BasicRecordType.CRM_CUSTOM_FIELD);
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
