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
