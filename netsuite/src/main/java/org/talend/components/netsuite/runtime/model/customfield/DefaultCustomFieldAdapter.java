package org.talend.components.netsuite.runtime.model.customfield;

import org.talend.components.netsuite.runtime.model.BasicRecordType;

/**
 * Generic custom field adapter for other custom field record types which have no specialized adapter.
 */
public class DefaultCustomFieldAdapter<T> extends CustomFieldAdapter<T> {

    private boolean applies;

    public DefaultCustomFieldAdapter(BasicRecordType type, boolean applies) {
        super(type);

        this.applies = applies;
    }

    @Override
    public boolean appliesTo(String recordTypeName, T field) {
        return applies;
    }

    @Override
    public CustomFieldRefType apply(T field) {
        return applies ? getFieldType(field) : null;
    }

}
