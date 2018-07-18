package org.talend.components.netsuite.runtime.client;

import java.util.Collection;
import java.util.Collections;
import java.util.Map;

import org.talend.components.netsuite.runtime.model.CustomFieldDesc;
import org.talend.components.netsuite.runtime.model.CustomRecordTypeInfo;
import org.talend.components.netsuite.runtime.model.RecordTypeInfo;

/**
 * Implementation of <code>CustomMetaDataSource</code> which provides empty customization meta data.
 */
public class EmptyCustomMetaDataSource implements CustomMetaDataSource {

    @Override
    public Collection<CustomRecordTypeInfo> getCustomRecordTypes() {
        return Collections.emptyList();
    }

    @Override
    public CustomRecordTypeInfo getCustomRecordType(String typeName) {
        return null;
    }

    @Override
    public Map<String, CustomFieldDesc> getCustomFields(RecordTypeInfo recordTypeInfo) {
        return Collections.emptyMap();
    }
}
