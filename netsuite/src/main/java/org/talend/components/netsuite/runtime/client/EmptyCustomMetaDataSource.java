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
