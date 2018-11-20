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
package org.talend.components.netsuite.runtime;

import java.util.Collection;
import java.util.List;

import org.talend.components.netsuite.runtime.model.RecordTypeInfo;
import org.talend.components.netsuite.runtime.schema.SearchInfo;
import org.talend.sdk.component.api.record.Schema;

/**
 * Provides information about NetSuite data set for components in design time or run time.
 */
public interface NetSuiteDatasetRuntime {

    /**
     * Get available record types.
     *
     * @return list of record types' names
     */
    Collection<RecordTypeInfo> getRecordTypes();

    /**
     * Get information about search data model.
     *
     * @param typeName name of target record type
     * @return search data model info
     */
    SearchInfo getSearchInfo(String typeName);

    /**
     * Get available search operators.
     *
     * @return list of search operators' names
     */
    List<String> getSearchFieldOperators(String recordType, String field);

    /**
     * Get schema entry for record type.
     *
     * @param typeName name of target record type
     * @return List of Entries
     */
    Schema getSchema(String typeName);
}
