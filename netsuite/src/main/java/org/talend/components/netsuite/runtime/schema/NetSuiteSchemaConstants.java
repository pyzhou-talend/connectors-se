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
package org.talend.components.netsuite.runtime.schema;

/**
 * Holds schema related constants that are used by NetSuite classes.
 */
public class NetSuiteSchemaConstants {

    private NetSuiteSchemaConstants() {
    }

    public static final String TALEND6_DYNAMIC_COLUMN_POSITION = "di.dynamic.column.position";

    public static final String TALEND6_DYNAMIC_COLUMN_ID = "di.dynamic.column.id";

    public static final String TALEND6_ADDITIONAL_PROPERTIES = "di.prop.";

    public static final String TALEND6_COMMENT = "di.table.comment";

    public static final String NS_PREFIX = TALEND6_ADDITIONAL_PROPERTIES + "netsuite.";

    public static final String NS_CUSTOM_RECORD = NS_PREFIX + "customRecord";

    public static final String NS_CUSTOM_RECORD_SCRIPT_ID = NS_PREFIX + "customRecord.scriptId";

    public static final String NS_CUSTOM_RECORD_INTERNAL_ID = NS_PREFIX + "customRecord.internalId";

    public static final String NS_CUSTOM_RECORD_CUSTOMIZATION_TYPE = NS_PREFIX + "customRecord.customizationType";

    public static final String NS_CUSTOM_RECORD_TYPE = NS_PREFIX + "customRecord.type";

    public static final String NS_CUSTOM_FIELD = NS_PREFIX + "customField";

    public static final String NS_CUSTOM_FIELD_SCRIPT_ID = NS_PREFIX + "customField.scriptId";

    public static final String NS_CUSTOM_FIELD_INTERNAL_ID = NS_PREFIX + "customField.internalId";

    public static final String NS_CUSTOM_FIELD_CUSTOMIZATION_TYPE = NS_PREFIX + "customField.customizationType";

    public static final String NS_CUSTOM_FIELD_TYPE = NS_PREFIX + "customField.type";
}