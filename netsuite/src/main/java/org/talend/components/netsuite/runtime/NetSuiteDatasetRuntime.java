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
package org.talend.components.netsuite.runtime;

import static java.util.stream.Collectors.toList;

import java.util.Collection;
import java.util.Comparator;
import java.util.List;

import javax.xml.datatype.XMLGregorianCalendar;

import org.talend.components.netsuite.runtime.client.MetaDataSource;
import org.talend.components.netsuite.runtime.model.CustomFieldDesc;
import org.talend.components.netsuite.runtime.model.FieldDesc;
import org.talend.components.netsuite.runtime.model.RecordTypeInfo;
import org.talend.components.netsuite.runtime.model.SearchRecordTypeDesc;
import org.talend.components.netsuite.runtime.model.TypeDesc;
import org.talend.components.netsuite.runtime.model.beans.Beans;
import org.talend.components.netsuite.runtime.model.customfield.CustomFieldRefType;
import org.talend.components.netsuite.runtime.model.search.SearchInfo;
import org.talend.sdk.component.api.record.Schema;
import org.talend.sdk.component.api.record.Schema.Entry;
import org.talend.sdk.component.api.record.Schema.Type;
import org.talend.sdk.component.api.service.record.RecordBuilderFactory;

import lombok.AllArgsConstructor;

/**
 * Provides information about NetSuite data set for components in design time or run time.
 */

@AllArgsConstructor
public class NetSuiteDatasetRuntime {

    /** Source of meta data. */
    private MetaDataSource metaDataSource;

    private final RecordBuilderFactory recordBuilderFactory;

    public Collection<RecordTypeInfo> getRecordTypes() {
        return metaDataSource.getRecordTypes();
    }

    public Schema getSchema(String typeName, List<String> stringSchema) {
        final boolean schemaNotDesigned = stringSchema == null;
        Schema.Builder builder = recordBuilderFactory.newSchemaBuilder(Type.RECORD);
        metaDataSource.getTypeInfo(typeName).getFields().stream()
                .filter(field -> schemaNotDesigned
                        || stringSchema.stream().anyMatch(element -> element.equalsIgnoreCase(field.getName())))
                .sorted(FieldDescComparator.INSTANCE).map(this::buildEntryFromFieldDescription).forEach(builder::withEntry);
        return builder.build();
    }

    private Entry buildEntryFromFieldDescription(FieldDesc desc) {
        return recordBuilderFactory.newEntryBuilder().withName(Beans.toInitialUpper(desc.getName()))
                .withType(NetSuiteDatasetRuntime.inferSchemaForField(desc)).withNullable(desc.isNullable()).build();
    }

    public SearchInfo getSearchInfo(String typeName) {
        final SearchRecordTypeDesc searchInfo = metaDataSource.getSearchRecordType(typeName);
        final TypeDesc searchRecordInfo = metaDataSource.getBasicMetaData().getTypeInfo(searchInfo.getSearchBasicClass());
        List<String> fields = searchRecordInfo.getFields().stream().map(FieldDesc::getName).sorted().collect(toList());
        return new SearchInfo(searchRecordInfo.getTypeName(), fields);
    }

    public List<String> getSearchFieldOperators(String recordType, String field) {
        final SearchRecordTypeDesc searchInfo = metaDataSource.getSearchRecordType(recordType);
        final FieldDesc fieldDesc = metaDataSource.getBasicMetaData().getTypeInfo(searchInfo.getSearchBasicClass())
                .getField(field);
        return metaDataSource.getBasicMetaData().getSearchOperatorNames(fieldDesc);
    }

    /**
     * Return type of value hold by a custom field with given <code>CustomFieldRefType</code>.
     *
     * @param customFieldRefType type of field
     * @return type of value
     */
    public static Class<?> getCustomFieldValueClass(CustomFieldRefType customFieldRefType) {
        Class<?> valueClass = null;
        switch (customFieldRefType) {
        case BOOLEAN:
            valueClass = Boolean.class;
            break;
        case STRING:
            valueClass = String.class;
            break;
        case LONG:
            valueClass = Long.class;
            break;
        case DOUBLE:
            valueClass = Double.class;
            break;
        case DATE:
            valueClass = XMLGregorianCalendar.class;
            break;
        case SELECT:
        case MULTI_SELECT:
            valueClass = String.class;
            break;
        }
        return valueClass;
    }

    private static class FieldDescComparator implements Comparator<FieldDesc> {

        public static final FieldDescComparator INSTANCE = new FieldDescComparator();

        @Override
        public int compare(FieldDesc o1, FieldDesc o2) {
            int result = Boolean.compare(o1.isKey(), o2.isKey());
            if (result != 0) {
                return result * -1;
            }
            result = o1.getName().compareTo(o2.getName());
            return result;
        }

    }

    public static Type inferSchemaForField(FieldDesc fieldDesc) {
        if (fieldDesc instanceof CustomFieldDesc) {
            CustomFieldDesc customFieldInfo = (CustomFieldDesc) fieldDesc;
            CustomFieldRefType customFieldRefType = customFieldInfo.getCustomFieldType();
            switch (customFieldRefType) {
            case BOOLEAN:
                return Type.BOOLEAN;
            case LONG:
                return Type.LONG;
            case DOUBLE:
                return Type.DOUBLE;
            case DATE:
                return Type.DATETIME;
            default:
                return Type.STRING;
            }
        } else {
            Class<?> fieldType = fieldDesc.getValueType();
            if (fieldType == Boolean.TYPE || fieldType == Boolean.class) {
                return Type.BOOLEAN;
            } else if (fieldType == Integer.TYPE || fieldType == Integer.class) {
                return Type.INT;
            } else if (fieldType == Long.TYPE || fieldType == Long.class) {
                return Type.LONG;
            } else if (fieldType == Float.TYPE || fieldType == Float.class) {
                return Type.FLOAT;
            } else if (fieldType == Double.TYPE || fieldType == Double.class) {
                return Type.DOUBLE;
            } else if (fieldType == XMLGregorianCalendar.class) {
                return Type.DATETIME;
            } else {
                return Type.STRING;
            }
        }
    }
}