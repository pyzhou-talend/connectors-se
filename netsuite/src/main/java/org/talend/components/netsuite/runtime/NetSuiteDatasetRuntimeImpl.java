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

import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import javax.xml.datatype.XMLGregorianCalendar;

import org.talend.components.netsuite.runtime.client.MetaDataSource;
import org.talend.components.netsuite.runtime.client.NetSuiteException;
import org.talend.components.netsuite.runtime.model.CustomFieldDesc;
import org.talend.components.netsuite.runtime.model.FieldDesc;
import org.talend.components.netsuite.runtime.model.SearchRecordTypeDesc;
import org.talend.components.netsuite.runtime.model.TypeDesc;
import org.talend.components.netsuite.runtime.model.beans.Beans;
import org.talend.components.netsuite.runtime.model.customfield.CustomFieldRefType;
import org.talend.components.netsuite.runtime.model.search.SearchFieldOperatorName;
import org.talend.components.netsuite.runtime.schema.SearchFieldInfo;
import org.talend.components.netsuite.runtime.schema.SearchInfo;
import org.talend.sdk.component.api.record.Schema;
import org.talend.sdk.component.api.record.Schema.Type;
import org.talend.sdk.component.api.service.completion.SuggestionValues;
import org.talend.sdk.component.api.service.record.RecordBuilderFactory;

import lombok.AllArgsConstructor;

/**
 * Provides information about NetSuite data set for components in design time or run time.
 */

@AllArgsConstructor
public class NetSuiteDatasetRuntimeImpl implements NetSuiteDatasetRuntime {

    /** Source of meta data. */
    private MetaDataSource metaDataSource;

    private final RecordBuilderFactory recordBuilderFactory;

    @Override
    public List<SuggestionValues.Item> getRecordTypes() {
        try {
            return metaDataSource.getRecordTypes().stream()
                    .map(record -> new SuggestionValues.Item(record.getName(), record.getDisplayName()))
                    .collect(Collectors.toList());
        } catch (NetSuiteException e) {
            throw new RuntimeException();
            // TODO:fix exception
        }
    }

    @Override
    public Schema getSchema(String typeName) {
        Schema.Builder builder = recordBuilderFactory.newSchemaBuilder(Type.RECORD);
        try {
            metaDataSource.getTypeInfo(typeName).getFields().stream().sorted(FieldDescComparator.INSTANCE)
                    .map(desc -> recordBuilderFactory.newEntryBuilder().withName(Beans.toInitialUpper(desc.getName()))
                            .withType(NetSuiteDatasetRuntimeImpl.inferSchemaForField(desc)).withNullable(desc.isNullable())
                            .withDefaultValue(FieldDesc.getDefaultValue(desc.getRecordValueType())).build())
                    .forEach(builder::withEntry);
            return builder.build();
        } catch (NetSuiteException e) {
            throw new RuntimeException();
            // TODO:fix exception
        }
    }

    @Override
    public SearchInfo getSearchInfo(String typeName) {
        try {
            final SearchRecordTypeDesc searchInfo = metaDataSource.getSearchRecordType(typeName);
            final TypeDesc searchRecordInfo = metaDataSource.getBasicMetaData().getTypeInfo(searchInfo.getSearchBasicClass());
            List<SearchFieldInfo> fields = searchRecordInfo.getFields().stream()
                    .map(fieldDesc -> new SearchFieldInfo(fieldDesc.getName(), fieldDesc.getValueType()))
                    .sorted((o1, o2) -> o1.getName().compareTo(o2.getName())).collect(Collectors.toList());
            return new SearchInfo(searchRecordInfo.getTypeName(), fields);

        } catch (NetSuiteException e) {
            throw new RuntimeException();
            // TODO:fix exception
        }
    }

    @Override
    public List<String> getSearchFieldOperators() {
        return metaDataSource.getBasicMetaData().getSearchOperatorNames().stream().map(SearchFieldOperatorName::getQualifiedName)
                .sorted().collect(Collectors.toList());
    }

    @Override
    public Schema getSchemaReject(String typeName, Schema schema) {
        return getSchemaForReject(schema, typeName + "_REJECT");
    }

    /**
     * Get schema for outgoing reject flow.
     *
     * @param schema schema to be used as base schema
     * @param newSchemaName name of new schema
     * @return schema
     */
    public Schema getSchemaForReject(Schema schema, String newSchemaName) {
        // Add errorCode and errorMessage schema fields.
        Schema.Builder builder = recordBuilderFactory.newSchemaBuilder(Type.RECORD);
        List<Schema.Entry> entries = new ArrayList<>(schema.getEntries());
        entries.add(
                recordBuilderFactory.newEntryBuilder().withName("errorCode").withType(Type.STRING).withDefaultValue("").build());
        entries.add(recordBuilderFactory.newEntryBuilder().withName("errorMessage").withType(Type.STRING).withDefaultValue("")
                .build());
        entries.forEach(builder::withEntry);

        return builder.build();
    }

    /**
     * Build and return map of custom field descriptors.
     *
     * @param fieldDescList list of custom field descriptors
     * @return map of custom field descriptors by names
     */
    public static Map<String, CustomFieldDesc> getCustomFieldDescMap(Collection<FieldDesc> fieldDescList) {
        return fieldDescList.stream().filter(CustomFieldDesc.class::isInstance).map(FieldDesc::asCustom)
                .collect(Collectors.toMap(CustomFieldDesc::getName, fieldDesc -> fieldDesc));
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

    /**
     * Return internal (NetSuite specific) name for a given <code>schema field</code>.
     *
     * @param field schema field
     * @return name
     */
    public static String getNsFieldName(Schema.Entry entry) {
        return Beans.toInitialLower(entry.getName());
    }

    public static boolean isSameType(Schema actual, Schema expect) {
        return actual.getType() == expect.getType();
    }

    /**
     * Find and return <code>schema field</code> by it's name.
     *
     * @param schema schema
     * @param fieldName name of field to be found
     * @return schema field or <code>null</code> if field was not found
     */
    public static Schema.Entry getNsFieldByName(Schema schema, String fieldName) {
        return schema.getEntries().stream().filter(field -> fieldName.equals(getNsFieldName(field))).findFirst().orElse(null);
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