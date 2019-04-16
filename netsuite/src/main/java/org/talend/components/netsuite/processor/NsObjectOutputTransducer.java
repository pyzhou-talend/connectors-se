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
package org.talend.components.netsuite.processor;

import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.talend.components.netsuite.datastore.NetSuiteDataStore.ApiVersion;
import org.talend.components.netsuite.runtime.NsObjectTransducer;
import org.talend.components.netsuite.runtime.client.NetSuiteClientService;
import org.talend.components.netsuite.runtime.client.NsRef;
import org.talend.components.netsuite.runtime.model.BasicRecordType;
import org.talend.components.netsuite.runtime.model.CustomRecordTypeInfo;
import org.talend.components.netsuite.runtime.model.FieldDesc;
import org.talend.components.netsuite.runtime.model.RecordTypeDesc;
import org.talend.components.netsuite.runtime.model.RecordTypeInfo;
import org.talend.components.netsuite.runtime.model.RefType;
import org.talend.components.netsuite.runtime.model.TypeDesc;
import org.talend.components.netsuite.runtime.model.beans.BeanInfo;
import org.talend.components.netsuite.runtime.model.beans.Beans;
import org.talend.components.netsuite.service.Messages;
import org.talend.sdk.component.api.record.Record;
import org.talend.sdk.component.api.record.Schema;
import org.talend.sdk.component.api.record.Schema.Entry;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Responsible for translating of input {@code Record} to output NetSuite data object.
 *
 * <p>
 * Output NetSuite data object can be {@code Record} or {@code RecordRef}.
 */
public class NsObjectOutputTransducer extends NsObjectTransducer {

    /** Name of target NetSuite data model object type. */
    private String typeName;

    /** Specifies whether output NetSuite data object is {@code RecordRef}. */
    private boolean reference;

    /** Descriptor of NetSuite data object type. */
    private TypeDesc typeDesc;

    /** Information about target record type. */
    private RecordTypeInfo recordTypeInfo;

    /** Information for picklist type */
    private String apiVersion;

    private Schema schema;

    public NsObjectOutputTransducer(NetSuiteClientService<?> clientService, Messages i18n, String typeName, Schema schema,
            String apiVersion) {
        super(clientService, i18n, apiVersion);
        this.typeName = typeName;
        this.schema = schema;
    }

    public boolean isReference() {
        return reference;
    }

    public void setReference(boolean reference) {
        this.reference = reference;
    }

    /**
     * Prepare processing of data object.
     */
    private void prepare() {
        if (typeDesc != null) {
            return;
        }

        recordTypeInfo = metaDataSource.getRecordType(typeName);
        if (reference) {
            // If target NetSuite data object is record ref then
            // we should get descriptor for RecordRef type.
            typeDesc = metaDataSource.getTypeInfo(recordTypeInfo.getRefType().getTypeName());
        } else {
            typeDesc = metaDataSource.getTypeInfo(typeName);
        }
    }

    /**
     * Translate input {@code Record} to output NetSuite data object.
     *
     * @param record record to be processed
     * @return NetSuite data object
     */
    public Object write(Record record) {
        prepare();

        Map<String, FieldDesc> fieldMap = typeDesc.getFieldMap();
        BeanInfo beanInfo = Beans.getBeanInfo(typeDesc.getTypeClass());

        String targetTypeName;
        if (recordTypeInfo != null && !reference) {
            RecordTypeDesc recordTypeDesc = recordTypeInfo.getRecordType();
            targetTypeName = recordTypeDesc.getTypeName();
        } else {
            targetTypeName = typeDesc.getTypeName();
        }

        Object nsObject = clientService.getBasicMetaData().createInstance(targetTypeName);

        // Names of fields to be null'ed.
        Set<String> nullFieldNames = new HashSet<>();

        // Custom fields by names.
        Map<String, Object> customFieldMap = Collections.emptyMap();

        if (!reference && beanInfo.getProperty(CUSTOM_FIELD_LIST) != null) {
            customFieldMap = new HashMap<>();

            Object customFieldListWrapper = Beans.getSimpleProperty(nsObject, CUSTOM_FIELD_LIST);
            if (customFieldListWrapper != null) {
                List<Object> customFieldList = (List<Object>) Beans.getSimpleProperty(customFieldListWrapper, CUSTOM_FIELD);
                for (Object customField : customFieldList) {
                    String scriptId = (String) Beans.getSimpleProperty(customField, SCRIPT_ID);
                    customFieldMap.put(scriptId, customField);
                }
            }
        }

        for (Entry entry : record.getSchema().getEntries()) {
            if (!schema.getEntries().stream().anyMatch(tempEntry -> entry.getName().equals(tempEntry.getName()))) {
                // TODO: Add logging that entry is not present in runtime schema
                continue;
            }
            String nsFieldName = Beans.toInitialLower(entry.getName());

            FieldDesc fieldDesc = fieldMap.get(nsFieldName);
            if (fieldDesc == null) {
                continue;
            }

            Object value = record.get(fieldDesc.getRecordValueType(), entry.getName());
            writeField(nsObject, fieldDesc, customFieldMap, nullFieldNames, value);
        }

        // Set record type identification data

        if (reference) {
            if (recordTypeInfo.getRefType() == RefType.RECORD_REF) {
                FieldDesc recTypeFieldDesc = typeDesc.getField(TYPE);
                RecordTypeDesc recordTypeDesc = recordTypeInfo.getRecordType();
                nullFieldNames.remove(TYPE);
                writeSimpleField(nsObject, recTypeFieldDesc.asSimple(), false, nullFieldNames, recordTypeDesc.getType());

            } else if (recordTypeInfo.getRefType() == RefType.CUSTOM_RECORD_REF) {
                CustomRecordTypeInfo customRecordTypeInfo = (CustomRecordTypeInfo) recordTypeInfo;
                NsRef customizationRef = customRecordTypeInfo.getCustomizationRef();

                FieldDesc typeIdFieldDesc = typeDesc.getField(TYPE_ID);
                nullFieldNames.remove(TYPE_ID);
                writeSimpleField(nsObject, typeIdFieldDesc.asSimple(), false, nullFieldNames, customizationRef.getInternalId());
            }
        } else if (recordTypeInfo != null) {
            RecordTypeDesc recordTypeDesc = recordTypeInfo.getRecordType();
            if (recordTypeDesc.getType().equals(BasicRecordType.CUSTOM_RECORD.getType())) {
                CustomRecordTypeInfo customRecordTypeInfo = (CustomRecordTypeInfo) recordTypeInfo;

                FieldDesc recTypeFieldDesc = typeDesc.getField(REC_TYPE);
                NsRef recordTypeRef = customRecordTypeInfo.getCustomizationRef();

                // Create custom record type ref as JSON to create native RecordRef
                ObjectNode recordRefNode = JsonNodeFactory.instance.objectNode();
                recordRefNode.set(INTERNAL_ID, JsonNodeFactory.instance.textNode(recordTypeRef.getInternalId()));
                recordRefNode.set(TYPE, JsonNodeFactory.instance.textNode(recordTypeDesc.getType()));

                nullFieldNames.remove(REC_TYPE);
                writeSimpleField(nsObject, recTypeFieldDesc.asSimple(), false, nullFieldNames, recordRefNode.toString());
            }
        }

        // Set null fields

        if (!nullFieldNames.isEmpty() && beanInfo.getProperty(NULL_FIELD_LIST) != null) {
            Object nullFieldListWrapper = clientService.getBasicMetaData().createInstance(NULL_FIELD);
            Beans.setSimpleProperty(nsObject, NULL_FIELD_LIST, nullFieldListWrapper);
            List<String> nullFields = (List<String>) Beans.getSimpleProperty(nullFieldListWrapper, NAME);
            nullFields.addAll(nullFieldNames);
        }

        return nsObject;
    }

    @Override
    public String getApiVersion() {
        return apiVersion;
    }

    public void setApiVersion(ApiVersion apiVersion) {
        this.apiVersion = apiVersion.getVersion();
    }

}