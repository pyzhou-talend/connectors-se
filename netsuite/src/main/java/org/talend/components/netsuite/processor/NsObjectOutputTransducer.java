package org.talend.components.netsuite.processor;
// ============================================================================
//
// Copyright (C) 2006-2017 Talend Inc. - www.talend.com
//
// This source code is available under agreement available at
// %InstallDIR%\features\org.talend.rcp.branding.%PRODUCTNAME%\%PRODUCTNAME%license.txt
//
// You should have received a copy of the agreement
// along with this program; if not, write to Talend SA
// 9 rue Pages 92150 Suresnes, France
//
// ============================================================================

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.talend.components.netsuite.datastore.NetsuiteDataStore.ApiVersion;
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
import org.talend.sdk.component.api.record.Record;
import org.talend.sdk.component.api.record.Schema;

import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;

/**
 * Responsible for translating of input {@code IndexedRecord} to output NetSuite data object.
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

    private final List<String> designFields;

    public NsObjectOutputTransducer(NetSuiteClientService<?> clientService, String typeName, List<String> designFields) {
        super(clientService);

        this.typeName = typeName;
        this.designFields = designFields;
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
     * Translate input {@code IndexedRecord} to output NetSuite data object.
     *
     * @param indexedRecord indexed record to be processed
     * @return NetSuite data object
     */
    public Object write(Record record) {
        prepare();

        Map<String, FieldDesc> fieldMap = typeDesc.getFieldMap();
        BeanInfo beanInfo = Beans.getBeanInfo(typeDesc.getTypeClass());

        Schema schema = record.getSchema();

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

        if (!reference && beanInfo.getProperty("customFieldList") != null) {
            customFieldMap = new HashMap<>();

            Object customFieldListWrapper = Beans.getSimpleProperty(nsObject, "customFieldList");
            if (customFieldListWrapper != null) {
                List<Object> customFieldList = (List<Object>) Beans.getSimpleProperty(customFieldListWrapper, "customField");
                for (Object customField : customFieldList) {
                    String scriptId = (String) Beans.getSimpleProperty(customField, "scriptId");
                    customFieldMap.put(scriptId, customField);
                }
            }
        }

        for (String fieldName : designFields) {
            Schema.Entry entry = record.getSchema().getEntries().stream().filter(name -> name.getName().equals(fieldName))
                    .findFirst().get();
            String nsFieldName = Beans.toInitialLower(entry.getName());

            FieldDesc fieldDesc = fieldMap.get(nsFieldName);
            if (fieldDesc == null) {
                continue;
            }
            // TODO: Wrong impl.
            Object value = record.get(entry.getDefaultValue(), entry.getName());

            writeField(nsObject, fieldDesc, customFieldMap, nullFieldNames, value);
        }

        // Set record type identification data

        if (reference) {
            if (recordTypeInfo.getRefType() == RefType.RECORD_REF) {
                FieldDesc recTypeFieldDesc = typeDesc.getField("type");
                RecordTypeDesc recordTypeDesc = recordTypeInfo.getRecordType();
                nullFieldNames.remove("type");
                writeSimpleField(nsObject, recTypeFieldDesc.asSimple(), false, nullFieldNames, recordTypeDesc.getType());

            } else if (recordTypeInfo.getRefType() == RefType.CUSTOM_RECORD_REF) {
                CustomRecordTypeInfo customRecordTypeInfo = (CustomRecordTypeInfo) recordTypeInfo;
                NsRef customizationRef = customRecordTypeInfo.getCustomizationRef();

                FieldDesc typeIdFieldDesc = typeDesc.getField("typeId");
                nullFieldNames.remove("typeId");
                writeSimpleField(nsObject, typeIdFieldDesc.asSimple(), false, nullFieldNames, customizationRef.getInternalId());
            }
        } else if (recordTypeInfo != null) {
            RecordTypeDesc recordTypeDesc = recordTypeInfo.getRecordType();
            if (recordTypeDesc.getType().equals(BasicRecordType.CUSTOM_RECORD.getType())) {
                CustomRecordTypeInfo customRecordTypeInfo = (CustomRecordTypeInfo) recordTypeInfo;

                FieldDesc recTypeFieldDesc = typeDesc.getField("recType");
                NsRef recordTypeRef = customRecordTypeInfo.getCustomizationRef();

                // Create custom record type ref as JSON to create native RecordRef
                ObjectNode recordRefNode = JsonNodeFactory.instance.objectNode();
                recordRefNode.set("internalId", JsonNodeFactory.instance.textNode(recordTypeRef.getInternalId()));
                recordRefNode.set("type", JsonNodeFactory.instance.textNode(recordTypeDesc.getType()));

                nullFieldNames.remove("recType");
                writeSimpleField(nsObject, recTypeFieldDesc.asSimple(), false, nullFieldNames, recordRefNode.toString());
            }
        }

        // Set null fields

        if (!nullFieldNames.isEmpty() && beanInfo.getProperty("nullFieldList") != null) {
            Object nullFieldListWrapper = clientService.getBasicMetaData().createInstance("NullField");
            Beans.setSimpleProperty(nsObject, "nullFieldList", nullFieldListWrapper);
            List<String> nullFields = (List<String>) Beans.getSimpleProperty(nullFieldListWrapper, "name");
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