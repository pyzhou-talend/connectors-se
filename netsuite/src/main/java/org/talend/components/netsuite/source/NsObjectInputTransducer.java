// ============================================================================
//
// Copyright (C) 2006-2018 Talend Inc. - www.talend.com
//
// This source code is available under agreement available at
// %InstallDIR%\features\org.talend.rcp.branding.%PRODUCTNAME%\%PRODUCTNAME%license.txt
//
// You should have received a copy of the agreement
// along with this program; if not, write to Talend SA
// 9 rue Pages 92150 Suresnes, France
//
// ============================================================================
package org.talend.components.netsuite.source;

import java.util.List;
import java.util.Map;

import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.IndexedRecord;
import org.talend.components.netsuite.datastore.NetsuiteDataStore.ApiVersion;
import org.talend.components.netsuite.runtime.NetSuiteDatasetRuntimeImpl;
import org.talend.components.netsuite.runtime.NsObjectTransducer;
import org.talend.components.netsuite.runtime.client.NetSuiteClientService;
import org.talend.components.netsuite.runtime.model.FieldDesc;
import org.talend.components.netsuite.runtime.model.TypeDesc;

/**
 * Responsible for translating of input NetSuite record to {@code IndexedRecord} according to schema.
 */
public class NsObjectInputTransducer extends NsObjectTransducer {

    /** Design schema for indexed record. */
    private List<String> schema;

    /** Actual schema for indexed record. */
    private Schema runtimeSchema;

    /** Descriptor of NetSuite data model object. */
    private TypeDesc typeDesc;

    private String apiVersion;

    public NsObjectInputTransducer(NetSuiteClientService<?> clientService, List<String> schema, String typeName,
            Schema runtimeSchema) {
        super(clientService);

        this.schema = schema;
        this.runtimeSchema = runtimeSchema;
        this.typeDesc = metaDataSource.getTypeInfo(typeName);
    }

    public List<String> getSchema() {
        return schema;
    }

    /**
     * Translate NetSuite data model object to {@code IndexedRecord}.
     *
     * @param data NetSuite data object
     * @return indexed record
     */
    public IndexedRecord read(Object data) {

        Map<String, FieldDesc> fieldMap = typeDesc.getFieldMap();
        Map<String, Object> mapView = getMapView(data, runtimeSchema, typeDesc);

        GenericRecord indexedRecord = new GenericData.Record(runtimeSchema);
        for (String fieldName : schema) {
            Field field = runtimeSchema.getField(fieldName);
            String nsFieldName = NetSuiteDatasetRuntimeImpl.getNsFieldName(field);

            FieldDesc fieldDesc = fieldMap.get(nsFieldName);
            if (fieldDesc == null) {
                continue;
            }
            Object value = readField(mapView, fieldDesc);
            indexedRecord.put(field.name(), value);
        }

        return indexedRecord;
    }

    @Override
    public String getApiVersion() {
        return apiVersion;
    }

    public void setApiVersion(ApiVersion apiVersion) {
        this.apiVersion = apiVersion.getVersion();
    }

}
