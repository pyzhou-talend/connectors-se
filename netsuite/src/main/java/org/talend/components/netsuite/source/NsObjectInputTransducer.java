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

import java.time.ZonedDateTime;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;

import org.talend.components.netsuite.datastore.NetsuiteDataStore.ApiVersion;
import org.talend.components.netsuite.runtime.NsObjectTransducer;
import org.talend.components.netsuite.runtime.client.NetSuiteClientService;
import org.talend.components.netsuite.runtime.model.FieldDesc;
import org.talend.components.netsuite.runtime.model.TypeDesc;
import org.talend.components.netsuite.runtime.model.beans.Beans;
import org.talend.sdk.component.api.record.Record;
import org.talend.sdk.component.api.record.Schema;
import org.talend.sdk.component.api.record.Schema.Entry;
import org.talend.sdk.component.api.service.record.RecordBuilderFactory;

/**
 * Responsible for translating of input NetSuite record to {@code IndexedRecord} according to schema.
 */
public class NsObjectInputTransducer extends NsObjectTransducer {

    private final RecordBuilderFactory recordBuilderFactory;

    private final Schema runtimeSchema;

    /** Design schema for indexed record. */
    private List<String> schema;

    /** Descriptor of NetSuite data model object. */
    private TypeDesc typeDesc;

    private String apiVersion;

    public NsObjectInputTransducer(NetSuiteClientService<?> clientService, RecordBuilderFactory recordBuilderFactory,
            Schema runtimeSchema, List<String> schema, String typeName) {
        super(clientService);
        this.recordBuilderFactory = recordBuilderFactory;
        this.runtimeSchema = runtimeSchema;
        this.schema = schema;
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
    public Record read(Supplier<Object> supplier) {

        Map<String, FieldDesc> fieldMap = typeDesc.getFieldMap();
        Record.Builder builder = recordBuilderFactory.newRecordBuilder();

        Map<String, Object> mapView = getMapView(supplier.get(), runtimeSchema, typeDesc);
        for (String fieldName : schema) {
            String nsFieldName = Beans.toInitialLower(fieldName);
            FieldDesc fieldDesc = fieldMap.get(nsFieldName);
            if (fieldDesc == null) {
                continue;
            }

            Object value = readField(mapView, fieldDesc);
            Entry entry = runtimeSchema.getEntries().stream().filter(tempEntry -> fieldName.equalsIgnoreCase(tempEntry.getName()))
                    .findFirst().orElse(null);
            if (entry == null) {
                continue;
            }

            if (fieldDesc.getRecordValueType() == Boolean.class) {
                builder.withBoolean(entry, value == null ? entry.getDefaultValue() : (Boolean) value);
            } else if (fieldDesc.getRecordValueType() == Double.class) {
                builder.withDouble(entry, value == null ? entry.getDefaultValue() : (Double) value);
            } else if (fieldDesc.getRecordValueType() == Integer.class) {
                builder.withInt(entry, value == null ? entry.getDefaultValue() : (Integer) value);
            } else if (fieldDesc.getRecordValueType() == Long.class) {
                builder.withLong(entry, value == null ? entry.getDefaultValue() : (Long) value);
            } else if (fieldDesc.getRecordValueType() == ZonedDateTime.class) {
                builder.withDateTime(entry, value == null ? entry.getDefaultValue() : (ZonedDateTime) value);
            } else {
                builder.withString(entry, (String) value);
            }
        }
        return builder.build();
    }

    @Override
    public String getApiVersion() {
        return apiVersion;
    }

    public void setApiVersion(ApiVersion apiVersion) {
        this.apiVersion = apiVersion.getVersion();
    }

}
