package org.talend.components.netsuite.runtime;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import org.apache.avro.Schema;
import org.talend.components.netsuite.runtime.client.CustomMetaDataSource;
import org.talend.components.netsuite.runtime.model.BasicMetaData;
import org.talend.components.netsuite.runtime.model.CustomFieldDesc;
import org.talend.components.netsuite.runtime.model.CustomRecordTypeInfo;
import org.talend.components.netsuite.runtime.model.RecordTypeInfo;

/**
 * Implementation of <code>CustomMetaDataSource</code> which uses Avro <code>schema</code>
 * as <code>source</code> of customization meta data.
 *
 * <p>
 * <code>SchemaCustomMetaDataSource</code> extracts customization meta data from a schema.
 * If meta data is present in schema then it is returned to a requester, otherwise
 * <code>SchemaCustomMetaDataSource</code> redirects a request to <code>default source</code>.
 *
 * <p>
 * If schema is <code>dynamic</code> then <code>SchemaCustomMetaDataSource</code> doesn't use
 * <code>schema</code> as source of meta data and redirects a request to <code>default source</code>.
 */
public class SchemaCustomMetaDataSource implements CustomMetaDataSource {

    private BasicMetaData basicMetaData;

    private CustomMetaDataSource defaultSource;

    private Schema schema;

    private CustomRecordTypeInfo customRecordTypeInfo;

    private Map<String, CustomFieldDesc> customFieldDescMap;

    /**
     * Create <code>SchemaCustomMetaDataSource</code> using given <code>basic meta data</code>,
     * <code>default source</code> and <code>schema</code>.
     *
     * @param basicMetaData basic meta data object
     * @param defaultSource default source to be used as secondary source meta data
     * @param schema schema to be used as primary source of meta data
     */
    public SchemaCustomMetaDataSource(BasicMetaData basicMetaData, CustomMetaDataSource defaultSource, Schema schema) {
        this.basicMetaData = basicMetaData;
        this.defaultSource = defaultSource;
        this.schema = schema;
    }

    @Override
    public Collection<CustomRecordTypeInfo> getCustomRecordTypes() {
        return defaultSource.getCustomRecordTypes();
    }

    @Override
    public Map<String, CustomFieldDesc> getCustomFields(RecordTypeInfo recordTypeInfo) {
        return Optional.ofNullable(schema.getProp("include-all-fields")).isPresent()
                ? defaultSource.getCustomFields(recordTypeInfo)
                : Optional.ofNullable(customFieldDescMap).orElse((customFieldDescMap = loadCustomFieldDescMap()));
    }

    private Map<String, CustomFieldDesc> loadCustomFieldDescMap() {
        schema.getFields().stream().map(field -> Optional.of(NetSuiteDatasetRuntimeImpl.readCustomField(field)));
        Map<String, CustomFieldDesc> customFieldDescMap = new HashMap<>();
        for (Schema.Field field : schema.getFields()) {
            CustomFieldDesc customFieldDesc = NetSuiteDatasetRuntimeImpl.readCustomField(field);
            if (customFieldDesc != null) {
                customFieldDescMap.put(customFieldDesc.getName(), customFieldDesc);
            }
        }
        return customFieldDescMap;
    }

    @Override
    public CustomRecordTypeInfo getCustomRecordType(String typeName) {

        if (customRecordTypeInfo == null) {
            customRecordTypeInfo = loadCustomRecordTypeInfo();
        }

        return Optional.ofNullable(customRecordTypeInfo)
                .filter((customRecordTypeInfo) -> customRecordTypeInfo.getName().equals(typeName))
                .orElse(defaultSource.getCustomRecordType(typeName));
    }

    private CustomRecordTypeInfo loadCustomRecordTypeInfo() {
        for (Schema.Field field : schema.getFields()) {
            CustomRecordTypeInfo customRecordTypeInfo = NetSuiteDatasetRuntimeImpl.readCustomRecord(basicMetaData, field);
            if (customRecordTypeInfo != null) {
                return customRecordTypeInfo;
            }
        }
        return null;
    }
}