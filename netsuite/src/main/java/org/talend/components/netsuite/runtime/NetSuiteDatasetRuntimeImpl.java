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
            // throw new ComponentException(e);
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
            // throw new ComponentException(e);
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
            // throw new ComponentException(e);
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
    //
    // /**
    // TODO:
    // * Infers an Avro schema for the given type. This can be an expensive operation so the schema
    // * should be cached where possible. This is always an {@link Schema.Type#RECORD}.
    // *
    // * @param name name of a record.
    // * @return the schema for data given from the object.
    // */
    // public static Schema inferSchemaForType(String name, List<FieldDesc> fieldDescList) {
    // List<Schema.Field> fields = new ArrayList<>();
    //
    // for (FieldDesc fieldDesc : fieldDescList) {
    // final String fieldName = fieldDesc.getName();
    // final String avroFieldName = Beans.toInitialUpper(fieldName);
    //
    // Schema.Field avroField = new Schema.Field(avroFieldName, inferSchemaForField(fieldDesc), null, (Object) null);
    //
    // // Add some Talend6 custom properties to the schema.
    //
    // Schema avroFieldSchema = unwrapIfNullable(avroField.schema());
    //
    // avroField.addProp(SchemaConstants.TALEND_COLUMN_DB_COLUMN_NAME, fieldDesc.getName());
    //
    // if (isSameType(avroFieldSchema, Schema.create(Schema.Type.STRING))) {
    // if (fieldDesc.getLength() != 0) {
    // avroField.addProp(SchemaConstants.TALEND_COLUMN_DB_LENGTH, String.valueOf(fieldDesc.getLength()));
    // }
    // }
    //
    // if (fieldDesc instanceof CustomFieldDesc) {
    // CustomFieldDesc customFieldInfo = (CustomFieldDesc) fieldDesc;
    // CustomFieldRefType customFieldRefType = customFieldInfo.getCustomFieldType();
    //
    // avroField.addProp(SchemaConstants.TALEND_COLUMN_DB_TYPE, customFieldRefType.getTypeName());
    //
    // if (customFieldRefType == CustomFieldRefType.DATE) {
    // avroField.addProp(SchemaConstants.TALEND_COLUMN_PATTERN, "yyyy-MM-dd'T'HH:mm:ss'.000Z'");
    // }
    //
    // NsRef ref = customFieldInfo.getCustomizationRef();
    // Optional<String> temp = Optional.ofNullable(ref.getName()).filter(((Predicate<String>)
    // String::isEmpty).negate());
    // if (temp.isPresent()) {
    // avroField.addProp(NetSuiteSchemaConstants.TALEND6_COMMENT, temp.get());
    // }
    //
    // } else {
    // Class<?> fieldType = fieldDesc.getValueType();
    //
    // avroField.addProp(SchemaConstants.TALEND_COLUMN_DB_TYPE, fieldType.getSimpleName());
    //
    // if (fieldType == XMLGregorianCalendar.class) {
    // avroField.addProp(SchemaConstants.TALEND_COLUMN_PATTERN, "yyyy-MM-dd'T'HH:mm:ss'.000Z'");
    // }
    // }
    //
    // if (avroField.defaultVal() != null) {
    // avroField.addProp(SchemaConstants.TALEND_COLUMN_DEFAULT, String.valueOf(avroField.defaultVal()));
    // }
    //
    // if (fieldDesc.isKey()) {
    // avroField.addProp(SchemaConstants.TALEND_COLUMN_IS_KEY, Boolean.TRUE.toString());
    // }
    //
    // fields.add(avroField);
    // }
    //
    // return Schema.createRecord(name, null, null, false, fields);
    // }

    // /**
    // TODO:
    // * Infers an Avro schema for the given FieldDesc. This can be an expensive operation so the schema should be
    // * cached where possible. The return type will be the Avro Schema that can contain the fieldDesc data without loss
    // * of
    // * precision.
    // *
    // * @param fieldDesc the <code>FieldDesc</code> to analyse.
    // * @return the schema for data that the fieldDesc describes.
    // */
    // public static Schema inferSchemaForField(FieldDesc fieldDesc) {
    // Schema base = Schema.create(Schema.Type.STRING);
    //
    // if (fieldDesc instanceof CustomFieldDesc) {
    // CustomFieldDesc customFieldInfo = (CustomFieldDesc) fieldDesc;
    // CustomFieldRefType customFieldRefType = customFieldInfo.getCustomFieldType();
    //
    // if (customFieldRefType == CustomFieldRefType.BOOLEAN) {
    // base = Schema.create(Schema.Type.BOOLEAN);
    // } else if (customFieldRefType == CustomFieldRefType.LONG) {
    // base = Schema.create(Schema.Type.LONG);
    // } else if (customFieldRefType == CustomFieldRefType.DOUBLE) {
    // base = Schema.create(Schema.Type.DOUBLE);
    // } else if (customFieldRefType == CustomFieldRefType.DATE) {
    // base = LogicalTypes.timestampMillis().addToSchema(Schema.create(Schema.Type.LONG));
    // } else if (customFieldRefType == CustomFieldRefType.STRING) {
    // base = Schema.create(Schema.Type.STRING);
    // } else {
    // base = Schema.create(Schema.Type.STRING);
    // }
    //
    // } else {
    // Class<?> fieldType = fieldDesc.getValueType();
    //
    // if (fieldType == Boolean.TYPE || fieldType == Boolean.class) {
    // base = Schema.create(Schema.Type.BOOLEAN);
    // } else if (fieldType == Integer.TYPE || fieldType == Integer.class) {
    // base = Schema.create(Schema.Type.INT);
    // } else if (fieldType == Long.TYPE || fieldType == Long.class) {
    // base = Schema.create(Schema.Type.LONG);
    // } else if (fieldType == Float.TYPE || fieldType == Float.class) {
    // base = Schema.create(Schema.Type.FLOAT);
    // } else if (fieldType == Double.TYPE || fieldType == Double.class) {
    // base = Schema.create(Schema.Type.DOUBLE);
    // } else if (fieldType == XMLGregorianCalendar.class) {
    // base = LogicalTypes.timestampMillis().addToSchema(Schema.create(Schema.Type.LONG));
    // } else if (fieldType == String.class) {
    // base = Schema.create(Schema.Type.STRING);
    // } else if (fieldType.isEnum()) {
    // base = Schema.create(Schema.Type.STRING);
    // } else {
    // base = Schema.create(Schema.Type.STRING);
    // }
    // }
    //
    // base = fieldDesc.isNullable() ? wrapAsNullable(base) : base;
    //
    // return base;
    // }

    // /**
    // TODO:
    // * Augment a given <code>Schema</code> with customization related meta data.
    // *
    // * @param metaDataSource source of meta data
    // * @param schema schema to be augmented
    // * @param recordTypeInfo information about record type to be used for augmentation
    // * @param fieldDescList list of field descriptors to be used for augmentation
    // */
    // public static void augmentSchemaWithCustomMetaData(final MetaDataSource metaDataSource, final Schema schema,
    // final RecordTypeInfo recordTypeInfo, final Collection<FieldDesc> fieldDescList) {
    //
    // if (recordTypeInfo == null) {
    // // Not a record, do nothing
    // return;
    // }
    // // Add custom record type meta data to a key field
    // Optional.of(recordTypeInfo).filter(CustomRecordTypeInfo.class::isInstance).map(CustomRecordTypeInfo.class::cast)
    // .ifPresent(customRecordTypeInfo -> schema.getFields().stream()
    // .forEach(field -> writeCustomRecord(metaDataSource.getBasicMetaData(), field, customRecordTypeInfo)));
    // // Add custom field meta data to fields
    // if (fieldDescList != null && !fieldDescList.isEmpty()) {
    // Map<String, CustomFieldDesc> customFieldDescMap = getCustomFieldDescMap(fieldDescList);
    // if (!customFieldDescMap.isEmpty()) {
    // for (Schema.Field field : schema.getFields()) {
    // String nsFieldName = getNsFieldName(field);
    // CustomFieldDesc customFieldDesc = customFieldDescMap.get(nsFieldName);
    // if (customFieldDesc != null) {
    // writeCustomField(field, customFieldDesc);
    // }
    // }
    // }
    // }
    // }

    // /**
    // TODO:
    // * Extend a schema with additional fields.
    // *
    // * @param sourceSchema source schema
    // * @param newSchemaName name of new schema
    // * @param fieldsToAdd fields to be added
    // * @return new schema
    // */
    // public static Schema extendSchema(Schema sourceSchema, String newSchemaName, List<Schema.Field> fieldsToAdd) {
    // Schema newSchema = Schema.createRecord(newSchemaName, sourceSchema.getDoc(), sourceSchema.getNamespace(),
    // sourceSchema.isError());
    //
    // List<Schema.Field> copyFieldList = sourceSchema.getFields().stream().map(NetSuiteDatasetRuntimeImpl::copyField)
    // .collect(Collectors.toList());
    //
    // copyFieldList.addAll(fieldsToAdd);
    //
    // newSchema.setFields(copyFieldList);
    //
    // for (Map.Entry<String, Object> entry : sourceSchema.getObjectProps().entrySet()) {
    // newSchema.addProp(entry.getKey(), entry.getValue());
    // }
    //
    // return newSchema;
    // }
    //
    // /**
    // TODO:
    // * Copy a schema field.
    // *
    // * @param sourceField source field to be copied
    // * @return new field
    // */
    // public static Schema.Field copyField(final Schema.Field sourceField) {
    // Schema.Field field = new Schema.Field(sourceField.name(), sourceField.schema(), sourceField.doc(),
    // sourceField.defaultVal(), sourceField.order());
    // field.getObjectProps().putAll(sourceField.getObjectProps());
    // for (Map.Entry<String, Object> entry : sourceField.getObjectProps().entrySet()) {
    // field.addProp(entry.getKey(), entry.getValue());
    // }
    // return field;
    // }

    // /**
    // TODO:
    // * Write custom record meta data to a given <code>JsonProperties</code>.
    // *
    // * @see NetSuiteSchemaConstants
    // *
    // * @param basicMetaData basic meta data
    // * @param properties properties object which to write meta data to
    // * @param recordTypeInfo information about record type to be used
    // */
    // public static void writeCustomRecord(BasicMetaData basicMetaData, Field properties, CustomRecordTypeInfo
    // recordTypeInfo) {
    // NsRef ref = recordTypeInfo.getCustomizationRef();
    // RecordTypeDesc recordTypeDesc = recordTypeInfo.getRecordType();
    //
    // properties.addProp(NetSuiteSchemaConstants.NS_CUSTOM_RECORD, "true");
    // properties.addProp(NetSuiteSchemaConstants.NS_CUSTOM_RECORD_SCRIPT_ID, ref.getScriptId());
    // properties.addProp(NetSuiteSchemaConstants.NS_CUSTOM_RECORD_INTERNAL_ID, ref.getInternalId());
    // properties.addProp(NetSuiteSchemaConstants.NS_CUSTOM_RECORD_CUSTOMIZATION_TYPE, ref.getType());
    // properties.addProp(NetSuiteSchemaConstants.NS_CUSTOM_RECORD_TYPE, recordTypeDesc.getType());
    // }

    // /**
    // TODO:
    // * Read custom record meta data from a given <code>JsonProperties</code>.
    // *
    // * @see NetSuiteSchemaConstants
    // *
    // * @param basicMetaData basic meta data
    // * @param properties properties object which to read meta data from
    // * @return custom record type info or <code>null</code> if meta data was not found
    // */
    // public static CustomRecordTypeInfo readCustomRecord(BasicMetaData basicMetaData, Field properties) {
    // String scriptId = properties.getProp(NetSuiteSchemaConstants.NS_CUSTOM_RECORD_SCRIPT_ID);
    // if (scriptId == null || scriptId.isEmpty()) {
    // return null;
    // }
    // String internalId = properties.getProp(NetSuiteSchemaConstants.NS_CUSTOM_RECORD_INTERNAL_ID);
    // String customizationType = properties.getProp(NetSuiteSchemaConstants.NS_CUSTOM_RECORD_CUSTOMIZATION_TYPE);
    // String recordType = properties.getProp(NetSuiteSchemaConstants.NS_CUSTOM_RECORD_TYPE);
    //
    // NsRef ref = new NsRef();
    // ref.setRefType(RefType.CUSTOMIZATION_REF);
    // ref.setScriptId(scriptId);
    // ref.setInternalId(internalId);
    // ref.setType(customizationType);
    //
    // RecordTypeDesc recordTypeDesc = basicMetaData.getRecordType(recordType);
    // CustomRecordTypeInfo recordTypeInfo = new CustomRecordTypeInfo(scriptId, recordTypeDesc, ref);
    //
    // return recordTypeInfo;
    // }

    // /**
    // TODO:
    // * Write custom field meta data to a given <code>JsonProperties</code>.
    // *
    // * @see NetSuiteSchemaConstants
    // *
    // * @param properties properties object which to write meta data to
    // * @param fieldDesc information about custom field to be used
    // */
    // public static void writeCustomField(Field properties, CustomFieldDesc fieldDesc) {
    // NsRef ref = fieldDesc.getCustomizationRef();
    // CustomFieldRefType customFieldRefType = fieldDesc.getCustomFieldType();
    //
    // properties.addProp(NetSuiteSchemaConstants.NS_CUSTOM_FIELD, "true");
    // properties.addProp(NetSuiteSchemaConstants.NS_CUSTOM_FIELD_SCRIPT_ID, ref.getScriptId());
    // properties.addProp(NetSuiteSchemaConstants.NS_CUSTOM_FIELD_INTERNAL_ID, ref.getInternalId());
    // properties.addProp(NetSuiteSchemaConstants.NS_CUSTOM_FIELD_CUSTOMIZATION_TYPE, ref.getType());
    // properties.addProp(NetSuiteSchemaConstants.NS_CUSTOM_FIELD_TYPE, customFieldRefType.name());
    // }

    // /**
    // TODO:
    // * Read custom field meta data from a given <code>JsonProperties</code>.
    // *
    // * @see NetSuiteSchemaConstants
    // *
    // * @param properties properties object which to read meta data from
    // * @return custom field info or <code>null</code> if meta data was not found
    // */
    // public static CustomFieldDesc readCustomField(Field properties) {
    // String scriptId = properties.getProp(NetSuiteSchemaConstants.NS_CUSTOM_FIELD_SCRIPT_ID);
    // if (scriptId == null || scriptId.isEmpty()) {
    // return null;
    // }
    // String internalId = properties.getProp(NetSuiteSchemaConstants.NS_CUSTOM_FIELD_INTERNAL_ID);
    // String customizationType = properties.getProp(NetSuiteSchemaConstants.NS_CUSTOM_FIELD_CUSTOMIZATION_TYPE);
    // String type = properties.getProp(NetSuiteSchemaConstants.NS_CUSTOM_FIELD_TYPE);
    //
    // NsRef ref = new NsRef();
    // ref.setRefType(RefType.CUSTOMIZATION_REF);
    // ref.setScriptId(scriptId);
    // ref.setInternalId(internalId);
    // ref.setType(customizationType);
    //
    // CustomFieldRefType customFieldRefType = CustomFieldRefType.valueOf(type);
    //
    // CustomFieldDesc fieldDesc = new CustomFieldDesc();
    // fieldDesc.setCustomFieldType(customFieldRefType);
    // fieldDesc.setCustomizationRef(ref);
    // fieldDesc.setName(scriptId);
    // fieldDesc.setValueType(getCustomFieldValueClass(customFieldRefType));
    // fieldDesc.setNullable(true);
    //
    // return fieldDesc;
    // }

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