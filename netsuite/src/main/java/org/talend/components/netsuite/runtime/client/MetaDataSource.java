package org.talend.components.netsuite.runtime.client;

import java.util.Collection;

import org.talend.components.netsuite.runtime.model.BasicMetaData;
import org.talend.components.netsuite.runtime.model.RecordTypeDesc;
import org.talend.components.netsuite.runtime.model.RecordTypeInfo;
import org.talend.components.netsuite.runtime.model.SearchRecordTypeDesc;
import org.talend.components.netsuite.runtime.model.TypeDesc;
import org.talend.sdk.component.api.service.completion.Values;

/**
 * Provides information about NetSuite data model.
 */
public interface MetaDataSource {

    boolean isCustomizationEnabled();

    void setCustomizationEnabled(boolean customizationEnabled);

    /**
     * Return meta data about basic (standard) data model, without customizations.
     *
     * @return basic meta data model
     */
    BasicMetaData getBasicMetaData();

    /**
     * Return source of customization related meta data.
     *
     * @return customization meta data source
     */
    CustomMetaDataSource getCustomMetaDataSource();

    /**
     * Set a new source of customization related meta data.
     *
     * @param customMetaDataSource customization meta data source to be set
     */
    void setCustomMetaDataSource(CustomMetaDataSource customMetaDataSource);

    /**
     * Return all available record types including custom record types.
     *
     * @return list of record types
     */
    Collection<RecordTypeInfo> getRecordTypes();

    /**
     * Return all available types which are searchable, including custom record types.
     *
     * @return list of searchable types' names
     */
    Collection<Values.Item> getSearchableTypes();

    /**
     * Return type descriptor for a given model object's class
     *
     * @param clazz model object's class
     * @return type descriptor
     */
    TypeDesc getTypeInfo(Class<?> clazz);

    /**
     * Return type descriptor for a given model object type's name
     *
     * @param typeName model object type's name
     * @return type descriptor
     */
    TypeDesc getTypeInfo(String typeName);

    /**
     * Return information about a record type by it's name.
     *
     * @param typeName name of record type
     * @return record type information
     */
    RecordTypeInfo getRecordType(String typeName);

    /**
     * Return search record type descriptor by a record type's name.
     *
     * @param recordTypeName name of record type
     * @return search record type descriptor
     */
    SearchRecordTypeDesc getSearchRecordType(String recordTypeName);

    /**
     * Return search record type descriptor by a record type descriptor.
     *
     * @param recordType record type descriptor
     * @return search record type descriptor
     */
    SearchRecordTypeDesc getSearchRecordType(RecordTypeDesc recordType);
}
