package org.talend.components.netsuite.runtime.model;

/**
 * Descriptor of NetSuite record type.
 *
 * <p>
 * Implementation is provided by concrete version of NetSuite runtime.
 *
 * @see SearchRecordTypeDesc
 */
public interface RecordTypeDesc {

    /**
     * Name of record type.
     *
     * @return name
     */
    String getType();

    /**
     * Get short name of record data object type.
     *
     * @return short name of record data object type
     */
    String getTypeName();

    /**
     * Get class of record data object type.
     *
     * @return class
     */
    Class<?> getRecordClass();

    /**
     * Get name of search record type corresponding to this record type.
     *
     * @see SearchRecordTypeDesc#getType()
     *
     * @return name of search record type
     */
    String getSearchRecordType();
}
