package org.talend.components.netsuite.runtime.model;

/**
 * Descriptor of NetSuite search record type.
 *
 * <p>
 * Implementation is provided by concrete version of NetSuite runtime.
 *
 * @see RecordTypeDesc
 */
public interface SearchRecordTypeDesc {

    /**
     * Name of search record type.
     *
     * @return name
     */
    String getType();

    /**
     * Get short name of record data object type.
     *
     * @see RecordTypeDesc#getTypeName()
     *
     * @return short name of record data object type
     */
    String getTypeName();

    /**
     * Get class of main search record data object type.
     *
     * @return class or {@code null} if search type doesn't have main search record
     */
    Class<?> getSearchClass();

    /**
     * Get class of search record basic data object type.
     *
     * @return class
     */
    Class<?> getSearchBasicClass();

    /**
     * Get class of search record advanced data object type.
     *
     * @return class or {@code null} if search type doesn't have advanced search record
     */
    Class<?> getSearchAdvancedClass();
}
