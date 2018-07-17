package org.talend.components.netsuite.runtime.model.beans;

/**
 * Used to access properties for classes generated from NetSuite's XML schemas.
 */
public interface PropertyAccessor<T> {

    /**
     * Get value of a property.
     *
     * @param target target object
     * @param name name of property
     * @return value
     */
    Object get(T target, String name);

    /**
     * Set value of a property.
     *
     * @param target target object
     * @param name name of property
     * @param value value to be set
     */
    void set(T target, String name, Object value);

}
