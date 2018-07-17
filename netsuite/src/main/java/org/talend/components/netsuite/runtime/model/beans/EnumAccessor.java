package org.talend.components.netsuite.runtime.model.beans;

/**
 * Used to access enum values for enum classes generated from NetSuite's XML schemas.
 */
public interface EnumAccessor {

    /**
     * Get string value for given enum constant.
     *
     * @param enumValue enum value
     * @return string value
     */
    String getStringValue(Enum<?> enumValue);

    /**
     * Get enum constant for given string value.
     *
     * @param value string value
     * @return enum constant
     */
    Enum<?> getEnumValue(String value);
}
