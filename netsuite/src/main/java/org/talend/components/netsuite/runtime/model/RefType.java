package org.talend.components.netsuite.runtime.model;

import lombok.AllArgsConstructor;

/**
 * Type of NetSuite reference.
 */
@AllArgsConstructor
public enum RefType {
    RECORD_REF("RecordRef"),
    CUSTOM_RECORD_REF("CustomRecordRef"),
    CUSTOMIZATION_REF("CustomizationRef");

    /** Short name of NetSuite native reference object type. */
    private String typeName;

    public String getTypeName() {
        return typeName;
    }

    /**
     * Get reference type enum constant by name of reference data object type.
     *
     * @param typeName reference data object type
     * @return reference type enum constant
     * @throws IllegalArgumentException if type name don't match known any type name
     */
    public static RefType getByTypeName(String typeName) {
        for (RefType value : values()) {
            if (value.typeName.equals(typeName)) {
                return value;
            }
        }
        throw new IllegalArgumentException("Invalid type name: " + typeName);
    }
}
