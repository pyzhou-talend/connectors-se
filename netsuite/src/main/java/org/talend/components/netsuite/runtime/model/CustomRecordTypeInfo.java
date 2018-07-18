package org.talend.components.netsuite.runtime.model;

import org.talend.components.netsuite.runtime.client.NsRef;

import lombok.ToString;

/**
 * Holds information about custom record type.
 */

@ToString
public class CustomRecordTypeInfo extends RecordTypeInfo {

    /** Customization ref for this record type. */
    private NsRef customizationRef;

    /**
     * Create instance of custom record type info using name, record type descriptor and
     * customization ref.
     *
     * @param name name of custom record type
     * @param recordType record type descriptor
     * @param ref customization ref
     */
    public CustomRecordTypeInfo(String name, RecordTypeDesc recordType, NsRef customizationRef) {
        super(name, recordType);
        this.customizationRef = customizationRef;
    }

    @Override
    public String getDisplayName() {
        return customizationRef.getName();
    }

    /**
     * Get customization ref.
     *
     * @return customization ref
     */
    public NsRef getCustomizationRef() {
        return customizationRef;
    }

    @Override
    public RefType getRefType() {
        return RefType.CUSTOM_RECORD_REF;
    }
}
