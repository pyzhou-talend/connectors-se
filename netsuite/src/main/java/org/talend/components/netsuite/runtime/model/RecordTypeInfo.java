package org.talend.components.netsuite.runtime.model;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.ToString;

/**
 * Holds information about record type.
 */

@Data
@ToString
@AllArgsConstructor
public class RecordTypeInfo {

    /**
     * Name identifying record type.
     *
     * <p>
     * For standard record types name is the same as {@link RecordTypeDesc#getTypeName()}.
     */
    protected String name;

    /** Record type descriptor. */
    protected RecordTypeDesc recordType;

    /**
     * Create instance of record type info using given record type descriptor.
     *
     * @param recordType record type descriptor
     */
    public RecordTypeInfo(RecordTypeDesc recordType) {
        this.name = recordType.getTypeName();
        this.recordType = recordType;
    }

    /**
     * Get display name of record type.
     *
     * @return display name
     */
    public String getDisplayName() {
        return recordType.getTypeName();
    }

    /**
     * Get type of reference for this record type.
     *
     * @return type of reference
     */
    public RefType getRefType() {
        return RefType.RECORD_REF;
    }
}
