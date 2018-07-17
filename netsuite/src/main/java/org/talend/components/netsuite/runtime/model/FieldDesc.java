package org.talend.components.netsuite.runtime.model;

import lombok.Data;

/**
 * Descriptor of data object type's field.
 *
 * @see TypeDesc
 * @see SimpleFieldDesc
 * @see CustomFieldDesc
 */
@Data
public abstract class FieldDesc {

    /** Name of field. */
    protected String name;

    /** Class of value stored by this field. */
    protected Class<?> valueType;

    /** Specifies whether this is key field. */
    protected boolean key;

    /** Specifies whether this field can accept {@code null} as value. */
    protected boolean nullable;

    /** Length of this field. */
    protected int length;

    /**
     * Get this field as {@link SimpleFieldDesc}.
     *
     * @return this field as {@link SimpleFieldDesc}
     */
    public SimpleFieldDesc asSimple() {
        return (SimpleFieldDesc) this;
    }

    /**
     * Get this field as {@link CustomFieldDesc}.
     *
     * @return this field as {@link CustomFieldDesc}
     */
    public CustomFieldDesc asCustom() {
        return (CustomFieldDesc) this;
    }
}
