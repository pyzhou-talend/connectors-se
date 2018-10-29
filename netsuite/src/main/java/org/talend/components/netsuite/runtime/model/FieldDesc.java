package org.talend.components.netsuite.runtime.model;

import java.time.ZonedDateTime;

import javax.xml.datatype.XMLGregorianCalendar;

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

    public Class<?> getRecordValueType() {
        if (valueType == Boolean.TYPE || valueType == Boolean.class) {
            return Boolean.class;
        } else if (valueType == Integer.TYPE || valueType == Integer.class) {
            return Integer.class;
        } else if (valueType == Long.TYPE || valueType == Long.class) {
            return Long.class;
        } else if (valueType == Double.TYPE || valueType == Double.class) {
            return Double.class;
        } else if (valueType == Float.TYPE || valueType == Float.class) {
            return Float.class;
        } else if (valueType == XMLGregorianCalendar.class) {
            return ZonedDateTime.class;
        } else {
            return String.class;
        }
    }

    @SuppressWarnings("unchecked")
    public static <T> T getDefaultValue(Class<T> clazz) {
        if (clazz == Boolean.TYPE || clazz == Boolean.class) {
            return (T) Boolean.FALSE;
        } else if (clazz == Integer.TYPE || clazz == Integer.class) {
            return (T) Integer.valueOf(0);
        } else if (clazz == Long.TYPE || clazz == Long.class) {
            return (T) Long.valueOf(0);
        } else if (clazz == Double.TYPE || clazz == Double.class) {
            return (T) Double.valueOf(0.0);
        } else if (clazz == Float.TYPE || clazz == Float.class) {
            return (T) Float.valueOf(0.0f);
        } else {
            return null;
        }
    }
}
