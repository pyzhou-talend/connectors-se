package org.talend.components.netsuite.runtime.model;

import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.ToString;

/**
 * Descriptor of field which is declared as {@code member field} in data object type class.
 */

@Data
@EqualsAndHashCode(callSuper = false)
@ToString
public class SimpleFieldDesc extends FieldDesc {

    /** Name of property ({@code member field}) corresponding this field. */
    private String propertyName;

    public SimpleFieldDesc() {
    }

    public SimpleFieldDesc(String name, Class<?> valueType, boolean key, boolean nullable) {
        this.name = name;
        this.valueType = valueType;
        this.key = key;
        this.nullable = nullable;
    }

    public String getPropertyName() {
        return propertyName;
    }

    public void setPropertyName(String propertyName) {
        this.propertyName = propertyName;
    }
}
