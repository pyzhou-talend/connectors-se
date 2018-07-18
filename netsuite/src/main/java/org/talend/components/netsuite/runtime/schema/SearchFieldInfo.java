package org.talend.components.netsuite.runtime.schema;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.ToString;

/**
 * Holds design-time specific information about single search field.
 */
@Data
@AllArgsConstructor
@ToString
public class SearchFieldInfo {

    /** Name of search field. */
    private String name;

    /** Type of value */
    private Class<?> valueType;
}
