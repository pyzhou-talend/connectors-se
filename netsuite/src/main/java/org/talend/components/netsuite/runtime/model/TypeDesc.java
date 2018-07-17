package org.talend.components.netsuite.runtime.model;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import lombok.ToString;

/**
 * Descriptor of NetSuite data object model type.
 */

@ToString
public class TypeDesc {

    /** Short name of data object type. */
    private String typeName;

    /** Class of data object type. */
    private Class<?> typeClass;

    /** List of field descriptors for this data object type. */
    private List<FieldDesc> fields;

    /** Map of field descriptors by names, for faster access. */
    private Map<String, FieldDesc> fieldMap;

    public TypeDesc(String typeName, Class<?> typeClass, List<FieldDesc> fields) {
        this.typeName = typeName;
        this.typeClass = typeClass;
        this.fields = fields;

        fieldMap = fields.stream().collect(Collectors.toMap(FieldDesc::getName, fieldDesc -> fieldDesc));
    }

    public String getTypeName() {
        return typeName;
    }

    public Class<?> getTypeClass() {
        return typeClass;
    }

    public FieldDesc getField(String name) {
        return fieldMap.get(name);
    }

    public Map<String, FieldDesc> getFieldMap() {
        return Collections.unmodifiableMap(fieldMap);
    }

    public List<FieldDesc> getFields() {
        return Collections.unmodifiableList(fields);
    }
}
