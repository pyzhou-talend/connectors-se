package org.talend.components.netsuite.runtime.schema;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import lombok.ToString;

/**
 * Holds design-time specific information about search data model.
 */

@ToString
public class SearchInfo {

    /** Name of search data object type */
    private String typeName;

    /** List of search fields */
    private List<SearchFieldInfo> fields;

    /** Map of search fields by names */
    private Map<String, SearchFieldInfo> fieldMap;

    public SearchInfo(String typeName, List<SearchFieldInfo> fields) {
        this.typeName = typeName;
        this.fields = fields;

        fieldMap = fields.stream().collect(Collectors.toMap(field -> field.getName(), field -> field));
    }

    public String getTypeName() {
        return typeName;
    }

    public List<SearchFieldInfo> getFields() {
        return fields;
    }

    public SearchFieldInfo getField(String name) {
        return fieldMap.get(name);
    }
}
