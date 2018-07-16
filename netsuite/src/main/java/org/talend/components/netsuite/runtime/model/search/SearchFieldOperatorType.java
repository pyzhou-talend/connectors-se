package org.talend.components.netsuite.runtime.model.search;

import lombok.AllArgsConstructor;

@AllArgsConstructor
public enum SearchFieldOperatorType {
    BOOLEAN("Boolean", "SearchBooleanFieldOperator"),
    STRING("String", "SearchStringFieldOperator"),
    LONG("Long", "SearchLongFieldOperator"),
    DOUBLE("Double", "SearchDoubleFieldOperator"),
    DATE("Date", "SearchDateFieldOperator"),
    PREDEFINED_DATE("PredefinedDate", "SearchDate"),
    TEXT_NUMBER("TextNumber", "SearchTextNumberFieldOperator"),
    MULTI_SELECT("List", "SearchMultiSelectFieldOperator"),
    ENUM_MULTI_SELECT("List", "SearchEnumMultiSelectFieldOperator");

    private String dataType;

    private String operatorTypeName;

    public boolean dataTypeEquals(String dataType) {
        return this.dataType.equals(dataType);
    }

    public String getDataType() {
        return this.dataType;
    }

    public String getOperatorTypeName() {
        return operatorTypeName;
    }
}
