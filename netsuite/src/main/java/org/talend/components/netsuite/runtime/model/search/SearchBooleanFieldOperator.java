package org.talend.components.netsuite.runtime.model.search;


public enum SearchBooleanFieldOperator {
    BOOLEAN("Boolean");

    private final String value;

    private SearchBooleanFieldOperator(String value) {
        this.value = value;
    }

    public String getValue() {
        return value;
    }
}
