package org.talend.components.netsuite.runtime.model;

import java.util.HashMap;
import java.util.Map;

import org.talend.components.netsuite.runtime.model.search.SearchFieldOperatorType;
import org.talend.components.netsuite.runtime.model.search.SearchFieldOperatorTypeDesc;

public class BasicMetaData {

    protected Map<String, Class<?>> typeMap = new HashMap<>();

    protected Map<String, Class<?>> searchFieldMap = new HashMap<>();

    protected Map<SearchFieldOperatorType, SearchFieldOperatorTypeDesc> searchFieldOperatorTypeMap = new HashMap<>();
}
