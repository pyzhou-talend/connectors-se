package org.talend.components.netsuite.runtime.model.search;

import java.util.List;

import org.talend.components.netsuite.runtime.model.BasicMetaData;
import org.talend.components.netsuite.runtime.model.beans.Beans;

/**
 * Search field adapter for {@code SearchLongField} and {@code SearchLongCustomField}.
 */
public class SearchLongFieldAdapter<T> extends SearchFieldAdapter<T> {

    public SearchLongFieldAdapter(BasicMetaData metaData, SearchFieldType fieldType, Class<T> fieldClass) {
        super(metaData, fieldType, fieldClass);
    }

    @Override
    public T populate(T fieldObject, String internalId, String operatorName, List<String> values) {
        T nsObject = fieldObject != null ? fieldObject : createField(internalId);

        if (values != null && values.size() != 0) {
            Beans.setSimpleProperty(nsObject, "searchValue", Long.valueOf(Long.parseLong(values.get(0))));
            String temp;
            if (values.size() > 1 && (temp = values.get(1)) != null && !temp.isEmpty()) {
                Beans.setSimpleProperty(nsObject, "searchValue2", Long.valueOf(Long.parseLong(temp)));
            }
        }

        Beans.setSimpleProperty(nsObject, "operator",
                metaData.getSearchFieldOperatorByName(fieldType.getFieldTypeName(), operatorName));

        return nsObject;
    }
}
