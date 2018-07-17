package org.talend.components.netsuite.runtime.model.search;

import java.util.List;

import org.talend.components.netsuite.runtime.model.BasicMetaData;
import org.talend.components.netsuite.runtime.model.beans.Beans;

/**
 * Search field adapter for {@code SearchMultiSelectField} and {@code SearchMultiSelectCustomField}.
 */
public class SearchMultiSelectFieldAdapter<T> extends SearchFieldAdapter<T> {

    public SearchMultiSelectFieldAdapter(BasicMetaData metaData, SearchFieldType fieldType, Class<T> fieldClass) {
        super(metaData, fieldType, fieldClass);
    }

    @Override
    public T populate(T fieldObject, String internalId, String operatorName, List<String> values) {
        T nsObject = fieldObject != null ? fieldObject : createField(internalId);

        List<Object> searchValue = (List<Object>) Beans.getSimpleProperty(nsObject, "searchValue");

        for (int i = 0; i < values.size(); i++) {
            String temp;
            if ((temp = values.get(i)) != null && !temp.isEmpty()) {
                Object item;
                if (SearchFieldType.MULTI_SELECT == fieldType) {
                    item = metaData.createInstance("RecordRef");
                } else if (SearchFieldType.CUSTOM_MULTI_SELECT == fieldType) {
                    item = metaData.createInstance("ListOrRecordRef");
                    Beans.setSimpleProperty(item, "name", temp);
                } else {
                    continue;
                }
                Beans.setSimpleProperty(item, "internalId", temp);
                searchValue.add(item);
            }
        }

        Beans.setSimpleProperty(nsObject, "operator",
                metaData.getSearchFieldOperatorByName(fieldType.getFieldTypeName(), operatorName));

        return nsObject;
    }
}
