package org.talend.components.netsuite.source;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import javax.json.JsonBuilderFactory;
import javax.json.JsonObject;
import javax.json.JsonObjectBuilder;

import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
import org.apache.avro.generic.IndexedRecord;
import org.apache.commons.lang3.StringUtils;
import org.talend.components.netsuite.dataset.NetsuiteInputDataSet;
import org.talend.components.netsuite.runtime.NetSuiteEndpoint;
import org.talend.components.netsuite.runtime.client.NetSuiteClientService;
import org.talend.components.netsuite.runtime.client.NetSuiteException;
import org.talend.components.netsuite.runtime.client.ResultSet;
import org.talend.components.netsuite.runtime.client.search.SearchCondition;
import org.talend.components.netsuite.runtime.client.search.SearchQuery;
import org.talend.components.netsuite.runtime.model.RecordTypeInfo;
import org.talend.components.netsuite.service.NetsuiteService;
import org.talend.sdk.component.api.configuration.Option;
import org.talend.sdk.component.api.input.Producer;
import org.talend.sdk.component.api.meta.Documentation;

@Documentation("TODO fill the documentation for this source")
public class NetsuiteInputSource implements Serializable {

    private final NetsuiteInputDataSet configuration;

    private final NetsuiteService service;

    private final JsonBuilderFactory jsonBuilderFactory;

    private Schema schema;

    private NetSuiteClientService<?> clientService;

    private ResultSet<?> rs;

    private NsObjectInputTransducer transducer;

    public NetsuiteInputSource(@Option("configuration") final NetsuiteInputDataSet configuration, final NetsuiteService service,
            final JsonBuilderFactory jsonBuilderFactory) {
        this.configuration = configuration;
        this.service = service;
        this.jsonBuilderFactory = jsonBuilderFactory;
    }

    @PostConstruct
    public void init() {
        service.connect(NetSuiteEndpoint.createConnectionConfig(configuration.getDataStore()));
        schema = service.getAvroSchema(configuration.getRecordType());
        clientService = service.getClientService();
        rs = search();
        // this method will be executed once for the whole component execution,
        // this is where you can establish a connection for instance
    }

    @Producer
    public JsonObject next() {
        if (rs.next()) {
            JsonObjectBuilder builder = jsonBuilderFactory.createObjectBuilder();
            Object record = rs.get();
            IndexedRecord indexedRecord = transducer.read(record);
            schema.getFields().stream().forEach(field -> convertIndexedRecordToJsonObject(field, builder, indexedRecord));
            return builder.build();
        }
        // this is the method allowing you to go through the dataset associated
        // to the component configuration
        //
        // return null means the dataset has no more data to go through
        // you can use the jsonBuilderFactory to create new JsonObjects.
        return null;
    }

    @PreDestroy
    public void release() {
        // this is the symmetric method of the init() one,
        // release potential connections you created or data you cached

        // Nothing to close
    }

    /**
     * Build and execute NetSuite search query.
     *
     * @return
     * @throws NetSuiteException if an error occurs during execution of search
     */
    private ResultSet<?> search() throws NetSuiteException {
        SearchQuery search = buildSearchQuery();

        RecordTypeInfo recordTypeInfo = search.getRecordTypeInfo();

        // Set up object translator
        transducer = new NsObjectInputTransducer(clientService, schema, recordTypeInfo.getName());

        ResultSet<?> resultSet = search.search();
        return resultSet;
    }

    /**
     * Build search query from properties.
     *
     * @return search query object
     */
    private SearchQuery buildSearchQuery() {
        String target = configuration.getRecordType();

        SearchQuery search = clientService.newSearch(clientService.getMetaDataSource());
        search.target(target);

        // Build search conditions

        // List<String> fieldNames = properties.module.searchQuery.field.getValue();
        // if (fieldNames != null && !fieldNames.isEmpty()) {
        // for (int i = 0; i < fieldNames.size(); i++) {
        // String fieldName = fieldNames.get(i);
        // String operator = properties.module.searchQuery.operator.getValue().get(i);
        // Object value1 = properties.module.searchQuery.value1.getValue().get(i);
        // Object value2 = properties.module.searchQuery.value2.getValue().get(i);
        // search.condition(buildSearchCondition(fieldName, operator, value1, value2));
        // }
        // }

        return search;
    }

    /**
     * Build search condition.
     *
     * @param fieldName name of search field
     * @param operator name of search operator
     * @param value1 first search value
     * @param value2 second search value
     * @return
     */
    private SearchCondition buildSearchCondition(String fieldName, String operator, Object value1, Object value2) {
        List<String> values = buildSearchConditionValueList(value1, value2);
        return new SearchCondition(fieldName, operator, values);
    }

    /**
     * Build search value list.
     *
     * @param value1 first search value
     * @param value2 second search value
     * @return
     */
    private List<String> buildSearchConditionValueList(Object value1, Object value2) {
        if (value1 == null) {
            return null;
        }

        List<String> valueList;
        // First, check whether first value is collection of values
        if (value1 instanceof Collection) {
            Collection<?> elements = (Collection<?>) value1;
            valueList = new ArrayList<>(elements.size());
            for (Object elemValue : elements) {
                if (elemValue != null) {
                    valueList.add(elemValue.toString());
                }
            }
        } else {
            // Create value list from value pair
            valueList = new ArrayList<>(2);
            String sValue1 = value1 != null ? value1.toString() : null;
            if (StringUtils.isNotEmpty(sValue1)) {
                valueList.add(sValue1);

                String sValue2 = value2 != null ? value2.toString() : null;
                if (StringUtils.isNotEmpty(sValue2)) {
                    valueList.add(sValue2);
                }
            }
        }

        return valueList;
    }

    private void convertIndexedRecordToJsonObject(Field field, JsonObjectBuilder builder, IndexedRecord indexedRecord) {

        Object value = indexedRecord.get(field.pos());
        if (value == null) {
            builder.addNull(field.name());
        } else {
            builder.add(field.name(), value.toString());
        }
    }
}