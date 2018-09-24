package org.talend.components.netsuite.runtime;

import java.util.List;

import org.apache.avro.Schema;
import org.talend.components.netsuite.runtime.schema.SearchInfo;
import org.talend.sdk.component.api.service.completion.SuggestionValues;

/**
 * Provides information about NetSuite data set for components in design time or run time.
 */
public interface NetSuiteDatasetRuntime {

    /**
     * Get available record types.
     *
     * @return list of record types' names
     */
    List<SuggestionValues.Item> getRecordTypes();

    /**
     * Get information about search data model.
     *
     * @param typeName name of target record type
     * @return search data model info
     */
    SearchInfo getSearchInfo(String typeName);

    /**
     * Get available search operators.
     *
     * @return list of search operators' names
     */
    List<String> getSearchFieldOperators();

    /**
     * Get schema entry for record type.
     *
     * @param typeName name of target record type
     * @return List of Entries
     */
    org.talend.sdk.component.api.record.Schema getSchema(String typeName);

    /**
     * Get schema for record type.
     *
     * @param typeName name of target record type
     * @return schema
     */
    Schema getAvroSchema(String typeName);

    /**
     * Get outgoing reject flow schema for record type.
     *
     * @param typeName name of target record type
     * @param schema schema to be used as base schema
     * @return schema
     */
    Schema getSchemaReject(String typeName, Schema schema);
}
