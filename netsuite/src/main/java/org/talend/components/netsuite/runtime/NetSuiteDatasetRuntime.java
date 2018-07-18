package org.talend.components.netsuite.runtime;

import java.util.List;

import org.apache.avro.Schema;
import org.talend.components.netsuite.runtime.schema.SearchInfo;

/**
 * Provides information about NetSuite data set for components in design time or run time.
 */
public interface NetSuiteDatasetRuntime {

    /**
     * Get available record types.
     *
     * @return list of record types' names
     */
    List<NamedThing> getRecordTypes();

    /**
     * Get record types which can be used for search.
     *
     * @return list of record types' names
     */
    List<NamedThing> getSearchableTypes();

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
     * Get schema for record type.
     *
     * @param typeName name of target record type
     * @return schema
     */
    Schema getSchema(String typeName);

    /**
     * Get schema for record type and {@code Add/Update/Upsert} output action.
     *
     * @param typeName name of target record type
     * @return schema
     */
    Schema getSchemaForUpdate(String typeName);

    /**
     * Get schema for record type and {@code Delete} output action.
     *
     * @param typeName name of target record type
     * @return schema
     */
    Schema getSchemaForDelete(String typeName);

    /**
     * Get outgoing success flow schema for record type and {@code Add/Update/Upsert} output action.
     *
     * @param typeName name of target record type
     * @param schema schema to be used as base schema
     * @return schema
     */
    Schema getSchemaForUpdateFlow(String typeName, Schema schema);

    /**
     * Get outgoing success flow schema for record type and {@code Delete} output action.
     *
     * @param typeName name of target record type
     * @param schema schema to be used as base schema
     * @return schema
     */
    Schema getSchemaForDeleteFlow(String typeName, Schema schema);

    /**
     * Get outgoing reject flow schema for record type and {@code Add/Update/Upsert} output action.
     *
     * @param typeName name of target record type
     * @param schema schema to be used as base schema
     * @return schema
     */
    Schema getSchemaForUpdateReject(String typeName, Schema schema);

    /**
     * Get outgoing reject flow schema for record type and {@code Delete} output action.
     *
     * @param typeName name of target record type
     * @param schema schema to be used as base schema
     * @return schema
     */
    Schema getSchemaForDeleteReject(String typeName, Schema schema);
}
