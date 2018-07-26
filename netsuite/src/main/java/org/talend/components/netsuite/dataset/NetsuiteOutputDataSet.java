package org.talend.components.netsuite.dataset;

import java.util.List;

import org.talend.components.netsuite.datastore.NetsuiteDataStore;
import org.talend.sdk.component.api.configuration.Option;
import org.talend.sdk.component.api.configuration.action.Suggestable;
import org.talend.sdk.component.api.configuration.type.DataSet;
import org.talend.sdk.component.api.configuration.ui.layout.GridLayout;
import org.talend.sdk.component.api.configuration.ui.layout.GridLayouts;
import org.talend.sdk.component.api.configuration.ui.widget.Structure;
import org.talend.sdk.component.api.configuration.ui.widget.Structure.Type;
import org.talend.sdk.component.api.meta.Documentation;

import lombok.Data;

@Data
@DataSet("output")
@GridLayouts({
        @GridLayout({ @GridLayout.Row({ "dataStore" }), @GridLayout.Row({ "recordType" }), @GridLayout.Row({ "action" }) }),
        @GridLayout(names = { GridLayout.FormType.ADVANCED }, value = { @GridLayout.Row({ "dataStore" }),
                @GridLayout.Row({ "schema" }), @GridLayout.Row({ "batchSize" }) }) })
public class NetsuiteOutputDataSet {

    enum DataAction {
        ADD,
        UPDATE,
        UPSERT,
        DELETE
    }

    @Option
    @Structure(discoverSchema = "guessSchema", type = Type.OUT)
    @Documentation("")
    private List<String> schema;

    @Option
    @Documentation("")
    private NetsuiteDataStore dataStore;

    @Option
    @Suggestable(value = "loadRecordTypes", parameters = { "dataStore" })
    @Documentation("TODO fill the documentation for this parameter")
    private String recordType;

    @Option
    @Documentation("TODO fill the documentation for this parameter")
    private DataAction action;

    @Option
    @Documentation("TODO fill the documentation for this parameter")
    private int batchSize;
}
