package org.talend.components.netsuite.dataset;

import org.talend.components.netsuite.datastore.NetsuiteDataStore;
import org.talend.sdk.component.api.configuration.Option;
import org.talend.sdk.component.api.configuration.action.Suggestable;
import org.talend.sdk.component.api.configuration.type.DataSet;
import org.talend.sdk.component.api.configuration.ui.layout.GridLayout;
import org.talend.sdk.component.api.configuration.ui.layout.GridLayouts;
import org.talend.sdk.component.api.meta.Documentation;

import lombok.Data;

@Data
@DataSet("output")
@GridLayouts({
        @GridLayout({ @GridLayout.Row({ "dataStore" }), @GridLayout.Row({ "recordType" }), @GridLayout.Row({ "action" }) }),
        @GridLayout(names = { GridLayout.FormType.ADVANCED }, value = { @GridLayout.Row({ "batchSize" }) }) })
public class NetsuiteOutputDataSet {

    enum DataAction {
        ADD,
        UPDATE,
        UPSERT,
        DELETE
    }

    @Option
    @Documentation("")
    private NetsuiteDataStore dataStore;

    @Option
    @Suggestable(value = "loadRecordTypes")
    @Documentation("TODO fill the documentation for this parameter")
    private String recordType;

    @Option
    @Documentation("TODO fill the documentation for this parameter")
    private DataAction action;

    @Option
    @Documentation("TODO fill the documentation for this parameter")
    private int batchSize;
}
