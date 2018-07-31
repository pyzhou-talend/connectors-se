package org.talend.components.netsuite.dataset;

import java.util.List;

import org.talend.components.netsuite.datastore.NetsuiteDataStore;
import org.talend.sdk.component.api.configuration.Option;
import org.talend.sdk.component.api.configuration.action.Suggestable;
import org.talend.sdk.component.api.configuration.condition.ActiveIf;
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
                @GridLayout.Row({ "useNativeUpsert" }), @GridLayout.Row({ "schemaIn" }), @GridLayout.Row({ "schemaOut" }),
                @GridLayout.Row({ "schemaRejected" }), @GridLayout.Row({ "batchSize" }) }) })
public class NetsuiteOutputDataSet {

    public enum DataAction {
        ADD,
        UPDATE,
        UPSERT,
        DELETE
    }

    @Option
    @Documentation("")
    private List<String> schemaIn;

    @Option
    @Structure(discoverSchema = "guessOutputSchema", type = Type.OUT)
    @Documentation("")
    private List<String> schemaOut;

    @Option
    @Documentation("")
    private List<String> schemaRejected;

    @Option
    @Documentation("")
    private NetsuiteDataStore dataStore;

    @Option
    @Suggestable(value = "loadRecordTypes", parameters = { "dataStore" })
    @Documentation("TODO fill the documentation for this parameter")
    private String recordType;

    @Option
    @Documentation("TODO fill the documentation for this parameter")
    private DataAction action = DataAction.ADD;

    @Option
    @Documentation("TODO fill the documentation for this parameter")
    private int batchSize;

    @Option
    @ActiveIf(target = "action", value = "UPSERT")
    @Documentation("")
    private boolean useNativeUpsert = false;
}
