package org.talend.components.netsuite.dataset;

import java.util.List;

import org.talend.sdk.component.api.configuration.Option;
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
@GridLayouts({ @GridLayout({ @GridLayout.Row({ "commonDataSet" }), @GridLayout.Row({ "action" }) }),
        @GridLayout(names = { GridLayout.FormType.ADVANCED }, value = { @GridLayout.Row({ "commonDataSet" }),
                @GridLayout.Row({ "useNativeUpsert" }), @GridLayout.Row({ "schema" }), @GridLayout.Row({ "batchSize" }) }) })
public class NetsuiteOutputDataSet {

    public enum DataAction {
        ADD,
        UPDATE,
        UPSERT,
        DELETE
    }

    private List<String> schemaIn;

    @Option
    @Structure(discoverSchema = "guessOutputSchema", type = Type.OUT)
    @Documentation("")
    private List<String> schema;

    @Option
    @Documentation("")
    private NetSuiteCommonDataSet commonDataSet;

    private List<String> schemaRejected;

    @Option
    @Documentation("TODO fill the documentation for this parameter")
    private DataAction action = DataAction.ADD;

    @Option
    @Documentation("TODO fill the documentation for this parameter")
    private int batchSize = 1;

    @Option
    @ActiveIf(target = "action", value = "UPSERT")
    @Documentation("")
    private boolean useNativeUpsert = true;
}
