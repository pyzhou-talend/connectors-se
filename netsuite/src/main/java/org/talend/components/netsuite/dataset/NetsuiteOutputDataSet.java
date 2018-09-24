package org.talend.components.netsuite.dataset;

import java.util.List;

import org.talend.sdk.component.api.configuration.Option;
import org.talend.sdk.component.api.configuration.condition.ActiveIf;
import org.talend.sdk.component.api.configuration.ui.layout.GridLayout;
import org.talend.sdk.component.api.configuration.ui.layout.GridLayouts;
import org.talend.sdk.component.api.meta.Documentation;

import lombok.Data;

@Data
@GridLayouts({ @GridLayout({ @GridLayout.Row({ "commonDataSet" }), @GridLayout.Row({ "action" }) }),
        @GridLayout(names = { GridLayout.FormType.ADVANCED }, value = { @GridLayout.Row({ "commonDataSet" }),
                @GridLayout.Row({ "useNativeUpsert" }) }) })
public class NetsuiteOutputDataSet {

    public enum DataAction {
        ADD,
        UPDATE,
        UPSERT,
        DELETE
    }

    private List<String> schemaIn;

    private List<String> schemaRejected;

    @Option
    @Documentation("")
    private NetSuiteCommonDataSet commonDataSet;

    @Option
    @Documentation("TODO fill the documentation for this parameter")
    private DataAction action = DataAction.ADD;

    @Option
    @ActiveIf(target = "action", value = "UPSERT")
    @Documentation("")
    private boolean useNativeUpsert = true;
}
