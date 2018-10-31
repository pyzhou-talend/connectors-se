package org.talend.components.netsuite.dataset;

import java.util.List;

import org.talend.sdk.component.api.configuration.Option;
import org.talend.sdk.component.api.configuration.condition.ActiveIf;
import org.talend.sdk.component.api.configuration.ui.layout.GridLayout;
import org.talend.sdk.component.api.configuration.ui.layout.GridLayouts;
import org.talend.sdk.component.api.meta.Documentation;

import lombok.Data;

@Data
@GridLayouts({ @GridLayout({ @GridLayout.Row({ "dataSet" }), @GridLayout.Row({ "action" }) }), @GridLayout(names = {
        GridLayout.FormType.ADVANCED }, value = { @GridLayout.Row({ "dataSet" }), @GridLayout.Row({ "useNativeUpsert" }) }) })
@Documentation("Properties for Output component")
public class NetSuiteOutputProperties {

    /**
     * Basic operation with NetSuite records.
     *
     */
    public enum DataAction {
        ADD,
        UPDATE,
        UPSERT,
        DELETE
    }

    private List<String> schemaIn;

    private List<String> schemaRejected;

    @Option
    @Documentation("Common dataset properties - datastore + module")
    private NetSuiteDataSet dataSet;

    @Option
    @Documentation("Operation to be performed with records. Default - ADD")
    private DataAction action = DataAction.ADD;

    @Option
    @ActiveIf(target = "action", value = "UPSERT")
    @Documentation("Changes UPSERT strategy. Default - true, uses NetSuite upsert; otherwise - custom")
    private boolean useNativeUpsert = true;
}
