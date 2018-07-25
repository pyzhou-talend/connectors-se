package org.talend.components.netsuite.dataset;

import java.util.List;

import org.talend.components.netsuite.datastore.NetsuiteDataStore;
import org.talend.sdk.component.api.configuration.Option;
import org.talend.sdk.component.api.configuration.action.Suggestable;
import org.talend.sdk.component.api.configuration.type.DataSet;
import org.talend.sdk.component.api.configuration.ui.layout.GridLayout;
import org.talend.sdk.component.api.configuration.ui.layout.GridLayouts;
import org.talend.sdk.component.api.meta.Documentation;

import lombok.Data;

@Data
@DataSet("input")
@GridLayouts({
        @GridLayout({ @GridLayout.Row({ "dataStore" }), @GridLayout.Row({ "recordType" }),
                @GridLayout.Row({ "searchCondition" }) }),
        @GridLayout(names = { GridLayout.FormType.ADVANCED }, value = { @GridLayout.Row({ "dataStore" }),
                @GridLayout.Row({ "bodyFieldsOnly" }) }) })
public class NetsuiteInputDataSet {

    @Option
    @Documentation("")
    private NetsuiteDataStore dataStore;

    @Option
    @Suggestable(value = "loadRecordTypes", parameters = { "../configuration.dataStore.account",
            "../configuration.dataStore.applicationId", "../configuration.dataStore.email",
            "../configuration.dataStore.enableCustomization", "../configuration.dataStore.endpoint",
            "../configuration.dataStore.password", "../configuration.dataStore.role" })
    @Documentation("TODO fill the documentation for this parameter")
    private String recordType;

    @Option
    @Documentation("TODO fill the documentation for this parameter")
    private List<SearchConditionConfiguration> searchCondition;

    @Option

    @Documentation("TODO fill the documentation for this parameter")
    private boolean bodyFieldsOnly;
}
