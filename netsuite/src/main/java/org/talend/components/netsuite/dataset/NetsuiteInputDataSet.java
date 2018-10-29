package org.talend.components.netsuite.dataset;

import java.util.List;

import org.talend.sdk.component.api.configuration.Option;
import org.talend.sdk.component.api.configuration.ui.layout.GridLayout;
import org.talend.sdk.component.api.configuration.ui.layout.GridLayouts;
import org.talend.sdk.component.api.meta.Documentation;

import lombok.Data;

@Data
@GridLayouts({ @GridLayout({ @GridLayout.Row({ "commonDataSet" }), @GridLayout.Row({ "searchCondition" }) }),
        @GridLayout(names = { GridLayout.FormType.ADVANCED }, value = { @GridLayout.Row({ "bodyFieldsOnly" }) }) })
@Documentation("Properties for Input component")
public class NetsuiteInputDataSet {

    @Option
    @Documentation("Common dataset properties - datastore + module")
    private NetSuiteCommonDataSet commonDataSet;

    @Option
    @Documentation("Properties that are required for search")
    private List<SearchConditionConfiguration> searchCondition;

    @Option
    @Documentation("Shows or hides Item List result. Default true - hides, uncheck it to show")
    private boolean bodyFieldsOnly = true;
}
