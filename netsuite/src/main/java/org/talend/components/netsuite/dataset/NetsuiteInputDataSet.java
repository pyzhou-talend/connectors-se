package org.talend.components.netsuite.dataset;

import java.util.List;

import org.talend.sdk.component.api.configuration.Option;
import org.talend.sdk.component.api.configuration.ui.layout.GridLayout;
import org.talend.sdk.component.api.configuration.ui.layout.GridLayouts;
import org.talend.sdk.component.api.meta.Documentation;

import lombok.Data;

@Data
@GridLayouts({ @GridLayout({ @GridLayout.Row({ "commonDataSet" }), @GridLayout.Row({ "searchCondition" }) }),
        @GridLayout(names = { GridLayout.FormType.ADVANCED }, value = { @GridLayout.Row({ "commonDataSet" }),
                @GridLayout.Row({ "bodyFieldsOnly" }) }) })
public class NetsuiteInputDataSet {

    @Option
    @Documentation("")
    private NetSuiteCommonDataSet commonDataSet;

    @Option
    @Documentation("TODO fill the documentation for this parameter")
    private List<SearchConditionConfiguration> searchCondition;

    @Option
    @Documentation("TODO fill the documentation for this parameter")
    private boolean bodyFieldsOnly = true;
}
