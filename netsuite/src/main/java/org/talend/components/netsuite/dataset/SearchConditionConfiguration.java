package org.talend.components.netsuite.dataset;

import java.io.Serializable;

import org.talend.sdk.component.api.configuration.Option;
import org.talend.sdk.component.api.configuration.action.Suggestable;
import org.talend.sdk.component.api.configuration.ui.layout.GridLayout;
import org.talend.sdk.component.api.meta.Documentation;

import lombok.Data;

@GridLayout({
        // the generated layout put one configuration entry per line,
        // customize it as much as needed
        @GridLayout.Row({ "field" }), @GridLayout.Row({ "operator" }), @GridLayout.Row({ "value" }),
        @GridLayout.Row({ "value2" }) })
@Documentation("TODO fill the documentation for this configuration")
@Data
public class SearchConditionConfiguration implements Serializable {

    @Option
    @Suggestable(value = "loadFields", parameters = { "../../commonDataSet" })
    @Documentation("TODO fill the documentation for this parameter")
    private String field;

    @Option
    @Suggestable(value = "loadOperators", parameters = { "../../commonDataSet/dataStore" })
    @Documentation("TODO fill the documentation for this parameter")
    private String operator;

    @Option
    @Documentation("TODO fill the documentation for this parameter")
    private String value;

    @Option
    @Documentation("TODO fill the documentation for this parameter")
    private String value2;
}