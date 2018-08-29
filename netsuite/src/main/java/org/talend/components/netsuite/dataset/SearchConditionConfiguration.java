package org.talend.components.netsuite.dataset;

import java.io.Serializable;

import org.talend.sdk.component.api.configuration.Option;
import org.talend.sdk.component.api.configuration.action.Suggestable;
import org.talend.sdk.component.api.configuration.condition.ActiveIf;
import org.talend.sdk.component.api.configuration.ui.layout.GridLayout;
import org.talend.sdk.component.api.meta.Documentation;

import lombok.Data;

@GridLayout({ @GridLayout.Row({ "field" }), @GridLayout.Row({ "operator" }), @GridLayout.Row({ "value" }),
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
    @ActiveIf(target = "operator", negate = true, value = { "String.empty", "String.notEmpty", "Long.empty", "Long.notEmpty",
            "Double.empty", "Double.notEmpty", "Date.empty", "Date.notEmpty", "TextNumber.empty", "TextNumber.notEmpty" })
    @Documentation("TODO fill the documentation for this parameter")
    private String value;

    @Option
    @ActiveIf(target = "operator", value = { "Long.between", "Long.notBetween", "Double.between", "Double.notBetween",
            "Date.within", "Date.notWithin", "TextNumber.between", "TextNumber.notBetween" })
    @Documentation("TODO fill the documentation for this parameter")
    private String value2;

}