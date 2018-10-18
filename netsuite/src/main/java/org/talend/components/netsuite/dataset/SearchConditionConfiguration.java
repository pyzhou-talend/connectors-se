package org.talend.components.netsuite.dataset;

import java.io.Serializable;

import org.talend.sdk.component.api.configuration.Option;
import org.talend.sdk.component.api.configuration.action.Suggestable;
import org.talend.sdk.component.api.configuration.condition.ActiveIf;
import org.talend.sdk.component.api.configuration.condition.ActiveIf.EvaluationStrategy;
import org.talend.sdk.component.api.configuration.ui.layout.GridLayout;
import org.talend.sdk.component.api.meta.Documentation;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@GridLayout({ @GridLayout.Row({ "field" }), @GridLayout.Row({ "operator" }), @GridLayout.Row({ "value" }),
        @GridLayout.Row({ "value2" }) })
@Documentation("Search Condition parameters")
@Data
@NoArgsConstructor
@AllArgsConstructor
public class SearchConditionConfiguration implements Serializable {

    @Option
    @Suggestable(value = "loadFields", parameters = { "../../commonDataSet" })
    @Documentation("Field")
    private String field = "";

    @Option
    @Suggestable(value = "loadOperators", parameters = { "../../commonDataSet/dataStore" })
    @Documentation("Operator")
    private String operator;

    @Option
    @ActiveIf(target = "operator", negate = true, value = { "String.empty", "String.notEmpty", "Long.empty", "Long.notEmpty",
            "Double.empty", "Double.notEmpty", "Date.empty", "Date.notEmpty", "TextNumber.empty", "TextNumber.notEmpty",
            "PredefinedDate" }, evaluationStrategy = EvaluationStrategy.CONTAINS)
    @Documentation("Search Value")
    private String value;

    @Option
    @ActiveIf(target = "operator", value = { "Long.between", "Long.notBetween", "Double.between", "Double.notBetween",
            "Date.within", "Date.notWithin", "TextNumber.between", "TextNumber.notBetween" })
    @Documentation("Search Value2")
    private String value2;

}