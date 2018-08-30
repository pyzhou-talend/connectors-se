package org.talend.components.magentocms.input;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import org.talend.sdk.component.api.configuration.Option;
import org.talend.sdk.component.api.configuration.action.Suggestable;
import org.talend.sdk.component.api.configuration.ui.layout.GridLayout;
import org.talend.sdk.component.api.meta.Documentation;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

@Getter
@NoArgsConstructor
@AllArgsConstructor
@GridLayout({ @GridLayout.Row({ "filterOperator" }), @GridLayout.Row({ "filterLines" }) })
@GridLayout(names = GridLayout.FormType.ADVANCED, value = { @GridLayout.Row({ "filterAdvancedValue" }) })
public class ConfigurationFilter implements Serializable {

    @Option
    @Documentation("Entity filter operator")
    private SelectionFilterOperator filterOperator = SelectionFilterOperator.OR;

    // selection filter, e.g. "sku eq 'MY SKU 1' and name like '%test name%'"
    // use conditions (like, eq etc.) from magento's REST help page
    @Option
    @Documentation("Entity filters")
    private List<SelectionFilter> filterLines = new ArrayList<>();

    @Option
    @Suggestable(value = "SuggestFilterAdvanced", parameters = { "filterOperator", "filterLines" })
    // @TextArea
    @Documentation("Full text of advanced filter")
    private String filterAdvancedValue = "";
}