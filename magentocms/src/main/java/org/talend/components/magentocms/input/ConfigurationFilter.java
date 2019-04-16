package org.talend.components.magentocms.input;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import org.talend.components.magentocms.helpers.ConfigurationHelper;
import org.talend.sdk.component.api.configuration.Option;
import org.talend.sdk.component.api.configuration.action.Updatable;
import org.talend.sdk.component.api.configuration.ui.layout.GridLayout;
import org.talend.sdk.component.api.meta.Documentation;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

@Getter
@NoArgsConstructor
@AllArgsConstructor
@GridLayout({ @GridLayout.Row({ "filterOperator" }), @GridLayout.Row({ "filterLines" }),
        @GridLayout.Row({ "filterAdvancedValueWrapper" }) })
@GridLayout(names = GridLayout.FormType.ADVANCED, value = { @GridLayout.Row({ "filterAdvancedValueWrapper" }) })
@Documentation("Selection filter, e.g. \"sku eq 'my sku 1' and name like '%test name%'\". "
        + "Use conditions (like, eq etc.). See Magento's 'Search using REST endpoints' article")
public class ConfigurationFilter implements Serializable {

    @Option
    @Documentation("Filter operator (OR, AND etc.) to join basic filter values")
    private SelectionFilterOperator filterOperator = SelectionFilterOperator.OR;

    @Option
    @Documentation("Basic filter values, contain column name, condition and value, eg. 'name like 123%'")
    private List<SelectionFilter> filterLines = new ArrayList<>();

    // @Option
    // @Documentation("Basic filter values, contain column name, condition and value, eg. 'name like 123%'")
    // private List<String> filterLinesTest = new ArrayList<>();

    @Option
    // @Suggestable(value = "SuggestFilterAdvanced", parameters = { "filterOperator", "filterLines" })
    // @TextArea
    @Documentation("Full text of advanced filter. Use '&' to join conditions. See Magento's 'Search using REST endpoints' article")
    @Updatable(value = ConfigurationHelper.UPDATABLE_FILTER_ADVANCED_ID, parameters = { "filterOperator",
            "filterLines" }, after = "filterAdvancedValue")
    private FilterAdvancedValueWrapper filterAdvancedValueWrapper = new FilterAdvancedValueWrapper();
}