package org.talend.components.magentocms.input;

import lombok.Data;
import org.talend.components.magentocms.common.MagentoCmsConfigurationBase;
import org.talend.sdk.component.api.configuration.Option;
import org.talend.sdk.component.api.configuration.type.DataSet;
import org.talend.sdk.component.api.configuration.ui.layout.GridLayout;
import org.talend.sdk.component.api.configuration.ui.widget.Structure;
import org.talend.sdk.component.api.meta.Documentation;

import java.util.ArrayList;
import java.util.List;

import static org.talend.sdk.component.api.configuration.ui.widget.Structure.Type.OUT;

@Data
@DataSet("MagentoInput")
@GridLayout({ @GridLayout.Row({ "magentoCmsConfigurationBase" }), @GridLayout.Row({ "selectionType" }),
        // @GridLayout.Row({ "selectionId" }),
        @GridLayout.Row({ "selectionFilter" }) })
@GridLayout(names = GridLayout.FormType.ADVANCED, value = { @GridLayout.Row({ "magentoCmsConfigurationBase" }),
        @GridLayout.Row({ "fields" }), @GridLayout.Row({ "selectionFilter" }), @GridLayout.Row({ "selectedFields" }) })
@Documentation("TODO fill the documentation for this configuration")
public class MagentoCmsInputMapperConfiguration {

    @Option
    @Documentation("magento CMS connection configuration")
    private MagentoCmsConfigurationBase magentoCmsConfigurationBase;

    // selection type, e.g. 'Products'
    @Option
    @Documentation("The type of information we want to get")
    private SelectionType selectionType;

    // // selection id, e.g. sku for 'products' selection type
    // @Option
    // @Documentation("The Id of entity we want to get")
    // private String selectionId;

    // @Option
    // @Documentation("Entity filter operator")
    // private SelectionFilterOperator selectionFilterOperator;
    //
    // // selection filter, e.g. "sku eq 'MY SKU 1' and name like '%test name%'"
    // // use conditions (like, eq etc.) from magento's REST help page
    // @Option
    // @Documentation("Entity filters")
    // private List<SelectionFilter> selectionFilter;
    //
    // @Option
    // @Documentation("Use advanced filter")
    // private boolean selectionFilterUseAdvanced;
    //
    // @Option
    // @ActiveIf(target = "selectionFilterUseAdvanced", value = "true")
    // @Documentation("Full text of advanced filter")
    // private String selectionFilterAdvancedValue;
    @Option
    @Documentation("Entity filter")
    private ConfigurationFilter selectionFilter = new ConfigurationFilter();

    // selection filter, e.g. "sku eq 'MY SKU 1' and name like '%test name%'"
    // use conditions (like, eq etc.) from magento's REST help page
    @Option
    @Documentation("Entity fields, use it as explained in magento's help 'Retrieve filtered responses for REST APIs'")
    private String selectedFields;

    @Option
    @Structure(discoverSchema = "guessTableSchema", type = OUT)
    // @Proposable("Proposable_GetTableFields")
    @Documentation("List of field names to return in the response.")
    private List<String> fields = new ArrayList<>();

}