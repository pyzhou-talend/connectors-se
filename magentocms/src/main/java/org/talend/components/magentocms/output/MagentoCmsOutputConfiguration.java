package org.talend.components.magentocms.output;

import lombok.Getter;
import org.talend.components.magentocms.common.MagentoCmsConfigurationBase;
import org.talend.components.magentocms.input.SelectionType;
import org.talend.sdk.component.api.configuration.Option;
import org.talend.sdk.component.api.configuration.type.DataSet;
import org.talend.sdk.component.api.configuration.ui.layout.GridLayout;
import org.talend.sdk.component.api.meta.Documentation;

@Getter
@DataSet("MagentoOutput")
@GridLayout({ @GridLayout.Row({ "magentoCmsConfigurationBase" }), @GridLayout.Row({ "selectionType" }) })
@Documentation("TODO fill the documentation for this configuration")
public class MagentoCmsOutputConfiguration {

    @Option
    @Documentation("magento CMS connection configuration")
    private MagentoCmsConfigurationBase magentoCmsConfigurationBase;

    // selection type, e.g. 'Products'
    @Option
    @Documentation("The type of information we want to get")
    private SelectionType selectionType;

}