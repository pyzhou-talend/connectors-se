package org.talend.components.magentocms.input;

import lombok.Getter;
import org.talend.components.magentocms.common.MagentoCmsConfigurationBase;
import org.talend.sdk.component.api.configuration.Option;
import org.talend.sdk.component.api.configuration.ui.layout.GridLayout;
import org.talend.sdk.component.api.meta.Documentation;

// @DataSet
@GridLayout({ @GridLayout.Row({ "magentoCmsConfigurationBase" }), @GridLayout.Row({ "selectionType" }),
        @GridLayout.Row({ "selectionId" }) })
@Documentation("TODO fill the documentation for this configuration")
public class MagentoCmsInputMapperConfiguration {

    @Option
    @Documentation("magento CMS connection configuration")
    @Getter
    private MagentoCmsConfigurationBase magentoCmsConfigurationBase;

    // selection type, e.g. 'Products'
    @Option
    @Documentation("The type of information we want to get")
    @Getter
    private SelectionType selectionType;

    // selection id, e.g. sku for 'products' selection type
    @Option
    @Documentation("The Id of entity we want to get")
    @Getter
    private String selectionId;

}