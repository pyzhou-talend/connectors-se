package org.talend.components.magentocms.output;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import org.talend.components.magentocms.common.MagentoCmsConfigurationBase;
import org.talend.components.magentocms.input.SelectionType;
import org.talend.sdk.component.api.configuration.Option;
import org.talend.sdk.component.api.configuration.type.DataSet;
import org.talend.sdk.component.api.configuration.ui.layout.GridLayout;
import org.talend.sdk.component.api.meta.Documentation;

@Getter
@NoArgsConstructor
@AllArgsConstructor
@DataSet("MagentoOutput")
@GridLayout({ @GridLayout.Row({ "magentoCmsConfigurationBase" }), @GridLayout.Row({ "selectionType" }) })
@GridLayout(names = GridLayout.FormType.ADVANCED, value = { @GridLayout.Row({ "magentoCmsConfigurationBase" }) })
@Documentation("TODO fill the documentation for this configuration")
public class MagentoCmsOutputConfiguration {

    @Option
    @Documentation("magento CMS connection configuration")
    private MagentoCmsConfigurationBase magentoCmsConfigurationBase;

    @Option
    @Documentation("The type of information we want to get, e.g. 'Products'")
    private SelectionType selectionType;

    public String getMagentoUrl() {
        String res = magentoCmsConfigurationBase.getMagentoWebServerUrl() + "/index.php/rest/"
                + magentoCmsConfigurationBase.getMagentoRestVersion() + "/" + selectionType.name().toLowerCase();
        return res;
    }
}