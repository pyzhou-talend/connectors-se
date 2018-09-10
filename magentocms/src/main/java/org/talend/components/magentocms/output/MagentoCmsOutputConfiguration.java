package org.talend.components.magentocms.output;

import lombok.Data;
import org.talend.components.magentocms.common.MagentoCmsConfigurationBase;
import org.talend.components.magentocms.input.SelectionType;
import org.talend.sdk.component.api.configuration.Option;
import org.talend.sdk.component.api.configuration.constraint.Max;
import org.talend.sdk.component.api.configuration.constraint.Min;
import org.talend.sdk.component.api.configuration.type.DataSet;
import org.talend.sdk.component.api.configuration.ui.layout.GridLayout;
import org.talend.sdk.component.api.meta.Documentation;

// @Getter
// @NoArgsConstructor
// @AllArgsConstructor
@Data
@DataSet("MagentoOutput")
@GridLayout({ @GridLayout.Row({ "magentoCmsConfigurationBase" }), @GridLayout.Row({ "selectionType" }) })
@GridLayout(names = GridLayout.FormType.ADVANCED, value = { @GridLayout.Row({ "magentoCmsConfigurationBase" }),
        @GridLayout.Row({ "parallelThreadsCount" }) })
@Documentation("Output component configuration")
public class MagentoCmsOutputConfiguration {

    @Option
    @Documentation("Connection to Magento CMS")
    private MagentoCmsConfigurationBase magentoCmsConfigurationBase;

    @Option
    @Documentation("The type of information we want to put, e.g. 'Products'")
    private SelectionType selectionType;

    @Option
    @Min(1)
    @Max(10)
    @Documentation("Count of threads that will be used for saving data into Magento CMS")
    private int parallelThreadsCount = 1;

    public String getMagentoUrl() {
        String res = "index.php/rest/" + magentoCmsConfigurationBase.getMagentoRestVersion() + "/"
                + selectionType.name().toLowerCase();
        return res;
    }
}