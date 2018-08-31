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
        @GridLayout.Row({ "selectionFilter" }) })
@GridLayout(names = GridLayout.FormType.ADVANCED, value = { @GridLayout.Row({ "magentoCmsConfigurationBase" }),
        @GridLayout.Row({ "fields" }), @GridLayout.Row({ "selectionFilter" })
        // , @GridLayout.Row({ "selectedFields" })
})
@Documentation("Input component configuration")
public class MagentoCmsInputMapperConfiguration {

    @Option
    @Documentation("Connection to Magento CMS")
    private MagentoCmsConfigurationBase magentoCmsConfigurationBase;

    @Option
    @Documentation("The type of information we want to get, e.g. 'Products'")
    private SelectionType selectionType;

    @Option
    @Documentation("Data filter")
    private ConfigurationFilter selectionFilter = new ConfigurationFilter();

    @Option
    @Structure(discoverSchema = "guessTableSchema", type = OUT)
    @Documentation("The schema of the component. Use 'Discover schema' button to fil it with sample data. "
            + "Schema is discovering by getting the frist record from particular data table, "
            + "e.g. first product in case of 'Product' selection type")
    private List<String> fields = new ArrayList<>();

    public String getMagentoUrl() {
        String res = magentoCmsConfigurationBase.getMagentoWebServerUrl() + "/index.php/rest/"
                + magentoCmsConfigurationBase.getMagentoRestVersion() + "/" + selectionType.name().toLowerCase();
        return res;
    }
}