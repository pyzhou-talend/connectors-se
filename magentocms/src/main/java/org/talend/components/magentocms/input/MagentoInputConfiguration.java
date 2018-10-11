package org.talend.components.magentocms.input;

import lombok.Data;
import org.talend.components.magentocms.common.MagentoDataStore;
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
@GridLayout({ @GridLayout.Row({ "magentoDataStore" }), @GridLayout.Row({ "selectionType" }),
        @GridLayout.Row({ "selectionFilter" }) })
@GridLayout(names = GridLayout.FormType.ADVANCED, value = { @GridLayout.Row({ "magentoDataStore" }),
        @GridLayout.Row({ "fields" }), @GridLayout.Row({ "selectionFilter" }) })
@Documentation("Input component configuration")
public class MagentoInputConfiguration {

    @Option
    @Documentation("Connection to Magento CMS")
    private MagentoDataStore magentoDataStore;

    @Option
    @Documentation("The type of information we want to get, e.g. 'Products'")
    private SelectionType selectionType;

    @Option
    @Documentation("Data filter")
    private ConfigurationFilter selectionFilter = new ConfigurationFilter();

    @Option
    @Structure(discoverSchema = "guessTableSchema", type = OUT)
    @Documentation("The schema of the component. Use 'Discover schema' button to fill it with sample data. "
            + "Schema is discovered by getting the first record from particular data table, "
            + "e.g. first product in case of 'Product' selection type")
    private List<String> fields = new ArrayList<>();

    public String getMagentoUrl() {
        String res = magentoDataStore.getMagentoBaseUrl() + "/" + selectionType.name().toLowerCase();
        return res;
    }
}