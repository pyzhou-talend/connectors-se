package org.talend.components.onedrive.input;

import lombok.Data;
import org.talend.components.onedrive.common.OneDriveDataStore;
import org.talend.components.onedrive.helpers.ConfigurationHelper;
import org.talend.sdk.component.api.configuration.Option;
import org.talend.sdk.component.api.configuration.type.DataSet;
import org.talend.sdk.component.api.configuration.ui.layout.GridLayout;
import org.talend.sdk.component.api.meta.Documentation;

@Data
@DataSet(ConfigurationHelper.DATA_SET_GET_ID)
@GridLayout({ @GridLayout.Row({ "dataStore" }) })
@GridLayout(names = GridLayout.FormType.ADVANCED, value = { @GridLayout.Row({ "dataStore" }), })
@Documentation("Input component configuration")
public class OneDriveInputConfiguration {

    @Option
    @Documentation("Connection to Magento CMS")
    private OneDriveDataStore dataStore;

    // @Option
    // @Structure(discoverSchema = ConfigurationHelper., type = Structure.Type.OUT)
    // @Documentation("The schema of the component. Use 'Discover schema' button to fil it with sample data. "
    // + "Schema is discovering by getting the frist record from particular data table, "
    // + "e.g. first product in case of 'Product' selection type")
    // private List<String> fields = new ArrayList<>();

    public String getMagentoUrl() {
        String res = "index.php/rest/" + "/";
        return res;
    }
}