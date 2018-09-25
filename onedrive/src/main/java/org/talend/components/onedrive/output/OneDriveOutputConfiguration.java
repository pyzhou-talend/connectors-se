package org.talend.components.onedrive.output;

import lombok.Data;
import org.talend.components.onedrive.common.OneDriveDataStore;
import org.talend.components.onedrive.helpers.ConfigurationHelper;
import org.talend.sdk.component.api.configuration.Option;
import org.talend.sdk.component.api.configuration.constraint.Max;
import org.talend.sdk.component.api.configuration.constraint.Min;
import org.talend.sdk.component.api.configuration.type.DataSet;
import org.talend.sdk.component.api.configuration.ui.layout.GridLayout;
import org.talend.sdk.component.api.meta.Documentation;

@Data
@DataSet(ConfigurationHelper.DATA_SET_PUT_ID)
@GridLayout({ @GridLayout.Row({ "dataStore" }) })
@GridLayout(names = GridLayout.FormType.ADVANCED, value = { @GridLayout.Row({ "dataStore" }),
        @GridLayout.Row({ "parallelThreadsCount" }) })
@Documentation("Output component configuration")
public class OneDriveOutputConfiguration {

    @Option
    @Documentation("Connection to server")
    private OneDriveDataStore dataStore;

    @Option
    @Min(1)
    @Max(10)
    @Documentation("Count of threads that will be used for saving data into One Drive")
    private int parallelThreadsCount = 1;

    public String getMagentoUrl() {
        String res = "index.php/rest/" + "/";
        return res;
    }
}