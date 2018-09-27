package org.talend.components.onedrive.sources.delete;

import lombok.Data;
import org.talend.components.onedrive.common.OneDriveDataStore;
import org.talend.components.onedrive.helpers.ConfigurationHelper;
import org.talend.components.onedrive.service.configuration.OneDriveConfiguration;
import org.talend.sdk.component.api.configuration.Option;
import org.talend.sdk.component.api.configuration.type.DataSet;
import org.talend.sdk.component.api.configuration.ui.layout.GridLayout;
import org.talend.sdk.component.api.meta.Documentation;

@Data
@DataSet(ConfigurationHelper.DATA_SET_DELETE_ID)
@GridLayout({ @GridLayout.Row({ "dataStore" }) })
@GridLayout(names = GridLayout.FormType.ADVANCED, value = { @GridLayout.Row({ "dataStore" }) })
@Documentation("'Delete component' configuration")
public class OneDriveDeleteConfiguration extends OneDriveConfiguration {

    @Option
    @Documentation("Connection to server")
    private OneDriveDataStore dataStore;

}