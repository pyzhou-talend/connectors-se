package org.talend.components.onedrive.sources.put;

import lombok.Data;
import org.talend.components.onedrive.common.OneDriveDataStore;
import org.talend.components.onedrive.helpers.ConfigurationHelper;
import org.talend.sdk.component.api.configuration.Option;
import org.talend.sdk.component.api.configuration.type.DataSet;
import org.talend.sdk.component.api.configuration.ui.layout.GridLayout;
import org.talend.sdk.component.api.meta.Documentation;

@Data
@DataSet(ConfigurationHelper.DATA_SET_PUT_ID)
@GridLayout({ @GridLayout.Row({ "dataStore" }), @GridLayout.Row({ "localSource" }) })
@GridLayout(names = GridLayout.FormType.ADVANCED, value = { @GridLayout.Row({ "dataStore" }) })
@Documentation("'Put component' configuration")
public class OneDrivePutConfiguration {

    @Option
    @Documentation("Connection to server")
    private OneDriveDataStore dataStore;

    @Option
    @Documentation("The name of file or folder to create. Use '/' as a directory delimiter")
    private boolean localSource;
}