package org.talend.components.onedrive.sources.create;

import lombok.Data;
import org.talend.components.onedrive.common.OneDriveDataStore;
import org.talend.components.onedrive.helpers.ConfigurationHelper;
import org.talend.components.onedrive.sources.list.OneDriveObjectType;
import org.talend.sdk.component.api.configuration.Option;
import org.talend.sdk.component.api.configuration.type.DataSet;
import org.talend.sdk.component.api.configuration.ui.layout.GridLayout;
import org.talend.sdk.component.api.meta.Documentation;

@Data
@DataSet(ConfigurationHelper.DATA_SET_CREATE_ID)
@GridLayout({ @GridLayout.Row({ "dataStore" }), @GridLayout.Row({ "objectType" }), @GridLayout.Row({ "objectPath" }) })
@GridLayout(names = GridLayout.FormType.ADVANCED, value = { @GridLayout.Row({ "dataStore" }) })
@Documentation("'Create component' configuration")
public class OneDriveCreateConfiguration {

    @Option
    @Documentation("Connection to server")
    private OneDriveDataStore dataStore;

    @Option
    @Documentation("The name of file or folder to create. Use '/' as a directory delimiter")
    private String objectPath = "";

    @Option
    @Documentation("Full path to OneDrive directory or file")
    private OneDriveObjectType objectType = OneDriveObjectType.DIRECTORY;

}