package org.talend.components.onedrive.sources.get;

import lombok.Data;
import org.talend.components.onedrive.common.OneDriveDataStore;
import org.talend.components.onedrive.helpers.ConfigurationHelper;
import org.talend.components.onedrive.service.configuration.OneDriveConfiguration;
import org.talend.sdk.component.api.configuration.Option;
import org.talend.sdk.component.api.configuration.condition.ActiveIf;
import org.talend.sdk.component.api.configuration.type.DataSet;
import org.talend.sdk.component.api.configuration.ui.layout.GridLayout;
import org.talend.sdk.component.api.meta.Documentation;

@Data
@DataSet(ConfigurationHelper.DATA_SET_GET_ID)
@GridLayout({ @GridLayout.Row({ "dataStore" }), @GridLayout.Row({ "storeFilesLocally" }), @GridLayout.Row({ "storeDirectory" }) })
@GridLayout(names = GridLayout.FormType.ADVANCED, value = { @GridLayout.Row({ "dataStore" }) })
@Documentation("'Get component' configuration")
public class OneDriveGetConfiguration extends OneDriveConfiguration {

    @Option
    @Documentation("Connection to server")
    private OneDriveDataStore dataStore;

    @Option
    @Documentation("store files to local file system")
    private boolean storeFilesLocally;

    @Option
    @ActiveIf(target = "storeFilesLocally", value = { "true" })
    @Documentation("The directory where files will be stored and folder structure will be created")
    private String storeDirectory;

}