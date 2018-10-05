package org.talend.components.onedrive.sources.put;

import lombok.Data;
import org.talend.components.onedrive.common.OneDriveDataStore;
import org.talend.components.onedrive.helpers.ConfigurationHelper;
import org.talend.sdk.component.api.configuration.Option;
import org.talend.sdk.component.api.configuration.type.DataSet;
import org.talend.sdk.component.api.configuration.ui.layout.GridLayout;
import org.talend.sdk.component.api.configuration.ui.widget.Structure;
import org.talend.sdk.component.api.meta.Documentation;

import java.util.ArrayList;
import java.util.List;

@Data
@DataSet(ConfigurationHelper.DATA_SET_PUT_ID)
@GridLayout({ @GridLayout.Row({ "dataStore" }), @GridLayout.Row({ "localSource" }) })
@GridLayout(names = GridLayout.FormType.ADVANCED, value = { @GridLayout.Row({ "fields" }) })
@Documentation("'Put component' configuration")
public class OneDrivePutConfiguration {

    @Option
    @Documentation("Connection to server")
    private OneDriveDataStore dataStore;

    @Option
    @Structure(discoverSchema = ConfigurationHelper.DISCOVER_SCHEMA_LIST_ID, type = Structure.Type.OUT)
    @Documentation("The schema of the component. Use 'Discover schema' button to fil it with sample data.")
    private List<String> fields = new ArrayList<>();

    @Option
    @Documentation("The name of file or folder to create. Use '/' as a directory delimiter")
    private boolean localSource;
}