package org.talend.components.onedrive.sources.list;

import lombok.Data;
import org.talend.components.onedrive.common.OneDriveDataStore;
import org.talend.components.onedrive.helpers.ConfigurationHelper;
import org.talend.sdk.component.api.configuration.Option;
import org.talend.sdk.component.api.configuration.type.DataSet;
import org.talend.sdk.component.api.configuration.ui.layout.GridLayout;
import org.talend.sdk.component.api.configuration.ui.widget.Structure;
import org.talend.sdk.component.api.meta.Documentation;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

@Data
@DataSet(ConfigurationHelper.DATA_SET_LIST_ID)
@GridLayout({ @GridLayout.Row({ "dataStore" }), @GridLayout.Row({ "fields" }), @GridLayout.Row({ "objectPath" }),
        @GridLayout.Row({ "recursively" }) })
@Documentation("Input component configuration")
public class OneDriveListConfiguration implements Serializable {

    @Option
    @Documentation("Connection to Onedrive")
    private OneDriveDataStore dataStore;

    @Option
    @Structure(discoverSchema = ConfigurationHelper.DISCOVER_SCHEMA_LIST_ID, type = Structure.Type.OUT)
    @Documentation("The schema of the component. Use 'Discover schema' button to fill it with sample data.")
    private List<String> fields = new ArrayList<>();

    @Option
    @Documentation("Full path to OneDrive directory or file")
    private String objectPath = "";

    // @Option
    // @Documentation("The type of object we want to list")
    // private OneDriveObjectType objectType = OneDriveObjectType.DIRECTORY;

    @Option
    @Documentation("List directory recursively")
    private boolean recursively;
}