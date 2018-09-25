package org.talend.components.onedrive.service.configuration;

import lombok.Getter;
import lombok.Setter;
import org.talend.components.onedrive.input.OneDriveInputConfiguration;
import org.talend.components.onedrive.input.list.OneDriveListConfiguration;
import org.talend.sdk.component.api.service.Service;

@Service
@Getter
@Setter
public class ConfigurationServiceList {

    private OneDriveListConfiguration configuration;

}
