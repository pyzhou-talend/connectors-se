package org.talend.components.onedrive.service.configuration;

import lombok.Getter;
import lombok.Setter;
import org.talend.components.onedrive.output.OneDriveOutputConfiguration;
import org.talend.sdk.component.api.service.Service;

@Service
@Getter
@Setter
public class ConfigurationServiceOutput {

    private OneDriveOutputConfiguration configuration;

}
