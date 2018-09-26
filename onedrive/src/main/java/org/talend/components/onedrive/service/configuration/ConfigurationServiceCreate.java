package org.talend.components.onedrive.service.configuration;

import lombok.Getter;
import lombok.Setter;
import org.talend.components.onedrive.sources.create.OneDriveCreateConfiguration;
import org.talend.sdk.component.api.service.Service;

@Service
@Getter
@Setter
public class ConfigurationServiceCreate {

    private OneDriveCreateConfiguration configuration;

}
