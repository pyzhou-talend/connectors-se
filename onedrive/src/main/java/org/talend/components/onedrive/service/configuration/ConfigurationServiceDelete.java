package org.talend.components.onedrive.service.configuration;

import lombok.Getter;
import lombok.Setter;
import org.talend.components.onedrive.sources.delete.OneDriveDeleteConfiguration;
import org.talend.sdk.component.api.service.Service;

@Service
@Getter
@Setter
public class ConfigurationServiceDelete {

    private OneDriveDeleteConfiguration configuration;

}
