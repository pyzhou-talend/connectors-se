package org.talend.components.onedrive.common;

import lombok.extern.slf4j.Slf4j;
import org.talend.components.onedrive.service.http.BadCredentialsException;
import org.talend.components.onedrive.service.http.OneDriveHttpClientService;
import org.talend.sdk.component.api.service.Service;

import java.io.IOException;
import java.io.Serializable;

@Slf4j
@Service
public class HealthChecker implements Serializable {

    @Service
    private OneDriveHttpClientService oneDriveHttpClientService = null;

    public boolean checkHealth(OneDriveDataStore dataStore)
            throws UnknownAuthenticationTypeException, IOException, BadCredentialsException {
        oneDriveHttpClientService.getRoot(dataStore);
        return true;
    }
}