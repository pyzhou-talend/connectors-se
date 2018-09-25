package org.talend.components.onedrive.helpers.authhandlers;

import org.talend.components.onedrive.common.OneDriveDataStore;
import org.talend.components.onedrive.common.UnknownAuthenticationTypeException;
import org.talend.components.onedrive.service.http.BadCredentialsException;

import java.io.IOException;

public interface AuthorizationHandler {

    String getAuthorization(OneDriveDataStore magentoCmsConfigurationBase)
            throws IOException, UnknownAuthenticationTypeException, BadCredentialsException;
}
