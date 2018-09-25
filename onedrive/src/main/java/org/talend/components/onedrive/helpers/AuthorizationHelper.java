package org.talend.components.onedrive.helpers;

import org.talend.components.onedrive.common.AuthenticationType;
import org.talend.components.onedrive.common.OneDriveDataStore;
import org.talend.components.onedrive.common.UnknownAuthenticationTypeException;
import org.talend.components.onedrive.helpers.authhandlers.AuthorizationHandler;
import org.talend.components.onedrive.helpers.authhandlers.AuthorizationHandlerLoginPassword;
import org.talend.components.onedrive.service.http.BadCredentialsException;
import org.talend.components.onedrive.service.http.OneDriveAuthHttpClientService;
import org.talend.sdk.component.api.service.Service;

import java.io.IOException;

@Service
public class AuthorizationHelper {

    @Service
    OneDriveAuthHttpClientService oneDriveAuthHttpClientService;

    public String getAuthorization(OneDriveDataStore magentoCmsConfigurationBase)
            throws UnknownAuthenticationTypeException, IOException, BadCredentialsException {
        AuthorizationHandler authenticationHandler = getAuthHandler(magentoCmsConfigurationBase.getAuthenticationType());
        return authenticationHandler.getAuthorization(magentoCmsConfigurationBase);
    }

    private AuthorizationHandler getAuthHandler(AuthenticationType authenticationType) throws UnknownAuthenticationTypeException {
        if (authenticationType == AuthenticationType.LOGIN_PASSWORD) {
            return new AuthorizationHandlerLoginPassword(oneDriveAuthHttpClientService);
        }
        throw new UnknownAuthenticationTypeException();
    }
}
