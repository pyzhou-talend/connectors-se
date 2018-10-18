package org.talend.components.onedrive.helpers;

import org.talend.components.onedrive.common.AuthenticationType;
import org.talend.components.onedrive.common.OneDriveDataStore;
import org.talend.components.onedrive.common.UnknownAuthenticationTypeException;
import org.talend.components.onedrive.helpers.authhandlers.AuthorizationHandler;
import org.talend.components.onedrive.helpers.authhandlers.AuthorizationHandlerLoginPassword;
import org.talend.components.onedrive.service.http.BadCredentialsException;
import org.talend.sdk.component.api.service.Service;

import java.io.IOException;

@Service
public class AuthorizationHelper {

    @Service
    private AuthorizationHandlerLoginPassword authorizationHandlerLoginPassword = null;

    public String getAuthorization(OneDriveDataStore oneDriveDataStore)
            throws UnknownAuthenticationTypeException, IOException, BadCredentialsException {
        AuthorizationHandler authenticationHandler = getAuthHandler(oneDriveDataStore.getAuthenticationType());
        return authenticationHandler.getAuthorization(oneDriveDataStore);
    }

    private AuthorizationHandler getAuthHandler(AuthenticationType authenticationType) throws UnknownAuthenticationTypeException {
        if (authenticationType == AuthenticationType.LOGIN_PASSWORD) {
            return authorizationHandlerLoginPassword;
        }
        throw new UnknownAuthenticationTypeException();
    }
}
