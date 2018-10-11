package org.talend.components.magentocms.helpers;

import org.talend.components.magentocms.common.AuthenticationType;
import org.talend.components.magentocms.common.MagentoDataStore;
import org.talend.components.magentocms.common.UnknownAuthenticationTypeException;
import org.talend.components.magentocms.helpers.authhandlers.AuthorizationHandler;
import org.talend.components.magentocms.helpers.authhandlers.AuthorizationHandlerAuthenticationToken;
import org.talend.components.magentocms.helpers.authhandlers.AuthorizationHandlerLoginPassword;
import org.talend.components.magentocms.service.http.BadCredentialsException;
import org.talend.components.magentocms.service.http.MagentoHttpClientService;
import org.talend.sdk.component.api.service.Service;

import java.io.IOException;

@Service
public class AuthorizationHelper {

    @Service
    MagentoHttpClientService magentoHttpClientService;

    public String getAuthorization(MagentoDataStore magentoDataStore)
            throws UnknownAuthenticationTypeException, IOException, BadCredentialsException {
        AuthorizationHandler authenticationHandler = getAuthHandler(magentoDataStore.getAuthenticationType());
        return authenticationHandler.getAuthorization(magentoDataStore);
    }

    private AuthorizationHandler getAuthHandler(AuthenticationType authenticationType) throws UnknownAuthenticationTypeException {
        if (authenticationType == AuthenticationType.OAUTH_1) {
            throw new UnsupportedOperationException("Incorrect usage of OAuth1 authentication handler");
        } else if (authenticationType == AuthenticationType.AUTHENTICATION_TOKEN) {
            return new AuthorizationHandlerAuthenticationToken();
        } else if (authenticationType == AuthenticationType.LOGIN_PASSWORD) {
            return new AuthorizationHandlerLoginPassword(magentoHttpClientService);
        }
        throw new UnknownAuthenticationTypeException();
    }
}
