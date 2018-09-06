package org.talend.components.magentocms.helpers;

import oauth.signpost.exception.OAuthCommunicationException;
import oauth.signpost.exception.OAuthExpectationFailedException;
import oauth.signpost.exception.OAuthMessageSignerException;
import org.talend.components.magentocms.common.AuthenticationType;
import org.talend.components.magentocms.common.MagentoCmsConfigurationBase;
import org.talend.components.magentocms.common.UnknownAuthenticationTypeException;
import org.talend.components.magentocms.helpers.authhandlers.AuthorizationHandler;
import org.talend.components.magentocms.helpers.authhandlers.AuthorizationHandlerAuthenticationToken;
import org.talend.components.magentocms.helpers.authhandlers.AuthorizationHandlerLoginPassword;
import org.talend.components.magentocms.service.http.BadCredentialsException;

import java.io.IOException;

public class AuthorizationHelper {

    public static String getAuthorization(MagentoCmsConfigurationBase magentoCmsConfigurationBase)
            throws UnknownAuthenticationTypeException, IOException, OAuthExpectationFailedException, OAuthCommunicationException,
            OAuthMessageSignerException, BadCredentialsException {
        AuthorizationHandler authenticationHandler = getAuthHandler(magentoCmsConfigurationBase.getAuthenticationType());
        return authenticationHandler.getAuthorization(magentoCmsConfigurationBase);
    }

    private static AuthorizationHandler getAuthHandler(AuthenticationType authenticationType)
            throws UnknownAuthenticationTypeException {
        if (authenticationType == AuthenticationType.OAUTH_1) {
            throw new UnsupportedOperationException("Incorrect usage of OAuth1 authentication handler");
        } else if (authenticationType == AuthenticationType.AUTHENTICATION_TOKEN) {
            return new AuthorizationHandlerAuthenticationToken();
        } else if (authenticationType == AuthenticationType.LOGIN_PASSWORD) {
            return new AuthorizationHandlerLoginPassword();
        }
        throw new UnknownAuthenticationTypeException();
    }
}
