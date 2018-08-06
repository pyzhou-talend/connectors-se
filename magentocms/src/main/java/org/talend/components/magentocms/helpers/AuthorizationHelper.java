package org.talend.components.magentocms.helpers;

import oauth.signpost.exception.OAuthCommunicationException;
import oauth.signpost.exception.OAuthExpectationFailedException;
import oauth.signpost.exception.OAuthMessageSignerException;
import org.talend.components.magentocms.common.AuthenticationSettings;
import org.talend.components.magentocms.common.AuthenticationType;
import org.talend.components.magentocms.common.RequestType;
import org.talend.components.magentocms.common.UnknownAuthenticationTypeException;
import org.talend.components.magentocms.helpers.authhandlers.AuthorizationHandler;
import org.talend.components.magentocms.helpers.authhandlers.AuthorizationHandlerOAuth1;

import java.net.MalformedURLException;

public class AuthorizationHelper {

    public static String getAuthorization(AuthenticationType authenticationType, AuthenticationSettings authenticationSettings,
            String magentoUrl, RequestType requestType) throws UnknownAuthenticationTypeException, MalformedURLException,
            OAuthExpectationFailedException, OAuthCommunicationException, OAuthMessageSignerException {
        AuthorizationHandler authenticationHandler = getAuthHandler(authenticationType);
        return authenticationHandler.getAuthorization(authenticationSettings, magentoUrl, requestType);
    }

    private static AuthorizationHandler getAuthHandler(AuthenticationType authenticationType)
            throws UnknownAuthenticationTypeException {
        if (authenticationType == AuthenticationType.OAUTH_1) {
            return new AuthorizationHandlerOAuth1();
        }
        throw new UnknownAuthenticationTypeException();
    }
}
