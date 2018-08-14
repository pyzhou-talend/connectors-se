package org.talend.components.magentocms.helpers;

import oauth.signpost.exception.OAuthCommunicationException;
import oauth.signpost.exception.OAuthExpectationFailedException;
import oauth.signpost.exception.OAuthMessageSignerException;
import oauth.signpost.http.HttpRequest;
import org.talend.components.magentocms.common.AuthenticationType;
import org.talend.components.magentocms.common.MagentoCmsConfigurationBase;
import org.talend.components.magentocms.common.UnknownAuthenticationTypeException;
import org.talend.components.magentocms.helpers.authhandlers.AuthorizationHandler;
import org.talend.components.magentocms.helpers.authhandlers.AuthorizationHandlerAuthenticationToken;
import org.talend.components.magentocms.helpers.authhandlers.AuthorizationHandlerLoginPassword;
import org.talend.components.magentocms.helpers.authhandlers.AuthorizationHandlerOAuth1;

import java.io.IOException;

public class AuthorizationHelper {

    // public static String getAuthorization(AuthenticationType authenticationType, AuthenticationSettings authenticationSettings,
    // String magentoUrl, Map<String, String> requestParameters, RequestType requestType)
    // throws UnknownAuthenticationTypeException, MalformedURLException, OAuthExpectationFailedException,
    // OAuthCommunicationException, OAuthMessageSignerException {
    // AuthorizationHandler authenticationHandler = getAuthHandler(authenticationType);
    // return authenticationHandler.getAuthorization(authenticationSettings, magentoUrl, requestParameters, requestType);
    // }

    public static void setAuthorization(HttpRequest httpRequest, MagentoCmsConfigurationBase magentoCmsConfigurationBase)
            throws UnknownAuthenticationTypeException, IOException, OAuthExpectationFailedException, OAuthCommunicationException,
            OAuthMessageSignerException {
        AuthorizationHandler authenticationHandler = getAuthHandler(magentoCmsConfigurationBase.getAuthenticationType());
        authenticationHandler.setAuthorization(httpRequest, magentoCmsConfigurationBase);
    }

    private static AuthorizationHandler getAuthHandler(AuthenticationType authenticationType)
            throws UnknownAuthenticationTypeException {
        if (authenticationType == AuthenticationType.OAUTH_1) {
            return new AuthorizationHandlerOAuth1();
        } else if (authenticationType == AuthenticationType.AUTHENTICATION_TOKEN) {
            return new AuthorizationHandlerAuthenticationToken();
        } else if (authenticationType == AuthenticationType.LOGIN_PASSWORD) {
            return new AuthorizationHandlerLoginPassword();
        }
        throw new UnknownAuthenticationTypeException();
    }
}
