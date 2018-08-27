package org.talend.components.magentocms.helpers.authhandlers;

import oauth.signpost.exception.OAuthCommunicationException;
import oauth.signpost.exception.OAuthExpectationFailedException;
import oauth.signpost.exception.OAuthMessageSignerException;
import oauth.signpost.http.HttpRequest;
import org.talend.components.magentocms.common.MagentoCmsConfigurationBase;
import org.talend.components.magentocms.common.UnknownAuthenticationTypeException;
import org.talend.components.magentocms.service.http.BadCredentialsException;

import java.io.IOException;

public interface AuthorizationHandler {

    // String getAuthorization(AuthenticationSettings authenticationSettings, String magentoUrl,
    // Map<String, String> requestParameters, RequestType requestType) throws MalformedURLException,
    // OAuthCommunicationException, OAuthExpectationFailedException, OAuthMessageSignerException;

    void setAuthorization(HttpRequest httpRequest, MagentoCmsConfigurationBase magentoCmsConfigurationBase)
            throws IOException, OAuthCommunicationException, OAuthExpectationFailedException, OAuthMessageSignerException,
            UnknownAuthenticationTypeException, BadCredentialsException;
}
