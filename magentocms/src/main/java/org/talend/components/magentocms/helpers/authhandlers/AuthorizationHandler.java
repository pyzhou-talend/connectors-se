package org.talend.components.magentocms.helpers.authhandlers;

import oauth.signpost.exception.OAuthCommunicationException;
import oauth.signpost.exception.OAuthExpectationFailedException;
import oauth.signpost.exception.OAuthMessageSignerException;
import oauth.signpost.http.HttpRequest;
import org.talend.components.magentocms.common.AuthenticationSettings;
import org.talend.components.magentocms.common.RequestType;

import java.net.MalformedURLException;
import java.util.Map;

public interface AuthorizationHandler {

    String getAuthorization(AuthenticationSettings authenticationSettings, String magentoUrl,
            Map<String, String> requestParameters, RequestType requestType) throws MalformedURLException,
            OAuthCommunicationException, OAuthExpectationFailedException, OAuthMessageSignerException;

    void setAuthorization(HttpRequest httpRequest, AuthenticationSettings authenticationSettings) throws MalformedURLException,
            OAuthCommunicationException, OAuthExpectationFailedException, OAuthMessageSignerException;
}
