package org.talend.components.magentocms.helpers.authhandlers;

import oauth.signpost.exception.OAuthCommunicationException;
import oauth.signpost.exception.OAuthExpectationFailedException;
import oauth.signpost.exception.OAuthMessageSignerException;
import oauth.signpost.http.HttpRequest;
import org.talend.components.magentocms.common.AuthenticationSettings;
import org.talend.components.magentocms.common.AuthenticationTokenSettings;
import org.talend.components.magentocms.common.RequestType;

import java.net.MalformedURLException;
import java.util.Map;

public class AuthorizationHandlerAuthenticationToken implements AuthorizationHandler {

    public String getAuthorization(AuthenticationSettings authenticationSettings, String magentoUrl,
            Map<String, String> requestParameters, RequestType requestType) {
        AuthenticationTokenSettings authSettings = (AuthenticationTokenSettings) authenticationSettings;
        String accessToken = authSettings.getAuthenticationAccessToken();

        return "Bearer " + accessToken;
    }

    @Override
    public void setAuthorization(HttpRequest httpRequest, AuthenticationSettings authenticationSettings)
            throws MalformedURLException, OAuthCommunicationException, OAuthExpectationFailedException,
            OAuthMessageSignerException {
        AuthenticationTokenSettings authSettings = (AuthenticationTokenSettings) authenticationSettings;
        String accessToken = authSettings.getAuthenticationAccessToken();
        httpRequest.setHeader("Authorization", "Bearer " + accessToken);
    }
}
