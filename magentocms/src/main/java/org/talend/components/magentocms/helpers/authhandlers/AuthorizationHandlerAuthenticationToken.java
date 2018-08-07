package org.talend.components.magentocms.helpers.authhandlers;

import org.talend.components.magentocms.common.AuthenticationOauth1Settings;
import org.talend.components.magentocms.common.AuthenticationSettings;
import org.talend.components.magentocms.common.RequestType;

import java.util.Map;

public class AuthorizationHandlerAuthenticationToken implements AuthorizationHandler {

    public String getAuthorization(AuthenticationSettings authenticationSettings, String magentoUrl,
            Map<String, String> requestParameters, RequestType requestType) {
        AuthenticationOauth1Settings authSettings = (AuthenticationOauth1Settings) authenticationSettings;
        // String consumerKey = authSettings.getAuthenticationOauth1ConsumerKey();
        // String consumerSecret = authSettings.getAuthenticationOauth1ConsumerSecret();
        String accessToken = authSettings.getAuthenticationOauth1AccessToken();
        // String accessTokenSecret = authSettings.getAuthenticationOauth1AccessTokenSecret();

        return "Bearer " + accessToken;
    }
}
