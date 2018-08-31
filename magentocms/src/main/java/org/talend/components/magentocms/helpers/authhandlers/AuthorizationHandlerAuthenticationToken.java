package org.talend.components.magentocms.helpers.authhandlers;

import oauth.signpost.http.HttpRequest;
import org.talend.components.magentocms.common.AuthenticationTokenSettings;
import org.talend.components.magentocms.common.MagentoCmsConfigurationBase;
import org.talend.components.magentocms.common.UnknownAuthenticationTypeException;

public class AuthorizationHandlerAuthenticationToken implements AuthorizationHandler {

    @Override
    public void setAuthorization(HttpRequest httpRequest, MagentoCmsConfigurationBase magentoCmsConfigurationBase)
            throws UnknownAuthenticationTypeException {
        AuthenticationTokenSettings authSettings = (AuthenticationTokenSettings) magentoCmsConfigurationBase.getAuthSettings();
        String accessToken = authSettings.getAuthenticationAccessToken();
        httpRequest.setHeader("Authorization", "Bearer " + accessToken);
    }
}
