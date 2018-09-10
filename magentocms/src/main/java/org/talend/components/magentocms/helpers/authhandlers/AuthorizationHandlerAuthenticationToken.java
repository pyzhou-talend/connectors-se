package org.talend.components.magentocms.helpers.authhandlers;

import org.talend.components.magentocms.common.AuthenticationTokenSettings;
import org.talend.components.magentocms.common.MagentoCmsConfigurationBase;
import org.talend.components.magentocms.common.UnknownAuthenticationTypeException;

public class AuthorizationHandlerAuthenticationToken implements AuthorizationHandler {

    @Override
    public String getAuthorization(MagentoCmsConfigurationBase magentoCmsConfigurationBase)
            throws UnknownAuthenticationTypeException {
        AuthenticationTokenSettings authSettings = (AuthenticationTokenSettings) magentoCmsConfigurationBase.getAuthSettings();
        String accessToken = authSettings.getAuthenticationAccessToken();
        return "Bearer " + accessToken;
    }
}
