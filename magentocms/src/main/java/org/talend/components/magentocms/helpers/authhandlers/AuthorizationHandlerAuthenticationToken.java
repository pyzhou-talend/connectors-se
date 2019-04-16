package org.talend.components.magentocms.helpers.authhandlers;

import org.talend.components.magentocms.common.AuthenticationTokenConfiguration;
import org.talend.components.magentocms.common.MagentoDataStore;
import org.talend.components.magentocms.common.UnknownAuthenticationTypeException;
import org.talend.sdk.component.api.service.Service;

@Service
public class AuthorizationHandlerAuthenticationToken implements AuthorizationHandler {

    @Override
    public String getAuthorization(MagentoDataStore magentoDataStore) throws UnknownAuthenticationTypeException {
        AuthenticationTokenConfiguration authSettings = (AuthenticationTokenConfiguration) magentoDataStore.getAuthSettings();
        String accessToken = authSettings.getAuthenticationAccessToken();
        return "Bearer " + accessToken;
    }
}
