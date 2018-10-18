package org.talend.components.onedrive.helpers.authhandlers;

import lombok.extern.slf4j.Slf4j;
import org.talend.components.onedrive.common.AuthenticationLoginPasswordConfiguration;
import org.talend.components.onedrive.common.OneDriveDataStore;
import org.talend.components.onedrive.common.UnknownAuthenticationTypeException;
import org.talend.components.onedrive.service.http.BadCredentialsException;
import org.talend.components.onedrive.service.http.OneDriveAuthHttpClientService;
import org.talend.sdk.component.api.service.Service;

import java.io.UnsupportedEncodingException;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

@Slf4j
@Service
public class AuthorizationHandlerLoginPassword implements AuthorizationHandler {

    private static Map<AuthenticationLoginPasswordConfiguration, String> cachedTokens = new ConcurrentHashMap<>();

    @Service
    private OneDriveAuthHttpClientService oneDriveAuthHttpClientService = null;

    public void clearTokenCache(AuthenticationLoginPasswordConfiguration authenticationLoginPasswordConfiguration) {
        cachedTokens.remove(authenticationLoginPasswordConfiguration);
    }

    @Override
    public String getAuthorization(OneDriveDataStore magentoCmsConfigurationBase)
            throws UnknownAuthenticationTypeException, BadCredentialsException, UnsupportedEncodingException {
        AuthenticationLoginPasswordConfiguration authSettings = (AuthenticationLoginPasswordConfiguration) magentoCmsConfigurationBase
                .getAuthSettings();

        String accessToken = cachedTokens.get(authSettings);
        if (accessToken == null) {
            synchronized (this) {
                accessToken = cachedTokens.get(authSettings);
                if (accessToken == null) {
                    accessToken = getToken(magentoCmsConfigurationBase);
                }
            }
        }

        if (accessToken == null) {
            throw new BadCredentialsException("Get user's token exception (token is not set)");
        }

        cachedTokens.put(authSettings, accessToken);
        return "Bearer " + accessToken;
    }

    private String getToken(OneDriveDataStore magentoCmsConfigurationBase)
            throws UnknownAuthenticationTypeException, UnsupportedEncodingException {
        AuthenticationLoginPasswordConfiguration authSettings = (AuthenticationLoginPasswordConfiguration) magentoCmsConfigurationBase
                .getAuthSettings();
        String login = authSettings.getAuthenticationLogin();
        String password = authSettings.getAuthenticationPassword();
        String tenantId = magentoCmsConfigurationBase.getTenantId();
        String applicationId = magentoCmsConfigurationBase.getApplicationId();

        String accessToken = oneDriveAuthHttpClientService.getToken(tenantId, applicationId, login, password);
        if (accessToken != null && accessToken.isEmpty()) {
            accessToken = null;
        }

        return accessToken;
    }
}
