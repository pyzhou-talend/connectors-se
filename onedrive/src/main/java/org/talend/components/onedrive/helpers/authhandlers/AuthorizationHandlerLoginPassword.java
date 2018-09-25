package org.talend.components.onedrive.helpers.authhandlers;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.talend.components.onedrive.common.AuthenticationLoginPasswordSettings;
import org.talend.components.onedrive.common.OneDriveDataStore;
import org.talend.components.onedrive.common.UnknownAuthenticationTypeException;
import org.talend.components.onedrive.service.http.BadCredentialsException;
import org.talend.components.onedrive.service.http.OneDriveAuthHttpClientService;

import java.io.UnsupportedEncodingException;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

@Slf4j
@RequiredArgsConstructor
public class AuthorizationHandlerLoginPassword implements AuthorizationHandler {

    private static Map<AuthenticationLoginPasswordSettings, String> cachedTokens = new ConcurrentHashMap<>();

    private final OneDriveAuthHttpClientService oneDriveAuthHttpClientService;

    public static void clearTokenCache(AuthenticationLoginPasswordSettings authenticationLoginPasswordSettings) {
        cachedTokens.remove(authenticationLoginPasswordSettings);
    }

    @Override
    public String getAuthorization(OneDriveDataStore magentoCmsConfigurationBase)
            throws UnknownAuthenticationTypeException, BadCredentialsException, UnsupportedEncodingException {
        AuthenticationLoginPasswordSettings authSettings = (AuthenticationLoginPasswordSettings) magentoCmsConfigurationBase
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
        AuthenticationLoginPasswordSettings authSettings = (AuthenticationLoginPasswordSettings) magentoCmsConfigurationBase
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
