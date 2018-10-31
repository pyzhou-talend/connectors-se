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
    public String getAuthorization(OneDriveDataStore oneDriveDataStore)
            throws UnknownAuthenticationTypeException, BadCredentialsException, UnsupportedEncodingException {
        AuthenticationLoginPasswordConfiguration authSettings = (AuthenticationLoginPasswordConfiguration) oneDriveDataStore
                .getAuthSettings();

        String accessToken = getCachedToken(authSettings);
        if (accessToken == null) {
            synchronized (this) {
                accessToken = getCachedToken(authSettings);
                if (accessToken == null) {
                    accessToken = getToken(oneDriveDataStore);
                }
            }
        }

        cachedTokens.put(authSettings, accessToken);
        return "Bearer " + accessToken;
    }

    String getCachedToken(AuthenticationLoginPasswordConfiguration authSettings) {
        return cachedTokens.get(authSettings);
    }

    private String getToken(OneDriveDataStore oneDriveDataStore)
            throws UnknownAuthenticationTypeException, UnsupportedEncodingException, BadCredentialsException {
        AuthenticationLoginPasswordConfiguration authSettings = (AuthenticationLoginPasswordConfiguration) oneDriveDataStore
                .getAuthSettings();
        String login = authSettings.getAuthenticationLogin();
        String password = authSettings.getAuthenticationPassword();
        String tenantId = oneDriveDataStore.getTenantId();
        String applicationId = oneDriveDataStore.getApplicationId();

        return oneDriveAuthHttpClientService.getToken(tenantId, applicationId, login, password);
    }
}
