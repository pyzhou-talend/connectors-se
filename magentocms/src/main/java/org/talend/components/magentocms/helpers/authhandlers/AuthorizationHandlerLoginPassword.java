package org.talend.components.magentocms.helpers.authhandlers;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.talend.components.magentocms.common.AuthenticationLoginPasswordSettings;
import org.talend.components.magentocms.common.MagentoCmsConfigurationBase;
import org.talend.components.magentocms.common.UnknownAuthenticationTypeException;
import org.talend.components.magentocms.service.http.BadCredentialsException;
import org.talend.components.magentocms.service.http.MagentoHttpClientService;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

@Slf4j
@RequiredArgsConstructor
public class AuthorizationHandlerLoginPassword implements AuthorizationHandler {

    private static Map<AuthenticationLoginPasswordSettings, String> cachedTokens = new ConcurrentHashMap<>();

    private final MagentoHttpClientService magentoHttpClientService;

    public static void clearTokenCache(AuthenticationLoginPasswordSettings authenticationLoginPasswordSettings) {
        cachedTokens.remove(authenticationLoginPasswordSettings);
    }

    @Override
    public String getAuthorization(MagentoCmsConfigurationBase magentoCmsConfigurationBase)
            throws UnknownAuthenticationTypeException, BadCredentialsException {
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

    private String getToken(MagentoCmsConfigurationBase magentoCmsConfigurationBase) throws UnknownAuthenticationTypeException {
        String accessToken;
        try {
            accessToken = getTokenForUser(magentoCmsConfigurationBase, UserType.USER_TYPE_CUSTOMER);
            if (accessToken == null) {
                accessToken = getTokenForUser(magentoCmsConfigurationBase, UserType.USER_TYPE_ADMIN);
            }
        } finally {
        }
        return accessToken;
    }

    private String getTokenForUser(MagentoCmsConfigurationBase magentoCmsConfigurationBase, UserType userType)
            throws UnknownAuthenticationTypeException {
        AuthenticationLoginPasswordSettings authSettings = (AuthenticationLoginPasswordSettings) magentoCmsConfigurationBase
                .getAuthSettings();
        String login = authSettings.getAuthenticationLogin();
        String password = authSettings.getAuthenticationPassword();

        String magentoUrl = "index.php/rest/" + magentoCmsConfigurationBase.getMagentoRestVersion() + "/integration/"
                + userType.getName() + "/token";
        String accessToken = magentoHttpClientService.getToken(magentoUrl, login, password);
        if (accessToken != null && accessToken.isEmpty()) {
            accessToken = null;
        }
        return accessToken;
    }

    @Getter
    @AllArgsConstructor
    enum UserType {
        USER_TYPE_ADMIN("admin"),
        USER_TYPE_CUSTOMER("customer");

        private String name;
    }

}
