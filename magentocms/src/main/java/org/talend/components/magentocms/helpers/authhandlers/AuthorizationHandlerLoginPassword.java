package org.talend.components.magentocms.helpers.authhandlers;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import oauth.signpost.http.HttpRequest;
import org.talend.components.magentocms.common.AuthenticationLoginPasswordSettings;
import org.talend.components.magentocms.common.MagentoCmsConfigurationBase;
import org.talend.components.magentocms.common.UnknownAuthenticationTypeException;
import org.talend.components.magentocms.service.http.BadCredentialsException;
import org.talend.components.magentocms.service.http.MagentoHttpClientService;

import java.io.IOException;
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
    public void setAuthorization(HttpRequest httpRequest, MagentoCmsConfigurationBase magentoCmsConfigurationBase)
            throws IOException, UnknownAuthenticationTypeException, BadCredentialsException {
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
        httpRequest.setHeader("Authorization", "Bearer " + accessToken);
    }

    @Override
    public String getAuthorization(MagentoCmsConfigurationBase magentoCmsConfigurationBase)
            throws IOException, UnknownAuthenticationTypeException, BadCredentialsException {
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

    private String getToken(MagentoCmsConfigurationBase magentoCmsConfigurationBase)
            throws IOException, UnknownAuthenticationTypeException {
        String accessToken;
        // CloseableHttpClient httpclient = HttpClients.createDefault();
        try {
            accessToken = getTokenForUser(magentoCmsConfigurationBase, UserType.USER_TYPE_CUSTOMER);
            if (accessToken == null) {
                accessToken = getTokenForUser(magentoCmsConfigurationBase, UserType.USER_TYPE_ADMIN);
            }
        } finally {
            // httpclient.close();
        }
        return accessToken;
    }

    private String getTokenForUser(MagentoCmsConfigurationBase magentoCmsConfigurationBase, UserType userType)
            throws UnknownAuthenticationTypeException {
        AuthenticationLoginPasswordSettings authSettings = (AuthenticationLoginPasswordSettings) magentoCmsConfigurationBase
                .getAuthSettings();
        String login = authSettings.getAuthenticationLogin();
        String password = authSettings.getAuthenticationPassword();

        String magentoUrl = "index.php/rest/"
                + magentoCmsConfigurationBase.getMagentoRestVersion() + "/integration/" + userType.getName() + "/token";
        // HttpPost httpPost = new HttpPost(magentoUrl);
        // httpPost.setEntity(new StringEntity("{\"username\":\"" + login + "\",\"password\":\"" + password + "\"}",
        // ContentType.APPLICATION_JSON));
        // String body = "{\"username\":\"" + login + "\",\"password\":\"" + password + "\"}";
        // CloseableHttpResponse response = httpClient.execute(httpPost);
        String accessToken = magentoHttpClientService.getToken(magentoUrl, login, password);

        // try {
        // int status = response.getStatusLine().getStatusCode();
        // if (status == 200) {
        // HttpEntity entity = response.getEntity();
        // accessToken = EntityUtils.toString(entity).replaceAll("\"", "");
        // EntityUtils.consume(entity);
        // }
        // } finally {
        // response.close();
        // }

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
