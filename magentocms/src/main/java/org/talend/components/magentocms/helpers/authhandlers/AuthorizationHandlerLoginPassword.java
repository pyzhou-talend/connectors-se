package org.talend.components.magentocms.helpers.authhandlers;

import lombok.AllArgsConstructor;
import lombok.Getter;
import oauth.signpost.exception.OAuthCommunicationException;
import oauth.signpost.exception.OAuthExpectationFailedException;
import oauth.signpost.exception.OAuthMessageSignerException;
import oauth.signpost.http.HttpRequest;
import org.apache.http.HttpEntity;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;
import org.talend.components.magentocms.common.AuthenticationLoginPasswordSettings;
import org.talend.components.magentocms.common.MagentoCmsConfigurationBase;
import org.talend.components.magentocms.common.UnknownAuthenticationTypeException;
import org.talend.components.magentocms.service.http.BadCredentialsException;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class AuthorizationHandlerLoginPassword implements AuthorizationHandler {

    private static Map<AuthenticationLoginPasswordSettings, String> cachedTokens = new ConcurrentHashMap<>();

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
            throws IOException, OAuthCommunicationException, OAuthExpectationFailedException, OAuthMessageSignerException,
            UnknownAuthenticationTypeException, BadCredentialsException {
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
        CloseableHttpClient httpclient = HttpClients.createDefault();
        try {
            accessToken = getTokenForUser(httpclient, magentoCmsConfigurationBase, UserType.USER_TYPE_CUSTOMER);
            if (accessToken == null) {
                accessToken = getTokenForUser(httpclient, magentoCmsConfigurationBase, UserType.USER_TYPE_ADMIN);
            }
        } finally {
            httpclient.close();
        }
        return accessToken;
    }

    private String getTokenForUser(CloseableHttpClient httpClient, MagentoCmsConfigurationBase magentoCmsConfigurationBase,
            UserType userType) throws UnknownAuthenticationTypeException, IOException {
        String accessToken = null;
        AuthenticationLoginPasswordSettings authSettings = (AuthenticationLoginPasswordSettings) magentoCmsConfigurationBase
                .getAuthSettings();
        String login = authSettings.getAuthenticationLogin();
        String password = authSettings.getAuthenticationPassword();

        String magentoUrl = magentoCmsConfigurationBase.getMagentoWebServerUrl() + "/rest/"
                + magentoCmsConfigurationBase.getMagentoRestVersion() + "/integration/" + userType.getName() + "/token";
        HttpPost httpPost = new HttpPost(magentoUrl);
        httpPost.setEntity(new StringEntity("{\"username\":\"" + login + "\",\"password\":\"" + password + "\"}",
                ContentType.APPLICATION_JSON));

        CloseableHttpResponse response = httpClient.execute(httpPost);
        try {
            int status = response.getStatusLine().getStatusCode();
            if (status == 200) {
                HttpEntity entity = response.getEntity();
                accessToken = EntityUtils.toString(entity).replaceAll("\"", "");
                EntityUtils.consume(entity);
            }
        } finally {
            response.close();
        }

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
