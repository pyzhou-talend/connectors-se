package org.talend.components.magentocms.helpers.authhandlers;

import lombok.RequiredArgsConstructor;
import oauth.signpost.OAuthConsumer;
import oauth.signpost.commonshttp.CommonsHttpOAuthConsumer;
import oauth.signpost.exception.OAuthCommunicationException;
import oauth.signpost.exception.OAuthExpectationFailedException;
import oauth.signpost.exception.OAuthMessageSignerException;
import oauth.signpost.http.HttpRequest;
import oauth.signpost.signature.AuthorizationHeaderSigningStrategy;
import org.talend.components.magentocms.common.AuthenticationOauth1Settings;
import org.talend.components.magentocms.common.MagentoCmsConfigurationBase;
import org.talend.components.magentocms.common.UnknownAuthenticationTypeException;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.Map;

@RequiredArgsConstructor
public class AuthorizationHandlerOAuth1 implements AuthorizationHandler {

    private final String httpMethod;

    private final String httpPath;

    private final Map<String, String> queryParameters;

    @Override
    public void setAuthorization(HttpRequest httpRequest, MagentoCmsConfigurationBase magentoCmsConfigurationBase)
            throws OAuthCommunicationException, OAuthExpectationFailedException, OAuthMessageSignerException,
            UnknownAuthenticationTypeException {
        AuthenticationOauth1Settings authSettings = (AuthenticationOauth1Settings) magentoCmsConfigurationBase.getAuthSettings();
        String consumerKey = authSettings.getAuthenticationOauth1ConsumerKey();
        String consumerSecret = authSettings.getAuthenticationOauth1ConsumerSecret();
        String accessToken = authSettings.getAuthenticationOauth1AccessToken();
        String accessTokenSecret = authSettings.getAuthenticationOauth1AccessTokenSecret();

        OAuthConsumer oAuthConsumer = new CommonsHttpOAuthConsumer(consumerKey, consumerSecret);
        oAuthConsumer.setTokenWithSecret(accessToken, accessTokenSecret);
        oAuthConsumer.setSigningStrategy(new AuthorizationHeaderSigningStrategy());

        oAuthConsumer.sign(httpRequest);
    }

    @Override
    public String getAuthorization(MagentoCmsConfigurationBase magentoCmsConfigurationBase) throws IOException {
        throw new UnsupportedEncodingException("Getting OAuth1 authorization is not supported");
    }
}
