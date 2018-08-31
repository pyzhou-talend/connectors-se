package org.talend.components.magentocms.helpers.authhandlers;

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

public class AuthorizationHandlerOAuth1 implements AuthorizationHandler {

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
}
