package org.talend.components.magentocms.helpers.authhandlers;

import oauth.signpost.OAuthConsumer;
import oauth.signpost.basic.HttpURLConnectionRequestAdapter;
import oauth.signpost.commonshttp.CommonsHttpOAuthConsumer;
import oauth.signpost.exception.OAuthCommunicationException;
import oauth.signpost.exception.OAuthExpectationFailedException;
import oauth.signpost.exception.OAuthMessageSignerException;
import oauth.signpost.signature.AuthorizationHeaderSigningStrategy;
import org.talend.components.magentocms.common.AuthenticationOauth1Settings;
import org.talend.components.magentocms.common.AuthenticationSettings;
import org.talend.components.magentocms.common.RequestType;

import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.ProtocolException;
import java.net.URL;

public class AuthorizationHandlerOAuth1 implements AuthorizationHandler {

    public String getAuthorization(AuthenticationSettings authenticationSettings, String magentoUrl, RequestType requestType)
            throws MalformedURLException, OAuthCommunicationException, OAuthExpectationFailedException,
            OAuthMessageSignerException {
        AuthenticationOauth1Settings authSettings = (AuthenticationOauth1Settings) authenticationSettings;
        String consumerKey = authSettings.getAuthenticationOauth1ConsumerKey();
        String consumerSecret = authSettings.getAuthenticationOauth1ConsumerSecret();
        String accessToken = authSettings.getAuthenticationOauth1AccessToken();
        String accessTokenSecret = authSettings.getAuthenticationOauth1AccessTokenSecret();

        OAuthConsumer oAuthConsumer = new CommonsHttpOAuthConsumer(consumerKey, consumerSecret);
        oAuthConsumer.setTokenWithSecret(accessToken, accessTokenSecret);
        oAuthConsumer.setSigningStrategy(new AuthorizationHeaderSigningStrategy());

        URL url = new URL(magentoUrl);
        HttpURLConnection urlConnection = new HttpURLConnection(url) {

            @Override
            public void disconnect() {
            }

            @Override
            public boolean usingProxy() {
                return false;
            }

            @Override
            public void connect() throws IOException {
            }
        };

        try {
            urlConnection.setRequestMethod(requestType.name());
        } catch (ProtocolException e) {
            e.printStackTrace();
        }
        HttpURLConnectionRequestAdapter requestAdapter = new HttpURLConnectionRequestAdapter(urlConnection);
        oAuthConsumer.sign(requestAdapter);
        String auth = requestAdapter.getHeader("Authorization");
        // String auth = "OAuth oauth_consumer_key=\"g9cqwq50ebbd86ac7q5o41384al11al5\"," +
        // "oauth_token=\"6499phs7bl8u4stv2nmdut9gli2c9gsc\"," +
        // "oauth_signature_method=\"HMAC-SHA1\",oauth_timestamp=\"1533209949\",oauth_nonce=\"WIL1yWVFIXI\"," +
        // "oauth_version=\"1.0\",oauth_signature=\"%2Bcc%2F09fxOK7BhRrBvhjHFwMXbeA%3D\"";

        return auth;
    }
}
