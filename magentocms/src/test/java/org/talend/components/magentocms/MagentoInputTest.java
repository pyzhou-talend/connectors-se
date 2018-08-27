package org.talend.components.magentocms;

import lombok.extern.slf4j.Slf4j;
import oauth.signpost.commonshttp.HttpRequestAdapter;
import oauth.signpost.exception.OAuthCommunicationException;
import oauth.signpost.exception.OAuthExpectationFailedException;
import oauth.signpost.exception.OAuthMessageSignerException;
import org.apache.http.client.methods.HttpGet;
import org.junit.jupiter.api.Test;
import org.talend.components.magentocms.common.*;
import org.talend.components.magentocms.helpers.AuthorizationHelper;
import org.talend.components.magentocms.service.http.BadCredentialsException;

import java.io.IOException;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Slf4j
public class MagentoInputTest {

    @Test
    public void testConnection() throws UnknownAuthenticationTypeException {
        AuthenticationOauth1Settings authenticationOauth1Settings = new AuthenticationOauth1Settings();
        AuthenticationTokenSettings authenticationTokenSettings = new AuthenticationTokenSettings();
        AuthenticationLoginPasswordSettings authenticationLoginPasswordSettings = new AuthenticationLoginPasswordSettings();
        MagentoCmsConfigurationBase magentoCmsConfigurationBase;
        magentoCmsConfigurationBase = new MagentoCmsConfigurationBase(null, null, AuthenticationType.OAUTH_1,
                authenticationOauth1Settings, authenticationTokenSettings, authenticationLoginPasswordSettings);
        assertEquals(authenticationOauth1Settings, magentoCmsConfigurationBase.getAuthSettings());
        magentoCmsConfigurationBase = new MagentoCmsConfigurationBase(null, null, AuthenticationType.AUTHENTICATION_TOKEN,
                authenticationOauth1Settings, authenticationTokenSettings, authenticationLoginPasswordSettings);
        assertEquals(authenticationTokenSettings, magentoCmsConfigurationBase.getAuthSettings());
        magentoCmsConfigurationBase = new MagentoCmsConfigurationBase(null, null, AuthenticationType.LOGIN_PASSWORD,
                authenticationOauth1Settings, authenticationTokenSettings, authenticationLoginPasswordSettings);
        assertEquals(authenticationLoginPasswordSettings, magentoCmsConfigurationBase.getAuthSettings());
    }

    @Test
    public void testOauthSign() throws UnknownAuthenticationTypeException, BadCredentialsException,
            OAuthExpectationFailedException, OAuthCommunicationException, OAuthMessageSignerException, IOException {
        AuthenticationOauth1Settings authenticationOauth1Settings = new AuthenticationOauth1Settings(
                "4jorv7co8fh64xuw58tljqgos50s3mph", "l4yiciq6wn9qs8oc9c1n7a9qo6mbxe6v", "1hxuj7fp1v54vtbl77tt7b8af5yl9hgg",
                "bsxkoh48xy00v1uamk4ewvbks2p4t16v");
        MagentoCmsConfigurationBase magentoCmsConfigurationBase;
        magentoCmsConfigurationBase = new MagentoCmsConfigurationBase(null, null, AuthenticationType.OAUTH_1,
                authenticationOauth1Settings, null, null);

        // get data
        String magentoUrl = "http://test";
        HttpGet httpGet = new HttpGet(magentoUrl);
        // add authentication
        HttpRequestAdapter httpRequestAdapter = new HttpRequestAdapter(httpGet);
        AuthorizationHelper.setAuthorization(httpRequestAdapter, magentoCmsConfigurationBase);

        String authHeader = httpRequestAdapter.getAllHeaders().get("Authorization");
        /*
         * OAuth oauth_consumer_key="4jorv7co8fh64xuw58tljqgos50s3mph", oauth_nonce="8307011346051803758",
         * oauth_signature="GHpRcztzkLRbMlOR98lUy9Y%2FqsY%3D", oauth_signature_method="HMAC-SHA1",
         * oauth_timestamp="1535376023", oauth_token="1hxuj7fp1v54vtbl77tt7b8af5yl9hgg", oauth_version="1.0"
         */
        assertTrue(authHeader.contains("oauth_consumer_key"));
    }

}
