package org.talend.components.magentocms;

import lombok.extern.slf4j.Slf4j;
import oauth.signpost.commonshttp.HttpRequestAdapter;
import oauth.signpost.exception.OAuthCommunicationException;
import oauth.signpost.exception.OAuthExpectationFailedException;
import oauth.signpost.exception.OAuthMessageSignerException;
import org.apache.http.HttpEntity;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;
import org.junit.jupiter.api.BeforeAll;
import org.talend.components.magentocms.common.*;
import org.talend.components.magentocms.helpers.AuthorizationHelper;

import java.io.IOException;

import static org.junit.jupiter.api.Assertions.*;

@Slf4j
class ITMagentoInput {

    @BeforeAll
    static void init() {
    }

    // @Test
    void input() throws IOException, UnknownAuthenticationTypeException, OAuthExpectationFailedException,
            OAuthCommunicationException, OAuthMessageSignerException {
        log.info("Integration test start ");
        String dockerHostAddress = System.getProperty("dockerHostAddress");
        String magentoHttpPort = System.getProperty("magentoHttpPort");
        String magentoAdminName = System.getProperty("magentoAdminName");
        String magentoAdminPassword = System.getProperty("magentoAdminPassword");
        log.info("docker machine: " + dockerHostAddress + ":" + magentoHttpPort);
        log.info("magento admin: " + magentoAdminName + " " + magentoAdminPassword);

        AuthenticationLoginPasswordSettings authenticationSettings = new AuthenticationLoginPasswordSettings(magentoAdminName,
                magentoAdminPassword);
        final MagentoCmsConfigurationBase dataStore = new MagentoCmsConfigurationBase(
                "http://" + dockerHostAddress + ":" + magentoHttpPort, RestVersion.V1, AuthenticationType.LOGIN_PASSWORD, null,
                null, authenticationSettings);

        CloseableHttpClient httpclient = HttpClients.createDefault();
        try {
            // get admin's token
            String adminToken = null;
            String magentoUrl = "http://" + dockerHostAddress + ":" + magentoHttpPort + "/rest/V1/integration/admin/token";
            HttpPost httpPost = new HttpPost(magentoUrl);
            httpPost.setEntity(
                    new StringEntity("{\"username\":\"" + magentoAdminName + "\",\"password\":\"" + magentoAdminPassword + "\"}",
                            ContentType.APPLICATION_JSON));
            // add authentication
            CloseableHttpResponse response = httpclient.execute(httpPost);
            try {
                int status = response.getStatusLine().getStatusCode();
                assertEquals(200, status);
                if (status == 200) {
                    HttpEntity entity = response.getEntity();
                    adminToken = EntityUtils.toString(entity).replaceAll("\"", "");
                    log.debug(adminToken);
                    assertNotNull(adminToken);
                    assertNotEquals("", adminToken);
                    EntityUtils.consume(entity);
                }
            } finally {
                response.close();
            }

            // get data
            magentoUrl = "http://" + dockerHostAddress + ":" + magentoHttpPort + "/rest/V1/categories";
            HttpGet httpGet = new HttpGet(magentoUrl);
            // add authentication
            HttpRequestAdapter httpRequestAdapter = new HttpRequestAdapter(httpGet);
            AuthorizationHelper.setAuthorization(httpRequestAdapter, dataStore);

            response = httpclient.execute(httpGet);
            try {
                int status = response.getStatusLine().getStatusCode();
                assertEquals(200, status);
                if (status == 200) {
                    HttpEntity entity = response.getEntity();
                    log.debug("response: " + EntityUtils.toString(entity));
                    EntityUtils.consume(entity);
                }
            } finally {
                response.close();
            }
        } finally {
            httpclient.close();
        }
    }
}
