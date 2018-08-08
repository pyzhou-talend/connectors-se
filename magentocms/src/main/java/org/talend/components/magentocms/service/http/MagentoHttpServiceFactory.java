package org.talend.components.magentocms.service.http;

import lombok.AllArgsConstructor;
import oauth.signpost.commonshttp.HttpRequestAdapter;
import oauth.signpost.exception.OAuthCommunicationException;
import oauth.signpost.exception.OAuthExpectationFailedException;
import oauth.signpost.exception.OAuthMessageSignerException;
import org.apache.http.HttpEntity;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;
import org.talend.components.magentocms.common.AuthenticationSettings;
import org.talend.components.magentocms.common.AuthenticationType;
import org.talend.components.magentocms.common.UnknownAuthenticationTypeException;
import org.talend.components.magentocms.helpers.AuthorizationHelper;
import org.talend.sdk.component.api.service.Service;

import javax.json.JsonObject;
import javax.json.stream.JsonParser;
import javax.json.stream.JsonParserFactory;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

@Service
public class MagentoHttpServiceFactory {

    @Service
    private JsonParserFactory jsonParserFactory;

    @AllArgsConstructor
    public class MagentoHttpService {

        private AuthenticationType authenticationType;

        private AuthenticationSettings authenticationSettings;

        public List<JsonObject> getRecords(String magentoUrl) throws IOException, OAuthCommunicationException,
                OAuthExpectationFailedException, OAuthMessageSignerException, UnknownAuthenticationTypeException {
            CloseableHttpClient httpclient = HttpClients.createDefault();
            try {
                HttpGet httpGet = new HttpGet(magentoUrl);
                HttpRequestAdapter httpRequestAdapter = new HttpRequestAdapter(httpGet);
                // oAuthConsumer.sign(httpRequestAdapter);

                AuthorizationHelper.setAuthorization(httpRequestAdapter, authenticationType, authenticationSettings);

                CloseableHttpResponse response = httpclient.execute(httpGet);
                try {
                    if (response.getStatusLine().getStatusCode() == 200) {
                        HttpEntity entity = response.getEntity();
                        List<JsonObject> dataList = new ArrayList<>();
                        JsonParser jsonParser = jsonParserFactory.createParser(entity.getContent());
                        jsonParser.getObject().getJsonArray("items").forEach((t) -> {
                            dataList.add(t.asJsonObject());
                        });
                        EntityUtils.consume(entity);
                        return dataList;
                    }
                } finally {
                    response.close();
                }
            } finally {
                httpclient.close();
            }

            // HttpPost httpPost = new HttpPost("http://targethost/login");
            // List <NameValuePair> nvps = new ArrayList <NameValuePair>();
            // nvps.add(new BasicNameValuePair("username", "vip"));
            // nvps.add(new BasicNameValuePair("password", "secret"));
            // httpPost.setEntity(new UrlEncodedFormEntity(nvps));
            // CloseableHttpResponse response2 = httpclient.execute(httpPost);
            //
            // try {
            // System.out.println(response2.getStatusLine());
            // HttpEntity entity2 = response2.getEntity();
            // // do something useful with the response body
            // // and ensure it is fully consumed
            // //EntityUtils.consume(entity2);
            // } finally {
            // response2.close();
            // }

            throw new RuntimeException("Get records error");
        }
    }

    public MagentoHttpService createMagentoHttpService(AuthenticationType authenticationType,
            AuthenticationSettings authenticationSettings) {
        return new MagentoHttpService(authenticationType, authenticationSettings);
    }

}
