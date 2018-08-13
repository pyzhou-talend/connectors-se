package org.talend.components.magentocms.service.http;

import lombok.AllArgsConstructor;
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
import org.talend.components.magentocms.common.AuthenticationSettings;
import org.talend.components.magentocms.common.AuthenticationType;
import org.talend.components.magentocms.common.UnknownAuthenticationTypeException;
import org.talend.components.magentocms.helpers.AuthorizationHelper;
import org.talend.sdk.component.api.service.Service;

import javax.json.JsonObject;
import javax.json.JsonValue;
import javax.json.stream.JsonParser;
import javax.json.stream.JsonParserFactory;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

@Service
@Slf4j
public class MagentoHttpServiceFactory {

    @Service
    private JsonParserFactory jsonParserFactory;

    @AllArgsConstructor
    public class MagentoHttpService {

        private AuthenticationType authenticationType;

        private AuthenticationSettings authenticationSettings;

        public List<JsonObject> getColumns(String magentoUrl)
                throws IOException, OAuthCommunicationException, OAuthExpectationFailedException, OAuthMessageSignerException,
                UnknownAuthenticationTypeException, BadRequestException {
            CloseableHttpClient httpclient = HttpClients.createDefault();
            try {
                HttpGet httpGet = new HttpGet(magentoUrl);
                // add authentication
                HttpRequestAdapter httpRequestAdapter = new HttpRequestAdapter(httpGet);
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
                        log.debug("get columns end");
                        EntityUtils.consume(entity);
                        return dataList;
                    } else if (response.getStatusLine().getStatusCode() == 400) {
                        int status = response.getStatusLine().getStatusCode();
                        HttpEntity entity = response.getEntity();
                        JsonParser jsonParser = jsonParserFactory.createParser(entity.getContent());
                        JsonObject errorObject = jsonParser.getObject();
                        /*
                         * process messages like this:
                         * {"message":"%fieldName is a required field.","parameters":{"fieldName":"searchCriteria"}}
                         */
                        String message = errorObject.getJsonString("message").getString();
                        if (errorObject.getJsonObject("parameters") != null) {
                            for (Map.Entry<String, JsonValue> parameter : errorObject.getJsonObject("parameters").entrySet()) {
                                message = message.replaceAll("%" + parameter.getKey(), parameter.getValue().toString());
                            }
                        }
                        throw new BadRequestException(message);
                    }
                } finally {
                    response.close();
                }
            } finally {
                httpclient.close();
            }

            throw new RuntimeException("Get records error");
        }

        public List<JsonObject> getRecords(String magentoUrl)
                throws IOException, OAuthCommunicationException, OAuthExpectationFailedException, OAuthMessageSignerException,
                UnknownAuthenticationTypeException, BadRequestException {
            CloseableHttpClient httpclient = HttpClients.createDefault();
            try {
                HttpGet httpGet = new HttpGet(magentoUrl);
                // add authentication
                HttpRequestAdapter httpRequestAdapter = new HttpRequestAdapter(httpGet);
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
                    } else if (response.getStatusLine().getStatusCode() == 400) {
                        int status = response.getStatusLine().getStatusCode();
                        HttpEntity entity = response.getEntity();
                        JsonParser jsonParser = jsonParserFactory.createParser(entity.getContent());
                        JsonObject errorObject = jsonParser.getObject();
                        /*
                         * process messages like this:
                         * {"message":"%fieldName is a required field.","parameters":{"fieldName":"searchCriteria"}}
                         */
                        String message = errorObject.getJsonString("message").getString();
                        if (errorObject.getJsonObject("parameters") != null) {
                            for (Map.Entry<String, JsonValue> parameter : errorObject.getJsonObject("parameters").entrySet()) {
                                message = message.replaceAll("%" + parameter.getKey(), parameter.getValue().toString());
                            }
                        }
                        throw new BadRequestException(message);
                    }
                } finally {
                    response.close();
                }
            } finally {
                httpclient.close();
            }

            throw new RuntimeException("Get records error");
        }

        public void postRecords(String magentoUrl, JsonObject dataList) throws IOException, OAuthCommunicationException,
                OAuthExpectationFailedException, OAuthMessageSignerException, UnknownAuthenticationTypeException {
            CloseableHttpClient httpclient = HttpClients.createDefault();
            try {
                HttpPost httpPost = new HttpPost(magentoUrl);
                httpPost.setEntity(new StringEntity(dataList.toString(), ContentType.APPLICATION_JSON));

                // add authentication
                HttpRequestAdapter httpRequestAdapter = new HttpRequestAdapter(httpPost);
                AuthorizationHelper.setAuthorization(httpRequestAdapter, authenticationType, authenticationSettings);

                CloseableHttpResponse response = httpclient.execute(httpPost);
                try {
                    if (response.getStatusLine().getStatusCode() == 200) {
                        HttpEntity entity = response.getEntity();
                        EntityUtils.consume(entity);
                        return;
                    }
                } finally {
                    response.close();
                }
            } finally {
                httpclient.close();
            }

            throw new RuntimeException("Get records error");
        }
    }

    public MagentoHttpService createMagentoHttpService(AuthenticationType authenticationType,
            AuthenticationSettings authenticationSettings) {
        return new MagentoHttpService(authenticationType, authenticationSettings);
    }
}
