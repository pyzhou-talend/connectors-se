package org.talend.components.magentocms.service.http;

import lombok.RequiredArgsConstructor;
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
import org.talend.components.magentocms.common.AuthenticationLoginPasswordSettings;
import org.talend.components.magentocms.common.AuthenticationType;
import org.talend.components.magentocms.common.MagentoCmsConfigurationBase;
import org.talend.components.magentocms.common.UnknownAuthenticationTypeException;
import org.talend.components.magentocms.helpers.AuthorizationHelper;
import org.talend.components.magentocms.helpers.authhandlers.AuthorizationHandlerLoginPassword;
import org.talend.sdk.component.api.service.Service;

import javax.json.JsonArray;
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

    @RequiredArgsConstructor
    public class MagentoHttpService {

        private final MagentoCmsConfigurationBase magentoCmsConfigurationBase;

        private CloseableHttpClient httpclient = HttpClients.createDefault();

        public List<JsonObject> getRecords(String magentoUrl)
                throws IOException, OAuthCommunicationException, OAuthExpectationFailedException, OAuthMessageSignerException,
                UnknownAuthenticationTypeException, BadRequestException, BadCredentialsException {
            List<JsonObject> dataList;
            try {
                dataList = execGetRecords(httpclient, magentoUrl);
                return dataList;
            } catch (UserTokenExpiredException e) {
                // try to get new token
                AuthenticationLoginPasswordSettings authSettings = (AuthenticationLoginPasswordSettings) magentoCmsConfigurationBase
                        .getAuthSettings();

                AuthorizationHandlerLoginPassword.clearTokenCache(authSettings);
                try {
                    dataList = execGetRecords(httpclient, magentoUrl);
                    return dataList;
                } catch (UserTokenExpiredException e1) {
                    throw new BadRequestException("User unauthorised exception");
                }
            }
        }

        private List<JsonObject> execGetRecords(CloseableHttpClient httpclient, String magentoUrl) throws BadRequestException,
                UnknownAuthenticationTypeException, OAuthExpectationFailedException, OAuthCommunicationException,
                OAuthMessageSignerException, IOException, UserTokenExpiredException, BadCredentialsException {
            HttpGet httpGet = new HttpGet(magentoUrl);
            // add authentication
            HttpRequestAdapter httpRequestAdapter = new HttpRequestAdapter(httpGet);
            AuthorizationHelper.setAuthorization(httpRequestAdapter, magentoCmsConfigurationBase);

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
                    handleBadRequest400(response, null);
                    return null;
                } else if (response.getStatusLine().getStatusCode() == 401
                        && magentoCmsConfigurationBase.getAuthenticationType() == AuthenticationType.LOGIN_PASSWORD) {
                    // maybe token is expired
                    throw new UserTokenExpiredException();
                } else {
                    throw new BadRequestException("unknown exception");
                }
            } finally {
                response.close();
            }
        }

        public JsonObject postRecords(String magentoUrl, JsonObject dataList)
                throws IOException, OAuthCommunicationException, OAuthExpectationFailedException, OAuthMessageSignerException,
                UnknownAuthenticationTypeException, BadRequestException, BadCredentialsException {
            try {
                JsonObject res = execPostRecords(httpclient, magentoUrl, dataList);
                return res;
            } catch (UserTokenExpiredException e) {
                // try to get new token
                AuthenticationLoginPasswordSettings authSettings = (AuthenticationLoginPasswordSettings) magentoCmsConfigurationBase
                        .getAuthSettings();

                AuthorizationHandlerLoginPassword.clearTokenCache(authSettings);
                try {
                    JsonObject res = execPostRecords(httpclient, magentoUrl, dataList);
                    return res;
                } catch (UserTokenExpiredException e1) {
                    throw new BadRequestException("User unauthorised exception");
                }
            }
        }

        private JsonObject execPostRecords(CloseableHttpClient httpclient, String magentoUrl, JsonObject dataList)
                throws IOException, OAuthCommunicationException, OAuthExpectationFailedException, OAuthMessageSignerException,
                UnknownAuthenticationTypeException, BadRequestException, UserTokenExpiredException, BadCredentialsException {

            HttpPost httpPost = new HttpPost(magentoUrl);
            httpPost.setEntity(new StringEntity(dataList.toString(), ContentType.APPLICATION_JSON));

            // add authentication
            HttpRequestAdapter httpRequestAdapter = new HttpRequestAdapter(httpPost);
            AuthorizationHelper.setAuthorization(httpRequestAdapter, magentoCmsConfigurationBase);

            CloseableHttpResponse response = httpclient.execute(httpPost);
            try {
                if (response.getStatusLine().getStatusCode() == 200) {
                    HttpEntity entity = response.getEntity();

                    JsonParser jsonParser = jsonParserFactory.createParser(entity.getContent());
                    JsonObject newRecord = jsonParser.getObject();
                    EntityUtils.consume(entity);
                    return newRecord;
                } else if (response.getStatusLine().getStatusCode() == 400) {
                    handleBadRequest400(response, dataList.toString());
                    return null;
                } else if (response.getStatusLine().getStatusCode() == 401
                        && magentoCmsConfigurationBase.getAuthenticationType() == AuthenticationType.LOGIN_PASSWORD) {
                    // maybe token is expired
                    throw new UserTokenExpiredException();
                } else {
                    throw new BadRequestException("unknown exception");
                }
            } finally {
                response.close();
            }
        }
    }

    private void handleBadRequest400(CloseableHttpResponse response, String requestObject)
            throws BadRequestException, IOException {
        int status = response.getStatusLine().getStatusCode();
        HttpEntity entity = response.getEntity();
        JsonParser jsonParser = jsonParserFactory.createParser(entity.getContent());
        JsonObject errorObject = jsonParser.getObject();
        /*
         * process messages like this:
         * {"message":"%fieldName is a required field.","parameters":{"fieldName":"searchCriteria"}}
         */
        String message = errorObject.getJsonString("message").getString();
        if (errorObject.get("parameters") != null) {
            if (errorObject.get("parameters").getValueType() == JsonValue.ValueType.OBJECT) {
                for (Map.Entry<String, JsonValue> parameter : errorObject.getJsonObject("parameters").entrySet()) {
                    message = message.replaceAll("%" + parameter.getKey(), parameter.getValue().toString());
                }
            } else if (errorObject.get("parameters").getValueType() == JsonValue.ValueType.ARRAY) {
                JsonArray params = errorObject.getJsonArray("parameters");
                for (int i = 0; i < params.size(); i++) {
                    message = message.replaceAll("%" + (i + 1), params.getString(i));
                }
            }
        }
        throw new BadRequestException(
                "An error occurred: " + message + (requestObject == null ? "" : "For object: " + requestObject));
    }

    public MagentoHttpService createMagentoHttpService(MagentoCmsConfigurationBase magentoCmsConfigurationBase) {
        return new MagentoHttpService(magentoCmsConfigurationBase);
    }
}
