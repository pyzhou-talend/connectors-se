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
import org.talend.components.magentocms.common.AuthenticationLoginPasswordSettings;
import org.talend.components.magentocms.common.AuthenticationType;
import org.talend.components.magentocms.common.MagentoCmsConfigurationBase;
import org.talend.components.magentocms.common.UnknownAuthenticationTypeException;
import org.talend.components.magentocms.helpers.AuthorizationHelper;
import org.talend.components.magentocms.helpers.authhandlers.AuthorizationHandlerLoginPassword;
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

        private MagentoCmsConfigurationBase magentoCmsConfigurationBase;

        // public List<JsonObject> getColumns(String magentoUrl)
        // throws IOException, OAuthCommunicationException, OAuthExpectationFailedException, OAuthMessageSignerException,
        // UnknownAuthenticationTypeException, BadRequestException {
        // CloseableHttpClient httpclient = HttpClients.createDefault();
        // try {
        // HttpGet httpGet = new HttpGet(magentoUrl);
        // // add authentication
        // HttpRequestAdapter httpRequestAdapter = new HttpRequestAdapter(httpGet);
        // AuthorizationHelper.setAuthorization(httpRequestAdapter, authenticationType, authenticationSettings);
        //
        // CloseableHttpResponse response = httpclient.execute(httpGet);
        // try {
        // if (response.getStatusLine().getStatusCode() == 200) {
        // HttpEntity entity = response.getEntity();
        // List<JsonObject> dataList = new ArrayList<>();
        // JsonParser jsonParser = jsonParserFactory.createParser(entity.getContent());
        // jsonParser.getObject().getJsonArray("items").forEach((t) -> {
        // dataList.add(t.asJsonObject());
        // });
        // log.debug("get columns end");
        // EntityUtils.consume(entity);
        // return dataList;
        // } else if (response.getStatusLine().getStatusCode() == 400) {
        // int status = response.getStatusLine().getStatusCode();
        // HttpEntity entity = response.getEntity();
        // JsonParser jsonParser = jsonParserFactory.createParser(entity.getContent());
        // JsonObject errorObject = jsonParser.getObject();
        // /*
        // * process messages like this:
        // * {"message":"%fieldName is a required field.","parameters":{"fieldName":"searchCriteria"}}
        // */
        // String message = errorObject.getJsonString("message").getString();
        // if (errorObject.getJsonObject("parameters") != null) {
        // for (Map.Entry<String, JsonValue> parameter : errorObject.getJsonObject("parameters").entrySet()) {
        // message = message.replaceAll("%" + parameter.getKey(), parameter.getValue().toString());
        // }
        // }
        // throw new BadRequestException(message);
        // }
        // } finally {
        // response.close();
        // }
        // } finally {
        // httpclient.close();
        // }
        //
        // throw new RuntimeException("Get records error");
        // }

        public List<JsonObject> getRecords(String magentoUrl)
                throws IOException, OAuthCommunicationException, OAuthExpectationFailedException, OAuthMessageSignerException,
                UnknownAuthenticationTypeException, BadRequestException {
            CloseableHttpClient httpclient = HttpClients.createDefault();
            try {
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
                // HttpGet httpGet = new HttpGet(magentoUrl);
                // // add authentication
                // HttpRequestAdapter httpRequestAdapter = new HttpRequestAdapter(httpGet);
                // AuthorizationHelper.setAuthorization(httpRequestAdapter, magentoCmsConfigurationBase);
                //
                // CloseableHttpResponse response = httpclient.execute(httpGet);
                // try {
                // if (response.getStatusLine().getStatusCode() == 200) {
                // HttpEntity entity = response.getEntity();
                // List<JsonObject> dataList = new ArrayList<>();
                // JsonParser jsonParser = jsonParserFactory.createParser(entity.getContent());
                // jsonParser.getObject().getJsonArray("items").forEach((t) -> {
                // dataList.add(t.asJsonObject());
                // });
                // EntityUtils.consume(entity);
                // return dataList;
                // } else if (response.getStatusLine().getStatusCode() == 400) {
                // int status = response.getStatusLine().getStatusCode();
                // HttpEntity entity = response.getEntity();
                // JsonParser jsonParser = jsonParserFactory.createParser(entity.getContent());
                // JsonObject errorObject = jsonParser.getObject();
                // /*
                // * process messages like this:
                // * {"message":"%fieldName is a required field.","parameters":{"fieldName":"searchCriteria"}}
                // */
                // String message = errorObject.getJsonString("message").getString();
                // if (errorObject.getJsonObject("parameters") != null) {
                // for (Map.Entry<String, JsonValue> parameter : errorObject.getJsonObject("parameters").entrySet()) {
                // message = message.replaceAll("%" + parameter.getKey(), parameter.getValue().toString());
                // }
                // }
                // throw new BadRequestException(message);
                // }
                // } finally {
                // response.close();
                // }
            } finally {
                httpclient.close();
            }
        }

        private List<JsonObject> execGetRecords(CloseableHttpClient httpclient, String magentoUrl)
                throws BadRequestException, UnknownAuthenticationTypeException, OAuthExpectationFailedException,
                OAuthCommunicationException, OAuthMessageSignerException, IOException, UserTokenExpiredException {
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

        public void postRecords(String magentoUrl, JsonObject dataList)
                throws IOException, OAuthCommunicationException, OAuthExpectationFailedException, OAuthMessageSignerException,
                UnknownAuthenticationTypeException, BadRequestException {
            CloseableHttpClient httpclient = HttpClients.createDefault();
            try {
                try {
                    execPostRecords(httpclient, magentoUrl, dataList);
                    return;
                } catch (UserTokenExpiredException e) {
                    // try to get new token
                    AuthenticationLoginPasswordSettings authSettings = (AuthenticationLoginPasswordSettings) magentoCmsConfigurationBase
                            .getAuthSettings();

                    AuthorizationHandlerLoginPassword.clearTokenCache(authSettings);
                    try {
                        execPostRecords(httpclient, magentoUrl, dataList);
                        return;
                    } catch (UserTokenExpiredException e1) {
                        throw new BadRequestException("User unauthorised exception");
                    }
                }

                // HttpPost httpPost = new HttpPost(magentoUrl);
                // httpPost.setEntity(new StringEntity(dataList.toString(), ContentType.APPLICATION_JSON));
                //
                // // add authentication
                // HttpRequestAdapter httpRequestAdapter = new HttpRequestAdapter(httpPost);
                // AuthorizationHelper.setAuthorization(httpRequestAdapter, magentoCmsConfigurationBase);
                //
                // CloseableHttpResponse response = httpclient.execute(httpPost);
                // try {
                // if (response.getStatusLine().getStatusCode() == 200) {
                // HttpEntity entity = response.getEntity();
                // EntityUtils.consume(entity);
                // return;
                // }
                // } finally {
                // response.close();
                // }
            } finally {
                httpclient.close();
            }

            // throw new RuntimeException("Post records error");
        }

        private void execPostRecords(CloseableHttpClient httpclient, String magentoUrl, JsonObject dataList)
                throws IOException, OAuthCommunicationException, OAuthExpectationFailedException, OAuthMessageSignerException,
                UnknownAuthenticationTypeException, BadRequestException, UserTokenExpiredException {

            HttpPost httpPost = new HttpPost(magentoUrl);
            httpPost.setEntity(new StringEntity(dataList.toString(), ContentType.APPLICATION_JSON));

            // add authentication
            HttpRequestAdapter httpRequestAdapter = new HttpRequestAdapter(httpPost);
            AuthorizationHelper.setAuthorization(httpRequestAdapter, magentoCmsConfigurationBase);

            CloseableHttpResponse response = httpclient.execute(httpPost);
            try {
                if (response.getStatusLine().getStatusCode() == 200) {
                    HttpEntity entity = response.getEntity();
                    EntityUtils.consume(entity);
                    return;
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

            // throw new RuntimeException("Post records error");
        }
    }

    public MagentoHttpService createMagentoHttpService(MagentoCmsConfigurationBase magentoCmsConfigurationBase) {
        return new MagentoHttpService(magentoCmsConfigurationBase);
    }
}
