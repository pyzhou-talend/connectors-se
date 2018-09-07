package org.talend.components.magentocms.service.http;

import lombok.extern.slf4j.Slf4j;
import oauth.signpost.exception.OAuthCommunicationException;
import oauth.signpost.exception.OAuthExpectationFailedException;
import oauth.signpost.exception.OAuthMessageSignerException;
import org.talend.components.magentocms.common.AuthenticationLoginPasswordSettings;
import org.talend.components.magentocms.common.AuthenticationType;
import org.talend.components.magentocms.common.MagentoCmsConfigurationBase;
import org.talend.components.magentocms.common.UnknownAuthenticationTypeException;
import org.talend.components.magentocms.helpers.AuthorizationHelper;
import org.talend.components.magentocms.helpers.authhandlers.AuthorizationHandlerLoginPassword;
import org.talend.components.magentocms.service.ConfigurationService;
import org.talend.sdk.component.api.service.Service;
import org.talend.sdk.component.api.service.http.Response;
import org.talend.sdk.component.api.service.http.configurer.oauth1.OAuth1;

import javax.json.*;
import javax.json.stream.JsonParserFactory;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@Service
@Slf4j
public class MagentoHttpClientService {

    @Service
    private JsonParserFactory jsonParserFactory;

    @Service
    private JsonBuilderFactory jsonBuilderFactory;

    @Service
    private MagentoHttpClient magentoHttpClient;

    @Service
    private ConfigurationService configurationService;

    @Service
    private AuthorizationHelper authorizationHelper;

    public void setBase(String base) {
        magentoHttpClient.base(base);
    }

//
//    public MagentoHttpClientService() {
//    }

//    @PostConstruct
//    public void init(@Option("configuration") final MagentoCmsConfigurationBase magentoCmsConfigurationBase) {
//        this.magentoCmsConfigurationBase = magentoCmsConfigurationBase;
//    }

//    public MagentoHttpClientService(@Option("configuration") final MagentoCmsConfigurationBase magentoCmsConfigurationBase) {
//        this.magentoCmsConfigurationBase = magentoCmsConfigurationBase;
//    }

//    public void setData(@Option("configuration") final MagentoCmsConfigurationBase magentoCmsConfigurationBase) {
//        this.magentoCmsConfigurationBase = magentoCmsConfigurationBase;
//    }

    public List<JsonObject> getRecords(String requestPath, Map<String, String> queryParameters)
            throws IOException, OAuthCommunicationException, OAuthExpectationFailedException, OAuthMessageSignerException,
            UnknownAuthenticationTypeException, BadRequestException, BadCredentialsException {
        List<JsonObject> dataList;
        try {
            dataList = execGetRecords(requestPath, queryParameters);
            return dataList;
        } catch (UserTokenExpiredException e) {
            // try to get new token
            MagentoCmsConfigurationBase magentoCmsConfigurationBase = configurationService.getMagentoCmsInputMapperConfiguration()
                    .getMagentoCmsConfigurationBase();

            AuthenticationLoginPasswordSettings authSettings =
                    (AuthenticationLoginPasswordSettings) magentoCmsConfigurationBase.getAuthSettings();

            AuthorizationHandlerLoginPassword.clearTokenCache(authSettings);
            try {
                dataList = execGetRecords(requestPath, queryParameters);
                return dataList;
            } catch (UserTokenExpiredException e1) {
                throw new BadRequestException("User unauthorised exception");
            }
        }
    }

    private List<JsonObject> execGetRecords(String requestPath, Map<String, String> queryParameters) throws BadRequestException,
            UnknownAuthenticationTypeException, OAuthExpectationFailedException, OAuthCommunicationException,
            OAuthMessageSignerException, IOException, UserTokenExpiredException, BadCredentialsException {
        // escape '[', ']' in parameters for correct OAuth1 authentication
        Map<String, String> queryParametersOauth1 = queryParameters.entrySet().stream().collect(Collectors.toMap(e -> {
            try {
                return URLEncoder.encode(e.getKey(), "UTF-8");
            } catch (UnsupportedEncodingException e1) {
                e1.printStackTrace();
            }
            return "";
        }, Map.Entry::getValue, (u, v) -> {
            throw new IllegalStateException(String.format("Duplicate key %s", u));
        }, LinkedHashMap::new));

        MagentoCmsConfigurationBase magentoCmsConfigurationBase = configurationService.getMagentoCmsInputMapperConfiguration()
                .getMagentoCmsConfigurationBase();
        Response<JsonObject> response;
        if (magentoCmsConfigurationBase.getAuthenticationType() == AuthenticationType.OAUTH_1) {
            OAuth1.Configuration oauth1Config = OAuth1.Configuration.builder()
                    .consumerKey(
                            magentoCmsConfigurationBase.getAuthenticationOauth1Settings().getAuthenticationOauth1ConsumerKey())
                    .consumerSecret(
                            magentoCmsConfigurationBase.getAuthenticationOauth1Settings().getAuthenticationOauth1ConsumerSecret())
                    .token(magentoCmsConfigurationBase.getAuthenticationOauth1Settings().getAuthenticationOauth1AccessToken())
                    .tokenSecret(magentoCmsConfigurationBase.getAuthenticationOauth1Settings()
                            .getAuthenticationOauth1AccessTokenSecret())
                    .build();
            response = magentoHttpClient.getRecords(requestPath, oauth1Config, queryParametersOauth1);
        } else {
            String auth = authorizationHelper.getAuthorization(magentoCmsConfigurationBase);
            response = magentoHttpClient.getRecords(requestPath, auth, queryParameters);
        }

        if (response.status() == 200) {
            List<JsonObject> dataList = new ArrayList<>();
            response.body().getJsonArray("items").forEach((t) -> {
                dataList.add(t.asJsonObject());
            });
            return dataList;
        } else if (response.status() == 400) {
            handleBadRequest400(response.error(JsonObject.class), null);
            return null;
        } else if (response.status() == 401
                && magentoCmsConfigurationBase.getAuthenticationType() == AuthenticationType.LOGIN_PASSWORD) {
            // maybe token is expired
            throw new UserTokenExpiredException();
        } else {
            throw new BadRequestException("unknown exception");
        }
    }

    public JsonObject postRecords(String requestPath, JsonObject dataList)
            throws IOException, OAuthCommunicationException, OAuthExpectationFailedException, OAuthMessageSignerException,
            UnknownAuthenticationTypeException, BadRequestException, BadCredentialsException {
        try {
            JsonObject res = execPostRecords(requestPath, dataList);
            return res;
        } catch (UserTokenExpiredException e) {
            // try to get new token
            MagentoCmsConfigurationBase magentoCmsConfigurationBase = configurationService.getMagentoCmsInputMapperConfiguration()
                    .getMagentoCmsConfigurationBase();

            AuthenticationLoginPasswordSettings authSettings = (AuthenticationLoginPasswordSettings) magentoCmsConfigurationBase
                    .getAuthSettings();

            AuthorizationHandlerLoginPassword.clearTokenCache(authSettings);
            try {
                JsonObject res = execPostRecords(requestPath, dataList);
                return res;
            } catch (UserTokenExpiredException e1) {
                throw new BadRequestException("User unauthorised exception");
            }
        }
    }

    private JsonObject execPostRecords(String requestPath, JsonObject dataList)
            throws IOException, OAuthCommunicationException, OAuthExpectationFailedException, OAuthMessageSignerException,
            UnknownAuthenticationTypeException, BadRequestException, UserTokenExpiredException, BadCredentialsException {
        Response<JsonObject> response;
        MagentoCmsConfigurationBase magentoCmsConfigurationBase = configurationService.getMagentoCmsInputMapperConfiguration()
                .getMagentoCmsConfigurationBase();

        if (magentoCmsConfigurationBase.getAuthenticationType() == AuthenticationType.OAUTH_1) {
            OAuth1.Configuration oauth1Config = OAuth1.Configuration.builder()
                    .consumerKey(
                            magentoCmsConfigurationBase.getAuthenticationOauth1Settings().getAuthenticationOauth1ConsumerKey())
                    .consumerSecret(
                            magentoCmsConfigurationBase.getAuthenticationOauth1Settings().getAuthenticationOauth1ConsumerSecret())
                    .token(magentoCmsConfigurationBase.getAuthenticationOauth1Settings().getAuthenticationOauth1AccessToken())
                    .tokenSecret(magentoCmsConfigurationBase.getAuthenticationOauth1Settings()
                            .getAuthenticationOauth1AccessTokenSecret())
                    .build();
            response = magentoHttpClient.postRecords(requestPath, oauth1Config, dataList);
        } else {
            String auth = authorizationHelper.getAuthorization(magentoCmsConfigurationBase);
            response = magentoHttpClient.postRecords(requestPath, auth, dataList);
        }

        if (response.status() == 200) {
            return response.body();
        } else if (response.status() == 400) {
            handleBadRequest400(response.error(JsonObject.class), dataList.toString());
            return null;
        } else if (response.status() == 401
                && magentoCmsConfigurationBase.getAuthenticationType() == AuthenticationType.LOGIN_PASSWORD) {
            // maybe token is expired
            throw new UserTokenExpiredException();
        } else {
            throw new BadRequestException("unknown exception");
        }
    }

    public String getToken(String requestPath, String login, String password) {
        final JsonObject body = jsonBuilderFactory.createObjectBuilder().add("username", login).add("password", password).build();
        Response<JsonValue> response = magentoHttpClient.getToken(requestPath, body);
        String accessToken = null;
        if (response.status() == 200) {
            JsonValue responseBody = response.body();
            accessToken = responseBody.toString().replaceAll("\"", "");
        }
        return accessToken;
    }

    private void handleBadRequest400(JsonObject errorObject, String requestObject) throws BadRequestException, IOException {
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
}
