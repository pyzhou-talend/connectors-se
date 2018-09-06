package org.talend.components.magentocms.service.http;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.talend.sdk.component.api.meta.Documentation;
import org.talend.sdk.component.api.service.http.*;
import org.talend.sdk.component.api.service.http.configurer.oauth1.OAuth1;

import javax.json.JsonObject;
import java.util.Map;

public interface MagentoApiClient extends HttpClient {

    String HEADER_Authorization = "Authorization";

    String HEADER_Content_Type = "Content-Type";

    Logger log = LoggerFactory.getLogger(MagentoApiClient.class);

    @Request(path = "{requestPath}")
    @Documentation("read record from the table according to the data set definition")
    Response<JsonObject> get(@Path("requestPath") String requestPath, @Header(HEADER_Authorization) String auth,
            @QueryParams Map<String, String> qp);

    @Request(path = "{requestPath}")
    @UseConfigurer(OAuth1.Configurer.class)
    @Documentation("read record from the table according to the data set definition. It uses OAuth1 authorization")
    Response<JsonObject> get(@Path("requestPath") String requestPath,
            @ConfigurerOption("option") OAuth1.Configuration oauthOptions, @QueryParams Map<String, String> qp);

    // get records using explicit Authorization header
    default Response<JsonObject> getRecords(String requestPath, String auth, Map<String, String> queryParameters) {
        return get(requestPath, auth, queryParameters);
    }

    // get records for Oauth1 authentication type
    default Response<JsonObject> getRecords(String requestPath, OAuth1.Configuration oaut1Config,
            Map<String, String> queryParameters) {
        return get(requestPath, oaut1Config, queryParameters);
    }

    ///////////////////////////////////////////

    @Request(method = "POST", path = "{requestPath}")
    @Documentation("read record from the table according to the data set definition")
    Response<JsonObject> post(@Path("requestPath") String requestPath, @Header(HEADER_Authorization) String auth,
            @Header(HEADER_Content_Type) String contentType, JsonObject record);

    @Request(method = "POST", path = "{requestPath}")
    @UseConfigurer(OAuth1.Configurer.class)
    @Documentation("read record from the table according to the data set definition. It uses OAuth1 authorization")
    Response<JsonObject> post(@Path("requestPath") String requestPath,
            @ConfigurerOption("option") OAuth1.Configuration oauthOptions, @Header(HEADER_Content_Type) String contentType,
            JsonObject record);

    default Response<JsonObject> postRecords(String requestPath, String auth, JsonObject dataList) {
        return post(requestPath, auth, "application/json", dataList);
    }

    default Response<JsonObject> postRecords(String requestPath, OAuth1.Configuration oaut1Config, JsonObject dataList) {
        return post(requestPath, oaut1Config, "application/json", dataList);
    }

    ///////////////////////////////

}
