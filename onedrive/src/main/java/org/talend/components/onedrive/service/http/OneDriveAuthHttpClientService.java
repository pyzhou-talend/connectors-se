package org.talend.components.onedrive.service.http;

import lombok.extern.slf4j.Slf4j;
import org.talend.sdk.component.api.service.Service;
import org.talend.sdk.component.api.service.http.Response;

import javax.json.JsonObject;
import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;

@Service
@Slf4j
public class OneDriveAuthHttpClientService {

    private final static  String AUTH_SERVER = "https://login.microsoftonline.com/";

    @Service
    private OneDriveAuthHttpClient oneDriveAuthHttpClient;

    public void setBase() {
        oneDriveAuthHttpClient.base(AUTH_SERVER);
    }

    public String getToken(String tenantId, String client_id, String login, String password) throws UnsupportedEncodingException {
        String requestPath = tenantId + "/oauth2/token";
        String resource = "https://graph.microsoft.com/";
        String grant_type = "password";

        String body = "client_id=" + client_id + "&resource=" + URLEncoder.encode(resource, "UTF-8") + "&grant_type=" + grant_type
                + "&username=" + URLEncoder.encode(login, "UTF-8") + "&password=" + URLEncoder.encode(password, "UTF-8");

        Response<JsonObject> response = oneDriveAuthHttpClient.getToken(requestPath, body);
        String accessToken = null;
        if (response.status() == 200) {
            JsonObject responseBody = response.body();
            // convert json-string to string
            accessToken = responseBody.getString("access_token");
        }
        return accessToken;
    }
}
