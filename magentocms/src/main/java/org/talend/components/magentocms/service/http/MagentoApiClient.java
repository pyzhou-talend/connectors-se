package org.talend.components.magentocms.service.http;

import oauth.signpost.OAuthConsumer;
import oauth.signpost.commonshttp.CommonsHttpOAuthConsumer;
import oauth.signpost.commonshttp.HttpRequestAdapter;
import oauth.signpost.exception.OAuthCommunicationException;
import oauth.signpost.exception.OAuthExpectationFailedException;
import oauth.signpost.exception.OAuthMessageSignerException;
import oauth.signpost.signature.AuthorizationHeaderSigningStrategy;
import org.apache.http.HttpEntity;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.talend.sdk.component.api.meta.Documentation;
import org.talend.sdk.component.api.service.http.*;

import javax.json.JsonObject;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public interface MagentoApiClient extends HttpClient {

    String HEADER_Authorization = "Authorization";

    String HEADER_Content_Type = "Content-Type";

    @Request
    // @Codec(decoder = { InvalidContentDecoder.class })
    @Documentation("read record from the table according to the data set definition")
    Response<JsonObject> get(@Header(HEADER_Authorization) String auth
    // , @QueryParams Map<String, String> qp
    );

    default List<JsonObject> getRecords(String auth, Map<String, String> filterParameters)
            throws IOException, OAuthCommunicationException, OAuthExpectationFailedException, OAuthMessageSignerException {
        final Response<JsonObject> resp = get(auth);
        if (resp.status() != 200) {
            throw new HttpException(resp);
        }

        CloseableHttpClient httpclient = HttpClients.createDefault();
        HttpGet httpGet = new HttpGet("http://targethost/homepage");

        String consumerKey = "";
        String consumerSecret = "";
        String accessToken = "";
        String accessTokenSecret = "";

        OAuthConsumer oAuthConsumer = new CommonsHttpOAuthConsumer(consumerKey, consumerSecret);
        oAuthConsumer.setTokenWithSecret(accessToken, accessTokenSecret);
        oAuthConsumer.setSigningStrategy(new AuthorizationHeaderSigningStrategy());

        HttpRequestAdapter httpRequestAdapter = new HttpRequestAdapter(httpGet);
        oAuthConsumer.sign(httpRequestAdapter);

        CloseableHttpResponse response1 = httpclient.execute(httpGet);
        // The underlying HTTP connection is still held by the response object
        // to allow the response content to be streamed directly from the network socket.
        // In order to ensure correct deallocation of system resources
        // the user MUST call CloseableHttpResponse#close() from a finally clause.
        // Please note that if response content is not fully consumed the underlying
        // connection cannot be safely re-used and will be shut down and discarded
        // by the connection manager.
        try {
            System.out.println(response1.getStatusLine());
            HttpEntity entity1 = response1.getEntity();
            // do something useful with the response body
            // and ensure it is fully consumed
            // EntityUtils.consume(entity1);
        } finally {
            response1.close();
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

        List<JsonObject> dataList = new ArrayList<>();
        resp.body().getJsonArray("items").forEach((t) -> {
            dataList.add(t.asJsonObject());
        });
        return dataList;
    }

    @Request
    // @Codec(decoder = { InvalidContentDecoder.class })
    @Documentation("read record from the table according to the data set definition")
    Response<JsonObject> post(@Header(HEADER_Authorization) String auth, @Header(HEADER_Content_Type) String contentType,
            JsonObject record);

    default JsonObject postRecords(String auth, JsonObject dataList) {
        final Response<JsonObject> resp = post(auth, "application/json", dataList);
        if (resp.status() != 200) {
            throw new HttpException(resp);
        }
        return resp.body();
    }
}
