package org.talend.components.magentocms.input;

import oauth.signpost.exception.OAuthCommunicationException;
import oauth.signpost.exception.OAuthExpectationFailedException;
import oauth.signpost.exception.OAuthMessageSignerException;
import org.talend.components.magentocms.common.RequestType;
import org.talend.components.magentocms.helpers.AuthorizationHelper;
import org.talend.components.magentocms.service.MagentoCmsService;
import org.talend.components.magentocms.service.http.MagentoApiClient;
import org.talend.sdk.component.api.configuration.Option;
import org.talend.sdk.component.api.input.Producer;
import org.talend.sdk.component.api.meta.Documentation;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import javax.json.JsonBuilderFactory;
import javax.json.JsonObject;
import javax.json.JsonValue;
import java.io.Serializable;
import java.net.MalformedURLException;
import java.util.Iterator;

@Documentation("TODO fill the documentation for this input")
public class MagentoCmsInputSource implements Serializable {

    private final MagentoCmsInputMapperConfiguration configuration;

    private final MagentoCmsService service;

    private final MagentoApiClient magentoApiClient;

    private final JsonBuilderFactory jsonBuilderFactory;

    private Iterator<JsonObject> dataArray;

    public MagentoCmsInputSource(@Option("configuration") final MagentoCmsInputMapperConfiguration configuration,
            final MagentoCmsService service, final JsonBuilderFactory jsonBuilderFactory,
            final MagentoApiClient magentoApiClient) {
        this.configuration = configuration;
        this.service = service;
        this.jsonBuilderFactory = jsonBuilderFactory;
        this.magentoApiClient = magentoApiClient;
    }

    @PostConstruct
    public void init() throws MalformedURLException, OAuthCommunicationException, OAuthExpectationFailedException,
            OAuthMessageSignerException {
        String magentoUrl = "http://" + configuration.getMagentoWebServerAddress() + "/index.php/rest/"
                + configuration.getMagentoRestVersion() + "/" + configuration.getSelectionType().name().toLowerCase() + "/"
                + configuration.getSelectionId();

        String auth = AuthorizationHelper.getAuthorizationOAuth1(configuration.getAuthenticationOauth1ConsumerKey(),
                configuration.getAuthenticationOauth1ConsumerSecret(), configuration.getAuthenticationOauth1AccessToken(),
                configuration.getAuthenticationOauth1AccessTokenSecret(), magentoUrl, RequestType.GET);

        magentoApiClient.base(magentoUrl);
        dataArray = magentoApiClient.getRecords(auth).iterator();
    }

    @Producer
    public JsonObject next() {
        if (dataArray.hasNext()) {
            JsonValue val = dataArray.next();
            return val.asJsonObject();
        }
        return null;
    }

    @PreDestroy
    public void release() {
    }
}