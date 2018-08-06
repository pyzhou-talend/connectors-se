package org.talend.components.magentocms.input;

import oauth.signpost.exception.OAuthCommunicationException;
import oauth.signpost.exception.OAuthExpectationFailedException;
import oauth.signpost.exception.OAuthMessageSignerException;
import org.talend.components.magentocms.common.RequestType;
import org.talend.components.magentocms.common.UnknownAuthenticationTypeException;
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
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

@Documentation("TODO fill the documentation for this input")
public class MagentoCmsInputSource implements Serializable {

    private final MagentoCmsInputMapperConfiguration configuration;

    private final MagentoCmsService service;

    private final MagentoApiClient magentoApiClient;

    private final JsonBuilderFactory jsonBuilderFactory;

    private Iterator<JsonObject> dataArrayIterator;

    public MagentoCmsInputSource(@Option("configuration") final MagentoCmsInputMapperConfiguration configuration,
            final MagentoCmsService service, final JsonBuilderFactory jsonBuilderFactory,
            final MagentoApiClient magentoApiClient) {
        this.configuration = configuration;
        this.service = service;
        this.jsonBuilderFactory = jsonBuilderFactory;
        this.magentoApiClient = magentoApiClient;
    }

    @PostConstruct
    public void init() throws UnknownAuthenticationTypeException, MalformedURLException, OAuthExpectationFailedException,
            OAuthCommunicationException, OAuthMessageSignerException {
        // filter parameters
        Map<String, String> filterParameters = new HashMap<>();
        for (SelectionFilter filter : configuration.getSelectionFilter()) {
            int groupId = 0;
            int filterId = 0;
            filterParameters.put("searchCriteria[filter_groups][" + groupId + "][filters][" + filterId + "][field]",
                    filter.getFieldName());
            filterParameters.put("searchCriteria[filter_groups][" + groupId + "][filters][" + filterId + "][condition_type]",
                    filter.getCondition());
            filterParameters.put("searchCriteria[filter_groups][" + groupId + "][filters][" + filterId + "][value]",
                    filter.getValue());
        }

        StringBuilder filterParametersStr = new StringBuilder();
        boolean addSeparator = false;
        for (Map.Entry entry : filterParameters.entrySet()) {
            if (addSeparator) {
                filterParametersStr.append("&");
            } else {
                addSeparator = true;
            }
            filterParametersStr.append(entry.getKey() + "=" + entry.getValue());
        }

        String magentoUrl = configuration.getMagentoCmsConfigurationBase().getMagentoWebServerUrl() + "/index.php/rest/"
                + configuration.getMagentoCmsConfigurationBase().getMagentoRestVersion() + "/"
                + configuration.getSelectionType().name().toLowerCase() + "?" + filterParametersStr;

        String auth = AuthorizationHelper.getAuthorization(configuration.getMagentoCmsConfigurationBase().getAuthenticationType(),
                configuration.getMagentoCmsConfigurationBase().getAuthSettings(), magentoUrl, RequestType.GET);
        // String auth = AuthorizationHelper.getAuthorizationOAuth1(configuration.getAuthenticationOauth1ConsumerKey(),
        // configuration.getAuthenticationOauth1ConsumerSecret(), configuration.getAuthenticationOauth1AccessToken(),
        // configuration.getAuthenticationOauth1AccessTokenSecret(), magentoUrl, RequestType.GET);

        magentoApiClient.base(magentoUrl);
        dataArrayIterator = magentoApiClient.getRecords(auth, filterParameters).iterator();
    }

    @Producer
    public JsonObject next() {
        if (dataArrayIterator.hasNext()) {
            JsonValue val = dataArrayIterator.next();
            return val.asJsonObject();
        }
        return null;
    }

    @PreDestroy
    public void release() {
    }
}