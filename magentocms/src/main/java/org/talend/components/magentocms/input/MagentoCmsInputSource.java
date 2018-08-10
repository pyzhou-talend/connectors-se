package org.talend.components.magentocms.input;

import oauth.signpost.exception.OAuthCommunicationException;
import oauth.signpost.exception.OAuthExpectationFailedException;
import oauth.signpost.exception.OAuthMessageSignerException;
import org.talend.components.magentocms.common.UnknownAuthenticationTypeException;
import org.talend.components.magentocms.service.http.BadRequestException;
import org.talend.components.magentocms.service.http.MagentoHttpServiceFactory;
import org.talend.sdk.component.api.configuration.Option;
import org.talend.sdk.component.api.input.Producer;
import org.talend.sdk.component.api.meta.Documentation;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import javax.json.JsonObject;
import javax.json.JsonValue;
import java.io.IOException;
import java.io.Serializable;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.TreeMap;

@Documentation("TODO fill the documentation for this input")
public class MagentoCmsInputSource implements Serializable {

    private final MagentoCmsInputMapperConfiguration configuration;

    private final MagentoHttpServiceFactory magentoHttpServiceFactory;

    private Iterator<JsonObject> dataArrayIterator;

    public MagentoCmsInputSource(@Option("configuration") final MagentoCmsInputMapperConfiguration configuration,
            final MagentoHttpServiceFactory magentoHttpServiceFactory) {
        this.configuration = configuration;
        this.magentoHttpServiceFactory = magentoHttpServiceFactory;
    }

    @PostConstruct
    public void init() throws UnknownAuthenticationTypeException, IOException, OAuthExpectationFailedException,
            OAuthCommunicationException, OAuthMessageSignerException {
        // filter parameters
        Map<String, String> allParameters = new TreeMap<>();
        Map<Integer, Integer> filterIds = new HashMap<>();
        if (configuration.getSelectionFilter() != null) {
            for (SelectionFilter filter : configuration.getSelectionFilter()) {
                int groupId = filter.getAndGroupNumber();

                Integer filterId = filterIds.get(groupId);
                if (filterId == null) {
                    filterId = 0;
                } else {
                    filterId++;
                }
                filterIds.put(groupId, filterId);

                allParameters.put("searchCriteria[filter_groups][" + groupId + "][filters][" + filterId + "][field]",
                        filter.getFieldName());
                allParameters.put("searchCriteria[filter_groups][" + groupId + "][filters][" + filterId + "][condition_type]",
                        filter.getFieldNameCondition());
                allParameters.put("searchCriteria[filter_groups][" + groupId + "][filters][" + filterId + "][value]",
                        filter.getValue());
            }
        }
        if (configuration.getSelectedFields() != null && !configuration.getSelectedFields().isEmpty()) {
            allParameters.put("fields", configuration.getSelectedFields());
        }
        StringBuilder allParametersStr = new StringBuilder();
        boolean addSeparator = false;
        for (Map.Entry entry : allParameters.entrySet()) {
            if (addSeparator) {
                allParametersStr.append("&");
            } else {
                addSeparator = true;
            }
            allParametersStr.append(entry.getKey() + "=" + entry.getValue());
        }

        String magentoUrl = configuration.getMagentoCmsConfigurationBase().getMagentoWebServerUrl() + "/index.php/rest/"
                + configuration.getMagentoCmsConfigurationBase().getMagentoRestVersion() + "/"
                + configuration.getSelectionType().name().toLowerCase();
        magentoUrl += "?" + allParametersStr;

        try {
            dataArrayIterator = magentoHttpServiceFactory
                    .createMagentoHttpService(configuration.getMagentoCmsConfigurationBase().getAuthenticationType(),
                            configuration.getMagentoCmsConfigurationBase().getAuthSettings())
                    .getRecords(magentoUrl).iterator();
        } catch (BadRequestException e) {
            System.err.println(e.getMessage());
        }
    }

    @Producer
    public JsonObject next() {
        if (dataArrayIterator != null && dataArrayIterator.hasNext()) {
            JsonValue val = dataArrayIterator.next();
            return val.asJsonObject();
        }
        return null;
    }

    @PreDestroy
    public void release() {
    }
}