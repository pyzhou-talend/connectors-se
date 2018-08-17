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
import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.TreeMap;
import java.util.stream.Collectors;

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
        // parameters
        Map<String, String> allParameters = new TreeMap<>();
        if (configuration.getSelectionFilter().getFilterAdvancedValue().trim().isEmpty()) {
            fillFilterParameters(allParameters, configuration.getSelectionFilter());
        }
        fillFieldsParameters(allParameters, configuration.getSelectedFields());
        // StringBuilder allParametersStr = new StringBuilder();

        String allParametersStr = allParameters.entrySet().stream().map(entry -> entry.getKey() + "=" + entry.getValue())
                .collect(Collectors.joining("&"));
        if (!configuration.getSelectionFilter().getFilterAdvancedValue().trim().isEmpty()) {
            allParametersStr += allParametersStr.isEmpty() ? "" : "&";
            allParametersStr += configuration.getSelectionFilter().getFilterAdvancedValue().trim();
        }

        String magentoUrl = configuration.getMagentoCmsConfigurationBase().getMagentoWebServerUrl() + "/index.php/rest/"
                + configuration.getMagentoCmsConfigurationBase().getMagentoRestVersion() + "/"
                + configuration.getSelectionType().name().toLowerCase();
        // magentoUrl += "?" + URLEncoder.encode(allParametersStr, "UTF-8");
        magentoUrl += "?" + allParametersStr;

        try {
            dataArrayIterator = magentoHttpServiceFactory.createMagentoHttpService(configuration.getMagentoCmsConfigurationBase())
                    .getRecords(magentoUrl).iterator();
        } catch (BadRequestException e) {
            System.err.println(e.getMessage());
        }
    }

    private void fillFilterParameters(Map<String, String> allParameters, ConfigurationFilter filterConfiguration)
            throws UnsupportedEncodingException {
        Map<Integer, Integer> filterIds = new HashMap<>();
        if (filterConfiguration != null) {
            int groupId = 0;
            for (SelectionFilter filter : filterConfiguration.getFilterLines()) {
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
                        URLEncoder.encode(filter.getValue(), "UTF-8"));

                if (filterConfiguration.getFilterOperator() == SelectionFilterOperator.AND) {
                    groupId++;
                }
            }
        }
    }

    private void fillFieldsParameters(Map<String, String> allParameters, String fields) {
        if (fields != null && !fields.isEmpty()) {
            allParameters.put("fields", fields);
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