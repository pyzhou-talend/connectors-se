package org.talend.components.magentocms.helpers;

import org.talend.components.magentocms.input.ConfigurationFilter;
import org.talend.components.magentocms.input.MagentoCmsInputMapperConfiguration;
import org.talend.components.magentocms.input.SelectionFilter;
import org.talend.components.magentocms.input.SelectionFilterOperator;
import org.talend.components.magentocms.output.MagentoCmsOutputConfiguration;
import org.talend.components.magentocms.service.ConfigurationServiceInput;
import org.talend.components.magentocms.service.ConfigurationServiceOutput;
import org.talend.components.magentocms.service.http.MagentoHttpClientService;

import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.util.HashMap;
import java.util.Map;

public class ConfigurationHelper {

    public static void fillFilterParameters(Map<String, String> allParameters, ConfigurationFilter filterConfiguration,
            boolean encodeValue) throws UnsupportedEncodingException {
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
                        encodeValue ? URLEncoder.encode(filter.getValue(), "UTF-8") : filter.getValue());

                if (filterConfiguration.getFilterOperator() == SelectionFilterOperator.AND) {
                    groupId++;
                }
            }
        }
    }

    public static void setupServicesInput(MagentoCmsInputMapperConfiguration configuration,
            ConfigurationServiceInput configurationServiceInput, MagentoHttpClientService magentoHttpClientService) {
        configurationServiceInput.setMagentoCmsInputMapperConfiguration(configuration);
        magentoHttpClientService.setBase(configuration.getMagentoCmsConfigurationBase().getMagentoWebServerUrl());
    }

    public static void setupServicesOutput(MagentoCmsOutputConfiguration configuration,
            ConfigurationServiceOutput configurationService, MagentoHttpClientService magentoHttpClientService) {
        configurationService.setMagentoCmsOutputConfiguration(configuration);
        magentoHttpClientService.setBase(configuration.getMagentoCmsConfigurationBase().getMagentoWebServerUrl());
    }
}
