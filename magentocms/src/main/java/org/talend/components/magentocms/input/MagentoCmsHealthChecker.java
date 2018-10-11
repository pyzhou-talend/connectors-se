package org.talend.components.magentocms.input;

import lombok.extern.slf4j.Slf4j;
import org.talend.components.magentocms.common.MagentoDataStore;
import org.talend.components.magentocms.common.UnknownAuthenticationTypeException;
import org.talend.components.magentocms.service.http.BadCredentialsException;
import org.talend.components.magentocms.service.http.BadRequestException;
import org.talend.components.magentocms.service.http.MagentoHttpClientService;
import org.talend.sdk.component.api.service.Service;

import java.io.IOException;
import java.io.Serializable;
import java.util.Map;
import java.util.TreeMap;

@Slf4j
@Service
public class MagentoCmsHealthChecker implements Serializable {

    // @Service
    // private ConfigurationServiceInput configurationServiceInput;

    @Service
    private MagentoHttpClientService magentoHttpClientService = null;

    public boolean checkHealth(MagentoDataStore dataStore)
            throws UnknownAuthenticationTypeException, IOException, BadRequestException, BadCredentialsException {
        // filter parameters
        Map<String, String> allParameters = new TreeMap<>();
        allParameters.put("searchCriteria[pageSize]", "1");
        allParameters.put("searchCriteria[currentPage]", "1");

        String magentoUrl = "index.php/rest/" + dataStore.getMagentoRestVersion() + "/" + "products";

        magentoHttpClientService.getRecords(dataStore, magentoUrl, allParameters);
        return true;
    }
}