package org.talend.components.onedrive.common;

import lombok.extern.slf4j.Slf4j;
import org.talend.components.onedrive.service.http.BadCredentialsException;
import org.talend.components.onedrive.service.http.BadRequestException;
import org.talend.components.onedrive.service.http.OneDriveHttpClientService;
import org.talend.sdk.component.api.service.Service;

import java.io.IOException;
import java.io.Serializable;
import java.util.Map;
import java.util.TreeMap;

@Slf4j
@Service
public class HealthChecker implements Serializable {

    // @Service
    // private ConfigurationService configurationService;

    @Service
    private OneDriveHttpClientService oneDriveHttpClientService;

    // public HealthChecker(final OneDriveDataStore configuration,
    // final OneDriveHttpClientService oneDriveHttpClientService) {
    // this.configuration = configuration;
    // this.oneDriveHttpClientService = oneDriveHttpClientService;
    // }

    public boolean checkHealth(OneDriveDataStore datastore)
            throws UnknownAuthenticationTypeException, IOException, BadRequestException, BadCredentialsException {
        // filter parameters
        Map<String, String> allParameters = new TreeMap<>();
        allParameters.put("searchCriteria[pageSize]", "1");
        allParameters.put("searchCriteria[currentPage]", "1");

        // OneDriveDataStore configuration = configurationService.getConfiguration().getDataStore();
        String magentoUrl = "index.php/rest/" + "/" + "products";

        // oneDriveHttpClientService.getRecords(magentoUrl, allParameters);
        return true;
    }
}