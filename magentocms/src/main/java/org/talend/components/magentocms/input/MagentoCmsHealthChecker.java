package org.talend.components.magentocms.input;

import lombok.extern.slf4j.Slf4j;
import oauth.signpost.exception.OAuthCommunicationException;
import oauth.signpost.exception.OAuthExpectationFailedException;
import oauth.signpost.exception.OAuthMessageSignerException;
import org.talend.components.magentocms.common.MagentoCmsConfigurationBase;
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

    @Service
    private MagentoCmsConfigurationBase configuration;

    @Service
    private MagentoHttpClientService magentoHttpClientService;

    // public MagentoCmsHealthChecker(final MagentoCmsConfigurationBase configuration,
    // final MagentoHttpClientService magentoHttpClientService) {
    // this.configuration = configuration;
    // this.magentoHttpClientService = magentoHttpClientService;
    // }

    public boolean checkHealth() throws UnknownAuthenticationTypeException, IOException, OAuthExpectationFailedException,
            OAuthCommunicationException, OAuthMessageSignerException, BadRequestException, BadCredentialsException {
        // filter parameters
        Map<String, String> allParameters = new TreeMap<>();
        allParameters.put("searchCriteria[pageSize]", "1");
        allParameters.put("searchCriteria[currentPage]", "1");

        String magentoUrl = configuration.getMagentoWebServerUrl() + "/index.php/rest/" + configuration.getMagentoRestVersion()
                + "/" + "products";

        magentoHttpClientService.getRecords(magentoUrl, allParameters);
        // magentoHttpServiceFactory.createMagentoHttpService(magentoUrl, configuration).getRecords(allParameters);
        return true;
    }
}