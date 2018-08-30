package org.talend.components.magentocms.input;

import lombok.extern.slf4j.Slf4j;
import oauth.signpost.exception.OAuthCommunicationException;
import oauth.signpost.exception.OAuthExpectationFailedException;
import oauth.signpost.exception.OAuthMessageSignerException;
import org.talend.components.magentocms.common.MagentoCmsConfigurationBase;
import org.talend.components.magentocms.common.UnknownAuthenticationTypeException;
import org.talend.components.magentocms.helpers.StringHelper;
import org.talend.components.magentocms.service.http.BadCredentialsException;
import org.talend.components.magentocms.service.http.BadRequestException;
import org.talend.components.magentocms.service.http.MagentoHttpServiceFactory;
import org.talend.sdk.component.api.meta.Documentation;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

@Slf4j
@Documentation("TODO fill the documentation for this input")
public class MagentoCmsHealtChecker implements Serializable {

    private final MagentoCmsConfigurationBase configuration;

    private final MagentoHttpServiceFactory magentoHttpServiceFactory;

    public MagentoCmsHealtChecker(final MagentoCmsConfigurationBase configuration,
            final MagentoHttpServiceFactory magentoHttpServiceFactory) {
        this.configuration = configuration;
        this.magentoHttpServiceFactory = magentoHttpServiceFactory;
    }

    public boolean checkHealth() throws UnknownAuthenticationTypeException, IOException, OAuthExpectationFailedException,
            OAuthCommunicationException, OAuthMessageSignerException, BadRequestException, BadCredentialsException {
        List<String> result = new ArrayList<>();

        // filter parameters
        Map<String, String> allParameters = new TreeMap<>();
        // allParameters.put("searchCriteria[filter_groups][0][filters][0][field]", "name");
        // allParameters.put("searchCriteria[filter_groups][0][filters][0][condition_type]", "notnull");
        // allParameters.put("searchCriteria[filter_groups][0][filters][0][value]", "");
        allParameters.put("searchCriteria[pageSize]", "1");
        allParameters.put("searchCriteria[currentPage]", "1");

        String allParametersStr = StringHelper.httpParametersMapToString(allParameters);

        String magentoUrl = configuration.getMagentoWebServerUrl() + "/index.php/rest/" + configuration.getMagentoRestVersion()
                + "/" + "products";
        magentoUrl += "?" + allParametersStr;

        // try {
        magentoHttpServiceFactory.createMagentoHttpService(configuration).getRecords(magentoUrl);
        // } catch (Exception e) {
        // log.error(e.getMessage());
        // }
        return true;
    }
}