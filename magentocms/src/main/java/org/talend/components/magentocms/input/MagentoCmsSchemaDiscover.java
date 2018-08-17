package org.talend.components.magentocms.input;

import lombok.extern.slf4j.Slf4j;
import oauth.signpost.exception.OAuthCommunicationException;
import oauth.signpost.exception.OAuthExpectationFailedException;
import oauth.signpost.exception.OAuthMessageSignerException;
import org.talend.components.magentocms.common.UnknownAuthenticationTypeException;
import org.talend.components.magentocms.helpers.StringHelper;
import org.talend.components.magentocms.service.http.BadRequestException;
import org.talend.components.magentocms.service.http.MagentoHttpServiceFactory;
import org.talend.sdk.component.api.configuration.Option;
import org.talend.sdk.component.api.meta.Documentation;

import javax.json.JsonObject;
import javax.json.JsonValue;
import java.io.IOException;
import java.io.Serializable;
import java.util.*;

@Slf4j
@Documentation("TODO fill the documentation for this input")
public class MagentoCmsSchemaDiscover implements Serializable {

    private final MagentoCmsInputMapperConfiguration configuration;

    private final MagentoHttpServiceFactory magentoHttpServiceFactory;

    public MagentoCmsSchemaDiscover(@Option("configuration") final MagentoCmsInputMapperConfiguration configuration,
            final MagentoHttpServiceFactory magentoHttpServiceFactory) {
        this.configuration = configuration;
        this.magentoHttpServiceFactory = magentoHttpServiceFactory;
    }

    public List<String> getColumns() throws UnknownAuthenticationTypeException, IOException, OAuthExpectationFailedException,
            OAuthCommunicationException, OAuthMessageSignerException {
        List<String> result = new ArrayList<>();

        // filter parameters
        Map<String, String> allParameters = new TreeMap<>();
        allParameters.put("searchCriteria[filter_groups][0][filters][0][field]", "name");
        allParameters.put("searchCriteria[filter_groups][0][filters][0][condition_type]", "notnull");
        allParameters.put("searchCriteria[filter_groups][0][filters][0][value]", "");
        allParameters.put("searchCriteria[pageSize]", "1");
        allParameters.put("searchCriteria[currentPage]", "1");

        String allParametersStr = StringHelper.httpParametersMapToString(allParameters);

        String magentoUrl = configuration.getMagentoCmsConfigurationBase().getMagentoWebServerUrl() + "/index.php/rest/"
                + configuration.getMagentoCmsConfigurationBase().getMagentoRestVersion() + "/"
                + configuration.getSelectionType().name().toLowerCase();
        magentoUrl += "?" + allParametersStr;

        try {
            Iterator<JsonObject> dataArrayIterator = magentoHttpServiceFactory
                    .createMagentoHttpService(configuration.getMagentoCmsConfigurationBase()).getRecords(magentoUrl).iterator();
            if (dataArrayIterator.hasNext()) {
                JsonValue val = dataArrayIterator.next();
                val.asJsonObject().forEach((columnName, value) -> result.add(columnName));
            }
        } catch (BadRequestException e) {
            log.error(e.getMessage());
        }
        return result;
    }
}