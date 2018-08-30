package org.talend.components.magentocms.service;

import lombok.extern.slf4j.Slf4j;
import oauth.signpost.exception.OAuthCommunicationException;
import oauth.signpost.exception.OAuthExpectationFailedException;
import oauth.signpost.exception.OAuthMessageSignerException;
import org.talend.components.magentocms.common.MagentoCmsConfigurationBase;
import org.talend.components.magentocms.common.UnknownAuthenticationTypeException;
import org.talend.components.magentocms.helpers.ConfigurationHelper;
import org.talend.components.magentocms.input.*;
import org.talend.components.magentocms.messages.Messages;
import org.talend.components.magentocms.service.http.MagentoHttpServiceFactory;
import org.talend.sdk.component.api.configuration.Option;
import org.talend.sdk.component.api.service.Service;
import org.talend.sdk.component.api.service.completion.SuggestionValues;
import org.talend.sdk.component.api.service.completion.Suggestions;
import org.talend.sdk.component.api.service.healthcheck.HealthCheck;
import org.talend.sdk.component.api.service.healthcheck.HealthCheckStatus;
import org.talend.sdk.component.api.service.schema.DiscoverSchema;
import org.talend.sdk.component.api.service.schema.Schema;
import org.talend.sdk.component.api.service.schema.Type;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.stream.Collectors;

import static java.util.stream.Collectors.toList;
import static org.talend.sdk.component.api.service.healthcheck.HealthCheckStatus.Status.KO;
import static org.talend.sdk.component.api.service.healthcheck.HealthCheckStatus.Status.OK;

@Service
@Slf4j
public class MagentoCmsService {

    @Service
    private Messages i18n;

    @Service
    MagentoHttpServiceFactory httpServiceFactory;

    @DiscoverSchema("guessTableSchema")
    public Schema guessTableSchema(final MagentoCmsInputMapperConfiguration dataSet, final MagentoHttpServiceFactory client)
            throws UnknownAuthenticationTypeException, OAuthExpectationFailedException, OAuthCommunicationException,
            OAuthMessageSignerException, IOException {
        log.debug("guess my schema");
        final MagentoCmsSchemaDiscover source = new MagentoCmsSchemaDiscover(dataSet, client);
        List<String> columns = source.getColumns();
        return new Schema(columns.stream().map(k -> new Schema.Entry(k, Type.STRING)).collect(toList()));
    }

    @Suggestions("SuggestFilterAdvanced")
    public SuggestionValues suggestFilterAdvanced(@Option("filterOperator") final SelectionFilterOperator filterOperator,
            @Option("filterLines") final List<SelectionFilter> filterLines) throws UnsupportedEncodingException {
        ConfigurationFilter filter = new ConfigurationFilter(filterOperator, filterLines, null);
        Map<String, String> allParameters = new TreeMap<>();
        ConfigurationHelper.fillFilterParameters(allParameters, filter, false);
        String allParametersStr = allParameters.entrySet().stream().map(entry -> entry.getKey() + "=" + entry.getValue())
                .collect(Collectors.joining("&"));
        return new SuggestionValues(false, Arrays.asList(new SuggestionValues.Item(allParametersStr, "Copy from basic filter")));
    }

    @HealthCheck("datastoreHealthcheck")
    public HealthCheckStatus validateBasicConnection(@Option final MagentoCmsConfigurationBase datastore) {
        try {
            log.debug("start health check");
            final MagentoCmsHealtChecker source = new MagentoCmsHealtChecker(datastore, httpServiceFactory);
            source.checkHealth();
        } catch (Exception e) {
            return new HealthCheckStatus(KO, i18n.healthCheckFailed(e.getMessage()));
        }
        return new HealthCheckStatus(OK, i18n.healthCheckOk());
    }
}