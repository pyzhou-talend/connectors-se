package org.talend.components.magentocms.service;

import lombok.extern.slf4j.Slf4j;
import org.talend.components.magentocms.common.MagentoCmsConfigurationBase;
import org.talend.components.magentocms.helpers.ConfigurationHelper;
import org.talend.components.magentocms.input.*;
import org.talend.components.magentocms.messages.Messages;
import org.talend.components.magentocms.service.http.MagentoHttpClientService;
import org.talend.sdk.component.api.configuration.Option;
import org.talend.sdk.component.api.service.Service;
import org.talend.sdk.component.api.service.healthcheck.HealthCheck;
import org.talend.sdk.component.api.service.healthcheck.HealthCheckStatus;
import org.talend.sdk.component.api.service.schema.DiscoverSchema;
import org.talend.sdk.component.api.service.schema.Schema;
import org.talend.sdk.component.api.service.schema.Type;
import org.talend.sdk.component.api.service.update.Update;

import java.io.UnsupportedEncodingException;
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

    // @Service
    // MagentoHttpServiceFactory httpServiceFactory;

    @Service
    MagentoCmsHealthChecker magentoCmsHealthChecker;

    @Service
    MagentoCmsSchemaDiscover magentoCmsSchemaDiscover;

    @Service
    private ConfigurationServiceInput configurationServiceInput;

    @Service
    private MagentoHttpClientService magentoHttpClientService;

    @DiscoverSchema("guessTableSchema")
    public Schema guessTableSchema(final MagentoCmsInputMapperConfiguration configuration) {
        log.debug("guess my schema");
        ConfigurationHelper.setupServicesInput(configuration, configurationServiceInput, magentoHttpClientService);

        // final MagentoCmsSchemaDiscover source = new MagentoCmsSchemaDiscover(dataSet, client);
        List<String> columns = magentoCmsSchemaDiscover.getColumns();
        return new Schema(columns.stream().map(k -> new Schema.Entry(k, Type.STRING)).collect(toList()));
    }

    @Update("updatableFilterAdvanced")
    public FilterAdvancedValueWrapper updatableFilterAdvanced(
            @Option("filterOperator") final SelectionFilterOperator filterOperator,
            @Option("filterLines") final List<SelectionFilter> filterLines) {
        log.debug("suggest advanced filter");
        System.out.println("start update: " + filterOperator + filterLines);
        ConfigurationFilter filter = new ConfigurationFilter(filterOperator, filterLines, null);
        Map<String, String> allParameters = new TreeMap<>();
        try {
            ConfigurationHelper.fillFilterParameters(allParameters, filter, false);
        } catch (UnsupportedEncodingException e) {
            throw new RuntimeException(e);
        }
        String allParametersStr = allParameters.entrySet().stream().map(entry -> entry.getKey() + "=" + entry.getValue())
                .collect(Collectors.joining("&"));
        return new FilterAdvancedValueWrapper(allParametersStr);
    }

    @Update("updatableStr")
    public InnerString updatableStr(@Option("filterOperator") final SelectionType selectionType) {
        log.debug("suggest advanced filter");
        System.out.println("start update: " + selectionType);
        return new InnerString("123");
    }
    // @Suggestions("SuggestFilterAdvanced")
    // public SuggestionValues suggestFilterAdvanced(@Option("filterOperator") final SelectionFilterOperator filterOperator,
    // @Option("filterLines") final List<SelectionFilter> filterLines) throws UnsupportedEncodingException {
    // log.debug("suggest advanced filter");
    // ConfigurationFilter filter = new ConfigurationFilter(filterOperator, filterLines, null);
    // Map<String, String> allParameters = new TreeMap<>();
    // ConfigurationHelper.fillFilterParameters(allParameters, filter, false);
    // String allParametersStr = allParameters.entrySet().stream().map(entry -> entry.getKey() + "=" + entry.getValue())
    // .collect(Collectors.joining("&"));
    // return new SuggestionValues(false, Arrays.asList(new SuggestionValues.Item(allParametersStr, "Copy from basic filter")));
    // }

    @HealthCheck("datastoreHealthcheck")
    public HealthCheckStatus validateBasicConnection(@Option final MagentoCmsConfigurationBase datastore) {
        try {
            log.debug("start health check");
            MagentoCmsInputMapperConfiguration config = new MagentoCmsInputMapperConfiguration();
            config.setMagentoCmsConfigurationBase(datastore);
            ConfigurationHelper.setupServicesInput(config, configurationServiceInput, magentoHttpClientService);
            magentoCmsHealthChecker.checkHealth();
        } catch (Exception e) {
            return new HealthCheckStatus(KO, i18n.healthCheckFailed(e.getMessage()));
        }
        return new HealthCheckStatus(OK, i18n.healthCheckOk());
    }
}