package org.talend.components.magentocms.service;

import lombok.extern.slf4j.Slf4j;
import org.talend.components.magentocms.common.MagentoDataStore;
import org.talend.components.magentocms.helpers.ConfigurationHelper;
import org.talend.components.magentocms.input.ConfigurationFilter;
import org.talend.components.magentocms.input.FilterAdvancedValueWrapper;
import org.talend.components.magentocms.input.InnerString;
import org.talend.components.magentocms.common.MagentoCmsHealthChecker;
import org.talend.components.magentocms.input.MagentoInputConfiguration;
import org.talend.components.magentocms.input.MagentoCmsSchemaDiscover;
import org.talend.components.magentocms.input.SelectionFilter;
import org.talend.components.magentocms.input.SelectionFilterOperator;
import org.talend.components.magentocms.input.SelectionType;
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
    private Messages i18n = null;

    // @Service
    // MagentoHttpServiceFactory httpServiceFactory;

    @Service
    private MagentoCmsHealthChecker magentoCmsHealthChecker = null;

    @Service
    private MagentoCmsSchemaDiscover magentoCmsSchemaDiscover = null;

    // @Service
    // private ConfigurationServiceInput configurationServiceInput;

    @Service
    private MagentoHttpClientService magentoHttpClientService = null;

    @DiscoverSchema("guessTableSchema")
    public Schema guessTableSchema(final MagentoInputConfiguration configuration) {
        log.debug("guess my schema");
        ConfigurationHelper.setupServicesInput(configuration, magentoHttpClientService);
        List<String> columns = magentoCmsSchemaDiscover.getColumns(configuration);
        return new Schema(columns.stream().map(k -> new Schema.Entry(k, Type.STRING)).collect(toList()));
    }

    @Update("updatableFilterAdvanced")
    public FilterAdvancedValueWrapper updatableFilterAdvanced(
            @Option("filterOperator") final SelectionFilterOperator filterOperator,
            @Option("filterLines") final List<SelectionFilter> filterLines) {
        log.debug("suggest advanced filter");
        ConfigurationFilter filter = new ConfigurationFilter(filterOperator, filterLines, null);
        Map<String, String> allParameters = new TreeMap<>();
        try {
            ConfigurationHelper.fillFilterParameters(allParameters, filter, false);
        } catch (UnsupportedEncodingException e) {
            throw new RuntimeException(e);
        }
        String allParametersStr = allParameters.entrySet().stream().map(entry -> entry.getKey() + "=" + entry.getValue())
                .collect(Collectors.joining("&"));
        FilterAdvancedValueWrapper filterAdvancedValueWrapper = new FilterAdvancedValueWrapper();
        filterAdvancedValueWrapper.setFilterAdvancedValue(allParametersStr);
        return filterAdvancedValueWrapper;
    }

    @Update("updatableStr")
    public InnerString updatableStr(@Option("filterOperator") final SelectionType selectionType) {
        log.debug("suggest advanced filter");
        System.out.println("start update: " + selectionType);
        return new InnerString();
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
    public HealthCheckStatus validateBasicConnection(@Option final MagentoDataStore datastore) {
        try {
            log.debug("start health check");
            MagentoInputConfiguration config = new MagentoInputConfiguration();
            config.setMagentoDataStore(datastore);
            ConfigurationHelper.setupServicesInput(config, magentoHttpClientService);
            magentoCmsHealthChecker.checkHealth(datastore);
        } catch (Exception e) {
            return new HealthCheckStatus(KO, i18n.healthCheckFailed(e.getMessage()));
        }
        return new HealthCheckStatus(OK, i18n.healthCheckOk());
    }
}