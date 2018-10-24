package org.talend.components.netsuite.service;

import java.util.Collections;
import java.util.List;

import org.apache.commons.lang3.StringUtils;
import org.talend.components.netsuite.dataset.NetSuiteCommonDataSet;
import org.talend.components.netsuite.datastore.NetsuiteDataStore;
import org.talend.sdk.component.api.configuration.Option;
import org.talend.sdk.component.api.record.Schema;
import org.talend.sdk.component.api.service.Service;
import org.talend.sdk.component.api.service.completion.SuggestionValues;
import org.talend.sdk.component.api.service.completion.Suggestions;
import org.talend.sdk.component.api.service.configuration.LocalConfiguration;
import org.talend.sdk.component.api.service.healthcheck.HealthCheck;
import org.talend.sdk.component.api.service.healthcheck.HealthCheckStatus;
import org.talend.sdk.component.api.service.healthcheck.HealthCheckStatus.Status;
import org.talend.sdk.component.api.service.schema.DiscoverSchema;

@Service
public class UIActionService {

    public static final String HEALTH_CHECK = "connection.healthcheck";

    @Service
    private NetsuiteService service;

    @Service
    private LocalConfiguration configuration;

    @Service
    private Messages i18n;

    @HealthCheck(HEALTH_CHECK)
    public HealthCheckStatus validateConnection(@Option final NetsuiteDataStore dataStore) {
        try {
            this.service.connect(dataStore);
        } catch (Exception e) {
            return new HealthCheckStatus(Status.KO, i18n.healthCheckFailed(e.getMessage()));
        }
        return new HealthCheckStatus(Status.OK, i18n.healthCheckOk());
    }

    @DiscoverSchema("guessSchema")
    public Schema guessSchema(@Option final NetSuiteCommonDataSet dataSet) {
        return service.getSchema(dataSet);
    }

    @Suggestions("loadRecordTypes")
    public SuggestionValues loadRecordTypes(@Option final NetsuiteDataStore dataStore) {
        try {
            List<SuggestionValues.Item> items = service.getRecordTypes(dataStore);
            return new SuggestionValues(true, items);
        } catch (Exception e) {
            throw new RuntimeException();
        }
    }

    @Suggestions("loadFields")
    public SuggestionValues loadFields(@Option final NetSuiteCommonDataSet dataSet) {
        return StringUtils.isEmpty(dataSet.getRecordType()) ? new SuggestionValues(false, Collections.emptyList())
                : new SuggestionValues(true, service.getSearchTypes(dataSet));
    }

    @Suggestions("loadOperators")
    public SuggestionValues loadOperators(@Option("dataStore") final NetsuiteDataStore dataStore) {
        return new SuggestionValues(true, service.getSearchFieldOperators(dataStore));
    }
}
