package org.talend.components.netsuite.service;

import java.util.ArrayList;
import java.util.List;

import org.talend.components.netsuite.dataset.NetsuiteInputDataSet;
import org.talend.components.netsuite.datastore.NetsuiteDataStore;
import org.talend.sdk.component.api.configuration.Option;
import org.talend.sdk.component.api.service.Service;
import org.talend.sdk.component.api.service.completion.DynamicValues;
import org.talend.sdk.component.api.service.completion.SuggestionValues;
import org.talend.sdk.component.api.service.completion.Suggestions;
import org.talend.sdk.component.api.service.completion.Values;
import org.talend.sdk.component.api.service.configuration.LocalConfiguration;
import org.talend.sdk.component.api.service.healthcheck.HealthCheck;
import org.talend.sdk.component.api.service.healthcheck.HealthCheckStatus;
import org.talend.sdk.component.api.service.healthcheck.HealthCheckStatus.Status;

@Service
public class UIActionService {

    @Service
    private NetsuiteService service;

    @Service
    private LocalConfiguration configuration;

    @HealthCheck("connection.healthcheck")
    public HealthCheckStatus validateConnection(@Option final NetsuiteDataStore dataStore, final Messages i18n,
            LocalConfiguration configuration) {
        try {
            this.service.connect(dataStore, configuration);
        } catch (Exception e) {
            return new HealthCheckStatus(Status.KO, i18n.healthCheckFailed(e.getMessage()));
        }
        return new HealthCheckStatus(Status.OK, i18n.healthCheckOk());
    }

    @Suggestions("loadRecordTypes")
    public SuggestionValues loadRecordType(@Option("configuration") final NetsuiteInputDataSet dataSet) {
        List<SuggestionValues.Item> types = new ArrayList<>();
        try {
            this.service.connect(dataSet.getDataStore(), configuration);
        } catch (Exception e) {

        }
        types.add(new SuggestionValues.Item(dataSet.getDataStore().getEmail(), dataSet.getDataStore().getEmail()));
        return new SuggestionValues(true, types);
    }

    @DynamicValues("loadSearch")
    public Values loadSearchTypes() {
        return new Values(null);
    }
}
