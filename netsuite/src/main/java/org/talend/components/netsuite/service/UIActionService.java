package org.talend.components.netsuite.service;

import java.util.Collections;
import java.util.List;

import org.apache.commons.lang3.StringUtils;
import org.talend.components.netsuite.datastore.NetsuiteDataStore;
import org.talend.components.netsuite.runtime.NetSuiteEndpoint;
import org.talend.components.netsuite.runtime.client.NetSuiteCredentials;
import org.talend.components.netsuite.runtime.client.NetSuiteVersion;
import org.talend.sdk.component.api.configuration.Option;
import org.talend.sdk.component.api.service.Service;
import org.talend.sdk.component.api.service.completion.SuggestionValues;
import org.talend.sdk.component.api.service.completion.Suggestions;
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
            this.service.connect(NetSuiteEndpoint.createConnectionConfig(dataStore));
        } catch (Exception e) {
            return new HealthCheckStatus(Status.KO, i18n.healthCheckFailed(e.getMessage()));
        }
        return new HealthCheckStatus(Status.OK, i18n.healthCheckOk());
    }

    @Suggestions("loadRecordTypes")
    public SuggestionValues loadRecordType(@Option final String account, @Option final String applicationId,
            @Option final String email, @Option final Boolean enableCustomization, @Option final String endpoint,
            @Option final String password, @Option final Integer role) {
        NetSuiteCredentials credentials = new NetSuiteCredentials(email, password, account, String.valueOf(role), applicationId);
        NetSuiteEndpoint.ConnectionConfig connectionConfig = new NetSuiteEndpoint.ConnectionConfig(endpoint,
                NetSuiteVersion.detectVersion(endpoint), credentials, true);// enableCustomization
        try {
            this.service.connect(connectionConfig);
            List<SuggestionValues.Item> items = service.getRecordTypes();
            return new SuggestionValues(true, items);
        } catch (Exception e) {
            throw new RuntimeException();
        }
    }

    @Suggestions("loadFields")
    public SuggestionValues loadFields(@Option String recordType) {
        return StringUtils.isEmpty(recordType) ? new SuggestionValues(false, Collections.emptyList())
                : new SuggestionValues(true, service.getSearchTypes(recordType));
    }

    @Suggestions("loadOperators")
    public SuggestionValues loadOperators(@Option String recordType) {
        return new SuggestionValues(true, service.getSearchFieldOperators());
    }
}
