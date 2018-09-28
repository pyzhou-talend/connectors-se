package org.talend.components.onedrive.service;

import lombok.extern.slf4j.Slf4j;
import org.talend.components.onedrive.common.HealthChecker;
import org.talend.components.onedrive.common.OneDriveDataStore;
import org.talend.components.onedrive.helpers.ConfigurationHelper;
import org.talend.components.onedrive.messages.Messages;
import org.talend.components.onedrive.service.http.OneDriveAuthHttpClientService;
import org.talend.components.onedrive.sources.create.OneDriveCreateConfiguration;
import org.talend.components.onedrive.sources.delete.OneDriveDeleteConfiguration;
import org.talend.components.onedrive.sources.list.OneDriveListConfiguration;
import org.talend.components.onedrive.sources.list.SchemaDiscoverList;
import org.talend.sdk.component.api.configuration.Option;
import org.talend.sdk.component.api.service.Service;
import org.talend.sdk.component.api.service.healthcheck.HealthCheck;
import org.talend.sdk.component.api.service.healthcheck.HealthCheckStatus;
import org.talend.sdk.component.api.service.schema.DiscoverSchema;
import org.talend.sdk.component.api.service.schema.Schema;
import org.talend.sdk.component.api.service.schema.Type;

import java.util.List;

import static java.util.stream.Collectors.toList;
import static org.talend.sdk.component.api.service.healthcheck.HealthCheckStatus.Status.OK;

@Service
@Slf4j
public class OneDriveService {

    @Service
    private Messages i18n;

    @Service
    HealthChecker healthChecker;

    @Service
    SchemaDiscoverList schemaDiscoverInput;

    // @Service
    // private ConfigurationService configurationServiceInput;

    @Service
    private OneDriveAuthHttpClientService oneDriveAuthHttpClientService;

    @DiscoverSchema(ConfigurationHelper.DISCOVER_SCHEMA_LIST_ID)
    public Schema guessTableSchemaList(final OneDriveListConfiguration configuration) {
        ConfigurationHelper.setupServices(oneDriveAuthHttpClientService);
        List<String> columns = schemaDiscoverInput.getColumns();
        return new Schema(columns.stream().map(k -> new Schema.Entry(k, Type.STRING)).collect(toList()));
    }

    @DiscoverSchema(ConfigurationHelper.DISCOVER_SCHEMA_CREATE_ID)
    public Schema guessTableSchemaCreate(final OneDriveCreateConfiguration configuration) {
        ConfigurationHelper.setupServices(oneDriveAuthHttpClientService);
        List<String> columns = schemaDiscoverInput.getColumns();
        return new Schema(columns.stream().map(k -> new Schema.Entry(k, Type.STRING)).collect(toList()));
    }

    @DiscoverSchema(ConfigurationHelper.DISCOVER_SCHEMA_DELETE_ID)
    public Schema guessTableSchemaDelete(final OneDriveDeleteConfiguration configuration) {
        ConfigurationHelper.setupServices(oneDriveAuthHttpClientService);
        List<String> columns = schemaDiscoverInput.getColumns();
        return new Schema(columns.stream().map(k -> new Schema.Entry(k, Type.STRING)).collect(toList()));
    }

    @HealthCheck(ConfigurationHelper.DATA_STORE_HEALTH_CHECK)
    public HealthCheckStatus validateBasicConnection(@Option final OneDriveDataStore datastore) {
        // try {
        // log.debug("start health check");
        // OneDriveConfiguration config = new OneDriveConfiguration();
        // config.setDataStore(datastore);
        // ConfigurationHelper.setupServices(config, configurationServiceInput, oneDriveAuthHttpClientService);
        // healthChecker.checkHealth(datastore);
        // } catch (Exception e) {
        // return new HealthCheckStatus(KO, i18n.healthCheckFailed(e.getMessage()));
        // }
        return new HealthCheckStatus(OK, i18n.healthCheckOk());
    }
}