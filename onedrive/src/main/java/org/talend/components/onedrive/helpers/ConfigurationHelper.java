package org.talend.components.onedrive.helpers;

import org.talend.components.onedrive.input.OneDriveInputConfiguration;
import org.talend.components.onedrive.output.OneDriveOutputConfiguration;
import org.talend.components.onedrive.service.configuration.ConfigurationServiceCreate;
import org.talend.components.onedrive.service.configuration.ConfigurationServiceInput;
import org.talend.components.onedrive.service.configuration.ConfigurationServiceList;
import org.talend.components.onedrive.service.configuration.ConfigurationServiceOutput;
import org.talend.components.onedrive.service.http.OneDriveAuthHttpClientService;
import org.talend.components.onedrive.sources.create.OneDriveCreateConfiguration;
import org.talend.components.onedrive.sources.list.OneDriveListConfiguration;

public class ConfigurationHelper {

    public static final String DATA_STORE_ID = "OneDriveDataStore";

    public static final String DATA_SET_GET_ID = "OneDriveDataSetGet";

    public static final String DATA_SET_PUT_ID = "OneDriveDataSetPut";

    public static final String DATA_SET_LIST_ID = "OneDriveDataSetList";

    public static final String DATA_SET_CREATE_ID = "OneDriveDataSetCreate";

    public static final String DATA_SET_DELETE_ID = "OneDriveDataSetDelete";

    public static final String DATA_STORE_HEALTH_CHECK = "DataStoreHealthCheck";

    public static final String DISCOVER_SCHEMA_LIST_ID = "DiscoverSchemaList";

    public static final String DISCOVER_SCHEMA_CREATE_ID = "DiscoverSchemaCreate";

    public static final String DISCOVER_SCHEMA_DELETE_ID = "DiscoverSchemaDelete";

    public static void setupServicesList(OneDriveListConfiguration configuration,
            ConfigurationServiceList configurationServiceList, OneDriveAuthHttpClientService oneDriveHttpClientService) {
        configurationServiceList.setConfiguration(configuration);
        oneDriveHttpClientService.setBase();
    }

    public static void setupServicesCreate(OneDriveCreateConfiguration configuration,
            ConfigurationServiceCreate configurationServiceCreate, OneDriveAuthHttpClientService oneDriveHttpClientService) {
        configurationServiceCreate.setConfiguration(configuration);
        oneDriveHttpClientService.setBase();
    }

    public static void setupServicesInput(OneDriveInputConfiguration configuration,
            ConfigurationServiceInput configurationServiceInput, OneDriveAuthHttpClientService oneDriveHttpClientService) {
        configurationServiceInput.setConfiguration(configuration);
        oneDriveHttpClientService.setBase();
    }

    public static void setupServicesOutput(OneDriveOutputConfiguration configuration,
            ConfigurationServiceOutput configurationService, OneDriveAuthHttpClientService oneDriveHttpClientService) {
        configurationService.setConfiguration(configuration);
        oneDriveHttpClientService.setBase();
    }
}
