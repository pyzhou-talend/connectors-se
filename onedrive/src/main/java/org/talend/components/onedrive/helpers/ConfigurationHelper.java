package org.talend.components.onedrive.helpers;

import org.talend.components.onedrive.service.configuration.ConfigurationService;
import org.talend.components.onedrive.service.configuration.OneDriveConfiguration;
import org.talend.components.onedrive.service.http.OneDriveAuthHttpClientService;

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

    // public static void setupServicesList(OneDriveListConfiguration configuration, ConfigurationService
    // configurationServiceList,
    // OneDriveAuthHttpClientService oneDriveAuthHttpClientService) {
    // configurationServiceList.setConfiguration(configuration);
    // oneDriveAuthHttpClientService.setBase();
    // }

    public static void setupServices(OneDriveConfiguration configuration, ConfigurationService configurationService,
            OneDriveAuthHttpClientService oneDriveAuthHttpClientService) {
        configurationService.setConfiguration(configuration);
        oneDriveAuthHttpClientService.setBase();
        System.out.println("__set config: " + configuration);
    }

    // public static void setupServicesDelete(OneDriveDeleteConfiguration configuration,
    // ConfigurationServiceDelete configurationServiceDelete, OneDriveAuthHttpClientService oneDriveAuthHttpClientService) {
    // configurationServiceDelete.setConfiguration(configuration);
    // oneDriveAuthHttpClientService.setBase();
    // }

}
