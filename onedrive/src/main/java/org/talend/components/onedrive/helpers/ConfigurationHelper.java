package org.talend.components.onedrive.helpers;

import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import org.talend.components.onedrive.service.http.OneDriveAuthHttpClientService;

@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class ConfigurationHelper {

    public static final String DATA_STORE_ID = "OneDriveDataStore";

    public static final String DATA_SET_GET_ID = "OneDriveDataSetGet";

    public static final String DATA_SET_PUT_ID = "OneDriveDataSetPut";

    public static final String DATA_SET_LIST_ID = "OneDriveDataSetList";

    public static final String DATA_SET_CREATE_ID = "OneDriveDataSetCreate";

    public static final String DATA_SET_DELETE_ID = "OneDriveDataSetDelete";

    public static final String DATA_STORE_HEALTH_CHECK = "DataStoreHealthCheck";

    public static final String DISCOVER_SCHEMA_LIST_ID = "DiscoverSchemaList";

    public static final String DISCOVER_SCHEMA_DELETE_ID = "DiscoverSchemaDelete";

    public static void setupServices(OneDriveAuthHttpClientService oneDriveAuthHttpClientService) {
    }

}
