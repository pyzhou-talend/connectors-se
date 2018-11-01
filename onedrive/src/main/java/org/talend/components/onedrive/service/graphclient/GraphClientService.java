package org.talend.components.onedrive.service.graphclient;

import com.microsoft.graph.models.extensions.DriveItem;
import lombok.extern.slf4j.Slf4j;
import org.talend.components.onedrive.common.OneDriveDataStore;
import org.talend.components.onedrive.helpers.AuthorizationHelper;
import org.talend.sdk.component.api.service.Service;

import javax.json.JsonObject;
import javax.json.JsonReaderFactory;
import java.io.StringReader;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

@Service
@Slf4j
public class GraphClientService {

    @Service
    private JsonReaderFactory jsonReaderFactory;

    @Service
    private AuthorizationHelper authorizationHelper;

    private Map<OneDriveDataStore, GraphClient> graphClients = new ConcurrentHashMap<>();

    public GraphClient getGraphClient(OneDriveDataStore oneDriveDataStore) {
        return graphClients.computeIfAbsent(oneDriveDataStore, key -> new GraphClient(oneDriveDataStore, authorizationHelper));
    }

    public JsonObject driveItemToJson(DriveItem item) {
        String jsonInString = item.getRawObject().toString();
        return jsonReaderFactory.createReader(new StringReader(jsonInString)).readObject();
    }
}
