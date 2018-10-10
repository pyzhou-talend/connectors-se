package org.talend.components.onedrive.service.graphclient;

import com.microsoft.graph.models.extensions.DriveItem;
import lombok.extern.slf4j.Slf4j;
import org.talend.components.onedrive.common.OneDriveDataStore;
import org.talend.components.onedrive.common.UnknownAuthenticationTypeException;
import org.talend.components.onedrive.helpers.AuthorizationHelper;
import org.talend.components.onedrive.service.http.BadCredentialsException;
import org.talend.sdk.component.api.service.Service;

import javax.json.JsonObject;
import javax.json.JsonReaderFactory;
import java.io.IOException;
import java.io.StringReader;
import java.util.concurrent.ConcurrentHashMap;

@Service
@Slf4j
public class GraphClientService {

    @Service
    private JsonReaderFactory jsonReaderFactory = null;

    @Service
    private AuthorizationHelper authorizationHelper = null;

    private ConcurrentHashMap<OneDriveDataStore, GraphClient> graphClients = new ConcurrentHashMap<>();

    public GraphClient getGraphClient(OneDriveDataStore oneDriveDataStore)
            throws IOException, BadCredentialsException, UnknownAuthenticationTypeException {
        long start = System.currentTimeMillis();
        GraphClient graphClient = graphClients.get(oneDriveDataStore);
        synchronized (oneDriveDataStore) {
            if (graphClient == null) {
                graphClient = new GraphClient(oneDriveDataStore, authorizationHelper);
                graphClients.put(oneDriveDataStore, graphClient);
            }
        }
        return graphClient;
    }

    public JsonObject driveItemToJson(DriveItem item) {
        String jsonInString = item.getRawObject().toString();
        JsonObject res = jsonReaderFactory.createReader(new StringReader(jsonInString)).readObject();
        return res;
    }
}
