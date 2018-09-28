package org.talend.components.onedrive.sources.list;

import com.microsoft.graph.models.extensions.DriveItem;
import org.talend.components.onedrive.common.UnknownAuthenticationTypeException;
import org.talend.components.onedrive.service.graphclient.GraphClientService;
import org.talend.components.onedrive.service.http.BadCredentialsException;
import org.talend.components.onedrive.service.http.OneDriveHttpClientService;
import org.talend.components.onedrive.sources.list.iterator.DriveItemWrapper;
import org.talend.sdk.component.api.configuration.Option;
import org.talend.sdk.component.api.input.Producer;
import org.talend.sdk.component.api.meta.Documentation;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import javax.json.JsonObject;
import javax.json.JsonReaderFactory;
import java.io.IOException;
import java.io.Serializable;
import java.util.Arrays;
import java.util.Iterator;
import java.util.NoSuchElementException;

@Documentation("Input data processing class")
public class OneDriveListSource implements Serializable {

    private final OneDriveListConfiguration configuration;

    private final OneDriveHttpClientService oneDriveHttpClientService;

    private Iterator<DriveItem> inputIterator;

    private JsonReaderFactory jsonReaderFactory;

    private GraphClientService graphClientService;

    public OneDriveListSource(@Option("configuration") final OneDriveListConfiguration configuration,
            final OneDriveHttpClientService oneDriveHttpClientService, JsonReaderFactory jsonReaderFactory,
            GraphClientService graphClientService) {
        this.configuration = configuration;
        this.oneDriveHttpClientService = oneDriveHttpClientService;
        this.jsonReaderFactory = jsonReaderFactory;
        this.graphClientService = graphClientService;
    }

    @PostConstruct
    public void init() throws UnknownAuthenticationTypeException, IOException, BadCredentialsException {
        DriveItem item = oneDriveHttpClientService.getItemByPath(configuration.getDataStore(), configuration.getObjectPath());
        if (configuration.isRecursively()) {
            DriveItemWrapper itemWrapper = new DriveItemWrapper(configuration.getDataStore(), oneDriveHttpClientService, item);
            inputIterator = itemWrapper;
        } else {
            inputIterator = Arrays.asList(item).stream().iterator();
        }
    }

    @Producer
    public JsonObject next() {
        if (inputIterator != null) {
            try {
                DriveItem item = inputIterator.next();
                System.out.println("the item is: " + (item == null ? null : item.getRawObject()));
                if (item == null) {
                    return null;
                }
                System.out.println("parent path: " + item.parentReference.path);
                JsonObject res = graphClientService.driveItemToJson(item);

                return res;
            } catch (NoSuchElementException e) {
                // stop processing
            }
        }
        return null;
    }

    @PreDestroy
    public void release() {
    }
}