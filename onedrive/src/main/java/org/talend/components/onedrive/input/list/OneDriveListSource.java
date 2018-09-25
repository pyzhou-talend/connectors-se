package org.talend.components.onedrive.input.list;

import com.microsoft.graph.models.extensions.DriveItem;
import org.talend.components.onedrive.common.UnknownAuthenticationTypeException;
import org.talend.components.onedrive.input.list.iterator.DriveItemWrapper;
import org.talend.components.onedrive.service.http.BadCredentialsException;
import org.talend.components.onedrive.service.http.OneDriveHttpClientService;
import org.talend.sdk.component.api.configuration.Option;
import org.talend.sdk.component.api.input.Producer;
import org.talend.sdk.component.api.meta.Documentation;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import javax.json.JsonObject;
import javax.json.JsonReaderFactory;
import java.io.IOException;
import java.io.Serializable;
import java.io.StringReader;
import java.util.Arrays;
import java.util.Iterator;
import java.util.NoSuchElementException;

@Documentation("Input data processing class")
public class OneDriveListSource implements Serializable {

    private final OneDriveListConfiguration configuration;

    private final OneDriveHttpClientService oneDriveHttpClientService;

    private Iterator<DriveItem> inputIterator;

    private JsonReaderFactory jsonReaderFactory;

    public OneDriveListSource(@Option("configuration") final OneDriveListConfiguration configuration,
            final OneDriveHttpClientService oneDriveHttpClientService, JsonReaderFactory jsonReaderFactory) {
        this.configuration = configuration;
        this.oneDriveHttpClientService = oneDriveHttpClientService;
        this.jsonReaderFactory = jsonReaderFactory;
    }

    @PostConstruct
    public void init() throws UnknownAuthenticationTypeException, IOException, BadCredentialsException {
        DriveItem item = oneDriveHttpClientService.getItemByPath(configuration.getObjectPath());
        if (configuration.isRecursively()) {
            DriveItemWrapper itemWrapper = new DriveItemWrapper(oneDriveHttpClientService, item);
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
                String jsonInString = item.getRawObject().toString();
                JsonObject res = jsonReaderFactory.createReader(new StringReader(jsonInString)).readObject();

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