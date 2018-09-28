package org.talend.components.onedrive.sources.put;

import com.microsoft.graph.models.extensions.DriveItem;
import lombok.extern.slf4j.Slf4j;
import org.talend.components.onedrive.helpers.ConfigurationHelper;
import org.talend.components.onedrive.service.graphclient.GraphClientService;
import org.talend.components.onedrive.service.http.BadCredentialsException;
import org.talend.components.onedrive.service.http.OneDriveAuthHttpClientService;
import org.talend.components.onedrive.service.http.OneDriveHttpClientService;
import org.talend.components.onedrive.sources.Reject;
import org.talend.sdk.component.api.component.Icon;
import org.talend.sdk.component.api.component.Version;
import org.talend.sdk.component.api.configuration.Option;
import org.talend.sdk.component.api.meta.Documentation;
import org.talend.sdk.component.api.processor.ElementListener;
import org.talend.sdk.component.api.processor.Input;
import org.talend.sdk.component.api.processor.Output;
import org.talend.sdk.component.api.processor.OutputEmitter;
import org.talend.sdk.component.api.processor.Processor;

import javax.annotation.PostConstruct;
import javax.json.JsonObject;
import java.io.File;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Iterator;
import java.util.List;

@Slf4j
@Version(1)
@Icon(value = Icon.IconType.CUSTOM, custom = "onedrive_put")
@Processor(name = "Put")
@Documentation("Data put processor")
public class OneDrivePutSource implements Serializable {

    private final OneDrivePutConfiguration configuration;

    private OneDriveHttpClientService oneDriveHttpClientService;

    private GraphClientService graphClientService;

    private List<JsonObject> batchData = new ArrayList<>();

    private Iterator<File> fileIterator = null;

    public OneDrivePutSource(@Option("configuration") final OneDrivePutConfiguration configuration,
                             final OneDriveHttpClientService oneDriveHttpClientService,
                             final OneDriveAuthHttpClientService oneDriveAuthHttpClientService, GraphClientService graphClientService) {
        this.configuration = configuration;
        this.oneDriveHttpClientService = oneDriveHttpClientService;
        this.graphClientService = graphClientService;
        ConfigurationHelper.setupServices(oneDriveAuthHttpClientService);
    }

    @PostConstruct
    public void init() {
        if (configuration.isLocalSource()) {
            // prepare file iterator
        }
    }

    @ElementListener
    public void onNext(@Input final JsonObject record, final @Output OutputEmitter<JsonObject> success,
                       final @Output("reject") OutputEmitter<Reject> reject) {
        processOutputElement(record, success, reject);
    }

    private void processOutputElement(final JsonObject record, OutputEmitter<JsonObject> success, OutputEmitter<Reject> reject) {
        try {
            DriveItem newItem = null;
            if (configuration.isLocalSource()) {
//                newItem = oneDriveHttpClientService.createItem(configuration.getDataStore(), null, OneDriveObjectType.DIRECTORY,
//                        record.getString("objectPath"));
                File f = fileIterator.next();
//                Path pathAbsolute = Paths.get("/var/data/stuff/xyz.dat");
//                Path pathBase = Paths.get("/var/data");
//                Path pathRelative = pathBase.relativize(pathAbsolute);
//                configuration.getDestinationDirectory() + f.
//                newItem = oneDriveHttpClientService.putItem(configuration.getDataStore(), , f);
            } else {
                String itemPath = record.getString("itemPath");
                String payloadBase64 = record.getString("payload");
                byte[] payload = payloadBase64 == null ? null : Base64.getDecoder().decode(payloadBase64);

//                newItem = oneDriveHttpClientService.putItem(configuration.getDataStore(), itemPath, payload);
            }
            JsonObject newRecord = graphClientService.driveItemToJson(newItem);
            success.emit(newRecord);
//        } catch (BadCredentialsException e) {
//            log.error(e.getMessage());
        } catch (Exception e) {
            log.warn(e.getMessage());
            reject.emit(new Reject(e.getMessage(), record));
        }
    }
}