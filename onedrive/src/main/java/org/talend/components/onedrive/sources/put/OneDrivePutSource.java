package org.talend.components.onedrive.sources.put;

import com.microsoft.graph.models.extensions.DriveItem;
import lombok.extern.slf4j.Slf4j;
import org.talend.components.onedrive.helpers.ConfigurationHelper;
import org.talend.components.onedrive.service.graphclient.GraphClientService;
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
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.Serializable;
import java.nio.file.Path;
import java.nio.file.Paths;
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
            final @Output("reject") OutputEmitter<Reject> reject) throws IOException {
        processOutputElement(record, success, reject);
    }

    private void processOutputElement(final JsonObject record, OutputEmitter<JsonObject> success, OutputEmitter<Reject> reject)
            throws IOException {
        InputStream inputStream = null;
        int fileLength = 0;
        String itemPath;
        try {
            DriveItem newItem = null;
            if (configuration.isLocalSource()) {
                File f = fileIterator.next();
                Path pathAbsolute = Paths.get(f.getCanonicalPath());
                Path pathBase = Paths.get(configuration.getLocalDirectory());
                Path pathRelative = pathBase.relativize(pathAbsolute);
                itemPath = configuration.getDestinationDirectory() + "/" + pathRelative.toString().replace("\\", "/");
                if (f.isFile()) {
                    inputStream = new FileInputStream(f);
                    fileLength = (int) f.length();
                }
                newItem = oneDriveHttpClientService.putItemData(configuration.getDataStore(), itemPath, inputStream, fileLength);
            } else {
                itemPath = record.getString("itemPath");
                String payloadBase64 = record.getString("payload");
                if (payloadBase64 != null) {
                    byte[] payload = Base64.getDecoder().decode(payloadBase64);
                    inputStream = new ByteArrayInputStream(payload);
                    fileLength = payload.length;
                }
                newItem = oneDriveHttpClientService.putItemData(configuration.getDataStore(), itemPath, inputStream, fileLength);
            }
            JsonObject newRecord = graphClientService.driveItemToJson(newItem);
            success.emit(newRecord);
            // } catch (BadCredentialsException e) {
            // log.error(e.getMessage());
        } catch (Exception e) {
            log.warn(e.getMessage());
            reject.emit(new Reject(e.getMessage(), record));
        } finally {
            if (inputStream != null) {
                inputStream.close();
            }
        }
    }
}