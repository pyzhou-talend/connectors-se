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

import javax.json.JsonObject;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.Serializable;
import java.util.Base64;

@Slf4j
@Version(1)
@Icon(value = Icon.IconType.CUSTOM, custom = "onedrive_put")
@Processor(name = "Put")
@Documentation("Data put processor")
public class OneDrivePutSource implements Serializable {

    private final OneDrivePutConfiguration configuration;

    private OneDriveHttpClientService oneDriveHttpClientService;

    private GraphClientService graphClientService;

    public OneDrivePutSource(@Option("configuration") final OneDrivePutConfiguration configuration,
            final OneDriveHttpClientService oneDriveHttpClientService,
            final OneDriveAuthHttpClientService oneDriveAuthHttpClientService, GraphClientService graphClientService) {
        this.configuration = configuration;
        this.oneDriveHttpClientService = oneDriveHttpClientService;
        this.graphClientService = graphClientService;
        ConfigurationHelper.setupServices(oneDriveAuthHttpClientService);
    }

    @ElementListener
    public void onNext(@Input final JsonObject record, final @Output OutputEmitter<JsonObject> success,
            final @Output("reject") OutputEmitter<Reject> reject) throws IOException {
        processOutputElement(record, success, reject);
    }

    private void processOutputElement(final JsonObject record, OutputEmitter<JsonObject> success, OutputEmitter<Reject> reject)
            throws IOException {
        log.debug("processOutputElement_local: ");

        InputStream inputStream = null;
        int fileLength = 0;
        String itemPath;
        try {
            DriveItem newItem;
            if (configuration.isLocalSource()) {
                itemPath = record.getString("itemPath");
                String localPath = record.getString("localPath", null);
                if (localPath != null && !localPath.isEmpty()) {
                    File f = new File(localPath);
                    if (f.isFile()) {
                        inputStream = new FileInputStream(f);
                        fileLength = (int) f.length();
                    }
                }
                log.debug("processOutputElement_local: " + itemPath + " : " + fileLength);
                newItem = oneDriveHttpClientService.putItemData(configuration.getDataStore(), itemPath, inputStream, fileLength);
            } else {
                itemPath = record.getString("itemPath");
                String payloadBase64 = record.getString("payload", null);
                if (payloadBase64 != null) {
                    byte[] payload = Base64.getDecoder().decode(payloadBase64);
                    inputStream = new ByteArrayInputStream(payload);
                    fileLength = payload.length;
                }
                newItem = oneDriveHttpClientService.putItemData(configuration.getDataStore(), itemPath, inputStream, fileLength);
            }

            JsonObject newRecord = graphClientService.driveItemToJson(newItem);
            success.emit(newRecord);
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