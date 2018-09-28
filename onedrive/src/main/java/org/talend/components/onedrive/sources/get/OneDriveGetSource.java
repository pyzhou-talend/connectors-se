package org.talend.components.onedrive.sources.get;

import lombok.extern.slf4j.Slf4j;
import org.talend.components.onedrive.helpers.ConfigurationHelper;
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
import org.talend.sdk.component.api.record.Record;

import javax.json.JsonObject;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

@Slf4j
@Version(1)
// @Icon(Icon.IconType.STAR)
@Icon(value = Icon.IconType.CUSTOM, custom = "onedrive_get")
@Processor(name = "Get")
@Documentation("Data deletion processor")
public class OneDriveGetSource implements Serializable {

    private final OneDriveGetConfiguration configuration;

    private OneDriveHttpClientService oneDriveHttpClientService;

    // private GraphClientService graphClientService;

    private List<JsonObject> batchData = new ArrayList<>();

    public OneDriveGetSource(@Option("configuration") final OneDriveGetConfiguration configuration,
            final OneDriveHttpClientService oneDriveHttpClientService,
            final OneDriveAuthHttpClientService oneDriveAuthHttpClientService
    // GraphClientService graphClientService
    ) {
        this.configuration = configuration;
        this.oneDriveHttpClientService = oneDriveHttpClientService;
        // this.graphClientService = graphClientService;
        ConfigurationHelper.setupServices(oneDriveAuthHttpClientService);
    }

    @ElementListener
    public void onNext(@Input final Record record, final @Output OutputEmitter<Record> success,
            final @Output("reject") OutputEmitter<Reject> reject) {
        processOutputElement(configuration, record, success, reject);
    }

    private void processOutputElement(OneDriveGetConfiguration configuration, final Record record, OutputEmitter<Record> success,
            OutputEmitter<Reject> reject) {
        String itemId = record.getString("id");
        try {
            Record newRecord = oneDriveHttpClientService.getItemData(configuration, itemId);
            success.emit(newRecord);
        } catch (BadCredentialsException e) {
            log.error(e.getMessage());
        } catch (Exception e) {
            log.warn(e.getMessage());
            reject.emit(new Reject(e.getMessage(), record));
        }
    }
}