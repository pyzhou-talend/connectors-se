package org.talend.components.onedrive.input;

import org.talend.components.onedrive.service.http.OneDriveHttpClientService;
import org.talend.sdk.component.api.configuration.Option;
import org.talend.sdk.component.api.input.Producer;
import org.talend.sdk.component.api.meta.Documentation;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import javax.json.JsonObject;
import java.io.IOException;
import java.io.Serializable;

@Documentation("Input data processing class")
public class OneDriveInputSource implements Serializable {

    private final OneDriveInputConfiguration configuration;

    private final OneDriveHttpClientService oneDriveHttpClientService;

    public OneDriveInputSource(@Option("configuration") final OneDriveInputConfiguration configuration,
            final OneDriveHttpClientService oneDriveHttpClientService) {
        this.configuration = configuration;
        this.oneDriveHttpClientService = oneDriveHttpClientService;
    }

    @PostConstruct
    public void init() throws IOException {
        // String magentoUrl = configuration.getMagentoUrl();
        // // magentoUrl += "?" + allParametersStr;
        //
        // inputIterator = new InputIterator(magentoUrl, allParameters, oneDriveHttpClientService,
        // configuration.getDataStore());
    }

    @Producer
    public JsonObject next() {
        // if (inputIterator != null && inputIterator.hasNext()) {
        // JsonValue val = inputIterator.next();
        // return val.asJsonObject();
        // }
        return null;
    }

    @PreDestroy
    public void release() {
    }
}