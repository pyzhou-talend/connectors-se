package org.talend.components.onedrive.input;

import org.talend.components.onedrive.helpers.ConfigurationHelper;
import org.talend.components.onedrive.service.configuration.ConfigurationServiceInput;
import org.talend.components.onedrive.service.http.OneDriveAuthHttpClientService;
import org.talend.components.onedrive.service.http.OneDriveHttpClientService;
import org.talend.sdk.component.api.component.Icon;
import org.talend.sdk.component.api.component.Version;
import org.talend.sdk.component.api.configuration.Option;
import org.talend.sdk.component.api.input.*;
import org.talend.sdk.component.api.meta.Documentation;

import java.io.Serializable;
import java.util.List;

import static java.util.Collections.singletonList;

//
// this class role is to enable the work to be distributed in environments supporting it.
//
@Version(1)
// default version is 1, if some configuration changes happen between 2 versions you can add a migrationHandler
// you can use a custom one using @Icon(value=CUSTOM, custom="filename") and adding
// icons/filename_icon32.png in resources
@Icon(Icon.IconType.BELL)
@PartitionMapper(name = "Input")
@Documentation("Input mapper class")
public class OneDriveInputMapper implements Serializable {

    private final OneDriveInputConfiguration configuration;

    private final OneDriveHttpClientService oneDriveHttpClientService;

    public OneDriveInputMapper(@Option("configuration") final OneDriveInputConfiguration configuration,
            OneDriveHttpClientService oneDriveHttpClientService, OneDriveAuthHttpClientService oneDriveAuthHttpClientService,
            ConfigurationServiceInput configurationServiceInput) {
        this.configuration = configuration;
        this.oneDriveHttpClientService = oneDriveHttpClientService;
        ConfigurationHelper.setupServicesInput(configuration, configurationServiceInput, oneDriveAuthHttpClientService);
    }

    @Assessor
    public long estimateSize() {
        // this method should return the estimation of the dataset size
        // it is recommended to return a byte value
        // if you don't have the exact size you can use a rough estimation
        return 1L;
    }

    @Split
    public List<OneDriveInputMapper> split(@PartitionSize final long bundles) {
        // overall idea here is to split the work related to configuration in bundles of size "bundles"
        //
        // for instance if your estimateSize() returned 1000 and you can run on 10 nodes
        // then the environment can decide to run it concurrently (10 * 100).
        // In this case bundles = 100 and we must try to return 10 OneDriveInputMapper with 1/10 of the overall work each.
        //
        // default implementation returns this which means it doesn't support the work to be split
        return singletonList(this);
    }

    @Emitter(name = "Input")
    public OneDriveInputSource createWorker() {
        // here we create an actual worker,
        // you are free to rework the configuration etc but our default generated implementation
        // propagates the partition mapper entries.
        return new OneDriveInputSource(configuration, oneDriveHttpClientService);
    }
}