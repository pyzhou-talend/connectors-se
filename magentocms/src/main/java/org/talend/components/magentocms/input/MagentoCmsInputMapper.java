package org.talend.components.magentocms.input;

import org.talend.components.magentocms.helpers.ConfigurationHelper;
import org.talend.components.magentocms.service.ConfigurationServiceInput;
import org.talend.components.magentocms.service.http.MagentoHttpClientService;
import org.talend.sdk.component.api.component.Icon;
import org.talend.sdk.component.api.component.Version;
import org.talend.sdk.component.api.configuration.Option;
import org.talend.sdk.component.api.input.*;
import org.talend.sdk.component.api.meta.Documentation;

import java.io.Serializable;
import java.util.List;

import static java.util.Collections.singletonList;

@Version(1)
@Icon(value = Icon.IconType.CUSTOM, custom = "magento_input")
@PartitionMapper(name = "Input")
@Documentation("Input mapper class")
public class MagentoCmsInputMapper implements Serializable {

    private final MagentoCmsInputMapperConfiguration configuration;

    private final MagentoHttpClientService magentoHttpClientService;

    public MagentoCmsInputMapper(@Option("configuration") final MagentoCmsInputMapperConfiguration configuration,
            MagentoHttpClientService magentoHttpClientService, ConfigurationServiceInput configurationServiceInput) {
        this.configuration = configuration;
        this.magentoHttpClientService = magentoHttpClientService;
        ConfigurationHelper.setupServicesInput(configuration, configurationServiceInput, magentoHttpClientService);
    }

    @Assessor
    public long estimateSize() {
        return 1L;
    }

    @Split
    public List<MagentoCmsInputMapper> split(@PartitionSize final long bundles) {
        return singletonList(this);
    }

    @Emitter(name = "Input")
    public MagentoCmsInputSource createWorker() {
        return new MagentoCmsInputSource(configuration, magentoHttpClientService);
    }
}