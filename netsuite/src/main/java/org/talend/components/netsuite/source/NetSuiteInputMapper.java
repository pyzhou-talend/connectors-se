/*
 * Copyright (C) 2006-2019 Talend Inc. - www.talend.com
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */
package org.talend.components.netsuite.source;

import org.talend.components.netsuite.dataset.NetSuiteInputProperties;
import org.talend.components.netsuite.service.Messages;
import org.talend.components.netsuite.service.NetSuiteService;
import org.talend.sdk.component.api.component.Icon;
import org.talend.sdk.component.api.component.Version;
import org.talend.sdk.component.api.configuration.Option;
import org.talend.sdk.component.api.input.Assessor;
import org.talend.sdk.component.api.input.Emitter;
import org.talend.sdk.component.api.input.PartitionMapper;
import org.talend.sdk.component.api.input.PartitionSize;
import org.talend.sdk.component.api.input.Split;
import org.talend.sdk.component.api.meta.Documentation;
import org.talend.sdk.component.api.service.record.RecordBuilderFactory;

import java.io.Serializable;
import java.util.List;

import static java.util.Collections.singletonList;

@Version(1)
@Icon(value = Icon.IconType.CUSTOM, custom = "NetSuiteInput")
@PartitionMapper(name = "Input")
@Documentation("Creates worker emitter with specified configurations")
public class NetSuiteInputMapper implements Serializable {

    private final NetSuiteInputProperties configuration;

    private final NetSuiteService service;

    private final RecordBuilderFactory recordBuilderFactory;

    private final Messages i18n;

    public NetSuiteInputMapper(@Option("configuration") final NetSuiteInputProperties configuration,
            final NetSuiteService service, final RecordBuilderFactory recordBuilderFactory, final Messages i18n) {
        this.configuration = configuration;
        this.service = service;
        this.recordBuilderFactory = recordBuilderFactory;
        this.i18n = i18n;
    }

    @Assessor
    public long estimateSize() {
        return 1L;
    }

    @Split
    public List<NetSuiteInputMapper> split(@PartitionSize final long bundles) {
        return singletonList(this);
    }

    @Emitter
    public NetSuiteInputSource createWorker() {
        return new NetSuiteInputSource(configuration, service, recordBuilderFactory, i18n);
    }
}