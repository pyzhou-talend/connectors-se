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
package org.talend.components.rest.output;

import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.talend.components.rest.configuration.RequestConfig;
import org.talend.components.rest.service.RestService;
import org.talend.sdk.component.api.component.Icon;
import org.talend.sdk.component.api.component.Version;
import org.talend.sdk.component.api.configuration.Option;
import org.talend.sdk.component.api.meta.Documentation;
import org.talend.sdk.component.api.processor.AfterGroup;
import org.talend.sdk.component.api.processor.BeforeGroup;
import org.talend.sdk.component.api.processor.ElementListener;
import org.talend.sdk.component.api.processor.Input;
import org.talend.sdk.component.api.processor.Output;
import org.talend.sdk.component.api.processor.Processor;
import org.talend.sdk.component.api.record.Record;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import java.io.Serializable;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

@Slf4j
@Getter
@Version
@Processor(name = "Output")
@Icon(value = Icon.IconType.CUSTOM, custom = "Http")
@Documentation("Http REST Output component")
public class RestOutput implements Serializable {

    private final RequestConfig config;

    private final RestService client;

    private transient List<Record> records;

    public RestOutput(@Option("configuration") final RequestConfig config, final RestService client) {
        this.config = config;
        this.client = client;
    }

    @PostConstruct
    public void init() {
    }

    @ElementListener
    public void process(final Record input) {
        Record result = client.execute(config, input);
    }

}
