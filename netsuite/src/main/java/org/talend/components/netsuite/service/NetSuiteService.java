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
package org.talend.components.netsuite.service;

import org.apache.commons.lang3.StringUtils;
import org.talend.components.netsuite.dataset.NetSuiteDataSet;
import org.talend.components.netsuite.datastore.NetSuiteDataStore;
import org.talend.components.netsuite.runtime.NetSuiteDatasetRuntime;
import org.talend.components.netsuite.runtime.NetSuiteEndpoint;
import org.talend.components.netsuite.runtime.client.NetSuiteClientService;
import org.talend.components.netsuite.runtime.v2018_2.client.NetSuiteClientFactoryImpl;
import org.talend.sdk.component.api.record.Schema;
import org.talend.sdk.component.api.service.Service;
import org.talend.sdk.component.api.service.completion.SuggestionValues;
import org.talend.sdk.component.api.service.record.RecordBuilderFactory;

import java.util.Collections;
import java.util.List;

import static java.util.stream.Collectors.toList;

@Service
public class NetSuiteService {

    private NetSuiteDatasetRuntime dataSetRuntime;

    private NetSuiteClientService<?> clientService;

    @Service
    private RecordBuilderFactory recordBuilderFactory;

    @Service
    private Messages i18n;

    public synchronized void connect(NetSuiteDataStore dataStore) {
        NetSuiteEndpoint endpoint = new NetSuiteEndpoint(NetSuiteClientFactoryImpl.getFactory(), i18n, dataStore);
        clientService = endpoint.getClientService();
        dataSetRuntime = new NetSuiteDatasetRuntime(clientService.getMetaDataSource(), recordBuilderFactory);
    }

    List<SuggestionValues.Item> getRecordTypes(NetSuiteDataStore dataStore) {
        if (dataSetRuntime == null) {
            connect(dataStore);
        }
        return dataSetRuntime.getRecordTypes().stream()
                .map(record -> new SuggestionValues.Item(record.getName(), record.getDisplayName())).collect(toList());
    }

    List<SuggestionValues.Item> getSearchTypes(NetSuiteDataSet dataSet) {
        if (StringUtils.isEmpty(dataSet.getRecordType())) {
            return Collections.emptyList();
        }
        if (dataSetRuntime == null) {
            connect(dataSet.getDataStore());
        }
        return dataSetRuntime.getSearchInfo(dataSet.getRecordType()).getFields().stream()
                .map(searchType -> new SuggestionValues.Item(searchType, searchType)).collect(toList());
    }

    List<SuggestionValues.Item> getSearchFieldOperators(NetSuiteDataSet dataSet, String field) {
        if (dataSetRuntime == null) {
            connect(dataSet.getDataStore());
        }
        return dataSetRuntime.getSearchFieldOperators(dataSet.getRecordType(), field).stream()
                .map(searchField -> new SuggestionValues.Item(searchField, searchField)).collect(toList());
    }

    public Schema getSchema(NetSuiteDataSet dataSet, List<String> stringSchema) {
        if (dataSetRuntime == null) {
            connect(dataSet.getDataStore());
        }
        return dataSetRuntime.getSchema(dataSet.getRecordType(), stringSchema);
    }

    public NetSuiteClientService<?> getClientService(NetSuiteDataStore dataStore) {
        if (clientService == null) {
            connect(dataStore);
        }
        return clientService;
    }

}