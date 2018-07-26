package org.talend.components.netsuite.service;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import org.talend.components.netsuite.runtime.NetSuiteDatasetRuntime;
import org.talend.components.netsuite.runtime.NetSuiteDatasetRuntimeImpl;
import org.talend.components.netsuite.runtime.NetSuiteEndpoint;
import org.talend.components.netsuite.runtime.NetSuiteEndpoint.ConnectionConfig;
import org.talend.components.netsuite.runtime.client.NetSuiteClientService;
import org.talend.components.netsuite.runtime.v2016_2.client.NetSuiteClientFactoryImpl;
import org.talend.sdk.component.api.service.Service;
import org.talend.sdk.component.api.service.completion.SuggestionValues;
import org.talend.sdk.component.api.service.schema.Schema;

@Service
public class NetsuiteService {

    private NetSuiteEndpoint endpoint;

    private NetSuiteDatasetRuntime dataSetRuntime;

    private NetSuiteClientService<?> clientService;

    public void connect(final ConnectionConfig connConfig) {
        if (endpoint == null) {
            endpoint = new NetSuiteEndpoint(NetSuiteClientFactoryImpl.INSTANCE, connConfig);
            clientService = endpoint.getClientService();
        }
        if (!clientService.isLoggedIn()) {
            clientService.login();
        }
        dataSetRuntime = new NetSuiteDatasetRuntimeImpl(clientService.getMetaDataSource());
    }

    List<SuggestionValues.Item> getRecordTypes() {

        return dataSetRuntime == null ? new ArrayList<>() : dataSetRuntime.getRecordTypes();
    }

    List<SuggestionValues.Item> getSearchTypes(String typeName) {
        return dataSetRuntime == null ? new ArrayList<>()
                : dataSetRuntime.getSearchInfo(typeName).getFields().stream()
                        .map(info -> new SuggestionValues.Item(info.getName(), info.getName())).collect(Collectors.toList());
    }

    List<SuggestionValues.Item> getSearchFieldOperators() {
        return dataSetRuntime == null ? new ArrayList<>()
                : dataSetRuntime.getSearchFieldOperators().stream().map(name -> new SuggestionValues.Item(name, name))
                        .collect(Collectors.toList());
    }

    public List<Schema.Entry> getSchema(String typeName) {
        return dataSetRuntime == null ? Collections.emptyList() : dataSetRuntime.getSchema(typeName);
    }

    public org.apache.avro.Schema getAvroSchema(String typeName) {
        return dataSetRuntime.getAvroSchema(typeName);
    }

    public NetSuiteClientService<?> getClientService() {
        return clientService;
    }

}