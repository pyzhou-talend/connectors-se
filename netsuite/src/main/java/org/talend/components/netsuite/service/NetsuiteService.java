package org.talend.components.netsuite.service;

import java.util.List;
import java.util.stream.Collectors;

import org.talend.components.netsuite.dataset.NetSuiteCommonDataSet;
import org.talend.components.netsuite.datastore.NetsuiteDataStore;
import org.talend.components.netsuite.runtime.NetSuiteDatasetRuntime;
import org.talend.components.netsuite.runtime.NetSuiteDatasetRuntimeImpl;
import org.talend.components.netsuite.runtime.NetSuiteEndpoint;
import org.talend.components.netsuite.runtime.client.NetSuiteClientService;
import org.talend.components.netsuite.runtime.v2018_2.client.NetSuiteClientFactoryImpl;
import org.talend.sdk.component.api.service.Service;
import org.talend.sdk.component.api.service.completion.SuggestionValues;
import org.talend.sdk.component.api.service.schema.Schema;

@Service
public class NetsuiteService {

    private NetSuiteEndpoint endpoint;

    private NetSuiteDatasetRuntime dataSetRuntime;

    private NetSuiteClientService<?> clientService;

    public void connect(NetsuiteDataStore dataStore) {
        endpoint = new NetSuiteEndpoint(NetSuiteClientFactoryImpl.INSTANCE, NetSuiteEndpoint.createConnectionConfig(dataStore));
        clientService = endpoint.getClientService();
        dataSetRuntime = new NetSuiteDatasetRuntimeImpl(clientService.getMetaDataSource());
    }

    List<SuggestionValues.Item> getRecordTypes(NetsuiteDataStore dataStore) {
        if (dataSetRuntime == null) {
            connect(dataStore);
        }
        return dataSetRuntime.getRecordTypes();
    }

    List<SuggestionValues.Item> getSearchTypes(NetSuiteCommonDataSet dataSet) {
        if (dataSetRuntime == null) {
            connect(dataSet.getDataStore());
        }
        return dataSetRuntime.getSearchInfo(dataSet.getRecordType()).getFields().stream()
                .map(info -> new SuggestionValues.Item(info.getName(), info.getName())).collect(Collectors.toList());
    }

    List<SuggestionValues.Item> getSearchFieldOperators(NetsuiteDataStore dataStore) {
        if (dataSetRuntime == null) {
            connect(dataStore);
        }
        return dataSetRuntime.getSearchFieldOperators().stream().map(name -> new SuggestionValues.Item(name, name))
                .collect(Collectors.toList());
    }

    public List<Schema.Entry> getSchema(NetSuiteCommonDataSet dataSet) {
        if (dataSetRuntime == null) {
            connect(dataSet.getDataStore());
        }
        return dataSetRuntime.getSchema(dataSet.getRecordType());
    }

    public org.apache.avro.Schema getAvroSchema(NetSuiteCommonDataSet dataSet) {
        if (dataSetRuntime == null) {
            connect(dataSet.getDataStore());
        }
        return dataSetRuntime.getAvroSchema(dataSet.getRecordType());
    }

    public org.apache.avro.Schema getRejectAvroSchema(NetSuiteCommonDataSet dataSet, org.apache.avro.Schema schema) {
        if (dataSetRuntime == null) {
            connect(dataSet.getDataStore());
        }
        return dataSetRuntime.getSchemaReject(dataSet.getRecordType(), schema);
    }

    public NetSuiteClientService<?> getClientService(NetsuiteDataStore dataStore) {
        if (clientService == null) {
            connect(dataStore);
        }
        return clientService;
    }

}