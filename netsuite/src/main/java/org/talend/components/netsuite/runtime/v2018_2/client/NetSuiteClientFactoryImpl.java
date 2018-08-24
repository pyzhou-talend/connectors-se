package org.talend.components.netsuite.runtime.v2018_2.client;

import org.talend.components.netsuite.runtime.client.NetSuiteClientFactory;
import org.talend.components.netsuite.runtime.client.NetSuiteClientService;
import org.talend.components.netsuite.runtime.client.NetSuiteException;
import org.talend.components.netsuite.runtime.client.NetSuiteVersion;

import com.netsuite.webservices.v2018_2.platform.NetSuitePortType;

/**
 *
 */
public class NetSuiteClientFactoryImpl implements NetSuiteClientFactory<NetSuitePortType> {

    public static final NetSuiteClientFactoryImpl INSTANCE = new NetSuiteClientFactoryImpl();

    @Override
    public NetSuiteClientService<NetSuitePortType> createClient() throws NetSuiteException {
        return new NetSuiteClientServiceImpl();
    }

    @Override
    public NetSuiteVersion getApiVersion() {
        return new NetSuiteVersion(2018, 1);
    }
}
