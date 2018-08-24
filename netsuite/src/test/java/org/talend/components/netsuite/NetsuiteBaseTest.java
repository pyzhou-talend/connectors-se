package org.talend.components.netsuite;

import org.junit.ClassRule;
import org.junit.jupiter.api.BeforeAll;
import org.talend.components.netsuite.datastore.NetsuiteDataStore;
import org.talend.components.netsuite.datastore.NetsuiteDataStore.ApiVersion;
import org.talend.components.netsuite.service.NetsuiteService;
import org.talend.sdk.component.junit.SimpleComponentRule;

public abstract class NetsuiteBaseTest {

    protected static NetsuiteDataStore dataStore;

    protected static NetsuiteService service;

    @ClassRule
    public static final SimpleComponentRule COMPONENT = new SimpleComponentRule("org.talend.components.netsuite");

    @BeforeAll
    public static void setupOnce() {
        dataStore = new NetsuiteDataStore();
        dataStore.setAccount(System.getProperty("netsuite.account"));
        dataStore.setRole(Integer.valueOf(System.getProperty("netsuite.roleId")));
        dataStore.setPassword(System.getProperty("netsuite.password"));
        dataStore.setApiVersion(ApiVersion.V2018_2);
        dataStore.setApplicationId(System.getProperty("netsuite.applicationId"));
        dataStore.setEmail(System.getProperty("netsuite.email"));
        dataStore.setEnableCustomization(true);
        dataStore.setEndpoint(System.getProperty("netsuite.endpoint.url"));

        service = COMPONENT.findService(NetsuiteService.class);
    }

}
