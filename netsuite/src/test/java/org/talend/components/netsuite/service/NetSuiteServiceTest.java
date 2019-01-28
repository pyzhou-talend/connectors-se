package org.talend.components.netsuite.service;

import static org.junit.jupiter.api.Assertions.assertThrows;

import org.junit.ClassRule;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.talend.components.netsuite.datastore.NetSuiteDataStore;
import org.talend.components.netsuite.datastore.NetSuiteDataStore.ApiVersion;
import org.talend.components.netsuite.datastore.NetSuiteDataStore.LoginType;
import org.talend.components.netsuite.runtime.client.NetSuiteException;
import org.talend.sdk.component.junit.SimpleComponentRule;
import org.talend.sdk.component.junit5.WithComponents;
import org.talend.sdk.component.maven.MavenDecrypter;
import org.talend.sdk.component.maven.Server;

@WithComponents("org.talend.components.netsuite")
public class NetSuiteServiceTest {

    private NetSuiteService service;

    @ClassRule
    public static final SimpleComponentRule COMPONENT = new SimpleComponentRule("org.talend.components.netsuite");

    private NetSuiteDataStore dataStore;

    @BeforeEach
    public void setUp() {
        service = COMPONENT.findService(NetSuiteService.class);
        COMPONENT.injectServices(service);
        dataStore = new NetSuiteDataStore();
    }

    @Test
    public void testConnectFailedMissingUserCredentials() {
        // Missing endpoint
        assertThrows(NetSuiteException.class, () -> service.connect(dataStore));
        dataStore.setEndpoint(System.getProperty("netsuite.endpoint.url"));

        // Missing account
        assertThrows(NetSuiteException.class, () -> service.connect(dataStore));

        // Missing email
        dataStore.setAccount(System.getProperty("netsuite.account"));
        dataStore.setLoginType(LoginType.BASIC);
        assertThrows(NetSuiteException.class, () -> service.connect(dataStore));

        // Missing password
        dataStore.setEmail(System.getProperty("netsuite.email"));
        assertThrows(NetSuiteException.class, () -> service.connect(dataStore));

        // Missing roleId
        dataStore.setPassword(System.getProperty("netsuite.password"));
        assertThrows(NetSuiteException.class, () -> service.connect(dataStore));

    }

    @Test
    public void testConnectFailedMissingTokenBasedCredentials() {
        // Missing endpoint
        assertThrows(NetSuiteException.class, () -> service.connect(dataStore));
        dataStore.setEndpoint("https://webservices.netsuite.com/services/NetSuitePort_2016_2");

        // Missing account
        assertThrows(NetSuiteException.class, () -> service.connect(dataStore));

        // Missing consumer key
        dataStore.setAccount(System.getProperty("netsuite.account"));
        dataStore.setLoginType(LoginType.TBA);
        assertThrows(NetSuiteException.class, () -> service.connect(dataStore));

        final MavenDecrypter decrypter = new MavenDecrypter();
        Server consumer = decrypter.find("netsuite.consumer");
        Server token = decrypter.find("netsuite.token");
        // Missing consumer secret
        dataStore.setConsumerKey(consumer.getUsername());
        assertThrows(NetSuiteException.class, () -> service.connect(dataStore));

        // Missing missing token id
        dataStore.setConsumerSecret(consumer.getPassword());
        assertThrows(NetSuiteException.class, () -> service.connect(dataStore));

        // Missing missing token secret
        dataStore.setTokenId(token.getUsername());
        assertThrows(NetSuiteException.class, () -> service.connect(dataStore));

        // Api version is different from endpoint.
        dataStore.setApiVersion(ApiVersion.V2018_2);
        dataStore.setTokenSecret(token.getPassword());
        assertThrows(NetSuiteException.class, () -> service.connect(dataStore));
    }
}
