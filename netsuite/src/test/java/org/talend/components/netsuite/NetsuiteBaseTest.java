package org.talend.components.netsuite;

import org.junit.ClassRule;
import org.junit.jupiter.api.BeforeAll;
import org.talend.components.netsuite.datastore.NetsuiteDataStore;
import org.talend.components.netsuite.datastore.NetsuiteDataStore.LoginType;
import org.talend.components.netsuite.service.NetsuiteService;
import org.talend.sdk.component.junit.SimpleComponentRule;
import org.talend.sdk.component.maven.MavenDecrypter;
import org.talend.sdk.component.maven.Server;

public abstract class NetsuiteBaseTest {

    protected static NetsuiteDataStore dataStore;

    protected static NetsuiteService service;

    @ClassRule
    public static final SimpleComponentRule COMPONENT = new SimpleComponentRule("org.talend.components.netsuite");

    @BeforeAll
    public static void setupOnce() {
        final MavenDecrypter decrypter = new MavenDecrypter();
        Server consumer = decrypter.find("netsuite.consumer");
        Server token = decrypter.find("netsuite.token");
        dataStore = new NetsuiteDataStore();
        dataStore.setAccount("1579066");
        dataStore.setEndpoint("https://webservices.na2.netsuite.com/services/NetSuitePort_2018_2");
        dataStore.setLoginType(LoginType.TBA);
        dataStore.setConsumerKey(consumer.getUsername());
        dataStore.setConsumerSecret(consumer.getPassword());
        dataStore.setTokenId(token.getUsername());
        dataStore.setTokenSecret(token.getPassword());
        service = COMPONENT.findService(NetsuiteService.class);
    }

}
