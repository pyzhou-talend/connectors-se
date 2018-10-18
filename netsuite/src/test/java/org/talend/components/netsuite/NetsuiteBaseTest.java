package org.talend.components.netsuite;

import org.junit.ClassRule;
import org.talend.components.netsuite.dataset.NetSuiteCommonDataSet;
import org.talend.components.netsuite.datastore.NetsuiteDataStore;
import org.talend.components.netsuite.datastore.NetsuiteDataStore.LoginType;
import org.talend.components.netsuite.service.Messages;
import org.talend.components.netsuite.service.NetsuiteService;
import org.talend.sdk.component.api.service.record.RecordBuilderFactory;
import org.talend.sdk.component.junit.SimpleComponentRule;
import org.talend.sdk.component.maven.MavenDecrypter;
import org.talend.sdk.component.maven.Server;

public abstract class NetsuiteBaseTest {

    protected static NetsuiteDataStore dataStore;

    protected NetSuiteCommonDataSet commonDataSet;

    protected static NetsuiteService service;

    protected static Messages messages;

    protected static RecordBuilderFactory factory;

    @ClassRule
    public static final SimpleComponentRule COMPONENT = new SimpleComponentRule("org.talend.components.netsuite");

    // @BeforeAll
    public static void setupOnce() {
        final MavenDecrypter decrypter = new MavenDecrypter();
        Server consumer = decrypter.find("netsuite.consumer");
        Server token = decrypter.find("netsuite.token");
        dataStore = new NetsuiteDataStore();
        dataStore.setEnableCustomization(false);
        dataStore.setAccount(System.getProperty("netsuite.account"));
        dataStore.setEndpoint(System.getProperty("netsuite.endpoint.url"));
        dataStore.setLoginType(LoginType.TBA);
        dataStore.setConsumerKey(consumer.getUsername());
        dataStore.setConsumerSecret(consumer.getPassword());
        dataStore.setTokenId(token.getUsername());
        dataStore.setTokenSecret(token.getPassword());
        service = COMPONENT.findService(NetsuiteService.class);
        messages = COMPONENT.findService(Messages.class);
        factory = COMPONENT.findService(RecordBuilderFactory.class);
    }

}
