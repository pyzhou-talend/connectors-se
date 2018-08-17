package org.talend.components.magentocms;

import lombok.extern.slf4j.Slf4j;
import oauth.signpost.exception.OAuthCommunicationException;
import oauth.signpost.exception.OAuthExpectationFailedException;
import oauth.signpost.exception.OAuthMessageSignerException;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.talend.components.magentocms.common.*;
import org.talend.components.magentocms.input.*;
import org.talend.components.magentocms.output.MagentoCmsOutputConfiguration;
import org.talend.components.magentocms.service.MagentoCmsService;
import org.talend.components.magentocms.service.http.MagentoHttpServiceFactory;
import org.talend.sdk.component.api.service.Service;
import org.talend.sdk.component.api.service.schema.Schema;
import org.talend.sdk.component.junit.BaseComponentsHandler;
import org.talend.sdk.component.junit5.Injected;
import org.talend.sdk.component.junit5.WithComponents;
import org.talend.sdk.component.runtime.manager.chain.Job;

import javax.json.JsonBuilderFactory;
import javax.json.JsonObject;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.talend.sdk.component.junit.SimpleFactory.configurationByExample;

@Slf4j
@DisplayName("Suite of test for the Magento Input component")
@WithComponents("org.talend.components.magentocms")
class ITMagentoInputEmitter {

    private static MagentoCmsConfigurationBase dataStore;

    @Injected
    private BaseComponentsHandler componentsHandler;

    @Service
    private JsonBuilderFactory jsonBuilderFactory;

    @Service
    private MagentoCmsService magentoCmsService;

    @Service
    private MagentoHttpServiceFactory magentoHttpServiceFactory;

    @BeforeAll
    static void init() {
        String dockerHostAddress = System.getProperty("dockerHostAddress");
        String magentoHttpPort = System.getProperty("magentoHttpPort");
        String magentoAdminName = System.getProperty("magentoAdminName");
        String magentoAdminPassword = System.getProperty("magentoAdminPassword");

        if (dockerHostAddress == null || dockerHostAddress.isEmpty()) {
            dockerHostAddress = "local.magento";
        }
        if (magentoHttpPort == null || magentoHttpPort.isEmpty()) {
            magentoHttpPort = "80";
        }
        if (magentoAdminName == null || magentoAdminName.isEmpty()) {
            magentoAdminName = "admin";
        }
        if (magentoAdminPassword == null || magentoAdminPassword.isEmpty()) {
            magentoAdminPassword = "magentorocks1";
        }

        log.info("docker machine: " + dockerHostAddress + ":" + magentoHttpPort);
        log.info("magento admin: " + magentoAdminName + " " + magentoAdminPassword);

        AuthenticationLoginPasswordSettings authenticationSettings = new AuthenticationLoginPasswordSettings(magentoAdminName,
                magentoAdminPassword);
        dataStore = new MagentoCmsConfigurationBase("http://" + dockerHostAddress + ":" + magentoHttpPort, RestVersion.V1,
                AuthenticationType.LOGIN_PASSWORD, null, null, authenticationSettings);
    }

    @Test
    @DisplayName("inputComponentProductBySku")
    void inputComponentProductBySku() {
        log.info("Integration test INPUT start ");
        MagentoCmsInputMapperConfiguration dataSet = new MagentoCmsInputMapperConfiguration();
        dataSet.setMagentoCmsConfigurationBase(dataStore);
        dataSet.setSelectionType(SelectionType.PRODUCTS);
        List<SelectionFilter> filterList = new ArrayList<>();
        SelectionFilter filter = new SelectionFilter("sku", "eq", "24-MB01");
        filterList.add(filter);
        dataSet.setSelectionFilter(new ConfigurationFilter(SelectionFilterOperator.OR, filterList, false, ""));

        final String config = configurationByExample().forInstance(dataSet).configured().toQueryString();
        Job.components().component("magento-input", "MagentoCMS://Input?" + config).component("collector", "test://collector")
                .connections().from("magento-input").to("collector").build().run();
        final List<JsonObject> res = componentsHandler.getCollectedData(JsonObject.class);
        assertEquals(1, res.size());
        assertTrue(res.iterator().next().getString("name").equals("Joust Duffle Bag"));
    }

    @Test
    @DisplayName("outputComponent")
    void outputComponent() {
        log.info("Integration test OUTPUT start ");
        MagentoCmsOutputConfiguration dataSet = new MagentoCmsOutputConfiguration(dataStore, SelectionType.PRODUCTS);
        final String config = configurationByExample().forInstance(dataSet).configured().toQueryString();

        JsonObject jsonObject = jsonBuilderFactory.createObjectBuilder().add("sku", "24-MB01_" + UUID.randomUUID().toString())
                .add("name", "Joust Duffle Bag_" + UUID.randomUUID().toString()).add("attribute_set_id", 15).add("price", 34)
                .add("status", 1).add("visibility", 4).add("type_id", "simple").add("created_at", "2018-08-01 13:28:05")
                .add("updated_at", "2018-08-01 13:28:05").build();
        componentsHandler.setInputData(Arrays.asList(jsonObject));

        Job.components().component("emitter", "test://emitter").component("magento-output", "MagentoCMS://Output?" + config)
                .connections().from("emitter").to("magento-output").build().run();
        assertTrue(true);
    }

    @Test
    @DisplayName("schemaDiscoveryTest")
    void schemaDiscoveryTest() throws UnknownAuthenticationTypeException, OAuthExpectationFailedException,
            OAuthCommunicationException, OAuthMessageSignerException, IOException {
        log.info("Integration test Schema Discovery start ");
        MagentoCmsInputMapperConfiguration dataSet = new MagentoCmsInputMapperConfiguration();
        dataSet.setMagentoCmsConfigurationBase(dataStore);
        dataSet.setSelectionType(SelectionType.PRODUCTS);
        // List<SelectionFilter> filterList = new ArrayList<>();
        // SelectionFilter filter = new SelectionFilter(0, "sku", "eq", "24-MB01");
        // filterList.add(filter);
        // dataSet.setSelectionFilter(filterList);

        Schema schema = magentoCmsService.guessTableSchema(dataSet, magentoHttpServiceFactory);
        assertTrue(schema.getEntries().stream().map(Schema.Entry::getName).collect(Collectors.toList())
                .containsAll(Arrays.asList("id", "sku", "name")));
    }

}
