package org.talend.components.magentocms;

import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.talend.components.magentocms.common.AuthenticationLoginPasswordConfiguration;
import org.talend.components.magentocms.common.AuthenticationOauth1Configuration;
import org.talend.components.magentocms.common.AuthenticationType;
import org.talend.components.magentocms.common.MagentoDataStore;
import org.talend.components.magentocms.common.RestVersion;
import org.talend.components.magentocms.common.UnknownAuthenticationTypeException;
import org.talend.components.magentocms.helpers.ConfigurationHelper;
import org.talend.components.magentocms.input.ConfigurationFilter;
import org.talend.components.magentocms.input.MagentoInputConfiguration;
import org.talend.components.magentocms.input.SelectionFilter;
import org.talend.components.magentocms.input.SelectionFilterOperator;
import org.talend.components.magentocms.input.SelectionType;
import org.talend.components.magentocms.output.MagentoOutputConfiguration;
import org.talend.components.magentocms.service.MagentoCmsService;
import org.talend.components.magentocms.service.http.BadCredentialsException;
import org.talend.components.magentocms.service.http.BadRequestException;
import org.talend.components.magentocms.service.http.MagentoHttpClientService;
import org.talend.sdk.component.api.service.Service;
import org.talend.sdk.component.api.service.healthcheck.HealthCheckStatus;
import org.talend.sdk.component.api.service.schema.Schema;
import org.talend.sdk.component.junit.BaseComponentsHandler;
import org.talend.sdk.component.junit.SimpleFactory;
import org.talend.sdk.component.junit5.Injected;
import org.talend.sdk.component.junit5.WithComponents;
import org.talend.sdk.component.runtime.manager.chain.Job;

import javax.json.JsonBuilderFactory;
import javax.json.JsonObject;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.TreeMap;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@Slf4j
@DisplayName("Suite of test for the Magento components")
@WithComponents("org.talend.components.magentocms")
class ITMagentoInputEmitter {

    private static MagentoDataStore dataStore;

    private static MagentoDataStore dataStoreSecure;

    private static MagentoDataStore dataStoreOauth1;

    @Injected
    private BaseComponentsHandler componentsHandler = null;

    @Service
    private JsonBuilderFactory jsonBuilderFactory = null;

    @Service
    private MagentoCmsService magentoCmsService = null;

    @Service
    private MagentoHttpClientService magentoHttpClientService = null;

    private static String dockerHostAddress;

    private static String magentoHttpPort;

    private static String magentoHttpPortSecure;

    private static String magentoAdminName;

    private static String magentoAdminPassword;

    @BeforeAll
    static void init() {
        dockerHostAddress = System.getProperty("dockerHostAddress");
        magentoHttpPort = System.getProperty("magentoHttpPort");
        magentoHttpPortSecure = System.getProperty("magentoHttpPortSecure");
        magentoAdminName = System.getProperty("magentoAdminName");
        magentoAdminPassword = System.getProperty("magentoAdminPassword");

        if (dockerHostAddress == null || dockerHostAddress.isEmpty()) {
            dockerHostAddress = "local.magento";
        }
        if (magentoHttpPort == null || magentoHttpPort.isEmpty()) {
            magentoHttpPort = "80";
        }
        if (magentoHttpPortSecure == null || magentoHttpPortSecure.isEmpty()) {
            magentoHttpPortSecure = "443";
        }
        if (magentoAdminName == null || magentoAdminName.isEmpty()) {
            magentoAdminName = "admin";
        }
        if (magentoAdminPassword == null || magentoAdminPassword.isEmpty()) {
            magentoAdminPassword = "magentorocks1";
        }

        log.info("docker machine: " + dockerHostAddress + ":" + magentoHttpPort);
        log.info("docker machine secure: " + dockerHostAddress + ":" + magentoHttpPortSecure);
        log.info("magento admin: " + magentoAdminName + " " + magentoAdminPassword);

        AuthenticationLoginPasswordConfiguration authenticationSettings = new AuthenticationLoginPasswordConfiguration(
                magentoAdminName, magentoAdminPassword);
        // get this variables from Magento's docker image.
        // http://MAGENTO_URL/admin -> system -> integrations -> TalendTest -> Edit -> Integration Details
        AuthenticationOauth1Configuration authenticationOauth1Settings = new AuthenticationOauth1Configuration(
                "7fqa5rplt4k9dubdbfea17mf3owyteqh", "cpln0ehi2yh7tg5ho9bvlbyprfi0ukqk", "j24y53g83te2fgye8fe8xondubqej4cl",
                "jxnbv58bc94dfsld1c9k7e6tvcqntrx2");
        dataStore = new MagentoDataStore(getBaseUrl(), RestVersion.V1, AuthenticationType.LOGIN_PASSWORD, null, null,
                authenticationSettings);
        dataStoreSecure = new MagentoDataStore(getBaseUrlSecure(), RestVersion.V1, AuthenticationType.LOGIN_PASSWORD, null, null,
                authenticationSettings);
        dataStoreOauth1 = new MagentoDataStore(getBaseUrl(), RestVersion.V1, AuthenticationType.OAUTH_1,
                authenticationOauth1Settings, null, null);
    }

    private static String getBaseUrl() {
        return "http://" + dockerHostAddress + (magentoHttpPort.equals("80") ? "" : ":" + magentoHttpPort);
    }

    private static String getBaseUrlSecure() {
        return "https://" + dockerHostAddress + (magentoHttpPortSecure.equals("443") ? "" : ":" + magentoHttpPortSecure);
    }

    @Test
    @DisplayName("Input. Get product by SKU")
    void inputComponentProductBySku() {
        log.info("Integration test 'Input. Get product by SKU' start ");
        MagentoInputConfiguration dataSet = new MagentoInputConfiguration();
        dataSet.setMagentoDataStore(dataStoreSecure);
        dataSet.setSelectionType(SelectionType.PRODUCTS);
        List<SelectionFilter> filterList = new ArrayList<>();
        // SelectionFilter filter = SelectionFilter.builder().fieldName("sku").fieldNameCondition("eq").value("24-MB01").build();
        SelectionFilter filter = new SelectionFilter("sku", "eq", "24-MB01");
        filterList.add(filter);
        dataSet.setSelectionFilter(new ConfigurationFilter(SelectionFilterOperator.OR, filterList, null));

        final String config = SimpleFactory.configurationByExample().forInstance(dataSet).configured().toQueryString();
        Job.components().component("magento-input", "Magento://Input?" + config).component("collector", "test://collector")
                .connections().from("magento-input").to("collector").build().run();
        final List<JsonObject> res = componentsHandler.getCollectedData(JsonObject.class);
        Assertions.assertEquals(1, res.size());
        Assertions.assertEquals("Joust Duffle Bag", res.iterator().next().getString("name"));
    }

    @Test
    @DisplayName("Input. Get product by SKU. Non secure")
    void inputComponentProductBySkuNonSecure() {
        log.info("Integration test 'Input. Get product by SKU. Non secure' start ");
        MagentoInputConfiguration dataSet = new MagentoInputConfiguration();
        dataSet.setMagentoDataStore(dataStore);
        dataSet.setSelectionType(SelectionType.PRODUCTS);
        List<SelectionFilter> filterList = new ArrayList<>();
        // SelectionFilter filter = SelectionFilter.builder().fieldName("sku").fieldNameCondition("eq").value("24-MB01").build();
        SelectionFilter filter = new SelectionFilter("sku", "eq", "24-MB01");
        filterList.add(filter);
        dataSet.setSelectionFilter(new ConfigurationFilter(SelectionFilterOperator.OR, filterList, null));

        final String config = SimpleFactory.configurationByExample().forInstance(dataSet).configured().toQueryString();
        Job.components().component("magento-input", "Magento://Input?" + config).component("collector", "test://collector")
                .connections().from("magento-input").to("collector").build().run();
        final List<JsonObject> res = componentsHandler.getCollectedData(JsonObject.class);
        Assertions.assertEquals(1, res.size());
        Assertions.assertEquals("Joust Duffle Bag", res.iterator().next().getString("name"));
    }

    @Test
    @DisplayName("Input. Get product by SKU. Non secure")
    void inputComponentProductBySkuNonSecureOauth1() {
        log.info("Integration test 'Input. Get product by SKU. Non secure' start ");
        MagentoInputConfiguration dataSet = new MagentoInputConfiguration();
        dataSet.setMagentoDataStore(dataStoreOauth1);
        dataSet.setSelectionType(SelectionType.PRODUCTS);
        List<SelectionFilter> filterList = new ArrayList<>();
        // SelectionFilter filter = SelectionFilter.builder().fieldName("sku").fieldNameCondition("eq").value("24-MB01").build();
        SelectionFilter filter = new SelectionFilter("sku", "eq", "24-MB01");
        filterList.add(filter);
        dataSet.setSelectionFilter(new ConfigurationFilter(SelectionFilterOperator.OR, filterList, null));

        final String config = SimpleFactory.configurationByExample().forInstance(dataSet).configured().toQueryString();
        Job.components().component("magento-input", "Magento://Input?" + config).component("collector", "test://collector")
                .connections().from("magento-input").to("collector").build().run();
        final List<JsonObject> res = componentsHandler.getCollectedData(JsonObject.class);
        Assertions.assertEquals(1, res.size());
        Assertions.assertEquals("Joust Duffle Bag", res.iterator().next().getString("name"));
    }

    @Test
    @DisplayName("Input. Bad credentials")
    void inputComponentBadCredentials() {
        log.info("Integration test 'Input. Bad credentials' start");
        AuthenticationLoginPasswordConfiguration authSettingsBad = new AuthenticationLoginPasswordConfiguration(magentoAdminName,
                magentoAdminPassword + "_make it bad");
        MagentoDataStore dataStoreBad = new MagentoDataStore("http://" + dockerHostAddress + ":" + magentoHttpPort,
                RestVersion.V1, AuthenticationType.LOGIN_PASSWORD, null, null, authSettingsBad);

        MagentoInputConfiguration dataSet = new MagentoInputConfiguration();
        dataSet.setMagentoDataStore(dataStoreBad);
        dataSet.setSelectionType(SelectionType.PRODUCTS);
        final String config = SimpleFactory.configurationByExample().forInstance(dataSet).configured().toQueryString();
        try {
            Job.components().component("magento-input", "Magento://Input?" + config).component("collector", "test://collector")
                    .connections().from("magento-input").to("collector").build().run();
        } catch (Exception e) {
            Assertions.assertTrue(e.getCause() instanceof BadCredentialsException);
        }
    }

    ///////////////////////////////////////

    @ParameterizedTest
    @MethodSource("methodSourceDataStores")
    @DisplayName("Output. Write custom product")
    void outputComponent(MagentoDataStore dataStoreCustom) {
        log.info("Integration test 'Output. Write custom product' start. " + dataStoreCustom);
        MagentoOutputConfiguration dataSet = new MagentoOutputConfiguration();
        dataSet.setMagentoDataStore(dataStoreCustom);
        dataSet.setSelectionType(SelectionType.PRODUCTS);
        final String config = SimpleFactory.configurationByExample().forInstance(dataSet).configured().toQueryString();

        JsonObject jsonObject = jsonBuilderFactory.createObjectBuilder().add("sku", "24-MB01_" + UUID.randomUUID().toString())
                .add("name", "Joust Duffle Bag_" + UUID.randomUUID().toString()).add("attribute_set_id", 15).add("price", 34)
                .add("status", 1).add("visibility", 4).add("type_id", "simple").add("created_at", "2018-08-01 13:28:05")
                .add("updated_at", "2018-08-01 13:28:05").build();
        componentsHandler.setInputData(Arrays.asList(jsonObject));

        Job.components().component("emitter", "test://emitter").component("magento-output", "Magento://Output?" + config)
                .component("collector", "test://collector").connections().from("emitter").to("magento-output")
                .from("magento-output").to("collector").build().run();
        final List<JsonObject> res = componentsHandler.getCollectedData(JsonObject.class);
        Assertions.assertEquals(1, res.size());
        Assertions.assertTrue(res.iterator().next().containsKey("id"));
    }

    private static Stream<Arguments> methodSourceDataStores() {
        return Stream.of(Arguments.of(dataStore), Arguments.of(dataStoreSecure), Arguments.of(dataStoreOauth1));
    }

    ////////////////////////////////////

    @Test
    @DisplayName("Schema discovery")
    void schemaDiscoveryTest() {
        log.info("Integration test 'Schema discovery' start ");
        MagentoInputConfiguration dataSet = new MagentoInputConfiguration();
        dataSet.setMagentoDataStore(dataStoreSecure);
        dataSet.setSelectionType(SelectionType.PRODUCTS);

        Schema schema = magentoCmsService.guessTableSchema(dataSet);
        Assertions.assertTrue(schema.getEntries().stream().map(item -> item.getName()).collect(Collectors.toList())
                .containsAll(Arrays.asList("id", "sku", "name")));
    }

    @Test
    @DisplayName("Health check")
    void healthCheckTest() {
        log.info("Integration test 'Health Check' start ");
        HealthCheckStatus healthCheckStatus = magentoCmsService.validateBasicConnection(dataStoreSecure);
        Assertions.assertEquals(HealthCheckStatus.Status.OK, healthCheckStatus.getStatus());
    }

    @Test
    @DisplayName("Input. Bad request")
    void inputBadRequestNoParameters() throws IOException, UnknownAuthenticationTypeException {
        log.info("Integration test 'Input. Bad request' start");
        MagentoInputConfiguration dataSet = new MagentoInputConfiguration();
        dataSet.setMagentoDataStore(dataStore);
        dataSet.setSelectionType(SelectionType.PRODUCTS);

        ConfigurationHelper.setupServicesInput(dataSet, magentoHttpClientService);

        try {
            magentoHttpClientService.getRecords(dataSet.getMagentoDataStore(), dataSet.getMagentoUrl(), new TreeMap<>());
            Assertions.fail("get records with no filters");
        } catch (BadRequestException e) {
            // right way
        } catch (BadCredentialsException e) {
            Assertions.fail("get records with no filters");
        }
    }

    @Test
    @DisplayName("Output. Bad request")
    void outputBadRequestNoParameters() throws IOException, UnknownAuthenticationTypeException {
        log.info("Integration test 'Output. Bad request' start");
        MagentoOutputConfiguration dataSet = new MagentoOutputConfiguration();
        dataSet.setMagentoDataStore(dataStore);
        dataSet.setSelectionType(SelectionType.PRODUCTS);

        ConfigurationHelper.setupServicesOutput(dataSet, magentoHttpClientService);

        try {
            JsonObject dataList = jsonBuilderFactory.createObjectBuilder().add("bad_field", "").build();
            magentoHttpClientService.postRecords(dataSet.getMagentoDataStore(), dataSet.getMagentoUrl(), dataList);
            Assertions.fail("get records with no filters");
        } catch (BadRequestException e) {
            // right way
        } catch (BadCredentialsException e) {
            Assertions.fail("get records with no filters");
        }
    }
}
