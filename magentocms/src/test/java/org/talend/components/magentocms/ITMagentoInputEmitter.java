package org.talend.components.magentocms;

import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.talend.components.magentocms.common.*;
import org.talend.components.magentocms.helpers.ConfigurationHelper;
import org.talend.components.magentocms.input.*;
import org.talend.components.magentocms.output.MagentoCmsOutputConfiguration;
import org.talend.components.magentocms.service.ConfigurationServiceInput;
import org.talend.components.magentocms.service.ConfigurationServiceOutput;
import org.talend.components.magentocms.service.MagentoCmsService;
import org.talend.components.magentocms.service.http.BadCredentialsException;
import org.talend.components.magentocms.service.http.BadRequestException;
import org.talend.components.magentocms.service.http.MagentoHttpClientService;
import org.talend.sdk.component.api.service.Service;
import org.talend.sdk.component.api.service.healthcheck.HealthCheckStatus;
import org.talend.sdk.component.api.service.schema.Schema;
import org.talend.sdk.component.junit.BaseComponentsHandler;
import org.talend.sdk.component.junit5.Injected;
import org.talend.sdk.component.junit5.WithComponents;
import org.talend.sdk.component.runtime.manager.chain.Job;

import javax.json.JsonBuilderFactory;
import javax.json.JsonObject;
import java.io.IOException;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.*;
import static org.talend.sdk.component.junit.SimpleFactory.configurationByExample;

@Slf4j
@DisplayName("Suite of test for the Magento components")
@WithComponents("org.talend.components.magentocms")
class ITMagentoInputEmitter {

    private static MagentoCmsConfigurationBase dataStore;

    private static MagentoCmsConfigurationBase dataStoreSecure;

    private static MagentoCmsConfigurationBase dataStoreOauth1;

    @Injected
    private BaseComponentsHandler componentsHandler;

    @Service
    private JsonBuilderFactory jsonBuilderFactory;

    @Service
    private MagentoCmsService magentoCmsService;

    @Service
    private ConfigurationServiceInput configurationServiceInput;

    @Service
    private ConfigurationServiceOutput configurationServiceOutput;

    @Service
    private MagentoHttpClientService magentoHttpClientService;

    static String dockerHostAddress;

    static String magentoHttpPort;

    static String magentoHttpPortSecure;

    static String magentoAdminName;

    static String magentoAdminPassword;

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

        AuthenticationLoginPasswordSettings authenticationSettings = new AuthenticationLoginPasswordSettings(magentoAdminName,
                magentoAdminPassword);
        // get this variables from Magento's docker image.
        // http://MAGENTO_URL/admin -> system -> integrations -> TalendTest -> Edit -> Integration Details
        AuthenticationOauth1Settings authenticationOauth1Settings = new AuthenticationOauth1Settings(
                "7fqa5rplt4k9dubdbfea17mf3owyteqh", "cpln0ehi2yh7tg5ho9bvlbyprfi0ukqk", "j24y53g83te2fgye8fe8xondubqej4cl",
                "jxnbv58bc94dfsld1c9k7e6tvcqntrx2");
        dataStore = new MagentoCmsConfigurationBase(getBaseUrl(), RestVersion.V1, AuthenticationType.LOGIN_PASSWORD, null, null,
                authenticationSettings);
        dataStoreSecure = new MagentoCmsConfigurationBase(getBaseUrlSecure(), RestVersion.V1, AuthenticationType.LOGIN_PASSWORD,
                null, null, authenticationSettings);
        dataStoreOauth1 = new MagentoCmsConfigurationBase(getBaseUrl(), RestVersion.V1, AuthenticationType.OAUTH_1,
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
        MagentoCmsInputMapperConfiguration dataSet = new MagentoCmsInputMapperConfiguration();
        dataSet.setMagentoCmsConfigurationBase(dataStoreSecure);
        dataSet.setSelectionType(SelectionType.PRODUCTS);
        List<SelectionFilter> filterList = new ArrayList<>();
        SelectionFilter filter = SelectionFilter.builder().fieldName("sku").fieldNameCondition("eq").value("24-MB01").build();
        // SelectionFilter filter = new SelectionFilter("sku", "eq", "24-MB01");
        filterList.add(filter);
        dataSet.setSelectionFilter(new ConfigurationFilter(SelectionFilterOperator.OR, filterList, null));

        final String config = configurationByExample().forInstance(dataSet).configured().toQueryString();
        Job.components().component("magento-input", "Magento://Input?" + config).component("collector", "test://collector")
                .connections().from("magento-input").to("collector").build().run();
        final List<JsonObject> res = componentsHandler.getCollectedData(JsonObject.class);
        assertEquals(1, res.size());
        assertEquals("Joust Duffle Bag", res.iterator().next().getString("name"));
    }

    @Test
    @DisplayName("Input. Get product by SKU. Non secure")
    void inputComponentProductBySkuNonSecure() {
        log.info("Integration test 'Input. Get product by SKU. Non secure' start ");
        MagentoCmsInputMapperConfiguration dataSet = new MagentoCmsInputMapperConfiguration();
        dataSet.setMagentoCmsConfigurationBase(dataStore);
        dataSet.setSelectionType(SelectionType.PRODUCTS);
        List<SelectionFilter> filterList = new ArrayList<>();
        SelectionFilter filter = SelectionFilter.builder().fieldName("sku").fieldNameCondition("eq").value("24-MB01").build();
        // SelectionFilter filter = new SelectionFilter("sku", "eq", "24-MB01");
        filterList.add(filter);
        dataSet.setSelectionFilter(new ConfigurationFilter(SelectionFilterOperator.OR, filterList, null));

        final String config = configurationByExample().forInstance(dataSet).configured().toQueryString();
        Job.components().component("magento-input", "Magento://Input?" + config).component("collector", "test://collector")
                .connections().from("magento-input").to("collector").build().run();
        final List<JsonObject> res = componentsHandler.getCollectedData(JsonObject.class);
        assertEquals(1, res.size());
        assertEquals("Joust Duffle Bag", res.iterator().next().getString("name"));
    }

    @Test
    @DisplayName("Input. Get product by SKU. Non secure")
    void inputComponentProductBySkuNonSecureOauth1() {
        log.info("Integration test 'Input. Get product by SKU. Non secure' start ");
        MagentoCmsInputMapperConfiguration dataSet = new MagentoCmsInputMapperConfiguration();
        dataSet.setMagentoCmsConfigurationBase(dataStoreOauth1);
        dataSet.setSelectionType(SelectionType.PRODUCTS);
        List<SelectionFilter> filterList = new ArrayList<>();
        SelectionFilter filter = SelectionFilter.builder().fieldName("sku").fieldNameCondition("eq").value("24-MB01").build();
        // SelectionFilter filter = new SelectionFilter("sku", "eq", "24-MB01");
        filterList.add(filter);
        dataSet.setSelectionFilter(new ConfigurationFilter(SelectionFilterOperator.OR, filterList, null));

        final String config = configurationByExample().forInstance(dataSet).configured().toQueryString();
        Job.components().component("magento-input", "Magento://Input?" + config).component("collector", "test://collector")
                .connections().from("magento-input").to("collector").build().run();
        final List<JsonObject> res = componentsHandler.getCollectedData(JsonObject.class);
        assertEquals(1, res.size());
        assertEquals("Joust Duffle Bag", res.iterator().next().getString("name"));
    }

    @Test
    @DisplayName("Input. Bad credentials")
    void inputComponentBadCredentials() {
        log.info("Integration test 'Input. Bad credentials' start");
        AuthenticationLoginPasswordSettings authSettingsBad = new AuthenticationLoginPasswordSettings(magentoAdminName,
                magentoAdminPassword + "_make it bad");
        MagentoCmsConfigurationBase dataStoreBad = new MagentoCmsConfigurationBase(
                "http://" + dockerHostAddress + ":" + magentoHttpPort, RestVersion.V1, AuthenticationType.LOGIN_PASSWORD, null,
                null, authSettingsBad);

        MagentoCmsInputMapperConfiguration dataSet = new MagentoCmsInputMapperConfiguration();
        dataSet.setMagentoCmsConfigurationBase(dataStoreBad);
        dataSet.setSelectionType(SelectionType.PRODUCTS);
        final String config = configurationByExample().forInstance(dataSet).configured().toQueryString();
        try {
            Job.components().component("magento-input", "Magento://Input?" + config).component("collector", "test://collector")
                    .connections().from("magento-input").to("collector").build().run();
        } catch (Exception e) {
            assertTrue(e.getCause() instanceof BadCredentialsException);
        }
    }

    ///////////////////////////////////////

    @ParameterizedTest
    @MethodSource("methodSourceDataStores")
    @DisplayName("Output. Write custom product")
    void outputComponent(MagentoCmsConfigurationBase dataStoreCustom) {
        log.info("Integration test 'Output. Write custom product' start. " + dataStoreCustom);
        MagentoCmsOutputConfiguration dataSet = new MagentoCmsOutputConfiguration();
        dataSet.setMagentoCmsConfigurationBase(dataStoreCustom);
        dataSet.setSelectionType(SelectionType.PRODUCTS);
        final String config = configurationByExample().forInstance(dataSet).configured().toQueryString();

        JsonObject jsonObject = jsonBuilderFactory.createObjectBuilder().add("sku", "24-MB01_" + UUID.randomUUID().toString())
                .add("name", "Joust Duffle Bag_" + UUID.randomUUID().toString()).add("attribute_set_id", 15).add("price", 34)
                .add("status", 1).add("visibility", 4).add("type_id", "simple").add("created_at", "2018-08-01 13:28:05")
                .add("updated_at", "2018-08-01 13:28:05").build();
        componentsHandler.setInputData(Arrays.asList(jsonObject));

        Job.components().component("emitter", "test://emitter").component("magento-output", "Magento://Output?" + config)
                .component("collector", "test://collector").connections().from("emitter").to("magento-output")
                .from("magento-output").to("collector").build().run();
        final List<JsonObject> res = componentsHandler.getCollectedData(JsonObject.class);
        assertEquals(1, res.size());
        assertTrue(res.iterator().next().containsKey("id"));
    }

    private static Stream<Arguments> methodSourceDataStores() {
        return Stream.of(Arguments.of(dataStore), Arguments.of(dataStoreSecure), Arguments.of(dataStoreOauth1));
    }

    ////////////////////////////////////

    @Test
    @DisplayName("Schema discovery")
    void schemaDiscoveryTest() {
        log.info("Integration test 'Schema discovery' start ");
        MagentoCmsInputMapperConfiguration dataSet = new MagentoCmsInputMapperConfiguration();
        dataSet.setMagentoCmsConfigurationBase(dataStoreSecure);
        dataSet.setSelectionType(SelectionType.PRODUCTS);

        Schema schema = magentoCmsService.guessTableSchema(dataSet);
        assertTrue(schema.getEntries().stream().map(item -> item.getName()).collect(Collectors.toList())
                .containsAll(Arrays.asList("id", "sku", "name")));
    }

    @Test
    @DisplayName("Health check")
    void healthCheckTest() {
        log.info("Integration test 'Health Check' start ");
        HealthCheckStatus healthCheckStatus = magentoCmsService.validateBasicConnection(dataStoreSecure);
        assertEquals(HealthCheckStatus.Status.OK, healthCheckStatus.getStatus());
    }

    @Test
    @DisplayName("Input. Bad request")
    void inputBadRequestNoParameters() throws IOException, UnknownAuthenticationTypeException {
        log.info("Integration test 'Input. Bad request' start");
        MagentoCmsInputMapperConfiguration dataSet = new MagentoCmsInputMapperConfiguration();
        dataSet.setMagentoCmsConfigurationBase(dataStore);
        dataSet.setSelectionType(SelectionType.PRODUCTS);
        String magentoUrl = dataSet.getMagentoUrl();

        ConfigurationHelper.setupServicesInput(dataSet, configurationServiceInput, magentoHttpClientService);

        try {
            magentoHttpClientService.getRecords(magentoUrl, new TreeMap<>());
            fail("get records with no filters");
        } catch (BadRequestException e) {
            // right way
        } catch (BadCredentialsException e) {
            fail("get records with no filters");
        }
    }

    @Test
    @DisplayName("Output. Bad request")
    void outputBadRequestNoParameters() throws IOException, UnknownAuthenticationTypeException {
        log.info("Integration test 'Output. Bad request' start");
        MagentoCmsOutputConfiguration dataSet = new MagentoCmsOutputConfiguration();
        dataSet.setMagentoCmsConfigurationBase(dataStore);
        dataSet.setSelectionType(SelectionType.PRODUCTS);
        String magentoUrl = dataSet.getMagentoUrl();

        ConfigurationHelper.setupServicesOutput(dataSet, configurationServiceOutput, magentoHttpClientService);

        try {
            JsonObject dataList = jsonBuilderFactory.createObjectBuilder().add("bad_field", "").build();
            magentoHttpClientService.postRecords(magentoUrl, dataList);
            fail("get records with no filters");
        } catch (BadRequestException e) {
            // right way
        } catch (BadCredentialsException e) {
            fail("get records with no filters");
        }
    }
}
