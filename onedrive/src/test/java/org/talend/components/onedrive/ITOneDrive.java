package org.talend.components.onedrive;

import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.talend.components.onedrive.common.AuthenticationLoginPasswordSettings;
import org.talend.components.onedrive.common.AuthenticationType;
import org.talend.components.onedrive.common.OneDriveDataStore;
import org.talend.components.onedrive.service.OneDriveService;
import org.talend.components.onedrive.service.http.OneDriveAuthHttpClientService;
import org.talend.components.onedrive.service.http.OneDriveHttpClientService;
import org.talend.sdk.component.api.service.Service;
import org.talend.sdk.component.api.service.healthcheck.HealthCheckStatus;
import org.talend.sdk.component.junit.BaseComponentsHandler;
import org.talend.sdk.component.junit5.Injected;
import org.talend.sdk.component.junit5.WithComponents;

import javax.json.JsonBuilderFactory;

import static org.junit.jupiter.api.Assertions.assertEquals;

@Slf4j
@DisplayName("Suite of test for the Magento components")
@WithComponents("org.talend.components.magentocms")
class ITOneDrive {

    private static OneDriveDataStore dataStoreLoginPassword;

    @Injected
    private BaseComponentsHandler componentsHandler;

    @Service
    private JsonBuilderFactory jsonBuilderFactory;

    @Service
    private OneDriveService oneDriveService;

    // @Service
    // private ConfigurationService configurationService = null;

    // @Service
    // private ConfigurationServiceOutput configurationServiceOutput;

    @Service
    private OneDriveAuthHttpClientService oneDriveAuthHttpClientService;

    @Service
    private OneDriveHttpClientService oneDriveHttpClientService;

    static String dockerHostAddress;

    static String magentoHttpPort;

    static String magentoHttpPortSecure;

    static String magentoAdminName;

    static String magentoAdminPassword;

    private static String tenantId;

    private static String applicationId;

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

        tenantId = "0333ca35-3f21-4f69-abef-c46d541d019d";
        applicationId = "eec80afa-f049-4b69-9004-f06f68962c87";

        log.info("docker machine: " + dockerHostAddress + ":" + magentoHttpPort);
        log.info("docker machine secure: " + dockerHostAddress + ":" + magentoHttpPortSecure);
        log.info("magento admin: " + magentoAdminName + " " + magentoAdminPassword);

        AuthenticationLoginPasswordSettings authenticationSettings = new AuthenticationLoginPasswordSettings(magentoAdminName,
                magentoAdminPassword);
        dataStoreLoginPassword = new OneDriveDataStore(tenantId, applicationId, AuthenticationType.LOGIN_PASSWORD,
                authenticationSettings);
    }

    private static String getBaseUrl() {
        return "http://" + dockerHostAddress + (magentoHttpPort.equals("80") ? "" : ":" + magentoHttpPort);
    }

    private static String getBaseUrlSecure() {
        return "https://" + dockerHostAddress + (magentoHttpPortSecure.equals("443") ? "" : ":" + magentoHttpPortSecure);
    }

    // @Test
    // @DisplayName("Input. Get product by SKU")
    // void inputComponentProductBySku() {
    // log.info("Integration test 'Input. Get product by SKU' start ");
    // OneDriveInputConfiguration dataSet = new OneDriveInputConfiguration();
    // dataSet.setDataStore(dataStoreLoginPassword);
    //
    // final String config = configurationByExample().forInstance(dataSet).configured().toQueryString();
    // Job.components().component("magento-input", "Magento://Input?" + config).component("collector", "test://collector")
    // .connections().from("magento-input").to("collector").build().run();
    // final List<JsonObject> res = componentsHandler.getCollectedData(JsonObject.class);
    // assertEquals(1, res.size());
    // assertEquals("Joust Duffle Bag", res.iterator().next().getString("name"));
    // }
    //
    // @Test
    // @DisplayName("Input. Get product by SKU. Non secure")
    // void inputComponentProductBySkuNonSecure() {
    // log.info("Integration test 'Input. Get product by SKU. Non secure' start ");
    // OneDriveInputConfiguration dataSet = new OneDriveInputConfiguration();
    // dataSet.setDataStore(dataStoreLoginPassword);
    //
    // final String config = configurationByExample().forInstance(dataSet).configured().toQueryString();
    // Job.components().component("magento-input", "Magento://Input?" + config).component("collector", "test://collector")
    // .connections().from("magento-input").to("collector").build().run();
    // final List<JsonObject> res = componentsHandler.getCollectedData(JsonObject.class);
    // assertEquals(1, res.size());
    // assertEquals("Joust Duffle Bag", res.iterator().next().getString("name"));
    // }
    //
    // @Test
    // @DisplayName("Input. Get product by SKU. Non secure")
    // void inputComponentProductBySkuNonSecureOauth1() {
    // log.info("Integration test 'Input. Get product by SKU. Non secure' start ");
    // OneDriveInputConfiguration dataSet = new OneDriveInputConfiguration();
    // dataSet.setDataStore(dataStoreLoginPassword);
    //
    // final String config = configurationByExample().forInstance(dataSet).configured().toQueryString();
    // Job.components().component("magento-input", "Magento://Input?" + config).component("collector", "test://collector")
    // .connections().from("magento-input").to("collector").build().run();
    // final List<JsonObject> res = componentsHandler.getCollectedData(JsonObject.class);
    // assertEquals(1, res.size());
    // assertEquals("Joust Duffle Bag", res.iterator().next().getString("name"));
    // }
    //
    // @Test
    // @DisplayName("Input. Bad credentials")
    // void inputComponentBadCredentials() {
    // log.info("Integration test 'Input. Bad credentials' start");
    // AuthenticationLoginPasswordSettings authSettingsBad = new AuthenticationLoginPasswordSettings(magentoAdminName,
    // magentoAdminPassword + "_make it bad");
    // OneDriveDataStore dataStoreBad = new OneDriveDataStore(tenantId, applicationId, AuthenticationType.LOGIN_PASSWORD,
    // authSettingsBad);
    //
    // OneDriveInputConfiguration dataSet = new OneDriveInputConfiguration();
    // dataSet.setDataStore(dataStoreBad);
    // final String config = configurationByExample().forInstance(dataSet).configured().toQueryString();
    // try {
    // Job.components().component("magento-input", "Magento://Input?" + config).component("collector", "test://collector")
    // .connections().from("magento-input").to("collector").build().run();
    // } catch (Exception e) {
    // assertTrue(e.getCause() instanceof BadCredentialsException);
    // }
    // }
    //
    // ///////////////////////////////////////
    //
    // @ParameterizedTest
    // @MethodSource("methodSourceDataStores")
    // @DisplayName("Output. Write custom product")
    // void outputComponent(OneDriveDataStore dataStoreCustom) {
    // log.info("Integration test 'Output. Write custom product' start. " + dataStoreCustom);
    // OneDriveOutputConfiguration dataSet = new OneDriveOutputConfiguration();
    // dataSet.setDataStore(dataStoreCustom);
    // final String config = configurationByExample().forInstance(dataSet).configured().toQueryString();
    //
    // JsonObject jsonObject = jsonBuilderFactory.createObjectBuilder().add("sku", "24-MB01_" + UUID.randomUUID().toString())
    // .add("name", "Joust Duffle Bag_" + UUID.randomUUID().toString()).add("attribute_set_id", 15).add("price", 34)
    // .add("status", 1).add("visibility", 4).add("type_id", "simple").add("created_at", "2018-08-01 13:28:05")
    // .add("updated_at", "2018-08-01 13:28:05").build();
    // componentsHandler.setInputData(Arrays.asList(jsonObject));
    //
    // Job.components().component("emitter", "test://emitter").component("magento-output", "Magento://Output?" + config)
    // .component("collector", "test://collector").connections().from("emitter").to("magento-output")
    // .from("magento-output").to("collector").build().run();
    // final List<JsonObject> res = componentsHandler.getCollectedData(JsonObject.class);
    // assertEquals(1, res.size());
    // assertTrue(res.iterator().next().containsKey("id"));
    // }
    //
    // private static Stream<Arguments> methodSourceDataStores() {
    // return Stream.of(Arguments.of(dataStoreLoginPassword));
    // }

    ////////////////////////////////////

    // @Test
    // @DisplayName("Schema discovery")
    // void schemaDiscoveryTest() {
    // log.info("Integration test 'Schema discovery' start ");
    // OneDriveInputConfiguration dataSet = new OneDriveInputConfiguration();
    // dataSet.setDataStore(dataStoreLoginPassword);
    //
    // Schema schema = oneDriveService.guessTableSchemaList(dataSet);
    // assertTrue(schema.getEntries().stream().map(Schema.Entry::getName).collect(Collectors.toList())
    // .containsAll(Arrays.asList("id", "sku", "name")));
    // }

    @Test
    @DisplayName("Health check")
    void healthCheckTest() {
        log.info("Integration test 'Health Check' start ");
        HealthCheckStatus healthCheckStatus = oneDriveService.validateBasicConnection(dataStoreLoginPassword);
        assertEquals(HealthCheckStatus.Status.OK, healthCheckStatus.getStatus());
    }

    // @Test
    // @DisplayName("Input. Bad request")
    // void inputBadRequestNoParameters() throws IOException, UnknownAuthenticationTypeException {
    // log.info("Integration test 'Input. Bad request' start");
    // OneDriveInputConfiguration dataSet = new OneDriveInputConfiguration();
    // dataSet.setDataStore(dataStoreLoginPassword);
    // String magentoUrl = dataSet.getMagentoUrl();
    //
    // ConfigurationHelper.setupServices(dataSet, configurationService, oneDriveAuthHttpClientService);
    //
    // try {
    // oneDriveHttpClientService.getRecords(magentoUrl, new TreeMap<>());
    // fail("get records with no filters");
    // } catch (BadRequestException e) {
    // // right way
    // } catch (BadCredentialsException e) {
    // fail("get records with no filters");
    // }
    // }
    //
    // @Test
    // @DisplayName("Output. Bad request")
    // void outputBadRequestNoParameters() throws IOException, UnknownAuthenticationTypeException {
    // log.info("Integration test 'Output. Bad request' start");
    // OneDriveOutputConfiguration dataSet = new OneDriveOutputConfiguration();
    // dataSet.setDataStore(dataStoreLoginPassword);
    // String magentoUrl = dataSet.getMagentoUrl();
    //
    // ConfigurationHelper.setupServices(dataSet, configurationService, oneDriveAuthHttpClientService);
    //
    // try {
    // JsonObject dataList = jsonBuilderFactory.createObjectBuilder().add("bad_field", "").build();
    // oneDriveHttpClientService.postRecords(magentoUrl, dataList);
    // fail("get records with no filters");
    // } catch (BadRequestException e) {
    // // right way
    // } catch (BadCredentialsException e) {
    // fail("get records with no filters");
    // }
    // }
}
