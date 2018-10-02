package org.talend.components.onedrive;

import com.microsoft.graph.models.extensions.DriveItem;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.talend.components.onedrive.common.AuthenticationLoginPasswordSettings;
import org.talend.components.onedrive.common.AuthenticationType;
import org.talend.components.onedrive.common.OneDriveDataStore;
import org.talend.components.onedrive.common.UnknownAuthenticationTypeException;
import org.talend.components.onedrive.helpers.ConfigurationHelper;
import org.talend.components.onedrive.service.OneDriveService;
import org.talend.components.onedrive.service.http.BadCredentialsException;
import org.talend.components.onedrive.service.http.OneDriveAuthHttpClientService;
import org.talend.components.onedrive.service.http.OneDriveHttpClientService;
import org.talend.components.onedrive.sources.create.OneDriveCreateConfiguration;
import org.talend.components.onedrive.sources.delete.OneDriveDeleteConfiguration;
import org.talend.components.onedrive.sources.list.OneDriveListConfiguration;
import org.talend.components.onedrive.sources.list.OneDriveObjectType;
import org.talend.sdk.component.api.service.Service;
import org.talend.sdk.component.junit.BaseComponentsHandler;
import org.talend.sdk.component.junit5.Injected;
import org.talend.sdk.component.junit5.WithComponents;
import org.talend.sdk.component.runtime.manager.chain.Job;

import javax.json.JsonBuilderFactory;
import javax.json.JsonObject;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.talend.sdk.component.junit.SimpleFactory.configurationByExample;

@Slf4j
@DisplayName("Suite of test for the OneDrive components")
@WithComponents("org.talend.components.onedrive")
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

    private static String oneDriveAdminName;

    private static String oneDriveAdminPassword;

    private static String tenantId;

    private static String applicationId;

    @BeforeAll
    static void init() {
        tenantId = "0333ca35-3f21-4f69-abef-c46d541d019d";
        applicationId = "eec80afa-f049-4b69-9004-f06f68962c87";

        oneDriveAdminName = "sbovsunovskyi@talend.com";
        oneDriveAdminPassword = "";

        AuthenticationLoginPasswordSettings authenticationSettings = new AuthenticationLoginPasswordSettings(oneDriveAdminName,
                oneDriveAdminPassword);
        dataStoreLoginPassword = new OneDriveDataStore(tenantId, applicationId, AuthenticationType.LOGIN_PASSWORD,
                authenticationSettings);
    }

    @Test
    @DisplayName("List. Get files in directory")
    void listComponentGetFilesInDirectory() {
        log.info("Integration test 'List. Get files in directory' start ");
        OneDriveListConfiguration dataSet = new OneDriveListConfiguration();
        dataSet.setDataStore(dataStoreLoginPassword);
        dataSet.setObjectPath("/fold1");
        dataSet.setObjectType(OneDriveObjectType.DIRECTORY);
        dataSet.setRecursively(true);

        final String config = configurationByExample().forInstance(dataSet).configured().toQueryString();
        Job.components().component("onedrive-list", "OneDrive://List?" + config).component("collector", "test://collector")
                .connections().from("onedrive-list").to("collector").build().run();
        final List<JsonObject> res = componentsHandler.getCollectedData(JsonObject.class);
        Assertions.assertEquals(2, res.size());
        Assertions.assertTrue("doc1.txt,doc2.txt".contains(res.get(0).getString("name")));
        Assertions.assertTrue("doc1.txt,doc2.txt".contains(res.get(1).getString("name")));
    }

    // @Test
    // @DisplayName("List. Get files in root")
    // void listComponentGetFilesInRoot() {
    // log.info("Integration test 'List. Get files in root' start ");
    // OneDriveListConfiguration dataSet = new OneDriveListConfiguration();
    // dataSet.setDataStore(dataStoreLoginPassword);
    // dataSet.setObjectPath("");
    // dataSet.setObjectType(OneDriveObjectType.DIRECTORY);
    // dataSet.setRecursively(true);
    //
    // final String config = configurationByExample().forInstance(dataSet).configured().toQueryString();
    // Job.components().component("onedrive-list", "OneDrive://List?" + config).component("collector", "test://collector")
    // .connections().from("onedrive-list").to("collector").build().run();
    // final List<JsonObject> res = componentsHandler.getCollectedData(JsonObject.class);
    //// Assertions.assertEquals(2, res.size());
    // Assertions.assertTrue(res.stream().map(item->item.getString("name")).collect(Collectors.toList()).contains("testFileInRoot.txt"));
    // }

    @Test
    @DisplayName("List. Get root")
    void listComponentGetRoot() {
        log.info("Integration test 'List. Get root' start ");
        OneDriveListConfiguration dataSet = new OneDriveListConfiguration();
        dataSet.setDataStore(dataStoreLoginPassword);

        final String config = configurationByExample().forInstance(dataSet).configured().toQueryString();
        Job.components().component("onedrive-list", "OneDrive://List?" + config).component("collector", "test://collector")
                .connections().from("onedrive-list").to("collector").build().run();
        final List<JsonObject> res = componentsHandler.getCollectedData(JsonObject.class);
        Assertions.assertEquals(1, res.size());
        Assertions.assertEquals("root", res.iterator().next().getString("name"));
    }

    @Test
    @DisplayName("Create. Create folder")
    void createComponentCreateFolder() throws IOException, BadCredentialsException, UnknownAuthenticationTypeException {
        log.info("Integration test 'Create. Create folder' start.");
        // create config
        OneDriveCreateConfiguration dataSetCreate = new OneDriveCreateConfiguration();
        dataSetCreate.setDataStore(dataStoreLoginPassword);
        dataSetCreate.setCreateDirectoriesByList(false);
        dataSetCreate.setObjectType(OneDriveObjectType.FILE);
        dataSetCreate.setObjectPath("newDir3/newdir4/newFile.txt");
        final String configCreate = configurationByExample().forInstance(dataSetCreate).configured().toQueryString();

        ConfigurationHelper.setupServices(oneDriveAuthHttpClientService);
        DriveItem root = oneDriveHttpClientService.getRoot(dataStoreLoginPassword);
        String parentId = root.id;
        JsonObject jsonObject = jsonBuilderFactory.createObjectBuilder().add("parentId", parentId).build();
        componentsHandler.setInputData(Arrays.asList(jsonObject));

        Job.components().component("emitter", "test://emitter")
                .component("onedrive-create", "OneDrive://Create?" + configCreate)
                .component("collector", "test://collector").connections()
                .from("emitter").to("onedrive-create")
                .from("onedrive-create").to("collector").build().run();
        final List<JsonObject> res = componentsHandler.getCollectedData(JsonObject.class);
        Assertions.assertEquals(1, res.size());
        JsonObject newItem = res.get(0);
        Assertions.assertTrue(newItem.containsKey("file"));
        Assertions.assertEquals("newFile.txt", newItem.getString("name"));
        Assertions.assertEquals("/drive/root:/newDir3/newdir4", newItem.getJsonObject("parentReference").getString("path"));
    }

    @Test
    @DisplayName("Create. Create folder list")
    void createComponentCreateFolderList() throws IOException, BadCredentialsException, UnknownAuthenticationTypeException {
        log.info("Integration test 'Create. Create folder list' start.");
        // create config
        OneDriveCreateConfiguration dataSetCreate = new OneDriveCreateConfiguration();
        dataSetCreate.setDataStore(dataStoreLoginPassword);
        dataSetCreate.setCreateDirectoriesByList(true);
        final String configCreate = configurationByExample().forInstance(dataSetCreate).configured().toQueryString();

        JsonObject jsonObject1 = jsonBuilderFactory.createObjectBuilder().add("objectPath", "folderInRoot1").build();
        JsonObject jsonObject2 = jsonBuilderFactory.createObjectBuilder().add("objectPath", "folderInRoot1/fold1").build();
        JsonObject jsonObject3 = jsonBuilderFactory.createObjectBuilder().add("objectPath", "folderInRoot1/fold2/fold22").build();
        componentsHandler.setInputData(Arrays.asList(jsonObject1, jsonObject2, jsonObject3));

        Job.components().component("emitter", "test://emitter")
                .component("onedrive-create", "OneDrive://Create?" + configCreate)
                .component("collector", "test://collector").connections()
                .from("emitter").to("onedrive-create")
                .from("onedrive-create").to("collector").build().run();
        final List<JsonObject> res = componentsHandler.getCollectedData(JsonObject.class);
        Assertions.assertEquals(3, res.size());

        Map<String, String> resMap1 = new HashMap<>();
        resMap1.put("file", null);
        resMap1.put("name", "folderInRoot1");
        resMap1.put("parentPath", "/drive/root:");
        Map<String, String> resMap2 = new HashMap<>();
        resMap2.put("file", null);
        resMap2.put("name", "fold1");
        resMap2.put("parentPath", "/drive/root:/folderInRoot1");
        Map<String, String> resMap3 = new HashMap<>();
        resMap3.put("file", null);
        resMap3.put("name", "fold22");
        resMap3.put("parentPath", "/drive/root:/folderInRoot1/fold2");
        List<Map<String, String>> goodResult = Arrays.asList(resMap1, resMap2, resMap3);

        List<Map<String, String>> result =res.stream().map(item ->{
            Map<String, String> resMap = new HashMap<>();
            resMap.put("file", item.containsKey("file") ? "" : null);
            resMap.put("name", item.getString("name"));
            resMap.put("parentPath", item.getJsonObject("parentReference").getString("path"));
            return resMap;
        }).collect(Collectors.toList());
        Assertions.assertEquals(goodResult, result);
    }

    @Test
    @DisplayName("Delete. Delete all files in folder")
    void deleteComponentAllFilesInFolder() throws IOException, BadCredentialsException, UnknownAuthenticationTypeException {
        log.info("Integration test 'Delete. All files in folder.");
        // create config
        OneDriveDeleteConfiguration dataSet = new OneDriveDeleteConfiguration();
        dataSet.setDataStore(dataStoreLoginPassword);
        final String config = configurationByExample().forInstance(dataSet).configured().toQueryString();

        ConfigurationHelper.setupServices(oneDriveAuthHttpClientService);
        DriveItem parent = oneDriveHttpClientService.createItem(dataStoreLoginPassword, null, OneDriveObjectType.FILE, "newDir3/newDir4/newFile.txt");
        String parentId = parent.id;
        JsonObject jsonObject = jsonBuilderFactory.createObjectBuilder().add("id", parentId).build();
        componentsHandler.setInputData(Arrays.asList(jsonObject));

        Job.components().component("emitter", "test://emitter")
                .component("onedrive-delete", "OneDrive://Delete?" + config)
                .component("collector", "test://collector").connections()
                .from("emitter").to("onedrive-delete")
                .from("onedrive-delete").to("collector").build().run();
        final List<JsonObject> res = componentsHandler.getCollectedData(JsonObject.class);
        Assertions.assertEquals(1, res.size());
    }

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

    // @Test
    // @DisplayName("Health check")
    // void healthCheckTest() {
    // log.info("Integration test 'Health Check' start ");
    // HealthCheckStatus healthCheckStatus = oneDriveService.validateBasicConnection(dataStoreLoginPassword);
    // assertEquals(HealthCheckStatus.Status.OK, healthCheckStatus.getStatus());
    // }

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
