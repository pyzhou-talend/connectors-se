package org.talend.components.magentocms;

import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.function.Executable;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.talend.components.magentocms.common.AuthenticationLoginPasswordConfiguration;
import org.talend.components.magentocms.common.AuthenticationType;
import org.talend.components.magentocms.common.MagentoDataStore;
import org.talend.components.magentocms.common.RestVersion;
import org.talend.components.magentocms.helpers.ConfigurationHelper;
import org.talend.components.magentocms.input.ConfigurationFilter;
import org.talend.components.magentocms.input.MagentoInputConfiguration;
import org.talend.components.magentocms.input.SelectionFilter;
import org.talend.components.magentocms.input.SelectionFilterOperator;
import org.talend.components.magentocms.input.SelectionType;
import org.talend.components.magentocms.output.MagentoOutputConfiguration;
import org.talend.components.magentocms.service.http.BadCredentialsException;
import org.talend.components.magentocms.service.http.BadRequestException;
import org.talend.components.magentocms.service.http.MagentoHttpClientService;
import org.talend.sdk.component.api.service.Service;
import org.talend.sdk.component.junit.BaseComponentsHandler;
import org.talend.sdk.component.junit.SimpleFactory;
import org.talend.sdk.component.junit5.Injected;
import org.talend.sdk.component.junit5.WithComponents;
import org.talend.sdk.component.runtime.manager.chain.Job;

import javax.json.JsonBuilderFactory;
import javax.json.JsonObject;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.TreeMap;
import java.util.UUID;
import java.util.stream.Stream;

@Slf4j
@DisplayName("Suite of test for the Magento components")
@WithComponents("org.talend.components.magentocms")
@ExtendWith(MagentoTestExtension.class)
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class MagentoTestIT {

    @Injected
    private BaseComponentsHandler componentsHandler = null;

    @Service
    private JsonBuilderFactory jsonBuilderFactory;

    // @Service
    // private JsonReaderFactory jsonReaderFactory = null;

    @Service
    private MagentoHttpClientService magentoHttpClientService;

    private MagentoTestExtension.TestContext testContext;

    @BeforeAll
    private void init(MagentoTestExtension.TestContext testContext) {
        log.info("init: " + testContext.getMagentoAdminPassword());
        this.testContext = testContext;
    }

    // @Test
    // void changingSchema() throws Exception {
    // try (final Jsonb jsonb = JsonbBuilder.create()) {
    // final RecordBuilderFactory factory = new AvroRecordBuilderFactoryProvider().apply("test");
    // final RecordConverters converters = new RecordConverters();
    // final JsonBuilderFactory builderFactory = Json.createBuilderFactory(emptyMap());
    //
    // try {
    // String js = "{\"extension_attributes\":{\"website_ids\":[]}}";
    // JsonObject jsObj = jsonReaderFactory.createReader(new ByteArrayInputStream(js.getBytes("UTF-8"))).readObject();
    // final Record record21 = converters.toRecord(jsObj, () -> jsonb, () -> factory);
    // final ByteArrayOutputStream buffer2 = new ByteArrayOutputStream();
    //
    // final Record record = converters.toRecord(
    // builderFactory.createObjectBuilder()
    // .add("value",
    // builderFactory.createObjectBuilder().add("somekey",
    // builderFactory.createArrayBuilder().build()))
    // .build(),
    // () -> jsonb, () -> factory);
    // final ByteArrayOutputStream buffer = new ByteArrayOutputStream();
    // SchemaRegistryCoder.of().encode(record, buffer);
    //
    // SchemaRegistryCoder.of().encode(record21, buffer2);
    // } catch (Exception e) {
    // String m = e.getMessage();
    // System.out.println(m);
    // Assertions.assertTrue(false);
    // }
    // }
    // }
    //
    // @Test
    // void changingSchema2() throws Exception {
    // try (final Jsonb jsonb = JsonbBuilder.create()) {
    // final RecordBuilderFactory factory = new AvroRecordBuilderFactoryProvider().apply("test");
    // final RecordConverters converters = new RecordConverters();
    // final JsonBuilderFactory builderFactory = Json.createBuilderFactory(emptyMap());
    //
    // try {
    // String js = "{\"extension_attributes\":{\"website_ids\":[1]}}";
    // JsonObject jsObj = jsonReaderFactory.createReader(new ByteArrayInputStream(js.getBytes("UTF-8"))).readObject();
    // final Record record21 = converters.toRecord(jsObj, () -> jsonb, () -> factory);
    // final ByteArrayOutputStream buffer2 = new ByteArrayOutputStream();
    // SchemaRegistryCoder.of().encode(record21, buffer2);
    // } catch (Exception e) {
    // String m = e.getMessage();
    // System.out.println(m);
    // Assertions.assertTrue(false);
    // }
    //
    // }
    // }
    //
    // @Test
    // void changingSchema3() throws Exception {
    // try (final Jsonb jsonb = JsonbBuilder.create()) {
    // final RecordBuilderFactory factory = new AvroRecordBuilderFactoryProvider().apply("test");
    // final RecordConverters converters = new RecordConverters();
    // final JsonBuilderFactory builderFactory = Json.createBuilderFactory(emptyMap());
    //
    // try {
    // String js = "{\"custom_attributes\":[" + "{\"attribute_code\":\"color\",\"value\":[]},"
    // + "{\"attribute_code\":\"category_ids\",\"value\":\"49\"}" + "]}";
    // JsonObject jsObj = jsonReaderFactory.createReader(new ByteArrayInputStream(js.getBytes("UTF-8"))).readObject();
    // final Record record21 = converters.toRecord(jsObj, () -> jsonb, () -> factory);
    // final ByteArrayOutputStream buffer2 = new ByteArrayOutputStream();
    // SchemaRegistryCoder.of().encode(record21, buffer2);
    // Assertions.assertTrue(true);
    // } catch (Exception e) {
    // String m = e.getMessage();
    // System.out.println(m);
    // Assertions.assertTrue(false);
    // }
    // }
    // }
    //
    // @Test
    // // created to help with TCOMP-1208
    // void avroRecordArrays() throws Exception {
    // try (final Jsonb jsonb = JsonbBuilder.create()) {
    // final RecordBuilderFactory factory = new AvroRecordBuilderFactoryProvider().apply("test");
    // final RecordConverters converters = new RecordConverters();
    // final JsonBuilderFactory builderFactory = Json.createBuilderFactory(emptyMap());
    // final Record record = converters
    // .toRecord(
    // builderFactory.createObjectBuilder()
    // .add("value",
    // builderFactory.createObjectBuilder().add("somekey",
    // builderFactory.createArrayBuilder().build()))
    // .build(),
    // () -> jsonb, () -> factory);
    // final ByteArrayOutputStream buffer = new ByteArrayOutputStream();
    // SchemaRegistryCoder.of().encode(record, buffer);
    // Assertions.assertTrue(SchemaRegistryCoder.of().decode(new ByteArrayInputStream(buffer.toByteArray()))
    // .getRecord("value").getArray(Object.class, "somekey").isEmpty());
    // }
    // }

    @Test
    @DisplayName("Input. Get product by SKU")
    void inputComponentProductBySku() {
        log.info("Integration test 'Input. Get product by SKU' start ");
        MagentoInputConfiguration dataSet = new MagentoInputConfiguration();
        dataSet.setMagentoDataStore(testContext.getDataStoreSecure());
        dataSet.setSelectionType(SelectionType.PRODUCTS);
        List<SelectionFilter> filterList = new ArrayList<>();
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
        dataSet.setMagentoDataStore(testContext.getDataStore());
        dataSet.setSelectionType(SelectionType.PRODUCTS);
        List<SelectionFilter> filterList = new ArrayList<>();
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
        dataSet.setMagentoDataStore(testContext.getDataStoreOauth1());
        dataSet.setSelectionType(SelectionType.PRODUCTS);
        List<SelectionFilter> filterList = new ArrayList<>();
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
        AuthenticationLoginPasswordConfiguration authSettingsBad = new AuthenticationLoginPasswordConfiguration(
                testContext.getMagentoAdminName(), testContext.getMagentoAdminPassword() + "_make it bad");
        MagentoDataStore dataStoreBad = new MagentoDataStore(
                "http://" + testContext.getDockerHostAddress() + ":" + testContext.getMagentoHttpPort(), RestVersion.V1,
                AuthenticationType.LOGIN_PASSWORD, null, null, authSettingsBad);

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

    private Stream<Arguments> methodSourceDataStores() {
        return Stream.of(Arguments.of(testContext.getDataStore()), Arguments.of(testContext.getDataStoreSecure()),
                Arguments.of(testContext.getDataStoreOauth1()));
    }

    @Test
    @DisplayName("Input. Bad request")
    void inputBadRequestNoParameters() {
        log.info("Integration test 'Input. Bad request' start");
        MagentoInputConfiguration dataSet = new MagentoInputConfiguration();
        dataSet.setMagentoDataStore(testContext.getDataStore());
        dataSet.setSelectionType(SelectionType.PRODUCTS);

        ConfigurationHelper.setupServicesInput(dataSet, magentoHttpClientService);

        Executable exec = () -> magentoHttpClientService.getRecords(dataSet.getMagentoDataStore(), dataSet.getMagentoUrl(),
                new TreeMap<>());
        Assertions.assertThrows(BadRequestException.class, exec);
    }

    @Test
    @DisplayName("Output. Bad request")
    void outputBadRequestNoParameters() {
        log.info("Integration test 'Output. Bad request' start");
        MagentoOutputConfiguration dataSet = new MagentoOutputConfiguration();
        dataSet.setMagentoDataStore(testContext.getDataStore());
        dataSet.setSelectionType(SelectionType.PRODUCTS);
        ConfigurationHelper.setupServicesOutput(dataSet, magentoHttpClientService);
        JsonObject dataList = jsonBuilderFactory.createObjectBuilder().add("bad_field", "").build();
        Executable exec = () -> magentoHttpClientService.postRecords(dataSet.getMagentoDataStore(), dataSet.getMagentoUrl(),
                dataList);
        Assertions.assertThrows(BadRequestException.class, exec);
    }
}
