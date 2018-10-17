package org.talend.components.magentocms;

import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.extension.ExtendWith;
import org.talend.components.magentocms.input.MagentoInputConfiguration;
import org.talend.components.magentocms.input.SelectionType;
import org.talend.components.magentocms.service.MagentoCmsService;
import org.talend.sdk.component.api.service.Service;
import org.talend.sdk.component.api.service.healthcheck.HealthCheckStatus;
import org.talend.sdk.component.api.service.schema.Schema;
import org.talend.sdk.component.junit5.WithComponents;

import java.util.Arrays;
import java.util.stream.Collectors;

@Slf4j
@DisplayName("Suite of test for the Magento components")
@WithComponents("org.talend.components.magentocms")
@ExtendWith(MagentoTestExtension.class)
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class MagentoServiceTestIT {

    @Service
    private MagentoCmsService magentoCmsService = null;

    private MagentoTestExtension.TestContext testContext;

    @BeforeAll
    private void init(MagentoTestExtension.TestContext testContext) {
        log.info("init: " + testContext.getMagentoAdminPassword());
        this.testContext = testContext;
    }

    @Test
    @DisplayName("Schema discovery")
    void schemaDiscoveryTest() {
        log.info("Integration test 'Schema discovery' start ");
        MagentoInputConfiguration dataSet = new MagentoInputConfiguration();
        dataSet.setMagentoDataStore(testContext.getDataStoreSecure());
        dataSet.setSelectionType(SelectionType.PRODUCTS);

        Schema schema = magentoCmsService.guessTableSchema(dataSet);
        Assertions.assertTrue(schema.getEntries().stream().map(item -> item.getName()).collect(Collectors.toList())
                .containsAll(Arrays.asList("id", "sku", "name")));
    }

    @Test
    @DisplayName("Health check")
    void healthCheckTest() {
        log.info("Integration test 'Health Check' start ");
        HealthCheckStatus healthCheckStatus = magentoCmsService.validateBasicConnection(testContext.getDataStoreSecure());
        Assertions.assertEquals(HealthCheckStatus.Status.OK, healthCheckStatus.getStatus());
    }
}
