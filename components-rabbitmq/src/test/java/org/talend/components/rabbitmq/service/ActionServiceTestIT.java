package org.talend.components.rabbitmq.service;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.extension.ExtendWith;
import org.talend.components.rabbitmq.RabbitMQTestExtention;
import org.talend.sdk.component.api.service.Service;
import org.talend.sdk.component.api.service.healthcheck.HealthCheckStatus;
import org.talend.sdk.component.junit5.WithComponents;

import static org.junit.jupiter.api.Assertions.assertEquals;

@WithComponents("org.talend.components.rabbitmq")
@ExtendWith(RabbitMQTestExtention.class)
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class ActionServiceTestIT {

    @Service
    private ActionService actionService;

    private RabbitMQTestExtention.TestContext testContext;

    @BeforeAll
    private void init(RabbitMQTestExtention.TestContext testContext) {
        this.testContext = testContext;
    }

    @Test
    public void testSuccessfulConnection() {
        HealthCheckStatus status = actionService.validateBasicDatastore(testContext.getDataStore());

        assertEquals(HealthCheckStatus.Status.OK, status.getStatus());
    }
}
