package org.talend.components.rabbitmq.service;

import org.junit.jupiter.api.Test;
import org.talend.components.rabbitmq.configuration.BasicConfiguration;
import org.talend.components.rabbitmq.datastore.RabbitMQDataStore;
import org.talend.sdk.component.api.record.Schema;
import org.talend.sdk.component.api.service.Service;
import org.talend.sdk.component.api.service.healthcheck.HealthCheckStatus;
import org.talend.sdk.component.junit5.WithComponents;

import java.util.Optional;

import static org.junit.Assert.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.talend.components.rabbitmq.MessageConst.MESSAGE_CONTENT;
import static org.talend.components.rabbitmq.testutils.RabbitMQTestConstants.HOSTNAME;
import static org.talend.components.rabbitmq.testutils.RabbitMQTestConstants.PASSWORD;
import static org.talend.components.rabbitmq.testutils.RabbitMQTestConstants.PORT;
import static org.talend.components.rabbitmq.testutils.RabbitMQTestConstants.USER_NAME;

@WithComponents("org.talend.components.rabbitmq")
class ActionServiceTest {

    @Service
    private ActionService actionService;

    @Test
    public void testNoConnection() {
        RabbitMQDataStore dataStore = new RabbitMQDataStore();
        dataStore.setHostname(HOSTNAME);
        dataStore.setPort(Integer.valueOf(PORT));
        dataStore.setUserName(USER_NAME);
        dataStore.setPassword(PASSWORD);
        HealthCheckStatus status = actionService.validateBasicDatastore(dataStore);

        assertEquals(HealthCheckStatus.Status.KO, status.getStatus());
    }

    @Test
    public void testGuessSchema() {
        Schema schema = actionService.guessSchema(new BasicConfiguration());
        assertNotNull(schema, "Guess Schema should not be null");
        Optional<Schema.Entry> optional = schema.getEntries().stream().findFirst();
        assertTrue(optional.isPresent(), "Guess Schema Entry was not set");
        assertEquals(MESSAGE_CONTENT, optional.get().getName());
        assertEquals(Schema.Type.STRING, optional.get().getType());
    }

    @Test
    public void testConnectionEmptyHostname() {
        RabbitMQDataStore dataStore = new RabbitMQDataStore();
        dataStore.setHostname("");
        dataStore.setPort(Integer.valueOf(PORT));
        assertThrows(IllegalArgumentException.class, () -> {
            actionService.validateBasicDatastore(dataStore);
        });
    }

    @Test
    public void testConnectionEmptyPort() {
        RabbitMQDataStore dataStore = new RabbitMQDataStore();
        dataStore.setHostname(HOSTNAME);
        dataStore.setPort(null);
        assertThrows(IllegalArgumentException.class, () -> {
            actionService.validateBasicDatastore(dataStore);
        });
    }

}
