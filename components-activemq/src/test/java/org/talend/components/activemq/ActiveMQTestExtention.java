package org.talend.components.activemq;

import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.extension.BeforeAllCallback;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.api.extension.ParameterContext;
import org.junit.jupiter.api.extension.ParameterResolutionException;
import org.junit.jupiter.api.extension.ParameterResolver;
import org.talend.components.activemq.datastore.ActiveMQDataStore;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.images.builder.ImageFromDockerfile;

import java.nio.file.Paths;
import java.time.Duration;

import static org.junit.jupiter.api.extension.ExtensionContext.Namespace.GLOBAL;

@Slf4j
public class ActiveMQTestExtention implements ExtensionContext.Store.CloseableResource, BeforeAllCallback, ParameterResolver {

    private final static int ACTIVEMQ_PORT = 61617;

    private TestContext testContext = new TestContext();

    private static final GenericContainer ACTIVEMQ_CONTAINER = new GenericContainer(
            new ImageFromDockerfile().withFileFromClasspath("Dockerfile", "docker/Dockerfile").withFileFromPath("activemq.xml",
                    Paths.get("src/test/resources/docker/activemq.xml"))).withExposedPorts(ACTIVEMQ_PORT)
                            .waitingFor(Wait.forHealthcheck().withStartupTimeout(Duration.ofSeconds(200)));

    private static boolean started = false;

    @Override
    public void beforeAll(ExtensionContext extensionContext) {
        log.info("extension before all start");
        if (!started) {
            started = true;
            ACTIVEMQ_CONTAINER.start();
            // The following line registers a callback hook when the root test context is shut down
            extensionContext.getRoot().getStore(GLOBAL).put("any unique name", this);
        }

        String dockerHostAddress = ACTIVEMQ_CONTAINER.getContainerIpAddress();
        Integer activemqHttpPort = ACTIVEMQ_CONTAINER.getMappedPort(ACTIVEMQ_PORT);

        ActiveMQDataStore dataStore = new ActiveMQDataStore();

        dataStore.setHost(dockerHostAddress);
        dataStore.setPort(activemqHttpPort);
        dataStore.setSSL(true);
        testContext.dataStore = dataStore;

        log.info("docker machine: " + dockerHostAddress + ":" + activemqHttpPort);
    }

    @Override
    public void close() {
        log.info("extension close call");
        ACTIVEMQ_CONTAINER.stop();
    }

    @Override
    public boolean supportsParameter(ParameterContext parameterContext, ExtensionContext extensionContext)
            throws ParameterResolutionException {
        return TestContext.class.equals(parameterContext.getParameter().getType());
    }

    @Override
    public Object resolveParameter(ParameterContext parameterContext, ExtensionContext extensionContext)
            throws ParameterResolutionException {
        return testContext;
    }

    @Data
    public static class TestContext {
        private ActiveMQDataStore dataStore;
    }
}