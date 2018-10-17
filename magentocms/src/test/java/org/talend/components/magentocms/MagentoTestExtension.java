package org.talend.components.magentocms;

import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.extension.BeforeAllCallback;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.api.extension.ParameterContext;
import org.junit.jupiter.api.extension.ParameterResolutionException;
import org.junit.jupiter.api.extension.ParameterResolver;
import org.talend.components.magentocms.common.AuthenticationLoginPasswordConfiguration;
import org.talend.components.magentocms.common.AuthenticationOauth1Configuration;
import org.talend.components.magentocms.common.AuthenticationType;
import org.talend.components.magentocms.common.MagentoDataStore;
import org.talend.components.magentocms.common.RestVersion;

@Slf4j
public class MagentoTestExtension implements BeforeAllCallback, ParameterResolver {

    /**
     * get this variables from Magento's docker image.
     * http://MAGENTO_URL/admin -> system -> integrations -> TalendTest -> Edit -> Integration Details
     */
    private static final String AUTHENTICATION_OAUTH1_CONSUMER_KEY = "7fqa5rplt4k9dubdbfea17mf3owyteqh";

    private static final String AUTHENTICATION_OAUTH1_CONSUMER_SECRET = "cpln0ehi2yh7tg5ho9bvlbyprfi0ukqk";

    private static final String AUTHENTICATION_OAUTH1_ACCESS_TOKEN = "j24y53g83te2fgye8fe8xondubqej4cl";

    private static final String AUTHENTICATION_OAUTH1_ACCESS_TOKEN_SECRET = "jxnbv58bc94dfsld1c9k7e6tvcqntrx2";

    private TestContext testContext = new TestContext();

    @Override
    public void beforeAll(ExtensionContext extensionContext) throws Exception {
        log.info("extension before all start");

        testContext.dockerHostAddress = System.getProperty("dockerHostAddress", "local.magento");
        testContext.magentoHttpPort = System.getProperty("magentoHttpPort", "80");
        testContext.magentoHttpPortSecure = System.getProperty("magentoHttpPortSecure", "443");
        testContext.magentoAdminName = System.getProperty("magentoAdminName");
        testContext.magentoAdminPassword = System.getProperty("magentoAdminPassword");

        log.info("docker machine: " + testContext.dockerHostAddress + ":" + testContext.magentoHttpPort);
        log.info("docker machine secure: " + testContext.dockerHostAddress + ":" + testContext.magentoHttpPortSecure);

        AuthenticationLoginPasswordConfiguration authenticationSettings = new AuthenticationLoginPasswordConfiguration(
                testContext.magentoAdminName, testContext.magentoAdminPassword);
        AuthenticationOauth1Configuration authenticationOauth1Settings = new AuthenticationOauth1Configuration(
                AUTHENTICATION_OAUTH1_CONSUMER_KEY, AUTHENTICATION_OAUTH1_CONSUMER_SECRET, AUTHENTICATION_OAUTH1_ACCESS_TOKEN,
                AUTHENTICATION_OAUTH1_ACCESS_TOKEN_SECRET);
        testContext.dataStore = new MagentoDataStore(getBaseUrl(), RestVersion.V1, AuthenticationType.LOGIN_PASSWORD, null, null,
                authenticationSettings);
        testContext.dataStoreSecure = new MagentoDataStore(getBaseUrlSecure(), RestVersion.V1, AuthenticationType.LOGIN_PASSWORD,
                null, null, authenticationSettings);
        testContext.dataStoreOauth1 = new MagentoDataStore(getBaseUrl(), RestVersion.V1, AuthenticationType.OAUTH_1,
                authenticationOauth1Settings, null, null);
    }

    private String getBaseUrl() {
        return "http://" + testContext.dockerHostAddress
                + (testContext.magentoHttpPort.equals("80") ? "" : ":" + testContext.magentoHttpPort);
    }

    private String getBaseUrlSecure() {
        return "https://" + testContext.dockerHostAddress
                + (testContext.magentoHttpPortSecure.equals("443") ? "" : ":" + testContext.magentoHttpPortSecure);
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
    static class TestContext {

        private MagentoDataStore dataStore;

        private MagentoDataStore dataStoreSecure;

        private MagentoDataStore dataStoreOauth1;

        private String dockerHostAddress;

        private String magentoHttpPort;

        private String magentoHttpPortSecure;

        private String magentoAdminName;

        private String magentoAdminPassword;
    }
}
