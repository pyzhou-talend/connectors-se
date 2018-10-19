package org.talend.components.onedrive;

import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.extension.BeforeAllCallback;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.api.extension.ParameterContext;
import org.junit.jupiter.api.extension.ParameterResolutionException;
import org.junit.jupiter.api.extension.ParameterResolver;
import org.talend.components.onedrive.common.AuthenticationLoginPasswordConfiguration;
import org.talend.components.onedrive.common.AuthenticationType;
import org.talend.components.onedrive.common.OneDriveDataStore;
import org.talend.sdk.component.maven.MavenDecrypter;
import org.talend.sdk.component.maven.Server;

@Slf4j
public class OneDriveTestExtension implements BeforeAllCallback, ParameterResolver {

    private final String TENANT_ID = "0333ca35-3f21-4f69-abef-c46d541d019d";

    private final String APPLICATION_ID = "eec80afa-f049-4b69-9004-f06f68962c87";

    /**
     *
     */
    private final String MAVEN_SERVER_NAME = "onedrive";

    private TestContext testContext = new TestContext();

    @Override
    public void beforeAll(ExtensionContext extensionContext) {
        log.info("extension before all start");
        Server oneDriveServer = new MavenDecrypter().find(MAVEN_SERVER_NAME);
        AuthenticationLoginPasswordConfiguration authenticationSettings = new AuthenticationLoginPasswordConfiguration(
                oneDriveServer.getUsername(), oneDriveServer.getPassword());
        testContext.dataStoreLoginPassword = new OneDriveDataStore(TENANT_ID, APPLICATION_ID, AuthenticationType.LOGIN_PASSWORD,
                authenticationSettings);
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

        private OneDriveDataStore dataStoreLoginPassword;

    }
}
