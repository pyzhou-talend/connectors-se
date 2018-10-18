package org.talend.components.onedrive;

import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.junit.Rule;
import org.junit.jupiter.api.extension.BeforeAllCallback;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.api.extension.ParameterContext;
import org.junit.jupiter.api.extension.ParameterResolutionException;
import org.junit.jupiter.api.extension.ParameterResolver;
import org.talend.components.onedrive.common.AuthenticationLoginPasswordConfiguration;
import org.talend.components.onedrive.common.AuthenticationType;
import org.talend.components.onedrive.common.OneDriveDataStore;
import org.talend.sdk.component.api.DecryptedServer;
import org.talend.sdk.component.junit.MavenDecrypterRule;
import org.talend.sdk.component.junit5.WithMavenServers;
import org.talend.sdk.component.maven.Server;

@Slf4j
@WithMavenServers
public class OneDriveTestExtension implements BeforeAllCallback, ParameterResolver {

    private final String TENANT_ID = "0333ca35-3f21-4f69-abef-c46d541d019d";

    private final String APPLICATION_ID = "eec80afa-f049-4b69-9004-f06f68962c87";

    private final String ADMIN_NAME = "sbovsunovskyi@talend.com";

    private final String ADMIN_PASSWORD = "";

    private TestContext testContext = new TestContext();

    // private final MavenDecrypter mavenDecrypter = new MavenDecrypter();

    @Rule
    public final MavenDecrypterRule mavenDecrypterRule = new MavenDecrypterRule(this);

    @DecryptedServer("onedrive")
    private Server oneDriveServer = null;

    @Override
    public void beforeAll(ExtensionContext extensionContext) throws Exception {
        log.info("extension before all start" + oneDriveServer.getPassword());

        AuthenticationLoginPasswordConfiguration authenticationSettings = new AuthenticationLoginPasswordConfiguration(ADMIN_NAME,
                ADMIN_PASSWORD);
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
