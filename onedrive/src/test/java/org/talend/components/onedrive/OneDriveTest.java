package org.talend.components.onedrive;

import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Test;
import org.talend.components.onedrive.common.AuthenticationLoginPasswordSettings;
import org.talend.components.onedrive.common.AuthenticationType;
import org.talend.components.onedrive.common.OneDriveDataStore;
import org.talend.components.onedrive.common.UnknownAuthenticationTypeException;

import static org.junit.jupiter.api.Assertions.assertEquals;

@Slf4j
public class OneDriveTest {

    @Test
    public void testConnection() throws UnknownAuthenticationTypeException {
        AuthenticationLoginPasswordSettings authenticationLoginPasswordSettings = new AuthenticationLoginPasswordSettings();
        OneDriveDataStore magentoCmsConfigurationBase;
        magentoCmsConfigurationBase = new OneDriveDataStore(null, null, AuthenticationType.LOGIN_PASSWORD,
                authenticationLoginPasswordSettings);
        assertEquals(authenticationLoginPasswordSettings, magentoCmsConfigurationBase.getAuthSettings());
    }
}
