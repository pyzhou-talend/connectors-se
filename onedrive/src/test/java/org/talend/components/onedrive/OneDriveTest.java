package org.talend.components.onedrive;

import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.talend.components.onedrive.common.AuthenticationLoginPasswordSettings;
import org.talend.components.onedrive.common.AuthenticationType;
import org.talend.components.onedrive.common.OneDriveDataStore;
import org.talend.components.onedrive.common.UnknownAuthenticationTypeException;
import org.talend.components.onedrive.service.OneDriveService;
import org.talend.components.onedrive.sources.delete.OneDriveDeleteConfiguration;
import org.talend.components.onedrive.sources.list.OneDriveListConfiguration;
import org.talend.sdk.component.api.record.Schema;
import org.talend.sdk.component.api.service.Service;
import org.talend.sdk.component.junit5.WithComponents;

import java.util.Arrays;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Slf4j
@WithComponents("org.talend.components.onedrive")
public class OneDriveTest {

    @Service
    private OneDriveService oneDriveService = null;

    @Test
    public void testConnection() throws UnknownAuthenticationTypeException {
        AuthenticationLoginPasswordSettings authenticationLoginPasswordSettings = new AuthenticationLoginPasswordSettings();
        OneDriveDataStore magentoCmsConfigurationBase;
        magentoCmsConfigurationBase = new OneDriveDataStore(null, null, AuthenticationType.LOGIN_PASSWORD,
                authenticationLoginPasswordSettings);
        assertEquals(authenticationLoginPasswordSettings, magentoCmsConfigurationBase.getAuthSettings());
    }

    @Test
    @DisplayName("Schema discovery List")
    void schemaDiscoveryListTest() {
        log.info("Integration test 'Schema discovery list' start ");
        OneDriveListConfiguration dataSet = new OneDriveListConfiguration();
        Schema schema = oneDriveService.guessTableSchemaList(dataSet);
        assertTrue(schema.getEntries().stream().map(Schema.Entry::getName).collect(Collectors.toList())
                .containsAll(Arrays.asList("id", "createdDateTime", "eTag", "lastModifiedDateTime", "name", "webUrl", "cTag",
                        "size", "createdBy", "lastModifiedBy", "parentReference", "fileSystemInfo", "folder", "file", "root")));
    }

    @Test
    @DisplayName("Schema discovery Delete")
    void schemaDiscoveryDeleteTest() {
        log.info("Integration test 'Schema discovery delete' start ");
        OneDriveDeleteConfiguration dataSet = new OneDriveDeleteConfiguration();
        Schema schema = oneDriveService.guessTableSchemaDelete(dataSet);
        assertTrue(schema.getEntries().stream().map(Schema.Entry::getName).collect(Collectors.toList())
                .containsAll(Arrays.asList("id")));
    }
}
