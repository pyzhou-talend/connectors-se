package org.talend.components.onedrive.common;

import lombok.AllArgsConstructor;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.ToString;
import org.talend.components.onedrive.helpers.ConfigurationHelper;
import org.talend.sdk.component.api.configuration.Option;
import org.talend.sdk.component.api.configuration.action.Checkable;
import org.talend.sdk.component.api.configuration.condition.ActiveIf;
import org.talend.sdk.component.api.configuration.type.DataStore;
import org.talend.sdk.component.api.configuration.ui.layout.GridLayout;
import org.talend.sdk.component.api.meta.Documentation;

import java.io.Serializable;

@Getter
@NoArgsConstructor
@AllArgsConstructor
@ToString
@EqualsAndHashCode
@DataStore(ConfigurationHelper.DATA_STORE_ID)
@Checkable(ConfigurationHelper.DATA_STORE_HEALTH_CHECK)
@GridLayout({ @GridLayout.Row({ "tenantId" }), @GridLayout.Row({ "applicationId" }), @GridLayout.Row({ "authenticationType" }),
        @GridLayout.Row({ "authenticationLoginPasswordSettings" }) })
@Documentation("Data store settings. OneDrive's server connection and authentication preferences")
public class OneDriveDataStore implements Serializable {

    @Option
    @Documentation("Tenant ID")
    private String tenantId = "";

    @Option
    @Documentation("Application ID")
    private String applicationId = "";

    @Option
    @Documentation("authentication type (Login etc.)")
    private AuthenticationType authenticationType = AuthenticationType.LOGIN_PASSWORD;

    @Option
    @Documentation("authentication Login settings")
    @ActiveIf(target = "authenticationType", value = { "LOGIN_PASSWORD" })
    private AuthenticationLoginPasswordSettings authenticationLoginPasswordSettings;

    public AuthenticationSettings getAuthSettings() throws UnknownAuthenticationTypeException {
        if (authenticationType == AuthenticationType.LOGIN_PASSWORD) {
            return authenticationLoginPasswordSettings;
        }
        throw new UnknownAuthenticationTypeException();
    }
}