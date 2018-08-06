package org.talend.components.magentocms.common;

import lombok.Getter;
import org.talend.sdk.component.api.configuration.Option;
import org.talend.sdk.component.api.configuration.condition.ActiveIf;
import org.talend.sdk.component.api.configuration.type.DataStore;
import org.talend.sdk.component.api.configuration.ui.layout.GridLayout;
import org.talend.sdk.component.api.meta.Documentation;

import java.io.Serializable;

@DataStore("MagentoDataStore")
@GridLayout({ @GridLayout.Row({ "magentoWebServerUrl" }), @GridLayout.Row({ "magentoRestVersion" }),
        @GridLayout.Row({ "authenticationType" }), @GridLayout.Row({ "authenticationOauth1Settings" }) })
public class MagentoCmsConfigurationBase implements Serializable {

    @Option
    @Documentation("URL of web server (including port after ':'), e.g. 'http://mymagentoserver.com:1234'")
    @Getter
    private String magentoWebServerUrl;

    @Option
    @Documentation("The version of Magento REST ,e.g. 'V1'")
    @Getter
    private RestVersion magentoRestVersion;

    @Option
    @Documentation("authentication type (OAuth 1.0 or else)")
    @Getter
    private AuthenticationType authenticationType;

    @Option
    @Documentation("authentication OAuth 1.0 consumer key")
    @ActiveIf(target = "authenticationType", value = "OAUTH_1")
    @Getter
    private AuthenticationOauth1Settings authenticationOauth1Settings;

    public AuthenticationSettings getAuthSettings() throws UnknownAuthenticationTypeException {
        if (authenticationType == AuthenticationType.OAUTH_1) {
            return authenticationOauth1Settings;
        }
        throw new UnknownAuthenticationTypeException();
    }
}