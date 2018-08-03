package org.talend.components.magentocms.output;

import lombok.Getter;
import org.talend.components.magentocms.common.MagentoCmsConfigurationBase;
import org.talend.components.magentocms.input.SelectionType;
import org.talend.sdk.component.api.configuration.Option;
import org.talend.sdk.component.api.configuration.ui.layout.GridLayout;
import org.talend.sdk.component.api.meta.Documentation;

@GridLayout({ @GridLayout.Row({ "magentoWebServerAddress" }), @GridLayout.Row({ "magentoRestVersion" }),
        @GridLayout.Row({ "authenticationType" }), @GridLayout.Row({ "authenticationOauth1ConsumerKey" }),
        @GridLayout.Row({ "authenticationOauth1ConsumerSecret" }), @GridLayout.Row({ "authenticationOauth1AccessToken" }),
        @GridLayout.Row({ "authenticationOauth1AccessTokenSecret" }), @GridLayout.Row({ "selectionType" }) })
@Documentation("TODO fill the documentation for this configuration")
public class MagentoCmsOutputConfiguration extends MagentoCmsConfigurationBase {
    // @Option
    // @Documentation("Address of web server (including port after ':'), e.g. 'mymagentoserver.com:1234'")
    // @Getter
    // private String magentoWebServerAddress;
    //
    // @Option
    // @Documentation("The version of Magento REST ,e.g. 'V1'")
    // @Getter
    // private RestVersion magentoRestVersion;
    //
    // @Option
    // @Documentation("authentication type (OAuth 1.0 or else)")
    // @Getter
    // private AuthenticationType authenticationType;
    //
    // @Option
    // @Documentation("authentication OAuth 1.0 consumer key")
    // @ActiveIf(target = "authenticationType", value = "OAUTH_1")
    // @Getter
    // private String authenticationOauth1ConsumerKey;
    //
    // @Option
    // @Documentation("authentication OAuth 1.0 consumer secret")
    // @ActiveIf(target = "authenticationType", value = "OAUTH_1")
    // @Getter
    // private String authenticationOauth1ConsumerSecret;
    //
    // @Option
    // @Documentation("authentication OAuth 1.0 access token")
    // @ActiveIf(target = "authenticationType", value = "OAUTH_1")
    // @Getter
    // private String authenticationOauth1AccessToken;
    //
    // @Option
    // @Documentation("authentication OAuth 1.0 access token secret")
    // @ActiveIf(target = "authenticationType", value = "OAUTH_1")
    // @Getter
    // private String authenticationOauth1AccessTokenSecret;

    @Option
    @Documentation("The type of information we want to get")
    @Getter
    private SelectionType selectionType;

}