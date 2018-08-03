package org.talend.components.magentocms.common;

import lombok.Getter;
import org.talend.sdk.component.api.configuration.Option;
import org.talend.sdk.component.api.configuration.condition.ActiveIf;
import org.talend.sdk.component.api.meta.Documentation;

import java.io.Serializable;

public class MagentoCmsConfigurationBase implements Serializable {

    @Option
    @Documentation("Address of web server (including port after ':'), e.g. 'mymagentoserver.com:1234'")
    @Getter
    private String magentoWebServerAddress;

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
    private String authenticationOauth1ConsumerKey;

    @Option
    @Documentation("authentication OAuth 1.0 consumer secret")
    @ActiveIf(target = "authenticationType", value = "OAUTH_1")
    @Getter
    private String authenticationOauth1ConsumerSecret;

    @Option
    @Documentation("authentication OAuth 1.0 access token")
    @ActiveIf(target = "authenticationType", value = "OAUTH_1")
    @Getter
    private String authenticationOauth1AccessToken;

    @Option
    @Documentation("authentication OAuth 1.0 access token secret")
    @ActiveIf(target = "authenticationType", value = "OAUTH_1")
    @Getter
    private String authenticationOauth1AccessTokenSecret;
}