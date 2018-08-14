package org.talend.components.magentocms.common;

import lombok.Getter;
import org.talend.sdk.component.api.configuration.Option;
import org.talend.sdk.component.api.configuration.ui.layout.GridLayout;
import org.talend.sdk.component.api.meta.Documentation;

import java.io.Serializable;

@Getter
@GridLayout({ @GridLayout.Row({ "authenticationOauth1ConsumerKey" }), @GridLayout.Row({ "authenticationOauth1ConsumerSecret" }),
        @GridLayout.Row({ "authenticationOauth1AccessToken" }), @GridLayout.Row({ "authenticationOauth1AccessTokenSecret" }) })
public class AuthenticationOauth1Settings implements Serializable, AuthenticationSettings {

    @Option
    @Documentation("authentication OAuth 1.0 consumer key")
    private String authenticationOauth1ConsumerKey;

    @Option
    @Documentation("authentication OAuth 1.0 consumer secret")
    private String authenticationOauth1ConsumerSecret;

    @Option
    @Documentation("authentication OAuth 1.0 access token")
    private String authenticationOauth1AccessToken;

    @Option
    @Documentation("authentication OAuth 1.0 access token secret")
    private String authenticationOauth1AccessTokenSecret;
}
