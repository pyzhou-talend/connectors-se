package org.talend.components.magentocms.common;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.ToString;
import org.talend.sdk.component.api.configuration.Option;
import org.talend.sdk.component.api.configuration.ui.layout.GridLayout;
import org.talend.sdk.component.api.configuration.ui.widget.Credential;
import org.talend.sdk.component.api.meta.Documentation;

import java.io.Serializable;

@Getter
@NoArgsConstructor
@AllArgsConstructor
@ToString
@GridLayout({ @GridLayout.Row({ "authenticationOauth1ConsumerKey" }), @GridLayout.Row({ "authenticationOauth1ConsumerSecret" }),
        @GridLayout.Row({ "authenticationOauth1AccessToken" }), @GridLayout.Row({ "authenticationOauth1AccessTokenSecret" }) })
@Documentation("'OAuth 1.0' authentication settings")
public class AuthenticationOauth1Configuration implements Serializable, AuthenticationConfiguration {

    @Option
    @Documentation("authentication OAuth 1.0 consumer key")
    private String authenticationOauth1ConsumerKey;

    @Option
    @Credential
    @Documentation("authentication OAuth 1.0 consumer secret")
    private String authenticationOauth1ConsumerSecret;

    @Option
    @Documentation("authentication OAuth 1.0 access token")
    private String authenticationOauth1AccessToken;

    @Option
    @Credential
    @Documentation("authentication OAuth 1.0 access token secret")
    private String authenticationOauth1AccessTokenSecret;
}
