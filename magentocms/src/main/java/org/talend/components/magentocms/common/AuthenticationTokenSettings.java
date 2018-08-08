package org.talend.components.magentocms.common;

import lombok.Getter;
import org.talend.sdk.component.api.configuration.Option;
import org.talend.sdk.component.api.configuration.ui.layout.GridLayout;
import org.talend.sdk.component.api.meta.Documentation;

import java.io.Serializable;

@GridLayout({ @GridLayout.Row({ "authenticationAccessToken" }) })
public class AuthenticationTokenSettings implements Serializable, AuthenticationSettings {

    @Option
    @Documentation("authentication OAuth 1.0 access token")
    @Getter
    private String authenticationAccessToken;
}
