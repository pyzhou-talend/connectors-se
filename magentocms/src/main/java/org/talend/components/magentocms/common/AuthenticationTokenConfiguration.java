package org.talend.components.magentocms.common;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.ToString;
import org.talend.sdk.component.api.configuration.Option;
import org.talend.sdk.component.api.configuration.ui.layout.GridLayout;
import org.talend.sdk.component.api.meta.Documentation;

import java.io.Serializable;

@Getter
@NoArgsConstructor
@AllArgsConstructor
@ToString
@GridLayout({ @GridLayout.Row({ "authenticationAccessToken" }) })
@Documentation("'Token' authentication settings")
public class AuthenticationTokenConfiguration implements Serializable, AuthenticationConfiguration {

    @Option
    @Documentation("access token for 'Token' authentication")
    private String authenticationAccessToken;
}
