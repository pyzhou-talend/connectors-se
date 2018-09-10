package org.talend.components.magentocms.common;

import lombok.*;
import org.talend.sdk.component.api.configuration.Option;
import org.talend.sdk.component.api.configuration.ui.layout.GridLayout;
import org.talend.sdk.component.api.configuration.ui.widget.Credential;
import org.talend.sdk.component.api.meta.Documentation;

import java.io.Serializable;

@Getter
@NoArgsConstructor
@AllArgsConstructor
@EqualsAndHashCode
@ToString
@GridLayout({ @GridLayout.Row({ "authenticationLogin" }), @GridLayout.Row({ "authenticationPassword" }) })
@Documentation("'Login' authentication settings")
public class AuthenticationLoginPasswordSettings implements Serializable, AuthenticationSettings {

    @Option
    @Documentation("authentication login for 'Login' authentication")
    private String authenticationLogin;

    @Option
    @Credential
    @Documentation("authentication password for 'Login' authentication")
    private String authenticationPassword;
}
