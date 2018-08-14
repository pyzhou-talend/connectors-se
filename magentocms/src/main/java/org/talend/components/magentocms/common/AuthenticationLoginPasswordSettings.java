package org.talend.components.magentocms.common;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import org.talend.sdk.component.api.configuration.Option;
import org.talend.sdk.component.api.configuration.ui.layout.GridLayout;
import org.talend.sdk.component.api.meta.Documentation;

import java.io.Serializable;

@Getter
@NoArgsConstructor
@AllArgsConstructor
@GridLayout({ @GridLayout.Row({ "authenticationLogin" }), @GridLayout.Row({ "authenticationPassword" }) })
public class AuthenticationLoginPasswordSettings implements Serializable, AuthenticationSettings {

    @Option
    @Documentation("authentication login")
    private String authenticationLogin;

    @Option
    @Documentation("authentication password")
    private String authenticationPassword;
}
