package org.talend.components.magentocms.common;

import lombok.AllArgsConstructor;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NoArgsConstructor;
import org.talend.components.magentocms.helpers.ConfigurationHelper;
import org.talend.sdk.component.api.configuration.Option;
import org.talend.sdk.component.api.configuration.action.Validable;
import org.talend.sdk.component.api.configuration.ui.layout.GridLayout;
import org.talend.sdk.component.api.configuration.ui.widget.Credential;
import org.talend.sdk.component.api.meta.Documentation;

import java.io.Serializable;

@Getter
@NoArgsConstructor
@AllArgsConstructor
@EqualsAndHashCode
@GridLayout({ @GridLayout.Row({ "authenticationLogin" }), @GridLayout.Row({ "authenticationPassword" }) })
@Documentation("'Login' authentication settings")
public class AuthenticationLoginPasswordConfiguration implements Serializable, AuthenticationConfiguration {

    @Option
    @Documentation("authentication login for 'Login' authentication")
    @Validable(ConfigurationHelper.VALIDATE_AUTH_LOGIN_PASSWORD_LOGIN_ID)
    private String authenticationLogin = "";

    @Option
    @Credential
    @Documentation("authentication password for 'Login' authentication")
    private String authenticationPassword = "";

}
