package org.talend.components.magentocms.common;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import org.talend.components.magentocms.helpers.ConfigurationHelper;
import org.talend.sdk.component.api.configuration.Option;
import org.talend.sdk.component.api.configuration.action.Validable;
import org.talend.sdk.component.api.configuration.ui.layout.GridLayout;
import org.talend.sdk.component.api.meta.Documentation;

import java.io.Serializable;

@Getter
@NoArgsConstructor
@AllArgsConstructor
@GridLayout({ @GridLayout.Row({ "authenticationAccessToken" }) })
@Documentation("'Token' authentication settings")
public class AuthenticationTokenConfiguration implements Serializable, AuthenticationConfiguration {

    @Option
    @Documentation("access token for 'Token' authentication")
    @Validable(ConfigurationHelper.VALIDATE_AUTH_TOKEN_ID)
    private String authenticationAccessToken = "";

    @Override
    public String toString() {
        return authenticationAccessToken;
    }
}
