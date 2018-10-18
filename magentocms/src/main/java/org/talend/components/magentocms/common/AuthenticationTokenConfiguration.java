package org.talend.components.magentocms.common;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import org.talend.components.magentocms.messages.Messages;
import org.talend.sdk.component.api.configuration.Option;
import org.talend.sdk.component.api.configuration.constraint.Required;
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
    @Required
    @Documentation("access token for 'Token' authentication")
    private String authenticationAccessToken = "";

    @Override
    public void validate(Messages i18n) {
        if (authenticationAccessToken.isEmpty()) {
            throw new RuntimeException(i18n.healthCheckTokenIsEmpty());
        }
    }

    @Override
    public String toString() {
        return authenticationAccessToken;
    }
}
