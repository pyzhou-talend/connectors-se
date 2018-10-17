package org.talend.components.magentocms.messages;

import org.talend.sdk.component.api.internationalization.Internationalized;

@Internationalized
public interface Messages {

    String healthCheckOk();

    String healthCheckFailed(String error);

    String healthCheckServerUrlIsEmpty();

    String healthCheckLoginIsEmpty();

    String healthCheckTokenIsEmpty();

    String healthCheckOauthParameterIsEmpty();
}
