/*
 * Copyright (C) 2006-2019 Talend Inc. - www.talend.com
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */
package org.talend.components.rest.configuration.auth;

import java.io.Serializable;

import org.talend.components.rest.configuration.auth.oauth2.Oauth2;
import org.talend.sdk.component.api.configuration.Option;
import org.talend.sdk.component.api.configuration.condition.ActiveIf;
import org.talend.sdk.component.api.configuration.ui.DefaultValue;
import org.talend.sdk.component.api.configuration.ui.layout.GridLayout;
import org.talend.sdk.component.api.configuration.ui.widget.Credential;
import org.talend.sdk.component.api.meta.Documentation;

import lombok.Data;

@Data
@GridLayout({ @GridLayout.Row({ "type" }), @GridLayout.Row({ "basic" }), @GridLayout.Row({ "bearerToken" }),
        @GridLayout.Row({ "oauth2" }), })
@Documentation("Http authentication data store")
public class Authentication implements Serializable {

    @Option
    @Documentation("")
    private Authorization.AuthorizationType type = Authorization.AuthorizationType.NoAuth;

    @Option
    @ActiveIf(target = "type", value = "Basic")
    @Documentation("")
    private Basic basic;

    @Option
    @Credential
    @ActiveIf(target = "type", value = "Bearer")
    @DefaultValue("")
    @Documentation("")
    private String bearerToken;

    @Option
    @ActiveIf(target = "type", value = "Oauth2")
    @Documentation("")
    private Oauth2 oauth2;

}
