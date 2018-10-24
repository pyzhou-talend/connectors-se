package org.talend.components.netsuite.datastore;

import org.talend.components.netsuite.service.UIActionService;
import org.talend.sdk.component.api.configuration.Option;
import org.talend.sdk.component.api.configuration.action.Checkable;
import org.talend.sdk.component.api.configuration.condition.ActiveIf;
import org.talend.sdk.component.api.configuration.type.DataStore;
import org.talend.sdk.component.api.configuration.ui.layout.GridLayout;
import org.talend.sdk.component.api.configuration.ui.layout.GridLayouts;
import org.talend.sdk.component.api.configuration.ui.widget.Credential;
import org.talend.sdk.component.api.meta.Documentation;

import lombok.AllArgsConstructor;
import lombok.Data;

@Data
@DataStore("NetsuiteConnection")
@Checkable(UIActionService.HEALTH_CHECK)
@GridLayouts({
        @GridLayout({ @GridLayout.Row({ "endpoint" }), @GridLayout.Row({ "apiVersion" }), @GridLayout.Row({ "loginType" }),
                @GridLayout.Row({ "email" }), @GridLayout.Row({ "password" }), @GridLayout.Row({ "role" }),
                @GridLayout.Row({ "account" }), @GridLayout.Row({ "applicationId" }),
                @GridLayout.Row({ "consumerKey", "consumerSecret" }), @GridLayout.Row({ "tokenId", "tokenSecret" }) }),
        @GridLayout(names = { GridLayout.FormType.ADVANCED }, value = { @GridLayout.Row({ "enableCustomization" }) }) })
public class NetsuiteDataStore {

    @Option
    @Documentation("TODO fill the documentation for this parameter")
    private String endpoint = "https://webservices.na2.netsuite.com/services/NetSuitePort_2018_2";

    @Option
    @Documentation("")
    private ApiVersion apiVersion = ApiVersion.V2018_2;

    @Option
    @Documentation("")
    private LoginType loginType = LoginType.BASIC;

    @Option
    @ActiveIf(target = "loginType", value = "BASIC")
    @Documentation("TODO fill the documentation for this parameter")
    private String email;

    @Option
    @ActiveIf(target = "loginType", value = "BASIC")
    @Credential
    @Documentation("TODO fill the documentation for this parameter")
    private String password;

    @Option
    @ActiveIf(target = "loginType", value = "BASIC")
    @Documentation("TODO fill the documentation for this parameter")
    private int role;

    @Option
    @Documentation("TODO fill the documentation for this parameter")
    private String account;

    @Option
    @ActiveIf(target = "loginType", value = "BASIC")
    @Documentation("TODO fill the documentation for this parameter")
    private String applicationId;

    @Option
    @ActiveIf(target = "loginType", value = "TBA")
    @Documentation("TODO fill the documentation for this parameter")
    private String consumerKey;

    @Option
    @ActiveIf(target = "loginType", value = "TBA")
    @Credential
    @Documentation("TODO fill the documentation for this parameter")
    private String consumerSecret;

    @Option
    @ActiveIf(target = "loginType", value = "TBA")
    @Documentation("TODO fill the documentation for this parameter")
    private String tokenId;

    @Option
    @ActiveIf(target = "loginType", value = "TBA")
    @Credential
    @Documentation("TODO fill the documentation for this parameter")
    private String tokenSecret;

    @Option
    @Documentation("TODO fill the documentation for this parameter")
    private boolean enableCustomization = true;

    @AllArgsConstructor
    public enum ApiVersion {
        V2018_2("2018.2");

        private String version;

        public String getVersion() {
            return this.version;
        }
    }

    public enum LoginType {
        BASIC,
        TBA;

    }
}
