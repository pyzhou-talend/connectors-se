package org.talend.components.netsuite.datastore;

import org.talend.sdk.component.api.configuration.Option;
import org.talend.sdk.component.api.configuration.action.Checkable;
import org.talend.sdk.component.api.configuration.type.DataStore;
import org.talend.sdk.component.api.configuration.ui.layout.GridLayout;
import org.talend.sdk.component.api.configuration.ui.layout.GridLayouts;
import org.talend.sdk.component.api.configuration.ui.widget.Credential;
import org.talend.sdk.component.api.meta.Documentation;

import lombok.AllArgsConstructor;
import lombok.Data;

@Data
@DataStore("NetsuiteConnection")
@Checkable("connection.healthcheck")
@GridLayouts({
        @GridLayout({ @GridLayout.Row({ "endpoint", "apiVersion" }), @GridLayout.Row({ "email" }),
                @GridLayout.Row({ "password" }), @GridLayout.Row({ "role" }), @GridLayout.Row({ "account" }),
                @GridLayout.Row({ "applicationId" }) }),
        @GridLayout(names = { GridLayout.FormType.ADVANCED }, value = { @GridLayout.Row({ "enableCustomization" }) }) })
public class NetsuiteDataStore {

    @Option
    @Documentation("TODO fill the documentation for this parameter")
    private String endpoint;

    @Option
    @Documentation("TODO fill the documentation for this parameter")
    private String email;

    @Option
    @Credential
    @Documentation("TODO fill the documentation for this parameter")
    private String password;

    @Option
    @Documentation("TODO fill the documentation for this parameter")
    private int role;

    @Option
    @Documentation("TODO fill the documentation for this parameter")
    private String account;

    @Option
    @Documentation("TODO fill the documentation for this parameter")
    private String applicationId;

    @Option
    @Documentation("TODO fill the documentation for this parameter")
    private boolean enableCustomization = true;

    @Option
    @Documentation("")
    private ApiVersion apiVersion;

    @AllArgsConstructor
    public enum ApiVersion {
        V2016_2("2016.2"),
        V2018_1("2018.1");

        private String version;

        public String getVersion() {
            return this.version;
        }
    }
}
