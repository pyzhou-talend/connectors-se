package org.talend.components.netsuite.datastore;

import org.talend.sdk.component.api.configuration.Option;
import org.talend.sdk.component.api.configuration.action.Checkable;
import org.talend.sdk.component.api.configuration.type.DataStore;
import org.talend.sdk.component.api.configuration.ui.layout.GridLayout;
import org.talend.sdk.component.api.configuration.ui.layout.GridLayouts;
import org.talend.sdk.component.api.configuration.ui.widget.Credential;
import org.talend.sdk.component.api.meta.Documentation;

@DataStore("NetsuiteConnection")
@Checkable("connection.healthcheck")
@GridLayouts({
        @GridLayout({ @GridLayout.Row({ "endpoint" }), @GridLayout.Row({ "email" }), @GridLayout.Row({ "password" }),
                @GridLayout.Row({ "role" }), @GridLayout.Row({ "account" }), @GridLayout.Row({ "applicationId" }) }),
        @GridLayout(names = { GridLayout.FormType.ADVANCED }, value = { @GridLayout.Row({ "enableCustomization" }) }) })
public class NetsuiteDataStore {

    @Option
    @Documentation("TODO fill the documentation for this parameter")
    private java.net.URL endpoint;

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
    private boolean enableCustomization;

    public java.net.URL getEndpoint() {
        return endpoint;
    }

    public NetsuiteDataStore setEndpoint(java.net.URL endpoint) {
        this.endpoint = endpoint;
        return this;
    }

    public String getEmail() {
        return email;
    }

    public NetsuiteDataStore setEmail(String email) {
        this.email = email;
        return this;
    }

    public String getPassword() {
        return password;
    }

    public NetsuiteDataStore setPassword(String password) {
        this.password = password;
        return this;
    }

    public int getRole() {
        return role;
    }

    public NetsuiteDataStore setRole(int role) {
        this.role = role;
        return this;
    }

    public String getAccount() {
        return account;
    }

    public NetsuiteDataStore setAccount(String account) {
        this.account = account;
        return this;
    }

    public String getApplicationId() {
        return applicationId;
    }

    public NetsuiteDataStore setApplicationId(String applicationId) {
        this.applicationId = applicationId;
        return this;
    }

    public boolean isEnableCustomization() {
        return enableCustomization;
    }

    public NetsuiteDataStore setEnableCustomization(boolean enableCustomization) {
        this.enableCustomization = enableCustomization;
        return this;
    }
}
