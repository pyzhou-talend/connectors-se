package org.talend.components.magentocms.output;

import lombok.Getter;
import org.talend.components.magentocms.common.MagentoCmsConfigurationBase;
import org.talend.components.magentocms.input.SelectionType;
import org.talend.sdk.component.api.configuration.Option;
import org.talend.sdk.component.api.configuration.ui.layout.GridLayout;
import org.talend.sdk.component.api.meta.Documentation;

@GridLayout({ @GridLayout.Row({ "magentoWebServerAddress" }), @GridLayout.Row({ "magentoRestVersion" }),
        @GridLayout.Row({ "authenticationType" }), @GridLayout.Row({ "authenticationOauth1ConsumerKey" }),
        @GridLayout.Row({ "authenticationOauth1ConsumerSecret" }), @GridLayout.Row({ "authenticationOauth1AccessToken" }),
        @GridLayout.Row({ "authenticationOauth1AccessTokenSecret" }), @GridLayout.Row({ "selectionType" }) })
@Documentation("TODO fill the documentation for this configuration")
public class MagentoCmsOutputConfiguration extends MagentoCmsConfigurationBase {
    // selection type, e.g. 'Products'
    @Option
    @Documentation("The type of information we want to get")
    @Getter
    private SelectionType selectionType;

}