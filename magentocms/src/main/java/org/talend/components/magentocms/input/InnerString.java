package org.talend.components.magentocms.input;

import lombok.AllArgsConstructor;
import lombok.Data;
import org.talend.sdk.component.api.configuration.Option;
import org.talend.sdk.component.api.configuration.ui.layout.GridLayout;
import org.talend.sdk.component.api.meta.Documentation;

@Data
@AllArgsConstructor
@GridLayout({ @GridLayout.Row({ "str" }) })
public class InnerString {

    @Option
    @Documentation("str property - updatable parameter")
    private String str;

}
