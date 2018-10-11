package org.talend.components.magentocms.input;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.talend.sdk.component.api.configuration.Option;
import org.talend.sdk.component.api.configuration.ui.layout.GridLayout;
import org.talend.sdk.component.api.meta.Documentation;

@Data
@NoArgsConstructor
@AllArgsConstructor
@GridLayout({ @GridLayout.Row({ "filterAdvancedValue" }) })
public class FilterAdvancedValueWrapper {

    @Option
    @Documentation("Full text of advanced filter. Use '&' to join conditions. See Magento's 'Search using REST endpoints' article")
    private String filterAdvancedValue = "";

    @Override
    public String toString() {
        return filterAdvancedValue;
    }
}
