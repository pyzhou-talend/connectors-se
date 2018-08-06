package org.talend.components.magentocms.input;

import lombok.Data;
import org.talend.sdk.component.api.configuration.Option;
import org.talend.sdk.component.api.configuration.constraint.Required;
import org.talend.sdk.component.api.configuration.ui.OptionsOrder;
import org.talend.sdk.component.api.meta.Documentation;

import java.io.Serializable;

@Data
@OptionsOrder({ "fieldName", "condition", "value" })
public class SelectionFilter implements Serializable {

    @Option
    @Required
    @Documentation("The name of field to filter, e.g. 'sku'")
    private String fieldName = "";

    @Option
    @Required
    @Documentation("The condition to filter, e.g. 'eq' or 'like'")
    private String condition = "";

    @Option
    @Required
    @Documentation("The value to filter, e.g. 'my_preferable_sku'")
    private String value = "";
}
