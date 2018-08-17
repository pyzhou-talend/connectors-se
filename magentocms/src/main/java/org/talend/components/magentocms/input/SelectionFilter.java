package org.talend.components.magentocms.input;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import org.talend.sdk.component.api.configuration.Option;
import org.talend.sdk.component.api.meta.Documentation;

import java.io.Serializable;

@Getter
@NoArgsConstructor
@AllArgsConstructor
// @OptionsOrder({ "fieldName", "condition", "value" })
public class SelectionFilter implements Serializable {

    // @Option
    // @Documentation("The number of 'and' group, filters with the same 'andGroupNumber' are linked by OR, 'and' groups are linked
    // by AND")
    // private int andGroupNumber;

    @Option
    @Documentation("The name of field to filter, e.g. 'sku'")
    private String fieldName = "";

    @Option
    @Documentation("The condition to filter, e.g. 'eq' or 'like'")
    private String fieldNameCondition = "";

    @Option
    @Documentation("The value to filter, e.g. 'my_preferable_sku'")
    private String value = "";
}
