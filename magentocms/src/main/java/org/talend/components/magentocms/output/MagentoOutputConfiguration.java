package org.talend.components.magentocms.output;

import lombok.Data;
import org.talend.components.magentocms.common.MagentoDataStore;
import org.talend.components.magentocms.common.Validatable;
import org.talend.components.magentocms.input.SelectionType;
import org.talend.components.magentocms.messages.Messages;
import org.talend.sdk.component.api.configuration.Option;
import org.talend.sdk.component.api.configuration.type.DataSet;
import org.talend.sdk.component.api.configuration.ui.layout.GridLayout;
import org.talend.sdk.component.api.meta.Documentation;

import java.io.Serializable;

@Data
@DataSet("MagentoOutput")
@GridLayout({ @GridLayout.Row({ "magentoDataStore" }), @GridLayout.Row({ "selectionType" }) })
@GridLayout(names = GridLayout.FormType.ADVANCED, value = { @GridLayout.Row({ "magentoDataStore" }) })
@Documentation("Output component configuration")
public class MagentoOutputConfiguration implements Serializable, Validatable {

    @Option
    @Documentation("Connection to Magento CMS")
    private MagentoDataStore magentoDataStore;

    @Option
    @Documentation("The type of information we want to put, e.g. 'Products'")
    private SelectionType selectionType = SelectionType.PRODUCTS;

    public String getMagentoUrl() {
        String res = magentoDataStore.getMagentoBaseUrl() + "/" + selectionType.name().toLowerCase();
        return res;
    }

    @Override
    public void validate(Messages i18n) {
        magentoDataStore.validate(i18n);
    }
}