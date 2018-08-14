package org.talend.components.magentocms;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.talend.components.magentocms.common.AuthenticationLoginPasswordSettings;
import org.talend.components.magentocms.common.AuthenticationType;
import org.talend.components.magentocms.common.MagentoCmsConfigurationBase;
import org.talend.components.magentocms.common.RestVersion;
import org.talend.components.magentocms.input.MagentoCmsInputMapperConfiguration;
import org.talend.components.magentocms.input.SelectionFilter;
import org.talend.components.magentocms.input.SelectionType;
import org.talend.sdk.component.junit.BaseComponentsHandler;
import org.talend.sdk.component.junit5.Injected;
import org.talend.sdk.component.junit5.WithComponents;
import org.talend.sdk.component.runtime.manager.chain.Job;

import javax.json.JsonObject;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.talend.sdk.component.junit.SimpleFactory.configurationByExample;

@DisplayName("Suite of test for the Magento Input component")
@WithComponents("org.talend.components.magentocms")
class ITMagentoInputEmitter {

    @Injected
    private BaseComponentsHandler componentsHandler;

    @Test
    @DisplayName("")
    void inputWithBadCredential() {
        System.out.println("Integration test INPUT start ");
        String dockerHostAddress = System.getProperty("dockerHostAddress");
        String magentoHttpPort = System.getProperty("magentoHttpPort");
        String magentoAdminName = System.getProperty("magentoAdminName");
        String magentoAdminPassword = System.getProperty("magentoAdminPassword");
        System.out.println("docker machine: " + dockerHostAddress + ":" + magentoHttpPort);
        System.out.println("magento admin: " + magentoAdminName + " " + magentoAdminPassword);

        AuthenticationLoginPasswordSettings authenticationSettings = new AuthenticationLoginPasswordSettings(magentoAdminName,
                magentoAdminPassword);
        final MagentoCmsConfigurationBase dataStore = new MagentoCmsConfigurationBase(
                "http://" + dockerHostAddress + ":" + magentoHttpPort, RestVersion.V1, AuthenticationType.AUTHENTICATION_TOKEN,
                null, null, authenticationSettings);

        MagentoCmsInputMapperConfiguration dataSet = new MagentoCmsInputMapperConfiguration();
        dataSet.setMagentoCmsConfigurationBase(dataStore);
        dataSet.setSelectionType(SelectionType.PRODUCTS);
        List<SelectionFilter> filterList = new ArrayList<>();
        SelectionFilter filter = new SelectionFilter();
        filterList.add(filter);
        dataSet.setSelectionFilter(filterList);

        final String config = configurationByExample().forInstance(dataSet).configured().toQueryString();
        Job.components().component("salesforce-input", "Salesforce://Input?" + config).component("collector", "test://collector")
                .connections().from("salesforce-input").to("collector").build().run();
        final List<JsonObject> res = componentsHandler.getCollectedData(JsonObject.class);
        assertEquals(4, res.size());
        assertTrue(res.iterator().next().getString("Name").contains("Oil"));
    }
}
