package org.talend.components.netsuite.source;

import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;

import java.io.IOException;

import javax.json.JsonObject;

import org.junit.ClassRule;
import org.talend.components.netsuite.dataset.NetSuiteInputProperties;
import org.talend.sdk.component.junit.SimpleComponentRule;
import org.talend.sdk.component.runtime.input.Mapper;

public class NetSuiteInputMapperTest {

    @ClassRule
    public static final SimpleComponentRule COMPONENT_FACTORY = new SimpleComponentRule("org.talend.components.netsuite");

    // @Test
    // @Ignore("You need to complete this test")
    public void produce() throws IOException {

        // Source configuration
        // Setup your component configuration for the test here
        final NetSuiteInputProperties configuration = new NetSuiteInputProperties()
        /*
         * .setRole()
         * .setBodyFieldsOnly()
         * .setAccount()
         * .setEmail()
         * .setEndpoint()
         * .setRecordType()
         * .setSearchCondition()
         * .setEnableCustomization()
         * .setApplicationId()
         * .setPassword()
         */;

        // We create the component mapper instance using the configuration filled above
        final Mapper mapper = COMPONENT_FACTORY.createMapper(NetSuiteInputMapper.class, configuration);

        // Collect the source as a list
        assertEquals(asList(/* TODO - give the expected data */), COMPONENT_FACTORY.collectAsList(JsonObject.class, mapper));
    }

}