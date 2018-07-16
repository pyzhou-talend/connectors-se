package org.talend.components.netsuite.source;

import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;

import java.io.IOException;

import javax.json.JsonObject;

import org.junit.ClassRule;
import org.junit.Ignore;
import org.junit.Test;
import org.talend.components.netsuite.dataset.NetsuiteInputDataSet;
import org.talend.sdk.component.junit.SimpleComponentRule;
import org.talend.sdk.component.runtime.input.Mapper;

public class NetsuiteInputMapperTest {

    @ClassRule
    public static final SimpleComponentRule COMPONENT_FACTORY = new SimpleComponentRule("org.talend.components.netsuite");

    @Test
    @Ignore("You need to complete this test")
    public void produce() throws IOException {

        // Source configuration
        // Setup your component configuration for the test here
        final NetsuiteInputDataSet configuration = new NetsuiteInputDataSet()
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
        final Mapper mapper = COMPONENT_FACTORY.createMapper(NetsuiteInputMapper.class, configuration);

        // Collect the source as a list
        assertEquals(asList(/* TODO - give the expected data */), COMPONENT_FACTORY.collectAsList(JsonObject.class, mapper));
    }

}