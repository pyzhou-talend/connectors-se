package org.talend.components.azure.output;

import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;

import java.io.IOException;

import org.junit.ClassRule;
import org.junit.Ignore;
import org.junit.Test;
import org.talend.components.azure.common.FileFormat;
import org.talend.components.azure.common.excel.ExcelFormat;
import org.talend.components.azure.common.excel.ExcelFormatOptions;
import org.talend.components.azure.common.exception.BlobRuntimeException;
import org.talend.components.azure.dataset.AzureBlobDataset;
import org.talend.components.azure.service.MessageService;
import org.talend.sdk.component.api.service.Service;
import org.talend.sdk.component.junit.JoinInputFactory;
import org.talend.sdk.component.junit.SimpleComponentRule;
import org.talend.sdk.component.runtime.output.Processor;

public class BlobOutputTest {

    @ClassRule
    public static final SimpleComponentRule COMPONENT = new SimpleComponentRule("org.talend.components.azure");

    @Service
    public MessageService messageService;

    @Test
    @Ignore("You need to complete this test")
    public void map() throws IOException {

        // Output configuration
        // Setup your component configuration for the test here
        final BlobOutputConfiguration configuration = new BlobOutputConfiguration();

        // We create the component processor instance using the configuration filled above
        final Processor processor = COMPONENT.createProcessor(BlobOutput.class, configuration);

        // The join input factory construct inputs test data for every input branch you have defined for this component
        // Make sure to fil in some test data for the branches you want to test
        // You can also remove the branches that you don't need from the factory below
        final JoinInputFactory joinInputFactory = new JoinInputFactory().withInput("__default__",
                asList(/* TODO - list of your input data for this branch. Instances of Record.class */));

        // Run the flow and get the outputs
        final SimpleComponentRule.Outputs outputs = COMPONENT.collect(processor, joinInputFactory);

        // TODO - Test Asserts
        assertEquals(0, outputs.size()); // test of the output branches count of the component

    }

    @Test(expected = BlobRuntimeException.class)
    public void testHTMLOutputNotSupported() {
        ExcelFormatOptions excelFormatOptions = new ExcelFormatOptions();
        excelFormatOptions.setExcelFormat(ExcelFormat.HTML);

        AzureBlobDataset dataset = new AzureBlobDataset();
        dataset.setFileFormat(FileFormat.EXCEL);
        dataset.setExcelOptions(excelFormatOptions);
        BlobOutputConfiguration outputConfiguration = new BlobOutputConfiguration();
        outputConfiguration.setDataset(dataset);

        BlobOutput output = new BlobOutput(outputConfiguration, null, COMPONENT.findService(MessageService.class));
        output.init();
    }
}