package org.talend.components.azure.output;

import org.junit.ClassRule;
import org.junit.Test;
import org.talend.components.azure.common.FileFormat;
import org.talend.components.azure.common.excel.ExcelFormat;
import org.talend.components.azure.common.excel.ExcelFormatOptions;
import org.talend.components.azure.common.exception.BlobRuntimeException;
import org.talend.components.azure.dataset.AzureBlobDataset;
import org.talend.components.azure.service.MessageService;
import org.talend.sdk.component.junit.SimpleComponentRule;

public class BlobOutputTest {

    @ClassRule
    public static final SimpleComponentRule COMPONENT = new SimpleComponentRule("org.talend.components.azure");

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