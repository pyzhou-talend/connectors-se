/*
 * Copyright (C) 2006-2019 Talend Inc. - www.talend.com
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */

package org.talend.components.azure.common.excel;

import java.io.Serializable;

import org.talend.components.azure.common.Encoding;
import org.talend.sdk.component.api.configuration.Option;
import org.talend.sdk.component.api.configuration.condition.ActiveIf;
import org.talend.sdk.component.api.configuration.condition.ActiveIfs;
import org.talend.sdk.component.api.configuration.constraint.Min;
import org.talend.sdk.component.api.configuration.ui.layout.GridLayout;
import org.talend.sdk.component.api.meta.Documentation;

import lombok.Data;

@GridLayout({ @GridLayout.Row("excelFormat"), @GridLayout.Row("sheetName"), @GridLayout.Row("encoding"),
        @GridLayout.Row("customEncoding"), @GridLayout.Row("useHeader"), @GridLayout.Row("header"), @GridLayout.Row("useFooter"),
        @GridLayout.Row("footer") })
@Data
public class ExcelFormatOptions implements Serializable {

    @Option
    @Documentation("Excel format")
    private ExcelFormat excelFormat = ExcelFormat.EXCEL2007;

    @Option
    @ActiveIf(target = "excelFormat", value = { "EXCEL2007", "EXCEL97" })
    @Documentation("Sheet name")
    private String sheetName;

    @Option
    @ActiveIf(target = "excelFormat", value = "HTML")
    @Documentation("Encoding")
    private Encoding encoding = Encoding.UFT8;

    @Option
    @ActiveIfs({ @ActiveIf(target = "excelFormat", value = "HTML"), @ActiveIf(target = "encoding", value = "OTHER") })
    @Documentation("Encoding")
    private String customEncoding;

    @Option
    @ActiveIf(target = "excelFormat", value = { "EXCEL2007", "EXCEL97" })
    @Documentation("Set header size")
    private boolean useHeader;

    @Option
    @ActiveIf(target = "useHeader", value = "true")
    @Documentation("Header size")
    @Min(-0.0)
    private int header = 0;

    @Option
    @ActiveIf(target = "excelFormat", value = { "EXCEL2007", "EXCEL97" })
    @Documentation("Set footer size")
    private boolean useFooter;

    @Option
    @ActiveIf(target = "useFooter", value = "true")
    @Documentation("Footer size")
    @Min(-0.0)
    private int footer = 0;
}
