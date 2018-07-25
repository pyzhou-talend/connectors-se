package org.talend.components.netsuite.runtime;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.ToString;

/**
 * NetSuite specific implementation of <code>ErrorCode</code>.
 */

@Data
@AllArgsConstructor
@ToString
@EqualsAndHashCode
public class NetSuiteErrorCode {

    public static final String PRODUCT_TALEND_COMPONENTS = "TCOMP";

    public static final String GROUP_COMPONENT_NETSUITE = "NETSUITE";

    public static final String INTERNAL_ERROR = "INTERNAL_ERROR";

    public static final String CLIENT_ERROR = "CLIENT_ERROR";

    private String code;

    private int httpStatusCode;

    private final List<String> expectedContextEntries;

    public NetSuiteErrorCode(String code) {
        this(code, 500, Collections.<String> emptyList());
    }

    public NetSuiteErrorCode(String code, String... contextEntries) {
        this(code, 500, Arrays.asList(contextEntries));
    }

    public NetSuiteErrorCode(String code, int httpStatusCode, String... contextEntries) {
        this(code, httpStatusCode, Arrays.asList(contextEntries));
    }

    public String getProduct() {
        return PRODUCT_TALEND_COMPONENTS;
    }

    public String getGroup() {
        return GROUP_COMPONENT_NETSUITE;
    }
}