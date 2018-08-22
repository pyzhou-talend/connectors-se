package org.talend.components.netsuite.runtime.client;

import lombok.Data;

@Data
public class NsTokenPassportSignature {

    protected String value;

    protected String algorithm;
}
