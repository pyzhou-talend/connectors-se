package org.talend.components.netsuite.runtime.client;

import lombok.Data;

@Data
public class NsTokenPassportSignature {

    protected String value;

    protected Algorithm algorithm;

    public enum Algorithm {
        Hmac_SHA256("HmacSHA256"),
        Hmac_SHA1("HmacSHA1");

        private final String algorithm;

        private Algorithm(String algorithm) {
            this.algorithm = algorithm;
        }

        String getAlgorithmString() {
            return this.algorithm;
        }
    }
}
