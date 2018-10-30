package org.talend.components.netsuite.runtime.client;

import org.talend.components.netsuite.runtime.NetSuiteErrorCode;

/**
 * Thrown when NetSuite related error occurs.
 *
 * @see org.talend.components.netsuite.NetSuiteErrorCode
 */
public class NetSuiteException extends RuntimeException {

    public NetSuiteException(String message) {
        super(message);
    }

    public NetSuiteException(String message, Throwable cause) {
        super(message, cause);
    }

    public NetSuiteException(NetSuiteErrorCode code, String message) {
        this(code.getCode() + ":" + message == null ? "" : message);
    }

    public NetSuiteException(NetSuiteErrorCode code, String message, Throwable cause) {
        this(code.getCode() + ":" + message == null ? "" : message, cause);
    }
}
