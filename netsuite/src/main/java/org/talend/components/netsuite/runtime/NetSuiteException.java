package org.talend.components.netsuite.runtime;

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

}
