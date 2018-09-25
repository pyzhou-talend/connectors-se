package org.talend.components.onedrive.common;

public class UnknownAuthenticationTypeException extends Exception {

    public UnknownAuthenticationTypeException() {
        super("Unknown or unset Authentication type");
    }
}
