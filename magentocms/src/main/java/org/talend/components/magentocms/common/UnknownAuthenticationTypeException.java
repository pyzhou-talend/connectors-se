package org.talend.components.magentocms.common;

public class UnknownAuthenticationTypeException extends Exception {

    public UnknownAuthenticationTypeException() {
        super("Unknown or unset Authentication type");
    }
}
