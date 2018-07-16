package org.talend.components.netsuite.runtime;

import java.util.concurrent.TimeUnit;

public abstract class NetSuiteClientService<PortT> {

    private static final long DEFAULT_CONNECTION_TIMEOUT = TimeUnit.SECONDS.toMicros(60);

    private static final long DEFAULT_RECEIVE_TIMEOUT = TimeUnit.SECONDS.toMicros(180);

    private static final int DEFAULT_SEARCH_PAGE_SIZE = 100;

    private boolean isLoggedIn = false;

    protected int searchPageSize = DEFAULT_SEARCH_PAGE_SIZE;

    private PortT port;

    protected NetSuiteClientService() {
        String prefix = null;
        try {
            prefix = Class.forName("com.sun.xml.bind.v2.runtime.JAXBContextImpl").getName();
        } catch (ClassNotFoundException e) {
            try {
                prefix = Class.forName("com.sun.xml.internal.bind.v2.runtime.JAXBContextImpl").getName();
            } catch (ClassNotFoundException e1) {
                // ignore
            }
        }
        if (prefix != null) {
            // Disable eager initialization of JAXBContext
            System.setProperty(prefix + ".fastBoot", "true");
        }
    }

    public void login() {
        login(false);
    }

    private void login(boolean isRelogin) {
        if (isRelogin) {
            isLoggedIn = false;
        }

        if (isLoggedIn) {
            return;
        }
        doLogin(isRelogin);
    }

    protected abstract void doLogin(boolean isRelogin);
}
