package org.talend.components.magentocms.helpers.authhandlers;

import org.talend.components.magentocms.common.MagentoDataStore;
import org.talend.components.magentocms.common.UnknownAuthenticationTypeException;
import org.talend.components.magentocms.service.http.BadCredentialsException;

import java.io.IOException;

public interface AuthorizationHandler {

    String getAuthorization(MagentoDataStore magentoDataStore)
            throws IOException, UnknownAuthenticationTypeException, BadCredentialsException;
}
