package org.talend.components.magentocms.helpers.authhandlers;

import oauth.signpost.exception.OAuthCommunicationException;
import oauth.signpost.exception.OAuthExpectationFailedException;
import oauth.signpost.exception.OAuthMessageSignerException;
import oauth.signpost.http.HttpRequest;
import org.talend.components.magentocms.common.MagentoCmsConfigurationBase;
import org.talend.components.magentocms.common.UnknownAuthenticationTypeException;
import org.talend.components.magentocms.service.http.BadCredentialsException;

import java.io.IOException;

public interface AuthorizationHandler {

    void setAuthorization(HttpRequest httpRequest, MagentoCmsConfigurationBase magentoCmsConfigurationBase)
            throws IOException, OAuthCommunicationException, OAuthExpectationFailedException, OAuthMessageSignerException,
            UnknownAuthenticationTypeException, BadCredentialsException;

    String getAuthorization(MagentoCmsConfigurationBase magentoCmsConfigurationBase)
            throws IOException, OAuthCommunicationException, OAuthExpectationFailedException, OAuthMessageSignerException,
            UnknownAuthenticationTypeException, BadCredentialsException;
}
