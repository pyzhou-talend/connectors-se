package org.talend.components.magentocms.helpers.authhandlers;

import oauth.signpost.exception.OAuthCommunicationException;
import oauth.signpost.exception.OAuthExpectationFailedException;
import oauth.signpost.exception.OAuthMessageSignerException;
import org.talend.components.magentocms.common.AuthenticationSettings;
import org.talend.components.magentocms.common.RequestType;

import java.net.MalformedURLException;

public interface AuthorizationHandler {

    String getAuthorization(AuthenticationSettings authenticationSettings, String magentoUrl, RequestType requestType)
            throws MalformedURLException, OAuthCommunicationException, OAuthExpectationFailedException,
            OAuthMessageSignerException;
}
