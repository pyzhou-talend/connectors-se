package org.talend.components.netsuite.runtime;

import org.talend.components.netsuite.datastore.NetsuiteDataStore;
import org.talend.components.netsuite.datastore.NetsuiteDataStore.ApiVersion;
import org.talend.components.netsuite.runtime.client.MetaDataSource;
import org.talend.components.netsuite.runtime.client.NetSuiteClientFactory;
import org.talend.components.netsuite.runtime.client.NetSuiteClientService;
import org.talend.components.netsuite.runtime.client.NetSuiteCredentials;
import org.talend.components.netsuite.runtime.client.NetSuiteException;
import org.talend.components.netsuite.runtime.client.NetSuiteVersion;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;
import lombok.ToString;

/**
 * Represents NetSuite Web Service endpoint.
 */

public class NetSuiteEndpoint {

    public static final String CONNECTION = "NetSuite_Connection";

    /** Creates instance of NetSuite client. */
    private NetSuiteClientFactory<?> clientFactory;

    /** Connection configuration for this endpoint. */
    private ConnectionConfig connectionConfig;

    /** NetSuite client. */
    private NetSuiteClientService<?> clientService;

    /**
     * Creates new instance using given client factory and connection configuration.
     *
     * @param clientFactory client factory
     * @param connectionConfig connection configuration
     */
    public NetSuiteEndpoint(NetSuiteClientFactory<?> clientFactory, ConnectionConfig connectionConfig) {
        this.clientFactory = clientFactory;
        this.connectionConfig = connectionConfig;
    }

    /**
     * Create connection configuration for given connection properties.
     *
     * @param properties connection properties
     * @return connection configuration
     * @throws NetSuiteException if connection configuration not valid
     */
    public static ConnectionConfig createConnectionConfig(NetsuiteDataStore properties) throws NetSuiteException {
        String temp;
        if ((temp = properties.getApplicationId()) == null || temp.isEmpty()) {
            // throw new NetSuiteException(new NetSuiteErrorCode(NetSuiteErrorCode.CLIENT_ERROR),
            // NetSuiteRuntimeI18n.MESSAGES.getMessage("error.endpointUrlRequired"));
            throw new RuntimeException();
        }
        if ((temp = properties.getApplicationId()) == null || temp.isEmpty()) {
            // throw new NetSuiteException(new NetSuiteErrorCode(NetSuiteErrorCode.CLIENT_ERROR),
            // NetSuiteRuntimeI18n.MESSAGES.getMessage("error.apiVersionRequired"));
            throw new RuntimeException();
        }
        if ((temp = properties.getApplicationId()) == null || temp.isEmpty()) {
            // throw new NetSuiteException(new NetSuiteErrorCode(NetSuiteErrorCode.CLIENT_ERROR),
            // NetSuiteRuntimeI18n.MESSAGES.getMessage("error.emailRequired"));
            throw new RuntimeException();
        }
        if ((temp = properties.getApplicationId()) == null || temp.isEmpty()) {
            // throw new NetSuiteException(new NetSuiteErrorCode(NetSuiteErrorCode.CLIENT_ERROR),
            // NetSuiteRuntimeI18n.MESSAGES.getMessage("error.passwordRequired"));
            throw new RuntimeException();
        }
        if ((temp = properties.getApplicationId()) == null || temp.isEmpty()) {
            // throw new NetSuiteException(new NetSuiteErrorCode(NetSuiteErrorCode.CLIENT_ERROR),
            // NetSuiteRuntimeI18n.MESSAGES.getMessage("error.accountRequired"));
            throw new RuntimeException();
        }
        if (properties.getRole() == 0) {
            // throw new NetSuiteException(new NetSuiteErrorCode(NetSuiteErrorCode.CLIENT_ERROR),
            // NetSuiteRuntimeI18n.MESSAGES.getMessage("error.roleRequired"));
            throw new RuntimeException();
        }

        String endpointUrl = properties.getEndpoint();

        NetSuiteVersion endpointApiVersion;
        try {
            endpointApiVersion = NetSuiteVersion.detectVersion(endpointUrl);
        } catch (IllegalArgumentException e) {
            // TODO: Exception
            // throw new NetSuiteException(new NetSuiteErrorCode(NetSuiteErrorCode.CLIENT_ERROR),
            // NetSuiteRuntimeI18n.MESSAGES.getMessage("error.couldNotDetectApiVersionFromEndpointUrl",
            // endpointUrl));
            throw new RuntimeException();
        }
        ApiVersion apiVersionString = properties.getApiVersion();
        NetSuiteVersion apiVersion;
        try {
            apiVersion = NetSuiteVersion.parseVersion(apiVersionString);
        } catch (IllegalArgumentException e) {
            // throw new NetSuiteException(new NetSuiteErrorCode(NetSuiteErrorCode.CLIENT_ERROR),
            // NetSuiteRuntimeI18n.MESSAGES.getMessage("error.invalidApiVersion", apiVersionString));
            throw new RuntimeException();
        }

        if (!endpointApiVersion.isSameMajor(apiVersion)) {
            // throw new NetSuiteException(new NetSuiteErrorCode(NetSuiteErrorCode.CLIENT_ERROR),
            // NetSuiteRuntimeI18n.MESSAGES.getMessage("error.endpointUrlApiVersionMismatch", endpointUrl,
            // apiVersionString));
            throw new RuntimeException();
        }
        if (apiVersion.getMajorYear() >= 2015 && (temp = properties.getApplicationId()) == null || temp.isEmpty()) {
            // throw new NetSuiteException(new NetSuiteErrorCode(NetSuiteErrorCode.CLIENT_ERROR),
            // NetSuiteRuntimeI18n.MESSAGES.getMessage("error.applicationIdRequired"));
            throw new RuntimeException();
        }

        String email = properties.getEmail();
        String password = properties.getPassword();
        Integer roleId = properties.getRole();
        String account = properties.getAccount();
        String applicationId = properties.getApplicationId();
        Boolean customizationEnabled = properties.isEnableCustomization();

        NetSuiteCredentials credentials = new NetSuiteCredentials();
        credentials.setEmail(email);
        credentials.setPassword(password);
        credentials.setRoleId(roleId.toString());
        credentials.setAccount(account);
        credentials.setApplicationId(applicationId);

        ConnectionConfig connectionConfig = new ConnectionConfig(endpointUrl, apiVersion.getMajor(), credentials);
        // connectionConfig.setReferenceComponentId(properties.getReferencedComponentId());
        // No shared connection in tacokit.
        connectionConfig.setCustomizationEnabled(customizationEnabled);
        return connectionConfig;
    }

    /**
     * Connect to NetSuite remote endpoint.
     *
     * @return NetSuite client
     * @throws NetSuiteException if an error occurs during connecting
     */
    public NetSuiteClientService<?> connect() throws NetSuiteException {
        clientService = connect(connectionConfig);

        return clientService;
    }

    public ConnectionConfig getConnectionConfig() {
        return connectionConfig;
    }

    /**
     * Return NetSuite client.
     *
     * <p>
     * If endpoint is not yet connected then the method creates client and
     * connects ({@link #connect()}) to NetSuite.
     *
     * @return client
     * @throws NetSuiteException if an error occurs during connecting
     */
    public NetSuiteClientService<?> getClientService() throws NetSuiteException {
        return clientService == null ? connect() : clientService;
    }

    /**
     * Creates new NetSuite client and connects to NetSuite remote endpoint.
     *
     * @param connectionConfig connection configuration
     * @return client
     * @throws NetSuiteException if an error occurs during connecting
     */
    private NetSuiteClientService<?> connect(ConnectionConfig connectionConfig) throws NetSuiteException {

        NetSuiteClientService<?> clientService = clientFactory.createClient();
        clientService.setEndpointUrl(connectionConfig.getEndpointUrl());
        clientService.setCredentials(connectionConfig.getCredentials());
        MetaDataSource metaDataSource = clientService.getMetaDataSource();
        metaDataSource.setCustomizationEnabled(connectionConfig.isCustomizationEnabled());

        clientService.login();

        return clientService;
    }

    public MetaDataSource getMetaDataSource() {
        return clientService.getMetaDataSource();
    }

    /**
     * Holds configuration for connecting to NetSuite.
     */
    @Data
    @EqualsAndHashCode
    @ToString
    @NoArgsConstructor
    @AllArgsConstructor
    public static class ConnectionConfig {

        private String endpointUrl;

        private NetSuiteVersion apiVersion;

        private NetSuiteCredentials credentials;

        private boolean customizationEnabled;

        public ConnectionConfig(String endpointUrl, NetSuiteVersion apiVersion, NetSuiteCredentials credentials) {
            this.endpointUrl = endpointUrl;
            this.apiVersion = apiVersion;
            this.credentials = credentials;
        }
    }
}