package org.talend.components.netsuite.runtime;

import org.talend.components.netsuite.datastore.NetsuiteDataStore;
import org.talend.components.netsuite.runtime.client.NetSuiteClientFactory;
import org.talend.components.netsuite.runtime.client.NetSuiteException;
import org.talend.sdk.component.api.service.asyncvalidation.ValidationResult;

import lombok.Data;

/**
 * Base class for all implementations of {@link NetSuiteRuntime}.
 *
 * Each version of NetSuite runtime should provide concrete implementation of this class.
 */

@Data
public abstract class AbstractNetSuiteRuntime implements NetSuiteRuntime {

    protected NetSuiteClientFactory<?> clientFactory;

    protected NetSuiteRuntime.Context context;

    @Override
    public NetSuiteDatasetRuntime getDatasetRuntime(NetsuiteDataStore properties) {
        NetSuiteEndpoint endpoint = getEndpoint(context, properties);
        return new NetSuiteDatasetRuntimeImpl(endpoint.getClientService().getMetaDataSource());
    }

    @Override
    public ValidationResult validateConnection(NetsuiteDataStore properties) {
        try {
            NetSuiteEndpoint endpoint = getEndpoint(context, properties);
            endpoint.getClientService();
            return new ValidationResult();
        } catch (NetSuiteException e) {
            // return ComponentExceptions.exceptionToValidationResult(e);
            throw new RuntimeException();
        }
    }

    /**
     * Get NetSuite endpoint object using given context and connection properties.
     *
     * <p>
     * If context specifies that caching is enabled then the method first tries to
     * load endpoint object from context. If endpoint object for given connection configuration
     * doesn't exist then the method creates new endpoint object and stores it in context.
     *
     * @param context context
     * @param properties connection properties
     * @return endpoint object
     * @throws NetSuiteException if an error occurs during obtaining of endpoint object
     */
    protected NetSuiteEndpoint getEndpoint(final NetSuiteRuntime.Context context, final NetsuiteDataStore properties)
            throws NetSuiteException {

        // Create connection configuration for given connection properties.
        NetSuiteEndpoint.ConnectionConfig connectionConfig = NetSuiteEndpoint.createConnectionConfig(properties);

        NetSuiteEndpoint endpoint = null;
        // If caching is enabled then we should first try to get cached endpoint object.
        if (context != null && context.isCachingEnabled()) {
            NetSuiteEndpoint.ConnectionConfig cachedConnectionConfig = (NetSuiteEndpoint.ConnectionConfig) context
                    .getAttribute(NetSuiteEndpoint.ConnectionConfig.class.getName());
            // If any of key properties in connection properties was changed then
            // we should not use this cached object, we should create new.
            if (cachedConnectionConfig != null && connectionConfig.equals(cachedConnectionConfig)) {
                endpoint = (NetSuiteEndpoint) context.getAttribute(NetSuiteEndpoint.class.getName());
            }
        }
        if (endpoint == null) {
            endpoint = new NetSuiteEndpoint(clientFactory, NetSuiteEndpoint.createConnectionConfig(properties));
            if (context != null && context.isCachingEnabled()) {
                // Store connection configuration and endpoint in context.
                context.setAttribute(NetSuiteEndpoint.class.getName(), endpoint);
                context.setAttribute(NetSuiteEndpoint.ConnectionConfig.class.getName(), endpoint.getConnectionConfig());
            }
        }

        return endpoint;
    }
}