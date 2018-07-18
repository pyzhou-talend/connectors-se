package org.talend.components.netsuite.runtime.client;

/**
 * Responsible for creation of NetSuite client.
 *
 * @param <T> type of NetSuite web service port
 */
public interface NetSuiteClientFactory<T> {

    /**
     * Create NetSuite client.
     *
     * @return NetSuite client
     * @throws NetSuiteException if error occurs during creation
     */
    NetSuiteClientService<T> createClient() throws NetSuiteException;

    /**
     * Get version of NetSuite runtime.
     *
     * @return version of NetSuite
     */
    NetSuiteVersion getApiVersion();

}