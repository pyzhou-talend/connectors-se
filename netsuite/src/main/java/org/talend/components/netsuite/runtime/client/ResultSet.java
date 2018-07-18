package org.talend.components.netsuite.runtime.client;

/**
 * Responsible for sequential retrieving of result data objects.
 */
public abstract class ResultSet<T> {

    /**
     * Advance to next result.
     *
     * @return {@code} true if result is available, {@code false} otherwise
     * @throws NetSuiteException if an error occurs during retrieving of results
     */
    public abstract boolean next() throws NetSuiteException;

    /**
     * Get last read result.
     *
     * @return result object
     * @throws NetSuiteException if an error occurs during retrieving of results
     */
    public abstract T get() throws NetSuiteException;
}
