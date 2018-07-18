package org.talend.components.netsuite.runtime.client.search;

import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

import org.talend.components.netsuite.runtime.client.NetSuiteClientService;
import org.talend.components.netsuite.runtime.client.NetSuiteException;
import org.talend.components.netsuite.runtime.client.NsSearchResult;
import org.talend.components.netsuite.runtime.client.ResultSet;
import org.talend.components.netsuite.runtime.model.BasicRecordType;
import org.talend.components.netsuite.runtime.model.RecordTypeDesc;
import org.talend.components.netsuite.runtime.model.SearchRecordTypeDesc;

/**
 * Result set for search results.
 *
 * @see SearchQuery#search()
 * @see NetSuiteClientService#search(Object)
 */
public class SearchResultSet<R> extends ResultSet<R> {

    /** NetSuite client which this result set is owned by. */
    private NetSuiteClientService<?> clientService;

    /** Descriptor of target record type. */
    private RecordTypeDesc recordTypeDesc;

    /** Descriptor of target search record type. */
    private SearchRecordTypeDesc searchRecordTypeDesc;

    /** NetSuite specific identifier of search. */
    private String searchId;

    /** Current search result being processed. */
    private NsSearchResult<R> result;

    /** List of records for current search result. */
    private List<R> recordList;

    /** Iterator of records for current search result. */
    private Iterator<R> recordIterator;

    /** Last retrieved record. */
    private R current;

    public SearchResultSet(NetSuiteClientService<?> clientService, RecordTypeDesc recordTypeDesc,
            SearchRecordTypeDesc searchRecordTypeDesc, NsSearchResult<R> result) {

        this.clientService = clientService;
        this.recordTypeDesc = recordTypeDesc;
        this.searchRecordTypeDesc = searchRecordTypeDesc;
        this.result = result;

        searchId = result.getSearchId();
        recordList = prepareRecordList();
        recordIterator = recordList.iterator();
    }

    public NetSuiteClientService<?> getClientService() {
        return clientService;
    }

    public RecordTypeDesc getRecordTypeDesc() {
        return recordTypeDesc;
    }

    public SearchRecordTypeDesc getSearchRecordTypeDesc() {
        return searchRecordTypeDesc;
    }

    public String getSearchId() {
        return searchId;
    }

    @Override
    public boolean next() throws NetSuiteException {
        if (!recordIterator.hasNext() && hasMore()) {
            recordList = getMoreRecords();
            recordIterator = recordList.iterator();
        }
        if (recordIterator.hasNext()) {
            current = recordIterator.next();
            return true;
        }
        return false;
    }

    @Override
    public R get() throws NetSuiteException {
        return current;
    }

    /**
     * Check whether search has more results that can be retrieved.
     *
     * @return {@code true} if there are more results, {@code false} otherwise
     */
    protected boolean hasMore() {
        if (this.result == null) {
            return false;
        }
        if (result.getPageIndex() == null) {
            return false;
        }
        if (result.getTotalPages() == null) {
            return false;
        }
        return result.getPageIndex().intValue() < result.getTotalPages().intValue();
    }

    /**
     * Retrieve next page of search results.
     *
     * @return list of records from retrieved search result
     * @throws NetSuiteException if an error occurs during retrieval
     */
    protected List<R> getMoreRecords() throws NetSuiteException {
        if (searchId != null) {
            int nextPageIndex = result.getPageIndex().intValue() + 1;
            NsSearchResult<R> nextPageResult = clientService.searchMoreWithId(searchId, nextPageIndex);
            if (!nextPageResult.isSuccess()) {
                NetSuiteClientService.checkError(nextPageResult.getStatus());
            }
            result = nextPageResult;
            return prepareRecordList();
        }
        return Collections.emptyList();
    }

    /**
     * Filter list of records before returning to a caller.
     *
     * @return list of records ready for consuming
     */
    protected List<R> prepareRecordList() {
        List<R> recordList = result.getRecordList();
        if (recordList == null || recordList.isEmpty()) {
            return Collections.emptyList();
        }
        return Optional.ofNullable(recordList)
                .filter((recList) -> BasicRecordType.ITEM.getType().equals(searchRecordTypeDesc.getType()))
                .orElse(recordList.stream().filter(record -> record.getClass() == recordTypeDesc.getRecordClass())
                        .collect(Collectors.toCollection(LinkedList::new)));
    }

}
