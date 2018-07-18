package org.talend.components.netsuite.runtime.client;

import java.util.List;

import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.ToString;

/**
 * Holds search result's data.
 *
 * <p>
 * This data object is mirror of NetSuite's native {@code SearchResult} data object.
 *
 * @param <RecT> type of record data object
 */

@Data
@NoArgsConstructor
@ToString
public class NsSearchResult<RecT> {

    /** Status of 'search' operation. */
    private NsStatus status;

    private Integer totalRecords;

    private Integer pageSize;

    private Integer totalPages;

    private Integer pageIndex;

    private String searchId;

    private List<RecT> recordList;

    public NsSearchResult(NsStatus status) {
        this.status = status;
    }

    public boolean isSuccess() {
        return status.isSuccess();
    }
}
