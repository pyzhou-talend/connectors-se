package org.talend.components.onedrive.input.list.iterator;

import com.microsoft.graph.models.extensions.DriveItem;
import com.microsoft.graph.requests.extensions.IDriveItemCollectionPage;
import org.talend.components.onedrive.service.http.OneDriveHttpClientService;

import java.util.Iterator;

public class PageWrapper implements Iterator<DriveItem> {

    private IDriveItemCollectionPage page;

    private Iterator<DriveItem> items;

    private OneDriveHttpClientService oneDriveHttpClientService;

    public PageWrapper(OneDriveHttpClientService oneDriveHttpClientService, IDriveItemCollectionPage page) {
        this.page = page;
        if (page != null)
            items = new DriveItemIterator(oneDriveHttpClientService, page.getCurrentPage());
    }

    public PageWrapper getNextPageWrapper() {
        if (getPage() == null || getPage().getNextPage() == null)
            return null;
        return new PageWrapper(oneDriveHttpClientService, getPage().getNextPage().buildRequest().get());
    }

    public IDriveItemCollectionPage getPage() {
        return page;
    }

    public void setPage(IDriveItemCollectionPage page) {
        this.page = page;
    }

    @Override
    public boolean hasNext() {
        return false;
    }

    @Override
    public DriveItem next() {
        return items == null ? null : items.next();
    }
}
