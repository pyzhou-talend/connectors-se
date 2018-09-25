package org.talend.components.onedrive.input.list.iterator;

import com.microsoft.graph.models.extensions.DriveItem;

import java.util.Iterator;

public class PageIterator implements Iterator<DriveItem> {

    private PageWrapper pageWrapper;

    public PageIterator(PageWrapper pageWrapper) {
        this.pageWrapper = pageWrapper;
    }

    @Override
    public boolean hasNext() {
        return false;
    }

    @Override
    public DriveItem next() {
        DriveItem res = pageWrapper.next();
        if (res == null) {
            pageWrapper = pageWrapper.getNextPageWrapper();
            if (pageWrapper != null)
                res = pageWrapper.next();
        }
        return res;
    }
}
