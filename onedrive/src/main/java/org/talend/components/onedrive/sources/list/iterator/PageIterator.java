package org.talend.components.onedrive.sources.list.iterator;

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
            try {
                pageWrapper = pageWrapper.getNextPageWrapper();
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
            if (pageWrapper != null)
                res = pageWrapper.next();
        }
        return res;
    }
}
