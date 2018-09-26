package org.talend.components.onedrive.sources.list.iterator;

import com.microsoft.graph.models.extensions.DriveItem;
import org.talend.components.onedrive.common.UnknownAuthenticationTypeException;
import org.talend.components.onedrive.service.http.BadCredentialsException;
import org.talend.components.onedrive.service.http.OneDriveHttpClientService;

import java.io.IOException;
import java.util.Iterator;

public class DriveItemWrapper implements Iterator<DriveItem> {

    private DriveItem driveItem;

    private Iterator<DriveItem> pages;

    private OneDriveHttpClientService oneDriveHttpClientService;

    public DriveItemWrapper(OneDriveHttpClientService oneDriveHttpClientService, DriveItem driveItem)
            throws IOException, BadCredentialsException, UnknownAuthenticationTypeException {
        this.driveItem = driveItem;
        this.oneDriveHttpClientService = oneDriveHttpClientService;
        if (driveItem != null)
            pages = new PageIterator(
                    new PageWrapper(oneDriveHttpClientService, oneDriveHttpClientService.getItemChildrens(driveItem)));
    }

    public DriveItem getDriveItem() {
        return driveItem;
    }

    @Override
    public boolean hasNext() {
        return false;
    }

    @Override
    public DriveItem next() {
        return pages == null ? null : pages.next();
    }
}
