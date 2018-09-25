package org.talend.components.onedrive.input.list.iterator;

import com.microsoft.graph.models.extensions.DriveItem;
import org.talend.components.onedrive.service.http.OneDriveHttpClientService;

import java.util.Iterator;
import java.util.List;

public class DriveItemIterator implements Iterator<DriveItem> {

    private Iterator<DriveItem> driveItemIterator;

    private DriveItemWrapper driveItemWrapper;

    private OneDriveHttpClientService oneDriveHttpClientService;

    public DriveItemIterator(OneDriveHttpClientService oneDriveHttpClientService, List<DriveItem> items) {
        this.oneDriveHttpClientService = oneDriveHttpClientService;
        if (items == null || items.isEmpty()) {
            driveItemIterator = null;
        }

        driveItemIterator = items.iterator();
        // post processing
        driveItemWrapper = new DriveItemWrapper(oneDriveHttpClientService, driveItemIterator.next());
    }

    @Override
    public boolean hasNext() {
        return false;
    }

    // pre processing
    // @Override
    // public DriveItem next() {
    // if (driveItemWrapper == null) {
    // DriveItem driveItem = driveItemIterator.next();
    // if (driveItem == null) {
    // return null;
    // }
    // driveItemWrapper = new DriveItemWrapper(driveItem);
    // return driveItem;
    // }
    //
    // DriveItem res = driveItemWrapper.next();
    // if (res == null) {
    // if (driveItemIterator.hasNext()) {
    // res = driveItemIterator.next();
    // driveItemWrapper = new DriveItemWrapper(res);
    // }
    // }
    // return res;
    // }

    // post processing
    @Override
    public DriveItem next() {
        if (driveItemIterator == null) {
            return null;
        }

        if (driveItemWrapper == null) {
            return null;
        }

        DriveItem res = driveItemWrapper.next();
        if (res == null) {
            res = driveItemWrapper.getDriveItem();
            if (driveItemIterator.hasNext()) {
                driveItemWrapper = new DriveItemWrapper(oneDriveHttpClientService, driveItemIterator.next());
            } else {
                driveItemWrapper = null;
            }
        }
        return res;
    }
}
