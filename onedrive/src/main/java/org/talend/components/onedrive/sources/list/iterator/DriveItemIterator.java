package org.talend.components.onedrive.sources.list.iterator;

import com.microsoft.graph.models.extensions.DriveItem;
import org.talend.components.onedrive.common.OneDriveDataStore;
import org.talend.components.onedrive.common.UnknownAuthenticationTypeException;
import org.talend.components.onedrive.service.http.BadCredentialsException;
import org.talend.components.onedrive.service.http.OneDriveHttpClientService;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;

public class DriveItemIterator implements Iterator<DriveItem> {

    private Iterator<DriveItem> driveItemIterator;

    private DriveItemWrapper driveItemWrapper;

    private OneDriveHttpClientService oneDriveHttpClientService;

    private OneDriveDataStore dataStore;

    public DriveItemIterator(OneDriveDataStore dataStore, OneDriveHttpClientService oneDriveHttpClientService,
            List<DriveItem> items) throws UnknownAuthenticationTypeException, IOException, BadCredentialsException {
        this.oneDriveHttpClientService = oneDriveHttpClientService;
        this.dataStore = dataStore;
        if (items == null || items.isEmpty()) {
            driveItemIterator = null;
        }

        driveItemIterator = items.iterator();
        // post processing
        if (driveItemIterator.hasNext()) {
            driveItemWrapper = new DriveItemWrapper(dataStore, oneDriveHttpClientService, driveItemIterator.next());
        }
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
                try {
                    driveItemWrapper = new DriveItemWrapper(dataStore, oneDriveHttpClientService, driveItemIterator.next());
                } catch (IOException e) {
                    e.printStackTrace();
                } catch (BadCredentialsException e) {
                    e.printStackTrace();
                } catch (UnknownAuthenticationTypeException e) {
                    e.printStackTrace();
                }
            } else {
                driveItemWrapper = null;
            }
        }
        return res;
    }
}
