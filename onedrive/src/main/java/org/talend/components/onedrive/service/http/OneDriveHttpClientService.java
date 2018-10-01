package org.talend.components.onedrive.service.http;

import com.microsoft.graph.concurrency.ChunkedUploadProvider;
import com.microsoft.graph.concurrency.IProgressCallback;
import com.microsoft.graph.core.ClientException;
import com.microsoft.graph.http.GraphServiceException;
import com.microsoft.graph.models.extensions.DriveItem;
import com.microsoft.graph.models.extensions.DriveItemUploadableProperties;
import com.microsoft.graph.models.extensions.Folder;
import com.microsoft.graph.models.extensions.UploadSession;
import com.microsoft.graph.requests.extensions.IDriveItemCollectionPage;
import com.microsoft.graph.requests.extensions.IDriveRequestBuilder;
import lombok.extern.slf4j.Slf4j;
import org.talend.components.onedrive.common.OneDriveDataStore;
import org.talend.components.onedrive.common.UnknownAuthenticationTypeException;
import org.talend.components.onedrive.helpers.AuthorizationHelper;
import org.talend.components.onedrive.service.graphclient.GraphClientService;
import org.talend.components.onedrive.sources.get.OneDriveGetConfiguration;
import org.talend.components.onedrive.sources.list.OneDriveObjectType;
import org.talend.sdk.component.api.service.Service;

import javax.json.JsonBuilderFactory;
import javax.json.JsonObject;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Base64;

@Service
@Slf4j
public class OneDriveHttpClientService {

    private final int ERROR_CODE_ITEM_NOT_FOUND = 404;

    @Service
    private AuthorizationHelper authorizationHelper = null;

    @Service
    private GraphClientService graphClientService = null;

    @Service
    private JsonBuilderFactory jsonBuilderFactory = null;

    private IDriveRequestBuilder getDriveRequestBuilder() {
        IDriveRequestBuilder driveRequestBuilder = graphClientService.getGraphClient().me().drive();
        return driveRequestBuilder;
    }

    // public IDriveItemCollectionPage getRootChildrens(OneDriveDataStore dataStore)
    // throws BadCredentialsException, IOException, UnknownAuthenticationTypeException {
    // System.out.println("get root chilren");
    // graphClientService.setAccessToken(authorizationHelper.getAuthorization(dataStore));
    // IDriveItemCollectionPage pages = getDriveRequestBuilder().root().children().buildRequest().get();
    // return pages;
    // }

    public DriveItem getRoot(OneDriveDataStore dataStore)
            throws BadCredentialsException, IOException, UnknownAuthenticationTypeException {
        System.out.println("get root");
        graphClientService.setAccessToken(authorizationHelper.getAuthorization(dataStore));
        DriveItem root = getDriveRequestBuilder().root().buildRequest().get();
        return root;
    }

    public DriveItem getItem(OneDriveDataStore dataStore, String itemId)
            throws BadCredentialsException, IOException, UnknownAuthenticationTypeException {
        System.out.println("get item");
        graphClientService.setAccessToken(authorizationHelper.getAuthorization(dataStore));
        DriveItem item = getDriveRequestBuilder().items(itemId).buildRequest().get();
        return item;
    }

    public IDriveItemCollectionPage getItemChildrens(OneDriveDataStore dataStore, DriveItem parent)
            throws BadCredentialsException, IOException, UnknownAuthenticationTypeException {
        System.out.println("get item's chilren: " + (parent == null ? null : parent.name));
        graphClientService.setAccessToken(authorizationHelper.getAuthorization(dataStore));
        IDriveItemCollectionPage pages = getDriveRequestBuilder().items(parent.id).children().buildRequest().get();
        return pages;
    }

    public DriveItem getItemByPath(OneDriveDataStore dataStore, String path)
            throws IOException, BadCredentialsException, UnknownAuthenticationTypeException {
        System.out.println("get item by path: " + path);
        DriveItem driveItem;
        if (path == null || path.isEmpty()) {
            driveItem = getRoot(dataStore);
        } else {
            graphClientService.setAccessToken(authorizationHelper.getAuthorization(dataStore));
            driveItem = getDriveRequestBuilder().root().itemWithPath(path).buildRequest().get();
        }
        return driveItem;
    }

    /**
     * Create file or folder. Use '/' as a path delimiter
     *
     * @param parentId - parent item id
     * @param objectType - File or Directory
     * @param itemPath - full path to new item relatively to parent
     * @throws BadCredentialsException
     * @throws IOException
     * @throws UnknownAuthenticationTypeException
     */
    public DriveItem createItem(OneDriveDataStore dataStore, String parentId, OneDriveObjectType objectType, String itemPath)
            throws BadCredentialsException, IOException, UnknownAuthenticationTypeException {
        System.out.println("create item: " + (parentId == null ? "root" : parentId));
        if (itemPath == null || itemPath.isEmpty()) {
            return null;
        }
        graphClientService.setAccessToken(authorizationHelper.getAuthorization(dataStore));

        if (parentId == null || parentId.isEmpty()) {
            parentId = getRoot(dataStore).id;
        }

        // create parent folders
        String[] pathParts = itemPath.split("/");
        for (int i = 0; i < pathParts.length - 1; i++) {
            String objName = pathParts[i];
            DriveItem parentItem = null;
            try {
                parentItem = getDriveRequestBuilder().items(parentId).itemWithPath(objName).buildRequest().get();
                parentId = parentItem.id;
            } catch (GraphServiceException e) {
                if (e.getResponseCode() != ERROR_CODE_ITEM_NOT_FOUND) {
                    throw e;
                }
            }
            if (parentItem == null) {
                DriveItem objectToCreate = new DriveItem();
                objectToCreate.name = objName;
                objectToCreate.folder = new Folder();
                parentId = getDriveRequestBuilder().items(parentId).children().buildRequest().post(objectToCreate).id;
            }
            System.out.println("new item " + parentId + " was created");
        }

        // create item (file or folder)
        String itemName = pathParts[pathParts.length - 1];
        DriveItem newItem = null;
        // check if the item exists
        try {
            newItem = getDriveRequestBuilder().items(parentId).itemWithPath(itemName).buildRequest().get();
        } catch (GraphServiceException e) {
            if (e.getResponseCode() != ERROR_CODE_ITEM_NOT_FOUND) {
                throw e;
            }
        }
        // create if the item does not exist
        if (newItem == null) {
            DriveItem objectToCreate = new DriveItem();
            objectToCreate.name = itemName;
            if (objectType == OneDriveObjectType.DIRECTORY) {
                objectToCreate.folder = new Folder();
            } else {
                objectToCreate.file = new com.microsoft.graph.models.extensions.File();
            }
            newItem = getDriveRequestBuilder().items(parentId).children().buildRequest().post(objectToCreate);
        }

        System.out.println("new item " + newItem.name + " was created");
        return newItem;
    }

    /**
     * Delete file or folder
     *
     * @param itemId - id of the deleted item
     * @throws BadCredentialsException
     * @throws IOException
     * @throws UnknownAuthenticationTypeException
     */
    public void deleteItem(OneDriveDataStore dataStore, String itemId)
            throws BadCredentialsException, IOException, UnknownAuthenticationTypeException {
        System.out.println("delete item: " + itemId);
        if (itemId == null || itemId.isEmpty()) {
            return;
        }

        graphClientService.setAccessToken(authorizationHelper.getAuthorization(dataStore));

        getDriveRequestBuilder().items(itemId).buildRequest().delete();

        System.out.println("item " + itemId + " was deleted");
    }

    public JsonObject getItemData(OneDriveGetConfiguration configuration, String itemId)
            throws BadCredentialsException, IOException, UnknownAuthenticationTypeException {
        System.out.println("get item data: " + itemId);
        if (itemId == null || itemId.isEmpty()) {
            return null;
        }

        graphClientService.setAccessToken(authorizationHelper.getAuthorization(configuration.getDataStore()));

        // get item data
        DriveItem item = getItem(configuration.getDataStore(), itemId);
        // check if it is a file
        // if (item.file == null) {
        // return null;
        // }

        boolean isFolder = (item.folder != null);
        boolean isFile = (item.file != null);

        // hashes
        if (isFile) {
            String quickXorHash = item.file.hashes.quickXorHash;
            String crc32Hash = item.file.hashes.crc32Hash;
            String sha1Hash = item.file.hashes.sha1Hash;
        }

        // get parent paths
        String parentPath = item.parentReference.path;
        final String pathStart = "/drive/root:";
        if (parentPath.startsWith(pathStart)) {
            parentPath = parentPath.substring(pathStart.length());
        }
        if (parentPath.startsWith("/")) {
            parentPath = parentPath.substring(1);
        }

        //
        JsonObject res = graphClientService.driveItemToJson(item);

        InputStream inputStream = getDriveRequestBuilder().items(itemId).content().buildRequest().get();
        if (configuration.isStoreFilesLocally()) {
            String storeDir = configuration.getStoreDirectory();
            String fileName = storeDir + "/" + parentPath + "/" + item.name;
            if (isFolder) {
                File directory = new File(fileName);
                if (!directory.exists()) {
                    if (!directory.mkdirs()) {
                        throw new RuntimeException("Could not create directory: " + fileName);
                    }
                }
            } else if (isFile) {
                int totalBytes = 0;
                System.out.println("getItemData. fileName: " + fileName);
                File newFile = new File(fileName);
                // create parent dir
                if (!newFile.getParentFile().exists()) {
                    if (!newFile.getParentFile().mkdirs()) {
                        throw new RuntimeException("Could not create directory: " + newFile.getParentFile());
                    }
                }
                try (OutputStream outputStream = new FileOutputStream(newFile)) {
                    int read = 0;
                    byte[] bytes = new byte[1024 * 1024];
                    while ((read = inputStream.read(bytes)) != -1) {
                        totalBytes += read;
                        outputStream.write(bytes, 0, read);
                        System.out.println("progress: " + fileName + ": " + totalBytes + ":" + item.size);
                    }
                } catch (IOException e) {
                    e.printStackTrace();
                } finally {
                    if (inputStream != null) {
                        try {
                            inputStream.close();
                        } catch (IOException e) {
                            e.printStackTrace();
                        }
                    }
                }
            }
        } else {
            if (isFile) {
                int fileSize = item.size.intValue();
                if (fileSize > Integer.MAX_VALUE) {
                    throw new RuntimeException("The file is bigger than " + Integer.MAX_VALUE + " bytes!");
                }
                int totalBytes = 0;
                try (ByteArrayOutputStream outputStream = new ByteArrayOutputStream()) {
                    int read = 0;
                    byte[] bytes = new byte[1024 * 1024];
                    while ((read = inputStream.read(bytes)) != -1) {
                        totalBytes += read;
                        outputStream.write(bytes, 0, read);
                        // System.out.println("progress: " + fileName + ": " + totalBytes + ":" + item.size);
                    }
                    byte[] allBytes = outputStream.toByteArray();
                    // res = recordBuilderFactory.newRecordBuilder().withBytes("payload", allBytes).build();
                    String allBytesBase64 = Base64.getEncoder().encodeToString(allBytes);
                    res = jsonBuilderFactory.createObjectBuilder(res).add("payload", allBytesBase64).build();
                } catch (IOException e) {
                    e.printStackTrace();
                } finally {
                    if (inputStream != null) {
                        try {
                            inputStream.close();
                        } catch (IOException e) {
                            e.printStackTrace();
                        }
                    }
                }
            }
        }

        System.out.println("item " + itemId + " was saved locally");
        return res;
    }

    public DriveItem putItemData(OneDriveDataStore dataStore, String itemPath, InputStream inputStream, int fileLength)
            throws IOException, BadCredentialsException, UnknownAuthenticationTypeException {
        System.out.println("put item data: " + itemPath);

        // InputStream inputStream = getDriveRequestBuilder().items(itemId).content().buildRequest().get();

        boolean isFile = true;
        OneDriveObjectType objectType = OneDriveObjectType.FILE;
        if (inputStream == null) {
            isFile = false;
            objectType = OneDriveObjectType.DIRECTORY;
        }

        // create item
        DriveItem newItem = createItem(dataStore, null, objectType, itemPath);
        System.out.println("new item was created: " + newItem.id);

        if (objectType == OneDriveObjectType.FILE) {
            // upload file content
            UploadSession uploadSession = getDriveRequestBuilder().items(newItem.id)
                    .createUploadSession(new DriveItemUploadableProperties()).buildRequest().post();
            // int maxChunkSize = 320 * 1024; // 320 KB - Change this to your chunk size. 5MB is the default.
            // try (InputStream inputStream = new ByteArrayInputStream(payload)) {
            ChunkedUploadProvider<DriveItem> provider = new ChunkedUploadProvider<>(uploadSession,
                    graphClientService.getGraphClient(), inputStream, fileLength, DriveItem.class);
            provider.upload(new IProgressCallback<DriveItem>() {

                @Override
                public void progress(long currentBytes, long allBytes) {
                    System.out.println("progress: " + itemPath + " -> " + currentBytes + ":" + allBytes);
                }

                @Override
                public void success(DriveItem o) {
                    System.out.println("file was uploaded: " + itemPath + ", itemId: " + o.id);
                }

                @Override
                public void failure(ClientException e) {
                    throw e;
                }
            });
        }

        return newItem;
    }

    // private void handleBadRequest400(JsonObject errorObject, String requestObject) throws BadRequestException, IOException {
    // /*
    // * process messages like this:
    // * {"message":"%fieldName is a required field.","parameters":{"fieldName":"searchCriteria"}}
    // */
    // String message = errorObject.getJsonString("message").getString();
    // if (errorObject.get("parameters") != null) {
    // if (errorObject.get("parameters").getValueType() == JsonValue.ValueType.OBJECT) {
    // for (Map.Entry<String, JsonValue> parameter : errorObject.getJsonObject("parameters").entrySet()) {
    // message = message.replaceAll("%" + parameter.getKey(), parameter.getValue().toString());
    // }
    // } else if (errorObject.get("parameters").getValueType() == JsonValue.ValueType.ARRAY) {
    // JsonArray params = errorObject.getJsonArray("parameters");
    // for (int i = 0; i < params.size(); i++) {
    // message = message.replaceAll("%" + (i + 1), params.getString(i));
    // }
    // }
    // }
    // throw new BadRequestException(
    // "An error occurred: " + message + (requestObject == null ? "" : "For object: " + requestObject));
    // }
}
