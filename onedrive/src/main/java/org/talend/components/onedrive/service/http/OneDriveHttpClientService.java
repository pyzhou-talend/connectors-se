package org.talend.components.onedrive.service.http;

import com.microsoft.graph.http.GraphServiceException;
import com.microsoft.graph.models.extensions.DriveItem;
import com.microsoft.graph.models.extensions.Folder;
import com.microsoft.graph.requests.extensions.IDriveItemCollectionPage;
import lombok.extern.slf4j.Slf4j;
import org.talend.components.onedrive.common.UnknownAuthenticationTypeException;
import org.talend.components.onedrive.helpers.AuthorizationHelper;
import org.talend.components.onedrive.service.configuration.ConfigurationService;
import org.talend.components.onedrive.service.graphclient.GraphClientService;
import org.talend.components.onedrive.sources.list.OneDriveObjectType;
import org.talend.sdk.component.api.service.Service;

import javax.json.JsonArray;
import javax.json.JsonBuilderFactory;
import javax.json.JsonObject;
import javax.json.JsonValue;
import javax.json.stream.JsonParserFactory;
import java.io.IOException;
import java.util.List;
import java.util.Map;

@Service
@Slf4j
public class OneDriveHttpClientService {

    @Service
    private JsonParserFactory jsonParserFactory = null;

    @Service
    private JsonBuilderFactory jsonBuilderFactory = null;

    // @Service
    // private ConfigurationServiceList configurationServiceList = null;
    @Service
    private ConfigurationService configurationService = null;

    // @Service
    // private ConfigurationServiceList configurationServiceInput = null;
    //
    // @Service
    // private ConfigurationServiceList configurationServiceOutput = null;

    @Service
    private AuthorizationHelper authorizationHelper = null;

    @Service
    private GraphClientService graphClientService = null;

    public List<JsonObject> getList(String requestPath, Map<String, String> queryParameters)
            throws IOException, UnknownAuthenticationTypeException, BadRequestException, BadCredentialsException {
        return null;
    }

    public IDriveItemCollectionPage getRootChildrens()
            throws BadCredentialsException, IOException, UnknownAuthenticationTypeException {
        System.out.println("get root chilren");
        graphClientService
                .setAccessToken(authorizationHelper.getAuthorization(configurationService.getConfiguration().getDataStore()));
        IDriveItemCollectionPage pages = graphClientService.getGraphClient().me().drive().root().children().buildRequest().get();
        return pages;
    }

    public DriveItem getRoot() throws BadCredentialsException, IOException, UnknownAuthenticationTypeException {
        System.out.println("get root");
        graphClientService
                .setAccessToken(authorizationHelper.getAuthorization(configurationService.getConfiguration().getDataStore()));
        DriveItem root = graphClientService.getGraphClient().me().drive().root().buildRequest().get();
        return root;
    }

    public IDriveItemCollectionPage getItemChildrens(DriveItem parent)
            throws BadCredentialsException, IOException, UnknownAuthenticationTypeException {
        System.out.println("get item's chilren: " + (parent == null ? null : parent.name));
        graphClientService
                .setAccessToken(authorizationHelper.getAuthorization(configurationService.getConfiguration().getDataStore()));
        IDriveItemCollectionPage pages = graphClientService.getGraphClient().me().drive().items(parent.id).children()
                .buildRequest().get();
        return pages;
    }

    public DriveItem getItemByPath(String path) throws IOException, BadCredentialsException, UnknownAuthenticationTypeException {
        System.out.println("get item by path: " + path);
        DriveItem driveItem;
        if (path == null || path.isEmpty()) {
            driveItem = getRoot();
        } else {
            System.out.println("authorizationHelper:" + authorizationHelper);
            System.out.println("configurationServiceList:" + configurationService);
            graphClientService
                    .setAccessToken(authorizationHelper.getAuthorization(configurationService.getConfiguration().getDataStore()));
            driveItem = graphClientService.getGraphClient().me().drive().root().itemWithPath(path).buildRequest().get();
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
    public DriveItem createItem(String parentId, OneDriveObjectType objectType, String itemPath)
            throws BadCredentialsException, IOException, UnknownAuthenticationTypeException {
        System.out.println("create item: " + (parentId == null ? "root" : parentId));
        if (itemPath == null || itemPath.isEmpty()) {
            return null;
        }
        System.out.println("graphClientService" + graphClientService);
        System.out.println("authorizationHelper" + authorizationHelper);
        System.out.println("configurationServiceList:" + configurationService);
        graphClientService
                .setAccessToken(authorizationHelper.getAuthorization(configurationService.getConfiguration().getDataStore()));

        if (parentId == null || parentId.isEmpty()) {
            parentId = getRoot().id;
        }

        String[] pathParts = itemPath.split("/");
        for (int i = 0; i < pathParts.length - 1; i++) {
            String objName = pathParts[i];
            DriveItem parentItem = null;
            try {
                parentItem = graphClientService.getGraphClient().me().drive().items(parentId).itemWithPath(objName).buildRequest()
                        .get();
                parentId = parentItem.id;
            } catch (GraphServiceException e) {
                if (e.getResponseCode() != 404) {
                    throw e;
                }
            }
            if (parentItem == null) {
                DriveItem objectToCreate = new DriveItem();
                objectToCreate.name = objName;
                objectToCreate.folder = new Folder();
                parentId = graphClientService.getGraphClient().me().drive().items(parentId).children().buildRequest()
                        .post(objectToCreate).id;
            }
            System.out.println("new item " + parentId + " was created");
        }

        String itemName = pathParts[pathParts.length - 1];
        DriveItem objectToCreate = new DriveItem();
        objectToCreate.name = itemName;
        if (objectType == OneDriveObjectType.DIRECTORY) {
            objectToCreate.folder = new Folder();
        } else {
            objectToCreate.file = new com.microsoft.graph.models.extensions.File();
        }
        DriveItem newItem = graphClientService.getGraphClient().me().drive().items(parentId).children().buildRequest()
                .post(objectToCreate);

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
    public void deleteItem(String itemId) throws BadCredentialsException, IOException, UnknownAuthenticationTypeException {
        System.out.println("delete item: " + itemId);
        if (itemId == null || itemId.isEmpty()) {
            return;
        }

        graphClientService
                .setAccessToken(authorizationHelper.getAuthorization(configurationService.getConfiguration().getDataStore()));

        graphClientService.getGraphClient().me().drive().items(itemId).buildRequest().delete();

        System.out.println("item " + itemId + " was deleted");
    }

    public List<JsonObject> getRecords(String requestPath, Map<String, String> queryParameters)
            throws IOException, UnknownAuthenticationTypeException, BadRequestException, BadCredentialsException {
        // List<JsonObject> dataList;
        // try {
        // dataList = execGetRecords(requestPath, queryParameters);
        // return dataList;
        // } catch (UserTokenExpiredException e) {
        // // try to get new token
        // OneDriveDataStore magentoCmsConfigurationBase = configurationServiceInput.getConfiguration()
        // .getMagentoCmsConfigurationBase();
        //
        // AuthenticationLoginPasswordSettings authSettings = (AuthenticationLoginPasswordSettings) magentoCmsConfigurationBase
        // .getAuthSettings();
        //
        // AuthorizationHandlerLoginPassword.clearTokenCache(authSettings);
        // try {
        // dataList = execGetRecords(requestPath, queryParameters);
        // return dataList;
        // } catch (UserTokenExpiredException e1) {
        // throw new BadRequestException("User unauthorised exception");
        // }
        // }
        return null;
    }

    public JsonObject postRecords(String requestPath, JsonObject dataList)
            throws IOException, UnknownAuthenticationTypeException, BadRequestException, BadCredentialsException {
        // try {
        // JsonObject res = execPostRecords(requestPath, dataList);
        // return res;
        // } catch (UserTokenExpiredException e) {
        // // try to get new token
        // OneDriveDataStore magentoCmsConfigurationBase = configurationServiceOutput.getConfiguration()
        // .getMagentoCmsConfigurationBase();
        //
        // AuthenticationLoginPasswordSettings authSettings = (AuthenticationLoginPasswordSettings) magentoCmsConfigurationBase
        // .getAuthSettings();
        //
        // AuthorizationHandlerLoginPassword.clearTokenCache(authSettings);
        // try {
        // JsonObject res = execPostRecords(requestPath, dataList);
        // return res;
        // } catch (UserTokenExpiredException e1) {
        // throw new BadRequestException("User unauthorised exception");
        // }
        // }
        return null;
    }

    private void handleBadRequest400(JsonObject errorObject, String requestObject) throws BadRequestException, IOException {
        /*
         * process messages like this:
         * {"message":"%fieldName is a required field.","parameters":{"fieldName":"searchCriteria"}}
         */
        String message = errorObject.getJsonString("message").getString();
        if (errorObject.get("parameters") != null) {
            if (errorObject.get("parameters").getValueType() == JsonValue.ValueType.OBJECT) {
                for (Map.Entry<String, JsonValue> parameter : errorObject.getJsonObject("parameters").entrySet()) {
                    message = message.replaceAll("%" + parameter.getKey(), parameter.getValue().toString());
                }
            } else if (errorObject.get("parameters").getValueType() == JsonValue.ValueType.ARRAY) {
                JsonArray params = errorObject.getJsonArray("parameters");
                for (int i = 0; i < params.size(); i++) {
                    message = message.replaceAll("%" + (i + 1), params.getString(i));
                }
            }
        }
        throw new BadRequestException(
                "An error occurred: " + message + (requestObject == null ? "" : "For object: " + requestObject));
    }
}
