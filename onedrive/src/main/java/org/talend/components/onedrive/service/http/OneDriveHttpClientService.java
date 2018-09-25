package org.talend.components.onedrive.service.http;

import com.microsoft.graph.models.extensions.DriveItem;
import com.microsoft.graph.requests.extensions.IDriveItemCollectionPage;
import lombok.extern.slf4j.Slf4j;
import org.talend.components.onedrive.common.UnknownAuthenticationTypeException;
import org.talend.components.onedrive.helpers.AuthorizationHelper;
import org.talend.components.onedrive.service.configuration.ConfigurationServiceInput;
import org.talend.components.onedrive.service.configuration.ConfigurationServiceList;
import org.talend.components.onedrive.service.configuration.ConfigurationServiceOutput;
import org.talend.components.onedrive.service.graphclient.GraphClientService;
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
    private JsonParserFactory jsonParserFactory;

    @Service
    private JsonBuilderFactory jsonBuilderFactory;

    @Service
    private ConfigurationServiceList configurationServiceList;

    @Service
    private ConfigurationServiceInput configurationServiceInput;

    @Service
    private ConfigurationServiceOutput configurationServiceOutput;

    @Service
    private AuthorizationHelper authorizationHelper;

    @Service
    private GraphClientService graphClientService;

    public List<JsonObject> getList(String requestPath, Map<String, String> queryParameters)
            throws IOException, UnknownAuthenticationTypeException, BadRequestException, BadCredentialsException {
        // final ICallback<IDriveItemCollectionPage> callback = new ICallback<IDriveItemCollectionPage>() {
        // @Override
        // public void success(final IDriveItemCollectionPage result) {
        // for (DriveItem item : result.getCurrentPage()) {
        // printItem(parent, item);
        // writeListItems(item);
        // }
        // // If there was more pages retrieve them too
        // while (result.getNextPage() != null) {
        // result.getNextPage().buildRequest()
        // .get(new ICallback<IDriveItemCollectionPage>() {
        // @Override
        // public void success(IDriveItemCollectionPage iDriveItemCollectionPage) {
        // for (DriveItem item : iDriveItemCollectionPage.getCurrentPage()) {
        // printItem(parent, item);
        // writeListItems(item);
        // }
        // }
        //
        // @Override
        // public void failure(ClientException e) {
        //
        // }
        // });
        // }
        // }
        //
        // @Override
        // public void failure(ClientException e) {
        //
        // }
        // };
        // if (parent == null) {
        // graphClientService.getGraphClient().me().drive().root().children().buildRequest().get(callback);
        // } else {
        // graphClientService.getGraphClient().me().drive().items(parent.id).children().buildRequest().get(callback);
        // }
        return null;
    }

    public IDriveItemCollectionPage getRootChildrens()
            throws BadCredentialsException, IOException, UnknownAuthenticationTypeException {
        System.out.println("get root chilren");
        graphClientService
                .setAccessToken(authorizationHelper.getAuthorization(configurationServiceList.getConfiguration().getDataStore()));
        IDriveItemCollectionPage pages = graphClientService.getGraphClient().me().drive().root().children().buildRequest().get();
        return pages;
    }

    public DriveItem getRoot() throws BadCredentialsException, IOException, UnknownAuthenticationTypeException {
        System.out.println("get root");
        graphClientService
                .setAccessToken(authorizationHelper.getAuthorization(configurationServiceList.getConfiguration().getDataStore()));
        DriveItem root = graphClientService.getGraphClient().me().drive().root().buildRequest().get();
        return root;
    }

    public IDriveItemCollectionPage getItemChildrens(DriveItem parent)
            throws BadCredentialsException, IOException, UnknownAuthenticationTypeException {
        System.out.println("get item's chilren: " + (parent == null ? null : parent.name));
        graphClientService
                .setAccessToken(authorizationHelper.getAuthorization(configurationServiceList.getConfiguration().getDataStore()));
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
            graphClientService.setAccessToken(
                    authorizationHelper.getAuthorization(configurationServiceList.getConfiguration().getDataStore()));
            driveItem = graphClientService.getGraphClient().me().drive().root().itemWithPath(path).buildRequest().get();
        }
        return driveItem;
    }

    // private void printItem(DriveItem parent, final DriveItem item) {
    // System.out.println((parent == null ? "ROOT" : parent.name) + " -> " + item.name);
    //
    // // print content
    // final String fileName = "c:/temp/2/" + item.name;
    // graphClientService.getGraphClient().me().drive().items(item.id).content()
    // .buildRequest()
    // .get(new ICallback<InputStream>() {
    // @Override
    // public void success(final InputStream inputStream) {
    // int totalBytes = 0;
    // try (OutputStream outputStream = new FileOutputStream(new File(fileName))) {
    // int read = 0;
    // byte[] bytes = new byte[1024*1024];
    // while ((read = inputStream.read(bytes)) != -1) {
    // totalBytes += read;
    // outputStream.write(bytes, 0, read);
    // System.out.println("progress: " + fileName + ": " + totalBytes + ":" + item.size);
    // }
    // } catch (IOException e) {
    // e.printStackTrace();
    // } finally {
    // if (inputStream != null) {
    // try {
    // inputStream.close();
    // } catch (IOException e) {
    // e.printStackTrace();
    // }
    // }
    // }
    // }
    //
    // @Override
    // public void failure(final ClientException ex) {
    // // Handle failure
    // }
    // });
    // }

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

    // private List<JsonObject> execGetRecords(String requestPath, Map<String, String> queryParameters) throws
    // BadRequestException,
    // UnknownAuthenticationTypeException, IOException, UserTokenExpiredException, BadCredentialsException {
    // OneDriveDataStore magentoCmsConfigurationBase = configurationServiceInput.getConfiguration()
    // .getMagentoCmsConfigurationBase();
    // Response<JsonObject> response;
    //
    // String auth = authorizationHelper.getAuthorization(magentoCmsConfigurationBase);
    // response = oneDriveAuthHttpClient.getRecords(requestPath, auth, queryParameters);
    //
    // if (response.status() == 200) {
    // List<JsonObject> dataList = new ArrayList<>();
    // response.body().getJsonArray("items").forEach((t) -> {
    // dataList.add(t.asJsonObject());
    // });
    // return dataList;
    // } else if (response.status() == 400) {
    // handleBadRequest400(response.error(JsonObject.class), null);
    // return null;
    // } else if (response.status() == 401
    // && magentoCmsConfigurationBase.getAuthenticationType() == AuthenticationType.LOGIN_PASSWORD) {
    // // maybe token is expired
    // throw new UserTokenExpiredException();
    // } else {
    // throw new BadRequestException("unknown exception");
    // }
    // }

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

    // private JsonObject execPostRecords(String requestPath, JsonObject dataList) throws IOException,
    // UnknownAuthenticationTypeException, BadRequestException, UserTokenExpiredException, BadCredentialsException {
    // Response<JsonObject> response;
    // OneDriveDataStore magentoCmsConfigurationBase = configurationServiceOutput.getConfiguration()
    // .getMagentoCmsConfigurationBase();
    //
    // String auth = authorizationHelper.getAuthorization(magentoCmsConfigurationBase);
    // response = oneDriveAuthHttpClient.postRecords(requestPath, auth, dataList);
    //
    // if (response.status() == 200) {
    // return response.body();
    // } else if (response.status() == 400) {
    // handleBadRequest400(response.error(JsonObject.class), dataList.toString());
    // return null;
    // } else if (response.status() == 401
    // && magentoCmsConfigurationBase.getAuthenticationType() == AuthenticationType.LOGIN_PASSWORD) {
    // // maybe token is expired
    // throw new UserTokenExpiredException();
    // } else {
    // throw new BadRequestException("unknown exception");
    // }
    // }

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
