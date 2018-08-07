package org.talend.components.magentocms.output;

import oauth.signpost.exception.OAuthCommunicationException;
import oauth.signpost.exception.OAuthExpectationFailedException;
import oauth.signpost.exception.OAuthMessageSignerException;
import org.talend.components.magentocms.common.RequestType;
import org.talend.components.magentocms.common.UnknownAuthenticationTypeException;
import org.talend.components.magentocms.helpers.AuthorizationHelper;
import org.talend.components.magentocms.service.MagentoCmsService;
import org.talend.components.magentocms.service.http.MagentoApiClient;
import org.talend.sdk.component.api.component.Icon;
import org.talend.sdk.component.api.component.Version;
import org.talend.sdk.component.api.configuration.Option;
import org.talend.sdk.component.api.meta.Documentation;
import org.talend.sdk.component.api.processor.*;
import org.talend.sdk.component.api.service.http.HttpException;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import javax.json.JsonBuilderFactory;
import javax.json.JsonObject;
import javax.json.JsonObjectBuilder;
import javax.json.JsonString;
import java.io.Serializable;
import java.net.MalformedURLException;

@Version(1)
// default version is 1, if some configuration changes happen between 2 versions you can add a migrationHandler
@Icon(Icon.IconType.STAR) // you can use a custom one using @Icon(value=CUSTOM, custom="filename") and adding
// icons/filename_icon32.png in resources
@Processor(name = "Output")
@Documentation("TODO fill the documentation for this processor")
public class MagentoCmsOutput implements Serializable {

    private final MagentoCmsOutputConfiguration configuration;

    private final MagentoCmsService service;

    private MagentoApiClient magentoApiClient;

    private String auth;

    private final JsonBuilderFactory jsonBuilderFactory;

    public MagentoCmsOutput(@Option("configuration") final MagentoCmsOutputConfiguration configuration,
            final MagentoCmsService service, final MagentoApiClient magentoApiClient,
            final JsonBuilderFactory jsonBuilderFactory) {
        this.configuration = configuration;
        this.service = service;
        this.magentoApiClient = magentoApiClient;
        this.jsonBuilderFactory = jsonBuilderFactory;
    }

    @PostConstruct
    public void init() throws UnknownAuthenticationTypeException, MalformedURLException, OAuthExpectationFailedException,
            OAuthCommunicationException, OAuthMessageSignerException {
        String magentoUrl = configuration.getMagentoCmsConfigurationBase().getMagentoWebServerUrl() + "/index.php/rest/"
                + configuration.getMagentoCmsConfigurationBase().getMagentoRestVersion() + "/"
                + configuration.getSelectionType().name().toLowerCase();

        auth = AuthorizationHelper.getAuthorization(configuration.getMagentoCmsConfigurationBase().getAuthenticationType(),
                configuration.getMagentoCmsConfigurationBase().getAuthSettings(), magentoUrl, null, RequestType.POST);

        magentoApiClient.base(magentoUrl);
    }

    @BeforeGroup
    public void beforeGroup() {
        // if the environment supports chunking this method is called at the beginning if a chunk
        // it can be used to start a local transaction specific to the backend you use
        // Note: if you don't need it you can delete it
    }

    @ElementListener
    public void onNext(@Input final JsonObject record, final @Output OutputEmitter<JsonObject> success,
            final @Output("reject") OutputEmitter<Reject> reject) {
        try {
            // delete 'id', change 'name' and 'sku'
            final JsonObject copy = record.entrySet().stream().filter(e -> !e.getKey().equals("id"))
                    .collect(jsonBuilderFactory::createObjectBuilder, (builder, a) -> {
                        if (a.getKey().equals("name") || a.getKey().equals("sku")) {
                            builder.add(a.getKey(), ((JsonString) a.getValue()).getString() + "_copy");
                        } else {
                            builder.add(a.getKey(), a.getValue());
                        }
                    }, JsonObjectBuilder::addAll).build();
            final JsonObject copy2 = jsonBuilderFactory.createObjectBuilder().add("product", copy).build();
            magentoApiClient.postRecords(auth, copy2);
            success.emit(record);
        } catch (HttpException httpError) {
            int status = httpError.getResponse().status();
            final JsonObject error = (JsonObject) httpError.getResponse().error(JsonObject.class);
            if (error != null && error.containsKey("message")) {
                reject.emit(new Reject(status, error.getString("message"), "", record));
            } else {
                reject.emit(new Reject(status, "unknown", "", record));
            }
        }
    }

    @AfterGroup
    public void afterGroup() {
        // symmetric method of the beforeGroup() executed after the chunk processing
        // Note: if you don't need it you can delete it
    }

    @PreDestroy
    public void release() {
        // this is the symmetric method of the init() one,
        // release potential connections you created or data you cached
        // Note: if you don't need it you can delete it
    }
}