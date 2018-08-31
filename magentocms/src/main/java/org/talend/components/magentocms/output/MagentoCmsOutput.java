package org.talend.components.magentocms.output;

import lombok.extern.slf4j.Slf4j;
import oauth.signpost.exception.OAuthCommunicationException;
import oauth.signpost.exception.OAuthExpectationFailedException;
import oauth.signpost.exception.OAuthMessageSignerException;
import org.talend.components.magentocms.common.UnknownAuthenticationTypeException;
import org.talend.components.magentocms.input.SelectionType;
import org.talend.components.magentocms.service.http.BadCredentialsException;
import org.talend.components.magentocms.service.http.BadRequestException;
import org.talend.components.magentocms.service.http.MagentoHttpServiceFactory;
import org.talend.sdk.component.api.component.Icon;
import org.talend.sdk.component.api.component.Version;
import org.talend.sdk.component.api.configuration.Option;
import org.talend.sdk.component.api.meta.Documentation;
import org.talend.sdk.component.api.processor.*;
import org.talend.sdk.component.api.service.http.HttpException;

import javax.annotation.PostConstruct;
import javax.json.JsonBuilderFactory;
import javax.json.JsonObject;
import javax.json.JsonObjectBuilder;
import java.io.IOException;
import java.io.Serializable;

@Slf4j
@Version(1)
// default version is 1, if some configuration changes happen between 2 versions you can add a migrationHandler
@Icon(Icon.IconType.STAR)
// you can use a custom one using @Icon(value=CUSTOM, custom="filename") and adding
// icons/filename_icon32.png in resources
@Processor(name = "Output")
@Documentation("Data output processor")
public class MagentoCmsOutput implements Serializable {

    private final MagentoCmsOutputConfiguration configuration;

    private final MagentoHttpServiceFactory magentoHttpServiceFactory;

    private final JsonBuilderFactory jsonBuilderFactory;

    private MagentoHttpServiceFactory.MagentoHttpService magentoHttpService;

    private String magentoUrl;

    public MagentoCmsOutput(@Option("configuration") final MagentoCmsOutputConfiguration configuration,
            final MagentoHttpServiceFactory magentoHttpServiceFactory, final JsonBuilderFactory jsonBuilderFactory) {
        this.configuration = configuration;
        this.magentoHttpServiceFactory = magentoHttpServiceFactory;
        this.jsonBuilderFactory = jsonBuilderFactory;
    }

    @PostConstruct
    public void init() {
        magentoUrl = configuration.getMagentoUrl();
        magentoHttpService = magentoHttpServiceFactory.createMagentoHttpService(configuration.getMagentoCmsConfigurationBase());
    }

    // @BeforeGroup
    // public void beforeGroup() {
    // // if the environment supports chunking this method is called at the beginning if a chunk
    // // it can be used to start a local transaction specific to the backend you use
    // // Note: if you don't need it you can delete it
    // }

    @ElementListener
    public void onNext(@Input final JsonObject record, final @Output OutputEmitter<JsonObject> success,
            final @Output("reject") OutputEmitter<Reject> reject) throws UnknownAuthenticationTypeException,
            OAuthExpectationFailedException, OAuthCommunicationException, OAuthMessageSignerException, IOException {
        try {
            // delete 'id'
            final JsonObject copy = record.entrySet().stream().filter(e -> !e.getKey().equals("id"))
                    .collect(jsonBuilderFactory::createObjectBuilder, (builder, a) -> builder.add(a.getKey(), a.getValue()),
                            JsonObjectBuilder::addAll)
                    .build();
            // get element name
            String jsonElementName;
            if (configuration.getSelectionType() == SelectionType.PRODUCTS) {
                jsonElementName = "product";
            } else {
                throw new RuntimeException("Selection type is not set");
            }

            final JsonObject copyWrapped = jsonBuilderFactory.createObjectBuilder().add(jsonElementName, copy).build();

            JsonObject newRecord = magentoHttpService.postRecords(magentoUrl, copyWrapped);

            success.emit(newRecord);
        } catch (HttpException httpError) {
            int status = httpError.getResponse().status();
            final JsonObject error = (JsonObject) httpError.getResponse().error(JsonObject.class);
            if (error != null && error.containsKey("message")) {
                reject.emit(new Reject(status, error.getString("message"), "", record));
            } else {
                reject.emit(new Reject(status, "unknown", "", record));
            }
        } catch (BadCredentialsException e) {
            log.error("Bad user credentials");
        } catch (BadRequestException e) {
            log.warn(e.getMessage());
            reject.emit(new Reject(400, e.getMessage(), "", record));
        }
    }

    // @AfterGroup
    // public void afterGroup() {
    // // symmetric method of the beforeGroup() executed after the chunk processing
    // // Note: if you don't need it you can delete it
    // }
    //
    // @PreDestroy
    // public void release() {
    // // this is the symmetric method of the init() one,
    // // release potential connections you created or data you cached
    // // Note: if you don't need it you can delete it
    // }
}