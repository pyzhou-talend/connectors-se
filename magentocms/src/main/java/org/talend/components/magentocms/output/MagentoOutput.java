package org.talend.components.magentocms.output;

import lombok.extern.slf4j.Slf4j;
import org.talend.components.magentocms.common.UnknownAuthenticationTypeException;
import org.talend.components.magentocms.helpers.ConfigurationHelper;
import org.talend.components.magentocms.input.SelectionType;
import org.talend.components.magentocms.service.http.BadCredentialsException;
import org.talend.components.magentocms.service.http.BadRequestException;
import org.talend.components.magentocms.service.http.MagentoHttpClientService;
import org.talend.sdk.component.api.component.Icon;
import org.talend.sdk.component.api.component.Version;
import org.talend.sdk.component.api.configuration.Option;
import org.talend.sdk.component.api.meta.Documentation;
import org.talend.sdk.component.api.processor.ElementListener;
import org.talend.sdk.component.api.processor.Input;
import org.talend.sdk.component.api.processor.Output;
import org.talend.sdk.component.api.processor.OutputEmitter;
import org.talend.sdk.component.api.processor.Processor;
import org.talend.sdk.component.api.service.http.HttpException;

import javax.json.JsonBuilderFactory;
import javax.json.JsonObject;
import javax.json.JsonObjectBuilder;
import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

@Slf4j
@Version(1)
@Icon(value = Icon.IconType.CUSTOM, custom = "magento_output")
@Processor(name = "Output")
@Documentation("Data output processor")
public class MagentoOutput implements Serializable {

    private final MagentoOutputConfiguration configuration;

    private final JsonBuilderFactory jsonBuilderFactory;

    private MagentoHttpClientService magentoHttpClientService;

    private List<JsonObject> batchData = new ArrayList<>();

    public MagentoOutput(@Option("configuration") final MagentoOutputConfiguration configuration,
            final MagentoHttpClientService magentoHttpClientService, final JsonBuilderFactory jsonBuilderFactory) {
        this.configuration = configuration;
        this.magentoHttpClientService = magentoHttpClientService;
        this.jsonBuilderFactory = jsonBuilderFactory;
        ConfigurationHelper.setupServicesOutput(configuration, magentoHttpClientService);
    }

    @ElementListener
    public void onNext(@Input final JsonObject record, final @Output OutputEmitter<JsonObject> success,
            final @Output("reject") OutputEmitter<Reject> reject) throws UnknownAuthenticationTypeException, IOException {
        processOutputElement(record, success, reject);
    }

    private void processOutputElement(final JsonObject record, OutputEmitter<JsonObject> success, OutputEmitter<Reject> reject)
            throws UnknownAuthenticationTypeException, IOException {
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

            String magentoUrl = configuration.getMagentoUrl();
            JsonObject newRecord = magentoHttpClientService.postRecords(configuration.getMagentoDataStore(), magentoUrl,
                    copyWrapped);

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
}