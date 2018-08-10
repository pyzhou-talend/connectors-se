package org.talend.components.magentocms.service;

import oauth.signpost.exception.OAuthCommunicationException;
import oauth.signpost.exception.OAuthExpectationFailedException;
import oauth.signpost.exception.OAuthMessageSignerException;
import org.talend.components.magentocms.common.UnknownAuthenticationTypeException;
import org.talend.components.magentocms.input.MagentoCmsInputMapperConfiguration;
import org.talend.components.magentocms.input.MagentoCmsSchemaDiscover;
import org.talend.components.magentocms.service.http.MagentoHttpServiceFactory;
import org.talend.sdk.component.api.service.Service;
import org.talend.sdk.component.api.service.schema.DiscoverSchema;
import org.talend.sdk.component.api.service.schema.Schema;
import org.talend.sdk.component.api.service.schema.Type;

import java.io.IOException;
import java.util.List;

import static java.util.stream.Collectors.toList;

@Service
public class MagentoCmsService {

    @DiscoverSchema("guessTableSchema")
    public Schema guessTableSchema(final MagentoCmsInputMapperConfiguration dataSet, final MagentoHttpServiceFactory client)
            throws UnknownAuthenticationTypeException, OAuthExpectationFailedException, OAuthCommunicationException,
            OAuthMessageSignerException, IOException {
        final MagentoCmsSchemaDiscover source = new MagentoCmsSchemaDiscover(dataSet, client);
        List<String> columns = source.getColumns();
        return new Schema(columns.stream().map(k -> new Schema.Entry(k, Type.STRING)).collect(toList()));
    }

    // @Cached
    // @DynamicValues("Proposable_GetTableFields")
    // public Values getTableFields() {
    // return new Values(Stream.of(new Values.Item("1", "1 name"), new Values.Item("2", "2 name"))
    // .collect(toList()));
    // }
}