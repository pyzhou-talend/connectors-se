/*
 * Copyright (C) 2006-2019 Talend Inc. - www.talend.com
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */
package org.talend.components.netsuite.source;

import org.apache.commons.lang3.StringUtils;
import org.talend.components.netsuite.dataset.NetSuiteInputProperties;
import org.talend.components.netsuite.dataset.SearchConditionConfiguration;
import org.talend.components.netsuite.runtime.client.NetSuiteClientService;
import org.talend.components.netsuite.runtime.client.NetSuiteException;
import org.talend.components.netsuite.runtime.client.ResultSet;
import org.talend.components.netsuite.runtime.client.search.SearchCondition;
import org.talend.components.netsuite.runtime.client.search.SearchQuery;
import org.talend.components.netsuite.runtime.model.RecordTypeInfo;
import org.talend.components.netsuite.service.Messages;
import org.talend.components.netsuite.service.NetSuiteService;
import org.talend.sdk.component.api.configuration.Option;
import org.talend.sdk.component.api.input.Producer;
import org.talend.sdk.component.api.meta.Documentation;
import org.talend.sdk.component.api.record.Record;
import org.talend.sdk.component.api.record.Schema;
import org.talend.sdk.component.api.service.record.RecordBuilderFactory;

import javax.annotation.PostConstruct;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

import static java.util.stream.Collectors.toList;

@Documentation("TODO fill the documentation for this source")
public class NetSuiteInputSource implements Serializable {

    private final NetSuiteInputProperties configuration;

    private final NetSuiteService service;

    private final RecordBuilderFactory recordBuilderFactory;

    private final Messages i18n;

    private Schema runtimeSchema;

    private NetSuiteClientService<?> clientService;

    private ResultSet<?> rs;

    private NsObjectInputTransducer transducer;

    public NetSuiteInputSource(@Option("configuration") final NetSuiteInputProperties configuration,
            final NetSuiteService service, final RecordBuilderFactory recordBuilderFactory, final Messages i18n) {
        this.configuration = configuration;
        this.service = service;
        this.recordBuilderFactory = recordBuilderFactory;
        this.i18n = i18n;
    }

    @PostConstruct
    public void init() {
        clientService = service.getClientService(configuration.getDataSet().getDataStore());
        runtimeSchema = service.getSchema(configuration.getDataSet(), configuration.getDataSet().getSchema());
        rs = search();
    }

    @Producer
    public Record next() {
        return rs.next() ? transducer.read(rs::get) : null;
    }

    /**
     * Build and execute NetSuite search query.
     *
     * @return
     * @throws NetSuiteException if an error occurs during execution of search
     */
    private ResultSet<?> search() throws NetSuiteException {
        SearchQuery<?, ?> search = buildSearchQuery();
        RecordTypeInfo recordTypeInfo = search.getRecordTypeInfo();

        transducer = new NsObjectInputTransducer(clientService, i18n, recordBuilderFactory, runtimeSchema,
                recordTypeInfo.getName(), configuration.getDataSet().getDataStore().getApiVersion().getVersion());
        return search.search();
    }

    /**
     * Build search query from properties.
     *
     * @return search query object
     */
    private SearchQuery<?, ?> buildSearchQuery() {
        String recordType = configuration.getDataSet().getRecordType();

        SearchQuery<?, ?> search = clientService.newSearch(clientService.getMetaDataSource());
        search.target(recordType);
        Optional<List<SearchConditionConfiguration>> searchConfigurationsOptional = Optional
                .ofNullable(configuration.getSearchCondition()).filter(list -> !list.isEmpty());
        if (searchConfigurationsOptional.isPresent()) {
            searchConfigurationsOptional.get().stream().map(this::buildSearchCondition).forEach(search::condition);
        }
        return search;
    }

    /**
     * Build search condition.
     *
     * @param fieldName name of search field
     * @param operator name of search operator
     * @param value1 first search value
     * @param value2 second search value
     * @return
     */
    private SearchCondition buildSearchCondition(SearchConditionConfiguration searchConfiguration) {
        List<String> values = buildSearchConditionValueList(searchConfiguration.getSearchValue(),
                searchConfiguration.getAdditionalSearchValue());
        return new SearchCondition(searchConfiguration.getField(), searchConfiguration.getOperator(), values);
    }

    /**
     * Build search value list.
     *
     * @param searchValue first search value
     * @param additionalSearchValue second search value
     * @return
     */
    private List<String> buildSearchConditionValueList(Object searchValue, Object additionalSearchValue) {
        if (searchValue == null) {
            return null;
        }

        List<String> valueList;
        if (searchValue instanceof Collection) {
            Collection<?> elements = (Collection<?>) searchValue;
            return elements.stream().filter(Objects::nonNull).map(Objects::toString).collect(toList());
        } else {
            valueList = new ArrayList<>(2);
            String value = searchValue.toString();
            if (StringUtils.isNotEmpty(value)) {
                valueList.add(value);
                Optional.ofNullable(additionalSearchValue).map(String.class::cast).filter(StringUtils::isNotEmpty)
                        .ifPresent(valueList::add);
            }
        }

        return valueList;
    }

}