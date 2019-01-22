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
package org.talend.components.netsuite.runtime.client;

import static java.util.stream.Collectors.toList;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import org.talend.components.netsuite.runtime.model.BasicMetaData;
import org.talend.components.netsuite.runtime.model.BasicRecordType;
import org.talend.components.netsuite.runtime.model.CustomRecordTypeInfo;
import org.talend.components.netsuite.runtime.model.FieldDesc;
import org.talend.components.netsuite.runtime.model.RecordTypeDesc;
import org.talend.components.netsuite.runtime.model.RecordTypeInfo;
import org.talend.components.netsuite.runtime.model.SearchRecordTypeDesc;
import org.talend.components.netsuite.runtime.model.TypeDesc;
import org.talend.sdk.component.api.service.completion.Values;

/**
 * Implementation of <code>MetaDataSource</code> which retrieves customization related meta data
 * from NetSuite web service via <code>NetSuiteClientService</code>.
 *
 * @see NetSuiteClientService
 * @see BasicMetaData
 * @see CustomMetaDataSource
 */
public class DefaultMetaDataSource implements MetaDataSource {

    protected NetSuiteClientService<?> clientService;

    protected boolean customizationEnabled = true;

    protected CustomMetaDataSource customMetaDataSource;

    public DefaultMetaDataSource(NetSuiteClientService<?> clientService) {
        this.clientService = clientService;

        customMetaDataSource = clientService.createDefaultCustomMetaDataSource();
    }

    public NetSuiteClientService<?> getClientService() {
        return clientService;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean isCustomizationEnabled() {
        return customizationEnabled;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void setCustomizationEnabled(boolean customizationEnabled) {
        this.customizationEnabled = customizationEnabled;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public BasicMetaData getBasicMetaData() {
        return clientService.getBasicMetaData();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public CustomMetaDataSource getCustomMetaDataSource() {
        return customMetaDataSource;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void setCustomMetaDataSource(CustomMetaDataSource customMetaDataSource) {
        this.customMetaDataSource = customMetaDataSource;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Collection<RecordTypeInfo> getRecordTypes() {

        List<RecordTypeInfo> recordTypes = clientService.getBasicMetaData().getRecordTypes().stream().map(RecordTypeInfo::new)
                .collect(toList());
        if (customizationEnabled) {
            recordTypes.addAll(customMetaDataSource.getCustomRecordTypes());
        }
        return recordTypes;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Collection<Values.Item> getSearchableTypes() {
        List<Values.Item> searchableTypes = new ArrayList<>(256);

        Collection<RecordTypeInfo> recordTypes = getRecordTypes();
        for (RecordTypeInfo recordTypeInfo : recordTypes) {
            RecordTypeDesc recordTypeDesc = recordTypeInfo.getRecordType();
            if (recordTypeDesc.getSearchRecordType() != null) {
                SearchRecordTypeDesc searchRecordType = clientService.getBasicMetaData().getSearchRecordType(recordTypeDesc);
                if (searchRecordType != null) {
                    searchableTypes.add(new Values.Item(recordTypeInfo.getName(), recordTypeInfo.getDisplayName()));
                }
            }
        }

        return searchableTypes;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public TypeDesc getTypeInfo(final Class<?> clazz) {
        return getTypeInfo(clazz.getSimpleName());
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public TypeDesc getTypeInfo(final String typeName) {
        TypeDesc baseTypeDesc;
        String targetTypeName = null;

        RecordTypeInfo recordTypeInfo = getRecordType(typeName);
        if (recordTypeInfo != null && recordTypeInfo instanceof CustomRecordTypeInfo) {
            CustomRecordTypeInfo customRecordTypeInfo = (CustomRecordTypeInfo) recordTypeInfo;
            baseTypeDesc = clientService.getBasicMetaData().getTypeInfo(customRecordTypeInfo.getRecordType().getTypeName());
            targetTypeName = customRecordTypeInfo.getName();
        } else {
            baseTypeDesc = clientService.getBasicMetaData().getTypeInfo(typeName);
            targetTypeName = baseTypeDesc.getTypeName();
        }

        List<FieldDesc> resultFieldDescList = baseTypeDesc.getFields().stream().filter(this::isNotCustomOrNullFieldList)
                .collect(toList());

        if (recordTypeInfo != null && customizationEnabled) {
            customMetaDataSource.getCustomFields(recordTypeInfo).values().stream().forEach(resultFieldDescList::add);
        }

        return new TypeDesc(targetTypeName, baseTypeDesc.getTypeClass(), resultFieldDescList);
    }

    private boolean isNotCustomOrNullFieldList(FieldDesc fieldDesc) {
        String fieldName = fieldDesc.getName();
        return !fieldName.equals("customFieldList") && !fieldName.equals("nullFieldList");
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public RecordTypeInfo getRecordType(String typeName) {
        RecordTypeDesc recordType = clientService.getBasicMetaData().getRecordType(typeName);
        if (recordType != null) {
            return new RecordTypeInfo(recordType);
        }
        if (customizationEnabled) {
            return customMetaDataSource.getCustomRecordType(typeName);
        }
        return null;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public SearchRecordTypeDesc getSearchRecordType(String recordTypeName) {
        SearchRecordTypeDesc searchRecordType = clientService.getBasicMetaData().getSearchRecordType(recordTypeName);
        if (searchRecordType != null) {
            return searchRecordType;
        }
        RecordTypeInfo recordTypeInfo = getRecordType(recordTypeName);
        if (recordTypeInfo != null) {
            return getSearchRecordType(recordTypeInfo.getRecordType());
        }
        return null;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public SearchRecordTypeDesc getSearchRecordType(RecordTypeDesc recordType) {
        if (recordType.getSearchRecordType() != null) {
            return clientService.getBasicMetaData().getSearchRecordType(recordType.getSearchRecordType());
        }
        if (recordType.getType().equals(BasicRecordType.CUSTOM_RECORD_TYPE.getType())) {
            return clientService.getBasicMetaData().getSearchRecordType(BasicRecordType.CUSTOM_RECORD.getType());
        }
        if (recordType.getType().equals(BasicRecordType.CUSTOM_TRANSACTION_TYPE.getType())) {
            return clientService.getBasicMetaData().getSearchRecordType(BasicRecordType.TRANSACTION.getType());
        }
        return null;
    }
}
