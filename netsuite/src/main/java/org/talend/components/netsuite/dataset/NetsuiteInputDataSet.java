package org.talend.components.netsuite.dataset;

import org.talend.components.netsuite.datastore.NetsuiteDataStore;
import org.talend.sdk.component.api.configuration.Option;
import org.talend.sdk.component.api.configuration.action.Suggestable;
import org.talend.sdk.component.api.configuration.type.DataSet;
import org.talend.sdk.component.api.configuration.ui.layout.GridLayout;
import org.talend.sdk.component.api.configuration.ui.layout.GridLayouts;
import org.talend.sdk.component.api.meta.Documentation;

@DataSet("input")
@GridLayouts({
        @GridLayout({ @GridLayout.Row({ "dataStore" }), @GridLayout.Row({ "recordType" }),
                @GridLayout.Row({ "searchCondition" }) }),
        @GridLayout(names = { GridLayout.FormType.ADVANCED }, value = @GridLayout.Row({ "bodyFieldsOnly" })) })
public class NetsuiteInputDataSet {

    @Option
    @Documentation("")
    private NetsuiteDataStore dataStore;

    @Option
    @Suggestable(value = "loadRecordTypes", parameters = { ".." })
    @Documentation("TODO fill the documentation for this parameter")
    private String recordType;

    @Option
    @Documentation("TODO fill the documentation for this parameter")
    private SearchConditionConfiguration searchCondition;

    @Option
    @Documentation("TODO fill the documentation for this parameter")
    private boolean bodyFieldsOnly;

    public String getRecordType() {
        return recordType;
    }

    public NetsuiteInputDataSet setRecordType(String recordType) {
        this.recordType = recordType;
        return this;
    }

    public SearchConditionConfiguration getSearchCondition() {
        return searchCondition;
    }

    public NetsuiteInputDataSet setSearchCondition(SearchConditionConfiguration searchCondition) {
        this.searchCondition = searchCondition;
        return this;
    }

    public boolean isBodyFieldsOnly() {
        return bodyFieldsOnly;
    }

    public NetsuiteInputDataSet setBodyFieldsOnly(boolean bodyFieldsOnly) {
        this.bodyFieldsOnly = bodyFieldsOnly;
        return this;
    }

    public NetsuiteDataStore getDataStore() {
        return this.dataStore;
    }

    public NetsuiteInputDataSet setDataStore(NetsuiteDataStore dataStore) {
        this.dataStore = dataStore;
        return this;
    }
}
