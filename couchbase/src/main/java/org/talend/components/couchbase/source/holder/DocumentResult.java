package org.talend.components.couchbase.source.holder;

import com.couchbase.client.java.document.json.JsonObject;

public class DocumentResult implements ResultHolder{

    private JsonObject value;
    private boolean hasNext = false;

    public DocumentResult(JsonObject value) {
        this.value = value;
        this.hasNext = true;
    }

    @Override
    public JsonObject next() {
        hasNext = false;
        return value;
    }

    @Override
    public boolean hasNext() {
        return hasNext;
    }
}
