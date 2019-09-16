package org.talend.components.couchbase.source.holder;


import com.couchbase.client.java.document.json.JsonObject;

public interface ResultHolder {
    JsonObject next();
    boolean hasNext();
}
