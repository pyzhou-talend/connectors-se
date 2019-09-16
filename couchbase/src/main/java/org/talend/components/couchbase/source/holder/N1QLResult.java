package org.talend.components.couchbase.source.holder;


import com.couchbase.client.java.document.json.JsonObject;
import com.couchbase.client.java.query.N1qlQueryRow;

import java.util.Iterator;

public class N1QLResult implements ResultHolder {

    private Iterator<N1qlQueryRow> value;

    public N1QLResult(Iterator<N1qlQueryRow> value) {
        this.value = value;
    }

    @Override
    public JsonObject next() {
        return value.next().value();
    }

    @Override
    public boolean hasNext() {
        return value.hasNext();
    }
}
