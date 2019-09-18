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
package org.talend.components.rest.service;

import org.talend.components.rest.configuration.Dataset;
import org.talend.components.rest.configuration.Datastore;
import org.talend.components.rest.configuration.RequestBody;
import org.talend.components.rest.configuration.RequestConfig;
import org.talend.components.rest.configuration.auth.Authentication;
import org.talend.components.rest.configuration.auth.Authorization;

import java.util.Collections;

public class RequestConfigBuilder {

    private RequestConfigBuilder() {
    }

    public static RequestConfig getEmptyRequestConfig() {
        RequestConfig config = new RequestConfig();

        Datastore datastore = new Datastore();

        Authentication authent = new Authentication();
        authent.setType(Authorization.AuthorizationType.NoAuth);

        RequestBody body = new RequestBody();
        body.setType(RequestBody.Type.RAW);
        body.setRawValue("");

        Dataset dataset = new Dataset();
        dataset.setDatastore(datastore);
        dataset.setAuthentication(authent);
        dataset.setBody(body);
        dataset.setHasQueryParams(false);
        dataset.setQueryParams(Collections.emptyList());
        dataset.setHasHeaders(false);
        dataset.setHeaders(Collections.emptyList());
        dataset.setHasPathParams(false);
        dataset.setPathParams(Collections.emptyList());

        config.setDataset(dataset);

        return config;
    }

}
