/*
 * Copyright (C) 2006-2024 Talend Inc. - www.talend.com
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
package org.talend.components.dynamicscrm.migration;

import java.util.Map;
import org.talend.sdk.component.api.component.MigrationHandler;

public class DynamicsDatasetMigrationHandler implements MigrationHandler {

    @Override
    public Map<String, String> migrate(int incomingVersion, Map<String, String> incomingData) {
        if (incomingVersion < 2 && "WEB".equals(incomingData.get("datastore.appType"))
                && incomingData.get("datastore.flow") == null) {
            DynamicsConnectionMigrationHandler.migrateConnectionSetOAuthDefaultValue(incomingData, "datastore.");
        }
        return incomingData;
    }
}
