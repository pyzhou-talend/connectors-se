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
package org.talend.components.cosmosDB.dataset;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class QueryDatasetTest {

    @Test
    void equalsTest() {
        QueryDataset q1 = new QueryDataset();
        QueryDataset q2 = new QueryDataset();
        q1.setQuery("TheQuery");
        q1.setUseQuery(true);
        q1.setCollectionID("c1");

        q2.setQuery("TheQuery");
        q2.setUseQuery(true);
        q2.setCollectionID("c2");

        final boolean equals = q1.equals(q2);
        Assertions.assertFalse(equals);
    }

}