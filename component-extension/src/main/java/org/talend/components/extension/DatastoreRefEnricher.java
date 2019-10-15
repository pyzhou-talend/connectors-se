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
package org.talend.components.extension;

import lombok.extern.slf4j.Slf4j;
import org.talend.sdk.component.api.component.Components;
import org.talend.sdk.component.api.configuration.type.DataStore;
import org.talend.sdk.component.runtime.manager.reflect.parameterenricher.BaseParameterEnricher;

import java.lang.annotation.Annotation;
import java.lang.reflect.Type;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;

import static java.util.Collections.emptyMap;

@Slf4j
public class DatastoreRefEnricher extends BaseParameterEnricher {

    public static final String META_PREFIX = "tcomp::configurationtyperef::";

    @Override
    public Map<String, String> onParameterAnnotation(final String parameterName, final Type parameterType,
            final Annotation annotation) {
        // todo : maybe contribute a validation listener to the model ?
        if (annotation instanceof DatastoreRef) {
            final Components c = findFamilyInfo(parameterType);
            if (c == null) {
                log.error("Invalid component", new IllegalStateException("Component family can't be found for parameter '"
                        + parameterName + "'" + "\nCheck that you have a valid package-info file."));
            } else {
                Class clazz = (Class) parameterType;
                if (!clazz.isAnnotationPresent(DataStore.class)) {
                    log.error("Invalid datastore ref",
                            new IllegalStateException("A @DatastoreRef can only be used on @Datastore parameters"));
                } else {
                    final String name = ((DataStore) clazz.getAnnotation(DataStore.class)).value();
                    final String family = c.family();
                    final HashMap<String, String> metas = new HashMap<>();
                    metas.put(META_PREFIX + "family", family);
                    metas.put(META_PREFIX + "name", name);
                    metas.put(META_PREFIX + "type", "datastore");
                    final DatastoreRef dsRef = (DatastoreRef) annotation;
                    final AtomicInteger index = new AtomicInteger(0);
                    Stream.of(dsRef.filters()).forEach(f -> {
                        final int i = index.getAndIncrement();
                        metas.put(META_PREFIX + "filter[" + i + "].key", f.key());
                        metas.put(META_PREFIX + "filter[" + i + "].value", f.value());
                    });
                    return metas;
                }
            }
        }
        return emptyMap();
    }

    private Components findFamilyInfo(Type parameterType) {
        final String p = ((Class) parameterType).getPackage().getName();
        final ClassLoader loader = ((Class) parameterType).getClassLoader();
        if (p != null && loader != null) {
            String currentPackage = p;
            do {
                try {
                    final Class<?> pckInfo = loader.loadClass(currentPackage + ".package-info");
                    if (pckInfo.isAnnotationPresent(Components.class)) {
                        return pckInfo.getAnnotation(Components.class);
                    }
                } catch (final ClassNotFoundException e) {
                    // no-op
                }
                final int endPreviousPackage = currentPackage.lastIndexOf('.');
                // we don't accept default package since it is not specific enough
                if (endPreviousPackage < 0) {
                    break;
                }
                currentPackage = currentPackage.substring(0, endPreviousPackage);
            } while (true);
        }
        return null;
    }
}
