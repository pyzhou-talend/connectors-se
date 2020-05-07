/*
 * Copyright (C) 2006-2020 Talend Inc. - www.talend.com
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
package org.talend.components.ftp.service.ftpclient;

import lombok.RequiredArgsConstructor;
import org.slf4j.Logger;

import java.io.Writer;

@RequiredArgsConstructor
public class LogWriter extends Writer {

    private final Logger log;

    private final StringBuilder buffer = new StringBuilder();

    @Override
    public void write(final char[] cbuf, final int off, final int len) {
        buffer.append(cbuf, off, len);
    }

    @Override
    public void flush() {
        if (buffer.length() > 0) {
            log.info(buffer.toString());
            buffer.setLength(0);
        }
    }

    @Override
    public void close() {
        flush();
    }
}
