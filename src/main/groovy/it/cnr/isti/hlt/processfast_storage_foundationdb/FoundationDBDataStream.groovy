/*
 * *****************
 *  Copyright 2015 Tiziano Fagni (tiziano.fagni@isti.cnr.it)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * *******************
 */

package it.cnr.isti.hlt.processfast_storage_foundationdb

import it.cnr.isti.hlt.processfast.data.DataStream

/**
 * @author Tiziano Fagni (tiziano.fagni@isti.cnr.it)
 */
class FoundationDBDataStream implements DataStream {

    /**
     * The parent storage.
     */
    final FoundationDBStorage storage

    /**
     * The data stream name.
     */
    final String name

    FoundationDBDataStream(FoundationDBStorage storage, String dataStreamName) {
        if (storage == null)
            throw new NullPointerException("The storage  is 'null'")
        if (dataStreamName == null || dataStreamName.empty)
            throw new IllegalArgumentException("The dictionary name is 'null' or empty")

        this.storage = storage
        this.name = dataStreamName
    }

    @Override
    InputStream getInputStreamForResource(String resourceName) {
        return null
    }

    @Override
    OutputStream getOutputStreamForResource(String resourceName) {
        return null
    }

    @Override
    void deleteResource(String resourceName) {

    }

    @Override
    void deleteAllResources() {

    }
}
