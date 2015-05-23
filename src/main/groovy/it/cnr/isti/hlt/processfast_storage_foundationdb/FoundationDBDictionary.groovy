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

import it.cnr.isti.hlt.processfast.data.Dictionary

/**
 * A dictionary implementation based on FoundationDB.
 *
 * @author Tiziano Fagni (tiziano.fagni@isti.cnr.it)
 * @since 1.0.0
 */
class FoundationDBDictionary implements Dictionary {

    /**
     * The parent storage.
     */
    final FoundationDBStorage storage

    /**
     * The dictionary name.
     */
    final String name

    FoundationDBDictionary(FoundationDBStorage storage, String dictionaryName) {
        if (storage == null)
            throw new NullPointerException("The storage  is 'null'")
        if (dictionaryName == null || dictionaryName.empty)
            throw new IllegalArgumentException("The dictionary name is 'null' or empty")

        this.storage = storage
        this.name = dictionaryName
    }

    @Override
    Dictionary clear() {
        return null
    }

    @Override
    def <T extends Serializable> Dictionary put(String key, T data) {
        return null
    }

    @Override
    def <T extends Serializable> Dictionary putAll(Map<String, T> map) {
        return null
    }

    @Override
    Dictionary remove(String key) {
        return null
    }

    @Override
    boolean containsKey(String key) {
        return false
    }

    @Override
    def <T extends Serializable> T get(String key) {
        return null
    }

    @Override
    Iterator<String> keySet() {
        return null
    }

    @Override
    long size() {
        return 0
    }
}
