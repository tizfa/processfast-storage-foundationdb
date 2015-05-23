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

import it.cnr.isti.hlt.processfast.data.ReadableDictionary
import it.cnr.isti.hlt.processfast.data.StorageManager
import it.cnr.isti.hlt.processfast.data.StorageManagerAtomicContext

/**
 * @author Tiziano Fagni (tiziano.fagni@isti.cnr.it)
 */
class FoundationDBStorageManagerAtomicContext implements  StorageManagerAtomicContext {

    final FoundationDBStorageManager storageManager
    final ReadableDictionary dict

    FoundationDBStorageManagerAtomicContext(FoundationDBStorageManager storageManager, ReadableDictionary dict) {
        if (storageManager == null)
            throw new NullPointerException("The storage manager is 'null'")
        if (dict == null)
            throw new NullPointerException("The specified dictionary is 'null'")
        this.storageManager = storageManager
        this.dict = dict
    }

    @Override
    StorageManager getStorageManager() {
        return storageManager
    }

    @Override
    ReadableDictionary getInputDictionary() {
        return dict
    }
}
