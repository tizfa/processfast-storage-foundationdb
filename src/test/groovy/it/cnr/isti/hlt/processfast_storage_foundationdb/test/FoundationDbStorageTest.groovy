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

package it.cnr.isti.hlt.processfast_storage_foundationdb.test

import it.cnr.isti.hlt.processfast.data.AbstractStorageTest
import it.cnr.isti.hlt.processfast.data.Storage
import it.cnr.isti.hlt.processfast_storage_foundationdb.FoundationDBStorageManagerProvider
import org.junit.AfterClass
import org.junit.BeforeClass

/**
 * @author Tiziano Fagni (tiziano.fagni@isti.cnr.it)
 */
class FoundationDbStorageTest extends AbstractStorageTest {

    static FoundationDBStorageManagerProvider provider

    @Override
    protected Storage initStorage(String storageName) {
        def sm = provider.getStorageManager("clientID")
        sm.clear()
        sm.createStorage("test")
    }

    /**
     * Init storage manager provider. Called one time before running any
     * test methods.
     */
    @BeforeClass
    static void openStorageManagerProvider() {
        provider = new FoundationDBStorageManagerProvider("test-storage-manager/tiziano")
        provider.open()
    }

    /**
     * Close storage manager provider. Called one time after have been executed all
     * defined test methods.
     */
    @AfterClass
    static void closeStorageManagerProvider() {
        if (provider != null) {
            provider.close()
            provider = null
        }
    }
}
