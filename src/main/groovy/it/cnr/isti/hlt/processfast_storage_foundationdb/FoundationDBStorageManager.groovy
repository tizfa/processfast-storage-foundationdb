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

import com.foundationdb.KeyValue
import com.foundationdb.Transaction
import com.foundationdb.TransactionContext
import com.foundationdb.async.Function
import com.foundationdb.async.ReadyFuture
import com.foundationdb.directory.DirectoryLayer
import com.foundationdb.tuple.Tuple
import it.cnr.isti.hlt.processfast.connector.FutureValuePromise
import it.cnr.isti.hlt.processfast.connector.ValuePromise
import it.cnr.isti.hlt.processfast.data.*

import java.util.concurrent.Future

/**
 * @author Tiziano Fagni (tiziano.fagni@isti.cnr.it)
 * @since 1.0.0
 */
class FoundationDBStorageManager implements StorageManager {

    final static String STORAGE_LIST = "storageList"


    final FoundationDBStorageManagerProvider provider

    /**
     * The parent FoundationDB transaction context used to perform
     * operations on DB.
     */
    final TransactionContext tc

    /**
     * An identifier for this client object.
     */
    String clientID

    FoundationDBStorageManager(FoundationDBStorageManagerProvider provider, TransactionContext tc, String clientID) {
        if (provider == null)
            throw new NullPointerException("The provider is 'null'")
        if (tc == null)
            throw new NullPointerException("The transaction context is 'null'")
        if (clientID == null || clientID.empty)
            throw new IllegalArgumentException("The client ID is 'null' or empty")
        this.provider = provider
        this.tc = tc
        this.clientID = clientID
    }


    @Override
    List<String> getStorageNames() {
        return getStorageNames(tc)
    }

    List<String> getStorageNames(TransactionContext tc) {
        if (tc == null)
            throw new NullPointerException("The transaction context is 'null'")
        return tc.run({ tr ->
            DirectoryLayer dir = new DirectoryLayer()
            def storageDir = dir.createOrOpen(tr, [provider.getFoundationDBStoragePath()].flatten()).get()
            def storageList = storageDir.subspace(Tuple.from(STORAGE_LIST))
            List<String> storageNames = new ArrayList<String>();
            for(KeyValue kv: tr.getRange(storageList.range())) {
                def t = Tuple.fromBytes(kv.getKey())
                storageNames.add(t.getString(2));
            }
            storageNames
        } as Function)
    }

    @Override
    boolean containsStorageName(String name) {
        return containsStorageName(tc, name)
    }

    boolean containsStorageName(TransactionContext tc, String name) {
        if (tc == null)
            throw new NullPointerException("The transaction context is 'null'")
        if (name == null || name.empty)
            throw new IllegalArgumentException("The storage name is 'null' or empty")

        return tc.run({ tr ->
            DirectoryLayer dir = new DirectoryLayer()
            def storageDir = dir.createOrOpen(tr, [provider.getFoundationDBStoragePath()].flatten()).get()
            def storageList = storageDir.subspace(Tuple.from(STORAGE_LIST))
            def val = tr.get(storageList.pack(Tuple.from(name))).get()
            if (val == null)
                return false
            else
                return true
        } as Function)
    }


    @Override
    Storage createStorage(String name) {
        return createStorage(tc, name)
    }


    Storage createStorage(TransactionContext db, String name) {
        if (tc == null)
            throw new NullPointerException("The transaction context is 'null'")
        if (name == null || name.empty)
            throw new IllegalArgumentException("The storage name is 'null' or empty")
        def storageManager = this
        return db.run({ Transaction tr ->
            DirectoryLayer dir = new DirectoryLayer()
            def storagesDir = dir.createOrOpen(tr, [provider.getFoundationDBStoragePath()].flatten()).get()
            def storageList = storagesDir.subspace(Tuple.from(STORAGE_LIST))
            tr.set(storageList.pack(Tuple.from(name)), Tuple.from(name).pack())
            storagesDir.createOrOpen(tr, [name]).get()
            return new FoundationDBStorage(storageManager, name)
        } as Function)
    }

    @Override
    void removeStorage(String name) {
        removeStorage(tc, name)
    }



    void removeStorage(TransactionContext db, String name) {
        if (tc == null)
            throw new NullPointerException("The transaction context is 'null'")
        if (name == null || name.empty)
            throw new IllegalArgumentException("The storage name is 'null' or empty")
        DirectoryLayer dir = new DirectoryLayer()
        def storageManager = this
        db.run( { Transaction tr ->
            def storagesDir = dir.createOrOpen(tr, [provider.getFoundationDBStoragePath()].flatten()).get()
            def storageList = storagesDir.subspace(Tuple.from(STORAGE_LIST))
            tr.clear(storageList.pack(Tuple.from(name)))
            storagesDir.removeIfExists(tr, [name]).get()
        } as Function)
    }


    FoundationDBStorage getStorage(TransactionContext tc, String name) {
        if (tc == null)
            throw new NullPointerException("The transaction context is 'null'")
        if (name == null || name.empty)
            throw new IllegalArgumentException("The storage name is 'null' or empty")
        def storageManager = this
        return tc.run({ Transaction tr ->
            DirectoryLayer dir = new DirectoryLayer()
            def storagesDir = dir.createOrOpen(tr, [provider.getFoundationDBStoragePath()].flatten()).get()
            def storageList = storagesDir.subspace(Tuple.from(STORAGE_LIST))
            def ret = tr.get(storageList.pack(Tuple.from(name))).get()
            if (ret == null)
                return null
            else {
                storagesDir.createOrOpen(tr, [name]).get()
                return new FoundationDBStorage(storageManager, name)
            }
        } as Function)
    }

    @Override
    Storage getStorage(String name) {
        return getStorage(tc, name)
    }

    @Override
    void flushData() {

    }

    @Override
    ValuePromise<Void> atomic(ReadableDictionary inputData, AtomicOperationsSet operations) {
        if (inputData == null)
            throw new NullPointerException("The set of input data is 'null'")
        if (operations == null)
            throw new NullPointerException("The set of atomic operations is 'null'")

        Future<Void> future = tc.runAsync { Transaction tr ->
            FoundationDBStorageManager manager = new FoundationDBStorageManager(provider, tr, clientID)
            operations.call(new FoundationDBStorageManagerAtomicContext(this), inputData)
            return ReadyFuture.DONE
        }

        return new FutureValuePromise<Void>(future)
    }

    @Override
    ValuePromise<Void> atomic(AtomicOperationsSet operations) {
        return atomic(new RamDictionary(), operations)
    }

    @Override
    ValuePromise<ReadableDictionary> atomicGet(ReadableDictionary inputData, AtomicGetOperationsSet operations) {
        if (inputData == null)
            throw new NullPointerException("The set of input data is 'null'")
        if (operations == null)
            throw new NullPointerException("The set of atomic operations is 'null'")

        Future<ReadableDictionary> future = tc.runAsync { Transaction tr ->
            FoundationDBStorageManager manager = new FoundationDBStorageManager(provider, tr, clientID)
            def retDict = operations.call(new FoundationDBStorageManagerAtomicContext(this), inputData)
            return new ReadyFuture<ReadableDictionary>(retDict)
        }

        return new FutureValuePromise<ReadableDictionary>(future)
    }

    @Override
    ValuePromise<ReadableDictionary> atomicGet(AtomicGetOperationsSet operations) {
        return atomicGet(new RamDictionary(), operations)
    }

    @Override
    void clear() {
        clear(tc)
    }

    void clear(TransactionContext tc) {
        tc.run({ tr ->
            DirectoryLayer dir = new DirectoryLayer()
            def storageDir = dir.createOrOpen(tr, [provider.getFoundationDBStoragePath()].flatten()).get()
            storageDir.removeIfExists(tr).get()
            dir.createOrOpen(tr, [provider.getFoundationDBStoragePath()].flatten()).get()
        } as Function)
    }
}
