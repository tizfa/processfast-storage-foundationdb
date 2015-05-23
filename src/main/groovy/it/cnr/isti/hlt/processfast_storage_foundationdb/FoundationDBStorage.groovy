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
import com.foundationdb.directory.DirectoryLayer
import com.foundationdb.tuple.Tuple
import it.cnr.isti.hlt.processfast.data.*

/**
 * A storage implementation based on FoundationDB.
 *
 * @author Tiziano Fagni (tiziano.fagni@isti.cnr.it)
 * @since 1.0.0
 */
class FoundationDBStorage implements Storage {

    private static final String ARRAY_LIST = "arrayList"
    private static final String MATRIX_LIST = "matrixList"
    private static final String DICTIONARY_LIST = "dictionaryList"
    private static final String DATASTREAM_LIST = "dataStreamList"


    private static final String ARRAY_PREFIX = "ar_"
    private static final String MATRIX_PREFIX = "ma_"
    private static final String DICTIONARY_PREFIX = "di_"
    private static final String DATASTREAM_PREFIX = "ds_"

    /**
     * The parent storage manager.
     */
    final FoundationDBStorageManager storageManager

    /**
     * The name of this storage.
     */
    String name


    FoundationDBStorage(FoundationDBStorageManager storageManager, String storageName) {
        if (storageManager == null)
            throw new NullPointerException("The storage manager is 'null'")
        if (storageName == null || storageName.empty)
            throw new IllegalArgumentException("The storage name is 'null' or empty")
        this.storageManager = storageManager
        this.name = storageName
    }

    String computeArrayName(String name) {
        return ARRAY_PREFIX + name
    }

    String computeMatrixName(String name) {
        return MATRIX_PREFIX + name
    }

    String computeDictionaryName(String name) {
        return DICTIONARY_PREFIX + name
    }

    String computeDataStreamName(String name) {
        return DATASTREAM_PREFIX + name
    }

    @Override
    String getName() {
        return name
    }

    @Override
    List<String> getArrayNames() {
        return getArrayNames(storageManager.tc)
    }


    List<String> getStoragePath() {
        return [storageManager.provider.getFoundationDBStoragePath(), name].flatten()
    }

    List<String> getArrayNames(TransactionContext tc) {
        if (tc == null)
            throw new NullPointerException("The transaction context is 'null'")
        return tc.run({ tr ->
            DirectoryLayer dir = new DirectoryLayer()
            def storageDir = dir.open(tr, getStoragePath()).get()
            def arrayList = storageDir.subspace(Tuple.from(ARRAY_LIST))
            List<String> storageNames = new ArrayList<String>();
            for (KeyValue kv : tr.getRange(arrayList.range())) {
                def t = Tuple.fromBytes(kv.getKey())
                storageNames.add(t.getString(2));
            }
            storageNames
        } as Function)
    }

    @Override
    boolean containsArrayName(String name) {
        return containsArrayName(storageManager.tc, name)
    }

    boolean containsArrayName(TransactionContext tc, String name) {
        if (tc == null)
            throw new NullPointerException("The transaction context is 'null'")
        if (name == null || name.empty)
            throw new IllegalArgumentException("The storage name is 'null' or empty")

        return tc.run({ tr ->
            DirectoryLayer dir = new DirectoryLayer()
            def storageDir = dir.open(tr, getStoragePath()).get()
            def arrayList = storageDir.subspace(Tuple.from(ARRAY_LIST))
            def val = tr.get(arrayList.pack(Tuple.from(name))).get()
            if (val == null)
                return false
            else
                return true
        } as Function)
    }

    @Override
    def <T extends Serializable> Array<T> createArray(String name, Class<T> cl) {
        return createArray(storageManager.tc, name, cl)
    }


    public <T extends Serializable> Array<T> createArray(TransactionContext tc, String name, Class<T> cl) {
        if (tc == null)
            throw new NullPointerException("The transaction context is 'null'")
        if (name == null || name.empty)
            throw new IllegalArgumentException("The storage name is 'null' or empty")
        if (cl == null)
            throw new NullPointerException("The class type is 'null'")
        def storage = this
        return tc.run({ Transaction tr ->
            DirectoryLayer dir = new DirectoryLayer()
            def storagesDir = dir.open(tr, getStoragePath()).get()
            def arrayList = storagesDir.subspace(Tuple.from(ARRAY_LIST))
            tr.set(arrayList.pack(Tuple.from(name)), Tuple.from(cl.getName()).pack())
            storagesDir.createOrOpen(tr, [computeArrayName(name)]).get()
            return new FoundationDBArray<T>(tr, storage, name)
        } as Function)
    }

    @Override
    void removeArray(String name) {
        removeArray(storageManager.tc, name)
    }


    void removeArray(TransactionContext tc, String name) {
        if (tc == null)
            throw new NullPointerException("The transaction context is 'null'")
        if (name == null || name.empty)
            throw new IllegalArgumentException("The array name is 'null' or empty")
        DirectoryLayer dir = new DirectoryLayer()
        tc.run({ Transaction tr ->
            def storagesDir = dir.open(tr, getStoragePath()).get()
            def arrayList = storagesDir.subspace(Tuple.from(ARRAY_LIST))
            tr.clear(arrayList.pack(Tuple.from(name)))
            storagesDir.removeIfExists(tr, [computeArrayName(name)]).get()
        } as Function)
    }

    @Override
    def <T extends Serializable> Array<T> getArray(String name, Class<T> cl) {
        getArray(storageManager.tc, name, cl)
    }


    public <T extends Serializable> FoundationDBArray<T> getArray(TransactionContext tc, String name, Class<T> cl) {
        if (tc == null)
            throw new NullPointerException("The transaction context is 'null'")
        if (name == null || name.empty)
            throw new IllegalArgumentException("The array name is 'null' or empty")
        if (cl == null)
            throw new NullPointerException("")
        def storage = this
        return tc.run({ Transaction tr ->
            DirectoryLayer dir = new DirectoryLayer()
            def storagesDir = dir.open(tr, getStoragePath()).get()
            def arrayList = storagesDir.subspace(Tuple.from(ARRAY_LIST))
            def ret = tr.get(arrayList.pack(Tuple.from(name))).get()
            if (ret == null)
                return null
            else {
                Class<?> storedCl = Class.forName(Tuple.fromBytes(ret).getString(0))
                if (!storedCl.isAssignableFrom(cl))
                    throw new IllegalArgumentException("The array created is of type ${storedCl.toString()} while you requested array of type ${cl.toString()}")
                storagesDir.createOrOpen(tr, [computeArrayName(name)]).get()
                new FoundationDBArray<T>(tr, storage, name)
            }
        } as Function)
    }

    @Override
    List<String> getMatrixNames() {
        return getMatrixNames(storageManager.tc)
    }

    List<String> getMatrixNames(TransactionContext tc) {
        if (tc == null)
            throw new NullPointerException("The transaction context is 'null'")
        return tc.run({ tr ->
            DirectoryLayer dir = new DirectoryLayer()
            def storageDir = dir.open(tr, getStoragePath()).get()
            def matrixList = storageDir.subspace(Tuple.from(MATRIX_LIST))
            List<String> matrixNames = new ArrayList<String>();
            for (KeyValue kv : tr.getRange(matrixList.range())) {
                def t = Tuple.fromBytes(kv.getKey())
                matrixNames.add(t.getString(2));
            }
            matrixNames
        } as Function)
    }

    @Override
    boolean containsMatrixName(String name) {
        return containsMatrixName(storageManager.tc, name)
    }

    boolean containsMatrixName(TransactionContext tc, String name) {
        if (tc == null)
            throw new NullPointerException("The transaction context is 'null'")
        if (name == null || name.empty)
            throw new IllegalArgumentException("The storage name is 'null' or empty")

        return tc.run({ tr ->
            DirectoryLayer dir = new DirectoryLayer()
            def storageDir = dir.open(tr, getStoragePath()).get()
            def matrixList = storageDir.subspace(Tuple.from(MATRIX_LIST))
            def val = tr.get(matrixList.pack(Tuple.from(name))).get()
            if (val == null)
                return false
            else
                return true
        } as Function)
    }

    @Override
    public <T extends Serializable> Matrix<T> createMatrix(String name, Class<T> cl, long numRows, long numCols) {
        return createMatrix(storageManager.tc, name, cl, numRows, numRows)
    }


    public <T extends Serializable> Matrix<T> createMatrix(TransactionContext tc, String name, Class<T> cl, long numRows, long numCols) {
        if (tc == null)
            throw new NullPointerException("The transaction context is 'null'")
        if (name == null || name.empty)
            throw new IllegalArgumentException("The storage name is 'null' or empty")
        if (cl == null)
            throw new NullPointerException("The class type is 'null'")
        if (numRows < 1)
            throw new IllegalArgumentException("The number of rows ia less than 1")
        if (numCols < 1)
            throw new IllegalArgumentException("The number of rows is less than 1")
        def storage = this
        return tc.run({ Transaction tr ->
            DirectoryLayer dir = new DirectoryLayer()
            def storagesDir = dir.open(tr, getStoragePath()).get()
            def matrixList = storagesDir.subspace(Tuple.from(MATRIX_LIST))
            tr.set(matrixList.pack(Tuple.from(name)), Tuple.from(cl.getName()).pack())
            storagesDir.createOrOpen(tr, [computeMatrixName(name)]).get()
            return new FoundationDBMatrix<T>(storage, name, numRows, numCols)
        } as Function)
    }

    @Override
    void removeMatrix(String name) {
        removeMatrix(storageManager.tc, name)
    }


    public void removeMatrix(TransactionContext tc, String name) {
        if (tc == null)
            throw new NullPointerException("The transaction context is 'null'")
        if (name == null || name.empty)
            throw new IllegalArgumentException("The matrix name is 'null' or empty")
        DirectoryLayer dir = new DirectoryLayer()
        tc.run({ Transaction tr ->
            def storagesDir = dir.open(tr, getStoragePath()).get()
            def matrixList = storagesDir.subspace(Tuple.from(MATRIX_LIST))
            tr.clear(matrixList.pack(Tuple.from(name)))
            storagesDir.removeIfExists(tr, [computeMatrixName(name)]).get()
        } as Function)
    }

    @Override
    def <T extends Serializable> Matrix<T> getMatrix(String name, Class<T> cl) {
        getMatrix(storageManager.tc, name, cl)
    }

    public <T extends Serializable> FoundationDBMatrix<T> getMatrix(TransactionContext tc, String name, Class<T> cl) {
        if (tc == null)
            throw new NullPointerException("The transaction context is 'null'")
        if (name == null || name.empty)
            throw new IllegalArgumentException("The array name is 'null' or empty")
        if (cl == null)
            throw new NullPointerException("")
        def storage = this
        return tc.run({ Transaction tr ->
            DirectoryLayer dir = new DirectoryLayer()
            def storagesDir = dir.open(tr, getStoragePath()).get()
            def matrixList = storagesDir.subspace(Tuple.from(MATRIX_LIST))
            def ret = tr.get(matrixList.pack(Tuple.from(name))).get()
            if (ret == null)
                return null
            else {
                Class<?> storedCl = Class.forName(Tuple.fromBytes(ret).getString(0))
                if (!storedCl.isAssignableFrom(cl))
                    throw new IllegalArgumentException("The matrix created is of type ${storedCl.toString()} while you requested matrix of type ${cl.toString()}")
                storagesDir.createOrOpen(tr, [computeMatrixName(name)]).get()
                new FoundationDBMatrix<T>(storage, name)
            }
        } as Function)
    }


    @Override
    List<String> getDictionaryNames() {
        getDictionaryNames(storageManager.tc)
    }

    List<String> getDictionaryNames(TransactionContext tc) {
        if (tc == null)
            throw new NullPointerException("The transaction context is 'null'")
        return tc.run({ tr ->
            DirectoryLayer dir = new DirectoryLayer()
            def storageDir = dir.open(tr, getStoragePath()).get()
            def arrayList = storageDir.subspace(Tuple.from(DICTIONARY_LIST))
            List<String> storageNames = new ArrayList<String>();
            for (KeyValue kv : tr.getRange(arrayList.range())) {
                def t = Tuple.fromBytes(kv.getKey())
                storageNames.add(t.getString(2));
            }
            storageNames
        } as Function)
    }

    @Override
    boolean containsDictionaryName(String name) {
        containsDictionaryName(storageManager.tc, name)
    }

    boolean containsDictionaryName(TransactionContext tc, String name) {
        if (tc == null)
            throw new NullPointerException("The transaction context is 'null'")
        if (name == null || name.empty)
            throw new IllegalArgumentException("The dictionary name is 'null' or empty")

        return tc.run({ tr ->
            DirectoryLayer dir = new DirectoryLayer()
            def storageDir = dir.open(tr, getStoragePath()).get()
            def arrayList = storageDir.subspace(Tuple.from(DICTIONARY_LIST))
            def val = tr.get(arrayList.pack(Tuple.from(name))).get()
            if (val == null)
                return false
            else
                return true
        } as Function)
    }

    @Override
    Dictionary createDictionary(String name) {
        createDictionary(storageManager.tc, name)
    }


    FoundationDBDictionary createDictionary(TransactionContext tc, String name) {
        if (tc == null)
            throw new NullPointerException("The transaction context is 'null'")
        if (name == null || name.empty)
            throw new IllegalArgumentException("The dictionary name is 'null' or empty")

        def storage = this
        return tc.run({ Transaction tr ->
            DirectoryLayer dir = new DirectoryLayer()
            def storagesDir = dir.open(tr, getStoragePath()).get()
            def arrayList = storagesDir.subspace(Tuple.from(DICTIONARY_LIST))
            tr.set(arrayList.pack(Tuple.from(name)), Tuple.from(name).pack())
            storagesDir.createOrOpen(tr, [computeDictionaryName(name)]).get()
            return new FoundationDBDictionary(storage, name)
        } as Function)
    }

    @Override
    void removeDictionary(String name) {
        removeDictionary(storageManager.tc, name)
    }

    void removeDictionary(TransactionContext tc, String name) {
        if (tc == null)
            throw new NullPointerException("The transaction context is 'null'")
        if (name == null || name.empty)
            throw new IllegalArgumentException("The dictionary name is 'null' or empty")
        DirectoryLayer dir = new DirectoryLayer()
        tc.run({ Transaction tr ->
            def storagesDir = dir.open(tr, getStoragePath()).get()
            def arrayList = storagesDir.subspace(Tuple.from(DICTIONARY_LIST))
            tr.clear(arrayList.pack(Tuple.from(name)))
            storagesDir.removeIfExists(tr, [computeDictionaryName(name)]).get()
        } as Function)
    }

    @Override
    Dictionary getDictionary(String name) {
        getDictionary(storageManager.tc, name)
    }


    FoundationDBDictionary getDictionary(TransactionContext tc, String name) {
        if (tc == null)
            throw new NullPointerException("The transaction context is 'null'")
        if (name == null || name.empty)
            throw new IllegalArgumentException("The dictionary name is 'null' or empty")
        def storage = this
        return tc.run({ Transaction tr ->
            DirectoryLayer dir = new DirectoryLayer()
            def storagesDir = dir.open(tr, getStoragePath()).get()
            def dictionaryList = storagesDir.subspace(Tuple.from(DICTIONARY_LIST))
            def ret = tr.get(dictionaryList.pack(Tuple.from(name))).get()
            if (ret == null)
                return null
            else {
                storagesDir.createOrOpen(tr, [computeDictionaryName(name)]).get()
                new FoundationDBDictionary(storage, name)
            }
        } as Function)
    }


    @Override
    List<String> getDataStreamNames() {
        getDataStreamNames(storageManager.tc)
    }

    List<String> getDataStreamNames(TransactionContext tc) {
        if (tc == null)
            throw new NullPointerException("The transaction context is 'null'")
        return tc.run({ tr ->
            DirectoryLayer dir = new DirectoryLayer()
            def storageDir = dir.open(tr, getStoragePath()).get()
            def dataStreamList = storageDir.subspace(Tuple.from(DATASTREAM_LIST))
            List<String> names = new ArrayList<String>();
            for (KeyValue kv : tr.getRange(dataStreamList.range())) {
                def t = Tuple.fromBytes(kv.getKey())
                names.add(t.getString(2));
            }
            names
        } as Function)
    }

    @Override
    boolean containsDataStreamName(String name) {
        containsDataStreamName(storageManager.tc, name)
    }

    boolean containsDataStreamName(TransactionContext tc, String name) {
        if (tc == null)
            throw new NullPointerException("The transaction context is 'null'")
        if (name == null || name.empty)
            throw new IllegalArgumentException("The data stream name is 'null' or empty")

        return tc.run({ tr ->
            DirectoryLayer dir = new DirectoryLayer()
            def storageDir = dir.open(tr, getStoragePath()).get()
            def dsList = storageDir.subspace(Tuple.from(DATASTREAM_LIST))
            def val = tr.get(dsList.pack(Tuple.from(name))).get()
            if (val == null)
                return false
            else
                return true
        } as Function)
    }

    @Override
    DataStream createDataStream(String name) {
        createDataStream(storageManager.tc, name)
    }


    DataStream createDataStream(TransactionContext tc, String name) {
        if (tc == null)
            throw new NullPointerException("The transaction context is 'null'")
        if (name == null || name.empty)
            throw new IllegalArgumentException("The data stream name is 'null' or empty")
        def storage = this
        return tc.run({ Transaction tr ->
            DirectoryLayer dir = new DirectoryLayer()
            def storagesDir = dir.open(tr, getStoragePath()).get()
            def arrayList = storagesDir.subspace(Tuple.from(DATASTREAM_LIST))
            tr.set(arrayList.pack(Tuple.from(name)), Tuple.from(name).pack())
            storagesDir.createOrOpen(tr, [computeDataStreamName(name)]).get()
            return new FoundationDBDataStream(storage, name)
        } as Function)
    }

    @Override
    void removeDataStream(String name) {
        removeDataStream(storageManager.tc, name)
    }

    void removeDataStream(TransactionContext tc, String name) {
        if (tc == null)
            throw new NullPointerException("The transaction context is 'null'")
        if (name == null || name.empty)
            throw new IllegalArgumentException("The dictionary name is 'null' or empty")
        DirectoryLayer dir = new DirectoryLayer()
        tc.run({ Transaction tr ->
            def storagesDir = dir.open(tr, getStoragePath()).get()
            def dsList = storagesDir.subspace(Tuple.from(DATASTREAM_LIST))
            tr.clear(dsList.pack(Tuple.from(name)))
            storagesDir.removeIfExists(tr, [computeDataStreamName(name)]).get()
        } as Function)
    }

    @Override
    DataStream getDataStream(String name) {
        getDataStream(storageManager.tc, name)
    }


    FoundationDBDataStream getDataStream(TransactionContext tc, String name) {
        if (tc == null)
            throw new NullPointerException("The transaction context is 'null'")
        if (name == null || name.empty)
            throw new IllegalArgumentException("The data stream name is 'null' or empty")
        def storage = this
        return tc.run({ Transaction tr ->
            DirectoryLayer dir = new DirectoryLayer()
            def storagesDir = dir.open(tr, getStoragePath()).get()
            def dictionaryList = storagesDir.subspace(Tuple.from(DATASTREAM_LIST))
            def ret = tr.get(dictionaryList.pack(Tuple.from(name))).get()
            if (ret == null)
                return null
            else {
                storagesDir.createOrOpen(tr, [computeDataStreamName(name)]).get()
                new FoundationDBDataStream(storage, name)
            }
        } as Function)
    }

    @Override
    void flushData() {

    }
}
