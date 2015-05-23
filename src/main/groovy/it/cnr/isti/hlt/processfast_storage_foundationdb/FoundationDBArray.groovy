package it.cnr.isti.hlt.processfast_storage_foundationdb

import com.foundationdb.KeySelector
import com.foundationdb.KeyValue
import com.foundationdb.Transaction
import com.foundationdb.TransactionContext
import com.foundationdb.async.Function
import com.foundationdb.directory.DirectoryLayer
import com.foundationdb.tuple.Tuple
import it.cnr.isti.hlt.processfast.data.Array
import it.cnr.isti.hlt.processfast.data.ArrayDataSourceIteratorProvider
import it.cnr.isti.hlt.processfast.data.ArrayIterator
import it.cnr.isti.hlt.processfast.data.ArrayPairDataSourceIteratorProvider
import it.cnr.isti.hlt.processfast.data.ImmutableDataSourceIteratorProvider
import it.cnr.isti.hlt.processfast.utils.Pair


/**
 * An array implementation based on FoundationDB.
 *
 * @author Tiziano Fagni (tiziano.fagni@isti.cnr.it)
 * @since 1.0.0
 */
class FoundationDBArray<T extends Serializable> implements Array<T> {

    class CachedItem<T> {
        T value
        boolean modified
    }

    private static final String ARRAY_SIZE_KEY = "size_key"
    private static final String DEFAULT_VALUE_KEY = "default_value_key"
    private static final String ITEM_PREFIX = "it_"
    private static final String NULL_VALUE = "**__N__**"

    /**
     * The parent storage.
     */
    final FoundationDBStorage storage

    /**
     * The array name.
     */
    final String name

    /**
     * The cache used internally.
     */
    final HashMap<Long, CachedItem<T>> cache = new HashMap<>(10000)

    /**
     * Create an empty array with dimension 0.
     *
     * @param storage The parent storage.
     * @param arrayName The name of the array.
     */
    FoundationDBArray(TransactionContext tc, FoundationDBStorage storage, String arrayName) {
        if (storage == null)
            throw new NullPointerException("The storage  is 'null'")
        if (arrayName == null || arrayName.empty)
            throw new IllegalArgumentException("The array name is 'null' or empty")
        this.storage = storage
        this.name = arrayName

        resize(tc, 0)
    }

    List<String> getArrayPath() {
        return [storage.getStoragePath(), storage.computeArrayName(name)].flatten()
    }

    @Override
    long size() {
        size(storage.storageManager.tc)
    }

    long size(TransactionContext tc) {
        if (tc == null)
            throw new NullPointerException("The transaction context is 'null'")
        return tc.run({ Transaction tr ->
            DirectoryLayer dir = new DirectoryLayer()
            def storagesDir = dir.open(tr, getArrayPath()).get()
            def ret = tr.get(storagesDir.pack(Tuple.from(ARRAY_SIZE_KEY))).get()
            if (ret != null)
                return Tuple.fromBytes(ret).getLong(0)
            else
                return -1
        } as Function)
    }


    Tuple computeItem(long index) {
        Tuple.from(ITEM_PREFIX, index)
    }


    @Override
    List<T> getValues(long fromIndex, long toIndex) {
        getValues(storage.storageManager.tc, fromIndex, toIndex)
    }

    List<T> getValues(TransactionContext tc, long fromIndex, long toIndex) {
        if (tc == null)
            throw new NullPointerException("The transaction context is 'null'")
        long curSize = size(tc)
        long to = toIndex
        if (fromIndex >= to)
            return []
        if (to >= curSize)
            to = curSize
        if (fromIndex < 0)
            throw new IllegalArgumentException("The fromIndex value is less than 0")

        long toAdd = to - fromIndex
        List<T> values = new ArrayList<T>((int) toAdd);
        for (long i = 0; i < toAdd; i++)
            values.add(getDefaultValue())

        tc.run({ tr ->
            DirectoryLayer dir = new DirectoryLayer()
            def storagesDir = dir.open(tr, getArrayPath()).get()
            def range = tr.getRange(storagesDir.pack(computeItem(fromIndex)), storagesDir.pack(computeItem(to)))
            for (KeyValue kv : range) {
                long curIndex = Tuple.fromBytes(kv.getKey()).getLong(2);
                def v = IOUtils.fromByteArray(kv.getValue())
                if (NULL_VALUE.equals(v))
                    v = null
                values.set((int) curIndex-fromIndex, v)
            }
        } as Function)

        synchronized (cache) {
            def iter = cache.keySet().iterator()
            while (iter.hasNext()) {
                long index = iter.next()
                T value = cache.get(index).value
                values.set((int) index, value)
            }
        }
        values
    }

    @Override
    T getValue(long index) {
        getValue(storage.storageManager.tc, index)
    }


    T getValue(TransactionContext tc, long index) {
        if (tc == null)
            throw new NullPointerException("The transaction context is 'null'")
        synchronized (cache) {
            if (cache.containsKey(index))
                return cache.get(index).value
        }
        tc.run({ Transaction tr ->
            long currentSize = size(tr)
            if (index < 0 || index >= currentSize)
                throw new IllegalArgumentException("The requested index is not valid. Min index: 0, Max index: ${currentSize - 1}, Requested index: ${index}")

            DirectoryLayer dir = new DirectoryLayer()
            def storagesDir = dir.open(tr, getArrayPath()).get()
            def value = tr.get(storagesDir.pack(computeItem(index))).get()
            if (value == null)
            // Return default value.
                getDefaultValue()
            else {
                def v = IOUtils.fromByteArray(value)
                if (NULL_VALUE.equals(v))
                    return null
                else
                    return v
            }
        } as Function)
    }


    @Override
    void setValue(long index, T value) {
        setValue(storage.storageManager.tc, index, value)
    }

    void setValue(TransactionContext tc, long index, T value) {
        if (tc == null)
            throw new NullPointerException("The transaction context is 'null'")
        synchronized (cache) {
            if (cache.containsKey(index)) {
                def cachedItem = cache.get(index)
                cachedItem.value = value
                cachedItem.modified = true
                return
            }
        }
        tc.run({ Transaction tr ->
            long currentSize = size(tr)
            if (index < 0 || index >= currentSize)
                throw new IllegalArgumentException("The requested index is not valid. Min index: 0, Max index: ${currentSize - 1}, Requested index: ${index}")

            DirectoryLayer dir = new DirectoryLayer()
            def storagesDir = dir.open(tr, getArrayPath()).get()
            Serializable toWrite = value
            if (toWrite == null)
                toWrite = NULL_VALUE
            tr.set(storagesDir.pack(computeItem(index)), IOUtils.toByteArray(toWrite))
        } as Function)
    }

    @Override
    void appendValue(T value) {
        appendValue(storage.storageManager.tc, value)
    }


    void appendValue(TransactionContext tc, T value) {
        if (tc == null)
            throw new NullPointerException("The transaction context is 'null'")
        tc.run({ Transaction tr ->
            long currentSize = size(tr)
            resize(tr, currentSize + 1)
            setValue(tr, currentSize, value)
        } as Function)
    }


    @Override
    void appendValues(long numItems, T value) {
        appendValues(storage.storageManager.tc, numItems, value)
    }


    void appendValues(TransactionContext tc, long numItems, T value) {
        if (tc == null)
            throw new NullPointerException("The transaction context is 'null'")
        if (numItems < 1)
            throw new IllegalArgumentException("The number of items to add is less than 1. Requested value: ${numItems}")
        tc.run({ Transaction tr ->
            long currentSize = size(tr)
            long newSize = currentSize + numItems
            resize(tr, newSize)
            for (int i = currentSize; i < newSize; i++) {
                setValue(tr, i, value)
            }
        } as Function)
    }

    @Override
    T getDefaultValue() {
        getDefaultValue(storage.storageManager.tc)
    }

    T getDefaultValue(TransactionContext tc) {
        if (tc == null)
            throw new NullPointerException("The transaction context is 'null'")
        tc.run({ Transaction tr ->
            DirectoryLayer dir = new DirectoryLayer()
            def storagesDir = dir.open(tr, getArrayPath()).get()
            def value = tr.get(storagesDir.pack(Tuple.from(DEFAULT_VALUE_KEY))).get()
            if (value == null)
            // Return null.
                return null
            else {
                def v = IOUtils.fromByteArray(value)
                if (NULL_VALUE.equals(v))
                    return null
                else
                    return v
            }
        } as Function)
    }

    @Override
    void setDefaultValue(T defaultValue) {
        setDefaultValue(storage.storageManager.tc, defaultValue)
    }

    void setDefaultValue(TransactionContext tc, T defaultValue) {
        if (tc == null)
            throw new NullPointerException("The transaction context is 'null'")
        tc.run({ Transaction tr ->
            DirectoryLayer dir = new DirectoryLayer()
            def storagesDir = dir.open(tr, getArrayPath()).get()
            Serializable valToWrite = NULL_VALUE
            if (defaultValue != null)
                valToWrite = defaultValue
            byte[] toWrite = IOUtils.toByteArray(valToWrite)
            tr.set(storagesDir.pack(Tuple.from(DEFAULT_VALUE_KEY)), toWrite)
        } as Function)
    }

    @Override
    void clear() {
        clear(storage.storageManager.tc)
    }

    void clear(TransactionContext tc) {
        resize(tc, 0)
    }


    @Override
    void resize(long newSize) {
        resize(storage.storageManager.tc, newSize)
    }

    /**
     * Compute a pair where the first value is less equals than second
     * value.
     *
     * @param s1 The first value to compare.
     * @param s2 The second value to compare.
     * @return A pair with first value less equals than second value.
     */
    protected Pair<String, String> getOrderedPair(String s1, String s2) {
        if (s1.compareTo(s2) < 0)
            return new Pair<String, String>(s1, s2)
        else if (s1.compareTo(s2) > 0)
            return new Pair<String, String>(s2, s1)
        else
            return new Pair<String, String>(s1, s2)
    }

    void resize(TransactionContext tc, long newSize) {
        if (tc == null)
            throw new NullPointerException("The transaction context is 'null'")
        if (newSize < 0)
            throw new IllegalArgumentException("The new getNumCols is not valid: ${newSize}")

        long oldSize = size(tc)
        tc.run({ Transaction tr ->
            DirectoryLayer dir = new DirectoryLayer()
            def storagesDir = dir.open(tr, getArrayPath()).get()
            if (oldSize == newSize)
                return
            tr.set(storagesDir.pack(Tuple.from(ARRAY_SIZE_KEY)), Tuple.from(newSize).pack())
            if (oldSize > newSize) {
                // Remove all items with key greater than maximum getNumCols.
                tr.clear(storagesDir.pack(computeItem(newSize)), storagesDir.pack(computeItem(oldSize)))
            }
        } as Function)

        // Remove cached items.
        synchronized (cache) {
            for (long i = newSize; i < oldSize; i++) {
                cache.remove(i)
            }
        }
    }


    @Override
    Iterator<T> asIterator(long numBufferedItems) {
        return new ArrayIterator<T>(this, numBufferedItems)
    }

    @Override
    Array<T> copyFrom(Array<T> source, boolean clearArrayContent, long numBufferedItems) {
        copyFrom(storage.storageManager.tc, source, clearArrayContent, numBufferedItems)
    }

    Array<T> copyFrom(TransactionContext tc, Array<T> source, boolean clearArrayContent, long numBufferedItems) {
        if (tc == null)
            throw new NullPointerException("The transaction context is 'null'")
        if (source == null)
            throw new NullPointerException("The source array is 'null'")

        if (clearArrayContent)
            clear(tc)
        long sourceSize = source.size()
        long startFrom = size(tc)
        resize(startFrom + sourceSize)
        long numRead = 0
        boolean done = false
        while (!done) {
            def values = source.getValues(numRead, numRead + numBufferedItems)
            if (values.size() == 0) {
                done = true
                break
            }

            tc.run({ Transaction tr ->
                for (int i = 0; i < values.size(); i++) {
                    setValue(tr, startFrom+numRead+i, values.get(i))
                }
            } as Function)
            numRead += values.size()
        }
        this
    }

    @Override
    Array<T> copyFrom(Collection<T> source, boolean clearArrayContent, long numBufferedItems) {
        copyFrom(storage.storageManager.tc, source, clearArrayContent, numBufferedItems)
    }

    Array<T> copyFrom(TransactionContext tc, Collection<T> source, boolean clearArrayContent, long numBufferedItems) {
        if (tc == null)
            throw new NullPointerException("The transaction context is 'null'")
        if (source == null)
            throw new NullPointerException("The source collection is 'null'")

        def iter = source.iterator()

        if (clearArrayContent)
            clear(tc)
        long sourceSize = source.size()
        long startFrom = size(tc)
        resize(startFrom + sourceSize)
        long numRead = 0
        boolean done = false
        while (!done) {
            def values = readValuesFromIterator(iter, numBufferedItems)
            if (values.size() == 0) {
                done = true
                break
            }

            tc.run({ Transaction tr ->
                for (int i = 0; i < values.size(); i++) {
                    setValue(tr, startFrom+numRead+i, values.get(i))
                }
            } as Function)
            numRead += values.size()
        }
        this
    }

    protected List<T> readValuesFromIterator(java.util.Iterator<T> iter, long numToRead) {
        List<T> l = new ArrayList<>()
        while (iter.hasNext()) {
            l.add(iter.next())
        }
        l
    }

    @Override
    void copyTo(Collection<T> dest, boolean clearList, long numBufferedItems) {
        if (clearList)
            dest.clear()
        ArrayIterator<T> iter = new ArrayIterator<T>(this, numBufferedItems)
        while (iter.hasNext()) {
            T item = iter.next()
            dest.add(item)
        }
    }

    @Override
    void copyTo(Array<T> dest, boolean clearArray, long numBufferedItems) {
        dest.copyFrom(this, clearArray, numBufferedItems)
    }

    @Override
    ImmutableDataSourceIteratorProvider<T> asIteratorProvider(long numBufferedItems) {
        if (numBufferedItems < 1)
            throw new IllegalArgumentException("The number of buffered items is less than 1")
        return new ArrayDataSourceIteratorProvider<T>(this, numBufferedItems)
    }

    @Override
    ImmutableDataSourceIteratorProvider<Pair<Long, T>> asIteratorProviderWithIndex(long numBufferedItems) {
        if (numBufferedItems < 1)
            throw new IllegalArgumentException("The number of buffered items is less than 1")
        return new ArrayPairDataSourceIteratorProvider<T>(this, numBufferedItems)
    }

    @Override
    void enableLocalCache(boolean enabled, long fromIndex, long toIndex) {
        long curSize = size()
        long to = toIndex
        if (toIndex == -1)
            to = curSize
        if (fromIndex >= to)
            return
        if (to >= curSize)
            to = curSize
        if (fromIndex < 0)
            throw new IllegalArgumentException("The fromIndex value is less than 0")

        if (enabled){
            // Load items in memory.
            List<T> values = getValues(fromIndex, to)
            synchronized (cache) {
                for (long i = 0; i < values.size(); i++) {
                    if (!cache.containsKey(fromIndex + i)) {
                        cache.put(fromIndex + i, new CachedItem<T>(value: values.get((int) i), modified: false))
                    }
                }
            }
        } else {
            synchronized (cache){
                // Discard items from memory.
                for (long i = fromIndex; i < to; i++) {
                    cache.remove(i)
                }
            }
        }
    }

    @Override
    boolean isLocalCacheEnabled(long index) {
        synchronized (cache) {
            return cache.containsKey(index)
        }
    }

    @Override
    void flush() {
        HashMap<Long, CachedItem> toUpdate = [:]
        synchronized (cache) {
            def iter = cache.keySet().iterator()
            while (iter.hasNext()) {
                long index = iter.next()
                def item = cache.get(index)
                if (item.modified) {
                    toUpdate.put(index, item)
                    item.modified = false
                }
            }
        }

        def iter = toUpdate.keySet().iterator()
        while (iter.hasNext()) {
            long index = iter.next()
            def item = toUpdate.get(index)
            enableLocalCache(false, index, index+1)
            setValue(index, item.value)
            enableLocalCache(true, index, index+1)
        }
    }
}
