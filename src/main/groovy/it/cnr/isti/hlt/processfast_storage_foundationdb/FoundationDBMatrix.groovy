package it.cnr.isti.hlt.processfast_storage_foundationdb

import com.foundationdb.KeyValue
import com.foundationdb.Range
import com.foundationdb.Transaction
import com.foundationdb.TransactionContext
import com.foundationdb.async.Function
import com.foundationdb.directory.DirectoryLayer
import com.foundationdb.tuple.Tuple
import it.cnr.isti.hlt.processfast.data.DataIterable
import it.cnr.isti.hlt.processfast.data.ImmutableDataSourceIteratorProvider
import it.cnr.isti.hlt.processfast.data.Matrix
import it.cnr.isti.hlt.processfast.utils.Pair

/**
 * A matrix implementation based on FoundationDB.
 *
 * @author Tiziano Fagni (tiziano.fagni@isti.cnr.it)
 * @since 1.0.0
 */
class FoundationDBMatrix<T extends Serializable> implements Matrix<T> {

    class CachedItem<T> {
        T value
        boolean modified
    }


    private static final String MATRIX_NUM_COLS_KEY = "m_cols_key"
    private static final String MATRIX_NUM_ROWS_KEY = "m_rows_key"
    private static final String DEFAULT_VALUE_KEY = "default_value_key"
    private static final String ROW_ITEM_PREFIX = "row_"
    private static final String COL_ITEM_PREFIX = "col_"
    private static final String NULL_VALUE = "**__N__**"

    /**
     * The cache used internally.
     */
    final HashMap<String, CachedItem<T>> cache = new HashMap<>(10000)

    /**
     * The parent storage.
     */
    final FoundationDBStorage storage

    /**
     * The matrix name.
     */
    final String name

    FoundationDBMatrix(FoundationDBStorage storage, String matrixName, long numRows, long numCols) {
        this(storage, matrixName)

        if (numRows < 1)
            throw new IllegalArgumentException("The number of rows ia less than 1")
        if (numCols < 1)
            throw new IllegalArgumentException("The number of rows is less than 1")

        // Resize the matrix to requested getNumCols.
        resize(numRows)
        resizeColumns(numCols)
    }


    FoundationDBMatrix(FoundationDBStorage storage, String matrixName) {
        if (storage == null)
            throw new NullPointerException("The storage  is 'null'")
        if (matrixName == null || matrixName.empty)
            throw new IllegalArgumentException("The matrix name is 'null' or empty")

        this.storage = storage
        this.name = matrixName

        // Create an empty matrix.
        resize(0,)
        resizeColumns(0)
    }


    List<String> getMatrixPath() {
        return [storage.getStoragePath(), storage.computeMatrixName(name)].flatten()
    }


    @Override
    long getNumCols() {
        getNumCols(storage.storageManager.tc)
    }

    long getNumCols(TransactionContext tc) {
        if (tc == null)
            throw new NullPointerException("The transaction context is 'null'")
        return tc.run({ Transaction tr ->
            DirectoryLayer dir = new DirectoryLayer()
            def storagesDir = dir.open(tr, getMatrixPath()).get()
            def ret = tr.get(storagesDir.pack(Tuple.from(MATRIX_NUM_COLS_KEY))).get()
            if (ret != null)
                return Tuple.fromBytes(ret).getLong(0)
            else
                return -1
        } as Function)
    }


    @Override
    long getNumRows() {
        getNumRows(storage.storageManager.tc)
    }

    long getNumRows(TransactionContext tc) {
        if (tc == null)
            throw new NullPointerException("The transaction context is 'null'")
        return tc.run({ Transaction tr ->
            DirectoryLayer dir = new DirectoryLayer()
            def storagesDir = dir.open(tr, getMatrixPath()).get()
            def ret = tr.get(storagesDir.pack(Tuple.from(MATRIX_NUM_ROWS_KEY))).get()
            if (ret != null)
                return Tuple.fromBytes(ret).getLong(0)
            else
                return -1
        } as Function)
    }


    @Override
    void resize(long numRows, long numCols) {
        resize(storage.storageManager.tc, numRows)
    }


    void resize(TransactionContext tc, long numRows, long numCols) {
        if (tc == null)
            throw new NullPointerException("The transaction context is 'null'")
        if (numRows < 0)
            throw new IllegalArgumentException("The new size is not valid: ${numRows}")

        long oldSize = getNumRows(tc)
        tc.run({ Transaction tr ->
            DirectoryLayer dir = new DirectoryLayer()
            def storagesDir = dir.open(tr, getMatrixPath()).get()
            if (oldSize == numRows)
                return
            tr.set(storagesDir.pack(Tuple.from(MATRIX_NUM_ROWS_KEY)), Tuple.from(numRows).pack())
            tr.set(storagesDir.pack(Tuple.from(MATRIX_NUM_COLS_KEY)), Tuple.from(numCols).pack())

            tr.clear(Range.startsWith(storagesDir.pack(Tuple.from(ROW_ITEM_PREFIX))));
            tr.clear(Range.startsWith(storagesDir.pack(Tuple.from(COL_ITEM_PREFIX))));
        } as Function)

        // Remove cached items.
        // Remove cached items.
        synchronized (cache) {
            cache.clear()
        }
    }


    Tuple computeRowItem(long row, long col) {
        Tuple.from(ROW_ITEM_PREFIX, row, col)
    }

    Tuple computeColItem(long row, long col) {
        Tuple.from(COL_ITEM_PREFIX, col, row)
    }


    @Override
    T getValue(long row, long column) {
        getValue(storage.storageManager.tc, row, column)
    }


    protected String computeCacheKey(long row, long col) {
        return "" + row + "_" + col
    }

    T getValue(TransactionContext tc, long row, long col) {
        if (tc == null)
            throw new NullPointerException("The transaction context is 'null'")
        String index = computeCacheKey(row, col)
        synchronized (cache) {
            if (cache.containsKey(index))
                return cache.get(index).value
        }
        tc.run({ Transaction tr ->
            long numRows = getNumRows(tr)
            if (row < 0 || row >= numRows)
                throw new IllegalArgumentException("The requested row index is not valid. Min index: 0, Max index: ${numRows - 1}, Requested index: ${row}")
            long numCols = getNumCols(tr)
            if (col < 0 || col >= numCols)
                throw new IllegalArgumentException("The requested column index is not valid. Min index: 0, Max index: ${numCols - 1}, Requested index: ${col}")

            DirectoryLayer dir = new DirectoryLayer()
            def storagesDir = dir.open(tr, getMatrixPath()).get()
            def value = tr.get(storagesDir.pack(computeRowItem(row, col))).get()
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
    void setValue(long row, long column, T value) {
        setValue(storage.storageManager.tc, row, column, value)
    }


    void setValue(TransactionContext tc, long row, long column, T value) {
        if (tc == null)
            throw new NullPointerException("The transaction context is 'null'")
        String index = computeCacheKey(row, column)
        synchronized (cache) {
            if (cache.containsKey(index)) {
                def cachedItem = cache.get(index)
                cachedItem.value = value
                cachedItem.modified = true
                return
            }
        }
        tc.run({ Transaction tr ->
            long numRows = getNumRows(tr)
            if (row < 0 || row >= numRows)
                throw new IllegalArgumentException("The requested row index is not valid. Min index: 0, Max index: ${numRows - 1}, Requested index: ${row}")
            long numCols = getNumCols(tr)
            if (column < 0 || column >= numCols)
                throw new IllegalArgumentException("The requested column index is not valid. Min index: 0, Max index: ${numCols - 1}, Requested index: ${col}")

            DirectoryLayer dir = new DirectoryLayer()
            def storagesDir = dir.open(tr, getMatrixPath()).get()
            Serializable toWrite = value
            if (toWrite == null)
                toWrite = NULL_VALUE
            def serialized = IOUtils.toByteArray(toWrite)
            tr.set(storagesDir.pack(computeRowItem(row, column)), serialized)
            tr.set(storagesDir.pack(computeColItem(row, column)), serialized)
        } as Function)
    }

    @Override
    void setDefaultValue(T value) {
        setDefaultValue(storage.storageManager.tc, value)
    }


    void setDefaultValue(TransactionContext tc, T defaultValue) {
        if (tc == null)
            throw new NullPointerException("The transaction context is 'null'")
        tc.run({ Transaction tr ->
            DirectoryLayer dir = new DirectoryLayer()
            def storagesDir = dir.open(tr, getMatrixPath()).get()
            Serializable valToWrite = NULL_VALUE
            if (defaultValue != null)
                valToWrite = defaultValue
            byte[] toWrite = IOUtils.toByteArray(valToWrite)
            tr.set(storagesDir.pack(Tuple.from(DEFAULT_VALUE_KEY)), toWrite)
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
            def storagesDir = dir.open(tr, getMatrixPath()).get()
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
    List<T> getRowValues(long row, long startCol, long endCol) {
        getRowValues(storage.storageManager.tc, row, startCol, endCol)
    }

    List<T> getRowValues(TransactionContext tc, long row, long startCol, long endCol) {
        if (tc == null)
            throw new NullPointerException("The transaction context is 'null'")

        long numRows = getNumRows(tc)
        if (row < 0 || row >= numRows)
            throw new IllegalArgumentException("The row index is not valid. Current value: ${row}, Max value: ${numRows}")
        long numCols = getNumCols(tc)
        if (startCol < 0 || startCol >= numCols)
            throw new IllegalArgumentException("The startRow index is not valid. Current value: ${startCol}, Max value: ${numCols-1}")
        if (endCol <= startCol || endCol > numCols)
            throw new IllegalArgumentException("The endCol index is not valid. Current value: ${endCol}, Max value: ${numCols}")

        long toAdd = endCol - startCol
        List<T> values = new ArrayList<T>((int) toAdd);
        for (long i = 0; i < toAdd; i++)
            values.add(getDefaultValue())

        tc.run({ tr ->
            DirectoryLayer dir = new DirectoryLayer()
            def storagesDir = dir.open(tr, getMatrixPath()).get()
            def range = tr.getRange(storagesDir.pack(computeRowItem(row, startCol)), storagesDir.pack(computeRowItem(row, endCol)))
            for (KeyValue kv : range) {
                def tu = Tuple.fromBytes(kv.getKey())
                long r = tu.getLong(2);
                long c = tu.getLong(3);
                def v = IOUtils.fromByteArray(kv.getValue())
                if (NULL_VALUE.equals(v))
                    v = null
                values.set((int) c-startCol, v)
            }
        } as Function)

        synchronized (cache) {
            def iter = cache.keySet().iterator()
            while (iter.hasNext()) {
                long index = iter.next()
                T value = cache.get(index).value
                long r = Long.parseLong(index.split("_")[0])
                long c = Long.parseLong(index.split("_")[1])
                if (r == row && (c >= startCol || c < endCol))
                    values.set((int) c-startCol, value)
            }
        }
        values
    }


    @Override
    List<T> getColValues(long col, long startRow, long endRow) {
        getColValues(storage.storageManager.tc, col, startRow, endRow)
    }


    List<T> getColValues(TransactionContext tc, long col, long startRow, long endRow) {
        if (tc == null)
            throw new NullPointerException("The transaction context is 'null'")

        long numCols = getNumCols(tc)
        if (col < 0 || col >= numCols)
            throw new IllegalArgumentException("The col index is not valid. Current value: ${col}, Max value: ${numCols}")
        long numRows = getNumRows(tc)
        if (startRow < 0 || startRow >= numRows)
            throw new IllegalArgumentException("The startRow index is not valid. Current value: ${startRow}, Max value: ${numRows-1}")
        if (endRow <= startRow || endRow > numRows)
            throw new IllegalArgumentException("The endRow index is not valid. Current value: ${endRow}, Max value: ${numRows}")

        long toAdd = endRow - startRow
        List<T> values = new ArrayList<T>((int) toAdd);
        for (long i = 0; i < toAdd; i++)
            values.add(getDefaultValue())

        tc.run({ tr ->
            DirectoryLayer dir = new DirectoryLayer()
            def storagesDir = dir.open(tr, getMatrixPath()).get()
            def range = tr.getRange(storagesDir.pack(computeColItem(startRow, col)), storagesDir.pack(computeColItem(endRow, col)))
            for (KeyValue kv : range) {
                def tu = Tuple.fromBytes(kv.getKey())
                long c = tu.getLong(2);
                long r = tu.getLong(3);
                def v = IOUtils.fromByteArray(kv.getValue())
                if (NULL_VALUE.equals(v))
                    v = null
                values.set((int) r-startRow, v)
            }
        } as Function)

        synchronized (cache) {
            def iter = cache.keySet().iterator()
            while (iter.hasNext()) {
                long index = iter.next()
                T value = cache.get(index).value
                long r = Long.parseLong(index.split("_")[0])
                long c = Long.parseLong(index.split("_")[1])
                if (c == col && (r >= startRow || r < endRow))
                    values.set((int) r-startRow, value)
            }
        }
        values
    }

    @Override
    void enableLocalCache(boolean enabled, long fromRowIndex, long toRowIndex, long fromColumnIndex, long toColumnIndex) {
        long numRows = getNumRows()
        long numCols = getNumCols()
        if (fromRowIndex < 0 || fromRowIndex >= numRows)
            throw new IllegalArgumentException("The requested fromRowIndex is less than 0 or greater equals than max num rows (${numRows}): Current value: ${fromRowIndex}")
        if (toRowIndex <= fromRowIndex)
            throw new IllegalArgumentException("The requested toRowIndex is less equals than fromRowIndex ${fromRowIndex}: Current value: ${toRowIndex}")
        if (fromColumnIndex < 0 || fromColumnIndex >= numCols)
            throw new IllegalArgumentException("The requested fromColumnIndex is less than 0 or greater equals than max num rows (${numCols}): Current value: ${fromColumnIndex}")
        if (toColumnIndex <= fromColumnIndex)
            throw new IllegalArgumentException("The requested toColumnIndex is less equals than fromColumnIndex ${fromColumnIndex}: Current value: ${toColumnIndex}")

        if (toRowIndex > numRows)
            toRowIndex = numRows
        if (toColumnIndex > numCols)
            toColumnIndex = numCols


        for (long row = fromRowIndex; row < toRowIndex; row++) {
            List<T> values = getRowValues(row, fromColumnIndex, toColumnIndex)
            synchronized (cache) {
                for (long col = fromColumnIndex; col < toColumnIndex; col++) {
                    if (enabled) {
                        if (!cache.containsKey(computeCacheKey(row, col))) {
                            String index = computeCacheKey(row, col)
                            T val = getValue(row, col)
                            cache.put(index, new CachedItem<T>(value: val, modified: false))
                        }
                    } else {
                        if (!cache.containsKey(computeCacheKey(row, col))) {
                            String index = computeCacheKey(row, col)
                            cache.remove(index)
                        }
                    }
                }
            }
        }
    }

    @Override
    boolean isLocalCacheEnabled(long row, long col) {
        synchronized (cache) {
            return cache.containsKey(computeCacheKey(row, col))
        }
    }

    @Override
    void flush() {
        HashMap<String, CachedItem> toUpdate = [:]
        synchronized (cache) {
            def iter = cache.keySet().iterator()
            while (iter.hasNext()) {
                String index = iter.next()
                def item = cache.get(index)
                if (item.modified) {
                    toUpdate.put(index, item)
                    item.modified = false
                }
            }
        }

        def iter = toUpdate.keySet().iterator()
        while (iter.hasNext()) {
            String index = iter.next()
            long row = Long.parseLong(index.split("_")[0])
            long col = Long.parseLong(index.split("_")[1])
            def item = toUpdate.get(index)
            enableLocalCache(false, row, row + 1, col, col + 1)
            setValue(row, col, item.value)
            enableLocalCache(true, row, row + 1, col, col + 1)
        }
    }
}

