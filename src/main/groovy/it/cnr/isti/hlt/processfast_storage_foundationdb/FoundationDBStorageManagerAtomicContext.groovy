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
