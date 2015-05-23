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
