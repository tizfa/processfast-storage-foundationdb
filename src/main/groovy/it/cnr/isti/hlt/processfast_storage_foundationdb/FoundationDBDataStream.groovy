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
