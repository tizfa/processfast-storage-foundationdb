package it.cnr.isti.hlt.processfast_storage_foundationdb

import com.foundationdb.Database
import com.foundationdb.FDB
import it.cnr.isti.hlt.processfast.data.StorageManager
import it.cnr.isti.hlt.processfast.data.StorageManagerProvider

/**
 * A storage provider for FoundationDB NoSql system.
 *
 * @author Tiziano Fagni (tiziano.fagni@isti.cnr.it)
 * @since 1.0.0
 */
class FoundationDBStorageManagerProvider implements StorageManagerProvider {

    FDB fdb;
    /**
     * The FoundationDB database handle.
     */
    Database db;

    /**
     * The main logical path used to store data. The path has
     * the form p1/p2/... where each subpath is separated by a
     * 'slash' character.
     */
    final String mainDirectoryPath

    /**
     * The cluster file path used to get coordinates for FoundationDB connection. If 'null', it
     * will be used the default filename "fdb.cluster" in default data directory.
     */
    String clusterFilePath = null


    FoundationDBStorageManagerProvider(String mainDirectoryPath) {
        if (mainDirectoryPath == null || mainDirectoryPath.empty)
            throw new IllegalArgumentException("The specified directory path is 'null' or empty")
        this.mainDirectoryPath = mainDirectoryPath
    }

    @Override
    StorageManager getStorageManager(String clientID) {
        return new FoundationDBStorageManager(this, db, clientID)
    }

    @Override
    synchronized  void open() {
        if (db == null) {
            fdb = FDB.selectAPIVersion(300);
            if (clusterFilePath == null)
                db = fdb.open();
            else
                db = fdb.open(clusterFilePath)
        }
    }

    @Override
    synchronized void close() {
        if (db != null) {
            db.dispose()
        }
    }

    /**
     * Get the FoundationDB storage path corresponding to {@link #mainDirectoryPath}.
     *
     * @return The FoundationDB storage path
     */
    List<String> getFoundationDBStoragePath() {
        mainDirectoryPath.split("/").grep{it.length() != 0}.toList()
    }
}
