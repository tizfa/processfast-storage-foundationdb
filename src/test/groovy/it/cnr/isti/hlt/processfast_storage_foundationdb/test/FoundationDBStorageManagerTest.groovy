package it.cnr.isti.hlt.processfast_storage_foundationdb.test

import it.cnr.isti.hlt.processfast.data.AbstractStorageManagerTest
import it.cnr.isti.hlt.processfast.data.StorageManager
import it.cnr.isti.hlt.processfast_storage_foundationdb.FoundationDBStorageManagerProvider
import org.junit.AfterClass
import org.junit.BeforeClass

/**
 * @author Tiziano Fagni (tiziano.fagni@isti.cnr.it)
 */
class FoundationDBStorageManagerTest extends AbstractStorageManagerTest {
    static FoundationDBStorageManagerProvider provider

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

    @Override
    protected StorageManager initStorageManager() {
        def sm = provider.getStorageManager("clientID")
        sm.clear()
        sm
    }
}
