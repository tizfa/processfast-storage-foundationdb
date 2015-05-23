package it.cnr.isti.hlt.processfast_storage_foundationdb.test

import it.cnr.isti.hlt.processfast.data.AbstractArrayTest
import it.cnr.isti.hlt.processfast.data.Array
import it.cnr.isti.hlt.processfast.data.Storage
import it.cnr.isti.hlt.processfast_storage_foundationdb.FoundationDBStorageManagerProvider
import org.junit.AfterClass
import org.junit.BeforeClass

/**
 * @author Tiziano Fagni (tiziano.fagni@isti.cnr.it)
 */
class FoundationDBArrayTest extends AbstractArrayTest {

    static FoundationDBStorageManagerProvider provider

    @Override
    protected Array<Double> initArray(String name, boolean clearData) {
        def sm = provider.getStorageManager("clientID")
        if (clearData)
            sm.clear()
        def storage = sm.createStorage("test")
        storage.createArray(name, Double.class)
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
