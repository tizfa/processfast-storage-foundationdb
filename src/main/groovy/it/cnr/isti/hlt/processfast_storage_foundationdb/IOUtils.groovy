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

/**
 * An utils class for I/O methods.
 *
 * @author Tiziano Fagni (tiziano.fagni@isti.cnr.it)
 */
class IOUtils {

    /**
     * Serialize the specified object to a byte array.
     *
     * @param t The object to serialize.
     * @return The corresponding byte array.
     */
    static <T extends Serializable> byte[] toByteArray(T t) {
        if (t == null)
            throw new NullPointerException("The specified object is 'null'")
        ByteArrayOutputStream bos = new ByteArrayOutputStream()
        ObjectOutputStream os = new ObjectOutputStream(bos)
        os.writeObject(t)
        os.close()
        return bos.toByteArray()
    }

    /**
     * Deserialize the specified byte array into a specific object.
     *
     * @param bytes The array to be deserialized.
     * @return The corresponding object.
     */
    static <T extends Serializable> T fromByteArray(byte[] bytes) {
        if (bytes == null)
            throw new NullPointerException("The bytes array is 'null'")
        ByteArrayInputStream bis = new ByteArrayInputStream(bytes)
        ObjectInputStream ois = new ObjectInputStream(bis)
        return (T) ois.readObject()
    }
}
