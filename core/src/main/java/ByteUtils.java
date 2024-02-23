/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * 
 * CHANGES: 
 * - changed package name
 * - removed public access modifier from the class
 * - removed unneeded members
 */

package ra3;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

/**
 * Utility methods for reading and writing bytes.
 *
 * @since 1.14
 */
final class ByteUtils {

    private static void checkReadLength(final int length) {
        if (length > 8) {
            throw new IllegalArgumentException("Can't read more than eight bytes into a long value");
        }
    }

   

    /**
     * Reads the given byte array as a little-endian long.
     *
     * @param bytes the byte array to convert
     * @return the number read
     */
    public static long fromLittleEndian(final byte[] bytes) {
        return fromLittleEndian(bytes, 0, bytes.length);
    }

    /**
     * Reads the given byte array as a little-endian long.
     *
     * @param bytes  the byte array to convert
     * @param off    the offset into the array that starts the value
     * @param length the number of bytes representing the value
     * @return the number read
     * @throws IllegalArgumentException if len is bigger than eight
     */
    public static long fromLittleEndian(final byte[] bytes, final int off, final int length) {
        checkReadLength(length);
        long l = 0;
        for (int i = 0; i < length; i++) {
            l |= (bytes[off + i] & 0xffL) << 8 * i;
        }
        return l;
    }

    // return l;
    // }

    // /**
    //  * Reads the given number of bytes from the given input as little-endian long.
    //  *
    //  * @param in     the input to read from
    //  * @param length the number of bytes representing the value
    //  * @return the number read
    //  * @throws IllegalArgumentException if len is bigger than eight
    //  * @throws IOException              if reading fails or the stream doesn't contain the given number of bytes anymore
    //  */
    public static long fromLittleEndian(final DataInput in, final int length) throws IOException {
        // somewhat duplicates the ByteSupplier version in order to save the creation of a wrapper object
        checkReadLength(length);
        long l = 0;
        for (int i = 0; i < length; i++) {
            final long b = in.readUnsignedByte();
            l |= b << i * 8;
        }
        return l;
    }

    /**
     * Reads the given number of bytes from the given stream as a little-endian long.
     *
     * @param in     the stream to read from
     * @param length the number of bytes representing the value
     * @return the number read
     * @throws IllegalArgumentException if len is bigger than eight
     * @throws IOException              if reading fails or the stream doesn't contain the given number of bytes anymore
     * @deprecated Unused
     */
    @Deprecated
    public static long fromLittleEndian(final InputStream in, final int length) throws IOException {
        // somewhat duplicates the ByteSupplier version in order to save the creation of a wrapper object
        checkReadLength(length);
        long l = 0;
        for (int i = 0; i < length; i++) {
            final long b = in.read();
            if (b == -1) {
                throw new IOException("Premature end of data");
            }
            l |= b << i * 8;
        }
        return l;
    }

    
}