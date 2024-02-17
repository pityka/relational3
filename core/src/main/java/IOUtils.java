/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 * 
 * CHANGES: 
 * - changed package name
 * - removed public access modifier from the class
 * - removed unneeded members
 * - copied the implementation of skip() from the apache commons io source tree which is distributed under the same license
 * - skip() allocates in each iteration
 */
package ra3.commons;

import java.io.ByteArrayOutputStream;
import java.io.Closeable;
import java.io.EOFException;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.channels.ReadableByteChannel;
import java.nio.file.Files;
import java.nio.file.LinkOption;

/**
 * Utility functions.
 *
 * @Immutable (has mutable data but it is write-only).
 */
final class IOUtils {

    private static final int COPY_BUF_SIZE = 8024;

    /**
     * Empty array of type {@link LinkOption}.
     *
     * @since 1.21
     */
    public static final LinkOption[] EMPTY_LINK_OPTIONS = {};

    

    /**
     * Skips the given number of bytes by repeatedly invoking skip on the given input stream if necessary.
     * <p>
     * In a case where the stream's skip() method returns 0 before the requested number of bytes has been skip this implementation will fall back to using the
     * read() method.
     * </p>
     * <p>
     * This method will only skip less than the requested number of bytes if the end of the input stream has been reached.
     * </p>
     *
     * @param input     stream to skip bytes in
     * @param numToSkip the number of bytes to skip
     * @return the number of bytes actually skipped
     * @throws IOException on error
     * @deprecated Use {@link org.apache.commons.io.IOUtils#skip(InputStream, long)}.
     */
    @Deprecated
    public static long skip(final InputStream input, final long numToSkip) throws IOException {
         if (numToSkip < 0) {
            throw new IllegalArgumentException("Skip count must be non-negative, actual: " + numToSkip);
        }
        //
        // No need to synchronize access to SCRATCH_BYTE_BUFFER_WO: We don't care if the buffer is written multiple
        // times or in parallel since the data is ignored. We reuse the same buffer, if the buffer size were variable or read-write,
        // we would need to synch or use a thread local to ensure some other thread safety.
        //
        long remain = numToSkip;
        while (remain > 0) {
            final byte[] skipBuffer = new byte[(int)Math.min(8192L,numToSkip)];
            // See https://issues.apache.org/jira/browse/IO-203 for why we use read() rather than delegating to skip()
            final long n = input.read(skipBuffer, 0, (int) Math.min(remain, skipBuffer.length));
            if (n < 0) { // EOF
                break;
            }
            remain -= n;
        }
        return numToSkip - remain;
    }

    

}